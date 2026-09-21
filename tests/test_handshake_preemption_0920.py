"""
Link handshakes pre-empt idle holds of the radio lock (phase 1, 2026-09-20).

Field evidence (`fieldtests/raw/Alpha0.1.3/`): link-critical attempts
(LINKREQUEST / LRPROOF / LRRTT) spent ~33 s in `lock_wait_s` over 26
attempts, median 1-3 s; at zero hop the holder was a completion
report/answer's ACK wait plus its listen (13 of 20 waits >= 0.5 s), a report
wait (4) or a bare send's ACK; at two hops a LINKREQUEST waited 3.2 s behind
a completion_answer's 8 s ACK miss inside a 17.4 s link. The largest tier-0
lock waits were not handshakes at all: a KEEPALIVE queued 20.3 s behind a raw
burst's duty-cycle throttle wait (26.3 s, the longest idle hold in the
session) -- which is why the pre-empting class is the LINK handshake only
(`_is_link_handshake`), never the whole PRIORITY_HANDSHAKE tier.

Pinned:
  * `_is_link_handshake`: LINKREQUEST, LRPROOF, LRRTT, LINKIDENTIFY,
    LINKPROOF pre-empt; KEEPALIVE, LINKCLOSE, RESOURCE_PRF, DATA do not;
  * the lock: a `preempt` waiter sets the event, a plain tier-0 waiter does
    not, the event clears only when the last pre-empting waiter is granted;
    `yield_to_preempt` re-queues the holder at YIELDED_PRIORITY -- behind
    the handshake, ahead of a NORMAL waiter that queued meanwhile;
  * `_idle_hold` ends early only on the event and only after its floor;
  * the duty-cycle throttle raises `_PreemptedForHandshake` when the event
    is set during its wait;
  * a raw burst yields after a fragment's gap (never inside it), resumes
    ahead of a NORMAL waiter, and its report still completes the send;
  * the report wait releases the lock on pre-emption and keeps listening;
    a report arriving then still completes the send without a QUERY;
  * a completion ANSWER's ACK wait is cut only after the expected-ACK floor,
    recorded as `ack_timeout_source="preempted"` with no backoff and no
    post-miss listen.
"""
import asyncio
import os
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet
from tests.test_completion_report_one_hop_0920 import _OneHopRawSend, PEER, TARGET

DEST = bytes.fromhex("c5427c7a1878532bdc340ba787698ce6")


class LinkHandshakeClass(SingleNodeCase):
    def test_classification(self):
        iface = self.iface
        yes = [build_rns_packet("link_request", dest_hash=DEST, payload=os.urandom(RNS.Link.ECPUBSIZE)),
               build_rns_packet("lrproof", dest_hash=DEST, payload=b"p" * 96)]
        for ctx in (RNS.Packet.LRRTT, RNS.Packet.LINKIDENTIFY, RNS.Packet.LINKPROOF):
            raw = bytearray(build_rns_packet("link_data", dest_hash=DEST, payload=b"x" * 8))
            raw[2 + 16] = ctx
            yes.append(bytes(raw))
        for raw in yes:
            self.assertTrue(iface._is_link_handshake(iface._parse_rns_header(raw)), raw[:20].hex())
        no = [build_rns_packet("data", dest_hash=DEST, payload=b"d"), build_rns_packet("resource", dest_hash=DEST, payload=b"r")]
        for ctx in (RNS.Packet.KEEPALIVE, RNS.Packet.LINKCLOSE, RNS.Packet.RESOURCE_PRF):
            raw = bytearray(build_rns_packet("link_data", dest_hash=DEST, payload=b"x" * 8))
            raw[2 + 16] = ctx
            no.append(bytes(raw))
        for raw in no:
            header = iface._parse_rns_header(raw)
            self.assertFalse(iface._is_link_handshake(header), raw[:20].hex())
        self.assertFalse(iface._is_link_handshake(None))
        # KEEPALIVE keeps its tier (tests/test_second_audit_0919.py pins it) -- it just does not pre-empt.
        raw = bytearray(build_rns_packet("link_data", dest_hash=DEST, payload=b"x" * 8))
        raw[2 + 16] = RNS.Packet.KEEPALIVE
        self.assertEqual(iface._priority_tier(iface._parse_rns_header(bytes(raw))), iface.PRIORITY_HANDSHAKE)


class LockPreemption(SingleNodeCase):
    def test_preempt_waiters_set_and_clear_the_event_and_yield_requeues_ahead_of_normal(self):
        iface = self.iface
        Lock = self.module._PriorityAsyncLock

        async def run():
            lock = Lock()
            order = []
            await lock.acquire(iface.PRIORITY_NORMAL)          # the burst
            event = lock.preempt_event()
            self.assertFalse(event.is_set())
            self.assertFalse(lock.preempt_requested())

            async def waiter(name, prio, preempt=False):
                await lock.acquire(prio, preempt=preempt)
                order.append(name)
                await asyncio.sleep(0.02)
                lock.release()

            t_keepalive = asyncio.ensure_future(waiter("keepalive", iface.PRIORITY_HANDSHAKE))
            await asyncio.sleep(0.01)
            self.assertFalse(event.is_set(), "a plain tier-0 waiter (KEEPALIVE) does not pre-empt")
            t_lr = asyncio.ensure_future(waiter("linkrequest", iface.PRIORITY_HANDSHAKE, preempt=True))
            t_lrrtt = asyncio.ensure_future(waiter("lrrtt", iface.PRIORITY_HANDSHAKE, preempt=True))
            await asyncio.sleep(0.01)
            self.assertTrue(event.is_set())
            self.assertTrue(lock.preempt_requested())
            t_normal = asyncio.ensure_future(waiter("normal", iface.PRIORITY_NORMAL))
            await asyncio.sleep(0.01)
            # The burst yields: the handshakes go (the KEEPALIVE too -- same
            # tier, FIFO ahead of them), then the burst resumes, and only
            # then the NORMAL waiter that queued meanwhile.
            await lock.yield_to_preempt()
            order.append("burst-resumed")
            self.assertFalse(event.is_set(), "cleared once the last pre-empting waiter was granted")
            lock.release()
            await asyncio.gather(t_keepalive, t_lr, t_lrrtt, t_normal)
            return order

        order = self.node.run_on_loop(run(), timeout=10.0)
        self.assertEqual(order, ["keepalive", "linkrequest", "lrrtt", "burst-resumed", "normal"])

    def test_event_clears_only_when_the_last_preempting_waiter_is_granted(self):
        Lock = self.module._PriorityAsyncLock

        async def run():
            lock = Lock()
            await lock.acquire(2)
            event = lock.preempt_event()
            a = asyncio.ensure_future(lock.acquire(0, preempt=True))
            b = asyncio.ensure_future(lock.acquire(0, preempt=True))
            await asyncio.sleep(0.01)
            self.assertTrue(event.is_set())
            lock.release()          # grants a
            await asyncio.sleep(0.01)
            self.assertTrue(event.is_set(), "b is still queued")
            lock.release()          # a's turn over -> grants b
            await asyncio.sleep(0.01)
            self.assertFalse(event.is_set())
            lock.release()
            await asyncio.gather(a, b)

        self.node.run_on_loop(run(), timeout=10.0)

    def test_idle_hold_ends_on_the_event_after_its_floor(self):
        iface = self.iface
        lock = iface._direct_exchange_lock

        async def run():
            t0 = time.monotonic()
            self.assertFalse(await iface._idle_hold(0.3))
            full = time.monotonic() - t0
            # a pre-empting waiter appears 0.1 s in; floor 0.2 s
            await lock.acquire(iface.PRIORITY_NORMAL)
            waiter = asyncio.ensure_future(lock.acquire(0, preempt=True))
            t0 = time.monotonic()
            cut = await iface._idle_hold(2.0, floor_s=0.2)
            took = time.monotonic() - t0
            lock.release()
            await waiter
            lock.release()
            return full, cut, took

        full, cut, took = self.node.run_on_loop(run(), timeout=10.0)
        self.assertGreaterEqual(full, 0.29)
        self.assertTrue(cut)
        self.assertGreaterEqual(took, 0.2, "never before the floor")
        self.assertLess(took, 0.6)

    def test_throttle_wait_raises_when_a_handshake_is_queued(self):
        iface = self.iface
        Limiter = self.module._DutyCycleLimiter

        async def run():
            limiter = Limiter(window_s=10.0, max_fraction=0.1)
            limiter.record(1.5)                     # the window is full
            event = asyncio.Event()
            asyncio.get_running_loop().call_later(0.1, event.set)
            t0 = time.monotonic()
            try:
                await limiter.wait_for_budget(0.5, interrupt=event)
            except self.module._PreemptedForHandshake:
                return "preempted", time.monotonic() - t0
            return "waited", time.monotonic() - t0

        outcome, took = self.node.run_on_loop(run(), timeout=10.0)
        self.assertEqual(outcome, "preempted")
        self.assertLess(took, 1.0)


class BurstYieldsToAHandshake(_OneHopRawSend):
    def test_burst_yields_after_a_gap_resumes_ahead_of_normal_and_the_report_still_completes(self):
        iface = self.iface
        pkt_id, frag_total = 601, 3
        lock = iface._direct_exchange_lock
        events = []

        def on_fragment(header, flagged, size):
            events.append(("frag", header.frag_idx, time.monotonic()))
            if header.frag_idx == 0:
                # a LINKREQUEST and an ordinary send queue while fragment 0's gap runs
                async def handshake():
                    await lock.acquire(iface.PRIORITY_HANDSHAKE, preempt=True)
                    events.append(("handshake", None, time.monotonic()))
                    await asyncio.sleep(0.05)
                    lock.release()

                async def normal():
                    await lock.acquire(iface.PRIORITY_NORMAL)
                    events.append(("normal", None, time.monotonic()))
                    lock.release()

                asyncio.ensure_future(handshake())
                asyncio.ensure_future(normal())

        sent, restore = self._install(on_fragment, lambda info: None)
        try:
            # A report for the whole burst arrives shortly after the last fragment.
            async def deliver_report_after_burst():
                while len([e for e in events if e[0] == "frag"]) < frag_total:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.05)
                self._report(pkt_id, frag_total, True, set(range(frag_total)), 0)

            async def run():
                asyncio.ensure_future(deliver_report_after_burst())
                return await iface._send_direct_raw_fragmented(
                    TARGET, PEER, self._payload_for(frag_total), pkt_id, priority=iface.PRIORITY_NORMAL, hop_count=1,
                )

            result = self.node.run_on_loop(run(), timeout=20.0)
            gap = iface._raw_fragment_gap_s(1, sent[0]["on_air"])   # with the fixture's gap factor
        finally:
            restore()
        self.assertIs(result, True, "the report completes the send; no QUERY was needed")
        kinds = [e[0] if e[0] != "frag" else f"frag{e[1]}" for e in events]
        self.assertEqual(kinds[:3], ["frag0", "handshake", "frag1"], f"yielded after fragment 0's gap: {kinds}")
        self.assertEqual(kinds[3:], ["frag2", "normal"], f"the burst resumed ahead of the NORMAL waiter: {kinds}")
        t_frag0 = [e for e in events if e[0] == "frag" and e[1] == 0][0][2]
        t_hs = [e for e in events if e[0] == "handshake"][0][2]
        self.assertGreaterEqual(t_hs - t_frag0, gap - 0.02, "the yield came after the gap, never inside it")

    def test_report_wait_releases_the_lock_and_a_late_report_still_completes(self):
        iface = self.iface
        pkt_id, frag_total = 602, 2
        lock = iface._direct_exchange_lock
        timeline = []
        queries = []
        saved_base = None

        sent, restore = self._install(lambda h, f, s: None, lambda info: queries.append(info))
        iface.direct_raw_report_wait_base_s = 1.5     # a long window: the handshake must not wait it out
        iface.direct_raw_report_wait_per_hop_s = 0.0
        try:
            async def run():
                async def handshake_after_burst():
                    while len(sent) < frag_total:
                        await asyncio.sleep(0.01)
                    await asyncio.sleep(0.3)   # inside the report wait
                    await lock.acquire(iface.PRIORITY_HANDSHAKE, preempt=True)
                    timeline.append(("handshake", time.monotonic()))
                    lock.release()
                    await asyncio.sleep(0.2)
                    # the report arrives after the lock was released, still inside the window
                    self._report(pkt_id, frag_total, True, {0, 1}, 0)
                    timeline.append(("report", time.monotonic()))

                asyncio.ensure_future(handshake_after_burst())
                t0 = time.monotonic()
                ok = await iface._send_direct_raw_fragmented(
                    TARGET, PEER, self._payload_for(frag_total), pkt_id, priority=iface.PRIORITY_NORMAL, hop_count=1,
                )
                return ok, time.monotonic() - t0

            ok, took = self.node.run_on_loop(run(), timeout=20.0)
        finally:
            restore()
        self.assertIs(ok, True)
        self.assertEqual(queries, [], "the late report, not a QUERY, completed the send")
        last_sent = max(s["t"] for s in sent)
        t_hs = [t for k, t in timeline if k == "handshake"][0]
        self.assertLess(t_hs - last_sent, 1.2, "the handshake got the radio before the 1.5 s window ran out")
        self.assertFalse(lock.locked())


class AnswerAckWaitIsPreemptible(SingleNodeCase):
    def test_cut_after_the_floor_records_preempted_without_backoff_or_listen(self):
        iface = self.iface
        lock = iface._direct_exchange_lock
        sent = type("S", (), {"payload": {"expected_ack": os.urandom(4), "suggested_timeout": 20000}})()
        iface.direct_ack_min_timeout_s = 8.0
        iface._ack_rtt.pop(PEER, None)

        async def run():
            await lock.acquire(iface.PRIORITY_ANSWER)
            waiter = asyncio.ensure_future(lock.acquire(0, preempt=True))
            await asyncio.sleep(0.01)
            t0 = time.monotonic()
            rx_window = iface._open_rx_log_window("ab" * 32)
            try:
                res = await iface._await_direct_ack(sent, PEER, 0, rx_window, t0, preemptible=True)
            finally:
                iface._close_rx_log_window(rx_window)
            took = time.monotonic() - t0
            lock.release()
            await waiter
            lock.release()
            return res, took

        (ok, waited_full, timeout_s, source, ack_latency, _abort), took = self.node.run_on_loop(run(), timeout=30.0)
        self.assertFalse(ok)
        self.assertFalse(waited_full)
        self.assertEqual(source, "preempted")
        floor = iface._ack_preempt_floor_s(PEER, 0)
        self.assertGreaterEqual(took, floor - 0.05, "never before the expected-ACK floor")
        self.assertLess(took, floor + 1.0)
        self.assertNotIn(PEER, iface._ack_rtt_snapshot, "no backoff / invalidation for a pre-empted wait")

    def test_not_preemptible_waits_the_full_timeout(self):
        iface = self.iface
        lock = iface._direct_exchange_lock
        sent = type("S", (), {"payload": {"expected_ack": os.urandom(4), "suggested_timeout": 1000}})()
        saved = iface.direct_ack_min_timeout_s
        iface.direct_ack_min_timeout_s = 1.0

        async def run():
            await lock.acquire(iface.PRIORITY_NORMAL)
            waiter = asyncio.ensure_future(lock.acquire(0, preempt=True))
            await asyncio.sleep(0.01)
            t0 = time.monotonic()
            rx_window = iface._open_rx_log_window("ab" * 32)
            try:
                res = await iface._await_direct_ack(sent, PEER, 0, rx_window, t0)
            finally:
                iface._close_rx_log_window(rx_window)
            took = time.monotonic() - t0
            lock.release()
            await waiter
            lock.release()
            return res, took

        try:
            (ok, _wf, _t, source, _l, _a), took = self.node.run_on_loop(run(), timeout=30.0)
        finally:
            iface.direct_ack_min_timeout_s = saved
        self.assertFalse(ok)
        self.assertNotEqual(source, "preempted")
        self.assertGreaterEqual(took, 0.95, "an ordinary frame's ACK wait is never cut")


if __name__ == "__main__":
    unittest.main()
