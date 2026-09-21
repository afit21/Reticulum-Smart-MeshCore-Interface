"""
Alpha 0.1.6, item 2: the bounded multi-hop window hold (2026-09-22).

The 2026-09-21 evening session at two hops (desktop capture, 22:25-22:28):
six LINKREQUESTs from the laptop in 2.5 minutes (MeshChat re-requests
every ~17 s), each answered by an LRPROOF of four attempts at 11 s ACK
timeouts queued at the handshake tier, so completion answers and reports
waited 50-125 s for the radio and a text fragment 182 s; every two-hop
window ran three rounds (a 3-fragment burst with 4.5 s gaps, a report
wait, up to two QUERY exchanges of ~18 s).

Pinned:
  * the rounds rule: `direct_raw_reconcile_rounds` at zero hop, capped at
    `direct_raw_window_max_rounds` (2) through repeaters, 0 = no cap;
  * a two-hop window yields between its parts to a queued handshake and to
    a queued completion report (the item-6 mechanism is hop-independent);
  * a handshake queued during a two-hop window's report wait gets the
    radio before the window's next round;
  * LRPROOF supersession: a newer LINKREQUEST from the same peer expires
    the LRPROOF still pending for its earlier link -- no further attempts,
    no path evidence, captured as `superseded` / `lrproof_superseded`;
    the newest link's own LRPROOF is untouched;
  * the QUERY's quiet hold yields to a queued completion report.
"""
import asyncio
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet, wait_until
from tests.test_completion_report_one_hop_0920 import PEER, TARGET, _sink
from tests.test_reconcile_m2_window_0920 import _WindowSend

TWO_HOP_PATH_HEX = "aabb"
DEST = bytes.fromhex("c5427c7a1878532bdc340ba787698ce6")


class RoundsRule(SingleNodeCase):
    def test_rounds_rule(self):
        rule = self.module.SmartMeshCoreInterface._raw_window_rounds_rule
        self.assertEqual(rule(3, 2, 0), 3, "zero hop keeps direct_raw_reconcile_rounds")
        self.assertEqual(rule(3, 2, 1), 2)
        self.assertEqual(rule(3, 2, 2), 2)
        self.assertEqual(rule(3, 0, 2), 3, "0 = no separate cap")
        self.assertEqual(rule(1, 2, 2), 1, "never more than the reconcile rounds")
        self.assertEqual(rule(0, 0, 0), 1)

    def test_shipped_default(self):
        bare = self.module.SmartMeshCoreInterface.__new__(self.module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertEqual(bare.direct_raw_window_max_rounds, 2)
        self.assertEqual(self.iface._raw_window_rounds(2), min(self.iface.direct_raw_reconcile_rounds, 2))


class _TwoHopWindow(_WindowSend):
    """The window fixture at two hops: the path aabb, hop_count 2."""

    def _install_two_hop(self, on_fragment, on_query, collect_s=0.3):
        sent, sink, restore_all = self._install_window(on_fragment, on_query, collect_s=collect_s)
        self.iface._resolved_paths[PEER] = self.module._ResolvedPath(TWO_HOP_PATH_HEX, 2, 1, time.monotonic())
        return sent, sink, restore_all

    def _run_parts_two_hop(self, payloads_by_pkt, report_after_burst=None, timeout=40.0):
        iface = self.iface

        async def run():
            tasks = {pkt: asyncio.ensure_future(iface._send_direct_raw_fragmented(
                TARGET, PEER, payload, pkt, priority=iface.PRIORITY_NORMAL, hop_count=2))
                for pkt, payload in payloads_by_pkt.items()}
            if report_after_burst is not None:
                asyncio.ensure_future(report_after_burst())
            return {pkt: await t for pkt, t in tasks.items()}

        return self.node.run_on_loop(run(), timeout=timeout)


class TwoHopWindowYields(_TwoHopWindow):
    def test_handshake_and_report_queued_during_part_one_go_out_before_part_two(self):
        iface = self.iface
        lock = iface._direct_exchange_lock
        order = []
        pkts = {931: self._payload_for(2), 932: self._payload_for(2), 933: self._payload_for(2)}

        def on_fragment(header, flagged, size):
            order.append(("frag", header.pkt_id, header.frag_idx))
            if header.pkt_id == 931 and header.frag_idx == 0 and header.attempt == 0:
                async def handshake():
                    await lock.acquire(iface.PRIORITY_HANDSHAKE, preempt=True)
                    order.append(("handshake", None, None))
                    await asyncio.sleep(0.02)
                    lock.release()

                async def report():
                    await lock.acquire(iface.PRIORITY_ANSWER, report=True)
                    order.append(("report", None, None))
                    await asyncio.sleep(0.02)
                    lock.release()

                asyncio.ensure_future(handshake())
                asyncio.ensure_future(report())

        sent, sink, restore = self._install_two_hop(on_fragment, lambda info: None)
        try:
            async def report_when_done():
                while len(sent) < 6:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.05)
                self._report_v4([(p, 2, True, {0, 1}) for p in pkts])

            results = self._run_parts_two_hop(pkts, report_when_done)
        finally:
            restore()
        self.assertEqual(set(results.values()), {True}, results)
        frags = [o for o in order if o[0] == "frag"]
        self.assertEqual(len(frags), 6)
        # Both waiters got the radio before part 932's first fragment: the
        # handshake at the first gap or part boundary, the report at the
        # boundary (never inside a part), the handshake first when both wait.
        idx = {o: i for i, o in enumerate(order)}
        first_932 = order.index(("frag", 932, 0))
        self.assertLess(idx[("handshake", None, None)], first_932, order)
        self.assertLess(idx[("report", None, None)], first_932, order)
        self.assertLess(idx[("handshake", None, None)], idx[("report", None, None)], order)
        self.assertGreater(idx[("report", None, None)], order.index(("frag", 931, 1)),
                           "the report goes out between parts, not inside part 931")
        recs = sink.records("raw_fragment_sent")
        self.assertEqual({r["hop_count"] for r in recs}, {2})
        self.assertGreaterEqual(max(r["report_yields"] for r in recs), 1)
        self.assertGreaterEqual(max(r["handshake_yields"] for r in recs), 1)

    def test_handshake_queued_in_the_report_wait_goes_before_the_next_round(self):
        iface = self.iface
        lock = iface._direct_exchange_lock
        timeline = []
        queries = []

        def on_fragment(header, flagged, size):
            timeline.append((f"frag{header.frag_idx}r{header.attempt}", time.monotonic()))

        sent, sink, restore = self._install_two_hop(on_fragment, lambda info: (queries.append(info), None)[1])
        iface.direct_raw_report_wait_base_s = 1.0
        iface.direct_raw_report_wait_per_hop_s = 0.0
        iface.direct_raw_reburst_after_unanswered = 1   # an unanswered round re-bursts at once
        try:
            async def handshake_in_report_wait():
                while len(sent) < 2:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.2)
                await lock.acquire(iface.PRIORITY_HANDSHAKE, preempt=True)
                timeline.append(("handshake", time.monotonic()))
                await asyncio.sleep(0.05)
                lock.release()

            async def run():
                asyncio.ensure_future(handshake_in_report_wait())
                return await iface._send_direct_raw_fragmented(
                    TARGET, PEER, self._payload_for(2), 941, priority=iface.PRIORITY_NORMAL, hop_count=2)
            result = self.node.run_on_loop(run(), timeout=40.0)
        finally:
            restore()
        self.assertFalse(result, "no report and no answer ever: the window fails after its rounds")
        kinds = [k for k, _t in timeline]
        self.assertIn("handshake", kinds, kinds)
        hs = kinds.index("handshake")
        round1 = [i for i, k in enumerate(kinds) if k.endswith("r1")]
        self.assertTrue(round1 and hs < round1[0], f"the handshake went before round 1's burst: {kinds}")
        self.assertEqual(len([k for k in kinds if k.startswith("frag")]), 4,
                         f"two rounds at two hops (direct_raw_window_max_rounds), not three: {kinds}")


class LinkProofSupersession(SingleNodeCase):
    def setUp(self):
        # Token learning (and so supersession) only from a bound peer.
        self.on_loop(self.iface._register_peer, PEER, True, "test")

    def tearDown(self):
        self.iface._peers.pop(PEER, None)

    def _lrproof(self, link_id):
        raw = build_rns_packet("lrproof", dest_hash=link_id, payload=b"p" * 96)
        header = self.iface._parse_rns_header(raw)
        return raw, header

    def test_key_and_direct_supersession(self):
        iface = self.iface
        link_a, link_b = bytes(range(16)), bytes(range(16, 32))
        raw_a, header_a = self._lrproof(link_a)
        key_a = iface._answered_send_key(raw_a, header_a)
        self.assertEqual(key_a, iface.LRPROOF_KEY_PREFIX + link_a)
        iface._pending_link_proofs[PEER] = {link_a, link_b}
        try:
            n = self.on_loop(iface._supersede_link_proofs, PEER, link_b)
            self.assertEqual(n, 1, "only the earlier link's proof is superseded")
            self.assertTrue(iface._send_superseded(key_a))
            self.assertFalse(iface._send_superseded(iface.LRPROOF_KEY_PREFIX + link_b))
            self.assertEqual(self.on_loop(iface._supersede_link_proofs, PEER, link_b), 0, "not counted twice")
            self.assertTrue(self.on_loop(iface._answered_send_event, key_a).superseded)
        finally:
            iface._pending_link_proofs.pop(PEER, None)
            iface._send_answered_at.pop(key_a, None)
            iface._send_answered_how.pop(key_a, None)
            iface._send_answered_events.pop(key_a, None)

    def test_a_newer_linkrequest_stops_the_earlier_lrproof_retries(self):
        iface = self.iface
        link_a = bytes(range(32, 48))
        raw_a, header_a = self._lrproof(link_a)
        key_a = iface._answered_send_key(raw_a, header_a)
        calls, recorded = [], []
        sink, restore_sink = _sink(iface)

        async def fake_send(target, frame, attempt=0, **kwargs):
            calls.append(attempt)
            await asyncio.sleep(0.3)
            return False, True

        original_send = iface._send_direct_frame_and_wait_for_ack
        original_record = iface.record_direct_send_result
        iface._send_direct_frame_and_wait_for_ack = fake_send
        iface.record_direct_send_result = lambda peer, succeeded, waited_full_timeout, **kw: recorded.append(succeeded)
        iface._pending_link_proofs[PEER] = {link_a}
        try:
            async def run():
                task = asyncio.ensure_future(iface._send_direct_payload(
                    TARGET, PEER, raw_a, priority=iface._priority_tier(header_a), expires_at=time.monotonic() + 30))
                await asyncio.sleep(0.1)   # attempt 0 in flight
                # The peer asks again: a LINKREQUEST for a NEW link arrives.
                new_req = build_rns_packet("link_request", dest_hash=DEST, payload=b"k" * 64)
                iface._observe_incoming_rns_packet(new_req, PEER)
                return await task
            ok = self.node.run_on_loop(run(), timeout=20.0)
        finally:
            iface._send_direct_frame_and_wait_for_ack = original_send
            iface.record_direct_send_result = original_record
            iface._pending_link_proofs.pop(PEER, None)
            restore_sink()
            for d in (iface._send_answered_at, iface._send_answered_how, iface._send_answered_events):
                d.pop(key_a, None)
        self.assertFalse(ok)
        self.assertEqual(calls, [0], f"attempt 1 of the superseded LRPROOF was not transmitted: {calls}")
        self.assertEqual(recorded, [], "a superseded proof is not path evidence")
        superseded = [r for r in sink.records("direct_attempt_result") if r.get("ack_timeout_source") == "superseded"]
        self.assertEqual(len(superseded), 1, "the skipped attempt is captured as superseded")

    def test_the_lrproof_of_the_newest_link_is_untouched(self):
        iface = self.iface
        new_req = build_rns_packet("link_request", dest_hash=DEST, payload=b"n" * 64)
        link_new = iface._compute_link_id(new_req)
        iface._pending_link_proofs[PEER] = {link_new}
        try:
            self.on_loop(iface._observe_incoming_rns_packet, new_req, PEER)
            self.assertFalse(iface._send_superseded(iface.LRPROOF_KEY_PREFIX + link_new))
        finally:
            iface._pending_link_proofs.pop(PEER, None)

    def test_superseded_during_the_rtt_delay_is_dropped_before_dispatch(self):
        iface = self.iface
        link_a = bytes(range(48, 64))
        raw_a, header_a = self._lrproof(link_a)
        iface._rns_token_peer[header_a.destination_hash] = PEER
        dispatched = []
        sink, restore_sink = _sink(iface)
        original = iface._dispatch_outgoing_packet

        async def fake_dispatch(data, header, expires_at=None, spawned=None):
            dispatched.append(header)

        iface._dispatch_outgoing_packet = fake_dispatch
        try:
            async def run():
                task = asyncio.ensure_future(iface._send_delayed_link_proof(raw_a, header_a, expires_at=time.monotonic() + 30))
                await asyncio.sleep(0.05)
                self.assertIn(link_a, iface._pending_link_proofs.get(PEER, set()))
                new_req = build_rns_packet("link_request", dest_hash=DEST, payload=b"q" * 64)
                iface._observe_incoming_rns_packet(new_req, PEER)
                await task
            self.node.run_on_loop(run(), timeout=20.0)
        finally:
            iface._dispatch_outgoing_packet = original
            restore_sink()
            iface._rns_token_peer.pop(header_a.destination_hash, None)
            key = iface.LRPROOF_KEY_PREFIX + link_a
            for d in (iface._send_answered_at, iface._send_answered_how, iface._send_answered_events):
                d.pop(key, None)
        self.assertEqual(dispatched, [], "the superseded LRPROOF never reached the dispatcher")
        self.assertNotIn(PEER, iface._pending_link_proofs)
        drops = [r for r in sink.records() if r.get("routing_decision") == "lrproof_superseded"]
        self.assertEqual(len(drops), 1)


class QuietHoldYieldsToAReport(SingleNodeCase):
    def test_wait_future_or_preempt_with_reports_is_cut_by_a_report_waiter(self):
        iface = self.iface
        lock = iface._direct_exchange_lock

        async def scenario():
            fut = asyncio.get_running_loop().create_future()
            await lock.acquire(iface.PRIORITY_NORMAL)
            try:
                async def report_waiter():
                    await asyncio.sleep(0.1)
                    await lock.acquire(iface.PRIORITY_ANSWER, report=True)
                    lock.release()
                w = asyncio.ensure_future(report_waiter())
                t0 = time.monotonic()
                done, cut = await iface._wait_future_or_preempt(fut, 2.0, also_reports=True)
                took = time.monotonic() - t0
            finally:
                lock.release()
            await w
            return done, cut, took

        done, cut, took = self.node.run_on_loop(scenario(), timeout=10.0)
        self.assertFalse(done)
        self.assertTrue(cut, "a queued completion report ends the hold")
        self.assertLess(took, 1.0)


if __name__ == "__main__":
    unittest.main()
