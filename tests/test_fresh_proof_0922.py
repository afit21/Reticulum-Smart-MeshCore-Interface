"""
Alpha 0.1.7, item 1 (2026-09-22): a young plain PROOF goes ahead of bulk.

The field (desktop capture, 2026-09-22 11:49:34-11:50:44, one hop): the
laptop sent one 211 B LXMF message six times. The desktop's RNS proved
every copy at once (`out PROOF NONE 83 B` on the same second as each `in
DATA`), but each proof left the radio 5-20 s later, queued behind the page
windows the desktop was serving; LXMF re-sends an unproved opportunistic
message after DELIVERY_RETRY_WAIT 10 s. A plain PROOF answering fresh DATA
is time-critical the way a handshake is, and it was bulk-tier.

Now a context-NONE PROOF younger than `proof_fresh_s` (8 s, measured from
the moment RNS queued it, which is within milliseconds of the DATA it
answers) pre-empts idle holds of the radio lock and is taken at the raw
window's existing yield points, exactly as an LRPROOF and the receiver's
own report are -- the `preempt` flag the lock already consults, re-read at
every attempt. Its tier (ANSWER), attempt budget and duty accounting are
unchanged; an older proof stays bulk-tier and still expires at
proof_max_age. The lock's yielding holder resumes half a step behind the
tier that pre-empted it (1.5 behind a fresh proof, 0.5 behind a handshake).
"""
import asyncio
import os
import time
import types
import unittest

from tests._support import SingleNodeCase, build_rns_packet
from tests.test_completion_report_one_hop_0920 import PEER, TARGET
from tests.test_reconcile_m2_window_0920 import _WindowSend


def _plain_proof(iface):
    data = build_rns_packet("data", dest_hash=os.urandom(16), payload=b"lxmf" + os.urandom(180))
    truncated = iface._compute_truncated_hash(data, iface._parse_rns_header(data).header_type)
    return build_rns_packet("proof", dest_hash=truncated, payload=os.urandom(64)), truncated


class FreshProofRule(SingleNodeCase):
    def test_rule_and_default(self):
        iface = self.iface
        bare = self.module.SmartMeshCoreInterface.__new__(self.module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertEqual(bare.proof_fresh_s, 8.0)
        self.assertEqual(bare.proof_max_age_s, 45.0, "unchanged")
        self.assertTrue(iface._proof_is_fresh(0.0))
        self.assertTrue(iface._proof_is_fresh(7.9))
        self.assertFalse(iface._proof_is_fresh(8.0))
        self.assertFalse(iface._proof_is_fresh(30.0))
        self.assertFalse(iface._proof_is_fresh(None))
        saved = iface.proof_fresh_s
        try:
            iface.proof_fresh_s = 0.0
            self.assertFalse(iface._proof_is_fresh(0.0), "0 disables")
        finally:
            iface.proof_fresh_s = saved

    def test_queue_time_is_recorded_for_plain_proofs_only(self):
        iface = self.iface
        proof, truncated = _plain_proof(iface)
        before = time.monotonic()
        iface.process_outgoing(proof)
        header = iface._parse_rns_header(proof)
        t = iface._proof_enqueued_at_for(header)
        self.assertIsNotNone(t)
        self.assertGreaterEqual(t, before)
        self.assertIsNone(iface._proof_enqueued_at_for(iface._parse_rns_header(build_rns_packet("lrproof", dest_hash=os.urandom(16)))))
        self.assertIsNone(iface._proof_enqueued_at_for(iface._parse_rns_header(build_rns_packet("data", dest_hash=truncated))))
        self.assertIsNone(iface._proof_enqueued_at_for(None))
        # drain what process_outgoing queued (an unresolvable proof is dropped by the dispatcher)
        time.sleep(0.2)

    def test_queue_times_are_bounded_and_swept(self):
        iface = self.iface
        iface._proof_enqueued_at.clear()
        now = time.monotonic()
        for i in range(iface.PROOF_ENQUEUED_MAX_KEYS + 5):
            iface._note_proof_enqueued(i.to_bytes(16, "big"), now)
        self.assertEqual(len(iface._proof_enqueued_at), iface.PROOF_ENQUEUED_MAX_KEYS)
        self.assertNotIn((0).to_bytes(16, "big"), iface._proof_enqueued_at)
        iface._note_proof_enqueued(b"old" * 5 + b"!", now - iface.proof_max_age_s - 1)   # evicts one more
        iface._proof_correlation_sweep(now)
        self.assertNotIn(b"old" * 5 + b"!", iface._proof_enqueued_at, "swept: older than proof_max_age")
        self.assertEqual(len(iface._proof_enqueued_at), iface.PROOF_ENQUEUED_MAX_KEYS - 1)
        iface._proof_enqueued_at.clear()


class LockResumeTier(SingleNodeCase):
    def test_resume_priority_follows_the_preempting_tier(self):
        iface = self.iface
        Lock = self.module._PriorityAsyncLock

        async def run():
            lock = Lock()
            await lock.acquire(iface.PRIORITY_NORMAL)
            self.assertEqual(lock.preempt_resume_priority(), Lock.YIELDED_PRIORITY)
            proof = asyncio.ensure_future(lock.acquire(iface.PRIORITY_ANSWER, preempt=True))
            await asyncio.sleep(0.01)
            self.assertEqual(lock.preempt_resume_priority(), 1.5, "behind a fresh proof at the ANSWER tier")
            handshake = asyncio.ensure_future(lock.acquire(iface.PRIORITY_HANDSHAKE, preempt=True))
            await asyncio.sleep(0.01)
            self.assertEqual(lock.preempt_resume_priority(), Lock.YIELDED_PRIORITY, "a handshake sets the resume tier")
            lock.release(); await handshake; lock.release(); await proof; lock.release()

        self.node.run_on_loop(run(), timeout=10.0)

    def test_yield_lets_every_queued_fresh_proof_out_before_the_holder_resumes(self):
        # The case the resume tier exists for: two fresh proofs queued; a
        # resume at 0.5 would splice the window between them.
        iface = self.iface
        Lock = self.module._PriorityAsyncLock

        async def run():
            lock = Lock()
            order = []
            await lock.acquire(iface.PRIORITY_NORMAL)

            async def waiter(name, prio, preempt=False):
                await lock.acquire(prio, preempt=preempt)
                order.append(name)
                await asyncio.sleep(0.02)
                lock.release()

            ta = asyncio.ensure_future(waiter("proofA", iface.PRIORITY_ANSWER, preempt=True))
            tb = asyncio.ensure_future(waiter("proofB", iface.PRIORITY_ANSWER, preempt=True))
            tn = asyncio.ensure_future(waiter("normal", iface.PRIORITY_NORMAL))
            await asyncio.sleep(0.01)
            self.assertTrue(lock.preempt_requested())
            await lock.yield_to_preempt()
            order.append("window-resumed")
            lock.release()
            await asyncio.gather(ta, tb, tn)
            return order

        self.assertEqual(self.node.run_on_loop(run(), timeout=10.0), ["proofA", "proofB", "window-resumed", "normal"])


class FreshProofAndTheWindow(_WindowSend):
    """A three-part window at one hop; a plain PROOF dispatched through the
    real `_send_direct_payload` -> `_send_direct_with_attempts` -> the radio
    lock, with only the radio keying itself stubbed (no ACK expected)."""

    def _run_with_proof(self, proof_age_s):
        iface = self.iface
        lock = iface._direct_exchange_lock
        order = []
        pkts = {941: self._payload_for(2), 942: self._payload_for(2), 943: self._payload_for(2)}
        proof, _truncated = _plain_proof(iface)
        header = iface._parse_rns_header(proof)
        iface._note_proof_enqueued(header.destination_hash, time.monotonic() - proof_age_s)
        saved_send = iface._send_direct_frame

        async def fake_send_direct_frame(target, frame, attempt=0, **kw):
            order.append(("proof", None, None))
            return types.SimpleNamespace(payload={})

        iface._send_direct_frame = fake_send_direct_frame
        proof_task = {}

        def on_fragment(header, flagged, size):
            order.append(("frag", header.pkt_id, header.frag_idx))
            if header.pkt_id == 941 and header.frag_idx == 0 and header.attempt == 0:
                proof_task["t"] = asyncio.ensure_future(iface._send_direct_payload(
                    TARGET, PEER, proof, priority=iface.PRIORITY_ANSWER, hop_count=1))

        sent, sink, restore = self._install_window(on_fragment, lambda info: None)
        try:
            async def report_when_done():
                while len(sent) < 6:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.05)
                self._report_v4([(p, 2, True, {0, 1}) for p in pkts])

            results = self._run_parts(pkts, report_when_done)
            self.node.run_on_loop(asyncio.wait_for(proof_task["t"], 20.0), timeout=25.0)
            self.assertFalse(lock.preempt_requested())
        finally:
            iface._send_direct_frame = saved_send
            restore()
        self.assertEqual(set(results.values()), {True}, results)
        attempts = [r for r in sink.records("direct_attempt_result") if r.get("proof_age_s") is not None]
        self.assertEqual(len(attempts), 1, attempts)
        return order, attempts[0]

    def test_a_fresh_proof_goes_out_before_the_next_part(self):
        order, attempt = self._run_with_proof(proof_age_s=0.5)
        idx = {o: i for i, o in enumerate(order)}
        self.assertIn(("proof", None, None), idx)
        self.assertLess(idx[("proof", None, None)], idx[("frag", 942, 0)], order)
        self.assertTrue(attempt["proof_fresh"])
        self.assertLess(attempt["proof_age_s"], self.iface.proof_fresh_s)
        self.assertTrue(attempt["ok"])

    def test_a_30s_old_proof_waits_for_the_window(self):
        order, attempt = self._run_with_proof(proof_age_s=30.0)
        idx = {o: i for i, o in enumerate(order)}
        self.assertGreater(idx[("proof", None, None)], idx[("frag", 943, 1)], order)
        self.assertFalse(attempt["proof_fresh"])
        self.assertGreaterEqual(attempt["proof_age_s"], 30.0)


if __name__ == "__main__":
    unittest.main()
