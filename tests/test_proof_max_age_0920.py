"""
Stale plain delivery PROOFs age out (`proof_max_age`, phase 1, 2026-09-20).

The far side's receipt deadline for a non-Link packet over this interface
is RNS `PacketReceipt.timeout` = `Transport.first_hop_timeout` (MTU 500 B
x 8 / `bitrate` 80 bps = 50 s, + `DEFAULT_PER_HOP_TIMEOUT` 6) + 6 s per RNS
hop = 62 s from the sender's transmit (`RNS/Packet.py` 428-431,
`RNS/Transport.py` `first_hop_timeout`); after it the receipt is FAILED and
a proof does nothing. The desktop's 2-hop phase of the 2026-09-20 session
(`fieldtests/raw/Alpha0.1.3/desktop_*`) queued 13 proofs while every
attempt missed and then transmitted 12 of them aged 45-105 s; replayed, a
45 s cap skips 16 attempts (~76 s of radio lock) and loses 3 proofs that
still landed inside the deadline, 60 s skips 12 and loses none, 30 s skips
24 and loses 4.

Pinned:
  * the default is 45 s and 0 disables;
  * the worker gives a plain PROOF `enqueued + proof_max_age` (the smaller
    of that and outgoing_max_age), a plain DATA outgoing_max_age, an
    LRPROOF / RESOURCE_PRF only outgoing_max_age;
  * a plain PROOF is dropped before ANY attempt once stale -- attempt 0
    misses, the age crosses the cap, attempt 1 is not transmitted, no path
    failure is recorded -- while an LRPROOF under the same timing retries;
  * an attempt-0 expiry inside the lock wait never falls through to a
    transmitted attempt 1 (a pre-existing gap closed with this change).
"""
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet

PEER = "abcdef012345"
DEST = bytes.fromhex("c5427c7a1878532bdc340ba787698ce6")


def _shipped(module):
    bare = module.SmartMeshCoreInterface.__new__(module.SmartMeshCoreInterface)
    bare._configure_retry({})
    return bare


class ProofMaxAgeDefaults(SingleNodeCase):
    def test_default_and_off(self):
        self.assertEqual(_shipped(self.module).proof_max_age_s, 45.0)
        bare = self.module.SmartMeshCoreInterface.__new__(self.module.SmartMeshCoreInterface)
        bare._configure_retry({"proof_max_age": "0"})
        self.assertEqual(bare.proof_max_age_s, 0.0)

    def test_plain_proof_classification(self):
        iface = self.iface
        self.assertTrue(iface._plain_proof(iface._parse_rns_header(build_rns_packet("proof", dest_hash=DEST, payload=b"s" * 64))))
        self.assertFalse(iface._plain_proof(iface._parse_rns_header(build_rns_packet("lrproof", dest_hash=DEST, payload=b"p" * 96))))
        self.assertFalse(iface._plain_proof(iface._parse_rns_header(build_rns_packet("data", dest_hash=DEST, payload=b"d"))))
        self.assertFalse(iface._plain_proof(None))


class WorkerDeadlines(SingleNodeCase):
    def test_deadline_per_packet_class(self):
        """Drive `_outgoing_worker`'s deadline arithmetic by capturing what
        `_send_outgoing_packet` receives as `expires_at`."""
        import asyncio
        iface = self.iface
        saved = (iface.outgoing_max_age_s, iface.proof_max_age_s)
        iface.outgoing_max_age_s, iface.proof_max_age_s = 120.0, 45.0
        seen = {}

        async def fake_send(data, header, expires_at=None, spawned=None):
            seen[header.context if header.packet_type == RNS.Packet.PROOF else "data"] = expires_at

        original = iface._send_outgoing_packet
        iface._send_outgoing_packet = fake_send
        try:
            for kind in ("proof", "lrproof", "data"):
                raw = build_rns_packet(kind, dest_hash=DEST, payload=b"x" * 64)
                self.on_loop(iface.process_outgoing, raw)
            deadline = time.monotonic() + 15.0
            while len(seen) < 3 and time.monotonic() < deadline:
                time.sleep(0.05)
        finally:
            iface._send_outgoing_packet = original
            iface.outgoing_max_age_s, iface.proof_max_age_s = saved
            iface._outgoing_inflight.clear()
        now = time.monotonic()
        self.assertEqual(len(seen), 3, f"worker delivered {seen}")
        self.assertAlmostEqual(seen[RNS.Packet.NONE] - now, 45.0, delta=3.0, msg="plain PROOF: proof_max_age")
        self.assertAlmostEqual(seen[RNS.Packet.LRPROOF] - now, 120.0, delta=3.0, msg="LRPROOF: outgoing_max_age only")
        self.assertAlmostEqual(seen["data"] - now, 120.0, delta=3.0, msg="plain DATA: outgoing_max_age")


class StaleProofIsNotRetried(SingleNodeCase):
    def _run(self, kind: str, expires_in_s: float):
        """Attempt 0 misses (fake), taking `expires_in_s` + 0.2 s of wall
        clock; returns the attempts transmitted and the results recorded."""
        iface = self.iface
        calls, recorded = [], []

        async def fake_send(target, frame, attempt=0, **kwargs):
            import asyncio
            calls.append(attempt)
            await asyncio.sleep(expires_in_s + 0.2)
            return False, True

        original_send = iface._send_direct_frame_and_wait_for_ack
        original_record = iface.record_direct_send_result
        iface._send_direct_frame_and_wait_for_ack = fake_send
        iface.record_direct_send_result = lambda peer, succeeded, waited_full_timeout: recorded.append(succeeded)
        raw = build_rns_packet(kind, dest_hash=DEST, payload=b"s" * 64)
        try:
            ok = self.node.run_on_loop(iface._send_direct_payload(
                "ab" * 32, PEER, raw, priority=iface._priority_tier(iface._parse_rns_header(raw)),
                expires_at=time.monotonic() + expires_in_s,
            ), timeout=30.0)
        finally:
            iface._send_direct_frame_and_wait_for_ack = original_send
            iface.record_direct_send_result = original_record
        return ok, calls, recorded

    def test_plain_proof_stops_at_the_cap_and_records_no_failure(self):
        ok, calls, recorded = self._run("proof", 0.3)
        self.assertFalse(ok)
        self.assertEqual(calls, [0], "attempt 1 must not be transmitted once the proof is stale")
        self.assertEqual(recorded, [], "an expired proof is not a path failure")

    def test_lrproof_under_the_same_timing_retries(self):
        iface = self.iface
        ok, calls, recorded = self._run("lrproof", 0.3)
        self.assertFalse(ok)
        self.assertEqual(calls, list(range(iface.direct_send_attempts_handshake)), "link-class proofs keep the full budget")
        self.assertEqual(recorded, [False])

    def test_plain_proof_with_the_cap_off_retries(self):
        iface = self.iface
        saved = iface.proof_max_age_s
        iface.proof_max_age_s = 0.0
        try:
            ok, calls, recorded = self._run("proof", 0.3)
        finally:
            iface.proof_max_age_s = saved
        self.assertEqual(calls, list(range(iface.direct_send_attempts)))


class ExpiryInTheLockWaitDoesNotFallThrough(SingleNodeCase):
    def test_attempt_zero_expired_in_lock_wait_is_not_followed_by_attempt_one(self):
        """The lock is held by someone else past the packet's deadline:
        attempt 0 returns from the ack-wait method untransmitted, and the
        loop must stop there (before this change it went on to attempt 1)."""
        import asyncio
        iface = self.iface
        raw = build_rns_packet("data", dest_hash=DEST, payload=b"d" * 32)
        sent = []
        original_frame = iface._send_direct_frame

        async def fake_frame(target, frame, attempt=0, **kwargs):
            sent.append(attempt)
            raise RuntimeError("should not transmit an expired packet")

        iface._send_direct_frame = fake_frame
        recorded = []
        original_record = iface.record_direct_send_result
        iface.record_direct_send_result = lambda peer, succeeded, waited_full_timeout: recorded.append(succeeded)
        try:
            async def drive():
                lock = iface._direct_exchange_lock_impl
                await lock.acquire(iface.PRIORITY_HANDSHAKE)
                task = asyncio.ensure_future(iface._send_direct_payload(
                    "ab" * 32, PEER, raw, priority=iface.PRIORITY_NORMAL, expires_at=time.monotonic() + 0.3,
                ))
                await asyncio.sleep(0.6)
                lock.release()
                return await task

            ok = self.node.run_on_loop(drive(), timeout=30.0)
        finally:
            iface._send_direct_frame = original_frame
            iface.record_direct_send_result = original_record
        self.assertFalse(ok)
        self.assertEqual(sent, [], f"transmitted attempts: {sent}")
        self.assertEqual(recorded, [])


if __name__ == "__main__":
    unittest.main()
