"""Alpha 0.1.8, item 1: the proof is the completion.

RNS proves every single-destination DATA packet, so for a raw window whose
packets it will prove, the PROOF already tells the sender exactly what the
completion report would -- "I have it" -- and it has to be sent anyway. The
report is therefore the redundant frame, not the proof.

Field evidence (2026-09-22 evening, `fieldtests/raw/Alpha0.1.7/`, the
two-hop stop 22:29-22:45):

  * the receiver sent 22 complete reports (`completion_report_sent`, 17 of
    them with `held_s` 0, i.e. immediately). The sender received 3 inside
    its report wait and 2 more stale: the report is a no-ACK frame, one
    transmission, never retried, and through two repeaters it mostly does
    not arrive. The sender then waited out its 10-18 s report wait and ran
    a QUERY -- 23 QUERYs in 17 minutes, 0.93 QUERY attempts per raw send
    against 0.22 at one hop in alpha 0.1.6 -- and every one that was
    answered said the data had already arrived.
  * because the receiver sends the report FIRST and then holds the radio
    for the report's own relay window, the proof keyed about 8 s after the
    packet landed and its first attempt succeeded 3 times of 14. Proof
    turnaround at two hops: 17.9 s median, 30.6 s p90, 45 s max.

Receiver: the complete report is held for `proof_report_grace_s` and
dropped if RNS proves the packet inside it. Sender: an inbound PROOF for a
packet in an open window completes that packet, and a window whose packets
are all proved ends with the outcome `proved` -- no report wait past the
proof, no QUERY.

Nothing here cuts a hold. The alpha 0.1.7 second cut (a fresh proof must
NOT cut the no-ACK report hold or the QUERY quiet hold, because a proof
keyed into the repeater's relay window for this node's own frame is a
certain miss -- MeshBench `large_payload` 22 s -> 45 s probe RTT) stands
untouched: this item works by not sending a frame.
"""
import asyncio
import os
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet
from tests.test_completion_report_one_hop_0920 import PEER, TARGET
from tests.test_reconcile_m2_window_0920 import _WindowSend

PKT_A, PKT_B = 21, 22


def _data_and_proof(iface, payload=None, dest_hash=None):
    """A plain DATA to a SINGLE destination and the PROOF RNS would answer
    it with: its destination field is the DATA's truncated packet hash."""
    data = build_rns_packet("data", dest_hash=dest_hash or os.urandom(16),
                            payload=payload if payload is not None else b"lxmf" + os.urandom(180))
    key = iface._compute_truncated_hash(data, iface._parse_rns_header(data).header_type)
    proof = build_rns_packet("proof", dest_hash=key, payload=os.urandom(64))
    return data, proof, key


class ProofKeyAndGates(SingleNodeCase):
    def test_default_and_that_zero_disables(self):
        bare = self.module.SmartMeshCoreInterface.__new__(self.module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertEqual(bare.proof_report_grace_s, 0.25)
        self.assertEqual(bare.proof_fresh_s, 8.0, "alpha 0.1.7's key is unchanged")
        # Measured on the installed RNS 1.4.2 with a real Reticulum and a
        # PROVE_ALL destination: the PROOF reaches process_outgoing a
        # median 0.08 ms and at worst 0.19 ms after Transport.inbound is
        # handed the packet (inbound is synchronous there, and LXMF's
        # delivery_packet calls prove() on its first line). The default is
        # ~1300x that, to cover RNS 1.5's inbound queue and a loaded host.
        self.assertGreater(bare.proof_report_grace_s, 0.19 / 1000.0 * 100)

    def test_only_a_plain_data_to_a_single_destination_expects_a_proof(self):
        iface = self.iface
        data, _proof, key = _data_and_proof(iface)
        self.assertEqual(iface._proof_expected_key(data, iface._parse_rns_header(data)), key)
        for kind in ("announce", "proof"):
            other = build_rns_packet(kind, dest_hash=os.urandom(16), payload=os.urandom(64))
            self.assertIsNone(iface._proof_expected_key(other, iface._parse_rns_header(other)),
                              f"{kind} is not proved per packet")
        self.assertIsNone(iface._proof_expected_key(b"", None))

    def test_the_four_gates(self):
        iface = self.iface
        data, _proof, key = _data_and_proof(iface)
        header = iface._parse_rns_header(data)
        saved_local = iface._is_local_destination
        saved_path = iface._resolved_paths.get(PEER)
        try:
            iface._is_local_destination = lambda h: True
            iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=False,
                                                         last_seen=time.time())
            iface._resolved_paths[PEER] = self.module._ResolvedPath("19", 1, 1, time.monotonic())
            iface._recent_raw_pkts.pop(PEER, None)
            self.assertEqual(iface._proof_may_replace_report(data, header, PEER, PEER), key,
                             "all four gates pass")

            # 1: RNS does not prove this packet per packet.
            ann = build_rns_packet("announce", dest_hash=os.urandom(16), payload=os.urandom(64))
            self.assertIsNone(iface._proof_may_replace_report(ann, iface._parse_rns_header(ann), PEER, PEER))

            # 2: the destination is not served by this node's own RNS.
            iface._is_local_destination = lambda h: False
            self.assertIsNone(iface._proof_may_replace_report(data, header, PEER, PEER),
                              "a transport node relaying it onward proves nothing")
            iface._is_local_destination = lambda h: True

            # 3: something else of this sender's is still incomplete -- its
            # bitmap is the sender's only way to re-drive exactly the
            # missing fragments without a QUERY, so the report still goes.
            iface._open_bucket_for_test = None
            iface._recent_raw_pkts.setdefault(PEER, {})[(PKT_B, 3)] = time.monotonic()
            self.assertIsNone(iface._proof_may_replace_report(data, header, PEER, PEER),
                              "an incomplete sibling part keeps the report")
            iface._recent_raw_pkts.pop(PEER, None)
            self.assertEqual(iface._proof_may_replace_report(data, header, PEER, PEER), key)

            # 4: the sender has no resolved path, so the proof would not
            # route back to it DIRECT.
            iface._resolved_paths.pop(PEER, None)
            self.assertIsNone(iface._proof_may_replace_report(data, header, PEER, PEER))
            iface._resolved_paths[PEER] = self.module._ResolvedPath("19", 1, 1, time.monotonic())

            # The key at 0 disables the whole item.
            saved_grace = iface.proof_report_grace_s
            iface.proof_report_grace_s = 0.0
            self.assertIsNone(iface._proof_may_replace_report(data, header, PEER, PEER))
            iface.proof_report_grace_s = saved_grace
        finally:
            iface._is_local_destination = saved_local
            iface._peers.pop(PEER, None)
            iface._recent_raw_pkts.pop(PEER, None)
            if saved_path is None:
                iface._resolved_paths.pop(PEER, None)
            else:
                iface._resolved_paths[PEER] = saved_path


class SenderWindowEndsOnTheProof(_WindowSend):
    """The sender's side: a window whose packets RNS proves ends `proved`."""

    def _payload_for(self, frags):
        return os.urandom(self.iface._direct_raw_payload_budget(1) * (frags - 1) + 10)

    def test_a_single_packet_window_is_completed_by_the_proof_with_no_query(self):
        iface = self.iface
        queries, reports = [], []
        sent, sink, restore = self._install_window(lambda h, f, s: None, lambda info: queries.append(info))
        captured = []
        saved_capture = iface._capture_event
        iface._capture_event = lambda direction, rec: captured.append(rec)
        try:
            data, proof, key = _data_and_proof(iface, payload=self._payload_for(2))

            async def prove_when_burst_done():
                while len(sent) < 2:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.05)
                # Exactly what _peers.py does for every inbound DIRECT PROOF.
                iface._signal_send_answered(key, "DIRECT PROOF", PEER)

            async def run():
                task = asyncio.ensure_future(iface._send_direct_raw_fragmented(
                    TARGET, PEER, data, PKT_A, priority=iface.PRIORITY_NORMAL, hop_count=1))
                asyncio.ensure_future(prove_when_burst_done())
                return await task

            t0 = time.monotonic()
            result = self.node.run_on_loop(run(), timeout=30.0)
            elapsed = time.monotonic() - t0
        finally:
            iface._capture_event = saved_capture
            iface._send_answered_at.pop(key, None)
            iface._send_answered_events.pop(key, None)
            restore()

        self.assertIs(result, True, "the window completed")
        self.assertEqual(queries, [], "no QUERY: the proof already said the data arrived")
        self.assertLess(elapsed, iface.direct_raw_report_wait_base_s + 1.0,
                        "the window ended on the proof, not by waiting the report out")
        outcomes = [r for r in captured if r.get("event") == "completion_check_result"]
        self.assertEqual([r["outcome"] for r in outcomes], ["proved"])
        self.assertEqual(outcomes[0]["pkt_id"], PKT_A)
        self.assertTrue(outcomes[0]["complete"])
        self.assertEqual(outcomes[0]["proved_by"], PEER, "path evidence only from the peer it was sent to")

    def test_a_proof_that_arrived_during_the_burst_still_ends_the_window(self):
        iface = self.iface
        queries = []
        sent, sink, restore = self._install_window(lambda h, f, s: None, lambda info: queries.append(info))
        try:
            data, _proof, key = _data_and_proof(iface, payload=self._payload_for(2))
            # Set before the send even starts: _answered_send_event pre-sets
            # from _send_answered_at, so the check catches it at entry.
            iface._signal_send_answered(key, "DIRECT PROOF", PEER)

            async def run():
                return await iface._send_direct_raw_fragmented(
                    TARGET, PEER, data, PKT_A, priority=iface.PRIORITY_NORMAL, hop_count=1)

            result = self.node.run_on_loop(run(), timeout=30.0)
        finally:
            iface._send_answered_at.pop(key, None)
            iface._send_answered_events.pop(key, None)
            restore()
        self.assertIs(result, True)
        self.assertEqual(queries, [])

    def test_an_unprovable_window_still_reports_and_queries(self):
        """A Resource part is context RESOURCE and is never proved per
        packet (a Resource is proved once, whole, as RESOURCE_PRF), so its
        window has no proof key and behaves exactly as in alpha 0.1.7."""
        iface = self.iface
        queries = []
        sent, sink, restore = self._install_window(lambda h, f, s: None,
                                                   lambda info: (queries.append(info), None)[1])
        try:
            payload = self._payload_for(2)
            resource_part = build_rns_packet("resource", dest_hash=os.urandom(16), payload=payload)
            self.assertIsNone(iface._proof_expected_key(resource_part, iface._parse_rns_header(resource_part)),
                              "a Resource part expects no per-packet proof")

            async def run():
                return await iface._send_direct_raw_fragmented(
                    TARGET, PEER, resource_part, PKT_B, priority=iface.PRIORITY_NORMAL, hop_count=1)

            self.node.run_on_loop(run(), timeout=30.0)
        finally:
            restore()
        self.assertTrue(queries, "no proof is coming, so the window still falls back to a QUERY")

    def test_a_proof_for_one_part_of_a_mixed_window_does_not_end_it(self):
        iface = self.iface
        data_a, _p, key_a = _data_and_proof(iface, payload=self._payload_for(2))
        part_b = build_rns_packet("resource", dest_hash=os.urandom(16), payload=self._payload_for(2))
        parts = []

        class _P:
            def __init__(self, pkt_id, proof_key, done=False):
                self.pkt_id, self.proof_key, self.frag_total = pkt_id, proof_key, 2
                self.acked = [False, False]
                self.resume_key = ("x", pkt_id)
                self.last_progress_at = None

                class _F:
                    def __init__(self): self._done = done
                    def done(self): return self._done
                    def set_result(self, v): self._done = True
                self.future = _F()

        iface._signal_send_answered(key_a, "DIRECT PROOF", PEER)
        try:
            parts = [_P(PKT_A, key_a), _P(PKT_B, None)]
            self.assertFalse(iface._window_all_proved(parts, PEER),
                             "the Resource part has no proof key: the window keeps waiting for its report")
            self.assertEqual([p.pkt_id for p in iface._window_proved_parts(parts, PEER)], [PKT_A])

            both = [_P(PKT_A, key_a), _P(PKT_B, key_a)]
            self.assertTrue(iface._window_all_proved(both, PEER))
        finally:
            iface._send_answered_at.pop(key_a, None)
            iface._send_answered_events.pop(key_a, None)


class ReceiverHoldsTheReportForTheProof(SingleNodeCase):
    def _arm(self, iface):
        iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=False,
                                                     last_seen=time.time())
        iface._resolved_paths[PEER] = self.module._ResolvedPath("19", 1, 1, time.monotonic())
        iface._recent_raw_pkts.pop(PEER, None)

    def _header(self, iface, pkt_id=PKT_A, frag_total=2):
        return self.module._FrameHeader(iface.RAW_VERSION if hasattr(iface, "RAW_VERSION") else 5,
                                        True, False, pkt_id, 0, frag_total, 0)

    def test_the_proof_inside_the_grace_drops_the_report(self):
        iface = self.iface
        self._arm(iface)
        sent_reports = []
        saved = (iface._send_completion_report, iface._is_local_destination)
        iface._send_completion_report = lambda *a, **k: sent_reports.append(a)
        iface._is_local_destination = lambda h: True
        try:
            data, _proof, key = _data_and_proof(iface)
            header = self._header(iface)
            # RNS queues the proof: exactly what process_outgoing records.
            iface._note_proof_enqueued(key, time.monotonic())
            self.assertIsNotNone(iface._proof_enqueued_at_for_key(key))

            async def run():
                iface._hold_report_for_proof(PEER, header, key)
                await asyncio.sleep(iface.proof_report_grace_s + 0.2)

            self.node.run_on_loop(run(), timeout=10.0)
            self.assertEqual(sent_reports, [], "the proof says it: no report on the air")
        finally:
            iface._send_completion_report, iface._is_local_destination = saved
            iface._peers.pop(PEER, None)
            iface._resolved_paths.pop(PEER, None)

    def test_the_grace_expiring_sends_the_report_exactly_as_before(self):
        iface = self.iface
        self._arm(iface)
        sent_reports = []
        saved = iface._send_completion_report

        def record(sender_token, header, complete, held, held_s=0.0):
            sent_reports.append({"complete": complete, "held": held, "held_s": held_s})

        iface._send_completion_report = record
        try:
            _data, _proof, key = _data_and_proof(iface)
            header = self._header(iface)

            async def run():
                iface._hold_report_for_proof(PEER, header, key)     # no proof ever queued
                await asyncio.sleep(iface.proof_report_grace_s + 0.3)

            self.node.run_on_loop(run(), timeout=10.0)
            self.assertEqual(len(sent_reports), 1, "no proof came: the report goes as today")
            self.assertTrue(sent_reports[0]["complete"])
            self.assertEqual(sent_reports[0]["held"], {0, 1})
        finally:
            iface._send_completion_report = saved
            iface._peers.pop(PEER, None)
            iface._resolved_paths.pop(PEER, None)

    def test_a_report_going_out_for_another_reason_cancels_the_grace(self):
        iface = self.iface
        self._arm(iface)
        try:
            _data, _proof, key = _data_and_proof(iface)
            header = self._header(iface)

            async def run():
                iface._hold_report_for_proof(PEER, header, key)
                self.assertIn(PEER, iface._pending_proof_graces)
                iface._cancel_proof_grace(PEER)
                await asyncio.sleep(0.05)

            self.node.run_on_loop(run(), timeout=10.0)
            self.assertNotIn(PEER, iface._pending_proof_graces)
        finally:
            iface._peers.pop(PEER, None)
            iface._resolved_paths.pop(PEER, None)


if __name__ == "__main__":
    unittest.main()
