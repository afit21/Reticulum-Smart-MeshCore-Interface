"""Alpha 0.1.9, items 1 and 2: a skipped report counts as reported, and the
proof that replaced it waits out the sender's burst tail.

Both correct alpha 0.1.8's item 1, and both come from the 2026-09-23
two-hop stop (`fieldtests/raw/Alpha0.1.8/`).

Item 1. The report was skipped 13 times at that stop and the proof keyed
within a second of completion -- but `_hold_report_for_proof` and the
inline skip at the end of the raw completion path recorded
`completion_report_skipped` without ever stamping
`_last_complete_report_at`. So the parity fragment arriving one fragment
spacing behind the completing data fragment was treated as a fresh trigger
and the complete report went out anyway: four times at that stop,
`completion_report_sent` with `held_s 0.0` at 3.8 to 4.9 s after the skip,
queued behind the proof's 11 s ACK timeout. Those skips saved nothing. The
proof IS the sender's signal in the sense `_report_recently_sent` means, so
it stamps the same way.

Item 2. The proofs that replaced a report did measurably worse on their
first attempt than proofs with no burst behind them: across the session at
two hops, 9 of 17 for the report-skipped population against 6 of 7 for
bare single-fragment packets, whose proofs turned round in 3.8 s median
against the raw window's 10.0 s. The collision partner is the sender's own
burst tail -- `_run_raw_window_rounds` appends a part's parity fragment
after its data fragments, one `_raw_fragment_gap_s` later (4.63 s at two
hops) -- so when the data fragments complete the packet, the proof is in
the relay chain as the parity is transmitted. In the four stop-2 cases
where the rx-log shows a RAW frame 3.8 to 4.9 s after completion, three of
the proofs missed.

The wait is one of the sender's spacings plus the half-airtime margin, i.e.
the existing `_report_hold_s(..., arriving=False)` and its existing
constant. Deliberately NOT the still-arriving hold (two spacings): at two
hops that is 9.55 s against the sender's own report wait of 9.0 s, so it
would expire the sender's window and provoke exactly the QUERY alpha
0.1.8's item 1 exists to remove. And it is a hold on a frame not yet sent,
so alpha 0.1.7's second cut -- a fresh proof must not cut a hold on a frame
already on the air -- is untouched.
"""
import asyncio
import os
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet
from tests.test_report_hold_during_burst_0921 import _ReceiverScaffold, PEER

PKT = 71


def _data_and_key(iface, payload=None):
    data = build_rns_packet("data", dest_hash=os.urandom(16),
                            payload=payload if payload is not None else b"lxmf" + os.urandom(120))
    key = iface._compute_truncated_hash(data, iface._parse_rns_header(data).header_type)
    return data, key


class _ProofScaffold(_ReceiverScaffold):
    """The receive path with the proof gates satisfiable: a bound peer with
    a resolved path, every destination local, RNS's hand-off recorded."""

    def setUp(self):
        super().setUp()
        iface = self.iface
        self.module_saved = (iface._is_local_destination, iface.process_incoming,
                             iface._canonical_peer_prefix)
        self.delivered = []
        iface._is_local_destination = lambda h: True
        iface.process_incoming = lambda data, **kw: self.delivered.append(bytes(data))
        iface._canonical_peer_prefix = lambda token: PEER
        iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=False,
                                                     last_seen=time.time())
        iface._resolved_paths[PEER] = self.module._ResolvedPath("19", 1, 1, time.monotonic())
        iface._last_complete_report_at.clear()
        iface._proof_enqueued_at.clear()
        # Tolerated as absent so this class also runs against an alpha
        # 0.1.8 deliverable (`SMCI_INTERFACE_PATH`), where the behaviour
        # tests below fail on their assertion -- the defect itself.
        getattr(iface, "_proof_tail_hold_until", {}).clear()

    def tearDown(self):
        iface = self.iface
        (iface._is_local_destination, iface.process_incoming,
         iface._canonical_peer_prefix) = self.module_saved
        iface._peers.pop(PEER, None)
        iface._resolved_paths.pop(PEER, None)
        iface._last_complete_report_at.clear()
        iface._proof_enqueued_at.clear()
        getattr(iface, "_proof_tail_hold_until", {}).clear()
        super().tearDown()

    def _record(self, sender_token, header, complete, held, held_s=None):
        # The scaffold records reports instead of transmitting them, so
        # mirror the one side effect of the real `_send_completion_report`
        # that these tests are about: a complete report stamps
        # `_last_complete_report_at`. Items 1 and 2 are precisely about the
        # SKIP sites doing the same, so the comparison has to be fair.
        # Written as the raw table entry rather than through
        # `_note_complete_report_sent`, so this scaffold runs unchanged
        # against an alpha 0.1.8 deliverable and the behaviour tests below
        # FAIL there on their assertion instead of erroring on a missing
        # method -- that is how the defect was demonstrated.
        super()._record(sender_token, header, complete, held, held_s=held_s)
        if complete and header.pkt_id is not None:
            self.iface._last_complete_report_at[(sender_token, header.pkt_id)] = time.monotonic()

    def _deliver_in_two(self, data, pkt_id=PKT, flagged_last=False):
        """The packet as the two raw data fragments of one window."""
        half = len(data) // 2
        h0 = self.module._FrameHeader(self.iface.PROTOCOL_VERSION, True, False, pkt_id, 0, 2, 0)
        h1 = self.module._FrameHeader(self.iface.PROTOCOL_VERSION, True, False, pkt_id, 1, 2, 0)
        self.on_loop(lambda: self.iface._handle_direct_multifragment_frame(
            h0, data[:half], PEER, raw=True, report_requested=False))
        self.on_loop(lambda: self.iface._handle_direct_multifragment_frame(
            h1, data[half:], PEER, raw=True, report_requested=flagged_last))


class TheSkipCountsAsReported(_ProofScaffold):
    def test_the_parity_in_the_burst_tail_does_not_re_trigger_the_report(self):
        iface = self.iface
        data, key = _data_and_key(iface)
        # RNS has already queued the proof, so the inline skip site fires.
        iface._note_proof_enqueued(key, time.monotonic())
        self._deliver_in_two(data)
        self.assertEqual(len(self.delivered), 1, "the packet reached RNS")
        self.assertEqual(self.sent, [], "the proof replaces the report: nothing on the air")

        # The parity fragment of the same burst, flagged, one spacing later:
        # a flagged frame for a packet already delivered. This is the field
        # symptom -- in alpha 0.1.8 it sent a complete report with
        # `held_s 0.0`, four times at the 2026-09-23 two-hop stop, and this
        # assertion fails against that deliverable.
        self._frag(PKT, 1, 2, flagged=True)
        self.assertEqual(self.sent, [], "no report inside the burst tail of the skip")
        self.assertIn((PEER, PKT), iface._last_complete_report_at,
                      "and the reason it is suppressed: the skip stamped")

    def test_a_re_drive_after_the_tail_is_still_reported(self):
        # The suppression is the burst tail, not forever: a sender that
        # never got the proof and re-drives later must still be told.
        iface = self.iface
        data, key = _data_and_key(iface)
        iface._note_proof_enqueued(key, time.monotonic())
        self._deliver_in_two(data)
        self.assertEqual(self.sent, [])
        iface._last_complete_report_at[(PEER, PKT)] -= 60.0
        self._frag(PKT, 1, 2, flagged=True)
        self.assertEqual(len(self.sent), 1, "a late re-drive is told again")
        self.assertTrue(self.sent[0]["complete"])

    def test_the_grace_expiring_still_sends_one_report(self):
        # No proof ever queued: the grace task runs, expires, and reports
        # exactly as alpha 0.1.8 did -- and that report stamps too, so the
        # tail behaves the same either way.
        iface = self.iface
        data, _key = _data_and_key(iface)
        self._deliver_in_two(data)
        self.assertEqual(self.sent, [], "held for the grace, not sent yet")
        time.sleep(iface.proof_report_grace_s + 0.3)
        self.assertEqual(len(self.sent), 1, "the grace expired: one report, as before")
        self.assertIn((PEER, PKT), iface._last_complete_report_at)
        self._frag(PKT, 1, 2, flagged=True)
        self.assertEqual(len(self.sent), 1, "and its own burst tail is suppressed as before")

    def test_the_stamp_has_one_writer(self):
        iface = self.iface
        iface._note_complete_report_sent(PEER, 300)
        self.assertTrue(iface._report_recently_sent(PEER, 300, 10 + iface.RAW_HEADER_SIZE))
        iface._note_complete_report_sent(PEER, None)          # no pkt_id: a no-op, not a crash
        self.assertFalse(iface._report_recently_sent(PEER, None, 10 + iface.RAW_HEADER_SIZE))


class TheProofWaitsForTheBurstTail(SingleNodeCase):
    FRAG = 161 + 9   # a full raw fragment on air

    def _header(self, frag_total=2, pkt_id=PKT):
        return self.module._FrameHeader(self.iface.PROTOCOL_VERSION, True, False, pkt_id, 0, frag_total, 0)

    class _Bucket:
        def __init__(self, parity=None):
            self.parity = parity or {}

    def _hold(self, hops, *, frag_total=2, bucket=None, report_requested=False):
        iface = self.iface
        saved = iface._receiver_hops_to
        iface._receiver_hops_to = lambda token: hops
        try:
            return iface._proof_tail_hold_s(PEER, self._header(frag_total), bucket,
                                            report_requested, self.FRAG)
        finally:
            iface._receiver_hops_to = saved

    def test_a_window_completed_before_its_parity_waits_one_spacing_plus_margin(self):
        iface = self.iface
        for hops in (1, 2, 3):
            expected = iface._report_hold_s(self.FRAG, hops, arriving=False)
            self.assertAlmostEqual(self._hold(hops), expected, places=6,
                                   msg=f"one spacing plus margin at {hops} hop(s)")
            # The whole point: it must stay inside the SENDER's report wait,
            # or the window times out and the QUERY comes back.
            self.assertLess(expected, iface._completion_report_wait_s(hops, PEER),
                            f"the hold must not expire the sender's window at {hops} hop(s)")
            # And it must be shorter than the still-arriving hold, which at
            # two hops (9.55 s) exceeds the sender's 9.0 s wait.
            self.assertLess(expected, iface._report_hold_s(self.FRAG, hops, arriving=True))

    def test_zero_hop_waits_nothing(self):
        # No parity below direct_raw_parity_min_hops, and a zero-hop burst
        # has no relay chain to collide with.
        self.assertEqual(self.iface.direct_raw_parity_min_hops, 1)
        self.assertEqual(self._hold(0, report_requested=True), 0.0)

    def test_a_window_completed_by_the_flagged_last_frame_with_parity_in_does_not_wait(self):
        # The parity already landed (it is in the bucket) and the flagged
        # frame completed the part: nothing of the burst is still due.
        self.assertEqual(self._hold(2, bucket=self._Bucket(parity={3: (10, b"xx")}),
                                    report_requested=True), 0.0)

    def test_an_unflagged_completion_waits_even_with_the_parity_in(self):
        # An unflagged fragment completed the part, so the burst's two
        # flagged frames are still to come -- the same condition that makes
        # the report path schedule instead of sending.
        iface = self.iface
        self.assertAlmostEqual(self._hold(2, bucket=self._Bucket(parity={3: (10, b"xx")}),
                                          report_requested=False),
                               iface._report_hold_s(self.FRAG, 2, arriving=False), places=6)

    def test_a_single_fragment_part_has_no_parity_and_does_not_wait(self):
        # The sender only adds parity to a part with two or more fragments
        # in the round, so a one-fragment part has no tail.
        self.assertEqual(self._hold(2, frag_total=1, report_requested=True), 0.0)

    def test_the_item_is_off_when_item_1_is(self):
        iface = self.iface
        saved = iface.proof_report_grace_s
        iface.proof_report_grace_s = 0.0
        try:
            self.assertEqual(self._hold(2), 0.0, "proof_report_grace = 0 disables both items")
        finally:
            iface.proof_report_grace_s = saved


class TheHoldIsReadOnceByTheOutgoingPath(SingleNodeCase):
    def test_a_bare_packets_proof_never_waits(self):
        # Nothing armed the hold, so the proof goes straight out -- which is
        # why bare packets' proofs already turned round in 3.8 s.
        iface = self.iface
        data = build_rns_packet("data", dest_hash=os.urandom(16), payload=b"x" * 40)
        key = iface._compute_truncated_hash(data, iface._parse_rns_header(data).header_type)
        proof = build_rns_packet("proof", dest_hash=key, payload=os.urandom(64))
        self.assertEqual(iface._proof_tail_hold_remaining(iface._parse_rns_header(proof)), 0.0)

    def test_an_armed_hold_is_returned_once_and_cleared(self):
        iface = self.iface
        data = build_rns_packet("data", dest_hash=os.urandom(16), payload=b"x" * 40)
        key = iface._compute_truncated_hash(data, iface._parse_rns_header(data).header_type)
        proof = build_rns_packet("proof", dest_hash=key, payload=os.urandom(64))
        header = iface._parse_rns_header(proof)
        iface._proof_tail_hold_until[bytes(key)] = time.monotonic() + 5.0
        first = iface._proof_tail_hold_remaining(header)
        self.assertGreater(first, 4.0)
        self.assertLessEqual(first, 5.0)
        self.assertEqual(iface._proof_tail_hold_remaining(header), 0.0,
                         "one proof waits once: a DIRECT-to-all proof must not wait per peer")

    def test_only_a_plain_proof_reads_the_hold(self):
        iface = self.iface
        other = build_rns_packet("data", dest_hash=os.urandom(16), payload=b"x" * 40)
        header = iface._parse_rns_header(other)
        iface._proof_tail_hold_until[bytes(header.destination_hash)] = time.monotonic() + 5.0
        try:
            self.assertEqual(iface._proof_tail_hold_remaining(header), 0.0)
        finally:
            iface._proof_tail_hold_until.clear()


if __name__ == "__main__":
    unittest.main()
