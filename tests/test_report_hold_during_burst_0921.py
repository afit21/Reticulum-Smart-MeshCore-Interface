"""
The receiver holds per-part reports while a window is still arriving
(alpha 0.1.5, item 2b, 2026-09-21).

Field evidence (`fieldtests/raw/Alpha0.1.4/`, the zero-hop 12-part page,
08:38): the desktop burst window [8, 9, 10, 11, 12] -- 15 raw fragments
queued into the firmware in 2.6 s against ~14 s of air -- and the laptop
reported each part the moment it completed. The report for part 8 reached
the desktop mid-burst and ended its report wait (`report_wait_s` 0.0); the
reports for parts 9 and 10 were transmitted into the desktop's own queued
burst and never heard; every one of the page's four on-air losses sat
within 2 s of one of those reports; parts 9-12 were re-burst although they
had landed. 18 fragments re-sent, 14 of them unnecessary.

Pinned here:
  * `_report_hold_s(bytes, hops, arriving=True)` -- the silence after which
    a receiver concludes a sender's window is over: the sender's start-to-
    start spacing at that hop count (airtime + `direct_raw_zero_hop_gap` at
    zero hop; the hop-scaled gap, which contains the airtime, through
    repeaters) plus half an airtime of margin; `arriving=False` is the M1
    gaps hold, unchanged;
  * a window of four parts arriving back to back produces ONE report -- on
    the flagged last fragment, at once -- and no held report fires after it;
  * a part completed by an UNFLAGGED fragment is reported only after the
    silence, and every further fragment from that sender re-arms the hold;
  * a bucket that has already seen a flagged frame (the burst's tail, e.g. a
    flagged parity that arrived first) reports its completion at once even
    when the completing fragment is unflagged;
  * `direct_report_hold_during_burst = no` restores the immediate report.
"""
import time
import unittest

from tests._support import SingleNodeCase, wait_until

PEER = "abcdef012345"


class ArrivingHoldArithmetic(SingleNodeCase):
    def test_zero_hop_hold_is_spacing_plus_half_an_airtime(self):
        iface = self.iface
        frag = 161 + iface.RAW_HEADER_SIZE
        airtime = iface._estimate_tx_airtime_s("", on_air_bytes=frag)
        self.assertAlmostEqual(iface._report_hold_s(frag, 0), airtime, places=6)   # M1 gaps hold, unchanged
        expected = airtime + max(0.0, iface.direct_raw_zero_hop_gap_s) + iface.RAW_ARRIVING_HOLD_MARGIN_AIRTIMES * airtime
        self.assertAlmostEqual(iface._report_hold_s(frag, 0, arriving=True), expected, places=6)
        self.assertGreater(iface._report_hold_s(frag, 0, arriving=True), iface._report_hold_s(frag, 0))

    def test_relayed_hold_is_the_hop_gap_plus_half_an_airtime(self):
        iface = self.iface
        frag = 161 + iface.RAW_HEADER_SIZE
        airtime = iface._estimate_tx_airtime_s("", on_air_bytes=frag)
        for hops in (1, 2, 3):
            gap = iface._raw_fragment_gap_s(hops, frag)
            self.assertAlmostEqual(iface._report_hold_s(frag, hops), gap, places=6)
            self.assertAlmostEqual(iface._report_hold_s(frag, hops, arriving=True),
                                   gap + iface.RAW_ARRIVING_HOLD_MARGIN_AIRTIMES * airtime, places=6)


class _ReceiverScaffold(SingleNodeCase):
    """Raw fragments handed straight to the receive path; reports recorded
    instead of transmitted."""

    def setUp(self):
        self.iface._recent_raw_pkts.pop(PEER, None)
        self.iface._cancel_sender_report(PEER)
        self.sent = []
        self._orig = self.iface._send_completion_report
        self.iface._send_completion_report = self._record

    def _record(self, sender_token, header, complete, held, held_s=None):
        # Mirror the real method's one side effect on the held report, so
        # "any report supersedes the held one" is exercised here too.
        self.iface._cancel_sender_report(sender_token)
        self.sent.append({"pkt_id": header.pkt_id, "complete": complete, "held": set(held), "held_s": held_s,
                          "t": time.monotonic()})

    def tearDown(self):
        self.iface._send_completion_report = self._orig
        self.iface._cancel_sender_report(PEER)
        for key in [k for k in list(self.iface._reassembly) if k[1] == PEER]:
            self.iface._reassembly.pop(key, None)
        for key in [k for k in list(self.iface._dedup) if k[1] == PEER]:
            self.iface._dedup.pop(key, None)

    def _frag(self, pkt_id, idx, total, flagged):
        h = self.module._FrameHeader(self.iface.PROTOCOL_VERSION, True, False, pkt_id, idx, total, 0)
        self.on_loop(lambda: self.iface._handle_direct_multifragment_frame(
            h, bytes([idx]) * 10, PEER, raw=True, report_requested=flagged))


class OneReportPerWindow(_ReceiverScaffold):
    def test_four_parts_back_to_back_produce_one_report_on_the_flagged_tail(self):
        # Four 2-fragment parts: 8 fragments, the sender flags the last two.
        burst = [(pkt, idx) for pkt in (100, 101, 102, 103) for idx in (0, 1)]
        for n, (pkt, idx) in enumerate(burst):
            self._frag(pkt, idx, 2, flagged=n >= len(burst) - 2)
        self.assertEqual(len(self.sent), 1, f"one report for the window, got {self.sent}")
        self.assertEqual(self.sent[0]["pkt_id"], 103)
        self.assertTrue(self.sent[0]["complete"])
        self.assertIsNone(self.sent[0]["held_s"], "the flagged last fragment reports at once, not from a hold")
        # Nothing held for this sender remains, and nothing fires later.
        self.assertNotIn(PEER, self.iface._pending_sender_reports)
        hold = self.iface._report_hold_s(10 + self.iface.RAW_HEADER_SIZE, 0, arriving=True)
        time.sleep(hold + 0.3)
        self.assertEqual(len(self.sent), 1)

    def test_unflagged_completion_is_held_and_rearmed_by_further_fragments(self):
        hold = self.iface._report_hold_s(10 + self.iface.RAW_HEADER_SIZE, 0, arriving=True)
        self._frag(110, 0, 2, False)
        self._frag(110, 1, 2, False)          # part 110 complete on an unflagged fragment
        self.assertEqual(self.sent, [])
        self.assertIn(PEER, self.iface._pending_sender_reports)
        armed_at = time.monotonic()
        # A further fragment from the same sender before the hold expires
        # pushes the deadline out again.
        time.sleep(hold * 0.6)
        self._frag(111, 0, 2, False)
        rearmed_at = time.monotonic()
        self.assertEqual(self.sent, [], "still arriving: no report yet")
        self.assertTrue(wait_until(lambda: len(self.sent) == 1, hold + 2.0), "the held report goes out after the silence")
        self.assertGreaterEqual(self.sent[0]["t"] - rearmed_at, hold - 0.1, "measured from the LAST fragment")
        self.assertGreaterEqual(self.sent[0]["t"] - armed_at, hold * 0.6 + hold - 0.1)
        self.assertEqual(self.sent[0]["pkt_id"], 110)
        self.assertTrue(self.sent[0]["complete"])
        self.assertAlmostEqual(self.sent[0]["held_s"], hold, places=3)

    def test_a_flagged_tail_seen_earlier_reports_the_completion_at_once(self):
        # The flagged fragment of a 3-fragment part arrives first (the
        # burst's tail is here); the unflagged fragments complete it later.
        self._frag(120, 2, 3, True)
        self.assertEqual(self.sent, [], "a flagged fragment with gaps arms the M1 gaps hold, not a report")
        self._frag(120, 0, 3, False)
        self._frag(120, 1, 3, False)
        self.assertEqual(len(self.sent), 1, "completion after the tail reports immediately")
        self.assertTrue(self.sent[0]["complete"])
        self.assertIsNone(self.sent[0]["held_s"])

    def test_knob_off_reports_every_completion_at_once(self):
        saved = self.iface.direct_report_hold_during_burst
        self.iface.direct_report_hold_during_burst = False
        try:
            self._frag(130, 0, 2, False)
            self._frag(130, 1, 2, False)
            self.assertEqual(len(self.sent), 1)
            self.assertTrue(self.sent[0]["complete"])
            self.assertNotIn(PEER, self.iface._pending_sender_reports)
        finally:
            self.iface.direct_report_hold_during_burst = saved


class ShippedDefault(unittest.TestCase):
    def test_hold_during_burst_is_on(self):
        from tests._support import load_interface_module
        module = load_interface_module()
        bare = module.SmartMeshCoreInterface.__new__(module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertTrue(bare.direct_report_hold_during_burst)
        self.assertEqual(module.SmartMeshCoreInterface.RAW_ARRIVING_HOLD_MARGIN_AIRTIMES, 0.5)


if __name__ == "__main__":
    unittest.main()
