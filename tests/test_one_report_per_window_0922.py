"""
Alpha 0.1.6, item 3: one report per window at zero hop (2026-09-22).

The 2026-09-21 session's receiver reports (`fieldtests/raw/Alpha0.1.5/`):
6 of 15 zero-hop reports were a gaps report and then the complete report
0.01 s apart -- the completing fragment landed just after the one-airtime
hold (0.91 s) expired; at two hops the same pair 0.1-1.55 s past a 1.93 s
hold; and seven packets were reported complete TWICE, 0.85-5.4 s apart, the
second on the flagged parity fragment arriving behind the completing data
fragment.

Pinned:
  * the gaps hold is one sender spacing (airtime + zero-hop gap; the hop
    gap through repeaters) plus half an airtime -- the M1 debounce with the
    field's margin;
  * a flagged frame (data or parity) for a packet already delivered within
    the burst tail of a complete report just sent is not reported again; a
    later one (a re-drive) is;
  * `held_s` is on every report record (0.0 for an immediate one).
"""
import time
import unittest

from tests._support import SingleNodeCase
from tests.test_report_hold_during_burst_0921 import _ReceiverScaffold, PEER


class GapsHoldRule(SingleNodeCase):
    def test_gaps_hold_is_one_spacing_plus_the_margin(self):
        iface = self.iface
        frag = 161 + iface.RAW_HEADER_SIZE
        airtime = iface._estimate_tx_airtime_s("", on_air_bytes=frag)
        margin = iface.RAW_ARRIVING_HOLD_MARGIN_AIRTIMES * airtime
        self.assertEqual(iface.RAW_GAPS_HOLD_SPACINGS, 1.0)
        zero = airtime + max(0.0, iface.direct_raw_zero_hop_gap_s)
        self.assertAlmostEqual(iface._report_hold_s(frag, 0), zero + margin, places=6)
        for hops in (1, 2, 3):
            self.assertAlmostEqual(iface._report_hold_s(frag, hops), iface._raw_fragment_gap_s(hops, frag) + margin, places=6)
        # the arriving hold spans two spacings, the gaps hold one
        self.assertLess(iface._report_hold_s(frag, 0), iface._report_hold_s(frag, 0, arriving=True))

    def test_the_field_lags_fall_inside_the_new_hold(self):
        """The 2026-09-21 completing fragments arrived this long after the
        OLD hold expired; the new hold covers each (SF7/BW62.5 frames of
        ~0.9 s: the hold is expressed relative to the airtime here)."""
        iface = self.iface
        frag = 161 + iface.RAW_HEADER_SIZE
        airtime = iface._estimate_tx_airtime_s("", on_air_bytes=frag)
        old_zero_hop = airtime
        self.assertGreater(iface._report_hold_s(frag, 0), old_zero_hop + 0.01 / 0.91 * airtime)
        # At two hops the field's receiver held at ITS hop count (one hop,
        # 1.93 s) against a sender spacing its fragments for two (4.6 s): the
        # hold now follows the larger of the two (`_receiver_hops_to`).
        self.assertGreater(iface._report_hold_s(frag, 2), iface._report_hold_s(frag, 1) + 1.55 / 0.91 * airtime)

    def test_receiver_hold_follows_the_senders_reported_path_length(self):
        iface = self.iface
        iface._path_boards.pop(PEER, None)
        saved = iface._resolved_paths.get(PEER)
        canonical = iface._canonical_peer_prefix
        iface._canonical_peer_prefix = lambda token: PEER   # no device contact in this sandbox
        try:
            iface._resolved_paths[PEER] = self.module._ResolvedPath("aa", 1, 1, time.monotonic())
            self.assertEqual(iface._receiver_hops_to(PEER), 1)
            iface._note_peer_reported_path(PEER, 2, 0.8)
            self.assertEqual(iface._receiver_hops_to(PEER), 2, "the sender's two-hop path sets the hold")
            iface._note_peer_reported_path(PEER, 0, 0.9)
            self.assertEqual(iface._receiver_hops_to(PEER), 1, "never below the receiver's own")
        finally:
            iface._path_boards.pop(PEER, None)
            iface._canonical_peer_prefix = canonical
            if saved is None:
                iface._resolved_paths.pop(PEER, None)
            else:
                iface._resolved_paths[PEER] = saved


class NoSecondReportInTheBurstTail(_ReceiverScaffold):
    def _record(self, sender_token, header, complete, held, held_s=None):
        super()._record(sender_token, header, complete, held, held_s=held_s)
        if complete:
            self.iface._last_complete_report_at[(sender_token, header.pkt_id)] = time.monotonic()

    def setUp(self):
        super().setUp()
        self.iface._last_complete_report_at.clear()

    def test_flagged_duplicate_right_after_the_complete_report_is_not_reported_again(self):
        self._frag(130, 0, 2, False)
        self._frag(130, 1, 2, True)            # completes on the flagged last fragment: one report
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.sent[0]["held_s"], 0.0)
        self._frag(130, 1, 2, True)            # the same flagged frame again (a relayed duplicate, the parity's slot)
        self.assertEqual(len(self.sent), 1, "no second report inside the burst tail")

    def test_flagged_duplicate_after_the_tail_is_a_re_drive_and_reported(self):
        self._frag(131, 0, 2, False)
        self._frag(131, 1, 2, True)
        self.assertEqual(len(self.sent), 1)
        # push the last report into the past: a re-drive after the sender's report wait
        self.iface._last_complete_report_at[(PEER, 131)] -= 60.0
        self._frag(131, 1, 2, True)
        self.assertEqual(len(self.sent), 2, "a late re-drive is told again")

    def test_recently_sent_rule(self):
        iface = self.iface
        frag = 10 + iface.RAW_HEADER_SIZE
        self.assertFalse(iface._report_recently_sent(PEER, 140, frag))
        iface._last_complete_report_at[(PEER, 140)] = time.monotonic()
        self.assertTrue(iface._report_recently_sent(PEER, 140, frag))
        iface._last_complete_report_at[(PEER, 140)] -= iface._report_hold_s(frag, 0, arriving=True) + 0.1
        self.assertFalse(iface._report_recently_sent(PEER, 140, frag))
        self.assertFalse(iface._report_recently_sent(PEER, None, frag))

    def test_gaps_report_is_dropped_when_the_completing_fragment_lands_inside_the_hold(self):
        iface = self.iface
        saved = iface.direct_report_debounce
        iface.direct_report_debounce = True
        try:
            hold = iface._report_hold_s(10 + iface.RAW_HEADER_SIZE, 0)
            self._frag(150, 0, 3, False)
            self._frag(150, 1, 3, True)        # flagged second-last: gaps -> held
            self.assertEqual(self.sent, [])
            time.sleep(min(hold * 0.5, 0.4))
            self._frag(150, 2, 3, True)        # completes inside the hold
            self.assertEqual(len(self.sent), 1)
            self.assertTrue(self.sent[0]["complete"])
            time.sleep(hold + 0.2)
            self.assertEqual(len(self.sent), 1, "the held gaps report never went")
        finally:
            iface.direct_report_debounce = saved


if __name__ == "__main__":
    unittest.main()
