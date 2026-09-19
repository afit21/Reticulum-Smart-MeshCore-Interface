"""
Regression tests for the MeshBench real-firmware findings of 2026-09-20
(changelog "MeshBench real-firmware test tier") and the review of the
night-session fixes the same day: the SELF_INFO radio block is bounded, the
raw-fragment gap includes the frame's own airtime, a completion ANSWER waits
out the QUERY's ACK relay, small-mesh DIRECT-to-all skips the broadcast
spacing, and the reconcile quiet window is anchored at the ACK and grows with
the measured round trip.
"""
import unittest

from tests._support import SingleNodeCase

PEER = "abcdef012345"


class RadioBlockSanity(SingleNodeCase):
    def test_plausible_block_is_kept_and_absurd_one_rejected(self):
        parse = self.iface._parse_radio_params
        self.assertEqual(parse({"radio_sf": 7, "radio_bw": 62.5, "radio_cr": 8}), (7, 62.5, 8))
        self.assertEqual(parse({"radio_sf": 12, "radio_bw": 500, "radio_cr": 5}), (12, 500.0, 5))
        # the MeshBench case: bw reported as 0.063 kHz
        self.assertIsNone(parse({"radio_sf": 7, "radio_bw": 0.063, "radio_cr": 8}))
        self.assertIsNone(parse({"radio_sf": 13, "radio_bw": 62.5, "radio_cr": 8}))
        self.assertIsNone(parse({"radio_sf": 7, "radio_bw": 62.5, "radio_cr": 9}))
        self.assertIsNone(parse({"radio_sf": "x", "radio_bw": 62.5, "radio_cr": 8}))
        self.assertIsNone(parse({}))


class RawGapAndAnswerHold(SingleNodeCase):
    def test_answer_hold_scales_with_hops_and_is_zero_at_zero_hop(self):
        iface = self.iface
        ack = iface._estimate_tx_airtime_s("", on_air_bytes=12)
        self.assertEqual(iface._completion_answer_hold_s(0), 0.0)
        self.assertAlmostEqual(iface._completion_answer_hold_s(1), ack * 3.5)
        self.assertAlmostEqual(iface._completion_answer_hold_s(2), ack * 6.0)
        self.assertGreater(iface._completion_answer_hold_s(2), iface._completion_answer_hold_s(1))

    def test_supplement_spacing_only_alongside_a_broadcast(self):
        iface = self.iface
        self.assertEqual(iface._supplement_spacing_s(2, alongside_broadcast=False), 0.0)
        lo, hi = iface._fragment_spacing_range(hop_count=2)
        for _ in range(5):
            self.assertTrue(lo <= iface._supplement_spacing_s(2, alongside_broadcast=True) <= hi)


class QuietWindowAnchoredAtTheAck(SingleNodeCase):
    def test_window_grows_with_the_measured_round_trip(self):
        iface = self.iface
        iface.direct_completion_quiet_base_s = 2.0
        iface.direct_completion_quiet_per_hop_s = 3.0
        iface._query_rtt.pop(PEER, None)
        self.assertAlmostEqual(iface._completion_quiet_window_s(1, 15.0, PEER), 5.0)
        # fewer than three samples: the prior stands
        iface._query_rtt[PEER] = {"srtt": 9.0, "rttvar": 1.0, "samples": 2}
        self.assertAlmostEqual(iface._completion_quiet_window_s(1, 15.0, PEER), 5.0)
        # three or more: srtt + 2 x rttvar when larger, still capped by the budget
        iface._query_rtt[PEER] = {"srtt": 9.0, "rttvar": 1.0, "samples": 3}
        self.assertAlmostEqual(iface._completion_quiet_window_s(1, 15.0, PEER), 11.0)
        self.assertAlmostEqual(iface._completion_quiet_window_s(1, 8.0, PEER), 8.0)
        # a fast peer never shrinks the prior
        iface._query_rtt[PEER] = {"srtt": 1.0, "rttvar": 0.2, "samples": 5}
        self.assertAlmostEqual(iface._completion_quiet_window_s(1, 15.0, PEER), 5.0)
        iface._query_rtt.pop(PEER, None)


if __name__ == "__main__":
    unittest.main()
