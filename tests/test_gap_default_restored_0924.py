"""
`direct_raw_gap_own_airtime` defaults to `yes` again (alpha 0.1.9, second
pass, item 4, 2026-09-24).

The first pass flipped it to `no` (commit 5bbe1a8) without the field A/B in
`fieldtests/AB_PROTOCOL.md`, which has still never run. The only field time
the `no` arm got (the 2026-09-23 22:04-23:07 session) was two raw parts at
one hop and three at two, all while both nodes were stuck on a dead
zero-hop path, so it measured nothing about the gap. MeshBench cannot
judge it either (no listen-before-talk), so the default goes back to the
arm the bench did measure, `(1 + 2 x hops)` airtimes through repeaters, and
the A/B decides. Both arms stay pinned in
`tests/test_raw_gap_own_airtime_0921.py`; this file pins the shipped one and
the two derived values the first pass had shrunk with it.
"""
import unittest

from tests._support import SingleNodeCase, load_interface_module


class TheGapDefaultIsYesAgain(SingleNodeCase):
    def test_configure_retry_default_is_yes(self):
        module = load_interface_module()
        bare = module.SmartMeshCoreInterface.__new__(module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertTrue(bare.direct_raw_gap_own_airtime)
        # An explicit `no` still selects the A/B's other arm with no rebuild.
        bare._configure_retry({"direct_raw_gap_own_airtime": "no"})
        self.assertFalse(bare.direct_raw_gap_own_airtime)

    def test_shipped_gap_keeps_the_frames_own_airtime(self):
        iface = self.iface
        self.assertTrue(iface.direct_raw_gap_own_airtime, "the shipped build, not a fixture")
        saved = (iface.direct_raw_hop_gap_factor, iface.direct_raw_zero_hop_gap_s)
        try:
            # The unit scaffold lowers these for speed; use the shipped values.
            iface.direct_raw_hop_gap_factor, iface.direct_raw_zero_hop_gap_s = 2.0, 0.15
            frag = 172
            airtime = iface._estimate_tx_airtime_s("", on_air_bytes=frag)
            self.assertAlmostEqual(iface._raw_fragment_gap_s(0, frag), 0.15, places=6)
            self.assertAlmostEqual(iface._raw_fragment_gap_s(1, frag), 3.0 * airtime, places=6)
            self.assertAlmostEqual(iface._raw_fragment_gap_s(2, frag), 5.0 * airtime, places=6)
            self.assertAlmostEqual(iface._raw_fragment_gap_s(3, frag), 7.0 * airtime, places=6)
        finally:
            iface.direct_raw_hop_gap_factor, iface.direct_raw_zero_hop_gap_s = saved

    def test_derived_holds_are_back_to_one_full_spacing(self):
        # The proof burst-tail hold (first pass, item 2) is one spacing plus
        # the arriving margin, so it grows back with the gap; it must still
        # sit inside the sender's report wait at every relayed hop count.
        iface = self.iface
        saved = (iface.direct_raw_hop_gap_factor, iface.direct_raw_zero_hop_gap_s)
        try:
            iface.direct_raw_hop_gap_factor, iface.direct_raw_zero_hop_gap_s = 2.0, 0.15
            frag = 172
            airtime = iface._estimate_tx_airtime_s("", on_air_bytes=frag)
            margin = iface.RAW_ARRIVING_HOLD_MARGIN_AIRTIMES * airtime
            for hops in (1, 2, 3):
                hold = iface._report_hold_s(frag, hops)
                self.assertAlmostEqual(hold, (1 + 2.0 * hops) * airtime + margin, places=6)
                self.assertLess(hold, iface._completion_report_wait_s(hops, ""))
        finally:
            iface.direct_raw_hop_gap_factor, iface.direct_raw_zero_hop_gap_s = saved


if __name__ == "__main__":
    unittest.main()
