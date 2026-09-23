"""
The one-hop fragment gap knob for the field A/B (alpha 0.1.5, item 4,
2026-09-21). Default unchanged.

MeshBench finding 2 (2026-09-20) made the raw gap through repeaters
`(1 + direct_raw_hop_gap_factor x hops) x airtime`: `send_raw_data` returns
when the frame is queued, so the frame's own airtime had been eaten out of
the gap. At one hop that gap is two thirds of a three-fragment part's time,
and MeshBench cannot judge it: its frames are ~30% slower than the field's
and its radio has no listen-before-talk. `direct_raw_gap_own_airtime = no`
drops the `+1 x airtime` term through repeaters for the field A/B
(`fieldtests/AB_PROTOCOL.md`); zero hop is untouched either way; every
`raw_fragment_sent` record carries the `gap_s` actually used, and
`field_ab_compare.py` reports the A/B's safety signals per hop (round-0
re-sends per fragment position, round-1 fragments per part, parity
reconstructions, the gap used).
"""
import unittest

from tests._support import SingleNodeCase, load_interface_module
from tests.test_completion_report_one_hop_0920 import _OneHopRawSend, _sink, PEER


class GapArithmetic(SingleNodeCase):
    def test_knob_on_and_off(self):
        iface = self.iface
        saved = (iface.direct_raw_gap_own_airtime, iface.direct_raw_hop_gap_factor, iface.direct_raw_zero_hop_gap_s)
        try:
            frag = 172
            airtime = iface._estimate_tx_airtime_s("", on_air_bytes=frag)
            iface.direct_raw_hop_gap_factor = 2.0
            iface.direct_raw_zero_hop_gap_s = 0.15
            iface.direct_raw_gap_own_airtime = True
            self.assertAlmostEqual(iface._raw_fragment_gap_s(1, frag), 3.0 * airtime, places=6)
            self.assertAlmostEqual(iface._raw_fragment_gap_s(2, frag), 5.0 * airtime, places=6)
            self.assertAlmostEqual(iface._raw_fragment_gap_s(0, frag), 0.15, places=6)
            iface.direct_raw_gap_own_airtime = False
            self.assertAlmostEqual(iface._raw_fragment_gap_s(1, frag), 2.0 * airtime, places=6, msg="the +1 airtime term dropped")
            self.assertAlmostEqual(iface._raw_fragment_gap_s(2, frag), 4.0 * airtime, places=6)
            self.assertAlmostEqual(iface._raw_fragment_gap_s(0, frag), 0.15, places=6, msg="zero hop untouched")
        finally:
            iface.direct_raw_gap_own_airtime, iface.direct_raw_hop_gap_factor, iface.direct_raw_zero_hop_gap_s = saved

    def test_shipped_default_keeps_the_own_airtime(self):
        module = load_interface_module()
        bare = module.SmartMeshCoreInterface.__new__(module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertTrue(bare.direct_raw_gap_own_airtime)
        self.assertEqual(bare.direct_raw_hop_gap_factor, 2.0)


class GapIsCaptured(_OneHopRawSend):
    def test_raw_fragment_sent_records_carry_gap_s(self):
        iface = self.iface
        sent, restore = self._install(lambda h, f, s: None, lambda info: None)
        sink, restore_sink = _sink(iface)
        saved = (iface.direct_raw_hop_gap_factor, iface.direct_raw_reconcile_rounds, iface.direct_raw_query_attempts)
        iface.direct_raw_hop_gap_factor = 0.5
        iface.direct_raw_reconcile_rounds, iface.direct_raw_query_attempts = 1, 1
        try:
            self._run_send(601, self._payload_for(2))
            records = sink.records("raw_fragment_sent")
            self.assertEqual(len(records), 2)
            for r in records:
                expected = iface._raw_fragment_gap_s(1, r["on_air_bytes"])
                self.assertAlmostEqual(r["gap_s"], expected, places=3)
                self.assertGreater(r["gap_s"], 0.0)
        finally:
            iface.direct_raw_hop_gap_factor, iface.direct_raw_reconcile_rounds, iface.direct_raw_query_attempts = saved
            restore_sink()
            restore()


if __name__ == "__main__":
    unittest.main()
