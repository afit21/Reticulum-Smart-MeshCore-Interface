"""
Alpha 0.1.6, item 5: the estimator calibration line corrected for the
radio's own frames (`testscripts/field_ab_compare.py`, 2026-09-22).

The 2026-09-21 session's laptop read `estimate / firmware tx air` = 0.56
against the desktop's 0.93. Two causes, both in the summariser: the
firmware's `tx_air_secs` includes the ACKs the radio sends for every
ACK-able frame it receives (about 200 in the laptop's 32 minutes), which
the interface never estimates; and the laptop's session was four capture
files (interface restarts) while the calibration spanned first to last
record across them -- the interface's counters restart with the process,
the firmware's run on. Per file and corrected: laptop 0.98, desktop 1.00.

Pinned: the LoRa airtime model matches the interface's; the corrected
ratio prices (radio frames sent - frames keyed) at an ACK's airtime and
says which ratio is which; several files sum; missing packet counters
leave only the raw ratio.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "testscripts"))
import field_ab_compare as fab  # noqa: E402

from tests._support import SingleNodeCase  # noqa: E402


class AirtimeModel(SingleNodeCase):
    def test_matches_the_interface_model(self):
        iface = self.iface
        saved = iface._radio_params
        iface._radio_params = (7, 62.5, 8)
        try:
            for n in (8, 40, 172):
                self.assertAlmostEqual(fab.lora_airtime_s(n, 7, 62.5, 8), iface._estimate_airtime_s(n), places=9)
        finally:
            iface._radio_params = saved
        self.assertAlmostEqual(fab.lora_airtime_s(fab.ACK_ON_AIR_BYTES, 7, 62.5, 8), 0.14, places=2)


class Calibration(unittest.TestCase):
    def test_raw_and_corrected(self):
        first = {"tx_air_secs": 100, "estimated_tx_air_s": 10.0, "frames_keyed": 10, "packets_sent": 100}
        last = {"tx_air_secs": 400, "estimated_tx_air_s": 262.0, "frames_keyed": 262, "packets_sent": 452}
        c = fab.calibration(first, last, radio=(7, 62.5, 8))
        self.assertEqual(c["firmware_tx_air_s"], 300)
        self.assertEqual(c["frames"], 252)
        self.assertAlmostEqual(c["estimate_over_firmware"], 252 / 300, places=3)
        self.assertEqual(c["radio_frames_sent"], 352)
        self.assertEqual(c["firmware_only_frames"], 100, "the radio's own ACKs, PATH returns and adverts")
        ack = fab.lora_airtime_s(fab.ACK_ON_AIR_BYTES, 7, 62.5, 8)
        self.assertAlmostEqual(c["corrected_firmware_tx_air_s"], round(300 - 100 * ack, 1), places=1)
        self.assertAlmostEqual(c["estimate_over_corrected"], round(252 / (300 - 100 * ack), 3), places=3)
        self.assertGreater(c["estimate_over_corrected"], c["estimate_over_firmware"])

    def test_flood_and_direct_counters_stand_in_for_packets_sent(self):
        first = {"tx_air_secs": 0, "estimated_tx_air_s": 0.0, "frames_keyed": 0, "flood_tx": 5, "direct_tx": 20}
        last = {"tx_air_secs": 50, "estimated_tx_air_s": 40.0, "frames_keyed": 40, "flood_tx": 10, "direct_tx": 75}
        c = fab.calibration(first, last)
        self.assertEqual(c["radio_frames_sent"], 60)
        self.assertEqual(c["firmware_only_frames"], 20)

    def test_without_packet_counters_only_the_raw_ratio(self):
        c = fab.calibration({"tx_air_secs": 0, "estimated_tx_air_s": 0.0, "frames_keyed": 0},
                            {"tx_air_secs": 10, "estimated_tx_air_s": 9.0, "frames_keyed": 9})
        self.assertAlmostEqual(c["estimate_over_firmware"], 0.9)
        self.assertNotIn("estimate_over_corrected", c)

    def test_more_keyed_than_sent_is_not_negative(self):
        c = fab.calibration({"tx_air_secs": 0, "estimated_tx_air_s": 0.0, "frames_keyed": 0, "packets_sent": 0},
                            {"tx_air_secs": 10, "estimated_tx_air_s": 9.0, "frames_keyed": 12, "packets_sent": 10})
        self.assertEqual(c["firmware_only_frames"], 0)
        self.assertEqual(c["corrected_firmware_tx_air_s"], 10)

    def test_several_files_sum(self):
        a = fab.calibration({"tx_air_secs": 0, "estimated_tx_air_s": 0.0, "frames_keyed": 0, "packets_sent": 0},
                            {"tx_air_secs": 100, "estimated_tx_air_s": 80.0, "frames_keyed": 80, "packets_sent": 100})
        b = fab.calibration({"tx_air_secs": 100, "estimated_tx_air_s": 0.0, "frames_keyed": 0, "packets_sent": 100},
                            {"tx_air_secs": 300, "estimated_tx_air_s": 160.0, "frames_keyed": 160, "packets_sent": 300})
        total = fab.sum_calibrations([a, b])
        self.assertEqual(total["firmware_tx_air_s"], 300)
        self.assertEqual(total["estimated_tx_air_s"], 240.0)
        self.assertEqual(total["records"], 2)
        self.assertEqual(total["firmware_only_frames"], 60)
        self.assertAlmostEqual(total["estimate_over_firmware"], 0.8)
        self.assertGreater(total["estimate_over_corrected"], 0.8)


if __name__ == "__main__":
    unittest.main()
