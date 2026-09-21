"""
Radio transmit statistics for estimator calibration (alpha 0.1.5, item 8,
2026-09-21) -- instrumentation only, the estimator is unchanged.

Ground truth: firmware v1.17.1's `CMD_GET_STATS` (56, companion protocol
v8+, `examples/companion_radio/MyMesh.cpp`) with STATS_TYPE_RADIO returns
`tx_air_secs` = `Dispatcher::getTotalAirTime() / 1000` -- the wall-clock
duration of every completed send summed in `Dispatcher::checkSend`
(`total_air_time += millis - outbound_start`) -- and `rx_air_secs`; with
STATS_TYPE_PACKETS the radio driver's sent / received counts and the
flood / direct tx / rx counts. The `meshcore` library (2.3.9.1) exposes them
as `CommandHandler.get_stats_radio()` -> EventType.STATS_RADIO and
`get_stats_packets()` -> EventType.STATS_PACKETS (reader.py parses
`<h b b I I` and `<I I I I I I [I]`).

Pinned:
  * the `radio_stats` record shape: the firmware's numbers beside the
    interface's own `estimated_tx_air_s` / `frames_keyed` since start;
  * a poll on the fake writes the record with the fake's counters, and the
    interface's estimate sum grows with every gated frame;
  * a library without the commands, or a firmware answering ERROR, is
    logged once and never polled again;
  * shipped default `radio_stats_interval` 300 s.
"""
import unittest

from tests._support import SingleNodeCase, load_interface_module
from tests.test_completion_report_one_hop_0920 import _sink


class RecordShape(SingleNodeCase):
    def test_pure_record(self):
        iface = self.iface
        iface._estimated_tx_air_total_s, iface._frames_keyed_total = 12.345, 7
        rec = iface._radio_stats_record("interval", {"tx_air_secs": 40, "rx_air_secs": 300, "noise_floor": -110,
                                                      "last_rssi": -55, "last_snr": 9.5},
                                        {"recv": 100, "sent": 50, "flood_tx": 10, "direct_tx": 40, "flood_rx": 60,
                                         "direct_rx": 40, "recv_errors": 2})
        self.assertEqual(rec["event"], "radio_stats")
        self.assertEqual(rec["reason"], "interval")
        self.assertEqual((rec["tx_air_secs"], rec["rx_air_secs"]), (40, 300))
        self.assertEqual((rec["packets_sent"], rec["flood_tx"], rec["direct_tx"], rec["recv_errors"]), (50, 10, 40, 2))
        self.assertEqual((rec["estimated_tx_air_s"], rec["frames_keyed"]), (12.345, 7))
        self.assertIn("uptime_s", rec)
        empty = iface._radio_stats_record("stop", None, None)
        self.assertIsNone(empty["tx_air_secs"])
        self.assertEqual(empty["frames_keyed"], 7)

    def test_shipped_default(self):
        module = load_interface_module()
        bare = module.SmartMeshCoreInterface.__new__(module.SmartMeshCoreInterface)
        bare._configure_observability({})
        self.assertEqual(bare.radio_stats_interval_s, 300.0)


class PollOnTheFake(SingleNodeCase):
    def test_poll_writes_the_record_and_the_estimate_grows(self):
        iface = self.iface
        sink, restore = _sink(iface)
        try:
            before = iface._frames_keyed_total
            est_before = iface._estimated_tx_air_total_s
            self.node.run_on_loop(iface._pre_transmit_gate("", skip_quiet_defer=True, on_air_bytes=172), timeout=5)
            self.assertEqual(iface._frames_keyed_total, before + 1)
            self.assertAlmostEqual(iface._estimated_tx_air_total_s - est_before,
                                   iface._estimate_tx_airtime_s("", on_air_bytes=172), places=6)
            rec = self.node.run_on_loop(iface._poll_radio_stats("interval"), timeout=10)
            self.assertIsNotNone(rec)
            self.assertFalse(iface._radio_stats_unsupported)
            records = sink.records("radio_stats")
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["reason"], "interval")
            self.assertEqual(records[0]["tx_air_secs"], int(self.node.radio.tx_air_ms // 1000))
            self.assertEqual(records[0]["packets_sent"], self.node.radio.counters.get("packets_sent", 0))
            self.assertEqual(records[0]["frames_keyed"], iface._frames_keyed_total)
        finally:
            restore()

    def test_unsupported_library_is_logged_once_and_not_polled_again(self):
        iface = self.iface
        commands = iface._mc.commands
        saved = commands.get_stats_radio
        try:
            del type(commands).get_stats_radio      # a library without the command
            self.assertFalse(hasattr(commands, "get_stats_radio"))
            iface._radio_stats_unsupported = False
            rec = self.node.run_on_loop(iface._poll_radio_stats("interval"), timeout=10)
            self.assertIsNone(rec)
            self.assertTrue(iface._radio_stats_unsupported)
        finally:
            type(commands).get_stats_radio = saved
            iface._radio_stats_unsupported = False

    def test_firmware_error_reply_stops_the_poll(self):
        iface = self.iface
        commands = iface._mc.commands
        module = self.module
        saved = type(commands).get_stats_radio

        async def error_reply(self_):
            from simmesh.fake_meshcore import SimEvent, EventType
            return SimEvent(EventType.ERROR, {"reason": "unsupported command"})

        try:
            type(commands).get_stats_radio = error_reply
            iface._radio_stats_unsupported = False
            rec = self.node.run_on_loop(iface._poll_radio_stats("start"), timeout=10)
            self.assertIsNone(rec)
            self.assertTrue(iface._radio_stats_unsupported)
        finally:
            type(commands).get_stats_radio = saved
            iface._radio_stats_unsupported = False


if __name__ == "__main__":
    unittest.main()
