"""
Own-transmit busy accounting and the burst end it gives a raw window
(alpha 0.1.5, item 2a, 2026-09-21).

Field evidence (2026-09-21 zero-hop session, the desktop's window of parts
[8..12]): fifteen raw fragments were queued into the firmware in 2.6 s
against ~14 s of estimated air. The firmware's CMD_SEND_RAW_DATA answers OK
when the frame is QUEUED, not sent (MyMesh.cpp: `sendDirect(...);
writeOKFrame();`), and the companion's packet pool is 16 entries shared
with reception (StaticPoolPacketManager), so the pre-0.1.5 burst loop --
which slept only the flat zero-hop gap between `send_raw_data` calls --
handed the radio a whole window in seconds, then treated the burst as over
while the radio had barely started on it: a receiver's report for part 8
landing mid-burst ended the completion wait with `report_wait_s 0.0`, and
the report-latency estimator was fed a burst end some 11 s of air
too early. The radio log's `since_own_tx_s` read the radio as idle for 9 s
while ten queued fragments were still transmitting.

What 2a added, pinned here one conclusion per test:

  * `_radio_busy_until` / `_note_radio_keyed` / `_radio_busy_remaining_s`
    (_observability.py): every frame handed to the firmware pushes the
    radio's estimated busy-until out by its airtime from the later of now
    and the previous busy-until;
  * `_pre_transmit_gate` (_direct.py) does that stamping for every
    radio-keying path and reports the value in its telemetry;
  * the `rx_log` capture record's `since_own_tx_s` is measured from the
    estimated END of this node's own air, so it goes negative while a
    queued burst is still on air;
  * `_raw_burst_next_send_wait_s` (_reconcile.py, pure): at zero hop the
    burst loop waits until at most `direct_raw_burst_queue_ahead` frames of
    air are ahead of the next one (shipped 1: back-to-back air, one frame
    queued); through repeaters the hop-scaled gap alone, as before;
  * the burst ends at `max(now, _radio_busy_until)` after the last queued
    fragment: `_expect_report` registers that, `_await_completion_report`
    measures its window from it, and `_record_report_latency` refuses a
    report that lands before it (it measures the airtime estimate, not
    the report path).

One thing the burst test pins that a first reading of "the loop paces
frames one airtime apart" gets wrong: with `queue_ahead = 1` the SECOND
frame follows the first after only the gap (the radio is on the first, one
may be queued), it is the THIRD that waits for the first to be off the air,
and when the last frame is queued one frame is still on air ahead of it, so
the burst end is TWO airtimes after the last `send_raw_data`, not one.
"""
import json
import time
import types
import unittest

from tests._support import SingleNodeCase

PEER = "abcdef012345"
TARGET = "ab" * 32           # the peer's full pubkey hex, as _send_direct_raw_fragmented takes it
ZERO_HOP_PATH_HEX = ""       # no source route: zero hop
ON_AIR_172 = 172             # a full raw fragment at the shipped budget: 9 header + 161 payload + 2


class _CaptureSink:
    """Stand-in for the capture file: `_capture_event` only checks that
    `_packet_capture_file` is not None and calls `.write()` on it."""

    def __init__(self):
        self.lines = []

    def write(self, line):
        self.lines.append(line)

    def records(self, event=None):
        out = [json.loads(line) for line in self.lines]
        return [r for r in out if event is None or r.get("event") == event]


def _sink(iface):
    """Install a `_CaptureSink` as the capture file; returns (sink, restore)."""
    sink = _CaptureSink()
    original = iface._packet_capture_file
    iface._packet_capture_file = sink

    def restore():
        iface._packet_capture_file = original
    return sink, restore


class _RadioBusyCase(SingleNodeCase):
    """Every test starts with the radio idle and no own transmission on
    record, whatever the previous test left behind on the shared node."""

    def setUp(self):
        self.iface._radio_busy_until = 0.0
        self.iface._last_own_tx_at = None

    def tearDown(self):
        self.iface._radio_busy_until = 0.0
        self.iface._last_own_tx_at = None


class NoteRadioKeyedAccumulatesQueuedAir(_RadioBusyCase):
    def test_five_frames_queued_at_once_are_busy_for_five_airtimes(self):
        """N frames handed to the firmware at the same instant (what the
        pre-0.1.5 loop effectively did) leave the radio busy until now +
        N x airtime: each call extends from the previous busy-until, not
        from now."""
        iface = self.iface
        now, air = 1000.0, 0.27
        for n in range(1, 6):
            got = iface._note_radio_keyed(air, now)
            self.assertAlmostEqual(got, now + n * air, places=9)
        self.assertAlmostEqual(iface._radio_busy_until, now + 5 * air, places=9)
        self.assertAlmostEqual(iface._radio_busy_remaining_s(now), 5 * air, places=9)

    def test_a_frame_after_the_radio_went_idle_starts_from_now(self):
        """A stale busy-until in the past does not shorten the new frame's
        air: the radio is busy from now for one airtime."""
        iface = self.iface
        iface._note_radio_keyed(0.27, 1000.0)          # busy until 1000.27
        later = 1010.0                                  # long idle
        got = iface._note_radio_keyed(0.5, later)
        self.assertAlmostEqual(got, later + 0.5, places=9)
        self.assertAlmostEqual(iface._radio_busy_until, later + 0.5, places=9)

    def test_remaining_is_zero_when_idle_and_positive_when_busy(self):
        iface = self.iface
        self.assertEqual(iface._radio_busy_remaining_s(1000.0), 0.0)
        iface._note_radio_keyed(0.4, 1000.0)
        self.assertAlmostEqual(iface._radio_busy_remaining_s(1000.1), 0.3, places=9)
        self.assertEqual(iface._radio_busy_remaining_s(1000.4), 0.0)
        self.assertEqual(iface._radio_busy_remaining_s(1005.0), 0.0, "never negative once the air is over")
        # the default clock is monotonic: a fresh note is busy right now
        iface._note_radio_keyed(0.5)
        self.assertGreater(iface._radio_busy_remaining_s(), 0.3)


class BurstPacingWaitIsPure(_RadioBusyCase):
    """`_raw_burst_next_send_wait_s(hops, gap_s, airtime_s, now, busy_until,
    queue_ahead)`: the sleep between two `send_raw_data` calls."""

    def test_through_a_repeater_the_hop_scaled_gap_is_the_answer(self):
        """At one hop the gap already exceeds the airtime, and the radio's
        queue is not consulted -- however far ahead busy-until lies."""
        iface = self.iface
        self.assertAlmostEqual(iface._raw_burst_next_send_wait_s(1, 0.8, 0.3, 100.0, 100.0, queue_ahead=1), 0.8)
        self.assertAlmostEqual(iface._raw_burst_next_send_wait_s(1, 0.8, 0.3, 100.0, 150.0, queue_ahead=1), 0.8)
        self.assertAlmostEqual(iface._raw_burst_next_send_wait_s(2, 1.6, 0.3, 100.0, 150.0, queue_ahead=1), 1.6)

    def test_zero_hop_waits_for_the_radio_to_have_one_frame_ahead(self):
        """queue_ahead=1, radio busy far ahead: wait until exactly one
        frame of air remains -- `busy_until - airtime - now` -- which
        exceeds the flat gap."""
        iface = self.iface
        now, air, gap, busy = 100.0, 0.3, 0.15, 103.0
        self.assertAlmostEqual(iface._raw_burst_next_send_wait_s(0, gap, air, now, busy, queue_ahead=1), busy - air - now)
        self.assertGreater(busy - air - now, gap)

    def test_zero_hop_with_the_radio_idle_is_just_the_gap(self):
        iface = self.iface
        self.assertAlmostEqual(iface._raw_burst_next_send_wait_s(0, 0.15, 0.3, 100.0, 0.0, queue_ahead=1), 0.15)
        self.assertAlmostEqual(iface._raw_burst_next_send_wait_s(0, 0.15, 0.3, 100.0, 99.0, queue_ahead=1), 0.15)
        # one frame just queued on an idle radio: one frame of air ahead is allowed, so the gap again
        self.assertAlmostEqual(iface._raw_burst_next_send_wait_s(0, 0.15, 0.3, 100.0, 100.3, queue_ahead=1), 0.15)

    def test_queue_ahead_zero_disables_the_pacing(self):
        """0 = off (the pre-0.1.5 loop): the gap regardless of the queue."""
        iface = self.iface
        self.assertAlmostEqual(iface._raw_burst_next_send_wait_s(0, 0.15, 0.3, 100.0, 110.0, queue_ahead=0), 0.15)
        self.assertAlmostEqual(iface._raw_burst_next_send_wait_s(0, 0.0, 0.3, 100.0, 110.0, queue_ahead=0), 0.0)

    def test_queue_ahead_two_allows_two_frames_of_air_ahead(self):
        iface = self.iface
        now, air, gap, busy = 100.0, 0.3, 0.15, 103.0
        self.assertAlmostEqual(iface._raw_burst_next_send_wait_s(0, gap, air, now, busy, queue_ahead=2), busy - 2 * air - now)

    def test_never_negative(self):
        iface = self.iface
        self.assertEqual(iface._raw_burst_next_send_wait_s(0, -1.0, 0.3, 100.0, 0.0, queue_ahead=1), 0.0)
        self.assertEqual(iface._raw_burst_next_send_wait_s(1, -1.0, 0.3, 100.0, 500.0, queue_ahead=1), 0.0)
        self.assertEqual(iface._raw_burst_next_send_wait_s(0, 0.0, 0.3, 100.0, 100.1, queue_ahead=1), 0.0)
        self.assertGreaterEqual(iface._raw_burst_next_send_wait_s(0, 0.0, 0.3, 100.0, 90.0, queue_ahead=3), 0.0)

    def test_queue_ahead_defaults_to_the_configured_value(self):
        """Without `queue_ahead` the interface's `direct_raw_burst_queue_ahead`
        applies (here the live node's value, 1)."""
        iface = self.iface
        saved = iface.direct_raw_burst_queue_ahead
        now, air, gap, busy = 100.0, 0.3, 0.15, 103.0
        try:
            iface.direct_raw_burst_queue_ahead = 1
            self.assertAlmostEqual(iface._raw_burst_next_send_wait_s(0, gap, air, now, busy), busy - air - now)
            iface.direct_raw_burst_queue_ahead = 0
            self.assertAlmostEqual(iface._raw_burst_next_send_wait_s(0, gap, air, now, busy), gap)
        finally:
            iface.direct_raw_burst_queue_ahead = saved


class PreTransmitGateStampsTheBusyClock(_RadioBusyCase):
    def test_two_back_to_back_gates_leave_the_radio_busy_for_two_airtimes(self):
        """The gate is the last common point every radio-keying path passes
        through: each call adds its frame's airtime on top of whatever is
        already queued, reports the new busy-until in `telemetry`, and
        `_radio_busy_remaining_s` reads two airtimes right after two calls.
        (Fresh interface in the fast profile: no duty-cycle wait, quiet
        defer skipped, medium holds off by default.)"""
        iface = self.iface
        self.assertFalse(iface.rx_log_holds_enabled, "the medium hold must not delay the gate here")
        a = iface._estimate_tx_airtime_s("", on_air_bytes=ON_AIR_172)
        self.assertGreater(a, 0.1)
        t1, t2 = {}, {}

        async def two_gates():
            await iface._pre_transmit_gate("", skip_quiet_defer=True, on_air_bytes=ON_AIR_172, telemetry=t1)
            await iface._pre_transmit_gate("", skip_quiet_defer=True, on_air_bytes=ON_AIR_172, telemetry=t2)
            return time.monotonic(), iface._radio_busy_remaining_s()

        after, remaining = self.node.run_on_loop(two_gates(), timeout=5.0)
        self.assertIn("radio_busy_until", t1)
        self.assertAlmostEqual(t2["radio_busy_until"], t1["radio_busy_until"] + a, delta=0.05)
        self.assertAlmostEqual(remaining, 2 * a, delta=0.1)
        self.assertEqual(iface._radio_busy_until, t2["radio_busy_until"])
        self.assertIsNotNone(iface._last_own_tx_at)
        self.assertLessEqual(iface._last_own_tx_at, after)
        self.assertAlmostEqual(t1["radio_busy_until"] - iface._last_own_tx_at, a, delta=0.05,
                               msg="the first frame was queued on an idle radio: busy for one airtime from the stamp")


class RxLogSinceOwnTxIsMeasuredFromTheEndOfOwnAir(_RadioBusyCase):
    def test_a_packet_heard_while_our_frame_is_still_on_air_has_a_negative_since_own_tx(self):
        """One frame just queued (one gate call), then the radio log
        reports a packet overheard: `since_own_tx_s` is now minus the
        estimated end of our own air -- negative, equal to minus the busy
        remaining -- where the pre-0.1.5 record (now minus the send stamp)
        would have read a small positive "idle" figure."""
        iface = self.iface
        sink, restore_sink = _sink(iface)
        event = types.SimpleNamespace(payload={
            "route_type": 1, "route_typename": "FLOOD", "payload_type": 4, "payload_typename": "ADVERT",
            "payload_ver": 0, "path_len": 0, "path": "", "payload_length": 40, "pkt_payload": b"",
            "pkt_hash": 1, "snr": 5.0, "rssi": -60,
        })
        try:
            async def scenario():
                await iface._pre_transmit_gate("", skip_quiet_defer=True, on_air_bytes=ON_AIR_172)
                remaining = iface._radio_busy_remaining_s()
                iface._on_rx_log_data(event)
                return remaining

            remaining = self.node.run_on_loop(scenario(), timeout=5.0)
            self.assertGreater(remaining, 0.1, "the frame is still estimated on air when the log arrives")
            recs = sink.records("rx_log")
            self.assertEqual(len(recs), 1)
            rec = recs[0]
            self.assertEqual(rec["payload_typename"], "ADVERT")
            self.assertIsNotNone(rec["since_own_tx_s"])
            self.assertLess(rec["since_own_tx_s"], 0.0, "our own queued frame is still on air")
            self.assertAlmostEqual(rec["since_own_tx_s"], -remaining, delta=0.02)
        finally:
            restore_sink()

    def test_no_own_transmission_yet_means_none(self):
        iface = self.iface
        sink, restore_sink = _sink(iface)
        event = types.SimpleNamespace(payload={
            "route_type": 1, "route_typename": "FLOOD", "payload_type": 4, "payload_typename": "ADVERT",
            "payload_ver": 0, "path_len": 0, "path": "", "payload_length": 40, "pkt_payload": b"",
            "pkt_hash": 2, "snr": 5.0, "rssi": -60,
        })
        try:
            self.on_loop(lambda: iface._on_rx_log_data(event))
            recs = sink.records("rx_log")
            self.assertEqual(len(recs), 1)
            self.assertIsNone(recs[0]["since_own_tx_s"])
        finally:
            restore_sink()


class BurstEndAnchorsTheReportWait(_RadioBusyCase):
    """The item's headline, driven through `_send_direct_raw_fragmented` at
    zero hop with the radio replaced by a hook that models the firmware
    queue the way the real `_send_raw_fragment`'s gate does (one
    `_note_radio_keyed` per frame, the frame's own estimated airtime)."""

    def _install(self):
        iface = self.iface
        saved = {k: getattr(iface, k) for k in (
            "_send_raw_fragment", "_query_remote_fragments", "_canonical_peer_prefix", "_expect_report",
            "direct_raw_report_wait_base_s", "direct_raw_report_wait_per_hop_s", "direct_raw_zero_hop_gap_s",
            "direct_raw_report_enabled", "direct_raw_reconcile_rounds", "direct_raw_query_attempts",
            "direct_raw_parity_enabled", "direct_raw_burst_queue_ahead", "direct_raw_window_collect_s",
        )}
        saved_path = iface._resolved_paths.get(PEER)
        sent, queries, expected = [], [], []

        async def fake_send_raw_fragment(path, frame, priority, telemetry=None, interrupt=None):
            header, payload, src, dst = iface._decode_raw_fragment(frame)
            on_air = 2 + len(path) + len(frame)
            # what `_pre_transmit_gate` does for the real send: the frame is queued, the radio is busy for its air
            busy_until = iface._note_radio_keyed(iface._estimate_tx_airtime_s("", on_air_bytes=on_air))
            sent.append({"t": time.monotonic(), "frag_idx": header.frag_idx, "round": header.attempt,
                         "on_air": on_air, "busy_until": busy_until})
            return True

        async def fake_query(target, peer_prefix, pkt_id, frag_total, stage, priority=0, hop_count=None, send_info=None, entries=None):
            queries.append({"t": time.monotonic(), "stage": stage, "hop_count": hop_count, "pkt_id": pkt_id})
            return None

        real_expect = saved["_expect_report"]

        def spy_expect_report(peer_prefix, pkt_id, burst_end):
            expected.append((peer_prefix, pkt_id, burst_end))
            return real_expect(peer_prefix, pkt_id, burst_end)

        iface._send_raw_fragment = fake_send_raw_fragment
        iface._query_remote_fragments = fake_query
        iface._canonical_peer_prefix = lambda token: PEER
        iface._expect_report = spy_expect_report
        iface.direct_raw_report_wait_base_s = 0.4
        iface.direct_raw_report_wait_per_hop_s = 0.0
        iface.direct_raw_zero_hop_gap_s = 0.05
        iface.direct_raw_report_enabled = True
        iface.direct_raw_reconcile_rounds = 1
        iface.direct_raw_query_attempts = 1
        iface.direct_raw_parity_enabled = False
        iface.direct_raw_burst_queue_ahead = 1
        iface.direct_raw_window_collect_s = 0.0
        iface._resolved_paths[PEER] = self.module._ResolvedPath(ZERO_HOP_PATH_HEX, 0, 1, time.monotonic())
        iface._query_rtt.pop(PEER, None)
        iface._report_rtt.pop(PEER, None)
        iface._last_firmware_ack_timeout_s.pop(PEER, None)

        def restore():
            for k, v in saved.items():
                setattr(iface, k, v)
            if saved_path is None:
                iface._resolved_paths.pop(PEER, None)
            else:
                iface._resolved_paths[PEER] = saved_path
            iface._raw_windows.pop(PEER, None)
            iface._resumable_sends.clear()
            iface._raw_incomplete_strikes.pop(PEER, None)
            iface._direct_path_recent_success.pop(PEER, None)
            iface._direct_path_failures.pop(PEER, None)
            iface._report_rtt.pop(PEER, None)
            for key in [k for k in iface._completion_query_waiters if k[0] == PEER]:
                iface._completion_query_waiters.pop(key, None)
            for key in [k for k in iface._report_expected if k[0] == PEER]:
                iface._report_expected.pop(key, None)
        return sent, queries, expected, restore

    def _payload_for(self, frag_total):
        budget = self.iface._direct_raw_payload_budget(len(bytes.fromhex(ZERO_HOP_PATH_HEX)))
        self.assertEqual(budget, 161, "the shipped zero-hop raw budget")
        size = budget * frag_total          # frag_total full chunks
        return (bytes(range(256)) * (size // 256 + 1))[:size]

    def test_zero_hop_burst_is_paced_by_the_air_and_the_report_wait_starts_at_the_burst_end(self):
        """Three full fragments, no report ever arrives, one round, one
        QUERY. With one frame allowed in the queue: fragment 1 follows
        fragment 0 after the flat gap (the radio is on 0, 1 may queue);
        fragment 2 is not handed over until fragment 0 is estimated off
        the air (one airtime after fragment 0, an airtime minus the gap
        after fragment 1); when fragment 2 is queued fragment 1 is still on
        air, so the burst end registered with `_expect_report` is the
        radio's busy-until -- two airtimes after the last send -- and the
        QUERY leaves `direct_raw_report_wait_base` (0.4 s) after THAT, not
        after the last `send_raw_data` returned."""
        iface = self.iface
        pkt_id, frag_total = 601, 3
        sent, queries, expected, restore = self._install()
        try:
            payload = self._payload_for(frag_total)
            air = iface._estimate_tx_airtime_s("", on_air_bytes=ON_AIR_172)
            gap = iface.direct_raw_zero_hop_gap_s
            self.assertGreater(air, gap + 0.1, "the scenario needs an airtime well above the flat gap")
            self.node.run_on_loop(iface._send_direct_raw_fragmented(
                TARGET, PEER, payload, pkt_id, priority=iface.PRIORITY_NORMAL, hop_count=0,
            ), timeout=20.0)

            self.assertEqual([s["frag_idx"] for s in sent], [0, 1, 2], "one round of three data fragments, no parity at zero hop")
            self.assertTrue(all(s["on_air"] == ON_AIR_172 for s in sent))
            t0, t1, t2 = (s["t"] for s in sent)
            # (a) pacing: 1 after the gap only, 2 once fragment 0 is off the air
            self.assertGreaterEqual(t1 - t0, gap - 0.02)
            self.assertLess(t1 - t0, air - 0.05, "with one frame allowed in the queue the second send does not wait an airtime")
            self.assertGreaterEqual(t2 - t1, air - gap - 0.05, f"fragment 2 left {t2 - t1:.3f}s after fragment 1; expected about air - gap = {air - gap:.3f}s")
            self.assertGreaterEqual(t2 - t0, air - 0.05, f"fragment 2 left {t2 - t0:.3f}s after fragment 0, before fragment 0's air ({air:.3f}s) was over")
            self.assertLess(t2 - t0, 2 * air, "and not later than the air allows")
            # (c) the burst end registered for the report estimator is the radio's busy-until after the last frame
            # (`_run_raw_window_rounds` registers once per missing fragment -- three for this part -- and
            # withdraws each after the round; all three carry the same burst end)
            registered = [e for e in expected if e[2] is not None]
            withdrawn = [e for e in expected if e[2] is None]
            self.assertEqual(len(registered), frag_total)
            self.assertEqual(len(withdrawn), frag_total)
            self.assertTrue(all(e[:2] == (PEER, pkt_id) for e in expected))
            burst_end = registered[0][2]
            self.assertTrue(all(e[2] == burst_end for e in registered), "one burst end for the whole burst")
            self.assertAlmostEqual(burst_end, sent[-1]["busy_until"], delta=0.01)
            self.assertAlmostEqual(burst_end - t2, 2 * air, delta=0.1,
                                   msg="one frame was still on air when the last was queued: the burst ends two airtimes later")
            # (b) the report wait runs from the burst end
            self.assertEqual(len(queries), 1)
            self.assertEqual((queries[0]["stage"], queries[0]["hop_count"]), ("raw0", 0))
            waited_from_end = queries[0]["t"] - burst_end
            self.assertGreaterEqual(waited_from_end, 0.4 - 0.05, f"the QUERY left {waited_from_end:.3f}s after the burst end; the 0.4 s report window had not elapsed")
            self.assertLess(waited_from_end, 0.4 + 0.5, f"the QUERY left {waited_from_end:.3f}s after the burst end -- far longer than the 0.4 s window")
            waited_from_last_send = queries[0]["t"] - t2
            self.assertGreaterEqual(waited_from_last_send, 2 * air + 0.4 - 0.05,
                                    f"measured from the last send_raw_data the QUERY left after {waited_from_last_send:.3f}s; "
                                    f"the pre-0.1.5 loop would have left after ~{gap + 0.4:.2f}s, before the burst was on air")
            self.assertNotIn((PEER, pkt_id), iface._report_expected, "the expectation is withdrawn after the round")
        finally:
            restore()


class ShippedBurstQueueAheadDefault(unittest.TestCase):
    def test_direct_raw_burst_queue_ahead_ships_as_one(self):
        """`_configure_retry({})` on a bare instance (the technique of
        tests/test_shipped_defaults.py; the key lives in `_configure_retry`
        next to `direct_raw_payload_cap`, not in `_configure_fragmentation`):
        one frame queued ahead of the one on air."""
        from tests._support import load_interface_module, quiet_rns
        quiet_rns()
        module = load_interface_module()
        bare = module.SmartMeshCoreInterface.__new__(module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertEqual(bare.direct_raw_burst_queue_ahead, 1)
        bare._configure_retry({"direct_raw_burst_queue_ahead": "0"})
        self.assertEqual(bare.direct_raw_burst_queue_ahead, 0)


class ReportLatencyIsNotSampledBeforeTheBurstEnd(_RadioBusyCase):
    def test_a_report_before_the_burst_end_trains_nothing_and_one_after_does(self):
        """`_record_report_latency` returns None and adds no `_report_rtt`
        sample when the report lands before the registered burst end (it
        would measure the airtime estimate's pessimism, not the report
        path); with the burst end in the past it returns the positive
        latency and seeds the estimator."""
        iface = self.iface
        pkt_id = 5
        iface._report_rtt.pop(PEER, None)
        try:
            iface._expect_report(PEER, pkt_id, time.monotonic() + 10.0)
            self.assertIsNone(iface._record_report_latency(PEER, pkt_id))
            self.assertNotIn(PEER, iface._report_rtt)
            self.assertIn((PEER, pkt_id), iface._report_expected, "the expectation stays until the sender withdraws it")

            iface._expect_report(PEER, pkt_id, time.monotonic() - 1.0)
            latency = iface._record_report_latency(PEER, pkt_id)
            self.assertIsNotNone(latency)
            self.assertGreater(latency, 0.9)
            self.assertLess(latency, 2.0)
            self.assertIn(PEER, iface._report_rtt)
            self.assertAlmostEqual(iface._report_rtt[PEER]["srtt"], latency, delta=0.01, msg="the first sample seeds srtt")

            iface._expect_report(PEER, pkt_id, None)
            self.assertIsNone(iface._record_report_latency(PEER, pkt_id), "nothing expected: nothing sampled")
        finally:
            iface._expect_report(PEER, pkt_id, None)
            iface._report_rtt.pop(PEER, None)


if __name__ == "__main__":
    unittest.main()
