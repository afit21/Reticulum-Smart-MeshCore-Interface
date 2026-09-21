"""
An early report is progress, not the end of the wait (alpha 0.1.5, item 2c,
2026-09-21).

The field sequence this pins (`fieldtests/raw/Alpha0.1.4/afipc_*082952`,
08:38): the desktop burst window [8, 9, 10, 11, 12] at zero hop; the laptop's
report for part 8 arrived while the desktop's radio still held most of the
window in its queue; `_await_completion_report` found its future already
resolved and returned at once (`report_wait_s` 0.0); parts 9-12 -- absent
from that report because they had not landed yet -- were re-burst
immediately behind the round-0 frames still in the radio, 8 fragments
re-sent for nothing, and the laptop's reports for 9 and 10 were transmitted
into that queue and lost.

Now (`_await_completion_report(..., burst_end=, on_early=)`): a report that
arrives before the burst has ended on air -- the future already done when the
wait starts, or a report landing while `time.monotonic() < burst_end` -- is
applied to the parts it names (`_apply_window_entries`) and the wait
continues to burst_end + window; parts absent from any report are re-burst
only after that wait has expired, and then the last early report is acted on
(captured as `reported_stale`, `early_reports` counted). An early report that
leaves nothing missing ends the wait at once.

Pinned:
  * the 08:38 sequence -- early report for part 8 mid-burst, the receiver's
    final report for the whole window after the burst end -- re-sends
    NOTHING: one round, no QUERY, one `completion_check_result` with
    `early_reports` 1;
  * the early report alone (the final one lost): the re-burst of parts 9-12
    starts only after burst_end + the report window, with no QUERY
    (`reported_stale`), and part 8 is not re-sent;
  * an early report that completes every part ends the wait at once.
"""
import asyncio
import time
import unittest

from tests._support import wait_until
from tests.test_completion_report_one_hop_0920 import _OneHopRawSend, PEER, TARGET, _sink

PARTS = (8, 9, 10, 11, 12)
FRAGS = 2


class _ZeroHopWindow(_OneHopRawSend):
    """The one-hop scaffold moved to zero hop with a firmware-queue model:
    every fake send extends the radio's busy-until by the frame's own
    airtime, as `_pre_transmit_gate` does for a real send."""

    def _install_zero_hop(self, on_fragment, on_query):
        iface = self.iface
        sent, restore = self._install(on_fragment, on_query)
        saved = {k: getattr(iface, k) for k in (
            "direct_raw_window_enabled", "direct_raw_window_collect_s", "direct_raw_window_max_parts",
            "direct_report_debounce", "direct_raw_zero_hop_gap_s", "direct_raw_burst_queue_ahead",
            "direct_raw_report_wait_base_s", "direct_raw_report_wait_per_hop_s", "direct_raw_query_attempts",
        )}
        iface.direct_raw_window_enabled, iface.direct_raw_window_collect_s, iface.direct_raw_window_max_parts = True, 0.3, 6
        iface.direct_report_debounce = False
        iface.direct_raw_zero_hop_gap_s = 0.02
        iface.direct_raw_burst_queue_ahead = 1
        iface.direct_raw_report_wait_base_s, iface.direct_raw_report_wait_per_hop_s = 0.5, 0.0
        iface.direct_raw_query_attempts = 1
        iface._resolved_paths[PEER] = self.module._ResolvedPath("", 0, 1, time.monotonic())
        iface._radio_busy_until = 0.0
        iface._report_rtt.pop(PEER, None)
        inner = iface._send_raw_fragment

        async def queued_send(path, frame, priority, telemetry=None, interrupt=None):
            ok = await inner(path, frame, priority, telemetry, interrupt)
            iface._note_radio_keyed(iface._estimate_tx_airtime_s("", on_air_bytes=2 + len(path) + len(frame)))
            return ok

        iface._send_raw_fragment = queued_send
        sink, restore_sink = _sink(iface)

        def restore_all():
            for k, v in saved.items():
                setattr(iface, k, v)
            restore_sink()
            iface._raw_windows.clear()
            for k in [k for k in iface._completion_query_waiters if k[0] == PEER]:
                iface._completion_query_waiters.pop(k, None)
            iface._radio_busy_until = 0.0
            restore()
        return sent, sink, restore_all

    def _report_v4(self, entries, rnd=0):
        frame = self.iface._encode_completion_frame_v4(
            self.iface.COMPLETION_TYPE_ANSWER, entries, nonce=self.iface.COMPLETION_REPORT_NONCE_BASE | rnd)
        self.iface._handle_incoming_completion_frame(frame, PEER)

    def _full_payload(self, frag_total):
        budget = self.iface._direct_raw_payload_budget(0)
        size = budget * frag_total   # every fragment full size: one airtime for all
        return (bytes(range(256)) * (size // 256 + 1))[:size]

    def _run_window(self, extra=None, timeout=40.0):
        iface = self.iface

        async def run():
            tasks = {pkt: asyncio.ensure_future(iface._send_direct_raw_fragmented(
                TARGET, PEER, self._full_payload(FRAGS), pkt, priority=iface.PRIORITY_NORMAL, hop_count=0))
                for pkt in PARTS}
            if extra is not None:
                asyncio.ensure_future(extra())
            return {pkt: await t for pkt, t in tasks.items()}

        return self.node.run_on_loop(run(), timeout=timeout)


class TheFieldSequenceResendsNothing(_ZeroHopWindow):
    def test_early_report_for_part_8_then_final_report_after_the_burst(self):
        iface = self.iface
        queries = []
        total = len(PARTS) * FRAGS
        state = {"early_at": None, "final_at": None, "burst_end": None}

        def on_fragment(header, flagged, size):
            # The laptop's report for part 8 lands while the desktop is still
            # queuing part 10 -- mid-burst, and silent about parts 9-12.
            if header.attempt == 0 and header.pkt_id == 10 and header.frag_idx == 0:
                state["early_at"] = time.monotonic()
                self._report_v4([(8, FRAGS, True, set(range(FRAGS)))])

        sent, sink, restore = self._install_zero_hop(on_fragment, lambda info: queries.append(info) or None)
        try:
            async def final_report_after_burst_end():
                while len(sent) < total:
                    await asyncio.sleep(0.01)
                state["burst_end"] = iface._radio_busy_until
                await asyncio.sleep(max(0.0, iface._radio_busy_until - time.monotonic()) + 0.15)
                state["final_at"] = time.monotonic()
                self._report_v4([(p, FRAGS, True, set(range(FRAGS))) for p in reversed(PARTS)])

            results = self._run_window(final_report_after_burst_end)
        finally:
            restore()
        self.assertEqual(results, {p: True for p in PARTS})
        self.assertEqual(queries, [], "no QUERY: the window was reported")
        self.assertEqual(len(sent), total, f"nothing re-sent: {[(s['round'], s['frag_idx']) for s in sent]}")
        self.assertTrue(all(s["round"] == 0 for s in sent))
        self.assertIsNotNone(state["early_at"])
        self.assertLess(state["early_at"], state["burst_end"], "the part-8 report was early by construction")
        checks = sink.records("completion_check_result")
        self.assertEqual([c["outcome"] for c in checks], ["reported"])
        self.assertEqual(checks[0]["early_reports"], 1)
        self.assertGreaterEqual(checks[0]["report_wait_s"], 0.1, "the wait ran past the burst end for the final report")
        self.assertEqual(sorted(e[0] for e in checks[0]["entries"]), sorted(PARTS))
        self.assertEqual(len(sink.records("raw_window")), 1)

    def test_early_report_alone_is_acted_on_only_after_the_wait_expires(self):
        iface = self.iface
        queries = []
        total = len(PARTS) * FRAGS
        state = {"round1_first_at": None, "burst_end": None}

        def on_fragment(header, flagged, size):
            if header.attempt == 0 and header.pkt_id == 10 and header.frag_idx == 0:
                self._report_v4([(8, FRAGS, True, set(range(FRAGS)))])
            if header.attempt == 1 and state["round1_first_at"] is None:
                state["round1_first_at"] = time.monotonic()
                state["burst_end"] = state.get("burst_end_seen")
            if header.attempt == 1:
                # Round 1's report: everything landed.
                if header.pkt_id == 12 and header.frag_idx == FRAGS - 1:
                    self._report_v4([(p, FRAGS, True, set(range(FRAGS))) for p in reversed(PARTS)], rnd=1)

        sent, sink, restore = self._install_zero_hop(on_fragment, lambda info: queries.append(info) or None)
        try:
            async def note_burst_end():
                while len(sent) < total:
                    await asyncio.sleep(0.01)
                state["burst_end_seen"] = iface._radio_busy_until

            results = self._run_window(note_burst_end)
            wait_s = iface.direct_raw_report_wait_base_s
        finally:
            restore()
        self.assertEqual(results, {p: True for p in PARTS})
        self.assertEqual(queries, [], "an early report kept as the fallback never costs a QUERY")
        round0 = [(s["pkt_id"] if "pkt_id" in s else None, s["frag_idx"]) for s in sent if s["round"] == 0]
        round1 = [s for s in sent if s["round"] == 1]
        self.assertEqual(len(round0), total)
        self.assertEqual(len(round1), (len(PARTS) - 1) * FRAGS, "parts 9-12 re-driven, part 8 not")
        self.assertIsNotNone(state["burst_end"])
        self.assertGreaterEqual(state["round1_first_at"] - state["burst_end"], wait_s - 0.05,
                                "the re-burst starts only after burst_end + the report window")
        checks = sink.records("completion_check_result")
        self.assertEqual([c["outcome"] for c in checks], ["reported_stale", "reported"])
        self.assertEqual(checks[0]["early_reports"], 1)

    def test_early_report_that_completes_everything_ends_the_wait_at_once(self):
        iface = self.iface
        queries = []
        total = len(PARTS) * FRAGS

        def on_fragment(header, flagged, size):
            # An (optimistic-estimate) receiver that already holds every part
            # by the time the last fragment is queued.
            if header.attempt == 0 and header.pkt_id == 12 and header.frag_idx == FRAGS - 1:
                self._report_v4([(p, FRAGS, True, set(range(FRAGS))) for p in reversed(PARTS)])

        sent, sink, restore = self._install_zero_hop(on_fragment, lambda info: queries.append(info) or None)
        try:
            t0 = time.monotonic()
            results = self._run_window()
            took = time.monotonic() - t0
        finally:
            restore()
        self.assertEqual(results, {p: True for p in PARTS})
        self.assertEqual(len(sent), total)
        checks = sink.records("completion_check_result")
        self.assertEqual([c["outcome"] for c in checks], ["reported"])
        self.assertEqual(checks[0]["early_reports"], 1)
        self.assertLess(checks[0]["report_wait_s"], 0.1, "nothing missing after the early report: no wait")


if __name__ == "__main__":
    unittest.main()
