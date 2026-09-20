"""
The receiver-initiated completion REPORT off the zero-hop happy path
(2026-09-20, module docstring entry "Receiver-initiated completion report for
raw bursts", commit f0a824a). `tests/test_completion_report_0920.py` pins the
wire flag, the nonce range, the waiter and the wait sizing, and its one
two-node scenario is zero-hop; the one-hop paths -- the last fragment lost
so the flagged second-last one's report has to do, a report that arrived
mid-burst being kept as the fallback, and no report at all falling back to
the QUERY after exactly the hop-scaled wait -- were exercised only by the
MeshBench `relay`/`large_payload` runs recorded in `changelog.md`. Each test
here drives the interface's own methods on a single node, with the radio
replaced by hooks, at one hop (a one-byte source route, `hop_count=1`).

Ground truth is the code, read 2026-09-20:

  * `_handle_direct_multifragment_frame` reports the bucket's bitmap on any
    flagged raw fragment that leaves gaps, and `complete=True` on a flagged
    duplicate of a packet already delivered;
  * `_send_direct_raw_fragmented` keeps an incomplete report that resolved
    the waiter before the burst ended, re-arms the waiter, and only after the
    post-burst wait yields nothing applies the kept one, captured as
    `completion_check_result` outcome `reported_stale`;
  * `_await_completion_report` writes no capture record when nothing
    arrives, and the QUERY that follows is what writes one.

One thing the code does that the docstring's "captured as `outcome=
"reported_stale"`" does not say: `_capture_completion_check_result` nulls
`complete` for every outcome but `"answered"`, so the `reported_stale`
record carries `complete: null` (the `reported` record, written by
`_await_completion_report` directly, carries the real flag). Pinned as it
is, and named in the test's docstring.
"""
import asyncio
import json
import time
import unittest

from tests._support import SingleNodeCase

PEER = "abcdef012345"
TARGET = "ab" * 32           # the peer's full pubkey hex, as _send_direct_raw_fragmented takes it
ONE_HOP_PATH_HEX = "aa"      # a one-byte source route: one repeater


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


class ReceiverReportsOnTheFlaggedSecondLastFragment(SingleNodeCase):
    """`_handle_direct_multifragment_frame`, the receive side, with the
    burst's LAST fragment never arriving: the sender flags the last two
    (`test_sender_flags_the_last_two_fragments_of_a_burst`), so the
    second-last one is what has to trigger the report."""

    def _header(self, pkt_id, frag_idx, frag_total, attempt=0):
        return self.module._FrameHeader(self.iface.PROTOCOL_VERSION, True, False, pkt_id, frag_idx, frag_total, attempt)

    def test_second_last_flagged_fragment_with_the_last_missing_reports_the_incomplete_bitmap(self):
        """Fragments 0, 1 (unflagged) and 2 (flagged, second-last of 4)
        land; 3 never does. The report goes out on fragment 2 with
        `complete=False`, `held={0,1,2}`, under the report nonce for the
        burst's round (the raw header's attempt bits), and the
        `completion_report_sent` capture record says the same."""
        iface = self.iface
        answers = []

        async def fake_answer(sender_token, pkt_id, frag_total, complete, **kwargs):
            answers.append((sender_token, pkt_id, frag_total, complete, kwargs))

        sink, restore_sink = _sink(iface)
        original_answer = iface._send_completion_answer
        iface._send_completion_answer = fake_answer
        pkt_id, frag_total, rnd = 43, 4, 2
        key = iface._reassembly_key(self._header(pkt_id, 0, frag_total, rnd), PEER, mode="direct")
        try:
            async def scenario():
                for idx in (0, 1):
                    iface._handle_direct_multifragment_frame(
                        self._header(pkt_id, idx, frag_total, rnd), b"x" * 10, PEER, raw=True, report_requested=False,
                    )
                await asyncio.sleep(0)
                self.assertEqual(answers, [], "unflagged fragments must not report")
                iface._handle_direct_multifragment_frame(
                    self._header(pkt_id, 2, frag_total, rnd), b"x" * 10, PEER, raw=True, report_requested=True,
                )
                for _ in range(3):
                    await asyncio.sleep(0)   # let the spawned answer task run
                # Phase 3 M1 (2026-09-20): the gaps report is held for one
                # fragment airtime plus the one-hop relay gap first.
                assert answers == [], "the gaps report is not sent at once (M1 debounce)"
                await asyncio.sleep(iface._report_hold_s(10 + iface.RAW_HEADER_SIZE, 1) + 0.5)
                for _ in range(3):
                    await asyncio.sleep(0)
            self.node.run_on_loop(scenario(), timeout=15.0)

            self.assertEqual(len(answers), 1, "exactly one report for the flagged second-last fragment")
            sender_token, got_pkt_id, got_total, complete, kwargs = answers[0]
            self.assertEqual((sender_token, got_pkt_id, got_total, complete), (PEER, pkt_id, frag_total, False))
            self.assertEqual(kwargs.get("held"), {0, 1, 2})
            self.assertTrue(kwargs.get("report"), "the report must be sent as a report (no QUERY nonce)")
            self.assertEqual(kwargs.get("nonce"), iface.COMPLETION_REPORT_NONCE_BASE | rnd,
                             "the report nonce carries the burst's round from the raw header's attempt bits")
            self.assertEqual(kwargs.get("version"), iface.COMPLETION_PROTOCOL_VERSION)

            sent = sink.records("completion_report_sent")
            self.assertEqual(len(sent), 1)
            self.assertEqual(sent[0]["sender_token"], PEER)
            self.assertEqual(sent[0]["pkt_id"], pkt_id)
            self.assertEqual(sent[0]["frag_total"], frag_total)
            self.assertIs(sent[0]["complete"], False)
            self.assertEqual(sent[0]["held"], [0, 1, 2])
            self.assertEqual(sent[0]["round"], rnd)
            # the bucket is still open: the sender's re-drive of fragment 3 completes it
            self.assertIn(key, iface._reassembly)
        finally:
            iface._send_completion_answer = original_answer
            restore_sink()
            iface._reassembly.pop(key, None)

    def test_flagged_duplicate_of_a_delivered_packet_reports_complete_again(self):
        """The sender never got the report (or the QUERY's answer) and
        re-burst: a flagged fragment of a packet already in the dedup
        cache is dropped as a duplicate AND answered with `complete=True`
        (the dedup branch of `_handle_direct_multifragment_frame`), so the
        sender stops without a QUERY. The packet reaches RNS once."""
        iface = self.iface
        reports = []
        original_report = iface._send_completion_report
        iface._send_completion_report = lambda *a, **k: reports.append((a, k))
        sink, restore_sink = _sink(iface)
        pkt_id, frag_total = 44, 2
        delivered_before = len(self.node.owner.received)
        try:
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(
                self._header(pkt_id, 0, frag_total), b"a" * 10, PEER, raw=True, report_requested=False))
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(
                self._header(pkt_id, 1, frag_total), b"b" * 10, PEER, raw=True, report_requested=True))
            self.assertEqual(len(reports), 1)
            self.assertTrue(reports[0][1].get("complete"))
            delivered = len(self.node.owner.received) - delivered_before
            self.assertEqual(delivered, 1, "the reassembled packet reaches RNS once")
            # the re-burst: a flagged copy of the last fragment, packet already delivered
            dropped_before = iface._incoming_dropped_total
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(
                self._header(pkt_id, 1, frag_total), b"b" * 10, PEER, raw=True, report_requested=True))
            self.assertEqual(len(reports), 2, "a flagged duplicate must be answered with a fresh report")
            self.assertTrue(reports[1][1].get("complete"))
            self.assertEqual(reports[1][1].get("held"), {0, 1})
            self.assertEqual(iface._incoming_dropped_total, dropped_before + 1, "the duplicate itself is dropped")
            self.assertEqual(len(self.node.owner.received) - delivered_before, 1, "and not delivered twice")
            # an UNFLAGGED duplicate stays silent: nothing asked for a report
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(
                self._header(pkt_id, 0, frag_total), b"a" * 10, PEER, raw=True, report_requested=False))
            self.assertEqual(len(reports), 2)
        finally:
            iface._send_completion_report = original_report
            restore_sink()


class _OneHopRawSend(SingleNodeCase):
    """Shared scaffolding: a resolved one-hop path for PEER, the raw
    fragment send replaced by a hook, short report waits, no hop gap
    factor (the per-fragment gap is then the fragment's own airtime)."""

    def _install(self, on_fragment, on_query):
        iface = self.iface
        saved = {k: getattr(iface, k) for k in (
            "_send_raw_fragment", "_query_remote_fragments", "_canonical_peer_prefix",
            "direct_raw_report_wait_base_s", "direct_raw_report_wait_per_hop_s",
            "direct_raw_hop_gap_factor", "direct_raw_report_enabled", "direct_raw_reconcile_rounds",
        )}
        saved_path = iface._resolved_paths.get(PEER)
        sent = []

        async def fake_send_raw_fragment(path, frame, priority, telemetry=None, interrupt=None):
            header, payload, src, dst = iface._decode_raw_fragment(frame)
            flagged = iface._raw_fragment_report_requested(frame)
            sent.append({"t": time.monotonic(), "frag_idx": header.frag_idx, "round": header.attempt,
                         "flagged": flagged, "path": path, "on_air": 2 + len(path) + len(frame)})
            on_fragment(header, flagged, len(frame))
            return True

        async def fake_query(target, peer_prefix, pkt_id, frag_total, stage, priority=0, hop_count=None, send_info=None, entries=None):
            return on_query({"t": time.monotonic(), "stage": stage, "hop_count": hop_count,
                             "pkt_id": pkt_id, "frag_total": frag_total})

        iface._send_raw_fragment = fake_send_raw_fragment
        iface._query_remote_fragments = fake_query
        iface._canonical_peer_prefix = lambda token: PEER
        iface.direct_raw_report_wait_base_s = 0.3
        iface.direct_raw_report_wait_per_hop_s = 0.3
        iface.direct_raw_hop_gap_factor = 0.0
        iface.direct_raw_report_enabled = True
        iface.direct_raw_reconcile_rounds = 3
        iface._resolved_paths[PEER] = self.module._ResolvedPath(ONE_HOP_PATH_HEX, 1, 1, time.monotonic())
        iface._query_rtt.pop(PEER, None)
        iface._last_firmware_ack_timeout_s.pop(PEER, None)

        def restore():
            for k, v in saved.items():
                setattr(iface, k, v)
            if saved_path is None:
                iface._resolved_paths.pop(PEER, None)
            else:
                iface._resolved_paths[PEER] = saved_path
            iface._completion_query_waiters.pop((PEER, 0), None)
            iface._resumable_sends.clear()
            iface._raw_incomplete_strikes.pop(PEER, None)
            iface._direct_path_recent_success.pop(PEER, None)
            iface._direct_path_failures.pop(PEER, None)
        return sent, restore

    def _report(self, pkt_id, frag_total, complete, held, rnd):
        """Deliver a receiver's REPORT the way the radio would hand it to
        `_handle_incoming_completion_frame`."""
        frame = self.iface._encode_completion_frame(
            self.iface.COMPLETION_TYPE_ANSWER, pkt_id, frag_total, complete=complete, held=held,
            version=self.iface.COMPLETION_PROTOCOL_VERSION, nonce=self.iface.COMPLETION_REPORT_NONCE_BASE | rnd,
        )
        self.iface._handle_incoming_completion_frame(frame, PEER)

    def _payload_for(self, frag_total):
        budget = self.iface._direct_raw_payload_budget(len(bytes.fromhex(ONE_HOP_PATH_HEX)))
        size = budget * (frag_total - 1) + 10   # frag_total chunks, the last one short
        return (bytes(range(256)) * (size // 256 + 1))[:size]

    def _run_send(self, pkt_id, payload, timeout=20.0):
        return self.node.run_on_loop(self.iface._send_direct_raw_fragmented(
            TARGET, PEER, payload, pkt_id, priority=self.iface.PRIORITY_NORMAL, hop_count=1,
        ), timeout=timeout)


class SenderKeepsAMidBurstReportAsTheFallback(_OneHopRawSend):
    def test_last_fragment_lost_the_second_last_ones_report_is_applied_as_reported_stale(self):
        """Round 0 bursts 3 fragments; the receiver's report for the
        flagged second-last one (held={0,1}) arrives while fragment 2 is
        still being sent, fragment 2 (or its report) is lost. The sender
        re-arms the waiter, waits the one-hop report window for a newer
        report, gets nothing, and applies the kept one -- capture record
        `completion_check_result` outcome `reported_stale`, stage `raw0`,
        held [0, 1] -- then round 1 re-drives exactly fragment 2 and no
        QUERY is ever sent. The record's `complete` field is `None`:
        `_capture_completion_check_result` only records it for outcome
        "answered" (the code as it is; the docstring does not say so)."""
        iface = self.iface
        pkt_id, frag_total = 501, 3
        queries = []

        def on_fragment(header, flagged, size):
            # a raw burst sends the missing fragments in index order (no
            # shuffle: `_send_direct_raw_fragmented` iterates `missing`), so
            # round 0 is 0, 1, 2 with 1 and 2 flagged
            if header.attempt == 0 and header.frag_idx == 1:
                self.assertTrue(flagged, "the second-last fragment carries the flag")
                self._report(pkt_id, frag_total, False, {0, 1}, 0)   # arrives while fragment 2 is being sent
            if header.attempt == 0 and header.frag_idx == 2:
                self.assertTrue(flagged)                              # its report is lost: nothing injected
            if header.attempt == 1:
                self._report(pkt_id, frag_total, True, set(range(frag_total)), 1)

        def on_query(info):
            queries.append(info)
            return None

        sent, restore = self._install(on_fragment, on_query)
        sink, restore_sink = _sink(iface)
        try:
            result = self._run_send(pkt_id, self._payload_for(frag_total))
            self.assertIs(result, True, "the send completes on round 1's report")
            self.assertEqual(queries, [], "a kept mid-burst report must never cost a QUERY round trip")

            round0 = [s["frag_idx"] for s in sent if s["round"] == 0]
            round1 = [s["frag_idx"] for s in sent if s["round"] == 1]
            self.assertEqual(round0, [0, 1, 2])
            self.assertEqual(round1, [2], "round 1 re-drives exactly the fragment sent after the kept report")

            checks = sink.records("completion_check_result")
            outcomes = [(r["stage"], r["outcome"]) for r in checks]
            self.assertEqual(outcomes, [("raw0", "reported_stale"), ("raw1", "reported")])
            stale = checks[0]
            self.assertEqual(stale["held"], [0, 1])
            self.assertEqual(stale["answer_version"], iface.COMPLETION_PROTOCOL_VERSION)
            self.assertEqual(stale["peer_prefix"], PEER)
            self.assertEqual(stale["pkt_id"], pkt_id)
            self.assertIsNone(stale["complete"], "as coded: complete is only recorded for outcome 'answered'")
            self.assertIs(checks[1]["complete"], True)
            self.assertNotIn((PEER, pkt_id), iface._completion_query_waiters, "the waiter is cleared after the send")
        finally:
            restore()
            restore_sink()

    def test_a_complete_mid_burst_report_is_not_stale_and_ends_the_send(self):
        """A `complete=True` report that arrives mid-burst (the receiver
        had the rest from an earlier round -- here a resumed send) is
        accepted as-is: no re-arm, no wait for a newer one, outcome
        `reported`, one round."""
        iface = self.iface
        pkt_id, frag_total = 502, 3
        queries = []

        def on_fragment(header, flagged, size):
            if flagged:
                self._report(pkt_id, frag_total, True, set(range(frag_total)), 0)

        sent, restore = self._install(on_fragment, lambda info: queries.append(info))
        sink, restore_sink = _sink(iface)
        try:
            t0 = time.monotonic()
            result = self._run_send(pkt_id, self._payload_for(frag_total))
            took = time.monotonic() - t0
            self.assertIs(result, True)
            self.assertEqual(queries, [])
            self.assertEqual(sorted(s["round"] for s in sent), [0, 0, 0], "one round only")
            checks = sink.records("completion_check_result")
            self.assertEqual([(r["stage"], r["outcome"], r["complete"]) for r in checks], [("raw0", "reported", True)])
            self.assertLess(checks[0]["report_wait_s"], 0.1, "a report already in hand costs no wait")
            self.assertLess(took, 5.0)
        finally:
            restore()
            restore_sink()


class ReportLostFallsBackToTheQueryAfterTheHopScaledWait(_OneHopRawSend):
    def test_no_report_at_one_hop_queries_after_exactly_the_one_hop_report_wait(self):
        """No report arrives (both flagged fragments' reports lost). The
        sender waits `_completion_report_wait_s(1, peer)` -- base + one
        per-hop term, larger than at zero hop and under the QUERY answer
        budget -- after the last fragment's gap, then sends the QUERY
        (`_query_remote_fragments`, stage `raw0`, `hop_count=1`). No
        `completion_check_result` record is written by the report path
        for a report that never came; the QUERY's own record is the only
        one (here the QUERY hook answers `complete`)."""
        iface = self.iface
        pkt_id, frag_total = 503, 2
        queries = []

        def on_query(info):
            queries.append(info)
            frame = iface._encode_completion_frame(
                iface.COMPLETION_TYPE_ANSWER, pkt_id, frag_total, complete=True,
                held=set(range(frag_total)), version=iface.COMPLETION_PROTOCOL_VERSION, nonce=1,
            )
            return iface._decode_completion_frame(frame)

        sent, restore = self._install(lambda header, flagged, size: None, on_query)
        sink, restore_sink = _sink(iface)
        try:
            wait_1hop = iface._completion_report_wait_s(1, PEER)
            wait_0hop = iface._completion_report_wait_s(0, PEER)
            budget_1hop = iface._completion_query_timeout_s(PEER, 1)
            self.assertGreater(wait_1hop, wait_0hop, "the report wait grows with the hop count")
            self.assertLess(wait_1hop, budget_1hop, "and stays under the QUERY answer budget it falls back to")

            result = self._run_send(pkt_id, self._payload_for(frag_total))
            self.assertIs(result, True, "the QUERY's answer completes the send")
            self.assertEqual(len(queries), 1, "exactly one QUERY, after the report window")
            self.assertEqual(queries[0]["stage"], "raw0")
            self.assertEqual(queries[0]["hop_count"], 1)
            self.assertEqual([s["frag_idx"] for s in sent if s["round"] == 0].__len__(), frag_total)
            self.assertTrue(all(s["flagged"] for s in sent), "both fragments of a 2-fragment burst are flagged")

            last = max(sent, key=lambda s: s["t"])
            last_sent_at = last["t"]
            gap = iface._raw_fragment_gap_s(1, last["on_air"])   # the last fragment's own gap (its airtime at 1 hop)
            waited = queries[0]["t"] - last_sent_at
            self.assertGreaterEqual(waited, gap + wait_1hop - 0.05,
                                    f"the QUERY left {waited:.2f}s after the last fragment; the gap ({gap:.2f}s) "
                                    f"plus the one-hop report wait ({wait_1hop:.2f}s) had not elapsed")
            self.assertLess(waited, gap + wait_1hop + 0.5,
                            f"the QUERY left {waited:.2f}s after the last fragment -- longer than the gap plus "
                            f"the one-hop report wait ({gap + wait_1hop:.2f}s); a QUERY budget ({budget_1hop:.1f}s) "
                            f"must not be waited for a report")
            self.assertEqual(sink.records("completion_check_result"), [],
                             "nothing arrived, so the report path writes no record (the QUERY writes its own)")
        finally:
            restore()
            restore_sink()


class OneHopReportWaitFromShippedDefaults(SingleNodeCase):
    def test_shipped_one_hop_wait_is_under_the_answer_budget_at_every_depth(self):
        """The values the interface ships (an empty config block): report
        wait 4.0 s + 2.5 s x hops (phase 1, 2026-09-20: was 2.0 + 3.0 x
        hops; the zero-hop receiver's report waited p90 4-5 s for its own
        lock, so a 2 s window sent 29 of 77 hop-0 rounds to a QUERY),
        completion budget floor 5.0 s + 2.5 s x hops capped at 15 s. So a
        lost report costs 4.0 s at zero hop, 6.5 s at one hop (the `relay`
        case), 9.0 s at two and 11.5 s at three -- every one strictly under
        the QUERY budget of the same depth, which the window's per-hop slope
        now matches. With no report ever measured the window IS the floor;
        `ReportWindowGrowsWithMeasuredLatency` (tests/test_report_window_
        0920.py) covers the estimator."""
        module = self.module
        bare = module.SmartMeshCoreInterface.__new__(module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertTrue(bare.direct_raw_report_enabled)
        self.assertEqual(bare.direct_raw_report_wait_base_s, 4.0)
        self.assertEqual(bare.direct_raw_report_wait_per_hop_s, 2.5)
        self.assertEqual(bare.direct_completion_check_timeout_s, 5.0)
        self.assertEqual(bare.direct_completion_check_timeout_per_hop_s, 2.5)
        self.assertEqual(bare.direct_completion_check_timeout_max_s, 15.0)

        iface = self.iface
        keys = ("direct_raw_report_wait_base_s", "direct_raw_report_wait_per_hop_s",
                "direct_completion_check_timeout_s", "direct_completion_check_timeout_per_hop_s",
                "direct_completion_check_timeout_max_s", "direct_completion_check_timeout_max_multihop_s")
        saved = {k: getattr(iface, k) for k in keys}
        for k in keys:
            setattr(iface, k, getattr(bare, k))
        iface._query_rtt.pop(PEER, None)
        iface._report_rtt.pop(PEER, None)
        iface._last_firmware_ack_timeout_s.pop(PEER, None)
        try:
            for hops, want_wait, want_budget in ((0, 4.0, 5.0), (1, 6.5, 7.5), (2, 9.0, 10.0), (3, 11.5, 12.5)):
                self.assertAlmostEqual(iface._completion_query_timeout_s(PEER, hops), want_budget, msg=f"hops={hops}")
                self.assertAlmostEqual(iface._completion_report_wait_s(hops, PEER), want_wait, msg=f"hops={hops}")
                self.assertLess(iface._completion_report_wait_s(hops, PEER), iface._completion_query_timeout_s(PEER, hops))
        finally:
            for k, v in saved.items():
                setattr(iface, k, v)


if __name__ == "__main__":
    unittest.main()
