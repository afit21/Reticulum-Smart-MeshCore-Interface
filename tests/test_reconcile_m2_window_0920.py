"""
Phase 3, M2 (2026-09-20, docs/reconcile_redesign.md): one report per WINDOW.

RNS's Resource sender emits a window of parts within milliseconds
(`RNS/Resource.py` `request` -> one `link.send` per requested part); each
part used to be its own burst-and-report exchange, two in flight per peer,
about 12 reports and 6 quiet gaps per 4-part window. Now consecutive
raw-eligible sends to one peer within `direct_raw_window_collect` (0.75 s)
form one window: every fragment back to back, the last two flagged, one
quiet period, one v4 report with a bitmap per part.

Pinned:
  * `_window_collect_s`: 0.75 s by default, 0 with batching off;
  * three parts arriving together form ONE window: one burst of all their
    fragments in part order, exactly the last two flagged, one report wait,
    one v4 report completing all three, no QUERY, one `raw_window` record;
  * a v4 report leaving one part with a gap re-drives exactly that
    fragment in round 1 (the completed parts are done);
  * no report -> ONE v4 QUERY listing every outstanding part; its v4 ANSWER
    completes them;
  * the receiver's report for a flagged fragment lists the sender's recent
    raw packets (newest first), and a v4 QUERY is answered entry by entry;
  * batching off -> each part is a window of one with no collect wait.
"""
import asyncio
import time
import unittest

from tests._support import SingleNodeCase, wait_until
from tests.test_completion_report_one_hop_0920 import _OneHopRawSend, PEER, TARGET, _sink

PKT_A, PKT_B, PKT_C = 701, 702, 703


class PureTiming(SingleNodeCase):
    def test_window_collect(self):
        iface = self.iface
        saved = (iface.direct_raw_window_enabled, iface.direct_raw_window_collect_s)
        try:
            iface.direct_raw_window_enabled, iface.direct_raw_window_collect_s = True, 0.75
            self.assertEqual(iface._window_collect_s(), 0.75)
            iface.direct_raw_window_enabled = False
            self.assertEqual(iface._window_collect_s(), 0.0)
        finally:
            iface.direct_raw_window_enabled, iface.direct_raw_window_collect_s = saved
        bare = self.module.SmartMeshCoreInterface.__new__(self.module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertTrue(bare.direct_raw_window_enabled)
        self.assertEqual(bare.direct_raw_window_collect_s, 0.75)
        self.assertEqual(bare.direct_raw_window_max_parts, 6)


class _WindowSend(_OneHopRawSend):
    def _install_window(self, on_fragment, on_query, collect_s=0.3):
        iface = self.iface
        sent, restore = self._install(on_fragment, on_query)
        saved = (iface.direct_raw_window_enabled, iface.direct_raw_window_collect_s, iface.direct_raw_window_max_parts,
                 iface.direct_report_debounce)
        iface.direct_raw_window_enabled, iface.direct_raw_window_collect_s, iface.direct_raw_window_max_parts = True, collect_s, 6
        iface.direct_report_debounce = False
        sink, restore_sink = _sink(iface)

        def restore_all():
            iface.direct_raw_window_enabled, iface.direct_raw_window_collect_s, iface.direct_raw_window_max_parts, iface.direct_report_debounce = saved
            restore_sink()
            iface._raw_windows.clear()
            for k in [k for k in iface._completion_query_waiters if k[0] == PEER]:
                iface._completion_query_waiters.pop(k, None)
            restore()
        return sent, sink, restore_all

    def _report_v4(self, entries, rnd=0):
        frame = self.iface._encode_completion_frame_v4(
            self.iface.COMPLETION_TYPE_ANSWER, entries, nonce=self.iface.COMPLETION_REPORT_NONCE_BASE | rnd)
        self.iface._handle_incoming_completion_frame(frame, PEER)

    def _run_parts(self, payloads_by_pkt, report_after_burst=None, timeout=30.0):
        """Send every part concurrently (as the outgoing worker would within
        milliseconds); returns {pkt_id: result}."""
        iface = self.iface

        async def run():
            tasks = {pkt: asyncio.ensure_future(iface._send_direct_raw_fragmented(
                TARGET, PEER, payload, pkt, priority=iface.PRIORITY_NORMAL, hop_count=1))
                for pkt, payload in payloads_by_pkt.items()}
            if report_after_burst is not None:
                asyncio.ensure_future(report_after_burst())
            results = {}
            for pkt, t in tasks.items():
                results[pkt] = await t
            return results

        return self.node.run_on_loop(run(), timeout=timeout)


class ThreePartsOneWindow(_WindowSend):
    def test_one_burst_one_report_completes_all_parts(self):
        iface = self.iface
        queries = []
        sent, sink, restore = self._install_window(lambda h, f, s: None, lambda info: queries.append(info))
        try:
            frag_totals = {PKT_A: 3, PKT_B: 2, PKT_C: 3}
            payloads = {pkt: self._payload_for(n) for pkt, n in frag_totals.items()}

            async def report_when_burst_done():
                total = sum(frag_totals.values())
                while len(sent) < total:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.1)
                self._report_v4([(pkt, n, True, set(range(n))) for pkt, n in frag_totals.items()])

            results = self._run_parts(payloads, report_when_burst_done)
        finally:
            restore()
        self.assertEqual(results, {PKT_A: True, PKT_B: True, PKT_C: True})
        self.assertEqual(queries, [], "no QUERY: the one report answered for the window")
        order = [(s["frag_idx"], s["round"]) for s in sent]
        self.assertEqual(len(sent), 8, f"every fragment of every part once: {order}")
        self.assertEqual([s["flagged"] for s in sent], [False] * 6 + [True, True], "exactly the last two fragments of the window are flagged")
        windows = sink.records("raw_window")
        self.assertEqual(len(windows), 1)
        self.assertEqual(sorted(windows[0]["parts"]), [PKT_A, PKT_B, PKT_C])
        self.assertEqual(windows[0]["fragments"], 8)
        checks = sink.records("completion_check_result")
        self.assertEqual(len(checks), 1, f"one report wait for the window: {checks}")
        self.assertEqual(checks[0]["outcome"], "reported")

    def test_report_with_one_gap_redrives_only_that_fragment(self):
        iface = self.iface
        rounds = {"n": 0}
        sent, sink, restore = self._install_window(lambda h, f, s: None, lambda info: None)
        try:
            frag_totals = {PKT_A: 3, PKT_B: 3}
            payloads = {pkt: self._payload_for(n) for pkt, n in frag_totals.items()}

            async def reports():
                while len(sent) < 6:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.1)
                # part A complete, part B missing fragment 1
                self._report_v4([(PKT_A, 3, True, {0, 1, 2}), (PKT_B, 3, False, {0, 2})], rnd=0)
                while len(sent) < 7:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.1)
                self._report_v4([(PKT_B, 3, True, {0, 1, 2})], rnd=1)

            results = self._run_parts(payloads, reports)
        finally:
            restore()
        self.assertEqual(results, {PKT_A: True, PKT_B: True})
        round1 = [(s["frag_idx"], s["round"]) for s in sent if s["round"] == 1]
        self.assertEqual(round1, [(1, 1)], f"round 1 re-drove exactly B's fragment 1: {[(s['frag_idx'], s['round']) for s in sent]}")

    def test_no_report_one_v4_query_for_the_outstanding_parts(self):
        iface = self.iface
        queries = []
        frag_totals = {PKT_A: 2, PKT_B: 2, PKT_C: 2}

        def on_query(info):
            queries.append(info)
            return iface._decode_completion_frame(iface._encode_completion_frame_v4(
                iface.COMPLETION_TYPE_ANSWER, [(pkt, n, True, set(range(n))) for pkt, n in frag_totals.items()], nonce=1))

        sent, sink, restore = self._install_window(lambda h, f, s: None, on_query)
        # The stub replaces _query_remote_fragments; record the entries it was asked for.
        original_fake = iface._query_remote_fragments

        async def recording_query(target, peer_prefix, pkt_id, frag_total, stage, priority=0, hop_count=None, send_info=None, entries=None):
            queries.append({"entries": entries, "stage": stage})
            return iface._decode_completion_frame(iface._encode_completion_frame_v4(
                iface.COMPLETION_TYPE_ANSWER, [(pkt, n, True, set(range(n))) for pkt, n in frag_totals.items()], nonce=1))

        iface._query_remote_fragments = recording_query
        try:
            payloads = {pkt: self._payload_for(n) for pkt, n in frag_totals.items()}
            results = self._run_parts(payloads)
        finally:
            iface._query_remote_fragments = original_fake
            restore()
        self.assertEqual(results, {PKT_A: True, PKT_B: True, PKT_C: True})
        self.assertEqual(len(queries), 1, f"exactly one QUERY for the window: {queries}")
        self.assertEqual(sorted(p for p, _t in queries[0]["entries"]), [PKT_A, PKT_B, PKT_C])

    def test_batching_off_is_a_window_of_one_without_the_collect_wait(self):
        iface = self.iface
        sent, sink, restore = self._install_window(lambda h, f, s: None, lambda info: None, collect_s=0.5)
        iface.direct_raw_window_enabled = False
        try:
            async def report():
                while len(sent) < 2:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.05)
                self._report_v4([(PKT_A, 2, True, {0, 1})])

            t0 = time.monotonic()
            results = self._run_parts({PKT_A: self._payload_for(2)}, report)
            took = time.monotonic() - t0
        finally:
            restore()
        self.assertEqual(results, {PKT_A: True})
        self.assertLess(took, 3.0)
        windows = sink.records("raw_window")
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0]["parts"], [PKT_A])


class ReceiverSideV4(SingleNodeCase):
    def _headers(self, pkt_id, total, attempt=0):
        return [self.module._FrameHeader(self.iface.PROTOCOL_VERSION, True, False, pkt_id, i, total, attempt) for i in range(total)]

    def test_report_lists_recent_packets_and_v4_query_is_answered_per_entry(self):
        iface = self.iface
        answers = []
        original = iface._send_completion_answer

        async def fake_answer(sender_token, pkt_id, frag_total, complete, held=None, version=None, nonce=None, report=False, entries=None):
            answers.append({"pkt_id": pkt_id, "complete": complete, "report": report, "entries": entries, "version": version})

        iface._send_completion_answer = fake_answer
        saved = (iface.direct_report_debounce, iface.direct_raw_report_enabled)
        iface.direct_report_debounce, iface.direct_raw_report_enabled = False, True
        keys = []
        try:
            # packet 801 complete (2 fragments), packet 802 flagged with a gap
            h1 = self._headers(801, 2)
            h2 = self._headers(802, 3)
            keys = [iface._reassembly_key(h1[0], PEER, mode="direct"), iface._reassembly_key(h2[0], PEER, mode="direct")]
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h1[0], b"a" * 10, PEER, raw=True, report_requested=False))
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h1[1], b"b" * 10, PEER, raw=True, report_requested=True))
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h2[0], b"c" * 10, PEER, raw=True, report_requested=False))
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h2[2], b"d" * 10, PEER, raw=True, report_requested=True))
            self.assertEqual(len(answers), 2)
            complete_report, gaps_report = answers
            self.assertTrue(complete_report["report"] and complete_report["complete"])
            self.assertEqual(gaps_report["pkt_id"], 802)
            self.assertFalse(gaps_report["complete"])
            entries = {e[0]: e for e in gaps_report["entries"]}
            self.assertEqual(entries[802][2], False)
            self.assertEqual(set(entries[802][3]), {0, 2})
            self.assertIn(801, entries, "the earlier, completed packet is listed too")
            self.assertTrue(entries[801][2])
            self.assertEqual(gaps_report["entries"][0][0], 802, "the triggering packet comes first")

            # a multi-entry QUERY about both is answered entry by entry (v5
            # since alpha 0.1.6: the v4 shape plus the sender's path view)
            query = iface._encode_completion_frame_v5(iface.COMPLETION_TYPE_QUERY, [(801, 2, False, None), (802, 3, False, None)], nonce=7)
            self.on_loop(lambda: iface._handle_incoming_completion_frame(query, PEER))
            self.assertTrue(wait_until(lambda: len(answers) == 3, 2.0))
            ans = answers[2]
            self.assertFalse(ans["report"])
            self.assertEqual(ans["version"], iface.COMPLETION_PROTOCOL_VERSION)
            by = {e[0]: e for e in ans["entries"]}
            self.assertTrue(by[801][2])
            self.assertEqual(set(by[802][3]), {0, 2})
        finally:
            iface._send_completion_answer = original
            iface.direct_report_debounce, iface.direct_raw_report_enabled = saved
            for k in keys:
                iface._reassembly.pop(k, None)
                iface._dedup.pop(k, None)
            iface._recent_raw_pkts.pop(PEER, None)


if __name__ == "__main__":
    unittest.main()
