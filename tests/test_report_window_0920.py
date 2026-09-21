"""
The completion-report window is sized from the MEASURED per-peer report
latency (phase 1, 2026-09-20), the way the QUERY answer budget already
follows `_query_rtt`.

Field numbers this pins (`fieldtests/raw/Alpha0.1.3/`, 2026-09-20, zero
hop): the receiver's report attempt waited a median 1.1-1.4 s and p90
4-5 s for its own radio lock on top of ~2.3 s of serial delivery latency,
so with the shipped 2 s window the desktop (sender) saw only 43 of its 77
hop-0 rounds `reported` and 29 fell back to a QUERY that was then
`answered` -- two frames, two ACKs and ~5 s for a report that was merely
late. The on-time reports' `report_wait_s` were median 1.18 s, max 1.97 s:
truncated by the window itself, which is why the estimator must also see
the LATE reports.

  * floor = `direct_raw_report_wait_base` (4.0 s) + `..._per_hop` (2.5 s)
    x hops; with no sample the window is the floor;
  * once reports have been measured the window is max(floor, srtt + 4 x
    rttvar), never above the QUERY answer budget;
  * a report is sampled in `_handle_incoming_completion_frame` when an
    expectation exists, whether or not a waiter still does (late reports
    included), and the expectation is withdrawn when the round ends;
  * the estimator is per peer and dropped with the peer's other path stats.
"""
import time
import unittest

from tests._support import SingleNodeCase

PEER = "abcdef012345"


class ReportWindowGrowsWithMeasuredLatency(SingleNodeCase):
    BUDGET_KEYS = ("direct_raw_report_wait_base_s", "direct_raw_report_wait_per_hop_s",
                   "direct_completion_check_timeout_s", "direct_completion_check_timeout_per_hop_s",
                   "direct_completion_check_timeout_max_s", "direct_completion_check_timeout_max_multihop_s")
    SHIPPED = (4.0, 2.5, 5.0, 2.5, 15.0, 18.0)   # the unit harness runs FAST_TIMING; these are the shipped values

    def _reset(self):
        iface = self.iface
        iface._report_rtt.pop(PEER, None)
        iface._query_rtt.pop(PEER, None)
        iface._last_firmware_ack_timeout_s.pop(PEER, None)
        iface._report_expected.clear()

    def test_floor_without_samples_then_measured_above_it(self):
        iface = self.iface
        saved = tuple(getattr(iface, k) for k in self.BUDGET_KEYS)
        for k, v in zip(self.BUDGET_KEYS, self.SHIPPED):
            setattr(iface, k, v)
        self._reset()
        try:
            self.assertAlmostEqual(iface._completion_report_wait_s(0, PEER), 4.0)
            # Reports that keep arriving 1.2 s after the burst: srtt ~1.2,
            # rttvar shrinks -- the floor still applies.
            for _ in range(8):
                iface._rtt_sample(iface._report_rtt, PEER, 1.2)
            self.assertAlmostEqual(iface._completion_report_wait_s(0, PEER), 4.0)
            # A burst of late reports (the field's p90 case) widens it.
            for latency in (5.0, 6.0, 5.5, 6.5):
                iface._rtt_sample(iface._report_rtt, PEER, latency)
            rs = iface._report_rtt[PEER]
            self.assertGreater(iface._completion_report_wait_s(0, PEER), 4.0)
            self.assertAlmostEqual(iface._completion_report_wait_s(0, PEER),
                                   min(rs["srtt"] + 4.0 * rs["rttvar"], iface._completion_query_timeout_s(PEER, 0)))
            # Never above the QUERY answer budget.
            for _ in range(10):
                iface._rtt_sample(iface._report_rtt, PEER, 40.0)
            self.assertAlmostEqual(iface._completion_report_wait_s(0, PEER), iface._completion_query_timeout_s(PEER, 0))
        finally:
            for k, v in zip(self.BUDGET_KEYS, saved):
                setattr(iface, k, v)
            self._reset()

    def test_late_report_is_sampled_without_a_waiter_and_expectation_is_one_round(self):
        """The sender registered an expectation at the burst's end; the
        report arrives after the window (no waiter registered any more):
        it is still measured. After the expectation is withdrawn, nothing
        is."""
        iface = self.iface
        self._reset()
        pkt_id = 4242
        original_canonical = iface._canonical_peer_prefix
        iface._canonical_peer_prefix = lambda token: PEER   # the peer is not a contact in the single-node sandbox
        try:
            iface._expect_report(PEER, pkt_id, time.monotonic() - 3.0)
            frame = iface._encode_completion_frame(
                iface.COMPLETION_TYPE_ANSWER, pkt_id, 4, complete=True, held={0, 1, 2, 3},
                nonce=iface.COMPLETION_REPORT_NONCE_BASE | 1,
            )
            iface._handle_incoming_completion_frame(frame, PEER)
            self.assertIn(PEER, iface._report_rtt)
            self.assertAlmostEqual(iface._report_rtt[PEER]["srtt"], 3.0, delta=0.3)
            self.assertEqual(iface._report_rtt[PEER]["samples"], 1)
            # A QUERY answer (nonce outside the report range) is not a report sample.
            answer = iface._encode_completion_frame(
                iface.COMPLETION_TYPE_ANSWER, pkt_id, 4, complete=True, held={0, 1, 2, 3}, nonce=0x11,
            )
            iface._handle_incoming_completion_frame(answer, PEER)
            self.assertEqual(iface._report_rtt[PEER]["samples"], 1)
            # Round over: the expectation is withdrawn, a stray report measures nothing.
            iface._expect_report(PEER, pkt_id, None)
            iface._handle_incoming_completion_frame(frame, PEER)
            self.assertEqual(iface._report_rtt[PEER]["samples"], 1)
        finally:
            iface._canonical_peer_prefix = original_canonical
            self._reset()

    def test_second_last_fragment_report_is_provisional(self):
        """2026-09-20 review of the desktop capture: 20 of its 43 hop-0
        "reported" rounds acted on the second-last fragment's incomplete
        report (missing only the last fragment sent) and re-drove that
        fragment as a duplicate. Now: provisional -- the wait continues up
        to half the window for the last fragment's own report; a complete
        one supersedes it, otherwise it is acted on (`provisional: true`)."""
        import asyncio
        iface = self.iface
        self._reset()
        saved = (iface.direct_raw_report_wait_base_s, iface.direct_raw_report_wait_per_hop_s)
        iface.direct_raw_report_wait_base_s, iface.direct_raw_report_wait_per_hop_s = 1.0, 0.0
        module = self.module
        incomplete = module._CompletionFrame(version=3, type=iface.COMPLETION_TYPE_ANSWER, complete=False,
                                             pkt_id=9, frag_total=4, held=frozenset({0, 1, 2}), nonce=0xF0)
        complete = module._CompletionFrame(version=3, type=iface.COMPLETION_TYPE_ANSWER, complete=True,
                                           pkt_id=9, frag_total=4, held=frozenset({0, 1, 2, 3}), nonce=0xF0)
        try:
            async def superseded():
                loop = asyncio.get_running_loop()
                fut = loop.create_future()
                fresh = loop.create_future()
                loop.call_later(0.1, fut.set_result, incomplete)
                loop.call_later(0.3, fresh.set_result, complete)
                t0 = time.monotonic()
                got = await iface._await_completion_report(fut, PEER, 9, 4, 0, "raw0", last_sent_idx=3, rearm=lambda: fresh)
                return got, time.monotonic() - t0

            got, took = self.node.run_on_loop(superseded(), timeout=10.0)
            self.assertTrue(got.complete, "the complete report that followed supersedes the provisional one")
            self.assertLess(took, 0.8)

            async def acted_on():
                loop = asyncio.get_running_loop()
                fut = loop.create_future()
                fresh = loop.create_future()   # never resolves: the last fragment really was lost
                loop.call_later(0.1, fut.set_result, incomplete)
                t0 = time.monotonic()
                got = await iface._await_completion_report(fut, PEER, 9, 4, 0, "raw0", last_sent_idx=3, rearm=lambda: fresh)
                return got, time.monotonic() - t0

            got, took = self.node.run_on_loop(acted_on(), timeout=10.0)
            self.assertFalse(got.complete)
            self.assertEqual(set(got.held), {0, 1, 2}, "the provisional report is acted on when nothing better comes")
            self.assertGreaterEqual(took, 0.55, "it waited up to half the window (0.5 s) past the provisional report")
            self.assertLess(took, 1.0)

            async def other_gap_is_final():
                loop = asyncio.get_running_loop()
                fut = loop.create_future()
                other = module._CompletionFrame(version=3, type=iface.COMPLETION_TYPE_ANSWER, complete=False,
                                                pkt_id=9, frag_total=4, held=frozenset({0, 2, 3}), nonce=0xF0)
                loop.call_later(0.1, fut.set_result, other)
                t0 = time.monotonic()
                got = await iface._await_completion_report(fut, PEER, 9, 4, 0, "raw0", last_sent_idx=3, rearm=lambda: loop.create_future())
                return got, time.monotonic() - t0

            got, took = self.node.run_on_loop(other_gap_is_final(), timeout=10.0)
            self.assertEqual(set(got.held), {0, 2, 3}, "a report with any other gap is final at once")
            self.assertLess(took, 0.4)
        finally:
            iface.direct_raw_report_wait_base_s, iface.direct_raw_report_wait_per_hop_s = saved
            self._reset()

    def test_estimator_is_dropped_with_the_peer_path_stats(self):
        iface = self.iface
        self._reset()
        try:
            iface._rtt_sample(iface._report_rtt, PEER, 2.0)
            iface._clear_peer_path_stats(PEER, "test")
            self.assertNotIn(PEER, iface._report_rtt)
        finally:
            self._reset()


if __name__ == "__main__":
    unittest.main()
