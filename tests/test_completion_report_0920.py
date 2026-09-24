"""
Regression tests for the receiver-initiated completion REPORT (2026-09-20,
module docstring entry "Receiver-initiated completion report"): after a raw
burst the receiver sends the ANSWER unsolicited, the sender waits for it with
its radio quiet, and the QUERY exchange is only the fallback. Pinned here:
the raw-header report flag, the report nonce range that QUERY nonces never
enter, the sender's waiter accepting a matching report (and rejecting a stale
incomplete one), the wait sizing, and -- in the slow two-node scenario -- a
zero-hop raw transfer completing with a report and no QUERY on air.
"""
import asyncio
import os
import tempfile
import unittest

from tests._support import SingleNodeCase, slow, wait_until, build_rns_packet, SimMesh, quiet_rns

PEER = "abcdef012345"


class ReportFlagAndNonces(SingleNodeCase):
    def test_report_flag_rides_byte0_without_disturbing_round_or_version(self):
        iface = self.iface
        plain = iface._encode_raw_fragment(b"x" * 8, "ab" * 32, "cd" * 6, pkt_id=7, frag_idx=0, frag_total=2, attempt=2)
        flagged = iface._encode_raw_fragment(b"x" * 8, "ab" * 32, "cd" * 6, pkt_id=7, frag_idx=1, frag_total=2, attempt=2, report=True)
        self.assertFalse(iface._raw_fragment_report_requested(plain))
        self.assertTrue(iface._raw_fragment_report_requested(flagged))
        header, payload, src, dst = iface._decode_raw_fragment(flagged)
        self.assertEqual((header.attempt, header.frag_idx, header.frag_total, header.pkt_id), (2, 1, 2, 7))
        self.assertEqual(payload, b"x" * 8)
        self.assertEqual(flagged[0] >> 4, iface.RAW_PROTOCOL_VERSION)

    def test_query_nonces_never_enter_the_report_range(self):
        iface = self.iface
        seen = set()
        for _ in range(600):
            iface._completion_query_nonce = (iface._completion_query_nonce % iface.COMPLETION_QUERY_NONCE_MAX) + 1
            seen.add(iface._completion_query_nonce)
        self.assertEqual(min(seen), 1)
        self.assertEqual(max(seen), iface.COMPLETION_QUERY_NONCE_MAX)
        for rnd in range(4):
            self.assertNotIn(iface.COMPLETION_REPORT_NONCE_BASE | rnd, seen)

    def test_report_wait_is_hop_scaled_and_capped_by_the_answer_budget(self):
        iface = self.iface
        iface.direct_raw_report_wait_base_s = 2.0
        iface.direct_raw_report_wait_per_hop_s = 3.0
        iface._query_rtt.pop(PEER, None)
        # base + per_hop x hops (the transit time), capped by the QUERY answer budget
        for hops, base in ((0, 2.0), (1, 5.0)):
            want = min(base, iface._completion_query_timeout_s(PEER, hops))
            self.assertAlmostEqual(iface._completion_report_wait_s(hops, PEER), want)
        budget = iface._completion_query_timeout_s(PEER, 3)
        self.assertLessEqual(iface._completion_report_wait_s(3, PEER), budget)
        iface.direct_raw_report_wait_base_s = 100.0
        self.assertAlmostEqual(iface._completion_report_wait_s(0, PEER), iface._completion_query_timeout_s(PEER, 0))
        iface.direct_raw_report_wait_base_s = 2.0


class ReportResolvesThePreRegisteredWaiter(SingleNodeCase):
    def _report(self, pkt_id, frag_total, complete, held, rnd):
        iface = self.iface
        frame = iface._encode_completion_frame(
            iface.COMPLETION_TYPE_ANSWER, pkt_id, frag_total, complete=complete, held=held,
            version=iface.COMPLETION_PROTOCOL_VERSION, nonce=iface.COMPLETION_REPORT_NONCE_BASE | rnd,
        )
        return frame

    def test_matching_round_report_resolves_and_stale_incomplete_is_discarded(self):
        iface = self.iface

        async def scenario():
            loop = asyncio.get_running_loop()
            original = iface._canonical_peer_prefix
            iface._canonical_peer_prefix = lambda token: PEER   # B is not a contact in the single-node sandbox
            try:
                await body(loop)
            finally:
                iface._canonical_peer_prefix = original

        async def body(loop):
            fut = loop.create_future()
            iface._completion_query_waiters[(PEER, 9)] = (fut, 3, iface.COMPLETION_REPORT_NONCE_BASE | 1)
            # an incomplete report from an EARLIER round: not this burst's answer
            iface._handle_incoming_completion_frame(self._report(9, 3, False, {0}, 0), PEER)
            await asyncio.sleep(0)
            self.assertFalse(fut.done())
            # the current round's incomplete report carries the bitmap
            iface._handle_incoming_completion_frame(self._report(9, 3, False, {0, 2}, 1), PEER)
            await asyncio.sleep(0)
            self.assertTrue(fut.done())
            got = fut.result()
            self.assertFalse(got.complete)
            self.assertEqual(set(got.held), {0, 2})
            # a complete report of any round is accepted (monotone)
            fut2 = loop.create_future()
            iface._completion_query_waiters[(PEER, 9)] = (fut2, 3, iface.COMPLETION_REPORT_NONCE_BASE | 2)
            iface._handle_incoming_completion_frame(self._report(9, 3, True, {0, 1, 2}, 0), PEER)
            await asyncio.sleep(0)
            self.assertTrue(fut2.done() and fut2.result().complete)
            iface._completion_query_waiters.pop((PEER, 9), None)

        self.node.run_on_loop(scenario(), timeout=10.0)

    def test_await_report_returns_none_after_the_window(self):
        iface = self.iface
        iface.direct_raw_report_wait_base_s = 0.2

        async def scenario():
            fut = asyncio.get_running_loop().create_future()
            got = await iface._await_completion_report(fut, PEER, 5, 2, 0, stage="raw0")
            self.assertIsNone(got)
            self.assertFalse(fut.done(), "the caller's future must survive the timeout (shielded)")

        self.node.run_on_loop(scenario(), timeout=10.0)
        iface.direct_raw_report_wait_base_s = 2.0


class LastTwoFragmentsCarryTheFlag(SingleNodeCase):
    """The flag rides the last two fragments of a burst, so a lost last
    fragment still yields a report from the second-last; the receiver
    reports its bitmap on any flagged fragment that leaves gaps."""

    def test_flagged_incomplete_fragment_reports_the_bitmap(self):
        iface = self.iface
        sent = []
        original = iface._send_completion_report
        iface._send_completion_report = lambda *a, **k: sent.append((a, k))
        try:
            h0 = self.module._FrameHeader(iface.PROTOCOL_VERSION, True, False, 41, 0, 3, 0)
            h1 = self.module._FrameHeader(iface.PROTOCOL_VERSION, True, False, 41, 1, 3, 0)
            key = iface._reassembly_key(h0, PEER, mode="direct")
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h0, b"a" * 10, PEER, raw=True, report_requested=False))
            self.assertEqual(sent, [], "an unflagged fragment must not report")
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h1, b"b" * 10, PEER, raw=True, report_requested=True))
            # Phase 3 M1 (2026-09-20): the gaps report is HELD for one fragment
            # airtime (the last fragment is usually right behind) and sent
            # only if the bucket is still incomplete when the hold ends.
            self.assertEqual(sent, [], "the gaps report is not sent at once (M1 debounce)")
            hold = iface._report_hold_s(10 + iface.RAW_HEADER_SIZE, 0)
            self.assertTrue(wait_until(lambda: len(sent) == 1, hold + 2.0), "the gaps report goes out after the hold")
            args, kwargs = sent[-1]
            self.assertEqual(args[0], PEER)
            self.assertFalse(kwargs.get("complete"))
            self.assertEqual(kwargs.get("held"), {0, 1})
        finally:
            iface._send_completion_report = original
            iface._reassembly.pop(key, None)

    def test_completion_reports_even_without_the_flag(self):
        """Alpha 0.1.5 (2b): an UNFLAGGED completion still reports, but only
        after the sender's fragments have stopped arriving for
        `_report_hold_s(..., arriving=True)` -- mid-window reports were what
        collided with the sender's own queued burst in the 2026-09-21 field
        session. The report still says complete."""
        iface = self.iface
        sent = []
        original = iface._send_completion_report
        iface._send_completion_report = lambda *a, **k: sent.append((a, k))
        try:
            h0 = self.module._FrameHeader(iface.PROTOCOL_VERSION, True, False, 42, 0, 2, 0)
            h1 = self.module._FrameHeader(iface.PROTOCOL_VERSION, True, False, 42, 1, 2, 0)
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h0, b"a" * 10, PEER, raw=True))
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h1, b"b" * 10, PEER, raw=True))
            self.assertEqual(sent, [], "an unflagged completion is held while the window may still be arriving")
            hold = iface._report_hold_s(10 + iface.RAW_HEADER_SIZE, 0, arriving=True)
            self.assertTrue(wait_until(lambda: len(sent) == 1, hold + 2.0), "the held report goes out after the silence")
            self.assertTrue(sent[-1][1].get("complete"))
            self.assertAlmostEqual(sent[-1][1].get("held_s"), hold, places=3)
        finally:
            iface._send_completion_report = original
            iface._cancel_sender_report(PEER)

    def test_sender_flags_the_last_two_fragments_of_a_burst(self):
        iface = self.iface
        frames = []
        for n, frag_idx in enumerate([0, 1, 2, 3]):
            frames.append(iface._encode_raw_fragment(b"x", "ab" * 32, "cd" * 6, 5, frag_idx, 4, attempt=0,
                                                     report=n >= 4 - 2))
        self.assertEqual([iface._raw_fragment_report_requested(f) for f in frames], [False, False, True, True])


RAW_CFG = {
    "direct_raw_fragments_enabled": "yes",
    "peer_discovery_target_peers": "1",
    "packet_capture_enabled": "yes",
    "debug_level": "info",
}


def _events(node, name):
    return [r for r in node.capture_records() if r.get("event") == name]


@slow
class ZeroHopRawTransferIsReportedNotQueried(unittest.TestCase):
    def tearDown(self):
        self.mesh.stop()

    def test_report_replaces_the_query(self):
        quiet_rns()
        mesh = SimMesh(["A-B"], seed=61, capture_dir=tempfile.mkdtemp(prefix="smci-report-cap-"))
        self.mesh = mesh
        for n in ("A", "B"):
            mesh.add_node(n, config=RAW_CFG)
        mesh.advert_all()
        self.assertTrue(mesh.wait_contacts(40.0))
        self.assertTrue(mesh.wait_bound(40.0))
        self.assertTrue(mesh.wait_resolved(60.0))
        a, b = mesh.nodes["A"], mesh.nodes["B"]
        b.send(build_rns_packet("announce", dest_hash=b.dest_hash, payload=b"prime"))   # item 3 of 0.1.7: an announce teaches the token
        self.assertTrue(wait_until(lambda: b.dest_hash in a.iface._rns_token_peer, 30.0))
        big = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"report-" + os.urandom(440))
        a.send(big)
        self.assertTrue(wait_until(lambda: big in b.owner.received, 40.0), "raw transfer never delivered")
        self.assertTrue(wait_until(lambda: any(r.get("outcome") == "reported" for r in _events(a, "completion_check_result")), 15.0),
                        "the sender never recorded a completion report")
        self.assertEqual(sum(1 for r in _events(a, "direct_attempt_result") if r.get("kind") == "completion_query"), 0,
                         "a QUERY went on air although the receiver reported")
        reports = _events(b, "completion_report_sent")
        self.assertTrue(reports and reports[-1]["complete"])


if __name__ == "__main__":
    unittest.main()
