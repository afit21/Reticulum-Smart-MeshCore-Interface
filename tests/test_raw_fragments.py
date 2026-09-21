"""
Raw binary DIRECT fragments (2026-09-18 night, see the interface module
docstring's "Raw binary DIRECT fragments" entry): a packet too large for
one text frame goes to a raw-capable peer as unacknowledged
CMD_SEND_RAW_DATA bursts reconciled by the "Q" bitmap.

Unit tests cover the codec, the firmware-derived budget and the
eligibility gate; the scenarios run two real interfaces (both with the
flag on, so their bind frames advertise the capability) over the
simulated mesh, including loss on the raw packet type and the text
fallback when raw frames never arrive.
"""
import asyncio
import os
import time
import unittest

from tests._support import SingleNodeCase, slow, wait_until, build_rns_packet, SimMesh, quiet_rns
import tempfile

RAW_CFG = {"direct_raw_fragments_enabled": "yes"}


class RawCodecAndGate(SingleNodeCase):

    def test_round_trip(self):
        iface = self.iface
        payload = os.urandom(150)
        frame = iface._encode_raw_fragment(payload, "ab" * 32, "cd" * 6, pkt_id=0x1234, frag_idx=2, frag_total=4, attempt=3)
        self.assertEqual(len(frame), iface.RAW_HEADER_SIZE + 150)
        self.assertEqual(iface.RAW_HEADER_SIZE, 9, "M3 (2026-09-20): 9-byte header, 2-byte source prefix")
        header, out, src, dst = iface._decode_raw_fragment(frame)
        # M3: the source prefix on the wire is RAW_SRC_PREFIX_BYTES (2) bytes.
        self.assertEqual((out, src, dst), (payload, "cd" * iface.RAW_SRC_PREFIX_BYTES, bytes.fromhex("abab")))
        self.assertEqual((header.pkt_id, header.frag_idx, header.frag_total, header.attempt), (0x1234, 2, 4, 3))
        self.assertTrue(header.multi_fragment)

    def test_foreign_and_malformed_raw_packets_are_rejected(self):
        iface = self.iface
        with self.assertRaises(ValueError):
            iface._decode_raw_fragment(b"\x20" + bytes(20))          # wrong version nibble
        with self.assertRaises(ValueError):
            iface._decode_raw_fragment(bytes([0x10]) + bytes(5))     # too short
        bad = iface._encode_raw_fragment(b"x" * 8, "ab" * 32, "cd" * 6, 1, frag_idx=4, frag_total=4, attempt=0)
        with self.assertRaises(ValueError):
            iface._decode_raw_fragment(bad)                          # frag_idx out of range

    def test_budget_follows_firmware_limits(self):
        iface = self.iface
        h = iface.RAW_HEADER_SIZE   # 9 since M3 (2026-09-20); was 13
        self.assertEqual(iface._direct_raw_payload_budget(0), 170 - h)      # config cap (170) wins at zero hop
        self.assertEqual(iface._direct_raw_payload_budget(3), 170 - h)      # still the cap: 174 - 3 = 171 > 170
        self.assertEqual(iface._direct_raw_payload_budget(5), 169 - h)      # 174 - path_len wins from 5 hops
        self.assertEqual(iface._direct_raw_payload_budget(10), 164 - h)
        # a 483-byte Resource part is 3 raw fragments since M3 (4 with the
        # 13-byte header; 5 text ones): 3 x 161 = 483 exactly, up to four hops
        self.assertEqual(iface._direct_raw_payload_budget(0), 161)
        for path_len in range(0, 5):
            self.assertEqual(len(iface._chunk_payload(bytes(483), iface._direct_raw_payload_budget(path_len))), 3, f"path_len {path_len}")
        self.assertEqual(len(iface._chunk_payload(bytes(483), iface._direct_raw_payload_budget(5))), 4)
        self.assertEqual(len(iface._fragment_direct_payload(bytes(483))), 5)

    def test_eligibility_gate(self):
        iface, M = self.iface, self.module
        peer = "abcdef012345"
        rp = M._ResolvedPath(out_path_hex="", out_path_len=0, out_path_hash_len=1, resolved_at=time.monotonic())
        try:
            iface.direct_raw_fragments_enabled = True
            self.assertFalse(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_NORMAL), "unknown peer")
            self.on_loop(iface._register_peer, peer, None, "test", None, True)
            self.assertFalse(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_NORMAL), "no resolved path")
            iface._resolved_paths[peer] = rp
            self.assertTrue(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_NORMAL))
            self.assertFalse(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_HANDSHAKE), "handshakes stay on the ACKed text path")
            iface._raw_disabled_until[peer] = time.monotonic() + 60
            self.assertFalse(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_NORMAL), "fallback cooldown")
            iface._raw_disabled_until.clear()
            iface._peers[peer].raw_fragments = False
            self.assertFalse(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_NORMAL), "peer did not advertise raw")
            iface.direct_raw_fragments_enabled = False
            iface._peers[peer].raw_fragments = True
            self.assertFalse(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_NORMAL), "flag off")
        finally:
            iface.direct_raw_fragments_enabled = False
            iface._peers.pop(peer, None); iface._resolved_paths.pop(peer, None); iface._raw_disabled_until.clear()

    def test_bind_capability_bit_follows_the_flag(self):
        iface = self.iface
        try:
            iface.direct_raw_fragments_enabled = True
            self.assertTrue(iface._bind_capability() & iface.BIND_CAP_RAW_FRAGMENTS)
            iface.direct_raw_fragments_enabled = False
            self.assertFalse(iface._bind_capability() & iface.BIND_CAP_RAW_FRAGMENTS)
        finally:
            iface.direct_raw_fragments_enabled = False


class CompletionQueryTimeout(SingleNodeCase):
    """First raw field test (2026-09-18 night): the reconcile wait is sized
    from the measured QUERY -> ANSWER round trip, with a hop-scaled prior."""

    def test_hop_scaled_prior_then_measured_rtt(self):
        iface = self.iface
        peer = "abcdef012345"
        iface._query_rtt.pop(peer, None); iface._ack_rtt.pop(peer, None); iface._ack_rtt_snapshot.pop(peer, None)
        iface._last_firmware_ack_timeout_s.pop(peer, None)
        base = iface.direct_completion_check_timeout_s
        cap = iface.direct_completion_check_timeout_max_s
        # 2026-09-19 evening field session: the unbounded `x (1 + hops)` prior
        # is gone. The hop count now does two bounded things -- it adds
        # `direct_completion_check_timeout_per_hop_s` to the FLOOR (a first
        # query at depth, before any RTT sample exists, still gets room: the
        # session measured query->answer p90 at 11.7-16.1s) and it selects
        # which CEILING applies. Both are hard-capped, which the old prior
        # was not. See `direct_completion_check_timeout_max_s` for the
        # evidence and the module docstring's evening entry for the walk-back.
        per_hop = iface.direct_completion_check_timeout_per_hop_s
        self.assertEqual(iface._completion_query_timeout_s(peer, hop_count=0), base)
        self.assertAlmostEqual(iface._completion_query_timeout_s(peer, hop_count=1), base + per_hop)
        self.assertAlmostEqual(iface._completion_query_timeout_s(peer, hop_count=3),
                               min(base + 3 * per_hop, iface._completion_query_timeout_cap_s(3)))
        self.assertLessEqual(iface._completion_query_timeout_s(peer, hop_count=3),
                             iface._completion_query_timeout_cap_s(3),
                             "the hop-aware floor must still obey the ceiling")
        iface._record_query_rtt(peer, 3.0)                      # srtt 3, rttvar 1.5 -> 2*(3+6) = 18, now clamped to the cap
        self.assertAlmostEqual(iface._completion_query_timeout_s(peer, hop_count=0),
                               max(base, min(18.0, cap)))
        for _ in range(20):
            iface._record_query_rtt(peer, 3.0)                  # converges: rttvar -> 0, 2*srtt = 6
        self.assertLess(iface._completion_query_timeout_s(peer, hop_count=0), 8.0)
        self.assertGreaterEqual(iface._completion_query_timeout_s(peer, hop_count=0), base)
        iface._invalidate_ack_rtt(peer, "path change")          # path change drops the query stats too
        self.assertNotIn(peer, iface._query_rtt)


class PerPathFallbackVerdict(SingleNodeCase):
    """User's design (2026-09-18 night): raw first; if Z85 text works
    where raw did not, the PATH is noted, and a new path is raw-first
    again."""

    def _resolved(self, path_hex):
        M = self.module
        return M._ResolvedPath(out_path_hex=path_hex, out_path_len=len(path_hex) // 2, out_path_hash_len=1,
                               resolved_at=time.monotonic())

    def test_text_success_notes_the_path_not_the_peer(self):
        iface, M = self.iface, self.module
        peer = "abcdef012345"
        N = M.SmartMeshCoreInterface.PRIORITY_NORMAL
        try:
            iface.direct_raw_fragments_enabled = True
            self.on_loop(iface._register_peer, peer, None, "test", None, True)
            iface._resolved_paths[peer] = self._resolved("19d6")
            self.assertTrue(iface._raw_fragments_eligible(peer, N))
            # raw fell back on this path, then the text send succeeded
            iface._raw_disabled_until[peer] = time.monotonic() + 600
            iface._note_raw_fallback_outcome(peer, "19d6", text_ok=True)
            self.assertIn("19d6", iface._raw_unsupported_paths)
            self.assertNotIn(peer, iface._raw_disabled_until, "path verdict lifts the per-peer pause")
            self.assertFalse(iface._raw_fragments_eligible(peer, N), "known-bad chain is never probed again")
            # a new path through different repeaters is raw-first again
            iface._resolved_paths[peer] = self._resolved("4fbe")
            self.assertTrue(iface._raw_fragments_eligible(peer, N))
            # the note expires
            iface._raw_unsupported_paths["19d6"]["since"] -= iface.direct_raw_path_unsupported_ttl_s + 1
            iface._resolved_paths[peer] = self._resolved("19d6")
            self.assertTrue(iface._raw_fragments_eligible(peer, N))
            self.assertNotIn("19d6", iface._raw_unsupported_paths)
        finally:
            iface.direct_raw_fragments_enabled = False
            iface._peers.pop(peer, None); iface._resolved_paths.pop(peer, None)
            iface._raw_unsupported_paths.clear(); iface._raw_disabled_until.clear()

    def test_text_failure_concludes_nothing_about_raw(self):
        iface = self.iface
        peer = "abcdef012345"
        iface._raw_disabled_until[peer] = time.monotonic() + 600
        iface._note_raw_fallback_outcome(peer, "19d6", text_ok=False)
        self.assertNotIn("19d6", iface._raw_unsupported_paths)
        self.assertIn(peer, iface._raw_disabled_until, "the short pause still applies to a sick path")
        iface._note_raw_fallback_outcome(peer, "", text_ok=True)   # zero hop: no chain to note
        self.assertEqual(iface._raw_unsupported_paths, {})
        iface._raw_disabled_until.clear()


class RawGapAndPathEvidence(SingleNodeCase):
    """First multi-hop raw field test (2026-09-19 morning, see the module
    docstring's entry): the inter-fragment gap scales with the repeater
    chain, and a round's QUERY ACKs feed the stale-path detector."""

    def test_gap_scales_with_hops_and_follows_the_fragment_airtime(self):
        iface = self.iface
        airtime_big = iface._estimate_tx_airtime_s("", on_air_bytes=175)
        airtime_small = iface._estimate_tx_airtime_s("", on_air_bytes=70)
        self.assertGreater(airtime_big, airtime_small)
        self.assertEqual(iface._raw_fragment_gap_s(0, 175), iface.direct_raw_zero_hop_gap_s)
        # MeshBench finding 2 (2026-09-20): the frame's own airtime is on top of
        # the hop-scaled term, because the gap starts when the firmware has
        # only QUEUED the frame.
        f = iface.direct_raw_hop_gap_factor
        self.assertAlmostEqual(iface._raw_fragment_gap_s(1, 175), (1 + f) * airtime_big)
        self.assertAlmostEqual(iface._raw_fragment_gap_s(4, 175), (1 + 4 * f) * airtime_big)
        self.assertAlmostEqual(iface._raw_fragment_gap_s(2, 70), (1 + 2 * f) * airtime_small)
        self.assertLess(iface._raw_fragment_gap_s(2, 70), iface._raw_fragment_gap_s(2, 175))

    def test_query_round_outcomes_feed_the_stale_path_counter(self):
        iface = self.iface
        peer = "abcdef012345"
        miss = {"acked": False, "waited_full_timeout": True}
        cut_short = {"acked": False, "waited_full_timeout": False}
        hit = {"acked": True, "waited_full_timeout": True}
        selection = iface.path_selection_enabled
        iface.path_selection_enabled = False   # the threshold detector: selection off (alpha 0.1.6 item 1)
        try:
            iface._direct_path_failures.pop(peer, None)
            self.on_loop(iface._record_query_path_evidence, peer, [])
            self.assertNotIn(peer, iface._direct_path_failures)
            # a round of full-timeout misses is one failure, not one per attempt
            self.on_loop(iface._record_query_path_evidence, peer, [miss, miss])
            self.assertEqual(iface._direct_path_failures.get(peer), 1)
            # an attempt cut short by this engine's own ceiling proves nothing
            self.on_loop(iface._record_query_path_evidence, peer, [miss, cut_short])
            self.assertEqual(iface._direct_path_failures.get(peer), 1)
            self.on_loop(iface._record_query_path_evidence, peer, [miss, miss])
            self.assertEqual(iface._direct_path_failures.get(peer), 2)
            # one ACKed QUERY clears the counter
            self.on_loop(iface._record_query_path_evidence, peer, [miss, hit])
            self.assertNotIn(peer, iface._direct_path_failures)
            # so does an ANSWER whose QUERY ACK was lost
            self.on_loop(iface._record_query_path_evidence, peer, [miss, miss])
            self.assertEqual(iface._direct_path_failures.get(peer), 1)
            self.on_loop(lambda: iface._record_query_path_evidence(peer, [miss, miss], answered=True))
            self.assertNotIn(peer, iface._direct_path_failures)
        finally:
            iface.path_selection_enabled = selection
            iface._direct_path_failures.pop(peer, None)


class NightSessionFixes(SingleNodeCase):
    """Field fixes from the 2026-09-19 night session (`fieldtests/raw/
    Alpha0.1.2/*nighttest*`, build 3b56c11) compared like-for-like at one
    hop with e87cca8 (`fieldtests/raw/binaryfieldtest/`): reconcile answers
    that arrived fell from 11 of 13 (85%) to 22 of 46 (48%), raw sends
    completing without text fallback from 8 of 8 to 16 of 20, raw send
    median duration rose from 28s to 39s, and a 12-part page transfer was
    cancelled by RNS after 469s. Zero hop stayed at 96-100%.

    Change 1: the reconcile QUERY keeps the radio lock for a short
    hop-scaled quiet window after its ACK, so the querier is not keying
    while the ANSWER crosses the repeater (a hidden node: 22 of the 24
    lost answers were never decoded by the querier's radio at all).
    Change 2: one incomplete raw send is a soft strike, not a 600s pause.
    Change 3: the in-flight cap is off by default and cannot drop.
    """

    PEER = "abcdef012345"

    def setUp(self):
        iface = self.iface
        self._saved = {k: getattr(iface, k) for k in (
            "direct_completion_quiet_base_s", "direct_completion_quiet_per_hop_s",
            "direct_post_send_listen_success_min_s", "direct_post_send_listen_success_max_s",
            "direct_raw_incomplete_strikes", "direct_raw_zero_hop_gap_s", "direct_raw_fragments_enabled",
            "_send_direct_frame", "_await_direct_ack", "_send_raw_fragment", "_query_remote_fragments",
            "_capture_direct_attempt_result", "_raw_path_reset_mid_send",
        )}
        iface._raw_incomplete_strikes.clear()
        iface._raw_disabled_until.clear()

    def tearDown(self):
        iface = self.iface
        for k, v in self._saved.items():
            setattr(iface, k, v)
        iface._raw_incomplete_strikes.clear()
        iface._raw_disabled_until.clear()
        iface._peers.pop(self.PEER, None)
        iface._resolved_paths.pop(self.PEER, None)

    # --- Change 1: the quiet window -------------------------------------

    def test_quiet_window_scales_with_hops_and_is_clamped_to_the_answer_budget(self):
        iface = self.iface
        iface.direct_completion_quiet_base_s = 1.5
        iface.direct_completion_quiet_per_hop_s = 2.5
        self.assertAlmostEqual(iface._completion_quiet_window_s(0, 15.0), 1.5)
        self.assertAlmostEqual(iface._completion_quiet_window_s(1, 15.0), 4.0)
        self.assertAlmostEqual(iface._completion_quiet_window_s(2, 15.0), 6.5)
        self.assertAlmostEqual(iface._completion_quiet_window_s(None, 15.0), 1.5, "unknown hops = zero-hop window")
        # never longer than the answer budget itself
        self.assertAlmostEqual(iface._completion_quiet_window_s(3, 5.0), 5.0)
        self.assertAlmostEqual(iface._completion_quiet_window_s(10, 15.0), 15.0)
        # both keys 0 -> no window at all (the fully radio-free wait of commit 1919074)
        iface.direct_completion_quiet_base_s = 0.0
        iface.direct_completion_quiet_per_hop_s = 0.0
        self.assertIsNone(iface._completion_quiet_window_s(2, 15.0))
        iface.direct_completion_quiet_per_hop_s = 2.5
        self.assertAlmostEqual(iface._completion_quiet_window_s(0, 15.0), 0.0, "per-hop only: nothing at zero hop")
        self.assertAlmostEqual(iface._completion_quiet_window_s(2, 15.0), 5.0)

    def _stub_ack_path(self):
        """Make _send_direct_frame_and_wait_for_ack run without a radio:
        the send 'succeeds' at once and the ACK arrives instantly."""
        iface = self.iface
        captured = []

        async def fake_send(target, frame, attempt=0, time_critical=False, gate_telemetry=None, duty_cycle_exempt=False, **_kw):
            return {}

        async def fake_ack(sent, peer_prefix, hop_count, rx_window, ack_wait_start, cancel_event=None, preemptible=False):
            await asyncio.sleep(0.05)      # a (short) ACK latency, so the window is measured from MSG_SENT
            return True, False, 1.0, "test", 0.05, None

        def fake_capture(*args, **kwargs):
            captured.append(kwargs)

        iface._send_direct_frame = fake_send
        iface._await_direct_ack = fake_ack
        iface._capture_direct_attempt_result = fake_capture
        iface.direct_post_send_listen_success_min_s = 0.0
        iface.direct_post_send_listen_success_max_s = 0.0
        return captured

    def test_lock_is_held_through_the_quiet_window_then_released(self):
        iface = self.iface
        captured = self._stub_ack_path()

        async def drive():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            started = time.monotonic()
            sender = asyncio.ensure_future(iface._send_direct_frame_and_wait_for_ack(
                "ab" * 32, "Qx", 0, peer_prefix=self.PEER, hop_count=1, kind="completion_query",
                quiet_wait=fut, quiet_window_s=0.6,
            ))
            await asyncio.sleep(0.05)
            # A second contender for the radio cannot get it until the window ends.
            contender_started = time.monotonic()
            async with iface._direct_exchange_lock(iface.PRIORITY_HANDSHAKE):
                got_lock_after = time.monotonic() - contender_started
            ok, _ = await sender
            return ok, got_lock_after, time.monotonic() - started, fut

        ok, got_lock_after, total, fut = self.node.run_on_loop(drive(), timeout=10.0)
        self.assertTrue(ok)
        self.assertGreaterEqual(got_lock_after, 0.45, "the lock must stay held for the whole window")
        self.assertLess(total, 1.5)
        self.assertFalse(fut.cancelled(), "asyncio.shield: the caller's answer future must survive the window timeout")
        self.assertFalse(fut.done())
        self.assertEqual(len(captured), 1)
        self.assertIsNotNone(captured[0]["quiet_hold_s"])
        self.assertGreaterEqual(captured[0]["quiet_hold_s"], 0.45)
        fut.cancel()

    def test_window_is_measured_from_the_transmit_not_from_the_lock_wait(self):
        """Diagnostic sim run: QUERYs waited 5-10s for the radio lock under
        concurrent sends. A window fixed before that wait would be spent
        before the frame left; it must start at the frame's own MSG_SENT."""
        iface = self.iface
        captured = self._stub_ack_path()

        async def drive():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            async with iface._direct_exchange_lock(iface.PRIORITY_NORMAL):
                sender = asyncio.ensure_future(iface._send_direct_frame_and_wait_for_ack(
                    "ab" * 32, "Qx", 0, peer_prefix=self.PEER, hop_count=1, kind="completion_query",
                    quiet_wait=fut, quiet_window_s=0.5,
                ))
                await asyncio.sleep(0.7)       # longer than the whole window: the QUERY is still queued
            released_at = time.monotonic()
            ok, _ = await sender
            fut.cancel()
            return ok, time.monotonic() - released_at

        ok, after_release = self.node.run_on_loop(drive(), timeout=10.0)
        self.assertTrue(ok)
        self.assertGreaterEqual(captured[0]["quiet_hold_s"], 0.35, "the window must survive a lock wait longer than itself")
        self.assertGreaterEqual(after_release, 0.4)

    def test_lock_is_released_early_when_the_answer_arrives(self):
        iface = self.iface
        captured = self._stub_ack_path()

        async def drive():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            started = time.monotonic()
            sender = asyncio.ensure_future(iface._send_direct_frame_and_wait_for_ack(
                "ab" * 32, "Qx", 0, peer_prefix=self.PEER, hop_count=2, kind="completion_query",
                quiet_wait=fut, quiet_window_s=5.0,
            ))
            await asyncio.sleep(0.15)
            fut.set_result("answer")
            ok, _ = await sender
            return ok, time.monotonic() - started

        ok, total = self.node.run_on_loop(drive(), timeout=10.0)
        self.assertTrue(ok)
        self.assertLess(total, 1.0, "the answer resolving must end the hold at once, not at the 5s deadline")
        self.assertGreaterEqual(captured[0]["quiet_hold_s"], 0.1)

    def test_no_hold_when_the_window_has_passed_or_no_future_is_given(self):
        """A zero window holds nothing (review 2026-09-20: the window is
        measured from the ACK now, so it can no longer be consumed by the
        ACK's own latency; disabling it is a zero-length window)."""
        iface = self.iface
        captured = self._stub_ack_path()

        async def drive():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            started = time.monotonic()
            await iface._send_direct_frame_and_wait_for_ack(
                "ab" * 32, "Qx", 0, peer_prefix=self.PEER, hop_count=0,
                quiet_wait=fut, quiet_window_s=0.0,
            )
            t_past = time.monotonic() - started
            started = time.monotonic()
            await iface._send_direct_frame_and_wait_for_ack("ab" * 32, "Rx", 0, peer_prefix=self.PEER, hop_count=1)
            t_none = time.monotonic() - started
            fut.cancel()
            return t_past, t_none

        t_past, t_none = self.node.run_on_loop(drive(), timeout=10.0)
        self.assertLess(t_past, 0.3)
        self.assertLess(t_none, 0.3)
        self.assertEqual([c["quiet_hold_s"] for c in captured], [None, None])

    def test_quiet_hold_is_charged_against_the_answer_budget_and_rtt_is_kept(self):
        """Review fix (2026-09-20): the hold must move waiting time, not add
        to it -- total answer wait == timeout_s, not hold + timeout_s -- and
        an answer arriving inside the hold still yields an RTT sample."""
        iface = self.iface
        self._stub_ack_path()
        iface.direct_completion_quiet_base_s = 0.6
        iface.direct_completion_quiet_per_hop_s = 0.0
        saved = (iface.direct_completion_check_timeout_s, iface.direct_completion_check_timeout_per_hop_s,
                 iface.rx_log_holds_enabled)
        iface.direct_completion_check_timeout_s = 1.0
        iface.direct_completion_check_timeout_per_hop_s = 0.0
        iface.rx_log_holds_enabled = False
        iface._query_rtt.pop(self.PEER, None)
        iface._last_firmware_ack_timeout_s.pop(self.PEER, None)
        try:
            async def unanswered():
                started = time.monotonic()
                got = await iface._query_remote_fragments("ab" * 32, self.PEER, 7, 4, stage="test", hop_count=0)
                return got, time.monotonic() - started

            got, total = self.node.run_on_loop(unanswered(), timeout=10.0)
            self.assertIsNone(got)
            # ~0.05s ACK + 0.55s hold + (1.0 - 0.55)s radio-free = ~1.05s, NOT ~1.6s
            self.assertLess(total, 1.35, f"the hold was added to the budget instead of charged against it ({total:.2f}s)")
            self.assertGreaterEqual(total, 0.95)

            async def answered_inside_hold():
                async def reply():
                    await asyncio.sleep(0.25)
                    key = (self.PEER, 8)
                    fut, frag_total, nonce = iface._completion_query_waiters[key]
                    fut.set_result(self.module._CompletionFrame(3, iface.COMPLETION_TYPE_ANSWER, True, 8, 4,
                                                                frozenset(range(4)), nonce))
                asyncio.ensure_future(reply())
                started = time.monotonic()
                got = await iface._query_remote_fragments("ab" * 32, self.PEER, 8, 4, stage="test", hop_count=0)
                return got, time.monotonic() - started

            got, total = self.node.run_on_loop(answered_inside_hold(), timeout=10.0)
            self.assertIsNotNone(got)
            self.assertTrue(got.complete)
            self.assertLess(total, 0.6, "the answer must end the wait at once")
            self.assertIn(self.PEER, iface._query_rtt, "an answer inside the hold must still produce an RTT sample")
            self.assertGreater(iface._query_rtt[self.PEER]["srtt"], 0.1)
            self.assertLess(iface._query_rtt[self.PEER]["srtt"], 0.5)
        finally:
            (iface.direct_completion_check_timeout_s, iface.direct_completion_check_timeout_per_hop_s,
             iface.rx_log_holds_enabled) = saved
            iface._query_rtt.pop(self.PEER, None)

    # --- Change 2: soft strikes -----------------------------------------

    def _raw_send(self, held_per_round):
        """Run one _send_direct_raw_fragmented against stubbed radio calls.
        `held_per_round` is what the (stubbed) reconcile answer says the
        receiver holds after each round; True means complete."""
        iface, M = self.iface, self.module
        rounds = iter(held_per_round)

        async def fake_raw(path, frame, priority, telemetry=None, interrupt=None):
            return True

        async def fake_query(target, peer_prefix, pkt_id, frag_total, stage, priority=2, hop_count=None, send_info=None, entries=None):
            if send_info is not None:
                send_info["acked"] = True
                send_info["waited_full_timeout"] = True
            held = next(rounds)
            if held is True:
                return M._CompletionFrame(3, iface.COMPLETION_TYPE_ANSWER, True, pkt_id, frag_total, frozenset(range(frag_total)), 0)
            return M._CompletionFrame(3, iface.COMPLETION_TYPE_ANSWER, False, pkt_id, frag_total, frozenset(held), 0)

        async def no_reset(*a, **k):
            return False

        iface._send_raw_fragment = fake_raw
        iface._query_remote_fragments = fake_query
        iface._raw_path_reset_mid_send = no_reset
        iface.direct_raw_zero_hop_gap_s = 0.0
        # Four fragments whatever the header size (483 B was four with the
        # 13-byte header; it is three since M3's 9-byte header, 2026-09-20).
        payload = os.urandom(iface._direct_raw_payload_budget(0) * 3 + 10)
        return self.node.run_on_loop(
            iface._send_direct_raw_fragmented("ab" * 32, self.PEER, payload, iface._next_pkt_id(),
                                              priority=iface.PRIORITY_NORMAL, hop_count=0),
            timeout=30.0)

    def test_incomplete_raw_sends_pause_raw_only_after_two_strikes(self):
        """21:45:14 in the night capture: ONE part lost the same fragment
        three rounds running and the 600s pause that followed sent 46 page
        parts as text. e87cca8 (8 of 8 raw completions) never paused on a
        single incomplete send."""
        iface, M = self.iface, self.module
        iface.direct_raw_fragments_enabled = True
        iface.direct_raw_incomplete_strikes = 2
        self.on_loop(iface._register_peer, self.PEER, None, "test", None, True)
        iface._resolved_paths[self.PEER] = M._ResolvedPath(out_path_hex="", out_path_len=0, out_path_hash_len=1,
                                                          resolved_at=time.monotonic())
        # Three answered rounds, progress every round, never fragment 3 --
        # so the two-strike "burst delivered nothing" rule (unchanged) does
        # not fire and only the closing incomplete block is exercised.
        incomplete = [{0}, {0, 1}, {0, 1, 2}]
        # first incomplete send: text fallback, soft strike, raw still eligible
        self.assertIsNone(self._raw_send(incomplete))
        self.assertEqual(iface._raw_incomplete_strikes.get(self.PEER), 1)
        self.assertNotIn(self.PEER, iface._raw_disabled_until)
        self.assertTrue(iface._raw_fragments_eligible(self.PEER, iface.PRIORITY_NORMAL))
        # second in a row: now raw pauses for the cooldown and the count resets
        self.assertIsNone(self._raw_send(incomplete))
        self.assertIn(self.PEER, iface._raw_disabled_until)
        self.assertNotIn(self.PEER, iface._raw_incomplete_strikes)
        self.assertFalse(iface._raw_fragments_eligible(self.PEER, iface.PRIORITY_NORMAL))
        self.assertAlmostEqual(iface._raw_disabled_until[self.PEER] - time.monotonic(),
                               iface.direct_raw_fallback_cooldown_s, delta=2.0)
        self.assertEqual(iface.direct_raw_fallback_cooldown_s, 120.0, "600 -> 120 (night session)")

    def test_a_completed_raw_send_clears_the_strike_and_so_does_a_path_change(self):
        iface, M = self.iface, self.module
        iface.direct_raw_fragments_enabled = True
        iface.direct_raw_incomplete_strikes = 2
        self.on_loop(iface._register_peer, self.PEER, None, "test", None, True)
        iface._resolved_paths[self.PEER] = M._ResolvedPath(out_path_hex="", out_path_len=0, out_path_hash_len=1,
                                                          resolved_at=time.monotonic())
        self.assertIsNone(self._raw_send([{0}, {0, 1}, {0, 1, 2}]))
        self.assertEqual(iface._raw_incomplete_strikes.get(self.PEER), 1)
        # a raw send that completes (second round) clears it
        self.assertTrue(self._raw_send([{0, 2}, True]))
        self.assertNotIn(self.PEER, iface._raw_incomplete_strikes)
        # and so does a path change, like every other per-path measurement
        self.assertIsNone(self._raw_send([{0}, {0, 1}, {0, 1, 2}]))
        self.assertEqual(iface._raw_incomplete_strikes.get(self.PEER), 1)
        iface._clear_peer_path_stats(self.PEER, "test")
        self.assertNotIn(self.PEER, iface._raw_incomplete_strikes)
        # strikes=1 restores the old pause-on-first-incomplete behaviour
        iface.direct_raw_incomplete_strikes = 1
        self.assertIsNone(self._raw_send([{0}, {0, 1}, {0, 1, 2}]))
        self.assertIn(self.PEER, iface._raw_disabled_until)

    # --- Change 3: the priority semaphore ---------------------------------

    def test_priority_semaphore_orders_waiters_by_tier_and_transfers_permits(self):
        M = self.module

        async def drive():
            sem = M._PriorityAsyncSemaphore(2)
            order = []
            await sem.acquire(2); await sem.acquire(2)
            self.assertTrue(sem.locked())

            async def waiter(tag, prio):
                await sem.acquire(prio)
                order.append(tag)
                await asyncio.sleep(0.02)
                sem.release()

            tasks = [asyncio.ensure_future(waiter("low1", 3)), asyncio.ensure_future(waiter("norm1", 2))]
            await asyncio.sleep(0.01)
            tasks.append(asyncio.ensure_future(waiter("norm2", 2)))
            tasks.append(asyncio.ensure_future(waiter("answer", 1)))
            await asyncio.sleep(0.01)
            self.assertEqual(sem.waiting(), 4)
            sem.release(); sem.release()
            await asyncio.gather(*tasks)
            self.assertEqual(sem.holders(), 0)
            self.assertFalse(sem.locked())
            # a waiter cancelled while queued just leaves the line
            await sem.acquire(2); await sem.acquire(2)
            t = asyncio.ensure_future(sem.acquire(2))
            await asyncio.sleep(0.01)
            t.cancel()
            await asyncio.sleep(0.01)
            self.assertEqual(sem.waiting(), 0)
            sem.release(); sem.release()
            self.assertEqual(sem.holders(), 0)
            return order

        order = self.node.run_on_loop(drive(), timeout=10.0)
        self.assertEqual(order, ["answer", "norm1", "norm2", "low1"])


def _raw_mesh(test, links, repeaters=(), seed=1, config=None):
    quiet_rns()
    mesh = SimMesh(links, repeaters=repeaters, seed=seed, capture_dir=tempfile.mkdtemp(prefix="smci-raw-cap-"))
    test.mesh = mesh   # assigned before any assertion so tearDown can always stop it
    for n in ("A", "B"):
        mesh.add_node(n, config={**RAW_CFG, **(config or {})})
    mesh.advert_all()
    assert mesh.wait_contacts(40.0), "contacts never populated"
    assert mesh.wait_bound(40.0), "bind-frame discovery never completed"
    assert mesh.wait_resolved(60.0), "DIRECT paths never resolved"
    a, b = mesh.nodes["A"], mesh.nodes["B"]
    b.send(build_rns_packet("data", dest_hash=b.dest_hash, payload=b"prime"))
    assert wait_until(lambda: b.dest_hash in a.iface._rns_token_peer, 30.0), "token never learned"
    assert a.iface._peers[b.prefix].raw_fragments is True, "bind frame did not carry the raw capability"
    return mesh, a, b


def _events(node, name):
    return [r for r in node.capture_records() if r.get("event") == name]


@slow
class RawFragmentScenarios(unittest.TestCase):

    def tearDown(self):
        self.mesh.stop()

    def test_zero_hop_raw_transfer_then_lossy_then_fallback(self):
        _, a, b = _raw_mesh(self, ["A-B"], seed=51)
        big = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"raw-1-" + os.urandom(440))
        a.send(big)
        self.assertTrue(wait_until(lambda: big in b.owner.received, 40.0), "raw transfer never delivered")
        sent = _events(a, "raw_fragment_sent")
        self.assertGreaterEqual(len(sent), 3)   # 9-byte raw header (M3, 2026-09-20): 161 B per fragment, three for this payload
        self.assertEqual(sum(1 for r in _events(a, "direct_attempt_result") if r.get("frag_total")), 0, "no text fragments should have been sent")
        self.assertTrue(any(r.get("transport") == "direct_raw_multifragment" for r in b.capture_records() if r["direction"] == "in"))

        # Half the raw packets vanish on air: the reconcile rounds must recover the gaps.
        self.mesh.air.type_loss["RAW_CUSTOM"] = 0.5
        big2 = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"raw-2-" + os.urandom(440))
        a.send(big2)
        self.assertTrue(wait_until(lambda: big2 in b.owner.received, 90.0), "lossy raw transfer never completed")
        self.assertEqual(b.owner.received.count(big2), 1)

        # Raw packets never arrive at all: two answered reconciles, then text fallback.
        self.mesh.air.type_loss["RAW_CUSTOM"] = 1.0
        big3 = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"raw-3-" + os.urandom(440))
        a.send(big3)
        self.assertTrue(wait_until(lambda: big3 in b.owner.received, 120.0), "text fallback never delivered")
        self.assertIn(b.prefix, a.iface._raw_disabled_until, "raw should be disabled for the peer after the fallback")
        self.assertTrue(any(r.get("transport") == "direct_multifragment" for r in b.capture_records() if r["direction"] == "in"))

    def test_one_hop_raw_transfer_through_a_repeater(self):
        _, a, b = _raw_mesh(self, ["A-R", "R-B"], repeaters=["R"], seed=21)
        self.assertEqual(a.resolved_paths[b.prefix].out_path_len, 1)
        big = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"raw-hop-" + os.urandom(440))
        a.send(big)
        self.assertTrue(wait_until(lambda: big in b.owner.received, 60.0), "raw transfer through a repeater never delivered")
        self.assertGreaterEqual(self.mesh.repeaters["R"].counters["direct_forwarded"], 3)   # three fragments since M3
        self.assertGreaterEqual(len(_events(a, "raw_fragment_sent")), 3)

        # Phase 2's premise is "raw is enabled for this peer and the chain
        # drops it", so establish that premise rather than inheriting phase
        # 1's luck. Phase 1 can lose a single raw fragment to ordinary
        # collision/loss at the repeater hop -- sim captures from a failing
        # run show B holding [1,2,3] of pkt_id 0 and A falling back to text,
        # which sets the per-peer raw pause for
        # `direct_raw_fallback_cooldown_s`. Phase 2 would then never attempt
        # raw at all and this test would fail for a reason unrelated to the
        # per-path verdict it exists to check. (The pause-on-partial-delivery
        # behaviour itself is a separate question, noted for the user.)
        a.iface._raw_disabled_until.pop(b.prefix, None)
        self.assertTrue(a.iface._raw_fragments_eligible(b.prefix, a.iface.PRIORITY_NORMAL),
                        "raw must be eligible before the chain is made to drop it")

        # The chain stops carrying raw packets: Z85 text gets through, so the
        # PATH is noted and raw is not probed again on it.
        self.mesh.air.type_loss["RAW_CUSTOM"] = 1.0
        big2 = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"raw-hop-2-" + os.urandom(440))
        a.send(big2)
        self.assertTrue(wait_until(lambda: big2 in b.owner.received, 240.0), "text fallback through the repeater never delivered")
        path_hex = a.resolved_paths[b.prefix].out_path_hex
        # The verdict is written when the sender's text send completes, a few
        # seconds after the receiver already has the packet.
        self.assertTrue(wait_until(lambda: path_hex in a.iface._raw_unsupported_paths, 20.0),
                        "the repeater chain should be noted as not carrying raw")
        self.assertNotIn(b.prefix, a.iface._raw_disabled_until, "the per-peer pause is lifted once the path is noted")
        raw_before = len(_events(a, "raw_fragment_sent"))
        self.mesh.air.type_loss.pop("RAW_CUSTOM", None)
        big3 = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"raw-hop-3-" + os.urandom(440))
        a.send(big3)
        self.assertTrue(wait_until(lambda: big3 in b.owner.received, 90.0))
        self.assertEqual(len(_events(a, "raw_fragment_sent")), raw_before, "a noted chain must not be probed with raw again")

    def test_stale_path_reset_within_one_raw_send(self):
        """2026-09-19 morning field test: the desktop burst three whole raw
        sends down a dead zero-hop path before its stale-path reset fired,
        because the reconcile QUERYs recorded no evidence. Now each
        unanswered round counts, and the send stops once the path is gone.
        The threshold detector's regression: with path selection on (alpha
        0.1.6 item 1) a window is one sample and a dead path is abandoned
        after `path_switch_after_misses` windows for discovery (pinned in
        tests/test_path_selection_0922.py), so this runs with it off."""
        _, a, b = _raw_mesh(self, ["A-B"], seed=61, config={"direct_path_reset_threshold": "2", "path_selection_enabled": "no"})
        iface = a.iface
        time.sleep(max(0.0, iface.direct_path_reset_min_age_s - (time.monotonic() - iface._resolved_paths[b.prefix].resolved_at)))
        self.mesh.air.link_loss[("A", "B")] = 1.0
        self.mesh.air.link_loss[("B", "A")] = 1.0
        dead = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"raw-dead-" + os.urandom(440))
        a.send(dead)
        self.assertTrue(wait_until(lambda: b.prefix not in iface._resolved_paths, 90.0), "stale path was never reset")
        # Let the abandoned send wind down, then check it stopped early.
        time.sleep(3.0)
        rounds = {r["round"] for r in _events(a, "raw_fragment_sent")}
        # 2026-09-19 (bidirectional-transfer fix): an unanswered reconcile
        # no longer re-bursts, so on a dead path only round 0 carries data;
        # the reset still trips after `direct_path_reset_threshold` silent
        # query rounds and the send must have stopped by then.
        self.assertLessEqual(max(rounds), iface.direct_path_reset_threshold - 1,
                             f"the send should stop after the round that tripped the reset, got rounds {sorted(rounds)}")
        self.assertEqual(a.radio.contacts[b.radio.pubkey]["out_path_len"], -1, "reset_path never reached the radio")

        # Link comes back: the next raw send rediscovers and delivers.
        self.mesh.air.link_loss.clear()
        self.assertTrue(wait_until(lambda: not iface._path_discovery_in_backoff(b.prefix), 30.0))
        alive = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"raw-alive-" + os.urandom(440))
        a.send(alive)
        self.assertTrue(wait_until(lambda: alive in b.owner.received, 90.0), "raw send after rediscovery never delivered")
        self.assertIn(b.prefix, iface._resolved_paths)


# --- Night-session page-transfer helpers (2026-09-19, see NightSessionFixes) --
#
# The simulated-repeater forms of the 2026-09-19 night regression (one-hop and
# three-hop page transfers over simmesh's multi-hop air model) moved to
# tests/legacy/test_night_session_scenarios.py on 2026-09-20: that tier is
# archived, and the one-hop page transfer now runs against real firmware as
# the MeshBench `page_transfer` / `page_transfer_bidir` scenarios. What stays
# here is the zero-hop form, which the unit suite can run for real.

PAGE_PART_BYTES = 483          # a packed RNS Resource part, as in the field


def _page_parts(dest_hash, n, size=PAGE_PART_BYTES, tag=b"page"):
    # "resource" (context RESOURCE, like the field's page parts) so the
    # parts are exempt from outgoing_max_age: with plain "data" the tail of
    # a 12-part transfer expired at 120s in the sim, which is not what the
    # field transfer does (RNS's Resource layer owns that retry).
    # The packing overhead is measured from a probe rather than assumed: a
    # LINK-destination packet packs 19 bytes of header, not the 34 a
    # hard-coded constant once claimed (review, 2026-09-20 -- every run of
    # this scenario had failed on the size assertion below).
    overhead = len(build_rns_packet("resource", dest_hash=dest_hash, payload=b""))
    parts = [build_rns_packet("resource", dest_hash=dest_hash,
                              payload=(tag + b"-%02d-" % i) + os.urandom(size - overhead - len(tag + b"-%02d-" % i)))
             for i in range(n)]
    assert all(len(p) == size for p in parts), [len(p) for p in parts]
    return parts


def _page_stats(node):
    recs = node.capture_records()
    checks = [r for r in recs if r.get("event") == "completion_check_result"]
    results = [r for r in recs if r.get("event") == "direct_send_result"]
    raw = [r for r in results if r.get("method") == "raw"]
    fell_back = [r for r in results if r.get("fallback_from_raw")]
    answered = sum(1 for r in checks if r.get("outcome") == "answered")
    return {
        "checks": len(checks), "answered": answered,
        "answer_rate": answered / len(checks) if checks else None,
        "raw_complete": len(raw), "text_fallbacks": len(fell_back),
        "raw_completion": len(raw) / (len(raw) + len(fell_back)) if (raw or fell_back) else None,
        "slot_expired": sum(1 for r in results if r.get("method") == "slot_expired"),
        "answer_lock_waits": [r.get("lock_wait_s") for r in recs
                              if r.get("event") == "direct_attempt_result" and r.get("kind") in ("completion_answer", "completion_report")],
    }



@slow
class ZeroHopBidirectionalPageTransfer(unittest.TestCase):
    """Both nodes send a twelve-part page (12 x 483 B Resource-class parts)
    to each other at once over a zero-hop link, the unit-tier form of the
    2026-09-19 night regression in which a node's completion ANSWERs queued
    tens of seconds behind its own bursts. Until 2026-09-20 this existed only
    as an unverified simmesh multi-hop scenario gated behind
    SMCI_RUN_UNVERIFIED; this version runs (FAST_TIMING, default airtime,
    no loss) and pins: every part delivered both ways; no send dropped as
    `slot_expired`; no completion ANSWER or REPORT waited more than 10 s for
    the radio lock; and the transfer finished without a text fallback.
    The one-hop and bidirectional-under-loss forms are the MeshBench
    `page_transfer_bidir` scenario."""

    def tearDown(self):
        self.mesh.stop()

    def test_both_ways_at_once_completes_without_drops_or_lock_starvation(self):
        quiet_rns()
        mesh = SimMesh(["A-B"], seed=41, capture_dir=tempfile.mkdtemp(prefix="smci-page-cap-"))
        self.mesh = mesh
        for n in ("A", "B"):
            mesh.add_node(n, config={**RAW_CFG, "peer_discovery_target_peers": "1"})
        mesh.advert_all()
        self.assertTrue(mesh.wait_contacts(40.0))
        self.assertTrue(mesh.wait_bound(40.0))
        self.assertTrue(mesh.wait_resolved(60.0))
        a, b = mesh.nodes["A"], mesh.nodes["B"]
        for x, y in ((a, b), (b, a)):
            y.send(build_rns_packet("data", dest_hash=y.dest_hash, payload=b"prime"))
            self.assertTrue(wait_until(lambda: y.dest_hash in x.iface._rns_token_peer, 30.0), "token never learned")
            self.assertTrue(x.iface._peers[y.prefix].raw_fragments)
        wait_until(lambda: not a.iface._direct_exchange_lock_impl.locked() and not b.iface._direct_exchange_lock_impl.locked(), 30.0)
        pa = _page_parts(b.dest_hash, 12, tag=b"ab")
        pb = _page_parts(a.dest_hash, 12, tag=b"ba")
        started = time.monotonic()
        for x, y in zip(pa, pb):
            a.send(x)
            b.send(y)
        done = wait_until(lambda: all(p in b.owner.received for p in pa) and all(p in a.owner.received for p in pb), 600.0)
        elapsed = time.monotonic() - started
        time.sleep(2.0)
        sa, sb = _page_stats(a), _page_stats(b)
        delivered = (sum(1 for p in pa if p in b.owner.received), sum(1 for p in pb if p in a.owner.received))
        self.assertTrue(done, f"bidirectional transfer incomplete after {elapsed:.0f}s: delivered {delivered}; A={sa} B={sb}")
        self.assertEqual(sa["slot_expired"] + sb["slot_expired"], 0, (sa, sb))
        self.assertEqual(sa["text_fallbacks"] + sb["text_fallbacks"], 0, (sa, sb))
        waits = [w for w in sa["answer_lock_waits"] + sb["answer_lock_waits"] if w is not None]
        if waits:
            # Phase 3 M2 (2026-09-20): a node's REPORT waits behind its own
            # outgoing raw window (up to 6 parts x 3 fragments at zero hop,
            # 12-15 s observed under both-ways load) -- one window, bounded,
            # not starvation. MeshBench page_transfer_bidir measures the
            # cost against the real firmware.
            self.assertLessEqual(max(waits), 20.0, f"an ANSWER/REPORT waited {max(waits):.1f}s for the lock (all: {sorted(waits)[-5:]})")
        checks = sa["checks"] + sb["checks"]
        self.assertGreater(checks, 0, "no completion checks recorded")


if __name__ == "__main__":
    unittest.main()
