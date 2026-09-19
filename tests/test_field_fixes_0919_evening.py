"""
Field fixes from the 2026-09-19 evening session (captures in
`fieldtests/raw/Alpha0.1.2/*eveningtest*`, analysed by five parallel review
passes; see the interface module docstring's entry for the full evidence).

The session's own summary, because each test below pins one conclusion from
it: 6 of 6 resource transfers completed with zero permanently lost parts,
zero-hop throughput sat at the hardware ceiling, and every degradation
tracked hop count -- but the interface spent 34.6% of wall clock holding the
one shared radio lock (89% of that waiting for ACKs, 1800s of it in waits
that were never going to be answered) while the radio itself was only ~13%
busy. Contention between the two nodes, which an earlier reading blamed, was
measured at 8.6% frame overlap against 6.9% expected by chance.

What is deliberately NOT changed, and why, is recorded in the module
docstring: the answer priority tier is kept, the raw/reconcile design is
kept (it confirmed 250 already-held fragments out of 288 transmitted), and
no attempt is made to learn routing state from CHANNEL receives, whose
sender is unauthenticated.
"""
import time
import unittest

import RNS

from tests._support import SingleNodeCase

PEER = "abcdef012345"


class AckTimeoutCap(SingleNodeCase):
    """The largest ACK that ever arrived in the session was 8.15s (p99
    5.82s; per hop max 3.00 / 6.06 / 8.15s) while the firmware's own
    suggestion produced waits up to 28s. The cap is hop-aware so a deeper
    path still gets the time it needs."""

    def setUp(self):
        # The shared fixture runs FAST_TIMING, which compresses
        # `direct_ack_timeout_routed_max` below the production base -- so the
        # absolute ceiling would mask the policy under test. Pin the shipped
        # defaults here and restore afterwards.
        iface = self.iface
        saved = {k: getattr(iface, k) for k in (
            "direct_ack_timeout_base_s", "direct_ack_timeout_per_hop_s",
            "direct_ack_timeout_routed_max_s",
        )}
        self.addCleanup(lambda: [setattr(iface, k, v) for k, v in saved.items()])
        iface.direct_ack_timeout_base_s = 8.0
        iface.direct_ack_timeout_per_hop_s = 4.0
        iface.direct_ack_timeout_routed_max_s = 45.0

    def test_cap_scales_with_hops_and_is_bounded(self):
        iface = self.iface
        base, per_hop = iface.direct_ack_timeout_base_s, iface.direct_ack_timeout_per_hop_s
        self.assertEqual(iface._ack_timeout_cap_s(0), base)
        self.assertEqual(iface._ack_timeout_cap_s(1), base + per_hop)
        self.assertEqual(iface._ack_timeout_cap_s(3), base + 3 * per_hop)
        # Unknown hop count must not mean "wait a long time".
        self.assertEqual(iface._ack_timeout_cap_s(None), base)
        # Never above the absolute ceiling, however deep the path claims to be.
        self.assertLessEqual(iface._ack_timeout_cap_s(100), iface.direct_ack_timeout_routed_max_s)

    def test_cap_covers_every_ack_latency_the_field_session_observed(self):
        # Worst observed per hop count, from the evening captures.
        for hops, worst in ((0, 3.00), (1, 6.06), (2, 8.15)):
            self.assertGreater(
                self.iface._ack_timeout_cap_s(hops), worst,
                msg=f"a {hops}-hop ACK that really took {worst}s would now be cut off",
            )


class CompletionBudgetCap(SingleNodeCase):
    """96% of answers arrived within 15s; every band beyond 20s produced two
    answers in the whole session; and the answer rate FELL as the budget
    grew. So: floor, adaptive middle, hard cap -- and no growth terms."""

    def setUp(self):
        self.iface._query_rtt.pop(PEER, None)
        self.iface._last_firmware_ack_timeout_s.pop(PEER, None)

    def test_no_information_gives_a_hop_aware_floor_under_the_cap(self):
        """The cap is the fix; the floor stays hop-aware. Measured
        query->answer round trips were median 3.2-5.7s with a p90 of
        11.7-16.1s, so a first query -- before any RTT sample exists to
        widen it -- must not be abandoned in the base interval at depth."""
        iface = self.iface
        previous = None
        for hops in (0, 1, 2, 3):
            budget = iface._completion_query_timeout_s(PEER, hops)
            expected = min(
                iface.direct_completion_check_timeout_s
                + iface.direct_completion_check_timeout_per_hop_s * hops,
                iface._completion_query_timeout_cap_s(hops),
            )
            self.assertAlmostEqual(budget, expected, places=6, msg=f"hops={hops}")
            self.assertLessEqual(budget, iface._completion_query_timeout_cap_s(hops))
            if previous is not None:
                self.assertGreaterEqual(budget, previous, "the floor must not shrink with depth")
            previous = budget
        self.assertEqual(iface._completion_query_timeout_s(PEER, 0),
                         iface.direct_completion_check_timeout_s,
                         "zero hop is still exactly the base interval")

    def test_hop_aware_floor_is_still_clamped_by_the_cap(self):
        iface = self.iface
        original = iface.direct_completion_check_timeout_per_hop_s
        try:
            iface.direct_completion_check_timeout_per_hop_s = 50.0
            for hops in (1, 2, 3):
                self.assertEqual(iface._completion_query_timeout_s(PEER, hops),
                                 iface._completion_query_timeout_cap_s(hops))
        finally:
            iface.direct_completion_check_timeout_per_hop_s = original

    def test_large_firmware_bound_is_clamped_to_the_cap(self):
        iface = self.iface
        iface._last_firmware_ack_timeout_s[PEER] = 28.0
        self.assertEqual(iface._completion_query_timeout_s(PEER, 0),
                         iface.direct_completion_check_timeout_max_s)
        self.assertEqual(iface._completion_query_timeout_s(PEER, 2),
                         iface.direct_completion_check_timeout_max_multihop_s)

    def test_fast_measured_round_trip_pulls_it_down_to_the_zero_hop_floor(self):
        iface = self.iface
        for _ in range(20):
            iface._record_query_rtt(PEER, 0.8)
        self.assertEqual(iface._completion_query_timeout_s(PEER, 0),
                         iface.direct_completion_check_timeout_s)

    def test_budget_never_exceeds_the_cap_for_any_rtt(self):
        iface = self.iface
        for rtt in (0.5, 3.0, 12.0, 40.0):
            iface._query_rtt.pop(PEER, None)
            for _ in range(5):
                iface._record_query_rtt(PEER, rtt)
            for hops in (0, 1, 2, 3):
                cap = (iface.direct_completion_check_timeout_max_multihop_s if hops >= 2
                       else iface.direct_completion_check_timeout_max_s)
                self.assertLessEqual(iface._completion_query_timeout_s(PEER, hops), cap)

    def test_own_queue_depth_no_longer_inflates_the_budget(self):
        iface = self.iface
        original = iface._direct_exchange_queue_depth
        try:
            iface._direct_exchange_queue_depth = 0
            base = iface._completion_query_timeout_s(PEER, 0)
            iface._direct_exchange_queue_depth = 40
            self.assertEqual(iface._completion_query_timeout_s(PEER, 0), base)
        finally:
            iface._direct_exchange_queue_depth = original


class CompletionNonce(SingleNodeCase):
    """Five checks resolved `answered` although no matching query ever
    reached the peer -- one applying `held=[]`, i.e. discarding every
    fragment the receiver actually had. The frag_total guard cannot catch a
    stale answer whose frag_total matches; a nonce can."""

    def test_v3_round_trips_the_nonce_and_v1_v2_do_not_carry_one(self):
        iface = self.iface
        for version, expected in (
            (iface.COMPLETION_PROTOCOL_VERSION_V1, None),
            (iface.COMPLETION_PROTOCOL_VERSION_V2, None),
            (iface.COMPLETION_PROTOCOL_VERSION, 42),
        ):
            frame = iface._encode_completion_frame(
                iface.COMPLETION_TYPE_ANSWER, 9, 3, complete=False, held={0, 2},
                version=version, nonce=42,
            )
            decoded = iface._decode_completion_frame(frame)
            self.assertEqual(decoded.nonce, expected, msg=f"version {version}")
            if version >= iface.COMPLETION_PROTOCOL_VERSION_V2:
                self.assertEqual(decoded.held, frozenset({0, 2}))

    def test_v3_bitmap_still_decodes_after_the_nonce(self):
        iface = self.iface
        for frag_total in (1, 8, 9, 255):
            held = {i for i in range(frag_total) if i % 3 == 0}
            frame = iface._encode_completion_frame(
                iface.COMPLETION_TYPE_ANSWER, 1, frag_total, held=held, nonce=200,
            )
            decoded = iface._decode_completion_frame(frame)
            self.assertEqual(decoded.held, frozenset(held))
            self.assertEqual(decoded.nonce, 200)

    def test_a_late_answer_may_only_report_more_never_less(self):
        """The monotone-completeness rule. A stale-nonce answer that claims
        `complete` is accepted (a receiver that had the whole packet cannot
        have less of it); one carrying a partial `held` set is discarded,
        because that is exactly the `held=[]` case that threw away fragments
        the receiver really had."""
        import asyncio
        iface = self.iface

        def deliver(nonce, complete, held, outstanding_nonce=7, frag_total=3):
            async def run():
                fut = asyncio.get_running_loop().create_future()
                iface._completion_query_waiters[(PEER, 55)] = (fut, frag_total, outstanding_nonce)
                frame = iface._encode_completion_frame(
                    iface.COMPLETION_TYPE_ANSWER, 55, frag_total,
                    complete=complete, held=held, nonce=nonce,
                )
                original = iface._canonical_peer_prefix
                iface._canonical_peer_prefix = lambda token: PEER
                try:
                    iface._handle_incoming_completion_frame(frame, PEER)
                finally:
                    iface._canonical_peer_prefix = original
                    iface._completion_query_waiters.pop((PEER, 55), None)
                return fut
            return self.node.run_on_loop(run())

        # Matching nonce: accepted, as before.
        self.assertTrue(deliver(7, False, {0, 1}).done())
        # Stale nonce, partial held: discarded.
        self.assertFalse(deliver(3, False, set()).done(), "a stale partial answer must not be applied")
        self.assertFalse(deliver(3, False, {0}).done(), "a stale partial answer must not be applied")
        # Stale nonce, complete: accepted, because completeness is monotone.
        late = deliver(3, True, {0, 1, 2})
        self.assertTrue(late.done(), "a late answer reporting completeness should finish the transfer")
        self.assertTrue(late.result().complete)

    def test_truncated_v3_frame_is_rejected_not_misread(self):
        iface = self.iface
        frame = iface._encode_completion_frame(iface.COMPLETION_TYPE_QUERY, 1, 2, nonce=5)
        body = iface._decode_completion_frame(frame)  # sanity
        self.assertEqual(body.nonce, 5)
        with self.assertRaises(ValueError):
            # A v3 body with the nonce byte removed.
            raw = bytes([iface.COMPLETION_PROTOCOL_VERSION, iface.COMPLETION_TYPE_QUERY, 0, 0, 1, 2])
            iface._decode_completion_frame(iface.COMPLETION_MARKER + self.module._z85_encode(raw))


class DuplicateInFlightDeadlock(SingleNodeCase):
    """Part `ca6b3d36db27` was delivered to the peer at 16:48:25, then seven
    consecutive re-sends were refused (16:49:55-16:52:53) because the
    original send's in-flight entry never cleared -- 178s of a 442s
    transfer, with RNS asking correctly and the interface declining."""

    def _key(self, raw):
        return RNS.Identity.truncated_hash(raw)

    def test_suppression_gives_way_after_the_limit(self):
        iface = self.iface
        raw = b"\x00" * 24
        key = self._key(raw)
        self.addCleanup(iface._outgoing_inflight.pop, key, None)
        self.addCleanup(iface._outgoing_duplicate_suppressed.pop, key, None)
        iface._outgoing_inflight.clear()
        iface._outgoing_duplicate_suppressed.clear()
        # Pretend a send for these bytes is stuck in flight.
        iface._outgoing_inflight[key] = time.monotonic()
        limit = iface.outgoing_duplicate_suppress_limit
        self.assertGreaterEqual(limit, 2, "the limit must leave room for real suppression")
        for i in range(1, limit):
            self.assertEqual(iface._outgoing_duplicate_suppressed.get(key, 0), i - 1)
            iface._outgoing_duplicate_suppressed[key] = i  # what process_outgoing records
        # On the limit-th ask, process_outgoing must let it through. Exercised
        # through the real method so the whole decision path is covered.
        before = iface._outgoing_dropped_total
        iface._outgoing_duplicate_suppressed[key] = limit - 1
        iface.process_outgoing(raw)
        self.assertEqual(
            iface._outgoing_dropped_total, before,
            msg="the packet should have been queued, not dropped, once the limit was reached",
        )
        self.assertNotIn(key, iface._outgoing_duplicate_suppressed)

    def test_ordinary_duplicate_is_still_suppressed(self):
        iface = self.iface
        raw = b"\x01" * 24
        key = self._key(raw)
        self.addCleanup(iface._outgoing_inflight.pop, key, None)
        self.addCleanup(iface._outgoing_duplicate_suppressed.pop, key, None)
        iface._outgoing_inflight.clear()
        iface._outgoing_duplicate_suppressed.clear()
        iface._outgoing_inflight[key] = time.monotonic()
        before = iface._outgoing_dropped_total
        iface.process_outgoing(raw)
        self.assertEqual(iface._outgoing_dropped_total, before + 1)
        self.assertEqual(iface._outgoing_duplicate_suppressed.get(key), 1)


class HealthyPathPatience(SingleNodeCase):
    """A 1-hop path running 91% was discarded after a 7-attempt bad patch.
    Failures still accumulate, but a recently-proven path takes more of them
    before being reset."""

    def setUp(self):
        self.iface._direct_path_recent_success.pop(PEER, None)
        self.iface._direct_path_failures.pop(PEER, None)

    def test_success_is_remembered_and_ages_out(self):
        iface = self.iface
        for _ in range(4):
            iface.record_direct_send_result(PEER, succeeded=True, waited_full_timeout=True)
        self.assertEqual(iface._recent_path_successes(PEER), 4)
        # Age every stamp past the window.
        iface._direct_path_recent_success[PEER] = [
            time.monotonic() - iface.direct_path_healthy_window_s - 1 for _ in range(4)
        ]
        self.assertEqual(iface._recent_path_successes(PEER), 0)

    def test_healthy_path_needs_more_failures_before_reset(self):
        iface = self.iface
        resets = []
        original = iface._spawn_background_task
        iface._spawn_background_task = lambda coro: (resets.append(1), coro.close())[0]
        self.addCleanup(setattr, iface, "_spawn_background_task", original)
        # Prove the path healthy.
        for _ in range(iface.direct_path_healthy_recent_successes):
            iface.record_direct_send_result(PEER, succeeded=True, waited_full_timeout=True)
        iface._direct_path_failures.pop(PEER, None)
        # The ordinary threshold's worth of failures must NOT reset it now.
        for _ in range(iface.direct_path_reset_threshold):
            iface.record_direct_send_result(PEER, succeeded=False, waited_full_timeout=True)
        self.assertEqual(resets, [], "a recently healthy path was reset on the ordinary threshold")
        # Enough failures still resets it.
        for _ in range(int(iface.direct_path_reset_threshold * iface.direct_path_healthy_patience_multiplier) + 1):
            iface.record_direct_send_result(PEER, succeeded=False, waited_full_timeout=True)
        self.assertTrue(resets, "a genuinely dead path must still be reset eventually")


class ProofsDoNotArmUnknownDestBackoff(SingleNodeCase):
    """Six proofs per session route as unknown_dest because the CHANNEL
    receive path cannot authenticate a sender. In the midday capture three
    such proofs armed a 300s cooldown that then dropped later proofs."""

    def test_proof_header_is_recognised(self):
        iface = self.iface
        proof = self.module._RnsHeader(
            packet_type=RNS.Packet.PROOF, destination_type=RNS.Destination.SINGLE,
            context=RNS.Packet.NONE, header_type=RNS.Packet.HEADER_1,
            destination_hash=bytes(range(16)),
        )
        data = self.module._RnsHeader(
            packet_type=RNS.Packet.DATA, destination_type=RNS.Destination.SINGLE,
            context=RNS.Packet.NONE, header_type=RNS.Packet.HEADER_1,
            destination_hash=bytes(range(16)),
        )
        self.assertTrue(iface._proof_like(proof))
        self.assertFalse(iface._proof_like(data))
        self.assertFalse(iface._proof_like(None))

    def test_a_proof_never_reaches_the_backoff_counter(self):
        iface = self.iface
        dest = bytes(range(16, 32))
        iface._unknown_dest_attempts.pop(dest, None)
        iface._unknown_dest_backoff_until.pop(dest, None)
        # Simulate what the dispatcher now does for a proof: skip the record.
        proof = self.module._RnsHeader(
            packet_type=RNS.Packet.PROOF, destination_type=RNS.Destination.SINGLE,
            context=RNS.Packet.NONE, header_type=RNS.Packet.HEADER_1, destination_hash=dest,
        )
        for _ in range(10):
            if not iface._proof_like(proof):
                iface._record_unknown_dest_attempt(dest)
        self.assertNotIn(dest, iface._unknown_dest_attempts)
        self.assertFalse(iface._unknown_dest_in_backoff(dest))


if __name__ == "__main__":
    unittest.main()
