"""
Regression tests for the dead-wait trims of 2026-09-20 (module docstring
entry "Dead-wait trims"), each pinning one measured conclusion from the three
2026-09-19 field sessions (3693 attempts, 539 completion checks):

  * the answer wait after a QUERY whose firmware ACK was missed is capped at
    a short grace (un-ACKed QUERYs were answered 6/19, 9/65, 4/31, 0/5 times
    at 0-3 hops, never later than 5.6 s at hop <= 1);
  * the hop-aware ACK-wait ceiling is 5 + 3 x hops (per-hop ACK maxima
    3.82 / 6.06 / 8.15 / 7.25 s over 2670 ACKs: zero cut off);
  * the post-miss listen draws from 0.2-1.0 s (mean 1.7 s on 598 misses
    bought nothing measurable against 8.6% vs 6.9%-by-chance overlap);
  * a completion ANSWER's own ACK wait is hop-aware (it ran with
    hop_count=None before).
"""
import unittest

from tests._support import SingleNodeCase

PEER = "abcdef012345"


class UnackedQueryGrace(SingleNodeCase):
    def test_grace_is_hop_tiered_and_zero_disables(self):
        iface = self.iface
        iface.direct_completion_unacked_grace_s = 6.0
        iface.direct_completion_unacked_grace_multihop_s = 10.0
        self.assertEqual(iface._completion_unacked_grace_s(0, PEER), 6.0)
        self.assertEqual(iface._completion_unacked_grace_s(1, PEER), 6.0)
        self.assertEqual(iface._completion_unacked_grace_s(2, PEER), 10.0)
        self.assertEqual(iface._completion_unacked_grace_s(3, PEER), 10.0)
        iface.direct_completion_unacked_grace_s = 0.0
        self.assertEqual(iface._completion_unacked_grace_s(1, PEER), 0.0)
        iface.direct_completion_unacked_grace_s = 6.0

    def test_unacked_query_waits_only_the_grace(self):
        """The QUERY goes out but its firmware ACK never comes: the answer
        wait must end at the grace, not at the full 15 s budget."""
        import asyncio, time
        iface = self.iface
        iface.direct_completion_unacked_grace_s = 0.5
        iface.direct_raw_report_enabled = False

        async def fake_send(*args, **kwargs):
            return False, True   # not ACKed, waited the full timeout

        original = iface._send_direct_frame_and_wait_for_ack
        iface._send_direct_frame_and_wait_for_ack = fake_send
        try:
            async def run():
                t0 = time.monotonic()
                got = await iface._query_remote_fragments("ab" * 32, PEER, 77, 3, stage="raw0", hop_count=1)
                return got, time.monotonic() - t0
            got, took = self.node.run_on_loop(run(), timeout=30.0)
        finally:
            iface._send_direct_frame_and_wait_for_ack = original
            iface.direct_completion_unacked_grace_s = 6.0
        self.assertIsNone(got)
        self.assertLess(took, 3.0, f"an un-ACKed QUERY waited {took:.1f}s for its answer")
        self.assertGreaterEqual(took, 0.4)


def _shipped_defaults(module):
    """The retry/peer-discovery defaults as `_configure_*` parses an EMPTY
    config block (the unit harness overrides several of them with
    FAST_TIMING, so the live interface cannot be used to pin a default)."""
    bare = module.SmartMeshCoreInterface.__new__(module.SmartMeshCoreInterface)
    bare._configure_retry({})
    bare._configure_peer_discovery({})
    return bare


class AckCeilingIs5Plus3PerHop(SingleNodeCase):
    def test_defaults_and_no_field_ack_cut_off(self):
        bare = _shipped_defaults(self.module)
        self.assertEqual(bare.direct_ack_timeout_base_s, 5.0)
        self.assertEqual(bare.direct_ack_timeout_per_hop_s, 3.0)
        iface = self.iface
        saved = (iface.direct_ack_timeout_base_s, iface.direct_ack_timeout_per_hop_s, iface.direct_ack_timeout_routed_max_s)
        iface.direct_ack_timeout_base_s, iface.direct_ack_timeout_per_hop_s, iface.direct_ack_timeout_routed_max_s = 5.0, 3.0, 45.0
        try:
            for hops, worst in ((0, 3.82), (1, 6.06), (2, 8.15), (3, 7.25)):
                cap = iface._ack_timeout_cap_s(hops)
                self.assertGreater(cap, worst, f"{hops} hops: cap {cap} would cut off a real ACK of {worst}s")
                self.assertAlmostEqual(cap, 5.0 + 3.0 * hops)
        finally:
            iface.direct_ack_timeout_base_s, iface.direct_ack_timeout_per_hop_s, iface.direct_ack_timeout_routed_max_s = saved


class PostMissListenRange(SingleNodeCase):
    def test_defaults_and_draws(self):
        bare = _shipped_defaults(self.module)
        self.assertEqual(bare.direct_post_send_listen_min_s, 0.2)
        self.assertEqual(bare.direct_post_send_listen_max_s, 1.0)
        self.assertEqual(bare.direct_completion_unacked_grace_s, 6.0)
        self.assertEqual(bare.direct_completion_unacked_grace_multihop_s, 10.0)
        iface = self.iface
        saved = (iface.direct_post_send_listen_min_s, iface.direct_post_send_listen_max_s, iface.rx_log_holds_enabled)
        iface.direct_post_send_listen_min_s, iface.direct_post_send_listen_max_s, iface.rx_log_holds_enabled = 0.2, 1.0, False
        try:
            for _ in range(50):
                d = iface._post_attempt_listen_s(False, "no_info")
                self.assertTrue(0.2 <= d <= 1.0, d)
            for _ in range(20):
                self.assertTrue(0.0 <= iface._post_attempt_listen_s(True, None) <= 0.4)
        finally:
            iface.direct_post_send_listen_min_s, iface.direct_post_send_listen_max_s, iface.rx_log_holds_enabled = saved


class CompletionAnswerAckWaitIsHopAware(SingleNodeCase):
    def test_answer_send_passes_the_resolved_hop_count(self):
        import asyncio
        iface = self.iface
        seen = {}

        async def fake_send(target, frame, attempt, **kwargs):
            seen.update(kwargs)
            return True, False

        original_send = iface._send_direct_frame_and_wait_for_ack
        original_contact = iface._resolve_contact
        original_canon = iface._canonical_peer_prefix
        original_noack = iface.direct_report_noack
        iface._send_direct_frame_and_wait_for_ack = fake_send
        iface._resolve_contact = lambda token: {"public_key": "ab" * 32, "out_path_len": 2}
        iface._canonical_peer_prefix = lambda token: PEER
        iface._resolved_paths.pop(PEER, None)
        # Phase 3 M1 (2026-09-20): answers go out without a firmware ACK by
        # default; this pins the ACKed path, so select it explicitly.
        iface.direct_report_noack = False
        try:
            self.node.run_on_loop(iface._send_completion_answer(PEER, 5, 3, True, held={0, 1, 2}, version=3, nonce=9), timeout=20.0)
        finally:
            iface._send_direct_frame_and_wait_for_ack = original_send
            iface._resolve_contact = original_contact
            iface._canonical_peer_prefix = original_canon
            iface.direct_report_noack = original_noack
        self.assertEqual(seen.get("hop_count"), 2)
        self.assertEqual(seen.get("kind"), "completion_answer")


if __name__ == "__main__":
    unittest.main()
