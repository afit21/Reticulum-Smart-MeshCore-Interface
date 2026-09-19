"""
Unit tests for the 2026-09-19 fixes derived from the 2026-09-18
drive-home (3-hop) field capture: spontaneous-announce pacing per
destination, RTO-style backoff instead of discarding a measured ACK
RTT on a miss, dropping queued packets for a Link that has since been
closed, and the doubling bind re-request schedule for fresh pairings.
"""
import os
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet, wait_until, slow

PEER = "abcdef012345"


class AnnouncePacingTests(SingleNodeCase):

    def setUp(self):
        self.iface._announce_last_sent_at.clear()

    def test_one_announce_per_destination_per_window(self):
        iface = self.iface
        d1, d2 = os.urandom(16), os.urandom(16)
        self.assertFalse(iface._announce_rate_limited(d1))
        self.assertTrue(iface._announce_rate_limited(d1))
        self.assertFalse(iface._announce_rate_limited(d2))
        iface._announce_last_sent_at[d1] = time.monotonic() - iface.announce_min_interval_s - 1
        self.assertFalse(iface._announce_rate_limited(d1))
        self.assertFalse(iface._announce_rate_limited(None))

    def test_disabled_when_interval_zero(self):
        iface = self.iface
        original = iface.announce_min_interval_s
        try:
            iface.announce_min_interval_s = 0
            d = os.urandom(16)
            self.assertFalse(iface._announce_rate_limited(d))
            self.assertFalse(iface._announce_rate_limited(d))
        finally:
            iface.announce_min_interval_s = original

    def test_repeated_announce_is_dropped_but_path_response_is_not(self):
        iface = self.iface
        dest = os.urandom(16)
        dropped_before = iface._outgoing_dropped_total
        iface.process_outgoing(build_rns_packet("announce", dest_hash=dest, payload=b"a1"))
        iface.process_outgoing(build_rns_packet("announce", dest_hash=dest, payload=b"a2"))
        self.assertTrue(wait_until(lambda: iface._outgoing_dropped_total == dropped_before + 1, 10.0))
        time.sleep(0.5)
        self.assertEqual(iface._outgoing_dropped_total, dropped_before + 1)
        # A path-response announce for the same destination is never paced by this.
        iface.process_outgoing(build_rns_packet("path_response", dest_hash=dest, payload=b"pr"))
        time.sleep(1.0)
        self.assertEqual(iface._outgoing_dropped_total, dropped_before + 1)


class AckRttBackoffTests(SingleNodeCase):

    def setUp(self):
        self.iface._ack_rtt.clear()
        self.iface._ack_rtt_snapshot.clear()

    def _measured(self, fw=30.0):
        for _ in range(self.iface.direct_ack_rtt_min_samples + 2):
            self.iface._record_ack_rtt(PEER, 1.0)
        timeout, source = self.iface._adaptive_ack_timeout(PEER, fw)
        self.assertEqual(source, "measured")
        return timeout

    def test_miss_widens_next_measured_timeout_and_ack_resets(self):
        iface = self.iface
        base = self._measured()
        iface._backoff_ack_rtt(PEER, "test miss")
        widened, source = iface._adaptive_ack_timeout(PEER, 30.0)
        self.assertEqual(source, "measured")
        self.assertAlmostEqual(widened, base * iface.direct_ack_rtt_miss_backoff, places=6)
        self.assertIn(PEER, iface._ack_rtt, "estimate must be kept, not discarded")
        iface._backoff_ack_rtt(PEER, "second miss")
        self.assertAlmostEqual(iface._adaptive_ack_timeout(PEER, 30.0)[0], base * iface.direct_ack_rtt_miss_backoff ** 2, places=6)
        iface._record_ack_rtt(PEER, 1.0)
        self.assertLess(iface._adaptive_ack_timeout(PEER, 30.0)[0], base * 1.5)

    def test_backoff_never_exceeds_firmware_timeout(self):
        iface = self.iface
        self._measured(fw=30.0)
        for _ in range(6):
            iface._backoff_ack_rtt(PEER, "miss")
        self.assertEqual(iface._adaptive_ack_timeout(PEER, 5.0), (5.0, "firmware"))

    def test_factor_at_most_one_restores_discard(self):
        iface = self.iface
        original = iface.direct_ack_rtt_miss_backoff
        try:
            iface.direct_ack_rtt_miss_backoff = 1.0
            self._measured()
            iface._backoff_ack_rtt(PEER, "miss")
            self.assertNotIn(PEER, iface._ack_rtt)
            self.assertIn(PEER, iface._ack_rtt_snapshot)
            self.assertEqual(iface._adaptive_ack_timeout(PEER, 30.0)[1], "firmware")
        finally:
            iface.direct_ack_rtt_miss_backoff = original


class ClosedLinkTests(SingleNodeCase):

    def setUp(self):
        self.iface._closed_links.clear()

    def test_link_packets_dropped_after_close_in_either_direction(self):
        iface = self.iface
        link_id = os.urandom(16)
        close_hdr = iface._parse_rns_header(build_rns_packet("link_close", dest_hash=link_id))
        data_hdr = iface._parse_rns_header(build_rns_packet("link_data", dest_hash=link_id))
        other_hdr = iface._parse_rns_header(build_rns_packet("link_data", dest_hash=os.urandom(16)))
        single_hdr = iface._parse_rns_header(build_rns_packet("data", dest_hash=link_id))
        self.assertFalse(iface._link_closed(data_hdr))
        iface._note_link_closed(close_hdr)
        self.assertTrue(iface._link_closed(data_hdr))
        self.assertFalse(iface._link_closed(close_hdr), "the LINKCLOSE itself must still go out")
        self.assertFalse(iface._link_closed(other_hdr))
        self.assertFalse(iface._link_closed(single_hdr), "only Link-addressed packets are affected")
        iface._closed_links[link_id] = time.monotonic() - iface.CLOSED_LINK_TTL_S - 1
        self.assertFalse(iface._link_closed(data_hdr))
        iface._closed_links_sweep(time.monotonic())
        self.assertNotIn(link_id, iface._closed_links)

    def test_incoming_linkclose_is_noted(self):
        iface = self.iface
        link_id = os.urandom(16)
        self.on_loop(iface.process_incoming, build_rns_packet("link_close", dest_hash=link_id))
        self.assertIn(link_id, iface._closed_links)

    def test_queued_packet_for_closed_link_is_dropped_at_dequeue(self):
        iface = self.iface
        link_id = os.urandom(16)
        dropped_before = iface._outgoing_dropped_total
        iface.process_outgoing(build_rns_packet("link_close", dest_hash=link_id, payload=b"bye"))
        iface.process_outgoing(build_rns_packet("link_data", dest_hash=link_id, payload=b"too late"))
        self.assertTrue(wait_until(lambda: iface._outgoing_dropped_total == dropped_before + 1, 10.0))


class RawProofCorrelationTests(SingleNodeCase):

    def setUp(self):
        self.iface._peers.clear()
        self.iface._resolved_paths.clear()
        self.iface._proof_correlation.clear()

    def _bind_and_resolve(self, prefix):
        self.iface._peers[prefix] = self.module._PeerRecord(pubkey_prefix=prefix, has_upstream_rns=False, last_seen=time.time())
        self.iface._resolved_paths[prefix] = self.module._ResolvedPath("", 0, 1, time.monotonic())

    def _proof_header_for(self, data):
        header = self.iface._parse_rns_header(data)
        truncated = self.iface._compute_truncated_hash(data, header.header_type)
        return truncated, self.iface._parse_rns_header(build_rns_packet("proof", dest_hash=truncated))

    def test_proof_routes_to_bound_resolved_peer_after_raw_receive(self):
        iface = self.iface
        data = build_rns_packet("data", dest_hash=os.urandom(16), payload=b"raw-received")
        truncated, proof_hdr = self._proof_header_for(data)
        self._bind_and_resolve(PEER)
        iface._correlate_raw_proof(data, PEER)
        self.assertIn(truncated, iface._proof_correlation)
        self.assertEqual(iface._resolve_routing_peer(proof_hdr), PEER)
        # Only the proof correlation is recorded -- never an RNS token.
        self.assertNotIn(iface._parse_rns_header(data).destination_hash, iface._rns_token_peer)

    def test_untrusted_claim_is_ignored(self):
        iface = self.iface
        data = build_rns_packet("data", dest_hash=os.urandom(16), payload=b"raw-received")
        truncated, proof_hdr = self._proof_header_for(data)
        iface._correlate_raw_proof(data, PEER)                # not bound
        self.assertNotIn(truncated, iface._proof_correlation)
        iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=False, last_seen=time.time())
        iface._correlate_raw_proof(data, PEER)                # bound, no resolved path
        self.assertNotIn(truncated, iface._proof_correlation)
        iface._correlate_raw_proof(data, None)
        self.assertNotIn(truncated, iface._proof_correlation)
        self.assertIsNone(iface._resolve_routing_peer(proof_hdr))

    def test_proof_packets_themselves_are_never_correlated(self):
        iface = self.iface
        self._bind_and_resolve(PEER)
        proof = build_rns_packet("proof", dest_hash=os.urandom(16), payload=b"p")
        iface._correlate_raw_proof(proof, PEER)
        self.assertEqual(iface._proof_correlation, {})


class ReconcilePriorityAndBudgetTests(SingleNodeCase):

    def test_answer_tier_sits_between_handshake_and_normal(self):
        iface = self.iface
        self.assertLess(iface.PRIORITY_HANDSHAKE, iface.PRIORITY_ANSWER)
        self.assertLess(iface.PRIORITY_ANSWER, iface.PRIORITY_NORMAL)
        self.assertLess(iface.PRIORITY_NORMAL, iface.PRIORITY_LOW)
        # RNS traffic itself never classifies into the ANSWER tier.
        for kind in ("data", "announce", "path_request", "link_request", "proof", "lrproof", "path_response"):
            self.assertNotEqual(iface._priority_tier(iface._parse_rns_header(build_rns_packet(kind))), iface.PRIORITY_ANSWER)

    def test_answer_budget_grows_with_own_queue_depth_and_caps(self):
        iface = self.iface
        original = iface._direct_exchange_queue_depth
        try:
            iface._direct_exchange_queue_depth = 0
            base = iface._completion_query_timeout_s(PEER, 0)
            iface._direct_exchange_queue_depth = 2
            self.assertAlmostEqual(iface._completion_query_timeout_s(PEER, 0), min(base + 2 * iface.direct_completion_check_timeout_s, iface.direct_ack_timeout_routed_max_s), places=6)
            iface._direct_exchange_queue_depth = 40
            widened = iface._completion_query_timeout_s(PEER, 0)
            self.assertLessEqual(widened, max(base, iface.direct_ack_timeout_routed_max_s))
            self.assertGreaterEqual(widened, base)
        finally:
            iface._direct_exchange_queue_depth = original


@slow
class DelayedAnswerScenario(unittest.TestCase):
    """The field analysis's proposed test: inject answer *delay*, not loss.
    The receiver holds every fragment after the first burst but its
    ANSWER arrives after the querier's budget; the sender must re-query,
    never re-burst data the peer already has, and finish once the late
    answer lands."""

    def tearDown(self):
        self.mesh.stop()

    def test_unanswered_round_requeries_instead_of_rebursting(self):
        import asyncio
        from tests.test_raw_fragments import _raw_mesh, _events
        _, a, b = _raw_mesh(self, ["A-B"], seed=71)
        original = b.iface._send_completion_answer

        async def delayed(*args, **kwargs):
            await asyncio.sleep(20.0)
            await original(*args, **kwargs)

        b.iface._send_completion_answer = delayed
        big = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"late-answer-" + os.urandom(440))
        a.send(big)
        self.assertTrue(wait_until(lambda: big in b.owner.received, 30.0), "fragments never arrived")
        self.assertTrue(wait_until(lambda: any(r.get("outcome") == "answered" for r in _events(a, "completion_check_result")), 90.0),
                        "the late answer never got through")
        time.sleep(2.0)
        checks = _events(a, "completion_check_result")
        self.assertGreaterEqual(sum(1 for r in checks if r.get("outcome") == "timeout"), 1, "no round was unanswered -- delay too short to exercise the fix")
        sent = _events(a, "raw_fragment_sent")
        frag_total = sent[0]["frag_total"]
        rounds_with_data = sorted({r["round"] for r in sent})
        valve = a.iface.direct_raw_reburst_after_unanswered
        # The first silent round must re-query, not re-burst; only after
        # `direct_raw_reburst_after_unanswered` consecutive silent rounds may
        # one safety-valve burst follow. With the answer held 20s, rounds 0
        # and 1 are silent, so round 2 may burst once -- never round 1.
        self.assertNotIn(1, rounds_with_data, f"data was re-burst on the first unanswered round: {[(r['round'], r['frag_idx']) for r in sent]}")
        self.assertTrue(all(rnd == 0 or rnd >= valve for rnd in rounds_with_data), f"burst before the valve threshold: {rounds_with_data}")
        self.assertLessEqual(len(sent), 2 * frag_total, f"more than one safety-valve re-burst: {[(r['round'], r['frag_idx']) for r in sent]}")
        self.assertEqual(b.owner.received.count(big), 1)
        self.assertNotIn(b.prefix, a.iface._raw_disabled_until, "re-query rounds must not count as raw fallback strikes")


class BindRerequestScheduleTests(SingleNodeCase):

    def test_doubles_from_initial_to_cap(self):
        iface = self.iface
        original = (iface.peer_discovery_rerequest_initial_s, iface.peer_discovery_rerequest_interval_s)
        try:
            iface.peer_discovery_rerequest_initial_s = 60.0
            iface.peer_discovery_rerequest_interval_s = 1800.0  # production values; the fast profile caps at 15s
            seq = [iface._next_rerequest_interval_s(None)]
            for _ in range(8):
                seq.append(iface._next_rerequest_interval_s(seq[-1]))
            self.assertEqual(seq[:6], [60.0, 120.0, 240.0, 480.0, 960.0, 1800.0])
            self.assertEqual(seq[-1], 1800.0)
        finally:
            iface.peer_discovery_rerequest_initial_s, iface.peer_discovery_rerequest_interval_s = original

    def test_initial_never_exceeds_cap(self):
        iface = self.iface
        original = iface.peer_discovery_rerequest_initial_s
        try:
            iface.peer_discovery_rerequest_initial_s = iface.peer_discovery_rerequest_interval_s * 10
            self.assertEqual(iface._next_rerequest_interval_s(None), iface.peer_discovery_rerequest_interval_s)
        finally:
            iface.peer_discovery_rerequest_initial_s = original


if __name__ == "__main__":
    unittest.main()
