"""
Regression tests for the second audit of the 2026-09-19 evening captures
(module docstring, "SECOND AUDIT"): a plain delivery PROOF leaves the
handshake tier, the hop-1 abort arms without per-peer echo samples, and
fragmented sends to one peer are bounded to a few in flight.
"""
import asyncio
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet

PEER = "aabbccddeeff"
OTHER = "112233445566"


class ProofPriorityTier(SingleNodeCase):
    """Desktop capture, evening session: 188 plain PROOFs at the handshake
    tier held the radio lock for 1716s and made completion ANSWERs wait
    up to 55s. Only a PROOF a Link or Resource hangs on keeps the tier."""

    def _tier(self, kind):
        return self.iface._priority_tier(self.iface._parse_rns_header(build_rns_packet(kind)))

    def test_plain_delivery_proof_rides_the_answer_tier(self):
        self.assertEqual(self._tier("proof"), self.iface.PRIORITY_ANSWER)

    def test_link_and_resource_proofs_keep_the_handshake_tier(self):
        iface = self.iface
        self.assertEqual(self._tier("lrproof"), iface.PRIORITY_HANDSHAKE)
        self.assertEqual(self._tier("link_request"), iface.PRIORITY_HANDSHAKE)
        header = iface._parse_rns_header(build_rns_packet("proof"))
        self.assertEqual(iface._priority_tier(header._replace(context=RNS.Packet.RESOURCE_PRF)), iface.PRIORITY_HANDSHAKE)
        self.assertEqual(iface._priority_tier(header._replace(context=RNS.Packet.KEEPALIVE)), iface.PRIORITY_HANDSHAKE)
        self.assertFalse(iface._proof_is_link_class(header))
        self.assertTrue(iface._proof_is_link_class(header._replace(context=RNS.Packet.LRPROOF)))

    def test_answer_tier_gets_the_ordinary_budget_and_no_duty_cycle_exemption(self):
        iface = self.iface
        self.assertFalse(iface._duty_cycle_exempt(iface.PRIORITY_ANSWER))
        self.assertLess(iface.direct_send_attempts, iface.direct_send_attempts_handshake)


class Hop1AbortArmsWithoutSamples(SingleNodeCase):
    """Laptop capture: 59 of 72 first-hop-silent misses ran the full
    firmware timeout because a path change or restart had just cleared
    the per-peer echo samples."""

    def setUp(self):
        self.iface._echo_stats.clear()
        self.iface._echo_stats_all.clear()
        self.iface.direct_hop1_abort_default_s = 8.0

    def test_default_deadline_when_nothing_is_measured(self):
        iface = self.iface
        self.assertEqual(iface._hop1_abort_deadline_s(PEER, 1, 20.0), 8.0)
        self.assertEqual(iface._hop1_abort_deadline_s(PEER, 2, 21.1), 8.0)
        # Never longer than the ACK wait it shortens, never at zero hops.
        self.assertIsNone(iface._hop1_abort_deadline_s(PEER, 1, 6.0))
        self.assertIsNone(iface._hop1_abort_deadline_s(PEER, 0, 20.0))
        self.assertIsNone(iface._hop1_abort_deadline_s(None, 1, 20.0))

    def test_zero_default_restores_samples_only_arming(self):
        self.iface.direct_hop1_abort_default_s = 0.0
        self.assertIsNone(self.iface._hop1_abort_deadline_s(PEER, 1, 20.0))

    def test_session_pool_beats_the_default_and_per_peer_samples_beat_the_pool(self):
        iface = self.iface
        n = iface.direct_hop1_abort_min_samples
        for _ in range(n):
            iface._record_echo(OTHER, 2, 3.0)
        # Another peer's echoes are evidence about repeaters in general.
        self.assertAlmostEqual(iface._hop1_abort_deadline_s(PEER, 1, 20.0), max(iface.direct_hop1_abort_min_s, 6.0))
        for _ in range(n):
            iface._record_echo(PEER, 1, 1.0)
        self.assertAlmostEqual(iface._hop1_abort_deadline_s(PEER, 1, 20.0), iface.direct_hop1_abort_min_s)
        # A path change clears this peer's samples but not the pool.
        iface._clear_peer_path_stats(PEER)
        self.assertAlmostEqual(iface._hop1_abort_deadline_s(PEER, 1, 20.0), max(iface.direct_hop1_abort_min_s, 6.0))


class FragmentedSendsPerPeerAreBounded(SingleNodeCase):
    """Desktop capture: 14 completion windows open at once, the querier
    transmitting when 11 of 25 lost ANSWERs arrived, Resource parts
    delivered out of order and discarded by RNS. Two in flight per peer."""

    def setUp(self):
        self.iface._fragmented_send_slots.clear()
        self.iface.direct_fragmented_max_in_flight = 2
        self._orig = self.iface._send_direct_fragmented_payload

    def tearDown(self):
        self.iface._send_direct_fragmented_payload = self._orig

    def test_handshake_class_and_disabled_cap_get_no_slot(self):
        iface = self.iface
        self.assertIsNone(iface._fragmented_send_slot(PEER, iface.PRIORITY_HANDSHAKE))
        self.assertIsNotNone(iface._fragmented_send_slot(PEER, iface.PRIORITY_NORMAL))
        iface.direct_fragmented_max_in_flight = 0
        self.assertIsNone(iface._fragmented_send_slot(PEER, iface.PRIORITY_NORMAL))

    def test_at_most_two_fragmented_sends_per_peer_run_concurrently(self):
        iface = self.iface
        big = b"x" * (iface._direct_payload_budget() + 200)
        state = {"active": 0, "peak": 0, "runs": 0}

        async def fake_fragmented(target, peer_prefix, data, priority, hop_count, expires_at, send_info):
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
            state["runs"] += 1
            await asyncio.sleep(0.15)
            state["active"] -= 1
            send_info["method"] = "raw"
            return True

        iface._send_direct_fragmented_payload = fake_fragmented

        async def drive():
            infos = [{} for _ in range(5)]
            results = await asyncio.gather(*[
                iface._send_direct_payload("target", PEER, big, priority=iface.PRIORITY_NORMAL,
                                           hop_count=1, send_info=info)
                for info in infos
            ])
            return results, infos

        results, infos = self.node.run_on_loop(drive(), timeout=10.0)
        self.assertEqual(results, [True] * 5)
        self.assertEqual(state["runs"], 5)
        self.assertEqual(state["peak"], 2)
        self.assertTrue(all("slot_wait_s" in info for info in infos))
        self.assertGreater(max(info["slot_wait_s"] for info in infos), 0.1)

    def test_a_send_that_never_gets_a_slot_is_dropped_not_stuck(self):
        iface = self.iface
        big = b"x" * (iface._direct_payload_budget() + 200)

        async def slow(target, peer_prefix, data, priority, hop_count, expires_at, send_info):
            await asyncio.sleep(1.0)
            return True

        iface._send_direct_fragmented_payload = slow

        async def drive():
            fillers = [asyncio.ensure_future(iface._send_direct_payload(
                "target", PEER, big, priority=iface.PRIORITY_NORMAL, hop_count=1, send_info={}))
                for _ in range(2)]
            await asyncio.sleep(0.05)
            info = {}
            dropped_before = iface._outgoing_dropped_total
            started = time.monotonic()
            # Already past its deadline: must give up at once, not wait a second.
            result = await iface._send_direct_payload(
                "target", PEER, big, priority=iface.PRIORITY_NORMAL, hop_count=1,
                expires_at=time.monotonic() - 1.0, send_info=info,
            )
            elapsed = time.monotonic() - started
            await asyncio.gather(*fillers)
            return result, info, elapsed, iface._outgoing_dropped_total - dropped_before

        result, info, elapsed, dropped = self.node.run_on_loop(drive(), timeout=10.0)
        self.assertFalse(result)
        self.assertEqual(info["method"], "slot_expired")
        self.assertEqual(dropped, 1)
        self.assertLess(elapsed, 0.5)


if __name__ == "__main__":
    unittest.main()
