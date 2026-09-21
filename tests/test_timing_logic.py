"""
Pure-logic tests for the reliability engine's bookkeeping: adaptive ACK
timeout, stale-path failure counting and reset, path-discovery backoff,
unknown-destination backoff, small-mesh mode, supplement target
selection, fragment spacing tiers, reassembly/dedup, and the two
concurrency primitives (priority lock, duty-cycle limiter). Each of
these used to be validated only by watching a field test.
"""
import asyncio
import os
import time
import unittest

from tests._support import SingleNodeCase, node_prefix

PEER = "abcdef012345"
OTHER = "0123456789ab"


class AdaptiveAckTimeoutTests(SingleNodeCase):

    def setUp(self):
        self.iface._ack_rtt.clear()

    def test_firmware_timeout_until_enough_samples(self):
        iface = self.iface
        self.assertEqual(iface._adaptive_ack_timeout(PEER, 10.0), (10.0, "firmware"))
        for _ in range(iface.direct_ack_rtt_min_samples - 1):
            iface._record_ack_rtt(PEER, 0.5)
        self.assertEqual(iface._adaptive_ack_timeout(PEER, 10.0)[1], "firmware")
        iface._record_ack_rtt(PEER, 0.5)
        timeout, source = iface._adaptive_ack_timeout(PEER, 10.0)
        self.assertEqual(source, "measured")
        self.assertLess(timeout, 10.0)
        self.assertGreaterEqual(timeout, iface.direct_ack_rtt_min_timeout_s)

    def test_measured_never_exceeds_firmware_timeout(self):
        iface = self.iface
        for _ in range(iface.direct_ack_rtt_min_samples + 2):
            iface._record_ack_rtt(PEER, 30.0)
        self.assertEqual(iface._adaptive_ack_timeout(PEER, 5.0), (5.0, "firmware"))

    def test_invalidate_drops_samples(self):
        iface = self.iface
        for _ in range(iface.direct_ack_rtt_min_samples):
            iface._record_ack_rtt(PEER, 0.4)
        self.assertEqual(iface._adaptive_ack_timeout(PEER, 10.0)[1], "measured")
        iface._invalidate_ack_rtt(PEER, "test")
        self.assertEqual(iface._adaptive_ack_timeout(PEER, 10.0)[1], "firmware")
        self.assertEqual(iface._rtt_capture_fields(PEER)["rtt_samples"], 0)

    def test_estimator_tracks_rtt(self):
        iface = self.iface
        for _ in range(20):
            iface._record_ack_rtt(PEER, 1.0)
        st = iface._ack_rtt[PEER]
        self.assertAlmostEqual(st["srtt"], 1.0, places=3)
        self.assertLess(st["rttvar"], 0.1)
        iface._record_ack_rtt(PEER, 3.0)
        self.assertGreater(iface._ack_rtt[PEER]["srtt"], 1.0)
        self.assertGreater(iface._ack_rtt[PEER]["rttvar"], 0.1)

    def test_disabled_always_firmware(self):
        iface = self.iface
        original = iface.direct_ack_rtt_adaptive_enabled
        try:
            iface.direct_ack_rtt_adaptive_enabled = False
            for _ in range(iface.direct_ack_rtt_min_samples + 1):
                iface._record_ack_rtt(PEER, 0.1)
            self.assertEqual(iface._adaptive_ack_timeout(PEER, 7.0), (7.0, "firmware"))
        finally:
            iface.direct_ack_rtt_adaptive_enabled = original


class StalePathResetTests(SingleNodeCase):

    def setUp(self):
        self.iface._direct_path_failures.clear()
        self.iface._resolved_paths.clear()

    def _resolve(self, prefix, age_s=0.0):
        self.iface._resolved_paths[prefix] = self.module._ResolvedPath(
            out_path_hex="19", out_path_len=1, out_path_hash_len=1, resolved_at=time.monotonic() - age_s,
        )

    def _fail(self, prefix, waited_full_timeout=True):
        self.on_loop(self.iface.record_direct_send_result, prefix, False, waited_full_timeout)
        time.sleep(0.05)  # let any spawned reset task run

    def test_success_clears_failures(self):
        iface = self.iface
        iface._direct_path_failures[PEER] = 2
        self.on_loop(iface.record_direct_send_result, PEER, True, True)
        self.assertNotIn(PEER, iface._direct_path_failures)

    def test_short_timeout_attempts_do_not_count(self):
        self._fail(PEER, waited_full_timeout=False)
        self.assertNotIn(PEER, self.iface._direct_path_failures)

    def test_below_threshold_keeps_path(self):
        self._resolve(PEER, age_s=1000)
        for _ in range(self.iface.direct_path_reset_threshold - 1):
            self._fail(PEER)
        self.assertIn(PEER, self.iface._resolved_paths)
        self.assertEqual(self.iface._direct_path_failures[PEER], self.iface.direct_path_reset_threshold - 1)

    def test_threshold_resets_old_path(self):
        self._resolve(PEER, age_s=self.iface.direct_path_reset_min_age_s + 1)
        for _ in range(self.iface.direct_path_reset_threshold):
            self._fail(PEER)
        self.assertNotIn(PEER, self.iface._resolved_paths)
        self.assertNotIn(PEER, self.iface._direct_path_failures)

    def test_threshold_spares_freshly_confirmed_path(self):
        self._resolve(PEER, age_s=0.0)
        for _ in range(self.iface.direct_path_reset_threshold + 1):
            self._fail(PEER)
        self.assertIn(PEER, self.iface._resolved_paths)
        # Failures are deliberately retained so the reset fires the moment the path is old enough.
        self.assertGreaterEqual(self.iface._direct_path_failures[PEER], self.iface.direct_path_reset_threshold)

    def test_good_rssi_raises_patience(self):
        iface = self.iface
        self._resolve(PEER, age_s=1000)
        good = iface.direct_path_reset_rssi_floor + 10
        for _ in range(iface.direct_path_reset_threshold):
            self.on_loop(iface.record_direct_send_result, PEER, False, True, good)
        time.sleep(0.05)
        self.assertIn(PEER, iface._resolved_paths)


class PathDiscoveryBackoffTests(SingleNodeCase):

    def setUp(self):
        self.iface._path_discovery_failures.clear()
        self.iface._path_discovery_backoff_until.clear()

    def test_backoff_grows_geometrically_and_caps(self):
        iface = self.iface
        self.assertFalse(iface._path_discovery_in_backoff(PEER))
        cooldowns = []
        for _ in range(12):
            iface._record_path_discovery_failure_round(PEER)
            cooldowns.append(iface._path_discovery_backoff_until[PEER] - time.monotonic())
        self.assertTrue(iface._path_discovery_in_backoff(PEER))
        self.assertAlmostEqual(cooldowns[0], iface.path_discovery_base_cooldown_s, delta=0.05)
        self.assertAlmostEqual(cooldowns[1], iface.path_discovery_base_cooldown_s * iface.path_discovery_backoff_factor, delta=0.05)
        self.assertLessEqual(max(cooldowns), iface.path_discovery_max_cooldown_s + 0.05)
        self.assertAlmostEqual(cooldowns[-1], iface.path_discovery_max_cooldown_s, delta=0.05)

    def test_success_clears(self):
        iface = self.iface
        iface._record_path_discovery_failure_round(PEER)
        iface._record_path_discovery_success(PEER)
        self.assertFalse(iface._path_discovery_in_backoff(PEER))
        self.assertNotIn(PEER, iface._path_discovery_failures)

    def test_discover_path_skips_while_in_backoff(self):
        iface = self.iface
        iface._record_path_discovery_failure_round(PEER)
        before = iface.radio_counter_snapshot() if hasattr(iface, "radio_counter_snapshot") else None
        result = self.node.run_on_loop(iface.discover_path(PEER))
        self.assertIsNone(result)
        self.assertEqual(self.node.radio.counters.get("path_req_sent", 0), 0)


class UnknownDestinationBackoffTests(SingleNodeCase):

    def setUp(self):
        self.iface._unknown_dest_attempts.clear()
        self.iface._unknown_dest_backoff_until.clear()

    def test_threshold_then_backoff_then_clear(self):
        iface = self.iface
        dest = os.urandom(16)
        for _ in range(iface.UNKNOWN_DEST_BOOTSTRAP_FAILURE_THRESHOLD - 1):
            iface._record_unknown_dest_attempt(dest)
            self.assertFalse(iface._unknown_dest_in_backoff(dest))
        iface._record_unknown_dest_attempt(dest)
        self.assertTrue(iface._unknown_dest_in_backoff(dest))
        iface._clear_unknown_dest_backoff(dest)
        self.assertFalse(iface._unknown_dest_in_backoff(dest))
        self.assertFalse(iface._unknown_dest_in_backoff(None))


class RoutingSelectionTests(SingleNodeCase):

    def setUp(self):
        self.iface._peers.clear()
        self.iface._resolved_paths.clear()
        self.iface._direct_path_failures.clear()

    def _peer(self, prefix, upstream=False, last_seen=None):
        self.iface._peers[prefix] = self.module._PeerRecord(
            pubkey_prefix=prefix, has_upstream_rns=upstream, last_seen=last_seen if last_seen is not None else time.time(),
        )

    def test_small_mesh_mode_boundaries(self):
        iface = self.iface
        self.assertFalse(iface._in_small_mesh_mode())
        for i in range(iface.SMALL_MESH_DIRECT_ONLY_MAX_PEERS):
            self._peer(f"{i:012x}")
            self.assertTrue(iface._in_small_mesh_mode())
        self._peer("f" * 12)
        self.assertFalse(iface._in_small_mesh_mode())

    def test_bootstrap_targets_prefer_healthy_then_recent(self):
        iface = self.iface
        now = time.time()
        self._peer("a" * 12, last_seen=now - 10)      # healthy, older
        self._peer("b" * 12, last_seen=now)           # healthy, newest
        self._peer("c" * 12, last_seen=now + 5)       # newest but failing
        iface._direct_path_failures["c" * 12] = 2
        targets = iface._select_bootstrap_supplement_targets()
        self.assertEqual(len(targets), min(3, iface.bootstrap_direct_supplement_cap))
        self.assertEqual(targets[0], "b" * 12)
        self.assertEqual(targets[1], "a" * 12)
        self.assertNotIn("c" * 12, targets[: iface.bootstrap_direct_supplement_cap] if iface.bootstrap_direct_supplement_cap < 3 else [])

    def test_path_request_supplement_needs_router_with_resolved_path(self):
        iface = self.iface
        self._peer("a" * 12, upstream=True)
        self._peer("b" * 12, upstream=False)
        self._peer("c" * 12, upstream=True)
        iface._resolved_paths["a" * 12] = self.module._ResolvedPath("", 0, 1, time.monotonic())
        iface._resolved_paths["b" * 12] = self.module._ResolvedPath("", 0, 1, time.monotonic())
        self.assertEqual(iface._select_direct_supplement_targets(), ["a" * 12])

    def test_all_bound_peers_most_recent_first(self):
        now = time.time()
        self._peer("a" * 12, last_seen=now - 5)
        self._peer("b" * 12, last_seen=now)
        self.assertEqual(self.iface._all_bound_peer_prefixes(), ["b" * 12, "a" * 12])


class SpacingTests(SingleNodeCase):

    def test_fragment_spacing_tiers(self):
        iface = self.iface
        self.assertEqual(iface._fragment_spacing_range(None), (iface.fragment_delay_min_s, iface.fragment_delay_max_s))
        self.assertEqual(iface._fragment_spacing_range(0), (iface.fragment_delay_zero_hop_min_s, iface.fragment_delay_zero_hop_max_s))
        self.assertEqual(
            iface._fragment_spacing_range(3),
            (iface.fragment_delay_per_hop_min_s * 3, iface.fragment_delay_per_hop_max_s * 3),
        )


class ReassemblyTests(SingleNodeCase):

    def _header(self, pkt_id, frag_idx, frag_total, attempt=0):
        return self.module._FrameHeader(self.iface.PROTOCOL_VERSION, True, False, pkt_id, frag_idx, frag_total, attempt)

    def test_out_of_order_reassembly_then_dedup(self):
        iface = self.iface
        chunks = [os.urandom(20) for _ in range(4)]
        key = iface._reassembly_key(self._header(77, 0, 4), "peer1", mode="direct")
        results = []
        for idx in (2, 0, 3, 1):
            results.append(self.on_loop(iface._add_channel_fragment, key, self._header(77, idx, 4), chunks[idx]))
        self.assertEqual(results[:3], [None, None, None])
        self.assertEqual(results[3], b"".join(chunks))
        self.assertTrue(self.on_loop(iface._dedup_contains, key))
        self.assertNotIn(key, iface._reassembly)

    def test_channel_and_direct_keys_never_collide(self):
        iface = self.iface
        h = self._header(5, 0, 2)
        self.assertNotEqual(iface._reassembly_key(h, "x", mode="channel"), iface._reassembly_key(h, "x", mode="direct"))

    def test_bucket_capacity_evicts_oldest(self):
        iface = self.iface
        original = iface.reassembly_max_keys
        try:
            iface.reassembly_max_keys = 3
            keys = []
            for pkt_id in range(4):
                key = iface._reassembly_key(self._header(1000 + pkt_id, 0, 2), "evict", mode="channel")
                keys.append(key)
                self.on_loop(iface._add_channel_fragment, key, self._header(1000 + pkt_id, 0, 2), b"a")
                time.sleep(0.01)
            self.assertNotIn(keys[0], iface._reassembly)
            self.assertTrue(all(k in iface._reassembly for k in keys[1:]))
        finally:
            iface.reassembly_max_keys = original
            for k in keys:
                iface._reassembly.pop(k, None)


class PriorityLockTests(unittest.TestCase):

    def test_grants_by_priority_then_fifo(self):
        from tests._support import load_interface_module
        module = load_interface_module()

        async def scenario():
            lock = module._PriorityAsyncLock()
            order = []
            await lock.acquire(1)

            async def waiter(tag, prio):
                async with lock(prio):
                    order.append(tag)

            tasks = [asyncio.create_task(waiter(t, p)) for t, p in (("low1", 2), ("normal1", 1), ("hs1", 0), ("normal2", 1), ("hs2", 0))]
            await asyncio.sleep(0.01)
            lock.release()
            await asyncio.gather(*tasks)
            return order

        self.assertEqual(asyncio.run(scenario()), ["hs1", "hs2", "normal1", "normal2", "low1"])

    def test_cancelled_waiter_does_not_deadlock(self):
        from tests._support import load_interface_module
        module = load_interface_module()

        async def scenario():
            lock = module._PriorityAsyncLock()
            await lock.acquire(1)
            t = asyncio.create_task(lock.acquire(1))
            await asyncio.sleep(0.01)
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
            lock.release()
            self.assertFalse(lock.locked())
            await asyncio.wait_for(lock.acquire(1), timeout=1.0)
            lock.release()

        asyncio.run(scenario())


class DutyCycleTests(unittest.TestCase):

    def test_waits_when_budget_exhausted(self):
        from tests._support import load_interface_module
        module = load_interface_module()

        async def scenario():
            limiter = module._DutyCycleLimiter(window_s=0.6, max_fraction=0.5)
            # Alpha 0.1.5: wait_for_budget returns (delay, ledger); one cap
            # given means both ledgers share it (the pre-0.1.5 behaviour).
            self.assertEqual(await limiter.wait_for_budget(0.1), (0.0, None))
            limiter.record(0.3)
            t0 = time.monotonic()
            waited, ledger = await limiter.wait_for_budget(0.1)
            self.assertGreater(waited, 0.0)
            self.assertEqual(ledger, "relayed")
            self.assertGreater(time.monotonic() - t0, 0.05)
            self.assertLess(time.monotonic() - t0, 1.0)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
