"""
Regression tests for the second audit of the 2026-09-19 evening captures
(module docstring, "SECOND AUDIT"): a plain delivery PROOF leaves the
handshake tier, the hop-1 abort arms without per-peer echo samples, and
fragmented sends to one peer can be bounded to a few in flight -- a cap
that is OFF by default since the night session measured it (see
`FragmentedSendsPerPeerAreBounded`).
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
    """Desktop capture, evening session: 14 completion windows open at once,
    the querier transmitting when 11 of 25 lost ANSWERs arrived, Resource
    parts delivered out of order and discarded by RNS. Two in flight per
    peer was the second audit's answer.

    Night session (`fieldtests/raw/Alpha0.1.2/*nighttest*`, build 3b56c11,
    the first with the cap on): it did not achieve its purpose and cost
    plenty, so the DEFAULT IS NOW 0 (off). Reconcile timeouts did not
    improve (desktop 53% timed out vs 35% the evening before); the
    desktop's fragmented sends waited a median 30s for a slot; four
    483-byte Resource parts were dropped after the 120s slot budget
    (method="slot_expired"); and two of the laptop's data packets were
    dropped while both of its slots were held by 30-minute LXMF announces
    reconciling at two hops -- the FIFO semaphore ignored priority. When
    the cap is enabled it is now priority-aware, announce-class sends have
    a single slot of their own, and a send that cannot get a slot in time
    proceeds with a warning instead of being dropped."""

    def setUp(self):
        self.iface._fragmented_send_slots.clear()
        self._orig_cap = self.iface.direct_fragmented_max_in_flight
        self.iface.direct_fragmented_max_in_flight = 2
        self._orig = self.iface._send_direct_fragmented_payload

    def tearDown(self):
        self.iface._send_direct_fragmented_payload = self._orig
        self.iface.direct_fragmented_max_in_flight = self._orig_cap
        self.iface._fragmented_send_slots.clear()

    def test_cap_defaults_to_two_non_dropping_slots(self):
        """Review 2026-09-20 (simulated one-hop page A/B, three seeds): with
        the drop removed and priority ordering, the cap at 2 delivered 12/12
        parts in 181-243s against 3-10/12 in 600s without it, so the default
        is 2 again. The fixture's interface was configured with the defaults
        (FAST_TIMING sets no cap key), so its value at setUp is the shipped
        default."""
        from tests._support import FAST_TIMING
        self.assertNotIn("direct_fragmented_max_in_flight", FAST_TIMING)
        self.assertEqual(self._orig_cap, 2)
        self.iface.direct_fragmented_max_in_flight = self._orig_cap
        self.assertIsNotNone(self.iface._fragmented_send_slot(PEER, self.iface.PRIORITY_NORMAL))

    def test_handshake_class_and_disabled_cap_get_no_slot(self):
        iface = self.iface
        self.assertIsNone(iface._fragmented_send_slot(PEER, iface.PRIORITY_HANDSHAKE))
        self.assertIsNotNone(iface._fragmented_send_slot(PEER, iface.PRIORITY_NORMAL))
        iface.direct_fragmented_max_in_flight = 0
        self.assertIsNone(iface._fragmented_send_slot(PEER, iface.PRIORITY_NORMAL))

    def test_announce_class_has_its_own_single_slot(self):
        """Laptop, night session: two data packets dropped while both data
        slots were held by announces reconciling at two hops. Announce-class
        (PRIORITY_LOW) sends now share one slot that is not a data slot."""
        iface = self.iface
        data_slot = iface._fragmented_send_slot(PEER, iface.PRIORITY_NORMAL)
        ann_slot = iface._fragmented_send_slot(PEER, iface.PRIORITY_LOW)
        self.assertIsNot(data_slot, ann_slot)
        self.assertEqual(data_slot.capacity, 2)
        self.assertEqual(ann_slot.capacity, 1)
        self.assertIs(iface._fragmented_send_slot(PEER, iface.PRIORITY_ANSWER), data_slot)
        self.assertIsNot(iface._fragmented_send_slot(OTHER, iface.PRIORITY_NORMAL), data_slot, "per peer")

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

    def test_a_higher_priority_waiter_gets_the_next_slot_first(self):
        """The FIFO semaphore served an ANSWER-tier proof behind queued bulk
        data. Waiters are now served by tier, FIFO within a tier."""
        iface = self.iface
        big = b"x" * (iface._direct_payload_budget() + 200)
        order = []

        async def fake_fragmented(target, peer_prefix, data, priority, hop_count, expires_at, send_info):
            order.append(send_info["tag"])
            await asyncio.sleep(0.1)
            return True

        iface._send_direct_fragmented_payload = fake_fragmented

        async def drive():
            def go(tag, prio):
                info = {"tag": tag}
                return asyncio.ensure_future(iface._send_direct_payload(
                    "target", PEER, big, priority=prio, hop_count=1, send_info=info))
            tasks = [go("h1", iface.PRIORITY_NORMAL), go("h2", iface.PRIORITY_NORMAL)]
            await asyncio.sleep(0.02)          # both slots held
            tasks.append(go("n1", iface.PRIORITY_NORMAL))
            tasks.append(go("n2", iface.PRIORITY_NORMAL))
            await asyncio.sleep(0.01)
            tasks.append(go("a1", iface.PRIORITY_ANSWER))   # arrives last, served first
            await asyncio.gather(*tasks)
            return order

        order = self.node.run_on_loop(drive(), timeout=10.0)
        self.assertEqual(order[:2], ["h1", "h2"])
        self.assertEqual(order[2], "a1", f"the ANSWER-tier send must get the next slot, got {order}")
        self.assertEqual(order[3:], ["n1", "n2"], "FIFO within the tier")

    def test_a_send_that_never_gets_a_slot_proceeds_instead_of_dropping(self):
        """Night session: four Resource parts and two data packets dropped
        as slot_expired. A drop is strictly worse than a late send -- RNS
        must notice and re-request through the same slow link -- so the
        slot is a pacing hint now: the budget runs out, the send goes."""
        iface = self.iface
        big = b"x" * (iface._direct_payload_budget() + 200)
        ran = []

        holders_seen = {}

        async def slow(target, peer_prefix, data, priority, hop_count, expires_at, send_info):
            ran.append(send_info.get("tag"))
            holders_seen[send_info.get("tag")] = iface._fragmented_send_slot(PEER, iface.PRIORITY_NORMAL).holders()
            await asyncio.sleep(1.0)
            send_info["method"] = "raw"
            return True

        iface._send_direct_fragmented_payload = slow

        async def drive():
            fillers = [asyncio.ensure_future(iface._send_direct_payload(
                "target", PEER, big, priority=iface.PRIORITY_NORMAL, hop_count=1, send_info={"tag": "filler"}))
                for _ in range(2)]
            await asyncio.sleep(0.05)
            info = {"tag": "late"}
            dropped_before = iface._outgoing_dropped_total
            started = time.monotonic()
            # Already past its deadline: must not wait a second for a slot --
            # and must not be dropped either.
            result = await iface._send_direct_payload(
                "target", PEER, big, priority=iface.PRIORITY_NORMAL, hop_count=1,
                expires_at=time.monotonic() - 1.0, send_info=info,
            )
            elapsed = time.monotonic() - started
            slot = iface._fragmented_send_slot(PEER, iface.PRIORITY_NORMAL)
            await asyncio.gather(*fillers)
            return result, info, elapsed, iface._outgoing_dropped_total - dropped_before, slot

        result, info, elapsed, dropped, slot = self.node.run_on_loop(drive(), timeout=10.0)
        self.assertTrue(result, "the send must go through")
        self.assertEqual(info["method"], "raw")
        self.assertNotEqual(info.get("method"), "slot_expired")
        self.assertEqual(dropped, 0)
        self.assertIn("late", ran)
        self.assertLess(elapsed, 1.6, "no extra slot wait beyond the (already expired) budget")
        self.assertIn("slot_wait_s", info)
        # The late send never held a permit, so it must not have released one:
        # the two fillers' permits are freed and nothing is left over.
        self.assertEqual(holders_seen["late"], 2, "the late sender ran while both permits were still held -- it must not have taken one")
        self.assertEqual(slot.holders(), 0, "and it must not have released one it never held")


if __name__ == "__main__":
    unittest.main()
