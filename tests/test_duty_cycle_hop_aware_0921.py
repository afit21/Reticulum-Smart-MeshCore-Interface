"""
Hop-aware duty-cycle cap (alpha 0.1.5, item 1, 2026-09-21).

Field evidence: the 2026-09-21 zero-hop 12-part page transfer took 147 s, of
which 109 s were duty-cycle waits at the single 30% cap -- two adjacent
radios throttled as if every frame cost a repeater air. The owner's rule
that followed: 85% for zero-hop DIRECT traffic, 30% for anything a repeater
relays (any DIRECT frame with a routed path, every CHANNEL flood), and the
30% is never loosened.

Pinned:
  * `_DutyCycleLimiter(window, max_fraction, max_fraction_total)` keeps two
    ledgers over the same window: `record(d, relayed)` charges the total
    ledger always and the relayed ledger only for relayed frames;
    `limiting_ledger` names the budget that would hold a frame ("relayed"
    first, then "total", else None) -- a zero-hop frame waits on the total
    budget only, a relayed frame waits on both; `wait_for_budget` returns
    `(delay, ledger)`; one cap given means both budgets share it (the
    pre-0.1.5 behaviour); a single over-budget frame is let through while
    its ledger is empty; samples age out of both ledgers after the window;
  * `_relayed_frame(hop_count, peer_prefix)`: hop_count > 0 is relayed, 0
    is not, None consults the peer's resolved path, and no path at all is
    charged as relayed (the stricter budget);
  * shipped defaults: `duty_cycle_max_fraction` 0.30 and
    `duty_cycle_max_fraction_zero_hop` 0.85;
  * `_pre_transmit_gate(..., relayed=, telemetry=)` reports the frame's hop
    class and the ledger that held it, so the capture's
    `direct_attempt_result` / `raw_fragment_sent` records can carry it.
"""
import asyncio
import time
import unittest

from tests._support import load_interface_module, SingleNodeCase, quiet_rns

PEER = "aabbccddeeff"


class HopAwareLimiterTests(unittest.TestCase):
    """The pure limiter: two ledgers, no interface, no waiting beyond a few
    hundred milliseconds."""

    @classmethod
    def setUpClass(cls):
        quiet_rns()
        cls.module = load_interface_module()

    def limiter(self, window_s=1.0, relayed_cap=0.30, total_cap=0.85):
        return self.module._DutyCycleLimiter(window_s, relayed_cap, total_cap)

    def test_record_charges_total_always_and_relayed_only_when_relayed(self):
        lim = self.limiter()
        lim.record(0.10, relayed=True)
        lim.record(0.20, relayed=False)
        now = time.monotonic()
        self.assertAlmostEqual(lim._busy_s(now), 0.30, places=6)
        self.assertAlmostEqual(lim._relayed_busy_s(now), 0.10, places=6)

    def test_zero_hop_frame_waits_on_the_total_budget_only(self):
        # window 1.0 s: relayed cap 0.30 s, total cap 0.85 s.
        lim = self.limiter()
        lim.record(0.29, relayed=True)      # relayed just under its cap; total 0.29
        lim.record(0.51, relayed=False)     # total 0.80, under its cap
        # Zero-hop: relayed is (nearly) exhausted but total has 0.05 s of room.
        self.assertIsNone(lim.limiting_ledger(0.04, relayed=False))
        # ... and is held by "total" only once total would be exceeded.
        self.assertEqual(lim.limiting_ledger(0.06, relayed=False), "total")
        # Push relayed past its cap outright: a zero-hop frame still does
        # not care about the relayed ledger.
        lim = self.limiter()
        lim.record(0.30, relayed=True)      # relayed exactly at cap
        self.assertEqual(lim.limiting_ledger(0.05, relayed=True), "relayed")
        self.assertIsNone(lim.limiting_ledger(0.05, relayed=False), "relayed exhausted, total has room")

    def test_relayed_frame_waits_on_both_budgets(self):
        # (b1) relayed at its 30% cap, total far below 85%: "relayed".
        lim = self.limiter()
        lim.record(0.30, relayed=True)
        self.assertEqual(lim.limiting_ledger(0.01, relayed=True), "relayed")
        # (b2) relayed at 0, total at the 85% cap from zero-hop traffic: "total".
        lim = self.limiter()
        lim.record(0.85, relayed=False)
        now = time.monotonic()
        self.assertEqual(lim._relayed_busy_s(now), 0.0)
        self.assertEqual(lim.limiting_ledger(0.01, relayed=True), "total")
        # The relayed ledger is checked first when both would hold the frame.
        lim = self.limiter()
        lim.record(0.30, relayed=True)
        lim.record(0.55, relayed=False)
        self.assertEqual(lim.limiting_ledger(0.01, relayed=True), "relayed")

    def test_wait_for_budget_holds_relayed_and_passes_zero_hop_at_the_same_moment(self):
        async def scenario():
            lim = self.limiter(window_s=0.5, relayed_cap=0.30, total_cap=0.85)
            lim.record(0.5 * 0.30, relayed=True)            # relayed exhausted; total at 30%
            # A zero-hop frame goes immediately.
            self.assertEqual(await lim.wait_for_budget(0.05, relayed=False), (0.0, None))
            # A relayed frame at the same moment waits on the relayed ledger.
            t0 = time.monotonic()
            delay, ledger = await lim.wait_for_budget(0.05, relayed=True)
            took = time.monotonic() - t0
            self.assertGreater(delay, 0.0)
            self.assertEqual(ledger, "relayed")
            self.assertGreater(took, 0.05)
            self.assertLess(took, 1.5)
            # And the reverse: total exhausted by zero-hop traffic holds a
            # relayed frame on the total ledger.
            lim = self.limiter(window_s=0.5, relayed_cap=0.30, total_cap=0.85)
            lim.record(0.5 * 0.85, relayed=False)
            delay, ledger = await lim.wait_for_budget(0.05, relayed=True)
            self.assertGreater(delay, 0.0)
            self.assertEqual(ledger, "total")

        asyncio.run(scenario())

    def test_one_cap_means_both_ledgers_share_it(self):
        # `_DutyCycleLimiter(w, 0.5)` -- the pre-0.1.5 constructor -- caps
        # total at the same 50%, so a zero-hop frame is held exactly where
        # a relayed one is.
        lim = self.module._DutyCycleLimiter(1.0, 0.5)
        self.assertEqual(lim._max_busy_s, lim._max_total_s)
        lim.record(0.5, relayed=False)
        self.assertEqual(lim.limiting_ledger(0.01, relayed=False), "total")
        self.assertEqual(lim.limiting_ledger(0.01, relayed=True), "total")
        lim = self.module._DutyCycleLimiter(1.0, 0.5)
        lim.record(0.5)                                      # relayed=True is the default
        self.assertEqual(lim.limiting_ledger(0.01, relayed=True), "relayed")
        self.assertEqual(lim.limiting_ledger(0.01, relayed=False), "total")

    def test_a_single_over_budget_frame_passes_while_its_ledger_is_empty(self):
        # Empty limiter: a frame longer than either whole budget is let through.
        lim = self.limiter()
        self.assertIsNone(lim.limiting_ledger(5.0, relayed=True))
        self.assertIsNone(lim.limiting_ledger(5.0, relayed=False))
        # Relayed ledger empty, total ledger non-empty: an over-budget
        # relayed frame is held by "total", not "relayed".
        lim = self.limiter()
        lim.record(0.01, relayed=False)
        self.assertEqual(lim.limiting_ledger(5.0, relayed=True), "total")
        # Relayed ledger non-empty: "relayed" comes first.
        lim = self.limiter()
        lim.record(0.01, relayed=True)
        self.assertEqual(lim.limiting_ledger(5.0, relayed=True), "relayed")

        async def scenario():
            lim = self.limiter()
            self.assertEqual(await lim.wait_for_budget(5.0, relayed=True), (0.0, None))
            self.assertEqual(await lim.wait_for_budget(5.0, relayed=False), (0.0, None))

        asyncio.run(scenario())

    def test_samples_age_out_of_both_ledgers_after_the_window(self):
        lim = self.limiter(window_s=0.2)
        lim.record(0.06, relayed=True)      # relayed at cap (0.2 * 0.30)
        lim.record(0.11, relayed=False)     # total at cap (0.17)
        self.assertEqual(lim.limiting_ledger(0.01, relayed=True), "relayed")
        self.assertEqual(lim.limiting_ledger(0.01, relayed=False), "total")
        later = time.monotonic() + 0.25
        self.assertIsNone(lim.limiting_ledger(0.01, relayed=True, now=later))
        self.assertIsNone(lim.limiting_ledger(0.01, relayed=False, now=later))
        self.assertEqual(len(lim._samples), 0)
        self.assertEqual(len(lim._relayed), 0)


class RelayedFrameClass(SingleNodeCase):
    """`_relayed_frame` on a live interface."""

    def tearDown(self):
        self.iface._resolved_paths.pop(PEER, None)

    def _set_path(self, out_path_len):
        self.iface._resolved_paths[PEER] = self.module._ResolvedPath(
            out_path_hex="ab" * out_path_len, out_path_len=out_path_len,
            out_path_hash_len=1, resolved_at=time.monotonic(),
        )

    def test_hop_count_decides_when_given(self):
        self.assertFalse(self.iface._relayed_frame(0))
        self.assertFalse(self.iface._relayed_frame(0, PEER))
        self.assertTrue(self.iface._relayed_frame(2))
        self.assertTrue(self.iface._relayed_frame(1, PEER))
        # An explicit hop count wins over whatever the resolved path says.
        self._set_path(3)
        self.assertFalse(self.iface._relayed_frame(0, PEER))

    def test_resolved_path_decides_when_hop_count_is_unknown(self):
        self._set_path(0)
        self.assertFalse(self.iface._relayed_frame(None, PEER))
        self._set_path(3)
        self.assertTrue(self.iface._relayed_frame(None, PEER))

    def test_unknown_route_is_charged_as_relayed(self):
        self.assertNotIn(PEER, self.iface._resolved_paths)
        self.assertTrue(self.iface._relayed_frame(None, PEER))
        self.assertTrue(self.iface._relayed_frame(None, None))
        self.assertTrue(self.iface._relayed_frame(None))


class ShippedCaps(unittest.TestCase):
    """The two caps as `_configure_transport` ships them, pinned on a bare
    instance (the live harness's FAST_TIMING overrides both)."""

    def test_shipped_duty_cycle_caps(self):
        quiet_rns()
        module = load_interface_module()
        cls = module.SmartMeshCoreInterface
        bare = cls.__new__(cls)
        bare._configure_transport({})
        # The owner's rule (2026-09-21): 30% for anything a repeater relays.
        # This number must never be loosened -- see the memory / history
        # entry for alpha 0.1.5; a change here is a policy change, not a
        # tuning knob.
        self.assertEqual(bare.duty_cycle_max_fraction, 0.30)
        # 85% for zero-hop DIRECT traffic between two adjacent radios,
        # which costs nobody else's repeater any air.
        self.assertEqual(bare.duty_cycle_max_fraction_zero_hop, 0.85)
        self.assertTrue(bare.duty_cycle_enabled)
        self.assertEqual(bare.duty_cycle_window_s, 60.0)


class GateTelemetry(SingleNodeCase):
    """`_pre_transmit_gate` reports the hop class and the ledger that held
    the frame."""

    def setUp(self):
        self._saved_limiter = self.iface._duty_cycle_impl

    def tearDown(self):
        self.iface._duty_cycle_impl = self._saved_limiter

    def _gate(self, relayed):
        telemetry = {}
        result = self.node.run_on_loop(
            self.iface._pre_transmit_gate("x" * 40, skip_quiet_defer=True, relayed=relayed, telemetry=telemetry),
            timeout=10.0,
        )
        return result, telemetry

    def test_idle_limiter_reports_no_ledger_and_the_hop_class(self):
        self.assertTrue(self.iface.duty_cycle_enabled)
        (quiet_s, duty_s, medium_s), t = self._gate(relayed=False)
        self.assertIs(t["duty_cycle_relayed"], False)
        self.assertIsNone(t["duty_cycle_ledger"])
        self.assertEqual(duty_s, 0.0)
        (quiet_s, duty_s, medium_s), t = self._gate(relayed=True)
        self.assertIs(t["duty_cycle_relayed"], True)
        self.assertIsNone(t["duty_cycle_ledger"])
        self.assertEqual(duty_s, 0.0)

    def test_zero_hop_frame_passes_an_exhausted_relayed_ledger(self):
        module = self.module
        limiter = module._DutyCycleLimiter(60.0, 0.30, 0.85)
        limiter.record(60.0 * 0.30, relayed=True)           # relayed cap spent for the whole minute
        self.iface._duty_cycle_impl = limiter
        # A relayed frame would now wait (not exercised: that is a 60 s wait).
        self.assertEqual(limiter.limiting_ledger(0.5, relayed=True), "relayed")
        # A zero-hop frame passes the gate with no wait: total is at 30% of 85%.
        t0 = time.monotonic()
        (quiet_s, duty_s, medium_s), t = self._gate(relayed=False)
        self.assertLess(time.monotonic() - t0, 2.0)
        self.assertIs(t["duty_cycle_relayed"], False)
        self.assertIsNone(t["duty_cycle_ledger"])
        self.assertEqual(duty_s, 0.0)
        # ... and was charged to the total ledger only.
        now = time.monotonic()
        self.assertAlmostEqual(limiter._relayed_busy_s(now), 18.0, places=6)
        self.assertGreater(limiter._busy_s(now), 18.0)
        # Still "relayed" for the next relayed frame: the zero-hop send did
        # not touch that ledger.
        self.assertEqual(limiter.limiting_ledger(0.5, relayed=True), "relayed")


if __name__ == "__main__":
    unittest.main()
