"""
Adaptive window collect (alpha 0.1.5, item 5, 2026-09-21).

M2's window collect was a fixed 0.75 s: every raw send waited it out, a lone
packet included (the zero-hop probe RTT carried ~0.7 s of it). RNS's
Resource sender emits a window's parts in one loop (`RNS/Resource.py`
`request`: `for part in requested_parts: part.send()`) and the outgoing
worker hands them over within a few loop turns, so a window's parts are
identifiable by arriving together.

Pinned:
  * `_window_collect_continue` (pure): never past the maximum or the part
    cap; keeps collecting while the outgoing queue holds packets or a part
    joined within the observed spacing; stops otherwise;
  * `_observed_part_spacing_s` (pure over the recorded arrivals): the floor
    (40 ms) with nothing observed, twice the median of the recent gaps that
    fell inside the maximum, clamped to [floor, maximum];
  * `direct_raw_window_collect` stays the maximum (0.75 s shipped);
  * a lone raw part starts within 50 ms of reaching the window machine;
  * four parts arriving together still form ONE window, and its collect
    ends well inside the maximum.
"""
import asyncio
import time
import unittest

from tests._support import SingleNodeCase
from tests.test_completion_report_one_hop_0920 import PEER, TARGET
from tests.test_reconcile_m2_window_0920 import _WindowSend


class PureCollectRules(SingleNodeCase):
    def test_collect_continue(self):
        cont = self.iface._window_collect_continue
        t0 = 100.0
        # past the maximum / the part cap: stop, whatever else is true
        self.assertFalse(cont(t0 + 0.75, t0, t0 + 0.74, 1, 6, 5, 0.5, 0.75))
        self.assertFalse(cont(t0 + 0.1, t0, t0 + 0.1, 6, 6, 5, 0.5, 0.75))
        # something still queued from RNS: keep collecting
        self.assertTrue(cont(t0 + 0.3, t0, t0, 1, 6, 1, 0.04, 0.75))
        # a part joined within the spacing: keep collecting
        self.assertTrue(cont(t0 + 0.3, t0, t0 + 0.28, 2, 6, 0, 0.04, 0.75))
        # nothing queued, no recent join: stop
        self.assertFalse(cont(t0 + 0.3, t0, t0 + 0.2, 2, 6, 0, 0.04, 0.75))
        self.assertFalse(cont(t0 + 0.05, t0, t0, 1, 6, 0, 0.04, 0.75), "a lone part stops after the floor")

    def test_observed_spacing(self):
        iface = self.iface
        floor = iface.RAW_WINDOW_COLLECT_FLOOR_S
        self.assertEqual(floor, 0.04)
        iface._raw_part_arrivals.pop(PEER, None)
        self.assertAlmostEqual(iface._observed_part_spacing_s(PEER, 0.75), floor)
        for t in (10.0, 10.01, 10.02, 10.03):            # a window's parts, 10 ms apart
            iface._note_raw_part_arrival(PEER, t)
        self.assertAlmostEqual(iface._observed_part_spacing_s(PEER, 0.75), floor, msg="2 x 10 ms is under the floor")
        iface._raw_part_arrivals.pop(PEER, None)
        for t in (20.0, 20.1, 20.2, 25.0, 25.1):         # 100 ms apart, one inter-window gap of 4.8 s ignored
            iface._note_raw_part_arrival(PEER, t)
        self.assertAlmostEqual(iface._observed_part_spacing_s(PEER, 0.75), 0.2)
        self.assertAlmostEqual(iface._observed_part_spacing_s(PEER, 0.15), 0.15, msg="never above the maximum")
        iface._raw_part_arrivals.pop(PEER, None)
        self.assertEqual(iface._observed_part_spacing_s(PEER, 0.0), 0.0)

    def test_collect_key_is_the_maximum(self):
        bare = self.module.SmartMeshCoreInterface.__new__(self.module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertEqual(bare.direct_raw_window_collect_s, 0.75)
        self.assertTrue(bare.direct_raw_window_enabled)


class LiveCollect(_WindowSend):
    def test_a_lone_part_starts_within_50_ms(self):
        iface = self.iface
        sent, sink, restore = self._install_window(lambda h, f, s: None, lambda info: None, collect_s=0.75)
        try:
            iface._raw_part_arrivals.pop(PEER, None)
            payload = self._payload_for(2)

            async def run():
                t0 = time.monotonic()
                fut = asyncio.ensure_future(iface._send_direct_raw_fragmented(
                    TARGET, PEER, payload, 901, priority=iface.PRIORITY_NORMAL, hop_count=1))
                while not sent:
                    await asyncio.sleep(0.005)
                first = sent[0]["t"] - t0
                self._report_v4([(901, 2, True, {0, 1})])
                await fut
                return first

            first_send_after = self.node.run_on_loop(run(), timeout=20.0)
        finally:
            restore()
        self.assertLess(first_send_after, 0.05, f"a lone part waited {first_send_after:.3f}s before its first fragment")
        collects = sink.records("raw_window_collect")
        self.assertEqual(len(collects), 1)
        self.assertEqual(collects[0]["parts"], 1)
        self.assertLess(collects[0]["collect_s"], 0.05)

    def test_four_parts_arriving_together_still_form_one_window(self):
        iface = self.iface
        sent, sink, restore = self._install_window(lambda h, f, s: None, lambda info: None, collect_s=0.75)
        try:
            iface._raw_part_arrivals.pop(PEER, None)
            pkts = {911: 2, 912: 2, 913: 2, 914: 2}
            payloads = {p: self._payload_for(n) for p, n in pkts.items()}
            total = sum(pkts.values())

            async def report_when_burst_done():
                while len(sent) < total:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.05)
                self._report_v4([(p, n, True, set(range(n))) for p, n in pkts.items()])

            results = self._run_parts(payloads, report_when_burst_done)
        finally:
            restore()
        self.assertEqual(results, {p: True for p in pkts})
        windows = sink.records("raw_window")
        self.assertEqual(len(windows), 1, f"one window for parts arriving together: {windows}")
        self.assertEqual(sorted(windows[0]["parts"]), sorted(pkts))
        collects = sink.records("raw_window_collect")
        self.assertEqual(len(collects), 1)
        self.assertEqual(collects[0]["parts"], 4)
        self.assertLess(collects[0]["collect_s"], 0.3, "the collect ended well inside the 0.75 s maximum")


if __name__ == "__main__":
    unittest.main()
