"""
A raw window yields to a pending completion REPORT between its parts
(alpha 0.1.5, item 6, 2026-09-21).

Under both-ways load a node's own REPORT for the far sender's window queued
behind its own outgoing window for the whole burst (12-15 s observed in the
phase-4 zero-hop slow scenario, bounded at 20 s), while the far sender's
report wait expired and it fell back to a QUERY. Link handshakes already
pre-empt idle holds of the radio lock (phase 1, 2026-09-20); reports now
have a class of their own that the window yields to between two PARTS --
never inside a part's burst, never at the other idle points a handshake
takes -- resuming behind the report's ANSWER tier and ahead of every
ordinary waiter.

Pinned:
  * the lock: `acquire(report=True)` is visible as `report_requested()`; a
    holder's `yield_to_preempt(REPORT_YIELDED_PRIORITY)` hands the lock to
    the report first and resumes before a NORMAL waiter that queued
    meanwhile; a handshake queued at the same time still goes first;
  * `_send_direct_noack_frame(kind="completion_report")` acquires as the
    report class, a QUERY ANSWER does not;
  * a report queued mid-window goes out before the next part starts (all of
    the current part's fragments first), counted as `report_yields` on the
    `raw_fragment_sent` records that follow.
"""
import asyncio
import time
import unittest

from tests._support import SingleNodeCase, load_interface_module
from tests.test_completion_report_one_hop_0920 import PEER, TARGET
from tests.test_reconcile_m2_window_0920 import _WindowSend


class LockReportClass(unittest.TestCase):
    def test_report_waiter_is_served_first_then_the_yielder_resumes_ahead_of_normal(self):
        module = load_interface_module()
        Lock = module._PriorityAsyncLock
        order = []

        async def scenario():
            lock = Lock()
            await lock.acquire(2)                      # the window holds it at NORMAL
            self.assertFalse(lock.report_requested())

            async def report():
                await lock.acquire(1, report=True)
                order.append("report")
                await asyncio.sleep(0.01)
                lock.release()

            async def normal():
                await lock.acquire(2)
                order.append("normal")
                lock.release()

            t_report = asyncio.ensure_future(report())
            await asyncio.sleep(0.01)
            self.assertTrue(lock.report_requested())
            t_normal = asyncio.ensure_future(normal())
            await asyncio.sleep(0.01)
            await lock.yield_to_preempt(lock.REPORT_YIELDED_PRIORITY)
            order.append("window resumes")
            self.assertFalse(lock.report_requested())
            lock.release()
            await asyncio.gather(t_report, t_normal)

        asyncio.run(scenario())
        self.assertEqual(order, ["report", "window resumes", "normal"])

    def test_a_handshake_still_goes_before_the_report(self):
        module = load_interface_module()
        Lock = module._PriorityAsyncLock
        order = []

        async def scenario():
            lock = Lock()
            await lock.acquire(2)

            async def waiter(name, priority, **kw):
                await lock.acquire(priority, **kw)
                order.append(name)
                lock.release()

            tasks = [asyncio.ensure_future(waiter("report", 1, report=True)),
                     asyncio.ensure_future(waiter("handshake", 0, preempt=True))]
            await asyncio.sleep(0.01)
            await lock.yield_to_preempt(lock.REPORT_YIELDED_PRIORITY)
            order.append("window resumes")
            lock.release()
            await asyncio.gather(*tasks)

        asyncio.run(scenario())
        self.assertEqual(order, ["handshake", "report", "window resumes"])
        self.assertEqual(module._PriorityAsyncLock.REPORT_YIELDED_PRIORITY, 1.5)


class NoAckFrameUsesTheReportClass(SingleNodeCase):
    def _run_noack(self, kind):
        iface = self.iface
        real = iface._direct_exchange_lock_impl
        calls = []

        class Recorder:
            def __call__(self, priority, preempt=False, report=False):
                calls.append({"priority": priority, "preempt": preempt, "report": report})
                return real(priority, preempt, report)

            def __getattr__(self, name):
                return getattr(real, name)

        async def fake_run_command(coro, *a, **k):
            coro.close()
            return None

        saved_run = iface._run_command
        iface._direct_exchange_lock_impl = Recorder()
        iface._run_command = fake_run_command
        try:
            self.node.run_on_loop(iface._send_direct_noack_frame(
                TARGET, "Q" + "x" * 10, 0, PEER, 0, kind, priority=iface.PRIORITY_ANSWER), timeout=10.0)
        finally:
            iface._direct_exchange_lock_impl = real
            iface._run_command = saved_run
        return calls

    def test_completion_report_acquires_as_report(self):
        calls = self._run_noack("completion_report")
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0]["report"])
        self.assertFalse(calls[0]["preempt"])

    def test_completion_answer_does_not(self):
        calls = self._run_noack("completion_answer")
        self.assertEqual(len(calls), 1)
        self.assertFalse(calls[0]["report"])


class WindowYieldsBetweenParts(_WindowSend):
    def _run(self, queue_report_at):
        """Three 2-fragment parts; a report waiter queues for the lock when
        the fragment `queue_report_at` (pkt_id, frag_idx) is sent. Returns
        (sent records, number of fragments sent when the report got the
        lock)."""
        iface = self.iface
        lock = iface._direct_exchange_lock
        got_at = {"n": None}

        async def report_waiter(sent):
            await lock.acquire(iface.PRIORITY_ANSWER, report=True)
            got_at["n"] = len(sent)
            await asyncio.sleep(0.02)
            lock.release()

        sent_ref = {}

        def on_fragment(header, flagged, size):
            if (header.pkt_id, header.frag_idx) == queue_report_at and header.attempt == 0:
                asyncio.ensure_future(report_waiter(sent_ref["sent"]))

        sent, sink, restore = self._install_window(on_fragment, lambda info: None)
        sent_ref["sent"] = sent
        try:
            pkts = {921: 2, 922: 2, 923: 2}
            payloads = {p: self._payload_for(n) for p, n in pkts.items()}

            async def report_when_burst_done():
                while len(sent) < 6:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.05)
                self._report_v4([(p, n, True, set(range(n))) for p, n in pkts.items()])

            results = self._run_parts(payloads, report_when_burst_done)
            # the fake send records pkt_id via the decoded header
            for rec in sent:
                rec.setdefault("pkt_id", None)
        finally:
            restore()
        self.assertEqual(results, {p: True for p in pkts})
        return sent, sink, got_at["n"]

    def test_report_queued_during_part_one_goes_out_before_part_two(self):
        sent, sink, got_at = self._run((921, 0))
        self.assertEqual(len(sent), 6)
        self.assertEqual(got_at, 2, "the report got the radio after part 1's two fragments, before part 2")
        yields = [r["report_yields"] for r in sink.records("raw_fragment_sent")]
        self.assertEqual(yields, [0, 0, 1, 1, 1, 1])
        self.assertEqual([r["handshake_yields"] for r in sink.records("raw_fragment_sent")], [0] * 6)

    def test_report_queued_inside_part_two_waits_for_that_part_to_finish(self):
        sent, sink, got_at = self._run((922, 0))
        self.assertEqual(got_at, 4, "never inside a part's burst: after part 2's second fragment")
        yields = [r["report_yields"] for r in sink.records("raw_fragment_sent")]
        self.assertEqual(yields, [0, 0, 0, 0, 1, 1])


if __name__ == "__main__":
    unittest.main()
