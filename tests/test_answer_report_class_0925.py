"""Pass 1 item 3 (2026-09-25): a QUERY's ANSWER takes the radio lock in the
REPORT class.

Alpha 0.1.5's item 6 gave completion REPORTs a lock class that a raw window
this node is sending yields to -- between two of its parts, during its
radio-free report wait, and during a QUERY's quiet hold -- because the far
sender is waiting on the report before it re-sends. A QUERY's ANSWER is the
same thing (the querier is stalled on it, and its answer budget is a few
seconds) but queued as an ordinary waiter, so it waited out the whole
burst: in the 2026-09-24 morning captures ANSWERs waited 13 s on the laptop
and 42 s on the desktop behind that node's own raw bursts. Both carriers --
the no-ACK frame (the default) and the acknowledged one -- now take the
report class.
"""
import asyncio

from tests._support import SingleNodeCase
from tests.test_completion_report_one_hop_0920 import PEER, TARGET


class AckedAnswerTakesTheReportClass(SingleNodeCase):
    def test_acked_answer_passes_report_true(self):
        iface = self.iface
        seen = {}

        async def fake_send(target, frame, attempt, **kwargs):
            seen.update(kwargs)
            return True, False

        saved = (iface._send_direct_frame_and_wait_for_ack, iface._resolve_contact,
                 iface._canonical_peer_prefix, iface.direct_report_noack)
        iface._send_direct_frame_and_wait_for_ack = fake_send
        iface._resolve_contact = lambda token: {"public_key": TARGET, "out_path_len": 0}
        iface._canonical_peer_prefix = lambda token: PEER
        iface.direct_report_noack = False
        try:
            self.node.run_on_loop(iface._send_completion_answer(
                PEER, 5, 3, True, held={0, 1, 2}, version=3, nonce=9), timeout=20.0)
        finally:
            (iface._send_direct_frame_and_wait_for_ack, iface._resolve_contact,
             iface._canonical_peer_prefix, iface.direct_report_noack) = saved
        self.assertEqual(seen.get("kind"), "completion_answer")
        self.assertIs(seen.get("report"), True)


class QueuedAnswerIsVisibleToAHolder(SingleNodeCase):
    def test_a_queued_noack_answer_sets_report_requested(self):
        """What a raw window checks between its parts and during its report
        wait: with the lock held, a queued ANSWER shows as a report waiter."""
        iface = self.iface
        lock = iface._direct_exchange_lock

        async def fake_run_command(coro, *a, **k):
            coro.close()
            return None

        async def scenario():
            await lock.acquire(iface.PRIORITY_NORMAL)
            task = asyncio.ensure_future(iface._send_direct_noack_frame(
                TARGET, "Q" + "x" * 10, 0, PEER, 0, "completion_answer", priority=iface.PRIORITY_ANSWER))
            await asyncio.sleep(0.05)
            requested = lock.report_requested()
            await lock.yield_to_preempt(lock.REPORT_YIELDED_PRIORITY)
            answered_first = task.done()
            lock.release()
            await task
            return requested, answered_first

        saved = iface._run_command
        iface._run_command = fake_run_command
        try:
            requested, answered_first = self.node.run_on_loop(scenario(), timeout=20.0)
        finally:
            iface._run_command = saved
        self.assertTrue(requested)
        self.assertTrue(answered_first, "the yield hands the radio to the ANSWER before the holder resumes")


if __name__ == "__main__":
    import unittest
    unittest.main()
