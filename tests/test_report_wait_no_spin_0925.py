"""2026-09-25 review: the raw window's report wait must not spin once one
of its PROOF events is already set.

`_await_completion_report` waits, radio released, on the report future and
the events this window's PROOFs set (alpha 0.1.8, item 1), and re-checks
`_window_all_proved` after every wakeup. `_wait_future_or_proof` returned
None without awaiting anything whenever ANY of those events was already
set, and the caller then `continue`d straight back into it. With one part
proved and another not (two LXMF messages in one window, part A's PROOF
first -- or a part with no proof key at all, which `_window_all_proved`
never counts as proved), the coroutine looped without ever suspending, so
the interface's whole event loop -- ACKs, inbound frames, the second
PROOF and the report that would have ended the wait -- stood still until
the report deadline (4 s + 2.5 s per hop), and the window then fell back
to a QUERY anyway. The code-review agent reproduced it as a 5.5 s block
with a 50 ms ticker task running 0 times.

The wait now listens only to the proof events that are still unset (a set
one has already been acted on by the caller's re-check), and to the report
future alone when none are left.
"""
import asyncio
import time
import unittest

from tests._support import SingleNodeCase

PEER = "34ab12cd56ef"


class ReportWaitDoesNotSpin(SingleNodeCase):
    WAIT_S = 1.0

    def _run_wait(self, events, proved_check, during=None):
        """Run `_await_completion_report` radio-released on the interface's
        loop next to a 20 ms ticker; `during(fut)` runs as its own task.
        Returns (result, ticks, elapsed_s)."""
        iface = self.iface
        iface._completion_report_wait_s = lambda hops, peer=None: self.WAIT_S
        self.addCleanup(lambda: iface.__dict__.pop("_completion_report_wait_s", None))

        async def run():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            ticks = 0

            async def ticker():
                nonlocal ticks
                while True:
                    await asyncio.sleep(0.02)
                    ticks += 1

            t = loop.create_task(ticker())
            side = loop.create_task(during(fut)) if during is not None else None
            started = time.monotonic()
            try:
                result = await iface._await_completion_report(
                    fut, PEER, 7, 2, 0, stage="raw0", proved_check=proved_check, proved_events=events)
            finally:
                t.cancel()
                if side is not None:
                    side.cancel()
            return result, ticks, time.monotonic() - started

        return self.node.run_on_loop(run(), timeout=self.WAIT_S + 10.0)

    def test_one_proof_set_other_pending_does_not_block_the_loop(self):
        async def make():
            a, b = asyncio.Event(), asyncio.Event()
            a.set()
            return [a, b]
        events = self.node.run_on_loop(make())
        result, ticks, elapsed = self._run_wait(events, lambda: False)
        self.assertIsNone(result, "no report and not all proved: falls back to a QUERY")
        self.assertGreaterEqual(elapsed, self.WAIT_S - 0.05)
        # 1 s at 20 ms is ~50 ticks; the spin gave 0.
        self.assertGreaterEqual(ticks, 20, f"event loop starved during the report wait ({ticks} ticks)")

    def test_every_event_set_but_window_not_proved_still_yields(self):
        # A part with no proof key: _window_all_proved stays False even
        # with every event set.
        async def make():
            a = asyncio.Event()
            a.set()
            return [a]
        events = self.node.run_on_loop(make())
        _result, ticks, _elapsed = self._run_wait(events, lambda: False)
        self.assertGreaterEqual(ticks, 20, f"event loop starved during the report wait ({ticks} ticks)")

    def test_second_proof_still_ends_the_wait_early(self):
        async def make():
            a, b = asyncio.Event(), asyncio.Event()
            a.set()
            return [a, b]
        events = self.node.run_on_loop(make())

        async def prove_b(_fut):
            await asyncio.sleep(0.2)
            events[1].set()

        result, _ticks, elapsed = self._run_wait(events, lambda: events[1].is_set(), during=prove_b)
        self.assertIs(result, self.module._WINDOW_PROVED)
        self.assertLess(elapsed, 0.6, "the pending PROOF must still wake the wait")

    def test_report_still_ends_the_wait_with_a_proof_already_set(self):
        async def make():
            a, b = asyncio.Event(), asyncio.Event()
            a.set()
            return [a, b]
        events = self.node.run_on_loop(make())
        iface = self.iface
        frame = iface._decode_completion_frame(iface._encode_completion_frame(
            iface.COMPLETION_TYPE_ANSWER, 7, 2, complete=True, held={0, 1},
            version=iface.COMPLETION_PROTOCOL_VERSION, nonce=1,
        ))

        async def report(fut):
            await asyncio.sleep(0.2)
            fut.set_result(frame)

        result, _ticks, elapsed = self._run_wait(events, lambda: False, during=report)
        self.assertIs(result, frame)
        self.assertLess(elapsed, 0.6, "the report must still wake the wait")


if __name__ == "__main__":
    unittest.main()
