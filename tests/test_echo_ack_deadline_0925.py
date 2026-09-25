"""Pass 1 item 1 (2026-09-25): once the first repeater's echo is heard, the
ACK wait ends at echo + a hop-scaled allowance.

The 2026-09-24 morning session spent about as long waiting out ACKs that
never came (1,101 s) as receiving the ones that did (1,106 s). At two hops a
miss waited a median of 10.1 s -- the hop cap of 5 + 3 s per hop, or the
firmware's own suggestion under it; the measured estimate (2 x (srtt +
4 rttvar)) almost never came in under that cap at two hops, and a replay of
the field ACKs through it at lower multipliers saved little. What the field
does show is that once this radio hears the first repeater forward its frame
(the echo, median about 2 s after the send), the ACK follows within a tight
window: across every capture to 2026-09-24, ACK time after the echo was at
most 4.21 s at one hop (1,162 ACKs), 5.94 s at two (1,044) and 7.54 s at
three (131) -- `tests/fixtures/field_ack_after_echo_0925.json` keeps the ten
largest per hop count. The wait now ends at echo + 2.0 + 2.5 s per hop (4.5 /
7 / 9.5 s after it), never later than the cap. A miss under it is path
evidence, captured as `ack_timeout_source="echo_deadline"`.

The wait is one continuous ACK subscription whose end moves when the echo
arrives, so an ACK cannot fall between two waits.
"""
import asyncio
import json
import os
import time
import unittest

from tests._support import REPO_ROOT, SingleNodeCase, load_interface_module

FIXTURE = os.path.join(REPO_ROOT, "tests", "fixtures", "field_ack_after_echo_0925.json")


class AfterEchoAllowance(unittest.TestCase):
    def setUp(self):
        module = load_interface_module()
        self.bare = module.SmartMeshCoreInterface.__new__(module.SmartMeshCoreInterface)
        self.bare._configure_peer_discovery({})
        self.PATH_ATTEMPT_MISS_SOURCES = module.PATH_ATTEMPT_MISS_SOURCES

    def test_allowance_per_hop(self):
        f = self.bare._ack_after_echo_s
        self.assertIsNone(f(0), "zero hop: no repeater, no echo")
        self.assertIsNone(f(None))
        self.assertEqual((f(1), f(2), f(3)), (4.5, 7.0, 9.5))

    def test_disabled(self):
        self.bare.direct_ack_echo_deadline_enabled = False
        self.assertIsNone(self.bare._ack_after_echo_s(2))

    def test_never_cuts_an_ack_in_the_field_history(self):
        with open(FIXTURE) as fh:
            fx = json.load(fh)
        for hop, row in fx["by_hop"].items():
            allowance = self.bare._ack_after_echo_s(int(hop))
            for echo_s, ack_s, _session in row["largest_after_echo"]:
                self.assertLess(ack_s - echo_s, allowance,
                                f"hop {hop}: an ACK {ack_s - echo_s:.2f} s after the echo would be cut")
        self.assertGreater(sum(r["n"] for r in fx["by_hop"].values()), 2000)

    def test_a_miss_under_it_is_path_evidence(self):
        self.assertIn("echo_deadline", self.PATH_ATTEMPT_MISS_SOURCES)


class EchoMovesTheDeadline(SingleNodeCase):
    FILTERS = {"code": "0badc0de"}

    def _window(self):
        return {"echo_seen_s": None, "echo_event": asyncio.Event()}

    def _run(self, echo_at_s, ack_at_s, timeout_s=3.0, after_echo_s=0.3):
        iface = self.iface
        module = self.module

        async def scenario():
            window = self._window()
            start = time.monotonic()
            loop = asyncio.get_running_loop()

            def echo():
                window["echo_seen_s"] = round(time.monotonic() - start, 3)
                window["echo_event"].set()

            def ack():
                loop.create_task(iface._mc.dispatcher.dispatch(
                    module_event(self.FILTERS["code"])))

            if echo_at_s is not None:
                loop.call_later(echo_at_s, echo)
            if ack_at_s is not None:
                loop.call_later(ack_at_s, ack)
            result = await iface._wait_for_ack_event_or_echo_deadline(
                self.FILTERS, timeout_s, None, window, start, after_echo_s)
            return result, time.monotonic() - start

        fake = __import__("simmesh.fake_meshcore", fromlist=["SimEvent", "EventType"])

        def module_event(code):
            return fake.SimEvent(fake.EventType.ACK, {"code": code}, {"code": code})

        return self.node.run_on_loop(scenario(), timeout=10)

    def test_echo_then_silence_ends_early(self):
        (ev, answered, cut), took = self._run(echo_at_s=0.1, ack_at_s=None)
        self.assertIsNone(ev)
        self.assertFalse(answered)
        self.assertTrue(cut)
        self.assertLess(took, 1.0, "ended at echo + allowance, not the 3 s ceiling")
        self.assertGreater(took, 0.35)

    def test_ack_inside_the_allowance_is_received(self):
        (ev, _answered, cut), took = self._run(echo_at_s=0.1, ack_at_s=0.3)
        self.assertIsNotNone(ev)
        self.assertFalse(cut)
        self.assertLess(took, 0.6)

    def test_no_echo_waits_the_ceiling(self):
        (ev, _answered, cut), took = self._run(echo_at_s=None, ack_at_s=None, timeout_s=1.0)
        self.assertIsNone(ev)
        self.assertFalse(cut)
        self.assertGreater(took, 0.95)

    def test_echo_after_the_ceiling_would_end_it_changes_nothing(self):
        (ev, _answered, cut), took = self._run(echo_at_s=0.9, ack_at_s=None, timeout_s=1.0, after_echo_s=5.0)
        self.assertIsNone(ev)
        self.assertFalse(cut, "the ceiling ended it; the echo only ever moves the end earlier")
        self.assertLess(took, 1.3)


class AwaitDirectAckReportsTheEchoDeadline(SingleNodeCase):
    def test_miss_after_echo_is_an_echo_deadline_miss(self):
        import types
        iface = self.iface
        saved = (iface.direct_ack_after_echo_base_s, iface.direct_ack_after_echo_per_hop_s)
        iface.direct_ack_after_echo_base_s, iface.direct_ack_after_echo_per_hop_s = 0.2, 0.1

        async def scenario():
            window = iface._open_rx_log_window("ab" * 32)
            start = time.monotonic()
            window["tx_at"] = start

            def echo():
                window["echo_seen_s"] = round(time.monotonic() - start, 3)
                window["echo_event"].set()
            asyncio.get_running_loop().call_later(0.1, echo)
            sent = types.SimpleNamespace(payload={"expected_ack": bytes.fromhex("0badc0de"), "suggested_timeout": 3000})
            try:
                return await iface._await_direct_ack(sent, None, 1, window, start), time.monotonic() - start
            finally:
                iface._close_rx_log_window(window)
        try:
            (ok, full, ack_timeout_s, source, latency, _abort), took = self.node.run_on_loop(scenario(), timeout=15)
        finally:
            iface.direct_ack_after_echo_base_s, iface.direct_ack_after_echo_per_hop_s = saved
        self.assertFalse(ok)
        self.assertTrue(full, "path evidence: waited_full_timeout")
        self.assertEqual(source, "echo_deadline")
        self.assertAlmostEqual(ack_timeout_s, 0.1 + 0.3, delta=0.08)
        self.assertLess(took, 1.0)


if __name__ == "__main__":
    unittest.main()
