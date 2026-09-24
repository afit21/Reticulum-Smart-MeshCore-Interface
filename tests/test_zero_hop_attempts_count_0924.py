"""Alpha 0.1.9, second pass, item 1: a zero-hop attempt counts.

Defect A (session 2 of 2026-09-23 evening, the first-pass build 65c26f1).
The zero-hop path's hex is "", and `_note_path_attempt_result` returned on
`if not path_hex`, so every zero-hop attempt was dropped as "no path". The
first pass had also stopped `_note_path_result` from incrementing the miss
count (to avoid billing a send twice), so on zero hop the count moved
nowhere: the path never reached `path_switch_after_misses`, the exhausted
branch never ran, and discovery was never asked for. After the laptop left
home it missed 144 consecutive zero-hop attempts, 22:18:24 to 22:40:22,
with one `path_selected` record in the whole capture (the 22:09 "discovered"
at start-up), and nothing at one to three hops got through meanwhile.

The replay fixture `tests/fixtures/field_0923_laptop_zero_hop_miss_run.json`
is every attempt of that run, at its real offsets.
"""
import json
import os
import unittest

from tests._support import REPO_ROOT
from tests.test_path_selection_0922 import _Scaffold, PEER

FIXTURE = os.path.join(REPO_ROOT, "tests", "fixtures", "field_0923_laptop_zero_hop_miss_run.json")


def _load():
    with open(FIXTURE) as f:
        return json.load(f)


class ZeroHopAttemptsCount(_Scaffold):
    def _board(self):
        return self.iface._path_boards[PEER]

    def _select(self):
        return self.node.run_on_loop(self.iface._select_path(PEER), timeout=5.0)

    def _arm_zero_hop_after_a_good_evening(self, sends=20):
        """The zero-hop path as the laptop held it at 22:18: discovered at
        start-up and delivering every message at home (about 130 in six
        minutes), so it carries a healthy measured record -- the case the
        fourth cut's patience applies to."""
        iface = self.iface
        iface._add_path_candidate(PEER, "", 0, 1, "flood")
        self.assertIsNotNone(self._select())
        self.assertEqual(iface._resolved_paths[PEER].out_path_hex, "")
        for _ in range(sends):
            self.on_loop(lambda: iface._note_path_attempt_result(PEER, True, True, "firmware", ack_latency_s=1.3))
            self.on_loop(iface.record_direct_send_result, PEER, True, True)
        return self._board().candidates[""]

    def _replay(self, cand, attempts, stop_when=None):
        """Feed the field attempts in order; a send result follows every
        missed ordinary send (its last attempt), as production records it."""
        iface = self.iface
        marks = {}
        for n, a in enumerate(attempts, start=1):
            self.on_loop(lambda a=a: iface._note_path_attempt_result(
                PEER, a["ok"], True, a["ack_timeout_source"], ack_latency_s=None))
            if a["kind"] is None and a["attempt"] >= iface.direct_send_attempts - 1:
                self.on_loop(iface.record_direct_send_result, PEER, False, True)
            for label, threshold in (("switch", iface.path_switch_after_misses),
                                     ("exhaust", self.module.PATH_EXHAUST_MISSES)):
                if label not in marks and cand.consecutive_misses >= threshold:
                    marks[label] = (n, a["offset_s"])
            if stop_when is not None and stop_when(n):
                break
        return marks

    def test_the_fixture_is_the_field_run(self):
        fx = _load()
        att = fx["attempts"]
        self.assertEqual(len(att), 144)
        self.assertTrue(all(a["hop_count"] == 0 and not a["ok"] and a["ack_timeout_source"] == "firmware" for a in att))
        self.assertAlmostEqual(att[-1]["offset_s"], 1317.1, delta=1.0, msg="22 minutes on a dead path")

    def test_a_zero_hop_miss_counts(self):
        cand = self._arm_zero_hop_after_a_good_evening(sends=0)
        self.on_loop(lambda: self.iface._note_path_attempt_result(PEER, False, True, "firmware"))
        self.assertEqual(cand.consecutive_misses, 1, "\"\" is the zero-hop path, not the absence of one")
        self.on_loop(lambda: self.iface._note_path_attempt_result(PEER, True, True, "firmware", ack_latency_s=1.1))
        self.assertEqual(cand.consecutive_misses, 0)

    def test_the_session_2_run_reaches_both_thresholds_and_discovery(self):
        cand = self._arm_zero_hop_after_a_good_evening()
        sink, restore = self._capture()
        try:
            att = _load()["attempts"]
            switch = self.iface.path_switch_after_misses
            marks = self._replay(cand, att[:switch])
            self.assertEqual(marks["switch"][0], 4, "path_switch_after_misses at the fourth attempt")
            # The fourth cut's patience: the path's record from home is
            # healthy, so the fourth miss alone does not exhaust it...
            self.assertIsNotNone(self._select(), "healthy record: kept until PATH_EXHAUST_MISSES")
            more = self._replay(cand, att[switch:self.module.PATH_EXHAUST_MISSES])
            self.assertEqual(switch + more["exhaust"][0], 8, "PATH_EXHAUST_MISSES at the eighth")
            # ...but the eighth does, 69 s into the run (22:19:33), against
            # never in the field.
            self.assertLess(more["exhaust"][1], 75.0)
            self.assertIsNone(self._select(), "exhausted: the caller runs discovery")
            self.assertNotIn(PEER, self.iface._resolved_paths,
                             "no resolved path: _send_direct_packet runs discovery next")
            reasons = [r["reason"] for r in sink if r.get("event") == "path_selected"]
            self.assertIn("exhausted", reasons)
        finally:
            restore()

    def test_the_whole_run_is_counted(self):
        cand = self._arm_zero_hop_after_a_good_evening()
        self._replay(cand, _load()["attempts"])
        self.assertEqual(cand.consecutive_misses, 144)

    def test_no_resolved_path_is_still_a_no_op(self):
        # The only case that returns: nothing resolved to the peer at all.
        iface = self.iface
        iface._add_path_candidate(PEER, "", 0, 1, "flood")
        iface._resolved_paths.pop(PEER, None)
        self.on_loop(lambda: iface._note_path_attempt_result(PEER, False, True, "firmware"))
        self.assertEqual(self._board().candidates[""].consecutive_misses, 0)


if __name__ == "__main__":
    unittest.main()
