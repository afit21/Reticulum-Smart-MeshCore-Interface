"""Alpha 0.1.9, second pass, item 5: `field_ab_compare.py` prints the path
scoreboard's cost -- decisions per node, the longest run of consecutive
counted misses on one path per hop count and what ended it, and the
decisions that chose a path and then got zero successes.

Evidence (2026-09-23 evening). Session 2's laptop missed 144 attempts in a
row on the zero-hop path after it drove off at 22:20 with no path decision
at all (the zero-hop hex "" was dropped as "no path" by the per-attempt
counter; item 1). Session 1's desktop made 23 decisions on its drive, 22 of
them choosing a path, and 16 of those never delivered: 84 attempts and
626 s of ACK waits, three on a zero-hop path measured dead (item 2). Both
were read with a throwaway script; these rows make them visible in the next
session without one.
"""
import json
import os
import tempfile
import unittest

from tests._support import REPO_ROOT  # noqa: F401  (puts testscripts on sys.path)

import field_ab_compare as fac

PEER = "7bd024b5d082"


def _write(path, records):
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _att(ts, ok, hop=0, source="firmware", latency=1.2, timeout=5.0, peer=PEER):
    return {"ts": ts, "event": "direct_attempt_result", "peer_prefix": peer, "ok": ok, "hop_count": hop,
            "ack_timeout_source": source, "ack_latency_s": latency if ok else None,
            "ack_timeout_s": None if ok else timeout}


def _sel(ts, reason, path_hex, peer=PEER):
    return {"ts": ts, "event": "path_selected", "peer_prefix": peer, "reason": reason, "path_hex": path_hex}


class PathDecisionRows(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="smci-abcmp-paths-")

    def _set(self, files):
        for stem, records in files.items():
            _write(os.path.join(self.tmpdir, stem), records)
        return fac.analyse_set(fac.collect([self.tmpdir]))["path_decisions"]

    def test_a_zero_hop_run_no_decision_ends_is_counted_to_the_capture_end(self):
        recs = [_sel(100.0, "discovered", ""), _att(101.0, True)]
        recs += [_att(200.0 + 6 * i, False) for i in range(40)]
        out = self._set({"a_capture_X_20260923T220958.jsonl": recs})["a"]
        run = out["longest_miss_run_by_hop"][0]
        self.assertEqual(run["misses"], 40)
        self.assertEqual(run["ended_by"], "capture end")
        self.assertEqual(out["decisions"], 1)

    def test_only_counted_misses_extend_a_run_and_only_an_acked_success_ends_it(self):
        recs = [_att(1.0, False), _att(2.0, False, source="report_window"), _att(3.0, False, source="expired"),
                _att(4.0, True, source="noack", latency=None), _att(5.0, False, source="hop1_abort"),
                _att(6.0, True), _att(7.0, False)]
        out = self._set({"afipc_capture_X_20260923T174144.jsonl": recs})["afipc"]
        # firmware, hop1_abort: two; the no-ACK "success" does not end it.
        self.assertEqual(out["longest_miss_run_by_hop"][0], {"misses": 2, "ended_by": "success", "started_ts": 1.0})

    def test_a_decision_or_a_hop_change_closes_the_run(self):
        recs = [_att(1.0, False, hop=2), _att(2.0, False, hop=2), _att(3.0, False, hop=2),
                _sel(4.0, "trial", "19"), _att(5.0, False, hop=1), _att(6.0, False, hop=4)]
        out = self._set({"a_capture_X_20260923T211552.jsonl": recs})["a"]
        self.assertEqual(out["longest_miss_run_by_hop"][2]["misses"], 3)
        self.assertEqual(out["longest_miss_run_by_hop"][2]["ended_by"], "decision")
        self.assertEqual(out["longest_miss_run_by_hop"][1]["ended_by"], "hop change")

    def test_decisions_with_zero_successes(self):
        recs = [_sel(10.0, "trial", "0219"), _att(11.0, False, hop=2, timeout=9.0), _att(21.0, False, hop=2, timeout=9.0),
                _sel(30.0, "trial", ""), _att(31.0, False, timeout=5.0),
                _sel(40.0, "exhausted", None),                      # chooses nothing: not a segment
                _sel(41.0, "discovered", "7619"), _att(42.0, False, hop=2), _att(50.0, True, hop=2),
                _sel(60.0, "trial", "be0219")]                      # no attempts before the capture ends
        out = self._set({"a_capture_X_20260923T211552.jsonl": recs})["a"]
        self.assertEqual(out["decisions"], 5)
        self.assertEqual(out["decisions_choosing_a_path"], 4)
        self.assertEqual(out["zero_success_decisions"], {"count": 2, "attempts": 3, "timeout_s": 23.0})

    def test_runs_are_per_capture_file(self):
        # A restart is a new interface start: a run does not continue across it.
        self._set({"a_capture_X_20260923T220958.jsonl": [_att(1.0, False), _att(2.0, False)]})
        out = self._set({"a_capture_X_20260923T224032.jsonl": [_att(3.0, False)]})["a"]
        self.assertEqual(out["longest_miss_run_by_hop"][0]["misses"], 2)

    def test_the_rows_print(self):
        import contextlib
        import io
        recs = [_sel(1.0, "trial", ""), _att(2.0, False)]
        _write(os.path.join(self.tmpdir, "a_capture_X_20260923T220958.jsonl"), recs)
        sets = {"x": fac.analyse_set(fac.collect([self.tmpdir]))}
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            fac.print_comparison(sets, 20)
        text = buf.getvalue()
        self.assertIn("path decisions", text)
        self.assertIn("a: h0 longest miss run (ended by)", text)
        self.assertIn("1 (1)", text)


DRIVE = os.path.join(REPO_ROOT, "fieldtests", "raw", "Alpha0.1.9-drive")
SESSION2 = os.path.join(REPO_ROOT, "fieldtests", "raw", "Alpha0.1.9-home")


@unittest.skipUnless(os.path.isdir(DRIVE), "session 1 captures not recovered locally")
class TheFieldSessionsReadAsDiagnosed(unittest.TestCase):
    def test_session_1_desktop(self):
        out = fac.analyse_set(fac.collect([DRIVE]))["path_decisions"]
        self.assertEqual(out["afipc"]["decisions"], 23)
        self.assertEqual(out["a"]["decisions"], 22)
        z = out["afipc"]["zero_success_decisions"]
        self.assertEqual((z["count"], z["attempts"]), (16, 84))
        self.assertAlmostEqual(z["timeout_s"], 626, delta=1)

    @unittest.skipUnless(os.path.isfile(os.path.join(SESSION2, "a_capture_Smart_MeshCore_Interface_20260923T220958.jsonl")),
                         "session 2 capture not present")
    def test_session_2_laptop_zero_hop_run(self):
        out = fac.analyse_set(fac.collect([os.path.join(SESSION2, "a_capture_Smart_MeshCore_Interface_20260923T220958.jsonl")]))
        run = out["path_decisions"]["a"]["longest_miss_run_by_hop"][0]
        self.assertEqual(run["misses"], 144)
        self.assertEqual(run["ended_by"], "capture end")
        self.assertEqual(out["path_decisions"]["a"]["decisions"], 1)


if __name__ == "__main__":
    unittest.main()
