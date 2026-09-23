"""Alpha 0.1.9, item 5: `field_ab_compare.py`'s part timing must not pair
across an interface restart, and the proof rows must separate the two
populations that differ by a factor of two.

Evidence (2026-09-23). The laptop's interface restarted mid-session, so
`fieldtests/raw/Alpha0.1.8/` holds three capture files for one node. pkt_id
is per-process and restarts from 0, and the part-time table keyed on
`(node, pkt_id)` alone, so a `raw_fragment_sent` of the 09:30 process paired
with a `completion_check_result` of the 11:28 one and the hop-3 row read a
median of 7434 s -- two hours, reported as a part time. The report summariser
was made restart-safe in alpha 0.1.7; the part-time table was not. One
capture file is one interface start, which is the key used now.

The proof rows are the second half. At the 2026-09-23 two-hop stop a proof
answering a raw multi-fragment window turned round in 10.0 s median while one
answering a bare single-fragment packet took 3.8 s, and their first attempts
succeeded at very different rates -- so the combined median that alpha 0.1.8
printed hides exactly what item 2 sets out to change.
"""
import json
import os
import tempfile
import unittest

from tests._support import REPO_ROOT  # noqa: F401  (puts testscripts on sys.path)

import field_ab_compare as fac


def _write(path, records):
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


class PartTimingIsRestartSafe(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="smci-abcmp-")

    def _capture(self, stem, records):
        path = os.path.join(self.tmpdir, f"a_capture_Smart_MeshCore_Interface_{stem}.jsonl")
        _write(path, records)
        return path

    def test_a_pkt_id_reused_after_a_restart_does_not_pair(self):
        # Process 1: pkt_id 0 burst at t=1000, completing at t=1020 (20 s).
        # Process 2, two hours later: the SAME pkt_id 0, completing 15 s in.
        # Keyed on (node, pkt_id) alone, process 2's completion pairs with
        # process 1's first fragment and reads 7220 s.
        self._capture("20260923T093038", [
            {"ts": 1000.0, "event": "raw_fragment_sent", "pkt_id": 0, "hop_count": 3, "ok": True, "round": 0, "frag_idx": 0},
            {"ts": 1020.0, "event": "completion_check_result", "pkt_id": 0, "complete": True},
        ])
        self._capture("20260923T112824", [
            {"ts": 8200.0, "event": "raw_fragment_sent", "pkt_id": 0, "hop_count": 3, "ok": True, "round": 0, "frag_idx": 0},
            {"ts": 8215.0, "event": "completion_check_result", "pkt_id": 0, "complete": True},
        ])
        recs = fac.collect([self.tmpdir])
        out = fac.analyse_set(recs)
        hop3 = out["part_time"][3]
        self.assertEqual(hop3["n"], 2, "one part per process, not one merged pair")
        self.assertEqual(hop3["max"], 20.0,
                         "the longest part is 20 s, not the 7220 s gap between the two processes")
        self.assertEqual(out["parts_started"], 2)
        self.assertEqual(out["parts_completed"], 2)

    def test_two_processes_of_one_node_are_still_one_node(self):
        # The restart must not split the node: both files are node `a`.
        self._capture("20260923T093038", [{"ts": 1000.0, "event": "raw_fragment_sent",
                                           "pkt_id": 0, "hop_count": 2, "ok": True, "round": 0, "frag_idx": 0}])
        self._capture("20260923T112824", [{"ts": 8200.0, "event": "raw_fragment_sent",
                                           "pkt_id": 0, "hop_count": 2, "ok": True, "round": 0, "frag_idx": 0}])
        out = fac.analyse_set(fac.collect([self.tmpdir]))
        self.assertEqual(out["nodes"], ["a"])


class ProofRowsSeparateThePopulations(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="smci-abcmp-")

    def _session(self, records):
        path = os.path.join(self.tmpdir, "a_capture_Smart_MeshCore_Interface_20260923T112824.jsonl")
        _write(path, records)
        return fac.analyse_set(fac.collect([self.tmpdir]))

    @staticmethod
    def _proved(t0, dest, transport, *, first_ok, turnaround, skipped_key=None):
        """One packet in, its proof out, one attempt, one send result."""
        recs = [
            {"ts": t0, "direction": "in", "packet_type_name": "DATA", "destination_type_name": "SINGLE",
             "context_name": "NONE", "destination_hash": "dead" + dest, "transport": transport,
             "hop_count": 2, "size_bytes": 200},
            {"ts": t0 + 0.2, "direction": "out", "packet_type_name": "PROOF", "context_name": "NONE",
             "destination_hash": dest, "target_peer": "7bd024b5d082", "size_bytes": 83},
            {"ts": t0 + turnaround - 0.1, "event": "direct_attempt_result", "peer_prefix": "7bd024b5d082",
             "attempt": 0, "ok": first_ok, "pkt_id": None, "hop_count": 2},
            {"ts": t0 + turnaround, "event": "direct_send_result", "peer_prefix": "7bd024b5d082",
             "destination_hash": dest, "ok": True, "method": "z85_bare", "size_bytes": 83},
        ]
        if skipped_key is not None:
            recs.insert(1, {"ts": t0 + 0.1, "event": "completion_report_skipped", "peer_prefix": "7bd024b5d082",
                            "pkt_id": 1, "report_skipped_for_proof": True, "proof_key": skipped_key,
                            "hop_count": 2})
        return recs

    def test_raw_window_and_bare_packet_proofs_are_reported_apart(self):
        out = self._session(
            self._proved(1000.0, "aa01", "direct_raw_multifragment", first_ok=False, turnaround=15.0)
            + self._proved(1100.0, "aa02", "direct_bare", first_ok=True, turnaround=4.0))
        kinds = out["proof_turnaround_by_kind"]
        self.assertEqual(kinds["raw window"][2]["med"], 15.0)
        self.assertEqual(kinds["bare packet"][2]["med"], 4.0)
        # The combined row averages the two and would hide either moving.
        self.assertEqual(out["proof_turnaround"][2]["n"], 2)
        first = out["proof_first_attempt"]
        self.assertEqual((first["raw window"][2]["ok"], first["raw window"][2]["n"]), (0, 1))
        self.assertEqual((first["bare packet"][2]["ok"], first["bare packet"][2]["n"]), (1, 1))
        self.assertEqual((first["all"][2]["ok"], first["all"][2]["n"]), (1, 2))

    def test_a_proof_that_replaced_a_report_is_its_own_population(self):
        # Matched on proof_key, which is the proof's destination hash --
        # this is the set alpha 0.1.9's item 2 holds for the burst tail.
        out = self._session(
            self._proved(1000.0, "aa01", "direct_raw_multifragment", first_ok=False,
                         turnaround=15.0, skipped_key="aa01")
            + self._proved(1100.0, "aa02", "direct_raw_multifragment", first_ok=True, turnaround=5.0))
        kinds = out["proof_turnaround_by_kind"]
        self.assertEqual(kinds["raw win, report skipped"][2]["n"], 1,
                         "only the window whose report the proof replaced")
        self.assertEqual(kinds["raw window"][2]["n"], 2, "both are still raw windows")
        first = out["proof_first_attempt"]["raw win, report skipped"][2]
        self.assertEqual((first["ok"], first["n"]), (0, 1))


class MeshbenchReportAgreesWithTheFieldComparison(unittest.TestCase):
    """The same proof reading is printed by two scripts -- the field
    comparison and the MeshBench run analysis -- so a bench run and a field
    session can be read against each other. They must not drift apart.
    """

    def _records(self):
        return (ProofRowsSeparateThePopulations._proved(
                    1000.0, "aa01", "direct_raw_multifragment", first_ok=False,
                    turnaround=15.0, skipped_key="aa01")
                + ProofRowsSeparateThePopulations._proved(
                    1100.0, "aa02", "direct_bare", first_ok=True, turnaround=4.0))

    def test_the_two_scripts_report_the_same_proof_numbers(self):
        import meshbench_report as mbr

        recs = self._records()
        pk_in = [r for r in recs if r.get("direction") == "in" and "event" not in r]
        pk_out = [r for r in recs if r.get("direction") == "out" and "event" not in r]
        att = [r for r in recs if r.get("event") == "direct_attempt_result"]
        got = mbr.proof_attempts(recs, pk_in, pk_out, att)

        self.assertEqual(got["turnaround_s"]["raw window"]["2"]["med"], 15.0)
        self.assertEqual(got["turnaround_s"]["bare packet"]["2"]["med"], 4.0)
        self.assertEqual(got["turnaround_s"]["raw win, report skipped"]["2"]["n"], 1)
        self.assertEqual(got["first_attempt"]["raw window"]["2"], {"ok": 0, "n": 1})
        self.assertEqual(got["first_attempt"]["bare packet"]["2"], {"ok": 1, "n": 1})
        self.assertEqual(got["first_attempt"]["all"]["2"], {"ok": 1, "n": 2})

        # And the field comparison, over the same records, agrees.
        import tempfile as _tf
        d = _tf.mkdtemp(prefix="smci-abcmp-")
        _write(os.path.join(d, "a_capture_Smart_MeshCore_Interface_20260923T112824.jsonl"), recs)
        out = fac.analyse_set(fac.collect([d]))
        for kind in ("raw window", "bare packet", "raw win, report skipped"):
            self.assertEqual(out["proof_turnaround_by_kind"][kind][2]["med"],
                             got["turnaround_s"][kind]["2"]["med"], kind)
            self.assertEqual(out["proof_first_attempt"][kind][2],
                             got["first_attempt"][kind]["2"], kind)

    def test_the_tail_hold_is_reported_when_the_attempt_carries_it(self):
        import meshbench_report as mbr

        recs = self._records()
        for r in recs:
            if r.get("event") == "direct_attempt_result" and r["ts"] < 1100.0:
                r["proof_tail_hold_s"] = 5.0
        got = mbr.proof_attempts(
            recs,
            [r for r in recs if r.get("direction") == "in" and "event" not in r],
            [r for r in recs if r.get("direction") == "out" and "event" not in r],
            [r for r in recs if r.get("event") == "direct_attempt_result"])
        self.assertEqual(got["tail_hold_s"]["n"], 1)
        self.assertEqual(got["tail_hold_s"]["med"], 5.0)


if __name__ == "__main__":
    unittest.main()
