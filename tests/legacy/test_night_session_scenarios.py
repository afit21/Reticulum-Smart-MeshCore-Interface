"""
Legacy simulated-repeater forms of the 2026-09-19 night regression (archived
2026-09-20 with the rest of the simmesh multi-hop tier; see tests/legacy/README.md).

Moved here from tests/test_raw_fragments.py, where they had been gated behind
SMCI_RUN_UNVERIFIED since they were written on 2026-09-20 and never run to a
pass (their `_bring_up` import had also broken when tests/test_sim_scenarios.py
was archived). They are superseded by:

  * tests/test_raw_fragments.py::ZeroHopBidirectionalPageTransfer -- the
    zero-hop bidirectional page transfer, which the unit suite runs for real;
  * testscripts/meshbench_scenarios.py `page_transfer`, `page_transfer_bidir`,
    `three_hop` -- the one-hop / bidirectional / three-hop forms against real
    MeshCore firmware, which is where those questions are now answered.

Kept runnable so the numbers quoted in the class docstring can be reproduced:

    SMCI_RUN_UNVERIFIED=1 python3 -m unittest tests.legacy.test_night_session_scenarios

Not collected by `unittest discover` (this directory is not a package).
"""
import os
import tempfile
import time
import unittest

from tests._support import SimMesh, _bring_up, build_rns_packet, quiet_rns, slow, wait_until
from tests.test_raw_fragments import RAW_CFG, _page_parts, _page_stats

PAGE_PROFILE = {"loss": 0.06, "airtime_base_ms": 200.0, "airtime_per_byte_ms": 1.0}


def _page_mesh(test, links, repeaters, seed, config=None, profile=PAGE_PROFILE):
    """A calibrated mesh brought up loss-free (an operator pressing advert),
    with the profile's loss applied to the transfer phase only."""
    quiet_rns()
    mesh = SimMesh(links, repeaters=repeaters, seed=seed, capture_dir=tempfile.mkdtemp(prefix="smci-page-cap-"),
                   airtime_base_ms=profile["airtime_base_ms"], airtime_per_byte_ms=profile["airtime_per_byte_ms"])
    test.mesh = mesh
    _bring_up(mesh, ["A", "B"], timeout=90.0, config={**RAW_CFG, **(config or {})})
    a, b = mesh.nodes["A"], mesh.nodes["B"]
    for x, y in ((a, b), (b, a)):
        y.send(build_rns_packet("data", dest_hash=y.dest_hash, payload=b"prime"))
        assert wait_until(lambda: y.dest_hash in x.iface._rns_token_peer, 60.0), "token never learned"
        assert x.iface._peers[y.prefix].raw_fragments is True
    wait_until(lambda: not a.iface._direct_exchange_lock_impl.locked() and not b.iface._direct_exchange_lock_impl.locked(), 30.0)
    mesh.air.loss = profile["loss"]
    return mesh, a, b


@slow
@unittest.skipUnless(os.environ.get("SMCI_RUN_UNVERIFIED"), "written 2026-09-20 but not yet run to a pass -- see the class docstring")
class NightSessionScenarios(unittest.TestCase):
    """Simulated forms of the 2026-09-19 night regression (module docstring,
    "Field regression fixed (2026-09-19 night session)"). Timing here is
    FAST_TIMING over a calibrated air model: these check the interface's
    logic under the field's collision geometry, not real delivery rates.

    STATUS (2026-09-20): written, NOT yet verified to pass -- the session
    that wrote them was asked to wrap up first. Gated behind
    SMCI_RUN_UNVERIFIED=1 so the suite stays green until someone runs them
    (each takes up to 15 minutes). Two things were learned on the way that
    the numbers below already reflect: the air model gained the firmware's
    listen-before-talk (without it the answerer keyed its ANSWER over the
    repeater's relay of its own ACK every time, 5% answer delivery in every
    configuration), and the parts must be Resource class -- plain DATA
    expires at outgoing_max_age (120s) partway through a 12-part transfer,
    which is why every run below shows fewer than 12 delivered at 900s.
    The measurements below were taken with plain DATA parts and should be
    re-run with the "resource" kind now used here.

    BASELINE, unmodified 3b56c11 interface on the LBT air model, seed 11 /
    21 (plain DATA parts, 12 x 483B, loss 0.06, airtime 200ms+1ms/B):
      answered 7/17 (41%) / 12/18 (67%); raw completion 2/3 / 4/6;
      slot_expired drops 6 / 5; delivered 6/12 / 7/12 at 900s.
    All four changes at their defaults, seed 11: answered 17/27 (63%),
    raw completion 7/8, no drops, delivered 8/12 at 900s. With the cap
    re-enabled at 2 (now priority-aware, non-dropping): 16/20 (80%) /
    15/20 (75%), raw 10/10 / 8/8, delivered 10/12 / 8/12 -- the best
    configuration in the sim, which is worth knowing given the field
    session that motivated turning it off.
    """

    def tearDown(self):
        self.mesh.stop()

    def _one_hop_page(self, seed, config=None, n=12):
        _, a, b = _page_mesh(self, ["A-R", "R-B"], ["R"], seed, config=config)
        self.assertEqual(a.resolved_paths[b.prefix].out_path_len, 1)
        parts = _page_parts(b.dest_hash, n)
        started = time.monotonic()
        for p in parts:
            a.send(p)
        done = wait_until(lambda: all(p in b.owner.received for p in parts), 600.0)
        elapsed = time.monotonic() - started
        time.sleep(3.0)
        stats = _page_stats(a)
        stats["elapsed_s"] = round(elapsed, 1)
        stats["delivered"] = sum(1 for p in parts if p in b.owner.received)
        return done, stats

    def test_one_hop_page_transfer_completes_with_answers_and_raw_intact(self):
        """(a) A-R-B, twelve 483-byte parts back to back. Acceptance: every
        part delivered, answers >= 80%, raw completion >= 90%, no drops."""
        totals = {"checks": 0, "answered": 0, "raw_complete": 0, "text_fallbacks": 0, "slot_expired": 0}
        per_seed = []
        for seed in (11, 21):
            done, stats = self._one_hop_page(seed)
            per_seed.append((seed, stats))
            self.assertTrue(done, f"seed {seed}: page transfer incomplete: {stats}")
            for k in totals:
                totals[k] += stats[k]
            self.mesh.stop()
        answer_rate = totals["answered"] / max(1, totals["checks"])
        raw_completion = totals["raw_complete"] / max(1, totals["raw_complete"] + totals["text_fallbacks"])
        self.assertGreaterEqual(answer_rate, 0.80, f"answer delivery {answer_rate:.0%}: {per_seed}")
        self.assertGreaterEqual(raw_completion, 0.90, f"raw completion {raw_completion:.0%}: {per_seed}")
        self.assertEqual(totals["slot_expired"], 0)

    def test_one_hop_page_transfer_records_the_quiet_hold(self):
        """The quiet window is what changed at one hop; the capture must
        show it (quiet_hold_s on the QUERY attempts, None everywhere else)."""
        done, stats = self._one_hop_page(31, n=4)
        self.assertTrue(done, stats)
        attempts = [r for r in self.mesh.nodes["A"].capture_records() if r.get("event") == "direct_attempt_result"]
        query_holds = [r["quiet_hold_s"] for r in attempts if r.get("kind") == "completion_query"]
        other_holds = [r["quiet_hold_s"] for r in attempts if r.get("kind") != "completion_query"]
        self.assertTrue(query_holds, "no completion QUERY attempts captured")
        self.assertTrue(any(h is not None and h > 0 for h in query_holds), query_holds)
        self.assertTrue(all(h is None for h in other_holds))

    def test_bidirectional_answers_do_not_queue_behind_the_lock(self):
        """(b) Both nodes send twelve parts to each other at once. The
        radio-free remainder of the answer wait is what keeps a node's own
        ANSWERs from queueing 50s behind its waits (commit 1919074); the
        quiet window must not bring that back. Bound: no completion ANSWER
        waits more than 10s for the lock."""
        _, a, b = _page_mesh(self, ["A-R", "R-B"], ["R"], seed=41)
        pa = _page_parts(b.dest_hash, 12, tag=b"ab")
        pb = _page_parts(a.dest_hash, 12, tag=b"ba")
        for x, y in zip(pa, pb):
            a.send(x)
            b.send(y)
        done = wait_until(lambda: all(p in b.owner.received for p in pa) and all(p in a.owner.received for p in pb), 900.0)
        time.sleep(3.0)
        sa, sb = _page_stats(a), _page_stats(b)
        self.assertTrue(done, f"bidirectional transfer incomplete: A={sa} B={sb}")
        waits = [w for w in sa["answer_lock_waits"] + sb["answer_lock_waits"] if w is not None]
        self.assertTrue(waits, "no completion ANSWERs captured")
        self.assertLessEqual(max(waits), 10.0, f"an ANSWER waited {max(waits):.1f}s for the lock (all: {sorted(waits)[-5:]})")
        self.assertEqual(sa["slot_expired"] + sb["slot_expired"], 0)

    def test_three_hop_mixed_traffic_with_the_cap_enabled_never_drops(self):
        """(c) A-R1-R2-R3-B, announces and data mixed, the in-flight cap
        ENABLED (2): the night session dropped six packets as slot_expired;
        a slot wait that times out now proceeds instead. No drops, and the
        announce-class sends must not have held a data slot."""
        _, a, b = _page_mesh(self, ["A-R1", "R1-R2", "R2-R3", "R3-B"], ["R1", "R2", "R3"], seed=51,
                             config={"direct_fragmented_max_in_flight": "2"})
        self.assertEqual(a.resolved_paths[b.prefix].out_path_len, 3)
        iface = a.iface
        self.assertEqual(iface.direct_fragmented_max_in_flight, 2)
        data = _page_parts(b.dest_hash, 4, tag=b"d3")
        announces = [build_rns_packet("announce", dest_hash=b.dest_hash, payload=b"ann-%d-" % i + os.urandom(200))
                     for i in range(3)]
        for i, p in enumerate(data):
            a.send(p)
            if i < len(announces):
                a.send(announces[i])
        wait_until(lambda: all(p in b.owner.received for p in data), 900.0)
        time.sleep(3.0)
        recs = a.capture_records()
        results = [r for r in recs if r.get("event") == "direct_send_result"]
        self.assertTrue(results, "no DIRECT sends captured")
        self.assertEqual([r for r in results if r.get("method") == "slot_expired"], [])
        self.assertEqual(sum(1 for p in data if p in b.owner.received), 4, "every data part must arrive")
        self.assertTrue(any(r.get("slot_wait_s") is not None for r in results), "the cap was in effect")
        # both slot kinds were created: announces went through their own
        self.assertIn((b.prefix, "announce"), iface._fragmented_send_slots)
        self.assertIn((b.prefix, "data"), iface._fragmented_send_slots)
        for slot in iface._fragmented_send_slots.values():
            self.assertEqual(slot.holders(), 0, "every permit released")



if __name__ == "__main__":
    unittest.main()
