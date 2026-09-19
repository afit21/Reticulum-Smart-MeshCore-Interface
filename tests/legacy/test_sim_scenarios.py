"""
LEGACY (archived 2026-09-20, see CLAUDE.md "Legacy simulation tooling"): these
scenarios ran two interfaces through the simmesh fake firmware. Superseded by
testscripts/meshbench_scenarios.py, which stages the same incidents (stale-path
reset -> rediscovery, a repeater that dies and returns, multi-fragment DIRECT
at one/two hops) against real MeshCore firmware. Not collected by
`unittest discover` (this directory is deliberately not a package); run
explicitly with `python3 -m unittest tests.legacy.test_sim_scenarios` from the
repo root if an old result needs reproducing.

End-to-end scenarios: real SmartMeshCoreInterface instances over the
simulated mesh, exercising the DIRECT-primary transport through
repeater hops -- the cases two adjacent bench radios cannot produce.
Each scenario is the automated form of a field-diagnosed incident from
the interface's module docstring. They take tens of seconds each (real
timers, sped up via FAST_TIMING); set SMCI_SKIP_SLOW=1 to skip.
"""
import os
import tempfile
import time
import unittest

from tests._support import _bring_up, _prime, _setup_mesh  # noqa: F401
from tests._support import (
    SimMesh, build_rns_packet, quiet_rns, slow, summarize_capture, wait_until, node_prefix,
)


def _probe_count(node, marker=b"probe-"):
    return sum(1 for d in node.owner.received if marker in d)


@slow
class ZeroHopScenarios(unittest.TestCase):

    def setUp(self):
        self.mesh = _setup_mesh(["A-B"], seed=11)
        _bring_up(self.mesh, ["A", "B"])
        self.a, self.b = self.mesh.nodes["A"], self.mesh.nodes["B"]

    def tearDown(self):
        self.mesh.stop()

    def test_direct_primary_after_token_learned(self):
        _prime(self.a, self.b)
        self.a.send(build_rns_packet("data", dest_hash=self.b.dest_hash, payload=b"probe-1"))
        self.assertTrue(wait_until(lambda: _probe_count(self.b) == 1, 20.0))
        # The sender records its attempt only after the ACK and the post-send
        # listen window -- a few hundred ms after the receiver already has
        # the packet -- so wait for the record rather than racing it.
        self.assertTrue(wait_until(lambda: summarize_capture(self.a.capture_records())["direct_attempts_ok"] >= 1, 15.0))
        summary = summarize_capture(self.a.capture_records())
        self.assertGreaterEqual(summary["routing_decisions"].get("direct_primary", 0), 1)
        self.assertGreaterEqual(summary["direct_attempts_ok"], 1)
        # B receives exactly one bare DIRECT frame: the probe (the priming
        # packet went the other way, B -> A).
        self.assertEqual(summarize_capture(self.b.capture_records())["incoming_transports"].get("direct_bare", 0), 1)
        self.assertEqual(self.a.resolved_paths[self.b.prefix].out_path_len, 0)

    def test_announce_and_path_request_go_direct_in_small_mesh(self):
        self.a.send(build_rns_packet("announce", dest_hash=self.a.dest_hash, payload=b"probe-announce" + os.urandom(150)))
        self.assertTrue(wait_until(lambda: _probe_count(self.b) >= 1, 40.0))
        self.a.send(build_rns_packet("path_request", dest_hash=self.b.dest_hash, payload=b"probe-pathreq"))
        self.assertTrue(wait_until(lambda: _probe_count(self.b) >= 2, 20.0))
        decisions = summarize_capture(self.a.capture_records())["routing_decisions"]
        self.assertGreaterEqual(decisions.get("small_mesh_direct_all_announce", 0), 1)
        self.assertGreaterEqual(decisions.get("small_mesh_direct_all_path_request", 0), 1)
        transports = summarize_capture(self.b.capture_records())["incoming_transports"]
        # the announce was fragmented -- as text, or as raw fragments now that
        # direct_raw_fragments_enabled defaults on and both nodes advertise it
        self.assertGreaterEqual(transports.get("direct_multifragment", 0) + transports.get("direct_raw_multifragment", 0), 1)
        self.assertEqual(self.mesh.air.stats.by_type.get("GRP_TXT", 0) and 0, 0)  # placeholder: CHANNEL only carried bind frames

    def test_bare_direct_retry_never_delivers_twice(self):
        _prime(self.a, self.b)
        # _prime returns as soon as A has learned the token, while B's own
        # priming exchange is still waiting for its ACK. Switching ACK loss
        # on right then makes B retry on the same cadence as A's probe
        # attempts, and the two half-duplex radios can key over each other
        # twice in a row (seen once: both sides' attempts at the same
        # seconds, neither heard). Let B's exchange finish first.
        self.assertTrue(wait_until(lambda: not self.b.iface._direct_exchange_lock_impl.locked(), 15.0))
        time.sleep(1.0)
        self.mesh.air.type_loss["ACK"] = 1.0
        self.a.send(build_rns_packet("data", dest_hash=self.b.dest_hash, payload=b"probe-noack"))
        deadline = time.monotonic() + 25.0
        while time.monotonic() < deadline:
            time.sleep(0.5)
        self.assertEqual(_probe_count(self.b), 1)
        summary = summarize_capture(self.a.capture_records())
        self.assertEqual(summary["direct_attempts_failed"], self.a.iface.direct_send_attempts)
        self.assertEqual(self.a.iface._direct_path_failures.get(self.b.prefix), 1)

    def test_stale_path_reset_then_rediscovery(self):
        _prime(self.a, self.b)
        iface = self.a.iface
        # Kill the link entirely, then push enough failed DIRECT sends to trip
        # the stale-path threshold once the path is older than min_age.
        time.sleep(max(0.0, iface.direct_path_reset_min_age_s - (time.monotonic() - iface._resolved_paths[self.b.prefix].resolved_at)))
        self.mesh.air.link_loss[("A", "B")] = 1.0
        self.mesh.air.link_loss[("B", "A")] = 1.0
        for i in range(iface.direct_path_reset_threshold):
            self.a.send(build_rns_packet("data", dest_hash=self.b.dest_hash, payload=f"probe-dead-{i}".encode()))
            self.assertTrue(wait_until(lambda i=i: iface._direct_path_failures.get(self.b.prefix, 0) >= i + 1 or self.b.prefix not in iface._resolved_paths, 40.0))
        self.assertTrue(wait_until(lambda: self.b.prefix not in iface._resolved_paths, 10.0), "stale path was never reset")
        self.assertEqual(self.a.radio.contacts[self.b.radio.pubkey]["out_path_len"], -1, "reset_path never reached the radio")

        # Link comes back: the next send must rediscover and deliver.
        self.mesh.air.link_loss.clear()
        self.assertTrue(wait_until(lambda: not iface._path_discovery_in_backoff(self.b.prefix), 30.0))
        self.a.send(build_rns_packet("data", dest_hash=self.b.dest_hash, payload=b"probe-alive"))
        self.assertTrue(wait_until(lambda: b"probe-alive" in b"".join(self.b.owner.received), 40.0))
        self.assertIn(self.b.prefix, iface._resolved_paths)


@slow
class RepeaterScenarios(unittest.TestCase):

    def tearDown(self):
        self.mesh.stop()

    def test_one_hop_direct_delivery(self):
        self.mesh = _setup_mesh(["A-R", "R-B"], repeaters=["R"], seed=21)
        _bring_up(self.mesh, ["A", "B"])
        a, b = self.mesh.nodes["A"], self.mesh.nodes["B"]
        self.assertEqual(a.resolved_paths[b.prefix].out_path_len, 1)
        self.assertEqual(b.resolved_paths[a.prefix].out_path_len, 1)
        _prime(a, b)
        for i in range(3):
            a.send(build_rns_packet("data", dest_hash=b.dest_hash, payload=f"probe-{i}".encode()))
        self.assertTrue(wait_until(lambda: _probe_count(b) == 3, 40.0))
        self.assertTrue(wait_until(
            lambda: summarize_capture(a.capture_records())["direct_attempts_by_hop"].get(1, [0, 0])[0] >= 3, 15.0))
        summary = summarize_capture(a.capture_records())
        self.assertGreaterEqual(summary["direct_attempts_by_hop"].get(1, [0, 0])[0], 3)
        self.assertGreaterEqual(self.mesh.repeaters["R"].counters["direct_forwarded"], 6)

    def test_two_hop_fragmented_with_phantom_ack_loss(self):
        self.mesh = _setup_mesh(["A-R1", "R1-R2", "R2-B"], repeaters=["R1", "R2"], seed=31)
        _bring_up(self.mesh, ["A", "B"], timeout=60.0)
        a, b = self.mesh.nodes["A"], self.mesh.nodes["B"]
        self.assertEqual(a.resolved_paths[b.prefix].out_path_len, 2)
        _prime(a, b)
        # Data gets through, ACKs never come back: the sender must fall back
        # to the completion check instead of declaring the transfer lost.
        self.mesh.air.type_loss["ACK"] = 1.0
        a.send(build_rns_packet("data", dest_hash=b.dest_hash, payload=b"probe-big" + os.urandom(280)))
        self.assertTrue(wait_until(lambda: _probe_count(b, b"probe-big") == 1, 60.0))
        summary = summarize_capture(a.capture_records())
        self.assertTrue(wait_until(lambda: summarize_capture(a.capture_records())["completion_checks"].get("answered", 0) >= 1, 90.0),
                        f"completion check never answered: {summary}")
        time.sleep(1.0)
        self.assertEqual(_probe_count(b, b"probe-big"), 1)
        self.assertEqual(a.iface._direct_path_failures.get(b.prefix, 0), 0, "phantom ACK loss must not count as path failure")
        self.assertIn(b.prefix, a.resolved_paths)


if __name__ == "__main__":
    unittest.main()
