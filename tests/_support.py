"""
Shared setup for the test suite. Standard-library unittest only (no
pytest dependency); pytest runs these too if installed.

    python3 -m unittest discover -s tests -v          # everything
    python3 -m unittest tests.test_wire_format -v     # one module
    python3 -m unittest tests.legacy.test_sim_scenarios   # archived simmesh scenarios, explicit only
    SMCI_SKIP_SLOW=1 python3 -m unittest discover -s tests   # skip the @slow two-node scenarios (~20 s)

A "unit" fixture here is a real SmartMeshCoreInterface brought online
against a one-node simulated mesh (testscripts/simmesh) -- construction
takes milliseconds and gives every method a fully configured instance,
which is simpler and more faithful than stubbing the constructor.
"""
import os
import sys
import tempfile
import time
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TESTSCRIPTS = os.path.join(REPO_ROOT, "testscripts")
if TESTSCRIPTS not in sys.path:
    sys.path.insert(0, TESTSCRIPTS)

from simmesh import SimMesh, ensure_rns, load_interface_module, wait_until, build_rns_packet, TEST_DEST_HASH  # noqa: E402,F401
from simmesh.harness import FAST_TIMING, dest_hash_for, summarize_capture  # noqa: E402,F401
from simmesh.radio import RadioOptions, node_prefix, node_pubkey  # noqa: E402,F401

SKIP_SLOW = os.environ.get("SMCI_SKIP_SLOW", "") not in ("", "0", "no", "false")
slow = unittest.skipIf(SKIP_SLOW, "SMCI_SKIP_SLOW set -- skipping simulated-mesh scenario tests")


def quiet_rns():
    """RNS.log at warning level and above only, so unit-test output stays readable."""
    ensure_rns()
    import RNS
    RNS.loglevel = RNS.LOG_WARNING


class SingleNodeCase(unittest.TestCase):
    """One online interface ("A") on a mesh where B exists in the topology
    but never comes up -- a sandbox for pure-logic tests."""

    mesh = None
    node = None

    @classmethod
    def setUpClass(cls):
        quiet_rns()
        cls.mesh = SimMesh(["A-B"], seed=1, startup_stagger_s=0.0,
                           radio_options=RadioOptions(auto_advert=False, advert_interval_s=0))
        cls.node = cls.mesh.add_node("A", config={"peer_discovery_enabled": "no"})
        cls.iface = cls.node.iface
        cls.module = cls.mesh.module

    @classmethod
    def tearDownClass(cls):
        cls.mesh.stop()

    def on_loop(self, fn, *args, timeout=10.0):
        """Run a plain function on the interface's own event loop (for the
        sync methods that spawn background tasks and must be called there)."""
        async def _call():
            return fn(*args)
        return self.node.run_on_loop(_call(), timeout=timeout)


# ---------------------------------------------------------------------------
# Two-node bring-up helpers (moved here 2026-09-20 from the archived
# tests/legacy/test_sim_scenarios.py, which still imports them): create the
# nodes, advert, bind, resolve DIRECT paths both ways, then prime a token.
# Used by the zero-hop scenario tests that remain in the unit suite.
# ---------------------------------------------------------------------------

def _setup_mesh(links, repeaters=(), seed=1, **kw):
    quiet_rns()
    capture_dir = tempfile.mkdtemp(prefix="smci-sim-cap-")
    mesh = SimMesh(links, repeaters=repeaters, seed=seed, capture_dir=capture_dir, **kw)
    return mesh


def _bring_up(mesh, names, timeout=40.0, config=None):
    """Create nodes, advert, bind, resolve DIRECT paths both ways."""
    for n in names:
        mesh.add_node(n, config=dict(config) if config else None)
    mesh.advert_all()
    try:
        # Re-adverts if the first flood didn't reach everyone (audit fix
        # 2026-09-19): mandatory for any mesh with two or more repeaters,
        # where a single advert round is a coin flip.
        assert mesh.advert_until_contacts(timeout=timeout), "contacts never populated"
        assert mesh.wait_bound(timeout), "bind-frame discovery never completed"
        if not mesh.wait_resolved(timeout):
            # Audit fix (2026-09-19): post-bind discovery is deliberately
            # bounded (POST_BIND_DISCOVERY_ROUNDS), and on a 2-repeater chain
            # all of those rounds can fall inside the window where the far
            # node's ADVERT has not arrived yet. The interface's documented
            # recovery for that is "the next send resolves it", so nudge a
            # small packet each way and wait again -- which is what a real
            # deployment does, rather than the harness demanding that
            # unsolicited discovery alone always win the race. Repeated:
            # a single nudge that lands inside a path-discovery backoff
            # window triggers no discovery at all (it takes the small-mesh
            # broadcast last resort instead), so keep nudging, spaced past
            # the fast profile's backoff, until the paths resolve. Each
            # nudge goes to a FRESH destination hash: three bootstrap
            # attempts for the same unknown destination trip the
            # interface's unknown-destination backoff (300s), after which
            # further nudges to it are dropped without any discovery.
            deadline = time.monotonic() + timeout
            while True:
                for name in names:
                    node = mesh.nodes[name]
                    if len(node.iface._resolved_paths) < len(names) - 1:
                        node.send(build_rns_packet("data", dest_hash=os.urandom(16), payload=b"nudge"))
                if mesh.wait_resolved(min(8.0, max(0.5, deadline - time.monotonic()))):
                    break
                assert time.monotonic() < deadline, "DIRECT paths never resolved"
            # Every nudge that was queued while paths were unresolved becomes
            # a real DIRECT send once they resolve; let that backlog drain
            # off both radios before the scenario's own traffic starts.
            wait_until(lambda: all(not n.iface._direct_exchange_lock_impl.locked() for n in mesh.nodes.values()), 90.0)
            time.sleep(1.0)
    except AssertionError:
        # unittest skips tearDown when setUp fails: stop the mesh here or its
        # interfaces (and their executor threads) outlive the test run.
        mesh.stop()
        raise


def _prime(sender, receiver, timeout=60.0):
    """One packet from receiver -> sender teaches the sender an RNS token
    for receiver.dest_hash, the way real traffic bootstraps DIRECT-primary."""
    receiver.send(build_rns_packet("data", dest_hash=receiver.dest_hash, payload=b"prime"))
    assert wait_until(lambda: receiver.dest_hash in sender.iface._rns_token_peer, timeout), "token never learned"


