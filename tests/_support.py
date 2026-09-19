"""
Shared setup for the test suite. Standard-library unittest only (no
pytest dependency); pytest runs these too if installed.

    python3 -m unittest discover -s tests -v          # everything
    python3 -m unittest tests.test_wire_format -v     # one module
    python3 -m unittest tests.legacy.test_sim_scenarios   # archived simmesh scenarios, explicit only
    (SMCI_SKIP_SLOW=1 still marks those as skipped; discover no longer collects them)

A "unit" fixture here is a real SmartMeshCoreInterface brought online
against a one-node simulated mesh (testscripts/simmesh) -- construction
takes milliseconds and gives every method a fully configured instance,
which is simpler and more faithful than stubbing the constructor.
"""
import os
import sys
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
