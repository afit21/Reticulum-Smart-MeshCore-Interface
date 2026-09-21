"""
The module split of 2026-09-20 (phase 2 of the airtime / throughput pass):
`Interface/SmartMeshCoreInterface.py` is assembled from the package
`Interface/src/smci/` by `Interface/build_interface.py`, because RNS
`exec()`s a custom interface as one text file (see
`testscripts/check_install_load.py`).

Pinned:
  * the module-level PRIORITY_* names (defaults of mixin methods) equal the
    class constants the code reads through `self.`;
  * the deliverable is exactly what the sources build (only for the repo's
    own deliverable, not for a build pinned with SMCI_INTERFACE_PATH);
  * the source package imports, and its class exposes the same public and
    private methods as the assembled one -- a method left in only one of
    the two would be a split error the golden tests cannot see;
  * every mixin's methods are unique across the MRO (no mixin shadows
    another's method of the same name).
"""
import importlib.util
import os
import subprocess
import sys
import unittest

from tests._support import load_interface_module, REPO_ROOT
from testscripts.simmesh.harness import INTERFACE_PATH

DELIVERABLE = os.path.join(REPO_ROOT, "Interface", "SmartMeshCoreInterface.py")
SRC = os.path.join(REPO_ROOT, "Interface", "src")


class ModuleSplit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_interface_module()

    def test_priority_constants_mirror_the_class(self):
        m = self.module
        cls = m.SmartMeshCoreInterface
        for name in ("PRIORITY_HANDSHAKE", "PRIORITY_ANSWER", "PRIORITY_NORMAL", "PRIORITY_LOW"):
            self.assertEqual(getattr(m, name), getattr(cls, name), name)

    def test_deliverable_matches_the_sources(self):
        if os.path.abspath(INTERFACE_PATH) != DELIVERABLE:
            self.skipTest("a pinned build is under test, not the repo's deliverable")
        r = subprocess.run([sys.executable, os.path.join(REPO_ROOT, "Interface", "build_interface.py"), "--check"],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stdout[-2000:])

    def test_source_package_has_the_same_methods_as_the_deliverable(self):
        if os.path.abspath(INTERFACE_PATH) != DELIVERABLE:
            self.skipTest("a pinned build is under test, not the repo's deliverable")
        sys.path.insert(0, SRC)
        try:
            for name in [n for n in list(sys.modules) if n == "smci" or n.startswith("smci.")]:
                del sys.modules[name]
            import smci  # noqa: E402
        finally:
            sys.path.remove(SRC)
        pkg_cls = smci.SmartMeshCoreInterface
        one_cls = self.module.SmartMeshCoreInterface
        def methods(c):
            return {n for n in dir(c) if callable(getattr(c, n, None)) and not n.startswith("__")}
        self.assertEqual(methods(pkg_cls), methods(one_cls))
        self.assertEqual([b.__name__ for b in pkg_cls.__bases__], [b.__name__ for b in one_cls.__bases__])

    def test_no_mixin_shadows_another(self):
        cls = self.module.SmartMeshCoreInterface
        seen = {}
        for base in cls.__mro__:
            if base.__name__ in ("object", "Interface") or not base.__name__.endswith("Mixin"):
                continue
            for name, value in vars(base).items():
                if callable(value) or isinstance(value, (staticmethod, classmethod, property)):
                    self.assertNotIn(name, seen, f"{name} defined in both {seen.get(name)} and {base.__name__}")
                    seen[name] = base.__name__


if __name__ == "__main__":
    unittest.main()
