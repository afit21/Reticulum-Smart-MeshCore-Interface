#!/usr/bin/env python3
"""
check_install_load.py -- load the deliverable interface file exactly the way
RNS loads a custom interface, and fail if that would not work.

Why this exists (2026-09-20): `RNS.Reticulum.__apply_config` does not import
a custom interface. It reads `<interfaces dir>/<type>.py` as TEXT and
`exec()`s it into a fresh globals dict that contains only `Interface` (the
base class) and `RNS` -- no `__file__`, no `sys.path` entry for the
directory, no package machinery -- and then takes `interface_globals[
"interface_class"]` (referenceprojects/Reticulum-master/RNS/Reticulum.py,
the "Loading external interface" branch; the installed RNS 1.4.2 does the
same). So the deliverable must be ONE self-contained module file whose
top level defines `interface_class`, and anything a source split introduces
(relative imports, `__file__`, a package) breaks the install silently at
rnsd startup. This script does that exec on the file (default:
`Interface/SmartMeshCoreInterface.py`, or the path given), from a
throwaway config directory that mimics `~/.reticulum/interfaces/`, and
then constructs nothing -- it only checks that the file executes and that
`interface_class` is a subclass of `RNS.Interfaces.Interface.Interface`
named SmartMeshCoreInterface. The MeshBench harness
(`testscripts/rns_multiprocess_sim.py node --backend real`) copies the
same file into a config dir and lets a real `RNS.Reticulum` load it; this
is the fast, hardware-free version of that step for the pre-commit hook.

    python3 testscripts/check_install_load.py [path/to/SmartMeshCoreInterface.py]
"""
import os
import shutil
import sys
import tempfile

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def main() -> int:
    src = sys.argv[1] if len(sys.argv) > 1 else os.path.join(REPO_ROOT, "Interface", "SmartMeshCoreInterface.py")
    if not os.path.isfile(src):
        print(f"no such file: {src}")
        return 2
    import RNS
    from RNS.Interfaces import Interface
    configdir = tempfile.mkdtemp(prefix="smci-install-check-")
    try:
        interfaces_dir = os.path.join(configdir, "interfaces")
        os.makedirs(interfaces_dir)
        dest = os.path.join(interfaces_dir, "SmartMeshCoreInterface.py")
        shutil.copy(src, dest)
        # Exactly RNS.Reticulum's loader: text in, exec, read interface_class.
        interface_globals = {"Interface": Interface.Interface, "RNS": RNS}
        with open(dest) as class_file:
            code = class_file.read()
        exec(code, interface_globals)
        cls = interface_globals.get("interface_class")
        if cls is None:
            print("FAIL: the file defines no top-level `interface_class`")
            return 1
        if not (isinstance(cls, type) and issubclass(cls, Interface.Interface)):
            print(f"FAIL: interface_class is {cls!r}, not an RNS Interface subclass")
            return 1
        if cls.__name__ != "SmartMeshCoreInterface":
            print(f"FAIL: interface_class is named {cls.__name__}, expected SmartMeshCoreInterface")
            return 1
        print(f"ok: {os.path.relpath(src, REPO_ROOT)} loads through RNS's exec() loader "
              f"({len(code)} bytes, {code.count(chr(10))} lines) and defines interface_class={cls.__name__}")
        return 0
    finally:
        shutil.rmtree(configdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
