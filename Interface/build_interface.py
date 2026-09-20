#!/usr/bin/env python3
"""
build_interface.py -- assemble the single-file deliverable
`Interface/SmartMeshCoreInterface.py` from the sources in `Interface/src/smci/`.

Why a build step (2026-09-20, phase 2 of the airtime / throughput pass): RNS
loads a custom interface by reading `<interfaces dir>/<type>.py` as TEXT and
`exec()`ing it into a globals dict that holds only `Interface` and `RNS` --
no `__file__`, no `sys.path` entry, no package machinery
(`RNS/Reticulum.py`, the "Loading external interface" branch; the check
`testscripts/check_install_load.py` does exactly that). A package cannot be
installed that way, so the code lives split by concern under `src/smci/`
(a real, importable package for development and tests) and this script
concatenates it into the one file `readme.md` and `update-interface.sh`
install. The result must be committed next to the sources: the pre-commit
hook runs `--check`, which refuses a deliverable that is not what the
sources build.

How the assembly works, so that the output is a plain module:

  * modules are concatenated in MODULE_ORDER (dependency order: helpers,
    locks, the mixins, then the class);
  * `__init__.py` contributes the package docstring, which becomes the
    module docstring;
  * top-level relative imports (`from .x import y`, `from . import x`) are
    dropped -- every name is a global of the one file;
  * top-level absolute imports are hoisted into one block after the
    docstring, deduplicated, first seen first;
  * everything else is copied verbatim (indented imports inside functions
    are left where they are);
  * the file ends with `interface_class = SmartMeshCoreInterface`, which
    `interface.py` defines.

Usage:
    python3 Interface/build_interface.py          # write the deliverable
    python3 Interface/build_interface.py --check  # exit 1 if it would differ
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "src", "smci")
OUT = os.path.join(HERE, "SmartMeshCoreInterface.py")

MODULE_ORDER = [
    "__init__",
    "_common",
    "_locks",
    "_config",
    "_observability",
    "_wire",
    "_peers",
    "_paths",
    "_direct",
    "_reconcile",
    "_routing",
    "interface",
]

_RELATIVE_IMPORT = re.compile(r"^from\s+\.\S*\s+import\s+|^from\s+\.\s+import\s+|^import\s+\.")
_ABSOLUTE_IMPORT = re.compile(r"^(import\s+\S+|from\s+[A-Za-z_]\S*\s+import\s+.+)$")
_DOCSTRING = re.compile(r'^"""(.*?)"""\n', re.S)


def _add_import(imports: list, line: str) -> None:
    """Hoist one top-level absolute import, first seen first; a
    `from X import a, b` merges its names into the first `from X import`
    line already collected (the assembled file has one import block)."""
    m = re.match(r"^from\s+(\S+)\s+import\s+(.+)$", line)
    if m:
        module, names = m.group(1), [n.strip() for n in m.group(2).split(",")]
        for i, existing in enumerate(imports):
            em = re.match(r"^from\s+(\S+)\s+import\s+(.+)$", existing)
            if em and em.group(1) == module:
                have = [n.strip() for n in em.group(2).split(",")]
                merged = have + [n for n in names if n not in have]
                imports[i] = f"from {module} import {', '.join(merged)}"
                return
    if line not in imports:
        imports.append(line)


def _read(name: str) -> str:
    path = os.path.join(SRC, name + ".py")
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def assemble() -> str:
    present = [m for m in MODULE_ORDER if os.path.isfile(os.path.join(SRC, m + ".py"))]
    if "__init__" not in present or "interface" not in present:
        raise SystemExit("src/smci needs at least __init__.py (the docstring) and interface.py (the class)")
    init_text = _read("__init__")
    m = _DOCSTRING.match(init_text)
    if not m:
        raise SystemExit("src/smci/__init__.py must start with the module docstring")
    docstring = m.group(0)
    imports: list = []
    bodies: list = []
    for name in present:
        text = _read(name)
        if name == "__init__":
            text = text[len(docstring):]
        body_lines = []
        in_paren_import = False
        for line in text.split("\n"):
            if in_paren_import:
                if ")" in line:
                    in_paren_import = False
                continue
            if _RELATIVE_IMPORT.match(line):
                if "(" in line and ")" not in line:
                    in_paren_import = True
                continue
            if _ABSOLUTE_IMPORT.match(line):
                _add_import(imports, line)
                continue
            body_lines.append(line)
        body = "\n".join(body_lines).strip("\n")
        if name == "__init__":
            if body.strip():
                raise SystemExit("src/smci/__init__.py may hold only the docstring and relative imports")
            continue
        bodies.append(f"# ---- {name}.py ----\n\n{body}\n")
    header = (
        "# Assembled by Interface/build_interface.py from Interface/src/smci/ -- edit the\n"
        "# sources and rebuild; the pre-commit hook refuses a stale deliverable.\n\n"
    )
    out = docstring + header + "\n".join(imports) + "\n\n\n" + "\n\n".join(bodies)
    if not out.endswith("\n"):
        out += "\n"
    return out


def main() -> int:
    check = "--check" in sys.argv[1:]
    text = assemble()
    if check:
        try:
            with open(OUT, encoding="utf-8") as fh:
                current = fh.read()
        except FileNotFoundError:
            current = ""
        if current == text:
            print(f"ok: {os.path.relpath(OUT)} matches the sources ({len(text)} bytes)")
            return 0
        import difflib
        diff = list(difflib.unified_diff(current.splitlines(), text.splitlines(), "deliverable", "sources", lineterm="", n=1))
        print("\n".join(diff[:80]))
        print(f"FAIL: {os.path.relpath(OUT)} differs from what src/smci builds ({len(diff)} diff lines); "
              f"run python3 Interface/build_interface.py")
        return 1
    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(f"wrote {os.path.relpath(OUT)}: {len(text)} bytes, {text.count(chr(10))} lines")
    return 0


if __name__ == "__main__":
    sys.exit(main())
