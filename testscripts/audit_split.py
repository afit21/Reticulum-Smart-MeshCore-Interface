#!/usr/bin/env python3
"""
audit_split.py -- check that one build of the interface is a PURE MOVE of
another: same functions with the same bodies, same class constants with the
same values, same `__init__`, same top-level names -- whatever module or
mixin class each now lives in.

Written for phase 2 of the 2026-09-20 airtime / throughput pass (the module
split), as the mechanical, independent-of-the-golden-tests audit of every
split commit: the golden tests pin defaults and encoded frames, this pins
the code text. Functions are matched by NAME across all classes and the
module level (a method moved from `SmartMeshCoreInterface` into a mixin
keeps its name); a body is compared after `ast.unparse`, so comments and
blank lines do not count but any token does. Class constants are every
ALL_CAPS or `_ALL_CAPS` assignment in a class body, compared by value.

    python3 testscripts/audit_split.py [old-build] [new-build]

defaults to the frozen alpha 0.1.3 build (referenceprojects/) versus the
current deliverable; `--against-head` compares HEAD's deliverable to the
working tree's instead. Exit 1 on any changed body or constant; added or
removed functions are listed (they are a phase-1/3 change, not a split
error, when the commit says so).
"""
import ast
import os
import subprocess
import sys

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FROZEN = os.path.join(REPO, "referenceprojects", "SmartMeshCoreInterface_alpha0.1.3.py")
CURRENT = os.path.join(REPO, "Interface", "SmartMeshCoreInterface.py")


def _index(source: str) -> dict:
    tree = ast.parse(source)
    funcs, consts, classes = {}, {}, {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            classes[node.name] = [b.id for b in node.bases if isinstance(b, ast.Name)]
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    key = item.name
                    if key in funcs and funcs[key][0] != node.name and key in ("__init__", "__str__", "acquire", "release",
                                                                                "_wake_next", "locked", "__aenter__", "__aexit__",
                                                                                "capacity", "holders", "waiting", "record", "_prune", "_busy_s",
                                                                                "wait_for_budget", "__call__"):
                        key = f"{node.name}.{item.name}"
                    funcs[key] = (node.name, ast.unparse(item))
                elif isinstance(item, ast.Assign) and len(item.targets) == 1 and isinstance(item.targets[0], ast.Name):
                    name = item.targets[0].id
                    if name.lstrip("_").isupper():
                        consts[f"{node.name}.{name}" if node.name != "SmartMeshCoreInterface" else name] = ast.unparse(item.value)
    for item in tree.body:
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            funcs[item.name] = ("<module>", ast.unparse(item))
        elif isinstance(item, ast.Assign) and len(item.targets) == 1 and isinstance(item.targets[0], ast.Name):
            consts[f"<module>.{item.targets[0].id}"] = ast.unparse(item.value)
    return {"funcs": funcs, "consts": consts, "classes": classes}


def audit(old_src: str, new_src: str) -> int:
    a, b = _index(old_src), _index(new_src)
    problems = 0
    for name in sorted(set(a["funcs"]) | set(b["funcs"])):
        if name not in b["funcs"]:
            print(f"  removed function: {name} (was in {a['funcs'][name][0]})")
        elif name not in a["funcs"]:
            print(f"  added function:   {name} (in {b['funcs'][name][0]})")
        elif a["funcs"][name][1] != b["funcs"][name][1]:
            print(f"  CHANGED BODY:     {name} ({a['funcs'][name][0]} -> {b['funcs'][name][0]})")
            problems += 1
        elif a["funcs"][name][0] != b["funcs"][name][0]:
            print(f"  moved (same body): {name} {a['funcs'][name][0]} -> {b['funcs'][name][0]}")
    for name in sorted(set(a["consts"]) | set(b["consts"])):
        if name not in b["consts"]:
            print(f"  removed constant: {name} = {a['consts'][name]}")
            if not name.startswith("<module>"):
                problems += 1
        elif name not in a["consts"]:
            print(f"  added constant:   {name} = {b['consts'][name]}")
        elif a["consts"][name] != b["consts"][name]:
            print(f"  CHANGED CONSTANT: {name}: {a['consts'][name]} -> {b['consts'][name]}")
            problems += 1
    if "SmartMeshCoreInterface" in b["classes"]:
        print(f"  SmartMeshCoreInterface bases: {b['classes']['SmartMeshCoreInterface']}")
    return problems


def main() -> int:
    args = [x for x in sys.argv[1:] if not x.startswith("--")]
    if "--against-head" in sys.argv:
        old = subprocess.run(["git", "show", "HEAD:Interface/SmartMeshCoreInterface.py"], cwd=REPO,
                             capture_output=True, text=True, check=True).stdout
        new = open(CURRENT, encoding="utf-8").read()
        print("audit: HEAD deliverable -> working tree")
    else:
        old_path = args[0] if args else FROZEN
        new_path = args[1] if len(args) > 1 else CURRENT
        old = open(old_path, encoding="utf-8").read()
        new = open(new_path, encoding="utf-8").read()
        print(f"audit: {os.path.relpath(old_path, REPO)} -> {os.path.relpath(new_path, REPO)}")
    problems = audit(old, new)
    print("PURE MOVE" if problems == 0 else f"{problems} changed body/constant(s)")
    return 0 if problems == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
