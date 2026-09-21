"""
Golden snapshot of every shipped configuration default (2026-09-20).

Why this exists: `Interface/SmartMeshCoreInterface.py` is about to be
refactored -- split into modules and reassembled -- and the refactor must
not move a single default. This test is the behavioural oracle for that:
`tests/golden/config_defaults.json` was generated from the frozen alpha
0.1.3 build (`referenceprojects/SmartMeshCoreInterface_alpha0.1.3.py`,
byte-identical to the interface at the time), and every run rebuilds the
same structure from the interface under test (`tests._support.
load_interface_module()`, i.e. `Interface/SmartMeshCoreInterface.py` or
`SMCI_INTERFACE_PATH`) and diffs it, per `_configure_*` method, key by key.
It complements `tests/test_shipped_defaults.py` (which pins the same values
as Python literals in its own source): that file is the hand-maintained
record, this one is a machine-generated snapshot of a known build, so the
two can never drift together by a single careless edit.

Method (the same technique as test_shipped_defaults, deliberately copied
rather than imported so the two tests stay independent): one bare instance
(`SmartMeshCoreInterface.__new__`, no `__init__`, no meshcore, no
RNS.Reticulum), each `_configure_*({})` called in constructor order, and the
attributes each call newly sets are diffed out of `vars(bare)`. Values are
JSON-shaped only: scalars, None, lists/dicts, sets as sorted lists;
callables, locks and handles are dropped. Types are compared too (5 is not
5.0, True is not 1).

A failure names every differing key with both values. It means either

  * an unintended drift -- restore the literal in the interface; or
  * a deliberate default change -- which must regenerate the snapshot IN THE
    SAME COMMIT, and the commit message must say the golden config snapshot
    was regenerated and why (the module docstring, changelog.md and
    readme.md also need the entry, as for any default change):

        python3 tests/test_golden_config_defaults.py --regenerate [path-to-build]

    `path-to-build` defaults to the frozen alpha 0.1.3 build; pass
    `Interface/SmartMeshCoreInterface.py` to snapshot the live interface
    after a deliberate change. Re-running --regenerate on the same build
    reproduces the file byte for byte.
"""
import json
import os
import sys
import unittest

if __package__ in (None, ""):
    # Run directly (`python3 tests/test_golden_config_defaults.py --regenerate`):
    # make the repo root importable so `tests._support` resolves as it does
    # under `python3 -m unittest discover -s tests`.
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tests._support import load_interface_module

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
GOLDEN_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "golden", "config_defaults.json")
FROZEN_BUILD = os.path.join(REPO_ROOT, "referenceprojects", "SmartMeshCoreInterface_alpha0.1.3.py")

# Constructor order of the _configure_* calls (SmartMeshCoreInterface.__init__).
CONFIGURE_ORDER = (
    "_configure_identity",
    "_configure_transport",
    "_configure_channel",
    "_configure_radio",
    "_configure_fragmentation",
    "_configure_retry",
    "_configure_path_discovery",
    "_configure_peer_discovery",
    "_configure_observability",
)

_SCALAR_TYPES = (bool, int, float, str, type(None), list, tuple, dict)


def _snapshot(module):
    """{method name: {attr: value}} for the attributes each `_configure_*({})`
    newly sets (or changes) on a bare, un-`__init__`ed instance, in
    constructor order. JSON-shaped values only; the result is passed through
    a JSON round trip so tuples become lists and it compares exactly like
    the file on disk."""
    cls = module.SmartMeshCoreInterface
    bare = cls.__new__(cls)
    per_method = {}
    for name in CONFIGURE_ORDER:
        before = dict(vars(bare))
        getattr(bare, name)({})
        new = {}
        for key in sorted(vars(bare)):
            value = vars(bare)[key]
            if key in before and before[key] == value:
                continue
            if isinstance(value, (set, frozenset)):
                new[key] = sorted(value)
            elif isinstance(value, _SCALAR_TYPES):
                new[key] = value
        per_method[name] = new
    return json.loads(json.dumps(per_method))


def _render(per_method):
    return json.dumps(per_method, indent=2, sort_keys=True) + "\n"


def _load_golden(path=GOLDEN_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def regenerate(build_path=FROZEN_BUILD, golden_path=GOLDEN_PATH):
    if not os.path.exists(build_path):
        raise SystemExit(f"build not found: {build_path}")
    module = load_interface_module(module_name="smci_golden_source", path=build_path)
    text = _render(_snapshot(module))
    os.makedirs(os.path.dirname(golden_path), exist_ok=True)
    with open(golden_path, "w", encoding="utf-8") as f:
        f.write(text)
    return text


def _same(a, b):
    """Equality plus type identity, so 5 != 5.0 and True != 1 (a default
    that silently changes type is a change)."""
    if type(a) is not type(b):
        return False
    if isinstance(a, list):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        return set(a) == set(b) and all(_same(a[k], b[k]) for k in a)
    return a == b


class GoldenConfigDefaults(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_interface_module()
        cls.actual = _snapshot(cls.module)
        cls.golden = _load_golden()

    def _check(self, method):
        self.assertIn(method, self.golden, f"{method} is missing from {GOLDEN_PATH}; regenerate it")
        expected = self.golden[method]
        actual = self.actual[method]
        problems = []
        for key in sorted(set(expected) | set(actual)):
            if key not in actual:
                problems.append(f"  {key}: no longer set (golden {expected[key]!r})")
            elif key not in expected:
                problems.append(f"  {key}: newly set to {actual[key]!r}, not in the golden snapshot")
            elif not _same(expected[key], actual[key]):
                problems.append(f"  {key}: now {actual[key]!r}, golden {expected[key]!r}")
        if problems:
            self.fail(
                f"{method}({{}}) defaults differ from the golden snapshot "
                f"({os.path.relpath(GOLDEN_PATH, REPO_ROOT)}):\n" + "\n".join(problems)
                + "\nEither restore the literal in the interface or, for a deliberate default change, "
                "regenerate the snapshot in the same commit "
                "(`python3 tests/test_golden_config_defaults.py --regenerate Interface/SmartMeshCoreInterface.py`) "
                "and say so in the commit message."
            )

    def test_configure_identity_defaults(self):
        self._check("_configure_identity")

    def test_configure_transport_defaults(self):
        self._check("_configure_transport")

    def test_configure_channel_defaults(self):
        self._check("_configure_channel")

    def test_configure_radio_defaults(self):
        self._check("_configure_radio")

    def test_configure_fragmentation_defaults(self):
        self._check("_configure_fragmentation")

    def test_configure_retry_defaults(self):
        self._check("_configure_retry")

    def test_configure_path_discovery_defaults(self):
        self._check("_configure_path_discovery")

    def test_configure_peer_discovery_defaults(self):
        self._check("_configure_peer_discovery")

    def test_configure_observability_defaults(self):
        self._check("_configure_observability")

    def test_every_configure_method_is_snapshotted(self):
        """A new or renamed `_configure_*` method must appear in the golden
        file (and in CONFIGURE_ORDER above, in constructor order)."""
        cls = self.module.SmartMeshCoreInterface
        methods = sorted(
            name for name in dir(cls)
            if name.startswith("_configure_") and callable(getattr(cls, name))
        )
        self.assertEqual(methods, sorted(CONFIGURE_ORDER),
                         "the interface's _configure_* methods no longer match CONFIGURE_ORDER")
        self.assertEqual(sorted(self.golden), sorted(CONFIGURE_ORDER),
                         f"{GOLDEN_PATH} does not cover exactly the _configure_* methods; regenerate it")

    def test_golden_file_is_canonical(self):
        """The file on disk is exactly what --regenerate writes for its own
        content (sorted keys, two-space indent, trailing newline), so a
        regenerate that changes nothing produces no diff."""
        with open(GOLDEN_PATH, "r", encoding="utf-8") as f:
            on_disk = f.read()
        self.assertEqual(on_disk, _render(self.golden))


if __name__ == "__main__":
    if "--regenerate" in sys.argv:
        i = sys.argv.index("--regenerate")
        build = sys.argv[i + 1] if len(sys.argv) > i + 1 and not sys.argv[i + 1].startswith("-") else FROZEN_BUILD
        text = regenerate(build)
        total = sum(len(v) for v in json.loads(text).values())
        print(f"wrote {GOLDEN_PATH}: {total} defaults across {len(CONFIGURE_ORDER)} _configure_* methods, from {build}")
    else:
        unittest.main()
