"""
Golden snapshot of every encoded frame the interface puts on the air
(2026-09-20).

Why this exists: `Interface/SmartMeshCoreInterface.py` is about to be
refactored (split into modules and reassembled) and, in a later phase
(phase 3 of the current work), its wire format changed deliberately. The
refactor must not move a single byte; the format change must move exactly
the bytes it means to. `tests/golden/wire_format.json` was generated from
the frozen alpha 0.1.3 build (`referenceprojects/SmartMeshCoreInterface_
alpha0.1.3.py`, byte-identical to the interface at the time) by replaying a
fixed list of encoder calls with FIXED inputs (fixed payloads, pkt_id
0x1234, fixed pubkeys) on a bare instance -- no meshcore, no RNS.Reticulum
-- and every run replays the same calls on the interface under test
(`tests._support.load_interface_module()`, i.e. `Interface/
SmartMeshCoreInterface.py` or `SMCI_INTERFACE_PATH`) and compares the
output byte for byte (text frames as the str, binary frames as hex).

Covered, per case name: the "R" RNS frames (`_encode_channel_fastpath`,
`_encode_channel_multifragment`, `_encode_direct_bare`), the "P" bind
frames for both types and every capability combination, the "Q"
completion QUERY/ANSWER frames across protocol versions v1/v2/v3,
`complete`, `held` bitmaps, frag_total and nonce, the 13-byte raw binary
DIRECT fragment header for every attempt/report flag combination and the
frag idx/total edges, the payload-budget helpers, and every ALL_CAPS
int/str/bytes constant on the class (markers, versions, flag bits, header
sizes, firmware limits). Each encoded frame in the snapshot is also decoded
by the build under test's own decoder and its fields checked against the
inputs, so the snapshot is known-valid and a decoder that stops accepting
its own build's frames fails too.

A failure names every differing case with both values. It means either

  * an unintended change -- the refactor moved a byte; restore it; or
  * a deliberate wire-format change (phase 3) -- which must regenerate the
    snapshot IN THE SAME COMMIT as the format change, and the commit message
    must say the golden wire-format snapshot was regenerated and which
    frames changed (the module docstring and changelog.md carry the design
    record, and `mixed_builds` in the MeshBench suite the cross-build check):

        python3 tests/test_golden_wire_format.py --regenerate [path-to-build]

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
    # Run directly (`python3 tests/test_golden_wire_format.py --regenerate`):
    # make the repo root importable so `tests._support` resolves as it does
    # under `python3 -m unittest discover -s tests`.
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tests._support import load_interface_module

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
GOLDEN_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "golden", "wire_format.json")
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

# Fixed inputs. The two pubkeys are the desktop's (7b...) and laptop's
# (34...) real prefixes padded with zeros -- recognisable in a diff, never
# a real full key.
OWN_PUBKEY_HEX = "7bd024b5d082" + "00" * 26
DST_PUBKEY_HEX = "343377c464a7" + "00" * 26
OWN_NODE_NAME = "afipc"
PKT_ID = 0x1234
FIXED_BYTES = bytes(range(0, 200))

# Instance attributes every encoder case starts from; a case's "attrs"
# overrides these. `declares_upstream_rns` and `direct_raw_fragments_enabled`
# drive `_bind_capability`; `_own_pubkey_hex` the bind frame's prefix;
# `_own_node_name` the CHANNEL budget's "<name>: " prefix cost.
BASE_ATTRS = {
    "_own_pubkey_hex": OWN_PUBKEY_HEX,
    "_own_node_name": OWN_NODE_NAME,
    "declares_upstream_rns": False,
    "direct_raw_fragments_enabled": True,
}

ENCODERS = (
    "_encode_channel_fastpath",
    "_encode_channel_multifragment",
    "_encode_direct_bare",
    "_encode_bind_frame",
    "_encode_completion_frame",
    "_encode_completion_frame_v4",
    "_encode_raw_fragment",
    "_encode_raw_parity",
)
BUDGETS = (
    "_channel_payload_budget",
    "_channel_multifragment_payload_budget",
    "_direct_payload_budget",
    "_direct_multifragment_payload_budget",
    "_direct_raw_payload_budget",
)


# --------------------------------------------------------------------------
# Bare instance
# --------------------------------------------------------------------------

def _bare(module, attrs=None):
    """A bare instance (`__new__`, no `__init__`) with every `_configure_*({})`
    applied in constructor order, then the fixed attributes the encoders
    read, then the case's own overrides."""
    cls = module.SmartMeshCoreInterface
    inst = cls.__new__(cls)
    for name in CONFIGURE_ORDER:
        getattr(inst, name)({})
    for key, value in BASE_ATTRS.items():
        setattr(inst, key, value)
    for key, value in (attrs or {}).items():
        setattr(inst, key, value)
    return inst


# --------------------------------------------------------------------------
# The case list. Order and inputs are fixed; a case is
#   {"name", "call", "args", "attrs"?} plus, once encoded, "kind"/"encoded".
# --------------------------------------------------------------------------

def _payload(n):
    return FIXED_BYTES[:n].hex()


def _case(name, call, attrs=None, **args):
    case = {"name": name, "call": call, "args": args}
    if attrs:
        case["attrs"] = attrs
    return case


def build_cases(module):
    cls = module.SmartMeshCoreInterface
    cases = []

    # -- "R": CHANNEL fast path --------------------------------------------
    for attempt in (0, 2):
        cases.append(_case(f"channel_fastpath_attempt{attempt}_p40", "_encode_channel_fastpath",
                           payload_hex=_payload(40), pkt_id=PKT_ID, attempt=attempt))
    cases.append(_case("channel_fastpath_attempt0_empty", "_encode_channel_fastpath",
                       payload_hex=_payload(0), pkt_id=PKT_ID, attempt=0))
    cases.append(_case("channel_fastpath_attempt0_p1", "_encode_channel_fastpath",
                       payload_hex=_payload(1), pkt_id=PKT_ID, attempt=0))
    cases.append(_case("channel_fastpath_attempt255_pktid_ffff", "_encode_channel_fastpath",
                       payload_hex=_payload(8), pkt_id=0xFFFF, attempt=255))

    # -- "R": shared multi-fragment shape (CHANNEL and DIRECT) ---------------
    for idx, total, attempt in ((0, 1, 0), (2, 5, 1), (254, 255, 3), (0, 2, 0), (1, 2, 0), (7, 8, 255)):
        cases.append(_case(f"multifragment_idx{idx}_of{total}_attempt{attempt}_p50",
                           "_encode_channel_multifragment", payload_hex=_payload(50),
                           pkt_id=PKT_ID, frag_idx=idx, frag_total=total, attempt=attempt))
    cases.append(_case("multifragment_idx0_of1_attempt0_empty", "_encode_channel_multifragment",
                       payload_hex=_payload(0), pkt_id=PKT_ID, frag_idx=0, frag_total=1, attempt=0))

    # -- "R": DIRECT bare -----------------------------------------------------
    for n in (0, 1, 40, 70):
        cases.append(_case(f"direct_bare_p{n}", "_encode_direct_bare", payload_hex=_payload(n)))

    # -- "P": bind frames -----------------------------------------------------
    for type_name, frame_type in (("request", cls.BIND_TYPE_REQUEST), ("response", cls.BIND_TYPE_RESPONSE)):
        for upstream in (False, True):
            for raw in (False, True):
                cases.append(_case(
                    f"bind_{type_name}_upstream{int(upstream)}_raw{int(raw)}_attempt0", "_encode_bind_frame",
                    attrs={"declares_upstream_rns": upstream, "direct_raw_fragments_enabled": raw},
                    frame_type=frame_type, attempt=0))
    cases.append(_case("bind_request_attempt1", "_encode_bind_frame", frame_type=cls.BIND_TYPE_REQUEST, attempt=1))
    cases.append(_case("bind_response_attempt7", "_encode_bind_frame", frame_type=cls.BIND_TYPE_RESPONSE, attempt=7))
    cases.append(_case("bind_request_attempt255", "_encode_bind_frame", frame_type=cls.BIND_TYPE_REQUEST, attempt=255))
    cases.append(_case("bind_request_attempt300_wraps", "_encode_bind_frame", frame_type=cls.BIND_TYPE_REQUEST, attempt=300))
    cases.append(_case("bind_request_no_own_pubkey", "_encode_bind_frame", attrs={"_own_pubkey_hex": ""},
                       frame_type=cls.BIND_TYPE_REQUEST, attempt=0))

    # -- "Q": completion frames ---------------------------------------------
    # v3 is listed explicitly since 2026-09-20 (M2 made v4 the default): its
    # layout stays pinned even though no frame is encoded as v3 by default.
    versions = (("v1", cls.COMPLETION_PROTOCOL_VERSION_V1), ("v2", cls.COMPLETION_PROTOCOL_VERSION_V2),
                ("v3", getattr(cls, "COMPLETION_PROTOCOL_VERSION_V3", 3)), ("default", None))
    types = (("query", cls.COMPLETION_TYPE_QUERY), ("answer", cls.COMPLETION_TYPE_ANSWER))
    for ver_name, version in versions:
        for type_name, frame_type in types:
            for frag_total in (1, 8, 9, 255):
                for held_name, complete in (("none", False), ("some", False), ("all", True)):
                    held = {"none": [], "some": [0, 3, 7], "all": list(range(frag_total))}[held_name]
                    nonces = (0, 0x5A, 0xF1) if (version is None or version >= 3) else (0,)
                    for nonce in nonces:
                        cases.append(_case(
                            f"completion_{ver_name}_{type_name}_total{frag_total}_held_{held_name}"
                            f"_complete{int(complete)}_nonce{nonce:02x}",
                            "_encode_completion_frame", frame_type=frame_type, pkt_id=PKT_ID,
                            frag_total=frag_total, complete=complete, held=held, version=version, nonce=nonce))
    # -- "Q" v4: the multi-entry frame (phase 3 M2, 2026-09-20) -------------
    # `entries` = [pkt_id, frag_total, complete, held-list]; a QUERY carries
    # zero bitmaps, an ANSWER / REPORT the have-bitmaps.
    v4_sets = {
        "one": [[PKT_ID, 3, False, [0, 2]]],
        "window4": [[PKT_ID, 3, True, [0, 1, 2]], [PKT_ID + 1, 4, False, [1, 3]], [PKT_ID + 2, 3, False, []],
                    [PKT_ID + 3, 9, False, [0, 8]]],
        "max8": [[PKT_ID + k, 3 + (k % 2), k % 3 == 0, list(range(k % 4))] for k in range(8)],
        "held_out_of_range": [[PKT_ID, 3, False, [0, 3, 7]]],
    }
    for set_name, entries in v4_sets.items():
        for type_name, frame_type in types:
            for nonce in (0, 0x5A, 0xF1):
                cases.append(_case(f"completion_v4_{type_name}_{set_name}_nonce{nonce:02x}",
                                   "_encode_completion_frame_v4", frame_type=frame_type, entries=entries, nonce=nonce))
    # nonce is ignored below v3; `complete` is independent of the bitmap.
    cases.append(_case("completion_v1_answer_total8_nonce5a_ignored", "_encode_completion_frame",
                       frame_type=cls.COMPLETION_TYPE_ANSWER, pkt_id=PKT_ID, frag_total=8, complete=False,
                       held=[0, 3, 7], version=cls.COMPLETION_PROTOCOL_VERSION_V1, nonce=0x5A))
    cases.append(_case("completion_v2_answer_total8_nonce5a_ignored", "_encode_completion_frame",
                       frame_type=cls.COMPLETION_TYPE_ANSWER, pkt_id=PKT_ID, frag_total=8, complete=False,
                       held=[0, 3, 7], version=cls.COMPLETION_PROTOCOL_VERSION_V2, nonce=0x5A))
    cases.append(_case("completion_default_answer_total8_held_none_complete1_nonce00", "_encode_completion_frame",
                       frame_type=cls.COMPLETION_TYPE_ANSWER, pkt_id=PKT_ID, frag_total=8, complete=True,
                       held=[], version=None, nonce=0))
    cases.append(_case("completion_default_query_pktid_ffff_nonce_ef", "_encode_completion_frame",
                       frame_type=cls.COMPLETION_TYPE_QUERY, pkt_id=0xFFFF, frag_total=2, complete=False,
                       held=[], version=None, nonce=cls.COMPLETION_QUERY_NONCE_MAX))

    # -- raw parity fragments (phase 3 M4, 2026-09-20) -----------------------
    # `fragments` = [[frag_idx, payload_hex]]: the XOR over the covered
    # fragments padded to the longest, prefixed by the last covered one's length.
    parity_sets = {
        "three_of_three": [[0, bytes(range(0, 40)).hex()], [1, bytes(range(40, 80)).hex()], [2, bytes(range(80, 100)).hex()]],
        "two_redrive": [[0, bytes(range(0, 40)).hex()], [2, bytes(range(80, 95)).hex()]],
        "eight": [[k, bytes([k] * (30 if k < 7 else 7)).hex()] for k in range(8)],
    }
    for set_name, frags in parity_sets.items():
        for attempt in (0, 3):
            for report in (False, True):
                cases.append(_case(f"raw_parity_{set_name}_attempt{attempt}_report{int(report)}", "_encode_raw_parity",
                                   fragments=frags, dst_pubkey_hex=DST_PUBKEY_HEX, src_prefix_hex=OWN_PUBKEY_HEX[:12],
                                   pkt_id=PKT_ID, frag_total=8 if set_name == "eight" else 3, attempt=attempt, report=report))

    # -- raw binary DIRECT fragments -----------------------------------------
    for idx, total in ((0, 1), (3, 4), (254, 255)):
        for attempt in range(4):
            for report in (False, True):
                cases.append(_case(
                    f"raw_idx{idx}_of{total}_attempt{attempt}_report{int(report)}_p157", "_encode_raw_fragment",
                    payload_hex=_payload(157), dst_pubkey_hex=DST_PUBKEY_HEX, src_prefix_hex=OWN_PUBKEY_HEX[:12],
                    pkt_id=PKT_ID, frag_idx=idx, frag_total=total, attempt=attempt, report=report))
    cases.append(_case("raw_idx0_of1_attempt0_report0_empty", "_encode_raw_fragment",
                       payload_hex=_payload(0), dst_pubkey_hex=DST_PUBKEY_HEX, src_prefix_hex=OWN_PUBKEY_HEX[:12],
                       pkt_id=PKT_ID, frag_idx=0, frag_total=1, attempt=0, report=False))
    cases.append(_case("raw_idx1_of2_attempt1_report1_pktid_ffff_p10", "_encode_raw_fragment",
                       payload_hex=_payload(10), dst_pubkey_hex=DST_PUBKEY_HEX, src_prefix_hex=OWN_PUBKEY_HEX[:12],
                       pkt_id=0xFFFF, frag_idx=1, frag_total=2, attempt=1, report=True))

    # -- payload budgets (defaults, node name "afipc") -------------------------
    for call in BUDGETS[:-1]:
        cases.append(_case(f"budget{call[:-len('_payload_budget')]}", call))
    for path_len in range(5):
        cases.append(_case(f"budget_direct_raw_path{path_len}", "_direct_raw_payload_budget", path_len=path_len))

    return cases


# --------------------------------------------------------------------------
# Replay and decode-check
# --------------------------------------------------------------------------

def _invoke(iface, call, a):
    if call == "_encode_channel_fastpath":
        return iface._encode_channel_fastpath(bytes.fromhex(a["payload_hex"]), a["pkt_id"], a["attempt"])
    if call == "_encode_channel_multifragment":
        return iface._encode_channel_multifragment(
            bytes.fromhex(a["payload_hex"]), a["pkt_id"], a["frag_idx"], a["frag_total"], a["attempt"])
    if call == "_encode_direct_bare":
        return iface._encode_direct_bare(bytes.fromhex(a["payload_hex"]))
    if call == "_encode_bind_frame":
        return iface._encode_bind_frame(a["frame_type"], a["attempt"])
    if call == "_encode_completion_frame":
        return iface._encode_completion_frame(
            a["frame_type"], a["pkt_id"], a["frag_total"], complete=a["complete"],
            held=set(a["held"]), version=a["version"], nonce=a["nonce"])
    if call == "_encode_completion_frame_v4":
        return iface._encode_completion_frame_v4(
            a["frame_type"], [(p, t, c, set(h)) for p, t, c, h in a["entries"]], nonce=a["nonce"])
    if call == "_encode_raw_parity":
        return iface._encode_raw_parity(
            [(i, bytes.fromhex(h)) for i, h in a["fragments"]], a["dst_pubkey_hex"], a["src_prefix_hex"],
            a["pkt_id"], a["frag_total"], a["attempt"], report=a["report"])
    if call == "_encode_raw_fragment":
        return iface._encode_raw_fragment(
            bytes.fromhex(a["payload_hex"]), a["dst_pubkey_hex"], a["src_prefix_hex"],
            a["pkt_id"], a["frag_idx"], a["frag_total"], a["attempt"], report=a["report"])
    if call == "_direct_raw_payload_budget":
        return iface._direct_raw_payload_budget(a["path_len"])
    if call in BUDGETS:
        return getattr(iface, call)()
    raise KeyError(f"unknown call {call}")


def _to_json(value):
    if isinstance(value, bytes):
        return "hex", value.hex()
    if isinstance(value, str):
        return "str", value
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"unexpected encoder result type {type(value).__name__}")
    return "int", value


def encode_case(module, case):
    iface = _bare(module, case.get("attrs"))
    kind, encoded = _to_json(_invoke(iface, case["call"], case["args"]))
    return kind, encoded


def check_decodes(module, case, kind, encoded):
    """The build's own decoder accepts the encoding and yields the inputs.
    Raises AssertionError (with the reason) otherwise."""
    iface = _bare(module, case.get("attrs"))
    cls = module.SmartMeshCoreInterface
    call, a = case["call"], case["args"]

    def eq(what, got, want):
        if got != want:
            raise AssertionError(f"decoded {what}={got!r}, expected {want!r}")

    if call in ("_encode_channel_fastpath", "_encode_channel_multifragment", "_encode_direct_bare"):
        modes = {"_encode_channel_fastpath": ("channel",), "_encode_direct_bare": ("direct",),
                 "_encode_channel_multifragment": ("channel", "direct")}[call]
        for mode in modes:
            header, payload = iface._decode_frame(encoded, mode)
            eq("payload", payload, bytes.fromhex(a["payload_hex"]))
            eq("version", header.version, cls.PROTOCOL_VERSION)
            eq("coop", header.coop, False)
            if call == "_encode_channel_fastpath":
                eq("multi_fragment", header.multi_fragment, False)
                eq("pkt_id", header.pkt_id, a["pkt_id"])
                eq("attempt", header.attempt, a["attempt"])
                eq("frag", (header.frag_idx, header.frag_total), (0, 1))
            elif call == "_encode_channel_multifragment":
                eq("multi_fragment", header.multi_fragment, True)
                eq("pkt_id", header.pkt_id, a["pkt_id"])
                eq("frag", (header.frag_idx, header.frag_total), (a["frag_idx"], a["frag_total"]))
                eq("attempt", header.attempt, a["attempt"])
            else:
                eq("multi_fragment", header.multi_fragment, False)
                eq("pkt_id", header.pkt_id, None)
                eq("attempt", header.attempt, None)
    elif call == "_encode_bind_frame":
        bf = iface._decode_bind_frame(encoded)
        eq("version", bf.version, cls.BIND_PROTOCOL_VERSION)
        eq("type", bf.type, a["frame_type"])
        eq("attempt", bf.attempt, a["attempt"] & 0xFF)
        want_cap = ((cls.BIND_CAP_HAS_UPSTREAM_RNS if iface.declares_upstream_rns else 0)
                    | (cls.BIND_CAP_RAW_FRAGMENTS if iface.direct_raw_fragments_enabled else 0))
        eq("cap", bf.cap, want_cap)
        own = iface._own_pubkey_hex
        want_prefix = own[:cls.BIND_PUBKEY_PREFIX_BYTES * 2] if len(own) >= cls.BIND_PUBKEY_PREFIX_BYTES * 2 \
            else "00" * cls.BIND_PUBKEY_PREFIX_BYTES
        eq("pubkey_prefix", bf.pubkey_prefix, want_prefix)
    elif call == "_encode_completion_frame":
        cf = iface._decode_completion_frame(encoded)
        version = a["version"] if a["version"] is not None else cls.COMPLETION_PROTOCOL_VERSION
        eq("version", cf.version, version)
        eq("type", cf.type, a["frame_type"])
        eq("complete", cf.complete, a["complete"])
        eq("pkt_id", cf.pkt_id, a["pkt_id"])
        eq("frag_total", cf.frag_total, a["frag_total"] & 0xFF)
        if version >= 2 and a["frame_type"] == cls.COMPLETION_TYPE_ANSWER:
            eq("held", set(cf.held), {i for i in a["held"] if 0 <= i < a["frag_total"]})
        else:
            eq("held", cf.held, None)
        eq("nonce", cf.nonce, (a["nonce"] & 0xFF) if version >= 3 else None)
    elif call == "_encode_completion_frame_v4":
        cf = iface._decode_completion_frame(encoded)
        eq("version", cf.version, cls.COMPLETION_PROTOCOL_VERSION)
        eq("type", cf.type, a["frame_type"])
        eq("nonce", cf.nonce, a["nonce"] & 0xFF)
        eq("n", len(cf.entries), len(a["entries"]))
        is_answer = a["frame_type"] == cls.COMPLETION_TYPE_ANSWER
        for got, want in zip(cf.entries, a["entries"]):
            p, t, c, h = want
            eq("entry pkt_id", got[0], p)
            eq("entry frag_total", got[1], t & 0xFF)
            eq("entry complete", got[2], c)
            eq("entry held", (set(got[3]) if got[3] is not None else None),
               ({i for i in h if 0 <= i < t} if is_answer else None))
        eq("first mirrored", (cf.pkt_id, cf.frag_total, cf.complete), (a["entries"][0][0], a["entries"][0][1] & 0xFF, a["entries"][0][2]))
    elif call == "_encode_raw_parity":
        raw = bytes.fromhex(encoded)
        header, payload, src_prefix_hex, dst = iface._decode_raw_fragment(raw)
        frags = [(i, bytes.fromhex(h)) for i, h in a["fragments"]]
        eq("parity flag", iface._raw_fragment_is_parity(raw), True)
        eq("report", iface._raw_fragment_report_requested(raw), a["report"])
        eq("mask", header.frag_idx, sum(1 << i for i, _p in frags))
        eq("frag_total", header.frag_total, a["frag_total"])
        eq("attempt", header.attempt, a["attempt"] & 0x03)
        top = max(i for i, _p in frags)
        eq("last_len", payload[0], len(dict(frags)[top]))
        width = max(len(p) for _i, p in frags)
        eq("width", len(payload) - 1, width)
        acc = bytearray(width)
        for _i, p in frags:
            for k, b in enumerate(p):
                acc[k] ^= b
        eq("xor", payload[1:], bytes(acc))
    elif call == "_encode_raw_fragment":
        raw = bytes.fromhex(encoded)
        header, payload, src_prefix_hex, dst = iface._decode_raw_fragment(raw)
        eq("payload", payload, bytes.fromhex(a["payload_hex"]))
        # The source prefix on the wire is RAW_SRC_PREFIX_BYTES (2 since M3,
        # 2026-09-20; the v1 header carried the full 6-byte prefix).
        src_bytes = getattr(cls, "RAW_SRC_PREFIX_BYTES", cls.BIND_PUBKEY_PREFIX_BYTES)
        eq("src_prefix", src_prefix_hex, a["src_prefix_hex"][:src_bytes * 2])
        eq("dst_prefix", dst, bytes.fromhex(a["dst_pubkey_hex"][:cls.RAW_DST_PREFIX_BYTES * 2]))
        eq("pkt_id", header.pkt_id, a["pkt_id"])
        eq("frag", (header.frag_idx, header.frag_total), (a["frag_idx"], a["frag_total"]))
        eq("attempt", header.attempt, a["attempt"] & 0x03)
        eq("multi_fragment", header.multi_fragment, True)
        eq("report", iface._raw_fragment_report_requested(raw), a["report"])
        eq("header size", len(raw) - len(payload), cls.RAW_HEADER_SIZE)
    elif call in BUDGETS:
        if kind != "int" or encoded < 0:
            raise AssertionError(f"budget {encoded!r} is not a non-negative int")
    else:
        raise AssertionError(f"no decode check for {call}")


def wire_constants(module):
    """Every ALL_CAPS attribute defined on the class itself (not inherited
    from RNS's Interface) whose value is an int, str or bytes."""
    cls = module.SmartMeshCoreInterface
    out = {}
    for name, value in vars(cls).items():
        if not (name.isupper() and name[0].isalpha()):
            continue
        if isinstance(value, bool) or not isinstance(value, (int, str, bytes)):
            continue
        out[name] = {"hex": value.hex()} if isinstance(value, bytes) else value
    return out


def snapshot(module):
    cases = []
    for case in build_cases(module):
        kind, encoded = encode_case(module, case)
        check_decodes(module, case, kind, encoded)
        full = dict(case)
        full["kind"] = kind
        full["encoded"] = encoded
        cases.append(full)
    return {"constants": wire_constants(module), "cases": cases}


def _render(data):
    return json.dumps(data, indent=2, sort_keys=True) + "\n"


def _load_golden(path=GOLDEN_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def regenerate(build_path=FROZEN_BUILD, golden_path=GOLDEN_PATH):
    if not os.path.exists(build_path):
        raise SystemExit(f"build not found: {build_path}")
    module = load_interface_module(module_name="smci_golden_source", path=build_path)
    data = snapshot(module)
    text = _render(data)
    os.makedirs(os.path.dirname(golden_path), exist_ok=True)
    with open(golden_path, "w", encoding="utf-8") as f:
        f.write(text)
    return data


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------

HOW_TO_FIX = (
    "\nEither restore the bytes in the interface or, for a deliberate wire-format change, regenerate "
    "the snapshot in the same commit (`python3 tests/test_golden_wire_format.py --regenerate "
    "Interface/SmartMeshCoreInterface.py`) and say so in the commit message."
)


class GoldenWireFormat(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_interface_module()
        cls.golden = _load_golden()
        cls.golden_cases = {c["name"]: c for c in cls.golden["cases"]}

    def _replay(self, call_prefix):
        """Replays every golden case whose `call` starts with `call_prefix`
        on the build under test and fails once, naming every case whose
        bytes differ."""
        cases = [c for c in self.golden["cases"] if c["call"].startswith(call_prefix)]
        self.assertTrue(cases, f"no golden cases for {call_prefix}; regenerate {GOLDEN_PATH}")
        problems = []
        for case in cases:
            try:
                kind, encoded = encode_case(self.module, case)
            except Exception as exc:  # noqa: BLE001 -- report, do not stop at the first
                problems.append(f"  {case['name']}: {case['call']} raised {type(exc).__name__}: {exc}")
                continue
            if kind != case["kind"] or encoded != case["encoded"]:
                problems.append(
                    f"  {case['name']}: {case['call']} now gives {kind} {encoded!r}, golden {case['kind']} {case['encoded']!r}"
                )
        if problems:
            self.fail(
                f"{len(problems)} of {len(cases)} {call_prefix} cases differ from the golden snapshot "
                f"({os.path.relpath(GOLDEN_PATH, REPO_ROOT)}):\n" + "\n".join(problems) + HOW_TO_FIX
            )
        return len(cases)

    def test_channel_fastpath_frames(self):
        self._replay("_encode_channel_fastpath")

    def test_multifragment_frames(self):
        self._replay("_encode_channel_multifragment")

    def test_direct_bare_frames(self):
        self._replay("_encode_direct_bare")

    def test_bind_frames(self):
        self._replay("_encode_bind_frame")

    def test_completion_frames(self):
        self._replay("_encode_completion_frame")

    def test_raw_fragments(self):
        self._replay("_encode_raw_fragment")

    def test_payload_budgets(self):
        for call in BUDGETS:
            self._replay(call)

    def test_wire_constants(self):
        actual = wire_constants(self.module)
        expected = self.golden["constants"]
        problems = []
        for name in sorted(set(actual) | set(expected)):
            if name not in actual:
                problems.append(f"  {name}: no longer defined (golden {expected[name]!r})")
            elif name not in expected:
                # A constant that did not exist when the snapshot was taken
                # cannot have changed any golden byte; not a failure (the
                # snapshot picks it up at the next --regenerate). A NEW
                # encoder is caught by test_every_encoder_and_budget_is_covered.
                continue
            elif actual[name] != expected[name] or type(actual[name]) is not type(expected[name]):
                problems.append(f"  {name}: now {actual[name]!r}, golden {expected[name]!r}")
        if problems:
            self.fail("class wire constants differ from the golden snapshot:\n"
                      + "\n".join(problems) + HOW_TO_FIX)

    def test_golden_frames_decode_under_build_under_test(self):
        """Every golden encoding is accepted by the build under test's
        decoders and yields the case's inputs -- the decoder half of the
        format. (The same check ran against the source build when the
        snapshot was generated, so the snapshot is known-valid.)"""
        problems = []
        for case in self.golden["cases"]:
            try:
                check_decodes(self.module, case, case["kind"], case["encoded"])
            except Exception as exc:  # noqa: BLE001
                problems.append(f"  {case['name']}: {type(exc).__name__}: {exc}")
        if problems:
            self.fail(f"{len(problems)} golden frames no longer decode on the build under test:\n"
                      + "\n".join(problems) + HOW_TO_FIX)

    def test_case_list_matches_golden(self):
        """The generated case list (names, calls, inputs) is exactly the
        golden file's, so a case added or renamed here without regenerating,
        or a golden file from another version of this test, is caught."""
        built = [{k: v for k, v in c.items()} for c in build_cases(self.module)]
        on_disk = [{k: v for k, v in c.items() if k not in ("kind", "encoded")} for c in self.golden["cases"]]
        self.assertEqual([c["name"] for c in built], [c["name"] for c in on_disk])
        self.assertEqual(built, on_disk)

    def test_every_encoder_and_budget_is_covered(self):
        """A new `_encode_*` or `*_payload_budget` method on the class must
        get golden cases."""
        cls = self.module.SmartMeshCoreInterface
        # dir() rather than vars(): since the 2026-09-20 module split the
        # encoders live on a mixin, and a new one anywhere in the MRO counts.
        encoders = sorted(n for n in dir(cls) if n.startswith("_encode_") and callable(getattr(cls, n)))
        budgets = sorted(n for n in dir(cls) if n.endswith("_payload_budget") and callable(getattr(cls, n))
                         and n != "_payload_budget")
        self.assertEqual(encoders, sorted(ENCODERS))
        self.assertEqual(budgets, sorted(BUDGETS))
        covered = {c["call"] for c in self.golden["cases"]}
        self.assertEqual(covered, set(ENCODERS) | set(BUDGETS))

    def test_golden_file_is_canonical(self):
        """The file on disk is exactly what --regenerate writes for its own
        content, so a regenerate that changes nothing produces no diff."""
        with open(GOLDEN_PATH, "r", encoding="utf-8") as f:
            on_disk = f.read()
        self.assertEqual(on_disk, _render(self.golden))


if __name__ == "__main__":
    if "--regenerate" in sys.argv:
        i = sys.argv.index("--regenerate")
        build = sys.argv[i + 1] if len(sys.argv) > i + 1 and not sys.argv[i + 1].startswith("-") else FROZEN_BUILD
        data = regenerate(build)
        print(f"wrote {GOLDEN_PATH}: {len(data['cases'])} cases, {len(data['constants'])} constants, from {build}")
    else:
        unittest.main()
