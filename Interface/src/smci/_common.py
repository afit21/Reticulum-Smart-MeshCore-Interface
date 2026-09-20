"""Shared helpers and record types: config parsing, the Z85 codec, the
frame / header / peer records. First module of the assembled file; its
import block is the deliverable's import block."""

import asyncio
import collections
import itertools
import json
import os
import queue
import random
import threading
import time
import traceback
from typing import NamedTuple, Optional

import RNS
from RNS.Interfaces.Interface import Interface


# The priority tiers as module-level names (2026-09-20, module split): the
# class constants SmartMeshCoreInterface.PRIORITY_* (HANDSHAKE 0 / ANSWER 1
# / NORMAL 2 / LOW 3, renumbered 2026-09-19) are what the code reads via
# `self.`; these mirror them so a mixin method can use one as a default
# argument value (a class body's names are not visible in another class
# body). tests/test_module_split_0920.py pins the two sets equal.
PRIORITY_HANDSHAKE = 0
PRIORITY_ANSWER = 1
PRIORITY_NORMAL = 2
PRIORITY_LOW = 3

_CFG_FALSY = ("no", "false", "0", "off", "n", "f", "none", "disabled", "disable")
_CFG_TRUTHY = ("yes", "true", "1", "on", "y", "t", "enabled", "enable")


def _cfg_bool(value) -> bool:
    """Parse a ConfigObj string value as a boolean.

    Audit fix (2026-09-19): the falsy set used to be only
    ("no", "false", "0"), and the docstring claimed an unrecognized value
    "fails safe toward the (documented) default behavior". It did not -- it
    failed safe toward **True**, which is the opposite of the default for
    every flag that defaults off. Verified through RNS's own vendored
    ConfigObj: it hands values to interfaces as raw strings without boolean
    coercion, so `rx_log_holds_enabled = off` reached here as "off" and
    turned the feature ON, as did `packet_capture_enabled = off`,
    `declares_upstream_rns = disabled`, and so on -- silently, with nothing
    logged. `off`/`n`/`disabled`/`none` are now falsy, the common truthy
    spellings are explicit, and anything unrecognized is still treated as
    True (the historical behaviour) but logged once so a typo is visible
    instead of silent."""
    text = str(value).strip().lower()
    if text in _CFG_FALSY:
        return False
    if text in _CFG_TRUTHY:
        return True
    RNS.log(
        f"SmartMeshCoreInterface: unrecognized boolean config value {value!r} -- "
        f"treating it as enabled. Use yes/no.",
        RNS.LOG_WARNING,
    )
    return True


# -------------------------------------------------------------------------
# Z85 codec (docs/wire_format_design.md's "constraint zero": send_chan_msg/
# send_msg take str and encode utf-8; the receiving side decodes with
# errors="ignore", which silently drops invalid UTF-8 sequences rather than
# erroring. Raw binary must be encoded into a restricted, ASCII-safe
# alphabet before it touches either call, or bytes get silently corrupted
# on the wire with no error anywhere. Z85 was already the right choice
# (25% expansion, close to the theoretical floor) -- kept, not reinvented.
# Self-describing padding: the first output character is a digit 0-3
# giving how many zero bytes were appended before encoding, so decode
# doesn't need the original length out-of-band.
# -------------------------------------------------------------------------

_Z85_ALPHABET = (
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
)
_Z85_DECODE = {c: i for i, c in enumerate(_Z85_ALPHABET)}



def _z85_encode(data: bytes) -> str:
    pad = (-len(data)) % 4
    padded = data + b"\x00" * pad

    out = []
    for i in range(0, len(padded), 4):
        value = int.from_bytes(padded[i:i + 4], "big")
        chars = []
        for _ in range(5):
            chars.append(_Z85_ALPHABET[value % 85])
            value //= 85
        out.append("".join(reversed(chars)))

    return str(pad) + "".join(out)


def _z85_decode(text: str) -> bytes:
    if not isinstance(text,str) or not text or text[0] not in "0123":
        raise ValueError("missing/invalid Z85 pad-count prefix")
    pad = int(text[0])
    body = text[1:]

    if len(body) % 5 != 0:
        raise ValueError(f"Z85 body length {len(body)} not a multiple of 5")

    out = bytearray()
    for i in range(0, len(body), 5):
        group = body[i:i + 5]
        value = 0
        for ch in group:
            try:
                value = value * 85 + _Z85_DECODE[ch]
            except KeyError:
                raise ValueError(f"invalid Z85 character: {ch!r}") from None
        if value > 0xFFFFFFFF:
            raise ValueError("Z85 group overflows 32 bits")
        out.extend(value.to_bytes(4, "big"))

    if pad:
        out = out[:-pad]
    return bytes(out)


class _FrameHeader(NamedTuple):
    """The decoded `[ver]`(`[|pkt_id|frag_idx|frag_total|attempt]`) header
    of one MeshCore CHANNEL/DIRECT frame, per docs/wire_format_design.md.
    `pkt_id`/`attempt` are None for a DIRECT bare (fits-in-one-message)
    frame, which carries neither -- the firmware's own ACK and content-
    derived attempt differentiation are that shape's reliability
    mechanism, not this header's."""

    version: int
    multi_fragment: bool
    coop: bool
    pkt_id: Optional[int]
    frag_idx: int
    frag_total: int
    attempt: Optional[int]


class _ReassemblyBucket:
    """One in-progress CHANNEL multi-fragment reassembly, keyed as
    docs/reliability_engine_design.md §5.2 specifies (see
    SmartMeshCoreInterface._reassembly_key). `last_progress` is a
    `time.monotonic()` timestamp, refreshed on every accepted fragment --
    §5.4's "time since last progress, not time since first fragment"
    staleness metric, and also the sort key §5.3's oldest-by-last-progress
    capacity eviction uses."""

    __slots__ = ("frag_total", "coop", "fragments", "last_progress")

    def __init__(self, frag_total: int, coop: bool):
        self.frag_total = frag_total
        self.coop = coop
        self.fragments: dict = {}
        self.last_progress = time.monotonic()


class _RnsHeader(NamedTuple):
    """The cleartext RNS packet-header fields this interface reads to
    make priority/retry decisions and (Milestone 5) routing decisions --
    packet_type, destination_type, the context byte (when present), the
    header_type bit, and the destination_hash field. All sit in the
    clear before RNS's own payload encryption begins (confirmed directly
    against the installed `RNS.Packet.unpack()`, not the old interface's
    own docstring paraphrase of this layout, which grouped the flags
    byte's bits slightly differently even though its two derived masks
    happened to still be correct) -- this interface decrypts nothing,
    per its security model. `context`/`destination_hash` are `None` when
    `data` is too short to contain them. `header_type`/`destination_hash`
    default to `0`/`None` so existing keyword-only test construction of
    this NamedTuple (pre-Milestone-5) keeps working unchanged."""

    packet_type: int
    destination_type: int
    context: Optional[int]
    header_type: int = 0
    destination_hash: Optional[bytes] = None


class _ResolvedPath(NamedTuple):
    """This interface's own last-verified path-discovery result for one
    peer, per docs/path_discovery_spec.md's function-level spec --
    authoritative for this interface's own routing/staleness decisions
    regardless of whether the device-table persist (_persist_resolved_path)
    itself succeeds, so a persist failure can never discard a path just
    proven to work. `out_path_hex`/`out_path_len` are empty/0 for a
    confirmed zero-hop (direct) peer -- still a genuine, worth-persisting
    result, not an absence of one."""

    out_path_hex: str
    out_path_len: int
    out_path_hash_len: int
    resolved_at: float  # time.monotonic()


class _BindFrame(NamedTuple):
    """The decoded `"P"`-marker peer-binding control frame
    (docs/peer_discovery_design.md §1) -- distinct from `_FrameHeader`,
    which decodes the `"R"`-marker RNS-carrying frame. `pubkey_prefix` is
    a 12-hex-char (6-byte) string, this interface's own canonical peer
    key throughout Milestone 5's peer registry/routing tables."""

    version: int
    type: int
    cap: int
    attempt: int
    pubkey_prefix: str


class _CompletionFrame(NamedTuple):
    """The decoded `"Q"`-marker DIRECT-delivery-completion-check control
    frame (see `_encode_completion_frame`'s own docstring for why this
    exists) -- distinct from both `_FrameHeader` (the `"R"`-marker RNS
    frame) and `_BindFrame` (the `"P"`-marker peer-binding frame).
    `complete` is meaningful only on an ANSWER frame (always `False` on a
    QUERY, which is asking the question rather than answering it)."""

    version: int
    type: int
    complete: bool
    pkt_id: int
    frag_total: int
    # Step 3 (2026-09-18): the receiver's have-bitmap on a v2 ANSWER --
    # the set of frag_idx it currently holds (complete or not). None on
    # any QUERY and on a v1 ANSWER, which only ever carried `complete`.
    held: "Optional[frozenset]" = None
    # v3 only (2026-09-19): the querier's nonce, echoed by the answerer.
    # None on a v1/v2 frame, meaning "cannot be verified" rather than
    # "mismatched".
    nonce: Optional[int] = None


class _PeerRecord:
    """One entry in this interface's own peer registry
    (docs/peer_discovery_design.md §4) -- deliberately minimal: just
    enough for the routing decisions this interface itself makes.
    MeshCore's own contact table (pubkey/name/out_path) is a separate,
    independently-owned data source this interface only ever reads
    (`_resolve_contact`), never duplicates here. `has_upstream_rns` is
    tri-state (`None` = no signal yet) -- §2's hard rule: only ever set
    from an actually-parsed bind frame, never inferred or defaulted.
    `last_seen` is a `time.time()` (wall-clock, not monotonic) timestamp
    specifically so it survives a restart meaningfully via the peer
    cache -- monotonic time resets to 0 on every process start and would
    make every cache-restored peer look artificially fresh."""

    __slots__ = ("pubkey_prefix", "has_upstream_rns", "last_seen", "raw_fragments")

    def __init__(self, pubkey_prefix: str, has_upstream_rns: Optional[bool], last_seen: float,
                 raw_fragments: Optional[bool] = None):
        self.pubkey_prefix = pubkey_prefix
        self.has_upstream_rns = has_upstream_rns
        self.last_seen = last_seen
        # Tri-state like has_upstream_rns: only ever set from a parsed bind
        # frame's BIND_CAP_RAW_FRAGMENTS bit (2026-09-18 night).
        self.raw_fragments = raw_fragments
