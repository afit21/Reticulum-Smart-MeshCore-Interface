"""
SmartMeshCoreInterface.py -- Smart Meshcore Interface for Reticulum

An `RNS.Interfaces.Interface` subclass that carries Reticulum (RNS) traffic
over a MeshCore LoRa mesh. This is a from-scratch rebuild -- see CLAUDE.md
and `docs/history.md` (the dated design record, read that first) for why
the previous implementation
(`referenceprojects/MeshCore_Dynamic_Interface_old.py` /
`_original_repo.py`) was retired rather than extended: it accumulated real,
working functionality on top of one wrong assumption about MeshCore
(reliable multi-packet CHANNEL delivery) that field testing showed cannot be
tuned into working. Nothing in this file is built on that code; it is a
fresh implementation against the design docs, referring back to the old
implementation only as a record of what was tried and why it didn't work.

STATUS -- alpha 0.1.3 plus the 2026-09-20 airtime / throughput pass; the
dated account of every design decision and field-driven fix from alpha
0.1.0 (2026-09-15) onward is `docs/history.md` (moved out of this
docstring on 2026-09-20, phase 2 of that pass, unchanged), with the
summary in `changelog.md`. The deliverable `Interface/SmartMeshCoreInterface.py`
is ASSEMBLED from the package `Interface/src/smci/` by
`Interface/build_interface.py` (RNS `exec()`s a custom interface as one
file, so a package cannot be installed as such -- see
`testscripts/check_install_load.py`); edit the sources, run the build,
commit both. `testscripts/audit_split.py` checks a rebuild is a pure move.

WIRE FORMAT (the golden snapshot `tests/golden/wire_format.json`, replayed
by `tests/test_golden_wire_format.py`, pins every byte below; a deliberate
change regenerates it in the same commit):

  Text frames ride MeshCore text messages -- `send_msg` (TXT_MSG) for
  DIRECT, `send_chan_msg` (GRP_TXT) for CHANNEL -- as one ASCII marker
  character followed by the Z85 encoding of a binary body. Payload
  budgets per frame come from FIRMWARE_TEXT_LIMIT (160) less the marker,
  Z85's 5/4 expansion, the header and PAYLOAD_MARGIN (4); CHANNEL also
  loses the firmware's own "<name>: " prefix.

  "R" (MARKER) -- an RNS packet, or one fragment of it. Byte 0 is
  PROTOCOL_VERSION (1) in the low six bits (VERSION_MASK 0x3F) with
  FLAG_MULTI_FRAGMENT (0x80) and FLAG_COOP (0x40, reserved):
    CHANNEL single frame  [ver][pkt_id:2 BE][attempt]            (4 B)
    DIRECT single frame   [ver]                                  (1 B)
    multi-fragment, both  [ver|0x80][pkt_id:2 BE][frag_idx][frag_total][attempt]  (6 B)
  followed by the RNS payload bytes. frag_total is 1..255; every attempt
  of a fragment carries the same payload bytes (the receiver evicts a
  bucket whose repeated index differs). Reassembly is keyed
  (mode, sender token, pkt_id, frag_total).

  "P" (PEER_MARKER) -- a bind frame, peer discovery over CHANNEL:
    [BIND_PROTOCOL_VERSION 1][type: 0 REQUEST / 1 RESPONSE][cap][attempt][pubkey_prefix:6]
  (BIND_FRAME_RAW_SIZE 10). cap bits: BIND_CAP_HAS_UPSTREAM_RNS 0x01,
  BIND_CAP_RAW_FRAGMENTS 0x02.

  "Q" (COMPLETION_MARKER) -- the DIRECT-only completion QUERY / ANSWER /
  REPORT (the have-bitmap reconcile):
    [version][type: 0 QUERY / 1 ANSWER][complete: 0/1][pkt_id:2 BE][frag_total]
    v3 adds  [nonce]                                        (after frag_total)
    v2+ ANSWER adds the have-bitmap, ceil(frag_total / 8) bytes, bit i = fragment i held
  COMPLETION_PROTOCOL_VERSION is 3; v1 and v2 frames still decode and a
  v1 QUERY is answered in v1. A QUERY's nonce cycles 1..0xEF
  (COMPLETION_QUERY_NONCE_MAX) and its ANSWER echoes it; a receiver-
  initiated REPORT is an ANSWER with nonce 0xF0 | round
  (COMPLETION_REPORT_NONCE_BASE), round being the raw header's attempt
  bits. A pre-v3 peer drops a v3 QUERY, so both nodes must run a v3
  build for reconciliation to work.

  Raw binary DIRECT fragments -- `send_raw_data` (PAYLOAD_TYPE_RAW_CUSTOM,
  no text framing, no firmware encryption, no firmware ACK), RAW_HEADER_SIZE
  13 bytes then the RNS payload chunk:
    [RAW_PROTOCOL_VERSION 1 << 4 | RAW_FLAG_REPORT 0x04 | attempt & 0x03]
    [dst_pubkey_prefix:2][src_pubkey_prefix:6][pkt_id:2 BE][frag_idx][frag_total]
  RAW_FLAG_REPORT marks the last two fragments of a burst (the receiver
  reports when one lands). Per-fragment payload is
  min(direct_raw_payload_cap, FIRMWARE_RAW_RX_PAYLOAD_LIMIT 172,
  FIRMWARE_RAW_TX_FRAME_LIMIT 174 - path_len) - 13: 157 bytes at the
  shipped cap of 170. The firmware dedups raw packets by content, so no
  two transmissions of a fragment may be byte-identical -- the attempt
  bits change per round (at most 4 rounds). Raw fragments land in the
  same reassembly bucket as text fragments from that sender and are
  reconciled by the same "Q" frames.

DESIGN INVARIANTS (carried forward from the prior implementation's own
field-diagnosed lessons, restated here per CLAUDE.md; the full justification
for each lives in `docs/meshcore_protocol_rules.md`'s "meshcore Python
library's own send/ACK contract" section and
`docs/reliability_engine_design.md`'s "RNS base-class contract" implementation
notes):

  1. Never trust a bare `await` of a `self._mc.commands.foo(...)` call to
     mean success. `send_chan_msg` resolves against [OK, ERROR] and never
     returns a delivery confirmation (CHANNEL has no ACK at all); `send_msg`
     resolves against [MSG_SENT, ERROR] and MSG_SENT itself only confirms
     the local radio queued the frame, not that it was delivered. Route
     every command's result through `_run_command()`, which checks the
     *actual* event type returned against what that specific call expected,
     and raises on `EventType.ERROR`, no response, or an unexpected type --
     never on "did it return without throwing."

  2. Never wait on a bare event TYPE when more than one in-flight operation
     could produce it. The `meshcore` library's `CommandHandlerBase.send()`
     matches a reply purely by event type, with no per-request correlation
     id -- two concurrent commands both waiting on the same type can steal
     each other's reply. `_run_command()` serializes every actual
     `commands.*()` call behind `self._command_lock` so only one command is
     ever in flight and awaiting its reply at a time. A longer-lived wait
     for something that can legitimately take tens of seconds (a DIRECT
     delivery ACK, once M4+ adds it) must be a persistent subscription
     matched by content identity, never a second concurrent bare-type wait
     that could collide with the next command's own reply.

  3. Every config value that's read via `cfg.get(...)` in a `_configure_*`
     method must be used somewhere else in this file, or a user setting it
     gets no error and no effect -- the exact bug class
     (`firmware_text_limit` parsed, documented, and never read) this
     project already hit once. No automated check enforces this: the AST
     scan this entry once cited (`tests/test_smart_meshcore_interface_
     config.py`) was never committed. Verify by hand when adding a key.
"""
# Assembled by Interface/build_interface.py from Interface/src/smci/ -- edit the
# sources and rebuild; the pre-commit hook refuses a stale deliverable.

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


# ---- _common.py ----

"""Shared helpers and record types: config parsing, the Z85 codec, the
frame / header / peer records. First module of the assembled file; its
import block is the deliverable's import block."""




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


# ---- _locks.py ----

"""The radio-lock primitives and the duty-cycle limiter."""

class _PreemptedForHandshake(Exception):
    """Raised out of an interruptible wait (the duty-cycle throttle) when a
    link handshake is queued for the radio lock the waiter holds (phase 1,
    2026-09-20). The holder yields (`_PriorityAsyncLock.yield_to_preempt`)
    and retries the step afterwards."""


class _PriorityAsyncLock:
    """User-requested architectural fix (2026-09-16, real 1-hop repeater
    field data): `_direct_exchange_lock` (see `_send_direct_frame_and_
    wait_for_ack`'s own docstring) used to be a plain `asyncio.Lock`,
    strictly FIFO across every DIRECT exchange regardless of what kind of
    packet it carried. A real 1-hop test found `_direct_exchange_queue_
    depth` reaching 14 -- meaning a LINK_REQUEST/PROOF-class exchange
    (already tagged `PRIORITY_HANDSHAKE` at the *outer* two-tier queue,
    docs/reliability_engine_design.md §3) arriving while a backlog of
    ordinary DATA/RESOURCE-fragment retries was already queued for this
    same lock had no way to jump that backlog -- it just joined the back
    of one shared FIFO like everything else, even though establishing (or
    proving) a Link is usually what everything else is waiting on in the
    first place. A plain `asyncio.Lock` cannot reorder waiters once
    they're queued (there is no "insert ahead" operation), so preserving
    that outer priority tier all the way down to the actual radio
    required a real priority-aware primitive, not just a config tweak.

    Behavior: a higher-priority waiter (lower `priority` integer, matching
    `PRIORITY_HANDSHAKE < PRIORITY_NORMAL`) is served before an earlier-
    arrived lower-priority one; waiters within the same tier stay FIFO
    among themselves, same as the stdlib lock. Used via `async with
    lock(priority):` (see `__call__`/`_PriorityLockContext` below).

    Deliberately minimal, not a wrapper around `asyncio.Lock` -- the
    stdlib lock's own internal waiter queue has no reordering operation
    to hook into, so ownership here is tracked directly (`_locked`) with
    one waiter deque per priority tier. Cancellation-safe: a waiter
    cancelled before being granted just removes itself from its deque; a
    waiter cancelled in the narrow window after being granted but before
    resuming passes ownership on to the next waiter rather than leaving
    the lock stuck locked forever with nothing able to ever acquire it
    again (only reachable via `detach()`'s `task.cancel()` sweep in
    practice -- this interface's own steady-state code never cancels a
    task waiting on this lock)."""

    # Phase 1 (2026-09-20): the tier a holder re-queues at when it yields
    # to a pre-empting waiter -- between HANDSHAKE (0) and ANSWER (1), so
    # the yielded exchange resumes ahead of everything but the handshakes
    # that pre-empted it (tiers are compared numerically; a float sorts).
    YIELDED_PRIORITY = 0.5

    def __init__(self):
        self._locked = False
        self._waiters: "dict[int, collections.deque]" = {}
        # Pre-emption (phase 1, 2026-09-20): futures of waiters that asked
        # to pre-empt an idle holder, and the event an idle holder watches.
        self._preempt_waiters: set = set()
        self._preempt_event: "Optional[asyncio.Event]" = None

    def locked(self) -> bool:
        return self._locked

    def preempt_requested(self) -> bool:
        """A waiter that may pre-empt idle holds is queued (2026-09-20)."""
        return bool(self._preempt_waiters)

    def preempt_event(self) -> "asyncio.Event":
        """The event set while a pre-empting waiter is queued; created on
        the running loop the first time it is asked for."""
        if self._preempt_event is None:
            self._preempt_event = asyncio.Event()
            if self._preempt_waiters:
                self._preempt_event.set()
        return self._preempt_event

    def _preempt_add(self, fut) -> None:
        self._preempt_waiters.add(fut)
        if self._preempt_event is not None:
            self._preempt_event.set()

    def _preempt_remove(self, fut) -> None:
        self._preempt_waiters.discard(fut)
        if not self._preempt_waiters and self._preempt_event is not None:
            self._preempt_event.clear()

    async def yield_to_preempt(self) -> None:
        """Called by a holder at an idle point when `preempt_requested()`:
        hands the lock over and re-acquires it at YIELDED_PRIORITY, so
        the pre-empting handshake goes first and this exchange resumes
        before any ordinary waiter that queued meanwhile (review,
        2026-09-20: a plain release + re-acquire at NORMAL would splice a
        whole other exchange into a raw burst)."""
        self.release()
        await self.acquire(self.YIELDED_PRIORITY)

    async def acquire(self, priority: int, preempt: bool = False) -> None:
        if not self._locked:
            self._locked = True
            return
        fut = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(priority, collections.deque()).append(fut)
        if preempt:
            self._preempt_add(fut)
        try:
            await fut
        except asyncio.CancelledError:
            dq = self._waiters.get(priority)
            if dq is not None and fut in dq:
                # Never granted -- just drop out of line, lock state
                # (owned by whoever currently holds it, if anyone) is
                # entirely unaffected by our own departure.
                dq.remove(fut)
            elif fut.done() and not fut.cancelled():
                # Already granted ownership in the same instant our own
                # cancellation was delivered -- pass it on rather than
                # leaving the lock permanently locked with no owner able
                # to release it. Code-review fix: this used to call
                # _wake_next() and ignore its return value, unlike
                # release()'s own `if not self._wake_next(): self._locked
                # = False`. When no other waiter existed (_wake_next()
                # returns False), that left `_locked` stuck True forever
                # with nobody holding it and nobody able to call release()
                # for it -- a silent, permanent deadlock of every future
                # acquire() on this lock, contradicting this class's own
                # cancellation-safety docstring above.
                if not self._wake_next():
                    self._locked = False
            raise
        finally:
            if preempt:
                self._preempt_remove(fut)

    def release(self) -> None:
        if not self._wake_next():
            self._locked = False

    def _wake_next(self) -> bool:
        """Hands ownership to the next waiter, highest priority (lowest
        integer) first, FIFO within a tier. Returns whether anyone was
        actually waiting -- the lock stays `_locked=True` (ownership
        transferred) when True, and the caller (`release`) marks it
        unlocked only when False."""
        for tier in sorted(self._waiters.keys()):
            dq = self._waiters[tier]
            while dq:
                fut = dq.popleft()
                if not fut.done():
                    fut.set_result(None)
                    return True
            del self._waiters[tier]
        return False

    def __call__(self, priority: int, preempt: bool = False) -> "_PriorityLockContext":
        return _PriorityLockContext(self, priority, preempt)


class _PriorityLockContext:
    """`async with priority_lock(priority):` sugar -- `_PriorityAsyncLock`
    itself isn't a context manager (it needs a `priority` argument
    `asyncio.Lock`'s own `__aenter__` has no room for), so `__call__`
    returns one of these instead, exactly the way `asyncio.Lock` fits an
    `async with` block despite `acquire`/`release` being its own real
    methods."""

    __slots__ = ("_lock", "_priority", "_preempt")

    def __init__(self, lock: _PriorityAsyncLock, priority: int, preempt: bool = False):
        self._lock = lock
        self._priority = priority
        self._preempt = preempt

    async def __aenter__(self) -> None:
        await self._lock.acquire(self._priority, preempt=self._preempt)

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._lock.release()


class _PriorityAsyncSemaphore:
    """The counting variant of `_PriorityAsyncLock` (field fix, 2026-09-19
    night): `capacity` holders at once, and when it is full the waiters
    are served highest priority (lowest integer) first, FIFO within a
    tier -- exactly the waiter ordering the radio lock uses, instead of
    `asyncio.Semaphore`'s strict FIFO.

    Why: the per-peer in-flight cap on fragmented sends
    (`direct_fragmented_max_in_flight`, second audit 2026-09-19 evening)
    was a plain `asyncio.Semaphore`. In the night session
    (`fieldtests/raw/Alpha0.1.2/*nighttest*`) that FIFO ignored priority
    and was held across every reconcile round: the desktop's fragmented
    sends waited a median 30s for a slot, and two of the laptop's data
    packets were dropped while both of its slots were held by two
    30-minute LXMF announces reconciling at two hops. With this class a
    data packet arriving behind two queued announces is granted the next
    slot before them; and announce-class sends get a slot of their own
    (see `_fragmented_send_slot`), so they cannot occupy the data slots at
    all.

    Same cancellation contract as `_PriorityAsyncLock`: a waiter cancelled
    before it is granted just leaves the queue; one cancelled in the
    instant after being granted passes the permit on. `locked()` reports
    whether a new acquire would have to wait."""

    def __init__(self, capacity: int):
        self._capacity = max(1, int(capacity))
        self._holders = 0
        self._waiters: "dict[int, collections.deque]" = {}

    @property
    def capacity(self) -> int:
        return self._capacity

    def locked(self) -> bool:
        return self._holders >= self._capacity

    def holders(self) -> int:
        return self._holders

    def waiting(self) -> int:
        return sum(len(dq) for dq in self._waiters.values())

    async def acquire(self, priority: int) -> None:
        if self._holders < self._capacity and not self._waiters:
            self._holders += 1
            return
        fut = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(priority, collections.deque()).append(fut)
        try:
            await fut
        except asyncio.CancelledError:
            dq = self._waiters.get(priority)
            if dq is not None and fut in dq:
                dq.remove(fut)
                if not dq:
                    del self._waiters[priority]
            elif fut.done() and not fut.cancelled():
                # Granted in the same instant we were cancelled: the permit
                # counted for us already -- hand it on, or free it.
                if not self._wake_next():
                    self._holders -= 1
            raise

    def release(self) -> None:
        if self._holders <= 0:
            raise RuntimeError("_PriorityAsyncSemaphore released too many times")
        # The permit passes straight to the next waiter (holders unchanged)
        # or is freed when nobody is waiting.
        if not self._wake_next():
            self._holders -= 1

    def _wake_next(self) -> bool:
        for tier in sorted(self._waiters.keys()):
            dq = self._waiters[tier]
            while dq:
                fut = dq.popleft()
                if not fut.done():
                    fut.set_result(None)
                    if not dq:
                        del self._waiters[tier]
                    return True
            del self._waiters[tier]
        return False


class _DutyCycleLimiter:
    """User-requested fix (2026-09-16, direct user instruction following
    the field-test synthesis above): "all interfaces should spend the
    majority of their time listening" -- a global cap on how much of any
    trailing `window_s` this interface spends transmitting, `max_fraction`
    (default 0.30, i.e. at most 30% of every rolling window -- 10s as
    first requested, 60s since 2026-09-18 by the same user's decision,
    see the module docstring's page-load entry), enforced across
    *every* actual radio-keying command this interface issues (CHANNEL
    fastpath/multi-fragment sends, DIRECT sends, bind frames alike -- see
    each of their own call sites for where `wait_for_budget`/`record` are
    used) -- not a per-transport-shape or per-priority-tier budget, a
    single shared one, since there's exactly one physical radio and every
    one of these already funnels through it regardless of which higher-
    level mechanism decided to send.

    Airtime is *estimated*, not measured: `size_bytes * 8 /
    duty_cycle_estimate_bitrate` -- a *separate* config value from this
    interface's own `bitrate`, not that value reused. First shipped
    reusing `bitrate` directly, and a real zero-hop field test the same
    day immediately showed why that was wrong: `bitrate`'s own deployed
    value is deliberately chosen to model CHANNEL's worst-case *sustained*
    throughput (dominated by deliberate inter-fragment spacing) for RNS
    core's own unrelated pacing/timeout math, not a real single-frame
    over-the-air rate -- reusing it estimated a small ~100-char frame at
    ~10 real seconds of airtime, instantly exhausting the budget on every
    single exchange. `duty_cycle_estimate_bitrate` defaults to 1200 (see
    that config value's own comment for the two-step tuning history --
    300 was tried first and still fired on ordinary Link+Resource
    traffic in the same real hardware test) -- a more realistic raw LoRa
    PHY figure, the right basis for *this* estimate specifically. Since
    2026-09-18 (evening) the estimate is the real LoRa time-on-air from
    `_estimate_tx_airtime_s` whenever SELF_INFO has provided the radio's
    SF/BW/CR (see that method: the bitrate figure quantized a 151-char
    fragment to 1.007s where the air really carries 0.877s at SF7/BW62.5/
    CR8, costing a third of the policy's own allowance); the bitrate
    figure remains the fallback. Either way it is deliberately NOT the wall-clock
    duration of the `send_msg`/`send_chan_msg` command call itself --
    that duration is dominated by local serial/BLE/TCP round-trip
    overhead to the companion radio, not real over-the-air time, and
    would make this limiter's accuracy hostage to transport-specific
    latency that has nothing to do with the actual duty cycle question.
    Good enough for a self-imposed courtesy limit, not a regulatory
    compliance guarantee.

    Implementation: a deque of `(start_time, duration)` samples (monotonic
    clock), pruned to the trailing window on every check. `wait_for_budget`
    sleeps in a loop -- never a single fixed sleep -- until transmitting
    for `estimated_duration_s` more would not push cumulative busy time
    over `window_s * max_fraction`, each iteration waking exactly when the
    oldest sample is due to age out of the window (not a fixed poll
    interval), so it wakes only as often as actually necessary."""

    def __init__(self, window_s: float, max_fraction: float):
        self._window_s = window_s
        self._max_busy_s = window_s * max_fraction
        self._samples: "collections.deque" = collections.deque()

    def _prune(self, now: float) -> None:
        cutoff = now - self._window_s
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def _busy_s(self, now: float) -> float:
        self._prune(now)
        return sum(duration for _start, duration in self._samples)

    async def wait_for_budget(self, estimated_duration_s: float, interrupt: "Optional[asyncio.Event]" = None) -> float:
        """Sleeps until sending for `estimated_duration_s` would not push
        the trailing window's cumulative busy time over the cap. Returns
        the total delay actually applied (0.0 if none was needed) --
        capture/debug-only, never affects whether the send itself
        proceeds. A single transmission longer than the entire cap on its
        own (shouldn't happen in practice -- every real frame this
        interface sends is small) is let through once the window is
        otherwise empty, rather than waiting forever for room that will
        never exist."""
        total_wait = 0.0
        while True:
            now = time.monotonic()
            self._prune(now)
            if self._busy_s(now) + estimated_duration_s <= self._max_busy_s or not self._samples:
                return total_wait
            oldest_start, _oldest_duration = self._samples[0]
            wait_s = max(0.01, (oldest_start + self._window_s) - now)
            if interrupt is not None:
                # Phase 1 (2026-09-20): a raw burst's throttle wait is the
                # longest idle hold of the radio lock in the field (26 s
                # in the zero-hop session, a KEEPALIVE queued 20 s behind
                # it); a queued link handshake -- itself duty-cycle exempt
                # -- ends it.
                if interrupt.is_set():
                    raise _PreemptedForHandshake()
                try:
                    await asyncio.wait_for(interrupt.wait(), timeout=wait_s)
                except asyncio.TimeoutError:
                    total_wait += wait_s
                    continue
                total_wait += time.monotonic() - now
                raise _PreemptedForHandshake()
            await asyncio.sleep(wait_s)
            total_wait += wait_s

    def record(self, duration_s: float) -> None:
        self._samples.append((time.monotonic(), duration_s))


# ---- _config.py ----

"""Configuration: the _configure_* family (each the authoritative source for its own keys and defaults, pinned by tests/test_shipped_defaults.py and tests/golden/config_defaults.json), the timing-budget sanity check and the loop-interval floor."""




class _ConfigMixin:
    # -------------------------------------------------------------------
    # Config loading (design invariant #3: every value read here must be
    # used somewhere else in this file -- no automated check exists,
    # verify by hand when adding a key)
    # -------------------------------------------------------------------

    def _configure_identity(self, cfg):
        self.name = cfg.get("name", "Smart MeshCore Interface")

    def _configure_transport(self, cfg):
        self.transport = cfg.get("transport", "serial").lower()

        self.port = cfg.get("port", "/dev/ttyUSB0")
        self.baudrate = int(cfg.get("baudrate", 115200))
        self.host = cfg.get("host", "127.0.0.1")
        self.tcp_port = int(cfg.get("tcp_port", 4403))
        self.ble_name = cfg.get("ble_name", "")

        # The meshcore library's own connection manager can detect a
        # dropped serial/BLE/TCP link and transparently reconnect
        # (CONNECTED/DISCONNECTED events, see _on_mc_connected/
        # _on_mc_disconnected below). Default on: an unattended field radio
        # should try to recover from a USB re-enumeration or a brief BLE
        # range loss rather than sitting dead until rnsd is restarted.
        self.auto_reconnect = _cfg_bool(cfg.get("auto_reconnect", "yes"))
        self.max_reconnect_attempts = int(cfg.get("max_reconnect_attempts", 3))

        # RNS-facing nominal bitrate. Deliberately NOT the base class's
        # 62500 default (`reliability_engine_design.md`'s base-class
        # contract notes call this out explicitly): a real LoRa link run
        # over MeshCore's default settings is far slower than that, and
        # RNS core uses this value for its own throughput-sensitive
        # decisions (announce pacing, Link-establishment timing). User-
        # requested (2026-09-16): defaults to 80, not just recommended in
        # the README/reasoned-but-unmeasured 300 this used to fall back
        # to -- deliberately low so RNS core stays patient/tolerant of
        # this radio's real transit time ("lowball RNS so that it acts
        # more patient," the user's own framing from the same real-
        # hardware session that settled on 80 as the actual deployed
        # value everywhere this interface has been field-tested since).
        # Set explicitly to override for a radio config known to sustain
        # something faster.
        self.bitrate = int(cfg.get("bitrate", 80))

        # User-requested fix (2026-09-16, direct user instruction): "all
        # interfaces should spend the majority of their time listening" --
        # a global cap on how much of any trailing duty_cycle_window this
        # interface spends transmitting, across every CHANNEL/DIRECT/bind-
        # frame send alike. See _DutyCycleLimiter's own docstring for the
        # full design (why the wait loop wakes exactly when room frees up
        # rather than polling on a fixed interval) and each of `_send_
        # channel_fastpath_frame`/`_send_channel_multifragment_pass`/
        # `_send_direct_frame`/`_send_bind_frame`'s own call sites for
        # where it's actually enforced. Defaults match the user's own
        # stated numbers exactly (30% of a rolling 10s window) --
        # window raised to 60s on 2026-09-18 at the user's decision after
        # the zero-hop page-load capture (see module docstring): the same
        # 30%, but a burst can now actually use it instead of the ~26% a
        # 10s window quantizes to --
        # deliberately not derived from any field measurement, a
        # precautionary ceiling rather than a data-driven one.
        self.duty_cycle_enabled = _cfg_bool(cfg.get("duty_cycle_enabled", "yes"))
        self.duty_cycle_window_s = float(cfg.get("duty_cycle_window", 60.0))
        self.duty_cycle_max_fraction = float(cfg.get("duty_cycle_max_fraction", 0.30))
        # User-requested (2026-09-18 evening, see module docstring): link-
        # maintenance traffic (PRIORITY_HANDSHAKE -- LINKREQUEST, PROOF,
        # KEEPALIVE..LRPROOF, RESOURCE_PRF/ICL/RCL) never waits for budget;
        # its airtime is still charged to the window so data pays for it.
        # A Link lost to a throttled keepalive costs far more air to
        # re-establish than the keepalive itself.
        self.duty_cycle_exempt_handshake = _cfg_bool(cfg.get("duty_cycle_exempt_handshake", "yes"))

        # Field-diagnosed fix (2026-09-16, real zero-hop hardware test, the
        # very next thing tried after the duty-cycle cap above shipped):
        # deliberately a *separate* value from `bitrate` above, not reused
        # -- and, per direct user instruction, deliberately decoupled from
        # `bitrate`'s own "lowball it so RNS stays patient" philosophy too.
        # First cut reused `bitrate` for the airtime estimate, and a real
        # zero-hop probe run immediately showed why that was wrong: every
        # exchange took a consistent, suspicious ~10.3-10.7s even fully
        # uncontended (queue_depth=1, lock_wait=0.00s in the capture/debug
        # log). `bitrate`'s own deployed value (80, per this file's own
        # "bitrate tuning guidance") is deliberately chosen to model
        # CHANNEL's *worst-case sustained* throughput and keep RNS core's
        # own unrelated timeout math patient -- not a real single-frame
        # over-the-air rate, and a value this interface *wants* to keep
        # low for that separate reason regardless of what's tuned here.
        # First fix (300, "DIRECT's real observed throughput" per
        # `bitrate`'s own history) still wasn't enough: the same real
        # hardware test's very next run showed a normal Link+Resource
        # exchange (several distinct frames -- LINKREQUEST, LRPROOF,
        # Resource parts, PROOF -- landing within a few seconds of each
        # other) still tripped the cap repeatedly, ~8-9s waits on
        # literally every 2-fragment exchange, because several such
        # frames' *estimated* airtime at only 300bps still added up past
        # the cap faster than real transmission plausibly would. Raised
        # to 1200 -- a more realistic raw LoRa PHY figure -- so the cap
        # still catches genuinely heavy bursts without firing on ordinary
        # traffic. Kept independently configurable (not hardcoded) since
        # a deployment with different real radio parameters (SF/BW/CR)
        # would have a different real answer here too.
        self.duty_cycle_estimate_bitrate = int(cfg.get("duty_cycle_estimate_bitrate", 1200))

        # User-requested fix (2026-09-16): "if we hear a message come in
        # via direct, we wait 3 seconds to hear another before we send
        # again... wait for the incoming interface to either stop
        # sending or hit its airtime limit." A real DIRECT frame arriving
        # is direct evidence the channel was *just* occupied by another
        # node -- this interface has no real-time channel-busy/CAD signal
        # from the `meshcore` library (confirmed: nothing exposes that),
        # so a received frame is the best available proxy for "someone
        # else is transmitting nearby right now" this design has access
        # to. Complementary to, not a replacement for, the duty-cycle cap
        # above: that one throttles based on *this interface's own*
        # recent transmit history; this one defers based on what it just
        # *heard*, to avoid keying the radio into the middle of a peer's
        # own multi-fragment burst (they're very likely mid-transfer if a
        # fragment was just heard, not done). `incoming_quiet_window_s`
        # (3.0s, matching the user's own number exactly) is a rolling
        # window: hearing another DIRECT frame while already waiting
        # extends it, mirroring "wait for them to stop sending." Since
        # this interface can't actually observe a peer's own airtime
        # budget or duty-cycle state, "or hit its airtime limit" is
        # approximated by `incoming_quiet_defer_max_wait_s` (15.0s) -- a
        # bound on this node's own patience, not a real measurement of
        # the other side's limit, so a continuously-chatty peer can never
        # starve this node's own outgoing traffic indefinitely.
        self.incoming_quiet_defer_enabled = _cfg_bool(cfg.get("incoming_quiet_defer_enabled", "yes"))
        self.incoming_quiet_window_s = float(cfg.get("incoming_quiet_window", 3.0))
        self.incoming_quiet_defer_max_wait_s = float(cfg.get("incoming_quiet_defer_max_wait", 15.0))

        # Step 4 of "lessen our reliance on arbitrary wait times"
        # (2026-09-18, see module docstring): holds derived from what the
        # radio just overheard (the step-1 RX-log feed) in place of the
        # fixed random windows. DEFAULT OFF -- every number below was
        # characterised on zero-hop hardware only (this project's standing
        # rule: no timing behaviour changes without field evidence from
        # the regime they target, and the losses live at 1-2 hops). The
        # model runs regardless of this flag so captures record what it
        # *would* have done (`predicted_hold_s`/`hold_reason` on every
        # rx_log record, `medium_busy_remaining_s`/`miss_diagnosis` on
        # every attempt); the flag only decides whether anything acts on
        # it. Two things it replaces when on:
        #   (a) pre-transmit: `_pre_transmit_gate` waits out `_medium_busy_
        #       until`, a rolling prediction of when the air goes quiet,
        #       extended by every overheard packet according to what must
        #       follow it -- a FLOOD packet will be re-flooded by every
        #       repeater in range (measured 0.5-1.2s after it, at SF7:
        #       `rx_log_hold_flood_factor` x its airtime); a DIRECT-routed
        #       packet with N path hashes left has N more forwards coming
        #       (`rx_log_hold_hop_factor` x airtime each, matching the
        #       firmware's 0-1x-airtime direct retransmit delay plus the
        #       forward itself) and, if it's an ACK-bearing type, an ACK
        #       turnaround after that (an ACK frame's airtime +
        #       `rx_log_hold_turnaround_s` of firmware/host processing);
        #       an ACK or ADVERT itself has nothing following it. Airtime
        #       comes from the real LoRa time-on-air formula using the
        #       radio's own SF/BW/CR from SELF_INFO (`_estimate_airtime_
        #       s`), so the holds scale correctly when a deployment runs
        #       SF10 instead of this test rig's SF7.
        #   (b) post-miss: instead of a flat random 0.3-3s after a missed
        #       ACK, the attempt's own RX window (step 2) is diagnosed --
        #       target-originated traffic heard while we waited means the
        #       target was transmitting, not listening (`target_busy`);
        #       our own frame heard forwarded by a repeater but no ACK
        #       means hop 1 worked (`downstream_loss`) and an immediate
        #       retry is as good as any; no forward heard where one was
        #       due means it died at hop 1 (`hop1_loss`). The hold is
        #       then "until the predicted busy window ends" plus a small
        #       jitter, or jitter alone for downstream_loss.
        # Every hold is capped at `rx_log_hold_max_s`; a flood of overheard
        # traffic can never stall a send longer than that per attempt.
        self.rx_log_holds_enabled = _cfg_bool(cfg.get("rx_log_holds_enabled", "no"))
        self.rx_log_hold_max_s = float(cfg.get("rx_log_hold_max", 4.0))
        self.rx_log_hold_flood_factor = float(cfg.get("rx_log_hold_flood_factor", 2.5))
        self.rx_log_hold_hop_factor = float(cfg.get("rx_log_hold_hop_factor", 1.5))
        self.rx_log_hold_turnaround_s = float(cfg.get("rx_log_hold_turnaround", 0.4))
        self.rx_log_hold_jitter_min_s = float(cfg.get("rx_log_hold_jitter_min", 0.2))
        self.rx_log_hold_jitter_max_s = float(cfg.get("rx_log_hold_jitter_max", 0.8))

    def _configure_channel(self, cfg):
        self.channel_idx = int(str(cfg.get("channel_idx", self.DEFAULT_CHANNEL_IDX)).strip())
        self.channel_name = cfg.get("channel_name", "RNSTunnel")

        raw_channel_secret = cfg.get("channel_secret")
        self._using_default_channel_secret = raw_channel_secret is None
        self.channel_secret_hex = (
            raw_channel_secret
            if raw_channel_secret is not None
            else self.DEFAULT_CHANNEL_SECRET_HEX
        )

    def _configure_radio(self, cfg):
        # Optional overrides applied to the connected node's radio at
        # startup. Left at 0 (falsy), the node keeps whatever radio
        # parameters are already stored on it.
        self.radio_freq = float(cfg.get("freq", 0))
        self.radio_bw = float(cfg.get("bw", 0))
        self.radio_sf = int(cfg.get("sf", 0))
        self.radio_cr = int(cfg.get("cr", 0))

    def _configure_fragmentation(self, cfg):
        # Inter-fragment spacing tiers (docs/reliability_engine_design.md
        # §2), selected by _fragment_spacing_range(). The DIRECT-fragmented
        # sender passes the resolved path's out_path_len (zero-hop or
        # known-N-hop tier); the CHANNEL multi-fragment path passes None and
        # gets the flat unknown-multi-hop range below, since a broadcast has
        # no single audience depth.
        self.fragment_delay_min_s = float(cfg.get("fragment_delay_min", 5.0))
        self.fragment_delay_max_s = float(cfg.get("fragment_delay_max", 10.0))
        self.fragment_delay_zero_hop_min_s = float(cfg.get("fragment_delay_zero_hop_min", 0.5))
        self.fragment_delay_zero_hop_max_s = float(cfg.get("fragment_delay_zero_hop_max", 1.5))
        self.fragment_delay_per_hop_min_s = float(cfg.get("fragment_delay_per_hop_min", 5.0))
        self.fragment_delay_per_hop_max_s = float(cfg.get("fragment_delay_per_hop_max", 10.0))

        # Each pass sends frag_idx in a freshly shuffled order rather than
        # always 0..N-1 -- targets the position-dependent half of the
        # loss pattern §2 documents (whichever fragment goes out first in
        # a pass carries no risk from a still-propagating predecessor).
        self.fragment_order_shuffle = _cfg_bool(cfg.get("fragment_order_shuffle", "yes"))

        # Reassembly lifecycle (§5.3-5.4) and whole-packet dedup (§7).
        # reassembly_idle_timeout raised 120 -> 200 (2026-09-19): must cover
        # the worst-case attempt budget a sender can spend on one fragment
        # (_validate_direct_timing_budget: (direct_ack_timeout_routed_max +
        # direct_post_send_listen_max) x max attempts = 48s x 4 = 192s once
        # direct_fragment_finish_attempts became 4). At 120 the interface
        # warned its own defaults were incoherent at every startup.
        self.reassembly_max_keys = int(cfg.get("reassembly_max_keys", 256))
        self.reassembly_idle_timeout_s = float(cfg.get("reassembly_idle_timeout", 200.0))
        self.reassembly_idle_timeout_coop_s = float(cfg.get("reassembly_idle_timeout_coop", 180.0))
        self.whole_packet_dedup_ttl_s = float(cfg.get("whole_packet_dedup_ttl", 150.0))

    def _configure_retry(self, cfg):
        # Per-traffic-class extra CHANNEL retry-pass budgets
        # (docs/reliability_engine_design.md §2's table), keyed off the
        # RNS header fields _parse_rns_header/_retry_extra_for read.
        # `announce_retransmit_extra` covers every ANNOUNCE uniformly --
        # distinguishing a path-response announce (budget 1) from a
        # spontaneous one (budget 0) needs to observe an in-flight path
        # request, which is peer/routing-adjacent state this interface
        # doesn't have until Milestone 5; every announce gets the
        # cheaper, spontaneous-announce default until then, flagged
        # explicitly rather than silently guessed at.
        self.announce_retransmit_extra = int(cfg.get("announce_retransmit_extra", 0))
        # Field-diagnosed (2026-09-18 drive-home capture, 3 hops): the same
        # destination's ANNOUNCE went out four times in five minutes, each a
        # 3-fragment DIRECT exchange traversing three repeaters -- more of
        # the channel's time than the data it carried (the `target_busy`
        # misses that session were mostly this). RNS re-announces on its own
        # schedule and a transport node re-broadcasts others'; this
        # interface forwards one spontaneous ANNOUNCE per destination hash
        # per window. Path-response announces (context PATH_RESPONSE) are
        # exempt: they answer a specific request and have their own 20s
        # limiter. 0 disables.
        self.announce_min_interval_s = float(cfg.get("announce_min_interval", 300.0))
        self.path_req_retransmit_extra = int(cfg.get("path_req_retransmit_extra", 1))
        self.ordinary_data_link_retransmit_extra = int(
            cfg.get("ordinary_data_link_retransmit_extra", 0)
        )
        self.ordinary_data_bare_retransmit_extra = int(
            cfg.get("ordinary_data_bare_retransmit_extra", 1)
        )

        # Independent jittered delay before each retry pass -- drawn
        # fresh per pass, layered outside that pass's own inter-fragment
        # spacing (_fragment_spacing_range), never derived from it. This
        # is what makes passes decorrelated rather than a fixed schedule.
        self.retransmit_jitter_min_s = float(cfg.get("retransmit_jitter_min", 8.0))
        self.retransmit_jitter_max_s = float(cfg.get("retransmit_jitter_max", 20.0))

        # Field-data-driven fix (2026-09-16): real capture from a 5-client
        # field test found DIRECT-fragmented messages where the receiver
        # had already fully reassembled every fragment while the sender
        # was still blindly retrying individual fragments for minutes,
        # because only the fragments' own firmware ACKs -- not the data
        # itself -- failed to make it back (an asymmetric/return-path
        # loss, not a forward-delivery failure). See
        # `_check_remote_completion`'s own docstring for the full
        # mechanism this enables: a lightweight DIRECT query, asked only
        # once both retry passes are exhausted and fragments still appear
        # missing, that lets the receiver's own dedup cache settle the
        # question directly instead of the sender guessing from silence.
        # Fully backward-compatible: a peer that doesn't understand the
        # query frame just never answers, and this falls back to exactly
        # today's give-up behavior once `direct_completion_check_timeout_s`
        # elapses.
        self.direct_completion_check_enabled = _cfg_bool(cfg.get("direct_completion_check_enabled", True))
        self.direct_completion_check_timeout_s = float(
            cfg.get("direct_completion_check_timeout", 5.0)
        )
        # Field fix (2026-09-19 evening session): a hard ceiling on the
        # completion-ANSWER wait, replacing the escalation stack that used to
        # sit on top of the floor above (hop multiplier x RTT term x queue-
        # depth contention term, ceilinged only by direct_ack_timeout_
        # routed_max = 45s). Evidence from that session's 190 completion
        # checks: of the 98 answers that arrived, 70% were within 5s of the
        # budget starting, 96% within 15s, 98% within 20s -- and every band
        # beyond 20s yielded TWO answers in total. Crucially the answer rate
        # FALLS as the budget grows (78% at 5-10s, 92% at 10-20s, 43% at
        # 20-30s, 34% at 40-45s), because a long budget is a symptom of bad
        # conditions rather than a cure for them: the old RTT escalation had
        # the causality backwards. Replaying the session with a 15s cap cuts
        # time spent inside completion waits from 3565s to 1720s (-52%) at a
        # cost of 4 of the 98 answers. A slightly higher ceiling applies from
        # 2 hops out, where the measured round trips are genuinely longer.
        self.direct_completion_check_timeout_max_s = float(
            cfg.get("direct_completion_check_timeout_max", 15.0)
        )
        self.direct_completion_check_timeout_max_multihop_s = float(
            cfg.get("direct_completion_check_timeout_max_multihop", 18.0)
        )
        # Dead-wait trims (2026-09-20): when the QUERY's own firmware ACK was
        # MISSED, the full answer budget above still applied -- yet across
        # the three 2026-09-19 sessions an un-ACKed QUERY was answered only
        # 6/19, 9/65, 4/31 and 0/5 times at 0-3 hops, every hop<=1 answer
        # arrived within 5.6 s, and with `miss_diagnosis=hop1_loss` 0 of 26
        # were ever answered: the missing ACK is the signal that the QUERY
        # never reached the peer (48% of unanswered reconciles, second
        # audit). The answer wait after an un-ACKed QUERY is therefore
        # capped at this grace (from 2 hops the multihop value, where two
        # late answers arrived at 9.0 and 24.3 s), so the re-query goes out
        # 9-12 s sooner. 0 disables the cap.
        self.direct_completion_unacked_grace_s = float(cfg.get("direct_completion_unacked_grace", 6.0))
        self.direct_completion_unacked_grace_multihop_s = float(
            cfg.get("direct_completion_unacked_grace_multihop", 10.0)
        )
        # Per-hop addition to the FLOOR (not the ceiling): a first query, before
        # any RTT sample exists, needs longer at depth. Measured query->answer
        # round trips that session: median 3.2s / 5.7s, p90 11.7s / 16.1s.
        self.direct_completion_check_timeout_per_hop_s = float(
            cfg.get("direct_completion_check_timeout_per_hop", 2.5)
        )
        # Field fix (2026-09-19 night): the RADIO-QUIET WINDOW a reconcile
        # QUERY keeps the radio lock for after its own firmware ACK, so this
        # node is not keying while the ANSWER it just asked for crosses the
        # repeater chain (see _query_remote_fragments and _send_direct_frame_
        # and_wait_for_ack's quiet_wait block). The deadline is
        # `base + per_hop x hops` after the QUERY's own transmit (its
        # MSG_SENT moment, so a lock wait before it does not eat the window),
        # capped by the answer budget itself. Sizing, from `fieldtests/raw/
        # Alpha0.1.2/*nighttest*`: query receipt -> ANSWER on air at the
        # answerer is median 1.3s, and one repeater forward is 1.5-3s per hop
        # (the session's own rx-log echo gap: median 1.75s, p90 2.72s).
        # Anchoring at the transmit rather than at the ACK is what keeps zero
        # hop all but untouched: 1.5s is about the zero-hop ACK latency
        # itself (1.45s median), so the hold there is a few hundred
        # milliseconds, and zero hop measured 96-100% answer delivery in every
        # build with no quiet window at all. 0 for both keys disables the
        # window and restores the fully radio-free answer wait (commit
        # 1919074).
        # Review (2026-09-20): the window is now measured from the QUERY's
        # firmware ACK, not its transmit, and sized 2.0 + 3.0 x hops. Measured
        # in the night and evening captures, answers reached the querier
        # (from the QUERY's ACK) at one hop p50 3.7-4.5s, p90 9-11s; the first
        # cut (1.5 + 2.5 x hops from the transmit, i.e. ~1s after a one-hop
        # ACK) covered only 19-32% of the answers that actually arrived and
        # the simulated one-hop page transfer showed a 9s window beating it on
        # every seed (12/12 parts vs 6-9/12). When three query round trips
        # have been measured for the peer, the window grows to srtt + 2 x
        # rttvar if that is larger, so a slow chain gets the quiet it needs
        # without a config change; the answer budget still caps it.
        self.direct_completion_quiet_base_s = float(cfg.get("direct_completion_quiet_base", 2.0))
        self.direct_completion_quiet_per_hop_s = float(cfg.get("direct_completion_quiet_per_hop", 3.0))

        # Step 3 of "lessen our reliance on arbitrary wait times"
        # (2026-09-18, see module docstring): send-once-then-reconcile for
        # DIRECT-fragmented sends. Pass 0 sends every fragment exactly
        # `direct_fragment_pass0_attempts` (1) time(s); if any fragment
        # got no ACK, ONE completion QUERY asks the receiver which
        # fragments it actually holds (a v2 have-bitmap ANSWER, see
        # `_encode_completion_frame`) and only the fragments it confirms
        # missing are re-driven in pass 1 with the normal attempt budget.
        # Motivation is the 2026-09-16 phantom-ACK field case: fragments
        # that had arrived were blindly retried for minutes because only
        # their ACKs were lost. One QUERY+ANSWER is two small frames; each
        # blind retry is a full fragment plus its ACK -- so this is a net
        # airtime *reduction* whenever at least one "missing" fragment
        # was actually held, and it releases `_direct_exchange_lock`
        # sooner in every case. Disabled (or facing a peer that never
        # answers), pass 0/1 behave exactly as before this step.
        # PRIORITY_HANDSHAKE fragments are exempt and keep their own
        # larger per-fragment budget in pass 0: a lost Link handshake
        # costs a full path rediscovery (see direct_send_attempts_
        # handshake's own comment), and the reconcile round trip would
        # only delay it.
        self.direct_fragment_reconcile_enabled = _cfg_bool(cfg.get("direct_fragment_reconcile_enabled", "yes"))
        self.direct_fragment_pass0_attempts = int(cfg.get("direct_fragment_pass0_attempts", 1))

        # Alpha 0.1.1 fixes (2026-09-18 night, see module docstring):
        # once the receiver provably holds part of a fragmented packet
        # (a pass-0 ACK, or a reconcile answer), the remaining fragments
        # get this larger pass-1 budget -- the drive capture's two path-
        # response announces each died one fragment short on the ordinary
        # budget of 2, wasting the fragments already delivered. And a
        # failed fragmented send is remembered so that RNS re-issuing the
        # identical bytes (its normal retry) resumes the receiver's
        # still-open bucket under the same pkt_id instead of starting a
        # fresh three-fragment send.
        self.direct_fragment_finish_attempts = int(cfg.get("direct_fragment_finish_attempts", 4))
        self.direct_fragment_resume_enabled = _cfg_bool(cfg.get("direct_fragment_resume_enabled", "yes"))

        # Raw binary DIRECT fragments (2026-09-18 night, see module
        # docstring). Default ON since the first field test the same night
        # (user decision): both radios ran it zero-hop and through the
        # public repeater with every transfer completing. Still capability-
        # gated by bind frame, so a peer on an older build never receives
        # raw frames. When on: packets too large for one text frame go to a
        # raw-capable peer as unacknowledged raw bursts reconciled by the
        # "Q" bitmap.
        self.direct_raw_fragments_enabled = _cfg_bool(cfg.get("direct_raw_fragments_enabled", "yes"))
        # Second audit (2026-09-19 evening session): how many fragmented
        # sends (raw or text) to ONE peer may be in flight at once. Bursts
        # were already serialised by the radio lock, but the reconcile
        # windows between them were not, so every queued packet started at
        # once: the desktop had 14 completion windows open simultaneously,
        # the laptop 9. Three costs, all measured in that session: (1) the
        # querier's own radio was transmitting other packets' fragments and
        # queries when 11 of the 25 lost ANSWERs arrived -- half-duplex
        # cannot hear an answer while keying; (2) Resource parts were
        # delivered minutes apart and out of order, and RNS credits a part
        # only inside its receive window from the last consecutive part
        # (Resource.receive_part), so a correctly delivered part
        # (`ca6b3d36db27`, 16:48:25) was discarded and re-requested seven
        # times; (3) one 483-byte packet's raw send stretched past three
        # minutes. Two slots keeps the pipeline full (one packet's fragments
        # can go while the other waits on its answer) without the fan-out.
        # 0 disables the cap. Handshake-class sends bypass it.
        #
        # Field fix (2026-09-19 night, `fieldtests/raw/Alpha0.1.2/*nighttest*`):
        # default 2 -> 0 (off). The first session with the cap on measured
        # against its own purpose: reconcile timeouts did NOT improve
        # (desktop 53% timed out vs 35% the evening before), while the cap
        # cost plenty -- the desktop's fragmented sends waited a median 30s
        # for a slot, four 483-byte Resource parts were dropped after the
        # 120s slot budget ("slot_expired"), and two of the laptop's data
        # packets were dropped while both its slots were held by 30-minute
        # LXMF announces reconciling at two hops. The half-duplex loss the
        # cap was meant to reduce is addressed at its actual location now
        # (the reconcile QUERY's quiet window, direct_completion_quiet_*).
        # When enabled, the slot is priority-aware, announce-class sends
        # get a slot of their own, and a send that cannot get a slot in time
        # proceeds with a warning instead of being dropped -- see
        # _fragmented_send_slot / _PriorityAsyncSemaphore.
        # Review (2026-09-20, simulated one-hop page A/B, seeds 11/21/31, 12 x
        # 483-byte Resource parts, calibrated loss): with the drop removed and
        # the slots priority-aware, the cap is the single most effective
        # change in the set -- every part delivered in 181-243s with raw
        # completion 90-100%, against 3-10 of 12 parts in 600s with the cap
        # off. The night session's objections (dropped parts, announces
        # starving data) are what the rewrite removed, so the default is 2
        # again; the field check the night entry asked for is still owed.
        self.direct_fragmented_max_in_flight = int(cfg.get("direct_fragmented_max_in_flight", 2))
        # Per-fragment raw payload cap on the wire, before the 13-byte
        # header; also bounded by the firmware limits above.
        self.direct_raw_payload_cap = int(cfg.get("direct_raw_payload_cap", 170))
        # Quiet time after each fragment of a burst (the last one included):
        # a flat gap at zero hop (the receiver sends no ACK, so only its own
        # processing needs covering), or this factor x hop count x the
        # fragment's airtime through repeaters -- each repeater in the chain
        # must re-transmit the fragment (after the firmware's random 0-1.5
        # airtime delay) before it can hear the next one. See
        # _raw_fragment_gap_s for the 2026-09-19 field evidence.
        self.direct_raw_zero_hop_gap_s = float(cfg.get("direct_raw_zero_hop_gap", 0.15))
        self.direct_raw_hop_gap_factor = float(cfg.get("direct_raw_hop_gap_factor", 2.0))
        # Burst-then-ask rounds per packet, and QUERY tries per round.
        # Audit fix (2026-09-19): clamped to 4. The raw header carries the
        # round in 2 bits (`attempt & 0x03`), and the firmware dedups
        # RAW_CUSTOM by a hash of payload type + payload bytes
        # (SimpleMeshTables::wasSeen, a 160-entry ring with no time expiry),
        # so round 4 would be byte-identical to round 0 and silently dropped
        # as already-seen at both the repeater and the receiver -- a whole
        # burst of airtime for nothing.
        self.direct_raw_reconcile_rounds = max(1, min(4, int(cfg.get("direct_raw_reconcile_rounds", 3))))
        self.direct_raw_query_attempts = int(cfg.get("direct_raw_query_attempts", 2))
        # Receiver-initiated completion report (2026-09-20, module docstring
        # entry of that date). After a raw burst the sender used to key its
        # reconcile QUERY the instant the last fragment's gap ended -- which
        # is exactly when the receiver, having just handed the packet to
        # RNS, transmits its own reaction (a delivery PROOF, the next
        # Resource request): at zero hop the two frames collided outright,
        # through a repeater they collided at the repeater as hidden nodes
        # (every baseline MeshBench run, 2026-09-20; field: `answering_
        # complete=True` on 49/82, 45/74 and 51/68 of QUERYs, 3.3 QUERY
        # attempts per raw send, receiver-complete p50 7.7 s vs sender-known
        # p50 34 s at one hop). With the report on, the receiver sends the
        # ANSWER unsolicited the moment the burst lands (complete, or its
        # bitmap when the flagged last fragment arrived with gaps), and the
        # sender keeps its radio quiet for `direct_raw_report_wait_base` +
        # `..._per_hop` x hops seconds after the burst (never longer than
        # the answer budget) before falling back to the QUERY path exactly
        # as before. Saves the QUERY frame and its firmware ACK (and their
        # relays) per delivered part, and removes the QUERY-vs-PROOF
        # collision from the common path. `no` restores burst-then-QUERY.
        self.direct_raw_report_enabled = _cfg_bool(cfg.get("direct_raw_report_enabled", "yes"))
        # Phase 1 (2026-09-20): base 2.0 -> 4.0 s, per hop 3.0 -> 2.5 s (the
        # answer budget's own slope, so the floor stays under the budget at
        # every depth: 4 / 6.5 / 9 / 11.5 s against 5 / 7.5 / 10 / 12.5 s),
        # and the window grows to the MEASURED report latency
        # (`_report_rtt`: srtt + 4 x rttvar, on-time and late reports both
        # sampled) above that floor, still capped by the answer budget. The
        # 2026-09-20 field session, zero hop: the receiver's report waited a
        # median 1.1-1.4 s and p90 4-5 s for its own radio lock (behind its
        # previous report's ACK wait and its own sends) on top of ~2.3 s of
        # serial delivery latency, so with a 2 s window only 43 of the
        # desktop's 77 hop-0 rounds were `reported` and 29 paid a QUERY
        # round trip (two frames, two ACKs, ~5 s) for a report that was
        # merely late. The base is what a lost report costs at zero hop.
        self.direct_raw_report_wait_base_s = float(cfg.get("direct_raw_report_wait_base", 4.0))
        self.direct_raw_report_wait_per_hop_s = float(cfg.get("direct_raw_report_wait_per_hop", 2.5))
        # The flag rides the LAST TWO fragments of a burst: when the last
        # one is lost (uniform ~18% per fragment at one hop in the field,
        # systematic in MeshBench's LBT-less radio) the report the second-
        # last fragment triggered still tells the sender what to re-drive --
        # it may have arrived while the burst was still going, in which case
        # the sender keeps it as the fallback and waits the transit time for
        # a newer one first. Two cuts before this one used a receiver-side
        # idle timer instead; see the module docstring entry for why not.
        # Field fix (2026-09-19, bidirectional image transfer): an unanswered
        # reconcile round used to re-burst every un-ACKed fragment. In that
        # capture 3 data sends went to a peer that already held the packet
        # complete -- its answers were stuck behind its own bursts -- and
        # each re-burst lengthened that queue. An unanswered round now
        # re-queries; a burst is allowed again only after this many
        # CONSECUTIVE unanswered rounds (a safety valve for answers that are
        # systematically lost rather than merely late -- the simulated
        # one-hop scenario produced exactly that). 1 restores the old
        # re-burst-every-round; 0 never re-bursts on silence.
        self.direct_raw_reburst_after_unanswered = int(cfg.get("direct_raw_reburst_after_unanswered", 2))
        # `direct_raw_fallback_strikes` answered reconciles in a row showing
        # a burst delivered nothing -> raw is paused for that peer for
        # `direct_raw_fallback_cooldown` and the packet goes as Z85 text on
        # the same path. If the text send succeeds, the PATH (the repeater
        # chain) is noted as not carrying raw packets for `direct_raw_path_
        # unsupported_ttl` and the peer's pause is lifted; a new path is
        # always tried raw-first again (user's design, 2026-09-18 night).
        self.direct_raw_fallback_strikes = int(cfg.get("direct_raw_fallback_strikes", 2))
        # Field fix (2026-09-19 night, `fieldtests/raw/Alpha0.1.2/*nighttest*`):
        # 600 -> 120. At 21:45:14 ONE raw send lost the same fragment three
        # rounds running and the pause that followed sent the next 46 page
        # parts as five Z85 text fragments plus five ACKs each, for ten
        # minutes -- on a path that had just carried 16 of 20 raw sends to
        # completion. The pause is a hedge against a chain that does not
        # carry raw at all (the strike rule above proves that case within
        # two answered reconciles); a lossy-but-working chain only needs a
        # short breather before raw is worth trying again.
        self.direct_raw_fallback_cooldown_s = float(cfg.get("direct_raw_fallback_cooldown", 120.0))
        # Field fix (2026-09-19 night): how many CONSECUTIVE raw sends may end
        # answered-but-incomplete (the receiver held part of the packet after
        # every round, the text path took the rest) before raw is paused for
        # the cooldown. The e87cca8 build -- 8 of 8 raw sends completing at
        # one hop, 85% answer delivery -- only ever paused on the strike rule
        # above; the unconditional pause after a single incomplete send
        # (review, 2026-09-19) is what turned one unlucky fragment into ten
        # minutes of text. A completed raw send clears the count. 1 restores
        # the pause-on-first-incomplete behaviour; 0 never pauses on
        # incomplete sends (the two-strike "delivered nothing" rule and the
        # per-path verdict are unaffected either way).
        self.direct_raw_incomplete_strikes = int(cfg.get("direct_raw_incomplete_strikes", 2))
        self.direct_raw_path_unsupported_ttl_s = float(cfg.get("direct_raw_path_unsupported_ttl", 86400.0))

        # Field-diagnosed (2026-09-18 drive-home capture, see module
        # docstring): give up on a multi-hop DIRECT attempt early when the
        # first-hop repeater was never heard forwarding our frame. Armed
        # per peer only after `direct_hop1_abort_min_samples` echoes have
        # been measured for the current path; the deadline is
        # `max(direct_hop1_abort_min, multiplier x slowest echo seen)`,
        # never longer than the ACK timeout it shortens. The 5s floor is
        # what keeps a stale hop_count harmless: a zero-hop ACK (1.3-2.2s
        # measured) always arrives first. An abort counts as a real
        # failure toward direct_path_reset_threshold -- silence where a
        # forward was due is evidence, unlike a plain timeout.
        self.direct_hop1_abort_enabled = _cfg_bool(cfg.get("direct_hop1_abort_enabled", "yes"))
        self.direct_hop1_abort_min_samples = int(cfg.get("direct_hop1_abort_min_samples", 3))
        self.direct_hop1_abort_echo_multiplier = float(cfg.get("direct_hop1_abort_echo_multiplier", 2.0))
        self.direct_hop1_abort_min_s = float(cfg.get("direct_hop1_abort_min", 5.0))
        # Second audit (2026-09-19 evening session): the deadline used when
        # NO echo samples exist for the peer -- and none exist exactly when
        # the abort matters most. Echo samples are cleared on every path
        # (re)discovery and stale reset (a new path is a new first hop) and
        # are empty after a restart, so a freshly discovered path -- the one
        # most likely to be wrong -- had no abort at all. The mobile node's
        # capture: 59 of its 72 first-hop-silent misses ran the full 13-21s
        # firmware timeout for want of samples, 835s of waiting where ~295s
        # was needed. Echo timings measured across that whole session were
        # median 2.0s and never above 4.0s at 1-3 hops, so 8s (twice the
        # worst) loses nothing. Applied only when the session-wide pool of
        # echo timings is also empty; 0 restores the samples-only behaviour.
        self.direct_hop1_abort_default_s = float(cfg.get("direct_hop1_abort_default", 8.0))

        # Field-diagnosed (same capture): a packet that has sat in this
        # interface's queue (or behind _direct_exchange_lock) longer than
        # this is dropped instead of sent -- 17 LXMF pings queued through
        # a 4-minute outage drained as a stale burst the moment the path
        # came back. ANNOUNCE is exempt (idempotent, and RNS won't re-send
        # one soon). 0 disables. (The default matched reassembly_idle_timeout's
        # 120s when added; that timeout is 200s since 2026-09-19 and the two
        # are independent.)
        # Refined the same evening (page-load capture, see module
        # docstring): the decision is made ONCE, before a packet's first
        # transmission -- never between fragments or attempts, where a drop
        # only wastes the air already spent -- and Resource data parts
        # (context RESOURCE) are exempt: RNS's Resource layer owns their
        # retransmission and re-requests what it lacks.
        self.outgoing_max_age_s = float(cfg.get("outgoing_max_age", 120.0))
        # Phase 1 (2026-09-20): a plain delivery PROOF (packet type PROOF,
        # context NONE -- not LRPROOF / RESOURCE_PRF / the Link band, which
        # `_proof_is_link_class` keeps as handshake class) is useful only
        # until the far side's receipt deadline: RNS `PacketReceipt.timeout`
        # for a non-Link packet over this interface is `first_hop_timeout`
        # (MTU 500 B x 8 / `bitrate` 80 bps = 50 s, + 6) + 6 s per RNS hop
        # = 62 s, measured from the sender's transmit, after which the
        # receipt is FAILED and the proof does nothing (`Transport.jobs`);
        # LXMF's opportunistic delivery retries every 10 s on top and never
        # waits longer. The desktop's 2-hop phase of the 2026-09-20 session
        # queued 13 proofs while every attempt missed (lock waits 8 -> 70 s)
        # and then transmitted 12 of them aged 45-105 s. Replaying that
        # capture: a 45 s cap skips 16 attempts (~76 s of radio lock) and
        # loses 3 proofs that still landed inside the deadline; 60 s skips
        # 12 and loses none; 30 s skips 24 and loses 4. With ~5 s of transit
        # each way at two hops, 45 s is where the deadline sits. Unlike
        # outgoing_max_age this is checked before EVERY attempt, not only
        # the first: a proof is one bare frame, so a stale retry wastes
        # nothing already spent. 0 disables.
        self.proof_max_age_s = float(cfg.get("proof_max_age", 45.0))
        # How many times the same bytes may be suppressed as "already in
        # flight" before the packet is forced through with a fresh in-flight
        # entry (field fix 2026-09-19: a stuck entry deadlocked a transfer for
        # 178s). 1 disables the suppression entirely.
        self.outgoing_duplicate_suppress_limit = int(cfg.get("outgoing_duplicate_suppress_limit", 3))

        # Field fix (2026-09-19 evening session): how much more evidence a
        # RECENTLY HEALTHY path needs before a stale-path reset discards it --
        # see record_direct_send_result for the incident.
        self.direct_path_healthy_window_s = float(cfg.get("direct_path_healthy_window", 120.0))
        self.direct_path_healthy_recent_successes = int(
            cfg.get("direct_path_healthy_recent_successes", 5)
        )
        self.direct_path_healthy_patience_multiplier = float(
            cfg.get("direct_path_healthy_patience_multiplier", 2.5)
        )

    def _configure_path_discovery(self, cfg):
        # docs/path_discovery_spec.md's "Retry and backoff structure" --
        # a quick-retry burst (each attempt already naturally spaced by
        # its own request/response wait, no additional artificial delay
        # layered on top), then per-target exponential backoff.
        self.path_discovery_quick_attempts = int(cfg.get("path_discovery_quick_attempts", 2))
        self.path_discovery_base_cooldown_s = float(cfg.get("path_discovery_base_cooldown", 20.0))
        self.path_discovery_max_cooldown_s = float(cfg.get("path_discovery_max_cooldown", 900.0))
        self.path_discovery_backoff_factor = float(cfg.get("path_discovery_backoff_factor", 1.8))

        # Stale cached-DIRECT-path detection (§8). Built and unit-tested in
        # Milestone 4; since Milestone 5 every live DIRECT send path feeds it
        # through record_direct_send_result() (_send_direct_with_attempts,
        # the raw sender's reconcile-query evidence, _send_direct_supplement).
        self.direct_path_reset_threshold = int(cfg.get("direct_path_reset_threshold", 3))
        self.direct_path_reset_rssi_floor = float(cfg.get("direct_path_reset_rssi_floor", -105.0))
        self.direct_path_reset_patience_multiplier = float(
            cfg.get("direct_path_reset_patience_multiplier", 3.0)
        )
        # User-requested fix (2026-09-15, post-alpha-0.1.0 2-hop field test):
        # `direct_path_reset_threshold`'s default at the time (2; 3 since
        # ef57809) meant just two
        # consecutive full-timeout DIRECT failures discard a cached path
        # and force a fresh `discover_path()` burst (up to
        # `path_discovery_quick_attempts` real over-the-air round trips)
        # on the very next send -- with no floor on how recently that path
        # was itself successfully confirmed. When several messages fail
        # close together for a reason that has nothing to do with the path
        # itself (shared-radio congestion, several concurrent sends
        # queued behind `_direct_exchange_lock`, a repeater mid-relay --
        # exactly the real 2-hop field test scenario), this can re-discover
        # a path that was only just confirmed seconds ago and hasn't
        # plausibly gone stale, adding real path-discovery airtime on top
        # of an already-congested channel -- a self-inflicted feedback
        # loop, not a genuine stale-path recovery. A path resolved more
        # recently than this floor is trusted regardless of how many
        # failures have piled up since; the failure count is NOT reset by
        # this skip (see record_direct_send_result), so a path that's
        # genuinely gone bad is still torn down and rediscovered once it's
        # old enough for that to be plausible, just not before.
        self.direct_path_reset_min_age_s = float(cfg.get("direct_path_reset_min_age", 60.0))

        # Live, periodic contact-table read -- reliability_engine_design.md
        # §2's "data-source gap" fix: never served from a cached/inherited
        # value. Feeds path discovery's own ensure_contacts() precondition
        # now; Milestone 5 is expected to also feed this into the
        # zero-hop/known-N-hop spacing tiers _fragment_spacing_range()
        # already implements but has no live data source for yet.
        self.contact_refresh_interval_s = float(cfg.get("contact_refresh_interval", 30.0))

        # Milestone 4 granted base-telemetry permission to every known
        # contact by default, flagged there as a placeholder: docs/
        # path_discovery_spec.md recommends granting only to peers
        # confirmed via this interface's own bind-frame protocol, which
        # didn't exist until Milestone 5. Now that it does
        # (_refresh_contacts_and_grant_telemetry grants bind-confirmed
        # peers unconditionally), this defaults to "no" -- set to "yes"
        # as an explicit escape hatch back to the old open-to-every-
        # contact behavior (exactly as open as any public MeshCore
        # channel already is), not because the bind-gated grant above
        # ever needs it to function.
        self.telemetry_grant_all_contacts = _cfg_bool(cfg.get("telemetry_grant_all_contacts", "no"))

        # Phase 1 (2026-09-20): answer an RNS path re-request from the
        # announce this interface already delivered. On a non-transport
        # node a pending Link that closes without activating makes RNS
        # `expire_path` the destination and request the path again
        # (`Transport.jobs`, pending-links check); the answering node
        # replies from ITS path table with the same cached announce bytes
        # (`Transport.path_request`), and the requester accepts them
        # because the destination is no longer in its table. The laptop
        # (2 hops, 2026-09-20) received the identical 235-byte announce for
        # one destination six times in an hour, each a 2-3 fragment raw
        # send plus reports at two hops, each preceded by a 2-hop DIRECT
        # request; the desktop answered 7 and suppressed 5 more as
        # duplicates in flight. Every ANNOUNCE handed to RNS from a bound
        # peer is cached (bytes as received, LRU, `announce_cache_ttl`);
        # a path request whose target is cached, whose source peer is still
        # bound and not in discovery backoff, and which has not been
        # answered locally within `path_request_local_answer_min_interval`
        # is answered by re-injecting the cached announce (context
        # rewritten to PATH_RESPONSE so a transport node does not
        # re-flood it) and is NOT transmitted. The local answer verifies
        # nothing: the next request for the same destination inside the
        # interval goes over the air, which is how a genuinely dead
        # destination is re-verified. 0 disables either.
        self.announce_cache_ttl_s = float(cfg.get("announce_cache_ttl", 3600.0))
        self.path_request_local_answer_min_interval_s = float(cfg.get("path_request_local_answer_min_interval", 120.0))

    def _configure_peer_discovery(self, cfg):
        # Master on/off switch for the entire bind-frame subsystem (both
        # sending this node's own REQUEST/RESPONSE and answering others').
        # Default on -- peer_discovery_design.md §4's "must not be
        # skippable" rule applies to *cached state* gating the bootstrap
        # REQUEST, not to this being an explicit, deliberate operator/test
        # override that exists independently of any cache. Tests for
        # Milestones 1-4 behavior that don't care about peer discovery set
        # this to "no" so a fake meshcore connection with no channel
        # traffic expectations beyond those milestones' own isn't also
        # asked to field an unrelated bootstrap bind-frame send.
        self.peer_discovery_enabled = _cfg_bool(cfg.get("peer_discovery_enabled", "yes"))

        # docs/peer_discovery_design.md's bind-frame protocol and routing
        # tables. Whether THIS node advertises router capability
        # (has_upstream_rns, §2) is a deployment fact -- whether it's also
        # bridging to the wider Reticulum network via another interface --
        # that this interface cannot infer on its own; defaults to edge
        # (conservative: an edge peer never gets DIRECT-nudged for path
        # requests it structurally can't answer, per routing_decisions.md).
        self.declares_upstream_rns = _cfg_bool(cfg.get("declares_upstream_rns", "no"))

        # Per-responder RESPONSE jitter (§3) -- collision/half-duplex-deaf-
        # repeater spacing, not suppression (every well-formed REQUEST still
        # gets answered). And a separate, much longer global minimum
        # re-response interval -- the real suppression mechanism, since a
        # peer that already heard this node's capability gains nothing from
        # hearing it again. Both "reasoned, not measured" per the doc's own
        # flag -- worth field-validating once there's code to test.
        self.bind_response_jitter_min_s = float(cfg.get("bind_response_jitter_min", 10.0))
        self.bind_response_jitter_max_s = float(cfg.get("bind_response_jitter_max", 30.0))
        self.bind_response_min_interval_s = float(cfg.get("bind_response_min_interval", 300.0))

        # Bootstrap REQUEST: always sent once, unconditionally, at process
        # start (§4's "must not be skippable" rule -- the old design's own
        # regression). peer_discovery_target_peers/_rerequest_interval
        # govern only the *optional* slow repeat while below that target;
        # never whether the first REQUEST happens at all.
        self.peer_discovery_target_peers = int(cfg.get("peer_discovery_target_peers", 3))
        self.peer_discovery_rerequest_interval_s = float(
            cfg.get("peer_discovery_rerequest_interval", 1800.0)
        )
        # Simulation finding (2026-09-19, calibrated 3-repeater bring-up):
        # a single startup REQUEST through several lossy hops is a coin
        # flip, and with the next one 30 minutes out a fresh pairing can
        # sit unbound for that long. While still below the target peer
        # count the repeat now starts here and doubles each round up to
        # peer_discovery_rerequest_interval -- 60, 120, 240 ... 1800s --
        # so a first meeting recovers in a minute or two at the cost of a
        # few small CHANNEL frames, and a lone node still quiets down to
        # the old rate.
        self.peer_discovery_rerequest_initial_s = float(cfg.get("peer_discovery_rerequest_initial", 60.0))

        # Peer expiry (§6) -- a day default, matching the old design's own
        # with no specific field evidence for a different number. Sweep
        # interval is an internal granularity choice (like
        # REASSEMBLY_CLEANUP_INTERVAL_S), generous relative to the TTL it
        # enforces.
        self.peer_ttl_s = float(cfg.get("peer_ttl", 86400.0))
        self.peer_ttl_sweep_interval_s = float(cfg.get("peer_ttl_sweep_interval", 300.0))

        # PROOF-correlation table TTL (§7) -- long enough to cover a
        # realistic PROOF round trip, short enough the table doesn't grow
        # to track every packet ever delivered. The old design used 120s;
        # kept as the starting point, no new field evidence for a different
        # number.
        self.proof_correlation_ttl_s = float(cfg.get("proof_correlation_ttl", 120.0))

        # Optional override for the peer-cache JSON file's path -- defaults
        # to a file under RNS.Reticulum.storagepath (verified populated
        # once a real RNS.Reticulum() instance exists) when left unset.
        self.peer_cache_path = cfg.get("peer_cache_path", None) or None

        # docs/routing_decisions.md's path-request DIRECT-supplement cap --
        # "a small number (e.g. the most-recently-confirmed few)", not
        # unconditionally every known router as the router count grows.
        self.path_request_direct_supplement_cap = int(cfg.get("path_request_direct_supplement_cap", 2))

        # Milestone 6's DIRECT-bootstrap-supplement cap (the fix for
        # peer_discovery_design.md §7's own bootstrap gap -- see
        # _select_bootstrap_supplement_targets) -- same reasoning as the
        # path-request cap above, kept as a separate knob since the two
        # mechanisms have different target-selection rules (this one
        # isn't filtered by router capability).
        self.bootstrap_direct_supplement_cap = int(cfg.get("bootstrap_direct_supplement_cap", 2))

        # Floor under a DIRECT send's ACK-wait timeout, in case a firmware
        # reply's own suggested_timeout is missing or unrealistically small
        # -- this interface's own safety margin, not a firmware constant.
        self.direct_ack_min_timeout_s = float(cfg.get("direct_ack_min_timeout", 5.0))

        # Milestone 6 (docs/reliability_engine_design.md §4): the outer
        # multi-attempt loop for a single DIRECT message -- mirrors, not
        # replaces, the firmware's own per-attempt ACK/content-attempt
        # mechanism; this engine decides how many times to ask the
        # firmware to try. Applies uniformly to a bare single-message
        # DIRECT send and, per-fragment, to each fragment of a DIRECT-
        # fragmented send.
        #
        # User-requested fix (2026-09-16, following the priority-lock
        # change above, same real 1-hop repeater field data): default
        # lowered from 3 to 2. At ~50% measured per-attempt loss, every
        # fragment that exhausts its own attempt budget without success
        # still holds `_direct_exchange_lock` (now priority-aware, but
        # still one radio) for the full cost of each failed attempt --
        # ack_timeout plus the post-send listen window -- before anything
        # else queued behind it gets a turn. A fragment that's going to
        # need more than 2 tries under these conditions isn't meaningfully
        # more likely to succeed on a 3rd attempt than to eventually get
        # picked up by `_send_direct_fragmented`'s own existing pass-1 re-
        # drive (a fresh attempt budget, not a continuation of a failing
        # one) or, above this interface entirely, RNS's own Resource-
        # transfer layer re-requesting specifically-missing parts once a
        # transfer stalls -- both already exist and have equal or better
        # information about what's actually still missing than blindly
        # spending a 3rd attempt on the same fragment does. Lowering this
        # trades a small amount of per-fragment persistence for freeing
        # the shared radio sooner under exactly the lossy conditions where
        # that trade matters -- still a flat, simple value, not made
        # adaptive to measured loss (that's a real further refinement,
        # deliberately not done here without more field data to justify
        # it).
        self.direct_send_attempts = int(cfg.get("direct_send_attempts", 2))

        # User-requested fix (2026-09-16), following the confidence
        # discussion above the retry-budget cut: a LINK_REQUEST/PROOF-
        # class exchange (PRIORITY_HANDSHAKE) is the worst candidate for
        # the same reduced budget ordinary DATA just got. Losing a DATA
        # fragment is comparatively cheap -- `_send_direct_fragmented`'s
        # own pass-1 re-drive, or RNS's own Resource-layer recovery,
        # already exist to pick it up. Losing a Link handshake is not
        # cheap: RNS's own Transport.py confirms (source-checked the same
        # day) that a failed local-client Link attempt tears the path
        # down and forces a full path rediscovery -- itself more airtime,
        # making the *next* handshake attempt less likely to succeed too.
        # That's a self-reinforcing loop a cheap handshake budget makes
        # worse, not better. A separate, larger budget for handshake-
        # class exchanges specifically targets preventing that cascade,
        # while the lower ordinary-DATA budget above still frees the
        # (now priority-aware) shared radio sooner for everything else.
        # See _send_direct_with_attempts's own docstring for exactly how
        # the two budgets are picked between.
        self.direct_send_attempts_handshake = int(cfg.get("direct_send_attempts_handshake", 4))

        # User-requested fix (2026-09-15), generalized the same day after
        # a second real 2-hop field test, then split by outcome the day
        # after that (2026-09-16) once a real zero-hop NomadNet session
        # showed the generalized version's actual cost: a DIRECT
        # send+ACK-wait used to release _direct_exchange_lock the instant
        # it resolved (ACKed or not), letting the very next contender --
        # a retry of the same fragment, the next fragment, or a completely
        # different queued message -- key the radio again immediately.
        # Fine if nothing else was going on, but if the miss was a
        # collision (a peer transmitting into us at the exact moment we
        # sent, or vice versa -- this is a shared half-duplex radio, and a
        # real repeater adds its own settling time on top), going again
        # instantly just repeats the same collision window, and does so
        # for every message queued behind it too.
        #
        # Applying one flat 0-5s range to *every* attempt regardless of
        # outcome (2026-09-15's generalization) turned out to have a real
        # cost the zero-hop field session's own packet capture made
        # obvious: 100% of DIRECT attempts were ACKed (as expected --
        # zero-hop, nothing to collide with), yet every single one still
        # paid an average ~2.5s tax before the lock released, and because
        # NomadNet's page transfer spawns many small RESOURCE-related
        # packets that all queue up behind the same lock,
        # _direct_exchange_queue_depth reached 14 with individual attempts
        # waiting up to 48s just for their own turn -- none of it bought
        # any real collision protection, since nothing was colliding.
        # `direct_post_send_listen_min_s`/`max_s` (0-5s, unchanged) now
        # applies ONLY when the attempt got no ACK -- the one case where
        # "something might have collided, don't retry into it instantly"
        # is actually a live hypothesis, independent of this node's own
        # queue depth (the cause could be entirely external). A
        # successful attempt -- which is itself real evidence the channel
        # was clear for this exchange -- instead draws from the much
        # smaller `direct_post_send_listen_success_min_s`/`max_s` (0-0.5s
        # default): still genuinely random every time (never a fixed
        # value, deliberately, so this can't settle into a lockstep
        # pattern with anything else on the channel), still real spacing
        # between successive different messages queued behind the lock,
        # just not the same "assume something might be wrong" cost a
        # clean ACK gives no reason to pay. Both remain simple, flat
        # ranges for now -- "we can tune this later" still applies. See
        # _send_direct_frame_and_wait_for_ack's own docstring for exactly
        # where each fires.
        #
        # 0.3-3 s -> 0.2-1 s (2026-09-20, dead-wait trims): the post-miss
        # listen averaged 1.7 s on 598 field misses -- ~1000 s of lock time
        # across the three 2026-09-19 sessions -- while the evening audit
        # measured frame overlap between the two nodes at 8.6% against 6.9%
        # expected by chance (loss, not contention, explains 53% of misses),
        # and the firmware's own listen-before-talk (Dispatcher::checkSend
        # defers while the radio reports a frame in progress) already keeps
        # the retry out of an audible frame. Still random, per the user's
        # standing instruction; just a smaller range.
        self.direct_post_send_listen_min_s = float(cfg.get("direct_post_send_listen_min", 0.2))
        self.direct_post_send_listen_max_s = float(cfg.get("direct_post_send_listen_max", 1.0))
        self.direct_post_send_listen_success_min_s = float(cfg.get("direct_post_send_listen_success_min", 0.0))
        self.direct_post_send_listen_success_max_s = float(cfg.get("direct_post_send_listen_success_max", 0.4))

        # The routed-mode ACK-wait ceiling §4 specifies (scaled off the
        # firmware's own hop-aware suggested_timeout, capped here). This
        # design's own routing rule (routing_decisions.md) never issues a
        # DIRECT send without already believing a resolved path exists --
        # every DIRECT send this interface makes is therefore always in
        # the "routed" regime from its own point of view, so only this
        # ceiling is used; the doc's *flood*-mode ceiling
        # (`direct_ack_timeout_max_s`, 10s) describes a regime this
        # interface's own dispatcher structurally never enters (it falls
        # back to CHANNEL broadcast instead of ever flooding a DIRECT
        # send with no resolved path), so it's deliberately not exposed
        # as a config value here -- there would be no code path that
        # reads it.
        self.direct_ack_timeout_routed_max_s = float(cfg.get("direct_ack_timeout_routed_max", 45.0))

        # Field fix (2026-09-19 evening session, multi-agent capture audit):
        # a hop-aware ceiling on the ACK wait, well below the absolute
        # `direct_ack_timeout_routed_max_s` ceiling above. Evidence from that
        # session (1080 attempts, 0-2 hops, both nodes): the LARGEST ACK
        # latency that ever actually arrived was 8.15s (p99 5.82s; per hop:
        # 0 hops max 3.00s, 1 hop 6.06s, 2 hops 8.15s), while the timeouts
        # the firmware's own suggestion produced ran to 28.0s (median 8.0s,
        # p90 16.2s). The interface spent 2510s -- 20% of the session -- in
        # ACK waits that were never going to be answered, holding
        # `_direct_exchange_lock` the whole time, while the radio itself was
        # only ~13% busy: the binding constraint on this interface is not
        # airtime or collisions, it is this serialised dead time. Replaying
        # the session, `base + per_hop x hops` = 8 + 4h would have cut that
        # to 2121s (-15%) while cutting off ZERO of the 559 ACKs that did
        # arrive. Kept proportional to path length rather than flat so a
        # deeper path still gets the time it genuinely needs.
        #
        # Tightened 2026-09-20 (dead-wait trims, module docstring entry of that
        # date) to 5 + 3h: re-derived over all three 2026-09-19 sessions
        # (2670 ACKs), the largest ACK that ever arrived was 3.82 / 6.06 /
        # 8.15 / 7.25 s at 0 / 1 / 2 / 3 hops, so 5 / 8 / 11 / 14 s still
        # cuts off ZERO of them (31-93% above the per-hop maximum) and, in
        # replay, saves a further 824 + 635 + 171 s of dead lock time at
        # 1-3 hops over 8 + 4h. `direct_ack_min_timeout` (5 s) is the hop-0
        # floor, so hop 0 is unchanged. Tuned for SF7/BW62.5/CR8 like every
        # absolute second in this file.
        self.direct_ack_timeout_base_s = float(cfg.get("direct_ack_timeout_base", 5.0))
        self.direct_ack_timeout_per_hop_s = float(cfg.get("direct_ack_timeout_per_hop", 3.0))

        # Step 2 of "lessen our reliance on arbitrary wait times"
        # (2026-09-18, see module docstring): measured ACK round-trip
        # time, per peer, driving the ACK-wait timeout. Until now the
        # timeout was the firmware's own hop-count-derived guess
        # (`suggested_timeout` x1.2, floored at direct_ack_min_timeout_s,
        # capped at direct_ack_timeout_routed_max_s) and was never
        # compared against reality; step 1's zero-hop hardware run showed
        # a 5.2s timeout guarding a 0.7-1.1s real RTT. Since a missed ACK
        # holds `_direct_exchange_lock` for the whole timeout -- and the
        # 2026-09-16 1-hop capture's mean 11s/max 49s lock waits were
        # almost entirely other sends' full timeouts -- a timeout sized to
        # the *measured* RTT is the single largest reduction in wasted
        # lock time available without touching airtime. Kept deliberately
        # conservative: Jacobson/Karels smoothing (srtt + 4*rttvar, then
        # `direct_ack_rtt_timeout_multiplier` on top), only after
        # `direct_ack_rtt_min_samples` real ACKs from that peer, never
        # below `direct_ack_rtt_min_timeout_s`, never ABOVE the firmware-
        # derived value it replaces (so the worst case is exactly today's
        # behaviour), and Karn-style invalidated on the first miss so a
        # link that got slower falls straight back to the firmware guess
        # until fresh samples exist. Stats are also dropped whenever the
        # peer's path changes (`_reset_stale_path`, a fresh
        # `discover_path` result) -- an RTT measured over one path says
        # nothing about another.
        self.direct_ack_rtt_adaptive_enabled = _cfg_bool(cfg.get("direct_ack_rtt_adaptive_enabled", "yes"))
        self.direct_ack_rtt_min_samples = int(cfg.get("direct_ack_rtt_min_samples", 3))
        self.direct_ack_rtt_timeout_multiplier = float(cfg.get("direct_ack_rtt_timeout_multiplier", 2.0))
        self.direct_ack_rtt_min_timeout_s = float(cfg.get("direct_ack_rtt_min_timeout", 3.0))
        # Field-diagnosed (2026-09-18 drive-home, 3 hops): a miss under a
        # measured timeout used to discard the estimate outright (Karn), so
        # the very next attempt paid the firmware's full 28s even on a path
        # that was merely slow. The dead-first-hop case is now caught by
        # the hop-1 abort; for a slow-but-alive path each consecutive miss
        # instead multiplies the measured timeout by this factor (RFC 6298's
        # RTO backoff), still never above the firmware value, and the next
        # real ACK resets it. <= 1 restores the discard behaviour.
        self.direct_ack_rtt_miss_backoff = float(cfg.get("direct_ack_rtt_miss_backoff", 2.0))

    def _configure_observability(self, cfg):
        # Per-interface debug logging, independent of RNS core's global
        # [logging] loglevel. RNS.log() gates every message (ours and RNS
        # core's own) on one global level, so raising it to DEBUG to see
        # this interface's own diagnostics would also enable RNS core's
        # own debug firehose. Messages logged via self._debug() below are
        # emitted at LOG_INFO, gated only by this flag.
        self.debug_logs = str(cfg.get("debug_level", "info")).strip().lower() == "debug"

        # How often the periodic structured stats snapshot (_stats_loop)
        # is logged. Per `docs/interface_architecture.md`'s observability
        # requirements, this mechanism exists from the first milestone
        # that connects to anything, even though M0 has little of
        # substance to report yet -- later milestones (queue depth,
        # reassembly bucket counts, per-peer state) extend the same
        # snapshot rather than inventing a second one.
        self.stats_interval_s = float(cfg.get("stats_interval", 60.0))

        # User-requested packet capture (2026-09-15): off by default,
        # every in/out RNS packet appended as one JSON line to a file
        # under this directory when enabled -- see _capture_event's own
        # docstring for the record format and _async_setup for where the
        # file actually gets opened (once this node is online and its
        # storage path is known).
        self.packet_capture_enabled = _cfg_bool(cfg.get("packet_capture_enabled", "no"))
        self.packet_capture_dir = cfg.get("packet_capture_dir", None)

        # User-requested (2026-09-18, "lessen our reliance on arbitrary
        # wait times" -- step 1 of that plan, see module docstring): tap
        # the companion firmware's own raw-RX log feed. `MyMesh::logRxRaw`
        # (referenceprojects/MeshCore-main/examples/companion_radio/
        # MyMesh.cpp) pushes EVERY packet the radio decodes -- addressed to
        # this node or not: other peers' DIRECT frames, flood repeats,
        # ACKs in transit, a repeater's echo of this node's own frame --
        # to the host as PUSH_CODE_LOG_RX_DATA, unconditionally whenever
        # the serial link is up (confirmed against firmware source: no
        # pref gates it), and the installed `meshcore` library (2.3.9.1,
        # reader.py's LOG_DATA branch) parses it into
        # `EventType.RX_LOG_DATA` with SNR/RSSI/route type/payload type/
        # path. That is the closest thing to a carrier-sense signal this
        # interface can get without a firmware fork (hardware CAD and the
        # RSSI interference threshold are both hard-coded off in the
        # companion build), and strictly more information than the
        # "a DIRECT frame decoded for us" proxy `_wait_for_incoming_quiet`
        # keys off today. THIS STEP IS OBSERVE-ONLY: counters in the
        # [STATS] snapshot and one `rx_log` record per overheard packet
        # in the packet capture, so real field captures can establish the
        # correlations (repeater echoes of our own frames, ACK sightings,
        # burst structure of a peer's fragmented send) before any timing
        # decision is allowed to depend on this feed. No routing/timing
        # logic reads it yet. Default on since subscribing costs nothing
        # over the air; configurable off in case a busy mesh's log volume
        # is unwanted on a slow serial/BLE link.
        self.rx_log_observe_enabled = _cfg_bool(cfg.get("rx_log_observe_enabled", "yes"))

    def _validate_direct_timing_budget(self) -> None:
        """Field-diagnosed fix (2026-09-18, see module docstring's
        2026-09-18 entry for the full incident this responds to): a
        real field test broke multi-hop delivery entirely after several
        individually-reasonable DIRECT timing knobs -- spread across
        `_configure_fragmentation`, `_configure_retry`, `_configure_
        transport`, and `_configure_peer_discovery`, each tuned in
        isolation in a separate field-driven fix -- combined to let a
        single fragment's worst-case retry cost approach or exceed
        `reassembly_idle_timeout_s`, the fixed clock the *receiver* is
        racing them against. Nothing before this method ever checked
        that those two sides of the same budget were still compatible
        after an operator (or a future field fix) changed one of them.

        The question it answers is deliberately narrow and framed to stay
        actionable in both directions: **how many clock-racing send
        attempts actually fit inside the receiver's idle window?** The
        receiver resets a bucket's clock only when a fragment genuinely
        *arrives* (`_ReassemblyBucket.last_progress`), so the relevant
        comparison is one attempt's worst-case cost against
        `reassembly_idle_timeout_s`. If fewer than `direct_send_attempts`
        of them fit, the receiver can evict a bucket while the sender is
        still legitimately working through that same fragment's own
        configured attempt budget for the first time -- the retry logic
        and the patience it's spending are then incoherent with each other
        by construction, regardless of link quality.

        An attempt that races the clock costs `direct_ack_timeout_routed_
        max_s` (the ACK wait) plus `direct_post_send_listen_max_s` (the
        post-send listen window, held under the same lock).
        `incoming_quiet_defer_max_wait_s` is deliberately NOT in that
        figure: post-2026-09-18 only a message's genuinely-first
        transmission pays it, and that one happens *before* the receiver
        has a bucket or a clock at all (see `_pre_transmit_gate`'s
        `skip_quiet_defer`). It is still reported in the warning, because
        re-broadening the quiet-defer trigger is exactly what made this
        ratio fail in the first place.

        Checked against the real incident: with that day's knobs and the
        pre-fix behaviour of taxing *every* attempt with the quiet defer,
        an attempt cost 63s against a 120s window -- 1.90 attempts, below
        the budget of 2, so this would have fired at startup. With the
        same knobs and the fix in place an attempt costs 48s -> 2.50
        attempts, which passes.

        Deliberately excludes costs that can't be bounded from config
        alone: `_direct_exchange_lock` queueing delay (depends on how many
        *other* messages are competing for the one radio), this message's
        own other fragments being attempted in between, and
        `_throttle_for_duty_cycle`. So a config that fails this check is
        confirmed incoherent; one that passes isn't guaranteed safe under
        heavy contention, just no longer broken by construction. Warns
        only -- an operator's explicit config is never silently
        overridden."""
        # Step 4 (2026-09-18): with rx_log_holds_enabled, one attempt can
        # additionally wait up to rx_log_hold_max_s before transmitting and
        # the post-miss hold is capped at the same value instead of
        # direct_post_send_listen_max_s -- both counted here.
        hold_cap_s = self.rx_log_hold_max_s if self.rx_log_holds_enabled else 0.0
        # Code review (2026-09-18): with holds on, the post-miss wait is
        # _post_miss_hold_s, capped at rx_log_hold_max_s -- it REPLACES the
        # flat listen range rather than adding to it (see _send_direct_
        # frame_and_wait_for_ack), so that is the term counted here.
        post_miss_cap_s = hold_cap_s if self.rx_log_holds_enabled else self.direct_post_send_listen_max_s
        clock_racing_attempt_s = self.direct_ack_timeout_routed_max_s + hold_cap_s + post_miss_cap_s
        if clock_racing_attempt_s <= 0:
            return
        attempts_that_fit = self.reassembly_idle_timeout_s / clock_racing_attempt_s

        # Audit fix (2026-09-19): this compared only against
        # `direct_send_attempts` (2), so on the shipped defaults
        # 120/48 = 2.5 fits and it stayed silent -- while the budgets that
        # actually apply to a fragment racing the receiver's clock are
        # larger: `direct_send_attempts_handshake` (4) for a handshake-class
        # exchange, and `direct_fragment_finish_attempts` (4) for a pass-1
        # finish re-drive. 4 x 48s = 192s against a 120s
        # reassembly_idle_timeout is exactly the incoherence this method
        # exists to catch.
        worst_attempts = max(
            self.direct_send_attempts,
            self.direct_send_attempts_handshake,
            self.direct_fragment_finish_attempts,
        )
        if attempts_that_fit < worst_attempts:
            breakdown = f"direct_ack_timeout_routed_max={self.direct_ack_timeout_routed_max_s:.1f}s"
            if self.rx_log_holds_enabled:
                breakdown += (
                    f" + rx_log_hold_max={hold_cap_s:.1f}s pre-transmit"
                    f" + rx_log_hold_max={hold_cap_s:.1f}s post-miss"
                )
            else:
                breakdown += f" + direct_post_send_listen_max={self.direct_post_send_listen_max_s:.1f}s"
            RNS.log(
                f"{self}: DIRECT timing budget is incoherent with reassembly patience -- one "
                f"send attempt racing the receiver's reassembly clock can cost up to "
                f"{clock_racing_attempt_s:.1f}s ({breakdown}), so only {attempts_that_fit:.2f} "
                f"attempts fit inside reassembly_idle_timeout "
                f"({self.reassembly_idle_timeout_s:.1f}s), fewer than this node's own "
                f"worst-case attempt budget of {worst_attempts} (max of direct_send_attempts="
                f"{self.direct_send_attempts}, direct_send_attempts_handshake="
                f"{self.direct_send_attempts_handshake}, direct_fragment_finish_attempts="
                f"{self.direct_fragment_finish_attempts}). The receiver can evict a "
                f"bucket while the sender is still working through that fragment's first "
                f"attempt budget -- real queueing delay and this message's other fragments only "
                f"make it worse. Fix by lowering direct_ack_timeout_routed_max/"
                f"direct_post_send_listen_max, lowering direct_send_attempts, or raising "
                f"reassembly_idle_timeout to at least "
                f"{clock_racing_attempt_s * worst_attempts:.0f}. "
                f"(incoming_quiet_defer_max_wait="
                f"{self.incoming_quiet_defer_max_wait_s if self.incoming_quiet_defer_enabled else 0:.1f}s "
                f"is excluded above -- only a message's first transmission pays it -- but "
                f"re-broadening that gate's trigger would add it to every attempt here.)",
                RNS.LOG_WARNING,
            )

    def _loop_interval_s(self, interval_s: float, name: str) -> float:
        if interval_s >= self.MIN_LOOP_INTERVAL_S:
            return interval_s
        RNS.log(
            f"{self}: {name}={interval_s} is below the {self.MIN_LOOP_INTERVAL_S:.0f}s floor "
            f"(0 would busy-spin this interface's event loop, not disable the loop) -- "
            f"using {self.MIN_LOOP_INTERVAL_S:.0f}s.",
            RNS.LOG_WARNING,
        )
        return self.MIN_LOOP_INTERVAL_S


# ---- _observability.py ----

"""Observability: debug logging, the optional packet capture (one JSON line per handled RNS packet and per transmit attempt -- the project's primary field-diagnosis tool), the [STATS] loop, the firmware RX-log tap with its per-attempt correlation window and busy-air model, and the LoRa time-on-air model every airtime figure comes from."""




class _ObservabilityMixin:
    def _debug(self, msg: str) -> None:
        if self.debug_logs:
            RNS.log(f"{self}: {msg}", RNS.LOG_INFO)

    # -------------------------------------------------------------------
    # Packet capture (user-requested, 2026-09-15) -- off by default
    # (packet_capture_enabled). One JSON object per line (JSONL: easy to
    # tail -f, grep, or load with any per-line JSON reader) per in/out
    # RNS packet, written to packet_capture_dir (default: a
    # "packet_capture" subdirectory of this node's own RNS storage path).
    # Deliberately synchronous, unbuffered writes: this transport's own
    # real throughput ceiling (docs/reliability_engine_design.md's field
    # data: tens of bytes/sec) means packets are inherently rare relative
    # to normal disk I/O speed, so the "don't block the event loop"
    # concern that applies to CHANNEL/DIRECT sends doesn't meaningfully
    # apply here -- and flushing every line means a capture survives an
    # ungraceful process exit, which matters more for a debugging/
    # analysis tool than avoiding a sub-millisecond stall ever would.
    # -------------------------------------------------------------------

    def _open_packet_capture(self) -> None:
        try:
            capture_dir = self.packet_capture_dir
            if not capture_dir:
                base = getattr(RNS.Reticulum, "storagepath", None)
                if not base:
                    RNS.log(
                        f"{self}: packet_capture_enabled but no packet_capture_dir "
                        f"configured and no RNS storage path available -- capture "
                        f"disabled for this run.",
                        RNS.LOG_WARNING,
                    )
                    return
                capture_dir = os.path.join(base, "packet_capture")
            os.makedirs(capture_dir, exist_ok=True)
            safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in self.name)
            filename = f"capture_{safe_name}_{time.strftime('%Y%m%dT%H%M%S')}.jsonl"
            path = os.path.join(capture_dir, filename)
            self._packet_capture_file = open(path, "a", buffering=1)
            RNS.log(f"{self}: packet capture enabled -- writing to {path}", RNS.LOG_INFO)
        except Exception as exc:
            RNS.log(f"{self}: failed to open packet capture file: {exc} -- capture disabled for this run.", RNS.LOG_WARNING)
            self._packet_capture_file = None

    def _close_packet_capture(self) -> None:
        with self._packet_capture_lock:
            if self._packet_capture_file is not None:
                try:
                    self._packet_capture_file.close()
                except Exception:
                    pass
                self._packet_capture_file = None

    def _capture_event(self, direction: str, fields: dict) -> None:
        """Appends one capture record. `direction` is "in" or "out";
        `fields` carries everything call-site-specific (see
        `_capture_outgoing`/`_capture_incoming`). Every record also gets
        a monotonically increasing `seq` (this process's own capture
        sequence, for unambiguous ordering independent of timestamp
        resolution) and both wall-clock and monotonic timestamps (the
        latter safe against system clock adjustments mid-capture)."""
        if self._packet_capture_file is None:
            return
        with self._packet_capture_lock:
            if self._packet_capture_file is None:
                return
            self._packet_capture_seq += 1
            record = {
                "seq": self._packet_capture_seq,
                "ts": time.time(),
                "ts_monotonic": time.monotonic(),
                "direction": direction,
                **fields,
            }
            try:
                self._packet_capture_file.write(json.dumps(record, default=str) + "\n")
            except Exception as exc:
                RNS.log(f"{self}: packet capture write failed: {exc}", RNS.LOG_WARNING)

    def _header_capture_fields(self, header: Optional[_RnsHeader]) -> dict:
        if header is None:
            return {
                "packet_type": None, "packet_type_name": None,
                "destination_type": None, "destination_type_name": None,
                "destination_hash": None, "context": None, "context_name": None,
                "header_type": None,
            }
        return {
            "packet_type": header.packet_type,
            "packet_type_name": self._PACKET_TYPE_NAMES.get(header.packet_type),
            "destination_type": header.destination_type,
            "destination_type_name": self._DESTINATION_TYPE_NAMES.get(header.destination_type),
            "destination_hash": header.destination_hash.hex() if header.destination_hash else None,
            "context": header.context,
            "context_name": self._CONTEXT_NAMES.get(header.context) if header.context is not None else None,
            "header_type": header.header_type,
        }

    def _payload_correlation_hash(self, data: bytes) -> str:
        """Field-data-analysis fix (2026-09-17): a short, non-cryptographic
        (for this purpose) identifier for `data`, added to every packet
        capture record that carries a payload. Analyzing the previous
        field test's phantom-ACK pattern required cross-referencing a
        sender's capture against a receiver's by `pkt_id` -- which only
        exists for DIRECT-*fragmented* sends; a bare (single-message)
        DIRECT or CHANNEL send had no correlator at all across separate
        capture files, making that whole analysis blind to roughly half
        of real traffic. `RNS.Identity.truncated_hash` is reused here
        (same primitive `_compute_truncated_hash` already uses) purely as
        a convenient, already-available hash -- this has no security role
        and is never compared against anything at runtime, only read back
        by an analysis script joining two nodes' capture files on this
        field, alongside `pkt_id` where that also exists."""
        return RNS.Identity.truncated_hash(data).hex()[:12]

    def _capture_outgoing(
        self, header: Optional[_RnsHeader], data: bytes, decision: str,
        target_peer: Optional[str] = None, candidate_peers: Optional[list] = None,
    ) -> None:
        if self._packet_capture_file is None:
            return
        # 2026-09-20: a path request's REQUESTED destination (the first 16
        # bytes of its data), so a capture can say which path RNS asked for
        # -- until now only the shared PLAIN path.request hash was recorded.
        requested = None
        if header is not None and header.packet_type == RNS.Packet.DATA and header.destination_type == RNS.Destination.PLAIN:
            requested = self._path_request_target(data, header)
        self._capture_event("out", {
            **self._header_capture_fields(header),
            "priority": self._priority_tier(header),
            "size_bytes": len(data),
            "payload_hash": self._payload_correlation_hash(data),
            "routing_decision": decision,
            "target_peer": target_peer,
            "candidate_peers": candidate_peers,
            "small_mesh_mode": self._in_small_mesh_mode(),
            "bound_peers": len(self._peers),
            "requested_hash": requested.hex() if requested else None,
        })

    def _capture_incoming(
        self, data: bytes, transport: str,
        sender_peer_prefix: Optional[str] = None, channel_sender_claimed: Optional[str] = None,
        frag_total: Optional[int] = None, pkt_id: Optional[int] = None,
    ) -> None:
        if self._packet_capture_file is None:
            return
        header = self._parse_rns_header(data)
        # User-requested (2026-09-16, for hop-based tuning-profile
        # analysis): this interface's own resolved outbound path length to
        # `sender_peer_prefix`, when one is known -- the best hop-count
        # proxy available here, since MeshCore never reports the actual
        # inbound path a given DIRECT frame took, only what this node has
        # resolved for sending back. Never set for CHANNEL (no sender
        # identity to resolve against at all).
        resolved = self._resolved_paths.get(sender_peer_prefix) if sender_peer_prefix else None
        self._capture_event("in", {
            **self._header_capture_fields(header),
            "priority": self._priority_tier(header),
            "size_bytes": len(data),
            "transport": transport,
            # Only ever set for DIRECT -- cryptographically tied to a
            # bound peer via the firmware's own per-contact shared
            # secret. Never conflate with channel_sender_claimed below.
            "sender_peer_prefix": sender_peer_prefix,
            # CHANNEL's own adv_name, unauthenticated and attacker-
            # controlled (this interface's security model: CHANNEL
            # carries no identity at all) -- included for visibility
            # only, never treated as a real sender identity anywhere
            # else in this interface.
            "channel_sender_claimed": channel_sender_claimed,
            "frag_total": frag_total,
            "pkt_id": pkt_id,
            "hop_count": resolved.out_path_len if resolved is not None else None,
            "payload_hash": self._payload_correlation_hash(data),
        })

    def _capture_fragment_received(
        self, mode: str, sender_token: str, pkt_id: int, frag_idx: int, frag_total: int, progress: int,
        raw: bool = False,
    ) -> None:
        """Field-data-analysis fix (2026-09-17): one record per individual
        fragment actually added to a reassembly bucket, not just the
        single record `_capture_incoming` emits once the whole message
        completes. The previous field test's phantom-ACK analysis (see
        `_check_remote_completion`'s own docstring for the case that
        prompted this) could only tell a fragment "arrived by such-and-
        such a time" from the bucket's *completion* timestamp -- there
        was no way to see when frag_idx 0 specifically showed up relative
        to the sender's own retry attempts for it, only that the whole
        bucket was done by some later point. This closes that gap
        directly: `mode`/`sender_token`/`pkt_id`/`frag_total` match
        `_reassembly_key`'s own tuple exactly, so a future analysis can
        join this against the sender's `direct_attempt_result` records
        (same `pkt_id`/`frag_idx`/`frag_total`) without guessing."""
        if self._packet_capture_file is None:
            return
        self._capture_event("in", {
            "event": "fragment_received",
            "mode": mode,
            "sender_token": sender_token,
            "pkt_id": pkt_id,
            "frag_idx": frag_idx,
            "frag_total": frag_total,
            "progress": progress,
            # User-requested (2026-09-19): raw binary fragment (True) or a
            # Z85 text one (False); both share the same reassembly bucket.
            "raw": raw,
        })

    def _capture_direct_attempt_result(
        self, peer_prefix: Optional[str], attempt: int, ok: bool,
        queue_depth: int, lock_wait_s: float, ack_timeout_s: Optional[float],
        pkt_id: Optional[int] = None, frag_idx: Optional[int] = None, frag_total: Optional[int] = None,
        listen_delay_s: Optional[float] = None, hop_count: Optional[int] = None,
        time_critical: bool = False, pass_number: Optional[int] = None,
        quiet_defer_wait_s: Optional[float] = None, duty_cycle_wait_s: Optional[float] = None,
        ack_timeout_source: str = "none", ack_latency_s: Optional[float] = None,
        send_cmd_latency_s: Optional[float] = None, rx_window: Optional[dict] = None,
        medium_hold_wait_s: Optional[float] = None, miss_diagnosis: Optional[str] = None,
        medium_busy_remaining_s: Optional[float] = None, kind: Optional[str] = None,
        hop1_abort_deadline_s: Optional[float] = None, duty_cycle_exempt: bool = False,
        quiet_hold_s: Optional[float] = None, on_air_bytes: Optional[int] = None,
    ) -> None:
        """User-requested observability addition (2026-09-15, post-alpha-
        0.1.0 2-hop field test): one record per individual DIRECT send
        attempt (bare, or one fragment of a DIRECT-fragmented send) --
        the granularity the prior 2-hop investigation was missing, having
        to reconstruct attempt-by-attempt outcomes from timestamps and raw
        `send_msg` call arguments in the plain log instead. Carries an
        `event` field (absent on every existing packet in/out record) so
        analysis scripts already written against this capture format can
        distinguish these from packet records without breaking. `queue_
        depth`/`lock_wait_s` are `_direct_exchange_lock`'s own contention
        signal -- see `_direct_exchange_queue_depth`'s docstring -- direct
        evidence for or against "several concurrent messages splitting the
        one shared radio" the next time that's a live hypothesis.
        `listen_delay_s` is the post-send listen window this exact attempt
        drew -- from `direct_post_send_listen_success_min_s`/`max_s` if
        `ok`, `direct_post_send_listen_min_s`/`max_s` otherwise (2026-09-16
        split, see that constant's own comment) -- still held while
        `_direct_exchange_lock` was held -- lets a future analysis
        directly correlate how much of the channel's idle time was this
        deliberate listen window versus genuine gaps between traffic.
        `hop_count` (2026-09-16, user-requested for hop-based tuning-
        profile analysis) is this interface's own `_resolved_paths` entry
        for `peer_prefix` at the moment this attempt was made -- the same
        `out_path_len` `_fragment_spacing_range` itself keys spacing off
        of -- threaded down from whichever `resolved` the caller
        (`_send_direct_packet`/`_send_direct_supplement`) already had in
        hand, not re-looked-up here; `None` if no path was resolved yet
        (e.g. this attempt is itself part of establishing one).

        `time_critical`/`quiet_defer_wait_s`/`duty_cycle_wait_s`/
        `pass_number` (2026-09-18, user-requested field-tuning data,
        added the same pass that fixed the incoming-quiet-defer mutual-
        reset-loop regression -- see the module docstring's 2026-09-18
        entry): previously the only way to tell how much of an attempt's
        own latency was `_pre_transmit_gate`'s two waits, versus `_direct_
        exchange_lock` queueing (`lock_wait_s`, already captured) versus
        the ACK wait itself (`ack_timeout_s`, the ceiling, not the actual
        wait -- this capture has never recorded the real wait, only what
        it was capped at), was to cross-reference separate `_debug` text
        lines by hand, exactly the manual correlation this incident's own
        diagnosis needed. `quiet_defer_wait_s`/`duty_cycle_wait_s` are
        `_pre_transmit_gate`'s actual return values for this attempt
        (`None` if `_send_direct_frame` wasn't reached at all, e.g. this
        capture is for context that never got that far -- distinct from
        `0.0`, which means the gate ran and genuinely waited nothing).
        `time_critical` is whether `_pre_transmit_gate` was told to skip
        the quiet-defer wait for this specific attempt (see `_send_
        direct_frame`'s own docstring for exactly which attempts that
        covers). `pass_number` is `0`/`1` for a fragment sent by `_send_
        direct_fragmented`'s first pass or its re-drive pass respectively,
        `None` for a bare (non-fragmented) DIRECT send, which has no pass
        structure -- lets a future analysis directly measure how often
        pass 1 actually fires and how often it then succeeds, a core
        reliability metric this capture couldn't answer before without
        reconstructing pass boundaries from `attempt`/`frag_idx` by hand."""
        if self._packet_capture_file is None:
            return
        self._capture_event("out", {
            "event": "direct_attempt_result",
            "peer_prefix": peer_prefix,
            "attempt": attempt,
            "ok": ok,
            "queue_depth": queue_depth,
            "lock_wait_s": round(lock_wait_s, 3),
            "ack_timeout_s": ack_timeout_s,
            "pkt_id": pkt_id,
            "frag_idx": frag_idx,
            "frag_total": frag_total,
            "listen_delay_s": round(listen_delay_s, 3) if listen_delay_s is not None else None,
            "hop_count": hop_count,
            "time_critical": time_critical,
            "pass_number": pass_number,
            "quiet_defer_wait_s": round(quiet_defer_wait_s, 3) if quiet_defer_wait_s is not None else None,
            "duty_cycle_wait_s": round(duty_cycle_wait_s, 3) if duty_cycle_wait_s is not None else None,
            # Step 2 (2026-09-18): measured timing and what was overheard
            # during this attempt. `ack_latency_s` is MSG_SENT -> ACK event
            # (the span the timeout actually guards); `send_cmd_latency_s`
            # is _pre_transmit_gate -> MSG_SENT (host/serial/firmware queue
            # cost, NOT airtime). `ack_timeout_source` says which estimate
            # governed the wait. The `rx_*` fields are the correlation
            # window -- offsets in seconds after MSG_SENT.
            "ack_timeout_source": ack_timeout_source,
            "ack_latency_s": round(ack_latency_s, 3) if ack_latency_s is not None else None,
            "send_cmd_latency_s": round(send_cmd_latency_s, 3) if send_cmd_latency_s is not None else None,
            **self._rtt_capture_fields(peer_prefix),
            "rx_echo_seen_s": rx_window.get("echo_seen_s") if rx_window else None,
            "rx_echo_path_len": rx_window.get("echo_path_len") if rx_window else None,
            "rx_ack_seen_on_air_s": rx_window.get("ack_seen_on_air_s") if rx_window else None,
            "rx_path_reply_seen_s": rx_window.get("path_reply_seen_s") if rx_window else None,
            "rx_foreign_count": rx_window.get("foreign_rx_count") if rx_window else None,
            "rx_foreign": rx_window.get("foreign_rx") if rx_window else None,
            # Step 4 (2026-09-18): what the hold model did (medium_hold_
            # wait_s, only non-zero when rx_log_holds_enabled) and what it
            # concluded (always computed): the post-miss diagnosis and how
            # long the air was predicted to stay busy when this attempt
            # finished -- so a capture with holds OFF still shows what
            # they would have done.
            "medium_hold_wait_s": round(medium_hold_wait_s, 3) if medium_hold_wait_s is not None else None,
            "miss_diagnosis": miss_diagnosis,
            "medium_busy_remaining_s": round(medium_busy_remaining_s, 3) if medium_busy_remaining_s is not None else None,
            "rx_log_holds_enabled": self.rx_log_holds_enabled,
            # Code review (2026-09-18): None for an "R" (RNS payload)
            # frame; "completion_answer" for a "Q" ANSWER, which now goes
            # through the same ACK-waited path -- so an analysis can keep
            # its per-attempt reliability stats to real payload frames.
            "kind": kind,
            # Field fix (2026-09-18 evening): the armed early-abort
            # deadline for this attempt (None = not armed); when it fired,
            # ack_timeout_source is "hop1_abort" and ack_timeout_s equals
            # this. "expired" means the packet aged out before transmit.
            "hop1_abort_deadline_s": round(hop1_abort_deadline_s, 3) if hop1_abort_deadline_s is not None else None,
            # User-requested (2026-09-18 evening): handshake-class frames
            # skip the duty-cycle wait (airtime still charged).
            "duty_cycle_exempt": duty_cycle_exempt,
            # Field fix (2026-09-19 night): how long this attempt kept the
            # radio lock AFTER its listen delay waiting for the answer it
            # asked for -- the hidden-node quiet window, non-null only on a
            # reconcile QUERY. See _send_direct_frame_and_wait_for_ack.
            "quiet_hold_s": round(quiet_hold_s, 3) if quiet_hold_s is not None else None,
            # 2026-09-20 (airtime pass): the frame's estimated on-air size,
            # None for an attempt that never keyed the radio.
            "on_air_bytes": on_air_bytes,
        })

    def _capture_channel_fragment_sent(
        self, pkt_id: int, attempt: int, frag_idx: int, frag_total: int, position: int,
        ok: bool, size_bytes: int, on_air_bytes: Optional[int] = None,
    ) -> None:
        """Observability addition (2026-09-18, user-requested field-tuning
        data): one record per individual CHANNEL fragment transmit
        attempt, the sender-side half of a gap this capture format already
        had on the receive side -- `_capture_fragment_received` (mode=
        "channel") has recorded every fragment a receiver actually got
        since 2026-09-17, but there was no equivalent for what the sender
        believed it sent, or in what shuffled order/attempt, or whether
        the local `send_chan_msg` command itself even succeeded. Without
        this, telling "the fragment was never sent" apart from "it was
        sent but never arrived" required cross-referencing the plain
        `_debug` text log by hand. `position` is this fragment's index
        within THIS pass's own shuffled send order (`_send_channel_
        multifragment_pass`'s own `order` list) -- distinct from `frag_idx`,
        the fragment's fixed logical index within the reassembled message --
        so a future analysis can check whether shuffle position correlates
        with loss the way `docs/reliability_engine_design.md` §2's
        position-dependent loss pattern predicts. `ok` reflects only the
        local `send_chan_msg` command outcome (CHANNEL has no ACK at all,
        so this can never confirm the fragment was actually heard over the
        air -- see `_send_channel_multifragment_pass`'s own comment on
        why a local failure doesn't stop the rest of the pass)."""
        if self._packet_capture_file is None:
            return
        self._capture_event("out", {
            "event": "channel_fragment_sent",
            "pkt_id": pkt_id,
            "attempt": attempt,
            "frag_idx": frag_idx,
            "frag_total": frag_total,
            "position": position,
            "ok": ok,
            "size_bytes": size_bytes,
            "on_air_bytes": on_air_bytes,
        })

    def _capture_direct_send_result(
        self, peer_prefix: str, destination_hash: Optional[bytes], ok: bool,
        resolved: "_ResolvedPath", size_bytes: int,
        method: Optional[str] = None, fallback_from_raw: bool = False,
        slot_wait_s: Optional[float] = None,
    ) -> None:
        """User-requested observability addition (2026-09-15, post-alpha-
        0.1.0 2-hop field test): one record per whole DIRECT message (every
        fragment across both passes, for a fragmented send), pairing the
        final ACKed/not-ACKed outcome with the hop path actually used --
        `out_path_len`/`out_path_hex` were previously visible only in a
        one-off RNS.log INFO line at the moment a path was freshly
        discovered, never at the moment a message using that (possibly
        long-cached) path actually succeeds or fails. This is the single
        record that answers "did this message get through, and over how
        many hops" without cross-referencing anything else."""
        if self._packet_capture_file is None:
            return
        self._capture_event("out", {
            "event": "direct_send_result",
            "peer_prefix": peer_prefix,
            "destination_hash": destination_hash.hex() if destination_hash else None,
            "ok": ok,
            "out_path_len": resolved.out_path_len,
            "out_path_hex": resolved.out_path_hex,
            "size_bytes": size_bytes,
            # User-requested (2026-09-19): how the packet was carried --
            # "z85_bare" (one text frame), "z85_text" (Z85 text fragments),
            # "raw" (raw binary fragments) -- and whether the text send was
            # the fallback after a raw attempt on this same packet.
            "method": method,
            "fallback_from_raw": fallback_from_raw,
            # Second audit (2026-09-19 evening): how long this fragmented
            # send waited for one of direct_fragmented_max_in_flight slots
            # (None when the cap is off -- the default since 2026-09-19
            # night; a send that timed out waiting still goes, so this can
            # equal the whole slot budget).
            "slot_wait_s": round(slot_wait_s, 3) if slot_wait_s is not None else None,
        })

    def _capture_completion_check_result(
        self, peer_prefix: str, pkt_id: int, frag_total: int, outcome: str, complete: bool,
        stage: str = "final", timeout_s: Optional[float] = None,
        answer_version: Optional[int] = None, held: Optional[list] = None,
    ) -> None:
        """Field-data-analysis fix (2026-09-17): one record per
        `_check_remote_completion` call, so the next field test can
        directly measure how often the phantom-ACK completion check
        (added this same pass -- see that method's own docstring) fires,
        and how it resolves, rather than only being inferable after the
        fact by cross-referencing two nodes' captures by hand the way the
        original phantom-ACK case was found. `outcome` is one of
        `"send_failed"` (the QUERY itself never got out locally),
        `"timeout"` (sent, but no ANSWER arrived within `direct_
        completion_check_timeout_s`), or `"answered"` (a real ANSWER came
        back -- `complete` is only meaningful in this case)."""
        if self._packet_capture_file is None:
            return
        self._capture_event("out", {
            "event": "completion_check_result",
            "peer_prefix": peer_prefix,
            "pkt_id": pkt_id,
            "frag_total": frag_total,
            "outcome": outcome,
            "complete": complete if outcome == "answered" else None,
            # Step 3 (2026-09-18): which stage asked ("reconcile" between
            # the passes, "final" after them), the timeout actually used
            # (RTT-derived when known), and the receiver's have-list.
            "stage": stage,
            "timeout_s": round(timeout_s, 3) if timeout_s is not None else None,
            "answer_version": answer_version,
            "held": held,
        })

    # -------------------------------------------------------------------
    # Observability: periodic structured stats snapshot
    # -------------------------------------------------------------------

    async def _stats_loop(self):
        """Per `docs/interface_architecture.md`'s observability
        requirements: a periodic structured snapshot of interface state,
        queryable without correlating scattered log lines by hand, built
        in from the first milestone that connects to anything -- not
        bolted on once "the real logic" exists. M0 has little of substance
        to report yet; later milestones (queue depth, reassembly bucket
        counts, per-peer state) extend this same snapshot rather than
        inventing a second mechanism."""
        try:
            while not self.detached:
                await asyncio.sleep(self._loop_interval_s(self.stats_interval_s, "stats_interval"))
                if self.detached:
                    break
                uptime = (
                    time.time() - self._connected_since
                    if self._connected_since is not None
                    else 0.0
                )
                RNS.log(
                    f"{self} [STATS] online={self.online} "
                    f"transport={self.transport} bitrate={self.bitrate}bps "
                    f"uptime={uptime:.0f}s "
                    f"outgoing_queue_depth={self._outqueue.qsize()} "
                    f"outgoing_dropped_total={self._outgoing_dropped_total} "
                    f"incoming_dropped_total={self._incoming_dropped_total} "
                    f"reassembly_buckets_open={len(self._reassembly)} "
                    f"dedup_cache_size={len(self._dedup)} "
                    f"pending_retry_passes={len(self._background_tasks)} "
                    f"resolved_paths={len(self._resolved_paths)} "
                    f"peers_in_discovery_backoff={len(self._path_discovery_backoff_until)} "
                    f"contacts_known={len(self._mc.contacts) if self._mc is not None else 0} "
                    f"bound_peers={len(self._peers)} "
                    f"rns_tokens_learned={len(self._rns_token_peer)} "
                    f"proof_correlations_pending={len(self._proof_correlation)} "
                    f"rx_log_feed={'seen' if self._rx_log_feed_seen else ('never' if self.rx_log_observe_enabled else 'off')} "
                    f"rx_log_events_total={self._rx_log_events_total} "
                    f"rx_log_by_type={dict(self._rx_log_by_payload_type)} "
                    f"raw_fragments_rx={self._raw_fragments_received} raw_frames_ignored={self._raw_frames_ignored} "
                    f"raw_unsupported_paths={list(self._raw_unsupported_paths)} "
                    f"ack_rtt={{{', '.join(f'{p!r}: srtt={st['srtt']:.2f}s rttvar={st['rttvar']:.2f}s n={st['samples']}' for p, st in self._ack_rtt.items())}}}",
                    RNS.LOG_INFO,
                )
                # Per-bucket fragment counts/ages (docs/interface_architecture.md's
                # observability requirements ask for these specifically) --
                # debug-gated so a busy channel doesn't spam the default
                # INFO-level snapshot above.
                if self.debug_logs and self._reassembly:
                    now = time.monotonic()
                    bucket_summary = ", ".join(
                        f"{key}: {len(bucket.fragments)}/{bucket.frag_total} "
                        f"age={now - bucket.last_progress:.0f}s"
                        for key, bucket in self._reassembly.items()
                    )
                    self._debug(f"[STATS] open reassembly buckets: {bucket_summary}")
        except asyncio.CancelledError:
            pass

    def _rtt_capture_fields(self, peer_prefix: Optional[str]) -> dict:
        st = self._ack_rtt.get(peer_prefix) if peer_prefix is not None else None
        if st is None:
            return {"rtt_srtt_s": None, "rtt_rttvar_s": None, "rtt_samples": 0}
        return {
            "rtt_srtt_s": round(st["srtt"], 3),
            "rtt_rttvar_s": round(st["rttvar"], 3),
            "rtt_samples": st["samples"],
        }

    # -- Per-attempt RX-log correlation window (step 2, capture-only) -----

    def _open_rx_log_window(self, target: str) -> dict:
        """Opened by `_send_direct_frame_and_wait_for_ack` just before it
        keys the radio, under `_direct_exchange_lock`, so exactly one
        window is ever open. `_on_rx_log_data` sorts every overheard
        packet into it while it's open (see `_classify_rx_log_for_window`).
        Sender/target identity is matched on MeshCore's 1-byte routing
        hashes (first byte of each pubkey) -- what the cleartext carries,
        so a hash-byte collision with an unrelated node on a big mesh can
        mislabel a foreign packet as ours; acceptable for capture-only
        data, and flagged here so nobody later promotes this to a routing
        decision without adding a stronger check."""
        own = self._own_pubkey_prefix() or ""
        window = {
            "opened_at": time.monotonic(),
            "tx_at": None,                # stamped once send_msg returns MSG_SENT
            "own_hash_byte": own[:2].lower() if own else None,
            "target_hash_byte": (target or "")[:2].lower() or None,
            "expected_ack": None,         # hex, set once MSG_SENT returns it
            "echo_seen_s": None,          # our own frame re-heard (repeater forwarded it)
            "echo_path_len": None,
            "ack_seen_on_air_s": None,    # bare ACK with our expected_ack code
            "path_reply_seen_s": None,    # PATH from target to us (the flood-mode ACK carrier)
            "foreign_rx_count": 0,
            "foreign_rx": [],             # up to _RX_LOG_WINDOW_FOREIGN_CAP (typename, route, path_len, t, src_hash)
        }
        self._rx_log_window = window
        return window

    def _close_rx_log_window(self, window: dict) -> dict:
        if self._rx_log_window is window:
            self._rx_log_window = None
        return window

    def _classify_rx_log_for_window(self, fields: dict, now: float) -> None:
        """Sorts one overheard packet (already reduced to capture fields)
        into the open window. Offsets are relative to our own transmit
        (`tx_at`, falling back to when the window opened if MSG_SENT
        hasn't returned yet)."""
        w = self._rx_log_window
        if w is None:
            return
        ref = w["tx_at"] if w["tx_at"] is not None else w["opened_at"]
        t = round(now - ref, 3)
        ptype = fields.get("payload_type")
        src, dst = fields.get("src_hash"), fields.get("dst_hash")
        if ptype == self._RX_LOG_PAYLOAD_TYPE_ACK and w["expected_ack"] and fields.get("ack_code") == w["expected_ack"]:
            if w["ack_seen_on_air_s"] is None:
                w["ack_seen_on_air_s"] = t
            return
        if (
            ptype == self._RX_LOG_PAYLOAD_TYPE_TEXT_MSG
            and src == w["own_hash_byte"] and dst == w["target_hash_byte"]
        ):
            # Our own radio never logs its own transmission, so a TEXT_MSG
            # from us to the target heard during our own attempt is a
            # repeater forwarding it -- the "did hop 1 happen" signal.
            if w["echo_seen_s"] is None:
                w["echo_seen_s"] = t
                w["echo_path_len"] = fields.get("path_len")
            return
        if (
            ptype == self._RX_LOG_PAYLOAD_TYPE_PATH
            and src == w["target_hash_byte"] and dst == w["own_hash_byte"]
        ):
            if w["path_reply_seen_s"] is None:
                w["path_reply_seen_s"] = t
            return
        w["foreign_rx_count"] += 1
        if len(w["foreign_rx"]) < self._RX_LOG_WINDOW_FOREIGN_CAP:
            w["foreign_rx"].append([fields.get("payload_typename"), fields.get("route_typename"), fields.get("path_len"), t, src])

    def _subscribe_rx_log_events(self) -> None:
        """Observe-only tap on the firmware's raw-RX log feed (2026-09-18,
        see `rx_log_observe_enabled`'s own comment for what the feed is
        and why it matters). Deliberately NOT in REQUIRED_EVENT_TYPES: an
        older `meshcore` library without `RX_LOG_DATA` just loses this
        observability, it doesn't lose the interface -- probed with
        `hasattr` per the library-contract rule ("never assume these
        names, always probe") and logged once either way, so a capture
        with no `rx_log` records can be told apart from a mesh that was
        genuinely silent."""
        if not self.rx_log_observe_enabled:
            RNS.log(f"{self}: raw-RX log observation disabled by config (rx_log_observe_enabled=no).", RNS.LOG_INFO)
            return
        if not hasattr(self._EventType, "RX_LOG_DATA"):
            RNS.log(
                f"{self}: installed meshcore library's EventType has no RX_LOG_DATA "
                f"member -- raw-RX log observation unavailable with this library "
                f"version (interface continues without it).",
                RNS.LOG_WARNING,
            )
            return
        self._mc_ready.subscribe(self._EventType.RX_LOG_DATA, self._on_rx_log_data)
        RNS.log(
            f"{self}: subscribed to the firmware's raw-RX log feed (RX_LOG_DATA) -- "
            f"observe-only; overheard packets are counted in [STATS] and, when "
            f"packet capture is on, recorded as 'rx_log' events.",
            RNS.LOG_INFO,
        )

    # -- Step 4 (2026-09-18): airtime model and predicted-busy holds -------

    def _estimate_airtime_s(self, nbytes: int) -> float:
        """LoRa time-on-air for an `nbytes` physical frame at the radio's
        own SF/BW/CR (explicit header, CRC on, low-data-rate optimisation
        when the symbol time exceeds 16ms, preamble 32 symbols at SF<=8
        else 16 -- the last two exactly as the firmware's
        `RadioLibWrapper::preambleLengthForSF`/RadioLib configure them).
        Falls back to a bitrate estimate when SELF_INFO hasn't provided
        radio params. Zero-hop check at SF7/BW62.5/CR8: a ~100-byte
        TEXT_MSG comes out at ~0.58s and an ACK at ~0.14s, consistent with
        the measured 1.03-1.25s ACK RTT once ~0.3-0.5s of firmware/host
        turnaround is added."""
        if self._radio_params is None:
            bitrate = max(1, self.duty_cycle_estimate_bitrate)
            return (max(1, nbytes) * 8) / bitrate
        sf, bw_khz, cr = self._radio_params
        tsym = (2 ** sf) / (bw_khz * 1000.0)
        n_preamble = 32 if sf <= 8 else 16
        t_preamble = (n_preamble + 4.25) * tsym
        de = 1 if tsym > 0.016 else 0
        num = 8 * max(1, nbytes) - 4 * sf + 28 + 16
        den = 4 * (sf - 2 * de)
        payload_symbols = 8 + max(0, -(-num // den)) * cr
        return t_preamble + payload_symbols * tsym

    def _estimate_tx_airtime_s(self, frame: str, path_len: int = 0, on_air_bytes: Optional[int] = None) -> float:
        """Airtime of one of this node's own `send_msg`/`send_chan_msg`
        frames: the LoRa time-on-air model (`_estimate_airtime_s`) over
        the frame's real on-air size per the framing above, when the
        radio's SF/BW/CR are known; else the flat bitrate estimate that
        the duty-cycle limiter used before 2026-09-18 (evening). `path_len`
        is the routed path's byte count (0 for zero-hop) -- one byte per
        hop, so a caller without it loses almost nothing by omitting it."""
        if on_air_bytes is not None:
            # A raw packet: header + path + payload, no text framing at all.
            if self._radio_params is None:
                return (on_air_bytes * 8) / max(1, self.duty_cycle_estimate_bitrate)
            return self._estimate_airtime_s(on_air_bytes)
        if self._radio_params is None:
            return (len(frame) * 8) / max(1, self.duty_cycle_estimate_bitrate)
        return self._estimate_airtime_s(self._text_frame_on_air_bytes(frame, path_len))

    def _text_frame_on_air_bytes(self, frame: str, path_len: int = 0) -> int:
        """Bytes one of this node's own TXT_MSG frames occupies on air, per
        the framing above (2026-09-20: also written to every transmit
        record as `on_air_bytes`, so a field capture can report on-air
        bytes per delivered RNS byte -- the metric of the airtime pass --
        the way `meshbench_report.py`'s ledger does from MeshBench's own
        event log). A CHANNEL frame's firmware framing differs slightly
        (channel hash instead of dest/src hashes); the same formula is
        used as an estimate."""
        plaintext = len(frame.encode("utf-8")) + self._TXT_MSG_PLAINTEXT_OVERHEAD_BYTES
        ciphertext = -(-plaintext // 16) * 16
        return self._TXT_MSG_FIXED_OVERHEAD_BYTES + max(0, path_len) + ciphertext

    def _predicted_hold_for_rx(self, fields: dict) -> "tuple[float, str]":
        """How long the air is expected to stay busy *after* one overheard
        packet, per the model in rx_log_holds_enabled's own comment.
        Returns (seconds, reason)."""
        nbytes = fields.get("payload_length") or 0
        if nbytes <= 0:
            return 0.0, "unknown"
        ptype = fields.get("payload_type")
        route = fields.get("route_type")
        airtime = self._estimate_airtime_s(nbytes)
        if ptype in self._RX_LOG_NOTHING_FOLLOWS_TYPES and route in self._RX_LOG_ROUTE_DIRECT:
            return 0.0, "ack_or_advert_direct"
        if route in self._RX_LOG_ROUTE_FLOOD:
            # Every repeater in range re-floods it (ADVERTs and channel
            # messages included), then -- for an addressed type -- the
            # target answers, itself flooded/echoed.
            hold = self.rx_log_hold_flood_factor * airtime
            reason = "flood_echo"
            if ptype in self._RX_LOG_ACK_BEARING_TYPES:
                hold += self._estimate_airtime_s(24) + self.rx_log_hold_turnaround_s
                reason = "flood_echo+reply"
            return hold, reason
        if route in self._RX_LOG_ROUTE_DIRECT:
            hops_left = int(fields.get("path_len") or 0)
            hold = self.rx_log_hold_hop_factor * airtime * hops_left
            reason = f"direct_forward_x{hops_left}"
            if ptype in self._RX_LOG_ACK_BEARING_TYPES:
                ack_airtime = self._estimate_airtime_s(12)
                hold += ack_airtime + self.rx_log_hold_turnaround_s + self.rx_log_hold_hop_factor * ack_airtime * hops_left
                reason += "+ack"
            return hold, reason
        return 0.0, "unknown"

    def _extend_medium_busy(self, hold_s: float, reason: str, now: float) -> None:
        if hold_s <= 0:
            return
        until = now + min(hold_s, self.rx_log_hold_max_s)
        if until > self._medium_busy_until:
            self._medium_busy_until = until
            self._medium_busy_reason = reason

    def _medium_busy_remaining_s(self, now: Optional[float] = None) -> float:
        now = time.monotonic() if now is None else now
        return max(0.0, self._medium_busy_until - now)

    async def _wait_for_medium_clear(self) -> float:
        """Pre-transmit hold (step 4, only when rx_log_holds_enabled): sleep
        until `_medium_busy_until`, re-checking because packets overheard
        *during* the wait extend it -- bounded by rx_log_hold_max_s in
        total so a busy mesh can't stall a send indefinitely. Returns the
        time actually waited."""
        if not self.rx_log_holds_enabled:
            return 0.0
        waited = 0.0
        while True:
            remaining = self._medium_busy_remaining_s()
            if remaining <= 0 or waited >= self.rx_log_hold_max_s:
                break
            step = min(remaining, self.rx_log_hold_max_s - waited)
            await asyncio.sleep(max(0.01, step))
            waited += step
        if waited > 0:
            self._debug(f"medium-busy hold: waited {waited:.2f}s ({self._medium_busy_reason}) before transmitting.")
        return waited

    def _diagnose_missed_ack(self, window: Optional[dict], hop_count: Optional[int]) -> str:
        """Post-miss diagnosis from the attempt's RX window (step 2 data):
        see rx_log_holds_enabled's own comment for the four outcomes."""
        if not window:
            return "no_info"
        target = window.get("target_hash_byte")
        if target and any(len(f) >= 5 and f[4] == target for f in window.get("foreign_rx", ())):
            return "target_busy"
        if window.get("echo_seen_s") is not None:
            return "downstream_loss"
        if hop_count is not None and hop_count >= 1:
            return "hop1_loss"
        return "no_info"

    def _post_miss_hold_s(self, diagnosis: str) -> float:
        jitter = random.uniform(self.rx_log_hold_jitter_min_s, self.rx_log_hold_jitter_max_s)
        if diagnosis == "downstream_loss":
            hold = jitter
        else:
            hold = self._medium_busy_remaining_s() + jitter
        return min(hold, self.rx_log_hold_max_s)

    def _on_rx_log_data(self, event) -> None:
        """One firmware-decoded packet heard on air, addressed to this node
        or not. Runs on this interface's own event loop (the `meshcore`
        dispatcher calls sync subscribers inline), so it must stay cheap:
        bump counters, stamp the last-heard time, write one capture
        record. Never raises -- a malformed/unparseable log frame is the
        library's problem to have already tolerated (reader.py populates
        sentinel fields), and nothing here is allowed to take the data
        path down."""
        if self.detached:
            return
        try:
            payload = event.payload if isinstance(event.payload, dict) else {}
            now = time.monotonic()
            since_last_rx = (now - self._last_rx_log_at) if self._last_rx_log_at is not None else None
            since_own_tx = (now - self._last_own_tx_at) if self._last_own_tx_at is not None else None
            self._last_rx_log_at = now
            self._rx_log_feed_seen = True
            self._rx_log_events_total += 1
            payload_typename = self._rx_log_typename(payload)
            self._rx_log_by_payload_type[payload_typename] += 1

            fields = self._rx_log_capture_fields(payload, since_last_rx, since_own_tx)
            hold_s, hold_reason = self._predicted_hold_for_rx(fields)
            self._extend_medium_busy(hold_s, hold_reason, now)
            fields["predicted_hold_s"] = round(hold_s, 3)
            fields["hold_reason"] = hold_reason
            fields["medium_busy_remaining_s"] = round(self._medium_busy_remaining_s(now), 3)
            if self._rx_log_window is not None:
                self._classify_rx_log_for_window(fields, now)
            if self._packet_capture_file is None and not self.debug_logs:
                return
            if self._packet_capture_file is not None:
                self._capture_event("in", fields)
            self._debug(
                f"rx_log: {fields.get('payload_typename')}/{fields.get('route_typename')} "
                f"len={fields.get('payload_length')} path_len={fields.get('path_len')} "
                f"snr={fields.get('snr')} rssi={fields.get('rssi')} "
                f"src={fields.get('src_hash')} dst={fields.get('dst_hash')} ack={fields.get('ack_code')} "
                f"since_last_rx={since_last_rx if since_last_rx is None else round(since_last_rx, 2)}s "
                f"since_own_tx={since_own_tx if since_own_tx is None else round(since_own_tx, 2)}s"
            )
        except Exception as exc:
            self._debug(f"rx_log handler error (ignored): {exc}")

    def _rx_log_typename(self, payload: dict) -> str:
        """The library's name table stops at CONTROL (11), so a raw packet
        (PAYLOAD_TYPE_RAW_CUSTOM, 0x0F) is reported as "UNK"; name it."""
        name = payload.get("payload_typename")
        if payload.get("payload_type") == self._RX_LOG_PAYLOAD_TYPE_RAW_CUSTOM and (not name or name == "UNK"):
            return "RAW_CUSTOM"
        return str(name if name is not None else "UNK")

    def _rx_log_capture_fields(self, payload: dict, since_last_rx: Optional[float], since_own_tx: Optional[float]) -> dict:
        """The `rx_log` capture record. Everything the firmware/library
        already decoded from the packet's *cleartext* (a DIRECT/CHANNEL
        payload body itself is encrypted end-to-end by the firmware, so
        this interface's own "R"/"P"/"Q" headers are never visible here
        for traffic that isn't ours -- that's fine, the MeshCore-level
        route/type/path is exactly what a third party needs), plus the
        two relative timings that make burst structure readable straight
        off the capture. `dst_hash`/`src_hash` are the 1-byte routing
        hashes MeshCore puts at the front of an addressed payload
        (Dispatcher.cpp's own RX log prints them as `[src -> dst]`);
        `ack_code` is an ACK's 4-byte code, the same value a MSG_SENT
        `expected_ack` carries, so a later analysis can match ACKs seen
        on air against ACK events this node was (or wasn't) delivered.
        `pkt_hash` is the library's hash of the payload -- the same packet
        seen twice in quick succession with a longer `path` is a repeater
        echo."""
        pkt_payload = payload.get("pkt_payload")
        if not isinstance(pkt_payload, (bytes, bytearray)):
            pkt_payload = b""
        payload_type = payload.get("payload_type")
        dst_hash = src_hash = ack_code = None
        if payload_type in self._RX_LOG_ADDRESSED_PAYLOAD_TYPES and len(pkt_payload) >= 2:
            dst_hash, src_hash = f"{pkt_payload[0]:02x}", f"{pkt_payload[1]:02x}"
        elif payload_type == self._RX_LOG_PAYLOAD_TYPE_ACK and len(pkt_payload) >= 4:
            ack_code = bytes(pkt_payload[:4]).hex()
        return {
            "event": "rx_log",
            "snr": payload.get("snr"),
            "rssi": payload.get("rssi"),
            "route_type": payload.get("route_type"),
            "route_typename": payload.get("route_typename"),
            "payload_type": payload_type,
            "payload_typename": self._rx_log_typename(payload),
            "payload_ver": payload.get("payload_ver"),
            "path_len": payload.get("path_len"),
            "path": payload.get("path"),
            "payload_length": payload.get("payload_length"),
            "pkt_hash": payload.get("pkt_hash"),
            "dst_hash": dst_hash,
            "src_hash": src_hash,
            "ack_code": ack_code,
            "since_last_rx_log_s": round(since_last_rx, 3) if since_last_rx is not None else None,
            "since_own_tx_s": round(since_own_tx, 3) if since_own_tx is not None else None,
            "rx_log_events_total": self._rx_log_events_total,
        }


# ---- _wire.py ----

"""Wire format: payload budgets, the R / P / Q text-frame encoders and decoders, the raw binary fragment header, payload chunking and spacing tiers, and the RNS header parse plus the packet classifications derived from it (priority tier, link class, handshake class, plain proof, path-request target). Every encoded byte is pinned by tests/golden/wire_format.json."""




class _WireFormatMixin:
    # -------------------------------------------------------------------
    # Wire format (docs/wire_format_design.md): payload budgets and
    # frame encode/decode
    # -------------------------------------------------------------------

    def _payload_budget(self, budget: int, header_size: int) -> int:
        """The general form from wire_format_design.md's payload-budget
        section, re-derived from the Z85 sizing relationship. `MARKER`'s
        length is deliberately a symbolic term here, never a bare literal
        -- the bug that section's own history warns against is a future
        marker-length change silently desyncing a hard-coded constant
        from the format it's supposed to be sizing."""
        raw = ((budget - len(self.MARKER) - 1) // 5) * 4 - header_size - self.PAYLOAD_MARGIN
        return max(0, raw)

    def _channel_text_budget(self) -> int:
        # "<name>: " is the firmware's own mandatory CHANNEL prefix
        # (meshcore_protocol_rules.md CHANNEL rule 2) -- eats into the
        # budget regardless of what this interface puts in the message,
        # or which of CHANNEL's two header shapes ends up using it.
        return self.FIRMWARE_TEXT_LIMIT - len(self._own_node_name) - 2

    def _channel_payload_budget(self) -> int:
        return self._payload_budget(self._channel_text_budget(), self.CHANNEL_FASTPATH_HEADER_SIZE)

    def _channel_multifragment_payload_budget(self) -> int:
        # Per-fragment usable payload when frag_total > 1 -- smaller than
        # the fast-path budget above by exactly the extra 2 header bytes
        # (frag_idx, frag_total) the multi-fragment shape carries.
        return self._payload_budget(self._channel_text_budget(), self.MULTI_FRAGMENT_HEADER_SIZE)

    def _direct_payload_budget(self) -> int:
        # DIRECT text framing has no name-prefix cost at all
        # (meshcore_protocol_rules.md DIRECT rule 1) -- full firmware
        # limit is available.
        return self._payload_budget(self.FIRMWARE_TEXT_LIMIT, self.DIRECT_BARE_HEADER_SIZE)

    def _direct_multifragment_payload_budget(self) -> int:
        # Per-fragment usable payload for DIRECT's needs-fragmenting shape
        # (Milestone 6) -- same MULTI_FRAGMENT_HEADER_SIZE as CHANNEL's
        # multi-fragment shape (wire_format_design.md: "identical shape to
        # the CHANNEL one above"), but no name-prefix cost, same as the
        # bare DIRECT budget above.
        return self._payload_budget(self.FIRMWARE_TEXT_LIMIT, self.MULTI_FRAGMENT_HEADER_SIZE)

    def _encode_channel_fastpath(self, payload: bytes, pkt_id: int, attempt: int = 0) -> str:
        header = bytes([self.PROTOCOL_VERSION]) + pkt_id.to_bytes(2, "big") + bytes([attempt])
        return self.MARKER + _z85_encode(header + payload)

    def _encode_channel_multifragment(
        self, payload: bytes, pkt_id: int, frag_idx: int, frag_total: int, attempt: int = 0
    ) -> str:
        header = (
            bytes([self.PROTOCOL_VERSION | self.FLAG_MULTI_FRAGMENT])
            + pkt_id.to_bytes(2, "big")
            + bytes([frag_idx, frag_total, attempt])
        )
        return self.MARKER + _z85_encode(header + payload)

    def _encode_direct_bare(self, payload: bytes) -> str:
        header = bytes([self.PROTOCOL_VERSION])
        return self.MARKER + _z85_encode(header + payload)

    def _decode_frame(self, marker_and_body: str, mode: str) -> "tuple[_FrameHeader, bytes]":
        """Decodes one CHANNEL or DIRECT frame's header (`mode` is
        "channel" or "direct" -- determines the fast-path/bare header
        shape when the multi-fragment bit is clear; the multi-fragment
        shape itself is identical between the two modes). Raises
        ValueError on anything malformed -- a missing marker, invalid
        Z85, an unsupported version, or a frame too short for the header
        its own flag bits claim. Per wire_format_design.md's own
        reasoning for the 1-character marker: a false-positive marker
        match on ordinary chat traffic is expected to happen sometimes,
        and costs exactly one cheap, local decode-and-reject here -- the
        caller logs and drops on ValueError, nothing more."""
        if not marker_and_body.startswith(self.MARKER):
            raise ValueError("missing marker")

        raw = _z85_decode(marker_and_body[len(self.MARKER):])
        if len(raw) < 1:
            raise ValueError("empty frame after marker")

        ver_byte = raw[0]
        version_number = ver_byte & self.VERSION_MASK
        multi_fragment = bool(ver_byte & self.FLAG_MULTI_FRAGMENT)
        coop = bool(ver_byte & self.FLAG_COOP)

        if version_number != self.PROTOCOL_VERSION:
            raise ValueError(f"unsupported protocol version {version_number}")

        if multi_fragment:
            if len(raw) < self.MULTI_FRAGMENT_HEADER_SIZE:
                raise ValueError("frame too short for multi-fragment header")
            pkt_id = int.from_bytes(raw[1:3], "big")
            frag_idx = raw[3]
            frag_total = raw[4]
            attempt = raw[5]
            if frag_total < 1 or frag_idx >= frag_total:
                # Malformed or hostile (this interface's security model
                # assumes any transmitter on the shared channel can send
                # anything): reject here, at decode time, rather than let
                # it reach reassembly, where an out-of-range frag_idx set
                # that happens to satisfy len(fragments) == frag_total
                # would raise an unhandled KeyError trying to join indices
                # that were never actually stored.
                raise ValueError(
                    f"invalid frag_idx/frag_total: {frag_idx}/{frag_total}"
                )
            payload = bytes(raw[self.MULTI_FRAGMENT_HEADER_SIZE:])
            return (
                _FrameHeader(version_number, True, coop, pkt_id, frag_idx, frag_total, attempt),
                payload,
            )

        if mode == "channel":
            if len(raw) < self.CHANNEL_FASTPATH_HEADER_SIZE:
                raise ValueError("frame too short for CHANNEL fast-path header")
            pkt_id = int.from_bytes(raw[1:3], "big")
            attempt = raw[3]
            payload = bytes(raw[self.CHANNEL_FASTPATH_HEADER_SIZE:])
            return (
                _FrameHeader(version_number, False, coop, pkt_id, 0, 1, attempt),
                payload,
            )

        # mode == "direct": bare shape -- no pkt_id/attempt at all, the
        # firmware's own ACK/content-derived-attempt cycle covers it.
        payload = bytes(raw[self.DIRECT_BARE_HEADER_SIZE:])
        return _FrameHeader(version_number, False, coop, None, 0, 1, None), payload

    # -- Bind frames (docs/peer_discovery_design.md §1) -- a separate
    # control protocol, distinct from the "R"-marker RNS wire format above:
    # different marker, no relationship to _decode_frame's shapes.

    def _bind_capability(self) -> int:
        cap = 0
        if self.declares_upstream_rns:
            cap |= self.BIND_CAP_HAS_UPSTREAM_RNS
        if self.direct_raw_fragments_enabled:
            cap |= self.BIND_CAP_RAW_FRAGMENTS
        return cap

    def _own_pubkey_prefix(self) -> Optional[str]:
        if len(self._own_pubkey_hex) < self.BIND_PUBKEY_PREFIX_BYTES * 2:
            return None
        return self._own_pubkey_hex[: self.BIND_PUBKEY_PREFIX_BYTES * 2]

    def _encode_bind_frame(self, frame_type: int, attempt: int) -> str:
        own_prefix = self._own_pubkey_prefix()
        prefix_bytes = (
            bytes.fromhex(own_prefix) if own_prefix is not None
            else b"\x00" * self.BIND_PUBKEY_PREFIX_BYTES
        )
        body = (
            bytes([self.BIND_PROTOCOL_VERSION, frame_type, self._bind_capability(), attempt & 0xFF])
            + prefix_bytes
        )
        return self.PEER_MARKER + _z85_encode(body)

    def _decode_bind_frame(self, marker_and_body: str) -> _BindFrame:
        if not marker_and_body.startswith(self.PEER_MARKER):
            raise ValueError("missing bind-frame marker")
        raw = _z85_decode(marker_and_body[len(self.PEER_MARKER):])
        if len(raw) != self.BIND_FRAME_RAW_SIZE:
            raise ValueError(f"bind frame wrong length: {len(raw)} (expected {self.BIND_FRAME_RAW_SIZE})")

        version, frame_type, cap, attempt = raw[0], raw[1], raw[2], raw[3]
        if version != self.BIND_PROTOCOL_VERSION:
            raise ValueError(f"unsupported bind-frame version {version}")
        if frame_type not in (self.BIND_TYPE_REQUEST, self.BIND_TYPE_RESPONSE):
            raise ValueError(f"unrecognized bind-frame type {frame_type}")

        pubkey_prefix = raw[4:4 + self.BIND_PUBKEY_PREFIX_BYTES].hex()
        return _BindFrame(version=version, type=frame_type, cap=cap, attempt=attempt, pubkey_prefix=pubkey_prefix)

    # --- DIRECT-fragmented completion check ("Q" marker) -----------------
    # Own control protocol, distinct from both "R" (RNS wire format) and
    # "P" (bind frames): different marker, no relationship to either's
    # shape.

    @staticmethod
    def _completion_bitmap_size(frag_total: int) -> int:
        return (max(0, frag_total) + 7) // 8

    def _encode_completion_frame(
        self, frame_type: int, pkt_id: int, frag_total: int, complete: bool = False,
        held: "Optional[set]" = None, version: Optional[int] = None,
        nonce: Optional[int] = None,
    ) -> str:
        """`version` defaults to this build's own (v2). Passing
        `COMPLETION_PROTOCOL_VERSION_V1` produces the pre-step-3 fixed-body
        frame -- used to answer a v1 QUERY in kind. `held` is only encoded
        on a v2 ANSWER; `complete` is carried by both versions (redundant
        with an all-ones bitmap on v2, kept so a v2 reader never has to
        infer it)."""
        if version is None:
            version = self.COMPLETION_PROTOCOL_VERSION
        body = bytes([
            version,
            frame_type,
            1 if complete else 0,
            (pkt_id >> 8) & 0xFF,
            pkt_id & 0xFF,
            frag_total & 0xFF,
        ])
        if version >= 3:
            body += bytes([(nonce or 0) & 0xFF])
        if version >= 2 and frame_type == self.COMPLETION_TYPE_ANSWER:
            bitmap = bytearray(self._completion_bitmap_size(frag_total))
            for idx in (held or ()):
                if 0 <= idx < frag_total:
                    bitmap[idx // 8] |= 1 << (idx % 8)
            body += bytes(bitmap)
        return self.COMPLETION_MARKER + _z85_encode(body)

    def _decode_completion_frame(self, marker_and_body: str) -> _CompletionFrame:
        if not marker_and_body.startswith(self.COMPLETION_MARKER):
            raise ValueError("missing completion-frame marker")
        raw = _z85_decode(marker_and_body[len(self.COMPLETION_MARKER):])
        if len(raw) < self.COMPLETION_FRAME_RAW_SIZE:
            raise ValueError(f"completion frame too short: {len(raw)} (expected >= {self.COMPLETION_FRAME_RAW_SIZE})")

        version, frame_type, complete_byte = raw[0], raw[1], raw[2]
        if version not in (
            self.COMPLETION_PROTOCOL_VERSION_V1, self.COMPLETION_PROTOCOL_VERSION_V2,
            self.COMPLETION_PROTOCOL_VERSION,
        ):
            raise ValueError(f"unsupported completion-frame version {version}")
        if frame_type not in (self.COMPLETION_TYPE_QUERY, self.COMPLETION_TYPE_ANSWER):
            raise ValueError(f"unrecognized completion-frame type {frame_type}")

        pkt_id = (raw[3] << 8) | raw[4]
        frag_total = raw[5]
        held = None
        nonce = None
        body_size = self.COMPLETION_FRAME_RAW_SIZE
        if version >= 3:
            if len(raw) < body_size + 1:
                raise ValueError("v3 completion frame too short for its nonce")
            nonce = raw[body_size]
            body_size += 1
        if version >= 2 and frame_type == self.COMPLETION_TYPE_ANSWER:
            expected = body_size + self._completion_bitmap_size(frag_total)
            if len(raw) != expected:
                raise ValueError(f"completion ANSWER wrong length: {len(raw)} (expected {expected} for frag_total={frag_total})")
            bitmap = raw[body_size:]
            held = frozenset(i for i in range(frag_total) if bitmap[i // 8] & (1 << (i % 8)))
        elif len(raw) != body_size:
            raise ValueError(f"completion frame wrong length: {len(raw)} (expected {body_size})")
        return _CompletionFrame(
            version=version, type=frame_type, complete=bool(complete_byte),
            pkt_id=pkt_id, frag_total=frag_total, held=held, nonce=nonce,
        )

    # --- Raw binary DIRECT fragments (2026-09-18 night, module docstring) ---

    def _direct_raw_payload_budget(self, path_len: int) -> int:
        """RNS payload bytes per raw fragment for a path of `path_len`
        bytes: the smaller of the configured cap, the firmware's receive
        push limit and its send-frame limit less the path, minus our
        13-byte header. 157 at zero hop with the defaults."""
        cap = min(self.direct_raw_payload_cap, self.FIRMWARE_RAW_RX_PAYLOAD_LIMIT,
                  self.FIRMWARE_RAW_TX_FRAME_LIMIT - max(0, path_len))
        return max(0, cap - self.RAW_HEADER_SIZE)

    def _encode_raw_fragment(
        self, payload: bytes, dst_pubkey_hex: str, src_prefix_hex: str,
        pkt_id: int, frag_idx: int, frag_total: int, attempt: int, report: bool = False,
    ) -> bytes:
        header = (
            bytes([(self.RAW_PROTOCOL_VERSION << 4) | (attempt & 0x03) | (self.RAW_FLAG_REPORT if report else 0)])
            + bytes.fromhex(dst_pubkey_hex[: self.RAW_DST_PREFIX_BYTES * 2])
            + bytes.fromhex(src_prefix_hex[: self.BIND_PUBKEY_PREFIX_BYTES * 2])
            + pkt_id.to_bytes(2, "big")
            + bytes([frag_idx & 0xFF, frag_total & 0xFF])
        )
        return header + payload

    def _raw_fragment_report_requested(self, raw: bytes) -> bool:
        """Whether byte 0 of a raw fragment carries RAW_FLAG_REPORT (the
        burst's last fragment, 2026-09-20). Read separately from
        `_decode_raw_fragment` so the decoder's 4-tuple contract, and the
        tests pinning it, stay unchanged."""
        return bool(raw) and bool(raw[0] & self.RAW_FLAG_REPORT)

    def _decode_raw_fragment(self, raw: bytes) -> "tuple[_FrameHeader, bytes, str, bytes]":
        """Returns (header, payload, src_prefix_hex, dst_prefix_bytes).
        Raises ValueError for anything that isn't one of ours -- callers
        drop those silently, since other applications' raw packets share
        this payload type. Bit 2 of byte 0 (RAW_FLAG_REPORT) is ignored
        here; see `_raw_fragment_report_requested`."""
        if len(raw) < self.RAW_HEADER_SIZE:
            raise ValueError("too short for a raw fragment header")
        if (raw[0] >> 4) != self.RAW_PROTOCOL_VERSION:
            raise ValueError(f"raw version nibble {raw[0] >> 4} is not ours")
        attempt = raw[0] & 0x03
        dst = raw[1:1 + self.RAW_DST_PREFIX_BYTES]
        i = 1 + self.RAW_DST_PREFIX_BYTES
        src_prefix_hex = raw[i:i + self.BIND_PUBKEY_PREFIX_BYTES].hex()
        i += self.BIND_PUBKEY_PREFIX_BYTES
        pkt_id = int.from_bytes(raw[i:i + 2], "big")
        frag_idx, frag_total = raw[i + 2], raw[i + 3]
        if frag_total < 1 or frag_idx >= frag_total:
            raise ValueError(f"invalid frag_idx/frag_total: {frag_idx}/{frag_total}")
        header = _FrameHeader(self.PROTOCOL_VERSION, True, False, pkt_id, frag_idx, frag_total, attempt)
        return header, bytes(raw[self.RAW_HEADER_SIZE:]), src_prefix_hex, bytes(dst)

    def _next_pkt_id(self) -> int:
        # Only ever called from this interface's own dedicated event loop
        # (via _send_channel, itself only invoked by _outgoing_worker
        # running on that same loop) -- no lock needed, since coroutines
        # on one asyncio loop never run concurrently with each other.
        pkt_id = self._pkt_id_counter
        self._pkt_id_counter = (self._pkt_id_counter + 1) & 0xFFFF
        return pkt_id

    def _chunk_payload(self, data: bytes, per_fragment: int) -> list:
        """Shared chunking body for `_fragment_payload`/
        `_fragment_direct_payload` below -- code-review fix: these two
        used to carry byte-identical bodies, differing only in which
        budget accessor supplied `per_fragment`, so a future change to the
        chunking algorithm itself had to be applied in two places by
        hand."""
        return [data[i:i + per_fragment] for i in range(0, len(data), per_fragment)]

    def _fragment_payload(self, data: bytes) -> list:
        """Splits `data` into chunks of at most the CHANNEL multi-fragment
        per-fragment budget. Caller (_send_channel_multifragment) already
        guarantees that budget is positive and `data` is non-empty --
        this only ever runs for a packet already established to be too
        large for the fast-path single-fragment budget."""
        return self._chunk_payload(data, self._channel_multifragment_payload_budget())

    def _fragment_direct_payload(self, data: bytes) -> list:
        """DIRECT's own sibling of `_fragment_payload` above -- same
        chunking logic, DIRECT's own (larger, no-name-prefix-cost) budget.
        Milestone 6, rare in practice (`wire_format_design.md`'s
        constraint one: everything but ANNOUNCE comfortably fits one
        DIRECT message, and ANNOUNCE never goes DIRECT in this design)."""
        return self._chunk_payload(data, self._direct_multifragment_payload_budget())

    def _fragment_spacing_range(self, hop_count: Optional[int]) -> "tuple[float, float]":
        """The tiered inter-fragment spacing rule from
        docs/reliability_engine_design.md §2. `hop_count` is `0` for a
        confirmed zero-hop (direct RF neighbor, no repeater) audience, a
        positive int for a confirmed N-hop audience, or `None` when it's
        unknown or the audience spans mixed depths with any unknown
        member -- both cases the design's own standing rule ("missing
        data always gets the conservative treatment") maps to the same
        flat fallback. For a mixed *all-known* audience, the caller is
        responsible for resolving that to a single `hop_count` first, by
        the maximum hop count present (§2's mixed-known-hop rule) --
        this method only implements per-value tier selection. Callers
        today: the DIRECT-fragmented sender passes the resolved path's
        `out_path_len`; the CHANNEL multi-fragment path passes `None`,
        since a broadcast has no single audience depth."""
        if hop_count == 0:
            return (self.fragment_delay_zero_hop_min_s, self.fragment_delay_zero_hop_max_s)
        if hop_count is not None and hop_count >= 1:
            return (
                self.fragment_delay_per_hop_min_s * hop_count,
                self.fragment_delay_per_hop_max_s * hop_count,
            )
        return (self.fragment_delay_min_s, self.fragment_delay_max_s)

    # -------------------------------------------------------------------
    # RNS packet-header classification (docs/reliability_engine_design.md
    # §3, §9) -- read-only, decrypts nothing (this interface's security
    # model: confidentiality/authentication are RNS's job one layer up).
    # -------------------------------------------------------------------

    def _parse_rns_header(self, data: bytes) -> Optional[_RnsHeader]:
        if len(data) < 2:
            return None
        flags = data[0]
        header_type = (flags & 0x40) >> 6
        packet_type = flags & 0x03
        destination_type = (flags >> 2) & 0x03
        dst_len = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
        # HEADER_2 (in-transport) packets carry a transport_id field before
        # destination_hash; HEADER_1 packets don't (confirmed directly
        # against RNS.Packet.unpack()) -- destination_hash is Milestone 5's
        # own addition, read here once, alongside the fields Milestone 3
        # already reads, rather than re-parsing this same layout a second
        # time at every routing-decision call site.
        dst_offset = (2 + dst_len) if header_type == 1 else 2
        context_offset = (2 + 2 * dst_len) if header_type == 1 else (2 + dst_len)
        destination_hash = (
            data[dst_offset:dst_offset + dst_len] if len(data) >= dst_offset + dst_len else None
        )
        context = data[context_offset] if len(data) > context_offset else None
        return _RnsHeader(
            packet_type=packet_type,
            destination_type=destination_type,
            context=context,
            header_type=header_type,
            destination_hash=destination_hash,
        )

    def _compute_truncated_hash(self, data: bytes, header_type: int) -> Optional[bytes]:
        """Replicates `RNS.Packet.get_hashable_part()`/`getTruncatedHash()`
        (confirmed byte-for-byte against a real constructed `RNS.Packet`
        while this milestone was built: `packet.generate_proof_destination()
        .hash` matched this exact computation) -- the value RNS itself
        would use as a PROOF's own destination-hash field for `data`,
        needed for §7's PROOF-correlation table. Calls the real
        `RNS.Identity.truncated_hash()` rather than re-deriving the hash
        algorithm -- only the framing (which header bytes are hashable) is
        reimplemented here, per this interface's security model (it reads
        cleartext header structure, never anything cryptographic)."""
        dst_len = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
        offset = (dst_len + 2) if header_type == 1 else 2
        if len(data) < 1 or len(data) < offset:
            return None
        hashable_part = bytes([data[0] & 0b00001111]) + data[offset:]
        return RNS.Identity.truncated_hash(hashable_part)

    def _compute_link_id(self, data: bytes) -> Optional[bytes]:
        """Code review (2026-09-18): replicates `RNS.Link.link_id_from_lr_
        packet()` for a packed LINKREQUEST `data` -- `get_hashable_part()`
        (the same framing `_compute_truncated_hash` above replicates),
        minus the trailing bytes beyond `Link.ECPUBSIZE` of packet data
        when a LINKREQUEST carries signalling bytes after the two public
        keys. Validated byte-for-byte in-process against real
        `RNS.Packet(...LINKREQUEST).pack()` + `RNS.Link.link_id_from_lr_
        packet()` for data lengths 32, 64, 66, 70 and 80 (both sides of
        the truncation branch) before being wired in. The link_id is what
        an LRPROOF, and every later packet on that Link, carries in its
        destination-hash field -- so this is the correlator that ties a
        Link back to the destination it was requested for
        (`_pending_link_requests`) and to the peer it was requested from
        (`_rns_token_peer`). Reads cleartext header structure only, like
        every other parser here."""
        if not data:
            return None
        dst_len = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
        header_type = (data[0] & 0x40) >> 6
        offset = (dst_len + 2) if header_type == 1 else 2
        data_offset = offset + dst_len + 1  # + the context byte pack() always writes
        if len(data) < data_offset:
            return None
        hashable_part = bytes([data[0] & 0b00001111]) + data[offset:]
        payload_len = len(data) - data_offset
        if payload_len > RNS.Link.ECPUBSIZE:
            hashable_part = hashable_part[:-(payload_len - RNS.Link.ECPUBSIZE)]
        return RNS.Identity.truncated_hash(hashable_part)

    def _priority_tier(self, header: Optional[_RnsHeader]) -> int:
        """docs/reliability_engine_design.md §3's original two-tier
        scheme, widened per §9 to also catch RNS's own give-up/
        connection-alive signals by context byte, not just packet type --
        a RESOURCE_ICL/RESOURCE_RCL/LINKCLOSE/KEEPALIVE/etc. packet is
        packet_type=DATA with a distinguishing context, not one of the
        LINK_REQUEST/PROOF packet *types* handled by the first check
        below. Widened again 2026-09-16 to a third tier, `PRIORITY_LOW`,
        for PATH_RESPONSE specifically -- see that constant's own
        comment for the field data behind it."""
        if header is None:
            return self.PRIORITY_NORMAL
        if header.packet_type == RNS.Packet.PROOF and not self._proof_is_link_class(header):
            # Field fix (2026-09-19 evening session, second audit): a plain
            # delivery PROOF -- the receipt RNS returns for every DATA packet
            # that asked for one -- is NOT handshake class. Nothing hangs on
            # it the way a Link hangs on its LRPROOF: if it is lost, the
            # sender's application simply retries the DATA. Yet at
            # PRIORITY_HANDSHAKE it took the 4-attempt handshake budget and
            # outranked completion ANSWERs, and the desktop's capture that
            # session shows what that cost: 188 such proofs, 1716s of the
            # radio lock spent on their ACK waits (1196s of it in misses at
            # 13-21s each), and completion ANSWERs waiting up to 55s behind
            # five consecutive attempts of two proofs -- which is exactly how
            # the "stale answer" incidents arose. One tier down, the ANSWER
            # tier: still ahead of bulk data (the peer is waiting on it), but
            # behind a Link handshake, with the ordinary 2-attempt budget and
            # no duty-cycle exemption.
            return self.PRIORITY_ANSWER
        if header.packet_type in (RNS.Packet.LINKREQUEST, RNS.Packet.PROOF):
            return self.PRIORITY_HANDSHAKE
        if header.context is not None:
            if header.context in (RNS.Packet.RESOURCE_PRF, RNS.Packet.RESOURCE_ICL, RNS.Packet.RESOURCE_RCL):
                return self.PRIORITY_HANDSHAKE
            # KEEPALIVE(0xFA)..LRPROOF(0xFF) -- RNS core's own boundary,
            # confirmed directly against RNS/Transport.py's own
            # `packet.context >= RNS.Packet.KEEPALIVE and packet.context
            # <= RNS.Packet.LRPROOF` check, not re-derived by guessing
            # which individual context values feel latency-sensitive.
            if RNS.Packet.KEEPALIVE <= header.context <= RNS.Packet.LRPROOF:
                return self.PRIORITY_HANDSHAKE
            if header.context == RNS.Packet.PATH_RESPONSE:
                return self.PRIORITY_LOW
        return self.PRIORITY_NORMAL

    def _is_link_handshake(self, header: Optional[_RnsHeader]) -> bool:
        """The packets that may PRE-EMPT an idle radio-lock hold (phase 1,
        2026-09-20): a Link's establishment and proof -- LINKREQUEST,
        LRPROOF, LRRTT, LINKIDENTIFY, LINKPROOF -- which MeshChat's 15 s
        window and RNS's own link timers wait on. NOT the rest of
        PRIORITY_HANDSHAKE: KEEPALIVE (32.6 of the laptop's 56.3 s of
        tier-0 lock wait in the 2026-09-20 session, 20 B nothing waits
        on), LINKCLOSE and the RESOURCE_PRF/ICL/RCL band keep their tier
        but pre-empt nothing."""
        if header is None:
            return False
        if header.packet_type == RNS.Packet.LINKREQUEST:
            return True
        return header.context in (
            RNS.Packet.LRPROOF, RNS.Packet.LRRTT, RNS.Packet.LINKIDENTIFY, RNS.Packet.LINKPROOF,
        )

    def _plain_proof(self, header: Optional[_RnsHeader]) -> bool:
        """A plain delivery PROOF: packet type PROOF and not link class
        (`proof_max_age` applies; phase 1, 2026-09-20)."""
        return header is not None and header.packet_type == RNS.Packet.PROOF and not self._proof_is_link_class(header)

    def _proof_is_link_class(self, header: _RnsHeader) -> bool:
        """Whether a PROOF packet is one a Link (or a Resource transfer)
        hangs on -- LRPROOF, RESOURCE_PRF, or any context in RNS core's own
        KEEPALIVE..LRPROOF band -- as opposed to a plain delivery receipt
        (context NONE) for one DATA packet. Only the former keeps
        PRIORITY_HANDSHAKE; see `_priority_tier`."""
        ctx = header.context
        if ctx is None:
            return False
        if ctx == RNS.Packet.RESOURCE_PRF:
            return True
        return RNS.Packet.KEEPALIVE <= ctx <= RNS.Packet.LRPROOF

    def _retry_extra_for(self, header: Optional[_RnsHeader]) -> int:
        """docs/reliability_engine_design.md §2's per-traffic-class extra
        CHANNEL retry-pass budget. See _configure_retry's comment on why
        every ANNOUNCE gets the spontaneous-announce default for now
        (path-response detection is Milestone 5). `LINK_REQUEST` and a
        CHANNEL-fallback `PROOF` correctly fall through to the bare-DATA
        default below -- neither has `destination_type == LINK` (a
        LINK_REQUEST addresses the target Destination directly, before
        any Link exists to carry it; PROOF's destination type mirrors
        whatever it's proving), exactly as §9 describes."""
        if header is None:
            return 0
        if header.packet_type == RNS.Packet.ANNOUNCE:
            return self.announce_retransmit_extra
        if header.packet_type == RNS.Packet.DATA and header.destination_type == RNS.Destination.PLAIN:
            return self.path_req_retransmit_extra
        if header.destination_type == RNS.Destination.LINK:
            return self.ordinary_data_link_retransmit_extra
        return self.ordinary_data_bare_retransmit_extra

    def _path_request_target(self, data: bytes, header: _RnsHeader) -> Optional[bytes]:
        """The destination a path request is asking about: the first
        TRUNCATED_HASHLENGTH bytes of the packet data (`RNS.Transport.
        request_path`: `destination_hash + [transport identity hash] +
        tag`, sent PLAIN so it sits in the clear). The packet's own
        destination-hash field is the shared path-request pseudo-
        destination, identical for every request, so it is useless as a
        rate-limit key."""
        dst_len = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
        data_offset = ((2 + dst_len) if header.header_type == 1 else 2) + dst_len + 1
        if len(data) < data_offset + dst_len:
            return None
        return data[data_offset:data_offset + dst_len]


# ---- interface.py ----

"""The interface class itself."""



class SmartMeshCoreInterface(_ConfigMixin, _ObservabilityMixin, _WireFormatMixin, Interface):
    """Milestones 0-6: scaffolding, wire format, fragmentation/reassembly,
    priority queueing/retry passes, native path discovery, peer
    discovery + routing decisions, and DIRECT fragmentation with full
    stale-path/routing integration. See the module docstring above for
    exact scope and what's still deliberately unimplemented."""

    # -------------------------------------------------------------------
    # Class-level constants
    # -------------------------------------------------------------------

    DEFAULT_IFAC_SIZE = 8

    # How long __init__ blocks waiting for the async setup coroutine to
    # finish (successfully or not) before giving up and logging an error --
    # RNS core expects interface construction to be synchronous, so this
    # can't be unbounded. Also reused as the teardown wait bound in
    # detach(), since disconnecting is a comparably-bounded operation.
    SETUP_TIMEOUT_S = 30.0

    # Every EventType name this interface (this milestone or a later one
    # already scaffolded for) relies on existing must be probed at startup
    # rather than assumed -- design invariant #1's library-contract
    # counterpart (`docs/meshcore_protocol_rules.md`'s library-contract
    # rule 4): these names have already drifted across `meshcore` library
    # versions in this project's own experience, and a fixed import-time
    # assumption fails silently rather than loudly on a version mismatch.
    REQUIRED_EVENT_TYPES = (
        "OK",
        "ERROR",
        "MSG_SENT",
        "ACK",
        "CHANNEL_MSG_RECV",
        "CONTACT_MSG_RECV",
        "SELF_INFO",
        "CONNECTED",
        "DISCONNECTED",
        "PATH_RESPONSE",
        "MESSAGES_WAITING",
    )

    # Default, publicly-known MeshCore channel secret so two nodes running
    # this interface with no channel config at all can find each other with
    # zero coordination. Deliberate, not an oversight: RNS already encrypts
    # and authenticates the actual application data end-to-end
    # (`docs/interface_architecture.md`'s security model), so a shared,
    # publicly-known MeshCore channel secret doesn't expose anything
    # RNS-level -- it only decides which MeshCore LoRa channel this radio
    # joins, the same way a WiFi SSID/password picks a network without
    # implying anything about what's encrypted on top of it. Set
    # channel_idx/channel_name/channel_secret explicitly for a private
    # channel instead.
    DEFAULT_CHANNEL_SECRET_HEX = "b99e9b45f61ab4bd4e355cf812711873"

    # User-requested (2026-09-16): channel_idx/channel_name should also
    # "just be default values", not something every install has to set --
    # same reasoning as DEFAULT_CHANNEL_SECRET_HEX above. Deliberately NOT
    # 0: the reference firmware pre-provisions channel slot 0 as "Public"
    # at boot (`examples/companion_radio/MyMesh.cpp`'s
    # `addChannel("Public", PUBLIC_GROUP_PSK)`, the very first `addChannel`
    # call, landing at index 0 in `BaseChatMesh::channels[]`) -- a 0
    # default here would silently overwrite a device's normal public MeshCore
    # chat channel with this interface's own name/secret the first time it
    # configures the radio (`commands.set_channel` replaces whatever
    # `ChannelDetails` was at that index, per `BaseChatMesh::setChannel`).
    # 35 is comfortably out of the way of MeshCore's own pre-provisioned
    # slots, and is the exact value this interface's own real field
    # testing (zero-hop through 3-hop, 2026-09-16) has been running on.
    DEFAULT_CHANNEL_IDX = 35

    # --- Wire format (docs/wire_format_design.md) -----------------------
    # Protocol constants, not user config -- a from-scratch wire format is
    # a clean break with no interop target, so these are fixed choices,
    # not tunable knobs.

    MARKER = "R"

    # The version byte's low 6 bits are the actual protocol version
    # number (0-63); bit 0x80 flags the multi-fragment header shape
    # (shared meaning across CHANNEL and DIRECT); bit 0x40 flags a
    # cooperative-dispatch fragment (CHANNEL-only, meaningless on DIRECT,
    # Milestone 7). No frame with a different low-6-bit version number
    # than PROTOCOL_VERSION is accepted -- treated as malformed/
    # incompatible rather than guessed at.
    PROTOCOL_VERSION = 1
    VERSION_MASK = 0x3F
    FLAG_MULTI_FRAGMENT = 0x80
    FLAG_COOP = 0x40

    # Header sizes in bytes, after Z85 decode, starting from the version
    # byte (i.e. not counting the "R" marker itself, which is outside the
    # Z85-encoded region).
    CHANNEL_FASTPATH_HEADER_SIZE = 4    # [ver][pkt_id:2][attempt]
    MULTI_FRAGMENT_HEADER_SIZE = 6      # [ver][pkt_id:2][frag_idx][frag_total][attempt]
    DIRECT_BARE_HEADER_SIZE = 1         # [ver] -- firmware's own ACK/attempt do the rest

    # MAX_TEXT_LEN, confirmed against firmware source
    # (meshcore_protocol_rules.md shared rule 1) -- the real ceiling any
    # single CHANNEL or DIRECT text message can occupy on the wire.
    FIRMWARE_TEXT_LIMIT = 160

    # Safety margin subtracted from every payload-budget calculation below,
    # covering firmware variation across builds and the 4th-attempt-
    # shrinks-by-2 quirk noted in wire_format_design.md's payload-budget
    # section. Not a config value: a user enlarging their own margin can
    # only make sends more conservative, never fix a real firmware-side
    # truncation, so there's no legitimate reason to expose it as a knob.
    PAYLOAD_MARGIN = 4

    # Bound on the outgoing FIFO queue process_outgoing() feeds and
    # _outgoing_worker() drains (see both below). Milestone 3 upgrades
    # this same queue to a priority-tiered one rather than replacing the
    # mechanism; sized generously for now, matching the old design's own
    # OUTQUEUE_MAXSIZE precedent.
    OUTQUEUE_MAXSIZE = 512

    # How often the reassembly-map idle-timeout sweep runs
    # (_reassembly_cleanup_loop). Not a documented config knob in
    # docs/reliability_engine_design.md (only the idle timeout itself is)
    # -- an internal granularity choice, generous relative to the 120s/
    # 180s timeouts it enforces, the same way OUTQUEUE_MAXSIZE above is a
    # class constant rather than user config.
    REASSEMBLY_CLEANUP_INTERVAL_S = 10.0

    # --- Priority queueing (docs/reliability_engine_design.md §3) -------
    # Originally two tiers, lower value dequeued/served first. Not user
    # config -- the design's own reasoning is that field data never
    # argued for a finer-grained scheme, so exposing tier count as a knob
    # would invite tuning with no evidence behind it. A third tier was
    # added below once real field data *did* argue for one.
    PRIORITY_HANDSHAKE = 0
    # Field fix (2026-09-19, bidirectional image transfer): reconcile
    # QUERY/ANSWER frames are tiny and a fragmented send on the other
    # node is blocked on them, yet they queued at PRIORITY_NORMAL behind
    # this node's own 4-fragment raw bursts -- a laptop ANSWER waited 30s
    # at queue depth 9 for a question it could answer in one second, and
    # 10 of 40 reconciles timed out for that reason alone. One tier above
    # NORMAL, below HANDSHAKE: a Link handshake still goes first.
    PRIORITY_ANSWER = 1
    PRIORITY_NORMAL = 2
    # User-requested fix (2026-09-16, same field data as PATH_RESPONSE_
    # RATE_LIMIT_WINDOW_S's own raise above): PATH_RESPONSE currently
    # rides PRIORITY_NORMAL, the same tier as real user data, at both the
    # outer two-tier outgoing queue and (via `_priority_tier`) the DIRECT
    # exchange lock -- but it's pure housekeeping, answering a question
    # this node didn't ask, for a destination that isn't necessarily even
    # the traffic this node's own user is waiting on. Rate-limiting alone
    # (above) can't suppress every repeat without also risking suppressing
    # a legitimate one, so real PATH_RESPONSE transmissions keep
    # happening under real client retry storms -- field data showed 17 in
    # one ~6.5-minute window, each one a real multi-fragment DIRECT
    # exchange competing for the same lock as actual data. PRIORITY_LOW
    # means real DATA and PRIORITY_HANDSHAKE exchanges both jump ahead of
    # it in `_PriorityAsyncLock`, so administrative traffic never blocks
    # what the user is actually waiting on, even during a retry storm
    # this interface can't fully suppress at the source.
    PRIORITY_LOW = 3

    # Outgoing-queue shutdown sentinel. A bare `None` (M1/M2's sentinel)
    # would be unsafe now that queue items are (priority, seq, data,
    # header) tuples in a PriorityQueue: if it ever needed comparing
    # against a real tuple at equal priority, comparing None to a tuple
    # raises TypeError. priority=-1 sorts before both real tiers (0, 1),
    # so it's also dequeued at the first opportunity on shutdown; seq=-1
    # never collides since _outqueue_seq only ever counts up from 0.
    _OUTQUEUE_SHUTDOWN_SENTINEL = (-1, -1, None, None, 0.0, None)

    # --- Path discovery / telemetry permission (docs/path_discovery_spec.md) --
    # Firmware constants, not user config -- confirmed directly against
    # examples/companion_radio/NodePrefs.h and src/helpers/SensorManager.h,
    # not the design doc's own paraphrase. TELEM_MODE_ALLOW_FLAGS tells the
    # firmware to gate base-telemetry answers (which path discovery rides
    # on) per-contact via that contact's own `flags` field, rather than
    # answering everyone or no one. TELEM_PERM_BASE_FLAG_BIT is bit 1 of a
    # contact's raw `flags` byte -- onContactRequest() computes
    # `cp = contact.flags >> 1` then checks `cp & TELEM_PERM_BASE(0x01)`,
    # which is bit 1 of the original, unshifted byte (bit 0 is a separate
    # "favourite" flag, left untouched by granting this).
    TELEM_MODE_ALLOW_FLAGS = 1
    TELEM_PERM_BASE_FLAG_BIT = 0x02

    # Human-readable names for _send_outgoing_packet's own per-packet
    # classification debug log (code review's observability fix) --
    # purely for log readability, not used for any actual routing logic.
    _PACKET_TYPE_NAMES = {
        RNS.Packet.DATA: "DATA",
        RNS.Packet.ANNOUNCE: "ANNOUNCE",
        RNS.Packet.LINKREQUEST: "LINKREQUEST",
        RNS.Packet.PROOF: "PROOF",
    }
    _DESTINATION_TYPE_NAMES = {
        RNS.Destination.SINGLE: "SINGLE",
        RNS.Destination.GROUP: "GROUP",
        RNS.Destination.PLAIN: "PLAIN",
        RNS.Destination.LINK: "LINK",
    }
    # Every context value RNS.Packet defines (RNS/Packet.py) -- used for
    # packet-capture readability (_capture_event) and covering the same
    # KEEPALIVE..LRPROOF range _priority_tier already special-cases.
    _CONTEXT_NAMES = {
        RNS.Packet.NONE: "NONE",
        RNS.Packet.RESOURCE: "RESOURCE",
        RNS.Packet.RESOURCE_ADV: "RESOURCE_ADV",
        RNS.Packet.RESOURCE_REQ: "RESOURCE_REQ",
        RNS.Packet.RESOURCE_HMU: "RESOURCE_HMU",
        RNS.Packet.RESOURCE_PRF: "RESOURCE_PRF",
        RNS.Packet.RESOURCE_ICL: "RESOURCE_ICL",
        RNS.Packet.RESOURCE_RCL: "RESOURCE_RCL",
        RNS.Packet.CACHE_REQUEST: "CACHE_REQUEST",
        RNS.Packet.REQUEST: "REQUEST",
        RNS.Packet.RESPONSE: "RESPONSE",
        RNS.Packet.PATH_RESPONSE: "PATH_RESPONSE",
        RNS.Packet.COMMAND: "COMMAND",
        RNS.Packet.COMMAND_STATUS: "COMMAND_STATUS",
        RNS.Packet.CHANNEL: "CHANNEL",
        RNS.Packet.KEEPALIVE: "KEEPALIVE",
        RNS.Packet.LINKIDENTIFY: "LINKIDENTIFY",
        RNS.Packet.LINKCLOSE: "LINKCLOSE",
        RNS.Packet.LINKPROOF: "LINKPROOF",
        RNS.Packet.LRRTT: "LRRTT",
        RNS.Packet.LRPROOF: "LRPROOF",
    }

    # Alpha 0.1.1 fix (2026-09-18 night, see module docstring): the RNS
    # packet contexts `RNS.Transport.packet_filter` exempts from its own
    # duplicate filter because RNS legitimately re-delivers byte-identical
    # packets for them (a Resource part re-requested after arriving
    # outside the receive window, a keepalive, a cache request). Mirrored
    # exactly -- the bare-DIRECT receive dedup must never be stricter than
    # RNS itself.
    _RNS_NO_DEDUP_CONTEXTS = frozenset({
        RNS.Packet.KEEPALIVE,
        RNS.Packet.RESOURCE_REQ,
        RNS.Packet.RESOURCE_PRF,
        RNS.Packet.RESOURCE,
        RNS.Packet.CACHE_REQUEST,
        RNS.Packet.CHANNEL,
    })

    # --- Peer discovery / bind frames (docs/peer_discovery_design.md) ---
    # A marker distinct from MARKER ("R") -- this control frame carries no
    # RNS packet bytes at all and must never be handed to _decode_frame.
    # Wire shape: "P" + Z85([ver:1][type:1][cap:1][attempt:1][pubkey_prefix:6]),
    # 10 raw bytes -> 17 characters on the wire (doc's own worked example),
    # comfortably one CHANNEL fragment under any realistic budget.
    PEER_MARKER = "P"
    BIND_PROTOCOL_VERSION = 1
    BIND_TYPE_REQUEST = 0
    BIND_TYPE_RESPONSE = 1
    BIND_CAP_HAS_UPSTREAM_RNS = 0x01
    # Raw binary DIRECT fragments (2026-09-18 night, see module docstring):
    # advertised only when direct_raw_fragments_enabled, required of a
    # peer before any raw fragment is sent to it.
    BIND_CAP_RAW_FRAGMENTS = 0x02
    BIND_PUBKEY_PREFIX_BYTES = 6
    BIND_FRAME_RAW_SIZE = 4 + BIND_PUBKEY_PREFIX_BYTES  # ver+type+cap+attempt + prefix

    # --- DIRECT-fragmented delivery completion check (field-data-driven
    # fix, 2026-09-16: see this frame's own send/receive helpers for the
    # full "phantom ACK loss" story that motivated it) ---
    # A marker distinct from MARKER ("R") and PEER_MARKER ("P") -- like
    # bind frames, this control frame carries no RNS packet bytes and must
    # never be handed to _decode_frame. DIRECT-only by construction (it
    # asks/answers "did you receive pkt_id X", which only makes sense
    # once both sides already have each other's authenticated identity --
    # bind frames are CHANNEL-only for the opposite reason, since identity
    # isn't established yet at that point).
    # Wire shape: "Q" + Z85([ver:1][type:1][complete:1][pkt_id_hi:1]
    # [pkt_id_lo:1][frag_total:1]), 6 raw bytes -> 10 characters on the
    # wire, comfortably one DIRECT bare message under any realistic budget.
    COMPLETION_MARKER = "Q"
    # Step 3 (2026-09-18): v2 ANSWER frames append a have-bitmap
    # (ceil(frag_total/8) bytes, bit i set = receiver holds frag_idx i)
    # after the fixed 6-byte v1 body; v2 QUERY frames are the fixed body
    # alone. v1 frames (fixed body only, both types) are still decoded
    # and a v1 QUERY is still answered in v1, so an older peer keeps the
    # complete/not-complete check it had; an older peer receiving a v2
    # frame drops it as an unsupported version, which the querying side
    # treats exactly like a lost answer (fall back to re-driving every
    # un-ACKed fragment -- the pre-step-3 behaviour).
    # Field fix (2026-09-19 evening session): v3 appends a one-byte NONCE to
    # both QUERY and ANSWER (after the fixed body, before the ANSWER's
    # bitmap), which the answerer echoes. `_completion_query_waiters` is
    # keyed only `(peer_prefix, pkt_id)`, and the frag_total guard added
    # earlier cannot catch a stale answer whose frag_total happens to match:
    # that session showed FIVE checks resolving `answered` although no
    # matching query ever reached the peer -- one applying `held=[]`
    # authoritatively, i.e. discarding every fragment the receiver really
    # had. A nonce makes the pairing exact. A v1/v2 ANSWER (no nonce) is
    # still accepted (None = "cannot verify", the same trust as before),
    # while a v3 answer whose nonce does not match the outstanding query is
    # discarded. Correction (second audit): a pre-v3 peer drops a v3 QUERY
    # as an unsupported version, so it never answers -- both nodes must be
    # on this build; the querying side does not fall back to v2.
    COMPLETION_PROTOCOL_VERSION = 3
    COMPLETION_PROTOCOL_VERSION_V2 = 2
    COMPLETION_PROTOCOL_VERSION_V1 = 1
    COMPLETION_TYPE_QUERY = 0
    COMPLETION_TYPE_ANSWER = 1
    COMPLETION_FRAME_RAW_SIZE = 6  # ver+type+complete+pkt_id(2)+frag_total -- the fixed body
    # Receiver-initiated completion REPORT (2026-09-20, see the module
    # docstring's entry of that date): an ANSWER the receiver sends
    # UNSOLICITED when a raw burst lands, so the sender does not have to
    # ask. Same v3 frame; the nonce byte distinguishes a report from an
    # echoed QUERY nonce: reports carry `COMPLETION_REPORT_NONCE_BASE |
    # round` (0xF0..0xF3, the raw header's 2-bit round), and QUERY nonces
    # cycle through 1..COMPLETION_QUERY_NONCE_MAX so the two ranges can never
    # meet. A sender that pre-registers a waiter for its burst round accepts
    # the matching report exactly as it would a QUERY's answer.
    COMPLETION_REPORT_NONCE_BASE = 0xF0
    COMPLETION_QUERY_NONCE_MAX = 0xEF

    # --- Raw binary DIRECT fragments (2026-09-18 night, module docstring) ---
    # [ver<<4 | attempt&3 : 1][dst_prefix : 2][src_prefix : 6][pkt_id : 2]
    # [frag_idx : 1][frag_total : 1] then payload. No marker character: a
    # raw packet is its own MeshCore payload type; the version nibble and
    # dst prefix are the filter against other applications' raw packets.
    RAW_PROTOCOL_VERSION = 1
    RAW_HEADER_SIZE = 13
    RAW_DST_PREFIX_BYTES = 2
    # Bit 2 of byte 0 (2026-09-20, completion report): "report what you hold
    # when this lands" -- set on the LAST fragment of every raw burst, so a
    # receiver whose bucket is still incomplete after the burst reports its
    # bitmap without being asked (a complete bucket reports regardless of
    # the bit). Bits 0-1 stay the round, the high nibble the version; an
    # older build masks the bit away and simply never reports.
    RAW_FLAG_REPORT = 0x04
    # Companion firmware limits (MAX_FRAME_SIZE 176 on the serial link):
    # onRawDataRecv pushes payload + 4 bytes, CMD_SEND_RAW_DATA carries
    # cmd + path_len + path + payload -- both confirmed in
    # examples/companion_radio/MyMesh.cpp and BaseSerialInterface.h.
    #
    # Audit fix (2026-09-19): the RX limit was 173, one too high. It was
    # derived from onRawDataRecv's own guard (`payload_len + 4 >
    # sizeof(out_frame)`, and out_frame is MAX_FRAME_SIZE+1 = 177), but the
    # write that follows it goes through ArduinoSerialInterface::writeFrame,
    # which refuses anything over MAX_FRAME_SIZE (176) and returns 0 --
    # silently, inside the receiving radio. A 173-byte raw payload builds a
    # 177-byte serial frame and is discarded there, so the fragment never
    # reaches the host at all. The default cap of 170 is safe, but a user
    # raising direct_raw_payload_cap to the advertised firmware limit made
    # every zero-hop and 1-hop fragment vanish -- and because nothing was
    # ever held, the fallback logic then blamed the repeater chain and
    # blacklisted the path for direct_raw_path_unsupported_ttl (24h).
    FIRMWARE_RAW_RX_PAYLOAD_LIMIT = 172
    FIRMWARE_RAW_TX_FRAME_LIMIT = 174

    # User-requested small-mesh rule (not a config knob -- a fixed,
    # topology-driven behavior): with this few bound peers, there's no
    # ambiguity about who a CHANNEL-broadcast-shaped packet (ANNOUNCE,
    # path request, or "no known token yet" traffic) is actually for, and
    # DIRECT (real firmware ACK/retry) is strictly more reliable than
    # CHANNEL's own unacknowledged flood-relay -- so in that regime this
    # traffic goes DIRECT to every bound peer instead of over CHANNEL at
    # all, rather than CHANNEL-plus-a-DIRECT-supplement. See
    # _in_small_mesh_mode() and _send_outgoing_packet()'s three routing
    # branches.
    SMALL_MESH_DIRECT_ONLY_MAX_PEERS = 3

    # Audit fix (2026-09-19): capacity bound for `_rns_token_peer` -- see its
    # own declaration for why a bound was needed. Generous: a real node sees
    # tens of destinations per hour, so this only ever trims pathological
    # growth over days of uptime, never a working set.
    RNS_TOKEN_PEER_MAX_KEYS = 4096

    # Audit fix (2026-09-19): how many post-bind path-discovery rounds
    # `_discover_path_after_bind` runs before leaving it to real traffic.
    # See that method for the two races it covers.
    POST_BIND_DISCOVERY_ROUNDS = 3

    # User-requested fix (not a config knob -- see _send_outgoing_packet's
    # own docstring for the full mechanism): delay an outgoing LRPROOF by
    # this long before sending, so RNS's one-time Link RTT sample lands
    # comfortably past RNS.Link.KEEPALIVE_MAX_RTT (1.75s) and every new
    # Link gets pinned at RNS's own maximum keepalive/stale_time (360s/
    # 720s) rather than an unrepresentatively short one measured from an
    # uncontended handshake. Comfortably clear of 1.75s with margin for
    # clock/measurement jitter, and trivially small next to RNS's own
    # link-establishment timeout (6s*hops + 360s).
    LINK_PROOF_RTT_INFLATION_DELAY_S = 1.5

    # User-requested fix (not a config knob, mirroring the shape of
    # path_discovery's own existing backoff -- see _unknown_dest_in_
    # backoff/_record_unknown_dest_attempt): after this many DIRECT-
    # bootstrap attempts to an unknown destination with no token ever
    # learned for it, back off rather than trying again on every single
    # outgoing packet still addressed to it. Threshold originally matched
    # path discovery's own `path_discovery_quick_attempts` default (3);
    # ef57809 lowered that to 2 and left this at 3, so they no longer
    # match. Cooldown grows the same way (doubling, capped) so a
    # destination that becomes reachable later is retried eventually,
    # not blocked forever -- learning a real token for it (§7) clears
    # this immediately regardless of where in the backoff it is.
    UNKNOWN_DEST_BOOTSTRAP_FAILURE_THRESHOLD = 3
    UNKNOWN_DEST_BOOTSTRAP_BASE_COOLDOWN_S = 300.0
    # How long a closed Link's id keeps dropping late packets for it. A
    # link_id is never reused (it is the LINKREQUEST's hash), so this only
    # bounds the dict, not correctness.
    CLOSED_LINK_TTL_S = 600.0
    UNKNOWN_DEST_BOOTSTRAP_MAX_COOLDOWN_S = 3600.0
    UNKNOWN_DEST_BOOTSTRAP_BACKOFF_FACTOR = 2.0

    # User-requested fix (2026-09-15, real 2-hop repeater field testing --
    # diagnosed jointly with another Claude session working the same field
    # test from the laptop side): an outgoing PATH_RESPONSE is RNS
    # Transport's own answer to a PATH_REQUEST it decided, on its own, to
    # answer -- this interface has no say over whether one gets generated,
    # only over how many times it actually keys the shared half-duplex
    # radio to transmit it. Packet-capture evidence from this exact field
    # test found a remote NomadNet client stuck in a tight connection-retry
    # loop, re-issuing PATH_REQUESTs for the same destination every 4-8s
    # while its own link attempt kept failing -- well under RNS.Transport's
    # own PATH_REQUEST_MI=20s floor, confirming these were explicit
    # request_path() calls from that client, not ordinary path aging. RNS
    # answered every single one, each one a real multi-fragment ANNOUNCE
    # transmission, landing squarely in the same ~10-minute window six
    # unrelated DIRECT DATA sends were failing to get through. This
    # interface can't fix the remote client's retry loop, but it can stop
    # re-spending airtime re-answering a question it already just answered
    # -- the destination's path/identity cannot plausibly have changed
    # within a few seconds of the last answer, and the requester already
    # has a reply in flight. Not a config knob, same reasoning as
    # LINK_PROOF_RTT_INFLATION_DELAY_S above: a self-throttling floor, not
    # a per-deployment tuning value.
    #
    # User-requested fix (2026-09-16): raised from 10s to 20s once real
    # 1-hop and 2-hop field data showed the original window's actual
    # coverage. The remote client's real retry cadence in later tests was
    # mostly 15-26s apart (not the tighter 4-8s from the original 2-hop
    # test that motivated this fix), so a 10s window still let the large
    # majority of repeats straight through -- one test window logged 17
    # real PATH_RESPONSE transmissions in ~6.5 minutes with only 2
    # suppressed. 20s covers meaningfully more of that observed cadence
    # without getting close to RNS.Transport's own PATH_REQUEST_MI=20s
    # floor for *automated* (non-explicit-retry) path requests, so a
    # destination with a genuine reason to re-request outside of a tight
    # client retry loop still gets answered promptly.
    PATH_RESPONSE_RATE_LIMIT_WINDOW_S = 20.0

    # Field-diagnosed (2026-09-18 drive-home capture, see module
    # docstring): the request-side mirror of the rule above. 14 identical
    # path requests for one destination in 4 minutes, 4-8s apart, each a
    # full 3-hop DIRECT exchange. Keyed on the REQUESTED destination
    # (inside the payload -- every path request shares one PLAIN
    # pseudo-destination hash). Same 20s as RNS.Transport's own automatic
    # PATH_REQUEST_MI floor, so only explicit client retries are affected.
    PATH_REQUEST_RATE_LIMIT_WINDOW_S = 20.0
    # Bound on the local announce cache (2026-09-20): LRU, one entry per
    # destination this interface delivered an announce for.
    ANNOUNCE_CACHE_MAX_KEYS = 256

    # -------------------------------------------------------------------
    # Construction
    # -------------------------------------------------------------------

    def __init__(self, owner, configuration):
        super().__init__()

        self.owner = owner
        cfg = configuration

        self._configure_identity(cfg)
        self._configure_transport(cfg)
        self._configure_channel(cfg)
        self._configure_radio(cfg)
        self._configure_fragmentation(cfg)
        self._configure_retry(cfg)
        self._configure_path_discovery(cfg)
        self._configure_peer_discovery(cfg)
        self._configure_observability(cfg)
        # Field-diagnosed fix (2026-09-18, see module docstring): these
        # timing knobs are spread across five different _configure_*
        # methods above, each independently tunable, but they aren't
        # actually independent -- the reassembly-idle-timeout side and the
        # DIRECT-retry-latency side are two ends of the same budget. Must
        # run after every _configure_* call above has set the values it
        # reads.
        self._validate_direct_timing_budget()

        # --- RNS core interface-contract attributes -------------------
        # RNS core reads these directly (Transport-layer MTU checks,
        # announce pacing, Link-establishment timeout estimates) -- see
        # `docs/reliability_engine_design.md`'s "RNS base-class contract"
        # notes. Set unconditionally here (not only on a successful
        # connection) so nothing downstream ever observes HW_MTU as the
        # base class's None default, even if this constructor's connection
        # attempt below fails and the interface stays offline.
        self.HW_MTU = RNS.Reticulum.MTU

        # --- Internal async/threading state -----------------------------
        # `self._mc` itself stays a plain, genuinely-Optional attribute --
        # unlike the other six below, its None-ness is real, recurring
        # runtime state (no live connection yet, or a torn-down one), not
        # just "not initialized yet"; several call sites correctly check
        # `self._mc is None` and must keep seeing that as a real
        # possibility. See `_mc_ready` further down for the narrow-and-
        # return accessor used at call sites that only ever run while a
        # connection is known to be live.
        self._mc = None
        # Backing fields for the `_mc_module`/`_EventType`/`_loop`/
        # `_command_lock`/`_direct_exchange_lock`/`_duty_cycle` properties
        # below (2026-09-16, Pylance cleanup): unlike `_mc` above, all six
        # of these really are None only until `_async_setup`/`_load_
        # meshcore_or_panic` run, then set exactly once and never reset to
        # None again for the rest of this instance's life (confirmed: no
        # other assignment site exists for any of them) -- a real
        # invariant, not just a convention, so each property asserts it
        # rather than every one of the many call sites needing its own
        # None-check or `# type: ignore`. `_loop` is the one exception
        # with a legitimate None-tolerant read (`detach()`'s teardown
        # safety net, since detach can run before setup ever completed) --
        # that one call site reads `_loop_impl` directly instead.
        self._mc_module_impl = None
        self._EventType_impl = None
        self._loop_impl = None
        self._loop_thread = None
        self._command_lock_impl = None
        # Field-diagnosed fix (2026-09-15, real MeshChat traffic): see
        # _send_direct_frame_and_wait_for_ack's own docstring for why this
        # exists alongside _command_lock, not instead of it.
        self._direct_exchange_lock_impl = None
        # User-requested fix (2026-09-16): see _DutyCycleLimiter's own
        # docstring. Constructed in _async_setup alongside the locks above
        # for the same reason -- no event-loop dependency at construction
        # time, but kept consistent with this file's own pattern for
        # per-connection state.
        self._duty_cycle_impl = None
        # User-requested fix (2026-09-16), narrowed 2026-09-18 -- see the
        # module docstring's 2026-09-18 entry: time.monotonic() of the most
        # recently received DIRECT fragment that left its own reassembly
        # bucket still incomplete (i.e. concrete evidence the sender has
        # more fragments of THIS transfer still to come), or None if
        # nothing like that has arrived yet this session. No longer set for
        # every DIRECT frame heard (ACKs, PROOFs, completion-checks, a
        # fragment that completed its bucket) -- see
        # _handle_direct_multifragment_frame, the only place this is set,
        # and _wait_for_incoming_quiet, which reads it.
        self._last_incoming_direct_at = None
        # User-requested observability addition (2026-09-15, post-alpha-0.1.0
        # 2-hop field test): how many DIRECT send+ACK-wait cycles are
        # currently either waiting for `_direct_exchange_lock` or holding
        # it, at any instant -- a live queue-depth signal for exactly the
        # "several concurrent messages splitting the one shared radio"
        # scenario a real field test surfaced. Incremented/decremented
        # around the lock acquisition in `_send_direct_frame_and_wait_for_
        # ack`, read (never written) anywhere else.
        self._direct_exchange_queue_depth = 0
        # Raw-RX-log observation (2026-09-18, step 1 -- see
        # rx_log_observe_enabled's own comment): counters and last-heard
        # timestamps fed by _on_rx_log_data, read only by the [STATS]
        # snapshot and the packet capture. `_rx_log_feed_seen` flips True
        # on the first event so the stats line can distinguish "firmware
        # never pushed the feed" (older companion build) from "quiet mesh".
        self._rx_log_events_total = 0
        self._rx_log_by_payload_type = collections.Counter()
        self._last_rx_log_at = None
        self._rx_log_feed_seen = False
        # time.monotonic() of this node's own most recent radio keying,
        # stamped in _pre_transmit_gate (every send path passes through
        # it). Capture-only today: lets an rx_log record say how long after
        # our own last transmit it was heard -- the raw material for
        # telling a repeater's echo of our frame apart from unrelated
        # traffic once real captures exist to check that against.
        self._last_own_tx_at = None
        # Step 2 (2026-09-18): per-peer measured ACK RTT -- peer_prefix ->
        # {"srtt", "rttvar", "samples", "last_rtt"}; see _record_ack_rtt/
        # _adaptive_ack_timeout/_invalidate_ack_rtt. Only ever touched on
        # this interface's own event loop.
        self._ack_rtt = {}
        # Field fix (2026-09-18 evening, see module docstring): the stats
        # Karn just discarded, kept only so _completion_query_timeout_s can
        # size a reconcile QUERY on the link it was just measured on; and
        # the firmware's last hop-aware ACK bound per peer as the fallback
        # when even that is gone. Both dropped on any path change.
        self._ack_rtt_snapshot = {}
        # First raw field test (2026-09-18 night): QUERY -> ANSWER round
        # trip per peer, measured directly -- raw bursts produce no ACK
        # samples, so this is what sizes the reconcile wait.
        self._query_rtt = {}
        # Phase 1 (2026-09-20): burst end -> completion REPORT arrival per
        # peer, on-time and late reports alike, which sizes the report
        # window (`_completion_report_wait_s`); (peer, pkt_id) -> the
        # burst's end time while a report for it may still be measured.
        self._report_rtt = {}
        self._report_expected = {}
        self._last_firmware_ack_timeout_s = {}
        # Rolling one-byte nonce for completion QUERYs (field fix 2026-09-19).
        self._completion_query_nonce = 0
        # Per-peer repeater-echo timings (seconds after our own transmit
        # that the first hop was heard forwarding our frame), the data
        # behind the early hop-1 abort -- see _hop1_abort_deadline_s.
        self._echo_stats = {}
        # Session-wide pool of the same echo timings across every peer
        # (second audit, 2026-09-19 evening): what arms the hop-1 abort
        # for a peer whose own per-path samples were just cleared.
        self._echo_stats_all = []
        # peer_prefix -> asyncio.Semaphore bounding how many fragmented
        # sends (raw or text) to that peer may be in flight at once; see
        # direct_fragmented_max_in_flight. Created lazily on the loop.
        # Keyed (peer_prefix, "data"|"announce") since 2026-09-19 night --
        # see _fragmented_send_slot.
        self._fragmented_send_slots = {}
        # Step 2: the currently open per-attempt RX-log correlation window
        # (None when no DIRECT attempt is in flight). Opened/closed by
        # _send_direct_frame_and_wait_for_ack under _direct_exchange_lock
        # -- so at most one is ever open -- and filled by _on_rx_log_data
        # with what the radio overheard while this node was waiting for
        # its ACK. Capture-only: nothing reads it back for a decision.
        self._rx_log_window = None
        self._last_fragmented_pkt_id = None
        self._last_fragmented_frag_total = None
        # Step 4 (2026-09-18): the radio's own SF/BW(kHz)/CR from SELF_INFO
        # (None until _fetch_own_identity succeeds -- _estimate_airtime_s
        # then falls back to duty_cycle_estimate_bitrate), and the rolling
        # "air is predicted busy until" monotonic timestamp _on_rx_log_data
        # extends. Always maintained; only acted on when rx_log_holds_
        # enabled (see that config's own comment).
        self._radio_params = None
        self._medium_busy_until = 0.0
        self._medium_busy_reason = None
        self._stats_task = None
        self._connected_since = None
        self._outgoing_dropped_total = 0
        self._incoming_dropped_total = 0
        self._packet_capture_file = None
        self._packet_capture_lock = threading.Lock()
        self._packet_capture_seq = 0

        # This node's own name, as reported by send_appstart -- feeds the
        # CHANNEL payload-budget formula (the firmware's mandatory
        # "<name>: " prefix eats into the usable budget). Defaults to ""
        # (matching a genuinely nameless node) until identity fetch
        # completes in _async_setup; see the warning logged there if that
        # fetch fails, since "" is indistinguishable from "genuinely no
        # name" and could understate the real prefix cost.
        self._own_node_name = ""

        # Wire-format state (docs/wire_format_design.md): a wrapping
        # 16-bit counter for pkt_id, and the outgoing priority queue
        # process_outgoing() feeds (called on RNS core's own thread) and
        # _outgoing_worker() drains (on this interface's event loop) --
        # the plain thread-safe queue plus executor-drain pattern
        # docs/reliability_engine_design.md's sync/async bridge notes
        # call for, so process_outgoing() never blocks RNS core's calling
        # thread. Items are (priority, seq, data, header) tuples; `seq`
        # is a monotonic tie-breaker from itertools.count() (atomic under
        # the GIL -- §3's own warning against a plain `x += 1` racing
        # between RNS's calling thread and this event loop) so two
        # same-priority items never need Python to compare their `data`/
        # `header` fields to break a tie.
        self._pkt_id_counter = 0
        # Field fix (2026-09-18 evening, page-load capture): truncated hash
        # of every RNS packet currently queued or being sent -> enqueue
        # time. process_outgoing (RNS's thread) drops a packet whose bytes
        # are already here; _outgoing_worker releases the entry once every
        # send task the packet spawned has finished. Guarded by a
        # threading.Lock since both threads touch it.
        self._outgoing_inflight = {}
        # Consecutive duplicate-suppression count per in-flight key, so a
        # stuck entry cannot block a packet RNS keeps re-requesting (field
        # fix 2026-09-19 -- see process_outgoing).
        self._outgoing_duplicate_suppressed = {}
        # peer_prefix -> monotonic timestamps of recent DIRECT successes, read
        # by the healthy-path guard in record_direct_send_result (2026-09-19).
        self._direct_path_recent_success = {}
        self._outgoing_inflight_lock = threading.Lock()
        self._outqueue = queue.PriorityQueue(maxsize=self.OUTQUEUE_MAXSIZE)
        self._outqueue_seq = itertools.count()
        self._outgoing_worker_task = None

        # Reassembly (docs/reliability_engine_design.md §5) and
        # whole-packet dedup (§7) state. Both are keyed the same way
        # (self._reassembly_key) and both only ever touched from this
        # interface's own event loop thread (incoming events and the
        # cleanup sweep both run there) -- no lock needed.
        self._reassembly = {}   # key -> _ReassemblyBucket
        self._dedup = {}        # key -> expiry (time.monotonic())
        self._reassembly_cleanup_task = None

        # Background-scheduled CHANNEL retry passes (§1-§2): tracked so
        # detach() can cancel any still-pending pass rather than leaving
        # it dangling. Self-removing via add_done_callback, mirroring the
        # `meshcore` library's own _spawn_background pattern
        # (serial_cx.py) for the same reason -- nothing else would ever
        # clear a normally-completed entry otherwise.
        self._background_tasks = set()

        # Path discovery (docs/path_discovery_spec.md), all keyed by
        # peer pubkey prefix (hex string) and only ever touched from this
        # interface's own event loop thread:
        #   _resolved_paths       -- this interface's own last-verified
        #                            path per peer, authoritative for its
        #                            own routing/staleness decisions
        #                            regardless of device-persist success.
        #   _path_discovery_failures / _path_discovery_backoff_until --
        #                            per-peer consecutive-failure count
        #                            and exponential-backoff cooldown.
        #   _direct_path_failures -- per-peer consecutive CACHED-PATH
        #                            send failures (§8), a different
        #                            counter than discovery failures above
        #                            -- not yet fed by an actual DIRECT
        #                            send path (Milestone 5+).
        self._resolved_paths = {}
        self._path_discovery_failures = {}
        self._path_discovery_backoff_until = {}
        self._direct_path_failures = {}
        # User-requested fix (2026-09-15, real field diagnosis via packet
        # capture): an unknown destination this node has no token for
        # gets a DIRECT-bootstrap attempt every single time something
        # tries to reach it, forever, with no memory of past attempts --
        # fine for a destination that's genuinely reachable (self-
        # limiting the moment a token is learned), wasteful airtime for
        # one that structurally never will be (confirmed in the field: an
        # LXMF propagation node out on the wider network, not reachable
        # through this node's only MeshCore peer at all, retried on its
        # own periodic schedule with zero backoff). See
        # _unknown_dest_in_backoff/_record_unknown_dest_attempt.
        self._unknown_dest_attempts = {}
        self._unknown_dest_backoff_until = {}
        # Code-review fix: last-attempt timestamp per destination_hash,
        # swept periodically (_unknown_dest_backoff_sweep) alongside
        # _dedup/_reassembly/_proof_correlation -- without this, a
        # destination that gets a few attempts and then simply stops being
        # addressed (an ephemeral or one-off destination_hash, never
        # succeeding and never crossing the backoff threshold either) sat
        # in _unknown_dest_attempts/_unknown_dest_backoff_until forever;
        # only a later success (via _clear_unknown_dest_backoff) ever
        # removed an entry.
        self._unknown_dest_last_attempt = {}
        
        # PATH_RESPONSE_RATE_LIMIT_WINDOW_S's own state -- destination_hash
        # -> time.monotonic() of the last outgoing PATH_RESPONSE actually
        # sent for it. See _path_response_rate_limited.
        self._path_response_last_sent_at = {}
        # destination_hash -> time.monotonic() of the last spontaneous
        # ANNOUNCE forwarded for it (announce_min_interval).
        self._announce_last_sent_at = {}
        # link_id -> time.monotonic() a LINKCLOSE for it was seen (either
        # direction); queued packets for that Link are dropped at dequeue.
        self._closed_links = {}
        # PATH_REQUEST_RATE_LIMIT_WINDOW_S's state -- requested destination
        # hash -> time.monotonic() of the last path request sent for it.
        self._path_request_last_sent_at = {}
        # Code review (2026-09-18): link_id -> (destination_hash, expiry)
        # for every LINKREQUEST this node sent, so the LRPROOF that answers
        # it (whose destination field is that link_id, never the
        # destination's own hash) can be tied back to the destination it
        # proves reachable -- see _compute_link_id/_observe_incoming_rns_
        # packet's PROOF branch. Swept by _pending_link_request_sweep.
        self._pending_link_requests = {}
        # truncated packet hash -> (destination_hash, expiry) for a DATA
        # packet sent by the unknown-destination bootstrap: its delivery
        # PROOF proves the destination is reachable through whoever sent
        # the proof (2026-09-19, see _remember_bootstrap_send).
        self._pending_dest_proofs = {}
        # Phase 1 (2026-09-20): a bare DIRECT send whose purpose is
        # fulfilled by a reply -- a LINKREQUEST by its LRPROOF, a bootstrap
        # DATA by its PROOF -- stops retrying the moment the reply is seen.
        # key (link_id, or the DATA's truncated hash) -> asyncio.Event the
        # in-flight send waits on; and key -> time.monotonic() the reply
        # was seen, so a send that only checks afterwards still learns it.
        # See _answered_send_key / _signal_send_answered.
        self._send_answered_events = {}
        self._send_answered_at = {}
        # Phase 1 (2026-09-20): destination_hash -> (announce bytes as
        # received, time.monotonic(), source peer prefix) for every ANNOUNCE
        # a bound peer delivered to RNS through this interface (LRU,
        # ANNOUNCE_CACHE_MAX_KEYS); destination_hash -> time.monotonic() of
        # the last path request answered from it. See
        # _answer_path_request_locally.
        self._announce_cache = collections.OrderedDict()
        self._path_request_local_answer_at = {}

        self._contact_refresh_task = None

        # DIRECT-fragmented completion-check state (see
        # _check_remote_completion's own docstring): one in-flight
        # asyncio.Future per (peer_prefix, pkt_id) we've asked about,
        # resolved by _handle_incoming_completion_frame when a matching
        # ANSWER arrives, or left to time out if none ever does.
        self._completion_query_waiters = {}
        # Alpha 0.1.1 (2026-09-18 night): (peer_prefix, payload truncated
        # hash) -> {"pkt_id", "frag_total", "acked", "expires_at"} for a
        # fragmented DIRECT send that failed with some fragments delivered
        # -- see _send_direct_payload/_send_direct_fragmented's resume
        # path. Swept by _resumable_sends_sweep.
        self._resumable_sends = {}
        # Raw fragments (2026-09-18 night): peer_prefix -> monotonic time
        # until which raw is disabled for that peer (fallback to text), and
        # receive-side counters for the [STATS] line.
        self._raw_disabled_until = {}
        # Field fix (2026-09-19 night): peer_prefix -> consecutive raw sends
        # that were answered-but-incomplete after every round. The pause
        # above is set only once this reaches direct_raw_incomplete_strikes;
        # a raw send that completes resets it. See _send_direct_raw_
        # fragmented's closing block for the 21:45:14 incident.
        self._raw_incomplete_strikes = {}
        # Per-PATH verdicts (user's design, 2026-09-18 night): out_path_hex
        # -> {"since", "peer"} for a repeater chain that provably drops
        # raw packets (Z85 text got through where raw did not), and the
        # peer -> path of a fallback whose text outcome is still pending.
        self._raw_unsupported_paths = {}
        # (peer_prefix, path_hex) -> monotonic time the raw attempt gave up
        # on that path; consumed by _send_direct_payload once the Z85 text
        # send that followed it has an outcome (audit fix 2026-09-19: keyed
        # on the path too, so concurrent sends can't cross-attribute).
        self._raw_fallback_pending = {}
        self._raw_fragments_received = 0
        self._raw_frames_ignored = 0

        # Milestone 6: concurrent DIRECT sends to the same not-yet-(or no
        # longer-)resolved peer share one in-flight discover_path() call
        # rather than each starting their own quick-attempts burst --
        # peer pubkey_prefix -> asyncio.Future[Optional[_ResolvedPath]].
        # See _discover_path_coalesced.
        self._pending_path_discoveries = {}

        # Peer discovery / routing (docs/peer_discovery_design.md,
        # docs/routing_decisions.md), Milestone 5. `_peers` is this
        # interface's own bind-protocol peer registry, keyed by the
        # canonical 6-byte (12-hex-char) pubkey prefix
        # (BIND_PUBKEY_PREFIX_BYTES) -- distinct from, and never
        # constructed from, MeshCore's own contact table. `_own_pubkey_hex`
        # is populated once identity fetch succeeds in _async_setup;
        # "" until then (matches _own_node_name's own same-reason default).
        self._peers = {}
        self._own_pubkey_hex = ""
        self._bind_attempt_counter = itertools.count()
        self._last_bind_response_sent = None  # time.monotonic(), global per-node throttle (§3)
        self._peer_discovery_task = None
        self._peer_ttl_sweep_task = None

        # Opportunistic RNS-token learning (§7), only ever populated from
        # DIRECT-received traffic -- a CHANNEL "R" frame carries no sender
        # pubkey at all (wire_format_design.md), so a reliable peer
        # attribution is structurally only available on the DIRECT receive
        # path. `_rns_token_peer`: destination_hash(bytes) -> pubkey_prefix,
        # no expiry (cleared only on peer TTL expiry, §6). `_proof_correlation`:
        # truncated_hash(bytes) -> (pubkey_prefix, expiry monotonic time),
        # short-TTL, never persisted (§4/§7 -- a pending PROOF has no reason
        # to still be pending after a restart).
        # Audit fix (2026-09-19): an OrderedDict with a bound. The design's
        # documented "no expiry, cleared only on peer TTL expiry (§6)" holds
        # for stable destination hashes, but `_observe_incoming_rns_packet`
        # also stores one entry per *ephemeral* Link id, and a peer that
        # stays active never reaches the 24h TTL that was the only reclaim
        # path -- so on a long-running node this grew monotonically (52-92
        # new destination hashes per hour in the 2026-09-18 captures, plus a
        # dead entry for every Link ever opened). Oldest-inserted entries are
        # evicted past RNS_TOKEN_PEER_MAX_KEYS; losing a token is graceful --
        # the next packet for it goes through discovery/broadcast exactly as
        # it did before the token was ever learned.
        self._rns_token_peer = collections.OrderedDict()
        self._proof_correlation = {}

        self._setup_done = threading.Event()

        self._load_meshcore_or_panic()
        self._start_async_bridge()

        if not self._setup_done.wait(timeout=self.SETUP_TIMEOUT_S):
            RNS.log(
                f"{self}: setup timed out after {self.SETUP_TIMEOUT_S:.0f}s "
                f"-- interface will remain offline.",
                RNS.LOG_ERROR,
            )
        elif not self.online:
            RNS.log(
                f"{self}: setup completed but the interface did not come "
                f"online -- see the specific failure logged above.",
                RNS.LOG_ERROR,
            )

    # -------------------------------------------------------------------
    # Connection-state accessors (2026-09-16, Pylance cleanup)
    #
    # `_mc_module`/`_EventType`/`_loop`/`_command_lock`/
    # `_direct_exchange_lock`/`_duty_cycle` are all set exactly once, in
    # `_load_meshcore_or_panic`/`_async_setup`, and never reset to None
    # again for the rest of this instance's life -- confirmed by grep,
    # not just assumed. Every call site that reads one of them runs only
    # after that one-time setup has already completed, so the `assert`
    # below is a real invariant check (documents and enforces the
    # ordering), not a formality -- and it lets every one of the many
    # call sites elsewhere keep its original `self._foo.bar(...)` spelling
    # instead of needing its own None-check or `# type: ignore`.
    #
    # `_mc` deliberately has no such property: unlike the six above, its
    # None-ness is real, recurring runtime state (no live connection yet,
    # or a torn-down one), and several call sites correctly branch on
    # `self._mc is None`. `_mc_ready` below is the narrow-and-return
    # accessor for the *other* call sites -- the ones that only ever run
    # while a connection is known to be live.
    @property
    def _mc_module(self):
        assert self._mc_module_impl is not None
        return self._mc_module_impl

    @property
    def _EventType(self):
        assert self._EventType_impl is not None
        return self._EventType_impl

    @property
    def _loop(self):
        assert self._loop_impl is not None
        return self._loop_impl

    @property
    def _command_lock(self):
        assert self._command_lock_impl is not None
        return self._command_lock_impl

    @property
    def _direct_exchange_lock(self):
        assert self._direct_exchange_lock_impl is not None
        return self._direct_exchange_lock_impl

    @property
    def _duty_cycle(self):
        assert self._duty_cycle_impl is not None
        return self._duty_cycle_impl

    @property
    def _mc_ready(self):
        assert self._mc is not None
        return self._mc

    # -------------------------------------------------------------------
    # Startup helpers
    # -------------------------------------------------------------------

    # Audit fix (2026-09-19): every periodic loop in this file is
    # `while not detached: await asyncio.sleep(self.<interval>)`, so a user
    # writing 0 -- which this config surface teaches elsewhere as "disable"
    # (outgoing_max_age) or "leave alone" (freq/bw/sf/cr) -- spun the
    # interface's event loop at 100% CPU, and for contact_refresh_interval
    # also flooded the serial link with ensure_contacts. Every such sleep
    # now goes through this floor.
    MIN_LOOP_INTERVAL_S = 1.0

    def _load_meshcore_or_panic(self):
        try:
            import meshcore as _mc_mod
        except ImportError:
            RNS.log(
                f"{self}: the 'meshcore' python library is not installed -- "
                f"cannot continue.",
                RNS.LOG_CRITICAL,
            )
            RNS.panic()
            return

        self._mc_module_impl = _mc_mod
        self._EventType_impl = _mc_mod.EventType
        self._verify_required_event_types()

    def _verify_required_event_types(self):
        missing = [
            event_name
            for event_name in self.REQUIRED_EVENT_TYPES
            if not hasattr(self._EventType, event_name)
        ]
        if missing:
            RNS.log(
                f"{self}: installed meshcore library's EventType enum is "
                f"missing required member(s) {missing} -- this library "
                f"version is incompatible with this interface "
                f"(meshcore_protocol_rules.md library-contract rule 4: "
                f"never assume these names, always probe). Refusing to "
                f"go online.",
                RNS.LOG_CRITICAL,
            )
            raise RuntimeError(f"meshcore EventType missing required member(s): {missing}")

    def _start_async_bridge(self):
        # A dedicated background thread owns its own asyncio event loop for
        # the entire life of the interface. Interface *construction* (which
        # RNS expects to be synchronous) blocks on that loop's async setup
        # completing, via run_coroutine_threadsafe plus a plain
        # threading.Event.wait() back on the constructing thread --
        # `docs/reliability_engine_design.md`'s "sync/async bridge" notes.
        self._loop_impl = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_loop, daemon=True, name=f"SMCI-loop-{self.name}"
        )
        self._loop_thread.start()

        setup_future = asyncio.run_coroutine_threadsafe(self._async_setup(), self._loop)

        def _on_setup_done(fut):
            if fut.cancelled():
                return
            exc = fut.exception()
            if exc is not None:
                RNS.log(f"{self}: setup raised an exception: {exc}", RNS.LOG_ERROR)
                RNS.log(
                    "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                    RNS.LOG_ERROR,
                )
            self._setup_done.set()

        setup_future.add_done_callback(_on_setup_done)

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        except Exception as exc:
            RNS.log(f"{self}: event loop crashed: {exc}", RNS.LOG_ERROR)

    # -------------------------------------------------------------------
    # Command chokepoint (design invariants #1 and #2)
    # -------------------------------------------------------------------

    async def _run_command(self, command_coro, context, expected_types):
        """Serialize the actual `commands.*()` call behind a single lock
        (invariant #2 -- the library matches replies by bare event type,
        with no per-request correlation id, so two concurrent commands
        waiting on the same type can steal each other's reply) and check
        the returned event's actual type against what THIS call expected
        (invariant #1 -- a non-exception return is not success; CHANNEL
        commands in particular never return a delivery confirmation at
        all). Raises on ERROR, no response, or an unexpected type."""
        if not isinstance(expected_types, (list, tuple)):
            expected_types = (expected_types,)

        async with self._command_lock:
            result = await command_coro

        self._debug(f"{context} -> {getattr(result, 'type', result)!r}")

        if result is None:
            raise RuntimeError(
                f"{context}: no event returned "
                f"(expected one of {[t.name for t in expected_types]})"
            )
        if result.type == self._EventType.ERROR:
            reason = (
                result.payload.get("reason", result.payload)
                if isinstance(result.payload, dict)
                else result.payload
            )
            raise RuntimeError(f"{context}: firmware ERROR ({reason})")
        if result.type not in expected_types:
            raise RuntimeError(
                f"{context}: unexpected event type {result.type!r} "
                f"(expected one of {[t.name for t in expected_types]})"
            )
        return result

    def _duty_cycle_exempt(self, priority: int) -> bool:
        """Whether a frame of this priority tier skips the duty-cycle wait
        (see duty_cycle_exempt_handshake). Its airtime is still recorded."""
        return self.duty_cycle_exempt_handshake and priority == self.PRIORITY_HANDSHAKE

    async def _throttle_for_duty_cycle(
        self, frame: str, exempt: bool = False, on_air_bytes: Optional[int] = None,
        interrupt: "Optional[asyncio.Event]" = None,
    ) -> float:
        """User-requested fix (2026-09-16): called at every actual radio-
        keying call site (`_send_channel_fastpath_frame`, one iteration
        of `_send_channel_multifragment_pass`'s per-fragment loop,
        `_send_direct_frame`, `_send_bind_frame`) immediately before the
        `send_msg`/`send_chan_msg` command itself -- estimates `frame`'s
        airtime from `duty_cycle_estimate_bitrate` (see that config
        value's own comment for why this is deliberately NOT the same as
        `bitrate`, confirmed via a real zero-hop field test the day this
        shipped -- reusing `bitrate` massively overestimated real per-
        frame airtime and forced an artificial ~10s wait on every single
        exchange), waits out whatever `_DutyCycleLimiter` says is needed,
        then records the estimate as consumed. Returns the delay actually
        applied -- logged at debug level and available to the caller for
        capture, never gates *whether* the send proceeds, only *when*
        it's allowed to start. A no-op returning 0.0 immediately when
        `duty_cycle_enabled` is off."""
        if not self.duty_cycle_enabled or self.duty_cycle_estimate_bitrate <= 0:
            return 0.0
        estimated_s = self._estimate_tx_airtime_s(frame, on_air_bytes=on_air_bytes)
        budget_s = self.duty_cycle_window_s * self.duty_cycle_max_fraction
        if estimated_s > budget_s and not getattr(self, "_duty_cycle_overrun_warned", False):
            # MeshBench finding 1 (2026-09-20): one absurd radio parameter
            # turned into one frame per window with nothing in the log.
            self._duty_cycle_overrun_warned = True
            RNS.log(
                f"{self}: a single {len(frame)}-char frame is estimated at {estimated_s:.0f}s of airtime, "
                f"more than the whole duty-cycle budget ({budget_s:.0f}s per {self.duty_cycle_window_s:.0f}s window) "
                f"-- radio params {self._radio_params!r}; outbound traffic will crawl at one frame per window "
                f"until the radio block is sane.",
                RNS.LOG_WARNING,
            )
        if exempt:
            # Link-maintenance traffic: charged, never delayed.
            self._duty_cycle.record(estimated_s)
            self._debug(
                f"duty-cycle: handshake-class {len(frame)}-char frame sent without waiting "
                f"for budget ({estimated_s:.2f}s airtime still charged to the window)."
            )
            return 0.0
        delay = await self._duty_cycle.wait_for_budget(estimated_s, interrupt=interrupt)
        self._duty_cycle.record(estimated_s)
        if delay > 0:
            self._debug(
                f"duty-cycle throttle: waited {delay:.2f}s before this "
                f"{f'{on_air_bytes}-byte raw' if on_air_bytes is not None else f'{len(frame)}-char'} frame "
                f"(estimated {estimated_s:.2f}s airtime, "
                f"{'LoRa model' if self._radio_params is not None else f'duty_cycle_estimate_bitrate={self.duty_cycle_estimate_bitrate}bps'})."
            )
        return delay

    async def _wait_for_incoming_quiet(self) -> float:
        """User-requested fix (2026-09-16): "if we hear a message come in
        via direct, we wait 3 seconds to hear another before we send
        again... wait for the incoming interface to either stop sending
        or hit its airtime limit." Called at the same radio-keying call
        sites as `_throttle_for_duty_cycle`, immediately before it, so
        this interface never keys the radio into the middle of a peer's
        own DIRECT burst it just heard evidence of (a received fragment
        is strong evidence more are likely still coming, if the peer is
        mid multi-fragment transfer).

        A rolling window, not a single fixed sleep: hearing another
        in-progress DIRECT fragment while already waiting
        (`_last_incoming_direct_at` moving forward, updated by
        `_handle_direct_multifragment_frame` -- see that method's own
        docstring for the 2026-09-18 narrowing) pushes the deadline out
        again, the same "keeps checking, wakes exactly when the deadline
        moves" shape `_DutyCycleLimiter.wait_for_budget` already uses.
        Since this interface has no way to actually observe a peer's own
        airtime budget or duty-cycle state, "or hit its airtime limit" is
        approximated by `incoming_quiet_defer_max_wait_s` -- a bound on
        this node's *own* patience, so a continuously-chatty peer can
        never starve this node's own outgoing traffic indefinitely.
        Returns the delay actually applied (0.0 if none was needed, e.g.
        nothing has been heard yet this session, or the last frame was
        already longer ago than the quiet window).

        Field-diagnosed fix (2026-09-18): originally reset on *any* DIRECT
        frame heard (ACKs, PROOFs, completion-checks, a fragment that
        completed its own bucket), not just "this peer still has more
        fragments of this transfer coming." On a link where both nodes are
        constantly exchanging that other traffic, a genuine 3s lull rarely
        occurred, so nearly every send -- including the fragment retries
        racing the receiver's own `reassembly_idle_timeout_s` -- got pushed
        toward the 15s patience ceiling. Real capture evidence
        (2026-09-18 field test): 0/8 messages completed, fragment gaps
        widening from ~20s to 60-90s apart within one run, then a later
        window with zero incoming fragments and 0% outgoing DIRECT success
        for 16+ minutes straight. See the module docstring's 2026-09-18
        entry for the full root-cause writeup and the paired fix in
        `_pre_transmit_gate`/`_send_direct_frame` (every transmission but a
        message's genuinely-first one skips this wait entirely -- they're
        already racing a clock, not being polite)."""
        if not self.incoming_quiet_defer_enabled or self._last_incoming_direct_at is None:
            return 0.0
        start = time.monotonic()
        hit_patience_ceiling = False
        while True:
            now = time.monotonic()
            total_waited = now - start
            if total_waited >= self.incoming_quiet_defer_max_wait_s:
                hit_patience_ceiling = True
                break
            quiet_for = now - self._last_incoming_direct_at
            if quiet_for >= self.incoming_quiet_window_s:
                break
            remaining_quiet = self.incoming_quiet_window_s - quiet_for
            remaining_patience = self.incoming_quiet_defer_max_wait_s - total_waited
            await asyncio.sleep(max(0.01, min(remaining_quiet, remaining_patience)))
        if total_waited > 0:
            self._debug(
                f"incoming-quiet defer: waited {total_waited:.2f}s for the "
                f"channel to go quiet after last hearing a DIRECT frame"
                + (" (hit incoming_quiet_defer_max_wait_s ceiling)" if hit_patience_ceiling else "")
                + "."
            )
        return total_waited

    async def _pre_transmit_gate(
        self, frame: str, skip_quiet_defer: bool = False, duty_cycle_exempt: bool = False,
        on_air_bytes: Optional[int] = None, interrupt: "Optional[asyncio.Event]" = None,
    ) -> "tuple[float, float, float]":
        """Code-review fix: `await self._wait_for_incoming_quiet()` then
        `await self._throttle_for_duty_cycle(frame)`, in that order, used
        to be copy-pasted verbatim at every one of this interface's radio-
        keying call sites (`_send_channel_fastpath_frame`, one iteration of
        `_send_channel_multifragment_pass`'s per-fragment loop,
        `_send_direct_frame`, `_send_bind_frame`) -- both methods' own
        docstrings already said as much ("called at every radio-keying
        call site"), but nothing enforced it structurally: a future fifth
        send path could easily add a `_run_command` call without either
        line and silently reintroduce the airtime/collision problems these
        two mechanisms were field-fix additions for. One call here covers
        both, in the required order, for every current and future send
        site.

        Field-diagnosed fix (2026-09-18, see module docstring): `skip_
        quiet_defer` lets a caller that's already racing a
        `reassembly_idle_timeout_s` clock the receiver has running skip
        `_wait_for_incoming_quiet` entirely. Threaded down from
        `_send_direct_fragmented` as `time_critical`, it covers every
        transmission of a multi-fragment message except the very first
        fragment's first attempt: continuation fragments (the receiver's
        bucket -- and its idle clock -- opened when fragment 0 landed),
        internal retries, and pass-1 re-drives alike. That wait is a
        heuristic collision-avoidance courtesy, reasonable for a fresh send
        but actively counterproductive once a deadline is already running
        on the other end: a late fragment is worse than a slightly-risky
        one. `_throttle_for_duty_cycle` is never skipped -- it enforces
        this node's own real self-imposed airtime cap, not a politeness
        heuristic, and a retry storm is exactly the case that cap exists
        to bound.

        Observability addition (2026-09-18, user-requested field-tuning
        data): returns `(quiet_defer_wait_s, duty_cycle_wait_s,
        medium_hold_wait_s)` -- the delays this call actually applied
        (the third added by step 4's `_wait_for_medium_clear`), instead
        of discarding them.
        `_send_direct_frame` forwards these into an optional `gate_
        telemetry` out-dict so `_capture_direct_attempt_result` can record
        exactly how much of a DIRECT attempt's own latency was this gate
        versus `_direct_exchange_lock` queueing versus the ACK wait itself
        -- the three previously had to be told apart by comparing separate
        `_debug` log lines by hand. The three callers that don't need this
        (`_send_channel_fastpath_frame`, `_send_channel_multifragment_
        pass`, `_send_bind_frame`) are unaffected -- they already discarded
        the old `None` return the same way they discard this tuple."""
        quiet_defer_wait_s = 0.0
        if not skip_quiet_defer:
            quiet_defer_wait_s = await self._wait_for_incoming_quiet()
        duty_cycle_wait_s = await self._throttle_for_duty_cycle(
            frame, exempt=duty_cycle_exempt, on_air_bytes=on_air_bytes, interrupt=interrupt,
        )
        # Step 4 (2026-09-18): last, so it reflects whatever was overheard
        # during the two waits above. A no-op unless rx_log_holds_enabled.
        medium_hold_wait_s = await self._wait_for_medium_clear()
        # Stamped here, not at the send_msg/send_chan_msg call itself: this
        # is the last common point every radio-keying path passes through,
        # and the command is issued immediately after this returns.
        self._last_own_tx_at = time.monotonic()
        return quiet_defer_wait_s, duty_cycle_wait_s, medium_hold_wait_s

    # -------------------------------------------------------------------
    # Connection bring-up
    # -------------------------------------------------------------------

    def _connection_description(self) -> str:
        if self.transport == "serial":
            return f"serial port={self.port} baudrate={self.baudrate}"
        if self.transport == "ble":
            return f"ble name={self.ble_name or '<first found>'}"
        if self.transport == "tcp":
            return f"tcp host={self.host} port={self.tcp_port}"
        return f"unknown transport '{self.transport}'"

    async def _connect(self, MeshCore):
        # Confirmed directly against the installed meshcore library
        # (2.3.9.1) while building this milestone: MeshCore.connect()
        # only cleans up its own dispatcher task on a *graceful* refusal
        # (the underlying connection_manager.connect() returning None) --
        # a raw OSError from the socket/serial layer itself (e.g. a
        # refused TCP connection, no device at the port) propagates
        # straight out of create_serial/create_ble/create_tcp without
        # that cleanup running, and without ever handing this method a
        # MeshCore instance to call disconnect() on. The visible symptom
        # is a harmless "Task was destroyed but it is pending" asyncio
        # warning logged once per failed connection attempt -- confirmed
        # to not affect this interface's own behavior (the exception is
        # still caught and self.online correctly stays False), but worth
        # recording here rather than re-diagnosing it as this interface's
        # own bug the next time it's seen in a log.
        if self.transport == "serial":
            return await MeshCore.create_serial(
                self.port,
                self.baudrate,
                auto_reconnect=self.auto_reconnect,
                max_reconnect_attempts=self.max_reconnect_attempts,
            )
        if self.transport == "ble":
            return await MeshCore.create_ble(
                self.ble_name or None,
                auto_reconnect=self.auto_reconnect,
                max_reconnect_attempts=self.max_reconnect_attempts,
            )
        if self.transport == "tcp":
            return await MeshCore.create_tcp(
                self.host,
                self.tcp_port,
                auto_reconnect=self.auto_reconnect,
                max_reconnect_attempts=self.max_reconnect_attempts,
            )
        raise ValueError(f"unknown transport '{self.transport}' (expected serial, ble, or tcp)")

    def _subscribe_connection_events(self):
        # Only ever called from _async_setup right after its own
        # `self._mc is None` guard passes -- `_mc_ready` documents that
        # instead of repeating the guard here.
        self._mc_ready.subscribe(self._EventType.CONNECTED, self._on_mc_connected)
        self._mc_ready.subscribe(self._EventType.DISCONNECTED, self._on_mc_disconnected)

    def _on_mc_connected(self, event):
        if self.detached:
            return
        was_offline = not self.online
        reconnected = bool((event.payload or {}).get("reconnected")) if isinstance(getattr(event, "payload", None), dict) else False
        self.online = True
        if was_offline or reconnected:
            RNS.log(
                f"{self}: MeshCore connection (re)established"
                + (" (library auto-reconnect)." if reconnected and not was_offline else "."),
                RNS.LOG_INFO,
            )
            # Code-review fix: _start_auto_message_fetching() was only
            # ever called once, from _async_setup. If a reconnect's own
            # internal get_msg() poll ever raises (plausible right as the
            # connection drops), the installed library's own _fetch_
            # messages_loop silently exits for good on any exception and
            # is never restarted on its own -- confirmed directly against
            # its source, which breaks the loop with no re-arm path.
            # Without re-arming here, a reconnect would quietly return
            # this interface to the exact "online but deaf" failure mode
            # the original M5 field-test fix (this same method's sibling)
            # exists to prevent, with no error logged anywhere.
            #
            # Audit fix (2026-09-19): gating this on `was_offline` alone made
            # it dead code for the default configuration. Confirmed against
            # the installed library (meshcore/connection_manager.py:99-121):
            # with `auto_reconnect` on -- the default here -- an unexpected
            # drop does NOT emit DISCONNECTED at all. It silently starts
            # `_attempt_reconnect` and emits CONNECTED{reconnected:True} on
            # success, so `_on_mc_disconnected` never runs, `self.online`
            # never goes False, and `was_offline` is False exactly when the
            # re-arm matters most. DISCONNECTED is only emitted when
            # auto-reconnect is off or every attempt has failed. The
            # `reconnected` flag from the event now also triggers the
            # re-arm; `_rearm_auto_message_fetching` stops first, so doing
            # it once too often is harmless.
            self._spawn_background_task(self._rearm_auto_message_fetching())
            if not self._own_pubkey_hex:
                self._spawn_background_task(self._fetch_own_identity())

    def _on_mc_disconnected(self, event):
        if self.detached:
            return
        if self.online:
            RNS.log(
                f"{self}: MeshCore connection lost -- interface going "
                f"offline until it reconnects.",
                RNS.LOG_WARNING,
            )
        self.online = False

    async def _fetch_own_identity(self) -> None:
        """Code-review fix: this was previously inlined in `_async_setup`
        and only ever attempted once, at initial connect. If `send_appstart`
        failed there (a plausible timing issue right after the radio link
        comes up), `_own_pubkey_hex` stayed empty for the interface's
        entire lifetime -- and `_own_pubkey_prefix()` returning None makes
        the self-echo guard in `_handle_incoming_bind_frame` (`own_prefix
        is not None and frame.pubkey_prefix == own_prefix`) silently skip
        the check forever, meaning this node's own bind frames bouncing
        back to it (e.g. via a repeater or CHANNEL rebroadcast) would be
        misprocessed as if from a genuine peer. Extracted into its own
        method so `_on_mc_connected` can retry it on every reconnect, not
        just at the very first one, closing that permanent-failure window.
        """
        try:
            # Only ever called once a connection is live (from _async_setup
            # or _on_mc_connected's reconnect path) -- see `_mc_ready`.
            result = await self._run_command(
                self._mc_ready.commands.send_appstart(),
                "send_appstart",
                self._EventType.SELF_INFO,
            )
        except Exception as exc:
            RNS.log(
                f"{self}: could not fetch node identity: {exc} -- "
                f"continuing anyway, but the CHANNEL payload budget below "
                f"will assume an empty node name until this succeeds; if "
                f"this node actually has a name configured, outgoing "
                f"CHANNEL fragments could be silently truncated by the "
                f"firmware as a result. This will be retried on the next "
                f"reconnect if the pubkey is still unknown by then.",
                RNS.LOG_WARNING,
            )
        else:
            info = result.payload if isinstance(result.payload, dict) else {}
            self._own_node_name = info.get("name", "")
            node_key = info.get("public_key", "")
            self._own_pubkey_hex = node_key.lower()
            params = self._parse_radio_params(info)
            if params is not None:
                self._radio_params = params
            elif any(k in info for k in ("radio_sf", "radio_bw", "radio_cr")):
                RNS.log(
                    f"{self}: SELF_INFO radio block is implausible (sf={info.get('radio_sf')!r} "
                    f"bw={info.get('radio_bw')!r}kHz cr={info.get('radio_cr')!r}) -- ignoring it; airtime "
                    f"estimates fall back to duty_cycle_estimate_bitrate until a sane block arrives.",
                    RNS.LOG_WARNING,
                )
            RNS.log(
                f"{self}: node identity '{self._own_node_name}' "
                f"key={node_key[:16]}...",
                RNS.LOG_INFO,
            )

    @staticmethod
    def _parse_radio_params(info: dict) -> "Optional[tuple]":
        """The radio's (sf, bw_khz, cr) from a SELF_INFO payload, or None
        when the block is missing or implausible. MeshBench finding 1
        (2026-09-20): a fresh-booted companion once reported bw as 0.063 kHz,
        which the old check (`sf >= 5 and bw > 0`) accepted; the LoRa model
        then priced a 38-byte frame at 1160s and the duty-cycle limiter let
        one frame out per minute, unlogged. LoRa bounds: SF 5-12, BW 7.8-500
        kHz, CR 5-8."""
        try:
            sf, bw, cr = int(info.get("radio_sf", 0)), float(info.get("radio_bw", 0)), int(info.get("radio_cr", 0))
        except (TypeError, ValueError):
            return None
        if 5 <= sf <= 12 and 7.8 <= bw <= 500.0 and 5 <= cr <= 8:
            return (sf, bw, cr)
        return None

    async def _async_setup(self):
        self._command_lock_impl = asyncio.Lock()
        self._direct_exchange_lock_impl = _PriorityAsyncLock()
        self._duty_cycle_impl = _DutyCycleLimiter(self.duty_cycle_window_s, self.duty_cycle_max_fraction)
        MeshCore = self._mc_module.MeshCore

        try:
            self._mc = await self._connect(MeshCore)
        except Exception as exc:
            RNS.log(
                f"{self}: connection failed ({self._connection_description()}): {exc}",
                RNS.LOG_ERROR,
            )
            return

        if self._mc is None:
            RNS.log(
                f"{self}: driver init returned no MeshCore instance "
                f"({self._connection_description()}).",
                RNS.LOG_ERROR,
            )
            return

        RNS.log(f"{self}: connected ({self._connection_description()}).", RNS.LOG_INFO)
        self._subscribe_connection_events()

        await self._fetch_own_identity()

        if self.radio_freq and self.radio_bw and self.radio_sf and self.radio_cr:
            try:
                await self._run_command(
                    self._mc_ready.commands.set_radio(
                        self.radio_freq, self.radio_bw, self.radio_sf, self.radio_cr
                    ),
                    "set_radio",
                    self._EventType.OK,
                )
                RNS.log(
                    f"{self}: radio override applied (freq={self.radio_freq}MHz "
                    f"bw={self.radio_bw}kHz sf={self.radio_sf} cr={self.radio_cr}).",
                    RNS.LOG_INFO,
                )
                # MeshBench finding 1 (2026-09-20): the airtime model must
                # follow the override, not the pre-override SELF_INFO block.
                params = self._parse_radio_params(
                    {"radio_sf": self.radio_sf, "radio_bw": self.radio_bw, "radio_cr": self.radio_cr}
                )
                if params is not None:
                    self._radio_params = params
            except Exception as exc:
                RNS.log(
                    f"{self}: radio override failed: {exc} -- continuing "
                    f"with the node's currently stored radio settings.",
                    RNS.LOG_WARNING,
                )
        else:
            RNS.log(
                f"{self}: no radio override configured -- using the "
                f"node's currently stored radio settings.",
                RNS.LOG_INFO,
            )

        try:
            secret_bytes = bytes.fromhex(self.channel_secret_hex)
            await self._run_command(
                self._mc_ready.commands.set_channel(
                    self.channel_idx, self.channel_name, secret_bytes
                ),
                "set_channel",
                self._EventType.OK,
            )
            RNS.log(
                f"{self}: channel configured (idx={self.channel_idx} "
                f"name='{self.channel_name}').",
                RNS.LOG_INFO,
            )
            if self._using_default_channel_secret:
                RNS.log(
                    f"{self}: no channel_secret configured -- joining the "
                    f"shared default channel so nodes can find each other "
                    f"with zero setup. This is fine for RNS traffic (it's "
                    f"already encrypted end-to-end); set channel_idx/"
                    f"channel_name/channel_secret explicitly for a private "
                    f"channel.",
                    RNS.LOG_INFO,
                )
        except Exception as exc:
            RNS.log(f"{self}: channel setup failed: {exc}", RNS.LOG_WARNING)

        try:
            await self._run_command(
                self._mc_ready.commands.set_telemetry_mode_base(self.TELEM_MODE_ALLOW_FLAGS),
                "set_telemetry_mode_base",
                self._EventType.OK,
            )
            RNS.log(
                f"{self}: telemetry_mode_base set to per-contact-flags -- "
                f"path discovery (docs/path_discovery_spec.md) is a "
                f"telemetry request under the hood, and only answers a "
                f"peer whose own contact entry has been granted the base "
                f"permission bit.",
                RNS.LOG_INFO,
            )
        except Exception as exc:
            RNS.log(
                f"{self}: setting telemetry_mode_base failed: {exc} -- "
                f"this node may not answer other nodes' path discovery "
                f"requests until this is retried.",
                RNS.LOG_WARNING,
            )

        # Code-review fix: self.online is set True here, BEFORE
        # _load_peer_cache() below, deliberately -- _load_peer_cache()
        # calls _register_peer() per cached entry, which (for a peer new
        # to this process's own _peers dict, true for every cache-
        # restored peer) schedules a proactive discover_path() background
        # task. That task's first real chance to run is at the next
        # await point in this same coroutine -- which used to be *before*
        # self.online was set, so discover_path()'s own first line
        # ("if ... not self.online: return None") silently no-opped for
        # every single cache-restored peer, with no log line at all
        # (unlike the backoff-skip case, which does self._debug()).
        # Confirmed by code review: this made the Milestone 6 "proactive
        # discovery on bind" fix quietly inert for exactly the startup
        # case it was added for. Nothing else in this method depends on
        # self.online being false up to this point.
        self.online = True
        self._connected_since = time.time()
        RNS.log(
            f"{self}: online (bitrate={self.bitrate}bps HW_MTU={self.HW_MTU} "
            f"channel_budget={self._channel_payload_budget()}B "
            f"direct_budget={self._direct_payload_budget()}B).",
            RNS.LOG_INFO,
        )

        if self.packet_capture_enabled:
            self._open_packet_capture()

        if self.peer_discovery_enabled:
            self._load_peer_cache()

        try:
            await self._refresh_contacts_and_grant_telemetry()
        except Exception as exc:
            RNS.log(f"{self}: initial contact refresh failed: {exc}", RNS.LOG_WARNING)

        self._subscribe_data_events()
        await self._start_auto_message_fetching()

        self._stats_task = asyncio.ensure_future(self._stats_loop())
        self._outgoing_worker_task = asyncio.ensure_future(self._outgoing_worker())
        self._reassembly_cleanup_task = asyncio.ensure_future(self._reassembly_cleanup_loop())
        self._contact_refresh_task = asyncio.ensure_future(self._contact_refresh_loop())
        if self.peer_discovery_enabled:
            self._peer_discovery_task = asyncio.ensure_future(self._peer_discovery_bootstrap())
            self._peer_ttl_sweep_task = asyncio.ensure_future(self._peer_ttl_sweep_loop())

    def _raw_fragments_eligible(self, peer_prefix: str, priority: int) -> bool:
        """Whether a too-big-for-one-text-frame packet to `peer_prefix` may
        go as raw fragments: flag on, library support present, the peer
        advertised BIND_CAP_RAW_FRAGMENTS, a resolved path (the raw send
        is source-routed), our own prefix known (it is the src field),
        not handshake priority, and not inside a fallback cooldown."""
        if not self.direct_raw_fragments_enabled or priority == self.PRIORITY_HANDSHAKE:
            return False
        if not (self.direct_fragment_reconcile_enabled and self.direct_completion_check_enabled):
            return False
        if self._mc is None or not hasattr(self._EventType, "RAW_DATA") or not hasattr(self._mc.commands, "send_raw_data"):
            return False
        peer = self._peers.get(peer_prefix)
        if peer is None or not peer.raw_fragments or peer_prefix not in self._resolved_paths:
            return False
        if self._own_pubkey_prefix() is None:
            return False
        path_hex = self._resolved_paths[peer_prefix].out_path_hex or ""
        if path_hex and self._raw_path_unsupported(path_hex):
            return False
        until = self._raw_disabled_until.get(peer_prefix)
        return not (until is not None and time.monotonic() < until)

    def _raw_path_unsupported(self, path_hex: str) -> bool:
        note = self._raw_unsupported_paths.get(path_hex)
        if note is None:
            return False
        if time.monotonic() - note["since"] >= self.direct_raw_path_unsupported_ttl_s:
            del self._raw_unsupported_paths[path_hex]
            return False
        return True

    def _note_raw_fallback_outcome(self, peer_prefix: str, path_hex: str, text_ok: bool) -> None:
        """Called after the Z85 text send that followed a raw fallback on
        `path_hex`. Text succeeded -> the chain drops raw packets: note the
        path and lift the peer's pause. Text failed too -> the path is
        sick; nothing is concluded about raw."""
        if not text_ok:
            self._debug(
                f"raw fallback to {peer_prefix!r} on path {path_hex or '<zero-hop>'}: the Z85 text send failed as "
                f"well -- a path problem, not a raw one; raw stays paused for the cooldown only."
            )
            return
        if not path_hex:
            # Zero hop: no repeater to blame -- the peer's own radio did not
            # deliver raw frames. The per-peer pause already covers it.
            RNS.log(
                f"{self}: Z85 text to {peer_prefix!r} succeeded at zero hop where raw fragments did not -- "
                f"raw paused for this peer for {self.direct_raw_fallback_cooldown_s:.0f}s.",
                RNS.LOG_WARNING,
            )
            return
        self._raw_unsupported_paths[path_hex] = {"since": time.monotonic(), "peer": peer_prefix}
        self._raw_disabled_until.pop(peer_prefix, None)
        RNS.log(
            f"{self}: Z85 text to {peer_prefix!r} over path {path_hex} succeeded where raw fragments did not -- "
            f"a repeater in that chain does not carry raw packets; noted for "
            f"{self.direct_raw_path_unsupported_ttl_s / 3600:.0f}h (raw resumes on a different path).",
            RNS.LOG_WARNING,
        )

    # --- Shared fragmented-send helpers (refactor, 2026-09-19) ---------------
    # The text and raw fragmented senders used to carry byte-identical
    # copies of these four pieces; the review that day found three logic
    # gaps in exactly that duplicated region. One copy each, now.

    def _resume_state(self, resume: Optional[dict], frag_total: int, pkt_id: int, peer_prefix: str,
                      raw: bool) -> "tuple[list, bool]":
        """(acked, resumed) to start a fragmented send from: everything
        False for a fresh send, or the remembered per-fragment state when
        `resume` matches this send's fragment count. Logs and captures a
        `direct_resume` record when resuming."""
        if resume is None or resume.get("frag_total") != frag_total or len(resume.get("acked", ())) != frag_total:
            return [False] * frag_total, False
        acked = list(resume["acked"])
        held_before = [i for i, a in enumerate(acked) if a]
        self._debug(
            f"{'RAW' if raw else 'DIRECT'} fragmented send pkt_id={pkt_id} to {peer_prefix!r}: RESUMING a failed "
            f"send -- receiver believed to hold {held_before} of {frag_total}."
        )
        if self._packet_capture_file is not None:
            record = {"event": "direct_resume", "peer_prefix": peer_prefix, "pkt_id": pkt_id,
                      "frag_total": frag_total, "held_before": held_before}
            if raw:
                record["raw"] = True
            self._capture_event("out", record)
        return acked, True

    def _remember_resumable(self, resume_key, pkt_id: int, frag_total: int, acked: list,
                            last_progress_at: Optional[float]) -> None:
        """A failed fragmented send with something delivered is worth
        resuming if RNS re-issues these bytes while the receiver's bucket
        is still alive (its idle clock restarted at our last confirmed
        delivery; keep a 25% margin under its timeout)."""
        if resume_key is None or not any(acked) or last_progress_at is None:
            return
        self._resumable_sends[resume_key] = {
            "pkt_id": pkt_id, "frag_total": frag_total, "acked": list(acked),
            "expires_at": last_progress_at + 0.75 * self.reassembly_idle_timeout_s,
        }

    def _held_from_answer(self, answer: "_CompletionFrame", frag_total: int) -> "Optional[set]":
        """The fragments a completion ANSWER says the receiver holds, or
        None when the answer carries no per-fragment information (a v1
        ANSWER without a bitmap -- audit fix 2026-09-19: never read that
        as "holds nothing")."""
        if answer.complete:
            return set(range(frag_total))
        if answer.held is None:
            return None
        return set(answer.held)

    def _rtt_sample(self, table: dict, peer_prefix: Optional[str], rtt_s: float, keep_last: bool = False) -> None:
        """One Jacobson/Karels update (srtt alpha 1/8, rttvar beta 1/4;
        the first sample seeds srtt directly and rttvar at half of it, as
        RFC 6298 does) into `table[peer_prefix]` -- shared by the ACK and
        QUERY round-trip estimators (refactor, 2026-09-19)."""
        if peer_prefix is None or rtt_s <= 0:
            return
        st = table.get(peer_prefix)
        if st is None:
            st = {"srtt": rtt_s, "rttvar": rtt_s / 2.0, "samples": 1}
            if keep_last:
                st["last_rtt"] = rtt_s
            table[peer_prefix] = st
            return
        err = rtt_s - st["srtt"]
        st["rttvar"] = 0.75 * st["rttvar"] + 0.25 * abs(err)
        st["srtt"] = st["srtt"] + 0.125 * err
        st["samples"] += 1
        if keep_last:
            st["last_rtt"] = rtt_s

    def _clear_peer_path_stats(self, peer_prefix: str, reason: str = "") -> None:
        """Everything measured about one peer's CURRENT path (refactor,
        2026-09-19 -- one list instead of two hand-maintained copies in
        `_invalidate_ack_rtt` and `_forget_peer_state`): RTT snapshot, echo
        timings, the firmware's last ACK bound, the QUERY round trip, and
        the raw pause/pending verdict. A new path is a new repeater chain.

        Logged (second audit, 2026-09-19 evening): this clear is what
        disarms the hop-1 abort until fresh echo samples exist, and it was
        silent -- the one question that session's terminal logs could not
        answer directly."""
        echo_samples = len(self._echo_stats.get(peer_prefix) or ())
        self._debug(
            f"clearing measured path stats for {peer_prefix!r} ({reason or 'path change'}): "
            f"{echo_samples} echo sample(s) dropped -- hop-1 abort falls back to the session pool "
            f"({len(self._echo_stats_all)} sample(s)) or direct_hop1_abort_default until re-measured."
        )
        self._ack_rtt_snapshot.pop(peer_prefix, None)
        self._echo_stats.pop(peer_prefix, None)
        self._last_firmware_ack_timeout_s.pop(peer_prefix, None)
        self._query_rtt.pop(peer_prefix, None)
        self._report_rtt.pop(peer_prefix, None)
        self._raw_disabled_until.pop(peer_prefix, None)
        self._raw_incomplete_strikes.pop(peer_prefix, None)
        self._direct_path_recent_success.pop(peer_prefix, None)
        for k in [k for k in self._raw_fallback_pending if k[0] == peer_prefix]:
            self._raw_fallback_pending.pop(k, None)

    async def _wait_future_or_preempt(self, fut: "asyncio.Future", timeout_s: float) -> "tuple[bool, bool]":
        """Await `fut` (shielded: it outlives this wait) for up to
        `timeout_s`, ending early when a Link handshake queues for the
        radio lock (phase 1, 2026-09-20). Returns `(future_done, cut_by_a
        _handshake)`; the future's own exception is the caller's."""
        if fut.done():
            return True, False
        event = self._direct_exchange_lock.preempt_event()
        if event.is_set():
            return False, True
        if timeout_s <= 0:
            return False, False
        loop = asyncio.get_running_loop()
        fut_wait = loop.create_task(asyncio.wait_for(asyncio.shield(fut), timeout=timeout_s))
        preempt_wait = loop.create_task(event.wait())
        try:
            done, _pending = await asyncio.wait({fut_wait, preempt_wait}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (fut_wait, preempt_wait):
                if not t.done():
                    t.cancel()
            # Retrieve the timed-out / cancelled task's exception so asyncio
            # does not log "Task exception was never retrieved".
            for t in (fut_wait, preempt_wait):
                try:
                    await t
                except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    pass
        if fut.done():
            return True, False
        return False, preempt_wait in done

    async def _idle_hold(self, seconds: float, floor_s: float = 0.0) -> bool:
        """Sleep `seconds` with the radio lock held, but return early
        (True) once a link handshake is queued for the lock and at least
        `floor_s` has passed (phase 1, 2026-09-20). The lock's pre-empt
        event is the signal; False when the whole time elapsed."""
        if seconds <= 0:
            return False
        event = self._direct_exchange_lock.preempt_event()
        started = time.monotonic()
        if floor_s > 0:
            await asyncio.sleep(min(seconds, floor_s))
            if seconds <= floor_s:
                return False
        remaining = seconds - (time.monotonic() - started)
        if remaining <= 0:
            return False
        if event.is_set():
            return True
        try:
            await asyncio.wait_for(event.wait(), timeout=remaining)
            return True
        except asyncio.TimeoutError:
            return False

    def _spawn_background_task(self, coro) -> "asyncio.Task":
        task = asyncio.ensure_future(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        task.add_done_callback(self._log_background_task_exception)
        return task

    def _log_background_task_exception(self, task: "asyncio.Task") -> None:
        """Code-review fix: `_outgoing_worker` wraps its own synchronously-
        awaited `_send_outgoing_packet` call in try/except and logs any
        exception via `RNS.log` -- but every fire-and-forget send this
        interface spawns instead (`_send_direct_packet`,
        `_send_direct_supplement`, the bootstrap `_discover_path_coalesced`
        call from `_register_peer`, etc.) only ever reaches this
        interface's own observability surface if it happens to guard its
        own internal awaits already; an exception that does escape one is
        otherwise only ever surfaced via asyncio's own default "Task
        exception was never retrieved" warning at garbage-collection time
        -- invisible to `RNS.log`, to the periodic `_stats_loop` snapshot,
        and to anyone monitoring this interface the way CLAUDE.md's
        observability requirement expects. This done-callback (attached to
        every task `_spawn_background_task` creates) closes that gap."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            RNS.log(f"{self}: background task raised an unhandled exception: {exc}", RNS.LOG_ERROR)
            RNS.log(
                "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                RNS.LOG_ERROR,
            )

    # -------------------------------------------------------------------
    # Outgoing (RNS core -> this interface)
    # -------------------------------------------------------------------

    def process_outgoing(self, data):
        # Called synchronously by RNS core on RNS's own thread -- must
        # never block. Per docs/reliability_engine_design.md's sync/async
        # bridge notes, this does only a plain thread-safe queue.put and
        # returns immediately; _outgoing_worker (running on this
        # interface's own event loop) does the actual encode/send. The
        # RNS header is parsed once, here, and carried through the queue
        # item -- both the priority decision (now) and the retry-extra
        # decision (_send_outgoing_packet, once this item is dequeued)
        # read the same parse rather than redoing it.
        if not self.online or self.detached:
            return
        raw = bytes(data)
        header = self._parse_rns_header(raw)
        self._note_link_closed(header)
        priority = self._priority_tier(header)
        # Field fix (2026-09-18 evening): identical bytes already queued or
        # in flight -> drop. RNS's Resource layer re-requests parts every
        # ~27s while the earlier copy is still waiting on the duty-cycle
        # limiter; the page-load capture queued 26 RESOURCE packets for 12
        # distinct payloads. The receiver would dedup them anyway.
        # Field fix (2026-09-19 evening session): the suppression above is
        # right in the common case and wrong when the in-flight entry is
        # STUCK. Measured deadlock, both captures agreeing: resource part
        # `ca6b3d36db27` was transmitted at 16:48:06 and fully delivered to
        # the peer's RNS at 16:48:25 -- but the peer's RNS did not credit it
        # and re-requested it, and this side then refused SEVEN consecutive
        # re-sends (16:49:55 through 16:52:53) because the original send's
        # in-flight entry never cleared: its completion checks kept timing
        # out, so `_release_inflight_when_done` never fired. 178 seconds --
        # 42% of that 442-second transfer -- were spent with RNS correctly
        # asking and this interface correctly-but-fatally declining, until an
        # unrelated retry path finally sent a fresh copy. So: after
        # `outgoing_duplicate_suppress_limit` consecutive suppressions of the
        # same bytes, let the packet through and start a fresh in-flight
        # entry. RNS only re-requests a Resource part it believes it lacks,
        # so by the third ask its belief should win over ours.
        inflight_key = RNS.Identity.truncated_hash(raw)
        with self._outgoing_inflight_lock:
            duplicate = inflight_key in self._outgoing_inflight
            if duplicate:
                n = self._outgoing_duplicate_suppressed.get(inflight_key, 0) + 1
                self._outgoing_duplicate_suppressed[inflight_key] = n
                if n >= max(1, self.outgoing_duplicate_suppress_limit):
                    # Force it through: replace the stuck entry with a fresh one.
                    duplicate = False
                    self._outgoing_duplicate_suppressed.pop(inflight_key, None)
                    self._outgoing_inflight[inflight_key] = time.monotonic()
                    forced = n
                else:
                    forced = 0
            else:
                forced = 0
                self._outgoing_duplicate_suppressed.pop(inflight_key, None)
                self._outgoing_inflight[inflight_key] = time.monotonic()
        if duplicate:
            self._outgoing_dropped_total += 1
            self._capture_outgoing(header, raw, "duplicate_in_flight")
            self._debug(
                f"dropping outgoing packet ({len(raw)} bytes, "
                f"{self._payload_correlation_hash(raw)}) -- identical bytes are already "
                f"queued or in flight."
            )
            return
        if forced:
            RNS.log(
                f"{self}: re-sending a packet ({len(raw)} bytes, "
                f"{self._payload_correlation_hash(raw)}) that was suppressed {forced} time(s) as "
                f"already in flight -- RNS keeps asking for it, so the in-flight entry is "
                f"treated as stuck and replaced.",
                RNS.LOG_WARNING,
            )
        seq = next(self._outqueue_seq)
        try:
            self._outqueue.put_nowait((priority, seq, raw, header, time.monotonic(), inflight_key))
        except queue.Full:
            self._release_inflight(inflight_key)
            self._outgoing_dropped_total += 1
            RNS.log(
                f"{self}: dropping outgoing packet ({len(data)} bytes) -- "
                f"outgoing queue is full ({self.OUTQUEUE_MAXSIZE} items "
                f"already queued).",
                RNS.LOG_WARNING,
            )

    async def _outgoing_worker(self):
        """Drains self._outqueue via a thread-pool executor, so the
        queue's blocking get() never blocks this event loop while
        waiting for the next packet -- the other half of the
        process_outgoing split above. Milestone 3 upgraded this from a
        plain FIFO to the two-tier PriorityQueue docs/
        reliability_engine_design.md §3 specifies, without changing this
        drain pattern. Exits cleanly on _OUTQUEUE_SHUTDOWN_SENTINEL,
        which detach() places in the queue (a plain task-cancel can't
        interrupt the blocking get() itself, since it runs on a separate
        executor thread)."""
        loop = asyncio.get_running_loop()
        while True:
            # Bounded wait (2026-09-18 night): an unbounded queue.get() here
            # parks a non-daemon executor thread forever if this interface
            # is never detach()ed (a test whose setUp failed, a script that
            # exits without detaching), and Python joins that thread at
            # interpreter exit -- the process hangs. One second bounds it.
            try:
                priority, seq, data, header, enqueued_at, inflight_key = await loop.run_in_executor(
                    None, self._outqueue.get, True, 1.0,
                )
            except queue.Empty:
                if self.detached:
                    return
                continue
            if data is None:
                self._outqueue.task_done()
                return
            spawned: list = []
            try:
                # Field fix (2026-09-18 evening): outgoing_max_age -- see
                # that config's own comment. ANNOUNCE never expires, nor do
                # Resource data parts (RNS's Resource layer owns those).
                expires_at = None
                if self.outgoing_max_age_s > 0 and not (
                    header is not None and (
                        header.packet_type == RNS.Packet.ANNOUNCE
                        or header.context == RNS.Packet.RESOURCE
                    )
                ):
                    expires_at = enqueued_at + self.outgoing_max_age_s
                plain_proof = self._plain_proof(header)
                if plain_proof and self.proof_max_age_s > 0:
                    # Phase 1 (2026-09-20): see proof_max_age.
                    proof_deadline = enqueued_at + self.proof_max_age_s
                    expires_at = proof_deadline if expires_at is None else min(expires_at, proof_deadline)
                if self._link_closed(header):
                    self._outgoing_dropped_total += 1
                    self._capture_outgoing(header, data, "link_closed")
                    RNS.log(
                        f"{self}: dropping outgoing packet ({len(data)} bytes) -- its Link "
                        f"{header.destination_hash.hex()} was closed "
                        f"{time.monotonic() - self._closed_links[header.destination_hash]:.0f}s ago.",
                        RNS.LOG_DEBUG,
                    )
                elif self._expired(expires_at):
                    self._outgoing_dropped_total += 1
                    self._capture_outgoing(header, data, "proof_expired_in_queue" if plain_proof else "expired_in_queue")
                    RNS.log(
                        f"{self}: dropping outgoing {'PROOF' if plain_proof else 'packet'} ({len(data)} bytes) -- sat "
                        f"{time.monotonic() - enqueued_at:.0f}s in the outgoing queue, past "
                        + (f"proof_max_age={self.proof_max_age_s:.0f}s." if plain_proof and self.proof_max_age_s > 0
                           else f"outgoing_max_age={self.outgoing_max_age_s:.0f}s."),
                        RNS.LOG_WARNING,
                    )
                else:
                    await self._send_outgoing_packet(data, header, expires_at=expires_at, spawned=spawned)
            except Exception as exc:
                RNS.log(
                    f"{self}: unexpected error sending an outgoing packet: {exc}",
                    RNS.LOG_ERROR,
                )
            finally:
                self._outqueue.task_done()
                self._release_inflight_when_done(inflight_key, spawned)

    def _release_inflight(self, inflight_key) -> None:
        if inflight_key is None:
            return
        with self._outgoing_inflight_lock:
            self._outgoing_inflight.pop(inflight_key, None)
            self._outgoing_duplicate_suppressed.pop(inflight_key, None)

    def _release_inflight_when_done(self, inflight_key, spawned: list) -> None:
        """Releases a packet's `_outgoing_inflight` entry once every send
        task `_dispatch_outgoing_packet` spawned for it has finished --
        success or failure -- so a genuinely failed copy can be re-sent
        by RNS immediately, while a copy still working through the queue
        or the radio lock keeps its duplicates out."""
        live = [t for t in spawned if t is not None and not t.done()]
        if not live:
            self._release_inflight(inflight_key)
            return

        async def _wait_then_release():
            await asyncio.gather(*live, return_exceptions=True)
            self._release_inflight(inflight_key)

        self._spawn_background_task(_wait_then_release())

    def _resumable_sends_sweep(self, now: float) -> None:
        """Alpha 0.1.1: a failed fragmented send is only worth resuming
        while the receiver's bucket can still be alive."""
        expired = [k for k, v in self._resumable_sends.items() if now >= v["expires_at"]]
        for k in expired:
            del self._resumable_sends[k]

    def _outgoing_inflight_sweep(self, now: float) -> None:
        """Safety net only: an entry should always be released by
        `_release_inflight_when_done`; anything older than ten minutes is
        a leak (a send path that raised before spawning, say) and is
        cleared so it can never block a destination for good."""
        with self._outgoing_inflight_lock:
            stale = [k for k, t in self._outgoing_inflight.items() if now - t > 600.0]
            for k in stale:
                del self._outgoing_inflight[k]

    def _expired(self, expires_at: Optional[float]) -> bool:
        """outgoing_max_age check -- `expires_at` is a time.monotonic()
        deadline threaded down from `_outgoing_worker`, or None when the
        packet never expires (ANNOUNCE, or the feature is disabled)."""
        return expires_at is not None and time.monotonic() >= expires_at

    def _in_small_mesh_mode(self) -> bool:
        """True when between 1 and SMALL_MESH_DIRECT_ONLY_MAX_PEERS peers
        are bound (3 since commit ef57809, "Lowered most hard coded delays
        for testing"; the module docstring's 2026-09-15 entry describes the
        original 2) -- deliberately False at exactly 0 bound peers, even
        though 0 is also under the cap: with nobody bound yet there is no DIRECT target to
        send to at all, and CHANNEL is the only way this node's very
        first peer can ever be discovered in the first place (bind frames
        themselves ride CHANNEL regardless of this mode). Re-evaluated on
        every send, not cached -- a peer expiring or a new one binding
        moves this node in or out of small-mesh mode automatically, with
        no restart and no config to touch."""
        peer_count = len(self._peers)
        return 0 < peer_count <= self.SMALL_MESH_DIRECT_ONLY_MAX_PEERS

    def _all_bound_peer_prefixes(self) -> list:
        """Every bound peer, most-recently-seen first -- deliberately
        unfiltered (no router-capability check, no cap) and independent
        of `bootstrap_direct_supplement_cap`/`path_request_direct_
        supplement_cap`, which exist to bound a *supplement* riding
        alongside a mandatory broadcast and would otherwise silently
        under-cover this mode's job of reaching every peer once CHANNEL
        is skipped entirely."""
        peers = sorted(self._peers.values(), key=lambda p: p.last_seen, reverse=True)
        return [p.pubkey_prefix for p in peers]

    async def _send_direct_to_all_peers(
        self, data: bytes, header: Optional[_RnsHeader] = None,
        priority: int = PRIORITY_NORMAL, expires_at: Optional[float] = None,
        spawned: Optional[list] = None,
    ) -> None:
        """Small-mesh replacement for a CHANNEL broadcast: one DIRECT
        copy to every bound peer instead, each spawned independently
        (never gated on another's outcome, same reasoning as every other
        fire-and-forget send in this dispatcher). Reuses
        `_send_direct_supplement` for the actual send -- it handles
        path-resolution-with-discovery, spacing, and bare-vs-fragmented
        dispatch regardless of caller.

        Audit fix (2026-09-19): but it does NOT provide a transport of last
        resort, and in this mode there is no broadcast running alongside to
        cover for it. `_send_direct_supplement` returns False when it never
        reached the radio (no resolved path even after discovery, no
        contact, expired), which for an ordinary supplement is correct --
        the broadcast it supplements already carried the packet. Here it
        would mean silent loss: one failed discovery round arms a cooldown
        of up to `path_discovery_backoff_max` (900s), during which every
        ANNOUNCE, path request and unknown-destination packet vanished with
        no log line, no `_outgoing_dropped_total` and nothing in the
        capture. So this now watches the per-peer results and falls back to
        a single CHANNEL broadcast if no peer got a transmission -- the
        same last-resort `_send_direct_packet` has always had, and not a
        weakening of DIRECT-primary: it fires only when DIRECT could not be
        attempted at all."""
        peer_prefixes = self._all_bound_peer_prefixes()
        tasks = []
        for peer_prefix in peer_prefixes:
            task = self._spawn_background_task(
                self._send_direct_supplement(
                    data, peer_prefix, trigger_discovery=True, priority=priority, expires_at=expires_at,
                    alongside_broadcast=False,
                )
            )
            tasks.append(task)
            if spawned is not None:
                spawned.append(task)

        async def broadcast_if_none_sent() -> None:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            if any(r is True for r in results):
                return
            if self.detached or not self.online or self._expired(expires_at):
                return
            RNS.log(
                f"{self}: small-mesh DIRECT-to-all reached no peer "
                f"({len(peer_prefixes)} bound, no resolvable path) -- falling back to one "
                f"CHANNEL broadcast rather than dropping this packet silently.",
                RNS.LOG_WARNING,
            )
            await self._send_broadcast_packet(data, header, expires_at=expires_at)

        fallback = self._spawn_background_task(broadcast_if_none_sent())
        if spawned is not None:
            spawned.append(fallback)

    def _unknown_dest_in_backoff(self, destination_hash: Optional[bytes]) -> bool:
        if destination_hash is None:
            return False
        until = self._unknown_dest_backoff_until.get(destination_hash)
        return until is not None and time.monotonic() < until

    def _recent_path_successes(self, pubkey_prefix: str) -> int:
        """How many DIRECT sends to this peer succeeded within
        `direct_path_healthy_window_s` (field fix, 2026-09-19)."""
        stamps = self._direct_path_recent_success.get(pubkey_prefix)
        if not stamps:
            return 0
        cutoff = time.monotonic() - self.direct_path_healthy_window_s
        fresh = [t for t in stamps if t >= cutoff]
        if len(fresh) != len(stamps):
            self._direct_path_recent_success[pubkey_prefix] = fresh
        return len(fresh)

    def _proof_like(self, header: Optional[_RnsHeader]) -> bool:
        """True for a PROOF, whose on-wire destination field is a one-shot
        value (the truncated hash of the packet it proves, or a link_id) --
        never a stable destination this node will address again."""
        return header is not None and header.packet_type == RNS.Packet.PROOF

    def _record_unknown_dest_attempt(self, destination_hash: Optional[bytes]) -> None:
        """Call once per outgoing packet that actually triggers a DIRECT-
        bootstrap attempt for `destination_hash` (not once per packet
        merely addressed to it while already in backoff -- see call
        sites in `_dispatch_outgoing_packet`). No positive "it failed"
        signal exists at this interface's own layer (a real MeshCore ACK
        from the bound peer only confirms *local* delivery to that peer,
        never that whatever it's being asked to relay ever replied), so
        this uses the same proxy `_send_path_request`'s own supplement
        selection implicitly relies on: repeated attempts with §7's token
        still never learned for this exact destination is itself the
        signal, checked lazily here rather than via any explicit success/
        failure callback threaded back from the send."""
        if destination_hash is None:
            return
        self._unknown_dest_last_attempt[destination_hash] = time.monotonic()
        attempts = self._unknown_dest_attempts.get(destination_hash, 0) + 1
        self._unknown_dest_attempts[destination_hash] = attempts
        if attempts >= self.UNKNOWN_DEST_BOOTSTRAP_FAILURE_THRESHOLD:
            cooldown = min(
                self.UNKNOWN_DEST_BOOTSTRAP_BASE_COOLDOWN_S * (
                    self.UNKNOWN_DEST_BOOTSTRAP_BACKOFF_FACTOR
                    ** (attempts - self.UNKNOWN_DEST_BOOTSTRAP_FAILURE_THRESHOLD)
                ),
                self.UNKNOWN_DEST_BOOTSTRAP_MAX_COOLDOWN_S,
            )
            self._unknown_dest_backoff_until[destination_hash] = time.monotonic() + cooldown
            RNS.log(
                f"{self}: {attempts} DIRECT-bootstrap attempt(s) to destination "
                f"{destination_hash.hex()} with no token ever learned for it -- "
                f"backing off {cooldown:.0f}s rather than spending more airtime "
                f"on a destination that doesn't seem reachable through this "
                f"node's bound peer(s).",
                RNS.LOG_WARNING,
            )

    def _remember_bootstrap_send(self, data: bytes, header: Optional[_RnsHeader]) -> None:
        """Field fix (2026-09-19 afternoon, `fieldtests/raw/Alpha0.1.2`): the
        unknown-destination backoff counts bootstrap attempts "with no token
        ever learned" as failures, but a destination whose replies are
        PROOFs (a plain DATA delivery, LXMF without a Link) never teaches a
        token that way -- the laptop's three bootstrap sends to d4c70c4b
        were all delivered and proved, and the interface still backed off
        and dropped the next 17. Remember the packet's truncated hash (the
        exact value its PROOF will carry as destination-hash field) so
        `_observe_incoming_rns_packet`'s PROOF branch can learn the route
        and clear the backoff when that proof arrives DIRECT -- the same
        shape `_pending_link_requests` uses for LRPROOFs."""
        if header is None or header.destination_hash is None or header.packet_type != RNS.Packet.DATA:
            return
        truncated_hash = self._compute_truncated_hash(data, header.header_type)
        if truncated_hash is None:
            return
        self._pending_dest_proofs[truncated_hash] = (
            header.destination_hash, time.monotonic() + self.proof_correlation_ttl_s,
        )

    def _note_channel_proof(self, header: Optional[_RnsHeader], transport: str) -> None:
        """A delivery PROOF that arrived over CHANNEL for a bootstrap DATA
        send this node remembered (2026-09-20, MeshBench `two_hop` baseline:
        probes 2 and 3 were delivered over CHANNEL and PROVED, the interface
        still counted them as bootstrap attempts "with no token learned",
        and after the third it dropped probes 4-7 outright for 300 s --
        `unknown_dest_backoff_drop` -- while the destination was provably
        answering). The CHANNEL receive path deliberately learns no routing
        token (its sender is unauthenticated, see `_handle_channel_frame`),
        and that stays so: this only clears the destination's
        unknown-destination backoff, the one decision a matched proof is
        entitled to change. The worst a forged CHANNEL proof can do is keep
        this node trying a destination it would otherwise have given up on
        for a while -- the pre-backoff behaviour. Phase 1 (2026-09-20)
        added a second, equally bounded decision: the bare send the proof
        answers stops retrying (`_signal_send_answered`), with NO path
        evidence recorded for any peer -- a forged CHANNEL proof can at
        most cost one retry the application's own retry then covers."""
        if header is None or header.packet_type != RNS.Packet.PROOF or header.destination_hash is None:
            return
        self._signal_send_answered(header.destination_hash, f"PROOF over {transport}", None)
        delivered = self._pending_dest_proofs.pop(header.destination_hash, None)
        if delivered is None:
            # The same for an LRPROOF answering a LINKREQUEST this node sent
            # to an unresolved destination (link_id -> requested destination,
            # the shape `_observe_incoming_rns_packet` uses for DIRECT).
            delivered = self._pending_link_requests.pop(header.destination_hash, None)
        if delivered is None:
            return
        proved_dest, _expiry = delivered
        had_backoff = proved_dest in self._unknown_dest_attempts or proved_dest in self._unknown_dest_backoff_until
        self._clear_unknown_dest_backoff(proved_dest)
        self._debug(
            f"PROOF over {transport} for a bootstrap DATA send to {proved_dest.hex()} -- destination is "
            f"reachable; unknown-destination backoff {'cleared' if had_backoff else 'not armed'} "
            f"(no token learned: CHANNEL senders are unauthenticated)."
        )

    def _pending_dest_proofs_sweep(self, now: float) -> None:
        stale = [k for k, (_dest, expiry) in self._pending_dest_proofs.items() if now >= expiry]
        for k in stale:
            del self._pending_dest_proofs[k]

    def _clear_unknown_dest_backoff(self, destination_hash: bytes) -> None:
        self._unknown_dest_attempts.pop(destination_hash, None)
        self._unknown_dest_backoff_until.pop(destination_hash, None)
        self._unknown_dest_last_attempt.pop(destination_hash, None)

    def _unknown_dest_backoff_sweep(self, now: float) -> None:
        """Code-review fix: `_unknown_dest_attempts`/`_unknown_dest_
        backoff_until` had no periodic reclaim, unlike `_dedup`/
        `_reassembly`/`_proof_correlation` (all swept from
        `_reassembly_cleanup_loop`) -- a destination_hash tried a few times
        and then never addressed again (an ephemeral/one-off destination,
        never resolved and never crossing the backoff threshold either)
        sat in these dicts for the rest of the process's life; only a
        later success (`_clear_unknown_dest_backoff`) ever removed an
        entry. Mirrors `_proof_correlation_sweep`'s shape: idle-since-
        last-attempt, not a fixed TTL from creation, so an actively-
        retried destination is never pruned out from under its own
        backoff schedule."""
        stale = [
            h for h, last in self._unknown_dest_last_attempt.items()
            if now - last > self.UNKNOWN_DEST_BOOTSTRAP_MAX_COOLDOWN_S
        ]
        for h in stale:
            self._unknown_dest_attempts.pop(h, None)
            self._unknown_dest_backoff_until.pop(h, None)
            self._unknown_dest_last_attempt.pop(h, None)

    def _cache_announce(self, data: bytes, header: Optional[_RnsHeader], sender_peer_prefix: Optional[str]) -> None:
        """Remember an ANNOUNCE a bound peer delivered DIRECT (phase 1,
        2026-09-20), bytes exactly as received. CHANNEL announces are never
        cached (no authenticated source, and `_answer_path_request_locally`
        needs one to gate on)."""
        if (header is None or header.packet_type != RNS.Packet.ANNOUNCE or header.destination_hash is None
                or sender_peer_prefix is None or sender_peer_prefix not in self._peers
                or self.announce_cache_ttl_s <= 0):
            return
        self._announce_cache.pop(header.destination_hash, None)
        self._announce_cache[header.destination_hash] = (bytes(data), time.monotonic(), sender_peer_prefix)
        while len(self._announce_cache) > self.ANNOUNCE_CACHE_MAX_KEYS:
            self._announce_cache.popitem(last=False)

    def _announce_cache_sweep(self, now: float) -> None:
        stale = [k for k, (_raw, t, _src) in self._announce_cache.items() if now - t > self.announce_cache_ttl_s]
        for k in stale:
            del self._announce_cache[k]
        stale = [k for k, t in self._path_request_local_answer_at.items() if now - t > self.path_request_local_answer_min_interval_s]
        for k in stale:
            del self._path_request_local_answer_at[k]

    def _answer_path_request_locally(self, requested_hash: Optional[bytes]) -> Optional[str]:
        """If this node's own RNS is asking for a path this interface has
        already delivered an announce for, hand that announce back to RNS
        and report the source peer; None when the request must go on air.
        See `announce_cache_ttl`'s comment for the mechanism and the
        evidence. Runs on the event loop (the outgoing worker), where
        `owner.inbound` is called for every real reception too."""
        if (requested_hash is None or self.announce_cache_ttl_s <= 0
                or self.path_request_local_answer_min_interval_s <= 0):
            return None
        entry = self._announce_cache.get(requested_hash)
        if entry is None:
            return None
        raw, cached_at, source_peer = entry
        now = time.monotonic()
        if now - cached_at > self.announce_cache_ttl_s:
            self._announce_cache.pop(requested_hash, None)
            return None
        if source_peer not in self._peers or self._path_discovery_in_backoff(source_peer):
            return None
        last = self._path_request_local_answer_at.get(requested_hash)
        if last is not None and now - last < self.path_request_local_answer_min_interval_s:
            # The second re-request inside the interval is the one that
            # verifies the destination over the air.
            return None
        header = self._parse_rns_header(raw)
        if header is None:
            return None
        # Context -> PATH_RESPONSE: what this announce is, and on a
        # transport node the value that keeps RNS from inserting it into
        # the announce table for re-flooding (Transport.inbound's
        # `packet.context != PATH_RESPONSE` guard). The announce signature
        # covers destination, key, name hash, random hash, ratchet and app
        # data -- not the context byte.
        dst_len = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
        context_offset = (2 + 2 * dst_len) if header.header_type == 1 else (2 + dst_len)
        if len(raw) <= context_offset:
            return None
        answer = bytearray(raw)
        answer[context_offset] = RNS.Packet.PATH_RESPONSE
        self._path_request_local_answer_at[requested_hash] = now
        self.process_incoming(bytes(answer), transport="local_announce_cache", sender_peer_prefix=source_peer)
        return source_peer

    def _path_request_rate_limited(self, requested_hash: Optional[bytes]) -> bool:
        """PATH_REQUEST_RATE_LIMIT_WINDOW_S -- same shape and fail-open
        convention as `_path_response_rate_limited`, including recording
        the send time as a side effect on the not-limited path."""
        if requested_hash is None:
            return False
        now = time.monotonic()
        last_sent = self._path_request_last_sent_at.get(requested_hash)
        if last_sent is not None and now - last_sent < self.PATH_REQUEST_RATE_LIMIT_WINDOW_S:
            return True
        self._path_request_last_sent_at[requested_hash] = now
        return False

    def _path_response_rate_limit_sweep(self, now: float) -> None:
        """Code review (2026-09-18): `_path_response_last_sent_at` had no
        reclaim at all (a TODO at the top of this file said as much) --
        one entry per destination ever answered, for the life of the
        process. An entry older than PATH_RESPONSE_RATE_LIMIT_WINDOW_S can
        never suppress anything again, so it is dropped here."""
        stale = [
            h for h, last in self._path_response_last_sent_at.items()
            if now - last >= self.PATH_RESPONSE_RATE_LIMIT_WINDOW_S
        ]
        for h in stale:
            del self._path_response_last_sent_at[h]
        stale = [
            h for h, last in self._path_request_last_sent_at.items()
            if now - last >= self.PATH_REQUEST_RATE_LIMIT_WINDOW_S
        ]
        for h in stale:
            del self._path_request_last_sent_at[h]

    def _pending_link_request_sweep(self, now: float) -> None:
        """Code review (2026-09-18): a LINKREQUEST whose LRPROOF never
        arrived (or arrived via CHANNEL, where nothing can be learned from
        it) leaves its `_pending_link_requests` entry behind; reclaimed on
        the same `proof_correlation_ttl_s` clock as `_proof_correlation`,
        which it is the Link-shaped sibling of."""
        expired = [k for k, (_dest, expiry) in self._pending_link_requests.items() if now >= expiry]
        for k in expired:
            del self._pending_link_requests[k]

    # -- Answered sends: stop retrying once the reply is in (2026-09-20) --

    def _answered_send_key(self, data: bytes, header: Optional[_RnsHeader]) -> Optional[bytes]:
        """The key under which the reply to this bare DIRECT packet will be
        signalled, or None when no reply is expected / correlatable: a
        LINKREQUEST's link_id (its LRPROOF carries it as destination), or
        the truncated hash of a bootstrap DATA send remembered in
        `_pending_dest_proofs` (its PROOF carries that). Derived here, in
        the one place bare sends are dispatched (`_send_direct_payload`),
        rather than threaded from the dispatcher: a supplement copy of the
        same LINKREQUEST gets the same key and stops on the same proof."""
        if header is None:
            return None
        if header.packet_type == RNS.Packet.LINKREQUEST:
            return self._compute_link_id(data)
        if (header.packet_type == RNS.Packet.DATA and header.context == RNS.Packet.NONE
                and header.destination_type == RNS.Destination.SINGLE):
            # Review (2026-09-20): every plain DATA to a SINGLE destination,
            # not only a bootstrap send -- its PROOF's destination field is
            # this truncated hash whether the token was known or not (a
            # Link packet's proof carries the link_id instead, so those are
            # left out). In the 2026-09-20 laptop captures 2 of 47 and 4 of
            # 15 bare retries followed the packet's own PROOF.
            return self._compute_truncated_hash(data, header.header_type)
        return None

    def _answered_send_event(self, key: bytes) -> "asyncio.Event":
        """The event an in-flight send with this key waits on; already set
        if the reply was seen before the send got this far (a proof that
        beat the retry loop to the key). Timestamped so an event whose
        send was never answered is swept too (review, 2026-09-20)."""
        entry = self._send_answered_events.get(key)
        if entry is None:
            event = asyncio.Event()
            if key in self._send_answered_at:
                event.set()
            self._send_answered_events[key] = (event, time.monotonic())
            return event
        return entry[0]

    def _send_answered_by(self, key: bytes) -> Optional[str]:
        """Which bound peer delivered the reply (None: a CHANNEL copy, or
        an unbound sender) -- only a reply from the peer the send was
        addressed to is evidence about THAT peer's path."""
        entry = self._send_answered_at.get(key)
        return entry[1] if entry is not None else None

    def _signal_send_answered(self, key: Optional[bytes], how: str, sender_peer_prefix: Optional[str] = None) -> None:
        """Phase 1 (2026-09-20, `fieldtests/raw/Alpha0.1.3/capture_*144922`
        at 2 hops): a LINKREQUEST's attempt 0 lost its firmware ACK, its
        11 s ACK wait ended 0.5 s AFTER the LRPROOF had arrived, and attempt
        1 re-sent the request 8 s after the link was already proven -- a
        99-byte frame plus a 3.4 s ACK at 2 hops, and 3.8 s of lock time
        the LRRTT then waited behind. The three places that pop
        `_pending_link_requests` / `_pending_dest_proofs` call this, and
        the retry loop (`_send_direct_with_attempts`) and the ACK wait
        (`_await_direct_ack`) observe it."""
        if key is None:
            return
        self._send_answered_at[key] = (time.monotonic(), sender_peer_prefix)
        entry = self._send_answered_events.get(key)
        if entry is not None and not entry[0].is_set():
            entry[0].set()
            self._debug(f"send {key.hex()} answered ({how}) while its retry loop was live -- no further attempts.")

    def _send_answered_sweep(self, now: float) -> None:
        ttl = self.proof_correlation_ttl_s
        for k in [k for k, (t, _by) in self._send_answered_at.items() if now - t > ttl]:
            del self._send_answered_at[k]
        for k in [k for k, (_ev, t) in self._send_answered_events.items() if now - t > ttl]:
            del self._send_answered_events[k]

    def _path_response_rate_limited(self, destination_hash: Optional[bytes]) -> bool:
        """True if an outgoing PATH_RESPONSE for `destination_hash` was
        already sent within PATH_RESPONSE_RATE_LIMIT_WINDOW_S -- see that
        constant's own docstring for why. A `None` hash (shouldn't happen
        for a real PATH_RESPONSE, but nothing here assumes it can't) is
        never rate-limited, matching `_unknown_dest_in_backoff`'s same
        fail-open convention for a missing hash. Records the send time as
        a side effect only on the not-limited path -- the caller is
        expected to actually send in that case, so this doubles as the
        "record this attempt" step without a separate call."""
        if destination_hash is None:
            return False
        now = time.monotonic()
        last_sent = self._path_response_last_sent_at.get(destination_hash)
        if last_sent is not None and now - last_sent < self.PATH_RESPONSE_RATE_LIMIT_WINDOW_S:
            return True
        self._path_response_last_sent_at[destination_hash] = now
        return False

    def _announce_rate_limited(self, destination_hash: Optional[bytes]) -> bool:
        """One spontaneous ANNOUNCE per destination per
        `announce_min_interval_s` (see that config's comment). Same
        record-on-the-not-limited-path convention as
        `_path_response_rate_limited`."""
        if destination_hash is None or self.announce_min_interval_s <= 0:
            return False
        now = time.monotonic()
        last_sent = self._announce_last_sent_at.get(destination_hash)
        if last_sent is not None and now - last_sent < self.announce_min_interval_s:
            return True
        self._announce_last_sent_at[destination_hash] = now
        return False

    def _note_link_closed(self, header: Optional[_RnsHeader]) -> None:
        """A LINKCLOSE in either direction: every Link packet carries the
        link_id as its destination-hash field (RNS Link.py), so the id is
        right there. Called from RNS's thread (process_outgoing) and this
        interface's loop (process_incoming); a dict store is atomic."""
        if (
            header is not None
            and header.context == RNS.Packet.LINKCLOSE
            and header.destination_hash is not None
        ):
            self._closed_links[header.destination_hash] = time.monotonic()

    def _link_closed(self, header: Optional[_RnsHeader]) -> bool:
        """True for a Link-addressed packet whose Link was closed within
        CLOSED_LINK_TTL_S -- the drive-home capture spent ~10 minutes of
        attempts on a 3-fragment DATA for a Link that had been closed
        before the packet reached the front of the queue. The LINKCLOSE
        itself is never dropped. Checked at dequeue, before the first
        transmission -- the same point outgoing_max_age is decided --
        never between fragments."""
        if (
            header is None
            or header.destination_type != RNS.Destination.LINK
            or header.destination_hash is None
            or header.context == RNS.Packet.LINKCLOSE
        ):
            return False
        closed_at = self._closed_links.get(header.destination_hash)
        return closed_at is not None and time.monotonic() - closed_at < self.CLOSED_LINK_TTL_S

    def _closed_links_sweep(self, now: float) -> None:
        stale = [k for k, t in self._closed_links.items() if now - t > self.CLOSED_LINK_TTL_S]
        for k in stale:
            del self._closed_links[k]

    async def _send_outgoing_packet(
        self, data: bytes, header: Optional[_RnsHeader], expires_at: Optional[float] = None,
        spawned: Optional[list] = None,
    ) -> None:
        """Entry point the outgoing worker calls for every packet -- logs
        the classification debug line, then either delays an LRPROOF
        (see below) or dispatches immediately via
        `_dispatch_outgoing_packet`, which has the actual three-way
        routing logic."""
        # Code-review fix: CLAUDE.md's observability requirement names
        # "transport chosen, priority tier, spacing tier" as things that
        # "must be logged... build it in at M0/M1, don't bolt it on once
        # the real logic is working" -- spacing tier already is (every
        # CHANNEL/DIRECT multi-fragment send logs its own spacing), but
        # priority tier and the DIRECT-vs-broadcast routing decision
        # itself never were: there was no way, from logs alone, to tell
        # which classification/route a given outgoing packet actually
        # took. One debug line per packet, correlatable against the
        # transport-specific send lines that already follow it.
        self._debug(
            f"outgoing packet classification: "
            f"packet_type={self._PACKET_TYPE_NAMES.get(header.packet_type, header.packet_type) if header else 'unknown'} "
            f"destination_type={self._DESTINATION_TYPE_NAMES.get(header.destination_type, header.destination_type) if header else 'unknown'} "
            f"priority={self._priority_tier(header)} "
            f"destination_hash={header.destination_hash.hex() if header and header.destination_hash else None}."
        )

        # User-requested fix (2026-09-15, real NomadNet field testing):
        # RNS.Link's own keepalive/staleness timing (Link.py) is computed
        # exactly once, from the initial LINK_REQUEST<->LRPROOF handshake
        # round trip (`self.rtt = time.time() - self.request_time`), and
        # never recalibrated afterward: `keepalive = clamp(rtt * 205.7,
        # 5s, 360s)`, `stale_time = keepalive * 2`. That initial handshake
        # is small and uncontended, so it tends to complete fast (tens of
        # ms) -- giving RNS an optimistic RTT sample that locks in a short
        # keepalive/stale_time (observed: ~15-18s / ~30-36s) even though
        # this transport's real round trips, once real traffic queues up
        # behind the DIRECT serialization fix above, routinely exceed
        # that. The Link then looks "stale" to RNS and gets torn down and
        # silently re-established -- re-sending already-in-flight content
        # for no reason, with no user action. Confirmed directly against
        # RNS/Link.py: `Link.rtt`/`keepalive` have no config-file hook at
        # all, so this can't be fixed from ~/.reticulum/config, and
        # `Link.py` never reads this interface's own `bitrate`.
        #
        # Fix: deliberately delay sending an outgoing LRPROOF (the proof
        # that answers a LINK_REQUEST, and the exact packet whose
        # arrival time RNS measures for that one-time RTT sample) so the
        # measured RTT lands comfortably past `KEEPALIVE_MAX_RTT` (1.75s),
        # pinning every new Link's keepalive/stale_time at RNS's own
        # maximum (360s/720s) instead of an unrepresentatively short
        # value. This targets only LRPROOF specifically -- a single small
        # packet sent once per Link establishment, never per message --
        # so it adds a one-time delay to establishing a *new* Link, not
        # to any ongoing CHANNEL/DIRECT application traffic, and RNS's
        # own establishment_timeout (`6s * hops + 360s` -- Link.py,
        # ESTABLISHMENT_TIMEOUT_PER_HOP/KEEPALIVE) has enormous headroom
        # over this delay, so it can never itself cause a link-
        # establishment failure.
        if header is not None and header.context == RNS.Packet.LRPROOF:
            self._debug(
                f"routing decision: LRPROOF -- delaying "
                f"{self.LINK_PROOF_RTT_INFLATION_DELAY_S}s before send so "
                f"RNS's own Link keepalive/staleness timing (calibrated "
                f"from this exact round trip) reflects this transport's "
                f"real latency rather than an optimistic handshake sample."
            )
            self._capture_outgoing(header, data, "lrproof_delayed")
            task = self._spawn_background_task(self._send_delayed_link_proof(data, header, expires_at))
            if spawned is not None:
                spawned.append(task)
            return

        # User-requested fix (2026-09-15, real 2-hop repeater field
        # testing): see PATH_RESPONSE_RATE_LIMIT_WINDOW_S's own docstring.
        # A remote client stuck re-requesting the same path every few
        # seconds shouldn't get this node re-keying the radio to answer
        # every single one -- drop the repeat, not the first answer.
        if header is not None and header.context == RNS.Packet.PATH_RESPONSE:
            if self._path_response_rate_limited(header.destination_hash):
                self._outgoing_dropped_total += 1
                self._capture_outgoing(header, data, "path_response_rate_limited")
                self._debug(
                    f"routing decision: PATH_RESPONSE for destination "
                    f"{header.destination_hash.hex() if header.destination_hash else None} "
                    f"-- dropped, already answered within "
                    f"{self.PATH_RESPONSE_RATE_LIMIT_WINDOW_S}s."
                )
                return

        # Field fix (2026-09-19, drive-home 3-hop capture): one spontaneous
        # ANNOUNCE per destination per announce_min_interval -- see that
        # config's own comment.
        if (
            header is not None
            and header.packet_type == RNS.Packet.ANNOUNCE
            and header.context != RNS.Packet.PATH_RESPONSE
            and self._announce_rate_limited(header.destination_hash)
        ):
            self._outgoing_dropped_total += 1
            self._capture_outgoing(header, data, "announce_rate_limited")
            self._debug(
                f"routing decision: ANNOUNCE for destination "
                f"{header.destination_hash.hex() if header.destination_hash else None} "
                f"-- dropped, one was already forwarded within "
                f"{self.announce_min_interval_s:.0f}s (announce_min_interval)."
            )
            return

        # Field fix (2026-09-18 evening): coalesce repeated path requests
        # for the same requested destination -- see
        # PATH_REQUEST_RATE_LIMIT_WINDOW_S's own comment.
        if (
            header is not None
            and header.packet_type == RNS.Packet.DATA
            and header.destination_type == RNS.Destination.PLAIN
        ):
            requested = self._path_request_target(data, header)
            if self._path_request_rate_limited(requested):
                self._outgoing_dropped_total += 1
                self._capture_outgoing(header, data, "path_request_rate_limited")
                self._debug(
                    f"routing decision: path request for "
                    f"{requested.hex() if requested else None} -- dropped, one was already "
                    f"sent within {self.PATH_REQUEST_RATE_LIMIT_WINDOW_S:.0f}s."
                )
                return
            answered_from = self._answer_path_request_locally(requested)
            if answered_from is not None:
                # Phase 1 (2026-09-20): answered from the cached announce,
                # nothing transmitted -- see announce_cache_ttl.
                self._capture_outgoing(header, data, "path_request_answered_locally", target_peer=answered_from)
                self._debug(
                    f"routing decision: path request for {requested.hex()} -- answered locally from the "
                    f"announce {answered_from!r} delivered earlier; not transmitted."
                )
                return

        # Code review (2026-09-18): remember which destination this
        # LINKREQUEST is for, keyed by the link_id its LRPROOF will carry --
        # see _observe_incoming_rns_packet's PROOF branch for the other
        # half (learning the destination's token and clearing its
        # unknown-destination backoff when that proof comes back DIRECT).
        if (
            header is not None
            and header.packet_type == RNS.Packet.LINKREQUEST
            and header.destination_hash is not None
        ):
            link_id = self._compute_link_id(data)
            if link_id is not None:
                self._pending_link_requests[link_id] = (
                    header.destination_hash, time.monotonic() + self.proof_correlation_ttl_s,
                )

        await self._dispatch_outgoing_packet(data, header, expires_at=expires_at, spawned=spawned)

    async def _send_delayed_link_proof(
        self, data: bytes, header: _RnsHeader, expires_at: Optional[float] = None,
    ) -> None:
        await asyncio.sleep(self.LINK_PROOF_RTT_INFLATION_DELAY_S)
        if self.detached or not self.online:
            return
        inner: list = []
        await self._dispatch_outgoing_packet(data, header, expires_at=expires_at, spawned=inner)
        # Finish everything the dispatch spawned before this task ends, so
        # the packet's in-flight entry (released when THIS task finishes)
        # really covers the whole send.
        live = [t for t in inner if t is not None]
        if live:
            await asyncio.gather(*live, return_exceptions=True)

    async def _dispatch_outgoing_packet(
        self, data: bytes, header: Optional[_RnsHeader], expires_at: Optional[float] = None,
        spawned: Optional[list] = None,
    ) -> None:
        """docs/routing_decisions.md's routing dispatcher -- the three
        outgoing situations, in the doc's own order:

        1. ANNOUNCE: broadcast only, UNLESS small-mesh mode applies (see
           `_in_small_mesh_mode`), in which case it goes DIRECT to every
           bound peer instead -- DIRECT fragmentation handles an
           announce's size the same way CHANNEL's own multi-fragment
           shape would. There's also no way for this interface to
           actually detect "this outgoing ANNOUNCE is answering peer X's
           path request" (RNS core doesn't expose that context to the
           interface layer), so outside small-mesh mode this remains a
           clean, total deferral, not a partial one.
        2. DATA+PLAIN (path request): broadcast plus a DIRECT supplement
           to a capped number of known router-capability peers with an
           already-resolved path, UNLESS small-mesh mode applies, in
           which case it goes DIRECT to every bound peer instead (no
           router-capability filter -- see _send_path_request).
        3. Everything else (DATA/SINGLE, LINK_REQUEST, PROOF): DIRECT,
           unconditionally, if this interface's own record shows a
           resolved path to the packet's peer (unaffected by small-mesh
           mode -- already DIRECT-only). Otherwise: broadcast fallback
           plus a capped DIRECT bootstrap-supplement, UNLESS small-mesh
           mode applies, in which case it goes DIRECT to every bound peer
           instead of broadcasting at all. "Resolved path known" is
           always this interface's own `_resolved_paths` record, never a
           fresh device-table read (routing_decisions.md's explicit
           instruction, carried over from path_discovery_spec.md's own
           persistence-failure note).
        """
        if header is not None and header.packet_type == RNS.Packet.DATA and header.destination_type == RNS.Destination.PLAIN:
            await self._send_path_request(data, header, expires_at=expires_at, spawned=spawned)
            return

        if header is not None and header.packet_type != RNS.Packet.ANNOUNCE:
            peer_prefix = self._resolve_routing_peer(header)
            if peer_prefix is not None:
                self._debug(f"routing decision: known peer {peer_prefix!r} -> DIRECT-primary.")
                self._capture_outgoing(header, data, "direct_primary", target_peer=peer_prefix)
                # A known peer routes to _send_direct_packet regardless of
                # whether a path is *currently* resolved -- Milestone 6
                # folds discover_path()-before-broadcast-fallback into
                # that method itself (§8), so this dispatcher no longer
                # gates DIRECT eligibility on _resolved_paths membership
                # the way Milestone 5 did; a peer with no (or a freshly
                # stale-path-reset) resolved path still belongs on the
                # DIRECT path, not an immediate broadcast fallback.
                # DIRECT is the primary transport here, not a supplement --
                # its own ACK wait can take several seconds, so it's spawned
                # rather than awaited, exactly like path discovery and the
                # CHANNEL extra-retry passes above: one slow operation must
                # not stall the worker from draining the next queued packet.
                task = self._spawn_background_task(
                    self._send_direct_packet(data, header, peer_prefix, expires_at=expires_at)
                )
                if spawned is not None:
                    spawned.append(task)
                return

            # Milestone 6 fix for a real field-diagnosed gap
            # (peer_discovery_design.md §7): opportunistic RNS-token
            # learning only ever learns from an incoming DIRECT receive
            # (CHANNEL carries no sender identity at all, so there's
            # nothing safer to learn from) -- meaning two bound peers
            # that have never yet exchanged a single DIRECT message have
            # no way to ever originate one, since neither side has a
            # token yet. Fix: broadcast (below, mandatory, unchanged)
            # plus a DIRECT-bootstrap-supplement to a capped number of
            # bound peers. Safe: it adds no new exposure beyond what the
            # broadcast already does (RNS's own encryption protects
            # content regardless of transport; a DIRECT copy reaching a
            # bound peer who isn't actually the intended recipient is
            # exactly as cryptographically inert to them as the CHANNEL
            # copy they were already going to receive). Self-limiting:
            # one successful delivery teaches the recipient a real token
            # immediately (§7's normal mechanism, via
            # _observe_incoming_rns_packet), after which ordinary
            # DIRECT-primary routing takes over for that destination and
            # this supplement never fires for it again.
            # User-requested fix (2026-09-15, field-diagnosed via packet
            # capture): every branch below that actually attempts a
            # DIRECT-bootstrap send for this destination_hash first
            # checks/records against _unknown_dest_in_backoff -- see that
            # method's own docstring and the class constants above for
            # why (an unreachable-through-this-peer destination retried
            # on its own schedule forever, with no memory of past
            # attempts, confirmed costing real repeated airtime for an
            # LXMF propagation node this node's only MeshCore peer simply
            # has no path to).
            backed_off = self._unknown_dest_in_backoff(header.destination_hash)
            if self._in_small_mesh_mode():
                if backed_off:
                    self._debug(
                        f"routing decision: no known peer for this destination -- "
                        f"backing off further DIRECT attempts (repeated tries, no "
                        f"token ever learned for it); dropping rather than "
                        f"spending more airtime."
                    )
                    self._capture_outgoing(header, data, "unknown_dest_backoff_drop")
                    self._outgoing_dropped_total += 1
                    return
                self._debug(
                    f"routing decision: no known peer for this destination, "
                    f"small mesh ({len(self._peers)} bound peer(s)) -- DIRECT "
                    f"to every bound peer instead of CHANNEL."
                )
                self._capture_outgoing(
                    header, data, "small_mesh_direct_all_unknown_dest",
                    candidate_peers=self._all_bound_peer_prefixes(),
                )
                if not self._proof_like(header):
                    # Field fix (2026-09-19 evening session): a PROOF must not
                    # arm this backoff. Its destination field is the truncated
                    # hash of the proved packet (or a link_id) -- a value seen
                    # once and never addressed again -- so counting "attempts
                    # with no token ever learned" against it is meaningless,
                    # and in the midday capture it was harmful: three such
                    # proofs armed a 300s cooldown that then DROPPED later
                    # proofs outright in small-mesh mode. Six proofs per
                    # session still route here because the CHANNEL receive
                    # path cannot authenticate a sender and so deliberately
                    # learns no token from it (the raw path's guard cannot be
                    # reused: CHANNEL carries only an attacker-choosable
                    # adv_name). Excluding proofs removes the harm without
                    # inventing trust.
                    self._record_unknown_dest_attempt(header.destination_hash)
                self._remember_bootstrap_send(data, header)
                await self._send_direct_to_all_peers(
                    data, header, priority=self._priority_tier(header), expires_at=expires_at, spawned=spawned,
                )
                return
            bootstrap_targets = [] if backed_off else self._select_bootstrap_supplement_targets()
            if bootstrap_targets:
                # Registered BEFORE the supplement tasks are spawned (review,
                # 2026-09-20): the proof correlation and the answered-send
                # key are read by the send path, so their order must not
                # rest on the tasks not running until the dispatcher yields.
                self._remember_bootstrap_send(data, header)
            for bootstrap_peer_prefix in bootstrap_targets:
                task = self._spawn_background_task(
                    self._send_direct_supplement(
                        data, bootstrap_peer_prefix, trigger_discovery=True,
                        priority=self._priority_tier(header), expires_at=expires_at,
                    )
                )
                if spawned is not None:
                    spawned.append(task)
            if bootstrap_targets:
                if not self._proof_like(header):
                    # Field fix (2026-09-19 evening session): a PROOF must not
                    # arm this backoff. Its destination field is the truncated
                    # hash of the proved packet (or a link_id) -- a value seen
                    # once and never addressed again -- so counting "attempts
                    # with no token ever learned" against it is meaningless,
                    # and in the midday capture it was harmful: three such
                    # proofs armed a 300s cooldown that then DROPPED later
                    # proofs outright in small-mesh mode. Six proofs per
                    # session still route here because the CHANNEL receive
                    # path cannot authenticate a sender and so deliberately
                    # learns no token from it (the raw path's guard cannot be
                    # reused: CHANNEL carries only an attacker-choosable
                    # adv_name). Excluding proofs removes the harm without
                    # inventing trust.
                    self._record_unknown_dest_attempt(header.destination_hash)
            self._debug(
                f"routing decision: no known peer for this destination -- "
                f"CHANNEL broadcast"
                + (f" + bootstrap DIRECT supplement to {bootstrap_targets}" if bootstrap_targets else "")
                + (" (DIRECT supplement backed off -- repeated no-reply attempts)" if backed_off else "")
                + "."
            )
            self._capture_outgoing(
                header, data,
                "unknown_dest_backoff_broadcast_only" if backed_off else "broadcast_bootstrap_supplement",
                candidate_peers=bootstrap_targets,
            )
        elif header is not None and header.packet_type == RNS.Packet.ANNOUNCE:
            if self._in_small_mesh_mode():
                self._debug(
                    f"routing decision: ANNOUNCE, small mesh "
                    f"({len(self._peers)} bound peer(s)) -- DIRECT to every "
                    f"bound peer instead of CHANNEL."
                )
                self._capture_outgoing(
                    header, data, "small_mesh_direct_all_announce",
                    candidate_peers=self._all_bound_peer_prefixes(),
                )
                await self._send_direct_to_all_peers(
                    data, header, priority=self._priority_tier(header), expires_at=expires_at, spawned=spawned,
                )
                return
            self._debug("routing decision: ANNOUNCE -> CHANNEL broadcast only (never DIRECT, by design).")
            self._capture_outgoing(header, data, "broadcast_announce_only")
        else:
            self._capture_outgoing(header, data, "broadcast_fallback")

        await self._send_broadcast_packet(data, header, expires_at=expires_at)

    def _select_bootstrap_supplement_targets(self) -> list:
        """Milestone 6's DIRECT-bootstrap-supplement target selection:
        ANY bound peer, not filtered by router capability the way
        `_select_direct_supplement_targets` (path requests) is -- the
        goal here isn't "find a router to forward through," it's "this
        specific bound peer might be the actual RNS-level counterpart
        for a destination this interface has no token for yet," so
        capability is irrelevant. Most-recently-seen first, capped
        (`bootstrap_direct_supplement_cap`) so this doesn't fan out to
        every bound peer as the peer count grows.

        Airtime-efficiency fix (2026-09-17): primarily ordered by this
        peer's own `_direct_path_failures` count (fewest first), most-
        recently-seen only as the tiebreaker among equally-healthy peers
        -- previously recency alone decided this, so a peer with a
        currently elevated failure count (already a full attempt budget
        away from a stale-path reset, but not yet at
        `direct_path_reset_threshold`) could still occupy a scarce capped
        slot ahead of a peer this interface has no reason to doubt,
        spending part of the bootstrap-supplement's own limited fan-out on
        a send unlikely to succeed. A peer with no recorded failures at
        all sorts as failure count `0`, same as one that's never been
        tried -- this is a deprioritization signal, not a hard exclusion,
        so a struggling link still gets a chance once it's the least-bad
        option available."""
        peers = sorted(
            self._peers.values(),
            key=lambda p: (self._direct_path_failures.get(p.pubkey_prefix, 0), -p.last_seen),
        )
        return [p.pubkey_prefix for p in peers[: self.bootstrap_direct_supplement_cap]]

    async def _send_broadcast_packet(
        self, data: bytes, header: Optional[_RnsHeader], expires_at: Optional[float] = None,
        spawned: Optional[list] = None,
    ) -> None:
        """The CHANNEL broadcast send, exactly as Milestones 1-3 built it
        -- extracted unchanged out of Milestone 3's own
        `_send_outgoing_packet` body so Milestone 5's dispatcher above can
        call it both as the ordinary case-1/no-resolved-path fallback
        (awaited directly, preserving the original serialization) and as
        one half of the path-request broadcast+DIRECT-supplement pattern
        (spawned as an independent background task there instead --
        see _send_path_request)."""
        retry_extra = self._retry_extra_for(header)
        pkt_id = self._next_pkt_id()
        duty_cycle_exempt = self._duty_cycle_exempt(self._priority_tier(header))

        await self._send_channel_pass(data, pkt_id, attempt=0, duty_cycle_exempt=duty_cycle_exempt)

        # §1-§2: each extra pass is scheduled unconditionally at send
        # time (CHANNEL has no ACK to react to) as an independent
        # background task, so a multi-second-to-tens-of-seconds jittered
        # wait for pkt_id's own retry never blocks the worker from moving
        # on to the next queued packet.
        for attempt in range(1, retry_extra + 1):
            task = self._spawn_background_task(
                self._delayed_retry_pass(data, pkt_id, attempt, expires_at, duty_cycle_exempt)
            )
            # Audit fix (2026-09-19): these were untracked, so
            # `_release_inflight_when_done` saw no live task for a broadcast
            # and freed the duplicate-suppression key as soon as pass 0
            # returned -- while the jittered retry passes for the same bytes
            # were still pending. RNS re-queueing an identical copy in that
            # window (its Resource layer does, ~27s apart) was then accepted
            # as "not in flight" and broadcast on top of them: the duplicate
            # storm the in-flight guard was added to stop.
            if spawned is not None:
                spawned.append(task)

    async def _send_path_request(
        self, data: bytes, header: _RnsHeader, expires_at: Optional[float] = None,
        spawned: Optional[list] = None,
    ) -> None:
        """docs/routing_decisions.md's path-request case: always
        broadcast, plus a DIRECT copy to a capped number of known
        router-capability peers that already have a resolved path --
        cheap (path requests are small, comfortably one fragment) and
        meaningfully more reliable than the broadcast alone. Never
        triggers path discovery just to enable this supplement: only
        already-resolved peers qualify.

        **Resolved: fired concurrently, neither gated on the other's
        outcome** (the doc's own explicit resolution) -- both the
        broadcast and every DIRECT supplement are spawned as independent
        background tasks rather than one awaiting the other, so this
        coroutine (and so the outgoing worker) returns immediately. They
        still funnel through the same single radio at the command layer
        (`_command_lock`), and `_send_direct_supplement` still waits out
        this design's own minimum inter-message gap before actually
        transmitting, so they're never issued back-to-back into the same
        half-duplex-deaf window either.

        In small-mesh mode (`_in_small_mesh_mode`), this whole broadcast-
        plus-supplement shape is replaced: DIRECT to every bound peer,
        not just router-capability ones with an already-resolved path,
        and no CHANNEL broadcast at all. The router-capability filter
        exists to keep a *supplement* small as peer count grows -- with
        only one or two peers total there's no growth to bound, and a
        peer this node hasn't confirmed as a router yet still deserves a
        chance to answer (CHANNEL would have reached it too); this also
        does trigger discovery for an unresolved peer, since there is no
        broadcast left to fall back on if it doesn't."""
        if self._in_small_mesh_mode():
            self._debug(
                f"routing decision: path request, small mesh "
                f"({len(self._peers)} bound peer(s)) -- DIRECT to every "
                f"bound peer instead of CHANNEL."
            )
            self._capture_outgoing(
                header, data, "small_mesh_direct_all_path_request",
                candidate_peers=self._all_bound_peer_prefixes(),
            )
            await self._send_direct_to_all_peers(
                data, header, priority=self._priority_tier(header), expires_at=expires_at, spawned=spawned,
            )
            return
        self._debug("routing decision: path request -> CHANNEL broadcast + router-peer DIRECT supplement.")
        supplement_targets = self._select_direct_supplement_targets()
        self._capture_outgoing(
            header, data, "broadcast_path_request_supplement", candidate_peers=supplement_targets,
        )
        tasks = [self._spawn_background_task(
            self._send_broadcast_packet(data, header, expires_at=expires_at, spawned=spawned)
        )]
        for peer_prefix in supplement_targets:
            tasks.append(self._spawn_background_task(
                self._send_direct_supplement(
                    data, peer_prefix, priority=self._priority_tier(header), expires_at=expires_at,
                )
            ))
        if spawned is not None:
            spawned.extend(tasks)

    def _select_direct_supplement_targets(self) -> list:
        """docs/routing_decisions.md's path-request DIRECT-supplement
        target selection: known router-capability peers (this project's
        own bind-frame protocol bit -- peer_discovery_design.md §2's hard
        rule against the old design's shipped `can_route` mislabeling bug:
        never anything read off the MeshCore contact table, which has no
        such field at all) that already have a resolved path, capped and
        most-recently-confirmed first -- not unconditionally every known
        router as the router count grows.

        Airtime-efficiency fix (2026-09-17): primarily ordered by this
        peer's own `_direct_path_failures` count (fewest first), most-
        recently-confirmed only as the tiebreaker among equally-healthy
        peers -- see `_select_bootstrap_supplement_targets`'s own note on
        this same fix for the full reasoning (a currently-failing peer,
        below `direct_path_reset_threshold` so still technically
        "resolved," could otherwise still win a capped supplement slot on
        recency alone). Not a hard exclusion: `_direct_path_failures`
        naturally clears the moment this peer's path is confirmed working
        again, or the peer drops out of `candidates` entirely once a
        stale-path reset removes it from `_resolved_paths`."""
        candidates = [
            peer for peer in self._peers.values()
            if peer.has_upstream_rns and peer.pubkey_prefix in self._resolved_paths
        ]
        candidates.sort(
            key=lambda p: (
                self._direct_path_failures.get(p.pubkey_prefix, 0),
                -self._resolved_paths[p.pubkey_prefix].resolved_at,
            )
        )
        return [p.pubkey_prefix for p in candidates[: self.path_request_direct_supplement_cap]]

    async def _send_direct_supplement(
        self, data: bytes, peer_prefix: str, trigger_discovery: bool = False,
        priority: int = PRIORITY_NORMAL, expires_at: Optional[float] = None,
        alongside_broadcast: bool = True,
    ) -> bool:
        """A DIRECT copy of `data` to one bound peer, fired alongside a
        mandatory broadcast this method never gates or is gated by (see
        `_send_path_request`'s and the bootstrap-supplement dispatcher's
        own docstrings). `trigger_discovery` distinguishes the two
        callers' different preconditions: the path-request supplement
        (`_select_direct_supplement_targets`) never triggers discovery
        just to enable itself (routing_decisions.md's explicit
        instruction -- the broadcast already covers this peer regardless);
        the DIRECT-bootstrap-supplement (Milestone 6,
        `_select_bootstrap_supplement_targets`) deliberately does, since
        triggering discovery for a not-yet-token-bootstrapped peer is the
        whole point of that mechanism.

        Returns True if the packet actually reached the radio, False if this
        method bailed before transmitting (audit fix, 2026-09-19 -- see
        `_send_direct_to_all_peers`, which is the one caller that has no
        broadcast running alongside to cover for a False and therefore needs
        to know)."""
        resolved = self._resolved_paths.get(peer_prefix)
        if resolved is None:
            if not trigger_discovery:
                return False
            resolved = await self._discover_path_coalesced(peer_prefix)
            if resolved is None:
                # Audit fix (2026-09-19): logged and counted like every
                # sibling drop path in this method (CLAUDE.md's "every drop
                # decision must be logged"). This was the last silent one.
                self._outgoing_dropped_total += 1
                self._debug(
                    f"DIRECT supplement to {peer_prefix!r} not sent -- no resolved path and "
                    f"discovery did not resolve one (peer may be inside a path-discovery backoff)."
                )
                return False

        # This design's own minimum inter-message gap (reliability_engine_
        # design.md §2), scaled by this specific peer's own known hop
        # depth when available -- routing_decisions.md's fix for the
        # half-duplex-deaf-repeater collision risk this supplement would
        # otherwise recreate against the broadcast's own fragment(s) if
        # fired with zero spacing.
        # MeshBench finding 4 (2026-09-20): the spacing exists to clear the
        # broadcast this copy rides alongside. Small-mesh DIRECT-to-all sends
        # no broadcast (see _send_direct_to_all_peers), yet paid 5-10s x hops
        # here -- most of one two-hop probe's 51s round trip -- so that caller
        # passes alongside_broadcast=False and skips it.
        spacing_s = self._supplement_spacing_s(resolved.out_path_len, alongside_broadcast)
        if spacing_s > 0:
            await asyncio.sleep(spacing_s)
        if self.detached or not self.online:
            return False
        if self._expired(expires_at):
            self._outgoing_dropped_total += 1
            self._debug(f"DIRECT supplement to {peer_prefix!r} skipped -- packet expired (outgoing_max_age).")
            return False

        contact = self._resolve_contact(peer_prefix)
        target = contact.get("public_key") if contact is not None else None
        if not target:
            # Code-review fix: this used to return with no log line and no
            # _outgoing_dropped_total increment, unlike every sibling drop
            # path in this method and in _send_direct_packet -- violating
            # CLAUDE.md's "every drop decision must be logged" rule for
            # this specific case (a bound peer whose contact record can't
            # be resolved, or has no public_key, at the moment a
            # supplement fires).
            self._outgoing_dropped_total += 1
            RNS.log(
                f"{self}: DIRECT supplement to {peer_prefix!r} dropped -- "
                f"no resolvable contact/public_key for this bound peer.",
                RNS.LOG_WARNING,
            )
            return False
        # Bare in the common case (a path request always fits DIRECT's
        # bare budget), but the bootstrap-supplement caller can carry an
        # arbitrary-size DATA/LINK_REQUEST/PROOF packet, so this goes
        # through the same bare-or-fragmented dispatch _send_direct_packet
        # uses rather than assuming bare unconditionally.
        ok = await self._send_direct_payload(
            target, peer_prefix, data, priority=priority, hop_count=resolved.out_path_len,
            expires_at=expires_at,
        )
        if ok is None:
            # Code-review fix: _send_direct_packet's own handling of this
            # same None-means-too-big-to-attempt contract logs and counts
            # it; this call site used to just drop the return value,
            # silently swallowing a supplement that never even tried to
            # go out with no log line and no _outgoing_dropped_total
            # increment anywhere -- violating CLAUDE.md's "every drop
            # decision (with its reason) must be logged" requirement for
            # this specific transport path. No CHANNEL fallback needed
            # here (this is only ever a supplement; the mandatory
            # broadcast this rides alongside already covers delivery).
            self._outgoing_dropped_total += 1
            RNS.log(
                f"{self}: DIRECT supplement to {peer_prefix!r} for a "
                f"{len(data)}-byte packet never attempted -- exceeds even "
                f"the fully-fragmented DIRECT budget.",
                RNS.LOG_WARNING,
            )
            return False
        return True

    def _supplement_spacing_s(self, hop_count: Optional[int], alongside_broadcast: bool) -> float:
        """The pre-send spacing for one DIRECT supplement: the design's
        hop-scaled inter-message gap when a broadcast of the same packet is
        on air alongside it, nothing otherwise (MeshBench finding 4,
        2026-09-20)."""
        if not alongside_broadcast:
            return 0.0
        spacing_min, spacing_max = self._fragment_spacing_range(hop_count=hop_count)
        return random.uniform(spacing_min, spacing_max)

    async def _send_direct_packet(
        self, data: bytes, header: Optional[_RnsHeader], peer_prefix: str,
        expires_at: Optional[float] = None,
    ) -> None:
        """Case 3 of docs/routing_decisions.md's summary table: DIRECT is
        the primary transport here, not a supplement -- no CHANNEL
        fallback on an ACK failure, only on the local resolution steps
        below failing outright (no resolved path even after trying
        discovery, or no contact/pubkey to send to at all, which this
        interface's own record being out of sync with the device contact
        table can cause -- see path_discovery_spec.md's persistence
        note)."""
        resolved = self._resolved_paths.get(peer_prefix)
        if resolved is None:
            # Milestone 6: docs/reliability_engine_design.md §8's "next
            # send attempt for this peer goes through discover_path()
            # first" recovery policy -- covers both a freshly
            # stale-path-reset peer and one that simply never had a path
            # resolved yet. Only falls through to broadcast if discovery
            # itself also fails.
            resolved = await self._discover_path_coalesced(peer_prefix)
        if resolved is None:
            # Audit fix (2026-09-19): expires_at was not threaded into these
            # three CHANNEL fallbacks, so a packet already past
            # outgoing_max_age got its full jittered retry-pass budget with
            # expiry checking disabled -- the stale-burst behaviour
            # outgoing_max_age exists to stop.
            #
            # Field fix (2026-09-19 evening session): and the fallback is now
            # RECORDED. It used to return before any capture call, so the
            # packet's only record was the dispatcher's earlier
            # `direct_primary` -- the capture said DIRECT while the packet
            # actually went out as an unencrypted CHANNEL flood. Seven packets
            # in that session were mislabelled this way (found only because
            # the peer logged them arriving as `channel_bare`), which silently
            # undermines any analysis that trusts `routing_decision`.
            self._capture_outgoing(header, data, "direct_unresolved_channel_fallback")
            await self._send_broadcast_packet(data, header, expires_at=expires_at)
            return

        contact = self._resolve_contact(peer_prefix)
        target = contact.get("public_key") if contact is not None else None
        if not target:
            self._capture_outgoing(header, data, "direct_no_contact_channel_fallback")
            await self._send_broadcast_packet(data, header, expires_at=expires_at)
            return

        send_info: dict = {}
        ok = await self._send_direct_payload(
            target, peer_prefix, data, priority=self._priority_tier(header), hop_count=resolved.out_path_len,
            expires_at=expires_at, send_info=send_info,
        )
        if ok is None:
            # Too big even for the fully-fragmented DIRECT budget --
            # _send_direct_payload already declined to attempt anything.
            self._outgoing_dropped_total += 1
            per_fragment_budget = self._direct_multifragment_payload_budget()
            RNS.log(
                f"{self}: dropping outgoing DIRECT packet to {peer_prefix!r} "
                f"({len(data)} bytes) -- exceeds even the fully-fragmented "
                f"DIRECT budget ({per_fragment_budget * 255} bytes across 255 "
                f"fragments at {per_fragment_budget} bytes each); falling "
                f"back to CHANNEL broadcast instead.",
                RNS.LOG_WARNING,
            )
            self._capture_outgoing(header, data, "direct_too_large_channel_fallback")
            await self._send_broadcast_packet(data, header, expires_at=expires_at)
            return

        self._capture_direct_send_result(
            peer_prefix, header.destination_hash if header is not None else None, ok, resolved, len(data),
            method=send_info.get("method"), fallback_from_raw=bool(send_info.get("fallback_from_raw")),
            slot_wait_s=send_info.get("slot_wait_s"),
        )
        if not ok and self._expired(expires_at):
            # Already logged and counted where the expiry was detected.
            return
        if not ok:
            RNS.log(
                f"{self}: DIRECT send to {peer_prefix!r} did not receive a "
                f"real delivery ACK (out_path_len={resolved.out_path_len}) -- "
                f"no CHANNEL fallback for this attempt (routing_decisions.md: "
                f"DIRECT is the primary transport once a path is resolved, "
                f"not a supplement); repeated failures will trigger stale-"
                f"path detection/reset (§8).",
                RNS.LOG_WARNING,
            )

    async def _send_direct_payload(
        self, target: str, peer_prefix: str, data: bytes, priority: int = PRIORITY_NORMAL,
        hop_count: Optional[int] = None, expires_at: Optional[float] = None,
        send_info: Optional[dict] = None,
    ) -> Optional[bool]:
        """Sends `data` DIRECT to `target`, choosing the bare or DIRECT-
        needs-fragmenting shape automatically based on size -- shared by
        the DIRECT-primary path (`_send_direct_packet`) and every DIRECT-
        supplement path (`_send_direct_supplement`) alike, so a
        supplement carrying an oversized packet fragments exactly the
        same way a primary send would, rather than assuming bare
        unconditionally. Returns `None` if `data` is too large even for
        the fully-fragmented DIRECT budget (nothing was attempted),
        otherwise whether every fragment (or the single bare message)
        was actually ACKed."""
        # `send_info` (2026-09-19, capture only): filled with "method" and
        # "fallback_from_raw" so direct_send_result can say how the packet
        # was carried.
        if send_info is None:
            send_info = {}
        fastpath_budget = self._direct_payload_budget()
        if len(data) <= fastpath_budget:
            send_info["method"] = "z85_bare"
            header = self._parse_rns_header(data)
            return await self._send_direct_with_attempts(
                target, lambda attempt, d=data: self._encode_direct_bare(d), peer_prefix,
                priority=priority, hop_count=hop_count, expires_at=expires_at,
                cancel_key=self._answered_send_key(data, header),
                # A plain PROOF expires before every attempt (proof_max_age).
                expire_retries=self._plain_proof(header) and self.proof_max_age_s > 0,
                # A Link handshake pre-empts idle holds of the radio lock.
                preempt=self._is_link_handshake(header),
            )

        # Milestone 6: DIRECT-needs-fragmenting shape
        # (wire_format_design.md) -- rare in practice (constraint one:
        # everything but ANNOUNCE, which never goes DIRECT here,
        # comfortably fits one DIRECT message), but a real packet can
        # still exceed it (e.g. a large Resource-transfer DATA packet).
        per_fragment_budget = self._direct_multifragment_payload_budget()
        max_total_payload = per_fragment_budget * 255  # frag_total is a 1-byte field
        if per_fragment_budget <= 0 or len(data) > max_total_payload:
            return None
        # Alpha 0.1.1 (2026-09-18 night): resume a recently failed send of
        # these exact bytes to this peer under its old pkt_id, so the
        # fragments the receiver still holds aren't sent again. Only when
        # the reconcile step will run afterwards to validate the assumption
        # (never for handshake-priority sends, which don't reconcile).
        # Second audit (2026-09-19 evening session): at most
        # direct_fragmented_max_in_flight fragmented sends per peer at once
        # -- see that option's comment for the three measured costs of the
        # unbounded fan-out. The slot is held for the WHOLE send (bursts and
        # reconcile windows alike), so packets to one peer complete roughly
        # in the order RNS handed them over.
        #
        # Field fix (2026-09-19 night, `fieldtests/raw/Alpha0.1.2/*nighttest*`):
        # OFF by default now (see the option's comment for why the cap did
        # not achieve its purpose), and when enabled the slot is priority-
        # aware (`_PriorityAsyncSemaphore`), announce-class sends have a slot
        # of their own, and a send that cannot get a slot within its budget
        # PROCEEDS with a warning instead of being dropped. The night session
        # dropped four 483-byte Resource parts and two of the laptop's data
        # packets this way (method="slot_expired") -- a drop the receiver
        # then had to notice and re-request through RNS, which is strictly
        # slower than sending late. The slot is a pacing hint, not a gate.
        slot = self._fragmented_send_slot(peer_prefix, priority)
        slot_wait_s = 0.0
        slot_held = False
        if slot is not None:
            wait_started = time.monotonic()
            slot_budget_s = max(0.0, min(
                self.outgoing_max_age_s,
                (expires_at - wait_started) if expires_at is not None else self.outgoing_max_age_s,
            ))
            try:
                await asyncio.wait_for(slot.acquire(priority), timeout=slot_budget_s)
                slot_held = True
            except asyncio.TimeoutError:
                RNS.log(
                    f"{self}: fragmented DIRECT send to {peer_prefix!r} ({len(data)} bytes) waited "
                    f"{slot_budget_s:.0f}s for one of {slot.capacity} in-flight slot(s) without getting one "
                    f"({slot.holders()} held, {slot.waiting()} still queued) -- sending anyway rather than "
                    f"dropping it (field fix 2026-09-19 night).",
                    RNS.LOG_WARNING,
                )
            slot_wait_s = time.monotonic() - wait_started
            send_info["slot_wait_s"] = round(slot_wait_s, 3)
            if slot_wait_s > 1.0 and slot_held:
                self._debug(
                    f"fragmented DIRECT send to {peer_prefix!r} waited {slot_wait_s:.1f}s for an "
                    f"in-flight slot (direct_fragmented_max_in_flight={self.direct_fragmented_max_in_flight})."
                )
        try:
            return await self._send_direct_fragmented_payload(
                target, peer_prefix, data, priority=priority, hop_count=hop_count,
                expires_at=expires_at, send_info=send_info,
            )
        finally:
            if slot_held:
                slot.release()

    def _fragmented_send_slot(self, peer_prefix: str, priority: int) -> "Optional[_PriorityAsyncSemaphore]":
        """The per-peer semaphore bounding concurrent fragmented sends, or
        None when the cap is off or the send is handshake class (a Link
        handshake never waits behind bulk transfers). Created lazily so it
        binds to the interface's own event loop.

        Field fix (2026-09-19 night): two semaphores per peer, not one.
        Announce-class sends (`PRIORITY_LOW`: ANNOUNCE, PATH_RESPONSE --
        see `_priority_tier`) share a single-permit slot of their own, so
        a 30-minute LXMF announce reconciling at two hops can never occupy
        a data slot: in the night capture both of the laptop's data slots
        were held by exactly such announces when two data packets were
        dropped. Everything else shares the `direct_fragmented_max_in_
        flight` data slots, served by priority (`_PriorityAsyncSemaphore`)
        rather than arrival order."""
        if self.direct_fragmented_max_in_flight <= 0 or priority == self.PRIORITY_HANDSHAKE:
            return None
        if priority == self.PRIORITY_LOW:
            key, capacity = (peer_prefix, "announce"), 1
        else:
            key, capacity = (peer_prefix, "data"), self.direct_fragmented_max_in_flight
        slot = self._fragmented_send_slots.get(key)
        if slot is None or slot.capacity != capacity:
            slot = _PriorityAsyncSemaphore(capacity)
            self._fragmented_send_slots[key] = slot
        return slot

    async def _send_direct_fragmented_payload(
        self, target: str, peer_prefix: str, data: bytes, priority: int,
        hop_count: Optional[int], expires_at: Optional[float], send_info: dict,
    ) -> bool:
        """The fragmented half of `_send_direct_payload` (raw first, text
        fallback), run inside one of the per-peer in-flight slots."""
        resume_key = (peer_prefix, RNS.Identity.truncated_hash(data))
        can_resume = (
            self.direct_fragment_resume_enabled
            and self.direct_fragment_reconcile_enabled
            and self.direct_completion_check_enabled
            and priority != self.PRIORITY_HANDSHAKE
        )
        resume = self._resumable_sends.get(resume_key) if can_resume else None
        if resume is not None:
            self._resumable_sends.pop(resume_key, None)
            if time.monotonic() >= resume["expires_at"]:
                resume = None
        pkt_id = resume["pkt_id"] if resume is not None else self._next_pkt_id()
        raw_path_hex = None
        if self._raw_fragments_eligible(peer_prefix, priority):
            # Remembered before the raw attempt: the verdict below is about
            # the path raw was actually tried on (audit fix, 2026-09-19).
            _raw_resolved = self._resolved_paths.get(peer_prefix)
            raw_path_hex = (_raw_resolved.out_path_hex or "") if _raw_resolved is not None else None
            raw_result = await self._send_direct_raw_fragmented(
                target, peer_prefix, data, pkt_id, priority=priority, hop_count=hop_count,
                expires_at=expires_at, resume=resume, resume_key=resume_key,
            )
            if raw_result is not None:
                send_info["method"] = "raw"
                return raw_result
            # None: raw declined or fell back mid-way -- fresh pkt_id, text path.
            send_info["fallback_from_raw"] = True
            pkt_id = self._next_pkt_id()
            resume = None
        send_info["method"] = "z85_text"
        text_ok = await self._send_direct_fragmented(
            target, peer_prefix, data, pkt_id, priority=priority, hop_count=hop_count,
            expires_at=expires_at, resume=resume, resume_key=resume_key,
        )
        pending_path = None
        if raw_path_hex is not None:
            if self._raw_fallback_pending.pop((peer_prefix, raw_path_hex), None) is not None:
                pending_path = raw_path_hex
        if pending_path is not None:
            self._note_raw_fallback_outcome(peer_prefix, pending_path, bool(text_ok))
        return text_ok

    async def _send_raw_fragment(
        self, path: bytes, frame: bytes, priority: int, telemetry: Optional[dict] = None,
        interrupt: "Optional[asyncio.Event]" = None,
    ) -> bool:
        """One raw fragment out through the same gate every transmission
        passes (quiet defer skipped: a burst is always racing the
        receiver's reassembly clock), then CMD_SEND_RAW_DATA. Returns
        whether the firmware accepted it; never waits for anything after."""
        on_air = 2 + len(path) + len(frame)
        gate = await self._pre_transmit_gate(
            "", skip_quiet_defer=True, duty_cycle_exempt=self._duty_cycle_exempt(priority), on_air_bytes=on_air,
            interrupt=interrupt,
        )
        if telemetry is not None:
            telemetry["quiet_defer_wait_s"], telemetry["duty_cycle_wait_s"], telemetry["medium_hold_wait_s"] = gate
        await self._run_command(
            self._mc_ready.commands.send_raw_data(frame, path), "send_raw_data", self._EventType.OK,
        )
        self.txb += len(frame)
        return True

    def _raw_fragment_gap_s(self, hops: int, on_air_bytes: int) -> float:
        """Quiet time after one raw fragment before this node transmits
        anything else (2026-09-19 morning field test, both captures).

        Zero hop: `direct_raw_zero_hop_gap` flat -- the receiver sends no
        ACK, only its own processing needs covering. Through repeaters:
        `direct_raw_hop_gap_factor` x `hops` x the fragment's own airtime.
        The chain is a half-duplex pipeline: each repeater re-transmits the
        fragment after a random delay (simple_repeater `getDirectRetransmit
        Delay`: rand(0..5) x `direct_tx_delay_factor` 0.3 x airtime, so 0 to
        1.5 airtimes per hop) and cannot hear the next fragment while it
        does. The pre-fix gap was 2 airtimes regardless of hop count; at 2
        and 4 hops every 2-fragment raw send in both directions lost
        exactly one fragment (laptop pkt 0/1/2, desktop pkt 19/20), and
        solo re-sends of the missing one arrived. `hops` x airtime is the
        chain's collision-free floor with no repeater delay at all; the
        factor of 2 sits between the firmware's mean (1.75x per hop) and
        worst case (2.5x). At one hop this equals the pre-fix gap the first
        raw field test passed with."""
        if hops <= 0:
            return max(0.0, self.direct_raw_zero_hop_gap_s)
        # MeshBench finding 2 (2026-09-20, real firmware): the gap starts when
        # send_raw_data returns OK, which the firmware gives when the frame is
        # QUEUED, so the fragment's own airtime was eaten out of the gap and
        # the next fragment (or the QUERY) left ~0.9s after the frame ended,
        # inside the repeater's relay of it -- 7/7 second fragments lost at R
        # in large_payload, 7/9 QUERYs in relay. The frame's own airtime is
        # now added on top of the hop-scaled term.
        airtime = self._estimate_tx_airtime_s("", on_air_bytes=on_air_bytes)
        return max(0.0, (1.0 + self.direct_raw_hop_gap_factor * hops) * airtime)

    def _completion_report_wait_s(self, hops: int, peer_prefix: str) -> float:
        """How long a raw sender keeps its radio quiet after a burst for the
        receiver's unsolicited completion report (2026-09-20): `direct_raw_
        report_wait_base` + `..._per_hop` x hops -- the report's own airtime
        plus one relay per repeater with the firmware's random forward
        delay, the same physics `_completion_quiet_window_s` sizes for a
        QUERY's answer -- never longer than the answer budget a QUERY would
        get. A report that does not arrive in that time was lost or is
        queued behind the receiver's own sends, and waiting longer only
        delays the QUERY fallback (third cut: the first two waited for a
        receiver-side idle timer as well and MeshBench `large_payload`, a
        lossy bidirectional one-hop case with a 60 s per-probe deadline,
        went 0/6 against a 1/6-4/6 baseline)."""
        window_s = self.direct_raw_report_wait_base_s + self.direct_raw_report_wait_per_hop_s * max(0, hops)
        # Phase 1 (2026-09-20): the measured report latency (burst end ->
        # report arrival, late reports included so the estimate is not
        # truncated by the window it sizes) widens the window above the
        # hop-scaled floor; the QUERY answer budget still caps it.
        rs = self._report_rtt.get(peer_prefix)
        if rs is not None:
            # srtt + 2 x rttvar (the factor `_completion_quiet_window_s`
            # uses), not RFC 6298's 4: a lost report at zero hop costs the
            # whole window and the true distribution there is median ~3 s,
            # p90 ~7 s (2026-09-20 review), which +4 x rttvar overshoots.
            window_s = max(window_s, rs["srtt"] + 2.0 * rs["rttvar"])
        budget_s = self._completion_query_timeout_s(peer_prefix, hops)
        return max(0.0, min(window_s, budget_s))

    def _record_report_latency(self, peer_prefix: Optional[str], pkt_id: int) -> Optional[float]:
        """One burst-end -> REPORT-arrival sample for the window estimator
        (2026-09-20), taken in `_handle_incoming_completion_frame` for every
        report that matches an expectation `_send_direct_raw_fragmented`
        registered -- whether the report arrives inside the window or after
        it (the sender may already be in its QUERY fallback). Returns the
        latency, or None when nothing was expected."""
        burst_end = self._report_expected.get((peer_prefix, pkt_id))
        if burst_end is None:
            return None
        latency_s = time.monotonic() - burst_end
        self._rtt_sample(self._report_rtt, peer_prefix, latency_s)
        return latency_s

    def _expect_report(self, peer_prefix: str, pkt_id: int, burst_end: Optional[float]) -> None:
        """Register (or, with None, withdraw) the burst end time a report
        for (peer, pkt_id) is measured against."""
        key = (peer_prefix, pkt_id)
        if burst_end is None:
            self._report_expected.pop(key, None)
        else:
            self._report_expected[key] = burst_end

    async def _await_completion_report(
        self, fut: "asyncio.Future", peer_prefix: str, pkt_id: int, frag_total: int, hops: int, stage: str,
        last_sent_idx: Optional[int] = None, rearm=None, release_lock=None,
    ) -> Optional[_CompletionFrame]:
        """Wait (lock held, radio quiet) for the receiver's completion
        report after a raw burst; None if none arrived inside
        `_completion_report_wait_s`, in which case the caller falls back to
        the QUERY path. A report is captured as a `completion_check_result`
        with outcome "reported" so the reconcile accounting stays in one
        record type; no record is written when nothing arrives (the QUERY
        that follows writes its own).

        Phase 1 (2026-09-20): an INCOMPLETE report whose only gap is
        `last_sent_idx` -- the burst's last fragment -- is the second-last
        fragment's report (both are flagged), sent by the receiver moments
        before the last fragment landed. Taken as final it re-drives that
        fragment as a duplicate: 20 of the desktop's 43 "reported" hop-0
        rounds in the 2026-09-20 session did exactly that (each followed by
        a one-fragment round and a second report). It is now provisional:
        `rearm()` puts a fresh future under the same key so the complete
        report can still land, the wait continues for up to half the window
        more (the complete report follows the incomplete one by a median
        1.5-1.9 s at zero hop -- its ACK wait -- and the receiver-side
        debounce of phase 3 removes the pair at the source), and the
        provisional report is acted on only if nothing better arrives
        (captured with `provisional: true`). When the last fragment really
        was lost this costs at most that extra half window; today's
        behaviour (re-drive it at once) is the fallback either way."""
        wait_s = self._completion_report_wait_s(hops, peer_prefix)
        started = time.monotonic()
        provisional: Optional[_CompletionFrame] = None
        deadline_s = wait_s
        released = release_lock is None
        while True:
            remaining = deadline_s - (time.monotonic() - started)
            if remaining <= 0:
                got = None
            else:
                try:
                    if not released:
                        # Phase 1 (2026-09-20): a queued Link handshake takes
                        # the radio; the rest of this wait is radio-free (the
                        # report future outlives the wait either way).
                        done, cut = await self._wait_future_or_preempt(fut, remaining)
                        if cut:
                            release_lock()
                            released = True
                            self._debug(
                                f"report wait ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}) released the radio to a "
                                f"Link handshake after {time.monotonic() - started:.2f}s; still listening for the report."
                            )
                            continue
                        got = fut.result() if done else None
                    else:
                        got = await asyncio.wait_for(asyncio.shield(fut), timeout=remaining)
                except (asyncio.TimeoutError, Exception):
                    got = None
            if got is None:
                if provisional is not None:
                    got = provisional
                    break
                self._debug(
                    f"no completion REPORT ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}) within "
                    f"{wait_s:.1f}s of the burst -- falling back to a QUERY."
                )
                return None
            missing_only_last = (
                not got.complete and got.held is not None and last_sent_idx is not None and rearm is not None
                and set(range(frag_total)) - set(got.held) == {last_sent_idx}
            )
            if missing_only_last and provisional is None:
                provisional = got
                fut = rearm()
                deadline_s = min(wait_s, (time.monotonic() - started) + wait_s / 2.0)
                self._debug(
                    f"completion REPORT ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}) misses only the "
                    f"last fragment sent ({last_sent_idx}) -- the second-last fragment's report; provisional, "
                    f"waiting the rest of the window for the last fragment's own."
                )
                continue
            break
        waited_s = time.monotonic() - started
        is_provisional = got is provisional
        self._debug(
            f"completion REPORT ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}): v{got.version} "
            f"complete={got.complete} held={sorted(got.held) if got.held is not None else None} "
            f"{waited_s:.1f}s after the burst's last gap."
        )
        if self._packet_capture_file is not None:
            self._capture_event("out", {
                "event": "completion_check_result",
                "peer_prefix": peer_prefix, "pkt_id": pkt_id, "frag_total": frag_total,
                "outcome": "reported", "complete": got.complete, "stage": stage,
                "timeout_s": round(wait_s, 3), "answer_version": got.version,
                "held": sorted(got.held) if got.held is not None else None,
                "report_wait_s": round(waited_s, 3), "provisional": is_provisional,
            })
        return got

    def _record_query_path_evidence(self, peer_prefix: str, infos: "list[dict]", answered: bool = False) -> None:
        """One raw reconcile round's QUERY sends, as stale-path evidence
        (2026-09-19 morning field test). Each QUERY is an ACKed DIRECT
        exchange over the cached path, so its firmware ACK proves the
        path, and `direct_raw_query_attempts` consecutive full-timeout
        misses disprove it as strongly as one text send's exhausted
        `direct_send_attempts` budget does. Before this, a raw send
        recorded one failure only after all its rounds were exhausted and
        the QUERYs recorded nothing: the desktop sat on a dead zero-hop
        path through 17 consecutive full-timeout QUERY misses and three
        whole 70s sends (09:58:23-10:01:05) before `record_direct_send_
        result` reached its threshold of 3. Counted per round, not per
        attempt, so a single lost ACK still is not a path failure."""
        if not infos:
            return
        if answered or any(i.get("acked") for i in infos):
            # An ANSWER proves the path even if the QUERY's own ACK was lost.
            self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
        elif all(i.get("waited_full_timeout") for i in infos):
            self.record_direct_send_result(peer_prefix, succeeded=False, waited_full_timeout=True)

    async def _raw_path_reset_mid_send(
        self, peer_prefix: str, path: bytes, pkt_id: int, rnd: int, acked: list, frag_total: int, remember,
    ) -> bool:
        """True when the path this raw send was started on is no longer
        the peer's resolved path (reset by `_reset_stale_path`, possibly
        from this send's own QUERY evidence, or re-resolved elsewhere):
        the remaining rounds would burst fragments source-routed down a
        path already known to be dead. The send is remembered for resume
        and abandoned; the next packet for this peer goes through
        discovery. `asyncio.sleep(0)` first lets the background reset
        task spawned a moment ago run before the check."""
        await asyncio.sleep(0)
        resolved = self._resolved_paths.get(peer_prefix)
        if resolved is not None and (resolved.out_path_hex or "") == path.hex():
            return False
        remember()
        self._outgoing_dropped_total += 1
        RNS.log(
            f"{self}: RAW send pkt_id={pkt_id} to {peer_prefix!r}: path {path.hex() or '<zero-hop>'} was reset "
            f"after round {rnd} -- abandoning the remaining rounds rather than bursting down a dead path; "
            f"receiver holds {sum(acked)}/{frag_total}.",
            RNS.LOG_WARNING,
        )
        return True

    async def _send_direct_raw_fragmented(
        self, target: str, peer_prefix: str, payload: bytes, pkt_id: int,
        priority: int = PRIORITY_NORMAL, hop_count: Optional[int] = None,
        expires_at: Optional[float] = None, resume: Optional[dict] = None, resume_key=None,
    ) -> Optional[bool]:
        """Raw binary fragments (2026-09-18 night, module docstring): burst
        every missing fragment, ask the receiver what it holds, repeat.
        Returns True (delivered), False (failed, recorded), or None (raw
        declined or disabled for this peer mid-way -- the caller re-sends
        as text fragments)."""
        resolved = self._resolved_paths.get(peer_prefix)
        own_prefix = self._own_pubkey_prefix()
        if resolved is None or own_prefix is None:
            return None
        try:
            path = bytes.fromhex(resolved.out_path_hex or "")
        except ValueError:
            return None
        budget = self._direct_raw_payload_budget(len(path))
        if budget <= 0:
            return None
        chunks = self._chunk_payload(payload, budget)
        frag_total = len(chunks)
        if frag_total > 255:
            return None
        if self._expired(expires_at):
            self._outgoing_dropped_total += 1
            RNS.log(f"{self}: dropping raw DIRECT send to {peer_prefix!r} -- packet expired before its first transmission.", RNS.LOG_WARNING)
            return False

        acked, resumed = self._resume_state(resume, frag_total, pkt_id, peer_prefix, raw=True)
        self._last_fragmented_pkt_id = pkt_id
        self._last_fragmented_frag_total = frag_total
        self._debug(
            f"RAW fragmented send starting: pkt_id={pkt_id} to {peer_prefix!r} frag_total={frag_total} "
            f"budget={budget}B path_len={len(path)} hop_count={hop_count}{' (resumed)' if resumed else ''}."
        )
        # Field fix (2026-09-19 morning): the gap is per fragment, scaled by
        # the number of repeaters that must each forward it before the
        # chain is clear -- see _raw_fragment_gap_s.
        gap_hops = max(0, hop_count if hop_count is not None else len(path))
        last_progress_at = time.monotonic() if resumed else None
        empty_answered_bursts = 0

        def remember() -> None:
            self._remember_resumable(resume_key, pkt_id, frag_total, acked, last_progress_at)

        rounds = max(1, self.direct_raw_reconcile_rounds)
        query_unanswered_rounds = 0
        # Field fix (2026-09-19, bidirectional image transfer): an unanswered
        # reconcile is "no information", and re-bursting every un-ACKed
        # fragment on it is a guess that lengthens the peer's queue -- the
        # very thing delaying its ANSWER. Bursts now only follow an answered
        # reconcile (or start the send); an unanswered round re-queries,
        # until `direct_raw_reburst_after_unanswered` consecutive silent
        # rounds allow one more burst as a safety valve.
        burst_allowed = True
        consecutive_unanswered = 0
        for rnd in range(rounds):
            # Audit fix (2026-09-19): expiry was checked once, before the
            # first burst. Three rounds of bursts plus their query waits far
            # exceed outgoing_max_age (120s), so a raw send could never
            # expire mid-flight the way the text path can.
            if rnd > 0 and self._expired(expires_at):
                self._outgoing_dropped_total += 1
                remember()
                self._debug(
                    f"RAW send pkt_id={pkt_id} to {peer_prefix!r}: giving up before round {rnd} -- "
                    f"packet expired (outgoing_max_age); receiver holds {sum(acked)}/{frag_total}."
                )
                return False
            missing = [i for i in range(frag_total) if not acked[i]]
            burst_this_round = bool(missing) and burst_allowed
            if missing and not burst_this_round:
                self._debug(
                    f"RAW send pkt_id={pkt_id} to {peer_prefix!r}: round {rnd} -- last reconcile "
                    f"unanswered, re-querying instead of re-bursting {len(missing)} fragment(s)."
                )
            report: Optional[_CompletionFrame] = None
            if burst_this_round:
                # Completion report (2026-09-20): the waiter is registered
                # BEFORE the burst -- for a multi-fragment burst the receiver
                # completes on the last fragment while this side is still
                # in that fragment's gap -- under the report nonce for this
                # round, so a stale round's incomplete bitmap can never be
                # applied (a `complete=True` report of any round is accepted
                # by the handler's monotone rule, as a late QUERY answer is).
                report_key = (peer_prefix, pkt_id)
                report_fut = None
                if self.direct_raw_report_enabled:
                    report_fut = asyncio.get_running_loop().create_future()
                    self._completion_query_waiters[report_key] = (
                        report_fut, frag_total, self.COMPLETION_REPORT_NONCE_BASE | (rnd & 0x03),
                    )
                # Phase 1 (2026-09-20): the lock is taken by hand so the
                # burst can YIELD it to a queued Link handshake -- during a
                # fragment's duty-cycle throttle wait, after (never inside)
                # a fragment's gap, and for the rest of the report wait --
                # and take it back at YIELDED_PRIORITY, ahead of ordinary
                # waiters (see _PriorityAsyncLock.yield_to_preempt).
                lock = self._direct_exchange_lock
                await lock.acquire(priority)
                lock_held = True
                yields = 0

                def release_for_handshake() -> None:
                    nonlocal lock_held
                    if lock_held:
                        lock.release()
                        lock_held = False

                try:
                    for n, frag_idx in enumerate(missing):
                        if self.detached or not self.online:
                            remember()
                            self._completion_query_waiters.pop(report_key, None)
                            return False
                        frame = self._encode_raw_fragment(
                            chunks[frag_idx], target, own_prefix, pkt_id, frag_idx, frag_total, attempt=rnd,
                            report=report_fut is not None and n >= len(missing) - 2,
                        )
                        telemetry: dict = {}
                        while True:
                            try:
                                await self._send_raw_fragment(path, frame, priority, telemetry, interrupt=lock.preempt_event())
                                sent_ok = True
                            except _PreemptedForHandshake:
                                # Yield inside the throttle wait: the
                                # handshake (duty-cycle exempt) goes, this
                                # fragment re-enters the gate afterwards.
                                yields += 1
                                await lock.yield_to_preempt()
                                if await self._raw_path_reset_mid_send(peer_prefix, path, pkt_id, rnd, acked, frag_total, remember):
                                    self._completion_query_waiters.pop(report_key, None)
                                    return False
                                continue
                            except Exception as exc:
                                sent_ok = False
                                RNS.log(f"{self}: raw fragment send failed locally (pkt_id={pkt_id} frag_idx={frag_idx}): {exc}", RNS.LOG_WARNING)
                            break
                        if self._packet_capture_file is not None:
                            self._capture_event("out", {
                                "event": "raw_fragment_sent", "peer_prefix": peer_prefix, "pkt_id": pkt_id,
                                "frag_idx": frag_idx, "frag_total": frag_total, "round": rnd, "ok": sent_ok,
                                "size_bytes": len(frame), "path_len": len(path), "hop_count": hop_count,
                                "on_air_bytes": (2 + len(path) + len(frame)) if sent_ok else None,
                                "duty_cycle_wait_s": telemetry.get("duty_cycle_wait_s"),
                                "medium_hold_wait_s": telemetry.get("medium_hold_wait_s"),
                                "handshake_yields": yields,
                            })
                        # Field fix (2026-09-19 morning): the gap follows EVERY
                        # fragment, the last one included, and is slept with
                        # the lock still held -- so the QUERY below (and any
                        # other send waiting on the lock) cannot enter the
                        # repeater chain while this fragment is still
                        # working its way down it.
                        gap_s = self._raw_fragment_gap_s(gap_hops, 2 + len(path) + len(frame))
                        if gap_s > 0:
                            await asyncio.sleep(gap_s)
                        if n < len(missing) - 1 and lock.preempt_requested():
                            # After the gap (the chain is clear), before the
                            # next fragment: let the handshake go.
                            yields += 1
                            await lock.yield_to_preempt()
                            if await self._raw_path_reset_mid_send(peer_prefix, path, pkt_id, rnd, acked, frag_total, remember):
                                self._completion_query_waiters.pop(report_key, None)
                                return False
                    if yields:
                        self._debug(f"RAW send pkt_id={pkt_id} to {peer_prefix!r}: round {rnd} yielded the radio to a Link handshake {yields} time(s).")
                    if report_fut is not None:
                        stale_report = None

                        def rearm(_key=report_key, _ft=frag_total, _rnd=rnd):
                            fresh = asyncio.get_running_loop().create_future()
                            self._completion_query_waiters[_key] = (
                                fresh, _ft, self.COMPLETION_REPORT_NONCE_BASE | (_rnd & 0x03),
                            )
                            return fresh

                        if report_fut.done() and not report_fut.result().complete:
                            # An INCOMPLETE report that arrived while this
                            # burst was still going (the second-last fragment
                            # is flagged too) describes a state the fragments
                            # sent since have changed. Keep it as the fallback
                            # and wait the transit time for the last
                            # fragment's own report first.
                            stale_report = report_fut.result()
                            report_fut = rearm()
                        # Radio kept quiet, lock still held: the receiver's
                        # report (and, right behind it, whatever RNS sends
                        # back) is crossing the chain now, and this node's
                        # next burst or QUERY is what used to collide with it.
                        # A report arriving from here until the round ends is
                        # measured for the window estimator (2026-09-20).
                        self._expect_report(peer_prefix, pkt_id, time.monotonic())
                        report = await self._await_completion_report(
                            report_fut, peer_prefix, pkt_id, frag_total, gap_hops, stage=f"raw{rnd}",
                            last_sent_idx=missing[-1] if missing else None, rearm=rearm,
                            release_lock=release_for_handshake,
                        )
                        if report is None and stale_report is not None:
                            # The last fragment (or its report) was lost: the
                            # earlier report is authoritative for everything
                            # but the fragments sent after it, which are
                            # re-driven -- at worst one duplicate fragment,
                            # never a QUERY round trip.
                            report = stale_report
                            self._capture_completion_check_result(
                                peer_prefix, pkt_id, frag_total, "reported_stale", stale_report.complete,
                                stage=f"raw{rnd}", answer_version=stale_report.version,
                                held=sorted(stale_report.held) if stale_report.held is not None else None,
                            )
                finally:
                    release_for_handshake()
                self._completion_query_waiters.pop(report_key, None)
            held_before = sum(acked)
            answer = report
            query_infos: list = []
            if report is not None:
                # The report is the ANSWER; the path evidence a QUERY's ACK
                # would have given is the report itself (it crossed the path).
                self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
            for q in range(max(1, self.direct_raw_query_attempts) if report is None else 0):
                info: dict = {}
                answer = await self._query_remote_fragments(
                    target, peer_prefix, pkt_id, frag_total, stage=f"raw{rnd}", priority=priority, hop_count=hop_count,
                    send_info=info,
                )
                query_infos.append(info)
                if answer is not None or self.detached or not self.online:
                    break
            self._expect_report(peer_prefix, pkt_id, None)
            if self.detached or not self.online:
                remember()
                return False
            # Field fix (2026-09-19 morning): the QUERYs are ACKed DIRECT
            # exchanges over the cached path -- their firmware ACKs are the
            # same stale-path evidence the text path's sends feed.
            self._record_query_path_evidence(peer_prefix, query_infos, answered=answer is not None)
            if answer is None:
                query_unanswered_rounds += 1
                consecutive_unanswered += 1
                burst_allowed = 0 < self.direct_raw_reburst_after_unanswered <= consecutive_unanswered
                self._debug(f"RAW send pkt_id={pkt_id} to {peer_prefix!r}: round {rnd} reconcile unanswered.")
                if await self._raw_path_reset_mid_send(peer_prefix, path, pkt_id, rnd, acked, frag_total, remember):
                    return False
                continue
            held = self._held_from_answer(answer, frag_total)
            if held is None:
                # Audit fix (2026-09-19): a v1 ANSWER carries no bitmap at
                # all, which is "no per-fragment information" -- NOT "holds
                # nothing" (reading it as an empty set once blacklisted a v1
                # peer's whole repeater chain for a day). Unanswered round.
                query_unanswered_rounds += 1
                consecutive_unanswered += 1
                burst_allowed = 0 < self.direct_raw_reburst_after_unanswered <= consecutive_unanswered
                self._debug(
                    f"RAW send pkt_id={pkt_id} to {peer_prefix!r}: round {rnd} answered v1 "
                    f"(no bitmap) -- no per-fragment information, treating as unanswered."
                )
                continue
            acked = [i in held for i in range(frag_total)]
            burst_allowed = True
            consecutive_unanswered = 0
            if held:
                last_progress_at = time.monotonic()
            self._debug(
                f"RAW send pkt_id={pkt_id} to {peer_prefix!r}: round {rnd} -- receiver holds "
                f"{sorted(held)} of {frag_total}."
            )
            if all(acked):
                self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
                self._resumable_sends.pop(resume_key, None)
                # Field fix (2026-09-19 night): a completed raw send is the
                # evidence that clears the soft incomplete-strike count.
                self._raw_incomplete_strikes.pop(peer_prefix, None)
                return True
            if sum(acked) <= held_before and burst_this_round:
                # A strike needs a burst that provably delivered nothing; a
                # re-query round sent no data and says nothing about raw.
                empty_answered_bursts += 1
                if empty_answered_bursts >= max(1, self.direct_raw_fallback_strikes):
                    # The text path works (the ANSWER came back) but raw
                    # frames are not arriving. Pause raw for this peer and
                    # fall back to Z85 on the same path; the caller records
                    # the verdict per PATH once the text send's outcome is
                    # known (_note_raw_fallback_outcome).
                    self._raw_disabled_until[peer_prefix] = time.monotonic() + self.direct_raw_fallback_cooldown_s
                    # A verdict on the chain is only possible if raw delivered
                    # NOTHING on it (review, 2026-09-19): a chain that carried
                    # fragments 0 and 1 and then stalled is lossy, not
                    # raw-incapable, and must not be noted.
                    nothing_ever_held = not any(acked)
                    if nothing_ever_held:
                        # Audit fix (2026-09-19): keyed (peer, path) rather
                        # than peer alone -- two concurrent sends to the same
                        # peer could otherwise cross wires and attribute one
                        # send's text success to the other's raw failure,
                        # blacklisting a chain for 24h on someone else's
                        # evidence.
                        self._raw_fallback_pending[(peer_prefix, path.hex())] = time.monotonic()
                    RNS.log(
                        f"{self}: raw fragments to {peer_prefix!r} are not arriving over path "
                        f"{path.hex() or '<zero-hop>'} ({empty_answered_bursts} answered reconciles, nothing new "
                        f"held; receiver holds {sum(acked)}/{frag_total}) -- re-sending as Z85 text"
                        + ("; if that succeeds the path is noted as not carrying raw." if nothing_ever_held
                           else " (raw did deliver part of it, so no verdict on the chain)."),
                        RNS.LOG_WARNING,
                    )
                    return None
            else:
                empty_answered_bursts = 0

        remember()
        if query_unanswered_rounds == rounds:
            # Unanswered throughout: nothing is known about the path -> a
            # real failure, recorded like any other.
            self.record_direct_send_result(peer_prefix, succeeded=False, waited_full_timeout=True)
            RNS.log(
                f"{self}: RAW fragmented send pkt_id={pkt_id} to {peer_prefix!r} gave up after {rounds} round(s) "
                f"with no reconcile ever answered: receiver holds {sum(acked)}/{frag_total}.",
                RNS.LOG_WARNING,
            )
            return False
        # Answered but still incomplete after every round: the path is alive
        # and raw made progress, it just did not finish under this loss.
        # Not a path failure and not a verdict on the chain -- hand the packet
        # to the Z85 text path (per-fragment ACKs, finishing budget) rather
        # than drop it. The receiver's raw bucket is remembered for resume.
        #
        # Field fix (2026-09-19 night): this used to pause raw for the peer
        # unconditionally, so the next packets under the same loss would go
        # straight to text instead of each spending three raw rounds first
        # (review, 2026-09-19). The night capture (`fieldtests/raw/Alpha0.1.2/
        # desktop_afipc_20260919T212602_nighttest.jsonl`) shows the cost: at
        # 21:45:14 one part lost the same fragment three rounds running,
        # and the 600s pause that followed carried the next 46 page parts as
        # five text fragments plus five ACKs each -- on a path where 16 of
        # the session's 20 raw sends had completed. One incomplete send is
        # one unlucky fragment, not evidence about the chain: the e87cca8
        # build (8 of 8 raw sends complete at one hop) only ever paused on
        # the two-strike "delivered nothing" rule above. So this is now a
        # SOFT strike, and raw pauses only when `direct_raw_incomplete_
        # strikes` (2) consecutive raw sends to this peer end this way; a
        # completed raw send clears the count (a path change clears it too,
        # via _clear_peer_path_stats).
        strikes = self._raw_incomplete_strikes.get(peer_prefix, 0) + 1
        self._raw_incomplete_strikes[peer_prefix] = strikes
        pause_raw = 0 < self.direct_raw_incomplete_strikes <= strikes
        if pause_raw:
            self._raw_disabled_until[peer_prefix] = time.monotonic() + self.direct_raw_fallback_cooldown_s
            self._raw_incomplete_strikes.pop(peer_prefix, None)
        # Review (2026-09-19): the chain verdict is about the whole send, not
        # the strike sequence -- if raw delivered nothing in any round while
        # the text-path reconcile was answered at least once, this is the
        # same "Z85 works, binary doesn't" evidence the strike rule looks
        # for, and an unanswered round in between must not hide it.
        if not any(acked):
            self._raw_fallback_pending[(peer_prefix, path.hex())] = time.monotonic()
        RNS.log(
            f"{self}: RAW fragmented send pkt_id={pkt_id} to {peer_prefix!r} incomplete after {rounds} round(s) "
            f"(receiver holds {sum(acked)}/{frag_total}) -- re-sending as Z85 text"
            + ("; nothing arrived raw, so a successful text send notes the path." if not any(acked) else ".")
            + (f" Raw paused for this peer for {self.direct_raw_fallback_cooldown_s:.0f}s "
               f"({strikes} consecutive incomplete raw send(s))." if pause_raw
               else f" Incomplete-send strike {strikes} of {self.direct_raw_incomplete_strikes} -- "
                    f"the next packet still goes raw-first."),
            RNS.LOG_WARNING,
        )
        return None

    async def _send_direct_fragmented(
        self, target: str, peer_prefix: str, payload: bytes, pkt_id: int,
        priority: int = PRIORITY_NORMAL, hop_count: Optional[int] = None,
        expires_at: Optional[float] = None,
        resume: Optional[dict] = None, resume_key=None,
    ) -> bool:
        """docs/reliability_engine_design.md §4's two-pass DIRECT-
        fragmentation structure, fixed by a logical review specifically
        to give "only re-drive the still-missing fragments" a concrete
        trigger: **pass 0** sends every `frag_idx` in order, one at a
        time -- never several back-to-back without waiting, the same
        half-duplex-derived rule as CHANNEL's own spacing, here enforced
        for free by fully awaiting each fragment's own send+ACK(+retry)
        cycle before starting the next -- each with its own bounded
        `direct_send_attempts` budget. **Pass 1** re-attempts only
        whatever never got ACKed in pass 0, in order, each again with a
        fresh attempt budget. If fragments still appear missing after
        both passes, one last-resort `_check_remote_completion` asks the
        receiver directly rather than assuming the data never arrived
        (field-data-driven fix, 2026-09-16 -- see that method's own
        docstring for the phantom-ACK-loss story this closes). Returns
        True if every fragment was eventually ACKed across both passes,
        or if the receiver later confirms it has the complete message
        anyway.

        Step 3 (2026-09-18) reshaped pass 0 for non-handshake sends into
        send-once-then-reconcile -- see `direct_fragment_reconcile_enabled`'s
        own comment and the inline notes below: pass 0 makes
        `direct_fragment_pass0_attempts` unrecorded attempt(s) per fragment,
        one QUERY asks the receiver what it holds, and pass 1 re-drives only
        the confirmed gaps with the normal recorded budget."""
        chunks = self._fragment_direct_payload(payload)
        frag_total = len(chunks)
        # Alpha 0.1.1 resume (see _send_direct_payload): start from what the
        # receiver is believed to hold; the reconcile QUERY below is forced
        # so that belief is checked against the receiver's actual bucket.
        acked, resumed = self._resume_state(resume, frag_total, pkt_id, peer_prefix, raw=False)
        # time.monotonic() of the most recent evidence that the receiver's
        # bucket made progress (an ACK, or a reconcile answer) -- the
        # receiver's idle clock restarts on each fragment it receives.
        last_progress_at = time.monotonic() if resumed else None
        # Most recent fragmented send's identity -- observability only
        # (testscripts/zero_hop_peer_discovery_test.py --verify-query reads
        # it to ask the receiver what it holds for this exact pkt_id).
        self._last_fragmented_pkt_id = pkt_id
        self._last_fragmented_frag_total = frag_total

        # Observability addition (2026-09-18, user-requested): CHANNEL's
        # own multi-fragment sender (_send_channel_multifragment_pass) logs
        # one "starting a send" line up front with frag_total/order/
        # spacing; this method had no equivalent, only the per-attempt line
        # inside _send_direct_frame_and_wait_for_ack and the pass-1 line
        # below -- so "what is this node about to send, and how many
        # fragments" wasn't visible until the first attempt had already
        # happened. DIRECT pass 0 is always sent in strict frag_idx order
        # (no shuffle, unlike CHANNEL -- see the fix note below), so there's
        # no "order" to log here.
        self._debug(
            f"DIRECT fragmented send starting: pkt_id={pkt_id} to {peer_prefix!r} "
            f"frag_total={frag_total} hop_count={hop_count}."
        )

        async def send_one(
            frag_idx: int, time_critical: bool = False, pass_number: int = 0,
            attempts_override: Optional[int] = None, record_result: bool = True,
        ) -> bool:
            return await self._send_direct_with_attempts(
                target,
                lambda attempt, c=chunks[frag_idx], fi=frag_idx, ft=frag_total: (
                    self._encode_channel_multifragment(c, pkt_id, fi, ft, attempt)
                ),
                peer_prefix,
                pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, priority=priority,
                hop_count=hop_count, time_critical=time_critical, pass_number=pass_number,
                attempts_override=attempts_override, record_result=record_result,
                # Page-load fix (2026-09-18 evening): expiry is decided only
                # before the packet's very first transmission. Every later
                # fragment/pass is already committed air.
                expires_at=(expires_at if (frag_idx == 0 and pass_number == 0) else None),
            )

        # Step 3 (2026-09-18, see direct_fragment_reconcile_enabled's own
        # comment): send-once-then-reconcile. Pass 0 sends each fragment
        # once (unrecorded -- a lost ACK isn't a path failure), then ONE
        # QUERY asks the receiver what it holds, and pass 1 re-drives only
        # what it confirms missing. Handshake-class sends keep the old
        # per-fragment budget in pass 0 and skip the reconcile.
        reconcile = (
            self.direct_fragment_reconcile_enabled
            and self.direct_completion_check_enabled
            and priority != self.PRIORITY_HANDSHAKE
        )
        pass0_attempts = max(1, self.direct_fragment_pass0_attempts) if reconcile else None

        # Field-diagnosed fix (2026-09-18, see module docstring): only
        # frag_idx 0 is a genuinely fresh transmission that can afford
        # _pre_transmit_gate's incoming-quiet-defer courtesy. The receiver
        # opens its reassembly bucket -- and starts its
        # reassembly_idle_timeout_s clock -- the moment that first fragment
        # lands (_ReassemblyBucket.last_progress), so every fragment after
        # it is already racing a running deadline, exactly like a re-drive.
        # Pass 0 is sent strictly in frag_idx order here (unlike the CHANNEL
        # path, this one never shuffles), so frag_idx > 0 is a reliable
        # "the receiver's clock is already ticking" test.
        def remember_for_resume() -> None:
            if reconcile:
                self._remember_resumable(resume_key, pkt_id, frag_total, acked, last_progress_at)

        for frag_idx in range(frag_total):
            if acked[frag_idx]:
                continue  # resumed: the receiver already holds this one
            acked[frag_idx] = await send_one(
                frag_idx, time_critical=(frag_idx > 0 or resumed), pass_number=0,
                attempts_override=pass0_attempts, record_result=not reconcile,
            )
            if acked[frag_idx]:
                last_progress_at = time.monotonic()
            if self.detached or not self.online:
                return False

        missing = [i for i in range(frag_total) if not acked[i]]
        if reconcile and any(acked) and not resumed:
            self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
        # Alpha 0.1.1: a resumed send always asks, even when nothing looks
        # missing -- the pre-marked fragments are a belief about the
        # receiver's bucket, and the answer below is the ground truth.
        if reconcile and (missing or resumed):
            answer = await self._query_remote_fragments(
                target, peer_prefix, pkt_id, frag_total, stage="reconcile", priority=priority, hop_count=hop_count,
            )
            if self.detached or not self.online:
                remember_for_resume()
                return False
            held = self._held_from_answer(answer, frag_total) if answer is not None else None
            if answer is not None and held is None:
                # Audit fix (2026-09-19): v1 ANSWER, no bitmap -- no
                # per-fragment information. Leave `acked` alone (pass 1 then
                # re-drives exactly what pass 0 could not confirm) rather
                # than discarding real pass-0 ACKs.
                self._debug(
                    f"DIRECT fragmented send pkt_id={pkt_id} to {peer_prefix!r}: reconcile answered v1 "
                    f"(no bitmap) -- keeping this pass's own ACK results."
                )
            elif answer is not None:
                confirmed = [i for i in missing if i in held]
                lost = [i for i in range(frag_total) if acked[i] and i not in held]
                # Authoritative: the receiver's bucket decides, in both
                # directions (Alpha 0.1.1 -- previously only un-ACKed
                # fragments were updated, so a bucket the receiver had
                # evicted could never be re-driven).
                acked = [i in held for i in range(frag_total)]
                if confirmed:
                    self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
                    last_progress_at = time.monotonic()
                elif held:
                    last_progress_at = time.monotonic()
                missing = [i for i in range(frag_total) if not acked[i]]
                self._debug(
                    f"DIRECT fragmented send pkt_id={pkt_id} to {peer_prefix!r}: reconcile -- "
                    f"receiver holds {sorted(held)}; {len(confirmed)} un-ACKed fragment(s) confirmed "
                    f"delivered, {len(lost)} believed-delivered fragment(s) NOT held, "
                    f"{len(missing)} still missing."
                )
                if answer.complete:
                    RNS.log(
                        f"{self}: DIRECT fragmented send pkt_id={pkt_id} to {peer_prefix!r} -- "
                        f"{len(confirmed)}/{frag_total} fragment(s) got no ACK but the receiver "
                        f"already holds the complete message; skipping the re-drive pass.",
                        RNS.LOG_WARNING,
                    )
                    self._resumable_sends.pop(resume_key, None)
                    return True
        if missing:
            # Alpha 0.1.1: when the receiver provably holds part of this
            # packet, the rest is the whole difference between wasted air
            # and a delivered packet -- spend the larger finishing budget.
            partially_held = any(acked)
            finish_attempts = self.direct_fragment_finish_attempts if (reconcile and partially_held) else None
            self._debug(
                f"DIRECT fragmented send pkt_id={pkt_id} to {peer_prefix!r}: "
                f"pass 1 re-driving {len(missing)}/{frag_total} still-missing "
                f"fragment(s)" + (f" with the finishing budget ({finish_attempts} attempts)." if finish_attempts else ".")
            )
            for frag_idx in missing:
                acked[frag_idx] = await send_one(
                    frag_idx, time_critical=True, pass_number=1, attempts_override=finish_attempts,
                )
                if acked[frag_idx]:
                    last_progress_at = time.monotonic()
                if self.detached or not self.online:
                    remember_for_resume()
                    return False

        if all(acked):
            self._resumable_sends.pop(resume_key, None)
            return True

        if self.direct_completion_check_enabled:
            confirmed = await self._check_remote_completion(
                target, peer_prefix, pkt_id, frag_total, priority=priority, hop_count=hop_count,
            )
            if confirmed:
                RNS.log(
                    f"{self}: DIRECT fragmented send pkt_id={pkt_id} to {peer_prefix!r} -- "
                    f"{sum(1 for a in acked if not a)}/{frag_total} fragment(s) never got a "
                    f"real ACK, but the receiver confirms it has the complete message "
                    f"anyway (lost ACK on the return path, not a delivery failure). "
                    f"Treating as delivered and clearing this peer's recorded failures.",
                    RNS.LOG_WARNING,
                )
                # Undo the false failure signal record_direct_send_result
                # already recorded per-fragment above -- a fresh success
                # unconditionally clears _direct_path_failures for this
                # peer, so a phantom failure here can't leave behind a
                # false trigger for the next unrelated send's stale-path
                # threshold check.
                self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
                self._resumable_sends.pop(resume_key, None)
                return True

        remember_for_resume()
        return False

    async def _check_remote_completion(
        self, target: str, peer_prefix: str, pkt_id: int, frag_total: int,
        priority: int = PRIORITY_NORMAL, hop_count: Optional[int] = None,
    ) -> bool:
        """Field-data-driven fix (2026-09-16): real capture from a 5-node
        field test found a concrete case (`pkt_id=3`, router -> a client)
        where 2 of 3 fragments were logged as "never acknowledged" by the
        sender after exhausting both retry passes -- roughly 4+ minutes
        and 8 fragment-attempts total -- yet the receiver's own capture
        showed a completed reassembly of *all three* fragments about a
        second *before* the sender's own final successful ACK for the
        third fragment even landed. That's direct proof the first two
        fragments physically arrived; only their ACKs failed to make it
        back, an asymmetric/return-path loss this design previously had
        no way to distinguish from genuine non-delivery -- so it just
        kept blindly retrying data the receiver already had, burning
        airtime and `_direct_exchange_lock` time other queued sends were
        waiting on, and risking a false `direct_path_reset_threshold`
        trip (`record_direct_send_result`) over a link that was actually
        fine.

        Called from `_send_direct_fragmented` only once both retry passes
        are exhausted and fragments still appear missing -- never a
        substitute for the real firmware ACK, only a last resort before
        giving up on data that might have already arrived. Sends one
        lightweight `"Q"`-marker QUERY frame and waits up to
        `direct_completion_check_timeout_s` for a matching ANSWER,
        correlated via `_completion_query_waiters` keyed by `(peer_prefix,
        pkt_id)` (`_handle_incoming_completion_frame` resolves the future
        on receipt). Fully backward-compatible and fails safe: a peer
        that doesn't understand `"Q"` frames, or whose own answer is
        itself lost -- the same class of loss this whole mechanism exists
        to route around, just at much lower stakes for one small frame --
        simply never resolves the future, and this returns False once
        `direct_completion_check_timeout_s` elapses, falling back to
        exactly today's give-up behavior. Never raises: a local send
        failure here is treated the same as no answer, not propagated.

        Step 3 (2026-09-18): now a thin wrapper over
        `_query_remote_fragments`, which is also called *between* the
        passes as the reconcile step -- see `_send_direct_fragmented`."""
        answer = await self._query_remote_fragments(
            target, peer_prefix, pkt_id, frag_total, stage="final", priority=priority, hop_count=hop_count,
        )
        return answer is not None and answer.complete

    def _record_query_rtt(self, peer_prefix: Optional[str], rtt_s: float) -> None:
        """One measured QUERY -> ANSWER round trip (first raw field test,
        2026-09-18 night). Same estimator shape as `_record_ack_rtt`."""
        self._rtt_sample(self._query_rtt, peer_prefix, rtt_s)

    def _completion_unacked_grace_s(self, hop_count: Optional[int], peer_prefix: Optional[str] = None) -> float:
        """Answer wait after a QUERY whose own firmware ACK was missed
        (2026-09-20, see `direct_completion_unacked_grace_s`): the multihop
        value from 2 hops, else the base; 0 = no cap."""
        if hop_count is None:
            resolved = self._resolved_paths.get(peer_prefix) if peer_prefix else None
            hop_count = resolved.out_path_len if resolved is not None else 0
        if hop_count is not None and hop_count >= 2:
            return max(0.0, self.direct_completion_unacked_grace_multihop_s)
        return max(0.0, self.direct_completion_unacked_grace_s)

    def _completion_query_timeout_cap_s(self, hop_count: Optional[int], peer_prefix: Optional[str] = None) -> float:
        """The ceiling on a completion-ANSWER wait (field fix, 2026-09-19 --
        see `direct_completion_check_timeout_max_s` for the evidence). With
        no `hop_count`, the peer's resolved path decides (second audit: an
        explicit argument instead of a shared mutable hint attribute)."""
        if hop_count is None:
            resolved = self._resolved_paths.get(peer_prefix) if peer_prefix else None
            hop_count = resolved.out_path_len if resolved is not None else 0
        if hop_count is not None and hop_count >= 2:
            return self.direct_completion_check_timeout_max_multihop_s
        return self.direct_completion_check_timeout_max_s

    def _completion_query_timeout_s(self, peer_prefix: str, hop_count: Optional[int] = None) -> float:
        """How long to wait for a completion ANSWER: the measured
        QUERY -> ANSWER round trip for this peer when one exists, else the
        firmware's own hop-aware ACK bound doubled (a round trip is two
        exchanges), clamped into
        [`direct_completion_check_timeout_s`, `_completion_query_timeout_cap_s`].

        Field fix (2026-09-19 evening session, multi-agent capture audit).
        This replaced a stack of three escalations -- a `x (1 + hops)`
        multiplier on the floor, an RTT term of `2 x (srtt + 4*rttvar)`, and
        a per-queued-exchange contention term -- whose combined effect was
        budgets of 21-45s (median 41.4s on the timed-out checks). The session
        evidence is unambiguous that this was counter-productive: 96% of
        answers that ever arrived did so within 15s, every band beyond 20s
        produced two answers in the whole session, and the answer rate FELL
        as the budget grew (92% at 10-20s vs 34% at 40-45s). A long budget
        marks bad conditions; it does not repair them. The dominant real
        causes of an unanswered reconcile, established by pairing every query
        against the peer's own records, are plain frame loss: 48% the QUERY
        never reached the peer's application at all, 38% the peer answered
        and the answer never arrived. Neither is helped by waiting longer.
        The RTT term is kept (inside the cap) because it is genuinely
        adaptive downward on a good link; only the unbounded growth is gone.
        """
        # The FLOOR stays hop-aware, only the ceiling is new (correction made
        # while testing this change: dropping the hop term from the floor as
        # well took a first query at 1 hop from 10s to 5s, and the session's
        # own measurements say a query->answer round trip is median 3.2-5.7s
        # with a p90 of 11.7-16.1s -- so a flat 5s floor would abandon the p90
        # case before any RTT sample exists to widen it. The simulated 1-hop
        # raw scenario caught exactly that.)
        hops = max(0, hop_count or 0)
        floor_s = (
            self.direct_completion_check_timeout_s
            + self.direct_completion_check_timeout_per_hop_s * hops
        )
        cap_s = max(self.direct_completion_check_timeout_s,
                    self._completion_query_timeout_cap_s(hop_count, peer_prefix))
        floor_s = min(floor_s, cap_s)
        timeout_s = floor_s
        qs = self._query_rtt.get(peer_prefix)
        if qs is not None:
            timeout_s = 2.0 * (qs["srtt"] + 4.0 * qs["rttvar"])
        else:
            fw = self._last_firmware_ack_timeout_s.get(peer_prefix)
            if fw is not None:
                timeout_s = 2.0 * fw
        if self.rx_log_holds_enabled:
            # The peer's ANSWER pays its own pre-transmit hold before it can
            # leave, assuming it runs the same hold cap -- the best this side
            # can know. Inside the cap, not added on top of it.
            timeout_s += self.rx_log_hold_max_s
        return min(max(timeout_s, floor_s), cap_s)

    def _completion_quiet_window_s(self, hop_count: Optional[int], timeout_s: float,
                                   peer_prefix: Optional[str] = None) -> Optional[float]:
        """How long after a reconcile QUERY's firmware ACK the radio-quiet
        window lasts (field fix 2026-09-19 night; re-anchored at the ACK and
        made RTT-adaptive 2026-09-20, see `direct_completion_quiet_base_s`):
        `base + per_hop x hops` (or the measured round trip's srtt + 2 x
        rttvar when larger), never more than
        `timeout_s` (the answer budget -- the window can only move time
        that was being spent waiting anyway). None when the window is
        disabled (both keys 0), so the answer wait is fully radio-free as
        it was in commit 1919074. `_send_direct_frame_and_wait_for_ack`
        anchors it at the frame's MSG_SENT moment."""
        if self.direct_completion_quiet_base_s <= 0 and self.direct_completion_quiet_per_hop_s <= 0:
            return None
        if hop_count is None and peer_prefix is not None:
            # Same fallback as _completion_query_timeout_cap_s: the peer's
            # resolved path knows the hop count when the caller did not.
            resolved = self._resolved_paths.get(peer_prefix)
            hop_count = resolved.out_path_len if resolved is not None else 0
        hops = max(0, hop_count if hop_count is not None else 0)
        window_s = self.direct_completion_quiet_base_s + self.direct_completion_quiet_per_hop_s * hops
        # Review (2026-09-20): adaptive upward from the measured QUERY -> ANSWER
        # round trip (itself measured from the QUERY's ACK, the same anchor
        # this window uses) once three samples exist; the budget still caps it.
        qs = self._query_rtt.get(peer_prefix) if peer_prefix else None
        if qs is not None and qs.get("samples", 0) >= 3:
            window_s = max(window_s, qs["srtt"] + 2.0 * qs["rttvar"])
        return max(0.0, min(timeout_s, window_s))

    async def _query_remote_fragments(
        self, target: str, peer_prefix: str, pkt_id: int, frag_total: int, stage: str,
        priority: int = PRIORITY_NORMAL, hop_count: Optional[int] = None,
        send_info: Optional[dict] = None,
    ) -> Optional[_CompletionFrame]:
        """Step 3 (2026-09-18, see module docstring): one `"Q"` QUERY to the
        receiver, answered with its have-bitmap (v2) or a bare complete
        flag (a v1 peer). Returns the decoded ANSWER, or None if none
        arrived (lost, or the peer predates `"Q"`/v2 frames) -- callers
        treat None as "no information", never as "nothing arrived".
        `stage` is "reconcile" (between pass 0 and pass 1) or "final"
        (after pass 1, the pre-step-3 last resort) -- capture-only.

        The QUERY is sent as one ordinary ACKed DIRECT exchange (lock held
        through its transmit and firmware ACK by `_send_direct_frame_and_
        wait_for_ack`), the lock is then kept for a short hop-scaled
        radio-quiet window (`direct_completion_quiet_base` + `..._per_hop`
        x hops after the transmit -- field fix 2026-09-19 night, the
        hidden-node collision at the repeater), and the rest of the ANSWER
        budget is awaited with the radio free -- see the comment at that
        call site for why holding the lock through the WHOLE answer wait
        was reverted. `priority` is the enclosing
        send's own tier (the reconcile stage sits inside a fragmented send
        whose receiver-side clock is already running; queueing it behind
        every ordinary send at PRIORITY_LOW defeated its purpose), and the
        QUERY is `time_critical` for the same reason.

        Audit fix (2026-09-19): an ANSWER whose `frag_total` does not match
        this query's is ignored (see `_handle_incoming_completion_frame`) --
        `_completion_query_waiters` is keyed only `(peer_prefix, pkt_id)`,
        so a late answer to a *previous* query for the same packet could
        otherwise be applied authoritatively to this one."""
        key = (peer_prefix, pkt_id)
        fut = asyncio.get_running_loop().create_future()
        # Field fix (2026-09-19): a per-query nonce so a late answer to an
        # EARLIER query for this same pkt_id cannot resolve this one (the
        # frag_total guard alone could not -- five such stale resolutions
        # happened in the evening session, one applying held=[]).
        # 2026-09-20: cycles 1..COMPLETION_QUERY_NONCE_MAX, leaving 0 and the
        # 0xF0.. range to receiver-initiated reports (see the constant).
        self._completion_query_nonce = (self._completion_query_nonce % self.COMPLETION_QUERY_NONCE_MAX) + 1
        query_nonce = self._completion_query_nonce
        self._completion_query_waiters[key] = (fut, frag_total, query_nonce)
        outcome = "send_failed"
        answer: Optional[_CompletionFrame] = None
        timeout_s = self._completion_query_timeout_s(peer_prefix, hop_count)
        try:
            frame = self._encode_completion_frame(
                self.COMPLETION_TYPE_QUERY, pkt_id, frag_total, nonce=query_nonce,
            )
            # First raw field test (2026-09-18 night): the QUERY is one
            # ordinary ACKed exchange -- lock held through its transmit and
            # firmware ACK -- and the ANSWER is then awaited with the radio
            # free. Holding the lock through the answer wait (the earlier
            # review's shape) blocked this node's own ANSWERs to the peer's
            # queries for up to 50s under bidirectional traffic.
            sent_at = time.monotonic()
            # Field fix (2026-09-19 night): the first seconds of that answer
            # wait are NOT radio-free any more. `_send_direct_frame_and_wait_
            # for_ack` keeps the lock past its own listen delay until this
            # query's answer future resolves or this deadline passes -- the
            # span in which the ANSWER is actually crossing the repeater
            # chain, where the querier's own next burst would collide with it
            # at the repeater (a hidden node from both ends). See
            # `direct_completion_quiet_base_s` for the measured sizing and
            # the module docstring's 2026-09-19 night entry for the answer-
            # delivery numbers that motivated it. Anchored at the QUERY's own
            # transmit (inside the ack-wait method, after any lock wait) and
            # never longer than the answer budget itself, so this can only
            # ever move time that was already being spent waiting.
            quiet_window_s = self._completion_quiet_window_s(hop_count, timeout_s, peer_prefix)
            quiet_info: dict = {}
            try:
                # 2026-09-19: the QUERY rides PRIORITY_ANSWER (unless the
                # enclosing send is a handshake, which is higher still) --
                # see that constant's comment. A stalled transfer's one
                # small question should not queue behind this node's own
                # bulk bursts to other packets.
                # Field fix (2026-09-19): `attempt` varies per try instead of
                # being hardcoded 0, so the firmware's own content-derived
                # dedup/retry differentiation is actually exercised on a
                # repeated query. The evening session found a repeated
                # (pkt_id, frag_total) query answered only 43% of the time
                # versus 90% for a first-time query, at equal link quality --
                # mechanism unexplained, but leaving the firmware's attempt
                # field pinned at 0 on every retry could only be contributing.
                # Derived from the rolling nonce, so it advances on every query
                # without threading a second counter through the callers.
                q_ok, q_waited_full = await self._send_direct_frame_and_wait_for_ack(
                    target, frame, query_nonce & 0x03, peer_prefix=peer_prefix,
                    priority=min(priority, self.PRIORITY_ANSWER),
                    time_critical=True, kind="completion_query", hop_count=hop_count,
                    quiet_wait=fut, quiet_window_s=quiet_window_s, quiet_info=quiet_info,
                )
                # Field fix (2026-09-19 morning): the QUERY's own firmware
                # ACK outcome, for the caller's stale-path evidence
                # (_record_query_path_evidence). `send_info` is the same
                # out-param shape _send_direct_payload uses.
                if send_info is not None:
                    send_info["acked"] = bool(q_ok)
                    send_info["waited_full_timeout"] = bool(q_waited_full)
            except Exception as exc:
                if send_info is not None:
                    send_info["acked"] = False
                    send_info["waited_full_timeout"] = False
                self._debug(
                    f"completion QUERY ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}): "
                    f"send failed locally: {exc} -- treating as no answer."
                )
                return None
            # Audit fix (2026-09-19): the ANSWER budget starts when the QUERY
            # is actually out, not when this coroutine began. `_completion_
            # query_timeout_s` documents itself as "how long to wait for a
            # completion ANSWER", but the send call above also covers
            # `_direct_exchange_lock` queueing (49s observed in the
            # 2026-09-18 captures), the pre-transmit gate and the QUERY's own
            # firmware ACK wait. Charging all of that against the peer's
            # reply left `max(0.5, ...)` -- i.e. 0.5s -- for an ANSWER that
            # really needed seconds, and 36 of 106 archived completion checks
            # timed out. A timeout here means "no information", so every one
            # of those cost a full re-drive of fragments the receiver already
            # held (or, on the raw path, a false fallback strike).
            answer_wait_start = time.monotonic()
            # Field fix (2026-09-19 night; review fix 2026-09-20): the quiet
            # hold already spent part of this budget with the radio held --
            # it is charged here, so the window moves waiting time rather
            # than adding to it (the evening session's evidence is that a
            # longer budget marks bad conditions, it does not repair them).
            # The round trip is measured from the QUERY's ACK either way: for
            # an answer that arrived inside the hold, from the timestamps
            # the ack-wait method handed back; otherwise from here (which
            # is the same point, since the hold ended before this line).
            quiet_hold_s = float(quiet_info.get("hold_s", 0.0) or 0.0)
            remaining = max(0.0, timeout_s - quiet_hold_s)
            if not q_ok:
                # Dead-wait trims (2026-09-20): no firmware ACK for the QUERY
                # -> it most likely never reached the peer; a short grace
                # covers the answers that do arrive (see the config comment).
                grace_s = self._completion_unacked_grace_s(hop_count, peer_prefix)
                if grace_s > 0:
                    remaining = min(remaining, grace_s)
            rtt_origin = quiet_info.get("ack_done_at", answer_wait_start)
            try:
                got: _CompletionFrame = await asyncio.wait_for(fut, timeout=remaining)
                answer = got
                outcome = "answered"
                # Measured from the QUERY's ACK, so the estimator models the
                # peer's reply latency rather than this node's own queueing
                # (which would inflate every later timeout and hold the
                # radio longer on failures).
                answered_at = quiet_info.get("answered_at") or time.monotonic()
                if not quiet_info.get("not_sent") and (
                        got.nonce is None or (got.nonce & 0xF0) != self.COMPLETION_REPORT_NONCE_BASE):
                    # A late REPORT that resolved this QUERY's future (the
                    # monotone rule) is the report estimator's sample, not
                    # a QUERY round trip (phase 1, 2026-09-20: 24 of 29
                    # hop-0 "answered" outcomes were this, and they were
                    # shrinking _query_rtt with ~0 s samples).
                    self._record_query_rtt(peer_prefix, answered_at - rtt_origin)
                self._debug(
                    f"completion ANSWER ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}): v{got.version} "
                    f"complete={got.complete} held={sorted(got.held) if got.held is not None else None} "
                    f"after {answered_at - rtt_origin:.1f}s from its ACK "
                    f"({quiet_hold_s:.1f}s of it inside the quiet hold; "
                    f"{time.monotonic() - sent_at:.1f}s including the QUERY's own send)."
                )
                return got
            except asyncio.TimeoutError:
                outcome = "timeout"
                self._debug(
                    f"completion QUERY ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}): "
                    f"no answer within {timeout_s:.1f}s ({quiet_hold_s:.1f}s of it as the quiet hold) -- "
                    f"no information, proceeding as if unanswered."
                )
                return None
        finally:
            self._completion_query_waiters.pop(key, None)
            self._capture_completion_check_result(
                peer_prefix, pkt_id, frag_total, outcome,
                answer.complete if answer is not None else False,
                stage=stage, timeout_s=timeout_s,
                answer_version=answer.version if answer is not None else None,
                held=sorted(answer.held) if answer is not None and answer.held is not None else None,
            )

    async def _send_direct_with_attempts(
        self, target: str, frame_builder, peer_prefix: str,
        pkt_id: Optional[int] = None, frag_idx: Optional[int] = None, frag_total: Optional[int] = None,
        priority: int = PRIORITY_NORMAL, hop_count: Optional[int] = None, time_critical: bool = False,
        pass_number: Optional[int] = None,
        attempts_override: Optional[int] = None, record_result: bool = True,
        expires_at: Optional[float] = None, cancel_key: Optional[bytes] = None,
        expire_retries: bool = False, preempt: bool = False,
    ) -> bool:
        """docs/reliability_engine_design.md §4's "outer multi-attempt
        loop for a single DIRECT message" (`direct_send_attempts`,
        default 2 since 2026-09-16 -- see below) -- applies identically to a bare single-message DIRECT
        send and, per-fragment, to each fragment of a DIRECT-fragmented
        send. `frame_builder(attempt)` re-encodes the frame fresh for
        every attempt (`wire_format_design.md`'s own rule: never resend a
        byte-identical encoded string with the attempt patched in place)
        -- for the bare shape this returns identical bytes every time
        (correct: `_encode_direct_bare` carries no attempt byte of its
        own, per §4's "must NOT reimplement per-attempt content
        variation for a single non-fragmented DIRECT message" -- dedup-
        busting across these retries is instead the firmware's own job,
        driven by the `attempt` integer this method threads through to
        `commands.send_msg`'s own `attempt` parameter); for the
        multi-fragment shape this varies both this interface's own
        header attempt byte AND the same firmware parameter.

        Exactly **one** `record_direct_send_result` call per invocation
        -- success on the first real ACK, or failure once the whole
        attempt budget is exhausted -- never one per individual attempt
        (§4's own fix note: "count a fragment exhausting its own attempt
        budget as one failure... regardless of other fragments'
        outcomes"). A local `send_msg` failure proves nothing about the
        path itself (§8's gate) and is retried within the same budget,
        not counted toward it directly -- only the final exhausted-budget
        outcome is, via whichever `waited_full_timeout` the last attempt
        actually observed.

        **User-requested fix (2026-09-16):** the attempt budget itself is
        priority-dependent -- `direct_send_attempts_handshake` (default 4)
        for a `PRIORITY_HANDSHAKE` exchange, `direct_send_attempts`
        (default 2) for everything else. See `direct_send_attempts_
        handshake`'s own comment for why a failed Link handshake deserves
        more persistence than a failed DATA fragment, not less.

        `time_critical` (2026-09-18, see module docstring): True when this
        whole call is already racing a reassembly clock the receiver has
        running -- a continuation fragment (`frag_idx > 0`) or a pass-1
        re-drive, both decided by `_send_direct_fragmented`. Forwarded to
        `_send_direct_frame_and_wait_for_ack` OR'd with `attempt > 0` (an
        internal retry within this same call, time-critical for the same
        reason), so `_pre_transmit_gate` skips the incoming-quiet-defer
        courtesy wait that only a genuinely fresh send can afford.

        `pass_number` (2026-09-18, user-requested field-tuning data):
        capture-only, forwarded unchanged to `_capture_direct_attempt_
        result` -- see that method's own docstring."""
        # Step 3 (2026-09-18): `attempts_override` lets _send_direct_
        # fragmented's pass 0 send each fragment exactly once before the
        # reconcile query; `record_result=False` keeps those single
        # unconfirmed attempts out of record_direct_send_result's stale-
        # path failure count -- with a budget of 1, a lost *ACK* would
        # otherwise count as a full path failure, and the reconcile step
        # exists precisely because lost ACKs aren't path failures. Pass 1
        # (normal budget, results recorded) still feeds the threshold for
        # fragments the receiver confirmed it never got.
        attempts_budget = (
            attempts_override if attempts_override is not None
            else self.direct_send_attempts_handshake if priority == self.PRIORITY_HANDSHAKE
            else self.direct_send_attempts
        )
        waited_full_timeout = False
        # Phase 1 (2026-09-20): a send whose reply has been seen (an
        # LRPROOF for this LINKREQUEST, a PROOF for this bootstrap DATA)
        # has nothing left to retry for -- see _signal_send_answered.
        cancel_event = self._answered_send_event(cancel_key) if cancel_key is not None else None
        for attempt in range(attempts_budget):
            if self.detached or not self.online:
                return False
            if cancel_event is not None and cancel_event.is_set():
                self._debug(
                    f"DIRECT send to {peer_prefix!r} answered before attempt {attempt} -- "
                    f"{'not sent' if attempt == 0 else 'no retry'}; the far side already replied."
                )
                # One record, `ack_timeout_source="answered"`, so the
                # capture shows the retry that did NOT happen.
                self._capture_direct_attempt_result(
                    peer_prefix, attempt, True, self._direct_exchange_queue_depth, 0.0, None,
                    pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, hop_count=hop_count,
                    time_critical=time_critical, pass_number=pass_number,
                    ack_timeout_source="answered",
                )
                # Path evidence only when THIS peer delivered the reply
                # (review, 2026-09-20): a DIRECT-to-all copy cancelled by a
                # proof relayed through another peer, or a CHANNEL copy,
                # says nothing about this peer's path -- like an expiry,
                # nothing is recorded.
                if record_result and self._send_answered_by(cancel_key) == peer_prefix:
                    self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
                return True
            if (attempt == 0 or expire_retries) and self._expired(expires_at):
                # Field fix (2026-09-18 evening): outgoing_max_age. Not a
                # path failure (nothing was learned about the path), so no
                # record_direct_send_result call; counted as a drop once.
                # Attempt 0 only (page-load fix, same day): a retry is
                # committed air, never expired mid-way -- except a plain
                # PROOF (`expire_retries`, proof_max_age, 2026-09-20): one
                # bare frame, nothing already spent, and past the far
                # side's receipt deadline a retry is pure airtime.
                self._outgoing_dropped_total += 1
                RNS.log(
                    f"{self}: dropping DIRECT send to {peer_prefix!r}"
                    f"{f' (pkt_id={pkt_id} frag_idx={frag_idx}/{frag_total})' if pkt_id is not None else ''}"
                    f" -- packet expired ({'proof_max_age=%.0fs' % self.proof_max_age_s if expire_retries else 'outgoing_max_age=%.0fs' % self.outgoing_max_age_s}) before "
                    f"attempt {attempt} could transmit.",
                    RNS.LOG_WARNING,
                )
                if attempt > 0:
                    self._capture_direct_attempt_result(
                        peer_prefix, attempt, False, self._direct_exchange_queue_depth, 0.0, None,
                        pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, hop_count=hop_count,
                        time_critical=time_critical, pass_number=pass_number, ack_timeout_source="expired",
                    )
                return False
            frame = frame_builder(attempt)
            attempt_info: dict = {}
            try:
                ok, waited_full_timeout = await self._send_direct_frame_and_wait_for_ack(
                    target, frame, attempt, peer_prefix=peer_prefix,
                    pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, priority=priority,
                    hop_count=hop_count, time_critical=(time_critical or attempt > 0),
                    pass_number=pass_number, expires_at=expires_at, cancel_event=cancel_event,
                    expire_retries=expire_retries, attempt_info=attempt_info, preempt=preempt,
                )
            except Exception as exc:
                RNS.log(
                    f"{self}: DIRECT send to {peer_prefix!r} failed locally "
                    f"(attempt {attempt}): {exc}",
                    RNS.LOG_WARNING,
                )
                ok, waited_full_timeout = False, False
            if ok:
                if record_result and not (
                        cancel_event is not None and cancel_event.is_set()
                        and self._send_answered_by(cancel_key) != peer_prefix):
                    self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
                return True
            # No per-attempt delay here anymore -- the post-send listen
            # window (outcome-dependent range, 2026-09-16) fires inside
            # _send_direct_frame_and_wait_for_ack itself, before it releases
            # _direct_exchange_lock, so it already happened before control
            # returned here regardless of this attempt's outcome.
            if self.detached or not self.online:
                return False
            if attempt_info.get("expired"):
                # The attempt above expired while waiting for the lock and
                # was never transmitted (the ack-wait method's own check):
                # nothing to retry. Found while adding proof_max_age
                # (2026-09-20) -- before this, an attempt-0 expiry inside
                # the lock wait fell through to attempt 1, which transmitted
                # the expired packet after all.
                self._outgoing_dropped_total += 1
                return False

        if record_result:
            self.record_direct_send_result(peer_prefix, succeeded=False, waited_full_timeout=waited_full_timeout)
        return False

    # -- Measured ACK RTT (step 2, 2026-09-18 -- see module docstring) ----

    def _record_ack_rtt(self, peer_prefix: Optional[str], rtt_s: float) -> None:
        """One real ACK latency sample for `peer_prefix`, folded into the
        classic Jacobson/Karels estimator (srtt alpha 1/8, rttvar beta
        1/4). The first sample seeds srtt directly and rttvar at half of
        it, exactly as RFC 6298 does -- a deliberately generous initial
        spread so the timeout doesn't collapse onto one lucky sample."""
        self._rtt_sample(self._ack_rtt, peer_prefix, rtt_s, keep_last=True)
        st = self._ack_rtt.get(peer_prefix)
        if st is not None:
            st["backoff"] = 1.0  # a real ACK ends any miss backoff

    def _backoff_ack_rtt(self, peer_prefix: Optional[str], reason: str) -> None:
        """A miss under a measured timeout: keep the estimate, widen the
        next wait by `direct_ack_rtt_miss_backoff` (compounding per
        consecutive miss; `_adaptive_ack_timeout` still caps at the
        firmware value). Falls back to the old Karn discard when the
        factor is <= 1."""
        if peer_prefix is None:
            return
        if self.direct_ack_rtt_miss_backoff <= 1.0:
            self._invalidate_ack_rtt(peer_prefix, reason, keep_for_query=True)
            return
        st = self._ack_rtt.get(peer_prefix)
        if st is None:
            return
        st["backoff"] = st.get("backoff", 1.0) * self.direct_ack_rtt_miss_backoff
        self._ack_rtt_snapshot[peer_prefix] = st
        self._debug(
            f"ACK RTT estimate for {peer_prefix!r} kept ({reason}); next measured timeout "
            f"x{st['backoff']:.0f}, capped at the firmware value."
        )

    def _invalidate_ack_rtt(self, peer_prefix: Optional[str], reason: str, keep_for_query: bool = False) -> None:
        """Karn-style: drop everything measured for this peer. Called on a
        missed ACK that was governed by the measured timeout (the estimate
        may simply have been too tight -- go back to the firmware's guess
        until fresh samples say otherwise) and on any path change (a new
        path is a new link with its own RTT).

        Field fix (2026-09-18 evening): `keep_for_query=True` (the
        missed-ACK case) parks the discarded stats in `_ack_rtt_snapshot`
        so `_completion_query_timeout_s` can still size the reconcile
        QUERY that this very miss is about to trigger -- the drive-home
        capture's two reconciles both fell to the 5s floor at 3 hops for
        want of exactly these numbers. A path change (`keep_for_query`
        False) drops the snapshot, the echo timings and the last firmware
        bound too: none of them describe the new path."""
        if peer_prefix is None:
            return
        st = self._ack_rtt.pop(peer_prefix, None)
        if keep_for_query:
            if st is not None:
                self._ack_rtt_snapshot[peer_prefix] = st
        else:
            self._clear_peer_path_stats(peer_prefix, reason)
        if st is not None:
            self._debug(f"ACK RTT estimate for {peer_prefix!r} discarded ({reason}); firmware timeout applies until re-measured.")

    def _ack_timeout_cap_s(self, hop_count: Optional[int]) -> float:
        """Hop-aware ceiling on one ACK wait (field fix, 2026-09-19 -- see
        `direct_ack_timeout_base_s` for the evidence). `hop_count` None means
        "unknown", which gets the base allowance rather than a long wait."""
        hops = max(0, hop_count or 0)
        return min(
            self.direct_ack_timeout_base_s + self.direct_ack_timeout_per_hop_s * hops,
            self.direct_ack_timeout_routed_max_s,
        )

    def _adaptive_ack_timeout(self, peer_prefix: Optional[str], firmware_timeout_s: float) -> "tuple[float, str]":
        """Returns `(timeout_s, source)` for one ACK wait. `source` is
        "firmware" (the pre-step-2 value, unchanged) or "measured" when
        enough samples exist for this peer. The measured value is
        `multiplier * (srtt + 4*rttvar)`, floored at
        `direct_ack_rtt_min_timeout_s` and -- the invariant that keeps
        this safe to ship on zero-hop-only evidence -- never larger than
        the firmware-derived timeout it replaces."""
        st = self._ack_rtt.get(peer_prefix) if peer_prefix is not None else None
        if (
            not self.direct_ack_rtt_adaptive_enabled
            or st is None
            or st["samples"] < self.direct_ack_rtt_min_samples
        ):
            return firmware_timeout_s, "firmware"
        measured = self.direct_ack_rtt_timeout_multiplier * (st["srtt"] + 4.0 * st["rttvar"]) * st.get("backoff", 1.0)
        measured = max(self.direct_ack_rtt_min_timeout_s, measured)
        if measured >= firmware_timeout_s:
            return firmware_timeout_s, "firmware"
        return measured, "measured"

    def _record_echo(self, peer_prefix: Optional[str], hop_count: Optional[int], echo_s: float) -> None:
        """One measured repeater-echo time (our own frame heard forwarded
        by the first hop, seconds after MSG_SENT) for a multi-hop peer.
        Last 16 samples; cleared with the RTT stats on any path change."""
        if peer_prefix is None or hop_count is None or hop_count < 1 or echo_s <= 0:
            return
        samples = self._echo_stats.setdefault(peer_prefix, [])
        samples.append(echo_s)
        del samples[:-16]
        self._echo_stats_all.append(echo_s)
        del self._echo_stats_all[:-16]

    def _hop1_abort_deadline_s(
        self, peer_prefix: Optional[str], hop_count: Optional[int], timeout_s: float,
    ) -> Optional[float]:
        """Field fix (2026-09-18 evening, see direct_hop1_abort_enabled's
        own comment): how long to wait for either the ACK or the first
        hop's echo before calling the attempt dead, or None when the abort
        is not armed for this peer (disabled, zero/unknown hop count, too
        few echo samples, or a deadline that wouldn't be shorter than the
        ACK timeout anyway)."""
        if (
            not self.direct_hop1_abort_enabled
            or peer_prefix is None
            or hop_count is None or hop_count < 1
        ):
            return None
        samples = self._echo_stats.get(peer_prefix)
        if samples and len(samples) >= self.direct_hop1_abort_min_samples:
            deadline_s = max(self.direct_hop1_abort_min_s, self.direct_hop1_abort_echo_multiplier * max(samples))
        elif len(self._echo_stats_all) >= self.direct_hop1_abort_min_samples:
            # Second audit (2026-09-19 evening): this peer's own samples were
            # cleared by a path change or never existed, but the first hop
            # of ANY path is a repeater running the same forwarding delay --
            # the session-wide pool is the next best evidence.
            deadline_s = max(
                self.direct_hop1_abort_min_s,
                self.direct_hop1_abort_echo_multiplier * max(self._echo_stats_all),
            )
        elif self.direct_hop1_abort_default_s > 0:
            # Nothing measured yet (fresh process): the configured default,
            # sized at twice the largest echo ever seen in the field.
            deadline_s = max(self.direct_hop1_abort_min_s, self.direct_hop1_abort_default_s)
        else:
            return None
        if deadline_s >= timeout_s:
            return None
        return deadline_s

    _RX_LOG_WINDOW_FOREIGN_CAP = 20
    _RX_LOG_PAYLOAD_TYPE_TEXT_MSG = 2
    _RX_LOG_PAYLOAD_TYPE_PATH = 8

    async def _wait_for_ack_event(self, ack_filters: dict, timeout_s: float, cancel_event: "Optional[asyncio.Event]"):
        """`wait_for_event(ACK, ...)` raced against `cancel_event` (2026-09-20):
        returns `(ack_event_or_None, answered)`. The library's wait
        unsubscribes in its own `finally`, so cancelling it is clean."""
        if cancel_event is None:
            return await self._mc_ready.wait_for_event(
                self._EventType.ACK, attribute_filters=ack_filters, timeout=timeout_s,
            ), False
        if cancel_event.is_set():
            return None, True
        loop = asyncio.get_running_loop()
        ack_task = loop.create_task(self._mc_ready.wait_for_event(
            self._EventType.ACK, attribute_filters=ack_filters, timeout=timeout_s,
        ))
        cancel_task = loop.create_task(cancel_event.wait())
        try:
            done, _pending = await asyncio.wait({ack_task, cancel_task}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (ack_task, cancel_task):
                if not t.done():
                    t.cancel()
        if ack_task in done:
            return ack_task.result(), False
        # Let the cancelled ACK wait unsubscribe before the caller moves on.
        try:
            await ack_task
        except (asyncio.CancelledError, Exception):
            pass
        return None, True

    def _ack_preempt_floor_s(self, peer_prefix: Optional[str], hop_count: Optional[int]) -> float:
        """How long a best-effort ANSWER/REPORT's ACK wait runs before a
        queued handshake may cut it (phase 1, 2026-09-20): the peer's
        expected ACK time (srtt + rttvar when measured; ACKs at hop 0 were
        median 0.65 s / p90 1.65 s, at hop 2 median 2.7 s / p90 3.5 s in
        the 2026-09-20 session), else 2 s + 1 s per hop. Keying the
        handshake into the peer's ACK would lose both at a repeater."""
        st = self._ack_rtt.get(peer_prefix) if peer_prefix else None
        if st is not None:
            return max(0.5, st["srtt"] + st["rttvar"])
        return 2.0 + 1.0 * max(0, hop_count or 0)

    async def _await_direct_ack(
        self, sent, peer_prefix: Optional[str], hop_count: Optional[int], rx_window: dict, ack_wait_start: float,
        cancel_event: "Optional[asyncio.Event]" = None, preemptible: bool = False,
    ) -> "tuple[bool, bool, Optional[float], str, Optional[float], Optional[float]]":
        """The ACK wait for one transmitted DIRECT frame (refactor,
        2026-09-19: lifted verbatim out of `_send_direct_frame_and_wait_
        for_ack`, which had grown to 250 lines). Derives the timeout
        (firmware bound, then the step-2 measured estimate), arms the
        hop-1 abort, waits, and does the RTT bookkeeping. Returns
        `(ok, waited_full_timeout, ack_timeout_s, ack_timeout_source,
        ack_latency_s, hop1_abort_deadline_s)`."""
        ack_timeout_source = "none"
        ack_latency_s = None
        hop1_abort_deadline_s = None
        payload_dict = sent.payload if isinstance(sent.payload, dict) else {}
        expected_ack = payload_dict.get("expected_ack")
        if not expected_ack:
            ok, waited_full_timeout, ack_timeout_s = True, True, None
        else:
            rx_window["expected_ack"] = expected_ack.hex()
            suggested_timeout_ms = payload_dict.get("suggested_timeout", 10000)
            timeout_s = max((float(suggested_timeout_ms) / 1000.0) * 1.2, self.direct_ack_min_timeout_s)
            # §4's routed-mode ceiling -- this interface's dispatcher
            # never issues a DIRECT send without already believing a
            # resolved path exists, so it's always in the "routed"
            # regime from its own point of view; see
            # _configure_peer_discovery's comment on why the doc's
            # separate flood-mode ceiling has no code path here.
            # Field fix (2026-09-19): a hop-aware ceiling as well as the
            # absolute one -- see `direct_ack_timeout_base_s` for the measured
            # justification (the largest ACK that ever arrived in a
            # 1080-attempt session was 8.15s, while the firmware's own
            # suggestion produced waits up to 28s, and 20% of that session was
            # spent in waits that were never going to be answered while
            # holding the one shared radio lock).
            timeout_s = min(
                timeout_s, self._ack_timeout_cap_s(hop_count), self.direct_ack_timeout_routed_max_s,
            )
            if peer_prefix is not None:
                self._last_firmware_ack_timeout_s[peer_prefix] = timeout_s
            timeout_s, ack_timeout_source = self._adaptive_ack_timeout(peer_prefix, timeout_s)

            # Field fix (2026-09-18 evening): early abort on a
            # dead first hop -- see _hop1_abort_deadline_s. Wait
            # for the ACK only until the deadline; if by then
            # neither the ACK nor the first hop's echo of our
            # frame has been heard, the frame never left this
            # radio's neighbourhood and the rest of the timeout
            # buys nothing. If the echo WAS heard, the frame is
            # in the mesh: keep waiting the remainder as before.
            hop1_abort_deadline_s = self._hop1_abort_deadline_s(peer_prefix, hop_count, timeout_s)
            ack_filters = {"code": expected_ack.hex()}
            if preemptible:
                # Phase 1 (2026-09-20): a completion ANSWER/REPORT is best
                # effort and never retried; once the peer's expected ACK
                # time has passed, a queued Link handshake may take the
                # radio. "preempted": not a miss (no backoff, no listen,
                # no path evidence), not a success.
                floor_s = min(timeout_s, self._ack_preempt_floor_s(peer_prefix, hop_count))
                ack_event, answered = await self._wait_for_ack_event(ack_filters, floor_s, cancel_event)
                if ack_event is None and not answered and timeout_s > floor_s:
                    preempt_event = self._direct_exchange_lock.preempt_event()
                    if preempt_event.is_set():
                        return False, False, timeout_s, "preempted", None, None
                    ack_event, answered = await self._wait_for_ack_event(
                        ack_filters, timeout_s - floor_s, preempt_event,
                    )
                    if answered:
                        return False, False, timeout_s, "preempted", None, None
                ok = ack_event is not None
                if ok:
                    ack_latency_s = time.monotonic() - ack_wait_start
                    self._record_ack_rtt(peer_prefix, ack_latency_s)
                elif ack_timeout_source == "measured":
                    self._backoff_ack_rtt(peer_prefix, "missed ACK under measured timeout")
                return ok, ok or ack_timeout_source != "measured", timeout_s, ack_timeout_source, ack_latency_s, None
            first_wait_s = hop1_abort_deadline_s if hop1_abort_deadline_s is not None else timeout_s
            ack_event, answered = await self._wait_for_ack_event(ack_filters, first_wait_s, cancel_event)
            aborted = False
            if answered:
                # Phase 1 (2026-09-20): the reply this frame exists to elicit
                # arrived while its firmware ACK was still awaited -- the
                # exchange succeeded by any useful definition. Success with
                # no ACK latency (nothing to feed the estimator), no backoff,
                # and `waited_full_timeout` False (no evidence about the path
                # beyond the reply itself, which the receipt path recorded).
                return True, False, timeout_s, "answered", None, hop1_abort_deadline_s
            if ack_event is None and hop1_abort_deadline_s is not None:
                # Audit refinement (2026-09-19, field evidence):
                # the abort's premise -- and the reason
                # `direct_hop1_abort_enabled`'s own comment says
                # it counts as a real path failure, unlike a
                # plain timeout -- is "silence where a forward
                # was due". Traffic from the TARGET itself heard
                # during the wait is not silence: it means the
                # target was transmitting rather than listening,
                # so the path is demonstrably alive and the ACK
                # is merely late. One of the four aborts in
                # fieldtests/raw/binaryfieldtest was exactly
                # this (miss_diagnosis="target_busy"), and
                # aborting there both shortened a wait that
                # would likely have succeeded and charged a
                # failure against direct_path_reset_threshold on
                # a good path. Keep waiting the remainder
                # instead, as when our own echo was heard.
                target_hash = rx_window.get("target_hash_byte")
                target_was_talking = bool(target_hash) and any(
                    len(f) >= 5 and f[4] == target_hash
                    for f in rx_window.get("foreign_rx", ())
                )
                if rx_window["echo_seen_s"] is None and not target_was_talking:
                    aborted = True
                else:
                    if target_was_talking and rx_window["echo_seen_s"] is None:
                        self._debug(
                            f"hop-1 abort deadline reached for {peer_prefix!r} but the target "
                            f"itself was heard transmitting during the wait -- not silence, "
                            f"so waiting out the remaining ACK timeout instead of aborting."
                        )
                    ack_event, answered = await self._wait_for_ack_event(
                        ack_filters, max(0.01, timeout_s - first_wait_s), cancel_event,
                    )
                    if answered:
                        return True, False, timeout_s, "answered", None, hop1_abort_deadline_s
            ok, waited_full_timeout = ack_event is not None, True
            ack_timeout_s = hop1_abort_deadline_s if aborted else timeout_s
            if aborted:
                ack_timeout_source = "hop1_abort"
            if ok:
                ack_latency_s = time.monotonic() - ack_wait_start
                self._record_ack_rtt(peer_prefix, ack_latency_s)
            elif ack_timeout_source == "measured":
                # The measured estimate governed this wait and it
                # missed -- maybe the link slowed, maybe the estimate
                # was tight. 2026-09-19: widen the next measured wait
                # (RTO backoff) rather than discard the estimate and pay
                # the firmware's full timeout at once; see
                # direct_ack_rtt_miss_backoff's own comment.
                self._backoff_ack_rtt(peer_prefix, "missed ACK under measured timeout")
                # Code review (2026-09-18): a miss under a timeout
                # this interface tightened on its own is exactly
                # §8's "cut short by this engine's own ceiling"
                # case -- it proves nothing about the path and
                # must not count toward direct_path_reset_
                # threshold. The next attempt runs on the firmware
                # timeout (just invalidated above); a miss THERE
                # counts, so the pre-step-2 behaviour is really
                # the worst case, as step 2 promised.
                waited_full_timeout = False
        return ok, waited_full_timeout, ack_timeout_s, ack_timeout_source, ack_latency_s, hop1_abort_deadline_s

    def _post_attempt_listen_s(self, ok: bool, miss_diagnosis: Optional[str]) -> float:
        """How long to keep the radio lock after one attempt (refactor,
        2026-09-19: lifted out of `_send_direct_frame_and_wait_for_ack`):
        the small success range after an ACK; the step-4 hold model after
        a miss when `rx_log_holds_enabled`; else the flat miss range.
        See the 2026-09-16 outcome-split entry for why success and miss
        draw from different ranges."""
        if ok:
            listen_min_s, listen_max_s = (
                self.direct_post_send_listen_success_min_s, self.direct_post_send_listen_success_max_s,
            )
            listen_delay_s = random.uniform(listen_min_s, listen_max_s)
        elif self.rx_log_holds_enabled:
            listen_delay_s = self._post_miss_hold_s(miss_diagnosis or "no_info")
        else:
            listen_min_s, listen_max_s = (
                self.direct_post_send_listen_min_s, self.direct_post_send_listen_max_s,
            )
            listen_delay_s = random.uniform(listen_min_s, listen_max_s)
        return listen_delay_s

    async def _send_direct_frame_and_wait_for_ack(
        self, target: str, frame: str, attempt: int = 0,
        peer_prefix: Optional[str] = None,
        pkt_id: Optional[int] = None, frag_idx: Optional[int] = None, frag_total: Optional[int] = None,
        priority: int = PRIORITY_NORMAL,  # see _PriorityAsyncLock's own docstring
        hop_count: Optional[int] = None,  # capture-only, see _capture_direct_attempt_result's docstring
        time_critical: bool = False,  # forwarded to _send_direct_frame -> _pre_transmit_gate, see module docstring 2026-09-18
        pass_number: Optional[int] = None,  # capture-only, see _capture_direct_attempt_result's docstring
        kind: Optional[str] = None,  # capture-only: None for an "R" frame, "completion_answer" for a "Q" ANSWER (2026-09-18 review)
        expires_at: Optional[float] = None,  # outgoing_max_age deadline (2026-09-18 evening), see _expired
        quiet_wait: "Optional[asyncio.Future]" = None,  # field fix 2026-09-19 night, see the quiet-window block below
        quiet_window_s: Optional[float] = None,  # seconds after this frame's own transmit (MSG_SENT) the hold may last
        quiet_info: Optional[dict] = None,  # out-param: "hold_s", "ack_done_at", "answered_at" (see the quiet-window block)
        cancel_event: "Optional[asyncio.Event]" = None,  # set when the reply to this frame has been seen (2026-09-20, _signal_send_answered)
        expire_retries: bool = False,  # a plain PROOF: expires_at applies to every attempt, not only the first (proof_max_age, 2026-09-20)
        attempt_info: Optional[dict] = None,  # out-param: "expired" True when the attempt aged out in the lock wait and never transmitted
        preempt: bool = False,  # a Link handshake: may pre-empt an idle hold of the lock (phase 1, 2026-09-20)
        preemptible: bool = False,  # a best-effort ANSWER/REPORT: its own ACK wait may be cut for a queued handshake
    ) -> "tuple[bool, bool]":
        """Sends one already-encoded DIRECT `frame` string (bare or
        multi-fragment shape) to `target` (a MeshCore pubkey) and waits
        for the real delivery ACK the firmware sends back, correlated by
        the `expected_ack` code the MSG_SENT event itself returns -- the
        `meshcore` library's own per-request correlation mechanism for
        ACK specifically (confirmed directly against the installed
        library's `commands/messaging.py` `send_msg_with_retry`, which
        uses this exact `wait_for_event(EventType.ACK,
        attribute_filters={"code": ...})` pattern; also confirmed against
        `reader.py`'s own ACK-frame parsing, which populates that same
        `code` attribute). This is real per-request correlation, unlike
        the bare-type-only matching invariant #2 warns against for every
        other event type -- so, like `discover_path`'s own PATH_RESPONSE
        wait, this is deliberately NOT routed through `_command_lock`: it
        can legitimately take several seconds, and serializing it would
        stall every other outgoing command (CHANNEL sends, path
        discovery) for that whole wait. (The `send_msg` call itself
        still goes through `_run_command`/`_command_lock` via
        `_send_direct_frame`.)

        **Field-diagnosed fix (2026-09-15, real MeshChat traffic):**
        this whole send-then-wait-for-ACK sequence is now serialized via
        `_direct_exchange_lock`, a *separate* lock from `_command_lock`
        held for the full duration, not just the local radio command.
        Without it, two DIRECT sends spawned as independent background
        tasks (`_send_direct_packet`/`_send_direct_supplement` are both
        fire-and-forget by design -- see their own call sites in
        `_send_outgoing_packet`/`_send_path_request`) could each acquire
        `_command_lock` just long enough to queue their own frame, then
        both sit waiting for their own remote ACK *concurrently* --
        letting this node's radio transmit a second DIRECT frame while
        the first one's ACK is still in flight. That's exactly the
        half-duplex collision this design's own "DIRECT-fragmented send
        sequencing: strictly one fragment in flight at a time" invariant
        (reliability_engine_design.md's implementation-notes table) was
        meant to prevent -- but that invariant was only ever enforced
        *within* one `_send_direct_fragmented` call's own pass-0 loop,
        never *across* concurrently-spawned ones. Real MeshChat usage
        (several messages sent close together, each becoming its own
        multi-fragment DIRECT send) surfaced this directly: six separate
        pkt_ids all needing pass-1 re-drives in the same ~30s window,
        repeated "no real delivery ACK" warnings, and cascading stale-
        path resets on both sides -- the signature of concurrent DIRECT
        exchanges colliding with each other, not of a single slow link.
        Deliberately a single interface-wide lock, not one per peer:
        there is exactly one physical radio, so two DIRECT exchanges to
        *different* peers would collide on air exactly the same way.
        CHANNEL sends are unaffected (no ACK wait, so nothing to
        serialize beyond `_command_lock`'s own brief hold already
        covers), and this doesn't reintroduce the "stall unrelated
        commands" problem `_command_lock` was kept out of this wait for
        in the first place -- it only ever blocks a *second concurrent
        DIRECT send*, which needed to wait its turn on the shared radio
        regardless.

        Pure send-and-wait, no `record_direct_send_result` side effect --
        that's the caller's (`_send_direct_with_attempts`'s) job, exactly
        once per its own whole attempt budget, not once per individual
        attempt here. Returns `(ack_received, waited_full_timeout)`; a
        missing `expected_ack` (no ack correlation available at all) is
        treated as local success with nothing to wait for, matching
        Milestone 5's original behavior for that edge case.

        **User-requested observability addition (2026-09-15, post-alpha-0.1.0
        2-hop field test):** `peer_prefix`/`pkt_id`/`frag_idx`/`frag_total`
        are capture/debug-only (never affect routing) -- they let this
        one attempt's outcome be logged with enough context to reconstruct
        exactly which attempt, of which fragment, of which message,
        succeeded or failed, plus `_direct_exchange_lock`'s own live
        contention (`_direct_exchange_queue_depth`, and how long this
        attempt actually waited for its turn) -- see
        `_capture_direct_attempt_result`'s own docstring."""
        self._direct_exchange_queue_depth += 1
        wait_start = time.monotonic()
        preempted = False
        try:
            async with self._direct_exchange_lock(priority, preempt=preempt):
                lock_wait_s = time.monotonic() - wait_start
                queue_depth_at_acquire = self._direct_exchange_queue_depth
                if quiet_wait is not None and quiet_wait.done():
                    # Phase 1 (2026-09-20): the answer this frame asks for is
                    # already in (a late REPORT resolved the QUERY's future
                    # while the QUERY waited for the lock). Not transmitted;
                    # `_query_remote_fragments` reads the future.
                    self._capture_direct_attempt_result(
                        peer_prefix, attempt, True, self._direct_exchange_queue_depth, time.monotonic() - wait_start, None,
                        pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, hop_count=hop_count,
                        time_critical=time_critical, pass_number=pass_number,
                        ack_timeout_source="answered_before_send", kind=kind,
                    )
                    if quiet_info is not None:
                        quiet_info["answered_at"] = time.monotonic()
                        quiet_info["not_sent"] = True
                    return True, False
                if (attempt == 0 or expire_retries) and self._expired(expires_at):
                    # Field fix (2026-09-18 evening): the lock wait itself
                    # (225s in the drive-home capture) is where a queued
                    # packet most often ages out. Recorded, not transmitted;
                    # the caller's own pre-attempt check logs the drop.
                    if attempt_info is not None:
                        attempt_info["expired"] = True
                    self._capture_direct_attempt_result(
                        peer_prefix, attempt, False, queue_depth_at_acquire, lock_wait_s, None,
                        pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, hop_count=hop_count,
                        time_critical=time_critical, pass_number=pass_number,
                        ack_timeout_source="expired", kind=kind,
                    )
                    return False, False
                # Code-review fix: a local exception raised anywhere in this
                # block (e.g. _send_direct_frame surfacing a firmware ERROR
                # via _run_command) used to propagate straight out of this
                # `async with`, skipping the post-send listen-window below
                # entirely -- letting the very next contender for
                # _direct_exchange_lock (a retry of this same attempt, or a
                # different queued DIRECT exchange) key the radio again with
                # zero quiet time, exactly the back-to-back-transmission
                # problem the listen window exists to prevent everywhere
                # else. Caught here so the listen delay still runs (using
                # the same "something might have collided" range a missed
                # ACK draws from -- a local send failure is at least as
                # uncertain), then re-raised so the caller
                # (_send_direct_with_attempts) still sees and logs it
                # exactly as before.
                send_exc = None
                gate_telemetry: dict = {}
                # Step 2 (2026-09-18): measured ACK latency + what the radio
                # overheard during this attempt -- see _record_ack_rtt/
                # _adaptive_ack_timeout and _open_rx_log_window.
                ack_timeout_source = "none"
                ack_latency_s = None
                send_cmd_latency_s = None
                hop1_abort_deadline_s = None
                ack_done_at = None
                rx_window = self._open_rx_log_window(target)
                try:
                    sent = await self._send_direct_frame(
                        target, frame, attempt, time_critical=time_critical, gate_telemetry=gate_telemetry,
                        duty_cycle_exempt=self._duty_cycle_exempt(priority),
                    )
                    ack_wait_start = time.monotonic()
                    rx_window["tx_at"] = ack_wait_start
                    if self._last_own_tx_at is not None:
                        send_cmd_latency_s = ack_wait_start - self._last_own_tx_at

                    (ok, waited_full_timeout, ack_timeout_s, ack_timeout_source,
                     ack_latency_s, hop1_abort_deadline_s) = await self._await_direct_ack(
                        sent, peer_prefix, hop_count, rx_window, ack_wait_start, cancel_event=cancel_event,
                        preemptible=preemptible,
                    )
                    preempted = ack_timeout_source == "preempted"
                    ack_done_at = time.monotonic()
                except Exception as exc:
                    send_exc = exc
                    ok, waited_full_timeout, ack_timeout_s = False, False, None
                finally:
                    self._close_rx_log_window(rx_window)

                # User-requested fix (2026-09-15, generalized after a
                # second real 2-hop field test, then split by outcome
                # 2026-09-16 -- see direct_post_send_listen_success_min_s's
                # own comment for the real zero-hop field data that
                # motivated the split): every attempt, of every fragment,
                # still listens before this method returns, and still
                # *while holding* _direct_exchange_lock so it's a real
                # quiet window on the shared radio, not just a delay this
                # one caller happens to observe -- but which random range
                # it draws from now depends on whether this attempt was
                # actually ACKed. A missed ACK is a live "something might
                # have collided" signal, independent of this node's own
                # queue depth, so it still draws the full direct_post_
                # send_listen_min_s/max_s (0-5s default) range. A real
                # ACK is itself evidence the channel was clear for this
                # exchange, so it draws the much smaller direct_post_
                # send_listen_success_min_s/max_s (0-0.5s default) range
                # instead -- still genuinely random every time (never
                # skipped to a fixed value, so this can't settle into a
                # lockstep pattern with anything else on the channel),
                # still real spacing before the next contender for the
                # lock (a retry of this fragment, the next fragment, or a
                # different queued message) can go, just not the same
                # "assume something's wrong" cost a clean ACK gives no
                # reason to pay.
                # Step 4 (2026-09-18): the diagnosis is always computed and
                # captured; it only *chooses* the hold when rx_log_holds_
                # enabled. Otherwise the flat ranges above still apply.
                if rx_window["echo_seen_s"] is not None:
                    self._record_echo(peer_prefix, hop_count, rx_window["echo_seen_s"])
                miss_diagnosis = None if ok else self._diagnose_missed_ack(rx_window, hop_count)
                medium_busy_remaining_s = self._medium_busy_remaining_s()
                listen_delay_s = self._post_attempt_listen_s(ok, miss_diagnosis)
                if preempted:
                    # The handshake that cut this wait takes the radio now;
                    # the listen it would have had is theirs.
                    listen_delay_s = 0.0
                elif listen_delay_s > 0:
                    # Phase 1 (2026-09-20): the listen after a MISS yields to
                    # a queued handshake once the rx-log prediction of busy
                    # air (0 unless holds are on) has passed; the short
                    # success listen runs in full.
                    if ok:
                        await asyncio.sleep(listen_delay_s)
                    elif await self._idle_hold(listen_delay_s, floor_s=medium_busy_remaining_s):
                        listen_delay_s = time.monotonic() - (ack_done_at or time.monotonic())

                # Field fix (2026-09-19 night, `fieldtests/raw/Alpha0.1.2/
                # *nighttest*` vs `fieldtests/raw/binaryfieldtest/`): an
                # optional RADIO-QUIET WINDOW, held after the listen delay
                # and still under _direct_exchange_lock, until `quiet_wait`
                # resolves or `quiet_window_s` has elapsed since this frame's
                # MSG_SENT. Only the reconcile
                # QUERY uses it (see _query_remote_fragments): at one hop the
                # answering node is a hidden node, so the querier starting
                # its next raw burst the instant the QUERY is ACKed collides
                # with the ANSWER *at the repeater*, where neither radio's
                # listen-before-talk can see it. The night session lost 24
                # answers at one hop; the querier's own RX log shows 22 of
                # them were never decoded by its radio at all. Answer
                # delivery fell from 85% (e87cca8, which held the lock
                # through the whole answer wait) to 48% once commit 1919074
                # made that wait radio-free. This window is the narrow part
                # of that hold -- the seconds the answer is actually in the
                # chain -- and the rest of the budget is still waited with
                # the radio free, so a node's own ANSWERs cannot queue 50s
                # behind its waits the way the full hold made them.
                # asyncio.shield keeps the caller's future alive when this
                # wait_for times out: the answer may still arrive during the
                # radio-free remainder, and cancelling it here would discard
                # it. The window is measured from THIS frame's own transmit
                # (`ack_wait_start`, the MSG_SENT moment) rather than from
                # when the caller decided to send: under concurrent sends the
                # QUERY can wait 5-10s for this very lock first (the diagnostic
                # sim run showed exactly that), and a deadline fixed before
                # that wait would be spent before the frame ever left. Zero
                # hop is deliberately all but unaffected -- 1.5s from the
                # transmit is about the zero-hop ACK latency itself (1.45s
                # median in the night captures), so the hold there is a few
                # hundred milliseconds at most, and the firmware's own LBT
                # covers that case anyway (zero hop measured 96-100% answer
                # delivery in every build).
                # `quiet_info` (review fix, 2026-09-20) hands the caller what
                # it needs to keep its own accounting honest: the hold is
                # charged against the caller's answer budget (otherwise the
                # window would silently EXTEND the budget the field evidence
                # capped), and an answer that arrives inside the hold still
                # gets a round-trip sample measured from the ACK, as one
                # arriving after it would.
                quiet_hold_s = None
                # Review (2026-09-20): anchored at the ACK, and only after an
                # ACK -- a QUERY whose ACK never came has no answer worth
                # holding the radio for (its answer budget still runs radio-
                # free), and anchoring at the transmit spent the whole window
                # on the ACK's own round trip (see direct_completion_quiet_base_s).
                if (quiet_wait is not None and quiet_window_s is not None and send_exc is None
                        and ok and ack_done_at is not None):
                    if quiet_info is not None:
                        quiet_info["ack_done_at"] = ack_done_at
                    quiet_remaining_s = ack_done_at + quiet_window_s - time.monotonic()
                    if quiet_remaining_s > 0:
                        quiet_started = time.monotonic()
                        # Phase 1 (2026-09-20): a queued Link handshake ends
                        # the hold early; the caller keeps waiting for the
                        # answer with the radio free, as it does after the
                        # window.
                        answered, cut = await self._wait_future_or_preempt(quiet_wait, quiet_remaining_s)
                        if answered and quiet_info is not None:
                            quiet_info["answered_at"] = time.monotonic()
                        if cut:
                            self._debug(
                                f"quiet window for {peer_prefix!r} cut at {time.monotonic() - quiet_started:.2f}s "
                                f"of {quiet_remaining_s:.2f}s -- a Link handshake is waiting for the radio."
                            )
                        quiet_hold_s = time.monotonic() - quiet_started
                    if quiet_info is not None:
                        quiet_info["hold_s"] = quiet_hold_s or 0.0

                self._debug(
                    f"DIRECT attempt={attempt} to {peer_prefix!r} "
                    f"(pkt_id={pkt_id} frag_idx={frag_idx}/{frag_total}): ok={ok} "
                    f"queue_depth={queue_depth_at_acquire} lock_wait={lock_wait_s:.2f}s "
                    f"ack_timeout={ack_timeout_s} ({ack_timeout_source}) hop1_abort_deadline={hop1_abort_deadline_s} "
                    f"ack_latency={ack_latency_s if ack_latency_s is None else round(ack_latency_s, 3)}s "
                    f"listen_delay={listen_delay_s:.2f}s "
                    f"rx_window: echo={rx_window['echo_seen_s']} ack_on_air={rx_window['ack_seen_on_air_s']} "
                    f"path_reply={rx_window['path_reply_seen_s']} foreign={rx_window['foreign_rx_count']} "
                    f"miss_diagnosis={miss_diagnosis} medium_busy_remaining={medium_busy_remaining_s:.2f}s "
                    f"medium_hold_wait={gate_telemetry.get('medium_hold_wait_s')} "
                    f"quiet_hold={quiet_hold_s if quiet_hold_s is None else round(quiet_hold_s, 2)}"
                    + (f" (local send exception: {send_exc})" if send_exc is not None else "") + "."
                )
                self._capture_direct_attempt_result(
                    peer_prefix, attempt, ok, queue_depth_at_acquire, lock_wait_s, ack_timeout_s,
                    pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, listen_delay_s=listen_delay_s,
                    hop_count=hop_count, time_critical=time_critical, pass_number=pass_number,
                    quiet_defer_wait_s=gate_telemetry.get("quiet_defer_wait_s"),
                    duty_cycle_wait_s=gate_telemetry.get("duty_cycle_wait_s"),
                    ack_timeout_source=ack_timeout_source, ack_latency_s=ack_latency_s,
                    send_cmd_latency_s=send_cmd_latency_s, rx_window=rx_window,
                    medium_hold_wait_s=gate_telemetry.get("medium_hold_wait_s"),
                    miss_diagnosis=miss_diagnosis, medium_busy_remaining_s=medium_busy_remaining_s,
                    kind=kind, hop1_abort_deadline_s=hop1_abort_deadline_s,
                    duty_cycle_exempt=bool(gate_telemetry.get("duty_cycle_exempt", False)),
                    quiet_hold_s=quiet_hold_s,
                    on_air_bytes=(self._text_frame_on_air_bytes(frame, hop_count or 0)
                                  if send_exc is None else None),
                )
                if send_exc is not None:
                    # Listened out the quiet window above first, same as any
                    # other failed attempt; now let the caller
                    # (_send_direct_with_attempts) see and log this exactly
                    # as it did before this fix.
                    raise send_exc
                return ok, waited_full_timeout
        finally:
            self._direct_exchange_queue_depth -= 1

    def _resolve_routing_peer(self, header: _RnsHeader) -> Optional[str]:
        """docs/routing_decisions.md's "resolved path known" lookup,
        peer-attribution half: maps an outgoing packet's own
        destination-hash field to a bound peer via the opportunistic
        RNS-token tables §7 populates (peer_discovery_design.md). A *bare*
        PROOF needs the separate short-TTL correlation table -- its own
        destination-hash field is then the truncated hash of the packet it
        proves, not a stable per-peer identity (§7's "PROOF exception") --
        but a proof carried on an established Link puts the link_id there
        instead, which the token table already knows, so that table is
        consulted first for every PROOF context (audit fix, 2026-09-19).

        Resolved gap (code review, 2026-09-18): for an outgoing LRPROOF
        (`context == RNS.Packet.LRPROOF`, answering a peer's LINKREQUEST),
        `RNS.Packet.pack()` writes the *link_id* into this same on-wire
        field, not a destination hash, so it was never found in
        `_proof_correlation`'s truncated-hash keyspace and always fell
        through to broadcast+supplement even for a known, DIRECT-resolved
        peer. `_observe_incoming_rns_packet` records `link_id -> peer` in
        `_rns_token_peer` for every LINKREQUEST received DIRECT (via
        `_compute_link_id`, validated in-process against
        `RNS.Link.link_id_from_lr_packet`). The 2026-09-19 audit found the
        same gap still open for every *other* Link-carried proof
        (RESOURCE_PRF above all), so the lookup below is no longer
        context-specific."""
        if header.destination_hash is None:
            return None
        if header.packet_type == RNS.Packet.PROOF:
            # Audit fix (2026-09-19, field evidence): the LRPROOF special
            # case below was the same gap, found and fixed one context at a
            # time. ANY proof carried on an established Link puts the
            # *link_id* in this on-wire field, not the truncated hash of the
            # proved packet -- and `_observe_incoming_rns_packet` already
            # records link_id -> peer for every LINKREQUEST received DIRECT.
            # In fieldtests/raw/binaryfieldtest the same link_id was routed
            # `direct_primary` for 54 DATA packets and `small_mesh_direct_
            # all_unknown_dest` for its RESOURCE_PRF, because only LRPROOF
            # consulted the table. That mattered twice over: a RESOURCE_PRF
            # is the sender's only transfer-complete signal, and each
            # misroute also charged `_record_unknown_dest_attempt` against
            # the live Link's id, so three of them armed a 300s backoff that
            # drops the proof outright in small-mesh mode. The two keyspaces
            # cannot collide: a genuine bare-proof truncated hash is never a
            # key in `_rns_token_peer`, so falling through is unchanged.
            token_peer = self._rns_token_peer.get(header.destination_hash)
            if token_peer is not None:
                return token_peer
            entry = self._proof_correlation.get(header.destination_hash)
            if entry is None:
                return None
            peer_prefix, expiry = entry
            if time.monotonic() >= expiry:
                del self._proof_correlation[header.destination_hash]
                return None
            return peer_prefix
        return self._rns_token_peer.get(header.destination_hash)

    async def _delayed_retry_pass(
        self, data: bytes, pkt_id: int, attempt: int, expires_at: Optional[float] = None,
        duty_cycle_exempt: bool = False,
    ) -> None:
        delay = random.uniform(self.retransmit_jitter_min_s, self.retransmit_jitter_max_s)
        await asyncio.sleep(delay)
        if self.detached or not self.online:
            return
        if self._expired(expires_at):
            self._debug(f"CHANNEL retry pass (attempt={attempt}) for pkt_id={pkt_id} skipped -- packet expired.")
            return
        self._debug(
            f"CHANNEL retry pass (attempt={attempt}) for pkt_id={pkt_id} "
            f"firing after {delay:.1f}s jitter."
        )
        await self._send_channel_pass(data, pkt_id, attempt, duty_cycle_exempt=duty_cycle_exempt)

    async def _send_channel_pass(
        self, data: bytes, pkt_id: int, attempt: int, duty_cycle_exempt: bool = False,
    ) -> None:
        """One full CHANNEL send pass for `data` under `pkt_id`, at a
        given `attempt` number -- re-fragments from scratch every time
        (§1), even though the fast-path-vs-multi-fragment shape decision
        itself doesn't depend on `attempt`; re-deriving it fresh per pass
        costs nothing and keeps this the single place that decides it."""
        fastpath_budget = self._channel_payload_budget()
        if len(data) <= fastpath_budget:
            await self._send_channel_fastpath_frame(data, pkt_id, attempt, duty_cycle_exempt)
            return

        # Per wire_format_design.md's "constraint one," ANNOUNCE is the
        # only RNS packet type that structurally needs this path today --
        # everything else comfortably fits the fast-path budget above.
        per_fragment_budget = self._channel_multifragment_payload_budget()
        max_total_payload = per_fragment_budget * 255  # frag_total is a 1-byte field
        if per_fragment_budget <= 0 or len(data) > max_total_payload:
            self._outgoing_dropped_total += 1
            RNS.log(
                f"{self}: dropping outgoing packet ({len(data)} bytes, "
                f"pkt_id={pkt_id} attempt={attempt}) -- exceeds even the "
                f"fully-fragmented CHANNEL budget ({max_total_payload} "
                f"bytes across 255 fragments at {per_fragment_budget} "
                f"bytes each); no larger transport is available for "
                f"CHANNEL traffic.",
                RNS.LOG_WARNING,
            )
            return

        await self._send_channel_multifragment_pass(data, pkt_id, attempt, duty_cycle_exempt)

    async def _send_channel_fastpath_frame(
        self, payload: bytes, pkt_id: int, attempt: int, duty_cycle_exempt: bool = False,
    ) -> None:
        frame = self._encode_channel_fastpath(payload, pkt_id, attempt)
        await self._pre_transmit_gate(frame, duty_cycle_exempt=duty_cycle_exempt)
        try:
            await self._run_command(
                self._mc_ready.commands.send_chan_msg(self.channel_idx, frame),
                "send_chan_msg",
                self._EventType.OK,
            )
        except Exception as exc:
            self._outgoing_dropped_total += 1
            RNS.log(
                f"{self}: CHANNEL send failed (pkt_id={pkt_id} "
                f"attempt={attempt}, {len(payload)}-byte payload): {exc}",
                RNS.LOG_WARNING,
            )
            return
        self.txb += len(frame)
        # 2026-09-20 (airtime pass): the single-frame CHANNEL send wrote no
        # transmit record at all, so a capture could not total this node's
        # air. One `channel_fragment_sent` with frag 0/1, like the others.
        self._capture_channel_fragment_sent(
            pkt_id, attempt, 0, 1, 0, ok=True, size_bytes=len(frame),
            on_air_bytes=self._text_frame_on_air_bytes(self._own_node_name + ": " + frame),
        )
        self._debug(
            f"CHANNEL send OK: pkt_id={pkt_id} attempt={attempt} "
            f"{len(payload)}-byte payload ({len(frame)} chars on wire)."
        )

    async def _send_channel_multifragment_pass(
        self, payload: bytes, pkt_id: int, attempt: int, duty_cycle_exempt: bool = False,
    ) -> None:
        """One CHANNEL retry pass (docs/reliability_engine_design.md §1-2):
        re-fragments `payload` fresh, sends every fragment (in a shuffled
        order by default) with independently-drawn inter-fragment
        spacing, all under the same `pkt_id` at the given `attempt`."""
        chunks = self._fragment_payload(payload)
        frag_total = len(chunks)

        order = list(range(frag_total))
        if self.fragment_order_shuffle:
            random.shuffle(order)

        # Milestone 2 has no live hop-count data source (module docstring)
        # -- always resolves to the flat unknown-multi-hop tier for now.
        spacing_min, spacing_max = self._fragment_spacing_range(hop_count=None)

        self._debug(
            f"CHANNEL multi-fragment send: pkt_id={pkt_id} attempt={attempt} "
            f"frag_total={frag_total} order={order} "
            f"spacing=[{spacing_min:.1f},{spacing_max:.1f})s"
        )

        for position, frag_idx in enumerate(order):
            if self.detached or not self.online:
                # Interface was detached, or the underlying MeshCore
                # connection dropped (_on_mc_disconnected) mid-send --
                # stop rather than burn through the remaining fragments
                # each individually failing against a dead connection.
                return
            frame = self._encode_channel_multifragment(
                chunks[frag_idx], pkt_id, frag_idx, frag_total, attempt
            )
            await self._pre_transmit_gate(frame, duty_cycle_exempt=duty_cycle_exempt)
            try:
                await self._run_command(
                    self._mc_ready.commands.send_chan_msg(self.channel_idx, frame),
                    "send_chan_msg",
                    self._EventType.OK,
                )
                self.txb += len(frame)
                self._capture_channel_fragment_sent(
                    pkt_id, attempt, frag_idx, frag_total, position, ok=True, size_bytes=len(frame),
                    on_air_bytes=self._text_frame_on_air_bytes(self._own_node_name + ": " + frame),
                )
            except Exception as exc:
                # CHANNEL is blind/unacknowledged (§0) -- a failure here
                # means the local radio itself rejected/errored the
                # command, not that the fragment wasn't heard over the
                # air. Still attempt the rest of the set: one local
                # command failure doesn't mean the others will also fail,
                # and a partial fragment set is still useful (§1's
                # union-of-passes reasoning applies within a pass too).
                self._outgoing_dropped_total += 1
                self._capture_channel_fragment_sent(
                    pkt_id, attempt, frag_idx, frag_total, position, ok=False, size_bytes=len(frame),
                )
                RNS.log(
                    f"{self}: CHANNEL fragment send failed (pkt_id={pkt_id} "
                    f"attempt={attempt} frag_idx={frag_idx}/{frag_total}): {exc}",
                    RNS.LOG_WARNING,
                )

            if position < frag_total - 1:
                await asyncio.sleep(random.uniform(spacing_min, spacing_max))

    async def _send_direct(self, target, payload: bytes):
        """A bare (fits-in-one-message) DIRECT send making exactly one
        attempt. Note this method itself is NOT on the real send path as
        of Milestone 6: `_send_outgoing_packet` drives DIRECT sends
        through `_send_direct_with_attempts`, whose frame_builder callback
        calls `_encode_direct_bare`/`_send_direct_frame_and_wait_for_ack`
        directly rather than going through this wrapper (it needs the
        ACK-correlation and outer-retry behavior those add, which this
        method doesn't have). `_send_direct` has been kept, unwired, since
        Milestone 1 as a minimal bare-encode-and-send primitive with its
        own direct unit tests (see test_smart_meshcore_interface_send_
        receive.py's "currently unwired DIRECT primitive" section);
        `_send_direct_frame`, which it delegates to, IS shared by the
        real send path. Returns the MSG_SENT event so a caller can read
        its `expected_ack`/`suggested_timeout`/`type` fields, raises on
        failure."""
        frame = self._encode_direct_bare(payload)
        return await self._send_direct_frame(target, frame)

    async def _send_direct_frame(
        self, target, frame: str, attempt: int = 0, time_critical: bool = False,
        gate_telemetry: Optional[dict] = None, duty_cycle_exempt: bool = False,
    ):
        """Sends one already-encoded DIRECT frame string (bare or
        multi-fragment shape -- this method doesn't care which) via
        `send_msg`, the one place either shape actually reaches the
        radio. `attempt` is forwarded to the `meshcore` library's own
        `send_msg(..., attempt=...)` parameter -- the firmware's own
        per-attempt content-derived ACK/dedup-busting mechanism
        (`wire_format_design.md`'s DIRECT section), meaningful on its own
        for the bare shape (whose own encoding never varies by attempt,
        per design) and redundant-but-harmless alongside this interface's
        own header attempt byte for the multi-fragment shape. Called from
        within `_send_direct_frame_and_wait_for_ack`'s own `_direct_
        exchange_lock` hold, so `_throttle_for_duty_cycle`'s wait (if any)
        correctly blocks that lock for the duration too -- nothing else
        should be transmitting during it either, for the same reason
        nothing else should be transmitting during the post-send listen
        window that same caller already enforces.

        `time_critical` (2026-09-18, see module docstring) is forwarded to
        `_pre_transmit_gate` as `skip_quiet_defer` -- see that method's own
        docstring for why a send that's already racing the receiver's
        reassembly clock shouldn't pay the incoming-quiet-defer cost only a
        genuinely fresh send can afford.

        `gate_telemetry` (2026-09-18, user-requested field-tuning data): if
        given a dict, it's filled in-place with `quiet_defer_wait_s`/
        `duty_cycle_wait_s` from `_pre_transmit_gate`'s return -- an out-
        param rather than widening this method's own return value, since
        only `_send_direct_frame_and_wait_for_ack` (for capture) needs it;
        the other callers (`_send_direct`, `_query_remote_fragments`)
        simply don't pass one and see no change. Filled unconditionally, even if `_run_command`
        below then raises -- the gate already ran and cost real time
        either way, and that's exactly the case a field-tuning analysis
        most wants visible."""
        quiet_defer_wait_s, duty_cycle_wait_s, medium_hold_wait_s = await self._pre_transmit_gate(
            frame, skip_quiet_defer=time_critical, duty_cycle_exempt=duty_cycle_exempt,
        )
        if gate_telemetry is not None:
            gate_telemetry["duty_cycle_exempt"] = duty_cycle_exempt
            gate_telemetry["quiet_defer_wait_s"] = quiet_defer_wait_s
            gate_telemetry["duty_cycle_wait_s"] = duty_cycle_wait_s
            gate_telemetry["medium_hold_wait_s"] = medium_hold_wait_s
        result = await self._run_command(
            self._mc_ready.commands.send_msg(target, frame, attempt=attempt),
            "send_msg",
            self._EventType.MSG_SENT,
        )
        self.txb += len(frame)
        return result

    # -------------------------------------------------------------------
    # Path discovery (docs/path_discovery_spec.md) -- Milestone 4
    # -------------------------------------------------------------------

    def _resolve_contact(self, pubkey_prefix: str):
        if self._mc is None:
            return None
        return self._mc.get_contact_by_key_prefix(pubkey_prefix)

    async def _contact_refresh_loop(self):
        """A live, periodic contact-table read -- never served from a
        cached/inherited value (reliability_engine_design.md §2's
        "data-source gap" fix, reinstating a real mechanism an earlier
        draft of this design set dropped). Feeds path discovery's own
        ensure_contacts() precondition; Milestone 5 is expected to also
        feed this same freshness into the zero-hop/known-N-hop spacing
        tiers, which have no live data source yet.

        Code-review fix: also retries `_fetch_own_identity()` here
        whenever `_own_pubkey_hex` is still empty. `_fetch_own_identity`'s
        own docstring says a failed initial fetch "will be retried on the
        next reconnect if the pubkey is still unknown by then" -- but
        `_on_mc_connected` only ever fires that retry on an actual
        DISCONNECTED-then-CONNECTED cycle. If the physical link comes up,
        the very first `send_appstart` fails, and the link then simply
        stays up for the rest of the process's life (no further CONNECTED
        events), that retry path never runs and `_own_pubkey_prefix()`
        stays `None` forever -- permanently disabling the self-echo guard
        in `_handle_incoming_bind_frame`, so this node's own bind frames
        bouncing back via a repeater or CHANNEL rebroadcast would be
        misprocessed as a genuine external peer for the rest of the
        session. This loop already runs periodically regardless of
        connection-state transitions, so it's a natural place to keep
        retrying until it finally succeeds."""
        try:
            while not self.detached:
                await asyncio.sleep(self._loop_interval_s(self.contact_refresh_interval_s, "contact_refresh_interval"))
                if self.detached:
                    break
                if not self._own_pubkey_hex:
                    self._spawn_background_task(self._fetch_own_identity())
                try:
                    await self._refresh_contacts_and_grant_telemetry()
                except Exception as exc:
                    RNS.log(f"{self}: periodic contact refresh failed: {exc}", RNS.LOG_WARNING)
        except asyncio.CancelledError:
            pass

    async def _refresh_contacts_and_grant_telemetry(self) -> None:
        """Milestone 5 tightens this to docs/peer_discovery_design.md
        §5's actual recommendation: grant base telemetry only to peers
        confirmed via this interface's own bind-frame protocol, not every
        known MeshCore contact -- superseding Milestone 4's
        `telemetry_grant_all_contacts`-default-on simplification, which
        was explicitly flagged there as a placeholder for exactly this.
        A contact not yet resolvable for an already-bound peer (§4's
        "progressively filled in" model -- bind frames and native contact
        adverts are independent floods with no ordering guarantee) is
        simply skipped this pass; the next periodic refresh retries it.
        `telemetry_grant_all_contacts` (default now off) is kept as an
        explicit escape hatch back to the old open-to-every-contact
        behavior, not removed -- a config value that stops being the
        default doesn't stop being real config.

        Code-review fix: `ensure_contacts()` is routed through
        `self._command_lock` here even though it isn't wrapped in
        `_run_command` (its return is a bool, not an Event with a `.type`
        to check -- `_run_command`'s contract doesn't fit it). Left
        unlocked, its internal `get_contacts()` waits on the library's
        shared ERROR/NEXT_CONTACT/CONTACTS events with no per-request
        correlation id (invariant #2) while it could run fully concurrently
        with any `_run_command`-guarded call elsewhere -- e.g. a
        concurrent `change_contact_flags` failing could fire an ERROR
        event this call's dangling wait_for_event(ERROR) would catch
        instead, misattributing an unrelated failure as a contacts-fetch
        failure. This command is quick and only fires at all when contacts
        haven't been fetched yet, so holding the lock across it doesn't
        create the multi-second stall `discover_path`'s own docstring
        warns `send_path_discovery_sync` would cause if it were
        similarly wrapped.

        Code-review fix: this used to call `ensure_contacts()` with no
        arguments. The installed `meshcore` library's `ensure_contacts(self,
        follow=False)` only re-fetches when `not self._contacts` OR
        `(follow and self._contacts_dirty)` -- with the default `follow=
        False`, every call after the very first successful fetch was a
        permanent no-op, even though the library already tracks
        `_contacts_dirty=True` internally on every ADVERTISEMENT/
        PATH_UPDATE event. That directly contradicted this method's own
        "the next periodic refresh retries it" docstring claim above: a
        peer's contact that arrived after this node's first contact fetch
        would never actually be pulled into `self._contacts` by any later
        periodic refresh. Passing `follow=True` here makes this call
        actually consult that dirty flag."""
        async with self._command_lock:
            await self._mc_ready.ensure_contacts(follow=True)
        for peer in list(self._peers.values()):
            contact = self._resolve_contact(peer.pubkey_prefix)
            if contact is not None:
                await self._grant_telemetry_permission_if_needed(contact)
        if self.telemetry_grant_all_contacts:
            for contact in list(self._mc_ready.contacts.values()):
                await self._grant_telemetry_permission_if_needed(contact)

    async def _grant_telemetry_permission_if_needed(self, contact) -> None:
        """docs/path_discovery_spec.md's telemetry-permission section:
        base telemetry (and so path discoverability) is gated per-contact
        via bit 0x02 of that contact's own `flags` field, as stored in
        THIS node's local contact table (confirmed directly against
        examples/companion_radio/MyMesh.cpp's onContactRequest --
        `cp = contact.flags >> 1`, checked against `TELEM_PERM_BASE`).
        Granting is therefore a local action this node takes per peer, not
        a message that reaches out to the peer. As of Milestone 5, called
        only for peers confirmed via this interface's own bind-frame
        protocol (peer_discovery_design.md §5) by default -- see
        `_refresh_contacts_and_grant_telemetry` and `_register_peer`.

        Code-review fix: the `flags` read and bitwise check below used to
        sit outside the `try` block that follows. `_refresh_contacts_and_
        grant_telemetry` calls this once per bound peer/contact in a plain
        `for` loop with no per-iteration isolation -- an unguarded
        `TypeError` here (e.g. a contact whose `flags` field is ever
        `None` or otherwise non-int) would abort that whole loop, silently
        skipping the telemetry-permission grant/refresh for every peer
        ordered after the offending one, with only a generic "contact
        refresh failed" line two frames up to show for it. Moved inside
        the `try` so one malformed contact can't take out every other
        peer's refresh in the same pass."""
        try:
            current_flags = contact.get("flags", 0)
            if current_flags & self.TELEM_PERM_BASE_FLAG_BIT:
                return  # already granted
            new_flags = current_flags | self.TELEM_PERM_BASE_FLAG_BIT
            await self._run_command(
                self._mc_ready.commands.change_contact_flags(contact, new_flags),
                "change_contact_flags",
                self._EventType.OK,
            )
            self._debug(
                f"granted base telemetry permission to contact "
                f"{contact.get('adv_name', '?')!r} ({contact.get('public_key', '?')[:12]}...)."
            )
        except Exception as exc:
            RNS.log(
                f"{self}: failed to grant telemetry permission to contact "
                f"{contact.get('adv_name', '?')!r}: {exc}",
                RNS.LOG_WARNING,
            )

    def _path_discovery_in_backoff(self, pubkey_prefix: str) -> bool:
        until = self._path_discovery_backoff_until.get(pubkey_prefix)
        return until is not None and time.monotonic() < until

    def _record_path_discovery_success(self, pubkey_prefix: str) -> None:
        self._path_discovery_failures.pop(pubkey_prefix, None)
        self._path_discovery_backoff_until.pop(pubkey_prefix, None)

    def _record_path_discovery_failure_round(self, pubkey_prefix: str) -> None:
        failures = self._path_discovery_failures.get(pubkey_prefix, 0) + 1
        self._path_discovery_failures[pubkey_prefix] = failures
        cooldown = min(
            self.path_discovery_base_cooldown_s * (self.path_discovery_backoff_factor ** (failures - 1)),
            self.path_discovery_max_cooldown_s,
        )
        self._path_discovery_backoff_until[pubkey_prefix] = time.monotonic() + cooldown
        RNS.log(
            f"{self}: path discovery to {pubkey_prefix!r} failed "
            f"({failures} consecutive round(s) of {self.path_discovery_quick_attempts} "
            f"quick attempt(s) each) -- backing off {cooldown:.0f}s.",
            RNS.LOG_WARNING,
        )

    async def discover_path(self, pubkey_prefix: str) -> Optional[_ResolvedPath]:
        """docs/path_discovery_spec.md's discover_path() function-level
        spec. Not yet called from anywhere in the automatic send path --
        Milestone 5's routing decisions need to exist first to have a
        DIRECT send that wants a path at all. Exists now, unit-tested
        directly against fakes, as the primitive that logic will call.

        Deliberately does NOT route send_path_discovery_sync() through
        _run_command()/self._command_lock: the library call already
        manages its own internal concurrency (a dedicated
        `_mesh_request_lock`) across its send-then-decoupled-wait-for-
        PATH_RESPONSE shape, which can legitimately take several seconds.
        Serializing it behind this interface's own single command lock
        too would stall every other outgoing command (CHANNEL sends,
        retries) for that whole wait -- exactly what §3's "independent
        queues, one slow operation shouldn't block everything else"
        design principle argues against. Invariant #1 (check the actual
        result, don't trust a bare non-None return) is still applied
        manually below, just without that lock."""
        if self._mc is None or not self.online:
            return None

        if self._path_discovery_in_backoff(pubkey_prefix):
            # Precondition fix from the logical review
            # (path_discovery_spec.md's own function-level spec): a
            # caller retrying this peer while it's still in backoff must
            # not fire a fresh quick-attempts burst and so contribute
            # another failed round to the same schedule it's already
            # respecting. No transmission at all in this case.
            self._debug(f"discover_path({pubkey_prefix!r}): target is in backoff cooldown -- skipping.")
            return None

        contact = self._resolve_contact(pubkey_prefix)
        if contact is None:
            # Code-review fix: locked for the same reason
            # `_refresh_contacts_and_grant_telemetry`'s own `ensure_contacts()`
            # call is (see that method's docstring) -- unlocked, this could
            # race any `_run_command`-guarded call elsewhere and steal its
            # ERROR event via the library's own type-only correlation
            # (invariant #2). Quick and only fires when contacts are
            # unresolved, so this doesn't create the stall
            # `send_path_discovery_sync` below is deliberately kept out of
            # the lock to avoid. Code-review fix: `follow=True`, same
            # reasoning as `_refresh_contacts_and_grant_telemetry`'s own
            # call -- without it, this fallback refresh is a permanent
            # no-op for any contact that arrived after the first fetch,
            # since the library's default `follow=False` never consults
            # its own `_contacts_dirty` flag.
            try:
                async with self._command_lock:
                    await self._mc_ready.ensure_contacts(follow=True)
            except Exception as exc:
                self._debug(f"discover_path({pubkey_prefix!r}): contact refresh failed: {exc}")
            contact = self._resolve_contact(pubkey_prefix)
        if contact is None:
            RNS.log(
                f"{self}: discover_path({pubkey_prefix!r}): peer is not a "
                f"known contact -- cannot discover a path to it.",
                RNS.LOG_WARNING,
            )
            return None

        for attempt in range(1, self.path_discovery_quick_attempts + 1):
            if self.detached or not self.online:
                return None

            try:
                result = await self._mc_ready.commands.send_path_discovery_sync(contact)
            except Exception as exc:
                self._debug(f"discover_path({pubkey_prefix!r}) attempt {attempt}: {exc}")
                result = None

            if result is not None and result.type == self._EventType.PATH_RESPONSE:
                payload = result.payload if isinstance(result.payload, dict) else {}
                responder_prefix = str(payload.get("pubkey_pre", "")).lower()
                full_pubkey = str(contact.get("public_key", "")).lower()
                # The underlying wait is not peer-filtered (a response for
                # a different in-flight discovery could otherwise be
                # mistaken for this one's answer) -- verify it actually
                # names the peer queried before accepting it.
                if not responder_prefix or not full_pubkey.startswith(responder_prefix):
                    self._debug(
                        f"discover_path({pubkey_prefix!r}) attempt {attempt}: "
                        f"PATH_RESPONSE pubkey_pre {responder_prefix!r} doesn't "
                        f"match this contact -- ignoring, treating this attempt "
                        f"as unanswered."
                    )
                else:
                    resolved = _ResolvedPath(
                        out_path_hex=str(payload.get("out_path", "")),
                        out_path_len=int(payload.get("out_path_len", 0)),
                        out_path_hash_len=int(payload.get("out_path_hash_len", 1)),
                        resolved_at=time.monotonic(),
                    )
                    # This interface's own record is authoritative for its
                    # own routing/staleness decisions regardless of
                    # whether the device persist below succeeds.
                    self._resolved_paths[pubkey_prefix] = resolved
                    # A freshly discovered path is a different link; an RTT
                    # measured over the previous one doesn't carry over.
                    self._invalidate_ack_rtt(pubkey_prefix, "path (re)discovered")
                    self._record_path_discovery_success(pubkey_prefix)
                    RNS.log(
                        f"{self}: path discovered to {pubkey_prefix!r} in "
                        f"{attempt} attempt(s): out_path_len={resolved.out_path_len}.",
                        RNS.LOG_INFO,
                    )
                    await self._persist_resolved_path(contact, resolved)
                    return resolved

            self._debug(f"discover_path({pubkey_prefix!r}) attempt {attempt}: no response.")

        self._record_path_discovery_failure_round(pubkey_prefix)
        return None

    async def _discover_path_coalesced(self, pubkey_prefix: str) -> Optional[_ResolvedPath]:
        """Milestone 6's part of folding stale-path reset fully into
        routing (docs/reliability_engine_design.md §8: "the next send
        attempt for this peer... goes through discover_path() first...
        and only falls through to genuinely flooding the message itself
        if discovery itself fails") -- called from the DIRECT-primary
        send path (`_send_direct_packet`) whenever no path is currently
        resolved for a peer, whether that's a freshly stale-path-reset
        peer or one that simply never had a path resolved yet.

        Coalesces concurrent callers for the same peer into one shared
        in-flight `discover_path()` attempt: several outgoing packets
        queued for the same not-yet-resolved peer at once would otherwise
        each independently kick off their own quick-attempts burst,
        wasting airtime on redundant PATH_DISCOVERY floods for what's
        really one underlying question. `discover_path()`'s own backoff
        and authoritative-record logic (Milestone 4) is unchanged and
        still applies underneath this -- this only de-duplicates
        concurrent callers, it isn't a second cooldown mechanism."""
        existing = self._pending_path_discoveries.get(pubkey_prefix)
        if existing is not None:
            return await existing

        future = asyncio.get_running_loop().create_future()
        # Retrieved unconditionally so a leader failure with no follower
        # parked on it doesn't log asyncio's "exception was never retrieved".
        future.add_done_callback(lambda f: f.cancelled() or f.exception())
        self._pending_path_discoveries[pubkey_prefix] = future
        try:
            result = await self.discover_path(pubkey_prefix)
        except Exception as exc:
            future.set_exception(exc)
            raise
        else:
            future.set_result(result)
            return result
        finally:
            self._pending_path_discoveries.pop(pubkey_prefix, None)
            if not future.done():
                # Audit fix (2026-09-19): `except Exception` does not catch
                # CancelledError, so a cancelled leader (detach, or any
                # future wait_for wrapper) left this future unresolved AND
                # unreachable -- every follower parked on `await existing`
                # then waited forever, and their _send_direct_packet never
                # returned, so the in-flight key for those packets was held
                # until the 600s sweep.
                future.cancel()

    async def _persist_resolved_path(self, contact, resolved: _ResolvedPath) -> None:
        """docs/path_discovery_spec.md's persistence fix: a successful
        discovery is NOT written to the device's own persistent contact
        record by the firmware itself (CMD_SEND_PATH_DISCOVERY_REQ's
        handler returns before reaching the code path that would). Skip
        this and this interface's own idea of "resolved" silently
        diverges from what the official app / device flash state show."""
        try:
            await self._run_command(
                self._mc_ready.commands.change_contact_path(
                    contact, resolved.out_path_hex, path_hash_mode=resolved.out_path_hash_len - 1
                ),
                "change_contact_path",
                self._EventType.OK,
            )
        except Exception as exc:
            RNS.log(
                f"{self}: persisting discovered path to the device contact "
                f"table failed: {exc} -- this interface's own record stays "
                f"authoritative for routing regardless, but the official "
                f"app / device flash state will disagree until this is "
                f"retried.",
                RNS.LOG_WARNING,
            )

    # -- Stale cached-path detection and reset (§8) ------------------------

    def record_direct_send_result(
        self,
        pubkey_prefix: str,
        succeeded: bool,
        waited_full_timeout: bool,
        rssi: Optional[float] = None,
    ) -> None:
        """docs/path_discovery_spec.md §8 / reliability_engine_design.md
        §8: call this after every DIRECT send attempt made against an
        already-cached path (not during discovery itself, and not for an
        attempt with no cached path to begin with). Not yet wired into an
        actual DIRECT send path -- Milestone 5+ adds the routing
        decisions that would call this for real. Exists now, unit-tested
        directly, as the primitive that logic will call.

        Although this method itself is plain (synchronous, not a
        coroutine), it can schedule a background reset task
        (_spawn_background_task) when the failure threshold is crossed --
        that only attaches correctly to this interface's own dedicated
        event loop if this method is called from a coroutine already
        running on it, exactly where its real Milestone 5+ caller (DIRECT-
        send routing logic) will naturally already be. Calling it from any
        other thread would schedule the reset task on the wrong loop.

        Code-review note on the `rssi` parameter: no current call site
        passes it (confirmed against every `record_direct_send_result`
        call in this file), and this isn't just an integration gap -- the
        installed `meshcore` library's real ACK event (the thing a DIRECT
        send actually correlates on) carries only `code`/`trip_time`, no
        signal-quality field at all. The nearest real signal, SNR (not
        RSSI -- a different unit than `direct_path_reset_rssi_floor`'s
        dBm-scale default), only appears on *inbound* CONTACT_MSG_RECV_V3
        frames, and only under protocol version 3, meaning "the last SNR
        this peer's DIRECT messages arrived with" would need its own
        tracking and a considered dBm<->dB conversion before it could
        feed this floor honestly. Left as configured/tested-but-unreached
        rather than deleted or faked with a converted value that hasn't
        been validated against real hardware."""
        if succeeded:
            self._direct_path_failures.pop(pubkey_prefix, None)
            # Field fix (2026-09-19): remember that this path just worked --
            # see the healthy-path guard below.
            recent = self._direct_path_recent_success.setdefault(pubkey_prefix, [])
            recent.append(time.monotonic())
            if len(recent) > 64:
                del recent[:-64]
            return
        if not waited_full_timeout:
            # An attempt cut short by this engine's own ceiling being too
            # tight proves nothing about the path itself -- the old
            # design's own field-diagnosed gate, kept unchanged.
            return

        failures = self._direct_path_failures.get(pubkey_prefix, 0) + 1
        self._direct_path_failures[pubkey_prefix] = failures

        effective_threshold = self.direct_path_reset_threshold
        # Field fix (2026-09-19 evening session): a path that was demonstrably
        # working moments ago needs more than one short burst of failures
        # before it is thrown away. Measured: a 1-hop path running 50/55 (91%)
        # was discarded after a 7-attempt bad patch, and the replacement
        # discovery adopted a 2-hop path that then managed 1/12 -- the
        # degradation ratcheted (1 -> 2 -> 3 hops) and never recovered,
        # because nothing remembered the previous path had been healthy. LoRa
        # links fade in bursts; requiring proportionally more evidence to
        # abandon a proven path is cheap, and failures still accumulate, so a
        # genuinely dead path is still reset -- just a little later.
        healthy = self._recent_path_successes(pubkey_prefix)
        if healthy >= self.direct_path_healthy_recent_successes:
            effective_threshold = max(
                effective_threshold,
                int(self.direct_path_reset_threshold * self.direct_path_healthy_patience_multiplier),
            )
            self._debug(
                f"record_direct_send_result({pubkey_prefix!r}): {failures} failure(s), but this path "
                f"had {healthy} success(es) in the last {self.direct_path_healthy_window_s:.0f}s -- "
                f"requiring {effective_threshold} failures before resetting it."
            )
        if rssi is not None and rssi > self.direct_path_reset_rssi_floor:
            # Conditions look fine -- be more patient before concluding
            # the path itself, rather than transient RF, is the problem.
            effective_threshold = int(
                self.direct_path_reset_threshold * self.direct_path_reset_patience_multiplier
            )

        if failures < effective_threshold:
            return

        # User-requested fix (2026-09-15, post-alpha-0.1.0 2-hop field
        # test): see direct_path_reset_min_age_s's own comment. A path
        # confirmed too recently to plausibly have gone stale is trusted
        # regardless of accumulated failures -- the failure count above is
        # deliberately NOT reset here, so this re-evaluates on every
        # subsequent failure and fires the moment the path is old enough,
        # rather than being silenced permanently by one early skip.
        resolved = self._resolved_paths.get(pubkey_prefix)
        if resolved is not None:
            age_s = time.monotonic() - resolved.resolved_at
            if age_s < self.direct_path_reset_min_age_s:
                self._debug(
                    f"record_direct_send_result({pubkey_prefix!r}): {failures} "
                    f"failure(s) reached the reset threshold, but this path "
                    f"was only confirmed {age_s:.1f}s ago (< "
                    f"{self.direct_path_reset_min_age_s:.0f}s) -- trusting it "
                    f"a while longer rather than spending a fresh discover_path() "
                    f"burst on a path that hasn't plausibly gone stale yet."
                )
                return

        self._spawn_background_task(self._reset_stale_path(pubkey_prefix))

    async def _reset_stale_path(self, pubkey_prefix: str) -> None:
        # Local state first (irreversible for this path either way, and
        # the device round-trip below can independently fail) -- the next
        # outgoing attempt for this peer should go through discover_path()
        # again rather than retry a path already known to be dead.
        self._direct_path_failures.pop(pubkey_prefix, None)
        self._resolved_paths.pop(pubkey_prefix, None)
        self._invalidate_ack_rtt(pubkey_prefix, "stale path reset")

        contact = self._resolve_contact(pubkey_prefix)
        if contact is None:
            return
        try:
            # reset_path() mutates the library's own local contact dict
            # as a side effect before the device round-trip even resolves
            # (confirmed directly against the installed library) -- the
            # returned event, not a re-read of the contact, is what
            # actually confirms the device-side command's own outcome.
            await self._run_command(
                self._mc_ready.commands.reset_path(contact),
                "reset_path",
                self._EventType.OK,
            )
            RNS.log(
                f"{self}: reset stale cached path for {pubkey_prefix!r} "
                f"after {self.direct_path_reset_threshold}+ consecutive "
                f"full-timeout DIRECT send failures.",
                RNS.LOG_WARNING,
            )
        except Exception as exc:
            RNS.log(
                f"{self}: failed to reset stale path for {pubkey_prefix!r}: {exc}",
                RNS.LOG_WARNING,
            )

    # -------------------------------------------------------------------
    # Peer discovery (docs/peer_discovery_design.md) -- Milestone 5
    # -------------------------------------------------------------------

    def _peer_cache_file_path(self) -> Optional[str]:
        if self.peer_cache_path:
            return self.peer_cache_path
        base = getattr(RNS, "Reticulum", None)
        base = getattr(base, "storagepath", None) if base is not None else None
        if not base:
            return None
        return os.path.join(base, "smci_peers.json")

    def _load_peer_cache(self) -> None:
        """§4 entry point 2: "loading the persisted peer cache at
        startup, once per cached entry" -- routed through the single
        entry-point function (_register_peer) exactly like every other
        way a peer can become known, per §4's single-entry-point rule.
        Each cached entry's own persisted `last_seen` is preserved
        (passed through explicitly), never refreshed to "now" -- a
        restart isn't a fresh sighting, and silently extending every
        cached peer's TTL on every restart would defeat §6's expiry."""
        path = self._peer_cache_file_path()
        if not path or not os.path.isfile(path):
            return
        try:
            with open(path, "r") as f:
                data = json.load(f)
            count = 0
            for entry in data.get("peers", []):
                prefix = str(entry.get("pubkey_prefix", "")).lower()
                if not prefix:
                    continue
                self._register_peer(
                    prefix,
                    has_upstream_rns=entry.get("has_upstream_rns"),
                    source="cache",
                    last_seen=float(entry.get("last_seen", time.time())),
                    raw_fragments=entry.get("raw_fragments"),
                )
                count += 1
            RNS.log(f"{self}: restored {count} peer(s) from cache ({path}).", RNS.LOG_INFO)
        except Exception as exc:
            RNS.log(
                f"{self}: failed to load peer cache ({path}): {exc} -- "
                f"starting with no cached peers.",
                RNS.LOG_WARNING,
            )

    def _save_peer_cache(self) -> None:
        path = self._peer_cache_file_path()
        if not path:
            return
        try:
            data = {
                "peers": [
                    {
                        "pubkey_prefix": peer.pubkey_prefix,
                        "has_upstream_rns": peer.has_upstream_rns,
                        "last_seen": peer.last_seen,
                        "raw_fragments": peer.raw_fragments,
                    }
                    for peer in self._peers.values()
                ]
            }
            tmp_path = path + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(data, f)
            os.replace(tmp_path, path)
        except Exception as exc:
            RNS.log(f"{self}: failed to save peer cache ({path}): {exc}", RNS.LOG_WARNING)

    def _register_peer(
        self, pubkey_prefix: str, has_upstream_rns: Optional[bool] = None,
        source: str = "", last_seen: Optional[float] = None,
        raw_fragments: Optional[bool] = None,
    ) -> None:
        """§4's single entry-point function -- every place a peer can
        become known calls this, and only this, so "a peer becomes known"
        never has two divergent code paths to keep in sync (the old
        design's `force_direct_path` bug, root-caused to exactly that).
        `has_upstream_rns=None` means "no signal, don't touch this field"
        -- only a call sourced from an actually-parsed bind frame may pass
        a real `True`/`False` (§2's hard rule); cache-restore also passes
        through whatever tri-state value was persisted, which is `None`
        for a peer that was never actually confirmed by a bind frame
        before this process last saved its cache.

        Per §5, also (re-)grants telemetry permission -- idempotently,
        unconditionally, every time this runs, including on cache
        restore -- since a bind-frame-gated grant is the actual filter
        that matters (peer_discovery_design.md §5's reasoning), not a
        persisted "did I already grant this" flag. Best-effort only here:
        if the MeshCore contact table hasn't caught up yet (§4's
        "progressively filled in" model -- bind frames and native contact
        adverts are independent floods with no ordering guarantee), the
        periodic contact-refresh loop is the retry path, not this call.

        Although this method itself is plain (synchronous, not a
        coroutine), it can spawn a background telemetry-grant task
        (_spawn_background_task) when a matching contact is already
        resolvable -- exactly the same threading requirement
        record_direct_send_result's own docstring documents (Milestone
        4): that only attaches correctly to this interface's own
        dedicated event loop if this method is called from a coroutine
        already running on it, which every real production call site
        (bind-frame receive, cache load during _async_setup) naturally
        is. Calling it from any other thread schedules the task on the
        wrong loop (or none at all)."""
        pubkey_prefix = pubkey_prefix.lower()
        now = last_seen if last_seen is not None else time.time()
        peer = self._peers.get(pubkey_prefix)
        changed = False
        is_new = peer is None

        if peer is None:
            peer = _PeerRecord(pubkey_prefix=pubkey_prefix, has_upstream_rns=has_upstream_rns, last_seen=now,
                               raw_fragments=raw_fragments)
            self._peers[pubkey_prefix] = peer
            changed = True
            RNS.log(f"{self}: peer bound: {pubkey_prefix!r} (source={source}).", RNS.LOG_INFO)
        else:
            peer.last_seen = now
            if has_upstream_rns is not None and peer.has_upstream_rns != has_upstream_rns:
                peer.has_upstream_rns = has_upstream_rns
                changed = True
            if raw_fragments is not None and peer.raw_fragments != raw_fragments:
                peer.raw_fragments = raw_fragments
                changed = True

        if changed:
            self._save_peer_cache()

        if is_new:
            # Milestone 6: proactively resolve a MeshCore path to a
            # freshly-bound peer rather than waiting for the first
            # outgoing send to discover one reactively -- closes half of
            # peer_discovery_design.md §7's bootstrap gap ahead of time
            # (the other half is the DIRECT-bootstrap-supplement in
            # _send_outgoing_packet), so the very first bootstrap send
            # doesn't also have to wait out a fresh discovery burst.
            # Coalesced like every other discover_path() call, and purely
            # best-effort -- a failure here is silently absorbed, since
            # the reactive path in _send_direct_packet/_send_direct_
            # supplement will simply try again when real traffic needs it.
            self._spawn_background_task(self._discover_path_after_bind(pubkey_prefix))

        contact = self._resolve_contact(pubkey_prefix)
        if contact is not None:
            self._spawn_background_task(self._grant_telemetry_permission_if_needed(contact))

    async def _discover_path_after_bind(self, pubkey_prefix: str) -> None:
        """Milestone 6's proactive discovery on bind, plus one retry
        (2026-09-18 night). The first attempt races the peer's own
        telemetry grant: the node that received our bind REQUEST grants us
        permission while registering us, but the node that sent the
        REQUEST only registers (and grants) us once our RESPONSE arrives --
        up to `bind_response_jitter_max_s` later -- so its own discovery of
        us, fired the instant it saw our REQUEST, is denied by the firmware
        and lands in path-discovery backoff with nothing to retry it until
        real traffic needs the path. Seen as "DIRECT paths never resolved"
        in the simulated-mesh scenarios and as the M5 field note that "the
        next send" is what recovers. One more attempt after the response
        window, with that first denied round's backoff cleared, costs one
        REQ and settles the race in both directions."""
        resolved = await self._discover_path_coalesced(pubkey_prefix)
        if resolved is not None or self.detached or not self.online:
            return
        # Audit fix (2026-09-19): retried a bounded number of times, not once,
        # and each round refreshes contacts first. There are TWO races here,
        # not one. The telemetry-grant race above is settled by waiting out
        # the bind-response window -- but a peer can also bind before its
        # own ADVERT has reached this node at all (bind frames ride CHANNEL
        # and take one hop; an advert has to flood the whole chain), and then
        # `discover_path` bails with "peer is not a known contact" and
        # nothing retries it until real traffic needs the path. That is
        # exactly what kept the suite's only 2-repeater scenario from ever
        # running: contacts and bind both succeeded, `path_req_sent` stayed
        # 0 on every radio, and both nodes logged "peer is not a known
        # contact" twice before giving up. Each round costs one REQ only if
        # it gets far enough to send one.
        for round_number in range(1, self.POST_BIND_DISCOVERY_ROUNDS + 1):
            await asyncio.sleep(self.bind_response_jitter_max_s + 5.0)
            if self.detached or not self.online:
                return
            if pubkey_prefix not in self._peers or pubkey_prefix in self._resolved_paths:
                return
            if self._resolve_contact(pubkey_prefix) is None:
                # The advert has not landed yet -- ask the radio again rather
                # than burning this round on a contact we know we don't have.
                try:
                    await self._refresh_contacts_and_grant_telemetry()
                except Exception as exc:
                    self._debug(f"discover_path({pubkey_prefix!r}): post-bind contact refresh failed: {exc}")
                if pubkey_prefix in self._resolved_paths:
                    return
            self._path_discovery_failures.pop(pubkey_prefix, None)
            self._path_discovery_backoff_until.pop(pubkey_prefix, None)
            self._debug(
                f"discover_path({pubkey_prefix!r}): post-bind retry {round_number}/"
                f"{self.POST_BIND_DISCOVERY_ROUNDS} -- the first attempt likely raced the peer's "
                f"telemetry grant or its advert."
            )
            if await self._discover_path_coalesced(pubkey_prefix) is not None:
                return

    def _touch_peer_seen(self, pubkey_prefix: str) -> None:
        """A lightweight last-seen refresh for a peer ALREADY in the
        registry (§6: "no traffic of any kind" resets the TTL, including
        RNS traffic attributed to a peer via §7's token learning) --
        deliberately NOT routed through _register_peer, which would
        create a brand-new peer record from RNS traffic alone. Only a
        bind frame (or a cache-restored entry) may ever create a peer;
        this can only refresh one that already exists. Memory-only, no
        immediate disk write -- the periodic TTL sweep persists
        accumulated last_seen changes at its own cadence instead, so an
        active peer doesn't trigger a disk write on every single packet."""
        peer = self._peers.get(pubkey_prefix)
        if peer is not None:
            peer.last_seen = time.time()

    async def _peer_discovery_bootstrap(self) -> None:
        """§3/§4: exactly one bootstrap REQUEST, unconditionally, at
        every process start, regardless of what the peer cache restored
        -- the old design's own regression (cache-restore silencing this
        node's own first-boot advertisement) made structurally impossible
        here by never gating this call on any cached/loaded state at all.
        After that, an optional slow repeat while still below
        peer_discovery_target_peers -- a judgment call with no field data
        behind the specific numbers, per the doc's own flag."""
        await self._send_bind_frame(self.BIND_TYPE_REQUEST)
        try:
            interval_s = self._next_rerequest_interval_s(None)
            while not self.detached:
                await asyncio.sleep(self._loop_interval_s(interval_s, "peer_discovery_rerequest_interval"))
                if self.detached:
                    break
                if len(self._peers) >= self.peer_discovery_target_peers:
                    interval_s = self.peer_discovery_rerequest_interval_s
                    continue
                await self._send_bind_frame(self.BIND_TYPE_REQUEST)
                interval_s = self._next_rerequest_interval_s(interval_s)
        except asyncio.CancelledError:
            pass

    def _next_rerequest_interval_s(self, current_s: Optional[float]) -> float:
        """Bind REQUEST repeat schedule while below the target peer count:
        peer_discovery_rerequest_initial, doubling, capped at
        peer_discovery_rerequest_interval (see the latter's comment)."""
        cap = self.peer_discovery_rerequest_interval_s
        if current_s is None:
            return min(self.peer_discovery_rerequest_initial_s, cap)
        return min(current_s * 2.0, cap)

    async def _send_bind_frame(self, frame_type: int) -> None:
        attempt = next(self._bind_attempt_counter) & 0xFF
        frame = self._encode_bind_frame(frame_type, attempt)
        await self._pre_transmit_gate(frame)
        try:
            await self._run_command(
                self._mc_ready.commands.send_chan_msg(self.channel_idx, frame),
                "send_chan_msg(bind)",
                self._EventType.OK,
            )
            self.txb += len(frame)
            self._debug(
                f"sent bind frame type={frame_type} attempt={attempt} "
                f"cap={self._bind_capability():#04x}."
            )
        except Exception as exc:
            RNS.log(f"{self}: bind-frame send failed: {exc}", RNS.LOG_WARNING)

    async def _respond_to_bind_request(self, requester_prefix: str) -> None:
        """§3's collision/storm-avoidance recommendation: per-responder
        randomized jitter before transmitting (collision/half-duplex-deaf-
        repeater spacing, not suppression -- every well-formed REQUEST
        still gets answered eventually), plus a separate, much longer
        global minimum re-response interval, which IS real suppression --
        this node's own capability hasn't changed just because a second
        REQUEST arrived shortly after the first RESPONSE went out."""
        delay = random.uniform(self.bind_response_jitter_min_s, self.bind_response_jitter_max_s)
        await asyncio.sleep(delay)
        if self.detached or not self.online:
            return

        now = time.monotonic()
        if (
            self._last_bind_response_sent is not None
            and now - self._last_bind_response_sent < self.bind_response_min_interval_s
        ):
            self._debug(
                f"skipping bind RESPONSE to {requester_prefix!r} -- still "
                f"within this node's own min re-response interval."
            )
            return

        await self._send_bind_frame(self.BIND_TYPE_RESPONSE)
        self._last_bind_response_sent = now

    def _handle_incoming_bind_frame(self, marker_and_body: str) -> None:
        if not self.peer_discovery_enabled:
            return
        try:
            frame = self._decode_bind_frame(marker_and_body)
        except ValueError as exc:
            self._debug(f"discarding malformed bind frame: {exc}")
            return

        own_prefix = self._own_pubkey_prefix()
        if own_prefix is not None and frame.pubkey_prefix == own_prefix:
            return  # this node's own bind frame, echoed back somehow -- not a peer

        has_upstream = bool(frame.cap & self.BIND_CAP_HAS_UPSTREAM_RNS)
        raw_capable = bool(frame.cap & self.BIND_CAP_RAW_FRAGMENTS)
        self._register_peer(
            frame.pubkey_prefix, has_upstream_rns=has_upstream, source="bind_frame", raw_fragments=raw_capable,
        )

        if frame.type == self.BIND_TYPE_REQUEST:
            self._spawn_background_task(self._respond_to_bind_request(frame.pubkey_prefix))

    def _handle_incoming_completion_frame(self, marker_and_body: str, sender_token: str) -> None:
        """Receive side of the `"Q"`-marker completion check (see
        `_check_remote_completion`'s own docstring for the full
        mechanism/motivation). A QUERY is answered directly from the
        existing whole-packet dedup cache -- `_add_channel_fragment`
        already records a completed DIRECT-fragmented reassembly there
        under exactly the key `(mode, sender_token, pkt_id, frag_total)`
        this method rebuilds, so answering "do you have pkt_id X
        complete" needs no new state of its own, just a lookup into state
        that already exists for an unrelated reason (§7's dedup). Uses
        `sender_token` as received here, uncanonicalized -- matching
        `_reassembly_key`'s own convention of keying on the raw
        MeshCore-native token, never this interface's canonical 6-byte
        peer prefix, so this lookup can never silently miss due to a
        canonicalization mismatch against how the entry was actually
        stored."""
        try:
            frame = self._decode_completion_frame(marker_and_body)
        except ValueError as exc:
            self._debug(f"discarding malformed completion-check frame from {sender_token!r}: {exc}")
            return

        if frame.type == self.COMPLETION_TYPE_QUERY:
            # Step 3 (2026-09-18): answer with what we actually hold, not
            # just complete/not. The key is exactly _reassembly_key's for
            # a non-coop DIRECT frame, so both the dedup cache (a finished
            # reassembly) and a still-open bucket are consulted with the
            # same tuple. A bucket that was evicted or idle-expired reads
            # as "holds nothing" -- correct: the sender must re-drive it
            # all, and would have had to anyway.
            key = ("direct", sender_token or "~anon", frame.pkt_id, frame.frag_total)
            complete = self._dedup_contains(key)
            if complete:
                held = set(range(frame.frag_total))
            else:
                bucket = self._reassembly.get(key)
                held = set(bucket.fragments.keys()) if bucket is not None else set()
            self._debug(
                f"completion QUERY (v{frame.version}) from {sender_token!r} for pkt_id={frame.pkt_id} "
                f"frag_total={frame.frag_total}: answering complete={complete} held={sorted(held)}."
            )
            if self._packet_capture_file is not None:
                self._capture_event("in", {
                    "event": "completion_query_received",
                    "sender_token": sender_token,
                    "pkt_id": frame.pkt_id,
                    "frag_total": frame.frag_total,
                    "query_version": frame.version,
                    "answering_complete": complete,
                    "answering_held": sorted(held),
                })
            self._spawn_background_task(
                self._send_completion_answer(
                    sender_token, frame.pkt_id, frame.frag_total, complete,
                    held=held, version=frame.version, nonce=frame.nonce,
                )
            )
            return

        # ANSWER: correlate against our own canonical peer prefix, since
        # that's the key _query_remote_fragments registered the waiter
        # future under. The whole decoded frame is handed over -- the
        # querying side decides what `complete`/`held` mean for its stage.
        peer_prefix = self._canonical_peer_prefix(sender_token)
        if peer_prefix is None:
            return
        is_report = frame.nonce is not None and (frame.nonce & 0xF0) == self.COMPLETION_REPORT_NONCE_BASE
        if is_report and frame.complete:
            # A receiver-initiated REPORT: sample its latency whether or
            # not a waiter still exists (a late report is the case the
            # estimator most needs to see) -- phase 1, 2026-09-20. Complete
            # reports only: the second-last fragment's incomplete report
            # arrives ~1.5-2 s before the complete one and would train the
            # window short.
            latency_s = self._record_report_latency(peer_prefix, frame.pkt_id)
            if latency_s is not None:
                self._debug(
                    f"completion REPORT from {sender_token!r} for pkt_id={frame.pkt_id} arrived "
                    f"{latency_s:.2f}s after the burst ended (window estimator "
                    f"srtt={self._report_rtt[peer_prefix]['srtt']:.2f}s)."
                )
        waiter = self._completion_query_waiters.get((peer_prefix, frame.pkt_id))
        if waiter is None:
            return
        fut, expected_frag_total, expected_nonce = waiter
        stale_nonce = (
            frame.nonce is not None and expected_nonce is not None and frame.nonce != expected_nonce
        )
        if stale_nonce and frame.complete and frame.frag_total == expected_frag_total:
            # A late answer may only ever tell us the receiver has MORE, never
            # less (field fix, 2026-09-19). An answer whose nonce belongs to an
            # earlier, already-timed-out query for this same packet describes
            # the peer's state as of that query -- so its `held` set is not
            # trustworthy as a replacement for our own (that is exactly the
            # `held=[]` case that discarded real fragments). But
            # `complete=True` is monotone: a receiver that had the whole
            # packet then cannot have less of it now, short of a bucket
            # eviction which only makes a re-send necessary anyway. Accepting
            # it finishes a transfer that is genuinely done instead of waiting
            # for an answer whose round trip exceeds the budget cap -- the
            # case where the peer's answers are consistently slower than
            # `direct_completion_check_timeout_max_s`.
            self._debug(
                f"accepting a LATE completion ANSWER from {sender_token!r} for pkt_id={frame.pkt_id}: "
                f"nonce {frame.nonce} is from an earlier query (outstanding is {expected_nonce}), but it "
                f"reports the packet complete, which cannot become untrue."
            )
            if not fut.done():
                fut.set_result(frame)
            return
        if stale_nonce:
            # Field fix (2026-09-19): a v3 answer whose echoed nonce does not
            # match the outstanding query is a reply to an earlier, already
            # timed-out query for the same packet. Applying it would overwrite
            # this send's fragment state with stale information.
            self._debug(
                f"discarding completion ANSWER from {sender_token!r} for pkt_id={frame.pkt_id}: "
                f"nonce {frame.nonce} does not match the outstanding query's {expected_nonce} "
                f"-- stale answer to an earlier query."
            )
            return
        if frame.frag_total != expected_frag_total:
            # Audit fix (2026-09-19): the waiter is keyed only on
            # (peer_prefix, pkt_id), so a late ANSWER to a *previous* query
            # for this packet -- a timed-out reconcile whose reply arrived
            # after the next query went out -- would otherwise resolve this
            # query's future and be applied authoritatively (the caller
            # overwrites `acked` from it by design). A mismatched frag_total
            # is the one stale case this side can detect for certain: raw
            # and text fragment the same payload into different counts, and
            # a resumed send re-fragments too. The same-frag_total stale
            # answer this comment used to call out as unaddressed is now
            # caught by the v3 query nonce checked above (2026-09-19
            # evening); this frag_total check remains as the v2-peer
            # fallback, since a v2 answer carries no nonce to check.
            self._debug(
                f"discarding completion ANSWER from {sender_token!r} for pkt_id={frame.pkt_id}: "
                f"frag_total={frame.frag_total} does not match the outstanding query's "
                f"{expected_frag_total} -- stale answer to an earlier query."
            )
            return
        if not fut.done():
            fut.set_result(frame)

    def _send_completion_report(self, sender_token: str, header: _FrameHeader, complete: bool, held: set) -> None:
        """Receiver-initiated completion report (2026-09-20): one unsolicited
        v3 ANSWER for a raw burst, nonce `COMPLETION_REPORT_NONCE_BASE |
        round` (the raw header's attempt bits), spawned from the raw receive
        path -- on completion, on a flagged last fragment that left gaps, and
        on a flagged duplicate of a packet already delivered. Best effort
        like the QUERY's answer; the sender's QUERY fallback is the recovery
        path if it is lost. Off when `direct_raw_report_enabled` is no."""
        if not self.direct_raw_report_enabled or header.pkt_id is None:
            return
        nonce = self.COMPLETION_REPORT_NONCE_BASE | ((header.attempt or 0) & 0x03)
        self._debug(
            f"completion REPORT to {sender_token!r} for pkt_id={header.pkt_id} frag_total={header.frag_total}: "
            f"complete={complete} held={sorted(held)} (round {(header.attempt or 0) & 0x03})."
        )
        if self._packet_capture_file is not None:
            self._capture_event("out", {
                "event": "completion_report_sent", "sender_token": sender_token, "pkt_id": header.pkt_id,
                "frag_total": header.frag_total, "complete": complete, "held": sorted(held),
                "round": (header.attempt or 0) & 0x03,
            })
        self._spawn_background_task(
            self._send_completion_answer(
                sender_token, header.pkt_id, header.frag_total, complete,
                held=held, version=self.COMPLETION_PROTOCOL_VERSION, nonce=nonce, report=True,
            )
        )

    async def _send_completion_answer(
        self, sender_token: str, pkt_id: int, frag_total: int, complete: bool,
        held: "Optional[set]" = None, version: Optional[int] = None,
        nonce: Optional[int] = None, report: bool = False,
    ) -> None:
        """Best-effort ANSWER send for `_handle_incoming_completion_frame`'s
        QUERY branch. Deliberately no retry loop: this is already the
        second half of a mechanism built to route around lost ACKs, so
        piling a multi-attempt cycle on top of the answer itself would
        just relocate the same risk rather than reduce it. If this answer
        is lost, the querying side's own `direct_completion_check_timeout_
        s` simply elapses and it falls back to today's give-up behavior --
        no worse than before this feature existed.

        Code review (2026-09-18): one attempt through `_send_direct_frame_
        and_wait_for_ack`, so -- like every other DIRECT exchange -- the
        frame's own firmware ACK is waited out while `_direct_exchange_
        lock` is held, rather than the lock being released with that ACK
        still in flight for the next send to collide with (the previous
        shape). `PRIORITY_NORMAL`, not `PRIORITY_LOW`: the querier is a
        stalled fragmented send waiting ~5s for this, and a LOW answer
        behind a single missed-ACK timeout on this node (5-45s) can never
        make that deadline, turning the peer's QUERY into pure wasted
        airtime. `time_critical` for the same reason. The ACK outcome is
        recorded (`kind="completion_answer"` in the capture) but not
        retried."""
        contact = self._resolve_contact(sender_token)
        target = contact.get("public_key") if contact is not None else None
        if not target:
            self._debug(
                f"completion ANSWER to {sender_token!r} (pkt_id={pkt_id}) not sent -- "
                f"no resolvable contact/public_key."
            )
            return
        frame = self._encode_completion_frame(
            self.COMPLETION_TYPE_ANSWER, pkt_id, frag_total, complete=complete, nonce=nonce,
            held=held, version=version,
        )
        peer_prefix = self._canonical_peer_prefix(sender_token)
        # Simulation finding (2026-09-19, one-hop raw scenario): this
        # ANSWER used to leave the radio right behind the firmware's own
        # ACK for the QUERY, and through a repeater it reached the chain
        # while the repeater was still forwarding that ACK -- half-duplex,
        # so six of six answers were lost with no ACK. Exactly the
        # fragment-chasing-fragment collision `_raw_fragment_gap_s` paces
        # raw bursts for; the same hop-scaled gap here, sized for the ACK
        # frame the repeater is busy with. Zero hop: no gap.
        resolved = self._resolved_paths.get(peer_prefix) if peer_prefix is not None else None
        if resolved is not None:
            hops = max(0, resolved.out_path_len)
        else:
            out_path_len = contact.get("out_path_len", 0) if isinstance(contact, dict) else 0
            hops = 1 if out_path_len is None or out_path_len < 0 else int(out_path_len)
        # A report (2026-09-20) follows a raw fragment, which the firmware
        # does not ACK, so there is no ACK relay to wait out: no hold.
        gap_s = 0.0 if report else self._completion_answer_hold_s(hops)
        if gap_s > 0:
            await asyncio.sleep(gap_s)
        try:
            ok, _waited_full_timeout = await self._send_direct_frame_and_wait_for_ack(
                target, frame, (nonce or 0) & 0x03, peer_prefix=peer_prefix,
                priority=self.PRIORITY_ANSWER, time_critical=True,
                kind="completion_report" if report else "completion_answer",
                # 2026-09-20: the ANSWER's own ACK wait is hop-aware too (it
                # used to run with hop_count=None, i.e. the flat firmware
                # suggestion, so neither the hop cap nor the abort applied).
                hop_count=hops,
                # Phase 1 (2026-09-20): best effort, never retried -- a
                # queued Link handshake may cut this ACK wait once the
                # peer's expected ACK time has passed.
                preemptible=True,
            )
            if not ok:
                self._debug(
                    f"completion ANSWER to {sender_token!r} (pkt_id={pkt_id}) got no ACK -- "
                    f"not retried; the querier's own timeout is the recovery path."
                )
        except Exception as exc:
            self._debug(
                f"completion ANSWER to {sender_token!r} (pkt_id={pkt_id}) failed "
                f"locally: {exc}."
            )

    def _completion_answer_hold_s(self, hops: int) -> float:
        """Quiet time before a completion ANSWER leaves, through repeaters
        (MeshBench finding 3, 2026-09-20, real firmware): the firmware ACKs
        the QUERY the instant it arrives, and every repeater in the chain
        then relays that ACK; the ANSWER used to go out the millisecond the
        ACK's own airtime ended, exactly as the first repeater keyed its
        relay of it, and all three two-hop ANSWERs in `two_hop` were lost
        that way. The ACK's airtime x (1 + 2.5 x hops) covers the relay
        chain including the repeaters' random forward delay (0 to 1.5
        airtimes each). Zero hop: no relay, no hold."""
        if hops <= 0:
            return 0.0
        ack_airtime_s = self._estimate_tx_airtime_s("", on_air_bytes=12)
        return max(0.0, ack_airtime_s * (1.0 + 2.5 * hops))

    async def _peer_ttl_sweep_loop(self) -> None:
        """§6: a peer (and any RNS-token bindings linked to it, §7) is
        dropped after no traffic of any kind for peer_ttl_s. Deliberately
        does NOT touch the MeshCore device's own contact/route table
        (independently owned by the firmware) or the telemetry grant
        (revoking needs a stronger, explicit negative signal than mere
        silence, per §6's own reasoning -- an unreachable peer isn't
        currently issuing anything for the grant to guard against)."""
        try:
            while not self.detached:
                await asyncio.sleep(self._loop_interval_s(self.peer_ttl_sweep_interval_s, "peer_ttl_sweep_interval"))
                if self.detached:
                    break
                now = time.time()
                expired = [
                    prefix for prefix, peer in self._peers.items()
                    if now - peer.last_seen > self.peer_ttl_s
                ]
                for prefix in expired:
                    del self._peers[prefix]
                    self._forget_peer_state(prefix)
                    RNS.log(
                        f"{self}: peer {prefix!r} expired (no traffic for "
                        f"{self.peer_ttl_s:.0f}s).",
                        RNS.LOG_INFO,
                    )
                if expired or self._peers:
                    # Also persists any last_seen drift accumulated since
                    # the previous sweep via _touch_peer_seen's
                    # memory-only updates.
                    self._save_peer_cache()
        except asyncio.CancelledError:
            pass

    def _forget_peer_state(self, pubkey_prefix: str) -> None:
        """Called when a peer is dropped on TTL expiry (`_peer_ttl_sweep_
        loop`) -- clears every per-peer dict keyed by `pubkey_prefix`
        anywhere in this interface, not just the RNS-token tables §7
        itself documents. Code-review fix: the original version of this
        method (named `_forget_rns_tokens_for_peer`, scoped to exactly
        what that name says) left `_resolved_paths`,
        `_path_discovery_failures`, `_path_discovery_backoff_until`,
        `_direct_path_failures`, and `_pending_path_discoveries` all
        untouched on peer expiry -- on a mesh with transient/mobile
        peers, each one that binds, goes stale, and gets swept leaves a
        permanent, never-cleaned entry in up to five other dicts,
        unbounded growth over a long-running unattended field radio, and
        `_stats_loop`'s own `resolved_paths`/`peers_in_discovery_backoff`
        counts permanently overstating live state."""
        stale = [dh for dh, prefix in self._rns_token_peer.items() if prefix == pubkey_prefix]
        for dh in stale:
            del self._rns_token_peer[dh]
        stale_proof = [
            h for h, (prefix, _expiry) in self._proof_correlation.items() if prefix == pubkey_prefix
        ]
        for h in stale_proof:
            del self._proof_correlation[h]

        self._resolved_paths.pop(pubkey_prefix, None)
        self._ack_rtt.pop(pubkey_prefix, None)
        self._clear_peer_path_stats(pubkey_prefix, "peer state forgotten")
        for k in [k for k in self._resumable_sends if k[0] == pubkey_prefix]:
            del self._resumable_sends[k]
        self._path_discovery_failures.pop(pubkey_prefix, None)
        self._path_discovery_backoff_until.pop(pubkey_prefix, None)
        self._direct_path_failures.pop(pubkey_prefix, None)
        # Deliberately NOT cancelled if still in flight -- _discover_path_
        # coalesced's own try/finally already pops this same entry once
        # discover_path() completes on its own; just dropping this
        # interface's reference to it here is enough to stop treating an
        # expired peer as having a resolved (or pending) path, without
        # risking an InvalidStateError from cancelling a future that
        # coroutine still intends to resolve normally.
        self._pending_path_discoveries.pop(pubkey_prefix, None)

    # -- Opportunistic RNS-token learning (§7) -----------------------------

    def _canonical_peer_prefix(self, raw_prefix: str) -> Optional[str]:
        """Resolves a MeshCore-native pubkey prefix (e.g. `pubkey_prefix`
        off a CONTACT_MSG_RECV event) to this interface's own canonical
        6-byte (12-hex-char) peer key, via a full contact lookup. Native
        prefixes handed to this interface by different meshcore events
        aren't guaranteed to share this project's own bind-frame-protocol
        prefix length (§1 fixes that length only for THIS interface's own
        control frames) -- so raw prefix strings of possibly-differing
        lengths are never compared or used as dict keys directly against
        `_peers`/`_rns_token_peer`/`_proof_correlation`; every lookup goes
        through this canonicalization first."""
        if not raw_prefix:
            return None
        contact = self._resolve_contact(raw_prefix)
        if contact is None:
            self._debug(
                f"_canonical_peer_prefix({raw_prefix!r}): no resolvable contact "
                f"-- token learning skipped for this receive. contacts_known="
                f"{len(self._mc.contacts) if self._mc is not None else 'n/a'}."
            )
            return None
        full_key = str(contact.get("public_key", "")).lower()
        if len(full_key) < self.BIND_PUBKEY_PREFIX_BYTES * 2:
            self._debug(
                f"_canonical_peer_prefix({raw_prefix!r}): resolved contact's own "
                f"public_key {full_key!r} is shorter than "
                f"{self.BIND_PUBKEY_PREFIX_BYTES * 2} chars -- token learning skipped."
            )
            return None
        return full_key[: self.BIND_PUBKEY_PREFIX_BYTES * 2]

    def _learn_rns_token(self, token: bytes, sender_peer_prefix: str) -> None:
        """The one place `_rns_token_peer` grows (audit fix, 2026-09-19 --
        added so the capacity bound cannot be bypassed by a future call
        site, in the spirit of `_register_peer` being the single entry point
        for peer state). Re-learning an existing token also refreshes its
        position, so the eviction below targets genuinely idle tokens."""
        self._rns_token_peer.pop(token, None)
        self._rns_token_peer[token] = sender_peer_prefix
        while len(self._rns_token_peer) > self.RNS_TOKEN_PEER_MAX_KEYS:
            evicted, _prefix = self._rns_token_peer.popitem(last=False)
            self._debug(
                f"RNS token table at capacity ({self.RNS_TOKEN_PEER_MAX_KEYS}) -- evicting the "
                f"least-recently-learned token {evicted.hex()[:12]}."
            )

    def _observe_raw_received_packet(self, data: bytes, claimed_peer_prefix: Optional[str]) -> None:
        """Field-diagnosed (2026-09-19, zero-hop image transfer, both
        captures): a packet received as raw fragments never had its
        outgoing PROOF attributed to a peer -- `_proof_correlation` is
        only filled by `_observe_incoming_rns_packet`, which the raw path
        skips because a raw frame's source prefix is unauthenticated. The
        proof (its destination-hash field is the proved packet's truncated
        hash, in no table) then fell through to the unknown-destination
        branch: DIRECT-to-all in small-mesh mode, a CHANNEL broadcast
        beyond three peers -- the transport raw exists to avoid.

        Only when the claimed prefix is a peer this node has already bound
        (bind frame) AND holds a resolved DIRECT path to. Threat model: a
        spoofer who claims a bound peer's prefix can at worst misdirect one
        PROOF to that peer -- a PROOF is cryptographically bound to the
        packet it proves, so it is useless to anyone else, and RNS simply
        re-sends the data. That is no wider than the fallback's own
        behaviour (small-mesh DIRECT-to-all already reaches every bound
        peer, the spoofer included) and strictly narrower than a broadcast.

        Widened the same afternoon (field test `fieldtests/raw/Alpha0.1.2/
        *T1250*`): with the same guard, the whole observe step runs -- RNS
        tokens included. The laptop received the desktop's path-response
        ANNOUNCEs for d4c70c4b five times as raw fragments (a 3-fragment
        announce always goes raw now), learned nothing from any of them,
        bootstrapped three DATA sends, then hit the 300 s unknown-
        destination backoff and dropped 17 packets to a destination that
        was answering every one. A token from a raw frame steers *data*
        to the claimed peer, which is why it was withheld -- but that
        peer is one this node already routes to on the strength of an
        authenticated bind frame, and a text-frame ANNOUNCE from the same
        peer teaches the same token today. The residual exposure is a
        third party who knows a bound peer's 6-byte prefix steering one
        destination's traffic to that (legitimate) peer, a nuisance
        bounded by the token's own expiry, against a default that dropped
        real traffic for five minutes."""
        if claimed_peer_prefix is None:
            return
        if claimed_peer_prefix not in self._peers or claimed_peer_prefix not in self._resolved_paths:
            self._debug(
                f"_observe_raw_received_packet: {claimed_peer_prefix!r} is not a bound peer with a "
                f"resolved path -- not trusting a raw frame's source claim; nothing learned."
            )
            return
        self._observe_incoming_rns_packet(data, claimed_peer_prefix)

    def _observe_incoming_rns_packet(self, data: bytes, sender_peer_prefix: Optional[str]) -> None:
        """§7: populated only from the DIRECT receive path -- a CHANNEL
        "R" frame carries no sender pubkey at all
        (wire_format_design.md), so reliable peer attribution is
        structurally only available here, not for CHANNEL-received
        traffic. Only learns from an already-bound peer (a token from an
        unbound sender would have nothing in `_peers` for §6's TTL/expiry
        bookkeeping to ever clean up)."""
        if sender_peer_prefix is None:
            self._debug("_observe_incoming_rns_packet: no canonical peer prefix -- nothing to learn from.")
            return
        if sender_peer_prefix not in self._peers:
            self._debug(
                f"_observe_incoming_rns_packet: {sender_peer_prefix!r} resolved to a "
                f"contact but isn't a bound peer (_peers={list(self._peers.keys())}) "
                f"-- token learning skipped."
            )
            return

        header = self._parse_rns_header(data)
        if header is None or header.destination_hash is None:
            self._debug(
                f"_observe_incoming_rns_packet: could not parse a destination_hash "
                f"from this {len(data)}-byte payload from {sender_peer_prefix!r} "
                f"(header={header!r}) -- token learning skipped."
            )
            return

        self._touch_peer_seen(sender_peer_prefix)

        if header.packet_type == RNS.Packet.PROOF:
            # Phase 1 (2026-09-20): whatever else this proof means, a bare
            # send keyed by its destination field (a link_id, or a plain
            # DATA's truncated hash) has been answered by this peer.
            self._signal_send_answered(header.destination_hash, "DIRECT PROOF", sender_peer_prefix)
            # Code review (2026-09-18): the one PROOF whose destination
            # field IS worth learning from -- an LRPROOF answering a
            # LINKREQUEST this node sent carries the link_id, a stable
            # identity for that Link's lifetime, and proves the destination
            # it was requested for is reachable through this peer. Learn
            # both tokens and clear that destination's unknown-destination
            # backoff (previously three good Links to the same destination
            # counted as three "failures" -- see the module docstring's
            # 2026-09-18 review entry).
            delivered = self._pending_dest_proofs.pop(header.destination_hash, None)
            if delivered is not None:
                proved_dest, _expiry = delivered
                self._learn_rns_token(proved_dest, sender_peer_prefix)
                self._clear_unknown_dest_backoff(proved_dest)
                self._debug(
                    f"_observe_incoming_rns_packet: PROOF from {sender_peer_prefix!r} for a bootstrap "
                    f"DATA send to {proved_dest.hex()} -- destination is reachable through this peer; "
                    f"token learned, unknown-destination backoff cleared "
                    f"(rns_tokens_learned now {len(self._rns_token_peer)})."
                )
                return
            pending = self._pending_link_requests.pop(header.destination_hash, None)
            if pending is not None:
                requested_dest, _expiry = pending
                self._learn_rns_token(header.destination_hash, sender_peer_prefix)
                self._learn_rns_token(requested_dest, sender_peer_prefix)
                self._clear_unknown_dest_backoff(requested_dest)
                self._debug(
                    f"_observe_incoming_rns_packet: LRPROOF from {sender_peer_prefix!r} answers "
                    f"this node's LINKREQUEST to {requested_dest.hex()} -- learned tokens for both "
                    f"the destination and link_id {header.destination_hash.hex()} "
                    f"(rns_tokens_learned now {len(self._rns_token_peer)})."
                )
                return
            # §7's "PROOF exception": a PROOF's own destination-hash field
            # is the truncated hash of the packet it proves, never a
            # stable per-peer identity -- never recorded in the normal
            # token table no matter how much traffic is observed.
            self._debug(
                f"_observe_incoming_rns_packet: {sender_peer_prefix!r}'s packet is a "
                f"PROOF -- destination_hash {header.destination_hash.hex()} is its "
                f"own truncated hash, not learned as a normal token (§7 exception)."
            )
            return

        self._learn_rns_token(header.destination_hash, sender_peer_prefix)
        self._debug(
            f"_observe_incoming_rns_packet: learned token "
            f"{header.destination_hash.hex()} -> {sender_peer_prefix!r} "
            f"(rns_tokens_learned now {len(self._rns_token_peer)})."
        )
        # A real token learned for this exact destination proves it IS
        # reachable through this peer after all -- clear any backoff
        # immediately rather than waiting for it to expire on its own.
        self._clear_unknown_dest_backoff(header.destination_hash)

        if header.packet_type == RNS.Packet.LINKREQUEST:
            # Code review (2026-09-18): this node's own LRPROOF answering
            # this request will carry the link_id in its destination field
            # (RNS.Packet.pack()), so learning link_id -> peer here is what
            # lets _resolve_routing_peer send that proof DIRECT-primary.
            link_id = self._compute_link_id(data)
            if link_id is not None:
                self._learn_rns_token(link_id, sender_peer_prefix)
                self._debug(
                    f"_observe_incoming_rns_packet: LINKREQUEST from {sender_peer_prefix!r} -- "
                    f"learned link_id {link_id.hex()} -> {sender_peer_prefix!r} for the LRPROOF reply."
                )

        truncated_hash = self._compute_truncated_hash(data, header.header_type)
        if truncated_hash is not None:
            self._proof_correlation[truncated_hash] = (
                sender_peer_prefix, time.monotonic() + self.proof_correlation_ttl_s
            )

    # -------------------------------------------------------------------
    # Incoming (MeshCore event -> RNS core)
    # -------------------------------------------------------------------

    def _subscribe_data_events(self):
        self._mc_ready.subscribe(self._EventType.CHANNEL_MSG_RECV, self._on_channel_msg_recv)
        self._mc_ready.subscribe(self._EventType.CONTACT_MSG_RECV, self._on_contact_msg_recv)
        if hasattr(self._EventType, "RAW_DATA"):
            self._mc_ready.subscribe(self._EventType.RAW_DATA, self._on_raw_data)
        self._subscribe_rx_log_events()

    def _on_raw_data(self, event) -> None:
        try:
            self._on_raw_data_inner(event)
        except Exception as exc:
            self._incoming_dropped_total += 1
            RNS.log(f"{self}: raw DIRECT receive handler failed: {exc}", RNS.LOG_ERROR)
            RNS.log(traceback.format_exc(), RNS.LOG_DEBUG)

    def _on_raw_data_inner(self, event) -> None:
        """Raw binary DIRECT fragments, receive side (2026-09-18 night).
        Anything without our version nibble or our dst prefix is another
        application's raw packet (or one for a neighbour that shares our
        last hop) and is dropped without a log line."""
        if self.detached:
            return
        payload = event.payload if isinstance(event.payload, dict) else {}
        raw = payload.get("payload")
        try:
            data = bytes.fromhex(raw) if isinstance(raw, str) else bytes(raw or b"")
            header, rns_payload, src_prefix, dst_prefix = self._decode_raw_fragment(data)
        except (ValueError, TypeError):
            self._raw_frames_ignored += 1
            return
        own = self._own_pubkey_hex
        if not own or bytes.fromhex(own[: self.RAW_DST_PREFIX_BYTES * 2]) != dst_prefix:
            self._raw_frames_ignored += 1
            return
        self._raw_fragments_received += 1
        self._handle_direct_multifragment_frame(
            header, rns_payload, src_prefix, raw=True,
            report_requested=self._raw_fragment_report_requested(data),
        )

    # MeshCore TXT_MSG framing (firmware: Mesh::createDatagram +
    # Utils::encryptThenMAC + BaseChatMesh::composeMsgPacket, and the
    # packet header): [header:1][path_len:1][path:N][dest_hash:1]
    # [src_hash:1][MAC:2][AES-ECB(timestamp:4 + flags:1 + text + NUL:1)
    # padded to 16]. Confirmed against the other radio's RX log in the
    # 2026-09-18 page-load capture: every full 151-char fragment was
    # heard as exactly 166 bytes = 2 + 4 + ceil16(151 + 6).
    _TXT_MSG_FIXED_OVERHEAD_BYTES = 2 + 1 + 1 + 2
    _TXT_MSG_PLAINTEXT_OVERHEAD_BYTES = 4 + 1 + 1

    _RX_LOG_ROUTE_FLOOD = {0, 1}   # TC_FLOOD, FLOOD (meshcore ROUTE_TYPENAMES order)
    _RX_LOG_ROUTE_DIRECT = {2, 3}  # DIRECT, TC_DIRECT
    _RX_LOG_ACK_BEARING_TYPES = {0, 2}  # REQ, TEXT_MSG -- the receiver answers with an ACK (or PATH when flooded)
    _RX_LOG_NOTHING_FOLLOWS_TYPES = {3, 4}  # ACK, ADVERT

    # MeshCore payload-type codes (referenceprojects/MeshCore-main/src/
    # Packet.h, mirrored by the meshcore library's PAYLOAD_TYPENAMES):
    # the ones whose cleartext payload starts with [dest_hash][src_hash].
    _RX_LOG_ADDRESSED_PAYLOAD_TYPES = frozenset({0, 1, 2, 8})  # REQ, RESPONSE, TEXT_MSG, PATH
    _RX_LOG_PAYLOAD_TYPE_ACK = 3

    _RX_LOG_PAYLOAD_TYPE_RAW_CUSTOM = 15

    async def _start_auto_message_fetching(self) -> None:
        """A real, field-diagnosed correction: the `meshcore` library does
        NOT push CHANNEL_MSG_RECV/CONTACT_MSG_RECV events on its own. The
        firmware queues incoming channel/contact messages internally and
        only notifies a connected client that something is waiting
        (`EventType.MESSAGES_WAITING`) -- actually reading the queued
        message's content off the device (`commands.get_msg()`, which is
        what a subscribed CHANNEL_MSG_RECV/CONTACT_MSG_RECV callback
        actually fires from, confirmed directly against the installed
        library: `get_msg()`'s own `self.send(..., [EventType.
        CONTACT_MSG_RECV, EventType.CHANNEL_MSG_RECV, ...])` uses the same
        shared dispatcher/subscribe mechanism every other subscription
        does, so it fans out to this interface's own long-lived
        subscriptions exactly like any other event) is something the
        client has to explicitly request. `start_auto_message_fetching()`
        is the library's own helper for this: it subscribes to
        MESSAGES_WAITING and drains with get_msg() every time one fires,
        plus does one immediate get_msg() check for anything already
        queued at connect time. Without calling this, `_subscribe_data_
        events()` above subscribes to event types that would simply never
        fire for real incoming traffic -- confirmed live: a two-node field
        test sent traffic that physically arrived and sat queued on the
        receiving device (readable directly via `commands.get_msg()`) while
        the connected interface, subscribed but never draining, received
        nothing. PATH_RESPONSE/ACK/OK/ERROR are unaffected -- those are
        pushed directly by the device as part of the request/reply they
        answer, not gated behind this queue-and-poll message mechanism."""
        try:
            await self._mc_ready.start_auto_message_fetching()
        except Exception as exc:
            RNS.log(
                f"{self}: failed to start auto message fetching: {exc} -- "
                f"incoming CHANNEL/DIRECT messages will not be received "
                f"until this is retried.",
                RNS.LOG_ERROR,
            )

    async def _rearm_auto_message_fetching(self) -> None:
        """Called from `_on_mc_connected` on every reconnect (not just
        the initial connect `_async_setup` already handles) -- see that
        call site's own comment for why this is needed. Stops first,
        best-effort, so a still-registered `MESSAGES_WAITING` subscription
        from before the reconnect doesn't accumulate a second one
        alongside the fresh one `_start_auto_message_fetching` adds."""
        if self._mc is None:
            return
        try:
            await self._mc.stop_auto_message_fetching()
        except Exception:
            pass
        await self._start_auto_message_fetching()

    def _on_channel_msg_recv(self, event):
        try:
            self._on_channel_msg_recv_inner(event)
        except Exception as exc:
            # Audit fix (2026-09-19): the three callbacks that carry RNS
            # payloads had no top-level guard (only _on_rx_log_data did), so
            # any unexpected exception was caught by the meshcore
            # dispatcher and logged through the library's `logging` only --
            # never RNS.log, never counted, invisible to the operator, with
            # the packet silently lost. Same failure mode
            # _log_background_task_exception exists to prevent on the send
            # side.
            self._incoming_dropped_total += 1
            RNS.log(f"{self}: CHANNEL receive handler failed: {exc}", RNS.LOG_ERROR)
            RNS.log(traceback.format_exc(), RNS.LOG_DEBUG)

    def _on_channel_msg_recv_inner(self, event):
        if self.detached:
            return
        payload = event.payload if isinstance(event.payload, dict) else {}
        raw_text = payload.get("text", "")

        # The firmware unconditionally prepends "<node_name>: " to every
        # CHANNEL text message it relays (meshcore_protocol_rules.md
        # CHANNEL rule 2) -- split on the first ": " to recover the
        # marker/header/payload that follows it. A node with no
        # configured name collapses this to a bare ": " prefix, which
        # this same split handles correctly (sender_name == "").
        sender_name, sep, remainder = raw_text.partition(": ")
        if not sep:
            return  # not framed the way this firmware's CHANNEL send always frames it

        if remainder.startswith(self.PEER_MARKER):
            # Bind frames (docs/peer_discovery_design.md §1) are a
            # separate control protocol riding the same CHANNEL, checked
            # here before RNS-frame handling since the two markers are
            # disjoint by construction ("P" vs "R"). The CHANNEL name
            # prefix is NOT used as the sender's identity for this --
            # unreliable/non-unique per §1; the bind frame carries its
            # own pubkey_prefix field for that.
            self._handle_incoming_bind_frame(remainder)
            return

        self._handle_incoming_frame(remainder, mode="channel", sender_token=sender_name)

    def _on_contact_msg_recv(self, event):
        try:
            self._on_contact_msg_recv_inner(event)
        except Exception as exc:
            self._incoming_dropped_total += 1
            RNS.log(f"{self}: DIRECT receive handler failed: {exc}", RNS.LOG_ERROR)
            RNS.log(traceback.format_exc(), RNS.LOG_DEBUG)

    def _on_contact_msg_recv_inner(self, event):
        if self.detached:
            return
        # Field-diagnosed fix (2026-09-18, see module docstring): this used
        # to stamp _last_incoming_direct_at here, unconditionally, for
        # every DIRECT frame heard -- see _handle_direct_multifragment_frame
        # for where that timestamp is set now and why.
        payload = event.payload if isinstance(event.payload, dict) else {}
        text = payload.get("text", "")
        sender_token = payload.get("pubkey_prefix", "")
        if text.startswith(self.COMPLETION_MARKER):
            # Completion-check frames (see _check_remote_completion's own
            # docstring) are DIRECT-only and checked here before RNS-frame
            # handling, the same way bind frames are checked before
            # _handle_incoming_frame on the CHANNEL side -- the two
            # markers are disjoint by construction ("Q" vs "R").
            self._handle_incoming_completion_frame(text, sender_token)
            return
        self._handle_incoming_frame(text, mode="direct", sender_token=sender_token)

    def _handle_incoming_frame(self, marker_and_body: str, mode: str, sender_token: str) -> None:
        if not marker_and_body.startswith(self.MARKER):
            return  # ordinary traffic sharing the channel/contact, not ours

        try:
            header, rns_payload = self._decode_frame(marker_and_body, mode=mode)
        except ValueError as exc:
            self._debug(f"discarding malformed {mode.upper()} frame from {sender_token!r}: {exc}")
            return

        if mode == "direct":
            if header.coop:
                # Code-review fix: the 0x40 cooperative-dispatch bit has
                # "no defined meaning in the DIRECT header shapes" per
                # wire_format_design.md's own text ("a DIRECT receiver
                # should never inspect it") -- previously accepted
                # silently, which combined with _reassembly_key's own
                # sender_token-based keying meant a DIRECT sender setting
                # this bit could land fragments in the same `("~coop",
                # pkt_id, frag_total)` bucket a CHANNEL cooperative-
                # dispatch fragment (or another DIRECT sender doing the
                # same) uses, merging unrelated senders' fragments into
                # one "complete" packet. Reject outright, matching every
                # other structurally-invalid-per-this-design's-own-rules
                # case this interface already treats as malformed.
                self._incoming_dropped_total += 1
                RNS.log(
                    f"{self}: dropping DIRECT frame from {sender_token!r} -- "
                    f"the cooperative-dispatch bit has no defined meaning on "
                    f"DIRECT and is never set by this interface's own "
                    f"encoder; treating as malformed.",
                    RNS.LOG_WARNING,
                )
                return
            if header.multi_fragment:
                self._handle_direct_multifragment_frame(header, rns_payload, sender_token)
                return
            self._debug(f"DIRECT frame from {sender_token!r}: {len(rns_payload)}-byte payload.")
            # Code-review fix: a bare DIRECT frame carries no pkt_id at all
            # (it relies entirely on the firmware's own ACK cycle, per
            # wire_format_design.md), so it can't reuse _reassembly_key's
            # pkt_id-based scheme -- but since Milestone 6 added this
            # interface's own _send_direct_with_attempts retry loop on top
            # of that firmware ACK cycle, a lost ACK now causes a genuine
            # resend (the firmware's own attempt-flag varies, but this
            # interface's own bare-DIRECT frame content never does), which
            # without a dedup check here would deliver the same logical
            # packet to RNS core twice -- the one receive path that skipped
            # the dedup discipline every other receive path already has.
            # Alpha 0.1.1 fix (2026-09-18 night, see module docstring): RNS
            # re-delivers identical bytes on purpose for the contexts its
            # own packet_filter exempts -- a Resource part re-requested
            # after arriving outside the receive window stalled a whole
            # transfer here when every re-send was dropped as a duplicate.
            # Those contexts bypass the dedup; everything else keeps it.
            rns_header = self._parse_rns_header(rns_payload)
            rns_dedups = rns_header is None or rns_header.context not in self._RNS_NO_DEDUP_CONTEXTS
            dedup_key = ("~direct_bare", sender_token or "~anon", rns_payload)
            if rns_dedups:
                if self._dedup_contains(dedup_key):
                    self._incoming_dropped_total += 1
                    self._debug(f"dropping duplicate bare DIRECT packet from {sender_token!r} (already delivered).")
                    return
                self._dedup_add(dedup_key, rns_payload)
            peer_prefix = self._canonical_peer_prefix(sender_token)
            self._observe_incoming_rns_packet(rns_payload, peer_prefix)
            
            # Log heard my own frame.
            # TODO: Review if this actually happens and make adjustments accordingly. This may need to be a filter
            # NOTE (2026-09-18 coherence review): this is deliberately
            # log-only -- it does NOT drop the frame, which is why the
            # message below no longer says "ignoring" (it used to, while
            # still falling through to process_incoming, which would have
            # actively misled anyone reading these logs to diagnose a field
            # issue). Bounded either way: _observe_incoming_rns_packet
            # above can't learn anything from a self-echo, since it only
            # learns from prefixes present in `_peers` and this node never
            # registers itself (see _handle_incoming_bind_frame's own
            # own_prefix guard). Deciding whether to add the filter is the
            # open TODO above -- it needs field evidence that this fires
            # at all, which is what this log line exists to gather.
            if peer_prefix == self._own_pubkey_prefix():
                self._debug(
                    f"heard my own bare DIRECT frame from {sender_token!r} -- "
                    f"still delivering it to RNS (no self-echo filter applied on this path)."
                )

            self.process_incoming(rns_payload, transport="direct_bare", sender_peer_prefix=peer_prefix)
            return

        self._handle_channel_frame(header, rns_payload, sender_token)

    def _handle_direct_multifragment_frame(
        self, header: _FrameHeader, payload: bytes, sender_token: str, raw: bool = False,
        report_requested: bool = False,
    ) -> None:
        """docs/wire_format_design.md's DIRECT-needs-fragmenting receive
        side (Milestone 6) -- reuses the exact same reassembly/dedup
        machinery `_handle_channel_frame`/`_add_channel_fragment` already
        built for CHANNEL. Both are keyed off `(mode, sender_token,
        pkt_id, frag_total)` (`_reassembly_key`) -- `mode` is included
        specifically so DIRECT's own `sender_token` (a peer's
        `pubkey_prefix`) and CHANNEL's (a node's own plaintext,
        attacker-controlled `adv_name`) can never collide in
        `self._reassembly`/`self._dedup`, even though nothing stops a
        CHANNEL sender from choosing an `adv_name` equal to some other
        peer's real pubkey_prefix (a code-review-found gap this method's
        own docstring used to wrongly claim was structurally impossible
        without this). The one real difference from CHANNEL: once
        reassembly completes, §7's
        opportunistic token learning runs on the *complete* packet, using
        the peer identity DIRECT alone can authenticate -- CHANNEL's own
        multi-fragment path can't do this at all (no sender identity to
        learn from), which is why `_add_channel_fragment` itself stays
        deliberately silent on this and this caller adds it instead of
        pushing DIRECT-specific behavior down into the shared helper.

        Field-diagnosed fix (2026-09-18, see module docstring): this is
        also the only place `_last_incoming_direct_at` (`_wait_for_
        incoming_quiet`'s trigger) gets set now, and only when this
        fragment leaves its bucket still incomplete -- concrete evidence
        this sender has more fragments of this specific transfer still
        coming, unlike the old "any DIRECT frame heard" trigger that
        counted its own ACKs/PROOFs/completion-checks and caused a mutual
        reset feedback loop between two chatty nodes."""
        key = self._reassembly_key(header, sender_token, mode="direct")

        if self._dedup_contains(key):
            self._incoming_dropped_total += 1
            self._debug(f"dropping late/duplicate DIRECT fragment for {key} (already delivered).")
            if raw and report_requested:
                # Completion report (2026-09-20): a flagged fragment for a
                # packet already delivered means the sender never got the
                # report (or a QUERY's answer) and re-burst -- tell it again,
                # so it stops without a QUERY round trip.
                self._send_completion_report(sender_token, header, complete=True, held=set(range(header.frag_total)))
            return

        complete_data = self._add_channel_fragment(key, header, payload, raw=raw)
        if complete_data is None:
            self._last_incoming_direct_at = time.monotonic()
            if raw and report_requested:
                # One of the burst's last two fragments landed but the bucket
                # has gaps: report the bitmap unasked, so the sender re-drives
                # exactly the missing fragments without a QUERY first.
                bucket = self._reassembly.get(key)
                held = set(bucket.fragments.keys()) if bucket is not None else set()
                self._send_completion_report(sender_token, header, complete=False, held=held)
        else:
            peer_prefix = self._canonical_peer_prefix(sender_token)
            if raw:
                # Report BEFORE RNS sees the packet, so the report enters the
                # radio lock ahead of whatever RNS sends back (a PROOF, the
                # next Resource request) and the sender learns first.
                self._send_completion_report(sender_token, header, complete=True, held=set(range(header.frag_total)))
            if not raw:
                self._observe_incoming_rns_packet(complete_data, peer_prefix)
            else:
                # raw=True (2026-09-18 night): the src prefix in a raw frame
                # is unauthenticated, so nothing is learned from it -- unless
                # the claimed peer is one this node already binds and routes
                # to (2026-09-19, twice in one day: first the PROOF
                # correlation, then the token learning a raw ANNOUNCE was
                # silently denied) -- see _observe_raw_received_packet.
                self._observe_raw_received_packet(complete_data, peer_prefix)
            self.process_incoming(
                complete_data, transport="direct_raw_multifragment" if raw else "direct_multifragment",
                sender_peer_prefix=peer_prefix, frag_total=header.frag_total, pkt_id=header.pkt_id,
            )

    def _handle_channel_frame(self, header: _FrameHeader, payload: bytes, sender_token: str) -> None:
        key = self._reassembly_key(header, sender_token, mode="channel")

        if not header.multi_fragment:
            # Single-fragment fast path: complete by construction, so the
            # dedup cache holds the *whole packet* under this key -- a
            # same-key hit is directly comparable against `payload`. Per
            # §6's "verify content identity, don't just assume it"
            # principle (already applied to reassembly's repeated-
            # frag_idx case), extended here to the same residual risk
            # §5.1 describes: two different anonymous ("~anon") senders
            # -- or two genuinely different senders whose 16-bit pkt_id
            # counters happen to collide -- picking the same key for two
            # *different* packets within the dedup TTL window is a real,
            # not just theoretical, scenario on a channel with more than
            # one unnamed node. Treating a same-key-different-content
            # arrival as "already delivered" would silently and
            # permanently drop a legitimate packet.
            cached_payload = self._dedup_get(key)
            if cached_payload is not None:
                if cached_payload == payload:
                    self._incoming_dropped_total += 1
                    self._debug(f"dropping duplicate CHANNEL packet {key} (already delivered).")
                    return
                RNS.log(
                    f"{self}: dedup-cache collision for key {key} -- a new "
                    f"arrival's content differs from what's cached under "
                    f"the same key (§5.1's blank/anonymous-sender residual "
                    f"risk); treating it as a distinct packet rather than "
                    f"risk silently dropping a legitimate one.",
                    RNS.LOG_WARNING,
                )
            self._dedup_add(key, payload)
            self._debug(
                f"CHANNEL frame from {sender_token or '~anon'}: pkt_id={header.pkt_id} "
                f"attempt={header.attempt} {len(payload)}-byte payload."
            )
            self.process_incoming(
                payload, transport="channel_bare", channel_sender_claimed=sender_token or None,
                pkt_id=header.pkt_id,
            )
            return

        # Multi-fragment: dedup is a pure key check here -- at this point
        # we only ever have one fragment's own chunk, never the whole
        # reassembled packet, so there's nothing meaningful to content-
        # compare against. A key match means this packet already
        # completed and was evicted from self._reassembly (§6's "late
        # duplicate after completion" case); once that's happened there's
        # no cached content left to verify a late fragment against, so a
        # key match alone is what the existing design already treats as
        # sufficient to drop it.
        if self._dedup_contains(key):
            self._incoming_dropped_total += 1
            self._debug(f"dropping late/duplicate CHANNEL fragment for {key} (already delivered).")
            return

        complete_data = self._add_channel_fragment(key, header, payload)
        if complete_data is not None:
            self.process_incoming(
                complete_data, transport="channel_multifragment",
                channel_sender_claimed=sender_token or None,
                frag_total=header.frag_total, pkt_id=header.pkt_id,
            )

    # -- Reassembly (docs/reliability_engine_design.md §5) ----------------

    def _reassembly_key(self, header: _FrameHeader, sender_token: str, mode: str):
        """§5.2's keying scheme. The `0x40`/coop branch keys on
        `(pkt_id, frag_total)` alone, excluding sender identity entirely
        -- required for cooperative broadcast's delegates (each
        transmitting under their own firmware-assigned name) to ever land
        in the same bucket, per wire_format_design.md's "cooperative-
        broadcast reassembly bug" fix. Nothing sets this bit until
        Milestone 7, but the branch is built now per the architecture
        doc's own instruction, so M7 only has to add a sender, never
        touch this receive path again (a DIRECT receiver rejects a
        coop-flagged frame outright before ever reaching this method --
        see `_handle_incoming_frame` -- so this branch is CHANNEL-only in
        practice today, but `mode` is still included below rather than
        relied on implicitly).

        `mode` ("channel"/"direct") is included in every returned key --
        a code-review-found gap, not just defensive style: DIRECT's own
        `sender_token` (a peer's `pubkey_prefix`) and CHANNEL's (a node's
        own plaintext, attacker-controlled `adv_name`) were previously
        assumed to be unable to collide since they're "different kinds of
        identifiers," but nothing actually enforced that -- a CHANNEL
        sender can choose any `adv_name` string it likes, including one
        that happens to equal another peer's real pubkey_prefix, which
        would otherwise land its fragments in the same
        `self._reassembly`/`self._dedup` bucket as that peer's genuine
        DIRECT-fragmented send. Namespacing by transport makes that
        structurally impossible instead of merely assumed-away, matching
        this project's own established preference (e.g. the capability
        hard-rule enforcement) for structural fixes over trusted
        invariants."""
        if header.coop:
            return (mode, "~coop", header.pkt_id, header.frag_total)
        return (mode, sender_token or "~anon", header.pkt_id, header.frag_total)

    def _new_reassembly_bucket(self, key, frag_total: int, coop: bool) -> _ReassemblyBucket:
        if len(self._reassembly) >= self.reassembly_max_keys:
            self._evict_oldest_reassembly_bucket()
        bucket = _ReassemblyBucket(frag_total=frag_total, coop=coop)
        self._reassembly[key] = bucket
        return bucket

    def _evict_oldest_reassembly_bucket(self) -> None:
        # §5.3: bounded capacity, oldest-by-last-progress -- a bucket
        # that's kept receiving fragments across retry passes is clearly
        # still alive regardless of when it happened to start, so
        # eviction targets staleness, not age.
        if not self._reassembly:
            return
        oldest_key = min(self._reassembly, key=lambda k: self._reassembly[k].last_progress)
        self._incoming_dropped_total += 1
        RNS.log(
            f"{self}: reassembly map at capacity ({self.reassembly_max_keys} "
            f"keys) -- evicting oldest-by-last-progress bucket {oldest_key}.",
            RNS.LOG_WARNING,
        )
        del self._reassembly[oldest_key]

    def _add_channel_fragment(self, key, header: _FrameHeader, payload: bytes, raw: bool = False) -> Optional[bytes]:
        """Shared reassembly-fragment-accumulation logic for both CHANNEL
        (`_handle_channel_frame`) and DIRECT
        (`_handle_direct_multifragment_frame`) multi-fragment receipt --
        the name predates DIRECT reassembly (Milestone 2) and is kept
        rather than churned, since the logic itself was already
        transport-agnostic (nothing below reads anything CHANNEL-
        specific). Returns the complete reassembled payload the moment
        the last fragment arrives, or `None` while still incomplete (or
        on a same-index content collision, handled by evicting the whole
        bucket) -- delivering to RNS core (`process_incoming`) and, for
        DIRECT, opportunistic token learning are both the caller's job,
        not this method's, since only the DIRECT caller needs the latter."""
        bucket = self._reassembly.get(key)
        if bucket is None:
            bucket = self._new_reassembly_bucket(key, header.frag_total, header.coop)

        if header.frag_idx in bucket.fragments:
            # §6: a repeated frag_idx must be byte-identical by
            # construction (every attempt of a given index carries the
            # same payload bytes) -- verify rather than assume, since
            # this is also the safety net for §5.1/§5.2's residual
            # collision risk (two different transmissions sharing a key).
            if bucket.fragments[header.frag_idx] != payload:
                self._incoming_dropped_total += 1
                RNS.log(
                    f"{self}: reassembly collision detected for key {key} at "
                    f"frag_idx {header.frag_idx} -- two different "
                    f"transmissions produced different bytes for the same "
                    f"slot; evicting rather than risk delivering a "
                    f"corrupted amalgam.",
                    RNS.LOG_WARNING,
                )
                self._reassembly.pop(key, None)
            return None  # identical repeat: already counted, nothing more to do

        bucket.fragments[header.frag_idx] = payload
        bucket.last_progress = time.monotonic()
        
        if header.pkt_id is not None:
            self._capture_fragment_received(
                key[0], key[1], header.pkt_id, header.frag_idx, header.frag_total, len(bucket.fragments), raw=raw,
            )

        if len(bucket.fragments) < bucket.frag_total:
            self._debug(
                f"reassembly progress {key}: {len(bucket.fragments)}/{bucket.frag_total}"
            )
            return None

        # Complete: per §6, delete from the reassembly map immediately
        # and record it in the whole-packet dedup cache in the same step
        # -- the two caches are sequential for a given key, never
        # concurrent.
        del self._reassembly[key]
        complete_data = b"".join(bucket.fragments[i] for i in range(bucket.frag_total))
        self._dedup_add(key, complete_data)
        self._debug(f"reassembly complete {key}: {len(complete_data)} bytes")
        return complete_data

    async def _reassembly_cleanup_loop(self):
        """§5.4's idle-since-last-progress TTL, swept periodically rather
        than checked lazily -- a bucket that simply stops receiving
        fragments needs to be reclaimed even if nothing ever queries it
        again. Also sweeps expired whole-packet dedup entries (§7),
        expired PROOF-correlation entries (§7's other table, Milestone 6
        fix below), and idle unknown-destination backoff state
        (code-review fix, `_unknown_dest_backoff_sweep`) in the same pass,
        since all four live only on this event loop thread and share the
        same natural cadence."""
        try:
            while not self.detached:
                await asyncio.sleep(self.REASSEMBLY_CLEANUP_INTERVAL_S)
                if self.detached:
                    break

                now = time.monotonic()
                stale_keys = [
                    key
                    for key, bucket in self._reassembly.items()
                    if now - bucket.last_progress > (
                        self.reassembly_idle_timeout_coop_s
                        if bucket.coop
                        else self.reassembly_idle_timeout_s
                    )
                ]
                for key in stale_keys:
                    del self._reassembly[key]
                    self._incoming_dropped_total += 1
                    RNS.log(
                        f"{self}: reassembly bucket {key} evicted -- idle "
                        f"timeout exceeded with no completion.",
                        RNS.LOG_WARNING,
                    )

                self._dedup_sweep(now)
                self._proof_correlation_sweep(now)
                self._unknown_dest_backoff_sweep(now)
                self._path_response_rate_limit_sweep(now)
                self._pending_link_request_sweep(now)
                self._send_answered_sweep(now)
                self._announce_cache_sweep(now)
                self._outgoing_inflight_sweep(now)
                self._resumable_sends_sweep(now)
                self._closed_links_sweep(now)
                self._pending_dest_proofs_sweep(now)
                for path_hex in [p for p, n in self._raw_unsupported_paths.items()
                                 if now - n["since"] >= self.direct_raw_path_unsupported_ttl_s]:
                    del self._raw_unsupported_paths[path_hex]
        except asyncio.CancelledError:
            pass

    def _proof_correlation_sweep(self, now: float) -> None:
        """Field-diagnosed gap: `_resolve_routing_peer`'s own PROOF-
        correlation lookup already expires an entry lazily, on the exact
        lookup that would use it (§7), but most delivered packets never
        actually get proved -- an RNS Link doesn't send a PROOF back for
        every single DATA packet -- so an entry whose PROOF never comes
        would otherwise sit in `_proof_correlation` forever, unlike
        `_dedup`/`_reassembly`, which both already get swept here
        regardless of whether anything ever queries them again. Confirmed
        live: `proof_correlations_pending` sat perfectly flat for minutes
        during a real field test, exactly the signature of a table with
        no time-based reclaim. This is that reclaim, mirroring
        `_dedup_sweep`'s own shape."""
        expired = [h for h, (_peer, expiry) in self._proof_correlation.items() if now >= expiry]
        for h in expired:
            del self._proof_correlation[h]

    # -- Whole-packet dedup (docs/reliability_engine_design.md §7) --------

    def _dedup_get(self, key) -> Optional[bytes]:
        """Returns the cached whole-packet payload for `key` if present
        and not yet expired (also lazily evicting an expired entry it
        finds along the way), else None."""
        entry = self._dedup.get(key)
        if entry is None:
            return None
        expiry, cached_payload = entry
        if time.monotonic() >= expiry:
            del self._dedup[key]
            return None
        return cached_payload

    def _dedup_contains(self, key) -> bool:
        return self._dedup_get(key) is not None

    def _dedup_add(self, key, payload: bytes) -> None:
        self._dedup[key] = (time.monotonic() + self.whole_packet_dedup_ttl_s, payload)

    def _dedup_sweep(self, now: float) -> None:
        expired = [k for k, (expiry, _payload) in self._dedup.items() if now >= expiry]
        for k in expired:
            del self._dedup[k]

    def process_incoming(
        self, data: bytes, *, transport: str = "unknown",
        sender_peer_prefix: Optional[str] = None, channel_sender_claimed: Optional[str] = None,
        frag_total: Optional[int] = None, pkt_id: Optional[int] = None,
    ) -> None:
        """The single funnel every complete incoming RNS payload passes
        through, regardless of shape (CHANNEL/DIRECT, bare/multi-
        fragment) -- not an RNS.Interface base-class method (confirmed:
        the base class has none by this name), so free to carry these
        capture-only keyword parameters without risking a base-class
        signature mismatch. All default to values meaning "not
        applicable/not known" so this stays a normal call for anything
        that doesn't care about packet capture."""
        if not self.online or self.detached:
            return
        if transport != "local_announce_cache":
            self.rxb += len(data)
        header = self._parse_rns_header(data)
        self._note_link_closed(header)
        if transport.startswith("channel"):
            self._note_channel_proof(header, transport)
        elif transport != "local_announce_cache":
            self._cache_announce(data, header, sender_peer_prefix)
        if self._packet_capture_file is not None:
            self._capture_incoming(
                data, transport=transport, sender_peer_prefix=sender_peer_prefix,
                channel_sender_claimed=channel_sender_claimed, frag_total=frag_total, pkt_id=pkt_id,
            )
        self.owner.inbound(data, self)

    # -------------------------------------------------------------------
    # Teardown
    # -------------------------------------------------------------------

    def detach(self):
        # Base RNS.Interfaces.Interface.detach() is a no-op -- without this
        # override, the dedicated event-loop thread and the underlying
        # MeshCore connection have nothing to actually close down when RNS
        # core detaches this interface
        # (`docs/reliability_engine_design.md`'s base-class contract notes).
        if self.detached:
            return

        self.detached = True
        self.online = False

        # Unblocks _outgoing_worker's blocking queue.Queue.get() (running
        # on a thread-pool executor thread, not this event loop) -- that
        # call has no cancellation mechanism of its own, so nothing else
        # would ever wake it once no more packets get queued. Safe to do
        # from this (synchronous, possibly-RNS-core) thread: queue.Queue
        # is thread-safe by design.
        try:
            self._outqueue.put_nowait(self._OUTQUEUE_SHUTDOWN_SENTINEL)
        except queue.Full:
            pass

        # Reads the raw backing field, not the `_loop` property above --
        # this teardown safety net must tolerate setup never having
        # completed (no event loop ever created), which the property's
        # assert deliberately treats as a real invariant violation
        # everywhere else.
        loop = self._loop_impl
        if loop is not None and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(self._async_teardown(), loop)
            try:
                future.result(timeout=self.SETUP_TIMEOUT_S)
            except Exception as exc:
                RNS.log(f"{self}: error during async teardown: {exc}", RNS.LOG_ERROR)
            loop.call_soon_threadsafe(loop.stop)

        if self._loop_thread is not None:
            self._loop_thread.join(timeout=self.SETUP_TIMEOUT_S)

        # Only safe to close once run_forever() has actually returned (i.e.
        # after the join above) -- closing a still-running loop is invalid.
        if loop is not None and not loop.is_running():
            loop.close()

        self._close_packet_capture()

        RNS.log(f"{self}: detached.", RNS.LOG_INFO)

    async def _async_teardown(self):
        if self._stats_task is not None:
            self._stats_task.cancel()
        if self._reassembly_cleanup_task is not None:
            self._reassembly_cleanup_task.cancel()
        if self._contact_refresh_task is not None:
            self._contact_refresh_task.cancel()
        if self._peer_discovery_task is not None:
            self._peer_discovery_task.cancel()
        if self._peer_ttl_sweep_task is not None:
            self._peer_ttl_sweep_task.cancel()
        for task in list(self._background_tasks):
            task.cancel()
        if self._outgoing_worker_task is not None:
            # The shutdown sentinel is already in the queue (detach()
            # placed it before scheduling this coroutine) -- just wait
            # for the worker to notice it and return on its own, with a
            # bounded fallback rather than hanging teardown if it somehow
            # doesn't.
            try:
                await asyncio.wait_for(self._outgoing_worker_task, timeout=5.0)
            except Exception:
                self._outgoing_worker_task.cancel()
        if self._mc is not None:
            try:
                await self._mc.stop_auto_message_fetching()
            except Exception as exc:
                RNS.log(f"{self}: error stopping auto message fetching: {exc}", RNS.LOG_WARNING)
            try:
                await self._mc.disconnect()
            except Exception as exc:
                RNS.log(
                    f"{self}: error disconnecting from MeshCore device: {exc}",
                    RNS.LOG_WARNING,
                )

    # -------------------------------------------------------------------

    def __str__(self):
        return f"SmartMeshCoreInterface[{self.name}]"


interface_class = SmartMeshCoreInterface
