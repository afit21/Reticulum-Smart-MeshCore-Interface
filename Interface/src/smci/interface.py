"""The interface class itself."""
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

from ._common import (_CFG_FALSY, _CFG_TRUTHY, _cfg_bool, _z85_encode, _z85_decode, _FrameHeader,
                      _ReassemblyBucket, _RnsHeader, _ResolvedPath, _BindFrame, _CompletionFrame, _PeerRecord)
from ._locks import (_PreemptedForHandshake, _PriorityAsyncLock, _PriorityLockContext, _PriorityAsyncSemaphore,
                     _DutyCycleLimiter)
from ._direct import _DirectSendMixin
from ._paths import _PathDiscoveryMixin
from ._peers import _PeerStateMixin
from ._wire import _WireFormatMixin
from ._observability import _ObservabilityMixin
from ._config import _ConfigMixin

class SmartMeshCoreInterface(_ConfigMixin, _ObservabilityMixin, _WireFormatMixin, _PeerStateMixin, _PathDiscoveryMixin, _DirectSendMixin, Interface):
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

    _RX_LOG_WINDOW_FOREIGN_CAP = 20
    _RX_LOG_PAYLOAD_TYPE_TEXT_MSG = 2
    _RX_LOG_PAYLOAD_TYPE_PATH = 8

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
