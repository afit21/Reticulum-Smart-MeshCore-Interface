"""The interface class itself."""
import asyncio
import collections
import itertools
import queue
import threading
import time
import traceback

import RNS
from RNS.Interfaces.Interface import Interface

from ._locks import _PriorityAsyncLock, _DutyCycleLimiter
from ._config import _ConfigMixin
from ._observability import _ObservabilityMixin
from ._wire import _WireFormatMixin
from ._peers import _PeerStateMixin
from ._paths import _PathDiscoveryMixin
from ._direct import _DirectSendMixin
from ._reconcile import _ReconcileMixin
from ._routing import _RoutingMixin

class SmartMeshCoreInterface(_ConfigMixin, _ObservabilityMixin, _WireFormatMixin, _PeerStateMixin, _PathDiscoveryMixin, _DirectSendMixin, _ReconcileMixin, _RoutingMixin, Interface):
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

    _RX_LOG_WINDOW_FOREIGN_CAP = 20
    _RX_LOG_PAYLOAD_TYPE_TEXT_MSG = 2
    _RX_LOG_PAYLOAD_TYPE_PATH = 8

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
