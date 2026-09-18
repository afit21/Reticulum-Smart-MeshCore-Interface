"""
SmartMeshCoreInterface.py -- Smart Meshcore Interface for Reticulum

An `RNS.Interfaces.Interface` subclass that carries Reticulum (RNS) traffic
over a MeshCore LoRa mesh. This is a from-scratch rebuild -- see CLAUDE.md
and `docs/interface_architecture.md` (read that first; it links the rest of
the design set) for why the previous implementation
(`referenceprojects/MeshCore_Dynamic_Interface_old.py` /
`_original_repo.py`) was retired rather than extended: it accumulated real,
working functionality on top of one wrong assumption about MeshCore
(reliable multi-packet CHANNEL delivery) that field testing showed cannot be
tuned into working. Nothing in this file is built on that code; it is a
fresh implementation against the design docs, referring back to the old
implementation only as a record of what was tried and why it didn't work.

STATUS -- alpha 0.1.0 (2026-09-15). Complete: Milestones 0 through 6 of
`docs/interface_architecture.md`'s "Recommended build milestones" (M7,
cooperative broadcast, is deliberately skipped -- see this docstring's
own closing note for why), field-validated on real hardware, plus
several further field-driven fixes and optimizations found through real
alpha-0.1.0 use (search this docstring for "2026-09-15" for the dated
account of each; `changelog.md`'s `SmartMeshCoreInterface` section has
the summarized history). M0 built the scaffolding (base-class
contract compliance, the sync/async bridge, config loading, observability
plumbing). M1 added the wire format and bare single-fragment CHANNEL/
DIRECT send/receive. M2 added outgoing fragmentation for a packet too
large for one CHANNEL message (multi-fragment header shape, shuffled
fragment order, tiered inter-fragment spacing -- zero-hop/known-N-hop
tiers implemented and unit-tested, but fed `hop_count=None` for now since
no live topology data source exists until Milestone 4/5), incoming
reassembly (buffer keying, including the `0x40`/`"~coop"` branch
cooperative broadcast needs even though nothing sets that bit until
Milestone 7; bounded capacity with oldest-by-last-progress eviction; an
idle-since-last-fragment TTL, longer for `"~coop"` buckets; byte-identity
verification on a repeated `frag_idx`), and whole-packet dedup -- all per
`docs/reliability_engine_design.md` §1-2 and §5-7. DIRECT fragmentation
is still not implemented (Milestone 6): an incoming DIRECT frame with the
multi-fragment bit set is recognized and dropped with that reason, not
mis-parsed as CHANNEL's shape.

M3, this pass, adds priority queueing and give-up-signal handling
(`docs/reliability_engine_design.md` §3, §9), scoped exactly as the
architecture doc's own M3 bullet names it -- "the two-tier PriorityQueue,
the context-byte range check, and the `dest_type == LINK` retry-extra
classification":

  - **The outgoing queue is now a real two-tier `PriorityQueue`**
    (`PRIORITY_HANDSHAKE`/`PRIORITY_NORMAL`), replacing M1/M2's plain
    FIFO without changing the underlying queue-plus-executor-drain
    mechanism. `LINK_REQUEST`/`PROOF` packet types, and any packet whose
    context byte matches RNS's own give-up/connection-alive signals
    (`RESOURCE_PRF`/`RESOURCE_ICL`/`RESOURCE_RCL`, or the
    `KEEPALIVE(0xFA)..LRPROOF(0xFF)` range RNS core itself treats as one
    class -- confirmed directly against `RNS/Transport.py`'s own range
    check, not re-derived by guesswork), jump the queue ahead of ordinary
    `DATA`.
  - **This milestone also finally implements actual CHANNEL retry-pass
    scheduling**, deliberately deferred out of M2's scope: `_parse_rns_header`
    now classifies every outgoing packet (verified directly against the
    installed `RNS.Packet.unpack()`, not the old interface's own
    docstring paraphrase of the header layout, which grouped the flags
    byte's bits slightly differently even though its derived masks
    happened to still be correct), and `_retry_extra_for` picks the
    per-traffic-class extra-pass budget from
    `docs/reliability_engine_design.md` §2's table. **One deliberate
    simplification, flagged rather than silently guessed at: every
    `ANNOUNCE` gets the spontaneous-announce budget.** Distinguishing a
    path-response announce (its own, higher budget) needs to observe an
    in-flight path request -- peer/routing-adjacent state this interface
    doesn't have until Milestone 5. Extra passes are scheduled as
    independent background tasks with their own jittered delay
    (`retransmit_jitter_min_s`/`max_s`), never blocking the outgoing
    worker from moving on to the next queued packet.
  - Still no routing decisions or DIRECT target resolution (Milestone 5)
    -- `process_outgoing` still only ever sends via CHANNEL. Stale-queued-
    fragment dropping under backlog (§3's own age-plus-depth rule) isn't
    implemented either -- the architecture doc's M3 bullet doesn't name
    it, so it's left for whenever backlog pressure is actually a
    concern this design has data on, not built preemptively.

M4, this pass, adds native path discovery (`docs/path_discovery_spec.md`),
scoped exactly as the architecture doc's own M4 bullet names it -- "native
MeshCore path discovery integration, telemetry-permission granting,
contact persistence, and stale-path detection/reset":

  - **`discover_path()`** implements the doc's own function-level spec: a
    quick-retry burst (each attempt already naturally spaced by its own
    request/response wait, per `send_path_discovery_sync`'s own blocking
    shape -- no additional artificial delay layered on top), verifying a
    `PATH_RESPONSE`'s `pubkey_pre` actually names the peer queried before
    accepting it (the underlying wait isn't peer-filtered), then
    exponential per-peer backoff once the burst is exhausted. **Both
    logical-review fixes the architecture doc calls out by name are
    implemented**: a target already inside an active backoff cooldown is
    skipped with no transmission at all (rather than contributing another
    failed round to the same schedule it's already respecting), and this
    interface's own `_resolved_paths` record -- not a fresh device-table
    read -- is authoritative for its own routing/staleness decisions, so
    a device-persist failure can never discard a path just proven to
    work. **Deliberately NOT routed through `_run_command`/
    `self._command_lock`**: `send_path_discovery_sync` manages its own
    internal concurrency (a dedicated `_mesh_request_lock`) across a
    send-then-decoupled-wait-for-`PATH_RESPONSE` shape that can
    legitimately take several seconds -- serializing it behind this
    interface's own single command lock too would stall every other
    outgoing command for that whole wait, exactly what §3's "independent
    queues" principle argues against.
  - **Contact persistence**: a successful discovery is written back to
    the device's own contact table via `change_contact_path` (the
    firmware's own path-discovery handler returns before reaching the
    code path that would otherwise do this) -- including the "`- 1`"
    byte-length-to-mode-index conversion the design doc flags as easy to
    get backwards, and checking the persist call's own result rather than
    treating it as fire-and-forget.
  - **Telemetry-permission granting**, confirmed against the actual
    firmware source (`examples/companion_radio/MyMesh.cpp`'s
    `onContactRequest`, not the design doc's own paraphrase): this node's
    `telemetry_mode_base` is set to gate per-contact via that contact's
    `flags` bit 0x02, and a periodic contact-refresh loop grants that bit
    to known contacts. **One deliberate simplification, flagged rather
    than silently narrowed**: the design doc recommends granting only to
    peers confirmed via this interface's own binding protocol, which is
    Milestone 5's peer discovery -- not built yet. Every known contact is
    granted by default until then (`telemetry_grant_all_contacts`),
    exactly as open as any public MeshCore channel already is.
  - **Stale cached-path detection/reset** (§8): `record_direct_send_result()`
    and the reset it can trigger are implemented and directly unit-tested,
    but -- like `_send_direct()` since Milestone 1 -- not yet called from
    anywhere automatic. Milestone 5's routing decisions are what will
    actually drive a DIRECT send against a cached path and call this
    after each attempt.
  - A live, periodic contact-table read (`_contact_refresh_loop`) is
    reinstated per `reliability_engine_design.md` §2's "data-source gap"
    note -- feeds path discovery's own `ensure_contacts()` precondition
    now; Milestone 5 is expected to also feed this same freshness into
    the zero-hop/known-N-hop spacing tiers `_fragment_spacing_range()`
    already implements but still has no live data source for.

M5, this pass, adds peer discovery and routing decisions together, exactly
as the architecture doc's own M5 bullet pairs them -- "bind frames,
capability exchange, persistence, and opportunistic RNS-token learning,
paired with the CHANNEL-vs-DIRECT dispatch logic and the broadcast+DIRECT-
supplement pattern":

  - **Bind frames** (`docs/peer_discovery_design.md` §1-§3): a `"P"`-marker
    control frame, distinct from the `"R"`-marker RNS wire format, carries
    a protocol version, REQUEST/RESPONSE type, a capability bitfield, an
    attempt counter, and a 6-byte pubkey prefix. A peer is bound the
    moment one well-formed frame -- either type -- is parsed from it; both
    REQUEST and RESPONSE convey identical content. One bootstrap REQUEST
    is sent unconditionally at every process start regardless of what the
    peer cache restored (the old design's own regression, made
    structurally impossible here rather than merely avoided), with an
    optional slow repeat while still below `peer_discovery_target_peers`.
    Every well-formed REQUEST heard gets a RESPONSE, after per-responder
    jitter (collision/half-duplex-deaf-repeater spacing, not suppression)
    and subject to a much longer global minimum re-response interval
    (real suppression: this node's own capability hasn't changed just
    because a second REQUEST arrived soon after the first RESPONSE).
  - **Capability** (§2): a bitfield (bit 0 = `has_upstream_rns`), not the
    old design's single router/edge character. **Structurally enforced,
    not just documented**: capability is only ever written by
    `_handle_incoming_bind_frame`, parsing an actual bind frame -- no
    other code path (a native contact-table event, cache restore without
    an explicit stored value) can set or infer it, the fix for a real
    shipped bug where a MeshCore contact field that didn't exist
    (`can_route`) silently defaulted `True` on every refresh.
  - **Persistence** (§4): the bound-peer set (pubkey prefix, capability,
    last-seen) is persisted to a small JSON file under
    `RNS.Reticulum.storagepath` by default, and restored at startup
    through the exact same single entry-point function
    (`_register_peer`) every other way a peer becomes known goes
    through -- the old design's `force_direct_path` bug traced to
    exactly two divergent "peer becomes known" code paths drifting
    apart, so this design has only one. Telemetry-grant state and
    RNS-token bindings are deliberately NOT persisted (cheap to rebuild,
    risky to resurrect stale, per §4/§7's own reasoning).
  - **Telemetry-permission granting is now bind-gated** (§5), superseding
    Milestone 4's `telemetry_grant_all_contacts`-default-on
    simplification, which was explicitly flagged there as a placeholder
    for exactly this: granted on first bind-frame parse, re-granted
    idempotently every time a peer becomes known (including cache
    restore), never gated behind any further handshake step.
    `telemetry_grant_all_contacts` still exists, defaulting off now, as
    an explicit escape hatch back to the old open behavior.
  - **Opportunistic RNS-token learning** (§7) is wired at the one place
    it can structurally work: the DIRECT receive path. A CHANNEL `"R"`
    frame carries no sender pubkey at all, so `_observe_incoming_rns_packet`
    is only ever called from `_handle_incoming_frame`'s DIRECT branch,
    via `_canonical_peer_prefix` (MeshCore-native prefixes of possibly-
    differing lengths are never compared directly against this
    interface's own 6-byte peer-key convention). Populates a normal
    `destination_hash -> peer_pubkey` table plus a separate, short-TTL
    PROOF-correlation table keyed by each delivered packet's own
    truncated hash (confirmed byte-for-byte against a real
    `RNS.Packet.generate_proof_destination().hash` while this milestone
    was built) -- the fix for the PROOF exception §7 documents: a
    PROOF's destination-hash field is the hash of the packet it proves,
    never a stable per-peer identity, so it can never land in the normal
    table no matter how much traffic is observed.
  - **Routing decisions** (`docs/routing_decisions.md`), wired into
    `_send_outgoing_packet`'s now-real dispatcher: a path request
    (DATA+PLAIN) always broadcasts and additionally fires a DIRECT
    supplement, concurrently and never gated on the broadcast's own
    outcome, to a capped number of known router-capability peers with an
    already-resolved path. Everything else besides ANNOUNCE goes DIRECT,
    unconditionally, the moment this interface's own `_resolved_paths`
    record (never a fresh device-table read) shows a resolved path to
    the packet's peer -- resolved via the token tables above -- with no
    CHANNEL fallback on a DIRECT failure (DIRECT is primary once
    resolved, not a supplement); repeated failures feed Milestone 4's
    already-built stale-path detection through a new
    `_send_direct_and_await_ack` helper that waits for the real
    firmware ACK (correlated by the `meshcore` library's own
    `expected_ack`/`EventType.ACK` `code`-attribute mechanism -- genuine
    per-request correlation, confirmed against the installed library's
    `send_msg_with_retry`, unlike the bare-type-only matching invariant
    #2 warns against elsewhere) rather than treating a locally-queued
    send as success. **One deliberate, total deferral, not a partial
    one**: the path-response-announce DIRECT supplement
    `routing_decisions.md` describes needs DIRECT fragmentation
    regardless (an announce never fits one message) -- Milestone 6 --
    and this interface has no way to even detect "this outgoing
    ANNOUNCE is answering peer X" without RNS core exposing that
    context, so every ANNOUNCE still broadcasts only, exactly as
    Milestones 1-4 left it.

**Field-testing M5 found a real, previously-undetected bug affecting
every milestone's receive path, not just this one**: `_start_auto_
message_fetching()` (called from `_async_setup`, right after `
_subscribe_data_events()`) fixes it. The `meshcore` library never pushes
`CHANNEL_MSG_RECV`/`CONTACT_MSG_RECV` on its own -- the firmware queues
incoming messages and only notifies a connected client that something is
waiting (`EventType.MESSAGES_WAITING`); actually reading a queued
message off the device (`commands.get_msg()`) requires an explicit
client request. Without calling the library's own `start_auto_message_
fetching()` helper (which subscribes to `MESSAGES_WAITING` and drains
with `get_msg()` every time one fires, plus does one immediate check at
connect time), this interface's own `_on_channel_msg_recv`/`
_on_contact_msg_recv` subscriptions -- correct in themselves -- would
simply never fire for real incoming traffic. Confirmed live during this
milestone's own field test: two real MeshCore radios exchanged bind
frames and a DIRECT test payload that physically arrived and sat queued
on each receiving device (readable directly via `commands.get_msg()`)
while a connected-but-not-fetching interface instance received nothing
at all -- for however long this bug had been present, going back to
Milestone 1. PATH_RESPONSE/ACK/OK/ERROR are unaffected by this fix
either way, since the device pushes those directly as part of the
request/reply they answer, never gating them behind this queue-and-poll
message mechanism -- which is also why Milestone 4's own path-discovery
field test could pass despite this bug already being present. See
`_start_auto_message_fetching`'s own docstring for the full mechanism.
Once fixed, a real zero-hop (direct RF neighbor, no repeater) field test
between two physical MeshCore radios confirmed the entire M5 chain
working end-to-end: bind-frame peer discovery, `discover_path()`
resolving in one attempt at `out_path_len=0`, the routing dispatcher
sending a DATA/SINGLE packet DIRECT, a real firmware ACK correlating
correctly via `_send_direct_and_await_ack`, and §7's opportunistic
token/PROOF-correlation learning populating for real off that same
DIRECT receive.

M6, this pass, adds DIRECT fragmentation and folds stale-path detection/
reset fully into routing, exactly as the architecture doc's own M6 bullet
names it -- "the rarer DIRECT-needs-fragmenting shape, its one-fragment-
at-a-time ACK-gated sequencing... and folding stale-path reset fully into
routing":

  - **DIRECT fragmentation** (`docs/wire_format_design.md`): a DIRECT
    packet too large for one message falls back to the identical
    multi-fragment header shape CHANNEL already uses (`_encode_channel_
    multifragment`, reused directly rather than duplicated -- the doc's
    own claim that the shapes are byte-identical), sent via `send_msg`
    instead of `send_chan_msg`. Rare in practice (constraint one:
    everything but ANNOUNCE, which never goes DIRECT in this design,
    comfortably fits one DIRECT message) but handled correctly when it
    happens, e.g. a large Resource-transfer DATA packet.
  - **The two-pass structure** (`docs/reliability_engine_design.md` §4,
    the logical review's own fix for a gap that section had left
    unstated): pass 0 sends every `frag_idx` in order, one at a time --
    never several back-to-back without waiting, enforced for free by
    fully awaiting each fragment's own send+ACK(+retry) cycle before
    starting the next, the same half-duplex-derived discipline CHANNEL's
    own spacing already reflects. Pass 1 re-attempts only whatever never
    got ACKed in pass 0. This is what makes DIRECT fragmentation's real
    structural advantage over CHANNEL's blind full-set resend -- only
    re-driving what's actually still missing -- concretely true rather
    than just claimed.
  - **The outer `direct_send_attempts` retry loop** (§4) is now real and
    shared: it applies identically to a bare single-message DIRECT send
    and, per-fragment, to each fragment of a DIRECT-fragmented send, via
    one new `_send_direct_with_attempts` helper both paths call. Exactly
    one `record_direct_send_result` call per invocation -- success on
    the first ACK, failure once the whole budget is exhausted -- never
    one per individual attempt, per §4's own fix note about a fragment
    exhausting its own attempt budget counting as one failure regardless
    of other fragments' outcomes. For the bare shape (whose own encoding
    never varies by attempt, per design -- that's the firmware's job)
    this drives the firmware's own `send_msg(..., attempt=...)`
    parameter instead; for the fragmented shape both this interface's
    own header attempt byte and that same firmware parameter vary
    together.
  - **Stale-path detection is now fully integrated into routing** (§8):
    `_send_direct_packet` no longer falls straight to CHANNEL broadcast
    the moment a peer has no *currently* resolved path -- it first tries
    `discover_path()` (via a new `_discover_path_coalesced` wrapper that
    shares one in-flight discovery attempt across concurrent callers for
    the same peer, rather than each independently kicking off its own
    quick-attempts burst) and only falls back to broadcast if discovery
    itself also fails. This closes the loop Milestone 4's own
    `record_direct_send_result`/`_reset_stale_path` opened but never
    finished: a reset path is no longer a dead end for that peer until
    some unrelated event happens to re-resolve it -- the very next
    DIRECT-primary send attempt is what drives recovery, exactly as §8
    specifies. The dispatcher-level gate this replaced (Milestone 5's
    `peer_prefix in self._resolved_paths` check, before ever calling
    `_send_direct_packet` at all) is gone -- a known peer always reaches
    `_send_direct_packet` now, which is the one place that decides
    resolved-vs-discover-vs-broadcast.

**Field-testing M6 over a real zero-hop link found a second real gap,
not covered by any remaining milestone (M7 is scoped entirely to ANNOUNCE
distribution, not point-to-point routing), so it's fixed here rather than
deferred**: peer_discovery_design.md §7's opportunistic RNS-token learning
only ever learns from an incoming DIRECT receive -- correct, since a
CHANNEL frame carries no sender identity at all and there's nothing safer
to learn from -- but that meant two bound peers that had never yet
exchanged a single DIRECT message had no way to ever originate one:
neither side has a token to route DIRECT with, and nothing ever creates
one without a DIRECT receive happening first. Confirmed live: a whole
MeshChat conversation between two freshly-bound zero-hop nodes stayed on
`resolved_paths=0`/`rns_tokens_learned=0` for its entire session, meaning
every message rode CHANNEL's multi-fragment path (this project's own
long-documented reliability cliff) even though a working DIRECT link
was one hop away the whole time.

Fix, extending the same broadcast+DIRECT-supplement pattern
`routing_decisions.md` already specifies for path requests rather than
loosening any identity assumption: `_send_outgoing_packet` now pairs the
mandatory broadcast for case-3 (DATA/SINGLE, LINK_REQUEST, PROOF) traffic
with no known token with a DIRECT-bootstrap-supplement to a capped number
of bound peers (`_select_bootstrap_supplement_targets`, any bound peer --
unlike the path-request case, capability doesn't matter here, since the
question isn't "who can route this" but "which bound peer might actually
be the counterpart"). This adds no new exposure beyond what the broadcast
already does -- RNS's own end-to-end encryption protects content
regardless of transport, so a DIRECT copy landing on a bound peer who
isn't actually the intended recipient is exactly as cryptographically
inert to them as the CHANNEL copy they were already going to receive.
It's also self-limiting: one successful delivery, from either side,
teaches the recipient a real token immediately via §7's existing
mechanism, and ordinary DIRECT-primary routing takes over for that
destination from then on -- the supplement never fires for it again.
`_register_peer` also now proactively kicks off `discover_path()` the
moment a peer is freshly bound, rather than only reactively on the first
outgoing send, so the very first bootstrap attempt doesn't also have to
wait out a fresh discovery burst. See `_send_direct_payload` (the new
shared bare-or-fragmented dispatch both the DIRECT-primary and every
DIRECT-supplement path now call through) and `_send_direct_supplement`'s
own `trigger_discovery` parameter for the implementation.

**The same field-test session found a second, more basic gap: DIRECT
fragmentation's receive side had never actually been built.** M6's own
send side (`_send_direct_fragmented` and everything under it) was
implemented and tested, but the *receive* path still carried the
original Milestone 1 stub -- an incoming DIRECT frame with the multi-
fragment bit set was recognized and unconditionally dropped with a
"DIRECT fragmentation is not implemented yet (Milestone 6)" log line,
even now that Milestone 6 was the milestone implementing it. Confirmed
live: a peer's larger DIRECT-fragmented sends (2 fragments) were dropped
on every single delivery attempt across multiple retries, each logged
individually, while the connection otherwise worked fine. Fixed by
routing an incoming DIRECT multi-fragment frame
(`_handle_direct_multifragment_frame`) through the exact same
reassembly/dedup machinery (`_add_channel_fragment`, now returning the
completed payload instead of calling `process_incoming` itself, so both
the CHANNEL and DIRECT callers can add their own post-completion step)
CHANNEL's own multi-fragment path already used since Milestone 2 --
safe to share because DIRECT's own `sender_token` (a peer's
cryptographically-attested `pubkey_prefix`) can never collide with
CHANNEL's (a node's own plaintext, unauthenticated name) for the same
physical node. The one real difference from CHANNEL: once a DIRECT
reassembly completes, §7's opportunistic token learning runs on the now-
complete packet -- something CHANNEL's own multi-fragment path
structurally can't do at all, and deliberately stays out of the shared
helper for exactly that reason.

**A third, smaller gap the same field session surfaced**: `_proof_
correlation` (§7) had no periodic reclaim of its own, unlike `_dedup`/
`_reassembly`, both already swept every `REASSEMBLY_CLEANUP_INTERVAL_S`
in `_reassembly_cleanup_loop`. Its only expiry path was lazy -- checked
on the exact lookup that would consume an entry
(`_resolve_routing_peer`'s PROOF branch) -- but most delivered packets
never actually get proved (an RNS Link doesn't PROOF every DATA packet),
so an entry with no matching PROOF ever coming back had no way to ever
be reclaimed. Confirmed live: `proof_correlations_pending` sat perfectly
flat for minutes of real traffic in the same session -- the exact
signature of a table with no time-based cleanup. Fixed by adding
`_proof_correlation_sweep`, called from the same periodic pass `_dedup`
already used.

**M7 (cooperative broadcast) is deliberately skipped, not deferred.**
Per the architecture doc's own framing, M7 was always optional and
last, resting entirely on an untested hypothesis
(`cooperative_broadcast_design.md`'s central open question), with an
explicit instruction that "a fully working interface from M0-M6 should
never be considered blocked on this milestone's outcome." M0-M6 is
that fully working interface, field-validated at zero-hop with three
real bugs found and fixed through actual use during this build. The
`0x40`/`"~coop"` reassembly-key branch M2 already built for M7's sake
(see the module docstring's M1/M2 notes and `_reassembly_key`) is
harmless dead code in this shape -- nothing ever sets that bit -- and
is left in place rather than torn back out, since removing a correct,
already-unit-tested branch to chase a milestone that was never started
would be pure churn for no benefit.

**Post-M6 field-driven addition: the small-mesh DIRECT-only rule
(2026-09-15).** Real zero-hop field testing surfaced that CHANNEL
traffic (ANNOUNCE, path requests, and the no-known-token fallback) was
the dominant source of airtime for a small, fixed-topology deployment --
and this project's own field data (`reliability_engine_design.md`'s
open question 1) already shows CHANNEL is the *less* reliable transport
of the two, not just the noisier one. `_in_small_mesh_mode()` /
`SMALL_MESH_DIRECT_ONLY_MAX_PEERS` (default 2) makes this self-tuning
rather than a config knob the user has to set and maintain: with 1-2
bound peers there's no ambiguity about who any of this traffic is for,
so it goes DIRECT to every bound peer instead of over CHANNEL at all
(`_send_direct_to_all_peers`), for all three of `_send_outgoing_packet`'s
routing branches -- including ANNOUNCE, which never went DIRECT before
this. Deliberately excludes the 0-bound-peer case (CHANNEL remains the
only way this node's very first peer can ever be discovered) and
re-evaluates on every send rather than caching, so the mesh growing past
2 peers reverts to the original broadcast(+capped supplement) behavior
automatically. Trade-off, surfaced rather than silently accepted: an
ANNOUNCE in small-mesh mode now only ever reaches this node's own bound
peers, never a not-yet-bound third party that might be passively
listening on CHANNEL -- judged acceptable for a small, known,
bind-frame-authenticated topology, where that discoverability was never
actually being used.

**Post-small-mesh field-driven fix: DIRECT exchanges now serialize
interface-wide (2026-09-15).** Real MeshChat usage (several messages
sent close together) surfaced that `_send_direct_packet`/
`_send_direct_supplement`, both spawned as independent background tasks
by design, had nothing preventing two of them from running their own
send-then-wait-for-real-ACK cycle concurrently -- letting this node's
radio transmit a second DIRECT frame while the first one's ACK was still
in flight. That's exactly the half-duplex collision this design's own
"DIRECT-fragmented send sequencing: one fragment in flight at a time"
invariant was meant to prevent, just never extended across concurrently-
spawned top-level sends, only within one's own pass-0 loop. Symptomatic
in the field as six separate pkt_ids all needing pass-1 re-drives in the
same ~30s window, repeated "no real delivery ACK" warnings, and
cascading stale-path resets on both ends -- concurrent DIRECT exchanges
colliding with each other, not a single slow link. Fixed with a new
`_direct_exchange_lock`, held for the full send+ACK-wait duration in
`_send_direct_frame_and_wait_for_ack` (see that method's own docstring
for why this is a separate lock from `_command_lock`, and why it's a
single interface-wide lock rather than one per peer -- there is exactly
one physical radio).

**User-requested addition: optional packet capture (2026-09-15).** Off
by default (`packet_capture_enabled`); when on, every in/out RNS packet
this interface handles is appended as one JSON line to a file under
`packet_capture_dir` (default: `<RNS storage path>/packet_capture/`) --
packet classification (type/destination type/context, with friendly
names), the routing decision made for outgoing packets (which of
`_dispatch_outgoing_packet`'s branches fired, and the target/candidate
peer(s) if any), transport and sender attribution for incoming packets
(DIRECT's authenticated `sender_peer_prefix` kept structurally distinct
from CHANNEL's unauthenticated `channel_sender_claimed`, per this
interface's own security model), size, and both wall-clock and
monotonic timestamps. See `_capture_event`/`_capture_outgoing`/
`_capture_incoming`'s own docstrings for the full record format and why
the writes are deliberately synchronous (this transport's own real
throughput ceiling makes that a non-issue here, unlike CHANNEL/DIRECT
sends).

**Field-diagnosed fix, found via the packet capture above (2026-09-15):
unknown-destination DIRECT-bootstrap attempts now back off.** Real usage
(ReticulumMeshChat running headless, periodically trying to sync an LXMF
propagation node) showed a destination this node has no token for
getting a full DIRECT-bootstrap attempt -- LINKREQUEST out, nothing ever
comes back -- every single time something addresses it, forever, with
no memory of the repeated failure. Confirmed in the capture: four full
Link-establishment attempts to the same destination in one 8-minute
idle window, none ever answered (that propagation node simply isn't
reachable through this node's bound peer at all). This interface has no
positive "it failed" signal at its own layer (a real MeshCore ACK from
the peer only confirms *local* delivery, never that whatever it's being
asked to relay ever replied) -- so `_record_unknown_dest_attempt` uses
the same proxy already implicit elsewhere in this design: repeated
attempts with §7's token still never learned for that exact destination.
After `UNKNOWN_DEST_BOOTSTRAP_FAILURE_THRESHOLD` attempts (mirroring
path discovery's own `path_discovery_quick_attempts` default), back off
with the same doubling-capped shape path discovery's own backoff uses --
see the class constants and `_unknown_dest_in_backoff`/
`_record_unknown_dest_attempt`/`_clear_unknown_dest_backoff`'s own
docstrings. Learning a real token for that destination (§7) clears the
backoff immediately, so a destination that becomes reachable later isn't
blocked forever. Scoped to case 3's "no known peer" branch only -- never
ANNOUNCE (no reply is ever expected for one's own outgoing announce) or
path requests (whose own `destination_hash` is a shared PLAIN pseudo-
destination, not the actual queried target, so per-destination backoff
wouldn't even apply meaningfully there).

**Field-diagnosed fix, found jointly with another Claude session working
a real 2-hop repeater field test (2026-09-15): outgoing PATH_RESPONSE is
now rate-limited per destination.** An outgoing PATH_RESPONSE is RNS
Transport's own answer to a PATH_REQUEST it decided, on its own, to
answer -- this interface has no say over whether one gets generated, only
over how many times it actually keys the shared half-duplex radio to
transmit it. Packet-capture evidence from that field test found a remote
NomadNet client stuck in a tight connection-retry loop, re-issuing
PATH_REQUESTs for the same destination every 4-8s while its own link
attempt kept failing -- well under `RNS.Transport`'s own
`PATH_REQUEST_MI=20s` floor, confirming these were explicit
`request_path()` calls from that client, not ordinary path aging. RNS
answered every single one, each one a real multi-fragment ANNOUNCE
transmission, landing squarely in the same ~10-minute window six unrelated
DIRECT DATA sends were failing to get through. This interface can't fix
the remote client's retry loop, but it can stop re-spending airtime
re-answering a question it already just answered: `_path_response_rate_
limited` drops a repeat PATH_RESPONSE for the same destination within
`PATH_RESPONSE_RATE_LIMIT_WINDOW_S` of the last one actually sent -- the
destination's path/identity cannot plausibly have changed that fast, and
the requester already has a reply in flight.

**User-requested fix (2026-09-15), generalized the same day into a
flat post-send listen window applied after *every* DIRECT attempt.**
First cut: a DIRECT send that got no ACK waited out a short randomized
delay before its *own* next retry attempt only (0.5-2.0s), on the
reasoning that retrying instantly after a collision just repeats the
same collision window. A second real 2-hop field test then surfaced the
sharper version of the same problem one layer up: `_direct_exchange_lock`
released the instant one attempt resolved (ACKed or not), letting the
very next contender -- a different queued message, not just a retry of
this one -- key the radio again immediately, with the repeater possibly
still settling from the previous exchange. Rather than add a second,
separate inter-message delay on top of the existing per-retry one, the
two were unified per direct user request into one flat rule: **every**
attempt of **every** fragment, ACKed or not, first or last, now sleeps a
random `direct_post_send_listen_min_s`/`max_s` (0-5s by default -- "we
can tune this later," direct user words) before
`_send_direct_frame_and_wait_for_ack` returns -- and critically, while
still holding `_direct_exchange_lock`, so it's a real quiet window on the
shared radio, not just a delay this one caller happens to observe. This
also means a fully successful send now pauses before releasing the lock
too, trading a little latency on the happy path for a consistent,
easy-to-reason-about rule instead of a patchwork of "delay here, not
there" special cases -- deliberately coarse for now, a first pass meant
to be tuned down (or made outcome/hop-count-sensitive) once real
capture data from the next field test shows what's actually needed.
Because this is an `await asyncio.sleep()` on the interface's own event
loop, it doubles as a real window for this interface to actually receive
anything that was in flight, the same as it already does during the
ACK-wait timeout itself.

**User-requested fix (2026-09-16): the flat post-send listen window
above is now split by outcome, one day after it shipped, once a real
zero-hop NomadNet field session's own packet capture showed its actual
cost.** All 176 DIRECT attempts captured across both machines that
session were ACKed -- 100%, exactly as expected at zero-hop, nothing to
collide with -- yet every one of them still paid the full flat 0-5s
random tax (averaging ~2.5-2.9s) before the lock released. Worse, this
compounds: NomadNet's page transfer spawns many small RESOURCE-related
packets that all queue up behind `_direct_exchange_lock`, and the
capture showed `_direct_exchange_queue_depth` reaching 14 with
individual attempts waiting up to **48 seconds** just for their own
turn, purely because everything ahead of them was "listening" after a
send that had already succeeded. None of that bought any real collision
protection -- nothing was colliding. Fixed: a missed ACK (the one case
where "something might have collided" is a live, evidence-backed
hypothesis) still draws from the full `direct_post_send_listen_min_s`/
`max_s` range; a real ACK -- itself direct evidence the channel was
clear -- now draws from the much smaller `direct_post_send_listen_
success_min_s`/`max_s` (0-0.5s default) instead. Both stay genuinely
randomized ranges, never a fixed value or a hard skip, per direct user
instruction ("the waits should still be random to avoid the interfaces
getting stuck in loops") -- the fix narrows the success-case range
rather than special-casing it away to zero.

**User-requested architectural fix (2026-09-16), following a real 1-hop
repeater field test: `_direct_exchange_lock` is now priority-aware.**
That test (peer's own laptop-side + this node's cross-checked capture,
diagnosed jointly with another Claude session) found real ~50% RF loss at
one hop and `_direct_exchange_queue_depth` reaching 14, with individual
DIRECT attempts waiting up to 94s just for their own turn at the lock.
Asked directly whether this design should keep being tuned or reconsider
the underlying architecture given a half-duplex, high-latency, lossy
medium: the answer was to keep DIRECT-primary (the fixes above are all
real and worth keeping) but recognize that a *plain* `asyncio.Lock`
treats every DIRECT exchange as equally urgent -- a LINK_REQUEST/PROOF-
class exchange (already tagged `PRIORITY_HANDSHAKE` at the *outer*
two-tier outgoing queue, docs/reliability_engine_design.md §3) arriving
while a deep backlog of ordinary DATA/RESOURCE-fragment retries is
already queued for this same lock has no way to jump that backlog once
queued -- a plain lock has no "insert ahead" operation, only FIFO. Since
establishing (or proving) a Link is usually the actual gate everything
else is waiting on, that outer priority distinction was being silently
discarded the moment multiple DIRECT sends started contending for the
radio. Fixed: `_direct_exchange_lock` is now a `_PriorityAsyncLock` (see
that class's own docstring for the full design and cancellation-safety
reasoning) -- a higher-priority waiter is served before an earlier-
arrived lower-priority one, FIFO within a tier, threaded down through
the whole DIRECT send call chain (`_send_direct_packet`/`_send_direct_
supplement`/`_send_direct_to_all_peers`/`_send_direct_payload`/
`_send_direct_fragmented`/`_send_direct_with_attempts`) via each call
site's own already-available `header` (`_priority_tier(header)`, the
same classification the outer queue already used). Scoped deliberately:
this reorders *waiting*, it doesn't reduce total demand on the shared
radio or change how many attempts a message gets -- the "should this
layer's own retry budget shrink in favor of RNS's own Resource-level
recovery" question raised alongside it remains open, not addressed here.

**User-requested fix (2026-09-16), the same day: that open question was
answered -- `direct_send_attempts`' own default lowered from 3 to 2.**
Directly requested after the confidence discussion above: under the same
real ~50% measured 1-hop loss, a fragment that exhausts its attempt
budget without an ACK still holds `_direct_exchange_lock` (priority-
aware now, but still one physical radio) for the full cost of each
failed attempt -- ack_timeout plus the post-send listen window -- before
anything else queued behind it gets a turn. A fragment that needs a 3rd
attempt under these conditions is no better served by getting one here
than by falling through to `_send_direct_fragmented`'s own existing
pass-1 re-drive (a fresh attempt budget, not a continuation of a failing
one), or, above this interface entirely, RNS's own Resource-transfer
layer re-requesting specifically-missing parts once a transfer stalls --
both mechanisms already exist and have equal or better information about
what's actually still missing. Still a flat value, not adaptive to
measured loss -- see that config value's own comment for why an
adaptive version was deliberately not attempted without more field data
to justify the added complexity.

**User-requested batch (2026-09-16), synthesized across all three field
tests run that day: four further reliability/airtime fixes, all
implemented and deployed together.**

1. *Handshake-class exchanges get a larger attempt budget than ordinary
   DATA, not the same reduced one.* `direct_send_attempts_handshake`
   (default 4) applies when `_send_direct_with_attempts`'s own `priority`
   parameter is `PRIORITY_HANDSHAKE`; `direct_send_attempts` (default 2)
   applies otherwise. See `direct_send_attempts_handshake`'s own comment
   for why a failed Link handshake -- confirmed, via direct RNS.
   Transport.py source inspection, to force a full path rediscovery and
   feed a self-reinforcing congestion loop -- deserves more persistence
   than a failed DATA fragment, which pass-1 re-drive or RNS's own
   Resource-layer recovery already exist to pick up cheaply.
2. `PATH_RESPONSE_RATE_LIMIT_WINDOW_S` *raised from 10s to 20s* once
   real 1-hop/2-hop field data showed the original window's actual
   coverage against a real client's observed retry cadence (mostly
   15-26s, not the tighter 4-8s that motivated the original 10s value).
3. *A third priority tier, `PRIORITY_LOW`, added for PATH_RESPONSE
   specifically.* `_priority_tier` now returns it for any
   `context == RNS.Packet.PATH_RESPONSE` packet -- pure housekeeping,
   below both `PRIORITY_HANDSHAKE` and `PRIORITY_NORMAL` at
   `_PriorityAsyncLock`, so it can never block real user data even
   during a client retry storm rate-limiting alone can't fully suppress
   (field data: 17 real PATH_RESPONSE transmissions in one ~6.5-minute
   window despite the rate limit).
4. *A global transmit duty-cycle cap, direct user instruction:* "all
   interfaces should spend the majority of their time listening...
   transmit should only take up like a max of 30% of total airtime in a
   10 second period." `_DutyCycleLimiter` (see its own docstring for the
   full design -- why airtime is estimated from `bitrate` rather than
   measured, why the wait loop wakes exactly when room frees up rather
   than polling) enforces this across *every* actual radio-keying
   command this interface issues -- CHANNEL fastpath/multi-fragment
   sends, DIRECT sends, bind frames alike, via `_throttle_for_duty_
   cycle` at each of their own call sites -- not scoped to any one
   transport shape or priority tier, since there's exactly one physical
   radio underneath all of them. `duty_cycle_enabled` (default on),
   `duty_cycle_window`/`duty_cycle_max_fraction` (10s/0.30, matching the
   user's own numbers exactly) are all configurable; the defaults are a
   precautionary ceiling, not derived from a specific field measurement
   the way most of this docstring's other numbers are.

**User-requested observability addition (2026-09-15): two new packet-
capture record kinds, `direct_attempt_result` and `direct_send_result`,**
added specifically because the 2-hop field test above needed information
this capture didn't yet carry -- out_path_len/out_path_hex (the hop path
actually resolved for a DIRECT peer) were previously visible only in a
one-off `RNS.log` INFO line at the moment a path was freshly discovered,
never at the moment a message using that (possibly long-cached) path
actually succeeds or fails; and there was no way to tell, from the
capture alone, which individual attempt (of a fragment, of a message)
got ACKed versus timed out, or how contended `_direct_exchange_lock` was
at that moment. `_capture_direct_attempt_result` (one record per
individual DIRECT send attempt -- bare, or one fragment of a fragmented
send) carries `peer_prefix`, `attempt`, `ok`, `queue_depth` and
`lock_wait_s` (`_direct_exchange_queue_depth`'s own live contention
count and how long this attempt waited its turn -- direct evidence for
or against "several concurrent messages splitting the one shared radio"
the next time that's a live hypothesis), `ack_timeout_s`, and (when
applicable) `pkt_id`/`frag_idx`/`frag_total`. `_capture_direct_send_
result` (one record per whole DIRECT message, after both fragmentation
passes if any) pairs the final ACKed/not-ACKed outcome with
`out_path_len`/`out_path_hex`. Both carry an `event` field absent from
every existing packet in/out record, so a script already written against
this capture format can keep ignoring records it doesn't recognize.

**User-requested addition (2026-09-16): every capture record now carries
`hop_count` too, not just the final `direct_send_result` summary.** The
stated goal is building tuning profiles per hop count/scenario from real
field data -- e.g. bucketing retry counts, ack timeouts, and listen
delays by hop count -- which the per-attempt `direct_attempt_result`
record couldn't support without cross-referencing it against the
matching `direct_send_result` by hand. `_capture_direct_attempt_result`
now carries the same `out_path_len` the caller (`_send_direct_packet`/
`_send_direct_supplement`) already had in hand from `_resolved_paths`,
threaded down through `_send_direct_payload`/`_send_direct_fragmented`/
`_send_direct_with_attempts` as a plain `hop_count` parameter -- never
re-resolved at the attempt layer itself, so it always reflects whatever
path the caller actually used for this send. `_capture_incoming` also
gained `hop_count`, best-effort: this interface's own resolved outbound
path length to `sender_peer_prefix` at receive time, since MeshCore
never reports the actual inbound path a given DIRECT frame took -- only
what this node currently has resolved for sending back to that peer.
`None` on any record where no path is resolved yet (unauthenticated
CHANNEL traffic, or a DIRECT exchange still establishing its first
path) -- never coerced to 0, which would be indistinguishable from a
genuine zero-hop link.

**User-requested fix (2026-09-15, post-alpha-0.1.0 2-hop field test):
stale-path reset now respects a minimum path age.** `direct_path_reset_
threshold`'s default (2) meant just two consecutive full-timeout DIRECT
failures discarded a cached path and forced a fresh `discover_path()`
burst (up to `path_discovery_quick_attempts` real over-the-air round
trips) on the very next send -- with no floor on how recently that exact
path was itself successfully confirmed. Asked directly whether the
interface might be "too trigger-happy with path requests once a message
fails": yes, confirmed against the code -- when several DIRECT sends to
the same peer fail close together for a reason that has nothing to do
with the path itself (shared-radio congestion, several messages queued
behind `_direct_exchange_lock`, a repeater mid-relay -- exactly the real
2-hop field test scenario), this could re-discover a path confirmed only
seconds earlier and re-run `reset_path()` on the device, adding real
path-discovery airtime on top of an already-congested channel: a self-
inflicted feedback loop, not a genuine stale-path recovery. Fixed:
`record_direct_send_result` now checks `_ResolvedPath.resolved_at`
against `direct_path_reset_min_age_s` (30s default) before resetting --
a path confirmed more recently than that is trusted regardless of
accumulated failures. The failure counter is deliberately NOT cleared by
this skip, so a path that's genuinely gone bad still gets torn down and
rediscovered the moment it's old enough for that to be plausible, just
not before.

**User-requested addition (2026-09-16), the same real zero-hop hardware
session that found the duty-cycle miscalibration above: defer
transmitting for a while after hearing a DIRECT frame.** Direct user
request: "if we hear a message come in via direct, we wait 3 seconds to
hear another before we send again... wait for the incoming interface to
either stop sending or hit its airtime limit." This interface has no
real-time channel-busy/CAD signal from the `meshcore` library (confirmed
-- nothing exposes that), so a received DIRECT frame is the best
available proxy for "someone else is transmitting nearby right now."
`_wait_for_incoming_quiet` (see its own docstring for the full design)
is called immediately before `_throttle_for_duty_cycle` at every one of
that method's own call sites (CHANNEL fastpath/multi-fragment sends,
DIRECT sends, bind frames) -- complementary to, not a replacement for,
the duty-cycle cap: that one throttles based on this interface's own
recent transmit history, this one defers based on what it just *heard*,
specifically to avoid keying the radio into the middle of a peer's own
multi-fragment DIRECT burst. `incoming_quiet_window_s` (3.0s, matching
the user's own number exactly) is a rolling window -- hearing another
DIRECT frame while already waiting pushes the deadline out again,
mirroring "wait for them to stop sending." Since this interface can't
actually observe a peer's own airtime budget, "or hit its airtime limit"
is approximated by `incoming_quiet_defer_max_wait_s` (15.0s default) --
a bound on this node's own patience, not a measurement of the other
side's real limit, so a continuously-chatty peer can never starve this
node's own outgoing traffic indefinitely. `_last_incoming_direct_at` is
updated in `_on_contact_msg_recv` for *every* DIRECT frame heard on the
channel, before the marker check even runs -- true even for a frame that
turns out malformed or not addressed to this node, since "the channel
was just occupied" doesn't depend on the frame being decodable.

**Code review pass (2026-09-16): one real deadlock and several silent-
failure gaps found and fixed, all verified against actual behavior
rather than assumed.** Highest severity: `_PriorityAsyncLock.acquire()`'s
`CancelledError` handler could leave `_locked` stuck `True` forever with
no owner -- reachable when a waiter is granted ownership and cancelled
in the same instant with no further waiters queued for any tier, since
that branch called `_wake_next()` without checking its return value the
way `release()` already does. Only reachable via `detach()`'s task-
cancel sweep in this interface's own steady-state code, but a real
deadlock of every future DIRECT send once hit. Also fixed: (1) both
`ensure_contacts()` call sites were missing `follow=True`, making the
installed `meshcore` library's own dirty-flag-driven refetch a permanent
no-op after the first successful contact fetch -- directly contradicting
`_refresh_contacts_and_grant_telemetry`'s own "the next periodic refresh
retries it" claim; (2) `_send_direct_supplement` silently dropped (no
log, no `_outgoing_dropped_total` increment) when a bound peer's contact
couldn't be resolved, unlike every sibling drop path; (3) `_fetch_own_
identity`'s documented reconnect-triggered retry never fires if the
initial fetch fails and the link then simply never drops again for the
rest of the process's life, permanently disabling the self-echo guard --
now also retried from the existing periodic contact-refresh loop; (4)
`_grant_telemetry_permission_if_needed`'s unguarded `flags` read sat
outside its own `try`, so one malformed contact could abort telemetry
refresh for every peer ordered after it in the same pass; (5) a local
exception from `_send_direct_frame` inside `_send_direct_frame_and_
wait_for_ack` used to skip the post-send listen window entirely, letting
the next contender for `_direct_exchange_lock` key the radio with zero
quiet time -- now caught, delayed, and re-raised. Also consolidated three
independently-duplicated pieces of logic flagged by the same pass:
`_fragment_payload`/`_fragment_direct_payload`'s identical chunking body
(-> `_chunk_payload`), the `_wait_for_incoming_quiet`/`_throttle_for_
duty_cycle` pair copy-pasted at all four radio-keying call sites (->
`_pre_transmit_gate`), and `_unknown_dest_attempts`/`_unknown_dest_
backoff_until`'s missing periodic reclaim (unlike `_dedup`/`_reassembly`/
`_proof_correlation`, all three already swept from the same loop) -- now
swept the same way via `_unknown_dest_backoff_sweep`. One further gap was
found and deliberately left as a flagged comment rather than fixed live:
`_resolve_routing_peer`'s PROOF-correlation lookup doesn't account for
`RNS.Packet.pack()` writing a link_id (not a destination hash) into an
outgoing LRPROOF's on-wire destination-hash field, so a Link-acceptance
reply to a known peer never resolves via that table -- bounded to an
efficiency loss (falls through to the existing broadcast+supplement
path, not a delivery or security issue), and a correct fix needs
`RNS.Link.link_id_from_lr_packet()`'s own ECPUBSIZE-based truncation
replicated exactly, which wasn't validated against real hardware in this
pass. All fixes re-verified against a real end-to-end run of
`testscripts/fake_meshcore_repeater_sim.py` (bind frames, single-fragment
and multi-fragment CHANNEL delivery) before and after, with identical
results -- no regression from the refactors.

**Field-data-driven addition (2026-09-17): DIRECT-fragmented delivery
completion check, closing a real "phantom ACK loss" gap the same field
test's captures proved.** Cross-referencing the sender's own ACK
bookkeeping against the receiver's own capture from that 5-node test found
a concrete case: `pkt_id=3` (router -> a client), 2 of 3 fragments logged
"never acknowledged" after both retry passes -- 4+ minutes, 8
fragment-attempts total -- yet the receiver had already fully reassembled
all 3 fragments about a second *before* the sender's own final successful
ACK for the third fragment even landed. That's direct proof the first two
fragments physically arrived; only their firmware ACKs failed on the
return trip, an asymmetric loss this design previously couldn't
distinguish from genuine non-delivery, so it kept blindly re-spending
airtime and `_direct_exchange_lock` time on data already delivered, and
risked tripping `direct_path_reset_threshold` over a link that was
actually fine. Fix: a new `"Q"`-marker DIRECT-only control frame (see
`_check_remote_completion`/`_handle_incoming_completion_frame`'s own
docstrings) -- `_send_direct_fragmented` asks, only as a last resort once
both passes are exhausted and fragments still appear missing, whether the
receiver already has the complete message; the receiver answers straight
from its existing whole-packet dedup cache (no new receive-side state).
Fully backward-compatible (an old peer simply never answers, and
`direct_completion_check_timeout_s` falls back to exactly today's
give-up behavior) and config-gated (`direct_completion_check_enabled`,
default on). Deliberately scoped to fragmented DIRECT sends only -- bare
DIRECT sends dedup on full payload bytes rather than a pkt_id and don't
fit this same query shape without further design work, left as a known
gap. Verified with a wire-format round-trip test and dedicated send-/
receive-side unit tests (dedup-hit/-miss answers, future correlation,
timeout/cleanup), plus a re-run of the CHANNEL-path fake-hardware smoke
test showing no regression -- real-hardware field validation of the new
frame itself is still outstanding.

**Airtime-efficiency fix (2026-09-17): supplement-target selection now
accounts for recent DIRECT failures, not just recency.**
`_select_direct_supplement_targets`/`_select_bootstrap_supplement_targets`
previously ranked candidates by recency alone, so a peer that had just
failed a DIRECT attempt -- but hadn't yet crossed
`direct_path_reset_threshold`, so was still technically eligible -- could
still win a scarce capped supplement slot ahead of an equally-recent,
untroubled peer. Both now sort primarily by each candidate's own
`_direct_path_failures` count (fewest first), falling back to the
original recency ordering only as a tiebreaker -- not a hard exclusion, a
struggling peer still gets picked once it's the least-bad option
available. Verified with dedicated unit tests plus a re-run of the
CHANNEL-path fake-hardware smoke test showing no regression.

**Field-diagnosed fix (2026-09-18): the incoming-quiet-defer feature
(2026-09-16) caused a mutual reset feedback loop between two chatty
nodes, collapsing multi-hop DIRECT delivery to 0/8 messages completed in
that night's field test, with the user having "tweaked some of the
delays" on both machines just beforehand.** Root cause, cross-diagnosed
between this session and a second Claude Code session running on the
user's laptop (over the real second radio) working the same incident from
the other end: `_last_incoming_direct_at` was updated in
`_on_contact_msg_recv` for *every* DIRECT frame heard -- ACKs, PROOFs,
completion-checks, a fragment that completed its own bucket, not just "a
fragment with more of this transfer still coming," which was the feature's
actual intent per its own 2026-09-16 request ("wait for the incoming
interface to either stop sending... to avoid keying the radio into the
middle of a peer's own multi-fragment DIRECT burst"). On a link where both
nodes are constantly exchanging that other traffic, a genuine 3s lull
(`incoming_quiet_window_s`) rarely occurred, so nearly every send --
including the fragment re-drives racing the receiver's own
`reassembly_idle_timeout_s` -- got pushed toward the 15s patience ceiling
(`incoming_quiet_defer_max_wait_s`). Confirmed directly against that
night's packet captures pulled from the laptop: fragment gaps widening
from ~20s to 60-90s apart within one run (`pkt_id=4`, `...083302.jsonl`),
then a later window (`...091341.jsonl`) with zero incoming fragments and
0/58 (0%) outgoing DIRECT attempts succeeding for 16+ minutes straight.

Two fixes, addressing both the specific bug and the pattern behind it
(this is the third field-driven addition in a row to stack a new
serialized delay onto the same DIRECT send path without checking it
against what else was already there -- see `_wait_for_incoming_quiet`'s
own 2026-09-16 entry, the priority-lock and duty-cycle entries before it,
and 2026-09-17's completion-check addition):

1. **Narrowed trigger + time-critical exemption.** `_last_incoming_
   direct_at` is now set only in `_handle_direct_multifragment_frame`,
   only when a received fragment leaves its bucket still incomplete --
   concrete evidence of more fragments actually coming, not "the channel
   was occupied by something." Separately, `_pre_transmit_gate` gained a
   `skip_quiet_defer` parameter, threaded down as `time_critical` from
   `_send_direct_fragmented` through `_send_direct_with_attempts`/
   `_send_direct_frame_and_wait_for_ack`/`_send_direct_frame`. The rule it
   encodes: **only a message's genuinely-first transmission (fragment 0,
   attempt 0) can afford the courtesy wait.** The receiver opens its
   reassembly bucket -- and starts its `reassembly_idle_timeout_s` clock
   (`_ReassemblyBucket.last_progress`) -- the instant fragment 0 lands, so
   continuation fragments (`frag_idx > 0`, and DIRECT pass 0 sends strictly
   in order, unlike the shuffled CHANNEL path), internal retries (`attempt
   > 0`) and pass-1 re-drives are all equally racing a deadline that's
   already running; taxing any of them with a collision-avoidance
   heuristic makes a late fragment likelier, not safer.
   `_throttle_for_duty_cycle` is untouched by either fix -- it's this
   node's own real airtime cap, not a heuristic, and a retry storm is
   exactly the case it exists to bound.

2. **`_validate_direct_timing_budget`, run once at startup after every
   `_configure_*` method.** The actual incident trigger wasn't the defer
   feature alone -- it was tuning DIRECT timing knobs (spread across four
   different `_configure_*` methods, each a separate field-driven fix
   over the past three days) without checking they were still coherent
   against `reassembly_idle_timeout_s`, the fixed clock on the other end
   of the same budget. It asks one narrow question -- how many worst-case
   clock-racing send attempts (`direct_ack_timeout_routed_max_s` +
   `direct_post_send_listen_max_s`) actually fit inside
   `reassembly_idle_timeout_s` -- and logs a `RNS.LOG_WARNING`, never
   silently overriding an operator's explicit config, if fewer than
   `direct_send_attempts` of them do, i.e. if the receiver can evict a
   bucket while the sender is still working through that fragment's first
   attempt budget. Deliberately framed as a ratio rather than "raise your
   timeout to N", so it names all three levers instead of biasing toward
   inflating reassembly patience. Verified against the incident itself: the
   pre-fix behaviour of taxing every attempt with the quiet defer gives
   63s/attempt against a 120s window = 1.90 attempts, under the budget of
   2, so this would have fired at that startup; with the fix in place the
   same knobs give 48s/attempt = 2.50 attempts and it stays quiet. Meant to
   catch exactly this incident's own root trigger ("tweaked some delays on
   both PCs") at the next startup, instead of only being discoverable hours
   into a live field test.

Not yet re-validated against real hardware (both machines' physical radios
weren't set up at the time of this fix) -- next field test should confirm
fragment re-drives now land promptly and multi-hop delivery recovers.

**Observability additions (2026-09-18, user-requested): the just-fixed
incident above needed cross-referencing separate `_debug` text lines by
hand to see how much of a DIRECT attempt's latency was the incoming-
quiet-defer wait versus lock queueing versus the ACK wait -- exactly the
kind of question `packet_capture_enabled`'s structured JSONL exists to
answer without that manual correlation, but it didn't carry this
particular breakdown yet.** Four additions, all field-tuning data only
(never read back by any routing/reliability decision this interface
makes):

1. `_pre_transmit_gate` now returns `(quiet_defer_wait_s,
   duty_cycle_wait_s)` instead of discarding both. `_send_direct_frame`
   forwards them through a new optional `gate_telemetry` out-dict
   parameter (an out-param rather than widening its own return value,
   since only `_send_direct_frame_and_wait_for_ack` needs it -- the other
   three callers are unaffected) into `_capture_direct_attempt_result`,
   which now records `quiet_defer_wait_s`/`duty_cycle_wait_s` alongside
   the `time_critical` flag that decided whether the first one applied at
   all. A field-test capture can now directly compute, per attempt, how
   the total latency split across queueing/gating/ACK-waiting instead of
   inferring it from separate log lines.
2. `_capture_direct_attempt_result` also gained `pass_number` (`0`/`1`/
   `None`), threaded from `_send_direct_fragmented` through `_send_
   direct_with_attempts`: lets a future analysis directly measure how
   often the pass-1 re-drive actually fires and how often it then
   succeeds -- a core reliability metric previously only reconstructible
   by hand from `attempt`/`frag_idx` patterns.
3. New `_capture_channel_fragment_sent` event, called from `_send_
   channel_multifragment_pass` for every fragment transmit attempt
   (success or local failure) -- the sender-side counterpart to
   `_capture_fragment_received`'s CHANNEL support, which has recorded the
   receive side since 2026-09-17 with no sender-side equivalent. Records
   `position` (this fragment's slot in this pass's own shuffled send
   order) separately from `frag_idx` (its fixed logical index), so a
   future analysis can check `docs/reliability_engine_design.md` §2's
   position-dependent loss pattern directly against real capture data.
4. `_send_direct_fragmented` gained a "starting a fragmented send" debug
   line (`pkt_id`/`frag_total`/`hop_count`) at the top of the method,
   mirroring `_send_channel_multifragment_pass`'s existing equivalent --
   previously the first visible sign a DIRECT-fragmented send existed at
   all was its first per-attempt line, after the first fragment had
   already been dispatched.

**User-requested (2026-09-18): step 1 of "lessen our reliance on arbitrary
wait times" -- observe-only tap on the firmware's raw-RX log feed.** The
user's framing: get fragments through more consistently *without* adding
delay or airtime, be smarter about when to transmit and listen given the
mesh's latency, without building a token-ring scheme. Reviewing the
2026-09-16-evening capture (`test5_client_b.jsonl`, 1-hop, 63 DIRECT
attempts) showed where the time actually goes: 43% of attempts got no ACK,
and the mean `_direct_exchange_lock` wait was ~11s (max 49s) -- almost all
of it queueing behind *other* sends' full ACK timeouts, not the deliberate
listen windows (~1.2s/attempt). So the lever is fewer collisions and
faster failure detection, not shorter sleeps; and every arbitrary sleep in
this file exists to cover the same blind spot: between keying the radio
and the ACK event, this interface knows nothing about what's on air.
`_wait_for_incoming_quiet`'s own comment ("no real-time channel-busy/CAD
signal from the meshcore library -- confirmed: nothing exposes that") is
right about CAD but missed a better signal: the companion firmware's
`MyMesh::logRxRaw` pushes EVERY packet the radio decodes to the host as
PUSH_CODE_LOG_RX_DATA -- unconditionally when serial is connected, no pref
gates it (checked against referenceprojects/MeshCore-main's companion_radio
source; hardware CAD and the RSSI interference threshold, by contrast, are
hard-coded off there "until configurable") -- and the installed `meshcore`
library (2.3.9.1, reader.py's LOG_DATA branch + meshcore_parser.py) parses
it into `EventType.RX_LOG_DATA` with SNR/RSSI, MeshCore route type,
payload type, path length and path hashes. That includes traffic not
addressed to this node: other peers' DIRECT frames, flood repeats, ACKs in
transit, and a repeater's echo of this node's own frame -- all zero
airtime, all currently thrown away.

Per this project's standing rule (field evidence before timing changes),
this step deliberately changes NO routing or timing behaviour. It only
makes the feed visible so real captures can establish the correlations the
later steps would rely on:

1. `rx_log_observe_enabled` (default yes) -- `_subscribe_rx_log_events`
   subscribes `_on_rx_log_data` to `RX_LOG_DATA` if the installed library
   has it (probed with `hasattr`, logged either way; deliberately NOT a
   REQUIRED_EVENT_TYPES member -- an older library loses only this
   observability, not the interface).
2. Every overheard packet bumps `_rx_log_events_total`/`_rx_log_by_
   payload_type` (new `rx_log_feed=seen|never|off`, `rx_log_events_total`,
   `rx_log_by_type` fields on the [STATS] line -- `never` vs `seen` is how
   a silent capture gets told apart from a firmware that doesn't push the
   feed) and, when packet capture is on, writes one `rx_log` record
   (`_rx_log_capture_fields`): SNR/RSSI, route/payload type, path, the
   1-byte dest/src routing hashes for addressed payload types (REQ/
   RESPONSE/TXT_MSG/PATH -- Mesh::createDatagram writes dest then src),
   the 4-byte code for an ACK (the same value MSG_SENT's `expected_ack`
   carries, so on-air ACK sightings can be matched against ACK events this
   node did or didn't get), the library's `pkt_hash` (the same hash seen
   twice in quick succession with a longer path is a repeater echo), and
   two relative timings: `since_last_rx_log_s` (burst structure) and
   `since_own_tx_s` (new `_last_own_tx_at`, stamped in `_pre_transmit_
   gate`, the last common point every keying path passes through).
3. `testscripts/rx_log_monitor.py` -- single-radio, RNS-independent,
   transmits nothing: prints the feed live and flags echoes, to confirm a
   given radio's firmware actually pushes it before relying on the
   interface's own records.

Verified the same day on real hardware (two Heltec V3 companions, desktop
"afipc" + laptop "a", physically adjacent, on the live Broken Hill public
mesh with one zero-hop repeater "19" in range; SF7/BW62.5/CR8) --
captures in `fieldtests/raw/2026-09-18-rxlog-step1/`. Findings the later
steps can build on:

- The feed is real on this firmware build: every packet, ours or not,
  arrives with SNR/RSSI/type/path; a quiet public mesh produced ~1
  packet/100s of background, so serial load is a non-issue here.
- `ack_code` seen on air matches MSG_SENT's `expected_ack` byte-for-byte
  (`9bada92d`, `7faf2e4b`), and lands ~0.2s before the library's ACK event.
- Zero-hop routed DIRECT: ACK on air 0.66-0.78s after `send_msg` returned
  (library-level), 1.09s after `_pre_transmit_gate` (interface-level,
  includes command latency). The interface's ACK timeout for that same
  exchange was 5.2s -- roughly 7x the measured RTT.
- A *flood* DIRECT (no known path) is acknowledged with a PAYLOAD_TYPE_PATH
  reply, not a bare ACK -- any "ACK sighting" logic must treat PATH as an
  ACK carrier for flood-mode sends.
- Repeater echo timing: the zero-hop repeater re-transmitted every flood
  packet it heard 0.5-1.2s later (`since_own_tx_s` 1.06s for our own REQ,
  +0.86s for the laptop's TEXT_MSG); so every flood we send costs ~2x
  airtime on the local channel, and "did the repeater pick it up" is
  answerable within ~1.5s. Routed DIRECT frames with an empty path were
  never echoed (as expected -- nothing left to forward).
- The stale-path failure mode, seen directly: the laptop's contact for
  afipc carried a 4-repeater path (`d6 4f be 19`); its DIRECT frame was
  heard by the desktop at -55dBm 10ms later but not accepted (path not
  exhausted), no repeater echoed it, no ACK -- and the sender sat out the
  full 30s timeout. With this feed, "our frame, no echo within ~1.5s" is
  a hop-1 failure signal available 20x sooner than the timeout.

**Step 2 (2026-09-18, same day, user-requested): measured ACK RTT drives
the ACK timeout; per-attempt RX-log correlation in the capture.** The
only timing change in the plan that's safe on zero-hop-only evidence,
because it can only ever *shorten* a wait, never lengthen one:

1. `_send_direct_frame_and_wait_for_ack` now measures MSG_SENT -> ACK
   latency on every real ACK and folds it into a per-peer Jacobson/Karels
   estimator (`_record_ack_rtt`, `_ack_rtt`). `_adaptive_ack_timeout`
   replaces the firmware-derived timeout with `direct_ack_rtt_timeout_
   multiplier * (srtt + 4*rttvar)` once `direct_ack_rtt_min_samples` (3)
   samples exist -- floored at `direct_ack_rtt_min_timeout_s` (3.0s),
   and NEVER above the firmware value it replaces (worst case is exactly
   the pre-step-2 behaviour). Karn-style invalidation
   (`_invalidate_ack_rtt`) on the first miss governed by the measured
   value, and on every path change (`_reset_stale_path`, a fresh
   `discover_path` result): an RTT over one path says nothing about
   another. `direct_ack_rtt_adaptive_enabled=no` restores the old
   behaviour entirely. Why this is the right first timing change: a
   missed ACK holds `_direct_exchange_lock` for the full timeout, and the
   2026-09-16 1-hop capture's 11s-mean/49s-max lock waits were almost
   entirely other sends' timeouts -- the timeout's size, not the listen
   windows, is where the wasted silence goes.
2. Each `direct_attempt_result` capture record gained `ack_latency_s`,
   `send_cmd_latency_s` (gate -> MSG_SENT: host/serial/firmware queueing,
   not airtime), `ack_timeout_source` (`firmware`/`measured`), the live
   `rtt_srtt_s`/`rtt_rttvar_s`/`rtt_samples`, and an RX-log correlation
   window (`_open_rx_log_window`/`_classify_rx_log_for_window`, open only
   while this attempt holds the lock, so at most one exists): `rx_echo_
   seen_s` (our own TEXT_MSG to the target re-heard -- a repeater
   forwarded it, i.e. hop 1 happened), `rx_ack_seen_on_air_s` (a bare ACK
   with our `expected_ack` code), `rx_path_reply_seen_s` (PATH from the
   target to us -- the flood-mode ACK carrier), and `rx_foreign_count`/
   `rx_foreign` (everything else heard while we waited, capped at 20).
   Identity matching uses MeshCore's 1-byte routing hashes (all the
   cleartext carries), so it's capture-grade, not decision-grade -- noted
   in `_open_rx_log_window` so nobody promotes it without a stronger
   check. `[STATS]` gained a per-peer `ack_rtt=` summary.
3. `testscripts/zero_hop_peer_discovery_test.py --repeat N` sends N test
   packets so the measured timeout actually engages within one run.

Verified zero-hop on the same two radios (captures in `fieldtests/raw/
2026-09-18-rxlog-step2/`; 6 packets, 7 attempts, 6/6 delivered):
`ack_latency_s` was 1.029-1.030s on all six ACKs -- zero-hop RTT is
essentially deterministic (airtime + firmware turnaround), and the on-air
ACK sighting preceded the library's ACK event by ~1ms every time.
`send_cmd_latency_s` was a steady 0.021s. The measured timeout engaged on
the 4th attempt and stepped 5.80s (firmware) -> 4.38 -> 3.80 -> 3.37s,
converging on the 3.0s floor. The one failed attempt (the very first)
had `rx_foreign` = the *laptop's own* path-discovery REQ, our PATH reply
echoed by the repeater, and the laptop's bind frame at +1.2/+1.4/+2.1s
after our transmit: the target was transmitting, not listening, when our
frame arrived -- a half-duplex collision at zero hop, caught directly in
the window. That is the signal step 4 should key its post-fail hold off
("target-originated traffic heard during our wait" = they were busy;
retry after their burst, not after a random 0.3-3s). All of this is
zero-hop data: it says nothing yet about the 1-2 hop regime where the
field-measured losses live, and the estimator's per-path invalidation is
there precisely because those RTTs will look nothing like 1.03s.

**Step 3 (2026-09-18, same day): have-bitmap completion ANSWER and
send-once-then-reconcile for DIRECT-fragmented sends.** The airtime
step of the plan -- the one change that reduces transmissions on a
lossy link rather than only reshaping waits:

1. `"Q"` completion frames are now v2: an ANSWER appends a have-bitmap
   (`ceil(frag_total/8)` bytes; at most 52 chars on the wire for 255
   fragments) after the unchanged 6-byte body, so the receiver reports
   *which* fragments it holds, not just complete/not. The receiver
   consults both the dedup cache (finished reassembly) and any still-open
   `_reassembly` bucket under exactly `_reassembly_key`'s tuple. v1
   frames still decode, a v1 QUERY is answered in v1, and an older peer
   drops a v2 frame as unsupported -- which the sender treats as "no
   answer", i.e. the pre-step-3 behaviour. `_check_remote_completion` is
   now a wrapper over `_query_remote_fragments`, whose wait uses
   `_completion_query_timeout_s` (the config value, or 3x the step-2
   measured RTT bound when that's larger).
2. `_send_direct_fragmented` (non-handshake priorities, `direct_fragment_
   reconcile_enabled`, default yes): pass 0 sends every fragment exactly
   `direct_fragment_pass0_attempts` (1) time(s), *unrecorded* against the
   stale-path failure count (a lost ACK is not a path failure -- the
   2026-09-16 phantom-ACK case is the whole motivation). If anything
   lacks an ACK, ONE reconcile QUERY asks what the receiver holds;
   un-ACKed fragments it confirms are marked delivered (and a success is
   recorded, since the data provably crossed the path), and pass 1
   re-drives only the rest with the normal recorded budget. A complete
   answer skips pass 1 entirely. No answer -> re-drive every un-ACKed
   fragment, exactly as before. Then the pre-existing final check runs as
   the last resort. PRIORITY_HANDSHAKE keeps its own larger pass-0 budget
   and no reconcile: a lost Link handshake forces a path rediscovery, and
   the extra round trip would only delay it. Airtime arithmetic: one
   QUERY+ANSWER is two ~10-50-char frames (plus their tiny firmware
   ACKs); each blind retry is a full ~160-char fragment plus its ACK --
   net saving whenever at least one "missing" fragment was actually held,
   and the shared radio lock is released sooner in every case because
   pass 0 no longer burns a second full ACK timeout per fragment before
   moving on.
3. `testscripts/zero_hop_peer_discovery_test.py --payload-size N` forces
   fragmentation; `--verify-query` sends one QUERY after the last packet
   and prints the bitmap ANSWER, so the v2 frame is exercised over the
   air even when (as at zero hop) no ACK is ever lost.

Verified zero-hop (captures in `fieldtests/raw/2026-09-18-rxlog-step3/`):
3 packets x 3 fragments, 9/9 ACKed on their single pass-0 attempt, 3/3
reassembled on the receiver, and the verify QUERY answered on air with
`v2 complete=True held=[0, 1, 2]` (receiver log: "answering
complete=True held=[0, 1, 2]"). Full ~160-char fragments measured a
steady 1.254-1.255s ACK RTT versus 1.03s for the small test packet --
RTT scales with airtime, and the step-2 estimator tracked it (timeout
7.44s firmware -> 4.57s measured by the 9th fragment). One fragment's
ACK took 1.98s: its RX window shows the receiver's own bind frame going
out 80ms after our transmit, so our ACK queued behind it -- the same
"target was busy" signature as step 2's collision, here costing latency
rather than the frame. The reconcile branch itself could not fire at
zero hop (nothing was lost) and is covered by the unit-level flow checks
run during development; its first real exercise will be a multi-hop
capture.

**Step 4 (2026-09-18, same day, user chose "build it behind a flag,
default off"): RX-log-derived transmit holds.** `rx_log_holds_enabled`
(default NO -- see that config's own comment for the model). What ships:

1. `_estimate_airtime_s`: the real LoRa time-on-air formula at the
   radio's own SF/BW/CR (read from SELF_INFO in `_fetch_own_identity`,
   preamble length per the firmware's `preambleLengthForSF` rule, LDRO
   when the symbol time exceeds 16ms), falling back to the bitrate
   estimate when unknown. Replaces "seconds measured at SF7" with
   something that scales: SF7/BW62.5/CR8 puts a 102-byte frame at 0.58s;
   SF12/BW125 puts it at 4.4s.
2. `_medium_busy_until`, extended by `_on_rx_log_data` for every
   overheard packet via `_predicted_hold_for_rx` (flood -> every repeater
   re-floods it, `rx_log_hold_flood_factor` x airtime, plus a reply
   turnaround for addressed types; routed DIRECT with N hops left ->
   N forwards at `rx_log_hold_hop_factor` x airtime, plus an ACK
   turnaround; an ACK/ADVERT on a direct route -> nothing follows). The
   model is ALWAYS maintained and recorded (`predicted_hold_s`/
   `hold_reason`/`medium_busy_remaining_s` on every `rx_log` record) so a
   capture taken with holds OFF still shows what they would have done --
   that is how the 1-2 hop decision to enable them gets made.
3. When enabled: `_pre_transmit_gate` waits out the prediction
   (`_wait_for_medium_clear`, re-checked as new packets extend it,
   capped at `rx_log_hold_max_s` total), and the post-miss listen window
   becomes `_post_miss_hold_s(_diagnose_missed_ack(...))`: `target_busy`
   (target-originated traffic in the attempt's RX window) / `hop1_loss`
   (a forward was due and none was heard) / `no_info` wait out the
   predicted busy window plus 0.2-0.8s jitter; `downstream_loss` (our
   frame was forwarded, the ACK just never came back) takes the jitter
   alone, since re-waiting a random 0.3-3s buys nothing there. The
   diagnosis is always computed and captured (`miss_diagnosis`) even when
   the flat ranges still choose the wait. `_validate_direct_timing_
   budget` counts both caps when the flag is on.

Verified zero-hop with the flag ON on both nodes (`fieldtests/raw/
2026-09-18-rxlog-step4/`): 3 packets x 3 fragments, 9/9 single-attempt
ACKs, 3/3 reassembled. The holds fired exactly where the step-2 collision
had happened -- during the bind/path-discovery burst -- as 2.9s + 3.1s
pre-transmit waits (`flood_echo`, the peer's REQ/PATH flood traffic and
its repeater echoes) before the first fragment, which then landed with 6
foreign packets in its window and a 1.53s ACK; every later fragment saw
0.00s of hold. Across the run the model predicted a mean 0.46s /
max 1.18s of busy air per overheard packet. The listener side held once
(1.79s) for the same reason. Zero-hop still can't exercise `hop1_loss`/
`downstream_loss`; those branches are unit-checked only and need the
multi-hop capture. Two things to watch for in that capture before
flipping the default: whether `flood_echo` over-holds on a repeater-dense
mesh (the 4s cap is the guard), and whether `target_busy` is the dominant
miss diagnosis at 1-2 hops the way it was for the only zero-hop miss.

The remaining follow-on steps, gated on what a multi-hop capture shows:
enable step 4 by default (`rx_log_holds_enabled=yes`) once 1-2 hop
captures confirm the model; then step 5, packing a "more packets queued
behind this one" hint into the multi-fragment header's `attempt` byte
(only 2 of its 8 bits are used) so the receiving peer can yield or
interleave on facts instead of a timer.

**Code review pass (2026-09-18, user-requested: "thoroughly review the
code, make sure design decisions are coherent, fix any error"). Every
finding below was checked against the installed `meshcore` library
(2.3.9.1), the firmware source in `referenceprojects/`, or RNS core
in-process -- nothing was changed on a guess.** Coherence fixes, in
order of consequence:

1. *The `"Q"` completion exchange no longer breaks the shared-radio
   invariant.* `_direct_exchange_lock`'s own contract (2026-09-15) is
   "held for the full send+ACK-wait duration of every DIRECT exchange",
   yet `_query_remote_fragments` (and its 2026-09-17 predecessor) released
   the lock the instant `send_msg` returned MSG_SENT -- while the QUERY's
   own firmware ACK and the peer's ANSWER were both still in flight, so
   the next queued send could key the radio straight into the reply this
   node was waiting for. Step 3 moved that exchange from a last resort
   into the main path of every fragmented send, which made this matter.
   Now the lock is held from the QUERY's transmit through the ANSWER (or
   its timeout), exactly like an ACK wait; `_send_completion_answer` goes
   through `_send_direct_frame_and_wait_for_ack` (one attempt, no retry --
   the querier's timeout is still the recovery path) so its own ACK is
   waited out under the lock too, with a `kind="completion_answer"`
   marker on the resulting `direct_attempt_result` record. Both frames
   are also `time_critical` (they sit inside a send whose receiver-side
   reassembly clock is already running -- the 2026-09-18 rule), and the
   QUERY inherits the send's own `priority` instead of `PRIORITY_LOW`:
   a reconcile step that queues behind every ordinary send while the
   receiver's `reassembly_idle_timeout_s` counts down defeats its own
   purpose. The ANSWER is `PRIORITY_NORMAL` for the same reason -- a
   `PRIORITY_LOW` reply behind one missed-ACK timeout on the answering
   node (5-45s) can never beat the querier's 5s wait, which turns every
   such query into pure wasted airtime. `_completion_query_timeout_s`
   also now adds `rx_log_hold_max_s` when step-4 holds are on, since the
   peer's ANSWER pays that pre-transmit hold before it can leave.
2. *A missed ACK under the step-2 measured timeout no longer counts
   toward `direct_path_reset_threshold`.* Step 2 promised "the worst case
   is exactly the pre-step-2 behaviour", but a miss under a timeout this
   interface had tightened on its own was still reported with
   `waited_full_timeout=True`, i.e. as a genuine path failure -- when it
   is precisely §8's "cut short by this engine's own ceiling" case that
   `record_direct_send_result`'s existing gate exists for. The estimate
   is Karn-invalidated on that miss (unchanged), so the very next attempt
   runs on the firmware timeout, and a miss *there* still counts.
3. *Successful Link establishments were being counted as
   unknown-destination failures.* `_record_unknown_dest_attempt` fires for
   every LINKREQUEST to a destination with no §7 token, and its only
   clearing signal was a token learned for that exact destination hash --
   but the reply to a LINKREQUEST is an LRPROOF whose destination field
   is the *link_id*, and everything after it rides that link_id too, so
   the target destination's own hash is only ever learned if its ANNOUNCE
   happened to arrive DIRECT (small-mesh mode). Outside that mode, three
   perfectly good Links to the same destination put it into a 5-60 minute
   backoff that strips the DIRECT-bootstrap supplement from every later
   LINKREQUEST. Fix: `_compute_link_id` replicates
   `RNS.Link.link_id_from_lr_packet` (validated in-process, byte-for-byte,
   against real `RNS.Packet`/`RNS.Link` for payloads of 32/64/66/70/80
   bytes -- the ECPUBSIZE truncation branch included); every outgoing
   LINKREQUEST records `link_id -> destination_hash` in
   `_pending_link_requests` (TTL `proof_correlation_ttl_s`, swept with the
   other tables), and an incoming DIRECT LRPROOF matching a pending
   link_id learns BOTH `link_id -> peer` and `destination_hash -> peer`
   and clears that destination's backoff. The same primitive closes the
   2026-09-16 "known gap" in `_resolve_routing_peer`: an incoming
   LINKREQUEST from a bound peer now records `link_id -> peer`, so this
   node's own outgoing LRPROOF (whose destination field is that link_id,
   per `RNS.Packet.pack()`) resolves DIRECT-primary instead of falling
   through to broadcast+supplement. As a side effect the initiator's
   first post-handshake packet (LRRTT) also goes DIRECT immediately
   rather than after one broadcast round.
4. *Housekeeping:* `_path_response_last_sent_at` (the 2026-09-15 rate
   limiter's state, flagged in a TODO as never reclaimed) is now swept
   from `_reassembly_cleanup_loop` like every other table;
   `_validate_direct_timing_budget`'s post-miss term with step-4 holds on
   is `rx_log_hold_max_s` (what `_post_miss_hold_s` is actually capped
   at), not `max(listen_max, hold_max)`, and its warning names the terms
   it actually summed; `asyncio.get_event_loop()` -> `get_running_loop()`
   where a coroutine already guarantees one.

Docstring drift found and corrected in place rather than left to mislead
the next reader: commit ef57809 ("Lowered most hard coded delays for
testing") changed several defaults this docstring's dated entries still
quote as current -- `SMALL_MESH_DIRECT_ONLY_MAX_PEERS` 2 -> 3,
`direct_path_reset_threshold` 2 -> 3, `direct_path_reset_min_age` 30 ->
60s, `path_discovery_quick_attempts` 3 -> 2, `direct_post_send_listen`
0-5s -> 0.3-3s and its success range 0-0.5s -> 0-0.4s. The dated entries
above are left as written (they are history); the live docstrings and
`readme.md`/`CLAUDE.md` now state the current values.

Verified with `testscripts/fake_meshcore_repeater_sim.py` before and
after (zero-hop 2x300B fragmented, and the two-hop lossy-return-path
case that drives the reconcile QUERY) -- see the run notes in the
review's own summary; real-hardware validation of the completion-frame
locking change is still outstanding.

**Field-diagnosed batch (2026-09-18, evening -- the first real multi-hop
capture: `fieldtests/raw/2026-09-18-drive-home/`, laptop side only, a
3-hop path (`?->d6->19->desktop`) that degraded to a dead first hop and
then collapsed to zero-hop as the car arrived home).** Diagnosed by a
second Claude session on the laptop and re-verified here against the
same JSONL before anything was changed. Four fixes:

1. *Reconcile QUERY timeout fell to the 5s floor at 3 hops, where the
   QUERY's own ACK alone takes ~4.5s* -- both reconcile queries in the
   capture timed out at exactly 5.0s with `rtt_samples=0`, because the
   miss that triggers a reconcile is (by construction) a miss under the
   measured timeout, and step 2's Karn invalidation had just discarded
   the RTT stats `_completion_query_timeout_s` needed. Karn is right not
   to *trust* that estimate for the next ACK wait; it is still the best
   information for sizing a two-frame exchange. `_invalidate_ack_rtt`
   now keeps the discarded stats in `_ack_rtt_snapshot` (for the
   missed-ACK case only -- a path change still drops everything), and
   with no snapshot at all the query waits the peer's last firmware
   hop-aware ACK bound (`_last_firmware_ack_timeout_s`) x2, capped as
   before at `direct_ack_timeout_routed_max_s`.
2. *Early abort on a dead first hop (`direct_hop1_abort_enabled`,
   default yes).* The outage cost was the timeout, not collisions: nine
   consecutive misses each burned the full 28s firmware timeout while
   the RX log heard *nothing at all* -- no repeater forward of our own
   frame, when every one of the 24 successful multi-hop attempts before
   the outage had one (`rx_echo_seen_s` 0.84-3.58s, median 1.73s). The
   stale-path reset therefore needed 4 minutes to fire and the queue
   backed up to 18 deep with 225s lock waits. Now
   `_send_direct_frame_and_wait_for_ack` learns each peer's echo timing
   (`_echo_stats`, last 16 samples, cleared on any path change) and,
   once `direct_hop1_abort_min_samples` (3) exist, waits only
   `max(direct_hop1_abort_min_s, direct_hop1_abort_echo_multiplier x
   the peer's slowest observed echo)` (5s / 2.0 by default) for EITHER
   the ACK or the echo; if neither has arrived it gives up on that
   attempt as `ack_timeout_source="hop1_abort"` -- a recorded failure,
   since silence where a forward was due is positive evidence, so the
   reset fires in ~30s instead of ~4 minutes. The guard the capture
   demanded: 18 successful attempts in the last minute had NO echo,
   because the laptop was already zero-hop (ACK in 1.3-2.2s) while the
   interface still carried `hop_count=3`. An ACK always wins the race
   against a >=5s deadline, so those keep succeeding. Self-disabling on
   a radio/library with no RX-log feed (no echo samples -> never arms),
   never applied at hop_count 0/None, never longer than the ACK timeout
   it shortens, and it does not Karn-invalidate the RTT estimate (the
   estimate wasn't what governed the wait).
3. *Outgoing path requests are coalesced per requested destination*
   (`PATH_REQUEST_RATE_LIMIT_WINDOW_S`, 20s, the mirror of the
   2026-09-15 PATH_RESPONSE rule): RNS emitted 14 identical requests for
   one destination at 4-8s gaps -- explicit client retries, under
   Transport's own 20s automatic floor -- and each became a 3-hop DIRECT
   exchange, driving queue depth to 7 on its own. Every path request
   shares one PLAIN pseudo-destination hash, so the key is the
   *requested* hash, the first 16 bytes of the packet data
   (`_path_request_target`, layout confirmed against
   `RNS.Transport.request_path`).
4. *Queued packets expire (`outgoing_max_age`, 120s; ANNOUNCE exempt).*
   17 LXMF pings queued during the outage drained as a stale burst in
   85s once the path came back. The age is checked at dequeue
   (`_outgoing_worker`), before every DIRECT attempt's lock acquisition
   and again right after it (`expires_at`, threaded down the DIRECT
   chain exactly like `hop_count`), and before each CHANNEL retry pass
   -- an expired packet is dropped with `ack_timeout_source="expired"` /
   routing decision `expired_in_queue` in the capture, counted in
   `outgoing_dropped_total`, and never recorded as a path failure. 120s
   matches `reassembly_idle_timeout`: past it the receiver has already
   given up on any fragmented packet anyway.

Not done from the same analysis, deliberately: dropping LINK-context
packets after a LINKCLOSE -- the capture shows zero later packets to the
closed link's hash, so there was no evidence to build against. The
multi-hop numbers the RX-log hold model was waiting on now exist (echo
1.4-3.0s, 3-hop ACK 3.5-7.0s mean 3.5s, RSSI -100 via repeater vs -52
direct); holds stayed off and the recorded predictions were small, which
fits link loss rather than contention, so the default is unchanged.

**Field-diagnosed batch (2026-09-18, late evening -- a zero-hop NomadNet
page load, both sides captured: `fieldtests/raw/2026-09-18-zero-hop-
nomadnet-page/`).** Link setup took 14s; the page's 12 Resource parts
(483B each, 5 DIRECT fragments apiece, 60 fragments) then took 7.6
minutes, during which the server transmitted 110 fragments -- every one
ACKed, mean ACK 1.24s, so the radio was not the problem. Three causes,
verified against the capture and fixed in the same order; the third was
self-inflicted by that morning's `outgoing_max_age`:

1. *The duty-cycle estimate was quantizing away a third of the policy's
   own allowance.* `_throttle_for_duty_cycle` estimated a full 151-char
   fragment at 1.007s (`151*8/1200`), so three in one 10s window came
   to 3.02s -- a hair over the 3.0s cap -- and the limiter admitted two
   per window (median 4.3s between ACKed sends; 304s of the 509s
   transfer spent waiting on it). The other radio's RX log shows what a
   fragment really is on air: 166 bytes, exactly the firmware's framing
   (`Mesh::createDatagram`: dest+src hash, 2-byte MAC, and
   `encryptThenMAC` padding timestamp+flags+text+NUL to a 16-byte
   cipher block, plus the 2-byte packet header), and at this rig's
   SF7/BW62.5/CR8 that is 0.877s by the step-4 time-on-air model. Three
   of those are 2.63s. `_estimate_tx_airtime_s` now feeds the limiter
   that model-derived figure whenever SELF_INFO has provided the radio
   parameters, falling back to `duty_cycle_estimate_bitrate` otherwise.
   The 30%-of-10s policy itself is untouched; at these settings it now
   admits the three fragments per window it always allowed for. (The
   `_DutyCycleLimiter` docstring's "no access to SF/BW/CR" was true when
   written and stopped being true at step 4.)
2. *`outgoing_max_age` dropped fragments mid-packet.* All nine expiries
   in the capture were fragments 2-4 of 5, in pass 0, of parts whose
   earlier fragments had already been transmitted -- each one threw away
   the air already spent, left the receiver's bucket to time out, and
   made RNS re-request the whole part as a fresh packet. The 120s age is
   simply shorter than a throttled 13-packet queue's drain time. Expiry
   is now decided once, before a packet's FIRST transmission (bare
   attempt 0, or fragment 0 of pass 0), and never afterwards; and
   packets carrying Resource data parts (`context == RESOURCE`) are
   exempt altogether -- RNS's Resource layer owns their retransmission
   (the receiver re-requests what it lacks), so this interface
   second-guessing it can only add round trips.
3. *RNS re-requested parts that were still queued here, so half the
   transfer was redundant:* 26 RESOURCE packets queued for 12 distinct
   payloads (the receiver re-requests every ~27s while the earlier copy
   is still waiting on the limiter). `process_outgoing` now drops a
   packet whose bytes are already queued or in flight
   (`_outgoing_inflight`, keyed by the packet's truncated hash, released
   only when every send task the packet spawned has finished -- never on
   a timer, so a copy that genuinely failed can be re-sent the moment
   the failure is known). Identical RNS bytes have identical effect and
   the receiver would dedup them anyway; the capture's own `payload_hash`
   is what showed the duplication. Recorded as `duplicate_in_flight`.

Policy change, user decision (2026-09-18, same evening): `duty_cycle_
window` default raised from 10s to 60s at the same 30% fraction. At
SF7/BW62.5 the 10s window capped a 60-fragment page at roughly 3 minutes
even with zero waste, because a burst could only ever reach the ~26% a
10s window quantizes to; over 60s a burst can use the full 18s of
allowance (about twenty 0.877s fragments back to back) before the
limiter pauses it, and the "majority of the time listening" intent
still holds over every rolling minute. The user's original 2026-09-16
instruction ("a max of 30% of total airtime in a 10 second period") is
superseded by this one; the fraction is unchanged.

**User-requested (2026-09-18, same evening): link-maintenance traffic
bypasses the duty-cycle wait** (`duty_cycle_exempt_handshake`, default
yes). The concern: a burst of page data can consume the whole 18s/60s
allowance, and a KEEPALIVE/LRRTT/LRPROOF queued behind it would then wait
for budget while RNS's own link timers run -- losing the Link, which
costs a full re-establishment, to protect a few hundred milliseconds of
air. The class is exactly `_priority_tier`'s PRIORITY_HANDSHAKE
(LINKREQUEST, PROOF, and the KEEPALIVE..LRPROOF / RESOURCE_PRF/ICL/RCL
contexts) -- the same packets that already jump `_direct_exchange_lock`'s
queue, so the two priority mechanisms now agree. Its airtime is still
RECORDED against the window (`_throttle_for_duty_cycle`'s `exempt` path
records without waiting), so ordinary data pays for it and the 30%
ceiling stays honest over any minute; only the wait is skipped. Applies
on both the DIRECT path (`_send_direct_frame_and_wait_for_ack` derives it
from `priority`) and the CHANNEL path (`_send_broadcast_packet` from the
header); bind and completion frames are unaffected. Captured per attempt
as `duty_cycle_exempt`.

**Field-diagnosed batch (2026-09-18, night -- Alpha 0.1.1 captures:
`fieldtests/raw/Alpha0.1.1/`, a zero-hop NomadNet page session and an
evening drive through 1-3 repeater hops, both sides captured).** Three
fixes, each traced to a specific packet sequence:

1. *The bare-DIRECT receive dedup broke an RNS contract and stalled a
   Resource transfer.* RNS's `Transport.packet_filter` deliberately
   exempts KEEPALIVE, RESOURCE, RESOURCE_REQ, RESOURCE_PRF, CACHE_REQUEST
   and CHANNEL contexts from its own duplicate filter, because it
   re-delivers byte-identical packets for them on purpose: a Resource
   receiver accepts a part only if its map hash sits inside the current
   receive window, so a part that arrives one slot early is discarded and
   re-requested, and the sender answers with the same bytes. The zero-hop
   page capture shows exactly that: a 35-byte last part arrived before
   the 483-byte part ahead of it (17:38:33 vs :36), RNS discarded it,
   re-requested it 16 more times over two minutes, the desktop re-sent it
   16 times -- and the laptop's `_handle_incoming_frame` dropped every
   copy after the first as a "duplicate bare DIRECT packet" (150s dedup
   TTL), so RNS never got a second chance; the transfer died with a
   cache-request and an ICL cancel. The dedup now consults the packet's
   own RNS context and lets exactly RNS's own exempt set through
   (`_RNS_NO_DEDUP_CONTEXTS`); everything else keeps the 2026-09-16
   protection against this interface's own retry re-delivering a packet.
2. *Fragmented sends gave up one fragment short.* Two PATH_RESPONSE
   announces at 2 hops (drive capture, pkt 20 and 21) each delivered 2 of
   3 fragments, the reconcile QUERY confirmed the receiver held them, and
   the last fragment then exhausted `direct_send_attempts` (2) in pass 1
   -- 3.5 minutes each, and the laptop never got a path to the desktop.
   When the receiver provably holds part of the packet (any pass-0 ACK or
   a reconcile answer), the remaining fragments are the whole difference
   between wasted air and a delivered packet, so pass 1 now uses
   `direct_fragment_finish_attempts` (default 4, the handshake budget)
   instead of the ordinary budget. The reconcile answer is also applied
   authoritatively now (`acked[i] = i in held` for every fragment, not
   only the un-ACKed ones): the receiver's bucket is ground truth, and a
   fragment it no longer holds must be re-driven whatever ACK we saw.
3. *A re-issued packet restarted from zero.* When pkt 20 failed, RNS
   re-answered the same path request with byte-identical bytes; the
   interface gave it a fresh pkt_id and sent all three fragments again
   while the receiver's bucket still held two of them under the old
   pkt_id (`reassembly_idle_timeout` is 120s). `_resumable_sends` now
   remembers, per (peer, payload hash), the pkt_id and per-fragment
   delivery state of a failed fragmented send for 75% of the receiver's
   idle timeout measured from the last confirmed delivery; a re-send of
   identical bytes to the same peer inside that window reuses the pkt_id,
   skips the fragments already held, and ALWAYS runs the reconcile QUERY
   afterwards so a bucket the receiver has since evicted is detected (the
   answer is authoritative, see 2) rather than assumed. Off for
   handshake-priority sends, which never reconcile. Captured as a
   `direct_resume` record.

Also seen in the same captures and left alone: the hop-1 abort's first
field firings (31 aborts at 5.0-6.3s in place of 20-28s timeouts; echo
seen on 134 of 139 successful multi-hop attempts and every echo-less
success ACKed inside its deadline, so no false abort); reconcile ANSWERs
lost at 2-3 hops in 4 of 15 queries (single-shot by design -- a retried
QUERY is the next candidate if this recurs); three local `send_msg`
failures around the laptop's radio restarts with no degradation visible
beforehand (command latency flat at 0.02-0.03s).

**Raw binary DIRECT fragments (2026-09-18 night, user-approved "go for
it" on the airtime review; `direct_raw_fragments_enabled`, DEFAULT OFF
until validated on hardware).** The largest remaining airtime cost was
per-fragment overhead: a DIRECT text fragment carries 114 bytes of RNS
payload in 166 bytes on air (Z85's 25%, our 6-byte header, the firmware's
text framing and 16-byte cipher padding) and costs a firmware ACK frame
plus an ACK wait per fragment -- measured sender idle beyond the
fragment's own airtime of 1.0/2.3/3.0/5.7s at 0/1/2/3 hops. The
companion firmware has a raw packet type the host can drive directly:
`CMD_SEND_RAW_DATA` (25) -> `Mesh::createRawData` -> `sendDirect(path)`,
delivered at the far end as `PUSH_CODE_RAW_DATA` (0x84) with SNR/RSSI;
the installed `meshcore` library (2.3.9.1) exposes it as
`commands.send_raw_data(payload, path)` and `EventType.RAW_DATA`. No
Z85, no text framing, no encryption (RNS already encrypts end to end),
and no firmware ACK. Limits, from source: `MAX_FRAME_SIZE` 176 on the
companion serial link caps a raw payload at 173 bytes received
(`onRawDataRecv`: payload + 4 push bytes must fit) and 174 minus the
path length sent (`CMD_SEND_RAW_DATA` frame = cmd + path_len + path +
payload); `Mesh.cpp` marks a raw packet seen and delivers it to every
node that hears it with its path exhausted, and byte-identical repeats
are dropped by `wasSeen` at repeaters and receivers alike -- so every
retransmission must differ.

Design (all of it reuses the existing reliability engine rather than
adding a second one):

- *Frame.* 13-byte header `[ver<<4 | attempt&3][dst_prefix:2]
  [src_prefix:6][pkt_id:2][frag_idx:1][frag_total:1]` + payload
  (`_encode_raw_fragment`/`_decode_raw_fragment`). `dst_prefix` (first 2
  bytes of the receiver's pubkey) filters the "every listener gets it"
  delivery; `src_prefix` is the sender's 6-byte prefix, the SAME token
  the text path gets from CONTACT_MSG_RECV, so raw fragments land in the
  same `_reassembly_key` bucket and the same `"Q"` completion QUERY can
  ask about them. The attempt bits change per re-drive round so a retry
  is never byte-identical (the `wasSeen` rule above). Per-fragment RNS
  payload is `min(direct_raw_payload_cap, 173, 174 - path_len) - 13`:
  157 at zero hop, so a 483-byte Resource part is 4 raw fragments
  (~0.72s each at SF7/BW62.5/CR8) instead of 5 text ones plus 5 ACKs --
  about 43% less sender airtime, and no ACK idle at all.
- *Reliability = the reconcile bitmap.* `_send_direct_raw_fragmented`
  bursts every missing fragment under `_direct_exchange_lock` (spaced by
  `direct_raw_zero_hop_gap`, or `direct_raw_hop_gap_factor` x airtime
  when a repeater must forward each one first), releases the lock, then
  asks the receiver what it holds with the existing `"Q"` QUERY (up to
  `direct_raw_query_attempts` tries), applies the answer authoritatively
  and repeats for up to `direct_raw_reconcile_rounds`. Resume
  (`_resumable_sends`) works unchanged. Handshake-priority packets never
  go raw (they keep the ACKed text path), nor does anything that fits a
  bare text frame.
- *Capability-gated.* A bind frame now advertises `BIND_CAP_RAW_
  FRAGMENTS` (0x02) when the flag is on; `_PeerRecord.raw_fragments`
  (tri-state, persisted in the peer cache like `has_upstream_rns`) must
  be True for a peer to receive raw fragments, so an old build never
  gets frames it cannot hear. Off by default until both radios have run
  it; flip it on both sides for the test.
- *Self-disabling fallback.* If a reconcile ANSWER arrives (the text
  path works) but shows the burst delivered nothing, twice, raw is
  disabled for that peer for `direct_raw_fallback_cooldown` (600s) and
  the packet is re-sent as text fragments -- the guard for the one
  unverified assumption, that every repeater on the path forwards
  PAYLOAD_TYPE_RAW_CUSTOM (Mesh.cpp forwards DIRECT packets by route type
  and path hash, not payload type, but the public repeater has not been
  tested). A raw failure with the QUERY itself unanswered counts as a
  path failure like any other; one with an answered QUERY does not.
- *Receive.* `_on_raw_data` (subscribed only when the library has
  `RAW_DATA`) drops anything without our version nibble or dst prefix
  silently -- other applications' raw packets are not ours to log -- and
  hands the rest to `_handle_direct_multifragment_frame(raw=True)`, which
  deliberately skips token learning: the raw src prefix is unauthenticated
  (a text frame's `pubkey_prefix` comes from the firmware's decryption),
  and bulk data only flows on links whose tokens were learned from the
  authenticated handshake anyway. Captured as `raw_fragment_sent` on the
  sender and transport `direct_raw_multifragment` on the receiver.

Verified in the simulator (`testscripts/simmesh` gained `send_raw_data`,
`RAW_DATA` and the RAW_CUSTOM packet type with the firmware's
seen-dedup): see `tests/test_raw_fragments.py`. Real hardware: not yet.

Two robustness fixes found while getting that suite to run reliably
(both pre-existing, confirmed by running the committed alpha-0.1.1 tree
against the same scenarios): `_outgoing_worker` now waits on the queue in
one-second slices -- an unbounded `queue.get()` inside the default
executor is a non-daemon thread the interpreter joins at exit, so any
process that constructed an interface and never `detach()`ed it (a test
whose setUp failed, a script that just exits) hung forever; and
`_register_peer`'s proactive discovery on bind gained one retry after the
bind-response window (`_discover_path_after_bind`), because the first
attempt races the peer's telemetry grant and, once denied, nothing
retried it until real traffic -- the "DIRECT paths never resolved"
flakiness in the simulated scenarios, and the same startup race the M5
field notes describe.

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
     project already hit once. `tests/test_smart_meshcore_interface_config.py`
     enforces this with a static AST scan, mirroring
     `tests/test_config_usage.py`'s pattern for the old implementation.
"""

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


def _cfg_bool(value) -> bool:
    """Parse a ConfigObj string value as a boolean the way the rest of this
    interface's config surface does: only an explicit falsy string turns a
    flag off, so an unrecognized value fails safe toward the (documented)
    default behavior rather than silently disabling something."""
    return str(value).strip().lower() not in ("no", "false", "0")


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

    def __init__(self):
        self._locked = False
        self._waiters: "dict[int, collections.deque]" = {}

    def locked(self) -> bool:
        return self._locked

    async def acquire(self, priority: int) -> None:
        if not self._locked:
            self._locked = True
            return
        fut = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(priority, collections.deque()).append(fut)
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

    def __call__(self, priority: int) -> "_PriorityLockContext":
        return _PriorityLockContext(self, priority)


class _PriorityLockContext:
    """`async with priority_lock(priority):` sugar -- `_PriorityAsyncLock`
    itself isn't a context manager (it needs a `priority` argument
    `asyncio.Lock`'s own `__aenter__` has no room for), so `__call__`
    returns one of these instead, exactly the way `asyncio.Lock` fits an
    `async with` block despite `acquire`/`release` being its own real
    methods."""

    __slots__ = ("_lock", "_priority")

    def __init__(self, lock: _PriorityAsyncLock, priority: int):
        self._lock = lock
        self._priority = priority

    async def __aenter__(self) -> None:
        await self._lock.acquire(self._priority)

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._lock.release()


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

    async def wait_for_budget(self, estimated_duration_s: float) -> float:
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
            await asyncio.sleep(wait_s)
            total_wait += wait_s

    def record(self, duration_s: float) -> None:
        self._samples.append((time.monotonic(), duration_s))


class SmartMeshCoreInterface(Interface):
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
    PRIORITY_NORMAL = 1
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
    PRIORITY_LOW = 2

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
    COMPLETION_PROTOCOL_VERSION = 2
    COMPLETION_PROTOCOL_VERSION_V1 = 1
    COMPLETION_TYPE_QUERY = 0
    COMPLETION_TYPE_ANSWER = 1
    COMPLETION_FRAME_RAW_SIZE = 6  # ver+type+complete+pkt_id(2)+frag_total -- the fixed body

    # --- Raw binary DIRECT fragments (2026-09-18 night, module docstring) ---
    # [ver<<4 | attempt&3 : 1][dst_prefix : 2][src_prefix : 6][pkt_id : 2]
    # [frag_idx : 1][frag_total : 1] then payload. No marker character: a
    # raw packet is its own MeshCore payload type; the version nibble and
    # dst prefix are the filter against other applications' raw packets.
    RAW_PROTOCOL_VERSION = 1
    RAW_HEADER_SIZE = 13
    RAW_DST_PREFIX_BYTES = 2
    # Companion firmware limits (MAX_FRAME_SIZE 176 on the serial link):
    # onRawDataRecv pushes payload + 4 bytes, CMD_SEND_RAW_DATA carries
    # cmd + path_len + path + payload -- both confirmed in
    # examples/companion_radio/MyMesh.cpp and BaseSerialInterface.h.
    FIRMWARE_RAW_RX_PAYLOAD_LIMIT = 173
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
        self._last_firmware_ack_timeout_s = {}
        # Per-peer repeater-echo timings (seconds after our own transmit
        # that the first hop was heard forwarding our frame), the data
        # behind the early hop-1 abort -- see _hop1_abort_deadline_s.
        self._echo_stats = {}
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
        self._rns_token_peer = {}
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
    # Config loading (design invariant #3: every value read here must be
    # used somewhere else in this file -- see
    # tests/test_smart_meshcore_interface_config.py)
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
        # §2). The zero-hop and known-N-hop tiers are wired into
        # _fragment_spacing_range() and unit-tested, but this milestone
        # has no live hop-count data source yet (Milestone 4/5's path
        # discovery/peer-topology work) -- every real send today resolves
        # to the flat unknown-multi-hop range below.
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
        self.reassembly_max_keys = int(cfg.get("reassembly_max_keys", 256))
        self.reassembly_idle_timeout_s = float(cfg.get("reassembly_idle_timeout", 120.0))
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
        # docstring). DEFAULT OFF until validated on real radios: it needs
        # both peers on this build (capability-gated by bind frame) and
        # one check that the public repeater forwards raw packets. When
        # on: packets too large for one text frame go to a raw-capable
        # peer as unacknowledged raw bursts reconciled by the "Q" bitmap.
        self.direct_raw_fragments_enabled = _cfg_bool(cfg.get("direct_raw_fragments_enabled", "yes"))
        # Per-fragment raw payload cap on the wire, before the 13-byte
        # header; also bounded by the firmware limits above.
        self.direct_raw_payload_cap = int(cfg.get("direct_raw_payload_cap", 170))
        # Spacing between fragments of one burst: a flat gap at zero hop
        # (the receiver sends no ACK, so only its own processing needs
        # covering), or this many airtimes when a repeater must forward
        # each fragment before it can hear the next one.
        self.direct_raw_zero_hop_gap_s = float(cfg.get("direct_raw_zero_hop_gap", 0.15))
        self.direct_raw_hop_gap_factor = float(cfg.get("direct_raw_hop_gap_factor", 2.0))
        # Burst-then-ask rounds per packet, and QUERY tries per round.
        self.direct_raw_reconcile_rounds = int(cfg.get("direct_raw_reconcile_rounds", 3))
        self.direct_raw_query_attempts = int(cfg.get("direct_raw_query_attempts", 2))
        # Two answered reconciles in a row showing a burst delivered
        # nothing -> raw is disabled for that peer for this long and the
        # packet goes as text fragments instead.
        self.direct_raw_fallback_cooldown_s = float(cfg.get("direct_raw_fallback_cooldown", 600.0))

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

        # Field-diagnosed (same capture): a packet that has sat in this
        # interface's queue (or behind _direct_exchange_lock) longer than
        # this is dropped instead of sent -- 17 LXMF pings queued through
        # a 4-minute outage drained as a stale burst the moment the path
        # came back. ANNOUNCE is exempt (idempotent, and RNS won't re-send
        # one soon). 0 disables. Default matches reassembly_idle_timeout.
        # Refined the same evening (page-load capture, see module
        # docstring): the decision is made ONCE, before a packet's first
        # transmission -- never between fragments or attempts, where a drop
        # only wastes the air already spent -- and Resource data parts
        # (context RESOURCE) are exempt: RNS's Resource layer owns their
        # retransmission and re-requests what it lacks.
        self.outgoing_max_age_s = float(cfg.get("outgoing_max_age", 120.0))

    def _configure_path_discovery(self, cfg):
        # docs/path_discovery_spec.md's "Retry and backoff structure" --
        # a quick-retry burst (each attempt already naturally spaced by
        # its own request/response wait, no additional artificial delay
        # layered on top), then per-target exponential backoff.
        self.path_discovery_quick_attempts = int(cfg.get("path_discovery_quick_attempts", 2))
        self.path_discovery_base_cooldown_s = float(cfg.get("path_discovery_base_cooldown", 20.0))
        self.path_discovery_max_cooldown_s = float(cfg.get("path_discovery_max_cooldown", 900.0))
        self.path_discovery_backoff_factor = float(cfg.get("path_discovery_backoff_factor", 1.8))

        # Stale cached-DIRECT-path detection (§8) -- not yet wired into an
        # actual DIRECT send path (Milestone 5+ adds the routing decisions
        # that call record_direct_send_result() for real); the mechanism
        # and its config surface exist now, unit-tested directly.
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
        self.direct_post_send_listen_min_s = float(cfg.get("direct_post_send_listen_min", 0.3))
        self.direct_post_send_listen_max_s = float(cfg.get("direct_post_send_listen_max", 3))
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

        if attempts_that_fit < self.direct_send_attempts:
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
                f"direct_send_attempts={self.direct_send_attempts}. The receiver can evict a "
                f"bucket while the sender is still working through that fragment's first "
                f"attempt budget -- real queueing delay and this message's other fragments only "
                f"make it worse. Fix by lowering direct_ack_timeout_routed_max/"
                f"direct_post_send_listen_max, lowering direct_send_attempts, or raising "
                f"reassembly_idle_timeout to at least "
                f"{clock_racing_attempt_s * self.direct_send_attempts:.0f}. "
                f"(incoming_quiet_defer_max_wait="
                f"{self.incoming_quiet_defer_max_wait_s if self.incoming_quiet_defer_enabled else 0:.1f}s "
                f"is excluded above -- only a message's first transmission pays it -- but "
                f"re-broadening that gate's trigger would add it to every attempt here.)",
                RNS.LOG_WARNING,
            )

    # -------------------------------------------------------------------
    # Startup helpers
    # -------------------------------------------------------------------

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

    def _capture_fragment_received(self, mode: str, sender_token: str, pkt_id: int, frag_idx: int, frag_total: int, progress: int) -> None:
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
        })

    def _capture_channel_fragment_sent(
        self, pkt_id: int, attempt: int, frag_idx: int, frag_total: int, position: int,
        ok: bool, size_bytes: int,
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
        })

    def _capture_direct_send_result(
        self, peer_prefix: str, destination_hash: Optional[bytes], ok: bool,
        resolved: "_ResolvedPath", size_bytes: int,
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

    async def _throttle_for_duty_cycle(self, frame: str, exempt: bool = False, on_air_bytes: Optional[int] = None) -> float:
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
        if exempt:
            # Link-maintenance traffic: charged, never delayed.
            self._duty_cycle.record(estimated_s)
            self._debug(
                f"duty-cycle: handshake-class {len(frame)}-char frame sent without waiting "
                f"for budget ({estimated_s:.2f}s airtime still charged to the window)."
            )
            return 0.0
        delay = await self._duty_cycle.wait_for_budget(estimated_s)
        self._duty_cycle.record(estimated_s)
        if delay > 0:
            self._debug(
                f"duty-cycle throttle: waited {delay:.2f}s before this "
                f"{len(frame)}-char frame (estimated {estimated_s:.2f}s airtime, "
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
        on_air_bytes: Optional[int] = None,
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
        duty_cycle_wait_s = await self._throttle_for_duty_cycle(frame, exempt=duty_cycle_exempt, on_air_bytes=on_air_bytes)
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
        self.online = True
        if was_offline:
            RNS.log(f"{self}: MeshCore connection (re)established.", RNS.LOG_INFO)
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
            try:
                sf, bw, cr = int(info.get("radio_sf", 0)), float(info.get("radio_bw", 0)), int(info.get("radio_cr", 0))
                if sf >= 5 and bw > 0 and 5 <= cr <= 8:
                    self._radio_params = (sf, bw, cr)
            except (TypeError, ValueError):
                pass
            RNS.log(
                f"{self}: node identity '{self._own_node_name}' "
                f"key={node_key[:16]}...",
                RNS.LOG_INFO,
            )

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
                await asyncio.sleep(self.stats_interval_s)
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
        if version not in (self.COMPLETION_PROTOCOL_VERSION_V1, self.COMPLETION_PROTOCOL_VERSION):
            raise ValueError(f"unsupported completion-frame version {version}")
        if frame_type not in (self.COMPLETION_TYPE_QUERY, self.COMPLETION_TYPE_ANSWER):
            raise ValueError(f"unrecognized completion-frame type {frame_type}")

        pkt_id = (raw[3] << 8) | raw[4]
        frag_total = raw[5]
        held = None
        if version >= 2 and frame_type == self.COMPLETION_TYPE_ANSWER:
            expected = self.COMPLETION_FRAME_RAW_SIZE + self._completion_bitmap_size(frag_total)
            if len(raw) != expected:
                raise ValueError(f"completion ANSWER wrong length: {len(raw)} (expected {expected} for frag_total={frag_total})")
            bitmap = raw[self.COMPLETION_FRAME_RAW_SIZE:]
            held = frozenset(i for i in range(frag_total) if bitmap[i // 8] & (1 << (i % 8)))
        elif len(raw) != self.COMPLETION_FRAME_RAW_SIZE:
            raise ValueError(f"completion frame wrong length: {len(raw)} (expected {self.COMPLETION_FRAME_RAW_SIZE})")
        return _CompletionFrame(
            version=version, type=frame_type, complete=bool(complete_byte),
            pkt_id=pkt_id, frag_total=frag_total, held=held,
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
        pkt_id: int, frag_idx: int, frag_total: int, attempt: int,
    ) -> bytes:
        header = (
            bytes([(self.RAW_PROTOCOL_VERSION << 4) | (attempt & 0x03)])
            + bytes.fromhex(dst_pubkey_hex[: self.RAW_DST_PREFIX_BYTES * 2])
            + bytes.fromhex(src_prefix_hex[: self.BIND_PUBKEY_PREFIX_BYTES * 2])
            + pkt_id.to_bytes(2, "big")
            + bytes([frag_idx & 0xFF, frag_total & 0xFF])
        )
        return header + payload

    def _decode_raw_fragment(self, raw: bytes) -> "tuple[_FrameHeader, bytes, str, bytes]":
        """Returns (header, payload, src_prefix_hex, dst_prefix_bytes).
        Raises ValueError for anything that isn't one of ours -- callers
        drop those silently, since other applications' raw packets share
        this payload type."""
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
        until = self._raw_disabled_until.get(peer_prefix)
        return not (until is not None and time.monotonic() < until)

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
        this method only implements per-value tier selection, since
        Milestone 2 has no audience/topology concept yet to do that
        resolution against (every real call today passes `None`)."""
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
        priority = self._priority_tier(header)
        # Field fix (2026-09-18 evening): identical bytes already queued or
        # in flight -> drop. RNS's Resource layer re-requests parts every
        # ~27s while the earlier copy is still waiting on the duty-cycle
        # limiter; the page-load capture queued 26 RESOURCE packets for 12
        # distinct payloads. The receiver would dedup them anyway.
        inflight_key = RNS.Identity.truncated_hash(raw)
        with self._outgoing_inflight_lock:
            duplicate = inflight_key in self._outgoing_inflight
            if not duplicate:
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
                if self._expired(expires_at):
                    self._outgoing_dropped_total += 1
                    self._capture_outgoing(header, data, "expired_in_queue")
                    RNS.log(
                        f"{self}: dropping outgoing packet ({len(data)} bytes) -- sat "
                        f"{time.monotonic() - enqueued_at:.0f}s in the outgoing queue, past "
                        f"outgoing_max_age={self.outgoing_max_age_s:.0f}s.",
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
        self, data: bytes, priority: int = PRIORITY_NORMAL, expires_at: Optional[float] = None,
        spawned: Optional[list] = None,
    ) -> None:
        """Small-mesh replacement for a CHANNEL broadcast: one DIRECT
        copy to every bound peer instead, each spawned independently
        (never gated on another's outcome, same reasoning as every other
        fire-and-forget send in this dispatcher). Reuses
        `_send_direct_supplement` unchanged for the actual send -- it
        already handles path-resolution-with-discovery, spacing, and
        bare-vs-fragmented dispatch correctly regardless of caller."""
        for peer_prefix in self._all_bound_peer_prefixes():
            task = self._spawn_background_task(
                self._send_direct_supplement(
                    data, peer_prefix, trigger_discovery=True, priority=priority, expires_at=expires_at,
                )
            )
            if spawned is not None:
                spawned.append(task)

    def _unknown_dest_in_backoff(self, destination_hash: Optional[bytes]) -> bool:
        if destination_hash is None:
            return False
        until = self._unknown_dest_backoff_until.get(destination_hash)
        return until is not None and time.monotonic() < until

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
                self._record_unknown_dest_attempt(header.destination_hash)
                await self._send_direct_to_all_peers(
                    data, priority=self._priority_tier(header), expires_at=expires_at, spawned=spawned,
                )
                return
            bootstrap_targets = [] if backed_off else self._select_bootstrap_supplement_targets()
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
                    data, priority=self._priority_tier(header), expires_at=expires_at, spawned=spawned,
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
            self._spawn_background_task(
                self._delayed_retry_pass(data, pkt_id, attempt, expires_at, duty_cycle_exempt)
            )

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
                data, priority=self._priority_tier(header), expires_at=expires_at, spawned=spawned,
            )
            return
        self._debug("routing decision: path request -> CHANNEL broadcast + router-peer DIRECT supplement.")
        supplement_targets = self._select_direct_supplement_targets()
        self._capture_outgoing(
            header, data, "broadcast_path_request_supplement", candidate_peers=supplement_targets,
        )
        tasks = [self._spawn_background_task(self._send_broadcast_packet(data, header, expires_at=expires_at))]
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
    ) -> None:
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
        whole point of that mechanism."""
        resolved = self._resolved_paths.get(peer_prefix)
        if resolved is None:
            if not trigger_discovery:
                return
            resolved = await self._discover_path_coalesced(peer_prefix)
            if resolved is None:
                return

        # This design's own minimum inter-message gap (reliability_engine_
        # design.md §2), scaled by this specific peer's own known hop
        # depth when available -- routing_decisions.md's fix for the
        # half-duplex-deaf-repeater collision risk this supplement would
        # otherwise recreate against the broadcast's own fragment(s) if
        # fired with zero spacing.
        spacing_min, spacing_max = self._fragment_spacing_range(hop_count=resolved.out_path_len)
        await asyncio.sleep(random.uniform(spacing_min, spacing_max))
        if self.detached or not self.online:
            return
        if self._expired(expires_at):
            self._outgoing_dropped_total += 1
            self._debug(f"DIRECT supplement to {peer_prefix!r} skipped -- packet expired (outgoing_max_age).")
            return

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
            return
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
            await self._send_broadcast_packet(data, header)
            return

        contact = self._resolve_contact(peer_prefix)
        target = contact.get("public_key") if contact is not None else None
        if not target:
            await self._send_broadcast_packet(data, header)
            return

        ok = await self._send_direct_payload(
            target, peer_prefix, data, priority=self._priority_tier(header), hop_count=resolved.out_path_len,
            expires_at=expires_at,
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
            await self._send_broadcast_packet(data, header)
            return

        self._capture_direct_send_result(
            peer_prefix, header.destination_hash if header is not None else None, ok, resolved, len(data),
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
        fastpath_budget = self._direct_payload_budget()
        if len(data) <= fastpath_budget:
            return await self._send_direct_with_attempts(
                target, lambda attempt, d=data: self._encode_direct_bare(d), peer_prefix,
                priority=priority, hop_count=hop_count, expires_at=expires_at,
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
        if self._raw_fragments_eligible(peer_prefix, priority):
            raw_result = await self._send_direct_raw_fragmented(
                target, peer_prefix, data, pkt_id, priority=priority, hop_count=hop_count,
                expires_at=expires_at, resume=resume, resume_key=resume_key,
            )
            if raw_result is not None:
                return raw_result
            # None: raw declined or fell back mid-way -- fresh pkt_id, text path.
            pkt_id = self._next_pkt_id()
            resume = None
        return await self._send_direct_fragmented(
            target, peer_prefix, data, pkt_id, priority=priority, hop_count=hop_count,
            expires_at=expires_at, resume=resume, resume_key=resume_key,
        )

    async def _send_raw_fragment(
        self, path: bytes, frame: bytes, priority: int, telemetry: Optional[dict] = None,
    ) -> bool:
        """One raw fragment out through the same gate every transmission
        passes (quiet defer skipped: a burst is always racing the
        receiver's reassembly clock), then CMD_SEND_RAW_DATA. Returns
        whether the firmware accepted it; never waits for anything after."""
        on_air = 2 + len(path) + len(frame)
        gate = await self._pre_transmit_gate(
            "", skip_quiet_defer=True, duty_cycle_exempt=self._duty_cycle_exempt(priority), on_air_bytes=on_air,
        )
        if telemetry is not None:
            telemetry["quiet_defer_wait_s"], telemetry["duty_cycle_wait_s"], telemetry["medium_hold_wait_s"] = gate
        await self._run_command(
            self._mc_ready.commands.send_raw_data(frame, path), "send_raw_data", self._EventType.OK,
        )
        self.txb += len(frame)
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

        acked = [False] * frag_total
        resumed = False
        if resume is not None and resume.get("frag_total") == frag_total and len(resume.get("acked", ())) == frag_total:
            acked = list(resume["acked"])
            resumed = True
            if self._packet_capture_file is not None:
                self._capture_event("out", {
                    "event": "direct_resume", "peer_prefix": peer_prefix, "pkt_id": pkt_id,
                    "frag_total": frag_total, "held_before": [i for i, a in enumerate(acked) if a], "raw": True,
                })
        self._last_fragmented_pkt_id = pkt_id
        self._last_fragmented_frag_total = frag_total
        self._debug(
            f"RAW fragmented send starting: pkt_id={pkt_id} to {peer_prefix!r} frag_total={frag_total} "
            f"budget={budget}B path_len={len(path)} hop_count={hop_count}{' (resumed)' if resumed else ''}."
        )
        if hop_count is not None and hop_count >= 1:
            gap_s = self.direct_raw_hop_gap_factor * self._estimate_tx_airtime_s("", on_air_bytes=2 + len(path) + self.RAW_HEADER_SIZE + budget)
        else:
            gap_s = self.direct_raw_zero_hop_gap_s
        last_progress_at = time.monotonic() if resumed else None
        empty_answered_bursts = 0

        def remember() -> None:
            if resume_key is not None and any(acked) and last_progress_at is not None:
                self._resumable_sends[resume_key] = {
                    "pkt_id": pkt_id, "frag_total": frag_total, "acked": list(acked),
                    "expires_at": last_progress_at + 0.75 * self.reassembly_idle_timeout_s,
                }

        rounds = max(1, self.direct_raw_reconcile_rounds)
        query_unanswered_rounds = 0
        for rnd in range(rounds):
            missing = [i for i in range(frag_total) if not acked[i]]
            if missing:
                async with self._direct_exchange_lock(priority):
                    for n, frag_idx in enumerate(missing):
                        if self.detached or not self.online:
                            remember()
                            return False
                        frame = self._encode_raw_fragment(
                            chunks[frag_idx], target, own_prefix, pkt_id, frag_idx, frag_total, attempt=rnd,
                        )
                        telemetry: dict = {}
                        try:
                            await self._send_raw_fragment(path, frame, priority, telemetry)
                            sent_ok = True
                        except Exception as exc:
                            sent_ok = False
                            RNS.log(f"{self}: raw fragment send failed locally (pkt_id={pkt_id} frag_idx={frag_idx}): {exc}", RNS.LOG_WARNING)
                        if self._packet_capture_file is not None:
                            self._capture_event("out", {
                                "event": "raw_fragment_sent", "peer_prefix": peer_prefix, "pkt_id": pkt_id,
                                "frag_idx": frag_idx, "frag_total": frag_total, "round": rnd, "ok": sent_ok,
                                "size_bytes": len(frame), "path_len": len(path), "hop_count": hop_count,
                                "duty_cycle_wait_s": telemetry.get("duty_cycle_wait_s"),
                                "medium_hold_wait_s": telemetry.get("medium_hold_wait_s"),
                            })
                        if n < len(missing) - 1 and gap_s > 0:
                            await asyncio.sleep(gap_s)
            held_before = sum(acked)
            answer = None
            for q in range(max(1, self.direct_raw_query_attempts)):
                answer = await self._query_remote_fragments(
                    target, peer_prefix, pkt_id, frag_total, stage=f"raw{rnd}", priority=priority,
                )
                if answer is not None or self.detached or not self.online:
                    break
            if self.detached or not self.online:
                remember()
                return False
            if answer is None:
                query_unanswered_rounds += 1
                self._debug(f"RAW send pkt_id={pkt_id} to {peer_prefix!r}: round {rnd} reconcile unanswered.")
                continue
            held = set(range(frag_total)) if answer.complete else set(answer.held or ())
            acked = [i in held for i in range(frag_total)]
            if held:
                last_progress_at = time.monotonic()
            self._debug(
                f"RAW send pkt_id={pkt_id} to {peer_prefix!r}: round {rnd} -- receiver holds "
                f"{sorted(held)} of {frag_total}."
            )
            if all(acked):
                self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
                self._resumable_sends.pop(resume_key, None)
                return True
            if sum(acked) <= held_before and missing:
                empty_answered_bursts += 1
                if empty_answered_bursts >= 2:
                    # The text path works (the ANSWER came back) but raw
                    # frames are not arriving: a repeater or firmware that
                    # does not carry them. Fall back for this peer.
                    self._raw_disabled_until[peer_prefix] = time.monotonic() + self.direct_raw_fallback_cooldown_s
                    RNS.log(
                        f"{self}: raw fragments to {peer_prefix!r} are not arriving (two answered reconciles, "
                        f"nothing new held) -- disabling raw for this peer for "
                        f"{self.direct_raw_fallback_cooldown_s:.0f}s and re-sending as text fragments.",
                        RNS.LOG_WARNING,
                    )
                    return None
            else:
                empty_answered_bursts = 0

        remember()
        # Unanswered throughout: nothing is known about the path -> a real
        # failure. Answered but incomplete: the path works, the data did not
        # all get there -- not a path failure.
        self.record_direct_send_result(
            peer_prefix, succeeded=False, waited_full_timeout=(query_unanswered_rounds == rounds),
        )
        RNS.log(
            f"{self}: RAW fragmented send pkt_id={pkt_id} to {peer_prefix!r} gave up after {rounds} round(s): "
            f"receiver holds {sum(acked)}/{frag_total}.",
            RNS.LOG_WARNING,
        )
        return False

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
        acked = [False] * frag_total
        # Alpha 0.1.1 resume (see _send_direct_payload): start from what the
        # receiver is believed to hold; the reconcile QUERY below is forced
        # so that belief is checked against the receiver's actual bucket.
        resumed = False
        if resume is not None and resume.get("frag_total") == frag_total and len(resume.get("acked", ())) == frag_total:
            acked = list(resume["acked"])
            resumed = True
            self._debug(
                f"DIRECT fragmented send pkt_id={pkt_id} to {peer_prefix!r}: RESUMING a failed send -- "
                f"receiver believed to hold {[i for i, a in enumerate(acked) if a]} of {frag_total}."
            )
            if self._packet_capture_file is not None:
                self._capture_event("out", {
                    "event": "direct_resume", "peer_prefix": peer_prefix, "pkt_id": pkt_id,
                    "frag_total": frag_total, "held_before": [i for i, a in enumerate(acked) if a],
                })
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
            # Alpha 0.1.1: a failed send with something delivered is worth
            # resuming if RNS re-issues these bytes while the receiver's
            # bucket is still alive (its idle clock restarted at our last
            # confirmed delivery; keep a 25% margin under its timeout).
            if resume_key is None or not reconcile or not any(acked) or last_progress_at is None:
                return
            self._resumable_sends[resume_key] = {
                "pkt_id": pkt_id, "frag_total": frag_total, "acked": list(acked),
                "expires_at": last_progress_at + 0.75 * self.reassembly_idle_timeout_s,
            }

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
                target, peer_prefix, pkt_id, frag_total, stage="reconcile", priority=priority,
            )
            if self.detached or not self.online:
                remember_for_resume()
                return False
            if answer is not None:
                held = set(range(frag_total)) if answer.complete else set(answer.held or ())
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
                target, peer_prefix, pkt_id, frag_total, priority=priority,
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
        priority: int = PRIORITY_NORMAL,
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
            target, peer_prefix, pkt_id, frag_total, stage="final", priority=priority,
        )
        return answer is not None and answer.complete

    def _completion_query_timeout_s(self, peer_prefix: str) -> float:
        """`direct_completion_check_timeout_s`, or longer when this peer's
        measured ACK RTT (step 2) says a QUERY+ANSWER round trip -- two
        DIRECT exchanges back to back, each with its own firmware ACK --
        plausibly takes more than that. Never shorter than the config
        value; the RTT-derived part is capped at
        `direct_ack_timeout_routed_max_s` (see below)."""
        st = self._ack_rtt.get(peer_prefix) or self._ack_rtt_snapshot.get(peer_prefix)
        timeout_s = self.direct_completion_check_timeout_s
        if st is None:
            # Field fix (2026-09-18 evening): no RTT information at all --
            # the firmware's hop-aware ACK bound for this peer, doubled for
            # the two exchanges a QUERY+ANSWER round trip is, is still far
            # better than a flat 5s at 3 hops (where one ACK alone took
            # ~4.5s in the drive-home capture).
            fw = self._last_firmware_ack_timeout_s.get(peer_prefix)
            if fw is not None:
                timeout_s = max(timeout_s, min(2.0 * fw, self.direct_ack_timeout_routed_max_s))
        if st is not None:
            # Pre-field-test tweak (2026-09-18): capped at the same ceiling
            # an ACK wait has. This wait now holds _direct_exchange_lock,
            # and the estimator's initial spread (rttvar = rtt/2) makes
            # 3*(srtt + 4*rttvar) about nine times the measured RTT --
            # 20-30s at 1-2 hops for one unanswered query. The cap keeps
            # the worst case equal to one already-accepted ACK timeout.
            timeout_s = max(timeout_s, min(
                3.0 * (st["srtt"] + 4.0 * st["rttvar"]), self.direct_ack_timeout_routed_max_s,
            ))
        if self.rx_log_holds_enabled:
            # Code review (2026-09-18): the peer's ANSWER pays step 4's
            # pre-transmit hold (up to rx_log_hold_max_s) before it can
            # leave -- assuming the peer runs the same hold cap, which is
            # the best this side can know.
            timeout_s += self.rx_log_hold_max_s
        return timeout_s

    async def _query_remote_fragments(
        self, target: str, peer_prefix: str, pkt_id: int, frag_total: int, stage: str,
        priority: int = PRIORITY_NORMAL,
    ) -> Optional[_CompletionFrame]:
        """Step 3 (2026-09-18, see module docstring): one `"Q"` QUERY to the
        receiver, answered with its have-bitmap (v2) or a bare complete
        flag (a v1 peer). Returns the decoded ANSWER, or None if none
        arrived (lost, or the peer predates `"Q"`/v2 frames) -- callers
        treat None as "no information", never as "nothing arrived".
        `stage` is "reconcile" (between pass 0 and pass 1) or "final"
        (after pass 1, the pre-step-3 last resort) -- capture-only.

        Code review (2026-09-18): this is one DIRECT exchange -- QUERY out,
        ANSWER back -- and is treated as one: `_direct_exchange_lock` is
        held from the transmit until the ANSWER arrives or the wait times
        out, exactly as `_send_direct_frame_and_wait_for_ack` holds it
        through an ACK wait. The previous shape released the lock as soon
        as `send_msg` returned, while the QUERY's own firmware ACK and the
        peer's ANSWER were both still on their way -- so the next queued
        send could key the radio into the very reply this node was
        waiting for, the collision the lock exists to prevent. `priority`
        is the enclosing send's own tier (the reconcile stage sits inside
        a fragmented send whose receiver-side clock is already running;
        queueing it behind every ordinary send at PRIORITY_LOW defeated
        its purpose), and the QUERY is `time_critical` for the same
        reason. No ACK wait of its own: the ANSWER supersedes it."""
        key = (peer_prefix, pkt_id)
        fut = asyncio.get_running_loop().create_future()
        self._completion_query_waiters[key] = fut
        outcome = "send_failed"
        answer: Optional[_CompletionFrame] = None
        timeout_s = self._completion_query_timeout_s(peer_prefix)
        try:
            frame = self._encode_completion_frame(self.COMPLETION_TYPE_QUERY, pkt_id, frag_total)
            async with self._direct_exchange_lock(priority):
                try:
                    await self._send_direct_frame(target, frame, time_critical=True)
                except Exception as exc:
                    self._debug(
                        f"completion QUERY ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}): "
                        f"send failed locally: {exc} -- treating as no answer."
                    )
                    return None
                try:
                    got: _CompletionFrame = await asyncio.wait_for(fut, timeout=timeout_s)
                    answer = got
                    outcome = "answered"
                    self._debug(
                        f"completion ANSWER ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}): v{got.version} "
                        f"complete={got.complete} held={sorted(got.held) if got.held is not None else None}."
                    )
                    return got
                except asyncio.TimeoutError:
                    outcome = "timeout"
                    self._debug(
                        f"completion QUERY ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}): "
                        f"no answer within {timeout_s:.1f}s -- no information, proceeding as if unanswered."
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
        expires_at: Optional[float] = None,
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
        for attempt in range(attempts_budget):
            if self.detached or not self.online:
                return False
            if attempt == 0 and self._expired(expires_at):
                # Field fix (2026-09-18 evening): outgoing_max_age. Not a
                # path failure (nothing was learned about the path), so no
                # record_direct_send_result call; counted as a drop once.
                # Attempt 0 only (page-load fix, same day): a retry is
                # committed air, never expired mid-way.
                self._outgoing_dropped_total += 1
                RNS.log(
                    f"{self}: dropping DIRECT send to {peer_prefix!r}"
                    f"{f' (pkt_id={pkt_id} frag_idx={frag_idx}/{frag_total})' if pkt_id is not None else ''}"
                    f" -- packet expired (outgoing_max_age={self.outgoing_max_age_s:.0f}s) before "
                    f"attempt {attempt} could transmit.",
                    RNS.LOG_WARNING,
                )
                return False
            frame = frame_builder(attempt)
            try:
                ok, waited_full_timeout = await self._send_direct_frame_and_wait_for_ack(
                    target, frame, attempt, peer_prefix=peer_prefix,
                    pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, priority=priority,
                    hop_count=hop_count, time_critical=(time_critical or attempt > 0),
                    pass_number=pass_number, expires_at=expires_at,
                )
            except Exception as exc:
                RNS.log(
                    f"{self}: DIRECT send to {peer_prefix!r} failed locally "
                    f"(attempt {attempt}): {exc}",
                    RNS.LOG_WARNING,
                )
                ok, waited_full_timeout = False, False
            if ok:
                if record_result:
                    self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
                return True
            # No per-attempt delay here anymore -- the post-send listen
            # window (outcome-dependent range, 2026-09-16) fires inside
            # _send_direct_frame_and_wait_for_ack itself, before it releases
            # _direct_exchange_lock, so it already happened before control
            # returned here regardless of this attempt's outcome.
            if self.detached or not self.online:
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
        if peer_prefix is None or rtt_s <= 0:
            return
        st = self._ack_rtt.get(peer_prefix)
        if st is None:
            self._ack_rtt[peer_prefix] = {"srtt": rtt_s, "rttvar": rtt_s / 2.0, "samples": 1, "last_rtt": rtt_s}
            return
        err = rtt_s - st["srtt"]
        st["rttvar"] = 0.75 * st["rttvar"] + 0.25 * abs(err)
        st["srtt"] = st["srtt"] + 0.125 * err
        st["samples"] += 1
        st["last_rtt"] = rtt_s

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
            self._ack_rtt_snapshot.pop(peer_prefix, None)
            self._echo_stats.pop(peer_prefix, None)
            self._last_firmware_ack_timeout_s.pop(peer_prefix, None)
        if st is not None:
            self._debug(f"ACK RTT estimate for {peer_prefix!r} discarded ({reason}); firmware timeout applies until re-measured.")

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
        measured = self.direct_ack_rtt_timeout_multiplier * (st["srtt"] + 4.0 * st["rttvar"])
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
        if not samples or len(samples) < self.direct_hop1_abort_min_samples:
            return None
        deadline_s = max(self.direct_hop1_abort_min_s, self.direct_hop1_abort_echo_multiplier * max(samples))
        if deadline_s >= timeout_s:
            return None
        return deadline_s

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

    _RX_LOG_WINDOW_FOREIGN_CAP = 20
    _RX_LOG_PAYLOAD_TYPE_TEXT_MSG = 2
    _RX_LOG_PAYLOAD_TYPE_PATH = 8

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
        try:
            async with self._direct_exchange_lock(priority):
                lock_wait_s = time.monotonic() - wait_start
                queue_depth_at_acquire = self._direct_exchange_queue_depth
                if attempt == 0 and self._expired(expires_at):
                    # Field fix (2026-09-18 evening): the lock wait itself
                    # (225s in the drive-home capture) is where a queued
                    # packet most often ages out. Recorded, not transmitted;
                    # the caller's own pre-attempt check logs the drop.
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
                        timeout_s = min(timeout_s, self.direct_ack_timeout_routed_max_s)
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
                        first_wait_s = hop1_abort_deadline_s if hop1_abort_deadline_s is not None else timeout_s
                        ack_event = await self._mc_ready.wait_for_event(
                            self._EventType.ACK, attribute_filters=ack_filters, timeout=first_wait_s,
                        )
                        aborted = False
                        if ack_event is None and hop1_abort_deadline_s is not None:
                            if rx_window["echo_seen_s"] is None:
                                aborted = True
                            else:
                                ack_event = await self._mc_ready.wait_for_event(
                                    self._EventType.ACK, attribute_filters=ack_filters,
                                    timeout=max(0.01, timeout_s - first_wait_s),
                                )
                        ok, waited_full_timeout = ack_event is not None, True
                        ack_timeout_s = hop1_abort_deadline_s if aborted else timeout_s
                        if aborted:
                            ack_timeout_source = "hop1_abort"
                        if ok:
                            ack_latency_s = time.monotonic() - ack_wait_start
                            self._record_ack_rtt(peer_prefix, ack_latency_s)
                        elif ack_timeout_source == "measured":
                            # Karn: the measured estimate governed this wait and
                            # it missed -- maybe the link slowed, maybe the
                            # estimate was tight. Either way, back to the
                            # firmware's guess until fresh samples exist.
                            self._invalidate_ack_rtt(
                                peer_prefix, "missed ACK under measured timeout", keep_for_query=True,
                            )
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
                if listen_delay_s > 0:
                    await asyncio.sleep(listen_delay_s)

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
                    f"medium_hold_wait={gate_telemetry.get('medium_hold_wait_s')}"
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
        RNS-token tables §7 populates (peer_discovery_design.md). A PROOF
        packet needs the separate short-TTL correlation table -- its own
        destination-hash field IS the truncated hash of the packet it
        proves, never a stable per-peer identity (§7's "PROOF
        exception").

        Resolved gap (code review, 2026-09-18; previously flagged here as
        known-but-unfixed): for an outgoing LRPROOF (`context ==
        RNS.Packet.LRPROOF`, answering a peer's LINKREQUEST),
        `RNS.Packet.pack()` writes the *link_id* into this same on-wire
        field, not a destination hash, so it was never found in
        `_proof_correlation`'s truncated-hash keyspace and always fell
        through to broadcast+supplement even for a known, DIRECT-resolved
        peer. `_observe_incoming_rns_packet` now records `link_id ->
        peer` in `_rns_token_peer` for every LINKREQUEST received DIRECT
        (via `_compute_link_id`, validated in-process against
        `RNS.Link.link_id_from_lr_packet`), and this branch consults that
        table for LRPROOF specifically. Every other PROOF still goes
        through the short-TTL correlation table as before."""
        if header.destination_hash is None:
            return None
        if header.packet_type == RNS.Packet.PROOF:
            if header.context == RNS.Packet.LRPROOF:
                return self._rns_token_peer.get(header.destination_hash)
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
                await asyncio.sleep(self.contact_refresh_interval_s)
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
            return
        if not waited_full_timeout:
            # An attempt cut short by this engine's own ceiling being too
            # tight proves nothing about the path itself -- the old
            # design's own field-diagnosed gate, kept unchanged.
            return

        failures = self._direct_path_failures.get(pubkey_prefix, 0) + 1
        self._direct_path_failures[pubkey_prefix] = failures

        effective_threshold = self.direct_path_reset_threshold
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
        await asyncio.sleep(self.bind_response_jitter_max_s + 5.0)
        if self.detached or not self.online:
            return
        if pubkey_prefix not in self._peers or pubkey_prefix in self._resolved_paths:
            return
        self._path_discovery_failures.pop(pubkey_prefix, None)
        self._path_discovery_backoff_until.pop(pubkey_prefix, None)
        self._debug(f"discover_path({pubkey_prefix!r}): post-bind retry -- the first attempt likely raced the peer's telemetry grant.")
        await self._discover_path_coalesced(pubkey_prefix)

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
            while not self.detached:
                await asyncio.sleep(self.peer_discovery_rerequest_interval_s)
                if self.detached:
                    break
                if len(self._peers) >= self.peer_discovery_target_peers:
                    continue
                await self._send_bind_frame(self.BIND_TYPE_REQUEST)
        except asyncio.CancelledError:
            pass

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
                    held=held, version=frame.version,
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
        fut = self._completion_query_waiters.get((peer_prefix, frame.pkt_id))
        if fut is not None and not fut.done():
            fut.set_result(frame)

    async def _send_completion_answer(
        self, sender_token: str, pkt_id: int, frag_total: int, complete: bool,
        held: "Optional[set]" = None, version: Optional[int] = None,
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
            self.COMPLETION_TYPE_ANSWER, pkt_id, frag_total, complete=complete,
            held=held, version=version,
        )
        try:
            ok, _waited_full_timeout = await self._send_direct_frame_and_wait_for_ack(
                target, frame, 0, peer_prefix=self._canonical_peer_prefix(sender_token),
                priority=self.PRIORITY_NORMAL, time_critical=True, kind="completion_answer",
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
                await asyncio.sleep(self.peer_ttl_sweep_interval_s)
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
        self._ack_rtt_snapshot.pop(pubkey_prefix, None)
        self._echo_stats.pop(pubkey_prefix, None)
        self._last_firmware_ack_timeout_s.pop(pubkey_prefix, None)
        for k in [k for k in self._resumable_sends if k[0] == pubkey_prefix]:
            del self._resumable_sends[k]
        self._raw_disabled_until.pop(pubkey_prefix, None)
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
            # Code review (2026-09-18): the one PROOF whose destination
            # field IS worth learning from -- an LRPROOF answering a
            # LINKREQUEST this node sent carries the link_id, a stable
            # identity for that Link's lifetime, and proves the destination
            # it was requested for is reachable through this peer. Learn
            # both tokens and clear that destination's unknown-destination
            # backoff (previously three good Links to the same destination
            # counted as three "failures" -- see the module docstring's
            # 2026-09-18 review entry).
            pending = self._pending_link_requests.pop(header.destination_hash, None)
            if pending is not None:
                requested_dest, _expiry = pending
                self._rns_token_peer[header.destination_hash] = sender_peer_prefix
                self._rns_token_peer[requested_dest] = sender_peer_prefix
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

        self._rns_token_peer[header.destination_hash] = sender_peer_prefix
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
                self._rns_token_peer[link_id] = sender_peer_prefix
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
        self._handle_direct_multifragment_frame(header, rns_payload, src_prefix, raw=True)

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

    # MeshCore TXT_MSG framing (firmware: Mesh::createDatagram +
    # Utils::encryptThenMAC + BaseChatMesh::composeMsgPacket, and the
    # packet header): [header:1][path_len:1][path:N][dest_hash:1]
    # [src_hash:1][MAC:2][AES-ECB(timestamp:4 + flags:1 + text + NUL:1)
    # padded to 16]. Confirmed against the other radio's RX log in the
    # 2026-09-18 page-load capture: every full 151-char fragment was
    # heard as exactly 166 bytes = 2 + 4 + ceil16(151 + 6).
    _TXT_MSG_FIXED_OVERHEAD_BYTES = 2 + 1 + 1 + 2
    _TXT_MSG_PLAINTEXT_OVERHEAD_BYTES = 4 + 1 + 1

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
        plaintext = len(frame.encode("utf-8")) + self._TXT_MSG_PLAINTEXT_OVERHEAD_BYTES
        ciphertext = -(-plaintext // 16) * 16
        return self._estimate_airtime_s(self._TXT_MSG_FIXED_OVERHEAD_BYTES + max(0, path_len) + ciphertext)

    _RX_LOG_ROUTE_FLOOD = {0, 1}   # TC_FLOOD, FLOOD (meshcore ROUTE_TYPENAMES order)
    _RX_LOG_ROUTE_DIRECT = {2, 3}  # DIRECT, TC_DIRECT
    _RX_LOG_ACK_BEARING_TYPES = {0, 2}  # REQ, TEXT_MSG -- the receiver answers with an ACK (or PATH when flooded)
    _RX_LOG_NOTHING_FOLLOWS_TYPES = {3, 4}  # ACK, ADVERT

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
            payload_typename = str(payload.get("payload_typename", "UNK"))
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

    # MeshCore payload-type codes (referenceprojects/MeshCore-main/src/
    # Packet.h, mirrored by the meshcore library's PAYLOAD_TYPENAMES):
    # the ones whose cleartext payload starts with [dest_hash][src_hash].
    _RX_LOG_ADDRESSED_PAYLOAD_TYPES = frozenset({0, 1, 2, 8})  # REQ, RESPONSE, TEXT_MSG, PATH
    _RX_LOG_PAYLOAD_TYPE_ACK = 3

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
            "payload_typename": payload.get("payload_typename"),
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
            return

        complete_data = self._add_channel_fragment(key, header, payload)
        if complete_data is None:
            self._last_incoming_direct_at = time.monotonic()
        else:
            peer_prefix = self._canonical_peer_prefix(sender_token)
            if not raw:
                self._observe_incoming_rns_packet(complete_data, peer_prefix)
            # raw=True (2026-09-18 night): the src prefix in a raw frame is
            # unauthenticated, so nothing is learned from it -- the packet
            # is only delivered.
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

    def _add_channel_fragment(self, key, header: _FrameHeader, payload: bytes) -> Optional[bytes]:
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
                key[0], key[1], header.pkt_id, header.frag_idx, header.frag_total, len(bucket.fragments),
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
                self._outgoing_inflight_sweep(now)
                self._resumable_sends_sweep(now)
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
        self.rxb += len(data)
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
