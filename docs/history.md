# SmartMeshCoreInterface -- design history

The dated design record of `Interface/SmartMeshCoreInterface.py`, moved out of the module
docstring on 2026-09-20 (phase 2 of the airtime / throughput pass) exactly as it stood, oldest
first: the alpha 0.1.0 STATUS snapshot, milestones M0-M6, and every field-driven or audit-driven
change after it, each dated and citing the capture, simulator run or user request that motivated
it. CLAUDE.md names this the authoritative record of *why* the code looks the way it does; the
docstring that remains in the interface carries the design invariants, the wire format and the
pointer here. New entries go at the END of this file in the same style (dated, the evidence, the
decision, the tests), and `changelog.md` gets the summary.

The design documents these entries cite by section number (`docs/interface_architecture.md`,
`docs/reliability_engine_design.md`, `docs/path_discovery_spec.md`, `docs/peer_discovery_design.md`,
`docs/wire_format_design.md`, `docs/routing_decisions.md`, `docs/cooperative_broadcast_design.md`,
`docs/meshcore_protocol_rules.md`) never existed in this repository (see CLAUDE.md, "Missing design
docs"); where an entry justifies a decision by citing one, the entry's own prose is the source of
truth.

Verbatim from the docstring follows; nothing below this line was edited in the move.

---

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
tiers implemented and unit-tested, fed `hop_count=None` at the time since
no live topology data source existed until Milestone 4/5 -- the DIRECT-
fragmented sender has passed the resolved path's `out_path_len` since M5,
while the CHANNEL multi-fragment path still passes `None`), incoming
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
    `_send_direct_frame_and_wait_for_ack` helper that waits for the real
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
correctly via `_send_direct_frame_and_wait_for_ack`, and §7's opportunistic
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
it" on the airtime review; `direct_raw_fragments_enabled`, off until the
first field test the same night confirmed it and the user switched the
default to ON).** The largest remaining airtime cost was
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
  bursts every missing fragment under `_direct_exchange_lock` (each
  followed by `direct_raw_zero_hop_gap`, or `direct_raw_hop_gap_factor` x
  hops x its airtime when repeaters must each forward it first -- see
  `_raw_fragment_gap_s` and the 2026-09-19 morning entry), releases the lock, then
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
  gets frames it cannot hear. On by default since the first field test;
  a peer that has not advertised the bit still gets text fragments.
- *Self-disabling fallback.* If a reconcile ANSWER arrives (the text
  path works) but shows the burst delivered nothing, twice, raw is
  disabled for that peer for `direct_raw_fallback_cooldown` (600s then;
  120s since 2026-09-19 night) and
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
seen-dedup): see `tests/test_raw_fragments.py`.

**First field test (2026-09-18, 21:57-23:01, `fieldtests/raw/
binaryfieldtest/`, both sides captured): the repeater assumption holds
and raw is the bulk transport from here.** Zero hop: 35/35 raw packets
delivered, 5 extra fragments in total, every reconcile answered. Hop 1
through the public repeater: 25 raw fragments sent, PATH_RESPONSE,
RESOURCE_ADV and 483-byte RESOURCE parts reassembled by the far side as
`direct_raw_multifragment`; two 483-byte parts arrived in exactly two
rounds each (burst delivered 3/4 and 2/4, the reconcile named the gaps,
one re-burst finished them). No fallback fired. Two things the capture
exposed, fixed the same night:

1. *The reconcile query had no RTT information when it mattered.* Raw
   bursts produce no ACKs, so the step-2 estimator never learns a raw-
   only peer's timing, and a path change clears it anyway; the first
   hop-1 raw packet after rediscovery ran its queries at the 5s floor,
   timed out twice and re-burst a fragment the peer already held, while
   later queries (fed by unrelated text sends) swung to 26-32s.
   `_query_rtt` now measures the QUERY -> ANSWER round trip itself per
   peer (`_record_query_rtt`, Jacobson/Karels like the ACK estimator, and
   the QUERY's own firmware ACK still feeds `_ack_rtt`), and before any
   sample exists the prior is hop-scaled: `direct_completion_check_
   timeout` x (hops + 1). Cleared with the ACK stats on a path change.
2. *Holding the radio lock through the ANSWER wait blocked the other
   node's answers.* At 22:08:56 the desktop's four completion ANSWERs to
   the laptop's queries waited 31-50s for `_direct_exchange_lock` because
   the desktop's own queries held it while idle; the laptop's queries
   timed out, it re-burst, and one send failed. Under bidirectional
   traffic the hold (a 2026-09-18 review decision, made on the half-
   duplex argument) is a head-of-line blocker with a measured cost, so
   the QUERY now goes through `_send_direct_frame_and_wait_for_ack`
   (`kind="completion_query"`: lock held through its own transmit and
   firmware ACK, exactly like every other frame) and the ANSWER is
   awaited with the radio free. A late ANSWER still resolves the next
   query for the same pkt_id, as before.

Also: overheard raw packets are named `RAW_CUSTOM` in the RX log and
[STATS] (the library's name table stops at CONTROL and reported them as
`UNK`).

**Raw-first with a per-PATH Z85 fallback (2026-09-18 night, user's
design).** "Default to binary, keep Z85 as the fallback if a repeater in
the chain doesn't support it; when a new path is detected try binary
again; if Z85 works but binary doesn't, note that a repeater in that
path's chain doesn't carry binary." The earlier fallback note was per
peer with a 10-minute cooldown, so it forgot and re-probed raw on the
same chain, and it never checked whether Z85 actually worked. Now:

- The strike rule is unchanged (`direct_raw_fallback_strikes`, default 2:
  answered reconciles showing a burst delivered nothing), but it only
  *pauses* raw for the peer (`direct_raw_fallback_cooldown`) and records
  `_raw_fallback_pending[peer] = path`.
- The packet is then sent as Z85 text on the same path. If THAT
  succeeds, the path -- the repeater chain, `out_path_hex`, not the peer
  -- goes into `_raw_unsupported_paths` for `direct_raw_path_
  unsupported_ttl` (a day) and the peer's pause is lifted: raw is off
  for that chain only, and the verdict is logged as "a repeater in the
  chain does not carry raw packets". If the text send fails too, nothing
  is noted about raw: the path itself is sick, and the short pause is
  all that applies.
- A path change (`_reset_stale_path`, a fresh discovery) clears the
  peer's pause, and `_raw_fragments_eligible` consults the new path's
  own entry -- so a new chain is always tried raw-first, and a chain
  already known to drop raw is never probed again while its note lives.
  Zero-hop paths (no repeater) are never noted.

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

**Code audit (2026-09-19, user-requested: "audit for bugs, unperformant
code or any other potential issues", then "verify the bugs are real and not
intentional, fix them and test").** Six parallel review passes plus a raw-
path read-through; every finding was re-checked against this file's own
stated intent before anything was touched, and the check mattered -- the
single highest-severity candidate turned out to be deliberate.

NOT bugs, deliberately left alone:
  * A hop-1 abort counting as a real failure toward
    `direct_path_reset_threshold`. `direct_hop1_abort_enabled`'s own comment
    says so explicitly ("silence where a forward was due is evidence,
    unlike a plain timeout"). What DID change is the definition of silence
    -- see the refinement below.
  * Skipping Karn invalidation when an abort fires under a measured
    timeout: the abort deadline is shorter than the timeout by
    construction, so the estimate was never actually disproven.
  * `acked` being overwritten wholesale from a reconcile ANSWER (documented
    as the receiver's bucket deciding "in both directions").
  * Raw fragments travelling unencrypted: that is what MeshCore's
    PAYLOAD_TYPE_RAW_CUSTOM is (firmware `Mesh::createRawData` does no
    `encryptThenMAC`, unlike `createDatagram`). Left as the design decision
    it is, but now documented in readme.md, because with the feature on by
    default it changes what a listener can see: RNS message *contents* stay
    encrypted by RNS, but the RNS header (destination hash, type, context)
    and this interface's own src/dst pubkey prefixes are in the clear where
    text DIRECT hid them. No authentication either, so a third party can
    read a pkt_id off the air and inject a corrupting fragment; the
    reassembly-collision guard then evicts the bucket. Worth a conscious
    choice rather than a silent one.
  * `detach` dropping queued packets (documented as intentional).

Fixed, each because the code contradicted its own documented intent or
because a real capture showed it happening:
 1. **Link-carried PROOFs were misrouted** (`_resolve_routing_peer`). Only
    LRPROOF consulted `_rns_token_peer`; every other proof on an
    established Link -- RESOURCE_PRF above all -- fell through to
    "unknown destination". In `fieldtests/raw/binaryfieldtest` the same
    link_id was routed `direct_primary` for 54 DATA packets and
    `small_mesh_direct_all_unknown_dest` for its RESOURCE_PRF. A
    RESOURCE_PRF is the sender's only transfer-complete signal, and each
    misroute also charged `_record_unknown_dest_attempt` against the live
    Link's id, so three of them armed a 300s backoff that drops the proof
    outright in small-mesh mode. The token table is now consulted for every
    PROOF context; the keyspaces cannot collide, so bare proofs are
    unaffected.
 2. **The completion-ANSWER wait was charged for the QUERY's own send.**
    `_completion_query_timeout_s` documents itself as "how long to wait for
    a completion ANSWER", but the budget was measured from before
    `_send_direct_frame_and_wait_for_ack` -- which includes
    `_direct_exchange_lock` queueing (49s observed), the pre-transmit gate
    and the QUERY's own firmware ACK -- so `max(0.5, ...)` routinely left
    0.5s for the reply. 36 of 106 completion checks across all archived
    captures timed out (34%), and each timeout means "no information", i.e.
    a full re-drive of fragments the receiver already held. The budget now
    starts when the QUERY is actually out, and `_record_query_rtt` measures
    from the same point so this node's own queueing no longer inflates the
    estimator.
 3. **A stale ANSWER could be applied authoritatively.**
    `_completion_query_waiters` is keyed only `(peer_prefix, pkt_id)`, so a
    late reply to a timed-out earlier query resolved the current query's
    future. An ANSWER whose `frag_total` differs from the outstanding
    query's is now discarded. A same-frag_total stale answer would need a
    query nonce in the frame; deliberately not added -- fix 2 removes most
    of the window, and a wire change for the remainder is not justified yet.
 4. **A v1 ANSWER read as "holds nothing".** `set(answer.held or ())`
    conflates "this protocol version carries no bitmap" with "the receiver
    has none of it". On the raw path that made `nothing_ever_held` true, so
    a v1 peer got its whole repeater chain blacklisted for
    `direct_raw_path_unsupported_ttl` (24h) on the strength of its version.
    Both paths now treat `held is None` as no information.
 5. **Announces and path requests could vanish in small-mesh mode.**
    `_send_direct_supplement` returns silently when no path resolves --
    correct for a supplement riding alongside a mandatory broadcast, but
    `_send_direct_to_all_peers` IS the transport in small-mesh mode, and
    one failed discovery round arms a cooldown of up to 900s during which
    every ANNOUNCE, path request and unknown-destination packet was dropped
    with no log line, no `_outgoing_dropped_total` and nothing in the
    capture. The supplement now reports whether it reached the radio, the
    drop is logged and counted, and `_send_direct_to_all_peers` falls back
    to one CHANNEL broadcast if no peer got a transmission. Not a
    weakening of DIRECT-primary: it fires only when DIRECT could not be
    attempted at all.
 6. **The auto-reconnect re-arm was dead code.** Confirmed against the
    installed library (`connection_manager.py:99-121`): with
    `auto_reconnect` on -- the default -- an unexpected drop emits no
    DISCONNECTED at all; it reconnects silently and emits
    CONNECTED{reconnected:True}. So `_on_mc_disconnected` never ran,
    `self.online` never went False, and the `was_offline` guard skipped the
    re-arm exactly when it mattered, leaving the interface "online but
    deaf" -- the failure mode its own comment describes. The event's
    `reconnected` flag now triggers it too.
 7. **`_cfg_bool` treated `off` and `disabled` as True.** Verified through
    RNS's own vendored ConfigObj: values reach an interface as raw strings
    with no boolean coercion, so `rx_log_holds_enabled = off` turned the
    experimental holds ON, as did `packet_capture_enabled = off`. The falsy
    set now covers off/n/none/disabled, truthy spellings are explicit, and
    anything unrecognized is still True (historical behaviour) but logged.
 8. **`_validate_direct_timing_budget` checked the wrong budget.** It
    compared only against `direct_send_attempts` (2), so 120/48 = 2.5 fit
    and it stayed silent -- while `direct_send_attempts_handshake` (4) and
    `direct_fragment_finish_attempts` (4) are the budgets that actually
    apply to a fragment racing the receiver's clock. It now uses the
    largest. NOTE: it therefore fires on the shipped defaults (4 x 48s =
    192s against a 120s `reassembly_idle_timeout`). That incoherence is
    real and pre-existing; resolving it is a tuning decision (raise
    `reassembly_idle_timeout`, or lower `direct_ack_timeout_routed_max`)
    deliberately left to the operator rather than changed here.
 9. **`FIRMWARE_RAW_RX_PAYLOAD_LIMIT` was one too high** (173 -> 172). The
    guard it was derived from is `onRawDataRecv`'s buffer check, but the
    write that follows goes through `ArduinoSerialInterface::writeFrame`,
    which refuses frames over MAX_FRAME_SIZE (176) and returns 0 --
    silently, inside the receiving radio. A 173-byte raw payload builds a
    177-byte serial frame and never reaches the host. The default cap (170)
    was safe; raising `direct_raw_payload_cap` to the advertised limit made
    every zero-hop and 1-hop fragment vanish and then blamed the repeater
    chain for it.
10. **`direct_raw_reconcile_rounds` is clamped to 4.** The round travels in
    2 header bits, and the firmware dedups RAW_CUSTOM by a hash of payload
    type + payload bytes (`SimpleMeshTables::wasSeen`, a 160-entry ring
    with no time expiry), so a 5th round would be byte-identical to the
    first and silently dropped at both repeater and receiver.
11. **Raw fallback verdicts are keyed `(peer, path)`**, not peer alone --
    two concurrent sends to one peer could otherwise cross wires and
    blacklist a chain for 24h on the other send's evidence. **Raw also
    re-checks expiry every round**; it was checked once before the first
    burst, so a raw send could never expire mid-flight the way text can.
12. **Broadcast retry-pass tasks are tracked** in `spawned`, so the
    duplicate-in-flight key is not released while jittered retries for the
    same bytes are still pending (RNS re-queues identical Resource parts
    ~27s apart, which is how the duplicate storm this guard exists to stop
    came back). `expires_at` is also threaded into the three CHANNEL
    fallbacks that omitted it.
13. **The three receive callbacks that carry RNS payloads are guarded.**
    Only `_on_rx_log_data` was; an exception in the others was caught by
    the library's dispatcher and logged through `logging` alone -- never
    `RNS.log`, never counted, with the packet silently lost.
14. **`_discover_path_coalesced` no longer strands followers.**
    `except Exception` does not catch `CancelledError`, so a cancelled
    leader left the future unresolved and unreachable and every follower
    awaited it forever, holding their in-flight keys until the 600s sweep.
15. **`_rns_token_peer` is bounded** (`RNS_TOKEN_PEER_MAX_KEYS`, LRU via a
    new single entry point `_learn_rns_token`). Its documented "no expiry"
    holds for stable destination hashes, but it also stores one entry per
    ephemeral Link id and the only reclaim was 24h peer silence, which an
    active peer never reaches: 52-92 new destination hashes per hour in the
    2026-09-18 captures, growing monotonically for the life of the process.
16. **Periodic loop intervals have a floor** (`_loop_interval_s`). Every
    such loop is `while ...: await asyncio.sleep(self.<interval>)`, so a 0
    -- which this config surface teaches elsewhere as "disable" or "leave
    alone" -- spun the event loop at 100% CPU, and for
    `contact_refresh_interval` also flooded the serial link.
17. **Hop-1 abort refinement (field-driven).** The abort means "silence
    where a forward was due". Traffic from the TARGET itself during the
    wait is not silence -- it means the target was transmitting rather than
    listening, so the path is demonstrably alive and the ACK is merely
    late. One of the four aborts in `fieldtests/raw/binaryfieldtest` was
    exactly that (`miss_diagnosis="target_busy"`), and aborting there both
    cut short a wait that would likely have succeeded and charged a failure
    against a good path. Such an attempt now waits out the remaining ACK
    timeout, as it already did when our own echo was heard.
18. **Post-bind path discovery retries a bounded number of rounds**
    (`POST_BIND_DISCOVERY_ROUNDS`, refreshing contacts each round). There
    are two races after a bind, not one: the telemetry-grant race the
    method already covered, and a peer binding before its ADVERT has
    arrived at all -- bind frames ride CHANNEL and take one hop, an advert
    has to flood the whole chain. In the second case `discover_path` bailed
    with "peer is not a known contact" and nothing retried it until real
    traffic needed the path. This is what kept the suite's only 2-repeater
    scenario from ever running (contacts and bind succeeded, `path_req_sent`
    stayed 0 on every radio); both nodes now resolve real 2-hop paths.

Test-suite fixes from the same audit: the flood-dedup test was flaky (2 of
3 runs) because the simulated packet id is a content hash including
`int(time.time())`, so two "identical" sends straddling a second boundary
hashed differently -- `cmd_send_chan_msg` now takes an injectable
timestamp. `advert_all` sends one un-retried advert per radio, which is a
coin flip across two repeaters, so `SimMesh.advert_until_contacts` retries
it (what an operator does when a node hasn't appeared). New regression
tests live in `tests/test_audit_fixes.py`, one class per fix above.

Known and deliberately NOT fixed here, so the next pass can pick them up:
packet-capture files are never rotated (~320 KB/hr, accumulating across
restarts); `_capture_event`'s `threading.Lock` is shared between the RNS
thread and the event loop, so a blocked write can stall the loop;
`owner.inbound` runs inline on the event loop, so RNS inbound processing
delays ACK correlation (moving it to an executor is a threading-model
change worth its own field test); `detach` can abandon an in-flight
`disconnect()` if teardown times out, leaving the serial port open; and
`_outgoing_dropped_total`/`rxb` are incremented from two threads without a
lock (stats only).

**Refactor pass (2026-09-19, user-requested after asking whether the file
needed one; behaviour-preserving, verified by the full suite and the
simulated-mesh scenarios before and after).** Measured first: 10.6k lines
with a 1.9k-line docstring, 205 methods, 74 instance attributes, 104
config keys, a 9-level DIRECT send chain threading 10-14 parameters, and
two 200-line fragmented senders (text and raw) carrying byte-identical
copies of the reconcile/resume logic -- where that day's review had found
three logic gaps. Done in this pass, all internal to the one file so the
drop-in install is unchanged:

- `_resume_state`, `_remember_resumable` and `_held_from_answer` are the
  single copies of the resume bookkeeping and the authoritative-answer
  reading both senders use (the v1 "no bitmap means no information" rule
  now lives in exactly one place).
- `_rtt_sample` is the one Jacobson/Karels update behind both the ACK and
  the QUERY round-trip estimators.
- `_clear_peer_path_stats` is the one list of per-peer, per-path state a
  path change or a peer expiry must drop; `_invalidate_ack_rtt` and
  `_forget_peer_state` call it instead of each keeping its own copy (the
  pattern that let earlier additions miss one of the two).
- `_send_direct_frame_and_wait_for_ack` (247 lines) lost its two
  self-contained halves: `_await_direct_ack` (timeout derivation, hop-1
  abort, RTT bookkeeping) and `_post_attempt_listen_s` (which listen
  range applies after an attempt).

Deliberately NOT done here, each a decision rather than a mechanical
move: a `SendContext` object to replace the 10-14 parameter signatures
(it would change every test fake's signature at once); merging the text
and raw senders into one engine with two fragment-drive strategies (the
control flows differ on purpose: per-fragment ACKed attempts versus burst-
then-ask); and moving this docstring's dated history into `changelog.md`,
which now duplicates most of it -- CLAUDE.md names this docstring the
authoritative design record, so where the history lives is the user's
call.

**Field-diagnosed batch (2026-09-19 morning, the first raw-fragment field
test through 2 and 4 repeater hops; both sides captured, user asked "what's
going on?" from the field and then for the fixes).** The laptop drove out
to 2 hops (76,19 / d6,19); the desktop still held the zero-hop path to it
from the night before. Three findings, two fixes:

1. *The desktop sat on the dead zero-hop path for 3.5 minutes.* Every one
   of the laptop's 12 path requests ARRIVED (desktop rx_log), but the
   desktop's firmware ACKs went back down the stale zero-hop path, so the
   laptop saw every send as `downstream_loss`; the laptop's own stale
   detector reset its side after 3 full-timeout failures (09:59:52). The
   desktop meanwhile answered each request with the 235-byte
   PATH_RESPONSE as raw fragments over path_len 0: three whole sends
   (pkt 14/15/16, 09:58:23-10:01:05, 18 raw frames and 17 full-timeout
   QUERY misses into nothing) before its own reset fired, because the raw
   sender recorded one path failure per exhausted send and the QUERY
   exchanges recorded none. Fix: `_record_query_path_evidence` -- each
   raw round's QUERYs (ACKed DIRECT exchanges over the cached path) feed
   `record_direct_send_result`: an ACK or an ANSWER clears the counter, a
   round in which every QUERY attempt missed after its full timeout counts
   one failure (per round, never per attempt, so one lost ACK still is not
   a path failure). `_query_remote_fragments` reports the QUERY's own ACK
   outcome through a `send_info` out-param. Replaying the capture, the
   desktop would have reset at ~09:59:34 instead of 10:01:05, before the
   second and third wasted sends. `_raw_path_reset_mid_send` then
   abandons a send's remaining rounds once its path is gone (remembered
   for resume), instead of bursting them down a path known dead. What
   actually delivered the announce was the small-mesh CHANNEL last resort
   (3 fragments via repeater 19, 10:01:21-10:01:43), added by the audit
   the night before.

2. *Raw fragments through repeaters lost exactly one of every two.* Once
   paths existed (laptop->desktop 2 hops; desktop->laptop 4 hops -- 19,
   d6, 4f, 76, asymmetric because MeshCore learns whichever flood copy
   arrives first), every 2-fragment raw send in both directions delivered
   one fragment (laptop pkt 0/1/2, desktop pkt 19/20). The inter-fragment
   gap was `direct_raw_hop_gap_factor` (2) x airtime regardless of hop
   count: 1.87s at 2 hops, 2.0s at 4. A fragment needs ~hops x airtime
   just to clear a half-duplex repeater chain, plus each repeater's random
   forward delay (simple_repeater `getDirectRetransmitDelay`: rand(0..5)
   x `direct_tx_delay_factor` 0.3 x airtime = 0-1.5 airtimes per hop), so
   the second fragment always reached some repeater while it was still
   transmitting the first. Confirmations: solo re-sends of the missing
   fragment at 2 hops arrived (laptop pkt 2 completed on round 1,
   10:03:50 -- the one LXMF message that got through; PROOF back at
   10:04:19); at 4 hops the desktop's re-sends were chased by the QUERY
   ~1s later and that QUERY then died at hop 1 with no echo
   (`hop1_abort`, 10:04:32); the laptop's pkt 0 second fragment and pkt 1
   first fragment went out 0.02s apart because the second send took the
   lock between the first send's burst and its QUERY; and after both
   desktop packets fell back to text, the ACK-paced text fragments went
   5/5 through 4 hops (10:06, 7-9s ACKs). Fix: `_raw_fragment_gap_s` --
   the gap is per fragment, factor x hops x that fragment's airtime,
   follows the LAST fragment too, and is slept with `_direct_exchange_
   lock` still held, so neither the QUERY nor another send can enter the
   chain until the fragment has cleared it. At one hop this is exactly
   the pre-fix gap the 2026-09-18 raw field test passed with; zero hop is
   unchanged (`direct_raw_zero_hop_gap`). Cost: raw at N hops now paces
   at ~2N airtimes per fragment, about what the text path's ACK round
   trip costs, so raw's win through repeaters shrinks to the 12% larger
   payload and the absent per-fragment ACKs; at zero hop nothing changes.

3. *Not fixed, noted.* The two directions' paths differ (2 vs 4 hops)
   and the interface uses whatever the firmware discovered; RNS re-sent
   the same LXMF message three times as three different ciphertexts
   (pkt 0/1/2), which `_outgoing_inflight`'s payload-hash dedup cannot
   see; pkt 0 and 1 expired at `outgoing_max_age` after their 45s QUERY
   timeouts. Verified: fast suite, `RawFragmentScenarios` (zero-hop, one-
   hop, and the new stale-path-within-one-send scenario) -- the user asked
   for the test suite only, not the standalone simulator runs.

**Default change (2026-09-19), found by running the simulator at production
timing:** `reassembly_idle_timeout` 120 -> 200 s. `_validate_direct_timing_
budget` fired at every startup with stock config once `direct_fragment_
finish_attempts` (4) became the worst-case per-fragment attempt budget:
one attempt racing the receiver's idle clock can cost `direct_ack_timeout_
routed_max` (45) + `direct_post_send_listen_max` (3) = 48 s, and only 2.5
of those fit in 120 s. The interface's own advice in that warning was
">= 192"; 200 leaves a little room. Nothing else moves: the 3-hop firmware
timeout the 2026-09-18 drive-home capture measured (28 s) is still well
under `routed_max`, and a bucket that lives 200 s instead of 120 costs
memory bounded by `reassembly_max_keys`, not airtime. Verified: fast suite;
the startup warning no longer appears at defaults.

**Field-diagnosed batch (2026-09-19), the four items the 2026-09-18
drive-home (3-hop -> zero-hop) capture left open after that day's fixes:**

1. *Announce pacing (`announce_min_interval`, 300s).* The same
   destination's ANNOUNCE was forwarded four times in five minutes, each a
   3-fragment DIRECT exchange over three repeaters; the session's
   `target_busy` misses were largely this. One spontaneous ANNOUNCE per
   destination hash per window now; path-response announces (context
   PATH_RESPONSE) keep their own 20s limiter and are exempt. Recorded as
   `announce_rate_limited`.
2. *RTO backoff instead of Karn discard (`direct_ack_rtt_miss_backoff`,
   2.0).* A miss under a measured timeout used to throw the estimate away,
   so the next attempt paid the firmware's 28s. With the hop-1 abort now
   covering a dead first hop, a slow-but-alive path instead doubles the
   measured wait per consecutive miss (still capped at the firmware
   value; the next real ACK resets it). `_backoff_ack_rtt`; <= 1 restores
   the discard.
3. *Closed-Link drop.* A 3-fragment DATA for a Link closed ten minutes
   earlier spent ~10 minutes of attempts. A LINKCLOSE seen in either
   direction (`_note_link_closed`, from process_outgoing and
   process_incoming) marks its link_id for CLOSED_LINK_TTL_S; a queued
   Link-addressed packet for it is dropped at dequeue (`link_closed`),
   the same before-first-transmission point as outgoing_max_age. Never
   the LINKCLOSE itself, never mid-packet.
4. *Bind re-request schedule (`peer_discovery_rerequest_initial`, 60s).*
   Simulation finding: a fresh pairing through three lossy hops lost its
   single startup REQUEST and had no second chance for 30 minutes. While
   below the target peer count the repeat now doubles from 60s to the
   existing 1800s cap (`_next_rerequest_interval_s`).

Verified: fast suite (`tests/test_field_fixes_0919.py` covers each), the
simulated-mesh scenarios; the field test that follows is the real check.

**Field-diagnosed fix (2026-09-19, zero-hop image transfer, both captures
in `fieldtests/raw/Alpha0.1.2`):** a packet received as raw fragments had
its PROOF routed `small_mesh_direct_all_unknown_dest` (seen twice, right
after each `direct_raw_multifragment` receive) because the raw path skips
`_observe_incoming_rns_packet` and so never filled `_proof_correlation`.
`_correlate_raw_proof` (widened and renamed `_observe_raw_received_packet`
later that day, see below) now records the correlation -- nothing else -- when
the raw frame's claimed source is an already-bound peer with a resolved
path; its docstring has the threat-model reasoning (a misdirected PROOF is
worth nothing to a spoofer, and the fallback already reached every bound
peer). Verified: fast suite (`tests/test_field_fixes_0919.py`).

**Field-diagnosed fix (2026-09-19, bidirectional zero-hop image transfer,
`fieldtests/raw/Alpha0.1.2/*image_send2*`/`*image_recv2*`):** 10 of 40
reconciles timed out, none of them to loss. The answering side knew the
answer within a second and could not transmit it for up to 30 s (queue
depth 9): its ANSWER sat at PRIORITY_NORMAL behind its own raw bursts.
Verified in both captures for the desktop's pkt 1: fragment sent 12:22:38,
laptop held it at 12:22:39, four queries timed out, two re-bursts of data
the peer already had, answer finally through at 12:23:15. Across both
nodes 5 fragments were genuinely lost (~6% under two-way load) and 3 data
sends plus ~10 query/answer exchanges were waste. One claim in the field
analysis did not hold: the laptop's timeouts were mostly at RTT-derived
budgets (14-18 s), not the 5 s floor -- so budget alone cannot fix this.

1. `PRIORITY_ANSWER`, a tier between HANDSHAKE and NORMAL, for the
   reconcile ANSWER and (unless the send is a handshake) the QUERY. Tiers
   renumbered HANDSHAKE 0 / ANSWER 1 / NORMAL 2 / LOW 3; the outgoing
   PriorityQueue and `_PriorityAsyncLock` order by integer, nothing
   compared the values numerically.
2. Raw sends no longer re-burst after an unanswered reconcile: the round
   re-queries, and bursts resume after an answered reconcile shows gaps
   -- or, as a safety valve for answers that are systematically lost
   rather than late, after `direct_raw_reburst_after_unanswered` (2)
   consecutive silent rounds. A fallback strike is only counted for a
   round that actually burst.
3. The ANSWER budget adds one flat prior per exchange queued on this node
   (at most four), capped at `direct_ack_timeout_routed_max_s` -- own
   queue depth as the proxy for the peer's under two-way load.
4. Found by the simulator while verifying 2: through a repeater the
   ANSWER left the radio right behind the firmware's ACK for the QUERY
   and reached the repeater while it was still forwarding that ACK --
   six of six answers lost, which the old blind re-burst had been hiding.
   `_send_completion_answer` now waits the same hop-scaled gap
   `_raw_fragment_gap_s` gives raw fragments (sized for the ACK frame)
   before transmitting; zero hop is unchanged.

Verified: fast suite, the raw-fragment scenarios, and a delayed-answer
scenario (`tests/test_field_fixes_0919.py::DelayedAnswerScenario`) that
injects answer latency rather than loss, as the field analysis proposed.

**Field-diagnosed fix (2026-09-19 afternoon, `fieldtests/raw/Alpha0.1.2/
*_fieldtest.jsonl`, 12:50-15:36, zero-hop then 1-2 hops):** the laptop
dropped 17 DATA packets to d4c70c4b between 12:55:43 and 13:00:19 as
`unknown_dest_backoff_drop`, while the desktop received every copy it did
send (20 in all) and proved them. Two gaps combined: (1) the desktop's
path-response ANNOUNCEs for that destination arrived raw (a 3-fragment
announce always does now) and the raw path learned nothing from them --
`_observe_raw_received_packet` now runs the full observe step for a
bound, path-resolved peer, the same trust a text-frame announce from that
peer already gets; (2) a destination whose replies are PROOFs rather than
announces could never satisfy "a token was learned", so three delivered
bootstrap sends still counted as three failures --
`_remember_bootstrap_send` keeps the packet's truncated hash and the
PROOF branch of `_observe_incoming_rns_packet` learns the route and
clears the backoff when that proof comes back, mirroring the LRPROOF
path. Same session, also confirmed in the field: reconcile ANSWERs at
one hop got 4 of 16 (laptop) and 10 of 23 (desktop) ACKs, with 20 of the
desktop's 25 reconciles timing out -- the ANSWER-behind-the-ACK loss the
simulator found earlier that day (fix 4 above; not in the field build).
Dead-hop abort fired 5 times at 5-6 s. Verified: fast suite
(`tests/test_field_fixes_0919.py`).

**Field fixes (2026-09-19 evening session, five parallel capture reviews;
captures in `fieldtests/raw/Alpha0.1.2/*eveningtest*`).** A 2.5-hour mobile
test: bench-adjacent until ~16:28, then the laptop drove away and both nodes
moved to 1-3 hops through the public repeaters. Outcome first, because it
frames everything below: **6 of 6 resource transfers completed with zero
permanently lost parts**, zero-hop throughput sat at the hardware ceiling
(42-46 B/s, ~75% of wall clock in duty-cycle wait, 0% fragment loss), and
every degradation tracked hop count (attempt success 93% / 65% / 51% / 42%
at 0 / 1 / 2 / 3 hops). Nothing here is a reliability regression; the fixes
are about the time and airtime spent getting there.

Three theories this session KILLED, recorded so they are not re-proposed:
  * *Contention between the two nodes.* A clock-skew-corrected merged
    timeline (+-5ms, validated against `since_own_tx_s`) puts real
    frame-to-frame overlap at 8.6% against **6.9% expected by chance** from a
    shuffle null -- no lockstep, barely any excess. Contention explains ~13%
    of one node's failures and ~1% of the other's. Session-wide the honest
    split of 349 failures is **loss 53%** (`hop1_loss` 105 + `downstream_
    loss` 81) versus **contention 37%** (`target_busy` 130). Third-party mesh
    traffic is <1% of channel time and is not a factor.
  * *`PRIORITY_ANSWER` causing collisions by keying sooner.* Refuted: answer
    `lock_wait_s` got LONGER across the builds (median 0.89s -> 3.07s), i.e.
    the tier is not visibly jumping the queue at all. Kept regardless: it
    costs nothing and the reasoning stands.
  * *The answering radio's firmware sitting on ANSWERs for 1-80s.* Refuted,
    and it was an analysis artefact of mine: a `completion_answer` capture
    record is written AFTER its own ACK wait, so its `ts` postdates the air
    transmit (median 1.55s), and pairing "first sighting after the ts" then
    latched onto an unrelated later frame. Properly paired, answerer-on-air
    to querier-hears is **median 0.12-0.86s, p90 1.3-9.7s**. The real causes
    of an unanswered reconcile are plain frame loss: **48% the QUERY never
    reached the peer's application, 38% the peer answered and the answer
    never arrived**, 11% arrived after the window.

What the session showed IS the binding constraint: **ACK-wait serialisation.**
34.6% of wall clock was spent inside DIRECT exchanges holding the single
interface-wide lock, 89% of that waiting for ACKs, and **1800s -- 20% of the
session -- in waits that were never going to be answered**, while the radio
itself was only ~13% busy and this node's own airtime was 4.7% of wall clock.
The fixes:

 1. **A hop-aware ACK-wait ceiling** (`direct_ack_timeout_base` 8.0 +
    `direct_ack_timeout_per_hop` 4.0, still under the absolute
    `direct_ack_timeout_routed_max`). The largest ACK that ever ARRIVED in
    1080 attempts was 8.15s (p99 5.82s; per-hop maxima 3.00 / 6.06 / 8.15s)
    while the firmware's own suggestion produced waits to 28s. Replaying the
    session, 8+4h would have cut failed-wait time 2510s -> 2121s while
    cutting off **zero** of the 559 ACKs that did arrive.
 2. **A hard cap on the completion-ANSWER budget**
    (`direct_completion_check_timeout_max` 15.0, `..._max_multihop` 18.0 from
    2 hops), replacing a stack of three escalations -- a `x (1 + hops)`
    multiplier, an RTT term, and a per-queued-exchange contention term --
    that produced budgets of 21-45s (median 41.4s on the timed-out checks).
    Of 98 answers, 70% arrived within 5s, 96% within 15s, 98% within 20s, and
    every band beyond 20s yielded TWO answers all session. Decisively, the
    answer rate FALLS as the budget grows (92% at 10-20s vs 34% at 40-45s):
    a long budget marks bad conditions rather than curing them, so the RTT
    escalation had the causality backwards. A 15s cap cuts time inside
    completion waits 3565s -> 1720s (-52%) for 4 of 98 answers. The RTT term
    is kept INSIDE the cap, since it is genuinely adaptive downward.
    CORRECTION, made while testing this change: the first cut removed the hop
    term from the FLOOR as well as capping the ceiling, which took a first
    query at 1 hop from 10s down to 5s -- and the same session measured
    query->answer at median 3.2/5.7s with a p90 of 11.7/16.1s, so before any
    RTT sample exists to widen it, a flat 5s floor abandons the p90 case. The
    floor is therefore hop-aware again (`direct_completion_check_timeout` +
    `direct_completion_check_timeout_per_hop` 2.5 x hops -> 5/7.5/10/12.5s at
    0-3 hops) but, unlike the old prior, is itself clamped by the ceiling. The
    one-hop simulated raw scenario caught this, which is the argument for
    running the slow scenarios and not just the unit tests.
 3. **A one-byte query nonce (completion frame v3)**, echoed by the answerer
    and checked on resolution. `_completion_query_waiters` is keyed only
    `(peer_prefix, pkt_id)`, and the earlier `frag_total` guard cannot catch a
    stale answer whose frag_total matches: five checks resolved `answered`
    although no matching query ever reached the peer, one of them applying
    `held=[]` authoritatively -- i.e. discarding every fragment the receiver
    really had. A v3 ANSWER whose nonce mismatches is discarded; a v1/v2
    ANSWER (no nonce) is still trusted exactly as before. NOT backward
    compatible in the other direction (second audit's correction): a pre-v3
    peer rejects a v3 QUERY as an unsupported version and never answers it,
    so both nodes must run this build for reconciliation to work at all. `attempt` also now varies per query/answer
    instead of being pinned at 0, so the firmware's own content-derived
    dedup/retry differentiation is exercised -- a repeated `(pkt_id,
    frag_total)` query was answered only 43% of the time versus 90% for a
    first-time query at equal link quality, a gap these captures do not
    explain and which deserves a simulator scenario.
 4. **The duplicate-in-flight suppression can no longer deadlock a transfer**
    (`outgoing_duplicate_suppress_limit`, 3). Measured, both captures
    agreeing: resource part `ca6b3d36db27` was transmitted at 16:48:06 and
    delivered to the peer's RNS at 16:48:25, the peer's RNS did not credit it
    and re-requested it, and this side then refused SEVEN consecutive
    re-sends (16:49:55-16:52:53) because the original send's in-flight entry
    never cleared -- its completion checks kept timing out, so
    `_release_inflight_when_done` never fired. 178s of a 442s transfer, with
    RNS asking correctly and this interface correctly-but-fatally declining.
    After the limit the packet is forced through with a fresh entry.
 5. **(Withdrawn by the second audit, same evening.)** A "prefer the
    radio's own shorter contact path over the discovered one" step was added
    here and removed again before it ever ran in the field. The firmware's
    contact path is learned from adverts by the same flood-first-arrival
    mechanism as a discovery answer, so it is not a better opinion, only an
    older one -- and a discovery answer has just proved its path alive,
    where an advert-learned path may be an hour stale. The one supporting
    observation (a path_len-1 echo of the node's own request at 17:07:48)
    says nothing about the contact record. Path resolution keeps its single
    source of truth, per CLAUDE.md's one-code-path rule.
 6. **A recently healthy path needs more failures before being discarded**
    (`direct_path_healthy_window` 120s, `..._recent_successes` 5,
    `..._patience_multiplier` 2.5). The 91% path above was thrown away after
    a 7-attempt bad patch. Failures still accumulate, so a genuinely dead
    path is still reset -- just later.
 7. **The CHANNEL fallback is no longer invisible.** `_send_direct_packet`'s
    three broadcast fallbacks returned before any capture call, so a packet's
    only record was the dispatcher's earlier `direct_primary` while it
    actually went out as an unencrypted CHANNEL flood -- seven packets that
    session, found only because the peer logged them arriving `channel_bare`.
    They now record `direct_unresolved_channel_fallback` /
    `direct_no_contact_channel_fallback` / `direct_too_large_channel_
    fallback`. This one matters beyond itself: every analysis that trusts
    `routing_decision` was exposed to it.
 8. **A PROOF no longer arms the unknown-destination backoff.** Six proofs
    per session still route `small_mesh_direct_all_unknown_dest`, because the
    CHANNEL receive path cannot authenticate a sender and so deliberately
    learns no token from it (the raw path's guard cannot be reused: CHANNEL
    carries only an attacker-choosable `adv_name`). A proof's destination
    field is a one-shot value, so counting "attempts with no token learned"
    against it is meaningless -- and in the midday capture three such proofs
    armed a 300s cooldown that then DROPPED later proofs outright in
    small-mesh mode. Excluding proofs removes the harm without inventing
    trust in a CHANNEL sender.

Regression tests: `tests/test_field_fixes_0919_evening.py`, one class per
fix, each citing the measurement it pins. Two pre-existing tests asserted the
superseded timeout policy (the hop-scaled prior, and the queue-depth
contention term) and were rewritten to assert the new one rather than
deleted, with the evidence in their docstrings.

Still open, deliberately: the 43%-vs-90% repeated-query gap (mechanism
unknown); the CHANNEL receive path learning nothing (correct but leaves six
unroutable proofs per session); and whether `rx_log_holds_enabled` should
default on -- it computed 1747s of holds it never applied this session, and
it targets `target_busy`, the one genuinely contention-driven failure mode.

2026-09-19, evening session -- SECOND AUDIT of the same captures, with both
nodes' rnsd terminal logs. Confirms the first audit's budget caps and nonce,
withdraws one of its changes (item 5 above), and adds the three fixes that
audit did not reach. Every number below is from `fieldtests/raw/Alpha0.1.2/`
(`*eveningtest.jsonl`, `laptop_a_evening_dump.txt`, `Eveningtest_afipc_
terminal_dump.txt`), re-derived independently.

What the second pass established:
  * The 88 reconcile timeouts split 47 QUERY-never-reached-peer, 25 ANSWER
    sent but never acknowledged, 15 ANSWER sent after the window closed,
    4 local send failures -- close to the first audit's 48/38/11%.
  * The "firmware queue" theory was wrong, but application-side queueing of
    ANSWERs is real: on the desktop, query receipt to ANSWER on air was
    median 3.2s, p90 13.7s, max 57.7s, almost entirely `lock_wait_s`. What
    held the lock was PROOF traffic: 188 plain delivery PROOFs at
    PRIORITY_HANDSHAKE with the 4-attempt handshake budget, 1716s of the
    lock in their ACK waits (1196s in misses). The 18:01:21 ANSWER waited
    54.6s behind five attempts of two 107-char PROOFs (terminal log). The
    three "stale answers" were all answers to the PREVIOUS round's query,
    delayed 38-55s this way and arriving during the re-query -- the nonce
    stops the misattribution, this fix removes the delay.
  * The hop-1 abort was disarmed for most of the mobile node's first-hop-
    silent misses: 59 of 72 ran the full firmware timeout (835s vs ~295s),
    because echo samples are cleared on every path (re)discovery, stale
    reset and restart (seven restarts that hour), and three fresh samples
    were required before the abort armed again.
  * Fan-out: the desktop had up to 14 completion windows open at once, the
    laptop 9. The querier's own radio was transmitting other packets'
    fragments/queries when 11 of the 25 lost ANSWERs arrived (the answerer
    logged the querier's TEXT_MSG during its ACK wait). And the "duplicate-
    in-flight deadlock" (item 4 above) had a different root: the laptop
    reassembled part `ca6b3d36db27` in full at 16:48:25 and delivered it to
    RNS, which discarded it -- `Resource.receive_part` credits a part only
    inside its window from the last consecutive part, and parts 42/43 were
    still incomplete -- then re-requested it seven times; the second copy
    at 16:53:03 was credited. Out-of-order, minutes-apart delivery is the
    cause; the suppression limit is a valve on the symptom and is kept.
  * The 43%-vs-90% "repeated query" gap is mostly selection: first checks
    answered 65%/58% (desktop/laptop), repeats 42%/40%, and a repeat only
    happens after a failure. Not worth a scenario.
  * The 17:05-17:08 path loss was a genuine fade (the desktop's frames fell
    from +10 to -3dB SNR over forty seconds before six aborted attempts),
    not a hasty reset; the healthy-path patience (item 6) is kept, the
    shorter-path preference (item 5) withdrawn.

The fixes:
 1. **A plain delivery PROOF is no longer handshake class** (`_priority_
    tier`, `_proof_is_link_class`). LRPROOF, RESOURCE_PRF and the
    KEEPALIVE..LRPROOF band keep PRIORITY_HANDSHAKE; a context-NONE PROOF
    rides PRIORITY_ANSWER -- ahead of bulk data, behind a Link handshake,
    the ordinary 2-attempt budget, no duty-cycle exemption. If it is lost,
    the sender's application retries the DATA; nothing else waits on it.
 2. **The hop-1 abort arms without per-peer samples** (`direct_hop1_abort_
    default` 8.0s, plus a session-wide pool of echo timings across peers).
    Per-peer samples still win when present; else the pool; else the
    default, sized at twice the largest echo seen all session (median 2.0s,
    max 4.0s at 1-3 hops). 0 restores samples-only arming.
 3. **At most `direct_fragmented_max_in_flight` (2 in this build; the
    default is 0 since the "Field regression fixed (2026-09-19 night
    session)" entry below) fragmented sends per peer at once**, the slot held across bursts AND reconcile windows, so
    packets to one peer complete roughly in RNS's order; handshake class
    bypasses it; a packet that cannot get a slot within outgoing_max_age is
    dropped even if its class is expiry-exempt (RNS has re-requested it by
    then). `direct_send_result` records `slot_wait_s`.
 4. The first audit's shared `_completion_timeout_peer_hint` attribute
    became an explicit `peer_prefix` argument, and its "backward compatible
    both ways" claim for completion v3 was corrected (a pre-v3 peer drops a
    v3 QUERY; both nodes must run this build).

Regression tests: `tests/test_second_audit_0919.py`.

**Field regression fixed (2026-09-19 night session, `fieldtests/raw/
Alpha0.1.2/*nighttest*`, build 3b56c11 -- the 12-part page at one hop):
raw-fragment DIRECT performance restored to the e87cca8 shape.** Three
sessions compared like for like at ONE MeshCore hop, desktop
(7bd024b5d082) serving the laptop (343377c464a7): `fieldtests/raw/
binaryfieldtest/` (e87cca8, 2026-09-18 night), `fieldtests/raw/
postAlpha0.1.1/*eveningtest*` (3c0a836) and `fieldtests/raw/Alpha0.1.2/
*nighttest*` (3b56c11):

    one hop, desktop -> laptop                    e87cca8      3b56c11
    reconcile answers sent in-window that arrived  11/13 (85%)  22/46 (48%)
    raw sends completing without text fallback     8/8          16/20
    raw send median duration                       28s          39s (74s evening)
    packets dropped for want of an in-flight slot  0            6
    12-part page transfer                          done         cancelled by RNS, 469s

Zero hop was 96-100% answer delivery in every build. Root causes, in
order of effect, and what changed:

 1. **The radio-free answer wait let the querier key over its own answer
    at the repeater.** e87cca8 held `_direct_exchange_lock` from the
    QUERY's transmit through the ANSWER wait; commit 1919074 made the wait
    radio-free (to stop a node's own ANSWERs queueing 50s behind its
    waits), which lets the querier's next raw burst start the instant the
    QUERY is ACKed. Through a repeater the answerer is a hidden node and
    the collision happens at the repeater: the querier's RX log shows 22 of
    the night's 24 lost answers were never decoded by its radio at all. At
    zero hop the firmware's listen-before-talk prevents it, which is why
    zero hop never moved. Fix: `_send_direct_frame_and_wait_for_ack` takes
    an optional `quiet_wait` future and `quiet_window_s`; after the ACK and
    the listen delay it keeps the lock until the future resolves or the
    window has elapsed since the frame's own MSG_SENT (`asyncio.shield`,
    so the caller's future survives the timeout; anchored at the real
    transmit because under concurrent sends the QUERY first waits 5-10s
    for this very lock, which a deadline fixed earlier would have spent).
    `_query_remote_fragments` passes its answer future and a window of
    `direct_completion_quiet_base (1.5) + direct_completion_quiet_per_hop
    (2.5) x hops`, capped by the answer budget
    (`_completion_quiet_window_s`) and CHARGED against it; the rest of
    the budget is waited radio-free (review fix 2026-09-20: the first
    cut started the budget after the hold, which would have quietly
    extended the capped answer wait by the hold; the ack-wait method now
    hands back `quiet_info` so the caller accounts for it and still
    measures a round trip for an answer that arrived inside the hold). Sizing: query receipt -> ANSWER on air at the
    answerer is median 1.3s, one repeater forward 1.5-3s. Anchored at the
    transmit, the window has already closed by the time a zero-hop ACK is
    in, so zero hop is untouched by construction. The hold is captured as
    `quiet_hold_s` on `direct_attempt_result`.
 2. **One incomplete raw send paused raw for 600s.** The "answered but
    incomplete after every round" exit paused raw unconditionally: at
    21:45:14 one part lost the same fragment three rounds running and the
    next 46 page parts went as five text fragments plus five ACKs each.
    e87cca8 only paused after two answered reconciles proved a burst
    delivered nothing. Fix: that exit records a SOFT strike
    (`_raw_incomplete_strikes`, per peer) and pauses raw only at
    `direct_raw_incomplete_strikes` (2) consecutive ones; a completed raw
    send clears the count, as does a path change. `direct_raw_fallback_
    cooldown` 600 -> 120. The two-strike "delivered nothing" rule, the
    per-path verdict and the text fallback of the current packet are
    unchanged.
 3. **The per-peer in-flight cap dropped packets and did not help.**
    `direct_fragmented_max_in_flight` (2, added in 3b56c11) was a FIFO
    `asyncio.Semaphore` held through every reconcile round: desktop
    fragmented sends waited a median 30s for a slot, four 483-byte Resource
    parts were dropped after the 120s slot budget (`slot_expired`), two
    laptop data packets were dropped while both slots were held by
    30-minute LXMF announces reconciling at two hops -- and reconcile
    timeouts did not improve (desktop 53% vs 35% the evening before). Fix:
    default 2 -> 0 (off). When enabled: `_PriorityAsyncSemaphore` (the
    counting form of `_PriorityAsyncLock`'s waiter ordering), a separate
    single slot for announce-class sends (PRIORITY_LOW: ANNOUNCE,
    PATH_RESPONSE) so they cannot occupy the data slots, and a send whose
    slot wait times out PROCEEDS with a warning instead of dropping --
    `slot_expired` no longer exists. `slot_wait_s` stays on
    `direct_send_result`.
 4. **Re-query instead of re-burst** (`direct_raw_reburst_after_
    unanswered`, 2, commit 8bbb12e): 10 of the night's 59 desktop raw
    rounds sent no data. To be decided by simulation; NOT changed in this
    pass -- see the closing note below.

Kept unchanged, because they measured well: the hop-aware ACK ceiling
(`direct_ack_timeout_base/_per_hop`), the completion-answer caps and
hop-aware floor (`_completion_query_timeout_s`), the v3 completion nonce,
the plain-PROOF priority tier, `direct_hop1_abort_default`, and the
per-path raw verdict (`_note_raw_fallback_outcome`). No CHANNEL fallback
for DIRECT traffic was added, and the wire format is unchanged (a 3b56c11
peer interoperates).

Verification status (2026-09-20, the session was asked to wrap up before
the simulated scenarios had been run to a pass): the unit tests
(`tests/test_raw_fragments.py::NightSessionFixes`,
`tests/test_second_audit_0919.py::FragmentedSendsPerPeerAreBounded`) pass;
the simulated scenarios (`NightSessionScenarios` in test_raw_fragments.py:
one-hop 12-part page, bidirectional answer-queueing bound, three-hop
mixed traffic with the cap on) are written but gated behind
`SMCI_RUN_UNVERIFIED=1` until someone has run them. Two simulator
findings from the partial A/B, recorded in `changelog.md`: the air model
lacked the firmware's listen-before-talk (added, `simmesh/air.py`), and
the scenario's parts must be Resource class or `outgoing_max_age` expires
them mid-transfer. Seed-11 numbers on the corrected air model, plain DATA
parts: baseline 3b56c11 answered 41% with 6 slot drops and 6/12 delivered
at 900s; all four changes at defaults 63%, no drops, 8/12; the same with
the (now non-dropping) cap re-enabled at 2: 80%, 10/12 -- the cap did
BETTER in the sim than in the field, so the 2 -> 0 default deserves a
field check rather than being taken as settled. Change 4's re-burst
default stays at 2: the single seed measured for 1 (61% answered, 4/12
delivered vs 63% and 8/12 at 2) is not enough to move it.

**Review of the night-session fixes against the simulators, plus the MeshBench
real-firmware findings (2026-09-20).** Two sources of evidence: the simulated
one-hop page transfer (`tests/test_raw_fragments.py::NightSessionScenarios`
and its helpers, twelve 483-byte Resource parts through one repeater,
calibrated loss, three seeds -- whose helper had never built a valid part
until this pass, so every earlier number quoted for it came from plain-DATA
runs that expired mid-transfer), and `testscripts/meshbench_scenarios.py`
against real MeshCore v1.17.1 firmware (changelog, "MeshBench real-firmware
test tier"). Six changes, none to the wire format:

 1. **The in-flight cap is back on at 2** (`direct_fragmented_max_in_flight`
    0 -> 2). In the simulated page transfer the non-dropping, priority-
    aware cap was the single most effective change: 12/12 parts in 181-243s
    with raw completion 90-100% on every seed, against 3-10 of 12 in 600s
    with the cap off. What the night session held against the cap (dropped
    parts, announces starving data) is exactly what the rewrite removed.
 2. **The reconcile quiet window is anchored at the QUERY's ACK, sized
    2.0 + 3.0 x hops, and grows with the measured round trip** (srtt + 2 x
    rttvar once three samples exist), still capped by the answer budget and
    only held after an ACK. The first cut measured 1.5 + 2.5 x hops from
    the transmit, which the ACK's own round trip consumed: in the field
    captures answers reached the querier (from the ACK) at one hop p50
    3.7-4.5s and p90 9-11s, so that window covered 19-32% of the answers
    that arrived, and in the simulated transfer a 9s window beat it on every
    seed. In MeshBench the hold now releases ~0.7s after the ACK when the
    answer is coming and runs its 5s when it is not.
 3. **The raw-fragment gap includes the frame's own airtime** through
    repeaters: `(1 + direct_raw_hop_gap_factor x hops) x airtime`. MeshBench
    finding 2: `send_raw_data` returns OK when the frame is queued, so the
    old gap of `factor x hops x airtime` had the fragment's ~1.3s on air
    eaten out of it and the next fragment or QUERY left ~0.9s after the
    frame ended, inside the repeater's relay -- 7/7 second fragments lost at
    R in `large_payload`, 7/9 QUERYs in `relay`. After: first bursts deliver
    2-4 of 4 fragments per packet instead of 3 of 4 with fragment 1 always
    missing, and `large_payload` delivered 2/6 round trips (was 1/6).
 4. **A completion ANSWER waits out the QUERY's ACK relay** before it
    leaves (`_completion_answer_hold_s`: ACK airtime x (1 + 2.5 x hops),
    zero at zero hop). MeshBench finding 3: the firmware ACKs the QUERY at
    once and the repeater relays that ACK; the ANSWER used to go out the
    millisecond the ACK's airtime ended and all three two-hop ANSWERs in
    `two_hop` were lost that way.
 5. **The SELF_INFO radio block is bounded** (`_parse_radio_params`: SF
    5-12, BW 7.8-500 kHz, CR 5-8), refreshed after the interface's own
    `set_radio`, and a single frame whose airtime estimate exceeds the whole
    duty-cycle budget is logged at WARNING once. MeshBench finding 1: a
    fresh-booted companion reported bw as 0.063 kHz, the old check accepted
    it, a 38-byte frame was priced at 1160s and the node sent one frame per
    minute with nothing in the log.
 6. **Small-mesh DIRECT-to-all no longer sleeps the CHANNEL spacing.**
    `_send_direct_supplement` takes `alongside_broadcast`; the hop-scaled
    5-10s x hops gap exists to clear a broadcast of the same packet, which
    `_send_direct_to_all_peers` never sends (MeshBench finding 4: most of
    one two-hop probe's 51s round trip).

Verification: fast suite 167 tests, the slow in-process scenarios 9/9, the
simulated page transfer on the new defaults (seed 11: 12/12 in 227s, raw
completion 92%), and MeshBench `relay` PASS 5/8 (RNS path in 33s, was 159s),
`large_payload` PASS 2/6 (was FAIL 1/6), `zero_hop` 6/8 and `two_hop` 2/8
with no DIRECT path ever resolved because B's advert never survived two
repeaters -- the bring-up coin flip of MeshBench finding 7, on code paths
none of these changes touch. Tests: `tests/test_meshbench_findings_0920.py`,
and the updated gap, cap-default and quiet-window cases. Still open from the
MeshBench report: the startup burst (finding 5), stale-path resets under
pure congestion (6), and the bring-up odds through repeaters (7).

**Receiver-initiated completion REPORT for raw bursts (2026-09-20, the
speed/airtime/reliability pass: five parallel reviews of the send path,
fragmentation, discovery, timing defaults and airtime, merged and ranked,
then evaluated one at a time against a two-run MeshBench baseline of
`zero_hop`, `relay`, `two_hop` and `large_payload` on the unmodified
tree).** The single mechanism three of the five reviews converged on, and
the one every baseline run showed directly: after a raw burst the sender
keyed its reconcile QUERY the instant the last fragment's gap ended --
exactly when the receiver, having just handed the packet to RNS, transmits
its own reaction (the delivery PROOF, or the next Resource request). At
zero hop the two frames collided outright (baseline `zero_hop`: 25-46%
DIRECT attempt success, 23-25 half-duplex misses per run, probe RTT 9-27 s
for a 147-byte packet); through a repeater they collided at the repeater as
hidden nodes (baseline `relay`: the receiver held probe 4 at t=187.9 s and
the sender learned it at t=223.8 s, after four QUERY attempts and four
PROOF attempts had taken turns colliding at R). The field captures say the
same thing from the other side: `answering_complete=True` on 49/82, 45/74
and 51/68 of the QUERYs the three 2026-09-19 sessions answered (the
receiver already held the whole packet when asked), 3.3 QUERY attempts per
raw send, receiver-complete p50 7.7 s versus sender-known p50 34 s at one
hop.

The change (`direct_raw_report_enabled`, default yes; `direct_raw_report_
wait_base` 2.0 s + `direct_raw_report_wait_per_hop` 3.0 s x hops, never
more than the QUERY answer budget):

- *Receiver.* `_handle_direct_multifragment_frame(raw=True)` sends the
  existing v3 ANSWER frame UNSOLICITED -- `_send_completion_report` -- when
  a raw bucket completes (spawned before `process_incoming`, so it enters
  the radio lock ahead of whatever RNS sends back), when the burst's
  flagged last fragment arrives and the bucket still has gaps (carrying
  the bitmap, so the sender re-drives exactly the missing fragments), and
  when a flagged fragment arrives for a packet already delivered (the
  sender re-burst because it never got the report). No
  `_completion_answer_hold_s`: a report follows a raw fragment, which the
  firmware does not ACK, so there is no ACK relay to wait out.
- *Wire.* Bit 2 of the raw header's byte 0 (`RAW_FLAG_REPORT`, 0x04; bits
  0-1 stay the round, the high nibble the version) marks the last fragment
  of every burst. Reports carry nonce `COMPLETION_REPORT_NONCE_BASE |
  round` (0xF0..0xF3); QUERY nonces now cycle 1..0xEF
  (`COMPLETION_QUERY_NONCE_MAX`) so the two ranges never meet. Both field
  nodes are updated together; an older peer masks the bit away and never
  reports, and the sender then pays the report wait before its QUERY.
- *Sender.* `_send_direct_raw_fragmented` registers the
  `(peer, pkt_id)` waiter under the round's report nonce BEFORE the burst
  (a multi-fragment burst completes at the receiver while the sender is
  still in the last fragment's gap), keeps the lock with the radio quiet
  for `_completion_report_wait_s` after the burst (`_await_completion_
  report`), and only if nothing arrives runs the QUERY path exactly as
  before. A matching-round report resolves the waiter; a `complete=True`
  report of any round is accepted by the handler's existing monotone rule;
  a stale round's incomplete bitmap is discarded. A report counts as path
  evidence (`record_direct_send_result` success), as an answered QUERY did.
  Captured as `completion_check_result` with `outcome="reported"` and
  `report_wait_s`; the receiver writes `completion_report_sent`, and the
  report's own DIRECT attempt is `kind="completion_report"`.
- *A lost last fragment (second and third cuts, same day).* The first
  cut flagged only the last fragment, so a LOST last fragment (uniform
  ~18% per fragment at one hop in the field; systematic in MeshBench's
  LBT-less radio, where the tail chases the repeater's relay of the
  fragment before it) meant no report, the whole report wait AND a
  QUERY: MeshBench `large_payload` went 0/6 and 0/6 against a 1/6-4/6
  baseline. The second cut added a receiver-side idle timer (report the
  bitmap once fragments stop arriving for two sender gaps + an airtime
  + 1 s) and a matching longer sender wait, and went 0/6 again: in that
  lossy, bidirectional scenario the timer's report queued behind the
  receiver's own sends, landed after the sender's wait had expired, and
  both sides then transmitted into each other at the repeater -- while
  the scenario's 60 s per-probe deadline sits exactly where the
  baseline's 34-48 s part times were, so any slowdown of the lossy case
  flips delivery. The third cut has no timer: the flag rides the LAST
  TWO fragments of a burst, the sender waits only the transit time
  (2 + 3 x hops s) for a report, and if the last fragment's report never
  comes it acts on the second-last fragment's report (kept when it
  arrived mid-burst, captured as `outcome="reported_stale"`; the
  fragments sent after it are re-driven, at worst one duplicate
  fragment) and only otherwise falls back to the QUERY. Bytes per part
  when both flagged fragments land: two reports + two ACKs, the same as
  a QUERY exchange -- the win is the missing round trip and the
  sender's silence, not bytes.

Airtime: the QUERY frame and its firmware ACK (and their relays) are gone
from every part that lands; a report is the ANSWER that was going to be
sent anyway. Speed: the sender learns of delivery one report airtime after
the burst instead of after gap + QUERY + ACK + answer. Reliability: the
sender is silent by construction while the receiver reacts, so the
QUERY-vs-PROOF collision leaves the common path.

MeshBench (two runs each, working tree with this change only, against the
two-run baseline of the unmodified tree; MeshBench's RF is optimistic, its
airtime ~30% pessimistic, its runs not reproducible, and -- established
this pass against `relay-1`'s event log and `Dispatcher::checkSend` -- its
virtual radio has no listen-before-talk, so it over-counts self-collisions
between nodes that can hear each other while hidden-node collisions at a
repeater are real): `zero_hop` 7/8 and 8/8 delivered (baseline 7/8 and 6/8, the latter a FAIL), probe RTT avg 5.0 / 8.2 s (12.3 / 14.6), zero-hop DIRECT attempt success 100% (25-46%), half-duplex misses 4-16 per run (23-25), QUERY attempts on air 0-2 (9-13); `relay` 8/8 and 7/8 (5/8, 4/8), the repeater relayed 67 / 63 frames (118 / 133), endpoint transmissions 42+42 / 40+39 (81+79 / 103+95); `large_payload` 3/6 and 3/6 (1/6 FAIL, 4/6), delivered-probe RTT avg 34.9 / 40.3 s (40.9 / 39.6), QUERY attempts 7 / 12 (24 / 22), R relayed 125 / 135 (184 / 138), per-part first-fragment-to-known-complete 18.6-64.6 s (34.4-83.0 s); `two_hop` 2/8, 3/8, 3/8, 2/8 over four runs (3/8, 5/8) -- in three of the four the DIRECT path resolved after 450 s or never (0-1 raw sends, no report code ran), and two runs lost 2-4 probes to the unknown-destination backoff that the third change below removes; the one run with a live 2-hop path (2/8) lost single raw fragments in the chain twice in a row and fell to the existing raw-pause strikes, with the report working where it applied.

**Dead-wait trims (2026-09-20, same pass, no wire change):** three
timing defaults whose evidence is the same 3693 `direct_attempt_result`
and 539 `completion_check_result` records of the 2026-09-19 sessions.

1. `direct_completion_unacked_grace` (6.0 s; `_multihop` 10.0 s from 2
   hops). The answer wait after a QUERY whose OWN firmware ACK was missed
   used to run the full 15-18 s budget. Across the three sessions such
   queries were answered 6/19, 9/65, 4/31 and 0/5 times at 0-3 hops, every
   hop<=1 answer inside 5.6 s, and with `miss_diagnosis=hop1_loss` 0 of 26
   ever: the missing ACK is the signal that the QUERY never reached the
   peer (48% of unanswered reconciles, second audit), and ~1300 s were
   waited for nothing. The re-query now goes out 9-12 s sooner.
2. `direct_ack_timeout_base`/`_per_hop` 8 + 4h -> 5 + 3h. Re-derived over
   2670 field ACKs, the largest that ever arrived was 3.82 / 6.06 / 8.15 /
   7.25 s at 0 / 1 / 2 / 3 hops; 5 / 8 / 11 / 14 s cuts off zero of them
   (31-93% margin) and, in replay, saves 824 + 635 + 171 s of dead lock
   time at 1-3 hops beyond what 8 + 4h saved. `direct_ack_min_timeout`
   (5 s) still floors hop 0. `_send_completion_answer` now passes
   `hop_count`, so an ANSWER's own ACK wait is hop-aware (it ran on the
   flat firmware suggestion before, capture records show `hop=None`).
3. `direct_post_send_listen_min/max` 0.3-3.0 -> 0.2-1.0 s. The post-miss
   listen averaged 1.7 s on 598 misses (~1000 s of lock time), while the
   evening audit measured frame overlap between the nodes at 8.6% against
   6.9% by chance, and the firmware's own listen-before-talk keeps a retry
   out of an audible frame anyway. Still a random draw, per the standing
   instruction.

MeshBench (two runs each, on top of the completion report, against that
change's runs): `relay` 8/8 and 8/8 (P1: 8/8, 7/8), probe RTT avg 10.4 / 13.6 s; `large_payload` 6/6 and 1/6 (P1: 3/6, 3/6) plus two more runs: 5/6, and one with no DIRECT path ever resolved (0 raw sends, uninformative), delivered-probe RTT avg 31.1 / 41.9 s, per-part first-fragment-to-known-complete 14.5-37.8 s; `two_hop` 2/8 and 2/8, both with the DIRECT path resolving after 497 / 543 s and 4 probes each dropped by the unknown-destination backoff (no trimmed wait was on the critical path). The mechanics are unambiguous across the captures: the longest missed-ACK wait at one hop 12.0 -> 8.0 s, post-miss listen mean 1.55-1.65 -> 0.59 s, completion-check timeouts 97 (baseline set) / 19 (P1 set) -> 8.

**A CHANNEL-carried PROOF clears the unknown-destination backoff
(2026-09-20, same pass):** baseline `two_hop-1` delivered probes 2 and 3
over CHANNEL (no DIRECT path yet) and their PROOFs came back over CHANNEL
(`channel_bare`), yet the interface counted three bootstrap attempts "with
no token learned" and dropped probes 4-7 outright for 300 s
(`unknown_dest_backoff_drop`) while the destination was provably
answering. The 2026-09-19 afternoon fix only clears the backoff for a
PROOF that arrives DIRECT (`_observe_incoming_rns_packet`), and the
CHANNEL receive path -- deliberately, its sender is unauthenticated --
observes nothing. `_note_channel_proof`, called from `process_incoming`
for every `channel_*` transport, matches the PROOF's destination field
against `_pending_dest_proofs` (a remembered bootstrap DATA send) or
`_pending_link_requests` (an LRPROOF's link_id) and clears that
destination's backoff. It learns NO token: a forged CHANNEL proof can at
most keep this node trying a destination it would otherwise have paused
on, which is the pre-backoff behaviour. A LINKREQUEST to an unresolved
destination takes the same path, so this is also what keeps a link
attempt inside MeshChat's 15 s window from being dropped by this
interface during a discovery backoff.

MeshBench (`two_hop`, `relay`, two runs each, on top of the two changes
above): `two_hop` 4/8 PASS and 5/8 (a mechanics FAIL only because no DIRECT path ever resolved -- the bring-up coin flip of finding 7; all five probes were delivered over CHANNEL), with ZERO `unknown_dest_backoff_drop` records, where every earlier late-path two_hop run (baseline, P1, P2: six runs) had dropped 2-4 of 8 probes itself and delivered 2-3; `relay` 8/8 and 8/8, probe RTT avg 12.1 / 8.6 s, unchanged from P1+P2 as expected.

Still open from the same pass, in the merged ranking (evidence in the
session report, `/tmp/mb/report-2026-09-20.md` at the time): bind frames
carrying the full 32-byte pubkey so `add_contact` + the telemetry grant
happen at bind and discovery no longer waits on an advert crossing the
repeaters (the `two_hop` bring-up coin flip); answering an RNS path
re-request from the cached announce locally; no reconcile quiet hold at
zero hop and a longer one from two hops; the one-hop raw gap giving the
sender's own listen-before-talk credit for the first repeater (field-only
check: MeshBench has no LBT); a 9-10-byte raw header so a 483 B part is
three fragments; HANDSHAKE-tier waiters pre-empting idle lock holds and
raw bursts yielding between fragments (MeshChat's 15 s link window);
duplicate suppression counting only copies that have started
transmitting; PATH_UPDATE adoption; shorter bind jitter and re-request
cadence; a peer-silence gate on the stale-path reset; a 3 s startup
stagger; the completion ANSWER as no-ACK CLI_DATA; one-AES-block "Q"
frames; small packets as ACKed text rather than raw; a third bare
attempt at >= 2 hops; the TXT_MSG airtime overhead 6 -> 5 B; resumed raw
sends not reusing round numbers.

**Airtime / throughput pass, phase 1 (2026-09-20 evening): small wins on
the existing code, one commit each, before the module split (phase 2) and
the reconcile redesign (phase 3).** The metric for every decision in this
pass is on-air bytes per delivered RNS byte, read with the delivery rate
and the per-part completion time; the evidence is the 2026-09-20 field
session (`fieldtests/raw/Alpha0.1.3/`, desktop serving pages to the
laptop at zero hop, then at two hops) read against the `alpha-0.1.3`
MeshBench baseline (`tests/baselines/alpha-0.1.3-simulatedbenchmark/`).
Phase 0 put two golden snapshot tests in place first (`tests/test_golden_
config_defaults.py`, `tests/test_golden_wire_format.py`, generated from
the frozen alpha 0.1.3 build) so every later change moves exactly the
bytes and defaults it means to. The LXMF finding from the same phase,
verified against `RNS/Resource.py` and LXMF 1.1.1 (the AppImage's LXMF
1.0.1 / RNS 1.3.7 bytecode carries the same constants): nothing in LXMF
or MeshChat times a transfer; the binding timer is RNS.Resource's
sender-side proof wait, entered the moment the LAST part has been sent
once (`AWAITING_PROOF`, `PROOF_TIMEOUT_FACTOR` 3, `SENDER_GRACE_TIME` 10,
three retries) -- four consecutive intervals of `3 x rtt_r + 10 s` with
no part request cancel the resource (`rtt_r` = advert to first request,
about this interface's DIRECT round trip: 56-112 s at 1.3-6 s), LXMF then
tears the Link down and retries the whole message from scratch (at most
four times). A lost tail part is re-requested by the receiver only after
twice the previous window's pace, so with a 4-part window the interface
must sustain more than `240 / (6 x rtt_r + 20)` parts per minute (7.5/min
at rtt 2 s, 5.5/min at 4 s) for one lost tail fragment to be recoverable;
the 39-part transfer at ~5 parts/min was cancelled exactly when its last
part had gone out once. That is the number phase 3 is sized against.

 0. *The metric is now in the capture.* Every transmit record --
    `direct_attempt_result` (text frames, via `_text_frame_on_air_bytes`,
    the framing the duty-cycle limiter already prices), `raw_fragment_
    sent` (2 + path + frame) and `channel_fragment_sent` (which the
    single-frame CHANNEL send now writes too, frag 0/1) -- carries
    `on_air_bytes`, and `testscripts/field_ab_compare.py` reports on-air
    bytes per delivered RNS byte (own transmissions over the RNS bytes of
    the DIRECT sends that completed). On the 2026-09-20 session, with the
    pre-field-values estimated from frame sizes: desktop 2.59 B/B
    (serving, zero then two hops), laptop 1.43 B/B (zero hop). Capture
    only; nothing decides on it.

 1. **A bare DIRECT send stops retrying once its reply is seen**
    (`_answered_send_key`, `_signal_send_answered`; `cancel_key` on
    `_send_direct_with_attempts`, `cancel_event` down to
    `_await_direct_ack`). Laptop capture `*144922` at two hops, relative
    to its first record: LINKREQUEST out at 2213.5 s; attempt 0 lost its
    firmware ACK (the ACK itself was heard on air 0.6 s after the 11 s
    wait ended) and the LRPROOF arrived at 2227.4 s, during attempt 0's
    post-miss listen; attempt 1 re-sent the request at 2235.4 s -- a
    99-byte frame plus a 3.4 s ACK at two hops, eight seconds after the
    Link was proven, with the LRRTT queued 3.8 s behind it. The DIRECT
    PROOF branch of `_observe_incoming_rns_packet` (every proof, before
    the `_pending_*` pops) and `_note_channel_proof` now signal the key;
    the retry loop makes no further attempt (one `direct_attempt_result`
    with `ack_timeout_source="answered"` for the attempt that did not
    happen), and an ACK wait already running ends at once, as
    "answered", with no RTT sample and no backoff. Path evidence
    (`record_direct_send_result` success) is recorded only when the
    reply came DIRECT from the peer the send was addressed to -- a
    DIRECT-to-all copy cancelled by another peer's relay, or a CHANNEL
    copy, learns nothing about this peer's path. The key is derived in
    `_send_direct_payload`, the one place bare sends are dispatched, so
    a supplement copy shares it: a LINKREQUEST's link_id, or a plain
    SINGLE-destination DATA's truncated hash (its PROOF's destination
    field; a Link packet's proof carries the link_id, so Link DATA has
    no key). Events and answered keys are swept on the proof-correlation
    TTL. Tests: `tests/test_answered_sends_0920.py`.

 5. **The completion-report window is sized from the measured report
    latency** (`_report_rtt`, `_record_report_latency`, `_expect_report`;
    default change `direct_raw_report_wait_base` 2.0 -> 4.0 s and
    `direct_raw_report_wait_per_hop` 3.0 -> 2.5 s). Zero hop, 2026-09-20
    session: the receiver's report attempt waited a median 1.1-1.4 s and
    p90 4-5 s for its own radio lock (behind its previous report's ACK
    wait and its own sends) on top of ~2.3 s of serial delivery latency,
    so with a 2 s window the desktop saw only 43 of its 77 hop-0 rounds
    `reported` while 29 fell back to a QUERY that was then `answered` --
    two frames, two ACKs and ~5 s for a report that was merely late --
    and the on-time reports' `report_wait_s` (median 1.18 s, max 1.97 s)
    were truncated by the window itself. Now: floor `base + per_hop x
    hops` (4 / 6.5 / 9 / 11.5 s, the answer budget's own slope so the
    floor sits under the budget at every depth); a per-peer
    Jacobson/Karels estimator of burst-end -> report-arrival, sampled in
    `_handle_incoming_completion_frame` for every report whose
    `(peer, pkt_id)` expectation the sender registered at the burst's
    end -- on time or LATE (the estimator must see the reports the window
    missed, or it can never grow past the window) -- withdrawn when the
    round ends; window = max(floor, srtt + 2 x rttvar), never above the
    QUERY answer budget; dropped with the peer's other path stats. The
    cost of a lost report at zero hop rises 2 -> 4 s of quiet radio (5 of
    77 rounds); the saving is a QUERY round trip on the late ones (29 of
    77). Tests: `tests/test_report_window_0920.py`; the shipped-default
    pins and the golden config snapshot re-pinned in the same commit.

    Three things the review of the same capture added (same commit set):
    (a) 20 of the desktop's 43 "reported" hop-0 rounds had acted on the
    SECOND-LAST fragment's report -- both of a burst's last two
    fragments are flagged, the receiver reports the gap the moment the
    second-last lands, and that report (missing only the last fragment)
    reached the sender first -- and each re-drove the last fragment as a
    duplicate before the complete report arrived (the complete report
    follows the incomplete one by a median 1.5-1.9 s at zero hop, its
    ACK wait). `_await_completion_report` now treats a report whose only
    gap is the last fragment sent as provisional: it re-arms the waiter
    and keeps waiting up to half the window for the last fragment's own
    report, acting on the provisional one only if nothing better arrives
    (`provisional: true` in the capture). Only COMPLETE reports feed the
    estimator, for the same reason. (b) 24 of the 29 "answered" hop-0
    outcomes were a late report resolving the QUERY's future through the
    monotone rule while the QUERY still waited for the lock -- the QUERY
    then went out anyway, was answered, and the ~0 s "round trip" shrank
    `_query_rtt`. A QUERY whose answer future is already done is no
    longer transmitted (`ack_timeout_source="answered_before_send"`),
    and a report that resolves a QUERY feeds `_report_rtt`, never
    `_query_rtt`. (c) The window uses srtt + 2 x rttvar, the factor
    `_completion_quiet_window_s` uses: the true zero-hop distribution is
    median ~3 s / p90 ~7 s and +4 x rttvar would settle at 8-10 s, which
    a lost report pays in full.

 2. **Stale plain PROOFs age out** (`proof_max_age`, 45 s; 0 = off;
    `_plain_proof`, `expire_retries` on the bare send path). A delivery
    PROOF for a non-Link packet is useful only until the sender's RNS
    receipt deadline: `PacketReceipt.timeout` = `first_hop_timeout` (MTU
    500 B x 8 / this interface's `bitrate` 80 bps = 50 s, + 6 s) + 6 s per
    RNS hop = 62 s from the sender's transmit (`RNS/Packet.py` 428-431,
    `Transport.first_hop_timeout`); LXMF's opportunistic delivery retries
    every 10 s on top and never waits longer. The desktop's 2-hop phase
    of the 2026-09-20 session queued 13 plain proofs while every attempt
    missed (lock waits 8 -> 70 s, queue depth 13) and then transmitted
    12 of them aged 45-105 s in a row once the channel cleared; 29 of
    102 proofs failed both attempts; age at last transmit was median
    11.5 s, p90 52.5 s, max 105.6 s. Replaying that capture with the
    per-tier FIFO lock: a 45 s cap skips 16 attempts (~76 s of radio
    lock) and loses 3 proofs that still reached the laptop inside its
    deadline (all between 45 and 62 s sender-side); 60 s skips 12,
    loses none; 30 s skips 24, loses 4. With ~5 s of transit each way at
    two hops, 45 s is where the deadline sits. Unlike `outgoing_max_age`
    (checked once, before the first transmission, because a retry is
    committed air) a plain proof expires before EVERY attempt -- it is
    one bare frame, nothing already spent -- at dequeue
    (`proof_expired_in_queue`), before each attempt and after each lock
    wait (`ack_timeout_source="expired"`), never as a path failure.
    LRPROOF / RESOURCE_PRF / the Link band keep only `outgoing_max_age`.
    Found and closed on the way: an attempt-0 expiry inside the lock
    wait used to fall through to attempt 1, which transmitted the
    expired packet after all (`attempt_info["expired"]`). Tests:
    `tests/test_proof_max_age_0920.py`; shipped-default pins and golden
    config re-pinned in the same commit.

 3. **An RNS path re-request is answered from the cached announce**
    (`_cache_announce`, `_answer_path_request_locally`;
    `announce_cache_ttl` 3600 s, `path_request_local_answer_min_interval`
    120 s, both 0 = off; `ANNOUNCE_CACHE_MAX_KEYS` 256). RNS mechanics,
    verified in `Transport.py`: on a non-transport node a pending Link
    that closes without activating makes `Transport.jobs` `expire_path`
    the destination and request the path again; the cull removes the
    entry, `has_path` is False, and the answering node replies from ITS
    path table with the cached announce bytes (`path_request` /
    `get_cached_packet`) -- the same bytes every time. `packet_filter`
    passes a duplicate SINGLE announce even if its hash is in the
    hashlist; the announce branch of `inbound` adds an unknown
    destination unconditionally (`should_add = True`) and silently
    ignores the same announce while the path exists; `request_path`
    records the destination in `path_requests` before sending, which is
    what exempts the answer from ingress limiting. Laptop captures
    `*144922` / `*153130` at two hops: the identical 235-byte announce
    for one destination arrived six times in an hour (payload hash
    b8efb9197b7a), each a 2-3 fragment raw send with its reports at two
    hops, each after a 2-hop DIRECT request (14 more requests
    rate-limited); on the desktop each answer cost ~2.7 raw fragments,
    ~2 QUERY attempts and ~14 s of radio lock, 7 answered and 5 more
    suppressed as duplicates in flight. Now every ANNOUNCE a bound peer
    delivers DIRECT is cached, bytes as received (CHANNEL announces are
    not: no authenticated source); a path request whose target is
    cached, whose source peer is still bound and not in discovery
    backoff, and which was not answered locally inside the interval is
    answered by handing the cached bytes back to RNS through
    `process_incoming` (transport `local_announce_cache`, `rxb` not
    counted) with the context byte rewritten to PATH_RESPONSE -- what it
    is, and on a transport node the value that keeps `inbound` from
    inserting it into the announce table for re-flooding; the announce
    signature does not cover the context byte -- and the request is not
    transmitted (`path_request_answered_locally`, naming the source
    peer). The local answer verifies nothing: the next request for the
    same destination inside the interval goes over the air, which is
    how a dead destination is re-verified. Every path-request capture
    record now carries `requested_hash`. Pinned against the real
    `RNS.Transport` of the test process (unknown destination added,
    duplicate ignored, accepted again after expire + cull) in
    `tests/test_local_announce_cache_0920.py`; shipped-default pins and
    golden config re-pinned in the same commit. Not persisted across a
    restart of the RNS process (a non-transport node reloads no path
    table either, so the first request after a restart is real).

 4. **Link handshakes pre-empt idle holds of the radio lock**
    (`_PriorityAsyncLock.acquire(preempt=True)` / `preempt_event` /
    `yield_to_preempt` at `YIELDED_PRIORITY` 0.5; `_is_link_handshake`;
    `_idle_hold`, `_wait_future_or_preempt`, `_PreemptedForHandshake`).
    The 2026-09-20 session: link-critical attempts (LINKREQUEST, LRPROOF,
    LRRTT) waited ~33 s for the lock over 26 attempts, median 1-3 s; at
    zero hop the holder was a completion report/answer's ACK wait plus
    its listen in 13 of the 20 waits of 0.5 s or more, a report wait in
    4; at two hops a LINKREQUEST waited 3.2 s behind a completion
    answer's 8 s ACK miss inside a 17.4 s link (MeshChat gives 15 s).
    The largest tier-0 waits were not handshakes: a KEEPALIVE queued
    20.3 s behind a raw burst's duty-cycle throttle wait -- 26.3 s, the
    longest idle hold of the lock in the session -- which is why the
    pre-empting class is the LINK handshake only (LINKREQUEST, LRPROOF,
    LRRTT, LINKIDENTIFY, LINKPROOF; KEEPALIVE / LINKCLOSE / the
    RESOURCE_PRF band keep PRIORITY_HANDSHAKE but pre-empt nothing --
    32.6 of the laptop's 56.3 s of tier-0 lock wait was KEEPALIVE, a
    20-byte packet nothing waits on). A pre-empting waiter sets the
    lock's event; the idle phases watch it: (a) a raw burst yields
    during a fragment's duty-cycle throttle wait (handshakes are exempt
    from the cycle, the burst was going nowhere) and after -- never
    inside -- a fragment's gap, re-acquiring at YIELDED_PRIORITY so it
    resumes ahead of any ordinary waiter that queued meanwhile and
    re-checking `_raw_path_reset_mid_send` afterwards; (b) the
    post-burst report wait releases the lock and keeps listening for the
    report radio-free (a report arriving then still completes the send);
    (c) a QUERY's quiet window ends early (the answer budget continues
    radio-free, as after the window); (d) the listen after a MISS yields
    once the rx-log prediction of busy air has passed, the short success
    listen never; (e) a completion ANSWER/REPORT's own ACK wait -- best
    effort, never retried -- is cut once the peer's expected ACK time
    has passed (`_ack_preempt_floor_s`: srtt + rttvar when measured,
    else 2 s + 1 s per hop; keying the handshake into the peer's ACK
    would lose both at a repeater), recorded as
    `ack_timeout_source="preempted"`: not a miss (no backoff, no
    listen, no path evidence), not a success. An ordinary frame's ACK
    wait is never cut. This is a deliberate exception to the 2026-09-15
    "lock held for the full send + ACK wait" contract and to the
    2026-09-20 report entry's "the sender is silent by construction":
    both now hold unless a Link handshake is waiting, which the field
    numbers say is worth ~0.5-1 s per handshake at zero hop, 3-4 s at
    two hops, and up to the whole throttle wait behind a burst. The
    summarisers (`meshbench_report.py`, `field_ab_compare.py`) count
    `preempted` / `answered` / `answered_before_send` / `expired`
    attempts apart from the per-hop success rate. Tests:
    `tests/test_handshake_preemption_0920.py`.

**Airtime / throughput pass, phase 2 (2026-09-20 evening): the module
split, no behaviour change.** Two steps, ten pure-move commits.

1. This history left the module docstring (540c1a1). The docstring keeps
   the rationale paragraph, the DESIGN INVARIANTS and a WIRE FORMAT section
   written from the code (`"R"` / `"P"` / `"Q"` text frames, the completion
   versions and nonce ranges, the 13-byte raw header and its payload
   budget), each pinned byte for byte by `tests/golden/wire_format.json`.
   New entries go at the end of this file.
2. The deliverable is assembled (b91d4a9 .. 38e7f2f). RNS loads a custom
   interface by `exec()`ing `<interfaces>/<type>.py` as text into a globals
   dict holding only `Interface` and `RNS` -- no `__file__`, no package
   machinery (`RNS/Reticulum.py`; `testscripts/check_install_load.py`
   does exactly that) -- so a package cannot be installed as such. The
   source is now the package `Interface/src/smci/` along the seams that
   already existed: `_common` (imports, `_cfg_bool`, Z85, the record
   types), `_locks` (the priority lock / semaphore, the duty-cycle
   limiter, `_PreemptedForHandshake`), then one mixin per concern --
   `_config` (`_configure_*`), `_observability` (capture, [STATS], the
   RX-log tap and airtime model), `_wire` (budgets, encoders/decoders,
   header parse and packet classes), `_peers` (bind protocol, peer cache,
   `_register_peer`, tokens, `_resolve_routing_peer`), `_paths`
   (discovery, stale-path detector, the per-peer estimators), `_direct`
   (the gate, bare/text sends, the attempt loop and ACK wait, the lock's
   idle-hold helpers), `_reconcile` (raw bursts, reports, QUERY/ANSWER,
   reassembly), `_routing` (the outgoing worker and dispatcher, CHANNEL,
   supplements, receive demux, `process_incoming`) -- and `interface.py`
   (constants, `__init__`, properties, lifecycle). `Interface/build_
   interface.py` concatenates them in dependency order, drops relative
   imports and hoists absolute ones into one block; `--check` refuses a
   stale deliverable and runs in the pre-commit hook. Each commit was
   audited with `testscripts/audit_split.py` (every function by name with
   its `ast.unparse`d body, every class constant by value, across any
   class): PURE MOVE, the only addition being the module-level
   `PRIORITY_*` mirrors in `_common` that mixin methods use as default
   argument values (`tests/test_module_split_0920.py` pins them equal to
   the class constants, the deliverable equal to what the sources build,
   and the package's class equal to the assembled one in methods).
   Method bodies, class constants, `__init__` and the `_configure_*`
   order are byte-identical to the phase-1 build (e7ba341). Gate: the
   full unit suite, and MeshBench `zero_hop` and `relay` twice each on
   this build and on the frozen alpha 0.1.3 build, recorded in
   `changelog.md` and the session report.

**Airtime / throughput pass, phase 3 (2026-09-20 night): the reconcile
redesign, `docs/reconcile_redesign.md`.** One module (`_reconcile.py`)
owns the burst-and-report state machine; every timing decision is a pure
function with a deterministic test pinned to the field number it came
from. Milestones in order, each gated on the full suite and MeshBench
`large_payload` + `relay` twice against the previous milestone's runs.

 M1. **Reports without a firmware ACK, debounced** (`direct_report_noack`,
     `direct_report_debounce`, both default yes; `_send_direct_noack_frame`,
     `_noack_frame_hold_s`, `_report_hold_s`, `_schedule_gaps_report`).
     Verified in the firmware: `TXT_TYPE_CLI_DATA` (1) is the same
     encrypted, MAC'd TXT_MSG datagram as a plain message
     (`BaseChatMesh::sendCommandData` -> `createDatagram`), relayed the
     same way, delivered to the host as CONTACT_MSG_RECV with `txt_type`
     1 (`MyMesh::onCommandDataRecv`), and never ACKed
     (`BaseChatMesh::onPeerDataRecv`: "no ack expected for CLI_DATA
     replies"; the companion's CMD_SEND_TXT_MSG handler sets expected_ack
     0). The library's `send_msg` hard-codes type 0, so the frame is
     built as it builds it -- `[0x02][txt_type][attempt][ts:4][dst:6]
     [text]` -- and sent through `commands.send()`. Every REPORT and
     every QUERY ANSWER now goes that way: the "Q" bytes are unchanged
     (the golden wire snapshot did not move), the receiver's lock is held
     only through the gate, the send and the frame's hold (its airtime
     plus the raw relay gap through repeaters; airtime plus the zero-hop
     gap at hop 0) -- never through the 1-3 s ACK wait that made reports
     queue behind each other (23 of the 31 report lock waits over 1 s in
     the 2026-09-20 zero-hop session). The QUERY stays ACKed (path
     evidence, the quiet window's anchor). Debounce: a flagged fragment
     that leaves gaps arms a hold of one fragment airtime (plus the relay
     gap through repeaters) instead of reporting at once; a completion
     inside the hold cancels it and only the complete report goes (the
     complete report followed the gaps report by 0.22-0.43 s at the
     receiver at zero hop; 146 reports for ~105 bursts, and the sender
     re-drove the last fragment as a duplicate 20 times on those). The
     sender's rules are unchanged: a lost report falls through to the
     QUERY. The fake `meshcore` gained the CLI_DATA semantics (`commands.
     send` of a SEND_TXT_MSG frame, delivered with txt_type 1, no ACK).
     Tests: `tests/test_reconcile_m1_noack_reports_0920.py`; the two
     receiver tests that expected an immediate gaps report now wait out
     the hold. Shipped-default pins and golden config re-pinned (two new
     keys). Gate results are in `changelog.md`.

 M2. **One report per window** (`direct_raw_window_enabled` yes,
     `direct_raw_window_collect` 0.75 s, `direct_raw_window_max_parts` 6;
     "Q" protocol v4; `_run_raw_window`, `_RawWindow`, `_RawPart`,
     `_window_collect_s`, `_apply_window_entries`,
     `_encode/_decode_completion_frame_v4`, `_recent_raw_entries`).
     RNS's Resource sender emits a window of 4-6 parts within
     milliseconds (`RNS/Resource.py` `request`); each was its own
     burst-and-report exchange, two in flight per peer, so a 4-part
     window cost about 12 reports and 6 quiet periods. Now consecutive
     raw-eligible sends to one peer that arrive within the collect
     window of the first form ONE window: every part's fragments burst
     back to back (in part order, the usual gaps, the last two fragments
     of the whole window flagged), one quiet period, one report. The
     window takes the per-peer in-flight slot the parts used to take
     (`direct_fragmented_max_in_flight` bounds windows now). The report
     is "Q" version 4 -- `[4][type][n][nonce]` then per entry
     `[pkt_id:2][frag_total][complete][bitmap]` -- listing every raw
     packet the receiver saw from that sender within
     RECENT_RAW_PKT_SPAN_S (60 s, newest first, at most 8, the
     triggering one first). The sender registers one waiter future under
     every pkt_id of the burst; the handler resolves it through the first
     entry that passes the per-part rules (nonce, monotone completion,
     frag_total) and `_apply_window_entries` applies every entry's bitmap
     authoritatively; parts complete leave the window, the rest are
     re-driven as the next round's burst; no report -> ONE v4 QUERY
     listing the outstanding parts (`_query_remote_fragments(entries=)`,
     the one future under every pkt_id), answered in v4 entry by entry.
     Every single-part rule -- resume, the provisional second-last-
     fragment report (its gap set is the union over the window's
     entries), the re-query-before-re-burst valve, handshake yields, the
     mid-send path-reset abort, the empty-burst and incomplete strikes,
     the text fallback -- applies to the window as it applied to the
     part; `_send_direct_raw_fragmented` is now "join or open the peer's
     window and await this part's outcome", and a single part (or
     batching off) is a window of one with no collect wait. v1-v3 frames
     still decode and a v3 QUERY is answered in v3; the golden wire
     snapshot was regenerated in the same commit: the 72 old
     default-version cases are byte-identical under their new `v3`
     names, the `default` cases moved to v4, and 96 v4 cases were added
     (`COMPLETION_PROTOCOL_VERSION` 3 -> 4). Both nodes must run this
     build. Tests: `tests/test_reconcile_m2_window_0920.py`; the golden
     wire and config snapshots and the shipped-default pins re-pinned.

 M3. **Three fragments per 483-byte part** (raw header version 2, 9
     bytes: `RAW_PROTOCOL_VERSION` 2, `RAW_HEADER_SIZE` 13 -> 9,
     `RAW_SRC_PREFIX_BYTES` 2; `_resolve_raw_src`, `_raw_src_ambiguous`).
     The version-1 header carried the sender's full 6-byte prefix so raw
     fragments would land in the same reassembly bucket as text
     fragments; bound peers are few (small-mesh mode caps at 3), so the
     receiver now resolves a 2-byte source prefix to the unique bound
     peer whose prefix starts with it -- and hands
     `_handle_direct_multifragment_frame` that peer's full prefix, so the
     bucket key is unchanged -- dropping a fragment whose prefix matches
     no bound peer or two (logged once per prefix), while a sender never
     uses raw to a peer whose short prefix another bound peer shares, or
     while a bound peer shares this node's own short prefix
     (`_raw_fragments_eligible`). Firmware limits re-read for this
     change: `onRawDataRecv` pushes payload + 4 bytes inside
     MAX_FRAME_SIZE 176 (172 received, the 2026-09-19 audit's number) and
     CMD_SEND_RAW_DATA is cmd + path_len + path + payload (174 -
     path_len). Per-fragment payload at the shipped cap of 170 is 161 up
     to four hops, and 3 x 161 = 483: the Link MDU part in three raw
     fragments instead of four (3 x 170 = 510 B on air instead of 4 x
     172 = 688, one fewer loss opportunity and gap per part). A v1 header
     is no longer decoded (both nodes are updated together). Golden wire
     snapshot regenerated in the same commit: the 31 raw-fragment and
     budget cases changed, nothing else. Tests:
     `tests/test_reconcile_m3_short_header_0920.py`; the codec, budget
     and strike tests updated to the 9-byte header (the strike scenario's
     payload is now sized to four fragments explicitly).

 M4. **Hop-adaptive XOR parity** (`RAW_FLAG_PARITY` 0x08; new keys
     `direct_raw_parity_enabled` yes, `direct_raw_parity_min_hops` 1;
     `_encode_raw_parity`, `_raw_parity_fits`, `_raw_parity_fragments`,
     `_reconstruct_from_parity`). From one hop up, every part's burst of
     two or more data fragments ends with one parity fragment: the raw
     header with the parity flag, frag_idx carrying the coverage mask,
     the payload the length of the highest covered fragment followed by
     the XOR of the covered fragments padded to the longest (at most 1 +
     161 bytes, which fits the firmware's 172 / 174 - path_len limits up
     to three hops; `_raw_parity_fits` withholds it where it would not).
     A round-1 re-drive of two or more fragments gets a parity over
     exactly those. The receiver keeps parities in the bucket
     (`_ReassemblyBucket.parity`, keyed by mask) and, after every data
     or parity fragment, reconstructs any covered fragment that is the
     only one missing -- the short last fragment's length comes from the
     parity's first byte -- then completes the bucket through the
     ordinary path, so the packet reaches RNS once and the have-bitmap
     reports data fragments only. Motivation (the 2026-09-19 field
     numbers): at one hop ~18 % of raw fragments are lost, so a three-
     fragment part loses exactly one 41 % of the time it loses any; each
     such part now completes in the burst instead of a report + re-drive
     round (one 170-byte frame instead of a report, a re-driven fragment
     and a second report, and a full report window sooner). At zero hop
     (~5 % loss) the parity's 25 % airtime is not worth it, so the
     shipped floor is one hop. Parity frames carry the burst's report
     flag (they are the burst's last frame) and appear in the capture as
     `raw_fragment_sent` with `parity_mask` and `raw_fragment_received`
     with `parity`. Golden wire snapshot: `_encode_raw_parity` cases
     added, nothing else changed. Tests:
     `tests/test_reconcile_m4_parity_0920.py` (the pure functions, the
     codec, reconstruction of each single loss with the parity before or
     after the loss is visible, two losses left to the report path, the
     burst and re-drive shapes at one hop, none at zero hop).

     Gate (M4 against M3, `large_payload` x3 + `relay` x2 each): M4
     4/6 @ 5.00, 5/6 @ 5.49, 1/6 @ 7.52 B/B; relay 6/8 + 1 late @ 6.52,
     8/8 @ 5.54 -- M3 2/6 @ 8.51, 6/6 @ 4.50, 6/6 @ 4.36; relay 7/8 @
     5.21, 7/8 @ 5.14. Mechanics PASS on every run (the 1/6 is a delivery-
     floor miss, not a mechanics FAIL), so by the milestone rule M4 is not
     reverted; but it showed no benefit and a cost, and MeshBench's own
     miss reasons say why: on every build of the pass, the frozen alpha
     0.1.3 included, R misses 30-40 % of A's raw fragments with "half-
     duplex: its own transmitter was keyed", in an alternating pattern
     (`XrX`, `XrXr`, `rXrX` per burst) -- the one-hop gap is (1 + 2 x 1) x
     airtime = 2.7 s from the real radio's ~0.9 s frame, and MeshBench's
     frames take ~1.3 s, so R is still relaying fragment N when N+1
     arrives. Under a strictly alternating loss a four-frame burst leaves
     two data fragments missing or the parity itself, the burst is a third
     longer, and B logged more half-duplex misses of its own (5 in run 3
     against 0-2 on M3). Reconstruction did work in the real stack (B
     logged `raw_parity_reconstructed` 1, 3 and 4 times in the three runs)
     -- the mechanism is sound, the regime is wrong. **Shipped off**
     (`direct_raw_parity_enabled` no; the code, tests and knob stay): the
     field's random one-hop loss is the case it was built for, and the
     field A/B (`fieldtests/AB_PROTOCOL.md`, both nodes `yes`) with
     `raw_parity_reconstructed` per single-loss burst as the field is what
     decides. The same MeshBench timing artefact is why `large_payload`
     swings 1/6-6/6 between runs of one build; `direct_raw_hop_gap_factor`
     is a spacing decision the field A/B owns, not MeshBench.

 Phase 4 (2026-09-20 night). Full suite 288 tests OK (the three `@slow`
     raw scenarios re-pinned: a 446-byte payload is three fragments since
     M3, and under both-ways zero-hop load a node's REPORT waits behind its
     own outgoing window -- one window of up to 6 parts, 12-15 s observed,
     bounded, pinned at 20 s; `page_transfer_bidir` measures that cost
     against the firmware). Version alpha 0.1.4 (both nodes must run it:
     "Q" v4 and raw v2 are not decoded by 0.1.3); readme lists the new
     keys and the build step. Baseline: `tests/baselines/2026-09-20-
     meshbench-6cf0876.md` (zero_hop, relay, two_hop, large_payload,
     page_transfer, page_transfer_bidir, link_setup x seeds 7/11/17; seed
     13 gives A a reserved identity the script refuses), against the
     frozen alpha 0.1.3 suite (`tests/baselines/alpha-0.1.3-
     simulatedbenchmark/`, same seeds), medians [range]: large_payload
     delivered 17 % [0-33] -> 83 % [50-100] at 12.76 -> 5.11 B/B (the one
     result outside the frozen spread, both ways); zero_hop 100 % at 3.39
     -> 2.85 B/B; relay 100 % [88-100] -> 88 % [75-88] at 5.74 -> 5.45;
     two_hop 75 % -> 88 % at 10.31 -> 9.31 but RTT median 12.2 -> 29.6 s
     (one run 3/8 + 1 late at 15.49 B/B; re-run before reading it either
     way); page_transfer 0 % [0-67] -> 33 % [0-67] at 8.07 -> 6.60, 12-part
     pages in 472-596 s (1.2-1.5 parts/min: MeshBench's one-hop relay
     regime, not a field number); page_transfer_bidir 0/3 both at 24.95 ->
     13.51; link_setup 75 -> 88 % delivered, handshakes inside MeshChat's
     15 s: 38 % -> 25 % (ranges 12-62 % both). One zero_hop run (seed 7,
     under the unit suite's CPU load) never resolved a MeshCore path in
     220 s -- B's firmware received all ten discovery requests and
     answered none (a path discovery is a flood + telemetry request,
     answered only after the responder's telemetry grant for the bound
     peer, `_grant_telemetry_permission_if_needed`); delivered 8/8 over the
     CHANNEL fallback, the unloaded re-run resolved at 87-97 s and passed
     (both runs are in the baseline). `mixed_builds` (responder on
     d7dcba9, an alpha-0.1.3-era build) delivered 0/6 where alpha 0.1.3
     got 3/6, 5/6, 0/6: the raw v2 bursts and v4 QUERYs are undecodable to
     the old node and the raw -> text fallback never fired in the run (4
     raw sends failed, 0 text fallbacks) -- the mismatch does not degrade
     cleanly. Not a follow-up: on 2026-09-21 the owner ruled that
     compatibility with earlier builds is not wanted while the project is
     in alpha -- both nodes run the same build, and that is the whole
     rule.

 2026-09-21. **Parity on by default** (`direct_raw_parity_enabled` no ->
     yes, the owner's decision on reading the pass report). The M4 gate's
     reading stands as written above -- under MeshBench's alternating
     relay loss the parity cannot pay for itself and the three-run
     comparison against M3 showed a cost -- and the owner chose the
     field's random-loss case over MeshBench's regime for the shipped
     default. Consequences on record: the alpha 0.1.4 baseline
     (`tests/baselines/2026-09-20-meshbench-6cf0876.md`) was taken with
     parity OFF; the parity-on MeshBench reference is the M4 gate
     (`large_payload` 4/6 @ 5.00, 5/6 @ 5.49, 1/6 @ 7.52 B/B; `relay` 6/8
     + 1 late @ 6.52, 8/8 @ 5.54; build 3ec8d7f, otherwise this code).
     A parity-on baseline suite is the next MeshBench run to make when a
     reference point is needed; the field A/B's `no` arm is the way to
     reverse this. Shipped-default pin and config golden re-pinned; the
     one-hop raw-send fixture (`tests/test_completion_report_one_hop_
     0920.py`) still turns parity off for its burst-shape pins.


**Alpha 0.1.5 pass (2026-09-21, from the alpha 0.1.4 field session's
captures in `fieldtests/raw/Alpha0.1.4/`).** The metric is unchanged:
on-air bytes per delivered RNS byte, read with the delivery rate and the
per-part completion time, per hop count; for zero hop, also the share of a
transfer's time spent in duty-cycle waits, because that is what the cap
change below moves. The owner's decisions in force for the pass: zero-hop
DIRECT traffic may use up to 85% of channel time, everything a repeater
relays stays at 30% and that number is never loosened; both field nodes
update together; parity stays on; aim for no wire change.

 1. **Hop-aware airtime cap** (`duty_cycle_max_fraction_zero_hop`, new,
    0.85; `duty_cycle_max_fraction` 0.30 unchanged in meaning). The field:
    the zero-hop 12-part page of 08:37-08:40 took 147 s, of which 109 s
    were duty-cycle waits at the single 30% cap -- the cap, not the
    radio, was the zero-hop ceiling, while two adjacent radios cost no
    repeater any air. `_DutyCycleLimiter` is now two ledgers over the
    same 60 s window: every frame is charged to the TOTAL ledger, capped
    at 85%; every frame a repeater will relay -- any DIRECT frame whose
    target has `out_path_len >= 1`, and every CHANNEL flood (announces,
    path requests, bind frames, channel fragments) -- is also charged to
    the RELAYED ledger, capped at 30%, and waits on both. A zero-hop
    DIRECT frame waits on the total budget only. The hop class travels
    through `_pre_transmit_gate(relayed=)` from every keying site: the
    raw fragment knows its path (`len(path) > 0`), the ACKed and no-ACK
    text frames carry the target's `hop_count` (`_relayed_frame`: the
    caller's out_path_len, else the peer's resolved path, else relayed --
    an unknown route is charged the stricter way, never the other), the
    CHANNEL and bind sites are always relayed. Handshake-class frames
    stay charged and never delayed, in whichever ledgers their class
    dictates. `wait_for_budget` now returns `(delay, ledger)` and the
    capture writes `duty_cycle_ledger` ("relayed" / "total" / None) on
    `direct_attempt_result` and `raw_fragment_sent` (the no-ACK frame's
    record now also carries its `duty_cycle_wait_s`, which it never did);
    `meshbench_report.py` sums the waits by ledger. Not gated, as before:
    the MeshCore path-discovery flood (`send_path_discovery_sync`, outside
    the gate by the M4 design) -- a few frames per hour, left alone.
    Tests: `tests/test_duty_cycle_hop_aware_0921.py` (both budgets, the
    mixed case, the one-cap compatibility, `_relayed_frame`, the gate's
    telemetry, the shipped pair); shipped-default pin and golden config
    re-pinned for the new key; the fast test profile adds the key at
    0.95 so the unit scenarios are not slowed. MeshBench gate in
    `changelog.md`: `zero_hop` and `duty_cycle_pages` should move,
    `large_payload` and `relay` must not (relayed traffic).

 2. **The burst / report collision** -- three coupled changes, one commit
    each, from the same 08:37-08:40 zero-hop timeline on both machines:
    the desktop queued window [8..12] (15 raw fragments, ~14 s of air at
    SF7/BW62.5) into the firmware in 2.6 s -- `send_raw_data` returns OK
    when the frame is QUEUED (`MyMesh.cpp` CMD_SEND_RAW_DATA: `sendDirect
    (...); writeOKFrame();`, the outbound queue drained by `Dispatcher::
    checkSend` one frame at a time) -- and treated the burst as over when
    the last command returned; the laptop reported each part as it
    completed; the report for part 8 arrived mid-burst and resolved the
    window's wait at once (`report_wait_s` 0.0), so parts 9-12, absent from
    it because they had not landed, were re-burst immediately behind the
    round-0 frames still in the radio; the laptop's reports for parts 9
    and 10 went out while the desktop's radio was transmitting that queue
    and were never heard; every one of the page's four on-air losses sat
    within 2 s of a laptop report. 33 round-0 fragments, 18 re-sent, 14 of
    them unnecessary. The radio log agreed from the other side:
    `since_own_tx_s` was measured from the last send command and read 9 s
    of "idle" while ten queued fragments were on air.

    2a. **Radio-busy accounting** (`_radio_busy_until`, `_note_radio_
    keyed`, `_radio_busy_remaining_s`; new key `direct_raw_burst_queue_
    ahead`, 1). `_pre_transmit_gate` -- the one point every keying path
    passes -- extends a per-interface busy-until by the frame's estimated
    airtime from the later of now and the previous value. The raw window
    reads it: the burst ends at `max(now, busy_until)` when the last
    fragment is queued, and that is what `_expect_report` registers and
    what `_await_completion_report` measures its window from (a report
    that lands before the estimated end trains nothing -- it measures the
    estimate, not the report path); `since_own_tx_s` in the radio log is
    now measured from the end of this node's own last frame and reads
    negative while a queued burst is still on air. And the zero-hop burst
    paces itself (`_raw_burst_next_send_wait_s`, pure): the next fragment
    is handed over when the radio is estimated to have at most
    `direct_raw_burst_queue_ahead` frames of air ahead of it -- one on
    air, one queued, the air back to back -- so the loop's clock is the
    radio's, a handshake yield between fragments actually reaches the
    air (it used to queue behind the whole window), and the companion's
    16-entry packet pool (`StaticPoolPacketManager(16)`, shared with
    reception) is never asked to hold a window; through repeaters the
    hop-scaled gap already exceeds the airtime, so nothing changes there.
    0 restores the old loop. The unit fake's SELF_INFO radio block moved
    from SF10/BW250 to SF8/BW250/CR5 so the interface's airtime estimate
    (0.27 s per 172-byte frame) agrees with the fake air model (0.22 s)
    instead of pricing it at 0.83 s: with pacing and a busy-until anchored
    wait, a 4x mismatch would have slowed every unit scenario. Tests:
    `tests/test_radio_busy_until_0921.py` (the accumulation, the pure
    pacing rule, the gate's stamp, the negative `since_own_tx_s`, and the
    headline: a paced zero-hop burst whose QUERY fallback leaves two
    airtimes plus the window after the last command, with `_expect_report`
    registered at the busy-until); shipped-default pin and golden config
    re-pinned for the new key.

    2b. **The receiver holds reports while a window is still arriving**
    (`direct_report_hold_during_burst`, yes; `_schedule_sender_report` /
    `_rearm_sender_report` / `_cancel_sender_report`, `_pending_sender_
    reports`; `_report_hold_s(..., arriving=True)`; `_ReassemblyBucket.
    flagged_seen`). A part completed by an UNFLAGGED fragment (not one of
    the burst's last two) is no longer reported at once: one complete
    report for the sender is held until its fragments stop arriving for
    the sender's start-to-start spacing at this hop count plus half an
    airtime (`_report_hold_s` generalised: airtime + `direct_raw_zero_hop_
    gap` + 0.5 airtime at zero hop, ~1.5 s at SF7/BW62.5; the hop-scaled
    gap + 0.5 airtime through repeaters), re-armed by every further
    fragment from that sender. A flagged fragment reports as before -- at
    once when it completes, after the M1 debounce when it leaves gaps --
    and a bucket that has already seen a flagged frame (the flagged
    parity that arrived first, the flagged fragment of a re-drive)
    reports its completion at once too, since the burst's tail is
    provably here. Every report lists the sender's recent packets, so
    whichever report goes out supersedes the held one (`_send_completion_
    report` cancels it), and a four-part window arriving back to back
    produces exactly one report, on its flagged last fragment. A lone
    single-part burst is unchanged (its last two fragments are flagged).
    The receiver's reports for parts 8, 9 and 10 of the field window --
    the one that ended the sender's wait early and the two the sender's
    own queue drowned -- are the reports this removes. Tests: `tests/
    test_report_hold_during_burst_0921.py` (the two hold arithmetics, one
    report per window, the re-armed silence hold, the flagged-tail-seen
    rule, the knob); `test_completion_reports_even_without_the_flag`
    re-pinned to the held report. Shipped-default pin and golden config
    re-pinned for the new key.

    2c. **An early report is progress, not the end of the wait**
    (`_await_completion_report(..., burst_end=, on_early=)`, `_frame_
    entries`). A report that arrives before the burst has ended on air --
    the waiter future already resolved when the wait starts, or a report
    landing while `time.monotonic() < burst_end` -- is applied to the
    parts it names (`_apply_window_entries`, so a completed part's future
    resolves and RNS moves on) and the wait continues to burst_end plus
    the report window for the receiver's word on the rest; only when that
    expires is the last early report acted on (captured as `reported_
    stale`, the outcome the pre-2c caller wrote for a kept mid-burst
    report, now with `early_reports` and the report's `entries`), so
    parts absent from any report are re-burst only after the wait has
    actually expired. An early report that leaves nothing missing ends
    the wait at once. The caller's own stale-report pre-handling is
    folded into this (one place decides what a report means for the
    wait). Tests: `tests/test_early_report_is_progress_0921.py` -- the
    08:38 sequence (the part-8 report mid-burst, the receiver's window
    report after the burst end) re-sends nothing: one round, no QUERY;
    the early report alone re-bursts parts 9-12 only after burst_end +
    window, without a QUERY, and never part 8; an early report that
    completes everything ends the wait at once. `SenderKeepsAMidBurst
    ReportAsTheFallback` re-pinned to the record's new fields (same
    outcome, same re-drive, still no QUERY). Target from the field for
    2a-2c together: re-sent fragments per part at zero hop from 0.55
    towards 0.1, the page's duty-cycle share to nearly all of its
    remaining time. MeshBench gate in `changelog.md`.

 7. **Capture hygiene** (`packet_capture_label`, new, empty; `_capture_
    filename`, pure). The capture file is `<label>_capture_<interface>_
    <stamp>.jsonl`, the label the MeshCore node name from SELF_INFO
    (`afipc`, `a`) unless the key sets one; empty and nameless keeps the
    old name. The 2026-09-21 session's desktop file had to be renamed by
    hand to be told from the laptop's. The three readers accept both
    forms (`meshbench_report.capture_files` -- its node key stays the
    interface name the scenario runner uses; `field_ab_compare.node_of`
    -- the node is the label; `simmesh.harness.read_capture`).
    `since_own_tx_s` reads the radio's busy-until since 2a. `fieldtests/
    AB_PROTOCOL.md` now asks for the label on the laptop's files and for
    MeshChat's RNS at loglevel 6, so link-validation lines exist next to
    the capture (two link requests to the desktop's LXMF destination went
    unanswered in this session with nothing to say why). Tests:
    `tests/test_capture_label_0921.py`; shipped-default pin and golden
    config re-pinned for the new key.

 5. **Adaptive window collect** (`_window_collect_continue`, `_observed_
    part_spacing_s`, `_note_raw_part_arrival`, `RAW_WINDOW_COLLECT_FLOOR_S`
    0.04; `direct_raw_window_collect` keeps its 0.75 s as the MAXIMUM).
    M2's collect was a fixed wait every raw send paid, a lone packet
    included -- ~0.7 s of every zero-hop probe's round trip. RNS's
    Resource sender emits a window's parts in one loop (`RNS/Resource.py`
    `request`) and the outgoing worker hands them over within a few loop
    turns, so the window now keeps collecting only while the outgoing
    queue still holds packets or a part joined within the transfer's
    observed inter-part spacing (twice the median of the recent gaps that
    fell inside the maximum, per peer, floored at 40 ms), and closes as
    soon as neither holds. A lone part starts within the floor; parts
    arriving together still batch. Captured as `raw_window_collect`
    (parts, collect_s, spacing_s, max_s). Tests: `tests/test_adaptive_
    window_collect_0921.py` (the two pure rules, the maximum, a lone part
    within 50 ms, four parts one window).

 6. **A raw window yields to a pending completion REPORT between its
    parts** (`_PriorityAsyncLock.acquire(report=True)`, `report_
    requested`, `yield_to_preempt(resume_priority)`, `REPORT_YIELDED_
    PRIORITY` 1.5). Phase 4 measured the cost under both-ways zero-hop
    load: a node's REPORT for the far sender's window waited behind its
    own outgoing window for the whole burst (12-15 s, pinned at 20 s),
    while the far sender's report wait expired and it re-queried. A
    completion report now queues for the radio lock as its own class
    (`_send_direct_noack_frame`, kind `completion_report`; a QUERY ANSWER
    does not), and the burst loop yields to it between two PARTS of the
    window -- never inside a part's burst, and not at the other idle
    points a Link handshake pre-empts -- the way handshakes pre-empt
    (phase 1.4), resuming behind the report's ANSWER tier and ahead of
    every ordinary waiter, with the mid-send path-reset check after the
    yield as for a handshake. Captured as `report_yields` on
    `raw_fragment_sent`. Tests: `tests/test_report_yield_between_parts_
    0921.py` (the lock's ordering with a report and with a handshake and
    a report, the no-ACK frame's class by kind, a report queued during
    part one out before part two, a report queued inside part two waits
    for that part).

 3. **Shorter-path adoption from a peer's own floods** (`path_adopt_
    enabled` yes, `path_adopt_window` 600 s, `PATH_ADOPT_MISS_LIMIT` 2;
    `_reverse_flood_path`, `_attribute_flood_to_peer`, `_note_flood_route`,
    `_shortest_flood_route`, `_maybe_adopt_shorter_path`, `_note_adopted_
    path_result`; no wire change). The field, from 11:05: the desktop's
    discovery returned a four-hop path (19 76 be d6) to the laptop while
    the laptop reached the desktop in two (d6 19); three stale-path
    resets rediscovered the same four hops; 35 minutes of proofs at 17 s
    ACK timeouts and 50% success; and the desktop's radio log had the
    laptop's floods arriving over the two-hop route the whole time
    (`rx_log` FLOOD REQ from 34, `path` d619, 17 copies). Firmware
    ground truth (`Mesh::routeRecvPacket`, `sendDirect`, `createPath
    Return`): each relaying repeater appends its hash at the END of a
    flood's path, a DIRECT frame consumes `path[0]` first, and the
    firmware never reverses a path -- so the reverse of a received flood
    path, same hash size, is a valid out_path to the originator (links
    assumed symmetric, which is what the field asymmetry violates from
    the other side). The rx-log tap now records, per bound peer, the
    routes its floods took (an ADVERT by its full `adv_key`; a REQ /
    RESPONSE / TEXT_MSG / PATH flood only when addressed to us and its
    1-byte source hash matches exactly one bound peer and no other device
    contact -- the rx-log window's own warning about promoting 1-byte
    hashes to routing decisions is honoured with that stronger check).
    In `_send_direct_packet`, the one resolved-vs-discover decision, a
    route within the window at least one hop shorter than the resolved
    path is adopted: set on the device contact with `change_contact_
    path` (the library call discovery persists with, hash mode carried
    from the flood), made the resolved path, RTT invalidated, captured
    as `path_adopted` (old and new lengths, source). Not while a raw
    window to the peer is in flight (its fragments are source-routed on
    the old path). The adopted path is provisional: `record_direct_send_
    result` confirms it on the first success (`path_adoption_confirmed`)
    or, after two consecutive full-timeout send failures with no success,
    drops it -- resolved path forgotten so the next send runs discovery
    exactly as before, the route on cooldown for the window (`path_
    adoption_failed`) -- without those failures counting towards the
    ordinary stale-path detector, whose min-age and healthy-patience
    guards would otherwise protect a fresh path far longer. Unit fake:
    the ADVERT rx-log record now carries `adv_key` as the library's does.
    MeshBench: new scenario `shortcut_appears` (`three_hop`'s chain,
    probes from three hops, then B moved to +8 km E where R1-B is +11 dB
    clear and A-B, R3-B blocked; hard check: A's capture shows a
    `path_adopted` shorter than the old path and no `path_adoption_
    failed`; the post-move `resolved` per probe is reported). Tests:
    `tests/test_shorter_path_adoption_0921.py`; shipped-default pin and
    golden config re-pinned for the two keys.

 4. **The one-hop fragment gap: the field A/B made possible** (`direct_
    raw_gap_own_airtime`, yes/no, default yes -- DEFAULT UNCHANGED; `gap_s`
    on every `raw_fragment_sent` record; `field_ab_compare.py` rows). The
    gap through repeaters is `(1 + factor x hops) x airtime` since MeshBench
    finding 2; at one hop it is two thirds of a three-fragment part's time.
    MeshBench cannot judge it (its frames are ~30% slower than the field's,
    so its one-hop loss alternates at any gap, and it has no listen-before-
    talk -- the mechanism that would let a real sender drop the `+1`,
    since the repeater's relay is audible to it); the field can. `no`
    drops the `+1 x airtime` term through repeaters, zero hop untouched.
    The receiver's holds (`_report_hold_s`, `_noack_frame_hold_s`) follow
    the sender's gap rule, as they always did. `field_ab_compare.py` now
    prints, per hop, the A/B's safety signals: the gap actually used,
    round-1 data fragments per part, round-0 re-sends per fragment
    position (the sender's view of loss), parity fragments sent and
    reconstructed (the receiver's view, when both captures are in the
    set). On the alpha 0.1.4 captures: zero hop 0.47 round-1 fragments per
    part with fragment 2 re-sent in 47% of its parts (the item-2 collision
    seen from this side), one hop 0.09 with parity repairing 2 of 15, two
    hops 0.56 with 6 of 9. Tests: `tests/test_raw_gap_own_airtime_0921.py`;
    shipped-default pin and golden config re-pinned for the new key.
    MeshBench: none, deliberately.

 8. **Airtime estimator calibration, instrumentation only** (`radio_stats_
    interval`, 300 s, 0 = start and stop only; `_poll_radio_stats`,
    `_radio_stats_record`, `_radio_stats_loop`; `_estimated_tx_air_total_s`
    / `_frames_keyed_total` accumulated in `_note_radio_keyed`). The
    statistic EXISTS: firmware v1.17.1's `CMD_GET_STATS` (56, companion
    protocol v8+, `examples/companion_radio/MyMesh.cpp`) with STATS_TYPE_
    RADIO returns `tx_air_secs` = `Dispatcher::getTotalAirTime() / 1000`
    -- the wall-clock duration of every completed send, summed in
    `Dispatcher::checkSend` (`total_air_time += millis - outbound_start`),
    reported in whole seconds -- with `rx_air_secs`, the noise floor and
    the last RSSI / SNR; STATS_TYPE_PACKETS returns the radio driver's
    sent / received counts and the flood / direct tx / rx counts. The
    `meshcore` library (2.3.9.1) exposes them as `get_stats_radio()` /
    `get_stats_packets()` (`commands/device.py`, parsed in `reader.py`).
    The interface reads both at start, at stop (best effort, in the
    teardown) and on the cadence into a `radio_stats` capture record that
    also carries its own summed airtime estimate and frame count since
    start; a library without the commands or a firmware answering ERROR
    is logged once and never asked again. `field_ab_compare.py` prints,
    per node, estimate / firmware transmit seconds over the session
    (first to last record) -- the calibration figure. The estimator is
    NOT changed: the ratio is what decides whether it should be, and the
    field has not produced one yet. The unit fake gained the two commands
    (measured from its own air model) and the counters behind them.
    Tests: `tests/test_radio_stats_0921.py`; shipped-default pin and
    golden config re-pinned for the new key.

    2b, second cut (same day, from the item-2 MeshBench gate). The first
    cut's "still arriving" silence was ONE sender spacing plus half an
    airtime (3.19 s at one hop against a 2.74 s spacing). MeshBench
    `page_transfer` (two runs, build 40e2ef9): the hold fired 8 times, all
    in re-drive rounds, 7 of them while the sender still had 1-7 frames of
    its burst to send -- the fragment after the completing one had been
    lost at the repeater, so the receiver's silence ran past one spacing
    mid-burst -- and 0 of the 8 reports reached the sender (MeshBench's
    events: half-duplex or collision at the repeater), while the M1 gaps
    reports of the same runs reached it 13 of 15 times. Exactly the
    collision 2b exists to remove, recreated by a single loss. The hold
    now spans TWO spacings plus the margin (`RAW_ARRIVING_HOLD_SPACINGS`
    2.0: ~2.6 s at zero hop, ~5.9 s at one hop at SF7/BW62.5), so one lost
    fragment does not end it; the flagged tail still reports at once. The
    same gate's other readings: `zero_hop` 7/8 and 8/8 with reports per
    window 1.00 and no re-sends (single-fragment probes -- the zero-hop
    pacing and the early-report path cannot show there; both are pinned by
    the unit tests instead); `large_payload` 4/6 and 3/6 inside the
    parity-on reference; `relay` 7/8 and 6/8 with RTT medians 12.8 / 14.2 s,
    the same as the item-1 build's 13.8 / 14.4 s on the same day (above the
    parity-off baseline, inside the parity-on reference); `page_transfer`
    1/3 and 0/3 inside the baseline's 2/3, 1/3, 0/3, with fewer round-1
    fragments per part (1.61 / 1.94 vs 2.03-2.83) and fewer reports per
    window (1.69 / 1.64 vs 2.27-2.88) but more QUERY timeouts (21 / 23 vs
    7-12), the 8 lost held reports being part of that. Negative
    `since_own_tx_s` appears in every multi-frame scenario (2a in use).

    Item 6, second cut (same day, from its MeshBench gate). `page_transfer_
    bidir` on the first cut (build 5993ec8): `report_yields` was 0 on every
    fragment. RNS had shrunk the Resource window to ONE part on the lossy
    one-hop link (every one of A's 14 windows had one part), so "between
    two parts" never occurred, while A's own reports waited a median 3 s
    and up to 13 s for the lock -- behind A's report WAIT, the radio-free
    idle phase a Link handshake already pre-empts (phase 1.4b). A queued
    report now releases that wait too (`_wait_future_or_preempt(...,
    also_reports=True)`, `_PriorityAsyncLock.report_event`): the lock is
    released, the report goes out, the wait keeps listening radio-free,
    exactly the handshake path. The between-parts yield stays for the
    windows RNS does hand over whole. Pinned in `tests/test_report_yield_
    between_parts_0921.py`.

    Item 3, second cut (same day, from `shortcut_appears`'s first run,
    build 1fd4e00). A's capture had B's one-hop floods (PATH and REQ from
    81 to 1c, path 6a) from 627 s -- but A's own sends over the dead
    three-hop path had already failed three times and its stale-path reset
    had forgotten the path at ~615 s, so `_maybe_adopt_shorter_path(None)`
    stood aside and the send went to discovery, which under its backoff
    resolved the one-hop path only at 843 s (`FAIL sender adopted a shorter
    path ... []`; probes 6-10 unresolved). "Instead of running discovery"
    has to cover that case: with no resolved path and a recent flood
    route, the route is adopted (provisional, the same two-miss fallback)
    and discovery is skipped; `path_adopted` then carries `old_path_len`
    None. The scenario's check accepts that form.
    Third cut, from the second run: A adopted a two-hop route (8be3) seen
    571 s earlier, before B moved, missed twice and dropped it (the
    fallback worked, at ~60 s), then discovery found the one-hop path. In
    the no-path case only routes seen since the peer's last reset or drop
    count (`_path_reset_at`): evidence older than the failure describes
    the topology that just failed. With a path still resolved, older
    evidence still counts -- the field's two-hop floods (11:02) were three
    minutes older than the four-hop path (11:05) and never refreshed in
    the 35 minutes after, which is the case the item exists for.
    Fourth cut, from the re-run on the third cut (build 4627e00, two runs):
    run 1 PASSED the hard check (adopted B's one-hop route 18 s after its
    addressed flood, where no path was resolved, discovery skipped,
    confirmed by the next delivery; probes 9-10 at one hop); run 2 saw
    B's floods at 586, 590, 615 and 765 s but adopted nothing, because
    A never learned an RNS token for B in that run (no PROOF ever came
    back) and every probe went DIRECT-to-all through `_send_direct_
    supplement`, which decides resolved-versus-discover on its own and
    never reaches `_send_direct_packet`. The same `_maybe_adopt_shorter_
    path` now runs at that second decision point too -- still one
    adoption function, called from the two places a send already decides
    resolved-versus-discover. The alpha 0.1.5 baseline's three
    `shortcut_appears` runs are on this cut.
