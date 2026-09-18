# Changelog

## unreleased (since alpha-0.1.1, 2026-09-18 night)

Field evidence: `fieldtests/raw/Alpha0.1.1/` -- a zero-hop NomadNet page
session and an evening drive through 1-3 repeater hops, both sides
captured. The module docstring's "Alpha 0.1.1 captures" and "Raw binary
DIRECT fragments" entries carry the packet-level detail.

### Fixed: bare-DIRECT receive dedup stalled a Resource transfer

RNS's `Transport.packet_filter` exempts KEEPALIVE, RESOURCE, RESOURCE_REQ,
RESOURCE_PRF, CACHE_REQUEST and CHANNEL from its own duplicate filter
because it re-delivers byte-identical packets for them on purpose: a
Resource part arriving one slot ahead of the receiver's window is
discarded and re-requested, and the sender answers with the same bytes.
The interface's bare-DIRECT dedup (150s, keyed on payload bytes) dropped
every copy after the first -- 16 re-sends of one 35-byte part over two
minutes in the page capture, until the transfer was cancelled. The dedup
now consults the packet's RNS context and lets exactly RNS's own exempt
set through (`_RNS_NO_DEDUP_CONTEXTS`).

### Fixed: fragmented sends gave up one fragment short; re-sends restarted from zero

Two PATH_RESPONSE announces at 2 hops each delivered 2 of 3 fragments,
the reconcile confirmed it, and the last fragment then exhausted the
ordinary budget of 2 -- 3.5 minutes each, and the laptop never got a path.
Now: once the receiver provably holds part of a packet, pass 1 uses
`direct_fragment_finish_attempts` (4); the reconcile answer is applied
authoritatively in both directions; and a failed fragmented send is
remembered per (peer, payload hash) so that RNS re-issuing the identical
bytes resumes the receiver's still-open bucket under the same pkt_id,
sends only the gaps, and always reconciles (`direct_fragment_resume_
enabled`, never for handshake priority). Tests: `tests/test_alpha011_
fixes.py`.

### Added: raw binary DIRECT fragments (off by default)

`direct_raw_fragments_enabled`. A packet too large for one text frame
goes to a peer that advertised `BIND_CAP_RAW_FRAGMENTS` as MeshCore raw
packets (`CMD_SEND_RAW_DATA` / `EventType.RAW_DATA`): a 13-byte header
(`[ver|attempt][dst_prefix:2][src_prefix:6][pkt_id:2][frag_idx][frag_
total]`) and up to 157 bytes of RNS payload per fragment at zero hop
(firmware limits: 173 received, 174 minus path sent), no Z85, no text
framing, no per-fragment ACK. A 483-byte Resource part is 4 raw fragments
instead of 5 text ones plus 5 ACKs, about 43% less sender airtime and no
ACK idle. Reliability is the existing have-bitmap reconcile: burst the
missing fragments under the radio lock, ask, repeat (`direct_raw_
reconcile_rounds`, `direct_raw_query_attempts`); resume works unchanged;
if two answered reconciles show a burst delivered nothing, raw is
disabled for that peer for `direct_raw_fallback_cooldown` and the packet
is re-sent as text. Raw frames carry an unauthenticated src prefix, so
nothing is learned from them. The simulator gained the RAW_CUSTOM packet
type with the firmware's seen-dedup; tests in `tests/test_raw_fragments.
py`; `zero_hop_peer_discovery_test.py --raw-fragments`. Not yet run on
hardware; needs both radios on this build and one pass through a public
repeater.

### Fixed: two pre-existing robustness issues surfaced by the test suite

An interface that was never `detach()`ed pinned the process at exit (the
outgoing worker's unbounded `queue.get()` in a non-daemon executor
thread); the worker now waits in one-second slices. And proactive path
discovery on bind could stay denied until real traffic flowed, because it
raced the peer's telemetry grant and then sat in backoff; one retry after
the bind-response window (`_discover_path_after_bind`) settles it. Both
reproduced on the committed alpha-0.1.1 tree, so neither is a regression
from the changes above. The scenario tests also stop their mesh when
setup fails and wait for the sender's attempt record instead of racing
it.

## alpha-0.1.1 (2026-09-18)

Merge of `development` into `main`. This release is everything after the
alpha 0.1.0 M0-M6 build: a code-review pass at the start of the cycle, a
second one at the end, and between them a run of field-driven work whose
common theme is moving this interface off fixed, guessed timings and onto
measurements taken from the radio itself.

The headline changes: the companion firmware's raw-RX log is now tapped
and used (measured per-peer ACK RTT drives the ACK timeout, a missing
repeater echo aborts a dead first hop early, and an optional medium-busy
model can hold transmits); DIRECT-fragmented sends send once and then
reconcile against what the receiver says it actually holds, instead of
blindly re-sending; duty-cycle accounting uses the real LoRa time-on-air
formula at the radio's own SF/BW/CR instead of a bitrate guess; the
outgoing queue drops duplicate and stale work rather than draining it as
a late burst; and there is now an automated test suite plus a simulated
mesh, so a change can be checked against multi-hop DIRECT behaviour
before it goes near a real repeater.

Field evidence for this release lives in `fieldtests/raw/postAlpha0.1.0/`
(a 2026-09-18 evening drive across 3, 2 and 1 hops down to zero hop, and
a zero-hop NomadNet page load captured from both ends).

### Added: RX-log awareness -- "lessen our reliance on arbitrary wait times"

The largest single line of work in this release, built in four steps
against a standing rule of field evidence before timing changes. The
starting observation, from the 2026-09-16 1-hop capture: 43% of DIRECT
attempts got no ACK and the mean `_direct_exchange_lock` wait was ~11s
(max 49s), almost all of it queueing behind *other* sends' full ACK
timeouts rather than behind the deliberate listen windows (~1.2s per
attempt). So the lever is fewer collisions and faster failure detection,
not shorter sleeps -- and every arbitrary sleep in this file exists to
cover the same blind spot: between keying the radio and the ACK event,
this interface knew nothing about what was on air.

It turns out it can. The companion firmware's `MyMesh::logRxRaw` pushes
every packet the radio decodes to the host, unconditionally, with no pref
gating it, and the installed `meshcore` library parses it into
`EventType.RX_LOG_DATA` with SNR/RSSI, route type, payload type, path
length and path hashes -- including traffic not addressed to this node:
other peers' DIRECT frames, flood repeats, ACKs in transit, and a
repeater's echo of this node's own frame. All zero airtime, all
previously thrown away.

1. **Observe only (`rx_log_observe_enabled`, default on).** No routing or
   timing behaviour changed in this step. Every overheard packet is
   counted onto the `[STATS]` line (`rx_log_feed=seen|never|off` tells a
   silent capture apart from a firmware that doesn't push the feed) and,
   with packet capture on, written as one `rx_log` record: SNR/RSSI,
   route/payload type, path, the 1-byte dest/src routing hashes, an ACK's
   4-byte code (matchable against the `expected_ack` this node did or
   didn't get), the library's `pkt_hash`, and two relative timings
   (`since_last_rx_log_s`, `since_own_tx_s`). Subscribed only if the
   installed library exposes the event -- an older library loses this
   observability, not the interface. `testscripts/rx_log_monitor.py`
   prints the feed live, transmits nothing, and needs no RNS, so a radio
   can be checked before anything relies on it.
2. **Measured ACK RTT drives the ACK timeout**
   (`direct_ack_rtt_adaptive_enabled`, default on). Every real ACK folds
   its MSG_SENT -> ACK latency into a per-peer Jacobson/Karels estimator;
   once 3 samples exist the firmware-derived timeout is replaced by
   `multiplier * (srtt + 4*rttvar)`, floored at 3.0s and *never* above
   the firmware value it replaces -- so the worst case is exactly the old
   behaviour. Karn-style invalidation on the first miss governed by the
   measured value, and on every path change (an RTT over one path says
   nothing about another). Chosen as the first timing change precisely
   because it can only ever shorten a wait: a missed ACK holds the shared
   DIRECT lock for the full timeout, which is where the wasted silence
   was going. Measured zero-hop, the timeout stepped 5.80s (firmware) ->
   4.38 -> 3.80 -> 3.37s against a near-deterministic 1.03s RTT.
   Per-attempt capture records gained `ack_latency_s`,
   `send_cmd_latency_s`, `ack_timeout_source`, the live RTT stats, and an
   RX-log correlation window (`rx_echo_seen_s`, `rx_ack_seen_on_air_s`,
   `rx_path_reply_seen_s`, `rx_foreign`) covering exactly the period the
   attempt held the lock.
3. **Have-bitmap completion answers and send-once-then-reconcile**
   (`direct_fragment_reconcile_enabled`, default on) -- the one change
   here that reduces transmissions rather than only reshaping waits. The
   `"Q"` completion frame is now v2: an answer appends a have-bitmap, so
   the receiver reports *which* fragments it holds (from the dedup cache
   and from any still-open reassembly bucket), not just complete/not. A
   fragmented send now transmits every fragment once, unrecorded against
   the stale-path failure count, then sends ONE reconcile query if
   anything lacks an ACK; fragments the receiver confirms are marked
   delivered (and recorded as a success -- the data provably crossed the
   path), and only the rest are re-driven. No answer means re-drive
   everything, exactly as before. v1 frames still decode, a v1 query is
   answered in v1, and a peer too old to parse v2 simply doesn't answer,
   which is the pre-existing behaviour. The arithmetic: one query+answer
   is two ~10-50-char frames, each blind retry is a full ~160-char
   fragment plus its ACK -- a net saving whenever at least one "missing"
   fragment was actually held, and the radio lock is released sooner in
   every case. Link handshakes keep their own larger first-pass budget
   and no reconcile: a lost handshake forces path rediscovery and the
   extra round trip would only delay it.
4. **RX-log-derived transmit holds (`rx_log_holds_enabled`, default
   NO).** `_estimate_airtime_s` implements the real LoRa time-on-air
   formula at the radio's own SF/BW/CR (read from SELF_INFO, with the
   firmware's preamble rule and LDRO), replacing "seconds measured at
   SF7" with something that scales -- a 102-byte frame is 0.58s at
   SF7/BW62.5/CR8 and 4.4s at SF12/BW125. From that, every overheard
   packet extends a predicted medium-busy window (a flood will be
   re-flooded by every repeater; a routed DIRECT with N hops left has N
   forwards and an ACK turnaround to come; an ACK on a direct route has
   nothing following it). The model is *always* maintained and recorded
   (`predicted_hold_s`, `hold_reason`, `medium_busy_remaining_s` on every
   `rx_log` record) even with holds off, so a capture shows what they
   would have done -- which is how the decision to enable them gets made.
   With the flag on, transmits wait out the prediction (capped by
   `rx_log_hold_max_s`) and the post-miss listen window is chosen from a
   diagnosis (`target_busy` / `hop1_loss` / `downstream_loss` /
   `no_info`) rather than a flat random range. The diagnosis is captured
   either way. Left off by default: the multi-hop capture that would
   justify flipping it showed small predicted holds, which fits link loss
   rather than contention.

### Added: DIRECT-fragmented delivery completion check (phantom-ACK fix)

Field-data-driven addition, following a full review of a real 5-node field
test's packet captures focused on turn-taking/collision behavior and
reliability (not raw throughput). Cross-referencing the sender's own
ACK bookkeeping against the receiver's capture found a concrete, provable
case: a 3-fragment DIRECT message (`pkt_id=3`, router -> client) where 2 of
3 fragments were logged as "never acknowledged" after both retry passes
(4+ minutes, 8 fragment-send attempts total) -- yet the receiver had
already fully reassembled all 3 fragments about a second *before* the
sender's own final successful ACK for the third fragment even landed. That
proves the first two fragments physically arrived; only their firmware
ACKs failed to make it back on the return path. This design previously had
no way to tell that apart from genuine non-delivery, so it kept blindly
retrying data the receiver already had -- burning airtime and
`_direct_exchange_lock` time other queued sends were waiting on, and
risking a false `direct_path_reset_threshold` trip over a link that was
actually fine.

Fix: a new lightweight `"Q"`-marker control frame (distinct from `"R"`
RNS-payload frames and `"P"` bind frames), DIRECT-only since it requires
already knowing the peer's authenticated identity, sent as a last resort
once both retry passes are exhausted and fragments still appear missing.
The receiver answers from its own existing whole-packet dedup cache -- no
new receive-side state. If the receiver answers "complete," the sender
treats the message as delivered and clears its recorded failure count for
that peer, undoing the false-failure signal recorded per-fragment during
the retry passes and avoiding an unwarranted stale-path reset. Fails safe:
a peer that doesn't understand `"Q"` frames, or whose answer is itself
lost, simply never answers and
`direct_completion_check_timeout_s` (default 5.0s) elapses into exactly
the old give-up behavior. New config: `direct_completion_check_enabled`,
`direct_completion_check_timeout`. This frame is what step 3 above later
extended into the v2 have-bitmap reconcile that now runs on the main path
of every fragmented send. Deliberately scoped to DIRECT-fragmented sends:
bare single-message DIRECT sends have no `pkt_id` and dedup on full
payload bytes instead, which doesn't fit this query shape -- left as a
known, smaller-impact gap.

### Fixed: DIRECT-supplement target selection ignored recent failure history

`_select_direct_supplement_targets` (path-request DIRECT supplement) and
`_select_bootstrap_supplement_targets` (unknown-destination DIRECT
bootstrap supplement) both picked their capped target list by recency
alone -- most-recently-confirmed/-seen first -- with no reference to
`_direct_path_failures`. A peer that had just failed a DIRECT attempt, but
hadn't yet crossed `direct_path_reset_threshold` (so was still fully
"resolved" and eligible), could still win a scarce supplement slot purely
on recency, ahead of an equally-recent peer this interface had no reason
to doubt -- spending part of a capped, airtime-costing fan-out on a send
statistically less likely to succeed.

Fix: both now sort primarily by each candidate's own failure count (fewest
first), falling back to the original recency ordering only as a tiebreaker
among equally-healthy peers. Not a hard exclusion -- a struggling peer
still gets picked once it's the least-bad option available, and the count
clears on a fresh success or drops the peer from candidacy entirely once a
stale-path reset fires. Verified with dedicated unit tests and a re-run of
the existing CHANNEL-path fake-hardware smoke test showing no regression.

### Fixed: incoming-quiet-defer caused a mutual reset feedback loop

The 2026-09-16 incoming-quiet-defer feature collapsed multi-hop DIRECT
delivery to 0/8 messages completed in a field test, after DIRECT timing
knobs had been tweaked on both machines. Root cause:
`_last_incoming_direct_at` was updated for *every* DIRECT frame heard --
ACKs, PROOFs, completion checks, a fragment that completed its own bucket
-- not just "a fragment with more of this transfer still coming," which
was the feature's actual intent. On a link where both nodes constantly
exchange that other traffic, a genuine 3s lull rarely occurred, so nearly
every send got pushed toward the 15s patience ceiling. Confirmed against
that night's captures: fragment gaps widening from ~20s to 60-90s within
one run, then a 16-minute window with 0/58 outgoing DIRECT attempts
succeeding.

Two fixes, for the bug and for the pattern behind it (three field-driven
additions in a row had each stacked a new serialized delay onto the same
DIRECT send path without checking it against what was already there):

1. **Narrowed trigger plus a retry exemption.** The timestamp is now set
   only when a received fragment leaves its bucket still incomplete --
   concrete evidence more fragments are coming, not "the channel was
   occupied by something." Separately, a re-drive (a fragment already
   known missing, racing the receiver's fixed reassembly deadline) skips
   the quiet-defer courtesy wait entirely; a fresh send still pays it.
   Duty-cycle throttling is untouched by either fix -- it is this node's
   real airtime cap, not a heuristic, and a retry storm is exactly what it
   exists to bound.
2. **`_validate_direct_timing_budget`, run once at startup.** The
   incident's actual trigger was tuning DIRECT knobs spread across five
   `_configure_*` methods without checking they were still coherent
   against `reassembly_idle_timeout_s` -- the fixed clock on the other end
   of the same budget. Startup now computes the worst-case wall-clock cost
   of one fragment surviving both retry passes and logs a warning if the
   reassembly timeout is lower than that. It never silently overrides an
   operator's config, and it is a floor on the real number rather than an
   exact prediction (lock contention can't be bounded from config alone),
   but a config that fails this check is confirmed too tight.

### Fixed: first real multi-hop capture (evening drive) -- four fixes

A 3-hop path that degraded to a dead first hop and then collapsed to zero
hop as the car arrived home. Diagnosed from the capture and re-verified
against the same JSONL before anything was changed:

1. **Reconcile query timeout fell to its 5s floor at 3 hops, where the
   query's own ACK alone takes ~4.5s.** Both reconcile queries in the
   capture timed out at exactly 5.0s with no RTT samples -- because the
   miss that triggers a reconcile is, by construction, a miss under the
   measured timeout, and Karn invalidation had just discarded the stats
   the query sizing needed. Karn is right not to *trust* that estimate for
   the next ACK wait; it is still the best information for sizing a
   two-frame exchange. Invalidated stats are now kept in a snapshot for
   this purpose (missed-ACK case only -- a path change still drops
   everything), and with no snapshot the query falls back to twice the
   peer's last firmware hop-aware bound, capped as before.
2. **Early abort on a dead first hop (`direct_hop1_abort_enabled`,
   default yes).** The outage cost was the timeout, not collisions: nine
   consecutive misses each burned the full 28s firmware timeout while the
   RX log heard nothing at all -- no repeater forward of our own frame,
   where every one of the 24 successful multi-hop attempts before the
   outage had one (echo 0.84-3.58s, median 1.73s). The stale-path reset
   needed 4 minutes to fire and the queue backed up 18 deep with 225s lock
   waits. Each peer's echo timing is now learned (last 16 samples, cleared
   on any path change) and, once 3 samples exist, an attempt waits only
   `max(5s, 2.0 x that peer's slowest observed echo)` for *either* the ACK
   or the echo; silence where a forward was due is positive evidence, so
   the attempt is given up as a recorded failure and the reset fires in
   ~30s instead of ~4 minutes. Guarded by the same capture's other
   finding: 18 successful attempts in the last minute had no echo because
   the laptop was already zero-hop while the interface still carried
   `hop_count=3` -- an ACK always wins the race against a >=5s deadline,
   so those keep succeeding. Self-disabling without an RX-log feed, never
   applied at hop 0, never longer than the ACK timeout it shortens.
3. **Outgoing path requests are coalesced per requested destination**
   (20s window, mirroring the existing PATH_RESPONSE rule). RNS emitted 14
   identical requests for one destination at 4-8s gaps -- explicit client
   retries under Transport's own automatic floor -- and each became a
   3-hop DIRECT exchange, driving queue depth to 7 on its own. Every path
   request shares one pseudo-destination hash, so the key is the
   *requested* hash read from the packet data.
4. **Queued packets expire (`outgoing_max_age`, 120s; announces exempt).**
   17 LXMF pings queued during the outage drained as a stale burst over
   85s once the path came back. Expired packets are dropped and counted,
   never recorded as a path failure. (Refined the same evening -- see
   below.)

### Fixed: zero-hop NomadNet page load -- three fixes

Both sides captured. Link setup took 14s; the page's 12 Resource parts
(60 DIRECT fragments) then took 7.6 minutes, during which the server
transmitted 110 fragments -- every one ACKed, mean ACK 1.24s. The radio
was not the problem:

1. **The duty-cycle estimate was quantizing away a third of the policy's
   own allowance.** A full 151-char fragment was estimated at 1.007s from
   a flat bitrate, so three in one 10s window came to 3.02s -- a hair over
   the 3.0s cap -- and the limiter admitted two per window, spending 304s
   of the 509s transfer waiting. The other radio's RX log shows what a
   fragment really is on air: 166 bytes with the firmware's framing, which
   at SF7/BW62.5/CR8 is 0.877s by the step-4 time-on-air model; three of
   those are 2.63s. The limiter now uses the model-derived figure whenever
   SELF_INFO has provided radio parameters, falling back to the bitrate
   estimate otherwise. The 30% policy itself is untouched -- it now admits
   the three fragments per window it always allowed for.
2. **`outgoing_max_age` was dropping fragments mid-packet.** All nine
   expiries in the capture were fragments 2-4 of 5, in the first pass, of
   parts whose earlier fragments had already been transmitted -- each
   threw away air already spent, left the receiver's bucket to time out,
   and made RNS re-request the whole part. Expiry is now decided once,
   before a packet's first transmission, and never afterwards; and
   packets carrying Resource data parts are exempt entirely, since RNS's
   Resource layer owns their retransmission and this interface
   second-guessing it can only add round trips.
3. **RNS re-requested parts that were still queued here**, so half the
   transfer was redundant: 26 Resource packets for 12 distinct payloads.
   `process_outgoing` now drops a packet whose exact bytes are already
   queued or in flight, keyed by the packet's truncated hash and released
   only when every send task that packet spawned has finished -- never on
   a timer, so a copy that genuinely failed can be re-sent the moment the
   failure is known.

### Changed: duty-cycle policy -- 60s window, and handshakes bypass the wait

The duty-cycle window default is raised from 10s to 60s at the same 30%
fraction (a user decision, superseding the original "30% of a 10 second
period"). At SF7/BW62.5 the 10s window capped a 60-fragment page at
roughly 3 minutes even with zero waste, because a burst could only ever
reach the ~26% a 10s window quantizes to; over 60s a burst can use the
full 18s of allowance before the limiter pauses it, and the "majority of
the time listening" intent still holds over every rolling minute.

Separately, link-maintenance traffic now bypasses the duty-cycle wait
(`duty_cycle_exempt_handshake`, default yes). A burst of page data can
consume the whole allowance, and a keepalive or link proof queued behind
it would wait for budget while RNS's own link timers run -- losing the
Link, which costs a full re-establishment, to protect a few hundred
milliseconds of air. The exempt class is exactly the handshake priority
tier that already jumps the DIRECT lock queue, so the two priority
mechanisms now agree. That airtime is still *recorded* against the window,
so ordinary data pays for it and the 30% ceiling stays honest; only the
wait is skipped.

### Fixed: code review pass (2026-09-18) -- coherence fixes

A second review, this one checking that the cycle's design decisions still
agreed with each other. Every finding was checked against the installed
`meshcore` library, the firmware source, or RNS core in-process:

1. **The `"Q"` completion exchange no longer breaks the shared-radio
   invariant.** The DIRECT lock's contract is "held for the full
   send+ACK-wait duration of every DIRECT exchange", yet the completion
   query released it the instant the send command returned -- while the
   query's own firmware ACK and the peer's answer were both still in
   flight, so the next queued send could key the radio straight into the
   reply this node was waiting for. Harmless when it was a last resort;
   step 3 moved it onto the main path of every fragmented send, which made
   it matter. The lock is now held from the query's transmit through the
   answer or its timeout, exactly like an ACK wait. Both frames are also
   marked time-critical, and the query inherits the send's own priority
   instead of the lowest one: a reconcile step that queues behind every
   ordinary send, while the receiver's reassembly clock counts down,
   defeats its own purpose.
2. **A missed ACK under the measured timeout no longer counts toward
   `direct_path_reset_threshold`.** Step 2 promised the worst case was
   exactly the old behaviour, but a miss under a timeout this interface
   had tightened on its own was still reported as a genuine path failure.
   The estimate is Karn-invalidated on that miss, so the next attempt runs
   on the firmware timeout -- and a miss *there* still counts.
3. **Successful Link establishments were being counted as
   unknown-destination failures.** The unknown-destination attempt counter
   fires for every link request to a destination with no known token, and
   its only clearing signal was a token learned for that exact destination
   hash -- but the reply to a link request is an LRPROOF whose destination
   field is the *link_id*, and everything after it rides that link_id too.
   Outside small-mesh mode, three perfectly good Links to the same
   destination put it into a 5-60 minute backoff that stripped the DIRECT
   bootstrap supplement from every later link request. Fix:
   `_compute_link_id` replicates RNS's own link-id derivation (validated
   in-process, byte-for-byte, against real `RNS.Packet`/`RNS.Link` for
   several payload sizes including the ECPUBSIZE truncation branch); every
   outgoing link request records `link_id -> destination_hash`, and a
   matching incoming LRPROOF learns both mappings and clears that
   destination's backoff.
4. **Housekeeping:** the path-response rate limiter's table is now swept
   like every other one; the startup timing-budget warning sums the terms
   it actually names; `asyncio.get_event_loop()` -> `get_running_loop()`
   where a coroutine already guarantees one.

Docstring drift from the "lowered most hard coded delays for testing"
commit was corrected in place rather than left to mislead the next reader:
`SMALL_MESH_DIRECT_ONLY_MAX_PEERS` 2 -> 3, `direct_path_reset_threshold`
2 -> 3, `direct_path_reset_min_age` 30 -> 60s,
`path_discovery_quick_attempts` 3 -> 2, `direct_post_send_listen` 0-5s ->
0.3-3s and its success range 0-0.5s -> 0-0.4s. The dated history entries
are left as written; the live docstrings and `readme.md` state the current
values.

### Fixed: one real deadlock (code review pass, 2026-09-16)

The first review of the cycle: eight independent angles (correctness scan,
removed-behavior audit, cross-file tracer against the installed `meshcore`
library and vendored RNS source, reuse/duplication, simplification,
efficiency, altitude, and convention compliance), with every candidate
finding manually verified against the actual source before anything was
changed.

- **`_PriorityAsyncLock.acquire()` could permanently deadlock every future
  DIRECT send.** Its `CancelledError` handler passed lock ownership to the
  next waiter via `_wake_next()` but, unlike `release()`, never checked
  that call's return value. When a waiter was granted ownership and
  cancelled in the same instant with no other waiter queued for any
  priority tier, `_wake_next()` returned `False` and the handler left
  `_locked` stuck `True` forever, with nobody holding the lock and nobody
  able to call `release()` for it. Every subsequent `acquire()` on
  `_direct_exchange_lock` (the single interface-wide lock guarding every
  DIRECT exchange) would then block forever. Only reachable via
  `detach()`'s own `task.cancel()` sweep in this interface's steady-state
  code, but a silent, total, unrecoverable deadlock of DIRECT sends if
  hit. Fixed to mirror `release()`'s own `if not self._wake_next():
  self._locked = False`.

### Fixed: silent failures and permanently-disabled safety checks

- **`_send_direct_supplement` dropped packets silently.** When a bound
  peer's contact record couldn't be resolved (or had no `public_key`) at
  the moment a DIRECT supplement fired, this method returned with no log
  line and no `_outgoing_dropped_total` increment -- the one drop path in
  this method that didn't follow the sibling convention already used
  everywhere else (including `_send_direct_packet`'s equivalent check).
  Now logs and counts like every other drop decision in the file.

- **Periodic contact refresh was a permanent no-op after the first
  fetch.** Both call sites of the installed `meshcore` library's
  `ensure_contacts()` omitted `follow=True`. The library's own
  `ensure_contacts(self, follow=False)` only re-fetches when
  `not self._contacts` or `(follow and self._contacts_dirty)` --  with
  the default `follow=False`, every call after the very first successful
  contact fetch was a no-op, even though the library already tracks
  `_contacts_dirty=True` internally on every ADVERTISEMENT/PATH_UPDATE
  event. This directly contradicted `_refresh_contacts_and_grant_
  telemetry`'s own docstring claim ("the next periodic refresh retries
  it"): a peer's contact arriving after the first fetch would never
  actually be pulled into `self._contacts` by any later refresh, silently
  blocking that peer's telemetry-permission grant and path discovery
  indefinitely. Fixed by passing `follow=True` at both call sites
  (`_refresh_contacts_and_grant_telemetry` and `discover_path`'s
  contact-resolution fallback).

- **One malformed contact could silently break telemetry refresh for
  every peer after it.** `_grant_telemetry_permission_if_needed`'s
  `flags` read and bitwise check sat outside its own `try` block. Since
  `_refresh_contacts_and_grant_telemetry` calls this once per bound
  peer/contact in a plain `for` loop with no per-iteration isolation, an
  unguarded exception there (e.g. a contact whose `flags` field is ever
  non-int) would abort the whole loop, skipping telemetry-permission
  grants for every peer ordered after the offending one, with only a
  generic "contact refresh failed" line to show for it. Moved inside the
  `try`.

- **A failed initial identity fetch could permanently disable the
  self-echo guard.** `_fetch_own_identity`'s own docstring says a failed
  `send_appstart` "will be retried on the next reconnect if the pubkey is
  still unknown by then," but `_on_mc_connected` only retries it on an
  actual DISCONNECTED-then-CONNECTED cycle. If the physical link comes up,
  the first fetch fails, and the link then simply never drops again for
  the rest of the process's life, `_own_pubkey_hex` stays empty forever --
  permanently disabling `_handle_incoming_bind_frame`'s self-echo guard,
  so this node's own bind frames bouncing back via a repeater or CHANNEL
  rebroadcast would be misprocessed as a genuine external peer for the
  rest of the session. Now also retried from the existing periodic
  `_contact_refresh_loop`, which runs regardless of connection-state
  transitions.

- **A local send exception skipped the post-send "listen quiet" window.**
  `_send_direct_frame_and_wait_for_ack`'s field-tuned post-send listen
  delay (the fix that stops this interface from re-keying the radio right
  after a possible collision) only ran on the normal
  ACKed-or-timed-out path. An exception raised by `_send_direct_frame`
  (e.g. a firmware ERROR surfaced through `_run_command`) propagated
  straight out of the `_direct_exchange_lock` block, skipping the delay
  entirely and letting the very next contender for that lock -- a retry
  of the same attempt, or an unrelated queued DIRECT exchange -- key the
  radio again with zero quiet time. Now caught, the listen delay still
  runs (drawing from the same range a missed ACK uses), and the exception
  is re-raised afterward so the caller's own logging is unchanged.

### Documented mid-cycle, then closed: LRPROOF routing-peer resolution gap

- **`_resolve_routing_peer`'s PROOF-correlation lookup didn't account
  for LRPROOF's different wire layout.** `RNS.Packet.pack()` writes a
  Link's `link_id` (not a destination hash) into the field this
  interface reads as `destination_hash` when `context ==
  RNS.Packet.LRPROOF` (confirmed against the vendored RNS source). Since
  `_proof_correlation` is keyed by truncated hashes of previously-received
  packets, not link_ids, an outgoing LRPROOF (a Link-acceptance reply)
  never correlated to a known peer through this table, even when that
  peer was otherwise DIRECT-resolved. The effect was bounded to an
  efficiency loss, not a correctness or security issue: the existing
  broadcast + capped DIRECT-bootstrap-supplement fallback still delivered
  it. The 2026-09-16 review deliberately left it flagged rather than
  guessed at, since a correct fix needed RNS's exact link-id hashing
  (including its ECPUBSIZE-based truncation) and real validation.
  **Closed later in this same release** by the 2026-09-18 review's
  `_compute_link_id` (item 3 above), validated byte-for-byte in-process
  against real `RNS.Packet`/`RNS.Link`: an incoming link request from a
  bound peer now records `link_id -> peer`, so this node's own outgoing
  LRPROOF resolves DIRECT-primary instead of falling through to
  broadcast. As a side effect the initiator's first post-handshake packet
  also goes DIRECT immediately rather than after one broadcast round.

### Cleanup: duplicated logic consolidated

- **`_fragment_payload`/`_fragment_direct_payload`** had byte-identical
  chunking bodies, differing only in which budget accessor supplied
  `per_fragment`. Extracted the shared body into `_chunk_payload(data,
  per_fragment)`; both methods are now one-line callers.
- **The `_wait_for_incoming_quiet()` + `_throttle_for_duty_cycle(frame)`
  pair** was copy-pasted verbatim at all four radio-keying call sites
  (`_send_channel_fastpath_frame`, `_send_channel_multifragment_pass`,
  `_send_direct_frame`, `_send_bind_frame`) -- both methods' own
  docstrings already claimed this but nothing enforced it structurally.
  Extracted into one `_pre_transmit_gate(frame)` helper used at all four
  sites (and any future one). This helper is also what later carried the
  `is_redrive` exemption, the RX-log holds, and the gate telemetry.
- **`_unknown_dest_attempts`/`_unknown_dest_backoff_until` had no
  periodic reclaim**, unlike `_dedup`/`_reassembly`/`_proof_correlation`
  (all three already swept every `REASSEMBLY_CLEANUP_INTERVAL_S`). A
  destination tried a few times and never addressed again (never
  resolved, never crossing the backoff threshold) sat in these dicts for
  the rest of the process's life; only a later success ever removed an
  entry. Added `_unknown_dest_last_attempt` (idle-since-last-attempt
  tracking) and `_unknown_dest_backoff_sweep`, wired into the same
  periodic pass as the other three tables.

### Added: automated tests and a simulated mesh

`python3 -m unittest discover -s tests` now runs a real suite: wire-format,
RNS-header and reliability-engine unit tests (about a second), plus
end-to-end scenarios that drive two real interface instances through
simulated repeater hops (`SMCI_SKIP_SLOW=1` skips those). The simulated
mesh in `testscripts/simmesh/` models DIRECT routing through repeaters,
ACKs, path discovery, contacts, flood dedup, half-duplex, collisions and
loss. `testscripts/fake_meshcore_repeater_sim.py` was rebuilt on top of it
and runs any topology you describe (`--link A-R --link R-B --repeater R`);
`testscripts/rns_multiprocess_sim.py` does the same with a full real
Reticulum instance per node; `testscripts/calibrate_sim_from_captures.py`
derives loss/latency settings for the simulator from real field captures.
None of this replaces field testing -- simulated timing is not real radio
timing -- but a change can now be checked against multi-hop DIRECT
behaviour before it reaches a repeater.

### Smaller fixes and observability

- **`_z85_decode` raised `TypeError` instead of `ValueError` on non-string
  input**, escaping the callers' own error handling. It now type-checks
  first and raises the documented `ValueError`.
- **The reconcile query's RTT-derived timeout is capped** at the same
  ceiling an ACK wait has. Since that wait holds the DIRECT lock and the
  estimator's initial spread makes `3*(srtt + 4*rttvar)` about nine times
  the measured RTT, one unanswered query could otherwise hold the radio
  for 20-30s at 1-2 hops.
- **New capture fields for latency attribution**: per-attempt
  `quiet_defer_wait_s` / `duty_cycle_wait_s` (so a capture shows how total
  latency split across gating, queueing and ACK-waiting, instead of
  needing separate log lines correlated by hand), `pass_number`,
  `duty_cycle_exempt`, `miss_diagnosis`, and a new
  `channel_fragment_sent` event -- the sender-side counterpart to the
  existing CHANNEL receive record, carrying each fragment's position in
  the shuffled send order separately from its logical index.
- A fragmented DIRECT send now logs a "starting" line with
  `pkt_id`/`frag_total`/`hop_count`, mirroring the CHANNEL path;
  previously the first sign one existed was its first per-attempt line.
- Hearing this node's own bare DIRECT frame is now logged rather than
  passing silently.
- Field-tuned defaults lowered after testing: `direct_post_send_listen`
  0-5s -> 0.3-3s (success range 0-0.5s -> 0-0.4s), plus the
  small-mesh/path-reset/path-discovery values listed under the review
  pass above.

### Verification

The 2026-09-16 review's fixes were re-verified with a real end-to-end run
of `testscripts/fake_meshcore_repeater_sim.py` before and after, with
identical delivery results, and `python3 -m py_compile` after every edit.
Each RX-log step was verified on real hardware the same day (two Heltec V3
companions on a live public mesh): step 1 confirmed the feed exists and
that on-air ACK codes match the library's own `expected_ack`; step 2
measured a 1.03s zero-hop RTT and watched the adaptive timeout converge on
its floor; step 3 sent 3 packets x 3 fragments with 9/9 single-pass ACKs
and a v2 bitmap answer read off the air; step 4 ran with holds on and saw
them fire exactly where step 2's one collision had happened. The 2026-09-18
review was checked with the simulator before and after (zero-hop
fragmented, and the two-hop lossy-return-path case that drives the
reconcile query). The two field batches above were each diagnosed from,
and re-verified against, their own captures in
`fieldtests/raw/postAlpha0.1.0/`.

Known gaps carried into the next cycle: the `hop1_loss` and
`downstream_loss` post-miss branches and the reconcile path itself are
unit-checked and simulator-checked but have not been exercised by a clean
multi-hop capture; RX-log transmit holds stay off by default until one
confirms the model; and bare (single-message) DIRECT sends still have no
completion-check equivalent.

Several findings from the 2026-09-16 review (a scattered small-mesh-mode
check duplicated across three dispatcher call sites, three independent
hardcoded administrative-context-policy special cases that could be
generalized into one table, `_priority_tier` being recomputed rather than
threaded through the outgoing-packet call chain,
`_direct_exchange_queue_depth` duplicating state already latent in
`_PriorityAsyncLock`, and a low-severity redundant `_reset_stale_path`
call when multiple fragments of one DIRECT-fragmented send fail in the
same window) were judged real but lower-severity design/efficiency
suggestions rather than bugs, and were left unchanged to keep that change
set scoped to fixes with a concrete failure scenario.

## alpha-0.1.0 (2026-09-15)

Initial alpha: milestones M0 through M6 of the rebuild, plus the post-M6
field-driven fixes made against real hardware. See the module docstring in
`Interface/SmartMeshCoreInterface.py` for the per-change history.
