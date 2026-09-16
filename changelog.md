# Changelog

## Unreleased

### Fixed: DIRECT-supplement target selection ignored recent failure history

Airtime-efficiency review (same pass that produced the completion-check
fix below): `_select_direct_supplement_targets` (path-request DIRECT
supplement) and `_select_bootstrap_supplement_targets` (unknown-destination
DIRECT bootstrap supplement) both picked their capped target list by
recency alone -- most-recently-confirmed/-seen first -- with no reference
to `_direct_path_failures`. A peer that had just failed a DIRECT attempt,
but hadn't yet crossed `direct_path_reset_threshold` (so was still fully
"resolved" and eligible), could still win a scarce supplement slot purely
on recency, ahead of an equally-recent peer this interface had no reason
to doubt -- spending part of a capped, airtime-costing fan-out on a send
statistically less likely to succeed.

Fix: both now sort primarily by each candidate's own `_direct_path_
failures` count (fewest first), falling back to the original recency
ordering only as a tiebreaker among equally-healthy peers. Not a hard
exclusion -- a struggling peer still gets picked once it's the least-bad
option available, and the count itself naturally clears on a fresh
success or drops the peer from candidacy entirely once a stale-path reset
fires. Verified with dedicated unit tests (a failing-but-more-recent peer
correctly loses its ranking to a healthier, older one in both functions)
and a re-run of the existing CHANNEL-path fake-hardware smoke test showing
no regression.

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
already knowing the peer's authenticated identity. `_send_direct_
fragmented` sends one QUERY (`pkt_id` + `frag_total`) only as a last
resort, once both retry passes are exhausted and fragments still appear
missing. The receiver answers directly from its own existing whole-packet
dedup cache (`_add_channel_fragment` already records a completed DIRECT
reassembly there under `(mode, sender_token, pkt_id, frag_total)` -- no
new receive-side state needed) -- correctly using the raw, uncanonicalized
sender token to match how that cache is actually keyed, verified with a
dedicated unit test. If the receiver answers "complete," the sender treats
the message as fully delivered and clears its recorded failure count for
that peer (undoing the false-failure signal already recorded per-fragment
during the retry passes), avoiding an unwarranted stale-path reset.

Fully backward-compatible and fails safe: a peer that doesn't understand
`"Q"` frames, or whose own answer is itself lost -- the same class of loss
this feature exists to route around, just at much lower stakes for one
small frame -- simply never answers, and `direct_completion_check_timeout_s`
(default 5.0s) elapses, falling back to exactly today's give-up behavior.
New config: `direct_completion_check_enabled` (default on),
`direct_completion_check_timeout` (default 5.0s). Deliberately scoped to
DIRECT-fragmented sends only -- bare (single-message) DIRECT sends have no
`pkt_id` and dedup on full payload bytes instead, which doesn't fit this
same query shape without carrying the payload (or a hash of it) in the
query itself; left as a known, smaller-impact gap for a future pass.

Verified: a standalone round-trip test of the new frame encode/decode, a
receive-side unit test (dedup-hit and dedup-miss query answers, answer-to-
waiter-future correlation), and a send-side unit test (prompt-answer and
timeout paths, including waiter cleanup) all pass; the existing
`testscripts/fake_meshcore_repeater_sim.py` CHANNEL-path smoke tests show
identical results before and after (this feature's own dispatch check
sits in the DIRECT receive path and doesn't touch CHANNEL handling).

## alpha-0.1.1 (2026-09-16)

Code-review pass over `Interface/SmartMeshCoreInterface.py` (the M0-M6 alpha
0.1.0 build plus its post-M6 field-driven fixes) -- eight independent review
angles (correctness line-by-line scan, removed-behavior audit, cross-file
tracer against the installed `meshcore` library and vendored RNS source,
reuse/duplication, simplification, efficiency, altitude, and CLAUDE.md
convention compliance), followed by manual verification of every candidate
finding against the actual source (and, in one case, against the two
angles that directly disagreed with each other) before anything was
changed. No new functionality; every change below is a bug fix, a
duplication cleanup, or a documented-but-deliberately-unfixed gap.

### Fixed: one real deadlock

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

### Documented, not fixed: LRPROOF routing-peer resolution gap

- **`_resolve_routing_peer`'s PROOF-correlation lookup doesn't account
  for LRPROOF's different wire layout.** `RNS.Packet.pack()` writes a
  Link's `link_id` (not a destination hash) into the field this
  interface reads as `destination_hash` when `context ==
  RNS.Packet.LRPROOF` (confirmed against the vendored RNS source). Since
  `_proof_correlation` is keyed by truncated hashes of previously-received
  packets, not link_ids, an outgoing LRPROOF (a Link-acceptance reply)
  never correlates to a known peer through this table, even when that
  peer is otherwise DIRECT-resolved. Effect is bounded to an efficiency
  loss, not a correctness or security issue: `_dispatch_outgoing_
  packet`'s existing case-3 fallback (broadcast + capped
  DIRECT-bootstrap-supplement) still delivers it. A correct fix needs a
  link_id -> peer table populated by replicating `RNS.Link.
  link_id_from_lr_packet()`'s exact hashing (including its ECPUBSIZE-based
  truncation), which was deliberately not attempted without real-hardware
  validation, consistent with this file's existing precedent of leaving
  an unvalidated crypto-adjacent computation flagged rather than guessed
  at (see `record_direct_send_result`'s own `rssi`-parameter note). Left
  as an explicit comment on `_resolve_routing_peer` for the next pass.

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
  sites (and any future one).
- **`_unknown_dest_attempts`/`_unknown_dest_backoff_until` had no
  periodic reclaim**, unlike `_dedup`/`_reassembly`/`_proof_correlation`
  (all three already swept every `REASSEMBLY_CLEANUP_INTERVAL_S`). A
  destination tried a few times and never addressed again (never
  resolved, never crossing the backoff threshold) sat in these dicts for
  the rest of the process's life; only a later success ever removed an
  entry. Added `_unknown_dest_last_attempt` (idle-since-last-attempt
  tracking) and `_unknown_dest_backoff_sweep`, wired into the same
  periodic pass as the other three tables.

### Verification

All fixes were re-verified with a real end-to-end run of
`testscripts/fake_meshcore_repeater_sim.py` (bind-frame peer discovery,
single-fragment and multi-fragment CHANNEL delivery) before and after this
pass, with identical delivery results -- no regressions from the
refactors. `python3 -m py_compile` was run after every edit.

Several additional findings from the same review pass (a scattered
small-mesh-mode check duplicated across three dispatcher call sites, three
independent hardcoded administrative-context-policy special cases that
could be generalized into one table, `_priority_tier` being recomputed
rather than threaded through the outgoing-packet call chain,
`_direct_exchange_queue_depth` duplicating state already latent in
`_PriorityAsyncLock`, and a low-severity redundant `_reset_stale_path`
call when multiple fragments of one DIRECT-fragmented send fail in the
same window) were judged to be real but lower-severity design/efficiency
suggestions rather than bugs, and were left unchanged in this pass to keep
the change set scoped to fixes with a concrete failure scenario.
