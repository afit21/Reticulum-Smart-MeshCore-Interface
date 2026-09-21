# Changelog

## Unreleased (alpha 0.1.5, 2026-09-21)

The items of the alpha 0.1.5 pass, each from the alpha 0.1.4 field session's captures
(`fieldtests/raw/Alpha0.1.4/`, desktop `afipc_` + laptop). No wire change so far. The dated design
record is `docs/history.md` ("Alpha 0.1.5 pass").

- **Hop-aware airtime cap** (new key `duty_cycle_max_fraction_zero_hop = 0.85`; `duty_cycle_max_fraction`
  stays 0.30). Two ledgers over the same 60 s window: everything a repeater relays (multi-hop DIRECT,
  every CHANNEL flood) is charged to both and waits on both, so it stays at 30%; a zero-hop DIRECT
  frame is charged to the total ledger only and waits on 85%. Field motivation: the zero-hop 12-part
  page of 2026-09-21 spent 109 of its 147 s in duty-cycle waits at 30%. Capture: `duty_cycle_ledger`
  on `direct_attempt_result` / `raw_fragment_sent`. Tests: `tests/test_duty_cycle_hop_aware_0921.py`;
  shipped-default pin and `tests/golden/config_defaults.json` re-pinned for the new key.
- **Radio-busy accounting (2a)** (new key `direct_raw_burst_queue_ahead = 1`). The firmware queues a
  frame and returns at once, so a zero-hop window of 15 fragments was "sent" in 2.6 s against ~14 s of
  air, and a report arriving meanwhile ended the sender's wait. Every keyed frame now extends a
  per-interface busy-until by its estimated airtime; the raw window's burst end, report wait and
  report-latency estimator anchor to it, `since_own_tx_s` in the radio log is measured from it
  (negative while our own queue is still on air), and a zero-hop burst hands the next fragment over
  only when at most one frame is queued ahead of the one on air. Tests:
  `tests/test_radio_busy_until_0921.py`; the unit fake's radio block is SF8/BW250 so its estimate
  matches its air model.
- **The receiver holds reports while a window is still arriving (2b)** (new key
  `direct_report_hold_during_burst = yes`). A part completed by an unflagged fragment is reported only
  once the sender's fragments have stopped arriving (one fragment spacing plus half an airtime,
  re-armed by every fragment); a flagged fragment -- or a bucket that has already seen one -- reports
  at once. One report per window instead of one per part; the mid-burst reports that ended the
  field sender's wait early and collided with its own queue are gone. Tests:
  `tests/test_report_hold_during_burst_0921.py`.
- **An early report is progress, not the end of the wait (2c).** A report that lands before the
  burst has ended on air is applied to the parts it names and the sender keeps waiting until
  burst end + report window; parts absent from any report are re-burst only after that wait has
  expired (the field's part-8 report ended the wait with parts 9-12 unmentioned and re-burst them
  behind the sender's own queue). The `completion_check_result` record gains `early_reports` and the
  report's `entries`. Tests: `tests/test_early_report_is_progress_0921.py` (the 08:38 sequence
  re-sends nothing).
- **Capture filename carries a node label** (new key `packet_capture_label`, default the MeshCore
  node name from SELF_INFO): `<label>_capture_<interface>_<stamp>.jsonl`. The capture readers accept
  both forms. `fieldtests/AB_PROTOCOL.md`: laptop files need the label; MeshChat's RNS at loglevel 6.
  Tests: `tests/test_capture_label_0921.py`.
- **Adaptive window collect.** `direct_raw_window_collect` (0.75 s) is now the maximum: a window
  keeps collecting only while packets are still queued from RNS or a part joined within the
  transfer's observed inter-part spacing (floor 40 ms). A lone packet starts within 50 ms instead
  of 0.75 s; a window of parts still batches. Capture: `raw_window_collect`. Tests:
  `tests/test_adaptive_window_collect_0921.py`.

## alpha-0.1.4 (2026-09-21)

Everything since alpha-0.1.1 (2026-09-18 night), released as one version: the raw binary DIRECT
fragments that alpha 0.1.2 introduced, the one-hop and multi-hop field fixes of alpha 0.1.3, the
MeshBench real-firmware test tier, and the 2026-09-20 airtime / throughput pass (phases 1-3 below)
that rebuilt the fragment reconcile. In short:

- **Less airtime per delivered byte, more of the large packets delivered.** MeshBench
  `large_payload` (483-byte parts through one repeater), medians over three seeds: delivered 17 % ->
  83 %, on-air bytes per RNS byte 12.76 -> 5.11 (`tests/baselines/2026-09-20-meshbench-6cf0876.md`
  against the frozen alpha 0.1.3 suite). Every other scenario inside its run-to-run spread. No field
  numbers for this build yet; the field A/B is the next step.
- **How:** fragment reports and answers no longer wait for a MeshCore ACK (M1); one report covers a
  whole window of parts ("Q" protocol v4, M2); a 483-byte part is three raw fragments instead of
  four (9-byte raw header, M3); from one hop up each burst carries an XOR parity fragment so a
  single lost fragment is rebuilt without a retry round (M4, on by default); a DIRECT send stops
  retrying once its reply is seen, stale PROOFs never go on air, RNS path re-requests are answered
  from a local announce cache, link handshakes pre-empt idle radio holds, and the report window is
  sized from measured report latency (phase 1).
- **Both nodes must run alpha 0.1.4 or later.** The "Q" completion frames (v4) and the raw fragment
  header (version 2) are not decoded by earlier builds; compatibility with earlier builds is not a
  goal while the project is in alpha.
- **Install is unchanged**: copy `Interface/SmartMeshCoreInterface.py` into `~/.reticulum/interfaces/`.
  The file is now assembled from `Interface/src/smci/` by `python3 Interface/build_interface.py`
  (edit the sources, run the build, commit both).
- **New config keys, all optional** (defaults shown): `direct_report_noack = yes`,
  `direct_report_debounce = yes`, `direct_raw_window_enabled = yes`, `direct_raw_window_collect = 0.75`,
  `direct_raw_window_max_parts = 6`, `direct_raw_parity_enabled = yes`, `direct_raw_parity_min_hops = 1`,
  `proof_max_age = 45`, `announce_cache_ttl = 3600`, `path_request_local_answer_min_interval = 120`;
  `direct_raw_report_wait_base` is now 4 s and `direct_raw_report_wait_per_hop` 2.5 s. Every default
  is pinned by `tests/test_shipped_defaults.py` and `tests/golden/config_defaults.json`, every wire
  byte by `tests/golden/wire_format.json`.
- **Tests:** 288 unit tests (`python3 -m unittest discover -s tests`), the MeshBench scenario suite
  (`testscripts/meshbench_scenarios.py`, nineteen scenarios against real MeshCore v1.17.1 firmware),
  and the dated history in `docs/history.md`.

The dated sections below are the record, newest first.


Field evidence: `fieldtests/raw/Alpha0.1.1/` -- a zero-hop NomadNet page
session and an evening drive through 1-3 repeater hops, both sides
captured. The module docstring's "Alpha 0.1.1 captures" and "Raw binary
DIRECT fragments" entries carry the packet-level detail.

### Changed: airtime / throughput pass, phase 3 -- the reconcile redesign (2026-09-20 night)

Design: `docs/reconcile_redesign.md`. One module owns the burst-and-report state machine; each
timing decision is a pure function with a test pinned to its field number. Each milestone is gated
on the full suite and MeshBench `large_payload` + `relay` (two runs) against the previous milestone.

- **M1: reports without a firmware ACK, debounced** (new keys `direct_report_noack`,
  `direct_report_debounce`, both yes). REPORTs and QUERY ANSWERs go out as MeshCore
  `TXT_TYPE_CLI_DATA`: encrypted and relayed like any text message, delivered as CONTACT_MSG_RECV
  with txt_type 1, never ACKed by the firmware (`BaseChatMesh::onPeerDataRecv`). The "Q" bytes are
  unchanged; the reporting node's lock is held for the frame's airtime and relay gap instead of an
  ACK wait. A gaps report is held one fragment airtime (plus the relay gap) and dropped if the
  bucket completes first -- the second-last fragment's report and the duplicate last fragment it
  caused (20 of 43 zero-hop rounds) are gone. Tests: `tests/test_reconcile_m1_noack_reports_0920.py`.
- **M2: one report per window** (new keys `direct_raw_window_enabled` yes, `direct_raw_window_collect`
  0.75 s, `direct_raw_window_max_parts` 6; "Q" protocol v4, `COMPLETION_PROTOCOL_VERSION` 3 -> 4 --
  both nodes must run this build). Parts to one peer arriving within the collect window burst as one,
  the last two fragments flagged, one quiet period, one v4 multi-entry report (a bitmap per part,
  the receiver's recent packets listed newest first); re-drives are batched the same way and the v4
  QUERY asks about the whole window. The window takes the in-flight slot. Golden wire snapshot
  regenerated: v1-v3 bytes unchanged (pinned under `v3` names), 96 v4 cases added. Tests:
  `tests/test_reconcile_m2_window_0920.py`.
- **M3: three fragments per 483-byte part** (raw header version 2, 9 bytes: a 2-byte source prefix
  resolved to the unique bound peer, `RAW_HEADER_SIZE` 13 -> 9; both nodes must run this build). The
  per-fragment payload at the shipped cap is 161 up to four hops and 3 x 161 = 483, so a Link MDU
  part is three raw fragments (510 B on air) instead of four (688 B). Raw is not used where the
  short prefix would be ambiguous. Golden wire snapshot regenerated (31 raw/budget cases). Tests:
  `tests/test_reconcile_m3_short_header_0920.py`.
- **M4: hop-adaptive XOR parity** (`RAW_FLAG_PARITY` 0x08; new keys `direct_raw_parity_enabled`
  **yes** -- shipped off by the M4 gate on 2026-09-20, switched on by the owner's decision on
  2026-09-21; the baseline below was taken with it off -- `direct_raw_parity_min_hops` 1). From one hop up
  a part's burst of two or more fragments ends with one parity fragment (coverage mask in frag_idx,
  payload = last covered length + XOR of the covered fragments); a receiver missing exactly one
  covered fragment reconstructs it and completes without a report round. Its gate (large_payload x3:
  4/6, 5/6, 1/6 against M3's 2/6, 6/6, 6/6) showed no benefit and a cost, because MeshBench's
  repeater loses fragments in an alternating pattern (its ~1.3 s frames against a gap sized for the
  real ~0.9 s) that no single parity repairs; reconstruction itself worked (1, 3 and 4 per run). The
  field's random loss is the case it is for; `direct_raw_parity_enabled = no` is the field A/B's
  other arm. Golden wire snapshot: parity cases added. Tests: `tests/test_reconcile_m4_parity_0920.py`.
- **Phase 4:** version alpha 0.1.4; full suite 288 tests OK (three `@slow` raw scenarios re-pinned
  to M2/M3: three fragments per 446-byte payload, and a REPORT under both-ways zero-hop load waits
  behind one outgoing window, 12-15 s observed, bound 20 s); baseline
  `tests/baselines/2026-09-20-meshbench-6cf0876.md` (seven scenarios x seeds 7/11/17) against the
  frozen alpha 0.1.3 suite: large_payload 17 % -> 83 % delivered at 12.76 -> 5.11 on-air B per RNS B
  (outside the frozen spread both ways); everything else inside its spread; two_hop's RTT median
  worse on one run (re-run before reading it). `mixed_builds` against an alpha-0.1.3-era responder
  delivered 0/6 (alpha 0.1.3: 3/6, 5/6, 0/6): both nodes must run this build. By the owner's
  decision (2026-09-21) compatibility with earlier builds is not a goal during alpha.

### Changed: airtime / throughput pass, phase 2 -- the module split, no behaviour change (2026-09-20)

- **The dated design history moved out of the module docstring into `docs/history.md`**, verbatim
  (the alpha 0.1.0 STATUS snapshot, M0-M6 and every dated entry since, ~2900 lines). The docstring
  keeps the rationale, the design invariants and a new WIRE FORMAT section written from the code
  ("R" / "P" / "Q" text frames and the 13-byte raw header, byte for byte, pinned by
  `tests/golden/wire_format.json`). New entries go at the end of `docs/history.md`. CLAUDE.md's
  "Missing design docs" note lists the documents the history cites that never existed; none was
  created.
- **The interface is assembled from a source package.** RNS `exec()`s a custom interface as one text
  file (no `__file__`, no package machinery -- `RNS/Reticulum.py`, checked by
  `testscripts/check_install_load.py`), so the split lives in `Interface/src/smci/` (`_common`,
  `_locks`, `_config`, `_observability`, `_wire`, `_peers`, `_paths`, `_direct`, `_reconcile`,
  `_routing` as mixins, `interface.py` the class) and `python3 Interface/build_interface.py`
  concatenates it into `Interface/SmartMeshCoreInterface.py`, which is still the file that is
  installed. Ten pure-move commits, one module each, each audited function by function against the
  previous deliverable (`testscripts/audit_split.py`: same bodies, same constants; the only addition
  is the module-level `PRIORITY_*` mirrors of the class constants that mixin methods use as default
  arguments) and pinned by `tests/test_module_split_0920.py`. Edit the sources, rebuild, commit both;
  the pre-commit hook refuses a stale deliverable. The install method and `update-interface.sh` are
  unchanged.

### Changed: airtime / throughput pass, phase 1 (2026-09-20 evening)

Small wins on the existing code before the module split; one commit, one regression test, one
docstring entry each. The metric is on-air bytes per delivered RNS byte, read with delivery rate and
per-part completion time, against `fieldtests/raw/Alpha0.1.3/` and the `alpha-0.1.3` MeshBench
baseline. Phase 0 first added `tests/test_golden_config_defaults.py` / `tests/test_golden_wire_format.py`
(snapshots of every default and every encoded frame, generated from the frozen alpha 0.1.3 build) and
`testscripts/check_install_load.py` (loads the file exactly as `RNS.Reticulum` does: `exec()` of the
text, so the deliverable must stay one self-contained file).

LXMF finding (phase 0, verified in `RNS/Resource.py` and LXMF 1.1.1; MeshChat v2.4.0 bundles the same
rules): neither LXMF nor MeshChat times a transfer. The binding timer is RNS.Resource's sender proof
wait once the last part has been sent once -- four intervals of `3 x rtt_r + 10 s` (56-112 s at
rtt_r 1.3-6 s) with no part request cancel the resource, LXMF tears the link down and restarts the
message from scratch (up to four times). With a 4-part window a lost tail part is only recoverable
above `240 / (6 x rtt_r + 20)` parts per minute (7.5/min at rtt 2 s, 5.5/min at 4 s).

- **The metric is in the capture.** `on_air_bytes` on every `direct_attempt_result`,
  `raw_fragment_sent` and `channel_fragment_sent` record (the single-frame CHANNEL send now writes
  one too), and `testscripts/field_ab_compare.py` reports on-air bytes per delivered RNS byte. The
  2026-09-20 session, estimated from frame sizes: desktop 2.59 B/B, laptop 1.43 B/B at zero hop.
- **A bare DIRECT send stops retrying once its reply is seen.** A LINKREQUEST whose attempt 0 lost
  its firmware ACK was re-sent 8 s after its LRPROOF had arrived (laptop `*144922`, two hops:
  99 B + 3.4 s ACK, LRRTT queued 3.8 s behind). The three receipt paths that correlate an LRPROOF
  / plain-DATA PROOF now signal the send (`_signal_send_answered`; the key is a LINKREQUEST's link_id
  or a SINGLE-destination DATA's truncated hash); the retry loop makes no further attempt and an ACK
  wait in progress ends as `ack_timeout_source="answered"` (no RTT sample, no backoff). Path evidence
  is recorded only when the reply came DIRECT from the addressed peer. Tests:
  `tests/test_answered_sends_0920.py`.
- **Completion-report window sized from measured report latency** (default change:
  `direct_raw_report_wait_base` 2.0 -> 4.0 s, `direct_raw_report_wait_per_hop` 3.0 -> 2.5 s). Zero
  hop: the receiver's report waited p90 4-5 s for its own radio lock, so a 2 s window sent 29 of the
  desktop's 77 hop-0 rounds to a QUERY round trip for a report that was merely late. The window is
  now max(floor 4 + 2.5 x hops, per-peer srtt + 2 x rttvar of burst-end -> complete-report arrival,
  late reports included), capped by the answer budget. From the review of the same capture: a
  report missing only the last fragment sent (the second-last fragment's, which 20 of 43 hop-0
  rounds had acted on, re-driving that fragment as a duplicate) is provisional for up to half the
  window; a QUERY whose answer future a late report already resolved is not transmitted
  (`answered_before_send`, 24 of 29 hop-0 "answered" rounds) and no longer shrinks `_query_rtt`.
  Tests: `tests/test_report_window_0920.py`; `tests/test_shipped_defaults.py` and
  `tests/golden/config_defaults.json` re-pinned.
- **Stale plain PROOFs age out** (new key `proof_max_age`, 45 s; 0 = off). The sender's RNS receipt
  for a non-Link packet over this interface fails at 62 s (`first_hop_timeout` from `bitrate` 80 +
  6 s/hop); the desktop's 2-hop phase transmitted 12 proofs aged 45-105 s after a 13-deep proof
  queue. Replayed, 45 s skips 16 attempts (~76 s of lock) and loses 3 proofs that still landed in
  time (60 s: 12 / 0; 30 s: 24 / 4). Checked before every attempt (one bare frame, nothing spent),
  never a path failure; link-class proofs exempt. Also closed: an attempt-0 expiry during the lock
  wait fell through to a transmitted attempt 1. Tests: `tests/test_proof_max_age_0920.py`.
- **An RNS path re-request is answered from the cached announce** (new keys `announce_cache_ttl`
  3600 s, `path_request_local_answer_min_interval` 120 s; 0 = off). A closed pending Link makes a
  non-transport RNS node expire the path and ask again, and the far side replays the same cached
  announce bytes: the laptop received one destination's 235-byte announce six times in an hour at two
  hops (2-3 raw fragments plus reports each, after a 2-hop DIRECT request each). Announces a bound
  peer delivered DIRECT are cached; a re-request for a cached, still-bound destination is answered
  by handing the bytes back to RNS (context PATH_RESPONSE, so a transport node does not re-flood it)
  and not transmitted; the next request inside the interval goes on air to verify. Path-request
  records carry `requested_hash`. Tests: `tests/test_local_announce_cache_0920.py` (including the
  real `RNS.Transport` accept / ignore / re-accept sequence).
- **Link handshakes pre-empt idle holds of the radio lock.** LINKREQUEST / LRPROOF / LRRTT /
  LINKIDENTIFY / LINKPROOF (not KEEPALIVE or LINKCLOSE, which share the tier) set the lock's pre-empt
  event; a raw burst yields during a fragment's duty-cycle throttle wait (the session's longest idle
  hold, 26 s) and after a fragment's gap, resuming ahead of ordinary waiters; the report wait
  releases the lock and keeps listening; a QUERY's quiet window and the post-miss listen end early;
  a completion ANSWER/REPORT's ACK wait is cut once the expected ACK time has passed
  (`ack_timeout_source="preempted"`, no backoff). Evidence: link-critical attempts waited ~33 s for
  the lock over 26 attempts (median 1-3 s), a 2-hop LINKREQUEST 3.2 s behind an answer's 8 s ACK
  miss inside a 17.4 s link. Tests: `tests/test_handshake_preemption_0920.py`.

### Added: alpha-0.1.3 simulated benchmark (2026-09-20, evening)

`tests/baselines/alpha-0.1.3-simulatedbenchmark/` -- the development branch (interface at 1b69fa7, the
build the day's field session ran on) through all twelve MeshBench scenarios over seeds 7, 11 and 17,
36 runs, produced by `meshbench_scenarios.py suite` in one command (`summary.md` generated,
`README.md` the commentary, `runs/*/result.json` the evidence). It supersedes
`2026-09-20-meshbench-1b69fa7.md`. Headlines: zero_hop and relay unchanged (100 % [88-100 %]
delivered, 86 % / 73 % of completion checks ending on a report); two_hop now measures the two-hop
DIRECT path behind the start gate (75 % [62-88 %], attempts 67 / 76 %); three_hop reached three hops
in two of three runs (attempts 39-47 %, ACK 3.7-4.0 s -- the field's 42 % / 5.6 s); one-hop pages,
bidirectional pages and one-hop handshakes are dominated by MeshBench's missing listen-before-talk
(pages 464 s or timed out, handshakes median 22 s vs 3-9 s zero-hop / 14-17 s two-hop in the field),
so their floors are unreachable here and are read relatively. Two findings: `many_peers` never bound
more than 3 of 4 peers in 7-12 minutes (five zero-hop nodes' bind/announce traffic colliding; the
>3-peer routing has still never run against firmware), and seed 13 gives node A a reserved 0x00
identity (eleven runs discarded, guarded since). Timing mechanics held in every run (missed-attempt
timeouts 5 / 8 / 11 / 14 s at 0-3 hops, post-miss listen <= 1 s, no backoff drops).

### Added: test-suite coverage pass (2026-09-20, evening) -- no interface change

The 2026-09-20 comparison work listed what the suite could not tell us; this
pass closes the gaps that do not need a MeshBench fix upstream. Nothing in
`Interface/SmartMeshCoreInterface.py` changed.

**MeshBench scenarios** (`testscripts/meshbench_scenarios.py`; `list` prints them all):

- Traffic modes. `rns_multiprocess_sim.py node` gained `--traffic probe|link|resource`: one unit is
  a PROVE_ALL DATA packet as before, an RNS Link handshake (LINKREQUEST -> LRPROOF, reported against
  MeshChat's 15 s window, `--link-deadline`), or a real `RNS.Resource` over a Link (`--resource-size`
  5100 B = 12 parts at the Link MDU, the field's NomadNet page; reports parts, re-sent parts,
  complete / failed / timed out, wall time). The responder can push its own Resource back on every
  Link (`--respond-resource-size`) for bidirectional load.
- New scenarios: `page_transfer` (one hop, three 12-part pages), `page_transfer_bidir` (both ends
  sending pages at once -- the 2026-09-19 night geometry), `duty_cycle_pages` (zero hop, back-to-back
  pages under the 30 %/60 s limiter), `link_setup` and `link_setup_two_hop` (handshake times at one
  and two hops), `bring_up` (two_hop topology, no probes: time to a DIRECT path at each end and the
  path requests it took), `three_hop` (A-R1-R2-R3-B), `many_peers` (five companions each with an RNS
  node, so the sender has four bound peers and small-mesh mode is off -- the CHANNEL / supplement
  routing runs against firmware for the first time; hard check on `bound_peers` and
  `small_mesh_mode=False` in the capture), `mixed_builds` (responder on `git:d7dcba9`, the tree
  before the completion report, so the protocol change is checked against a peer without it;
  `--responder-interface` takes a path or `git:<rev>`), `companion_restart` (the sender's companion
  firmware rebooted mid-run; informational) and `soak` (`--duration`, default 30 min, with health
  snapshots: RSS, threads, sizes of the interface's growable maps).
- `two_hop`, `three_hop` and `link_setup_two_hop` now hold the sender until BOTH ends have a DIRECT
  path (`--start-after-paths`, `--gate-timeout` 600 s), so they measure their hop count rather than
  the advert coin flip that dominated `two_hop` on 2026-09-20; `bring_up` measures that on purpose.
- Late deliveries: a PROOF that arrives after the 60 s probe timeout is reported as late
  (`--late-grace` 90 s) instead of counted as lost, and the RTT distribution (min / median / p90 /
  max) is reported, so the timeout is no longer a cliff between PASS and FAIL.
- `suite` subcommand: several scenarios x several seeds (default 7, 11, 13), `--parallel` runs at a
  time, `run.log` per run, then `summary.md` / `summary.json` with per-run rows and per-scenario
  medians with ranges; `--write-baseline tests/baselines/<file>.md` regenerates a baseline file in
  one command, `--summarise-existing` rebuilds it from the run directories already there. `report`
  summarises finished run directories; `topology <scenario> --place NAME=E,N[,H]` measures a
  scenario's link budget against the terrain without running it (how `three_hop`'s placements were
  found: the terrain east of R2 is flat for 50 km, so the chain bends north).
- Two MeshBench v0.1.0 quirks found by the first benchmark run of the new suite, both now guarded:
  seeded identities ignore MeshCore's reserved first bytes (seed 13 gives node A a public key
  starting 0x00, which the firmware never generates because the first byte is the on-air path hash;
  the other side held A in its firmware contact store yet path discovery never answered and no
  DIRECT frame was exchanged in eleven runs) -- every run now reads each node's `_main.id` after the
  firmware starts and stops with exit 3 on a 0x00/0xFF prefix; and `node.move()` does not re-price
  `link.pair`, so `topology` re-creates the project per trial.

**Analysis** (`testscripts/meshbench_report.py`, the summariser that produced the 2026-09-20 tables,
moved out of the session's scratch directory): every `run` now embeds it in `result.json`
("analysis") and prints it -- per-hop attempt success and ACK latency, attempt kinds, completion
checks by outcome, raw fragments by reconcile round, the wait breakdown (lock / ACK / listen / quiet
hold / duty cycle / slot), per-part burst landings and time-to-complete, on-air transmissions and
bytes per node from MeshBench's events, misses by cause with the **LBT-preventable** share (a
half-duplex miss where the receiver keyed its own transmitter into a frame already arriving -- the
case the real firmware defers and MeshBench v0.1.0 does not model), and an airtime ledger (on-air
bytes per RNS byte accepted, per delivered unit). `--timeline` prints the merged event timeline.

**Unit tier** (`SMCI_SKIP_SLOW=1 python3 -m unittest discover -s tests`: 178 -> 199 tests, 7 skipped slow ones; the four gated simmesh scenarios left the count):

- `tests/test_shipped_defaults.py` pins every default each `_configure_*` method sets on an empty
  config block (133 keys), since FAST_TIMING makes them unpinnable through the live interface;
  `python3 tests/test_shipped_defaults.py --dump` regenerates the literals after a deliberate change.
- `tests/test_completion_report_one_hop_0920.py`: the one-hop report path -- the flagged second-last
  fragment with the last one missing reports the incomplete bitmap, `reported_stale` applies a
  mid-burst report when the post-burst wait yields nothing, a lost report falls back to the QUERY
  after exactly the one-hop report wait, and the shipped 1-hop wait (5 s) sits under the 7.5 s
  answer budget.
- `tests/test_linkrequest_bootstrap_backoff_0920.py`: LINKREQUESTs to an unknown destination arm the
  bootstrap backoff like DATA (the fourth is dropped in small-mesh mode, broadcast-only past the
  cap), a CHANNEL LRPROOF clears it through `_pending_link_requests` without learning a token, and a
  LINKREQUEST straight after a clear is routed.
- `ZeroHopBidirectionalPageTransfer` (`tests/test_raw_fragments.py`, `@slow`, ~75 s): both nodes send a
  twelve-part page to each other at once at zero hop; every part delivered, no `slot_expired`, no text
  fallback, no ANSWER / REPORT waiting more than 10 s for the lock. It replaces the unverified,
  SMCI_RUN_UNVERIFIED-gated simmesh multi-hop `NightSessionScenarios`, which moved to
  `tests/legacy/test_night_session_scenarios.py` (their `_bring_up` import had already broken when
  `test_sim_scenarios.py` was archived); the one-hop and bidirectional forms are the MeshBench
  `page_transfer` / `page_transfer_bidir` scenarios.

**Field protocol**: `fieldtests/AB_PROTOCOL.md` (same route, same page, two builds back to back or
alternated per fetch, both ends on the same build, >= 20 attempts per hop per build) and
`testscripts/field_ab_compare.py`, which puts two capture sets side by side per hop count on the
report's section-5 fields (attempt success and ACK latency, dead waits, completion outcomes and
QUERYs per raw send, part time, handshakes against 15 s, backoff drops, stale-path triples, airtime).
Run on the two existing same-build sessions (`Alpha0.1.2` vs `postAlpha0.1.1`, desktop) it shows the
run-to-run spread the protocol has to beat: hop-1 attempt success 86 % vs 70 % on the same tree.

Two things the unit work turned up in the interface, **not changed** here (the user's call):
`_note_channel_proof` pops the `_pending_dest_proofs` / `_pending_link_requests` entry when a proof's
CHANNEL copy arrives first, so a DIRECT copy arriving second learns no token for that destination
(pinned as-is in `test_channel_lrproof_consumes_the_correlation_so_a_later_direct_copy_learns_no_token`);
and the `reported_stale` capture record writes `complete: null` where the stale report's flag was
meant (observability only).

Not covered, deliberately: repeater-chain asymmetry (2 vs 4 hops each way) has no deterministic
construction in MeshBench's symmetric link model; the one-hop raw gap, post-send listen and answer
hold remain field-A/B decisions until MeshBench models listen-before-talk.

### Changed: speed / airtime / reliability pass (2026-09-20, later the same day)

Five parallel reviews (DIRECT send path, fragmentation and raw fragments,
discovery and binding, timing defaults, airtime) over the 2026-09-19 field
captures and the firmware/library/RNS source, merged into a ranked list of 23
proposals (`/tmp/mb/proposals-ranked.md` in the session; the top of the
remaining list is reproduced in the module docstring entries), then the top
three evaluated one at a time against a two-run MeshBench baseline
(`zero_hop`, `relay`, `two_hop`, `large_payload`) of the unmodified tree.
One methodological finding first: MeshBench v0.1.0's virtual radio has no
listen-before-talk (23 of `relay-1`'s 52 half-duplex misses were a node
keying 20-980 ms into a frame it was already receiving, which the real
firmware's `Dispatcher::checkSend` defers), so it over-counts
self-collisions between nodes that can hear each other -- zero hop, and a
sender against its own repeater at one hop -- while hidden-node collisions
at a repeater are real. Its numbers below are read with that split.

- **Receiver-initiated completion REPORT for raw bursts** (protocol change:
  `RAW_FLAG_REPORT` bit 2 of the raw header's byte 0 on a burst's last two
  fragments; ANSWER nonces 0xF0-0xF3 reserved for reports, QUERY nonces cycle
  1-0xEF; both nodes must run this build). The receiver of a raw burst sends
  the existing v3 ANSWER unsolicited when the packet completes, or its bitmap
  when a flagged fragment lands with gaps; the sender registers its waiter
  before the burst, keeps the radio quiet for `direct_raw_report_wait_base`
  (2 s) + `direct_raw_report_wait_per_hop` (3 s) x hops, falls back to the
  second-last fragment's report if the last one's never comes, and only then
  to the QUERY path as before. Every baseline run showed the QUERY keyed at
  the instant the receiver transmitted its own reaction (a PROOF): zero-hop
  attempt success 25-46%. `direct_raw_report_enabled = no` restores
  burst-then-QUERY. Two earlier cuts (flag on the last fragment only; a
  receiver-side idle timer) regressed `large_payload` to 0/6 and were
  replaced -- the docstring entry has the mechanism. MeshBench: zero_hop 7/8, 8/8 (baseline 7/8, 6/8 FAIL) with probe RTT avg 5-8 s (12-15) and 100% zero-hop attempt success (25-46%); relay 8/8, 7/8 (5/8, 4/8) with the repeater relaying 63-67 frames (118-133); large_payload 3/6, 3/6 (1/6 FAIL, 4/6) with a third of the QUERYs; two_hop bring-up-dominated (see the docstring entry).
- **Dead-wait trims** (no wire change): `direct_completion_unacked_grace`
  6 s (`_multihop` 10 s) caps the answer wait after a QUERY whose own
  firmware ACK was missed (answered 14% of the time in the field, never
  later than 5.6 s at hop <= 1); `direct_ack_timeout_base`/`_per_hop`
  8 + 4h -> 5 + 3h s (largest field ACK ever seen 3.82 / 6.06 / 8.15 /
  7.25 s at 0-3 hops over 2670 ACKs: none cut); `direct_post_send_listen_min`
  /`_max` 0.3-3 -> 0.2-1 s (1.7 s mean on 598 misses against 8.6%-vs-6.9%
  contention); a completion ANSWER's own ACK wait is hop-aware. MeshBench:
  relay 8/8, 8/8; large_payload 6/6, 1/6 (+5/6 and one run with no DIRECT path at all); two_hop bring-up-dominated; longest one-hop missed-ACK wait 12 -> 8 s, post-miss listen 1.6 -> 0.6 s mean, completion-check timeouts 97 -> 8 across the run sets.
- **A CHANNEL-carried PROOF clears the unknown-destination backoff.** Baseline
  `two_hop-1`: probes 2 and 3 delivered over CHANNEL and proved, three
  bootstrap attempts counted as "no token learned", probes 4-7 dropped by the
  interface for 300 s (`unknown_dest_backoff_drop`). `_note_channel_proof`
  matches a CHANNEL PROOF against the remembered bootstrap send (or pending
  LINKREQUEST) and clears the backoff, learning no token. MeshBench: two_hop 4/8 and 5/8 (the latter with no DIRECT path ever resolved) with zero backoff drops, where every earlier late-path run dropped 2-4 of 8; relay 8/8, 8/8.

Tests: `tests/test_completion_report_0920.py`, `tests/test_dead_wait_trims_0920.py`,
`tests/test_channel_proof_backoff_0920.py`. Field test proposed in the
session report; none of this is field-tested yet.

### Fixed: night-session fixes revised against the simulators and the MeshBench findings (2026-09-20)

Six interface changes, no wire-format change (module docstring entry
"Review of the night-session fixes against the simulators, plus the
MeshBench real-firmware findings (2026-09-20)" has the evidence):

- **In-flight cap back on at 2** (`direct_fragmented_max_in_flight` 0 -> 2).
  Simulated one-hop page transfer, twelve 483-byte Resource parts, three
  seeds: with the non-dropping, priority-aware cap every part arrived in
  181-243 s with raw completion 90-100%; with it off, 3-10 of 12 in 600 s.
  The scenario helper had never built a valid part before this pass (a
  hard-coded packing overhead of 34 where a LINK packet packs 19), so the
  earlier numbers quoted for it came from plain-DATA runs.
- **Quiet window anchored at the QUERY's ACK, 2.0 + 3.0 s x hops, RTT-
  adaptive upward** (srtt + 2 x rttvar after three samples), only after an
  ACK, still capped by the answer budget. Measured from the transmit, the
  first cut covered 19-32% of the one-hop answers that actually arrived.
- **Raw-fragment gap includes the frame's own airtime** (`(1 + factor x
  hops) x airtime`, MeshBench finding 2): `send_raw_data` returns when the
  frame is queued, not when it is off the air.
- **Completion ANSWER waits out the QUERY's ACK relay** (ACK airtime x
  (1 + 2.5 x hops), MeshBench finding 3).
- **SELF_INFO radio block bounded** (SF 5-12, BW 7.8-500 kHz, CR 5-8),
  refreshed after the interface's own `set_radio`, and a WARNING when one
  frame's airtime exceeds the whole duty-cycle budget (finding 1).
- **Small-mesh DIRECT-to-all skips the broadcast spacing** (finding 4).

Verified: fast suite 167 OK, slow in-process scenarios 9/9, simulated page
transfer on the new defaults (12/12 in 227 s, raw 92%, seed 11), MeshBench
`relay` PASS 5/8 with the RNS path up in 33 s (was 159 s) and
`large_payload` PASS 2/6 (was FAIL 1/6) with first raw bursts delivering
2-4 of 4 fragments instead of 3 of 4 with fragment 1 always lost;
`zero_hop` 6/8 and `two_hop` 2/8 in the same batch, the latter with no
DIRECT path resolved because B's advert never crossed two repeaters
(finding 7, untouched by these changes). Tests:
`tests/test_meshbench_findings_0920.py`.

### Added: MeshBench real-firmware test tier (2026-09-20)

`testscripts/meshbench_scenarios.py` (first written as `meshbench_relay_test.py`) runs the full stack (real `RNS.Reticulum`
per node, the interface loaded as `rnsd` loads it, the real `meshcore` library
over TCP) against [MeshBench](https://meshbench.github.io/), where every
simulated companion and repeater is the actual MeshCore firmware compiled
natively and only the radio channel is modelled. Two scenarios: `relay`
(A - R - B, asserts delivery, a 1-hop resolved path from the firmware's own
path discovery, and that R relayed on air) and `failover` (A - {R1, R2} - B,
R1's firmware stopped mid-run, asserts stale-path reset -> rediscovery via
R2). `rns_multiprocess_sim.py node` gained `--backend real` for this (the
installed `meshcore` library against a TCP companion endpoint instead of the
fake over the simmesh air) and now reports the interface's resolved paths per
probe. Nothing in the interface changed.

MeshBench was installed and the `relay` scenario run the same day (build
3b56c11 + working tree, 916.575 MHz / 62.5 kHz / SF7 / CR8 on every node,
A - R - B with A and B 16 km apart and out of each other's reach, checked
against MeshBench's own event log: 0 direct A<->B receptions). Findings:

- The interface came online against real `companion_radio` v1.17.1 firmware
  over TCP unchanged, bound its peer over CHANNEL, and resolved a 1-hop
  DIRECT path from the firmware's own path discovery; probes crossed the real
  `simple_repeater` (103 relays).
- A companion's single flood advert at bring-up was lost at the repeater
  because the repeater was still relaying that node's bind frame ("its own
  transmitter was keyed"), so the far side never had it as a contact and
  ignored its path-discovery REQs -- everything fell back to CHANNEL until a
  re-advert landed. Real companion firmware never re-adverts on its own; the
  test harness now does, on the sim backend's cadence. Worth knowing for the
  field: the interface itself never sends an advert.
- Delivery through the repeater was 2/5 with hop-1 DIRECT attempts ~50%
  successful on an idealised channel. 62 of ~80 missed receptions were
  half-duplex collisions at R between the two hidden endpoints, and the
  colliding frames were mostly the interface's own completion QUERY/ANSWER
  reconcile traffic plus DIRECT announces and path requests -- the mechanism
  the 2026-09-19 night-session fix above was aimed at, now reproducible
  without a repeater in the field.
- The firmware's `suggested_timeout` at one hop was 4.6 s for small frames
  and 6.4-10 s for full-size fragments, in line with the field's ~10.6 s.

#### Legacy simulation tooling archived (2026-09-20)

By the user's decision the simmesh-based fidelity tier is legacy:
`testscripts/fake_meshcore_repeater_sim.py` and
`calibrate_sim_from_captures.py` moved to `testscripts/legacy/`,
`tests/test_sim_scenarios.py` to `tests/legacy/` (not a package, so
`unittest discover` no longer collects it; `SMCI_SKIP_SLOW=1` still skips the
`@slow` two-node scenarios that remain in the unit files), its shared
bring-up helpers moved to `tests/_support.py`, and
`rns_multiprocess_sim.py`'s `run` orchestrator is marked legacy while its
`node` subcommand stays as the MeshBench suite's RNS end node. `simmesh`
itself remains as the unit suite's fake `meshcore`. CLAUDE.md has the
rationale and the verification order (unit suite -> MeshBench scenario ->
field test).

#### Scenario suite results (2026-09-20, later the same night)

`meshbench_scenarios.py` grew into eight scenarios (`list` prints them); all
eight were run, four of them in parallel by subagents, each reading the
interface's capture against MeshBench's event log. Results, build 3b56c11 +
working tree, production timing, 916.575/62.5/SF7/CR8 on every node:

| scenario | result | what it showed |
|---|---|---|
| zero_hop | PASS 5/5, 0 hops, RNS path 8 s | the bench case; only losses are the two radios' own half-duplex |
| relay | PASS 5/8, 1 hop, RNS path 159 s | hop-1 DIRECT attempts 42%/43%; 54 of R's misses half-duplex, 21 of them the 39 B QUERY/ANSWER frames |
| two_hop | PASS 6/8, 2 hops, RNS path 158 s | firmware suggested_timeout 6.7 s for a 40 B QUERY at 2 hops; all 3 completion ANSWERs lost the same way (see below) |
| repeater_returns | PASS 7/12 | R died after probe 3: stale path reset in 57 s, rediscovered 6 s after R's restart, 5/6 delivered afterwards |
| failover (cold standby) | PASS 5/10, 1 hop | R1 died after probe 3 and R2 started: stale path reset, 1-hop path rediscovered through a repeater never seen before, probe 5 delivered 78 s after the swap; the original hot-pair layout never brought up (see overlap_default) |
| large_payload (383 B) | FAIL 1/6 on delivery floor | B received 5/6 probes; fragment 1 of 4 lost at R in 7/7 probes (see below), PROOFs starved on the way back |
| busy_repeater | FAIL 0/0, RNS path never | C's chatter caused only ~20% of the loss; B's 2-fragment announces never both arrived under A's own request traffic |
| overlap_default | informational, RNS path never | two repeaters relayed every flood both (23/23), 19 overlapped, the far end lost all 19 |

Interface behaviour the suite surfaced (evidence in the scenario logs; the
interface itself is unchanged -- these are findings, not fixes):

1. **Bad SELF_INFO radio block disables outbound traffic silently.** A
   fresh-booted companion reported `radio_bw` as 63 (0.063 kHz after the
   library's /1000), which the check at `_fetch_own_identity` (`sf >= 5 and
   bw > 0 and 5 <= cr <= 8`) accepts, so `_estimate_airtime_s` priced a
   38 B frame at 1160 s and the duty-cycle limiter let one frame out per
   60 s window -- B sent 2 CHANNEL fragments in 3 minutes and every
   announce/PATH_RESPONSE queued behind them (`relay --seed 11`). Real
   Heltecs report sane values, but the failure mode is one absurd
   parameter -> one frame a minute, unlogged. Proposed: bound the check
   (5 <= sf <= 12, 7.8 <= bw_khz <= 500), refresh `_radio_params` after the
   interface's own `set_radio`, and log at WARNING when a single frame's
   estimate exceeds the whole duty-cycle budget.
2. **The gap after a raw fragment is timed from the send command, not from
   the end of the frame's airtime.** `_raw_fragment_gap_s` (2.0 x hops x
   airtime) starts when `send_raw_data` returns OK, which the firmware gives
   when the frame is *queued*; the fragment's own ~1.3 s on air eats most of
   it, so the next fragment or the completion QUERY leaves ~0.9 s after the
   fragment ends -- inside the repeater's 1.3 s relay of it -- and the
   repeater, half duplex, loses the new frame. Seen on 7/7 second fragments
   in large_payload, 7/9 QUERYs in relay, 3/3 second announce fragments in
   relay, 23 of R's 34 half-duplex misses in repeater_returns. A gap of
   airtime x (1 + factor x hops), started after the estimated end of the
   frame, would remove most of the one-hop self-collisions.
3. **Completion ANSWER leaves exactly as the repeater relays the firmware's
   ACK for the QUERY.** At two hops all three ANSWERs (`two_hop`) went out
   the millisecond B's own ACK ended, i.e. as R2 keyed its relay of that
   ACK, and were lost. A short hold after receiving a routed DIRECT message
   (~ACK airtime x 2.5 x hops) before the next own transmission would cover
   it.
4. **Small-mesh DIRECT-to-all sleeps a CHANNEL-sized spacing it never
   needs.** `_send_direct_supplement` waits `uniform(5, 10) x hops` seconds
   before the DIRECT copy of an unknown-destination packet (14-18 s at two
   hops), spacing meant to clear a CHANNEL broadcast that small-mesh mode
   does not send; most of probe 6's 51 s RTT in two_hop.
5. **Startup burst.** REQ, bind frame and (harness) advert leave within
   0.7 s of coming online; the repeater is still relaying the first when the
   others arrive, and the first path-discovery attempt was lost to it in
   every run that had a repeater.
6. Stale-path resets fired correctly after a dead repeater (57 s) but also
   as false positives under pure congestion (large_payload: A reset the path
   to B while B was completing A's packet; B reset after three lost PROOFs).
7. **RNS bring-up through one repeater is a coin flip at 180 s.** Both
   MeshCore paths were discovered within ~45 s in every run, but the RNS path
   (B's PATH_RESPONSE announce, 167 B = two CHANNEL fragments, or a 2-fragment
   raw DIRECT send once bound) took 53 s, 69 s, 158 s and 159 s in the runs
   that passed and never arrived within 180 s in three others. Each fragment
   is ~50% at one hop, both must arrive, A's own path request every 20 s
   plus its DIRECT supplement is most of what they collide with, and B
   rate-limits the responses it does get to send (`announce_rate_limited`
   6-18 per run). The scenario harness now allows 420 s and requests every
   40 s so bring-up variance does not mask the staged events, but a fresh
   multi-hop bring-up in the field has the same odds.

MeshBench-side caveats recorded in the script's docstring: `node.start`
starts every stopped node; per-node filesystems are keyed by node name (the
script now gives each run its own `MESHBENCH_NODEFS`); engine airtime runs
1.2-1.45x RadioLib's formula for the configured settings, so absolute
latencies here are ~30% pessimistic; two repeaters that both hear a source
both relay it (MeshCore v1.17.1 never cancels a queued relay), so any
hot-pair layout collides at the endpoints.

### Fixed: raw-fragment DIRECT performance at one hop (2026-09-19 night session)

`fieldtests/raw/Alpha0.1.2/*nighttest*` (build 3b56c11) compared like for
like at one MeshCore hop with `fieldtests/raw/binaryfieldtest/` (e87cca8):
reconcile answers that arrived 85% -> 48%, raw sends completing without
text fallback 8/8 -> 16/20, raw send median duration 28s -> 39s, six
packets dropped for want of an in-flight slot, and the 12-part page
transfer cancelled by RNS after 469s. Zero hop stayed at 96-100%. Four
changes, in order of effect (module docstring entry "Field regression
fixed (2026-09-19 night session)" has the packet-level evidence):

- **Radio-quiet window after every reconcile QUERY.** The querier's next
  raw burst started the instant its QUERY was ACKed and met the ANSWER at
  the repeater (a hidden node: 22 of 24 lost answers were never decoded by
  the querier's radio). The QUERY now keeps the radio lock until its answer
  arrives or `direct_completion_quiet_base` (1.5s) +
  `direct_completion_quiet_per_hop` (2.5s) x hops has passed since its
  transmit, capped by the answer budget and charged against it; the rest
  of the wait is still radio-free. Anchored at the transmit, the window has closed before a
  zero-hop ACK is in, so zero hop is untouched. `direct_attempt_result`
  records the hold as `quiet_hold_s`.
- **Raw pauses only after repeated evidence.** One answered-but-incomplete
  raw send used to pause raw for the peer for 600s (21:45:14: one unlucky
  fragment sent the next 46 page parts as text). It is a soft strike now;
  raw pauses at `direct_raw_incomplete_strikes` (2) in a row, a completed
  raw send clears the count, and `direct_raw_fallback_cooldown` is 120s
  (was 600). The two-strike "burst delivered nothing" rule and the
  per-path verdict are unchanged.
- **The per-peer in-flight cap is off by default** (`direct_fragmented_
  max_in_flight` 2 -> 0). It did not reduce reconcile timeouts (desktop 53%
  vs 35% the evening before), fragmented sends waited a median 30s for a
  slot, and six packets were dropped as `slot_expired` -- two of them data
  behind two 30-minute LXMF announces holding both slots. When enabled it
  is now priority-aware (`_PriorityAsyncSemaphore`), announce-class sends
  get a single slot of their own, and a send whose slot wait times out
  proceeds with a warning instead of being dropped (`slot_expired` is
  gone; `slot_wait_s` stays).
- **Re-burst after unanswered reconciles** (`direct_raw_reburst_after_
  unanswered`): unchanged at 2. The one simulated seed measured at 1 was
  no better (see the table), not enough evidence to move a default.

Kept, because they measured well: the hop-aware ACK ceiling, the
completion-answer caps and hop-aware floor, the v3 completion nonce, the
plain-PROOF priority tier, `direct_hop1_abort_default` and the per-path raw
verdict. No wire-format change; a 3b56c11 peer interoperates.

**Simulator fidelity fix found on the way (`testscripts/simmesh/air.py`):
the air model had no listen-before-talk.** Reproducing the night session
in the simulator first gave 5% answer delivery at one hop under every
configuration, including a full radio hold; the air log showed why -- the
answering node keyed its ANSWER 0.3s after its own firmware ACK, while the
repeater was still relaying that ACK, and the half-duplex repeater missed
it every time. Real radios never do that: `Dispatcher::checkSend()` defers
on `_radio->isReceiving()` (retry `nextInt(1,4)*120` ms, forced after 4s).
The air now models exactly that (`lbt=True`, `lbt_defers` in its stats);
a hidden node still collides as before, since LBT only hears what the
topology says a transmitter can hear. With it in place a single one-hop
raw packet reconciles cleanly, and the A/B below is against that model.

Simulator A/B (`tests/_support`/`simmesh` harness, A-R-B, twelve 483-byte
parts sent back to back, seeds 11/21/31, loss 0.06 during the transfer
and airtime 200ms + 1ms/byte -- the one-hop loss from
`calibrate_sim_from_captures.py fieldtests/raw/Alpha0.1.2` with the airtime
base scaled from 605ms so a run takes minutes; FAST_TIMING throughout;
logic checks, not delivery-rate predictions):

| configuration (seed 11; seed 21 where run)      | answers | raw completion | slot drops | delivered @900s | median delivery |
|--------------------------------------------------|---------|----------------|------------|-----------------|-----------------|
| baseline 3b56c11                                 | 41% / 67% | 2/3 / 4/6    | 6 / 5      | 6/12 / 7/12     | 104s / 64s      |
| all four changes, defaults                       | 63%     | 7/8            | 0          | 8/12            | 68s             |
| defaults, quiet window off                       | 74% / 58% | 4/4 / 4/4    | 0          | 4/12 / 4/12     | 54s / 66s       |
| defaults, one-strike 600s pause (old rule)       | 84%     | 7/8            | 0          | 8/12            | 69s             |
| defaults, cap re-enabled at 2 (priority, no drop)| 80% / 75% | 10/10 / 8/8  | 0          | 10/12 / 8/12    | 66s / 57s       |
| defaults, re-burst after 1 unanswered            | 61%     | 4/4            | 0          | 4/12            | 105s            |

Read with care: one or two seeds each, and every run used plain DATA parts
that hit `outgoing_max_age` (120s) partway through -- hence "delivered
@900s" under 12 everywhere; the scenario now uses Resource-class parts and
has not been re-run. What the table does support: the baseline reproduces
the field's shape (answers in the 40s, slot drops, an unfinished page); no
configuration of the new code drops anything; and the non-dropping cap at
2 was the strongest configuration in the sim, which argues for a field
check of the 2 -> 0 default rather than treating it as settled.

Tests: `tests/test_raw_fragments.py::NightSessionFixes` (unit, passing),
`tests/test_second_audit_0919.py::FragmentedSendsPerPeerAreBounded`
(updated to the new cap semantics, passing), and `NightSessionScenarios`
(simulated one-hop page transfer, bidirectional answer-queueing bound,
three-hop mixed traffic with the cap enabled) -- written, gated behind
`SMCI_RUN_UNVERIFIED=1` until run to a pass.

### Fixed: first multi-hop raw-fragment field test (2026-09-19 morning) -- two fixes

Both sides captured (laptop at 2 hops, desktop's path back at 4 hops).

- **Raw fragments through repeaters lost one of every two.** The gap
  between raw fragments was 2 airtimes regardless of hop count, but a
  fragment needs about hops x airtime to clear a half-duplex repeater
  chain, plus each repeater's random forward delay (0 to 1.5 airtimes in
  the simple_repeater firmware). Every 2-fragment raw send in both
  directions delivered exactly one fragment; solo re-sends arrived. The
  gap is now `direct_raw_hop_gap_factor` x hops x the fragment's own
  airtime (`_raw_fragment_gap_s`), follows the last fragment too, and is
  slept with the radio lock held so the reconcile QUERY (or another
  send's burst) cannot enter the chain early. Zero hop and one hop are
  unchanged from the gaps the earlier field tests passed with.
- **A raw sender on a dead cached path took three whole sends to notice.**
  The desktop kept the previous night's zero-hop path to the laptop and
  answered its path requests with raw fragments down it for 3.5 minutes
  (18 raw frames and 17 full-timeout QUERY misses), because the raw sender
  recorded one stale-path failure per exhausted send and the QUERY
  exchanges recorded none. Each raw round's QUERYs now feed
  `record_direct_send_result` (`_record_query_path_evidence`: an ACK or
  ANSWER clears the counter, a round whose every QUERY attempt missed
  after its full timeout counts one failure), and a send abandons its
  remaining rounds once its path has been reset
  (`_raw_path_reset_mid_send`). Its firmware's ACKs went down the same
  dead path, which is why the laptop saw every arriving send as lost.

Noted, not changed: the two directions' discovered paths differed (2 vs
4 hops); RNS re-sent one LXMF message as three distinct ciphertexts that
no payload-hash dedup can fold. Tests: `RawGapAndPathEvidence` (unit) and
`RawFragmentScenarios.test_stale_path_reset_within_one_raw_send`.

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

### Added: raw binary DIRECT fragments (on by default after the first field test)

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
py`; `zero_hop_peer_discovery_test.py --raw-fragments`. Switched on by
default after the field test below; a peer that has not advertised the
capability still receives text fragments.

### Changed: raw-first with a per-path Z85 fallback verdict (user's design)

The fallback note is now kept per path (the repeater chain) rather than
per peer: after `direct_raw_fallback_strikes` answered reconciles show a
burst delivered nothing, raw is paused for the peer and the packet goes
as Z85 text on the same path; if that succeeds the path is noted as not
carrying raw packets for `direct_raw_path_unsupported_ttl` (a day) and
the peer's pause is lifted, if it fails nothing is concluded about raw.
A new path is always tried raw-first again. `[STATS]` lists the noted
paths. A raw send that runs out of rounds with its reconciles answered
(the path is alive, the loss was just too high) is re-sent as Z85 text
rather than dropped (and raw is paused for that peer for the cooldown, without a
verdict on the chain); only an unanswered send counts as a path failure. A path is
only ever noted as not carrying raw when raw delivered nothing at all on it.

### Changed: refactor pass (behaviour-preserving)

Shared helpers replace the duplicated resume/reconcile bookkeeping in the
text and raw fragmented senders (`_resume_state`, `_remember_resumable`,
`_held_from_answer`), one Jacobson/Karels update serves both RTT
estimators (`_rtt_sample`), per-peer path state is cleared from one list
(`_clear_peer_path_stats`), and the ACK wait and post-attempt listen
decision are their own methods (`_await_direct_ack`,
`_post_attempt_listen_s`). No wire or behaviour change; the full suite
and the simulated-mesh scenarios pass before and after.

### Added: capture records name the send method

`direct_send_result` carries `method` (`z85_bare`, `z85_text` or `raw`)
and `fallback_from_raw`; `fragment_received` carries `raw`. The
receiver-side `transport` field (`direct_raw_multifragment` vs
`direct_multifragment`) already distinguished them.

### Fixed: first raw field test (2026-09-18 night) -- reconcile timing under load

The field run (`fieldtests/raw/binaryfieldtest/`, both sides captured)
confirmed the public repeater forwards raw packets: 35/35 raw packets at
zero hop and 483-byte parts through one repeater in two rounds each. It
also showed the reconcile QUERY running at the flat 5s floor after a
path change (raw bursts give the ACK-RTT estimator nothing to learn
from) and, worse, completion ANSWERs waiting up to 50s for the radio
lock because the node's own queries held it while idle. Now the
QUERY -> ANSWER round trip is measured per peer (`_query_rtt`) with a
hop-scaled prior before any sample exists, and the QUERY is sent as an
ordinary ACKed exchange with the ANSWER awaited radio-free. Overheard raw
packets are named `RAW_CUSTOM` in the RX log.

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
