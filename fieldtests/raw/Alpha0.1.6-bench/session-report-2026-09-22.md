# Alpha 0.1.6 pass -- session report (2026-09-21 23:24 -> 2026-09-22)

## 1. Tree state as found

Branch `development` at `5a625d8` (the alpha 0.1.5 close-out), clean apart from the untracked
`fieldtests/raw/Alpha0.1.5/` (the owner's captures, left untracked). MeshBench installed and
working; no `rnsd` running on the desktop (the 18:43 one had been stopped at 21:33, its log
`~/.reticulum/rnsd-desktop-20260921T184313.log` ends with "Detaching interfaces"); MeshChat
running on both machines; nothing holding `/dev/ttyUSB0` on either; the laptop reachable, alpha
0.1.5 (`5a625d8`, md5 457afef5...) installed under `~/.reticulum/interfaces/` on both.

## 2. Per item

### Item 1 -- path selection by measured reliability (commits 8a21bb9, 24181c1 [second cut], 5e892bc [third cut], 9aa4b7d, 09105ae [fourth cut])

What changed: `_paths.py` replaces shorter-path adoption with a per-peer scoreboard
(`_PathBoard` / `_PathCandidate`, at most four candidates: discovered, the reverse of each flood
copy, zero hop once heard directly, the peer's reported path). Pure rules `_path_delivery_rate`
(0.5 ** (age / 180 s) weights, nothing older than 600 s), `_path_prior` (0.8 optimistic; 0.25 for
a zero-hop candidate heard below `path_weak_snr_db`; the peer's reported rate when it sent one),
`_path_score` = (hops + 1) / rate, `_rank_paths`, `_choose_path` (current kept under the miss
threshold; past it the best eligible candidate as a trial; a candidate is eligible again once its
last miss is older than the cooldown; all ineligible = exhausted -> discovery),
`_switch_for_good` (margin + switch-back cooldown). The one decision `_select_path` is called
from `_send_direct_packet` and `_send_direct_supplement`; it sets the device contact's path
(`change_contact_path`, text frames are routed by it) and mirrors into `_resolved_paths`.
`record_direct_send_result` feeds one sample per send/window (QUERY-round evidence passes with
`path_sample=False`, third cut); the ACK the rx-log matches to our own send gives the path's
last-leg SNR/RSSI. Every selection / trial / switch / exhaustion / discovery is a `path_selected`
capture record with every candidate's score.

The weak prior is 0.25, not the 0.4 the item named: at 0.4 a weak direct path ties an untried
one-hop path (2.5 = 2.5) and the hop tiebreak would choose the direct path the prior exists to
avoid; the owner's rule prefers a two-hop path (3.75) over a weak direct one, so the prior must be
below 0.8 / 3.

Wire: "Q" protocol v5 = the v4 header + `[path_len 0xFF none][rate 1/250 steps, 0xFF untried]`
before the v4 entries; every QUERY / ANSWER / REPORT carries it; the receiver adds a reported
zero-hop path as a candidate and uses the reported rate as the prior for untried candidates of
that hop count. v1-v4 still decode; a v4 QUERY is answered in v4. Both nodes must run this build.

Config: `path_selection_enabled` yes (alias `path_adopt_enabled`), `path_weak_snr_db` 3.0,
`path_switch_after_misses` 2, `path_switch_margin` 0.25, `path_switch_cooldown` 120 s; removed
`path_adopt_window`. Constants PATH_CANDIDATES_KEPT 4, PATH_SAMPLES_KEPT 8, PATH_SAMPLE_WINDOW_S
600, PATH_SAMPLE_HALF_LIFE_S 180, PATH_PRIOR_OPTIMISTIC 0.8, PATH_PRIOR_WEAK 0.25, PATH_RATE_FLOOR
0.05; COMPLETION_PROTOCOL_VERSION 5, COMPLETION_V5_HEADER_SIZE 6, COMPLETION_PATH_UNKNOWN 0xFF,
COMPLETION_RATE_SCALE 250.

Tests: `tests/test_path_selection_0922.py` (33 tests: the pure rules; the field replay from
`tests/fixtures/field_0921_desktop_22h.json` -- at 22:00:49 the 504 s old zero-hop route is not
chosen over the confirmed one-hop path, and after the two misses on `1976` at 22:10:12 / 22:10:24
the one-hop route is trialled with no flood newer than 22:08:42; the v5 codec; the scoreboard on
the fake node; one sample per window). `tests/test_shorter_path_adoption_0921.py` removed with
the mechanism. Golden wire regenerated (74 default cases byte-identical under `v4` names, 121 v5
cases added), config golden and shipped-default pins re-pinned.

Fourth cut (from the first close-out suite on 9aa4b7d: `link_setup` handshakes inside 15 s 38 % on
every seed against 62 % [25-75], with a delivering path exhausted and rediscovered on two
consecutive misses): a candidate with a weighted delivery rate of at least PATH_HEALTHY_RATE (0.5)
stays eligible until PATH_EXHAUST_MISSES (4) misses -- a better alternative is still trialled
after two, but alone the path stays in use and no relayed discovery flood runs. On the final
build's suite `link_setup` is back at 62 % [50-62] inside 15 s.

Cuts forced by MeshBench: second cut -- `_select_path` no longer stands aside for an in-flight raw
window (under continuous traffic the next part always arrived while the previous window ran, so no
trial ever happened: six misses in a row and no `path_selected` at all); third cut -- one sample
per window (a failing window counted four or five misses through the per-round QUERY evidence and
exhausted a path on one send).

MeshBench (`/tmp/mb/016/item1/`, build 24181c1):
- `shortcut_appears` x2: PASS / PASS on the hard check (run 1: trial of B's one-hop route 35 s
  after the move, delivered, switched for good, probes 7-10 at one hop; run 2: selected a
  two-hop flood-copy route first, switched to three hops when it failed, then trialled and
  switched to the one-hop route after the move, probes 8-10 at one hop). Baseline 2 of 3 seeds.
- `weak_direct` (new; A -5 km, R mast 50 m, B +6 km: A-R +20.7, R-B +12.5, A-B +3.0 dB, scenario
  gate margin 2 dB) x2: the direct path delivered 16/16 sends in both runs -- MeshBench loses
  nothing on a +3 dB link and reports SNR 0.0 for every frame, so neither route to the one-hop
  path (misses, or the weak-SNR prior) can occur; a delivering path is kept by design. The
  one-hop expectation is informational while the direct path delivers >= 90 %; the `path_selected`
  check is hard and passed (1 and 2 records). The weak-direct decision is the field's.
- `two_hop` x2 (item 1 pair): PASS 5/8 and 6/8, 69 % [62-75], 8.49 B/B [8.38-8.59], RTT 21.3 s
  (baseline 75 % [62-100], 9.75 [7.24-12.62], RTT 10.9 [10.9-29.1]) -- inside the spread.

### Item 2 -- bound the multi-hop window hold (commit 66c5514)

Reading the lock's holders against the source first: the window already released the lock before
every QUERY round and its between-parts yields carried no hop condition; what held the radio at
22:25-22:28 was the handshake tier itself -- six LINKREQUESTs in 2.5 minutes (MeshChat re-requests
every ~17 s), each answered by an LRPROOF of four attempts at 11 s ACK timeouts, queued at tier 0
ahead of everything, while the proof for the link the laptop had already abandoned was still
being retried (answers and reports waited 50-125 s; a text fragment 182 s).

What changed: `direct_raw_window_max_rounds` (new, 2) caps a window's rounds through repeaters
(zero hop keeps `direct_raw_reconcile_rounds`, 3; pure `_raw_window_rounds_rule`); a newer
LINKREQUEST from a peer supersedes every LRPROOF still pending for its earlier link
(`LRPROOF_KEY_PREFIX` + link_id as the answered-send key, registered for the whole 1.5 s delay and
send; no further attempts, an in-flight ACK wait cut, captured as `ack_timeout_source="superseded"`
or `routing_decision="lrproof_superseded"`, no path evidence, counted as a drop); the QUERY's quiet
hold yields to a queued completion report as it did to a handshake.

Tests: `tests/test_multihop_window_hold_0922.py` (9 tests: the rounds rule and default; a two-hop
window yields between parts to a handshake and a report; a handshake queued in the report wait
goes before round 1 and the window stops after two rounds; the LRPROOF key, supersession, the
retry loop stopping with `superseded` captured, the newest link untouched, a supersession inside
the RTT delay dropped before dispatch; the quiet hold cut by a report waiter).

MeshBench (`/tmp/mb/016/item2/`, build 24181c1):
- `link_setup` x2: PASS 6/8 and 7/8, 81 % [75-88], handshakes inside 15 s 50 % [25-75], links
  median 17.9 s [7.7-28.1] (baseline 88 % [75-100], 62 % [25-75], 8.8 s [8.1-18.3]) -- inside the
  spread. No supersession fired: the scenario's link requests are one per link with a 90 s wait,
  not MeshChat's 17 s re-requests, so MeshBench cannot show the effect; the field can.
- `page_transfer_bidir` x2: 0/3 and 0/3 (the baseline's own 0 % [0-33]), attempt rate 44 / 45 %
  (baseline 37 / 37 %), 13.1 B/B [11.9-14.3] (baseline 29.45 [7.24-33.75]).
- `two_hop` x2: PASS 7/8 and 3/8, 62 % [38-88], 13.51 B/B [9.51-17.50], RTT 29.8 s [23-36]. The
  3/8 run: B's flood copies over the marginal skip links (-4.5 dB, "must-block") gave the
  scoreboard phantom one-hop candidates that each cost a failed trial window before it switched
  back to the two-hop path; that build lacked the third cut. Four two_hop runs on 24181c1:
  75 / 62 / 88 / 38 % delivered, 8.4 / 8.6 / 9.5 / 17.5 B/B against the baseline's 62-100 % and
  7.2-12.6. Read the baseline suite's three seeds below for the final build.

### Item 4 -- a resilient serial connection (commit 24181c1)

What changed (`interface.py`): the interface builds the library's `SerialConnection` /
`TCPConnection` / `BLEConnection` and `MeshCore(cx, auto_reconnect=False)` itself, opens the port
(`dispatcher.start()` + `connection_manager.connect()`), releases the constructor as soon as the
port is open (or at once when it cannot be), settles `serial_open_settle` (2 s; opening a serial
port asserts DTR/RTS and resets a Heltec V3), flushes the pyserial input buffer, runs
`send_appstart` up to `handshake_attempts` (5) times at `handshake_timeout` (5 s) with a flush
before each, then the full device setup, and on DISCONNECTED tears down and retries with backoff
5, 10, 20, 40, 60 s forever (`max_reconnect_attempts` 3 -> 0 = forever; `auto_reconnect = no`
stays offline). Before opening a serial port it scans `/proc/*/fd` for another holder and logs
its PID and command line unmistakably, then proceeds. `_run_command` keeps waiting for the
expected reply after a reader-noise ERROR (`invalid_frame_length`, `binary_parse_error`,
`unknown_stats_type`) until `command_timeout` (15 s), counts them, and warns once a minute above
`serial_noise_warn_per_min` (5): "serial stream corrupted ... is another process reading the
port?". Every state change is a `connection_state` capture record (buffered until the capture
opens). The library facts behind each cause are in `docs/history.md` item 4.

Config: `connect_retry_min` 5, `connect_retry_max` 60, `serial_open_settle` 2, `handshake_attempts`
5, `handshake_timeout` 5, `command_timeout` 15, `serial_noise_warn_per_min` 5;
`max_reconnect_attempts` default 3 -> 0.

Tests: `tests/test_connection_supervisor_0922.py` (11 tests against the fake `meshcore`, which
now models the library's connection lifecycle with fault injection: a handshake answered on the
third attempt without reopening the port; never answered -> close, retry, up once the radio
answers; a port that cannot be opened retried with the backoff while the constructor returned; a
drop followed by a full re-setup on the same radio with fetching re-armed; the `connection_state`
sequence; `auto_reconnect = no`; reader noise during a command does not fail it; what is noise;
the once-a-minute warning; the /proc scan against a fake tree; the defaults). The harness reuses
one radio per node across reconnects and waits for online (`add_node(require_online=)`);
`zero_hop_peer_discovery_test.py` waits for online for 60 s.

MeshBench: none (the install-load check and the fast suite, as the item says; every MeshBench run
of the night brought its nodes up through the supervisor over TCP).

Hardware (desktop, real port, `fieldtests/raw/Alpha0.1.6-bench/afipc-bench-item4_capture_*`):
constructor returned 0.07 s after the port opened; handshake answered on the first attempt after
the settle; online 2.5 s after construction with the radio block (7, 62.5, 8), zero stream noise,
three times over. A forced close of the process's own serial transport underneath the library:
DISCONNECTED (`serial_disconnect`) in 0.1 s, the 5 s backoff, reopen, handshake, online again 7.7 s
after the drop on a fresh MeshCore object. Not run: the second-reader and port-holder checks
against a deliberately started second process (the session's tooling refused the second reader on
the port; the scan is pinned against a fake tree).

### Item 3 -- one report per window (commit 18059a2)

What changed (`_reconcile.py`): the gaps hold is one sender spacing plus half an airtime
(`RAW_GAPS_HOLD_SPACINGS` 1.0; ~1.5 s at zero hop, the hop gap + 0.45 s through repeaters -- it was
one airtime at zero hop, and the 2026-09-21 completing fragments landed 0.01 s past it at zero hop
and 0.1-1.55 s past the relay-gap hold at two hops); the receiver's holds scale by the larger of
its own hop count and the path length the sender reports in its v5 frames (the laptop had held at
its one-hop count against the desktop's two-hop spacing); a flagged frame for a packet already
delivered within the burst tail of a complete report just sent is not reported again (seven
packets were reported complete twice on the parity behind the completing fragment); `held_s` is
on every report record (0.0 = at once). `meshbench_report.py` prints reports per reported packet
and the held count.

Tests: `tests/test_one_report_per_window_0922.py` (7 tests); the M1 / 2b hold pins re-pinned.

MeshBench (`/tmp/mb/016/item3/`, build 18059a2):
- `large_payload` x2: PASS 6/6 and 6/6 at 4.64 / 4.74 B/B, reported fraction 75 %, receiver
  reports per reported packet 1.0 (5 of 10-11 held) -- baseline 50 % [33-67] at 5.61
  [5.15-10.62], reported 50 %.
- `zero_hop` x2: PASS 8/8 and 8/8 at 2.86 / 3.07 B/B, reports per reported packet 1.0 (baseline
  100 % [88-100] at 2.72 [2.71-3.59]).

Hardware (zero hop, laptop listener + desktop sender, 12 x 495-byte packets = 48 raw fragments in
12 windows, `fieldtests/raw/Alpha0.1.6-bench/a_capture_smci-m5-listener_*` and
`afipc_capture_smci-m5-sender_*20260922T004703`): 12 of 12 delivered in round 0, no re-sent
fragment, 12 receiver reports for 12 packets (1.00 per reported packet, all immediate on the
flagged last fragment), every sender window `reported` with a 0.45-0.74 s report wait.

### Item 5 -- calibration line and capture hygiene (commit 18059a2)

What changed: `field_ab_compare.py` computes the calibration per capture file and sums (the
interface's counters restart with the process, the firmware's run on -- the laptop's 0.56 was
mostly this: four files in the session) and prints two ratios, RAW and CORRECTED: (radio frames
sent - frames keyed) priced at an ACK's airtime (8 bytes on air at `--radio` SF,BW,CR, default
7,62.5,8: 0.14 s) and taken out of the firmware seconds; the printout says which is which and
which to read on a receiver. `fieldtests/AB_PROTOCOL.md`: rnsd started with its output redirected
to a log file (RNS block-buffers it), and the two ratios explained.

On the 2026-09-21 captures: laptop raw 0.92, corrected 0.98; desktop raw 0.93, corrected 1.00.
On the bench (real `radio_stats`): sender 36.6 s estimated / 37 s firmware (raw 0.99; 54 radio
frames against 52 keyed, corrected 1.00); receiver 6.2 / 7 (raw 0.89; 22 against 16, six ACKs,
corrected 1.01). The estimator is calibrated and is not changed.

Tests: `tests/test_calibration_summary_0922.py` (6 tests). MeshBench cannot show this (no
`radio_stats` semantics worth reading in the simulator's firmware counters).

## 3. Final kept set, baseline, version, readme, wire

Kept: all five items (commits 8a21bb9, 66c5514, 24181c1, 18059a2, 5e892bc, 9aa4b7d, 59f383f, 09105ae
[item 1 fourth cut: healthy-path patience], 7c26c7d [close-out]); pushed to origin/development. Version alpha 0.1.6 (module docstring STATUS, readme "Features (Version
alpha0.1.6)", changelog section). Wire versions changed: "Q" completion protocol 4 -> 5 (both
nodes must run alpha 0.1.6). Readme rows: the "New in alpha 0.1.6" list (path selection, the
bounded window hold, the resilient connection, one report per window) and the Path Selection
feature row (replacing Shorter-Path Adoption); the Fragment Reconciliation row says alpha 0.1.6.
Baseline: `tests/baselines/2026-09-22-meshbench-09105ae.md` (see section 6).

Full unit suite: 417 tests OK (`SMCI_SKIP_SLOW` off), after `test_stale_path_reset_within_one_
raw_send` was scoped to the threshold detector (selection off), which it pins.

## 4. Cut or deferred

- The second-reader / port-holder hardware checks (item 4): not run tonight (tooling refusal);
  pinned by unit tests; the field test's laptop, with MeshChat and rnsd both loading the
  interface, is where it will show.
- `weak_direct`'s one-hop expectation: informational under MeshBench (no fading at +3 dB, SNR
  0.0); the scenario stays for the `path_selected` record and the direct path's delivery rate.
- The one-hop gap A/B (`direct_raw_gap_own_airtime`): still not run; the knob and the safety
  signals are in place.
- Deferred by the brief and untouched: no-ACK QUERY, a second parity fragment at three hops,
  bind frames carrying the full key, the mixed-builds fallback.

## 5. Nodes

- Desktop (`afipc`): every `rnsd` ended, `origin/development` pushed (5a625d8..7c26c7d), the final
  build (`Interface/SmartMeshCoreInterface.py` at 09105ae/7c26c7d, md5 aeb6aa14...) installed as
  `~/.reticulum/interfaces/SmartMeshCoreInterface.py` with the 0.1.5 file kept as
  `SmartMeshCoreInterface.py.bak.20260922T105425`, and `rnsd` restarted at 10:54 with
  `nohup rnsd > ~/.reticulum/rnsd-desktop-20260922T105425.log 2>&1 &`; its capture
  `~/.reticulum/storage/packet_capture/afipc_capture_..._20260922T105429.jsonl` shows
  `connection_state` connecting -> open -> online and the radio's stats at start. rnsd (pid
  82515) holds `/dev/ttyUSB0`; MeshChat was left running throughout.
- Laptop (`a`): still on alpha 0.1.5 (`~/.reticulum/interfaces/`, untouched) -- the owner runs the
  update script there. Nothing of mine runs on it; its port was free when I last looked. The
  scratch directory `/tmp/smci016/` (the build, the zero-hop script, its capture and log) could
  not be removed: the laptop became unreachable ("No route to host") at 10:56 -- please
  `rm -rf /tmp/smci016` there when convenient.
- Radio settings, contacts, identities, channel and firmware: untouched. Transmit time tonight:
  about 45 s per radio (12 x 4 raw fragments plus bind/discovery frames at zero hop).

## 6. Baseline suite (final build 09105ae, seeds 7/11/17) against alpha 0.1.5, medians [ranges]

| scenario | alpha 0.1.6 | alpha 0.1.5 | mechanics |
|---|---|---|---|
| zero_hop | 100 % [88-100], 2.85 B/B, RTT 2.8 s | 100 % [88-100], 2.72, 2.8 | PASS x3 |
| relay | 88 % [62-88], 5.79 B/B, RTT 15.0 | 88 % [75-100], 5.96, 14.1 | PASS x3 |
| two_hop | 50 % [50-75], 11.37 B/B [8.6-12.4], RTT 16.8; B h2 attempts 50 % [37-53] | 75 % [62-100], 9.75 [7.2-12.6], 10.9; 85 % [60-95] | PASS x3 |
| large_payload | 33 % [17-50], 6.36 [6.2-7.1] | 50 % [33-67], 5.61 [5.2-10.6] | 2 PASS, 1 floor miss (1/6) |
| page_transfer | 0 % [0-0] | 33 % [0-33] | 3 floor misses (as 0.1.5's 0/3 and 1/3 runs) |
| page_transfer_bidir | 0 % [0-0], 12.1 B/B | 0 % [0-33], 29.5 | 3 floor misses (as 0.1.5) |
| link_setup | 100 % [100-100]; inside 15 s 62 % [50-62]; links 11.6 s | 88 % [75-100]; 62 % [25-75]; 8.8 s | 2 PASS, 1 bring-up (A never resolved a path, 8/8 delivered anyway) |
| duty_cycle_pages | 75 % [50-75], 1.76 B/B, 0 duty waits | 75 %, 1.84 | 2 PASS, 1 floor miss (2/4) |
| shortcut_appears | hard check 2 of 3 seeds | 2 of 3 | seed 11 never left three hops (no one-hop flood copy reached A) |
| weak_direct (new) | 3 of 3 on `path_selected`; direct link delivered 88 % [81-88] | - | PASS x3 (one-hop expectation informational) |

Read with the baseline files' caveats. Two readings I cannot resolve from MeshBench alone and the
field should read first: `two_hop` delivered 50 % on two seeds (below 0.1.5's 62-100 range; ten
two-hop runs across the night's builds delivered 38-100 %, mean 67 %, every one mechanics PASS)
with the responder's two-hop attempt success down (50 % against 85 %); and `large_payload` /
`page_transfer` at the low end of their coin-flip ranges on this suite after the item-3 gate's
6/6 + 6/6. Nothing in this pass touched the two-hop send path itself except the rounds cap
(2 instead of 3 through repeaters) and the path scoreboard; if the field's two-hop attempt
success drops against the 2026-09-21 session's 50 %, `direct_raw_window_max_rounds = 3` and
`path_selection_enabled = no` are the two knobs that isolate them.

## 7. The field test proposed

Both nodes on alpha 0.1.6 (7c26c7d; the desktop is on it, the laptop via the update script --
the "Q" v5 frames are not decoded by 0.1.5), rnsd started with the log redirect (the desktop's
is), capture on, the 2026-09-21 evening route and pages (zero hop by the desk, then the one-,
two- and three-hop stops through the public repeaters, the 12-part page at each). Read, per hop:

1. `path_selected` records on the desktop against its 22:00-22:35 sequence: after the laptop
   drives off, the zero-hop candidate must not be re-selected on its stale record (reason
   "trial" only after two misses on the current path, and the trial goes to the best-scoring
   candidate); a delivering one-hop path must be kept ("current") through single misses; a
   two-hop path must be trialled against the one-hop route once the one-hop route's cooldown
   ends even without a new flood; every record carries the scores -- check the zero-hop
   candidate's `snr` when the laptop is far (the 2026-09-21 ACKs read 2.0 and -1.75 dB) puts
   it on the weak prior (rate 0.25) while untried.
2. Link times and `expired` LRPROOFs: LINKREQUEST -> LRPROOF inside MeshChat's 15 s against 6 of
   16; `ack_timeout_source="superseded"` and `routing_decision="lrproof_superseded"` records
   when MeshChat re-requests (every ~17 s at two hops on 2026-09-21).
3. Lock waits at two hops (`direct_attempt_result.lock_wait_s`): p90 against 27 s, max against
   182 s; raw windows through repeaters run at most 2 rounds (`raw_fragment_sent.round` <= 1).
4. Reports per window at zero hop (`completion_report_sent`, per (sender, pkt_id, round)):
   against 2 (6 of 15 doubled), target 1.0-1.2; `held_s` on every record.
5. The corrected calibration ratio (`field_ab_compare.py --set 016=<dir>`), per node, against
   the bench's 1.00 / 1.01; the RAW one is no longer the number to read on the laptop.
6. `connection_state` records across a laptop suspend / USB drop: disconnected (reason) ->
   retry_wait (5, 10, 20 ... 60 s) -> connecting -> open -> online, and a `port_shared` record
   if MeshChat's RNS and a separate rnsd both load the interface on the laptop (plus the
   "serial stream corrupted" warning if they do).
7. Two-hop attempt success and delivery against the 2026-09-21 session (50 % / the numbers in
   item 6 above) -- the reading MeshBench left open.
8. At last, the one-hop gap A/B (`fieldtests/AB_PROTOCOL.md`, `direct_raw_gap_own_airtime` yes
   vs no on both nodes) with its safety signals (round-1 fragments per part, round-0 re-sends per
   position, parity sent / reconstructed).
