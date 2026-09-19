# Changelog

## unreleased (since alpha-0.1.1, 2026-09-18 night)

Field evidence: `fieldtests/raw/Alpha0.1.1/` -- a zero-hop NomadNet page
session and an evening drive through 1-3 repeater hops, both sides
captured. The module docstring's "Alpha 0.1.1 captures" and "Raw binary
DIRECT fragments" entries carry the packet-level detail.

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
`unittest discover` no longer collects it; `SMCI_SKIP_SLOW` is moot), and
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
