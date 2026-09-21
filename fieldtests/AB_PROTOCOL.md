# Field A/B protocol

For decisions MeshBench cannot make. Its virtual radio has no listen-before-talk (a real SX1262
firmware defers while `isReceiving()`; MeshBench v0.1.0 keys straight over the frame), so any
setting that spaces a node's own transmissions where it can hear the other party is punished there
on an artefact: the one-hop raw fragment gap, the post-send listen at zero hop, the answer hold, the
quiet-hold size, the listen ranges. Those are decided in the field, and only by comparing two builds
on the same route with the same traffic. Field tests so far were single sessions of whatever traffic
happened; this is the procedure that makes two of them comparable.

## Setup

- Two builds of `Interface/SmartMeshCoreInterface.py`: **A** (the reference, normally the last
  baseline commit — see `tests/baselines/`) and **B** (the change). Note both commit hashes.
- Both machines (desktop `afipc`, laptop `a`) on the **same** build at any one time: several
  changes are protocol changes (the completion report, the QUERY nonce), and a mixed pair measures
  the fallback, not the change. Swap both ends together.
- `packet_capture_enabled = yes` on both, capturing to a fresh directory per session. Note the
  radio settings once (`SELF_INFO`: 916.575 / 62.5 / SF7 / CR8 on 2026-09-18); the interface does
  not log them and every airtime number depends on them.
- Since alpha 0.1.5 the capture file is named `<label>_capture_<interface>_<stamp>.jsonl`, the
  label being the MeshCore node name (`afipc` on the desktop, `a` on the laptop) unless
  `packet_capture_label` sets one. Check that the laptop's files carry their label before copying
  them into `fieldtests/raw/`; `field_ab_compare.py` keys on it. Two files with the same
  label in one set are two runs of the same machine.
- MeshChat's RNS instance should run with logging at level 6 (`[logging]` / `loglevel = 6` in
  `~/.reticulum/config`, restart MeshChat) on both machines, so RNS's own link-validation lines
  exist alongside the capture: the 2026-09-21 session had two link requests to the desktop's
  LXMF delivery destination handed to RNS and never answered, and without the RNS log there is
  no telling whether RNS refused them or MeshChat never replied. That is RNS/MeshChat behaviour,
  not the interface's, but the A/B verdict should be able to set it aside on evidence.
- One route, driven the same way both times, and one page: the NomadNet page that gives 12 parts of
  483 B (the field's page-transfer class), fetched from the laptop off the desktop's node. A few LXMF
  messages per phase are fine; the page fetch is the unit of comparison.

## Two ways to run it

**Back to back.** Phase A: drive the route on build A, fetch the page at each stop (zero hop by the
desk, then the usual 1–3 hop stops through the public repeaters), N fetches per hop. Swap both ends
to build B, drive the same route the same way, same stops, same N. Compare with

    python3 testscripts/field_ab_compare.py --set A=fieldtests/raw/<session-A> --set B=fieldtests/raw/<session-B>

**Alternated per fetch** (better when the mesh's condition changes over an evening). Stay at one
stop; fetch on A, swap both ends to B, fetch on B, swap back, repeat. Log the swap times. Compare by
time window on one capture directory:

    python3 testscripts/field_ab_compare.py \
        --set A=fieldtests/raw/<session> --window A=19:00..19:20 \
        --set B=fieldtests/raw/<session> --window B=19:20..19:40

Alternating costs a restart per swap (the interface is loaded at `rnsd` / MeshChat start), so plan
~2 minutes per swap and bring the radios up in the same order each time.

## How many

Per hop count you want a verdict at: at least 20 DIRECT attempts per build (`field_ab_compare.py`
flags buckets under `--min-n`, default 20), which is roughly 3–4 twelve-part page fetches at one
hop. Hop 0 accumulates quickly; hops 2–3 need the stops. Note the hop count at each stop from the
interface log (`path discovered to … (N hops)`) or the capture's `hop_count`; the comparison is
per hop, never in aggregate, because every rate here tracks hop count first.

## What is compared

The script prints these side by side, per hop, with sample sizes (the fields of the 2026-09-20
report's section 5):

1. **DIRECT attempt success and ACK latency** (`direct_attempt_result.ok`, `ack_latency_s`
   median / p90) — the primary number. Field reference 2026-09-19: hop 0 92–96 % / 1.3 s, hop 1
   65 % / 3.1 s, hop 2 51 % / 4.0 s, hop 3 42 % / 5.6 s.
2. **Dead waits**: missed-attempt `ack_timeout_s` (≤ 8 s at one hop, ≤ 11 s at two since ef73c8b),
   post-attempt `listen_delay_s` (≤ 1.0 s after a miss), lock waits, quiet-hold totals.
3. **The report replaces the QUERY**: `completion_check_result` outcomes (`reported`,
   `reported_stale`, `answered`, `timeout`), QUERY attempts per raw send (2026-09-19 night: 3.7;
   target under 1), timeout-outcome durations.
4. **Part time**: first `raw_fragment_sent` of a pkt_id → the completion check that knew it
   complete, median / p90 per hop, duty-cycle waits excluded (night session: 34 s median at one hop;
   target ≤ 15 s at one hop, ≤ 6 s at zero).
5. **Link handshakes**: LINKREQUEST out → LRPROOF in, and how many inside MeshChat's 15 s window
   (field: 6.8–11 s at one hop before, 21.8 s with one lost frame).
6. **Backoff**: `unknown_dest_backoff_drop` records, and how many while PROOFs for that destination
   were arriving (should be none since 1b69fa7).
7. **Stale paths**: `direct_send_result` failures and consecutive-failure triples — a reset firing
   under congestion rather than on a dead hop is the regression to look for with tighter caps.
8. **Airtime**: RNS bytes out / in, raw fragment bytes, `channel_fragment_sent`, and the other
   node's `rx_log` TEXT_MSG frames of 38–40 B (QUERY / ANSWER / REPORT class) per raw send.

## The one-hop gap arm (alpha 0.1.5)

The first A/B this procedure is for: **A** = both nodes on the same build with
`direct_raw_gap_own_airtime = yes` (the default: the raw gap through repeaters is
`(1 + 2 x hops) x airtime`), **B** = both nodes with `direct_raw_gap_own_airtime = no` (the
`+1 x airtime` term dropped). Zero hop is unaffected, so the verdict is at one hop and above;
MeshBench cannot judge it (no listen-before-talk, slower frames). Read, from the script's "one-hop
gap A/B safety signals" block per hop: the gap actually used (`gap_s`), round-1 data fragments per
part, round-0 re-sends per fragment position (fragment 1 re-sent more often than fragment 0 is the
signature of the next fragment leaving inside the repeater's relay), and parity fragments sent /
reconstructed. The primary number stays the DIRECT attempt success and the part time.

## Reading it

- A difference inside the run-to-run spread of the same build is not evidence; the MeshBench
  baseline file records that spread for the simulator, and two same-build field sessions
  (`fieldtests/raw/Alpha0.1.2` vs `postAlpha0.1.1`) show the same for the field. Compare medians
  with their p90s and n.
- Third-party traffic on the Broken Hill mesh is < 1 % of channel time; it is not a factor. The
  route and hop mix are — which is why the comparison is per hop.
- When the two columns disagree on the primary number at a hop with n ≥ 20 both sides, and the
  dead-wait and airtime rows move the same way, the change is real. Record the verdict in
  `changelog.md` with the session directories and the command line, and add the session's
  captures under `fieldtests/raw/<date>-ab-<label>/`.
