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
- **Start `rnsd` with its output redirected to a log file** (alpha 0.1.6, item 5) -- the level-6
  link lines the LXMF question needs did not exist for the 2026-09-21 session because rnsd's
  output went to a terminal that was gone by the time they were wanted:

      nohup rnsd > ~/.reticulum/rnsd-$(hostname)-$(date +%Y%m%dT%H%M%S).log 2>&1 &

  Note that RNS (Python) block-buffers its output when it is redirected, so the file lags the
  terminal by up to a few kilobytes; the lines are all there once rnsd exits or flushes, so read
  the log after the session, not during it (or start it with `PYTHONUNBUFFERED=1` when watching
  live). Keep the log next to the session's captures.
- The calibration line (`field_ab_compare.py`, "airtime estimator vs the radio's own transmit
  time") prints two ratios since alpha 0.1.6: the RAW `estimate / firmware tx air`, which counts
  the ACKs the radio sends for every ACK-able frame it receives against the interface's estimate
  (the laptop's 2026-09-21 raw ratio read 0.56 for that reason plus interface restarts), and the
  CORRECTED one with the radio's own frames (packet counters minus frames the interface keyed)
  priced as ACKs and taken out -- read the corrected one on a node that receives a lot. Both are
  computed per capture file and summed, because the interface's counters restart with the
  process while the firmware's run on.
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

## The one-hop gap A/B, step by step (alpha 0.1.7, item 5)

Written so it can be run at the next one-hop stop without re-deriving anything. It has never been
run in the field: the 2026-09-22 session's one-hop page rate was gap-bound (about 15 s per part for
2.7 s of airtime, two gaps of 2.73 s each per part), and MeshBench cannot judge the gap (no
listen-before-talk). The knob is `direct_raw_gap_own_airtime`; the interface reads its config at
start, so each arm is a restart of `rnsd`.

1. Both machines on the same build (note the commit), `packet_capture_enabled = yes`, `loglevel = 6`
   in `~/.reticulum/config` on both, MeshChat launched with its log kept (next section).
2. **Arm A** = the default: in the interface block of `~/.reticulum/config` on BOTH nodes,
   `direct_raw_gap_own_airtime = yes` (or the line absent). Start rnsd with its output redirected
   (`nohup rnsd > ~/.reticulum/rnsd-$(hostname)-$(date +%Y%m%dT%H%M%S).log 2>&1 &`), then MeshChat.
   Note the time.
3. Drive to the one-hop stop (check the hop count in the interface log, `path discovered to ... (1
   hops)`, or `hop_count` in the capture; a stop that resolves at zero or two hops is a different
   bucket). Fetch the 12-part page from the laptop three or four times, a minute apart. That is 36-48
   parts, well over the 20 attempts a hop bucket needs.
4. **Arm B**: on BOTH nodes set `direct_raw_gap_own_airtime = no`, stop rnsd and MeshChat on both,
   start them again in the same order (about two minutes), note the time. Same stop, same three or
   four fetches.
5. If time allows, a second pair of arms (A again, then B again) at the same stop: the mesh's
   condition drifts over an evening, and alternating is what makes the comparison fair.
6. Afterwards copy the captures under `fieldtests/raw/<date>-ab-gap/`, both machines' files, and
   compare by time window on that one directory:

       python3 testscripts/field_ab_compare.py \
           --set A=fieldtests/raw/<date>-ab-gap --window A=<armA start>..<armA end> \
           --set B=fieldtests/raw/<date>-ab-gap --window B=<armB start>..<armB end> --radio 7,62.5,8

   Read, at hop 1: the primary number (DIRECT attempt success and the part time), then the "one-hop
   gap A/B safety signals" block -- `gap_s` used (A about 2.7 s, B about 1.8 s at these radio
   settings), round-1 data fragments per part, round-0 re-sends by fragment position (fragment 1
   re-sent more often than fragment 0 under B is the signature of the next fragment leaving inside
   the repeater's relay), and parity fragments sent / reconstructed. The verdict is B's part time
   against A's with the re-send signals not moving the wrong way; a difference inside one arm's own
   spread is not evidence. Record it in `changelog.md` with the directory and the command line.

## MeshChat's own RNS log (alpha 0.1.7, item 5)

The LXMF link question cannot be answered from rnsd's log: MeshChat's LXMF delivery destinations
live in MeshChat's own RNS instance (a client of rnsd over the shared instance), so RNS's
"Validating link request" / "Incoming link request" lines for a link to those destinations go to
MeshChat's stdout, not to rnsd's log. The two unanswered link requests of 2026-09-21 are the case
to look for. To keep that output, launch the AppImage from a terminal with its output redirected,
with `loglevel = 6` under `[logging]` in `~/.reticulum/config` (MeshChat's instance reads the same
config):

    ~/ReticulumMeshChat-v2.4.0-linux.AppImage > ~/.reticulum/meshchat-$(date +%Y%m%dT%H%M%S).log 2>&1 &

Keep that file next to the session's captures and rnsd's log. It is Python output redirected to a
file, so like rnsd's it is block-buffered: read it after MeshChat exits, or launch with
`PYTHONUNBUFFERED=1` in the environment when watching live.

## One two-hop stop (alpha 0.1.7, item 5)

Neither the alpha 0.1.5 evening nor the 0.1.6 session had a two-hop stop with capture on, and the
MeshBench two-hop reading of the 0.1.6 pass needs its field counterpart. At the next session, one
stop that resolves at two hops (`hop_count` 2 in the capture), with capture on, at least one page
fetch and a couple of LXMF messages each way. Read from it: the `path_selected` records (how many
trials, on which candidates, with what SNR and source -- a trial of a one-hop candidate that never
delivers is the phantom-candidate case), the number of raw windows that reached the round cap
(`completion_check_result` at stage `raw1` not complete, followed by a text fallback), reports
per window, and the two-hop attempt success and ACK latency against the 2026-09-19 reference
(51 % / 4.0 s).

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
