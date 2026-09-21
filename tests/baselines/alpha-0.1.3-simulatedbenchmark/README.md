# alpha-0.1.3 — MeshBench simulated benchmark (2026-09-20)

The reference point for the development branch as it stood on the evening of 2026-09-20: interface
`Interface/SmartMeshCoreInterface.py` at commit `1b69fa7` (unchanged since; the working tree's later
commits touch only tests and tooling — this is the build the same day's field session in
`fieldtests/raw/Alpha0.1.3/` ran on), run through every scenario of the expanded MeshBench suite over
three seeds. It supersedes `../2026-09-20-meshbench-1b69fa7.md` as the file to compare a change against,
and it is the first baseline produced by one command:

    python3 testscripts/meshbench_scenarios.py suite --scenarios zero_hop,relay,two_hop,large_payload,\
        page_transfer,page_transfer_bidir,link_setup,bring_up,many_peers,mixed_builds,three_hop,duty_cycle_pages \
        --seeds 7,11,17 --parallel 3 --out-dir /tmp/mb/alpha-0.1.3 \
        --write-baseline tests/baselines/alpha-0.1.3-simulatedbenchmark/summary.md

Files here:

- `summary.md` — the generated tables: per scenario medians with [min–max] over the three seeds, one
  row per run, verdicts, per-part bursts. Every column is defined in `testscripts/meshbench_report.py`.
- `summary.json` — the same, machine-readable (`aggregate` and `per_run`).
- `runs/<scenario>-s<seed>-1/result.json` — each run's measurements and embedded analysis (the
  captures and MeshBench event logs themselves stayed under `/tmp/mb/alpha-0.1.3/`, 36 runs).

Unit suite at this tree: `SMCI_SKIP_SLOW=1 python3 -m unittest discover -s tests` 199 tests OK (66 s);
`python3 -m unittest discover -s tests` 199 tests OK (335 s, with two MeshBench runs sharing the CPU).

Setup: MeshBench v0.1.0, native MeshCore v1.17.1 firmware, radios 916.575 MHz / 62.5 kHz / SF7 / CR8,
production interface timing, scenario defaults, three runs at a time on the desktop. Seeds **7, 11, 17**
— seed 13 was run first and discarded (see "Seed 13" below).

## Headlines, against the previous baseline (`1b69fa7`, seeds 7 only)

| scenario | 2026-09-20 midday baseline (1–4 runs) | this file (3 seeds, median [range]) | reading |
|---|---|---|---|
| zero_hop | 7–8/8, RTT avg 5–8 s, attempt success ~100 %, 86–100 % reported | 100 % [88–100 %] delivered, RTT med 3.6 s, attempts 88 % / 85 %, 86 % [43–100 %] reported, 15 half-duplex misses (7 LBT-preventable) | same |
| relay | 8/8, 8/8; RTT avg 8.6–12 s; 7–8 of 9–12 checks reported | 100 % [88–100 %], RTT med 12.6 s, attempts 73 % / 73 %, 73 % [47–100 %] reported | same |
| two_hop | bring-up dominated: DIRECT path at 138–543 s or never; 4–5/8 | with the start gate: 75 % [62–88 %] delivered *on the two-hop DIRECT path*, attempts 67 % / 76 %, ACK 2.9 / 3.3 s; time to DIRECT path 86–125 s in two runs, 674 s in one | now measures what it says |
| large_payload | 1–6/6 (that wide), RTT avg 30–42 s | 17 % [0–33 %] delivered, RTT med 39 s; per part 1–3 of 4 round-0 fragments land | low end of the recorded spread; see the LBT note |
| bring_up (new) | — | time to a DIRECT path 120 / 136 / 289 s (A) and 126 / 125 / 304 s (B); RNS path 30–59 s; 1–2 path requests | the two_hop coin flip, measured |
| three_hop (new) | — | 2 of 3 runs reached three hops (gate timed out at 600 s, paths at 610–800 s); 88 % and 25 % delivered; attempts 39 % / 47 % at hop 3, ACK 3.7 / 4.0 s | field 2026-09-19 at hop 3: 42 %, 5.6 s |
| link_setup (new) | — | handshakes median 22 s [14–24], 38 % [25–62 %] inside 15 s, attempts 46 % / 36 % at one hop | field zero hop 3–9 s; field two hops 14–17 s |
| page_transfer (new) | — | 12-part page at one hop: 0 % [0–67 %] complete; the one that finished took 464 s; the rest timed out at 600 s | MeshBench's LBT artefact at its worst; field one-hop parts 13–34 s |
| page_transfer_bidir (new) | — | 0/3 in every run; both ends' handshakes 49–54 s; the Link itself died mid-transfer twice | see below |
| duty_cycle_pages (new) | — | zero-hop 12-part pages: 3 of 4 complete in every run, 162–261 s each; the first attempt of each run fails (Link 90 s timeout before any DIRECT path exists, or the page timing out at 600 s); duty-cycle waits on 21–40 fragments per run, median 2.2–2.7 s, max 10–13 s | field zero-hop pages ≈ 1–1.5 min |
| many_peers (new) | — | 0/3: the sender bound 1–3 of 4 peers, never left small-mesh mode, and the RNS path failed in two runs | a real finding, see below |
| mixed_builds (new) | — | responder on `d7dcba9` (no completion report): 2 of 3 PASS, 5/6 and 5/6 delivered, 0 % reported as expected; one run 0/6 with the DIRECT path reset by the end | degrades to QUERY as designed |

Timing mechanics held in every run: missed-attempt `ack_timeout_s` 5 / 8 / 11 / 14 s at 0–3 hops,
post-miss listen ≤ 1 s, completion timeouts ≤ 6 s (10 s beyond one hop), zero `unknown_dest_backoff_drop`.

## Which FAILs are what

36 runs, 21 exit 0. The 15 exit 2 split as:

- **Delivery floors under the LBT artefact** — `page_transfer` ×2, `page_transfer_bidir` ×3,
  `large_payload` ×2, `mixed_builds` ×1. These are the measured rate falling under a floor that was set
  from field expectations; MeshBench's virtual radio keys straight over frames it is receiving (a real
  SX1262 defers), so every raw burst at one hop loses roughly half its fragments at the sender's own
  repeater (`lbt_preventable` is 40–50 % of all half-duplex misses in these runs, 129–174 misses per
  page-transfer run). Treat the *relative* numbers as the reference, not the floors. The `page_transfer`
  floor (50 %) and `page_transfer_bidir` floor (34 %) are unreachable in MeshBench v0.1.0 as it stands;
  they stay as written so a MeshBench with LBT would show up as an improvement, and a run that beats them
  here would be remarkable.
- **"resolved DIRECT path {}" at the end** — `large_payload-s17`, `mixed_builds-s17`,
  `page_transfer-s17`, `page_transfer_bidir-s7/s11`: the DIRECT path existed and was reset by the
  stale-path detector after the run's own failure streak (three consecutive DIRECT failures under the
  LBT losses), then not rediscovered before the run ended. Mechanics-check FAILs by definition, but they
  are the delivery collapse's consequence, not a separate defect; in the field the same detector fired
  once in the two-hop phase and rediscovered within minutes.
- **`three_hop-s11`** — the RNS path (announce over CHANNEL through three repeaters) never arrived
  in 420 s, so nothing was sent. The other two seeds took 610–800 s to a DIRECT path: three hidden hops
  are at the edge of what one advert per minute brings up in this simulator.
- **`many_peers` ×3** — the only FAIL set that is neither LBT nor bring-up luck; see below.

## Findings

**many_peers: bind discovery did not reach four peers.** Five companions within a kilometre, each with
its own RNS node. The sender bound 2, 1 and 3 peers (seeds 7, 11, 17) in 7–12 minutes, never left
small-mesh mode, and in two runs never even received the responder's RNS announce (path request timed
out at 420 s while 11 path requests went out). Bind REQUESTs, RESPONSEs, five nodes' announces and
adverts all share the zero-hop channel; without listen-before-talk in the simulator most of them
collide (160–217 half-duplex misses per run, 54–83 LBT-preventable), and the interface's re-request
schedule slows once 3 peers are bound (`peer_discovery_target_peers`). So part of this is the artefact —
but every field capture so far has two nodes, so the CHANNEL / supplement routing above three bound
peers has still never run against firmware, and this scenario says a five-node zero-hop bring-up is
not quick. The first thing to try is a field or bench test with three radios.

**page_transfer_bidir: the Link dies.** In two of three runs both directions' Resources failed at the
same instant (`elapsed_s` 212.9 and 369.8 s for every resource of the run): the RNS Link timed out and
tore everything down, after the second handshake had itself taken 49–54 s. Under the LBT artefact the
two ends' raw bursts, reports and Resource requests collide at the repeater continuously (216 half-duplex
misses, 98 LBT-preventable per run, DIRECT attempt success 24–32 %). Direction and geometry match the
2026-09-19 night regression; magnitude does not — the field's bidirectional zero-hop session the same
day completed its pages, with 38–57 s slot waits as its worst symptom. The in-flight cap's slot waits
are the number to watch in both.

**duty_cycle_pages: the limiter and the in-flight cap are the zero-hop ceiling.** 12 parts in
162–261 s at zero hop. Per run the sender's summed waits were: slot waits (the in-flight cap, summed
over the parts queued behind it) 379–925 s, lock waits 324–486 s, duty-cycle waits 105–163 s on 21–40
fragments (median 2.2–2.7 s, max 10–13 s), against 750–1320 s of traffic; the field's zero-hop page the
same day took about a minute, with 38–57 s slot waits as its worst symptom too. A third of the gap is
MeshBench's airtime (1.2–1.45×), the rest LBT and the 30 %/60 s policy compounding. The scenario also
shows the first attempt of a run failing every time: the LINKREQUEST goes out before any DIRECT path
exists, as a CHANNEL bootstrap, and either its LRPROOF never comes back inside 90 s or the page that
follows times out at 600 s.

**Seed 13 gives node A a reserved identity.** MeshCore refuses public keys whose first byte is 0x00 or
0xFF (they are the path hash on air); MeshBench's seeded identity generation does not, and seed 13
gave A the key `0032090d…` in all twelve topologies. B's firmware held A as a contact, yet path
discovery to A never answered and no DIRECT frame was ever exchanged — eleven runs of a node that no
real radio can be. They are kept under `/tmp/mb/alpha-0.1.3-seed13-reserved-identity/` for the record
and replaced by seed 17 here; every run now checks each node's `_main.id` after the firmware starts and
stops with exit 3 on a reserved prefix.

## Caveats (read before comparing anything to this file)

1. MeshBench's RF is optimistic, its airtime 1.2–1.45× RadioLib's, and its virtual radio has no
   listen-before-talk. Zero- and one-hop delivery and timing here are **ceilings on how bad it can be**,
   not predictions; two hops and beyond are roughly field-like (compare `three_hop`'s 39–47 % attempt
   success with the field's 42 % at hop 3).
2. Runs are wall-clock driven and not reproducible; the [min–max] next to every median is the same
   tree's spread. A change has to move a median outside that range, across seeds, to count.
3. Mechanics are the hard checks. Delivery floors were set from field expectations and several are
   unreachable in this simulator (see above); a floor FAIL is a prompt to read the analysis, a mechanics
   FAIL (topology, hop count, repeater relayed, bound peers, reserved identity) is a regression.
4. `three_hop`'s and `two_hop`'s probes start only once both ends have a DIRECT path (start gate, 600 s);
   `bring_up` measures bring-up on its own. The gate timing out is reported, not failed.

## Comparing a change

    SMCI_SKIP_SLOW=1 python3 -m unittest discover -s tests
    python3 testscripts/meshbench_scenarios.py suite --scenarios quick --seeds 7 --parallel 3 \
        --out-dir /tmp/mb/quick-$(git rev-parse --short HEAD)          # ~15 min: zero_hop, relay, large_payload
    python3 testscripts/meshbench_scenarios.py suite --scenarios all --seeds 7,11,17 --parallel 3 \
        --out-dir /tmp/mb/<label> --write-baseline tests/baselines/<label>/summary.md   # ~4 h, the full set

Then `summary.md` against this directory's, medians and ranges per scenario. Field figures to hold
both against: `fieldtests/raw/Alpha0.1.3/` (this build, 2026-09-20: zero hop 91–98 % attempt success,
0.9 s ACK, 5–7 s part time, handshakes 3–9 s; two hops 51–59 %, 3.4–4.6 s ACK, handshakes 14–17 s).
