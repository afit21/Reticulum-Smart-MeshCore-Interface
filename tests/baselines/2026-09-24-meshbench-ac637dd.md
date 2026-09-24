# alpha 0.1.9 second pass, close-out (ac637dd deliverable) on RNS 1.5.4

**Read against** `2026-09-24-meshbench-2b968d0-rns154.md` (alpha 0.1.8 on the same RNS 1.5.4, same
scenarios and seeds). 16 of 16 PASS against the reference's 14 of 16. Delivery and RTT medians are
inside the reference's spread. The time to an RNS path at the sender is bimodal on RNS 1.5.4 (about
25-40 s, or about 90-150 s when the first path request or announce is lost and RNS's longer 1.5 retry
follows). It landed in the high mode more often here (`repeater_returns` 93 and 101 s against 24 and
40 s). In the run read (`repeater_returns-s7`) the scoreboard made no decision before the path formed:
B's direct announce was lost at the relay and A's path requests were answered at 92.6 s. Watch this
number; it is not attributed to this pass.

Scenarios ['failover', 'large_payload', 'relay', 'repeater_returns', 'shortcut_appears', 'two_hop', 'weak_direct', 'zero_hop'], seeds [7, 11], 1 run(s) per seed, interface `/tmp/mb/019b/smci_final.py`, run arguments defaults. Produced by `meshbench_scenarios.py suite`; per-run details in each `<scenario>-s<seed>-<n>/result.json` ("analysis") and `run.log`.

Read with the two caveats every baseline file carries: MeshBench's RF is optimistic and its airtime 1.2-1.45x RadioLib's, its runs are wall-clock driven and not reproducible, and its virtual radio has no listen-before-talk (the LBT-preventable column counts the half-duplex misses a real SX1262 would have deferred). Mechanics are the hard checks; delivery and timing are measured rates -- compare medians and ranges, not single runs.

## Per scenario: medians [min-max] over the runs

| scenario | runs (pass) | seeds | delivered | late | RTT med s | RNS path s | DIRECT attempt rate by hop | ACK med by hop s | reported fraction | half-duplex misses (LBT-prev.) | on-air B/RNS B | links med s (≤deadline) | resources complete / med s / re-sent |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| failover | 2 (2) | 7,11 | 50% [40%–60%] | 0 [0–0] | 12.6 [5.2–20.0] | 33 [29–37] | A h1 17% [0%–35%]; B h1 61% [59%–64%] | A h1 1.7; B h1 2.8 [2.8–2.8] | 0% [0%–0%] | 28 [21–35] (13 [10–16]) | 8.68 [8.56–8.81] | - (-) | - / - / - |
| large_payload | 2 (2) | 7,11 | 58% [33%–83%] | 0 [0–0] | 27.9 [27.3–28.5] | 100 [74–126] | A h1 73% [66%–80%]; B h1 59% [54%–64%] | A h1 1.7 [1.6–1.8]; B h1 2.2 [1.7–2.8] | 26% [21%–31%] | 70 [59–81] (33 [27–39]) | 5.96 [4.94–6.99] | - (-) | - / - / - |
| relay | 2 (2) | 7,11 | 81% [75%–88%] | 0 [0–0] | 8.9 [6.3–11.4] | 92 [73–110] | A h1 63% [60%–67%]; B h1 76% [75%–77%] | A h1 1.7 [1.6–1.7]; B h1 2.7 [2.6–2.9] | 0% [0%–0%] | 35 [26–44] (17 [12–22]) | 6.35 [5.34–7.35] | - (-) | - / - / - |
| repeater_returns | 2 (2) | 7,11 | 67% [67%–67%] | 0 [0–0] | 14.2 [9.9–18.4] | 97 [93–101] | A h1 62% [62%–63%]; B h1 50% [49%–52%] | A h1 2.5 [2.3–2.8]; B h1 2.7 [2.6–2.8] | 0% [0%–0%] | 31 [20–42] (14 [8–21]) | 8.06 [7.67–8.45] | - (-) | - / - / - |
| shortcut_appears | 2 (2) | 7,11 | 15% [10%–20%] | 0 [0–0] | 17.3 [15.4–19.3] | 38 [38–39] | A h1 65% [53%–77%]; A h2 0%; A h3 12% [0%–23%]; B h1 38% [33%–42%]; B h2 62%; B h3 14% [0%–28%] | A h1 2.2 [1.6–2.7]; A h3 4.8; B h1 2.1; B h3 5.0 | 0% [0%–0%] | 64 [63–65] (31 [30–32]) | 23.66 [19.87–27.46] | - (-) | - / - / - |
| two_hop | 2 (2) | 7,11 | 69% [62%–75%] | 0 [0–0] | 21.2 [18.3–24.1] | 90 [33–147] | A h2 56% [50%–62%]; B h2 62% [50%–74%] | A h2 3.0 [2.3–3.6]; B h2 4.3 [4.2–4.4] | 0% [0%–0%] | 40 [36–44] (18 [16–19]) | 12.22 [9.92–14.53] | - (-) | - / - / - |
| weak_direct | 2 (2) | 7,11 | 97% [94%–100%] | 0 [0–0] | 6.1 [3.8–8.4] | 45 [19–71] | A h0 72% [71%–72%]; A h1 100% [100%–100%]; B h0 84% [77%–90%] | A h0 0.9 [0.9–1.0]; B h0 2.1 [1.3–2.8] | 0% [0%–0%] | 43 [36–50] (20 [18–23]) | 3.58 [3.37–3.79] | - (-) | - / - / - |
| zero_hop | 2 (2) | 7,11 | 94% [88%–100%] | 0 [0–0] | 6.8 [2.3–11.3] | 49 [49–50] | B h0 100% | B h0 1.3 | 0% [0%–0%] | 9 [6–12] (4 [3–6]) | 3.30 [2.86–3.75] | - (-) | - / - / - |

## Per run

| run | verdict | probe RTT (RNS packet -> PROOF back) | RNS path s (sender) / time to DIRECT path per node | DIRECT attempts ok/total by hop (median ACK) | A completion checks answered-or-reported / total (rep = reported) | A raw fragments sent (by reconcile round) | MeshBench on-air per node (tx / bytes / s) | top miss reasons | half-duplex misses (LBT-preventable) | on-air B per RNS B |
|---|---|---|---|---|---|---|---|---|---|---|
| failover-s11-1 | PASS 6/10 | min=5.76s avg=21.97s max=54.74s | 37.4 / {'A': 26.5, 'B': 34.0} | A h1:16/46 ack1.7s; B h1:24/41 ack2.8s | 7/27 (rep 0) | 10 ({0: 8, 1: 2}) | A:84tx/6360B/54s; B:88tx/5982B/52s; R1:61tx/3589B/32s; R2:40tx/2764B/24s | R1-half-duplex:14; R2-collision:14; R1-collision:13; R2-locked:11 | 35 (16) | 8.81 |
| failover-s7-1 | PASS 4/10 | min=4.80s avg=8.98s max=20.74s | 29.2 / {'A': 120.0, 'B': 124.0} | A h1:0/20 ack-s; B h1:14/22 ack2.8s | 0/23 (rep 0) | 8 ({0: 8}) | A:61tx/4420B/38s; B:59tx/3985B/35s; R1:28tx/2424B/20s; R2:36tx/2686B/23s | R1-half-duplex:8; R2-locked:5; B-half-duplex:5; A-half-duplex:5 | 21 (10) | 8.56 |
| large_payload-s11-1 | PASS 2/6 | min=24.43s avg=27.30s max=30.16s | 125.5 / {'A': 35.0, 'B': 31.0} | A h1:19/29 ack1.8s; B h1:19/35 ack1.7s | 11/16 (rep 5) | 39 ({0: 24, 1: 15}) | A:93tx/10291B/83s; B:80tx/5738B/50s; R:112tx/9016B/76s | R-half-duplex:40; A-half-duplex:26; B-half-duplex:15; R-locked:12 | 81 (39) | 6.99 |
| large_payload-s7-1 | PASS 5/6 | min=21.02s avg=33.30s max=47.52s | 74.0 / {'A': 48.0, 'B': 38.5} | A h1:8/10 ack1.6s; B h1:18/28 ack2.8s | 7/14 (rep 3) | 36 ({0: 24, 1: 12}) | A:72tx/7862B/63s; B:52tx/4273B/36s; R:82tx/7196B/60s | R-half-duplex:31; A-half-duplex:17; B-half-duplex:11; R-locked:8 | 59 (27) | 4.94 |
| relay-s11-1 | PASS 7/8 | min=5.08s avg=10.24s max=17.77s | 73.2 / {'A': 48.0, 'B': 75.0} | A h1:6/10 ack1.7s; B h1:12/16 ack2.6s | 3/13 (rep 0) | 10 ({0: 8, 1: 2}) | A:43tx/3088B/27s; B:39tx/3320B/28s; R:57tx/4252B/36s | R-half-duplex:13; B-half-duplex:7; A-half-duplex:6; R-locked:6 | 26 (12) | 5.34 |
| relay-s7-1 | PASS 6/8 | min=5.03s avg=11.44s max=17.72s | 110.0 / {'A': 83.0, 'B': 58.5} | A h1:14/21 ack1.6s; B h1:17/22 ack2.9s | 7/14 (rep 0) | 12 ({0: 8, 1: 4}) | A:60tx/4791B/41s; B:58tx/4217B/36s; R:80tx/5277B/46s | R-half-duplex:22; A-half-duplex:12; B-half-duplex:10; R-locked:9 | 44 (22) | 7.35 |
| repeater_returns-s11-1 | PASS 8/12 | min=8.65s avg=21.45s max=45.22s | 101.0 / {'A': 80.5, 'B': 58.5} | A h1:27/43 ack2.3s; B h1:17/35 ack2.6s | 3/8 (rep 0) | 1 ({0: 1}) | A:96tx/7457B/63s; B:84tx/5997B/52s; R:111tx/7028B/62s | R-half-duplex:21; B-half-duplex:12; A-half-duplex:9; R-locked:9 | 42 (21) | 8.45 |
| repeater_returns-s7-1 | PASS 8/12 | min=7.89s avg=16.72s max=44.37s | 92.6 / {'A': 31.5, 'B': 38.0} | A h1:24/39 ack2.8s; B h1:17/33 ack2.8s | 2/7 (rep 0) | 1 ({0: 1}) | A:84tx/6446B/55s; B:80tx/6036B/52s; R:98tx/6102B/54s | R-collision:12; R-half-duplex:11; R-locked:9; B-half-duplex:5 | 20 (8) | 7.67 |
| shortcut_appears-s11-1 | PASS 1/10 | min=15.38s avg=15.38s max=15.38s | 38.0 / {'A': 157.0, 'B': 300.5} | A h1:10/13 ack1.6s h3:0/16 ack-s; B h1:1/3 ack-s h2:13/21 ack-s h3:0/6 ack-s | 1/30 (rep 0) | 10 ({0: 10}) | A:64tx/4711B/41s; B:83tx/5808B/50s; R1:104tx/7029B/61s; R2:87tx/5648B/49s; R3:68tx/5257B/45s | R2-snr:28; R1-half-duplex:21; R2-half-duplex:19; A-snr:16 | 65 (32) | 27.46 |
| shortcut_appears-s7-1 | PASS 2/10 | min=9.20s avg=19.30s max=29.40s | 39.0 / {'A': 137.5, 'B': 128.5} | A h1:10/19 ack2.7s h2:0/2 ack-s h3:7/30 ack4.8s; B h1:8/19 ack2.1s h3:5/18 ack5.0s | 6/16 (rep 0) | 5 ({0: 4, 1: 1}) | A:93tx/7314B/62s; B:88tx/6754B/58s; R1:110tx/7442B/65s; R2:68tx/5080B/43s; R3:70tx/5175B/44s | R2-snr:27; R1-half-duplex:21; R1-locked:17; A-half-duplex:15 | 63 (30) | 19.87 |
| two_hop-s11-1 | PASS 6/8 | min=8.30s avg=18.87s max=31.56s | 147.1 / {'A': 80.5, 'B': 125.5} | A h2:10/16 ack2.3s; B h2:14/19 ack4.2s | 4/16 (rep 0) | 11 ({0: 8, 1: 3}) | A:55tx/4000B/34s; B:56tx/4911B/41s; R1:80tx/5614B/49s; R2:82tx/5958B/51s | R1-half-duplex:14; A-snr:13; R2-half-duplex:10; R2-locked:10 | 36 (16) | 9.92 |
| two_hop-s7-1 | PASS 5/8 | min=9.84s avg=27.73s max=46.69s | 33.0 / {'A': 130.0, 'B': 125.0} | A h2:16/32 ack3.6s; B h2:12/24 ack4.4s | 5/9 (rep 0) | 3 ({0: 2, 1: 1}) | A:62tx/5020B/42s; B:66tx/4847B/42s; R1:90tx/5954B/52s; R2:91tx/6187B/54s | R2-half-duplex:16; R1-half-duplex:16; A-snr:10; R2-collision:10 | 44 (19) | 14.53 |
| weak_direct-s11-1 | PASS 16/16 | min=2.24s avg=9.90s max=24.55s | 70.8 / {'A': 66.0, 'B': 27.0} | A h0:26/36 ack1.0s h1:1/1 ack-s; B h0:24/31 ack1.3s | 8/19 (rep 0) | 13 ({0: 9, 1: 4}) | A:87tx/6733B/57s; B:80tx/4923B/44s; R:23tx/1633B/14s | B-half-duplex:19; R-half-duplex:16; A-half-duplex:15; B-snr:7 | 50 (23) | 3.37 |
| weak_direct-s7-1 | PASS 15/16 | min=3.35s avg=6.04s max=21.19s | 19.4 / {'A': 600.1, 'B': 26.0} | A h0:5/7 ack0.9s h1:1/1 ack-s; B h0:18/20 ack2.8s | 3/21 (rep 0) | 18 ({0: 16, 1: 2}) | A:70tx/5325B/45s; B:58tx/4954B/42s; R:67tx/5449B/46s | R-half-duplex:16; B-half-duplex:10; A-half-duplex:10; A-locked:6 | 36 (18) | 3.79 |
| zero_hop-s11-1 | PASS 7/8 | min=3.10s avg=13.50s max=21.64s | 49.2 / {'A': 237.0, 'B': 56.5} | A -; B h0:3/3 ack1.3s | 0/1 (rep 0) | 1 ({0: 1}) | A:53tx/4593B/38s; B:27tx/2996B/24s | B-half-duplex:6; A-half-duplex:6 | 12 (6) | 3.75 |
| zero_hop-s7-1 | PASS 8/8 | min=2.34s avg=7.76s max=38.78s | 49.6 / {'A': 108.0, 'B': 56.5} | A -; B - | 0/6 (rep 0) | 6 ({0: 6}) | A:28tx/2884B/23s; B:26tx/3152B/25s | B-half-duplex:3; A-half-duplex:3 | 6 (3) | 2.86 |

## Verdicts

- failover-s11-1: exit 0 (PASS)
- failover-s7-1: exit 0 (PASS)
- large_payload-s11-1: exit 0 (PASS)
- large_payload-s7-1: exit 0 (PASS)
- relay-s11-1: exit 0 (PASS)
- relay-s7-1: exit 0 (PASS)
- repeater_returns-s11-1: exit 0 (PASS)
- repeater_returns-s7-1: exit 0 (PASS)
- shortcut_appears-s11-1: exit 0 (PASS)
- shortcut_appears-s7-1: exit 0 (PASS)
- two_hop-s11-1: exit 0 (PASS)
- two_hop-s7-1: exit 0 (PASS)
- weak_direct-s11-1: exit 0 (PASS)
- weak_direct-s7-1: exit 0 (PASS)
- zero_hop-s11-1: exit 0 (PASS)
- zero_hop-s7-1: exit 0 (PASS)

## Per-part bursts (pkt: round-0 sent/landed, rounds, first fragment -> known complete s)

- large_payload-s11-1: 1: 4/1, 2r, None; 3: 4/2, 2r, 53.1; 4: 4/2, 2r, 36.6; 5: 4/2, 2r, 30.1; 6: 4/2, 2r, 36.9; 7: 4/2, 2r, 24.4
- large_payload-s7-1: 2: 4/1, 2r, 46.3; 3: 4/2, 2r, 38.9; 4: 4/2, 2r, 21.0; 5: 4/2, 2r, 28.4; 6: 4/2, 2r, 22.3; 7: 4/1, 2r, 47.5
