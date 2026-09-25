# pass 1 (beee499 deliverable) on RNS 1.5.4: echo deadline, tx-path labels, ANSWER report class

**Read against** `2026-09-24-meshbench-ac637dd.md` (alpha 0.1.9 second pass, same RNS 1.5.4, same eight
scenarios on seeds 7 and 11), plus `three_hop` and `page_transfer_bidir`. 16 of 20 PASS on the first
suite. Two seed-7 FAILs were bring-up, not this pass: `large_payload-s7` never bound its peer (A's
`_peers` stayed empty, every send a CHANNEL bootstrap) and `shortcut_appears-s7` never reached its
probes; both PASSed on re-run (`large_payload-s7` 4/6, `shortcut_appears-s7` 4/10 with the shortcut
adopted) -- the re-run rows are at the end. `page_transfer_bidir` FAILed both seeds (0/3), as it has on
every build since alpha 0.1.7 (`2026-09-23-meshbench-6a3d38c.md` 0 of 3 PASS, `2b968d0` 0 of 2,
delivered 0-33 %); links inside MeshChat's 15 s went 0 % -> 50 % [0-100 %]. Against ac637dd: `two_hop`
delivered 81 % [75-88] vs 69 % [62-75] with RTT median 9.1 s vs 21.2 s; `relay` 100 % vs 81 %;
`repeater_returns` 54 % [50-58] vs 67 %, RTT 5.8 vs 14.2 s; the rest inside the reference's spread.
Two runs per scenario: a direction, not a measurement. The echo deadline ended 79 missed-ACK waits
(1 hop 50, 2 hops 6, 3 hops 23), about 137 s against the hop cap; none of those attempts saw its ACK
on air inside the post-attempt listen window (a weak check: the window closes soon after).

Scenarios ['failover', 'large_payload', 'relay', 'repeater_returns', 'shortcut_appears', 'two_hop', 'weak_direct', 'zero_hop', 'three_hop', 'page_transfer_bidir'], seeds [7, 11], 1 run(s) per seed, interface `/tmp/mb/pass1/smci_beee499.py`, run arguments defaults. Produced by `meshbench_scenarios.py suite`; per-run details in each `<scenario>-s<seed>-<n>/result.json` ("analysis") and `run.log`.

Read with the two caveats every baseline file carries: MeshBench's RF is optimistic and its airtime 1.2-1.45x RadioLib's, its runs are wall-clock driven and not reproducible, and its virtual radio has no listen-before-talk (the LBT-preventable column counts the half-duplex misses a real SX1262 would have deferred). Mechanics are the hard checks; delivery and timing are measured rates -- compare medians and ranges, not single runs.

## Per scenario: medians [min-max] over the runs

| scenario | runs (pass) | seeds | delivered | late | RTT med s | RNS path s | DIRECT attempt rate by hop | ACK med by hop s | reported fraction | half-duplex misses (LBT-prev.) | on-air B/RNS B | links med s (≤deadline) | resources complete / med s / re-sent |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| failover | 2 (2) | 7,11 | 60% [60%–60%] | 0 [0–0] | 7.8 [5.9–9.8] | 64 [52–76] | A h1 28% [19%–38%]; B h1 59% [53%–65%] | A h1 1.5 [1.4–1.7]; B h1 2.7 [2.6–2.8] | 0% [0%–0%] | 26 [18–34] (12 [8–16]) | 7.54 [7.10–7.98] | - (-) | - / - / - |
| large_payload | 2 (1) | 7,11 | 58% [33%–83%] | 0 [0–0] | 34.3 [29.8–38.8] | 24 [24–25] | A h1 100% [100%–100%]; B h1 53% [33%–73%] | A h1 1.8; B h1 2.8 [2.7–2.8] | 22% | 44 [42–45] (21 [20–22]) | 9.57 [4.58–14.56] | - (-) | - / - / - |
| page_transfer_bidir | 2 (0) | 7,11 | 0% [0%–0%] | 0 [0–0] | - | 52 [25–79] | A h1 63% [62%–64%]; B h1 69% [61%–77%] | A h1 2.5 [1.8–3.3]; B h1 2.6 [1.7–3.4] | 40% [28%–53%] | 150 [55–246] (68 [26–111]) | 6.36 [6.13–6.60] | 18.4 [10.2–26.5] (50% [0%–100%]) | 0% [0%–0%] / - / 0 [0–0] |
| relay | 2 (2) | 7,11 | 100% [100%–100%] | 0 [0–0] | 7.3 [5.7–8.9] | 61 [22–101] | A h1 68% [50%–85%]; B h1 83% [67%–100%] | A h1 2.0 [1.7–2.3]; B h1 3.0 [2.8–3.2] | 0% [0%–0%] | 22 [14–30] (10 [7–12]) | 5.21 [4.13–6.30] | - (-) | - / - / - |
| repeater_returns | 2 (2) | 7,11 | 54% [50%–58%] | 0 [0–0] | 5.8 [5.6–5.9] | 74 [69–78] | A h1 46% [42%–50%]; B h1 53% [50%–56%] | A h1 1.5 [1.5–1.6]; B h1 2.7 [2.7–2.7] | 0% [0%–0%] | 35 [31–39] (16 [15–16]) | 7.19 [7.12–7.26] | - (-) | - / - / - |
| shortcut_appears | 2 (1) | 7,11 | 30% | 0 [0–0] | 11.7 | 32 | A h1 90% [80%–100%]; A h3 64% [28%–100%]; B h1 0%; B h2 0%; B h3 47% [42%–52%] | A h3 3.2; B h3 3.6 [3.5–3.8] | 0% | 56 [38–74] (26 [19–34]) | 35.07 [23.41–46.74] | - (-) | - / - / - |
| three_hop | 2 (2) | 7,11 | 81% [62%–100%] | 0 [0–0] | 24.4 [12.3–36.5] | 39 [39–40] | A h3 54% [53%–56%]; B h3 65% [60%–70%] | A h3 3.0 [3.0–3.0]; B h3 6.2 [6.2–6.2] | 0% [0%–0%] | 32 [26–39] (16 [13–19]) | 13.54 [10.91–16.18] | - (-) | - / - / - |
| two_hop | 2 (2) | 7,11 | 81% [75%–88%] | 0 [0–0] | 9.1 [8.9–9.4] | 137 [135–138] | A h1 60% [20%–100%]; A h2 83% [67%–100%]; B h2 68% [57%–79%] | A h1 2.8; A h2 2.3 [2.2–2.4]; B h2 4.5 [4.5–4.5] | 0% [0%–0%] | 27 [24–30] (14 [12–15]) | 10.44 [9.85–11.02] | - (-) | - / - / - |
| weak_direct | 2 (2) | 7,11 | 94% [88%–100%] | 0 [0–0] | 6.6 [2.2–11.0] | 90 [90–91] | A h0 78%; A h1 90% [79%–100%]; B h0 67% [59%–76%] | A h0 1.0; A h1 2.5; B h0 1.3 [1.3–1.3] | 0% [0%–0%] | 61 [40–82] (28 [19–38]) | 3.87 [2.96–4.78] | - (-) | - / - / - |
| zero_hop | 2 (2) | 7,11 | 94% [88%–100%] | 0 [0–0] | 6.9 [2.3–11.4] | 51 [51–51] | A h0 42% [33%–50%]; B h0 73% [67%–80%] | A h0 0.8 [0.8–0.9]; B h0 1.3 [1.3–1.3] | 0% [0%–0%] | 13 [12–14] (6 [5–6]) | 3.20 [2.97–3.43] | - (-) | - / - / - |

## Per run

| run | verdict | probe RTT (RNS packet -> PROOF back) | RNS path s (sender) / time to DIRECT path per node | DIRECT attempts ok/total by hop (median ACK) | A completion checks answered-or-reported / total (rep = reported) | A raw fragments sent (by reconcile round) | MeshBench on-air per node (tx / bytes / s) | top miss reasons | half-duplex misses (LBT-preventable) | on-air B per RNS B |
|---|---|---|---|---|---|---|---|---|---|---|
| failover-s7-1 | PASS 6/10 | min=4.64s avg=12.21s max=44.19s | 52.0 / {'A': 108.5, 'B': 57.5} | A h1:3/16 ack1.7s; B h1:11/17 ack2.8s | 0/21 (rep 0) | 9 ({0: 9}) | A:57tx/4016B/35s; B:50tx/3800B/32s; R1:40tx/2659B/23s; R2:26tx/2276B/19s | R1-half-duplex:9; B-half-duplex:7; R2-locked:6; R2-collision:5 | 18 (8) | 7.10 |
| failover-s11-1 | PASS 6/10 | min=4.75s avg=13.02s max=31.21s | 75.8 / {'A': 30.0, 'B': 37.5} | A h1:11/29 ack1.4s; B h1:18/34 ack2.6s | 3/25 (rep 0) | 12 ({0: 10, 1: 2}) | A:73tx/4487B/40s; B:69tx/5614B/47s; R1:40tx/2995B/26s; R2:43tx/3332B/28s | R2-locked:11; R1-locked:11; R2-half-duplex:10; B-half-duplex:8 | 34 (16) | 7.98 |
| large_payload-s11-1 | PASS 5/6 | min=16.81s avg=26.88s max=34.09s | 25.0 / {'A': 33.5, 'B': 29.5} | A h1:2/2 ack1.8s; B h1:8/11 ack2.7s | 4/9 (rep 2) | 30 ({0: 20, 1: 10}) | A:57tx/7654B/60s; B:27tx/2320B/19s; R:57tx/6190B/50s | R-half-duplex:21; A-half-duplex:16; B-half-duplex:5; R-locked:3 | 42 (20) | 4.58 |
| large_payload-s7-1 | FAIL 2/6 | min=35.18s avg=38.78s max=42.38s | 23.6 / {'B': 29.5} | A h1:1/1 ack-s; B h1:2/6 ack2.8s | 0/0 (rep 0) | 0 ({}) | A:72tx/9870B/77s; B:23tx/2137B/18s; R:58tx/7646B/60s | R-half-duplex:23; A-half-duplex:16; R-locked:7; R-collision:7 | 45 (22) | 14.56 |
| relay-s11-1 | PASS 8/8 | min=4.80s avg=12.10s max=34.01s | 22.0 / {'A': 48.5, 'B': 29.0} | A h1:1/2 ack1.7s; B h1:8/8 ack3.2s | 1/9 (rep 0) | 8 ({0: 7, 1: 1}) | A:31tx/2590B/22s; B:21tx/2030B/17s; R:44tx/3874B/32s | R-half-duplex:7; A-half-duplex:5; B-half-duplex:2 | 14 (7) | 4.13 |
| relay-s7-1 | PASS 8/8 | min=8.20s avg=14.61s max=41.94s | 100.6 / {'A': 37.0, 'B': 32.0} | A h1:23/27 ack2.3s; B h1:16/24 ack2.8s | 3/3 (rep 0) | 2 ({0: 1, 1: 1}) | A:55tx/3935B/34s; B:65tx/4394B/38s; R:92tx/6001B/52s | R-half-duplex:16; B-half-duplex:7; A-half-duplex:7; R-collision:6 | 30 (12) | 6.30 |
| repeater_returns-s11-1 | PASS 7/12 | min=5.22s avg=7.68s max=17.75s | 78.4 / {'A': 34.5, 'B': 37.5} | A h1:8/19 ack1.5s; B h1:15/27 ack2.7s | 4/21 (rep 0) | 13 ({0: 11, 1: 2}) | A:68tx/4897B/42s; B:59tx/4975B/42s; R:74tx/5530B/47s | R-half-duplex:15; B-half-duplex:9; A-half-duplex:7; R-locked:4 | 31 (15) | 7.26 |
| repeater_returns-s7-1 | PASS 6/12 | min=5.29s avg=12.96s max=28.58s | 69.0 / {'A': 31.0, 'B': 35.0} | A h1:11/22 ack1.6s; B h1:14/28 ack2.7s | 5/21 (rep 0) | 12 ({0: 11, 1: 1}) | A:71tx/4844B/42s; B:66tx/5485B/46s; R:81tx/5750B/50s | R-half-duplex:20; A-half-duplex:10; B-half-duplex:9; R-locked:6 | 39 (16) | 7.12 |
| shortcut_appears-s7-1 | FAIL 0/0 | n/a | None / {'A': 411.5, 'B': 101.5} | A h1:4/4 ack-s h3:1/1 ack-s; B h3:5/12 ack3.8s | 0/0 (rep 0) | 0 ({}) | A:45tx/3413B/29s; B:46tx/4145B/35s; R1:58tx/4619B/39s; R2:54tx/4294B/36s; R3:59tx/4982B/42s | R1-half-duplex:13; A-half-duplex:9; R2-locked:8; R1-locked:7 | 38 (19) | 46.74 |
| shortcut_appears-s11-1 | PASS 3/10 | min=5.39s avg=11.05s max=16.07s | 31.8 / {'A': 600.1, 'B': 127.5} | A h1:4/5 ack-s h3:7/25 ack3.2s; B h1:0/2 ack-s h2:0/2 ack-s h3:13/25 ack3.5s | 1/17 (rep 0) | 7 ({0: 6, 1: 1}) | A:104tx/7591B/65s; B:79tx/6734B/57s; R1:123tx/8868B/76s; R2:112tx/7879B/68s; R3:113tx/8326B/71s | R2-half-duplex:22; R1-locked:21; R1-collision:21; R1-half-duplex:20 | 74 (34) | 23.41 |
| two_hop-s7-1 | PASS 6/8 | min=8.48s avg=11.79s max=26.95s | 135.3 / {'A': 600.1, 'B': 101.0} | A h1:1/5 ack-s h2:3/3 ack2.4s; B h2:8/14 ack4.5s | 3/13 (rep 0) | 10 ({0: 8, 1: 2}) | A:50tx/4423B/37s; B:54tx/4865B/40s; R1:74tx/6681B/55s; R2:72tx/6230B/52s | R2-locked:11; R2-half-duplex:9; R2-collision:9; B-half-duplex:8 | 30 (15) | 11.02 |
| weak_direct-s7-1 | PASS 14/16 | min=6.63s avg=12.06s max=22.62s | 89.6 / {'A': 36.5, 'B': 35.0} | A h1:50/63 ack2.5s; B h0:27/46 ack1.3s | 5/9 (rep 0) | 1 ({0: 1}) | A:106tx/7298B/64s; B:119tx/6829B/62s; R:98tx/6597B/57s | A-half-duplex:30; B-half-duplex:28; R-half-duplex:24; A-snr:7 | 82 (38) | 4.78 |
| two_hop-s11-1 | PASS 7/8 | min=8.27s avg=11.62s max=25.98s | 138.5 / {'A': 600.1, 'B': 300.5} | A h1:4/4 ack2.8s h2:2/3 ack2.2s; B h2:11/14 ack4.5s | 2/10 (rep 0) | 9 ({0: 8, 1: 1}) | A:51tx/4071B/35s; B:52tx/5062B/41s; R1:77tx/6670B/56s; R2:78tx/6804B/57s | A-snr:9; R2-locked:9; R1-half-duplex:8; R2-collision:7 | 24 (12) | 9.85 |
| weak_direct-s11-1 | PASS 16/16 | min=2.24s avg=8.30s max=43.23s | 91.4 / {'A': 80.5, 'B': 24.5} | A h0:18/23 ack1.0s h1:2/2 ack-s; B h0:25/33 ack1.3s | 8/25 (rep 0) | 20 ({0: 16, 2: 1, 1: 3}) | A:80tx/5917B/51s; B:71tx/5075B/44s; R:23tx/1852B/16s | B-half-duplex:15; R-half-duplex:13; A-half-duplex:12; B-snr:7 | 40 (19) | 2.96 |
| zero_hop-s7-1 | PASS 7/8 | min=2.24s avg=5.62s max=19.59s | 51.2 / {'A': 76.0, 'B': 56.5} | A h0:2/4 ack0.9s; B h0:4/6 ack1.3s | 2/10 (rep 0) | 8 ({0: 7, 1: 1}) | A:35tx/2935B/25s; B:31tx/3081B/25s | B-half-duplex:6; A-half-duplex:6 | 12 (5) | 2.97 |
| zero_hop-s11-1 | PASS 8/8 | min=2.25s avg=13.53s max=28.95s | 50.6 / {'A': 142.5, 'B': 56.5} | A h0:1/3 ack0.8s; B h0:4/5 ack1.3s | 2/6 (rep 0) | 5 ({0: 4, 1: 1}) | A:47tx/4011B/33s; B:31tx/3221B/26s | B-half-duplex:7; A-half-duplex:7 | 14 (6) | 3.43 |
| three_hop-s7-1 | PASS 8/8 | min=11.33s avg=32.24s max=56.23s | 39.8 / {'A': 157.5, 'B': 300.5} | A h3:5/9 ack3.0s; B h3:9/15 ack6.2s | 5/15 (rep 0) | 13 ({0: 8, 1: 5}) | A:47tx/3958B/33s; B:49tx/3962B/34s; R1:74tx/5638B/48s; R2:71tx/5327B/45s; R3:70tx/5382B/46s | R3-half-duplex:8; R3-locked:7; A-snr:7; R2-half-duplex:6 | 26 (13) | 10.91 |
| three_hop-s11-1 | PASS 5/8 | min=11.03s avg=23.96s max=50.00s | 38.6 / {'A': 136.0, 'B': 127.5} | A h3:10/19 ack3.0s; B h3:16/23 ack6.2s | 4/17 (rep 0) | 11 ({0: 8, 1: 3}) | A:60tx/4198B/37s; B:56tx/4432B/38s; R1:88tx/5769B/51s; R2:82tx/5431B/47s; R3:85tx/6020B/52s | R3-half-duplex:10; R2-half-duplex:10; A-snr:10; R2-snr:9 | 39 (19) | 16.18 |
| page_transfer_bidir-s11-1 | FAIL 0/3 | n/a | 24.8 / {'A': 18.0, 'B': 37.0} | A h1:14/22 ack1.8s; B h1:20/26 ack1.7s | 15/19 (rep 10) | 40 ({0: 36, 1: 4}) | A:86tx/8893B/72s; B:65tx/4148B/37s; R:108tx/9096B/76s | R-half-duplex:28; A-half-duplex:20; R-locked:8; B-half-duplex:7 | 55 (26) | 6.60 |
| page_transfer_bidir-s7-1 | FAIL 0/3 | n/a | 78.6 / {'A': 36.5, 'B': 33.0} | A h1:106/172 ack3.3s; B h1:127/208 ack3.4s | 26/40 (rep 11) | 103 ({0: 72, 1: 31}) | A:414tx/42681B/348s; B:352tx/28601B/243s; R:536tx/43766B/366s | R-half-duplex:121; A-half-duplex:83; R-locked:60; R-collision:48 | 246 (111) | 6.13 |

## Verdicts

- failover-s7-1: exit 0 (PASS), 7.4 min
- failover-s11-1: exit 0 (PASS), 7.9 min
- large_payload-s11-1: exit 0 (PASS), 4.6 min
- large_payload-s7-1: exit 2 (FAIL), 6.7 min
- relay-s11-1: exit 0 (PASS), 3.1 min
- relay-s7-1: exit 0 (PASS), 4.7 min
- repeater_returns-s11-1: exit 0 (PASS), 8.6 min
- repeater_returns-s7-1: exit 0 (PASS), 9.9 min
- shortcut_appears-s7-1: exit 2 (FAIL), 7.7 min
- shortcut_appears-s11-1: exit 0 (PASS), 19.0 min
- two_hop-s7-1: exit 0 (PASS), 14.4 min
- weak_direct-s7-1: exit 0 (PASS), 8.1 min
- two_hop-s11-1: exit 0 (PASS), 13.6 min
- weak_direct-s11-1: exit 0 (PASS), 5.5 min
- zero_hop-s7-1: exit 0 (PASS), 3.5 min
- zero_hop-s11-1: exit 0 (PASS), 3.6 min
- three_hop-s7-1: exit 0 (PASS), 10.4 min
- three_hop-s11-1: exit 0 (PASS), 8.6 min
- page_transfer_bidir-s11-1: exit 2 (FAIL), 13.2 min
- page_transfer_bidir-s7-1: exit 2 (FAIL), 32.4 min

## Per-part bursts (pkt: round-0 sent/landed, rounds, first fragment -> known complete s)

- large_payload-s11-1: 65488: 4/1, 2r, 31.9; 65489: 4/2, 2r, 21.8; 65490: 4/1, 2r, 34.0; 65491: 4/4, 1r, 16.8; 65492: 4/2, 2r, 29.7
- page_transfer_bidir-s11-1: 63773: 3/3, 1r, 26.2; 63774: 3/2, 1r, 16.3; 63775: 3/1, 2r, 18.9; 63776: 3/3, 1r, 7.9; 63777: 3/0, 1r, None; 63778: 3/1, 2r, 18.2; 63779: 3/3, 1r, 9.2; 63780: 3/3, 1r, 8.5; 63781: 3/1, 2r, 19.1; 63782: 3/3, 1r, 8.6; 63783: 3/1, 2r, 18.8; 63784: 3/3, 1r, 8.7
- page_transfer_bidir-s7-1: 37853: 3/2, 1r, 16.0; 37854: 1/1, 1r, 3.6; 37855: 1/1, 1r, 3.9; 37856: 3/1, 2r, None; 37858: 3/1, 1r, None; 37859: 3/1, 2r, None; 37865: 3/3, 1r, 8.6; 37866: 4/2, 2r, None; 37867: 4/2, 2r, None; 37868: 4/2, 2r, None; 37869: 4/2, 2r, None; 37874: 3/3, 1r, 8.9; 37875: 4/1, 2r, 83.1; 37876: 4/2, 2r, None; 37877: 4/2, 2r, None; 37878: 4/2, 2r, None; 37880: 4/1, 2r, None; 37881: 4/2, 2r, None; 37882: 4/2, 2r, 37.2; 37886: 4/2, 2r, None; 37887: 4/2, 2r, None

## Re-run of the two seed-7 bring-up FAILs

| scenario | runs (pass) | seeds | delivered | late | RTT med s | RNS path s | DIRECT attempt rate by hop | ACK med by hop s | reported fraction | half-duplex misses (LBT-prev.) | on-air B/RNS B | links med s (≤deadline) | resources complete / med s / re-sent |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| large_payload | 1 (1) | 7 | 67% | 0 | 34.8 | 72 | A h1 76%; B h1 74% | A h1 2.3; B h1 2.9 | 24% | 83 (37) | 6.02 | - (-) | - / - / - |
| shortcut_appears | 1 (1) | 7 | 40% | 0 | 5.5 | 77 | A h1 0%; A h3 23%; B h1 100%; B h2 25%; B h3 50% | A h3 3.0; B h1 3.2; B h2 2.1; B h3 7.2 | 0% | 39 (19) | 20.00 | - (-) | - / - / - |
| run | verdict | probe RTT (RNS packet -> PROOF back) | RNS path s (sender) / time to DIRECT path per node | DIRECT attempts ok/total by hop (median ACK) | A completion checks answered-or-reported / total (rep = reported) | A raw fragments sent (by reconcile round) | MeshBench on-air per node (tx / bytes / s) | top miss reasons | half-duplex misses (LBT-preventable) | on-air B per RNS B |
|---|---|---|---|---|---|---|---|---|---|---|
| large_payload-s7-1 | PASS 4/6 | min=27.31s avg=36.26s max=48.11s | 71.6 / {'A': 32.0, 'B': 38.5} | A h1:19/25 ack2.3s; B h1:23/31 ack2.9s | 9/17 (rep 4) | 43 ({0: 24, 1: 19}) | A:91tx/10866B/87s; B:69tx/4768B/42s; R:112tx/9543B/80s | R-half-duplex:43; A-half-duplex:27; B-half-duplex:13; R-locked:4 | 83 (37) | 6.02 |
| shortcut_appears-s7-1 | PASS 4/10 | min=5.19s avg=6.85s max=11.23s | 77.2 / {'A': 241.5, 'B': 298.0} | A h1:0/9 ack-s h3:5/22 ack3.0s; B h1:2/2 ack3.2s h2:1/4 ack2.1s h3:11/22 ack7.2s | 1/25 (rep 0) | 10 ({0: 10}) | A:71tx/5287B/45s; B:74tx/5442B/47s; R1:84tx/6358B/54s; R2:63tx/4558B/39s; R3:67tx/5070B/43s | R2-snr:19; R1-collision:12; R1-half-duplex:11; R1-locked:11 | 39 (19) | 20.00 |
