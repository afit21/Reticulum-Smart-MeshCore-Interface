# MeshBench simulated benchmark -- alpha-0.1.3 (development branch, 0733f91 + working tree, 2026-09-20)

Scenarios ['zero_hop', 'relay', 'two_hop', 'large_payload', 'page_transfer', 'page_transfer_bidir', 'link_setup', 'bring_up', 'many_peers', 'mixed_builds', 'three_hop', 'duty_cycle_pages'], seeds [7, 11, 13], 1 run(s) per seed, interface `/home/afi/Documents/Reticulum-Smart-MeshCore-Interface/Reticulum-Smart-MeshCore-Interface/Interface/SmartMeshCoreInterface.py`, run arguments defaults. Produced by `meshbench_scenarios.py suite`; per-run details in each `<scenario>-s<seed>-<n>/result.json` ("analysis") and `run.log`.

Read with the two caveats every baseline file carries: MeshBench's RF is optimistic and its airtime 1.2-1.45x RadioLib's, its runs are wall-clock driven and not reproducible, and its virtual radio has no listen-before-talk (the LBT-preventable column counts the half-duplex misses a real SX1262 would have deferred). Mechanics are the hard checks; delivery and timing are measured rates -- compare medians and ranges, not single runs.

## Per scenario: medians [min-max] over the runs

| scenario | runs (pass) | seeds | delivered | late | RTT med s | RNS path s | DIRECT attempt rate by hop | ACK med by hop s | reported fraction | half-duplex misses (LBT-prev.) | on-air B/RNS B | links med s (≤deadline) | resources complete / med s / re-sent |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| bring_up | 3 (3) | 7,11,13 | - | 0 [0–0] | - | 32 [31–59] |  |  | - | 10 [4–10] (4 [2–5]) | 18.79 [18.67–26.54] | - (-) | - / - / - |
| duty_cycle_pages | 3 (2) | 7,11,13 | 75% [0%–75%] | 0 [0–0] | 221.0 [215.5–226.4] | 51 [48–51] | A h0 80% [80%–81%]; A h1 33%; B h0 72% [72%–72%] | A h0 0.9 [0.8–1.0]; A h1 0.9; B h0 0.9 [0.9–0.9] | 31% [28%–34%] | 132 [17–164] (55 [8–68]) | 2.21 [2.18–5.97] | 46.9 [21.6–90.0] (0% [0%–50%]) | 75% [0%–100%] / 221 [216–226] / 0 [0–0] |
| large_payload | 3 (1) | 7,11,13 | 0% [0%–33%] | 0 [0–0] | 31.2 | 39 [33–55] | A h1 58%; B h1 40% | A h1 1.7; B h1 2.6 | 40% | 50 [41–74] (24 [20–33]) | 9.80 [8.27–12.76] | - (-) | - / - / - |
| link_setup | 3 (2) | 7,11,13 | 75% [25%–75%] | 0 [0–0] | 18.8 [16.0–22.2] | 104 [48–106] | A h1 43% [39%–46%]; B h1 40% [35%–45%] | A h1 3.1 [3.1–3.1]; B h1 3.3 [3.3–3.3] | - | 52 [31–65] (24 [14–26]) | 8.52 [7.79–12.39] | 23.7 [22.3–90.0] (25% [0%–38%]) | - / - / - |
| many_peers | 3 (0) | 7,11,13 | 75% | 0 [0–0] | 20.7 | 101 | A h0 62% [55%–69%]; B h0 55% [53%–57%] | A h0 1.1 [1.1–1.1]; B h0 1.2 [0.9–1.6] | - | 160 [90–199] (54 [36–63]) | 6.08 [1.69–7.60] | - (-) | - / - / - |
| mixed_builds | 3 (2) | 7,11,13 | 50% [0%–83%] | 0 [0–0] | 34.8 [30.6–38.9] | 40 [38–77] | A h1 74% [70%–78%]; B h1 58% [33%–83%] | A h1 1.6 [1.6–1.6]; B h1 2.6 [2.1–3.0] | 0% [0%–0%] | 58 [53–67] (27 [26–28]) | 7.29 [5.89–72.81] | - (-) | - / - / - |
| page_transfer | 3 (1) | 7,11,13 | 0% [0%–67%] | 0 [0–0] | 463.8 | 47 [35–154] | A h1 69% [61%–77%]; B h1 45% [39%–51%] | A h1 2.4 [2.2–2.7]; B h1 2.3 [2.1–2.5] | 42% [34%–50%] | 285 [22–370] (129 [11–174]) | 8.10 [6.19–11.52] | 40.4 [26.7–42.8] (0% [0%–50%]) | 0% [0%–67%] / 464 / -16 [-36–0] |
| page_transfer_bidir | 3 (0) | 7,11,13 | 0% [0%–0%] | 0 [0–0] | - | 60 [33–194] | A h1 29% [26%–32%]; B h1 24% [24%–24%] | A h1 2.3 [1.8–2.8]; B h1 1.8 [1.6–2.1] | 4% [4%–5%] | 102 [40–216] (47 [20–98]) | 24.95 [18.48–31.23] | 51.1 [48.5–63.0] (50% [0%–50%]) | 0% [0%–0%] / - / -24 [-24–-24] |
| relay | 3 (2) | 7,11,13 | 88% [62%–100%] | 0 [0–1] | 12.6 [11.7–30.0] | 67 [33–96] | A h1 73% [60%–86%]; B h1 71% [70%–73%] | A h1 1.7 [1.6–1.8]; B h1 2.4 [2.1–2.6] | 60% [47%–73%] | 31 [24–51] (15 [11–24]) | 5.74 [5.43–10.09] | - (-) | - / - / - |
| three_hop-s11 | 1 (1) | None | - | 0 | - | - |  |  | - | 0 (0) | - | - (-) | - / - / - |
| three_hop-s13 | 1 (1) | None | - | 0 | - | - |  |  | - | 0 (0) | - | - (-) | - / - / - |
| three_hop-s7 | 1 (1) | None | - | 0 | - | - |  |  | - | 0 (0) | - | - (-) | - / - / - |
| two_hop | 3 (2) | 7,11,13 | 62% [62%–75%] | 0 [0–0] | 25.9 [12.1–30.1] | 33 [33–34] | A h1 50%; A h2 58% [44%–72%]; B h2 66% [56%–76%] | A h1 4.1; A h2 2.6 [2.4–2.9]; B h2 3.0 [2.7–3.3] | 43% [40%–46%] | 35 [26–48] (16 [11–19]) | 14.60 [10.31–17.27] | - (-) | - / - / - |
| zero_hop | 3 (2) | 7,11,13 | 100% [88%–100%] | 0 [0–0] | 3.6 [3.1–21.2] | 51 [48–131] | A h0 100%; B h0 89% [85%–93%] | A h0 1.0; B h0 0.8 [0.8–0.8] | 93% [86%–100%] | 16 [12–18] (7 [6–9]) | 3.39 [2.90–4.16] | - (-) | - / - / - |

## Per run

| run | verdict | probe RTT (RNS packet -> PROOF back) | RNS path s (sender) / time to DIRECT path per node | DIRECT attempts ok/total by hop (median ACK) | A completion checks answered-or-reported / total (rep = reported) | A raw fragments sent (by reconcile round) | MeshBench on-air per node (tx / bytes / s) | top miss reasons | half-duplex misses (LBT-preventable) | on-air B per RNS B |
|---|---|---|---|---|---|---|---|---|---|---|
| zero_hop-s7-1 | PASS 8/8 | min=3.07s avg=10.26s max=44.94s | 47.6 / {'A': 112.0, 'B': 120.5} | A h0:1/1 ack1.0s; B h0:13/14 ack0.8s | 7/7 (rep 6) | 7 ({0: 6, 1: 1}) | A:46tx/3264B/28s; B:33tx/2855B/24s | B-half-duplex:6; A-half-duplex:6 | 12 (6) | 2.90 |
| zero_hop-s13-1 | FAIL 8/8 | min=7.88s avg=19.79s max=30.12s | 51.2 / {} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:61tx/5164B/43s; B:37tx/3609B/30s | B-half-duplex:8; A-half-duplex:8 | 16 (7) | 4.16 |
| zero_hop-s11-1 | PASS 7/8 | min=3.08s avg=5.18s max=17.35s | 130.7 / {'A': 178.0, 'B': 181.5} | A -; B h0:11/13 ack0.8s | 6/6 (rep 6) | 6 ({0: 6}) | A:53tx/3710B/32s; B:37tx/3506B/29s | B-half-duplex:9; A-half-duplex:9 | 18 (9) | 3.39 |
| relay-s7-1 | PASS 8/8 | min=6.38s avg=12.98s max=21.79s | 95.6 / {'A': 31.0, 'B': 104.0} | A h1:6/10 ack1.6s; B h1:16/22 ack2.1s | 11/11 (rep 8) | 11 ({0: 8, 1: 3}) | A:53tx/3406B/30s; B:54tx/3576B/31s; R:88tx/5421B/48s | R-half-duplex:15; B-half-duplex:11; A-half-duplex:5; B-snr:2 | 31 (15) | 5.74 |
| relay-s11-1 | PASS 7/8 | min=6.71s avg=12.63s max=20.59s | 33.0 / {'A': 30.0, 'B': 99.0} | A h1:6/7 ack1.8s; B h1:16/23 ack2.6s | 10/15 (rep 7) | 12 ({0: 8, 2: 1, 1: 3}) | A:49tx/3259B/29s; B:49tx/3081B/27s; R:81tx/4827B/43s | R-half-duplex:11; B-half-duplex:8; A-half-duplex:5; R-collision:4 | 24 (11) | 5.43 |
| relay-s13-1 | FAIL 5/8 (+1 late) | min=24.94s avg=34.86s max=47.62s | 67.2 / {} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:63tx/5711B/47s; B:44tx/3712B/31s; R:79tx/6707B/56s | R-half-duplex:26; A-half-duplex:13; B-half-duplex:12; B-snr:2 | 51 (24) | 10.09 |
| two_hop-s7-1 | PASS 5/8 | min=10.89s avg=21.83s max=47.01s | 32.8 / {'A': 85.5, 'B': 90.5} | A h2:7/16 ack2.4s; B h2:19/25 ack2.7s | 11/13 (rep 6) | 10 ({0: 7, 1: 3}) | A:61tx/4129B/36s; B:56tx/3795B/33s; R1:93tx/5627B/50s; R2:90tx/5305B/47s | B-snr:11; A-snr:11; R2-half-duplex:8; R1-half-duplex:8 | 26 (11) | 10.31 |
| large_payload-s7-1 | PASS 2/6 | min=28.70s avg=31.24s max=33.77s | 55.4 / {'A': 103.0, 'B': 54.0} | A h1:14/24 ack1.7s; B h1:14/35 ack2.6s | 13/15 (rep 6) | 28 ({0: 16, 1: 8, 2: 4}) | A:100tx/8708B/73s; B:73tx/4398B/39s; R:117tx/8127B/70s | R-half-duplex:37; A-half-duplex:28; R-locked:13; B-half-duplex:9 | 74 (33) | 8.27 |
| two_hop-s11-1 | PASS 6/8 | min=10.81s avg=25.80s max=48.94s | 32.6 / {'A': 673.6, 'B': 228.0} | A h1:1/2 ack4.1s h2:13/18 ack2.9s; B h2:15/27 ack3.3s | 9/10 (rep 4) | 7 ({0: 5, 1: 2}) | A:79tx/5551B/48s; B:87tx/5832B/51s; R1:129tx/8167B/72s; R2:131tx/8372B/74s | A-snr:19; R1-half-duplex:18; R2-half-duplex:15; B-snr:13 | 48 (19) | 14.60 |
| large_payload-s11-1 | FAIL 0/6 | n/a | 39.4 / {'A': 407.5} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:71tx/10032B/78s; B:36tx/2781B/24s; R:70tx/8462B/67s | R-half-duplex:24; A-half-duplex:16; B-half-duplex:10; R-locked:6 | 50 (24) | 12.76 |
| two_hop-s13-1 | FAIL 5/8 | min=24.55s avg=36.34s max=54.64s | 34.2 / {} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:73tx/6309B/53s; B:53tx/4899B/41s; R1:90tx/7510B/63s; R2:90tx/7785B/65s | R1-locked:12; B-snr:12; R2-snr:11; R1-collision:11 | 35 (16) | 17.27 |
| large_payload-s13-1 | FAIL 0/6 | n/a | 33.4 / {} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:91tx/10576B/84s; B:39tx/3069B/26s; R:91tx/9059B/74s | R-half-duplex:21; A-half-duplex:13; R-collision:10; R-locked:8 | 41 (20) | 9.80 |
| page_transfer-s7-1 | PASS 2/3 | min=430.81s avg=463.78s max=496.76s | 47.4 / {'A': 49.5, 'B': 54.0} | A h1:132/171 ack2.2s; B h1:91/179 ack2.5s | 100/121 (rep 60) | 237 ({0: 130, 1: 72, 2: 35}) | A:565tx/50254B/418s; B:383tx/17273B/166s; R:691tx/44937B/393s | R-half-duplex:186; A-half-duplex:136; B-half-duplex:48; R-locked:42 | 370 (174) | 6.19 |
| page_transfer-s13-1 | FAIL 0/3 | n/a | 153.5 / {} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:93tx/7191B/61s; B:77tx/6941B/58s; R:144tx/11571B/98s | R-half-duplex:11; A-half-duplex:8; R-locked:7; R-collision:7 | 22 (11) | 11.52 |
| page_transfer-s11-1 | FAIL 0/3 | n/a | 35.2 / {'A': 21.5, 'B': 39.0} | A h1:125/204 ack2.7s; B h1:56/144 ack2.1s | 47/70 (rep 24) | 97 ({0: 59, 1: 25, 2: 13}) | A:421tx/39943B/330s; B:359tx/19696B/180s; R:559tx/37213B/324s | R-half-duplex:141; A-half-duplex:86; B-half-duplex:58; R-locked:45 | 285 (129) | 8.10 |
| page_transfer_bidir-s7-1 | FAIL 0/3 | n/a | 32.8 / {'A': 48.0, 'B': 99.0} | A h1:19/59 ack1.8s; B h1:13/54 ack1.6s | 7/20 (rep 1) | 12 ({0: 6, 2: 4, 1: 2}) | A:125tx/11080B/92s; B:125tx/10127B/86s; R:149tx/10386B/90s | R-half-duplex:51; B-half-duplex:29; R-collision:24; R-locked:24 | 102 (47) | 24.95 |
| link_setup-s7-1 | PASS 6/8 | min=7.77s avg=22.83s max=53.61s | 47.6 / {'A': 22.5, 'B': 54.0} | A h1:20/51 ack3.1s; B h1:12/34 ack3.3s | 0/0 (rep 0) | 0 ({}) | A:85tx/6985B/59s; B:94tx/6811B/59s; R:115tx/8107B/70s | R-half-duplex:33; B-half-duplex:20; R-locked:16; R-collision:14 | 65 (26) | 7.79 |
| page_transfer_bidir-s11-1 | FAIL 0/3 | n/a | 59.8 / {'A': 35.5, 'B': 98.5} | A h1:30/114 ack2.8s; B h1:24/98 ack2.1s | 10/25 (rep 1) | 10 ({0: 7, 2: 2, 1: 1}) | A:204tx/17841B/149s; B:242tx/21721B/180s; R:249tx/17685B/152s | R-half-duplex:110; B-half-duplex:57; A-half-duplex:49; R-locked:45 | 216 (98) | 31.23 |
| link_setup-s11-1 | PASS 6/8 | min=7.83s avg=18.58s max=37.40s | 106.2 / {'A': 142.0, 'B': 54.0} | A h1:18/39 ack3.1s; B h1:17/38 ack3.3s | 0/0 (rep 0) | 0 ({}) | A:90tx/7062B/60s; B:87tx/6939B/59s; R:120tx/8525B/73s | R-half-duplex:25; R-collision:16; R-locked:15; A-half-duplex:14 | 52 (24) | 8.52 |
| bring_up-s7-1 | PASS 0/0 | n/a | 59.2 / {'A': 120.0, 'B': 126.0} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:11tx/781B/7s; B:11tx/941B/8s; R1:16tx/1168B/10s; R2:17tx/1206B/10s | A-snr:3; R1-locked:2; R1-collision:2; B-half-duplex:1 | 4 (2) | 18.79 |
| link_setup-s13-1 | FAIL 2/8 | min=17.67s avg=22.23s max=26.79s | 103.6 / {} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:62tx/5818B/48s; B:62tx/5716B/47s; R:93tx/7902B/66s | R-half-duplex:15; A-half-duplex:9; R-collision:8; B-half-duplex:7 | 31 (14) | 12.39 |
| bring_up-s11-1 | PASS 0/0 | n/a | 31.2 / {'A': 135.5, 'B': 124.5} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:12tx/645B/6s; B:12tx/1052B/9s; R1:17tx/1187B/10s; R2:17tx/1186B/10s | A-snr:4; A-half-duplex:3; R1-half-duplex:3; B-half-duplex:2 | 10 (5) | 18.67 |
| page_transfer_bidir-s13-1 | FAIL 0/3 | n/a | 193.7 / {} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:86tx/6434B/55s; B:87tx/8325B/68s; R:135tx/11238B/95s | R-half-duplex:20; B-half-duplex:11; A-half-duplex:9; R-collision:9 | 40 (20) | 18.48 |
| many_peers-s7-1 | FAIL 0/0 | n/a | None / {'A': 71.5, 'E': 145.0} | A h0:11/16 ack1.1s; B - | 0/0 (rep 0) | 0 ({}) | A:44tx/2991B/26s; B:25tx/2155B/18s; C:23tx/1635B/14s; D:28tx/1689B/15s; E:37tx/1817B/17s | C-half-duplex:38; A-half-duplex:34; B-half-duplex:31; D-half-duplex:29 | 160 (63) | 7.60 |
| many_peers-s11-1 | FAIL 0/0 | n/a | None / {'A': 201.0, 'B': 152.0, 'D': 316.0, 'E': 306.0} | A h0:6/11 ack1.1s; B h0:8/15 ack0.9s | 0/0 (rep 0) | 0 ({}) | A:38tx/2813B/24s; B:70tx/4595B/40s; C:25tx/1679B/15s; D:57tx/2632B/25s; E:67tx/3145B/30s | D-half-duplex:50; E-half-duplex:48; B-half-duplex:46; C-half-duplex:39 | 199 (54) | 6.08 |
| bring_up-s13-1 | PASS 0/0 | n/a | 32.4 / {} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:22tx/1518B/13s; B:29tx/2475B/21s; R1:40tx/3133B/27s; R2:39tx/3090B/26s | R2-collision:6; A-snr:5; R2-locked:4; R1-half-duplex:4 | 10 (4) | 26.54 |
| mixed_builds-s7-1 | PASS 3/6 | min=33.06s avg=39.25s max=45.77s | 39.6 / {'A': 47.5, 'B': 91.5} | A h1:19/27 ack1.6s; B h1:3/9 ack3.0s | 11/18 (rep 0) | 32 ({0: 20, 2: 4, 1: 8}) | A:98tx/9296B/76s; B:65tx/3156B/30s; R:121tx/8559B/74s | R-half-duplex:29; A-half-duplex:20; B-half-duplex:9; R-collision:7 | 58 (28) | 7.29 |
| many_peers-s13-1 | FAIL 6/8 | min=19.79s avg=25.28s max=36.21s | 101.2 / {'B': 189.0, 'E': 159.0} | A -; B h0:4/7 ack1.6s | 0/0 (rep 0) | 0 ({}) | A:61tx/5628B/46s; B:56tx/5286B/43s; C:23tx/1621B/14s; D:19tx/1416B/12s; E:26tx/1701B/15s | D-locked:37; D-collision:31; B-half-duplex:31; E-locked:25 | 90 (36) | 1.69 |
| three_hop-s7-1 | PASS None/None | n/a | None / {} | A -; B - | 0/0 (rep 0) | 0 ({}) |  |  | 0 (0) | - |
| three_hop-s11-1 | PASS None/None | n/a | None / {} | A -; B - | 0/0 (rep 0) | 0 ({}) |  |  | 0 (0) | - |
| three_hop-s13-1 | PASS None/None | n/a | None / {} | A -; B - | 0/0 (rep 0) | 0 ({}) |  |  | 0 (0) | - |
| mixed_builds-s11-1 | PASS 5/6 | min=24.64s avg=32.60s max=45.06s | 77.4 / {'A': 69.0, 'B': 60.0} | A h1:18/23 ack1.6s; B h1:10/12 ack2.1s | 14/18 (rep 0) | 39 ({0: 24, 1: 11, 2: 4}) | A:105tx/7781B/67s; B:73tx/3819B/35s; R:132tx/7645B/69s | R-half-duplex:35; A-half-duplex:23; B-half-duplex:9; R-collision:5 | 67 (27) | 5.89 |
| mixed_builds-s13-1 | FAIL 0/6 | n/a | 38.2 / {} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:71tx/7769B/63s; B:28tx/2028B/17s; R:68tx/6076B/51s | R-half-duplex:27; A-half-duplex:21; B-half-duplex:5; B-snr:2 | 53 (26) | 72.81 |
| duty_cycle_pages-s11-1 | PASS 3/4 | min=161.90s avg=214.97s max=256.57s | 48.0 / {'A': 107.0, 'B': 122.0} | A h0:74/93 ack0.8s; B h0:122/170 ack0.9s | 84/100 (rep 34) | 204 ({0: 154, 1: 41, 2: 9}) | A:453tx/34168B/291s; B:286tx/14127B/132s | B-half-duplex:75; A-half-duplex:57 | 132 (55) | 2.18 |
| duty_cycle_pages-s7-1 | PASS 3/4 | min=203.00s avg=226.44s max=260.81s | 50.8 / {'A': 135.0, 'B': 101.5} | A h0:136/168 ack1.0s h1:1/3 ack0.9s; B h0:122/170 ack0.9s | 81/107 (rep 30) | 199 ({0: 150, 1: 38, 2: 11}) | A:547tx/43234B/366s; B:373tx/15927B/154s | B-half-duplex:93; A-half-duplex:71 | 164 (68) | 2.21 |
| duty_cycle_pages-s13-1 | FAIL 0/4 | n/a | 50.6 / {} | A -; B - | 0/0 (rep 0) | 0 ({}) | A:78tx/5964B/51s; B:76tx/7304B/60s | B-half-duplex:9; A-half-duplex:8 | 17 (8) | 5.97 |

## Verdicts

- zero_hop-s7-1: exit 0 (PASS), 3.1 min
- zero_hop-s13-1: exit 2 (FAIL), 4.4 min
- zero_hop-s11-1: exit 0 (PASS), 4.7 min
- relay-s7-1: exit 0 (PASS), 4.4 min
- relay-s11-1: exit 0 (PASS), 4.1 min
- relay-s13-1: exit 2 (FAIL), 8.1 min
- two_hop-s7-1: exit 0 (PASS), 7.5 min
- large_payload-s7-1: exit 0 (PASS), 6.9 min
- two_hop-s11-1: exit 0 (PASS), 15.8 min
- large_payload-s11-1: exit 2 (FAIL), 7.6 min
- two_hop-s13-1: exit 2 (FAIL), 17.2 min
- large_payload-s13-1: exit 2 (FAIL), 7.5 min
- page_transfer-s7-1: exit 0 (PASS), 27.3 min
- page_transfer-s13-1: exit 2 (FAIL), 26.6 min
- page_transfer-s11-1: exit 2 (FAIL), 32.1 min
- page_transfer_bidir-s7-1: exit 2 (FAIL), 9.9 min
- link_setup-s7-1: exit 0 (PASS), 7.4 min
- page_transfer_bidir-s11-1: exit 2 (FAIL), 15.7 min
- link_setup-s11-1: exit 0 (PASS), 7.9 min
- bring_up-s7-1: exit 0 (PASS), 2.6 min
- link_setup-s13-1: exit 2 (FAIL), 12.6 min
- bring_up-s11-1: exit 0 (PASS), 2.9 min
- page_transfer_bidir-s13-1: exit 2 (FAIL), 25.7 min
- many_peers-s7-1: exit 2 (FAIL), 7.5 min
- many_peers-s11-1: exit 2 (FAIL), 7.5 min
- bring_up-s13-1: exit 0 (PASS), 10.6 min
- mixed_builds-s7-1: exit 0 (PASS), 6.6 min
- many_peers-s13-1: exit 2 (FAIL), 7.3 min
- three_hop-s7-1: exit 3 (ERROR), 0.1 min
- three_hop-s11-1: exit 3 (ERROR), 0.1 min
- three_hop-s13-1: exit 3 (ERROR), 0.1 min
- mixed_builds-s11-1: exit 0 (PASS), 6.0 min
- mixed_builds-s13-1: exit 2 (FAIL), 7.6 min
- duty_cycle_pages-s11-1: exit 0 (PASS), 13.7 min
- duty_cycle_pages-s7-1: exit 0 (PASS), 23.2 min
- duty_cycle_pages-s13-1: exit 2 (FAIL), 25.0 min

## Per-part bursts (pkt: round-0 sent/landed, rounds, first fragment -> known complete s)

- large_payload-s7-1: 3: 4/2, 3r, None; 5: 4/2, 3r, 50.0; 6: 4/2, 3r, 34.9; 7: 4/2, 3r, 29.1
- page_transfer-s7-1: 2: 2/0, 3r, 33.4; 3: 4/2, 3r, 49.3; 4: 4/2, 3r, 79.3; 5: 4/2, 3r, 39.4; 6: 4/2, 3r, 42.8; 7: 4/2, 3r, 62.5; 8: 4/1, 3r, None; 9: 4/2, 3r, 31.4; 10: 4/2, 3r, 41.3; 12: 4/1, 3r, 46.9; 13: 4/2, 3r, None; 14: 4/2, 3r, 31.8; 16: 2/1, 2r, 7.8; 17: 4/2, 3r, 48.0; 18: 4/1, 3r, 43.4; 19: 4/1, 2r, None; 20: 4/2, 3r, None; 21: 4/2, 2r, None; 22: 4/0, 3r, None; 29: 4/2, 3r, 33.7; 30: 4/2, 3r, 27.6; 31: 4/2, 3r, 23.7; 32: 2/1, 2r, 7.8; 33: 4/2, 3r, 47.8; 34: 4/2, 3r, 60.4; 35: 4/2, 3r, None; 36: 4/2, 3r, None; 37: 4/2, 2r, 83.9; 38: 4/2, 3r, None; 39: 4/1, 2r, None; 44: 4/2, 3r, 44.3; 45: 4/2, 3r, 58.1; 46: 4/0, 3r, None; 47: 4/2, 3r, None
- page_transfer-s11-1: 1: 2/1, 2r, 14.9; 2: 4/2, 3r, 36.9; 3: 4/2, 3r, None; 4: 4/2, 3r, 46.4; 5: 4/3, 2r, 18.0; 7: 4/2, 2r, None; 8: 4/2, 2r, None; 14: 4/2, 2r, None; 15: 4/2, 3r, None; 19: 2/1, 2r, 7.5; 20: 2/1, 2r, 7.6; 21: 2/1, 2r, 7.7; 22: 4/2, 3r, None; 23: 4/2, 3r, None; 28: 4/2, 3r, 35.4; 29: 4/2, 3r, None; 31: 3/1, 1r, None
- page_transfer_bidir-s7-1: 2: 2/0, 2r, None; 3: 2/1, 3r, None; 4: 2/1, 3r, None
- page_transfer_bidir-s11-1: 1: 2/0, 2r, None; 2: 1/1, 1r, 3.5; 3: 2/1, 2r, None; 12: 2/1, 1r, None
- mixed_builds-s7-1: 2: 4/3, 2r, 51.2; 3: 4/2, 3r, None; 4: 4/2, 2r, 41.3; 6: 4/2, 3r, None; 8: 4/2, 2r, None
- mixed_builds-s11-1: 2: 4/3, 2r, 27.1; 3: 4/2, 3r, 50.2; 4: 4/2, 2r, 29.5; 5: 4/2, 3r, 39.7; 6: 4/2, 2r, 46.2; 7: 4/2, 3r, None
- duty_cycle_pages-s11-1: 3: 2/1, 2r, 9.6; 4: 4/3, 2r, 32.3; 5: 4/3, 3r, 31.7; 6: 4/2, 3r, 39.8; 7: 4/3, 2r, 39.2; 8: 4/4, 1r, 17.2; 9: 4/3, 2r, 19.7; 10: 4/3, 2r, 23.2; 11: 4/3, 2r, 19.4; 12: 4/3, 2r, 33.0; 13: 4/3, 2r, 16.3; 14: 4/3, 2r, 19.1; 15: 4/2, 3r, 38.1; 16: 4/3, 2r, 23.4; 17: 2/1, 2r, 8.5; 18: 4/1, 3r, None; 19: 4/3, 2r, 24.0; 20: 4/4, 1r, 11.8; 22: 4/3, 2r, 27.9; 23: 4/3, 2r, 14.0; 24: 4/3, 2r, 13.4; 25: 4/3, 2r, 27.9; 26: 4/3, 2r, 20.0; 27: 4/2, 3r, 30.7; 28: 4/3, 3r, 62.3; 29: 4/3, 2r, 11.0; 30: 4/3, 2r, 14.0; 31: 4/3, 2r, 15.7; 32: 2/1, 2r, 9.1; 33: 4/3, 2r, 14.1; 34: 4/3, 2r, 13.4; 35: 4/4, 1r, 7.2; 36: 4/3, 2r, 16.7; 37: 4/3, 3r, 34.4; 38: 4/3, 2r, 12.7; 39: 4/3, 3r, 27.7; 40: 4/3, 2r, 20.8; 41: 4/3, 2r, 13.3; 42: 4/3, 2r, None; 43: 4/3, 2r, 13.2
- duty_cycle_pages-s7-1: 6: 4/3, 2r, 44.7; 7: 4/3, 2r, 26.9; 8: 4/3, 2r, 11.6; 9: 4/3, 2r, 14.7; 10: 4/3, 2r, 14.1; 11: 4/3, 2r, 20.3; 12: 4/3, 2r, 43.0; 13: 4/3, 2r, 24.4; 14: 4/3, 2r, 20.8; 15: 4/2, 3r, 38.3; 16: 4/3, 3r, None; 17: 2/2, 1r, 3.6; 18: 4/3, 2r, 15.3; 28: 4/3, 2r, 27.0; 29: 4/3, 2r, 22.7; 30: 2/0, 2r, None; 32: 4/3, 2r, 14.1; 33: 4/3, 2r, 21.8; 34: 4/3, 2r, 26.2; 35: 4/4, 1r, 10.0; 36: 4/3, 2r, 32.8; 37: 4/2, 3r, 19.6; 38: 4/3, 2r, 27.2; 39: 4/3, 2r, 23.6; 40: 4/3, 2r, 21.1; 41: 4/3, 2r, 13.7; 42: 4/3, 2r, 37.0; 43: 2/1, 2r, 9.8; 44: 4/3, 2r, 30.3; 45: 4/3, 2r, 70.1; 46: 4/3, 2r, 11.0; 47: 4/3, 2r, 11.3; 48: 4/3, 2r, 55.3; 49: 4/2, 2r, None; 50: 4/3, 2r, 12.6; 51: 4/3, 2r, 12.9; 52: 4/3, 2r, 11.0; 53: 4/2, 3r, 28.7; 55: 4/3, 2r, 16.4
