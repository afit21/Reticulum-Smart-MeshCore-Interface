# The reconcile redesign (phase 3 of the 2026-09-20 airtime / throughput pass)

Written for the project owner before implementation. Every claim about the firmware, the
`meshcore` library or RNS below was read in `referenceprojects/` (file and function named); every
number comes from `fieldtests/raw/Alpha0.1.3/` (the 2026-09-20 session) or the MeshBench baseline
`tests/baselines/alpha-0.1.3-simulatedbenchmark/`. The metric is on-air bytes per delivered RNS
byte, read with delivery rate and per-part completion time.

## Where the airtime goes today (one 483-byte Resource part, zero hop)

The raw burst-then-report design (docs/history.md, 2026-09-18 night and 2026-09-20 entries) costs,
per part that lands cleanly:

| item | on air | who | notes |
|---|---|---|---|
| 4 raw fragments (13 B header + 121/121/121/120 B) | 4 x ~172 B = 688 B | sender | `send_raw_data`, no ACK |
| gaps report (second-last fragment flagged) | ~40 B TXT_MSG | receiver | + its firmware ACK ~14 B from the sender |
| complete report (last fragment flagged) | ~40 B | receiver | + ACK ~14 B |
| the receiver's lock: two ACK waits | 0 B, ~2 x 1.2 s | receiver | why reports queued 1.1-4 s behind each other |

That is 796 B on air for 483 RNS bytes (1.65 B/B) when nothing is lost, and the desktop's field
figure was 2.59 B/B across the session (retries, QUERY rounds, the duplicate last fragment of
phase 1.5b's finding, PROOFs). The receiver sent 146 reports for about 105 bursts. At one and two
hops every frame is relayed once per repeater and per-fragment loss is ~18 % (one hop) so about half
of the bursts need a second round.

RNS gives the sender a window of 4 (up to 10; `WINDOW_MAX_VERY_SLOW` 4 below 250 B/s, which is this
link) parts at a time (`RNS/Resource.py` `request_next` / `accept`), and cancels the transfer if, once
the LAST part has been sent once, no part request arrives within four intervals of `3 x rtt + 10 s`
(phase 0's LXMF finding) -- so the per-part completion time in the tail of a transfer is what
decides whether a 39-part LXMF message survives.

## The four changes, in order

### M1 -- reports without a firmware ACK, debounced

**Mechanism.** MeshCore text messages carry a type in the encrypted body. `TXT_TYPE_CLI_DATA` (1)
is encrypted and MAC'd exactly like a plain message (`BaseChatMesh::sendCommandData` ->
`createDatagram` with the shared secret), relayed identically (repeaters route by path, not by
payload), delivered to the host as `CONTACT_MSG_RECV` with `txt_type=1`
(`MyMesh::onCommandDataRecv` -> `queueMessage`), and **never ACKed**: `BaseChatMesh::onPeerDataRecv`
line 258, "no ack expected for CLI_DATA replies"; the companion's `CMD_SEND_TXT_MSG` handler sets
`expected_ack = 0` for it. The library's `send_msg` hard-codes type 0; the frame is
`b"\x02" + txt_type + attempt + timestamp(4) + dst_prefix(6) + text`, sent through
`commands.send(data, [MSG_SENT, ERROR])` exactly as `send_msg` does (`meshcore/commands/messaging.py`).
The interface's receive path (`_on_contact_msg_recv_inner`) reads `text` regardless of `txt_type`, so
an older build receives such a report too.

**Change.** Every REPORT and every QUERY ANSWER goes out as CLI_DATA: the "Q" frame bytes are
unchanged (the golden wire snapshot does not move), only the MeshCore text type does. No firmware
ACK is generated, so the receiver's lock is held only through the gate, the send command and the
frame's airtime plus its relay gap (`_raw_fragment_gap_s`, the same rule raw fragments use), not
through a 1-3 s ACK wait -- the wait that made reports queue behind each other. The QUERY itself
stays ACKed: its ACK is stale-path evidence and the anchor of the quiet window.

**Debounce (receiver).** A flagged fragment that leaves gaps no longer reports at once. The bucket
arms a hold of `report_hold_s = airtime(one fragment) + relay_gap(hops)` -- the time the burst's
last fragment needs to arrive; if the bucket completes inside the hold, only the complete report is
sent; if not, the gaps report goes out at the end of the hold. At zero hop the complete report
followed the gaps report by 0.22-0.43 s at the receiver in the field, so one fragment airtime
(0.9 s) catches nearly all of them. The sender's phase-1.5b provisional handling stays as the
fallback for a report that still arrives out of order.

**Confirmation.** The sender's next action confirms a report: a report that is lost falls through to
the QUERY path exactly as today (the report window and its estimator are unchanged).

**Expected.** Per clean part at zero hop: two reports + two ACKs (108 B) become one report (40 B);
the receiver's report latency loses the previous report's ACK wait (the 23-of-31 cause of >1 s
report lock waits). About 30 % less control airtime per part; more reports inside the window, fewer
QUERY rounds.

**Config / wire.** `direct_report_noack` (default yes; `no` restores ACKed reports). No frame change.

### M2 -- one report per window

**Mechanism.** RNS sends a window's parts back to back; the interface today runs each part as its own
burst-and-report exchange (two in flight per peer), so a 4-part window costs 4 quiet periods and 4-8
reports. Instead: consecutive raw-eligible sends to one peer that arrive within `window_collect_s`
(0.75 s -- RNS emits a window's parts within milliseconds; the outgoing worker dequeues them one
per loop turn) or up to `direct_raw_window_max_parts` (6) form ONE window burst: all their
fragments back to back with the usual gaps, the last two fragments of the whole window flagged, one
quiet period, one report.

**Wire.** "Q" version 4 carries a list of entries: `[4][type][n][nonce]` then per entry
`[pkt_id:2][frag_total:1][complete:1][bitmap: ceil(frag_total/8)]`. A v4 REPORT lists every bucket
from that sender touched since its last report (at most 8, most recent first) -- completed ones
from the dedup cache, open ones from `_reassembly`; a v4 QUERY lists the pkt_ids the sender wants
answered and gets a v4 ANSWER with the same entries. v3 frames still decode and a v3 QUERY is
answered in v3; the golden snapshot gains the v4 cases in the same commit. Both nodes must run
this build (as for v3).

**Sender state machine** (the one module `_reconcile.py` owns; `_send_direct_raw_fragmented`
becomes `_run_window` over a `_RawWindow` of parts): register one window waiter per peer
(`_window_waiters[peer] = (future, {pkt_id...}, report nonce)`); burst; wait the report window;
apply the report's entries to each part; parts complete leave the window; the rest are re-driven as
the next window burst (batched the same way, up to `direct_raw_reconcile_rounds`); no report ->
one v4 QUERY for the outstanding pkt_ids, else re-burst; all of phase 1's rules (provisional
second-last report, pre-emption yields, path-reset abort, resume, fallback strikes) apply to the
window as they applied to the part. The per-peer in-flight cap becomes "one window in flight".

**Expected.** Reports and quiet gaps per window: ~12 and 6 today (2 parts in flight x 2 reports +
QUERYs) -> 1 and 1; the sender's silent time per part drops from one report window per part to one
per window.

### M3 -- three fragments per 483-byte part

**Mechanism.** The raw header is 13 bytes (`[flags][dst:2][src:6][pkt_id:2][idx][total]`); the
6-byte source prefix exists to land raw fragments in the same reassembly bucket as text fragments
from that sender (`_reassembly_key` uses the sender's 6-byte prefix). Bound peers are few (small-mesh
mode caps at 3); a 2-byte source prefix resolves to a bound peer uniquely in practice, and the
receiver checks that: if two bound peers share the 2-byte prefix the fragment is dropped (logged),
and the sender never uses raw to a peer whose 2-byte prefix collides with another bound peer's.

**Wire.** Raw header version 2, 9 bytes: `[2<<4 | flags | attempt&3][dst:2][src:2][pkt_id:2][idx][total]`.
Per-fragment payload `min(direct_raw_payload_cap 170, 172, 174 - path_len) - 9` = 161 up to four
hops -- and 3 x 161 = 483, the Link MDU part, exactly. Firmware limits re-read for this change:
`MAX_FRAME_SIZE` 176 on the companion serial link (`onRawDataRecv`: payload + 4 push bytes; the
2026-09-19 audit's 172 receive limit stands) and `CMD_SEND_RAW_DATA` = cmd + path_len + path +
payload -> 174 - path_len. A v1 raw fragment (13-byte header) is no longer decoded (both nodes are
updated together; the version nibble tells them apart and a v1 frame is dropped silently).

**Expected.** 3 fragments instead of 4 per part: 3 x 170 = 510 B on air instead of 4 x 172 = 688
(-26 % fragment airtime, more than the task's estimate because the header shrink and the fragment
count both help), one fewer loss opportunity and one fewer gap per part.

### M4 -- hop-adaptive parity

**Mechanism.** One XOR parity fragment per burst at one hop and above (none at zero hop, where
per-fragment loss is a few percent): `parity_fragments(hops) = 0 if hops == 0 else 1`; two at two
hops and above is left for the numbers to justify (the design allows n parity fragments over
disjoint halves). The parity fragment is a raw fragment with `RAW_FLAG_PARITY` (0x08) set and `idx`
= the coverage mask (bit i = data fragment i is covered; at most 8 data fragments, which the 3-4
fragment parts satisfy), payload = `[last_covered_len:1]` + XOR over the covered fragments padded to
the fragment budget. It is 1 + 161 + 9 = 171 bytes on air, inside the 172 receive limit and the
174 - path_len send limit up to three hops; beyond that no parity is sent.

**Receiver.** `_ReassemblyBucket` keeps the parity fragment; whenever exactly one covered data
fragment is missing and the parity is held, it is reconstructed (XOR of parity and the others, the
last one trimmed to `last_covered_len`), the bucket completes, and the report says so. The bitmap
reports data fragments only.

**Re-drives.** A round that re-drives two or more fragments at one hop or more sends a fresh parity
over the re-driven set (its own coverage mask).

**Expected.** With ~18 % per-fragment loss at one hop, a 3-fragment part loses exactly one fragment
41 % of the time and none 55 %: parity turns most of that 41 % into a first-round completion at the
cost of one extra fragment per burst (+33 % fragment airtime on a burst that would otherwise need a
report + a re-burst + a second report). MeshBench `large_payload` (one hop, repeater-side loss real,
sender-side collisions overstated) decides whether it stays.

## Pure timing functions (tested directly, each pinned to its field number)

| function | inputs | rule | pinned to |
|---|---|---|---|
| `report_hold_s(airtime_s, hops)` | fragment airtime, hop count | `airtime + relay_gap(hops)` | receiver gap 0.22-0.43 s at hop 0; one relay per hop |
| `noack_frame_hold_s(on_air_bytes, hops)` | report size, hops | `_raw_fragment_gap_s` rule | the raw gap's field derivation |
| `window_collect_s()` | -- | 0.75 s | RNS emits a window within ms |
| `window_report_wait_s(...)` | as `_completion_report_wait_s` | unchanged, per window | phase 1.5 |
| `raw_payload_budget(cap, path_len, header)` | -- | `min(cap, 172, 174 - path_len) - header` | firmware limits |
| `parity_fragments(hops)` | hops | 0 at hop 0, else 1 | 18 % per-fragment loss at one hop |
| `parity_fits(budget, path_len)` | -- | 1 + budget + 9 <= min(172, 174 - path_len) | firmware limits |

## Order, gates and what "regress" means

M1, M2, M3, M4, one commit each with its golden-snapshot update where a frame changes, its tests,
its history and changelog entries. Each milestone's gate is the full unit suite plus MeshBench
`large_payload` and `relay`, two runs each, read against the PREVIOUS milestone's runs (mechanics
identical; the on-air bytes per RNS byte, reports and QUERY attempts per part, and per-part
completion time are the numbers; a delivery difference inside the spread is not evidence). A
milestone whose mechanics fail twice is reverted and the reason recorded.

## Out of scope here

The CHANNEL path, bind frames and path discovery are untouched; DIRECT stays primary with no
CHANNEL fallback; `_register_peer` and `_send_direct_packet` stay the single entry points.
