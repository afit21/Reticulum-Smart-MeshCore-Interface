#!/usr/bin/env python3
"""
field_ab_compare.py

Side-by-side comparison of two (or more) field capture sets -- build A against
build B on the same route and the same page -- on the fields the 2026-09-20
report's "proposed field test" listed, stratified by hop count. This is the
analysis half of the A/B protocol in fieldtests/AB_PROTOCOL.md; the other half
is the procedure (same route, same page, two builds back to back or alternated
per fetch, capture on at both ends).

    python3 testscripts/field_ab_compare.py \\
        --set old=fieldtests/raw/2026-09-19-evening \\
        --set new=fieldtests/raw/2026-09-21-ab-new

    # both builds in one session, alternated per fetch: split one capture
    # directory by wall-clock windows (local time, ISO or HH:MM)
    python3 testscripts/field_ab_compare.py \\
        --set old=fieldtests/raw/2026-09-21 --window old=10:00..10:40 \\
        --set new=fieldtests/raw/2026-09-21 --window new=10:40..11:20

    # only one node's captures (the page server), only one hop count
    python3 testscripts/field_ab_compare.py --set old=... --set new=... --node afipc --hop 1

Each set is every capture_*.jsonl under the path(s) given (a directory is
searched recursively, a file is taken as is), optionally cut to a time window.
Sets are compared per hop count of the DIRECT path the sender had resolved at
the time (`hop_count` on the attempt / fragment records), because every rate
and latency here tracks hop count more than anything else (2026-09-19: hop 0
~92-96% attempt success and ~1.3 s ACK; hop 1 ~65% / 3.1 s; hop 2 ~51% / 4.0 s;
hop 3 ~42% / 5.6 s) and a set with a different hop mix is not comparable in
aggregate.

WHAT IS COMPARED (per set, per hop, with n)

  attempts       DIRECT attempt success (`direct_attempt_result.ok`), ACK latency median / p90,
                 missed-attempt `ack_timeout_s` median / max, post-attempt `listen_delay_s` median / max,
                 `lock_wait_s` median / max, `quiet_hold_s` sum, `miss_diagnosis` counts
  completion     `completion_check_result` outcomes (reported / reported_stale / answered / timeout),
                 QUERY attempts per raw send (`kind=completion_query` / `direct_send_result method=raw`),
                 timeout-outcome durations
  part time      per raw pkt_id, first `raw_fragment_sent` -> first `completion_check_result` with
                 complete=true (median / p90), duty-cycle waits excluded where recorded
  handshakes     LINKREQUEST out -> the next LRPROOF in (median / p90 / max, count within 15 s)
  backoff        `unknown_dest_backoff_drop` records, and how many while PROOFs were arriving
  airtime        RNS bytes out / in, raw fragment bytes, `channel_fragment_sent`, `rx_log` TEXT_MSG frames
                 of 36-44 B (QUERY / ANSWER / REPORT class) per delivered raw send; and, since the
                 2026-09-20 airtime pass, ON-AIR BYTES PER DELIVERED RNS BYTE -- this node's own
                 transmissions (`on_air_bytes` on every `direct_attempt_result`, `raw_fragment_sent`
                 and `channel_fragment_sent` record; records from builds before that field are
                 counted at their frame size) divided by the RNS bytes of its DIRECT sends that
                 completed (`direct_send_result ok=true`, `size_bytes`). The headline metric of that
                 pass, lower is better; the MeshBench ledger (`meshbench_report.py`) is the
                 all-nodes equivalent. Captures without `on_air_bytes` print the ratio with a '~'.
  stale paths    `direct_send_result` failures and consecutive-failure triples (the stale-path reset trigger)

Nothing here decides; it puts the two columns next to each other with their
sample sizes so the decision is made on numbers, and prints a one-line
caution when a hop bucket has fewer than --min-n attempts in either set.
"""
import argparse
import collections
import glob
import json
import os
import statistics
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from meshbench_report import dist, fmt, load_jsonl, med, pct  # noqa: E402

DEADLINE_S = 15.0        # MeshChat's NomadNet link window
# QUERY / ANSWER / REPORT ride TEXT_MSG frames whose rx_log payload_length is
# 38-40 B (measured across the 2026-09-19/20 field captures: 276 of 1157
# TEXT_MSG frames were exactly 38 B; data-carrying ones are 54-168 B).
CONTROL_FRAME_BYTES = range(36, 45)


def parse_when(text: str, reference: float) -> float:
    """ISO 8601 (local), or HH:MM[:SS] on the day of `reference`."""
    text = text.strip()
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        pass
    day = datetime.fromtimestamp(reference).date()
    for fmt_ in ("%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(text, fmt_).time()
            return datetime.combine(day, t).timestamp()
        except ValueError:
            continue
    raise SystemExit(f"cannot parse time {text!r} (ISO 8601 or HH:MM)")


def node_of(path: str) -> str:
    """The interface writes capture_<iface name>_<stamp>.jsonl, or since
    alpha 0.1.5 <node label>_capture_<iface name>_<stamp>.jsonl (the label
    is the MeshCore node name or packet_capture_label); the older field
    captures under fieldtests/raw/ were renamed by hand to
    <machine>_<iface name>_<stamp>[_<tag>].jsonl. Either way the node is
    the label when there is one, else what precedes the first
    timestamp-looking token."""
    stem = os.path.basename(path)[:-len(".jsonl")]
    parts = stem.split("_")
    if parts[0] == "capture":
        parts = parts[1:]
    elif "capture" in parts:
        # alpha 0.1.5 item 7: <label>_capture_<iface>_<stamp>.jsonl -- the
        # node is the label the interface wrote (its MeshCore name).
        parts = parts[:parts.index("capture")]
    keep = []
    for part in parts:
        if part[:8].isdigit() and len(part) >= 8:
            break
        keep.append(part)
    return "_".join(keep) or stem


def collect(paths: list, node_filter: str = None) -> list:
    files = []
    for p in paths:
        if os.path.isdir(p):
            files += sorted(glob.glob(os.path.join(p, "**", "*.jsonl"), recursive=True))
        elif os.path.isfile(p):
            files.append(p)
        else:
            files += sorted(glob.glob(p))
    recs = []
    for f in files:
        node = node_of(f)
        if node_filter and node_filter not in node:
            continue
        for r in load_jsonl(f):
            r["_node"] = node
            r["_file"] = f
            recs.append(r)
    recs.sort(key=lambda r: r.get("ts", 0))
    return recs


def analyse_set(recs: list, hop_filter=None, radio=(7, 62.5, 8)) -> dict:
    out = {"records": len(recs), "nodes": sorted({r["_node"] for r in recs}),
           "span_s": (recs[-1]["ts"] - recs[0]["ts"]) if len(recs) > 1 else 0.0}
    att = [r for r in recs if r.get("event") == "direct_attempt_result"
           # neither a success nor a failure of the path (phase 1, 2026-09-20)
           and r.get("ack_timeout_source") not in ("expired", "answered", "answered_before_send", "preempted")]
    if hop_filter is not None:
        att = [r for r in att if r.get("hop_count") == hop_filter]
    by_hop = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in att:
        h = r.get("hop_count")
        b = by_hop[h]
        b["ok"].append(1 if r.get("ok") else 0)
        if r.get("ok") and r.get("ack_latency_s") is not None:
            b["ack"].append(r["ack_latency_s"])
        if not r.get("ok") and r.get("ack_timeout_s") is not None:
            b["timeout"].append(r["ack_timeout_s"])
        if r.get("listen_delay_s") is not None:
            b["listen"].append(r["listen_delay_s"])
        if r.get("lock_wait_s") is not None:
            b["lock"].append(r["lock_wait_s"])
        if r.get("quiet_hold_s"):
            b["quiet_hold"].append(r["quiet_hold_s"])
        if not r.get("ok"):
            b["diag"].append(r.get("miss_diagnosis"))
        b["kind"].append(r.get("kind"))
    out["by_hop"] = {}
    for h, b in sorted(by_hop.items(), key=lambda kv: (kv[0] is None, kv[0] if kv[0] is not None else -1)):
        out["by_hop"][h] = {
            "attempts": len(b["ok"]), "success": (sum(b["ok"]) / len(b["ok"])) if b["ok"] else None,
            "ack": dist(b["ack"]), "missed_timeout": dist(b["timeout"]), "listen": dist(b["listen"]),
            "lock": dist(b["lock"]), "quiet_hold_sum": sum(b["quiet_hold"]),
            "diag": dict(collections.Counter(b["diag"])), "kinds": dict(collections.Counter(b["kind"])),
        }
    cc = [r for r in recs if r.get("event") == "completion_check_result"]
    out["completion"] = dict(collections.Counter(r.get("outcome") for r in cc))
    out["completion_n"] = len(cc)
    out["completion_timeout_s"] = dist([r.get("timeout_s") for r in cc if r.get("outcome") == "timeout"])
    dsr = [r for r in recs if r.get("event") == "direct_send_result"]
    raw_sends = [r for r in dsr if r.get("method") == "raw"]
    queries = [r for r in att if r.get("kind") == "completion_query"]
    out["raw_sends"] = len(raw_sends)
    out["queries"] = len(queries)
    out["queries_per_raw_send"] = (len(queries) / len(raw_sends)) if raw_sends else None
    out["send_fail"] = sum(1 for r in dsr if not r.get("ok"))
    out["send_ok"] = sum(1 for r in dsr if r.get("ok"))
    # consecutive failure triples per peer: the stale-path reset trigger
    streak, triples = collections.Counter(), 0
    for r in dsr:
        peer = r.get("peer_prefix")
        if r.get("ok"):
            streak[peer] = 0
        else:
            streak[peer] += 1
            if streak[peer] == 3:
                triples += 1
    out["fail_triples"] = triples
    # part time per pkt_id (first raw fragment -> known complete), by hop
    first_frag, hop_of, dc_wait = {}, {}, collections.Counter()
    for r in recs:
        if r.get("event") == "raw_fragment_sent":
            key = (r["_node"], r["pkt_id"])
            first_frag.setdefault(key, r["ts"])
            hop_of.setdefault(key, r.get("hop_count"))
            if r.get("duty_cycle_wait_s"):
                dc_wait[key] += r["duty_cycle_wait_s"]
    part_time = collections.defaultdict(list)
    seen = set()
    for r in recs:
        if r.get("event") == "completion_check_result" and r.get("complete"):
            key = (r["_node"], r["pkt_id"])
            if key in first_frag and key not in seen:
                seen.add(key)
                h = hop_of.get(key)
                if hop_filter is None or h == hop_filter:
                    part_time[h].append(r["ts"] - first_frag[key] - dc_wait.get(key, 0.0))
    out["part_time"] = {h: dist(v) for h, v in sorted(part_time.items(), key=lambda kv: (kv[0] is None, kv[0] if kv[0] is not None else -1))}
    out["parts_started"] = len(first_frag)
    out["parts_completed"] = len(seen)
    # handshakes
    pk = [r for r in recs if "event" not in r]
    lr_out = [(r["ts"], r["_node"]) for r in pk if r.get("direction") == "out" and r.get("packet_type_name") == "LINKREQUEST"]
    lrp_in = sorted(r["ts"] for r in pk if r.get("direction") == "in" and r.get("packet_type_name") == "PROOF"
                    and (r.get("context_name") or "") == "LRPROOF")
    hs = []
    j = 0
    for t, _ in sorted(lr_out):
        while j < len(lrp_in) and lrp_in[j] < t:
            j += 1
        if j < len(lrp_in) and lrp_in[j] - t <= 120.0:
            hs.append(lrp_in[j] - t)
            j += 1
    out["handshakes"] = dist(hs)
    out["handshakes_within_deadline"] = sum(1 for x in hs if x <= DEADLINE_S)
    out["linkrequests"] = len(lr_out)
    # backoff drops, and while proofs were arriving (within 60 s of a PROOF in)
    proofs_in = sorted(r["ts"] for r in pk if r.get("direction") == "in" and r.get("packet_type_name") == "PROOF")
    drops = [r["ts"] for r in pk if r.get("routing_decision") == "unknown_dest_backoff_drop"]
    out["backoff_drops"] = len(drops)
    out["backoff_drops_near_proofs"] = sum(1 for t in drops if any(abs(t - p) <= 60.0 for p in proofs_in))
    # airtime
    out["rns_bytes_out"] = sum(r.get("size_bytes") or 0 for r in pk if r.get("direction") == "out")
    out["rns_bytes_in"] = sum(r.get("size_bytes") or 0 for r in pk if r.get("direction") == "in")
    out["raw_bytes"] = sum(r.get("size_bytes") or 0 for r in recs if r.get("event") == "raw_fragment_sent")
    out["raw_fragments"] = sum(1 for r in recs if r.get("event") == "raw_fragment_sent")
    out["channel_fragments"] = sum(1 for r in recs if r.get("event") == "channel_fragment_sent")
    # On-air bytes per delivered RNS byte (2026-09-20): own transmissions
    # over the RNS bytes of the DIRECT sends that completed.
    on_air = 0
    estimated = False
    for r in recs:
        ev = r.get("event")
        if ev == "direct_attempt_result":
            if r.get("ack_timeout_source") in ("expired", "answered", "answered_before_send"):
                continue   # never keyed the radio
            b = r.get("on_air_bytes")
            if b is None:
                estimated = True
                b = 40 if r.get("kind") in ("completion_query", "completion_answer", "completion_report") else 120
            on_air += b
        elif ev == "raw_fragment_sent" and r.get("ok", True):
            b = r.get("on_air_bytes")
            if b is None:
                estimated = True
                b = 2 + (r.get("path_len") or 0) + (r.get("size_bytes") or 0)
            on_air += b
        elif ev == "channel_fragment_sent" and r.get("ok", True):
            b = r.get("on_air_bytes")
            if b is None:
                estimated = True
                b = (r.get("size_bytes") or 0) + 16
            on_air += b
    delivered = sum(r.get("size_bytes") or 0 for r in dsr if r.get("ok"))
    out["on_air_bytes"] = on_air
    out["rns_bytes_delivered"] = delivered
    out["on_air_per_delivered_rns_byte"] = (on_air / delivered) if delivered else None
    out["on_air_ratio_estimated"] = estimated
    rx = [r for r in recs if r.get("event") == "rx_log"]
    out["rx_log_frames"] = len(rx)
    control = sum(1 for r in rx if (r.get("payload_typename") or "") == "TEXT_MSG" and (r.get("payload_length") or 0) in CONTROL_FRAME_BYTES)
    out["control_frames_overheard"] = control
    out["control_frames_per_raw_send"] = (control / len(raw_sends)) if raw_sends and control else None
    out["routing"] = dict(collections.Counter(r.get("routing_decision") for r in pk if r.get("direction") == "out"))
    out["in_transport"] = dict(collections.Counter(r.get("transport") for r in pk if r.get("direction") == "in"))
    # Alpha 0.1.5 (item 4): the safety signals of the one-hop gap A/B
    # (`direct_raw_gap_own_airtime`), per hop. Round-0 loss per fragment
    # position is the sender's view -- a data fragment re-sent in round 1
    # -- since a sender's capture does not see what landed except through
    # the report. Round-1 data fragments per part, and the receiver-side
    # parity reconstructions (both nodes' captures in one set give both
    # views). `gap_s` is the gap the sender actually used, by hop.
    rf = [r for r in recs if r.get("event") == "raw_fragment_sent" and r.get("ok", True)]
    if hop_filter is not None:
        rf = [r for r in rf if r.get("hop_count") == hop_filter]
    parts = collections.defaultdict(lambda: {"r0": set(), "r1": [], "hop": None})
    gaps = collections.defaultdict(list)
    for r in rf:
        key = (r["_node"], r.get("pkt_id"))
        p = parts[key]
        p["hop"] = r.get("hop_count")
        if r.get("parity_mask") is not None:
            continue   # parity frames are not positions
        if r.get("round") == 0:
            p["r0"].add(r.get("frag_idx"))
        elif r.get("round") == 1:
            p["r1"].append(r.get("frag_idx"))
        if r.get("gap_s") is not None:
            gaps[r.get("hop_count")].append(r["gap_s"])
    pos = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0]))   # hop -> idx -> [resent, sent]
    r1_per_part = collections.defaultdict(list)
    for p in parts.values():
        if not p["r0"]:
            continue
        r1 = set(p["r1"])
        for idx in p["r0"]:
            pos[p["hop"]][idx][1] += 1
            if idx in r1:
                pos[p["hop"]][idx][0] += 1
        r1_per_part[p["hop"]].append(len(p["r1"]))
    out["round0_resent_by_position"] = {
        h: {idx: (v[0] / v[1] if v[1] else None, v[1]) for idx, v in sorted(d.items(), key=lambda kv: (kv[0] is None, kv[0] or 0))}
        for h, d in sorted(pos.items(), key=lambda kv: (kv[0] is None, kv[0] if kv[0] is not None else -1))}
    out["round1_fragments_per_part"] = {
        h: (sum(v) / len(v) if v else None, len(v)) for h, v in sorted(r1_per_part.items(), key=lambda kv: (kv[0] is None, kv[0] if kv[0] is not None else -1))}
    out["gap_s_by_hop"] = {h: dist(v) for h, v in sorted(gaps.items(), key=lambda kv: (kv[0] is None, kv[0] if kv[0] is not None else -1))}
    # Receiver side: reconstructions by the receiver's hop count to the sender
    # (its `fragment_received` records do not carry hops; use the sender node's
    # hop count for that (node, pkt) when both captures are in the set, else
    # count them unstratified).
    sender_hop = {}
    for r in recs:
        if r.get("event") == "raw_fragment_sent":
            sender_hop[(r["_node"], r.get("pkt_id"))] = r.get("hop_count")
    recon = collections.Counter()
    for r in recs:
        if r.get("event") == "raw_parity_reconstructed":
            hop = next((h for (node, pkt), h in sender_hop.items() if pkt == r.get("pkt_id") and node != r["_node"]), None)
            recon[hop] += 1
    out["parity_reconstructions_by_hop"] = dict(recon)
    out["parity_fragments_sent_by_hop"] = dict(collections.Counter(
        r.get("hop_count") for r in rf if r.get("parity_mask") is not None))
    # Alpha 0.1.5 (item 8): estimator calibration -- the firmware's measured
    # transmit time (CMD_GET_STATS, whole seconds) against the interface's
    # summed airtime estimate, first to last `radio_stats` record per node.
    # Alpha 0.1.6 (item 5): the firmware's transmit time includes every
    # frame the RADIO sent that the interface never keyed -- the ACK it
    # returns for each ACK-able frame it receives, PATH returns, its own
    # adverts (about 200 ACKs in the laptop's 32-minute 2026-09-21 capture:
    # its raw ratio was 0.56 while the desktop's, which received little,
    # was 0.93). The packet counters give the radio's total transmissions;
    # the frames the interface keyed are known; the remainder is priced as
    # ACKs at the ACK's airtime and taken out of the firmware seconds.
    # Per capture FILE, then summed: the interface's own counters restart
    # with the process (the laptop's session was four files), while the
    # firmware's counters run on across restarts.
    calib = {}
    for node in out["nodes"]:
        by_file = collections.defaultdict(list)
        for r in recs:
            if r.get("event") == "radio_stats" and r["_node"] == node and r.get("tx_air_secs") is not None:
                by_file[r.get("_file")].append(r)
        parts = [calibration(rs[0], rs[-1], radio=radio) for rs in by_file.values() if len(rs) >= 2]
        if parts:
            calib[node] = sum_calibrations(parts)
    out["estimator_calibration"] = calib
    # Alpha 0.1.7 (item 4): proof turnaround per hop -- an inbound DATA to
    # the proof RNS answers it with leaving the radio -- and LXMF-style
    # duplicate deliveries. The 2026-09-22 one-hop session: one 211 B LXMF
    # message arrived six times in 70 s because each proof left 5-20 s
    # after its DATA (LXMF re-sends after DELIVERY_RETRY_WAIT 10 s), so
    # these two numbers read together say whether the proof is fast
    # enough. The capture holds no packet bytes, so the join is by order:
    # RNS proves synchronously inside `owner.inbound`, and the `out PROOF`
    # (context NONE) packet record follows its `in DATA` on the same
    # second; that proof's `direct_send_result` (same destination_hash)
    # is when it left the radio. Stratified by the DATA's hop count.
    out["proof_turnaround"], out["proof_pending"] = proof_turnaround(recs, hop_filter=hop_filter)
    out["lxmf_duplicates"] = duplicate_deliveries(recs, hop_filter=hop_filter)
    return out


PROOF_JOIN_S = 2.0          # an `out PROOF` this soon after an `in DATA` answers it
DUPLICATE_WINDOW_S = 30.0   # LXMF re-sends every 10-14 s; six copies span ~70 s, so a chain of 30 s links


def proof_turnaround(recs: list, hop_filter=None) -> "tuple[dict, int]":
    """Per hop: the seconds from an inbound DATA packet to the
    `direct_send_result` of the plain PROOF answering it (see
    `analyse_set`); also how many such proofs never got a send result in
    the capture (dropped, expired, or still queued at the end)."""
    pk = [r for r in recs if "event" not in r]
    dsr_by_dest = collections.defaultdict(list)
    for r in recs:
        if r.get("event") == "direct_send_result" and r.get("destination_hash"):
            dsr_by_dest[(r["_node"], r["destination_hash"])].append(r["ts"])
    last_data = {}   # node -> (ts, hop_count) of the latest inbound DATA
    by_hop = collections.defaultdict(list)
    pending = 0
    for r in sorted(pk, key=lambda x: x["ts"]):
        node = r["_node"]
        if r.get("direction") == "in" and r.get("packet_type_name") == "DATA":
            last_data[node] = (r["ts"], r.get("hop_count"))
            continue
        if not (r.get("direction") == "out" and r.get("packet_type_name") == "PROOF"
                and (r.get("context_name") or "NONE") == "NONE"):
            continue
        data = last_data.get(node)
        if data is None or r["ts"] - data[0] > PROOF_JOIN_S:
            continue
        hop = data[1]
        if hop_filter is not None and hop != hop_filter:
            continue
        sent = [t for t in dsr_by_dest.get((node, r.get("destination_hash")), []) if t >= r["ts"]]
        if not sent:
            pending += 1
            continue
        by_hop[hop].append(sent[0] - data[0])
    return ({h: dist(v) for h, v in sorted(by_hop.items(), key=lambda kv: (kv[0] is None, kv[0] if kv[0] is not None else -1))},
            pending)


def duplicate_deliveries(recs: list, hop_filter=None) -> dict:
    """LXMF-style duplicate deliveries: an inbound DATA (context NONE, a
    SINGLE destination) to the same destination with the same size within
    DUPLICATE_WINDOW_S of the previous copy. Returns per hop the number of
    copies beyond the first (`repeats`) and the messages they belong to
    (`messages`), plus the worst chain length."""
    last = {}   # (node, dest, size) -> ts of the previous copy
    chain = collections.Counter()
    repeats = collections.defaultdict(int)
    messages = collections.defaultdict(set)
    longest = 0
    for r in sorted((r for r in recs if "event" not in r), key=lambda x: x["ts"]):
        if not (r.get("direction") == "in" and r.get("packet_type_name") == "DATA"
                and (r.get("context_name") or "NONE") == "NONE"
                and (r.get("destination_type_name") or "SINGLE") == "SINGLE"):
            continue
        hop = r.get("hop_count")
        if hop_filter is not None and hop != hop_filter:
            continue
        key = (r["_node"], r.get("destination_hash"), r.get("size_bytes"))
        prev = last.get(key)
        last[key] = r["ts"]
        if prev is not None and r["ts"] - prev <= DUPLICATE_WINDOW_S:
            chain[key] += 1
            repeats[hop] += 1
            messages[hop].add(key)
            longest = max(longest, chain[key] + 1)
        else:
            chain[key] = 0
    return {"repeats_by_hop": dict(repeats), "messages_by_hop": {h: len(v) for h, v in messages.items()},
            "longest_chain": longest}


def sum_calibrations(parts: list) -> dict:
    """One node's calibration over several capture files (pure)."""
    total = {k: sum(p.get(k) or 0 for p in parts) for k in (
        "firmware_tx_air_s", "estimated_tx_air_s", "frames", "radio_frames_sent", "firmware_only_frames",
        "firmware_only_air_s", "corrected_firmware_tx_air_s")}
    fw, est = total["firmware_tx_air_s"], total["estimated_tx_air_s"]
    out = {"firmware_tx_air_s": fw, "estimated_tx_air_s": round(est, 1), "frames": total["frames"],
           "estimate_over_firmware": (round(est / fw, 3) if fw else None), "records": len(parts)}
    if any(p.get("radio_frames_sent") is not None for p in parts):
        corrected = total["corrected_firmware_tx_air_s"]
        out.update({"radio_frames_sent": total["radio_frames_sent"], "firmware_only_frames": total["firmware_only_frames"],
                    "firmware_only_air_s": round(total["firmware_only_air_s"], 1),
                    "ack_airtime_s": next(p["ack_airtime_s"] for p in parts if "ack_airtime_s" in p),
                    "corrected_firmware_tx_air_s": round(corrected, 1),
                    "estimate_over_corrected": (round(est / corrected, 3) if corrected > 0 else None)})
    return out


def lora_airtime_s(nbytes: int, sf: int, bw_khz: float, cr: int) -> float:
    """The interface's own LoRa time-on-air model (`_estimate_airtime_s`:
    explicit header, CRC, low-data-rate optimisation above 16 ms symbols,
    preamble 32 symbols at SF<=8 else 16, as the firmware configures)."""
    tsym = (2 ** sf) / (bw_khz * 1000.0)
    n_preamble = 32 if sf <= 8 else 16
    t_preamble = (n_preamble + 4.25) * tsym
    de = 1 if tsym > 0.016 else 0
    num = 8 * max(1, nbytes) - 4 * sf + 28 + 16
    den = 4 * (sf - 2 * de)
    payload_symbols = 8 + max(0, -(-num // den)) * cr
    return t_preamble + payload_symbols * tsym


ACK_ON_AIR_BYTES = 8   # header + path_len + a 1-2 byte path + the 4-byte ACK code


def calibration(first: dict, last: dict, radio=(7, 62.5, 8)) -> dict:
    """The estimator calibration between two `radio_stats` records (pure):
    the raw ratio `estimate / firmware tx air` and, when the packet counters
    are present, the ratio corrected for the frames the radio sent on its
    own -- `packets_sent` (or flood_tx + direct_tx) minus `frames_keyed` --
    each priced at an ACK's airtime at `radio` (sf, bw kHz, cr). The raw
    ratio is a calibration only on a node that receives little; the
    corrected one is what to read on a receiver."""
    fw = last["tx_air_secs"] - first["tx_air_secs"]
    est = (last.get("estimated_tx_air_s") or 0.0) - (first.get("estimated_tx_air_s") or 0.0)
    frames = (last.get("frames_keyed") or 0) - (first.get("frames_keyed") or 0)

    def sent(r):
        if r.get("packets_sent") is not None:
            return r["packets_sent"]
        if r.get("flood_tx") is not None or r.get("direct_tx") is not None:
            return (r.get("flood_tx") or 0) + (r.get("direct_tx") or 0)
        return None

    radio_sent = None if sent(first) is None or sent(last) is None else sent(last) - sent(first)
    out = {"firmware_tx_air_s": fw, "estimated_tx_air_s": round(est, 1), "frames": frames,
           "estimate_over_firmware": (round(est / fw, 3) if fw else None), "records": None}
    if radio_sent is not None:
        extra = max(0, radio_sent - frames)
        ack_s = lora_airtime_s(ACK_ON_AIR_BYTES, *radio)
        corrected_fw = fw - extra * ack_s
        out.update({"radio_frames_sent": radio_sent, "firmware_only_frames": extra,
                    "firmware_only_air_s": round(extra * ack_s, 1), "ack_airtime_s": round(ack_s, 3),
                    "corrected_firmware_tx_air_s": round(corrected_fw, 1),
                    "estimate_over_corrected": (round(est / corrected_fw, 3) if corrected_fw > 0 else None)})
    return out


def d_str(d, nd=1):
    if not d or not d.get("n"):
        return "-"
    return f"{fmt(d['med'], nd)}/{fmt(d['p90'], nd)}/{fmt(d['max'], nd)} (n={d['n']})"


def print_comparison(sets: dict, min_n: int) -> None:
    names = list(sets)
    width = max(28, *(len(n) for n in names)) + 2

    def row(label, values):
        print(f"  {label:<44}" + "".join(f"{str(v):<{width}}" for v in values))
    print("field".ljust(46) + "".join(n.ljust(width) for n in names))
    print("-" * (46 + width * len(names)))
    row("records / nodes", [f"{s['records']} / {','.join(s['nodes'])}" for s in sets.values()])
    row("capture span (min)", [f"{s['span_s'] / 60:.0f}" for s in sets.values()])
    hops = sorted({h for s in sets.values() for h in s["by_hop"]}, key=lambda h: (h is None, h if h is not None else -1))
    for h in hops:
        print(f"\n  -- DIRECT attempts at hop {h} --")
        bs = [s["by_hop"].get(h) for s in sets.values()]
        thin = [n for n, b in zip(names, bs) if not b or b["attempts"] < min_n]
        row("attempts (n)", [b["attempts"] if b else 0 for b in bs])
        row("success", [f"{b['success']:.0%}" if b and b["success"] is not None else "-" for b in bs])
        row("ACK latency med/p90/max s", [d_str(b["ack"], 2) if b else "-" for b in bs])
        row("missed-attempt ack_timeout med/p90/max s", [d_str(b["missed_timeout"]) if b else "-" for b in bs])
        row("post-attempt listen med/p90/max s", [d_str(b["listen"], 2) if b else "-" for b in bs])
        row("lock wait med/p90/max s", [d_str(b["lock"]) if b else "-" for b in bs])
        row("quiet hold sum s", [fmt(b["quiet_hold_sum"]) if b else "-" for b in bs])
        row("miss diagnosis", [b["diag"] if b else "-" for b in bs])
        row("attempt kinds", [b["kinds"] if b else "-" for b in bs])
        if thin:
            print(f"  ! fewer than {min_n} attempts at hop {h} in {thin}: not evidence")
    print("\n  -- completion (sender side) --")
    row("checks (n)", [s["completion_n"] for s in sets.values()])
    row("outcomes", [s["completion"] for s in sets.values()])
    row("timeout-outcome duration med/p90/max s", [d_str(s["completion_timeout_s"]) for s in sets.values()])
    row("raw sends / QUERY attempts", [f"{s['raw_sends']} / {s['queries']}" for s in sets.values()])
    row("QUERY attempts per raw send", [fmt(s["queries_per_raw_send"], 2) for s in sets.values()])
    print("\n  -- part time: first raw fragment -> known complete (duty-cycle waits excluded) --")
    row("parts started / completed", [f"{s['parts_started']} / {s['parts_completed']}" for s in sets.values()])
    for h in sorted({h for s in sets.values() for h in s["part_time"]}, key=lambda h: (h is None, h if h is not None else -1)):
        row(f"hop {h} med/p90/max s", [d_str(s["part_time"].get(h)) for s in sets.values()])
    print("\n  -- proof turnaround: inbound DATA -> its plain PROOF's direct_send_result (item 1 of 0.1.7; field 5-20 s at one hop) --")
    for h in sorted({h for s in sets.values() for h in s["proof_turnaround"]}, key=lambda h: (h is None, h if h is not None else -1)):
        row(f"hop {h} med/p90/max s", [d_str(s["proof_turnaround"].get(h)) for s in sets.values()])
    row("proofs with no send result", [s["proof_pending"] for s in sets.values()])
    print("\n  -- LXMF-style duplicate deliveries: same destination and size within 30 s (field: six copies of one message) --")
    for h in sorted({h for s in sets.values() for h in s["lxmf_duplicates"]["repeats_by_hop"]}, key=lambda h: (h is None, h if h is not None else -1)):
        row(f"hop {h} repeat copies (messages)", [f"{s['lxmf_duplicates']['repeats_by_hop'].get(h, 0)} ({s['lxmf_duplicates']['messages_by_hop'].get(h, 0)})"
                                                 for s in sets.values()])
    row("longest chain of copies", [s["lxmf_duplicates"]["longest_chain"] for s in sets.values()])
    print("\n  -- link handshakes (LINKREQUEST out -> LRPROOF in) --")
    row("LINKREQUESTs / matched", [f"{s['linkrequests']} / {s['handshakes']['n']}" for s in sets.values()])
    row("med/p90/max s", [d_str(s["handshakes"]) for s in sets.values()])
    row(f"within {DEADLINE_S:.0f} s", [f"{s['handshakes_within_deadline']}/{s['handshakes']['n']}" for s in sets.values()])
    print("\n  -- backoff and stale paths --")
    row("unknown_dest_backoff_drop (near PROOFs)", [f"{s['backoff_drops']} ({s['backoff_drops_near_proofs']})" for s in sets.values()])
    row("direct sends ok / failed", [f"{s['send_ok']} / {s['send_fail']}" for s in sets.values()])
    row("failure triples (stale-path reset trigger)", [s["fail_triples"] for s in sets.values()])
    print("\n  -- one-hop gap A/B safety signals (direct_raw_gap_own_airtime) --")
    hops_seen = sorted({h for a in sets.values() for k in ("gap_s_by_hop", "round1_fragments_per_part") for h in a.get(k, {})},
                       key=lambda h: (h is None, h if h is not None else -1))
    for h in hops_seen:
        row(f"h{h} gap used med/p90/max s", [d_str(a.get("gap_s_by_hop", {}).get(h), 2) for a in sets.values()])
        row(f"h{h} round-1 data frags per part (parts)",
            [(f"{fmt(v[0], 2)} ({v[1]})" if v and v[0] is not None else "-")
             for v in (a.get("round1_fragments_per_part", {}).get(h) for a in sets.values())])
        idxs = sorted({i for a in sets.values() for i in a.get("round0_resent_by_position", {}).get(h, {})}, key=lambda i: (i is None, i or 0))
        for idx in idxs:
            row(f"h{h} round-0 frag {idx} re-sent (sent)",
                [(f"{fmt(v[0] * 100, 0)}% ({v[1]})" if v and v[0] is not None else "-")
                 for v in (a.get("round0_resent_by_position", {}).get(h, {}).get(idx) for a in sets.values())])
        row(f"h{h} parity sent / reconstructed",
            [f"{a.get('parity_fragments_sent_by_hop', {}).get(h, 0)} / {a.get('parity_reconstructions_by_hop', {}).get(h, 0)}"
             for a in sets.values()])
    print("\n  -- airtime estimator vs the radio's own transmit time (radio_stats, item 8; corrected line item 5 of 0.1.6) --")
    nodes_seen = sorted({n for a in sets.values() for n in a.get("estimator_calibration", {})})
    for node in nodes_seen:
        row(f"{node}: RAW estimate / firmware tx air s (frames keyed)",
            [(f"{c['estimated_tx_air_s']} / {c['firmware_tx_air_s']} = {fmt(c['estimate_over_firmware'], 2)} ({c['frames']})" if c else "-")
             for c in (a.get("estimator_calibration", {}).get(node) for a in sets.values())])
        row(f"{node}: CORRECTED estimate / (firmware - radio's own ACKs) (radio frames - keyed = ACK-priced)",
            [(f"{c['estimated_tx_air_s']} / {c['corrected_firmware_tx_air_s']} = {fmt(c['estimate_over_corrected'], 2)} "
              f"({c['radio_frames_sent']} - {c['frames']} = {c['firmware_only_frames']} x {c['ack_airtime_s']} s)"
              if c and c.get("radio_frames_sent") is not None else "-")
             for c in (a.get("estimator_calibration", {}).get(node) for a in sets.values())])
    if nodes_seen:
        print("  (the RAW ratio counts the ACKs the radio sends for every ACK-able frame it receives against the interface;"
              " read the CORRECTED one on a node that receives a lot)")
    else:
        print("  (no radio_stats records: a pre-0.1.5 capture, or a firmware without CMD_GET_STATS)")
    print("\n  -- airtime --")
    row("RNS bytes out / in", [f"{s['rns_bytes_out']} / {s['rns_bytes_in']}" for s in sets.values()])
    row("raw fragments (bytes)", [f"{s['raw_fragments']} ({s['raw_bytes']})" for s in sets.values()])
    row("channel fragments sent", [s["channel_fragments"] for s in sets.values()])
    row("rx_log frames / control-size TEXT frames", [f"{s['rx_log_frames']} / {s['control_frames_overheard']}" for s in sets.values()])
    row("control frames per raw send", [fmt(s["control_frames_per_raw_send"], 2) for s in sets.values()])
    row("on-air B per delivered RNS B (own tx / ok sends)", [
        f"{'~' if s['on_air_ratio_estimated'] else ''}{fmt(s['on_air_per_delivered_rns_byte'], 2)} "
        f"({s['on_air_bytes']} / {s['rns_bytes_delivered']})" for s in sets.values()])
    row("routing decisions", [s["routing"] for s in sets.values()])
    row("incoming transports", [s["in_transport"] for s in sets.values()])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", action="append", required=True, metavar="LABEL=PATH[,PATH...]",
                    help="A capture set: label and one or more directories / files / globs")
    ap.add_argument("--window", action="append", default=[], metavar="LABEL=START..END",
                    help="Cut that set to a wall-clock window (local ISO 8601 or HH:MM)")
    ap.add_argument("--node", default=None, help="Only this interface's captures (capture_<name>_*.jsonl)")
    ap.add_argument("--hop", type=int, default=None, help="Only attempts / parts at this hop count")
    ap.add_argument("--min-n", type=int, default=20, help="Flag hop buckets with fewer attempts than this")
    ap.add_argument("--json", action="store_true", help="Also print the analysis as JSON")
    ap.add_argument("--radio", default="7,62.5,8", type=lambda t: [float(x) if i == 1 else int(float(x)) for i, x in enumerate(t.split(","))],
                    help="SF,BW kHz,CR the captures were made at (the ACK airtime the corrected calibration prices "
                         "the radio's own frames at); the field Heltecs: 7,62.5,8")
    args = ap.parse_args()

    windows = {}
    for w in args.window:
        label, span = w.split("=", 1)
        windows[label] = span.split("..", 1)
    sets = {}
    for spec in args.set:
        label, paths = spec.split("=", 1)
        recs = collect(paths.split(","), args.node)
        if not recs:
            sys.exit(f"set {label!r}: no capture records under {paths}")
        if label in windows:
            ref = recs[0]["ts"]
            lo, hi = (parse_when(windows[label][0], ref), parse_when(windows[label][1], ref))
            recs = [r for r in recs if lo <= r.get("ts", 0) <= hi]
            if not recs:
                sys.exit(f"set {label!r}: no records inside the window {windows[label]}")
        sets[label] = analyse_set(recs, args.hop, radio=tuple(args.radio))
    print(f"field A/B comparison -- {time.strftime('%Y-%m-%d %H:%M')} -- {len(sets)} set(s)"
          + (f", node {args.node}" if args.node else "") + (f", hop {args.hop} only" if args.hop is not None else ""))
    print_comparison(sets, args.min_n)
    if args.json:
        print(json.dumps({k: {**v, "by_hop": {str(h): b for h, b in v["by_hop"].items()},
                              "part_time": {str(h): d for h, d in v["part_time"].items()}} for k, v in sets.items()},
                         indent=1, default=str))


if __name__ == "__main__":
    main()
