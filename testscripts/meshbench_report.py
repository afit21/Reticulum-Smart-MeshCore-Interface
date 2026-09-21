#!/usr/bin/env python3
"""
meshbench_report.py

Reads what a MeshBench scenario run left behind and turns it into the numbers
a change is judged by. This is the analysis that produced the 2026-09-20
comparison tables (tests/baselines/2026-09-20-meshbench-1b69fa7.md); it lived
in a scratch directory that day and moved here so every run gets it, and so
`meshbench_scenarios.py run` writes it into result.json ("analysis") instead
of leaving the summarising to whoever reads the capture next.

    python3 testscripts/meshbench_report.py /tmp/mb/x/relay-1 [...]          one block per run
    python3 testscripts/meshbench_report.py --md /tmp/mb/x/*-*               markdown rows (baseline-file format)
    python3 testscripts/meshbench_report.py --aggregate /tmp/mb/x/*-*        medians and ranges per scenario
    python3 testscripts/meshbench_report.py --bursts /tmp/mb/x/large_payload-1   per-part burst landings
    python3 testscripts/meshbench_report.py --timeline /tmp/mb/x/relay-1 [start_s end_s]   merged event timeline

INPUTS (all under one run's --capture-dir)

  run.log                  the scenario's own stdout, if the suite runner saved it (checks, verdict lines)
  result.json              the scenario's measurements and harness events
  capture_<node>_*.jsonl   the interface's packet capture per RNS end node
  meshbench_events.jsonl   MeshBench's engine event dump: tx / rx / miss with the miss reason

WHAT IT COMPUTES

  * Verdict, delivered/sent, RTT distribution (min / median / p90 / max) and late deliveries
    (PROOFs that arrived after the probe timeout -- reported separately since 2026-09-20 so the
    60 s timeout is no longer a cliff between PASS and FAIL).
  * Time-to-DIRECT-path per node (harness `direct_path` events; run.log fallback).
  * Per node, from the interface capture: DIRECT attempts ok/total by hop with median/p90 ACK
    latency; attempt kinds; completion checks by outcome; raw fragments by reconcile round;
    fragments received; direct sends by outcome; routing decisions; RNS bytes in/out; the wait
    breakdown (lock / ACK / post-send listen / quiet hold / quiet defer / duty cycle / slot).
  * From MeshBench's events: transmissions, bytes and seconds on air per node; misses by
    (receiver, cause); and for each half-duplex miss whether the receiver keyed its own transmitter
    *into* a frame it was already receiving -- the case the firmware's listen-before-talk
    (Dispatcher::checkSend defers while isReceiving()) would have prevented and MeshBench v0.1.0's
    virtual radio does not model. Those are counted as `lbt_preventable`; a change that is only
    punished by them is not being punished by anything real.
  * An airtime ledger: on-air bytes across every node per RNS byte the end nodes accepted
    (`direction=in` records), plus the same per delivered probe.
  * Per-part bursts (multi-fragment raw sends): round-0 fragments sent vs landed at the receiver
    before round 1, reconcile rounds, first fragment -> sender knows complete.
  * Link handshakes and Resource transfers (traffic modes added 2026-09-20): times, the count
    within MeshChat's 15 s window, parts / re-sent parts / status per transfer.

Nothing here asserts. The scenario runner keeps its hard checks (mechanics); these are the
measured rates and distributions to compare against the baseline file, run to run.
"""
import collections
import glob
import json
import os
import re
import statistics
import sys


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

def load_jsonl(path: str) -> list:
    out = []
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    except OSError:
        pass
    return out


def pct(vals, p):
    if not vals:
        return None
    s = sorted(vals)
    k = max(0, min(len(s) - 1, int(round((len(s) - 1) * p))))
    return s[k]


def med(vals):
    return statistics.median(vals) if vals else None


def fmt(x, nd=1):
    if x is None:
        return "-"
    if isinstance(x, float):
        return f"{x:.{nd}f}"
    return str(x)


def dist(vals: list) -> dict:
    vals = [v for v in vals if v is not None]
    if not vals:
        return {"n": 0, "min": None, "med": None, "p90": None, "max": None, "mean": None}
    return {"n": len(vals), "min": min(vals), "med": med(vals), "p90": pct(vals, 0.9), "max": max(vals),
            "mean": sum(vals) / len(vals)}


def dist_str(d: dict, nd=1) -> str:
    if not d or not d.get("n"):
        return "-"
    return f"n={d['n']} min={fmt(d['min'], nd)} med={fmt(d['med'], nd)} p90={fmt(d['p90'], nd)} max={fmt(d['max'], nd)}"


def _miss_cause(detail: str) -> str:
    return ("half-duplex" if "own transmitter" in detail else
            "locked" if "locked" in detail else
            "collision" if "decoded its header" in detail else
            "snr" if "SNR" in detail else detail[:30])


# ---------------------------------------------------------------------------
# per-node interface capture
# ---------------------------------------------------------------------------

def capture_files(run_dir: str) -> dict:
    """{node_name: [capture paths]} -- the interface names its file
    capture_<iface name>_<stamp>.jsonl."""
    out = collections.defaultdict(list)
    for cap in sorted(glob.glob(os.path.join(run_dir, "capture_*.jsonl"))):
        stem = os.path.basename(cap)[len("capture_"):-len(".jsonl")]
        node = stem.rsplit("_", 1)[0] if "_" in stem else stem
        out[node].append(cap)
    return dict(out)


def analyse_capture(recs: list) -> dict:
    n = {"records": len(recs)}
    att_all = [r for r in recs if r.get("event") == "direct_attempt_result"]
    # Attempts that never keyed the radio (expired in the lock wait, cancelled
    # by the reply, not sent because the answer was already in) or whose wait
    # was cut for a Link handshake (phase 1, 2026-09-20) are neither a success
    # nor a failure of the path; they are counted separately.
    NON_ATTEMPTS = ("expired", "answered", "answered_before_send", "preempted")
    att = [r for r in att_all if r.get("ack_timeout_source") not in NON_ATTEMPTS]
    n["attempts_not_on_air"] = dict(collections.Counter(
        r.get("ack_timeout_source") for r in att_all if r.get("ack_timeout_source") in NON_ATTEMPTS))
    by_hop = collections.defaultdict(lambda: {"ok": 0, "fail": 0, "ack": [], "lock": [], "quiet_hold": [], "timeout": []})
    waits = {"lock_wait_s": 0.0, "ack_latency_s": 0.0, "listen_delay_s": 0.0, "quiet_hold_s": 0.0,
             "quiet_defer_wait_s": 0.0, "duty_cycle_wait_s": 0.0, "medium_hold_wait_s": 0.0, "missed_ack_timeout_s": 0.0}
    for r in att:
        h = r.get("hop_count")
        b = by_hop[h]
        if r.get("ok"):
            b["ok"] += 1
            if r.get("ack_latency_s") is not None:
                b["ack"].append(r["ack_latency_s"])
                waits["ack_latency_s"] += r["ack_latency_s"]
        else:
            b["fail"] += 1
            if r.get("ack_timeout_s") is not None:
                b["timeout"].append(r["ack_timeout_s"])
                waits["missed_ack_timeout_s"] += r["ack_timeout_s"]
        if r.get("lock_wait_s") is not None:
            b["lock"].append(r["lock_wait_s"])
        if r.get("quiet_hold_s"):
            b["quiet_hold"].append(r["quiet_hold_s"])
        for key in ("lock_wait_s", "listen_delay_s", "quiet_hold_s", "quiet_defer_wait_s", "duty_cycle_wait_s", "medium_hold_wait_s"):
            if r.get(key):
                waits[key] += r[key]
    n["by_hop"] = {}
    for h, b in sorted(by_hop.items(), key=lambda kv: (kv[0] is None, kv[0] if kv[0] is not None else -1)):
        tot = b["ok"] + b["fail"]
        n["by_hop"][h] = {"attempts": tot, "ok": b["ok"], "rate": b["ok"] / tot if tot else None,
                          "ack_med": med(b["ack"]), "ack_p90": pct(b["ack"], 0.9), "ack_max": max(b["ack"]) if b["ack"] else None,
                          "lock_med": med(b["lock"]), "quiet_hold_sum": sum(b["quiet_hold"]),
                          "missed_timeout_max": max(b["timeout"]) if b["timeout"] else None}
    n["attempt_kinds"] = dict(collections.Counter(r.get("kind") for r in att))
    n["attempt_kinds_failed"] = dict(collections.Counter(r.get("kind") for r in att if not r.get("ok")))
    n["miss_diagnosis"] = dict(collections.Counter(r.get("miss_diagnosis") for r in att if not r.get("ok")))
    cc = [r for r in recs if r.get("event") == "completion_check_result"]
    n["completion"] = dict(collections.Counter(r.get("outcome") for r in cc))
    n["completion_n"] = len(cc)
    n["completion_timeout_s"] = dist([r.get("timeout_s") for r in cc if r.get("outcome") == "timeout"])
    rf = [r for r in recs if r.get("event") == "raw_fragment_sent"]
    n["raw_sent"] = len(rf)
    n["raw_rounds"] = dict(collections.Counter(r.get("round") for r in rf))
    n["raw_bytes"] = sum(r.get("size_bytes") or 0 for r in rf)
    for r in rf:
        if r.get("duty_cycle_wait_s"):
            waits["duty_cycle_wait_s"] += r["duty_cycle_wait_s"]
    n["duty_cycle_waits"] = dist([r.get("duty_cycle_wait_s") for r in rf if r.get("duty_cycle_wait_s")])
    fr = [r for r in recs if r.get("event") == "fragment_received"]
    n["frag_recv"] = len(fr)
    n["frag_recv_raw"] = sum(1 for r in fr if r.get("raw"))
    dsr = [r for r in recs if r.get("event") == "direct_send_result"]
    n["send_results"] = dict(collections.Counter("ok" if r.get("ok") else "fail" for r in dsr))
    n["send_methods"] = dict(collections.Counter(r.get("method") for r in dsr))
    n["text_fallbacks"] = sum(1 for r in dsr if r.get("fallback_from_raw"))
    slot = [r.get("slot_wait_s") for r in dsr if r.get("slot_wait_s")]
    waits["slot_wait_s"] = sum(slot)
    n["slot_waits"] = dist(slot)
    n["waits_s"] = {k: round(v, 1) for k, v in waits.items()}
    pk_out = [r for r in recs if "event" not in r and r.get("direction") == "out"]
    n["routing"] = dict(collections.Counter(r.get("routing_decision") for r in pk_out))
    n["out_bytes_rns"] = sum(r.get("size_bytes") or 0 for r in pk_out)
    n["out_types"] = dict(collections.Counter(r.get("packet_type_name") for r in pk_out))
    n["backoff_drops"] = sum(1 for r in pk_out if r.get("routing_decision") == "unknown_dest_backoff_drop")
    pk_in = [r for r in recs if "event" not in r and r.get("direction") == "in"]
    n["in_transport"] = dict(collections.Counter(r.get("transport") for r in pk_in))
    n["in_bytes_rns"] = sum(r.get("size_bytes") or 0 for r in pk_in)
    n["in_types"] = dict(collections.Counter(r.get("packet_type_name") for r in pk_in))
    n["reports_sent"] = sum(1 for r in recs if r.get("event") == "completion_report_sent")
    n["reports_sent_complete"] = sum(1 for r in recs if r.get("event") == "completion_report_sent" and r.get("complete"))
    n["queries_received"] = sum(1 for r in recs if r.get("event") == "completion_query_received")
    n["small_mesh_mode"] = dict(collections.Counter(r.get("small_mesh_mode") for r in pk_out))
    n["bound_peers_max"] = max((r.get("bound_peers") or 0 for r in pk_out), default=0)
    n["other_events"] = dict(collections.Counter(r.get("event") for r in recs if r.get("event") not in (
        "direct_attempt_result", "completion_check_result", "raw_fragment_sent", "fragment_received",
        "direct_send_result", "rx_log", "completion_report_sent", "completion_query_received", None)))
    # Link handshakes seen at this node, from the packet records: LINKREQUEST
    # out -> PROOF (context LRPROOF) in, matched in order (one handshake at a
    # time in every scenario here).
    lr_out = [r["ts"] for r in pk_out if r.get("packet_type_name") == "LINKREQUEST"]
    lrp_in = [r["ts"] for r in pk_in if r.get("packet_type_name") == "PROOF" and (r.get("context_name") or "") == "LRPROOF"]
    handshakes = []
    j = 0
    for t in lr_out:
        while j < len(lrp_in) and lrp_in[j] < t:
            j += 1
        if j < len(lrp_in):
            handshakes.append(round(lrp_in[j] - t, 2))
            j += 1
    n["linkrequest_out"] = len(lr_out)
    n["lrproof_in"] = len(lrp_in)
    n["handshake_s"] = dist(handshakes)
    return n


def burst_table(run_dir: str, sender: str = "A", receiver: str = "B") -> list:
    """Per raw pkt_id on the sender: round-0 fragments sent vs received at the
    receiver before round 1 began, reconcile rounds, first fragment -> the
    sender's completion check that knew it complete (s)."""
    caps = capture_files(run_dir)
    if sender not in caps or receiver not in caps:
        return []
    A = [r for p in caps[sender] for r in load_jsonl(p)]
    B = [r for p in caps[receiver] for r in load_jsonl(p)]
    rows = []
    sent = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in A:
        if r.get("event") == "raw_fragment_sent":
            sent[r["pkt_id"]][r["round"]].append(r["ts"])
    for pkt, rounds in sorted(sent.items()):
        r0 = rounds.get(0, [])
        if not r0:
            continue
        next_start = min((min(v) for k, v in rounds.items() if k > 0), default=1e18)
        got = sum(1 for r in B if r.get("event") == "fragment_received" and r.get("raw") and r.get("pkt_id") == pkt and r["ts"] < next_start)
        done = [r for r in A if r.get("event") == "completion_check_result" and r.get("pkt_id") == pkt and r.get("complete")]
        t_done = (min(r["ts"] for r in done) - min(r0)) if done else None
        rows.append({"pkt_id": pkt, "sent_round0": len(r0), "landed_round0": got, "rounds": len(rounds),
                     "complete_after_s": round(t_done, 1) if t_done is not None else None})
    return rows


# ---------------------------------------------------------------------------
# MeshBench engine events
# ---------------------------------------------------------------------------

def analyse_events(path: str) -> dict:
    """Transmissions per node, misses by (receiver, cause), and whether each
    half-duplex miss was one listen-before-talk would have prevented: the
    receiver's own transmission that keyed over the missed frame started
    AFTER that frame had begun arriving (so a real SX1262 firmware, which
    defers while isReceiving(), would have held it)."""
    events = load_jsonl(path)
    air = {}
    tx_by_packet = {}
    tx_by_node = collections.defaultdict(list)     # node -> [(start_ms, end_ms)]
    for e in events:
        if e.get("kind") != "tx":
            continue
        m = re.match(r"(\d+) bytes, (\d+) ms on air", e.get("detail", ""))
        nbytes, ms = (int(m.group(1)), int(m.group(2))) if m else (0, 0)
        a = air.setdefault(e.get("from"), {"tx": 0, "bytes": 0, "ms": 0})
        a["tx"] += 1
        a["bytes"] += nbytes
        a["ms"] += ms
        start = e.get("at_ms", 0)
        tx_by_packet[e.get("packet_id")] = (e.get("from"), start, start + ms, nbytes)
        tx_by_node[e.get("from")].append((start, start + ms))
    miss = collections.Counter()
    lbt = {"half_duplex": 0, "lbt_preventable": 0, "keyed_into_frame_ms": []}
    lbt_by_node = collections.Counter()
    direct = collections.Counter()
    for e in events:
        k = e.get("kind")
        if k not in ("rx", "miss"):
            continue
        if k == "rx":
            direct[f"{e.get('from')}->{e.get('to')}"] += 1
            continue
        cause = _miss_cause(e.get("detail", ""))
        miss[(e.get("to"), cause)] += 1
        if cause != "half-duplex":
            continue
        lbt["half_duplex"] += 1
        tx = tx_by_packet.get(e.get("packet_id"))
        if not tx:
            continue
        _, f_start, f_end, _ = tx
        # the receiver's own transmission overlapping [f_start, f_end)
        for s, t in tx_by_node.get(e.get("to"), ()):
            if s < f_end and t > f_start:
                if s > f_start:
                    lbt["lbt_preventable"] += 1
                    lbt_by_node[e.get("to")] += 1
                    lbt["keyed_into_frame_ms"].append(s - f_start)
                break
    total_tx_bytes = sum(a["bytes"] for a in air.values())
    total_ms = sum(a["ms"] for a in air.values())
    span_ms = (max((e.get("at_ms", 0) for e in events), default=0) - min((e.get("at_ms", 0) for e in events), default=0))
    return {"events": len(events), "air": air, "miss": miss, "receptions": direct, "lbt": lbt,
            "lbt_preventable_by_node": dict(lbt_by_node), "total_tx_bytes": total_tx_bytes, "total_tx_ms": total_ms,
            "span_s": span_ms / 1000.0, "channel_busy_fraction": (total_ms / span_ms) if span_ms else None}


# ---------------------------------------------------------------------------
# one run
# ---------------------------------------------------------------------------

def analyse(run_dir: str) -> dict:
    res = {"dir": run_dir, "name": os.path.basename(run_dir.rstrip("/"))}
    result = {}
    try:
        with open(os.path.join(run_dir, "result.json")) as f:
            result = json.load(f)
    except (OSError, json.JSONDecodeError):
        pass
    meas = result.get("measurements", {})
    m = re.match(r"^(.*?)(?:-s(\d+))?-\d+$", res["name"])
    res["scenario"] = result.get("scenario") or (m.group(1) if m else res["name"])
    res["seed"] = (result.get("args") or {}).get("seed")
    if res["seed"] is None and m and m.group(2):
        res["seed"] = int(m.group(2))
    res["exit_code"] = result.get("exit_code")
    res["informational"] = bool(result.get("informational"))
    log_path = os.path.join(run_dir, "run.log")
    log = ""
    if os.path.exists(log_path):
        try:
            log = open(log_path, errors="replace").read()
        except OSError:
            log = ""
    res["checks"] = [(ok.strip(), what) for ok, what in re.findall(r"^\d\d:\d\d:\d\d (ok    |FAIL  )(.*)$", log, re.M)]
    if "failures" in result:
        res["failures"] = result["failures"]
        res["passed"] = not result["failures"]
    else:
        # no result.json: the run stopped before its traffic phase (topology
        # gate, firmware, RNS bring-up) -- never a pass
        res["failures"] = [w for ok, w in res["checks"] if ok == "FAIL"] or ["no result.json (run aborted before the traffic phase)"]
        res["passed"] = False
    res["sent"] = meas.get("sent")
    res["delivered"] = meas.get("delivered")
    res["traffic"] = meas.get("traffic", "probe")
    rtts = meas.get("rtts") or []
    res["rtt"] = dist(rtts)
    res["rtt_str"] = f"min={min(rtts):.2f}s avg={sum(rtts)/len(rtts):.2f}s max={max(rtts):.2f}s" if rtts else "n/a"
    res["late"] = meas.get("late", 0)
    res["late_rtts"] = meas.get("late_rtts") or []
    res["rns_path_sender_s"] = meas.get("rns_path_time_s")
    res["time_to_direct_path"] = meas.get("time_to_direct_path") or {}
    res["gate"] = meas.get("start_gate")
    res["link_times_s"] = meas.get("link_times_s") or []
    res["links"] = dist(res["link_times_s"])
    res["links_within_deadline"] = meas.get("links_within_deadline")
    res["link_deadline_s"] = meas.get("link_deadline_s")
    def clean_resources(items):
        out = []
        for x in items or []:
            x = dict(x)
            if x.get("resent_parts") is not None and x["resent_parts"] < 0:
                x["resent_parts"] = None     # never got past the advertisement (sent_parts 0)
            out.append(x)
        return out
    res["resources"] = clean_resources(meas.get("resources"))
    res["resources_back"] = clean_resources(meas.get("resources_back"))
    res["health"] = meas.get("health") or []
    if not res["time_to_direct_path"] and log:
        # fallback: interface log lines `online` -> `path discovered to`
        online = {}
        for mm in re.finditer(r"^\[(\w+)\] \[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)\] \[\w+\]\s+SmartMeshCoreInterface\[\w+\]: (.*)$", log, re.M):
            node, ts, msg = mm.group(1), mm.group(2), mm.group(3)
            h, mi, s = (int(x) for x in ts.split(" ")[1].split(":"))
            t = h * 3600 + mi * 60 + s
            if msg.startswith("online") and node not in online:
                online[node] = t
            elif msg.startswith("path discovered to") and node in online and node not in res["time_to_direct_path"]:
                res["time_to_direct_path"][node] = t - online[node]
    res["nodes"] = {}
    for node, paths in capture_files(run_dir).items():
        recs = [r for p in paths for r in load_jsonl(p)]
        res["nodes"][node] = analyse_capture(recs)
    ev = analyse_events(os.path.join(run_dir, "meshbench_events.jsonl"))
    res["air"] = ev["air"]
    res["miss"] = ev["miss"]
    res["lbt"] = ev["lbt"]
    res["lbt_preventable_by_node"] = ev["lbt_preventable_by_node"]
    res["channel_busy_fraction"] = ev["channel_busy_fraction"]
    rns_in = sum(n["in_bytes_rns"] for n in res["nodes"].values())
    res["ledger"] = {
        "on_air_bytes": ev["total_tx_bytes"], "on_air_s": ev["total_tx_ms"] / 1000.0,
        "rns_bytes_accepted": rns_in,
        "on_air_bytes_per_rns_byte": (ev["total_tx_bytes"] / rns_in) if rns_in else None,
        "on_air_bytes_per_delivered_unit": (ev["total_tx_bytes"] / res["delivered"]) if res.get("delivered") else None,
    }
    sender = (result.get("args") or {}).get("sender") or "A"
    responder = (result.get("args") or {}).get("responder") or "B"
    res["sender"], res["responder"] = sender, responder
    res["bursts"] = burst_table(run_dir, sender, responder)
    return res


def compact(res: dict) -> dict:
    """The JSON-serialisable form written into result.json."""
    out = dict(res)
    out["miss"] = {f"{k[0]}|{k[1]}": v for k, v in res["miss"].items()}
    out["lbt"] = {**res["lbt"], "keyed_into_frame_ms": dist(res["lbt"]["keyed_into_frame_ms"])}
    nodes = {}
    for name, n in res["nodes"].items():
        nn = dict(n)
        nn["by_hop"] = {str(k): v for k, v in n["by_hop"].items()}
        nn["small_mesh_mode"] = {str(k): v for k, v in n["small_mesh_mode"].items()}
        nodes[name] = nn
    out["nodes"] = nodes
    return out


# ---------------------------------------------------------------------------
# presentation
# ---------------------------------------------------------------------------

def print_block(r: dict) -> None:
    print(f"=== {r['dir']} ===")
    print(f"verdict: {'PASS' if r['passed'] else 'FAIL'}{' (informational)' if r.get('informational') else ''}  "
          f"delivered {r.get('delivered')}/{r.get('sent')} late {r.get('late')}  RTT {dist_str(r['rtt'], 2)}  "
          f"RNS path (sender) {r.get('rns_path_sender_s')} s  traffic {r.get('traffic')}")
    for ok, what in r["checks"]:
        print(f"  {ok:4} {what}")
    if r["time_to_direct_path"]:
        print(f"  time to first DIRECT path per node (s): {r['time_to_direct_path']}" + (f"  gate: {r['gate']}" if r.get("gate") else ""))
    if r["link_times_s"]:
        within = (f"; within {r['link_deadline_s']:.0f} s: {r['links_within_deadline']}/{len(r['link_times_s'])}"
                  if r.get("link_deadline_s") is not None and r.get("links_within_deadline") is not None else "")
        print(f"  link handshakes: {dist_str(r['links'], 2)}{within}")
    for rs in r["resources"]:
        print(f"  resource {rs.get('tag')}: {'complete' if rs.get('complete') else 'FAILED/timeout'} in {rs.get('elapsed_s')} s, "
              f"{rs.get('total_parts')} parts, {rs.get('resent_parts')} re-sent")
    for rs in r["resources_back"]:
        print(f"  return resource {rs.get('tag')}: {'complete' if rs.get('complete') else 'FAILED/timeout'} in {rs.get('elapsed_s')} s, "
              f"{rs.get('total_parts')} parts, {rs.get('resent_parts')} re-sent")
    for node, n in r["nodes"].items():
        print(f"  node {node}: {n['records']} records; routing {n['routing']}; in {n['in_transport']}; "
              f"RNS bytes out {n['out_bytes_rns']} in {n['in_bytes_rns']}; backoff drops {n['backoff_drops']}; "
              f"small-mesh {n['small_mesh_mode']} (max bound {n['bound_peers_max']})")
        for h, b in n["by_hop"].items():
            print(f"    hop {h}: attempts {b['attempts']} ok {b['ok']} ({fmt(b['rate']*100 if b['rate'] is not None else None, 0)}%) "
                  f"ack med {fmt(b['ack_med'], 2)} p90 {fmt(b['ack_p90'], 2)} max {fmt(b['ack_max'], 2)} "
                  f"lock med {fmt(b['lock_med'], 2)} quiet_hold sum {fmt(b['quiet_hold_sum'], 1)} missed-ACK timeout max {fmt(b['missed_timeout_max'], 1)}")
        print(f"    attempt kinds {n['attempt_kinds']} failed {n['attempt_kinds_failed']} diagnosis {n['miss_diagnosis']}")
        print(f"    completion checks {n['completion_n']} {n['completion']}; reports sent {n['reports_sent']} "
              f"(complete {n['reports_sent_complete']}); queries received {n['queries_received']}")
        print(f"    raw fragments sent {n['raw_sent']} ({n['raw_bytes']} B) rounds {n['raw_rounds']}; fragments received {n['frag_recv']} (raw {n['frag_recv_raw']}); "
              f"duty-cycle waits {dist_str(n['duty_cycle_waits'])}")
        print(f"    direct sends {n['send_results']} methods {n['send_methods']} text fallbacks {n['text_fallbacks']}; slot waits {dist_str(n['slot_waits'])}")
        print(f"    waits (s, summed): {n['waits_s']}")
        if n["linkrequest_out"] or n["lrproof_in"]:
            print(f"    LINKREQUEST out {n['linkrequest_out']}, LRPROOF in {n['lrproof_in']}, handshake {dist_str(n['handshake_s'], 2)}")
        if n["other_events"]:
            print(f"    other events {n['other_events']}")
    print("  air (meshbench): " + "; ".join(f"{k}: {v['tx']} tx, {v['bytes']} B, {v['ms']/1000:.1f} s" for k, v in sorted(r["air"].items()))
          + (f"; channel busy {r['channel_busy_fraction']:.0%}" if r.get("channel_busy_fraction") is not None else ""))
    top = sorted(r["miss"].items(), key=lambda kv: -kv[1])[:8]
    print("  misses: " + "; ".join(f"{k[0]} {k[1]}: {v}" for k, v in top))
    lbt = r["lbt"]
    print(f"  half-duplex misses {lbt['half_duplex']}, of which LBT-preventable {lbt['lbt_preventable']} "
          f"(receiver keyed {dist_str(dist(lbt['keyed_into_frame_ms']), 0)} ms into the frame) by node {r['lbt_preventable_by_node']}")
    L = r["ledger"]
    print(f"  ledger: {L['on_air_bytes']} B on air ({L['on_air_s']:.0f} s) for {L['rns_bytes_accepted']} RNS B accepted = "
          f"{fmt(L['on_air_bytes_per_rns_byte'], 2)} B/B; {fmt(L['on_air_bytes_per_delivered_unit'], 0)} B per delivered unit")
    if r["bursts"]:
        print("  bursts (pkt: sent0/landed0, rounds, first-frag->complete s): " + "; ".join(
            f"{b['pkt_id']}:{b['sent_round0']}/{b['landed_round0']},{b['rounds']}r,{b['complete_after_s']}" for b in r["bursts"]))
    if r["health"]:
        first, last = r["health"][0], r["health"][-1]
        print(f"  health: RSS {first.get('rss_kb')} -> {last.get('rss_kb')} kB, threads {first.get('threads')} -> {last.get('threads')}, "
              f"sizes {last.get('sizes')} over {last.get('uptime_s')} s")


MD_HEADER = ("| run | verdict | probe RTT (RNS packet -> PROOF back) | RNS path s (sender) / time to DIRECT path per node | "
             "DIRECT attempts ok/total by hop (median ACK) | A completion checks answered-or-reported / total (rep = reported) | "
             "A raw fragments sent (by reconcile round) | MeshBench on-air per node (tx / bytes / s) | top miss reasons | "
             "half-duplex misses (LBT-preventable) | on-air B per RNS B |")
MD_SEP = "|---|---|---|---|---|---|---|---|---|---|---|"


def md_row(r: dict) -> str:
    def hop_str(n):
        parts = []
        for h, b in n.get("by_hop", {}).items():
            if h is None:
                continue
            parts.append(f"h{h}:{b['ok']}/{b['attempts']} ack{fmt(b['ack_med'], 1)}s")
        return " ".join(parts) or "-"
    A = r["nodes"].get(r.get("sender", "A"), {})
    B = r["nodes"].get(r.get("responder", "B"), {})
    cc = A.get("completion", {})
    ccs = f"{cc.get('answered', 0) + cc.get('reported', 0) + cc.get('reported_stale', 0)}/{A.get('completion_n', 0)} (rep {cc.get('reported', 0)})"
    air = "; ".join(f"{k}:{v['tx']}tx/{v['bytes']}B/{v['ms']/1000:.0f}s" for k, v in sorted(r["air"].items()))
    miss = "; ".join(f"{k[0]}-{k[1]}:{v}" for k, v in sorted(r["miss"].items(), key=lambda kv: -kv[1])[:4])
    verdict = f"{'PASS' if r['passed'] else 'FAIL'} {r.get('delivered')}/{r.get('sent')}" + (f" (+{r['late']} late)" if r.get("late") else "")
    return (f"| {r['name']} | {verdict} | {r.get('rtt_str')} | {r.get('rns_path_sender_s')} / {r['time_to_direct_path']} | "
            f"A {hop_str(A)}; B {hop_str(B)} | {ccs} | {A.get('raw_sent', 0)} ({A.get('raw_rounds', {})}) | {air} | {miss} | "
            f"{r['lbt']['half_duplex']} ({r['lbt']['lbt_preventable']}) | {fmt(r['ledger']['on_air_bytes_per_rns_byte'], 2)} |")


def aggregate(results: list) -> dict:
    """Medians and ranges per scenario over several runs (seeds): the shape a
    baseline file should quote, since one MeshBench run is a coin flip on
    bring-up and the RNS side is wall-clock driven."""
    by_scn = collections.defaultdict(list)
    for r in results:
        by_scn[r["scenario"]].append(r)
    out = {}
    for scn, rs in sorted(by_scn.items()):
        def series(fn):
            vals = []
            for r in rs:
                try:
                    v = fn(r)
                except (KeyError, TypeError, ZeroDivisionError):
                    v = None
                if v is not None:
                    vals.append(v)
            return dist(vals)
        sender = rs[0].get("sender", "A")
        responder = rs[0].get("responder", "B")

        def hop_rate(node, hop):
            def f(r):
                b = r["nodes"][node]["by_hop"].get(hop)
                return b["rate"] if b and b["attempts"] else None
            return f

        def hop_ack(node, hop):
            def f(r):
                b = r["nodes"][node]["by_hop"].get(hop)
                return b["ack_med"] if b else None
            return f
        hops = sorted({h for r in rs for n in r["nodes"].values() for h in n["by_hop"] if h is not None})
        out[scn] = {
            "runs": len(rs), "passed": sum(1 for r in rs if r["passed"]), "seeds": sorted({r.get("seed") for r in rs}, key=lambda x: (x is None, x)),
            "delivered_fraction": series(lambda r: r["delivered"] / r["sent"] if r["sent"] else None),
            "late": series(lambda r: r["late"]),
            "rtt_med_s": series(lambda r: r["rtt"]["med"]),
            "rtt_p90_s": series(lambda r: r["rtt"]["p90"]),
            "rns_path_sender_s": series(lambda r: r["rns_path_sender_s"]),
            "time_to_direct_path_s": {node: series(lambda r, node=node: r["time_to_direct_path"].get(node))
                                      for node in sorted({k for r in rs for k in r["time_to_direct_path"]})},
            "attempt_rate_by_hop": {f"{node} h{h}": series(hop_rate(node, h)) for node in (sender, responder) for h in hops},
            "ack_med_by_hop_s": {f"{node} h{h}": series(hop_ack(node, h)) for node in (sender, responder) for h in hops},
            "completion_reported_fraction": series(lambda r: (r["nodes"][sender]["completion"].get("reported", 0) + r["nodes"][sender]["completion"].get("reported_stale", 0))
                                                   / r["nodes"][sender]["completion_n"] if r["nodes"][sender]["completion_n"] else None),
            "completion_timeouts": series(lambda r: r["nodes"][sender]["completion"].get("timeout", 0)),
            "raw_sent": series(lambda r: r["nodes"][sender]["raw_sent"]),
            "tx_per_node": {node: series(lambda r, node=node: r["air"][node]["tx"]) for node in sorted({k for r in rs for k in r["air"]})},
            "half_duplex_misses": series(lambda r: r["lbt"]["half_duplex"]),
            "lbt_preventable": series(lambda r: r["lbt"]["lbt_preventable"]),
            "on_air_bytes_per_rns_byte": series(lambda r: r["ledger"]["on_air_bytes_per_rns_byte"]),
            "link_handshake_med_s": series(lambda r: r["links"]["med"]),
            "links_within_deadline_fraction": series(lambda r: r["links_within_deadline"] / len(r["link_times_s"]) if r["link_times_s"] else None),
            "resource_complete_fraction": series(lambda r: sum(1 for x in r["resources"] if x.get("complete")) / len(r["resources"]) if r["resources"] else None),
            "resource_elapsed_med_s": series(lambda r: med([x["elapsed_s"] for x in r["resources"] if x.get("complete")])),
            "resource_resent_parts": series(lambda r: sum(x.get("resent_parts") or 0 for x in r["resources"]) if r["resources"] else None),
            "burst_landed_round0_fraction": series(lambda r: sum(b["landed_round0"] for b in r["bursts"]) / sum(b["sent_round0"] for b in r["bursts"]) if r["bursts"] else None),
            "burst_complete_after_med_s": series(lambda r: med([b["complete_after_s"] for b in r["bursts"] if b["complete_after_s"] is not None])),
        }
    return out


def aggregate_md(agg: dict) -> str:
    lines = ["| scenario | runs (pass) | seeds | delivered | late | RTT med s | RNS path s | DIRECT attempt rate by hop | ACK med by hop s | "
             "reported fraction | half-duplex misses (LBT-prev.) | on-air B/RNS B | links med s (≤deadline) | resources complete / med s / re-sent |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]

    def rng(d, nd=2, pctg=False):
        if not d or not d.get("n"):
            return "-"
        f = (lambda v: f"{v*100:.0f}%") if pctg else (lambda v: fmt(v, nd))
        return f"{f(d['med'])} [{f(d['min'])}–{f(d['max'])}]" if d["n"] > 1 else f(d["med"])
    for scn, a in agg.items():
        lines.append(
            f"| {scn} | {a['runs']} ({a['passed']}) | {','.join(str(s) for s in a['seeds'])} | {rng(a['delivered_fraction'], pctg=True)} | "
            f"{rng(a['late'], 0)} | {rng(a['rtt_med_s'], 1)} | {rng(a['rns_path_sender_s'], 0)} | "
            + "; ".join(f"{k} {rng(v, pctg=True)}" for k, v in a["attempt_rate_by_hop"].items() if v.get("n")) + " | "
            + "; ".join(f"{k} {rng(v, 1)}" for k, v in a["ack_med_by_hop_s"].items() if v.get("n")) + " | "
            f"{rng(a['completion_reported_fraction'], pctg=True)} | {rng(a['half_duplex_misses'], 0)} ({rng(a['lbt_preventable'], 0)}) | "
            f"{rng(a['on_air_bytes_per_rns_byte'], 2)} | {rng(a['link_handshake_med_s'], 1)} ({rng(a['links_within_deadline_fraction'], pctg=True)}) | "
            f"{rng(a['resource_complete_fraction'], pctg=True)} / {rng(a['resource_elapsed_med_s'], 0)} / {rng(a['resource_resent_parts'], 0)} |")
    return "\n".join(lines)


def timeline(run_dir: str, lo: float = 0.0, hi: float = 1e12) -> None:
    """Merged, compact event timeline of every node's interface capture."""
    rows = []
    for node, paths in capture_files(run_dir).items():
        for p in paths:
            for r in load_jsonl(p):
                rows.append((r["ts"], node, r))
    rows.sort(key=lambda x: x[0])
    t0 = rows[0][0] if rows else 0
    for ts, node, r in rows:
        t = ts - t0
        if t < lo or t > hi:
            continue
        e = r.get("event")
        if e == "rx_log":
            continue
        if e is None:
            s = (f"PKT {r['direction']} {r.get('packet_type_name')} ctx={r.get('context_name')} size={r.get('size_bytes')} "
                 f"rd={r.get('routing_decision') or r.get('transport')} hop={r.get('hop_count')} pkt_id={r.get('pkt_id')} "
                 f"ft={r.get('frag_total')} ph={r.get('payload_hash')}")
        elif e == "direct_attempt_result":
            s = (f"  ATT kind={r.get('kind')} ok={r['ok']} att={r['attempt']} frag={r.get('frag_idx')}/{r.get('frag_total')} pkt={r.get('pkt_id')} "
                 f"hop={r.get('hop_count')} ack={r.get('ack_latency_s')} to={r.get('ack_timeout_s')} src={r.get('ack_timeout_source')} "
                 f"lock={r.get('lock_wait_s')} listen={r.get('listen_delay_s')} qh={r.get('quiet_hold_s')} diag={r.get('miss_diagnosis')} "
                 f"echo={r.get('rx_echo_seen_s')} qd={r.get('queue_depth')}")
        elif e == "completion_check_result":
            s = f"  CC pkt={r['pkt_id']} {r['outcome']} complete={r.get('complete')} held={r.get('held')} stage={r.get('stage')} to={r.get('timeout_s')}"
        elif e == "raw_fragment_sent":
            s = (f"  RAW pkt={r['pkt_id']} frag={r['frag_idx']}/{r['frag_total']} round={r['round']} ok={r['ok']} size={r['size_bytes']} "
                 f"hop={r.get('hop_count')} dc={r.get('duty_cycle_wait_s')}")
        elif e == "direct_send_result":
            s = f"  RES ok={r['ok']} method={r.get('method')} size={r['size_bytes']} path={r.get('out_path_len')} slot={r.get('slot_wait_s')} fb={r.get('fallback_from_raw')}"
        elif e == "fragment_received":
            s = f"  FRAG_RX pkt={r['pkt_id']} frag={r['frag_idx']}/{r['frag_total']} progress={r['progress']} raw={r['raw']} mode={r['mode']}"
        else:
            s = f"  {e} " + json.dumps({k: v for k, v in r.items() if k not in ("ts", "ts_monotonic", "seq", "event", "direction")})[:160]
        print(f"{t:7.1f} {node} {s}")


# ---------------------------------------------------------------------------

def main(argv: list) -> None:
    flags = {a for a in argv if a.startswith("--")}
    rest = [a for a in argv if not a.startswith("--")]
    if "--timeline" in flags:
        if not rest:
            sys.exit("usage: meshbench_report.py --timeline <run-dir> [start_s end_s]")
        lo = float(rest[1]) if len(rest) > 1 else 0.0
        hi = float(rest[2]) if len(rest) > 2 else 1e12
        timeline(rest[0], lo, hi)
        return
    dirs = [d for d in rest if os.path.isdir(d)]
    if not dirs:
        sys.exit(__doc__)
    if "--bursts" in flags:
        for d in dirs:
            rows = burst_table(d)
            print(os.path.basename(d.rstrip("/")), "pkt: sent0/landed0, rounds, first-frag->known-complete s:",
                  "; ".join(f"{b['pkt_id']}:{b['sent_round0']}/{b['landed_round0']},{b['rounds']}r,{b['complete_after_s']}" for b in rows))
        return
    results = [analyse(d) for d in dirs]
    if "--md" in flags:
        print(MD_HEADER)
        print(MD_SEP)
        for r in results:
            print(md_row(r))
    if "--aggregate" in flags:
        agg = aggregate(results)
        print(aggregate_md(agg))
        if "--json" in flags:
            print(json.dumps(agg, indent=1, default=str))
    if not (flags & {"--md", "--aggregate"}):
        for r in results:
            print_block(r)
            print()


if __name__ == "__main__":
    main(sys.argv[1:])
