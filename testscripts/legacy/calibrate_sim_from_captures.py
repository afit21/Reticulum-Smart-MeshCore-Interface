#!/usr/bin/env python3
"""
LEGACY (archived 2026-09-20, see CLAUDE.md "Legacy simulation tooling"): this
produced --profile values for the simmesh-based fake_meshcore_repeater_sim.py,
now superseded by testscripts/meshbench_scenarios.py (real firmware, modelled
RF). Kept for reading old results.

calibrate_sim_from_captures.py

Reads the interface's own JSONL packet captures from real field tests
(fieldtests/raw/<session>/*.jsonl, or any directory of them) and
summarizes what the simulator needs to imitate them: per-hop-count
DIRECT attempt success rate and ACK latency distribution, completion-
check outcomes, and the raw-RX-log echo timing. Prints a suggested set
of flags for testscripts/fake_meshcore_repeater_sim.py and writes them
as a JSON profile that script accepts via --profile.

These are starting points derived from small samples, not measurements
of the radio: the point is to run the interface's logic against loss
and latency in the same ballpark the field saw, so a change can be
compared A/B before it goes anywhere near a real repeater.

    python3 calibrate_sim_from_captures.py fieldtests/raw/2026-09-16-evening
    python3 calibrate_sim_from_captures.py fieldtests/raw --profile-out /tmp/field.json
"""
import argparse
import collections
import json
import os
import statistics
import sys


def load_records(paths):
    records = []
    for path in paths:
        if os.path.isdir(path):
            for root, _dirs, files in os.walk(path):
                for fn in sorted(files):
                    if fn.endswith(".jsonl"):
                        records.extend(_load_file(os.path.join(root, fn)))
        elif path.endswith(".jsonl"):
            records.extend(_load_file(path))
    return records


def _load_file(path):
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            r["_file"] = os.path.basename(path)
            out.append(r)
    return out


def pct(values, p):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def fmt(v, unit="s"):
    return "n/a" if v is None else f"{v:.2f}{unit}"


def analyze(records):
    attempts = [r for r in records if r.get("event") == "direct_attempt_result"]
    by_hop = collections.defaultdict(lambda: {"ok": 0, "fail": 0, "latency": [], "lock_wait": [], "timeouts": []})
    for r in attempts:
        hop = r.get("hop_count")
        b = by_hop[hop]
        b["ok" if r.get("ok") else "fail"] += 1
        if r.get("ack_latency_s") is not None:
            b["latency"].append(float(r["ack_latency_s"]))
        if r.get("lock_wait_s") is not None:
            b["lock_wait"].append(float(r["lock_wait_s"]))
        if r.get("ack_timeout_s") is not None:
            b["timeouts"].append(float(r["ack_timeout_s"]))

    completion = collections.Counter(r.get("outcome") for r in records if r.get("event") == "completion_check_result")
    send_results = [r for r in records if r.get("event") == "direct_send_result"]
    send_ok = sum(1 for r in send_results if r.get("ok"))

    rx = [r for r in records if r.get("event") == "rx_log"]
    echo_gaps = [r["since_own_tx_s"] for r in rx if r.get("since_own_tx_s") is not None and 0 < r["since_own_tx_s"] < 3.0]
    routing = collections.Counter(r.get("routing_decision") for r in records if r.get("direction") == "out" and r.get("routing_decision"))
    transports = collections.Counter(r.get("transport") for r in records if r.get("direction") == "in" and r.get("transport"))

    return {
        "n_records": len(records), "attempts": len(attempts), "by_hop": dict(by_hop),
        "completion": dict(completion), "send_results": (send_ok, len(send_results)),
        "echo_gaps": echo_gaps, "routing": dict(routing), "transports": dict(transports),
    }


def suggest(analysis):
    """Turn attempt success per hop into a per-delivery loss for the sim's
    --loss (an attempt is a round trip: hops+1 transmissions each way)."""
    per_hop_loss = {}
    for hop, b in analysis["by_hop"].items():
        n = b["ok"] + b["fail"]
        if n == 0 or hop is None:
            continue
        success = b["ok"] / n
        transmissions = 2 * (int(hop) + 1)
        per_hop_loss[hop] = 1.0 - success ** (1.0 / transmissions) if success > 0 else 0.5
    loss = statistics.median(per_hop_loss.values()) if per_hop_loss else 0.0

    # Zero-hop ACK latency ~ 2 x airtime + firmware turnaround; the sim's
    # linear model only has base + per-byte, so put the whole thing in base.
    zero = analysis["by_hop"].get(0, {}).get("latency") or []
    airtime_base_ms = 50.0
    if zero:
        airtime_base_ms = max(20.0, (statistics.median(zero) * 1000.0) / 2.0 - 120.0)
    echo = analysis["echo_gaps"]
    return {
        "loss": round(min(0.6, max(0.0, loss)), 3),
        "airtime_base_ms": round(airtime_base_ms, 1),
        "airtime_per_byte_ms": 1.0,
        "per_hop_loss_estimate": {str(k): round(v, 3) for k, v in per_hop_loss.items()},
        "repeater_echo_gap_s_median": round(statistics.median(echo), 3) if echo else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("paths", nargs="+", help="Capture .jsonl files or directories of them")
    parser.add_argument("--profile-out", default=None, help="Write the suggested sim profile JSON here")
    args = parser.parse_args()

    records = load_records(args.paths)
    if not records:
        sys.exit("no capture records found")
    a = analyze(records)

    print(f"{a['n_records']} records, {a['attempts']} DIRECT attempt result(s), "
          f"{a['send_results'][0]}/{a['send_results'][1]} DIRECT sends succeeded")
    print(f"routing decisions: {a['routing']}")
    print(f"incoming transports: {a['transports']}")
    print(f"completion checks: {a['completion']}")
    print("\nDIRECT attempts by hop count:")
    for hop in sorted(a["by_hop"], key=lambda h: (h is None, h)):
        b = a["by_hop"][hop]
        n = b["ok"] + b["fail"]
        lat = b["latency"]
        print(f"  hop={hop!s:>4}: {b['ok']}/{n} ok ({100.0 * b['ok'] / n:.0f}%)  "
              f"ack_latency med={fmt(pct(lat, 0.5))} p90={fmt(pct(lat, 0.9))} max={fmt(max(lat) if lat else None)}  "
              f"lock_wait med={fmt(pct(b['lock_wait'], 0.5))}  ack_timeout med={fmt(pct(b['timeouts'], 0.5))}")
    if a["echo_gaps"]:
        print(f"\nrepeater echo after own tx (rx_log since_own_tx_s < 3s): n={len(a['echo_gaps'])} "
              f"med={fmt(pct(a['echo_gaps'], 0.5))} p90={fmt(pct(a['echo_gaps'], 0.9))}")

    s = suggest(a)
    print("\nSuggested simulator flags (starting points, not measurements):")
    print(f"  --loss {s['loss']} --airtime-base-ms {s['airtime_base_ms']} --airtime-per-byte-ms {s['airtime_per_byte_ms']}")
    print(f"  per-hop loss estimates: {s['per_hop_loss_estimate']}")
    if args.profile_out:
        with open(args.profile_out, "w") as f:
            json.dump(s, f, indent=2)
        print(f"profile written to {args.profile_out} (use with fake_meshcore_repeater_sim.py --profile)")


if __name__ == "__main__":
    main()
