#!/usr/bin/env python3
"""
LEGACY (archived 2026-09-20, see CLAUDE.md "Legacy simulation tooling"): the
simmesh-based fidelity tier this script drives has been superseded by
testscripts/meshbench_scenarios.py, which runs the same interface against
real MeshCore firmware under MeshBench. Kept runnable for reading old
results; do not add scenarios here.

fake_meshcore_repeater_sim.py

Runs real Interface/SmartMeshCoreInterface.py instances against an
entirely in-process simulated MeshCore mesh (testscripts/simmesh/) -- no
radio hardware, a topology you define exactly, and a --seed for
reproducible A/B runs. Built for the cases two adjacent bench radios
can't reproduce: DIRECT delivery through one, two, or three repeater
hops, asymmetric loss (data arrives, ACKs don't), and the interface's
own retry/completion-check/stale-path logic under those conditions.

Unlike testscripts/relay_delivery_test.py and rns_reliability_probe.py
(real Reticulum over real radios) this constructs the interfaces directly
with a recording stand-in for RNS.Transport, the same isolation
zero_hop_peer_discovery_test.py uses -- but feeds them *real packed
RNS.Packet bytes* of each type the routing dispatcher distinguishes, so
every branch (DIRECT-primary, bootstrap supplement, path request,
ANNOUNCE, small-mesh DIRECT-all) is exercised, not just the CHANNEL
fallback. For the full stack (real RNS Transport, Links, LXMF) with the
same simulated mesh, see testscripts/rns_multiprocess_sim.py.

WHAT IS AND ISN'T SIMULATED: see testscripts/simmesh/__init__.py and
the docstrings of simmesh/air.py and simmesh/radio.py. Timing numbers
out of this tool are not real-world predictions; use it to check this
interface's *logic* -- does reassembly complete, does the completion
check recover a phantom ACK loss, does a stale path get reset and
rediscovered -- the same way the field tests separate "RF problem" from
"interface-layer problem".

USAGE

    # Zero-hop pair (small-mesh mode, DIRECT for everything):
    python3 fake_meshcore_repeater_sim.py --link A-B --send-from A --send-to B

    # One repeater hop, five DATA packets, reproducible:
    python3 fake_meshcore_repeater_sim.py --link A-R --link R-B --repeater R \\
        --send-from A --send-to B --count 5 --seed 7

    # Two hops with a lossy return path (ACKs get lost, data doesn't):
    python3 fake_meshcore_repeater_sim.py --link A-R1 --link R1-R2 --link R2-B \\
        --repeater R1 --repeater R2 --send-from A --send-to B \\
        --link-loss 'R1>A=0.6' --count 3 --payload-size 300 --seed 3

    # A first hop that dies 20 s after the first send and recovers at 110 s
    # (the 2026-09-18 drive-home outage shape) -- quote anything with '>'
    # or the shell treats it as a redirection:
    python3 fake_meshcore_repeater_sim.py --link A-R --link R-B --repeater R \\
        --send-from A --send-to B --count 8 --interval 15 \\
        --loss-at '20:A>R=1.0' --loss-at '110:A>R=0.0' --seed 12

    # A path request / announce instead of DATA, with capture output:
    python3 fake_meshcore_repeater_sim.py --link A-R --link R-B --repeater R \\
        --send-from A --send-to B --packet-type path_request \\
        --packet-capture-dir /tmp/simcap

Timing: by default the interfaces run with the FAST_TIMING profile
(seconds, not minutes -- see simmesh/harness.py). Pass --production-timing
to run against the interface's real defaults.
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))  # testscripts/, for simmesh

from simmesh import SimMesh, build_rns_packet, wait_until, summarize_capture  # noqa: E402
from simmesh.air import parse_loss_schedule  # noqa: E402
from simmesh.harness import format_summary  # noqa: E402
from simmesh.radio import RadioOptions  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--link", action="append", required=True, metavar="A-B", help="Direct-hearing edge, repeatable.")
    parser.add_argument("--repeater", action="append", default=[], metavar="NAME", help="Relay-only node (no interface). Repeatable.")
    parser.add_argument("--send-from", required=True, metavar="NAME")
    parser.add_argument("--send-to", required=True, metavar="NAME")
    parser.add_argument("--packet-type", default="data", choices=["data", "announce", "path_request", "link_request"],
                        help="RNS packet type to send (default: data -> DATA/SINGLE, routed DIRECT-primary once a token is known)")
    parser.add_argument("--payload-size", type=int, default=64, help="RNS payload bytes (default 64; >~110 fragments on DIRECT)")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between sends")
    parser.add_argument("--loss", type=float, default=0.0, help="Per-delivery loss probability, every link (default 0)")
    parser.add_argument("--link-loss", action="append", default=[], metavar="FROM>TO=P",
                        help="Directional loss override, e.g. 'R>A=0.5' makes the return path lossy (quote it: '>' is a shell "
                             "redirection). Repeatable.")
    parser.add_argument("--type-loss", action="append", default=[], metavar="TYPENAME=P",
                        help="Loss by MeshCore payload type, e.g. ACK=1.0 (data arrives, ACKs never do -- the phantom-ACK case). Repeatable.")
    parser.add_argument("--loss-at", action="append", default=[], metavar="SECONDS:FROM>TO=P",
                        help="Change one direction's loss at a time offset from the first send, e.g. '60:A>R1=1.0' then "
                             "'240:A>R1=0.0' replays a first hop that dies and comes back (quote it: '>' is a shell "
                             "redirection). Repeatable.")
    parser.add_argument("--iface-option", action="append", default=[], metavar="KEY=VALUE",
                        help="Interface config override applied to every node (after the timing profile), e.g. "
                             "peer_discovery_rerequest_interval=20 or reassembly_idle_timeout=200. Repeatable.")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--airtime-base-ms", type=float, default=None)
    parser.add_argument("--airtime-per-byte-ms", type=float, default=None)
    parser.add_argument("--profile", default=None,
                        help="JSON from calibrate_sim_from_captures.py; supplies loss/airtime defaults derived from real field captures")
    parser.add_argument("--production-timing", action="store_true", help="Use the interface's real timing defaults instead of FAST_TIMING")
    parser.add_argument("--no-auto-advert", action="store_true", help="Radios don't advert at startup (contacts never populate unless something else does)")
    parser.add_argument("--no-telemetry-gating", action="store_true", help="Answer path discovery without the telemetry permission bit")
    parser.add_argument("--no-prime", action="store_true",
                        help="Don't have the receiver send one packet first. Without priming, the sender has no RNS token for the "
                             "destination, so DATA goes broadcast+bootstrap-supplement (or small-mesh DIRECT-all) instead of DIRECT-primary.")
    parser.add_argument("--bind-timeout", type=float, default=20.0, help="Seconds to wait for bind-frame discovery before sending")
    parser.add_argument("--wait", type=float, default=20.0, help="Seconds to wait after the last send before reporting")
    parser.add_argument("--packet-capture-dir", default=None, help="Enable the interfaces' own JSONL packet capture into this dir and print a summary")
    parser.add_argument("--debug", action="store_true", help="Interface debug logging")
    parser.add_argument("--quiet-air", action="store_true", help="Suppress per-packet [AIR] lines")
    args = parser.parse_args()

    def log(msg: str) -> None:
        if args.quiet_air and msg.startswith("[AIR]"):
            return
        print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)

    profile = {}
    if args.profile:
        with open(args.profile) as f:
            profile = json.load(f)
    loss = args.loss if args.loss else float(profile.get("loss", 0.0))
    airtime_base_ms = args.airtime_base_ms if args.airtime_base_ms is not None else float(profile.get("airtime_base_ms", 50.0))
    airtime_per_byte_ms = args.airtime_per_byte_ms if args.airtime_per_byte_ms is not None else float(profile.get("airtime_per_byte_ms", 1.0))

    repeaters = set(args.repeater)
    radio_options = RadioOptions(
        auto_advert=not args.no_auto_advert,
        require_telemetry_permission=not args.no_telemetry_gating,
    )
    mesh = SimMesh(
        args.link, repeaters=repeaters, seed=args.seed, loss=loss, link_loss=args.link_loss, type_loss=args.type_loss,
        airtime_base_ms=airtime_base_ms, airtime_per_byte_ms=airtime_per_byte_ms,
        log=log, radio_options=radio_options, fast=not args.production_timing,
        capture_dir=args.packet_capture_dir, debug=args.debug,
    )
    end_nodes = [n for n in mesh.air.nodes() if n not in repeaters]
    for name in (args.send_from, args.send_to):
        if name not in end_nodes:
            parser.error(f"{name!r} must be a non-repeater node in the --link topology")
    iface_overrides = dict(kv.split("=", 1) for kv in args.iface_option)
    loss_schedule = parse_loss_schedule(args.loss_at)

    log(f"Topology: {{ {', '.join(f'{k}: {sorted(v)}' for k, v in mesh.air.adjacency.items())} }} "
        f"repeaters={sorted(repeaters)} seed={args.seed} loss={loss} link_loss={mesh.air.link_loss} type_loss={mesh.air.type_loss} "
        f"airtime={airtime_base_ms}ms+{airtime_per_byte_ms}ms/B{' (profile: ' + args.profile + ')' if args.profile else ''}")

    try:
        for name in end_nodes:
            mesh.add_node(name, config=iface_overrides)
        sender, receiver = mesh.nodes[args.send_from], mesh.nodes[args.send_to]

        if not args.no_auto_advert:
            log("Every radio adverts, repeated until every node holds every other as a contact (the 'press advert "
                "on each radio until it shows up' field-setup step)...")
            if not mesh.advert_until_contacts(rounds=6, timeout=max(10.0, args.bind_timeout / 3)):
                log("WARNING: not every node has every other as a contact -- continuing anyway.")
        log(f"Waiting up to {args.bind_timeout:.0f}s for bind-frame peer discovery...")
        if not mesh.wait_bound(args.bind_timeout):
            log("WARNING: not every node bound every other node -- continuing anyway.")
        for node in mesh.nodes.values():
            log(f"  {node.name}: peers={node.peers} contacts={sorted(c['adv_name'] for c in node.radio.contacts.values())}")

        if args.packet_type == "data" and not args.no_prime:
            # One packet from the receiver teaches the sender an RNS token
            # (destination -> peer) via the DIRECT receive path, exactly the
            # way real traffic bootstraps DIRECT-primary routing.
            log(f"Priming: {receiver.name} sends one DATA packet so {sender.name} learns a token for {receiver.dest_hash.hex()}...")
            receiver.send(build_rns_packet("data", dest_hash=receiver.dest_hash, payload=b"prime"))
            primed = wait_until(lambda: receiver.dest_hash in sender.iface._rns_token_peer, timeout=30.0)
            log(f"Priming {'succeeded' if primed else 'did NOT complete (sender will route without a token)'}.")

        for at_s, frm, to, prob in loss_schedule:
            mesh.air.schedule_link_loss(at_s, frm, to, prob)
        log(f"Sending {args.count} x {args.packet_type} ({args.payload_size}B) from {sender.name} to {receiver.name}..."
            + (f" loss schedule: {loss_schedule}" if loss_schedule else ""))
        marker_base = len(receiver.owner.received)
        for i in range(args.count):
            marker = f"sim-probe-{i}-".encode()
            payload = marker + os.urandom(max(0, args.payload_size - len(marker)))
            sender.send(build_rns_packet(args.packet_type, dest_hash=receiver.dest_hash, payload=payload))
            if i < args.count - 1:
                time.sleep(args.interval)

        deadline = time.time() + args.wait
        while time.time() < deadline:
            if len(receiver.owner.received) - marker_base >= args.count and args.wait > 5:
                time.sleep(2.0)
                break
            time.sleep(0.25)

        delivered = sum(1 for d in receiver.owner.received[marker_base:] if d.find(b"sim-probe-") >= 0)
        log("")
        log(f"=== Result: {delivered}/{args.count} probe payload(s) delivered to {receiver.name} ===")
        log(f"air: {mesh.air.stats.summary()}")
        for node in mesh.nodes.values():
            iface = node.iface
            log(f"  {node.name}: peers={node.peers} resolved_paths={ {k: v.out_path_len for k, v in node.resolved_paths.items()} } "
                f"direct_failures={dict(iface._direct_path_failures)} out_dropped={iface._outgoing_dropped_total} "
                f"in_dropped={iface._incoming_dropped_total} reassembly_open={len(iface._reassembly)} radio={dict(node.radio.counters)}")
        for name, radio in mesh.repeaters.items():
            log(f"  {name} (repeater): {dict(radio.counters)}")
        if args.packet_capture_dir:
            for node in mesh.nodes.values():
                log(f"--- capture summary for {node.name} ---")
                for line in format_summary(summarize_capture(node.capture_records())).splitlines():
                    log(f"  {line}")
        sys.exit(0 if delivered == args.count else 2)
    finally:
        mesh.stop()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
