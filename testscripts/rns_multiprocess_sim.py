#!/usr/bin/env python3
"""
rns_multiprocess_sim.py

The full stack -- a real RNS.Reticulum per node, loading
Interface/SmartMeshCoreInterface.py exactly the way rnsd does (from a
config directory's interfaces/ folder), real RNS Transport, announces,
path requests, packets and delivery proofs -- over the simulated
MeshCore mesh from testscripts/simmesh/, with no radio hardware.

This is testscripts/rns_reliability_probe.py's responder/sender pair
(same probe payloads, same receipt-based delivered/lost/RTT accounting)
run against a topology you define, so the RNS-semantics class of field
issue (Link keepalive/RTT calibration, PATH_RESPONSE storms, announce
handling) can be reproduced on one machine. RNS.Reticulum is a
per-process singleton, so every node is its own subprocess; the air
(physics + repeaters) lives in the orchestrator and end nodes reach it
over a local TCP socket (simmesh/remote.py).

USAGE

    # Two RNS nodes through one repeater, 5 probes:
    python3 rns_multiprocess_sim.py run --link A-R --link R-B --repeater R \\
        --responder B --sender A --probes 5 --wait 3 --seed 1

    # Same, lossy, with the interfaces' packet capture written per node:
    python3 rns_multiprocess_sim.py run --link A-R1 --link R1-R2 --link R2-B \\
        --repeater R1 --repeater R2 --responder B --sender A \\
        --probes 10 --wait 5 --loss 0.1 --seed 4 --capture-dir /tmp/mpcap

Each node subprocess writes its RNS log to stdout prefixed with its
name; lines starting with '@@' are status JSON the orchestrator parses.
Exit code 0 iff every probe was delivered.
"""
import argparse
import json
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from simmesh.air import Air, parse_link_loss, parse_links, parse_type_loss  # noqa: E402
from simmesh.harness import FAST_TIMING, INTERFACE_PATH  # noqa: E402
from simmesh.radio import RadioOptions, SimRadio  # noqa: E402
from simmesh.remote import AirServer, RemoteAir  # noqa: E402

APP_NAME = "smci_mp_sim"
ASPECT = "probe"
PAYLOAD_HEADER = struct.Struct(">Id")


def status(event: str, **fields) -> None:
    print("@@" + json.dumps({"event": event, **fields}), flush=True)


# ---------------------------------------------------------------------------
# node subprocess
# ---------------------------------------------------------------------------

def write_node_config(configdir: str, name: str, server_host: str, server_port: int, fast: bool,
                      capture_dir: str, transport_node: bool, loglevel: int, extra: dict) -> None:
    os.makedirs(os.path.join(configdir, "interfaces"), exist_ok=True)
    shutil.copy(INTERFACE_PATH, os.path.join(configdir, "interfaces", "SmartMeshCoreInterface.py"))
    lines = [
        "[reticulum]",
        "  share_instance = No",
        f"  enable_transport = {'Yes' if transport_node else 'No'}",
        "  panic_on_interface_error = No",
        "",
        "[logging]",
        f"  loglevel = {loglevel}",
        "",
        "[interfaces]",
        f"  [[{name}]]",
        "    type = SmartMeshCoreInterface",
        "    interface_enabled = yes",
        "    transport = tcp",
        f"    host = {server_host}",
        f"    tcp_port = {server_port}",
        f"    name = {name}",
        "    stats_interval = 3600",
        f"    peer_cache_path = {os.path.join(configdir, 'smci_peers.json')}",
    ]
    options = {}
    if fast:
        options.update(FAST_TIMING)
    if capture_dir:
        options["packet_capture_enabled"] = "yes"
        options["packet_capture_dir"] = capture_dir
    options.update(extra)
    options.pop("stats_interval", None)  # configobj rejects duplicate keys; it's already above
    for key, value in options.items():
        lines.append(f"    {key} = {value}")
    with open(os.path.join(configdir, "config"), "w") as f:
        f.write("\n".join(lines) + "\n")


def run_node(args) -> None:
    host, port = args.server.rsplit(":", 1)
    port = int(port)
    configdir = args.configdir or tempfile.mkdtemp(prefix=f"smci-mp-{args.name}-")
    extra = dict(kv.split("=", 1) for kv in args.iface_option)
    write_node_config(configdir, args.name, host, port, not args.production_timing, args.capture_dir,
                      args.transport_node, args.loglevel, extra)

    def log(msg: str) -> None:
        if args.air_log:
            print(f"[{args.name}] {msg}", flush=True)

    radio_options = RadioOptions(advert_interval_s=args.advert_interval)

    def radio_factory() -> SimRadio:
        return SimRadio(args.name, RemoteAir(host, port, args.name, log=log), options=radio_options)

    from simmesh.fake_meshcore import make_fake_meshcore_module
    sys.modules["meshcore"] = make_fake_meshcore_module(radio_factory)

    import RNS
    reticulum = RNS.Reticulum(configdir=configdir)
    status("rns_up", name=args.name, configdir=configdir)

    iface = next((i for i in RNS.Transport.interfaces if getattr(i, "name", "") == args.name), None)
    if iface is None or not getattr(iface, "online", False):
        status("error", name=args.name, reason="interface did not come online")
        sys.exit(1)
    status("iface_online", name=args.name)

    # Advert once now that the air link is up (the field-setup step).
    iface._loop.call_soon_threadsafe(iface._mc.radio.cmd_send_advert)

    if args.role == "responder":
        identity = RNS.Identity()
        destination = RNS.Destination(identity, RNS.Destination.IN, RNS.Destination.SINGLE, APP_NAME, ASPECT)
        destination.set_proof_strategy(RNS.Destination.PROVE_ALL)
        received = {"count": 0}

        def on_packet(data, packet):
            received["count"] += 1
            seq = PAYLOAD_HEADER.unpack(data[:PAYLOAD_HEADER.size])[0] if len(data) >= PAYLOAD_HEADER.size else None
            status("probe_received", name=args.name, seq=seq, count=received["count"], size=len(data))

        destination.set_packet_callback(on_packet)
        status("ready", name=args.name, dest=destination.hash.hex())
        next_announce = 0.0
        while True:
            now = time.monotonic()
            if now >= next_announce:
                destination.announce()
                status("announced", name=args.name, peers=list(iface._peers.keys()), resolved=list(iface._resolved_paths.keys()))
                next_announce = now + args.announce_interval
            time.sleep(0.5)

    # sender
    destination_hash = bytes.fromhex(args.dest)
    deadline = time.monotonic() + args.path_timeout
    requested_at = 0.0
    while not RNS.Transport.has_path(destination_hash) and time.monotonic() < deadline:
        if time.monotonic() - requested_at > args.path_request_interval:
            RNS.Transport.request_path(destination_hash)
            requested_at = time.monotonic()
            status("path_requested", name=args.name)
        time.sleep(0.2)
    if not RNS.Transport.has_path(destination_hash):
        status("done", name=args.name, sent=0, delivered=0, rtts=[], reason="path request timed out")
        sys.exit(2)
    status("path_resolved", name=args.name, hops=RNS.Transport.hops_to(destination_hash))

    server_identity = RNS.Identity.recall(destination_hash)
    request_destination = RNS.Destination(server_identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, ASPECT)
    sent = delivered = 0
    rtts = []
    for seq in range(1, args.probes + 1):
        if sent:
            time.sleep(args.wait)
        payload = PAYLOAD_HEADER.pack(seq, time.time()) + os.urandom(max(0, args.size - PAYLOAD_HEADER.size))
        receipt = RNS.Packet(request_destination, payload).send()
        sent += 1
        probe_deadline = time.monotonic() + args.probe_timeout
        while receipt.status == RNS.PacketReceipt.SENT and time.monotonic() < probe_deadline:
            time.sleep(0.05)
        if receipt.status == RNS.PacketReceipt.DELIVERED:
            delivered += 1
            rtts.append(receipt.get_rtt())
            status("probe", name=args.name, seq=seq, delivered=True, rtt_s=round(receipt.get_rtt(), 3))
        else:
            status("probe", name=args.name, seq=seq, delivered=False)
    status("done", name=args.name, sent=sent, delivered=delivered, rtts=[round(r, 3) for r in rtts],
           peers=list(iface._peers.keys()), resolved={k: v.out_path_len for k, v in iface._resolved_paths.items()})
    sys.exit(0 if delivered == sent else 2)


# ---------------------------------------------------------------------------
# orchestrator
# ---------------------------------------------------------------------------

class NodeProcess:
    def __init__(self, name: str, argv: list, echo: bool):
        self.name = name
        self.proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        self.events = []
        self._lock = threading.Lock()
        self._echo = echo
        self._thread = threading.Thread(target=self._pump, daemon=True)
        self._thread.start()

    def _pump(self) -> None:
        for line in self.proc.stdout:
            line = line.rstrip("\n")
            if line.startswith("@@"):
                try:
                    event = json.loads(line[2:])
                except json.JSONDecodeError:
                    continue
                with self._lock:
                    self.events.append(event)
                print(f"{time.strftime('%H:%M:%S')} [{self.name}] {event}", flush=True)
            elif self._echo:
                print(f"[{self.name}] {line}", flush=True)

    def wait_event(self, name: str, timeout: float):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                for event in self.events:
                    if event.get("event") == name:
                        return event
                if any(e.get("event") == "error" for e in self.events):
                    return None
            if self.proc.poll() is not None:
                return None
            time.sleep(0.1)
        return None

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        # Let the reader thread echo whatever the child printed on its way out.
        self._thread.join(timeout=5)


def run_orchestrator(args) -> None:
    adjacency = parse_links(args.link)
    repeaters = set(args.repeater)
    for name in (args.responder, args.sender):
        if name not in adjacency or name in repeaters:
            sys.exit(f"{name!r} must be a non-repeater node in the --link topology")

    def log(msg: str) -> None:
        if not args.quiet_air:
            print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)

    air = Air(adjacency, seed=args.seed, loss=args.loss, link_loss=parse_link_loss(args.link_loss),
              type_loss=parse_type_loss(args.type_loss), airtime_base_ms=args.airtime_base_ms,
              airtime_per_byte_ms=args.airtime_per_byte_ms, log=log)
    repeater_radios = {}
    for name in repeaters:
        radio = SimRadio(name, air, is_repeater=True, options=RadioOptions(advert_interval_s=args.advert_interval))
        radio.attach(None)
        repeater_radios[name] = radio
    server = AirServer(air, "127.0.0.1", 0)
    port = server.start()
    print(f"air server on 127.0.0.1:{port} topology={ {k: sorted(v) for k, v in adjacency.items()} } repeaters={sorted(repeaters)} seed={args.seed}", flush=True)

    capture_dir = args.capture_dir
    if capture_dir:
        os.makedirs(capture_dir, exist_ok=True)

    def node_argv(name: str, role: str, extra: list) -> list:
        argv = [sys.executable, os.path.abspath(__file__), "node", "--name", name, "--server", f"127.0.0.1:{port}",
                "--role", role, "--loglevel", str(args.loglevel), "--advert-interval", str(args.advert_interval)]
        if args.production_timing:
            argv.append("--production-timing")
        if capture_dir:
            argv += ["--capture-dir", capture_dir]
        if name in args.transport_node:
            argv.append("--transport-node")
        if args.air_log:
            argv.append("--air-log")
        for opt in args.iface_option:
            argv += ["--iface-option", opt]
        return argv + extra

    procs = []
    exit_code = 2
    try:
        responder = NodeProcess(args.responder, node_argv(args.responder, "responder", ["--announce-interval", str(args.announce_interval)]), echo=not args.quiet_nodes)
        procs.append(responder)
        ready = responder.wait_event("ready", timeout=60)
        if ready is None:
            responder.stop()
            print(f"responder never became ready (exit code {responder.proc.returncode}) -- see its output above", flush=True)
            sys.exit(2)
        time.sleep(args.settle)

        sender = NodeProcess(args.sender, node_argv(args.sender, "sender", [
            "--dest", ready["dest"], "--probes", str(args.probes), "--wait", str(args.wait), "--size", str(args.size),
            "--path-timeout", str(args.path_timeout), "--probe-timeout", str(args.probe_timeout),
        ]), echo=not args.quiet_nodes)
        procs.append(sender)
        done = sender.wait_event("done", timeout=args.path_timeout + args.probes * (args.wait + args.probe_timeout) + 30)
        print("")
        if done is None:
            print("=== Result: sender never reported completion ===")
        else:
            rtts = done.get("rtts", [])
            rtt_str = f"min={min(rtts):.2f}s avg={sum(rtts)/len(rtts):.2f}s max={max(rtts):.2f}s" if rtts else "n/a"
            print(f"=== Result: {done.get('delivered')}/{done.get('sent')} probe(s) delivered, RTT {rtt_str} ===")
            if done.get("reason"):
                print(f"    reason: {done['reason']}")
            exit_code = 0 if done.get("sent") and done.get("delivered") == done.get("sent") else 2
        received = [e for e in responder.events if e.get("event") == "probe_received"]
        print(f"    responder received {len(received)} probe packet(s)")
        print(f"    air: {air.stats.summary()}")
        for name, radio in repeater_radios.items():
            print(f"    {name} (repeater): {dict(radio.counters)}")
    finally:
        for p in procs:
            p.stop()
        server.stop()
        air.stop()
    sys.exit(exit_code)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="mode", required=True)

    run = sub.add_parser("run", help="Orchestrate: simulated air + one subprocess per RNS node")
    run.add_argument("--link", action="append", required=True, metavar="A-B")
    run.add_argument("--repeater", action="append", default=[], metavar="NAME")
    run.add_argument("--responder", required=True)
    run.add_argument("--sender", required=True)
    run.add_argument("--transport-node", action="append", default=[], metavar="NAME", help="Enable RNS transport on this node")
    run.add_argument("--probes", type=int, default=5)
    run.add_argument("--wait", type=float, default=3.0, help="Seconds between probes")
    run.add_argument("--size", type=int, default=32)
    run.add_argument("--loss", type=float, default=0.0)
    run.add_argument("--link-loss", action="append", default=[], metavar="FROM>TO=P")
    run.add_argument("--type-loss", action="append", default=[], metavar="TYPENAME=P")
    run.add_argument("--seed", type=int, default=None)
    run.add_argument("--airtime-base-ms", type=float, default=50.0)
    run.add_argument("--airtime-per-byte-ms", type=float, default=1.0)
    run.add_argument("--production-timing", action="store_true")
    run.add_argument("--iface-option", action="append", default=[], metavar="KEY=VALUE", help="Extra interface config for every node")
    run.add_argument("--capture-dir", default=None)
    run.add_argument("--announce-interval", type=float, default=15.0)
    run.add_argument("--advert-interval", type=float, default=20.0,
                     help="Seconds between each radio's re-adverts; a node that boots after its neighbor's "
                          "advert only learns it from the next one, so this bounds setup time")
    run.add_argument("--settle", type=float, default=8.0, help="Seconds after the responder is ready before starting the sender")
    run.add_argument("--path-timeout", type=float, default=120.0)
    run.add_argument("--probe-timeout", type=float, default=60.0)
    run.add_argument("--loglevel", type=int, default=4, help="RNS loglevel in node processes (4 = info)")
    run.add_argument("--quiet-air", action="store_true")
    run.add_argument("--quiet-nodes", action="store_true", help="Don't echo node RNS logs, only status events")
    run.add_argument("--air-log", action="store_true", help="Node-side radio logs")

    node = sub.add_parser("node", help="(internal) one RNS node")
    node.add_argument("--name", required=True)
    node.add_argument("--server", required=True)
    node.add_argument("--role", choices=["responder", "sender"], required=True)
    node.add_argument("--dest", default=None)
    node.add_argument("--probes", type=int, default=5)
    node.add_argument("--wait", type=float, default=3.0)
    node.add_argument("--size", type=int, default=32)
    node.add_argument("--configdir", default=None)
    node.add_argument("--production-timing", action="store_true")
    node.add_argument("--capture-dir", default=None)
    node.add_argument("--transport-node", action="store_true")
    node.add_argument("--iface-option", action="append", default=[])
    node.add_argument("--announce-interval", type=float, default=15.0)
    node.add_argument("--advert-interval", type=float, default=20.0)
    node.add_argument("--path-timeout", type=float, default=120.0)
    node.add_argument("--path-request-interval", type=float, default=20.0)
    node.add_argument("--probe-timeout", type=float, default=60.0)
    node.add_argument("--loglevel", type=int, default=4)
    node.add_argument("--air-log", action="store_true")

    args = parser.parse_args()
    if args.mode == "run":
        run_orchestrator(args)
    else:
        if args.role == "sender" and not args.dest:
            parser.error("--dest is required for the sender role")
        run_node(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
