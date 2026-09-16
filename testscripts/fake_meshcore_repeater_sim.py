#!/usr/bin/env python3
"""
fake_meshcore_repeater_sim.py

A fake `meshcore` library plus an in-process simulated CHANNEL flood-relay
network, so Interface/SmartMeshCoreInterface.py's fragmentation/reassembly/
spacing behavior (Milestones 1-3) can be exercised against a multi-hop
topology -- something two physical radios sitting side by side can't
reproduce -- with no real radio hardware at all.

Unlike testscripts/relay_delivery_test.py (which talks to real Reticulum
over a real physical repeater hop) or path_discovery_diag.py/
rf_activity_monitor.py (which talk to a real meshcore-connected radio),
this constructs real `SmartMeshCoreInterface` instances directly (bypassing
RNS.Reticulum/Transport entirely, the same "talk to the interface, not the
whole stack" isolation those two scripts use) against an entirely
in-process, software-simulated mesh. That trade gets you two things a real
radio can't: a topology you can define exactly (who can hear whom), and a
network you can run with a fixed --seed for a reproducible A/B comparison
-- exactly the property docs/interface_architecture.md's Testing approach
section flags as missing from the old fragment-count experiment tool's own
--simulate-loss mode.

WHAT IS AND ISN'T SIMULATED

Modeled, per docs/meshcore_protocol_rules.md's shared rules:
  - Content-hash dedup with no TTL (rule 3): a 160-slot ring buffer per
    node (SimpleMeshTables' real size) -- a node that's already relayed a
    given packet's hash silently won't relay it again, no matter how much
    later a duplicate arrives.
  - Per-hop flood relay jitter, 0-5x the packet's own airtime (rule 7)
    -- an independent random draw at every hop.
  - The confirmed half-duplex blind spot (rule 7): while a node is itself
    mid-transmission (relaying something), it cannot receive anything --
    a repeater busy relaying fragment 0 can genuinely miss fragment 1
    arriving from the origin in that exact window.
  - Optional independent random loss per hop (--loss), for a lossier
    baseline than dedup/half-duplex alone produce.

Also modeled, since Milestone 5 (docs/peer_discovery_design.md): bind
frames ride the same simulated CHANNEL as everything else, so peer
discovery -- bootstrap REQUESTs, RESPONSEs, dedup/jitter/half-duplex
losses affecting them exactly like any other CHANNEL traffic -- is
exercised for real across the defined topology, not stubbed out.

NOT modeled (out of scope for what this tool exists to test):
  - DIRECT sends (this interface's DIRECT code path isn't exercised here
    at all -- send_msg always returns ERROR from the simulated firmware).
  - MeshCore's own native contact/route table: `contacts` is always
    empty and `get_contact_by_key_prefix` always returns None, so
    telemetry-permission granting, path discovery, and Milestone 5's
    DIRECT-primary/DIRECT-supplement routing decisions all fall straight
    back to their no-contact-resolvable fallback (broadcast, or a no-op)
    every time -- exactly the DIRECT-not-simulated behavior above, just
    reached from a different angle. Milestone 5's own peer registry
    (`_peers`, bind frames) is independent of this and unaffected.
  - CAD-busy retry timing, real LoRa symbol-level airtime (SF/BW/CR), or
    RSSI/SNR -- airtime is a simple linear bytes-to-milliseconds estimate
    (--airtime-base-ms / --airtime-per-byte-ms), explicitly an
    approximation good enough to drive the jitter/half-duplex mechanisms
    above, not a physical-layer-accurate model. Don't read timing numbers
    out of this tool as real-world predictions -- use it to check this
    interface's own *logic* (does reassembly complete under jitter and
    occasional loss, does the union-of-passes effect show up, does a
    heavily lossy multi-hop topology behave differently from a clean
    one), the same way this project's field tests separate "is this an
    RF problem" from "is this an interface-layer problem."

USAGE

Define a topology as a set of direct-hearing links (`--link A-R1` means A
and R1 can hear each other; A and B in a chain A-R1-B can only reach each
other by relaying through R1), mark any relay-only nodes with --repeater
(they get no RNS interface of their own, exactly like a real MeshCore
repeater has no application layer), then send from one non-repeater node
and observe delivery at another:

    # Two-hop chain: A and B can only reach each other via R1.
    python3 fake_meshcore_repeater_sim.py \\
        --link A-R1 --link R1-B --repeater R1 \\
        --send-from A --send-to B --payload-size 300 --count 3

    # Same topology with a second, more lossy repeater path, and enough
    # loss that dedup/half-duplex/retry interactions actually show up:
    python3 fake_meshcore_repeater_sim.py \\
        --link A-R1 --link R1-B --link A-R2 --link R2-B \\
        --repeater R1 --repeater R2 \\
        --send-from A --send-to B --loss 0.3 --seed 7 \\
        --payload-size 300 --count 5 --wait 60

Pass --fragment-delay-min/--fragment-delay-max to shrink the interface's
own real 8-15s inter-fragment spacing for faster interactive runs; leave
them unset to test against the real production default.
"""
import argparse
import asyncio
import collections
import hashlib
import importlib.util
import os
import random
import sys
import tempfile
import threading
import time
import types

import RNS

INTERFACE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "Interface", "SmartMeshCoreInterface.py"
)


def _load_interface_module():
    spec = importlib.util.spec_from_file_location("smci_sim_under_test", INTERFACE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# -------------------------------------------------------------------------
# Fake meshcore library surface, backed by a simulated multi-hop network
# -------------------------------------------------------------------------

class SimEventType:
    """Enough of the real meshcore.EventType for what
    SmartMeshCoreInterface actually uses -- checked against
    REQUIRED_EVENT_TYPES so this fake fails loudly, the same way the real
    interface's own startup probe would, if it ever drifts out of sync."""

    OK = "OK"
    ERROR = "ERROR"
    MSG_SENT = "MSG_SENT"
    ACK = "ACK"
    CHANNEL_MSG_RECV = "CHANNEL_MSG_RECV"
    CONTACT_MSG_RECV = "CONTACT_MSG_RECV"
    SELF_INFO = "SELF_INFO"
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    PATH_RESPONSE = "PATH_RESPONSE"
    MESSAGES_WAITING = "MESSAGES_WAITING"


class SimEvent:
    def __init__(self, type, payload=None, attributes=None):
        self.type = type
        self.payload = payload or {}
        self.attributes = attributes or {}


class VirtualMeshNetwork:
    """Simulates MeshCore's CHANNEL flood-relay behavior across a defined
    topology. Runs its own dedicated background event loop/thread,
    decoupled from any one simulated node's own interface loop, so relay
    timing doesn't depend on which node's thread happened to call
    transmit_channel()."""

    RING_BUFFER_SIZE = 160  # SimpleMeshTables' real size, src/helpers/SimpleMeshTables.h

    def __init__(
        self,
        adjacency: dict,
        repeaters: set,
        seed=None,
        loss=0.0,
        airtime_base_ms=50.0,
        airtime_per_byte_ms=1.0,
        log=print,
    ):
        self.adjacency = {name: set(neighbors) for name, neighbors in adjacency.items()}
        for name, neighbors in list(self.adjacency.items()):
            for neighbor in neighbors:
                self.adjacency.setdefault(neighbor, set()).add(name)

        self.repeaters = set(repeaters)
        self.rng = random.Random(seed)
        self.loss = loss
        self.airtime_base_ms = airtime_base_ms
        self.airtime_per_byte_ms = airtime_per_byte_ms
        self.log = log

        self._seen = {name: collections.deque(maxlen=self.RING_BUFFER_SIZE) for name in self.adjacency}
        self._busy_until = {name: 0.0 for name in self.adjacency}
        self._end_nodes = {}  # name -> {"loop": ..., "deliver": coroutine function}

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="VirtualMeshNetwork")
        self._thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def stop(self):
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)

    def register_end_node(self, name, loop, deliver_channel) -> None:
        self._end_nodes[name] = {"loop": loop, "deliver": deliver_channel}

    def _airtime_s(self, text: str) -> float:
        return (self.airtime_base_ms + self.airtime_per_byte_ms * len(text)) / 1000.0

    def transmit_channel(self, from_name: str, raw_text: str) -> None:
        """Called synchronously from the sending node's own interface
        event loop (inside the fake send_chan_msg) -- schedules the relay
        simulation on this network's own loop and returns immediately,
        matching the real firmware's own fire-and-forget CHANNEL send."""
        asyncio.run_coroutine_threadsafe(self._simulate(from_name, raw_text), self._loop)

    async def _simulate(self, origin: str, raw_text: str) -> None:
        packet_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()[:16]
        self._seen[origin].append(packet_hash)
        self.log(f"[SIM] {origin} transmits ({len(raw_text)} chars, hash={packet_hash}).")
        await self._propagate(origin, raw_text, packet_hash, frontier={origin})

    async def _propagate(self, from_node, raw_text, packet_hash, frontier) -> None:
        neighbors = self.adjacency.get(from_node, set()) - frontier
        if not neighbors:
            return
        await asyncio.gather(
            *(
                self._deliver_to(from_node, neighbor, raw_text, packet_hash, frontier | {neighbor})
                for neighbor in neighbors
            )
        )

    async def _deliver_to(self, from_node, to_node, raw_text, packet_hash, frontier) -> None:
        airtime = self._airtime_s(raw_text)
        jitter = self.rng.uniform(0, 5) * airtime
        await asyncio.sleep(jitter)

        now = time.monotonic()
        if now < self._busy_until.get(to_node, 0.0):
            self.log(
                f"[SIM] {to_node} was mid-transmission (half-duplex blind spot) "
                f"-- missed this relay from {from_node}."
            )
            return

        if self.rng.random() < self.loss:
            self.log(f"[SIM] simulated RF loss: {from_node} -> {to_node} dropped.")
            return

        if packet_hash in self._seen[to_node]:
            self.log(f"[SIM] {to_node} already saw this packet -- deduped, not re-relaying.")
            return
        self._seen[to_node].append(packet_hash)

        if to_node in self._end_nodes:
            self.log(f"[SIM] {from_node} -> {to_node}: delivered to end node.")
            node = self._end_nodes[to_node]
            asyncio.run_coroutine_threadsafe(node["deliver"](raw_text), node["loop"])

        if to_node in self.repeaters:
            self._busy_until[to_node] = time.monotonic() + airtime
            self.log(
                f"[SIM] {from_node} -> {to_node}: relaying "
                f"(busy transmitting for {airtime * 1000:.0f}ms)."
            )
            await self._propagate(to_node, raw_text, packet_hash, frontier)


class SimCommands:
    """Stand-in for meshcore.MeshCore.commands, bound to one simulated
    node's identity within a VirtualMeshNetwork."""

    def __init__(self, node_name: str, network: VirtualMeshNetwork):
        self.node_name = node_name
        self.network = network

    async def send_appstart(self):
        return SimEvent(
            SimEventType.SELF_INFO,
            {"name": self.node_name, "public_key": hashlib.sha256(self.node_name.encode()).hexdigest()},
        )

    async def set_radio(self, freq, bw, sf, cr, repeat=None):
        return SimEvent(SimEventType.OK, {})

    async def set_channel(self, channel_idx, channel_name, channel_secret=None):
        return SimEvent(SimEventType.OK, {})

    async def send_chan_msg(self, chan, msg, timestamp=None):
        # The firmware unconditionally prepends "<name>: " exactly once,
        # at the point of origin (meshcore_protocol_rules.md CHANNEL rule
        # 2) -- repeaters relay this same text verbatim, they don't
        # re-prefix it with their own name.
        self.network.transmit_channel(self.node_name, f"{self.node_name}: {msg}")
        return SimEvent(SimEventType.OK, {})

    async def send_msg(self, dst, msg, timestamp=None, attempt=0):
        # DIRECT is out of scope for this tool -- see the module
        # docstring's "NOT modeled" section. Fail loudly rather than
        # silently pretending to succeed.
        return SimEvent(SimEventType.ERROR, {"reason": "DIRECT is not simulated by this tool"})

    async def set_telemetry_mode_base(self, telemetry_mode_base):
        # Called unconditionally during _async_setup since Milestone 4 --
        # this tool doesn't model MeshCore's own contact/telemetry-
        # permission table at all (see get_contact_by_key_prefix below),
        # so this is a pure no-op that just needs to not raise.
        return SimEvent(SimEventType.OK, {})

    async def change_contact_flags(self, contact, flags):
        return SimEvent(SimEventType.OK, {})

    async def change_contact_path(self, contact, path, path_hash_mode=None):
        return SimEvent(SimEventType.OK, {})

    async def reset_path(self, contact):
        return SimEvent(SimEventType.OK, {})

    async def send_path_discovery_sync(self, contact, timeout=0, min_timeout=0):
        # Path discovery, like DIRECT, is out of scope for this tool --
        # get_contact_by_key_prefix always returning None means
        # discover_path() never reaches this call in practice (Milestone
        # 5's routing dispatcher only calls DIRECT-related code once a
        # contact resolves), but this exists so a direct call doesn't
        # raise AttributeError.
        return None


class SimMeshCore:
    """Stand-in for a connected meshcore.MeshCore instance, bound to one
    simulated node's identity within a VirtualMeshNetwork."""

    def __init__(self, node_name: str, network: VirtualMeshNetwork):
        self.node_name = node_name
        self.network = network
        self.commands = SimCommands(node_name, network)
        self._subscriptions = {}
        network.register_end_node(node_name, asyncio.get_running_loop(), self._deliver_channel)

    @property
    def contacts(self):
        # This tool doesn't model MeshCore's own native contact/route
        # table at all (see the module docstring's "NOT modeled" section)
        # -- always empty. Milestone 5's peer registry (bind frames,
        # _peers) is independent of this and still fully exercised: it's
        # only DIRECT sends and telemetry/path-discovery device calls,
        # all gated on a resolvable contact, that this stubs out.
        return {}

    async def ensure_contacts(self, follow=False):
        return True

    def get_contact_by_key_prefix(self, prefix):
        return None

    async def wait_for_event(self, event_type, attribute_filters=None, timeout=None):
        # No ACK is ever simulated (send_msg always errors, and no
        # contact ever resolves for a DIRECT send to reach this point in
        # the first place) -- exists only so Milestone 5's
        # _send_direct_and_await_ack doesn't hit AttributeError if it's
        # ever reached. Behaves like a real timeout: waits it out, then
        # returns None.
        if timeout:
            await asyncio.sleep(timeout)
        return None

    def subscribe(self, event_type, callback):
        self._subscriptions.setdefault(event_type, []).append(callback)

    async def _deliver_channel(self, raw_text: str) -> None:
        event = SimEvent(SimEventType.CHANNEL_MSG_RECV, {"text": raw_text})
        for callback in self._subscriptions.get(SimEventType.CHANNEL_MSG_RECV, []):
            callback(event)

    async def start_auto_message_fetching(self):
        # This simulator's own _deliver_channel already pushes
        # CHANNEL_MSG_RECV directly to subscribers the moment the
        # VirtualMeshNetwork delivers a packet -- it never models the real
        # firmware's queue-then-MESSAGES_WAITING-then-get_msg() semantics
        # (see SmartMeshCoreInterface._start_auto_message_fetching's own
        # docstring for why the real interface needs this call at all), so
        # there's nothing here to actually start.
        pass

    async def stop_auto_message_fetching(self):
        pass

    async def disconnect(self):
        pass


def make_fake_meshcore_module(node_name: str, network: VirtualMeshNetwork):
    module = types.ModuleType("meshcore")
    module.EventType = SimEventType

    class SimMeshCoreFactory:
        @staticmethod
        async def create_serial(port, baudrate, auto_reconnect=False, max_reconnect_attempts=3):
            return SimMeshCore(node_name, network)

        @staticmethod
        async def create_ble(name, auto_reconnect=False, max_reconnect_attempts=3):
            return SimMeshCore(node_name, network)

        @staticmethod
        async def create_tcp(host, port, auto_reconnect=False, max_reconnect_attempts=3):
            return SimMeshCore(node_name, network)

    module.MeshCore = SimMeshCoreFactory
    return module


class SimConfig(dict):
    def get(self, key, default=None):
        return super().get(key, default)


class RecordingOwner:
    """Stand-in for RNS.Transport -- this tool constructs
    SmartMeshCoreInterface directly rather than through real RNS
    Reticulum/Transport (see the module docstring), so this just records
    and prints every inbound() delivery."""

    def __init__(self, name: str, log):
        self.name = name
        self.log = log
        self.received = []

    def inbound(self, data, interface):
        self.received.append(data)
        self.log(f"[{self.name}] received {len(data)} bytes: {bytes(data[:40])!r}...")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--link", action="append", required=True, metavar="A-B",
        help="A direct-hearing edge in the simulated topology, e.g. --link A-R1. Repeatable.",
    )
    parser.add_argument(
        "--repeater", action="append", default=[], metavar="NAME",
        help="Mark a node as relay-only: no RNS interface of its own, exactly like a real "
             "MeshCore repeater has no application layer. Repeatable.",
    )
    parser.add_argument("--send-from", required=True, metavar="NAME")
    parser.add_argument("--send-to", required=True, metavar="NAME")
    parser.add_argument("--payload-size", type=int, default=64, help="Outgoing payload size in bytes (default: 64)")
    parser.add_argument("--count", type=int, default=1, help="Number of payloads to send (default: 1)")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between sends (default: 2.0)")
    parser.add_argument("--loss", type=float, default=0.0, help="Independent simulated loss probability per hop, 0.0-1.0 (default: 0.0)")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducible jitter/loss draws (default: unseeded)")
    parser.add_argument("--airtime-base-ms", type=float, default=50.0, help="Simplified airtime model base (ms, default: 50)")
    parser.add_argument("--airtime-per-byte-ms", type=float, default=1.0, help="Simplified airtime model, ms per byte (default: 1.0)")
    parser.add_argument(
        "--fragment-delay-min", type=float, default=None,
        help="Override fragment_delay_min (seconds). Default: unset, uses the interface's real 8.0s production default.",
    )
    parser.add_argument(
        "--fragment-delay-max", type=float, default=None,
        help="Override fragment_delay_max (seconds). Default: unset, uses the interface's real 15.0s production default.",
    )
    parser.add_argument("--wait", type=float, default=30.0, help="Seconds to wait after the last send before reporting results (default: 30)")
    parser.add_argument("--debug", action="store_true", help="Enable the interfaces' own debug-level logging")
    args = parser.parse_args()

    adjacency = {}
    for link in args.link:
        if "-" not in link:
            parser.error(f"--link {link!r} must be of the form A-B")
        a, b = link.split("-", 1)
        adjacency.setdefault(a, []).append(b)
        adjacency.setdefault(b, []).append(a)

    repeaters = set(args.repeater)
    end_node_names = [name for name in adjacency if name not in repeaters]
    if args.send_from not in end_node_names:
        parser.error(f"--send-from {args.send_from!r} must be a non-repeater node present in --link topology")
    if args.send_to not in end_node_names:
        parser.error(f"--send-to {args.send_to!r} must be a non-repeater node present in --link topology")

    def log(msg: str) -> None:
        print(f"{time.strftime('%H:%M:%S')} {msg}")

    module = _load_interface_module()

    if RNS.Reticulum.get_instance() is None:
        RNS.Reticulum(configdir=tempfile.mkdtemp())

    network = VirtualMeshNetwork(
        adjacency, repeaters, seed=args.seed, loss=args.loss,
        airtime_base_ms=args.airtime_base_ms, airtime_per_byte_ms=args.airtime_per_byte_ms,
        log=log,
    )

    log(f"Topology: {dict(adjacency)}  repeaters={sorted(repeaters)}  seed={args.seed}  loss={args.loss}")

    interfaces = {}
    owners = {}
    orig_meshcore = sys.modules.get("meshcore")
    try:
        for name in end_node_names:
            owner = RecordingOwner(name, log)
            sys.modules["meshcore"] = make_fake_meshcore_module(name, network)
            cfg_kwargs = dict(name=name, transport="tcp", stats_interval=3600)
            if args.debug:
                cfg_kwargs["debug_level"] = "debug"
            if args.fragment_delay_min is not None:
                cfg_kwargs["fragment_delay_min"] = args.fragment_delay_min
            if args.fragment_delay_max is not None:
                cfg_kwargs["fragment_delay_max"] = args.fragment_delay_max
            iface = module.SmartMeshCoreInterface(owner=owner, configuration=SimConfig(**cfg_kwargs))
            if not iface.online:
                log(f"WARNING: interface for {name!r} did not come online.")
            interfaces[name] = iface
            owners[name] = owner
    finally:
        if orig_meshcore is not None:
            sys.modules["meshcore"] = orig_meshcore
        else:
            sys.modules.pop("meshcore", None)

    try:
        sender = interfaces[args.send_from]
        log(
            f"Sending {args.count} payload(s) of {args.payload_size} bytes from "
            f"{args.send_from} (channel budget {sender._channel_payload_budget()}B), "
            f"observing at {args.send_to}..."
        )

        for i in range(args.count):
            marker = f"sim-probe-{i}-".encode()
            payload = marker + os.urandom(max(0, args.payload_size - len(marker)))
            sender.process_outgoing(payload)
            if i < args.count - 1:
                time.sleep(args.interval)

        deadline = time.time() + args.wait
        while time.time() < deadline:
            time.sleep(0.5)

        received = owners[args.send_to].received
        log(f"\n=== Result: {len(received)}/{args.count} payload(s) delivered to {args.send_to} ===")
        for iface in interfaces.values():
            log(
                f"  {iface.name}: outgoing_dropped_total={iface._outgoing_dropped_total} "
                f"incoming_dropped_total={iface._incoming_dropped_total} "
                f"reassembly_buckets_open={len(iface._reassembly)}"
            )
    finally:
        for iface in interfaces.values():
            iface.detach()
        network.stop()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
