#!/usr/bin/env python3
"""
zero_hop_peer_discovery_test.py

Milestone 5 field test: bind-frame peer discovery and DIRECT routing/ACK
between two real, physically-adjacent (zero-hop, no repeater) MeshCore
radios. Constructs a real SmartMeshCoreInterface directly (bypassing RNS
Reticulum/Transport, same "talk to the interface" isolation
testscripts/relay_delivery_test.py and fake_meshcore_repeater_sim.py use)
against a real serial-connected radio.

SAFETY: always pass --channel-secret with a private, non-default value
when either radio is also joined to a shared/public MeshCore channel with
real third-party participants -- never run this against the public default
channel (see the project's own field-test-channel-isolation lesson).
Bind frames and the one test DATA packet this sends are small and stay on
the private channel/DIRECT only; nothing is sent to any other contact.

USAGE (run one role on each of the two radios):

    # On the radio that will listen and (once a path is resolved) accept
    # a test DIRECT packet:
    python3 zero_hop_peer_discovery_test.py --role listener \\
        --port /dev/ttyUSB0 --channel-secret <64-hex-chars> --duration 90

    # On the other radio, once the listener is already running:
    python3 zero_hop_peer_discovery_test.py --role sender \\
        --port /dev/ttyUSB0 --channel-secret <64-hex-chars> --duration 90

Both roles: wait for bind-frame peer discovery to bind the other node,
attempt discover_path() to it, and -- once resolved -- exercise Milestone
5's DIRECT-send/ACK path via a fixed, hardcoded test destination hash
(both roles reference the same literal bytes, sidestepping needing a real
prior DIRECT delivery to learn it via §7's normal opportunistic-token
mechanism, since this test's whole point is validating that mechanism's
downstream consumer in isolation) -- the sender sends a small DATA/SINGLE
packet against it, the listener seeds its own routing table to expect it.
"""
import argparse
import asyncio
import importlib.util
import os
import sys
import time

import RNS

INTERFACE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "Interface", "SmartMeshCoreInterface.py"
)

# Both roles hardcode the same test destination hash -- see module
# docstring. 16 bytes, matches RNS.Reticulum.TRUNCATED_HASHLENGTH//8.
TEST_DEST_HASH = bytes.fromhex("5a" * 16)


def _load_interface_module():
    spec = importlib.util.spec_from_file_location("smci_field_test", INTERFACE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Config(dict):
    def get(self, key, default=None):
        return super().get(key, default)


class Owner:
    def __init__(self, log):
        self.log = log
        self.received = []

    def inbound(self, data, interface):
        self.received.append(bytes(data))
        self.log(f"received {len(data)} bytes: {bytes(data[:48])!r}...")


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def _wait_until(predicate, timeout, interval=0.2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--role", choices=["sender", "listener"], required=True)
    parser.add_argument("--port", default="/dev/ttyUSB0")
    parser.add_argument("--baudrate", type=int, default=115200)
    parser.add_argument("--channel-secret", required=True, help="64 hex chars -- MUST be private, never the public default")
    parser.add_argument("--channel-idx", type=int, default=0)
    parser.add_argument("--channel-name", default="smci-m5-test")
    parser.add_argument("--duration", type=float, default=90.0, help="Total seconds to run before reporting and exiting")
    parser.add_argument("--bind-timeout", type=float, default=60.0, help="Seconds to wait for bind-frame peer discovery")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--packet-capture-dir", default=None,
                        help="Enable the interface's own JSONL packet capture (packet_capture_enabled) into this "
                             "directory -- includes the observe-only 'rx_log' records (2026-09-18) for every packet "
                             "the radio overhears, so this test doubles as a check of that tap on real hardware.")
    args = parser.parse_args()

    module = _load_interface_module()

    if RNS.Reticulum.get_instance() is None:
        import tempfile
        RNS.Reticulum(configdir=tempfile.mkdtemp())

    owner = Owner(log)
    cfg = Config(
        name=f"smci-m5-{args.role}",
        transport="serial",
        port=args.port,
        baudrate=args.baudrate,
        channel_idx=args.channel_idx,
        channel_name=args.channel_name,
        channel_secret=args.channel_secret,
        stats_interval=15,
        debug_level="debug" if args.debug else "info",
        # Fast-ish bind-response jitter so this interactive test doesn't
        # need to wait out the full production 10-30s window on top of
        # everything else -- still real over-the-air behavior, just a
        # tighter, still-plausible window.
        bind_response_jitter_min=1.0,
        bind_response_jitter_max=4.0,
        **({"packet_capture_enabled": "yes", "packet_capture_dir": args.packet_capture_dir}
           if args.packet_capture_dir else {}),
    )

    log(f"[{args.role}] connecting to {args.port}...")
    iface = module.SmartMeshCoreInterface(owner=owner, configuration=cfg)
    if not iface.online:
        log(f"[{args.role}] FAILED to come online -- see errors above.")
        sys.exit(1)
    log(f"[{args.role}] online. Waiting up to {args.bind_timeout:.0f}s for bind-frame peer discovery...")

    deadline = time.monotonic() + args.duration
    try:
        bound = _wait_until(lambda: len(iface._peers) > 0, timeout=args.bind_timeout)
        if not bound:
            log(f"[{args.role}] no peer bound within {args.bind_timeout:.0f}s -- check both sides are using "
                f"the same --channel-secret/--channel-idx/--channel-name and are within RF range.")
        else:
            for prefix, peer in iface._peers.items():
                log(f"[{args.role}] peer bound: {prefix!r} has_upstream_rns={peer.has_upstream_rns} "
                    f"last_seen={time.time() - peer.last_seen:.1f}s ago")

        peer_prefix = next(iter(iface._peers), None)
        resolved = None
        if peer_prefix is not None:
            log(f"[{args.role}] attempting discover_path({peer_prefix!r})...")
            try:
                resolved = asyncio.run_coroutine_threadsafe(
                    iface.discover_path(peer_prefix), iface._loop
                ).result(timeout=30.0)
            except Exception as exc:
                log(f"[{args.role}] discover_path raised: {exc}")

            if resolved is not None:
                log(f"[{args.role}] path resolved: out_path_len={resolved.out_path_len}")
            else:
                log(f"[{args.role}] path discovery did not resolve (peer may not have granted "
                    f"telemetry permission yet -- try again, or increase --duration).")

        if peer_prefix is not None and resolved is not None:
            if args.role == "listener":
                iface._rns_token_peer[TEST_DEST_HASH] = peer_prefix
                log(f"[{args.role}] seeded routing table: {TEST_DEST_HASH.hex()} -> {peer_prefix!r} "
                    f"(normally learned via §7's opportunistic token learning off a real DIRECT "
                    f"receive -- seeded directly here since this test's point is validating the "
                    f"DIRECT-send/ACK path itself, not token learning, which M4/M5's unit tests "
                    f"already cover).")
            else:
                iface._rns_token_peer[TEST_DEST_HASH] = peer_prefix
                import types
                fake_dest = types.SimpleNamespace(
                    type=RNS.Destination.SINGLE, hash=TEST_DEST_HASH, mtu=RNS.Reticulum.MTU,
                    encrypt=lambda data: b"\x00" * 16 + data,
                )
                packet = RNS.Packet(fake_dest, b"m5-zero-hop-direct-test-payload", packet_type=RNS.Packet.DATA)
                packet.pack()
                log(f"[{args.role}] sending test DATA/SINGLE packet DIRECT via process_outgoing()...")
                iface.process_outgoing(packet.raw)

        remaining = deadline - time.monotonic()
        if remaining > 0:
            log(f"[{args.role}] waiting {remaining:.0f}s more before reporting final state...")
            time.sleep(remaining)

        log(f"[{args.role}] === FINAL STATE ===")
        log(f"[{args.role}] peers: {list(iface._peers.keys())}")
        log(f"[{args.role}] resolved_paths: {list(iface._resolved_paths.keys())}")
        log(f"[{args.role}] direct_path_failures: {dict(iface._direct_path_failures)}")
        log(f"[{args.role}] outgoing_dropped_total={iface._outgoing_dropped_total} "
            f"incoming_dropped_total={iface._incoming_dropped_total}")
        log(f"[{args.role}] owner received {len(owner.received)} payload(s): {owner.received}")
    finally:
        iface.detach()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
