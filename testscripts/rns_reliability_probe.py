#!/usr/bin/env python3
"""
rns_reliability_probe.py

End-to-end RNS delivery reliability test for SmartMeshCoreInterface.py --
sends real numbered probe packets through a real `RNS.Reticulum` instance,
over whatever interface(s) that instance's config has configured (in
practice, the `[[Smart MeshCore Interface]]` block), and reports a
sent/delivered/lost summary with round-trip-time stats. This exercises the
interface exactly the way MeshChat, NomadNet, or any other real RNS
application does -- routing decisions, fragmentation, retries, and all --
not the interface's internals directly (see testscripts/
zero_hop_peer_discovery_test.py for that kind of white-box test, and
testscripts/meshbench_scenarios.py (or the archived testscripts/legacy/fake_meshcore_repeater_sim.py) for topology-controlled
simulation with no radio hardware at all).

SETUP

This project's normal deployment is one `rnsd` per physical machine, each
with its own `~/.reticulum/config` and its own MeshCore radio, already
running continuously (see CLAUDE.md). By default this script connects to
whichever Reticulum instance is *already running* on this machine (the
same shared-instance mechanism `rnstatus`/`rnpath` use) rather than
starting a second one -- pass --config only to point at a different
config directory (e.g. the old single-machine "two local rnsd instances"
bench-test layout).

  1. On the responder side, start listening. It prints its destination
     hash once ready:

         python3 rns_reliability_probe.py responder

  2. On the sender side, run the probe run against that hash:

         python3 rns_reliability_probe.py sender \\
             --dest <hash printed by the responder> --probes 20 --wait 20

     Pick --wait deliberately, not a default -- this transport's own
     field data (see docs/reliability_engine_design.md's open question 1)
     shows a single round trip can legitimately take several seconds to
     tens of seconds depending on payload size and hop count, and
     spacing probes tighter than the actual round-trip time causes the
     interface's outgoing queue to back up behind itself (again, see that
     same open question) -- looks like reliability failure at the test
     level even though nothing was actually dropped. 20-30s is a
     reasonable starting point at zero hop; widen it for multi-hop or
     larger --size.

SAFETY: if either radio is also joined to a shared/public MeshCore
channel with real third-party participants, run this only against a
private, non-default channel_secret (see the project's own
field-test-channel-isolation lesson) -- this script doesn't touch channel
config itself (that's the interface's own config block), but the probes
it sends are still real traffic on whatever channel that config points
at.

OUTPUT

Per-probe delivery/RTT/RSSI/SNR lines, a final loss-percentage summary,
and optionally a full per-probe CSV (--csv) for offline analysis. Exit
code is 0 if every probe was delivered, 2 otherwise -- suitable for
scripting into a longer soak/regression check.
"""
import argparse
import csv
import os
import struct
import sys
import time

import RNS

APP_NAME_DEFAULT = "smci_reliability_probe"
ASPECT_DEFAULT = "probe"
DEFAULT_PROBE_SIZE = 32
DEFAULT_TIMEOUT = 45.0
PAYLOAD_HEADER = struct.Struct(">Id")  # seq:uint32, send_time:double
FALLBACK_MEDIUM_PATH_TIMEOUT = 60.0


def medium_path_timeout(reticulum) -> float:
    """RNS.Reticulum.get_medium_path_timeout() isn't present on every
    installed RNS version -- fall back to a fixed, generous timeout
    rather than crashing the probe on an AttributeError."""
    getter = getattr(reticulum, "get_medium_path_timeout", None)
    if getter is None:
        return FALLBACK_MEDIUM_PATH_TIMEOUT
    return getter()


def run_responder(args) -> None:
    reticulum = RNS.Reticulum(configdir=args.config, loglevel=3 + args.verbose)

    identity_path = args.identity_file or os.path.join(
        RNS.Reticulum.storagepath, "reliability_probe_identity"
    )
    if os.path.isfile(identity_path):
        identity = RNS.Identity.from_file(identity_path)
        print(f"Loaded existing probe identity from {identity_path}")
    else:
        identity = RNS.Identity()
        identity.to_file(identity_path)
        print(f"Created new probe identity, saved to {identity_path}")

    destination = RNS.Destination(
        identity, RNS.Destination.IN, RNS.Destination.SINGLE, args.app_name, args.aspect,
    )
    destination.set_proof_strategy(RNS.Destination.PROVE_ALL)

    received = {"count": 0}

    def on_packet(data, packet):
        received["count"] += 1
        seq, send_time = (None, None)
        if len(data) >= PAYLOAD_HEADER.size:
            try:
                seq, send_time = PAYLOAD_HEADER.unpack(data[: PAYLOAD_HEADER.size])
            except struct.error:
                pass
        age_ms = f"{(time.time() - send_time) * 1000:.1f}ms one-way" if send_time else "unknown age"
        rssi = f" rssi={packet.rssi}dBm" if getattr(packet, "rssi", None) is not None else ""
        snr = f" snr={packet.snr}dB" if getattr(packet, "snr", None) is not None else ""
        print(
            f"[{received['count']:4d}] probe seq={seq if seq is not None else '?'} "
            f"({len(data)} bytes, {age_ms}){rssi}{snr} -- proof sent automatically"
        )

    destination.set_packet_callback(on_packet)

    print("Responder ready.")
    print(f"Destination hash: {RNS.prettyhexrep(destination.hash)}")
    print(f"Full name: {args.app_name}.{args.aspect}")
    print("Announcing now, then waiting for probes (Ctrl+C to stop)...\n")
    destination.announce()

    try:
        if args.announce_interval > 0:
            while True:
                time.sleep(args.announce_interval)
                destination.announce()
                print(f"(re-announced at {time.strftime('%H:%M:%S')})")
        else:
            while True:
                time.sleep(3600)
    except KeyboardInterrupt:
        print(f"\nStopped. Received {received['count']} probe(s) total.")


def run_sender(args) -> None:
    dest_len = (RNS.Reticulum.TRUNCATED_HASHLENGTH // 8) * 2
    if len(args.dest) != dest_len:
        print(f"Error: destination hash must be {dest_len} hex characters ({dest_len // 2} bytes).")
        sys.exit(1)
    try:
        destination_hash = bytes.fromhex(args.dest)
    except ValueError:
        print("Error: destination hash is not valid hex.")
        sys.exit(1)

    reticulum = RNS.Reticulum(configdir=args.config, loglevel=3 + args.verbose)

    def wait_window() -> float:
        computed = max(
            args.timeout + reticulum.get_first_hop_timeout(destination_hash),
            medium_path_timeout(reticulum),
        )
        return max(computed, args.timeout)

    if not RNS.Transport.has_path(destination_hash):
        RNS.Transport.request_path(destination_hash)
        print(f"Path to {RNS.prettyhexrep(destination_hash)} requested, waiting...")

    path_timeout = time.time() + wait_window()
    while not RNS.Transport.has_path(destination_hash) and time.time() < path_timeout:
        time.sleep(0.1)

    if not RNS.Transport.has_path(destination_hash):
        print("Path request timed out -- is the responder running and reachable "
              "(has it announced at least once, and can that announce reach this "
              "side, possibly through repeater(s))? Try --timeout higher for a "
              "multi-hop or otherwise slow link.")
        sys.exit(1)

    server_identity = RNS.Identity.recall(destination_hash)
    if server_identity is None:
        print("Path resolved but identity unknown -- this shouldn't normally "
              "happen once has_path() is true. Try again in a moment.")
        sys.exit(1)

    request_destination = RNS.Destination(
        server_identity, RNS.Destination.OUT, RNS.Destination.SINGLE, args.app_name, args.aspect,
    )

    csv_writer = None
    csv_file = None
    if args.csv:
        csv_file = open(args.csv, "w", newline="")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["seq", "sent_unix_ts", "delivered", "rtt_ms", "rssi_dbm", "snr_db"])

    rtts = []
    sent = 0
    delivered = 0

    try:
        for seq in range(1, args.probes + 1):
            if sent > 0:
                time.sleep(args.wait)

            pad_len = max(0, args.size - PAYLOAD_HEADER.size)
            payload = PAYLOAD_HEADER.pack(seq, time.time()) + os.urandom(pad_len)

            packet = RNS.Packet(request_destination, payload)
            receipt = packet.send()
            sent += 1
            send_ts = time.time()

            probe_timeout = time.time() + wait_window()
            while receipt.status == RNS.PacketReceipt.SENT and time.time() < probe_timeout:
                time.sleep(0.05)

            rssi = snr = None
            if receipt.status == RNS.PacketReceipt.DELIVERED:
                delivered += 1
                rtt = receipt.get_rtt()
                rtts.append(rtt)

                if reticulum.is_connected_to_shared_instance:
                    rssi = reticulum.get_packet_rssi(receipt.proof_packet.packet_hash)
                    snr = reticulum.get_packet_snr(receipt.proof_packet.packet_hash)
                elif receipt.proof_packet is not None:
                    rssi = receipt.proof_packet.rssi
                    snr = receipt.proof_packet.snr

                rtt_str = f"{rtt * 1000:.1f}ms" if rtt < 1 else f"{rtt:.3f}s"
                extra = ""
                if rssi is not None:
                    extra += f" rssi={rssi}dBm"
                if snr is not None:
                    extra += f" snr={snr}dB"
                print(f"[{seq:4d}/{args.probes}] delivered  rtt={rtt_str}{extra}")
            else:
                print(f"[{seq:4d}/{args.probes}] LOST (no delivery proof within {wait_window():.0f}s timeout)")

            if csv_writer:
                rtt_ms = (receipt.get_rtt() * 1000) if receipt.status == RNS.PacketReceipt.DELIVERED else ""
                csv_writer.writerow([
                    seq, f"{send_ts:.3f}",
                    receipt.status == RNS.PacketReceipt.DELIVERED,
                    f"{rtt_ms:.1f}" if rtt_ms != "" else "",
                    rssi if rssi is not None else "",
                    snr if snr is not None else "",
                ])
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        if csv_file:
            csv_file.close()

    loss_pct = (1 - delivered / sent) * 100 if sent else 0.0
    print(f"\nSent {sent}, delivered {delivered}, lost {sent - delivered} ({loss_pct:.1f}% loss)")
    if rtts:
        print(
            f"RTT over {len(rtts)} delivered probe(s): "
            f"min={min(rtts)*1000:.1f}ms avg={sum(rtts)/len(rtts)*1000:.1f}ms max={max(rtts)*1000:.1f}ms"
        )
    if args.csv:
        print(f"Per-probe results written to {args.csv}")

    sys.exit(0 if delivered == sent else 2)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="mode", required=True)

    common_app = argparse.ArgumentParser(add_help=False)
    common_app.add_argument("--app-name", default=APP_NAME_DEFAULT, help=f"RNS app name (default: {APP_NAME_DEFAULT})")
    common_app.add_argument("--aspect", default=ASPECT_DEFAULT, help=f"RNS aspect (default: {ASPECT_DEFAULT})")
    common_app.add_argument("--config", default=None, help="Reticulum config directory (default: ~/.reticulum, connecting to the rnsd already running there as a shared-instance client -- pass a different directory only to start/target a separate instance)")
    common_app.add_argument("-v", "--verbose", action="count", default=0, help="increase RNS core log verbosity")

    p_responder = sub.add_parser("responder", parents=[common_app], help="Listen for probes and auto-prove delivery")
    p_responder.add_argument("--identity-file", default=None, help="Path to persist the probe identity (default: <storagepath>/reliability_probe_identity)")
    p_responder.add_argument("--announce-interval", type=float, default=0, help="Seconds between re-announces (0 = announce once at startup only)")

    p_sender = sub.add_parser("sender", parents=[common_app], help="Send probes and report delivery/RTT stats")
    p_sender.add_argument("--dest", required=True, help="Responder's destination hash (hex, printed by 'responder' mode)")
    p_sender.add_argument("-n", "--probes", type=int, default=10, help="Number of probes to send (default: 10)")
    p_sender.add_argument(
        "-w", "--wait", type=float, required=True,
        help="Seconds between probes -- no default on purpose, see the module docstring's SETUP section "
             "on why too-tight spacing looks like reliability failure without actually losing anything.",
    )
    p_sender.add_argument("-s", "--size", type=int, default=DEFAULT_PROBE_SIZE, help=f"Probe payload size in bytes (default: {DEFAULT_PROBE_SIZE})")
    p_sender.add_argument("--csv", default=None, help="Optional path to write per-probe results as CSV")
    p_sender.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help=f"Minimum seconds to wait for path resolution and each probe's delivery proof (default: {DEFAULT_TIMEOUT}). Raise this for multi-hop links or larger --size -- this transport's own field data shows a single round trip can take tens of seconds.")

    args = parser.parse_args()

    if args.mode == "responder":
        run_responder(args)
    else:
        run_sender(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
