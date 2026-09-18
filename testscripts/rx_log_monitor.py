#!/usr/bin/env python3
"""
rx_log_monitor.py

Subscribes to a local MeshCore companion's raw-RX log feed
(`EventType.RX_LOG_DATA`) and prints one line per packet the radio decodes
-- addressed to this node or not. This script never transmits anything;
it only listens to what the firmware already pushes over serial.

What it isolates: whether THIS radio + firmware + installed `meshcore`
library actually deliver the RX log feed the interface's observe-only tap
(`rx_log_observe_enabled`, added 2026-09-18) depends on, and what the feed
looks like on a real mesh -- independent of RNS, of the interface, and of
any other node's cooperation. The companion firmware source
(referenceprojects/MeshCore-main/examples/companion_radio/MyMesh.cpp,
`logRxRaw`) pushes PUSH_CODE_LOG_RX_DATA for every received packet with no
pref gating it, but an older companion build on a given radio may differ:
run this first. If nothing prints while another node is known to be
transmitting nearby, the feed isn't available on that radio and the
interface's `rx_log` capture records will be absent for the same reason.

Each line shows the time since the previous logged packet (burst
structure), SNR/RSSI, MeshCore payload type and route type, path length
and path hashes, the 1-byte dest/src routing hashes for addressed payload
types, the 4-byte code for an ACK, and the library's packet hash. A packet
whose hash was already seen within --echo-window seconds is flagged `ECHO`
-- that's a repeater re-transmitting something this radio already heard
(the raw material for "did the repeater pick up my frame" once the
interface starts reasoning about this feed).

Usage:
    python3 rx_log_monitor.py --port /dev/ttyUSB0 --duration 120
    python3 rx_log_monitor.py --port /dev/ttyUSB0 --duration 0   # run until Ctrl-C
"""
import argparse
import asyncio
import collections
import time

from meshcore import MeshCore, EventType

ADDRESSED_PAYLOAD_TYPES = {0, 1, 2, 8}  # REQ, RESPONSE, TXT_MSG, PATH: payload starts [dest_hash][src_hash]
PAYLOAD_TYPE_ACK = 3


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", default="/dev/ttyUSB0", help="Serial port (default: /dev/ttyUSB0)")
    parser.add_argument("--baud", type=int, default=115200, help="Baud rate (default: 115200)")
    parser.add_argument("--duration", type=float, default=120.0, help="Seconds to run; 0 = until Ctrl-C (default: 120)")
    parser.add_argument("--echo-window", type=float, default=10.0,
                        help="Seconds within which a repeated pkt_hash is flagged as a repeater echo (default: 10)")
    args = parser.parse_args()

    if not hasattr(EventType, "RX_LOG_DATA"):
        print("Installed meshcore library has no EventType.RX_LOG_DATA -- upgrade the library; nothing to monitor.")
        return

    print(f"Connecting to {args.port} @ {args.baud}...")
    mc = await MeshCore.create_serial(args.port, args.baud)
    print("Connected. Listening to the raw-RX log feed"
          + (f" for {args.duration:.0f}s" if args.duration > 0 else " until Ctrl-C")
          + ". This script transmits nothing.\n")

    start = time.monotonic()
    state = {"last_at": None, "total": 0}
    by_type = collections.Counter()
    echoes = 0
    recent_hashes = collections.OrderedDict()  # pkt_hash -> monotonic time first seen

    header = (f"{'t+s':>7}  {'Δprev':>6}  {'snr':>5}  {'rssi':>5}  {'type':<9} {'route':<9} "
              f"{'plen':>4}  {'path':<14} {'src>dst':<7} {'ack':<8} {'len':>3}  {'pkt_hash':<10} note")
    print(header)
    print("-" * len(header))

    def on_rx_log(event):
        nonlocal echoes
        p = event.payload if isinstance(event.payload, dict) else {}
        now = time.monotonic()
        delta = (now - state["last_at"]) if state["last_at"] is not None else None
        state["last_at"] = now
        state["total"] += 1

        ptype = p.get("payload_type")
        ptname = str(p.get("payload_typename", "UNK"))
        by_type[ptname] += 1
        raw = p.get("pkt_payload") or b""
        srcdst = ack = ""
        if ptype in ADDRESSED_PAYLOAD_TYPES and len(raw) >= 2:
            srcdst = f"{raw[1]:02x}>{raw[0]:02x}"
        elif ptype == PAYLOAD_TYPE_ACK and len(raw) >= 4:
            ack = bytes(raw[:4]).hex()

        pkt_hash = p.get("pkt_hash")
        note = ""
        # prune, then check
        for h, t in list(recent_hashes.items()):
            if now - t > args.echo_window:
                recent_hashes.pop(h, None)
        if pkt_hash is not None:
            if pkt_hash in recent_hashes:
                note = f"ECHO (+{now - recent_hashes[pkt_hash]:.2f}s)"
                echoes += 1
            else:
                recent_hashes[pkt_hash] = now

        print(
            f"{now - start:7.2f}  {('%.2f' % delta) if delta is not None else '-':>6}  "
            f"{p.get('snr')!s:>5}  {p.get('rssi')!s:>5}  {ptname:<9} {str(p.get('route_typename', '?')):<9} "
            f"{p.get('path_len')!s:>4}  {str(p.get('path', '')):<14} {srcdst:<7} {ack:<8} "
            f"{p.get('payload_length')!s:>3}  {str(pkt_hash):<10} {note}"
        )

    mc.subscribe(EventType.RX_LOG_DATA, on_rx_log)

    try:
        if args.duration > 0:
            await asyncio.sleep(args.duration)
        else:
            while True:
                await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await mc.disconnect()

    elapsed = time.monotonic() - start
    print(f"\nDone: {state['total']} packet(s) heard in {elapsed:.0f}s, {echoes} flagged as repeater echoes.")
    if by_type:
        print("By payload type: " + ", ".join(f"{k}={v}" for k, v in by_type.most_common()))
    else:
        print("Nothing heard. If another node was transmitting within range the whole time, this radio's "
              "firmware isn't pushing the RX log feed (older companion build?) -- the interface's rx_log "
              "capture records will be absent on this radio for the same reason.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
