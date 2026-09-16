#!/usr/bin/env python3
"""
rf_activity_monitor.py

Polls a local MeshCore device's own radio/packet counters (get_stats_radio,
get_stats_packets) at a fixed interval and prints a timestamped delta each
time. These are local, no-mesh-airtime commands -- this script never
transmits anything itself, it only reads counters the firmware already
maintains.

Purpose: run this on one node WHILE triggering something (e.g.
path_discovery_diag.py) from the OTHER node, to see whether this node's
`recv` count and `rx_air_secs` actually move during that window. If they
don't move at all, this node isn't physically receiving anything from the
other end over that stretch of time, regardless of what any higher-level
protocol exchange (RNSBIND, path discovery, etc.) reports -- which tells
you the problem is on the RF/physical side, not in a specific command's
handling.

Usage:
    python3 rf_activity_monitor.py --port /dev/ttyUSB0 --interval 2 --duration 60
"""
import argparse
import asyncio
import time

from meshcore import MeshCore, EventType


async def poll_once(mc):
    radio = await mc.commands.get_stats_radio()
    pkts = await mc.commands.get_stats_packets()
    radio_ok = radio is not None and radio.type != EventType.ERROR
    pkts_ok = pkts is not None and pkts.type != EventType.ERROR
    return (
        radio.payload if radio_ok else None,
        pkts.payload if pkts_ok else None,
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", default="/dev/ttyUSB0", help="Serial port (default: /dev/ttyUSB0)")
    parser.add_argument("--baud", type=int, default=115200, help="Baud rate (default: 115200)")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between polls (default: 2.0)")
    parser.add_argument("--duration", type=float, default=60.0, help="Total seconds to run (default: 60)")
    args = parser.parse_args()

    print(f"Connecting to {args.port} @ {args.baud}...")
    mc = await MeshCore.create_serial(args.port, args.baud)
    print("Connected. Polling every "
          f"{args.interval}s for {args.duration}s -- trigger your test on "
          f"the other node now.\n")

    start = time.monotonic()
    prev_recv = None
    prev_rx_air = None

    header = f"{'t+s':>6}  {'recv':>6}  {'Δrecv':>6}  {'rx_air_s':>9}  {'Δrx_air':>8}  {'last_rssi':>9}  {'last_snr':>8}  {'recv_err':>8}"
    print(header)
    print("-" * len(header))

    try:
        while time.monotonic() - start < args.duration:
            elapsed = time.monotonic() - start
            radio, pkts = await poll_once(mc)

            if radio is None or pkts is None:
                print(f"{elapsed:6.1f}  -- poll failed/timed out (radio={radio is not None} pkts={pkts is not None}) --")
            else:
                recv = pkts.get("recv")
                rx_air = radio.get("rx_air_secs")
                d_recv = (recv - prev_recv) if (prev_recv is not None and recv is not None) else 0
                d_rx_air = (rx_air - prev_rx_air) if (prev_rx_air is not None and rx_air is not None) else 0
                prev_recv, prev_rx_air = recv, rx_air

                print(
                    f"{elapsed:6.1f}  {recv!s:>6}  {d_recv!s:>6}  {rx_air!s:>9}  "
                    f"{d_rx_air!s:>8}  {radio.get('last_rssi')!s:>9}  "
                    f"{radio.get('last_snr')!s:>8}  {pkts.get('recv_errors')!s:>8}"
                )

            await asyncio.sleep(args.interval)
    finally:
        await mc.disconnect()

    print("\nDone. If Δrecv/Δrx_air stayed at 0 the whole time the other "
          "side was transmitting, this node never physically received "
          "anything from it during this window.")


if __name__ == "__main__":
    asyncio.run(main())
