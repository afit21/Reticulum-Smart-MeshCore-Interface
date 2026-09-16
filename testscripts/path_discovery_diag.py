#!/usr/bin/env python3
"""
path_discovery_diag.py

Standalone diagnostic for MeshCore's path-discovery command, independent of
this repo's RNS interface / rnsd entirely. Talks directly to a local
MeshCore device over serial and exercises send_path_discovery_sync() against
one target contact, printing every intermediate result in full instead of
collapsing it to "no response" the way application logs usually do.

Use this to answer one question: does the RAW SEND succeed (MSG_SENT), and
does a PATH_RESPONSE actually come back within the firmware's own suggested
timeout? Both are checked and printed separately, since a plain "None"
result from send_path_discovery_sync() can mean either "the send itself
failed" or "the send worked but nothing replied" -- those are very
different problems, and this script tells you which one you're looking at.

Run this on BOTH ends of a link you're trying to diagnose, ideally around
the same wall-clock moment, and compare:
  - Does the raw send succeed on both sides?
  - Does either side ever get a reply from the other?
  - What does each side's own local RF snapshot (RSSI/SNR/noise floor) look
    like at that moment?

Usage:
    python3 path_discovery_diag.py --port /dev/ttyUSB0 --target 343377c464a79a48
    python3 path_discovery_diag.py --port /dev/ttyUSB0 --target 343377c464a79a48 --attempts 5 --baud 115200

--target is a hex prefix of the OTHER node's MeshCore public key (as much
of it as you have -- 8+ hex chars is normally enough to disambiguate). It
must already be a known contact on this device (run with --list-contacts
first if you're not sure of the exact prefix or whether it's known at all).
"""
import argparse
import asyncio
import sys

from meshcore import MeshCore, EventType


async def list_contacts(mc) -> None:
    await mc.ensure_contacts()
    if not mc.contacts:
        print("No contacts known on this device.")
        return
    print(f"{len(mc.contacts)} contact(s) known:")
    for key, c in mc.contacts.items():
        print(
            f"  {key[:16]}...  adv_name={c.get('adv_name')!r}  "
            f"out_path_len={c.get('out_path_len')}  out_path={c.get('out_path')!r}"
        )


def print_contact(label: str, contact: dict) -> None:
    print(f"{label}:")
    for k, v in contact.items():
        print(f"  {k}: {v!r}")


async def run_diagnostic(mc, target_prefix: str, attempts: int) -> None:
    await mc.ensure_contacts()
    contact = mc.get_contact_by_key_prefix(target_prefix)
    if contact is None:
        print(f"No contact found matching prefix {target_prefix!r}.")
        await list_contacts(mc)
        return

    print_contact("Contact record BEFORE", contact)

    radio = await mc.commands.get_stats_radio()
    if radio and radio.type != EventType.ERROR:
        print(f"\nLocal RF snapshot right now: {radio.payload}")
    else:
        print("\nLocal RF snapshot: unavailable (get_stats_radio failed/timed out)")

    for attempt in range(1, attempts + 1):
        print(f"\n--- attempt {attempt}/{attempts} ---")

        # Raw send first, checked on its own -- this is the step that would
        # fail (MSG_SENT vs ERROR) if something were wrong with how the
        # request itself is built/sent, independent of whether anyone
        # answers it.
        raw = await mc.commands._send_path_discovery_raw(contact["public_key"])
        if raw is None:
            print("Raw send: no result at all (serial/library-level failure)")
            continue
        print(f"Raw send: {raw.type} payload={raw.payload}")
        if raw.type != EventType.MSG_SENT:
            print("  -> request itself was rejected; not waiting for a reply.")
            continue

        suggested_ms = raw.payload.get("suggested_timeout", 4000)
        wait_s = suggested_ms / 800.0
        print(f"Waiting {wait_s:.2f}s for a PATH_RESPONSE "
              f"(firmware-suggested timeout, same math the interface uses)...")
        resp = await mc.dispatcher.wait_for_event(EventType.PATH_RESPONSE, timeout=wait_s)
        if resp is None:
            print("PATH_RESPONSE: none arrived within the window "
                  "(genuine no-reply, not a send failure)")
        else:
            print(f"PATH_RESPONSE: {resp.type} payload={resp.payload}")

        await asyncio.sleep(1)

    print()
    contact_after = mc.get_contact_by_key_prefix(target_prefix)
    if contact_after:
        print_contact("Contact record AFTER", contact_after)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", default="/dev/ttyUSB0", help="Serial port (default: /dev/ttyUSB0)")
    parser.add_argument("--baud", type=int, default=115200, help="Baud rate (default: 115200)")
    parser.add_argument("--target", help="Hex prefix of the target contact's public key")
    parser.add_argument("--attempts", type=int, default=3, help="Number of discovery attempts (default: 3)")
    parser.add_argument("--list-contacts", action="store_true", help="Just list known contacts and exit")
    args = parser.parse_args()

    if not args.target and not args.list_contacts:
        parser.error("--target is required unless --list-contacts is given")

    print(f"Connecting to {args.port} @ {args.baud}...")
    mc = await MeshCore.create_serial(args.port, args.baud)
    print("Connected.")

    try:
        if args.list_contacts:
            await list_contacts(mc)
        else:
            await run_diagnostic(mc, args.target, args.attempts)
    finally:
        await mc.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(1)
