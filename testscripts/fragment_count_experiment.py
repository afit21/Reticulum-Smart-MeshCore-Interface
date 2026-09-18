#!/usr/bin/env python3
"""
fragment_count_experiment.py

Standalone MeshCore CHANNEL-delivery experiment, independent of RNS/rnsd
entirely (same style as path_discovery_diag.py) -- talks to a local MeshCore
device directly and sends/receives the *exact* on-wire multi-fragment
framing Interface/SmartMeshCoreInterface.py uses (its own encoder/decoder,
imported directly from that file, so this can never drift out of sync with
what actually ships). For the same experiment with no hardware, see
fake_meshcore_repeater_sim.py, which models the flood-dedup behavior this
script measures.

Why this exists: field testing (fieldtests/reports/alpha-0.1-snapshot2.md,
alpha-0.1-snapshot3.md) found multi-fragment CHANNEL packets over a
multi-hop repeater chain were "bimodal" -- a fast full success, or nothing
at all even after 90s -- while the official MeshCore app, which never sends
a multi-fragment message over a channel at all (confirmed against the
firmware source, see changelog.md), got through reliably on the same
repeaters. Two questions this script is built to answer directly, with
fragment count as the controlled variable RNS's own traffic mix couldn't
isolate:

  1. Does delivery success drop off as fragment count increases (1, 2, 3+),
     independent of anything RNS-specific?
  2. Does a lost fragment's RETRY actually reach any further when its bytes
     are varied between attempts (--vary-retry), versus resent unchanged?
     This tests the specific fix in changelog.md's "Fragment retransmits
     were being silently absorbed by the mesh's own flood dedup" entry:
     every MeshCore node dedupes flood packets by content hash with no
     time-based expiry, so an unchanged resend should be silently dropped
     by any node that already relayed the original, while a varied resend
     (this project's new per-attempt nonce) should propagate fresh.

Run the receiver first, on the far side of whatever link you're testing
(ideally physically separated by real repeater hops, not close range --
close range can't reproduce the failure mode this is investigating):

    python3 fragment_count_experiment.py receiver --port /dev/ttyUSB0

Then the sender, from the other end:

    python3 fragment_count_experiment.py sender --port /dev/ttyUSB0 \\
        --fragment-counts 1,2,3,4 --trials 10

Both ends must already share the same MeshCore channel (idx/name/secret) --
this doesn't configure that; use whatever channel your radios are already
on (matches this project's normal RNSTunnel channel by default, but works
on any channel both sides are joined to).

Output: a summary table of success rate and latency per fragment count,
printed by the sender once all trials complete (it waits for a per-trial
completion marker echoed back by the receiver -- see below). The receiver
also prints a live per-fragment log useful for eyeballing exactly which
index was lost on a failed trial.
"""
import argparse
import asyncio
import importlib.util
import json
import os
import random
import statistics
import sys
import time

from meshcore import MeshCore, EventType

INTERFACE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "Interface", "SmartMeshCoreInterface.py"
)


def load_interface_module():
    """Load SmartMeshCoreInterface.py by path (no package/__init__.py in
    this repo) so this script always tests the exact fragment framing
    that ships, never a hand-copied approximation of it."""
    spec = importlib.util.spec_from_file_location("mci_under_test", INTERFACE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def frame_codec(mci):
    """An uninitialized interface instance: the "R"-marker encoders/decoder
    only use class constants and the module's Z85 helpers, so no radio
    connection (and no __init__) is needed to get byte-exact frames."""
    cls = mci.SmartMeshCoreInterface
    return cls.__new__(cls)


MARKER_PREFIX = "FRAGEXP:"   # distinguishes our test payloads from real RNS tunnel traffic sharing the "R" marker
ACK_PREFIX    = "FRAGEXPACK:"  # receiver -> sender per-trial completion report


def build_trial_payload(size: int) -> bytes:
    """Deterministic-but-distinguishable payload of exactly `size` bytes,
    so a receiver can sanity-check reassembled content, not just fragment
    count/order."""
    return bytes((i * 37 + 11) % 256 for i in range(size))


async def run_sender(args) -> None:
    mci = load_interface_module()
    codec = frame_codec(mci)

    mc = await MeshCore.create_serial(args.port, args.baud)
    info = await mc.commands.send_appstart()
    node_name = info.payload.get("name", "") if isinstance(info.payload, dict) else ""
    # The firmware prepends "<name>: " to every CHANNEL text at the origin;
    # that plus the encoded frame must fit FIRMWARE_TEXT_LIMIT.
    name_prefix_cost = len(f"{node_name}: ")
    await mc.commands.set_channel(
        args.channel_idx, args.channel_name, bytes.fromhex(args.channel_secret)
    )
    # Required for the meshcore library to actually poll the device for
    # new messages at all -- without this, CHANNEL_MSG_RECV never fires
    # over serial (matches what the real interface does at startup).
    await mc.start_auto_message_fetching()
    results = {}  # fragment_count -> list of (success: bool, latency_s or None)

    ack_events = {}  # pkt_id -> asyncio.Event
    ack_data   = {}  # pkt_id -> parsed ack dict

    def on_channel_msg(event):
        text = event.payload.get("text", "")
        # Same "<name>: " compose-time prefix applies to the receiver's
        # plain-text ack -- search for ACK_PREFIX rather than anchoring at
        # index 0 (see the matching comment in run_receiver).
        ack_idx = text.find(ACK_PREFIX)
        if ack_idx == -1:
            return
        try:
            data = json.loads(text[ack_idx + len(ACK_PREFIX):])
        except Exception:
            return
        pkt_id = data.get("pkt_id")
        if pkt_id in ack_events:
            ack_data[pkt_id] = data
            ack_events[pkt_id].set()

    mc.subscribe(EventType.CHANNEL_MSG_RECV, on_channel_msg)

    try:
        fragment_counts = [int(x) for x in args.fragment_counts.split(",")]
        pkt_id_counter = random.randint(0, 0xFFFF)  # the wire pkt_id is 16-bit; random start avoids a prior run's ids

        def encode_fragments(data: bytes, pkt_id: int, per_frag: int, attempt: int) -> list:
            chunks = [data[i:i + per_frag] for i in range(0, len(data), per_frag)]
            frames = [
                codec._encode_channel_multifragment(chunk, pkt_id, idx, len(chunks), attempt)
                for idx, chunk in enumerate(chunks)
            ]
            for frame in frames:
                if name_prefix_cost + len(frame) > codec.FIRMWARE_TEXT_LIMIT:
                    raise SystemExit(
                        f"--payload-size {per_frag} makes a {name_prefix_cost + len(frame)}-char CHANNEL text "
                        f"(limit {codec.FIRMWARE_TEXT_LIMIT} incl. this node's '{node_name}: ' prefix) -- lower it"
                    )
            return frames

        for frag_count in fragment_counts:
            print(f"\n=== fragment_count={frag_count} ({args.trials} trial(s)) ===")
            for trial in range(args.trials):
                pkt_id_counter = (pkt_id_counter + 1) & 0xFFFF
                pkt_id = pkt_id_counter

                # payload_size chosen so chunking produces exactly
                # frag_count fragments: (frag_count-1) full fragments plus
                # a smaller remainder, using a fixed per-fragment size.
                per_frag = args.payload_size
                total_len = per_frag * (frag_count - 1) + max(1, per_frag // 2)
                data = MARKER_PREFIX.encode() + build_trial_payload(total_len - len(MARKER_PREFIX))

                fragments = encode_fragments(data, pkt_id, per_frag, attempt=0)
                assert len(fragments) == frag_count, (
                    f"expected {frag_count} fragments, got {len(fragments)} -- adjust --payload-size"
                )

                ack_events[pkt_id] = asyncio.Event()
                start = time.monotonic()

                drop_idx = None
                if args.simulate_loss and frag_count > 1:
                    drop_idx = random.randrange(frag_count)

                for idx, frag_str in enumerate(fragments):
                    if idx == drop_idx:
                        print(f"  [trial {trial}] simulating loss of fragment {idx}")
                        continue
                    await mc.commands.send_chan_msg(args.channel_idx, frag_str)
                    await asyncio.sleep(args.fragment_delay)

                if drop_idx is not None:
                    await asyncio.sleep(args.retry_delay)
                    retry_attempt = 1 if args.vary_retry else 0
                    retry_fragments = encode_fragments(data, pkt_id, per_frag, attempt=retry_attempt)
                    print(
                        f"  [trial {trial}] resending fragment {drop_idx} "
                        f"(attempt={retry_attempt})"
                    )
                    await mc.commands.send_chan_msg(args.channel_idx, retry_fragments[drop_idx])

                try:
                    await asyncio.wait_for(ack_events[pkt_id].wait(), timeout=args.trial_timeout)
                    latency = time.monotonic() - start
                    ok = ack_data[pkt_id].get("complete", False)
                    results.setdefault(frag_count, []).append((ok, latency))
                    print(
                        f"  [trial {trial}] "
                        f"{'OK' if ok else 'INCOMPLETE'} "
                        f"({ack_data[pkt_id].get('have')}/{frag_count} fragments) "
                        f"in {latency:.1f}s"
                    )
                except asyncio.TimeoutError:
                    results.setdefault(frag_count, []).append((False, None))
                    print(f"  [trial {trial}] NO ACK within {args.trial_timeout:.0f}s")
                finally:
                    ack_events.pop(pkt_id, None)
                    ack_data.pop(pkt_id, None)

                await asyncio.sleep(args.inter_trial_delay)

        print("\n=== Summary ===")
        print(f"{'frag_count':>10} {'success':>10} {'success%':>9} {'avg_latency_s':>14}")
        for frag_count in sorted(results):
            trials = results[frag_count]
            successes = [t for t in trials if t[0]]
            latencies = [t[1] for t in successes if t[1] is not None]
            pct = 100.0 * len(successes) / len(trials) if trials else 0.0
            avg_lat = statistics.mean(latencies) if latencies else float("nan")
            print(
                f"{frag_count:>10} {len(successes):>4}/{len(trials):<5} "
                f"{pct:>8.1f}% {avg_lat:>14.1f}"
            )
    finally:
        await mc.disconnect()


async def run_receiver(args) -> None:
    mci = load_interface_module()
    codec = frame_codec(mci)

    mc = await MeshCore.create_serial(args.port, args.baud)
    await mc.commands.set_channel(
        args.channel_idx, args.channel_name, bytes.fromhex(args.channel_secret)
    )
    # Required for the meshcore library to actually poll the device for
    # new messages at all -- without this, CHANNEL_MSG_RECV never fires
    # over serial (matches what the real interface does at startup).
    await mc.start_auto_message_fetching()

    # pkt_id -> {frag_idx: payload}, plus frag_total once known
    assembly = {}
    assembly_total = {}

    def on_channel_msg(event):
        text = event.payload.get("text", "")
        # The SENDING node's own firmware unconditionally prepends
        # "<node_name>: " to every channel text message at compose time
        # (src/helpers/BaseChatMesh.cpp sendGroupMessage), so the "R"
        # marker is never at index 0 -- split exactly the way the real
        # interface's _on_channel_msg_recv does.
        _sender_name, sep, remainder = text.partition(": ")
        if not sep or not remainder.startswith(codec.MARKER):
            return
        try:
            header, payload = codec._decode_frame(remainder, mode="channel")
        except ValueError as exc:
            print(f"  dropped unparsable fragment: {exc}")
            return
        if not header.multi_fragment:
            return  # a single-fragment fast-path frame: real tunnel traffic, not a trial
        frag_idx, pkt_id, frag_total, attempt = header.frag_idx, header.pkt_id, header.frag_total, header.attempt

        if frag_idx == 0 and not payload.startswith(MARKER_PREFIX.encode()):
            # Real RNS tunnel traffic shares the "R" marker and this exact
            # frame shape -- only content this experiment itself sent
            # carries MARKER_PREFIX right after the header, so anything
            # else is unrelated traffic sharing the channel, not a trial.
            return

        path_len = event.payload.get("path_len")
        bucket = assembly.setdefault(pkt_id, {})
        is_new = frag_idx not in bucket
        bucket[frag_idx] = payload
        assembly_total[pkt_id] = frag_total

        print(
            f"  pkt_id={pkt_id} frag_idx={frag_idx}/{frag_total - 1} "
            f"attempt={attempt} path_len={path_len} "
            f"{'(new)' if is_new else '(duplicate)'} "
            f"-- have {len(bucket)}/{frag_total}"
        )

        if len(bucket) >= frag_total:
            complete = len(bucket) == frag_total and all(
                i in bucket for i in range(frag_total)
            )
            ack = {
                "pkt_id": pkt_id,
                "complete": complete,
                "have": len(bucket),
            }
            asyncio.create_task(
                mc.commands.send_chan_msg(args.channel_idx, ACK_PREFIX + json.dumps(ack))
            )
            print(f"  pkt_id={pkt_id} COMPLETE -- sent ack")
            del assembly[pkt_id]
            del assembly_total[pkt_id]

    mc.subscribe(EventType.CHANNEL_MSG_RECV, on_channel_msg)
    print("Receiver ready. Listening for FRAGEXP: traffic (Ctrl+C to stop)...\n")
    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        await mc.disconnect()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="role", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--port", default="/dev/ttyUSB0")
    common.add_argument("--baud", type=int, default=115200)
    common.add_argument("--channel-idx", type=int, default=35, help="must match the channel both radios are joined to")
    common.add_argument("--channel-name", default="RNSTunnel", help="programmed into the device's channel table at startup (matches this project's default)")
    common.add_argument("--channel-secret", default="95add19b65c179fa0ad562c3756bb338", help="32 hex chars, must match on both ends -- matches this project's shared default secret")

    p_recv = sub.add_parser("receiver", parents=[common])

    p_send = sub.add_parser("sender", parents=[common])
    p_send.add_argument("--fragment-counts", default="1,2,3", help="comma-separated list, e.g. 1,2,3,4")
    p_send.add_argument("--trials", type=int, default=10, help="trials per fragment count")
    p_send.add_argument("--payload-size", type=int, default=64, help="bytes per fragment, matches this project's default")
    p_send.add_argument("--fragment-delay", type=float, default=2.5, help="seconds between fragments, matches this project's default")
    p_send.add_argument("--trial-timeout", type=float, default=90.0, help="seconds to wait for the receiver's completion ack")
    p_send.add_argument("--inter-trial-delay", type=float, default=5.0, help="seconds of quiet between trials")
    p_send.add_argument(
        "--simulate-loss", action="store_true",
        help="deliberately skip one random fragment per trial, then resend just that one after --retry-delay (see --vary-retry)"
    )
    p_send.add_argument("--retry-delay", type=float, default=10.0, help="seconds to wait before resending the dropped fragment")
    p_send.add_argument(
        "--vary-retry", action="store_true",
        help="resend the dropped fragment with attempt=1 (varied bytes) instead of attempt=0 (identical to the original) -- "
             "compare success rate with/without this flag to directly test whether the mesh's flood dedup swallows identical resends"
    )

    args = p.parse_args()
    if args.role == "receiver":
        asyncio.run(run_receiver(args))
    else:
        asyncio.run(run_sender(args))


if __name__ == "__main__":
    main()
