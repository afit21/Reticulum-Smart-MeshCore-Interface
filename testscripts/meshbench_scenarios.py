#!/usr/bin/env python3
"""
meshbench_scenarios.py

Scenario suite for the interface against REAL MeshCore firmware, with no radio
hardware: one real RNS.Reticulum per end node (loading
Interface/SmartMeshCoreInterface.py from a config dir exactly as rnsd does),
the real `meshcore` Python library over TCP, and every simulated node --
companions and repeaters -- running the actual MeshCore firmware compiled
natively by MeshBench (https://meshbench.github.io/), which models the radio
channel and the terrain.

    python3 testscripts/meshbench_scenarios.py list
    python3 testscripts/meshbench_scenarios.py run relay --capture-dir /tmp/mb/relay
    python3 testscripts/meshbench_scenarios.py run failover --probes 12 --fail-after 4

WHAT THIS ISOLATES, AND WHY IT IS A SEPARATE TIER

testscripts/rns_multiprocess_sim.py runs the same RNS/interface stack over
testscripts/simmesh/ -- a *reimplementation* of the firmware (contacts,
DIRECT source-routing, ACKs, path discovery, suggested_timeout, the inbox)
written from reading the firmware source. Every assumption in that model is a
place the interface can be right against the model and wrong against the
device. MeshBench removes that layer: the companion the interface talks to is
`companion_radio` firmware speaking its own serial protocol byte for byte, the
repeaters are `simple_repeater` firmware, airtime is the firmware's own
formula, and flood suppression / CSMA / retransmit delays are the real code.
What stays modelled is the RF (bare-earth terrain from real elevation tiles,
no multipath, no outside interference -- deliberately optimistic, see
MeshBench's own "what it does not do"), so a result here is a check of the
interface's *logic against the real firmware*, never a prediction of delivery
rates over the real Broken Hill repeaters. Compare against fieldtests/raw/
for those.

Two things make a run here worth the minutes it costs over a simmesh run:

  * the firmware's own numbers land in the interface -- suggested_timeout,
    out_path_len, ACK timing, PATH_RESPONSE behaviour, contact handling,
    flood relay delays -- so a divergence between simmesh and firmware shows
    up as a divergence between this suite's result and rns_multiprocess_sim's;
  * MeshBench's engine event log (meshbench_events.jsonl in the capture dir:
    one line per transmission, reception and *missed* reception with the
    reason -- half duplex, demodulator locked, SNR, header decoded then lost)
    is ground truth to read the interface's own packet capture against.

SCENARIOS (see SCENARIOS below, or `list`)

  zero_hop          A and B a kilometre apart, no repeater: DIRECT at 0 hops.
  relay             A - R - B, endpoints hidden from each other: DIRECT at 1 hop.
  two_hop           A - R1 - R2 - B chain, only adjacent links clear: 2 hops.
  failover          A - {R1, R2} - B with tuned relay delays; R1's firmware is
                    stopped mid-run and the path must re-resolve via R2.
  repeater_returns  A - R - B; R dies mid-run and comes back later (the
                    fake_meshcore_repeater_sim --loss-at case with firmware).
  large_payload     relay with full-size probes: multi-fragment raw DIRECT and
                    the completion QUERY/ANSWER reconcile at one hop.
  busy_repeater     relay plus a third companion beside R flooding public
                    channel chatter through it (third-party traffic).
  overlap_default   A - {R1, R2} - B with the repeaters on their compiled
                    default relay delays. Informational: two repeaters that
                    both hear everything relay every flood ~0.5 s apart and
                    their relays collide at the endpoints (observed 2026-09-20:
                    the RNS path request never resolved in 180 s). Reports
                    time-to-path and delivery; asserts only the mechanics.

WHAT A PASS MEANS

Hard checks are the mechanics: the topology gate (MeshBench's link budget
against real terrain says the links that must work do and the ones that must
not do not), the firmware radios match --radio, the interface's resolved
DIRECT path has the scenario's expected hop count (the real firmware's path
discovery answer), the repeaters relayed on air, and the staged event took
effect. Delivery is a measured rate with a floor (--min-delivered, per-scenario
default), not a promise: with hidden endpoints, whenever one transmits while
the repeater is still relaying the other's frame the repeater loses it ("its
own transmitter was keyed"), and on 2026-09-20 that was 62 of ~80 missed
receptions in the relay scenario, hop-1 DIRECT attempts ~50% successful, with
the interface's own completion QUERY/ANSWER traffic and DIRECT announces/path
requests supplying most of the colliding frames -- the mechanism the
2026-09-19 field captures showed at one hop.

REQUIREMENTS

  * MeshBench: the `meshbench` binary on PATH (the "compact" Linux tarball is
    enough -- native firmware needs no emulator; installed 2026-09-20 under
    ~/.local/opt/meshbench, symlinked into ~/.local/bin) and its Python
    client (`pip install --user meshbench`, same release as the binary). The
    script downloads the native companion and repeater builds (~600 KB each,
    from MeshBench/meshcore-native) into ~/.cache/meshbench/firmware when
    missing, and fetches the site's elevation tiles (~1 MB) with
    `meshbench terrain` so links are priced against terrain.
  * The real `meshcore` library and RNS installed in this Python.

CLOCK AND TIMING

  MeshBench's simulated clock is normally the test's to advance; here it
  cannot be, because rnsd and the interface run on wall-clock timeouts. The
  suite runs the sim *playing* at 1x and checks the pacing ratio before
  starting the RNS nodes. The interface runs with its production timing by
  default: simmesh.harness.FAST_TIMING caps the routed ACK timeout at 6 s,
  which is fine against simmesh's constant airtime and wrong against the
  firmware's real one (--fast-timing overrides). Expect 5-15 minutes a run.

MESHBENCH v0.1.0 QUIRKS THIS SCRIPT WORKS AROUND (all observed 2026-09-20)

  * `radio.preset` moves only the channel model; a native build boots on its
    compiled EU default (the repeater answered `get radio` with
    869.62,62.5,8,5), so repeaters get `set radio` typed at their console and
    restarted, and companions get freq/bw/sf/cr through the interface's own
    config, which calls set_radio at connect.
  * `node.radio()` on a *served* node opens the workbench's own companion
    session and takes the TCP endpoint down; nothing touches a served node's
    companion port once rnsd owns it.
  * `firmware.download` wants the application name (`companion_radio`), not
    the client's role word, and an empty board means native.
  * `-seed` needs a fixture; the seed is set on the sim once nodes exist.
  * `link.result` refuses (raises) until the pair worker has started.
  * `sim.inject` is inert while firmware runs.
  * A companion's `reported` radio block is garbage; `assumed` is the model's.
  * `node.start` starts every stopped node, not just the named one; the
    action handler re-stops the ones meant to stay down.
  * Per-node filesystems live under one root keyed by node name, so two
    concurrent runs with nodes named A/B collide; each run gets its own
    root via MESHBENCH_NODEFS (under --capture-dir when given).
  * Miss strings say "needed at SF10" whatever the settings; the threshold
    quoted (-7.5 dB) is SF7's, so it is a label quirk.
  * The engine's "ms on air" runs 1.2-1.45x RadioLib's formula for the
    configured SF7/62.5/CR8 (9 B 188 ms, 37 B 385 ms, 165 B 1270 ms; no
    standard SF/BW/CR reproduces the set), so every collision window and
    ACK latency here is ~30% longer than the field radios', and the
    firmware's own airtime-derived suggested_timeout is correspondingly
    tighter than the channel. Relative comparisons between runs hold;
    absolute latencies are pessimistic. Cause not determined (the native
    processes are launched with the scenario-default --sf/--bw flags and
    then reconfigured by the firmware).

Exit code 0 iff every hard check held.
"""
import argparse
import collections
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from rns_multiprocess_sim import NodeProcess  # noqa: E402
from simmesh.harness import format_summary, read_capture, summarize_capture  # noqa: E402

try:
    from meshbench import Kind, MeshbenchError, Transport, Workbench
except ImportError:  # reported at runtime with instructions
    Workbench = None

# Site: open country north-east of Broken Hill, where this project's field
# tests run. Positions below are km east/north of this origin; the terrain
# under them is MeshBench's real elevation data. Chosen 2026-09-20 by sweeping
# link.pair: with the field radios' settings and 1.5 m companions, A at -8 km
# and B at +8 km with a 50 m mast between them measured A-R +13.8 dB,
# R-B +11.2 dB, A-B -6.2 dB (and 0 direct A<->B receptions in a run).
SITE_LAT = -31.80
SITE_LON = 141.90
# The MeshCore community preset that is exactly the field radios' settings
# (916.575 MHz / BW 62.5 kHz / SF7 / CR 4:8, read back from both Heltecs on
# 2026-09-18, confirmed against MeshBench's model on 2026-09-20), and the
# same as freq_MHz,bw_kHz,sf,cr for the firmware.
DEFAULT_PRESET = "Australia (Narrow)"
DEFAULT_RADIO = "916.575,62.5,7,8"
NATIVE_ROLES = {"companion_radio": "companion-{v}", "simple_repeater": "repeater-{v}"}
HEAD_M = 1.5


# ---------------------------------------------------------------------------
# scenario definitions
# ---------------------------------------------------------------------------

@dataclass
class N:
    name: str
    kind: str          # "companion" | "repeater"
    east_km: float
    north_km: float = 0.0
    height_m: float = HEAD_M
    console: list = field(default_factory=list)   # repeater console lines typed before its restart
    standby: bool = False                          # firmware stopped after configuration, started by an action


@dataclass
class After:
    """A staged action fired once the sender has reported probe `probe`."""
    probe: int
    action: str        # "stop" | "start" | "move"
    node: str
    args: dict = field(default_factory=dict)
    fired_at_probe: Optional[int] = None


@dataclass
class Scenario:
    name: str
    summary: str
    nodes: list
    must_link: list
    must_block: list
    expected_hops: Optional[int]      # None: not asserted
    min_delivered: float
    sender: str = "A"
    responder: str = "B"
    probes: int = 8
    size: int = 32
    actions: Callable = lambda args: []      # args -> [After, ...]
    traffic: Optional[dict] = None           # {"node": name, "every_s": float}: public-channel chatter
    informational: bool = False
    notes: str = ""


def rep(name, east, north=0.0, mast=50.0, console=(), standby=False):
    return N(name, "repeater", east, north, mast, list(console), standby)


def comp(name, east, north=0.0):
    return N(name, "companion", east, north, HEAD_M)


SCENARIOS = {
    "zero_hop": Scenario(
        "zero_hop", "A and B 1 km apart, no repeater: DIRECT at zero hops.",
        nodes=[comp("A", -0.5), comp("B", 0.5)],
        must_link=[("A", "B")], must_block=[], expected_hops=0, min_delivered=0.8,
        notes="The bench case (two radios side by side). Field: hop 0 ~92-96% attempt success, ~1.3 s ACK.",
    ),
    "relay": Scenario(
        "relay", "A - R - B with A and B hidden from each other: DIRECT at one hop.",
        nodes=[comp("A", -8), rep("R", 0), comp("B", 8)],
        must_link=[("A", "R"), ("R", "B")], must_block=[("A", "B")], expected_hops=1, min_delivered=0.4,
        notes="2026-09-20: 2/5 delivered, hop-1 attempts ~50%; losses are half-duplex collisions at R between the hidden endpoints.",
    ),
    "two_hop": Scenario(
        "two_hop", "A - R1 - R2 - B chain; only adjacent links clear: DIRECT at two hops.",
        nodes=[comp("A", -8), rep("R1", 0), rep("R2", 22, mast=30), comp("B", 30)],
        must_link=[("A", "R1"), ("R1", "R2"), ("R2", "B")], must_block=[("A", "R2"), ("R1", "B"), ("A", "B")],
        expected_hops=2, min_delivered=0.3, probes=8,
        notes="Skip links measured -4.5/-5.0 dB (marginal): the run reports any direct skip receptions from the event log. "
              "2026-09-20: PASS 6/8 at 2 hops; firmware suggested_timeout 6.7 s for a 40 B frame at 2 hops.",
    ),
    "failover": Scenario(
        "failover", "A - R1 - B with R2 a cold standby; after --fail-after probes R1's firmware dies and R2's starts. "
                    "The path must be reset and rediscovered through a repeater the interface has never seen.",
        nodes=[comp("A", -8), rep("R1", 0, 1.0), rep("R2", 0, -1.0, standby=True), comp("B", 8)],
        must_link=[("A", "R1"), ("R1", "B"), ("A", "R2"), ("R2", "B")], must_block=[("A", "B")],
        expected_hops=1, min_delivered=0.0, probes=10,
        actions=lambda args: [After(args.fail_after, "stop", "R1"), After(args.fail_after, "start", "R2")],
        notes="R2 is off until R1 dies, deliberately: two repeaters that both hear A and B relay every flood ~0.5 s "
              "apart and the copies collide at the endpoints (see overlap_default), and MeshCore v1.17.1 never "
              "cancels a queued relay on hearing another repeater's copy, so a hot pair never brings up at all "
              "(2026-09-20, with txdelay 0.2/1.5 too). The staged swap isolates the interface's stale-path reset "
              "and rediscovery, which is the thing under test. 2026-09-20: PASS, path re-resolved via R2 and probe 5 "
              "delivered 78 s after the swap.",
    ),
    "repeater_returns": Scenario(
        "repeater_returns", "A - R - B; R's firmware dies after --fail-after probes and returns --outage-probes later.",
        nodes=[comp("A", -8), rep("R", 0), comp("B", 8)],
        must_link=[("A", "R"), ("R", "B")], must_block=[("A", "B")], expected_hops=1, min_delivered=0.0, probes=12,
        actions=lambda args: [After(args.fail_after, "stop", "R"), After(args.fail_after + args.outage_probes, "start", "R")],
        notes="fake_meshcore_repeater_sim's --loss-at '60:A>R=1.0' case: a hop that dies and comes back, with the firmware deciding. "
              "2026-09-20: PASS 7/12, stale path reset 57 s after R died, rediscovered 6 s after R's restart, 5/6 delivered after.",
    ),
    "large_payload": Scenario(
        "large_payload", "relay topology with full-size probes: raw multi-fragment DIRECT and the completion reconcile at one hop.",
        nodes=[comp("A", -8), rep("R", 0), comp("B", 8)],
        must_link=[("A", "R"), ("R", "B")], must_block=[("A", "B")], expected_hops=1, min_delivered=0.2, probes=6, size=383,
        notes="383 B is RNS.Packet.ENCRYPTED_MDU, the most one SINGLE-destination packet carries (900 B was refused by "
              "RNS.Packet.pack as 1011 B > MTU 500 on 2026-09-20): a 483 B packed packet = 4 raw DIRECT fragments "
              "(3 x 170 B + 25 B), the NomadNet page-transfer class of traffic from the 2026-09-19 field regression.",
    ),
    "busy_repeater": Scenario(
        "busy_repeater", "relay topology plus companion C beside R sending public-channel messages every --traffic-interval s.",
        nodes=[comp("A", -8), rep("R", 0), comp("B", 8), comp("C", 1.0, 1.0)],
        must_link=[("A", "R"), ("R", "B"), ("C", "R")], must_block=[("A", "B")], expected_hops=1, min_delivered=0.2, probes=8,
        traffic={"node": "C", "every_s": None},
        notes="Third-party flood traffic through the same repeater; the field mesh's is <1% of channel time, this is heavier on purpose.",
    ),
    "overlap_default": Scenario(
        "overlap_default", "A - {R1, R2} - B with the repeaters on their compiled default relay delays (informational).",
        nodes=[comp("A", -8), rep("R1", 0, 1.0), rep("R2", 0, -1.0), comp("B", 8)],
        must_link=[("A", "R1"), ("R1", "B"), ("A", "R2"), ("R2", "B")], must_block=[("A", "B")],
        expected_hops=None, min_delivered=0.0, probes=6, informational=True,
        notes="Default flood relay delay is random(0, 2.5 x airtime) per repeater, so two repeaters hearing the same frame "
              "collide at the endpoints most of the time. 2026-09-20: RNS path request never resolved in 180 s.",
    ),
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def offset(east_km: float, north_km: float) -> tuple:
    lat = SITE_LAT + north_km / 111.32
    lon = SITE_LON + east_km / (111.32 * math.cos(math.radians(SITE_LAT)))
    return lat, lon


def prefetch_terrain(args, scenario: Scenario) -> None:
    """Elevation tiles covering every node plus a margin (~1 MB, cached under
    ~/.cache/meshbench/terrain), so links are priced against terrain rather
    than free space from the first link.pair."""
    es = [n.east_km for n in scenario.nodes]
    ns = [n.north_km for n in scenario.nodes]
    s_lat, w_lon = offset(min(es) - 4, min(ns) - 4)
    n_lat, e_lon = offset(max(es) + 4, max(ns) + 4)
    cmd = [args.meshbench_binary or "meshbench", "terrain", "-north", f"{n_lat:.3f}", "-south", f"{s_lat:.3f}",
           "-west", f"{w_lon:.3f}", "-east", f"{e_lon:.3f}"]
    try:
        subprocess.run(cmd, check=False, timeout=300, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except (OSError, subprocess.TimeoutExpired) as e:
        log(f"terrain prefetch skipped: {e}")


def ensure_native_firmware(wb, version: str, wait_s: float) -> None:
    """Download the native companion and repeater builds if the library lacks
    them. The verb wants the application name and a per-role tag; an empty
    board means native."""
    have = {(b["role"], b["version"]) for b in wb.call("firmware.library", {})["builds"] if b.get("on_disk")}
    started = []
    for role, tag in NATIVE_ROLES.items():
        tag = tag.format(v=version)
        if (role, tag) in have:
            continue
        answer = wb.call("firmware.download", {"role": role, "version": tag})
        started.append(answer.get("job", f"fw-{tag}-{role}"))
        log(f"downloading native {role} {tag} ...")
    deadline = time.monotonic() + wait_s
    while started and time.monotonic() < deadline:
        jobs = {j["id"]: j for j in wb.call("job.list", {}).get("jobs", [])}
        if any(j in jobs and jobs[j]["failed"] for j in started):
            sys.exit(f"firmware download failed: {started}")
        if not any(j in jobs and not jobs[j]["finished"] for j in started):
            break
        time.sleep(2)


def measure_link(wb, a: str, b: str, timeout_s: float) -> dict:
    """link.pair answers at once; the margins land later in link.result,
    which refuses until the worker has started."""
    wb.call("link.pair", {"a": a, "b": b})
    deadline = time.monotonic() + timeout_s
    last = {}
    while time.monotonic() < deadline:
        try:
            last = wb.call("link.result", {}) or {}
        except MeshbenchError:
            time.sleep(0.5)
            continue
        if {last.get("from"), last.get("to")} == {a, b} and last.get("a_to_b_db") is not None:
            return last
        time.sleep(0.5)
    return last


def topology_gate(wb, scenario: Scenario, args) -> tuple:
    """Measure every must-link and must-block pair. A must-link pair needs
    both directions above --link-margin; a must-block pair needs both below
    --block-margin (the budget is a best case; the run also reports what the
    channel actually did)."""
    rows, ok = [], True
    for a, b in scenario.must_link + scenario.must_block:
        res = measure_link(wb, a, b, args.link_timeout)
        ab, ba = res.get("a_to_b_db"), res.get("b_to_a_db")
        want = "link" if (a, b) in scenario.must_link else "block"
        good = ab is not None and ba is not None and (
            min(ab, ba) >= args.link_margin if want == "link" else max(ab, ba) <= args.block_margin)
        ok = ok and good
        rows.append((a, b, want, res.get("km"), ab, ba, "ok" if good else "FAIL", (res.get("verdict") or "")[:60]))
    log("link budget (MeshBench, real terrain, bare earth, best case):")
    print(f"  {'from':>3} {'to':>3} {'want':>5} {'km':>6} {'a->b dB':>8} {'b->a dB':>8}  gate  verdict")
    for a, b, want, km, ab, ba, verdict, note in rows:
        f = lambda v, fmt: (fmt % v) if isinstance(v, (int, float)) else "?"
        print(f"  {a:>3} {b:>3} {want:>5} {f(km, '%6.1f'):>6} {f(ab, '%+.1f'):>8} {f(ba, '%+.1f'):>8}  {verdict:<4}  {note}")
    return rows, ok


def pacing_ratio(wb, seconds: float) -> float:
    t0, s0 = time.monotonic(), wb.sim.now_ms
    time.sleep(seconds)
    return ((wb.sim.now_ms - s0) / 1000.0) / max(time.monotonic() - t0, 1e-6)


def console_lines(wb, node: str, command: str, settle_s: float = 2.0) -> list:
    wb.call("console.type", {"node": node, "command": command})
    time.sleep(settle_s)
    return wb.call("console.read", {"node": node}).get("tail", [])


def configure_repeater(wb, node, radio: str, extra: list) -> str:
    """Type `set radio F,B,SF,CR` plus the scenario's own lines at the
    repeater console, restart its firmware so saved prefs take effect, and
    return what `get radio` then says."""
    if radio:
        console_lines(wb, node.name, f"set radio {radio}")
    for line in extra:
        console_lines(wb, node.name, line, settle_s=1.0)
    node.stop()
    time.sleep(1.0)
    node.start()
    node.wait_running(timedelta(seconds=60))
    time.sleep(2.0)
    for line in reversed(console_lines(wb, node.name, "get radio")):
        if "->" in line and "," in line:
            return line.split("->", 1)[1].strip().lstrip("> ").strip()
    return "?"


def radio_matches(reported: str, wanted: str) -> bool:
    try:
        r = [float(x) for x in reported.split(",")]
        w = [float(x) for x in wanted.split(",")]
    except ValueError:
        return False
    return len(r) == 4 and len(w) == 4 and abs(r[0] - w[0]) < 0.01 and abs(r[1] - w[1]) < 0.1 and r[2:] == w[2:]


def endpoint_host_port(addr: str) -> tuple:
    host, port = addr.rsplit(":", 1)
    if host in ("", "0.0.0.0", "[::]", "::"):
        host = "127.0.0.1"
    return host, int(port)


def stats_by_name(wb) -> dict:
    return {(st if isinstance(st, dict) else vars(st)).get("name"): (st if isinstance(st, dict) else vars(st))
            for st in wb.nodes.stats()}


def events_analysis(events_path: str, companions: list, repeaters: list) -> dict:
    """What the channel actually did, from MeshBench's event dump: per-pair
    direct receptions between companions (should be 0 where the scenario
    says blocked), tx counts, and missed receptions by cause."""
    out = {"direct": collections.Counter(), "tx": collections.Counter(), "miss": collections.Counter(), "events": 0}
    try:
        with open(events_path) as f:
            for line in f:
                e = json.loads(line)
                out["events"] += 1
                k = e.get("kind")
                if k == "tx":
                    out["tx"][e.get("from")] += 1
                elif k in ("rx", "miss"):
                    if e.get("from") in companions and e.get("to") in companions:
                        out["direct"][f"{e['from']}->{e['to']}" + ("" if k == "rx" else " miss")] += 1
                    if k == "miss":
                        d = e.get("detail", "")
                        cause = ("half-duplex at receiver" if "own transmitter" in d else
                                 "demodulator locked to another frame" if "locked" in d else
                                 "header decoded, then lost (collision)" if "decoded its header" in d else
                                 "SNR below threshold" if "SNR" in d else d[:40])
                        out["miss"][(e.get("to"), cause)] += 1
    except OSError:
        pass
    return out


# ---------------------------------------------------------------------------
# rns node subprocesses (rns_multiprocess_sim.py node --backend real)
# ---------------------------------------------------------------------------

def node_argv(args, name: str, endpoint: str, role: str, extra: list) -> list:
    argv = [sys.executable, os.path.join(HERE, "rns_multiprocess_sim.py"), "node",
            "--backend", "real", "--name", name, "--server", endpoint, "--role", role,
            "--loglevel", str(args.loglevel), "--advert-interval", str(args.advert_interval)]
    if not args.fast_timing:
        argv.append("--production-timing")
    if args.capture_dir:
        argv += ["--capture-dir", args.capture_dir]
    for opt in args.iface_option:
        argv += ["--iface-option", opt]
    return argv + extra


def probe_events(sender: NodeProcess) -> list:
    with sender._lock:
        return [e for e in sender.events if e.get("event") == "probe"]


def first_event(proc: NodeProcess, name: str):
    with proc._lock:
        return next((e for e in proc.events if e.get("event") == name), None)


# ---------------------------------------------------------------------------
# the run
# ---------------------------------------------------------------------------

def run_scenario(scenario: Scenario, args) -> int:
    if Workbench is None:
        sys.exit("the meshbench Python client is not installed: pip install --user meshbench "
                 "(and put the meshbench binary on PATH) -- see https://meshbench.github.io/docs/scripting.html")
    if not args.meshbench_binary and shutil.which("meshbench") is None:
        sys.exit("no `meshbench` binary on PATH; pass --meshbench-binary /path/to/meshbench")

    probes = args.probes or scenario.probes
    size = args.size or scenario.size
    capture_dir = args.capture_dir
    if capture_dir:
        os.makedirs(capture_dir, exist_ok=True)
    events_out = args.events_out or (os.path.join(capture_dir, "meshbench_events.jsonl") if capture_dir else None)
    if args.radio and not any(o.split("=", 1)[0] in ("freq", "bw", "sf", "cr") for o in args.iface_option):
        freq, bw, sf, cr = args.radio.split(",")
        args.iface_option = [f"freq={freq}", f"bw={bw}", f"sf={sf}", f"cr={cr}"] + args.iface_option

    companions = [n.name for n in scenario.nodes if n.kind == "companion"]
    repeaters = [n.name for n in scenario.nodes if n.kind == "repeater"]
    failures, measurements = [], {}

    def check(condition: bool, what: str) -> None:
        log(("ok    " if condition else "FAIL  ") + what)
        if not condition:
            failures.append(what)

    log(f"scenario {scenario.name}: {scenario.summary}")
    if scenario.notes:
        log(f"  note: {scenario.notes}")
    # MeshBench keys each node's persistent filesystem (identity, prefs,
    # contacts) by node NAME under one shared root, and refuses to start a
    # node whose directory another process holds ("nodefs/A is already in
    # use by another node process", seen 2026-09-20 with two scenarios
    # running at once). A private root per run keeps concurrent runs apart
    # and also starts every node factory-fresh, so no saved preference from
    # an earlier run can shadow this run's `set radio`.
    os.environ.setdefault("MESHBENCH_NODEFS",
                          os.path.join(capture_dir, "nodefs") if capture_dir else tempfile.mkdtemp(prefix="meshbench-nodefs-"))
    log(f"node filesystems under {os.environ['MESHBENCH_NODEFS']}")
    prefetch_terrain(args, scenario)
    procs = []
    exit_code = 2
    with Workbench.headless(fixture="", binary=args.meshbench_binary) as wb:
        wb.project.new()
        ensure_native_firmware(wb, args.firmware_version, wait_s=300)
        nodes = {}
        for n in scenario.nodes:
            lat, lon = offset(n.east_km, n.north_km)
            kind = Kind.COMPANION if n.kind == "companion" else Kind.SIMPLE_REPEATER
            nodes[n.name] = wb.nodes.place(n.name, kind, lat, lon, height_m=n.height_m)
            log(f"placed {n.name:<2} {n.kind:<9} at ({n.east_km:+.1f} km E, {n.north_km:+.1f} km N) h={n.height_m} m")
        wb.sim.seed = args.seed
        pinned = wb.firmware.use_what_is_here()
        log(f"firmware: { {str(k): getattr(v, 'version', v) for k, v in pinned.items()} } seed={wb.sim.seed}")
        if args.preset:
            wb.call("radio.preset", {"preset": args.preset})
            log(f"channel model on preset {args.preset!r}")

        rows, topo_ok = topology_gate(wb, scenario, args)
        check(topo_ok, "topology gate: must-link pairs clear, must-block pairs blocked (link budget)")
        if not topo_ok and not args.ignore_topology:
            log("stopping: the scenario would not be testing what it says (--ignore-topology to run anyway)")
            return 3

        log("starting firmware on every node and playing the sim ...")
        wb.sim.start()
        wb.firmware.wait_started(timedelta(seconds=args.firmware_wait))
        for n in scenario.nodes:
            if n.kind == "repeater":
                got = configure_repeater(wb, nodes[n.name], args.radio, n.console)
                check(not args.radio or radio_matches(got, args.radio), f"{n.name} firmware radio {got} matches {args.radio}"
                      + (f"; console: {n.console}" if n.console else ""))
                if n.standby:
                    nodes[n.name].stop()
                    log(f"{n.name} configured and stopped (standby until a staged action starts it)")
        ratio = pacing_ratio(wb, 4.0)
        measurements["sim_pacing"] = round(ratio, 3)
        log(f"sim pacing {ratio:.2f} simulated s per wall s")
        if not 0.8 <= ratio <= 1.25:
            log("WARNING: wall-clock clients need ~1.0; interface timeouts are off by that ratio")

        endpoints = {}
        for name in (scenario.responder, scenario.sender):
            endpoints[name] = nodes[name].serve(Transport.TCP)
            log(f"{name} companion served at tcp {endpoints[name]}")
        stats_before = stats_by_name(wb)

        try:
            host, port = endpoint_host_port(endpoints[scenario.responder])
            responder = NodeProcess(scenario.responder, node_argv(args, scenario.responder, f"{host}:{port}", "responder",
                                                                  ["--announce-interval", str(args.announce_interval)]),
                                    echo=not args.quiet_nodes)
            procs.append(responder)
            ready = responder.wait_event("ready", timeout=args.rns_start_timeout)
            if ready is None:
                log(f"responder never became ready (exit {responder.proc.returncode}); see its output above")
                return 2
            time.sleep(args.settle)

            host, port = endpoint_host_port(endpoints[scenario.sender])
            sender = NodeProcess(scenario.sender, node_argv(args, scenario.sender, f"{host}:{port}", "sender", [
                "--dest", ready["dest"], "--probes", str(probes), "--wait", str(args.wait),
                "--size", str(size), "--path-timeout", str(args.path_timeout),
                "--path-request-interval", str(args.path_request_interval),
                "--probe-timeout", str(args.probe_timeout),
            ]), echo=not args.quiet_nodes)
            procs.append(sender)

            actions = scenario.actions(args)
            traffic_every = args.traffic_interval if scenario.traffic else None
            traffic_node = scenario.traffic["node"] if scenario.traffic else None
            next_traffic, traffic_sent = time.monotonic() + 5.0, 0
            stats_at = {}
            deadline = time.monotonic() + args.path_timeout + probes * (args.wait + args.probe_timeout) + 60
            done = None
            while time.monotonic() < deadline:
                done = first_event(sender, "done")
                if done or sender.proc.poll() is not None:
                    break
                seqs = {p.get("seq") for p in probe_events(sender)}
                for act in actions:
                    if act.fired_at_probe is None and act.probe in seqs:
                        act.fired_at_probe = act.probe
                        stats_at[act.probe] = stats_by_name(wb)
                        log(f"probe {act.probe} reported; staged action: {act.action} {act.node} (sim t={wb.sim.now_ms / 1000:.1f}s)")
                        node = nodes[act.node]
                        if act.action == "stop":
                            node.stop()
                        elif act.action == "start":
                            # node.start "goes through the whole-mesh attach and
                            # so starts every other stopped node with it"
                            # (MeshBench reference-control): re-stop anything
                            # that was meant to stay down.
                            node.start()
                            node.wait_running(timedelta(seconds=60))
                            time.sleep(1.0)
                            for other in nodes.values():
                                keep_down = other.name != act.node and (
                                    any(x.action == "stop" and x.node == other.name and x.fired_at_probe is not None
                                        and not any(y.action == "start" and y.node == other.name and y.fired_at_probe is not None for y in actions)
                                        for x in actions)
                                    or any(n.name == other.name and n.standby for n in scenario.nodes)
                                    and not any(y.action == "start" and y.node == other.name and y.fired_at_probe is not None for y in actions))
                                if keep_down and a_running(stats_by_name(wb), other.name):
                                    other.stop()
                                    log(f"{other.name} re-stopped (node.start brings every stopped node up)")
                        elif act.action == "move":
                            node.move(*offset(act.args["east_km"], act.args.get("north_km", 0.0)))
                if traffic_every and time.monotonic() >= next_traffic:
                    try:
                        wb.call("console.cli", {"node": traffic_node, "command": f"public chatter {traffic_sent} from {traffic_node}"})
                        traffic_sent += 1
                    except MeshbenchError as e:
                        log(f"background traffic send failed: {e}")
                    next_traffic = time.monotonic() + traffic_every
                time.sleep(0.5)
            if done is None:
                done = first_event(sender, "done")
            print("")
            if done is None:
                log("=== Result: sender never reported completion ===")
                return 2

            online = first_event(sender, "iface_online")
            resolved_ev = first_event(sender, "path_resolved")
            if online and resolved_ev:
                measurements["rns_path_time_s"] = round(resolved_ev["_t"] - online["_t"], 1)
            rtts = done.get("rtts", [])
            rtt_str = f"min={min(rtts):.2f}s avg={sum(rtts)/len(rtts):.2f}s max={max(rtts):.2f}s" if rtts else "n/a"
            log(f"=== Result: {done.get('delivered')}/{done.get('sent')} probe(s) delivered, RTT {rtt_str}; "
                f"RNS path in {measurements.get('rns_path_time_s', 'never')} s ===")
            if done.get("reason"):
                log(f"    reason: {done['reason']}")
            plist = probe_events(sender)
            log("    per probe: " + " ".join(
                f"#{p['seq']}{'+' if p.get('delivered') else '-'}" + (f"[{','.join(str(h) for h in p.get('resolved', {}).values())}]" if p.get("resolved") else "")
                for p in plist))
            stats_after = stats_by_name(wb)
            for name in repeaters:
                b, a = stats_before.get(name, {}), stats_after.get(name, {})
                log(f"    {name} (repeater firmware): sent {b.get('sent')} -> {a.get('sent')}, running={a.get('running')}")
            if traffic_sent:
                log(f"    background traffic: {traffic_sent} public-channel messages from {traffic_node}")
            measurements.update(sent=done.get("sent", 0), delivered=done.get("delivered", 0), rtts=rtts,
                                resolved=done.get("resolved", {}), background_msgs=traffic_sent)

            # ---- checks --------------------------------------------------
            sent, delivered = done.get("sent", 0), done.get("delivered", 0)
            resolved_final = done.get("resolved", {})
            min_delivered = args.min_delivered if args.min_delivered is not None else scenario.min_delivered
            if scenario.informational:
                log(f"info  delivered {delivered}/{sent}; resolved paths {resolved_final}; RNS path time {measurements.get('rns_path_time_s')}")
            else:
                check(sent > 0 and delivered >= math.ceil(min_delivered * sent),
                      f"delivered {delivered}/{sent} >= {min_delivered:.0%} floor")
                if scenario.expected_hops is not None:
                    check(bool(resolved_final) and all(h == scenario.expected_hops for h in resolved_final.values()),
                          f"interface's resolved DIRECT path is {scenario.expected_hops} hop(s) (firmware path discovery): {resolved_final}")
            for name in repeaters:
                stopped_forever = any(a.action == "stop" and a.node == name and not any(
                    b.action == "start" and b.node == name for b in actions) for a in actions)
                b, a = stats_before.get(name, {}).get("sent") or 0, stats_after.get(name, {}).get("sent") or 0
                standby = any(n.name == name and n.standby for n in scenario.nodes)
                if not standby or any(x.action == "start" and x.node == name and x.fired_at_probe is not None for x in actions):
                    check(a > b, f"{name} relayed on air (sent {b} -> {a})")
                if stopped_forever:
                    check(not a_running(stats_after, name), f"{name} firmware stayed stopped")
            for act in actions:
                check(act.fired_at_probe is not None, f"staged action {act.action} {act.node} after probe {act.probe} was reached")
            if actions:
                last_stop = max((a.probe for a in actions if a.action == "stop"), default=None)
                last_start = max((a.probe for a in actions if a.action == "start"), default=None)
                boundary = last_start if last_start is not None else last_stop
                if boundary is not None:
                    after = [p for p in plist if (p.get("seq") or 0) > boundary]
                    got = [p.get("seq") for p in after if p.get("delivered")]
                    phase = "after the repeater came back" if last_start is not None else "after the failure"
                    check(len(got) >= args.recover_probes,
                          f"probes delivered {phase}: {got} of {[p.get('seq') for p in after]} (need >= {args.recover_probes})")
                    for act in actions:
                        if act.action == "start" and act.fired_at_probe is not None:
                            before_start = stats_at.get(act.probe, {}).get(act.node, {}).get("sent") or 0
                            after_start = stats_after.get(act.node, {}).get("sent") or 0
                            check(after_start > before_start, f"{act.node} relayed after it was started (sent {before_start} -> {after_start})")
                            check(a_running(stats_after, act.node), f"{act.node} firmware is running at the end")
            exit_code = 0 if not failures else 2
        finally:
            for p in procs:
                p.stop()
            if events_out:
                try:
                    n = wb.events.dump(events_out)
                    an = events_analysis(events_out, companions, repeaters)
                    log(f"meshbench engine events: {n} written to {events_out}")
                    log(f"    on-air tx per node: {dict(an['tx'])}")
                    log(f"    direct companion<->companion receptions: {dict(an['direct']) or 'none'}")
                    top = sorted(an["miss"].items(), key=lambda kv: -kv[1])[:8]
                    log("    missed receptions by (receiver, cause): " + "; ".join(f"{k[0]} {k[1]}: {v}" for k, v in top))
                    measurements["direct_receptions"] = dict(an["direct"])
                    measurements["miss_causes"] = {f"{k[0]}|{k[1]}": v for k, v in an["miss"].items()}
                except Exception as e:  # the run is over; a dump failure is not a test failure
                    log(f"events.dump failed: {e}")
            for name in (scenario.responder, scenario.sender):
                try:
                    nodes[name].unserve()
                except Exception:
                    pass

    if capture_dir:
        for name in (scenario.sender, scenario.responder):
            records = read_capture(capture_dir, name)
            if records:
                summary = summarize_capture(records)
                measurements[f"capture_{name}"] = {
                    "direct_attempts_ok": summary["direct_attempts_ok"], "direct_attempts_failed": summary["direct_attempts_failed"],
                    "by_hop": {str(k): v for k, v in summary["direct_attempts_by_hop"].items()},
                    "ack_latency_s": summary["ack_latency_s"], "completion_checks": dict(summary["completion_checks"]),
                    "routing_decisions": dict(summary["routing_decisions"]),
                }
                print(f"\n--- interface packet capture, node {name} ({len(records)} records) ---")
                print(format_summary(summary))
        with open(os.path.join(capture_dir, "result.json"), "w") as f:
            json.dump({"scenario": scenario.name, "informational": scenario.informational, "failures": failures,
                       "measurements": measurements, "args": {k: v for k, v in vars(args).items() if k != "func"}}, f, indent=1, default=str)
        log(f"result.json written to {capture_dir}")
    if failures:
        print("\nFAILED:\n  " + "\n  ".join(failures))
    return exit_code


def a_running(stats: dict, name: str) -> bool:
    return bool(stats.get(name, {}).get("running"))


# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("list", help="Print the scenarios")
    run = sub.add_parser("run", help="Run one scenario")
    run.add_argument("scenario", choices=sorted(SCENARIOS))
    # meshbench
    run.add_argument("--meshbench-binary", default=None)
    run.add_argument("--seed", type=int, default=7, help="MeshBench run seed (identities, noise)")
    run.add_argument("--preset", default=DEFAULT_PRESET, help="Channel-model preset; '' leaves MeshBench's own")
    run.add_argument("--radio", default=DEFAULT_RADIO, metavar="MHZ,KHZ,SF,CR",
                     help="Firmware radio settings: typed at repeater consoles, passed to the interface as freq/bw/sf/cr; '' leaves compiled defaults")
    run.add_argument("--firmware-version", default="v1.17.1", help="Native build tag suffix to download when missing")
    run.add_argument("--firmware-wait", type=float, default=300.0)
    run.add_argument("--link-timeout", type=float, default=90.0)
    run.add_argument("--link-margin", type=float, default=6.0, help="dB a must-link pair needs in both directions")
    run.add_argument("--block-margin", type=float, default=-4.0, help="dB a must-block pair must stay under in both directions")
    run.add_argument("--ignore-topology", action="store_true", help="Run even if the topology gate fails")
    run.add_argument("--events-out", default=None)
    # rns nodes
    run.add_argument("--probes", type=int, default=None, help="Override the scenario's probe count")
    run.add_argument("--size", type=int, default=None, help="Override the scenario's probe payload size (max 383, RNS's single-packet MDU)")
    run.add_argument("--wait", type=float, default=5.0, help="Seconds between probes")
    run.add_argument("--fast-timing", action="store_true", help="Interface on simmesh's FAST_TIMING instead of production timing")
    run.add_argument("--iface-option", action="append", default=[], metavar="KEY=VALUE")
    run.add_argument("--capture-dir", default=None, help="Interface packet capture per node, MeshBench event dump, result.json")
    run.add_argument("--announce-interval", type=float, default=30.0)
    run.add_argument("--advert-interval", type=float, default=60.0, help="Companion re-advert cadence (real firmware never re-adverts by itself)")
    run.add_argument("--settle", type=float, default=15.0)
    run.add_argument("--rns-start-timeout", type=float, default=120.0)
    run.add_argument("--path-timeout", type=float, default=420.0,
                     help="Seconds the sender waits for an RNS path. Bring-up through one repeater took 53-159 s in passing "
                          "runs and timed out at 180 s in others: B's PATH_RESPONSE is two CHANNEL fragments that must both "
                          "survive the repeater, under A's own request traffic")
    run.add_argument("--path-request-interval", type=float, default=40.0,
                     help="Seconds between the sender's RNS path requests while unresolved (each one is more traffic for "
                          "B's announce fragments to collide with at the repeater)")
    run.add_argument("--probe-timeout", type=float, default=60.0)
    run.add_argument("--min-delivered", type=float, default=None, help="Override the scenario's delivery floor (fraction)")
    run.add_argument("--fail-after", type=int, default=3, help="failover/repeater_returns: stage the failure after this probe")
    run.add_argument("--outage-probes", type=int, default=3, help="repeater_returns: probes between the repeater dying and returning")
    run.add_argument("--recover-probes", type=int, default=1, help="At least N probes after the staged event must be delivered")
    run.add_argument("--traffic-interval", type=float, default=8.0, help="busy_repeater: seconds between C's public-channel messages")
    run.add_argument("--loglevel", type=int, default=4)
    run.add_argument("--quiet-nodes", action="store_true")
    args = parser.parse_args()

    if args.mode == "list":
        for name, sc in SCENARIOS.items():
            print(f"{name:<18} {sc.summary}")
            print(f"{'':<18} nodes: {', '.join(f'{n.name}({n.kind[0]},{n.east_km:+g}E,{n.north_km:+g}N,{n.height_m:g}m)' for n in sc.nodes)}")
            print(f"{'':<18} expects {sc.expected_hops if sc.expected_hops is not None else 'n/a'} hop(s), delivery floor {sc.min_delivered:.0%}, "
                  f"{sc.probes} probes of {sc.size} B{'; informational' if sc.informational else ''}")
            if sc.notes:
                print(f"{'':<18} {sc.notes}")
        return
    scenario = SCENARIOS[args.scenario]
    if (args.size or scenario.size) > 383:
        parser.error("--size above 383 exceeds RNS.Packet.ENCRYPTED_MDU; the sender's RNS.Packet.pack() would refuse it")
    probes = args.probes or scenario.probes
    acts = scenario.actions(args)
    if acts and max(a.probe for a in acts) + args.recover_probes > probes:
        parser.error(f"staged actions reach probe {max(a.probe for a in acts)} and need {args.recover_probes} more; raise --probes")
    sys.exit(run_scenario(scenario, args))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
