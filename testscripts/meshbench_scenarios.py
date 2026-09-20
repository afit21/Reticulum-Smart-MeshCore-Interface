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
    python3 testscripts/meshbench_scenarios.py run page_transfer_bidir --capture-dir /tmp/mb/ptb
    python3 testscripts/meshbench_scenarios.py suite --scenarios zero_hop,relay,large_payload,page_transfer \
        --seeds 7,11,13 --parallel 2 --out-dir /tmp/mb/suite-$(git rev-parse --short HEAD) \
        --write-baseline tests/baselines/$(date +%F)-meshbench-$(git rev-parse --short HEAD).md
    python3 testscripts/meshbench_scenarios.py report --aggregate /tmp/mb/suite-*/relay-*

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

  Added 2026-09-20 (evening) for the coverage gaps the day's comparison
  work exposed -- the field workload was not represented:

  page_transfer     relay topology; each probe is one real RNS.Resource of
                    ~12 parts over an RNS Link (the NomadNet page fetch).
  page_transfer_bidir  the same with the responder pushing its own page back
                    on the Link at once (the 2026-09-19 night geometry).
  duty_cycle_pages  zero_hop topology, pages back to back: the duty-cycle
                    limiter under load.
  link_setup        relay topology; each probe is one Link handshake, counted
                    against MeshChat's 15 s window.
  link_setup_two_hop  the same at two hops.
  bring_up          two_hop topology, no probes: time-to-DIRECT-path per end.
  three_hop         A - R1 - R2 - R3 - B.
  many_peers        five companions, each an RNS node: >3 bound peers, so
                    small-mesh mode is off and the CHANNEL/supplement paths run.
  mixed_builds      responder on an older build (git:d7dcba9 by default).
  companion_restart the sender's companion firmware rebooted mid-run (informational).
  soak              --duration (30 min default) with health snapshots.

  Hop-count scenarios (two_hop, three_hop, link_setup_two_hop, bring_up)
  hold the sender's traffic until BOTH ends have a DIRECT path (the start
  gate, --gate-timeout), so they measure their hops and not bring-up.
  Every run reports late deliveries (a PROOF after the probe timeout) and
  the RTT distribution separately, embeds meshbench_report.py's analysis in
  result.json, and the `suite` subcommand runs scenarios x seeds and writes
  the medians-and-ranges summary a baseline file is made of.

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
  * Seeded identities ignore MeshCore's reserved first bytes: seed 13 gives
    node A a public key starting 0x00, which the firmware itself never
    generates (path hash 0x00/0xFF is reserved) -- path discovery to that
    node never answers and no DIRECT frame is ever exchanged. Every run now
    checks each node's _main.id after the firmware starts and stops (exit 3)
    on a reserved prefix, like the topology gate.
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
import glob
import json
import math
import os
import re
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

import meshbench_report  # noqa: E402
from rns_multiprocess_sim import NodeProcess  # noqa: E402
from simmesh.harness import INTERFACE_PATH, format_summary, read_capture, summarize_capture  # noqa: E402

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
    action: str        # "stop" | "start" | "restart" | "move"
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
    # --- added 2026-09-20 (coverage gaps: the field workload was not represented) ---
    unit: str = "probe"                      # what one "probe" is: "probe" (DATA packet) | "link" (handshake) | "resource" (page)
    resource_size: int = 5100                # unit="resource": random bytes per Resource (5100 B = 12 parts at the Link MDU)
    respond_resource_size: int = 0           # responder pushes a Resource of this size back on every Link (bidirectional load)
    respond_resources: int = 1
    start_after_paths: bool = False          # hold the sender's traffic until BOTH ends have a DIRECT path (two_hop, bring_up)
    responder_interface: Optional[str] = None   # path or "git:<rev>": the responder runs that build (mixed_builds)
    extra_rns_nodes: list = field(default_factory=list)   # companions that also run an RNS responder (many_peers)
    min_bound_peers: Optional[int] = None    # hard check: the sender's capture shows at least this many bound peers
    duration_s: float = 0.0                  # unit loop runs for this long instead of a fixed count (soak)
    health_interval_s: float = 0.0           # nodes emit health events this often (soak)


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
        expected_hops=2, min_delivered=0.3, probes=8, start_after_paths=True,
        notes="Skip links measured -4.5/-5.0 dB (marginal): the run reports any direct skip receptions from the event log. "
              "2026-09-20: PASS 6/8 at 2 hops; firmware suggested_timeout 6.7 s for a 40 B frame at 2 hops. Since 2026-09-20 "
              "the probes start only once BOTH ends have a DIRECT path (--gate-timeout), so this measures the two-hop DIRECT "
              "path; bring-up itself is the bring_up scenario.",
    ),
    "bring_up": Scenario(
        "bring_up", "two_hop topology, no probes: time from online to a DIRECT path at each end, adverts and path requests it took.",
        nodes=[comp("A", -8), rep("R1", 0), rep("R2", 22, mast=30), comp("B", 30)],
        must_link=[("A", "R1"), ("R1", "R2"), ("R2", "B")], must_block=[("A", "R2"), ("R1", "B"), ("A", "B")],
        expected_hops=None, min_delivered=0.0, probes=0, start_after_paths=True, informational=True,
        notes="2026-09-20: two_hop's DIRECT path resolved at 140-540 s or never in most runs, so that scenario mostly measured "
              "the advert coin flip. This one measures it on purpose: reports time_to_direct_path per node, path_requested "
              "count, and the sender's RNS path time; asserts only the mechanics.",
    ),
    "three_hop": Scenario(
        "three_hop", "A - R1 - R2 - R3 - B chain, only adjacent links clear: DIRECT at three hops (the field sees them).",
        nodes=[comp("A", -8), rep("R1", 0), rep("R2", 22, mast=30), rep("R3", 26, 16, mast=15), comp("B", 20.3, 21.7)],
        must_link=[("A", "R1"), ("R1", "R2"), ("R2", "R3"), ("R3", "B")],
        must_block=[("A", "R2"), ("R1", "R3"), ("R2", "B"), ("R1", "B"), ("A", "B")],
        expected_hops=3, min_delivered=0.2, probes=8, start_after_paths=True,
        notes="Field 2026-09-19: hop 3 ~42% attempt success, ~5.6 s ACK. Hop-scaled values (ACK cap 5+3h = 14 s, answer hold, "
              "raw gap) are what this exercises; expect bring-up to take several minutes (--gate-timeout). The chain bends "
              "north after R2 because the terrain east of R2 is flat for 50 km (a straight R3 stayed clear of R1 at 44 km and "
              "a ridge at ~51 km east blocked R3-B): found with `topology three_hop --place ...` on 2026-09-20 -- R2-R3 +16 dB, "
              "R3-B +12 dB, R1-R3 -6.7 dB, R2-B -6.6 dB, R1-B -16 dB.",
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
    "page_transfer": Scenario(
        "page_transfer", "relay topology; each probe is one RNS.Resource of ~12 parts over a Link: the NomadNet page fetch.",
        nodes=[comp("A", -8), rep("R", 0), comp("B", 8)],
        must_link=[("A", "R"), ("R", "B")], must_block=[("A", "B")], expected_hops=1, min_delivered=0.5, probes=3,
        unit="resource", resource_size=5100,
        notes="The field's failure mode (2026-09-19): a 12-part page against RNS's AWAITING_PROOF window (12 x rtt + 40 s) "
              "and the Resource re-request cadence; the in-flight cap, duplicate suppression and slot waits only matter here. "
              "Reports per transfer: link handshake time (MeshChat gives it 15 s), parts, re-sent parts, complete/failed, wall "
              "time; per part: round-0 fragments landed, reconcile rounds, first fragment -> known complete.",
    ),
    "page_transfer_bidir": Scenario(
        "page_transfer_bidir", "page_transfer with the responder pushing its own 12-part Resource back on the same Link at once.",
        nodes=[comp("A", -8), rep("R", 0), comp("B", 8)],
        must_link=[("A", "R"), ("R", "B")], must_block=[("A", "B")], expected_hops=1, min_delivered=0.34, probes=3,
        unit="resource", resource_size=5100, respond_resource_size=5100, respond_resources=1,
        notes="The 2026-09-19 night regression (answers queued 30 s behind the answerer's own bursts) needs both ends sending "
              "Resources at the same time; the completion report is most exposed exactly there. Reports both directions.",
    ),
    "duty_cycle_pages": Scenario(
        "duty_cycle_pages", "zero_hop topology, back-to-back 12-part Resources: the 30%/60 s duty-cycle limiter under load.",
        nodes=[comp("A", -0.5), comp("B", 0.5)],
        must_link=[("A", "B")], must_block=[], expected_hops=0, min_delivered=0.6, probes=4,
        unit="resource", resource_size=5100,
        notes="The duty-cycle policy is the zero-hop ceiling and nothing exercised the limiter under load before 2026-09-20. "
              "Read duty_cycle_wait_s on raw_fragment_sent (the report's duty-cycle waits distribution) and the transfer wall times.",
    ),
    "link_setup": Scenario(
        "link_setup", "relay topology; each probe is one RNS Link handshake (LINKREQUEST out, LRPROOF back) at one hop.",
        nodes=[comp("A", -8), rep("R", 0), comp("B", 8)],
        must_link=[("A", "R"), ("R", "B")], must_block=[("A", "B")], expected_hops=1, min_delivered=0.5, probes=8,
        unit="link",
        notes="MeshChat's NomadNet downloader gives a link 15 s (--link-deadline): the run counts handshakes inside that window "
              "and reports the distribution (field 2026-09-19: 6.8-11 s at one hop, 21.8 s with one lost frame).",
    ),
    "link_setup_two_hop": Scenario(
        "link_setup_two_hop", "two_hop topology; each probe is one RNS Link handshake at two hops (probes start once both paths exist).",
        nodes=[comp("A", -8), rep("R1", 0), rep("R2", 22, mast=30), comp("B", 30)],
        must_link=[("A", "R1"), ("R1", "R2"), ("R2", "B")], must_block=[("A", "R2"), ("R1", "B"), ("A", "B")],
        expected_hops=2, min_delivered=0.3, probes=8, unit="link", start_after_paths=True,
        notes="Same 15 s window at two hops.",
    ),
    "many_peers": Scenario(
        "many_peers", "five companions within a kilometre, each with its own RNS node: 4 bound peers, so small-mesh mode is OFF.",
        nodes=[comp("A", -0.6), comp("B", 0.6), comp("C", 0.0, 0.5), comp("D", 0.0, -0.5), comp("E", -0.3, -0.3)],
        must_link=[("A", "B"), ("A", "C"), ("A", "D"), ("A", "E"), ("B", "C")], must_block=[], expected_hops=0, min_delivered=0.5,
        probes=8, extra_rns_nodes=["C", "D", "E"], min_bound_peers=4,
        notes="Every field capture so far is small-mesh (<= 3 bound peers), so the CHANNEL / DIRECT-supplement routing "
              "(broadcast announces, path-request supplements, bootstrap supplements) has never run against firmware. The hard "
              "check is that the sender's capture shows >= 4 bound peers and small_mesh_mode=False on its sends.",
    ),
    "mixed_builds": Scenario(
        "mixed_builds", "relay topology with the responder on an OLDER build (--responder-interface, default git:d7dcba9): protocol changes must degrade cleanly.",
        nodes=[comp("A", -8), rep("R", 0), comp("B", 8)],
        must_link=[("A", "R"), ("R", "B")], must_block=[("A", "B")], expected_hops=1, min_delivered=0.34, probes=6, size=383,
        responder_interface="git:d7dcba9",
        notes="The completion report (f0a824a) is a protocol change: an old peer that never reports must still be reconciled by "
              "QUERY/ANSWER, and its v3 answers must still be read. d7dcba9 is the tree as found on 2026-09-20 before the "
              "report/trim/backoff commits. Full-size probes so raw bursts and the reconcile are exercised.",
    ),
    "companion_restart": Scenario(
        "companion_restart", "relay topology; the sender's companion firmware is restarted after --fail-after probes (serial/TCP reconnect mid-run).",
        nodes=[comp("A", -8), rep("R", 0), comp("B", 8)],
        must_link=[("A", "R"), ("R", "B")], must_block=[("A", "B")], expected_hops=1, min_delivered=0.0, probes=8, informational=True,
        actions=lambda args: [After(args.fail_after, "restart", "A")],
        notes="Informational (2026-09-20): whether the meshcore library's auto-reconnect and the interface's _on_mc_connected "
              "re-arm (module docstring item 6) bring the interface back after its companion reboots. MeshBench's served TCP "
              "endpoint may not survive a firmware restart at all; the run reports what happened rather than asserting it.",
    ),
    "soak": Scenario(
        "soak", "relay topology, probes for --duration seconds (default 1800) with health snapshots: peer cache, maps, RSS, threads.",
        nodes=[comp("A", -8), rep("R", 0), comp("B", 8)],
        must_link=[("A", "R"), ("R", "B")], must_block=[("A", "B")], expected_hops=1, min_delivered=0.3, probes=0,
        duration_s=1800.0, health_interval_s=60.0,
        notes="Reports the first and last health snapshot per node (RSS, threads, sizes of _peers/_resolved_paths/_rns_token_peer/"
              "reassembly/dedup maps) so unbounded growth over half an hour shows. --duration overrides.",
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


def reserved_identities(nodefs_root: str, names: list) -> dict:
    """{node: pubkey prefix} for every node whose MeshBench-generated identity
    starts with 0x00 or 0xFF. MeshCore reserves those first bytes ("reserved
    id hashes": companion_radio/MyMesh.cpp regenerates its identity until
    the first byte is neither, and Identity.cpp's validatePrivateKey refuses
    them), because the first byte is the node's path hash on air. MeshBench
    v0.1.0's seeded identities skip that rule: seed 13 gave node A the key
    0032090d... on 2026-09-20, and in every seed-13 run the other side held
    A as a contact in its firmware store yet path discovery to it never
    answered and no DIRECT frame was ever exchanged -- eleven runs of a
    node no real radio can be. The identity lives in <nodefs>/<name>/_main.id
    (first 32 bytes = public key) once the firmware has started."""
    out = {}
    for name in names:
        path = os.path.join(nodefs_root, name, "_main.id")
        try:
            with open(path, "rb") as f:
                pub = f.read(32)
        except OSError:
            continue
        if len(pub) == 32 and pub[0] in (0x00, 0xFF):
            out[name] = pub[:4].hex()
    return out


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

def node_argv(args, name: str, endpoint: str, role: str, extra: list, interface_path: Optional[str] = None) -> list:
    argv = [sys.executable, os.path.join(HERE, "rns_multiprocess_sim.py"), "node",
            "--backend", "real", "--name", name, "--server", endpoint, "--role", role,
            "--loglevel", str(args.loglevel), "--advert-interval", str(args.advert_interval)]
    if not args.fast_timing:
        argv.append("--production-timing")
    if args.capture_dir:
        argv += ["--capture-dir", args.capture_dir]
    for opt in args.iface_option:
        argv += ["--iface-option", opt]
    if interface_path:
        argv += ["--interface-path", interface_path]
    if args.health_interval:
        argv += ["--health-interval", str(args.health_interval)]
    return argv + extra


def resolve_interface_build(spec: Optional[str], capture_dir: Optional[str]) -> Optional[str]:
    """`--responder-interface`: a file path, or `git:<rev>` for that
    revision's Interface/SmartMeshCoreInterface.py written next to the
    captures (mixed_builds runs the responder on an older tree)."""
    if not spec:
        return None
    if not spec.startswith("git:"):
        return os.path.abspath(spec)
    rev = spec[4:]
    repo = os.path.dirname(HERE)
    out_dir = capture_dir or tempfile.mkdtemp(prefix="smci-build-")
    os.makedirs(out_dir, exist_ok=True)
    dest = os.path.join(out_dir, f"SmartMeshCoreInterface_{rev.replace('/', '_')}.py")
    with open(dest, "w") as f:
        subprocess.run(["git", "-C", repo, "show", f"{rev}:Interface/SmartMeshCoreInterface.py"], check=True, stdout=f)
    return dest


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

    probes = args.probes if args.probes is not None else scenario.probes
    size = args.size or scenario.size
    unit = args.unit or scenario.unit
    duration = args.duration if args.duration is not None else scenario.duration_s
    if scenario.health_interval_s and not args.health_interval:
        args.health_interval = scenario.health_interval_s
    capture_dir = args.capture_dir
    if capture_dir:
        os.makedirs(capture_dir, exist_ok=True)
    events_out = args.events_out or (os.path.join(capture_dir, "meshbench_events.jsonl") if capture_dir else None)
    if args.radio and not any(o.split("=", 1)[0] in ("freq", "bw", "sf", "cr") for o in args.iface_option):
        freq, bw, sf, cr = args.radio.split(",")
        args.iface_option = [f"freq={freq}", f"bw={bw}", f"sf={sf}", f"cr={cr}"] + args.iface_option

    companions = [n.name for n in scenario.nodes if n.kind == "companion"]
    repeaters = [n.name for n in scenario.nodes if n.kind == "repeater"]
    failures, measurements = [], {"unit": unit, "traffic": unit}
    responder_build = resolve_interface_build(args.responder_interface or scenario.responder_interface, capture_dir)
    if responder_build:
        measurements["responder_interface"] = responder_build
    # How long one unit of traffic may take, for the run's own deadline.
    unit_timeout = {"probe": args.probe_timeout, "link": args.rns_link_timeout,
                    "resource": args.rns_link_timeout + args.resource_timeout}[unit]

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
        reserved = reserved_identities(os.environ["MESHBENCH_NODEFS"], [n.name for n in scenario.nodes])
        check(not reserved, "no node was given a reserved MeshCore identity (public key first byte 0x00/0xFF): "
              + (", ".join(f"{k}={v}" for k, v in reserved.items()) or "none"))
        if reserved and not args.ignore_topology:
            log(f"stopping: seed {args.seed} gives {sorted(reserved)} an identity real firmware never has; pick another seed "
                "(--ignore-topology to run anyway)")
            return 3
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
        rns_names = [scenario.responder, scenario.sender] + list(scenario.extra_rns_nodes)
        for name in rns_names:
            endpoints[name] = nodes[name].serve(Transport.TCP)
            log(f"{name} companion served at tcp {endpoints[name]}")
        stats_before = stats_by_name(wb)

        try:
            host, port = endpoint_host_port(endpoints[scenario.responder])
            responder_extra = ["--announce-interval", str(args.announce_interval)]
            respond_size = args.respond_resource_size if args.respond_resource_size is not None else scenario.respond_resource_size
            if respond_size:
                responder_extra += ["--respond-resource-size", str(respond_size), "--respond-resources", str(scenario.respond_resources),
                                    "--resource-timeout", str(args.resource_timeout)]
            responder = NodeProcess(scenario.responder, node_argv(args, scenario.responder, f"{host}:{port}", "responder",
                                                                  responder_extra, interface_path=responder_build),
                                    echo=not args.quiet_nodes)
            procs.append(responder)
            ready = responder.wait_event("ready", timeout=args.rns_start_timeout)
            if ready is None:
                log(f"responder never became ready (exit {responder.proc.returncode}); see its output above")
                return 2
            # Extra RNS peers (many_peers): more companions, each its own RNS
            # responder that announces and binds like B, so the sender ends
            # up with more bound peers than small-mesh mode allows.
            extra_procs = {}
            for name in scenario.extra_rns_nodes:
                host, port = endpoint_host_port(endpoints[name])
                extra_procs[name] = NodeProcess(name, node_argv(args, name, f"{host}:{port}", "responder",
                                                                ["--announce-interval", str(args.announce_interval * 2)]),
                                                echo=not args.quiet_nodes)
                procs.append(extra_procs[name])
                if extra_procs[name].wait_event("ready", timeout=args.rns_start_timeout) is None:
                    log(f"extra RNS node {name} never became ready (exit {extra_procs[name].proc.returncode})")
                    return 2
                time.sleep(3.0)
            time.sleep(args.settle)

            host, port = endpoint_host_port(endpoints[scenario.sender])
            sender_extra = [
                "--dest", ready["dest"], "--probes", str(probes), "--wait", str(args.wait),
                "--size", str(size), "--path-timeout", str(args.path_timeout),
                "--path-request-interval", str(args.path_request_interval),
                "--probe-timeout", str(args.probe_timeout), "--late-grace", str(args.late_grace),
                "--traffic", unit, "--resource-size", str(args.resource_size or scenario.resource_size),
                "--resource-timeout", str(args.resource_timeout), "--link-timeout", str(args.rns_link_timeout),
                "--link-deadline", str(args.link_deadline),
            ]
            if scenario.start_after_paths or args.start_after_paths:
                sender_extra.append("--start-gate")
            if duration:
                sender_extra += ["--duration", str(duration)]
            sender = NodeProcess(scenario.sender, node_argv(args, scenario.sender, f"{host}:{port}", "sender", sender_extra),
                                 echo=not args.quiet_nodes)
            procs.append(sender)

            # Start gate (2026-09-20): hold the traffic until both ends have
            # a DIRECT path, so a hop-count scenario measures its hops and
            # not the advert coin flip. bring_up is this gate on its own.
            gate = {"used": bool(scenario.start_after_paths or args.start_after_paths)}
            if gate["used"]:
                t_gate = time.monotonic()
                gate_deadline = t_gate + args.gate_timeout
                while time.monotonic() < gate_deadline:
                    have_a = bool(sender.events_named("direct_path"))
                    have_b = bool(responder.events_named("direct_path"))
                    waiting = bool(sender.events_named("waiting_for_go"))
                    if sender.proc.poll() is not None:
                        break
                    if have_a and have_b and waiting:
                        break
                    time.sleep(0.5)
                gate.update(sender_path=bool(sender.events_named("direct_path")), responder_path=bool(responder.events_named("direct_path")),
                            waited_s=round(time.monotonic() - t_gate, 1), timed_out=time.monotonic() >= gate_deadline)
                log(f"start gate: sender DIRECT path {gate['sender_path']}, responder DIRECT path {gate['responder_path']}, "
                    f"waited {gate['waited_s']} s{' (TIMED OUT)' if gate['timed_out'] else ''}; releasing the sender")
                sender.send_line("go")
            measurements["start_gate"] = gate

            actions = scenario.actions(args)
            traffic_every = args.traffic_interval if scenario.traffic else None
            traffic_node = scenario.traffic["node"] if scenario.traffic else None
            next_traffic, traffic_sent = time.monotonic() + 5.0, 0
            stats_at = {}
            deadline = time.monotonic() + args.path_timeout + (args.gate_timeout if gate["used"] else 0) + (
                duration + unit_timeout if duration else probes * (args.wait + unit_timeout)) + args.late_grace + 60
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
                        elif act.action == "restart":
                            # companion_restart: the firmware behind a served
                            # endpoint reboots; the interface must reconnect.
                            node.stop()
                            time.sleep(act.args.get("down_s", 5.0))
                            node.start()
                            node.wait_running(timedelta(seconds=60))
                            for other in nodes.values():
                                if other.name != act.node and any(n.name == other.name and n.standby for n in scenario.nodes) \
                                        and a_running(stats_by_name(wb), other.name):
                                    other.stop()
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
                                resolved=done.get("resolved", {}), background_msgs=traffic_sent,
                                late=done.get("late", 0), late_rtts=done.get("late_rtts", []),
                                link_times_s=done.get("link_times_s", []), resources=done.get("resources", []),
                                traffic_elapsed_s=done.get("elapsed_s"))
            link_probes = [p for p in plist if p.get("kind") == "link"] or sender.events_named("link_setup")
            if link_probes:
                measurements["links_within_deadline"] = sum(1 for p in link_probes if p.get("within_deadline"))
                measurements["link_deadline_s"] = args.link_deadline
            if done.get("late"):
                log(f"    late deliveries (PROOF after the {args.probe_timeout:.0f} s probe timeout): {done['late']} "
                    f"with RTT {done.get('late_rtts')}")
            if measurements["link_times_s"]:
                lt = measurements["link_times_s"]
                log(f"    link handshakes: {lt} s; within {args.link_deadline:.0f} s: "
                    f"{measurements.get('links_within_deadline', 'n/a')}/{len(link_probes) or len(lt)}")
            for rs in measurements["resources"]:
                log(f"    resource {rs.get('tag')}: {'complete' if rs.get('complete') else 'FAILED/timeout'} in {rs.get('elapsed_s')} s, "
                    f"{rs.get('total_parts')} parts, {rs.get('resent_parts')} re-sent")
            back = [e for e in responder.events_named("resource_sent")]
            if back:
                measurements["resources_back"] = [{k: v for k, v in e.items() if k not in ("event", "name", "_t")} for e in back]
                for rs in measurements["resources_back"]:
                    log(f"    return resource {rs.get('tag')}: {'complete' if rs.get('complete') else 'FAILED/timeout'} in {rs.get('elapsed_s')} s, "
                        f"{rs.get('total_parts')} parts, {rs.get('resent_parts')} re-sent")
            received_back = [e for e in sender.events_named("resource_received")]
            if received_back:
                measurements["resources_received_by_sender"] = [{k: v for k, v in e.items() if k not in ("event", "name", "_t")} for e in received_back]
            ttp = {}
            for proc in (sender, responder, *extra_procs.values()):
                ev = proc.events_named("direct_path")
                if ev:
                    ttp[proc.name] = ev[0].get("since_online_s")
            measurements["time_to_direct_path"] = ttp
            measurements["path_requests"] = len(sender.events_named("path_requested"))
            log(f"    time to first DIRECT path per node (s since online): {ttp}; sender RNS path requests: {measurements['path_requests']}")
            health = [e for e in sender.events_named("health")] + [e for e in responder.events_named("health")]
            if done.get("health"):
                health.append({"name": scenario.sender, "final": True, **done["health"]})
            if health:
                measurements["health"] = [{k: v for k, v in e.items() if k not in ("event", "_t")} for e in health]
                for name in (scenario.sender, scenario.responder):
                    mine = [h for h in measurements["health"] if h.get("name") == name]
                    if mine:
                        log(f"    {name} health: RSS {mine[0].get('rss_kb')} -> {mine[-1].get('rss_kb')} kB, threads "
                            f"{mine[0].get('threads')} -> {mine[-1].get('threads')}, sizes {mine[-1].get('sizes')}")

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
            if scenario.min_bound_peers is not None and capture_dir:
                recs = read_capture(capture_dir, scenario.sender)
                bound_max = max((r.get("bound_peers") or 0 for r in recs if r.get("direction") == "out" and "event" not in r), default=0)
                non_small = sum(1 for r in recs if r.get("direction") == "out" and "event" not in r
                                and r.get("small_mesh_mode") is False and (r.get("bound_peers") or 0) >= scenario.min_bound_peers)
                check(bound_max >= scenario.min_bound_peers,
                      f"sender bound >= {scenario.min_bound_peers} peers (max seen {bound_max})")
                check(non_small > 0, f"sender routed with small-mesh mode OFF ({non_small} sends outside small-mesh mode)")
                measurements.update(bound_peers_max=bound_max, sends_outside_small_mesh=non_small)
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
                last_start = max((a.probe for a in actions if a.action in ("start", "restart")), default=None)
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
                        if act.action == "restart" and act.fired_at_probe is not None:
                            check(a_running(stats_after, act.node), f"{act.node} firmware is running at the end")
                            online_again = [e for e in sender.events_named("direct_path")]
                            log(f"info  {act.node} restarted after probe {act.probe}; sender DIRECT-path events: {len(online_again)}")
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
        result_path = os.path.join(capture_dir, "result.json")
        result = {"scenario": scenario.name, "informational": scenario.informational, "failures": failures, "exit_code": exit_code,
                  "measurements": measurements, "args": {k: v for k, v in vars(args).items() if k != "func"},
                  "interface_path": INTERFACE_PATH, "git_head": git_head()}
        with open(result_path, "w") as f:
            json.dump(result, f, indent=1, default=str)
        # The analysis every run should carry (2026-09-20): per-hop attempt
        # success and ACK latency, completion checks by outcome, per-part
        # bursts, wait breakdown, on-air bytes and misses by cause from the
        # engine events with the LBT-preventable share, the airtime ledger.
        try:
            analysis = meshbench_report.analyse(capture_dir)
            result["analysis"] = meshbench_report.compact(analysis)
            with open(result_path, "w") as f:
                json.dump(result, f, indent=1, default=str)
            print("\n--- analysis (meshbench_report.py) ---")
            meshbench_report.print_block(analysis)
        except Exception as e:  # noqa: BLE001 - the run's verdict does not depend on the summary
            log(f"analysis failed: {e}")
        log(f"result.json written to {capture_dir}")
    if failures:
        print("\nFAILED:\n  " + "\n  ".join(failures))
    return exit_code


def a_running(stats: dict, name: str) -> bool:
    return bool(stats.get(name, {}).get("running"))


def git_head() -> Optional[str]:
    try:
        return subprocess.run(["git", "-C", os.path.dirname(HERE), "rev-parse", "--short", "HEAD"],
                              check=True, capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception:  # noqa: BLE001
        return None


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
    # traffic modes and metrics (2026-09-20)
    run.add_argument("--unit", choices=["probe", "link", "resource"], default=None,
                     help="Override what one probe is: a DATA packet, an RNS Link handshake, or an RNS Resource (page) over a Link")
    run.add_argument("--resource-size", type=int, default=None, help="unit=resource: random bytes per Resource (scenario default 5100 = 12 parts)")
    run.add_argument("--resource-timeout", type=float, default=600.0, help="unit=resource: seconds to wait for one Resource to conclude")
    run.add_argument("--respond-resource-size", type=int, default=None,
                     help="Responder pushes a Resource of this size back on every Link (bidirectional load); scenario default")
    run.add_argument("--rns-link-timeout", type=float, default=90.0,
                     help="unit=link/resource: seconds to wait for an RNS Link to establish (--link-timeout is MeshBench's link budget)")
    run.add_argument("--link-deadline", type=float, default=15.0, help="Handshakes inside this many seconds are counted (MeshChat's window)")
    run.add_argument("--late-grace", type=float, default=90.0,
                     help="After the last probe, keep watching outstanding receipts this long; late PROOFs are reported, not counted as lost")
    run.add_argument("--start-after-paths", action="store_true", help="Hold the traffic until both ends have a DIRECT path (scenario default for two_hop)")
    run.add_argument("--gate-timeout", type=float, default=600.0, help="Longest the start gate waits for both DIRECT paths before releasing anyway")
    run.add_argument("--responder-interface", default=None, help="Path or git:<rev>: the responder runs that interface build (mixed_builds)")
    run.add_argument("--duration", type=float, default=None, help="soak: seconds of traffic instead of a fixed probe count")
    run.add_argument("--health-interval", type=float, default=0.0, help="Nodes emit health snapshots this often (soak default 60)")

    suite = sub.add_parser("suite", help="Run several scenarios over several seeds and summarise them (medians and ranges)")
    suite.add_argument("--scenarios", default="zero_hop,relay,two_hop,large_payload",
                       help="Comma-separated scenario names, 'all', or 'quick' (zero_hop, relay, large_payload: the ~15 min regression check)")
    suite.add_argument("--seeds", default="7,11,13", help="Comma-separated MeshBench seeds; each scenario runs once per seed")
    suite.add_argument("--runs-per-seed", type=int, default=1)
    suite.add_argument("--parallel", type=int, default=2, help="Concurrent runs (each has its own node filesystem root)")
    suite.add_argument("--out-dir", required=True, help="One subdirectory per run: <scenario>-s<seed>-<n>/ with run.log, captures, result.json")
    suite.add_argument("--run-arg", action="append", default=[], help="Extra argument passed to every `run` (repeatable)")
    suite.add_argument("--write-baseline", default=None, metavar="PATH",
                       help="Also write the summary as a baseline file (e.g. tests/baselines/<date>-meshbench-<commit>.md)")
    suite.add_argument("--label", default=None, help="Title line for the summary / baseline file")
    suite.add_argument("--summarise-existing", action="store_true",
                       help="Run nothing: summarise every <scenario>-s<seed>-<n> directory already under --out-dir "
                            "(after re-running a scenario into the same directory, or to rebuild summary.md)")

    topo = sub.add_parser("topology", help="Measure a scenario's link budget against the terrain without running it")
    topo.add_argument("scenario", choices=sorted(SCENARIOS))
    topo.add_argument("--meshbench-binary", default=None)
    topo.add_argument("--preset", default=DEFAULT_PRESET)
    topo.add_argument("--link-timeout", type=float, default=90.0)
    topo.add_argument("--link-margin", type=float, default=6.0)
    topo.add_argument("--block-margin", type=float, default=-4.0)
    topo.add_argument("--place", action="append", default=[], metavar="NAME=EAST,NORTH[,HEIGHT]",
                      help="Override a node's placement (km east, km north, metres) to try alternatives")
    topo.add_argument("--pair", action="append", default=[], metavar="A-B", help="Extra pairs to measure")

    report = sub.add_parser("report", help="Summarise finished run directories (meshbench_report.py)")
    report.add_argument("dirs", nargs="+")
    report.add_argument("--md", action="store_true")
    report.add_argument("--aggregate", action="store_true")
    report.add_argument("--bursts", action="store_true")
    args = parser.parse_args()

    if args.mode == "list":
        for name, sc in SCENARIOS.items():
            print(f"{name:<20} {sc.summary}")
            print(f"{'':<20} nodes: {', '.join(f'{n.name}({n.kind[0]},{n.east_km:+g}E,{n.north_km:+g}N,{n.height_m:g}m)' for n in sc.nodes)}")
            what = {"probe": f"{sc.probes} probes of {sc.size} B", "link": f"{sc.probes} Link handshakes",
                    "resource": f"{sc.probes} Resources of {sc.resource_size} B" + (f" + {sc.respond_resource_size} B back" if sc.respond_resource_size else "")}[sc.unit]
            if sc.duration_s:
                what = f"probes for {sc.duration_s:.0f} s"
            print(f"{'':<20} expects {sc.expected_hops if sc.expected_hops is not None else 'n/a'} hop(s), delivery floor {sc.min_delivered:.0%}, "
                  f"{what}{'; probes start once both DIRECT paths exist' if sc.start_after_paths else ''}"
                  f"{'; extra RNS nodes ' + ','.join(sc.extra_rns_nodes) if sc.extra_rns_nodes else ''}"
                  f"{'; responder on ' + sc.responder_interface if sc.responder_interface else ''}{'; informational' if sc.informational else ''}")
            if sc.notes:
                print(f"{'':<20} {sc.notes}")
        return
    if args.mode == "topology":
        sys.exit(measure_topology(SCENARIOS[args.scenario], args))
    if args.mode == "report":
        flags = [f for f, on in (("--md", args.md), ("--aggregate", args.aggregate), ("--bursts", args.bursts)) if on]
        meshbench_report.main(flags + args.dirs)
        return
    if args.mode == "suite":
        sys.exit(run_suite(args))
    scenario = SCENARIOS[args.scenario]
    if (args.size or scenario.size) > 383:
        parser.error("--size above 383 exceeds RNS.Packet.ENCRYPTED_MDU; the sender's RNS.Packet.pack() would refuse it")
    probes = args.probes if args.probes is not None else scenario.probes
    acts = scenario.actions(args)
    if acts and max(a.probe for a in acts) + args.recover_probes > probes:
        parser.error(f"staged actions reach probe {max(a.probe for a in acts)} and need {args.recover_probes} more; raise --probes")
    sys.exit(run_scenario(scenario, args))


# ---------------------------------------------------------------------------
# topology: the link-budget gate on its own (placing a new scenario)
# ---------------------------------------------------------------------------

def measure_topology(scenario: Scenario, args) -> int:
    """Place the scenario's nodes (with --place overrides) and run only the
    topology gate, plus any --pair. Added 2026-09-20 after three_hop's first
    placements failed the gate against the terrain (R3-B blocked by a hill,
    R1-R3 clear at 44 km): a chain of hidden hops has to be found by
    measuring, and this is far cheaper than a full run."""
    if Workbench is None:
        sys.exit("the meshbench Python client is not installed")
    nodes_spec = {n.name: N(n.name, n.kind, n.east_km, n.north_km, n.height_m) for n in scenario.nodes}
    for spec in args.place:
        name, rest = spec.split("=", 1)
        parts = [float(x) for x in rest.split(",")]
        n = nodes_spec[name]
        n.east_km, n.north_km = parts[0], parts[1]
        if len(parts) > 2:
            n.height_m = parts[2]
    trial = Scenario(scenario.name, scenario.summary, list(nodes_spec.values()), scenario.must_link, scenario.must_block,
                     scenario.expected_hops, scenario.min_delivered)
    for pair in args.pair:
        a, b = pair.split("-", 1)
        trial.must_link = trial.must_link + [(a, b)]
    os.environ.setdefault("MESHBENCH_NODEFS", tempfile.mkdtemp(prefix="meshbench-nodefs-topo-"))
    args.firmware_version = "v1.17.1"
    prefetch_terrain(args, trial)
    with Workbench.headless(fixture="", binary=args.meshbench_binary) as wb:
        wb.project.new()
        for n in trial.nodes:
            lat, lon = offset(n.east_km, n.north_km)
            kind = Kind.COMPANION if n.kind == "companion" else Kind.SIMPLE_REPEATER
            wb.nodes.place(n.name, kind, lat, lon, height_m=n.height_m)
            log(f"placed {n.name:<2} {n.kind:<9} at ({n.east_km:+.1f} km E, {n.north_km:+.1f} km N) h={n.height_m} m")
        if args.preset:
            wb.call("radio.preset", {"preset": args.preset})
        rows, ok = topology_gate(wb, trial, args)
    log("topology gate " + ("PASSES" if ok else "FAILS") + " -- the scenario's placements are:")
    print("    nodes=[" + ", ".join(
        (f'comp("{n.name}", {n.east_km:g}' + (f", {n.north_km:g}" if n.north_km else "") + ")") if n.kind == "companion"
        else f'rep("{n.name}", {n.east_km:g}, {n.north_km:g}, mast={n.height_m:g})' for n in trial.nodes) + "],")
    return 0 if ok else 3


# ---------------------------------------------------------------------------
# suite: several scenarios x several seeds, summarised
# ---------------------------------------------------------------------------

def run_suite(args) -> int:
    """Runs each scenario once per seed (times --runs-per-seed), --parallel
    at a time, each as its own `run` subprocess with run.log saved beside
    its captures, then writes summary.md / summary.json (per-run rows and
    per-scenario medians with ranges) -- the shape tests/baselines/ files
    quote. One MeshBench run is a coin flip on bring-up and the RNS side is
    wall-clock driven, so a baseline is several seeds, never one run."""
    # `quick`: the under-20-minute regression check (2026-09-20) -- the DIRECT
    # fast path and report mechanism (zero_hop), one-hop timing (relay) and
    # fragmentation/reconcile (large_payload), one seed, three at a time.
    # two_hop is left out (its start gate can wait 10 min for bring-up), as is
    # anything with pages (25-45 min a run) and soak.
    QUICK = ["zero_hop", "relay", "large_payload"]
    names = (list(SCENARIOS) if args.scenarios.strip() == "all" else QUICK if args.scenarios.strip() == "quick"
             else [x.strip() for x in args.scenarios.split(",") if x.strip()])
    unknown = [n for n in names if n not in SCENARIOS]
    if unknown:
        sys.exit(f"unknown scenario(s): {unknown}; see `list`")
    seeds = [int(x) for x in args.seeds.split(",") if x.strip()]
    os.makedirs(args.out_dir, exist_ok=True)
    jobs = []
    for name in names:
        for seed in seeds:
            for i in range(1, args.runs_per_seed + 1):
                run_dir = os.path.join(args.out_dir, f"{name}-s{seed}-{i}")
                jobs.append((name, seed, run_dir))
    running = {}
    pending = list(jobs)
    finished = []
    if args.summarise_existing:
        pending = []
        for d in sorted(glob.glob(os.path.join(args.out_dir, "*-s*-*"))):
            m = re.match(r"^(.*)-s(\d+)-(\d+)$", os.path.basename(d))
            if not m or not os.path.isdir(d):
                continue
            code, took = None, 0.0
            try:
                with open(os.path.join(d, "result.json")) as f:
                    code = json.load(f).get("exit_code")
            except (OSError, json.JSONDecodeError):
                code = 3
            finished.append((m.group(1), int(m.group(2)), d, code if code is not None else 2, took))
        names = sorted({f[0] for f in finished})
        seeds = sorted({f[1] for f in finished})
        log(f"suite: summarising {len(finished)} existing run(s) under {args.out_dir}")
    else:
        log(f"suite: {len(jobs)} run(s) -- {names} x seeds {seeds} x {args.runs_per_seed}, {args.parallel} at a time, under {args.out_dir}")
    while pending or running:
        while pending and len(running) < args.parallel:
            name, seed, run_dir = pending.pop(0)
            os.makedirs(run_dir, exist_ok=True)
            argv = [sys.executable, os.path.abspath(__file__), "run", name, "--seed", str(seed), "--capture-dir", run_dir,
                    "--quiet-nodes"] + args.run_arg
            logf = open(os.path.join(run_dir, "run.log"), "w")
            env = dict(os.environ)
            env.pop("MESHBENCH_NODEFS", None)   # each run picks its own root under its capture dir
            proc = subprocess.Popen(argv, stdout=logf, stderr=subprocess.STDOUT, env=env)
            running[proc.pid] = (proc, logf, name, seed, run_dir, time.monotonic())
            log(f"started {os.path.basename(run_dir)} (pid {proc.pid})")
            time.sleep(5.0)   # stagger MeshBench start-ups
        for pid, (proc, logf, name, seed, run_dir, t0) in list(running.items()):
            if proc.poll() is not None:
                logf.close()
                took = time.monotonic() - t0
                finished.append((name, seed, run_dir, proc.returncode, took))
                log(f"finished {os.path.basename(run_dir)}: exit {proc.returncode} in {took / 60:.1f} min")
                del running[pid]
        time.sleep(2.0)
    results = []
    for name, seed, run_dir, code, took in finished:
        try:
            results.append(meshbench_report.analyse(run_dir))
        except Exception as e:  # noqa: BLE001
            log(f"analysis of {run_dir} failed: {e}")
    agg = meshbench_report.aggregate(results)
    title = args.label or f"MeshBench suite -- {time.strftime('%Y-%m-%d')} -- commit {git_head()}"
    lines = [f"# {title}", "",
             f"Scenarios {names}, seeds {seeds}, {args.runs_per_seed} run(s) per seed, interface `{INTERFACE_PATH}`, "
             f"run arguments {args.run_arg or 'defaults'}. Produced by `meshbench_scenarios.py suite`; per-run details in each "
             f"`<scenario>-s<seed>-<n>/result.json` (\"analysis\") and `run.log`.", "",
             "Read with the two caveats every baseline file carries: MeshBench's RF is optimistic and its airtime 1.2-1.45x "
             "RadioLib's, its runs are wall-clock driven and not reproducible, and its virtual radio has no listen-before-talk "
             "(the LBT-preventable column counts the half-duplex misses a real SX1262 would have deferred). Mechanics are the "
             "hard checks; delivery and timing are measured rates -- compare medians and ranges, not single runs.", "",
             "## Per scenario: medians [min-max] over the runs", "", meshbench_report.aggregate_md(agg), "",
             "## Per run", "", meshbench_report.MD_HEADER, meshbench_report.MD_SEP]
    for r in results:
        lines.append(meshbench_report.md_row(r))
    lines += ["", "## Verdicts", ""]
    for name, seed, run_dir, code, took in finished:
        lines.append(f"- {os.path.basename(run_dir)}: exit {code} ({'PASS' if code == 0 else 'FAIL' if code == 2 else 'topology gate / aborted' if code == 3 else 'ERROR'})"
                     + (f", {took / 60:.1f} min" if took else ""))
    bursts = [(r["name"], r["bursts"]) for r in results if r["bursts"] and any(b["sent_round0"] > 1 for b in r["bursts"])]
    if bursts:
        lines += ["", "## Per-part bursts (pkt: round-0 sent/landed, rounds, first fragment -> known complete s)", ""]
        for name, rows in bursts:
            lines.append(f"- {name}: " + "; ".join(f"{b['pkt_id']}: {b['sent_round0']}/{b['landed_round0']}, {b['rounds']}r, {b['complete_after_s']}" for b in rows))
    summary = "\n".join(lines) + "\n"
    with open(os.path.join(args.out_dir, "summary.md"), "w") as f:
        f.write(summary)
    with open(os.path.join(args.out_dir, "summary.json"), "w") as f:
        json.dump({"title": title, "runs": [{"scenario": n, "seed": sd, "dir": d, "exit": c, "minutes": round(t / 60, 1)}
                                             for n, sd, d, c, t in finished],
                   "aggregate": agg, "per_run": [meshbench_report.compact(r) for r in results]}, f, indent=1, default=str)
    if args.write_baseline:
        os.makedirs(os.path.dirname(os.path.abspath(args.write_baseline)), exist_ok=True)
        with open(args.write_baseline, "w") as f:
            f.write(summary)
        log(f"baseline written to {args.write_baseline}")
    print(summary)
    log(f"summary.md / summary.json written to {args.out_dir}")
    return 0 if all(c == 0 for _, _, _, c, _ in finished) else 2


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
