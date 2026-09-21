"""
Glue between the simulated mesh and real SmartMeshCoreInterface
instances, shared by the unit tests under tests/, testscripts/meshbench_scenarios.py
(capture summaries only) and the archived testscripts/legacy/fake_meshcore_repeater_sim.py
and rns_multiprocess_sim.py `run` mode.

The interface is loaded from Interface/SmartMeshCoreInterface.py by path
(no package), constructed directly with a RecordingOwner standing in for
RNS.Transport (the same "talk to the interface, not the whole stack"
isolation the field scripts use), and handed real packed RNS.Packet
bytes so every branch of its routing dispatcher -- ANNOUNCE, path
request (DATA/PLAIN), DIRECT-primary, bootstrap supplement, small-mesh
-- is reachable from a test.
"""
import asyncio
import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import threading
import time
import types
from typing import Callable, Dict, Iterable, List, Optional

import RNS

from .air import Air, parse_link_loss, parse_links, parse_type_loss
from .fake_meshcore import make_fake_meshcore_module
from .radio import RadioOptions, SimRadio, node_prefix, node_pubkey

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# SMCI_INTERFACE_PATH overrides the interface under test (2026-09-20): lets a
# scenario run against a saved copy of another build, e.g. a control run of
# the pre-edit tree, without touching the working file.
INTERFACE_PATH = os.environ.get("SMCI_INTERFACE_PATH") or os.path.join(REPO_ROOT, "Interface", "SmartMeshCoreInterface.py")

# Same 16-byte fixed destination the zero-hop field test hardcodes.
TEST_DEST_HASH = bytes.fromhex("5a" * 16)

# Sped-up timing so a scenario runs in seconds instead of minutes. Every
# value is still a real config key the interface reads; the ratios that
# _validate_direct_timing_budget checks (reassembly_idle_timeout >=
# (direct_ack_timeout_routed_max + direct_post_send_listen_max) *
# direct_send_attempts) are preserved. Pass fast=False to run a scenario
# against production defaults.
FAST_TIMING = {
    "bind_response_jitter_min": 0.2,
    "bind_response_jitter_max": 0.8,
    "bind_response_min_interval": 2.0,
    "peer_discovery_rerequest_interval": 15.0,
    "fragment_delay_min": 0.3,
    "fragment_delay_max": 0.8,
    "fragment_delay_zero_hop_min": 0.1,
    "fragment_delay_zero_hop_max": 0.3,
    "fragment_delay_per_hop_min": 0.3,
    "fragment_delay_per_hop_max": 0.8,
    "retransmit_jitter_min": 0.5,
    "retransmit_jitter_max": 1.5,
    "direct_ack_min_timeout": 1.5,
    "direct_ack_timeout_routed_max": 6.0,
    "direct_post_send_listen_min": 0.05,
    "direct_post_send_listen_max": 0.3,
    "direct_post_send_listen_success_min": 0.0,
    "direct_post_send_listen_success_max": 0.1,
    "incoming_quiet_window": 0.5,
    "incoming_quiet_defer_max_wait": 2.0,
    "path_discovery_base_cooldown": 3.0,
    "path_discovery_max_cooldown": 20.0,
    "contact_refresh_interval": 2.0,
    "reassembly_idle_timeout": 30.0,
    "reassembly_idle_timeout_coop": 30.0,
    "whole_packet_dedup_ttl": 60.0,
    "direct_completion_check_timeout": 3.0,
    "direct_path_reset_min_age": 5.0,
    "duty_cycle_window": 10.0,
    "duty_cycle_max_fraction": 0.9,
    "duty_cycle_max_fraction_zero_hop": 0.95,
    "stats_interval": 3600,
}

_module_cache: Dict[str, types.ModuleType] = {}
_module_lock = threading.Lock()


def load_interface_module(module_name: str = "smci_under_test", path: str = INTERFACE_PATH) -> types.ModuleType:
    with _module_lock:
        cached = _module_cache.get(module_name)
        if cached is not None:
            return cached
        spec = importlib.util.spec_from_file_location(module_name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _module_cache[module_name] = module
        return module


HERMETIC_RNS_CONFIG = """[reticulum]
  share_instance = No
  enable_transport = No
  panic_on_interface_error = No

[logging]
  loglevel = 3

[interfaces]
"""


def ensure_rns(loglevel: Optional[int] = None) -> None:
    """One RNS.Reticulum per process (it's a singleton) with a throwaway,
    non-shared config dir -- so RNS.Reticulum.storagepath and RNS.log
    work, and this process never attaches to (or is attached by) a real
    rnsd or another sim run via the shared-instance RPC socket."""
    if RNS.Reticulum.get_instance() is None:
        configdir = tempfile.mkdtemp(prefix="smci-sim-rns-")
        with open(os.path.join(configdir, "config"), "w") as f:
            f.write(HERMETIC_RNS_CONFIG)
        kwargs = {"configdir": configdir}
        if loglevel is not None:
            kwargs["loglevel"] = loglevel
        RNS.Reticulum(**kwargs)


def wait_until(predicate: Callable[[], bool], timeout: float, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


def dest_hash_for(name: str) -> bytes:
    """A deterministic 16-byte RNS destination hash standing in for
    'the application destination that lives behind node <name>'."""
    return hashlib.sha256(f"smci-sim-dest:{name}".encode()).digest()[: RNS.Reticulum.TRUNCATED_HASHLENGTH // 8]


def _fake_destination(dest_type: int, dest_hash: bytes):
    return types.SimpleNamespace(
        type=dest_type, hash=dest_hash, link_id=dest_hash, mtu=RNS.Reticulum.MTU,
        encrypt=lambda data: b"\x00" * 16 + data,
    )


def build_rns_packet(kind: str, dest_hash: bytes = TEST_DEST_HASH, payload: bytes = b"", header_type: int = None) -> bytes:
    """Real `RNS.Packet(...).pack()` bytes of the shape the routing
    dispatcher classifies:
      data          DATA / SINGLE          -> DIRECT-primary (token known) or bootstrap
      announce      ANNOUNCE / SINGLE      -> CHANNEL only, or small-mesh DIRECT-all
      path_request  DATA / PLAIN           -> broadcast + router supplement
      link_request  LINKREQUEST / SINGLE   -> handshake priority
      proof         PROOF / SINGLE         -> proof-correlation routing
      lrproof       PROOF, context LRPROOF -> delayed link proof
      resource      DATA / LINK, context RESOURCE -> a Resource part (never expires)
    """
    kinds = {
        "data": (RNS.Packet.DATA, RNS.Destination.SINGLE, RNS.Packet.NONE),
        "announce": (RNS.Packet.ANNOUNCE, RNS.Destination.SINGLE, RNS.Packet.NONE),
        "path_request": (RNS.Packet.DATA, RNS.Destination.PLAIN, RNS.Packet.NONE),
        "link_request": (RNS.Packet.LINKREQUEST, RNS.Destination.SINGLE, RNS.Packet.NONE),
        "proof": (RNS.Packet.PROOF, RNS.Destination.SINGLE, RNS.Packet.NONE),
        "lrproof": (RNS.Packet.PROOF, RNS.Destination.LINK, RNS.Packet.LRPROOF),
        "path_response": (RNS.Packet.DATA, RNS.Destination.SINGLE, RNS.Packet.PATH_RESPONSE),
        "link_data": (RNS.Packet.DATA, RNS.Destination.LINK, RNS.Packet.NONE),
        # A Resource data part on a Link (2026-09-20): context RESOURCE, which
        # the interface exempts from outgoing_max_age (RNS's Resource layer
        # owns the retry) -- the field's 483-byte page parts are this class,
        # and a plain "data" stand-in expires after 120s mid-transfer.
        "resource": (RNS.Packet.DATA, RNS.Destination.LINK, RNS.Packet.RESOURCE),
        "link_close": (RNS.Packet.DATA, RNS.Destination.LINK, RNS.Packet.LINKCLOSE),
    }
    if kind not in kinds:
        raise ValueError(f"unknown packet kind {kind!r} (choose from {sorted(kinds)})")
    packet_type, dest_type, context = kinds[kind]
    kwargs = {"packet_type": packet_type, "context": context, "create_receipt": False}
    if header_type is not None:
        kwargs["header_type"] = header_type
        if header_type == RNS.Packet.HEADER_2:
            kwargs["transport_id"] = bytes(RNS.Reticulum.TRUNCATED_HASHLENGTH // 8)
            kwargs["transport_type"] = RNS.Transport.TRANSPORT
    packet = RNS.Packet(_fake_destination(dest_type, dest_hash), payload, **kwargs)
    packet.pack()
    return packet.raw


class RecordingOwner:
    """Stand-in for RNS.Transport: records every inbound() delivery."""

    def __init__(self, name: str, log: Optional[Callable] = None):
        self.name = name
        self.log = log or (lambda msg: None)
        self.received: List[bytes] = []
        self.received_at: List[float] = []

    def inbound(self, data, interface):
        self.received.append(bytes(data))
        self.received_at.append(time.monotonic())
        self.log(f"[{self.name}] RNS inbound {len(data)} bytes: {bytes(data[:24])!r}...")


class SimNode:
    """One end node: a SimRadio driven by a real SmartMeshCoreInterface."""

    def __init__(self, name: str, radio: SimRadio, iface, owner: RecordingOwner, capture_dir: Optional[str]):
        self.name = name
        self.radio = radio
        self.iface = iface
        self.owner = owner
        self.capture_dir = capture_dir
        self.prefix = node_prefix(name)
        self.dest_hash = dest_hash_for(name)

    # convenience accessors over interface state a test cares about
    @property
    def peers(self) -> list:
        return list(self.iface._peers.keys())

    @property
    def resolved_paths(self) -> dict:
        return dict(self.iface._resolved_paths)

    def send(self, data: bytes) -> None:
        self.iface.process_outgoing(data)

    def run_on_loop(self, coro, timeout: float = 30.0):
        return asyncio.run_coroutine_threadsafe(coro, self.iface._loop).result(timeout=timeout)

    def seed_token(self, dest_hash: bytes, peer_prefix: str) -> None:
        """Bypass opportunistic token learning for a test that isn't about it."""
        self.iface._rns_token_peer[dest_hash] = peer_prefix

    def capture_records(self) -> list:
        if not self.capture_dir:
            return []
        return read_capture(self.capture_dir, self.iface.name)

    def detach(self) -> None:
        self.iface.detach()
        self.radio.detach()


_sys_modules_lock = threading.Lock()


def create_node(
    name: str, air: Air, module: types.ModuleType, config: Optional[dict] = None, fast: bool = True,
    capture_dir: Optional[str] = None, debug: bool = False, radio_options: Optional[RadioOptions] = None,
    log: Optional[Callable] = None, state_dir: Optional[str] = None,
    fake_options=None, online_wait_s: float = 10.0,
) -> SimNode:
    """Construct a real interface for `name` against `air`. Swaps
    sys.modules["meshcore"] for the duration of the constructor (that's
    where the interface imports it) -- serialized so parallel test
    processes/threads can't cross-wire two nodes."""
    state_dir = state_dir or tempfile.mkdtemp(prefix=f"smci-sim-{name}-")
    cfg = {
        "name": name, "transport": "tcp", "host": "sim", "tcp_port": 0,
        "peer_cache_path": os.path.join(state_dir, f"peers_{name}.json"),
        "stats_interval": 3600,
    }
    if fast:
        cfg.update(FAST_TIMING)
    if debug:
        cfg["debug_level"] = "debug"
    if capture_dir:
        cfg["packet_capture_enabled"] = "yes"
        cfg["packet_capture_dir"] = capture_dir
    cfg.update(config or {})

    owner = RecordingOwner(name, log)
    radio_holder = {}

    def radio_factory() -> SimRadio:
        # One radio per node for the process's life: a reconnect (alpha
        # 0.1.6 item 4's supervisor) re-attaches it, never a second one.
        radio = radio_holder.get("radio")
        if radio is None:
            radio = SimRadio(name, air, is_repeater=False, options=radio_options)
            radio_holder["radio"] = radio
        return radio

    fake_module = make_fake_meshcore_module(radio_factory, options=fake_options)
    with _sys_modules_lock:
        original = sys.modules.get("meshcore")
        sys.modules["meshcore"] = fake_module
        try:
            iface = module.SmartMeshCoreInterface(owner=owner, configuration=cfg)
        finally:
            if original is not None:
                sys.modules["meshcore"] = original
            else:
                sys.modules.pop("meshcore", None)
    # The constructor returns once the connection is open (alpha 0.1.6
    # item 4); the handshake and setup finish on the loop.
    deadline = time.monotonic() + online_wait_s
    while not iface.online and time.monotonic() < deadline and not iface.detached:
        time.sleep(0.01)
    radio = radio_holder.get("radio")
    if radio is None:
        raise RuntimeError(f"interface for {name!r} never connected to its simulated radio")
    node = SimNode(name, radio, iface, owner, capture_dir)
    node.fake_module = fake_module
    return node


class SimMesh:
    """A topology plus its repeaters, ready to have end nodes created on it.

        mesh = SimMesh(["A-R", "R-B"], repeaters=["R"], seed=1)
        a = mesh.add_node("A"); b = mesh.add_node("B")
        ...
        mesh.stop()
    """

    def __init__(
        self, links: Iterable[str], repeaters: Iterable[str] = (), seed: Optional[int] = None, loss: float = 0.0,
        link_loss=None, type_loss=None, airtime_base_ms: float = 50.0, airtime_per_byte_ms: float = 1.0,
        log: Optional[Callable] = None, radio_options: Optional[RadioOptions] = None,
        fast: bool = True, capture_dir: Optional[str] = None, debug: bool = False, module_name: str = "smci_under_test",
        startup_stagger_s: float = 0.4,
    ):
        ensure_rns()
        self.log = log or (lambda msg: None)
        adjacency = parse_links(links) if not isinstance(links, dict) else links
        if isinstance(link_loss, (list, tuple)):
            link_loss = parse_link_loss(link_loss)
        if isinstance(type_loss, (list, tuple)):
            type_loss = parse_type_loss(type_loss)
        self.air = Air(
            adjacency, seed=seed, loss=loss, link_loss=link_loss, type_loss=type_loss,
            airtime_base_ms=airtime_base_ms, airtime_per_byte_ms=airtime_per_byte_ms, log=self.log,
        )
        self.module = load_interface_module(module_name)
        self.radio_options = radio_options or RadioOptions()
        self.fast = fast
        self.capture_dir = capture_dir
        self.debug = debug
        self.state_dir = tempfile.mkdtemp(prefix="smci-sim-state-")
        self.startup_stagger_s = startup_stagger_s
        self.repeaters: Dict[str, SimRadio] = {}
        self.nodes: Dict[str, SimNode] = {}
        for name in repeaters:
            radio = SimRadio(name, self.air, is_repeater=True, options=self.radio_options)
            radio.attach(None)
            self.repeaters[name] = radio

    def add_node(self, name: str, config: Optional[dict] = None, radio_options: Optional[RadioOptions] = None,
                 fake_options=None, require_online: bool = True) -> SimNode:
        node = create_node(
            name, self.air, self.module, config=config, fast=self.fast, capture_dir=self.capture_dir,
            debug=self.debug, radio_options=radio_options or self.radio_options, log=self.log, state_dir=self.state_dir,
            fake_options=fake_options, online_wait_s=10.0 if require_online else 0.0,
        )
        if require_online and not node.iface.online:
            raise RuntimeError(f"interface for {name!r} did not come online")
        self.nodes[name] = node
        # Real nodes never boot in the same millisecond; without this, every
        # node's startup advert + bind REQUEST land on top of each other.
        if self.startup_stagger_s > 0:
            time.sleep(self.startup_stagger_s + self.air.rng.uniform(0, self.startup_stagger_s))
        return node

    def all_radios(self) -> List[SimRadio]:
        return list(self.repeaters.values()) + [n.radio for n in self.nodes.values()]

    def advert_all(self, spacing_s: Optional[float] = None) -> None:
        """Every radio adverts once, staggered so no node is transmitting
        its own advert while a repeater's relay of the previous one is
        still propagating -- the 'press advert on each radio' step a
        real field test starts with, so every node has every other as a
        contact before traffic flows. Default spacing covers ~3 relay
        hops of worst-case jitter (5x airtime per hop)."""
        if spacing_s is None:
            spacing_s = 4 * 6 * self.air.airtime_s(120) + 0.3
        for i, radio in enumerate(self.all_radios()):
            radio.advert_later(i * spacing_s + self.air.rng.uniform(0, 0.2))

    def advert_until_contacts(self, rounds: int = 4, timeout: float = 20.0) -> bool:
        """`advert_all` until every end node knows every other, or `rounds`
        tries are used up. Returns whether contacts populated.

        Audit fix (2026-09-19): `advert_all` sends exactly ONE un-retried
        advert per radio. Across a single repeater that is reliable enough,
        but on a 2-repeater chain half-duplex deafness and collisions ate the
        advert flood often enough that bring-up failed for most seeds -- a
        probe of the 3-link A-R1-R2-B chain found 2 of 3 seeds never
        populating contacts at all. The one scenario in the suite with two
        repeaters (`test_two_hop_fragmented_with_phantom_ack_loss`) therefore
        never actually ran: it died in its own setup assertion, which reads
        like a product bug rather than a harness one. Retrying is also what a
        real operator does -- you press advert again when a node hasn't
        appeared -- so this is closer to the field procedure, not a fudge."""
        for attempt in range(max(1, rounds)):
            if attempt:
                self.advert_all()
            if self.wait_contacts(timeout):
                return True
        return False

    def wait_contacts(self, timeout: float = 20.0) -> bool:
        """Every end node's radio has every other end node as a contact."""
        names = list(self.nodes)
        return wait_until(
            lambda: all(
                all(node_pubkey(other) in self.nodes[n].radio.contacts for other in names if other != n) for n in names
            ),
            timeout,
        )

    def wait_bound(self, timeout: float = 20.0) -> bool:
        """Every end node has bound every other end node."""
        want = len(self.nodes) - 1
        return wait_until(lambda: all(len(n.iface._peers) >= want for n in self.nodes.values()), timeout)

    def wait_resolved(self, timeout: float = 30.0) -> bool:
        """Every end node has a resolved DIRECT path to every other."""
        want = len(self.nodes) - 1
        return wait_until(lambda: all(len(n.iface._resolved_paths) >= want for n in self.nodes.values()), timeout)

    def stop(self) -> None:
        for node in self.nodes.values():
            try:
                node.detach()
            except Exception:
                pass
        for radio in self.repeaters.values():
            radio.detach()
        self.air.stop()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()


# -- packet capture helpers ---------------------------------------------------

def read_capture(capture_dir: str, iface_name: str) -> list:
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in iface_name)
    records = []
    if not os.path.isdir(capture_dir):
        return records
    for fn in sorted(os.listdir(capture_dir)):
        # alpha 0.1.5 item 7: a node label may precede "capture_".
        if (fn.startswith(f"capture_{safe_name}_") or f"_capture_{safe_name}_" in fn) and fn.endswith(".jsonl"):
            with open(os.path.join(capture_dir, fn)) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
    return records


def summarize_capture(records: list) -> dict:
    """Counts of the things a field-test analysis usually starts from."""
    import collections
    out = {
        "routing_decisions": collections.Counter(),
        "incoming_transports": collections.Counter(),
        "direct_attempts_ok": 0,
        "direct_attempts_failed": 0,
        "direct_attempts_by_hop": collections.defaultdict(lambda: [0, 0]),
        "ack_latency_s": [],
        "completion_checks": collections.Counter(),
        "rx_log_types": collections.Counter(),
        "other_events": collections.Counter(),
    }
    for r in records:
        event = r.get("event")
        if event == "rx_log":
            out["rx_log_types"][r.get("payload_typename")] += 1
        elif event == "direct_attempt_result":
            ok = bool(r.get("ok"))
            out["direct_attempts_ok" if ok else "direct_attempts_failed"] += 1
            out["direct_attempts_by_hop"][r.get("hop_count")][0 if ok else 1] += 1
            if r.get("ack_latency_s") is not None:
                out["ack_latency_s"].append(r["ack_latency_s"])
        elif event == "completion_check_result":
            out["completion_checks"][r.get("outcome")] += 1
        elif event is not None:
            out["other_events"][event] += 1
        elif r.get("direction") == "out" and r.get("routing_decision"):
            out["routing_decisions"][r["routing_decision"]] += 1
        elif r.get("direction") == "in" and r.get("transport"):
            out["incoming_transports"][r["transport"]] += 1
    out["direct_attempts_by_hop"] = dict(out["direct_attempts_by_hop"])
    return out


def format_summary(summary: dict) -> str:
    lat = summary["ack_latency_s"]
    lat_str = (
        f"n={len(lat)} min={min(lat):.2f}s avg={sum(lat)/len(lat):.2f}s max={max(lat):.2f}s" if lat else "n=0"
    )
    return (
        f"routing_decisions={dict(summary['routing_decisions'])}\n"
        f"incoming_transports={dict(summary['incoming_transports'])}\n"
        f"direct_attempts ok={summary['direct_attempts_ok']} failed={summary['direct_attempts_failed']} "
        f"by_hop(ok,fail)={summary['direct_attempts_by_hop']}\n"
        f"ack_latency {lat_str}\n"
        f"completion_checks={dict(summary['completion_checks'])} rx_log={dict(summary['rx_log_types'])} "
        f"other={dict(summary['other_events'])}"
    )
