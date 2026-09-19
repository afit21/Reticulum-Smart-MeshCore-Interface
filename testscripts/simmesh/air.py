"""
The air: a simulated shared LoRa medium over a defined topology.

Modeled (all deliberately simple):
  - Topology as undirected direct-hearing links. A packet transmitted by
    X is offered to every neighbor of X after one airtime; nobody else
    hears it. Multi-hop reach only ever comes from a SimRadio relaying.
  - Airtime: `airtime_base_ms + airtime_per_byte_ms * size` -- a linear
    approximation, not SF/BW/CR symbol timing.
  - Half-duplex: a node transmitting during any part of another packet's
    airtime does not hear that packet. Every node, not just repeaters.
    A node's own sends serialize behind each other (one radio).
  - Collisions: two neighbors of a receiver on air at overlapping times
    -> the receiver gets neither (no capture effect).
  - Loss: an independent draw per (transmitter, receiver) delivery, from
    a global probability plus optional per-direction overrides so
    asymmetric links (data gets through, ACKs don't) are reproducible.
  - One seeded `random.Random` shared with the radios, so a fixed --seed
    gives a reproducible run for A/B comparisons.

Not modeled: capture effect, CAD/LBT, RSSI-dependent loss, clock drift.
"""
import asyncio
import collections
import hashlib
import json
import random
import threading
import time
from dataclasses import dataclass, field, replace
from typing import Callable, Dict, Optional, Tuple

ROUTE_FLOOD = "FLOOD"
ROUTE_DIRECT = "DIRECT"

# MeshCore payload type codes, as the installed meshcore library names them
# (meshcore_parser.PAYLOAD_TYPENAMES) -- used verbatim in RX_LOG_DATA.
PTYPE_REQ = 0
PTYPE_RESPONSE = 1
PTYPE_TXT_MSG = 2
PTYPE_ACK = 3
PTYPE_ADVERT = 4
PTYPE_GRP_TXT = 5
PTYPE_PATH = 8
PTYPE_RAW_CUSTOM = 15  # Packet.h PAYLOAD_TYPE_RAW_CUSTOM: host-driven raw bytes, DIRECT only, no ACK
PAYLOAD_TYPENAMES = ["REQ", "RESPONSE", "TEXT_MSG", "ACK", "ADVERT", "GRP_TXT", "GRP_DATA", "ANON_REQ", "PATH", "TRACE", "MULTIPART", "CONTROL", "UNK12", "UNK13", "UNK14", "RAW_CUSTOM"]
ROUTE_TYPE_CODES = {ROUTE_FLOOD: 1, ROUTE_DIRECT: 2}

MAX_PATH_HASHES = 64


@dataclass(frozen=True)
class SimPacket:
    """One over-the-air packet. `path` is the accumulated repeater hash
    list for a FLOOD packet, or the remaining source route for a DIRECT
    one (Mesh.cpp: a repeater forwards a DIRECT packet only when path[0]
    is its own hash, then strips it). `dst`/`src` are node names standing
    in for the end-to-end crypto: only `dst` can "decrypt" an addressed
    packet. `pkt_id` is the content hash the firmware's flood-dedup ring
    keys on -- it excludes `path`, so a relayed copy is the same packet."""
    route: str
    ptype: int
    src: str
    src_hash: int
    dst: Optional[str]
    dst_hash: Optional[int]
    body: dict
    path: Tuple[int, ...] = ()
    pkt_id: str = ""
    size: int = 0

    def __post_init__(self):
        if not self.pkt_id:
            material = json.dumps([self.ptype, self.src, self.dst, self.body], sort_keys=True, default=str)
            object.__setattr__(self, "pkt_id", hashlib.sha256(material.encode()).hexdigest()[:16])
        if not self.size:
            object.__setattr__(self, "size", 4 + len(json.dumps(self.body, default=str)))

    def with_path(self, path) -> "SimPacket":
        return replace(self, path=tuple(path))

    def to_dict(self) -> dict:
        return {
            "route": self.route, "ptype": self.ptype, "src": self.src, "src_hash": self.src_hash,
            "dst": self.dst, "dst_hash": self.dst_hash, "body": self.body, "path": list(self.path),
            "pkt_id": self.pkt_id, "size": self.size,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "SimPacket":
        return cls(
            route=d["route"], ptype=d["ptype"], src=d["src"], src_hash=d["src_hash"],
            dst=d.get("dst"), dst_hash=d.get("dst_hash"), body=d["body"], path=tuple(d.get("path", ())),
            pkt_id=d.get("pkt_id", ""), size=d.get("size", 0),
        )

    @property
    def typename(self) -> str:
        return PAYLOAD_TYPENAMES[self.ptype] if 0 <= self.ptype < len(PAYLOAD_TYPENAMES) else "UNK"


@dataclass
class AirStats:
    transmissions: collections.Counter = field(default_factory=collections.Counter)
    deliveries: collections.Counter = field(default_factory=collections.Counter)
    deaf_drops: collections.Counter = field(default_factory=collections.Counter)
    collision_drops: collections.Counter = field(default_factory=collections.Counter)
    loss_drops: collections.Counter = field(default_factory=collections.Counter)
    by_type: collections.Counter = field(default_factory=collections.Counter)

    def summary(self) -> str:
        return (
            f"tx={dict(self.transmissions)} delivered={sum(self.deliveries.values())} "
            f"half_duplex_deaf={sum(self.deaf_drops.values())} collisions={sum(self.collision_drops.values())} "
            f"lost={sum(self.loss_drops.values())} by_type={dict(self.by_type)}"
        )


class Air:
    def __init__(
        self, adjacency: Dict[str, set], seed: Optional[int] = None, loss: float = 0.0,
        link_loss: Optional[Dict[Tuple[str, str], float]] = None, type_loss: Optional[Dict[str, float]] = None,
        airtime_base_ms: float = 50.0, airtime_per_byte_ms: float = 1.0, log: Optional[Callable] = None,
    ):
        self.adjacency: Dict[str, set] = {n: set(nb) for n, nb in adjacency.items()}
        for name, neighbors in list(self.adjacency.items()):
            for neighbor in neighbors:
                self.adjacency.setdefault(neighbor, set()).add(name)
        self.seed = seed
        self.rng = random.Random(seed)
        self.loss = loss
        self.link_loss = dict(link_loss or {})
        # Loss by payload typename ("ACK", "TEXT_MSG", ...), e.g. {"ACK": 1.0}
        # reproduces the phantom-ACK-loss field case: data arrives, ACKs don't.
        self.type_loss = dict(type_loss or {})
        self.airtime_base_ms = airtime_base_ms
        self.airtime_per_byte_ms = airtime_per_byte_ms
        self.log = log or (lambda msg: None)
        self.stats = AirStats()

        self._busy_until: Dict[str, float] = {name: 0.0 for name in self.adjacency}
        # Recent (start, end) transmissions per node, for half-duplex and
        # collision checks. Bounded: nothing looks back further than one
        # packet's airtime plus queueing.
        self._tx_log: Dict[str, collections.deque] = {name: collections.deque(maxlen=64) for name in self.adjacency}
        self._receivers: Dict[str, Tuple[Optional[asyncio.AbstractEventLoop], Callable]] = {}
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True, name="simmesh-air")
        self._thread.start()

    # -- lifecycle ---------------------------------------------------------

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def stop(self) -> None:
        if self._loop.is_running():
            async def _cancel_all():
                for task in asyncio.all_tasks():
                    if task is not asyncio.current_task():
                        task.cancel()
            try:
                asyncio.run_coroutine_threadsafe(_cancel_all(), self._loop).result(timeout=5)
            except Exception:
                pass
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        if not self._loop.is_running():
            self._loop.close()

    def nodes(self):
        return list(self.adjacency.keys())

    # -- attach ------------------------------------------------------------

    def attach(self, name: str, on_air: Callable, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """Register `on_air(packet, from_name)` for `name`. With `loop`
        given, it's invoked via call_soon_threadsafe on that loop; with
        None, it runs directly on the air's own loop (repeaters)."""
        if name not in self.adjacency:
            raise ValueError(f"{name!r} is not in the topology")
        self._receivers[name] = (loop, on_air)

    def detach(self, name: str) -> None:
        self._receivers.pop(name, None)

    # -- physics -------------------------------------------------------------

    def airtime_s(self, size: int) -> float:
        return (self.airtime_base_ms + self.airtime_per_byte_ms * size) / 1000.0

    def loss_for(self, from_name: str, to_name: str, typename: Optional[str] = None) -> float:
        p = self.link_loss.get((from_name, to_name), self.loss)
        if typename is not None:
            p = max(p, self.type_loss.get(typename, 0.0))
        return p

    def schedule_link_loss(self, at_s: float, from_name: str, to_name: str, prob: float) -> None:
        """Change one direction's loss `at_s` seconds from now -- a link
        that degrades and recovers over time (the 2026-09-18 drive-home
        capture: a repeater hop sliding from usable to dead and back)."""
        def _apply():
            self.link_loss[(from_name, to_name)] = prob
            self.log(f"[AIR] t+{at_s:.0f}s: loss {from_name}>{to_name} = {prob}")
        self._loop.call_soon_threadsafe(self._loop.call_later, at_s, _apply)

    def transmit(self, from_name: str, packet: SimPacket) -> None:
        """Thread-safe, fire-and-forget: the firmware's own send is too."""
        self._loop.call_soon_threadsafe(self._loop.create_task, self._transmit(from_name, packet))

    async def _transmit(self, from_name: str, packet: SimPacket) -> None:
        airtime = self.airtime_s(packet.size)
        # One radio, one transmission at a time: a send issued while this
        # node is still on air queues behind it (the firmware's outbound
        # queue), it doesn't overlap itself.
        start = max(time.monotonic(), self._busy_until.get(from_name, 0.0))
        end = start + airtime
        self._busy_until[from_name] = end
        self._tx_log[from_name].append((start, end))
        delay = start - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        self.stats.transmissions[from_name] += 1
        self.stats.by_type[packet.typename] += 1
        self.log(
            f"[AIR] {from_name} tx {packet.route}/{packet.typename} size={packet.size} "
            f"path={[f'{h:02x}' for h in packet.path]} id={packet.pkt_id} airtime={airtime*1000:.0f}ms"
        )
        for neighbor in self.adjacency.get(from_name, ()):
            self._loop.create_task(self._deliver(from_name, neighbor, packet, start, end))

    def _overlapping_transmitter(self, to_name: str, from_name: str, start: float, end: float) -> Optional[str]:
        """Another neighbor of `to_name` (not `from_name`) that was on air
        during [start, end] -> collision at `to_name`. No capture effect."""
        for other in self.adjacency.get(to_name, ()):
            if other == from_name:
                continue
            for (s, e) in self._tx_log.get(other, ()):
                if s < end and e > start:
                    return other
        return None

    async def _deliver(self, from_name: str, to_name: str, packet: SimPacket, start: float, end: float) -> None:
        await asyncio.sleep(max(0.0, end - time.monotonic()))
        # Receiver transmitted at some point after this packet started -> it
        # was deaf for part of it. Half-duplex, every node.
        for (s, e) in self._tx_log.get(to_name, ()):
            if s < end and e > start:
                self.stats.deaf_drops[to_name] += 1
                self.log(f"[AIR] {to_name} was transmitting (half-duplex) -- missed {packet.typename} from {from_name}.")
                return
        collider = self._overlapping_transmitter(to_name, from_name, start, end)
        if collider is not None:
            self.stats.collision_drops[to_name] += 1
            self.log(f"[AIR] collision at {to_name}: {packet.typename} from {from_name} overlapped a transmission from {collider}.")
            return
        if self.rng.random() < self.loss_for(from_name, to_name, packet.typename):
            self.stats.loss_drops[to_name] += 1
            self.log(f"[AIR] loss: {from_name} -> {to_name} {packet.typename} id={packet.pkt_id} dropped.")
            return
        receiver = self._receivers.get(to_name)
        if receiver is None:
            return
        self.stats.deliveries[to_name] += 1
        loop, callback = receiver
        if loop is None:
            callback(packet, from_name)
        else:
            loop.call_soon_threadsafe(callback, packet, from_name)


def parse_links(links) -> Dict[str, set]:
    """`["A-R1", "R1-B"]` -> adjacency dict."""
    adjacency: Dict[str, set] = {}
    for link in links:
        if "-" not in link:
            raise ValueError(f"link {link!r} must be of the form A-B")
        a, b = link.split("-", 1)
        adjacency.setdefault(a, set()).add(b)
        adjacency.setdefault(b, set()).add(a)
    return adjacency


def parse_type_loss(specs) -> Dict[str, float]:
    """`["ACK=1.0", "TEXT_MSG=0.2"]` -> per-payload-type loss."""
    out: Dict[str, float] = {}
    for spec in specs or ():
        try:
            name, prob = spec.split("=", 1)
            out[name.strip().upper()] = float(prob)
        except ValueError:
            raise ValueError(f"type loss {spec!r} must be of the form TYPENAME=probability")
    return out


def parse_loss_schedule(specs):
    """`["60:A>R1=1.0", "240:A>R1=0.0"]` -> [(at_s, from, to, prob), ...]."""
    out = []
    for spec in specs or ():
        try:
            at, rest = spec.split(":", 1)
            (pair, prob), = parse_link_loss([rest]).items()
            out.append((float(at), pair[0], pair[1], prob))
        except ValueError:
            raise ValueError(f"loss schedule {spec!r} must be of the form SECONDS:FROM>TO=probability")
    return out


def parse_link_loss(specs) -> Dict[Tuple[str, str], float]:
    """`["A>R1=0.3", "R1>A=0.0"]` -> directional loss overrides."""
    out: Dict[Tuple[str, str], float] = {}
    for spec in specs or ():
        try:
            pair, prob = spec.split("=", 1)
            a, b = pair.split(">", 1)
            out[(a, b)] = float(prob)
        except ValueError:
            raise ValueError(f"link loss {spec!r} must be of the form FROM>TO=probability")
    return out
