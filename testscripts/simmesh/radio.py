"""
SimRadio -- one node's MeshCore companion/repeater firmware, as far as
SmartMeshCoreInterface can observe it through the `meshcore` library.

Identity: pubkey = sha256(name) (32 bytes, like a real ed25519 key),
routing hash byte = pubkey[0], the 6-byte prefix the interface keys
peers on = pubkey[:6].

Modeled:
  - Contact table populated by ADVERT floods (auto-advert at attach by
    default -- real radios need a manual/periodic advert too). A contact
    learned from a zero-path advert is a zero-hop neighbor (out_path_len
    0); one learned via repeaters gets the reversed accumulated path.
  - Flood dedup: 160-slot ring of content hashes, no TTL, so an
    unchanged retransmit is silently absorbed by every node that already
    relayed the original. The interface's per-attempt content variation
    exists precisely to defeat this.
  - Flood relay (repeaters only): append own hash byte, retransmit after
    0-5x airtime of jitter (the firmware's own rule of thumb).
  - DIRECT forwarding (repeaters only): forward iff path[0] == own hash,
    strip it, small jitter. Companions never forward.
  - TXT_MSG receive: only the addressed node can decrypt, and only if the
    sender is a known contact (no contact, no shared secret -> dropped).
    Delivery queues the message and pushes MESSAGES_WAITING; the app must
    drain with get_msg() -- the exact queue-then-poll semantics the
    interface once shipped without and lost every incoming message to.
    An ACK is generated for every decrypted TXT_MSG and routed back via
    the contact's known path (DIRECT) or flood.
  - Path learning: a flood-routed TXT_MSG or path REQ teaches the
    receiver the reversed path to the sender.
  - Path discovery: REQ floods; the target answers with the accumulated
    path, DIRECT along its reverse, only if the requester's contact entry
    carries the telemetry-base permission bit (telemetry_mode_base 1) --
    this is what the interface's change_contact_flags grant is for. The
    requester's *contact record is not updated* by a response: the
    interface's own docstring notes the firmware never persists it, and
    calls change_contact_path itself.
  - RX_LOG_DATA for every packet heard on air, addressed or not, with the
    fields the interface's rx-window correlation reads.

Not modeled: encryption itself, GRP_DATA, multipart, trace, transport
codes, contact-table capacity limits, flash persistence.
"""
import asyncio
import collections
import hashlib
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

from .air import (
    ROUTE_DIRECT, ROUTE_FLOOD, ROUTE_TYPE_CODES, MAX_PATH_HASHES,
    PTYPE_ACK, PTYPE_ADVERT, PTYPE_GRP_TXT, PTYPE_PATH, PTYPE_RAW_CUSTOM, PTYPE_REQ, PTYPE_TXT_MSG,
    SimPacket,
)

TELEM_PERM_BASE_FLAG_BIT = 0x02
DEDUP_RING_SIZE = 160  # SimpleMeshTables' real size


def node_pubkey(name: str) -> str:
    return hashlib.sha256(name.encode("utf-8")).hexdigest()


def node_hash_byte(name: str) -> int:
    return int(node_pubkey(name)[:2], 16)


def node_prefix(name: str) -> str:
    return node_pubkey(name)[:12]


@dataclass
class RadioOptions:
    auto_advert: bool = True
    # Real repeaters/companions re-advert on a slow schedule; a late joiner
    # only learns a neighbor from the next one. SimMesh.advert_all() is the
    # explicit "press advert on every radio" step a field test starts with.
    advert_interval_s: float = 120.0
    advert_teaches_path: bool = True
    learn_paths_on_receive: bool = True
    require_telemetry_permission: bool = True
    flood_relay_jitter_airtimes: float = 5.0
    direct_relay_jitter_airtimes: float = 1.0
    # The companion firmware's own suggested_timeout arithmetic
    # (examples/companion_radio/MyMesh.cpp calcDirectTimeoutMillisFor /
    # calcFloodTimeoutMillisFor):
    #   direct: BASE + (airtime * PERHOP_FACTOR + PERHOP_EXTRA) * (hops + 1)
    #   flood:  BASE + FLOOD_FACTOR * airtime
    # With a calibrated airtime (~0.7-0.9 s for a full text frame at the
    # field radios' settings) this reproduces the 2026-09-18 captures'
    # ~6 / 10.6 / 16 / 28 s at 0 / 1 / 2 / 3 hops.
    timeout_base_ms: float = 500.0
    timeout_direct_perhop_factor: float = 6.0
    timeout_direct_perhop_extra_ms: float = 250.0
    timeout_flood_factor: float = 16.0


class SimRadio:
    def __init__(self, name: str, air, is_repeater: bool = False, options: Optional[RadioOptions] = None, rng=None):
        self.name = name
        self.air = air
        self.is_repeater = is_repeater
        self.options = options or RadioOptions()
        self.rng = rng if rng is not None else air.rng
        self.pubkey = node_pubkey(name)
        self.hash_byte = node_hash_byte(name)
        self.prefix = node_prefix(name)

        self.contacts: Dict[str, dict] = {}
        self.telemetry_mode_base = 0
        self.channel_idx = 0
        self._seen = collections.deque(maxlen=DEDUP_RING_SIZE)
        self._inbox = collections.deque()
        self._pending_acks: Dict[str, float] = {}
        self._push: Optional[Callable[[str, dict, dict], None]] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.counters = collections.Counter()
        self.log = getattr(air, "log", None) or (lambda msg: None)

    # -- wiring --------------------------------------------------------------

    def attach(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self.loop = loop if loop is not None else self.air.loop
        self.air.attach(self.name, self.on_air, loop)
        self._detached = False
        if self.options.auto_advert:
            self.cmd_send_advert()
        if self.options.advert_interval_s > 0:
            self.loop.call_soon_threadsafe(self._schedule_periodic_advert)

    def _schedule_periodic_advert(self) -> None:
        if getattr(self, "_detached", False):
            return
        delay = self.options.advert_interval_s * self.rng.uniform(0.8, 1.2)
        self.loop.call_later(delay, self._periodic_advert)

    def _periodic_advert(self) -> None:
        if getattr(self, "_detached", False):
            return
        self.cmd_send_advert()
        self._schedule_periodic_advert()

    def advert_later(self, delay: float) -> None:
        """Thread-safe: advert after `delay` seconds on this radio's loop."""
        self.loop.call_soon_threadsafe(self.loop.call_later, delay, self.cmd_send_advert)

    def detach(self) -> None:
        self._detached = True
        self.air.detach(self.name)

    def set_push_handler(self, handler: Callable[[str, dict, dict], None]) -> None:
        """`handler(event_name, payload, attributes)` -- the fake library
        turns these into dispatched Events. Repeaters have none."""
        self._push = handler

    def _push_event(self, name: str, payload: dict, attributes: Optional[dict] = None) -> None:
        if self._push is not None:
            self._push(name, payload, attributes or {})

    def _later(self, delay: float, fn, *args) -> None:
        self.loop.call_later(delay, fn, *args)

    def _tx(self, packet: SimPacket) -> None:
        self.air.transmit(self.name, packet)

    # -- contact helpers -------------------------------------------------------

    def _contact_for_prefix(self, prefix_hex: str) -> Optional[dict]:
        prefix_hex = prefix_hex.lower()
        for key, contact in self.contacts.items():
            if key.startswith(prefix_hex):
                return contact
        return None

    def _upsert_contact(self, pubkey: str, adv_name: str, path=None, teach_path: bool = True) -> dict:
        contact = self.contacts.get(pubkey)
        if contact is None:
            contact = {
                "public_key": pubkey, "type": 1, "flags": 0,
                "out_path_len": -1, "out_path_hash_mode": 0, "out_path": "",
                "adv_name": adv_name, "last_advert": int(time.time()), "lastmod": int(time.time()),
                "adv_lat": 0, "adv_lon": 0,
            }
            self.contacts[pubkey] = contact
            self.counters["contacts_added"] += 1
        else:
            contact["adv_name"] = adv_name
            contact["last_advert"] = int(time.time())
        if teach_path and path is not None:
            self._set_contact_path(contact, path)
        return contact

    @staticmethod
    def _set_contact_path(contact: dict, path) -> None:
        path = list(path)[:MAX_PATH_HASHES]
        contact["out_path_len"] = len(path)
        contact["out_path"] = "".join(f"{h:02x}" for h in path)
        contact["out_path_hash_mode"] = 0
        contact["lastmod"] = int(time.time())

    @staticmethod
    def _contact_route(contact: dict):
        """(route, path) the firmware would use to reach this contact."""
        if contact.get("out_path_len", -1) >= 0:
            hex_path = contact.get("out_path", "")
            return ROUTE_DIRECT, tuple(int(hex_path[i:i + 2], 16) for i in range(0, len(hex_path), 2))
        return ROUTE_FLOOD, ()

    # -- suggested timeouts (approximation) --------------------------------------

    def _airtime_ms(self, size: int) -> float:
        return self.air.airtime_s(size) * 1000.0

    def _suggested_timeout_ms(self, route: str, path_len: int, size: int) -> int:
        est = self._airtime_ms(size)
        o = self.options
        if route == ROUTE_DIRECT:
            return int(o.timeout_base_ms + (est * o.timeout_direct_perhop_factor + o.timeout_direct_perhop_extra_ms) * (path_len + 1))
        return int(o.timeout_base_ms + o.timeout_flood_factor * est)

    # -- commands (called by the fake library, on self.loop) ----------------------

    def cmd_send_advert(self) -> None:
        self.counters["adverts_sent"] += 1
        pkt = SimPacket(
            route=ROUTE_FLOOD, ptype=PTYPE_ADVERT, src=self.name, src_hash=self.hash_byte, dst=None, dst_hash=None,
            body={"pubkey": self.pubkey, "name": self.name, "ts": int(time.time()),
                  "seq": self.counters["adverts_sent"], "repeater": self.is_repeater},
        )
        self._seen.append(pkt.pkt_id)
        self._tx(pkt)

    def cmd_send_chan_msg(self, channel_idx: int, text: str, ts: Optional[int] = None) -> None:
        # The firmware prepends "<name>: " exactly once at the origin;
        # repeaters relay the text verbatim.
        #
        # `ts` is injectable purely so a test can send the SAME packet twice
        # and actually get the same `pkt_id` (audit fix, 2026-09-19): the id
        # is a content hash, and with `int(time.time())` inside the body two
        # "identical" sends straddling a wall-clock second boundary hashed
        # differently, so the flood-dedup test failed intermittently
        # (observed 2 of 3 runs). The real firmware timestamps the same way;
        # this only removes the race from the test's control flow.
        pkt = SimPacket(
            route=ROUTE_FLOOD, ptype=PTYPE_GRP_TXT, src=self.name, src_hash=self.hash_byte, dst=None, dst_hash=None,
            body={"chan": channel_idx, "text": f"{self.name}: {text}",
                  "ts": int(time.time()) if ts is None else int(ts)},
        )
        self._seen.append(pkt.pkt_id)
        self._tx(pkt)

    def cmd_send_msg(self, dst_pubkey_hex: str, text: str, attempt: int = 0) -> Optional[dict]:
        """Returns the MSG_SENT payload, or None when `dst` isn't a contact
        (the real command errors: no contact, no shared secret)."""
        contact = self._contact_for_prefix(dst_pubkey_hex)
        if contact is None:
            return None
        dst_name = contact["adv_name"]
        ts = int(time.time())
        ack = hashlib.sha256(f"{self.pubkey}|{contact['public_key']}|{ts}|{attempt}|{text}".encode()).hexdigest()[:8]
        route, path = self._contact_route(contact)
        pkt = SimPacket(
            route=route, ptype=PTYPE_TXT_MSG, src=self.name, src_hash=self.hash_byte,
            dst=dst_name, dst_hash=int(contact["public_key"][:2], 16),
            body={"text": text, "ts": ts, "attempt": attempt, "ack": ack}, path=path,
        )
        self._seen.append(pkt.pkt_id)
        self._pending_acks[ack] = time.monotonic()
        self._tx(pkt)
        self.counters["txt_sent_" + route.lower()] += 1
        return {
            "type": 1 if route == ROUTE_DIRECT else 0,
            "expected_ack": bytes.fromhex(ack),
            "suggested_timeout": self._suggested_timeout_ms(route, len(path), pkt.size),
        }

    def cmd_send_raw_data(self, path: bytes, payload: bytes) -> bool:
        """CMD_SEND_RAW_DATA (25): `Mesh::createRawData` + `sendDirect(path)`.
        Source-routed DIRECT, no ACK, no encryption; delivered to every
        node that hears it with the path exhausted (`Mesh.cpp`
        PAYLOAD_TYPE_RAW_CUSTOM: markSeen + onRawDataRecv). Firmware
        limits: payload <= 174 - path_len on this frame, >= 4 bytes."""
        if len(payload) < 4 or len(payload) + len(path) + 2 > 176:
            return False
        pkt = SimPacket(
            route=ROUTE_DIRECT, ptype=PTYPE_RAW_CUSTOM, src=self.name, src_hash=self.hash_byte, dst=None, dst_hash=None,
            body={"payload": bytes(payload).hex()}, path=tuple(path), size=2 + len(path) + len(payload),
        )
        self._seen.append(pkt.pkt_id)
        self.counters["raw_sent"] += 1
        self._tx(pkt)
        return True

    def cmd_send_path_discovery(self, dst_pubkey_hex: str) -> Optional[dict]:
        contact = self._contact_for_prefix(dst_pubkey_hex)
        if contact is None:
            return None
        pkt = SimPacket(
            route=ROUTE_FLOOD, ptype=PTYPE_REQ, src=self.name, src_hash=self.hash_byte,
            dst=contact["adv_name"], dst_hash=int(contact["public_key"][:2], 16),
            body={"kind": "path_discovery", "ts": int(time.time()), "nonce": self.rng.random()},
        )
        self._seen.append(pkt.pkt_id)
        self._tx(pkt)
        self.counters["path_req_sent"] += 1
        return {"type": 0, "expected_ack": b"\x00\x00\x00\x00",
                "suggested_timeout": self._suggested_timeout_ms(ROUTE_FLOOD, 0, pkt.size)}

    def cmd_get_contacts(self) -> Dict[str, dict]:
        return {k: dict(v) for k, v in self.contacts.items()}

    def cmd_change_contact_flags(self, pubkey_hex: str, flags: int) -> bool:
        contact = self._contact_for_prefix(pubkey_hex)
        if contact is None:
            return False
        contact["flags"] = int(flags)
        contact["lastmod"] = int(time.time())
        return True

    def cmd_change_contact_path(self, pubkey_hex: str, path_hex: str, path_hash_mode=None) -> bool:
        contact = self._contact_for_prefix(pubkey_hex)
        if contact is None:
            return False
        path = [int(path_hex[i:i + 2], 16) for i in range(0, len(path_hex or ""), 2)]
        self._set_contact_path(contact, path)
        return True

    def cmd_reset_path(self, pubkey_hex: str) -> bool:
        contact = self._contact_for_prefix(pubkey_hex)
        if contact is None:
            return False
        contact["out_path_len"] = -1
        contact["out_path"] = ""
        contact["lastmod"] = int(time.time())
        return True

    def cmd_get_msg(self):
        """Pops one queued message: ("CONTACT"|"CHANNEL", payload) or None."""
        if not self._inbox:
            return None
        return self._inbox.popleft()

    # -- receive path (on self.loop) ---------------------------------------------

    def on_air(self, packet: SimPacket, from_name: str) -> None:
        self.counters["heard"] += 1
        self._push_rx_log(packet)

        if packet.route == ROUTE_DIRECT:
            self._on_direct(packet)
            return

        if packet.pkt_id in self._seen:
            if packet.src == self.name:
                self.counters["own_echo_heard"] += 1  # a repeater relaying our own packet back
            else:
                self.counters["dedup_dropped"] += 1
                self.log(f"[{self.name}] already saw {packet.typename} id={packet.pkt_id} -- deduped.")
            return
        self._seen.append(packet.pkt_id)

        self._consume(packet)

        if self.is_repeater and len(packet.path) < MAX_PATH_HASHES:
            delay = self.rng.uniform(0, self.options.flood_relay_jitter_airtimes) * self.air.airtime_s(packet.size)
            self._later(delay, self._tx, packet.with_path(packet.path + (self.hash_byte,)))

    def _on_direct(self, packet: SimPacket) -> None:
        if packet.path:
            if packet.path[0] != self.hash_byte:
                return  # not the next hop
            if not self.is_repeater:
                return  # companions never forward
            delay = self.rng.uniform(0, self.options.direct_relay_jitter_airtimes) * self.air.airtime_s(packet.size)
            self.counters["direct_forwarded"] += 1
            self._later(delay, self._tx, packet.with_path(packet.path[1:]))
            return
        self._consume(packet)

    def _consume(self, packet: SimPacket) -> None:
        ptype = packet.ptype
        if ptype == PTYPE_ADVERT:
            self._on_advert(packet)
        elif ptype == PTYPE_GRP_TXT:
            self._on_group_text(packet)
        elif ptype == PTYPE_TXT_MSG:
            self._on_text_msg(packet)
        elif ptype == PTYPE_ACK:
            self._on_ack(packet)
        elif ptype == PTYPE_REQ:
            self._on_path_request(packet)
        elif ptype == PTYPE_PATH:
            self._on_path_response(packet)
        elif ptype == PTYPE_RAW_CUSTOM:
            self._on_raw_custom(packet)

    def _on_advert(self, packet: SimPacket) -> None:
        if packet.src == self.name:
            return
        teach = self.options.advert_teaches_path and packet.body["pubkey"] not in self.contacts
        self._upsert_contact(packet.body["pubkey"], packet.body["name"], path=tuple(reversed(packet.path)), teach_path=teach)
        self._push_event("ADVERTISEMENT", {"public_key": packet.body["pubkey"], "adv_name": packet.body["name"]})
        self._push_event("NEW_CONTACT", dict(self.contacts[packet.body["pubkey"]]))

    def _on_group_text(self, packet: SimPacket) -> None:
        if packet.body.get("chan") != self.channel_idx:
            return
        self._enqueue("CHANNEL", {
            "type": "CHAN", "channel_idx": packet.body["chan"], "path_len": len(packet.path),
            "path_hash_mode": 0, "txt_type": 0, "sender_timestamp": packet.body["ts"], "text": packet.body["text"],
        })

    def _on_text_msg(self, packet: SimPacket) -> None:
        if packet.dst != self.name:
            return
        sender = self._contact_for_prefix(node_pubkey(packet.src)) if packet.src != self.name else None
        if sender is None:
            self.counters["txt_from_unknown_contact_dropped"] += 1
            self.log(f"[{self.name}] TXT_MSG from {packet.src} but no contact for it -- cannot decrypt, dropped.")
            return
        if packet.route == ROUTE_FLOOD and self.options.learn_paths_on_receive:
            self._set_contact_path(sender, reversed(packet.path))
            self._push_event("PATH_UPDATE", {"public_key": sender["public_key"], "out_path_len": sender["out_path_len"]})
        self.counters["txt_received"] += 1
        self._enqueue("CONTACT", {
            "type": "PRIV", "pubkey_prefix": sender["public_key"][:12], "path_len": len(packet.path) if packet.route == ROUTE_FLOOD else 255,
            "path_hash_mode": 0 if packet.route == ROUTE_FLOOD else -1, "txt_type": 0,
            "sender_timestamp": packet.body["ts"], "text": packet.body["text"],
        })
        route, path = self._contact_route(sender)
        ack = SimPacket(
            route=route, ptype=PTYPE_ACK, src=self.name, src_hash=self.hash_byte,
            dst=packet.src, dst_hash=packet.src_hash, body={"ack": packet.body["ack"], "for": packet.pkt_id}, path=path,
        )
        self._seen.append(ack.pkt_id)
        self.counters["ack_sent_" + route.lower()] += 1
        self._tx(ack)

    def _on_raw_custom(self, packet: SimPacket) -> None:
        """PAYLOAD_TYPE_RAW_CUSTOM at a node whose path is exhausted: the
        firmware dedups it (`wasSeen`), so a byte-identical retry is
        dropped, then pushes PUSH_CODE_RAW_DATA (SNR, RSSI, reserved,
        payload) -- no ACK, no sender identity."""
        if packet.pkt_id in self._seen:
            self.counters["raw_dedup_dropped"] += 1
            return
        self._seen.append(packet.pkt_id)
        if packet.src == self.name:
            return
        self.counters["raw_received"] += 1
        self._push_event("RAW_DATA", {
            "SNR": round(self.rng.uniform(6.0, 12.0), 2), "RSSI": -50 - 12 * len(packet.path),
            "payload": packet.body["payload"],
        })

    def _on_ack(self, packet: SimPacket) -> None:
        code = packet.body.get("ack")
        sent_at = self._pending_acks.pop(code, None)
        if sent_at is None:
            return
        self.counters["ack_matched"] += 1
        trip_ms = int((time.monotonic() - sent_at) * 1000)
        self._push_event("ACK", {"code": code, "trip_time": trip_ms}, {"code": code})

    def _on_path_request(self, packet: SimPacket) -> None:
        if packet.dst != self.name or packet.body.get("kind") != "path_discovery":
            return
        requester = self._contact_for_prefix(node_pubkey(packet.src))
        if requester is None:
            self.counters["path_req_from_unknown_contact"] += 1
            return
        if self.options.learn_paths_on_receive and packet.route == ROUTE_FLOOD:
            self._set_contact_path(requester, reversed(packet.path))
        if self.options.require_telemetry_permission:
            if self.telemetry_mode_base == 0 or (
                self.telemetry_mode_base == 1 and not (requester.get("flags", 0) & TELEM_PERM_BASE_FLAG_BIT)
            ):
                self.counters["path_req_denied_no_permission"] += 1
                self.log(f"[{self.name}] path discovery from {packet.src} denied -- no telemetry permission granted.")
                return
        self.counters["path_resp_sent"] += 1
        out_path = tuple(packet.path)  # requester -> me, as accumulated
        resp = SimPacket(
            route=ROUTE_DIRECT, ptype=PTYPE_PATH, src=self.name, src_hash=self.hash_byte,
            dst=packet.src, dst_hash=packet.src_hash,
            body={"pubkey_pre": self.pubkey[:12], "out_path": "".join(f"{h:02x}" for h in out_path),
                  "out_path_len": len(out_path), "nonce": packet.body.get("nonce")},
            path=tuple(reversed(out_path)),
        )
        self._seen.append(resp.pkt_id)
        self._tx(resp)

    def _on_path_response(self, packet: SimPacket) -> None:
        if packet.dst != self.name:
            return
        self.counters["path_resp_received"] += 1
        body = packet.body
        # Deliberately no contact update here -- see module docstring.
        self._push_event("PATH_RESPONSE", {
            "pubkey_pre": body["pubkey_pre"], "out_path": body["out_path"], "out_path_len": body["out_path_len"],
            "out_path_hash_len": 1, "in_path": "", "in_path_len": 0, "in_path_hash_len": 1,
        }, {"pubkey_pre": body["pubkey_pre"]})

    # -- app-facing queue and RX log ---------------------------------------------

    def _enqueue(self, kind: str, payload: dict) -> None:
        self._inbox.append((kind, payload))
        self._push_event("MESSAGES_WAITING", {})

    def _push_rx_log(self, packet: SimPacket) -> None:
        if self._push is None:
            return
        if packet.ptype == PTYPE_ACK:
            pkt_payload = bytes.fromhex(packet.body["ack"])
        elif packet.ptype == PTYPE_RAW_CUSTOM:
            pkt_payload = bytes.fromhex(packet.body["payload"])[:4]
        elif packet.dst_hash is not None:
            pkt_payload = bytes([packet.dst_hash, packet.src_hash])
        else:
            pkt_payload = b""
        path_hex = "".join(f"{h:02x}" for h in packet.path)
        payload = {
            "snr": round(self.rng.uniform(6.0, 12.0), 2), "rssi": -50 - 12 * len(packet.path),
            "route_type": ROUTE_TYPE_CODES[packet.route], "route_typename": packet.route,
            "payload_type": packet.ptype, "payload_typename": packet.typename, "payload_ver": 0,
            "path_len": len(packet.path), "path_hash_size": 1, "path": path_hex,
            "payload_length": packet.size, "pkt_payload": pkt_payload,
            "pkt_hash": int(packet.pkt_id[:8], 16), "recv_time": int(time.time()),
        }
        self._push_event("RX_LOG_DATA", payload, {
            "route_type": payload["route_type"], "payload_type": packet.ptype,
            "path_len": len(packet.path), "path": path_hex, "recv_time": payload["recv_time"],
        })
