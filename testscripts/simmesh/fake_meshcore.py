"""
A stand-in for the `meshcore` Python library (2.3.9.x), backed by a
SimRadio. Only the surface SmartMeshCoreInterface actually uses is
implemented, but that surface mirrors the real library's *semantics*,
not just its names, because several field-diagnosed interface bugs were
library-semantics bugs:

  - Incoming messages are NOT pushed. The radio queues them and pushes
    MESSAGES_WAITING; CONTACT_MSG_RECV/CHANNEL_MSG_RECV only fire from a
    get_msg() drain, which start_auto_message_fetching() wires up
    (meshcore.py). Skip that call and nothing ever arrives.
  - ensure_contacts(follow=False) is a no-op after the first fetch;
    only follow=True consults the _contacts_dirty flag that ADVERTISEMENT/
    PATH_UPDATE set (meshcore.py _contact_change).
  - wait_for_event matches on bare type plus optional attribute filters;
    ACK correlates on attributes["code"] (reader.py), PATH_RESPONSE on
    attributes["pubkey_pre"]. Type-only waits can steal each other's
    replies exactly as the interface's invariant #2 warns.
  - send_path_discovery_sync waits suggested_timeout/800 seconds
    (messaging.py, sic) for a PATH_RESPONSE that is not peer-filtered.
  - send_msg returns MSG_SENT with `expected_ack` (bytes) and
    `suggested_timeout` (ms); ERROR when the destination isn't a contact.
"""
import asyncio
import enum
import types
from typing import Any, Callable, Dict, List, Optional

from .radio import SimRadio


class EventType(enum.Enum):
    OK = "ok"
    ERROR = "error"
    CONTACTS = "contacts"
    SELF_INFO = "self_info"
    CONTACT_MSG_RECV = "contact_message"
    CHANNEL_MSG_RECV = "channel_message"
    NO_MORE_MSGS = "no_more_messages"
    MSG_SENT = "message_sent"
    NEW_CONTACT = "new_contact"
    NEXT_CONTACT = "next_contact"
    ADVERTISEMENT = "advertisement"
    PATH_UPDATE = "path_update"
    ACK = "acknowledgement"
    MESSAGES_WAITING = "messages_waiting"
    PATH_RESPONSE = "path_response"
    RX_LOG_DATA = "rx_log_data"
    RAW_DATA = "raw_data"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    DEVICE_INFO = "device_info"
    BATTERY = "battery_info"


class SimEvent:
    def __init__(self, type: EventType, payload: Any = None, attributes: Optional[dict] = None):
        self.type = type
        self.payload = payload if payload is not None else {}
        self.attributes = attributes or {}

    def __repr__(self) -> str:
        return f"SimEvent({self.type.name}, {self.payload!r})"


class _Subscription:
    __slots__ = ("event_type", "callback", "filters")

    def __init__(self, event_type, callback, filters):
        self.event_type = event_type
        self.callback = callback
        self.filters = filters or {}

    def matches(self, event: SimEvent) -> bool:
        if event.type != self.event_type:
            return False
        return all(event.attributes.get(k) == v for k, v in self.filters.items())


class SimDispatcher:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self._subs: List[_Subscription] = []

    def subscribe(self, event_type, callback, attribute_filters=None) -> _Subscription:
        sub = _Subscription(event_type, callback, attribute_filters)
        self._subs.append(sub)
        return sub

    def unsubscribe(self, sub) -> None:
        try:
            self._subs.remove(sub)
        except ValueError:
            pass

    async def dispatch(self, event: SimEvent) -> None:
        for sub in list(self._subs):
            if not sub.matches(event):
                continue
            result = sub.callback(event)
            if asyncio.iscoroutine(result):
                await result

    def dispatch_soon(self, event: SimEvent) -> None:
        self.loop.create_task(self.dispatch(event))

    async def wait_for_event(self, event_type, attribute_filters=None, timeout=None) -> Optional[SimEvent]:
        fut = self.loop.create_future()

        def handler(event):
            if not fut.done():
                fut.set_result(event)

        sub = self.subscribe(event_type, handler, attribute_filters)
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self.unsubscribe(sub)


class SimCommands:
    def __init__(self, mc: "SimMeshCore"):
        self._mc = mc
        self._radio = mc.radio
        self._mesh_request_lock = asyncio.Lock()

    async def send_appstart(self) -> SimEvent:
        return SimEvent(EventType.SELF_INFO, {
            "name": self._radio.name, "public_key": self._radio.pubkey, "adv_type": 1,
            "tx_power": 20, "max_tx_power": 22, "radio_freq": 915.5, "radio_bw": 250, "radio_sf": 10, "radio_cr": 5,
        })

    async def set_radio(self, freq, bw, sf, cr, repeat=None) -> SimEvent:
        return SimEvent(EventType.OK, {})

    async def set_channel(self, channel_idx, channel_name, channel_secret=None) -> SimEvent:
        self._radio.channel_idx = int(channel_idx)
        return SimEvent(EventType.OK, {})

    async def set_telemetry_mode_base(self, telemetry_mode_base) -> SimEvent:
        self._radio.telemetry_mode_base = int(telemetry_mode_base)
        return SimEvent(EventType.OK, {})

    async def send_advert(self, flood: bool = False) -> SimEvent:
        self._radio.cmd_send_advert()
        return SimEvent(EventType.OK, {})

    async def send_chan_msg(self, chan, msg, timestamp=None) -> SimEvent:
        self._radio.cmd_send_chan_msg(int(chan), msg)
        return SimEvent(EventType.OK, {})

    async def send_msg(self, dst, msg, timestamp=None, attempt=0) -> SimEvent:
        dst_hex = dst["public_key"] if isinstance(dst, dict) else (dst.hex() if isinstance(dst, bytes) else str(dst))
        result = self._radio.cmd_send_msg(dst_hex, msg, attempt=int(attempt))
        if result is None:
            return SimEvent(EventType.ERROR, {"reason": "destination is not a known contact"})
        return SimEvent(EventType.MSG_SENT, result, {"type": result["type"], "expected_ack": result["expected_ack"].hex()})

    async def send(self, data: bytes, expected_events=None) -> SimEvent:
        """`CommandHandlerBase.send(data, expected_events)` -- the raw
        command frame the library's own `send_msg`/`send_cmd` build
        (2026-09-20). Only CMD_SEND_TXT_MSG (0x02) is modelled:
        `[0x02][txt_type][attempt][timestamp:4 LE][dst_prefix:6][text]`,
        the frame `MyMesh::onSerialFrame` parses; txt_type 0 (PLAIN) is
        ACKed, 1 (CLI_DATA) is delivered and never ACKed, anything else
        errors as the firmware does (`recipient && (PLAIN || CLI_DATA)`)."""
        data = bytes(data)
        if not data or data[0] != 0x02 or len(data) < 13:
            return SimEvent(EventType.ERROR, {"reason": "unsupported command frame in the fake"})
        txt_type, attempt = data[1], data[2]
        dst_hex = data[7:13].hex()
        text = data[13:].decode("utf-8", "ignore")
        result = self._radio.cmd_send_msg(dst_hex, text, attempt=int(attempt), txt_type=int(txt_type))
        if result is None:
            return SimEvent(EventType.ERROR, {"reason": "destination is not a known contact or unsupported txt_type"})
        return SimEvent(EventType.MSG_SENT, result, {"type": result["type"], "expected_ack": result["expected_ack"].hex()})

    async def send_raw_data(self, payload: bytes, path: bytes = b"") -> SimEvent:
        """meshcore 2.3.9.1 commands/messaging.py send_raw_data: payload
        bytes (>= 4) and optional path bytes; resolves OK or ERROR."""
        if not isinstance(payload, (bytes, bytearray)):
            raise TypeError("payload must be bytes-like")
        if len(payload) < 4:
            raise ValueError("payload must be at least 4 bytes")
        ok = self._radio.cmd_send_raw_data(bytes(path), bytes(payload))
        return SimEvent(EventType.OK, {}) if ok else SimEvent(EventType.ERROR, {"reason": "raw payload too large"})

    async def _send_path_discovery_raw(self, dst) -> SimEvent:
        dst_hex = dst["public_key"] if isinstance(dst, dict) else str(dst)
        result = self._radio.cmd_send_path_discovery(dst_hex)
        if result is None:
            return SimEvent(EventType.ERROR, {"reason": "destination is not a known contact"})
        return SimEvent(EventType.MSG_SENT, result)

    async def send_path_discovery_sync(self, dst, timeout=0, min_timeout=0) -> Optional[SimEvent]:
        # Mirrors messaging.py exactly, including the /800 quirk.
        async with self._mesh_request_lock:
            result = await self._send_path_discovery_raw(dst)
            if result is None or result.type == EventType.ERROR:
                return None
            timeout = result.payload["suggested_timeout"] / 800 if timeout == 0 else timeout
            timeout = timeout if timeout > min_timeout else min_timeout
            return await self._mc.dispatcher.wait_for_event(EventType.PATH_RESPONSE, timeout=timeout)

    async def get_contacts(self, lastmod=0, timeout=5) -> SimEvent:
        contacts = self._radio.cmd_get_contacts()
        event = SimEvent(EventType.CONTACTS, contacts, {"lastmod": max([c.get("lastmod", 0) for c in contacts.values()] or [0])})
        await self._mc.dispatcher.dispatch(event)
        return event

    async def change_contact_flags(self, contact, flags) -> SimEvent:
        ok = self._radio.cmd_change_contact_flags(contact["public_key"], flags)
        if ok:
            contact["flags"] = flags
        return SimEvent(EventType.OK if ok else EventType.ERROR, {})

    async def change_contact_path(self, contact, path, path_hash_mode=None) -> SimEvent:
        ok = self._radio.cmd_change_contact_path(contact["public_key"], path, path_hash_mode)
        if ok:
            contact["out_path"] = path
            contact["out_path_len"] = len(path) // 2
        return SimEvent(EventType.OK if ok else EventType.ERROR, {})

    async def reset_path(self, contact) -> SimEvent:
        # The real command mutates the local contact dict before the device
        # round-trip resolves (noted in the interface's own _reset_stale_path).
        key = contact["public_key"] if isinstance(contact, dict) else str(contact)
        if isinstance(contact, dict):
            contact["out_path_len"] = -1
            contact["out_path"] = ""
        ok = self._radio.cmd_reset_path(key)
        return SimEvent(EventType.OK if ok else EventType.ERROR, {})

    async def get_msg(self, timeout=None) -> SimEvent:
        item = self._radio.cmd_get_msg()
        if item is None:
            event = SimEvent(EventType.NO_MORE_MSGS, {})
        else:
            kind, payload = item
            event = SimEvent(EventType.CONTACT_MSG_RECV if kind == "CONTACT" else EventType.CHANNEL_MSG_RECV, payload)
        await self._mc.dispatcher.dispatch(event)
        return event


class SimMeshCore:
    """Stand-in for a connected meshcore.MeshCore, bound to one SimRadio.
    Must be created on the event loop the interface runs it from."""

    def __init__(self, radio: SimRadio):
        self.radio = radio
        self.loop = asyncio.get_running_loop()
        self.dispatcher = SimDispatcher(self.loop)
        self.commands = SimCommands(self)
        self._contacts: Dict[str, dict] = {}
        self._contacts_dirty = True
        self._auto_fetch_task = None
        self._auto_fetch_running = False
        self._auto_fetch_subscription = None
        self.subscribe(EventType.CONTACTS, self._update_contacts)
        self.subscribe(EventType.ADVERTISEMENT, self._contact_change)
        self.subscribe(EventType.PATH_UPDATE, self._contact_change)
        radio.set_push_handler(self._on_radio_push)
        radio.attach(self.loop)

    # -- radio -> events --------------------------------------------------------

    def _on_radio_push(self, name: str, payload: dict, attributes: dict) -> None:
        self.dispatcher.dispatch_soon(SimEvent(EventType[name], payload, attributes))

    # -- library state tracking (meshcore.py _setup_data_tracking) --------------

    async def _update_contacts(self, event: SimEvent) -> None:
        for c in event.payload.values():
            if c["public_key"] in self._contacts:
                self._contacts[c["public_key"]].update(c)
            else:
                self._contacts[c["public_key"]] = c
        self._contacts_dirty = False

    async def _contact_change(self, event: SimEvent) -> None:
        self._contacts_dirty = True

    @property
    def contacts(self) -> Dict[str, dict]:
        return self._contacts

    @property
    def contacts_dirty(self) -> bool:
        return self._contacts_dirty

    async def ensure_contacts(self, follow: bool = False) -> bool:
        if not self._contacts or (follow and self._contacts_dirty):
            await self.commands.get_contacts()
            return True
        return False

    def get_contact_by_key_prefix(self, prefix: str) -> Optional[dict]:
        if not self._contacts or not prefix:
            return None
        prefix = prefix.lower()
        for key, contact in self._contacts.items():
            if key.startswith(prefix):
                return contact
        return None

    # -- events ----------------------------------------------------------------

    def subscribe(self, event_type, callback, attribute_filters=None):
        return self.dispatcher.subscribe(event_type, callback, attribute_filters)

    def unsubscribe(self, subscription) -> None:
        self.dispatcher.unsubscribe(subscription)

    async def wait_for_event(self, event_type, attribute_filters=None, timeout=None) -> Optional[SimEvent]:
        return await self.dispatcher.wait_for_event(event_type, attribute_filters, timeout)

    # -- message fetching (meshcore.py start_auto_message_fetching) -------------

    async def start_auto_message_fetching(self):
        self._auto_fetch_task = None
        self._auto_fetch_running = True

        async def _fetch_messages_loop():
            while self._auto_fetch_running:
                result = await self.commands.get_msg()
                if result.type in (EventType.NO_MORE_MSGS, EventType.ERROR):
                    break
                await asyncio.sleep(0.01)

        async def _handle_messages_waiting(event):
            if not self._auto_fetch_task or self._auto_fetch_task.done():
                self._auto_fetch_task = asyncio.create_task(_fetch_messages_loop())

        self._auto_fetch_subscription = self.subscribe(EventType.MESSAGES_WAITING, _handle_messages_waiting)
        await self.commands.get_msg()
        return self._auto_fetch_subscription

    async def stop_auto_message_fetching(self) -> None:
        if self._auto_fetch_subscription is not None:
            self.unsubscribe(self._auto_fetch_subscription)
            self._auto_fetch_subscription = None
        self._auto_fetch_running = False
        if self._auto_fetch_task is not None and not self._auto_fetch_task.done():
            self._auto_fetch_task.cancel()

    async def disconnect(self) -> None:
        await self.stop_auto_message_fetching()
        self.radio.detach()


def make_fake_meshcore_module(radio_factory: Callable[[], SimRadio]) -> types.ModuleType:
    """Builds a module object to install as sys.modules["meshcore"] for
    one interface construction. `radio_factory()` is called on the
    interface's own event loop inside create_serial/ble/tcp."""
    module = types.ModuleType("meshcore")
    module.EventType = EventType
    module.Event = SimEvent

    class MeshCore:
        @staticmethod
        async def create_serial(port, baudrate=115200, auto_reconnect=False, max_reconnect_attempts=3, **kw):
            return SimMeshCore(radio_factory())

        @staticmethod
        async def create_ble(name=None, auto_reconnect=False, max_reconnect_attempts=3, **kw):
            return SimMeshCore(radio_factory())

        @staticmethod
        async def create_tcp(host, port, auto_reconnect=False, max_reconnect_attempts=3, **kw):
            return SimMeshCore(radio_factory())

    module.MeshCore = MeshCore
    return module
