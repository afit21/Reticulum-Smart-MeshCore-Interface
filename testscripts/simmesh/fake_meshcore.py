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
  - The connection lifecycle (alpha 0.1.6 item 4): `MeshCore(cx, ...)` around
    a `SerialConnection` / `TCPConnection` / `BLEConnection`, opened with
    `dispatcher.start()` + `connection_manager.connect()` (the library's
    `create_*` does that plus one `send_appstart`), `connection_manager.
    disconnect()` / `is_connected`, and a DISCONNECTED event on an
    unexpected drop. `FakeOptions` on the module inject the faults the
    supervisor exists for: `connect_failures` (OSError from connect that
    many times), `appstart_failures` (the handshake answers ERROR timeout
    that many times), `noise_errors` (the next commands return the reader's
    `invalid_frame_length` ERROR first, the real reply dispatched after --
    the library returns the first ERROR it sees, `commands/base.py`), and
    `mc.simulate_disconnect(reason)`.
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
    STATS_RADIO = "stats_radio"        # item 8 (alpha 0.1.5): CMD_GET_STATS replies
    STATS_PACKETS = "stats_packets"


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
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

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


class FakeOptions:
    """Fault injection for the connection path (see the module docstring)."""

    def __init__(self):
        self.connect_failures = 0
        self.appstart_failures = 0
        self.noise_errors = 0
        self.appstart_calls = 0
        self.connect_calls = 0


class SimCommands:
    def __init__(self, mc: "SimMeshCore"):
        self._mc = mc
        self._radio = mc.radio
        self._mesh_request_lock = asyncio.Lock()
        self.default_timeout = 15.0

    def _maybe_noise(self, result: SimEvent) -> SimEvent:
        """`noise_errors` > 0: this command's real reply is dispatched a
        moment later and a reader-noise ERROR is returned first, exactly
        what `CommandHandlerBase.send` does when a garbled frame reaches
        the reader before the reply."""
        options = self._mc.options
        if options is not None and options.noise_errors > 0:
            options.noise_errors -= 1
            self._mc.dispatcher.loop.call_later(0.05, self._mc.dispatcher.dispatch_soon, result)
            return SimEvent(EventType.ERROR, {"reason": "invalid_frame_length"})
        return result

    async def send_appstart(self) -> SimEvent:
        options = self._mc.options
        if options is not None:
            options.appstart_calls += 1
            if options.appstart_failures > 0:
                options.appstart_failures -= 1
                await asyncio.sleep(0.05)
                return SimEvent(EventType.ERROR, {"reason": "timeout"})
        return SimEvent(EventType.SELF_INFO, {
            "name": self._radio.name, "public_key": self._radio.pubkey, "adv_type": 1,
            # SF8/BW250/CR5 (alpha 0.1.5, 2026-09-21): the interface prices its
            # frames with the LoRa time-on-air of this block, and since 2a
            # paces zero-hop bursts and sizes its report waits by that
            # estimate, the block should agree with the fake air model
            # (`airtime_base_ms` 50 + 1 ms/byte): SF8/BW250 gives 0.27 s for
            # a 172-byte raw fragment against the fake's 0.22 s and 0.10 s
            # for a 40-byte report against 0.09 s. The previous SF10 priced
            # them at 0.83 / 0.30 s, four times the fake's air.
            "tx_power": 20, "max_tx_power": 22, "radio_freq": 915.5, "radio_bw": 250, "radio_sf": 8, "radio_cr": 5,
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
        return self._maybe_noise(
            SimEvent(EventType.MSG_SENT, result, {"type": result["type"], "expected_ack": result["expected_ack"].hex()}))

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

    async def get_stats_radio(self) -> SimEvent:
        """meshcore `get_stats_radio` (CMD_GET_STATS + STATS_TYPE_RADIO, v8+):
        the firmware's measured transmit / receive airtime in whole seconds
        (`Dispatcher::total_air_time`), noise floor, last RSSI / SNR."""
        return SimEvent(EventType.STATS_RADIO, {
            "noise_floor": -110, "last_rssi": -60, "last_snr": 8.0,
            "tx_air_secs": int(self._radio.tx_air_ms // 1000), "rx_air_secs": 0,
        })

    async def get_stats_packets(self) -> SimEvent:
        """meshcore `get_stats_packets` (STATS_TYPE_PACKETS): counts."""
        c = self._radio.counters
        return SimEvent(EventType.STATS_PACKETS, {
            "recv": c.get("packets_recv", 0), "sent": c.get("packets_sent", 0),
            "flood_tx": c.get("flood_tx", 0), "direct_tx": c.get("direct_tx", 0),
            "flood_rx": 0, "direct_rx": 0, "recv_errors": 0,
        })

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


class SimConnection:
    """`SerialConnection` / `TCPConnection` / `BLEConnection`: a holder
    of the endpoint, opened by the connection manager."""

    def __init__(self, kind: str, **fields):
        self.kind = kind
        self.transport = None
        for k, v in fields.items():
            setattr(self, k, v)


class SimConnectionManager:
    """`connection_manager`: `connect()` attaches the radio (and honours
    `connect_failures`), `disconnect()` detaches it, `is_connected`."""

    def __init__(self, mc: "SimMeshCore"):
        self._mc = mc
        self._is_connected = False

    @property
    def connection(self):
        return self._mc.cx

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    async def connect(self):
        options = self._mc.options
        if options is not None:
            options.connect_calls += 1
            if options.connect_failures > 0:
                options.connect_failures -= 1
                raise OSError(f"could not open port {getattr(self._mc.cx, 'port', '?')}: [Errno 2] No such file or directory")
        self._mc._attach_radio()
        self._is_connected = True
        self._mc.dispatcher.dispatch_soon(SimEvent(EventType.CONNECTED, {"connection_info": "sim"}))
        return "sim"

    async def disconnect(self):
        if self._is_connected:
            self._is_connected = False
            self._mc.radio.detach()
            self._mc.dispatcher.dispatch_soon(SimEvent(EventType.DISCONNECTED, {"reason": "manual_disconnect"}))


class SimMeshCore:
    """Stand-in for a connected meshcore.MeshCore, bound to one SimRadio.
    Must be created on the event loop the interface runs it from. With
    `cx` given (the library's constructor shape) the radio is attached by
    `connection_manager.connect()`; `create_*` attaches it at once."""

    def __init__(self, radio: SimRadio, cx=None, options: Optional[FakeOptions] = None, attach: bool = True):
        self.radio = radio
        self.cx = cx
        self.options = options
        self.loop = asyncio.get_running_loop()
        self.dispatcher = SimDispatcher(self.loop)
        self.connection_manager = SimConnectionManager(self)
        self.commands = SimCommands(self)
        self._contacts: Dict[str, dict] = {}
        self._contacts_dirty = True
        self._auto_fetch_task = None
        self._auto_fetch_running = False
        self._auto_fetch_subscription = None
        self.subscribe(EventType.CONTACTS, self._update_contacts)
        self.subscribe(EventType.ADVERTISEMENT, self._contact_change)
        self.subscribe(EventType.PATH_UPDATE, self._contact_change)
        if attach:
            self._attach_radio()
            self.connection_manager._is_connected = True

    def _attach_radio(self) -> None:
        self.radio.set_push_handler(self._on_radio_push)
        self.radio.attach(self.loop)

    @property
    def is_connected(self) -> bool:
        return self.connection_manager.is_connected

    def simulate_disconnect(self, reason: str = "serial_disconnect") -> None:
        """An unexpected drop: the radio goes away and, as the library does
        with auto_reconnect off, DISCONNECTED is emitted at once."""
        self.connection_manager._is_connected = False
        self.radio.detach()
        self.dispatcher.dispatch_soon(SimEvent(EventType.DISCONNECTED, {"reason": reason}))

    def inject_error(self, reason: str = "invalid_frame_length") -> None:
        """A reader-noise ERROR event, as a garbled inbound frame produces."""
        self.dispatcher.dispatch_soon(SimEvent(EventType.ERROR, {"reason": reason}))

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
        await self.connection_manager.disconnect()
        self.radio.detach()


def make_fake_meshcore_module(radio_factory: Callable[[], SimRadio], options: Optional[FakeOptions] = None) -> types.ModuleType:
    """Builds a module object to install as sys.modules["meshcore"] for
    one interface construction. `radio_factory()` is called on the
    interface's own event loop when the connection is opened (inside
    `MeshCore(cx)` / `connection_manager.connect()`, or create_serial/ble/tcp);
    it must return the SAME radio for the same node on a reconnect."""
    module = types.ModuleType("meshcore")
    module.EventType = EventType
    module.Event = SimEvent
    module.options = options if options is not None else FakeOptions()
    module.SerialConnection = lambda port, baudrate=115200, **kw: SimConnection("serial", port=port, baudrate=baudrate)
    module.TCPConnection = lambda host, port, **kw: SimConnection("tcp", host=host, port=port)
    module.BLEConnection = lambda address=None, **kw: SimConnection("ble", address=address)

    class MeshCore(SimMeshCore):
        """The library's constructor shape: not connected until
        `connection_manager.connect()`."""

        def __init__(self, cx, debug=False, only_error=False, default_timeout=None,
                     auto_reconnect=False, max_reconnect_attempts=3):
            super().__init__(radio_factory(), cx=cx, options=module.options, attach=False)
            if default_timeout is not None:
                self.commands.default_timeout = default_timeout

        @staticmethod
        async def create_serial(port, baudrate=115200, auto_reconnect=False, max_reconnect_attempts=3, **kw):
            return SimMeshCore(radio_factory(), options=module.options)

        @staticmethod
        async def create_ble(name=None, auto_reconnect=False, max_reconnect_attempts=3, **kw):
            return SimMeshCore(radio_factory(), options=module.options)

        @staticmethod
        async def create_tcp(host, port, auto_reconnect=False, max_reconnect_attempts=3, **kw):
            return SimMeshCore(radio_factory(), options=module.options)

    module.MeshCore = MeshCore
    return module
