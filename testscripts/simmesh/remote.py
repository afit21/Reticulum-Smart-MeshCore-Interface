"""
Socket transport for the simulated air, so end nodes can live in other
processes (each running a real RNS.Reticulum -- see
testscripts/rns_multiprocess_sim.py). The physics stays in one place
(the server's Air); a remote SimRadio only ever calls transmit() and
receives on_air() callbacks, exactly like an in-process one.

Wire protocol: newline-delimited JSON over TCP.
    client -> server   {"op": "hello", "name": "A"}
    server -> client   {"op": "welcome", "airtime_base_ms": .., "airtime_per_byte_ms": .., "seed": ..}
    client -> server   {"op": "tx", "packet": {...SimPacket.to_dict()...}}
    server -> client   {"op": "rx", "from": "R1", "packet": {...}}
"""
import asyncio
import json
import random
import socket
import threading
from typing import Callable, Optional

from .air import Air, SimPacket


class AirServer:
    """Runs on the Air's own event loop. Each connected client is one end
    node in the topology; the server attaches a receiver for it that
    forwards deliveries down the socket."""

    def __init__(self, air: Air, host: str = "127.0.0.1", port: int = 0):
        self.air = air
        self.host = host
        self.port = port
        self._server: Optional[asyncio.AbstractServer] = None
        self._clients = {}

    def start(self) -> int:
        fut = asyncio.run_coroutine_threadsafe(self._start(), self.air.loop)
        return fut.result(timeout=10)

    async def _start(self) -> int:
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        return self.port

    def stop(self) -> None:
        if self._server is None:
            return

        async def _close():
            self._server.close()
            await self._server.wait_closed()

        try:
            asyncio.run_coroutine_threadsafe(_close(), self.air.loop).result(timeout=5)
        except Exception:
            pass

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        name = None
        try:
            line = await reader.readline()
            hello = json.loads(line)
            if hello.get("op") != "hello" or hello.get("name") not in self.air.adjacency:
                writer.write((json.dumps({"op": "error", "reason": "unknown node"}) + "\n").encode())
                await writer.drain()
                writer.close()
                return
            name = hello["name"]
            self._clients[name] = writer

            def on_air(packet: SimPacket, from_name: str) -> None:
                try:
                    writer.write((json.dumps({"op": "rx", "from": from_name, "packet": packet.to_dict()}) + "\n").encode())
                except Exception:
                    pass

            self.air.attach(name, on_air, loop=None)
            writer.write((json.dumps({
                "op": "welcome", "airtime_base_ms": self.air.airtime_base_ms,
                "airtime_per_byte_ms": self.air.airtime_per_byte_ms, "seed": self.air.seed,
            }) + "\n").encode())
            await writer.drain()

            while True:
                line = await reader.readline()
                if not line:
                    break
                msg = json.loads(line)
                if msg.get("op") == "tx":
                    self.air.transmit(name, SimPacket.from_dict(msg["packet"]))
        except (asyncio.IncompleteReadError, ConnectionError, json.JSONDecodeError):
            pass
        finally:
            if name is not None:
                self.air.detach(name)
                self._clients.pop(name, None)
            try:
                writer.close()
            except Exception:
                pass


class RemoteAir:
    """Client-side stand-in for `Air`, exposing exactly what SimRadio uses:
    rng, log, loop, airtime_s(), attach(), detach(), transmit()."""

    def __init__(self, host: str, port: int, name: str, log: Optional[Callable] = None, seed: Optional[int] = None):
        self.host, self.port, self.name = host, port, name
        self.log = log or (lambda msg: None)
        self.rng = random.Random(seed if seed is not None else hash(name) & 0xFFFFFFFF)
        self.airtime_base_ms = 50.0
        self.airtime_per_byte_ms = 1.0
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._on_air: Optional[Callable] = None
        self._sock: Optional[socket.socket] = None
        self._wlock = threading.Lock()
        self._reader: Optional[threading.Thread] = None
        self._connected = threading.Event()

    def airtime_s(self, size: int) -> float:
        return (self.airtime_base_ms + self.airtime_per_byte_ms * size) / 1000.0

    def attach(self, name: str, on_air: Callable, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self._on_air = on_air
        self.loop = loop
        self._sock = socket.create_connection((self.host, self.port), timeout=10)
        self._file = self._sock.makefile("r", encoding="utf-8")
        self._send({"op": "hello", "name": name})
        welcome = json.loads(self._file.readline())
        if welcome.get("op") != "welcome":
            raise RuntimeError(f"air server refused {name!r}: {welcome}")
        self.airtime_base_ms = float(welcome.get("airtime_base_ms", self.airtime_base_ms))
        self.airtime_per_byte_ms = float(welcome.get("airtime_per_byte_ms", self.airtime_per_byte_ms))
        self._sock.settimeout(None)
        self._reader = threading.Thread(target=self._read_loop, daemon=True, name=f"simmesh-remote-{name}")
        self._reader.start()
        self._connected.set()

    def detach(self, name: str) -> None:
        self._connected.clear()
        try:
            if self._sock is not None:
                self._sock.close()
        except Exception:
            pass

    def transmit(self, from_name: str, packet: SimPacket) -> None:
        self._send({"op": "tx", "packet": packet.to_dict()})

    def _send(self, msg: dict) -> None:
        data = (json.dumps(msg) + "\n").encode()
        with self._wlock:
            if self._sock is not None:
                try:
                    self._sock.sendall(data)
                except OSError:
                    pass

    def _read_loop(self) -> None:
        try:
            for line in self._file:
                msg = json.loads(line)
                if msg.get("op") == "rx" and self._on_air is not None:
                    packet = SimPacket.from_dict(msg["packet"])
                    if self.loop is not None:
                        self.loop.call_soon_threadsafe(self._on_air, packet, msg.get("from"))
                    else:
                        self._on_air(packet, msg.get("from"))
        except (OSError, ValueError):
            pass
