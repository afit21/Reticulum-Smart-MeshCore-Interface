"""
Alpha 0.1.6, item 4: the connection supervisor (2026-09-22).

The owner's "event failed" errors and rnsd stuck connecting on restart,
read against `meshcore` 2.3.9.1: one handshake attempt right after the
port opens (which resets the Heltec V3 through DTR / RTS, so it goes to a
rebooting radio), three one-second reconnect attempts and then a dead
object for good, and a command's reply matched by event type only while
the reader emits ERROR events of its own for garbled frames.

Pinned against the fake `meshcore` (which models the library's connection
lifecycle and injects the faults, `testscripts/simmesh/fake_meshcore.py`):
  * a handshake that answers only on the third attempt brings the node up
    without reopening the port;
  * a port that cannot be opened at first (OSError) is retried with the
    backoff and the constructor did not block on it;
  * a disconnect is followed by a full re-setup on the same radio: the
    node is online again, message fetching re-armed, `connection_state`
    records written;
  * reader-noise ERRORs during a command do not fail it (the reply that
    follows is taken), and the noise counter warns above the rate;
  * the port-holder scan reads a fake /proc tree and names the holder;
  * the shipped defaults.
"""
import asyncio
import os
import tempfile
import time
import unittest

from tests._support import SingleNodeCase, load_interface_module, quiet_rns, wait_until
from testscripts.simmesh.fake_meshcore import FakeOptions
from testscripts.simmesh.harness import RadioOptions, SimMesh

FAST_CONNECT = {"peer_discovery_enabled": "no", "connect_retry_min": "1", "connect_retry_max": "2",
                "handshake_timeout": "1", "serial_open_settle": "0"}


class _Mesh:
    """One mesh per test with its own fault options."""

    def __init__(self, options: FakeOptions, config=None, require_online=True):
        quiet_rns()
        self.mesh = SimMesh(["A-B"], seed=1, startup_stagger_s=0.0,
                            radio_options=RadioOptions(auto_advert=False, advert_interval_s=0))
        cfg = dict(FAST_CONNECT)
        cfg.update(config or {})
        self.node = self.mesh.add_node("A", config=cfg, fake_options=options, require_online=require_online)
        self.iface = self.node.iface

    def stop(self):
        self.mesh.stop()


class HandshakeRetries(unittest.TestCase):
    def test_handshake_answered_on_the_third_attempt(self):
        options = FakeOptions()
        options.appstart_failures = 2
        m = _Mesh(options)
        try:
            self.assertTrue(m.iface.online)
            self.assertEqual(options.appstart_calls, 3, "two unanswered handshakes, the third answered")
            self.assertEqual(options.connect_calls, 1, "the port was opened once, not reopened per attempt")
            self.assertTrue(m.iface._own_pubkey_hex)
        finally:
            m.stop()

    def test_handshake_never_answered_closes_and_retries(self):
        options = FakeOptions()
        options.appstart_failures = 50
        m = _Mesh(options, config={"handshake_attempts": "2"}, require_online=False)
        try:
            self.assertFalse(m.iface.online)
            # a second connection attempt follows after connect_retry_min
            self.assertTrue(wait_until(lambda: options.connect_calls >= 2, 5.0), options.connect_calls)
            options.appstart_failures = 0
            self.assertTrue(wait_until(lambda: m.iface.online, 8.0), "comes online once the radio answers")
        finally:
            m.stop()


class PortOpenFailure(unittest.TestCase):
    def test_port_that_cannot_be_opened_is_retried_and_the_constructor_returns(self):
        options = FakeOptions()
        options.connect_failures = 2
        t0 = time.monotonic()
        m = _Mesh(options)
        try:
            # the constructor returned after the failed first attempt; the
            # supervisor then retried at 1 s and 2 s.
            self.assertTrue(m.iface.online, "online after the backoff retries")
            self.assertEqual(options.connect_calls, 3)
            self.assertGreaterEqual(time.monotonic() - t0, 1.0)
        finally:
            m.stop()


class DisconnectAndRecover(SingleNodeCase):
    def test_a_drop_is_followed_by_a_full_resetup(self):
        iface = self.iface
        mc_before = iface._mc
        options = self.node.fake_module.options
        calls_before = options.connect_calls
        appstart_before = options.appstart_calls
        iface.connect_retry_min_s = 1.0
        iface.connect_retry_max_s = 2.0
        iface.handshake_timeout_s = 1.0
        self.on_loop(mc_before.simulate_disconnect, "serial_disconnect")
        self.assertTrue(wait_until(lambda: not iface.online, 2.0), "offline at once")
        self.assertTrue(wait_until(lambda: iface.online, 10.0), "back online after the backoff")
        self.assertIsNot(iface._mc, mc_before, "a fresh MeshCore object")
        self.assertEqual(options.connect_calls, calls_before + 1)
        self.assertGreaterEqual(options.appstart_calls, appstart_before + 1, "the handshake ran again")
        self.assertIsNotNone(iface._mc._auto_fetch_subscription, "message fetching re-armed")
        self.assertIs(iface._mc.radio, self.node.radio, "the same radio, re-attached")
        self.assertEqual(iface._connection_state, "online")
        self.assertIsNotNone(iface._outgoing_worker_task)

    def test_connection_state_records_are_captured(self):
        iface = self.iface
        records = []
        original = iface._capture_event
        iface._capture_event = lambda direction, fields: records.append(dict(fields))
        saved_file = iface._packet_capture_file
        iface._packet_capture_file = object()
        try:
            iface.connect_retry_min_s = 1.0
            self.on_loop(iface._mc.simulate_disconnect, "usb_gone")
            self.assertTrue(wait_until(lambda: iface.online and any(
                r.get("state") == "online" for r in records), 10.0), records)
        finally:
            iface._capture_event = original
            iface._packet_capture_file = saved_file
        states = [r["state"] for r in records if r.get("event") == "connection_state"]
        self.assertEqual(states[0], "disconnected", states)
        self.assertEqual(records[0]["reason"], "usb_gone")
        for want in ("retry_wait", "connecting", "open", "online"):
            self.assertIn(want, states, states)

    def test_auto_reconnect_off_stays_offline(self):
        iface = self.iface
        saved = iface.auto_reconnect
        iface.auto_reconnect = False
        try:
            self.on_loop(iface._mc.simulate_disconnect, "serial_disconnect")
            self.assertTrue(wait_until(lambda: not iface.online, 2.0))
            time.sleep(1.5)
            self.assertFalse(iface.online)
        finally:
            iface.auto_reconnect = saved
            # bring it back for the other tests: a fresh supervisor
            self.node.run_on_loop(self._restart_supervisor(), timeout=15.0)
            self.assertTrue(wait_until(lambda: iface.online, 10.0))

    async def _restart_supervisor(self):
        iface = self.iface
        if iface._supervisor_task is not None and not iface._supervisor_task.done():
            iface._supervisor_task.cancel()
            try:
                await iface._supervisor_task
            except (asyncio.CancelledError, Exception):
                pass
        iface._startup_gate = asyncio.get_running_loop().create_future()
        iface._supervisor_task = asyncio.ensure_future(iface._connection_supervisor())


class ErrorCorrelation(SingleNodeCase):
    def test_reader_noise_during_a_command_does_not_fail_it(self):
        iface = self.iface
        options = self.node.fake_module.options
        contact = next(iter(iface._mc.contacts.values()), None) if iface._mc.contacts else None
        # the fake's send_msg to a known contact, two noise ERRORs first
        self.on_loop(self.node.radio._upsert_contact, "34" * 32, "peer") if hasattr(self.node.radio, "_upsert_contact") else None
        options.noise_errors = 2
        noise_before = iface._serial_noise_total
        result = self.node.run_on_loop(iface._run_command(
            iface._mc.commands.send_msg("34" * 32, "hello"), "send_msg", iface._EventType.MSG_SENT), timeout=10.0)
        self.assertEqual(result.type, iface._EventType.MSG_SENT, "the real reply, after the noise")
        self.assertEqual(iface._serial_noise_total, noise_before + 1, "one noise ERROR counted per re-wait (the second reply is the real one)")
        self.assertEqual(options.noise_errors, 1)

    def test_firmware_error_and_timeout_are_not_noise(self):
        iface = self.iface
        Ev = self.module.SmartMeshCoreInterface
        SimEvent = self.node.fake_module.Event
        E = iface._EventType
        self.assertEqual(iface._error_is_noise(SimEvent(E.ERROR, {"reason": "invalid_frame_length"})), "invalid_frame_length")
        self.assertEqual(iface._error_is_noise(SimEvent(E.ERROR, {"reason": "binary_parse_error: unpack"})), "binary_parse_error: unpack")
        self.assertIsNone(iface._error_is_noise(SimEvent(E.ERROR, {"reason": "timeout"})))
        self.assertIsNone(iface._error_is_noise(SimEvent(E.ERROR, {"error_code": 2, "code_string": "NOT_FOUND"})))
        self.assertIsNone(iface._error_is_noise(SimEvent(E.OK, {})))
        self.assertIsNone(iface._error_is_noise(None))

    def test_noise_rate_warns_once_a_minute_with_a_capture_record(self):
        iface = self.iface
        records = []
        original = iface._capture_event
        iface._capture_event = lambda direction, fields: records.append(dict(fields))
        saved_file = iface._packet_capture_file
        iface._packet_capture_file = object()
        iface._serial_noise_times.clear()
        iface._serial_noise_warned_at = 0.0
        try:
            for _ in range(iface.serial_noise_warn_per_min + 1):
                iface._note_serial_noise("invalid_frame_length", "test")
        finally:
            iface._capture_event = original
            iface._packet_capture_file = saved_file
        noise = [r for r in records if r.get("state") == "serial_noise"]
        self.assertEqual(len(noise), 1, "one warning per minute, not one per frame")
        self.assertGreaterEqual(noise[0]["per_min"], iface.serial_noise_warn_per_min)


class PortHolderScan(unittest.TestCase):
    def test_scan_names_the_other_holder_and_skips_itself(self):
        module = load_interface_module()
        cls = module.SmartMeshCoreInterface
        with tempfile.TemporaryDirectory() as root:
            dev = os.path.join(root, "ttyUSB0")
            open(dev, "w").close()
            for pid, cmd, holds in ((4242, "python3 -m rnsd", True), (5151, "MeshChat", True), (7, "sleep", False), (9999, "self", True)):
                os.makedirs(os.path.join(root, str(pid), "fd"))
                with open(os.path.join(root, str(pid), "cmdline"), "wb") as f:
                    f.write(cmd.replace(" ", "\0").encode() + b"\0")
                if holds:
                    os.symlink(dev, os.path.join(root, str(pid), "fd", "5"))
                os.symlink("/dev/null", os.path.join(root, str(pid), "fd", "0"))
            holders = cls._port_holders(dev, proc_root=root, own_pid=9999)
        self.assertEqual(sorted(holders), [(4242, "python3 -m rnsd"), (5151, "MeshChat")])
        self.assertEqual(cls._port_holders("/dev/nonexistent-tty", proc_root="/nonexistent"), [])


class ShippedDefaults(SingleNodeCase):
    def test_defaults(self):
        bare = self.module.SmartMeshCoreInterface.__new__(self.module.SmartMeshCoreInterface)
        bare._configure_transport({})
        self.assertEqual((bare.connect_retry_min_s, bare.connect_retry_max_s), (5.0, 60.0))
        self.assertEqual(bare.serial_open_settle_s, 2.0)
        self.assertEqual((bare.handshake_attempts, bare.handshake_timeout_s), (5, 5.0))
        self.assertEqual(bare.command_timeout_s, 15.0)
        self.assertEqual(bare.serial_noise_warn_per_min, 5)
        self.assertEqual(bare.max_reconnect_attempts, 0, "0 = retry forever")
        self.assertTrue(bare.auto_reconnect)


if __name__ == "__main__":
    unittest.main()
