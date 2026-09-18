"""
Self-tests for the simulated mesh itself (testscripts/simmesh), with no
interface involved: the firmware behaviors the scenario tests rely on
must hold on their own -- flood relay with path accumulation, content
dedup, source-routed DIRECT forwarding, ACK return, path discovery
permission gating, the queue-then-MESSAGES_WAITING inbox.
"""
import asyncio
import unittest

from tests._support import wait_until
from simmesh.air import Air, parse_links, ROUTE_DIRECT
from simmesh.radio import SimRadio, RadioOptions, node_pubkey, node_hash_byte


class _Recorder:
    def __init__(self):
        self.events = []

    def __call__(self, name, payload, attributes):
        self.events.append((name, payload, attributes))

    def of(self, name):
        return [p for n, p, _ in self.events if n == name]


def call_on(loop, fn, *args, timeout=5.0):
    async def _run():
        return fn(*args)
    return asyncio.run_coroutine_threadsafe(_run(), loop).result(timeout=timeout)


class SimMeshSelfTests(unittest.TestCase):

    def setUp(self):
        self.air = Air(parse_links(["A-R", "R-B"]), seed=42)
        opts = RadioOptions(auto_advert=False, advert_interval_s=0)
        self.A = SimRadio("A", self.air, options=opts)
        self.R = SimRadio("R", self.air, is_repeater=True, options=opts)
        self.B = SimRadio("B", self.air, options=opts)
        self.recA, self.recB = _Recorder(), _Recorder()
        self.A.set_push_handler(self.recA)
        self.B.set_push_handler(self.recB)
        for radio in (self.A, self.R, self.B):
            radio.attach(None)  # all on the air's loop for this test

    def tearDown(self):
        self.air.stop()

    def _advert_all(self):
        call_on(self.air.loop, self.A.cmd_send_advert)
        self.assertTrue(wait_until(lambda: node_pubkey("A") in self.B.contacts, 5.0))
        call_on(self.air.loop, self.B.cmd_send_advert)
        self.assertTrue(wait_until(lambda: node_pubkey("B") in self.A.contacts, 5.0))

    def test_advert_relayed_with_path_and_learned_as_one_hop_contact(self):
        self._advert_all()
        contact = self.B.contacts[node_pubkey("A")]
        self.assertEqual(contact["out_path_len"], 1)
        self.assertEqual(contact["out_path"], f"{node_hash_byte('R'):02x}")
        self.assertEqual(self.R.contacts[node_pubkey("A")]["out_path_len"], 0)  # zero-hop neighbor
        self.assertTrue(any(p["adv_name"] == "A" for p in self.recB.of("NEW_CONTACT")))

    def test_unchanged_flood_retransmit_is_absorbed_by_dedup(self):
        self._advert_all()
        call_on(self.air.loop, self.A.cmd_send_chan_msg, 0, "same text")
        self.assertTrue(wait_until(lambda: len(self.recB.of("MESSAGES_WAITING")) >= 1, 5.0))
        call_on(self.air.loop, self.A.cmd_send_chan_msg, 0, "same text")  # same second, same content
        self.assertFalse(wait_until(lambda: len(self.recB.of("MESSAGES_WAITING")) >= 2, 1.5))
        self.assertGreaterEqual(self.R.counters["dedup_dropped"], 1)
        kind, payload = call_on(self.air.loop, self.B.cmd_get_msg)
        self.assertEqual(kind, "CHANNEL")
        self.assertEqual(payload["text"], "A: same text")
        self.assertEqual(payload["path_len"], 1)
        self.assertIsNone(call_on(self.air.loop, self.B.cmd_get_msg))

    def test_direct_message_forwarded_by_repeater_and_acked(self):
        self._advert_all()
        result = call_on(self.air.loop, self.A.cmd_send_msg, node_pubkey("B"), "hello", 0)
        self.assertEqual(result["type"], 1)  # DIRECT (path known from advert)
        code = result["expected_ack"].hex()
        self.assertTrue(wait_until(lambda: any(p.get("code") == code for p in self.recA.of("ACK")), 5.0))
        self.assertEqual(self.R.counters["direct_forwarded"], 2)  # message out, ACK back
        kind, payload = call_on(self.air.loop, self.B.cmd_get_msg)
        self.assertEqual((kind, payload["text"]), ("CONTACT", "hello"))
        self.assertEqual(payload["pubkey_prefix"], node_pubkey("A")[:12])
        self.assertEqual(payload["path_len"], 255)  # arrived direct-routed

    def test_send_msg_to_unknown_contact_fails(self):
        self.assertIsNone(call_on(self.air.loop, self.A.cmd_send_msg, node_pubkey("B"), "x", 0))

    def test_flood_direct_message_teaches_return_path(self):
        self._advert_all()
        call_on(self.air.loop, self.A.cmd_reset_path, node_pubkey("B"))
        result = call_on(self.air.loop, self.A.cmd_send_msg, node_pubkey("B"), "flooded", 0)
        self.assertEqual(result["type"], 0)
        self.assertTrue(wait_until(lambda: len(self.recA.of("ACK")) >= 1, 5.0))
        self.assertEqual(self.B.contacts[node_pubkey("A")]["out_path_len"], 1)
        self.assertTrue(self.recB.of("PATH_UPDATE"))

    def test_path_discovery_is_gated_on_telemetry_permission(self):
        self._advert_all()
        self.B.telemetry_mode_base = 1
        call_on(self.air.loop, self.A.cmd_send_path_discovery, node_pubkey("B"))
        self.assertFalse(wait_until(lambda: self.recA.of("PATH_RESPONSE"), 2.0))
        self.assertEqual(self.B.counters["path_req_denied_no_permission"], 1)

        call_on(self.air.loop, self.B.cmd_change_contact_flags, node_pubkey("A"), 0x02)
        call_on(self.air.loop, self.A.cmd_send_path_discovery, node_pubkey("B"))
        self.assertTrue(wait_until(lambda: self.recA.of("PATH_RESPONSE"), 5.0))
        resp = self.recA.of("PATH_RESPONSE")[0]
        self.assertEqual(resp["pubkey_pre"], node_pubkey("B")[:12])
        self.assertEqual(resp["out_path_len"], 1)
        self.assertEqual(resp["out_path"], f"{node_hash_byte('R'):02x}")

    def test_rx_log_emitted_for_overheard_packets(self):
        self._advert_all()
        types = {p["payload_typename"] for p in self.recB.of("RX_LOG_DATA")}
        self.assertIn("ADVERT", types)
        entry = self.recB.of("RX_LOG_DATA")[0]
        self.assertIn("pkt_payload", entry)
        self.assertEqual(entry["route_typename"], "FLOOD")

    def test_type_loss_drops_only_that_type(self):
        self._advert_all()
        self.air.type_loss["ACK"] = 1.0
        result = call_on(self.air.loop, self.A.cmd_send_msg, node_pubkey("B"), "no ack for you", 0)
        self.assertTrue(wait_until(lambda: self.B.counters.get("txt_received", 0) >= 1, 5.0))
        self.assertFalse(wait_until(lambda: self.recA.of("ACK"), 1.5))
        self.assertGreaterEqual(sum(self.air.stats.loss_drops.values()), 1)


if __name__ == "__main__":
    unittest.main()
