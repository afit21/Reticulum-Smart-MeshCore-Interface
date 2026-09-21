"""
Shorter-path adoption from a peer's own floods (alpha 0.1.5, item 3,
2026-09-21; no wire change).

Field evidence (`fieldtests/raw/Alpha0.1.4/afipc_*082952`, from 11:05): the
desktop's discovery returned a four-hop path (19 76 be d6) to the laptop
while the laptop reached the desktop in two (d6 19); three stale-path resets
rediscovered the same four hops; 35 minutes of proofs at 17 s ACK timeouts
and 50 % success. The desktop's own radio log had seen the laptop's floods
arrive over the two-hop route the whole time (`rx_log` FLOOD REQ from `34`,
`path` d619, 17 copies).

Firmware ground truth (referenceprojects/MeshCore-main): each relaying
repeater appends its hash at the END of a flood's path
(`Mesh::routeRecvPacket`), `sendDirect` consumes `path[0]` first, and the
firmware never reverses a path itself -- so the reverse of a received flood
path, with the same hash size, is a valid out_path to the originator.
`change_contact_path` (the library's `update_contact(path=...)`) is what
discovery already persists with.

Pinned:
  * `_reverse_flood_path`: d619 -> 19d6, hash size honoured, empty stays empty;
  * attribution is conservative: an ADVERT by its full key when that key is
    a bound peer; an addressed flood only when addressed to us, with a
    1-byte source hash that matches exactly one bound peer and no other
    device contact; nothing for a DIRECT frame or a stranger;
  * the shortest route within `path_adopt_window` wins, cooled-down routes
    are skipped, ties go to the most recent;
  * the scenario: a four-hop resolved path and a two-hop flood route become
    a two-hop resolved path AND a two-hop device contact, captured as
    `path_adopted`; a route only as long as the path is not adopted; nothing
    is adopted while a raw window to the peer is in flight;
  * provisional: two full-timeout send failures before any success drop the
    adopted path (resolved path forgotten -> discovery next, route on
    cooldown); one success confirms it;
  * the rx-log tap feeds the observer (an ADVERT event with `adv_key`);
  * shipped defaults: enabled, 600 s window, miss limit 2.
"""
import time
import unittest

from tests._support import SingleNodeCase, load_interface_module

PEER = "34ab12cd56ef"                 # the laptop's role: prefix begins 34
PEER_KEY = PEER + "00" * 26           # a full 32-byte key with that prefix
OTHER = "34ff00112233"                # another node sharing the first byte


class _Scaffold(SingleNodeCase):
    def setUp(self):
        iface = self.iface
        self._own = iface._own_pubkey_hex
        iface._own_pubkey_hex = "7b" + "11" * 31   # the desktop's role: prefix begins 7b
        iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=True, last_seen=time.time())
        self.node.radio._upsert_contact(PEER_KEY, "laptop")
        self._refresh_contacts()
        iface._flood_routes_seen.pop(PEER, None)
        iface._adopted_paths.pop(PEER, None)
        iface._adoption_cooldown.pop(PEER, None)
        iface._resolved_paths.pop(PEER, None)
        iface._raw_windows.pop(PEER, None)

    def _refresh_contacts(self):
        # The library caches the device's contact table; the interface reads
        # that cache (`_mc.contacts`, `get_contact_by_key_prefix`).
        self.node.run_on_loop(self.iface._mc.commands.get_contacts(), timeout=5)

    def tearDown(self):
        iface = self.iface
        iface._own_pubkey_hex = self._own
        iface._peers.pop(PEER, None)
        iface._peers.pop(OTHER, None)
        self.node.radio.contacts.pop(PEER_KEY, None)
        for k in [k for k in self.node.radio.contacts if k.startswith(OTHER)]:
            self.node.radio.contacts.pop(k, None)
        self._refresh_contacts()
        for d in (iface._flood_routes_seen, iface._adopted_paths, iface._adoption_cooldown, iface._resolved_paths,
                  iface._raw_windows, iface._direct_path_failures):
            d.pop(PEER, None)

    def _flood(self, ptype, path_hex, src=None, dst=None, adv_key=None, route=1):
        fields = {"route_type": route, "payload_type": ptype, "path_len": len(path_hex) // 2, "path": path_hex,
                  "src_hash": src, "dst_hash": dst}
        payload = {"path_hash_size": 1}
        if adv_key is not None:
            payload["adv_key"] = adv_key
        return payload, fields


class ReverseAndAttribute(_Scaffold):
    def test_reverse_flood_path(self):
        rev = self.iface._reverse_flood_path
        self.assertEqual(rev("d619"), "19d6")
        self.assertEqual(rev("1976bed6"), "d6be7619")
        self.assertEqual(rev("aabbccdd", 2), "ccddaabb")
        self.assertEqual(rev(""), "")
        self.assertEqual(rev("d6"), "d6")

    def test_advert_attributed_by_full_key(self):
        iface = self.iface
        self.assertEqual(iface._attribute_flood_to_peer(*self._flood(4, "d619", adv_key=PEER_KEY)), (PEER, "advert"))
        self.assertIsNone(iface._attribute_flood_to_peer(*self._flood(4, "d619", adv_key="ee" * 32)), "a stranger's advert")
        self.assertIsNone(iface._attribute_flood_to_peer(*self._flood(4, "d619", adv_key=PEER_KEY, route=2)), "not a flood")

    def test_addressed_flood_needs_our_dst_and_a_unique_src(self):
        iface = self.iface
        self.assertEqual(iface._attribute_flood_to_peer(*self._flood(0, "d619", src="34", dst="7b")), (PEER, "addressed"))
        self.assertIsNone(iface._attribute_flood_to_peer(*self._flood(0, "d619", src="34", dst="99")), "addressed elsewhere")
        self.assertIsNone(iface._attribute_flood_to_peer(*self._flood(0, "d619", src="35", dst="7b")), "no bound peer with that byte")
        iface._peers[OTHER] = self.module._PeerRecord(pubkey_prefix=OTHER, has_upstream_rns=True, last_seen=time.time())
        self.assertIsNone(iface._attribute_flood_to_peer(*self._flood(0, "d619", src="34", dst="7b")), "two bound peers share the byte")
        iface._peers.pop(OTHER)
        self.node.radio._upsert_contact(OTHER + "00" * 26, "stranger")
        self._refresh_contacts()
        self.assertIsNone(iface._attribute_flood_to_peer(*self._flood(0, "d619", src="34", dst="7b")),
                          "another device contact shares the byte")


class ShortestRoute(_Scaffold):
    def test_window_cooldown_and_ties(self):
        iface = self.iface
        now = time.monotonic()
        iface._note_flood_route(*self._flood(4, "1976bed6", adv_key=PEER_KEY), now - 5)
        iface._note_flood_route(*self._flood(0, "d619", src="34", dst="7b"), now - 100)
        iface._note_flood_route(*self._flood(0, "19d6", src="34", dst="7b"), now - 50)      # same length, newer
        iface._note_flood_route(*self._flood(0, "19", src="34", dst="7b"), now - 900)        # outside the 600 s window
        best = iface._shortest_flood_route(PEER, now)
        self.assertEqual((best[0], best[1]), (2, "d619"), "shortest inside the window, most recent among equals")
        iface._adoption_cooldown[PEER] = {"d619": now + 60}
        best = iface._shortest_flood_route(PEER, now)
        self.assertEqual((best[0], best[1]), (2, "19d6"), "a cooled-down route is skipped")
        self.assertIsNone(iface._shortest_flood_route(PEER, now + 700), "everything ages out of the window")


class AdoptionScenario(_Scaffold):
    def _resolved(self, path_hex):
        return self.module._ResolvedPath(path_hex, len(path_hex) // 2, 1, time.monotonic() - 100)

    def test_four_hop_contact_and_two_hop_flood_become_a_two_hop_contact(self):
        iface = self.iface
        iface._resolved_paths[PEER] = self._resolved("1976bed6")
        iface._note_flood_route(*self._flood(0, "d619", src="34", dst="7b"), time.monotonic() - 3)
        sink = []
        orig = iface._capture_event
        iface._capture_event = lambda direction, fields: sink.append(fields)
        iface._packet_capture_file = object()
        try:
            adopted = self.node.run_on_loop(iface._maybe_adopt_shorter_path(PEER, iface._resolved_paths[PEER]), timeout=10)
        finally:
            iface._capture_event = orig
            iface._packet_capture_file = None
        self.assertEqual((adopted.out_path_hex, adopted.out_path_len, adopted.out_path_hash_len), ("19d6", 2, 1))
        self.assertIs(iface._resolved_paths[PEER], adopted, "the interface's own record is the adopted path")
        contact = self.node.radio.contacts[PEER_KEY]
        self.assertEqual((contact["out_path"], contact["out_path_len"]), ("19d6", 2), "persisted to the device contact")
        self.assertIn(PEER, iface._adopted_paths, "provisional until it delivers")
        events = [f for f in sink if f.get("event") == "path_adopted"]
        self.assertEqual(len(events), 1)
        self.assertEqual((events[0]["old_path_len"], events[0]["new_path_len"], events[0]["new_path_hex"]), (4, 2, "19d6"))

    def test_not_adopted_unless_at_least_one_hop_shorter(self):
        iface = self.iface
        iface._resolved_paths[PEER] = self._resolved("1976")
        iface._note_flood_route(*self._flood(0, "d619", src="34", dst="7b"), time.monotonic())
        same = self.node.run_on_loop(iface._maybe_adopt_shorter_path(PEER, iface._resolved_paths[PEER]), timeout=10)
        self.assertEqual(same.out_path_hex, "1976", "a two-hop route does not replace a two-hop path")
        self.assertNotIn(PEER, iface._adopted_paths)
        iface._resolved_paths[PEER] = self._resolved("1976be")
        shorter = self.node.run_on_loop(iface._maybe_adopt_shorter_path(PEER, iface._resolved_paths[PEER]), timeout=10)
        self.assertEqual(shorter.out_path_hex, "19d6", "one hop shorter is enough")

    def test_not_adopted_while_a_raw_window_is_in_flight_or_disabled(self):
        iface = self.iface
        iface._resolved_paths[PEER] = self._resolved("1976bed6")
        iface._note_flood_route(*self._flood(0, "d619", src="34", dst="7b"), time.monotonic())
        iface._raw_windows[PEER] = object()
        same = self.node.run_on_loop(iface._maybe_adopt_shorter_path(PEER, iface._resolved_paths[PEER]), timeout=10)
        self.assertEqual(same.out_path_hex, "1976bed6")
        iface._raw_windows.pop(PEER)
        saved = iface.path_adopt_enabled
        iface.path_adopt_enabled = False
        try:
            same = self.node.run_on_loop(iface._maybe_adopt_shorter_path(PEER, iface._resolved_paths[PEER]), timeout=10)
            self.assertEqual(same.out_path_hex, "1976bed6")
        finally:
            iface.path_adopt_enabled = saved
        self.assertIsNone(self.node.run_on_loop(iface._maybe_adopt_shorter_path(PEER, None), timeout=10),
                          "no resolved path: discovery's job, unchanged")

    def _adopt(self):
        iface = self.iface
        iface._resolved_paths[PEER] = self._resolved("1976bed6")
        iface._note_flood_route(*self._flood(0, "d619", src="34", dst="7b"), time.monotonic())
        return self.node.run_on_loop(iface._maybe_adopt_shorter_path(PEER, iface._resolved_paths[PEER]), timeout=10)

    def test_two_misses_drop_the_adopted_path_for_discovery(self):
        iface = self.iface
        adopted = self._adopt()
        self.assertEqual(adopted.out_path_len, 2)
        self.on_loop(iface.record_direct_send_result, PEER, False, True)
        self.assertIn(PEER, iface._resolved_paths, "one miss: still provisional")
        self.assertEqual(iface._adopted_paths[PEER]["misses"], 1)
        self.on_loop(iface.record_direct_send_result, PEER, False, True)
        self.assertNotIn(PEER, iface._resolved_paths, "two misses: forgotten, the next send discovers")
        self.assertNotIn(PEER, iface._adopted_paths)
        self.assertGreater(iface._adoption_cooldown[PEER]["19d6"], time.monotonic(), "the route is on cooldown")
        self.assertIsNone(iface._shortest_flood_route(PEER, time.monotonic()), "and is not offered again")
        self.assertEqual(iface._direct_path_failures.get(PEER, 0), 0, "the ordinary detector did not count them")

    def test_a_success_confirms_the_adoption(self):
        iface = self.iface
        self._adopt()
        self.on_loop(iface.record_direct_send_result, PEER, False, True)
        self.on_loop(iface.record_direct_send_result, PEER, True, True)
        self.assertNotIn(PEER, iface._adopted_paths, "confirmed")
        self.assertEqual(iface._resolved_paths[PEER].out_path_hex, "19d6")
        self.on_loop(iface.record_direct_send_result, PEER, False, True)
        self.assertEqual(iface._resolved_paths[PEER].out_path_hex, "19d6", "after confirmation the ordinary detector rules")
        self.assertEqual(iface._direct_path_failures.get(PEER, 0), 1)


class RxLogTapFeedsTheObserver(_Scaffold):
    def test_advert_event_records_the_reversed_route(self):
        iface = self.iface
        payload = {"route_type": 1, "route_typename": "FLOOD", "payload_type": 4, "payload_typename": "ADVERT",
                   "payload_ver": 0, "path_len": 2, "path_hash_size": 1, "path": "d619", "payload_length": 60,
                   "pkt_payload": b"", "pkt_hash": 7, "snr": 6.0, "rssi": -70, "adv_key": PEER_KEY}
        event = type("E", (), {"payload": payload})()
        self.on_loop(iface._on_rx_log_data, event)
        routes = list(iface._flood_routes_seen.get(PEER, ()))
        self.assertEqual(len(routes), 1)
        self.assertEqual((routes[0][1], routes[0][2], routes[0][4]), (2, "19d6", "advert"))


class ShippedDefaults(unittest.TestCase):
    def test_defaults(self):
        module = load_interface_module()
        bare = module.SmartMeshCoreInterface.__new__(module.SmartMeshCoreInterface)
        bare._configure_path_discovery({})
        self.assertTrue(bare.path_adopt_enabled)
        self.assertEqual(bare.path_adopt_window_s, 600.0)
        self.assertEqual(module.SmartMeshCoreInterface.PATH_ADOPT_MISS_LIMIT, 2)


if __name__ == "__main__":
    unittest.main()
