"""Alpha 0.1.8, item 4: the announce cache survives a restart, and one
path-request verification per interval goes on the air instead of most of
them.

Field evidence (2026-09-22 evening, `fieldtests/raw/Alpha0.1.7/`, laptop
`a_`): the laptop's interface restarted at 22:28, 22:45 and 22:53, and each
restart at two hops cost about three minutes of path requests -- 8
transmitted and 9 rate-limited between 22:29:07 and 22:32:33 -- answered by
the desktop with three-fragment announce windows through two repeaters
(`small_mesh_direct_all_announce` 6, `duplicate_in_flight` 4 in that span).
Every one was for the single destination `6b9f66014d98`, which the desktop
had announced before 22:20 and the laptop had cached in its previous
process. The cache was per-process, so a restart threw it away.

The same trace shows the second half of this item. Over the hour the laptop
transmitted about 20 path requests for that one destination and answered 12
locally, because the 0.1.6 rule capped the LOCAL answers at one per
`path_request_local_answer_min_interval` and let every other request
transmit -- with RNS re-requesting every 30-70 s, the verification budget
had become the common case. The pair at 22:37:02 and 22:37:38 went out 70 s
and 106 s after the local answer of 22:35:52 for exactly that reason. The
rule is now the other way round: one verification per interval goes on the
air, everything in between is answered from the cache.
"""
import json
import os
import tempfile
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet

PEER = "abcdef012345"
# The destination the laptop asked for all evening.
DEST = bytes.fromhex("6b9f66014d9853faab220fba47d02761")


class _FakeOwner:
    def __init__(self):
        self.received = []

    def inbound(self, data, interface):
        self.received.append(bytes(data))


class AnnounceCacheAcrossRestarts(SingleNodeCase):
    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.mkdtemp(prefix="smci-announce-")
        self.cache_path = os.path.join(self.tmpdir, "smci_announces.json")

    def _install(self):
        iface = self.iface
        saved = {k: getattr(iface, k) for k in
                 ("owner", "_dispatch_outgoing_packet", "_capture_outgoing", "announce_cache_path")}
        owner = _FakeOwner()
        dispatched, decisions = [], []

        async def fake_dispatch(data, header, expires_at=None, spawned=None):
            dispatched.append(header)

        iface.owner = owner
        iface._dispatch_outgoing_packet = fake_dispatch
        iface._capture_outgoing = lambda header, data, decision, target_peer=None, candidate_peers=None: \
            decisions.append((decision, target_peer))
        iface.announce_cache_path = self.cache_path
        iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=False, last_seen=time.time())
        iface._announce_cache.clear()
        iface._announce_cache_dirty = False

        def restore():
            for k, v in saved.items():
                setattr(iface, k, v)
            iface._peers.pop(PEER, None)
            iface._announce_cache.clear()
            iface._path_request_last_sent_at.clear()
            iface._path_request_local_answer_at.clear()

        return owner, dispatched, decisions, restore

    def _request(self, requested: bytes) -> bytes:
        dst = RNS.Destination(None, RNS.Destination.OUT, RNS.Destination.PLAIN, "rnstransport", "path", "request")
        pkt = RNS.Packet(dst, requested + RNS.Identity.get_random_hash(), packet_type=RNS.Packet.DATA,
                         transport_type=RNS.Transport.BROADCAST, header_type=RNS.Packet.HEADER_1, create_receipt=False)
        pkt.pack()
        return pkt.raw

    def _send_request(self, iface):
        request = self._request(DEST)
        self.node.run_on_loop(iface._send_outgoing_packet(request, iface._parse_rns_header(request)), timeout=10.0)

    # -- persistence ------------------------------------------------------

    def test_a_restart_with_the_file_present_answers_the_first_request_locally(self):
        iface = self.iface
        owner, dispatched, decisions, restore = self._install()
        try:
            announce = build_rns_packet("announce", dest_hash=DEST, payload=b"a" * 120)
            self.on_loop(lambda: iface.process_incoming(
                announce, transport="direct_raw_multifragment", sender_peer_prefix=PEER))
            self.assertIn(DEST, iface._announce_cache)
            self.assertTrue(iface._announce_cache_dirty)

            iface._save_announce_cache()
            self.assertTrue(os.path.isfile(self.cache_path))
            with open(self.cache_path) as f:
                on_disk = json.load(f)
            self.assertEqual(len(on_disk["announces"]), 1)
            entry = on_disk["announces"][0]
            self.assertEqual(entry["destination_hash"], DEST.hex())
            self.assertEqual(bytes.fromhex(entry["raw"]), announce)
            self.assertEqual(entry["source_peer"], PEER)
            # Persisted as wall clock: monotonic has no meaning across a restart.
            self.assertAlmostEqual(entry["cached_at_wall"], time.time(), delta=30.0)

            # The restart: the process's cache is empty, the file is not.
            iface._announce_cache.clear()
            iface._load_announce_cache()
            self.assertIn(DEST, iface._announce_cache)
            raw, _cached_at, src, _verified_at = iface._announce_cache[DEST]
            self.assertEqual((raw, src), (announce, PEER), "restored byte for byte, with its source peer")

            # The first path request after the restart: answered locally,
            # nothing on the air. This is the three minutes the field paid.
            del dispatched[:]
            owner.received.clear()
            self._send_request(iface)
            self.assertEqual(dispatched, [], "nothing transmitted after a restart")
            self.assertEqual(decisions[-1], ("path_request_answered_locally", PEER))
            self.assertEqual(len(owner.received), 1)
        finally:
            restore()

    def test_a_stale_entry_is_dropped_on_load(self):
        iface = self.iface
        _owner, _dispatched, _decisions, restore = self._install()
        try:
            announce = build_rns_packet("announce", dest_hash=DEST, payload=b"a" * 120)
            with open(self.cache_path, "w") as f:
                json.dump({"announces": [{
                    "destination_hash": DEST.hex(), "raw": announce.hex(), "source_peer": PEER,
                    "cached_at_wall": time.time() - iface.announce_cache_ttl_s - 60.0,
                }]}, f)
            iface._announce_cache.clear()
            iface._load_announce_cache()
            self.assertNotIn(DEST, iface._announce_cache,
                             "older than announce_cache_ttl: dropped exactly as the sweep would drop it")
        finally:
            restore()

    def test_a_future_dated_or_unreadable_entry_is_dropped_not_fatal(self):
        iface = self.iface
        _owner, _dispatched, _decisions, restore = self._install()
        try:
            announce = build_rns_packet("announce", dest_hash=DEST, payload=b"a" * 120)
            with open(self.cache_path, "w") as f:
                json.dump({"announces": [
                    {"destination_hash": DEST.hex(), "raw": announce.hex(), "source_peer": PEER,
                     "cached_at_wall": time.time() + 3600.0},           # clock moved back
                    {"destination_hash": "zz", "raw": "zz", "source_peer": PEER, "cached_at_wall": time.time()},
                    {"raw": announce.hex()},                            # missing keys
                ]}, f)
            iface._announce_cache.clear()
            iface._load_announce_cache()
            self.assertEqual(len(iface._announce_cache), 0)
        finally:
            restore()

    def test_a_missing_or_corrupt_file_is_not_fatal(self):
        iface = self.iface
        _owner, _dispatched, _decisions, restore = self._install()
        try:
            iface._load_announce_cache()                                 # no file at all
            self.assertEqual(len(iface._announce_cache), 0)
            with open(self.cache_path, "w") as f:
                f.write("{not json")
            iface._load_announce_cache()
            self.assertEqual(len(iface._announce_cache), 0)
        finally:
            restore()

    def test_the_cache_is_not_written_when_nothing_changed(self):
        iface = self.iface
        _owner, _dispatched, _decisions, restore = self._install()
        try:
            iface._announce_cache_dirty = False
            iface._save_announce_cache()
            self.assertFalse(os.path.isfile(self.cache_path), "a no-op unless something changed")
        finally:
            restore()

    # -- the verification rate -------------------------------------------

    def test_one_verification_per_interval_goes_on_air(self):
        iface = self.iface
        owner, dispatched, decisions, restore = self._install()
        saved_interval = iface.path_request_local_answer_min_interval_s
        iface.path_request_local_answer_min_interval_s = 120.0
        try:
            announce = build_rns_packet("announce", dest_hash=DEST, payload=b"a" * 120)
            self.on_loop(lambda: iface.process_incoming(
                announce, transport="direct_raw_multifragment", sender_peer_prefix=PEER))
            owner.received.clear()
            del dispatched[:]

            # The laptop's cadence that evening: a re-request every ~35 s.
            # The old rule put two of every three on the air.
            for _ in range(4):
                iface._path_request_last_sent_at.clear()
                self._send_request(iface)
            self.assertEqual(dispatched, [], "every one answered from the cache inside the interval")
            self.assertEqual(len(owner.received), 4)

            # Past the interval, exactly one goes out to re-verify.
            raw, cached_at, src, verified_at = iface._announce_cache[DEST]
            iface._announce_cache[DEST] = (raw, cached_at, src, verified_at - 121.0)
            iface._path_request_last_sent_at.clear()
            self._send_request(iface)
            self.assertEqual(len(dispatched), 1, "the periodic verification is kept")
            self.assertEqual(len(owner.received), 4)

            # ... and it re-arms the cache, so the next is local again.
            iface._path_request_last_sent_at.clear()
            self._send_request(iface)
            self.assertEqual(len(dispatched), 1)
            self.assertEqual(len(owner.received), 5)
        finally:
            iface.path_request_local_answer_min_interval_s = saved_interval
            restore()

    def test_a_restored_entry_is_verified_from_the_restore_instant(self):
        """Coming up is not evidence that a destination died, so a restored
        entry answers the first request and the verification falls due one
        interval later -- not immediately, which is what made the restart
        expensive in the field."""
        iface = self.iface
        _owner, dispatched, _decisions, restore = self._install()
        try:
            announce = build_rns_packet("announce", dest_hash=DEST, payload=b"a" * 120)
            with open(self.cache_path, "w") as f:
                json.dump({"announces": [{
                    "destination_hash": DEST.hex(), "raw": announce.hex(), "source_peer": PEER,
                    # Cached half an hour ago: well past the 120 s interval,
                    # well inside the one-hour TTL.
                    "cached_at_wall": time.time() - 1800.0,
                }]}, f)
            iface._announce_cache.clear()
            iface._load_announce_cache()
            _raw, cached_at, _src, verified_at = iface._announce_cache[DEST]
            self.assertGreater(verified_at, cached_at,
                               "verified at the restore instant, cached at its true age")
            # The cache time stays truthful, so the TTL still applies.
            self.assertAlmostEqual(time.monotonic() - cached_at, 1800.0, delta=30.0)
            del dispatched[:]
            self._send_request(iface)
            self.assertEqual(dispatched, [], "answered locally although the entry is half an hour old")
        finally:
            restore()


if __name__ == "__main__":
    unittest.main()
