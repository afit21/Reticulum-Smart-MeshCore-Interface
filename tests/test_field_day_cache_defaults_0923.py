"""Alpha 0.1.9, item 3: the announce cache's defaults must survive a day in
the field, and the on-air verification must cost one request per interval
rather than one every couple of minutes.

Field evidence (2026-09-23, `fieldtests/raw/Alpha0.1.8/`). The session had
two stops, 09:25-09:48 and 11:40-12:07 -- about two hours apart. Alpha
0.1.8's `announce_cache_ttl` was 3600 s, so every entry cached at the first
stop had expired before the second began and eight announces went over the
air again at two hops (`direct_raw_multifragment` announces in the laptop's
11:28 capture; the desktop's `announces out` between 11:41 and 12:04) for
destinations it had already held once that morning. Separately, with
`path_request_local_answer_min_interval` at 120 s, the verification rule put
six requests on the air for three destinations in the three and a half
minutes between 11:40:51 and 11:44:16 -- each one a relayed DIRECT request
answered with a multi-fragment announce window.

The TTL is now a week, which is what RNS itself keeps: a path learned over a
MODE_FULL interface expires at `Transport.PATHFINDER_E` and the path table is
culled at `Transport.DESTINATION_TIMEOUT`, both 60*60*24*7 (`RNS/Transport.py`,
read 2026-09-23). The cache therefore expires exactly when the answering
node's own record of the same announce would, and never later. What bounds a
long TTL is liveness, not age: the entry answers only while the peer that
delivered it is still bound and out of path-discovery backoff, and the
periodic on-air verification still runs -- now once per ten minutes per
destination.
"""
import json
import os
import tempfile
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet

PEER = "abcdef012345"
DEST = bytes.fromhex("6b9f66014d9853faab220fba47d02761")
# The gap between the 2026-09-23 session's two stops.
TWO_HOURS = 2 * 60 * 60.0


class _FakeOwner:
    def __init__(self):
        self.received = []

    def inbound(self, data, interface):
        self.received.append(bytes(data))


class FieldDayCacheDefaults(SingleNodeCase):
    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.mkdtemp(prefix="smci-fieldday-")
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

    def _age_entry(self, iface, cached_by: float, verified_by: float) -> None:
        """Back-date the cached entry: the cache holds monotonic stamps."""
        raw, cached_at, source_peer, verified_at = iface._announce_cache[DEST]
        iface._announce_cache[DEST] = (raw, cached_at - cached_by, source_peer, verified_at - verified_by)

    # -- the TTL ----------------------------------------------------------

    def test_the_shipped_ttl_is_what_rns_itself_keeps_a_path_for(self):
        # Not an arbitrary number: it is the lifetime of the answering
        # node's own record of the same announce. `_configure_transport`'s
        # comment cites these two; this pins them against the vendored RNS.
        self.assertEqual(self.iface.announce_cache_ttl_s, float(RNS.Transport.PATHFINDER_E))
        self.assertEqual(self.iface.announce_cache_ttl_s, float(RNS.Transport.DESTINATION_TIMEOUT))
        self.assertGreaterEqual(self.iface.announce_cache_ttl_s, 24 * 60 * 60.0,
                                "a day is the floor: a field session spans one")

    def test_an_entry_cached_two_hours_ago_still_answers_locally(self):
        iface = self.iface
        owner, dispatched, decisions, restore = self._install()
        try:
            announce = build_rns_packet("announce", dest_hash=DEST, payload=b"a" * 120)
            self.on_loop(lambda: iface.process_incoming(
                announce, transport="direct_raw_multifragment", sender_peer_prefix=PEER))
            self.assertIn(DEST, iface._announce_cache)

            # The two hours between the 2026-09-23 stops. The entry was
            # verified when it was cached, so age both stamps together:
            # this is exactly the state the laptop drove back into at 11:40.
            self._age_entry(iface, cached_by=TWO_HOURS, verified_by=0.0)

            del dispatched[:]
            owner.received.clear()
            self._send_request(iface)
            self.assertEqual(dispatched, [],
                             "at 0.1.8's 3600 s TTL this entry had expired and the announce went on air again")
            self.assertEqual(decisions[-1], ("path_request_answered_locally", PEER))
            self.assertEqual(len(owner.received), 1)
            self.assertIn(DEST, iface._announce_cache, "answering does not consume the entry")
        finally:
            restore()

    def test_a_restart_two_hours_later_still_answers_locally(self):
        # The persisted half of the same story: 0.1.8 made the cache
        # survive a restart, but the load-time age cap then threw a
        # two-hour-old entry away anyway.
        iface = self.iface
        owner, dispatched, decisions, restore = self._install()
        try:
            announce = build_rns_packet("announce", dest_hash=DEST, payload=b"a" * 120)
            with open(self.cache_path, "w") as f:
                json.dump({"announces": [{
                    "destination_hash": DEST.hex(), "raw": announce.hex(), "source_peer": PEER,
                    "cached_at_wall": time.time() - TWO_HOURS,
                }]}, f)
            iface._announce_cache.clear()
            iface._load_announce_cache()
            self.assertIn(DEST, iface._announce_cache)

            del dispatched[:]
            owner.received.clear()
            self._send_request(iface)
            self.assertEqual(dispatched, [])
            self.assertEqual(decisions[-1], ("path_request_answered_locally", PEER))
        finally:
            restore()

    def test_an_entry_older_than_a_week_is_still_dropped(self):
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
                             "the TTL is longer, not absent")
        finally:
            restore()

    # -- the verification interval ----------------------------------------

    def test_the_verification_costs_one_request_per_ten_minutes(self):
        iface = self.iface
        _owner, _dispatched, _decisions, restore = self._install()
        try:
            announce = build_rns_packet("announce", dest_hash=DEST, payload=b"a" * 120)
            self.on_loop(lambda: iface.process_incoming(
                announce, transport="direct_raw_multifragment", sender_peer_prefix=PEER))

            self.assertEqual(iface.path_request_local_answer_min_interval_s, 600.0)

            # The field's 11:40:51 -> 11:44:16 span: 205 s of RNS
            # re-requesting. At 120 s that put two verifications on the air
            # per destination; at 600 s it puts none, because the entry was
            # verified when it was cached.
            for elapsed in (0.0, 70.0, 106.0, 205.0):
                self._age_entry(iface, cached_by=0.0, verified_by=elapsed)
                self.assertEqual(iface._answer_path_request_locally(DEST), PEER,
                                 f"{elapsed:.0f}s after the last verification: answered locally")
                # _age_entry is relative, so undo this step's ageing.
                self._age_entry(iface, cached_by=0.0, verified_by=-elapsed)

            # One interval later the verification does fall due, and
            # dispatching it re-arms the entry for another ten minutes.
            self._age_entry(iface, cached_by=0.0, verified_by=601.0)
            self.assertIsNone(iface._answer_path_request_locally(DEST),
                              "a genuinely dead destination is still re-checked, once per interval")
            iface._note_path_request_on_air(DEST)
            self.assertEqual(iface._answer_path_request_locally(DEST), PEER,
                             "the request on the air IS the verification: re-armed")
        finally:
            restore()


if __name__ == "__main__":
    unittest.main()
