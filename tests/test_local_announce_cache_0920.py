"""
An RNS path re-request is answered from the announce this interface already
delivered (phase 1, 2026-09-20; `announce_cache_ttl`,
`path_request_local_answer_min_interval`).

RNS mechanics this rests on (`referenceprojects/Reticulum-master/RNS/
Transport.py`): on a non-transport node a pending Link that closes without
activating calls `expire_path` and re-requests the path (`jobs`, the
pending-links check); the cull removes the destination, so `has_path` is
False when the request reaches this interface; the answering node replies
from its own path table with the CACHED announce bytes (`path_request`,
`get_cached_packet`); `packet_filter` passes a duplicate SINGLE announce
even when its hash is in the hashlist; and the announce branch of `inbound`
adds an unknown destination unconditionally (`should_add = True`). A
transport node would insert a context-NONE announce into its announce table
for re-flooding, so the re-injected copy carries context PATH_RESPONSE.

Field evidence: laptop captures `*144922` / `*153130` (2 hops) received the
identical 235-byte announce for one destination six times in an hour, each a
2-3 fragment raw send with reports at two hops, each after a 2-hop DIRECT
path request (plus 14 rate-limited repeats); the desktop answered 7 and
suppressed 5 as duplicates in flight.

Pinned:
  * an ANNOUNCE delivered DIRECT by a bound peer is cached; one received
    over CHANNEL, or from an unbound sender, is not;
  * a path request for a cached destination is answered locally: the cached
    bytes go back to RNS with context PATH_RESPONSE and nothing else changed,
    the request is not dispatched, the record says
    `path_request_answered_locally` and names the source peer;
  * a second request inside the interval, a request for an uncached hash, and
    a request whose source peer is no longer bound (or is in discovery
    backoff) go on air; an expired cache entry is evicted;
  * every path-request capture record carries `requested_hash`.
"""
import os
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet

PEER = "abcdef012345"
DEST = bytes.fromhex("d4c70c4b0a7e67265fa8982e36b43c05")


class _FakeOwner:
    def __init__(self):
        self.received = []

    def inbound(self, data, interface):
        self.received.append(bytes(data))


class LocalAnnounceCache(SingleNodeCase):
    def _install(self):
        iface = self.iface
        saved = {k: getattr(iface, k) for k in ("owner", "_dispatch_outgoing_packet", "_capture_outgoing")}
        owner = _FakeOwner()
        dispatched, decisions = [], []

        async def fake_dispatch(data, header, expires_at=None, spawned=None):
            dispatched.append(header)

        iface.owner = owner
        iface._dispatch_outgoing_packet = fake_dispatch
        iface._capture_outgoing = lambda header, data, decision, target_peer=None, candidate_peers=None: decisions.append(
            (decision, target_peer))
        iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=False, last_seen=time.time())
        iface._announce_cache.clear()
        iface._path_request_local_answer_at.clear()

        def restore():
            for k, v in saved.items():
                setattr(iface, k, v)
            iface._peers.pop(PEER, None)
            iface._announce_cache.clear()
            iface._path_request_local_answer_at.clear()
            iface._path_request_last_sent_at.clear()
            iface._path_discovery_backoff_until.pop(PEER, None)

        return owner, dispatched, decisions, restore

    def _request(self, requested: bytes) -> bytes:
        """Exactly `RNS.Transport.request_path`'s packet: DATA to the PLAIN
        `rnstransport.path.request` destination, data = destination_hash +
        request tag (a non-transport node), 51 bytes -- the field's size."""
        dst = RNS.Destination(None, RNS.Destination.OUT, RNS.Destination.PLAIN, "rnstransport", "path", "request")
        pkt = RNS.Packet(dst, requested + RNS.Identity.get_random_hash(), packet_type=RNS.Packet.DATA,
                         transport_type=RNS.Transport.BROADCAST, header_type=RNS.Packet.HEADER_1, create_receipt=False)
        pkt.pack()
        return pkt.raw

    def test_direct_announce_is_cached_channel_and_unbound_are_not(self):
        iface = self.iface
        owner, dispatched, decisions, restore = self._install()
        try:
            announce = build_rns_packet("announce", dest_hash=DEST, payload=b"a" * 120)
            self.on_loop(lambda: iface.process_incoming(announce, transport="channel_bare", channel_sender_claimed="x"))
            self.assertNotIn(DEST, iface._announce_cache, "CHANNEL: unauthenticated source, not cached")
            self.on_loop(lambda: iface.process_incoming(announce, transport="direct_bare", sender_peer_prefix="ffffffffffff"))
            self.assertNotIn(DEST, iface._announce_cache, "unbound sender: not cached")
            self.on_loop(lambda: iface.process_incoming(announce, transport="direct_raw_multifragment", sender_peer_prefix=PEER))
            self.assertIn(DEST, iface._announce_cache)
            raw, _t, src, _verified_at = iface._announce_cache[DEST]
            self.assertEqual(raw, announce)
            self.assertEqual(src, PEER)
        finally:
            restore()

    def test_re_request_is_answered_locally_then_verified_on_air(self):
        iface = self.iface
        owner, dispatched, decisions, restore = self._install()
        saved_interval = iface.path_request_local_answer_min_interval_s
        iface.path_request_local_answer_min_interval_s = 120.0
        try:
            announce = build_rns_packet("announce", dest_hash=DEST, payload=b"a" * 120)
            self.on_loop(lambda: iface.process_incoming(announce, transport="direct_raw_multifragment", sender_peer_prefix=PEER))
            owner.received.clear()

            request = self._request(DEST)
            self.node.run_on_loop(iface._send_outgoing_packet(request, iface._parse_rns_header(request)), timeout=10.0)
            self.assertEqual(dispatched, [], "answered locally: the request must not go on air")
            self.assertEqual(decisions[-1], ("path_request_answered_locally", PEER))
            self.assertEqual(len(owner.received), 1, "one announce handed back to RNS")
            got = owner.received[0]
            self.assertEqual(len(got), len(announce))
            header = iface._parse_rns_header(got)
            self.assertEqual(header.packet_type, RNS.Packet.ANNOUNCE)
            self.assertEqual(header.destination_hash, DEST)
            self.assertEqual(header.context, RNS.Packet.PATH_RESPONSE, "context rewritten so a transport node does not re-flood it")
            ctx = (2 + 2 * 16) if header.header_type == 1 else (2 + 16)
            self.assertEqual(got[:ctx] + got[ctx + 1:], announce[:ctx] + announce[ctx + 1:], "every other byte as received")

            # Reversed by alpha 0.1.8 (item 4): a re-request INSIDE the
            # interval is answered from the cache too. The old rule capped
            # the local answers at one per interval and let every other
            # request transmit; with RNS re-requesting every 30-70 s the
            # laptop's 2026-09-22 capture put 20 requests on the air for
            # this one destination against 12 answered locally.
            iface._path_request_last_sent_at.clear()
            request2 = self._request(DEST)
            self.node.run_on_loop(iface._send_outgoing_packet(request2, iface._parse_rns_header(request2)), timeout=10.0)
            self.assertEqual(dispatched, [], "still answered locally inside the interval")
            self.assertEqual(len(owner.received), 2)

            # Past the interval ONE request goes over the air: the
            # verification is kept, it is only rate-limited now.
            raw, cached_at, src, verified_at = iface._announce_cache[DEST]
            iface._announce_cache[DEST] = (raw, cached_at, src, verified_at - 121.0)
            iface._path_request_last_sent_at.clear()
            request3 = self._request(DEST)
            self.node.run_on_loop(iface._send_outgoing_packet(request3, iface._parse_rns_header(request3)), timeout=10.0)
            self.assertEqual(len(dispatched), 1, "the periodic verification")
            self.assertEqual(len(owner.received), 2)

            # ... and that verification re-arms the cache for the next one.
            iface._path_request_last_sent_at.clear()
            request4 = self._request(DEST)
            self.node.run_on_loop(iface._send_outgoing_packet(request4, iface._parse_rns_header(request4)), timeout=10.0)
            self.assertEqual(len(dispatched), 1, "answered locally again")
            self.assertEqual(len(owner.received), 3)
        finally:
            iface.path_request_local_answer_min_interval_s = saved_interval
            restore()

    def test_uncached_unbound_backed_off_and_expired_go_on_air(self):
        iface = self.iface
        owner, dispatched, decisions, restore = self._install()
        try:
            announce = build_rns_packet("announce", dest_hash=DEST, payload=b"a" * 120)
            self.on_loop(lambda: iface.process_incoming(announce, transport="direct_bare", sender_peer_prefix=PEER))
            owner.received.clear()

            def send(requested):
                iface._path_request_last_sent_at.clear()
                req = self._request(requested)
                self.node.run_on_loop(iface._send_outgoing_packet(req, iface._parse_rns_header(req)), timeout=10.0)

            send(os.urandom(16))
            self.assertEqual(len(dispatched), 1, "uncached destination: on air")

            iface._path_discovery_backoff_until[PEER] = time.monotonic() + 60.0
            send(DEST)
            self.assertEqual(len(dispatched), 2, "source peer in discovery backoff: on air")
            iface._path_discovery_backoff_until.pop(PEER, None)

            iface._peers.pop(PEER)
            send(DEST)
            self.assertEqual(len(dispatched), 3, "source peer no longer bound: on air")
            iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=False, last_seen=time.time())

            raw, t, src, _verified_at = iface._announce_cache[DEST]
            iface._announce_cache[DEST] = (raw, t - iface.announce_cache_ttl_s - 1.0, src, t)
            send(DEST)
            self.assertEqual(len(dispatched), 4, "expired entry: on air")
            self.assertNotIn(DEST, iface._announce_cache, "and evicted")
            self.assertEqual(owner.received, [])
        finally:
            restore()

    def test_capture_record_carries_the_requested_hash(self):
        iface = self.iface
        records = []
        original = iface._capture_event
        original_file = iface._packet_capture_file
        iface._capture_event = lambda direction, fields: records.append(fields)
        iface._packet_capture_file = object()   # truthy: capture "open"
        try:
            req = self._request(DEST)
            iface._capture_outgoing(iface._parse_rns_header(req), req, "path_request_rate_limited")
            self.assertEqual(records[-1]["requested_hash"], DEST.hex())
            data = build_rns_packet("data", dest_hash=DEST, payload=b"x")
            iface._capture_outgoing(iface._parse_rns_header(data), data, "direct_primary")
            self.assertIsNone(records[-1]["requested_hash"])
        finally:
            iface._capture_event = original
            iface._packet_capture_file = original_file

    @staticmethod
    def _wait_for_path(dest_hash, want=True, timeout=3.0):
        """`RNS.Transport.inbound` hands the packet to a worker rather than
        processing it on the caller's thread (`preprocess_inbound` on the
        installed RNS), so a path appears a few tens of milliseconds after
        the call returns -- measured at ~50 ms here. Asserting synchronously
        made this test depend on whatever else had run first; it passed in a
        full suite and failed on its own, on every build, until 2026-09-23.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            if RNS.Transport.has_path(dest_hash) == want:
                return True
            time.sleep(0.02)
        return RNS.Transport.has_path(dest_hash) == want

    def test_real_rns_transport_accepts_the_re_injected_announce(self):
        """Against the real `RNS.Transport` of the test process (a hermetic
        `RNS.Reticulum`, non-transport): a signed announce re-injected with
        context PATH_RESPONSE for a destination NOT in the path table adds
        the path (`has_path` True); re-injected again while the path exists
        it is ignored; after `expire_path` and a cull it is accepted again --
        the exact sequence a closed pending Link produces."""
        identity = RNS.Identity()
        dest = RNS.Destination(identity, RNS.Destination.IN, RNS.Destination.SINGLE, "smci", "cache", "test")
        packet = dest.announce(send=False)
        packet.pack()
        raw = bytes(packet.raw)
        iface = self.iface
        # Two things this test needs from RNS that the unit harness does not
        # provide, both found on 2026-09-23 when the test began failing in
        # the full suite (it had always failed on its own, on every build --
        # it was relying on state left by whichever tests ran before it).
        #
        # `Transport.preprocess_inbound` reads `interface.ifac_size`, which
        # `RNS.Reticulum` sets when IT configures an interface; None is what
        # it sets for an interface with no IFAC, which is this one
        # (`RNS/Reticulum.py`) -- the interface now sets that itself, so
        # this test exercises the shipped default rather than patching it.
        #
        # And a PATH_RESPONSE is only accepted on a NON-TRANSPORT node for a
        # destination RNS actually has an outstanding request for -- which is
        # also what exempts it from the interface's announce ingress limiter
        # ("Skipping ingress limit check ... due to waiting path requests").
        # That is exactly the production situation: this cache answers a path
        # request RNS itself just made, so `Transport.path_requests` holds the
        # hash. The second half of this test already sets it up that way and
        # says so; the first injection was relying on state left by whichever
        # tests happened to run before it, which is why it failed on its own
        # on every build.
        self.assertTrue(hasattr(iface, "ifac_size"),
                        "RNS 1.5 reads ifac_size on every inbound frame; the interface must define it")
        with RNS.Transport.path_requests_lock:
            RNS.Transport.path_requests[dest.hash] = time.time()
        # Not a local destination as far as Transport is concerned, or it
        # would answer from the destinations map instead of the path table.
        RNS.Transport.deregister_destination(dest)
        try:
            self.assertFalse(RNS.Transport.has_path(dest.hash))
            header = iface._parse_rns_header(raw)
            ctx = (2 + 2 * 16) if header.header_type == 1 else (2 + 16)
            answer = bytearray(raw)
            answer[ctx] = RNS.Packet.PATH_RESPONSE
            RNS.Transport.inbound(bytes(answer), iface)
            self.assertTrue(self._wait_for_path(dest.hash), "an unknown destination's announce is added")
            entry_before = list(RNS.Transport.path_table[dest.hash])
            RNS.Transport.inbound(bytes(answer), iface)
            time.sleep(0.2)
            self.assertEqual(RNS.Transport.path_table[dest.hash][0], entry_before[0], "a duplicate while the path exists is ignored")
            RNS.Transport.expire_path(dest.hash)
            # Cull what expire_path marked (Transport.jobs does this on its
            # next pass; done here directly so the test does not wait on it).
            with RNS.Transport.path_table_lock:
                RNS.Transport.path_table.pop(dest.hash, None)
            self.assertFalse(RNS.Transport.has_path(dest.hash))
            # `Transport.request_path` records the destination in
            # `path_requests` BEFORE it sends (Transport.py, request_path),
            # which is what exempts the answer from ingress limiting in
            # `preprocess_inbound` ("Skipping ingress limit check ... due to
            # waiting path requests"); a re-injection without it can be held
            # by the interface's ingress control. Mirrored here.
            with RNS.Transport.path_requests_lock:
                RNS.Transport.path_requests[dest.hash] = time.time()
            RNS.Transport.inbound(bytes(answer), iface)
            self.assertTrue(self._wait_for_path(dest.hash), "accepted again once the path was expired and culled")
        finally:
            with RNS.Transport.path_table_lock:
                RNS.Transport.path_table.pop(dest.hash, None)
            with RNS.Transport.path_requests_lock:
                RNS.Transport.path_requests.pop(dest.hash, None)

    def test_shipped_defaults(self):
        bare = self.module.SmartMeshCoreInterface.__new__(self.module.SmartMeshCoreInterface)
        bare._configure_path_discovery({})
        # Both re-pinned by alpha 0.1.9 (item 3): 3600 s was shorter than a
        # field day, so every entry cached at the 2026-09-23 session's first
        # stop had expired before the second two hours later, and 120 s made
        # the on-air verification cost six requests for three destinations in
        # three and a half minutes. See tests/test_field_day_cache_defaults_0923.py.
        self.assertEqual(bare.announce_cache_ttl_s, 604800.0)
        self.assertEqual(bare.path_request_local_answer_min_interval_s, 600.0)
        self.assertEqual(self.module.SmartMeshCoreInterface.ANNOUNCE_CACHE_MAX_KEYS, 256)


if __name__ == "__main__":
    unittest.main()
