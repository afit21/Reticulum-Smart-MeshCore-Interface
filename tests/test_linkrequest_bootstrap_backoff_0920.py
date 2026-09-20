"""
The unknown-destination bootstrap backoff meeting a Link handshake
(2026-09-20). MeshChat opens a Link to a destination this interface has no
RNS token for: the LINKREQUEST (packet type LINKREQUEST, destination SINGLE,
PRIORITY_HANDSHAKE) is routed as an unknown destination -- in small-mesh mode
DIRECT to every bound peer (`small_mesh_direct_all_unknown_dest`), otherwise
a CHANNEL broadcast plus a capped DIRECT bootstrap supplement -- and its
LRPROOF comes back with the LINK ID, not the destination hash, in its
destination field. `tests/test_channel_proof_backoff_0920.py` covers a DATA
send's PROOF over CHANNEL (commit 1b69fa7) and
`tests/test_field_fixes_0919_evening.py::ProofsDoNotArmUnknownDestBackoff`
covers outgoing proofs; nothing covered the handshake.

Ground truth is the code, read 2026-09-20 (where the module docstring
differs, the code wins and the test says so):

  * `_dispatch_outgoing_packet` exempts nothing but PROOFs (`_proof_like`)
    from `_record_unknown_dest_attempt`: a LINKREQUEST counts like DATA, and
    the third one (`UNKNOWN_DEST_BOOTSTRAP_FAILURE_THRESHOLD`) arms a
    `UNKNOWN_DEST_BOOTSTRAP_BASE_COOLDOWN_S` (300 s) backoff. Only in
    small-mesh mode is a backed-off packet DROPPED (`unknown_dest_backoff_
    drop`); past the small-mesh cap it is still broadcast, without the
    DIRECT supplement (`unknown_dest_backoff_broadcast_only`). With no bound
    peer at all nothing is ever counted: the backoff is about DIRECT
    bootstrap airtime, and there is none to spend.
  * The correlation link_id -> requested destination EXISTS:
    `_send_outgoing_packet` records it in `_pending_link_requests` for every
    outgoing LINKREQUEST (`_compute_link_id`, RNS's own
    `Link.link_id_from_lr_packet` re-derived), and `_note_channel_proof`
    (1b69fa7) consults that table after `_pending_dest_proofs`, so an LRPROOF
    received over CHANNEL clears the requested destination's backoff -- and
    teaches no token, as every CHANNEL receive. (`_pending_link_request_
    sweep`'s docstring still says an LRPROOF that "arrived via CHANNEL"
    leaves its entry behind with nothing learned from it; since 1b69fa7 the
    CHANNEL arrival pops the entry.)
  * Popping the entry is a one-shot: a DIRECT copy of the same LRPROOF that
    arrives AFTER the CHANNEL copy finds no correlation and learns no token
    for the destination or the link_id -- `_observe_incoming_rns_packet`'s
    PROOF branch then takes its "§7 exception" path. The reply to a
    LINKREQUEST that reached the peer over CHANNEL only is routed there as
    an unknown destination -- broadcast PLUS a DIRECT bootstrap supplement
    beyond the small-mesh cap -- so both copies do arrive here in that
    topology, and the order decides whether the token is learned. Pinned as
    a known limitation, not asserted as desirable.
"""
import os
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet, wait_until

PEER = "abcdef012345"
OTHER_PEERS = ["111111111111", "222222222222", "333333333333"]


def _link_id_the_rns_way(link_request_raw: bytes) -> bytes:
    """`RNS.Link.link_id_from_lr_packet` on packed bytes: hashable part
    (flags nibble + everything past the two-byte header for HEADER_1),
    minus the packet data beyond ECPUBSIZE. Independent of the interface's
    `_compute_link_id`, so the test cross-checks it."""
    dst_len = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
    hashable = bytes([link_request_raw[0] & 0x0F]) + link_request_raw[2:]
    data_len = len(link_request_raw) - (2 + dst_len + 1)
    if data_len > RNS.Link.ECPUBSIZE:
        hashable = hashable[:-(data_len - RNS.Link.ECPUBSIZE)]
    return RNS.Identity.truncated_hash(hashable)


def _link_request(dest):
    # a real LINKREQUEST carries two 32-byte public keys: 64 bytes of data
    return build_rns_packet("link_request", dest_hash=dest, payload=os.urandom(RNS.Link.ECPUBSIZE))


class _BootstrapSandbox(SingleNodeCase):
    """The single-node fixture with the radio side of the dispatcher
    replaced: bound peers as plain registry entries (small-mesh mode is
    `0 < len(_peers) <= 3`), DIRECT-to-all and the CHANNEL broadcast
    swallowed, `_capture_outgoing` recording every routing decision."""

    def _install(self, peers):
        iface = self.iface
        saved = {k: getattr(iface, k) for k in ("_send_direct_to_all_peers", "_send_broadcast_packet", "_capture_outgoing",
                                                 "_send_direct_supplement")}
        decisions = []
        direct_all = []
        supplements = []

        async def fake_direct_all(data, header=None, priority=0, expires_at=None, spawned=None):
            direct_all.append(header)

        async def fake_broadcast(data, header, expires_at=None, spawned=None):
            pass

        async def fake_supplement(data, peer_prefix, **kwargs):
            supplements.append(peer_prefix)
            return True

        iface._send_direct_to_all_peers = fake_direct_all
        iface._send_broadcast_packet = fake_broadcast
        iface._send_direct_supplement = fake_supplement
        iface._capture_outgoing = lambda header, data, decision, target_peer=None, candidate_peers=None: decisions.append(
            {"decision": decision, "header": header, "candidate_peers": candidate_peers})
        for p in peers:
            iface._peers[p] = self.module._PeerRecord(pubkey_prefix=p, has_upstream_rns=False, last_seen=time.time())

        def restore():
            for k, v in saved.items():
                setattr(iface, k, v)
            for p in peers:
                iface._peers.pop(p, None)
            iface._unknown_dest_attempts.clear()
            iface._unknown_dest_backoff_until.clear()
            iface._unknown_dest_last_attempt.clear()
            iface._pending_link_requests.clear()
            iface._pending_dest_proofs.clear()
            iface._outgoing_inflight.clear()
            for k in [k for k in iface._rns_token_peer if iface._rns_token_peer[k] == PEER]:
                iface._rns_token_peer.pop(k, None)
        return decisions, direct_all, supplements, restore

    def _send(self, data, decisions):
        """`process_outgoing` as RNS calls it, then wait for the worker to
        dispatch it (one more routing decision recorded)."""
        before = len(decisions)
        self.on_loop(self.iface.process_outgoing, data)
        self.assertTrue(wait_until(lambda: len(decisions) > before, 5.0), "the outgoing worker never dispatched")
        return decisions[-1]

    def _receive_over_channel(self, data):
        self.on_loop(lambda: self.iface.process_incoming(data, transport="channel_bare"))


class LinkRequestsArmTheBackoffLikeData(_BootstrapSandbox):
    def test_third_linkrequest_arms_the_backoff_and_the_fourth_is_dropped_in_small_mesh_mode(self):
        """One bound peer (small-mesh mode). Three LINKREQUESTs to one
        unknown destination each go `small_mesh_direct_all_unknown_dest`
        and each count; the third arms the backoff; a fourth is dropped
        with `unknown_dest_backoff_drop`, never reaching DIRECT-to-all.
        PRIORITY_HANDSHAKE buys a LINKREQUEST no exemption here -- only
        `_proof_like` packets are exempt."""
        iface = self.iface
        decisions, direct_all, _sup, restore = self._install([PEER])
        dest = os.urandom(16)
        try:
            self.assertTrue(iface._in_small_mesh_mode())
            self.assertEqual(iface.UNKNOWN_DEST_BOOTSTRAP_FAILURE_THRESHOLD, 3)
            self.assertEqual(iface.UNKNOWN_DEST_BOOTSTRAP_BASE_COOLDOWN_S, 300.0)
            header = iface._parse_rns_header(_link_request(dest))
            self.assertEqual(iface._priority_tier(header), iface.PRIORITY_HANDSHAKE)
            self.assertFalse(iface._proof_like(header), "a LINKREQUEST is not proof-like, so it is counted")

            for n in (1, 2, 3):
                got = self._send(_link_request(dest), decisions)
                self.assertEqual(got["decision"], "small_mesh_direct_all_unknown_dest", f"LINKREQUEST {n}")
                self.assertEqual(got["candidate_peers"], [PEER])
                self.assertEqual(iface._unknown_dest_attempts.get(dest), n)
                self.assertEqual(len(direct_all), n)
                self.assertEqual(iface._unknown_dest_in_backoff(dest), n >= 3, f"after LINKREQUEST {n}")
            until = iface._unknown_dest_backoff_until[dest] - time.monotonic()
            self.assertGreater(until, 290.0)
            self.assertLessEqual(until, 300.0)

            dropped_before = iface._outgoing_dropped_total
            got = self._send(_link_request(dest), decisions)
            self.assertEqual(got["decision"], "unknown_dest_backoff_drop")
            self.assertEqual(iface._outgoing_dropped_total, dropped_before + 1)
            self.assertEqual(len(direct_all), 3, "a dropped LINKREQUEST must not reach DIRECT-to-all")
            self.assertEqual(iface._unknown_dest_attempts.get(dest), 3, "a drop is not another attempt")
            # each LINKREQUEST left its link_id -> destination correlation behind
            self.assertEqual([d for d, _exp in iface._pending_link_requests.values()].count(dest), 4,
                             "every LINKREQUEST (the dropped one too, recorded before dispatch) is pending")
        finally:
            restore()

    def test_with_no_bound_peer_a_linkrequest_is_broadcast_and_never_counted(self):
        """Zero bound peers: not small-mesh mode, no bootstrap supplement
        targets, so the LINKREQUEST is a plain broadcast
        (`broadcast_bootstrap_supplement` with no candidates) and the
        attempt counter is never touched -- five in a row arm nothing."""
        iface = self.iface
        decisions, direct_all, supplements, restore = self._install([])
        dest = os.urandom(16)
        try:
            self.assertFalse(iface._in_small_mesh_mode())
            for _ in range(5):
                got = self._send(_link_request(dest), decisions)
                self.assertEqual(got["decision"], "broadcast_bootstrap_supplement")
                self.assertEqual(got["candidate_peers"], [])
            self.assertNotIn(dest, iface._unknown_dest_attempts)
            self.assertFalse(iface._unknown_dest_in_backoff(dest))
            self.assertEqual(direct_all, [])
            self.assertEqual(supplements, [])
        finally:
            restore()

    def test_past_the_small_mesh_cap_a_backed_off_linkrequest_is_still_broadcast_not_dropped(self):
        """Four bound peers (above SMALL_MESH_DIRECT_ONLY_MAX_PEERS): the
        first LINKREQUESTs go broadcast + capped DIRECT bootstrap supplement
        and count; once backed off, the LINKREQUEST is still broadcast
        (`unknown_dest_backoff_broadcast_only`), only the supplement is
        withheld. The drop is a small-mesh-only consequence."""
        iface = self.iface
        peers = [PEER] + OTHER_PEERS
        decisions, direct_all, supplements, restore = self._install(peers)
        dest = os.urandom(16)
        try:
            self.assertFalse(iface._in_small_mesh_mode())
            self.assertGreater(len(peers), iface.SMALL_MESH_DIRECT_ONLY_MAX_PEERS)
            for n in (1, 2, 3):
                got = self._send(_link_request(dest), decisions)
                self.assertEqual(got["decision"], "broadcast_bootstrap_supplement", f"LINKREQUEST {n}")
                self.assertEqual(len(got["candidate_peers"]), min(len(peers), iface.bootstrap_direct_supplement_cap))
                self.assertEqual(iface._unknown_dest_attempts.get(dest), n)
            self.assertTrue(iface._unknown_dest_in_backoff(dest))
            sent_supplements = len(supplements)
            dropped_before = iface._outgoing_dropped_total
            got = self._send(_link_request(dest), decisions)
            self.assertEqual(got["decision"], "unknown_dest_backoff_broadcast_only")
            self.assertEqual(got["candidate_peers"], [])
            self.assertEqual(iface._outgoing_dropped_total, dropped_before, "not dropped: the broadcast still goes")
            self.assertTrue(wait_until(lambda: len(supplements) == sent_supplements, 1.0))
            self.assertEqual(len(supplements), sent_supplements, "no DIRECT supplement while backed off")
            self.assertEqual(direct_all, [], "DIRECT-to-all is small-mesh only")
        finally:
            restore()


class ChannelLrproofClearsTheBackoff(_BootstrapSandbox):
    def test_channel_lrproof_for_a_bootstrap_linkrequest_clears_the_backoff_without_learning(self):
        """Three LINKREQUESTs arm the backoff. The LRPROOF (PROOF, context
        LRPROOF, destination field = link_id of the third request) arrives
        over CHANNEL: the interface correlates link_id -> requested
        destination through `_pending_link_requests`, clears that
        destination's backoff and attempt count, pops that one entry, and
        learns no token for either the destination or the link_id. The
        next LINKREQUEST is routed again, not dropped, with the counter
        starting over at 1."""
        iface = self.iface
        decisions, direct_all, _sup, restore = self._install([PEER])
        dest = os.urandom(16)
        try:
            requests = [_link_request(dest) for _ in range(3)]
            for req in requests:
                self._send(req, decisions)
            self.assertTrue(iface._unknown_dest_in_backoff(dest))
            self.assertEqual(self._send(_link_request(dest), decisions)["decision"], "unknown_dest_backoff_drop")

            link_id = _link_id_the_rns_way(requests[2])
            self.assertEqual(iface._compute_link_id(requests[2]), link_id, "the interface's link_id matches RNS's derivation")
            self.assertEqual(iface._pending_link_requests[link_id][0], dest)
            lrproof = build_rns_packet("lrproof", dest_hash=link_id)
            header = iface._parse_rns_header(lrproof)
            self.assertEqual((header.packet_type, header.context, header.destination_type),
                             (RNS.Packet.PROOF, RNS.Packet.LRPROOF, RNS.Destination.LINK))
            self.assertEqual(header.destination_hash, link_id, "an LRPROOF carries the link_id, not the destination hash")

            inbound_before = len(self.node.owner.received)
            self._receive_over_channel(lrproof)
            self.assertEqual(len(self.node.owner.received), inbound_before + 1, "the LRPROOF still reaches RNS")
            self.assertFalse(iface._unknown_dest_in_backoff(dest), "a correlated CHANNEL LRPROOF clears the backoff")
            self.assertNotIn(dest, iface._unknown_dest_attempts)
            self.assertNotIn(link_id, iface._pending_link_requests, "the matched entry is consumed")
            self.assertEqual(sum(1 for d, _e in iface._pending_link_requests.values() if d == dest), 3,
                             "the other requests' entries (and the dropped one's) stay until their TTL")
            self.assertNotIn(dest, iface._rns_token_peer, "a CHANNEL LRPROOF teaches no destination token")
            self.assertNotIn(link_id, iface._rns_token_peer, "nor a link_id token")

            got = self._send(_link_request(dest), decisions)
            self.assertEqual(got["decision"], "small_mesh_direct_all_unknown_dest", "the next LINKREQUEST is not dropped")
            self.assertEqual(iface._unknown_dest_attempts.get(dest), 1)
        finally:
            restore()

    def test_an_lrproof_for_an_unknown_link_id_changes_nothing(self):
        iface = self.iface
        decisions, _da, _sup, restore = self._install([PEER])
        dest = os.urandom(16)
        try:
            for _ in range(3):
                self._send(_link_request(dest), decisions)
            self.assertTrue(iface._unknown_dest_in_backoff(dest))
            self._receive_over_channel(build_rns_packet("lrproof", dest_hash=os.urandom(16)))
            self.assertTrue(iface._unknown_dest_in_backoff(dest), "an uncorrelated LRPROOF must not clear anything")
            self.assertEqual(iface._unknown_dest_attempts.get(dest), 3)
        finally:
            restore()

    def test_channel_lrproof_consumes_the_correlation_so_a_later_direct_copy_learns_no_token(self):
        """Known limitation (code as of 1b69fa7, pinned so a change is
        deliberate). Control first: an LRPROOF arriving DIRECT from the
        bound peer with the correlation intact learns tokens for BOTH the
        destination and the link_id (`_observe_incoming_rns_packet`'s PROOF
        branch) and clears the backoff. Then the case at issue: when a
        CHANNEL copy of the LRPROOF arrives first (the peer that got our
        LINKREQUEST over CHANNEL replies broadcast + DIRECT supplement past
        the small-mesh cap), `_note_channel_proof` pops the entry, and the
        DIRECT copy that follows learns nothing -- the destination stays
        token-less and the next Link traffic to it is bootstrap-routed
        again."""
        iface = self.iface
        decisions, _da, _sup, restore = self._install([PEER])
        try:
            # control: DIRECT copy, correlation intact
            dest_a = os.urandom(16)
            req_a = _link_request(dest_a)
            for _ in range(2):
                self._send(_link_request(dest_a), decisions)
            self._send(req_a, decisions)
            self.assertTrue(iface._unknown_dest_in_backoff(dest_a))
            link_a = iface._compute_link_id(req_a)
            self.on_loop(lambda: iface._observe_incoming_rns_packet(build_rns_packet("lrproof", dest_hash=link_a), PEER))
            self.assertEqual(iface._rns_token_peer.get(dest_a), PEER, "DIRECT LRPROOF: destination token learned")
            self.assertEqual(iface._rns_token_peer.get(link_a), PEER, "DIRECT LRPROOF: link_id token learned")
            self.assertFalse(iface._unknown_dest_in_backoff(dest_a))

            # the case at issue: CHANNEL copy first, then the DIRECT copy
            dest_b = os.urandom(16)
            req_b = _link_request(dest_b)
            self._send(req_b, decisions)
            link_b = iface._compute_link_id(req_b)
            lrproof_b = build_rns_packet("lrproof", dest_hash=link_b)
            self._receive_over_channel(lrproof_b)
            self.assertNotIn(link_b, iface._pending_link_requests)
            self.on_loop(lambda: iface._observe_incoming_rns_packet(lrproof_b, PEER))
            self.assertNotIn(dest_b, iface._rns_token_peer,
                             "as coded: the CHANNEL copy consumed the correlation, the DIRECT copy learns no destination token")
            self.assertNotIn(link_b, iface._rns_token_peer, "nor a link_id token")
            # so the next packet for that destination is bootstrap-routed again
            self.assertIsNone(iface._resolve_routing_peer(iface._parse_rns_header(build_rns_packet("data", dest_hash=dest_b))))
        finally:
            restore()


class LinkRequestAfterAChannelDataProofClearedTheBackoff(_BootstrapSandbox):
    def test_linkrequest_straight_after_the_clear_is_routed_not_dropped(self):
        """The 1b69fa7 case from the LINKREQUEST's side: three DATA
        bootstrap sends arm the backoff and a LINKREQUEST to the same
        destination is dropped; a plain PROOF for one of the DATA sends
        then arrives over CHANNEL and clears it; the LINKREQUEST retried
        right after goes out (`small_mesh_direct_all_unknown_dest`) and
        the counter restarts at 1, not 4."""
        iface = self.iface
        decisions, direct_all, _sup, restore = self._install([PEER])
        dest = os.urandom(16)
        try:
            datas = [build_rns_packet("data", dest_hash=dest, payload=b"bootstrap-%d" % n) for n in range(3)]
            for data in datas:
                got = self._send(data, decisions)
                self.assertEqual(got["decision"], "small_mesh_direct_all_unknown_dest")
            self.assertTrue(iface._unknown_dest_in_backoff(dest))
            self.assertEqual(self._send(_link_request(dest), decisions)["decision"], "unknown_dest_backoff_drop")
            sent_before_proof = len(direct_all)

            proved = datas[1]
            truncated = iface._compute_truncated_hash(proved, iface._parse_rns_header(proved).header_type)
            self.assertEqual(iface._pending_dest_proofs[truncated][0], dest)
            self._receive_over_channel(build_rns_packet("proof", dest_hash=truncated))
            self.assertFalse(iface._unknown_dest_in_backoff(dest))
            self.assertNotIn(dest, iface._rns_token_peer)

            got = self._send(_link_request(dest), decisions)
            self.assertEqual(got["decision"], "small_mesh_direct_all_unknown_dest")
            self.assertEqual(len(direct_all), sent_before_proof + 1, "the LINKREQUEST reached DIRECT-to-all")
            self.assertEqual(iface._unknown_dest_attempts.get(dest), 1)
            self.assertFalse(iface._unknown_dest_in_backoff(dest))
        finally:
            restore()


if __name__ == "__main__":
    unittest.main()
