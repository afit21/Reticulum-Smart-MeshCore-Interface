"""
Alpha 0.1.7, item 3 (2026-09-22): token learning never maps a local
destination, and only announce-class and Link-carried packets teach a
`destination_hash -> peer` token.

The field (desktop rnsd log, 2026-09-22 11:49:34-11:50:44 and 12:40:16):
"learned token d4c70c4b... -> '343377c464a7'" seven times, once per inbound
LXMF DATA addressed to the desktop's OWN LXMF delivery destination -- the
generic branch of `_observe_incoming_rns_packet` learned the destination
field of every non-PROOF packet, including packets addressed TO this node.
A DATA or LINKREQUEST names its recipient, which is this node's own
destination or one beyond another interface; on a transport node the learn
overwrote the announce-learned token. An ANNOUNCE (any context) and a
packet carried on a Link (destination = link_id, a bidirectional session)
are the shapes that do say "this hash lives in the sender's direction".

`d4c70c4b` was registered in MeshChat's process (a shared-instance client of
rnsd), not in rnsd's `RNS.Transport.destinations`, so the own-destination
guard also reads RNS's own local-client test (a path_table entry at zero
hops or received on a local client interface).
"""
import os
import time
import unittest

from _support import SingleNodeCase, build_rns_packet, quiet_rns

quiet_rns()
import RNS  # noqa: E402

PEER = "abcdef012345"


class _FakeLocalClientInterface:
    """What RNS's `is_local_client_interface` recognises: an interface whose
    parent has `is_local_shared_instance`."""
    class _Parent:
        is_local_shared_instance = True
    parent_interface = _Parent()


class TokenLearningRuleTests(SingleNodeCase):

    def setUp(self):
        iface = self.iface
        iface._peers.clear()
        iface._resolved_paths.clear()
        iface._rns_token_peer.clear()
        iface._proof_correlation.clear()
        iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=False, last_seen=time.time())
        iface._resolved_paths[PEER] = self.module._ResolvedPath("", 0, 1, time.monotonic())
        self._added_map = []
        self._added_paths = []

    def tearDown(self):
        for h in self._added_map:
            RNS.Transport.destinations_map.pop(h, None)
        for h in self._added_paths:
            RNS.Transport.path_table.pop(h, None)

    def _register_local(self, dest_hash):
        RNS.Transport.destinations_map[dest_hash] = object()
        self._added_map.append(dest_hash)

    def _register_client(self, dest_hash, hops=0, iface=None):
        RNS.Transport.path_table[dest_hash] = [time.time(), dest_hash, hops, time.time() + 600, [], iface, None]
        self._added_paths.append(dest_hash)

    def _observe(self, data):
        self.on_loop(self.iface._observe_incoming_rns_packet, data, PEER)

    # -- the rule --

    def test_learnable_shapes(self):
        iface = self.iface
        h = iface._parse_rns_header
        self.assertTrue(iface._token_learnable_from(h(build_rns_packet("announce", dest_hash=os.urandom(16)))))
        self.assertTrue(iface._token_learnable_from(h(build_rns_packet("path_response_announce", dest_hash=os.urandom(16)))))
        self.assertTrue(iface._token_learnable_from(h(build_rns_packet("path_response", dest_hash=os.urandom(16)))))
        self.assertTrue(iface._token_learnable_from(h(build_rns_packet("link_data", dest_hash=os.urandom(16)))))
        self.assertTrue(iface._token_learnable_from(h(build_rns_packet("resource", dest_hash=os.urandom(16)))))
        self.assertTrue(iface._token_learnable_from(h(build_rns_packet("link_close", dest_hash=os.urandom(16)))))
        self.assertFalse(iface._token_learnable_from(h(build_rns_packet("data", dest_hash=os.urandom(16)))))
        self.assertFalse(iface._token_learnable_from(h(build_rns_packet("link_request", dest_hash=os.urandom(16)))))
        self.assertFalse(iface._token_learnable_from(h(build_rns_packet("path_request", dest_hash=os.urandom(16)))))

    def test_inbound_data_to_local_destination_learns_nothing(self):
        # The field case: LXMF DATA to this node's own delivery destination.
        iface = self.iface
        dest = os.urandom(16)
        self._register_local(dest)
        data = build_rns_packet("data", dest_hash=dest, payload=b"lxmf" + os.urandom(200))
        self._observe(data)
        self.assertNotIn(dest, iface._rns_token_peer)
        # ... but the PROOF this node answers with is still routed to the peer.
        truncated = iface._compute_truncated_hash(data, iface._parse_rns_header(data).header_type)
        self.assertIn(truncated, iface._proof_correlation)
        self.assertEqual(iface._resolve_routing_peer(iface._parse_rns_header(build_rns_packet("proof", dest_hash=truncated))), PEER)

    def test_inbound_data_to_any_single_destination_learns_nothing(self):
        # A transport node: the destination lies beyond another interface;
        # the announce-learned token must survive the DATA that arrives for it.
        iface = self.iface
        dest = os.urandom(16)
        other = "0123456789ab"
        iface._peers[other] = self.module._PeerRecord(pubkey_prefix=other, has_upstream_rns=False, last_seen=time.time())
        iface._resolved_paths[other] = self.module._ResolvedPath("", 0, 1, time.monotonic())
        self.on_loop(iface._observe_incoming_rns_packet, build_rns_packet("announce", dest_hash=dest), other)
        self.assertEqual(iface._rns_token_peer.get(dest), other)
        self._observe(build_rns_packet("data", dest_hash=dest, payload=b"forwarded"))
        self.assertEqual(iface._rns_token_peer.get(dest), other, "the DATA for that destination must not overwrite the announce's token")

    def test_linkrequest_learns_link_id_not_requested_destination(self):
        iface = self.iface
        dest = os.urandom(16)
        self._register_local(dest)
        req = build_rns_packet("link_request", dest_hash=dest, payload=os.urandom(RNS.Link.ECPUBSIZE))
        self._observe(req)
        self.assertNotIn(dest, iface._rns_token_peer)
        self.assertEqual(iface._rns_token_peer.get(iface._compute_link_id(req)), PEER)

    def test_link_carried_packet_learns_link_id(self):
        iface = self.iface
        link_id = os.urandom(16)
        self._observe(build_rns_packet("link_data", dest_hash=link_id, payload=b"on the link"))
        self.assertEqual(iface._rns_token_peer.get(link_id), PEER)

    def test_announce_and_path_response_still_learn(self):
        iface = self.iface
        d1, d2, d3 = os.urandom(16), os.urandom(16), os.urandom(16)
        iface._record_unknown_dest_attempt(d1)
        self._observe(build_rns_packet("announce", dest_hash=d1, payload=os.urandom(120)))
        self._observe(build_rns_packet("path_response_announce", dest_hash=d2, payload=os.urandom(120)))
        self._observe(build_rns_packet("path_response", dest_hash=d3, payload=os.urandom(120)))
        for d in (d1, d2, d3):
            self.assertEqual(iface._rns_token_peer.get(d), PEER)
        self.assertFalse(iface._unknown_dest_in_backoff(d1))

    # -- the own-destination guard at the single entry point --

    def test_learn_refuses_a_registered_destination(self):
        iface = self.iface
        dest = os.urandom(16)
        self._register_local(dest)
        iface._learn_rns_token(dest, PEER)
        self.assertNotIn(dest, iface._rns_token_peer)
        self.assertTrue(iface._is_local_destination(dest))

    def test_learn_refuses_a_shared_instance_clients_destination(self):
        # MeshChat's delivery destination as rnsd sees it: a path_table entry
        # at zero hops (RNS's `for_local_client`) or received on a local
        # client interface (`is_local_client_interface`).
        iface = self.iface
        zero_hop, via_client, remote = os.urandom(16), os.urandom(16), os.urandom(16)
        self._register_client(zero_hop, hops=0)
        self._register_client(via_client, hops=1, iface=_FakeLocalClientInterface())
        self._register_client(remote, hops=2, iface=object())
        self.assertTrue(iface._is_local_destination(zero_hop))
        self.assertTrue(iface._is_local_destination(via_client))
        self.assertFalse(iface._is_local_destination(remote))
        for d in (zero_hop, via_client):
            iface._learn_rns_token(d, PEER)
            self.assertNotIn(d, iface._rns_token_peer)
        iface._learn_rns_token(remote, PEER)
        self.assertEqual(iface._rns_token_peer.get(remote), PEER)

    def test_announce_for_own_destination_is_not_learned(self):
        # A reflected copy of this node's own announce must not map it to a peer.
        iface = self.iface
        dest = os.urandom(16)
        self._register_local(dest)
        self._observe(build_rns_packet("announce", dest_hash=dest, payload=os.urandom(120)))
        self.assertNotIn(dest, iface._rns_token_peer)


if __name__ == "__main__":
    unittest.main()
