"""
RNS header classification against real packed RNS.Packet bytes: the
interface parses packet_type / destination_type / context /
destination_hash itself (it never decrypts), and its routing dispatcher
branches on exactly those fields. Checked here against what
RNS.Packet.unpack() says about the same bytes, for every packet kind
the dispatcher distinguishes, in both header formats.
"""
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet, TEST_DEST_HASH

KINDS = {
    "data": (RNS.Packet.DATA, RNS.Destination.SINGLE, RNS.Packet.NONE),
    "announce": (RNS.Packet.ANNOUNCE, RNS.Destination.SINGLE, RNS.Packet.NONE),
    "path_request": (RNS.Packet.DATA, RNS.Destination.PLAIN, RNS.Packet.NONE),
    "link_request": (RNS.Packet.LINKREQUEST, RNS.Destination.SINGLE, RNS.Packet.NONE),
    "proof": (RNS.Packet.PROOF, RNS.Destination.SINGLE, RNS.Packet.NONE),
    "lrproof": (RNS.Packet.PROOF, RNS.Destination.LINK, RNS.Packet.LRPROOF),
    "path_response": (RNS.Packet.DATA, RNS.Destination.SINGLE, RNS.Packet.PATH_RESPONSE),
}


def _unpacked(raw: bytes) -> RNS.Packet:
    p = RNS.Packet(None, raw)
    p.unpack()
    return p


class RnsHeaderTests(SingleNodeCase):

    def test_header_fields_match_rns_unpack_header_1(self):
        for kind, (ptype, dtype, context) in KINDS.items():
            raw = build_rns_packet(kind, payload=b"hello")
            header = self.iface._parse_rns_header(raw)
            ref = _unpacked(raw)
            self.assertIsNotNone(header, kind)
            self.assertEqual(header.packet_type, ptype, kind)
            self.assertEqual(header.packet_type, ref.packet_type, kind)
            self.assertEqual(header.destination_type, dtype, kind)
            self.assertEqual(header.context, context, kind)
            self.assertEqual(header.context, ref.context, kind)
            self.assertEqual(header.header_type, RNS.Packet.HEADER_1, kind)
            self.assertEqual(header.destination_hash, ref.destination_hash, kind)
            self.assertEqual(header.destination_hash, TEST_DEST_HASH, kind)

    def test_header_2_in_transport_packet(self):
        # RNS.Packet.pack() only builds HEADER_2 for announces (other packets in
        # transport are forwarded raw with the header rewritten), so that's the
        # one shape a packer can produce for this check.
        raw = build_rns_packet("announce", payload=b"x", header_type=RNS.Packet.HEADER_2)
        header = self.iface._parse_rns_header(raw)
        ref = _unpacked(raw)
        self.assertEqual(header.header_type, RNS.Packet.HEADER_2)
        self.assertEqual(header.packet_type, RNS.Packet.ANNOUNCE)
        self.assertEqual(header.destination_hash, ref.destination_hash)
        self.assertEqual(header.destination_hash, TEST_DEST_HASH)
        self.assertEqual(header.context, ref.context)

    def test_truncated_hash_matches_rns(self):
        for kind in ("data", "link_request", "proof"):
            raw = build_rns_packet(kind, payload=b"payload-bytes")
            header = self.iface._parse_rns_header(raw)
            ours = self.iface._compute_truncated_hash(raw, header.header_type)
            ref = _unpacked(raw)
            self.assertEqual(ours, RNS.Identity.truncated_hash(ref.get_hashable_part()), kind)

    def test_garbage_and_short_input(self):
        self.assertIsNone(self.iface._parse_rns_header(b""))
        self.assertIsNone(self.iface._parse_rns_header(b"\x00"))
        header = self.iface._parse_rns_header(b"\x00\x00")
        self.assertIsNotNone(header)
        self.assertIsNone(header.destination_hash)

    def test_priority_tiers(self):
        iface = self.iface
        tier = lambda kind: iface._priority_tier(iface._parse_rns_header(build_rns_packet(kind)))
        self.assertEqual(tier("link_request"), iface.PRIORITY_HANDSHAKE)
        self.assertEqual(tier("lrproof"), iface.PRIORITY_HANDSHAKE)
        self.assertEqual(tier("path_response"), iface.PRIORITY_LOW)
        self.assertEqual(tier("data"), iface.PRIORITY_NORMAL)
        self.assertEqual(iface._priority_tier(None), iface.PRIORITY_NORMAL)


if __name__ == "__main__":
    unittest.main()
