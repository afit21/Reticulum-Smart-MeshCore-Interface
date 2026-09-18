"""
Raw binary DIRECT fragments (2026-09-18 night, see the interface module
docstring's "Raw binary DIRECT fragments" entry): a packet too large for
one text frame goes to a raw-capable peer as unacknowledged
CMD_SEND_RAW_DATA bursts reconciled by the "Q" bitmap.

Unit tests cover the codec, the firmware-derived budget and the
eligibility gate; the scenarios run two real interfaces (both with the
flag on, so their bind frames advertise the capability) over the
simulated mesh, including loss on the raw packet type and the text
fallback when raw frames never arrive.
"""
import os
import time
import unittest

from tests._support import SingleNodeCase, slow, wait_until, build_rns_packet, SimMesh, quiet_rns
import tempfile

RAW_CFG = {"direct_raw_fragments_enabled": "yes"}


class RawCodecAndGate(SingleNodeCase):

    def test_round_trip(self):
        iface = self.iface
        payload = os.urandom(150)
        frame = iface._encode_raw_fragment(payload, "ab" * 32, "cd" * 6, pkt_id=0x1234, frag_idx=2, frag_total=4, attempt=3)
        self.assertEqual(len(frame), iface.RAW_HEADER_SIZE + 150)
        header, out, src, dst = iface._decode_raw_fragment(frame)
        self.assertEqual((out, src, dst), (payload, "cd" * 6, bytes.fromhex("abab")))
        self.assertEqual((header.pkt_id, header.frag_idx, header.frag_total, header.attempt), (0x1234, 2, 4, 3))
        self.assertTrue(header.multi_fragment)

    def test_foreign_and_malformed_raw_packets_are_rejected(self):
        iface = self.iface
        with self.assertRaises(ValueError):
            iface._decode_raw_fragment(b"\x20" + bytes(20))          # wrong version nibble
        with self.assertRaises(ValueError):
            iface._decode_raw_fragment(bytes([0x10]) + bytes(5))     # too short
        bad = iface._encode_raw_fragment(b"x" * 8, "ab" * 32, "cd" * 6, 1, frag_idx=4, frag_total=4, attempt=0)
        with self.assertRaises(ValueError):
            iface._decode_raw_fragment(bad)                          # frag_idx out of range

    def test_budget_follows_firmware_limits(self):
        iface = self.iface
        self.assertEqual(iface._direct_raw_payload_budget(0), 170 - 13)      # config cap (170) wins at zero hop
        self.assertEqual(iface._direct_raw_payload_budget(3), 170 - 13)      # still the cap: 174 - 3 = 171 > 170
        self.assertEqual(iface._direct_raw_payload_budget(5), 169 - 13)      # 174 - path_len wins from 5 hops
        self.assertEqual(iface._direct_raw_payload_budget(10), 164 - 13)
        # a 483-byte Resource part is 4 raw fragments (5 text ones today)
        self.assertEqual(len(iface._chunk_payload(bytes(483), iface._direct_raw_payload_budget(0))), 4)
        self.assertEqual(len(iface._fragment_direct_payload(bytes(483))), 5)

    def test_eligibility_gate(self):
        iface, M = self.iface, self.module
        peer = "abcdef012345"
        rp = M._ResolvedPath(out_path_hex="", out_path_len=0, out_path_hash_len=1, resolved_at=time.monotonic())
        try:
            iface.direct_raw_fragments_enabled = True
            self.assertFalse(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_NORMAL), "unknown peer")
            self.on_loop(iface._register_peer, peer, None, "test", None, True)
            self.assertFalse(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_NORMAL), "no resolved path")
            iface._resolved_paths[peer] = rp
            self.assertTrue(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_NORMAL))
            self.assertFalse(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_HANDSHAKE), "handshakes stay on the ACKed text path")
            iface._raw_disabled_until[peer] = time.monotonic() + 60
            self.assertFalse(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_NORMAL), "fallback cooldown")
            iface._raw_disabled_until.clear()
            iface._peers[peer].raw_fragments = False
            self.assertFalse(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_NORMAL), "peer did not advertise raw")
            iface.direct_raw_fragments_enabled = False
            iface._peers[peer].raw_fragments = True
            self.assertFalse(iface._raw_fragments_eligible(peer, M.SmartMeshCoreInterface.PRIORITY_NORMAL), "flag off")
        finally:
            iface.direct_raw_fragments_enabled = False
            iface._peers.pop(peer, None); iface._resolved_paths.pop(peer, None); iface._raw_disabled_until.clear()

    def test_bind_capability_bit_follows_the_flag(self):
        iface = self.iface
        try:
            iface.direct_raw_fragments_enabled = True
            self.assertTrue(iface._bind_capability() & iface.BIND_CAP_RAW_FRAGMENTS)
            iface.direct_raw_fragments_enabled = False
            self.assertFalse(iface._bind_capability() & iface.BIND_CAP_RAW_FRAGMENTS)
        finally:
            iface.direct_raw_fragments_enabled = False


def _raw_mesh(test, links, repeaters=(), seed=1):
    quiet_rns()
    mesh = SimMesh(links, repeaters=repeaters, seed=seed, capture_dir=tempfile.mkdtemp(prefix="smci-raw-cap-"))
    test.mesh = mesh   # assigned before any assertion so tearDown can always stop it
    for n in ("A", "B"):
        mesh.add_node(n, config=dict(RAW_CFG))
    mesh.advert_all()
    assert mesh.wait_contacts(40.0), "contacts never populated"
    assert mesh.wait_bound(40.0), "bind-frame discovery never completed"
    assert mesh.wait_resolved(60.0), "DIRECT paths never resolved"
    a, b = mesh.nodes["A"], mesh.nodes["B"]
    b.send(build_rns_packet("data", dest_hash=b.dest_hash, payload=b"prime"))
    assert wait_until(lambda: b.dest_hash in a.iface._rns_token_peer, 30.0), "token never learned"
    assert a.iface._peers[b.prefix].raw_fragments is True, "bind frame did not carry the raw capability"
    return mesh, a, b


def _events(node, name):
    return [r for r in node.capture_records() if r.get("event") == name]


@slow
class RawFragmentScenarios(unittest.TestCase):

    def tearDown(self):
        self.mesh.stop()

    def test_zero_hop_raw_transfer_then_lossy_then_fallback(self):
        _, a, b = _raw_mesh(self, ["A-B"], seed=51)
        big = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"raw-1-" + os.urandom(440))
        a.send(big)
        self.assertTrue(wait_until(lambda: big in b.owner.received, 40.0), "raw transfer never delivered")
        sent = _events(a, "raw_fragment_sent")
        self.assertGreaterEqual(len(sent), 4)
        self.assertEqual(sum(1 for r in _events(a, "direct_attempt_result") if r.get("frag_total")), 0, "no text fragments should have been sent")
        self.assertTrue(any(r.get("transport") == "direct_raw_multifragment" for r in b.capture_records() if r["direction"] == "in"))

        # Half the raw packets vanish on air: the reconcile rounds must recover the gaps.
        self.mesh.air.type_loss["RAW_CUSTOM"] = 0.5
        big2 = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"raw-2-" + os.urandom(440))
        a.send(big2)
        self.assertTrue(wait_until(lambda: big2 in b.owner.received, 90.0), "lossy raw transfer never completed")
        self.assertEqual(b.owner.received.count(big2), 1)

        # Raw packets never arrive at all: two answered reconciles, then text fallback.
        self.mesh.air.type_loss["RAW_CUSTOM"] = 1.0
        big3 = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"raw-3-" + os.urandom(440))
        a.send(big3)
        self.assertTrue(wait_until(lambda: big3 in b.owner.received, 120.0), "text fallback never delivered")
        self.assertIn(b.prefix, a.iface._raw_disabled_until, "raw should be disabled for the peer after the fallback")
        self.assertTrue(any(r.get("transport") == "direct_multifragment" for r in b.capture_records() if r["direction"] == "in"))

    def test_one_hop_raw_transfer_through_a_repeater(self):
        _, a, b = _raw_mesh(self, ["A-R", "R-B"], repeaters=["R"], seed=21)
        self.assertEqual(a.resolved_paths[b.prefix].out_path_len, 1)
        big = build_rns_packet("data", dest_hash=b.dest_hash, payload=b"raw-hop-" + os.urandom(440))
        a.send(big)
        self.assertTrue(wait_until(lambda: big in b.owner.received, 60.0), "raw transfer through a repeater never delivered")
        self.assertGreaterEqual(self.mesh.repeaters["R"].counters["direct_forwarded"], 4)
        self.assertGreaterEqual(len(_events(a, "raw_fragment_sent")), 4)


if __name__ == "__main__":
    unittest.main()
