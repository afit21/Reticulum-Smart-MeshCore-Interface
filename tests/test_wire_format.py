"""
Wire-format round trips: the "R" RNS frames (CHANNEL fast-path, shared
multi-fragment shape, DIRECT bare), "P" bind frames, "Q" completion
frames, and the payload budgets that keep every encoded frame inside
the firmware's 160-char text limit. These are the invariants a
receiver on another build depends on -- a change here is a protocol
change.
"""
import os
import unittest

from tests._support import SingleNodeCase, node_prefix


class WireFormatTests(SingleNodeCase):

    def test_channel_fastpath_roundtrip(self):
        iface = self.iface
        payload = os.urandom(40)
        frame = iface._encode_channel_fastpath(payload, pkt_id=0x1234, attempt=3)
        self.assertTrue(frame.startswith(iface.MARKER))
        header, decoded = iface._decode_frame(frame, mode="channel")
        self.assertEqual(decoded, payload)
        self.assertFalse(header.multi_fragment)
        self.assertFalse(header.coop)
        self.assertEqual(header.pkt_id, 0x1234)
        self.assertEqual(header.attempt, 3)
        self.assertEqual((header.frag_idx, header.frag_total), (0, 1))

    def test_multifragment_roundtrip_identical_for_channel_and_direct(self):
        iface = self.iface
        payload = os.urandom(50)
        frame = iface._encode_channel_multifragment(payload, pkt_id=0xBEEF, frag_idx=2, frag_total=5, attempt=1)
        for mode in ("channel", "direct"):
            header, decoded = iface._decode_frame(frame, mode=mode)
            self.assertEqual(decoded, payload, mode)
            self.assertTrue(header.multi_fragment)
            self.assertEqual((header.pkt_id, header.frag_idx, header.frag_total, header.attempt), (0xBEEF, 2, 5, 1))

    def test_direct_bare_roundtrip(self):
        iface = self.iface
        payload = os.urandom(70)
        frame = iface._encode_direct_bare(payload)
        header, decoded = iface._decode_frame(frame, mode="direct")
        self.assertEqual(decoded, payload)
        self.assertFalse(header.multi_fragment)
        self.assertIsNone(header.pkt_id)
        self.assertIsNone(header.attempt)

    def test_direct_bare_frame_never_carries_attempt_byte(self):
        # The firmware's own attempt flag is the dedup-buster for bare DIRECT
        # retries; the frame content itself must be byte-identical per attempt.
        payload = b"same"
        self.assertEqual(self.iface._encode_direct_bare(payload), self.iface._encode_direct_bare(payload))

    def test_multifragment_frames_differ_per_attempt(self):
        # The flood-dedup ring keys on content: an unchanged retransmit is
        # silently absorbed, so every attempt must produce different bytes.
        iface = self.iface
        a = iface._encode_channel_multifragment(b"x", 1, 0, 2, attempt=0)
        b = iface._encode_channel_multifragment(b"x", 1, 0, 2, attempt=1)
        self.assertNotEqual(a, b)

    def test_decode_rejects_malformed(self):
        iface = self.iface
        with self.assertRaises(ValueError):
            iface._decode_frame("X" + "abcd", mode="channel")            # wrong marker
        with self.assertRaises(ValueError):
            iface._decode_frame(iface.MARKER, mode="channel")           # empty
        bad_version = iface.MARKER + self.module._z85_encode(bytes([0x3F]) + b"\x00\x01\x00")
        with self.assertRaises(ValueError):
            iface._decode_frame(bad_version, mode="channel")
        # frag_idx >= frag_total must be rejected at decode time.
        raw = bytes([iface.PROTOCOL_VERSION | iface.FLAG_MULTI_FRAGMENT]) + (1).to_bytes(2, "big") + bytes([3, 3, 0]) + b"p"
        with self.assertRaises(ValueError):
            iface._decode_frame(iface.MARKER + self.module._z85_encode(raw), mode="direct")
        # Too short for the header its flag bits claim.
        short = bytes([iface.PROTOCOL_VERSION | iface.FLAG_MULTI_FRAGMENT]) + b"\x00\x01"
        with self.assertRaises(ValueError):
            iface._decode_frame(iface.MARKER + self.module._z85_encode(short), mode="channel")

    def test_bind_frame_roundtrip_carries_own_prefix(self):
        iface = self.iface
        frame = iface._encode_bind_frame(iface.BIND_TYPE_REQUEST, attempt=7)
        self.assertTrue(frame.startswith(iface.PEER_MARKER))
        decoded = iface._decode_bind_frame(frame)
        self.assertEqual(decoded.type, iface.BIND_TYPE_REQUEST)
        self.assertEqual(decoded.attempt, 7)
        self.assertEqual(decoded.pubkey_prefix, node_prefix("A"))
        self.assertEqual(decoded.version, iface.BIND_PROTOCOL_VERSION)
        with self.assertRaises(ValueError):
            iface._decode_bind_frame(iface.MARKER + frame[1:])
        with self.assertRaises(ValueError):
            iface._decode_bind_frame(iface.PEER_MARKER + self.module._z85_encode(b"\x01\x00\x00"))

    def test_bind_frame_capability_bit(self):
        iface = self.iface
        original = iface.declares_upstream_rns
        try:
            iface.declares_upstream_rns = True
            frame = iface._encode_bind_frame(iface.BIND_TYPE_RESPONSE, attempt=0)
            self.assertTrue(iface._decode_bind_frame(frame).cap & iface.BIND_CAP_HAS_UPSTREAM_RNS)
            iface.declares_upstream_rns = False
            frame = iface._encode_bind_frame(iface.BIND_TYPE_RESPONSE, attempt=0)
            self.assertFalse(iface._decode_bind_frame(frame).cap & iface.BIND_CAP_HAS_UPSTREAM_RNS)
        finally:
            iface.declares_upstream_rns = original

    def test_completion_frames_roundtrip(self):
        iface = self.iface
        query = iface._encode_completion_frame(iface.COMPLETION_TYPE_QUERY, pkt_id=0x0102, frag_total=9)
        q = iface._decode_completion_frame(query)
        self.assertEqual((q.type, q.pkt_id, q.frag_total, q.version), (iface.COMPLETION_TYPE_QUERY, 0x0102, 9, iface.COMPLETION_PROTOCOL_VERSION))
        self.assertIsNone(q.held)

        answer = iface._encode_completion_frame(
            iface.COMPLETION_TYPE_ANSWER, pkt_id=0x0102, frag_total=9, complete=False, held={0, 3, 8},
        )
        a = iface._decode_completion_frame(answer)
        self.assertEqual(a.type, iface.COMPLETION_TYPE_ANSWER)
        self.assertFalse(a.complete)
        self.assertEqual(set(a.held), {0, 3, 8})

        v1 = iface._encode_completion_frame(
            iface.COMPLETION_TYPE_ANSWER, pkt_id=5, frag_total=3, complete=True, version=iface.COMPLETION_PROTOCOL_VERSION_V1,
        )
        d = iface._decode_completion_frame(v1)
        self.assertEqual(d.version, iface.COMPLETION_PROTOCOL_VERSION_V1)
        self.assertTrue(d.complete)
        self.assertIsNone(d.held)

        with self.assertRaises(ValueError):
            iface._decode_completion_frame(answer[:-2])  # wrong length for its bitmap

    def test_payload_budgets_keep_frames_within_firmware_limit(self):
        iface = self.iface
        limit = iface.FIRMWARE_TEXT_LIMIT
        name_prefix_cost = len(f"{iface._own_node_name}: ")

        fast = iface._channel_payload_budget()
        frame = iface._encode_channel_fastpath(b"\xff" * fast, 0xFFFF, 255)
        self.assertLessEqual(name_prefix_cost + len(frame), limit)
        # Z85 packs 4 bytes into 5 chars, so a budget can leave up to one
        # group of slack; two extra groups must always overflow.
        self.assertGreater(name_prefix_cost + len(iface._encode_channel_fastpath(b"\xff" * (fast + 8), 0xFFFF, 255)), limit)

        multi = iface._channel_multifragment_payload_budget()
        frame = iface._encode_channel_multifragment(b"\xff" * multi, 0xFFFF, 254, 255, 255)
        self.assertLessEqual(name_prefix_cost + len(frame), limit)

        bare = iface._direct_payload_budget()
        self.assertLessEqual(len(iface._encode_direct_bare(b"\xff" * bare)), limit)
        self.assertGreater(len(iface._encode_direct_bare(b"\xff" * (bare + 8))), limit)

        direct_multi = iface._direct_multifragment_payload_budget()
        self.assertLessEqual(len(iface._encode_channel_multifragment(b"\xff" * direct_multi, 0xFFFF, 254, 255, 255)), limit)
        self.assertGreaterEqual(bare, direct_multi)

    def test_fragmentation_chunks_rejoin(self):
        iface = self.iface
        data = os.urandom(5 * iface._direct_multifragment_payload_budget() + 7)
        chunks = iface._fragment_direct_payload(data)
        self.assertEqual(len(chunks), 6)
        self.assertEqual(b"".join(chunks), data)
        self.assertTrue(all(len(c) <= iface._direct_multifragment_payload_budget() for c in chunks))
        chunks = iface._fragment_payload(data)
        self.assertEqual(b"".join(chunks), data)
        self.assertTrue(all(len(c) <= iface._channel_multifragment_payload_budget() for c in chunks))


if __name__ == "__main__":
    unittest.main()
