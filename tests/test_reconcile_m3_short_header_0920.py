"""
Phase 3, M3 (2026-09-20, docs/reconcile_redesign.md): the raw header shrinks
from 13 to 9 bytes (version 2: a 2-byte source prefix instead of 6), so the
per-fragment payload at the shipped cap is 161 and a 483-byte Link MDU part
is three fragments (3 x 161 = 483) up to four hops. Firmware limits re-read
for this change: 172 bytes received (`onRawDataRecv`, MAX_FRAME_SIZE 176
less the 4 push bytes) and 174 - path_len sent (CMD_SEND_RAW_DATA = cmd +
path_len + path + payload).

Pinned:
  * the header layout and budget (the golden wire snapshot has the bytes);
  * `_resolve_raw_src`: the unique bound peer whose 6-byte prefix starts
    with the 2-byte source prefix; none -> None; two -> None and one WARNING;
  * a received raw fragment lands in the bucket keyed by the peer's FULL
    prefix (the same bucket a text fragment from that peer would use);
  * `_raw_fragments_eligible` refuses raw to a peer whose short prefix
    another bound peer shares, or when this node's own short prefix is
    shared by a bound peer;
  * a version-1 (13-byte) header is not decoded.
"""
import time
import unittest

from tests._support import SingleNodeCase

PEER = "abcdef012345"
PEER_SAME_SHORT = "abcd99999999"
PEER_OTHER = "112233445566"


class ShortSourcePrefix(SingleNodeCase):
    def _bind(self, *prefixes):
        for p in prefixes:
            self.iface._peers[p] = self.module._PeerRecord(pubkey_prefix=p, has_upstream_rns=False, last_seen=time.time())

    def _unbind(self, *prefixes):
        for p in prefixes:
            self.iface._peers.pop(p, None)

    def test_header_is_nine_bytes_and_483_is_three_fragments(self):
        iface = self.iface
        self.assertEqual(iface.RAW_HEADER_SIZE, 9)
        self.assertEqual(iface.RAW_SRC_PREFIX_BYTES, 2)
        self.assertEqual(iface.RAW_PROTOCOL_VERSION, 2)
        frame = iface._encode_raw_fragment(b"x" * 161, "ab" * 32, PEER, 7, 0, 3, attempt=1, report=True)
        self.assertEqual(len(frame), 170)
        self.assertEqual(frame[0], (2 << 4) | 0x04 | 1)
        self.assertEqual(frame[1:3], bytes.fromhex("abab"))
        self.assertEqual(frame[3:5], bytes.fromhex(PEER[:4]))
        self.assertEqual(frame[5:7], (7).to_bytes(2, "big"))
        self.assertEqual(frame[7:9], bytes([0, 3]))
        for path_len in range(5):
            self.assertEqual(iface._direct_raw_payload_budget(path_len), 161, f"path_len {path_len}")
            self.assertLessEqual(2 + path_len + iface.RAW_HEADER_SIZE + 161, 176, "cmd + path_len + path + payload fits MAX_FRAME_SIZE 176")
        self.assertLessEqual(iface.RAW_HEADER_SIZE + 161, 172, "fits the receive push limit")
        self.assertEqual(len(iface._chunk_payload(bytes(483), 161)), 3)
        v1 = bytes([0x10]) + bytes.fromhex("abab") + bytes.fromhex(PEER) + bytes([0, 7, 0, 3]) + b"x" * 20
        with self.assertRaises(ValueError):
            iface._decode_raw_fragment(v1)

    def test_resolve_raw_src(self):
        iface = self.iface
        self._bind(PEER, PEER_OTHER)
        try:
            self.assertEqual(iface._resolve_raw_src(PEER[:4]), PEER)
            self.assertEqual(iface._resolve_raw_src(PEER_OTHER[:4]), PEER_OTHER)
            self.assertIsNone(iface._resolve_raw_src("ffff"))
            self.assertIsNone(iface._resolve_raw_src(""))
            self._bind(PEER_SAME_SHORT)
            self.assertIsNone(iface._resolve_raw_src(PEER[:4]), "two bound peers share the short prefix")
            self.assertIn(PEER[:4], iface._raw_src_ambiguous_logged)
        finally:
            self._unbind(PEER, PEER_OTHER, PEER_SAME_SHORT)
            iface._raw_src_ambiguous_logged.clear()

    def test_received_fragment_lands_in_the_full_prefix_bucket(self):
        iface = self.iface
        self._bind(PEER)
        own = iface._own_pubkey_hex
        handled = []
        original = iface._handle_direct_multifragment_frame
        iface._handle_direct_multifragment_frame = lambda header, payload, sender_token, raw=False, report_requested=False: handled.append(
            (sender_token, header.pkt_id, header.frag_idx, raw, report_requested))
        try:
            frame = iface._encode_raw_fragment(b"y" * 20, own, PEER, 9, 1, 3, attempt=0, report=True)
            event = type("E", (), {"payload": {"payload": frame.hex()}})()
            self.on_loop(iface._on_raw_data_inner, event)
            self.assertEqual(handled, [(PEER, 9, 1, True, True)], "the 2-byte prefix resolved to the bound peer's full prefix")
            ignored_before = iface._raw_frames_ignored
            frame2 = iface._encode_raw_fragment(b"y" * 20, own, "ffff00000000", 9, 1, 3, attempt=0)
            self.on_loop(iface._on_raw_data_inner, type("E", (), {"payload": {"payload": frame2.hex()}})())
            self.assertEqual(len(handled), 1, "an unresolvable source is dropped")
            self.assertEqual(iface._raw_frames_ignored, ignored_before + 1)
        finally:
            iface._handle_direct_multifragment_frame = original
            self._unbind(PEER)

    def test_eligibility_refuses_ambiguous_short_prefixes(self):
        iface, M = self.iface, self.module
        saved = iface.direct_raw_fragments_enabled
        iface.direct_raw_fragments_enabled = True
        self._bind(PEER)
        iface._peers[PEER].raw_fragments = True
        iface._resolved_paths[PEER] = M._ResolvedPath(out_path_hex="", out_path_len=0, out_path_hash_len=1, resolved_at=time.monotonic())
        try:
            self.assertFalse(iface._raw_src_ambiguous(PEER))
            eligible = iface._raw_fragments_eligible(PEER, iface.PRIORITY_NORMAL)
            self._bind(PEER_SAME_SHORT)
            self.assertTrue(iface._raw_src_ambiguous(PEER))
            self.assertFalse(iface._raw_fragments_eligible(PEER, iface.PRIORITY_NORMAL))
            self._unbind(PEER_SAME_SHORT)
            self.assertEqual(iface._raw_fragments_eligible(PEER, iface.PRIORITY_NORMAL), eligible)
            # a bound peer sharing THIS node's own short prefix makes our
            # fragments ambiguous at the far end
            own_short = iface._own_pubkey_prefix()[:4]
            clash = own_short + "00000000"
            self._bind(clash)
            self.assertTrue(iface._raw_src_ambiguous(PEER))
            self._unbind(clash)
        finally:
            iface.direct_raw_fragments_enabled = saved
            iface._resolved_paths.pop(PEER, None)
            self._unbind(PEER, PEER_SAME_SHORT)


if __name__ == "__main__":
    unittest.main()
