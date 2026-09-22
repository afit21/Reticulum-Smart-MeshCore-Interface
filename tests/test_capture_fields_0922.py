"""
Alpha 0.1.7, item 4 (2026-09-22): the peer's reported path view is a
capture field. `peer_path_len` (the path length the sender put in its "Q"
v5 header) scales the receiver's holds (`_receiver_hops_to`) but the 0.1.6
captures did not carry it, so a held report could not be read against the
number that held it. Now `completion_report_sent` and
`completion_query_received` carry `peer_path_len`, `peer_rate` and this
node's own `hop_count`, and `path_selected` carries them when a candidate
came from the peer's report.
"""
import asyncio
import time
import unittest

from tests._support import SingleNodeCase
from tests.test_completion_report_one_hop_0920 import PEER, _sink


class PeerViewOnCaptureRecords(SingleNodeCase):

    def _header(self, pkt_id, frag_idx, frag_total, attempt=0):
        return self.module._FrameHeader(self.iface.PROTOCOL_VERSION, True, False, pkt_id, frag_idx, frag_total, attempt)

    def setUp(self):
        iface = self.iface
        iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=False, last_seen=time.time())
        iface._resolved_paths[PEER] = self.module._ResolvedPath("aa", 1, 1, time.monotonic())
        iface._path_boards.pop(PEER, None)
        self._saved_canonical = iface._canonical_peer_prefix
        iface._canonical_peer_prefix = lambda token: PEER

    def tearDown(self):
        self.iface._canonical_peer_prefix = self._saved_canonical
        self.iface._path_boards.pop(PEER, None)

    def test_report_and_query_records_carry_the_peer_view(self):
        iface = self.iface
        answers = []

        async def fake_answer(sender_token, pkt_id, frag_total, complete, **kwargs):
            answers.append(pkt_id)

        sink, restore_sink = _sink(iface)
        original_answer = iface._send_completion_answer
        iface._send_completion_answer = fake_answer
        try:
            # the peer's v5 QUERY says: two hops to us, 60 % delivered
            query = iface._encode_completion_frame_v5(
                iface.COMPLETION_TYPE_QUERY, [(77, 1, False, set())], nonce=5, path_len=2, rate=0.6)

            async def scenario():
                iface._handle_incoming_completion_frame(query, PEER)
                for _ in range(3):
                    await asyncio.sleep(0)
                # a one-fragment raw packet lands complete: an immediate report
                iface._handle_direct_multifragment_frame(
                    self._header(78, 0, 1), b"y" * 10, PEER, raw=True, report_requested=True)
                for _ in range(3):
                    await asyncio.sleep(0)

            self.node.run_on_loop(scenario(), timeout=15.0)
        finally:
            iface._send_completion_answer = original_answer
            restore_sink()
        q = sink.records("completion_query_received")
        self.assertEqual(len(q), 1)
        self.assertEqual((q[0]["peer_path_len"], q[0]["peer_rate"], q[0]["hop_count"]), (2, 0.6, 1))
        r = sink.records("completion_report_sent")
        self.assertEqual(len(r), 1, r)
        self.assertEqual((r[0]["peer_path_len"], r[0]["peer_rate"], r[0]["hop_count"]), (2, 0.6, 1))
        self.assertEqual(iface._receiver_hops_to(PEER), 2, "the hold scales by the larger of own hops and the reported path")

    def test_path_selected_carries_the_peer_view_for_a_reported_candidate(self):
        iface = self.iface
        sink, restore_sink = _sink(iface)
        try:
            now = time.monotonic()
            # the peer reports a zero-hop path to us: the zero-hop candidate
            iface._note_peer_reported_path(PEER, 0, 0.8, now=now)
            iface._add_path_candidate(PEER, "aa", 1, 1, "discovered", now=now)
            board = iface._path_boards[PEER]
            ranked = iface._rank_paths([iface._path_view(c) for c in board.candidates.values()], now,
                                       iface.path_weak_snr_db, iface.PATH_SAMPLE_WINDOW_S, iface.PATH_SAMPLE_HALF_LIFE_S)
            iface._capture_path_selected(PEER, "selected", None, None, ranked)
        finally:
            restore_sink()
        ps = sink.records("path_selected")
        self.assertEqual(len(ps), 1)
        self.assertIn("peer_report", [s["source"] for s in ps[0]["scores"]])
        self.assertEqual((ps[0]["peer_path_len"], ps[0]["peer_rate"]), (0, 0.8))


if __name__ == "__main__":
    unittest.main()
