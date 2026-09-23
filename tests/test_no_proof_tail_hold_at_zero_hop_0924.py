"""Alpha 0.1.9, second pass, item 3: no proof tail hold at zero hop.

The first pass's item 2 holds the proof that replaces a raw window's
complete report for one of the sender's fragment spacings, because through
repeaters the parity trails the data by a relay-scaled gap and the proof
would be in the relay chain when it is transmitted. At zero hop the parity
follows within `direct_raw_zero_hop_gap` (0.15 s) and there is no chain, so
the hold buys nothing and costs latency; the first-pass brief asked for
zero there. Session 2 of 2026-09-23 (the first-pass build, at home) shows
`proof_tail_hold_s` of 0.96 to 2.27 s on zero-hop proofs: the
unflagged-completion branch still held one zero-hop spacing, and a stale
one-hop peer report (`_receiver_hops_to` takes the larger of this node's
own hop count and the peer's last report) made the hold a one-hop one.

Zero hop is THIS node's own resolved path to the sender -- the path the
proof goes over -- whatever the peer last reported.
"""
import unittest

from tests.test_proof_tail_and_skip_stamp_0923 import TheProofWaitsForTheBurstTail, PEER


class NoHoldAtZeroHop(TheProofWaitsForTheBurstTail):
    def setUp(self):
        super().setUp()
        # No device contact in this scaffold: resolve the sender token to
        # the peer key directly, as a contact lookup would.
        self._saved_canon = self.iface._canonical_peer_prefix
        self.iface._canonical_peer_prefix = lambda token: PEER if token else None

    def tearDown(self):
        self.iface._canonical_peer_prefix = self._saved_canon
        super().tearDown()

    def _own_path(self, hops):
        iface = self.iface
        module = self.module
        key = iface._canonical_peer_prefix(PEER)
        saved = iface._resolved_paths.get(key)
        iface._resolved_paths[key] = module._ResolvedPath(
            out_path_hex="19" * hops, out_path_len=hops, out_path_hash_len=1, resolved_at=0.0)
        return key, saved

    def _restore(self, key, saved):
        if saved is None:
            self.iface._resolved_paths.pop(key, None)
        else:
            self.iface._resolved_paths[key] = saved

    def _real_hold(self, *, frag_total=2, report_requested=False):
        # `_receiver_hops_to` unpatched: own path and peer report both count.
        return self.iface._proof_tail_hold_s(PEER, self._header(frag_total), None, report_requested, self.FRAG)

    def test_unflagged_completion_at_zero_hop_does_not_wait(self):
        self.assertEqual(self._hold(0, report_requested=False), 0.0)

    def test_own_zero_hop_path_wins_over_a_stale_peer_report(self):
        iface = self.iface
        key, saved = self._own_path(0)
        board = iface._path_board(key)
        saved_len = board.peer_path_len
        board.peer_path_len = 1          # the peer last said one hop
        try:
            self.assertEqual(iface._receiver_hops_to(PEER), 1, "the report still sets the receiver's hop count")
            self.assertEqual(self._real_hold(), 0.0)
            self.assertEqual(self._real_hold(frag_total=4, report_requested=True), 0.0)
        finally:
            board.peer_path_len = saved_len
            self._restore(key, saved)

    def test_through_repeaters_the_hold_is_unchanged(self):
        iface = self.iface
        key, saved = self._own_path(1)
        try:
            expected = iface._report_hold_s(self.FRAG, 1, arriving=False)
            self.assertGreater(expected, 0.0)
            self.assertAlmostEqual(self._real_hold(), expected, places=6)
        finally:
            self._restore(key, saved)


if __name__ == "__main__":
    unittest.main()
