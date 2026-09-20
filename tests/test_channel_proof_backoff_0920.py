"""
Regression test for the 2026-09-20 MeshBench `two_hop` baseline finding
(module docstring entry "A CHANNEL-carried PROOF clears the unknown-destination
backoff"): probes 2 and 3 were delivered over CHANNEL and their PROOFs came
back over CHANNEL, the interface still counted three bootstrap attempts "with
no token learned", and dropped probes 4-7 outright for 300 s
(`unknown_dest_backoff_drop`) while the destination was provably answering.
Pinned: a PROOF received over CHANNEL whose destination field matches a
remembered bootstrap send (or a pending LINKREQUEST's link_id) clears that
destination's backoff -- and teaches NO routing token, since a CHANNEL
sender is unauthenticated.
"""
import os
import unittest

from tests._support import SingleNodeCase, build_rns_packet


class ChannelProofClearsUnknownDestBackoff(SingleNodeCase):
    def _proof_for(self, data):
        header = self.iface._parse_rns_header(data)
        truncated = self.iface._compute_truncated_hash(data, header.header_type)
        return build_rns_packet("proof", dest_hash=truncated)

    def test_channel_proof_for_a_bootstrap_send_clears_the_backoff_without_learning(self):
        iface = self.iface
        dest = os.urandom(16)
        data = build_rns_packet("data", dest_hash=dest, payload=b"bootstrap")
        header = iface._parse_rns_header(data)
        # three bootstrap attempts, as the DIRECT-to-all path records them
        for _ in range(3):
            iface._record_unknown_dest_attempt(dest)
            iface._remember_bootstrap_send(data, header)
        self.assertTrue(iface._unknown_dest_in_backoff(dest), "three attempts must arm the backoff")
        # the PROOF arrives over CHANNEL (channel_bare), not DIRECT
        iface._note_channel_proof(iface._parse_rns_header(self._proof_for(data)), "channel_bare")
        self.assertFalse(iface._unknown_dest_in_backoff(dest), "a matched CHANNEL proof must clear the backoff")
        self.assertNotIn(dest, iface._rns_token_peer, "a CHANNEL proof must not teach a routing token")

    def test_unrelated_channel_proof_changes_nothing(self):
        iface = self.iface
        dest = os.urandom(16)
        data = build_rns_packet("data", dest_hash=dest, payload=b"bootstrap")
        for _ in range(3):
            iface._record_unknown_dest_attempt(dest)
            iface._remember_bootstrap_send(data, iface._parse_rns_header(data))
        self.assertTrue(iface._unknown_dest_in_backoff(dest))
        other = build_rns_packet("data", dest_hash=os.urandom(16), payload=b"other")
        iface._note_channel_proof(iface._parse_rns_header(self._proof_for(other)), "channel_bare")
        self.assertTrue(iface._unknown_dest_in_backoff(dest), "an unmatched proof must not clear anything")
        iface._clear_unknown_dest_backoff(dest)

    def test_process_incoming_over_channel_runs_the_hook(self):
        iface = self.iface
        dest = os.urandom(16)
        data = build_rns_packet("data", dest_hash=dest, payload=b"bootstrap")
        for _ in range(3):
            iface._record_unknown_dest_attempt(dest)
            iface._remember_bootstrap_send(data, iface._parse_rns_header(data))
        self.assertTrue(iface._unknown_dest_in_backoff(dest))
        proof = self._proof_for(data)
        self.on_loop(lambda: iface.process_incoming(proof, transport="channel_bare"))
        self.assertFalse(iface._unknown_dest_in_backoff(dest))


if __name__ == "__main__":
    unittest.main()
