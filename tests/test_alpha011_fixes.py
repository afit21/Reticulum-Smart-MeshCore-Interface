"""
Alpha 0.1.1 field fixes (2026-09-18 night captures, see the interface
module docstring's "Alpha 0.1.1 captures" entry):

  1. bare-DIRECT receive dedup lets RNS's own no-dedup contexts through
     (a re-requested Resource part must reach RNS again);
  2. a fragmented send the receiver provably holds part of finishes with
     the larger `direct_fragment_finish_attempts` budget;
  3. a failed fragmented send resumes under its old pkt_id when the same
     bytes are re-sent to the same peer while the receiver's bucket can
     still be alive, with a forced reconcile to validate the assumption.

2 and 3 are pure pass-structure logic, exercised by scripting the
per-fragment send and the reconcile answer; the receive-side effect of 1
runs through the real frame codec on a live interface.
"""
import os
import unittest

import RNS

from tests._support import SingleNodeCase, slow, wait_until, build_rns_packet
from tests.test_sim_scenarios import _setup_mesh, _bring_up, _prime


def _link_packet(context: int, data: bytes) -> bytes:
    """Raw RNS bytes of a DATA packet to a LINK destination with `context`
    -- the shape Resource parts travel in (HEADER_1: flags, hops, dest,
    context, data)."""
    flags = (RNS.Packet.HEADER_1 << 6) | (RNS.Transport.BROADCAST << 4) | (RNS.Destination.LINK << 2) | RNS.Packet.DATA
    return bytes([flags, 0]) + bytes(16) + bytes([context]) + data


class BareDirectDedupExemptions(SingleNodeCase):

    def _deliver(self, raw: bytes, sender: str = "cafebabe0000") -> int:
        before = len(self.node.owner.received)
        frame = self.iface._encode_direct_bare(raw)
        self.on_loop(self.iface._handle_incoming_frame, frame, "direct", sender)
        self.on_loop(self.iface._handle_incoming_frame, frame, "direct", sender)
        return len(self.node.owner.received) - before

    def test_resource_part_reaches_rns_every_time(self):
        for ctx in (RNS.Packet.RESOURCE, RNS.Packet.RESOURCE_REQ, RNS.Packet.RESOURCE_PRF,
                    RNS.Packet.KEEPALIVE, RNS.Packet.CACHE_REQUEST, RNS.Packet.CHANNEL):
            self.assertEqual(self._deliver(_link_packet(ctx, os.urandom(16))), 2, f"context {ctx:#x} was deduped")

    def test_ordinary_packet_is_still_deduped(self):
        self.assertEqual(self._deliver(_link_packet(RNS.Packet.NONE, os.urandom(16))), 1)
        self.assertEqual(self._deliver(build_rns_packet("data", payload=os.urandom(20))), 1)


class FragmentedFinishAndResume(SingleNodeCase):
    """Scripts `_send_direct_with_attempts` and `_query_remote_fragments`
    underneath a real `_send_direct_payload` call."""

    PEER = "abcdef012345"
    TARGET = "ab" * 32

    def setUp(self):
        self.calls = []
        self.queries = []
        self.script = {}      # (pass_number, frag_idx) -> bool
        self.answers = []     # popped per query
        iface = self.iface
        self._orig = (iface._send_direct_with_attempts, iface._query_remote_fragments)
        M = self.module

        async def fake_send(target, frame_builder, peer_prefix, pkt_id=None, frag_idx=None, frag_total=None,
                            priority=M.SmartMeshCoreInterface.PRIORITY_NORMAL, hop_count=None, time_critical=False,
                            pass_number=None, attempts_override=None, record_result=True, expires_at=None):
            self.calls.append((pass_number, frag_idx, attempts_override))
            return self.script.get((pass_number, frag_idx), False)

        async def fake_query(target, peer_prefix, pkt_id, frag_total, stage, priority=M.SmartMeshCoreInterface.PRIORITY_NORMAL, hop_count=None):
            self.queries.append((stage, pkt_id))
            held = self.answers.pop(0) if self.answers else None
            if held is None:
                return None
            return M._CompletionFrame(version=2, type=1, complete=(held == "all"), pkt_id=pkt_id,
                                      frag_total=frag_total, held=(None if held == "all" else frozenset(held)))

        iface._send_direct_with_attempts = fake_send
        iface._query_remote_fragments = fake_query
        iface._resumable_sends.clear()
        self.data = b"three-fragments-" + os.urandom(280)
        self.assertEqual(len(iface._fragment_direct_payload(self.data)), 3)

    def tearDown(self):
        self.iface._send_direct_with_attempts, self.iface._query_remote_fragments = self._orig
        self.iface._resumable_sends.clear()

    def _send(self):
        return self.node.run_on_loop(self.iface._send_direct_payload(self.TARGET, self.PEER, self.data))

    def test_partial_delivery_uses_finish_budget_then_remembers_for_resume(self):
        self.script = {(0, 0): True, (0, 1): False, (0, 2): True, (1, 1): False}
        self.answers = [{0, 2}, {0, 2}]   # reconcile, then the final check
        self.assertFalse(self._send())
        pass1 = [c for c in self.calls if c[0] == 1]
        self.assertEqual(pass1, [(1, 1, self.iface.direct_fragment_finish_attempts)], "pass 1 must use the finishing budget")
        self.assertEqual([q[0] for q in self.queries], ["reconcile", "final"])
        entries = list(self.iface._resumable_sends.values())
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["acked"], [True, False, True])
        self.assertEqual(entries[0]["frag_total"], 3)

    def test_resend_resumes_only_the_missing_fragment_and_forces_reconcile(self):
        self.script = {(0, 0): True, (0, 1): False, (0, 2): True, (1, 1): False}
        self.answers = [{0, 2}, {0, 2}]
        self.assertFalse(self._send())
        pkt_id = list(self.iface._resumable_sends.values())[0]["pkt_id"]
        self.calls.clear(); self.queries.clear()
        self.script = {(0, 1): True}
        self.answers = ["all"]           # receiver now holds everything
        self.assertTrue(self._send())
        self.assertEqual([c[:2] for c in self.calls], [(0, 1)], "only the missing fragment is sent on resume")
        self.assertEqual(self.queries, [("reconcile", pkt_id)], "a resumed send always reconciles, under the old pkt_id")
        self.assertEqual(self.iface._resumable_sends, {})

    def test_resume_is_validated_against_the_receivers_bucket(self):
        self.script = {(0, 0): True, (0, 1): False, (0, 2): True, (1, 1): False}
        self.answers = [{0, 2}, {0, 2}]
        self.assertFalse(self._send())
        self.calls.clear(); self.queries.clear()
        # The receiver's bucket has since been evicted: it holds nothing.
        self.script = {(0, 1): True, (1, 0): True, (1, 1): True, (1, 2): True}
        self.answers = [set()]
        self.assertTrue(self._send())
        self.assertEqual(sorted(c[1] for c in self.calls if c[0] == 1), [0, 1, 2], "believed-held fragments the receiver lost are re-driven")
        self.assertIsNone([c for c in self.calls if c[0] == 1][0][2], "nothing held -> ordinary budget, not the finishing one")

    def test_no_resume_after_the_receivers_bucket_would_have_expired(self):
        self.script = {(0, 0): True, (0, 1): False, (0, 2): True, (1, 1): False}
        self.answers = [{0, 2}, {0, 2}]
        self.assertFalse(self._send())
        for v in self.iface._resumable_sends.values():
            v["expires_at"] = 0.0
        self.calls.clear()
        self.script = {(0, 0): True, (0, 1): True, (0, 2): True}
        self.assertTrue(self._send())
        self.assertEqual(sorted(c[1] for c in self.calls if c[0] == 0), [0, 1, 2], "expired entry -> fresh full send")

    def test_handshake_priority_never_resumes(self):
        self.script = {(0, 0): True, (0, 1): False, (0, 2): True, (1, 1): False}
        self.answers = [{0, 2}, {0, 2}]
        self.assertFalse(self._send())
        self.calls.clear()
        P = self.module.SmartMeshCoreInterface.PRIORITY_HANDSHAKE
        self.script = {(0, 0): True, (0, 1): True, (0, 2): True}
        self.assertTrue(self.node.run_on_loop(self.iface._send_direct_payload(self.TARGET, self.PEER, self.data, priority=P)))
        self.assertEqual(sorted(c[1] for c in self.calls if c[0] == 0), [0, 1, 2])


@slow
class ResourcePartRedeliveryScenario(unittest.TestCase):
    """Alpha 0.1.1 fix 1 end to end: RNS re-sends a byte-identical
    Resource part; the receiving interface must hand it to RNS again."""

    def setUp(self):
        self.mesh = _setup_mesh(["A-B"], seed=41)
        _bring_up(self.mesh, ["A", "B"])
        self.a, self.b = self.mesh.nodes["A"], self.mesh.nodes["B"]

    def tearDown(self):
        self.mesh.stop()

    def test_identical_resource_part_delivered_twice(self):
        _prime(self.a, self.b)
        self.a.seed_token(bytes(16), self.b.prefix)   # LINK dest hash used by _link_packet
        part = _link_packet(RNS.Packet.RESOURCE, b"part-" + os.urandom(11))
        self.a.send(part)
        self.assertTrue(wait_until(lambda: self.b.owner.received.count(part) == 1, 30.0))
        self.assertTrue(wait_until(lambda: not self.a.iface._outgoing_inflight, 15.0))
        self.a.send(part)   # RNS re-request -> identical bytes again
        self.assertTrue(wait_until(lambda: self.b.owner.received.count(part) == 2, 30.0),
                        "second copy of a RESOURCE part never reached RNS on the receiver")
        plain = build_rns_packet("data", dest_hash=self.b.dest_hash, payload=b"once-" + os.urandom(11))
        self.a.send(plain)
        self.assertTrue(wait_until(lambda: self.b.owner.received.count(plain) == 1, 30.0))
        self.assertTrue(wait_until(lambda: not self.a.iface._outgoing_inflight, 15.0))
        self.a.send(plain)
        self.assertFalse(wait_until(lambda: self.b.owner.received.count(plain) == 2, 12.0),
                         "an ordinary packet must still be deduped on retry")


if __name__ == "__main__":
    unittest.main()
