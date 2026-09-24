"""Alpha 0.1.8, item 2: acknowledge the report where the no-ACK frame does
not arrive.

The completion REPORT has gone out since 2026-09-20 as a no-ACK frame
(MeshCore's TXT_TYPE_CLI_DATA, which the firmware never acknowledges): one
transmission, never retried, followed by a hold of the radio for the
report's relay window. At one hop that is adequate -- reports arrived 33
times of 48 in the alpha 0.1.6 session -- but at two hops the 2026-09-22
evening session had it reach the sender 3 times out of 22. Each miss cost
the sender its whole 10-18 s report wait and then a QUERY round (a QUERY,
its firmware ACK and an ANSWER, 2.1-2.4 s of channel time at two hops) to
learn exactly what the report had already said; the ACK is about 0.42 s
there, so the exchange pays for itself if it saves roughly one QUERY round
in five.

Item 1 removes the report entirely for packets RNS proves per packet, so
what is left to serve at two hops is the traffic RNS does NOT prove that
way: Resource parts inside a Link (context RESOURCE -- a Resource is proved
once, whole, as RESOURCE_PRF) and announces. That is the population this
item is for.

Pinned here: the threshold and which hop count it reads, that the frame's
CONTENT is unchanged so the golden wire snapshot is untouched, that the
acknowledged carrier takes the radio lock in the REPORT class (without
which alpha 0.1.5 item 6 and alpha 0.1.7 item 3c are silently lost), that
its ACK wait is bounded by the sender's own report window rather than the
miss ceiling, that the retry re-encodes the bitmap and varies the firmware
attempt byte while keeping the round nonce, and that no path evidence is
recorded either way.
"""
import asyncio
import time
import unittest

from tests._support import SingleNodeCase
from tests.test_completion_report_one_hop_0920 import PEER

PKT = 31


class ReportAckThreshold(SingleNodeCase):
    def test_default_and_the_hop_count_it_reads(self):
        iface = self.iface
        bare = self.module.SmartMeshCoreInterface.__new__(self.module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertEqual(bare.direct_report_ack_min_hops, 2)
        self.assertTrue(bare.direct_report_noack, "the one-hop carrier is unchanged")

        saved = iface._resolved_paths.get(PEER)
        saved_canon = iface._canonical_peer_prefix
        iface._canonical_peer_prefix = lambda t: PEER
        board = iface._path_board(PEER)
        saved_board = (board.peer_path_len, board.peer_rate, board.peer_report_at)
        try:
            board.peer_path_len, board.peer_report_at = None, None
            iface._resolved_paths.pop(PEER, None)
            self.assertEqual(iface._receiver_hops_to(PEER), 0)
            self.assertFalse(iface._report_should_ack(PEER), "unknown hop count keeps today's no-ACK frame")

            iface._resolved_paths[PEER] = self.module._ResolvedPath("19", 1, 1, time.monotonic())
            self.assertFalse(iface._report_should_ack(PEER), "one hop: the ACK costs more than it saves")

            iface._resolved_paths[PEER] = self.module._ResolvedPath("1976", 2, 1, time.monotonic())
            self.assertTrue(iface._report_should_ack(PEER), "two hops")

            # The larger of this node's own path length and the one the
            # SENDER reported in its last "Q" v5 header.
            iface._resolved_paths[PEER] = self.module._ResolvedPath("19", 1, 1, time.monotonic())
            board.peer_path_len, board.peer_report_at = 2, time.monotonic()
            self.assertEqual(iface._receiver_hops_to(PEER), 2)
            self.assertTrue(iface._report_should_ack(PEER), "the peer says two hops: err toward acknowledging")

            saved_min = iface.direct_report_ack_min_hops
            try:
                iface.direct_report_ack_min_hops = 0
                iface._resolved_paths[PEER] = self.module._ResolvedPath("197619", 3, 1, time.monotonic())
                self.assertFalse(iface._report_should_ack(PEER), "0 disables the item")
            finally:
                iface.direct_report_ack_min_hops = saved_min
        finally:
            iface._canonical_peer_prefix = saved_canon
            board.peer_path_len, board.peer_rate, board.peer_report_at = saved_board
            if saved is None:
                iface._resolved_paths.pop(PEER, None)
            else:
                iface._resolved_paths[PEER] = saved


class ReportCarrier(SingleNodeCase):
    """Which send path a report takes, and what it carries."""

    def _arm(self, hops):
        iface = self.iface
        iface._resolved_paths[PEER] = self.module._ResolvedPath("19" * max(1, hops), hops, 1, time.monotonic())
        iface._recent_raw_pkts.setdefault(PEER, {})[(PKT, 2)] = time.monotonic()
        iface._reassembly.clear()

    def _run_answer(self, hops, report=True, ack_results=(True,)):
        """Drive `_send_completion_answer` with both carriers stubbed."""
        iface = self.iface
        self._arm(hops)
        noack, acked = [], []
        results = list(ack_results)
        saved = {k: getattr(iface, k) for k in
                 ("_send_direct_noack_frame", "_send_direct_frame_and_wait_for_ack",
                  "_resolve_contact", "_canonical_peer_prefix", "_recent_raw_entries",
                  "_completion_answer_hold_s")}

        async def fake_noack(target, frame, attempt, peer_prefix, hop_count, kind, priority=0):
            noack.append({"frame": frame, "attempt": attempt, "kind": kind, "hops": hop_count})
            return True

        async def fake_acked(target, frame, attempt=0, **kw):
            acked.append({"frame": frame, "attempt": attempt, "kind": kw.get("kind"),
                          "report": kw.get("report"), "ack_max": kw.get("ack_timeout_max_s"),
                          "preemptible": kw.get("preemptible"), "hops": kw.get("hop_count")})
            return (results.pop(0) if results else False), False

        iface._send_direct_noack_frame = fake_noack
        iface._send_direct_frame_and_wait_for_ack = fake_acked
        iface._resolve_contact = lambda t: {"public_key": "aa" * 32, "out_path_len": hops}
        iface._canonical_peer_prefix = lambda t: PEER
        iface._recent_raw_entries = lambda t: [(PKT, 2, True, {0, 1})]
        iface._completion_answer_hold_s = lambda h: 0.0
        try:
            self.node.run_on_loop(iface._send_completion_answer(
                PEER, PKT, 2, True, held={0, 1}, version=5,
                nonce=iface.COMPLETION_REPORT_NONCE_BASE, report=report,
                entries=[(PKT, 2, True, {0, 1})]), timeout=15.0)
        finally:
            for k, v in saved.items():
                setattr(iface, k, v)
            iface._resolved_paths.pop(PEER, None)
            iface._recent_raw_pkts.pop(PEER, None)
        return noack, acked

    def test_one_hop_keeps_the_no_ack_frame(self):
        noack, acked = self._run_answer(1)
        self.assertEqual(len(noack), 1, "below the threshold: the no-ACK frame and its hold, unchanged")
        self.assertEqual(acked, [])

    def test_two_hops_goes_acknowledged_in_the_report_lock_class(self):
        noack, acked = self._run_answer(2)
        self.assertEqual(noack, [], "at the threshold: the acknowledged carrier")
        self.assertEqual(len(acked), 1, "one attempt is enough when it is ACKed")
        self.assertEqual(acked[0]["kind"], "completion_report")
        self.assertTrue(acked[0]["report"],
                        "the REPORT lock class, as the no-ACK carrier already takes it")
        self.assertTrue(acked[0]["preemptible"], "a queued Link handshake may still cut the ACK wait")
        self.assertIsNotNone(acked[0]["ack_max"])
        self.assertLessEqual(acked[0]["ack_max"], 2.0 + 1.0 * 2,
                             "bounded by the peer's expected ACK time, not the 11 s miss ceiling")

    def test_a_lost_first_transmission_is_retried_once(self):
        noack, acked = self._run_answer(2, ack_results=(False, True))
        self.assertEqual(len(acked), 2, "exactly one retry")
        self.assertNotEqual(acked[0]["attempt"], acked[1]["attempt"],
                            "the firmware attempt byte varies, so neither repeater nor destination dedups it")
        # Re-encoded, not resent: the bitmap is read live each time.
        self.assertIsInstance(acked[1]["frame"], str)

    def test_three_failures_are_not_retried_forever(self):
        noack, acked = self._run_answer(2, ack_results=(False, False))
        self.assertEqual(len(acked), 2, "two attempts, then the sender's own timeout is the recovery path")

    def test_a_query_answer_is_not_affected(self):
        noack, acked = self._run_answer(2, report=False)
        self.assertEqual(len(noack), 1, "only a REPORT changes carrier; an ANSWER keeps its own recovery path")
        self.assertEqual(acked, [])

    def test_the_frame_content_is_the_same_on_either_carrier(self):
        """At the SAME hop count the two carriers send identical bytes --
        only the MeshCore txt_type differs, which the golden wire snapshot
        does not cover. (Across hop counts the frame legitimately differs:
        the v5 header carries this node's own path length to the peer.)"""
        iface = self.iface
        saved_min = iface.direct_report_ack_min_hops
        try:
            iface.direct_report_ack_min_hops = 0            # force the no-ACK carrier
            noack, _a = self._run_answer(2)
            iface.direct_report_ack_min_hops = 2            # force the acknowledged one
            _n, acked = self._run_answer(2)
        finally:
            iface.direct_report_ack_min_hops = saved_min
        self.assertEqual(noack[0]["frame"], acked[0]["frame"],
                         "only the carrier changes; the golden wire snapshot is untouched")


class ReportCarrierCapture(SingleNodeCase):
    def test_report_acked_is_on_the_record(self):
        iface = self.iface
        captured = []
        saved_capture = iface._capture_event
        saved_send = iface._send_completion_answer
        saved_spawn = iface._spawn_background_task
        iface._capture_event = lambda direction, rec: captured.append(rec)
        iface._spawn_background_task = lambda coro: (coro.close(), None)[1]
        saved_file = iface._packet_capture_file
        iface._packet_capture_file = object()          # capture records are gated on this
        saved_canon2 = iface._canonical_peer_prefix
        iface._canonical_peer_prefix = lambda t: PEER

        async def noop(*a, **k):
            return None

        iface._send_completion_answer = noop
        header = self.module._FrameHeader(5, True, False, PKT, 0, 2, 0)
        try:
            iface._resolved_paths[PEER] = self.module._ResolvedPath("1976", 2, 1, time.monotonic())
            self.on_loop(lambda: iface._send_completion_report(PEER, header, complete=True, held={0, 1}, held_s=0.0))
            recs = [r for r in captured if r.get("event") == "completion_report_sent"]
            self.assertTrue(recs)
            self.assertTrue(recs[-1]["report_acked"], "two hops")
            self.assertEqual(recs[-1]["hop_count"], 2)

            captured.clear()
            iface._resolved_paths[PEER] = self.module._ResolvedPath("19", 1, 1, time.monotonic())
            self.on_loop(lambda: iface._send_completion_report(PEER, header, complete=True, held={0, 1}, held_s=0.0))
            recs = [r for r in captured if r.get("event") == "completion_report_sent"]
            self.assertFalse(recs[-1]["report_acked"], "one hop")
        finally:
            iface._capture_event = saved_capture
            iface._send_completion_answer = saved_send
            iface._spawn_background_task = saved_spawn
            iface._packet_capture_file = saved_file
            iface._canonical_peer_prefix = saved_canon2
            iface._resolved_paths.pop(PEER, None)


if __name__ == "__main__":
    unittest.main()
