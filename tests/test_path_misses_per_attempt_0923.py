"""Alpha 0.1.9, item 4: a path's miss count is attempts, not sends.

Airtime is spent per attempt; the scoreboard learned per send. Worse, the
two paths that spend the most airtime recorded nothing at all -- a
fragmented send's per-fragment attempts pass `record_result=False`, and a
QUERY round's evidence passes `path_sample=False` (alpha 0.1.6's second
cut, for the good reason that one failing window must not count four or
five samples).

The field bill. The desktop's 2026-09-23 capture, between 11:29:35 and
11:31:26 on a two-hop path that was dead: nine raw-fragment attempts
(pkt_id 8) and two QUERY attempts, every one a `firmware` miss at hop 2,
and not a single `direct_send_result` in the whole span. The board did not
reach its trial threshold until 11:41:22 -- ten minutes and a stop later --
and never reached "exhausted" at all. The same shape on 2026-09-22 between
22:28:01 and 22:30:38 cost 14 attempts and 2.5 minutes, and registered as
3 misses.

The unit that changed is the COUNT, not the rate. The delivery rate is
computed from `samples`, put on the wire in the "Q" v5 rate byte and read
by the peer as the first rule of `_path_prior`; `PATH_PRIOR_OPTIMISTIC`
(0.8), `PATH_PRIOR_WEAK` (0.25), `PATH_HEALTHY_RATE` (0.5) and
`PATH_RATE_FLOOR` are all calibrated against per-send rates, as is the
replay fixture `tests/fixtures/field_0921_desktop_22h.json`. So `samples`
stays one per send and `consecutive_misses` becomes per attempt, with both
thresholds doubled -- a missed send is exactly `direct_send_attempts` (2)
consecutive missed attempts -- so today's patience on a healthy path is
preserved exactly while a send with a bigger budget costs what it spends.
"""
import time
import unittest

from tests._support import SingleNodeCase
from tests.test_path_selection_0922 import _Scaffold, PEER


class MissesAreAttempts(_Scaffold):
    def _board(self):
        return self.iface._path_boards[PEER]

    def _select(self):
        return self.node.run_on_loop(self.iface._select_path(PEER), timeout=5.0)

    def _arm(self, path_hex="19", hops=1):
        self.iface._add_path_candidate(PEER, path_hex, hops, 1, "flood")
        self._select()
        return self._board().candidates[path_hex]

    def _attempt(self, ok, source="firmware", waited=True, latency=None):
        self.on_loop(lambda: self.iface._note_path_attempt_result(
            PEER, ok, waited, source, ack_latency_s=latency))

    # -- the thresholds and their unit ------------------------------------

    def test_the_thresholds_are_the_old_ones_in_attempts(self):
        iface = self.iface
        self.assertEqual(iface.direct_send_attempts, 2, "the factor the rescale uses")
        self.assertEqual(iface.path_switch_after_misses, 2 * 2)
        self.assertEqual(self.module.PATH_EXHAUST_MISSES, 4 * 2)

    def test_each_missed_attempt_counts_and_a_success_resets(self):
        cand = self._arm()
        for n in (1, 2, 3):
            self._attempt(False)
            self.assertEqual(cand.consecutive_misses, n)
        self._attempt(True, latency=1.2)
        self.assertEqual(cand.consecutive_misses, 0)

    def test_the_send_outcome_is_the_rate_sample_and_not_a_second_miss(self):
        # The airtime of a send is its attempts; counting the send as well
        # would bill the same transmissions twice.
        cand = self._arm()
        self._attempt(False)
        self._attempt(False)
        self.assertEqual(cand.consecutive_misses, 2)
        self.assertEqual(len(cand.samples), 0)
        self.on_loop(self.iface.record_direct_send_result, PEER, False, True)
        self.assertEqual(cand.consecutive_misses, 2, "the attempts already counted this send")
        self.assertEqual([ok for _t, ok in cand.samples], [False], "one rate sample per send, as before")

    # -- the field sequences ----------------------------------------------

    def test_the_2026_09_23_dead_path_burst_reaches_discovery_in_a_minute(self):
        """11:29:35 to 11:31:26: nine raw-fragment attempts and two QUERY
        attempts, all `firmware` misses at two hops, no send result at all.
        Under alpha 0.1.8 the board sat at 0 recorded misses through the
        whole burst; the trial came at 11:41:22 and "exhausted" never."""
        # The real inter-attempt times from the capture, as offsets.
        offsets = [0.0, 11.0, 20.0, 29.0, 51.0, 62.0, 74.0, 85.0, 94.0, 102.0, 111.0]
        cand = self._arm("1902", hops=2)
        sink, restore = self._capture()
        try:
            trial_at = exhausted_at = None
            for n, t in enumerate(offsets):
                self._attempt(False)
                if trial_at is None and cand.consecutive_misses >= self.iface.path_switch_after_misses:
                    trial_at = t
                if exhausted_at is None and cand.consecutive_misses >= self.module.PATH_EXHAUST_MISSES:
                    exhausted_at = t
            self.assertEqual(cand.consecutive_misses, 11, "every attempt on the dead path is counted")
            self.assertIsNotNone(trial_at)
            self.assertLessEqual(trial_at, 60.0,
                                 "the path stops being 'current' inside the first minute of the burst")
            self.assertIsNotNone(exhausted_at)
            self.assertLessEqual(exhausted_at, 120.0,
                                 "and the board exhausts before the burst ends, against never in 0.1.8")
            # The single candidate is past PATH_EXHAUST_MISSES with no
            # measured record, so the board exhausts and the caller runs
            # discovery -- the branch that never fired in the field.
            self.assertIsNone(self._select())
            self.assertNotIn(PEER, self.iface._resolved_paths,
                             "no resolved path: _send_direct_packet runs discovery next")
            reasons = [f["reason"] for f in sink if f.get("event") == "path_selected"]
            self.assertIn("exhausted", reasons)
        finally:
            restore()

    def test_the_2026_09_22_sequence_no_longer_registers_as_three_misses(self):
        """22:28:01 to 22:30:38: 14 failed attempts that the scoreboard
        recorded as 3 misses, so the exhaust rule (which needed 2 to 4) was
        never reached before an unrelated announce supplied a way out."""
        cand = self._arm("1976", hops=2)
        for _ in range(14):
            self._attempt(False)
        self.assertEqual(cand.consecutive_misses, 14)
        self.assertGreaterEqual(cand.consecutive_misses, self.module.PATH_EXHAUST_MISSES)
        self.assertIsNone(self._select(), "exhausted well before the 14th attempt")

    def test_a_healthy_path_at_fifty_percent_attempt_success_is_not_abandoned(self):
        """The fourth cut's patience, preserved. A send that succeeds on
        its second attempt is the normal shape at one hop; two consecutive
        missed attempts inside such a send must not exhaust the path."""
        cand = self._arm()
        # Six delivered sends: a real measured record above PATH_HEALTHY_RATE.
        for _ in range(6):
            self._attempt(False)
            self._attempt(True, latency=1.4)
            self.on_loop(self.iface.record_direct_send_result, PEER, True, True)
        rate = [ok for _t, ok in cand.samples]
        self.assertEqual(rate, [True] * 6)
        # Now one missed send: two consecutive missed attempts.
        self._attempt(False)
        self._attempt(False)
        self.on_loop(self.iface.record_direct_send_result, PEER, False, True)
        self.assertEqual(cand.consecutive_misses, 2)
        self.assertLess(cand.consecutive_misses, self.iface.path_switch_after_misses,
                        "one missed send does not even make it stop being the current path")
        self.assertIsNotNone(self._select(), "still resolved: no discovery flood")

    # -- what must NOT count ----------------------------------------------

    def test_only_a_genuine_path_miss_counts(self):
        """The allow-list, not a deny-list. Counting a locally shortened
        ceiling or a pre-empted wait as a path miss would kill live paths
        on this node's own decisions -- 6 of the 21 failed attempts in the
        desktop's 11:20-11:45 window were `report_window`."""
        cand = self._arm()
        for source in ("report_window", "preempted", "superseded", "answered",
                       "answered_before_send", "expired", "measured", "noack"):
            self._attempt(False, source=source)
            self.assertEqual(cand.consecutive_misses, 0, f"{source} is not evidence about the path")
        # A miss that did not wait the full ceiling never counted and still does not.
        self._attempt(False, source="firmware", waited=False)
        self.assertEqual(cand.consecutive_misses, 0)
        # hop1_abort does count: silence where a forward was due.
        self._attempt(False, source="hop1_abort")
        self.assertEqual(cand.consecutive_misses, 1)
        self._attempt(False, source="firmware")
        self.assertEqual(cand.consecutive_misses, 2)

    def test_a_no_ack_success_does_not_reset_the_count(self):
        # A no-ACK frame reports ok with no ACK behind it; treating that as
        # a delivery would clear a dying path's record on a frame that
        # proves nothing.
        cand = self._arm()
        self._attempt(False)
        self._attempt(False)
        self._attempt(True, source="noack")
        self.assertEqual(cand.consecutive_misses, 2)
        self._attempt(True, source="firmware", latency=None)
        self.assertEqual(cand.consecutive_misses, 2, "an ok with no measured ACK latency is not a delivery")
        self._attempt(True, source="firmware", latency=0.9)
        self.assertEqual(cand.consecutive_misses, 0)

    def test_an_unknown_path_or_peer_is_a_no_op(self):
        iface = self.iface
        self.on_loop(lambda: iface._note_path_attempt_result(None, False, True, "firmware"))
        self.on_loop(lambda: iface._note_path_attempt_result("ffffffffffff", False, True, "firmware"))
        self.assertNotIn("ffffffffffff", iface._path_boards)

    def test_the_item_is_off_with_path_selection(self):
        iface = self.iface
        cand = self._arm()
        saved = iface.path_selection_enabled
        iface.path_selection_enabled = False
        try:
            self._attempt(False)
            self.assertEqual(cand.consecutive_misses, 0)
        finally:
            iface.path_selection_enabled = saved


if __name__ == "__main__":
    unittest.main()
