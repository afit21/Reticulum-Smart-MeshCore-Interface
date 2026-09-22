"""Alpha 0.1.8, item 3: the scoreboard ages its evidence.

Each test replays one record from the 2026-09-22 evening field session
(`fieldtests/raw/Alpha0.1.7/`, desktop `afipc_` + laptop `a_`) through the
pure rules of `_paths.py`, which need no interface:

  * the desktop's 22:49:42 `path_selected` trialled the ZERO-HOP path while
    the laptop was two hops away, scored `rate 1.0, measured False, misses
    3, snr 11.75`. Its send outcomes had aged out of PATH_SAMPLE_WINDOW_S
    (hence `measured False`) but its peer-reported rate of 22:07 and its
    SNR reading of the zero-hop period never aged, so a dead path scored
    1.0 and outranked a one-hop candidate heard at 12.25 dB. Six more
    misses and 70 s. With the readings aged, that candidate is STALE, scores
    with PATH_PRIOR_WEAK and ranks behind every candidate with evidence
    inside the window: the rule now trials `19`.
  * the laptop's 22:30:26 record trialled a THREE-hop candidate `4fbe02`
    heard once at -9 dB, because the weak-signal prior read `hops == 0`
    only. It now applies at any hop count, so that candidate scores 0.25
    (16.0) instead of 0.8 (5.0) and no longer ranks first.
  * the laptop's later `d619` trial read -9.5 dB but carried a FRESH
    peer-reported rate of 0.668, was trialled on it and delivered (it
    became the current path two records later). The peer's own rate still
    comes before the weak-signal rule, so that trial still happens.

Also pinned: evidence that NEVER existed is not staleness (an untried
candidate keeps the optimistic prior, which is what bring-up depends on),
a reading with no timestamp is not aged (the pure-rule replays and every
candidate built before this release carry the value alone), and the
peer's reported path length makes a shorter untried candidate weak.
"""
import unittest

from tests._support import load_interface_module

MISSES, COOLDOWN, WEAK_SNR, WINDOW, HALF_LIFE = 2, 120.0, 3.0, 600.0, 180.0
NOW = 10_000.0
AGED = NOW - 2_500.0          # 41 minutes back: outside the 600 s window


def _view(path_hex, hops, samples=(), snr=None, signal_at=None, peer_rate=None, peer_rate_at=None,
          cooldown_until=0.0, consecutive_misses=0, last_failure_at=None, last_seen=0.0, source="flood"):
    return {"path_hex": path_hex, "hops": hops, "samples": list(samples), "snr": snr, "signal_at": signal_at,
            "peer_rate": peer_rate, "peer_rate_at": peer_rate_at, "cooldown_until": cooldown_until,
            "consecutive_misses": consecutive_misses, "last_failure_at": last_failure_at,
            "last_seen": last_seen, "source": source}


def _order(ranked):
    return [v["path_hex"] for _s, v, _r, _m in ranked]


class PathEvidenceAgeing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.Iface = load_interface_module().SmartMeshCoreInterface

    # -- the readings age ------------------------------------------------

    def test_peer_rate_and_signal_count_only_inside_the_window(self):
        evidence = self.Iface._path_evidence
        fresh = _view("", 0, snr=11.75, signal_at=NOW - 60, peer_rate=1.0, peer_rate_at=NOW - 60)
        self.assertEqual(evidence(fresh, NOW, WINDOW), (1.0, 11.75, False))
        old = _view("", 0, snr=11.75, signal_at=AGED, peer_rate=1.0, peer_rate_at=AGED)
        self.assertEqual(evidence(old, NOW, WINDOW), (None, None, True),
                         "readings outside the window score as unknown, and nothing else is left: stale")

    def test_a_reading_without_a_timestamp_is_not_aged(self):
        # The pure-rule replays in tests/test_path_selection_0922.py and any
        # candidate built before alpha 0.1.8 carry the value with no `_at`.
        view = _view("", 0, snr=2.0, peer_rate=None)
        self.assertEqual(self.Iface._path_evidence(view, NOW, WINDOW), (None, 2.0, False))
        self.assertEqual(self.Iface._path_prior(0, 2.0, WEAK_SNR), 0.25)

    def test_never_evidenced_is_untried_not_stale(self):
        # Staleness is evidence that expired, not evidence that never
        # existed -- an untried flood candidate must still be tried first.
        blank = _view("19", 1)
        self.assertEqual(self.Iface._path_evidence(blank, NOW, WINDOW), (None, None, False))
        self.assertEqual(self.Iface._path_prior(1, None, WEAK_SNR), 0.8)

    def test_a_stale_candidate_scores_weak_and_ranks_behind_evidence(self):
        stale = _view("", 0, samples=[(AGED, True)], snr=11.75, signal_at=AGED,
                      peer_rate=1.0, peer_rate_at=AGED, last_seen=NOW)
        # Measured at 30 %: it SCORES worse (10.0) than the stale
        # candidate's weak prior (4.0), and must still rank ahead of it.
        fresh_two_hop = _view("1976", 2, snr=11.75, signal_at=NOW - 30,
                              samples=[(NOW - 30, True), (NOW - 20, False), (NOW - 10, False)])
        ranked = self.Iface._rank_paths([stale, fresh_two_hop], NOW, WEAK_SNR, WINDOW, HALF_LIFE)
        self.assertEqual(_order(ranked), ["1976", ""],
                         "a stale candidate ranks behind one with evidence in the window, whatever it scores")
        by_hex = {v["path_hex"]: (s, r) for s, v, r, _m in ranked}
        self.assertEqual(by_hex[""][1], self.Iface.PATH_PRIOR_WEAK, "stale scores with the weak prior")
        self.assertLess(by_hex[""][0], by_hex["1976"][0], "and it still ranks last although it SCORES better")

    # -- the field records ----------------------------------------------

    def test_desktop_2249_trials_the_fresh_one_hop_not_the_stale_zero_hop(self):
        """The 22:49:42 record: '' stale, '19' fresh at 12.25 dB, '1976'
        the current path with two misses. Before item 3 the stale zero-hop
        candidate scored 1.0 and was trialled; six more misses and 70 s."""
        zero_hop = _view("", 0, samples=[(NOW - 1150, False)] * 3, snr=11.75, signal_at=AGED,
                         peer_rate=1.0, peer_rate_at=AGED, consecutive_misses=3, last_failure_at=NOW - 1150)
        one_hop = _view("19", 1, snr=12.25, signal_at=NOW - 60)
        current = _view("1976", 2, samples=[(NOW - 60, True), (NOW - 40, False), (NOW - 20, False)],
                        snr=11.75, signal_at=NOW - 30, consecutive_misses=2, last_failure_at=NOW - 20)
        views = [zero_hop, one_hop, current]

        chosen, reason, ranked = self.Iface._choose_path(
            views, "1976", NOW, MISSES, COOLDOWN, WEAK_SNR, WINDOW, HALF_LIFE)
        self.assertEqual((reason, chosen), ("trial", "19"), "the fresh one-hop candidate, not the stale zero-hop one")
        self.assertEqual(_order(ranked)[-1], "", "the stale candidate ranks last")
        self.assertTrue(self.Iface._path_evidence(zero_hop, NOW, WINDOW)[2])

        # The field's own scoring, for the record: rate 1.0 and score 1.0.
        self.assertEqual(self.Iface._path_prior(0, 11.75, WEAK_SNR, peer_rate=1.0), 1.0)
        self.assertEqual(self.Iface._path_score(0, 1.0), 1.0)

    def test_laptop_2230_no_longer_ranks_a_minus_nine_db_three_hop_first(self):
        """The 22:30:26 record: `4fbe02`, three hops, heard once at -9 dB,
        scored with the optimistic prior (0.8 -> 5.0) and ranked ahead of
        the current two-hop path at 10.143. Two misses, "exhausted",
        rediscovery, 26 s."""
        weak_three_hop = _view("4fbe02", 3, snr=-9.0, signal_at=NOW - 30)
        current = _view("7619", 2, samples=[(NOW - 300, True), (NOW - 60, False), (NOW - 30, False)],
                        snr=4.25, signal_at=NOW - 300, consecutive_misses=2, last_failure_at=NOW - 30)

        self.assertEqual(self.Iface._path_prior(3, -9.0, WEAK_SNR), self.Iface.PATH_PRIOR_WEAK,
                         "the weak-signal prior applies at any hop count")
        ranked = self.Iface._rank_paths([weak_three_hop, current], NOW, WEAK_SNR, WINDOW, HALF_LIFE)
        by_hex = {v["path_hex"]: s for s, v, _r, _m in ranked}
        self.assertEqual(by_hex["4fbe02"], 16.0, "was 5.0 on the optimistic prior")
        self.assertGreater(by_hex["4fbe02"], self.Iface._path_score(2, 0.296),
                           "and now scores worse than the current path did that evening (10.143)")

    def test_laptop_d619_keeps_its_trial_on_a_fresh_peer_rate(self):
        """The same session trialled `d619` at -9.5 dB on a fresh
        peer-reported rate of 0.668; it delivered and became current. The
        peer's own rate must still come before the weak-signal rule."""
        self.assertEqual(self.Iface._path_prior(2, -9.5, WEAK_SNR, peer_rate=0.668), 0.668)
        cand = _view("d619", 2, snr=-9.5, signal_at=NOW - 30, peer_rate=0.668, peer_rate_at=NOW - 30)
        current = _view("7619", 2, samples=[(NOW - 60, False), (NOW - 30, False)], snr=11.75, signal_at=NOW - 30,
                        consecutive_misses=2, last_failure_at=NOW - 30)
        chosen, reason, _ranked = self.Iface._choose_path(
            [cand, current], "7619", NOW, MISSES, COOLDOWN, WEAK_SNR, WINDOW, HALF_LIFE)
        self.assertEqual((reason, chosen), ("trial", "d619"))

    def test_an_aged_peer_rate_stops_counting_but_stays_on_the_capture(self):
        cand = _view("", 0, snr=11.75, signal_at=NOW - 30, peer_rate=1.0, peer_rate_at=AGED)
        peer_rate, snr, stale = self.Iface._path_evidence(cand, NOW, WINDOW)
        self.assertIsNone(peer_rate, "the 22:07 rate no longer counts")
        self.assertEqual(snr, 11.75, "the fresh signal still does")
        self.assertFalse(stale, "it is not stale: one reading is still inside the window")
        self.assertEqual(cand["peer_rate"], 1.0, "the reading itself is kept for the capture")
        self.assertEqual(self.Iface._path_prior(0, snr, WEAK_SNR, peer_rate), 0.8,
                         "without the stale rate it falls back to the prior, not to 1.0")

    # -- the peer's reported path length ---------------------------------

    def test_a_candidate_shorter_than_the_peers_reported_path_is_weak(self):
        prior = self.Iface._path_prior
        self.assertEqual(prior(0, None, WEAK_SNR, peer_path_len=2), 0.25,
                         "the peer says it needs two hops to reach us: an untried zero-hop claim is weak")
        self.assertEqual(prior(2, None, WEAK_SNR, peer_path_len=2), 0.8, "the same length is not weak")
        self.assertEqual(prior(3, None, WEAK_SNR, peer_path_len=2), 0.8, "a longer one is not weak")
        self.assertEqual(prior(0, None, WEAK_SNR, peer_rate=0.9, peer_path_len=2), 0.9,
                         "fresh evidence of its own still wins")

    def test_rank_kwargs_carry_the_peer_report_only_while_it_is_fresh(self):
        from tests._support import SingleNodeCase  # noqa: F401  (import cost only)
        iface = _StubBoard()
        iface.PATH_SAMPLE_WINDOW_S = WINDOW
        iface.path_weak_snr_db = WEAK_SNR
        iface.PATH_SAMPLE_HALF_LIFE_S = HALF_LIFE
        iface.PATH_PRIOR_OPTIMISTIC, iface.PATH_PRIOR_WEAK, iface.PATH_RATE_FLOOR = 0.8, 0.25, 0.05
        board = iface._path_boards["peer"]
        board.peer_path_len, board.peer_report_at = 2, NOW - 60
        kwargs = self.Iface._path_rank_kwargs(iface, "peer", now=NOW)
        self.assertEqual(kwargs.get("peer_path_len"), 2)
        board.peer_report_at = AGED
        self.assertNotIn("peer_path_len", self.Iface._path_rank_kwargs(iface, "peer", now=NOW),
                         "an aged peer report is not evidence")


class _StubBoard:
    """Just enough of the interface for `_path_rank_kwargs` (pure lookup)."""

    def __init__(self):
        module = load_interface_module()
        self._path_boards = {"peer": module._PathBoard()}


if __name__ == "__main__":
    unittest.main()
