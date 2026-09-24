"""
Path selection by measured reliability (alpha 0.1.6, item 1, 2026-09-22;
"Q" protocol v5). Replaces alpha 0.1.5's shorter-path adoption
(tests/test_shorter_path_adoption_0921.py), whose shortest-in-window rule
the field (2026-09-21 evening, desktop capture 21:33) showed adopting a
504 s old zero-hop route over a one-hop path confirmed 2 s earlier, missing
twice, resetting, and then sitting on a two-hop path for 32 minutes while
the laptop reached the desktop in one -- because the one-hop route was
never re-tried without a fresh flood.

Now every route to a peer (discovered, the reverse of its floods, the
zero-hop option, what the peer reports in a v5 frame) is a candidate on a
per-peer scoreboard; each scores as expected transmissions per delivered
frame times (hops + 1), from a windowed, recency-weighted delivery rate
(an untried path uses a prior: the peer's reported rate, else 0.25 for a
zero-hop candidate heard below `path_weak_snr_db`, else 0.8). The current
path is kept while it delivers; after `path_switch_after_misses` misses
the next send is a trial on the best alternative; a trial that beats the
current score by `path_switch_margin` becomes current for good, and only
when every candidate is exhausted does discovery run.

Pinned:
  * the pure functions: `_path_delivery_rate` (window, half-life),
    `_path_prior` (0.8 / 0.25 / the peer's rate), `_path_score`
    ((hops+1)/rate with the floor), `_rank_paths` (cooldown last, then
    score, hops, most recent), `_choose_path` (every reason) and
    `_switch_for_good` (margin and cooldown);
  * the field replay (tests/fixtures/field_0921_desktop_22h.json): at
    22:00:49 the one-hop path just confirmed is kept ("current"), not the
    504 s old zero-hop route; after the 22:10:12 / 22:10:24 misses on the
    two-hop path the one-hop candidate is trialled on its own record, with
    no flood newer than 22:08:42;
  * the v5 frame: path_len / rate round-trip (rate quantised to 1/250),
    0xFF = unknown, a v4 frame still decodes with both None, header size 6,
    a full frame fits the 160-character text limit;
  * the scoreboard on the interface: at most PATH_CANDIDATES_KEPT per
    peer, the rx-log tap adds a "flood" candidate, a peer report adds the
    zero-hop candidate, `_select_path` selects / trials / exhausts and
    writes the device contact and one `path_selected` event per decision,
    a v5 REPORT feeds `_note_peer_reported_path`;
  * shipped defaults and the `path_adopt_enabled` alias.
"""
import collections
import json
import os
import time
import unittest

from tests._support import SingleNodeCase, load_interface_module

PEER = "34ab12cd56ef"                 # the laptop's role: prefix begins 34
PEER_KEY = PEER + "00" * 26           # a full 32-byte key with that prefix
FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "field_0921_desktop_22h.json")

# The shipped tuning, spelled out so every pure call below reads on its own.
MISSES, COOLDOWN, WEAK_SNR, WINDOW, HALF_LIFE = 2, 120.0, 3.0, 600.0, 180.0


def _view(path_hex, hops, samples=(), snr=None, peer_rate=None, cooldown_until=0.0, consecutive_misses=0,
          last_failure_at=None, last_seen=0.0, source="flood"):
    return {"path_hex": path_hex, "hops": hops, "samples": list(samples), "snr": snr, "peer_rate": peer_rate,
            "cooldown_until": cooldown_until, "consecutive_misses": consecutive_misses,
            "last_failure_at": last_failure_at, "last_seen": last_seen, "source": source}


def _choose(Iface, views, current, now, misses=MISSES, cooldown=COOLDOWN):
    return Iface._choose_path(views, current, now, misses, cooldown, WEAK_SNR, WINDOW, HALF_LIFE)


def _order(ranked):
    return [r[1]["path_hex"] for r in ranked]


class _Pure(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_interface_module()
        cls.Iface = cls.module.SmartMeshCoreInterface


class PureScore(_Pure):
    def test_delivery_rate_is_none_without_samples_and_one_when_all_delivered(self):
        now = time.monotonic()
        self.assertIsNone(self.Iface._path_delivery_rate([], now, WINDOW, HALF_LIFE))
        self.assertEqual(self.Iface._path_delivery_rate([(now - 5, True), (now - 50, True)], now, WINDOW, HALF_LIFE), 1.0)
        self.assertEqual(self.Iface._path_delivery_rate([(now - 5, False), (now - 50, False)], now, WINDOW, HALF_LIFE), 0.0)

    def test_samples_older_than_the_window_count_for_nothing(self):
        now = time.monotonic()
        self.assertEqual(self.Iface._path_delivery_rate([(now - 700, False), (now - 10, True)], now, WINDOW, HALF_LIFE), 1.0)
        self.assertIsNone(self.Iface._path_delivery_rate([(now - 700, False)], now, WINDOW, HALF_LIFE),
                          "a sample outside the window is not a sample")

    def test_newer_samples_weigh_more(self):
        now = time.monotonic()
        # A miss now (weight 1) against a success three half-lives ago (weight 1/8).
        rate = self.Iface._path_delivery_rate([(now, False), (now - 3 * HALF_LIFE, True)], now, WINDOW, HALF_LIFE)
        self.assertLess(rate, 0.5)
        self.assertAlmostEqual(rate, 0.125 / 1.125, places=3)
        flipped = self.Iface._path_delivery_rate([(now, True), (now - 3 * HALF_LIFE, False)], now, WINDOW, HALF_LIFE)
        self.assertGreater(flipped, 0.5)

    def test_prior_is_optimistic_unless_zero_hop_and_weak_or_reported(self):
        prior = self.Iface._path_prior
        self.assertEqual(prior(1, None, WEAK_SNR), 0.8, "untried, no signal: optimistic")
        self.assertEqual(prior(0, None, WEAK_SNR), 0.8, "zero hop with no signal yet: optimistic")
        self.assertEqual(prior(0, 2.0, WEAK_SNR), 0.25, "zero hop heard below 3 dB: weak")
        self.assertEqual(prior(0, 11.75, WEAK_SNR), 0.8, "zero hop heard well: optimistic")
        self.assertEqual(prior(0, 3.0, WEAK_SNR), 0.8, "the threshold itself is not weak")
        # Reversed deliberately by alpha 0.1.8 (item 3): a weak last leg IS
        # weak evidence at any hop count. The laptop's 2026-09-22 22:30:26
        # record trialled a three-hop candidate heard once at -9 dB ahead of
        # its current path because this rule read `hops == 0` only.
        self.assertEqual(prior(2, 1.0, WEAK_SNR), 0.25, "a weak last leg is weak at any hop count (0.1.8 item 3)")
        self.assertEqual(prior(0, 2.0, WEAK_SNR, peer_rate=0.6), 0.6, "the peer's reported rate overrides the priors")
        self.assertEqual(prior(1, None, WEAK_SNR, peer_rate=0.6), 0.6)
        self.assertEqual(self.Iface.PATH_PRIOR_OPTIMISTIC, 0.8)
        self.assertEqual(self.Iface.PATH_PRIOR_WEAK, 0.25)

    def test_score_is_transmissions_per_delivery_times_hops_plus_one(self):
        score = self.Iface._path_score
        self.assertEqual(score(0, 1.0), 1.0)
        self.assertEqual(score(1, 0.5), 4.0)
        self.assertEqual(score(2, 0.8), 3.75)
        self.assertEqual(self.Iface.PATH_RATE_FLOOR, 0.05)
        self.assertEqual(score(0, 0.0), 1.0 / 0.05, "a zero rate scores at the floor, not infinity")
        self.assertEqual(score(0, 0.01), 1.0 / 0.05)
        # The reason the weak prior is 0.25 and not 0.4: a weak zero-hop
        # candidate (4.0) must score behind an untried two-hop one (3.75).
        self.assertGreater(score(0, self.Iface.PATH_PRIOR_WEAK), score(2, self.Iface.PATH_PRIOR_OPTIMISTIC))

    def test_rank_puts_cooldown_last_then_score_hops_recency(self):
        now = time.monotonic()
        rank = lambda views: self.Iface._rank_paths(views, now, WEAK_SNR, WINDOW, HALF_LIFE)
        best_but_cooling = _view("", 0, samples=[(now - 1, True)], cooldown_until=now + 60)
        one_hop = _view("19", 1)
        two_hop = _view("1976", 2)
        ranked = rank([best_but_cooling, two_hop, one_hop])
        self.assertEqual(_order(ranked), ["19", "1976", ""], "the cooled-down path ranks last whatever its score")
        self.assertEqual([round(r[0], 3) for r in ranked], [2.5, 3.75, 1.0])
        self.assertEqual([r[3] for r in ranked], [False, False, True], "measured flag")
        self.assertEqual([round(r[2], 3) for r in ranked], [0.8, 0.8, 1.0], "rate used")
        # A measured two-hop path at 100 % (3.0) still ranks behind an untried
        # one-hop (2.5); a weak untried zero-hop (4.0) behind an untried two-hop.
        self.assertEqual(_order(rank([_view("1976", 2, samples=[(now - 1, True)]), one_hop])), ["19", "1976"])
        self.assertEqual(_order(rank([_view("", 0, snr=-1.75), two_hop])), ["1976", ""])
        # Ties: same score, fewer hops first; same hops, most recently seen first.
        zero_hop_at_25 = _view("", 0, samples=[(now, True), (now, False), (now, False), (now, False)])   # 1 / 0.25 = 4.0
        one_hop_at_50 = _view("19", 1, samples=[(now, True), (now, False)])                             # 2 / 0.5 = 4.0
        self.assertEqual(_order(rank([one_hop_at_50, zero_hop_at_25])), ["", "19"])
        self.assertEqual(_order(rank([_view("19", 1, last_seen=now - 100), _view("76", 1, last_seen=now - 5)])),
                         ["76", "19"])


class PureChoose(_Pure):
    def test_none_without_candidates(self):
        self.assertEqual(_choose(self.Iface, [], None, time.monotonic()), (None, "none", []))

    def test_selected_the_best_eligible_when_there_is_no_current(self):
        now = time.monotonic()
        hex_, reason, ranked = _choose(self.Iface, [_view("1976", 2), _view("19", 1)], None, now)
        self.assertEqual((hex_, reason), ("19", "selected"))
        self.assertEqual(_order(ranked), ["19", "1976"])
        hex_, reason, _ = _choose(self.Iface, [_view("1976", 2), _view("19", 1)], "unknown", now)
        self.assertEqual((hex_, reason), ("19", "selected"), "a current path that is no candidate is no current path")

    def test_current_is_kept_below_the_miss_threshold_even_when_outscored(self):
        now = time.monotonic()
        current = _view("1976", 2, samples=[(now - 30, True), (now - 1, False)], consecutive_misses=1, last_failure_at=now - 1)
        better = _view("", 0, snr=12.0)                     # untried zero hop: score 1.25 against 6.0
        hex_, reason, ranked = _choose(self.Iface, [current, better], "1976", now)
        self.assertEqual((hex_, reason), ("1976", "current"))
        self.assertEqual(_order(ranked)[0], "", "outscored, and kept anyway: a delivering path is not abandoned on hop count")

    def test_current_best_when_the_current_is_the_best_eligible_after_its_misses(self):
        now = time.monotonic()
        # Two misses 130 s ago on an otherwise good path: past the threshold,
        # but its cooldown has elapsed, and its measured score (about 2.7)
        # still beats an untried two-hop alternative (3.75).
        samples = [(now - 140 - i, True) for i in range(6)] + [(now - 131, False), (now - 130, False)]
        current = _view("19", 1, samples=samples, consecutive_misses=2, last_failure_at=now - 130)
        hex_, reason, _ = _choose(self.Iface, [current, _view("1976", 2)], "19", now)
        self.assertEqual((hex_, reason), ("19", "current_best"))

    def test_a_healthy_current_is_not_exhausted_on_two_misses(self):
        """Fourth cut (the 2026-09-22 baseline suite): at one hop's ~50 %
        attempt success two consecutive missed sends are common; a path
        with a measured rate of at least PATH_HEALTHY_RATE stays in use
        (no discovery flood) until PATH_EXHAUST_MISSES misses.

        Re-pinned by alpha 0.1.9 (item 4) in ATTEMPTS: the counter now
        moves per attempt, so both thresholds doubled (a missed send is
        exactly `direct_send_attempts` = 2 consecutive missed attempts) and
        the scenario below is the same one expressed in the new unit. The
        patience the fourth cut bought is unchanged -- about 20 sends to a
        trial and about 340 to exhaustion on a path at 50 % attempt
        success."""
        now = time.monotonic()
        healthy = [(now - 60 + i, True) for i in range(6)] + [(now - 10, False), (now - 5, False)]
        # two missed sends = four missed attempts
        current = _view("19", 1, samples=healthy, consecutive_misses=4, last_failure_at=now - 5)
        hex_, reason, _ = _choose(self.Iface, [current], "19", now)
        self.assertEqual((hex_, reason), ("19", "current_best"))
        # a better-scoring alternative is still trialled after those misses
        hex_, reason, _ = _choose(self.Iface, [current, _view("", 0, snr=12.0)], "19", now)
        self.assertEqual((hex_, reason), ("", "trial"))
        # four missed sends (eight attempts) exhaust it even with the good record
        four = healthy + [(now - 3, False), (now - 1, False)]
        exhausted = _view("19", 1, samples=four, consecutive_misses=8, last_failure_at=now - 1)
        self.assertEqual(_choose(self.Iface, [exhausted], "19", now)[1], "exhausted")
        # an unhealthy one (no successes) is exhausted on two sends, as before
        fresh = _view("19", 1, samples=[(now - 10, False), (now - 5, False)], consecutive_misses=4, last_failure_at=now - 5)
        self.assertEqual(_choose(self.Iface, [fresh], "19", now)[1], "exhausted")
        self.assertEqual((self.module.PATH_HEALTHY_RATE, self.module.PATH_EXHAUST_MISSES), (0.5, 8))

    def test_trial_on_the_best_alternative_after_the_misses(self):
        now = time.monotonic()
        current = _view("1976", 2, samples=[(now - 12, False), (now, False)], consecutive_misses=2, last_failure_at=now)
        hex_, reason, _ = _choose(self.Iface, [current, _view("19", 1), _view("", 0, snr=-1.75)], "1976", now)
        self.assertEqual((hex_, reason), ("19", "trial"), "the weak zero-hop candidate (4.0) loses to the untried one-hop (2.5)")

    def test_exhausted_when_every_candidate_missed_recently(self):
        now = time.monotonic()
        views = [_view("1976", 2, consecutive_misses=2, last_failure_at=now - 5),
                 _view("19", 1, consecutive_misses=3, last_failure_at=now - 60),
                 _view("", 0, consecutive_misses=2, last_failure_at=now - 119)]
        hex_, reason, ranked = _choose(self.Iface, views, "1976", now)
        self.assertEqual((hex_, reason), (None, "exhausted"))
        self.assertEqual(len(ranked), 3, "the ranking is still reported")
        self.assertEqual(_choose(self.Iface, views, None, now)[:2], (None, "exhausted"), "with no current path too")
        never_failed = _view("aa", 1, consecutive_misses=2, last_failure_at=None)
        self.assertEqual(_choose(self.Iface, views + [never_failed], "1976", now)[:2], (None, "exhausted"),
                         "misses with no recorded failure time cannot age out")

    def test_a_candidate_is_eligible_again_once_its_last_miss_is_older_than_the_cooldown(self):
        now = time.monotonic()
        current = _view("1976", 2, consecutive_misses=2, last_failure_at=now)
        aged = _view("19", 1, consecutive_misses=2, last_failure_at=now - 121)
        self.assertEqual(_choose(self.Iface, [current, aged], "1976", now)[:2], ("19", "trial"))
        fresh = _view("19", 1, consecutive_misses=2, last_failure_at=now - 119)
        self.assertEqual(_choose(self.Iface, [current, fresh], "1976", now)[:2], (None, "exhausted"))
        self.assertEqual(_choose(self.Iface, [aged], None, now)[:2], ("19", "selected"))

    def test_switch_for_good_needs_the_margin_and_no_cooldown(self):
        now = time.monotonic()
        switch = self.Iface._switch_for_good
        self.assertTrue(switch(4.0, 3.0, 0.25, 0.0, now), "exactly 25 % better")
        self.assertFalse(switch(4.0, 3.01, 0.25, 0.0, now), "just short of the margin")
        self.assertTrue(switch(4.0, 4.0, 0.0, 0.0, now), "no margin: equal is enough")
        self.assertFalse(switch(4.0, 1.0, 0.25, now + 1, now), "a cooled-down candidate never switches back")
        self.assertTrue(switch(4.0, 1.0, 0.25, now, now), "cooldown just elapsed")


class FieldReplay(_Pure):
    """The 2026-09-21 desktop capture replayed through the PURE functions
    on a scoreboard of plain views: floods -> candidates (reversed path,
    signal), sends -> samples on the path they were sent on."""

    # Seconds since 21:50:00 local of the two decision points (the fixture's `t`).
    T_A = 649.969      # 22:00:49.969: the old rule adopted the 504 s old zero-hop route
    T_B = 1224.251     # 22:10:24.251: the second consecutive miss on "1976"
    T_MISS_1 = 1212.921
    T_NEWEST_FLOOD_BEFORE_B = 1122.84    # 22:08:42.840, a two-hop flood copy (path 7619)
    T_NEWEST_ONE_HOP_FLOOD_BEFORE_B = 848.345   # 22:04:08.345 (PATH, path 19)
    # The desktop's own ACK-derived signal for the zero-hop path: the capture's
    # `rx_log` DIRECT ACK at 21:55:03.820 (snr -1.75, rssi -114) matched the
    # zero-hop send whose direct_send_result is at 21:55:04.074 -- the laptop
    # driving out of direct range. The fixture carries no ACK records, so it
    # is applied here at its time.
    T_ZERO_HOP_ACK, ZERO_HOP_ACK_SNR, ZERO_HOP_ACK_RSSI = 303.820, -1.75, -114

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        with open(FIXTURE) as f:
            cls.fixture = json.load(f)
        cls.events = cls.fixture["events"]
        assert cls.events == sorted(cls.events, key=lambda e: e["t"]), "fixture events are in time order"

    def _replay(self, until):
        """Scoreboard views after every fixture event at or before `until`."""
        Iface = self.Iface
        views = {}

        def view(path_hex, hops, source):
            if path_hex not in views:
                views[path_hex] = _view(path_hex, hops, samples=collections.deque(maxlen=Iface.PATH_SAMPLES_KEPT), source=source)
            return views[path_hex]

        # The ACK-derived zero-hop signal, merged into the stream at its time.
        signal = {"t": self.T_ZERO_HOP_ACK, "kind": "signal", "path": "", "snr": self.ZERO_HOP_ACK_SNR,
                  "rssi": self.ZERO_HOP_ACK_RSSI}
        for ev in sorted(self.events + [signal], key=lambda e: e["t"]):
            if ev["t"] > until:
                break
            if ev["kind"] == "signal":
                if ev["path"] in views:
                    views[ev["path"]]["snr"], views[ev["path"]]["rssi"] = ev["snr"], ev["rssi"]
            elif ev["kind"] == "flood":
                if ev.get("src") != "34":
                    continue    # an ADVERT without adv_key in the capture: not attributable to the peer
                hash_size = max(1, (len(ev["path"]) // 2) // ev["path_len"]) if ev["path_len"] else 1
                v = view(Iface._reverse_flood_path(ev["path"], hash_size), ev["path_len"], "flood")
                v["snr"], v["rssi"], v["last_seen"] = ev["snr"], ev["rssi"], ev["t"]
            elif ev["kind"] == "send":
                v = view(ev["path"], ev["hops"], "discovered")
                v["samples"].append((ev["t"], ev["ok"]))
                v["last_seen"] = max(v["last_seen"], ev["t"])
                if ev["ok"]:
                    v["consecutive_misses"], v["last_success_at"] = 0, ev["t"]
                else:
                    v["consecutive_misses"] += 1
                    v["last_failure_at"] = ev["t"]
        return views

    def _at(self, kind, t):
        found = [e for e in self.events if e["kind"] == kind and e["t"] == t]
        self.assertEqual(len(found), 1, f"exactly one {kind} event at t={t}: {found}")
        return found[0]

    def test_fixture_shape(self):
        self.assertEqual(self.fixture["window"], "21:50:00-22:40:00")
        self.assertEqual(self.fixture["source"], "afipc_capture_Smart_MeshCore_Interface_20260921T213341.jsonl")
        kinds = collections.Counter(e["kind"] for e in self.events)
        self.assertEqual(kinds["send"], 95)
        self.assertEqual(kinds["flood"], 16)
        self.assertEqual(kinds["adopted"], 5)

    def test_a_the_confirmed_one_hop_path_is_kept_over_the_stale_zero_hop_route(self):
        # 22:00:47 success on "19" (adoption confirmed), 22:00:49 the old rule
        # adopted the zero-hop route last seen 503.8 s earlier.
        adopted = self._at("adopted", self.T_A)
        self.assertEqual((adopted["path"], adopted["old_path"], adopted["seen_age_s"]), ("", "19", 503.8))
        views = self._replay(self.T_A)
        self.assertEqual(views["19"]["consecutive_misses"], 0)
        self.assertEqual(views["19"]["samples"][-1][1], True)
        hex_, reason, ranked = _choose(self.Iface, views.values(), "19", self.T_A)
        self.assertEqual((hex_, reason), ("19", "current"))
        # The zero-hop candidate scores better on its (aging) 21:52-21:55 record;
        # "current" is what keeps the path that just delivered.
        self.assertEqual(_order(ranked)[0], "")

    def test_b_after_two_misses_on_the_two_hop_path_the_one_hop_candidate_is_trialled(self):
        miss1, miss2 = self._at("send", self.T_MISS_1), self._at("send", self.T_B)
        self.assertEqual((miss1["ok"], miss1["path"], miss2["ok"], miss2["path"]), (False, "1976", False, "1976"))
        between = [e for e in self.events if e["kind"] == "send" and self.T_MISS_1 < e["t"] < self.T_B]
        self.assertEqual(between, [], "the two misses are consecutive sends")
        before = [e for e in self.events if e["kind"] == "send" and e["t"] < self.T_MISS_1][-1]
        self.assertEqual((before["ok"], before["path"]), (True, "1976"), "and the send before them delivered")
        floods_before = [e["t"] for e in self.events if e["kind"] == "flood" and e["t"] < self.T_B]
        self.assertEqual(max(floods_before), self.T_NEWEST_FLOOD_BEFORE_B, "no flood newer than 22:08:42")
        one_hop_floods = [e["t"] for e in self.events if e["kind"] == "flood" and e["t"] < self.T_B and e["path_len"] == 1]
        self.assertEqual(max(one_hop_floods), self.T_NEWEST_ONE_HOP_FLOOD_BEFORE_B, "no one-hop flood for 376 s")

        views = self._replay(self.T_B)
        self.assertEqual(set(views), {"", "19", "1976"})
        self.assertEqual(views["1976"]["consecutive_misses"], 2)
        self.assertEqual(views[""]["snr"], self.ZERO_HOP_ACK_SNR)
        hex_, reason, ranked = _choose(self.Iface, views.values(), "1976", self.T_B)
        self.assertEqual((hex_, reason), ("19", "trial"), f"ranked: {[(round(s, 2), v['path_hex']) for s, v, _r, _m in ranked]}")
        self.assertLess(_order(ranked).index("19"), _order(ranked).index(""), "the one-hop candidate outranks the zero-hop one")
        # The one-hop path was chosen on its own 22:00-22:03 record: no flood after 22:04:08 refreshed it.
        self.assertEqual(views["19"]["last_seen"], self.T_NEWEST_ONE_HOP_FLOOD_BEFORE_B)

    def test_c_prior_values_from_the_capture(self):
        # 21:55:00 ACK at snr 2.0 (rssi -111) and the 21:54 ACKs around 11.75.
        self.assertEqual(self.Iface._path_prior(0, 2.0, WEAK_SNR), 0.25)
        self.assertEqual(self.Iface._path_prior(0, 11.75, WEAK_SNR), 0.8)
        self.assertEqual(self.Iface._path_prior(0, self.ZERO_HOP_ACK_SNR, WEAK_SNR), 0.25)


class WireV5(SingleNodeCase):
    def _entries(self, n=1, frag_total=3, complete=True):
        return [(700 + i, frag_total, complete, set(range(frag_total)) if complete else {0}) for i in range(n)]

    def test_constants(self):
        self.assertEqual(self.iface.COMPLETION_PROTOCOL_VERSION, 5)
        self.assertEqual(self.iface.COMPLETION_V5_HEADER_SIZE, 6)

    def test_v5_round_trips_path_len_and_rate(self):
        iface = self.iface
        frame = iface._encode_completion_frame_v5(iface.COMPLETION_TYPE_ANSWER, self._entries(2), nonce=0x2A, path_len=2, rate=0.733)
        decoded = iface._decode_completion_frame(frame)
        self.assertEqual((decoded.version, decoded.type, decoded.nonce), (5, iface.COMPLETION_TYPE_ANSWER, 0x2A))
        self.assertEqual(decoded.peer_path_len, 2)
        self.assertLessEqual(abs(decoded.peer_rate - 0.733), 0.003, "rate quantised to 1/250")
        self.assertEqual([(p, t, c) for p, t, c, _h in decoded.entries], [(700, 3, True), (701, 3, True)])
        self.assertEqual(decoded.entries[0][3], frozenset({0, 1, 2}))
        self.assertEqual((decoded.pkt_id, decoded.frag_total, decoded.complete), (700, 3, True), "first entry mirrored")
        edges = iface._decode_completion_frame(iface._encode_completion_frame_v5(
            iface.COMPLETION_TYPE_QUERY, self._entries(1, complete=False), nonce=1, path_len=0, rate=1.0))
        self.assertEqual((edges.peer_path_len, edges.peer_rate), (0, 1.0), "zero hop and a perfect rate are real values")
        self.assertIsNone(edges.held, "a QUERY's bitmap carries nothing")
        zero = iface._decode_completion_frame(iface._encode_completion_frame_v5(
            iface.COMPLETION_TYPE_ANSWER, self._entries(1), nonce=1, path_len=1, rate=0.0))
        self.assertEqual(zero.peer_rate, 0.0, "a measured 0 % is a value, not unknown")

    def test_unknown_path_len_and_rate_are_0xff(self):
        iface = self.iface
        frame = iface._encode_completion_frame_v5(iface.COMPLETION_TYPE_ANSWER, self._entries(1), nonce=3)
        raw = self.module._z85_decode(frame[len(iface.COMPLETION_MARKER):])
        self.assertEqual(list(raw[:6]), [5, iface.COMPLETION_TYPE_ANSWER, 1, 3, 0xFF, 0xFF])
        decoded = iface._decode_completion_frame(frame)
        self.assertIsNone(decoded.peer_path_len)
        self.assertIsNone(decoded.peer_rate)
        known = self.module._z85_decode(iface._encode_completion_frame_v5(
            iface.COMPLETION_TYPE_ANSWER, self._entries(1), nonce=3, path_len=1, rate=0.9)[len(iface.COMPLETION_MARKER):])
        self.assertEqual(list(known[4:6]), [1, 225], "rate byte = round(rate * 250)")

    def test_a_v4_frame_still_decodes_with_no_peer_fields(self):
        iface = self.iface
        frame = iface._encode_completion_frame_v4(iface.COMPLETION_TYPE_ANSWER, self._entries(2), nonce=7)
        self.assertEqual(self.module._z85_decode(frame[len(iface.COMPLETION_MARKER):])[0], 4, "the v4 encoder still writes v4")
        decoded = iface._decode_completion_frame(frame)
        self.assertEqual(decoded.version, 4, "the version is preserved so a v4 QUERY is answered in v4")
        self.assertIsNone(decoded.peer_path_len)
        self.assertIsNone(decoded.peer_rate)
        self.assertEqual([p for p, _t, _c, _h in decoded.entries], [700, 701])

    def test_a_full_v5_frame_fits_the_text_limit(self):
        iface = self.iface
        frame = iface._encode_completion_frame_v5(iface.COMPLETION_TYPE_ANSWER, self._entries(8), nonce=0xFF, path_len=3, rate=0.5)
        self.assertLessEqual(len(frame), 160, f"{len(frame)} characters")
        self.assertEqual(len(iface._decode_completion_frame(frame).entries), 8)


class _Scaffold(SingleNodeCase):
    def setUp(self):
        iface = self.iface
        self._own = iface._own_pubkey_hex
        iface._own_pubkey_hex = "7b" + "11" * 31   # the desktop's role: prefix begins 7b
        iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=True, last_seen=time.time())
        self.node.radio._upsert_contact(PEER_KEY, "laptop")
        self._refresh_contacts()
        self._clear()

    def _clear(self):
        iface = self.iface
        for d in (iface._path_boards, iface._resolved_paths, iface._raw_windows, iface._direct_path_failures):
            d.pop(PEER, None)

    def _refresh_contacts(self):
        # The library caches the device's contact table; the interface reads
        # that cache (`_mc.contacts`, `get_contact_by_key_prefix`).
        self.node.run_on_loop(self.iface._mc.commands.get_contacts(), timeout=5)

    def tearDown(self):
        iface = self.iface
        iface._own_pubkey_hex = self._own
        iface._peers.pop(PEER, None)
        self.node.radio.contacts.pop(PEER_KEY, None)
        self._refresh_contacts()
        self._clear()

    def _capture(self):
        iface = self.iface
        sink = []
        orig = iface._capture_event
        iface._capture_event = lambda direction, fields: sink.append(fields)
        iface._packet_capture_file = object()

        def restore():
            iface._capture_event = orig
            iface._packet_capture_file = None
        return sink, restore

    def _select(self):
        return self.node.run_on_loop(self.iface._select_path(PEER), timeout=10)

    def _board(self):
        return self.iface._path_board(PEER)


class ScoreboardOnTheInterface(_Scaffold):
    def _missed_send(self, peer=None, attempts=None):
        """One missed DIRECT send as production produces it (alpha 0.1.9,
        item 4): each of its attempts recorded through
        `_note_path_attempt_result`, then the send's own outcome. The miss
        COUNT comes from the attempts now and the rate SAMPLE from the send,
        so a test that drives only the send outcome no longer moves the
        counter -- deliberately, since that would count the airtime twice."""
        peer = peer or PEER
        for _ in range(attempts if attempts is not None else self.iface.direct_send_attempts):
            self.on_loop(lambda: self.iface._note_path_attempt_result(peer, False, True, "firmware"))
        self.on_loop(self.iface.record_direct_send_result, peer, False, True)

    def test_at_most_path_candidates_kept_per_peer(self):
        iface = self.iface
        now = time.monotonic()
        kept = iface.PATH_CANDIDATES_KEPT
        self.assertEqual(kept, 4)
        iface._add_path_candidate(PEER, "", 0, 1, "flood", now=now - 40, snr=1.0)      # weak: the worst score
        iface._add_path_candidate(PEER, "19", 1, 1, "flood", now=now - 30)
        iface._add_path_candidate(PEER, "1976", 2, 1, "flood", now=now - 20)
        iface._add_path_candidate(PEER, "aabb", 2, 1, "flood", now=now - 10)
        self.assertEqual(len(self._board().candidates), 4)
        iface._add_path_candidate(PEER, "cc", 1, 1, "flood", now=now)
        board = self._board()
        self.assertEqual(len(board.candidates), kept)
        self.assertNotIn("", board.candidates, "the worst-ranked non-current candidate was evicted")
        self.assertIn("cc", board.candidates)
        # The current path survives an eviction whatever it scores.
        board.current = "cc"
        iface._add_path_candidate(PEER, "cc", 1, 1, "flood", now=now, snr=-5.0)
        iface._add_path_candidate(PEER, "dd", 1, 1, "flood", now=now + 1)
        self.assertEqual(len(board.candidates), kept)
        self.assertIn("cc", board.candidates)
        # Refreshing keeps the first source and statistics, updates last_seen and the signal.
        cand = iface._add_path_candidate(PEER, "19", 1, 1, "discovered", now=now + 2, snr=9.0, rssi=-60)
        self.assertEqual((cand.source, cand.last_seen, cand.snr, cand.rssi), ("flood", now + 2, 9.0, -60))

    def test_rx_log_advert_adds_a_flood_candidate_with_the_reversed_route_and_signal(self):
        iface = self.iface
        payload = {"route_type": 1, "route_typename": "FLOOD", "payload_type": 4, "payload_typename": "ADVERT",
                   "payload_ver": 0, "path_len": 2, "path_hash_size": 1, "path": "d619", "payload_length": 60,
                   "pkt_payload": b"", "pkt_hash": 7, "snr": 6.0, "rssi": -70, "adv_key": PEER_KEY}
        event = type("E", (), {"payload": payload})()
        self.on_loop(iface._on_rx_log_data, event)
        cand = self._board().candidates.get("19d6")
        self.assertIsNotNone(cand, f"candidates: {list(self._board().candidates)}")
        self.assertEqual((cand.hops, cand.hash_size, cand.source, cand.snr, cand.rssi), (2, 1, "flood", 6.0, -70))
        self.assertEqual(len(cand.samples), 0, "untried")

    def test_peer_report_adds_the_zero_hop_candidate_and_sets_peer_rate_by_hop_count(self):
        iface = self.iface
        iface._add_path_candidate(PEER, "19", 1, 1, "flood")
        iface._note_peer_reported_path(PEER, 0, 0.9)
        board = self._board()
        self.assertIn("", board.candidates)
        self.assertEqual((board.candidates[""].hops, board.candidates[""].source), (0, "peer_report"))
        self.assertEqual(board.candidates[""].peer_rate, 0.9)
        self.assertIsNone(board.candidates["19"].peer_rate, "the report is about the peer's zero-hop path")
        iface._note_peer_reported_path(PEER, 1, 0.5)
        self.assertEqual(board.candidates["19"].peer_rate, 0.5)
        self.assertEqual(board.candidates[""].peer_rate, 0.9)
        self.assertEqual(len(board.candidates), 2, "a one-hop report does not invent a route")

    def test_select_path_sets_the_resolved_path_the_contact_and_one_event(self):
        iface = self.iface
        iface._add_path_candidate(PEER, "19", 1, 1, "flood")
        sink, restore = self._capture()
        try:
            resolved = self._select()
            again = self._select()
        finally:
            restore()
        self.assertEqual((resolved.out_path_hex, resolved.out_path_len, resolved.out_path_hash_len), ("19", 1, 1))
        self.assertIs(iface._resolved_paths[PEER], resolved)
        self.assertEqual(self._board().current, "19")
        contact = iface._resolve_contact(PEER)
        self.assertEqual((contact["out_path"], contact["out_path_len"]), ("19", 1), "persisted to the device contact")
        self.assertEqual(self._board().device_path, "19")
        events = [f for f in sink if f.get("event") == "path_selected"]
        self.assertEqual(len(events), 1, f"one decision, one event: {events}")
        self.assertEqual((events[0]["peer_prefix"], events[0]["reason"], events[0]["path_hex"], events[0]["path_len"]),
                         (PEER, "selected", "19", 1))
        self.assertEqual((events[0]["previous_path_hex"], events[0]["previous_path_len"]), (None, None))
        self.assertEqual([s["path_hex"] for s in events[0]["scores"]], ["19"])
        self.assertTrue({"path_hex", "hops", "score", "rate", "measured", "misses", "snr", "source"} <= set(events[0]["scores"][0]),
                        f"score entry fields: {events[0]['scores'][0]}")
        self.assertEqual(again.out_path_hex, "19", "the second call keeps the path")
        self.assertEqual(len([f for f in sink if f.get("event") == "path_selected"]), 1, "and writes no further event")

    def test_two_misses_then_a_trial_on_the_other_candidate(self):
        iface = self.iface
        iface._add_path_candidate(PEER, "19", 1, 1, "flood")
        iface._add_path_candidate(PEER, "1976", 2, 1, "flood")
        sink, restore = self._capture()
        try:
            first = self._select()
            self.assertEqual(first.out_path_hex, "19", "the untried one-hop candidate scores best")
            self._missed_send()
            self.assertEqual(iface._resolved_paths[PEER].out_path_hex, "19", "one missed send: nothing changes")
            self._missed_send()
            trial = self._select()
        finally:
            restore()
        board = self._board()
        # Two missed sends, now counted as their four attempts (item 4).
        self.assertEqual(board.candidates["19"].consecutive_misses, 4)
        self.assertEqual(trial.out_path_hex, "1976")
        self.assertEqual(iface._resolved_paths[PEER].out_path_hex, "1976")
        self.assertEqual(iface._resolve_contact(PEER)["out_path"], "1976", "the trial goes to the radio")
        self.assertEqual(board.current, "19", "a trial does not make the alternative current")
        self.assertEqual(board.candidates["1976"].trials, 1)
        reasons = [(f["reason"], f["path_hex"]) for f in sink if f.get("event") == "path_selected"]
        self.assertEqual(reasons, [("selected", "19"), ("trial", "1976")])
        self.assertEqual(iface._direct_path_failures.get(PEER, 0), 0, "the old stale-path detector did not count the misses")

    def test_query_round_evidence_is_not_a_sample_of_its_own(self):
        """Item 1, second cut (MeshBench shortcut_appears): a failing raw
        window recorded four or five misses -- each QUERY round's evidence
        plus the give-up -- and exhausted a path on one send. The window's
        outcome is the one sample; the per-round evidence passes through
        with `path_sample=False`."""
        iface = self.iface
        iface._add_path_candidate(PEER, "19", 1, 1, "flood")
        sink, restore = self._capture()
        try:
            self._select()
            miss = {"acked": False, "waited_full_timeout": True}
            self.on_loop(iface._record_query_path_evidence, PEER, [miss, miss])
            self.on_loop(iface._record_query_path_evidence, PEER, [miss, miss])
            self.assertEqual(self._board().candidates["19"].consecutive_misses, 0, "QUERY rounds are not samples")
            self.assertEqual(len(self._board().candidates["19"].samples), 0)
            # Alpha 0.1.9 (item 4) re-pin. A QUERY round is still not a rate
            # SAMPLE of its own -- that is what this test is about and it is
            # unchanged. What did change is that the round's individual
            # ATTEMPTS are now counted, because each one is airtime spent on
            # this path; `_record_query_path_evidence` does not make them, the
            # QUERY's own `_send_direct_frame_and_wait_for_ack` calls do.
            self.on_loop(lambda: iface._note_path_attempt_result(PEER, False, True, "firmware"))
            self.assertEqual(self._board().candidates["19"].consecutive_misses, 1, "the attempt is")
            self.on_loop(iface.record_direct_send_result, PEER, False, True)
            self.assertEqual(self._board().candidates["19"].consecutive_misses, 1,
                             "the send outcome adds the sample, not a second miss for the same airtime")
            self.assertEqual(len(self._board().candidates["19"].samples), 1, "the window's outcome is the sample")
            self.on_loop(lambda: iface.record_direct_send_result(PEER, False, True, path_sample=False))
            self.assertEqual(self._board().candidates["19"].consecutive_misses, 1)
            self.assertEqual(len(self._board().candidates["19"].samples), 1)
        finally:
            restore()

    def test_two_misses_on_the_only_candidate_exhaust_the_board_for_discovery(self):
        iface = self.iface
        iface._add_path_candidate(PEER, "19", 1, 1, "flood")
        sink, restore = self._capture()
        try:
            self._select()
            self._missed_send()
            self._missed_send()
            exhausted = self._select()
            again = self._select()
        finally:
            restore()
        self.assertIsNone(exhausted)
        self.assertIsNone(again)
        self.assertNotIn(PEER, iface._resolved_paths, "no resolved path: the caller runs discovery")
        events = [f for f in sink if f.get("event") == "path_selected"]
        self.assertEqual([f["reason"] for f in events], ["selected", "exhausted"], "exhaustion is reported once")
        self.assertEqual((events[1]["path_hex"], events[1]["previous_path_hex"]), (None, "19"))
        self.assertIn("19", self._board().candidates, "the candidate stays on the board (eligible again after the cooldown)")

    def test_a_v5_report_feeds_the_peer_reported_path(self):
        iface = self.iface
        frame = iface._encode_completion_frame_v5(
            iface.COMPLETION_TYPE_ANSWER, [(701, 2, True, {0, 1})], nonce=iface.COMPLETION_REPORT_NONCE_BASE | 0,
            path_len=0, rate=0.9)
        self.on_loop(iface._handle_incoming_completion_frame, frame, PEER)
        board = self._board()
        self.assertIn("", board.candidates, f"a zero-hop report makes the zero-hop candidate: {list(board.candidates)}")
        self.assertEqual(board.candidates[""].source, "peer_report")
        self.assertAlmostEqual(board.candidates[""].peer_rate, 0.9, places=2)


class ShippedDefaults(unittest.TestCase):
    def test_defaults_and_the_legacy_alias(self):
        module = load_interface_module()
        Iface = module.SmartMeshCoreInterface
        bare = Iface.__new__(Iface)
        bare._configure_path_discovery({})
        self.assertTrue(bare.path_selection_enabled)
        self.assertEqual(bare.path_weak_snr_db, 3.0)
        # Alpha 0.1.9 (item 4): 4 attempts = the 2 sends this used to mean.
        self.assertEqual(bare.path_switch_after_misses, 4)
        self.assertEqual(bare.path_switch_margin, 0.25)
        self.assertEqual(bare.path_switch_cooldown_s, 120.0)
        self.assertEqual((Iface.PATH_CANDIDATES_KEPT, Iface.PATH_SAMPLES_KEPT), (4, 8))
        self.assertEqual((Iface.PATH_SAMPLE_WINDOW_S, Iface.PATH_SAMPLE_HALF_LIFE_S), (600.0, 180.0))
        legacy = Iface.__new__(Iface)
        legacy._configure_path_discovery({"path_adopt_enabled": "no"})
        self.assertFalse(legacy.path_selection_enabled, "the alpha 0.1.5 key is an alias")
        explicit = Iface.__new__(Iface)
        explicit._configure_path_discovery({"path_adopt_enabled": "no", "path_selection_enabled": "yes"})
        self.assertTrue(explicit.path_selection_enabled, "the new key wins over the alias")
        self.assertFalse(hasattr(Iface, "PATH_ADOPT_MISS_LIMIT"), "the adoption rule is gone")
        self.assertFalse(hasattr(Iface, "_maybe_adopt_shorter_path"))


if __name__ == "__main__":
    unittest.main()
