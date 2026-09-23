"""Alpha 0.1.9, second pass, item 2: path eligibility weighs the measured rate.

Defect B (session 1 of 2026-09-23 evening, the 57-minute drive out to four
hops on `cb4cd49`). The desktop made 23 path decisions and the laptop 22;
16 of the desktop's decisions that chose a path never delivered (84
attempts, 626 s of ACK waits), three of them trials of the zero-hop path at
21:33:37, 21:41:43 and 21:46:17 -- measured dead, 0.0 over five and then
ten missed sends. Two rules let that happen. A candidate was re-admitted
`path_switch_cooldown` (120 s) after its last miss whatever its measured
rate, and the healthy-path patience that keeps a delivering path through
its misses applied only above PATH_HEALTHY_RATE (0.5) -- which no path
reaches when moving at two to four hops, so the current path lost its
eligibility on its misses and the board cycled through everything on
cooldown, dead zero-hop path included.

The rule now (`_choose_path`, still pure):
  * a candidate with PATH_EXHAUST_MISSES consecutive missed attempts is DEAD
    and stays ineligible, whatever the cooldown says, until fresh external
    evidence for it arrives (a flood copy, a zero-hop peer report, a
    discovery result -- anything that refreshes `last_seen` after its last
    failure; discovery also resets its count);
  * after its cooldown a candidate is eligible only if its measured rate is
    unknown, at least the current path's measured rate, or fresh evidence
    has arrived since its last miss;
  * past the miss threshold the current path is KEPT ("current_best") while
    it has delivered in the window and its measured rate beats the rate of
    every eligible alternative -- measured, or the prior it is ranked with
    -- unless it is dead. This generalises the 0.5 patience to "better than
    the alternatives";
  * nothing eligible: "exhausted", and the caller runs discovery, as before.

The replays read `tests/fixtures/field_0923_path_decisions.json`: every
decision of both drive captures and alpha 0.1.8's 11:41 pair, as the
records printed the scoreboard, with the per-send miss counts of those
builds rescaled to attempts. Replayed through the rule these builds ran,
the fixture reproduces every field decision that came out of
`_choose_path` (checked when the fixture was built; the old rule is gone
from the tree, so the check is recorded in docs/history.md rather than
re-run here).
"""
import json
import os
import time
import unittest

from tests._support import REPO_ROOT, load_interface_module
from tests.test_path_selection_0922 import _Scaffold, _view, PEER

FIXTURE = os.path.join(REPO_ROOT, "tests", "fixtures", "field_0923_path_decisions.json")

# The shipped tuning: switch after 4 missed attempts, 120 s cooldown.
SWITCH, COOLDOWN, WEAK_SNR, WINDOW, HALF_LIFE = 4, 120.0, 3.0, 600.0, 180.0
KW = dict(optimistic=0.8, weak=0.25, rate_floor=0.05)


def _choose(Iface, views, current, now, **kw):
    return Iface._choose_path(views, current, now, SWITCH, COOLDOWN, WEAK_SNR, WINDOW, HALF_LIFE, **dict(KW, **kw))


def _rate_samples(now, rate, n=1000):
    k = round(rate * n)
    return [(now - 1, True)] * k + [(now - 1, False)] * (n - k)


def _decision_views(d):
    """The scoreboard at one field decision, as plain views."""
    now = d["ts"]
    out = []
    for c in d["candidates"]:
        if c["measured"]:
            samples = _rate_samples(now, c["rate"])
        elif c["stale"]:
            samples = [(now - WINDOW - 100, False)]      # evidence that aged out: stale
        else:
            samples = []
        out.append(_view(c["path_hex"], c["hops"], samples=samples, snr=c["snr"], peer_rate=c["peer_rate"],
                         cooldown_until=c["cooldown_until_ts"] or 0.0, consecutive_misses=c["misses_attempts"],
                         last_failure_at=c["last_failure_ts"], last_seen=None, source=c["source"]))
    return out


class _Pure(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_interface_module()
        cls.Iface = cls.module.SmartMeshCoreInterface
        with open(FIXTURE) as f:
            cls.fixture = json.load(f)

    def _decision(self, session, hhmmss):
        found = [d for d in self.fixture["sessions"][session]["decisions"] if d["hhmmss"] == hhmmss]
        self.assertEqual(len(found), 1, f"{session} {hhmmss}")
        return found[0]

    def _replay(self, session, hhmmss):
        d = self._decision(session, hhmmss)
        kw = {"peer_path_len": d["peer_path_len"]} if d.get("peer_path_len") is not None else {}
        hex_, reason, ranked = _choose(self.Iface, _decision_views(d), d["current"], d["ts"], **kw)
        return d, hex_, reason, ranked


class TheFieldDecisions(_Pure):
    def test_fixture_shape(self):
        s = self.fixture["sessions"]
        self.assertEqual(len(s["session1_desktop"]["decisions"]), 23)
        self.assertEqual(len(s["session1_laptop"]["decisions"]), 11)
        d = self._decision("session1_desktop", "21:41:43")
        zero = next(c for c in d["candidates"] if c["path_hex"] == "")
        self.assertEqual((zero["rate"], zero["measured"], zero["misses_sends"]), (0.0, True, 5))

    def test_desktop_no_trial_of_the_dead_zero_hop_path(self):
        """21:33:37, 21:41:43, 21:46:17: the field trialled the zero-hop
        path each time -- stale-weak at the first, measured 0.0 over five
        and then ten missed sends at the others."""
        for hhmmss in ("21:33:37", "21:41:43", "21:46:17"):
            d, hex_, reason, _ = self._replay("session1_desktop", hhmmss)
            self.assertEqual((d["field_reason"], d["field_choice"]), ("trial", ""), "the field's decision")
            self.assertNotEqual(hex_, "", f"{hhmmss}: no trial of the dead zero-hop path")

    def test_desktop_2133_keeps_the_two_hop_path_at_0_31(self):
        _d, hex_, reason, _ = self._replay("session1_desktop", "21:33:37")
        self.assertEqual((hex_, reason), ("0276", "current_best"),
                         "0.31 measured beats every eligible alternative: the zero-hop and one-hop paths are dead, "
                         "1976 (0.36) missed 54 s ago and is inside its cooldown")

    def test_desktop_2141_trials_the_better_measured_four_hop_path(self):
        # The current two-hop path has fallen to 0.21; the four-hop path is
        # measured at 0.43 and its last miss is 220 s old -- the field
        # trialled it 95 s later anyway, after the dead zero-hop path.
        _d, hex_, reason, _ = self._replay("session1_desktop", "21:41:43")
        self.assertEqual((hex_, reason), ("19be4f76", "trial"))

    def test_desktop_2146_runs_discovery(self):
        _d, hex_, reason, _ = self._replay("session1_desktop", "21:46:17")
        self.assertEqual((hex_, reason), (None, "exhausted"),
                         "every candidate dead or worse than 0.07 and cooling: discovery")

    def test_laptop_2128_to_2132_one_trial_then_the_one_hop_path(self):
        """The laptop trialled a two-, a three- and a four-hop candidate four
        times with no evidence for them while its one-hop path read 0.31
        to 0.37. At most one trial, then discovery -- here the one trial is
        the two-hop candidate the peer reported at 1.0, and after it the
        one-hop path is kept until it dies, when discovery runs."""
        trials = []
        for hhmmss in ("21:28:20", "21:29:08", "21:30:22", "21:30:44", "21:31:12", "21:31:49", "21:32:14", "21:32:21"):
            _d, hex_, reason, _ = self._replay("session1_laptop", hhmmss)
            if reason == "trial":
                trials.append((hhmmss, hex_))
            else:
                self.assertEqual((hex_, reason), ("19", "current_best"), hhmmss)
        self.assertEqual(trials, [("21:28:20", "0219")])

    def test_alpha018_1141_still_trials_the_fresh_candidate_after_a_dead_path(self):
        d, hex_, reason, _ = self._replay("alpha018_desktop", "11:41:22")
        self.assertEqual((d["field_reason"], d["field_choice"]), ("trial", "1902"))
        self.assertEqual((hex_, reason), ("1902", "trial"))


class TheRules(_Pure):
    def test_a_dead_candidate_stays_out_whatever_the_cooldown_says(self):
        now = time.monotonic()
        current = _view("0276", 2, samples=_rate_samples(now, 0.3), consecutive_misses=4, last_failure_at=now - 1)
        dead = _view("", 0, samples=_rate_samples(now, 0.0), consecutive_misses=10, last_failure_at=now - 900)
        hex_, reason, _ = _choose(self.Iface, [current, dead], "0276", now)
        self.assertEqual((hex_, reason), ("0276", "current_best"))
        # Alone with nothing better than dead, a path at 0.0 is not kept.
        zero = _view("0276", 2, samples=_rate_samples(now, 0.0), consecutive_misses=4, last_failure_at=now - 1)
        self.assertEqual(_choose(self.Iface, [zero, dead], "0276", now)[:2], (None, "exhausted"))

    def test_fresh_external_evidence_readmits_a_dead_candidate(self):
        now = time.monotonic()
        current = _view("0276", 2, samples=_rate_samples(now, 0.0), consecutive_misses=4, last_failure_at=now - 1)
        heard = _view("", 0, samples=_rate_samples(now, 0.0), consecutive_misses=10, last_failure_at=now - 300,
                      last_seen=now - 20, snr=11.0)
        self.assertEqual(_choose(self.Iface, [current, heard], "0276", now)[:2], ("", "trial"),
                         "a flood copy after its last failure: tried again, whatever its old rate")
        # And against a DELIVERING current path it competes on that copy's
        # prior (11 dB: 0.8), not on its old 0.0.
        delivering = dict(current, samples=_rate_samples(now, 0.4))
        self.assertEqual(_choose(self.Iface, [delivering, heard], "0276", now)[:2], ("", "trial"))
        weakly = dict(heard, snr=-5.0)
        self.assertEqual(_choose(self.Iface, [delivering, weakly], "0276", now)[:2], ("0276", "current_best"),
                         "heard again, but weakly (0.25): the 0.4 path is kept")
        # ...but only after its cooldown, like any other candidate.
        recent = dict(heard, last_failure_at=now - 30)
        self.assertEqual(_choose(self.Iface, [current, recent], "0276", now)[:2], (None, "exhausted"))

    def test_after_the_cooldown_only_a_candidate_no_worse_than_the_current_is_eligible(self):
        now = time.monotonic()
        current = _view("0276", 2, samples=_rate_samples(now, 0.05), consecutive_misses=6, last_failure_at=now - 1)
        worse = _view("19", 1, samples=_rate_samples(now, 0.02), consecutive_misses=5, last_failure_at=now - 200)
        self.assertEqual(_choose(self.Iface, [current, worse], "0276", now)[:2], ("0276", "current_best"))
        better = dict(worse, samples=_rate_samples(now, 0.4))
        self.assertEqual(_choose(self.Iface, [current, better], "0276", now)[:2], ("19", "trial"))
        unknown = dict(worse, samples=[])
        self.assertEqual(_choose(self.Iface, [current, unknown], "0276", now)[:2], ("19", "trial"),
                         "an unmeasured candidate after its cooldown is scored on its prior (0.8 beats 0.05)")

    def test_the_current_path_is_kept_while_it_beats_every_eligible_alternative(self):
        """The generalised patience: 0.36 at two hops is kept against a
        weak untried candidate (0.25), not against an optimistic one (0.8),
        and not once it is dead."""
        now = time.monotonic()
        current = _view("1976", 2, samples=_rate_samples(now, 0.36), consecutive_misses=6, last_failure_at=now - 1)
        weak = _view("0276", 2, snr=-4.0)
        self.assertEqual(_choose(self.Iface, [current, weak], "1976", now)[:2], ("1976", "current_best"))
        optimistic = _view("19", 1)
        self.assertEqual(_choose(self.Iface, [current, weak, optimistic], "1976", now)[:2], ("19", "trial"))
        dead = dict(current, consecutive_misses=self.module.PATH_EXHAUST_MISSES)
        self.assertEqual(_choose(self.Iface, [dead, weak], "1976", now)[:2], ("0276", "trial"))
        # Under the miss threshold nothing changed: "current", whatever scores better.
        fresh = dict(current, consecutive_misses=1)
        self.assertEqual(_choose(self.Iface, [fresh, optimistic], "1976", now)[:2], ("1976", "current"))


class OnTheScoreboard(_Scaffold):
    def _board(self):
        return self.iface._path_boards[PEER]

    def _select(self):
        return self.node.run_on_loop(self.iface._select_path(PEER), timeout=5.0)

    def test_a_dead_zero_hop_candidate_is_not_trialled_until_heard_again(self):
        iface = self.iface
        iface._add_path_candidate(PEER, "0276", 2, 1, "flood")
        zero = iface._add_path_candidate(PEER, "", 0, 1, "flood")
        self._board().current = "0276"
        now = time.monotonic()
        cur = self._board().candidates["0276"]
        cur.samples.extend([(now - 30, True), (now - 20, False), (now - 10, False)])
        cur.consecutive_misses, cur.last_failure_at = iface.path_switch_after_misses, now - 1
        zero.samples.extend([(now - 400, False), (now - 300, False)])
        zero.consecutive_misses, zero.last_failure_at = self.module.PATH_EXHAUST_MISSES + 2, now - 300
        zero.last_seen = now - 600
        self.assertEqual(self._select().out_path_hex, "0276", "kept: the dead zero-hop path is not trialled")
        # A zero-hop flood copy arrives: fresh evidence re-admits it, and it
        # competes on what that copy says (heard at 10 dB: optimistic, 0.8)
        # rather than on the misses it superseded.
        iface._add_path_candidate(PEER, "", 0, 1, "flood", snr=10.0)
        self.assertEqual(self._select().out_path_hex, "", "heard directly again: trialled")


if __name__ == "__main__":
    unittest.main()
