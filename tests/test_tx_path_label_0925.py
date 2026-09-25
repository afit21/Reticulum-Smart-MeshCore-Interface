"""Pass 1 item 4 (2026-09-25): an attempt is labelled and scored by the path
it actually went over.

Text frames ("R" and "Q") are routed by the path stored on the device
contact. `direct_attempt_result.hop_count` was the path the caller resolved
when the SEND started, and `_note_path_attempt_result` credited the path
resolved when the attempt ENDED -- so when another send's `_select_path`
moved the contact to a trial path mid-send, or the firmware rewrote the
contact's path itself (a PATH_UPDATE the interface never subscribed to), the
capture labelled the attempt with one path and the scoreboard may have
credited another. The 2026-09-24 captures show attempts labelled hop 1
whose own echo came back at path length 3.

Now `_tx_path` reads the path with the radio lock held, just before the
frame is sent; the capture records it as `hop_count` / `path_hex` (the old
value stays as `hop_count_at_send_start`) and the scoreboard credits it. A
firmware PATH_UPDATE marks the contact's path unknown, so the next
`_select_path` puts the scoreboard's choice back, and is captured as
`contact_path_changed`.
"""
import types

from tests.test_path_selection_0922 import _Scaffold, PEER, PEER_KEY


class TxPathRule(_Scaffold):
    def test_device_path_wins_over_the_resolved_path(self):
        iface = self.iface
        iface._add_path_candidate(PEER, "", 0, 1, "flood")
        self.assertIsNotNone(self._select())
        iface._add_path_candidate(PEER, "aabbcc", 3, 1, "discovered")
        self.assertEqual(iface._tx_path(PEER), ("", 0), "the contact holds the selected path")
        self._board().device_path = "aabbcc"      # another send's trial moved the contact
        self.assertEqual(iface._tx_path(PEER), ("aabbcc", 3))

    def test_unknown_device_path_falls_back_to_the_resolved_path(self):
        iface = self.iface
        iface._add_path_candidate(PEER, "19", 1, 1, "flood")
        self.assertIsNotNone(self._select())
        self._board().device_path = None
        self.assertEqual(iface._tx_path(PEER), ("19", 1))
        self.assertEqual(iface._tx_path("000000000000"), (None, None))
        self.assertEqual(iface._tx_path(None), (None, None))


class AttemptIsLabelledAndScoredByItsTxPath(_Scaffold):
    def test_a_mid_send_trial_relabels_and_rescores_the_attempt(self):
        iface = self.iface
        iface._add_path_candidate(PEER, "", 0, 1, "flood")
        self.assertIsNotNone(self._select())
        trial = iface._add_path_candidate(PEER, "aabbcc", 3, 1, "discovered")
        zero = self._board().candidates[""]
        self._board().device_path = "aabbcc"

        async def fake_ack(*_a, **_k):
            # (ok, waited_full_timeout, ack_timeout_s, source, ack_latency_s, hop1_abort_deadline_s)
            return True, False, 10.0, "firmware", 4.1, None
        orig = iface._await_direct_ack
        iface._await_direct_ack = fake_ack
        sink, restore = self._capture()
        try:
            ok, _full = self.node.run_on_loop(iface._send_direct_frame_and_wait_for_ack(
                PEER_KEY, "Rprobe", 0, peer_prefix=PEER, hop_count=0), timeout=15)
        finally:
            iface._await_direct_ack = orig
            restore()
        self.assertTrue(ok)
        rec = [r for r in sink if r.get("event") == "direct_attempt_result"][-1]
        self.assertEqual(rec["hop_count"], 3)
        self.assertEqual(rec["path_hex"], "aabbcc")
        self.assertEqual(rec["hop_count_at_send_start"], 0)
        self.assertIsNotNone(trial.last_success_at, "the path the frame went over gets the credit")
        self.assertIsNone(zero.last_success_at, "not the path resolved when the send started")


class PathUpdateMarksTheContactPathUnknown(_Scaffold):
    def test_path_update_clears_device_path_and_is_captured(self):
        iface = self.iface
        iface._add_path_candidate(PEER, "19", 1, 1, "flood")
        self.assertIsNotNone(self._select())
        self.assertEqual(self._board().device_path, "19")
        sink, restore = self._capture()
        try:
            iface._on_path_update(types.SimpleNamespace(payload={"public_key": PEER_KEY}))
            iface._on_path_update(types.SimpleNamespace(payload={"public_key": "99" * 32}))   # not a peer
            iface._on_path_update(types.SimpleNamespace(payload=None))
        finally:
            restore()
        self.assertIsNone(self._board().device_path)
        self.assertEqual([r["event"] for r in sink], ["contact_path_changed"])
        self.assertEqual(sink[0]["previous_device_path"], "19")
        self.assertIsNotNone(self._select())
        self.assertEqual(self._board().device_path, "19", "the next selection puts the scoreboard's choice back")


if __name__ == "__main__":
    import unittest
    unittest.main()
