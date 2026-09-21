"""
Phase 3, M1 (2026-09-20, docs/reconcile_redesign.md): completion REPORTs and
QUERY ANSWERs go out as MeshCore TXT_TYPE_CLI_DATA -- delivered, never ACKed
by the firmware -- and a gaps report is debounced behind the last fragment.

Firmware facts pinned by the fake `meshcore` (testscripts/simmesh): a
CMD_SEND_TXT_MSG frame with txt_type 1 (`[0x02][1][attempt][ts:4][dst:6]
[text]`, the frame the library's own `send_msg` builds with type 0) is
delivered as CONTACT_MSG_RECV with `txt_type: 1` and produces NO ACK
(`BaseChatMesh::onPeerDataRecv`: "no ack expected for CLI_DATA replies";
`MyMesh::onSerialFrame` sets expected_ack 0). Field numbers: 146 reports for
~105 bursts at zero hop; 23 of the 31 report lock waits over 1 s were the
previous report's ACK wait; the complete report followed the gaps report by
0.22-0.43 s at the receiver.

Pinned here:
  * `_noack_frame_hold_s`: airtime + zero-hop gap at hop 0; the raw relay
    gap rule (which includes the airtime) through repeaters;
  * `_report_hold_s`: one fragment airtime at hop 0; the raw relay gap at
    hops >= 1;
  * `_send_direct_noack_frame` against the fake: the frame arrives with
    txt_type 1, no ACK event is produced, the lock is held for the hold and
    released, one `direct_attempt_result` of the kind with
    `ack_timeout_source="noack"` and `on_air_bytes` is written;
  * `_send_completion_answer` uses the no-ACK path by default and the ACKed
    path with `direct_report_noack = no`;
  * debounce: a flagged fragment with gaps arms a hold; completion inside the
    hold cancels it and only the complete report goes; no completion sends
    the gaps report with the bitmap as of firing; `direct_report_debounce =
    no` reports at once.
"""
import asyncio
import time
import unittest

from tests._support import SingleNodeCase, SimMesh, wait_until, quiet_rns
from simmesh.radio import RadioOptions

PEER = "abcdef012345"


class PureTimingFunctions(SingleNodeCase):
    def test_noack_frame_hold(self):
        iface = self.iface
        saved = (iface.direct_raw_zero_hop_gap_s, iface.direct_raw_hop_gap_factor)
        iface.direct_raw_zero_hop_gap_s, iface.direct_raw_hop_gap_factor = 0.15, 2.0
        try:
            airtime = iface._estimate_tx_airtime_s("", on_air_bytes=40)
            self.assertAlmostEqual(iface._noack_frame_hold_s(40, 0), airtime + 0.15)
            self.assertAlmostEqual(iface._noack_frame_hold_s(40, 1), iface._raw_fragment_gap_s(1, 40))
            self.assertAlmostEqual(iface._noack_frame_hold_s(40, 1), 3.0 * airtime)
            self.assertAlmostEqual(iface._noack_frame_hold_s(40, 2), 5.0 * airtime)
        finally:
            iface.direct_raw_zero_hop_gap_s, iface.direct_raw_hop_gap_factor = saved

    def test_report_hold(self):
        iface = self.iface
        saved = iface.direct_raw_hop_gap_factor
        iface.direct_raw_hop_gap_factor = 2.0
        try:
            frag = 172
            airtime = iface._estimate_tx_airtime_s("", on_air_bytes=frag)
            # Alpha 0.1.6 (item 3): one sender spacing plus half an airtime
            # (it was one airtime at zero hop, the relay gap through repeaters).
            margin = iface.RAW_ARRIVING_HOLD_MARGIN_AIRTIMES * airtime
            self.assertAlmostEqual(iface._report_hold_s(frag, 0), airtime + max(0.0, iface.direct_raw_zero_hop_gap_s) + margin)
            self.assertAlmostEqual(iface._report_hold_s(frag, 1), 3.0 * airtime + margin)
            self.assertGreater(iface._report_hold_s(frag, 2), iface._report_hold_s(frag, 1))
        finally:
            iface.direct_raw_hop_gap_factor = saved

    def test_defaults(self):
        bare = self.module.SmartMeshCoreInterface.__new__(self.module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertTrue(bare.direct_report_noack)
        self.assertTrue(bare.direct_report_debounce)


class NoAckSendOnTheFakeMesh(unittest.TestCase):
    """Two nodes, zero hop, contacts known: A sends B one CLI_DATA frame."""

    @classmethod
    def setUpClass(cls):
        quiet_rns()
        cls.mesh = SimMesh(["A-B"], seed=3, startup_stagger_s=0.0, radio_options=RadioOptions(auto_advert=False, advert_interval_s=0))
        cls.a = cls.mesh.add_node("A", config={"peer_discovery_enabled": "no"})
        cls.b = cls.mesh.add_node("B", config={"peer_discovery_enabled": "no"})
        cls.mesh.advert_all()
        assert cls.mesh.advert_until_contacts(timeout=20.0), "contacts never populated"

    @classmethod
    def tearDownClass(cls):
        cls.mesh.stop()

    def test_frame_arrives_as_cli_data_without_an_ack(self):
        a, b = self.a, self.b
        received = []
        acks = []
        b.iface._mc.subscribe(b.iface._EventType.CONTACT_MSG_RECV, lambda ev: received.append(dict(ev.payload)))
        a.iface._mc.subscribe(a.iface._EventType.ACK, lambda ev: acks.append(ev))
        target = b.radio.pubkey
        frame = a.iface._encode_completion_frame(a.iface.COMPLETION_TYPE_ANSWER, 77, 3, complete=True, held={0, 1, 2},
                                                 nonce=a.iface.COMPLETION_REPORT_NONCE_BASE)
        captured = []
        original = a.iface._capture_direct_attempt_result
        a.iface._capture_direct_attempt_result = lambda *args, **kw: captured.append(kw)
        try:
            async def run():
                t0 = time.monotonic()
                ok = await a.iface._send_direct_noack_frame(target, frame, 0, b.prefix, 0, kind="completion_report",
                                                            priority=a.iface.PRIORITY_ANSWER)
                return ok, time.monotonic() - t0, a.iface._direct_exchange_lock.locked()

            ok, took, locked_after = a.run_on_loop(run(), timeout=20.0)
        finally:
            a.iface._capture_direct_attempt_result = original
        self.assertTrue(ok)
        self.assertFalse(locked_after, "the lock is released after the hold")
        self.assertTrue(wait_until(lambda: any(r.get("text") == frame for r in received), 10.0), f"not delivered: {received}")
        got = next(r for r in received if r.get("text") == frame)
        self.assertEqual(got.get("txt_type"), 1, "delivered as CLI_DATA")
        time.sleep(1.0)
        self.assertEqual(acks, [], "a CLI_DATA frame is never ACKed")
        hold = a.iface._noack_frame_hold_s(a.iface._text_frame_on_air_bytes(frame, 0), 0)
        self.assertGreaterEqual(took, hold - 0.05, "the lock was held for the frame's hold")
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].get("kind"), "completion_report")
        self.assertEqual(captured[0].get("ack_timeout_source"), "noack")
        self.assertIsNotNone(captured[0].get("on_air_bytes"))

    def test_completion_answer_uses_the_noack_path_by_default(self):
        a = self.a
        calls = {"noack": 0, "acked": 0}

        async def fake_noack(*args, **kwargs):
            calls["noack"] += 1
            return True

        async def fake_acked(*args, **kwargs):
            calls["acked"] += 1
            return True, True

        # The interface's own contact table is filled by its refresh loop;
        # run one refresh so `_resolve_contact(B)` finds B.
        a.run_on_loop(a.iface._refresh_contacts_and_grant_telemetry(), timeout=20.0)
        self.assertIsNotNone(a.iface._resolve_contact(self.b.prefix))
        saved = (a.iface._send_direct_noack_frame, a.iface._send_direct_frame_and_wait_for_ack, a.iface.direct_report_noack)
        a.iface._send_direct_noack_frame = fake_noack
        a.iface._send_direct_frame_and_wait_for_ack = fake_acked
        try:
            a.run_on_loop(a.iface._send_completion_answer(self.b.prefix, 5, 2, True, held={0, 1},
                                                          version=a.iface.COMPLETION_PROTOCOL_VERSION, nonce=1), timeout=10.0)
            self.assertEqual(calls, {"noack": 1, "acked": 0})
            a.iface.direct_report_noack = False
            a.run_on_loop(a.iface._send_completion_answer(self.b.prefix, 5, 2, True, held={0, 1},
                                                          version=a.iface.COMPLETION_PROTOCOL_VERSION, nonce=1), timeout=10.0)
            self.assertEqual(calls, {"noack": 1, "acked": 1})
        finally:
            a.iface._send_direct_noack_frame, a.iface._send_direct_frame_and_wait_for_ack, a.iface.direct_report_noack = saved


class GapsReportDebounce(SingleNodeCase):
    def _headers(self, pkt_id, total, attempt=0):
        return [self.module._FrameHeader(self.iface.PROTOCOL_VERSION, True, False, pkt_id, i, total, attempt) for i in range(total)]

    def _install(self):
        iface = self.iface
        sent = []
        original = iface._send_completion_report
        iface._send_completion_report = lambda token, header, complete, held, held_s=None: sent.append(
            {"complete": complete, "held": set(held), "held_s": held_s})
        saved = (iface.direct_report_debounce, iface.direct_raw_report_enabled)
        iface.direct_report_debounce, iface.direct_raw_report_enabled = True, True

        def restore(key):
            iface._send_completion_report = original
            iface.direct_report_debounce, iface.direct_raw_report_enabled = saved
            iface._cancel_gaps_report(key)
            iface._reassembly.pop(key, None)
            iface._dedup.pop(key, None) if hasattr(iface, "_dedup") else None
        return sent, restore

    def test_completion_inside_the_hold_sends_only_the_complete_report(self):
        iface = self.iface
        sent, restore = self._install()
        h = self._headers(91, 3)
        key = iface._reassembly_key(h[0], PEER, mode="direct")
        try:
            hold = iface._report_hold_s(10 + iface.RAW_HEADER_SIZE, 0)
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h[0], b"a" * 10, PEER, raw=True, report_requested=False))
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h[1], b"b" * 10, PEER, raw=True, report_requested=True))
            self.assertEqual(sent, [], "the gaps report is held")
            self.assertIn(key, iface._pending_gap_reports)
            time.sleep(min(0.2, hold / 2))
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h[2], b"c" * 10, PEER, raw=True, report_requested=True))
            self.assertTrue(wait_until(lambda: len(sent) == 1, 2.0))
            self.assertTrue(sent[0]["complete"])
            self.assertNotIn(key, iface._pending_gap_reports, "the held gaps report was cancelled")
            time.sleep(hold + 0.3)
            self.assertEqual(len(sent), 1, "no gaps report after the complete one")
        finally:
            restore(key)

    def test_no_completion_sends_the_gaps_report_after_the_hold(self):
        iface = self.iface
        sent, restore = self._install()
        h = self._headers(92, 3)
        key = iface._reassembly_key(h[0], PEER, mode="direct")
        try:
            hold = iface._report_hold_s(10 + iface.RAW_HEADER_SIZE, 0)
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h[0], b"a" * 10, PEER, raw=True, report_requested=False))
            t0 = time.monotonic()
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h[1], b"b" * 10, PEER, raw=True, report_requested=True))
            self.assertEqual(sent, [])
            self.assertTrue(wait_until(lambda: len(sent) == 1, hold + 2.0))
            self.assertGreaterEqual(time.monotonic() - t0, hold - 0.05)
            self.assertFalse(sent[0]["complete"])
            self.assertEqual(sent[0]["held"], {0, 1})
            self.assertAlmostEqual(sent[0]["held_s"], hold, places=3)
        finally:
            restore(key)

    def test_debounce_off_reports_at_once(self):
        iface = self.iface
        sent, restore = self._install()
        iface.direct_report_debounce = False
        h = self._headers(93, 3)
        key = iface._reassembly_key(h[0], PEER, mode="direct")
        try:
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h[0], b"a" * 10, PEER, raw=True, report_requested=False))
            self.on_loop(lambda: iface._handle_direct_multifragment_frame(h[1], b"b" * 10, PEER, raw=True, report_requested=True))
            self.assertEqual(len(sent), 1)
            self.assertFalse(sent[0]["complete"])
        finally:
            restore(key)


if __name__ == "__main__":
    unittest.main()
