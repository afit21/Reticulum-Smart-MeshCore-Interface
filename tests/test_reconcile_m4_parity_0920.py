"""
Phase 3, M4 (2026-09-20, docs/reconcile_redesign.md): hop-adaptive XOR
parity. From one hop up (`direct_raw_parity_min_hops` 1) every part's burst
carries one parity fragment over its data fragments: RAW_FLAG_PARITY set,
frag_idx = the coverage mask, payload = [last covered fragment's length:1] +
XOR of the covered fragments padded to the longest. A receiver missing
exactly one covered fragment reconstructs it. With ~18 % per-fragment loss
at one hop a 3-fragment part loses exactly one fragment 41 % of the time.

Pinned:
  * `_raw_parity_fragments`: 0 at hop 0, 1 from the configured hop, 0 when
    disabled; `_raw_parity_fits`: a 161-byte budget fits up to three hops
    (9 + 1 + 161 = 171 <= 172 and <= 174 - path_len);
  * encode / decode: mask, last length, XOR; a mask of 0 or beyond
    frag_total is rejected;
  * receiver: any one lost data fragment (first, middle, or the short last
    one) is reconstructed from the parity, whether the parity arrives
    before or after the loss is visible, the packet reaches RNS exactly once
    with the original bytes, and the bitmap reports data fragments only;
  * sender: at one hop a 3-fragment part bursts 3 data + 1 parity (mask
    0b111, flagged as the burst's last fragment); a round-1 re-drive of two
    fragments carries a parity over those two; at zero hop no parity.
"""
import asyncio
import os
import time
import unittest

from tests._support import SingleNodeCase, wait_until
from tests.test_completion_report_one_hop_0920 import _OneHopRawSend, PEER, TARGET, _sink

PKT = 901


class PureAndCodec(SingleNodeCase):
    def test_parity_count_and_fit(self):
        iface = self.iface
        saved = (iface.direct_raw_parity_enabled, iface.direct_raw_parity_min_hops)
        try:
            iface.direct_raw_parity_enabled, iface.direct_raw_parity_min_hops = True, 1
            self.assertEqual(iface._raw_parity_fragments(0), 0)
            self.assertEqual(iface._raw_parity_fragments(1), 1)
            self.assertEqual(iface._raw_parity_fragments(3), 1)
            iface.direct_raw_parity_min_hops = 0
            self.assertEqual(iface._raw_parity_fragments(0), 1)
            iface.direct_raw_parity_enabled = False
            self.assertEqual(iface._raw_parity_fragments(2), 0)
        finally:
            iface.direct_raw_parity_enabled, iface.direct_raw_parity_min_hops = saved
        for path_len in range(4):
            self.assertTrue(iface._raw_parity_fits(161, path_len), path_len)
        self.assertFalse(iface._raw_parity_fits(161, 4))
        self.assertTrue(iface._raw_parity_fits(157, 4))
        bare = self.module.SmartMeshCoreInterface.__new__(self.module.SmartMeshCoreInterface)
        bare._configure_retry({})
        self.assertFalse(bare.direct_raw_parity_enabled, "shipped off pending a field A/B (M4 gate, 2026-09-20)")
        self.assertEqual(bare.direct_raw_parity_min_hops, 1)

    def test_encode_decode(self):
        iface = self.iface
        frags = [(0, bytes(range(0, 50))), (1, bytes(range(50, 100))), (2, bytes(range(100, 120)))]
        frame = iface._encode_raw_parity(frags, "ab" * 32, PEER, PKT, 3, attempt=2, report=True)
        self.assertTrue(iface._raw_fragment_is_parity(frame))
        self.assertTrue(iface._raw_fragment_report_requested(frame))
        header, payload, src, dst = iface._decode_raw_fragment(frame)
        self.assertEqual(header.frag_idx, 0b111)
        self.assertEqual(header.frag_total, 3)
        self.assertEqual(header.attempt, 2)
        self.assertEqual(payload[0], 20, "the highest covered fragment's length")
        self.assertEqual(len(payload), 1 + 50)
        acc = bytearray(50)
        for _i, p in frags:
            for k, b in enumerate(p):
                acc[k] ^= b
        self.assertEqual(payload[1:], bytes(acc))
        self.assertEqual(len(frame), iface.RAW_HEADER_SIZE + 1 + 50)
        bad = bytearray(frame)
        bad[7] = 0
        with self.assertRaises(ValueError):
            iface._decode_raw_fragment(bytes(bad))
        bad[7] = 0b1000
        with self.assertRaises(ValueError):
            iface._decode_raw_fragment(bytes(bad))


class ReceiverReconstructs(SingleNodeCase):
    def _run(self, lost_idx, parity_first=False):
        iface = self.iface
        chunks = [bytes(range(0, 60)), bytes(range(60, 120)), bytes(range(120, 143))]
        original = b"".join(chunks)
        own = iface._own_pubkey_hex
        received = []
        orig_pi = iface.process_incoming
        orig_report = iface._send_completion_report
        reports = []
        iface.process_incoming = lambda data, **kw: received.append(bytes(data))
        iface._send_completion_report = lambda token, header, complete, held, held_s=None: reports.append((complete, set(held)))
        iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=False, last_seen=time.time())
        saved = iface.direct_report_debounce
        iface.direct_report_debounce = False
        key = None
        try:
            frames = [iface._encode_raw_fragment(c, own, PEER, PKT, i, 3, attempt=0, report=(i == 2)) for i, c in enumerate(chunks)]
            parity = iface._encode_raw_parity([(i, c) for i, c in enumerate(chunks)], own, PEER, PKT, 3, attempt=0, report=True)
            order = [parity] if parity_first else []
            order += [f for i, f in enumerate(frames) if i != lost_idx]
            if not parity_first:
                order.append(parity)
            for f in order:
                self.on_loop(iface._on_raw_data_inner, type("E", (), {"payload": {"payload": f.hex()}})())
            key = ("direct", PEER, PKT, 3)
            return received, reports, original, key
        finally:
            iface.process_incoming = orig_pi
            iface._send_completion_report = orig_report
            iface.direct_report_debounce = saved
            iface._peers.pop(PEER, None)

    def _cleanup(self, key):
        self.iface._reassembly.pop(key, None)
        self.iface._dedup.pop(key, None)
        self.iface._recent_raw_pkts.pop(PEER, None)

    def test_each_single_loss_is_repaired(self):
        for lost in (0, 1, 2):
            for parity_first in (False, True):
                received, reports, original, key = self._run(lost, parity_first)
                try:
                    self.assertEqual(received, [original], f"lost {lost}, parity_first={parity_first}: {[len(r) for r in received]}")
                    self.assertTrue(any(c for c, _h in reports), "a complete report went out")
                    self.assertTrue(all(h <= {0, 1, 2} for _c, h in reports), "bitmaps report data fragments only")
                    self.assertNotIn(key, self.iface._reassembly)
                finally:
                    self._cleanup(key)

    def test_two_losses_are_not_repaired_and_report_the_gaps(self):
        iface = self.iface
        chunks = [bytes(range(0, 60)), bytes(range(60, 120)), bytes(range(120, 143))]
        own = iface._own_pubkey_hex
        received, reports = [], []
        orig_pi, orig_report = iface.process_incoming, iface._send_completion_report
        iface.process_incoming = lambda data, **kw: received.append(bytes(data))
        iface._send_completion_report = lambda token, header, complete, held, held_s=None: reports.append((complete, set(held)))
        iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=False, last_seen=time.time())
        saved = iface.direct_report_debounce
        iface.direct_report_debounce = False
        key = ("direct", PEER, PKT, 3)
        try:
            f0 = iface._encode_raw_fragment(chunks[0], own, PEER, PKT, 0, 3, attempt=0)
            parity = iface._encode_raw_parity([(i, c) for i, c in enumerate(chunks)], own, PEER, PKT, 3, attempt=0, report=True)
            for f in (f0, parity):
                self.on_loop(iface._on_raw_data_inner, type("E", (), {"payload": {"payload": f.hex()}})())
            self.assertEqual(received, [])
            self.assertEqual(reports, [(False, {0})], "the gaps report lists the data fragments held")
            self.assertIn(0b111, iface._reassembly[key].parity, "the parity is kept for a later fragment")
            f1 = iface._encode_raw_fragment(chunks[1], own, PEER, PKT, 1, 3, attempt=1)
            self.on_loop(iface._on_raw_data_inner, type("E", (), {"payload": {"payload": f1.hex()}})())
            self.assertEqual(received, [b"".join(chunks)], "once one gap remains the parity repairs it")
        finally:
            iface.process_incoming, iface._send_completion_report = orig_pi, orig_report
            iface.direct_report_debounce = saved
            iface._peers.pop(PEER, None)
            self._cleanup(key)


class SenderAddsParity(_OneHopRawSend):
    def _install_parity(self, on_query):
        iface = self.iface
        frames = []

        def on_fragment(header, flagged, size):
            pass
        sent, restore = self._install(on_fragment, on_query)
        # the fixture turns parity off; this is the parity test
        iface.direct_raw_parity_enabled = True
        iface.direct_raw_parity_min_hops = 1
        iface.direct_report_debounce = False
        original_send = iface._send_raw_fragment

        async def fake_send(path, frame, priority, telemetry=None, interrupt=None):
            header, payload, src, dst = iface._decode_raw_fragment(frame)
            frames.append({"parity": iface._raw_fragment_is_parity(frame), "idx_or_mask": header.frag_idx,
                           "round": header.attempt, "flagged": iface._raw_fragment_report_requested(frame), "size": len(frame)})
            return True
        iface._send_raw_fragment = fake_send
        sink, restore_sink = _sink(iface)

        def restore_all():
            iface._send_raw_fragment = original_send
            restore_sink()
            restore()
        return frames, sink, restore_all

    def test_burst_has_a_parity_and_a_redrive_of_two_gets_its_own(self):
        iface = self.iface
        frames, sink, restore = self._install_parity(lambda info: None)
        try:
            async def reports():
                while len(frames) < 4:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.1)
                self._report(PKT, 3, False, {1}, 0)          # fragments 0 and 2 lost (parity could not help)
                while len(frames) < 7:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.1)
                self._report(PKT, 3, True, {0, 1, 2}, 1)

            async def run():
                asyncio.ensure_future(reports())
                return await iface._send_direct_raw_fragmented(TARGET, PEER, self._payload_for(3), PKT,
                                                               priority=iface.PRIORITY_NORMAL, hop_count=1)
            result = self.node.run_on_loop(run(), timeout=30.0)
        finally:
            restore()
        self.assertIs(result, True)
        round0 = [f for f in frames if f["round"] == 0]
        self.assertEqual([f["parity"] for f in round0], [False, False, False, True], f"three data fragments then one parity: {round0}")
        self.assertEqual(round0[3]["idx_or_mask"], 0b111)
        self.assertEqual([f["flagged"] for f in round0], [False, False, True, True], "the last two frames of the burst are flagged")
        round1 = [f for f in frames if f["round"] == 1]
        self.assertEqual([(f["parity"], f["idx_or_mask"]) for f in round1], [(False, 0), (False, 2), (True, 0b101)],
                         f"the re-drive of two fragments carries a parity over exactly those two: {round1}")
        sent = sink.records("raw_fragment_sent")
        self.assertEqual([r.get("parity_mask") for r in sent if r["round"] == 0], [None, None, None, 0b111])

    def test_no_parity_at_zero_hop_or_when_disabled(self):
        iface, M = self.iface, self.module
        frames, sink, restore = self._install_parity(lambda info: None)
        try:
            iface._resolved_paths[PEER] = M._ResolvedPath("", 0, 1, time.monotonic())

            async def report():
                while len(frames) < 3:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.05)
                self._report(PKT, 3, True, {0, 1, 2}, 0)

            async def run():
                asyncio.ensure_future(report())
                return await iface._send_direct_raw_fragmented(TARGET, PEER, self._payload_for(3), PKT,
                                                               priority=iface.PRIORITY_NORMAL, hop_count=0)
            self.assertIs(self.node.run_on_loop(run(), timeout=30.0), True)
            self.assertEqual([f["parity"] for f in frames], [False, False, False], "no parity at zero hop")
        finally:
            restore()


if __name__ == "__main__":
    unittest.main()
