"""
A bare DIRECT send stops retrying once its reply has been seen (phase 1,
2026-09-20). Field evidence, `fieldtests/raw/Alpha0.1.3/capture_*144922`
(laptop, 2 hops), relative to the capture's first record: LINKREQUEST out
at 2213.5 s; attempt 0 lost its firmware ACK and its 11 s ACK wait ended at
2227.9 s -- 0.5 s AFTER the LRPROOF had arrived (2227.4 s); attempt 1 then
re-transmitted the request at 2235.4 s, eight seconds after the Link was
already proven: one 99-byte frame at 2 hops plus a 3.4 s ACK, and 3.8 s of
lock time the LRRTT queued behind.

Pinned here:

  * `_answered_send_key`: a LINKREQUEST's key is its link_id (cross-checked
    against RNS's own `Link.link_id_from_lr_packet` derivation), a
    bootstrap DATA's key is its truncated hash but ONLY while
    `_pending_dest_proofs` remembers it, anything else has no key;
  * the retry loop (`_send_direct_with_attempts`) makes no further attempt
    once `_signal_send_answered` fired for its key, returns success and
    records success (the far side provably received the frame);
  * an ACK wait already in progress (`_await_direct_ack`) ends the moment
    the reply is signalled, as "answered", with no RTT sample and no
    backoff;
  * both receipt paths signal: a DIRECT LRPROOF/PROOF through
    `_observe_incoming_rns_packet`, a CHANNEL copy through
    `_note_channel_proof`.
"""
import asyncio
import os
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet

PEER = "abcdef012345"
DEST = bytes.fromhex("c5427c7a1878532bdc340ba787698ce6")


def _link_id_the_rns_way(link_request_raw: bytes) -> bytes:
    dst_len = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
    hashable = bytes([link_request_raw[0] & 0x0F]) + link_request_raw[2:]
    data_len = len(link_request_raw) - (2 + dst_len + 1)
    if data_len > RNS.Link.ECPUBSIZE:
        hashable = hashable[:-(data_len - RNS.Link.ECPUBSIZE)]
    return RNS.Identity.truncated_hash(hashable)


class _FakeSent:
    def __init__(self, expected_ack: bytes, suggested_timeout_ms: int):
        self.payload = {"expected_ack": expected_ack, "suggested_timeout": suggested_timeout_ms}


class AnsweredSendKey(SingleNodeCase):
    def test_link_request_key_is_the_link_id(self):
        raw = build_rns_packet("link_request", dest_hash=DEST, payload=os.urandom(RNS.Link.ECPUBSIZE))
        header = self.iface._parse_rns_header(raw)
        self.assertEqual(self.iface._answered_send_key(raw, header), _link_id_the_rns_way(raw))

    def test_plain_single_data_key_is_its_truncated_hash(self):
        """Every plain DATA to a SINGLE destination (review 2026-09-20: not
        only a bootstrap send) -- its PROOF's destination field is exactly
        this value (RNS `ProofDestination`)."""
        iface = self.iface
        raw = build_rns_packet("data", dest_hash=DEST, payload=b"probe")
        header = iface._parse_rns_header(raw)
        self.assertEqual(iface._answered_send_key(raw, header), iface._compute_truncated_hash(raw, header.header_type))

    def test_other_packets_have_no_key(self):
        """A Link packet's proof carries the link_id, not the packet hash,
        so Link DATA / Resource parts have no key; nor do announces, path
        requests or proofs themselves."""
        iface = self.iface
        for kind in ("announce", "path_request", "proof", "link_data", "resource"):
            raw = build_rns_packet(kind, dest_hash=DEST, payload=b"x" * 8)
            self.assertIsNone(iface._answered_send_key(raw, iface._parse_rns_header(raw)), kind)


class RetryLoopStopsWhenAnswered(SingleNodeCase):
    def test_no_retry_after_the_reply_is_signalled(self):
        """Attempt 0 misses its ACK; the reply is signalled while that
        attempt is in flight; attempt 1 must not be transmitted."""
        iface = self.iface
        key = os.urandom(16)
        calls = []
        recorded = []

        async def fake_send(target, frame, attempt=0, **kwargs):
            calls.append(attempt)
            # The reply lands while this attempt's ACK is being awaited.
            iface._signal_send_answered(key, "test")
            return False, True

        original_send = iface._send_direct_frame_and_wait_for_ack
        original_record = iface.record_direct_send_result
        iface._send_direct_frame_and_wait_for_ack = fake_send
        iface.record_direct_send_result = lambda peer, succeeded, waited_full_timeout: recorded.append(succeeded)
        try:
            ok = self.node.run_on_loop(iface._send_direct_with_attempts(
                "ab" * 32, lambda attempt: "Rframe", PEER, priority=iface.PRIORITY_HANDSHAKE, cancel_key=key,
            ), timeout=20.0)
        finally:
            iface._send_direct_frame_and_wait_for_ack = original_send
            iface.record_direct_send_result = original_record
            iface._send_answered_events.pop(key, None)
            iface._send_answered_at.pop(key, None)
        self.assertTrue(ok)
        self.assertEqual(calls, [0], f"attempts transmitted: {calls}")
        self.assertEqual(recorded, [], "answered by nobody in particular (a CHANNEL copy): no path evidence either way")

    def test_reply_from_the_addressed_peer_credits_its_path_and_from_another_peer_does_not(self):
        """A DIRECT-to-all LINKREQUEST: peer A relays the LRPROOF, so B's
        copy is cancelled too -- B's path learned nothing (review
        2026-09-20); A's copy records a success."""
        iface = self.iface
        recorded = []

        async def fake_send(target, frame, attempt=0, **kwargs):
            return False, True

        original_send = iface._send_direct_frame_and_wait_for_ack
        original_record = iface.record_direct_send_result
        iface._send_direct_frame_and_wait_for_ack = fake_send
        iface.record_direct_send_result = lambda peer, succeeded, waited_full_timeout: recorded.append((peer, succeeded))
        try:
            for answered_by, want in (("111111111111", [("111111111111", True)]), ("222222222222", [])):
                key = os.urandom(16)
                recorded.clear()
                iface._signal_send_answered(key, "test", answered_by)
                ok = self.node.run_on_loop(iface._send_direct_with_attempts(
                    "ab" * 32, lambda attempt: "Rframe", "111111111111", priority=iface.PRIORITY_HANDSHAKE, cancel_key=key,
                ), timeout=20.0)
                self.assertTrue(ok)
                self.assertEqual(recorded, want, f"answered by {answered_by}")
                iface._send_answered_events.pop(key, None)
                iface._send_answered_at.pop(key, None)
        finally:
            iface._send_direct_frame_and_wait_for_ack = original_send
            iface.record_direct_send_result = original_record

    def test_unanswered_events_are_swept_and_a_fresh_key_transmits(self):
        iface = self.iface
        key = os.urandom(16)
        self.on_loop(iface._answered_send_event, key)
        self.assertIn(key, iface._send_answered_events)
        iface._send_answered_sweep(time.monotonic() + iface.proof_correlation_ttl_s + 1.0)
        self.assertNotIn(key, iface._send_answered_events, "an event whose send was never answered is swept")
        calls = []

        async def fake_send(target, frame, attempt=0, **kwargs):
            calls.append(attempt)
            return True, True

        original_send = iface._send_direct_frame_and_wait_for_ack
        iface._send_direct_frame_and_wait_for_ack = fake_send
        try:
            self.node.run_on_loop(iface._send_direct_with_attempts(
                "ab" * 32, lambda attempt: "Rframe", PEER, cancel_key=os.urandom(16), record_result=False,
            ), timeout=20.0)
        finally:
            iface._send_direct_frame_and_wait_for_ack = original_send
        self.assertEqual(calls, [0], "a send with a fresh (unanswered) key transmits")

    def test_already_answered_key_skips_attempt_zero(self):
        iface = self.iface
        key = os.urandom(16)
        calls = []

        async def fake_send(target, frame, attempt=0, **kwargs):
            calls.append(attempt)
            return False, True

        original_send = iface._send_direct_frame_and_wait_for_ack
        iface._send_direct_frame_and_wait_for_ack = fake_send
        iface._signal_send_answered(key, "test")   # before the send exists
        try:
            ok = self.node.run_on_loop(iface._send_direct_with_attempts(
                "ab" * 32, lambda attempt: "Rframe", PEER, cancel_key=key, record_result=False,
            ), timeout=20.0)
        finally:
            iface._send_direct_frame_and_wait_for_ack = original_send
            iface._send_answered_events.pop(key, None)
            iface._send_answered_at.pop(key, None)
        self.assertTrue(ok)
        self.assertEqual(calls, [])

    def test_without_a_key_the_budget_is_unchanged(self):
        iface = self.iface
        calls = []

        async def fake_send(target, frame, attempt=0, **kwargs):
            calls.append(attempt)
            return False, True

        original_send = iface._send_direct_frame_and_wait_for_ack
        original_record = iface.record_direct_send_result
        iface._send_direct_frame_and_wait_for_ack = fake_send
        iface.record_direct_send_result = lambda *a, **k: None
        try:
            ok = self.node.run_on_loop(iface._send_direct_with_attempts(
                "ab" * 32, lambda attempt: "Rframe", PEER, priority=iface.PRIORITY_HANDSHAKE,
            ), timeout=20.0)
        finally:
            iface._send_direct_frame_and_wait_for_ack = original_send
            iface.record_direct_send_result = original_record
        self.assertFalse(ok)
        self.assertEqual(calls, list(range(iface.direct_send_attempts_handshake)))


class AckWaitEndsWhenAnswered(SingleNodeCase):
    def test_in_progress_ack_wait_returns_answered(self):
        iface = self.iface
        key = os.urandom(16)
        sent = _FakeSent(os.urandom(4), suggested_timeout_ms=20000)
        iface.direct_ack_min_timeout_s = 8.0

        async def run():
            event = iface._answered_send_event(key)
            loop = asyncio.get_running_loop()
            loop.call_later(0.3, iface._signal_send_answered, key, "test")
            t0 = time.monotonic()
            rx_window = iface._open_rx_log_window("ab" * 32)
            try:
                result = await iface._await_direct_ack(sent, PEER, 2, rx_window, t0, cancel_event=event)
            finally:
                iface._close_rx_log_window(rx_window)
            return result, time.monotonic() - t0

        try:
            (ok, waited_full, timeout_s, source, ack_latency, _abort), took = self.node.run_on_loop(run(), timeout=30.0)
        finally:
            iface._send_answered_events.pop(key, None)
            iface._send_answered_at.pop(key, None)
        self.assertTrue(ok)
        self.assertFalse(waited_full)
        self.assertEqual(source, "answered")
        self.assertIsNone(ack_latency, "no ACK arrived, so no RTT sample")
        self.assertLess(took, 3.0, f"the wait ran {took:.1f}s past the reply")
        self.assertNotIn(PEER, iface._ack_rtt, "an answered wait feeds no estimator and triggers no backoff")


class ReceiptPathsSignal(SingleNodeCase):
    def test_channel_proof_for_plain_data_signals_with_no_peer(self):
        iface = self.iface
        raw = build_rns_packet("data", dest_hash=DEST, payload=b"probe")
        key = iface._answered_send_key(raw, iface._parse_rns_header(raw))
        event = self.on_loop(iface._answered_send_event, key)
        try:
            proof = build_rns_packet("proof", dest_hash=key, payload=b"s" * 64)
            self.on_loop(iface._note_channel_proof, iface._parse_rns_header(proof), "channel_bare")
            self.assertTrue(event.is_set())
            self.assertIsNone(iface._send_answered_by(key), "a CHANNEL proof names no peer -> no path credit")
        finally:
            iface._send_answered_events.pop(key, None)
            iface._send_answered_at.pop(key, None)

    def test_channel_lrproof_signals_the_link_id(self):
        iface = self.iface
        raw = build_rns_packet("link_request", dest_hash=DEST, payload=os.urandom(RNS.Link.ECPUBSIZE))
        link_id = iface._compute_link_id(raw)
        iface._pending_link_requests[link_id] = (DEST, time.monotonic() + 60.0)
        event = self.on_loop(iface._answered_send_event, link_id)
        try:
            lrproof = build_rns_packet("lrproof", dest_hash=link_id, payload=b"p" * 96)
            self.on_loop(iface._note_channel_proof, iface._parse_rns_header(lrproof), "channel_bare")
            self.assertTrue(event.is_set())
            self.assertIn(link_id, iface._send_answered_at)
        finally:
            iface._pending_link_requests.pop(link_id, None)
            iface._send_answered_events.pop(link_id, None)
            iface._send_answered_at.pop(link_id, None)

    def test_direct_proof_for_a_bootstrap_data_signals(self):
        """`_observe_incoming_rns_packet` learns only from a BOUND peer (its
        first guard), so the peer is registered for the duration."""
        iface = self.iface
        raw = build_rns_packet("data", dest_hash=DEST, payload=b"probe")
        header = iface._parse_rns_header(raw)
        iface._remember_bootstrap_send(raw, header)
        key = iface._answered_send_key(raw, header)
        event = self.on_loop(iface._answered_send_event, key)
        iface._peers[PEER] = self.module._PeerRecord(pubkey_prefix=PEER, has_upstream_rns=False, last_seen=time.time())
        try:
            proof = build_rns_packet("proof", dest_hash=key, payload=b"s" * 64)
            self.on_loop(iface._observe_incoming_rns_packet, proof, PEER)
            self.assertTrue(event.is_set())
            self.assertEqual(iface._send_answered_by(key), PEER)
        finally:
            iface._peers.pop(PEER, None)
            iface._pending_dest_proofs.clear()
            iface._send_answered_events.pop(key, None)
            iface._send_answered_at.pop(key, None)
            iface._rns_token_peer.pop(DEST, None)
            iface._unknown_dest_attempts.pop(DEST, None)


if __name__ == "__main__":
    unittest.main()
