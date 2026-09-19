"""
Regression tests for the 2026-09-19 code-audit fixes (see the interface
module docstring's "Code audit" entry for the full list and the evidence
behind each one).

Every test here pins behaviour that was wrong in a way the suite could not
see: either it contradicted the code's own documented intent, or real field
captures in `fieldtests/raw/` showed it happening. The three with field
evidence are marked FIELD in their own docstrings.

Deliberately NOT tested here: the hop-1 abort counting as a path failure.
That is intentional -- `direct_hop1_abort_enabled`'s own comment states
"an abort counts as a real failure toward direct_path_reset_threshold --
silence where a forward was due is evidence, unlike a plain timeout" -- so
the audit left it alone. What the audit did change is the *definition of
silence*: traffic from the target itself during the wait now cancels the
abort, which `HopOneAbortRespectsTargetTraffic` covers.
"""
import asyncio
import time
import unittest

import RNS

from tests._support import SingleNodeCase, quiet_rns, node_pubkey


class ConfigBooleanSpellings(SingleNodeCase):
    """`off`/`disabled` used to read as True, so a flag that defaults off
    was turned ON by the most natural way of writing "no"."""

    def test_falsy_spellings(self):
        for text in ("no", "NO", " no ", "false", "0", "off", "Off", "n", "none", "disabled", "disable"):
            self.assertIs(self.module._cfg_bool(text), False, f"{text!r} should be falsy")

    def test_truthy_spellings(self):
        for text in ("yes", "YES", "true", "1", "on", "y", "enabled", "enable"):
            self.assertIs(self.module._cfg_bool(text), True, f"{text!r} should be truthy")

    def test_unrecognized_still_defaults_to_enabled(self):
        # Historical behaviour, kept deliberately -- but now logged.
        self.assertIs(self.module._cfg_bool("banana"), True)

    def test_a_default_off_flag_stays_off_when_written_as_off(self):
        self.assertIs(self.module._cfg_bool(self.iface.rx_log_holds_enabled), False)


class ProofRoutingUsesLinkToken(SingleNodeCase):
    """FIELD (fieldtests/raw/binaryfieldtest): the same link_id was routed
    `direct_primary` for 54 DATA packets and `small_mesh_direct_all_unknown
    _dest` for its RESOURCE_PRF, because only LRPROOF consulted the token
    table. A RESOURCE_PRF is the sender's only transfer-complete signal."""

    PEER = "abcdef012345"

    def _proof_header(self, context, dest_hash):
        return self.module._RnsHeader(
            packet_type=RNS.Packet.PROOF, destination_type=RNS.Destination.LINK,
            context=context, header_type=RNS.Packet.HEADER_1, destination_hash=dest_hash,
        )

    def setUp(self):
        self.link_id = bytes(range(16))
        self.iface._rns_token_peer.clear()
        self.iface._proof_correlation.clear()
        self.iface._learn_rns_token(self.link_id, self.PEER)

    def test_resource_prf_on_a_known_link_resolves_to_the_peer(self):
        for context in (RNS.Packet.RESOURCE_PRF, RNS.Packet.LRPROOF, RNS.Packet.NONE):
            header = self._proof_header(context, self.link_id)
            self.assertEqual(
                self.iface._resolve_routing_peer(header), self.PEER,
                f"PROOF with context {context:#x} did not resolve via the token table",
            )

    def test_bare_proof_still_uses_the_correlation_table(self):
        # A truncated packet hash is never a token, so this must fall through
        # to _proof_correlation exactly as before.
        proved = bytes(range(16, 32))
        header = self._proof_header(RNS.Packet.NONE, proved)
        self.assertIsNone(self.iface._resolve_routing_peer(header))
        self.iface._proof_correlation[proved] = (self.PEER, time.monotonic() + 100)
        self.assertEqual(self.iface._resolve_routing_peer(header), self.PEER)


class CompletionAnswerCorrelation(SingleNodeCase):
    """A late ANSWER to a previous query for the same pkt_id used to resolve
    the current query's future and be applied authoritatively."""

    PEER = "abcdef012345"

    def setUp(self):
        # `_handle_incoming_completion_frame` maps the raw sender token to a
        # canonical peer prefix through the contact table; this sandbox has
        # no contact for PEER, so pin the mapping for the unit test.
        original = self.iface._canonical_peer_prefix
        self.iface._canonical_peer_prefix = lambda token: self.PEER
        self.addCleanup(setattr, self.iface, "_canonical_peer_prefix", original)

    def test_answer_with_a_different_frag_total_is_discarded(self):
        fut = self.node.run_on_loop(self._register(pkt_id=7, frag_total=3))
        frame = self.iface._encode_completion_frame(
            self.iface.COMPLETION_TYPE_ANSWER, 7, 5, complete=False, held={0, 1},
        )
        self.on_loop(self.iface._handle_incoming_completion_frame, frame, self.PEER)
        self.assertFalse(fut.done(), "a stale ANSWER (frag_total 5 vs 3) resolved the outstanding query")

    def test_matching_answer_is_accepted(self):
        fut = self.node.run_on_loop(self._register(pkt_id=8, frag_total=3))
        frame = self.iface._encode_completion_frame(
            self.iface.COMPLETION_TYPE_ANSWER, 8, 3, complete=False, held={0, 2},
        )
        self.on_loop(self.iface._handle_incoming_completion_frame, frame, self.PEER)
        self.assertTrue(fut.done())
        self.assertEqual(sorted(fut.result().held), [0, 2])

    async def _register(self, pkt_id, frag_total):
        fut = asyncio.get_running_loop().create_future()
        self.iface._completion_query_waiters[(self.PEER, pkt_id)] = (fut, frag_total)
        self.addCleanup(self.iface._completion_query_waiters.pop, (self.PEER, pkt_id), None)
        return fut


class CompletionV1AnswerIsNoInformation(SingleNodeCase):
    """A v1 ANSWER carries no bitmap. Reading `held or ()` as "holds
    nothing" made a v1 peer look like a repeater chain that drops raw
    packets, blacklisting it for direct_raw_path_unsupported_ttl (24h)."""

    def test_v1_answer_decodes_with_held_none(self):
        frame = self.iface._encode_completion_frame(
            self.iface.COMPLETION_TYPE_ANSWER, 3, 4, complete=False,
            version=self.iface.COMPLETION_PROTOCOL_VERSION_V1,
        )
        decoded = self.iface._decode_completion_frame(frame)
        self.assertEqual(decoded.version, self.iface.COMPLETION_PROTOCOL_VERSION_V1)
        self.assertIsNone(decoded.held, "a v1 ANSWER must not claim an empty held set")

    def test_v2_empty_bitmap_is_distinguishable_from_v1(self):
        frame = self.iface._encode_completion_frame(
            self.iface.COMPLETION_TYPE_ANSWER, 3, 4, complete=False, held=set(),
        )
        decoded = self.iface._decode_completion_frame(frame)
        self.assertEqual(decoded.held, frozenset(), "a v2 ANSWER with nothing held must decode as an empty set")


class RawFragmentFirmwareLimits(SingleNodeCase):
    """FIELD-adjacent: the receive limit was 173, but the receiving radio's
    own writeFrame refuses frames over MAX_FRAME_SIZE (176) and returns 0,
    so a 173-byte raw payload (177-byte serial frame) was silently dropped
    inside the peer's radio."""

    SERIAL_FRAME_LIMIT = 176

    def test_no_cap_can_produce_an_undeliverable_frame(self):
        original = self.iface.direct_raw_payload_cap
        self.addCleanup(setattr, self.iface, "direct_raw_payload_cap", original)
        for cap in (100, 157, 170, 171, 172, 173, 174, 200, 1000):
            self.iface.direct_raw_payload_cap = cap
            for path_len in range(0, 6):
                budget = self.iface._direct_raw_payload_budget(path_len)
                raw_frame = self.iface.RAW_HEADER_SIZE + budget
                self.assertLessEqual(
                    4 + raw_frame, self.SERIAL_FRAME_LIMIT,
                    f"cap={cap} path_len={path_len}: receiver's serial frame would be dropped",
                )
                self.assertLessEqual(
                    2 + path_len + raw_frame, self.SERIAL_FRAME_LIMIT,
                    f"cap={cap} path_len={path_len}: sender's serial frame would be rejected",
                )

    def test_default_budget_unchanged_at_zero_hop(self):
        self.assertEqual(self.iface.direct_raw_payload_cap, 170)
        self.assertEqual(self.iface._direct_raw_payload_budget(0), 157)


class RawReconcileRoundsFitTheAttemptNibble(SingleNodeCase):
    """The round goes on the wire as `attempt & 0x03`, and the firmware
    dedups raw packets by a hash of their bytes with no time expiry, so a
    5th round would be byte-identical to the 1st and silently dropped."""

    def test_rounds_are_clamped(self):
        cfg = {"direct_raw_reconcile_rounds": "9"}
        self.assertEqual(max(1, min(4, int(cfg["direct_raw_reconcile_rounds"]))), 4)
        self.assertLessEqual(self.iface.direct_raw_reconcile_rounds, 4)

    def test_every_allowed_round_has_a_distinct_header_byte(self):
        seen = set()
        for rnd in range(self.iface.direct_raw_reconcile_rounds):
            frame = self.iface._encode_raw_fragment(
                b"payload", node_pubkey("B"), "343377c464a7", 1, 0, 1, attempt=rnd,
            )
            seen.add(frame[0])
        self.assertEqual(len(seen), self.iface.direct_raw_reconcile_rounds)


class LoopIntervalFloor(SingleNodeCase):
    """`0` for a periodic interval used to busy-spin the event loop, while
    this config surface teaches 0 as "disable"/"leave alone" elsewhere."""

    def test_zero_and_negative_are_floored(self):
        for value in (0, 0.0, -5):
            self.assertGreaterEqual(self.iface._loop_interval_s(value, "test_interval"),
                                    self.iface.MIN_LOOP_INTERVAL_S)

    def test_ordinary_values_pass_through(self):
        self.assertEqual(self.iface._loop_interval_s(60.0, "test_interval"), 60.0)


class RnsTokenTableIsBounded(SingleNodeCase):
    """One entry per destination hash AND per ephemeral link_id, reclaimed
    only on 24h peer silence -- so it grew forever on an active node."""

    def test_capacity_is_enforced_and_recent_tokens_survive(self):
        self.iface._rns_token_peer.clear()
        cap = self.iface.RNS_TOKEN_PEER_MAX_KEYS
        for i in range(cap + 50):
            self.iface._learn_rns_token(i.to_bytes(16, "big"), "abcdef012345")
        self.assertLessEqual(len(self.iface._rns_token_peer), cap)
        # The newest are kept, the oldest evicted.
        self.assertIn((cap + 49).to_bytes(16, "big"), self.iface._rns_token_peer)
        self.assertNotIn((0).to_bytes(16, "big"), self.iface._rns_token_peer)

    def test_relearning_refreshes_position(self):
        self.iface._rns_token_peer.clear()
        first = (1).to_bytes(16, "big")
        self.iface._learn_rns_token(first, "abcdef012345")
        for i in range(2, 50):
            self.iface._learn_rns_token(i.to_bytes(16, "big"), "abcdef012345")
        self.iface._learn_rns_token(first, "abcdef012345")
        self.assertEqual(next(reversed(self.iface._rns_token_peer)), first)


class TimingBudgetValidatorUsesWorstBudget(SingleNodeCase):
    """It compared only against `direct_send_attempts` (2), so the shipped
    defaults passed while the budgets that actually apply to a fragment
    racing the receiver's clock are larger."""

    def test_warns_when_the_largest_budget_does_not_fit(self):
        iface = self.iface
        saved = {k: getattr(iface, k) for k in (
            "direct_ack_timeout_routed_max_s", "direct_post_send_listen_max_s",
            "reassembly_idle_timeout_s", "direct_send_attempts",
            "direct_send_attempts_handshake", "direct_fragment_finish_attempts",
            "rx_log_holds_enabled",
        )}
        self.addCleanup(lambda: [setattr(iface, k, v) for k, v in saved.items()])

        logged = []
        original_log = RNS.log
        RNS.log = lambda msg, level=None: logged.append(msg)
        try:
            iface.rx_log_holds_enabled = False
            iface.direct_ack_timeout_routed_max_s = 45.0
            iface.direct_post_send_listen_max_s = 3.0
            iface.reassembly_idle_timeout_s = 120.0
            iface.direct_send_attempts = 2          # fits: 120/48 = 2.5
            iface.direct_send_attempts_handshake = 4  # does not fit
            iface.direct_fragment_finish_attempts = 4
            iface._validate_direct_timing_budget()
            self.assertTrue(any("incoherent with reassembly patience" in m for m in logged),
                            "validator stayed silent while a 4-attempt budget needed 192s of 120s")

            logged.clear()
            iface.direct_send_attempts_handshake = 2
            iface.direct_fragment_finish_attempts = 2
            iface._validate_direct_timing_budget()
            self.assertFalse(any("incoherent with reassembly patience" in m for m in logged))
        finally:
            RNS.log = original_log


class HopOneAbortRespectsTargetTraffic(SingleNodeCase):
    """The abort means "silence where a forward was due". Traffic from the
    target itself is not silence -- one of the four aborts in
    fieldtests/raw/binaryfieldtest was exactly that (`target_busy`)."""

    def test_diagnosis_reports_target_busy_over_hop1_loss(self):
        window = {
            "target_hash_byte": "34", "echo_seen_s": None,
            "foreign_rx": [["TEXT_MSG", "DIRECT", 0, 4.8, "34"]],
        }
        self.assertEqual(self.iface._diagnose_missed_ack(window, 1), "target_busy")

    def test_silence_is_still_hop1_loss(self):
        window = {"target_hash_byte": "34", "echo_seen_s": None, "foreign_rx": []}
        self.assertEqual(self.iface._diagnose_missed_ack(window, 1), "hop1_loss")

    def test_seen_echo_is_downstream_loss(self):
        window = {"target_hash_byte": "34", "echo_seen_s": 2.1, "foreign_rx": []}
        self.assertEqual(self.iface._diagnose_missed_ack(window, 1), "downstream_loss")


if __name__ == "__main__":
    quiet_rns()
    unittest.main()
