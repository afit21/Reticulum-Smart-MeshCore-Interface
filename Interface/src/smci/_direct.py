"""The DIRECT send path: the pre-transmit gate (incoming-quiet defer, duty-cycle throttle, rx-log hold), bare and text-fragmented sends, the attempt loop with its cancellation on a seen reply, the ACK wait (firmware bound, measured timeout, hop-1 abort, pre-emption floor), the post-attempt listen and the radio-quiet window -- all under the single interface-wide priority lock, and the idle-hold helpers that let a Link handshake pre-empt it."""
import asyncio
import random
import time
from typing import Optional

import RNS

from ._common import _RnsHeader, PRIORITY_NORMAL
from ._locks import _PriorityAsyncSemaphore


class _DirectSendMixin:
    def _duty_cycle_exempt(self, priority: int) -> bool:
        """Whether a frame of this priority tier skips the duty-cycle wait
        (see duty_cycle_exempt_handshake). Its airtime is still recorded."""
        return self.duty_cycle_exempt_handshake and priority == self.PRIORITY_HANDSHAKE

    def _relayed_frame(self, hop_count: Optional[int], peer_prefix: Optional[str] = None) -> bool:
        """Whether a DIRECT frame will be relayed by a repeater -- the hop
        class the duty-cycle limiter's two ledgers key on (alpha 0.1.5,
        2026-09-21). `hop_count` is the target's `out_path_len` when the
        caller has it; otherwise the peer's resolved path is consulted; a
        frame whose route is unknown is charged as relayed (the stricter
        budget), never the other way round."""
        if hop_count is not None:
            return hop_count > 0
        if peer_prefix is not None:
            resolved = self._resolved_paths.get(peer_prefix)
            if resolved is not None:
                return resolved.out_path_len > 0
        return True

    async def _throttle_for_duty_cycle(
        self, frame: str, exempt: bool = False, on_air_bytes: Optional[int] = None,
        interrupt: "Optional[asyncio.Event]" = None, relayed: bool = True,
    ) -> "tuple[float, Optional[str]]":
        """User-requested fix (2026-09-16): called at every actual radio-
        keying call site (`_send_channel_fastpath_frame`, one iteration
        of `_send_channel_multifragment_pass`'s per-fragment loop,
        `_send_direct_frame`, `_send_bind_frame`) immediately before the
        `send_msg`/`send_chan_msg` command itself -- estimates `frame`'s
        airtime from `duty_cycle_estimate_bitrate` (see that config
        value's own comment for why this is deliberately NOT the same as
        `bitrate`, confirmed via a real zero-hop field test the day this
        shipped -- reusing `bitrate` massively overestimated real per-
        frame airtime and forced an artificial ~10s wait on every single
        exchange), waits out whatever `_DutyCycleLimiter` says is needed,
        then records the estimate as consumed. Returns `(delay, ledger)`:
        the delay actually applied and the ledger that held the frame
        ("relayed" / "total" / None) -- logged at debug level and available
        to the caller for capture, never gates *whether* the send
        proceeds, only *when* it's allowed to start. `(0.0, None)`
        immediately when `duty_cycle_enabled` is off.

        Alpha 0.1.5 (2026-09-21): `relayed` is the frame's hop class. A
        frame a repeater will relay -- any DIRECT frame with a routed path,
        every CHANNEL flood -- is charged to both ledgers and waits on both
        budgets (`duty_cycle_max_fraction`, 30%, and the total cap); a
        zero-hop DIRECT frame is charged to the total ledger only and waits
        on `duty_cycle_max_fraction_zero_hop` (85%). The owner's rule: what
        touches a repeater stays at 30%, two adjacent radios may use the
        channel between them."""
        if not self.duty_cycle_enabled or self.duty_cycle_estimate_bitrate <= 0:
            return 0.0, None
        estimated_s = self._estimate_tx_airtime_s(frame, on_air_bytes=on_air_bytes)
        budget_s = self.duty_cycle_window_s * (
            self.duty_cycle_max_fraction if relayed else self.duty_cycle_max_fraction_zero_hop)
        if estimated_s > budget_s and not getattr(self, "_duty_cycle_overrun_warned", False):
            # MeshBench finding 1 (2026-09-20): one absurd radio parameter
            # turned into one frame per window with nothing in the log.
            self._duty_cycle_overrun_warned = True
            RNS.log(
                f"{self}: a single {len(frame)}-char frame is estimated at {estimated_s:.0f}s of airtime, "
                f"more than the whole duty-cycle budget ({budget_s:.0f}s per {self.duty_cycle_window_s:.0f}s window) "
                f"-- radio params {self._radio_params!r}; outbound traffic will crawl at one frame per window "
                f"until the radio block is sane.",
                RNS.LOG_WARNING,
            )
        if exempt:
            # Link-maintenance traffic: charged, never delayed.
            self._duty_cycle.record(estimated_s, relayed=relayed)
            self._debug(
                f"duty-cycle: handshake-class {len(frame)}-char frame sent without waiting "
                f"for budget ({estimated_s:.2f}s airtime still charged to the {'relayed and total' if relayed else 'total'} ledger)."
            )
            return 0.0, None
        delay, ledger = await self._duty_cycle.wait_for_budget(estimated_s, interrupt=interrupt, relayed=relayed)
        self._duty_cycle.record(estimated_s, relayed=relayed)
        if delay > 0:
            self._debug(
                f"duty-cycle throttle: waited {delay:.2f}s on the {ledger} ledger before this "
                f"{f'{on_air_bytes}-byte raw' if on_air_bytes is not None else f'{len(frame)}-char'} "
                f"{'relayed' if relayed else 'zero-hop'} frame (estimated {estimated_s:.2f}s airtime, "
                f"{'LoRa model' if self._radio_params is not None else f'duty_cycle_estimate_bitrate={self.duty_cycle_estimate_bitrate}bps'})."
            )
        return delay, ledger

    async def _wait_for_incoming_quiet(self) -> float:
        """User-requested fix (2026-09-16): "if we hear a message come in
        via direct, we wait 3 seconds to hear another before we send
        again... wait for the incoming interface to either stop sending
        or hit its airtime limit." Called at the same radio-keying call
        sites as `_throttle_for_duty_cycle`, immediately before it, so
        this interface never keys the radio into the middle of a peer's
        own DIRECT burst it just heard evidence of (a received fragment
        is strong evidence more are likely still coming, if the peer is
        mid multi-fragment transfer).

        A rolling window, not a single fixed sleep: hearing another
        in-progress DIRECT fragment while already waiting
        (`_last_incoming_direct_at` moving forward, updated by
        `_handle_direct_multifragment_frame` -- see that method's own
        docstring for the 2026-09-18 narrowing) pushes the deadline out
        again, the same "keeps checking, wakes exactly when the deadline
        moves" shape `_DutyCycleLimiter.wait_for_budget` already uses.
        Since this interface has no way to actually observe a peer's own
        airtime budget or duty-cycle state, "or hit its airtime limit" is
        approximated by `incoming_quiet_defer_max_wait_s` -- a bound on
        this node's *own* patience, so a continuously-chatty peer can
        never starve this node's own outgoing traffic indefinitely.
        Returns the delay actually applied (0.0 if none was needed, e.g.
        nothing has been heard yet this session, or the last frame was
        already longer ago than the quiet window).

        Field-diagnosed fix (2026-09-18): originally reset on *any* DIRECT
        frame heard (ACKs, PROOFs, completion-checks, a fragment that
        completed its own bucket), not just "this peer still has more
        fragments of this transfer coming." On a link where both nodes are
        constantly exchanging that other traffic, a genuine 3s lull rarely
        occurred, so nearly every send -- including the fragment retries
        racing the receiver's own `reassembly_idle_timeout_s` -- got pushed
        toward the 15s patience ceiling. Real capture evidence
        (2026-09-18 field test): 0/8 messages completed, fragment gaps
        widening from ~20s to 60-90s apart within one run, then a later
        window with zero incoming fragments and 0% outgoing DIRECT success
        for 16+ minutes straight. See the module docstring's 2026-09-18
        entry for the full root-cause writeup and the paired fix in
        `_pre_transmit_gate`/`_send_direct_frame` (every transmission but a
        message's genuinely-first one skips this wait entirely -- they're
        already racing a clock, not being polite)."""
        if not self.incoming_quiet_defer_enabled or self._last_incoming_direct_at is None:
            return 0.0
        start = time.monotonic()
        hit_patience_ceiling = False
        while True:
            now = time.monotonic()
            total_waited = now - start
            if total_waited >= self.incoming_quiet_defer_max_wait_s:
                hit_patience_ceiling = True
                break
            quiet_for = now - self._last_incoming_direct_at
            if quiet_for >= self.incoming_quiet_window_s:
                break
            remaining_quiet = self.incoming_quiet_window_s - quiet_for
            remaining_patience = self.incoming_quiet_defer_max_wait_s - total_waited
            await asyncio.sleep(max(0.01, min(remaining_quiet, remaining_patience)))
        if total_waited > 0:
            self._debug(
                f"incoming-quiet defer: waited {total_waited:.2f}s for the "
                f"channel to go quiet after last hearing a DIRECT frame"
                + (" (hit incoming_quiet_defer_max_wait_s ceiling)" if hit_patience_ceiling else "")
                + "."
            )
        return total_waited

    async def _pre_transmit_gate(
        self, frame: str, skip_quiet_defer: bool = False, duty_cycle_exempt: bool = False,
        on_air_bytes: Optional[int] = None, interrupt: "Optional[asyncio.Event]" = None,
        relayed: bool = True, telemetry: Optional[dict] = None,
    ) -> "tuple[float, float, float]":
        """Code-review fix: `await self._wait_for_incoming_quiet()` then
        `await self._throttle_for_duty_cycle(frame)`, in that order, used
        to be copy-pasted verbatim at every one of this interface's radio-
        keying call sites (`_send_channel_fastpath_frame`, one iteration of
        `_send_channel_multifragment_pass`'s per-fragment loop,
        `_send_direct_frame`, `_send_bind_frame`) -- both methods' own
        docstrings already said as much ("called at every radio-keying
        call site"), but nothing enforced it structurally: a future fifth
        send path could easily add a `_run_command` call without either
        line and silently reintroduce the airtime/collision problems these
        two mechanisms were field-fix additions for. One call here covers
        both, in the required order, for every current and future send
        site.

        Field-diagnosed fix (2026-09-18, see module docstring): `skip_
        quiet_defer` lets a caller that's already racing a
        `reassembly_idle_timeout_s` clock the receiver has running skip
        `_wait_for_incoming_quiet` entirely. Threaded down from
        `_send_direct_fragmented` as `time_critical`, it covers every
        transmission of a multi-fragment message except the very first
        fragment's first attempt: continuation fragments (the receiver's
        bucket -- and its idle clock -- opened when fragment 0 landed),
        internal retries, and pass-1 re-drives alike. That wait is a
        heuristic collision-avoidance courtesy, reasonable for a fresh send
        but actively counterproductive once a deadline is already running
        on the other end: a late fragment is worse than a slightly-risky
        one. `_throttle_for_duty_cycle` is never skipped -- it enforces
        this node's own real self-imposed airtime cap, not a politeness
        heuristic, and a retry storm is exactly the case that cap exists
        to bound.

        Observability addition (2026-09-18, user-requested field-tuning
        data): returns `(quiet_defer_wait_s, duty_cycle_wait_s,
        medium_hold_wait_s)` -- the delays this call actually applied
        (the third added by step 4's `_wait_for_medium_clear`), instead
        of discarding them.
        `_send_direct_frame` forwards these into an optional `gate_
        telemetry` out-dict so `_capture_direct_attempt_result` can record
        exactly how much of a DIRECT attempt's own latency was this gate
        versus `_direct_exchange_lock` queueing versus the ACK wait itself
        -- the three previously had to be told apart by comparing separate
        `_debug` log lines by hand. The three callers that don't need this
        (`_send_channel_fastpath_frame`, `_send_channel_multifragment_
        pass`, `_send_bind_frame`) are unaffected -- they already discarded
        the old `None` return the same way they discard this tuple."""
        quiet_defer_wait_s = 0.0
        if not skip_quiet_defer:
            quiet_defer_wait_s = await self._wait_for_incoming_quiet()
        duty_cycle_wait_s, duty_cycle_ledger = await self._throttle_for_duty_cycle(
            frame, exempt=duty_cycle_exempt, on_air_bytes=on_air_bytes, interrupt=interrupt, relayed=relayed,
        )
        if telemetry is not None:
            # Alpha 0.1.5: which ledger a wait was charged to ("relayed" /
            # "total" / None), and the frame's hop class, for the capture.
            telemetry["duty_cycle_ledger"] = duty_cycle_ledger
            telemetry["duty_cycle_relayed"] = relayed
        # Step 4 (2026-09-18): last, so it reflects whatever was overheard
        # during the two waits above. A no-op unless rx_log_holds_enabled.
        medium_hold_wait_s = await self._wait_for_medium_clear()
        # Stamped here, not at the send_msg/send_chan_msg call itself: this
        # is the last common point every radio-keying path passes through,
        # and the command is issued immediately after this returns.
        now = time.monotonic()
        self._last_own_tx_at = now
        # Alpha 0.1.5 (2a): the frame is about to be QUEUED in the firmware;
        # the radio is busy for its airtime after whatever it already holds.
        busy_until = self._note_radio_keyed(self._estimate_tx_airtime_s(frame, on_air_bytes=on_air_bytes), now)
        if telemetry is not None:
            telemetry["radio_busy_until"] = busy_until
        return quiet_defer_wait_s, duty_cycle_wait_s, medium_hold_wait_s

    async def _wait_future_or_preempt(self, fut: "asyncio.Future", timeout_s: float,
                                      also_reports: bool = False) -> "tuple[bool, bool]":
        """Await `fut` (shielded: it outlives this wait) for up to
        `timeout_s`, ending early when a Link handshake queues for the
        radio lock (phase 1, 2026-09-20) -- or, with `also_reports` (item
        6, alpha 0.1.5), when a completion REPORT this node owes the far
        sender does: the report wait is radio-free, so the holder loses
        nothing by letting the report out. Returns `(future_done, cut)`;
        the future's own exception is the caller's."""
        if fut.done():
            return True, False
        lock = self._direct_exchange_lock
        events = [lock.preempt_event()] + ([lock.report_event()] if also_reports else [])
        if any(e.is_set() for e in events):
            return False, True
        if timeout_s <= 0:
            return False, False
        loop = asyncio.get_running_loop()
        fut_wait = loop.create_task(asyncio.wait_for(asyncio.shield(fut), timeout=timeout_s))
        waits = [fut_wait] + [loop.create_task(e.wait()) for e in events]
        try:
            done, _pending = await asyncio.wait(set(waits), return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in waits:
                if not t.done():
                    t.cancel()
            # Retrieve the timed-out / cancelled task's exception so asyncio
            # does not log "Task exception was never retrieved".
            for t in waits:
                try:
                    await t
                except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    pass
        if fut.done():
            return True, False
        return False, any(w in done for w in waits[1:])

    async def _idle_hold(self, seconds: float, floor_s: float = 0.0) -> bool:
        """Sleep `seconds` with the radio lock held, but return early
        (True) once a link handshake is queued for the lock and at least
        `floor_s` has passed (phase 1, 2026-09-20). The lock's pre-empt
        event is the signal; False when the whole time elapsed."""
        if seconds <= 0:
            return False
        event = self._direct_exchange_lock.preempt_event()
        started = time.monotonic()
        if floor_s > 0:
            await asyncio.sleep(min(seconds, floor_s))
            if seconds <= floor_s:
                return False
        remaining = seconds - (time.monotonic() - started)
        if remaining <= 0:
            return False
        if event.is_set():
            return True
        try:
            await asyncio.wait_for(event.wait(), timeout=remaining)
            return True
        except asyncio.TimeoutError:
            return False

    def _expired(self, expires_at: Optional[float]) -> bool:
        """outgoing_max_age check -- `expires_at` is a time.monotonic()
        deadline threaded down from `_outgoing_worker`, or None when the
        packet never expires (ANNOUNCE, or the feature is disabled)."""
        return expires_at is not None and time.monotonic() >= expires_at

    # -- Answered sends: stop retrying once the reply is in (2026-09-20) --

    def _answered_send_key(self, data: bytes, header: Optional[_RnsHeader]) -> Optional[bytes]:
        """The key under which the reply to this bare DIRECT packet will be
        signalled, or None when no reply is expected / correlatable: a
        LINKREQUEST's link_id (its LRPROOF carries it as destination), or
        the truncated hash of a bootstrap DATA send remembered in
        `_pending_dest_proofs` (its PROOF carries that). Derived here, in
        the one place bare sends are dispatched (`_send_direct_payload`),
        rather than threaded from the dispatcher: a supplement copy of the
        same LINKREQUEST gets the same key and stops on the same proof."""
        if header is None:
            return None
        if header.packet_type == RNS.Packet.LINKREQUEST:
            return self._compute_link_id(data)
        if header.packet_type == RNS.Packet.PROOF and header.context == RNS.Packet.LRPROOF \
                and header.destination_hash:
            # Alpha 0.1.6 (item 2): an LRPROOF is keyed by its link_id so a
            # newer LINKREQUEST from the same peer can supersede it
            # (`_supersede_link_proofs`); its retries stop like an answered
            # send's, but as a drop, not a success.
            return self.LRPROOF_KEY_PREFIX + bytes(header.destination_hash)
        if (header.packet_type == RNS.Packet.DATA and header.context == RNS.Packet.NONE
                and header.destination_type == RNS.Destination.SINGLE):
            # Review (2026-09-20): every plain DATA to a SINGLE destination,
            # not only a bootstrap send -- its PROOF's destination field is
            # this truncated hash whether the token was known or not (a
            # Link packet's proof carries the link_id instead, so those are
            # left out). In the 2026-09-20 laptop captures 2 of 47 and 4 of
            # 15 bare retries followed the packet's own PROOF.
            return self._compute_truncated_hash(data, header.header_type)
        return None

    def _answered_send_event(self, key: bytes) -> "asyncio.Event":
        """The event an in-flight send with this key waits on; already set
        if the reply was seen before the send got this far (a proof that
        beat the retry loop to the key). Timestamped so an event whose
        send was never answered is swept too (review, 2026-09-20)."""
        entry = self._send_answered_events.get(key)
        if entry is None:
            event = asyncio.Event()
            if key in self._send_answered_at:
                event.set()
            event.superseded = self._send_answered_how.get(key) == "superseded"
            self._send_answered_events[key] = (event, time.monotonic())
            return event
        return entry[0]

    def _send_answered_by(self, key: bytes) -> Optional[str]:
        """Which bound peer delivered the reply (None: a CHANNEL copy, or
        an unbound sender) -- only a reply from the peer the send was
        addressed to is evidence about THAT peer's path."""
        entry = self._send_answered_at.get(key)
        return entry[1] if entry is not None else None

    def _signal_send_answered(self, key: Optional[bytes], how: str, sender_peer_prefix: Optional[str] = None) -> None:
        """Phase 1 (2026-09-20, `fieldtests/raw/Alpha0.1.3/capture_*144922`
        at 2 hops): a LINKREQUEST's attempt 0 lost its firmware ACK, its
        11 s ACK wait ended 0.5 s AFTER the LRPROOF had arrived, and attempt
        1 re-sent the request 8 s after the link was already proven -- a
        99-byte frame plus a 3.4 s ACK at 2 hops, and 3.8 s of lock time
        the LRRTT then waited behind. The three places that pop
        `_pending_link_requests` / `_pending_dest_proofs` call this, and
        the retry loop (`_send_direct_with_attempts`) and the ACK wait
        (`_await_direct_ack`) observe it."""
        if key is None:
            return
        self._send_answered_at[key] = (time.monotonic(), sender_peer_prefix)
        self._send_answered_how[key] = how
        entry = self._send_answered_events.get(key)
        if entry is not None and not entry[0].is_set():
            entry[0].superseded = how == "superseded"
            entry[0].set()
            self._debug(f"send {key.hex()} answered ({how}) while its retry loop was live -- no further attempts.")

    def _send_superseded(self, key: Optional[bytes]) -> bool:
        """Whether the send with this key was superseded (alpha 0.1.6 item 2:
        an LRPROOF whose peer has since sent a newer LINKREQUEST) rather
        than answered."""
        return key is not None and self._send_answered_how.get(key) == "superseded"

    def _supersede_link_proofs(self, peer_prefix: str, new_link_id: Optional[bytes]) -> int:
        """A new LINKREQUEST from `peer_prefix` supersedes every LRPROOF
        still pending for an earlier link of that peer (alpha 0.1.6, item
        2): RNS on the far side has abandoned that link after its client's
        window (MeshChat gives a link 15 s), so the frame is pure airtime.
        The 2026-09-21 session at two hops: six LINKREQUESTs in 2.5 minutes,
        each answered by an LRPROOF of four attempts at 11 s ACK timeouts,
        queued at the handshake tier ahead of everything -- completion
        answers and reports waited up to 125 s behind them. Returns how
        many were superseded."""
        pending = self._pending_link_proofs.get(peer_prefix)
        if not pending:
            return 0
        superseded = 0
        for link_id in list(pending):
            if new_link_id is not None and link_id == new_link_id:
                continue
            key = self.LRPROOF_KEY_PREFIX + link_id
            if not self._send_superseded(key):
                self._signal_send_answered(key, "superseded")
                superseded += 1
        if superseded:
            self._outgoing_dropped_total += superseded
            RNS.log(
                f"{self}: {superseded} queued LRPROOF(s) for {peer_prefix!r} expired -- superseded by its newer "
                f"LINKREQUEST (the far side abandoned that link); not (re)sent.",
                RNS.LOG_DEBUG,
            )
        return superseded

    def _send_answered_sweep(self, now: float) -> None:
        ttl = self.proof_correlation_ttl_s
        for k in [k for k, (t, _by) in self._send_answered_at.items() if now - t > ttl]:
            del self._send_answered_at[k]
        for k in [k for k, (_ev, t) in self._send_answered_events.items() if now - t > ttl]:
            del self._send_answered_events[k]
        for k in [k for k in self._send_answered_how if k not in self._send_answered_at]:
            del self._send_answered_how[k]

    async def _send_direct_packet(
        self, data: bytes, header: Optional[_RnsHeader], peer_prefix: str,
        expires_at: Optional[float] = None,
    ) -> None:
        """Case 3 of docs/routing_decisions.md's summary table: DIRECT is
        the primary transport here, not a supplement -- no CHANNEL
        fallback on an ACK failure, only on the local resolution steps
        below failing outright (no resolved path even after trying
        discovery, or no contact/pubkey to send to at all, which this
        interface's own record being out of sync with the device contact
        table can cause -- see path_discovery_spec.md's persistence
        note)."""
        # Alpha 0.1.6 (item 1): the scoreboard's one decision -- the current
        # path while it delivers, a trial of the best-scoring alternative
        # after it misses, None once every candidate has failed -- inside
        # the one place that decides resolved-versus-discover, never beside
        # it (alpha 0.1.5's shorter-path adoption stood here before).
        resolved = await self._select_path(peer_prefix)
        if resolved is None:
            # Milestone 6: docs/reliability_engine_design.md §8's "next
            # send attempt for this peer goes through discover_path()
            # first" recovery policy -- covers both a freshly
            # stale-path-reset peer and one that simply never had a path
            # resolved yet. Only falls through to broadcast if discovery
            # itself also fails.
            resolved = await self._discover_path_coalesced(peer_prefix)
        if resolved is None:
            # Audit fix (2026-09-19): expires_at was not threaded into these
            # three CHANNEL fallbacks, so a packet already past
            # outgoing_max_age got its full jittered retry-pass budget with
            # expiry checking disabled -- the stale-burst behaviour
            # outgoing_max_age exists to stop.
            #
            # Field fix (2026-09-19 evening session): and the fallback is now
            # RECORDED. It used to return before any capture call, so the
            # packet's only record was the dispatcher's earlier
            # `direct_primary` -- the capture said DIRECT while the packet
            # actually went out as an unencrypted CHANNEL flood. Seven packets
            # in that session were mislabelled this way (found only because
            # the peer logged them arriving as `channel_bare`), which silently
            # undermines any analysis that trusts `routing_decision`.
            self._capture_outgoing(header, data, "direct_unresolved_channel_fallback")
            await self._send_broadcast_packet(data, header, expires_at=expires_at)
            return

        contact = self._resolve_contact(peer_prefix)
        target = contact.get("public_key") if contact is not None else None
        if not target:
            self._capture_outgoing(header, data, "direct_no_contact_channel_fallback")
            await self._send_broadcast_packet(data, header, expires_at=expires_at)
            return

        send_info: dict = {}
        ok = await self._send_direct_payload(
            target, peer_prefix, data, priority=self._priority_tier(header), hop_count=resolved.out_path_len,
            expires_at=expires_at, send_info=send_info,
        )
        if ok is None:
            # Too big even for the fully-fragmented DIRECT budget --
            # _send_direct_payload already declined to attempt anything.
            self._outgoing_dropped_total += 1
            per_fragment_budget = self._direct_multifragment_payload_budget()
            RNS.log(
                f"{self}: dropping outgoing DIRECT packet to {peer_prefix!r} "
                f"({len(data)} bytes) -- exceeds even the fully-fragmented "
                f"DIRECT budget ({per_fragment_budget * 255} bytes across 255 "
                f"fragments at {per_fragment_budget} bytes each); falling "
                f"back to CHANNEL broadcast instead.",
                RNS.LOG_WARNING,
            )
            self._capture_outgoing(header, data, "direct_too_large_channel_fallback")
            await self._send_broadcast_packet(data, header, expires_at=expires_at)
            return

        self._capture_direct_send_result(
            peer_prefix, header.destination_hash if header is not None else None, ok, resolved, len(data),
            method=send_info.get("method"), fallback_from_raw=bool(send_info.get("fallback_from_raw")),
            slot_wait_s=send_info.get("slot_wait_s"),
        )
        if not ok and self._expired(expires_at):
            # Already logged and counted where the expiry was detected.
            return
        if not ok:
            RNS.log(
                f"{self}: DIRECT send to {peer_prefix!r} did not receive a "
                f"real delivery ACK (out_path_len={resolved.out_path_len}) -- "
                f"no CHANNEL fallback for this attempt (routing_decisions.md: "
                f"DIRECT is the primary transport once a path is resolved, "
                f"not a supplement); repeated failures will trigger stale-"
                f"path detection/reset (§8).",
                RNS.LOG_WARNING,
            )

    async def _send_direct_payload(
        self, target: str, peer_prefix: str, data: bytes, priority: int = PRIORITY_NORMAL,
        hop_count: Optional[int] = None, expires_at: Optional[float] = None,
        send_info: Optional[dict] = None,
    ) -> Optional[bool]:
        """Sends `data` DIRECT to `target`, choosing the bare or DIRECT-
        needs-fragmenting shape automatically based on size -- shared by
        the DIRECT-primary path (`_send_direct_packet`) and every DIRECT-
        supplement path (`_send_direct_supplement`) alike, so a
        supplement carrying an oversized packet fragments exactly the
        same way a primary send would, rather than assuming bare
        unconditionally. Returns `None` if `data` is too large even for
        the fully-fragmented DIRECT budget (nothing was attempted),
        otherwise whether every fragment (or the single bare message)
        was actually ACKed."""
        # `send_info` (2026-09-19, capture only): filled with "method" and
        # "fallback_from_raw" so direct_send_result can say how the packet
        # was carried.
        if send_info is None:
            send_info = {}
        fastpath_budget = self._direct_payload_budget()
        if len(data) <= fastpath_budget:
            send_info["method"] = "z85_bare"
            header = self._parse_rns_header(data)
            return await self._send_direct_with_attempts(
                target, lambda attempt, d=data: self._encode_direct_bare(d), peer_prefix,
                priority=priority, hop_count=hop_count, expires_at=expires_at,
                cancel_key=self._answered_send_key(data, header),
                # A plain PROOF expires before every attempt (proof_max_age).
                expire_retries=self._plain_proof(header) and self.proof_max_age_s > 0,
                # A Link handshake pre-empts idle holds of the radio lock.
                preempt=self._is_link_handshake(header),
            )

        # Milestone 6: DIRECT-needs-fragmenting shape
        # (wire_format_design.md) -- rare in practice (constraint one:
        # everything but ANNOUNCE, which never goes DIRECT here,
        # comfortably fits one DIRECT message), but a real packet can
        # still exceed it (e.g. a large Resource-transfer DATA packet).
        per_fragment_budget = self._direct_multifragment_payload_budget()
        max_total_payload = per_fragment_budget * 255  # frag_total is a 1-byte field
        if per_fragment_budget <= 0 or len(data) > max_total_payload:
            return None
        # Alpha 0.1.1 (2026-09-18 night): resume a recently failed send of
        # these exact bytes to this peer under its old pkt_id, so the
        # fragments the receiver still holds aren't sent again. Only when
        # the reconcile step will run afterwards to validate the assumption
        # (never for handshake-priority sends, which don't reconcile).
        # Second audit (2026-09-19 evening session): at most
        # direct_fragmented_max_in_flight fragmented sends per peer at once
        # -- see that option's comment for the three measured costs of the
        # unbounded fan-out. The slot is held for the WHOLE send (bursts and
        # reconcile windows alike), so packets to one peer complete roughly
        # in the order RNS handed them over.
        #
        # Field fix (2026-09-19 night, `fieldtests/raw/Alpha0.1.2/*nighttest*`):
        # OFF by default now (see the option's comment for why the cap did
        # not achieve its purpose), and when enabled the slot is priority-
        # aware (`_PriorityAsyncSemaphore`), announce-class sends have a slot
        # of their own, and a send that cannot get a slot within its budget
        # PROCEEDS with a warning instead of being dropped. The night session
        # dropped four 483-byte Resource parts and two of the laptop's data
        # packets this way (method="slot_expired") -- a drop the receiver
        # then had to notice and re-request through RNS, which is strictly
        # slower than sending late. The slot is a pacing hint, not a gate.
        # M2 (2026-09-20): with window batching on, a raw-eligible part joins
        # its peer's window and the WINDOW takes the in-flight slot
        # (`_run_raw_window`), so every part of an RNS window can join.
        window_batched = self.direct_raw_window_enabled and self._raw_fragments_eligible(peer_prefix, priority)
        slot = None if window_batched else self._fragmented_send_slot(peer_prefix, priority)
        slot_wait_s = 0.0
        slot_held = False
        if slot is not None:
            wait_started = time.monotonic()
            slot_budget_s = max(0.0, min(
                self.outgoing_max_age_s,
                (expires_at - wait_started) if expires_at is not None else self.outgoing_max_age_s,
            ))
            try:
                await asyncio.wait_for(slot.acquire(priority), timeout=slot_budget_s)
                slot_held = True
            except asyncio.TimeoutError:
                RNS.log(
                    f"{self}: fragmented DIRECT send to {peer_prefix!r} ({len(data)} bytes) waited "
                    f"{slot_budget_s:.0f}s for one of {slot.capacity} in-flight slot(s) without getting one "
                    f"({slot.holders()} held, {slot.waiting()} still queued) -- sending anyway rather than "
                    f"dropping it (field fix 2026-09-19 night).",
                    RNS.LOG_WARNING,
                )
            slot_wait_s = time.monotonic() - wait_started
            send_info["slot_wait_s"] = round(slot_wait_s, 3)
            if slot_wait_s > 1.0 and slot_held:
                self._debug(
                    f"fragmented DIRECT send to {peer_prefix!r} waited {slot_wait_s:.1f}s for an "
                    f"in-flight slot (direct_fragmented_max_in_flight={self.direct_fragmented_max_in_flight})."
                )
        try:
            return await self._send_direct_fragmented_payload(
                target, peer_prefix, data, priority=priority, hop_count=hop_count,
                expires_at=expires_at, send_info=send_info,
            )
        finally:
            if slot_held:
                slot.release()

    def _fragmented_send_slot(self, peer_prefix: str, priority: int) -> "Optional[_PriorityAsyncSemaphore]":
        """The per-peer semaphore bounding concurrent fragmented sends, or
        None when the cap is off or the send is handshake class (a Link
        handshake never waits behind bulk transfers). Created lazily so it
        binds to the interface's own event loop.

        Field fix (2026-09-19 night): two semaphores per peer, not one.
        Announce-class sends (`PRIORITY_LOW`: ANNOUNCE, PATH_RESPONSE --
        see `_priority_tier`) share a single-permit slot of their own, so
        a 30-minute LXMF announce reconciling at two hops can never occupy
        a data slot: in the night capture both of the laptop's data slots
        were held by exactly such announces when two data packets were
        dropped. Everything else shares the `direct_fragmented_max_in_
        flight` data slots, served by priority (`_PriorityAsyncSemaphore`)
        rather than arrival order."""
        if self.direct_fragmented_max_in_flight <= 0 or priority == self.PRIORITY_HANDSHAKE:
            return None
        if priority == self.PRIORITY_LOW:
            key, capacity = (peer_prefix, "announce"), 1
        else:
            key, capacity = (peer_prefix, "data"), self.direct_fragmented_max_in_flight
        slot = self._fragmented_send_slots.get(key)
        if slot is None or slot.capacity != capacity:
            slot = _PriorityAsyncSemaphore(capacity)
            self._fragmented_send_slots[key] = slot
        return slot

    async def _send_direct_fragmented_payload(
        self, target: str, peer_prefix: str, data: bytes, priority: int,
        hop_count: Optional[int], expires_at: Optional[float], send_info: dict,
    ) -> bool:
        """The fragmented half of `_send_direct_payload` (raw first, text
        fallback), run inside one of the per-peer in-flight slots."""
        resume_key = (peer_prefix, RNS.Identity.truncated_hash(data))
        can_resume = (
            self.direct_fragment_resume_enabled
            and self.direct_fragment_reconcile_enabled
            and self.direct_completion_check_enabled
            and priority != self.PRIORITY_HANDSHAKE
        )
        resume = self._resumable_sends.get(resume_key) if can_resume else None
        if resume is not None:
            self._resumable_sends.pop(resume_key, None)
            if time.monotonic() >= resume["expires_at"]:
                resume = None
        pkt_id = resume["pkt_id"] if resume is not None else self._next_pkt_id()
        raw_path_hex = None
        if self._raw_fragments_eligible(peer_prefix, priority):
            # Remembered before the raw attempt: the verdict below is about
            # the path raw was actually tried on (audit fix, 2026-09-19).
            _raw_resolved = self._resolved_paths.get(peer_prefix)
            raw_path_hex = (_raw_resolved.out_path_hex or "") if _raw_resolved is not None else None
            raw_result = await self._send_direct_raw_fragmented(
                target, peer_prefix, data, pkt_id, priority=priority, hop_count=hop_count,
                expires_at=expires_at, resume=resume, resume_key=resume_key,
            )
            if raw_result is not None:
                send_info["method"] = "raw"
                return raw_result
            # None: raw declined or fell back mid-way -- fresh pkt_id, text path.
            send_info["fallback_from_raw"] = True
            pkt_id = self._next_pkt_id()
            resume = None
        send_info["method"] = "z85_text"
        text_ok = await self._send_direct_fragmented(
            target, peer_prefix, data, pkt_id, priority=priority, hop_count=hop_count,
            expires_at=expires_at, resume=resume, resume_key=resume_key,
        )
        pending_path = None
        if raw_path_hex is not None:
            if self._raw_fallback_pending.pop((peer_prefix, raw_path_hex), None) is not None:
                pending_path = raw_path_hex
        if pending_path is not None:
            self._note_raw_fallback_outcome(peer_prefix, pending_path, bool(text_ok))
        return text_ok

    async def _send_direct_fragmented(
        self, target: str, peer_prefix: str, payload: bytes, pkt_id: int,
        priority: int = PRIORITY_NORMAL, hop_count: Optional[int] = None,
        expires_at: Optional[float] = None,
        resume: Optional[dict] = None, resume_key=None,
    ) -> bool:
        """docs/reliability_engine_design.md §4's two-pass DIRECT-
        fragmentation structure, fixed by a logical review specifically
        to give "only re-drive the still-missing fragments" a concrete
        trigger: **pass 0** sends every `frag_idx` in order, one at a
        time -- never several back-to-back without waiting, the same
        half-duplex-derived rule as CHANNEL's own spacing, here enforced
        for free by fully awaiting each fragment's own send+ACK(+retry)
        cycle before starting the next -- each with its own bounded
        `direct_send_attempts` budget. **Pass 1** re-attempts only
        whatever never got ACKed in pass 0, in order, each again with a
        fresh attempt budget. If fragments still appear missing after
        both passes, one last-resort `_check_remote_completion` asks the
        receiver directly rather than assuming the data never arrived
        (field-data-driven fix, 2026-09-16 -- see that method's own
        docstring for the phantom-ACK-loss story this closes). Returns
        True if every fragment was eventually ACKed across both passes,
        or if the receiver later confirms it has the complete message
        anyway.

        Step 3 (2026-09-18) reshaped pass 0 for non-handshake sends into
        send-once-then-reconcile -- see `direct_fragment_reconcile_enabled`'s
        own comment and the inline notes below: pass 0 makes
        `direct_fragment_pass0_attempts` unrecorded attempt(s) per fragment,
        one QUERY asks the receiver what it holds, and pass 1 re-drives only
        the confirmed gaps with the normal recorded budget."""
        chunks = self._fragment_direct_payload(payload)
        frag_total = len(chunks)
        # Alpha 0.1.1 resume (see _send_direct_payload): start from what the
        # receiver is believed to hold; the reconcile QUERY below is forced
        # so that belief is checked against the receiver's actual bucket.
        acked, resumed = self._resume_state(resume, frag_total, pkt_id, peer_prefix, raw=False)
        # time.monotonic() of the most recent evidence that the receiver's
        # bucket made progress (an ACK, or a reconcile answer) -- the
        # receiver's idle clock restarts on each fragment it receives.
        last_progress_at = time.monotonic() if resumed else None
        # Most recent fragmented send's identity -- observability only
        # (testscripts/zero_hop_peer_discovery_test.py --verify-query reads
        # it to ask the receiver what it holds for this exact pkt_id).
        self._last_fragmented_pkt_id = pkt_id
        self._last_fragmented_frag_total = frag_total

        # Observability addition (2026-09-18, user-requested): CHANNEL's
        # own multi-fragment sender (_send_channel_multifragment_pass) logs
        # one "starting a send" line up front with frag_total/order/
        # spacing; this method had no equivalent, only the per-attempt line
        # inside _send_direct_frame_and_wait_for_ack and the pass-1 line
        # below -- so "what is this node about to send, and how many
        # fragments" wasn't visible until the first attempt had already
        # happened. DIRECT pass 0 is always sent in strict frag_idx order
        # (no shuffle, unlike CHANNEL -- see the fix note below), so there's
        # no "order" to log here.
        self._debug(
            f"DIRECT fragmented send starting: pkt_id={pkt_id} to {peer_prefix!r} "
            f"frag_total={frag_total} hop_count={hop_count}."
        )

        async def send_one(
            frag_idx: int, time_critical: bool = False, pass_number: int = 0,
            attempts_override: Optional[int] = None, record_result: bool = True,
        ) -> bool:
            return await self._send_direct_with_attempts(
                target,
                lambda attempt, c=chunks[frag_idx], fi=frag_idx, ft=frag_total: (
                    self._encode_channel_multifragment(c, pkt_id, fi, ft, attempt)
                ),
                peer_prefix,
                pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, priority=priority,
                hop_count=hop_count, time_critical=time_critical, pass_number=pass_number,
                attempts_override=attempts_override, record_result=record_result,
                # Page-load fix (2026-09-18 evening): expiry is decided only
                # before the packet's very first transmission. Every later
                # fragment/pass is already committed air.
                expires_at=(expires_at if (frag_idx == 0 and pass_number == 0) else None),
            )

        # Step 3 (2026-09-18, see direct_fragment_reconcile_enabled's own
        # comment): send-once-then-reconcile. Pass 0 sends each fragment
        # once (unrecorded -- a lost ACK isn't a path failure), then ONE
        # QUERY asks the receiver what it holds, and pass 1 re-drives only
        # what it confirms missing. Handshake-class sends keep the old
        # per-fragment budget in pass 0 and skip the reconcile.
        reconcile = (
            self.direct_fragment_reconcile_enabled
            and self.direct_completion_check_enabled
            and priority != self.PRIORITY_HANDSHAKE
        )
        pass0_attempts = max(1, self.direct_fragment_pass0_attempts) if reconcile else None

        # Field-diagnosed fix (2026-09-18, see module docstring): only
        # frag_idx 0 is a genuinely fresh transmission that can afford
        # _pre_transmit_gate's incoming-quiet-defer courtesy. The receiver
        # opens its reassembly bucket -- and starts its
        # reassembly_idle_timeout_s clock -- the moment that first fragment
        # lands (_ReassemblyBucket.last_progress), so every fragment after
        # it is already racing a running deadline, exactly like a re-drive.
        # Pass 0 is sent strictly in frag_idx order here (unlike the CHANNEL
        # path, this one never shuffles), so frag_idx > 0 is a reliable
        # "the receiver's clock is already ticking" test.
        def remember_for_resume() -> None:
            if reconcile:
                self._remember_resumable(resume_key, pkt_id, frag_total, acked, last_progress_at)

        for frag_idx in range(frag_total):
            if acked[frag_idx]:
                continue  # resumed: the receiver already holds this one
            acked[frag_idx] = await send_one(
                frag_idx, time_critical=(frag_idx > 0 or resumed), pass_number=0,
                attempts_override=pass0_attempts, record_result=not reconcile,
            )
            if acked[frag_idx]:
                last_progress_at = time.monotonic()
            if self.detached or not self.online:
                return False

        missing = [i for i in range(frag_total) if not acked[i]]
        if reconcile and any(acked) and not resumed:
            self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
        # Alpha 0.1.1: a resumed send always asks, even when nothing looks
        # missing -- the pre-marked fragments are a belief about the
        # receiver's bucket, and the answer below is the ground truth.
        if reconcile and (missing or resumed):
            answer = await self._query_remote_fragments(
                target, peer_prefix, pkt_id, frag_total, stage="reconcile", priority=priority, hop_count=hop_count,
            )
            if self.detached or not self.online:
                remember_for_resume()
                return False
            held = self._held_from_answer(answer, frag_total) if answer is not None else None
            if answer is not None and held is None:
                # Audit fix (2026-09-19): v1 ANSWER, no bitmap -- no
                # per-fragment information. Leave `acked` alone (pass 1 then
                # re-drives exactly what pass 0 could not confirm) rather
                # than discarding real pass-0 ACKs.
                self._debug(
                    f"DIRECT fragmented send pkt_id={pkt_id} to {peer_prefix!r}: reconcile answered v1 "
                    f"(no bitmap) -- keeping this pass's own ACK results."
                )
            elif answer is not None:
                confirmed = [i for i in missing if i in held]
                lost = [i for i in range(frag_total) if acked[i] and i not in held]
                # Authoritative: the receiver's bucket decides, in both
                # directions (Alpha 0.1.1 -- previously only un-ACKed
                # fragments were updated, so a bucket the receiver had
                # evicted could never be re-driven).
                acked = [i in held for i in range(frag_total)]
                if confirmed:
                    self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
                    last_progress_at = time.monotonic()
                elif held:
                    last_progress_at = time.monotonic()
                missing = [i for i in range(frag_total) if not acked[i]]
                self._debug(
                    f"DIRECT fragmented send pkt_id={pkt_id} to {peer_prefix!r}: reconcile -- "
                    f"receiver holds {sorted(held)}; {len(confirmed)} un-ACKed fragment(s) confirmed "
                    f"delivered, {len(lost)} believed-delivered fragment(s) NOT held, "
                    f"{len(missing)} still missing."
                )
                if answer.complete:
                    RNS.log(
                        f"{self}: DIRECT fragmented send pkt_id={pkt_id} to {peer_prefix!r} -- "
                        f"{len(confirmed)}/{frag_total} fragment(s) got no ACK but the receiver "
                        f"already holds the complete message; skipping the re-drive pass.",
                        RNS.LOG_WARNING,
                    )
                    self._resumable_sends.pop(resume_key, None)
                    return True
        if missing:
            # Alpha 0.1.1: when the receiver provably holds part of this
            # packet, the rest is the whole difference between wasted air
            # and a delivered packet -- spend the larger finishing budget.
            partially_held = any(acked)
            finish_attempts = self.direct_fragment_finish_attempts if (reconcile and partially_held) else None
            self._debug(
                f"DIRECT fragmented send pkt_id={pkt_id} to {peer_prefix!r}: "
                f"pass 1 re-driving {len(missing)}/{frag_total} still-missing "
                f"fragment(s)" + (f" with the finishing budget ({finish_attempts} attempts)." if finish_attempts else ".")
            )
            for frag_idx in missing:
                acked[frag_idx] = await send_one(
                    frag_idx, time_critical=True, pass_number=1, attempts_override=finish_attempts,
                )
                if acked[frag_idx]:
                    last_progress_at = time.monotonic()
                if self.detached or not self.online:
                    remember_for_resume()
                    return False

        if all(acked):
            self._resumable_sends.pop(resume_key, None)
            return True

        if self.direct_completion_check_enabled:
            confirmed = await self._check_remote_completion(
                target, peer_prefix, pkt_id, frag_total, priority=priority, hop_count=hop_count,
            )
            if confirmed:
                RNS.log(
                    f"{self}: DIRECT fragmented send pkt_id={pkt_id} to {peer_prefix!r} -- "
                    f"{sum(1 for a in acked if not a)}/{frag_total} fragment(s) never got a "
                    f"real ACK, but the receiver confirms it has the complete message "
                    f"anyway (lost ACK on the return path, not a delivery failure). "
                    f"Treating as delivered and clearing this peer's recorded failures.",
                    RNS.LOG_WARNING,
                )
                # Undo the false failure signal record_direct_send_result
                # already recorded per-fragment above -- a fresh success
                # unconditionally clears _direct_path_failures for this
                # peer, so a phantom failure here can't leave behind a
                # false trigger for the next unrelated send's stale-path
                # threshold check.
                self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
                self._resumable_sends.pop(resume_key, None)
                return True

        remember_for_resume()
        return False

    async def _send_direct_with_attempts(
        self, target: str, frame_builder, peer_prefix: str,
        pkt_id: Optional[int] = None, frag_idx: Optional[int] = None, frag_total: Optional[int] = None,
        priority: int = PRIORITY_NORMAL, hop_count: Optional[int] = None, time_critical: bool = False,
        pass_number: Optional[int] = None,
        attempts_override: Optional[int] = None, record_result: bool = True,
        expires_at: Optional[float] = None, cancel_key: Optional[bytes] = None,
        expire_retries: bool = False, preempt: bool = False,
    ) -> bool:
        """docs/reliability_engine_design.md §4's "outer multi-attempt
        loop for a single DIRECT message" (`direct_send_attempts`,
        default 2 since 2026-09-16 -- see below) -- applies identically to a bare single-message DIRECT
        send and, per-fragment, to each fragment of a DIRECT-fragmented
        send. `frame_builder(attempt)` re-encodes the frame fresh for
        every attempt (`wire_format_design.md`'s own rule: never resend a
        byte-identical encoded string with the attempt patched in place)
        -- for the bare shape this returns identical bytes every time
        (correct: `_encode_direct_bare` carries no attempt byte of its
        own, per §4's "must NOT reimplement per-attempt content
        variation for a single non-fragmented DIRECT message" -- dedup-
        busting across these retries is instead the firmware's own job,
        driven by the `attempt` integer this method threads through to
        `commands.send_msg`'s own `attempt` parameter); for the
        multi-fragment shape this varies both this interface's own
        header attempt byte AND the same firmware parameter.

        Exactly **one** `record_direct_send_result` call per invocation
        -- success on the first real ACK, or failure once the whole
        attempt budget is exhausted -- never one per individual attempt
        (§4's own fix note: "count a fragment exhausting its own attempt
        budget as one failure... regardless of other fragments'
        outcomes"). A local `send_msg` failure proves nothing about the
        path itself (§8's gate) and is retried within the same budget,
        not counted toward it directly -- only the final exhausted-budget
        outcome is, via whichever `waited_full_timeout` the last attempt
        actually observed.

        **User-requested fix (2026-09-16):** the attempt budget itself is
        priority-dependent -- `direct_send_attempts_handshake` (default 4)
        for a `PRIORITY_HANDSHAKE` exchange, `direct_send_attempts`
        (default 2) for everything else. See `direct_send_attempts_
        handshake`'s own comment for why a failed Link handshake deserves
        more persistence than a failed DATA fragment, not less.

        `time_critical` (2026-09-18, see module docstring): True when this
        whole call is already racing a reassembly clock the receiver has
        running -- a continuation fragment (`frag_idx > 0`) or a pass-1
        re-drive, both decided by `_send_direct_fragmented`. Forwarded to
        `_send_direct_frame_and_wait_for_ack` OR'd with `attempt > 0` (an
        internal retry within this same call, time-critical for the same
        reason), so `_pre_transmit_gate` skips the incoming-quiet-defer
        courtesy wait that only a genuinely fresh send can afford.

        `pass_number` (2026-09-18, user-requested field-tuning data):
        capture-only, forwarded unchanged to `_capture_direct_attempt_
        result` -- see that method's own docstring."""
        # Step 3 (2026-09-18): `attempts_override` lets _send_direct_
        # fragmented's pass 0 send each fragment exactly once before the
        # reconcile query; `record_result=False` keeps those single
        # unconfirmed attempts out of record_direct_send_result's stale-
        # path failure count -- with a budget of 1, a lost *ACK* would
        # otherwise count as a full path failure, and the reconcile step
        # exists precisely because lost ACKs aren't path failures. Pass 1
        # (normal budget, results recorded) still feeds the threshold for
        # fragments the receiver confirmed it never got.
        attempts_budget = (
            attempts_override if attempts_override is not None
            else self.direct_send_attempts_handshake if priority == self.PRIORITY_HANDSHAKE
            else self.direct_send_attempts
        )
        waited_full_timeout = False
        # Phase 1 (2026-09-20): a send whose reply has been seen (an
        # LRPROOF for this LINKREQUEST, a PROOF for this bootstrap DATA)
        # has nothing left to retry for -- see _signal_send_answered.
        cancel_event = self._answered_send_event(cancel_key) if cancel_key is not None else None
        for attempt in range(attempts_budget):
            if self.detached or not self.online:
                return False
            if cancel_event is not None and cancel_event.is_set() and self._send_superseded(cancel_key):
                # Alpha 0.1.6 (item 2): an LRPROOF superseded by the peer's
                # newer LINKREQUEST -- expired, like a stale plain proof.
                self._outgoing_dropped_total += 1
                self._capture_direct_attempt_result(
                    peer_prefix, attempt, False, self._direct_exchange_queue_depth, 0.0, None,
                    pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, hop_count=hop_count,
                    time_critical=time_critical, pass_number=pass_number,
                    ack_timeout_source="superseded",
                )
                return False
            if cancel_event is not None and cancel_event.is_set():
                self._debug(
                    f"DIRECT send to {peer_prefix!r} answered before attempt {attempt} -- "
                    f"{'not sent' if attempt == 0 else 'no retry'}; the far side already replied."
                )
                # One record, `ack_timeout_source="answered"`, so the
                # capture shows the retry that did NOT happen.
                self._capture_direct_attempt_result(
                    peer_prefix, attempt, True, self._direct_exchange_queue_depth, 0.0, None,
                    pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, hop_count=hop_count,
                    time_critical=time_critical, pass_number=pass_number,
                    ack_timeout_source="answered",
                )
                # Path evidence only when THIS peer delivered the reply
                # (review, 2026-09-20): a DIRECT-to-all copy cancelled by a
                # proof relayed through another peer, or a CHANNEL copy,
                # says nothing about this peer's path -- like an expiry,
                # nothing is recorded.
                if record_result and self._send_answered_by(cancel_key) == peer_prefix:
                    self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
                return True
            if (attempt == 0 or expire_retries) and self._expired(expires_at):
                # Field fix (2026-09-18 evening): outgoing_max_age. Not a
                # path failure (nothing was learned about the path), so no
                # record_direct_send_result call; counted as a drop once.
                # Attempt 0 only (page-load fix, same day): a retry is
                # committed air, never expired mid-way -- except a plain
                # PROOF (`expire_retries`, proof_max_age, 2026-09-20): one
                # bare frame, nothing already spent, and past the far
                # side's receipt deadline a retry is pure airtime.
                self._outgoing_dropped_total += 1
                RNS.log(
                    f"{self}: dropping DIRECT send to {peer_prefix!r}"
                    f"{f' (pkt_id={pkt_id} frag_idx={frag_idx}/{frag_total})' if pkt_id is not None else ''}"
                    f" -- packet expired ({'proof_max_age=%.0fs' % self.proof_max_age_s if expire_retries else 'outgoing_max_age=%.0fs' % self.outgoing_max_age_s}) before "
                    f"attempt {attempt} could transmit.",
                    RNS.LOG_WARNING,
                )
                if attempt > 0:
                    self._capture_direct_attempt_result(
                        peer_prefix, attempt, False, self._direct_exchange_queue_depth, 0.0, None,
                        pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, hop_count=hop_count,
                        time_critical=time_critical, pass_number=pass_number, ack_timeout_source="expired",
                    )
                return False
            frame = frame_builder(attempt)
            attempt_info: dict = {}
            try:
                ok, waited_full_timeout = await self._send_direct_frame_and_wait_for_ack(
                    target, frame, attempt, peer_prefix=peer_prefix,
                    pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, priority=priority,
                    hop_count=hop_count, time_critical=(time_critical or attempt > 0),
                    pass_number=pass_number, expires_at=expires_at, cancel_event=cancel_event,
                    expire_retries=expire_retries, attempt_info=attempt_info, preempt=preempt,
                )
            except Exception as exc:
                RNS.log(
                    f"{self}: DIRECT send to {peer_prefix!r} failed locally "
                    f"(attempt {attempt}): {exc}",
                    RNS.LOG_WARNING,
                )
                ok, waited_full_timeout = False, False
            if ok and self._send_superseded(cancel_key):
                # Cut mid-wait by the supersession: the frame went out, no
                # ACK came before the newer LINKREQUEST; not path evidence.
                self._outgoing_dropped_total += 1
                return False
            if ok:
                if record_result and not (
                        cancel_event is not None and cancel_event.is_set()
                        and self._send_answered_by(cancel_key) != peer_prefix):
                    self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True,
                                                   ack_latency_s=attempt_info.get("ack_latency_s"))
                return True
            # No per-attempt delay here anymore -- the post-send listen
            # window (outcome-dependent range, 2026-09-16) fires inside
            # _send_direct_frame_and_wait_for_ack itself, before it releases
            # _direct_exchange_lock, so it already happened before control
            # returned here regardless of this attempt's outcome.
            if self.detached or not self.online:
                return False
            if attempt_info.get("expired"):
                # The attempt above expired while waiting for the lock and
                # was never transmitted (the ack-wait method's own check):
                # nothing to retry. Found while adding proof_max_age
                # (2026-09-20) -- before this, an attempt-0 expiry inside
                # the lock wait fell through to attempt 1, which transmitted
                # the expired packet after all.
                self._outgoing_dropped_total += 1
                return False

        if record_result:
            self.record_direct_send_result(peer_prefix, succeeded=False, waited_full_timeout=waited_full_timeout)
        return False

    async def _wait_for_ack_event(self, ack_filters: dict, timeout_s: float, cancel_event: "Optional[asyncio.Event]"):
        """`wait_for_event(ACK, ...)` raced against `cancel_event` (2026-09-20):
        returns `(ack_event_or_None, answered)`. The library's wait
        unsubscribes in its own `finally`, so cancelling it is clean."""
        if cancel_event is None:
            return await self._mc_ready.wait_for_event(
                self._EventType.ACK, attribute_filters=ack_filters, timeout=timeout_s,
            ), False
        if cancel_event.is_set():
            return None, True
        loop = asyncio.get_running_loop()
        ack_task = loop.create_task(self._mc_ready.wait_for_event(
            self._EventType.ACK, attribute_filters=ack_filters, timeout=timeout_s,
        ))
        cancel_task = loop.create_task(cancel_event.wait())
        try:
            done, _pending = await asyncio.wait({ack_task, cancel_task}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (ack_task, cancel_task):
                if not t.done():
                    t.cancel()
        if ack_task in done:
            return ack_task.result(), False
        # Let the cancelled ACK wait unsubscribe before the caller moves on.
        try:
            await ack_task
        except (asyncio.CancelledError, Exception):
            pass
        return None, True

    def _ack_preempt_floor_s(self, peer_prefix: Optional[str], hop_count: Optional[int]) -> float:
        """How long a best-effort ANSWER/REPORT's ACK wait runs before a
        queued handshake may cut it (phase 1, 2026-09-20): the peer's
        expected ACK time (srtt + rttvar when measured; ACKs at hop 0 were
        median 0.65 s / p90 1.65 s, at hop 2 median 2.7 s / p90 3.5 s in
        the 2026-09-20 session), else 2 s + 1 s per hop. Keying the
        handshake into the peer's ACK would lose both at a repeater."""
        st = self._ack_rtt.get(peer_prefix) if peer_prefix else None
        if st is not None:
            return max(0.5, st["srtt"] + st["rttvar"])
        return 2.0 + 1.0 * max(0, hop_count or 0)

    async def _await_direct_ack(
        self, sent, peer_prefix: Optional[str], hop_count: Optional[int], rx_window: dict, ack_wait_start: float,
        cancel_event: "Optional[asyncio.Event]" = None, preemptible: bool = False,
    ) -> "tuple[bool, bool, Optional[float], str, Optional[float], Optional[float]]":
        """The ACK wait for one transmitted DIRECT frame (refactor,
        2026-09-19: lifted verbatim out of `_send_direct_frame_and_wait_
        for_ack`, which had grown to 250 lines). Derives the timeout
        (firmware bound, then the step-2 measured estimate), arms the
        hop-1 abort, waits, and does the RTT bookkeeping. Returns
        `(ok, waited_full_timeout, ack_timeout_s, ack_timeout_source,
        ack_latency_s, hop1_abort_deadline_s)`."""
        ack_timeout_source = "none"
        ack_latency_s = None
        hop1_abort_deadline_s = None
        payload_dict = sent.payload if isinstance(sent.payload, dict) else {}
        expected_ack = payload_dict.get("expected_ack")
        if not expected_ack:
            ok, waited_full_timeout, ack_timeout_s = True, True, None
        else:
            rx_window["expected_ack"] = expected_ack.hex()
            suggested_timeout_ms = payload_dict.get("suggested_timeout", 10000)
            timeout_s = max((float(suggested_timeout_ms) / 1000.0) * 1.2, self.direct_ack_min_timeout_s)
            # §4's routed-mode ceiling -- this interface's dispatcher
            # never issues a DIRECT send without already believing a
            # resolved path exists, so it's always in the "routed"
            # regime from its own point of view; see
            # _configure_peer_discovery's comment on why the doc's
            # separate flood-mode ceiling has no code path here.
            # Field fix (2026-09-19): a hop-aware ceiling as well as the
            # absolute one -- see `direct_ack_timeout_base_s` for the measured
            # justification (the largest ACK that ever arrived in a
            # 1080-attempt session was 8.15s, while the firmware's own
            # suggestion produced waits up to 28s, and 20% of that session was
            # spent in waits that were never going to be answered while
            # holding the one shared radio lock).
            timeout_s = min(
                timeout_s, self._ack_timeout_cap_s(hop_count), self.direct_ack_timeout_routed_max_s,
            )
            if peer_prefix is not None:
                self._last_firmware_ack_timeout_s[peer_prefix] = timeout_s
            timeout_s, ack_timeout_source = self._adaptive_ack_timeout(peer_prefix, timeout_s)

            # Field fix (2026-09-18 evening): early abort on a
            # dead first hop -- see _hop1_abort_deadline_s. Wait
            # for the ACK only until the deadline; if by then
            # neither the ACK nor the first hop's echo of our
            # frame has been heard, the frame never left this
            # radio's neighbourhood and the rest of the timeout
            # buys nothing. If the echo WAS heard, the frame is
            # in the mesh: keep waiting the remainder as before.
            hop1_abort_deadline_s = self._hop1_abort_deadline_s(peer_prefix, hop_count, timeout_s)
            ack_filters = {"code": expected_ack.hex()}
            if preemptible:
                # Phase 1 (2026-09-20): a completion ANSWER/REPORT is best
                # effort and never retried; once the peer's expected ACK
                # time has passed, a queued Link handshake may take the
                # radio. "preempted": not a miss (no backoff, no listen,
                # no path evidence), not a success.
                floor_s = min(timeout_s, self._ack_preempt_floor_s(peer_prefix, hop_count))
                ack_event, answered = await self._wait_for_ack_event(ack_filters, floor_s, cancel_event)
                if ack_event is None and not answered and timeout_s > floor_s:
                    preempt_event = self._direct_exchange_lock.preempt_event()
                    if preempt_event.is_set():
                        return False, False, timeout_s, "preempted", None, None
                    ack_event, answered = await self._wait_for_ack_event(
                        ack_filters, timeout_s - floor_s, preempt_event,
                    )
                    if answered:
                        return False, False, timeout_s, "preempted", None, None
                ok = ack_event is not None
                if ok:
                    ack_latency_s = time.monotonic() - ack_wait_start
                    self._record_ack_rtt(peer_prefix, ack_latency_s)
                elif ack_timeout_source == "measured":
                    self._backoff_ack_rtt(peer_prefix, "missed ACK under measured timeout")
                return ok, ok or ack_timeout_source != "measured", timeout_s, ack_timeout_source, ack_latency_s, None
            first_wait_s = hop1_abort_deadline_s if hop1_abort_deadline_s is not None else timeout_s
            ack_event, answered = await self._wait_for_ack_event(ack_filters, first_wait_s, cancel_event)
            aborted = False
            if answered:
                # Phase 1 (2026-09-20): the reply this frame exists to elicit
                # arrived while its firmware ACK was still awaited -- the
                # exchange succeeded by any useful definition. Success with
                # no ACK latency (nothing to feed the estimator), no backoff,
                # and `waited_full_timeout` False (no evidence about the path
                # beyond the reply itself, which the receipt path recorded).
                return True, False, timeout_s, "answered", None, hop1_abort_deadline_s
            if ack_event is None and hop1_abort_deadline_s is not None:
                # Audit refinement (2026-09-19, field evidence):
                # the abort's premise -- and the reason
                # `direct_hop1_abort_enabled`'s own comment says
                # it counts as a real path failure, unlike a
                # plain timeout -- is "silence where a forward
                # was due". Traffic from the TARGET itself heard
                # during the wait is not silence: it means the
                # target was transmitting rather than listening,
                # so the path is demonstrably alive and the ACK
                # is merely late. One of the four aborts in
                # fieldtests/raw/binaryfieldtest was exactly
                # this (miss_diagnosis="target_busy"), and
                # aborting there both shortened a wait that
                # would likely have succeeded and charged a
                # failure against direct_path_reset_threshold on
                # a good path. Keep waiting the remainder
                # instead, as when our own echo was heard.
                target_hash = rx_window.get("target_hash_byte")
                target_was_talking = bool(target_hash) and any(
                    len(f) >= 5 and f[4] == target_hash
                    for f in rx_window.get("foreign_rx", ())
                )
                if rx_window["echo_seen_s"] is None and not target_was_talking:
                    aborted = True
                else:
                    if target_was_talking and rx_window["echo_seen_s"] is None:
                        self._debug(
                            f"hop-1 abort deadline reached for {peer_prefix!r} but the target "
                            f"itself was heard transmitting during the wait -- not silence, "
                            f"so waiting out the remaining ACK timeout instead of aborting."
                        )
                    ack_event, answered = await self._wait_for_ack_event(
                        ack_filters, max(0.01, timeout_s - first_wait_s), cancel_event,
                    )
                    if answered:
                        return True, False, timeout_s, "answered", None, hop1_abort_deadline_s
            ok, waited_full_timeout = ack_event is not None, True
            ack_timeout_s = hop1_abort_deadline_s if aborted else timeout_s
            if aborted:
                ack_timeout_source = "hop1_abort"
            if ok:
                ack_latency_s = time.monotonic() - ack_wait_start
                self._record_ack_rtt(peer_prefix, ack_latency_s)
            elif ack_timeout_source == "measured":
                # The measured estimate governed this wait and it
                # missed -- maybe the link slowed, maybe the estimate
                # was tight. 2026-09-19: widen the next measured wait
                # (RTO backoff) rather than discard the estimate and pay
                # the firmware's full timeout at once; see
                # direct_ack_rtt_miss_backoff's own comment.
                self._backoff_ack_rtt(peer_prefix, "missed ACK under measured timeout")
                # Code review (2026-09-18): a miss under a timeout
                # this interface tightened on its own is exactly
                # §8's "cut short by this engine's own ceiling"
                # case -- it proves nothing about the path and
                # must not count toward direct_path_reset_
                # threshold. The next attempt runs on the firmware
                # timeout (just invalidated above); a miss THERE
                # counts, so the pre-step-2 behaviour is really
                # the worst case, as step 2 promised.
                waited_full_timeout = False
        return ok, waited_full_timeout, ack_timeout_s, ack_timeout_source, ack_latency_s, hop1_abort_deadline_s

    def _post_attempt_listen_s(self, ok: bool, miss_diagnosis: Optional[str]) -> float:
        """How long to keep the radio lock after one attempt (refactor,
        2026-09-19: lifted out of `_send_direct_frame_and_wait_for_ack`):
        the small success range after an ACK; the step-4 hold model after
        a miss when `rx_log_holds_enabled`; else the flat miss range.
        See the 2026-09-16 outcome-split entry for why success and miss
        draw from different ranges."""
        if ok:
            listen_min_s, listen_max_s = (
                self.direct_post_send_listen_success_min_s, self.direct_post_send_listen_success_max_s,
            )
            listen_delay_s = random.uniform(listen_min_s, listen_max_s)
        elif self.rx_log_holds_enabled:
            listen_delay_s = self._post_miss_hold_s(miss_diagnosis or "no_info")
        else:
            listen_min_s, listen_max_s = (
                self.direct_post_send_listen_min_s, self.direct_post_send_listen_max_s,
            )
            listen_delay_s = random.uniform(listen_min_s, listen_max_s)
        return listen_delay_s

    async def _send_direct_frame_and_wait_for_ack(
        self, target: str, frame: str, attempt: int = 0,
        peer_prefix: Optional[str] = None,
        pkt_id: Optional[int] = None, frag_idx: Optional[int] = None, frag_total: Optional[int] = None,
        priority: int = PRIORITY_NORMAL,  # see _PriorityAsyncLock's own docstring
        hop_count: Optional[int] = None,  # capture-only, see _capture_direct_attempt_result's docstring
        time_critical: bool = False,  # forwarded to _send_direct_frame -> _pre_transmit_gate, see module docstring 2026-09-18
        pass_number: Optional[int] = None,  # capture-only, see _capture_direct_attempt_result's docstring
        kind: Optional[str] = None,  # capture-only: None for an "R" frame, "completion_answer" for a "Q" ANSWER (2026-09-18 review)
        expires_at: Optional[float] = None,  # outgoing_max_age deadline (2026-09-18 evening), see _expired
        quiet_wait: "Optional[asyncio.Future]" = None,  # field fix 2026-09-19 night, see the quiet-window block below
        quiet_window_s: Optional[float] = None,  # seconds after this frame's own transmit (MSG_SENT) the hold may last
        quiet_info: Optional[dict] = None,  # out-param: "hold_s", "ack_done_at", "answered_at" (see the quiet-window block)
        cancel_event: "Optional[asyncio.Event]" = None,  # set when the reply to this frame has been seen (2026-09-20, _signal_send_answered)
        expire_retries: bool = False,  # a plain PROOF: expires_at applies to every attempt, not only the first (proof_max_age, 2026-09-20)
        attempt_info: Optional[dict] = None,  # out-param: "expired" True when the attempt aged out in the lock wait and never transmitted
        preempt: bool = False,  # a Link handshake: may pre-empt an idle hold of the lock (phase 1, 2026-09-20)
        preemptible: bool = False,  # a best-effort ANSWER/REPORT: its own ACK wait may be cut for a queued handshake
    ) -> "tuple[bool, bool]":
        """Sends one already-encoded DIRECT `frame` string (bare or
        multi-fragment shape) to `target` (a MeshCore pubkey) and waits
        for the real delivery ACK the firmware sends back, correlated by
        the `expected_ack` code the MSG_SENT event itself returns -- the
        `meshcore` library's own per-request correlation mechanism for
        ACK specifically (confirmed directly against the installed
        library's `commands/messaging.py` `send_msg_with_retry`, which
        uses this exact `wait_for_event(EventType.ACK,
        attribute_filters={"code": ...})` pattern; also confirmed against
        `reader.py`'s own ACK-frame parsing, which populates that same
        `code` attribute). This is real per-request correlation, unlike
        the bare-type-only matching invariant #2 warns against for every
        other event type -- so, like `discover_path`'s own PATH_RESPONSE
        wait, this is deliberately NOT routed through `_command_lock`: it
        can legitimately take several seconds, and serializing it would
        stall every other outgoing command (CHANNEL sends, path
        discovery) for that whole wait. (The `send_msg` call itself
        still goes through `_run_command`/`_command_lock` via
        `_send_direct_frame`.)

        **Field-diagnosed fix (2026-09-15, real MeshChat traffic):**
        this whole send-then-wait-for-ACK sequence is now serialized via
        `_direct_exchange_lock`, a *separate* lock from `_command_lock`
        held for the full duration, not just the local radio command.
        Without it, two DIRECT sends spawned as independent background
        tasks (`_send_direct_packet`/`_send_direct_supplement` are both
        fire-and-forget by design -- see their own call sites in
        `_send_outgoing_packet`/`_send_path_request`) could each acquire
        `_command_lock` just long enough to queue their own frame, then
        both sit waiting for their own remote ACK *concurrently* --
        letting this node's radio transmit a second DIRECT frame while
        the first one's ACK is still in flight. That's exactly the
        half-duplex collision this design's own "DIRECT-fragmented send
        sequencing: strictly one fragment in flight at a time" invariant
        (reliability_engine_design.md's implementation-notes table) was
        meant to prevent -- but that invariant was only ever enforced
        *within* one `_send_direct_fragmented` call's own pass-0 loop,
        never *across* concurrently-spawned ones. Real MeshChat usage
        (several messages sent close together, each becoming its own
        multi-fragment DIRECT send) surfaced this directly: six separate
        pkt_ids all needing pass-1 re-drives in the same ~30s window,
        repeated "no real delivery ACK" warnings, and cascading stale-
        path resets on both sides -- the signature of concurrent DIRECT
        exchanges colliding with each other, not of a single slow link.
        Deliberately a single interface-wide lock, not one per peer:
        there is exactly one physical radio, so two DIRECT exchanges to
        *different* peers would collide on air exactly the same way.
        CHANNEL sends are unaffected (no ACK wait, so nothing to
        serialize beyond `_command_lock`'s own brief hold already
        covers), and this doesn't reintroduce the "stall unrelated
        commands" problem `_command_lock` was kept out of this wait for
        in the first place -- it only ever blocks a *second concurrent
        DIRECT send*, which needed to wait its turn on the shared radio
        regardless.

        Pure send-and-wait, no `record_direct_send_result` side effect --
        that's the caller's (`_send_direct_with_attempts`'s) job, exactly
        once per its own whole attempt budget, not once per individual
        attempt here. Returns `(ack_received, waited_full_timeout)`; a
        missing `expected_ack` (no ack correlation available at all) is
        treated as local success with nothing to wait for, matching
        Milestone 5's original behavior for that edge case.

        **User-requested observability addition (2026-09-15, post-alpha-0.1.0
        2-hop field test):** `peer_prefix`/`pkt_id`/`frag_idx`/`frag_total`
        are capture/debug-only (never affect routing) -- they let this
        one attempt's outcome be logged with enough context to reconstruct
        exactly which attempt, of which fragment, of which message,
        succeeded or failed, plus `_direct_exchange_lock`'s own live
        contention (`_direct_exchange_queue_depth`, and how long this
        attempt actually waited for its turn) -- see
        `_capture_direct_attempt_result`'s own docstring."""
        self._direct_exchange_queue_depth += 1
        wait_start = time.monotonic()
        preempted = False
        try:
            async with self._direct_exchange_lock(priority, preempt=preempt):
                lock_wait_s = time.monotonic() - wait_start
                queue_depth_at_acquire = self._direct_exchange_queue_depth
                if quiet_wait is not None and quiet_wait.done():
                    # Phase 1 (2026-09-20): the answer this frame asks for is
                    # already in (a late REPORT resolved the QUERY's future
                    # while the QUERY waited for the lock). Not transmitted;
                    # `_query_remote_fragments` reads the future.
                    self._capture_direct_attempt_result(
                        peer_prefix, attempt, True, self._direct_exchange_queue_depth, time.monotonic() - wait_start, None,
                        pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, hop_count=hop_count,
                        time_critical=time_critical, pass_number=pass_number,
                        ack_timeout_source="answered_before_send", kind=kind,
                    )
                    if quiet_info is not None:
                        quiet_info["answered_at"] = time.monotonic()
                        quiet_info["not_sent"] = True
                    return True, False
                if (attempt == 0 or expire_retries) and self._expired(expires_at):
                    # Field fix (2026-09-18 evening): the lock wait itself
                    # (225s in the drive-home capture) is where a queued
                    # packet most often ages out. Recorded, not transmitted;
                    # the caller's own pre-attempt check logs the drop.
                    if attempt_info is not None:
                        attempt_info["expired"] = True
                    self._capture_direct_attempt_result(
                        peer_prefix, attempt, False, queue_depth_at_acquire, lock_wait_s, None,
                        pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, hop_count=hop_count,
                        time_critical=time_critical, pass_number=pass_number,
                        ack_timeout_source="expired", kind=kind,
                    )
                    return False, False
                # Code-review fix: a local exception raised anywhere in this
                # block (e.g. _send_direct_frame surfacing a firmware ERROR
                # via _run_command) used to propagate straight out of this
                # `async with`, skipping the post-send listen-window below
                # entirely -- letting the very next contender for
                # _direct_exchange_lock (a retry of this same attempt, or a
                # different queued DIRECT exchange) key the radio again with
                # zero quiet time, exactly the back-to-back-transmission
                # problem the listen window exists to prevent everywhere
                # else. Caught here so the listen delay still runs (using
                # the same "something might have collided" range a missed
                # ACK draws from -- a local send failure is at least as
                # uncertain), then re-raised so the caller
                # (_send_direct_with_attempts) still sees and logs it
                # exactly as before.
                send_exc = None
                gate_telemetry: dict = {}
                # Step 2 (2026-09-18): measured ACK latency + what the radio
                # overheard during this attempt -- see _record_ack_rtt/
                # _adaptive_ack_timeout and _open_rx_log_window.
                ack_timeout_source = "none"
                ack_latency_s = None
                send_cmd_latency_s = None
                hop1_abort_deadline_s = None
                ack_done_at = None
                rx_window = self._open_rx_log_window(target)
                try:
                    sent = await self._send_direct_frame(
                        target, frame, attempt, time_critical=time_critical, gate_telemetry=gate_telemetry,
                        duty_cycle_exempt=self._duty_cycle_exempt(priority),
                        hop_count=hop_count, peer_prefix=peer_prefix,
                    )
                    ack_wait_start = time.monotonic()
                    rx_window["tx_at"] = ack_wait_start
                    if self._last_own_tx_at is not None:
                        send_cmd_latency_s = ack_wait_start - self._last_own_tx_at

                    (ok, waited_full_timeout, ack_timeout_s, ack_timeout_source,
                     ack_latency_s, hop1_abort_deadline_s) = await self._await_direct_ack(
                        sent, peer_prefix, hop_count, rx_window, ack_wait_start, cancel_event=cancel_event,
                        preemptible=preemptible,
                    )
                    preempted = ack_timeout_source == "preempted"
                    if ack_timeout_source == "answered" and getattr(cancel_event, "superseded", False):
                        # Alpha 0.1.6 (item 2): cut by a supersession, not a reply.
                        ok, ack_timeout_source = False, "superseded"
                    ack_done_at = time.monotonic()
                except Exception as exc:
                    send_exc = exc
                    ok, waited_full_timeout, ack_timeout_s = False, False, None
                finally:
                    self._close_rx_log_window(rx_window)

                # User-requested fix (2026-09-15, generalized after a
                # second real 2-hop field test, then split by outcome
                # 2026-09-16 -- see direct_post_send_listen_success_min_s's
                # own comment for the real zero-hop field data that
                # motivated the split): every attempt, of every fragment,
                # still listens before this method returns, and still
                # *while holding* _direct_exchange_lock so it's a real
                # quiet window on the shared radio, not just a delay this
                # one caller happens to observe -- but which random range
                # it draws from now depends on whether this attempt was
                # actually ACKed. A missed ACK is a live "something might
                # have collided" signal, independent of this node's own
                # queue depth, so it still draws the full direct_post_
                # send_listen_min_s/max_s (0-5s default) range. A real
                # ACK is itself evidence the channel was clear for this
                # exchange, so it draws the much smaller direct_post_
                # send_listen_success_min_s/max_s (0-0.5s default) range
                # instead -- still genuinely random every time (never
                # skipped to a fixed value, so this can't settle into a
                # lockstep pattern with anything else on the channel),
                # still real spacing before the next contender for the
                # lock (a retry of this fragment, the next fragment, or a
                # different queued message) can go, just not the same
                # "assume something's wrong" cost a clean ACK gives no
                # reason to pay.
                # Step 4 (2026-09-18): the diagnosis is always computed and
                # captured; it only *chooses* the hold when rx_log_holds_
                # enabled. Otherwise the flat ranges above still apply.
                if rx_window["echo_seen_s"] is not None:
                    self._record_echo(peer_prefix, hop_count, rx_window["echo_seen_s"])
                miss_diagnosis = None if ok else self._diagnose_missed_ack(rx_window, hop_count)
                medium_busy_remaining_s = self._medium_busy_remaining_s()
                listen_delay_s = self._post_attempt_listen_s(ok, miss_diagnosis)
                if preempted:
                    # The handshake that cut this wait takes the radio now;
                    # the listen it would have had is theirs.
                    listen_delay_s = 0.0
                elif listen_delay_s > 0:
                    # Phase 1 (2026-09-20): the listen after a MISS yields to
                    # a queued handshake once the rx-log prediction of busy
                    # air (0 unless holds are on) has passed; the short
                    # success listen runs in full.
                    if ok:
                        await asyncio.sleep(listen_delay_s)
                    elif await self._idle_hold(listen_delay_s, floor_s=medium_busy_remaining_s):
                        listen_delay_s = time.monotonic() - (ack_done_at or time.monotonic())

                # Field fix (2026-09-19 night, `fieldtests/raw/Alpha0.1.2/
                # *nighttest*` vs `fieldtests/raw/binaryfieldtest/`): an
                # optional RADIO-QUIET WINDOW, held after the listen delay
                # and still under _direct_exchange_lock, until `quiet_wait`
                # resolves or `quiet_window_s` has elapsed since this frame's
                # MSG_SENT. Only the reconcile
                # QUERY uses it (see _query_remote_fragments): at one hop the
                # answering node is a hidden node, so the querier starting
                # its next raw burst the instant the QUERY is ACKed collides
                # with the ANSWER *at the repeater*, where neither radio's
                # listen-before-talk can see it. The night session lost 24
                # answers at one hop; the querier's own RX log shows 22 of
                # them were never decoded by its radio at all. Answer
                # delivery fell from 85% (e87cca8, which held the lock
                # through the whole answer wait) to 48% once commit 1919074
                # made that wait radio-free. This window is the narrow part
                # of that hold -- the seconds the answer is actually in the
                # chain -- and the rest of the budget is still waited with
                # the radio free, so a node's own ANSWERs cannot queue 50s
                # behind its waits the way the full hold made them.
                # asyncio.shield keeps the caller's future alive when this
                # wait_for times out: the answer may still arrive during the
                # radio-free remainder, and cancelling it here would discard
                # it. The window is measured from THIS frame's own transmit
                # (`ack_wait_start`, the MSG_SENT moment) rather than from
                # when the caller decided to send: under concurrent sends the
                # QUERY can wait 5-10s for this very lock first (the diagnostic
                # sim run showed exactly that), and a deadline fixed before
                # that wait would be spent before the frame ever left. Zero
                # hop is deliberately all but unaffected -- 1.5s from the
                # transmit is about the zero-hop ACK latency itself (1.45s
                # median in the night captures), so the hold there is a few
                # hundred milliseconds at most, and the firmware's own LBT
                # covers that case anyway (zero hop measured 96-100% answer
                # delivery in every build).
                # `quiet_info` (review fix, 2026-09-20) hands the caller what
                # it needs to keep its own accounting honest: the hold is
                # charged against the caller's answer budget (otherwise the
                # window would silently EXTEND the budget the field evidence
                # capped), and an answer that arrives inside the hold still
                # gets a round-trip sample measured from the ACK, as one
                # arriving after it would.
                quiet_hold_s = None
                # Review (2026-09-20): anchored at the ACK, and only after an
                # ACK -- a QUERY whose ACK never came has no answer worth
                # holding the radio for (its answer budget still runs radio-
                # free), and anchoring at the transmit spent the whole window
                # on the ACK's own round trip (see direct_completion_quiet_base_s).
                if (quiet_wait is not None and quiet_window_s is not None and send_exc is None
                        and ok and ack_done_at is not None):
                    if quiet_info is not None:
                        quiet_info["ack_done_at"] = ack_done_at
                    quiet_remaining_s = ack_done_at + quiet_window_s - time.monotonic()
                    if quiet_remaining_s > 0:
                        quiet_started = time.monotonic()
                        # Phase 1 (2026-09-20): a queued Link handshake ends
                        # the hold early; the caller keeps waiting for the
                        # answer with the radio free, as it does after the
                        # window.
                        # Alpha 0.1.6 (item 2): and so does a completion
                        # REPORT this node owes the far sender (the item-6
                        # class), as the window's report wait already did.
                        answered, cut = await self._wait_future_or_preempt(quiet_wait, quiet_remaining_s, also_reports=True)
                        if answered and quiet_info is not None:
                            quiet_info["answered_at"] = time.monotonic()
                        if cut:
                            self._debug(
                                f"quiet window for {peer_prefix!r} cut at {time.monotonic() - quiet_started:.2f}s "
                                f"of {quiet_remaining_s:.2f}s -- a Link handshake or a completion report is waiting for the radio."
                            )
                        quiet_hold_s = time.monotonic() - quiet_started
                    if quiet_info is not None:
                        quiet_info["hold_s"] = quiet_hold_s or 0.0

                self._debug(
                    f"DIRECT attempt={attempt} to {peer_prefix!r} "
                    f"(pkt_id={pkt_id} frag_idx={frag_idx}/{frag_total}): ok={ok} "
                    f"queue_depth={queue_depth_at_acquire} lock_wait={lock_wait_s:.2f}s "
                    f"ack_timeout={ack_timeout_s} ({ack_timeout_source}) hop1_abort_deadline={hop1_abort_deadline_s} "
                    f"ack_latency={ack_latency_s if ack_latency_s is None else round(ack_latency_s, 3)}s "
                    f"listen_delay={listen_delay_s:.2f}s "
                    f"rx_window: echo={rx_window['echo_seen_s']} ack_on_air={rx_window['ack_seen_on_air_s']} "
                    f"path_reply={rx_window['path_reply_seen_s']} foreign={rx_window['foreign_rx_count']} "
                    f"miss_diagnosis={miss_diagnosis} medium_busy_remaining={medium_busy_remaining_s:.2f}s "
                    f"medium_hold_wait={gate_telemetry.get('medium_hold_wait_s')} "
                    f"quiet_hold={quiet_hold_s if quiet_hold_s is None else round(quiet_hold_s, 2)}"
                    + (f" (local send exception: {send_exc})" if send_exc is not None else "") + "."
                )
                self._capture_direct_attempt_result(
                    peer_prefix, attempt, ok, queue_depth_at_acquire, lock_wait_s, ack_timeout_s,
                    pkt_id=pkt_id, frag_idx=frag_idx, frag_total=frag_total, listen_delay_s=listen_delay_s,
                    hop_count=hop_count, time_critical=time_critical, pass_number=pass_number,
                    quiet_defer_wait_s=gate_telemetry.get("quiet_defer_wait_s"),
                    duty_cycle_wait_s=gate_telemetry.get("duty_cycle_wait_s"),
                    duty_cycle_ledger=gate_telemetry.get("duty_cycle_ledger"),
                    ack_timeout_source=ack_timeout_source, ack_latency_s=ack_latency_s,
                    send_cmd_latency_s=send_cmd_latency_s, rx_window=rx_window,
                    medium_hold_wait_s=gate_telemetry.get("medium_hold_wait_s"),
                    miss_diagnosis=miss_diagnosis, medium_busy_remaining_s=medium_busy_remaining_s,
                    kind=kind, hop1_abort_deadline_s=hop1_abort_deadline_s,
                    duty_cycle_exempt=bool(gate_telemetry.get("duty_cycle_exempt", False)),
                    quiet_hold_s=quiet_hold_s,
                    on_air_bytes=(self._text_frame_on_air_bytes(frame, hop_count or 0)
                                  if send_exc is None else None),
                )
                if send_exc is not None:
                    # Listened out the quiet window above first, same as any
                    # other failed attempt; now let the caller
                    # (_send_direct_with_attempts) see and log this exactly
                    # as it did before this fix.
                    raise send_exc
                if attempt_info is not None:
                    attempt_info["ack_latency_s"] = ack_latency_s
                if ok and peer_prefix is not None and rx_window.get("ack_snr") is not None:
                    # Alpha 0.1.6 (item 1): the ACK the radio log matched to
                    # this frame is the last frame received over the path
                    # it went on -- its signal is the candidate's.
                    _r = self._resolved_paths.get(peer_prefix)
                    self._note_path_signal(peer_prefix, _r.out_path_hex if _r is not None else None,
                                           rx_window.get("ack_snr"), rx_window.get("ack_rssi"))
                return ok, waited_full_timeout
        finally:
            self._direct_exchange_queue_depth -= 1

    # -- No-ACK text frames (phase 3 M1, 2026-09-20) ------------------------

    TXT_TYPE_PLAIN = 0
    TXT_TYPE_CLI_DATA = 1

    def _noack_frame_hold_s(self, on_air_bytes: int, hops: int) -> float:
        """How long the radio lock stays held after a no-ACK frame leaves
        (pure function, docs/reconcile_redesign.md): `send_msg` returns
        when the frame is QUEUED (MeshBench finding 2), so at least its own
        airtime; through repeaters the same hop-scaled relay gap a raw
        fragment gets (`_raw_fragment_gap_s`, which includes the airtime);
        at zero hop the airtime plus `direct_raw_zero_hop_gap`."""
        if hops > 0:
            return self._raw_fragment_gap_s(hops, on_air_bytes)
        return self._estimate_tx_airtime_s("", on_air_bytes=on_air_bytes) + max(0.0, self.direct_raw_zero_hop_gap_s)

    async def _send_direct_noack_frame(
        self, target: str, frame: str, attempt: int, peer_prefix: Optional[str], hop_count: int,
        kind: str, priority: int = PRIORITY_NORMAL,
    ) -> bool:
        """One text frame as TXT_TYPE_CLI_DATA: the firmware delivers it
        (CONTACT_MSG_RECV, txt_type 1) and never ACKs it, so the lock is
        held only through the gate, the send command and the frame's
        hold (`_noack_frame_hold_s`), never through an ACK wait. The
        command frame is the one the library's own `send_msg` builds
        (`meshcore/commands/messaging.py`) with the type byte set:
        `[0x02][txt_type][attempt][timestamp:4 LE][dst_prefix:6][text]`
        (`MyMesh::onSerialFrame`, CMD_SEND_TXT_MSG). Returns whether the
        firmware accepted it. Captured as a `direct_attempt_result` of
        `kind` with `ack_timeout_source="noack"`."""
        self._direct_exchange_queue_depth += 1
        wait_start = time.monotonic()
        try:
            # Item 6 (alpha 0.1.5): a completion REPORT queues as the report
            # class, which a raw window this node is sending yields to
            # between two of its parts (`_run_raw_window_rounds`).
            async with self._direct_exchange_lock(priority, report=(kind == "completion_report")):
                lock_wait_s = time.monotonic() - wait_start
                queue_depth_at_acquire = self._direct_exchange_queue_depth
                gate_telemetry: dict = {}
                on_air_bytes = self._text_frame_on_air_bytes(frame, hop_count)
                ok = False
                send_exc = None
                hold_s = 0.0
                try:
                    _q, duty_cycle_wait_s, _m = await self._pre_transmit_gate(
                        frame, skip_quiet_defer=True, duty_cycle_exempt=self._duty_cycle_exempt(priority),
                        on_air_bytes=on_air_bytes, relayed=hop_count > 0, telemetry=gate_telemetry,
                    )
                    gate_telemetry["duty_cycle_exempt"] = self._duty_cycle_exempt(priority)
                    gate_telemetry["duty_cycle_wait_s"] = duty_cycle_wait_s
                    dst = bytes.fromhex(str(target)[:12])
                    data = (
                        bytes([0x02, self.TXT_TYPE_CLI_DATA, attempt & 0xFF])
                        + int(time.time()).to_bytes(4, "little") + dst + frame.encode("utf-8")
                    )
                    await self._run_command(
                        self._mc_ready.commands.send(data, [self._EventType.MSG_SENT, self._EventType.ERROR]),
                        "send_txt_msg(cli_data)", self._EventType.MSG_SENT,
                    )
                    self.txb += len(frame)
                    ok = True
                    hold_s = self._noack_frame_hold_s(on_air_bytes, hop_count)
                    # The frame is on air / in the chain: keep the radio
                    # quiet for its hold, yielding to a Link handshake only
                    # once the frame itself is off the air.
                    airtime_s = self._estimate_tx_airtime_s("", on_air_bytes=on_air_bytes)
                    await self._idle_hold(hold_s, floor_s=airtime_s)
                except Exception as exc:
                    send_exc = exc
                self._debug(
                    f"no-ACK {kind} to {peer_prefix!r}: sent={ok} lock_wait={lock_wait_s:.2f}s "
                    f"hold={hold_s:.2f}s hop={hop_count}" + (f" (local send exception: {send_exc})" if send_exc else "") + "."
                )
                self._capture_direct_attempt_result(
                    peer_prefix, attempt, ok, queue_depth_at_acquire, lock_wait_s, None,
                    hop_count=hop_count, time_critical=True, listen_delay_s=hold_s,
                    ack_timeout_source="noack", kind=kind,
                    duty_cycle_exempt=bool(gate_telemetry.get("duty_cycle_exempt", False)),
                    duty_cycle_wait_s=gate_telemetry.get("duty_cycle_wait_s"),
                    duty_cycle_ledger=gate_telemetry.get("duty_cycle_ledger"),
                    on_air_bytes=on_air_bytes if ok else None,
                )
                return ok
        finally:
            self._direct_exchange_queue_depth -= 1

    async def _send_direct(self, target, payload: bytes):
        """A bare (fits-in-one-message) DIRECT send making exactly one
        attempt. Note this method itself is NOT on the real send path as
        of Milestone 6: `_send_outgoing_packet` drives DIRECT sends
        through `_send_direct_with_attempts`, whose frame_builder callback
        calls `_encode_direct_bare`/`_send_direct_frame_and_wait_for_ack`
        directly rather than going through this wrapper (it needs the
        ACK-correlation and outer-retry behavior those add, which this
        method doesn't have). `_send_direct` has been kept, unwired, since
        Milestone 1 as a minimal bare-encode-and-send primitive with its
        own direct unit tests (see test_smart_meshcore_interface_send_
        receive.py's "currently unwired DIRECT primitive" section);
        `_send_direct_frame`, which it delegates to, IS shared by the
        real send path. Returns the MSG_SENT event so a caller can read
        its `expected_ack`/`suggested_timeout`/`type` fields, raises on
        failure."""
        frame = self._encode_direct_bare(payload)
        return await self._send_direct_frame(target, frame)

    async def _send_direct_frame(
        self, target, frame: str, attempt: int = 0, time_critical: bool = False,
        gate_telemetry: Optional[dict] = None, duty_cycle_exempt: bool = False,
        hop_count: Optional[int] = None, peer_prefix: Optional[str] = None,
    ):
        """Sends one already-encoded DIRECT frame string (bare or
        multi-fragment shape -- this method doesn't care which) via
        `send_msg`, the one place either shape actually reaches the
        radio. `attempt` is forwarded to the `meshcore` library's own
        `send_msg(..., attempt=...)` parameter -- the firmware's own
        per-attempt content-derived ACK/dedup-busting mechanism
        (`wire_format_design.md`'s DIRECT section), meaningful on its own
        for the bare shape (whose own encoding never varies by attempt,
        per design) and redundant-but-harmless alongside this interface's
        own header attempt byte for the multi-fragment shape. Called from
        within `_send_direct_frame_and_wait_for_ack`'s own `_direct_
        exchange_lock` hold, so `_throttle_for_duty_cycle`'s wait (if any)
        correctly blocks that lock for the duration too -- nothing else
        should be transmitting during it either, for the same reason
        nothing else should be transmitting during the post-send listen
        window that same caller already enforces.

        `time_critical` (2026-09-18, see module docstring) is forwarded to
        `_pre_transmit_gate` as `skip_quiet_defer` -- see that method's own
        docstring for why a send that's already racing the receiver's
        reassembly clock shouldn't pay the incoming-quiet-defer cost only a
        genuinely fresh send can afford.

        `gate_telemetry` (2026-09-18, user-requested field-tuning data): if
        given a dict, it's filled in-place with `quiet_defer_wait_s`/
        `duty_cycle_wait_s` from `_pre_transmit_gate`'s return -- an out-
        param rather than widening this method's own return value, since
        only `_send_direct_frame_and_wait_for_ack` (for capture) needs it;
        the other callers (`_send_direct`, `_query_remote_fragments`)
        simply don't pass one and see no change. Filled unconditionally, even if `_run_command`
        below then raises -- the gate already ran and cost real time
        either way, and that's exactly the case a field-tuning analysis
        most wants visible."""
        # Alpha 0.1.5: the frame's hop class for the duty-cycle ledgers --
        # `hop_count` from the caller (the target's out_path_len) or the
        # peer's resolved path; unknown counts as relayed.
        gate_info: dict = {}
        quiet_defer_wait_s, duty_cycle_wait_s, medium_hold_wait_s = await self._pre_transmit_gate(
            frame, skip_quiet_defer=time_critical, duty_cycle_exempt=duty_cycle_exempt,
            relayed=self._relayed_frame(hop_count, peer_prefix), telemetry=gate_info,
        )
        if gate_telemetry is not None:
            gate_telemetry["duty_cycle_exempt"] = duty_cycle_exempt
            gate_telemetry["duty_cycle_ledger"] = gate_info.get("duty_cycle_ledger")
            gate_telemetry["quiet_defer_wait_s"] = quiet_defer_wait_s
            gate_telemetry["duty_cycle_wait_s"] = duty_cycle_wait_s
            gate_telemetry["medium_hold_wait_s"] = medium_hold_wait_s
        result = await self._run_command(
            self._mc_ready.commands.send_msg(target, frame, attempt=attempt),
            "send_msg",
            self._EventType.MSG_SENT,
        )
        self.txb += len(frame)
        return result
