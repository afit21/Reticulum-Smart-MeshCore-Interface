"""Path discovery and per-peer path state: the MeshCore contact refresh and telemetry grant, discover_path with its coalescing and backoff, the stale-path detector (record_direct_send_result, _reset_stale_path -- the one place a cached path is dropped), and the per-peer estimators a path change resets: ACK RTT (Jacobson/Karels, Karn backoff), echo timing for the hop-1 abort, the QUERY round trip."""
import asyncio
import collections
import time
from typing import Optional

import RNS

from ._common import _PathBoard, _PathCandidate, _ResolvedPath


# Alpha 0.1.6 (item 1, fourth cut): a candidate whose weighted delivery rate
# is at least PATH_HEALTHY_RATE stays eligible (a trial target and, alone,
# the path still used) until PATH_EXHAUST_MISSES consecutive missed sends;
# below the rate, `path_switch_after_misses` misses exhaust it as before.
# Alpha 0.1.9 second pass (item 2): PATH_EXHAUST_MISSES is also the DEAD
# mark -- a candidate with that many consecutive missed attempts stays
# ineligible, cooldown or not, until fresh external evidence arrives for it
# -- and the current path's patience no longer needs 0.5: it is kept while
# its measured rate beats every eligible alternative (`_choose_path`).
PATH_HEALTHY_RATE = 0.5
# Alpha 0.1.9 (item 4): `consecutive_misses` counts ATTEMPTS, not sends, so
# both thresholds are doubled -- a missed send is exactly
# `direct_send_attempts` (2) consecutive missed attempts, so 8 attempts is
# the 4 missed sends this was before, and `path_switch_after_misses` 4 is
# the 2 it was. See `_note_path_attempt_result` for why the unit changed.
PATH_EXHAUST_MISSES = 8
# The only two missed-attempt reasons that are evidence about the PATH.
# Everything else in `ack_timeout_source` is this node's own decision or a
# reply that arrived another way, and must not kill a path: "measured" (the
# engine's own tightened ceiling, which already sets waited_full_timeout
# False), "report_window" (alpha 0.1.8 item 2's deliberately short local
# ceiling on a report/answer -- 6 of the 21 failed attempts in the desktop's
# 2026-09-23 11:20-11:45 window were these), "preempted", "superseded",
# "answered", "answered_before_send", "expired" and "noack" (a no-ACK frame
# has no outcome at all). "hop1_abort" IS included deliberately: its premise
# is silence where a forward was due, which is exactly a path failure.
PATH_ATTEMPT_MISS_SOURCES = ("firmware", "hop1_abort")


class _PathDiscoveryMixin:
    def _rtt_sample(self, table: dict, peer_prefix: Optional[str], rtt_s: float, keep_last: bool = False) -> None:
        """One Jacobson/Karels update (srtt alpha 1/8, rttvar beta 1/4;
        the first sample seeds srtt directly and rttvar at half of it, as
        RFC 6298 does) into `table[peer_prefix]` -- shared by the ACK and
        QUERY round-trip estimators (refactor, 2026-09-19)."""
        if peer_prefix is None or rtt_s <= 0:
            return
        st = table.get(peer_prefix)
        if st is None:
            st = {"srtt": rtt_s, "rttvar": rtt_s / 2.0, "samples": 1}
            if keep_last:
                st["last_rtt"] = rtt_s
            table[peer_prefix] = st
            return
        err = rtt_s - st["srtt"]
        st["rttvar"] = 0.75 * st["rttvar"] + 0.25 * abs(err)
        st["srtt"] = st["srtt"] + 0.125 * err
        st["samples"] += 1
        if keep_last:
            st["last_rtt"] = rtt_s

    def _clear_peer_path_stats(self, peer_prefix: str, reason: str = "") -> None:
        """Everything measured about one peer's CURRENT path (refactor,
        2026-09-19 -- one list instead of two hand-maintained copies in
        `_invalidate_ack_rtt` and `_forget_peer_state`): RTT snapshot, echo
        timings, the firmware's last ACK bound, the QUERY round trip, and
        the raw pause/pending verdict. A new path is a new repeater chain.

        Logged (second audit, 2026-09-19 evening): this clear is what
        disarms the hop-1 abort until fresh echo samples exist, and it was
        silent -- the one question that session's terminal logs could not
        answer directly."""
        echo_samples = len(self._echo_stats.get(peer_prefix) or ())
        self._debug(
            f"clearing measured path stats for {peer_prefix!r} ({reason or 'path change'}): "
            f"{echo_samples} echo sample(s) dropped -- hop-1 abort falls back to the session pool "
            f"({len(self._echo_stats_all)} sample(s)) or direct_hop1_abort_default until re-measured."
        )
        self._ack_rtt_snapshot.pop(peer_prefix, None)
        self._echo_stats.pop(peer_prefix, None)
        self._last_firmware_ack_timeout_s.pop(peer_prefix, None)
        self._query_rtt.pop(peer_prefix, None)
        self._report_rtt.pop(peer_prefix, None)
        self._raw_disabled_until.pop(peer_prefix, None)
        self._raw_incomplete_strikes.pop(peer_prefix, None)
        self._direct_path_recent_success.pop(peer_prefix, None)
        for k in [k for k in self._raw_fallback_pending if k[0] == peer_prefix]:
            self._raw_fallback_pending.pop(k, None)

    def _recent_path_successes(self, pubkey_prefix: str) -> int:
        """How many DIRECT sends to this peer succeeded within
        `direct_path_healthy_window_s` (field fix, 2026-09-19)."""
        stamps = self._direct_path_recent_success.get(pubkey_prefix)
        if not stamps:
            return 0
        cutoff = time.monotonic() - self.direct_path_healthy_window_s
        fresh = [t for t in stamps if t >= cutoff]
        if len(fresh) != len(stamps):
            self._direct_path_recent_success[pubkey_prefix] = fresh
        return len(fresh)

    def _record_query_rtt(self, peer_prefix: Optional[str], rtt_s: float) -> None:
        """One measured QUERY -> ANSWER round trip (first raw field test,
        2026-09-18 night). Same estimator shape as `_record_ack_rtt`."""
        self._rtt_sample(self._query_rtt, peer_prefix, rtt_s)

    # -- Measured ACK RTT (step 2, 2026-09-18 -- see module docstring) ----

    def _record_ack_rtt(self, peer_prefix: Optional[str], rtt_s: float) -> None:
        """One real ACK latency sample for `peer_prefix`, folded into the
        classic Jacobson/Karels estimator (srtt alpha 1/8, rttvar beta
        1/4). The first sample seeds srtt directly and rttvar at half of
        it, exactly as RFC 6298 does -- a deliberately generous initial
        spread so the timeout doesn't collapse onto one lucky sample."""
        self._rtt_sample(self._ack_rtt, peer_prefix, rtt_s, keep_last=True)
        st = self._ack_rtt.get(peer_prefix)
        if st is not None:
            st["backoff"] = 1.0  # a real ACK ends any miss backoff

    def _backoff_ack_rtt(self, peer_prefix: Optional[str], reason: str) -> None:
        """A miss under a measured timeout: keep the estimate, widen the
        next wait by `direct_ack_rtt_miss_backoff` (compounding per
        consecutive miss; `_adaptive_ack_timeout` still caps at the
        firmware value). Falls back to the old Karn discard when the
        factor is <= 1."""
        if peer_prefix is None:
            return
        if self.direct_ack_rtt_miss_backoff <= 1.0:
            self._invalidate_ack_rtt(peer_prefix, reason, keep_for_query=True)
            return
        st = self._ack_rtt.get(peer_prefix)
        if st is None:
            return
        st["backoff"] = st.get("backoff", 1.0) * self.direct_ack_rtt_miss_backoff
        self._ack_rtt_snapshot[peer_prefix] = st
        self._debug(
            f"ACK RTT estimate for {peer_prefix!r} kept ({reason}); next measured timeout "
            f"x{st['backoff']:.0f}, capped at the firmware value."
        )

    def _invalidate_ack_rtt(self, peer_prefix: Optional[str], reason: str, keep_for_query: bool = False) -> None:
        """Karn-style: drop everything measured for this peer. Called on a
        missed ACK that was governed by the measured timeout (the estimate
        may simply have been too tight -- go back to the firmware's guess
        until fresh samples say otherwise) and on any path change (a new
        path is a new link with its own RTT).

        Field fix (2026-09-18 evening): `keep_for_query=True` (the
        missed-ACK case) parks the discarded stats in `_ack_rtt_snapshot`
        so `_completion_query_timeout_s` can still size the reconcile
        QUERY that this very miss is about to trigger -- the drive-home
        capture's two reconciles both fell to the 5s floor at 3 hops for
        want of exactly these numbers. A path change (`keep_for_query`
        False) drops the snapshot, the echo timings and the last firmware
        bound too: none of them describe the new path."""
        if peer_prefix is None:
            return
        st = self._ack_rtt.pop(peer_prefix, None)
        if keep_for_query:
            if st is not None:
                self._ack_rtt_snapshot[peer_prefix] = st
        else:
            self._clear_peer_path_stats(peer_prefix, reason)
        if st is not None:
            self._debug(f"ACK RTT estimate for {peer_prefix!r} discarded ({reason}); firmware timeout applies until re-measured.")

    def _ack_timeout_cap_s(self, hop_count: Optional[int]) -> float:
        """Hop-aware ceiling on one ACK wait (field fix, 2026-09-19 -- see
        `direct_ack_timeout_base_s` for the evidence). `hop_count` None means
        "unknown", which gets the base allowance rather than a long wait."""
        hops = max(0, hop_count or 0)
        return min(
            self.direct_ack_timeout_base_s + self.direct_ack_timeout_per_hop_s * hops,
            self.direct_ack_timeout_routed_max_s,
        )

    def _adaptive_ack_timeout(self, peer_prefix: Optional[str], firmware_timeout_s: float) -> "tuple[float, str]":
        """Returns `(timeout_s, source)` for one ACK wait. `source` is
        "firmware" (the pre-step-2 value, unchanged) or "measured" when
        enough samples exist for this peer. The measured value is
        `multiplier * (srtt + 4*rttvar)`, floored at
        `direct_ack_rtt_min_timeout_s` and -- the invariant that keeps
        this safe to ship on zero-hop-only evidence -- never larger than
        the firmware-derived timeout it replaces."""
        st = self._ack_rtt.get(peer_prefix) if peer_prefix is not None else None
        if (
            not self.direct_ack_rtt_adaptive_enabled
            or st is None
            or st["samples"] < self.direct_ack_rtt_min_samples
        ):
            return firmware_timeout_s, "firmware"
        measured = self.direct_ack_rtt_timeout_multiplier * (st["srtt"] + 4.0 * st["rttvar"]) * st.get("backoff", 1.0)
        measured = max(self.direct_ack_rtt_min_timeout_s, measured)
        if measured >= firmware_timeout_s:
            return firmware_timeout_s, "firmware"
        return measured, "measured"

    def _record_echo(self, peer_prefix: Optional[str], hop_count: Optional[int], echo_s: float) -> None:
        """One measured repeater-echo time (our own frame heard forwarded
        by the first hop, seconds after MSG_SENT) for a multi-hop peer.
        Last 16 samples; cleared with the RTT stats on any path change."""
        if peer_prefix is None or hop_count is None or hop_count < 1 or echo_s <= 0:
            return
        samples = self._echo_stats.setdefault(peer_prefix, [])
        samples.append(echo_s)
        del samples[:-16]
        self._echo_stats_all.append(echo_s)
        del self._echo_stats_all[:-16]

    def _hop1_abort_deadline_s(
        self, peer_prefix: Optional[str], hop_count: Optional[int], timeout_s: float,
    ) -> Optional[float]:
        """Field fix (2026-09-18 evening, see direct_hop1_abort_enabled's
        own comment): how long to wait for either the ACK or the first
        hop's echo before calling the attempt dead, or None when the abort
        is not armed for this peer (disabled, zero/unknown hop count, too
        few echo samples, or a deadline that wouldn't be shorter than the
        ACK timeout anyway)."""
        if (
            not self.direct_hop1_abort_enabled
            or peer_prefix is None
            or hop_count is None or hop_count < 1
        ):
            return None
        samples = self._echo_stats.get(peer_prefix)
        if samples and len(samples) >= self.direct_hop1_abort_min_samples:
            deadline_s = max(self.direct_hop1_abort_min_s, self.direct_hop1_abort_echo_multiplier * max(samples))
        elif len(self._echo_stats_all) >= self.direct_hop1_abort_min_samples:
            # Second audit (2026-09-19 evening): this peer's own samples were
            # cleared by a path change or never existed, but the first hop
            # of ANY path is a repeater running the same forwarding delay --
            # the session-wide pool is the next best evidence.
            deadline_s = max(
                self.direct_hop1_abort_min_s,
                self.direct_hop1_abort_echo_multiplier * max(self._echo_stats_all),
            )
        elif self.direct_hop1_abort_default_s > 0:
            # Nothing measured yet (fresh process): the configured default,
            # sized at twice the largest echo ever seen in the field.
            deadline_s = max(self.direct_hop1_abort_min_s, self.direct_hop1_abort_default_s)
        else:
            return None
        if deadline_s >= timeout_s:
            return None
        return deadline_s

    # -------------------------------------------------------------------
    # Path discovery (docs/path_discovery_spec.md) -- Milestone 4
    # -------------------------------------------------------------------

    def _resolve_contact(self, pubkey_prefix: str):
        if self._mc is None:
            return None
        return self._mc.get_contact_by_key_prefix(pubkey_prefix)

    async def _contact_refresh_loop(self):
        """A live, periodic contact-table read -- never served from a
        cached/inherited value (reliability_engine_design.md §2's
        "data-source gap" fix, reinstating a real mechanism an earlier
        draft of this design set dropped). Feeds path discovery's own
        ensure_contacts() precondition; Milestone 5 is expected to also
        feed this same freshness into the zero-hop/known-N-hop spacing
        tiers, which have no live data source yet.

        Code-review fix: also retries `_fetch_own_identity()` here
        whenever `_own_pubkey_hex` is still empty. `_fetch_own_identity`'s
        own docstring says a failed initial fetch "will be retried on the
        next reconnect if the pubkey is still unknown by then" -- but
        `_on_mc_connected` only ever fires that retry on an actual
        DISCONNECTED-then-CONNECTED cycle. If the physical link comes up,
        the very first `send_appstart` fails, and the link then simply
        stays up for the rest of the process's life (no further CONNECTED
        events), that retry path never runs and `_own_pubkey_prefix()`
        stays `None` forever -- permanently disabling the self-echo guard
        in `_handle_incoming_bind_frame`, so this node's own bind frames
        bouncing back via a repeater or CHANNEL rebroadcast would be
        misprocessed as a genuine external peer for the rest of the
        session. This loop already runs periodically regardless of
        connection-state transitions, so it's a natural place to keep
        retrying until it finally succeeds."""
        try:
            while not self.detached:
                await asyncio.sleep(self._loop_interval_s(self.contact_refresh_interval_s, "contact_refresh_interval"))
                if self.detached:
                    break
                if not self._own_pubkey_hex:
                    self._spawn_background_task(self._fetch_own_identity())
                try:
                    await self._refresh_contacts_and_grant_telemetry()
                except Exception as exc:
                    RNS.log(f"{self}: periodic contact refresh failed: {exc}", RNS.LOG_WARNING)
        except asyncio.CancelledError:
            pass

    async def _refresh_contacts_and_grant_telemetry(self) -> None:
        """Milestone 5 tightens this to docs/peer_discovery_design.md
        §5's actual recommendation: grant base telemetry only to peers
        confirmed via this interface's own bind-frame protocol, not every
        known MeshCore contact -- superseding Milestone 4's
        `telemetry_grant_all_contacts`-default-on simplification, which
        was explicitly flagged there as a placeholder for exactly this.
        A contact not yet resolvable for an already-bound peer (§4's
        "progressively filled in" model -- bind frames and native contact
        adverts are independent floods with no ordering guarantee) is
        simply skipped this pass; the next periodic refresh retries it.
        `telemetry_grant_all_contacts` (default now off) is kept as an
        explicit escape hatch back to the old open-to-every-contact
        behavior, not removed -- a config value that stops being the
        default doesn't stop being real config.

        Code-review fix: `ensure_contacts()` is routed through
        `self._command_lock` here even though it isn't wrapped in
        `_run_command` (its return is a bool, not an Event with a `.type`
        to check -- `_run_command`'s contract doesn't fit it). Left
        unlocked, its internal `get_contacts()` waits on the library's
        shared ERROR/NEXT_CONTACT/CONTACTS events with no per-request
        correlation id (invariant #2) while it could run fully concurrently
        with any `_run_command`-guarded call elsewhere -- e.g. a
        concurrent `change_contact_flags` failing could fire an ERROR
        event this call's dangling wait_for_event(ERROR) would catch
        instead, misattributing an unrelated failure as a contacts-fetch
        failure. This command is quick and only fires at all when contacts
        haven't been fetched yet, so holding the lock across it doesn't
        create the multi-second stall `discover_path`'s own docstring
        warns `send_path_discovery_sync` would cause if it were
        similarly wrapped.

        Code-review fix: this used to call `ensure_contacts()` with no
        arguments. The installed `meshcore` library's `ensure_contacts(self,
        follow=False)` only re-fetches when `not self._contacts` OR
        `(follow and self._contacts_dirty)` -- with the default `follow=
        False`, every call after the very first successful fetch was a
        permanent no-op, even though the library already tracks
        `_contacts_dirty=True` internally on every ADVERTISEMENT/
        PATH_UPDATE event. That directly contradicted this method's own
        "the next periodic refresh retries it" docstring claim above: a
        peer's contact that arrived after this node's first contact fetch
        would never actually be pulled into `self._contacts` by any later
        periodic refresh. Passing `follow=True` here makes this call
        actually consult that dirty flag."""
        async with self._command_lock:
            await self._mc_ready.ensure_contacts(follow=True)
        for peer in list(self._peers.values()):
            contact = self._resolve_contact(peer.pubkey_prefix)
            if contact is not None:
                await self._grant_telemetry_permission_if_needed(contact)
        if self.telemetry_grant_all_contacts:
            for contact in list(self._mc_ready.contacts.values()):
                await self._grant_telemetry_permission_if_needed(contact)

    async def _grant_telemetry_permission_if_needed(self, contact) -> None:
        """docs/path_discovery_spec.md's telemetry-permission section:
        base telemetry (and so path discoverability) is gated per-contact
        via bit 0x02 of that contact's own `flags` field, as stored in
        THIS node's local contact table (confirmed directly against
        examples/companion_radio/MyMesh.cpp's onContactRequest --
        `cp = contact.flags >> 1`, checked against `TELEM_PERM_BASE`).
        Granting is therefore a local action this node takes per peer, not
        a message that reaches out to the peer. As of Milestone 5, called
        only for peers confirmed via this interface's own bind-frame
        protocol (peer_discovery_design.md §5) by default -- see
        `_refresh_contacts_and_grant_telemetry` and `_register_peer`.

        Code-review fix: the `flags` read and bitwise check below used to
        sit outside the `try` block that follows. `_refresh_contacts_and_
        grant_telemetry` calls this once per bound peer/contact in a plain
        `for` loop with no per-iteration isolation -- an unguarded
        `TypeError` here (e.g. a contact whose `flags` field is ever
        `None` or otherwise non-int) would abort that whole loop, silently
        skipping the telemetry-permission grant/refresh for every peer
        ordered after the offending one, with only a generic "contact
        refresh failed" line two frames up to show for it. Moved inside
        the `try` so one malformed contact can't take out every other
        peer's refresh in the same pass."""
        try:
            current_flags = contact.get("flags", 0)
            if current_flags & self.TELEM_PERM_BASE_FLAG_BIT:
                return  # already granted
            new_flags = current_flags | self.TELEM_PERM_BASE_FLAG_BIT
            await self._run_command(
                self._mc_ready.commands.change_contact_flags(contact, new_flags),
                "change_contact_flags",
                self._EventType.OK,
            )
            self._debug(
                f"granted base telemetry permission to contact "
                f"{contact.get('adv_name', '?')!r} ({contact.get('public_key', '?')[:12]}...)."
            )
        except Exception as exc:
            RNS.log(
                f"{self}: failed to grant telemetry permission to contact "
                f"{contact.get('adv_name', '?')!r}: {exc}",
                RNS.LOG_WARNING,
            )

    def _path_discovery_in_backoff(self, pubkey_prefix: str) -> bool:
        until = self._path_discovery_backoff_until.get(pubkey_prefix)
        return until is not None and time.monotonic() < until

    def _record_path_discovery_success(self, pubkey_prefix: str) -> None:
        self._path_discovery_failures.pop(pubkey_prefix, None)
        self._path_discovery_backoff_until.pop(pubkey_prefix, None)

    def _record_path_discovery_failure_round(self, pubkey_prefix: str) -> None:
        failures = self._path_discovery_failures.get(pubkey_prefix, 0) + 1
        self._path_discovery_failures[pubkey_prefix] = failures
        cooldown = min(
            self.path_discovery_base_cooldown_s * (self.path_discovery_backoff_factor ** (failures - 1)),
            self.path_discovery_max_cooldown_s,
        )
        self._path_discovery_backoff_until[pubkey_prefix] = time.monotonic() + cooldown
        RNS.log(
            f"{self}: path discovery to {pubkey_prefix!r} failed "
            f"({failures} consecutive round(s) of {self.path_discovery_quick_attempts} "
            f"quick attempt(s) each) -- backing off {cooldown:.0f}s.",
            RNS.LOG_WARNING,
        )

    async def discover_path(self, pubkey_prefix: str) -> Optional[_ResolvedPath]:
        """docs/path_discovery_spec.md's discover_path() function-level
        spec. Not yet called from anywhere in the automatic send path --
        Milestone 5's routing decisions need to exist first to have a
        DIRECT send that wants a path at all. Exists now, unit-tested
        directly against fakes, as the primitive that logic will call.

        Deliberately does NOT route send_path_discovery_sync() through
        _run_command()/self._command_lock: the library call already
        manages its own internal concurrency (a dedicated
        `_mesh_request_lock`) across its send-then-decoupled-wait-for-
        PATH_RESPONSE shape, which can legitimately take several seconds.
        Serializing it behind this interface's own single command lock
        too would stall every other outgoing command (CHANNEL sends,
        retries) for that whole wait -- exactly what §3's "independent
        queues, one slow operation shouldn't block everything else"
        design principle argues against. Invariant #1 (check the actual
        result, don't trust a bare non-None return) is still applied
        manually below, just without that lock."""
        if self._mc is None or not self.online:
            return None

        if self._path_discovery_in_backoff(pubkey_prefix):
            # Precondition fix from the logical review
            # (path_discovery_spec.md's own function-level spec): a
            # caller retrying this peer while it's still in backoff must
            # not fire a fresh quick-attempts burst and so contribute
            # another failed round to the same schedule it's already
            # respecting. No transmission at all in this case.
            self._debug(f"discover_path({pubkey_prefix!r}): target is in backoff cooldown -- skipping.")
            return None

        contact = self._resolve_contact(pubkey_prefix)
        if contact is None:
            # Code-review fix: locked for the same reason
            # `_refresh_contacts_and_grant_telemetry`'s own `ensure_contacts()`
            # call is (see that method's docstring) -- unlocked, this could
            # race any `_run_command`-guarded call elsewhere and steal its
            # ERROR event via the library's own type-only correlation
            # (invariant #2). Quick and only fires when contacts are
            # unresolved, so this doesn't create the stall
            # `send_path_discovery_sync` below is deliberately kept out of
            # the lock to avoid. Code-review fix: `follow=True`, same
            # reasoning as `_refresh_contacts_and_grant_telemetry`'s own
            # call -- without it, this fallback refresh is a permanent
            # no-op for any contact that arrived after the first fetch,
            # since the library's default `follow=False` never consults
            # its own `_contacts_dirty` flag.
            try:
                async with self._command_lock:
                    await self._mc_ready.ensure_contacts(follow=True)
            except Exception as exc:
                self._debug(f"discover_path({pubkey_prefix!r}): contact refresh failed: {exc}")
            contact = self._resolve_contact(pubkey_prefix)
        if contact is None:
            RNS.log(
                f"{self}: discover_path({pubkey_prefix!r}): peer is not a "
                f"known contact -- cannot discover a path to it.",
                RNS.LOG_WARNING,
            )
            return None

        for attempt in range(1, self.path_discovery_quick_attempts + 1):
            if self.detached or not self.online:
                return None

            try:
                result = await self._mc_ready.commands.send_path_discovery_sync(contact)
            except Exception as exc:
                self._debug(f"discover_path({pubkey_prefix!r}) attempt {attempt}: {exc}")
                result = None

            if result is not None and result.type == self._EventType.PATH_RESPONSE:
                payload = result.payload if isinstance(result.payload, dict) else {}
                responder_prefix = str(payload.get("pubkey_pre", "")).lower()
                full_pubkey = str(contact.get("public_key", "")).lower()
                # The underlying wait is not peer-filtered (a response for
                # a different in-flight discovery could otherwise be
                # mistaken for this one's answer) -- verify it actually
                # names the peer queried before accepting it.
                if not responder_prefix or not full_pubkey.startswith(responder_prefix):
                    self._debug(
                        f"discover_path({pubkey_prefix!r}) attempt {attempt}: "
                        f"PATH_RESPONSE pubkey_pre {responder_prefix!r} doesn't "
                        f"match this contact -- ignoring, treating this attempt "
                        f"as unanswered."
                    )
                else:
                    resolved = _ResolvedPath(
                        out_path_hex=str(payload.get("out_path", "")),
                        out_path_len=int(payload.get("out_path_len", 0)),
                        out_path_hash_len=int(payload.get("out_path_hash_len", 1)),
                        resolved_at=time.monotonic(),
                    )
                    # This interface's own record is authoritative for its
                    # own routing/staleness decisions regardless of
                    # whether the device persist below succeeds.
                    self._resolved_paths[pubkey_prefix] = resolved
                    # A freshly discovered path is a different link; an RTT
                    # measured over the previous one doesn't carry over.
                    self._invalidate_ack_rtt(pubkey_prefix, "path (re)discovered")
                    self._record_path_discovery_success(pubkey_prefix)
                    if self.path_selection_enabled:
                        # Alpha 0.1.6 (item 1): a candidate on the scoreboard,
                        # with the PATH_RESPONSE that just came back over it
                        # as its first delivery; current when nothing is.
                        board = self._path_board(pubkey_prefix)
                        cand = self._add_path_candidate(
                            pubkey_prefix, resolved.out_path_hex, resolved.out_path_len,
                            resolved.out_path_hash_len, "discovered", now=resolved.resolved_at,
                        )
                        cand.consecutive_misses = 0
                        cand.samples.append((resolved.resolved_at, True))
                        cand.last_success_at = resolved.resolved_at
                        previous = board.candidates.get(board.current) if board.current is not None else None
                        if board.current is None or board.current not in board.candidates \
                                or board.candidates[board.current].consecutive_misses >= self.path_switch_after_misses:
                            board.current = cand.path_hex
                        board.last_reason = "discovered"
                        # A `path_selected` record for the field: how the
                        # scoreboard stood when discovery set (or refreshed)
                        # the path.
                        self._capture_path_selected(
                            pubkey_prefix, "discovered", cand, previous if previous is not cand else None,
                            self._rank_paths([self._path_view(c) for c in board.candidates.values()],
                                             resolved.resolved_at,
                                             **self._path_rank_kwargs(pubkey_prefix, now=resolved.resolved_at)),
                            now=resolved.resolved_at)
                    RNS.log(
                        f"{self}: path discovered to {pubkey_prefix!r} in "
                        f"{attempt} attempt(s): out_path_len={resolved.out_path_len}.",
                        RNS.LOG_DEBUG,
                    )
                    await self._persist_resolved_path(contact, resolved, peer_prefix=pubkey_prefix)
                    return resolved

            self._debug(f"discover_path({pubkey_prefix!r}) attempt {attempt}: no response.")

        self._record_path_discovery_failure_round(pubkey_prefix)
        return None

    async def _discover_path_coalesced(self, pubkey_prefix: str) -> Optional[_ResolvedPath]:
        """Milestone 6's part of folding stale-path reset fully into
        routing (docs/reliability_engine_design.md §8: "the next send
        attempt for this peer... goes through discover_path() first...
        and only falls through to genuinely flooding the message itself
        if discovery itself fails") -- called from the DIRECT-primary
        send path (`_send_direct_packet`) whenever no path is currently
        resolved for a peer, whether that's a freshly stale-path-reset
        peer or one that simply never had a path resolved yet.

        Coalesces concurrent callers for the same peer into one shared
        in-flight `discover_path()` attempt: several outgoing packets
        queued for the same not-yet-resolved peer at once would otherwise
        each independently kick off their own quick-attempts burst,
        wasting airtime on redundant PATH_DISCOVERY floods for what's
        really one underlying question. `discover_path()`'s own backoff
        and authoritative-record logic (Milestone 4) is unchanged and
        still applies underneath this -- this only de-duplicates
        concurrent callers, it isn't a second cooldown mechanism."""
        existing = self._pending_path_discoveries.get(pubkey_prefix)
        if existing is not None:
            return await existing

        future = asyncio.get_running_loop().create_future()
        # Retrieved unconditionally so a leader failure with no follower
        # parked on it doesn't log asyncio's "exception was never retrieved".
        future.add_done_callback(lambda f: f.cancelled() or f.exception())
        self._pending_path_discoveries[pubkey_prefix] = future
        try:
            result = await self.discover_path(pubkey_prefix)
        except Exception as exc:
            future.set_exception(exc)
            raise
        else:
            future.set_result(result)
            return result
        finally:
            self._pending_path_discoveries.pop(pubkey_prefix, None)
            if not future.done():
                # Audit fix (2026-09-19): `except Exception` does not catch
                # CancelledError, so a cancelled leader (detach, or any
                # future wait_for wrapper) left this future unresolved AND
                # unreachable -- every follower parked on `await existing`
                # then waited forever, and their _send_direct_packet never
                # returned, so the in-flight key for those packets was held
                # until the 600s sweep.
                future.cancel()

    async def _persist_resolved_path(self, contact, resolved: _ResolvedPath, peer_prefix: Optional[str] = None) -> None:
        """docs/path_discovery_spec.md's persistence fix: a successful
        discovery is NOT written to the device's own persistent contact
        record by the firmware itself (CMD_SEND_PATH_DISCOVERY_REQ's
        handler returns before reaching the code path that would). Skip
        this and this interface's own idea of "resolved" silently
        diverges from what the official app / device flash state show.
        Since alpha 0.1.6 it is also how a selected path reaches the
        radio for text frames (`_select_path`); the scoreboard remembers
        what the contact holds (`device_path`) so a path is set once."""
        try:
            await self._run_command(
                self._mc_ready.commands.change_contact_path(
                    contact, resolved.out_path_hex, path_hash_mode=resolved.out_path_hash_len - 1
                ),
                "change_contact_path",
                self._EventType.OK,
            )
            if peer_prefix is not None:
                self._path_board(peer_prefix).device_path = (resolved.out_path_hex or "").lower()
        except Exception as exc:
            RNS.log(
                f"{self}: persisting discovered path to the device contact "
                f"table failed: {exc} -- this interface's own record stays "
                f"authoritative for routing regardless, but the official "
                f"app / device flash state will disagree until this is "
                f"retried.",
                RNS.LOG_WARNING,
            )

    # -- Path selection by measured reliability (alpha 0.1.6, item 1) ------
    # Every route this node learns to a peer is a candidate on the peer's
    # scoreboard (`_PathBoard`): the discovered path, the reverse of each
    # distinct flood copy the peer's own floods took (`_note_flood_route`,
    # from the rx-log tap), the zero-hop option once the peer has been heard
    # directly, and the path the peer reports in every "Q" v5 frame
    # (`_note_peer_reported_path`). Each candidate is scored as expected
    # transmissions per delivered frame times (hops + 1) -- airtime per
    # delivered byte in frame units, lower is better -- from its measured
    # delivery rate (`_note_path_result`, fed by `record_direct_send_result`)
    # or, untried, a prior. `_select_path` is the one decision, consumed by
    # `_send_direct_packet` and `_send_direct_supplement` where alpha 0.1.5's
    # `_maybe_adopt_shorter_path` was; the pure functions below hold every
    # rule and are tested directly (`tests/test_path_selection_0922.py`,
    # replaying the 2026-09-21 22:00-22:35 desktop sequence).
    #
    # Why this replaced the shortest-route-in-window rule: at 22:00:49 that
    # rule adopted a zero-hop route the laptop had shown 504 s earlier --
    # before it drove off -- over a one-hop path confirmed 2 s earlier; two
    # misses, a reset, and discovery returned a two-hop path the desktop
    # then kept for 32 minutes while the laptop reached it in one hop the
    # whole time, because the one-hop route was never re-tried without a
    # fresh flood (three usable floods in half an hour). Here a delivering
    # path is kept, a missing one is trialled against the best alternative
    # on the next real packet, and a candidate that failed is eligible
    # again once its last miss is older than the switch cooldown.

    @staticmethod
    def _reverse_flood_path(path_hex: str, hash_size: int = 1) -> str:
        """The route back along a flood's recorded path (pure): the hashes
        in reverse order, `hash_size` bytes each. The firmware appends each
        relaying repeater's hash at the end (`Mesh::routeRecvPacket`), and
        `sendDirect` consumes `path[0]` first, so the reverse is exactly the
        out_path a DIRECT frame to the originator needs. Never reversed by
        the firmware itself: a PATH return carries the received path
        unreversed to its originator, for whom it is already forward."""
        size = max(1, int(hash_size)) * 2
        chunks = [path_hex[i:i + size] for i in range(0, len(path_hex) - len(path_hex) % size, size)]
        return "".join(reversed(chunks))

    def _attribute_flood_to_peer(self, payload: dict, fields: dict) -> "Optional[tuple]":
        """Which bound peer originated this overheard FLOOD, and how
        ("advert" / "addressed"), or None. Conservative on purpose (the
        rx-log window's docstring forbids promoting the 1-byte hashes to a
        routing decision without a stronger check): an ADVERT names its
        full public key in the clear; a REQ / RESPONSE / TEXT_MSG / PATH
        flood is attributed only when it is addressed to THIS node
        (`dst_hash` is our own first byte) and its 1-byte source hash
        matches exactly one bound peer and no other contact on the
        device."""
        if fields.get("route_type") not in self._RX_LOG_ROUTE_FLOOD:
            return None
        ptype = fields.get("payload_type")
        if ptype == self._RX_LOG_PAYLOAD_TYPE_ADVERT:
            key = str(payload.get("adv_key") or "").lower()
            if len(key) >= 12 and key[:12] in self._peers:
                return key[:12], "advert"
            return None
        if ptype in self._RX_LOG_ADDRESSED_PAYLOAD_TYPES:
            own = (self._own_pubkey_hex or "")[:2].lower()
            src, dst = fields.get("src_hash"), fields.get("dst_hash")
            if not own or not src or dst != own:
                return None
            candidates = [p for p in self._peers if p[:2].lower() == src]
            if len(candidates) != 1:
                return None
            contacts = self._mc.contacts if self._mc is not None and getattr(self._mc, "contacts", None) else {}
            others = [k for k in contacts if str(k)[:2].lower() == src and not str(k).lower().startswith(candidates[0])]
            if others:
                return None
            return candidates[0], "addressed"
        return None

    def _note_flood_route(self, payload: dict, fields: dict, now: float) -> None:
        """Record the route an overheard flood from a bound peer took as a
        candidate path (cheap: runs inline on the rx-log tap). Every copy of
        a flood is logged -- one per repeater that relayed it -- so one
        flood can add several candidates. The record's SNR / RSSI is the
        signal of the LAST leg (the relaying repeater's transmission for a
        relayed copy, the peer's own for a zero-hop one)."""
        if not self.path_selection_enabled:
            return
        try:
            who = self._attribute_flood_to_peer(payload, fields)
            if who is None:
                return
            peer_prefix, source = who
            hops = int(fields.get("path_len") or 0)
            hash_size = int(payload.get("path_hash_size") or 1)
            reversed_hex = self._reverse_flood_path(str(fields.get("path") or ""), hash_size)
            if hops > 0 and len(reversed_hex) != hops * hash_size * 2:
                return
            self._add_path_candidate(
                peer_prefix, reversed_hex, hops, hash_size, "flood", now=now,
                snr=fields.get("snr"), rssi=fields.get("rssi"),
            )
        except Exception as exc:
            self._debug(f"flood route observer: ignored an rx-log record: {exc}")

    # The pure rules. `views` are plain dicts so the tests drive them from
    # a field replay without an interface: {path_hex, hops, samples
    # [(t, ok)], snr, peer_rate, cooldown_until, consecutive_misses,
    # last_failure_at, last_seen}.

    @staticmethod
    def _path_delivery_rate(samples, now: float, window_s: float, half_life_s: float) -> Optional[float]:
        """The weighted delivery rate over `samples` [(t, ok)] (pure): each
        outcome weighs 0.5 ** (age / half_life_s), outcomes older than
        `window_s` count for nothing. None when nothing counts."""
        num = den = 0.0
        for t, ok in samples:
            age = now - t
            if age < 0 or age > window_s:
                continue
            w = 0.5 ** (age / max(1e-6, half_life_s))
            den += w
            if ok:
                num += w
        return (num / den) if den > 0 else None

    @staticmethod
    def _path_evidence(view, now: float, window_s: float) -> "tuple[Optional[float], Optional[float], bool]":
        """What a candidate's record still says (pure): `(peer_rate, snr,
        stale)`, the peer-reported rate and the last-leg signal only while
        those readings are inside `window_s`, and whether the candidate is
        STALE -- evidence it once had, every piece of it now aged out.

        Alpha 0.1.8 (item 3), from the 2026-09-22 field session: the
        desktop's 22:49:42 record trialled the zero-hop path while the
        laptop was two hops away, scored `rate 1.0, measured False, misses
        3, snr 11.75`. Its own send outcomes HAD aged out of the window
        (hence `measured False`), but the peer-reported rate of 22:07 and
        the SNR reading of the zero-hop period never aged, so the dead path
        still scored 1.0 and outranked a one-hop candidate heard at
        12.25 dB. A reading only counts while it is inside the same window
        the samples are weighed over.

        A reading with no timestamp is NOT aged: the pure-rule replays in
        `tests/` and every candidate built before this release carry the
        value alone, and ageing those would silently turn a known-good
        candidate into a weak one. A candidate that never had any evidence
        is UNTRIED, not stale, and keeps the optimistic prior -- staleness
        is evidence that expired, not evidence that never existed."""
        expired = False
        fresh_any = False

        def keep(value, at):
            nonlocal expired, fresh_any
            if value is None:
                return None
            if at is not None:
                age = now - float(at)
                if age < 0 or age > window_s:
                    expired = True
                    return None
            fresh_any = True
            return value

        peer_rate = keep(view.get("peer_rate"), view.get("peer_rate_at"))
        snr = keep(view.get("snr"), view.get("signal_at"))
        for t, _ok in (view.get("samples") or ()):
            age = now - t
            if 0 <= age <= window_s:
                fresh_any = True
            else:
                expired = True
        return peer_rate, snr, (expired and not fresh_any)

    @staticmethod
    def _path_prior(hops: int, snr: Optional[float], weak_snr_db: float, peer_rate: Optional[float] = None,
                    optimistic: float = 0.8, weak: float = 0.25, stale: bool = False,
                    peer_path_len: Optional[int] = None) -> float:
        """The delivery rate an UNTRIED candidate is scored with (pure): the
        weak prior for a candidate whose evidence has all aged out, else the
        peer's own reported rate on a path of this length when it sent one,
        else the weak prior for a candidate whose last direct frame was
        below `weak_snr_db`, else the weak prior for a candidate claiming
        FEWER hops than the peer says it needs to reach us, else the
        optimistic prior -- so the shortest untried path with honest
        evidence behind it scores best and is tried first.

        Alpha 0.1.8 (item 3): the weak-signal rule applies at ANY hop count.
        It was written for `hops == 0` alone, and the laptop's 22:30:26
        field record is what that cost: a three-hop candidate `4fbe02`
        heard once at -9 dB was scored with the optimistic prior (0.8,
        score 5.0) and trialled ahead of the current two-hop path, for two
        misses and 26 s before the board gave up and rediscovered. A weak
        last leg is weak evidence whatever precedes it; the zero-hop rule
        is a special case of this, not an exception to it. The peer's own
        reported rate still comes FIRST: the same session's 22:51:37 trial
        of `d619` read -9.5 dB and a fresh peer rate of 0.668, was
        trialled on that rate, delivered, and became the current path."""
        if stale:
            return weak
        if peer_rate is not None:
            return max(0.0, min(1.0, float(peer_rate)))
        if snr is not None and float(snr) < weak_snr_db:
            return weak
        if peer_path_len is not None and int(hops) < int(peer_path_len):
            return weak
        return optimistic

    @staticmethod
    def _path_score(hops: int, rate: float, rate_floor: float = 0.05) -> float:
        """Expected transmissions per delivered frame times (hops + 1)
        (pure): airtime per delivered byte in frame units, lower is
        better."""
        return (max(0, int(hops)) + 1) / max(rate_floor, float(rate))

    @classmethod
    def _rank_paths(cls, views, now: float, weak_snr_db: float, window_s: float, half_life_s: float,
                    optimistic: float = 0.8, weak: float = 0.25, rate_floor: float = 0.05,
                    peer_path_len: Optional[int] = None) -> list:
        """Every candidate scored (pure): [(score, view, rate, measured)]
        sorted best first -- a candidate on switch-back cooldown ranks
        behind every other, then a STALE candidate behind every one with
        evidence inside `window_s` (alpha 0.1.8, item 3), then by score,
        then fewer hops (the tiebreak), then most recently seen.

        The peer-reported rate and the signal reading are aged the same way
        the samples are (`_path_evidence`); `peer_path_len` is the peer's
        own reported path length to us while that report is itself inside
        the window, which makes an untried candidate claiming fewer hops
        than that score weak rather than optimistic."""
        ranked = []
        stale_by_id = {}
        for v in views:
            rate = cls._path_delivery_rate(v.get("samples") or (), now, window_s, half_life_s)
            measured = rate is not None
            peer_rate, snr, stale = cls._path_evidence(v, now, window_s)
            stale_by_id[id(v)] = stale
            if not measured:
                rate = cls._path_prior(int(v.get("hops") or 0), snr, weak_snr_db, peer_rate,
                                       optimistic=optimistic, weak=weak, stale=stale,
                                       peer_path_len=peer_path_len)
            score = cls._path_score(int(v.get("hops") or 0), rate, rate_floor=rate_floor)
            ranked.append((score, v, rate, measured))
        ranked.sort(key=lambda r: ((r[1].get("cooldown_until") or 0.0) > now, stale_by_id[id(r[1])], r[0],
                                   int(r[1].get("hops") or 0), -float(r[1].get("last_seen") or 0.0)))
        return ranked

    @classmethod
    def _choose_path(cls, views, current_hex: Optional[str], now: float, switch_after_misses: int, cooldown_s: float,
                     weak_snr_db: float, window_s: float, half_life_s: float, **kw) -> "tuple[Optional[str], str, list]":
        """The switching rule (pure): (path hex or None, reason, ranked).

        Eligibility. A candidate that has missed PATH_EXHAUST_MISSES
        consecutive attempts is DEAD and stays ineligible, whatever the
        cooldown says, until fresh external evidence for it arrives -- a
        flood copy, a zero-hop peer report or a discovery result, i.e.
        anything that refreshes `last_seen` after its last failure (a
        discovery result also resets the count). Otherwise a candidate is
        eligible while it has missed fewer than `switch_after_misses`
        consecutive attempts, while its measured delivery rate is at least
        PATH_HEALTHY_RATE, or once its last miss is older than `cooldown_s`
        -- and then only if its measured rate is unknown, or at least the
        current path's measured rate, or fresh evidence has arrived since
        that miss (alpha 0.1.9 second pass, item 2).

        Choice. With no current path the best eligible candidate is
        "selected". A current path under the miss threshold is kept
        ("current", whatever the alternatives score -- a delivering path is
        not abandoned on hop count). Past it, the current path is still
        kept ("current_best") while it has delivered in the window and its
        measured rate beats the rate of every eligible alternative --
        measured, or the prior it is ranked with, or its prior when fresh
        evidence has arrived since its last miss -- unless it is dead, or an
        eligible alternative is UNTRIED (no send outcome, no miss, evidence
        not aged out): that one always gets its trial, in rank order.
        Otherwise the best eligible candidate is used: the current one
        itself ("current_best") or another as a "trial". "exhausted":
        nothing is eligible -- the caller runs discovery; "none": nothing is
        known."""
        views = list(views)
        if not views:
            return None, "none", []
        ranked = cls._rank_paths(views, now, weak_snr_db, window_s, half_life_s, **kw)
        measured = {id(v): (rate if m else None) for _s, v, rate, m in ranked}
        used_rate = {id(v): rate for _s, v, rate, _m in ranked}
        by_hex = {v["path_hex"]: v for v in views}
        current = by_hex.get(current_hex) if current_hex is not None else None
        current_rate = measured.get(id(current)) if current is not None else None

        def fresh_evidence(v) -> bool:
            seen, failed = v.get("last_seen"), v.get("last_failure_at")
            return seen is not None and failed is not None and float(seen) > float(failed)

        def dead(v) -> bool:
            return int(v.get("consecutive_misses") or 0) >= PATH_EXHAUST_MISSES and not fresh_evidence(v)

        def eligible(v) -> bool:
            if dead(v):
                return False
            misses = int(v.get("consecutive_misses") or 0)
            if misses < switch_after_misses:
                return True
            rate = measured.get(id(v))
            # Fourth cut (2026-09-22): a healthy measured record keeps a
            # candidate eligible through a run of misses short of dead.
            if rate is not None and rate >= PATH_HEALTHY_RATE:
                return True
            last = v.get("last_failure_at")
            if last is None or now - float(last) < cooldown_s:
                return False
            if v is current or current_rate is None or rate is None or fresh_evidence(v):
                return True
            return rate >= current_rate

        if current is None:
            for _score, v, _rate, _m in ranked:
                if eligible(v):
                    return v["path_hex"], "selected", ranked
            return None, "exhausted", ranked
        if int(current.get("consecutive_misses") or 0) < switch_after_misses:
            return current_hex, "current", ranked
        def rival_rate(v) -> float:
            # A candidate heard from since its last failure competes on what
            # that evidence says (its prior), not on the misses it superseded
            # -- the same reading that re-opens it above. The 2026-09-21
            # replay (tests/test_path_selection_0922.py) is this case: a
            # one-hop flood copy 53 s after that path's last miss.
            if not fresh_evidence(v):
                return used_rate[id(v)]
            peer_rate, snr, stale = cls._path_evidence(v, now, window_s)
            return cls._path_prior(int(v.get("hops") or 0), snr, weak_snr_db, peer_rate,
                                   optimistic=kw.get("optimistic", 0.8), weak=kw.get("weak", 0.25), stale=stale,
                                   peer_path_len=kw.get("peer_path_len"))

        def untried(v) -> bool:
            # Never sent over, never missed, and its evidence not aged out:
            # a candidate the scoreboard knows nothing against. Its prior
            # ORDERS it among the others but does not keep it from one trial
            # -- second cut, from MeshBench `shortcut_appears` (item 2's
            # first cut compared the current path's 0.27-0.33 against the
            # 0.25 weak prior every flood-learned candidate gets at
            # MeshBench's 0 dB, so the one-hop shortcut was trialled only
            # once the three-hop path was dead, minutes after the move).
            if v.get("samples") or int(v.get("consecutive_misses") or 0):
                return False
            return not cls._path_evidence(v, now, window_s)[2]

        if current_rate is not None and current_rate > 0.0 and not dead(current):
            alternatives = [v for _s, v, _r, _m in ranked if v is not current and eligible(v)]
            if not any(untried(v) for v in alternatives) and all(current_rate > rival_rate(v) for v in alternatives):
                return current_hex, "current_best", ranked
        for _score, v, _rate, _m in ranked:
            if eligible(v):
                return v["path_hex"], ("current_best" if v is current else "trial"), ranked
        return None, "exhausted", ranked

    @staticmethod
    def _switch_for_good(current_score: float, candidate_score: float, margin: float,
                         candidate_cooldown_until: float, now: float) -> bool:
        """Whether a trial that delivered becomes the current path (pure):
        its score must beat the current path's by `margin` and it must not
        be on switch-back cooldown."""
        if candidate_cooldown_until > now:
            return False
        return candidate_score <= current_score * (1.0 - margin)

    # The scoreboard.

    def _path_board(self, peer_prefix: str) -> "_PathBoard":
        board = self._path_boards.get(peer_prefix)
        if board is None:
            board = _PathBoard()
            self._path_boards[peer_prefix] = board
        return board

    def _path_view(self, cand: "_PathCandidate") -> dict:
        return {
            "path_hex": cand.path_hex, "hops": cand.hops, "samples": list(cand.samples), "snr": cand.snr,
            "peer_rate": cand.peer_rate, "cooldown_until": cand.cooldown_until,
            "consecutive_misses": cand.consecutive_misses, "last_failure_at": cand.last_failure_at,
            "last_seen": cand.last_seen, "source": cand.source,
            # Alpha 0.1.8 (item 3): when each reading was taken, so the
            # pure rules can age it. The values stay on the view whatever
            # their age -- the capture prints them either way.
            "peer_rate_at": cand.peer_rate_at, "signal_at": cand.signal_at,
        }

    def _path_rank_kwargs(self, peer_prefix: Optional[str] = None, now: Optional[float] = None) -> dict:
        """The tuning the pure rules read. With a peer, also that peer's own
        reported path length to us while the report is inside the sample
        window (alpha 0.1.8, item 3) -- an untried candidate shorter than
        the peer says it needs scores weak, not optimistic."""
        kwargs = {
            "weak_snr_db": self.path_weak_snr_db, "window_s": self.PATH_SAMPLE_WINDOW_S,
            "half_life_s": self.PATH_SAMPLE_HALF_LIFE_S, "optimistic": self.PATH_PRIOR_OPTIMISTIC,
            "weak": self.PATH_PRIOR_WEAK, "rate_floor": self.PATH_RATE_FLOOR,
        }
        if peer_prefix is not None:
            board = self._path_boards.get(peer_prefix)
            if board is not None and board.peer_path_len is not None and board.peer_report_at is not None:
                age = (time.monotonic() if now is None else now) - float(board.peer_report_at)
                if 0 <= age <= self.PATH_SAMPLE_WINDOW_S:
                    kwargs["peer_path_len"] = int(board.peer_path_len)
        return kwargs

    def _add_path_candidate(self, peer_prefix: str, path_hex: str, hops: int, hash_size: int, source: str,
                            now: Optional[float] = None, snr: Optional[float] = None,
                            rssi: Optional[float] = None) -> "_PathCandidate":
        """Create or refresh a candidate path to `peer_prefix`. A known
        route keeps its statistics and first source and gains the fresher
        last-seen time (and signal, when given); a fifth candidate evicts
        the worst-ranked one that is not current."""
        now = time.monotonic() if now is None else now
        board = self._path_board(peer_prefix)
        path_hex = (path_hex or "").lower()
        cand = board.candidates.get(path_hex)
        if cand is None:
            if len(board.candidates) >= self.PATH_CANDIDATES_KEPT:
                ranked = self._rank_paths([self._path_view(c) for c in board.candidates.values()], now,
                                          **self._path_rank_kwargs(peer_prefix, now=now))
                for _score, v, _rate, _m in reversed(ranked):
                    if v["path_hex"] != board.current:
                        board.candidates.pop(v["path_hex"], None)
                        break
            cand = _PathCandidate(path_hex, int(hops), max(1, int(hash_size)), source, now)
            board.candidates[path_hex] = cand
        cand.last_seen = now
        if snr is not None or rssi is not None:
            cand.snr, cand.rssi, cand.signal_at = snr, rssi, now
        return cand

    def _note_path_signal(self, peer_prefix: str, path_hex: Optional[str], snr: Optional[float],
                          rssi: Optional[float], now: Optional[float] = None) -> None:
        """The signal of the last frame received over a candidate path --
        for a relayed path that is its LAST leg only (the repeater's
        transmission), the only leg this radio hears; for the zero-hop
        path it is the peer's own signal, which is what the weak-direct
        prior reads. Fed with the ACK the rx-log matched to our own send
        (`_classify_rx_log_for_window`)."""
        if path_hex is None or (snr is None and rssi is None):
            return
        cand = self._path_board(peer_prefix).candidates.get((path_hex or "").lower())
        if cand is not None:
            cand.snr, cand.rssi, cand.signal_at = snr, rssi, (time.monotonic() if now is None else now)

    def _note_peer_reported_path(self, peer_prefix: str, hops: Optional[int], rate: Optional[float],
                                 now: Optional[float] = None) -> None:
        """The peer's own view, from a "Q" v5 frame: its current path
        length to us and its delivery rate on it. A zero-hop report makes
        (or refreshes) the zero-hop candidate -- the peer hears us
        directly; a reported rate is evidence for the symmetric path: every
        candidate of that hop count scores with it while untried."""
        if hops is None or not self.path_selection_enabled:
            return
        now = time.monotonic() if now is None else now
        board = self._path_board(peer_prefix)
        board.peer_path_len, board.peer_rate, board.peer_report_at = int(hops), rate, now
        if hops == 0:
            self._add_path_candidate(peer_prefix, "", 0, 1, "peer_report", now=now)
        if rate is not None:
            for cand in board.candidates.values():
                if cand.hops == hops:
                    cand.peer_rate, cand.peer_rate_at = float(rate), now

    def _path_rate_for_wire(self, peer_prefix: Optional[str]) -> "tuple[Optional[int], Optional[float]]":
        """What this node tells the peer in a "Q" v5 header: the hop count
        of its current path to the peer and the measured delivery rate on
        it (None while untried)."""
        if peer_prefix is None:
            return None, None
        resolved = self._resolved_paths.get(peer_prefix)
        board = self._path_boards.get(peer_prefix)
        if resolved is None:
            return None, None
        cand = board.candidates.get((resolved.out_path_hex or "").lower()) if board is not None else None
        rate = None
        if cand is not None:
            rate = self._path_delivery_rate(cand.samples, time.monotonic(), self.PATH_SAMPLE_WINDOW_S,
                                            self.PATH_SAMPLE_HALF_LIFE_S)
        return int(resolved.out_path_len), rate

    def _note_path_attempt_result(self, peer_prefix: Optional[str], ok: bool, waited_full_timeout: bool,
                                  ack_timeout_source: str, ack_latency_s: Optional[float] = None) -> None:
        """One ATTEMPT's outcome on the path it went over (alpha 0.1.9,
        item 4). Moves `consecutive_misses` only: the delivery-rate samples
        stay one per send.

        The defect this fixes. Airtime is spent per attempt, but the
        scoreboard learned per send -- and a fragmented send's per-fragment
        attempts were not recorded at all (`record_result=False` on the
        fragment passes), while a QUERY round's were deliberately not a
        sample (`path_sample=False`, alpha 0.1.6's second cut). The
        desktop's 2026-09-23 capture shows what that costs: between
        11:29:35 and 11:31:26 it spent nine raw-fragment attempts and two
        QUERY attempts on a two-hop path that was dead, every one a
        `firmware` miss, and no `direct_send_result` at all in that span --
        so almost nothing reached the scoreboard, the board did not reach
        its trial threshold until 11:41:22 and never reached "exhausted".
        The same shape cost 14 attempts and 2.5 minutes on 2026-09-22
        between 22:28 and 22:31, where 14 attempts registered as 3 misses.
        Counting attempts, the 11:29 burst passes `path_switch_after_misses`
        at its fourth attempt and `PATH_EXHAUST_MISSES` at its eighth, so
        discovery runs about a minute in rather than ten.

        Why the count and not the rate (design (B), written down as the
        release asked). The delivery rate is not a local number: it is
        computed from `samples`, put on the wire in the "Q" v5 rate byte by
        `_path_rate_for_wire`, and read by the peer as the FIRST rule of
        `_path_prior`. `PATH_PRIOR_OPTIMISTIC` (0.8), `PATH_PRIOR_WEAK`
        (0.25), `PATH_HEALTHY_RATE` (0.5) and `PATH_RATE_FLOOR` are all
        calibrated against per-send rates, and so is the replay fixture
        `tests/fixtures/field_0921_desktop_22h.json`. A per-attempt rate
        would settle near 0.65 at one hop and 0.51 at two (the measured
        field figures), below the 0.8 optimistic prior, so every untried
        candidate would outscore every measured one -- the churn alpha
        0.1.8's item 3 was written to stop -- and all four constants would
        have to be re-derived first. `consecutive_misses` is purely local
        and is the quantity the death clock actually reads, so it is the
        one that changes unit.

        The thresholds are rescaled by `direct_send_attempts` so today's
        patience is preserved exactly: a missed send IS two consecutive
        missed attempts (any successful attempt both ends the send and
        resets the counter), so 2 -> 4 and 4 -> 8. On a healthy path at
        50 % attempt success that still means about 20 sends to a trial and
        about 340 to exhaustion, as before -- the "fourth cut" patience in
        `_choose_path` is untouched. What changes is that a send with a
        LARGER budget now costs what it spends: a four-attempt handshake or
        pass-1 finish counts four, not one.

        Which attempts count is deliberately an allow-list, not a
        deny-list (`PATH_ATTEMPT_MISS_SOURCES`): a miss is evidence about
        the path only when this node transmitted and waited the full miss
        ceiling and the silence is the path's. A locally shortened ceiling,
        a pre-empted wait, a supersession, an expiry in the lock queue, a
        reply that arrived another way and a no-ACK frame are all excluded,
        and so, as before, is anything with `waited_full_timeout` False."""
        if not self.path_selection_enabled or not peer_prefix:
            return
        if ok:
            if ack_timeout_source == "noack" or ack_latency_s is None:
                return          # no real ACK came back: not evidence either way
        elif not (waited_full_timeout and ack_timeout_source in PATH_ATTEMPT_MISS_SOURCES):
            return
        resolved = self._resolved_paths.get(peer_prefix)
        path_hex = (resolved.out_path_hex or "").lower() if resolved is not None else None
        # Alpha 0.1.9, second pass (item 1): "" IS a path -- the zero-hop
        # one -- so only a missing resolved path returns here. The first
        # pass wrote `if not path_hex`, which dropped every zero-hop attempt
        # as "no path"; since `_note_path_result` no longer increments the
        # counter, the zero-hop path's misses were then counted nowhere.
        # Session 2 of 2026-09-23 evening: after the laptop drove off at
        # 22:20 it missed 144 consecutive zero-hop attempts over 20 minutes
        # with one `path_selected` record in the whole capture, and the
        # desktop did the same, until the laptop was restarted.
        if path_hex is None:
            return
        board = self._path_board(peer_prefix)
        cand = board.candidates.get(path_hex)
        if cand is None:
            return              # nothing known about this path yet; the send outcome adds it
        now = time.monotonic()
        if ok:
            cand.consecutive_misses = 0
            cand.last_success_at = now
        else:
            cand.consecutive_misses += 1
            cand.last_failure_at = now

    def _note_path_result(self, peer_prefix: str, path_hex: Optional[str], ok: bool,
                          ack_latency_s: Optional[float] = None, now: Optional[float] = None) -> None:
        """One send's outcome on a candidate path (a send = its whole
        attempt budget, as `record_direct_send_result` counts). A success
        on a trial path that beats the current one by `path_switch_margin`
        makes it current for good and puts the old current on switch-back
        cooldown (`path_switch_cooldown`)."""
        if path_hex is None:
            return
        now = time.monotonic() if now is None else now
        board = self._path_board(peer_prefix)
        path_hex = (path_hex or "").lower()
        cand = board.candidates.get(path_hex)
        if cand is None:
            hops = len(path_hex) // 2
            cand = self._add_path_candidate(peer_prefix, path_hex, hops, 1, "discovered", now=now)
        cand.samples.append((now, bool(ok)))
        if ok:
            cand.consecutive_misses = 0
            cand.last_success_at = now
            if ack_latency_s is not None and ack_latency_s > 0:
                cand.ack_latencies.append(float(ack_latency_s))
        else:
            # Alpha 0.1.9 (item 4): the miss COUNT is kept per attempt by
            # `_note_path_attempt_result`, which has already counted every
            # attempt behind this send; incrementing again here would
            # double-count it. The delivery-rate SAMPLE stays per send --
            # that number is published on the wire and read by the peer as
            # a prior, so its unit must not change.
            #
            # The invariant that makes this safe: every send that reaches
            # here with `succeeded=False` has at least one counted attempt
            # behind it. `record_direct_send_result` is only reached on a
            # failure with `waited_full_timeout` True, and an attempt that
            # waited the full miss ceiling has `ack_timeout_source`
            # "firmware" or "hop1_abort" -- the two in
            # PATH_ATTEMPT_MISS_SOURCES. Every other source either returns
            # before recording (expired, superseded, answered) or sets
            # `waited_full_timeout` False (measured), and "report_window"
            # belongs to the report/answer sends, which never record a send
            # result at all. A miss counted here as well would be the same
            # airtime counted twice.
            cand.last_failure_at = now
        if board.current is None:
            board.current = path_hex
            return
        if ok and path_hex != board.current:
            ranked = self._rank_paths([self._path_view(c) for c in board.candidates.values()], now,
                                      **self._path_rank_kwargs(peer_prefix, now=now))
            scores = {v["path_hex"]: s for s, v, _r, _m in ranked}
            if board.current in scores and self._switch_for_good(
                    scores[board.current], scores[path_hex], self.path_switch_margin, cand.cooldown_until, now):
                previous = board.candidates.get(board.current)
                if previous is not None:
                    previous.cooldown_until = now + self.path_switch_cooldown_s
                board.current = path_hex
                self._capture_path_selected(peer_prefix, "switch", cand, previous, ranked, now=now)
                RNS.log(
                    f"{self}: path to {peer_prefix!r} switched to {path_hex or '<zero-hop>'} ({cand.hops} hop(s), "
                    f"score {scores[path_hex]:.2f}) from {previous.path_hex or '<zero-hop>' if previous else '?'} "
                    f"(score {scores.get(previous.path_hex, 0.0) if previous else 0.0:.2f}) for good.",
                    RNS.LOG_DEBUG,
                )

    def _capture_path_selected(self, peer_prefix: str, reason: str, cand: "Optional[_PathCandidate]",
                               previous: "Optional[_PathCandidate]", ranked, now: Optional[float] = None) -> None:
        if self._packet_capture_file is None:
            return
        # `now` is the instant `ranked` was scored at, so the freshness
        # flags below read the same ages the decision did (alpha 0.1.8).
        now = time.monotonic() if now is None else now
        board = self._path_boards.get(peer_prefix)
        peer_reported = any(v.get("source") == "peer_report" for _s, v, _r, _m in ranked)
        self._capture_event("out", {
            "event": "path_selected", "peer_prefix": peer_prefix, "reason": reason,
            "path_hex": cand.path_hex if cand is not None else None,
            "path_len": cand.hops if cand is not None else None,
            "previous_path_hex": previous.path_hex if previous is not None else None,
            "previous_path_len": previous.hops if previous is not None else None,
            # Alpha 0.1.7 (item 4): the peer's reported view when one of the
            # candidates came from its report (the v5 header).
            "peer_path_len": board.peer_path_len if peer_reported and board is not None else None,
            "peer_rate": (round(board.peer_rate, 3) if peer_reported and board is not None and board.peer_rate is not None else None),
            # Alpha 0.1.8 (item 3): `stale` (every piece of this candidate's
            # evidence aged out of PATH_SAMPLE_WINDOW_S) and `snr_fresh` /
            # `peer_rate_fresh` (whether each reading still counted), so a
            # field capture shows WHY a candidate scored as it did. The
            # readings themselves are printed whatever their age.
            "scores": [dict({
                "path_hex": v["path_hex"], "hops": v["hops"], "score": round(score, 3), "rate": round(rate, 3),
                "measured": measured, "misses": v["consecutive_misses"], "snr": v.get("snr"), "source": v.get("source"),
            }, **(lambda pr, sn, st: {
                "stale": st, "snr_fresh": sn is not None, "peer_rate_fresh": pr is not None,
                "peer_rate": round(v["peer_rate"], 3) if v.get("peer_rate") is not None else None,
            })(*self._path_evidence(v, now, self.PATH_SAMPLE_WINDOW_S)))
                for score, v, rate, measured in ranked],
        })

    async def _select_path(self, peer_prefix: str) -> "Optional[_ResolvedPath]":
        """The one path decision for a DIRECT send to `peer_prefix`: the
        `_ResolvedPath` to use, set on the device contact when it differs
        from what the contact holds (text frames are routed by the contact's
        stored path; raw fragments carry theirs explicitly), or None when
        every candidate is exhausted (the caller runs discovery, exactly
        where it did before) or nothing is known.

        A raw window in flight to the peer does not hold the decision
        (second cut, from MeshBench `shortcut_appears` on the first cut:
        under continuous traffic the next part always arrived while the
        previous window was still running, so a guard that stood aside
        for an in-flight window never let a trial happen -- six missed
        sends in a row on a dead three-hop path and no `path_selected`
        at all). While the current path delivers nothing changes and the
        window is untouched; when a trial or selection is due the
        window on the failing path is aborted by its own mid-send check
        (`_raw_path_reset_mid_send`, the parts remembered for resume) and
        the next sends go on the chosen path."""
        resolved = self._resolved_paths.get(peer_prefix)
        if not self.path_selection_enabled:
            return resolved
        now = time.monotonic()
        board = self._path_board(peer_prefix)
        if resolved is not None and (resolved.out_path_hex or "").lower() not in board.candidates:
            cand = self._add_path_candidate(peer_prefix, resolved.out_path_hex, resolved.out_path_len,
                                            resolved.out_path_hash_len, "discovered", now=resolved.resolved_at)
            if board.current is None:
                board.current = cand.path_hex
        views = [self._path_view(c) for c in board.candidates.values()]
        chosen_hex, reason, ranked = self._choose_path(
            views, board.current, now, self.path_switch_after_misses, self.path_switch_cooldown_s,
            **self._path_rank_kwargs(peer_prefix, now=now),
        )
        previous = board.candidates.get((resolved.out_path_hex or "").lower()) if resolved is not None else None
        if chosen_hex is None:
            if reason != board.last_reason:
                self._capture_path_selected(peer_prefix, reason, None, previous, ranked, now=now)
                if reason == "exhausted":
                    RNS.log(
                        f"{self}: every candidate path to {peer_prefix!r} has missed its last "
                        f"{self.path_switch_after_misses} attempt(s) -- running path discovery.",
                        RNS.LOG_WARNING,
                    )
            board.last_reason = reason
            if resolved is not None:
                self._resolved_paths.pop(peer_prefix, None)
                self._invalidate_ack_rtt(peer_prefix, "candidate paths exhausted")
            return None
        cand = board.candidates[chosen_hex]
        if reason == "selected":
            board.current = chosen_hex
        elif reason == "trial":
            cand.trials += 1
        changed = resolved is None or (resolved.out_path_hex or "").lower() != chosen_hex
        if changed or reason != board.last_reason and reason in ("selected", "trial", "switch"):
            self._capture_path_selected(peer_prefix, reason, cand, previous, ranked, now=now)
            RNS.log(
                f"{self}: path to {peer_prefix!r}: {reason} {chosen_hex or '<zero-hop>'} ({cand.hops} hop(s), "
                f"{cand.source}" + (f", {cand.consecutive_misses} miss(es)" if cand.consecutive_misses else "") + ")"
                + (f" instead of {previous.path_hex or '<zero-hop>'} ({previous.hops} hop(s), "
                   f"{previous.consecutive_misses} miss(es))" if previous is not None and previous is not cand else "")
                + ".",
                RNS.LOG_DEBUG,
            )
        board.last_reason = reason
        if changed:
            resolved = _ResolvedPath(out_path_hex=chosen_hex, out_path_len=cand.hops,
                                     out_path_hash_len=cand.hash_size, resolved_at=now)
            self._resolved_paths[peer_prefix] = resolved
            self._invalidate_ack_rtt(peer_prefix, "path selected")
        if board.device_path != chosen_hex:
            contact = self._resolve_contact(peer_prefix)
            if contact is not None:
                await self._persist_resolved_path(contact, resolved, peer_prefix=peer_prefix)
        return resolved

    # -- Stale cached-path detection and reset (§8) ------------------------

    def record_direct_send_result(
        self,
        pubkey_prefix: str,
        succeeded: bool,
        waited_full_timeout: bool,
        rssi: Optional[float] = None,
        path_hex: Optional[str] = None,
        ack_latency_s: Optional[float] = None,
        path_sample: bool = True,
    ) -> None:
        """docs/path_discovery_spec.md §8 / reliability_engine_design.md
        §8: call this after every DIRECT send attempt made against an
        already-cached path (not during discovery itself, and not for an
        attempt with no cached path to begin with). Not yet wired into an
        actual DIRECT send path -- Milestone 5+ adds the routing
        decisions that would call this for real. Exists now, unit-tested
        directly, as the primitive that logic will call.

        Although this method itself is plain (synchronous, not a
        coroutine), it can schedule a background reset task
        (_spawn_background_task) when the failure threshold is crossed --
        that only attaches correctly to this interface's own dedicated
        event loop if this method is called from a coroutine already
        running on it, exactly where its real Milestone 5+ caller (DIRECT-
        send routing logic) will naturally already be. Calling it from any
        other thread would schedule the reset task on the wrong loop.

        Code-review note on the `rssi` parameter: no current call site
        passes it (confirmed against every `record_direct_send_result`
        call in this file), and this isn't just an integration gap -- the
        installed `meshcore` library's real ACK event (the thing a DIRECT
        send actually correlates on) carries only `code`/`trip_time`, no
        signal-quality field at all. The nearest real signal, SNR (not
        RSSI -- a different unit than `direct_path_reset_rssi_floor`'s
        dBm-scale default), only appears on *inbound* CONTACT_MSG_RECV_V3
        frames, and only under protocol version 3, meaning "the last SNR
        this peer's DIRECT messages arrived with" would need its own
        tracking and a considered dBm<->dB conversion before it could
        feed this floor honestly. Left as configured/tested-but-unreached
        rather than deleted or faked with a converted value that hasn't
        been validated against real hardware."""
        # Alpha 0.1.6 (item 1): with path selection on, every send outcome
        # is a sample on the path it went over (`path_hex`, else the current
        # resolved path) and the scoreboard's exhaustion rule -- every
        # candidate missed its last sends -> discovery -- replaces the
        # threshold detector below, whose min-age and healthy-patience
        # guards belong to a world with one path per peer.
        # `path_sample=False` (item 1, second cut): the raw window's per-
        # round QUERY evidence is not a sample of its own -- one failing
        # window counted four or five misses (each QUERY round and the
        # give-up), exhausting a path on one send; the window's outcome is
        # the one sample.
        if self.path_selection_enabled:
            if path_hex is None:
                resolved = self._resolved_paths.get(pubkey_prefix)
                path_hex = resolved.out_path_hex if resolved is not None else None
            if path_sample and (succeeded or waited_full_timeout):
                self._note_path_result(pubkey_prefix, path_hex, succeeded, ack_latency_s=ack_latency_s)
            return
        if succeeded:
            self._direct_path_failures.pop(pubkey_prefix, None)
            # Field fix (2026-09-19): remember that this path just worked --
            # see the healthy-path guard below.
            recent = self._direct_path_recent_success.setdefault(pubkey_prefix, [])
            recent.append(time.monotonic())
            if len(recent) > 64:
                del recent[:-64]
            return
        if not waited_full_timeout:
            # An attempt cut short by this engine's own ceiling being too
            # tight proves nothing about the path itself -- the old
            # design's own field-diagnosed gate, kept unchanged.
            return

        failures = self._direct_path_failures.get(pubkey_prefix, 0) + 1
        self._direct_path_failures[pubkey_prefix] = failures

        effective_threshold = self.direct_path_reset_threshold
        # Field fix (2026-09-19 evening session): a path that was demonstrably
        # working moments ago needs more than one short burst of failures
        # before it is thrown away. Measured: a 1-hop path running 50/55 (91%)
        # was discarded after a 7-attempt bad patch, and the replacement
        # discovery adopted a 2-hop path that then managed 1/12 -- the
        # degradation ratcheted (1 -> 2 -> 3 hops) and never recovered,
        # because nothing remembered the previous path had been healthy. LoRa
        # links fade in bursts; requiring proportionally more evidence to
        # abandon a proven path is cheap, and failures still accumulate, so a
        # genuinely dead path is still reset -- just a little later.
        healthy = self._recent_path_successes(pubkey_prefix)
        if healthy >= self.direct_path_healthy_recent_successes:
            effective_threshold = max(
                effective_threshold,
                int(self.direct_path_reset_threshold * self.direct_path_healthy_patience_multiplier),
            )
            self._debug(
                f"record_direct_send_result({pubkey_prefix!r}): {failures} failure(s), but this path "
                f"had {healthy} success(es) in the last {self.direct_path_healthy_window_s:.0f}s -- "
                f"requiring {effective_threshold} failures before resetting it."
            )
        if rssi is not None and rssi > self.direct_path_reset_rssi_floor:
            # Conditions look fine -- be more patient before concluding
            # the path itself, rather than transient RF, is the problem.
            effective_threshold = int(
                self.direct_path_reset_threshold * self.direct_path_reset_patience_multiplier
            )

        if failures < effective_threshold:
            return

        # User-requested fix (2026-09-15, post-alpha-0.1.0 2-hop field
        # test): see direct_path_reset_min_age_s's own comment. A path
        # confirmed too recently to plausibly have gone stale is trusted
        # regardless of accumulated failures -- the failure count above is
        # deliberately NOT reset here, so this re-evaluates on every
        # subsequent failure and fires the moment the path is old enough,
        # rather than being silenced permanently by one early skip.
        resolved = self._resolved_paths.get(pubkey_prefix)
        if resolved is not None:
            age_s = time.monotonic() - resolved.resolved_at
            if age_s < self.direct_path_reset_min_age_s:
                self._debug(
                    f"record_direct_send_result({pubkey_prefix!r}): {failures} "
                    f"failure(s) reached the reset threshold, but this path "
                    f"was only confirmed {age_s:.1f}s ago (< "
                    f"{self.direct_path_reset_min_age_s:.0f}s) -- trusting it "
                    f"a while longer rather than spending a fresh discover_path() "
                    f"burst on a path that hasn't plausibly gone stale yet."
                )
                return

        self._spawn_background_task(self._reset_stale_path(pubkey_prefix))

    async def _reset_stale_path(self, pubkey_prefix: str) -> None:
        # Local state first (irreversible for this path either way, and
        # the device round-trip below can independently fail) -- the next
        # outgoing attempt for this peer should go through discover_path()
        # again rather than retry a path already known to be dead.
        self._direct_path_failures.pop(pubkey_prefix, None)
        self._resolved_paths.pop(pubkey_prefix, None)
        self._invalidate_ack_rtt(pubkey_prefix, "stale path reset")

        contact = self._resolve_contact(pubkey_prefix)
        if contact is None:
            return
        try:
            # reset_path() mutates the library's own local contact dict
            # as a side effect before the device round-trip even resolves
            # (confirmed directly against the installed library) -- the
            # returned event, not a re-read of the contact, is what
            # actually confirms the device-side command's own outcome.
            await self._run_command(
                self._mc_ready.commands.reset_path(contact),
                "reset_path",
                self._EventType.OK,
            )
            RNS.log(
                f"{self}: reset stale cached path for {pubkey_prefix!r} "
                f"after {self.direct_path_reset_threshold}+ consecutive "
                f"full-timeout DIRECT send failures.",
                RNS.LOG_WARNING,
            )
        except Exception as exc:
            RNS.log(
                f"{self}: failed to reset stale path for {pubkey_prefix!r}: {exc}",
                RNS.LOG_WARNING,
            )
