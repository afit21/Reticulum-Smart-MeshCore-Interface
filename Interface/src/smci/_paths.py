"""Path discovery and per-peer path state: the MeshCore contact refresh and telemetry grant, discover_path with its coalescing and backoff, the stale-path detector (record_direct_send_result, _reset_stale_path -- the one place a cached path is dropped), and the per-peer estimators a path change resets: ACK RTT (Jacobson/Karels, Karn backoff), echo timing for the hop-1 abort, the QUERY round trip."""
import asyncio
import time
from typing import Optional

import RNS

from ._common import _ResolvedPath


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
                    RNS.log(
                        f"{self}: path discovered to {pubkey_prefix!r} in "
                        f"{attempt} attempt(s): out_path_len={resolved.out_path_len}.",
                        RNS.LOG_INFO,
                    )
                    await self._persist_resolved_path(contact, resolved)
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

    async def _persist_resolved_path(self, contact, resolved: _ResolvedPath) -> None:
        """docs/path_discovery_spec.md's persistence fix: a successful
        discovery is NOT written to the device's own persistent contact
        record by the firmware itself (CMD_SEND_PATH_DISCOVERY_REQ's
        handler returns before reaching the code path that would). Skip
        this and this interface's own idea of "resolved" silently
        diverges from what the official app / device flash state show."""
        try:
            await self._run_command(
                self._mc_ready.commands.change_contact_path(
                    contact, resolved.out_path_hex, path_hash_mode=resolved.out_path_hash_len - 1
                ),
                "change_contact_path",
                self._EventType.OK,
            )
        except Exception as exc:
            RNS.log(
                f"{self}: persisting discovered path to the device contact "
                f"table failed: {exc} -- this interface's own record stays "
                f"authoritative for routing regardless, but the official "
                f"app / device flash state will disagree until this is "
                f"retried.",
                RNS.LOG_WARNING,
            )

    # -- Stale cached-path detection and reset (§8) ------------------------

    def record_direct_send_result(
        self,
        pubkey_prefix: str,
        succeeded: bool,
        waited_full_timeout: bool,
        rssi: Optional[float] = None,
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
