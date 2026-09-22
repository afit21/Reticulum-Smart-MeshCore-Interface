"""Peer state and binding: the bind-frame protocol (request / response / re-request schedule), the persisted peer cache, the single registration entry point _register_peer, peer expiry, the RNS-token tables that tie destinations to peers (learned only from authenticated DIRECT receptions), routing-peer resolution and small-mesh mode."""
import asyncio
import json
import os
import random
import time
from typing import Optional

import RNS

from ._common import _RnsHeader, _PeerRecord


class _PeerStateMixin:
    def _in_small_mesh_mode(self) -> bool:
        """True when between 1 and SMALL_MESH_DIRECT_ONLY_MAX_PEERS peers
        are bound (3 since commit ef57809, "Lowered most hard coded delays
        for testing"; the module docstring's 2026-09-15 entry describes the
        original 2) -- deliberately False at exactly 0 bound peers, even
        though 0 is also under the cap: with nobody bound yet there is no DIRECT target to
        send to at all, and CHANNEL is the only way this node's very
        first peer can ever be discovered in the first place (bind frames
        themselves ride CHANNEL regardless of this mode). Re-evaluated on
        every send, not cached -- a peer expiring or a new one binding
        moves this node in or out of small-mesh mode automatically, with
        no restart and no config to touch."""
        peer_count = len(self._peers)
        return 0 < peer_count <= self.SMALL_MESH_DIRECT_ONLY_MAX_PEERS

    def _all_bound_peer_prefixes(self) -> list:
        """Every bound peer, most-recently-seen first -- deliberately
        unfiltered (no router-capability check, no cap) and independent
        of `bootstrap_direct_supplement_cap`/`path_request_direct_
        supplement_cap`, which exist to bound a *supplement* riding
        alongside a mandatory broadcast and would otherwise silently
        under-cover this mode's job of reaching every peer once CHANNEL
        is skipped entirely."""
        peers = sorted(self._peers.values(), key=lambda p: p.last_seen, reverse=True)
        return [p.pubkey_prefix for p in peers]

    def _resolve_routing_peer(self, header: _RnsHeader) -> Optional[str]:
        """docs/routing_decisions.md's "resolved path known" lookup,
        peer-attribution half: maps an outgoing packet's own
        destination-hash field to a bound peer via the opportunistic
        RNS-token tables §7 populates (peer_discovery_design.md). A *bare*
        PROOF needs the separate short-TTL correlation table -- its own
        destination-hash field is then the truncated hash of the packet it
        proves, not a stable per-peer identity (§7's "PROOF exception") --
        but a proof carried on an established Link puts the link_id there
        instead, which the token table already knows, so that table is
        consulted first for every PROOF context (audit fix, 2026-09-19).

        Resolved gap (code review, 2026-09-18): for an outgoing LRPROOF
        (`context == RNS.Packet.LRPROOF`, answering a peer's LINKREQUEST),
        `RNS.Packet.pack()` writes the *link_id* into this same on-wire
        field, not a destination hash, so it was never found in
        `_proof_correlation`'s truncated-hash keyspace and always fell
        through to broadcast+supplement even for a known, DIRECT-resolved
        peer. `_observe_incoming_rns_packet` records `link_id -> peer` in
        `_rns_token_peer` for every LINKREQUEST received DIRECT (via
        `_compute_link_id`, validated in-process against
        `RNS.Link.link_id_from_lr_packet`). The 2026-09-19 audit found the
        same gap still open for every *other* Link-carried proof
        (RESOURCE_PRF above all), so the lookup below is no longer
        context-specific."""
        if header.destination_hash is None:
            return None
        if header.packet_type == RNS.Packet.PROOF:
            # Audit fix (2026-09-19, field evidence): the LRPROOF special
            # case below was the same gap, found and fixed one context at a
            # time. ANY proof carried on an established Link puts the
            # *link_id* in this on-wire field, not the truncated hash of the
            # proved packet -- and `_observe_incoming_rns_packet` already
            # records link_id -> peer for every LINKREQUEST received DIRECT.
            # In fieldtests/raw/binaryfieldtest the same link_id was routed
            # `direct_primary` for 54 DATA packets and `small_mesh_direct_
            # all_unknown_dest` for its RESOURCE_PRF, because only LRPROOF
            # consulted the table. That mattered twice over: a RESOURCE_PRF
            # is the sender's only transfer-complete signal, and each
            # misroute also charged `_record_unknown_dest_attempt` against
            # the live Link's id, so three of them armed a 300s backoff that
            # drops the proof outright in small-mesh mode. The two keyspaces
            # cannot collide: a genuine bare-proof truncated hash is never a
            # key in `_rns_token_peer`, so falling through is unchanged.
            token_peer = self._rns_token_peer.get(header.destination_hash)
            if token_peer is not None:
                return token_peer
            entry = self._proof_correlation.get(header.destination_hash)
            if entry is None:
                return None
            peer_prefix, expiry = entry
            if time.monotonic() >= expiry:
                del self._proof_correlation[header.destination_hash]
                return None
            return peer_prefix
        return self._rns_token_peer.get(header.destination_hash)

    # -------------------------------------------------------------------
    # Peer discovery (docs/peer_discovery_design.md) -- Milestone 5
    # -------------------------------------------------------------------

    def _peer_cache_file_path(self) -> Optional[str]:
        if self.peer_cache_path:
            return self.peer_cache_path
        base = getattr(RNS, "Reticulum", None)
        base = getattr(base, "storagepath", None) if base is not None else None
        if not base:
            return None
        return os.path.join(base, "smci_peers.json")

    def _load_peer_cache(self) -> None:
        """§4 entry point 2: "loading the persisted peer cache at
        startup, once per cached entry" -- routed through the single
        entry-point function (_register_peer) exactly like every other
        way a peer can become known, per §4's single-entry-point rule.
        Each cached entry's own persisted `last_seen` is preserved
        (passed through explicitly), never refreshed to "now" -- a
        restart isn't a fresh sighting, and silently extending every
        cached peer's TTL on every restart would defeat §6's expiry."""
        path = self._peer_cache_file_path()
        if not path or not os.path.isfile(path):
            return
        try:
            with open(path, "r") as f:
                data = json.load(f)
            count = 0
            for entry in data.get("peers", []):
                prefix = str(entry.get("pubkey_prefix", "")).lower()
                if not prefix:
                    continue
                self._register_peer(
                    prefix,
                    has_upstream_rns=entry.get("has_upstream_rns"),
                    source="cache",
                    last_seen=float(entry.get("last_seen", time.time())),
                    raw_fragments=entry.get("raw_fragments"),
                )
                count += 1
            RNS.log(f"{self}: restored {count} peer(s) from cache ({path}).", RNS.LOG_INFO)
        except Exception as exc:
            RNS.log(
                f"{self}: failed to load peer cache ({path}): {exc} -- "
                f"starting with no cached peers.",
                RNS.LOG_WARNING,
            )

    def _save_peer_cache(self) -> None:
        path = self._peer_cache_file_path()
        if not path:
            return
        try:
            data = {
                "peers": [
                    {
                        "pubkey_prefix": peer.pubkey_prefix,
                        "has_upstream_rns": peer.has_upstream_rns,
                        "last_seen": peer.last_seen,
                        "raw_fragments": peer.raw_fragments,
                    }
                    for peer in self._peers.values()
                ]
            }
            tmp_path = path + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(data, f)
            os.replace(tmp_path, path)
        except Exception as exc:
            RNS.log(f"{self}: failed to save peer cache ({path}): {exc}", RNS.LOG_WARNING)

    def _register_peer(
        self, pubkey_prefix: str, has_upstream_rns: Optional[bool] = None,
        source: str = "", last_seen: Optional[float] = None,
        raw_fragments: Optional[bool] = None,
    ) -> None:
        """§4's single entry-point function -- every place a peer can
        become known calls this, and only this, so "a peer becomes known"
        never has two divergent code paths to keep in sync (the old
        design's `force_direct_path` bug, root-caused to exactly that).
        `has_upstream_rns=None` means "no signal, don't touch this field"
        -- only a call sourced from an actually-parsed bind frame may pass
        a real `True`/`False` (§2's hard rule); cache-restore also passes
        through whatever tri-state value was persisted, which is `None`
        for a peer that was never actually confirmed by a bind frame
        before this process last saved its cache.

        Per §5, also (re-)grants telemetry permission -- idempotently,
        unconditionally, every time this runs, including on cache
        restore -- since a bind-frame-gated grant is the actual filter
        that matters (peer_discovery_design.md §5's reasoning), not a
        persisted "did I already grant this" flag. Best-effort only here:
        if the MeshCore contact table hasn't caught up yet (§4's
        "progressively filled in" model -- bind frames and native contact
        adverts are independent floods with no ordering guarantee), the
        periodic contact-refresh loop is the retry path, not this call.

        Although this method itself is plain (synchronous, not a
        coroutine), it can spawn a background telemetry-grant task
        (_spawn_background_task) when a matching contact is already
        resolvable -- exactly the same threading requirement
        record_direct_send_result's own docstring documents (Milestone
        4): that only attaches correctly to this interface's own
        dedicated event loop if this method is called from a coroutine
        already running on it, which every real production call site
        (bind-frame receive, cache load during _async_setup) naturally
        is. Calling it from any other thread schedules the task on the
        wrong loop (or none at all)."""
        pubkey_prefix = pubkey_prefix.lower()
        now = last_seen if last_seen is not None else time.time()
        peer = self._peers.get(pubkey_prefix)
        changed = False
        is_new = peer is None

        if peer is None:
            peer = _PeerRecord(pubkey_prefix=pubkey_prefix, has_upstream_rns=has_upstream_rns, last_seen=now,
                               raw_fragments=raw_fragments)
            self._peers[pubkey_prefix] = peer
            changed = True
            RNS.log(f"{self}: peer bound: {pubkey_prefix!r} (source={source}).", RNS.LOG_INFO)
        else:
            peer.last_seen = now
            if has_upstream_rns is not None and peer.has_upstream_rns != has_upstream_rns:
                peer.has_upstream_rns = has_upstream_rns
                changed = True
            if raw_fragments is not None and peer.raw_fragments != raw_fragments:
                peer.raw_fragments = raw_fragments
                changed = True

        if changed:
            self._save_peer_cache()

        if is_new:
            # Milestone 6: proactively resolve a MeshCore path to a
            # freshly-bound peer rather than waiting for the first
            # outgoing send to discover one reactively -- closes half of
            # peer_discovery_design.md §7's bootstrap gap ahead of time
            # (the other half is the DIRECT-bootstrap-supplement in
            # _send_outgoing_packet), so the very first bootstrap send
            # doesn't also have to wait out a fresh discovery burst.
            # Coalesced like every other discover_path() call, and purely
            # best-effort -- a failure here is silently absorbed, since
            # the reactive path in _send_direct_packet/_send_direct_
            # supplement will simply try again when real traffic needs it.
            self._spawn_background_task(self._discover_path_after_bind(pubkey_prefix))

        contact = self._resolve_contact(pubkey_prefix)
        if contact is not None:
            self._spawn_background_task(self._grant_telemetry_permission_if_needed(contact))

    async def _discover_path_after_bind(self, pubkey_prefix: str) -> None:
        """Milestone 6's proactive discovery on bind, plus one retry
        (2026-09-18 night). The first attempt races the peer's own
        telemetry grant: the node that received our bind REQUEST grants us
        permission while registering us, but the node that sent the
        REQUEST only registers (and grants) us once our RESPONSE arrives --
        up to `bind_response_jitter_max_s` later -- so its own discovery of
        us, fired the instant it saw our REQUEST, is denied by the firmware
        and lands in path-discovery backoff with nothing to retry it until
        real traffic needs the path. Seen as "DIRECT paths never resolved"
        in the simulated-mesh scenarios and as the M5 field note that "the
        next send" is what recovers. One more attempt after the response
        window, with that first denied round's backoff cleared, costs one
        REQ and settles the race in both directions."""
        resolved = await self._discover_path_coalesced(pubkey_prefix)
        if resolved is not None or self.detached or not self.online:
            return
        # Audit fix (2026-09-19): retried a bounded number of times, not once,
        # and each round refreshes contacts first. There are TWO races here,
        # not one. The telemetry-grant race above is settled by waiting out
        # the bind-response window -- but a peer can also bind before its
        # own ADVERT has reached this node at all (bind frames ride CHANNEL
        # and take one hop; an advert has to flood the whole chain), and then
        # `discover_path` bails with "peer is not a known contact" and
        # nothing retries it until real traffic needs the path. That is
        # exactly what kept the suite's only 2-repeater scenario from ever
        # running: contacts and bind both succeeded, `path_req_sent` stayed
        # 0 on every radio, and both nodes logged "peer is not a known
        # contact" twice before giving up. Each round costs one REQ only if
        # it gets far enough to send one.
        for round_number in range(1, self.POST_BIND_DISCOVERY_ROUNDS + 1):
            await asyncio.sleep(self.bind_response_jitter_max_s + 5.0)
            if self.detached or not self.online:
                return
            if pubkey_prefix not in self._peers or pubkey_prefix in self._resolved_paths:
                return
            if self._resolve_contact(pubkey_prefix) is None:
                # The advert has not landed yet -- ask the radio again rather
                # than burning this round on a contact we know we don't have.
                try:
                    await self._refresh_contacts_and_grant_telemetry()
                except Exception as exc:
                    self._debug(f"discover_path({pubkey_prefix!r}): post-bind contact refresh failed: {exc}")
                if pubkey_prefix in self._resolved_paths:
                    return
            self._path_discovery_failures.pop(pubkey_prefix, None)
            self._path_discovery_backoff_until.pop(pubkey_prefix, None)
            self._debug(
                f"discover_path({pubkey_prefix!r}): post-bind retry {round_number}/"
                f"{self.POST_BIND_DISCOVERY_ROUNDS} -- the first attempt likely raced the peer's "
                f"telemetry grant or its advert."
            )
            if await self._discover_path_coalesced(pubkey_prefix) is not None:
                return

    def _touch_peer_seen(self, pubkey_prefix: str) -> None:
        """A lightweight last-seen refresh for a peer ALREADY in the
        registry (§6: "no traffic of any kind" resets the TTL, including
        RNS traffic attributed to a peer via §7's token learning) --
        deliberately NOT routed through _register_peer, which would
        create a brand-new peer record from RNS traffic alone. Only a
        bind frame (or a cache-restored entry) may ever create a peer;
        this can only refresh one that already exists. Memory-only, no
        immediate disk write -- the periodic TTL sweep persists
        accumulated last_seen changes at its own cadence instead, so an
        active peer doesn't trigger a disk write on every single packet."""
        peer = self._peers.get(pubkey_prefix)
        if peer is not None:
            peer.last_seen = time.time()

    async def _peer_discovery_bootstrap(self) -> None:
        """§3/§4: exactly one bootstrap REQUEST, unconditionally, at
        every process start, regardless of what the peer cache restored
        -- the old design's own regression (cache-restore silencing this
        node's own first-boot advertisement) made structurally impossible
        here by never gating this call on any cached/loaded state at all.
        After that, an optional slow repeat while still below
        peer_discovery_target_peers -- a judgment call with no field data
        behind the specific numbers, per the doc's own flag."""
        await self._send_bind_frame(self.BIND_TYPE_REQUEST)
        try:
            interval_s = self._next_rerequest_interval_s(None)
            while not self.detached:
                await asyncio.sleep(self._loop_interval_s(interval_s, "peer_discovery_rerequest_interval"))
                if self.detached:
                    break
                if len(self._peers) >= self.peer_discovery_target_peers:
                    interval_s = self.peer_discovery_rerequest_interval_s
                    continue
                await self._send_bind_frame(self.BIND_TYPE_REQUEST)
                interval_s = self._next_rerequest_interval_s(interval_s)
        except asyncio.CancelledError:
            pass

    def _next_rerequest_interval_s(self, current_s: Optional[float]) -> float:
        """Bind REQUEST repeat schedule while below the target peer count:
        peer_discovery_rerequest_initial, doubling, capped at
        peer_discovery_rerequest_interval (see the latter's comment)."""
        cap = self.peer_discovery_rerequest_interval_s
        if current_s is None:
            return min(self.peer_discovery_rerequest_initial_s, cap)
        return min(current_s * 2.0, cap)

    async def _send_bind_frame(self, frame_type: int) -> None:
        attempt = next(self._bind_attempt_counter) & 0xFF
        frame = self._encode_bind_frame(frame_type, attempt)
        await self._pre_transmit_gate(frame)
        try:
            await self._run_command(
                self._mc_ready.commands.send_chan_msg(self.channel_idx, frame),
                "send_chan_msg(bind)",
                self._EventType.OK,
            )
            self.txb += len(frame)
            self._debug(
                f"sent bind frame type={frame_type} attempt={attempt} "
                f"cap={self._bind_capability():#04x}."
            )
        except Exception as exc:
            RNS.log(f"{self}: bind-frame send failed: {exc}", RNS.LOG_WARNING)

    async def _respond_to_bind_request(self, requester_prefix: str) -> None:
        """§3's collision/storm-avoidance recommendation: per-responder
        randomized jitter before transmitting (collision/half-duplex-deaf-
        repeater spacing, not suppression -- every well-formed REQUEST
        still gets answered eventually), plus a separate, much longer
        global minimum re-response interval, which IS real suppression --
        this node's own capability hasn't changed just because a second
        REQUEST arrived shortly after the first RESPONSE went out."""
        delay = random.uniform(self.bind_response_jitter_min_s, self.bind_response_jitter_max_s)
        await asyncio.sleep(delay)
        if self.detached or not self.online:
            return

        now = time.monotonic()
        if (
            self._last_bind_response_sent is not None
            and now - self._last_bind_response_sent < self.bind_response_min_interval_s
        ):
            self._debug(
                f"skipping bind RESPONSE to {requester_prefix!r} -- still "
                f"within this node's own min re-response interval."
            )
            return

        await self._send_bind_frame(self.BIND_TYPE_RESPONSE)
        self._last_bind_response_sent = now

    def _handle_incoming_bind_frame(self, marker_and_body: str) -> None:
        if not self.peer_discovery_enabled:
            return
        try:
            frame = self._decode_bind_frame(marker_and_body)
        except ValueError as exc:
            self._debug(f"discarding malformed bind frame: {exc}")
            return

        own_prefix = self._own_pubkey_prefix()
        if own_prefix is not None and frame.pubkey_prefix == own_prefix:
            return  # this node's own bind frame, echoed back somehow -- not a peer

        has_upstream = bool(frame.cap & self.BIND_CAP_HAS_UPSTREAM_RNS)
        raw_capable = bool(frame.cap & self.BIND_CAP_RAW_FRAGMENTS)
        self._register_peer(
            frame.pubkey_prefix, has_upstream_rns=has_upstream, source="bind_frame", raw_fragments=raw_capable,
        )

        if frame.type == self.BIND_TYPE_REQUEST:
            self._spawn_background_task(self._respond_to_bind_request(frame.pubkey_prefix))

    async def _peer_ttl_sweep_loop(self) -> None:
        """§6: a peer (and any RNS-token bindings linked to it, §7) is
        dropped after no traffic of any kind for peer_ttl_s. Deliberately
        does NOT touch the MeshCore device's own contact/route table
        (independently owned by the firmware) or the telemetry grant
        (revoking needs a stronger, explicit negative signal than mere
        silence, per §6's own reasoning -- an unreachable peer isn't
        currently issuing anything for the grant to guard against)."""
        try:
            while not self.detached:
                await asyncio.sleep(self._loop_interval_s(self.peer_ttl_sweep_interval_s, "peer_ttl_sweep_interval"))
                if self.detached:
                    break
                now = time.time()
                expired = [
                    prefix for prefix, peer in self._peers.items()
                    if now - peer.last_seen > self.peer_ttl_s
                ]
                for prefix in expired:
                    del self._peers[prefix]
                    self._forget_peer_state(prefix)
                    RNS.log(
                        f"{self}: peer {prefix!r} expired (no traffic for "
                        f"{self.peer_ttl_s:.0f}s).",
                        RNS.LOG_INFO,
                    )
                if expired or self._peers:
                    # Also persists any last_seen drift accumulated since
                    # the previous sweep via _touch_peer_seen's
                    # memory-only updates.
                    self._save_peer_cache()
        except asyncio.CancelledError:
            pass

    def _forget_peer_state(self, pubkey_prefix: str) -> None:
        """Called when a peer is dropped on TTL expiry (`_peer_ttl_sweep_
        loop`) -- clears every per-peer dict keyed by `pubkey_prefix`
        anywhere in this interface, not just the RNS-token tables §7
        itself documents. Code-review fix: the original version of this
        method (named `_forget_rns_tokens_for_peer`, scoped to exactly
        what that name says) left `_resolved_paths`,
        `_path_discovery_failures`, `_path_discovery_backoff_until`,
        `_direct_path_failures`, and `_pending_path_discoveries` all
        untouched on peer expiry -- on a mesh with transient/mobile
        peers, each one that binds, goes stale, and gets swept leaves a
        permanent, never-cleaned entry in up to five other dicts,
        unbounded growth over a long-running unattended field radio, and
        `_stats_loop`'s own `resolved_paths`/`peers_in_discovery_backoff`
        counts permanently overstating live state."""
        stale = [dh for dh, prefix in self._rns_token_peer.items() if prefix == pubkey_prefix]
        for dh in stale:
            del self._rns_token_peer[dh]
        stale_proof = [
            h for h, (prefix, _expiry) in self._proof_correlation.items() if prefix == pubkey_prefix
        ]
        for h in stale_proof:
            del self._proof_correlation[h]

        self._resolved_paths.pop(pubkey_prefix, None)
        self._ack_rtt.pop(pubkey_prefix, None)
        self._clear_peer_path_stats(pubkey_prefix, "peer state forgotten")
        for k in [k for k in self._resumable_sends if k[0] == pubkey_prefix]:
            del self._resumable_sends[k]
        self._path_discovery_failures.pop(pubkey_prefix, None)
        self._path_discovery_backoff_until.pop(pubkey_prefix, None)
        self._direct_path_failures.pop(pubkey_prefix, None)
        # Deliberately NOT cancelled if still in flight -- _discover_path_
        # coalesced's own try/finally already pops this same entry once
        # discover_path() completes on its own; just dropping this
        # interface's reference to it here is enough to stop treating an
        # expired peer as having a resolved (or pending) path, without
        # risking an InvalidStateError from cancelling a future that
        # coroutine still intends to resolve normally.
        self._pending_path_discoveries.pop(pubkey_prefix, None)
        # Alpha 0.1.5 (2b): a held complete report for this sender.
        self._cancel_sender_report(pubkey_prefix)
        self._raw_part_arrivals.pop(pubkey_prefix, None)   # item 5
        self._path_boards.pop(pubkey_prefix, None)   # alpha 0.1.6 item 1: the path scoreboard
        self._pending_link_proofs.pop(pubkey_prefix, None)   # alpha 0.1.6 item 2

    # -- Opportunistic RNS-token learning (§7) -----------------------------

    def _resolve_raw_src(self, src_prefix_hex: str) -> Optional[str]:
        """The bound peer a raw fragment's RAW_SRC_PREFIX_BYTES-byte source
        prefix names (phase 3 M3, 2026-09-20): exactly one bound peer whose
        6-byte prefix starts with it, else None (no match, or two bound
        peers sharing the short prefix -- both dropped by the caller, the
        ambiguous case logged once per prefix)."""
        short = (src_prefix_hex or "").lower()
        if len(short) < self.RAW_SRC_PREFIX_BYTES * 2:
            return None
        short = short[: self.RAW_SRC_PREFIX_BYTES * 2]
        matches = [p for p in self._peers if p.startswith(short)]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1 and short not in self._raw_src_ambiguous_logged:
            self._raw_src_ambiguous_logged.add(short)
            RNS.log(f"{self}: raw fragments from source prefix {short} are ambiguous between bound peers {matches} -- dropped.", RNS.LOG_WARNING)
        return None

    def _raw_src_ambiguous(self, peer_prefix: str) -> bool:
        """Whether another bound peer shares `peer_prefix`'s short raw
        source prefix -- then raw fragments from this node would be
        ambiguous at the far end, so the sender uses text (M3)."""
        short = peer_prefix[: self.RAW_SRC_PREFIX_BYTES * 2]
        own = (self._own_pubkey_prefix() or "")[: self.RAW_SRC_PREFIX_BYTES * 2]
        # The far end resolves OUR prefix against ITS bound peers; the best
        # this side can check is that no other bound peer of ours shares
        # our short prefix (the two nodes' peer sets coincide in a small
        # mesh) -- and that the target's own short prefix is unique here.
        others = [p for p in self._peers if p != peer_prefix and p.startswith(short)]
        own_clash = [p for p in self._peers if own and p.startswith(own)]
        return bool(others) or bool(own_clash)

    def _canonical_peer_prefix(self, raw_prefix: str) -> Optional[str]:
        """Resolves a MeshCore-native pubkey prefix (e.g. `pubkey_prefix`
        off a CONTACT_MSG_RECV event) to this interface's own canonical
        6-byte (12-hex-char) peer key, via a full contact lookup. Native
        prefixes handed to this interface by different meshcore events
        aren't guaranteed to share this project's own bind-frame-protocol
        prefix length (§1 fixes that length only for THIS interface's own
        control frames) -- so raw prefix strings of possibly-differing
        lengths are never compared or used as dict keys directly against
        `_peers`/`_rns_token_peer`/`_proof_correlation`; every lookup goes
        through this canonicalization first."""
        if not raw_prefix:
            return None
        contact = self._resolve_contact(raw_prefix)
        if contact is None:
            self._debug(
                f"_canonical_peer_prefix({raw_prefix!r}): no resolvable contact "
                f"-- token learning skipped for this receive. contacts_known="
                f"{len(self._mc.contacts) if self._mc is not None else 'n/a'}."
            )
            return None
        full_key = str(contact.get("public_key", "")).lower()
        if len(full_key) < self.BIND_PUBKEY_PREFIX_BYTES * 2:
            self._debug(
                f"_canonical_peer_prefix({raw_prefix!r}): resolved contact's own "
                f"public_key {full_key!r} is shorter than "
                f"{self.BIND_PUBKEY_PREFIX_BYTES * 2} chars -- token learning skipped."
            )
            return None
        return full_key[: self.BIND_PUBKEY_PREFIX_BYTES * 2]

    def _is_local_destination(self, destination_hash: bytes) -> bool:
        """Whether `destination_hash` is served by this node's own RNS
        (alpha 0.1.7, item 3): a destination registered in this process
        (`RNS.Transport.destinations_map`, IN destinations only --
        `Transport.register_destination`), or one a shared-instance client
        of this rnsd registered -- RNS's own test for that is a path_table
        entry at zero hops (`Transport.inbound`'s `for_local_client`) or
        received on a local client interface (`is_local_client_interface`;
        the entry indices IDX_PT_HOPS 2 / IDX_PT_RVCD_IF 5 are module
        constants in RNS/Transport.py). The field's `d4c70c4b` was the
        second kind: MeshChat's LXMF delivery destination, registered in
        MeshChat's process, not in rnsd's."""
        transport = getattr(RNS, "Transport", None)
        if transport is None or not destination_hash:
            return False
        try:
            if destination_hash in (getattr(transport, "destinations_map", None) or {}):
                return True
            entry = (getattr(transport, "path_table", None) or {}).get(destination_hash)
            if entry is None:
                return False
            if entry[2] == 0:
                return True
            is_local_if = getattr(transport, "is_local_client_interface", None)
            return bool(is_local_if(entry[5])) if callable(is_local_if) else False
        except Exception:
            return False

    def _learn_rns_token(self, token: bytes, sender_peer_prefix: str) -> None:
        """The one place `_rns_token_peer` grows (audit fix, 2026-09-19 --
        added so the capacity bound cannot be bypassed by a future call
        site, in the spirit of `_register_peer` being the single entry point
        for peer state). Re-learning an existing token also refreshes its
        position, so the eviction below targets genuinely idle tokens.

        Alpha 0.1.7 (item 3): a token for one of this node's own
        destinations is never learned -- this node never routes to itself,
        and the 2026-09-22 desktop learned `d4c70c4b -> laptop` seven times
        from inbound LXMF DATA to its own delivery destination."""
        if self._is_local_destination(token):
            self._debug(
                f"_learn_rns_token: {token.hex()} is one of this node's own destinations -- "
                f"not mapped to {sender_peer_prefix!r}."
            )
            return
        self._rns_token_peer.pop(token, None)
        self._rns_token_peer[token] = sender_peer_prefix
        while len(self._rns_token_peer) > self.RNS_TOKEN_PEER_MAX_KEYS:
            evicted, _prefix = self._rns_token_peer.popitem(last=False)
            self._debug(
                f"RNS token table at capacity ({self.RNS_TOKEN_PEER_MAX_KEYS}) -- evicting the "
                f"least-recently-learned token {evicted.hex()[:12]}."
            )

    def _observe_raw_received_packet(self, data: bytes, claimed_peer_prefix: Optional[str]) -> None:
        """Field-diagnosed (2026-09-19, zero-hop image transfer, both
        captures): a packet received as raw fragments never had its
        outgoing PROOF attributed to a peer -- `_proof_correlation` is
        only filled by `_observe_incoming_rns_packet`, which the raw path
        skips because a raw frame's source prefix is unauthenticated. The
        proof (its destination-hash field is the proved packet's truncated
        hash, in no table) then fell through to the unknown-destination
        branch: DIRECT-to-all in small-mesh mode, a CHANNEL broadcast
        beyond three peers -- the transport raw exists to avoid.

        Only when the claimed prefix is a peer this node has already bound
        (bind frame) AND holds a resolved DIRECT path to. Threat model: a
        spoofer who claims a bound peer's prefix can at worst misdirect one
        PROOF to that peer -- a PROOF is cryptographically bound to the
        packet it proves, so it is useless to anyone else, and RNS simply
        re-sends the data. That is no wider than the fallback's own
        behaviour (small-mesh DIRECT-to-all already reaches every bound
        peer, the spoofer included) and strictly narrower than a broadcast.

        Widened the same afternoon (field test `fieldtests/raw/Alpha0.1.2/
        *T1250*`): with the same guard, the whole observe step runs -- RNS
        tokens included. The laptop received the desktop's path-response
        ANNOUNCEs for d4c70c4b five times as raw fragments (a 3-fragment
        announce always goes raw now), learned nothing from any of them,
        bootstrapped three DATA sends, then hit the 300 s unknown-
        destination backoff and dropped 17 packets to a destination that
        was answering every one. A token from a raw frame steers *data*
        to the claimed peer, which is why it was withheld -- but that
        peer is one this node already routes to on the strength of an
        authenticated bind frame, and a text-frame ANNOUNCE from the same
        peer teaches the same token today. The residual exposure is a
        third party who knows a bound peer's 6-byte prefix steering one
        destination's traffic to that (legitimate) peer, a nuisance
        bounded by the token's own expiry, against a default that dropped
        real traffic for five minutes."""
        if claimed_peer_prefix is None:
            return
        if claimed_peer_prefix not in self._peers or claimed_peer_prefix not in self._resolved_paths:
            self._debug(
                f"_observe_raw_received_packet: {claimed_peer_prefix!r} is not a bound peer with a "
                f"resolved path -- not trusting a raw frame's source claim; nothing learned."
            )
            return
        self._observe_incoming_rns_packet(data, claimed_peer_prefix)

    def _token_learnable_from(self, header: _RnsHeader) -> bool:
        """Which inbound packets teach `destination_hash -> sender` (alpha
        0.1.7, item 3). An ANNOUNCE (any context; a path response is an
        ANNOUNCE with context PATH_RESPONSE, `RNS.Destination.announce`)
        names a destination that lives in the sender's direction -- the
        same inference RNS's own path table makes from it. A packet carried
        on a Link puts the link_id in that field, and a Link is one
        bidirectional session between two nodes, so the peer that delivered
        it is the peer this node's own packets on that Link go to. Every
        other packet -- DATA to a SINGLE destination, a LINKREQUEST (its
        link_id is learned separately below), a PLAIN path request -- is
        addressed TO a destination that is either this node's own or lies
        beyond some other interface; "outgoing to this hash -> this peer"
        is wrong either way, and on a transport node it overwrote the
        announce-learned token. The 2026-09-22 desktop learned its own LXMF
        delivery destination from every inbound LXMF packet this way."""
        if header.packet_type == RNS.Packet.ANNOUNCE or header.context == RNS.Packet.PATH_RESPONSE:
            return True
        return header.destination_type == RNS.Destination.LINK

    def _observe_incoming_rns_packet(self, data: bytes, sender_peer_prefix: Optional[str]) -> None:
        """§7: populated only from the DIRECT receive path -- a CHANNEL
        "R" frame carries no sender pubkey at all
        (wire_format_design.md), so reliable peer attribution is
        structurally only available here, not for CHANNEL-received
        traffic. Only learns from an already-bound peer (a token from an
        unbound sender would have nothing in `_peers` for §6's TTL/expiry
        bookkeeping to ever clean up)."""
        if sender_peer_prefix is None:
            self._debug("_observe_incoming_rns_packet: no canonical peer prefix -- nothing to learn from.")
            return
        if sender_peer_prefix not in self._peers:
            self._debug(
                f"_observe_incoming_rns_packet: {sender_peer_prefix!r} resolved to a "
                f"contact but isn't a bound peer (_peers={list(self._peers.keys())}) "
                f"-- token learning skipped."
            )
            return

        header = self._parse_rns_header(data)
        if header is None or header.destination_hash is None:
            self._debug(
                f"_observe_incoming_rns_packet: could not parse a destination_hash "
                f"from this {len(data)}-byte payload from {sender_peer_prefix!r} "
                f"(header={header!r}) -- token learning skipped."
            )
            return

        self._touch_peer_seen(sender_peer_prefix)

        if header.packet_type == RNS.Packet.PROOF:
            # Phase 1 (2026-09-20): whatever else this proof means, a bare
            # send keyed by its destination field (a link_id, or a plain
            # DATA's truncated hash) has been answered by this peer.
            self._signal_send_answered(header.destination_hash, "DIRECT PROOF", sender_peer_prefix)
            # Code review (2026-09-18): the one PROOF whose destination
            # field IS worth learning from -- an LRPROOF answering a
            # LINKREQUEST this node sent carries the link_id, a stable
            # identity for that Link's lifetime, and proves the destination
            # it was requested for is reachable through this peer. Learn
            # both tokens and clear that destination's unknown-destination
            # backoff (previously three good Links to the same destination
            # counted as three "failures" -- see the module docstring's
            # 2026-09-18 review entry).
            delivered = self._pending_dest_proofs.pop(header.destination_hash, None)
            if delivered is not None:
                proved_dest, _expiry = delivered
                self._learn_rns_token(proved_dest, sender_peer_prefix)
                self._clear_unknown_dest_backoff(proved_dest)
                self._debug(
                    f"_observe_incoming_rns_packet: PROOF from {sender_peer_prefix!r} for a bootstrap "
                    f"DATA send to {proved_dest.hex()} -- destination is reachable through this peer; "
                    f"token learned, unknown-destination backoff cleared "
                    f"(rns_tokens_learned now {len(self._rns_token_peer)})."
                )
                return
            pending = self._pending_link_requests.pop(header.destination_hash, None)
            if pending is not None:
                requested_dest, _expiry = pending
                self._learn_rns_token(header.destination_hash, sender_peer_prefix)
                self._learn_rns_token(requested_dest, sender_peer_prefix)
                self._clear_unknown_dest_backoff(requested_dest)
                self._debug(
                    f"_observe_incoming_rns_packet: LRPROOF from {sender_peer_prefix!r} answers "
                    f"this node's LINKREQUEST to {requested_dest.hex()} -- learned tokens for both "
                    f"the destination and link_id {header.destination_hash.hex()} "
                    f"(rns_tokens_learned now {len(self._rns_token_peer)})."
                )
                return
            # §7's "PROOF exception": a PROOF's own destination-hash field
            # is the truncated hash of the packet it proves, never a
            # stable per-peer identity -- never recorded in the normal
            # token table no matter how much traffic is observed.
            self._debug(
                f"_observe_incoming_rns_packet: {sender_peer_prefix!r}'s packet is a "
                f"PROOF -- destination_hash {header.destination_hash.hex()} is its "
                f"own truncated hash, not learned as a normal token (§7 exception)."
            )
            return

        if self._token_learnable_from(header):
            self._learn_rns_token(header.destination_hash, sender_peer_prefix)
            self._debug(
                f"_observe_incoming_rns_packet: learned token "
                f"{header.destination_hash.hex()} -> {sender_peer_prefix!r} "
                f"(rns_tokens_learned now {len(self._rns_token_peer)})."
            )
            # A real token learned for this exact destination proves it IS
            # reachable through this peer after all -- clear any backoff
            # immediately rather than waiting for it to expire on its own.
            self._clear_unknown_dest_backoff(header.destination_hash)
        else:
            self._debug(
                f"_observe_incoming_rns_packet: {sender_peer_prefix!r}'s packet (type {header.packet_type}) "
                f"is addressed TO {header.destination_hash.hex()}, which is this node's or beyond another "
                f"interface -- nothing learned from its destination field (item 3, alpha 0.1.7)."
            )

        if header.packet_type == RNS.Packet.LINKREQUEST:
            # Code review (2026-09-18): this node's own LRPROOF answering
            # this request will carry the link_id in its destination field
            # (RNS.Packet.pack()), so learning link_id -> peer here is what
            # lets _resolve_routing_peer send that proof DIRECT-primary.
            link_id = self._compute_link_id(data)
            if link_id is not None:
                self._learn_rns_token(link_id, sender_peer_prefix)
                # Alpha 0.1.6 (item 2): the peer asked again -- any LRPROOF
                # still pending for its earlier link is pure airtime.
                self._supersede_link_proofs(sender_peer_prefix, link_id)
                self._debug(
                    f"_observe_incoming_rns_packet: LINKREQUEST from {sender_peer_prefix!r} -- "
                    f"learned link_id {link_id.hex()} -> {sender_peer_prefix!r} for the LRPROOF reply."
                )

        truncated_hash = self._compute_truncated_hash(data, header.header_type)
        if truncated_hash is not None:
            self._proof_correlation[truncated_hash] = (
                sender_peer_prefix, time.monotonic() + self.proof_correlation_ttl_s
            )
