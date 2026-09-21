"""Routing decisions and the two funnels: process_outgoing / the outgoing worker (queue hygiene, duplicate suppression, expiry), _send_outgoing_packet and _dispatch_outgoing_packet (LRPROOF delay, path-request and announce rate limits, the local announce cache, DIRECT-primary versus broadcast-plus-supplement versus small-mesh DIRECT-to-all, the unknown-destination backoff), the CHANNEL sends, the supplement targets and spacing, the receive callbacks and frame demux, and process_incoming."""
import asyncio
import queue
import random
import time
import traceback
from typing import Optional

import RNS

from ._common import _FrameHeader, _RnsHeader, PRIORITY_NORMAL


class _RoutingMixin:
    def _spawn_background_task(self, coro) -> "asyncio.Task":
        task = asyncio.ensure_future(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        task.add_done_callback(self._log_background_task_exception)
        return task

    def _log_background_task_exception(self, task: "asyncio.Task") -> None:
        """Code-review fix: `_outgoing_worker` wraps its own synchronously-
        awaited `_send_outgoing_packet` call in try/except and logs any
        exception via `RNS.log` -- but every fire-and-forget send this
        interface spawns instead (`_send_direct_packet`,
        `_send_direct_supplement`, the bootstrap `_discover_path_coalesced`
        call from `_register_peer`, etc.) only ever reaches this
        interface's own observability surface if it happens to guard its
        own internal awaits already; an exception that does escape one is
        otherwise only ever surfaced via asyncio's own default "Task
        exception was never retrieved" warning at garbage-collection time
        -- invisible to `RNS.log`, to the periodic `_stats_loop` snapshot,
        and to anyone monitoring this interface the way CLAUDE.md's
        observability requirement expects. This done-callback (attached to
        every task `_spawn_background_task` creates) closes that gap."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            RNS.log(f"{self}: background task raised an unhandled exception: {exc}", RNS.LOG_ERROR)
            RNS.log(
                "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                RNS.LOG_ERROR,
            )

    # -------------------------------------------------------------------
    # Outgoing (RNS core -> this interface)
    # -------------------------------------------------------------------

    def process_outgoing(self, data):
        # Called synchronously by RNS core on RNS's own thread -- must
        # never block. Per docs/reliability_engine_design.md's sync/async
        # bridge notes, this does only a plain thread-safe queue.put and
        # returns immediately; _outgoing_worker (running on this
        # interface's own event loop) does the actual encode/send. The
        # RNS header is parsed once, here, and carried through the queue
        # item -- both the priority decision (now) and the retry-extra
        # decision (_send_outgoing_packet, once this item is dequeued)
        # read the same parse rather than redoing it.
        if not self.online or self.detached:
            return
        raw = bytes(data)
        header = self._parse_rns_header(raw)
        self._note_link_closed(header)
        priority = self._priority_tier(header)
        # Field fix (2026-09-18 evening): identical bytes already queued or
        # in flight -> drop. RNS's Resource layer re-requests parts every
        # ~27s while the earlier copy is still waiting on the duty-cycle
        # limiter; the page-load capture queued 26 RESOURCE packets for 12
        # distinct payloads. The receiver would dedup them anyway.
        # Field fix (2026-09-19 evening session): the suppression above is
        # right in the common case and wrong when the in-flight entry is
        # STUCK. Measured deadlock, both captures agreeing: resource part
        # `ca6b3d36db27` was transmitted at 16:48:06 and fully delivered to
        # the peer's RNS at 16:48:25 -- but the peer's RNS did not credit it
        # and re-requested it, and this side then refused SEVEN consecutive
        # re-sends (16:49:55 through 16:52:53) because the original send's
        # in-flight entry never cleared: its completion checks kept timing
        # out, so `_release_inflight_when_done` never fired. 178 seconds --
        # 42% of that 442-second transfer -- were spent with RNS correctly
        # asking and this interface correctly-but-fatally declining, until an
        # unrelated retry path finally sent a fresh copy. So: after
        # `outgoing_duplicate_suppress_limit` consecutive suppressions of the
        # same bytes, let the packet through and start a fresh in-flight
        # entry. RNS only re-requests a Resource part it believes it lacks,
        # so by the third ask its belief should win over ours.
        inflight_key = RNS.Identity.truncated_hash(raw)
        with self._outgoing_inflight_lock:
            duplicate = inflight_key in self._outgoing_inflight
            if duplicate:
                n = self._outgoing_duplicate_suppressed.get(inflight_key, 0) + 1
                self._outgoing_duplicate_suppressed[inflight_key] = n
                if n >= max(1, self.outgoing_duplicate_suppress_limit):
                    # Force it through: replace the stuck entry with a fresh one.
                    duplicate = False
                    self._outgoing_duplicate_suppressed.pop(inflight_key, None)
                    self._outgoing_inflight[inflight_key] = time.monotonic()
                    forced = n
                else:
                    forced = 0
            else:
                forced = 0
                self._outgoing_duplicate_suppressed.pop(inflight_key, None)
                self._outgoing_inflight[inflight_key] = time.monotonic()
        if duplicate:
            self._outgoing_dropped_total += 1
            self._capture_outgoing(header, raw, "duplicate_in_flight")
            self._debug(
                f"dropping outgoing packet ({len(raw)} bytes, "
                f"{self._payload_correlation_hash(raw)}) -- identical bytes are already "
                f"queued or in flight."
            )
            return
        if forced:
            RNS.log(
                f"{self}: re-sending a packet ({len(raw)} bytes, "
                f"{self._payload_correlation_hash(raw)}) that was suppressed {forced} time(s) as "
                f"already in flight -- RNS keeps asking for it, so the in-flight entry is "
                f"treated as stuck and replaced.",
                RNS.LOG_WARNING,
            )
        seq = next(self._outqueue_seq)
        try:
            self._outqueue.put_nowait((priority, seq, raw, header, time.monotonic(), inflight_key))
        except queue.Full:
            self._release_inflight(inflight_key)
            self._outgoing_dropped_total += 1
            RNS.log(
                f"{self}: dropping outgoing packet ({len(data)} bytes) -- "
                f"outgoing queue is full ({self.OUTQUEUE_MAXSIZE} items "
                f"already queued).",
                RNS.LOG_WARNING,
            )

    async def _outgoing_worker(self):
        """Drains self._outqueue via a thread-pool executor, so the
        queue's blocking get() never blocks this event loop while
        waiting for the next packet -- the other half of the
        process_outgoing split above. Milestone 3 upgraded this from a
        plain FIFO to the two-tier PriorityQueue docs/
        reliability_engine_design.md §3 specifies, without changing this
        drain pattern. Exits cleanly on _OUTQUEUE_SHUTDOWN_SENTINEL,
        which detach() places in the queue (a plain task-cancel can't
        interrupt the blocking get() itself, since it runs on a separate
        executor thread)."""
        loop = asyncio.get_running_loop()
        while True:
            # Bounded wait (2026-09-18 night): an unbounded queue.get() here
            # parks a non-daemon executor thread forever if this interface
            # is never detach()ed (a test whose setUp failed, a script that
            # exits without detaching), and Python joins that thread at
            # interpreter exit -- the process hangs. One second bounds it.
            try:
                priority, seq, data, header, enqueued_at, inflight_key = await loop.run_in_executor(
                    None, self._outqueue.get, True, 1.0,
                )
            except queue.Empty:
                if self.detached:
                    return
                continue
            if data is None:
                self._outqueue.task_done()
                return
            spawned: list = []
            try:
                # Field fix (2026-09-18 evening): outgoing_max_age -- see
                # that config's own comment. ANNOUNCE never expires, nor do
                # Resource data parts (RNS's Resource layer owns those).
                expires_at = None
                if self.outgoing_max_age_s > 0 and not (
                    header is not None and (
                        header.packet_type == RNS.Packet.ANNOUNCE
                        or header.context == RNS.Packet.RESOURCE
                    )
                ):
                    expires_at = enqueued_at + self.outgoing_max_age_s
                plain_proof = self._plain_proof(header)
                if plain_proof and self.proof_max_age_s > 0:
                    # Phase 1 (2026-09-20): see proof_max_age.
                    proof_deadline = enqueued_at + self.proof_max_age_s
                    expires_at = proof_deadline if expires_at is None else min(expires_at, proof_deadline)
                if self._link_closed(header):
                    self._outgoing_dropped_total += 1
                    self._capture_outgoing(header, data, "link_closed")
                    RNS.log(
                        f"{self}: dropping outgoing packet ({len(data)} bytes) -- its Link "
                        f"{header.destination_hash.hex()} was closed "
                        f"{time.monotonic() - self._closed_links[header.destination_hash]:.0f}s ago.",
                        RNS.LOG_DEBUG,
                    )
                elif self._expired(expires_at):
                    self._outgoing_dropped_total += 1
                    self._capture_outgoing(header, data, "proof_expired_in_queue" if plain_proof else "expired_in_queue")
                    RNS.log(
                        f"{self}: dropping outgoing {'PROOF' if plain_proof else 'packet'} ({len(data)} bytes) -- sat "
                        f"{time.monotonic() - enqueued_at:.0f}s in the outgoing queue, past "
                        + (f"proof_max_age={self.proof_max_age_s:.0f}s." if plain_proof and self.proof_max_age_s > 0
                           else f"outgoing_max_age={self.outgoing_max_age_s:.0f}s."),
                        RNS.LOG_WARNING,
                    )
                else:
                    await self._send_outgoing_packet(data, header, expires_at=expires_at, spawned=spawned)
            except Exception as exc:
                RNS.log(
                    f"{self}: unexpected error sending an outgoing packet: {exc}",
                    RNS.LOG_ERROR,
                )
            finally:
                self._outqueue.task_done()
                self._release_inflight_when_done(inflight_key, spawned)

    def _release_inflight(self, inflight_key) -> None:
        if inflight_key is None:
            return
        with self._outgoing_inflight_lock:
            self._outgoing_inflight.pop(inflight_key, None)
            self._outgoing_duplicate_suppressed.pop(inflight_key, None)

    def _release_inflight_when_done(self, inflight_key, spawned: list) -> None:
        """Releases a packet's `_outgoing_inflight` entry once every send
        task `_dispatch_outgoing_packet` spawned for it has finished --
        success or failure -- so a genuinely failed copy can be re-sent
        by RNS immediately, while a copy still working through the queue
        or the radio lock keeps its duplicates out."""
        live = [t for t in spawned if t is not None and not t.done()]
        if not live:
            self._release_inflight(inflight_key)
            return

        async def _wait_then_release():
            await asyncio.gather(*live, return_exceptions=True)
            self._release_inflight(inflight_key)

        self._spawn_background_task(_wait_then_release())

    def _outgoing_inflight_sweep(self, now: float) -> None:
        """Safety net only: an entry should always be released by
        `_release_inflight_when_done`; anything older than ten minutes is
        a leak (a send path that raised before spawning, say) and is
        cleared so it can never block a destination for good."""
        with self._outgoing_inflight_lock:
            stale = [k for k, t in self._outgoing_inflight.items() if now - t > 600.0]
            for k in stale:
                del self._outgoing_inflight[k]

    async def _send_direct_to_all_peers(
        self, data: bytes, header: Optional[_RnsHeader] = None,
        priority: int = PRIORITY_NORMAL, expires_at: Optional[float] = None,
        spawned: Optional[list] = None,
    ) -> None:
        """Small-mesh replacement for a CHANNEL broadcast: one DIRECT
        copy to every bound peer instead, each spawned independently
        (never gated on another's outcome, same reasoning as every other
        fire-and-forget send in this dispatcher). Reuses
        `_send_direct_supplement` for the actual send -- it handles
        path-resolution-with-discovery, spacing, and bare-vs-fragmented
        dispatch regardless of caller.

        Audit fix (2026-09-19): but it does NOT provide a transport of last
        resort, and in this mode there is no broadcast running alongside to
        cover for it. `_send_direct_supplement` returns False when it never
        reached the radio (no resolved path even after discovery, no
        contact, expired), which for an ordinary supplement is correct --
        the broadcast it supplements already carried the packet. Here it
        would mean silent loss: one failed discovery round arms a cooldown
        of up to `path_discovery_backoff_max` (900s), during which every
        ANNOUNCE, path request and unknown-destination packet vanished with
        no log line, no `_outgoing_dropped_total` and nothing in the
        capture. So this now watches the per-peer results and falls back to
        a single CHANNEL broadcast if no peer got a transmission -- the
        same last-resort `_send_direct_packet` has always had, and not a
        weakening of DIRECT-primary: it fires only when DIRECT could not be
        attempted at all."""
        peer_prefixes = self._all_bound_peer_prefixes()
        tasks = []
        for peer_prefix in peer_prefixes:
            task = self._spawn_background_task(
                self._send_direct_supplement(
                    data, peer_prefix, trigger_discovery=True, priority=priority, expires_at=expires_at,
                    alongside_broadcast=False,
                )
            )
            tasks.append(task)
            if spawned is not None:
                spawned.append(task)

        async def broadcast_if_none_sent() -> None:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            if any(r is True for r in results):
                return
            if self.detached or not self.online or self._expired(expires_at):
                return
            RNS.log(
                f"{self}: small-mesh DIRECT-to-all reached no peer "
                f"({len(peer_prefixes)} bound, no resolvable path) -- falling back to one "
                f"CHANNEL broadcast rather than dropping this packet silently.",
                RNS.LOG_WARNING,
            )
            await self._send_broadcast_packet(data, header, expires_at=expires_at)

        fallback = self._spawn_background_task(broadcast_if_none_sent())
        if spawned is not None:
            spawned.append(fallback)

    def _unknown_dest_in_backoff(self, destination_hash: Optional[bytes]) -> bool:
        if destination_hash is None:
            return False
        until = self._unknown_dest_backoff_until.get(destination_hash)
        return until is not None and time.monotonic() < until

    def _proof_like(self, header: Optional[_RnsHeader]) -> bool:
        """True for a PROOF, whose on-wire destination field is a one-shot
        value (the truncated hash of the packet it proves, or a link_id) --
        never a stable destination this node will address again."""
        return header is not None and header.packet_type == RNS.Packet.PROOF

    def _record_unknown_dest_attempt(self, destination_hash: Optional[bytes]) -> None:
        """Call once per outgoing packet that actually triggers a DIRECT-
        bootstrap attempt for `destination_hash` (not once per packet
        merely addressed to it while already in backoff -- see call
        sites in `_dispatch_outgoing_packet`). No positive "it failed"
        signal exists at this interface's own layer (a real MeshCore ACK
        from the bound peer only confirms *local* delivery to that peer,
        never that whatever it's being asked to relay ever replied), so
        this uses the same proxy `_send_path_request`'s own supplement
        selection implicitly relies on: repeated attempts with §7's token
        still never learned for this exact destination is itself the
        signal, checked lazily here rather than via any explicit success/
        failure callback threaded back from the send."""
        if destination_hash is None:
            return
        self._unknown_dest_last_attempt[destination_hash] = time.monotonic()
        attempts = self._unknown_dest_attempts.get(destination_hash, 0) + 1
        self._unknown_dest_attempts[destination_hash] = attempts
        if attempts >= self.UNKNOWN_DEST_BOOTSTRAP_FAILURE_THRESHOLD:
            cooldown = min(
                self.UNKNOWN_DEST_BOOTSTRAP_BASE_COOLDOWN_S * (
                    self.UNKNOWN_DEST_BOOTSTRAP_BACKOFF_FACTOR
                    ** (attempts - self.UNKNOWN_DEST_BOOTSTRAP_FAILURE_THRESHOLD)
                ),
                self.UNKNOWN_DEST_BOOTSTRAP_MAX_COOLDOWN_S,
            )
            self._unknown_dest_backoff_until[destination_hash] = time.monotonic() + cooldown
            RNS.log(
                f"{self}: {attempts} DIRECT-bootstrap attempt(s) to destination "
                f"{destination_hash.hex()} with no token ever learned for it -- "
                f"backing off {cooldown:.0f}s rather than spending more airtime "
                f"on a destination that doesn't seem reachable through this "
                f"node's bound peer(s).",
                RNS.LOG_WARNING,
            )

    def _remember_bootstrap_send(self, data: bytes, header: Optional[_RnsHeader]) -> None:
        """Field fix (2026-09-19 afternoon, `fieldtests/raw/Alpha0.1.2`): the
        unknown-destination backoff counts bootstrap attempts "with no token
        ever learned" as failures, but a destination whose replies are
        PROOFs (a plain DATA delivery, LXMF without a Link) never teaches a
        token that way -- the laptop's three bootstrap sends to d4c70c4b
        were all delivered and proved, and the interface still backed off
        and dropped the next 17. Remember the packet's truncated hash (the
        exact value its PROOF will carry as destination-hash field) so
        `_observe_incoming_rns_packet`'s PROOF branch can learn the route
        and clear the backoff when that proof arrives DIRECT -- the same
        shape `_pending_link_requests` uses for LRPROOFs."""
        if header is None or header.destination_hash is None or header.packet_type != RNS.Packet.DATA:
            return
        truncated_hash = self._compute_truncated_hash(data, header.header_type)
        if truncated_hash is None:
            return
        self._pending_dest_proofs[truncated_hash] = (
            header.destination_hash, time.monotonic() + self.proof_correlation_ttl_s,
        )

    def _note_channel_proof(self, header: Optional[_RnsHeader], transport: str) -> None:
        """A delivery PROOF that arrived over CHANNEL for a bootstrap DATA
        send this node remembered (2026-09-20, MeshBench `two_hop` baseline:
        probes 2 and 3 were delivered over CHANNEL and PROVED, the interface
        still counted them as bootstrap attempts "with no token learned",
        and after the third it dropped probes 4-7 outright for 300 s --
        `unknown_dest_backoff_drop` -- while the destination was provably
        answering). The CHANNEL receive path deliberately learns no routing
        token (its sender is unauthenticated, see `_handle_channel_frame`),
        and that stays so: this only clears the destination's
        unknown-destination backoff, the one decision a matched proof is
        entitled to change. The worst a forged CHANNEL proof can do is keep
        this node trying a destination it would otherwise have given up on
        for a while -- the pre-backoff behaviour. Phase 1 (2026-09-20)
        added a second, equally bounded decision: the bare send the proof
        answers stops retrying (`_signal_send_answered`), with NO path
        evidence recorded for any peer -- a forged CHANNEL proof can at
        most cost one retry the application's own retry then covers."""
        if header is None or header.packet_type != RNS.Packet.PROOF or header.destination_hash is None:
            return
        self._signal_send_answered(header.destination_hash, f"PROOF over {transport}", None)
        delivered = self._pending_dest_proofs.pop(header.destination_hash, None)
        if delivered is None:
            # The same for an LRPROOF answering a LINKREQUEST this node sent
            # to an unresolved destination (link_id -> requested destination,
            # the shape `_observe_incoming_rns_packet` uses for DIRECT).
            delivered = self._pending_link_requests.pop(header.destination_hash, None)
        if delivered is None:
            return
        proved_dest, _expiry = delivered
        had_backoff = proved_dest in self._unknown_dest_attempts or proved_dest in self._unknown_dest_backoff_until
        self._clear_unknown_dest_backoff(proved_dest)
        self._debug(
            f"PROOF over {transport} for a bootstrap DATA send to {proved_dest.hex()} -- destination is "
            f"reachable; unknown-destination backoff {'cleared' if had_backoff else 'not armed'} "
            f"(no token learned: CHANNEL senders are unauthenticated)."
        )

    def _pending_dest_proofs_sweep(self, now: float) -> None:
        stale = [k for k, (_dest, expiry) in self._pending_dest_proofs.items() if now >= expiry]
        for k in stale:
            del self._pending_dest_proofs[k]

    def _clear_unknown_dest_backoff(self, destination_hash: bytes) -> None:
        self._unknown_dest_attempts.pop(destination_hash, None)
        self._unknown_dest_backoff_until.pop(destination_hash, None)
        self._unknown_dest_last_attempt.pop(destination_hash, None)

    def _unknown_dest_backoff_sweep(self, now: float) -> None:
        """Code-review fix: `_unknown_dest_attempts`/`_unknown_dest_
        backoff_until` had no periodic reclaim, unlike `_dedup`/
        `_reassembly`/`_proof_correlation` (all swept from
        `_reassembly_cleanup_loop`) -- a destination_hash tried a few times
        and then never addressed again (an ephemeral/one-off destination,
        never resolved and never crossing the backoff threshold either)
        sat in these dicts for the rest of the process's life; only a
        later success (`_clear_unknown_dest_backoff`) ever removed an
        entry. Mirrors `_proof_correlation_sweep`'s shape: idle-since-
        last-attempt, not a fixed TTL from creation, so an actively-
        retried destination is never pruned out from under its own
        backoff schedule."""
        stale = [
            h for h, last in self._unknown_dest_last_attempt.items()
            if now - last > self.UNKNOWN_DEST_BOOTSTRAP_MAX_COOLDOWN_S
        ]
        for h in stale:
            self._unknown_dest_attempts.pop(h, None)
            self._unknown_dest_backoff_until.pop(h, None)
            self._unknown_dest_last_attempt.pop(h, None)

    def _cache_announce(self, data: bytes, header: Optional[_RnsHeader], sender_peer_prefix: Optional[str]) -> None:
        """Remember an ANNOUNCE a bound peer delivered DIRECT (phase 1,
        2026-09-20), bytes exactly as received. CHANNEL announces are never
        cached (no authenticated source, and `_answer_path_request_locally`
        needs one to gate on)."""
        if (header is None or header.packet_type != RNS.Packet.ANNOUNCE or header.destination_hash is None
                or sender_peer_prefix is None or sender_peer_prefix not in self._peers
                or self.announce_cache_ttl_s <= 0):
            return
        self._announce_cache.pop(header.destination_hash, None)
        self._announce_cache[header.destination_hash] = (bytes(data), time.monotonic(), sender_peer_prefix)
        while len(self._announce_cache) > self.ANNOUNCE_CACHE_MAX_KEYS:
            self._announce_cache.popitem(last=False)

    def _announce_cache_sweep(self, now: float) -> None:
        stale = [k for k, (_raw, t, _src) in self._announce_cache.items() if now - t > self.announce_cache_ttl_s]
        for k in stale:
            del self._announce_cache[k]
        stale = [k for k, t in self._path_request_local_answer_at.items() if now - t > self.path_request_local_answer_min_interval_s]
        for k in stale:
            del self._path_request_local_answer_at[k]

    def _answer_path_request_locally(self, requested_hash: Optional[bytes]) -> Optional[str]:
        """If this node's own RNS is asking for a path this interface has
        already delivered an announce for, hand that announce back to RNS
        and report the source peer; None when the request must go on air.
        See `announce_cache_ttl`'s comment for the mechanism and the
        evidence. Runs on the event loop (the outgoing worker), where
        `owner.inbound` is called for every real reception too."""
        if (requested_hash is None or self.announce_cache_ttl_s <= 0
                or self.path_request_local_answer_min_interval_s <= 0):
            return None
        entry = self._announce_cache.get(requested_hash)
        if entry is None:
            return None
        raw, cached_at, source_peer = entry
        now = time.monotonic()
        if now - cached_at > self.announce_cache_ttl_s:
            self._announce_cache.pop(requested_hash, None)
            return None
        if source_peer not in self._peers or self._path_discovery_in_backoff(source_peer):
            return None
        last = self._path_request_local_answer_at.get(requested_hash)
        if last is not None and now - last < self.path_request_local_answer_min_interval_s:
            # The second re-request inside the interval is the one that
            # verifies the destination over the air.
            return None
        header = self._parse_rns_header(raw)
        if header is None:
            return None
        # Context -> PATH_RESPONSE: what this announce is, and on a
        # transport node the value that keeps RNS from inserting it into
        # the announce table for re-flooding (Transport.inbound's
        # `packet.context != PATH_RESPONSE` guard). The announce signature
        # covers destination, key, name hash, random hash, ratchet and app
        # data -- not the context byte.
        dst_len = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
        context_offset = (2 + 2 * dst_len) if header.header_type == 1 else (2 + dst_len)
        if len(raw) <= context_offset:
            return None
        answer = bytearray(raw)
        answer[context_offset] = RNS.Packet.PATH_RESPONSE
        self._path_request_local_answer_at[requested_hash] = now
        self.process_incoming(bytes(answer), transport="local_announce_cache", sender_peer_prefix=source_peer)
        return source_peer

    def _path_request_rate_limited(self, requested_hash: Optional[bytes]) -> bool:
        """PATH_REQUEST_RATE_LIMIT_WINDOW_S -- same shape and fail-open
        convention as `_path_response_rate_limited`, including recording
        the send time as a side effect on the not-limited path."""
        if requested_hash is None:
            return False
        now = time.monotonic()
        last_sent = self._path_request_last_sent_at.get(requested_hash)
        if last_sent is not None and now - last_sent < self.PATH_REQUEST_RATE_LIMIT_WINDOW_S:
            return True
        self._path_request_last_sent_at[requested_hash] = now
        return False

    def _path_response_rate_limit_sweep(self, now: float) -> None:
        """Code review (2026-09-18): `_path_response_last_sent_at` had no
        reclaim at all (a TODO at the top of this file said as much) --
        one entry per destination ever answered, for the life of the
        process. An entry older than PATH_RESPONSE_RATE_LIMIT_WINDOW_S can
        never suppress anything again, so it is dropped here."""
        stale = [
            h for h, last in self._path_response_last_sent_at.items()
            if now - last >= self.PATH_RESPONSE_RATE_LIMIT_WINDOW_S
        ]
        for h in stale:
            del self._path_response_last_sent_at[h]
        stale = [
            h for h, last in self._path_request_last_sent_at.items()
            if now - last >= self.PATH_REQUEST_RATE_LIMIT_WINDOW_S
        ]
        for h in stale:
            del self._path_request_last_sent_at[h]

    def _pending_link_request_sweep(self, now: float) -> None:
        """Code review (2026-09-18): a LINKREQUEST whose LRPROOF never
        arrived (or arrived via CHANNEL, where nothing can be learned from
        it) leaves its `_pending_link_requests` entry behind; reclaimed on
        the same `proof_correlation_ttl_s` clock as `_proof_correlation`,
        which it is the Link-shaped sibling of."""
        expired = [k for k, (_dest, expiry) in self._pending_link_requests.items() if now >= expiry]
        for k in expired:
            del self._pending_link_requests[k]

    def _path_response_rate_limited(self, destination_hash: Optional[bytes]) -> bool:
        """True if an outgoing PATH_RESPONSE for `destination_hash` was
        already sent within PATH_RESPONSE_RATE_LIMIT_WINDOW_S -- see that
        constant's own docstring for why. A `None` hash (shouldn't happen
        for a real PATH_RESPONSE, but nothing here assumes it can't) is
        never rate-limited, matching `_unknown_dest_in_backoff`'s same
        fail-open convention for a missing hash. Records the send time as
        a side effect only on the not-limited path -- the caller is
        expected to actually send in that case, so this doubles as the
        "record this attempt" step without a separate call."""
        if destination_hash is None:
            return False
        now = time.monotonic()
        last_sent = self._path_response_last_sent_at.get(destination_hash)
        if last_sent is not None and now - last_sent < self.PATH_RESPONSE_RATE_LIMIT_WINDOW_S:
            return True
        self._path_response_last_sent_at[destination_hash] = now
        return False

    def _announce_rate_limited(self, destination_hash: Optional[bytes]) -> bool:
        """One spontaneous ANNOUNCE per destination per
        `announce_min_interval_s` (see that config's comment). Same
        record-on-the-not-limited-path convention as
        `_path_response_rate_limited`."""
        if destination_hash is None or self.announce_min_interval_s <= 0:
            return False
        now = time.monotonic()
        last_sent = self._announce_last_sent_at.get(destination_hash)
        if last_sent is not None and now - last_sent < self.announce_min_interval_s:
            return True
        self._announce_last_sent_at[destination_hash] = now
        return False

    def _note_link_closed(self, header: Optional[_RnsHeader]) -> None:
        """A LINKCLOSE in either direction: every Link packet carries the
        link_id as its destination-hash field (RNS Link.py), so the id is
        right there. Called from RNS's thread (process_outgoing) and this
        interface's loop (process_incoming); a dict store is atomic."""
        if (
            header is not None
            and header.context == RNS.Packet.LINKCLOSE
            and header.destination_hash is not None
        ):
            self._closed_links[header.destination_hash] = time.monotonic()

    def _link_closed(self, header: Optional[_RnsHeader]) -> bool:
        """True for a Link-addressed packet whose Link was closed within
        CLOSED_LINK_TTL_S -- the drive-home capture spent ~10 minutes of
        attempts on a 3-fragment DATA for a Link that had been closed
        before the packet reached the front of the queue. The LINKCLOSE
        itself is never dropped. Checked at dequeue, before the first
        transmission -- the same point outgoing_max_age is decided --
        never between fragments."""
        if (
            header is None
            or header.destination_type != RNS.Destination.LINK
            or header.destination_hash is None
            or header.context == RNS.Packet.LINKCLOSE
        ):
            return False
        closed_at = self._closed_links.get(header.destination_hash)
        return closed_at is not None and time.monotonic() - closed_at < self.CLOSED_LINK_TTL_S

    def _closed_links_sweep(self, now: float) -> None:
        stale = [k for k, t in self._closed_links.items() if now - t > self.CLOSED_LINK_TTL_S]
        for k in stale:
            del self._closed_links[k]

    async def _send_outgoing_packet(
        self, data: bytes, header: Optional[_RnsHeader], expires_at: Optional[float] = None,
        spawned: Optional[list] = None,
    ) -> None:
        """Entry point the outgoing worker calls for every packet -- logs
        the classification debug line, then either delays an LRPROOF
        (see below) or dispatches immediately via
        `_dispatch_outgoing_packet`, which has the actual three-way
        routing logic."""
        # Code-review fix: CLAUDE.md's observability requirement names
        # "transport chosen, priority tier, spacing tier" as things that
        # "must be logged... build it in at M0/M1, don't bolt it on once
        # the real logic is working" -- spacing tier already is (every
        # CHANNEL/DIRECT multi-fragment send logs its own spacing), but
        # priority tier and the DIRECT-vs-broadcast routing decision
        # itself never were: there was no way, from logs alone, to tell
        # which classification/route a given outgoing packet actually
        # took. One debug line per packet, correlatable against the
        # transport-specific send lines that already follow it.
        self._debug(
            f"outgoing packet classification: "
            f"packet_type={self._PACKET_TYPE_NAMES.get(header.packet_type, header.packet_type) if header else 'unknown'} "
            f"destination_type={self._DESTINATION_TYPE_NAMES.get(header.destination_type, header.destination_type) if header else 'unknown'} "
            f"priority={self._priority_tier(header)} "
            f"destination_hash={header.destination_hash.hex() if header and header.destination_hash else None}."
        )

        # User-requested fix (2026-09-15, real NomadNet field testing):
        # RNS.Link's own keepalive/staleness timing (Link.py) is computed
        # exactly once, from the initial LINK_REQUEST<->LRPROOF handshake
        # round trip (`self.rtt = time.time() - self.request_time`), and
        # never recalibrated afterward: `keepalive = clamp(rtt * 205.7,
        # 5s, 360s)`, `stale_time = keepalive * 2`. That initial handshake
        # is small and uncontended, so it tends to complete fast (tens of
        # ms) -- giving RNS an optimistic RTT sample that locks in a short
        # keepalive/stale_time (observed: ~15-18s / ~30-36s) even though
        # this transport's real round trips, once real traffic queues up
        # behind the DIRECT serialization fix above, routinely exceed
        # that. The Link then looks "stale" to RNS and gets torn down and
        # silently re-established -- re-sending already-in-flight content
        # for no reason, with no user action. Confirmed directly against
        # RNS/Link.py: `Link.rtt`/`keepalive` have no config-file hook at
        # all, so this can't be fixed from ~/.reticulum/config, and
        # `Link.py` never reads this interface's own `bitrate`.
        #
        # Fix: deliberately delay sending an outgoing LRPROOF (the proof
        # that answers a LINK_REQUEST, and the exact packet whose
        # arrival time RNS measures for that one-time RTT sample) so the
        # measured RTT lands comfortably past `KEEPALIVE_MAX_RTT` (1.75s),
        # pinning every new Link's keepalive/stale_time at RNS's own
        # maximum (360s/720s) instead of an unrepresentatively short
        # value. This targets only LRPROOF specifically -- a single small
        # packet sent once per Link establishment, never per message --
        # so it adds a one-time delay to establishing a *new* Link, not
        # to any ongoing CHANNEL/DIRECT application traffic, and RNS's
        # own establishment_timeout (`6s * hops + 360s` -- Link.py,
        # ESTABLISHMENT_TIMEOUT_PER_HOP/KEEPALIVE) has enormous headroom
        # over this delay, so it can never itself cause a link-
        # establishment failure.
        if header is not None and header.context == RNS.Packet.LRPROOF:
            self._debug(
                f"routing decision: LRPROOF -- delaying "
                f"{self.LINK_PROOF_RTT_INFLATION_DELAY_S}s before send so "
                f"RNS's own Link keepalive/staleness timing (calibrated "
                f"from this exact round trip) reflects this transport's "
                f"real latency rather than an optimistic handshake sample."
            )
            self._capture_outgoing(header, data, "lrproof_delayed")
            task = self._spawn_background_task(self._send_delayed_link_proof(data, header, expires_at))
            if spawned is not None:
                spawned.append(task)
            return

        # User-requested fix (2026-09-15, real 2-hop repeater field
        # testing): see PATH_RESPONSE_RATE_LIMIT_WINDOW_S's own docstring.
        # A remote client stuck re-requesting the same path every few
        # seconds shouldn't get this node re-keying the radio to answer
        # every single one -- drop the repeat, not the first answer.
        if header is not None and header.context == RNS.Packet.PATH_RESPONSE:
            if self._path_response_rate_limited(header.destination_hash):
                self._outgoing_dropped_total += 1
                self._capture_outgoing(header, data, "path_response_rate_limited")
                self._debug(
                    f"routing decision: PATH_RESPONSE for destination "
                    f"{header.destination_hash.hex() if header.destination_hash else None} "
                    f"-- dropped, already answered within "
                    f"{self.PATH_RESPONSE_RATE_LIMIT_WINDOW_S}s."
                )
                return

        # Field fix (2026-09-19, drive-home 3-hop capture): one spontaneous
        # ANNOUNCE per destination per announce_min_interval -- see that
        # config's own comment.
        if (
            header is not None
            and header.packet_type == RNS.Packet.ANNOUNCE
            and header.context != RNS.Packet.PATH_RESPONSE
            and self._announce_rate_limited(header.destination_hash)
        ):
            self._outgoing_dropped_total += 1
            self._capture_outgoing(header, data, "announce_rate_limited")
            self._debug(
                f"routing decision: ANNOUNCE for destination "
                f"{header.destination_hash.hex() if header.destination_hash else None} "
                f"-- dropped, one was already forwarded within "
                f"{self.announce_min_interval_s:.0f}s (announce_min_interval)."
            )
            return

        # Field fix (2026-09-18 evening): coalesce repeated path requests
        # for the same requested destination -- see
        # PATH_REQUEST_RATE_LIMIT_WINDOW_S's own comment.
        if (
            header is not None
            and header.packet_type == RNS.Packet.DATA
            and header.destination_type == RNS.Destination.PLAIN
        ):
            requested = self._path_request_target(data, header)
            if self._path_request_rate_limited(requested):
                self._outgoing_dropped_total += 1
                self._capture_outgoing(header, data, "path_request_rate_limited")
                self._debug(
                    f"routing decision: path request for "
                    f"{requested.hex() if requested else None} -- dropped, one was already "
                    f"sent within {self.PATH_REQUEST_RATE_LIMIT_WINDOW_S:.0f}s."
                )
                return
            answered_from = self._answer_path_request_locally(requested)
            if answered_from is not None:
                # Phase 1 (2026-09-20): answered from the cached announce,
                # nothing transmitted -- see announce_cache_ttl.
                self._capture_outgoing(header, data, "path_request_answered_locally", target_peer=answered_from)
                self._debug(
                    f"routing decision: path request for {requested.hex()} -- answered locally from the "
                    f"announce {answered_from!r} delivered earlier; not transmitted."
                )
                return

        # Code review (2026-09-18): remember which destination this
        # LINKREQUEST is for, keyed by the link_id its LRPROOF will carry --
        # see _observe_incoming_rns_packet's PROOF branch for the other
        # half (learning the destination's token and clearing its
        # unknown-destination backoff when that proof comes back DIRECT).
        if (
            header is not None
            and header.packet_type == RNS.Packet.LINKREQUEST
            and header.destination_hash is not None
        ):
            link_id = self._compute_link_id(data)
            if link_id is not None:
                self._pending_link_requests[link_id] = (
                    header.destination_hash, time.monotonic() + self.proof_correlation_ttl_s,
                )

        await self._dispatch_outgoing_packet(data, header, expires_at=expires_at, spawned=spawned)

    async def _send_delayed_link_proof(
        self, data: bytes, header: _RnsHeader, expires_at: Optional[float] = None,
    ) -> None:
        await asyncio.sleep(self.LINK_PROOF_RTT_INFLATION_DELAY_S)
        if self.detached or not self.online:
            return
        inner: list = []
        await self._dispatch_outgoing_packet(data, header, expires_at=expires_at, spawned=inner)
        # Finish everything the dispatch spawned before this task ends, so
        # the packet's in-flight entry (released when THIS task finishes)
        # really covers the whole send.
        live = [t for t in inner if t is not None]
        if live:
            await asyncio.gather(*live, return_exceptions=True)

    async def _dispatch_outgoing_packet(
        self, data: bytes, header: Optional[_RnsHeader], expires_at: Optional[float] = None,
        spawned: Optional[list] = None,
    ) -> None:
        """docs/routing_decisions.md's routing dispatcher -- the three
        outgoing situations, in the doc's own order:

        1. ANNOUNCE: broadcast only, UNLESS small-mesh mode applies (see
           `_in_small_mesh_mode`), in which case it goes DIRECT to every
           bound peer instead -- DIRECT fragmentation handles an
           announce's size the same way CHANNEL's own multi-fragment
           shape would. There's also no way for this interface to
           actually detect "this outgoing ANNOUNCE is answering peer X's
           path request" (RNS core doesn't expose that context to the
           interface layer), so outside small-mesh mode this remains a
           clean, total deferral, not a partial one.
        2. DATA+PLAIN (path request): broadcast plus a DIRECT supplement
           to a capped number of known router-capability peers with an
           already-resolved path, UNLESS small-mesh mode applies, in
           which case it goes DIRECT to every bound peer instead (no
           router-capability filter -- see _send_path_request).
        3. Everything else (DATA/SINGLE, LINK_REQUEST, PROOF): DIRECT,
           unconditionally, if this interface's own record shows a
           resolved path to the packet's peer (unaffected by small-mesh
           mode -- already DIRECT-only). Otherwise: broadcast fallback
           plus a capped DIRECT bootstrap-supplement, UNLESS small-mesh
           mode applies, in which case it goes DIRECT to every bound peer
           instead of broadcasting at all. "Resolved path known" is
           always this interface's own `_resolved_paths` record, never a
           fresh device-table read (routing_decisions.md's explicit
           instruction, carried over from path_discovery_spec.md's own
           persistence-failure note).
        """
        if header is not None and header.packet_type == RNS.Packet.DATA and header.destination_type == RNS.Destination.PLAIN:
            await self._send_path_request(data, header, expires_at=expires_at, spawned=spawned)
            return

        if header is not None and header.packet_type != RNS.Packet.ANNOUNCE:
            peer_prefix = self._resolve_routing_peer(header)
            if peer_prefix is not None:
                self._debug(f"routing decision: known peer {peer_prefix!r} -> DIRECT-primary.")
                self._capture_outgoing(header, data, "direct_primary", target_peer=peer_prefix)
                # A known peer routes to _send_direct_packet regardless of
                # whether a path is *currently* resolved -- Milestone 6
                # folds discover_path()-before-broadcast-fallback into
                # that method itself (§8), so this dispatcher no longer
                # gates DIRECT eligibility on _resolved_paths membership
                # the way Milestone 5 did; a peer with no (or a freshly
                # stale-path-reset) resolved path still belongs on the
                # DIRECT path, not an immediate broadcast fallback.
                # DIRECT is the primary transport here, not a supplement --
                # its own ACK wait can take several seconds, so it's spawned
                # rather than awaited, exactly like path discovery and the
                # CHANNEL extra-retry passes above: one slow operation must
                # not stall the worker from draining the next queued packet.
                task = self._spawn_background_task(
                    self._send_direct_packet(data, header, peer_prefix, expires_at=expires_at)
                )
                if spawned is not None:
                    spawned.append(task)
                return

            # Milestone 6 fix for a real field-diagnosed gap
            # (peer_discovery_design.md §7): opportunistic RNS-token
            # learning only ever learns from an incoming DIRECT receive
            # (CHANNEL carries no sender identity at all, so there's
            # nothing safer to learn from) -- meaning two bound peers
            # that have never yet exchanged a single DIRECT message have
            # no way to ever originate one, since neither side has a
            # token yet. Fix: broadcast (below, mandatory, unchanged)
            # plus a DIRECT-bootstrap-supplement to a capped number of
            # bound peers. Safe: it adds no new exposure beyond what the
            # broadcast already does (RNS's own encryption protects
            # content regardless of transport; a DIRECT copy reaching a
            # bound peer who isn't actually the intended recipient is
            # exactly as cryptographically inert to them as the CHANNEL
            # copy they were already going to receive). Self-limiting:
            # one successful delivery teaches the recipient a real token
            # immediately (§7's normal mechanism, via
            # _observe_incoming_rns_packet), after which ordinary
            # DIRECT-primary routing takes over for that destination and
            # this supplement never fires for it again.
            # User-requested fix (2026-09-15, field-diagnosed via packet
            # capture): every branch below that actually attempts a
            # DIRECT-bootstrap send for this destination_hash first
            # checks/records against _unknown_dest_in_backoff -- see that
            # method's own docstring and the class constants above for
            # why (an unreachable-through-this-peer destination retried
            # on its own schedule forever, with no memory of past
            # attempts, confirmed costing real repeated airtime for an
            # LXMF propagation node this node's only MeshCore peer simply
            # has no path to).
            backed_off = self._unknown_dest_in_backoff(header.destination_hash)
            if self._in_small_mesh_mode():
                if backed_off:
                    self._debug(
                        f"routing decision: no known peer for this destination -- "
                        f"backing off further DIRECT attempts (repeated tries, no "
                        f"token ever learned for it); dropping rather than "
                        f"spending more airtime."
                    )
                    self._capture_outgoing(header, data, "unknown_dest_backoff_drop")
                    self._outgoing_dropped_total += 1
                    return
                self._debug(
                    f"routing decision: no known peer for this destination, "
                    f"small mesh ({len(self._peers)} bound peer(s)) -- DIRECT "
                    f"to every bound peer instead of CHANNEL."
                )
                self._capture_outgoing(
                    header, data, "small_mesh_direct_all_unknown_dest",
                    candidate_peers=self._all_bound_peer_prefixes(),
                )
                if not self._proof_like(header):
                    # Field fix (2026-09-19 evening session): a PROOF must not
                    # arm this backoff. Its destination field is the truncated
                    # hash of the proved packet (or a link_id) -- a value seen
                    # once and never addressed again -- so counting "attempts
                    # with no token ever learned" against it is meaningless,
                    # and in the midday capture it was harmful: three such
                    # proofs armed a 300s cooldown that then DROPPED later
                    # proofs outright in small-mesh mode. Six proofs per
                    # session still route here because the CHANNEL receive
                    # path cannot authenticate a sender and so deliberately
                    # learns no token from it (the raw path's guard cannot be
                    # reused: CHANNEL carries only an attacker-choosable
                    # adv_name). Excluding proofs removes the harm without
                    # inventing trust.
                    self._record_unknown_dest_attempt(header.destination_hash)
                self._remember_bootstrap_send(data, header)
                await self._send_direct_to_all_peers(
                    data, header, priority=self._priority_tier(header), expires_at=expires_at, spawned=spawned,
                )
                return
            bootstrap_targets = [] if backed_off else self._select_bootstrap_supplement_targets()
            if bootstrap_targets:
                # Registered BEFORE the supplement tasks are spawned (review,
                # 2026-09-20): the proof correlation and the answered-send
                # key are read by the send path, so their order must not
                # rest on the tasks not running until the dispatcher yields.
                self._remember_bootstrap_send(data, header)
            for bootstrap_peer_prefix in bootstrap_targets:
                task = self._spawn_background_task(
                    self._send_direct_supplement(
                        data, bootstrap_peer_prefix, trigger_discovery=True,
                        priority=self._priority_tier(header), expires_at=expires_at,
                    )
                )
                if spawned is not None:
                    spawned.append(task)
            if bootstrap_targets:
                if not self._proof_like(header):
                    # Field fix (2026-09-19 evening session): a PROOF must not
                    # arm this backoff. Its destination field is the truncated
                    # hash of the proved packet (or a link_id) -- a value seen
                    # once and never addressed again -- so counting "attempts
                    # with no token ever learned" against it is meaningless,
                    # and in the midday capture it was harmful: three such
                    # proofs armed a 300s cooldown that then DROPPED later
                    # proofs outright in small-mesh mode. Six proofs per
                    # session still route here because the CHANNEL receive
                    # path cannot authenticate a sender and so deliberately
                    # learns no token from it (the raw path's guard cannot be
                    # reused: CHANNEL carries only an attacker-choosable
                    # adv_name). Excluding proofs removes the harm without
                    # inventing trust.
                    self._record_unknown_dest_attempt(header.destination_hash)
            self._debug(
                f"routing decision: no known peer for this destination -- "
                f"CHANNEL broadcast"
                + (f" + bootstrap DIRECT supplement to {bootstrap_targets}" if bootstrap_targets else "")
                + (" (DIRECT supplement backed off -- repeated no-reply attempts)" if backed_off else "")
                + "."
            )
            self._capture_outgoing(
                header, data,
                "unknown_dest_backoff_broadcast_only" if backed_off else "broadcast_bootstrap_supplement",
                candidate_peers=bootstrap_targets,
            )
        elif header is not None and header.packet_type == RNS.Packet.ANNOUNCE:
            if self._in_small_mesh_mode():
                self._debug(
                    f"routing decision: ANNOUNCE, small mesh "
                    f"({len(self._peers)} bound peer(s)) -- DIRECT to every "
                    f"bound peer instead of CHANNEL."
                )
                self._capture_outgoing(
                    header, data, "small_mesh_direct_all_announce",
                    candidate_peers=self._all_bound_peer_prefixes(),
                )
                await self._send_direct_to_all_peers(
                    data, header, priority=self._priority_tier(header), expires_at=expires_at, spawned=spawned,
                )
                return
            self._debug("routing decision: ANNOUNCE -> CHANNEL broadcast only (never DIRECT, by design).")
            self._capture_outgoing(header, data, "broadcast_announce_only")
        else:
            self._capture_outgoing(header, data, "broadcast_fallback")

        await self._send_broadcast_packet(data, header, expires_at=expires_at)

    def _select_bootstrap_supplement_targets(self) -> list:
        """Milestone 6's DIRECT-bootstrap-supplement target selection:
        ANY bound peer, not filtered by router capability the way
        `_select_direct_supplement_targets` (path requests) is -- the
        goal here isn't "find a router to forward through," it's "this
        specific bound peer might be the actual RNS-level counterpart
        for a destination this interface has no token for yet," so
        capability is irrelevant. Most-recently-seen first, capped
        (`bootstrap_direct_supplement_cap`) so this doesn't fan out to
        every bound peer as the peer count grows.

        Airtime-efficiency fix (2026-09-17): primarily ordered by this
        peer's own `_direct_path_failures` count (fewest first), most-
        recently-seen only as the tiebreaker among equally-healthy peers
        -- previously recency alone decided this, so a peer with a
        currently elevated failure count (already a full attempt budget
        away from a stale-path reset, but not yet at
        `direct_path_reset_threshold`) could still occupy a scarce capped
        slot ahead of a peer this interface has no reason to doubt,
        spending part of the bootstrap-supplement's own limited fan-out on
        a send unlikely to succeed. A peer with no recorded failures at
        all sorts as failure count `0`, same as one that's never been
        tried -- this is a deprioritization signal, not a hard exclusion,
        so a struggling link still gets a chance once it's the least-bad
        option available."""
        peers = sorted(
            self._peers.values(),
            key=lambda p: (self._direct_path_failures.get(p.pubkey_prefix, 0), -p.last_seen),
        )
        return [p.pubkey_prefix for p in peers[: self.bootstrap_direct_supplement_cap]]

    async def _send_broadcast_packet(
        self, data: bytes, header: Optional[_RnsHeader], expires_at: Optional[float] = None,
        spawned: Optional[list] = None,
    ) -> None:
        """The CHANNEL broadcast send, exactly as Milestones 1-3 built it
        -- extracted unchanged out of Milestone 3's own
        `_send_outgoing_packet` body so Milestone 5's dispatcher above can
        call it both as the ordinary case-1/no-resolved-path fallback
        (awaited directly, preserving the original serialization) and as
        one half of the path-request broadcast+DIRECT-supplement pattern
        (spawned as an independent background task there instead --
        see _send_path_request)."""
        retry_extra = self._retry_extra_for(header)
        pkt_id = self._next_pkt_id()
        duty_cycle_exempt = self._duty_cycle_exempt(self._priority_tier(header))

        await self._send_channel_pass(data, pkt_id, attempt=0, duty_cycle_exempt=duty_cycle_exempt)

        # §1-§2: each extra pass is scheduled unconditionally at send
        # time (CHANNEL has no ACK to react to) as an independent
        # background task, so a multi-second-to-tens-of-seconds jittered
        # wait for pkt_id's own retry never blocks the worker from moving
        # on to the next queued packet.
        for attempt in range(1, retry_extra + 1):
            task = self._spawn_background_task(
                self._delayed_retry_pass(data, pkt_id, attempt, expires_at, duty_cycle_exempt)
            )
            # Audit fix (2026-09-19): these were untracked, so
            # `_release_inflight_when_done` saw no live task for a broadcast
            # and freed the duplicate-suppression key as soon as pass 0
            # returned -- while the jittered retry passes for the same bytes
            # were still pending. RNS re-queueing an identical copy in that
            # window (its Resource layer does, ~27s apart) was then accepted
            # as "not in flight" and broadcast on top of them: the duplicate
            # storm the in-flight guard was added to stop.
            if spawned is not None:
                spawned.append(task)

    async def _send_path_request(
        self, data: bytes, header: _RnsHeader, expires_at: Optional[float] = None,
        spawned: Optional[list] = None,
    ) -> None:
        """docs/routing_decisions.md's path-request case: always
        broadcast, plus a DIRECT copy to a capped number of known
        router-capability peers that already have a resolved path --
        cheap (path requests are small, comfortably one fragment) and
        meaningfully more reliable than the broadcast alone. Never
        triggers path discovery just to enable this supplement: only
        already-resolved peers qualify.

        **Resolved: fired concurrently, neither gated on the other's
        outcome** (the doc's own explicit resolution) -- both the
        broadcast and every DIRECT supplement are spawned as independent
        background tasks rather than one awaiting the other, so this
        coroutine (and so the outgoing worker) returns immediately. They
        still funnel through the same single radio at the command layer
        (`_command_lock`), and `_send_direct_supplement` still waits out
        this design's own minimum inter-message gap before actually
        transmitting, so they're never issued back-to-back into the same
        half-duplex-deaf window either.

        In small-mesh mode (`_in_small_mesh_mode`), this whole broadcast-
        plus-supplement shape is replaced: DIRECT to every bound peer,
        not just router-capability ones with an already-resolved path,
        and no CHANNEL broadcast at all. The router-capability filter
        exists to keep a *supplement* small as peer count grows -- with
        only one or two peers total there's no growth to bound, and a
        peer this node hasn't confirmed as a router yet still deserves a
        chance to answer (CHANNEL would have reached it too); this also
        does trigger discovery for an unresolved peer, since there is no
        broadcast left to fall back on if it doesn't."""
        if self._in_small_mesh_mode():
            self._debug(
                f"routing decision: path request, small mesh "
                f"({len(self._peers)} bound peer(s)) -- DIRECT to every "
                f"bound peer instead of CHANNEL."
            )
            self._capture_outgoing(
                header, data, "small_mesh_direct_all_path_request",
                candidate_peers=self._all_bound_peer_prefixes(),
            )
            await self._send_direct_to_all_peers(
                data, header, priority=self._priority_tier(header), expires_at=expires_at, spawned=spawned,
            )
            return
        self._debug("routing decision: path request -> CHANNEL broadcast + router-peer DIRECT supplement.")
        supplement_targets = self._select_direct_supplement_targets()
        self._capture_outgoing(
            header, data, "broadcast_path_request_supplement", candidate_peers=supplement_targets,
        )
        tasks = [self._spawn_background_task(
            self._send_broadcast_packet(data, header, expires_at=expires_at, spawned=spawned)
        )]
        for peer_prefix in supplement_targets:
            tasks.append(self._spawn_background_task(
                self._send_direct_supplement(
                    data, peer_prefix, priority=self._priority_tier(header), expires_at=expires_at,
                )
            ))
        if spawned is not None:
            spawned.extend(tasks)

    def _select_direct_supplement_targets(self) -> list:
        """docs/routing_decisions.md's path-request DIRECT-supplement
        target selection: known router-capability peers (this project's
        own bind-frame protocol bit -- peer_discovery_design.md §2's hard
        rule against the old design's shipped `can_route` mislabeling bug:
        never anything read off the MeshCore contact table, which has no
        such field at all) that already have a resolved path, capped and
        most-recently-confirmed first -- not unconditionally every known
        router as the router count grows.

        Airtime-efficiency fix (2026-09-17): primarily ordered by this
        peer's own `_direct_path_failures` count (fewest first), most-
        recently-confirmed only as the tiebreaker among equally-healthy
        peers -- see `_select_bootstrap_supplement_targets`'s own note on
        this same fix for the full reasoning (a currently-failing peer,
        below `direct_path_reset_threshold` so still technically
        "resolved," could otherwise still win a capped supplement slot on
        recency alone). Not a hard exclusion: `_direct_path_failures`
        naturally clears the moment this peer's path is confirmed working
        again, or the peer drops out of `candidates` entirely once a
        stale-path reset removes it from `_resolved_paths`."""
        candidates = [
            peer for peer in self._peers.values()
            if peer.has_upstream_rns and peer.pubkey_prefix in self._resolved_paths
        ]
        candidates.sort(
            key=lambda p: (
                self._direct_path_failures.get(p.pubkey_prefix, 0),
                -self._resolved_paths[p.pubkey_prefix].resolved_at,
            )
        )
        return [p.pubkey_prefix for p in candidates[: self.path_request_direct_supplement_cap]]

    async def _send_direct_supplement(
        self, data: bytes, peer_prefix: str, trigger_discovery: bool = False,
        priority: int = PRIORITY_NORMAL, expires_at: Optional[float] = None,
        alongside_broadcast: bool = True,
    ) -> bool:
        """A DIRECT copy of `data` to one bound peer, fired alongside a
        mandatory broadcast this method never gates or is gated by (see
        `_send_path_request`'s and the bootstrap-supplement dispatcher's
        own docstrings). `trigger_discovery` distinguishes the two
        callers' different preconditions: the path-request supplement
        (`_select_direct_supplement_targets`) never triggers discovery
        just to enable itself (routing_decisions.md's explicit
        instruction -- the broadcast already covers this peer regardless);
        the DIRECT-bootstrap-supplement (Milestone 6,
        `_select_bootstrap_supplement_targets`) deliberately does, since
        triggering discovery for a not-yet-token-bootstrapped peer is the
        whole point of that mechanism.

        Returns True if the packet actually reached the radio, False if this
        method bailed before transmitting (audit fix, 2026-09-19 -- see
        `_send_direct_to_all_peers`, which is the one caller that has no
        broadcast running alongside to cover for a False and therefore needs
        to know)."""
        resolved = self._resolved_paths.get(peer_prefix)
        # Alpha 0.1.5 (item 3, from shortcut_appears): the same shorter-path
        # adoption `_send_direct_packet` makes -- this is the other place a
        # send decides resolved-versus-discover (DIRECT-to-all and the
        # supplements route a destination with no token through here), and
        # a run in which no PROOF ever came back never reached the first.
        resolved = await self._maybe_adopt_shorter_path(peer_prefix, resolved)
        if resolved is None:
            if not trigger_discovery:
                return False
            resolved = await self._discover_path_coalesced(peer_prefix)
            if resolved is None:
                # Audit fix (2026-09-19): logged and counted like every
                # sibling drop path in this method (CLAUDE.md's "every drop
                # decision must be logged"). This was the last silent one.
                self._outgoing_dropped_total += 1
                self._debug(
                    f"DIRECT supplement to {peer_prefix!r} not sent -- no resolved path and "
                    f"discovery did not resolve one (peer may be inside a path-discovery backoff)."
                )
                return False

        # This design's own minimum inter-message gap (reliability_engine_
        # design.md §2), scaled by this specific peer's own known hop
        # depth when available -- routing_decisions.md's fix for the
        # half-duplex-deaf-repeater collision risk this supplement would
        # otherwise recreate against the broadcast's own fragment(s) if
        # fired with zero spacing.
        # MeshBench finding 4 (2026-09-20): the spacing exists to clear the
        # broadcast this copy rides alongside. Small-mesh DIRECT-to-all sends
        # no broadcast (see _send_direct_to_all_peers), yet paid 5-10s x hops
        # here -- most of one two-hop probe's 51s round trip -- so that caller
        # passes alongside_broadcast=False and skips it.
        spacing_s = self._supplement_spacing_s(resolved.out_path_len, alongside_broadcast)
        if spacing_s > 0:
            await asyncio.sleep(spacing_s)
        if self.detached or not self.online:
            return False
        if self._expired(expires_at):
            self._outgoing_dropped_total += 1
            self._debug(f"DIRECT supplement to {peer_prefix!r} skipped -- packet expired (outgoing_max_age).")
            return False

        contact = self._resolve_contact(peer_prefix)
        target = contact.get("public_key") if contact is not None else None
        if not target:
            # Code-review fix: this used to return with no log line and no
            # _outgoing_dropped_total increment, unlike every sibling drop
            # path in this method and in _send_direct_packet -- violating
            # CLAUDE.md's "every drop decision must be logged" rule for
            # this specific case (a bound peer whose contact record can't
            # be resolved, or has no public_key, at the moment a
            # supplement fires).
            self._outgoing_dropped_total += 1
            RNS.log(
                f"{self}: DIRECT supplement to {peer_prefix!r} dropped -- "
                f"no resolvable contact/public_key for this bound peer.",
                RNS.LOG_WARNING,
            )
            return False
        # Bare in the common case (a path request always fits DIRECT's
        # bare budget), but the bootstrap-supplement caller can carry an
        # arbitrary-size DATA/LINK_REQUEST/PROOF packet, so this goes
        # through the same bare-or-fragmented dispatch _send_direct_packet
        # uses rather than assuming bare unconditionally.
        ok = await self._send_direct_payload(
            target, peer_prefix, data, priority=priority, hop_count=resolved.out_path_len,
            expires_at=expires_at,
        )
        if ok is None:
            # Code-review fix: _send_direct_packet's own handling of this
            # same None-means-too-big-to-attempt contract logs and counts
            # it; this call site used to just drop the return value,
            # silently swallowing a supplement that never even tried to
            # go out with no log line and no _outgoing_dropped_total
            # increment anywhere -- violating CLAUDE.md's "every drop
            # decision (with its reason) must be logged" requirement for
            # this specific transport path. No CHANNEL fallback needed
            # here (this is only ever a supplement; the mandatory
            # broadcast this rides alongside already covers delivery).
            self._outgoing_dropped_total += 1
            RNS.log(
                f"{self}: DIRECT supplement to {peer_prefix!r} for a "
                f"{len(data)}-byte packet never attempted -- exceeds even "
                f"the fully-fragmented DIRECT budget.",
                RNS.LOG_WARNING,
            )
            return False
        return True

    def _supplement_spacing_s(self, hop_count: Optional[int], alongside_broadcast: bool) -> float:
        """The pre-send spacing for one DIRECT supplement: the design's
        hop-scaled inter-message gap when a broadcast of the same packet is
        on air alongside it, nothing otherwise (MeshBench finding 4,
        2026-09-20)."""
        if not alongside_broadcast:
            return 0.0
        spacing_min, spacing_max = self._fragment_spacing_range(hop_count=hop_count)
        return random.uniform(spacing_min, spacing_max)

    async def _delayed_retry_pass(
        self, data: bytes, pkt_id: int, attempt: int, expires_at: Optional[float] = None,
        duty_cycle_exempt: bool = False,
    ) -> None:
        delay = random.uniform(self.retransmit_jitter_min_s, self.retransmit_jitter_max_s)
        await asyncio.sleep(delay)
        if self.detached or not self.online:
            return
        if self._expired(expires_at):
            self._debug(f"CHANNEL retry pass (attempt={attempt}) for pkt_id={pkt_id} skipped -- packet expired.")
            return
        self._debug(
            f"CHANNEL retry pass (attempt={attempt}) for pkt_id={pkt_id} "
            f"firing after {delay:.1f}s jitter."
        )
        await self._send_channel_pass(data, pkt_id, attempt, duty_cycle_exempt=duty_cycle_exempt)

    async def _send_channel_pass(
        self, data: bytes, pkt_id: int, attempt: int, duty_cycle_exempt: bool = False,
    ) -> None:
        """One full CHANNEL send pass for `data` under `pkt_id`, at a
        given `attempt` number -- re-fragments from scratch every time
        (§1), even though the fast-path-vs-multi-fragment shape decision
        itself doesn't depend on `attempt`; re-deriving it fresh per pass
        costs nothing and keeps this the single place that decides it."""
        fastpath_budget = self._channel_payload_budget()
        if len(data) <= fastpath_budget:
            await self._send_channel_fastpath_frame(data, pkt_id, attempt, duty_cycle_exempt)
            return

        # Per wire_format_design.md's "constraint one," ANNOUNCE is the
        # only RNS packet type that structurally needs this path today --
        # everything else comfortably fits the fast-path budget above.
        per_fragment_budget = self._channel_multifragment_payload_budget()
        max_total_payload = per_fragment_budget * 255  # frag_total is a 1-byte field
        if per_fragment_budget <= 0 or len(data) > max_total_payload:
            self._outgoing_dropped_total += 1
            RNS.log(
                f"{self}: dropping outgoing packet ({len(data)} bytes, "
                f"pkt_id={pkt_id} attempt={attempt}) -- exceeds even the "
                f"fully-fragmented CHANNEL budget ({max_total_payload} "
                f"bytes across 255 fragments at {per_fragment_budget} "
                f"bytes each); no larger transport is available for "
                f"CHANNEL traffic.",
                RNS.LOG_WARNING,
            )
            return

        await self._send_channel_multifragment_pass(data, pkt_id, attempt, duty_cycle_exempt)

    async def _send_channel_fastpath_frame(
        self, payload: bytes, pkt_id: int, attempt: int, duty_cycle_exempt: bool = False,
    ) -> None:
        frame = self._encode_channel_fastpath(payload, pkt_id, attempt)
        await self._pre_transmit_gate(frame, duty_cycle_exempt=duty_cycle_exempt)
        try:
            await self._run_command(
                self._mc_ready.commands.send_chan_msg(self.channel_idx, frame),
                "send_chan_msg",
                self._EventType.OK,
            )
        except Exception as exc:
            self._outgoing_dropped_total += 1
            RNS.log(
                f"{self}: CHANNEL send failed (pkt_id={pkt_id} "
                f"attempt={attempt}, {len(payload)}-byte payload): {exc}",
                RNS.LOG_WARNING,
            )
            return
        self.txb += len(frame)
        # 2026-09-20 (airtime pass): the single-frame CHANNEL send wrote no
        # transmit record at all, so a capture could not total this node's
        # air. One `channel_fragment_sent` with frag 0/1, like the others.
        self._capture_channel_fragment_sent(
            pkt_id, attempt, 0, 1, 0, ok=True, size_bytes=len(frame),
            on_air_bytes=self._text_frame_on_air_bytes(self._own_node_name + ": " + frame),
        )
        self._debug(
            f"CHANNEL send OK: pkt_id={pkt_id} attempt={attempt} "
            f"{len(payload)}-byte payload ({len(frame)} chars on wire)."
        )

    async def _send_channel_multifragment_pass(
        self, payload: bytes, pkt_id: int, attempt: int, duty_cycle_exempt: bool = False,
    ) -> None:
        """One CHANNEL retry pass (docs/reliability_engine_design.md §1-2):
        re-fragments `payload` fresh, sends every fragment (in a shuffled
        order by default) with independently-drawn inter-fragment
        spacing, all under the same `pkt_id` at the given `attempt`."""
        chunks = self._fragment_payload(payload)
        frag_total = len(chunks)

        order = list(range(frag_total))
        if self.fragment_order_shuffle:
            random.shuffle(order)

        # Milestone 2 has no live hop-count data source (module docstring)
        # -- always resolves to the flat unknown-multi-hop tier for now.
        spacing_min, spacing_max = self._fragment_spacing_range(hop_count=None)

        self._debug(
            f"CHANNEL multi-fragment send: pkt_id={pkt_id} attempt={attempt} "
            f"frag_total={frag_total} order={order} "
            f"spacing=[{spacing_min:.1f},{spacing_max:.1f})s"
        )

        for position, frag_idx in enumerate(order):
            if self.detached or not self.online:
                # Interface was detached, or the underlying MeshCore
                # connection dropped (_on_mc_disconnected) mid-send --
                # stop rather than burn through the remaining fragments
                # each individually failing against a dead connection.
                return
            frame = self._encode_channel_multifragment(
                chunks[frag_idx], pkt_id, frag_idx, frag_total, attempt
            )
            await self._pre_transmit_gate(frame, duty_cycle_exempt=duty_cycle_exempt)
            try:
                await self._run_command(
                    self._mc_ready.commands.send_chan_msg(self.channel_idx, frame),
                    "send_chan_msg",
                    self._EventType.OK,
                )
                self.txb += len(frame)
                self._capture_channel_fragment_sent(
                    pkt_id, attempt, frag_idx, frag_total, position, ok=True, size_bytes=len(frame),
                    on_air_bytes=self._text_frame_on_air_bytes(self._own_node_name + ": " + frame),
                )
            except Exception as exc:
                # CHANNEL is blind/unacknowledged (§0) -- a failure here
                # means the local radio itself rejected/errored the
                # command, not that the fragment wasn't heard over the
                # air. Still attempt the rest of the set: one local
                # command failure doesn't mean the others will also fail,
                # and a partial fragment set is still useful (§1's
                # union-of-passes reasoning applies within a pass too).
                self._outgoing_dropped_total += 1
                self._capture_channel_fragment_sent(
                    pkt_id, attempt, frag_idx, frag_total, position, ok=False, size_bytes=len(frame),
                )
                RNS.log(
                    f"{self}: CHANNEL fragment send failed (pkt_id={pkt_id} "
                    f"attempt={attempt} frag_idx={frag_idx}/{frag_total}): {exc}",
                    RNS.LOG_WARNING,
                )

            if position < frag_total - 1:
                await asyncio.sleep(random.uniform(spacing_min, spacing_max))

    # -------------------------------------------------------------------
    # Incoming (MeshCore event -> RNS core)
    # -------------------------------------------------------------------

    def _subscribe_data_events(self):
        self._mc_ready.subscribe(self._EventType.CHANNEL_MSG_RECV, self._on_channel_msg_recv)
        self._mc_ready.subscribe(self._EventType.CONTACT_MSG_RECV, self._on_contact_msg_recv)
        if hasattr(self._EventType, "RAW_DATA"):
            self._mc_ready.subscribe(self._EventType.RAW_DATA, self._on_raw_data)
        self._subscribe_rx_log_events()

    def _on_raw_data(self, event) -> None:
        try:
            self._on_raw_data_inner(event)
        except Exception as exc:
            self._incoming_dropped_total += 1
            RNS.log(f"{self}: raw DIRECT receive handler failed: {exc}", RNS.LOG_ERROR)
            RNS.log(traceback.format_exc(), RNS.LOG_DEBUG)

    def _on_raw_data_inner(self, event) -> None:
        """Raw binary DIRECT fragments, receive side (2026-09-18 night).
        Anything without our version nibble or our dst prefix is another
        application's raw packet (or one for a neighbour that shares our
        last hop) and is dropped without a log line."""
        if self.detached:
            return
        payload = event.payload if isinstance(event.payload, dict) else {}
        raw = payload.get("payload")
        try:
            data = bytes.fromhex(raw) if isinstance(raw, str) else bytes(raw or b"")
            header, rns_payload, src_prefix, dst_prefix = self._decode_raw_fragment(data)
        except (ValueError, TypeError):
            self._raw_frames_ignored += 1
            return
        own = self._own_pubkey_hex
        if not own or bytes.fromhex(own[: self.RAW_DST_PREFIX_BYTES * 2]) != dst_prefix:
            self._raw_frames_ignored += 1
            return
        # M3 (2026-09-20): the 2-byte source prefix names a bound peer (the
        # 6-byte token every text frame from that peer carries, so raw and
        # text fragments share one reassembly bucket); no or two matches ->
        # dropped, counted.
        sender_token = self._resolve_raw_src(src_prefix)
        if sender_token is None:
            self._raw_frames_ignored += 1
            self._debug(f"raw fragment from unresolvable source prefix {src_prefix!r} dropped (no unique bound peer).")
            return
        self._raw_fragments_received += 1
        self._handle_direct_multifragment_frame(
            header, rns_payload, sender_token, raw=True,
            report_requested=self._raw_fragment_report_requested(data),
            parity=self._raw_fragment_is_parity(data),
        )

    def _on_channel_msg_recv(self, event):
        try:
            self._on_channel_msg_recv_inner(event)
        except Exception as exc:
            # Audit fix (2026-09-19): the three callbacks that carry RNS
            # payloads had no top-level guard (only _on_rx_log_data did), so
            # any unexpected exception was caught by the meshcore
            # dispatcher and logged through the library's `logging` only --
            # never RNS.log, never counted, invisible to the operator, with
            # the packet silently lost. Same failure mode
            # _log_background_task_exception exists to prevent on the send
            # side.
            self._incoming_dropped_total += 1
            RNS.log(f"{self}: CHANNEL receive handler failed: {exc}", RNS.LOG_ERROR)
            RNS.log(traceback.format_exc(), RNS.LOG_DEBUG)

    def _on_channel_msg_recv_inner(self, event):
        if self.detached:
            return
        payload = event.payload if isinstance(event.payload, dict) else {}
        raw_text = payload.get("text", "")

        # The firmware unconditionally prepends "<node_name>: " to every
        # CHANNEL text message it relays (meshcore_protocol_rules.md
        # CHANNEL rule 2) -- split on the first ": " to recover the
        # marker/header/payload that follows it. A node with no
        # configured name collapses this to a bare ": " prefix, which
        # this same split handles correctly (sender_name == "").
        sender_name, sep, remainder = raw_text.partition(": ")
        if not sep:
            return  # not framed the way this firmware's CHANNEL send always frames it

        if remainder.startswith(self.PEER_MARKER):
            # Bind frames (docs/peer_discovery_design.md §1) are a
            # separate control protocol riding the same CHANNEL, checked
            # here before RNS-frame handling since the two markers are
            # disjoint by construction ("P" vs "R"). The CHANNEL name
            # prefix is NOT used as the sender's identity for this --
            # unreliable/non-unique per §1; the bind frame carries its
            # own pubkey_prefix field for that.
            self._handle_incoming_bind_frame(remainder)
            return

        self._handle_incoming_frame(remainder, mode="channel", sender_token=sender_name)

    def _on_contact_msg_recv(self, event):
        try:
            self._on_contact_msg_recv_inner(event)
        except Exception as exc:
            self._incoming_dropped_total += 1
            RNS.log(f"{self}: DIRECT receive handler failed: {exc}", RNS.LOG_ERROR)
            RNS.log(traceback.format_exc(), RNS.LOG_DEBUG)

    def _on_contact_msg_recv_inner(self, event):
        if self.detached:
            return
        # Field-diagnosed fix (2026-09-18, see module docstring): this used
        # to stamp _last_incoming_direct_at here, unconditionally, for
        # every DIRECT frame heard -- see _handle_direct_multifragment_frame
        # for where that timestamp is set now and why.
        payload = event.payload if isinstance(event.payload, dict) else {}
        text = payload.get("text", "")
        sender_token = payload.get("pubkey_prefix", "")
        if text.startswith(self.COMPLETION_MARKER):
            # Completion-check frames (see _check_remote_completion's own
            # docstring) are DIRECT-only and checked here before RNS-frame
            # handling, the same way bind frames are checked before
            # _handle_incoming_frame on the CHANNEL side -- the two
            # markers are disjoint by construction ("Q" vs "R").
            self._handle_incoming_completion_frame(text, sender_token)
            return
        self._handle_incoming_frame(text, mode="direct", sender_token=sender_token)

    def _handle_incoming_frame(self, marker_and_body: str, mode: str, sender_token: str) -> None:
        if not marker_and_body.startswith(self.MARKER):
            return  # ordinary traffic sharing the channel/contact, not ours

        try:
            header, rns_payload = self._decode_frame(marker_and_body, mode=mode)
        except ValueError as exc:
            self._debug(f"discarding malformed {mode.upper()} frame from {sender_token!r}: {exc}")
            return

        if mode == "direct":
            if header.coop:
                # Code-review fix: the 0x40 cooperative-dispatch bit has
                # "no defined meaning in the DIRECT header shapes" per
                # wire_format_design.md's own text ("a DIRECT receiver
                # should never inspect it") -- previously accepted
                # silently, which combined with _reassembly_key's own
                # sender_token-based keying meant a DIRECT sender setting
                # this bit could land fragments in the same `("~coop",
                # pkt_id, frag_total)` bucket a CHANNEL cooperative-
                # dispatch fragment (or another DIRECT sender doing the
                # same) uses, merging unrelated senders' fragments into
                # one "complete" packet. Reject outright, matching every
                # other structurally-invalid-per-this-design's-own-rules
                # case this interface already treats as malformed.
                self._incoming_dropped_total += 1
                RNS.log(
                    f"{self}: dropping DIRECT frame from {sender_token!r} -- "
                    f"the cooperative-dispatch bit has no defined meaning on "
                    f"DIRECT and is never set by this interface's own "
                    f"encoder; treating as malformed.",
                    RNS.LOG_WARNING,
                )
                return
            if header.multi_fragment:
                self._handle_direct_multifragment_frame(header, rns_payload, sender_token)
                return
            self._debug(f"DIRECT frame from {sender_token!r}: {len(rns_payload)}-byte payload.")
            # Code-review fix: a bare DIRECT frame carries no pkt_id at all
            # (it relies entirely on the firmware's own ACK cycle, per
            # wire_format_design.md), so it can't reuse _reassembly_key's
            # pkt_id-based scheme -- but since Milestone 6 added this
            # interface's own _send_direct_with_attempts retry loop on top
            # of that firmware ACK cycle, a lost ACK now causes a genuine
            # resend (the firmware's own attempt-flag varies, but this
            # interface's own bare-DIRECT frame content never does), which
            # without a dedup check here would deliver the same logical
            # packet to RNS core twice -- the one receive path that skipped
            # the dedup discipline every other receive path already has.
            # Alpha 0.1.1 fix (2026-09-18 night, see module docstring): RNS
            # re-delivers identical bytes on purpose for the contexts its
            # own packet_filter exempts -- a Resource part re-requested
            # after arriving outside the receive window stalled a whole
            # transfer here when every re-send was dropped as a duplicate.
            # Those contexts bypass the dedup; everything else keeps it.
            rns_header = self._parse_rns_header(rns_payload)
            rns_dedups = rns_header is None or rns_header.context not in self._RNS_NO_DEDUP_CONTEXTS
            dedup_key = ("~direct_bare", sender_token or "~anon", rns_payload)
            if rns_dedups:
                if self._dedup_contains(dedup_key):
                    self._incoming_dropped_total += 1
                    self._debug(f"dropping duplicate bare DIRECT packet from {sender_token!r} (already delivered).")
                    return
                self._dedup_add(dedup_key, rns_payload)
            peer_prefix = self._canonical_peer_prefix(sender_token)
            self._observe_incoming_rns_packet(rns_payload, peer_prefix)
            
            # Log heard my own frame.
            # TODO: Review if this actually happens and make adjustments accordingly. This may need to be a filter
            # NOTE (2026-09-18 coherence review): this is deliberately
            # log-only -- it does NOT drop the frame, which is why the
            # message below no longer says "ignoring" (it used to, while
            # still falling through to process_incoming, which would have
            # actively misled anyone reading these logs to diagnose a field
            # issue). Bounded either way: _observe_incoming_rns_packet
            # above can't learn anything from a self-echo, since it only
            # learns from prefixes present in `_peers` and this node never
            # registers itself (see _handle_incoming_bind_frame's own
            # own_prefix guard). Deciding whether to add the filter is the
            # open TODO above -- it needs field evidence that this fires
            # at all, which is what this log line exists to gather.
            if peer_prefix == self._own_pubkey_prefix():
                self._debug(
                    f"heard my own bare DIRECT frame from {sender_token!r} -- "
                    f"still delivering it to RNS (no self-echo filter applied on this path)."
                )

            self.process_incoming(rns_payload, transport="direct_bare", sender_peer_prefix=peer_prefix)
            return

        self._handle_channel_frame(header, rns_payload, sender_token)

    def _handle_channel_frame(self, header: _FrameHeader, payload: bytes, sender_token: str) -> None:
        key = self._reassembly_key(header, sender_token, mode="channel")

        if not header.multi_fragment:
            # Single-fragment fast path: complete by construction, so the
            # dedup cache holds the *whole packet* under this key -- a
            # same-key hit is directly comparable against `payload`. Per
            # §6's "verify content identity, don't just assume it"
            # principle (already applied to reassembly's repeated-
            # frag_idx case), extended here to the same residual risk
            # §5.1 describes: two different anonymous ("~anon") senders
            # -- or two genuinely different senders whose 16-bit pkt_id
            # counters happen to collide -- picking the same key for two
            # *different* packets within the dedup TTL window is a real,
            # not just theoretical, scenario on a channel with more than
            # one unnamed node. Treating a same-key-different-content
            # arrival as "already delivered" would silently and
            # permanently drop a legitimate packet.
            cached_payload = self._dedup_get(key)
            if cached_payload is not None:
                if cached_payload == payload:
                    self._incoming_dropped_total += 1
                    self._debug(f"dropping duplicate CHANNEL packet {key} (already delivered).")
                    return
                RNS.log(
                    f"{self}: dedup-cache collision for key {key} -- a new "
                    f"arrival's content differs from what's cached under "
                    f"the same key (§5.1's blank/anonymous-sender residual "
                    f"risk); treating it as a distinct packet rather than "
                    f"risk silently dropping a legitimate one.",
                    RNS.LOG_WARNING,
                )
            self._dedup_add(key, payload)
            self._debug(
                f"CHANNEL frame from {sender_token or '~anon'}: pkt_id={header.pkt_id} "
                f"attempt={header.attempt} {len(payload)}-byte payload."
            )
            self.process_incoming(
                payload, transport="channel_bare", channel_sender_claimed=sender_token or None,
                pkt_id=header.pkt_id,
            )
            return

        # Multi-fragment: dedup is a pure key check here -- at this point
        # we only ever have one fragment's own chunk, never the whole
        # reassembled packet, so there's nothing meaningful to content-
        # compare against. A key match means this packet already
        # completed and was evicted from self._reassembly (§6's "late
        # duplicate after completion" case); once that's happened there's
        # no cached content left to verify a late fragment against, so a
        # key match alone is what the existing design already treats as
        # sufficient to drop it.
        if self._dedup_contains(key):
            self._incoming_dropped_total += 1
            self._debug(f"dropping late/duplicate CHANNEL fragment for {key} (already delivered).")
            return

        complete_data = self._add_channel_fragment(key, header, payload)
        if complete_data is not None:
            self.process_incoming(
                complete_data, transport="channel_multifragment",
                channel_sender_claimed=sender_token or None,
                frag_total=header.frag_total, pkt_id=header.pkt_id,
            )

    def process_incoming(
        self, data: bytes, *, transport: str = "unknown",
        sender_peer_prefix: Optional[str] = None, channel_sender_claimed: Optional[str] = None,
        frag_total: Optional[int] = None, pkt_id: Optional[int] = None,
    ) -> None:
        """The single funnel every complete incoming RNS payload passes
        through, regardless of shape (CHANNEL/DIRECT, bare/multi-
        fragment) -- not an RNS.Interface base-class method (confirmed:
        the base class has none by this name), so free to carry these
        capture-only keyword parameters without risking a base-class
        signature mismatch. All default to values meaning "not
        applicable/not known" so this stays a normal call for anything
        that doesn't care about packet capture."""
        if not self.online or self.detached:
            return
        if transport != "local_announce_cache":
            self.rxb += len(data)
        header = self._parse_rns_header(data)
        self._note_link_closed(header)
        if transport.startswith("channel"):
            self._note_channel_proof(header, transport)
        elif transport != "local_announce_cache":
            self._cache_announce(data, header, sender_peer_prefix)
        if self._packet_capture_file is not None:
            self._capture_incoming(
                data, transport=transport, sender_peer_prefix=sender_peer_prefix,
                channel_sender_claimed=channel_sender_claimed, frag_total=frag_total, pkt_id=pkt_id,
            )
        self.owner.inbound(data, self)
