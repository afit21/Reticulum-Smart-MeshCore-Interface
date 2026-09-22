"""Observability: debug logging, the optional packet capture (one JSON line per handled RNS packet and per transmit attempt -- the project's primary field-diagnosis tool), the [STATS] loop, the firmware RX-log tap with its per-attempt correlation window and busy-air model, and the LoRa time-on-air model every airtime figure comes from."""
import asyncio
import json
import os
import random
import time
from typing import Optional

import RNS

from ._common import _RnsHeader


class _ObservabilityMixin:
    def _debug(self, msg: str) -> None:
        if self.debug_logs:
            RNS.log(f"{self}: {msg}", RNS.LOG_INFO)

    # -------------------------------------------------------------------
    # Packet capture (user-requested, 2026-09-15) -- off by default
    # (packet_capture_enabled). One JSON object per line (JSONL: easy to
    # tail -f, grep, or load with any per-line JSON reader) per in/out
    # RNS packet, written to packet_capture_dir (default: a
    # "packet_capture" subdirectory of this node's own RNS storage path).
    # Deliberately synchronous, unbuffered writes: this transport's own
    # real throughput ceiling (docs/reliability_engine_design.md's field
    # data: tens of bytes/sec) means packets are inherently rare relative
    # to normal disk I/O speed, so the "don't block the event loop"
    # concern that applies to CHANNEL/DIRECT sends doesn't meaningfully
    # apply here -- and flushing every line means a capture survives an
    # ungraceful process exit, which matters more for a debugging/
    # analysis tool than avoiding a sub-millisecond stall ever would.
    # -------------------------------------------------------------------

    @staticmethod
    def _capture_filename(label: str, iface_name: str, stamp: str) -> str:
        """The capture file's name (pure, alpha 0.1.5 item 7):
        `<label>_capture_<interface>_<stamp>.jsonl` -- the field's own
        convention for the desktop's files -- or the pre-0.1.5
        `capture_<interface>_<stamp>.jsonl` when there is no label. Both
        parts are reduced to [A-Za-z0-9-_]."""
        def safe(text: str) -> str:
            return "".join(c if c.isalnum() or c in "-_" else "_" for c in text)
        base = f"capture_{safe(iface_name)}_{stamp}.jsonl"
        label = safe(label.strip()) if label else ""
        return f"{label}_{base}" if label else base

    def _open_packet_capture(self) -> None:
        try:
            capture_dir = self.packet_capture_dir
            if not capture_dir:
                base = getattr(RNS.Reticulum, "storagepath", None)
                if not base:
                    RNS.log(
                        f"{self}: packet_capture_enabled but no packet_capture_dir "
                        f"configured and no RNS storage path available -- capture "
                        f"disabled for this run.",
                        RNS.LOG_WARNING,
                    )
                    return
                capture_dir = os.path.join(base, "packet_capture")
            os.makedirs(capture_dir, exist_ok=True)
            # Item 7: the node label -- packet_capture_label, else the
            # MeshCore node name SELF_INFO gave (known by now: the capture
            # opens once the node is online).
            label = self.packet_capture_label or (self._own_node_name or "")
            filename = self._capture_filename(label, self.name, time.strftime('%Y%m%dT%H%M%S'))
            path = os.path.join(capture_dir, filename)
            self._packet_capture_file = open(path, "a", buffering=1)
            RNS.log(f"{self}: packet capture enabled -- writing to {path}", RNS.LOG_INFO)
            # Alpha 0.1.6 (item 4): connection_state records from before
            # the node had a name (the capture needs the handshake).
            pending, self._pending_capture_events = self._pending_capture_events, []
            for record in pending:
                self._capture_event("out", record)
        except Exception as exc:
            RNS.log(f"{self}: failed to open packet capture file: {exc} -- capture disabled for this run.", RNS.LOG_WARNING)
            self._packet_capture_file = None

    def _close_packet_capture(self) -> None:
        with self._packet_capture_lock:
            if self._packet_capture_file is not None:
                try:
                    self._packet_capture_file.close()
                except Exception:
                    pass
                self._packet_capture_file = None

    def _capture_event(self, direction: str, fields: dict) -> None:
        """Appends one capture record. `direction` is "in" or "out";
        `fields` carries everything call-site-specific (see
        `_capture_outgoing`/`_capture_incoming`). Every record also gets
        a monotonically increasing `seq` (this process's own capture
        sequence, for unambiguous ordering independent of timestamp
        resolution) and both wall-clock and monotonic timestamps (the
        latter safe against system clock adjustments mid-capture)."""
        if self._packet_capture_file is None:
            return
        with self._packet_capture_lock:
            if self._packet_capture_file is None:
                return
            self._packet_capture_seq += 1
            record = {
                "seq": self._packet_capture_seq,
                "ts": time.time(),
                "ts_monotonic": time.monotonic(),
                "direction": direction,
                **fields,
            }
            try:
                self._packet_capture_file.write(json.dumps(record, default=str) + "\n")
            except Exception as exc:
                RNS.log(f"{self}: packet capture write failed: {exc}", RNS.LOG_WARNING)

    def _header_capture_fields(self, header: Optional[_RnsHeader]) -> dict:
        if header is None:
            return {
                "packet_type": None, "packet_type_name": None,
                "destination_type": None, "destination_type_name": None,
                "destination_hash": None, "context": None, "context_name": None,
                "header_type": None,
            }
        return {
            "packet_type": header.packet_type,
            "packet_type_name": self._PACKET_TYPE_NAMES.get(header.packet_type),
            "destination_type": header.destination_type,
            "destination_type_name": self._DESTINATION_TYPE_NAMES.get(header.destination_type),
            "destination_hash": header.destination_hash.hex() if header.destination_hash else None,
            "context": header.context,
            "context_name": self._CONTEXT_NAMES.get(header.context) if header.context is not None else None,
            "header_type": header.header_type,
        }

    def _payload_correlation_hash(self, data: bytes) -> str:
        """Field-data-analysis fix (2026-09-17): a short, non-cryptographic
        (for this purpose) identifier for `data`, added to every packet
        capture record that carries a payload. Analyzing the previous
        field test's phantom-ACK pattern required cross-referencing a
        sender's capture against a receiver's by `pkt_id` -- which only
        exists for DIRECT-*fragmented* sends; a bare (single-message)
        DIRECT or CHANNEL send had no correlator at all across separate
        capture files, making that whole analysis blind to roughly half
        of real traffic. `RNS.Identity.truncated_hash` is reused here
        (same primitive `_compute_truncated_hash` already uses) purely as
        a convenient, already-available hash -- this has no security role
        and is never compared against anything at runtime, only read back
        by an analysis script joining two nodes' capture files on this
        field, alongside `pkt_id` where that also exists."""
        return RNS.Identity.truncated_hash(data).hex()[:12]

    def _capture_outgoing(
        self, header: Optional[_RnsHeader], data: bytes, decision: str,
        target_peer: Optional[str] = None, candidate_peers: Optional[list] = None,
    ) -> None:
        if self._packet_capture_file is None:
            return
        # 2026-09-20: a path request's REQUESTED destination (the first 16
        # bytes of its data), so a capture can say which path RNS asked for
        # -- until now only the shared PLAIN path.request hash was recorded.
        requested = None
        if header is not None and header.packet_type == RNS.Packet.DATA and header.destination_type == RNS.Destination.PLAIN:
            requested = self._path_request_target(data, header)
        self._capture_event("out", {
            **self._header_capture_fields(header),
            "priority": self._priority_tier(header),
            "size_bytes": len(data),
            "payload_hash": self._payload_correlation_hash(data),
            "routing_decision": decision,
            "target_peer": target_peer,
            "candidate_peers": candidate_peers,
            "small_mesh_mode": self._in_small_mesh_mode(),
            "bound_peers": len(self._peers),
            "requested_hash": requested.hex() if requested else None,
        })

    def _capture_incoming(
        self, data: bytes, transport: str,
        sender_peer_prefix: Optional[str] = None, channel_sender_claimed: Optional[str] = None,
        frag_total: Optional[int] = None, pkt_id: Optional[int] = None,
    ) -> None:
        if self._packet_capture_file is None:
            return
        header = self._parse_rns_header(data)
        # User-requested (2026-09-16, for hop-based tuning-profile
        # analysis): this interface's own resolved outbound path length to
        # `sender_peer_prefix`, when one is known -- the best hop-count
        # proxy available here, since MeshCore never reports the actual
        # inbound path a given DIRECT frame took, only what this node has
        # resolved for sending back. Never set for CHANNEL (no sender
        # identity to resolve against at all).
        resolved = self._resolved_paths.get(sender_peer_prefix) if sender_peer_prefix else None
        self._capture_event("in", {
            **self._header_capture_fields(header),
            "priority": self._priority_tier(header),
            "size_bytes": len(data),
            "transport": transport,
            # Only ever set for DIRECT -- cryptographically tied to a
            # bound peer via the firmware's own per-contact shared
            # secret. Never conflate with channel_sender_claimed below.
            "sender_peer_prefix": sender_peer_prefix,
            # CHANNEL's own adv_name, unauthenticated and attacker-
            # controlled (this interface's security model: CHANNEL
            # carries no identity at all) -- included for visibility
            # only, never treated as a real sender identity anywhere
            # else in this interface.
            "channel_sender_claimed": channel_sender_claimed,
            "frag_total": frag_total,
            "pkt_id": pkt_id,
            "hop_count": resolved.out_path_len if resolved is not None else None,
            "payload_hash": self._payload_correlation_hash(data),
        })

    def _capture_fragment_received(
        self, mode: str, sender_token: str, pkt_id: int, frag_idx: int, frag_total: int, progress: int,
        raw: bool = False, parity: bool = False,
    ) -> None:
        """Field-data-analysis fix (2026-09-17): one record per individual
        fragment actually added to a reassembly bucket, not just the
        single record `_capture_incoming` emits once the whole message
        completes. The previous field test's phantom-ACK analysis (see
        `_check_remote_completion`'s own docstring for the case that
        prompted this) could only tell a fragment "arrived by such-and-
        such a time" from the bucket's *completion* timestamp -- there
        was no way to see when frag_idx 0 specifically showed up relative
        to the sender's own retry attempts for it, only that the whole
        bucket was done by some later point. This closes that gap
        directly: `mode`/`sender_token`/`pkt_id`/`frag_total` match
        `_reassembly_key`'s own tuple exactly, so a future analysis can
        join this against the sender's `direct_attempt_result` records
        (same `pkt_id`/`frag_idx`/`frag_total`) without guessing."""
        if self._packet_capture_file is None:
            return
        self._capture_event("in", {
            "event": "fragment_received",
            "mode": mode,
            "sender_token": sender_token,
            "pkt_id": pkt_id,
            "frag_idx": frag_idx,
            "frag_total": frag_total,
            "parity": parity,   # M4 (2026-09-20): a parity fragment; frag_idx is then its coverage mask
            "progress": progress,
            # User-requested (2026-09-19): raw binary fragment (True) or a
            # Z85 text one (False); both share the same reassembly bucket.
            "raw": raw,
        })

    def _capture_direct_attempt_result(
        self, peer_prefix: Optional[str], attempt: int, ok: bool,
        queue_depth: int, lock_wait_s: float, ack_timeout_s: Optional[float],
        pkt_id: Optional[int] = None, frag_idx: Optional[int] = None, frag_total: Optional[int] = None,
        listen_delay_s: Optional[float] = None, hop_count: Optional[int] = None,
        time_critical: bool = False, pass_number: Optional[int] = None,
        quiet_defer_wait_s: Optional[float] = None, duty_cycle_wait_s: Optional[float] = None,
        ack_timeout_source: str = "none", ack_latency_s: Optional[float] = None,
        send_cmd_latency_s: Optional[float] = None, rx_window: Optional[dict] = None,
        medium_hold_wait_s: Optional[float] = None, miss_diagnosis: Optional[str] = None,
        medium_busy_remaining_s: Optional[float] = None, kind: Optional[str] = None,
        hop1_abort_deadline_s: Optional[float] = None, duty_cycle_exempt: bool = False,
        duty_cycle_ledger: Optional[str] = None,
        quiet_hold_s: Optional[float] = None, on_air_bytes: Optional[int] = None,
        proof_age_s: Optional[float] = None, proof_fresh: Optional[bool] = None,
    ) -> None:
        """User-requested observability addition (2026-09-15, post-alpha-
        0.1.0 2-hop field test): one record per individual DIRECT send
        attempt (bare, or one fragment of a DIRECT-fragmented send) --
        the granularity the prior 2-hop investigation was missing, having
        to reconstruct attempt-by-attempt outcomes from timestamps and raw
        `send_msg` call arguments in the plain log instead. Carries an
        `event` field (absent on every existing packet in/out record) so
        analysis scripts already written against this capture format can
        distinguish these from packet records without breaking. `queue_
        depth`/`lock_wait_s` are `_direct_exchange_lock`'s own contention
        signal -- see `_direct_exchange_queue_depth`'s docstring -- direct
        evidence for or against "several concurrent messages splitting the
        one shared radio" the next time that's a live hypothesis.
        `listen_delay_s` is the post-send listen window this exact attempt
        drew -- from `direct_post_send_listen_success_min_s`/`max_s` if
        `ok`, `direct_post_send_listen_min_s`/`max_s` otherwise (2026-09-16
        split, see that constant's own comment) -- still held while
        `_direct_exchange_lock` was held -- lets a future analysis
        directly correlate how much of the channel's idle time was this
        deliberate listen window versus genuine gaps between traffic.
        `hop_count` (2026-09-16, user-requested for hop-based tuning-
        profile analysis) is this interface's own `_resolved_paths` entry
        for `peer_prefix` at the moment this attempt was made -- the same
        `out_path_len` `_fragment_spacing_range` itself keys spacing off
        of -- threaded down from whichever `resolved` the caller
        (`_send_direct_packet`/`_send_direct_supplement`) already had in
        hand, not re-looked-up here; `None` if no path was resolved yet
        (e.g. this attempt is itself part of establishing one).

        `time_critical`/`quiet_defer_wait_s`/`duty_cycle_wait_s`/
        `pass_number` (2026-09-18, user-requested field-tuning data,
        added the same pass that fixed the incoming-quiet-defer mutual-
        reset-loop regression -- see the module docstring's 2026-09-18
        entry): previously the only way to tell how much of an attempt's
        own latency was `_pre_transmit_gate`'s two waits, versus `_direct_
        exchange_lock` queueing (`lock_wait_s`, already captured) versus
        the ACK wait itself (`ack_timeout_s`, the ceiling, not the actual
        wait -- this capture has never recorded the real wait, only what
        it was capped at), was to cross-reference separate `_debug` text
        lines by hand, exactly the manual correlation this incident's own
        diagnosis needed. `quiet_defer_wait_s`/`duty_cycle_wait_s` are
        `_pre_transmit_gate`'s actual return values for this attempt
        (`None` if `_send_direct_frame` wasn't reached at all, e.g. this
        capture is for context that never got that far -- distinct from
        `0.0`, which means the gate ran and genuinely waited nothing).
        `time_critical` is whether `_pre_transmit_gate` was told to skip
        the quiet-defer wait for this specific attempt (see `_send_
        direct_frame`'s own docstring for exactly which attempts that
        covers). `pass_number` is `0`/`1` for a fragment sent by `_send_
        direct_fragmented`'s first pass or its re-drive pass respectively,
        `None` for a bare (non-fragmented) DIRECT send, which has no pass
        structure -- lets a future analysis directly measure how often
        pass 1 actually fires and how often it then succeeds, a core
        reliability metric this capture couldn't answer before without
        reconstructing pass boundaries from `attempt`/`frag_idx` by hand."""
        if self._packet_capture_file is None:
            return
        self._capture_event("out", {
            "event": "direct_attempt_result",
            "peer_prefix": peer_prefix,
            "attempt": attempt,
            "ok": ok,
            "queue_depth": queue_depth,
            "lock_wait_s": round(lock_wait_s, 3),
            "ack_timeout_s": ack_timeout_s,
            "pkt_id": pkt_id,
            "frag_idx": frag_idx,
            "frag_total": frag_total,
            "listen_delay_s": round(listen_delay_s, 3) if listen_delay_s is not None else None,
            "hop_count": hop_count,
            "time_critical": time_critical,
            "pass_number": pass_number,
            "quiet_defer_wait_s": round(quiet_defer_wait_s, 3) if quiet_defer_wait_s is not None else None,
            "duty_cycle_wait_s": round(duty_cycle_wait_s, 3) if duty_cycle_wait_s is not None else None,
            # Step 2 (2026-09-18): measured timing and what was overheard
            # during this attempt. `ack_latency_s` is MSG_SENT -> ACK event
            # (the span the timeout actually guards); `send_cmd_latency_s`
            # is _pre_transmit_gate -> MSG_SENT (host/serial/firmware queue
            # cost, NOT airtime). `ack_timeout_source` says which estimate
            # governed the wait. The `rx_*` fields are the correlation
            # window -- offsets in seconds after MSG_SENT.
            "ack_timeout_source": ack_timeout_source,
            # Alpha 0.1.7 (item 1): a plain PROOF's age since RNS queued it
            # (None for any other frame) and whether it pre-empted as fresh.
            "proof_age_s": round(proof_age_s, 3) if proof_age_s is not None else None,
            "proof_fresh": proof_fresh,
            "ack_latency_s": round(ack_latency_s, 3) if ack_latency_s is not None else None,
            "send_cmd_latency_s": round(send_cmd_latency_s, 3) if send_cmd_latency_s is not None else None,
            **self._rtt_capture_fields(peer_prefix),
            "rx_echo_seen_s": rx_window.get("echo_seen_s") if rx_window else None,
            "rx_echo_path_len": rx_window.get("echo_path_len") if rx_window else None,
            "rx_ack_seen_on_air_s": rx_window.get("ack_seen_on_air_s") if rx_window else None,
            "rx_path_reply_seen_s": rx_window.get("path_reply_seen_s") if rx_window else None,
            "rx_foreign_count": rx_window.get("foreign_rx_count") if rx_window else None,
            "rx_foreign": rx_window.get("foreign_rx") if rx_window else None,
            # Step 4 (2026-09-18): what the hold model did (medium_hold_
            # wait_s, only non-zero when rx_log_holds_enabled) and what it
            # concluded (always computed): the post-miss diagnosis and how
            # long the air was predicted to stay busy when this attempt
            # finished -- so a capture with holds OFF still shows what
            # they would have done.
            "medium_hold_wait_s": round(medium_hold_wait_s, 3) if medium_hold_wait_s is not None else None,
            "miss_diagnosis": miss_diagnosis,
            "medium_busy_remaining_s": round(medium_busy_remaining_s, 3) if medium_busy_remaining_s is not None else None,
            "rx_log_holds_enabled": self.rx_log_holds_enabled,
            # Code review (2026-09-18): None for an "R" (RNS payload)
            # frame; "completion_answer" for a "Q" ANSWER, which now goes
            # through the same ACK-waited path -- so an analysis can keep
            # its per-attempt reliability stats to real payload frames.
            "kind": kind,
            # Field fix (2026-09-18 evening): the armed early-abort
            # deadline for this attempt (None = not armed); when it fired,
            # ack_timeout_source is "hop1_abort" and ack_timeout_s equals
            # this. "expired" means the packet aged out before transmit.
            "hop1_abort_deadline_s": round(hop1_abort_deadline_s, 3) if hop1_abort_deadline_s is not None else None,
            # User-requested (2026-09-18 evening): handshake-class frames
            # skip the duty-cycle wait (airtime still charged).
            "duty_cycle_exempt": duty_cycle_exempt,
            # Alpha 0.1.5: the ledger a duty-cycle wait was charged to
            # ("relayed" = the 30% cap, "total" = the zero-hop cap, None =
            # no wait / not gated).
            "duty_cycle_ledger": duty_cycle_ledger,
            # Field fix (2026-09-19 night): how long this attempt kept the
            # radio lock AFTER its listen delay waiting for the answer it
            # asked for -- the hidden-node quiet window, non-null only on a
            # reconcile QUERY. See _send_direct_frame_and_wait_for_ack.
            "quiet_hold_s": round(quiet_hold_s, 3) if quiet_hold_s is not None else None,
            # 2026-09-20 (airtime pass): the frame's estimated on-air size,
            # None for an attempt that never keyed the radio.
            "on_air_bytes": on_air_bytes,
        })

    def _capture_channel_fragment_sent(
        self, pkt_id: int, attempt: int, frag_idx: int, frag_total: int, position: int,
        ok: bool, size_bytes: int, on_air_bytes: Optional[int] = None,
    ) -> None:
        """Observability addition (2026-09-18, user-requested field-tuning
        data): one record per individual CHANNEL fragment transmit
        attempt, the sender-side half of a gap this capture format already
        had on the receive side -- `_capture_fragment_received` (mode=
        "channel") has recorded every fragment a receiver actually got
        since 2026-09-17, but there was no equivalent for what the sender
        believed it sent, or in what shuffled order/attempt, or whether
        the local `send_chan_msg` command itself even succeeded. Without
        this, telling "the fragment was never sent" apart from "it was
        sent but never arrived" required cross-referencing the plain
        `_debug` text log by hand. `position` is this fragment's index
        within THIS pass's own shuffled send order (`_send_channel_
        multifragment_pass`'s own `order` list) -- distinct from `frag_idx`,
        the fragment's fixed logical index within the reassembled message --
        so a future analysis can check whether shuffle position correlates
        with loss the way `docs/reliability_engine_design.md` §2's
        position-dependent loss pattern predicts. `ok` reflects only the
        local `send_chan_msg` command outcome (CHANNEL has no ACK at all,
        so this can never confirm the fragment was actually heard over the
        air -- see `_send_channel_multifragment_pass`'s own comment on
        why a local failure doesn't stop the rest of the pass)."""
        if self._packet_capture_file is None:
            return
        self._capture_event("out", {
            "event": "channel_fragment_sent",
            "pkt_id": pkt_id,
            "attempt": attempt,
            "frag_idx": frag_idx,
            "frag_total": frag_total,
            "position": position,
            "ok": ok,
            "size_bytes": size_bytes,
            "on_air_bytes": on_air_bytes,
        })

    def _capture_direct_send_result(
        self, peer_prefix: str, destination_hash: Optional[bytes], ok: bool,
        resolved: "_ResolvedPath", size_bytes: int,
        method: Optional[str] = None, fallback_from_raw: bool = False,
        slot_wait_s: Optional[float] = None,
    ) -> None:
        """User-requested observability addition (2026-09-15, post-alpha-
        0.1.0 2-hop field test): one record per whole DIRECT message (every
        fragment across both passes, for a fragmented send), pairing the
        final ACKed/not-ACKed outcome with the hop path actually used --
        `out_path_len`/`out_path_hex` were previously visible only in a
        one-off RNS.log INFO line at the moment a path was freshly
        discovered, never at the moment a message using that (possibly
        long-cached) path actually succeeds or fails. This is the single
        record that answers "did this message get through, and over how
        many hops" without cross-referencing anything else."""
        if self._packet_capture_file is None:
            return
        self._capture_event("out", {
            "event": "direct_send_result",
            "peer_prefix": peer_prefix,
            "destination_hash": destination_hash.hex() if destination_hash else None,
            "ok": ok,
            "out_path_len": resolved.out_path_len,
            "out_path_hex": resolved.out_path_hex,
            "size_bytes": size_bytes,
            # User-requested (2026-09-19): how the packet was carried --
            # "z85_bare" (one text frame), "z85_text" (Z85 text fragments),
            # "raw" (raw binary fragments) -- and whether the text send was
            # the fallback after a raw attempt on this same packet.
            "method": method,
            "fallback_from_raw": fallback_from_raw,
            # Second audit (2026-09-19 evening): how long this fragmented
            # send waited for one of direct_fragmented_max_in_flight slots
            # (None when the cap is off -- the default since 2026-09-19
            # night; a send that timed out waiting still goes, so this can
            # equal the whole slot budget).
            "slot_wait_s": round(slot_wait_s, 3) if slot_wait_s is not None else None,
        })

    def _capture_completion_check_result(
        self, peer_prefix: str, pkt_id: int, frag_total: int, outcome: str, complete: bool,
        stage: str = "final", timeout_s: Optional[float] = None,
        answer_version: Optional[int] = None, held: Optional[list] = None,
    ) -> None:
        """Field-data-analysis fix (2026-09-17): one record per
        `_check_remote_completion` call, so the next field test can
        directly measure how often the phantom-ACK completion check
        (added this same pass -- see that method's own docstring) fires,
        and how it resolves, rather than only being inferable after the
        fact by cross-referencing two nodes' captures by hand the way the
        original phantom-ACK case was found. `outcome` is one of
        `"send_failed"` (the QUERY itself never got out locally),
        `"timeout"` (sent, but no ANSWER arrived within `direct_
        completion_check_timeout_s`), or `"answered"` (a real ANSWER came
        back -- `complete` is only meaningful in this case)."""
        if self._packet_capture_file is None:
            return
        self._capture_event("out", {
            "event": "completion_check_result",
            "peer_prefix": peer_prefix,
            "pkt_id": pkt_id,
            "frag_total": frag_total,
            "outcome": outcome,
            "complete": complete if outcome == "answered" else None,
            # Step 3 (2026-09-18): which stage asked ("reconcile" between
            # the passes, "final" after them), the timeout actually used
            # (RTT-derived when known), and the receiver's have-list.
            "stage": stage,
            "timeout_s": round(timeout_s, 3) if timeout_s is not None else None,
            "answer_version": answer_version,
            "held": held,
        })

    # -------------------------------------------------------------------
    # Observability: periodic structured stats snapshot
    # -------------------------------------------------------------------

    async def _stats_loop(self):
        """Per `docs/interface_architecture.md`'s observability
        requirements: a periodic structured snapshot of interface state,
        queryable without correlating scattered log lines by hand, built
        in from the first milestone that connects to anything -- not
        bolted on once "the real logic" exists. M0 has little of substance
        to report yet; later milestones (queue depth, reassembly bucket
        counts, per-peer state) extend this same snapshot rather than
        inventing a second mechanism."""
        try:
            while not self.detached:
                await asyncio.sleep(self._loop_interval_s(self.stats_interval_s, "stats_interval"))
                if self.detached:
                    break
                uptime = (
                    time.time() - self._connected_since
                    if self._connected_since is not None
                    else 0.0
                )
                RNS.log(
                    f"{self} [STATS] online={self.online} "
                    f"transport={self.transport} bitrate={self.bitrate}bps "
                    f"uptime={uptime:.0f}s "
                    f"outgoing_queue_depth={self._outqueue.qsize()} "
                    f"outgoing_dropped_total={self._outgoing_dropped_total} "
                    f"incoming_dropped_total={self._incoming_dropped_total} "
                    f"reassembly_buckets_open={len(self._reassembly)} "
                    f"dedup_cache_size={len(self._dedup)} "
                    f"pending_retry_passes={len(self._background_tasks)} "
                    f"resolved_paths={len(self._resolved_paths)} "
                    f"peers_in_discovery_backoff={len(self._path_discovery_backoff_until)} "
                    f"contacts_known={len(self._mc.contacts) if self._mc is not None else 0} "
                    f"bound_peers={len(self._peers)} "
                    f"rns_tokens_learned={len(self._rns_token_peer)} "
                    f"proof_correlations_pending={len(self._proof_correlation)} "
                    f"rx_log_feed={'seen' if self._rx_log_feed_seen else ('never' if self.rx_log_observe_enabled else 'off')} "
                    f"rx_log_events_total={self._rx_log_events_total} "
                    f"rx_log_by_type={dict(self._rx_log_by_payload_type)} "
                    f"raw_fragments_rx={self._raw_fragments_received} raw_frames_ignored={self._raw_frames_ignored} "
                    f"raw_unsupported_paths={list(self._raw_unsupported_paths)} "
                    f"ack_rtt={{{', '.join(f'{p!r}: srtt={st['srtt']:.2f}s rttvar={st['rttvar']:.2f}s n={st['samples']}' for p, st in self._ack_rtt.items())}}}",
                    RNS.LOG_INFO,
                )
                # Per-bucket fragment counts/ages (docs/interface_architecture.md's
                # observability requirements ask for these specifically) --
                # debug-gated so a busy channel doesn't spam the default
                # INFO-level snapshot above.
                if self.debug_logs and self._reassembly:
                    now = time.monotonic()
                    bucket_summary = ", ".join(
                        f"{key}: {len(bucket.fragments)}/{bucket.frag_total} "
                        f"age={now - bucket.last_progress:.0f}s"
                        for key, bucket in self._reassembly.items()
                    )
                    self._debug(f"[STATS] open reassembly buckets: {bucket_summary}")
        except asyncio.CancelledError:
            pass

    def _rtt_capture_fields(self, peer_prefix: Optional[str]) -> dict:
        st = self._ack_rtt.get(peer_prefix) if peer_prefix is not None else None
        if st is None:
            return {"rtt_srtt_s": None, "rtt_rttvar_s": None, "rtt_samples": 0}
        return {
            "rtt_srtt_s": round(st["srtt"], 3),
            "rtt_rttvar_s": round(st["rttvar"], 3),
            "rtt_samples": st["samples"],
        }

    # -- Per-attempt RX-log correlation window (step 2, capture-only) -----

    def _open_rx_log_window(self, target: str) -> dict:
        """Opened by `_send_direct_frame_and_wait_for_ack` just before it
        keys the radio, under `_direct_exchange_lock`, so exactly one
        window is ever open. `_on_rx_log_data` sorts every overheard
        packet into it while it's open (see `_classify_rx_log_for_window`).
        Sender/target identity is matched on MeshCore's 1-byte routing
        hashes (first byte of each pubkey) -- what the cleartext carries,
        so a hash-byte collision with an unrelated node on a big mesh can
        mislabel a foreign packet as ours; acceptable for capture-only
        data, and flagged here so nobody later promotes this to a routing
        decision without adding a stronger check."""
        own = self._own_pubkey_prefix() or ""
        window = {
            "opened_at": time.monotonic(),
            "tx_at": None,                # stamped once send_msg returns MSG_SENT
            "own_hash_byte": own[:2].lower() if own else None,
            "target_hash_byte": (target or "")[:2].lower() or None,
            "expected_ack": None,         # hex, set once MSG_SENT returns it
            "echo_seen_s": None,          # our own frame re-heard (repeater forwarded it)
            "echo_path_len": None,
            "ack_seen_on_air_s": None,    # bare ACK with our expected_ack code
            "path_reply_seen_s": None,    # PATH from target to us (the flood-mode ACK carrier)
            "foreign_rx_count": 0,
            "foreign_rx": [],             # up to _RX_LOG_WINDOW_FOREIGN_CAP (typename, route, path_len, t, src_hash)
            "ack_snr": None,              # the matched ACK's signal (alpha 0.1.6 item 1)
            "ack_rssi": None,
        }
        self._rx_log_window = window
        return window

    def _close_rx_log_window(self, window: dict) -> dict:
        if self._rx_log_window is window:
            self._rx_log_window = None
        return window

    def _classify_rx_log_for_window(self, fields: dict, now: float) -> None:
        """Sorts one overheard packet (already reduced to capture fields)
        into the open window. Offsets are relative to our own transmit
        (`tx_at`, falling back to when the window opened if MSG_SENT
        hasn't returned yet)."""
        w = self._rx_log_window
        if w is None:
            return
        ref = w["tx_at"] if w["tx_at"] is not None else w["opened_at"]
        t = round(now - ref, 3)
        ptype = fields.get("payload_type")
        src, dst = fields.get("src_hash"), fields.get("dst_hash")
        if ptype == self._RX_LOG_PAYLOAD_TYPE_ACK and w["expected_ack"] and fields.get("ack_code") == w["expected_ack"]:
            if w["ack_seen_on_air_s"] is None:
                w["ack_seen_on_air_s"] = t
                # Alpha 0.1.6 (item 1): the ACK's signal is the last leg of
                # the path this frame went on (`_note_path_signal`).
                w["ack_snr"], w["ack_rssi"] = fields.get("snr"), fields.get("rssi")
            return
        if (
            ptype == self._RX_LOG_PAYLOAD_TYPE_TEXT_MSG
            and src == w["own_hash_byte"] and dst == w["target_hash_byte"]
        ):
            # Our own radio never logs its own transmission, so a TEXT_MSG
            # from us to the target heard during our own attempt is a
            # repeater forwarding it -- the "did hop 1 happen" signal.
            if w["echo_seen_s"] is None:
                w["echo_seen_s"] = t
                w["echo_path_len"] = fields.get("path_len")
            return
        if (
            ptype == self._RX_LOG_PAYLOAD_TYPE_PATH
            and src == w["target_hash_byte"] and dst == w["own_hash_byte"]
        ):
            if w["path_reply_seen_s"] is None:
                w["path_reply_seen_s"] = t
            return
        w["foreign_rx_count"] += 1
        if len(w["foreign_rx"]) < self._RX_LOG_WINDOW_FOREIGN_CAP:
            w["foreign_rx"].append([fields.get("payload_typename"), fields.get("route_typename"), fields.get("path_len"), t, src])

    def _subscribe_rx_log_events(self) -> None:
        """Observe-only tap on the firmware's raw-RX log feed (2026-09-18,
        see `rx_log_observe_enabled`'s own comment for what the feed is
        and why it matters). Deliberately NOT in REQUIRED_EVENT_TYPES: an
        older `meshcore` library without `RX_LOG_DATA` just loses this
        observability, it doesn't lose the interface -- probed with
        `hasattr` per the library-contract rule ("never assume these
        names, always probe") and logged once either way, so a capture
        with no `rx_log` records can be told apart from a mesh that was
        genuinely silent."""
        if not self.rx_log_observe_enabled:
            RNS.log(f"{self}: raw-RX log observation disabled by config (rx_log_observe_enabled=no).", RNS.LOG_INFO)
            return
        if not hasattr(self._EventType, "RX_LOG_DATA"):
            RNS.log(
                f"{self}: installed meshcore library's EventType has no RX_LOG_DATA "
                f"member -- raw-RX log observation unavailable with this library "
                f"version (interface continues without it).",
                RNS.LOG_WARNING,
            )
            return
        self._mc_ready.subscribe(self._EventType.RX_LOG_DATA, self._on_rx_log_data)
        RNS.log(
            f"{self}: subscribed to the firmware's raw-RX log feed (RX_LOG_DATA) -- "
            f"observe-only; overheard packets are counted in [STATS] and, when "
            f"packet capture is on, recorded as 'rx_log' events.",
            RNS.LOG_INFO,
        )

    # -- Step 4 (2026-09-18): airtime model and predicted-busy holds -------

    def _estimate_airtime_s(self, nbytes: int) -> float:
        """LoRa time-on-air for an `nbytes` physical frame at the radio's
        own SF/BW/CR (explicit header, CRC on, low-data-rate optimisation
        when the symbol time exceeds 16ms, preamble 32 symbols at SF<=8
        else 16 -- the last two exactly as the firmware's
        `RadioLibWrapper::preambleLengthForSF`/RadioLib configure them).
        Falls back to a bitrate estimate when SELF_INFO hasn't provided
        radio params. Zero-hop check at SF7/BW62.5/CR8: a ~100-byte
        TEXT_MSG comes out at ~0.58s and an ACK at ~0.14s, consistent with
        the measured 1.03-1.25s ACK RTT once ~0.3-0.5s of firmware/host
        turnaround is added."""
        if self._radio_params is None:
            bitrate = max(1, self.duty_cycle_estimate_bitrate)
            return (max(1, nbytes) * 8) / bitrate
        sf, bw_khz, cr = self._radio_params
        tsym = (2 ** sf) / (bw_khz * 1000.0)
        n_preamble = 32 if sf <= 8 else 16
        t_preamble = (n_preamble + 4.25) * tsym
        de = 1 if tsym > 0.016 else 0
        num = 8 * max(1, nbytes) - 4 * sf + 28 + 16
        den = 4 * (sf - 2 * de)
        payload_symbols = 8 + max(0, -(-num // den)) * cr
        return t_preamble + payload_symbols * tsym

    def _estimate_tx_airtime_s(self, frame: str, path_len: int = 0, on_air_bytes: Optional[int] = None) -> float:
        """Airtime of one of this node's own `send_msg`/`send_chan_msg`
        frames: the LoRa time-on-air model (`_estimate_airtime_s`) over
        the frame's real on-air size per the framing above, when the
        radio's SF/BW/CR are known; else the flat bitrate estimate that
        the duty-cycle limiter used before 2026-09-18 (evening). `path_len`
        is the routed path's byte count (0 for zero-hop) -- one byte per
        hop, so a caller without it loses almost nothing by omitting it."""
        if on_air_bytes is not None:
            # A raw packet: header + path + payload, no text framing at all.
            if self._radio_params is None:
                return (on_air_bytes * 8) / max(1, self.duty_cycle_estimate_bitrate)
            return self._estimate_airtime_s(on_air_bytes)
        if self._radio_params is None:
            return (len(frame) * 8) / max(1, self.duty_cycle_estimate_bitrate)
        return self._estimate_airtime_s(self._text_frame_on_air_bytes(frame, path_len))

    def _text_frame_on_air_bytes(self, frame: str, path_len: int = 0) -> int:
        """Bytes one of this node's own TXT_MSG frames occupies on air, per
        the framing above (2026-09-20: also written to every transmit
        record as `on_air_bytes`, so a field capture can report on-air
        bytes per delivered RNS byte -- the metric of the airtime pass --
        the way `meshbench_report.py`'s ledger does from MeshBench's own
        event log). A CHANNEL frame's firmware framing differs slightly
        (channel hash instead of dest/src hashes); the same formula is
        used as an estimate."""
        plaintext = len(frame.encode("utf-8")) + self._TXT_MSG_PLAINTEXT_OVERHEAD_BYTES
        ciphertext = -(-plaintext // 16) * 16
        return self._TXT_MSG_FIXED_OVERHEAD_BYTES + max(0, path_len) + ciphertext

    def _predicted_hold_for_rx(self, fields: dict) -> "tuple[float, str]":
        """How long the air is expected to stay busy *after* one overheard
        packet, per the model in rx_log_holds_enabled's own comment.
        Returns (seconds, reason)."""
        nbytes = fields.get("payload_length") or 0
        if nbytes <= 0:
            return 0.0, "unknown"
        ptype = fields.get("payload_type")
        route = fields.get("route_type")
        airtime = self._estimate_airtime_s(nbytes)
        if ptype in self._RX_LOG_NOTHING_FOLLOWS_TYPES and route in self._RX_LOG_ROUTE_DIRECT:
            return 0.0, "ack_or_advert_direct"
        if route in self._RX_LOG_ROUTE_FLOOD:
            # Every repeater in range re-floods it (ADVERTs and channel
            # messages included), then -- for an addressed type -- the
            # target answers, itself flooded/echoed.
            hold = self.rx_log_hold_flood_factor * airtime
            reason = "flood_echo"
            if ptype in self._RX_LOG_ACK_BEARING_TYPES:
                hold += self._estimate_airtime_s(24) + self.rx_log_hold_turnaround_s
                reason = "flood_echo+reply"
            return hold, reason
        if route in self._RX_LOG_ROUTE_DIRECT:
            hops_left = int(fields.get("path_len") or 0)
            hold = self.rx_log_hold_hop_factor * airtime * hops_left
            reason = f"direct_forward_x{hops_left}"
            if ptype in self._RX_LOG_ACK_BEARING_TYPES:
                ack_airtime = self._estimate_airtime_s(12)
                hold += ack_airtime + self.rx_log_hold_turnaround_s + self.rx_log_hold_hop_factor * ack_airtime * hops_left
                reason += "+ack"
            return hold, reason
        return 0.0, "unknown"

    # -- Own-transmit busy accounting (alpha 0.1.5, 2a) ----------------------

    def _note_radio_keyed(self, airtime_s: float, now: Optional[float] = None) -> float:
        """One more frame handed to the firmware: the radio is busy until
        the later of now and its previous busy-until, plus this frame's
        estimated airtime (pure over its inputs; the firmware's own CAD
        deferral and tx budget can only push the real end later, so this is
        a floor). Returns the new busy-until."""
        now = time.monotonic() if now is None else now
        self._radio_busy_until = max(now, self._radio_busy_until) + max(0.0, airtime_s)
        self._estimated_tx_air_total_s += max(0.0, airtime_s)   # item 8: the estimator's running sum
        self._frames_keyed_total += 1
        return self._radio_busy_until

    def _radio_busy_remaining_s(self, now: Optional[float] = None) -> float:
        """Seconds until this node's own queued frames are estimated to be
        off the air (0.0 when idle)."""
        now = time.monotonic() if now is None else now
        return max(0.0, self._radio_busy_until - now)

    # -- Radio transmit statistics (alpha 0.1.5, item 8) --------------------

    def _radio_stats_record(self, reason: str, radio: Optional[dict], packets: Optional[dict]) -> dict:
        """The `radio_stats` capture record (pure over its inputs): the
        firmware's measured transmit / receive airtime and packet counts
        beside this interface's own summed airtime estimate and frame count
        since start, so a field summary can calibrate the estimator against
        the radio (`tx_air_secs` is `Dispatcher::total_air_time` in whole
        seconds, wall-clock from send start to send complete)."""
        radio = radio or {}
        packets = packets or {}
        return {
            "event": "radio_stats", "reason": reason,
            "tx_air_secs": radio.get("tx_air_secs"), "rx_air_secs": radio.get("rx_air_secs"),
            "noise_floor": radio.get("noise_floor"), "last_rssi": radio.get("last_rssi"), "last_snr": radio.get("last_snr"),
            "packets_sent": packets.get("sent"), "packets_recv": packets.get("recv"),
            "flood_tx": packets.get("flood_tx"), "direct_tx": packets.get("direct_tx"),
            "flood_rx": packets.get("flood_rx"), "direct_rx": packets.get("direct_rx"),
            "recv_errors": packets.get("recv_errors"),
            "estimated_tx_air_s": round(self._estimated_tx_air_total_s, 3),
            "frames_keyed": self._frames_keyed_total,
            "uptime_s": round(time.time() - self._connected_since, 1) if self._connected_since else None,
        }

    async def _poll_radio_stats(self, reason: str) -> Optional[dict]:
        """Read the firmware's radio and packet statistics (CMD_GET_STATS,
        companion protocol v8+) and write one `radio_stats` record. Best
        effort: a firmware or library without the command is logged once and
        never asked again. Returns the record, or None."""
        if self._radio_stats_unsupported or self._mc is None:
            return None
        commands = getattr(self._mc, "commands", None)
        if commands is None or not hasattr(commands, "get_stats_radio") or not hasattr(commands, "get_stats_packets"):
            self._radio_stats_unsupported = True
            self._debug("radio stats: the meshcore library has no get_stats_radio/get_stats_packets -- not polled.")
            return None
        radio = packets = None
        try:
            async with self._command_lock:
                ev = await asyncio.wait_for(commands.get_stats_radio(), timeout=5.0)
                if ev is not None and ev.type == self._EventType.ERROR:
                    raise RuntimeError(f"ERROR {ev.payload}")
                radio = ev.payload if ev is not None and isinstance(ev.payload, dict) else None
                ev = await asyncio.wait_for(commands.get_stats_packets(), timeout=5.0)
                if ev is not None and ev.type == self._EventType.ERROR:
                    raise RuntimeError(f"ERROR {ev.payload}")
                packets = ev.payload if ev is not None and isinstance(ev.payload, dict) else None
        except Exception as exc:
            if not self._radio_stats_unsupported:
                self._radio_stats_unsupported = True
                RNS.log(f"{self}: radio statistics unavailable from this firmware ({exc}) -- not polled again.", RNS.LOG_INFO)
            return None
        record = self._radio_stats_record(reason, radio, packets)
        self._debug(
            f"radio stats ({reason}): firmware tx_air {record['tx_air_secs']}s rx_air {record['rx_air_secs']}s "
            f"sent {record['packets_sent']} -- interface estimate {record['estimated_tx_air_s']}s over {record['frames_keyed']} frames."
        )
        if self._packet_capture_file is not None:
            self._capture_event("out", record)
        return record

    async def _radio_stats_loop(self):
        """`radio_stats` on a fixed cadence (`radio_stats_interval`; 0 =
        start and stop only)."""
        try:
            while not self.detached and self.radio_stats_interval_s > 0 and not self._radio_stats_unsupported:
                await asyncio.sleep(self._loop_interval_s(self.radio_stats_interval_s, "radio_stats_interval"))
                if self.detached:
                    break
                await self._poll_radio_stats("interval")
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._debug(f"radio stats loop stopped: {exc}")

    def _extend_medium_busy(self, hold_s: float, reason: str, now: float) -> None:
        if hold_s <= 0:
            return
        until = now + min(hold_s, self.rx_log_hold_max_s)
        if until > self._medium_busy_until:
            self._medium_busy_until = until
            self._medium_busy_reason = reason

    def _medium_busy_remaining_s(self, now: Optional[float] = None) -> float:
        now = time.monotonic() if now is None else now
        return max(0.0, self._medium_busy_until - now)

    async def _wait_for_medium_clear(self) -> float:
        """Pre-transmit hold (step 4, only when rx_log_holds_enabled): sleep
        until `_medium_busy_until`, re-checking because packets overheard
        *during* the wait extend it -- bounded by rx_log_hold_max_s in
        total so a busy mesh can't stall a send indefinitely. Returns the
        time actually waited."""
        if not self.rx_log_holds_enabled:
            return 0.0
        waited = 0.0
        while True:
            remaining = self._medium_busy_remaining_s()
            if remaining <= 0 or waited >= self.rx_log_hold_max_s:
                break
            step = min(remaining, self.rx_log_hold_max_s - waited)
            await asyncio.sleep(max(0.01, step))
            waited += step
        if waited > 0:
            self._debug(f"medium-busy hold: waited {waited:.2f}s ({self._medium_busy_reason}) before transmitting.")
        return waited

    def _diagnose_missed_ack(self, window: Optional[dict], hop_count: Optional[int]) -> str:
        """Post-miss diagnosis from the attempt's RX window (step 2 data):
        see rx_log_holds_enabled's own comment for the four outcomes."""
        if not window:
            return "no_info"
        target = window.get("target_hash_byte")
        if target and any(len(f) >= 5 and f[4] == target for f in window.get("foreign_rx", ())):
            return "target_busy"
        if window.get("echo_seen_s") is not None:
            return "downstream_loss"
        if hop_count is not None and hop_count >= 1:
            return "hop1_loss"
        return "no_info"

    def _post_miss_hold_s(self, diagnosis: str) -> float:
        jitter = random.uniform(self.rx_log_hold_jitter_min_s, self.rx_log_hold_jitter_max_s)
        if diagnosis == "downstream_loss":
            hold = jitter
        else:
            hold = self._medium_busy_remaining_s() + jitter
        return min(hold, self.rx_log_hold_max_s)

    def _on_rx_log_data(self, event) -> None:
        """One firmware-decoded packet heard on air, addressed to this node
        or not. Runs on this interface's own event loop (the `meshcore`
        dispatcher calls sync subscribers inline), so it must stay cheap:
        bump counters, stamp the last-heard time, write one capture
        record. Never raises -- a malformed/unparseable log frame is the
        library's problem to have already tolerated (reader.py populates
        sentinel fields), and nothing here is allowed to take the data
        path down."""
        if self.detached:
            return
        try:
            payload = event.payload if isinstance(event.payload, dict) else {}
            now = time.monotonic()
            since_last_rx = (now - self._last_rx_log_at) if self._last_rx_log_at is not None else None
            # Alpha 0.1.5 (2a): measured from the estimated END of this
            # node's own last frame on air, not from the send command --
            # negative while a queued burst is still estimated to be on air
            # (the field's radio log read 9 s "idle" with ten queued
            # fragments transmitting).
            since_own_tx = (now - self._radio_busy_until) if self._last_own_tx_at is not None else None
            self._last_rx_log_at = now
            self._rx_log_feed_seen = True
            self._rx_log_events_total += 1
            payload_typename = self._rx_log_typename(payload)
            self._rx_log_by_payload_type[payload_typename] += 1

            fields = self._rx_log_capture_fields(payload, since_last_rx, since_own_tx)
            hold_s, hold_reason = self._predicted_hold_for_rx(fields)
            self._extend_medium_busy(hold_s, hold_reason, now)
            fields["predicted_hold_s"] = round(hold_s, 3)
            fields["hold_reason"] = hold_reason
            fields["medium_busy_remaining_s"] = round(self._medium_busy_remaining_s(now), 3)
            if self._rx_log_window is not None:
                self._classify_rx_log_for_window(fields, now)
            # Alpha 0.1.5 (item 3): a bound peer's flood shows a route to it.
            self._note_flood_route(payload, fields, now)
            if self._packet_capture_file is None and not self.debug_logs:
                return
            if self._packet_capture_file is not None:
                self._capture_event("in", fields)
            self._debug(
                f"rx_log: {fields.get('payload_typename')}/{fields.get('route_typename')} "
                f"len={fields.get('payload_length')} path_len={fields.get('path_len')} "
                f"snr={fields.get('snr')} rssi={fields.get('rssi')} "
                f"src={fields.get('src_hash')} dst={fields.get('dst_hash')} ack={fields.get('ack_code')} "
                f"since_last_rx={since_last_rx if since_last_rx is None else round(since_last_rx, 2)}s "
                f"since_own_tx={since_own_tx if since_own_tx is None else round(since_own_tx, 2)}s"
            )
        except Exception as exc:
            self._debug(f"rx_log handler error (ignored): {exc}")

    def _rx_log_typename(self, payload: dict) -> str:
        """The library's name table stops at CONTROL (11), so a raw packet
        (PAYLOAD_TYPE_RAW_CUSTOM, 0x0F) is reported as "UNK"; name it."""
        name = payload.get("payload_typename")
        if payload.get("payload_type") == self._RX_LOG_PAYLOAD_TYPE_RAW_CUSTOM and (not name or name == "UNK"):
            return "RAW_CUSTOM"
        return str(name if name is not None else "UNK")

    def _rx_log_capture_fields(self, payload: dict, since_last_rx: Optional[float], since_own_tx: Optional[float]) -> dict:
        """The `rx_log` capture record. Everything the firmware/library
        already decoded from the packet's *cleartext* (a DIRECT/CHANNEL
        payload body itself is encrypted end-to-end by the firmware, so
        this interface's own "R"/"P"/"Q" headers are never visible here
        for traffic that isn't ours -- that's fine, the MeshCore-level
        route/type/path is exactly what a third party needs), plus the
        two relative timings that make burst structure readable straight
        off the capture. `dst_hash`/`src_hash` are the 1-byte routing
        hashes MeshCore puts at the front of an addressed payload
        (Dispatcher.cpp's own RX log prints them as `[src -> dst]`);
        `ack_code` is an ACK's 4-byte code, the same value a MSG_SENT
        `expected_ack` carries, so a later analysis can match ACKs seen
        on air against ACK events this node was (or wasn't) delivered.
        `pkt_hash` is the library's hash of the payload -- the same packet
        seen twice in quick succession with a longer `path` is a repeater
        echo."""
        pkt_payload = payload.get("pkt_payload")
        if not isinstance(pkt_payload, (bytes, bytearray)):
            pkt_payload = b""
        payload_type = payload.get("payload_type")
        dst_hash = src_hash = ack_code = None
        if payload_type in self._RX_LOG_ADDRESSED_PAYLOAD_TYPES and len(pkt_payload) >= 2:
            dst_hash, src_hash = f"{pkt_payload[0]:02x}", f"{pkt_payload[1]:02x}"
        elif payload_type == self._RX_LOG_PAYLOAD_TYPE_ACK and len(pkt_payload) >= 4:
            ack_code = bytes(pkt_payload[:4]).hex()
        return {
            "event": "rx_log",
            "snr": payload.get("snr"),
            "rssi": payload.get("rssi"),
            "route_type": payload.get("route_type"),
            "route_typename": payload.get("route_typename"),
            "payload_type": payload_type,
            "payload_typename": self._rx_log_typename(payload),
            "payload_ver": payload.get("payload_ver"),
            "path_len": payload.get("path_len"),
            "path": payload.get("path"),
            "payload_length": payload.get("payload_length"),
            "pkt_hash": payload.get("pkt_hash"),
            "dst_hash": dst_hash,
            "src_hash": src_hash,
            "ack_code": ack_code,
            "since_last_rx_log_s": round(since_last_rx, 3) if since_last_rx is not None else None,
            "since_own_tx_s": round(since_own_tx, 3) if since_own_tx is not None else None,
            "rx_log_events_total": self._rx_log_events_total,
        }
