"""Raw fragmentation and the reconcile: the raw burst-then-report/QUERY sender with its resume and text-fallback verdicts, the report window and QUERY answer budgets, the completion QUERY / ANSWER / REPORT handling on both sides, and the per-sender reassembly buckets, dedup cache and their sweeps. Phase 3 of the 2026-09-20 pass redesigns this module around one burst-and-report state machine."""
import asyncio
import collections
import time
from typing import Optional

import RNS

from ._common import _FrameHeader, _ReassemblyBucket, _CompletionFrame, PRIORITY_NORMAL
from ._locks import _PreemptedForHandshake


# Alpha 0.1.8 (item 1): what `_await_completion_report` returns when the
# far side's RNS proved every packet of the window -- the proof is the
# completion, so there is no report to wait for and nothing to QUERY.
_WINDOW_PROVED = object()


class _ReconcileMixin:
    def _raw_fragments_eligible(self, peer_prefix: str, priority: int) -> bool:
        """Whether a too-big-for-one-text-frame packet to `peer_prefix` may
        go as raw fragments: flag on, library support present, the peer
        advertised BIND_CAP_RAW_FRAGMENTS, a resolved path (the raw send
        is source-routed), our own prefix known (it is the src field),
        not handshake priority, and not inside a fallback cooldown."""
        if not self.direct_raw_fragments_enabled or priority == self.PRIORITY_HANDSHAKE:
            return False
        if not (self.direct_fragment_reconcile_enabled and self.direct_completion_check_enabled):
            return False
        if self._mc is None or not hasattr(self._EventType, "RAW_DATA") or not hasattr(self._mc.commands, "send_raw_data"):
            return False
        peer = self._peers.get(peer_prefix)
        if peer is None or not peer.raw_fragments or peer_prefix not in self._resolved_paths:
            return False
        if self._own_pubkey_prefix() is None:
            return False
        if self._raw_src_ambiguous(peer_prefix):
            return False   # M3: a 2-byte source prefix must name one bound peer
        path_hex = self._resolved_paths[peer_prefix].out_path_hex or ""
        if path_hex and self._raw_path_unsupported(path_hex):
            return False
        until = self._raw_disabled_until.get(peer_prefix)
        return not (until is not None and time.monotonic() < until)

    def _raw_path_unsupported(self, path_hex: str) -> bool:
        note = self._raw_unsupported_paths.get(path_hex)
        if note is None:
            return False
        if time.monotonic() - note["since"] >= self.direct_raw_path_unsupported_ttl_s:
            del self._raw_unsupported_paths[path_hex]
            return False
        return True

    def _note_raw_fallback_outcome(self, peer_prefix: str, path_hex: str, text_ok: bool) -> None:
        """Called after the Z85 text send that followed a raw fallback on
        `path_hex`. Text succeeded -> the chain drops raw packets: note the
        path and lift the peer's pause. Text failed too -> the path is
        sick; nothing is concluded about raw."""
        if not text_ok:
            self._debug(
                f"raw fallback to {peer_prefix!r} on path {path_hex or '<zero-hop>'}: the Z85 text send failed as "
                f"well -- a path problem, not a raw one; raw stays paused for the cooldown only."
            )
            return
        if not path_hex:
            # Zero hop: no repeater to blame -- the peer's own radio did not
            # deliver raw frames. The per-peer pause already covers it.
            RNS.log(
                f"{self}: Z85 text to {peer_prefix!r} succeeded at zero hop where raw fragments did not -- "
                f"raw paused for this peer for {self.direct_raw_fallback_cooldown_s:.0f}s.",
                RNS.LOG_WARNING,
            )
            return
        self._raw_unsupported_paths[path_hex] = {"since": time.monotonic(), "peer": peer_prefix}
        self._raw_disabled_until.pop(peer_prefix, None)
        RNS.log(
            f"{self}: Z85 text to {peer_prefix!r} over path {path_hex} succeeded where raw fragments did not -- "
            f"a repeater in that chain does not carry raw packets; noted for "
            f"{self.direct_raw_path_unsupported_ttl_s / 3600:.0f}h (raw resumes on a different path).",
            RNS.LOG_WARNING,
        )

    # --- Shared fragmented-send helpers (refactor, 2026-09-19) ---------------
    # The text and raw fragmented senders used to carry byte-identical
    # copies of these four pieces; the review that day found three logic
    # gaps in exactly that duplicated region. One copy each, now.

    def _resume_state(self, resume: Optional[dict], frag_total: int, pkt_id: int, peer_prefix: str,
                      raw: bool) -> "tuple[list, bool]":
        """(acked, resumed) to start a fragmented send from: everything
        False for a fresh send, or the remembered per-fragment state when
        `resume` matches this send's fragment count. Logs and captures a
        `direct_resume` record when resuming."""
        if resume is None or resume.get("frag_total") != frag_total or len(resume.get("acked", ())) != frag_total:
            return [False] * frag_total, False
        acked = list(resume["acked"])
        held_before = [i for i, a in enumerate(acked) if a]
        self._debug(
            f"{'RAW' if raw else 'DIRECT'} fragmented send pkt_id={pkt_id} to {peer_prefix!r}: RESUMING a failed "
            f"send -- receiver believed to hold {held_before} of {frag_total}."
        )
        if self._packet_capture_file is not None:
            record = {"event": "direct_resume", "peer_prefix": peer_prefix, "pkt_id": pkt_id,
                      "frag_total": frag_total, "held_before": held_before}
            if raw:
                record["raw"] = True
            self._capture_event("out", record)
        return acked, True

    def _remember_resumable(self, resume_key, pkt_id: int, frag_total: int, acked: list,
                            last_progress_at: Optional[float]) -> None:
        """A failed fragmented send with something delivered is worth
        resuming if RNS re-issues these bytes while the receiver's bucket
        is still alive (its idle clock restarted at our last confirmed
        delivery; keep a 25% margin under its timeout)."""
        if resume_key is None or not any(acked) or last_progress_at is None:
            return
        self._resumable_sends[resume_key] = {
            "pkt_id": pkt_id, "frag_total": frag_total, "acked": list(acked),
            "expires_at": last_progress_at + 0.75 * self.reassembly_idle_timeout_s,
        }

    def _held_from_answer(self, answer: "_CompletionFrame", frag_total: int) -> "Optional[set]":
        """The fragments a completion ANSWER says the receiver holds, or
        None when the answer carries no per-fragment information (a v1
        ANSWER without a bitmap -- audit fix 2026-09-19: never read that
        as "holds nothing")."""
        if answer.complete:
            return set(range(frag_total))
        if answer.held is None:
            return None
        return set(answer.held)

    def _resumable_sends_sweep(self, now: float) -> None:
        """Alpha 0.1.1: a failed fragmented send is only worth resuming
        while the receiver's bucket can still be alive."""
        expired = [k for k, v in self._resumable_sends.items() if now >= v["expires_at"]]
        for k in expired:
            del self._resumable_sends[k]

    async def _send_raw_fragment(
        self, path: bytes, frame: bytes, priority: int, telemetry: Optional[dict] = None,
        interrupt: "Optional[asyncio.Event]" = None,
    ) -> bool:
        """One raw fragment out through the same gate every transmission
        passes (quiet defer skipped: a burst is always racing the
        receiver's reassembly clock), then CMD_SEND_RAW_DATA. Returns
        whether the firmware accepted it; never waits for anything after."""
        on_air = 2 + len(path) + len(frame)
        # A source-routed raw fragment is relayed once per path byte; an
        # empty path is the zero-hop class (alpha 0.1.5 duty-cycle ledgers).
        gate = await self._pre_transmit_gate(
            "", skip_quiet_defer=True, duty_cycle_exempt=self._duty_cycle_exempt(priority), on_air_bytes=on_air,
            interrupt=interrupt, relayed=len(path) > 0, telemetry=telemetry,
        )
        if telemetry is not None:
            telemetry["quiet_defer_wait_s"], telemetry["duty_cycle_wait_s"], telemetry["medium_hold_wait_s"] = gate
        await self._run_command(
            self._mc_ready.commands.send_raw_data(frame, path), "send_raw_data", self._EventType.OK,
        )
        self.txb += len(frame)
        return True

    def _raw_fragment_gap_s(self, hops: int, on_air_bytes: int) -> float:
        """Quiet time after one raw fragment before this node transmits
        anything else (2026-09-19 morning field test, both captures).

        Zero hop: `direct_raw_zero_hop_gap` flat -- the receiver sends no
        ACK, only its own processing needs covering. Through repeaters:
        `direct_raw_hop_gap_factor` x `hops` x the fragment's own airtime.
        The chain is a half-duplex pipeline: each repeater re-transmits the
        fragment after a random delay (simple_repeater `getDirectRetransmit
        Delay`: rand(0..5) x `direct_tx_delay_factor` 0.3 x airtime, so 0 to
        1.5 airtimes per hop) and cannot hear the next fragment while it
        does. The pre-fix gap was 2 airtimes regardless of hop count; at 2
        and 4 hops every 2-fragment raw send in both directions lost
        exactly one fragment (laptop pkt 0/1/2, desktop pkt 19/20), and
        solo re-sends of the missing one arrived. `hops` x airtime is the
        chain's collision-free floor with no repeater delay at all; the
        factor of 2 sits between the firmware's mean (1.75x per hop) and
        worst case (2.5x). At one hop this equals the pre-fix gap the first
        raw field test passed with."""
        if hops <= 0:
            return max(0.0, self.direct_raw_zero_hop_gap_s)
        # MeshBench finding 2 (2026-09-20, real firmware): the gap starts when
        # send_raw_data returns OK, which the firmware gives when the frame is
        # QUEUED, so the fragment's own airtime was eaten out of the gap and
        # the next fragment (or the QUERY) left ~0.9s after the frame ended,
        # inside the repeater's relay of it -- 7/7 second fragments lost at R
        # in large_payload, 7/9 QUERYs in relay. The frame's own airtime is
        # now added on top of the hop-scaled term.
        airtime = self._estimate_tx_airtime_s("", on_air_bytes=on_air_bytes)
        # Alpha 0.1.5 (item 4): the field A/B's `no` arm drops the frame's own
        # airtime from the gap through repeaters; the default keeps it.
        own = 1.0 if self.direct_raw_gap_own_airtime else 0.0
        return max(0.0, (own + self.direct_raw_hop_gap_factor * hops) * airtime)

    def _raw_burst_next_send_wait_s(self, hops: int, gap_s: float, airtime_s: float, now: float,
                                    busy_until: float, queue_ahead: Optional[int] = None) -> float:
        """How long the burst loop sleeps before handing the firmware the
        NEXT fragment (pure function, alpha 0.1.5 2a). Through repeaters the
        hop-scaled gap (which contains the frame's airtime) is the answer, as
        before. At zero hop the flat gap alone let the loop queue a whole
        window into the firmware in seconds; the loop now also waits until
        the radio is estimated to have at most `direct_raw_burst_queue_ahead`
        frames of air left ahead of it -- `busy_until - queue_ahead x
        airtime` -- so the air stays back to back with a bounded queue and
        the loop's own clock tracks the radio's. 0 frames ahead disables
        the pacing (the pre-0.1.5 loop)."""
        wait_s = max(0.0, gap_s)
        queue_ahead = self.direct_raw_burst_queue_ahead if queue_ahead is None else queue_ahead
        if hops <= 0 and queue_ahead > 0:
            wait_s = max(wait_s, (busy_until - queue_ahead * max(0.0, airtime_s)) - now)
        return max(0.0, wait_s)

    def _completion_report_wait_s(self, hops: int, peer_prefix: str) -> float:
        """How long a raw sender keeps its radio quiet after a burst for the
        receiver's unsolicited completion report (2026-09-20): `direct_raw_
        report_wait_base` + `..._per_hop` x hops -- the report's own airtime
        plus one relay per repeater with the firmware's random forward
        delay, the same physics `_completion_quiet_window_s` sizes for a
        QUERY's answer -- never longer than the answer budget a QUERY would
        get. A report that does not arrive in that time was lost or is
        queued behind the receiver's own sends, and waiting longer only
        delays the QUERY fallback (third cut: the first two waited for a
        receiver-side idle timer as well and MeshBench `large_payload`, a
        lossy bidirectional one-hop case with a 60 s per-probe deadline,
        went 0/6 against a 1/6-4/6 baseline)."""
        window_s = self.direct_raw_report_wait_base_s + self.direct_raw_report_wait_per_hop_s * max(0, hops)
        # Phase 1 (2026-09-20): the measured report latency (burst end ->
        # report arrival, late reports included so the estimate is not
        # truncated by the window it sizes) widens the window above the
        # hop-scaled floor; the QUERY answer budget still caps it.
        rs = self._report_rtt.get(peer_prefix)
        if rs is not None:
            # srtt + 2 x rttvar (the factor `_completion_quiet_window_s`
            # uses), not RFC 6298's 4: a lost report at zero hop costs the
            # whole window and the true distribution there is median ~3 s,
            # p90 ~7 s (2026-09-20 review), which +4 x rttvar overshoots.
            window_s = max(window_s, rs["srtt"] + 2.0 * rs["rttvar"])
        budget_s = self._completion_query_timeout_s(peer_prefix, hops)
        return max(0.0, min(window_s, budget_s))

    def _record_report_latency(self, peer_prefix: Optional[str], pkt_id: int) -> Optional[float]:
        """One burst-end -> REPORT-arrival sample for the window estimator
        (2026-09-20), taken in `_handle_incoming_completion_frame` for every
        report that matches an expectation `_send_direct_raw_fragmented`
        registered -- whether the report arrives inside the window or after
        it (the sender may already be in its QUERY fallback). Returns the
        latency, or None when nothing was expected."""
        burst_end = self._report_expected.get((peer_prefix, pkt_id))
        if burst_end is None:
            return None
        latency_s = time.monotonic() - burst_end
        if latency_s < 0:
            # 2a: a report that lands before the burst's estimated end on air
            # measures the airtime estimate's pessimism, not the report path;
            # it does not train the window.
            return None
        self._rtt_sample(self._report_rtt, peer_prefix, latency_s)
        return latency_s

    async def _wait_future_or_proof(self, fut: "asyncio.Future", timeout_s: float, events):
        """The report future against the events this window's PROOFs set,
        with the radio already released (alpha 0.1.8, item 1). Returns the
        report if it arrived, else None -- the caller re-checks whether a
        proof completed the window and, if not, keeps waiting."""
        if fut.done():
            return fut.result()
        if timeout_s <= 0:
            return None
        if any(e.is_set() for e in events):
            return None
        loop = asyncio.get_running_loop()
        fut_wait = loop.create_task(asyncio.wait_for(asyncio.shield(fut), timeout=timeout_s))
        waits = [fut_wait] + [loop.create_task(e.wait()) for e in events]
        try:
            await asyncio.wait(set(waits), return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in waits:
                if not t.done():
                    t.cancel()
            for t in waits:
                try:
                    await t
                except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    pass
        return fut.result() if fut.done() else None

    def _expect_report(self, peer_prefix: str, pkt_id: int, burst_end: Optional[float]) -> None:
        """Register (or, with None, withdraw) the burst end time a report
        for (peer, pkt_id) is measured against."""
        key = (peer_prefix, pkt_id)
        if burst_end is None:
            self._report_expected.pop(key, None)
        else:
            self._report_expected[key] = burst_end

    async def _await_completion_report(
        self, fut: "asyncio.Future", peer_prefix: str, pkt_id: int, frag_total: int, hops: int, stage: str,
        last_sent_idx: Optional[int] = None, rearm=None, release_lock=None, window_pkts=None,
        burst_end: Optional[float] = None, on_early=None, proved_check=None, proved_events=None,
    ):
        """Wait (lock held, radio quiet) for the receiver's completion
        report after a raw burst; None if none arrived inside
        `_completion_report_wait_s`, in which case the caller falls back to
        the QUERY path. A report is captured as a `completion_check_result`
        with outcome "reported" so the reconcile accounting stays in one
        record type; no record is written when nothing arrives (the QUERY
        that follows writes its own).

        Phase 1 (2026-09-20): an INCOMPLETE report whose only gap is
        `last_sent_idx` -- the burst's last fragment -- is the second-last
        fragment's report (both are flagged), sent by the receiver moments
        before the last fragment landed. Taken as final it re-drives that
        fragment as a duplicate: 20 of the desktop's 43 "reported" hop-0
        rounds in the 2026-09-20 session did exactly that (each followed by
        a one-fragment round and a second report). It is now provisional:
        `rearm()` puts a fresh future under the same key so the complete
        report can still land, the wait continues for up to half the window
        more (the complete report follows the incomplete one by a median
        1.5-1.9 s at zero hop -- its ACK wait -- and the receiver-side
        debounce of phase 3 removes the pair at the source), and the
        provisional report is acted on only if nothing better arrives
        (captured with `provisional: true`). When the last fragment really
        was lost this costs at most that extra half window; today's
        behaviour (re-drive it at once) is the fallback either way."""
        wait_s = self._completion_report_wait_s(hops, peer_prefix)
        # 2a (alpha 0.1.5): the window is measured from the burst's
        # estimated end on air (`burst_end`, the radio's busy-until when the
        # last fragment was queued), which may still be in the future.
        started = time.monotonic() if burst_end is None else max(burst_end, 0.0)
        provisional: Optional[_CompletionFrame] = None
        deadline_s = wait_s
        # 2c (alpha 0.1.5): a report that arrives BEFORE the burst has ended
        # on air -- the future already resolved when this wait starts, or a
        # report landing while `time.monotonic() < burst_end` -- is progress,
        # not the end of the wait: `on_early(frame)` applies it to the parts
        # it mentions and says whether anything is still missing; the wait
        # then continues to burst_end + window for the receiver's word on
        # the rest. Only when that expires is the last early report acted
        # on (captured as `reported_stale`), so parts absent from any report
        # are re-burst only after the wait has actually expired -- the
        # field's part-8 report ended the wait with parts 9-12 unmentioned
        # and still in the sender's own radio queue. With nothing missing
        # after an early report the wait ends at once.
        early: Optional[_CompletionFrame] = None
        early_reports = 0
        done_at_entry = fut.done()
        released = release_lock is None
        while True:
            # Alpha 0.1.8 (item 1): RNS proves every single-destination DATA
            # packet, and the proof says the same thing the report would --
            # "I have it" -- with none of the report's own frame, hold or
            # QUERY behind it. Checked before the wait and again after every
            # wakeup, since a proof may have landed during the burst.
            if proved_check is not None and proved_check():
                self._debug(
                    f"RAW window to {peer_prefix!r} ({stage}): every packet proved by the far side's RNS "
                    f"{max(0.0, time.monotonic() - started):.1f}s after the burst -- no report wait, no QUERY."
                )
                return _WINDOW_PROVED
            remaining = deadline_s - (time.monotonic() - started)
            if remaining <= 0:
                got = None
            else:
                try:
                    if not released:
                        # Phase 1 (2026-09-20): a queued Link handshake takes
                        # the radio; the rest of this wait is radio-free (the
                        # report future outlives the wait either way). Item 6
                        # (alpha 0.1.5): so does a completion REPORT this
                        # node owes the far sender -- under both-ways load
                        # it waited 11-13 s behind this very wait.
                        done, cut = await self._wait_future_or_preempt(
                            fut, remaining, also_reports=True, extra_events=proved_events)
                        if cut and proved_check is not None and proved_check():
                            continue
                        if cut:
                            release_lock()
                            released = True
                            self._debug(
                                f"report wait ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}) released the radio to a "
                                f"queued Link handshake or completion report after {time.monotonic() - started:.2f}s; "
                                f"still listening for the report."
                            )
                            continue
                        got = fut.result() if done else None
                    elif proved_events:
                        # Radio-free, so no lock event is consulted here
                        # (one that stayed set would spin): the report
                        # future against the proof events, whichever first.
                        got = await self._wait_future_or_proof(fut, remaining, proved_events)
                        if got is None and (time.monotonic() - started) < deadline_s - 0.001:
                            # A proof woke this, not the deadline: re-check
                            # at the top of the loop.
                            continue
                    else:
                        got = await asyncio.wait_for(asyncio.shield(fut), timeout=remaining)
                except (asyncio.TimeoutError, Exception):
                    got = None
            if got is None:
                if provisional is not None:
                    got = provisional
                    break
                if early is not None:
                    got = early
                    break
                self._debug(
                    f"no completion REPORT ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}) within "
                    f"{wait_s:.1f}s of the burst -- falling back to a QUERY."
                )
                return None
            arrived_early = on_early is not None and rearm is not None and (
                done_at_entry or (burst_end is not None and time.monotonic() < burst_end))
            done_at_entry = False
            if arrived_early:
                early_reports += 1
                early = got
                still_missing = on_early(got)
                if not still_missing:
                    # Everything this window sent is held: nothing to wait for.
                    break
                fut = rearm()
                self._debug(
                    f"completion REPORT ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}) arrived before the burst "
                    f"ended on air ({self._radio_busy_remaining_s():.1f}s of own air left) -- applied as progress, "
                    f"waiting for the receiver's word on the rest."
                )
                continue
            if got.entries and window_pkts is not None:
                # v4 window report (M2): the gaps of every entry that belongs
                # to this window, as (pkt_id, frag_idx) pairs.
                gaps = {(e[0], i) for e in got.entries if e[0] in window_pkts and e[3] is not None
                        for i in range(e[1]) if i not in e[3]}
                missing_only_last = (
                    last_sent_idx is not None and rearm is not None and gaps == {(pkt_id, last_sent_idx)}
                )
            else:
                missing_only_last = (
                    not got.complete and got.held is not None and last_sent_idx is not None and rearm is not None
                    and set(range(frag_total)) - set(got.held) == {last_sent_idx}
                )
            if missing_only_last and provisional is None:
                provisional = got
                fut = rearm()
                deadline_s = min(wait_s, (time.monotonic() - started) + wait_s / 2.0)
                self._debug(
                    f"completion REPORT ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}) misses only the "
                    f"last fragment sent ({last_sent_idx}) -- the second-last fragment's report; provisional, "
                    f"waiting the rest of the window for the last fragment's own."
                )
                continue
            break
        waited_s = max(0.0, time.monotonic() - started)
        is_provisional = got is provisional
        acted_on_early = early is not None and got is early and (time.monotonic() - started) >= deadline_s - 0.001
        self._debug(
            f"completion REPORT ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}): v{got.version} "
            f"complete={got.complete} held={sorted(got.held) if got.held is not None else None} "
            f"{waited_s:.1f}s after the burst's last gap."
        )
        if self._packet_capture_file is not None:
            self._capture_event("out", {
                "event": "completion_check_result",
                "peer_prefix": peer_prefix, "pkt_id": pkt_id, "frag_total": frag_total,
                "outcome": "reported_stale" if acted_on_early else "reported", "complete": got.complete, "stage": stage,
                "timeout_s": round(wait_s, 3), "answer_version": got.version,
                "held": sorted(got.held) if got.held is not None else None,
                "entries": [(e[0], e[1], e[2], sorted(e[3]) if e[3] is not None else None) for e in got.entries] or None,
                "report_wait_s": round(waited_s, 3), "provisional": is_provisional,
                "early_reports": early_reports,
            })
        return got

    def _record_query_path_evidence(self, peer_prefix: str, infos: "list[dict]", answered: bool = False) -> None:
        """One raw reconcile round's QUERY sends, as stale-path evidence
        (2026-09-19 morning field test). Each QUERY is an ACKed DIRECT
        exchange over the cached path, so its firmware ACK proves the
        path, and `direct_raw_query_attempts` consecutive full-timeout
        misses disprove it as strongly as one text send's exhausted
        `direct_send_attempts` budget does. Before this, a raw send
        recorded one failure only after all its rounds were exhausted and
        the QUERYs recorded nothing: the desktop sat on a dead zero-hop
        path through 17 consecutive full-timeout QUERY misses and three
        whole 70s sends (09:58:23-10:01:05) before `record_direct_send_
        result` reached its threshold of 3. Counted per round, not per
        attempt, so a single lost ACK still is not a path failure."""
        if not infos:
            return
        if answered or any(i.get("acked") for i in infos):
            # An ANSWER proves the path even if the QUERY's own ACK was lost.
            self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True, path_sample=False)
        elif all(i.get("waited_full_timeout") for i in infos):
            self.record_direct_send_result(peer_prefix, succeeded=False, waited_full_timeout=True, path_sample=False)

    async def _raw_path_reset_mid_send(
        self, peer_prefix: str, path: bytes, pkt_id: int, rnd: int, acked: list, frag_total: int, remember,
    ) -> bool:
        """True when the path this raw send was started on is no longer
        the peer's resolved path (reset by `_reset_stale_path`, possibly
        from this send's own QUERY evidence, or re-resolved elsewhere):
        the remaining rounds would burst fragments source-routed down a
        path already known to be dead. The send is remembered for resume
        and abandoned; the next packet for this peer goes through
        discovery. `asyncio.sleep(0)` first lets the background reset
        task spawned a moment ago run before the check."""
        await asyncio.sleep(0)
        resolved = self._resolved_paths.get(peer_prefix)
        if resolved is not None and (resolved.out_path_hex or "") == path.hex():
            return False
        remember()
        self._outgoing_dropped_total += 1
        RNS.log(
            f"{self}: RAW send pkt_id={pkt_id} to {peer_prefix!r}: path {path.hex() or '<zero-hop>'} was reset "
            f"after round {rnd} -- abandoning the remaining rounds rather than bursting down a dead path; "
            f"receiver holds {sum(acked)}/{frag_total}.",
            RNS.LOG_WARNING,
        )
        return True

    # -- The raw window state machine (phase 3 M2, 2026-09-20) --------------
    # docs/reconcile_redesign.md. One machine, `_run_raw_window`, over a
    # window of one or more parts to one peer: burst every missing fragment
    # of every part back to back, flag the last two, keep the radio quiet
    # once for the receiver's single v4 report, apply its entries, re-drive
    # what is missing as the next round's burst; a lost report falls to one
    # v4 QUERY for the outstanding parts. Every rule of the single-part
    # sender it replaces (resume, the provisional second-last report, the
    # re-query-before-re-burst valve, handshake yields, path-reset abort,
    # the fallback strikes and the text fallback) applies to the window as
    # it applied to the part; a single part is a window of one.

    class _RawPart:
        __slots__ = ("data", "pkt_id", "chunks", "frag_total", "acked", "resume_key", "expires_at",
                     "future", "last_progress_at", "resumed", "hop_count", "priority", "proof_key")

        def __init__(self, data, pkt_id, expires_at, resume_key, hop_count, priority):
            self.data = data
            self.pkt_id = pkt_id
            self.chunks = []
            self.frag_total = 0
            self.acked = []
            self.resume_key = resume_key
            self.expires_at = expires_at
            self.future = None
            self.last_progress_at = None
            self.resumed = False
            self.hop_count = hop_count
            self.priority = priority
            # Alpha 0.1.8 (item 1): the truncated hash the PROOF for this
            # packet will carry in its destination field, or None when RNS
            # does not prove this packet per packet (`_answered_send_key`).
            self.proof_key = None

    class _RawWindow:
        __slots__ = ("peer_prefix", "target", "parts", "opened_at", "leader", "closed", "last_join_at")

        def __init__(self, peer_prefix, target):
            self.peer_prefix = peer_prefix
            self.target = target
            self.parts = []
            self.opened_at = time.monotonic()
            self.leader = None
            self.closed = False
            self.last_join_at = self.opened_at   # item 5: the collect's silence clock

    # Alpha 0.1.5 (item 5): the shortest silence after which a window stops
    # waiting for more parts -- RNS's Resource sender emits a window's parts
    # in one loop and the outgoing worker hands them over within a few
    # loop turns, so a lone packet starts within this.
    RAW_WINDOW_COLLECT_FLOOR_S = 0.04
    # How many recent part arrivals per peer feed the spacing estimate.
    RAW_PART_ARRIVALS_KEPT = 8

    def _window_collect_s(self) -> float:
        """The LONGEST a window stays open for further parts after its first
        (pure function): `direct_raw_window_collect` (0.75 s), the maximum
        since alpha 0.1.5 (item 5) -- the collect ends earlier once nothing
        is queued from RNS and no part has joined within the observed
        inter-part spacing (`_window_collect_continue`); 0 when window
        batching is off (a single part never waits)."""
        if not self.direct_raw_window_enabled:
            return 0.0
        return max(0.0, self.direct_raw_window_collect_s)

    def _note_raw_part_arrival(self, peer_prefix: str, now: float) -> None:
        """A raw-eligible part for this peer reached the window machine."""
        arrivals = self._raw_part_arrivals.setdefault(peer_prefix, collections.deque(maxlen=self.RAW_PART_ARRIVALS_KEPT))
        arrivals.append(now)

    def _observed_part_spacing_s(self, peer_prefix: str, max_s: float) -> float:
        """The inter-part spacing of this peer's current transfer (pure over
        the recorded arrivals, alpha 0.1.5 item 5): twice the median gap
        between consecutive recent arrivals that fell inside `max_s` (gaps
        longer than the collect maximum are between windows, not between a
        window's parts), never below RAW_WINDOW_COLLECT_FLOOR_S nor above
        `max_s`; the floor alone when nothing has been observed yet."""
        floor = self.RAW_WINDOW_COLLECT_FLOOR_S
        arrivals = list(self._raw_part_arrivals.get(peer_prefix, ()))
        gaps = sorted(b - a for a, b in zip(arrivals, arrivals[1:]) if 0.0 <= b - a < max_s)
        if not gaps:
            return min(max_s, floor) if max_s > 0 else 0.0
        median = gaps[len(gaps) // 2]
        return max(floor, min(max_s, 2.0 * median))

    def _window_collect_continue(self, now: float, opened_at: float, last_join_at: float, n_parts: int,
                                 max_parts: int, queue_depth: int, spacing_s: float, max_s: float) -> bool:
        """Whether the window keeps collecting (pure function, alpha 0.1.5
        item 5): never past `max_s` from opening or `max_parts` parts; while
        the outgoing queue still holds packets (RNS's next part may be one
        of them) or a part joined within the observed inter-part spacing.
        A lone part -- nothing queued, no join within the spacing -- stops
        within the floor instead of the 0.75 s maximum, which the
        zero-hop probe RTT paid on every single packet."""
        if now - opened_at >= max_s or n_parts >= max_parts:
            return False
        if queue_depth > 0:
            return True
        return (now - last_join_at) < spacing_s

    async def _send_direct_raw_fragmented(
        self, target: str, peer_prefix: str, payload: bytes, pkt_id: int,
        priority: int = PRIORITY_NORMAL, hop_count: Optional[int] = None,
        expires_at: Optional[float] = None, resume: Optional[dict] = None, resume_key=None,
    ) -> Optional[bool]:
        """Raw binary fragments (2026-09-18 night; the window machine since
        phase 3 M2, 2026-09-20): join (or open) this peer's window with
        this part and wait for the part's outcome. Returns True
        (delivered), False (failed, recorded), or None (raw declined or
        disabled for this peer mid-way -- the caller re-sends as text
        fragments)."""
        resolved = self._resolved_paths.get(peer_prefix)
        own_prefix = self._own_pubkey_prefix()
        if resolved is None or own_prefix is None:
            return None
        try:
            path = bytes.fromhex(resolved.out_path_hex or "")
        except ValueError:
            return None
        budget = self._direct_raw_payload_budget(len(path))
        if budget <= 0:
            return None
        chunks = self._chunk_payload(payload, budget)
        if len(chunks) > 255:
            return None
        if self._expired(expires_at):
            self._outgoing_dropped_total += 1
            RNS.log(f"{self}: dropping raw DIRECT send to {peer_prefix!r} -- packet expired before its first transmission.", RNS.LOG_WARNING)
            return False
        part = self._RawPart(payload, pkt_id, expires_at, resume_key, hop_count, priority)
        # Alpha 0.1.8 (item 1): the same key the bare DIRECT path already
        # uses to stop retrying once the proof lands (`_answered_send_key`,
        # `_direct.py`) -- a plain DATA to a SINGLE destination only, so a
        # Link packet's proof (which carries the link_id and so names no
        # particular packet), a Resource part and an announce all leave it
        # None and those windows keep reporting as they do today.
        part.proof_key = self._answered_send_key(payload, self._parse_rns_header(payload))
        part.chunks = chunks
        part.frag_total = len(chunks)
        part.acked, part.resumed = self._resume_state(resume, part.frag_total, pkt_id, peer_prefix, raw=True)
        part.last_progress_at = time.monotonic() if part.resumed else None
        part.future = asyncio.get_running_loop().create_future()
        self._note_raw_part_arrival(peer_prefix, time.monotonic())
        window = self._raw_windows.get(peer_prefix)
        max_parts = max(1, self.direct_raw_window_max_parts)
        if window is None or window.closed or len(window.parts) >= max_parts or window.target != target:
            window = self._RawWindow(peer_prefix, target)
            self._raw_windows[peer_prefix] = window
            window.parts.append(part)
            window.leader = part
            self._last_fragmented_pkt_id = pkt_id
            self._last_fragmented_frag_total = part.frag_total
            try:
                await self._run_raw_window(window, path, own_prefix)
            finally:
                if self._raw_windows.get(peer_prefix) is window:
                    self._raw_windows.pop(peer_prefix, None)
                for p in window.parts:
                    if not p.future.done():
                        p.future.set_result(None)
            return part.future.result()
        window.parts.append(part)
        window.last_join_at = time.monotonic()
        self._debug(f"RAW part pkt_id={pkt_id} joined the open window to {peer_prefix!r} ({len(window.parts)} parts).")
        return await part.future

    def _frame_entries(self, frame: "_CompletionFrame") -> tuple:
        """A completion frame's per-part entries: the v4 list, or the single
        v1-v3 part as one entry (held None when it carried no bitmap)."""
        if frame.entries:
            return tuple(frame.entries)
        return ((frame.pkt_id, frame.frag_total, frame.complete, self._held_from_answer(frame, frame.frag_total)),)

    def _capture_window_proved(self, peer_prefix: str, parts, stage: str) -> None:
        """The `proved` outcome, recorded in the same record type the
        report and the QUERY answer use, so the reconcile accounting stays
        in one place (alpha 0.1.8, item 1)."""
        if self._packet_capture_file is None:
            return
        for part in parts:
            self._capture_event("out", {
                "event": "completion_check_result", "peer_prefix": peer_prefix,
                "pkt_id": part.pkt_id, "frag_total": part.frag_total,
                "outcome": "proved", "complete": True, "stage": stage,
                "timeout_s": None, "answer_version": None, "held": None, "entries": None,
                "report_wait_s": None, "provisional": False, "early_reports": 0,
                "proved_by": self._send_answered_by(part.proof_key) if part.proof_key is not None else None,
            })

    def _mark_part_proved(self, window, part, peer_prefix: str) -> None:
        """One part of a window completed by the PROOF its packet drew from
        the far side's RNS, rather than by a completion report (alpha
        0.1.8, item 1). The same three steps `_apply_window_entries` takes
        for a part a report completed."""
        part.acked = [True] * part.frag_total
        part.last_progress_at = time.monotonic()
        self._resumable_sends.pop(part.resume_key, None)
        if not part.future.done():
            part.future.set_result(True)
        self._debug(f"RAW part pkt_id={part.pkt_id} to {peer_prefix!r} complete (proof).")

    def _window_proved_parts(self, parts, peer_prefix: str) -> list:
        """The parts of this window whose packet the far side's RNS has
        proved, from the signal `_signal_send_answered` already raises for
        every inbound DIRECT PROOF (`_peers.py`). `proof_key` is None for
        anything RNS does not prove per packet -- a Resource part, anything
        inside a Link, an announce -- so those parts never appear here and
        the window keeps waiting for its report."""
        proved = []
        for part in parts:
            key = getattr(part, "proof_key", None)
            if key is None or part.future.done():
                continue
            if self._answered_send_event(key).is_set():
                proved.append(part)
        return proved

    def _window_all_proved(self, parts, peer_prefix: str) -> bool:
        """True when every part still outstanding has been proved -- the
        window is complete and neither the report wait nor a QUERY has
        anything left to ask about."""
        outstanding = [p for p in parts if not p.future.done()]
        if not outstanding:
            return False
        if any(getattr(p, "proof_key", None) is None for p in outstanding):
            return False
        return all(self._answered_send_event(p.proof_key).is_set() for p in outstanding)

    def _window_missing(self, parts) -> list:
        """[(part, frag_idx)] still to send, in part order."""
        return [(p, i) for p in parts if not p.future.done() for i in range(p.frag_total) if not p.acked[i]]

    def _apply_window_entries(self, window, entries, source: str) -> "tuple[int, int]":
        """Apply a v4 report / answer to the window's parts: each entry's
        bitmap is authoritative for its part (the receiver's bucket decides,
        2026-09-18). Returns (parts newly complete, parts that gained a
        fragment). Entries for pkt_ids not in the window are ignored."""
        by_pkt = {p.pkt_id: p for p in window.parts if not p.future.done()}
        completed = progressed = 0
        for pkt_id, frag_total, complete, held in entries:
            part = by_pkt.get(pkt_id)
            if part is None or frag_total != part.frag_total:
                continue
            if held is None:
                continue   # a v1-style "no bitmap": no per-fragment information
            before = sum(part.acked)
            part.acked = [i in held for i in range(part.frag_total)]
            if complete:
                part.acked = [True] * part.frag_total
            if sum(part.acked) > before:
                part.last_progress_at = time.monotonic()
                progressed += 1
            if all(part.acked):
                completed += 1
                self._resumable_sends.pop(part.resume_key, None)
                part.future.set_result(True)
                self._debug(f"RAW part pkt_id={pkt_id} to {window.peer_prefix!r} complete ({source}).")
        return completed, progressed

    async def _run_raw_window(self, window, path: bytes, own_prefix: str) -> None:
        """The burst-and-report state machine for one window (docs/
        reconcile_redesign.md, M2). Sets every part's future: True
        delivered, False failed (recorded), None -> the caller's text
        fallback."""
        peer_prefix, target = window.peer_prefix, window.target
        collect_s = self._window_collect_s()
        if collect_s > 0:
            # Item 5 (alpha 0.1.5): adaptive collect -- stop as soon as
            # nothing is queued from RNS and no part has joined within the
            # transfer's observed inter-part spacing; `collect_s` is the cap.
            spacing_s = self._observed_part_spacing_s(peer_prefix, collect_s)
            while self._window_collect_continue(
                time.monotonic(), window.opened_at, window.last_join_at, len(window.parts),
                max(1, self.direct_raw_window_max_parts), self._outqueue.qsize(), spacing_s, collect_s,
            ):
                await asyncio.sleep(0.01)
            if self._packet_capture_file is not None:
                self._capture_event("out", {
                    "event": "raw_window_collect", "peer_prefix": peer_prefix, "parts": len(window.parts),
                    "collect_s": round(time.monotonic() - window.opened_at, 3), "spacing_s": round(spacing_s, 3),
                    "max_s": collect_s,
                })
        window.closed = True
        parts = window.parts
        hop_count = parts[0].hop_count
        priority = min(p.priority for p in parts)
        gap_hops = max(0, hop_count if hop_count is not None else len(path))
        # The window takes the per-peer in-flight slot the parts skipped
        # (`_send_direct_payload`): same budget and same proceed-with-a-
        # warning rule as a single fragmented send had.
        slot = self._fragmented_send_slot(peer_prefix, priority) if self.direct_raw_window_enabled else None
        slot_held = False
        if slot is not None:
            budget_s = max(0.0, min(self.outgoing_max_age_s,
                                    min((p.expires_at - time.monotonic()) for p in parts if p.expires_at is not None)
                                    if any(p.expires_at is not None for p in parts) else self.outgoing_max_age_s))
            try:
                await asyncio.wait_for(slot.acquire(priority), timeout=budget_s)
                slot_held = True
            except asyncio.TimeoutError:
                RNS.log(f"{self}: raw window to {peer_prefix!r} waited {budget_s:.0f}s for an in-flight slot without getting one -- sending anyway.", RNS.LOG_WARNING)
        try:
            await self._run_raw_window_rounds(window, parts, path, own_prefix, hop_count, priority, gap_hops)
        finally:
            if slot_held:
                slot.release()

    @staticmethod
    def _raw_window_rounds_rule(reconcile_rounds: int, max_rounds_relayed: int, hops: int) -> int:
        """Burst-and-reconcile rounds a window gets (pure, alpha 0.1.6 item
        2): `reconcile_rounds` at zero hop; through repeaters the smaller
        of that and `max_rounds_relayed` (0: no separate cap). The field's
        two-hop windows ran three rounds of a burst, a report wait and up
        to two ~18 s QUERY exchanges each while link proofs waited."""
        rounds = max(1, int(reconcile_rounds))
        if hops >= 1 and max_rounds_relayed > 0:
            rounds = max(1, min(rounds, int(max_rounds_relayed)))
        return rounds

    def _raw_window_rounds(self, gap_hops: int) -> int:
        return self._raw_window_rounds_rule(self.direct_raw_reconcile_rounds, self.direct_raw_window_max_rounds, gap_hops)

    async def _run_raw_window_rounds(self, window, parts, path: bytes, own_prefix: str, hop_count, priority: int, gap_hops: int) -> None:
        peer_prefix, target = window.peer_prefix, window.target
        self._debug(
            f"RAW window to {peer_prefix!r}: {len(parts)} part(s) {[p.pkt_id for p in parts]} "
            f"({sum(p.frag_total for p in parts)} fragments) path_len={len(path)} hop_count={hop_count}"
            + (" (resumed part)" if any(p.resumed for p in parts) else "") + "."
        )
        if self._packet_capture_file is not None:
            self._capture_event("out", {
                "event": "raw_window", "peer_prefix": peer_prefix, "parts": [p.pkt_id for p in parts],
                "fragments": sum(p.frag_total for p in parts), "hop_count": hop_count,
            })

        def remember_all() -> None:
            for p in parts:
                if not p.future.done():
                    self._remember_resumable(p.resume_key, p.pkt_id, p.frag_total, p.acked, p.last_progress_at)

        def fail_rest(value) -> None:
            for p in parts:
                if not p.future.done():
                    p.future.set_result(value)

        rounds = self._raw_window_rounds(gap_hops)
        query_unanswered_rounds = 0
        empty_answered_bursts = 0
        burst_allowed = True
        consecutive_unanswered = 0
        for rnd in range(rounds):
            if rnd > 0:
                # Expiry per part (audit fix 2026-09-19: re-checked every round).
                for p in parts:
                    if not p.future.done() and self._expired(p.expires_at):
                        self._outgoing_dropped_total += 1
                        self._remember_resumable(p.resume_key, p.pkt_id, p.frag_total, p.acked, p.last_progress_at)
                        self._debug(f"RAW part pkt_id={p.pkt_id} to {peer_prefix!r}: expired before round {rnd} (outgoing_max_age).")
                        p.future.set_result(False)
            missing = self._window_missing(parts)
            if not missing:
                return
            burst_this_round = burst_allowed
            if not burst_this_round:
                self._debug(
                    f"RAW window to {peer_prefix!r}: round {rnd} -- last reconcile unanswered, "
                    f"re-querying instead of re-bursting {len(missing)} fragment(s)."
                )
            report: Optional[_CompletionFrame] = None
            live_pkts = sorted({p.pkt_id for p, _ in missing})
            report_fut = None
            if burst_this_round:
                # One waiter future for the whole window, registered under
                # every live pkt_id BEFORE the burst (the receiver may report
                # while the sender is still in the last fragment's gap).
                report_nonce = self.COMPLETION_REPORT_NONCE_BASE | (rnd & 0x03)
                if self.direct_raw_report_enabled:
                    report_fut = asyncio.get_running_loop().create_future()
                    for p, _ in missing:
                        self._completion_query_waiters[(peer_prefix, p.pkt_id)] = (report_fut, p.frag_total, report_nonce)

                def rearm(_fut_holder=[None]):
                    fresh = asyncio.get_running_loop().create_future()
                    for p, _ in missing:
                        self._completion_query_waiters[(peer_prefix, p.pkt_id)] = (fresh, p.frag_total, report_nonce)
                    return fresh

                lock = self._direct_exchange_lock
                await lock.acquire(priority)
                lock_held = True
                yields = 0
                report_yields = 0

                def release_for_handshake() -> None:
                    nonlocal lock_held
                    if lock_held:
                        lock.release()
                        lock_held = False

                # M4: the burst as a list of (part, frag_idx-or-None, frame
                # builder); a parity frame follows each part's data
                # fragments when parity applies to this hop count, the
                # frame fits, and the part sends two or more fragments.
                burst = []
                n_parity = self._raw_parity_fragments(gap_hops) if self._raw_parity_fits(
                    self._direct_raw_payload_budget(len(path)), len(path)) else 0
                for part in parts:
                    idxs = [i for p, i in missing if p is part]
                    for i in idxs:
                        burst.append((part, i, None))
                    if n_parity and len(idxs) >= 2:
                        burst.append((part, None, [(i, part.chunks[i]) for i in idxs]))
                try:
                    for n, (part, frag_idx, parity_over) in enumerate(burst):
                        if self.detached or not self.online:
                            remember_all()
                            fail_rest(False)
                            return
                        flagged = report_fut is not None and n >= len(burst) - 2
                        if parity_over is None:
                            frame = self._encode_raw_fragment(
                                part.chunks[frag_idx], target, own_prefix, part.pkt_id, frag_idx, part.frag_total, attempt=rnd,
                                report=flagged,
                            )
                        else:
                            frame = self._encode_raw_parity(
                                parity_over, target, own_prefix, part.pkt_id, part.frag_total, attempt=rnd, report=flagged,
                            )
                        telemetry: dict = {}
                        while True:
                            try:
                                await self._send_raw_fragment(path, frame, priority, telemetry, interrupt=lock.preempt_event())
                                sent_ok = True
                            except _PreemptedForHandshake:
                                yields += 1
                                await lock.yield_to_preempt()
                                if await self._raw_path_reset_mid_send(peer_prefix, path, part.pkt_id, rnd, part.acked, part.frag_total, remember_all):
                                    fail_rest(False)
                                    return
                                continue
                            except Exception as exc:
                                sent_ok = False
                                RNS.log(f"{self}: raw fragment send failed locally (pkt_id={part.pkt_id} frag_idx={frag_idx}): {exc}", RNS.LOG_WARNING)
                            break
                        if self._packet_capture_file is not None:
                            self._capture_event("out", {
                                "event": "raw_fragment_sent", "peer_prefix": peer_prefix, "pkt_id": part.pkt_id,
                                "frag_idx": frag_idx, "frag_total": part.frag_total, "round": rnd, "ok": sent_ok,
                                "size_bytes": len(frame), "path_len": len(path), "hop_count": hop_count,
                                "on_air_bytes": (2 + len(path) + len(frame)) if sent_ok else None,
                                "gap_s": round(self._raw_fragment_gap_s(gap_hops, 2 + len(path) + len(frame)), 3),
                                "duty_cycle_wait_s": telemetry.get("duty_cycle_wait_s"),
                                "duty_cycle_ledger": telemetry.get("duty_cycle_ledger"),
                                "medium_hold_wait_s": telemetry.get("medium_hold_wait_s"),
                                "handshake_yields": yields, "report_yields": report_yields, "window_parts": len(parts),
                                "parity_mask": (sum(1 << i for i, _p in parity_over) if parity_over is not None else None),
                            })
                        on_air_bytes = 2 + len(path) + len(frame)
                        gap_s = self._raw_fragment_gap_s(gap_hops, on_air_bytes)
                        # 2a: at zero hop, also wait for the radio to catch
                        # up (bounded firmware queue, loop clock = air clock).
                        wait_s = self._raw_burst_next_send_wait_s(
                            gap_hops, gap_s, self._estimate_tx_airtime_s("", on_air_bytes=on_air_bytes),
                            time.monotonic(), self._radio_busy_until,
                        )
                        if wait_s > 0:
                            await asyncio.sleep(wait_s)
                        if n < len(burst) - 1 and lock.preempt_requested():
                            yields += 1
                            await lock.yield_to_preempt()
                            if await self._raw_path_reset_mid_send(peer_prefix, path, part.pkt_id, rnd, part.acked, part.frag_total, remember_all):
                                fail_rest(False)
                                return
                        elif n < len(burst) - 1 and burst[n + 1][0] is not part and lock.report_requested():
                            # Item 6 (alpha 0.1.5): between two PARTS of the
                            # window (never inside a part's burst) a
                            # completion REPORT this node owes the far
                            # sender goes out first -- under both-ways load
                            # its report otherwise waits behind the whole
                            # window (12-15 s observed in the phase-4 slow
                            # scenario) while the far sender's report wait
                            # expires and it re-queries. Resumes behind the
                            # report's tier, ahead of ordinary waiters.
                            report_yields += 1
                            await lock.yield_to_preempt(lock.REPORT_YIELDED_PRIORITY)
                            if await self._raw_path_reset_mid_send(peer_prefix, path, part.pkt_id, rnd, part.acked, part.frag_total, remember_all):
                                fail_rest(False)
                                return
                    if yields or report_yields:
                        self._debug(f"RAW window to {peer_prefix!r}: round {rnd} yielded the radio to a Link handshake "
                                    f"{yields} time(s) and to a completion report {report_yields} time(s).")
                    if report_fut is not None:
                        # 2a: the burst ends when the radio is estimated to
                        # have finished the last queued fragment, not when
                        # the last send command returned -- the report wait
                        # and the report-latency estimator both anchor here.
                        burst_end = max(time.monotonic(), self._radio_busy_until)
                        for p, _ in missing:
                            self._expect_report(peer_prefix, p.pkt_id, burst_end)
                        last_part, last_idx = missing[-1]

                        def apply_early(frame) -> bool:
                            # 2c: a report from before the burst's end on
                            # air (the field's part-8 report) is applied to
                            # the parts it names; True while parts of this
                            # window are still missing.
                            self._apply_window_entries(window, self._frame_entries(frame), "early report")
                            return bool(self._window_missing(parts))

                        # Alpha 0.1.8 (item 1): the PROOFs this window's own
                        # packets will draw. `_signal_send_answered` already
                        # sets these for every inbound DIRECT PROOF; nothing
                        # was listening on the raw path until now.
                        proof_keys = [p.proof_key for p, _ in missing if p.proof_key is not None]
                        proved_events = [self._answered_send_event(k) for k in dict.fromkeys(proof_keys)]
                        report = await self._await_completion_report(
                            report_fut, peer_prefix, last_part.pkt_id, last_part.frag_total, gap_hops, stage=f"raw{rnd}",
                            last_sent_idx=last_idx, rearm=rearm, release_lock=release_for_handshake,
                            window_pkts=live_pkts, burst_end=burst_end, on_early=apply_early,
                            proved_check=(lambda: self._window_all_proved(parts, peer_prefix)) if proved_events else None,
                            proved_events=proved_events or None,
                        )
                finally:
                    release_for_handshake()
                for p, _ in missing:
                    self._completion_query_waiters.pop((peer_prefix, p.pkt_id), None)
                if report is _WINDOW_PROVED:
                    # Alpha 0.1.8 (item 1): the window ends here. No report
                    # wait was spent past the proof, no QUERY is sent, and
                    # the parts are marked exactly as a report would mark
                    # them. Path evidence only from a proof the PEER this
                    # window was addressed to delivered -- the same rule the
                    # bare path's `answered` outcome applies, since a proof
                    # relayed by another peer or over CHANNEL says nothing
                    # about this peer's path.
                    proved_parts = list(dict.fromkeys(p for p, _ in missing))
                    for part in proved_parts:
                        self._mark_part_proved(window, part, peer_prefix)
                    for p, _ in missing:
                        self._expect_report(peer_prefix, p.pkt_id, None)
                    if any(self._send_answered_by(p.proof_key) == peer_prefix
                           for p in proved_parts if p.proof_key is not None):
                        self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
                    self._raw_incomplete_strikes.pop(peer_prefix, None)
                    self._capture_window_proved(peer_prefix, proved_parts, stage=f"raw{rnd}")
                    return
            held_before = sum(sum(p.acked) for p in parts)
            answer = report
            query_infos: list = []
            if report is not None:
                self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
            for q in range(max(1, self.direct_raw_query_attempts) if report is None else 0):
                info: dict = {}
                answer = await self._query_remote_fragments(
                    target, peer_prefix, live_pkts[0], next(p.frag_total for p, _ in missing if p.pkt_id == live_pkts[0]),
                    stage=f"raw{rnd}", priority=priority, hop_count=hop_count, send_info=info,
                    entries=[(p.pkt_id, p.frag_total) for p in parts if not p.future.done()],
                )
                query_infos.append(info)
                if answer is not None or self.detached or not self.online:
                    break
            for p, _ in missing:
                self._expect_report(peer_prefix, p.pkt_id, None)
            if self.detached or not self.online:
                remember_all()
                fail_rest(False)
                return
            self._record_query_path_evidence(peer_prefix, query_infos, answered=answer is not None)
            if answer is None:
                query_unanswered_rounds += 1
                consecutive_unanswered += 1
                burst_allowed = 0 < self.direct_raw_reburst_after_unanswered <= consecutive_unanswered
                self._debug(f"RAW window to {peer_prefix!r}: round {rnd} reconcile unanswered.")
                if await self._raw_path_reset_mid_send(peer_prefix, path, live_pkts[0], rnd, parts[0].acked, parts[0].frag_total, remember_all):
                    fail_rest(False)
                    return
                continue
            entries = self._frame_entries(answer)
            if all(e[3] is None for e in entries):
                query_unanswered_rounds += 1
                consecutive_unanswered += 1
                burst_allowed = 0 < self.direct_raw_reburst_after_unanswered <= consecutive_unanswered
                self._debug(f"RAW window to {peer_prefix!r}: round {rnd} answered with no per-fragment information -- treated as unanswered.")
                continue
            completed, _progressed = self._apply_window_entries(window, entries, "report" if report is not None else "answer")
            burst_allowed = True
            consecutive_unanswered = 0
            if not self._window_missing(parts):
                self.record_direct_send_result(peer_prefix, succeeded=True, waited_full_timeout=True)
                self._raw_incomplete_strikes.pop(peer_prefix, None)
                return
            held_now = sum(sum(p.acked) for p in parts)
            if held_now <= held_before and burst_this_round:
                empty_answered_bursts += 1
                if empty_answered_bursts >= max(1, self.direct_raw_fallback_strikes):
                    self._raw_disabled_until[peer_prefix] = time.monotonic() + self.direct_raw_fallback_cooldown_s
                    nothing_ever_held = not any(any(p.acked) for p in parts)
                    if nothing_ever_held:
                        self._raw_fallback_pending[(peer_prefix, path.hex())] = time.monotonic()
                    RNS.log(
                        f"{self}: raw fragments to {peer_prefix!r} are not arriving over path "
                        f"{path.hex() or '<zero-hop>'} ({empty_answered_bursts} answered reconciles, nothing new "
                        f"held; window {[p.pkt_id for p in parts]}) -- re-sending the rest as Z85 text"
                        + ("; if that succeeds the path is noted as not carrying raw." if nothing_ever_held
                           else " (raw did deliver part of it, so no verdict on the chain)."),
                        RNS.LOG_WARNING,
                    )
                    remember_all()
                    fail_rest(None)
                    return
            else:
                empty_answered_bursts = 0

        remember_all()
        if query_unanswered_rounds == rounds:
            self.record_direct_send_result(peer_prefix, succeeded=False, waited_full_timeout=True)
            RNS.log(
                f"{self}: RAW window to {peer_prefix!r} gave up after {rounds} round(s) with no reconcile ever "
                f"answered: parts {[(p.pkt_id, sum(p.acked), p.frag_total) for p in parts if not p.future.done()]}.",
                RNS.LOG_WARNING,
            )
            fail_rest(False)
            return
        strikes = self._raw_incomplete_strikes.get(peer_prefix, 0) + 1
        self._raw_incomplete_strikes[peer_prefix] = strikes
        pause_raw = 0 < self.direct_raw_incomplete_strikes <= strikes
        if pause_raw:
            self._raw_disabled_until[peer_prefix] = time.monotonic() + self.direct_raw_fallback_cooldown_s
            self._raw_incomplete_strikes.pop(peer_prefix, None)
        if not any(any(p.acked) for p in parts):
            self._raw_fallback_pending[(peer_prefix, path.hex())] = time.monotonic()
        RNS.log(
            f"{self}: RAW window to {peer_prefix!r} incomplete after {rounds} round(s) "
            f"(parts {[(p.pkt_id, sum(p.acked), p.frag_total) for p in parts if not p.future.done()]}) -- re-sending the rest as Z85 text"
            + (f". Raw paused for this peer for {self.direct_raw_fallback_cooldown_s:.0f}s "
               f"({strikes} consecutive incomplete raw send(s))." if pause_raw
               else f". Incomplete-send strike {strikes} of {self.direct_raw_incomplete_strikes} -- "
                    f"the next packet still goes raw-first."),
            RNS.LOG_WARNING,
        )
        fail_rest(None)

    async def _check_remote_completion(
        self, target: str, peer_prefix: str, pkt_id: int, frag_total: int,
        priority: int = PRIORITY_NORMAL, hop_count: Optional[int] = None,
    ) -> bool:
        """Field-data-driven fix (2026-09-16): real capture from a 5-node
        field test found a concrete case (`pkt_id=3`, router -> a client)
        where 2 of 3 fragments were logged as "never acknowledged" by the
        sender after exhausting both retry passes -- roughly 4+ minutes
        and 8 fragment-attempts total -- yet the receiver's own capture
        showed a completed reassembly of *all three* fragments about a
        second *before* the sender's own final successful ACK for the
        third fragment even landed. That's direct proof the first two
        fragments physically arrived; only their ACKs failed to make it
        back, an asymmetric/return-path loss this design previously had
        no way to distinguish from genuine non-delivery -- so it just
        kept blindly retrying data the receiver already had, burning
        airtime and `_direct_exchange_lock` time other queued sends were
        waiting on, and risking a false `direct_path_reset_threshold`
        trip (`record_direct_send_result`) over a link that was actually
        fine.

        Called from `_send_direct_fragmented` only once both retry passes
        are exhausted and fragments still appear missing -- never a
        substitute for the real firmware ACK, only a last resort before
        giving up on data that might have already arrived. Sends one
        lightweight `"Q"`-marker QUERY frame and waits up to
        `direct_completion_check_timeout_s` for a matching ANSWER,
        correlated via `_completion_query_waiters` keyed by `(peer_prefix,
        pkt_id)` (`_handle_incoming_completion_frame` resolves the future
        on receipt). Fully backward-compatible and fails safe: a peer
        that doesn't understand `"Q"` frames, or whose own answer is
        itself lost -- the same class of loss this whole mechanism exists
        to route around, just at much lower stakes for one small frame --
        simply never resolves the future, and this returns False once
        `direct_completion_check_timeout_s` elapses, falling back to
        exactly today's give-up behavior. Never raises: a local send
        failure here is treated the same as no answer, not propagated.

        Step 3 (2026-09-18): now a thin wrapper over
        `_query_remote_fragments`, which is also called *between* the
        passes as the reconcile step -- see `_send_direct_fragmented`."""
        answer = await self._query_remote_fragments(
            target, peer_prefix, pkt_id, frag_total, stage="final", priority=priority, hop_count=hop_count,
        )
        return answer is not None and answer.complete

    def _completion_unacked_grace_s(self, hop_count: Optional[int], peer_prefix: Optional[str] = None) -> float:
        """Answer wait after a QUERY whose own firmware ACK was missed
        (2026-09-20, see `direct_completion_unacked_grace_s`): the multihop
        value from 2 hops, else the base; 0 = no cap."""
        if hop_count is None:
            resolved = self._resolved_paths.get(peer_prefix) if peer_prefix else None
            hop_count = resolved.out_path_len if resolved is not None else 0
        if hop_count is not None and hop_count >= 2:
            return max(0.0, self.direct_completion_unacked_grace_multihop_s)
        return max(0.0, self.direct_completion_unacked_grace_s)

    def _completion_query_timeout_cap_s(self, hop_count: Optional[int], peer_prefix: Optional[str] = None) -> float:
        """The ceiling on a completion-ANSWER wait (field fix, 2026-09-19 --
        see `direct_completion_check_timeout_max_s` for the evidence). With
        no `hop_count`, the peer's resolved path decides (second audit: an
        explicit argument instead of a shared mutable hint attribute)."""
        if hop_count is None:
            resolved = self._resolved_paths.get(peer_prefix) if peer_prefix else None
            hop_count = resolved.out_path_len if resolved is not None else 0
        if hop_count is not None and hop_count >= 2:
            return self.direct_completion_check_timeout_max_multihop_s
        return self.direct_completion_check_timeout_max_s

    def _completion_query_timeout_s(self, peer_prefix: str, hop_count: Optional[int] = None) -> float:
        """How long to wait for a completion ANSWER: the measured
        QUERY -> ANSWER round trip for this peer when one exists, else the
        firmware's own hop-aware ACK bound doubled (a round trip is two
        exchanges), clamped into
        [`direct_completion_check_timeout_s`, `_completion_query_timeout_cap_s`].

        Field fix (2026-09-19 evening session, multi-agent capture audit).
        This replaced a stack of three escalations -- a `x (1 + hops)`
        multiplier on the floor, an RTT term of `2 x (srtt + 4*rttvar)`, and
        a per-queued-exchange contention term -- whose combined effect was
        budgets of 21-45s (median 41.4s on the timed-out checks). The session
        evidence is unambiguous that this was counter-productive: 96% of
        answers that ever arrived did so within 15s, every band beyond 20s
        produced two answers in the whole session, and the answer rate FELL
        as the budget grew (92% at 10-20s vs 34% at 40-45s). A long budget
        marks bad conditions; it does not repair them. The dominant real
        causes of an unanswered reconcile, established by pairing every query
        against the peer's own records, are plain frame loss: 48% the QUERY
        never reached the peer's application at all, 38% the peer answered
        and the answer never arrived. Neither is helped by waiting longer.
        The RTT term is kept (inside the cap) because it is genuinely
        adaptive downward on a good link; only the unbounded growth is gone.
        """
        # The FLOOR stays hop-aware, only the ceiling is new (correction made
        # while testing this change: dropping the hop term from the floor as
        # well took a first query at 1 hop from 10s to 5s, and the session's
        # own measurements say a query->answer round trip is median 3.2-5.7s
        # with a p90 of 11.7-16.1s -- so a flat 5s floor would abandon the p90
        # case before any RTT sample exists to widen it. The simulated 1-hop
        # raw scenario caught exactly that.)
        hops = max(0, hop_count or 0)
        floor_s = (
            self.direct_completion_check_timeout_s
            + self.direct_completion_check_timeout_per_hop_s * hops
        )
        cap_s = max(self.direct_completion_check_timeout_s,
                    self._completion_query_timeout_cap_s(hop_count, peer_prefix))
        floor_s = min(floor_s, cap_s)
        timeout_s = floor_s
        qs = self._query_rtt.get(peer_prefix)
        if qs is not None:
            timeout_s = 2.0 * (qs["srtt"] + 4.0 * qs["rttvar"])
        else:
            fw = self._last_firmware_ack_timeout_s.get(peer_prefix)
            if fw is not None:
                timeout_s = 2.0 * fw
        if self.rx_log_holds_enabled:
            # The peer's ANSWER pays its own pre-transmit hold before it can
            # leave, assuming it runs the same hold cap -- the best this side
            # can know. Inside the cap, not added on top of it.
            timeout_s += self.rx_log_hold_max_s
        return min(max(timeout_s, floor_s), cap_s)

    def _completion_quiet_window_s(self, hop_count: Optional[int], timeout_s: float,
                                   peer_prefix: Optional[str] = None) -> Optional[float]:
        """How long after a reconcile QUERY's firmware ACK the radio-quiet
        window lasts (field fix 2026-09-19 night; re-anchored at the ACK and
        made RTT-adaptive 2026-09-20, see `direct_completion_quiet_base_s`):
        `base + per_hop x hops` (or the measured round trip's srtt + 2 x
        rttvar when larger), never more than
        `timeout_s` (the answer budget -- the window can only move time
        that was being spent waiting anyway). None when the window is
        disabled (both keys 0), so the answer wait is fully radio-free as
        it was in commit 1919074. `_send_direct_frame_and_wait_for_ack`
        anchors it at the frame's MSG_SENT moment."""
        if self.direct_completion_quiet_base_s <= 0 and self.direct_completion_quiet_per_hop_s <= 0:
            return None
        if hop_count is None and peer_prefix is not None:
            # Same fallback as _completion_query_timeout_cap_s: the peer's
            # resolved path knows the hop count when the caller did not.
            resolved = self._resolved_paths.get(peer_prefix)
            hop_count = resolved.out_path_len if resolved is not None else 0
        hops = max(0, hop_count if hop_count is not None else 0)
        window_s = self.direct_completion_quiet_base_s + self.direct_completion_quiet_per_hop_s * hops
        # Review (2026-09-20): adaptive upward from the measured QUERY -> ANSWER
        # round trip (itself measured from the QUERY's ACK, the same anchor
        # this window uses) once three samples exist; the budget still caps it.
        qs = self._query_rtt.get(peer_prefix) if peer_prefix else None
        if qs is not None and qs.get("samples", 0) >= 3:
            window_s = max(window_s, qs["srtt"] + 2.0 * qs["rttvar"])
        return max(0.0, min(timeout_s, window_s))

    async def _query_remote_fragments(
        self, target: str, peer_prefix: str, pkt_id: int, frag_total: int, stage: str,
        priority: int = PRIORITY_NORMAL, hop_count: Optional[int] = None,
        send_info: Optional[dict] = None, entries=None,
    ) -> Optional[_CompletionFrame]:
        """Step 3 (2026-09-18, see module docstring): one `"Q"` QUERY to the
        receiver, answered with its have-bitmap (v2) or a bare complete
        flag (a v1 peer). Returns the decoded ANSWER, or None if none
        arrived (lost, or the peer predates `"Q"`/v2 frames) -- callers
        treat None as "no information", never as "nothing arrived".
        `stage` is "reconcile" (between pass 0 and pass 1) or "final"
        (after pass 1, the pre-step-3 last resort) -- capture-only.

        The QUERY is sent as one ordinary ACKed DIRECT exchange (lock held
        through its transmit and firmware ACK by `_send_direct_frame_and_
        wait_for_ack`), the lock is then kept for a short hop-scaled
        radio-quiet window (`direct_completion_quiet_base` + `..._per_hop`
        x hops after the transmit -- field fix 2026-09-19 night, the
        hidden-node collision at the repeater), and the rest of the ANSWER
        budget is awaited with the radio free -- see the comment at that
        call site for why holding the lock through the WHOLE answer wait
        was reverted. `priority` is the enclosing
        send's own tier (the reconcile stage sits inside a fragmented send
        whose receiver-side clock is already running; queueing it behind
        every ordinary send at PRIORITY_LOW defeated its purpose), and the
        QUERY is `time_critical` for the same reason.

        Audit fix (2026-09-19): an ANSWER whose `frag_total` does not match
        this query's is ignored (see `_handle_incoming_completion_frame`) --
        `_completion_query_waiters` is keyed only `(peer_prefix, pkt_id)`,
        so a late answer to a *previous* query for the same packet could
        otherwise be applied authoritatively to this one."""
        key = (peer_prefix, pkt_id)
        fut = asyncio.get_running_loop().create_future()
        # M2 (2026-09-20): `entries` = [(pkt_id, frag_total)] asks about a
        # whole window in one v4 QUERY; the one future is registered under
        # every pkt_id so a v4 ANSWER resolves it through any of them.
        query_entries = list(entries) if entries else [(pkt_id, frag_total)]
        extra_keys = [(peer_prefix, p) for p, _t in query_entries if p != pkt_id]
        # Field fix (2026-09-19): a per-query nonce so a late answer to an
        # EARLIER query for this same pkt_id cannot resolve this one (the
        # frag_total guard alone could not -- five such stale resolutions
        # happened in the evening session, one applying held=[]).
        # 2026-09-20: cycles 1..COMPLETION_QUERY_NONCE_MAX, leaving 0 and the
        # 0xF0.. range to receiver-initiated reports (see the constant).
        self._completion_query_nonce = (self._completion_query_nonce % self.COMPLETION_QUERY_NONCE_MAX) + 1
        query_nonce = self._completion_query_nonce
        self._completion_query_waiters[key] = (fut, frag_total, query_nonce)
        for (kp, p), (_p, t) in zip([(peer_prefix, p) for p, _t in query_entries], query_entries):
            self._completion_query_waiters[(kp, p)] = (fut, t, query_nonce)
        outcome = "send_failed"
        answer: Optional[_CompletionFrame] = None
        timeout_s = self._completion_query_timeout_s(peer_prefix, hop_count)
        try:
            wire_path_len, wire_rate = self._path_rate_for_wire(peer_prefix)
            frame = self._encode_completion_frame_v5(
                self.COMPLETION_TYPE_QUERY, [(p, t, False, None) for p, t in query_entries], nonce=query_nonce,
                path_len=wire_path_len, rate=wire_rate,
            )
            # First raw field test (2026-09-18 night): the QUERY is one
            # ordinary ACKed exchange -- lock held through its transmit and
            # firmware ACK -- and the ANSWER is then awaited with the radio
            # free. Holding the lock through the answer wait (the earlier
            # review's shape) blocked this node's own ANSWERs to the peer's
            # queries for up to 50s under bidirectional traffic.
            sent_at = time.monotonic()
            # Field fix (2026-09-19 night): the first seconds of that answer
            # wait are NOT radio-free any more. `_send_direct_frame_and_wait_
            # for_ack` keeps the lock past its own listen delay until this
            # query's answer future resolves or this deadline passes -- the
            # span in which the ANSWER is actually crossing the repeater
            # chain, where the querier's own next burst would collide with it
            # at the repeater (a hidden node from both ends). See
            # `direct_completion_quiet_base_s` for the measured sizing and
            # the module docstring's 2026-09-19 night entry for the answer-
            # delivery numbers that motivated it. Anchored at the QUERY's own
            # transmit (inside the ack-wait method, after any lock wait) and
            # never longer than the answer budget itself, so this can only
            # ever move time that was already being spent waiting.
            quiet_window_s = self._completion_quiet_window_s(hop_count, timeout_s, peer_prefix)
            quiet_info: dict = {}
            try:
                # 2026-09-19: the QUERY rides PRIORITY_ANSWER (unless the
                # enclosing send is a handshake, which is higher still) --
                # see that constant's comment. A stalled transfer's one
                # small question should not queue behind this node's own
                # bulk bursts to other packets.
                # Field fix (2026-09-19): `attempt` varies per try instead of
                # being hardcoded 0, so the firmware's own content-derived
                # dedup/retry differentiation is actually exercised on a
                # repeated query. The evening session found a repeated
                # (pkt_id, frag_total) query answered only 43% of the time
                # versus 90% for a first-time query, at equal link quality --
                # mechanism unexplained, but leaving the firmware's attempt
                # field pinned at 0 on every retry could only be contributing.
                # Derived from the rolling nonce, so it advances on every query
                # without threading a second counter through the callers.
                q_ok, q_waited_full = await self._send_direct_frame_and_wait_for_ack(
                    target, frame, query_nonce & 0x03, peer_prefix=peer_prefix,
                    priority=min(priority, self.PRIORITY_ANSWER),
                    time_critical=True, kind="completion_query", hop_count=hop_count,
                    quiet_wait=fut, quiet_window_s=quiet_window_s, quiet_info=quiet_info,
                )
                # Field fix (2026-09-19 morning): the QUERY's own firmware
                # ACK outcome, for the caller's stale-path evidence
                # (_record_query_path_evidence). `send_info` is the same
                # out-param shape _send_direct_payload uses.
                if send_info is not None:
                    send_info["acked"] = bool(q_ok)
                    send_info["waited_full_timeout"] = bool(q_waited_full)
            except Exception as exc:
                if send_info is not None:
                    send_info["acked"] = False
                    send_info["waited_full_timeout"] = False
                self._debug(
                    f"completion QUERY ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}): "
                    f"send failed locally: {exc} -- treating as no answer."
                )
                return None
            # Audit fix (2026-09-19): the ANSWER budget starts when the QUERY
            # is actually out, not when this coroutine began. `_completion_
            # query_timeout_s` documents itself as "how long to wait for a
            # completion ANSWER", but the send call above also covers
            # `_direct_exchange_lock` queueing (49s observed in the
            # 2026-09-18 captures), the pre-transmit gate and the QUERY's own
            # firmware ACK wait. Charging all of that against the peer's
            # reply left `max(0.5, ...)` -- i.e. 0.5s -- for an ANSWER that
            # really needed seconds, and 36 of 106 archived completion checks
            # timed out. A timeout here means "no information", so every one
            # of those cost a full re-drive of fragments the receiver already
            # held (or, on the raw path, a false fallback strike).
            answer_wait_start = time.monotonic()
            # Field fix (2026-09-19 night; review fix 2026-09-20): the quiet
            # hold already spent part of this budget with the radio held --
            # it is charged here, so the window moves waiting time rather
            # than adding to it (the evening session's evidence is that a
            # longer budget marks bad conditions, it does not repair them).
            # The round trip is measured from the QUERY's ACK either way: for
            # an answer that arrived inside the hold, from the timestamps
            # the ack-wait method handed back; otherwise from here (which
            # is the same point, since the hold ended before this line).
            quiet_hold_s = float(quiet_info.get("hold_s", 0.0) or 0.0)
            remaining = max(0.0, timeout_s - quiet_hold_s)
            if not q_ok:
                # Dead-wait trims (2026-09-20): no firmware ACK for the QUERY
                # -> it most likely never reached the peer; a short grace
                # covers the answers that do arrive (see the config comment).
                grace_s = self._completion_unacked_grace_s(hop_count, peer_prefix)
                if grace_s > 0:
                    remaining = min(remaining, grace_s)
            rtt_origin = quiet_info.get("ack_done_at", answer_wait_start)
            try:
                got: _CompletionFrame = await asyncio.wait_for(fut, timeout=remaining)
                answer = got
                outcome = "answered"
                # Measured from the QUERY's ACK, so the estimator models the
                # peer's reply latency rather than this node's own queueing
                # (which would inflate every later timeout and hold the
                # radio longer on failures).
                answered_at = quiet_info.get("answered_at") or time.monotonic()
                if not quiet_info.get("not_sent") and (
                        got.nonce is None or (got.nonce & 0xF0) != self.COMPLETION_REPORT_NONCE_BASE):
                    # A late REPORT that resolved this QUERY's future (the
                    # monotone rule) is the report estimator's sample, not
                    # a QUERY round trip (phase 1, 2026-09-20: 24 of 29
                    # hop-0 "answered" outcomes were this, and they were
                    # shrinking _query_rtt with ~0 s samples).
                    self._record_query_rtt(peer_prefix, answered_at - rtt_origin)
                self._debug(
                    f"completion ANSWER ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}): v{got.version} "
                    f"complete={got.complete} held={sorted(got.held) if got.held is not None else None} "
                    f"after {answered_at - rtt_origin:.1f}s from its ACK "
                    f"({quiet_hold_s:.1f}s of it inside the quiet hold; "
                    f"{time.monotonic() - sent_at:.1f}s including the QUERY's own send)."
                )
                return got
            except asyncio.TimeoutError:
                outcome = "timeout"
                self._debug(
                    f"completion QUERY ({stage}, pkt_id={pkt_id}, peer={peer_prefix!r}): "
                    f"no answer within {timeout_s:.1f}s ({quiet_hold_s:.1f}s of it as the quiet hold) -- "
                    f"no information, proceeding as if unanswered."
                )
                return None
        finally:
            self._completion_query_waiters.pop(key, None)
            for k in extra_keys:
                self._completion_query_waiters.pop(k, None)
            self._capture_completion_check_result(
                peer_prefix, pkt_id, frag_total, outcome,
                answer.complete if answer is not None else False,
                stage=stage, timeout_s=timeout_s,
                answer_version=answer.version if answer is not None else None,
                held=sorted(answer.held) if answer is not None and answer.held is not None else None,
            )

    def _handle_incoming_completion_frame(self, marker_and_body: str, sender_token: str) -> None:
        """Receive side of the `"Q"`-marker completion check (see
        `_check_remote_completion`'s own docstring for the full
        mechanism/motivation). A QUERY is answered directly from the
        existing whole-packet dedup cache -- `_add_channel_fragment`
        already records a completed DIRECT-fragmented reassembly there
        under exactly the key `(mode, sender_token, pkt_id, frag_total)`
        this method rebuilds, so answering "do you have pkt_id X
        complete" needs no new state of its own, just a lookup into state
        that already exists for an unrelated reason (§7's dedup). Uses
        `sender_token` as received here, uncanonicalized -- matching
        `_reassembly_key`'s own convention of keying on the raw
        MeshCore-native token, never this interface's canonical 6-byte
        peer prefix, so this lookup can never silently miss due to a
        canonicalization mismatch against how the entry was actually
        stored."""
        try:
            frame = self._decode_completion_frame(marker_and_body)
        except ValueError as exc:
            self._debug(f"discarding malformed completion-check frame from {sender_token!r}: {exc}")
            return
        if frame.peer_path_len is not None:
            # v5 (alpha 0.1.6, item 1): the sender's path view feeds this
            # node's scoreboard for the symmetric path.
            _peer = self._canonical_peer_prefix(sender_token)
            if _peer is not None:
                self._note_peer_reported_path(_peer, frame.peer_path_len, frame.peer_rate)

        if frame.type == self.COMPLETION_TYPE_QUERY:
            # Step 3 (2026-09-18): answer with what we actually hold, not
            # just complete/not. The key is exactly _reassembly_key's for
            # a non-coop DIRECT frame, so both the dedup cache (a finished
            # reassembly) and a still-open bucket are consulted with the
            # same tuple. A bucket that was evicted or idle-expired reads
            # as "holds nothing" -- correct: the sender must re-drive it
            # all, and would have had to anyway.
            if frame.version >= 4:
                # M2 (2026-09-20): one v4 QUERY asks about a whole window;
                # one v4 ANSWER lists every part's state.
                entries = [(p, t) + self._bucket_state(sender_token, p, t) for p, t, _c, _h in frame.entries]
                self._debug(
                    f"completion QUERY (v{frame.version}) from {sender_token!r} for {[(p, t) for p, t, _c, _h in frame.entries]}: "
                    f"answering {[(p, c, sorted(h)) for p, _t, c, h in entries]}."
                )
                if self._packet_capture_file is not None:
                    self._capture_event("in", {
                        "event": "completion_query_received", "sender_token": sender_token,
                        "pkt_id": frame.pkt_id, "frag_total": frame.frag_total, "query_version": frame.version,
                        "answering_complete": entries[0][2], "answering_held": sorted(entries[0][3]),
                        "entries": [(p, t, c, sorted(h)) for p, t, c, h in entries],
                        # alpha 0.1.7 (item 4): the v5 header as received
                        "peer_path_len": frame.peer_path_len,
                        "peer_rate": round(frame.peer_rate, 3) if frame.peer_rate is not None else None,
                        "hop_count": self._peer_view_fields(sender_token)["hop_count"],
                    })
                self._spawn_background_task(
                    self._send_completion_answer(
                        sender_token, frame.pkt_id, frame.frag_total, entries[0][2], held=entries[0][3],
                        version=frame.version, nonce=frame.nonce, entries=entries,
                    )
                )
                return
            complete, held = self._bucket_state(sender_token, frame.pkt_id, frame.frag_total)
            self._debug(
                f"completion QUERY (v{frame.version}) from {sender_token!r} for pkt_id={frame.pkt_id} "
                f"frag_total={frame.frag_total}: answering complete={complete} held={sorted(held)}."
            )
            if self._packet_capture_file is not None:
                self._capture_event("in", {
                    "event": "completion_query_received",
                    "sender_token": sender_token,
                    "pkt_id": frame.pkt_id,
                    "frag_total": frame.frag_total,
                    "query_version": frame.version,
                    "answering_complete": complete,
                    "answering_held": sorted(held),
                    "peer_path_len": frame.peer_path_len,
                    "peer_rate": round(frame.peer_rate, 3) if frame.peer_rate is not None else None,
                    "hop_count": self._peer_view_fields(sender_token)["hop_count"],
                })
            self._spawn_background_task(
                self._send_completion_answer(
                    sender_token, frame.pkt_id, frame.frag_total, complete,
                    held=held, version=frame.version, nonce=frame.nonce,
                )
            )
            return

        # ANSWER: correlate against our own canonical peer prefix, since
        # that's the key _query_remote_fragments registered the waiter
        # future under. The whole decoded frame is handed over -- the
        # querying side decides what `complete`/`held` mean for its stage.
        peer_prefix = self._canonical_peer_prefix(sender_token)
        if peer_prefix is None:
            return
        is_report = frame.nonce is not None and (frame.nonce & 0xF0) == self.COMPLETION_REPORT_NONCE_BASE
        if is_report and frame.entries:
            for p, _t, c, _h in frame.entries[1:]:
                if c:
                    self._record_report_latency(peer_prefix, p)
        if is_report and frame.complete:
            # A receiver-initiated REPORT: sample its latency whether or
            # not a waiter still exists (a late report is the case the
            # estimator most needs to see) -- phase 1, 2026-09-20. Complete
            # reports only: the second-last fragment's incomplete report
            # arrives ~1.5-2 s before the complete one and would train the
            # window short.
            latency_s = self._record_report_latency(peer_prefix, frame.pkt_id)
            if latency_s is not None:
                self._debug(
                    f"completion REPORT from {sender_token!r} for pkt_id={frame.pkt_id} arrived "
                    f"{latency_s:.2f}s after the burst ended (window estimator "
                    f"srtt={self._report_rtt[peer_prefix]['srtt']:.2f}s)."
                )
        if frame.entries and len(frame.entries) > 1:
            # M2 (2026-09-20): a v4 multi-part ANSWER / REPORT. The window
            # machine registered ONE future under every pkt_id it sent; the
            # first entry that passes the per-part rules (nonce, monotone
            # completion, frag_total) resolves it with the whole frame.
            for pkt_id, frag_total, complete, _held in frame.entries:
                waiter = self._completion_query_waiters.get((peer_prefix, pkt_id))
                if waiter is None:
                    continue
                fut, expected_frag_total, expected_nonce = waiter
                if frag_total != expected_frag_total:
                    continue
                stale = frame.nonce is not None and expected_nonce is not None and frame.nonce != expected_nonce
                if stale and not complete:
                    continue
                if not fut.done():
                    fut.set_result(frame)
                return
            return
        waiter = self._completion_query_waiters.get((peer_prefix, frame.pkt_id))
        if waiter is None:
            return
        fut, expected_frag_total, expected_nonce = waiter
        stale_nonce = (
            frame.nonce is not None and expected_nonce is not None and frame.nonce != expected_nonce
        )
        if stale_nonce and frame.complete and frame.frag_total == expected_frag_total:
            # A late answer may only ever tell us the receiver has MORE, never
            # less (field fix, 2026-09-19). An answer whose nonce belongs to an
            # earlier, already-timed-out query for this same packet describes
            # the peer's state as of that query -- so its `held` set is not
            # trustworthy as a replacement for our own (that is exactly the
            # `held=[]` case that discarded real fragments). But
            # `complete=True` is monotone: a receiver that had the whole
            # packet then cannot have less of it now, short of a bucket
            # eviction which only makes a re-send necessary anyway. Accepting
            # it finishes a transfer that is genuinely done instead of waiting
            # for an answer whose round trip exceeds the budget cap -- the
            # case where the peer's answers are consistently slower than
            # `direct_completion_check_timeout_max_s`.
            self._debug(
                f"accepting a LATE completion ANSWER from {sender_token!r} for pkt_id={frame.pkt_id}: "
                f"nonce {frame.nonce} is from an earlier query (outstanding is {expected_nonce}), but it "
                f"reports the packet complete, which cannot become untrue."
            )
            if not fut.done():
                fut.set_result(frame)
            return
        if stale_nonce:
            # Field fix (2026-09-19): a v3 answer whose echoed nonce does not
            # match the outstanding query is a reply to an earlier, already
            # timed-out query for the same packet. Applying it would overwrite
            # this send's fragment state with stale information.
            self._debug(
                f"discarding completion ANSWER from {sender_token!r} for pkt_id={frame.pkt_id}: "
                f"nonce {frame.nonce} does not match the outstanding query's {expected_nonce} "
                f"-- stale answer to an earlier query."
            )
            return
        if frame.frag_total != expected_frag_total:
            # Audit fix (2026-09-19): the waiter is keyed only on
            # (peer_prefix, pkt_id), so a late ANSWER to a *previous* query
            # for this packet -- a timed-out reconcile whose reply arrived
            # after the next query went out -- would otherwise resolve this
            # query's future and be applied authoritatively (the caller
            # overwrites `acked` from it by design). A mismatched frag_total
            # is the one stale case this side can detect for certain: raw
            # and text fragment the same payload into different counts, and
            # a resumed send re-fragments too. The same-frag_total stale
            # answer this comment used to call out as unaddressed is now
            # caught by the v3 query nonce checked above (2026-09-19
            # evening); this frag_total check remains as the v2-peer
            # fallback, since a v2 answer carries no nonce to check.
            self._debug(
                f"discarding completion ANSWER from {sender_token!r} for pkt_id={frame.pkt_id}: "
                f"frag_total={frame.frag_total} does not match the outstanding query's "
                f"{expected_frag_total} -- stale answer to an earlier query."
            )
            return
        if not fut.done():
            fut.set_result(frame)

    def _bucket_state(self, sender_token: str, pkt_id: int, frag_total: int) -> "tuple[bool, set]":
        """(complete, held) for one of this sender's packets: the dedup
        cache (a finished reassembly) or the open bucket under exactly
        `_reassembly_key`'s tuple; an evicted or expired bucket reads as
        "holds nothing"."""
        key = ("direct", sender_token or "~anon", pkt_id, frag_total)
        if self._dedup_contains(key):
            return True, set(range(frag_total))
        bucket = self._reassembly.get(key)
        return False, (set(bucket.fragments.keys()) if bucket is not None else set())

    def _note_recent_raw_pkt(self, sender_token: str, pkt_id: int, frag_total: int) -> None:
        """Remember this sender's raw packets (M2): a window report lists
        every one touched within `RECENT_RAW_PKT_SPAN_S`, newest first, at
        most COMPLETION_V4_MAX_ENTRIES."""
        table = self._recent_raw_pkts.setdefault(sender_token, {})
        table[(pkt_id, frag_total)] = time.monotonic()
        if len(table) > 4 * self.COMPLETION_V4_MAX_ENTRIES:
            for k in sorted(table, key=table.get)[: len(table) - 2 * self.COMPLETION_V4_MAX_ENTRIES]:
                table.pop(k, None)

    # -- Item 1 (alpha 0.1.8): the proof is the completion ---------------

    def _proof_expected_key(self, data: bytes, header) -> Optional[bytes]:
        """The value the PROOF for this just-delivered packet will carry in
        its destination field, when RNS proves it per packet at all: a
        plain DATA to a SINGLE destination, which is what `_answered_send_
        key` keys the sender's side on. None for everything RNS does not
        prove one packet at a time -- a Resource part (context RESOURCE;
        a Resource is proved once, whole, as RESOURCE_PRF), anything else
        carried inside a Link, and every announce."""
        if header is None or not data:
            return None
        if (header.packet_type == RNS.Packet.DATA and header.context == RNS.Packet.NONE
                and header.destination_type == RNS.Destination.SINGLE):
            return self._compute_truncated_hash(data, header.header_type)
        return None

    def _proof_may_replace_report(self, complete_data: bytes, header, sender_token: str,
                                  peer_prefix: Optional[str]) -> Optional[bytes]:
        """The proof key to wait for before sending this window's complete
        report, or None to report exactly as before (alpha 0.1.8, item 1).

        Four gates, each of which would otherwise cost the sender its only
        signal:

        1. RNS proves this packet per packet (`_proof_expected_key`).
        2. Its destination is served by this node's own RNS
           (`_is_local_destination`). On a transport node relaying the
           packet onward nothing proves it, so no proof is ever coming.
        3. Nothing else of this sender's recent raw packets is incomplete.
           The complete report is NOT about one packet: it carries a
           per-fragment bitmap for every recent pkt_id of this sender
           (`_recent_raw_entries`), and those bitmaps are how the sender
           re-drives exactly the missing fragments of its OTHER parts
           without a QUERY first. A proof says only "this one packet
           arrived", so while anything else is incomplete the report is
           worth far more than the proof's latency.
        4. The sender is a bound peer with a resolved path, so the proof
           routes back to it DIRECT (`_proof_correlation`'s own gate in
           `_observe_raw_received_packet`). Without that the proof may go
           out over CHANNEL, to every peer, or not at all."""
        if self.proof_report_grace_s <= 0:
            return None
        key = self._proof_expected_key(complete_data, header)
        if key is None:
            return None
        if header.destination_hash is None or not self._is_local_destination(header.destination_hash):
            return None
        if any(not complete for _pkt, _total, complete, _held in self._recent_raw_entries(sender_token)):
            return None
        if peer_prefix is None or peer_prefix not in self._peers or peer_prefix not in self._resolved_paths:
            return None
        return key

    def _cancel_proof_grace(self, sender_token: str) -> None:
        task = self._pending_proof_graces.pop(sender_token, None)
        if task is not None and not task.done():
            task.cancel()

    def _hold_report_for_proof(self, sender_token: str, header: "_FrameHeader", proof_key: bytes) -> None:
        """Hold this window's complete report for `proof_report_grace_s`.
        If RNS hands the proof over inside it, the proof IS the completion
        and the report is dropped -- one frame instead of two, and the
        proof no longer queues behind the report's own relay hold, which at
        two hops in the 2026-09-22 field session put it on the air about
        8 s after the packet landed (proof turnaround 17.9 s median, 30.6 s
        p90). If the grace expires, the report goes exactly as before.

        Nothing here bypasses a hold or shortens one. The 0.1.7 second cut
        stands untouched: this works by not sending a frame, never by
        cutting the relay window of one already sent."""
        self._cancel_proof_grace(sender_token)

        async def grace():
            try:
                deadline = time.monotonic() + self.proof_report_grace_s
                while True:
                    if self._proof_enqueued_at_for_key(proof_key) is not None:
                        self._capture_report_skipped_for_proof(sender_token, header, proof_key)
                        self._debug(
                            f"completion REPORT to {sender_token!r} (pkt_id={header.pkt_id}) skipped: RNS proved the "
                            f"packet inside the {self.proof_report_grace_s:.2f}s grace, and the proof says the same "
                            f"thing with one frame instead of two."
                        )
                        return
                    if time.monotonic() >= deadline or self.detached or not self.online:
                        break
                    await asyncio.sleep(min(self.PROOF_GRACE_POLL_S, max(0.0, deadline - time.monotonic())))
                self._send_completion_report(
                    sender_token, header, complete=True, held=set(range(header.frag_total)),
                    held_s=round(self.proof_report_grace_s, 3),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                RNS.log(f"{self}: proof grace for {sender_token!r} failed: {exc}", RNS.LOG_WARNING)
            finally:
                if self._pending_proof_graces.get(sender_token) is task_holder.get("task"):
                    self._pending_proof_graces.pop(sender_token, None)

        task_holder = {}
        task_holder["task"] = asyncio.get_running_loop().create_task(grace())
        self._pending_proof_graces[sender_token] = task_holder["task"]

    def _capture_report_skipped_for_proof(self, sender_token: str, header: "_FrameHeader", proof_key: bytes) -> None:
        if self._packet_capture_file is None:
            return
        self._capture_event("out", dict({
            "event": "completion_report_skipped", "peer_prefix": self._canonical_peer_prefix(sender_token),
            "pkt_id": header.pkt_id, "frag_total": header.frag_total,
            "report_skipped_for_proof": True, "proof_key": proof_key.hex(),
            "grace_s": round(self.proof_report_grace_s, 3),
        }, **self._peer_view_fields(self._canonical_peer_prefix(sender_token))))

    def _recent_raw_entries(self, sender_token: str) -> list:
        """The v4 report entries for this sender: (pkt_id, frag_total,
        complete, held) for its recent raw packets, newest first."""
        now = time.monotonic()
        table = self._recent_raw_pkts.get(sender_token, {})
        keys = sorted((k for k, t in table.items() if now - t <= self.RECENT_RAW_PKT_SPAN_S), key=lambda k: -table[k])
        entries = []
        for pkt_id, frag_total in keys[: self.COMPLETION_V4_MAX_ENTRIES]:
            complete, held = self._bucket_state(sender_token, pkt_id, frag_total)
            entries.append((pkt_id, frag_total, complete, held))
        return entries

    # Alpha 0.1.5 (2b): the margin, in fragment airtimes, a "still arriving"
    # hold adds to the sender's spacing for relay and host jitter before the
    # receiver concludes the burst has ended.
    RAW_ARRIVING_HOLD_MARGIN_AIRTIMES = 0.5
    # How many of the sender's start-to-start spacings the hold spans: TWO,
    # so one lost fragment does not end the silence. MeshBench
    # `page_transfer` on the first cut (one spacing + the margin, 3.19 s at
    # one hop against a 2.74 s spacing): the hold fired 8 times, 7 of them
    # while the sender still had 1-7 frames of its re-drive to send -- the
    # fragment after the completing one had been lost at the repeater, so
    # the receiver's silence ran past one spacing mid-burst -- and 0 of the
    # 8 reports reached the sender (MeshBench: half-duplex / collision at
    # the repeater). Exactly the collision 2b exists to remove.
    RAW_ARRIVING_HOLD_SPACINGS = 2.0
    # Alpha 0.1.6 (item 3): the gaps hold spans ONE sender spacing plus the
    # margin (it was one airtime at zero hop, the relay gap through
    # repeaters, with no margin). The 2026-09-21 session: the completing
    # fragment landed 0.01 s after the 0.91 s hold at zero hop and 0.1-1.55 s
    # after the 1.93 s hold at two hops, so 6 of 15 receiver reports were a
    # gaps report and a complete report back to back.
    RAW_GAPS_HOLD_SPACINGS = 1.0

    def _report_hold_s(self, fragment_on_air_bytes: int, hops: int, arriving: bool = False) -> float:
        """How long a receiver holds a report (pure function, phase 3 M1;
        generalised in alpha 0.1.5 2b; the gaps hold widened in alpha 0.1.6
        item 3).

        Gaps case (`arriving=False`): after a flagged fragment that left
        gaps, the time the burst's LAST fragment needs to arrive -- one of
        the sender's start-to-start spacings at this hop count (a fragment's
        airtime plus `direct_raw_zero_hop_gap` at zero hop; the hop-scaled
        relay gap, which contains the airtime, through repeaters) plus half
        an airtime of margin. Field: the complete report followed the gaps
        report by 0.22-0.43 s at zero hop in the 2026-09-20 session, but in
        the 2026-09-21 session the completing fragment landed just outside
        the one-airtime hold (0.01 s at zero hop, up to 1.55 s past the
        relay-gap hold at two hops).

        Still-arriving case (`arriving=True`): after an UNFLAGGED fragment
        completed a part, the silence that says the sender's window burst
        is over -- RAW_ARRIVING_HOLD_SPACINGS (two) of the sender's start-
        to-start spacings at this hop count (airtime + `direct_raw_zero_
        hop_gap` at zero hop; the hop-scaled gap, which contains the
        airtime, through repeaters), so one lost fragment does not end the
        silence, plus half an airtime for relay and host jitter. Re-armed
        by every fragment."""
        airtime = self._estimate_tx_airtime_s("", on_air_bytes=fragment_on_air_bytes)
        if hops > 0:
            hold = self._raw_fragment_gap_s(hops, fragment_on_air_bytes)
        else:
            hold = airtime
        spacing = hold + (max(0.0, self.direct_raw_zero_hop_gap_s) if hops <= 0 else 0.0)
        spacings = self.RAW_ARRIVING_HOLD_SPACINGS if arriving else self.RAW_GAPS_HOLD_SPACINGS
        return spacings * spacing + self.RAW_ARRIVING_HOLD_MARGIN_AIRTIMES * airtime

    def _report_recently_sent(self, sender_token: str, pkt_id: Optional[int], fragment_on_air_bytes: int) -> bool:
        """Alpha 0.1.6 (item 3): whether a complete report for this packet
        went out within the sender's burst tail -- a flagged frame arriving
        inside that window (the parity fragment behind the completing data
        fragment, a duplicate relayed twice) is the same burst, not a
        re-drive, and reporting again is the second report per window the
        field counted (six of fifteen at zero hop; pairs 0.85-5.4 s apart
        at two hops). A re-drive comes after the sender's report wait, well
        past the window, and is reported as before."""
        if pkt_id is None:
            return False
        sent_at = self._last_complete_report_at.get((sender_token, pkt_id))
        if sent_at is None:
            return False
        window = self._report_hold_s(fragment_on_air_bytes, self._receiver_hops_to(sender_token), arriving=True)
        return time.monotonic() - sent_at < window

    def _receiver_hops_to(self, sender_token: str) -> int:
        """The hop count a receiver scales its report holds by: the larger
        of its own resolved hop count to the sender and the path length the
        sender reported in its last "Q" v5 frame (alpha 0.1.6, item 3). The
        sender's fragment spacing follows the SENDER's path, and the field's
        two-hop doubles came from a laptop holding at its own one-hop count
        (1.93 s) while the desktop spaced its fragments for two hops
        (4.6 s), so the completing fragment landed up to 1.55 s after the
        hold. 0 when neither is known."""
        peer_prefix = self._canonical_peer_prefix(sender_token)
        resolved = self._resolved_paths.get(peer_prefix) if peer_prefix else None
        own = max(0, resolved.out_path_len) if resolved is not None else 0
        board = self._path_boards.get(peer_prefix) if peer_prefix else None
        reported = board.peer_path_len if board is not None and board.peer_path_len is not None else 0
        return max(own, reported)

    def _peer_view_fields(self, sender_token: str) -> dict:
        """Capture fields for the peer's reported view of the path between
        us (alpha 0.1.7, item 4): this node's own resolved hop count to the
        sender, and the path length and delivery rate the peer last put in
        a "Q" v5 header (`_note_peer_reported_path`) -- the numbers the
        receiver's holds scale by (`_receiver_hops_to`), which the field
        could not read from the 0.1.6 captures."""
        peer_prefix = self._canonical_peer_prefix(sender_token)
        resolved = self._resolved_paths.get(peer_prefix) if peer_prefix else None
        board = self._path_boards.get(peer_prefix) if peer_prefix else None
        return {
            "hop_count": resolved.out_path_len if resolved is not None else None,
            "peer_path_len": board.peer_path_len if board is not None else None,
            "peer_rate": round(board.peer_rate, 3) if board is not None and board.peer_rate is not None else None,
        }

    def _schedule_sender_report(self, sender_token: str, header: _FrameHeader, fragment_on_air_bytes: int) -> None:
        """Alpha 0.1.5 (2b): a part completed on an unflagged fragment --
        the sender's window is still in the air. Hold ONE complete report
        for this sender until its fragments stop arriving for
        `_report_hold_s(..., arriving=True)`; `_rearm_sender_report` pushes
        the deadline on every further fragment, and any report that goes
        out meanwhile (the flagged last fragment's, a gaps report, a
        duplicate's) supersedes it, since every report lists the sender's
        recent packets. `header` names the part whose completion is the
        report's trigger."""
        if not self.direct_raw_report_enabled:
            return
        hold_s = self._report_hold_s(fragment_on_air_bytes, self._receiver_hops_to(sender_token), arriving=True)
        self._cancel_sender_report(sender_token)
        entry = {"header": header, "frag_bytes": fragment_on_air_bytes, "hold_s": hold_s, "task": None}

        async def fire():
            try:
                await asyncio.sleep(hold_s)
                if self._pending_sender_reports.get(sender_token) is not entry:
                    return
                self._pending_sender_reports.pop(sender_token, None)
                self._send_completion_report(
                    sender_token, header, complete=True, held=set(range(header.frag_total)), held_s=hold_s,
                )
            except asyncio.CancelledError:
                pass

        entry["task"] = self._spawn_background_task(fire())
        self._pending_sender_reports[sender_token] = entry

    def _rearm_sender_report(self, sender_token: str) -> None:
        """Another fragment from this sender: the held report waits again."""
        entry = self._pending_sender_reports.get(sender_token)
        if entry is None:
            return
        self._schedule_sender_report(sender_token, entry["header"], entry["frag_bytes"])

    def _cancel_sender_report(self, sender_token: str) -> None:
        entry = self._pending_sender_reports.pop(sender_token, None)
        if entry is not None and entry.get("task") is not None and not entry["task"].done():
            entry["task"].cancel()

    def _schedule_gaps_report(self, key, sender_token: str, header: _FrameHeader, fragment_on_air_bytes: int) -> None:
        """Hold the gaps report for `_report_hold_s` (M1 debounce); if the
        bucket completes first the complete report supersedes it
        (`_cancel_gaps_report`). A second flagged fragment while one is
        held re-arms the hold (the bitmap is read when it fires). With
        `direct_report_debounce = no` the report goes at once."""
        def held_now() -> set:
            bucket = self._reassembly.get(key)
            return set(bucket.fragments.keys()) if bucket is not None else set()

        if not self.direct_report_debounce or not self.direct_raw_report_enabled:
            self._send_completion_report(sender_token, header, complete=False, held=held_now())
            return
        hold_s = self._report_hold_s(fragment_on_air_bytes, self._receiver_hops_to(sender_token))
        self._cancel_gaps_report(key)

        async def fire():
            try:
                await asyncio.sleep(hold_s)
                if self._pending_gap_reports.get(key) is not asyncio.current_task():
                    return
                self._pending_gap_reports.pop(key, None)
                if key not in self._reassembly or self._dedup_contains(key):
                    return   # completed (its report went) or gone meanwhile
                self._send_completion_report(sender_token, header, complete=False, held=held_now(), held_s=hold_s)
            except asyncio.CancelledError:
                pass

        self._pending_gap_reports[key] = self._spawn_background_task(fire())

    def _cancel_gaps_report(self, key) -> None:
        task = self._pending_gap_reports.pop(key, None)
        if task is not None and not task.done():
            task.cancel()

    def _send_completion_report(self, sender_token: str, header: _FrameHeader, complete: bool, held: set,
                                held_s: Optional[float] = None) -> None:
        """Receiver-initiated completion report (2026-09-20): one unsolicited
        v3 ANSWER for a raw burst, nonce `COMPLETION_REPORT_NONCE_BASE |
        round` (the raw header's attempt bits), spawned from the raw receive
        path -- on completion, on a flagged last fragment that left gaps, and
        on a flagged duplicate of a packet already delivered. Best effort
        like the QUERY's answer; the sender's QUERY fallback is the recovery
        path if it is lost. Off when `direct_raw_report_enabled` is no."""
        if not self.direct_raw_report_enabled or header.pkt_id is None:
            return
        # 2b: every report lists the sender's recent packets, so a report
        # held for "still arriving" is covered by whichever goes out now.
        self._cancel_sender_report(sender_token)
        # Alpha 0.1.8 (item 1): and so is a report held for a proof -- one
        # report per window, whichever trigger wins (the 0.1.6 item 3 rule).
        self._cancel_proof_grace(sender_token)
        if complete:
            # Item 3 (alpha 0.1.6): a flagged frame of this packet within the
            # burst tail of this report is not reported again.
            self._last_complete_report_at[(sender_token, header.pkt_id)] = time.monotonic()
            if len(self._last_complete_report_at) > 256:
                for k in list(self._last_complete_report_at)[:64]:
                    self._last_complete_report_at.pop(k, None)
        nonce = self.COMPLETION_REPORT_NONCE_BASE | ((header.attempt or 0) & 0x03)
        self._debug(
            f"completion REPORT to {sender_token!r} for pkt_id={header.pkt_id} frag_total={header.frag_total}: "
            f"complete={complete} held={sorted(held)} (round {(header.attempt or 0) & 0x03})."
        )
        if self._packet_capture_file is not None:
            self._capture_event("out", {
                "event": "completion_report_sent", "sender_token": sender_token, "pkt_id": header.pkt_id,
                "frag_total": header.frag_total, "complete": complete, "held": sorted(held),
                "round": (header.attempt or 0) & 0x03,
                "held_s": round(held_s, 3) if held_s is not None else None,   # the hold this report waited (0.0: at once; item 3)
                "noack": self.direct_report_noack,
                **self._peer_view_fields(sender_token),   # alpha 0.1.7 (item 4): what the hold scaled by
            })
        # M2 (2026-09-20): the report lists this sender's recent raw packets
        # (newest first, the triggering one guaranteed), so one report
        # answers for a whole window burst.
        entries = [(p, t, c, h) for p, t, c, h in self._recent_raw_entries(sender_token)
                   if not (p == header.pkt_id and t == header.frag_total)]
        entries = [(header.pkt_id, header.frag_total, complete, set(held))] + entries
        self._spawn_background_task(
            self._send_completion_answer(
                sender_token, header.pkt_id, header.frag_total, complete,
                held=held, version=self.COMPLETION_PROTOCOL_VERSION, nonce=nonce, report=True,
                entries=entries[: self.COMPLETION_V4_MAX_ENTRIES],
            )
        )

    async def _send_completion_answer(
        self, sender_token: str, pkt_id: int, frag_total: int, complete: bool,
        held: "Optional[set]" = None, version: Optional[int] = None,
        nonce: Optional[int] = None, report: bool = False, entries=None,
    ) -> None:
        """Best-effort ANSWER send for `_handle_incoming_completion_frame`'s
        QUERY branch. Deliberately no retry loop: this is already the
        second half of a mechanism built to route around lost ACKs, so
        piling a multi-attempt cycle on top of the answer itself would
        just relocate the same risk rather than reduce it. If this answer
        is lost, the querying side's own `direct_completion_check_timeout_
        s` simply elapses and it falls back to today's give-up behavior --
        no worse than before this feature existed.

        Code review (2026-09-18): one attempt through `_send_direct_frame_
        and_wait_for_ack`, so -- like every other DIRECT exchange -- the
        frame's own firmware ACK is waited out while `_direct_exchange_
        lock` is held, rather than the lock being released with that ACK
        still in flight for the next send to collide with (the previous
        shape). `PRIORITY_NORMAL`, not `PRIORITY_LOW`: the querier is a
        stalled fragmented send waiting ~5s for this, and a LOW answer
        behind a single missed-ACK timeout on this node (5-45s) can never
        make that deadline, turning the peer's QUERY into pure wasted
        airtime. `time_critical` for the same reason. The ACK outcome is
        recorded (`kind="completion_answer"` in the capture) but not
        retried."""
        contact = self._resolve_contact(sender_token)
        target = contact.get("public_key") if contact is not None else None
        if not target:
            self._debug(
                f"completion ANSWER to {sender_token!r} (pkt_id={pkt_id}) not sent -- "
                f"no resolvable contact/public_key."
            )
            return
        peer_prefix = self._canonical_peer_prefix(sender_token)
        # v5 (alpha 0.1.6, item 1): every ANSWER and REPORT carries this
        # node's path length to the peer and its delivery rate on it.
        wire_path_len, wire_rate = self._path_rate_for_wire(peer_prefix)
        if entries and (version is None or version >= 5):
            frame = self._encode_completion_frame_v5(self.COMPLETION_TYPE_ANSWER, entries, nonce=nonce,
                                                     path_len=wire_path_len, rate=wire_rate)
        elif entries and version >= 4:
            frame = self._encode_completion_frame_v4(self.COMPLETION_TYPE_ANSWER, entries, nonce=nonce)
        else:
            frame = self._encode_completion_frame(
                self.COMPLETION_TYPE_ANSWER, pkt_id, frag_total, complete=complete, nonce=nonce,
                held=held, version=version, path_len=wire_path_len, rate=wire_rate,
            )
        # Simulation finding (2026-09-19, one-hop raw scenario): this
        # ANSWER used to leave the radio right behind the firmware's own
        # ACK for the QUERY, and through a repeater it reached the chain
        # while the repeater was still forwarding that ACK -- half-duplex,
        # so six of six answers were lost with no ACK. Exactly the
        # fragment-chasing-fragment collision `_raw_fragment_gap_s` paces
        # raw bursts for; the same hop-scaled gap here, sized for the ACK
        # frame the repeater is busy with. Zero hop: no gap.
        resolved = self._resolved_paths.get(peer_prefix) if peer_prefix is not None else None
        if resolved is not None:
            hops = max(0, resolved.out_path_len)
        else:
            out_path_len = contact.get("out_path_len", 0) if isinstance(contact, dict) else 0
            hops = 1 if out_path_len is None or out_path_len < 0 else int(out_path_len)
        # A report (2026-09-20) follows a raw fragment, which the firmware
        # does not ACK, so there is no ACK relay to wait out: no hold.
        gap_s = 0.0 if report else self._completion_answer_hold_s(hops)
        if gap_s > 0:
            await asyncio.sleep(gap_s)
        if self.direct_report_noack:
            # Phase 3 M1 (2026-09-20): TXT_TYPE_CLI_DATA -- delivered, never
            # ACKed; the querier's / sender's next action confirms it.
            try:
                await self._send_direct_noack_frame(
                    target, frame, (nonce or 0) & 0x03, peer_prefix, hops,
                    kind="completion_report" if report else "completion_answer",
                    priority=self.PRIORITY_ANSWER,
                )
            except Exception as exc:
                self._debug(f"no-ACK completion {'REPORT' if report else 'ANSWER'} to {sender_token!r} (pkt_id={pkt_id}) failed locally: {exc}.")
            return
        try:
            ok, _waited_full_timeout = await self._send_direct_frame_and_wait_for_ack(
                target, frame, (nonce or 0) & 0x03, peer_prefix=peer_prefix,
                priority=self.PRIORITY_ANSWER, time_critical=True,
                kind="completion_report" if report else "completion_answer",
                # 2026-09-20: the ANSWER's own ACK wait is hop-aware too (it
                # used to run with hop_count=None, i.e. the flat firmware
                # suggestion, so neither the hop cap nor the abort applied).
                hop_count=hops,
                # Phase 1 (2026-09-20): best effort, never retried -- a
                # queued Link handshake may cut this ACK wait once the
                # peer's expected ACK time has passed.
                preemptible=True,
            )
            if not ok:
                self._debug(
                    f"completion ANSWER to {sender_token!r} (pkt_id={pkt_id}) got no ACK -- "
                    f"not retried; the querier's own timeout is the recovery path."
                )
        except Exception as exc:
            self._debug(
                f"completion ANSWER to {sender_token!r} (pkt_id={pkt_id}) failed "
                f"locally: {exc}."
            )

    def _completion_answer_hold_s(self, hops: int) -> float:
        """Quiet time before a completion ANSWER leaves, through repeaters
        (MeshBench finding 3, 2026-09-20, real firmware): the firmware ACKs
        the QUERY the instant it arrives, and every repeater in the chain
        then relays that ACK; the ANSWER used to go out the millisecond the
        ACK's own airtime ended, exactly as the first repeater keyed its
        relay of it, and all three two-hop ANSWERs in `two_hop` were lost
        that way. The ACK's airtime x (1 + 2.5 x hops) covers the relay
        chain including the repeaters' random forward delay (0 to 1.5
        airtimes each). Zero hop: no relay, no hold."""
        if hops <= 0:
            return 0.0
        ack_airtime_s = self._estimate_tx_airtime_s("", on_air_bytes=12)
        return max(0.0, ack_airtime_s * (1.0 + 2.5 * hops))

    def _reconstruct_from_parity(self, key, bucket) -> "Optional[tuple[int, bytes]]":
        """M4: if any held parity covers exactly one missing data fragment,
        return (frag_idx, payload) for it -- XOR of the parity and the
        other covered fragments padded to the parity's width, the last
        covered fragment trimmed to its recorded length."""
        for mask, (last_len, xor) in list(bucket.parity.items()):
            covered = [i for i in range(bucket.frag_total) if mask & (1 << i)]
            missing = [i for i in covered if i not in bucket.fragments]
            if len(missing) != 1:
                continue
            idx = missing[0]
            acc = bytearray(xor)
            for i in covered:
                if i == idx:
                    continue
                for k, b in enumerate(bucket.fragments[i]):
                    if k < len(acc):
                        acc[k] ^= b
            data = bytes(acc[:last_len]) if idx == max(covered) else bytes(acc)
            self._debug(f"reassembly {key}: fragment {idx} reconstructed from parity mask {mask:#x}.")
            if self._packet_capture_file is not None:
                self._capture_event("in", {"event": "raw_parity_reconstructed", "sender_token": key[1],
                                           "pkt_id": key[2], "frag_total": key[3], "frag_idx": idx, "mask": mask})
            bucket.parity.pop(mask, None)
            return idx, data
        return None

    def _handle_direct_multifragment_frame(
        self, header: _FrameHeader, payload: bytes, sender_token: str, raw: bool = False,
        report_requested: bool = False, parity: bool = False,
    ) -> None:
        """docs/wire_format_design.md's DIRECT-needs-fragmenting receive
        side (Milestone 6) -- reuses the exact same reassembly/dedup
        machinery `_handle_channel_frame`/`_add_channel_fragment` already
        built for CHANNEL. Both are keyed off `(mode, sender_token,
        pkt_id, frag_total)` (`_reassembly_key`) -- `mode` is included
        specifically so DIRECT's own `sender_token` (a peer's
        `pubkey_prefix`) and CHANNEL's (a node's own plaintext,
        attacker-controlled `adv_name`) can never collide in
        `self._reassembly`/`self._dedup`, even though nothing stops a
        CHANNEL sender from choosing an `adv_name` equal to some other
        peer's real pubkey_prefix (a code-review-found gap this method's
        own docstring used to wrongly claim was structurally impossible
        without this). The one real difference from CHANNEL: once
        reassembly completes, §7's
        opportunistic token learning runs on the *complete* packet, using
        the peer identity DIRECT alone can authenticate -- CHANNEL's own
        multi-fragment path can't do this at all (no sender identity to
        learn from), which is why `_add_channel_fragment` itself stays
        deliberately silent on this and this caller adds it instead of
        pushing DIRECT-specific behavior down into the shared helper.

        Field-diagnosed fix (2026-09-18, see module docstring): this is
        also the only place `_last_incoming_direct_at` (`_wait_for_
        incoming_quiet`'s trigger) gets set now, and only when this
        fragment leaves its bucket still incomplete -- concrete evidence
        this sender has more fragments of this specific transfer still
        coming, unlike the old "any DIRECT frame heard" trigger that
        counted its own ACKs/PROOFs/completion-checks and caused a mutual
        reset feedback loop between two chatty nodes."""
        key = self._reassembly_key(header, sender_token, mode="direct")
        if raw and header.pkt_id is not None:
            self._note_recent_raw_pkt(sender_token, header.pkt_id, header.frag_total)
            # 2b: this sender is still bursting -- a held complete report
            # waits for the silence again.
            self._rearm_sender_report(sender_token)

        if raw and parity:
            # M4: a parity fragment. Keep it on the bucket (opening one if
            # needed), then reconstruct if exactly one covered fragment is
            # missing; the reconstructed fragment re-enters this method as
            # an ordinary fragment, so completion, reports and RNS delivery
            # take the one path.
            if self._dedup_contains(key):
                self._incoming_dropped_total += 1
                if report_requested and not self._report_recently_sent(sender_token, header.pkt_id, len(payload) + self.RAW_HEADER_SIZE):
                    self._send_completion_report(sender_token, header, complete=True, held=set(range(header.frag_total)), held_s=0.0)
                return
            bucket = self._reassembly.get(key)
            if bucket is None:
                bucket = self._new_reassembly_bucket(key, header.frag_total, header.coop)
            if report_requested:
                bucket.flagged_seen = True
            if len(payload) >= 2:
                bucket.parity[header.frag_idx] = (payload[0], bytes(payload[1:]))
                bucket.last_progress = time.monotonic()
                self._capture_fragment_received(key[0], key[1], header.pkt_id, header.frag_idx, header.frag_total,
                                                len(bucket.fragments), raw=True, parity=True)
            reconstructed = self._reconstruct_from_parity(key, bucket)
            if reconstructed is not None:
                idx, data = reconstructed
                self._handle_direct_multifragment_frame(
                    _FrameHeader(header.version, True, header.coop, header.pkt_id, idx, header.frag_total, header.attempt),
                    data, sender_token, raw=True, report_requested=report_requested or bucket.flagged_seen,
                )
                return
            if report_requested and key in self._reassembly:
                self._schedule_gaps_report(key, sender_token, header, len(payload) + self.RAW_HEADER_SIZE)
            return

        if self._dedup_contains(key):
            self._incoming_dropped_total += 1
            self._debug(f"dropping late/duplicate DIRECT fragment for {key} (already delivered).")
            if raw and report_requested and not self._report_recently_sent(
                    sender_token, header.pkt_id, len(payload) + self.RAW_HEADER_SIZE):
                # Completion report (2026-09-20): a flagged fragment for a
                # packet already delivered means the sender never got the
                # report (or a QUERY's answer) and re-burst -- tell it again,
                # so it stops without a QUERY round trip. Not within the
                # burst tail of a report just sent (item 3, alpha 0.1.6).
                self._send_completion_report(sender_token, header, complete=True, held=set(range(header.frag_total)), held_s=0.0)
            return

        # 2b: the burst's tail is here if THIS fragment is flagged or an
        # earlier flagged frame (data or parity) of this packet was; read
        # before the bucket is consumed by a completion.
        bucket_before = self._reassembly.get(key)
        tail_seen = bool(report_requested or (bucket_before is not None and bucket_before.flagged_seen))
        complete_data = self._add_channel_fragment(key, header, payload, raw=raw)
        if complete_data is None and raw:
            bucket = self._reassembly.get(key)
            if bucket is not None and report_requested:
                bucket.flagged_seen = True   # the bucket may have been opened by this fragment
            # M4: a parity that arrived earlier may now cover exactly one gap.
            if bucket is not None and bucket.parity:
                reconstructed = self._reconstruct_from_parity(key, bucket)
                if reconstructed is not None:
                    idx, data = reconstructed
                    self._handle_direct_multifragment_frame(
                        _FrameHeader(header.version, True, header.coop, header.pkt_id, idx, header.frag_total, header.attempt),
                        data, sender_token, raw=True, report_requested=report_requested or bucket.flagged_seen,
                    )
                    return
        if complete_data is None:
            self._last_incoming_direct_at = time.monotonic()
            if raw and report_requested:
                # One of the burst's last two fragments landed but the bucket
                # has gaps: report the bitmap unasked, so the sender re-drives
                # exactly the missing fragments without a QUERY first --
                # after a hold, since the flagged second-last fragment is
                # usually followed by the last one within a fragment airtime
                # (phase 3 M1, 2026-09-20; docs/reconcile_redesign.md).
                self._schedule_gaps_report(key, sender_token, header, len(payload) + self.RAW_HEADER_SIZE)
        else:
            peer_prefix = self._canonical_peer_prefix(sender_token)
            proof_key = None
            if raw:
                # Report BEFORE RNS sees the packet, so the report enters the
                # radio lock ahead of whatever RNS sends back (a PROOF, the
                # next Resource request) and the sender learns first. A gaps
                # report still held for this bucket is dropped: complete
                # supersedes it (M1 debounce).
                #
                # Alpha 0.1.8 (item 1): with one exception. When RNS is
                # about to prove this very packet, its PROOF tells the
                # sender everything the report would, so the report is held
                # a moment and dropped if the proof appears -- and then RNS
                # must see the packet FIRST, or there is no proof to wait
                # for. `_proof_may_replace_report` states the four gates.
                self._cancel_gaps_report(key)
                rns_header = self._parse_rns_header(complete_data)
                proof_key = self._proof_may_replace_report(complete_data, rns_header, sender_token, peer_prefix)
                if proof_key is not None:
                    pass          # decided below, after process_incoming
                elif tail_seen or not self.direct_report_hold_during_burst:
                    self._send_completion_report(sender_token, header, complete=True, held=set(range(header.frag_total)), held_s=0.0)
                else:
                    # 2b: an unflagged fragment completed this part, so the
                    # sender's window is still on the air -- one report for
                    # the whole window once its fragments stop arriving,
                    # unless the flagged last fragment reports first.
                    self._schedule_sender_report(sender_token, header, len(payload) + self.RAW_HEADER_SIZE)
            if not raw:
                self._observe_incoming_rns_packet(complete_data, peer_prefix)
            else:
                # raw=True (2026-09-18 night): the src prefix in a raw frame
                # is unauthenticated, so nothing is learned from it -- unless
                # the claimed peer is one this node already binds and routes
                # to (2026-09-19, twice in one day: first the PROOF
                # correlation, then the token learning a raw ANNOUNCE was
                # silently denied) -- see _observe_raw_received_packet.
                self._observe_raw_received_packet(complete_data, peer_prefix)
            self.process_incoming(
                complete_data, transport="direct_raw_multifragment" if raw else "direct_multifragment",
                sender_peer_prefix=peer_prefix, frag_total=header.frag_total, pkt_id=header.pkt_id,
            )
            if proof_key is not None:
                # RNS has the packet now. On the installed RNS the proof is
                # queued within milliseconds (`Transport.inbound` is
                # synchronous and LXMF's `delivery_packet` proves on its
                # first line); the grace covers RNS 1.5's inbound queue and
                # a loaded host, and expires into the report as before.
                if self._proof_enqueued_at_for_key(proof_key) is not None:
                    self._capture_report_skipped_for_proof(sender_token, header, proof_key)
                else:
                    self._hold_report_for_proof(sender_token, header, proof_key)

    # -- Reassembly (docs/reliability_engine_design.md §5) ----------------

    def _reassembly_key(self, header: _FrameHeader, sender_token: str, mode: str):
        """§5.2's keying scheme. The `0x40`/coop branch keys on
        `(pkt_id, frag_total)` alone, excluding sender identity entirely
        -- required for cooperative broadcast's delegates (each
        transmitting under their own firmware-assigned name) to ever land
        in the same bucket, per wire_format_design.md's "cooperative-
        broadcast reassembly bug" fix. Nothing sets this bit until
        Milestone 7, but the branch is built now per the architecture
        doc's own instruction, so M7 only has to add a sender, never
        touch this receive path again (a DIRECT receiver rejects a
        coop-flagged frame outright before ever reaching this method --
        see `_handle_incoming_frame` -- so this branch is CHANNEL-only in
        practice today, but `mode` is still included below rather than
        relied on implicitly).

        `mode` ("channel"/"direct") is included in every returned key --
        a code-review-found gap, not just defensive style: DIRECT's own
        `sender_token` (a peer's `pubkey_prefix`) and CHANNEL's (a node's
        own plaintext, attacker-controlled `adv_name`) were previously
        assumed to be unable to collide since they're "different kinds of
        identifiers," but nothing actually enforced that -- a CHANNEL
        sender can choose any `adv_name` string it likes, including one
        that happens to equal another peer's real pubkey_prefix, which
        would otherwise land its fragments in the same
        `self._reassembly`/`self._dedup` bucket as that peer's genuine
        DIRECT-fragmented send. Namespacing by transport makes that
        structurally impossible instead of merely assumed-away, matching
        this project's own established preference (e.g. the capability
        hard-rule enforcement) for structural fixes over trusted
        invariants."""
        if header.coop:
            return (mode, "~coop", header.pkt_id, header.frag_total)
        return (mode, sender_token or "~anon", header.pkt_id, header.frag_total)

    def _new_reassembly_bucket(self, key, frag_total: int, coop: bool) -> _ReassemblyBucket:
        if len(self._reassembly) >= self.reassembly_max_keys:
            self._evict_oldest_reassembly_bucket()
        bucket = _ReassemblyBucket(frag_total=frag_total, coop=coop)
        self._reassembly[key] = bucket
        return bucket

    def _evict_oldest_reassembly_bucket(self) -> None:
        # §5.3: bounded capacity, oldest-by-last-progress -- a bucket
        # that's kept receiving fragments across retry passes is clearly
        # still alive regardless of when it happened to start, so
        # eviction targets staleness, not age.
        if not self._reassembly:
            return
        oldest_key = min(self._reassembly, key=lambda k: self._reassembly[k].last_progress)
        self._incoming_dropped_total += 1
        RNS.log(
            f"{self}: reassembly map at capacity ({self.reassembly_max_keys} "
            f"keys) -- evicting oldest-by-last-progress bucket {oldest_key}.",
            RNS.LOG_WARNING,
        )
        del self._reassembly[oldest_key]

    def _add_channel_fragment(self, key, header: _FrameHeader, payload: bytes, raw: bool = False) -> Optional[bytes]:
        """Shared reassembly-fragment-accumulation logic for both CHANNEL
        (`_handle_channel_frame`) and DIRECT
        (`_handle_direct_multifragment_frame`) multi-fragment receipt --
        the name predates DIRECT reassembly (Milestone 2) and is kept
        rather than churned, since the logic itself was already
        transport-agnostic (nothing below reads anything CHANNEL-
        specific). Returns the complete reassembled payload the moment
        the last fragment arrives, or `None` while still incomplete (or
        on a same-index content collision, handled by evicting the whole
        bucket) -- delivering to RNS core (`process_incoming`) and, for
        DIRECT, opportunistic token learning are both the caller's job,
        not this method's, since only the DIRECT caller needs the latter."""
        bucket = self._reassembly.get(key)
        if bucket is None:
            bucket = self._new_reassembly_bucket(key, header.frag_total, header.coop)

        if header.frag_idx in bucket.fragments:
            # §6: a repeated frag_idx must be byte-identical by
            # construction (every attempt of a given index carries the
            # same payload bytes) -- verify rather than assume, since
            # this is also the safety net for §5.1/§5.2's residual
            # collision risk (two different transmissions sharing a key).
            if bucket.fragments[header.frag_idx] != payload:
                self._incoming_dropped_total += 1
                RNS.log(
                    f"{self}: reassembly collision detected for key {key} at "
                    f"frag_idx {header.frag_idx} -- two different "
                    f"transmissions produced different bytes for the same "
                    f"slot; evicting rather than risk delivering a "
                    f"corrupted amalgam.",
                    RNS.LOG_WARNING,
                )
                self._reassembly.pop(key, None)
            return None  # identical repeat: already counted, nothing more to do

        bucket.fragments[header.frag_idx] = payload
        bucket.last_progress = time.monotonic()
        
        if header.pkt_id is not None:
            self._capture_fragment_received(
                key[0], key[1], header.pkt_id, header.frag_idx, header.frag_total, len(bucket.fragments), raw=raw,
            )

        if len(bucket.fragments) < bucket.frag_total:
            self._debug(
                f"reassembly progress {key}: {len(bucket.fragments)}/{bucket.frag_total}"
            )
            return None

        # Complete: per §6, delete from the reassembly map immediately
        # and record it in the whole-packet dedup cache in the same step
        # -- the two caches are sequential for a given key, never
        # concurrent.
        del self._reassembly[key]
        complete_data = b"".join(bucket.fragments[i] for i in range(bucket.frag_total))
        self._dedup_add(key, complete_data)
        self._debug(f"reassembly complete {key}: {len(complete_data)} bytes")
        return complete_data

    async def _reassembly_cleanup_loop(self):
        """§5.4's idle-since-last-progress TTL, swept periodically rather
        than checked lazily -- a bucket that simply stops receiving
        fragments needs to be reclaimed even if nothing ever queries it
        again. Also sweeps expired whole-packet dedup entries (§7),
        expired PROOF-correlation entries (§7's other table, Milestone 6
        fix below), and idle unknown-destination backoff state
        (code-review fix, `_unknown_dest_backoff_sweep`) in the same pass,
        since all four live only on this event loop thread and share the
        same natural cadence."""
        try:
            while not self.detached:
                await asyncio.sleep(self.REASSEMBLY_CLEANUP_INTERVAL_S)
                if self.detached:
                    break

                now = time.monotonic()
                stale_keys = [
                    key
                    for key, bucket in self._reassembly.items()
                    if now - bucket.last_progress > (
                        self.reassembly_idle_timeout_coop_s
                        if bucket.coop
                        else self.reassembly_idle_timeout_s
                    )
                ]
                for key in stale_keys:
                    del self._reassembly[key]
                    self._incoming_dropped_total += 1
                    RNS.log(
                        f"{self}: reassembly bucket {key} evicted -- idle "
                        f"timeout exceeded with no completion.",
                        RNS.LOG_WARNING,
                    )

                self._dedup_sweep(now)
                self._proof_correlation_sweep(now)
                self._unknown_dest_backoff_sweep(now)
                self._path_response_rate_limit_sweep(now)
                self._pending_link_request_sweep(now)
                self._send_answered_sweep(now)
                self._announce_cache_sweep(now)
                # Alpha 0.1.8 (item 4): persist it here rather than at
                # detach, so an unclean exit (the field's restarts) still
                # leaves a usable file. A no-op unless something changed.
                self._save_announce_cache()
                self._outgoing_inflight_sweep(now)
                self._resumable_sends_sweep(now)
                self._closed_links_sweep(now)
                self._pending_dest_proofs_sweep(now)
                for path_hex in [p for p, n in self._raw_unsupported_paths.items()
                                 if now - n["since"] >= self.direct_raw_path_unsupported_ttl_s]:
                    del self._raw_unsupported_paths[path_hex]
        except asyncio.CancelledError:
            pass

    def _proof_correlation_sweep(self, now: float) -> None:
        """Field-diagnosed gap: `_resolve_routing_peer`'s own PROOF-
        correlation lookup already expires an entry lazily, on the exact
        lookup that would use it (§7), but most delivered packets never
        actually get proved -- an RNS Link doesn't send a PROOF back for
        every single DATA packet -- so an entry whose PROOF never comes
        would otherwise sit in `_proof_correlation` forever, unlike
        `_dedup`/`_reassembly`, which both already get swept here
        regardless of whether anything ever queries them again. Confirmed
        live: `proof_correlations_pending` sat perfectly flat for minutes
        during a real field test, exactly the signature of a table with
        no time-based reclaim. This is that reclaim, mirroring
        `_dedup_sweep`'s own shape."""
        expired = [h for h, (_peer, expiry) in self._proof_correlation.items() if now >= expiry]
        for h in expired:
            del self._proof_correlation[h]
        # Alpha 0.1.7 (item 1): a proof older than proof_max_age (or 120 s
        # with that disabled) is no longer in the queue either way.
        max_age = self.proof_max_age_s if self.proof_max_age_s > 0 else 120.0
        for h in [h for h, t in self._proof_enqueued_at.items() if now - t > max_age]:
            del self._proof_enqueued_at[h]

    # -- Whole-packet dedup (docs/reliability_engine_design.md §7) --------

    def _dedup_get(self, key) -> Optional[bytes]:
        """Returns the cached whole-packet payload for `key` if present
        and not yet expired (also lazily evicting an expired entry it
        finds along the way), else None."""
        entry = self._dedup.get(key)
        if entry is None:
            return None
        expiry, cached_payload = entry
        if time.monotonic() >= expiry:
            del self._dedup[key]
            return None
        return cached_payload

    def _dedup_contains(self, key) -> bool:
        return self._dedup_get(key) is not None

    def _dedup_add(self, key, payload: bytes) -> None:
        self._dedup[key] = (time.monotonic() + self.whole_packet_dedup_ttl_s, payload)

    def _dedup_sweep(self, now: float) -> None:
        expired = [k for k, (expiry, _payload) in self._dedup.items() if now >= expiry]
        for k in expired:
            del self._dedup[k]
