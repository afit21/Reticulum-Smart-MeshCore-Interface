"""Configuration: the _configure_* family (each the authoritative source for its own keys and defaults, pinned by tests/test_shipped_defaults.py and tests/golden/config_defaults.json), the timing-budget sanity check and the loop-interval floor."""

import RNS

from ._common import _cfg_bool


class _ConfigMixin:
    # -------------------------------------------------------------------
    # Config loading (design invariant #3: every value read here must be
    # used somewhere else in this file -- no automated check exists,
    # verify by hand when adding a key)
    # -------------------------------------------------------------------

    def _configure_identity(self, cfg):
        self.name = cfg.get("name", "Smart MeshCore Interface")

    def _configure_transport(self, cfg):
        self.transport = cfg.get("transport", "serial").lower()

        self.port = cfg.get("port", "/dev/ttyUSB0")
        self.baudrate = int(cfg.get("baudrate", 115200))
        self.host = cfg.get("host", "127.0.0.1")
        self.tcp_port = int(cfg.get("tcp_port", 4403))
        self.ble_name = cfg.get("ble_name", "")

        # The meshcore library's own connection manager can detect a
        # dropped serial/BLE/TCP link and transparently reconnect
        # (CONNECTED/DISCONNECTED events, see _on_mc_connected/
        # _on_mc_disconnected below). Default on: an unattended field radio
        # should try to recover from a USB re-enumeration or a brief BLE
        # range loss rather than sitting dead until rnsd is restarted.
        self.auto_reconnect = _cfg_bool(cfg.get("auto_reconnect", "yes"))
        # Alpha 0.1.6 (item 4, 2026-09-22): the interface's own connection
        # supervisor replaces the library's reconnect (which retried three
        # times a second apart and then stayed dead). `max_reconnect_
        # attempts` is now the supervisor's cap, 0 = forever (was the
        # library's 3); `auto_reconnect = no` stays offline after a drop.
        # The retry delay starts at `connect_retry_min` and doubles to
        # `connect_retry_max`. Opening a serial port asserts DTR / RTS,
        # which resets a Heltec V3 (boot text on the UART for a second or
        # two), so the handshake waits `serial_open_settle` after the open,
        # flushes the input and tries `handshake_attempts` times at
        # `handshake_timeout` each. `command_timeout` is how long a command
        # is owed its reply while the reader's own ERROR events for garbled
        # frames are ignored (the library's 15 s default); more than
        # `serial_noise_warn_per_min` of those in a minute is logged as a
        # corrupted stream (two processes on one port, typically).
        self.max_reconnect_attempts = int(cfg.get("max_reconnect_attempts", 0))
        self.connect_retry_min_s = max(1.0, float(cfg.get("connect_retry_min", 5.0)))
        self.connect_retry_max_s = max(self.connect_retry_min_s, float(cfg.get("connect_retry_max", 60.0)))
        self.serial_open_settle_s = max(0.0, float(cfg.get("serial_open_settle", 2.0)))
        self.handshake_attempts = max(1, int(cfg.get("handshake_attempts", 5)))
        self.handshake_timeout_s = max(1.0, float(cfg.get("handshake_timeout", 5.0)))
        self.command_timeout_s = max(1.0, float(cfg.get("command_timeout", 15.0)))
        self.serial_noise_warn_per_min = max(1, int(cfg.get("serial_noise_warn_per_min", 5)))

        # RNS-facing nominal bitrate. Deliberately NOT the base class's
        # 62500 default (`reliability_engine_design.md`'s base-class
        # contract notes call this out explicitly): a real LoRa link run
        # over MeshCore's default settings is far slower than that, and
        # RNS core uses this value for its own throughput-sensitive
        # decisions (announce pacing, Link-establishment timing). User-
        # requested (2026-09-16): defaults to 80, not just recommended in
        # the README/reasoned-but-unmeasured 300 this used to fall back
        # to -- deliberately low so RNS core stays patient/tolerant of
        # this radio's real transit time ("lowball RNS so that it acts
        # more patient," the user's own framing from the same real-
        # hardware session that settled on 80 as the actual deployed
        # value everywhere this interface has been field-tested since).
        # Set explicitly to override for a radio config known to sustain
        # something faster.
        self.bitrate = int(cfg.get("bitrate", 80))

        # User-requested fix (2026-09-16, direct user instruction): "all
        # interfaces should spend the majority of their time listening" --
        # a global cap on how much of any trailing duty_cycle_window this
        # interface spends transmitting, across every CHANNEL/DIRECT/bind-
        # frame send alike. See _DutyCycleLimiter's own docstring for the
        # full design (why the wait loop wakes exactly when room frees up
        # rather than polling on a fixed interval) and each of `_send_
        # channel_fastpath_frame`/`_send_channel_multifragment_pass`/
        # `_send_direct_frame`/`_send_bind_frame`'s own call sites for
        # where it's actually enforced. Defaults match the user's own
        # stated numbers exactly (30% of a rolling 10s window) --
        # window raised to 60s on 2026-09-18 at the user's decision after
        # the zero-hop page-load capture (see module docstring): the same
        # 30%, but a burst can now actually use it instead of the ~26% a
        # 10s window quantizes to --
        # deliberately not derived from any field measurement, a
        # precautionary ceiling rather than a data-driven one.
        self.duty_cycle_enabled = _cfg_bool(cfg.get("duty_cycle_enabled", "yes"))
        self.duty_cycle_window_s = float(cfg.get("duty_cycle_window", 60.0))
        self.duty_cycle_max_fraction = float(cfg.get("duty_cycle_max_fraction", 0.30))
        # Alpha 0.1.5 (2026-09-21, the owner's decision after the alpha
        # 0.1.4 field session): two ledgers over the same window. Every
        # frame a repeater will relay -- any DIRECT frame with a routed
        # path, every CHANNEL flood (announces, path requests, bind frames)
        # -- is charged to both and waits on both budgets, so what touches
        # a repeater stays at `duty_cycle_max_fraction` (30%). A zero-hop
        # DIRECT frame (the target's out_path_len is 0) is charged to the
        # total ledger only and waits on this cap: 85%, because the
        # zero-hop 12-part page of that session took 147 s of which 109 s
        # were waits at 30%, and a frame between two adjacent radios costs
        # nobody else's repeater any air. Never loosen the 30%.
        self.duty_cycle_max_fraction_zero_hop = float(cfg.get("duty_cycle_max_fraction_zero_hop", 0.85))
        # User-requested (2026-09-18 evening, see module docstring): link-
        # maintenance traffic (PRIORITY_HANDSHAKE -- LINKREQUEST, PROOF,
        # KEEPALIVE..LRPROOF, RESOURCE_PRF/ICL/RCL) never waits for budget;
        # its airtime is still charged to the window so data pays for it.
        # A Link lost to a throttled keepalive costs far more air to
        # re-establish than the keepalive itself.
        self.duty_cycle_exempt_handshake = _cfg_bool(cfg.get("duty_cycle_exempt_handshake", "yes"))

        # Field-diagnosed fix (2026-09-16, real zero-hop hardware test, the
        # very next thing tried after the duty-cycle cap above shipped):
        # deliberately a *separate* value from `bitrate` above, not reused
        # -- and, per direct user instruction, deliberately decoupled from
        # `bitrate`'s own "lowball it so RNS stays patient" philosophy too.
        # First cut reused `bitrate` for the airtime estimate, and a real
        # zero-hop probe run immediately showed why that was wrong: every
        # exchange took a consistent, suspicious ~10.3-10.7s even fully
        # uncontended (queue_depth=1, lock_wait=0.00s in the capture/debug
        # log). `bitrate`'s own deployed value (80, per this file's own
        # "bitrate tuning guidance") is deliberately chosen to model
        # CHANNEL's *worst-case sustained* throughput and keep RNS core's
        # own unrelated timeout math patient -- not a real single-frame
        # over-the-air rate, and a value this interface *wants* to keep
        # low for that separate reason regardless of what's tuned here.
        # First fix (300, "DIRECT's real observed throughput" per
        # `bitrate`'s own history) still wasn't enough: the same real
        # hardware test's very next run showed a normal Link+Resource
        # exchange (several distinct frames -- LINKREQUEST, LRPROOF,
        # Resource parts, PROOF -- landing within a few seconds of each
        # other) still tripped the cap repeatedly, ~8-9s waits on
        # literally every 2-fragment exchange, because several such
        # frames' *estimated* airtime at only 300bps still added up past
        # the cap faster than real transmission plausibly would. Raised
        # to 1200 -- a more realistic raw LoRa PHY figure -- so the cap
        # still catches genuinely heavy bursts without firing on ordinary
        # traffic. Kept independently configurable (not hardcoded) since
        # a deployment with different real radio parameters (SF/BW/CR)
        # would have a different real answer here too.
        self.duty_cycle_estimate_bitrate = int(cfg.get("duty_cycle_estimate_bitrate", 1200))

        # User-requested fix (2026-09-16): "if we hear a message come in
        # via direct, we wait 3 seconds to hear another before we send
        # again... wait for the incoming interface to either stop
        # sending or hit its airtime limit." A real DIRECT frame arriving
        # is direct evidence the channel was *just* occupied by another
        # node -- this interface has no real-time channel-busy/CAD signal
        # from the `meshcore` library (confirmed: nothing exposes that),
        # so a received frame is the best available proxy for "someone
        # else is transmitting nearby right now" this design has access
        # to. Complementary to, not a replacement for, the duty-cycle cap
        # above: that one throttles based on *this interface's own*
        # recent transmit history; this one defers based on what it just
        # *heard*, to avoid keying the radio into the middle of a peer's
        # own multi-fragment burst (they're very likely mid-transfer if a
        # fragment was just heard, not done). `incoming_quiet_window_s`
        # (3.0s, matching the user's own number exactly) is a rolling
        # window: hearing another DIRECT frame while already waiting
        # extends it, mirroring "wait for them to stop sending." Since
        # this interface can't actually observe a peer's own airtime
        # budget or duty-cycle state, "or hit its airtime limit" is
        # approximated by `incoming_quiet_defer_max_wait_s` (15.0s) -- a
        # bound on this node's own patience, not a real measurement of
        # the other side's limit, so a continuously-chatty peer can never
        # starve this node's own outgoing traffic indefinitely.
        self.incoming_quiet_defer_enabled = _cfg_bool(cfg.get("incoming_quiet_defer_enabled", "yes"))
        self.incoming_quiet_window_s = float(cfg.get("incoming_quiet_window", 3.0))
        self.incoming_quiet_defer_max_wait_s = float(cfg.get("incoming_quiet_defer_max_wait", 15.0))

        # Step 4 of "lessen our reliance on arbitrary wait times"
        # (2026-09-18, see module docstring): holds derived from what the
        # radio just overheard (the step-1 RX-log feed) in place of the
        # fixed random windows. DEFAULT OFF -- every number below was
        # characterised on zero-hop hardware only (this project's standing
        # rule: no timing behaviour changes without field evidence from
        # the regime they target, and the losses live at 1-2 hops). The
        # model runs regardless of this flag so captures record what it
        # *would* have done (`predicted_hold_s`/`hold_reason` on every
        # rx_log record, `medium_busy_remaining_s`/`miss_diagnosis` on
        # every attempt); the flag only decides whether anything acts on
        # it. Two things it replaces when on:
        #   (a) pre-transmit: `_pre_transmit_gate` waits out `_medium_busy_
        #       until`, a rolling prediction of when the air goes quiet,
        #       extended by every overheard packet according to what must
        #       follow it -- a FLOOD packet will be re-flooded by every
        #       repeater in range (measured 0.5-1.2s after it, at SF7:
        #       `rx_log_hold_flood_factor` x its airtime); a DIRECT-routed
        #       packet with N path hashes left has N more forwards coming
        #       (`rx_log_hold_hop_factor` x airtime each, matching the
        #       firmware's 0-1x-airtime direct retransmit delay plus the
        #       forward itself) and, if it's an ACK-bearing type, an ACK
        #       turnaround after that (an ACK frame's airtime +
        #       `rx_log_hold_turnaround_s` of firmware/host processing);
        #       an ACK or ADVERT itself has nothing following it. Airtime
        #       comes from the real LoRa time-on-air formula using the
        #       radio's own SF/BW/CR from SELF_INFO (`_estimate_airtime_
        #       s`), so the holds scale correctly when a deployment runs
        #       SF10 instead of this test rig's SF7.
        #   (b) post-miss: instead of a flat random 0.3-3s after a missed
        #       ACK, the attempt's own RX window (step 2) is diagnosed --
        #       target-originated traffic heard while we waited means the
        #       target was transmitting, not listening (`target_busy`);
        #       our own frame heard forwarded by a repeater but no ACK
        #       means hop 1 worked (`downstream_loss`) and an immediate
        #       retry is as good as any; no forward heard where one was
        #       due means it died at hop 1 (`hop1_loss`). The hold is
        #       then "until the predicted busy window ends" plus a small
        #       jitter, or jitter alone for downstream_loss.
        # Every hold is capped at `rx_log_hold_max_s`; a flood of overheard
        # traffic can never stall a send longer than that per attempt.
        self.rx_log_holds_enabled = _cfg_bool(cfg.get("rx_log_holds_enabled", "no"))
        self.rx_log_hold_max_s = float(cfg.get("rx_log_hold_max", 4.0))
        self.rx_log_hold_flood_factor = float(cfg.get("rx_log_hold_flood_factor", 2.5))
        self.rx_log_hold_hop_factor = float(cfg.get("rx_log_hold_hop_factor", 1.5))
        self.rx_log_hold_turnaround_s = float(cfg.get("rx_log_hold_turnaround", 0.4))
        self.rx_log_hold_jitter_min_s = float(cfg.get("rx_log_hold_jitter_min", 0.2))
        self.rx_log_hold_jitter_max_s = float(cfg.get("rx_log_hold_jitter_max", 0.8))

    def _configure_channel(self, cfg):
        self.channel_idx = int(str(cfg.get("channel_idx", self.DEFAULT_CHANNEL_IDX)).strip())
        self.channel_name = cfg.get("channel_name", "RNSTunnel")

        raw_channel_secret = cfg.get("channel_secret")
        self._using_default_channel_secret = raw_channel_secret is None
        self.channel_secret_hex = (
            raw_channel_secret
            if raw_channel_secret is not None
            else self.DEFAULT_CHANNEL_SECRET_HEX
        )

    def _configure_radio(self, cfg):
        # Optional overrides applied to the connected node's radio at
        # startup. Left at 0 (falsy), the node keeps whatever radio
        # parameters are already stored on it.
        self.radio_freq = float(cfg.get("freq", 0))
        self.radio_bw = float(cfg.get("bw", 0))
        self.radio_sf = int(cfg.get("sf", 0))
        self.radio_cr = int(cfg.get("cr", 0))

    def _configure_fragmentation(self, cfg):
        # Inter-fragment spacing tiers (docs/reliability_engine_design.md
        # §2), selected by _fragment_spacing_range(). The DIRECT-fragmented
        # sender passes the resolved path's out_path_len (zero-hop or
        # known-N-hop tier); the CHANNEL multi-fragment path passes None and
        # gets the flat unknown-multi-hop range below, since a broadcast has
        # no single audience depth.
        self.fragment_delay_min_s = float(cfg.get("fragment_delay_min", 5.0))
        self.fragment_delay_max_s = float(cfg.get("fragment_delay_max", 10.0))
        self.fragment_delay_zero_hop_min_s = float(cfg.get("fragment_delay_zero_hop_min", 0.5))
        self.fragment_delay_zero_hop_max_s = float(cfg.get("fragment_delay_zero_hop_max", 1.5))
        self.fragment_delay_per_hop_min_s = float(cfg.get("fragment_delay_per_hop_min", 5.0))
        self.fragment_delay_per_hop_max_s = float(cfg.get("fragment_delay_per_hop_max", 10.0))

        # Each pass sends frag_idx in a freshly shuffled order rather than
        # always 0..N-1 -- targets the position-dependent half of the
        # loss pattern §2 documents (whichever fragment goes out first in
        # a pass carries no risk from a still-propagating predecessor).
        self.fragment_order_shuffle = _cfg_bool(cfg.get("fragment_order_shuffle", "yes"))

        # Reassembly lifecycle (§5.3-5.4) and whole-packet dedup (§7).
        # reassembly_idle_timeout raised 120 -> 200 (2026-09-19): must cover
        # the worst-case attempt budget a sender can spend on one fragment
        # (_validate_direct_timing_budget: (direct_ack_timeout_routed_max +
        # direct_post_send_listen_max) x max attempts = 48s x 4 = 192s once
        # direct_fragment_finish_attempts became 4). At 120 the interface
        # warned its own defaults were incoherent at every startup.
        self.reassembly_max_keys = int(cfg.get("reassembly_max_keys", 256))
        self.reassembly_idle_timeout_s = float(cfg.get("reassembly_idle_timeout", 200.0))
        self.reassembly_idle_timeout_coop_s = float(cfg.get("reassembly_idle_timeout_coop", 180.0))
        self.whole_packet_dedup_ttl_s = float(cfg.get("whole_packet_dedup_ttl", 150.0))

    def _configure_retry(self, cfg):
        # Per-traffic-class extra CHANNEL retry-pass budgets
        # (docs/reliability_engine_design.md §2's table), keyed off the
        # RNS header fields _parse_rns_header/_retry_extra_for read.
        # `announce_retransmit_extra` covers every ANNOUNCE uniformly --
        # distinguishing a path-response announce (budget 1) from a
        # spontaneous one (budget 0) needs to observe an in-flight path
        # request, which is peer/routing-adjacent state this interface
        # doesn't have until Milestone 5; every announce gets the
        # cheaper, spontaneous-announce default until then, flagged
        # explicitly rather than silently guessed at.
        self.announce_retransmit_extra = int(cfg.get("announce_retransmit_extra", 0))
        # Field-diagnosed (2026-09-18 drive-home capture, 3 hops): the same
        # destination's ANNOUNCE went out four times in five minutes, each a
        # 3-fragment DIRECT exchange traversing three repeaters -- more of
        # the channel's time than the data it carried (the `target_busy`
        # misses that session were mostly this). RNS re-announces on its own
        # schedule and a transport node re-broadcasts others'; this
        # interface forwards one spontaneous ANNOUNCE per destination hash
        # per window. Path-response announces (context PATH_RESPONSE) are
        # exempt: they answer a specific request and have their own 20s
        # limiter. 0 disables.
        self.announce_min_interval_s = float(cfg.get("announce_min_interval", 300.0))
        self.path_req_retransmit_extra = int(cfg.get("path_req_retransmit_extra", 1))
        self.ordinary_data_link_retransmit_extra = int(
            cfg.get("ordinary_data_link_retransmit_extra", 0)
        )
        self.ordinary_data_bare_retransmit_extra = int(
            cfg.get("ordinary_data_bare_retransmit_extra", 1)
        )

        # Independent jittered delay before each retry pass -- drawn
        # fresh per pass, layered outside that pass's own inter-fragment
        # spacing (_fragment_spacing_range), never derived from it. This
        # is what makes passes decorrelated rather than a fixed schedule.
        self.retransmit_jitter_min_s = float(cfg.get("retransmit_jitter_min", 8.0))
        self.retransmit_jitter_max_s = float(cfg.get("retransmit_jitter_max", 20.0))

        # Field-data-driven fix (2026-09-16): real capture from a 5-client
        # field test found DIRECT-fragmented messages where the receiver
        # had already fully reassembled every fragment while the sender
        # was still blindly retrying individual fragments for minutes,
        # because only the fragments' own firmware ACKs -- not the data
        # itself -- failed to make it back (an asymmetric/return-path
        # loss, not a forward-delivery failure). See
        # `_check_remote_completion`'s own docstring for the full
        # mechanism this enables: a lightweight DIRECT query, asked only
        # once both retry passes are exhausted and fragments still appear
        # missing, that lets the receiver's own dedup cache settle the
        # question directly instead of the sender guessing from silence.
        # Fully backward-compatible: a peer that doesn't understand the
        # query frame just never answers, and this falls back to exactly
        # today's give-up behavior once `direct_completion_check_timeout_s`
        # elapses.
        self.direct_completion_check_enabled = _cfg_bool(cfg.get("direct_completion_check_enabled", True))
        self.direct_completion_check_timeout_s = float(
            cfg.get("direct_completion_check_timeout", 5.0)
        )
        # Field fix (2026-09-19 evening session): a hard ceiling on the
        # completion-ANSWER wait, replacing the escalation stack that used to
        # sit on top of the floor above (hop multiplier x RTT term x queue-
        # depth contention term, ceilinged only by direct_ack_timeout_
        # routed_max = 45s). Evidence from that session's 190 completion
        # checks: of the 98 answers that arrived, 70% were within 5s of the
        # budget starting, 96% within 15s, 98% within 20s -- and every band
        # beyond 20s yielded TWO answers in total. Crucially the answer rate
        # FALLS as the budget grows (78% at 5-10s, 92% at 10-20s, 43% at
        # 20-30s, 34% at 40-45s), because a long budget is a symptom of bad
        # conditions rather than a cure for them: the old RTT escalation had
        # the causality backwards. Replaying the session with a 15s cap cuts
        # time spent inside completion waits from 3565s to 1720s (-52%) at a
        # cost of 4 of the 98 answers. A slightly higher ceiling applies from
        # 2 hops out, where the measured round trips are genuinely longer.
        self.direct_completion_check_timeout_max_s = float(
            cfg.get("direct_completion_check_timeout_max", 15.0)
        )
        self.direct_completion_check_timeout_max_multihop_s = float(
            cfg.get("direct_completion_check_timeout_max_multihop", 18.0)
        )
        # Dead-wait trims (2026-09-20): when the QUERY's own firmware ACK was
        # MISSED, the full answer budget above still applied -- yet across
        # the three 2026-09-19 sessions an un-ACKed QUERY was answered only
        # 6/19, 9/65, 4/31 and 0/5 times at 0-3 hops, every hop<=1 answer
        # arrived within 5.6 s, and with `miss_diagnosis=hop1_loss` 0 of 26
        # were ever answered: the missing ACK is the signal that the QUERY
        # never reached the peer (48% of unanswered reconciles, second
        # audit). The answer wait after an un-ACKed QUERY is therefore
        # capped at this grace (from 2 hops the multihop value, where two
        # late answers arrived at 9.0 and 24.3 s), so the re-query goes out
        # 9-12 s sooner. 0 disables the cap.
        self.direct_completion_unacked_grace_s = float(cfg.get("direct_completion_unacked_grace", 6.0))
        self.direct_completion_unacked_grace_multihop_s = float(
            cfg.get("direct_completion_unacked_grace_multihop", 10.0)
        )
        # Per-hop addition to the FLOOR (not the ceiling): a first query, before
        # any RTT sample exists, needs longer at depth. Measured query->answer
        # round trips that session: median 3.2s / 5.7s, p90 11.7s / 16.1s.
        self.direct_completion_check_timeout_per_hop_s = float(
            cfg.get("direct_completion_check_timeout_per_hop", 2.5)
        )
        # Field fix (2026-09-19 night): the RADIO-QUIET WINDOW a reconcile
        # QUERY keeps the radio lock for after its own firmware ACK, so this
        # node is not keying while the ANSWER it just asked for crosses the
        # repeater chain (see _query_remote_fragments and _send_direct_frame_
        # and_wait_for_ack's quiet_wait block). The deadline is
        # `base + per_hop x hops` after the QUERY's own transmit (its
        # MSG_SENT moment, so a lock wait before it does not eat the window),
        # capped by the answer budget itself. Sizing, from `fieldtests/raw/
        # Alpha0.1.2/*nighttest*`: query receipt -> ANSWER on air at the
        # answerer is median 1.3s, and one repeater forward is 1.5-3s per hop
        # (the session's own rx-log echo gap: median 1.75s, p90 2.72s).
        # Anchoring at the transmit rather than at the ACK is what keeps zero
        # hop all but untouched: 1.5s is about the zero-hop ACK latency
        # itself (1.45s median), so the hold there is a few hundred
        # milliseconds, and zero hop measured 96-100% answer delivery in every
        # build with no quiet window at all. 0 for both keys disables the
        # window and restores the fully radio-free answer wait (commit
        # 1919074).
        # Review (2026-09-20): the window is now measured from the QUERY's
        # firmware ACK, not its transmit, and sized 2.0 + 3.0 x hops. Measured
        # in the night and evening captures, answers reached the querier
        # (from the QUERY's ACK) at one hop p50 3.7-4.5s, p90 9-11s; the first
        # cut (1.5 + 2.5 x hops from the transmit, i.e. ~1s after a one-hop
        # ACK) covered only 19-32% of the answers that actually arrived and
        # the simulated one-hop page transfer showed a 9s window beating it on
        # every seed (12/12 parts vs 6-9/12). When three query round trips
        # have been measured for the peer, the window grows to srtt + 2 x
        # rttvar if that is larger, so a slow chain gets the quiet it needs
        # without a config change; the answer budget still caps it.
        self.direct_completion_quiet_base_s = float(cfg.get("direct_completion_quiet_base", 2.0))
        self.direct_completion_quiet_per_hop_s = float(cfg.get("direct_completion_quiet_per_hop", 3.0))

        # Step 3 of "lessen our reliance on arbitrary wait times"
        # (2026-09-18, see module docstring): send-once-then-reconcile for
        # DIRECT-fragmented sends. Pass 0 sends every fragment exactly
        # `direct_fragment_pass0_attempts` (1) time(s); if any fragment
        # got no ACK, ONE completion QUERY asks the receiver which
        # fragments it actually holds (a v2 have-bitmap ANSWER, see
        # `_encode_completion_frame`) and only the fragments it confirms
        # missing are re-driven in pass 1 with the normal attempt budget.
        # Motivation is the 2026-09-16 phantom-ACK field case: fragments
        # that had arrived were blindly retried for minutes because only
        # their ACKs were lost. One QUERY+ANSWER is two small frames; each
        # blind retry is a full fragment plus its ACK -- so this is a net
        # airtime *reduction* whenever at least one "missing" fragment
        # was actually held, and it releases `_direct_exchange_lock`
        # sooner in every case. Disabled (or facing a peer that never
        # answers), pass 0/1 behave exactly as before this step.
        # PRIORITY_HANDSHAKE fragments are exempt and keep their own
        # larger per-fragment budget in pass 0: a lost Link handshake
        # costs a full path rediscovery (see direct_send_attempts_
        # handshake's own comment), and the reconcile round trip would
        # only delay it.
        self.direct_fragment_reconcile_enabled = _cfg_bool(cfg.get("direct_fragment_reconcile_enabled", "yes"))
        self.direct_fragment_pass0_attempts = int(cfg.get("direct_fragment_pass0_attempts", 1))

        # Alpha 0.1.1 fixes (2026-09-18 night, see module docstring):
        # once the receiver provably holds part of a fragmented packet
        # (a pass-0 ACK, or a reconcile answer), the remaining fragments
        # get this larger pass-1 budget -- the drive capture's two path-
        # response announces each died one fragment short on the ordinary
        # budget of 2, wasting the fragments already delivered. And a
        # failed fragmented send is remembered so that RNS re-issuing the
        # identical bytes (its normal retry) resumes the receiver's
        # still-open bucket under the same pkt_id instead of starting a
        # fresh three-fragment send.
        self.direct_fragment_finish_attempts = int(cfg.get("direct_fragment_finish_attempts", 4))
        self.direct_fragment_resume_enabled = _cfg_bool(cfg.get("direct_fragment_resume_enabled", "yes"))

        # Raw binary DIRECT fragments (2026-09-18 night, see module
        # docstring). Default ON since the first field test the same night
        # (user decision): both radios ran it zero-hop and through the
        # public repeater with every transfer completing. Still capability-
        # gated by bind frame, so a peer on an older build never receives
        # raw frames. When on: packets too large for one text frame go to a
        # raw-capable peer as unacknowledged raw bursts reconciled by the
        # "Q" bitmap.
        self.direct_raw_fragments_enabled = _cfg_bool(cfg.get("direct_raw_fragments_enabled", "yes"))
        # Second audit (2026-09-19 evening session): how many fragmented
        # sends (raw or text) to ONE peer may be in flight at once. Bursts
        # were already serialised by the radio lock, but the reconcile
        # windows between them were not, so every queued packet started at
        # once: the desktop had 14 completion windows open simultaneously,
        # the laptop 9. Three costs, all measured in that session: (1) the
        # querier's own radio was transmitting other packets' fragments and
        # queries when 11 of the 25 lost ANSWERs arrived -- half-duplex
        # cannot hear an answer while keying; (2) Resource parts were
        # delivered minutes apart and out of order, and RNS credits a part
        # only inside its receive window from the last consecutive part
        # (Resource.receive_part), so a correctly delivered part
        # (`ca6b3d36db27`, 16:48:25) was discarded and re-requested seven
        # times; (3) one 483-byte packet's raw send stretched past three
        # minutes. Two slots keeps the pipeline full (one packet's fragments
        # can go while the other waits on its answer) without the fan-out.
        # 0 disables the cap. Handshake-class sends bypass it.
        #
        # Field fix (2026-09-19 night, `fieldtests/raw/Alpha0.1.2/*nighttest*`):
        # default 2 -> 0 (off). The first session with the cap on measured
        # against its own purpose: reconcile timeouts did NOT improve
        # (desktop 53% timed out vs 35% the evening before), while the cap
        # cost plenty -- the desktop's fragmented sends waited a median 30s
        # for a slot, four 483-byte Resource parts were dropped after the
        # 120s slot budget ("slot_expired"), and two of the laptop's data
        # packets were dropped while both its slots were held by 30-minute
        # LXMF announces reconciling at two hops. The half-duplex loss the
        # cap was meant to reduce is addressed at its actual location now
        # (the reconcile QUERY's quiet window, direct_completion_quiet_*).
        # When enabled, the slot is priority-aware, announce-class sends
        # get a slot of their own, and a send that cannot get a slot in time
        # proceeds with a warning instead of being dropped -- see
        # _fragmented_send_slot / _PriorityAsyncSemaphore.
        # Review (2026-09-20, simulated one-hop page A/B, seeds 11/21/31, 12 x
        # 483-byte Resource parts, calibrated loss): with the drop removed and
        # the slots priority-aware, the cap is the single most effective
        # change in the set -- every part delivered in 181-243s with raw
        # completion 90-100%, against 3-10 of 12 parts in 600s with the cap
        # off. The night session's objections (dropped parts, announces
        # starving data) are what the rewrite removed, so the default is 2
        # again; the field check the night entry asked for is still owed.
        self.direct_fragmented_max_in_flight = int(cfg.get("direct_fragmented_max_in_flight", 2))
        # Per-fragment raw payload cap on the wire, before the 13-byte
        # header; also bounded by the firmware limits above.
        self.direct_raw_payload_cap = int(cfg.get("direct_raw_payload_cap", 170))
        # Alpha 0.1.5 (2a, 2026-09-21): how many frames a zero-hop raw burst
        # may hold queued in the firmware ahead of the one on air. The
        # firmware queues a frame and returns OK at once, so the pre-0.1.5
        # loop handed a whole window (15 fragments, ~14 s of air) to the
        # radio in 2.6 s: the burst was "over" before the radio had started
        # on most of it, a report arriving meanwhile was read as the end of
        # the wait, handshake yields between fragments yielded nothing (the
        # handshake queued behind the burst), and the companion's packet
        # pool is 16 entries shared with reception (StaticPoolPacketManager
        # in MyMesh.cpp). Now the next fragment is handed over when the
        # radio is estimated to have at most this many frames ahead of it
        # (`_raw_burst_next_send_wait_s`); 1 keeps the air back to back
        # with one frame queued. Through repeaters the hop-scaled gap
        # already exceeds the airtime, so this never binds there. 0 = off
        # (the pre-0.1.5 behaviour).
        self.direct_raw_burst_queue_ahead = int(cfg.get("direct_raw_burst_queue_ahead", 1))
        # Quiet time after each fragment of a burst (the last one included):
        # a flat gap at zero hop (the receiver sends no ACK, so only its own
        # processing needs covering), or this factor x hop count x the
        # fragment's airtime through repeaters -- each repeater in the chain
        # must re-transmit the fragment (after the firmware's random 0-1.5
        # airtime delay) before it can hear the next one. See
        # _raw_fragment_gap_s for the 2026-09-19 field evidence.
        self.direct_raw_zero_hop_gap_s = float(cfg.get("direct_raw_zero_hop_gap", 0.15))
        self.direct_raw_hop_gap_factor = float(cfg.get("direct_raw_hop_gap_factor", 2.0))
        # Alpha 0.1.5 (item 4, 2026-09-21): the field A/B knob for the one-hop
        # gap. MeshBench finding 2 (2026-09-20) added the frame's own airtime
        # to the hop-scaled gap -- `(1 + factor x hops) x airtime` -- because
        # `send_raw_data` returns when the frame is queued, not sent. At one
        # hop that gap is two thirds of a three-fragment part's time, and
        # MeshBench cannot judge it (its frames are ~30% slower than the
        # field's, so its one-hop loss alternates at any gap; and it has no
        # listen-before-talk, which is what would let a real radio drop the
        # `+1` -- the repeater's relay is audible to the sender). `no` drops
        # the `+1 x airtime` term through repeaters (zero hop is untouched);
        # every `raw_fragment_sent` record carries the `gap_s` actually used.
        #
        # Alpha 0.1.9 (2026-09-23): DEFAULT NOW `no`, by the owner's
        # decision. Stated plainly, because this comment used to say the
        # A/B decides and this is the A/B's `no` arm being adopted without
        # it: the procedure in `fieldtests/AB_PROTOCOL.md` has still never
        # been run, so this is a judgement about spending less airtime per
        # burst, not a measured result. What it changes, at SF7 / BW
        # 62.5 kHz / CR 4/8 (the field radios, airtime 0.91 s for a full
        # 170-byte fragment): the gap through repeaters goes from
        # `(1 + 2 x hops)` to `(2 x hops)` airtimes -- one hop 2.73 ->
        # 1.82 s (-33 %), two hops 4.55 -> 3.64 s (-20 %), three hops
        # 6.37 -> 5.46 s (-14 %). Zero hop is untouched.
        #
        # The risk the `+1` covered (MeshBench finding 2, 2026-09-20):
        # `send_raw_data` returns when the frame is QUEUED, not sent, so
        # without the term the fragment's own airtime comes out of the gap
        # and the next fragment can key inside the repeater's relay of the
        # previous one -- which cost 7/7 second fragments in
        # `large_payload` and 7/9 QUERYs in `relay`. Against that: a real
        # SX1262 has listen-before-talk and would defer on hearing that
        # relay, which MeshBench's virtual radio cannot do, so MeshBench
        # systematically overstates this risk. That asymmetry is exactly
        # why the knob exists and why the A/B was specified for the field.
        # Two derived values shrink with the gap and are the ones to
        # watch: `_report_hold_s` (so alpha 0.1.9's proof burst-tail hold
        # drops to 2.28 s at one hop and 4.09 s at two) and the burst-tail
        # suppression window in `_report_recently_sent` (two spacings plus
        # margin: 7.74 s at two hops, which now sits INSIDE the sender's
        # 9.0 s report wait rather than just outside it).
        self.direct_raw_gap_own_airtime = _cfg_bool(cfg.get("direct_raw_gap_own_airtime", "no"))
        # Burst-then-ask rounds per packet, and QUERY tries per round.
        # Audit fix (2026-09-19): clamped to 4. The raw header carries the
        # round in 2 bits (`attempt & 0x03`), and the firmware dedups
        # RAW_CUSTOM by a hash of payload type + payload bytes
        # (SimpleMeshTables::wasSeen, a 160-entry ring with no time expiry),
        # so round 4 would be byte-identical to round 0 and silently dropped
        # as already-seen at both the repeater and the receiver -- a whole
        # burst of airtime for nothing.
        self.direct_raw_reconcile_rounds = max(1, min(4, int(cfg.get("direct_raw_reconcile_rounds", 3))))
        self.direct_raw_query_attempts = int(cfg.get("direct_raw_query_attempts", 2))
        # Alpha 0.1.6 (item 2, 2026-09-22): through repeaters a window's
        # rounds are capped lower than `direct_raw_reconcile_rounds` (which
        # zero hop keeps). Every round at two hops is a burst (three
        # fragments at ~0.9 s plus 4.5 s gaps), a report wait and up to two
        # QUERY exchanges of ~18 s each, and the 2026-09-21 session's
        # two-hop windows ran all three while link proofs and answers
        # queued behind them (23 sends waited more than 30 s for the
        # radio). After this many rounds the window falls back to the
        # existing text path (per-fragment ACKs) or fails, exactly as it
        # does when the rounds are exhausted today. 0: no separate cap.
        self.direct_raw_window_max_rounds = max(0, int(cfg.get("direct_raw_window_max_rounds", 2)))
        # Receiver-initiated completion report (2026-09-20, module docstring
        # entry of that date). After a raw burst the sender used to key its
        # reconcile QUERY the instant the last fragment's gap ended -- which
        # is exactly when the receiver, having just handed the packet to
        # RNS, transmits its own reaction (a delivery PROOF, the next
        # Resource request): at zero hop the two frames collided outright,
        # through a repeater they collided at the repeater as hidden nodes
        # (every baseline MeshBench run, 2026-09-20; field: `answering_
        # complete=True` on 49/82, 45/74 and 51/68 of QUERYs, 3.3 QUERY
        # attempts per raw send, receiver-complete p50 7.7 s vs sender-known
        # p50 34 s at one hop). With the report on, the receiver sends the
        # ANSWER unsolicited the moment the burst lands (complete, or its
        # bitmap when the flagged last fragment arrived with gaps), and the
        # sender keeps its radio quiet for `direct_raw_report_wait_base` +
        # `..._per_hop` x hops seconds after the burst (never longer than
        # the answer budget) before falling back to the QUERY path exactly
        # as before. Saves the QUERY frame and its firmware ACK (and their
        # relays) per delivered part, and removes the QUERY-vs-PROOF
        # collision from the common path. `no` restores burst-then-QUERY.
        self.direct_raw_report_enabled = _cfg_bool(cfg.get("direct_raw_report_enabled", "yes"))
        # Phase 3 M1 (2026-09-20, docs/reconcile_redesign.md): the REPORT
        # and the QUERY's ANSWER go out as MeshCore TXT_TYPE_CLI_DATA --
        # encrypted and MAC'd like any text message, relayed identically,
        # delivered to the host as CONTACT_MSG_RECV with txt_type 1, and
        # NEVER acknowledged by the firmware (`BaseChatMesh::onPeerDataRecv`:
        # "no ack expected for CLI_DATA replies"; `CMD_SEND_TXT_MSG` sets
        # expected_ack 0 for it). The sender's next action confirms a
        # report; a lost one falls through to the QUERY as before. Saves
        # the ACK frame (and its relays) per report and, on the reporting
        # side, the 1-3 s ACK wait that made reports queue behind each
        # other (23 of the 31 report lock waits over 1 s in the 2026-09-20
        # zero-hop session were the previous report's ACK wait). `no`
        # restores ACKed reports and answers.
        self.direct_report_noack = _cfg_bool(cfg.get("direct_report_noack", "yes"))
        # Alpha 0.1.8 (item 2): from this hop count up, a completion REPORT
        # goes through the ACKNOWLEDGED send path instead, with one retry.
        # The no-ACK frame is one transmission and is never retried, and at
        # two hops the 2026-09-22 field session had it reach the sender 3
        # times out of 22 -- the sender then waited out its 10-18 s report
        # wait and spent a whole QUERY round (a QUERY, its ACK and an
        # ANSWER, 2.1-2.4 s of channel time at two hops) to learn what the
        # report already said. The ACK costs about 0.42 s of channel time
        # at two hops, so it pays for itself if it saves roughly one QUERY
        # round in five. At ONE hop it does not: reports arrived 33 times
        # of 48 there in alpha 0.1.6 and the ACK would cost more than it
        # saves, so below the threshold the no-ACK frame and its hold are
        # untouched. The frame's CONTENT is identical either way -- only
        # its carrier changes -- so the golden wire snapshot is unaffected.
        # 0 disables the item entirely.
        self.direct_report_ack_min_hops = max(0, int(cfg.get("direct_report_ack_min_hops", 2)))
        # Phase 3 M1: a flagged fragment that leaves gaps no longer reports
        # at once -- the second-last fragment is flagged too, so at zero hop
        # the receiver sent a gaps report and, 0.2-0.4 s later, the complete
        # one (146 reports for ~105 bursts in the 2026-09-20 session, and the
        # sender re-drove the last fragment as a duplicate 20 times). The
        # gaps report is held for one fragment's airtime plus its relay gap
        # (`_report_hold_s`) and dropped if the bucket completes first. `no`
        # reports immediately as before.
        self.direct_report_debounce = _cfg_bool(cfg.get("direct_report_debounce", "yes"))
        # Alpha 0.1.5 (2b, 2026-09-21): the receiver sends no per-part
        # complete report while fragments of the same sender's window are
        # still arriving. The field (zero-hop 12-part page, 08:38): the
        # laptop reported each part the moment it completed, while the
        # desktop's radio was still transmitting the rest of the window --
        # the report for part 8 reached the desktop mid-burst and ended its
        # wait early, the reports for parts 9 and 10 were transmitted into
        # the desktop's own queue and never heard, and every one of the
        # four on-air losses of that page sat within 2 s of one of those
        # reports. Now a completed part that arrived UNFLAGGED (not one of
        # the burst's last two fragments) is reported after a silence of one
        # fragment's start-to-start spacing at this hop count plus half an
        # airtime (`_report_hold_s(..., arriving=True)`), re-armed by every
        # further fragment from that sender; a flagged fragment reports at
        # once (complete) or after the M1 debounce (gaps), as before, and
        # every report lists the sender's recent packets, so one report
        # covers the window. A lone single-part burst is unchanged: its last
        # two fragments are flagged. `no` reports every completion at once.
        self.direct_report_hold_during_burst = _cfg_bool(cfg.get("direct_report_hold_during_burst", "yes"))
        # Phase 3 M2 (2026-09-20): one report per WINDOW. RNS hands the
        # sender a window of 4-6 Resource parts within milliseconds; each
        # used to be its own burst-and-report exchange (two in flight per
        # peer: ~12 reports and 6 quiet gaps per window). Now consecutive
        # raw-eligible sends to one peer that arrive within
        # `direct_raw_window_collect` seconds of the first (or up to
        # `direct_raw_window_max_parts`) form ONE window burst: every
        # fragment back to back, one quiet period, one v4 report carrying a
        # bitmap per part; re-drives are batched the same way and the v4
        # QUERY asks about the whole window. `direct_raw_window_enabled =
        # no` sends each part as a window of one, with no collect wait.
        # Alpha 0.1.5 (item 5): `direct_raw_window_collect` is the MAXIMUM
        # -- the collect ends as soon as nothing is queued from RNS and no
        # part has joined within the transfer's observed inter-part spacing
        # (floor 40 ms), so a lone packet starts within that floor instead
        # of paying the whole 0.75 s (every zero-hop probe did), while a
        # window of parts arriving together still batches.
        self.direct_raw_window_enabled = _cfg_bool(cfg.get("direct_raw_window_enabled", "yes"))
        self.direct_raw_window_collect_s = float(cfg.get("direct_raw_window_collect", 0.75))
        self.direct_raw_window_max_parts = int(cfg.get("direct_raw_window_max_parts", 6))
        # Phase 3 M4 (2026-09-20): one XOR parity fragment per part per burst
        # from `direct_raw_parity_min_hops` (1) hops up -- none at zero hop,
        # where per-fragment loss is a few percent. With ~18 % per-fragment
        # loss at one hop a 3-fragment part loses exactly one fragment 41 %
        # of the time (none 55 %): parity turns most of that into a first-
        # round completion for one extra fragment on the burst instead of a
        # report + re-burst + report. A re-drive of two or more fragments
        # gets its own parity over the re-driven set. Sent only where the
        # 171-byte parity frame fits the firmware's limits (up to three
        # hops). ON BY DEFAULT at the owner's decision (2026-09-21). The M4
        # gate had shipped it off: under MeshBench the repeater's loss
        # alternates (its relay of fragment N overlaps N+1, the interface's
        # gap being sized to the real ~0.9 s airtime where MeshBench's
        # frames take ~1.3 s), so a four-frame burst leaves two data
        # fragments or the parity itself missing -- three runs 4/6, 5/6,
        # 1/6 against M3's 2/6, 6/6, 6/6, at 25 % more raw bytes per part,
        # while reconstruction itself worked (1, 3, 4 per run). The field's
        # random loss is the case it is for, and the field A/B
        # (fieldtests/AB_PROTOCOL.md, `raw_parity_reconstructed` per
        # single-loss burst) is what confirms or reverses this default;
        # `no` is the other arm.
        self.direct_raw_parity_enabled = _cfg_bool(cfg.get("direct_raw_parity_enabled", "yes"))
        self.direct_raw_parity_min_hops = int(cfg.get("direct_raw_parity_min_hops", 1))
        # Phase 1 (2026-09-20): base 2.0 -> 4.0 s, per hop 3.0 -> 2.5 s (the
        # answer budget's own slope, so the floor stays under the budget at
        # every depth: 4 / 6.5 / 9 / 11.5 s against 5 / 7.5 / 10 / 12.5 s),
        # and the window grows to the MEASURED report latency
        # (`_report_rtt`: srtt + 4 x rttvar, on-time and late reports both
        # sampled) above that floor, still capped by the answer budget. The
        # 2026-09-20 field session, zero hop: the receiver's report waited a
        # median 1.1-1.4 s and p90 4-5 s for its own radio lock (behind its
        # previous report's ACK wait and its own sends) on top of ~2.3 s of
        # serial delivery latency, so with a 2 s window only 43 of the
        # desktop's 77 hop-0 rounds were `reported` and 29 paid a QUERY
        # round trip (two frames, two ACKs, ~5 s) for a report that was
        # merely late. The base is what a lost report costs at zero hop.
        self.direct_raw_report_wait_base_s = float(cfg.get("direct_raw_report_wait_base", 4.0))
        self.direct_raw_report_wait_per_hop_s = float(cfg.get("direct_raw_report_wait_per_hop", 2.5))
        # The flag rides the LAST TWO fragments of a burst: when the last
        # one is lost (uniform ~18% per fragment at one hop in the field,
        # systematic in MeshBench's LBT-less radio) the report the second-
        # last fragment triggered still tells the sender what to re-drive --
        # it may have arrived while the burst was still going, in which case
        # the sender keeps it as the fallback and waits the transit time for
        # a newer one first. Two cuts before this one used a receiver-side
        # idle timer instead; see the module docstring entry for why not.
        # Field fix (2026-09-19, bidirectional image transfer): an unanswered
        # reconcile round used to re-burst every un-ACKed fragment. In that
        # capture 3 data sends went to a peer that already held the packet
        # complete -- its answers were stuck behind its own bursts -- and
        # each re-burst lengthened that queue. An unanswered round now
        # re-queries; a burst is allowed again only after this many
        # CONSECUTIVE unanswered rounds (a safety valve for answers that are
        # systematically lost rather than merely late -- the simulated
        # one-hop scenario produced exactly that). 1 restores the old
        # re-burst-every-round; 0 never re-bursts on silence.
        self.direct_raw_reburst_after_unanswered = int(cfg.get("direct_raw_reburst_after_unanswered", 2))
        # `direct_raw_fallback_strikes` answered reconciles in a row showing
        # a burst delivered nothing -> raw is paused for that peer for
        # `direct_raw_fallback_cooldown` and the packet goes as Z85 text on
        # the same path. If the text send succeeds, the PATH (the repeater
        # chain) is noted as not carrying raw packets for `direct_raw_path_
        # unsupported_ttl` and the peer's pause is lifted; a new path is
        # always tried raw-first again (user's design, 2026-09-18 night).
        self.direct_raw_fallback_strikes = int(cfg.get("direct_raw_fallback_strikes", 2))
        # Field fix (2026-09-19 night, `fieldtests/raw/Alpha0.1.2/*nighttest*`):
        # 600 -> 120. At 21:45:14 ONE raw send lost the same fragment three
        # rounds running and the pause that followed sent the next 46 page
        # parts as five Z85 text fragments plus five ACKs each, for ten
        # minutes -- on a path that had just carried 16 of 20 raw sends to
        # completion. The pause is a hedge against a chain that does not
        # carry raw at all (the strike rule above proves that case within
        # two answered reconciles); a lossy-but-working chain only needs a
        # short breather before raw is worth trying again.
        self.direct_raw_fallback_cooldown_s = float(cfg.get("direct_raw_fallback_cooldown", 120.0))
        # Field fix (2026-09-19 night): how many CONSECUTIVE raw sends may end
        # answered-but-incomplete (the receiver held part of the packet after
        # every round, the text path took the rest) before raw is paused for
        # the cooldown. The e87cca8 build -- 8 of 8 raw sends completing at
        # one hop, 85% answer delivery -- only ever paused on the strike rule
        # above; the unconditional pause after a single incomplete send
        # (review, 2026-09-19) is what turned one unlucky fragment into ten
        # minutes of text. A completed raw send clears the count. 1 restores
        # the pause-on-first-incomplete behaviour; 0 never pauses on
        # incomplete sends (the two-strike "delivered nothing" rule and the
        # per-path verdict are unaffected either way).
        self.direct_raw_incomplete_strikes = int(cfg.get("direct_raw_incomplete_strikes", 2))
        self.direct_raw_path_unsupported_ttl_s = float(cfg.get("direct_raw_path_unsupported_ttl", 86400.0))

        # Field-diagnosed (2026-09-18 drive-home capture, see module
        # docstring): give up on a multi-hop DIRECT attempt early when the
        # first-hop repeater was never heard forwarding our frame. Armed
        # per peer only after `direct_hop1_abort_min_samples` echoes have
        # been measured for the current path; the deadline is
        # `max(direct_hop1_abort_min, multiplier x slowest echo seen)`,
        # never longer than the ACK timeout it shortens. The 5s floor is
        # what keeps a stale hop_count harmless: a zero-hop ACK (1.3-2.2s
        # measured) always arrives first. An abort counts as a real
        # failure toward direct_path_reset_threshold -- silence where a
        # forward was due is evidence, unlike a plain timeout.
        self.direct_hop1_abort_enabled = _cfg_bool(cfg.get("direct_hop1_abort_enabled", "yes"))
        self.direct_hop1_abort_min_samples = int(cfg.get("direct_hop1_abort_min_samples", 3))
        self.direct_hop1_abort_echo_multiplier = float(cfg.get("direct_hop1_abort_echo_multiplier", 2.0))
        self.direct_hop1_abort_min_s = float(cfg.get("direct_hop1_abort_min", 5.0))
        # Second audit (2026-09-19 evening session): the deadline used when
        # NO echo samples exist for the peer -- and none exist exactly when
        # the abort matters most. Echo samples are cleared on every path
        # (re)discovery and stale reset (a new path is a new first hop) and
        # are empty after a restart, so a freshly discovered path -- the one
        # most likely to be wrong -- had no abort at all. The mobile node's
        # capture: 59 of its 72 first-hop-silent misses ran the full 13-21s
        # firmware timeout for want of samples, 835s of waiting where ~295s
        # was needed. Echo timings measured across that whole session were
        # median 2.0s and never above 4.0s at 1-3 hops, so 8s (twice the
        # worst) loses nothing. Applied only when the session-wide pool of
        # echo timings is also empty; 0 restores the samples-only behaviour.
        self.direct_hop1_abort_default_s = float(cfg.get("direct_hop1_abort_default", 8.0))

        # Field-diagnosed (same capture): a packet that has sat in this
        # interface's queue (or behind _direct_exchange_lock) longer than
        # this is dropped instead of sent -- 17 LXMF pings queued through
        # a 4-minute outage drained as a stale burst the moment the path
        # came back. ANNOUNCE is exempt (idempotent, and RNS won't re-send
        # one soon). 0 disables. (The default matched reassembly_idle_timeout's
        # 120s when added; that timeout is 200s since 2026-09-19 and the two
        # are independent.)
        # Refined the same evening (page-load capture, see module
        # docstring): the decision is made ONCE, before a packet's first
        # transmission -- never between fragments or attempts, where a drop
        # only wastes the air already spent -- and Resource data parts
        # (context RESOURCE) are exempt: RNS's Resource layer owns their
        # retransmission and re-requests what it lacks.
        self.outgoing_max_age_s = float(cfg.get("outgoing_max_age", 120.0))
        # Phase 1 (2026-09-20): a plain delivery PROOF (packet type PROOF,
        # context NONE -- not LRPROOF / RESOURCE_PRF / the Link band, which
        # `_proof_is_link_class` keeps as handshake class) is useful only
        # until the far side's receipt deadline: RNS `PacketReceipt.timeout`
        # for a non-Link packet over this interface is `first_hop_timeout`
        # (MTU 500 B x 8 / `bitrate` 80 bps = 50 s, + 6) + 6 s per RNS hop
        # = 62 s, measured from the sender's transmit, after which the
        # receipt is FAILED and the proof does nothing (`Transport.jobs`);
        # LXMF's opportunistic delivery retries every 10 s on top and never
        # waits longer. The desktop's 2-hop phase of the 2026-09-20 session
        # queued 13 proofs while every attempt missed (lock waits 8 -> 70 s)
        # and then transmitted 12 of them aged 45-105 s. Replaying that
        # capture: a 45 s cap skips 16 attempts (~76 s of radio lock) and
        # loses 3 proofs that still landed inside the deadline; 60 s skips
        # 12 and loses none; 30 s skips 24 and loses 4. With ~5 s of transit
        # each way at two hops, 45 s is where the deadline sits. Unlike
        # outgoing_max_age this is checked before EVERY attempt, not only
        # the first: a proof is one bare frame, so a stale retry wastes
        # nothing already spent. 0 disables.
        self.proof_max_age_s = float(cfg.get("proof_max_age", 45.0))
        # Alpha 0.1.7 (item 1): a plain PROOF younger than this (measured
        # from the moment RNS handed it to this interface, which is within
        # milliseconds of the DATA it answers arriving) is treated like a
        # Link handshake for the RADIO LOCK only: it pre-empts idle holds
        # and is taken at the raw window's existing yield points, exactly
        # as an LRPROOF and the receiver's own completion report are. Its
        # tier (ANSWER), attempt budget and duty-cycle accounting do not
        # change, and it still expires at proof_max_age. The 2026-09-22
        # one-hop session: one 211 B LXMF message arrived six times in 70 s
        # because each proof left the radio 5-20 s after its DATA, queued
        # behind the page windows this node was serving, and LXMF re-sends
        # an unproved opportunistic message after DELIVERY_RETRY_WAIT 10 s
        # (checked every 4 s, up to 5 attempts). 8 s: the far side's retry
        # is due at 10-14 s after its send, minus ~2 s of transit at one
        # hop. Past it the proof is bulk-tier as before. 0 disables.
        self.proof_fresh_s = float(cfg.get("proof_fresh_s", 8.0))
        # Alpha 0.1.8 (item 1): how long a raw window's COMPLETE report is
        # held while RNS decides whether to prove the packet that window
        # delivered. RNS proves every single-destination DATA packet, and
        # its PROOF tells the sender exactly what the report would --
        # "I have it" -- so when the proof appears inside the grace the
        # report is dropped and the sender's window ends on the proof
        # instead. Measured on the installed RNS 1.4.2 the proof reaches
        # `process_outgoing` within a millisecond of the packet being
        # handed over (`Transport.inbound` is synchronous and LXMF's
        # `delivery_packet` calls `prove()` on its first line); the grace
        # is set two orders of magnitude above that to cover RNS 1.5's
        # inbound queue (`USE_INBOUND_QUEUE`, one thread hop) and a loaded
        # host. It is only ever spent when a proof is genuinely plausible
        # -- see `_proof_may_replace_report`'s four gates -- so it costs
        # nothing on Resource parts, Link traffic, announces or a transport
        # node's relayed packets. 0 disables the whole item and every such
        # window reports as it did in alpha 0.1.7.
        self.proof_report_grace_s = max(0.0, float(cfg.get("proof_report_grace", 0.25)))
        # How many times the same bytes may be suppressed as "already in
        # flight" before the packet is forced through with a fresh in-flight
        # entry (field fix 2026-09-19: a stuck entry deadlocked a transfer for
        # 178s). 1 disables the suppression entirely.
        self.outgoing_duplicate_suppress_limit = int(cfg.get("outgoing_duplicate_suppress_limit", 3))

        # Field fix (2026-09-19 evening session): how much more evidence a
        # RECENTLY HEALTHY path needs before a stale-path reset discards it --
        # see record_direct_send_result for the incident.
        self.direct_path_healthy_window_s = float(cfg.get("direct_path_healthy_window", 120.0))
        self.direct_path_healthy_recent_successes = int(
            cfg.get("direct_path_healthy_recent_successes", 5)
        )
        self.direct_path_healthy_patience_multiplier = float(
            cfg.get("direct_path_healthy_patience_multiplier", 2.5)
        )

    def _configure_path_discovery(self, cfg):
        # docs/path_discovery_spec.md's "Retry and backoff structure" --
        # a quick-retry burst (each attempt already naturally spaced by
        # its own request/response wait, no additional artificial delay
        # layered on top), then per-target exponential backoff.
        self.path_discovery_quick_attempts = int(cfg.get("path_discovery_quick_attempts", 2))
        self.path_discovery_base_cooldown_s = float(cfg.get("path_discovery_base_cooldown", 20.0))
        self.path_discovery_max_cooldown_s = float(cfg.get("path_discovery_max_cooldown", 900.0))
        self.path_discovery_backoff_factor = float(cfg.get("path_discovery_backoff_factor", 1.8))
        # Alpha 0.1.6 (item 1, 2026-09-22): path selection by measured
        # reliability, replacing alpha 0.1.5's shorter-path adoption. Every
        # route this node learns to a peer -- the discovered path, the
        # reverse of each distinct flood copy the peer's own floods took,
        # the zero-hop option when the peer has been heard directly, and the
        # path the peer itself reports in every "Q" v5 frame -- is a
        # candidate on a per-peer scoreboard (`_PathBoard`, `_paths.py`).
        # Each candidate is scored as expected transmissions per delivered
        # frame times (hops + 1): airtime per delivered byte, lower is
        # better, from its delivery rate over its last PATH_SAMPLES_KEPT
        # sends (older ones weighted down, nothing older than
        # PATH_SAMPLE_WINDOW_S counted). An untried path scores with the
        # optimistic prior PATH_PRIOR_OPTIMISTIC, except a zero-hop candidate
        # whose last direct frame from the peer was below `path_weak_snr_db`
        # (the owner's repeater assumption: a two-hop path via a well-placed
        # repeater beats a weak direct one), which scores with
        # PATH_PRIOR_WEAK. The current path is kept while it delivers; after
        # `path_switch_after_misses` consecutive missed sends the next real
        # packet goes on the best-scoring alternative (a trial, no dedicated
        # probe); the switch is made for good only when the alternative's
        # score beats the current one by `path_switch_margin`, and a path
        # switched away from is not switched back to for
        # `path_switch_cooldown` seconds. Discovery runs only when every
        # candidate has missed its last `path_switch_after_misses` sends
        # (and a candidate whose last miss is older than the cooldown is
        # tried again). The field (2026-09-21 evening, 22:00-22:35): the
        # shortest-in-window rule adopted a 504 s old zero-hop route over a
        # one-hop path confirmed 2 s earlier, missed twice, reset, and the
        # desktop then sat on a two-hop path for 32 minutes while the
        # laptop reached it in one, because the one-hop route was never
        # re-tried without a fresh flood. The old key `path_adopt_enabled`
        # is accepted as an alias of `path_selection_enabled`.
        legacy = cfg.get("path_adopt_enabled")
        self.path_selection_enabled = _cfg_bool(cfg.get("path_selection_enabled", "yes" if legacy is None else legacy))
        self.path_weak_snr_db = float(cfg.get("path_weak_snr_db", 3.0))
        # Alpha 0.1.9 (item 4): 2 -> 4 because the unit changed. This
        # counts consecutive missed ATTEMPTS now, not missed sends, and a
        # missed send is exactly `direct_send_attempts` (2) consecutive
        # missed attempts -- so 4 is the same patience 2 used to buy, while
        # a send with a larger budget (a four-attempt handshake, a pass-1
        # finish) finally costs what it spends. See
        # `_note_path_attempt_result` for the field evidence and for why
        # the delivery-rate samples stay per send.
        self.path_switch_after_misses = max(1, int(cfg.get("path_switch_after_misses", 4)))
        self.path_switch_margin = max(0.0, float(cfg.get("path_switch_margin", 0.25)))
        self.path_switch_cooldown_s = max(0.0, float(cfg.get("path_switch_cooldown", 120.0)))

        # Stale cached-DIRECT-path detection (§8). Built and unit-tested in
        # Milestone 4; since Milestone 5 every live DIRECT send path feeds it
        # through record_direct_send_result() (_send_direct_with_attempts,
        # the raw sender's reconcile-query evidence, _send_direct_supplement).
        self.direct_path_reset_threshold = int(cfg.get("direct_path_reset_threshold", 3))
        self.direct_path_reset_rssi_floor = float(cfg.get("direct_path_reset_rssi_floor", -105.0))
        self.direct_path_reset_patience_multiplier = float(
            cfg.get("direct_path_reset_patience_multiplier", 3.0)
        )
        # User-requested fix (2026-09-15, post-alpha-0.1.0 2-hop field test):
        # `direct_path_reset_threshold`'s default at the time (2; 3 since
        # ef57809) meant just two
        # consecutive full-timeout DIRECT failures discard a cached path
        # and force a fresh `discover_path()` burst (up to
        # `path_discovery_quick_attempts` real over-the-air round trips)
        # on the very next send -- with no floor on how recently that path
        # was itself successfully confirmed. When several messages fail
        # close together for a reason that has nothing to do with the path
        # itself (shared-radio congestion, several concurrent sends
        # queued behind `_direct_exchange_lock`, a repeater mid-relay --
        # exactly the real 2-hop field test scenario), this can re-discover
        # a path that was only just confirmed seconds ago and hasn't
        # plausibly gone stale, adding real path-discovery airtime on top
        # of an already-congested channel -- a self-inflicted feedback
        # loop, not a genuine stale-path recovery. A path resolved more
        # recently than this floor is trusted regardless of how many
        # failures have piled up since; the failure count is NOT reset by
        # this skip (see record_direct_send_result), so a path that's
        # genuinely gone bad is still torn down and rediscovered once it's
        # old enough for that to be plausible, just not before.
        self.direct_path_reset_min_age_s = float(cfg.get("direct_path_reset_min_age", 60.0))

        # Live, periodic contact-table read -- reliability_engine_design.md
        # §2's "data-source gap" fix: never served from a cached/inherited
        # value. Feeds path discovery's own ensure_contacts() precondition
        # now; Milestone 5 is expected to also feed this into the
        # zero-hop/known-N-hop spacing tiers _fragment_spacing_range()
        # already implements but has no live data source for yet.
        self.contact_refresh_interval_s = float(cfg.get("contact_refresh_interval", 30.0))

        # Milestone 4 granted base-telemetry permission to every known
        # contact by default, flagged there as a placeholder: docs/
        # path_discovery_spec.md recommends granting only to peers
        # confirmed via this interface's own bind-frame protocol, which
        # didn't exist until Milestone 5. Now that it does
        # (_refresh_contacts_and_grant_telemetry grants bind-confirmed
        # peers unconditionally), this defaults to "no" -- set to "yes"
        # as an explicit escape hatch back to the old open-to-every-
        # contact behavior (exactly as open as any public MeshCore
        # channel already is), not because the bind-gated grant above
        # ever needs it to function.
        self.telemetry_grant_all_contacts = _cfg_bool(cfg.get("telemetry_grant_all_contacts", "no"))

        # Phase 1 (2026-09-20): answer an RNS path re-request from the
        # announce this interface already delivered. On a non-transport
        # node a pending Link that closes without activating makes RNS
        # `expire_path` the destination and request the path again
        # (`Transport.jobs`, pending-links check); the answering node
        # replies from ITS path table with the same cached announce bytes
        # (`Transport.path_request`), and the requester accepts them
        # because the destination is no longer in its table. The laptop
        # (2 hops, 2026-09-20) received the identical 235-byte announce for
        # one destination six times in an hour, each a 2-3 fragment raw
        # send plus reports at two hops, each preceded by a 2-hop DIRECT
        # request; the desktop answered 7 and suppressed 5 more as
        # duplicates in flight. Every ANNOUNCE handed to RNS from a bound
        # peer is cached (bytes as received, LRU, `announce_cache_ttl`);
        # a path request whose target is cached, whose source peer is still
        # bound and not in discovery backoff, and which has not been
        # answered locally within `path_request_local_answer_min_interval`
        # is answered by re-injecting the cached announce (context
        # rewritten to PATH_RESPONSE so a transport node does not
        # re-flood it) and is NOT transmitted. The local answer verifies
        # nothing: the next request for the same destination inside the
        # interval goes over the air, which is how a genuinely dead
        # destination is re-verified. 0 disables either.
        #
        # Alpha 0.1.9 (item 3): the TTL was 3600 s, which is shorter than
        # a field day. The 2026-09-23 session had two stops two hours
        # apart; every entry cached at the first stop had expired by the
        # second, so eight announces went over the air again at two hops
        # (the laptop's 11:28 capture, `direct_raw_multifragment`) for
        # destinations it had already held. A week matches what RNS
        # itself keeps: a path learned over a MODE_FULL interface expires
        # at `Transport.PATHFINDER_E` (60*60*24*7) and the path table is
        # culled at `Transport.DESTINATION_TIMEOUT` (also a week) --
        # `RNS/Transport.py`, verified 2026-09-23 -- so the cache now
        # expires exactly when the answering node's OWN record of the
        # same announce would, and never later. Serving a week-old entry
        # is bounded by liveness rather than by age: the entry answers
        # only while the peer that delivered it is still bound and out of
        # path-discovery backoff (`_answer_path_request_locally`), and
        # one on-air verification per destination per
        # `path_request_local_answer_min_interval` still runs. The cache
        # is LRU-bounded at ANNOUNCE_CACHE_MAX_KEYS (256), so the longer
        # TTL costs bounded memory and a bounded file.
        self.announce_cache_ttl_s = float(cfg.get("announce_cache_ttl", 604800.0))
        # Alpha 0.1.8 (item 4): where the cache is persisted. Empty means
        # `smci_announces.json` beside the peer cache under
        # RNS.Reticulum.storagepath; `announce_cache_ttl = 0` disables
        # both the cache and its file.
        self.announce_cache_path = str(cfg.get("announce_cache_path", "") or "")
        # Alpha 0.1.9 (item 3): 120 s put six verification requests on the
        # air for three destinations in the 3 1/2 minutes between
        # 11:40:51 and 11:44:16 of the 2026-09-23 two-hop stop, each one a
        # relayed DIRECT request answered with a multi-fragment announce
        # window. Ten minutes keeps the re-verification of a genuinely
        # dead destination (0.1.6's purpose for the rule) while costing
        # one request per destination per interval instead of five.
        self.path_request_local_answer_min_interval_s = float(cfg.get("path_request_local_answer_min_interval", 600.0))

    def _configure_peer_discovery(self, cfg):
        # Master on/off switch for the entire bind-frame subsystem (both
        # sending this node's own REQUEST/RESPONSE and answering others').
        # Default on -- peer_discovery_design.md §4's "must not be
        # skippable" rule applies to *cached state* gating the bootstrap
        # REQUEST, not to this being an explicit, deliberate operator/test
        # override that exists independently of any cache. Tests for
        # Milestones 1-4 behavior that don't care about peer discovery set
        # this to "no" so a fake meshcore connection with no channel
        # traffic expectations beyond those milestones' own isn't also
        # asked to field an unrelated bootstrap bind-frame send.
        self.peer_discovery_enabled = _cfg_bool(cfg.get("peer_discovery_enabled", "yes"))

        # docs/peer_discovery_design.md's bind-frame protocol and routing
        # tables. Whether THIS node advertises router capability
        # (has_upstream_rns, §2) is a deployment fact -- whether it's also
        # bridging to the wider Reticulum network via another interface --
        # that this interface cannot infer on its own; defaults to edge
        # (conservative: an edge peer never gets DIRECT-nudged for path
        # requests it structurally can't answer, per routing_decisions.md).
        self.declares_upstream_rns = _cfg_bool(cfg.get("declares_upstream_rns", "no"))

        # Per-responder RESPONSE jitter (§3) -- collision/half-duplex-deaf-
        # repeater spacing, not suppression (every well-formed REQUEST still
        # gets answered). And a separate, much longer global minimum
        # re-response interval -- the real suppression mechanism, since a
        # peer that already heard this node's capability gains nothing from
        # hearing it again. Both "reasoned, not measured" per the doc's own
        # flag -- worth field-validating once there's code to test.
        self.bind_response_jitter_min_s = float(cfg.get("bind_response_jitter_min", 10.0))
        self.bind_response_jitter_max_s = float(cfg.get("bind_response_jitter_max", 30.0))
        self.bind_response_min_interval_s = float(cfg.get("bind_response_min_interval", 300.0))

        # Bootstrap REQUEST: always sent once, unconditionally, at process
        # start (§4's "must not be skippable" rule -- the old design's own
        # regression). peer_discovery_target_peers/_rerequest_interval
        # govern only the *optional* slow repeat while below that target;
        # never whether the first REQUEST happens at all.
        self.peer_discovery_target_peers = int(cfg.get("peer_discovery_target_peers", 3))
        self.peer_discovery_rerequest_interval_s = float(
            cfg.get("peer_discovery_rerequest_interval", 1800.0)
        )
        # Simulation finding (2026-09-19, calibrated 3-repeater bring-up):
        # a single startup REQUEST through several lossy hops is a coin
        # flip, and with the next one 30 minutes out a fresh pairing can
        # sit unbound for that long. While still below the target peer
        # count the repeat now starts here and doubles each round up to
        # peer_discovery_rerequest_interval -- 60, 120, 240 ... 1800s --
        # so a first meeting recovers in a minute or two at the cost of a
        # few small CHANNEL frames, and a lone node still quiets down to
        # the old rate.
        self.peer_discovery_rerequest_initial_s = float(cfg.get("peer_discovery_rerequest_initial", 60.0))

        # Peer expiry (§6) -- a day default, matching the old design's own
        # with no specific field evidence for a different number. Sweep
        # interval is an internal granularity choice (like
        # REASSEMBLY_CLEANUP_INTERVAL_S), generous relative to the TTL it
        # enforces.
        self.peer_ttl_s = float(cfg.get("peer_ttl", 86400.0))
        self.peer_ttl_sweep_interval_s = float(cfg.get("peer_ttl_sweep_interval", 300.0))

        # PROOF-correlation table TTL (§7) -- long enough to cover a
        # realistic PROOF round trip, short enough the table doesn't grow
        # to track every packet ever delivered. The old design used 120s;
        # kept as the starting point, no new field evidence for a different
        # number.
        self.proof_correlation_ttl_s = float(cfg.get("proof_correlation_ttl", 120.0))

        # Optional override for the peer-cache JSON file's path -- defaults
        # to a file under RNS.Reticulum.storagepath (verified populated
        # once a real RNS.Reticulum() instance exists) when left unset.
        self.peer_cache_path = cfg.get("peer_cache_path", None) or None

        # docs/routing_decisions.md's path-request DIRECT-supplement cap --
        # "a small number (e.g. the most-recently-confirmed few)", not
        # unconditionally every known router as the router count grows.
        self.path_request_direct_supplement_cap = int(cfg.get("path_request_direct_supplement_cap", 2))

        # Milestone 6's DIRECT-bootstrap-supplement cap (the fix for
        # peer_discovery_design.md §7's own bootstrap gap -- see
        # _select_bootstrap_supplement_targets) -- same reasoning as the
        # path-request cap above, kept as a separate knob since the two
        # mechanisms have different target-selection rules (this one
        # isn't filtered by router capability).
        self.bootstrap_direct_supplement_cap = int(cfg.get("bootstrap_direct_supplement_cap", 2))

        # Floor under a DIRECT send's ACK-wait timeout, in case a firmware
        # reply's own suggested_timeout is missing or unrealistically small
        # -- this interface's own safety margin, not a firmware constant.
        self.direct_ack_min_timeout_s = float(cfg.get("direct_ack_min_timeout", 5.0))

        # Milestone 6 (docs/reliability_engine_design.md §4): the outer
        # multi-attempt loop for a single DIRECT message -- mirrors, not
        # replaces, the firmware's own per-attempt ACK/content-attempt
        # mechanism; this engine decides how many times to ask the
        # firmware to try. Applies uniformly to a bare single-message
        # DIRECT send and, per-fragment, to each fragment of a DIRECT-
        # fragmented send.
        #
        # User-requested fix (2026-09-16, following the priority-lock
        # change above, same real 1-hop repeater field data): default
        # lowered from 3 to 2. At ~50% measured per-attempt loss, every
        # fragment that exhausts its own attempt budget without success
        # still holds `_direct_exchange_lock` (now priority-aware, but
        # still one radio) for the full cost of each failed attempt --
        # ack_timeout plus the post-send listen window -- before anything
        # else queued behind it gets a turn. A fragment that's going to
        # need more than 2 tries under these conditions isn't meaningfully
        # more likely to succeed on a 3rd attempt than to eventually get
        # picked up by `_send_direct_fragmented`'s own existing pass-1 re-
        # drive (a fresh attempt budget, not a continuation of a failing
        # one) or, above this interface entirely, RNS's own Resource-
        # transfer layer re-requesting specifically-missing parts once a
        # transfer stalls -- both already exist and have equal or better
        # information about what's actually still missing than blindly
        # spending a 3rd attempt on the same fragment does. Lowering this
        # trades a small amount of per-fragment persistence for freeing
        # the shared radio sooner under exactly the lossy conditions where
        # that trade matters -- still a flat, simple value, not made
        # adaptive to measured loss (that's a real further refinement,
        # deliberately not done here without more field data to justify
        # it).
        self.direct_send_attempts = int(cfg.get("direct_send_attempts", 2))

        # User-requested fix (2026-09-16), following the confidence
        # discussion above the retry-budget cut: a LINK_REQUEST/PROOF-
        # class exchange (PRIORITY_HANDSHAKE) is the worst candidate for
        # the same reduced budget ordinary DATA just got. Losing a DATA
        # fragment is comparatively cheap -- `_send_direct_fragmented`'s
        # own pass-1 re-drive, or RNS's own Resource-layer recovery,
        # already exist to pick it up. Losing a Link handshake is not
        # cheap: RNS's own Transport.py confirms (source-checked the same
        # day) that a failed local-client Link attempt tears the path
        # down and forces a full path rediscovery -- itself more airtime,
        # making the *next* handshake attempt less likely to succeed too.
        # That's a self-reinforcing loop a cheap handshake budget makes
        # worse, not better. A separate, larger budget for handshake-
        # class exchanges specifically targets preventing that cascade,
        # while the lower ordinary-DATA budget above still frees the
        # (now priority-aware) shared radio sooner for everything else.
        # See _send_direct_with_attempts's own docstring for exactly how
        # the two budgets are picked between.
        self.direct_send_attempts_handshake = int(cfg.get("direct_send_attempts_handshake", 4))

        # User-requested fix (2026-09-15), generalized the same day after
        # a second real 2-hop field test, then split by outcome the day
        # after that (2026-09-16) once a real zero-hop NomadNet session
        # showed the generalized version's actual cost: a DIRECT
        # send+ACK-wait used to release _direct_exchange_lock the instant
        # it resolved (ACKed or not), letting the very next contender --
        # a retry of the same fragment, the next fragment, or a completely
        # different queued message -- key the radio again immediately.
        # Fine if nothing else was going on, but if the miss was a
        # collision (a peer transmitting into us at the exact moment we
        # sent, or vice versa -- this is a shared half-duplex radio, and a
        # real repeater adds its own settling time on top), going again
        # instantly just repeats the same collision window, and does so
        # for every message queued behind it too.
        #
        # Applying one flat 0-5s range to *every* attempt regardless of
        # outcome (2026-09-15's generalization) turned out to have a real
        # cost the zero-hop field session's own packet capture made
        # obvious: 100% of DIRECT attempts were ACKed (as expected --
        # zero-hop, nothing to collide with), yet every single one still
        # paid an average ~2.5s tax before the lock released, and because
        # NomadNet's page transfer spawns many small RESOURCE-related
        # packets that all queue up behind the same lock,
        # _direct_exchange_queue_depth reached 14 with individual attempts
        # waiting up to 48s just for their own turn -- none of it bought
        # any real collision protection, since nothing was colliding.
        # `direct_post_send_listen_min_s`/`max_s` (0-5s, unchanged) now
        # applies ONLY when the attempt got no ACK -- the one case where
        # "something might have collided, don't retry into it instantly"
        # is actually a live hypothesis, independent of this node's own
        # queue depth (the cause could be entirely external). A
        # successful attempt -- which is itself real evidence the channel
        # was clear for this exchange -- instead draws from the much
        # smaller `direct_post_send_listen_success_min_s`/`max_s` (0-0.5s
        # default): still genuinely random every time (never a fixed
        # value, deliberately, so this can't settle into a lockstep
        # pattern with anything else on the channel), still real spacing
        # between successive different messages queued behind the lock,
        # just not the same "assume something might be wrong" cost a
        # clean ACK gives no reason to pay. Both remain simple, flat
        # ranges for now -- "we can tune this later" still applies. See
        # _send_direct_frame_and_wait_for_ack's own docstring for exactly
        # where each fires.
        #
        # 0.3-3 s -> 0.2-1 s (2026-09-20, dead-wait trims): the post-miss
        # listen averaged 1.7 s on 598 field misses -- ~1000 s of lock time
        # across the three 2026-09-19 sessions -- while the evening audit
        # measured frame overlap between the two nodes at 8.6% against 6.9%
        # expected by chance (loss, not contention, explains 53% of misses),
        # and the firmware's own listen-before-talk (Dispatcher::checkSend
        # defers while the radio reports a frame in progress) already keeps
        # the retry out of an audible frame. Still random, per the user's
        # standing instruction; just a smaller range.
        self.direct_post_send_listen_min_s = float(cfg.get("direct_post_send_listen_min", 0.2))
        self.direct_post_send_listen_max_s = float(cfg.get("direct_post_send_listen_max", 1.0))
        self.direct_post_send_listen_success_min_s = float(cfg.get("direct_post_send_listen_success_min", 0.0))
        self.direct_post_send_listen_success_max_s = float(cfg.get("direct_post_send_listen_success_max", 0.4))

        # The routed-mode ACK-wait ceiling §4 specifies (scaled off the
        # firmware's own hop-aware suggested_timeout, capped here). This
        # design's own routing rule (routing_decisions.md) never issues a
        # DIRECT send without already believing a resolved path exists --
        # every DIRECT send this interface makes is therefore always in
        # the "routed" regime from its own point of view, so only this
        # ceiling is used; the doc's *flood*-mode ceiling
        # (`direct_ack_timeout_max_s`, 10s) describes a regime this
        # interface's own dispatcher structurally never enters (it falls
        # back to CHANNEL broadcast instead of ever flooding a DIRECT
        # send with no resolved path), so it's deliberately not exposed
        # as a config value here -- there would be no code path that
        # reads it.
        self.direct_ack_timeout_routed_max_s = float(cfg.get("direct_ack_timeout_routed_max", 45.0))

        # Field fix (2026-09-19 evening session, multi-agent capture audit):
        # a hop-aware ceiling on the ACK wait, well below the absolute
        # `direct_ack_timeout_routed_max_s` ceiling above. Evidence from that
        # session (1080 attempts, 0-2 hops, both nodes): the LARGEST ACK
        # latency that ever actually arrived was 8.15s (p99 5.82s; per hop:
        # 0 hops max 3.00s, 1 hop 6.06s, 2 hops 8.15s), while the timeouts
        # the firmware's own suggestion produced ran to 28.0s (median 8.0s,
        # p90 16.2s). The interface spent 2510s -- 20% of the session -- in
        # ACK waits that were never going to be answered, holding
        # `_direct_exchange_lock` the whole time, while the radio itself was
        # only ~13% busy: the binding constraint on this interface is not
        # airtime or collisions, it is this serialised dead time. Replaying
        # the session, `base + per_hop x hops` = 8 + 4h would have cut that
        # to 2121s (-15%) while cutting off ZERO of the 559 ACKs that did
        # arrive. Kept proportional to path length rather than flat so a
        # deeper path still gets the time it genuinely needs.
        #
        # Tightened 2026-09-20 (dead-wait trims, module docstring entry of that
        # date) to 5 + 3h: re-derived over all three 2026-09-19 sessions
        # (2670 ACKs), the largest ACK that ever arrived was 3.82 / 6.06 /
        # 8.15 / 7.25 s at 0 / 1 / 2 / 3 hops, so 5 / 8 / 11 / 14 s still
        # cuts off ZERO of them (31-93% above the per-hop maximum) and, in
        # replay, saves a further 824 + 635 + 171 s of dead lock time at
        # 1-3 hops over 8 + 4h. `direct_ack_min_timeout` (5 s) is the hop-0
        # floor, so hop 0 is unchanged. Tuned for SF7/BW62.5/CR8 like every
        # absolute second in this file.
        self.direct_ack_timeout_base_s = float(cfg.get("direct_ack_timeout_base", 5.0))
        self.direct_ack_timeout_per_hop_s = float(cfg.get("direct_ack_timeout_per_hop", 3.0))

        # Step 2 of "lessen our reliance on arbitrary wait times"
        # (2026-09-18, see module docstring): measured ACK round-trip
        # time, per peer, driving the ACK-wait timeout. Until now the
        # timeout was the firmware's own hop-count-derived guess
        # (`suggested_timeout` x1.2, floored at direct_ack_min_timeout_s,
        # capped at direct_ack_timeout_routed_max_s) and was never
        # compared against reality; step 1's zero-hop hardware run showed
        # a 5.2s timeout guarding a 0.7-1.1s real RTT. Since a missed ACK
        # holds `_direct_exchange_lock` for the whole timeout -- and the
        # 2026-09-16 1-hop capture's mean 11s/max 49s lock waits were
        # almost entirely other sends' full timeouts -- a timeout sized to
        # the *measured* RTT is the single largest reduction in wasted
        # lock time available without touching airtime. Kept deliberately
        # conservative: Jacobson/Karels smoothing (srtt + 4*rttvar, then
        # `direct_ack_rtt_timeout_multiplier` on top), only after
        # `direct_ack_rtt_min_samples` real ACKs from that peer, never
        # below `direct_ack_rtt_min_timeout_s`, never ABOVE the firmware-
        # derived value it replaces (so the worst case is exactly today's
        # behaviour), and Karn-style invalidated on the first miss so a
        # link that got slower falls straight back to the firmware guess
        # until fresh samples exist. Stats are also dropped whenever the
        # peer's path changes (`_reset_stale_path`, a fresh
        # `discover_path` result) -- an RTT measured over one path says
        # nothing about another.
        self.direct_ack_rtt_adaptive_enabled = _cfg_bool(cfg.get("direct_ack_rtt_adaptive_enabled", "yes"))
        self.direct_ack_rtt_min_samples = int(cfg.get("direct_ack_rtt_min_samples", 3))
        self.direct_ack_rtt_timeout_multiplier = float(cfg.get("direct_ack_rtt_timeout_multiplier", 2.0))
        self.direct_ack_rtt_min_timeout_s = float(cfg.get("direct_ack_rtt_min_timeout", 3.0))
        # Field-diagnosed (2026-09-18 drive-home, 3 hops): a miss under a
        # measured timeout used to discard the estimate outright (Karn), so
        # the very next attempt paid the firmware's full 28s even on a path
        # that was merely slow. The dead-first-hop case is now caught by
        # the hop-1 abort; for a slow-but-alive path each consecutive miss
        # instead multiplies the measured timeout by this factor (RFC 6298's
        # RTO backoff), still never above the firmware value, and the next
        # real ACK resets it. <= 1 restores the discard behaviour.
        self.direct_ack_rtt_miss_backoff = float(cfg.get("direct_ack_rtt_miss_backoff", 2.0))

    def _configure_observability(self, cfg):
        # Per-interface debug logging, independent of RNS core's global
        # [logging] loglevel. RNS.log() gates every message (ours and RNS
        # core's own) on one global level, so raising it to DEBUG to see
        # this interface's own diagnostics would also enable RNS core's
        # own debug firehose. Messages logged via self._debug() below are
        # emitted at LOG_INFO, gated only by this flag.
        self.debug_logs = str(cfg.get("debug_level", "info")).strip().lower() == "debug"

        # How often the periodic structured stats snapshot (_stats_loop)
        # is logged. Per `docs/interface_architecture.md`'s observability
        # requirements, this mechanism exists from the first milestone
        # that connects to anything, even though M0 has little of
        # substance to report yet -- later milestones (queue depth,
        # reassembly bucket counts, per-peer state) extend the same
        # snapshot rather than inventing a second one.
        self.stats_interval_s = float(cfg.get("stats_interval", 60.0))

        # User-requested packet capture (2026-09-15): off by default,
        # every in/out RNS packet appended as one JSON line to a file
        # under this directory when enabled -- see _capture_event's own
        # docstring for the record format and _async_setup for where the
        # file actually gets opened (once this node is online and its
        # storage path is known).
        self.packet_capture_enabled = _cfg_bool(cfg.get("packet_capture_enabled", "no"))
        self.packet_capture_dir = cfg.get("packet_capture_dir", None)
        # Alpha 0.1.5 (item 7, 2026-09-21): the capture file carries a node
        # label so two machines' captures of one session tell apart at a
        # glance -- the MeshCore node name from SELF_INFO by default (the
        # field's `afipc` and `a`; the desktop's 2026-09-21 file had to be
        # renamed by hand), or this value. Empty and no node name gives the
        # pre-0.1.5 filename. `_capture_filename` is the pure rule.
        self.packet_capture_label = str(cfg.get("packet_capture_label", "") or "").strip()
        # Alpha 0.1.5 (item 8, 2026-09-21): airtime estimator calibration,
        # instrumentation only. Firmware v1.17.1's CMD_GET_STATS (56,
        # companion protocol v8+; `MyMesh.cpp`) returns the radio's measured
        # transmit time -- `Dispatcher::checkSend` adds the wall-clock
        # duration of every completed send to `total_air_time`; reported as
        # whole seconds -- and per-type packet counts; the library exposes
        # them as `get_stats_radio()` (tx_air_secs, rx_air_secs, noise floor,
        # last RSSI/SNR) and `get_stats_packets()` (recv, sent, flood/direct
        # tx/rx). The interface reads both at start, at stop and every
        # `radio_stats_interval` seconds (0 = start and stop only) into a
        # `radio_stats` capture record next to its own summed airtime
        # estimate and frame count since start, so a field summary can
        # compare the estimator against the radio. The estimator itself is
        # unchanged. A firmware without the command (an ERROR reply) is
        # logged once and the poll stops.
        self.radio_stats_interval_s = float(cfg.get("radio_stats_interval", 300.0))

        # User-requested (2026-09-18, "lessen our reliance on arbitrary
        # wait times" -- step 1 of that plan, see module docstring): tap
        # the companion firmware's own raw-RX log feed. `MyMesh::logRxRaw`
        # (referenceprojects/MeshCore-main/examples/companion_radio/
        # MyMesh.cpp) pushes EVERY packet the radio decodes -- addressed to
        # this node or not: other peers' DIRECT frames, flood repeats,
        # ACKs in transit, a repeater's echo of this node's own frame --
        # to the host as PUSH_CODE_LOG_RX_DATA, unconditionally whenever
        # the serial link is up (confirmed against firmware source: no
        # pref gates it), and the installed `meshcore` library (2.3.9.1,
        # reader.py's LOG_DATA branch) parses it into
        # `EventType.RX_LOG_DATA` with SNR/RSSI/route type/payload type/
        # path. That is the closest thing to a carrier-sense signal this
        # interface can get without a firmware fork (hardware CAD and the
        # RSSI interference threshold are both hard-coded off in the
        # companion build), and strictly more information than the
        # "a DIRECT frame decoded for us" proxy `_wait_for_incoming_quiet`
        # keys off today. THIS STEP IS OBSERVE-ONLY: counters in the
        # [STATS] snapshot and one `rx_log` record per overheard packet
        # in the packet capture, so real field captures can establish the
        # correlations (repeater echoes of our own frames, ACK sightings,
        # burst structure of a peer's fragmented send) before any timing
        # decision is allowed to depend on this feed. No routing/timing
        # logic reads it yet. Default on since subscribing costs nothing
        # over the air; configurable off in case a busy mesh's log volume
        # is unwanted on a slow serial/BLE link.
        self.rx_log_observe_enabled = _cfg_bool(cfg.get("rx_log_observe_enabled", "yes"))

    def _validate_direct_timing_budget(self) -> None:
        """Field-diagnosed fix (2026-09-18, see module docstring's
        2026-09-18 entry for the full incident this responds to): a
        real field test broke multi-hop delivery entirely after several
        individually-reasonable DIRECT timing knobs -- spread across
        `_configure_fragmentation`, `_configure_retry`, `_configure_
        transport`, and `_configure_peer_discovery`, each tuned in
        isolation in a separate field-driven fix -- combined to let a
        single fragment's worst-case retry cost approach or exceed
        `reassembly_idle_timeout_s`, the fixed clock the *receiver* is
        racing them against. Nothing before this method ever checked
        that those two sides of the same budget were still compatible
        after an operator (or a future field fix) changed one of them.

        The question it answers is deliberately narrow and framed to stay
        actionable in both directions: **how many clock-racing send
        attempts actually fit inside the receiver's idle window?** The
        receiver resets a bucket's clock only when a fragment genuinely
        *arrives* (`_ReassemblyBucket.last_progress`), so the relevant
        comparison is one attempt's worst-case cost against
        `reassembly_idle_timeout_s`. If fewer than `direct_send_attempts`
        of them fit, the receiver can evict a bucket while the sender is
        still legitimately working through that same fragment's own
        configured attempt budget for the first time -- the retry logic
        and the patience it's spending are then incoherent with each other
        by construction, regardless of link quality.

        An attempt that races the clock costs `direct_ack_timeout_routed_
        max_s` (the ACK wait) plus `direct_post_send_listen_max_s` (the
        post-send listen window, held under the same lock).
        `incoming_quiet_defer_max_wait_s` is deliberately NOT in that
        figure: post-2026-09-18 only a message's genuinely-first
        transmission pays it, and that one happens *before* the receiver
        has a bucket or a clock at all (see `_pre_transmit_gate`'s
        `skip_quiet_defer`). It is still reported in the warning, because
        re-broadening the quiet-defer trigger is exactly what made this
        ratio fail in the first place.

        Checked against the real incident: with that day's knobs and the
        pre-fix behaviour of taxing *every* attempt with the quiet defer,
        an attempt cost 63s against a 120s window -- 1.90 attempts, below
        the budget of 2, so this would have fired at startup. With the
        same knobs and the fix in place an attempt costs 48s -> 2.50
        attempts, which passes.

        Deliberately excludes costs that can't be bounded from config
        alone: `_direct_exchange_lock` queueing delay (depends on how many
        *other* messages are competing for the one radio), this message's
        own other fragments being attempted in between, and
        `_throttle_for_duty_cycle`. So a config that fails this check is
        confirmed incoherent; one that passes isn't guaranteed safe under
        heavy contention, just no longer broken by construction. Warns
        only -- an operator's explicit config is never silently
        overridden."""
        # Step 4 (2026-09-18): with rx_log_holds_enabled, one attempt can
        # additionally wait up to rx_log_hold_max_s before transmitting and
        # the post-miss hold is capped at the same value instead of
        # direct_post_send_listen_max_s -- both counted here.
        hold_cap_s = self.rx_log_hold_max_s if self.rx_log_holds_enabled else 0.0
        # Code review (2026-09-18): with holds on, the post-miss wait is
        # _post_miss_hold_s, capped at rx_log_hold_max_s -- it REPLACES the
        # flat listen range rather than adding to it (see _send_direct_
        # frame_and_wait_for_ack), so that is the term counted here.
        post_miss_cap_s = hold_cap_s if self.rx_log_holds_enabled else self.direct_post_send_listen_max_s
        clock_racing_attempt_s = self.direct_ack_timeout_routed_max_s + hold_cap_s + post_miss_cap_s
        if clock_racing_attempt_s <= 0:
            return
        attempts_that_fit = self.reassembly_idle_timeout_s / clock_racing_attempt_s

        # Audit fix (2026-09-19): this compared only against
        # `direct_send_attempts` (2), so on the shipped defaults
        # 120/48 = 2.5 fits and it stayed silent -- while the budgets that
        # actually apply to a fragment racing the receiver's clock are
        # larger: `direct_send_attempts_handshake` (4) for a handshake-class
        # exchange, and `direct_fragment_finish_attempts` (4) for a pass-1
        # finish re-drive. 4 x 48s = 192s against a 120s
        # reassembly_idle_timeout is exactly the incoherence this method
        # exists to catch.
        worst_attempts = max(
            self.direct_send_attempts,
            self.direct_send_attempts_handshake,
            self.direct_fragment_finish_attempts,
        )
        if attempts_that_fit < worst_attempts:
            breakdown = f"direct_ack_timeout_routed_max={self.direct_ack_timeout_routed_max_s:.1f}s"
            if self.rx_log_holds_enabled:
                breakdown += (
                    f" + rx_log_hold_max={hold_cap_s:.1f}s pre-transmit"
                    f" + rx_log_hold_max={hold_cap_s:.1f}s post-miss"
                )
            else:
                breakdown += f" + direct_post_send_listen_max={self.direct_post_send_listen_max_s:.1f}s"
            RNS.log(
                f"{self}: DIRECT timing budget is incoherent with reassembly patience -- one "
                f"send attempt racing the receiver's reassembly clock can cost up to "
                f"{clock_racing_attempt_s:.1f}s ({breakdown}), so only {attempts_that_fit:.2f} "
                f"attempts fit inside reassembly_idle_timeout "
                f"({self.reassembly_idle_timeout_s:.1f}s), fewer than this node's own "
                f"worst-case attempt budget of {worst_attempts} (max of direct_send_attempts="
                f"{self.direct_send_attempts}, direct_send_attempts_handshake="
                f"{self.direct_send_attempts_handshake}, direct_fragment_finish_attempts="
                f"{self.direct_fragment_finish_attempts}). The receiver can evict a "
                f"bucket while the sender is still working through that fragment's first "
                f"attempt budget -- real queueing delay and this message's other fragments only "
                f"make it worse. Fix by lowering direct_ack_timeout_routed_max/"
                f"direct_post_send_listen_max, lowering direct_send_attempts, or raising "
                f"reassembly_idle_timeout to at least "
                f"{clock_racing_attempt_s * worst_attempts:.0f}. "
                f"(incoming_quiet_defer_max_wait="
                f"{self.incoming_quiet_defer_max_wait_s if self.incoming_quiet_defer_enabled else 0:.1f}s "
                f"is excluded above -- only a message's first transmission pays it -- but "
                f"re-broadening that gate's trigger would add it to every attempt here.)",
                RNS.LOG_WARNING,
            )

    def _loop_interval_s(self, interval_s: float, name: str) -> float:
        if interval_s >= self.MIN_LOOP_INTERVAL_S:
            return interval_s
        RNS.log(
            f"{self}: {name}={interval_s} is below the {self.MIN_LOOP_INTERVAL_S:.0f}s floor "
            f"(0 would busy-spin this interface's event loop, not disable the loop) -- "
            f"using {self.MIN_LOOP_INTERVAL_S:.0f}s.",
            RNS.LOG_WARNING,
        )
        return self.MIN_LOOP_INTERVAL_S
