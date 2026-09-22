"""
Pins the shipped default of every configuration attribute each
`_configure_*` method of SmartMeshCoreInterface sets when parsing an EMPTY
config block (2026-09-20).

Why this exists: the unit harness (testscripts/simmesh/harness.py,
FAST_TIMING) overrides many timing defaults so the two-node scenarios finish
in seconds, which means a live interface from `tests._support.SingleNodeCase`
cannot be used to pin a default -- FAST_TIMING overrides make shipped
defaults unpinnable through the live interface. Earlier tests worked around
that one key at a time with a bare `__new__` instance and a single
`_configure_retry({})` call; this file generalises that to every method, so a
drift in ANY default (a wire-format-neutral but field-relevant timing change,
e.g. someone editing a literal in `_configure_retry`) fails a test that names
the key and both values.

Method: one bare instance (`SmartMeshCoreInterface.__new__`, no `__init__`),
each `_configure_*({})` called in constructor order, and the attributes each
call newly set are diffed out of `vars(bare)` and compared against the literal
block below for that method. Every attribute the methods currently set is a
plain scalar or None, so nothing is skipped; if one ever becomes a set it is
compared as a sorted list, and callables/locks/handles are dropped (see
`_snapshot`).

A failure means one of two things:
  * an unintended drift -- restore the literal in the interface; or
  * a deliberate default change -- which must also be recorded in the module
    docstring of Interface/SmartMeshCoreInterface.py, in changelog.md and in
    readme.md, and then re-pinned here. To regenerate the literal blocks run

        python3 tests/test_shipped_defaults.py --dump

    from the repo root: it prints every block as Python literals in the same
    sorted, one-key-per-line layout, ready to paste over the ones below.

The last test asserts `_validate_direct_timing_budget()` stays silent on the
shipped defaults (it only warns via RNS.log, never raises), because the
2026-09-19 entry in the module docstring records the interface once shipped a
set of defaults that warned about their own incoherence at every startup.
"""
import os
import sys
import unittest

if __package__ in (None, ""):
    # Run directly (`python3 tests/test_shipped_defaults.py --dump`): make the
    # repo root importable so `tests._support` resolves as it does under
    # `python3 -m unittest discover -s tests`.
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tests._support import load_interface_module

# Constructor order of the _configure_* calls (SmartMeshCoreInterface.__init__).
# Called in this order on one instance so any method that read an attribute an
# earlier one set would see it -- as of 2026-09-20 none does; `_configure_
# channel` reads only the DEFAULT_CHANNEL_* class attributes.
CONFIGURE_ORDER = (
    "_configure_identity",
    "_configure_transport",
    "_configure_channel",
    "_configure_radio",
    "_configure_fragmentation",
    "_configure_retry",
    "_configure_path_discovery",
    "_configure_peer_discovery",
    "_configure_observability",
)

_SCALAR_TYPES = (bool, int, float, str, type(None), list, tuple, dict)


def _snapshot(module):
    """{method name: {attr: value}} for the attributes each `_configure_*({})`
    newly sets (or changes) on a bare, un-`__init__`ed instance, in
    constructor order. Only JSON-shaped values are kept: scalars, None,
    lists/tuples/dicts as-is, sets as sorted lists; anything else
    (callables, locks, file handles, objects) is left out."""
    cls = module.SmartMeshCoreInterface
    bare = cls.__new__(cls)
    per_method = {}
    for name in CONFIGURE_ORDER:
        before = dict(vars(bare))
        getattr(bare, name)({})
        new = {}
        for key in sorted(vars(bare)):
            value = vars(bare)[key]
            if key in before and before[key] == value:
                continue
            if isinstance(value, (set, frozenset)):
                new[key] = sorted(value)
            elif isinstance(value, _SCALAR_TYPES):
                new[key] = value
        per_method[name] = new
    return bare, per_method


def _dump(per_method):
    for name in CONFIGURE_ORDER:
        block = per_method[name]
        print(f"# {name}: {len(block)} keys")
        print(f"{name.upper()[1:]} = {{")
        for key, value in block.items():
            print(f"    {key!r}: {value!r},")
        print("}\n")


# --------------------------------------------------------------------------
# The shipped defaults, one block per _configure_* method (generated with
# --dump on 2026-09-20 from Interface/SmartMeshCoreInterface.py).
# --------------------------------------------------------------------------

CONFIGURE_IDENTITY = {
    'name': 'Smart MeshCore Interface',
}

CONFIGURE_TRANSPORT = {
    'auto_reconnect': True,
    'baudrate': 115200,
    'bitrate': 80,
    'ble_name': '',
    'command_timeout_s': 15.0,
    'connect_retry_max_s': 60.0,
    'connect_retry_min_s': 5.0,
    'duty_cycle_enabled': True,
    'duty_cycle_estimate_bitrate': 1200,
    'duty_cycle_exempt_handshake': True,
    'duty_cycle_max_fraction': 0.3,
    'duty_cycle_max_fraction_zero_hop': 0.85,
    'duty_cycle_window_s': 60.0,
    'handshake_attempts': 5,
    'handshake_timeout_s': 5.0,
    'host': '127.0.0.1',
    'incoming_quiet_defer_enabled': True,
    'incoming_quiet_defer_max_wait_s': 15.0,
    'incoming_quiet_window_s': 3.0,
    'max_reconnect_attempts': 0,
    'port': '/dev/ttyUSB0',
    'rx_log_hold_flood_factor': 2.5,
    'rx_log_hold_hop_factor': 1.5,
    'rx_log_hold_jitter_max_s': 0.8,
    'rx_log_hold_jitter_min_s': 0.2,
    'rx_log_hold_max_s': 4.0,
    'rx_log_hold_turnaround_s': 0.4,
    'rx_log_holds_enabled': False,
    'serial_noise_warn_per_min': 5,
    'serial_open_settle_s': 2.0,
    'tcp_port': 4403,
    'transport': 'serial',
}

CONFIGURE_CHANNEL = {
    '_using_default_channel_secret': True,
    'channel_idx': 35,
    'channel_name': 'RNSTunnel',
    'channel_secret_hex': 'b99e9b45f61ab4bd4e355cf812711873',
}

CONFIGURE_RADIO = {
    'radio_bw': 0.0,
    'radio_cr': 0,
    'radio_freq': 0.0,
    'radio_sf': 0,
}

CONFIGURE_FRAGMENTATION = {
    'fragment_delay_max_s': 10.0,
    'fragment_delay_min_s': 5.0,
    'fragment_delay_per_hop_max_s': 10.0,
    'fragment_delay_per_hop_min_s': 5.0,
    'fragment_delay_zero_hop_max_s': 1.5,
    'fragment_delay_zero_hop_min_s': 0.5,
    'fragment_order_shuffle': True,
    'reassembly_idle_timeout_coop_s': 180.0,
    'reassembly_idle_timeout_s': 200.0,
    'reassembly_max_keys': 256,
    'whole_packet_dedup_ttl_s': 150.0,
}

CONFIGURE_RETRY = {
    'announce_min_interval_s': 300.0,
    'announce_retransmit_extra': 0,
    'direct_completion_check_enabled': True,
    'direct_completion_check_timeout_max_multihop_s': 18.0,
    'direct_completion_check_timeout_max_s': 15.0,
    'direct_completion_check_timeout_per_hop_s': 2.5,
    'direct_completion_check_timeout_s': 5.0,
    'direct_completion_quiet_base_s': 2.0,
    'direct_completion_quiet_per_hop_s': 3.0,
    'direct_completion_unacked_grace_multihop_s': 10.0,
    'direct_completion_unacked_grace_s': 6.0,
    'direct_fragment_finish_attempts': 4,
    'direct_fragment_pass0_attempts': 1,
    'direct_fragment_reconcile_enabled': True,
    'direct_fragment_resume_enabled': True,
    'direct_fragmented_max_in_flight': 2,
    'direct_hop1_abort_default_s': 8.0,
    'direct_hop1_abort_echo_multiplier': 2.0,
    'direct_hop1_abort_enabled': True,
    'direct_hop1_abort_min_s': 5.0,
    'direct_hop1_abort_min_samples': 3,
    'direct_path_healthy_patience_multiplier': 2.5,
    'direct_path_healthy_recent_successes': 5,
    'direct_path_healthy_window_s': 120.0,
    'direct_raw_burst_queue_ahead': 1,
    'direct_raw_fallback_cooldown_s': 120.0,
    'direct_raw_fallback_strikes': 2,
    'direct_raw_fragments_enabled': True,
    'direct_raw_gap_own_airtime': True,
    'direct_raw_hop_gap_factor': 2.0,
    'direct_raw_incomplete_strikes': 2,
    'direct_raw_parity_enabled': True,
    'direct_raw_parity_min_hops': 1,
    'direct_raw_path_unsupported_ttl_s': 86400.0,
    'direct_raw_payload_cap': 170,
    'direct_raw_query_attempts': 2,
    'direct_raw_reburst_after_unanswered': 2,
    'direct_raw_reconcile_rounds': 3,
    'direct_raw_report_enabled': True,
    'direct_raw_report_wait_base_s': 4.0,
    'direct_raw_report_wait_per_hop_s': 2.5,
    'direct_raw_window_collect_s': 0.75,
    'direct_raw_window_enabled': True,
    'direct_raw_window_max_parts': 6,
    'direct_raw_window_max_rounds': 2,
    'direct_raw_zero_hop_gap_s': 0.15,
    'direct_report_debounce': True,
    'direct_report_hold_during_burst': True,
    'direct_report_noack': True,
    'ordinary_data_bare_retransmit_extra': 1,
    'ordinary_data_link_retransmit_extra': 0,
    'outgoing_duplicate_suppress_limit': 3,
    'outgoing_max_age_s': 120.0,
    'path_req_retransmit_extra': 1,
    'proof_max_age_s': 45.0,
    'proof_fresh_s': 8.0,
    'proof_report_grace_s': 0.25,
    'retransmit_jitter_max_s': 20.0,
    'retransmit_jitter_min_s': 8.0,
}

CONFIGURE_PATH_DISCOVERY = {
    'announce_cache_path': '',
    'announce_cache_ttl_s': 3600.0,
    'contact_refresh_interval_s': 30.0,
    'direct_path_reset_min_age_s': 60.0,
    'direct_path_reset_patience_multiplier': 3.0,
    'direct_path_reset_rssi_floor': -105.0,
    'direct_path_reset_threshold': 3,
    'path_discovery_backoff_factor': 1.8,
    'path_discovery_base_cooldown_s': 20.0,
    'path_discovery_max_cooldown_s': 900.0,
    'path_discovery_quick_attempts': 2,
    'path_request_local_answer_min_interval_s': 120.0,
    'path_selection_enabled': True,
    'path_switch_after_misses': 2,
    'path_switch_cooldown_s': 120.0,
    'path_switch_margin': 0.25,
    'path_weak_snr_db': 3.0,
    'telemetry_grant_all_contacts': False,
}

CONFIGURE_PEER_DISCOVERY = {
    'bind_response_jitter_max_s': 30.0,
    'bind_response_jitter_min_s': 10.0,
    'bind_response_min_interval_s': 300.0,
    'bootstrap_direct_supplement_cap': 2,
    'declares_upstream_rns': False,
    'direct_ack_min_timeout_s': 5.0,
    'direct_ack_rtt_adaptive_enabled': True,
    'direct_ack_rtt_min_samples': 3,
    'direct_ack_rtt_min_timeout_s': 3.0,
    'direct_ack_rtt_miss_backoff': 2.0,
    'direct_ack_rtt_timeout_multiplier': 2.0,
    'direct_ack_timeout_base_s': 5.0,
    'direct_ack_timeout_per_hop_s': 3.0,
    'direct_ack_timeout_routed_max_s': 45.0,
    'direct_post_send_listen_max_s': 1.0,
    'direct_post_send_listen_min_s': 0.2,
    'direct_post_send_listen_success_max_s': 0.4,
    'direct_post_send_listen_success_min_s': 0.0,
    'direct_send_attempts': 2,
    'direct_send_attempts_handshake': 4,
    'path_request_direct_supplement_cap': 2,
    'peer_cache_path': None,
    'peer_discovery_enabled': True,
    'peer_discovery_rerequest_initial_s': 60.0,
    'peer_discovery_rerequest_interval_s': 1800.0,
    'peer_discovery_target_peers': 3,
    'peer_ttl_s': 86400.0,
    'peer_ttl_sweep_interval_s': 300.0,
    'proof_correlation_ttl_s': 120.0,
}

CONFIGURE_OBSERVABILITY = {
    'debug_logs': False,
    'packet_capture_dir': None,
    'packet_capture_enabled': False,
    'packet_capture_label': '',
    'radio_stats_interval_s': 300.0,
    'rx_log_observe_enabled': True,
    'stats_interval_s': 60.0,
}

EXPECTED = {
    "_configure_identity": CONFIGURE_IDENTITY,
    "_configure_transport": CONFIGURE_TRANSPORT,
    "_configure_channel": CONFIGURE_CHANNEL,
    "_configure_radio": CONFIGURE_RADIO,
    "_configure_fragmentation": CONFIGURE_FRAGMENTATION,
    "_configure_retry": CONFIGURE_RETRY,
    "_configure_path_discovery": CONFIGURE_PATH_DISCOVERY,
    "_configure_peer_discovery": CONFIGURE_PEER_DISCOVERY,
    "_configure_observability": CONFIGURE_OBSERVABILITY,
}


class ShippedDefaults(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_interface_module()
        cls.bare, cls.actual = _snapshot(cls.module)

    def _check(self, method):
        expected = EXPECTED[method]
        actual = self.actual[method]
        problems = []
        for key in sorted(set(expected) | set(actual)):
            if key not in actual:
                problems.append(f"  {key}: no longer set (pinned {expected[key]!r})")
            elif key not in expected:
                problems.append(f"  {key}: newly set to {actual[key]!r}, not pinned here")
            elif expected[key] != actual[key] or type(expected[key]) is not type(actual[key]):
                problems.append(
                    f"  {key}: shipped default is now {actual[key]!r}, pinned {expected[key]!r}"
                )
        self.assertFalse(
            problems,
            f"{method}({{}}) defaults drifted from the pinned snapshot -- either restore the "
            f"literal in Interface/SmartMeshCoreInterface.py or, for a deliberate change, record "
            f"it in the module docstring, changelog.md and readme.md and re-pin with "
            f"`python3 tests/test_shipped_defaults.py --dump`:\n" + "\n".join(problems),
        )

    def test_configure_identity_defaults(self):
        self._check("_configure_identity")

    def test_configure_transport_defaults(self):
        self._check("_configure_transport")

    def test_configure_channel_defaults(self):
        self._check("_configure_channel")

    def test_configure_radio_defaults(self):
        self._check("_configure_radio")

    def test_configure_fragmentation_defaults(self):
        self._check("_configure_fragmentation")

    def test_configure_retry_defaults(self):
        self._check("_configure_retry")

    def test_configure_path_discovery_defaults(self):
        self._check("_configure_path_discovery")

    def test_configure_peer_discovery_defaults(self):
        self._check("_configure_peer_discovery")

    def test_configure_observability_defaults(self):
        self._check("_configure_observability")

    def test_every_configure_method_is_covered(self):
        """A new `_configure_*` method must get its own block here."""
        methods = sorted(
            name for name in dir(self.module.SmartMeshCoreInterface)
            if name.startswith("_configure_") and callable(getattr(self.module.SmartMeshCoreInterface, name))
        )
        self.assertEqual(methods, sorted(CONFIGURE_ORDER))

    def test_direct_timing_budget_is_coherent_on_shipped_defaults(self):
        """`_validate_direct_timing_budget()` reads only attributes the
        `_configure_*` calls above set, and reports incoherence solely via
        RNS.log(..., LOG_WARNING) -- so the check is that it logs nothing.
        The module's `RNS` binding is patched for the call so the assertion
        does not depend on the RNS log level. On the shipped defaults one
        clock-racing attempt costs 45 + 1 s and 200 / 46 = 4.35 attempts fit,
        against a worst-case budget of 4 (direct_send_attempts_handshake /
        direct_fragment_finish_attempts)."""
        module = self.module
        logged = []
        real_log = module.RNS.log
        module.RNS.log = lambda *args, **kwargs: logged.append(args)
        try:
            self.bare._validate_direct_timing_budget()
        finally:
            module.RNS.log = real_log
        self.assertEqual(
            logged, [],
            "shipped defaults warn about their own DIRECT timing budget at startup: "
            + "\n".join(str(entry[0]) for entry in logged),
        )
        bare = self.bare
        attempt_s = bare.direct_ack_timeout_routed_max_s + bare.direct_post_send_listen_max_s
        worst = max(bare.direct_send_attempts, bare.direct_send_attempts_handshake,
                    bare.direct_fragment_finish_attempts)
        self.assertGreaterEqual(bare.reassembly_idle_timeout_s / attempt_s, worst)


if __name__ == "__main__":
    if "--dump" in sys.argv:
        _, per_method = _snapshot(load_interface_module())
        _dump(per_method)
    else:
        unittest.main()
