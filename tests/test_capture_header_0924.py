"""
Capture header and default directory (0.1.0, user request 2026-09-24).

Every capture file now opens with a `capture_header` record: the settings the
interface runs with (every value the `_configure_*` calls set, defaults
included), the keys the config block gave, and the radio SELF_INFO reported.
Before this, a capture said nothing about the build's settings or the radio,
and reading one meant knowing both from elsewhere. The channel secret is
redacted and the node's advertised position is left out, since captures get
committed under fieldtests/raw/. A later SELF_INFO with different radio
fields (a reconnect after the radio was reconfigured) writes a
`radio_settings` record.

The default directory is `meshcore_packet_capture` under the RNS storage
path (~/.reticulum/storage for a default install), falling back to
~/.reticulum/storage when RNS has none, so `packet_capture_enabled = yes`
is the only setting a capture needs; `packet_capture_dir` still overrides it.
"""
import json
import os
import tempfile
import unittest
from unittest import mock

from tests._support import SingleNodeCase, load_interface_module


def _records(tmp):
    files = sorted(os.listdir(tmp))
    with open(os.path.join(tmp, files[0])) as f:
        return [json.loads(line) for line in f]


class CaptureHeader(SingleNodeCase):
    def _open(self, **overrides):
        iface = self.iface
        saved = {k: getattr(iface, k) for k in ("packet_capture_dir", "_packet_capture_file", "_captured_radio")}
        tmp = tempfile.mkdtemp(prefix="smci-header-")
        iface.packet_capture_dir, iface._packet_capture_file = tmp, None
        self.addCleanup(lambda: [iface._close_packet_capture(), [setattr(iface, k, v) for k, v in saved.items()]])
        iface._open_packet_capture()
        self.assertIsNotNone(iface._packet_capture_file)
        return tmp

    def test_first_record_is_the_header(self):
        tmp = self._open()
        self.iface._capture_event("out", {"event": "probe"})
        recs = _records(tmp)
        self.assertEqual([r["event"] for r in recs], ["capture_header", "probe"])
        head = recs[0]
        self.assertEqual(head["seq"], 1)
        self.assertEqual(head["interface"], self.iface.name)
        # The fake firmware's SELF_INFO radio block (simmesh/fake_meshcore.py).
        for key, value in {"radio_freq": 915.5, "radio_bw": 250, "radio_sf": 8, "radio_cr": 5, "tx_power": 20}.items():
            self.assertEqual(head["radio"][key], value, key)
        self.assertNotIn("adv_lat", head["radio"])
        self.assertNotIn("adv_lon", head["radio"])
        self.assertIsNotNone(head["radio_params"])
        settings = head["settings"]
        self.assertIn("direct_raw_gap_own_airtime", settings)
        self.assertIs(settings["packet_capture_enabled"], self.iface.packet_capture_enabled)
        self.assertEqual(settings["channel_secret_hex"], "<redacted>")
        self.assertIsInstance(head["config_given"], dict)

    def test_radio_change_writes_a_radio_settings_record(self):
        tmp = self._open()
        info = dict(self.iface._self_info)
        self.iface._apply_self_info(info)
        info["radio_sf"] = 9
        self.iface._apply_self_info(info)
        events = [r["event"] for r in _records(tmp)]
        self.assertEqual(events, ["capture_header", "radio_settings"],
                         "an unchanged SELF_INFO writes nothing; a changed one writes one record")


class SettingsSnapshot(unittest.TestCase):
    def test_config_given_is_redacted(self):
        cls = load_interface_module().SmartMeshCoreInterface
        self.assertEqual(cls._capture_safe_value("channel_secret", "abcd"), "<redacted>")
        self.assertEqual(cls._capture_safe_value("port", "/dev/ttyUSB0"), "/dev/ttyUSB0")
        self.assertEqual(cls._capture_safe_value("x", {3, 1}), [1, 3])


class DefaultDirectory(unittest.TestCase):
    def setUp(self):
        module = load_interface_module()
        self.RNS = module.RNS
        self.bare = module.SmartMeshCoreInterface.__new__(module.SmartMeshCoreInterface)
        self.bare._configure_observability({})

    def test_default_is_meshcore_packet_capture_under_rns_storage(self):
        with mock.patch.object(self.RNS.Reticulum, "storagepath", "/x/.reticulum/storage", create=True):
            self.assertEqual(self.bare._capture_dir(), "/x/.reticulum/storage/meshcore_packet_capture")

    def test_without_rns_storage_falls_back_to_the_default_install(self):
        with mock.patch.object(self.RNS.Reticulum, "storagepath", None, create=True):
            self.assertEqual(self.bare._capture_dir(),
                             os.path.expanduser("~/.reticulum/storage/meshcore_packet_capture"))

    def test_configured_directory_wins_and_expands_home(self):
        self.bare.packet_capture_dir = "~/caps"
        self.assertEqual(self.bare._capture_dir(), os.path.expanduser("~/caps"))


if __name__ == "__main__":
    unittest.main()
