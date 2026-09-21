"""
Capture hygiene (alpha 0.1.5, item 7, 2026-09-21): the capture filename
carries a node label.

The alpha 0.1.4 field session left two machines' captures of one session
indistinguishable by name (the desktop's had to be renamed `afipc_...` by
hand). The interface now names its file
`<label>_capture_<interface>_<stamp>.jsonl`, the label being the MeshCore
node name SELF_INFO gave (`afipc` on the desktop, `a` on the laptop) unless
`packet_capture_label` sets it; with neither, the pre-0.1.5 name is kept.
The readers (`meshbench_report.capture_files`, `field_ab_compare.node_of`,
`simmesh.harness.read_capture`) accept both forms.

`since_own_tx_s` reading the radio's busy-until is 2a's
(`tests/test_radio_busy_until_0921.py`).
"""
import os
import sys
import tempfile
import unittest

from tests._support import SingleNodeCase, TESTSCRIPTS, load_interface_module

if TESTSCRIPTS not in sys.path:
    sys.path.insert(0, TESTSCRIPTS)


class FilenameRule(unittest.TestCase):
    def test_pure_filename(self):
        module = load_interface_module()
        fn = module.SmartMeshCoreInterface._capture_filename
        self.assertEqual(fn("afipc", "Smart MeshCore Interface", "20260921T082952"),
                         "afipc_capture_Smart_MeshCore_Interface_20260921T082952.jsonl")
        self.assertEqual(fn("", "Smart MeshCore Interface", "20260921T082952"),
                         "capture_Smart_MeshCore_Interface_20260921T082952.jsonl")
        self.assertEqual(fn("  a ", "X", "s"), "a_capture_X_s.jsonl")
        self.assertEqual(fn("node/one:two", "X", "s"), "node_one_two_capture_X_s.jsonl", "labels are made filename-safe")

    def test_shipped_default_label_is_empty(self):
        module = load_interface_module()
        bare = module.SmartMeshCoreInterface.__new__(module.SmartMeshCoreInterface)
        bare._configure_observability({})
        self.assertEqual(bare.packet_capture_label, "")


class OpenUsesTheNodeName(SingleNodeCase):
    def _open(self, label_cfg, node_name):
        iface = self.iface
        saved = (iface.packet_capture_dir, iface.packet_capture_label, iface._own_node_name, iface._packet_capture_file)
        tmp = tempfile.mkdtemp(prefix="smci-label-")
        iface.packet_capture_dir, iface.packet_capture_label, iface._own_node_name = tmp, label_cfg, node_name
        iface._packet_capture_file = None
        try:
            iface._open_packet_capture()
            self.assertIsNotNone(iface._packet_capture_file)
            iface._capture_event("out", {"event": "probe"})
            return tmp, sorted(os.listdir(tmp))
        finally:
            iface._close_packet_capture()
            iface.packet_capture_dir, iface.packet_capture_label, iface._own_node_name, iface._packet_capture_file = saved

    def test_node_name_from_self_info_is_the_default_label(self):
        tmp, files = self._open("", "afipc")
        self.assertEqual(len(files), 1)
        self.assertTrue(files[0].startswith("afipc_capture_"), files[0])
        from simmesh.harness import read_capture
        self.assertEqual([r["event"] for r in read_capture(tmp, self.iface.name)], ["probe"],
                         "the harness reader finds the labelled file")
        from field_ab_compare import node_of
        self.assertEqual(node_of(files[0]), "afipc")
        from meshbench_report import capture_files
        self.assertEqual(list(capture_files(tmp).keys()), [
            "".join(c if c.isalnum() or c in "-_" else "_" for c in self.iface.name)])

    def test_packet_capture_label_overrides_the_node_name(self):
        _tmp, files = self._open("laptop-b", "a")
        self.assertTrue(files[0].startswith("laptop-b_capture_"), files[0])

    def test_no_label_and_no_name_keeps_the_old_filename(self):
        _tmp, files = self._open("", "")
        self.assertTrue(files[0].startswith("capture_"), files[0])


if __name__ == "__main__":
    unittest.main()
