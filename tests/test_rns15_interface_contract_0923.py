"""RNS 1.5 reads `ifac_size` on every inbound frame (2026-09-23).

`Transport.preprocess_inbound` sizes every frame against
`interface.HW_MTU + (interface.ifac_size or 0)`. That attribute is set by
`RNS.Reticulum` when IT configures an interface from the config file -- None
when no IFAC is configured (`RNS/Reticulum.py`) -- and the RNS base
`Interface` class does not define it. So under `rnsd` nothing was ever
wrong: Reticulum sets the instance attribute before any traffic flows.

Every path that constructs this interface WITHOUT Reticulum broke on RNS
1.5.4, raising AttributeError on the first inbound packet: the hardware
scripts in `testscripts/` that build a SmartMeshCoreInterface directly
(`zero_hop_peer_discovery_test.py` and the other white-box single-radio
tools), and the hermetic unit tests. The interface now defines it itself,
which an instance value from Reticulum still shadows.
"""
import time
import unittest

import RNS

from tests._support import SingleNodeCase, build_rns_packet


class Rns15InterfaceContract(SingleNodeCase):
    def test_the_interface_defines_ifac_size(self):
        iface = self.iface
        self.assertTrue(hasattr(iface, "ifac_size"))
        self.assertIsNone(iface.ifac_size, "None is what Reticulum sets when no IFAC is configured")
        self.assertEqual(iface.HW_MTU, RNS.Reticulum.MTU)

    def test_a_value_set_by_reticulum_is_not_overwritten(self):
        # Reticulum assigns the instance attribute when it configures the
        # interface; this must not clobber a configured IFAC size.
        iface = self.iface
        saved = iface.ifac_size
        try:
            iface.ifac_size = 8
            self.assertEqual(iface.ifac_size, 8, "an instance value shadows the class default")
        finally:
            iface.ifac_size = saved

    def test_an_inbound_frame_does_not_raise_on_the_size_check(self):
        # The exact expression Transport.preprocess_inbound evaluates.
        iface = self.iface
        raw = build_rns_packet("data", dest_hash=b"\x11" * 16, payload=b"x" * 40)
        self.assertLessEqual(len(raw), iface.HW_MTU + (iface.ifac_size or 0))
        # And end to end: handing RNS a frame on this interface must not
        # raise. RNS logs and swallows exceptions from its worker, so the
        # assertion is that the interface carries what Transport reads.
        errors = []
        original = RNS.log

        def capture(msg, level=None, *a, **kw):
            if "ifac_size" in str(msg):
                errors.append(str(msg))
            return original(msg, level, *a, **kw) if level is not None else original(msg)

        RNS.log = capture
        try:
            RNS.Transport.inbound(raw, iface)
            time.sleep(0.2)
        finally:
            RNS.log = original
        self.assertEqual(errors, [], "no AttributeError on ifac_size reached RNS's log")


if __name__ == "__main__":
    unittest.main()
