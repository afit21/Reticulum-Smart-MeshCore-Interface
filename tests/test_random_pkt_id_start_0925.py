"""2026-09-25 review: the pkt_id counter starts at a random value.

It started at 0 in every process. A receiver keeps whole-packet dedup
entries keyed (sender, pkt_id, frag_total) for `whole_packet_dedup_ttl`
(150 s) -- matched on the key alone, never the bytes -- and idle
reassembly buckets for longer. So when a sender's rnsd restarted inside
that time (the normal step after installing a build), its first new
fragmented packets reused the previous run's ids: the receiver dropped
them as duplicates and still reported them complete, so the sender
counted them delivered while RNS never saw them; or, against a stale
partial bucket, merged old and new fragments into one corrupt packet.
The code-review agent reproduced the silent drop on the one-node fixture.

A random 16-bit start makes a collision with the previous run's recent ids
~k/65536 instead of certain. These tests pin that the start is drawn from
the full 16-bit range, that a new interface takes its counter from it, and
that the restart scenario no longer collides for the counts a restart
actually leaves behind.
"""
import unittest
from unittest import mock

from tests._support import SingleNodeCase, SimMesh, RadioOptions, quiet_rns


class RandomPktIdStart(SingleNodeCase):
    def test_start_is_random_over_the_full_16_bit_range(self):
        cls = self.module.SmartMeshCoreInterface
        draws = [cls._initial_pkt_id() for _ in range(200)]
        self.assertTrue(all(0 <= d <= 0xFFFF for d in draws))
        self.assertGreater(len(set(draws)), 150, "not random: the start repeats")
        self.assertGreater(max(draws), 0xFF, "must use both bytes of the pkt_id field")

    def test_a_new_interface_counts_from_the_random_start(self):
        quiet_rns()
        mesh = SimMesh(["C-D"], seed=2, startup_stagger_s=0.0,
                       radio_options=RadioOptions(auto_advert=False, advert_interval_s=0))
        self.addCleanup(mesh.stop)
        with mock.patch.object(self.module.SmartMeshCoreInterface, "_initial_pkt_id",
                               staticmethod(lambda: 0xFFFE)):
            node = mesh.add_node("C", config={"peer_discovery_enabled": "no"})
        iface = node.iface
        got = [self.node.run_on_loop(_call(iface._next_pkt_id)) for _ in range(3)]
        self.assertEqual(got, [0xFFFE, 0xFFFF, 0x0000], "counts on from the start and wraps at 16 bits")

    def test_restart_does_not_reuse_the_previous_runs_ids(self):
        # The previous run sent 20 fragmented packets (ids 0..19 when it
        # started at 0); a restarted run's first 20 ids must almost never
        # land on them. With a start at 0 every restart collided.
        cls = self.module.SmartMeshCoreInterface
        before = {(cls._initial_pkt_id() + i) & 0xFFFF for i in range(20)}
        collisions = 0
        for _ in range(500):
            start = cls._initial_pkt_id()
            if any(((start + i) & 0xFFFF) in before for i in range(20)):
                collisions += 1
        # Expected ~0.06% (39/65536 per restart); allow wide slack.
        self.assertLess(collisions, 25, f"{collisions}/500 restarts reused a recent pkt_id")


async def _call(fn):
    return fn()


if __name__ == "__main__":
    unittest.main()
