"""
simmesh -- an in-process simulated MeshCore mesh for exercising
Interface/SmartMeshCoreInterface.py with no radio hardware.

Role since 2026-09-20 (see CLAUDE.md "Legacy simulation tooling"): the fake
`meshcore` library and one-process fake firmware that the unit suite under
tests/ brings a real interface up against in milliseconds. The multi-hop
air/radio model below is what the archived simulators in testscripts/legacy/
and tests/legacy/ drove; that fidelity role now belongs to
testscripts/meshbench_scenarios.py, which runs the interface against real
MeshCore firmware under MeshBench. Keep the library semantics here faithful
(the unit tests depend on them); do not extend the air model.

Three layers, each independently reusable:

  air.py            The physics: topology (who can hear whom), per-node
                    half-duplex blind spots, seeded relay jitter, global
                    and per-link (directional) loss, a linear airtime
                    model. Nothing in here knows what a packet *means*.

  radio.py          One node's firmware: contact table, flood dedup ring,
                    source-routed DIRECT forwarding (repeaters only),
                    ACK generation and return routing, path discovery
                    request/response with telemetry-permission gating,
                    path learning, the queue-then-MESSAGES_WAITING inbox,
                    and the raw-RX log feed. Repeaters are SimRadios with
                    no application attached.

  fake_meshcore.py  A stand-in for the `meshcore` Python library, driving
                    a SimRadio: EventType enum, event dispatcher with
                    attribute-filtered wait_for_event, the commands the
                    interface actually calls, contacts/ensure_contacts
                    dirty-flag semantics, start_auto_message_fetching.

  harness.py        Glue for tests and scripts: load the interface module
                    by path, construct interfaces against a topology, build
                    real packed RNS packets of every type the routing
                    dispatcher distinguishes, read/summarize the
                    interface's own packet capture output.

  remote.py         Socket transport so end nodes can live in other
                    processes (testscripts/rns_multiprocess_sim.py runs a
                    real RNS.Reticulum per node).

See each module's own docstring for what is and isn't modeled. The
firmware behaviors modeled here were checked against
referenceprojects/MeshCore-main (direct-route forwarding in Mesh.cpp:
a repeater forwards a DIRECT packet only when path[0] is its own hash
byte, then strips it) and against the installed `meshcore` library
(reader.py event shapes, meshcore.py contacts semantics, messaging.py
send_path_discovery_sync's timeout arithmetic). Everything else --
airtime, jitter magnitudes, suggested_timeout values, RSSI/SNR -- is a
deliberately simple approximation good enough to drive the interface's
logic, not a physical-layer model.
"""
from .air import Air, SimPacket, ROUTE_FLOOD, ROUTE_DIRECT
from .radio import SimRadio, RadioOptions, node_pubkey, node_hash_byte, node_prefix
from .fake_meshcore import EventType, SimEvent, SimMeshCore, make_fake_meshcore_module
from .harness import (
    FAST_TIMING, SimMesh, SimNode, RecordingOwner, build_rns_packet, ensure_rns,
    load_interface_module, wait_until, read_capture, summarize_capture, TEST_DEST_HASH,
)

__all__ = [
    "Air", "SimPacket", "ROUTE_FLOOD", "ROUTE_DIRECT",
    "SimRadio", "RadioOptions", "node_pubkey", "node_hash_byte", "node_prefix",
    "EventType", "SimEvent", "SimMeshCore", "make_fake_meshcore_module",
    "FAST_TIMING", "SimMesh", "SimNode", "RecordingOwner", "build_rns_packet", "ensure_rns",
    "load_interface_module", "wait_until", "read_capture", "summarize_capture", "TEST_DEST_HASH",
]
