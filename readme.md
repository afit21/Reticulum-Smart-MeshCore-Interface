# SmartMeshCoreInterface

A [Reticulum](https://reticulum.network/) (RNS) interface that lets RNS nodes communicate over a [MeshCore](https://meshcore.co.uk/) LoRa mesh — implemented in [`Interface/SmartMeshCoreInterface.py`](Interface/SmartMeshCoreInterface.py), a single self-contained file.

## Status: alpha 0.1.0
In short, this current version allows you to send LXMF messages and browse nomadnet sites reliably over zero-hop MeshCore (nomadnet is quite slow). This interface has only been tested this interface over 1, 2 and 3 MeshCore repeater hops with two total RNS nodes communicating over this interface.

## Why this exists, and how it's different

MeshCore's own CHANNEL broadcast is unauthenticated and unacknowledged — great for reach, unreliable for anything beyond a single small fragment. A prior implementation of this idea tried to tune multi-fragment CHANNEL delivery into reliability and hit a hard wall: field testing found ~80% delivery at one fragment, 0% at two or three, no matter how much the spacing was tuned. This interface takes a different approach:

- **DIRECT is the primary transport, not a fallback.** Once two nodes have exchanged bind frames (this interface's own lightweight peer-discovery protocol) and RNS has opportunistically learned which MeshCore peer a given RNS destination belongs to, traffic between them goes DIRECT — MeshCore's real, ACK'd, cryptographically-identified point-to-point transport — instead of broadcasting on CHANNEL.
- **CHANNEL is reserved for what actually needs it**: announces, path requests, and bootstrapping a brand-new destination before a DIRECT route is known — and even then, a small-mesh optimization (see below) skips CHANNEL entirely once there are only a few bound peers (three or fewer by default), since there's no ambiguity left about who a packet is for.
- **Self-throttling, not just self-limiting.** Real field use found gaps this design didn't anticipate on paper — DIRECT sends colliding with each other when issued concurrently, RNS's own Link-keepalive timing getting miscalibrated by an unrepresentatively fast handshake, a destination that will never answer getting retried forever — and each was found, fixed, and documented from actual packet-capture evidence, not guessed at.

- **Send once, then ask.** A DIRECT-fragmented send transmits each fragment once; if any ACK is missing it asks the receiver which fragments it actually holds (a small have-bitmap reply) and re-sends only the true gaps — instead of blindly retrying data that arrived but whose ACK was lost, the failure mode real field captures showed. Peers running an older version simply don't answer, and the sender falls back to re-driving everything. `direct_fragment_reconcile_enabled = no` restores the old per-fragment retry budget.
- **Measured, not guessed, ACK timeouts.** Every DIRECT send's real ACK round-trip is measured per peer; once a few samples exist, the ACK-wait timeout is sized from that measurement (never larger than the firmware's own hop-count estimate, never below `direct_ack_rtt_min_timeout`, and discarded on the first miss or any path change so a slower link falls straight back to the conservative value). A missed ACK holds the shared radio for the whole timeout, so this is where wasted air-silence actually goes. Set `direct_ack_rtt_adaptive_enabled = no` to keep the firmware estimate only.

This interface essentially aims to inspect RNS packets & automatically drop unnecessary traffic

- **A packet capture tool built in.** Optional, off by default (`packet_capture_enabled`) — logs every in/out RNS packet as one JSON line (classification, routing decision, sender/target, timing) to a configurable directory, for exactly this kind of real-evidence debugging. When capture is on, the interface also records every packet the radio overhears on air (the companion firmware's raw-RX log feed, `rx_log_observe_enabled`, default on) — addressed to this node or not — with SNR, MeshCore packet type, path, and timing relative to this node's own last transmit.

## Requirements

- A MeshCore-flashed LoRa radio (serial, BLE, or TCP-connected), reachable via the [`meshcore`](https://pypi.org/project/meshcore/) Python library.
- [Reticulum](https://pypi.org/project/rns/) (`pip install rns`).
- Python 3.9+.

## Installation

Copy the interface into your Reticulum install's interfaces directory:

```
cp Interface/SmartMeshCoreInterface.py ~/.reticulum/interfaces/SmartMeshCoreInterface.py
```

Then add a block to `~/.reticulum/config`, under `[interfaces]`. A minimal real-world example. Please just use the reference config if you're not sure if your local MeshCore operators would be chill with you experimenting (serial-connected radio):

Reference config for a transfer node - by default, the interface will use the MeshCore settings saved to your companion:

```ini
[[Smart MeshCore Interface]]
  type = SmartMeshCoreInterface
  interface_enabled = yes
  transport = serial
  port = /dev/ttyUSB0 #Please verify this is your MeshCore radio
  baudrate = 115200
  
  mode = access_point
```

Reference config for a non-transfer node:

```ini
[[Smart MeshCore Interface]]
  type = SmartMeshCoreInterface
  interface_enabled = yes
  transport = serial
  port = /dev/ttyUSB0 #Please verify this is your MeshCore radio
  baudrate = 115200
```

Restart `rnsd` (or the app hosting your Reticulum instance) to pick it up. The full config surface (~55 options — retry budgets, timeouts, spacing tiers, packet capture, etc.) is documented inline in the interface's own `_configure_*` methods; see [`docs/interface_architecture.md`](docs/interface_architecture.md)'s "Config surface" section for a consolidated list.

## Roadmap (in no particular order)

### Efficiency
Improve the efficiency of data transfers

### Dynamic transmit & listed timing
Once more data has been collected, the goal is to have the interface automatically adjust it's own parameters situationally to adjust to traffic conditions, radio settings, and distance from target peers.

### Improved packet inspection and filtering
Tune the packet inspection and filtering functions to more make better decisions

## Testing

This table summarizes the real world scenarios that I've tested the interface against.
Hops in this table refer to MeshCore Hops.

All tests were conducted on Heltec V3 MeshCore companions over a fairly quiet MeshCore network.

|                 | LXMF Messages (MeshChat) | Nomad Network |
| --------------- | ------------------------ | ------------- |
| 0 Hops (direct) | Works Well               | Works Well    |
| 1 Hop           | Slow                     | Unreliable    |
| 2 Hops          | Slow                     | Not working   |
| 3 Hops          | Slow                     | Not working   |

Please keep in mind that this project is in very early stages. This interface currently works better than all others I've been able to test.

### Automated tests and simulation (no hardware)

`python3 -m unittest discover -s tests` runs the automated suite: wire-format, RNS-header and reliability-engine unit tests (about a second), plus end-to-end scenarios that run two real interface instances through simulated repeater hops (`SMCI_SKIP_SLOW=1` skips those). The simulated mesh lives in `testscripts/simmesh/` and models DIRECT routing through repeaters, ACKs, path discovery, contacts, flood dedup, half-duplex, collisions and loss. `testscripts/fake_meshcore_repeater_sim.py` runs the interface over any topology you describe (`--link A-R --link R-B --repeater R`), `testscripts/rns_multiprocess_sim.py` does the same with a full real Reticulum instance per node, and `testscripts/calibrate_sim_from_captures.py` derives loss/latency settings for the simulator from real field captures. None of this replaces the field table above — simulated timing is not real radio timing — but it lets a change be checked against multi-hop DIRECT behavior before it goes anywhere near a real repeater.

## Credits

- [comms-engineer/RNS_Over_Meshcore](https://github.com/comms-engineer/RNS_Over_Meshcore) — inspiration taken from this project for the discovery protocol.
- [Akita Engineering/Akita-Zmodem-MeshCore](https://github.com/AkitaEngineering/Akita-Zmodem-MeshCore/) — source code referenced for data transfer features.

## Contributing
Packet captures from your field tests are always appreciated if you'd like to contact me or submit a pull request adding your tests to the fieldtests folder.

Feel free to contribute code if you'd like to by opening a pull request :)

## AI Usage

This repo makes heavy use of Claude. The majority of code is written with Claude but is human validated.
Builds are field tested over real hardware.
