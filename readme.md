# Smart MeshCore Interface

### Overview

An [Reticulum](https://reticulum.network/) (RNS) interface that lets RNS nodes communicate over a [MeshCore](https://meshcore.co.uk/) LoRa mesh without nuking your local MeshCore network!

This project aims to let you access Nomadnet and send LXMF messages over a MeshCore network as a 'last mile' RNS hop without flooding MeshCore with traffic. In summary this is achieved by firewalling traffic entering the interface, prioritizing, artificially delaying some traffic, and making decisions on how to efficiently rout traffic over MeshCore.

The project also aims to be as easy as possible to configure on your RNS nodes. In most situations, a minimal config is needed, just setup your MeshCore companion radio with the MeshCore app before using the interface.

This diagram isn't accurate to how the interface works but should give you a basic idea :)
<img width="1083" height="502" alt="RNSMESHCOREINTERFACEDIAGRAM.png" src="https://github.com/user-attachments/assets/505840d1-2e79-494c-b930-09b395ab4ec0" />


## Features (Version alpha0.1.1)

In short, this current version allows you to send LXMF messages and browse nomadnet sites over MeshCore. This interface has only been tested this interface over 1, 2 and 3 MeshCore repeater hops with two total RNS nodes communicating over this interface. See the [testing section](#testing) for more details.

New in alpha 0.1.1: the interface now listens to the raw RX log from your companion radio, so instead of guessing with fixed delays it can measure what the mesh is actually doing — see the last seven rows of the table below.

**Battle Tested** - Tested and confident this is reliable.

**Working** - Working, but testing has been limited.

**Experimental** - Works most of the time or unreliable over multiple MeshCore hops or other conditions.

**Unstable** - Works sometimes.

**Basic** - only a bare bones implementation of this feature exists. It may not be tested at all.

|    Feature    |    State    |        Description        |
|----|----|----|
| Automatic Link | Working | The interface will automatically peer with other nodes running this interface provided same MeshCore settings on companion. |
| Z85 Encode | Battle Tested | Encode and decode RNS packets in Z85 for transport over MeshCore. This provides a size efficiency compared to base64 encoding. ~25% overhead compared to ~35% base 64 overhead. |
| Meshcore Routing | Experimental | Route broadcast traffic over channels, and direct traffic over direct messages. The interface keeps a cache of which RNS addresses route to each MeshCore contact. If a low amount of peers are known, the interface sends broadcast traffic over direct message to avoid unnecessary floods. |
| RNS Packet Aware Firewalling | Experimental | Inspects outgoing traffic and automatically drops unnecessary traffic. This is a sacrifice made to maintain respect for MeshCore users. |
| Packet Aware Self-throttling | Working | Not just a speed limit. The interface tries to delay and prioritize packets. For example, packets related to a link handshake will be delayed so that less keep alive packets are required during the life of an RNS link.
| Packet Capture Debug Tool | Battle tested | A built in packet capture tool used for debugging the interface. Also logs every packet the radio overhears, not just our own. |
| Radio Traffic Awareness | Working | The interface taps the raw RX log from your companion radio, so it can see every packet the radio decodes — including traffic that isn't ours. Costs no airtime and no extra transmissions. |
| Adaptive ACK Timing | Working | Measures the real round trip time to each peer and shortens its own ACK waits to match, instead of always waiting out a fixed worst case. It never waits longer than it did before, only shorter. |
| Fragment Reconciliation | Experimental | After sending a fragmented message once, the interface asks the peer which fragments it actually holds and re-sends only the missing ones, instead of blindly repeating everything unacknowledged. Since alpha 0.1.1: once the peer provably holds part of the packet, the remaining fragments get a larger retry budget (`direct_fragment_finish_attempts`), a send that still fails is resumed under the same packet id if RNS retries the identical bytes while the peer's reassembly bucket is alive (`direct_fragment_resume_enabled`), and the receive-side duplicate filter lets through the contexts RNS itself never dedups (Resource parts and requests, keepalives, cache requests). |
| Dead Hop Detection | Experimental | If the first repeater never echoes our frame when one was due, the attempt is abandoned early and a stale path gets re-discovered in about 30 seconds instead of 4 minutes. |
| Airtime Duty Cycle | Working | Caps this interface at 30% of airtime over a rolling 60 seconds, calculated from real LoRa time-on-air at your radio's own settings. Link keepalive traffic skips the wait so links don't drop, but still counts against the cap. |
| Queue Hygiene | Working | Drops packets whose bytes are already queued or in flight, and stale queued packets, so a backlog isn't dumped onto the mesh when a path comes back. |
| Predictive Transmit Holds | Basic | Uses overheard traffic to predict how long the channel stays busy and waits for it to clear before transmitting. Off by default until there's more multi-hop data behind it. |
| Raw Binary Fragments | Experimental | On by default (`direct_raw_fragments_enabled`) after a successful first field test at zero hop and through a public repeater. Packets too large for one MeshCore text message go to a peer that advertised the capability as raw binary packets (MeshCore's raw data type): no Z85, no text framing, no per-fragment ACK, source-routed along the known path. A 483-byte Resource part is 4 raw fragments instead of 5 text ones plus 5 ACKs (about 43% less sender airtime). Reliability comes from the same have-bitmap reconcile the text path uses; if raw frames provably never arrive on a path but Z85 text does, that repeater chain is noted as not carrying raw (for a day) and a new path is tried raw-first again. A peer that has not advertised the capability (an older build) still gets text fragments. **Privacy note:** MeshCore raw packets are not encrypted by the firmware, unlike DIRECT text messages. Your message contents are still end-to-end encrypted by Reticulum itself, but the RNS packet header (which destination is being talked to) and the two nodes' key prefixes are visible on air to anyone listening, where text DIRECT hid them. Raw packets are also unauthenticated, so a third party can inject a fragment into a transfer in progress (Reticulum's own crypto rejects the result, but the transfer has to be re-sent). Set `direct_raw_fragments_enabled = no` if you would rather keep the smaller airtime win off and stay fully inside MeshCore's encryption. |

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

Restart `rnsd` (or the app hosting your Reticulum instance) to pick it up. The full config surface (~98 options — retry budgets, timeouts, spacing tiers, duty cycle, RX-log behaviour, packet capture, etc.) is documented inline in the interface's own `_configure_*` methods. None of it is required; the defaults are what the field tests ran on.

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

Alpha 0.1.1 field data (2026-09-18, raw captures in `fieldtests/raw/postAlpha0.1.0/`): a zero-hop NomadNet page load and an evening drive test that ran down through 3, 2 and 1 hops. Individual DIRECT frames were delivered 138/141 at 0 hops, 49/53 at 1 hop, 37/45 at 2 hops and 23/30 at 3 hops. That's per frame, not per message — a message split into several fragments is only as good as its worst fragment, which is why the table above is harsher than those numbers look.

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
