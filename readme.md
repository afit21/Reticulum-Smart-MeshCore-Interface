# Smart MeshCore Interface

### Overview

An [Reticulum](https://reticulum.network/) (RNS) interface that lets RNS nodes communicate over a [MeshCore](https://meshcore.co.uk/) LoRa mesh without nuking your local MeshCore network!

This project aims to let you access Nomadnet and send LXMF messages over a MeshCore network as a 'last mile' RNS hop without flooding MeshCore with traffic. In summary this is achieved by firewalling traffic entering the interface, prioritizing, artificially delaying some traffic, and making decisions on how to efficiently rout traffic over MeshCore.

The project also aims to be as easy as possible to configure on your RNS nodes. In most situations, a minimal config is needed, just setup your MeshCore companion radio with the MeshCore app before using the interface.

This diagram isn't 100% accurate to how the interface works but should give you a basic idea :)
<img width="1156" height="700" alt="senddiagram" src="https://github.com/user-attachments/assets/f5efbb53-4530-4287-ad89-6438dbd2a88f" />


## TLDR: Please Respect MeshCore Users (don't remove airtime limiters)

This project intentionally caps performance out of respect for the regular MeshCore users. In the current version (alpha0.1.2) I have airtime capped at 30% which in my field tests is the minimum which allows for a usable Nomadnet experience.

As more data is collected, we can move this to a dynamic cap to automically increase this based on other factors such as MeshCore hop count, the Mesh's radio settings, etc.

As this project is under a GPL license, there is nothing stopping you from lifting the caps in your own fork, however I would consider doing this on an established MeshCore Mesh for the sake of personal performance without testing wreckless and disrespectful to those who contribute to the infrustrucure you're using.

In the future I plan on making this interface hostile to other peers transmitting more than their fair share to discourage this.

## Features (Version alpha0.1.2)

In short, this current version allows you to send LXMF messages and browse nomadnet sites over MeshCore. This interface has only been tested this interface over 1, 2 and 3 MeshCore repeater hops with two total RNS nodes communicating over this interface. See the [testing section](#testing) for more details.

New in alpha 0.1.2: Raw data is now sent by default while using Z85 encoding as a fallback if a repeater in the path doesn't support raw binary messages.

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
| Adaptive ACK Timing | Working | Measures the real round trip time to each peer and shortens its own ACK waits to match, instead of always waiting out a fixed worst case. It never waits longer than it did before, only shorter. A missed ACK under a measured wait now doubles the next wait (still capped at the firmware's estimate) instead of discarding the measurement outright — a slow path stays measured, a dead first hop is handled by Dead Hop Detection. |
| Fragment Reconciliation | Experimental | After sending a fragmented message once, the interface asks the peer which fragments it actually holds and re-sends only the missing ones, instead of blindly repeating everything unacknowledged. Since alpha 0.1.1: once the peer provably holds part of the packet, the remaining fragments get a larger retry budget (`direct_fragment_finish_attempts`), a send that still fails is resumed under the same packet id if RNS retries the identical bytes while the peer's reassembly bucket is alive (`direct_fragment_resume_enabled`), and the receive-side duplicate filter lets through the contexts RNS itself never dedups (Resource parts and requests, keepalives, cache requests). Reconcile questions and answers travel one priority tier above bulk data (a bidirectional transfer once held an answer for 30 s behind the answering node's own bursts), an unanswered question is re-asked rather than answered with a speculative re-burst, and the answer budget widens with this node's own queue depth. |
| Dead Hop Detection | Experimental | If the first repeater never echoes our frame when one was due, the attempt is abandoned early and a stale path gets re-discovered in about 30 seconds instead of 4 minutes. |
| Airtime Duty Cycle | Working | Caps this interface at 30% of airtime over a rolling 60 seconds, calculated from real LoRa time-on-air at your radio's own settings. Link keepalive traffic skips the wait so links don't drop, but still counts against the cap. |
| Queue Hygiene | Working | Drops packets whose bytes are already queued or in flight, stale queued packets, and queued packets for a Link that has since been closed, so a backlog isn't dumped onto the mesh when a path comes back. Also forwards at most one spontaneous announce per destination every `announce_min_interval` (300 s) — a 3-fragment announce crossing three repeaters four times in five minutes was most of one field session's channel time. |
| Predictive Transmit Holds | Basic | Uses overheard traffic to predict how long the channel stays busy and waits for it to clear before transmitting. Off by default until there's more multi-hop data behind it. |
| Raw Binary Fragments | Experimental | On by default (`direct_raw_fragments_enabled`) after a successful first field test at zero hop and through a public repeater. Packets too large for one MeshCore text message go to a peer that advertised the capability as raw binary packets (MeshCore's raw data type): no Z85, no text framing, no per-fragment ACK, source-routed along the known path. A 483-byte Resource part is 4 raw fragments instead of 5 text ones plus 5 ACKs (about 43% less sender airtime). Reliability comes from the same have-bitmap reconcile the text path uses; if raw frames provably never arrive on a path but Z85 text does, that repeater chain is noted as not carrying raw (for a day) and a new path is tried raw-first again. A peer that has not advertised the capability (an older build) still gets text fragments. A packet that arrived as raw fragments is treated like any other receive for route learning (delivery proofs, announces) only when the peer it claims to come from is already bound and path-resolved — a raw frame's source is unauthenticated, so an unknown claimant teaches nothing. A destination that answers with delivery proofs rather than announces now has its route learned from those proofs, so a bootstrap send that works can no longer back itself off. Through repeaters each fragment is followed by a quiet time of `direct_raw_hop_gap_factor` (2) × hops × its airtime, so the next transmission cannot catch it inside the half-duplex repeater chain: the first 2- and 4-hop field test (2026-09-19) lost exactly one of every two fragments with a flat 2-airtime gap, and recovered them only on the reconcile round. The reconcile QUERYs also feed the stale-path detector, so a raw sender on a dead cached path resets it within one send instead of three. **Privacy note:** MeshCore raw packets are not encrypted by the firmware, unlike DIRECT text messages. Your message contents are still end-to-end encrypted by Reticulum itself, but the RNS packet header (which destination is being talked to) and the two nodes' key prefixes are visible on air to anyone listening, where text DIRECT hid them. Raw packets are also unauthenticated, so a third party can inject a fragment into a transfer in progress (Reticulum's own crypto rejects the result, but the transfer has to be re-sent). Set `direct_raw_fragments_enabled = no` if you would rather keep the smaller airtime win off and stay fully inside MeshCore's encryption. |

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


## Field Testing

This table summarizes the real world scenarios that I've tested the interface against.
Hops in this table refer to MeshCore Hops. Note - This is the results with airtime useage capped at 30% as is hard-coded and not configurable by design. A dynamic airtime usage cap is in the roadmap.

All tests were conducted on Heltec V3 MeshCore companions over a fairly quiet MeshCore network.

|                 | LXMF Messages (MeshChat) | Nomad Network |
| --------------- | ------------------------ | ------------- |
| 0 Hops (direct) | Works Well               | Works Well    |
| 1 Hop           | Works Well               | Slow          |
| 2 Hops          | Works Well               | Slow          |
| 3 Hops          | Works Well               | Slow          |


Reference speeds - SF7, BW 62.5 kHz, CR 4/8, 916.575 MHz:
|         | One Direction | Bidirectional | RNS Latency |
| ------- | ------------- | ------------- | ----------- |
| 0 Hops  | 391bps        | 338 bps       | 1672ms      |


Please keep in mind that this project is in very early stages. However, this interface currently works better than all others I've been able to test.

## Roadmap (in no particular order)

- Airtime efficiency improvements - Always looking to optimise airtime usage

- A dynamic airtime usage cap is in the roadmap

- Improve security - Security so far has not been a focus.

- Enforce rate limiting as a reciever - This feautre is intended to discourage others from 

- Airtime limiting improvements - I've picked fairly arbitrary numbers for airtime limits. This will later be evaluated against real-world data. I suspect a feature to limit airtime based on the radio settings a MeshCore mesh uses would be the path forward here

- Automatically update MeshCore companion settings to optimise for this purpose

- Overall reliability - Always looking for methods to improve the reliability of this interface

- Compatibility with other MeshCore interfaces - I'd like to make this interface automatically detect other popular MeshCore interfaces and translate our own direct transmits to be able to speak with them

- Remove meshcore_py dependency - Bake in a meshcore library to allow for easy install

- Smarter routing - Better advertise node's routing functions and optimise paths to avoid unesessary MeshCore traffic.

- Better accounting for radio conditions. This interface will adapt to radio conditions. More testing in the real world is required inorder to improve this capability.

- Better compatability with LXMF clients other than MeshChat

- Smarter traffic throttling


## Credits

- [comms-engineer/RNS_Over_Meshcore](https://github.com/comms-engineer/RNS_Over_Meshcore) — inspiration taken from this project for the discovery protocol.
- [Akita Engineering/Akita-Zmodem-MeshCore](https://github.com/AkitaEngineering/Akita-Zmodem-MeshCore/) — source code referenced for data transfer features.

## Contributing
Packet captures from your field tests are always appreciated if you'd like to contact me or submit a pull request adding your tests to the fieldtests folder.

Enable packet captures with:
``` ini
  [[Smart MeshCore Interface]]
    packet_capture_enabled = yes
```

Feel free to contribute code if you'd like to by opening a pull request :)

## AI Usage

This repo makes heavy use of Claude. The majority of code is written with Claude but is human validated.
Builds are field tested over real hardware.
