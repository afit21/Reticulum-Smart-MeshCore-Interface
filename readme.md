<div align="center">

  <img width="450" height="110" alt="rnsmeshcoreinterface" src="https://github.com/user-attachments/assets/63093ecc-61b0-43d4-8db7-619bf3d7f8f3" />

  <h1>Smart MeshCore Interface for Reticulum</h1>

  <p>
    A <a href="https://reticulum.network/">Reticulum</a> (RNS) interface that lets RNS nodes communicate over a <a href="https://meshcore.co.uk/">MeshCore</a> LoRa mesh without nuking your local MeshCore network!
  </p>
  
</div>


## Overview
This project aims to let you access Nomadnet and send LXMF messages over a MeshCore network as a 'last mile' RNS hop without flooding MeshCore with traffic. In summary this is achieved by using direct messages where possible, limiting traffic, only sending whats necessary, artificially delaying, and prioritizing some RNS traffic.

The project also aims to be as easy as possible to configure on your RNS nodes. In most situations, a minimal config is needed, just setup your MeshCore companion radio with the MeshCore app before using the interface.


## TLDR: Please Respect MeshCore Users (don't remove airtime limiters)

This project intentionally caps performance out of respect for the regular MeshCore users. In the current version (alpha0.1.4) I have airtime capped at 30% which results in a usable experience.
As more testing is done, the project will move towards dynamic airtime limiting, however please don't remove the limits unless you know what you're doing or your local user base is fine with it.

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
    declares_upstream_rns = yes
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

Restart `rnsd` (or the app hosting your Reticulum instance) to pick it up. The full config surface (~140 options — retry budgets, timeouts, spacing tiers, duty cycle, RX-log behaviour, packet capture, etc.) is documented inline in the interface's own `_configure_*` methods. None of it is required; the defaults are what the field tests ran on.

## Features (Version alpha0.1.4)

In short, this version lets you send LXMF messages and browse NomadNet sites over MeshCore. It has been tested over 1, 2 and 3 MeshCore repeater hops with two RNS nodes communicating over this interface. See the [testing section](#field-testing) for more details.

New in alpha 0.1.4:

- less airtime per delivered byte. Fragment reports no longer wait for a MeshCore ACK, one report covers a whole window of parts, a large packet is three raw fragments instead of four, and from one hop up each burst carries a parity fragment so a single lost fragment is repaired without a retry round.

- ~60 % less airtime per delivered byte on multi-fragment transfers, with about five times the delivery rate 

**Battle Tested** - Tested and confident this is reliable.

**Working** - Working, but testing has been limited.

**Experimental** - Works most of the time, or unreliable over multiple MeshCore hops or other conditions.

**Unstable** - Works sometimes.

**Basic** - Only a bare-bones implementation exists. It may not be tested at all.

|    Feature    |    State    |        Description        |
|----|----|----|
| Automatic Link | Working | Automatically peers with other nodes running this interface, provided the companions share the same MeshCore settings. |
| Raw Binary | Working | RNS packets are fragmented and sent over MeshCore in raw binary |
| Z85 Encode | Battle Tested | Encodes RNS packets in Z85 for transport over MeshCore text messages (~25% overhead vs ~35% for base64). This is used for channel traffic and as a backup when repeaters in a path aren't capable of forwarding raw binary messages |
| MeshCore Routing | Experimental | Broadcast traffic goes over a channel, direct traffic over direct messages. Keeps a cache of which RNS addresses route to each MeshCore contact, and with only a few known peers sends broadcast traffic as direct messages to avoid unnecessary floods. |
| RNS Packet Aware Firewalling | Working | Inspects outgoing traffic and drops what the mesh doesn't need, out of respect for MeshCore users. |
| Packet Aware Self-throttling | Working | Not just a speed limit: delays and prioritises packets by type, e.g. holding link-handshake packets so fewer keepalives are needed over the life of an RNS link. |
| Packet Capture Debug Tool | Battle Tested | Built-in packet capture for debugging the interface. Also logs every packet the radio overhears, not just our own. |
| Radio Traffic Awareness | Working | Taps the companion radio's raw RX log to see every packet it decodes, including traffic that isn't ours. Costs no airtime. |
| Adaptive ACK Timing | Working | Measures the real round trip to each peer and shortens ACK waits to match, never waiting longer than the hop-scaled ceiling. A missed ACK doubles the next wait rather than discarding the measurement. |
| Fragment Reconciliation | Experimental | After sending a fragmented packet, the receiver reports which fragments it holds (or the sender asks), and only the missing ones are re-sent. Both nodes must run alpha 0.1.4 or later (the wire format changed in 0.1.4). |
| Dead Hop Detection | Experimental | If the first repeater never echoes our frame, the attempt is abandoned early and a stale path is re-discovered in about 30 seconds instead of 4 minutes. |
| Airtime Duty Cycle | Working | Caps the interface at 30% airtime over a rolling 60 seconds, using real LoRa time-on-air at your radio's settings. Link keepalives skip the wait but still count against the cap. |
| Queue Hygiene | Working | Drops duplicate, stale and closed-link packets from the queue so a backlog isn't dumped onto the mesh when a path comes back, and forwards at most one spontaneous announce per destination every 5 minutes. |
| Predictive Transmit Holds | Basic | Uses overheard traffic to predict how long the channel stays busy and waits for it to clear. Off by default. |
| Parity Fragments | Experimental | On by default. From one MeshCore hop up, each burst of raw fragments ends with an XOR parity fragment, so a receiver that lost exactly one fragment rebuilds it instead of waiting for a retry round. Costs one extra fragment per burst; set `direct_raw_parity_enabled = no` to turn it off. |

This diagram isn't 100% accurate to how the interface works but should give you a basic idea :)
<img width="1056" height="600" alt="senddiagram" src="https://github.com/user-attachments/assets/f5efbb53-4530-4287-ad89-6438dbd2a88f" />

## Field Testing

This table summarizes the real world scenarios that I've tested the interface against.
Hops in this table refer to MeshCore Hops. Note - This is the results with airtime useage capped at 30% as is hard-coded and not configurable by design. A dynamic airtime usage cap is in the roadmap.

All tests were conducted on Heltec V3 MeshCore companions over a fairly quiet MeshCore network.

|                 | LXMF Messages (MeshChat) | Nomad Network |
| --------------- | ------------------------ | ------------- |
| 0 Hops (direct) | Works Well               | Works Well    |
| 1 Hop           | Works Well               | Works Well    |
| 2 Hops          | Works Well               | Works Well    |
| 3 Hops          | Works Well               | Not tested    |


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

The interface file is assembled: the source lives in `Interface/src/smci/` (one module per concern) and `python3 Interface/build_interface.py` builds `Interface/SmartMeshCoreInterface.py` from it. Edit the sources, run the build, and commit both; the unit suite is `python3 -m unittest discover -s tests`.

## AI Usage

This repo makes heavy use of Claude. The majority of code is written with Claude but is human validated.
Builds are field tested over real hardware.
