<div align="center">

  <img width="450" height="110" alt="Smart MeshCore Interface for Reticulum logo" src="https://github.com/user-attachments/assets/63093ecc-61b0-43d4-8db7-619bf3d7f8f3" />

  <h1>Smart MeshCore Interface for Reticulum</h1>

  <p>Run Reticulum (RNS) over a MeshCore LoRa mesh: send LXMF messages and browse NomadNet through MeshCore repeaters, without flooding your local MeshCore network.</p>
  
</div>

# What is Smart MeshCore Interface?

Smart MeshCore Interface is a custom Reticulum interface that uses a MeshCore
companion radio (e.g. Heltec V3) as a transport. It turns MeshCore into a
"last mile" RNS link using direct messages, fragmentation, and airtime limits,
so RNS traffic stays polite to regular MeshCore users.

This project aims to let you access Nomadnet and send LXMF messages over a MeshCore network as a 'last mile' RNS hop without flooding MeshCore with traffic. In summary this is achieved by using direct messages where possible, limiting traffic, only sending whats necessary, artificially delaying, and prioritizing some RNS traffic.

The project also aims to be as easy as possible to configure on your RNS nodes. In most situations, a minimal config is needed, just setup your MeshCore companion radio with the MeshCore app before using the interface.

Highlights:
- Doesn't flood the MeshCore network
- NomadNet accessible over MeshCore repeaters

# How to install the MeshCore interface for Reticulum

## Requirements

- A MeshCore-flashed LoRa radio (serial, BLE, or TCP-connected), reachable via the [`meshcore`](https://pypi.org/project/meshcore/) Python library. (Heltec V3, Heltec V4, RAK WisBlock, Seeed Studio SenseCAP, etc)

- [`meshcore`](https://pypi.org/project/meshcore/) Python library. (installed by install script)

- [Reticulum](https://pypi.org/project/rns/) (`pip install rns`).

- Python 3.9+.

## Install Script (Install Option A)

One liner command to install the interface (Linux only):

`curl -fsSL https://raw.githubusercontent.com/afit21/Reticulum-Smart-MeshCore-Interface/main/install-interface.sh | bash`

Update your RNS config file (see 'Configuration' section below)

## Manual (Install Option B)

Install [`meshcore`](https://pypi.org/project/meshcore/) Python library:
`pip install meshcore`

Copy the interface into your Reticulum install's interfaces directory:

```
cp Interface/SmartMeshCoreInterface.py ~/.reticulum/interfaces/SmartMeshCoreInterface.py
```


## How to configure the MeshCore interface

Add a block to `~/.reticulum/config`, under `[interfaces]`. A minimal real-world example. Please just use the reference config if you're not sure if your local MeshCore operators would be chill with you experimenting (serial-connected radio):

Reference config for a transfer node - by default, the interface will use the MeshCore settings saved to your companion:

Remember - The interface will use the radio options configured on your MeshCore companion.

```ini
  [[Smart MeshCore Interface]]
    type = SmartMeshCoreInterface
    interface_enabled = yes
    transport = serial
    port = /dev/ttyUSB0 #Please verify this is your MeshCore radio
    baudrate = 115200
    declares_upstream_rns = yes
    mode = access_point
    #packet_capture_enabled = yes
```

Reference config for a non-transfer node:

```ini
  [[Smart MeshCore Interface]]
    type = SmartMeshCoreInterface
    interface_enabled = yes
    transport = serial
    port = /dev/ttyUSB0 #Please verify this is your MeshCore radio
    baudrate = 115200
    #packet_capture_enabled = yes
```

Restart `rnsd` (or the app hosting your Reticulum instance) to pick it up. The full config surface (~140 options — retry budgets, timeouts, spacing tiers, duty cycle, RX-log behaviour, packet capture, etc.) is documented inline in the interface's own `_configure_*` methods. None of it is required; the defaults are what the field tests ran on.

# Features (Version 1.0.0)

**Battle Tested** - Tested and confident this is reliable.

**Working** - Working, but testing has been limited.

**Experimental** - Works most of the time, or unreliable over multiple MeshCore hops or other conditions.

**Unstable** - Works sometimes.

**Basic** - Only a bare-bones implementation exists. It may not be tested at all.

|    Feature    |    State    |        Description        |
|----|----|----|
| Automatic Link | Working | Automatically peers with other nodes running this interface, provided the companions share the same MeshCore settings. |
| Raw Binary Transport | Working | RNS packets are fragmented and sent over MeshCore in raw binary |
| Z85 Encode | Battle Tested | Encodes RNS packets in Z85 for transport over MeshCore text messages (~25% overhead vs ~35% for base64). This is used for channel traffic and as a backup when repeaters in a path aren't capable of forwarding raw binary messages |
| MeshCore Routing | Battle Tested | Broadcast traffic goes over a channel, direct traffic over direct messages. Keeps a cache of which RNS addresses route to each MeshCore contact, and with only a few known peers sends broadcast traffic as direct messages to avoid unnecessary floods. |
| RNS Packet Aware Firewalling | Working | Inspects outgoing traffic and drops what the mesh doesn't need, out of respect for MeshCore users. |
| Packet Aware Self-throttling | Working | Not just a speed limit: delays and prioritises packets by type, e.g. holding link-handshake packets so fewer keepalives are needed over the life of an RNS link. |
| Packet Capture Debug Tool | Battle Tested | Built-in packet capture for debugging the interface. Also logs every packet the radio overhears, not just our own. |
| Radio Traffic Awareness | Working | Taps the companion radio's raw RX log to see every packet it decodes, including traffic that isn't ours. Costs no airtime. |
| Adaptive ACK Timing | Working | Measures the real round trip to each peer and shortens ACK waits to match, never waiting longer than the hop-scaled ceiling. A missed ACK doubles the next wait rather than discarding the measurement. |
| Dead Hop Detection | Working | If the first repeater never echoes our frame, the attempt is abandoned early and a stale path is re-discovered in about 30 seconds instead of 4 minutes. |
| Airtime Duty Cycle | Working | Caps the interface at 30% airtime over a rolling 60 seconds, using real LoRa time-on-air at your radio's settings. Link keepalives skip the wait but still count against the cap. |
| Queue Hygiene | Working | Drops duplicate, stale and closed-link packets from the queue so a backlog isn't dumped onto the mesh when a path comes back, and forwards at most one spontaneous announce per destination every 5 minutes. |
| Parity Fragments | Working | On by default. From one MeshCore hop up, each burst of raw fragments ends with an XOR parity fragment, so a receiver that lost exactly one fragment rebuilds it instead of waiting for a retry round. Costs one extra fragment per burst; set `direct_raw_parity_enabled = no` to turn it off. |

This diagram isn't 100% accurate to how the interface works but should give you a basic idea :)
<img width="1056" height="600" alt="senddiagram" src="https://github.com/user-attachments/assets/f5efbb53-4530-4287-ad89-6438dbd2a88f" />

# Reliability in the field

More data needed. If you use this interface, please consider enabling packet capture and sending them my way.

In the current state, you can expect up to 3 repeaters in a path to be usable at the following or similar settings SF7, BW 62.5 kHz, CR 4/8, 916.575 MHz. (Data Rate 1.71kbps)
The main issue with routing over MeshCore is latency. Latency quickly adds up over repeaters.
Other radio settings are untested as of writing this, however, I suspect that settings with a higher resultant bitrate would work better in real scenarios. Please look at the contributing section if you'd like to test radio settings for me.

This table summarizes the real world scenarios that I've tested the interface against.
Hops in this table refer to the number of MeshCore repeaters.

All tests were conducted on Heltec V3 MeshCore companions over a fairly quiet MeshCore network.
MeshCore radio settings were: SF7, BW 62.5 kHz, CR 4/8, 916.575 MHz:

|                 | LXMF Messages (MeshChat) | Nomad Network |
| --------------- | ------------------------ | ------------- |
| 0 Hops (direct) | Works Well               | Works Well    |
| 1 Hop           | Works Well               | Works Well    |
| 2 Hops          | Works Well               | Works Well    |
| 3 Hops          | Slow               | Slow    |


Reference speeds - SF7, BW 62.5 kHz, CR 4/8, 916.575 MHz:
|         | One Direction | Bidirectional | RNS Latency |
| ------- | ------------- | ------------- | ----------- |
| 0 Hops  | 391bps        | 338 bps       | 1700ms      |
| 1 Hops  | Not tested    | Not tested  | Not tested  |
| 2 Hops  | 143 bps    | 100 bps  | 700ms to 1800ms  |


Please keep in mind that this project is in early stages. However, this interface currently works better than all others I've been able to test.

# Roadmap (in no particular order)

- Airtime efficiency improvements - Always looking to optimize airtime usage

- Dynamic airtime limiter (adjust airtime limit based on traffic)

- Improve security - Security so far has not been a focus.

- Option to automatically update MeshCore companion settings to optimize for this interface

- Compatibility with other MeshCore interfaces - I'd like to make this interface automatically detect other popular MeshCore interfaces and translate our own direct transmits to be able to speak with them

- Better accounting for radio conditions. This interface will adapt to radio conditions. More testing in the real world is required in-order to improve this capability.

- Better comparability with LXMF clients other than MeshChat


# Credits

- [comms-engineer/RNS_Over_Meshcore](https://github.com/comms-engineer/RNS_Over_Meshcore) — inspiration taken from this project for the discovery protocol.

- [Akita Engineering/Akita-Zmodem-MeshCore](https://github.com/AkitaEngineering/Akita-Zmodem-MeshCore/) — source code referenced for data transfer features.

- [Alex B](https://github.com/A13xB0) - Helpful advice

- [MeshBench](https://meshbench.github.io/) - MeshCore simulator used for simulated benchmarks

# Contributing
Packet captures from your field tests are always appreciated:

Enable packet captures with:
``` ini
  [[Smart MeshCore Interface]]
    packet_capture_enabled = yes
```

The capture file is named after your MeshCore node (`<node name>_capture_..._<timestamp>.jsonl`); set `packet_capture_label = <name>` to use a different label.
Default location is; . ~/.reticulum/storage/meshcore_packet_capture/

You can send these to me by opening an issue, discord at 'afit21', or my LXMF: d4c70c4b0a7e67265fa8982e36b43c05

If you'd like to contribute code, feel free to fork this repo and open a pull request :)

# Speed limited out of respect for MeshCore users

TLDR: Please Respect MeshCore Users (don't remove airtime limiters)


This project intentionally caps performance out of respect for the regular MeshCore users. In the current version I have airtime capped at 30% (85% for zero hop) which results in a usable experience.
As more testing is done, the project will move towards dynamic airtime limiting, however please don't remove the limits unless you know what you're doing or your local user base is fine with it.

## AI Usage

This repo makes heavy use of Claude. The majority of code is written with Claude but is human validated.
Builds are field tested over real hardware.
