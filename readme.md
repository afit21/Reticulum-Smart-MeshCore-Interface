# Smart MeshCore Interface

### Overview

An [Reticulum](https://reticulum.network/) (RNS) interface that lets RNS nodes communicate over a [MeshCore](https://meshcore.co.uk/) LoRa mesh without nuking your local MeshCore network!

This project aims to let you access Nomadnet and send LXMF messages over a MeshCore network as a 'last mile' RNS hop without flooding MeshCore with traffic. In summary this is achieved by firewalling traffic entering the interface, prioritizing, artificially delaying some traffic, and making decisions on how to efficiently rout traffic over MeshCore.

The project also aims to be as easy as possible to configure on your RNS nodes. In most situations, a minimal config is needed, just setup your MeshCore companion radio with the MeshCore app before using the interface.

This diagram isn't 100% accurate to how the interface works but should give you a basic idea :)
<img width="1156" height="700" alt="senddiagram" src="https://github.com/user-attachments/assets/f5efbb53-4530-4287-ad89-6438dbd2a88f" />


## TLDR: Please Respect MeshCore Users (don't remove airtime limiters)

This project intentionally caps performance out of respect for the regular MeshCore users. Airtime is capped over a rolling 60 second window, with two numbers since alpha 0.1.5:

- **30%** for anything a repeater relays: multi-hop direct traffic and every channel broadcast (announces, path requests, peer discovery). This is the number that matters to everyone else on the mesh, and it is not going up.
- **85%** for zero-hop direct traffic between two adjacent radios. A frame that no repeater carries costs nobody else's infrastructure any air, and the field tests showed the 30% cap, not the radio, was the ceiling there (a zero-hop page transfer spent 109 of 147 seconds waiting on it).

The 30% is `duty_cycle_max_fraction`, the 85% is `duty_cycle_max_fraction_zero_hop`; both are counted against the same window, and multi-hop traffic counts against both.

As more data is collected, this can become more dynamic, based on factors such as the mesh's radio settings.

As this project is under a GPL license, there is nothing stopping you from lifting the caps in your own fork, however I would consider doing this on an established MeshCore Mesh for the sake of personal performance without testing wreckless and disrespectful to those who contribute to the infrustrucure you're using.

In the future I plan on making this interface hostile to other peers transmitting more than their fair share to discourage this.

## Features (Version alpha0.1.7)

In short, this version lets you send LXMF messages and browse NomadNet sites over MeshCore. It has been tested over 1, 2 and 3 MeshCore repeater hops with two RNS nodes communicating over this interface. See the [testing section](#field-testing) for more details.

New in alpha 0.1.7 (no wire change; alpha 0.1.6 and 0.1.7 nodes work together):

- a delivery proof answering a message that just arrived goes ahead of bulk traffic on the radio, the way a link handshake does, so LXMF stops re-sending messages that were already delivered (`proof_fresh_s`, default 8 s; 0 turns it off).
- the interface no longer learns a route to its own destinations from the messages addressed to them.
- capture records carry the other node's reported path length and delivery rate, and the field comparison script reports proof turnaround and duplicate deliveries.

New in alpha 0.1.6 (**wire change: both nodes must run alpha 0.1.6** -- the fragment-reconciliation frames now carry each node's view of its path to the other):

- path selection by measured reliability replaces shorter-path adoption: every route a node learns to a peer (discovered, seen on the peer's own floods, direct, or reported by the peer) is scored by airtime per delivered byte from its measured delivery rate, a delivering path is kept, a path that misses twice is trialled against the best alternative, and a weak direct signal is scored below a repeater path (`path_selection_enabled`, `path_weak_snr_db`, `path_switch_after_misses`, `path_switch_margin`, `path_switch_cooldown`; `path_adopt_window` is gone, `path_adopt_enabled` still works as an alias).
- through repeaters a fragment window runs at most two reconcile rounds before falling back (`direct_raw_window_max_rounds`), a link proof the other side has already re-requested is dropped instead of retried, and a completion report owed to the other side also goes out during a reconcile query's quiet hold -- link handshakes at two hops no longer wait a minute or more for the radio.
- a resilient serial connection: the interface opens the port itself, waits out the radio's reset, retries the handshake, and reconnects forever with backoff after a USB drop or a laptop suspend instead of going dead after three seconds; a garbled frame from the radio no longer fails the command in flight ("event failed"), and it warns when another process is reading the same port (`connect_retry_min`, `connect_retry_max`, `serial_open_settle`, `handshake_attempts`, `handshake_timeout`, `command_timeout`, `serial_noise_warn_per_min`; `max_reconnect_attempts` now means the supervisor's cap, 0 = forever).
- one completion report per fragment window at zero hop: the receiver's gaps report waits a full fragment spacing and is dropped when the last fragment lands inside it, and the same packet is no longer reported twice.

New in alpha 0.1.5 (no wire change from 0.1.4; both nodes should still run the same build):

- zero-hop direct traffic may use up to 85% of channel time while anything a repeater relays stays at 30% (see the airtime section above; `duty_cycle_max_fraction_zero_hop`).
- the sender tracks when its radio has actually finished transmitting a burst, paces zero-hop bursts to the radio (`direct_raw_burst_queue_ahead`), the receiver sends one completion report per window instead of one per part (`direct_report_hold_during_burst`), and a report that arrives early is progress rather than the end of the wait -- the field session's unnecessary re-sends are what this removes.
- shorter-path adoption from a peer's own floods (`path_adopt_enabled`, `path_adopt_window`).
- a lone packet no longer waits the window-collect time (`direct_raw_window_collect` is now a maximum), and a completion report owed to the other side goes out between the parts of this node's own window.
- capture files are named after the MeshCore node (`packet_capture_label`), the radio's own transmit statistics are recorded (`radio_stats_interval`), and `direct_raw_gap_own_airtime` exists for the one-hop gap field A/B (default unchanged).

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
| Z85 Encode | Battle Tested | Encodes RNS packets in Z85 for transport over MeshCore text messages (~25% overhead vs ~35% for base64). |
| MeshCore Routing | Experimental | Broadcast traffic goes over a channel, direct traffic over direct messages. Keeps a cache of which RNS addresses route to each MeshCore contact, and with only a few known peers sends broadcast traffic as direct messages to avoid unnecessary floods. |
| RNS Packet Aware Firewalling | Experimental | Inspects outgoing traffic and drops what the mesh doesn't need, out of respect for MeshCore users. |
| Packet Aware Self-throttling | Working | Not just a speed limit: delays and prioritises packets by type, e.g. holding link-handshake packets so fewer keepalives are needed over the life of an RNS link. |
| Packet Capture Debug Tool | Battle Tested | Built-in packet capture for debugging the interface. Also logs every packet the radio overhears, not just our own. |
| Radio Traffic Awareness | Working | Taps the companion radio's raw RX log to see every packet it decodes, including traffic that isn't ours. Costs no airtime. |
| Adaptive ACK Timing | Working | Measures the real round trip to each peer and shortens ACK waits to match, never waiting longer than the hop-scaled ceiling. A missed ACK doubles the next wait rather than discarding the measurement. |
| Fragment Reconciliation | Experimental | After sending a fragmented packet, the receiver reports which fragments it holds (or the sender asks), and only the missing ones are re-sent. Both nodes must run alpha 0.1.6 (the wire format changed in 0.1.4 and again in 0.1.6). |
| Dead Hop Detection | Experimental | If the first repeater never echoes our frame, the attempt is abandoned early and a stale path is re-discovered in about 30 seconds instead of 4 minutes. |
| Airtime Duty Cycle | Working | Caps the interface's airtime over a rolling 60 seconds, using real LoRa time-on-air at your radio's settings: 30% for anything a repeater relays (`duty_cycle_max_fraction`), 85% for zero-hop direct traffic (`duty_cycle_max_fraction_zero_hop`). Link keepalives skip the wait but still count against the cap. |
| Queue Hygiene | Working | Drops duplicate, stale and closed-link packets from the queue so a backlog isn't dumped onto the mesh when a path comes back, and forwards at most one spontaneous announce per destination every 5 minutes. |
| Predictive Transmit Holds | Basic | Uses overheard traffic to predict how long the channel stays busy and waits for it to clear. Off by default. |
| Raw Binary Fragments | Experimental | On by default. Packets too large for one text message go to a capable peer as MeshCore raw binary packets: no Z85, no text framing, no per-fragment ACK (about 43% less sender airtime). Reliability comes from Fragment Reconciliation; if raw never arrives on a path but text does, that path falls back to text for a while. Older peers still get text fragments. **Privacy note:** MeshCore raw packets are not encrypted or authenticated by the firmware. Your contents are still end-to-end encrypted by Reticulum, but the RNS packet header and both nodes' key prefixes are visible on air, and a third party could inject a fragment (Reticulum rejects it, but the transfer has to be re-sent). Set `direct_raw_fragments_enabled = no` to stay fully inside MeshCore's encryption. |
| Path Selection | Experimental | On by default. Keeps a scoreboard of every route it knows to a peer -- the discovered path, routes seen on the peer's own MeshCore floods, the direct link, and the path the peer reports -- scored by airtime per delivered byte from the measured delivery rate. A path that is delivering is kept; after two missed sends the next packet tries the best-scoring alternative, and a switch is made for good only when it clearly wins. A direct link heard below `path_weak_snr_db` (3 dB) is scored below a repeater path. `path_selection_enabled = no` turns it off; `path_switch_after_misses` (2), `path_switch_margin` (0.25), `path_switch_cooldown` (120 s) tune it. |
| Parity Fragments | Experimental | On by default. From one MeshCore hop up, each burst of raw fragments ends with an XOR parity fragment, so a receiver that lost exactly one fragment rebuilds it instead of waiting for a retry round. Costs one extra fragment per burst; set `direct_raw_parity_enabled = no` to turn it off. |

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


## Field Testing

This table summarizes the real world scenarios that I've tested the interface against.
Hops in this table refer to MeshCore Hops. Note - These results were measured with airtime capped at 30% for everything (the alpha 0.1.4 cap); since alpha 0.1.5 zero-hop direct traffic may use 85% while anything a repeater relays stays at 30% (see the airtime section above).

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

The capture file is named after your MeshCore node (`<node name>_capture_..._<timestamp>.jsonl`); set `packet_capture_label = <name>` to use a different label.

Feel free to contribute code if you'd like to by opening a pull request :)

The interface file is assembled: the source lives in `Interface/src/smci/` (one module per concern) and `python3 Interface/build_interface.py` builds `Interface/SmartMeshCoreInterface.py` from it. Edit the sources, run the build, and commit both; the unit suite is `python3 -m unittest discover -s tests`.

## AI Usage

This repo makes heavy use of Claude. The majority of code is written with Claude but is human validated.
Builds are field tested over real hardware.
