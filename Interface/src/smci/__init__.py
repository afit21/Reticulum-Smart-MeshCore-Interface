"""
SmartMeshCoreInterface.py -- Smart Meshcore Interface for Reticulum

An `RNS.Interfaces.Interface` subclass that carries Reticulum (RNS) traffic
over a MeshCore LoRa mesh. This is a from-scratch rebuild -- see CLAUDE.md
and `docs/history.md` (the dated design record, read that first) for why
the previous implementation
(`referenceprojects/MeshCore_Dynamic_Interface_old.py` /
`_original_repo.py`) was retired rather than extended: it accumulated real,
working functionality on top of one wrong assumption about MeshCore
(reliable multi-packet CHANNEL delivery) that field testing showed cannot be
tuned into working. Nothing in this file is built on that code; it is a
fresh implementation against the design docs, referring back to the old
implementation only as a record of what was tried and why it didn't work.

STATUS -- alpha 0.1.3 plus the 2026-09-20 airtime / throughput pass; the
dated account of every design decision and field-driven fix from alpha
0.1.0 (2026-09-15) onward is `docs/history.md` (moved out of this
docstring on 2026-09-20, phase 2 of that pass, unchanged), with the
summary in `changelog.md`. The deliverable `Interface/SmartMeshCoreInterface.py`
is ASSEMBLED from the package `Interface/src/smci/` by
`Interface/build_interface.py` (RNS `exec()`s a custom interface as one
file, so a package cannot be installed as such -- see
`testscripts/check_install_load.py`); edit the sources, run the build,
commit both. `testscripts/audit_split.py` checks a rebuild is a pure move.

WIRE FORMAT (the golden snapshot `tests/golden/wire_format.json`, replayed
by `tests/test_golden_wire_format.py`, pins every byte below; a deliberate
change regenerates it in the same commit):

  Text frames ride MeshCore text messages -- `send_msg` (TXT_MSG) for
  DIRECT, `send_chan_msg` (GRP_TXT) for CHANNEL -- as one ASCII marker
  character followed by the Z85 encoding of a binary body. Payload
  budgets per frame come from FIRMWARE_TEXT_LIMIT (160) less the marker,
  Z85's 5/4 expansion, the header and PAYLOAD_MARGIN (4); CHANNEL also
  loses the firmware's own "<name>: " prefix.

  "R" (MARKER) -- an RNS packet, or one fragment of it. Byte 0 is
  PROTOCOL_VERSION (1) in the low six bits (VERSION_MASK 0x3F) with
  FLAG_MULTI_FRAGMENT (0x80) and FLAG_COOP (0x40, reserved):
    CHANNEL single frame  [ver][pkt_id:2 BE][attempt]            (4 B)
    DIRECT single frame   [ver]                                  (1 B)
    multi-fragment, both  [ver|0x80][pkt_id:2 BE][frag_idx][frag_total][attempt]  (6 B)
  followed by the RNS payload bytes. frag_total is 1..255; every attempt
  of a fragment carries the same payload bytes (the receiver evicts a
  bucket whose repeated index differs). Reassembly is keyed
  (mode, sender token, pkt_id, frag_total).

  "P" (PEER_MARKER) -- a bind frame, peer discovery over CHANNEL:
    [BIND_PROTOCOL_VERSION 1][type: 0 REQUEST / 1 RESPONSE][cap][attempt][pubkey_prefix:6]
  (BIND_FRAME_RAW_SIZE 10). cap bits: BIND_CAP_HAS_UPSTREAM_RNS 0x01,
  BIND_CAP_RAW_FRAGMENTS 0x02.

  "Q" (COMPLETION_MARKER) -- the DIRECT-only completion QUERY / ANSWER /
  REPORT (the have-bitmap reconcile):
    [version][type: 0 QUERY / 1 ANSWER][complete: 0/1][pkt_id:2 BE][frag_total]
    v3 adds  [nonce]                                        (after frag_total)
    v2+ ANSWER adds the have-bitmap, ceil(frag_total / 8) bytes, bit i = fragment i held
  COMPLETION_PROTOCOL_VERSION is 3; v1 and v2 frames still decode and a
  v1 QUERY is answered in v1. A QUERY's nonce cycles 1..0xEF
  (COMPLETION_QUERY_NONCE_MAX) and its ANSWER echoes it; a receiver-
  initiated REPORT is an ANSWER with nonce 0xF0 | round
  (COMPLETION_REPORT_NONCE_BASE), round being the raw header's attempt
  bits. A pre-v3 peer drops a v3 QUERY, so both nodes must run a v3
  build for reconciliation to work.

  Raw binary DIRECT fragments -- `send_raw_data` (PAYLOAD_TYPE_RAW_CUSTOM,
  no text framing, no firmware encryption, no firmware ACK), RAW_HEADER_SIZE
  13 bytes then the RNS payload chunk:
    [RAW_PROTOCOL_VERSION 1 << 4 | RAW_FLAG_REPORT 0x04 | attempt & 0x03]
    [dst_pubkey_prefix:2][src_pubkey_prefix:6][pkt_id:2 BE][frag_idx][frag_total]
  RAW_FLAG_REPORT marks the last two fragments of a burst (the receiver
  reports when one lands). Per-fragment payload is
  min(direct_raw_payload_cap, FIRMWARE_RAW_RX_PAYLOAD_LIMIT 172,
  FIRMWARE_RAW_TX_FRAME_LIMIT 174 - path_len) - 13: 157 bytes at the
  shipped cap of 170. The firmware dedups raw packets by content, so no
  two transmissions of a fragment may be byte-identical -- the attempt
  bits change per round (at most 4 rounds). Raw fragments land in the
  same reassembly bucket as text fragments from that sender and are
  reconciled by the same "Q" frames.

DESIGN INVARIANTS (carried forward from the prior implementation's own
field-diagnosed lessons, restated here per CLAUDE.md; the full justification
for each lives in `docs/meshcore_protocol_rules.md`'s "meshcore Python
library's own send/ACK contract" section and
`docs/reliability_engine_design.md`'s "RNS base-class contract" implementation
notes):

  1. Never trust a bare `await` of a `self._mc.commands.foo(...)` call to
     mean success. `send_chan_msg` resolves against [OK, ERROR] and never
     returns a delivery confirmation (CHANNEL has no ACK at all); `send_msg`
     resolves against [MSG_SENT, ERROR] and MSG_SENT itself only confirms
     the local radio queued the frame, not that it was delivered. Route
     every command's result through `_run_command()`, which checks the
     *actual* event type returned against what that specific call expected,
     and raises on `EventType.ERROR`, no response, or an unexpected type --
     never on "did it return without throwing."

  2. Never wait on a bare event TYPE when more than one in-flight operation
     could produce it. The `meshcore` library's `CommandHandlerBase.send()`
     matches a reply purely by event type, with no per-request correlation
     id -- two concurrent commands both waiting on the same type can steal
     each other's reply. `_run_command()` serializes every actual
     `commands.*()` call behind `self._command_lock` so only one command is
     ever in flight and awaiting its reply at a time. A longer-lived wait
     for something that can legitimately take tens of seconds (a DIRECT
     delivery ACK, once M4+ adds it) must be a persistent subscription
     matched by content identity, never a second concurrent bare-type wait
     that could collide with the next command's own reply.

  3. Every config value that's read via `cfg.get(...)` in a `_configure_*`
     method must be used somewhere else in this file, or a user setting it
     gets no error and no effect -- the exact bug class
     (`firmware_text_limit` parsed, documented, and never read) this
     project already hit once. No automated check enforces this: the AST
     scan this entry once cited (`tests/test_smart_meshcore_interface_
     config.py`) was never committed. Verify by hand when adding a key.
"""

from .interface import SmartMeshCoreInterface, interface_class  # noqa: F401,E402
