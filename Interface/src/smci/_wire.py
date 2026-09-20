"""Wire format: payload budgets, the R / P / Q text-frame encoders and decoders, the raw binary fragment header, payload chunking and spacing tiers, and the RNS header parse plus the packet classifications derived from it (priority tier, link class, handshake class, plain proof, path-request target). Every encoded byte is pinned by tests/golden/wire_format.json."""
from typing import Optional

import RNS

from ._common import _z85_encode, _z85_decode, _FrameHeader, _RnsHeader, _BindFrame, _CompletionFrame


class _WireFormatMixin:
    # -------------------------------------------------------------------
    # Wire format (docs/wire_format_design.md): payload budgets and
    # frame encode/decode
    # -------------------------------------------------------------------

    def _payload_budget(self, budget: int, header_size: int) -> int:
        """The general form from wire_format_design.md's payload-budget
        section, re-derived from the Z85 sizing relationship. `MARKER`'s
        length is deliberately a symbolic term here, never a bare literal
        -- the bug that section's own history warns against is a future
        marker-length change silently desyncing a hard-coded constant
        from the format it's supposed to be sizing."""
        raw = ((budget - len(self.MARKER) - 1) // 5) * 4 - header_size - self.PAYLOAD_MARGIN
        return max(0, raw)

    def _channel_text_budget(self) -> int:
        # "<name>: " is the firmware's own mandatory CHANNEL prefix
        # (meshcore_protocol_rules.md CHANNEL rule 2) -- eats into the
        # budget regardless of what this interface puts in the message,
        # or which of CHANNEL's two header shapes ends up using it.
        return self.FIRMWARE_TEXT_LIMIT - len(self._own_node_name) - 2

    def _channel_payload_budget(self) -> int:
        return self._payload_budget(self._channel_text_budget(), self.CHANNEL_FASTPATH_HEADER_SIZE)

    def _channel_multifragment_payload_budget(self) -> int:
        # Per-fragment usable payload when frag_total > 1 -- smaller than
        # the fast-path budget above by exactly the extra 2 header bytes
        # (frag_idx, frag_total) the multi-fragment shape carries.
        return self._payload_budget(self._channel_text_budget(), self.MULTI_FRAGMENT_HEADER_SIZE)

    def _direct_payload_budget(self) -> int:
        # DIRECT text framing has no name-prefix cost at all
        # (meshcore_protocol_rules.md DIRECT rule 1) -- full firmware
        # limit is available.
        return self._payload_budget(self.FIRMWARE_TEXT_LIMIT, self.DIRECT_BARE_HEADER_SIZE)

    def _direct_multifragment_payload_budget(self) -> int:
        # Per-fragment usable payload for DIRECT's needs-fragmenting shape
        # (Milestone 6) -- same MULTI_FRAGMENT_HEADER_SIZE as CHANNEL's
        # multi-fragment shape (wire_format_design.md: "identical shape to
        # the CHANNEL one above"), but no name-prefix cost, same as the
        # bare DIRECT budget above.
        return self._payload_budget(self.FIRMWARE_TEXT_LIMIT, self.MULTI_FRAGMENT_HEADER_SIZE)

    def _encode_channel_fastpath(self, payload: bytes, pkt_id: int, attempt: int = 0) -> str:
        header = bytes([self.PROTOCOL_VERSION]) + pkt_id.to_bytes(2, "big") + bytes([attempt])
        return self.MARKER + _z85_encode(header + payload)

    def _encode_channel_multifragment(
        self, payload: bytes, pkt_id: int, frag_idx: int, frag_total: int, attempt: int = 0
    ) -> str:
        header = (
            bytes([self.PROTOCOL_VERSION | self.FLAG_MULTI_FRAGMENT])
            + pkt_id.to_bytes(2, "big")
            + bytes([frag_idx, frag_total, attempt])
        )
        return self.MARKER + _z85_encode(header + payload)

    def _encode_direct_bare(self, payload: bytes) -> str:
        header = bytes([self.PROTOCOL_VERSION])
        return self.MARKER + _z85_encode(header + payload)

    def _decode_frame(self, marker_and_body: str, mode: str) -> "tuple[_FrameHeader, bytes]":
        """Decodes one CHANNEL or DIRECT frame's header (`mode` is
        "channel" or "direct" -- determines the fast-path/bare header
        shape when the multi-fragment bit is clear; the multi-fragment
        shape itself is identical between the two modes). Raises
        ValueError on anything malformed -- a missing marker, invalid
        Z85, an unsupported version, or a frame too short for the header
        its own flag bits claim. Per wire_format_design.md's own
        reasoning for the 1-character marker: a false-positive marker
        match on ordinary chat traffic is expected to happen sometimes,
        and costs exactly one cheap, local decode-and-reject here -- the
        caller logs and drops on ValueError, nothing more."""
        if not marker_and_body.startswith(self.MARKER):
            raise ValueError("missing marker")

        raw = _z85_decode(marker_and_body[len(self.MARKER):])
        if len(raw) < 1:
            raise ValueError("empty frame after marker")

        ver_byte = raw[0]
        version_number = ver_byte & self.VERSION_MASK
        multi_fragment = bool(ver_byte & self.FLAG_MULTI_FRAGMENT)
        coop = bool(ver_byte & self.FLAG_COOP)

        if version_number != self.PROTOCOL_VERSION:
            raise ValueError(f"unsupported protocol version {version_number}")

        if multi_fragment:
            if len(raw) < self.MULTI_FRAGMENT_HEADER_SIZE:
                raise ValueError("frame too short for multi-fragment header")
            pkt_id = int.from_bytes(raw[1:3], "big")
            frag_idx = raw[3]
            frag_total = raw[4]
            attempt = raw[5]
            if frag_total < 1 or frag_idx >= frag_total:
                # Malformed or hostile (this interface's security model
                # assumes any transmitter on the shared channel can send
                # anything): reject here, at decode time, rather than let
                # it reach reassembly, where an out-of-range frag_idx set
                # that happens to satisfy len(fragments) == frag_total
                # would raise an unhandled KeyError trying to join indices
                # that were never actually stored.
                raise ValueError(
                    f"invalid frag_idx/frag_total: {frag_idx}/{frag_total}"
                )
            payload = bytes(raw[self.MULTI_FRAGMENT_HEADER_SIZE:])
            return (
                _FrameHeader(version_number, True, coop, pkt_id, frag_idx, frag_total, attempt),
                payload,
            )

        if mode == "channel":
            if len(raw) < self.CHANNEL_FASTPATH_HEADER_SIZE:
                raise ValueError("frame too short for CHANNEL fast-path header")
            pkt_id = int.from_bytes(raw[1:3], "big")
            attempt = raw[3]
            payload = bytes(raw[self.CHANNEL_FASTPATH_HEADER_SIZE:])
            return (
                _FrameHeader(version_number, False, coop, pkt_id, 0, 1, attempt),
                payload,
            )

        # mode == "direct": bare shape -- no pkt_id/attempt at all, the
        # firmware's own ACK/content-derived-attempt cycle covers it.
        payload = bytes(raw[self.DIRECT_BARE_HEADER_SIZE:])
        return _FrameHeader(version_number, False, coop, None, 0, 1, None), payload

    # -- Bind frames (docs/peer_discovery_design.md §1) -- a separate
    # control protocol, distinct from the "R"-marker RNS wire format above:
    # different marker, no relationship to _decode_frame's shapes.

    def _bind_capability(self) -> int:
        cap = 0
        if self.declares_upstream_rns:
            cap |= self.BIND_CAP_HAS_UPSTREAM_RNS
        if self.direct_raw_fragments_enabled:
            cap |= self.BIND_CAP_RAW_FRAGMENTS
        return cap

    def _own_pubkey_prefix(self) -> Optional[str]:
        if len(self._own_pubkey_hex) < self.BIND_PUBKEY_PREFIX_BYTES * 2:
            return None
        return self._own_pubkey_hex[: self.BIND_PUBKEY_PREFIX_BYTES * 2]

    def _encode_bind_frame(self, frame_type: int, attempt: int) -> str:
        own_prefix = self._own_pubkey_prefix()
        prefix_bytes = (
            bytes.fromhex(own_prefix) if own_prefix is not None
            else b"\x00" * self.BIND_PUBKEY_PREFIX_BYTES
        )
        body = (
            bytes([self.BIND_PROTOCOL_VERSION, frame_type, self._bind_capability(), attempt & 0xFF])
            + prefix_bytes
        )
        return self.PEER_MARKER + _z85_encode(body)

    def _decode_bind_frame(self, marker_and_body: str) -> _BindFrame:
        if not marker_and_body.startswith(self.PEER_MARKER):
            raise ValueError("missing bind-frame marker")
        raw = _z85_decode(marker_and_body[len(self.PEER_MARKER):])
        if len(raw) != self.BIND_FRAME_RAW_SIZE:
            raise ValueError(f"bind frame wrong length: {len(raw)} (expected {self.BIND_FRAME_RAW_SIZE})")

        version, frame_type, cap, attempt = raw[0], raw[1], raw[2], raw[3]
        if version != self.BIND_PROTOCOL_VERSION:
            raise ValueError(f"unsupported bind-frame version {version}")
        if frame_type not in (self.BIND_TYPE_REQUEST, self.BIND_TYPE_RESPONSE):
            raise ValueError(f"unrecognized bind-frame type {frame_type}")

        pubkey_prefix = raw[4:4 + self.BIND_PUBKEY_PREFIX_BYTES].hex()
        return _BindFrame(version=version, type=frame_type, cap=cap, attempt=attempt, pubkey_prefix=pubkey_prefix)

    # --- DIRECT-fragmented completion check ("Q" marker) -----------------
    # Own control protocol, distinct from both "R" (RNS wire format) and
    # "P" (bind frames): different marker, no relationship to either's
    # shape.

    @staticmethod
    def _completion_bitmap_size(frag_total: int) -> int:
        return (max(0, frag_total) + 7) // 8

    def _encode_completion_frame(
        self, frame_type: int, pkt_id: int, frag_total: int, complete: bool = False,
        held: "Optional[set]" = None, version: Optional[int] = None,
        nonce: Optional[int] = None,
    ) -> str:
        """`version` defaults to this build's own (v2). Passing
        `COMPLETION_PROTOCOL_VERSION_V1` produces the pre-step-3 fixed-body
        frame -- used to answer a v1 QUERY in kind. `held` is only encoded
        on a v2 ANSWER; `complete` is carried by both versions (redundant
        with an all-ones bitmap on v2, kept so a v2 reader never has to
        infer it)."""
        if version is None:
            version = self.COMPLETION_PROTOCOL_VERSION
        body = bytes([
            version,
            frame_type,
            1 if complete else 0,
            (pkt_id >> 8) & 0xFF,
            pkt_id & 0xFF,
            frag_total & 0xFF,
        ])
        if version >= 3:
            body += bytes([(nonce or 0) & 0xFF])
        if version >= 2 and frame_type == self.COMPLETION_TYPE_ANSWER:
            bitmap = bytearray(self._completion_bitmap_size(frag_total))
            for idx in (held or ()):
                if 0 <= idx < frag_total:
                    bitmap[idx // 8] |= 1 << (idx % 8)
            body += bytes(bitmap)
        return self.COMPLETION_MARKER + _z85_encode(body)

    def _decode_completion_frame(self, marker_and_body: str) -> _CompletionFrame:
        if not marker_and_body.startswith(self.COMPLETION_MARKER):
            raise ValueError("missing completion-frame marker")
        raw = _z85_decode(marker_and_body[len(self.COMPLETION_MARKER):])
        if len(raw) < self.COMPLETION_FRAME_RAW_SIZE:
            raise ValueError(f"completion frame too short: {len(raw)} (expected >= {self.COMPLETION_FRAME_RAW_SIZE})")

        version, frame_type, complete_byte = raw[0], raw[1], raw[2]
        if version not in (
            self.COMPLETION_PROTOCOL_VERSION_V1, self.COMPLETION_PROTOCOL_VERSION_V2,
            self.COMPLETION_PROTOCOL_VERSION,
        ):
            raise ValueError(f"unsupported completion-frame version {version}")
        if frame_type not in (self.COMPLETION_TYPE_QUERY, self.COMPLETION_TYPE_ANSWER):
            raise ValueError(f"unrecognized completion-frame type {frame_type}")

        pkt_id = (raw[3] << 8) | raw[4]
        frag_total = raw[5]
        held = None
        nonce = None
        body_size = self.COMPLETION_FRAME_RAW_SIZE
        if version >= 3:
            if len(raw) < body_size + 1:
                raise ValueError("v3 completion frame too short for its nonce")
            nonce = raw[body_size]
            body_size += 1
        if version >= 2 and frame_type == self.COMPLETION_TYPE_ANSWER:
            expected = body_size + self._completion_bitmap_size(frag_total)
            if len(raw) != expected:
                raise ValueError(f"completion ANSWER wrong length: {len(raw)} (expected {expected} for frag_total={frag_total})")
            bitmap = raw[body_size:]
            held = frozenset(i for i in range(frag_total) if bitmap[i // 8] & (1 << (i % 8)))
        elif len(raw) != body_size:
            raise ValueError(f"completion frame wrong length: {len(raw)} (expected {body_size})")
        return _CompletionFrame(
            version=version, type=frame_type, complete=bool(complete_byte),
            pkt_id=pkt_id, frag_total=frag_total, held=held, nonce=nonce,
        )

    # --- Raw binary DIRECT fragments (2026-09-18 night, module docstring) ---

    def _direct_raw_payload_budget(self, path_len: int) -> int:
        """RNS payload bytes per raw fragment for a path of `path_len`
        bytes: the smaller of the configured cap, the firmware's receive
        push limit and its send-frame limit less the path, minus our
        13-byte header. 157 at zero hop with the defaults."""
        cap = min(self.direct_raw_payload_cap, self.FIRMWARE_RAW_RX_PAYLOAD_LIMIT,
                  self.FIRMWARE_RAW_TX_FRAME_LIMIT - max(0, path_len))
        return max(0, cap - self.RAW_HEADER_SIZE)

    def _encode_raw_fragment(
        self, payload: bytes, dst_pubkey_hex: str, src_prefix_hex: str,
        pkt_id: int, frag_idx: int, frag_total: int, attempt: int, report: bool = False,
    ) -> bytes:
        header = (
            bytes([(self.RAW_PROTOCOL_VERSION << 4) | (attempt & 0x03) | (self.RAW_FLAG_REPORT if report else 0)])
            + bytes.fromhex(dst_pubkey_hex[: self.RAW_DST_PREFIX_BYTES * 2])
            + bytes.fromhex(src_prefix_hex[: self.BIND_PUBKEY_PREFIX_BYTES * 2])
            + pkt_id.to_bytes(2, "big")
            + bytes([frag_idx & 0xFF, frag_total & 0xFF])
        )
        return header + payload

    def _raw_fragment_report_requested(self, raw: bytes) -> bool:
        """Whether byte 0 of a raw fragment carries RAW_FLAG_REPORT (the
        burst's last fragment, 2026-09-20). Read separately from
        `_decode_raw_fragment` so the decoder's 4-tuple contract, and the
        tests pinning it, stay unchanged."""
        return bool(raw) and bool(raw[0] & self.RAW_FLAG_REPORT)

    def _decode_raw_fragment(self, raw: bytes) -> "tuple[_FrameHeader, bytes, str, bytes]":
        """Returns (header, payload, src_prefix_hex, dst_prefix_bytes).
        Raises ValueError for anything that isn't one of ours -- callers
        drop those silently, since other applications' raw packets share
        this payload type. Bit 2 of byte 0 (RAW_FLAG_REPORT) is ignored
        here; see `_raw_fragment_report_requested`."""
        if len(raw) < self.RAW_HEADER_SIZE:
            raise ValueError("too short for a raw fragment header")
        if (raw[0] >> 4) != self.RAW_PROTOCOL_VERSION:
            raise ValueError(f"raw version nibble {raw[0] >> 4} is not ours")
        attempt = raw[0] & 0x03
        dst = raw[1:1 + self.RAW_DST_PREFIX_BYTES]
        i = 1 + self.RAW_DST_PREFIX_BYTES
        src_prefix_hex = raw[i:i + self.BIND_PUBKEY_PREFIX_BYTES].hex()
        i += self.BIND_PUBKEY_PREFIX_BYTES
        pkt_id = int.from_bytes(raw[i:i + 2], "big")
        frag_idx, frag_total = raw[i + 2], raw[i + 3]
        if frag_total < 1 or frag_idx >= frag_total:
            raise ValueError(f"invalid frag_idx/frag_total: {frag_idx}/{frag_total}")
        header = _FrameHeader(self.PROTOCOL_VERSION, True, False, pkt_id, frag_idx, frag_total, attempt)
        return header, bytes(raw[self.RAW_HEADER_SIZE:]), src_prefix_hex, bytes(dst)

    def _next_pkt_id(self) -> int:
        # Only ever called from this interface's own dedicated event loop
        # (via _send_channel, itself only invoked by _outgoing_worker
        # running on that same loop) -- no lock needed, since coroutines
        # on one asyncio loop never run concurrently with each other.
        pkt_id = self._pkt_id_counter
        self._pkt_id_counter = (self._pkt_id_counter + 1) & 0xFFFF
        return pkt_id

    def _chunk_payload(self, data: bytes, per_fragment: int) -> list:
        """Shared chunking body for `_fragment_payload`/
        `_fragment_direct_payload` below -- code-review fix: these two
        used to carry byte-identical bodies, differing only in which
        budget accessor supplied `per_fragment`, so a future change to the
        chunking algorithm itself had to be applied in two places by
        hand."""
        return [data[i:i + per_fragment] for i in range(0, len(data), per_fragment)]

    def _fragment_payload(self, data: bytes) -> list:
        """Splits `data` into chunks of at most the CHANNEL multi-fragment
        per-fragment budget. Caller (_send_channel_multifragment) already
        guarantees that budget is positive and `data` is non-empty --
        this only ever runs for a packet already established to be too
        large for the fast-path single-fragment budget."""
        return self._chunk_payload(data, self._channel_multifragment_payload_budget())

    def _fragment_direct_payload(self, data: bytes) -> list:
        """DIRECT's own sibling of `_fragment_payload` above -- same
        chunking logic, DIRECT's own (larger, no-name-prefix-cost) budget.
        Milestone 6, rare in practice (`wire_format_design.md`'s
        constraint one: everything but ANNOUNCE comfortably fits one
        DIRECT message, and ANNOUNCE never goes DIRECT in this design)."""
        return self._chunk_payload(data, self._direct_multifragment_payload_budget())

    def _fragment_spacing_range(self, hop_count: Optional[int]) -> "tuple[float, float]":
        """The tiered inter-fragment spacing rule from
        docs/reliability_engine_design.md §2. `hop_count` is `0` for a
        confirmed zero-hop (direct RF neighbor, no repeater) audience, a
        positive int for a confirmed N-hop audience, or `None` when it's
        unknown or the audience spans mixed depths with any unknown
        member -- both cases the design's own standing rule ("missing
        data always gets the conservative treatment") maps to the same
        flat fallback. For a mixed *all-known* audience, the caller is
        responsible for resolving that to a single `hop_count` first, by
        the maximum hop count present (§2's mixed-known-hop rule) --
        this method only implements per-value tier selection. Callers
        today: the DIRECT-fragmented sender passes the resolved path's
        `out_path_len`; the CHANNEL multi-fragment path passes `None`,
        since a broadcast has no single audience depth."""
        if hop_count == 0:
            return (self.fragment_delay_zero_hop_min_s, self.fragment_delay_zero_hop_max_s)
        if hop_count is not None and hop_count >= 1:
            return (
                self.fragment_delay_per_hop_min_s * hop_count,
                self.fragment_delay_per_hop_max_s * hop_count,
            )
        return (self.fragment_delay_min_s, self.fragment_delay_max_s)

    # -------------------------------------------------------------------
    # RNS packet-header classification (docs/reliability_engine_design.md
    # §3, §9) -- read-only, decrypts nothing (this interface's security
    # model: confidentiality/authentication are RNS's job one layer up).
    # -------------------------------------------------------------------

    def _parse_rns_header(self, data: bytes) -> Optional[_RnsHeader]:
        if len(data) < 2:
            return None
        flags = data[0]
        header_type = (flags & 0x40) >> 6
        packet_type = flags & 0x03
        destination_type = (flags >> 2) & 0x03
        dst_len = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
        # HEADER_2 (in-transport) packets carry a transport_id field before
        # destination_hash; HEADER_1 packets don't (confirmed directly
        # against RNS.Packet.unpack()) -- destination_hash is Milestone 5's
        # own addition, read here once, alongside the fields Milestone 3
        # already reads, rather than re-parsing this same layout a second
        # time at every routing-decision call site.
        dst_offset = (2 + dst_len) if header_type == 1 else 2
        context_offset = (2 + 2 * dst_len) if header_type == 1 else (2 + dst_len)
        destination_hash = (
            data[dst_offset:dst_offset + dst_len] if len(data) >= dst_offset + dst_len else None
        )
        context = data[context_offset] if len(data) > context_offset else None
        return _RnsHeader(
            packet_type=packet_type,
            destination_type=destination_type,
            context=context,
            header_type=header_type,
            destination_hash=destination_hash,
        )

    def _compute_truncated_hash(self, data: bytes, header_type: int) -> Optional[bytes]:
        """Replicates `RNS.Packet.get_hashable_part()`/`getTruncatedHash()`
        (confirmed byte-for-byte against a real constructed `RNS.Packet`
        while this milestone was built: `packet.generate_proof_destination()
        .hash` matched this exact computation) -- the value RNS itself
        would use as a PROOF's own destination-hash field for `data`,
        needed for §7's PROOF-correlation table. Calls the real
        `RNS.Identity.truncated_hash()` rather than re-deriving the hash
        algorithm -- only the framing (which header bytes are hashable) is
        reimplemented here, per this interface's security model (it reads
        cleartext header structure, never anything cryptographic)."""
        dst_len = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
        offset = (dst_len + 2) if header_type == 1 else 2
        if len(data) < 1 or len(data) < offset:
            return None
        hashable_part = bytes([data[0] & 0b00001111]) + data[offset:]
        return RNS.Identity.truncated_hash(hashable_part)

    def _compute_link_id(self, data: bytes) -> Optional[bytes]:
        """Code review (2026-09-18): replicates `RNS.Link.link_id_from_lr_
        packet()` for a packed LINKREQUEST `data` -- `get_hashable_part()`
        (the same framing `_compute_truncated_hash` above replicates),
        minus the trailing bytes beyond `Link.ECPUBSIZE` of packet data
        when a LINKREQUEST carries signalling bytes after the two public
        keys. Validated byte-for-byte in-process against real
        `RNS.Packet(...LINKREQUEST).pack()` + `RNS.Link.link_id_from_lr_
        packet()` for data lengths 32, 64, 66, 70 and 80 (both sides of
        the truncation branch) before being wired in. The link_id is what
        an LRPROOF, and every later packet on that Link, carries in its
        destination-hash field -- so this is the correlator that ties a
        Link back to the destination it was requested for
        (`_pending_link_requests`) and to the peer it was requested from
        (`_rns_token_peer`). Reads cleartext header structure only, like
        every other parser here."""
        if not data:
            return None
        dst_len = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
        header_type = (data[0] & 0x40) >> 6
        offset = (dst_len + 2) if header_type == 1 else 2
        data_offset = offset + dst_len + 1  # + the context byte pack() always writes
        if len(data) < data_offset:
            return None
        hashable_part = bytes([data[0] & 0b00001111]) + data[offset:]
        payload_len = len(data) - data_offset
        if payload_len > RNS.Link.ECPUBSIZE:
            hashable_part = hashable_part[:-(payload_len - RNS.Link.ECPUBSIZE)]
        return RNS.Identity.truncated_hash(hashable_part)

    def _priority_tier(self, header: Optional[_RnsHeader]) -> int:
        """docs/reliability_engine_design.md §3's original two-tier
        scheme, widened per §9 to also catch RNS's own give-up/
        connection-alive signals by context byte, not just packet type --
        a RESOURCE_ICL/RESOURCE_RCL/LINKCLOSE/KEEPALIVE/etc. packet is
        packet_type=DATA with a distinguishing context, not one of the
        LINK_REQUEST/PROOF packet *types* handled by the first check
        below. Widened again 2026-09-16 to a third tier, `PRIORITY_LOW`,
        for PATH_RESPONSE specifically -- see that constant's own
        comment for the field data behind it."""
        if header is None:
            return self.PRIORITY_NORMAL
        if header.packet_type == RNS.Packet.PROOF and not self._proof_is_link_class(header):
            # Field fix (2026-09-19 evening session, second audit): a plain
            # delivery PROOF -- the receipt RNS returns for every DATA packet
            # that asked for one -- is NOT handshake class. Nothing hangs on
            # it the way a Link hangs on its LRPROOF: if it is lost, the
            # sender's application simply retries the DATA. Yet at
            # PRIORITY_HANDSHAKE it took the 4-attempt handshake budget and
            # outranked completion ANSWERs, and the desktop's capture that
            # session shows what that cost: 188 such proofs, 1716s of the
            # radio lock spent on their ACK waits (1196s of it in misses at
            # 13-21s each), and completion ANSWERs waiting up to 55s behind
            # five consecutive attempts of two proofs -- which is exactly how
            # the "stale answer" incidents arose. One tier down, the ANSWER
            # tier: still ahead of bulk data (the peer is waiting on it), but
            # behind a Link handshake, with the ordinary 2-attempt budget and
            # no duty-cycle exemption.
            return self.PRIORITY_ANSWER
        if header.packet_type in (RNS.Packet.LINKREQUEST, RNS.Packet.PROOF):
            return self.PRIORITY_HANDSHAKE
        if header.context is not None:
            if header.context in (RNS.Packet.RESOURCE_PRF, RNS.Packet.RESOURCE_ICL, RNS.Packet.RESOURCE_RCL):
                return self.PRIORITY_HANDSHAKE
            # KEEPALIVE(0xFA)..LRPROOF(0xFF) -- RNS core's own boundary,
            # confirmed directly against RNS/Transport.py's own
            # `packet.context >= RNS.Packet.KEEPALIVE and packet.context
            # <= RNS.Packet.LRPROOF` check, not re-derived by guessing
            # which individual context values feel latency-sensitive.
            if RNS.Packet.KEEPALIVE <= header.context <= RNS.Packet.LRPROOF:
                return self.PRIORITY_HANDSHAKE
            if header.context == RNS.Packet.PATH_RESPONSE:
                return self.PRIORITY_LOW
        return self.PRIORITY_NORMAL

    def _is_link_handshake(self, header: Optional[_RnsHeader]) -> bool:
        """The packets that may PRE-EMPT an idle radio-lock hold (phase 1,
        2026-09-20): a Link's establishment and proof -- LINKREQUEST,
        LRPROOF, LRRTT, LINKIDENTIFY, LINKPROOF -- which MeshChat's 15 s
        window and RNS's own link timers wait on. NOT the rest of
        PRIORITY_HANDSHAKE: KEEPALIVE (32.6 of the laptop's 56.3 s of
        tier-0 lock wait in the 2026-09-20 session, 20 B nothing waits
        on), LINKCLOSE and the RESOURCE_PRF/ICL/RCL band keep their tier
        but pre-empt nothing."""
        if header is None:
            return False
        if header.packet_type == RNS.Packet.LINKREQUEST:
            return True
        return header.context in (
            RNS.Packet.LRPROOF, RNS.Packet.LRRTT, RNS.Packet.LINKIDENTIFY, RNS.Packet.LINKPROOF,
        )

    def _plain_proof(self, header: Optional[_RnsHeader]) -> bool:
        """A plain delivery PROOF: packet type PROOF and not link class
        (`proof_max_age` applies; phase 1, 2026-09-20)."""
        return header is not None and header.packet_type == RNS.Packet.PROOF and not self._proof_is_link_class(header)

    def _proof_is_link_class(self, header: _RnsHeader) -> bool:
        """Whether a PROOF packet is one a Link (or a Resource transfer)
        hangs on -- LRPROOF, RESOURCE_PRF, or any context in RNS core's own
        KEEPALIVE..LRPROOF band -- as opposed to a plain delivery receipt
        (context NONE) for one DATA packet. Only the former keeps
        PRIORITY_HANDSHAKE; see `_priority_tier`."""
        ctx = header.context
        if ctx is None:
            return False
        if ctx == RNS.Packet.RESOURCE_PRF:
            return True
        return RNS.Packet.KEEPALIVE <= ctx <= RNS.Packet.LRPROOF

    def _retry_extra_for(self, header: Optional[_RnsHeader]) -> int:
        """docs/reliability_engine_design.md §2's per-traffic-class extra
        CHANNEL retry-pass budget. See _configure_retry's comment on why
        every ANNOUNCE gets the spontaneous-announce default for now
        (path-response detection is Milestone 5). `LINK_REQUEST` and a
        CHANNEL-fallback `PROOF` correctly fall through to the bare-DATA
        default below -- neither has `destination_type == LINK` (a
        LINK_REQUEST addresses the target Destination directly, before
        any Link exists to carry it; PROOF's destination type mirrors
        whatever it's proving), exactly as §9 describes."""
        if header is None:
            return 0
        if header.packet_type == RNS.Packet.ANNOUNCE:
            return self.announce_retransmit_extra
        if header.packet_type == RNS.Packet.DATA and header.destination_type == RNS.Destination.PLAIN:
            return self.path_req_retransmit_extra
        if header.destination_type == RNS.Destination.LINK:
            return self.ordinary_data_link_retransmit_extra
        return self.ordinary_data_bare_retransmit_extra

    def _path_request_target(self, data: bytes, header: _RnsHeader) -> Optional[bytes]:
        """The destination a path request is asking about: the first
        TRUNCATED_HASHLENGTH bytes of the packet data (`RNS.Transport.
        request_path`: `destination_hash + [transport identity hash] +
        tag`, sent PLAIN so it sits in the clear). The packet's own
        destination-hash field is the shared path-request pseudo-
        destination, identical for every request, so it is useless as a
        rate-limit key."""
        dst_len = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
        data_offset = ((2 + dst_len) if header.header_type == 1 else 2) + dst_len + 1
        if len(data) < data_offset + dst_len:
            return None
        return data[data_offset:data_offset + dst_len]
