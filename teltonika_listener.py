#!/usr/bin/env python3
"""
Teltonika AVL TCP listener.

Accepts TCP connections from Teltonika FMB-series trackers (FMB920 and
friends), decodes Codec 8 / Codec 8 Extended / Codec 16 AVL packets and
emits one JSON object per AVL record.

Wire format implemented (TCP):

    Device -> Server   IMEI handshake
        [2 bytes] IMEI length (normally 0x000F)
        [N bytes] IMEI as ASCII

    Server -> Device   1 byte, 0x01 = accepted, 0x00 = rejected

    Device -> Server   AVL data packet
        [4 bytes] preamble, always 0x00000000
        [4 bytes] data field length  (codec_id .. num_data_2, inclusive)
        [1 byte ] codec id
        [1 byte ] number of data 1
        [N bytes] AVL data array
        [1 byte ] number of data 2   (must equal number of data 1)
        [4 bytes] CRC-16/IBM over codec_id .. num_data_2

    Server -> Device   4 bytes, big endian, number of records accepted

All multi-byte integers are big endian.

Usage:
    python3 teltonika_listener.py --host 0.0.0.0 --port 5027
    python3 teltonika_listener.py --decode-hex "000000000000003608010000..."

Author's note: IO element names come from io_definitions.json. That file
ships with only the IO ids I could verify. Unknown ids still decode, they
just come out unnamed. Fill the file in from the Teltonika wiki page for
your exact device model.
"""

from __future__ import annotations

import argparse
import binascii
import json
import logging
import logging.handlers
import os
import signal
import socket
import struct
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

CODEC_8 = 0x08
CODEC_8_EXT = 0x8E
CODEC_16 = 0x10
CODEC_12 = 0x0C          # GPRS commands / responses
CODEC_13 = 0x0D
CODEC_14 = 0x0E

AVL_CODECS = {CODEC_8, CODEC_8_EXT, CODEC_16}
COMMAND_CODECS = {CODEC_12, CODEC_13, CODEC_14}

CODEC_NAMES = {
    CODEC_8: "Codec8",
    CODEC_8_EXT: "Codec8E",
    CODEC_16: "Codec16",
    CODEC_12: "Codec12",
    CODEC_13: "Codec13",
    CODEC_14: "Codec14",
}

PRIORITY_NAMES = {0: "low", 1: "high", 2: "panic"}

# Codec 16 only. Fairly confident about this mapping but worth a
# sanity check against the wiki if you actually run Codec 16 devices.
GENERATION_TYPE_NAMES = {
    0: "on_exit",
    1: "on_entrance",
    2: "on_both",
    3: "reserved",
    4: "hysteresis",
    5: "on_change",
    6: "eventual",
    7: "periodical",
}

# Guard rails. A real AVL packet is a few KB at most; anything larger is
# either a bug on our side or someone poking the port.
MAX_DATA_FIELD_LENGTH = 1 * 1024 * 1024
MAX_RECORDS_PER_PACKET = 255
IMEI_MIN_LEN = 8
IMEI_MAX_LEN = 20

log = logging.getLogger("teltonika")
record_log = logging.getLogger("teltonika.records")
raw_log = logging.getLogger("teltonika.raw")


# --------------------------------------------------------------------------
# CRC-16 / IBM (also known as CRC-16/ARC)
#   poly 0x8005 reflected -> 0xA001, init 0x0000, no final xor
# --------------------------------------------------------------------------

def crc16_ibm(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF


# --------------------------------------------------------------------------
# Byte reader
# --------------------------------------------------------------------------

class ParseError(Exception):
    """Raised when a packet cannot be decoded. Carries the offset."""

    def __init__(self, message: str, offset: int | None = None):
        super().__init__(message)
        self.offset = offset


class ByteReader:
    """Sequential big-endian reader that tracks its offset."""

    __slots__ = ("data", "offset")

    def __init__(self, data: bytes, offset: int = 0):
        self.data = data
        self.offset = offset

    def remaining(self) -> int:
        return len(self.data) - self.offset

    def _take(self, n: int) -> bytes:
        if n < 0:
            raise ParseError(f"negative read length {n}", self.offset)
        if self.remaining() < n:
            raise ParseError(
                f"tried to read {n} byte(s) with only {self.remaining()} left",
                self.offset,
            )
        chunk = self.data[self.offset:self.offset + n]
        self.offset += n
        return chunk

    def bytes(self, n: int) -> bytes:
        return self._take(n)

    def u8(self) -> int:
        return self._take(1)[0]

    def u16(self) -> int:
        return struct.unpack(">H", self._take(2))[0]

    def u32(self) -> int:
        return struct.unpack(">I", self._take(4))[0]

    def u64(self) -> int:
        return struct.unpack(">Q", self._take(8))[0]

    def i16(self) -> int:
        return struct.unpack(">h", self._take(2))[0]

    def i32(self) -> int:
        return struct.unpack(">i", self._take(4))[0]

    def uint(self, size: int) -> int:
        return int.from_bytes(self._take(size), "big", signed=False)


# --------------------------------------------------------------------------
# IO definitions
# --------------------------------------------------------------------------

class IODefinitions:
    """
    Maps IO element ids to a human readable name, unit and scaling.

    Definition entries look like:
        "66": {"name": "External Voltage", "unit": "V",
               "multiplier": 0.001, "signed": false}
        "239": {"name": "Ignition", "enum": {"0": "off", "1": "on"}}
    """

    def __init__(self, table: dict[str, dict[str, Any]] | None = None):
        self.table: dict[int, dict[str, Any]] = {}
        if table:
            for key, spec in table.items():
                try:
                    self.table[int(key)] = spec
                except (TypeError, ValueError):
                    log.warning("ignoring non-integer IO definition key %r", key)

    @classmethod
    def load(cls, path: str | os.PathLike | None) -> "IODefinitions":
        if not path:
            return cls()
        p = Path(path)
        if not p.exists():
            log.warning("IO definition file %s not found, ids will be unnamed", p)
            return cls()
        try:
            with p.open("r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            log.error("could not read IO definitions from %s: %s", p, exc)
            return cls()
        table = raw.get("io_elements", raw)
        log.info("loaded %d IO definition(s) from %s", len(table), p)
        return cls(table)

    def describe(self, io_id: int, raw_value: int, size: int) -> dict[str, Any]:
        """Turn a raw IO value into a labelled, scaled entry."""
        out: dict[str, Any] = {"id": io_id, "raw": raw_value}
        spec = self.table.get(io_id)

        if spec is None:
            out["name"] = None
            out["value"] = raw_value
            return out

        out["name"] = spec.get("name")

        value: Any = raw_value
        if spec.get("signed"):
            bits = size * 8
            if value >= (1 << (bits - 1)):
                value -= (1 << bits)

        multiplier = spec.get("multiplier")
        if multiplier:
            value = round(value * multiplier, 6)

        enum = spec.get("enum")
        if enum:
            out["state"] = enum.get(str(raw_value), f"unknown({raw_value})")

        out["value"] = value
        if spec.get("unit"):
            out["unit"] = spec["unit"]
        return out


# --------------------------------------------------------------------------
# AVL parsing
# --------------------------------------------------------------------------

def _decode_timestamp(ms: int) -> str | None:
    """Teltonika sends ms since Unix epoch. 0 means the device had no time."""
    if ms == 0:
        return None
    # Sanity window: 2000-01-01 .. 2100-01-01. Devices with a dead RTC
    # occasionally emit garbage here and we do not want fromtimestamp to
    # throw halfway through a batch.
    if not (946684800000 <= ms <= 4102444800000):
        return None
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat(
            timespec="milliseconds"
        )
    except (OverflowError, OSError, ValueError):
        return None


def _parse_gps(reader: ByteReader) -> dict[str, Any]:
    longitude = reader.i32()
    latitude = reader.i32()
    altitude = reader.i16()
    angle = reader.u16()
    satellites = reader.u8()
    speed = reader.u16()

    # Both coordinates zero plus zero satellites is Teltonika's "no fix".
    valid = not (longitude == 0 and latitude == 0) and satellites > 0

    return {
        "longitude": round(longitude / 1e7, 7),
        "latitude": round(latitude / 1e7, 7),
        "altitude_m": altitude,
        "angle_deg": angle,
        "satellites": satellites,
        "speed_kmh": speed,
        "valid": valid,
    }


def _parse_io_groups(
    reader: ByteReader,
    id_size: int,
    count_size: int,
    io_defs: IODefinitions,
) -> dict[str, Any]:
    """
    Read the N1 / N2 / N4 / N8 fixed-width IO groups.

    id_size    1 for Codec 8, 2 for Codec 8E and Codec 16
    count_size 1 for Codec 8 and Codec 16, 2 for Codec 8E
    """
    elements: dict[str, Any] = {}
    for value_size in (1, 2, 4, 8):
        count = reader.uint(count_size)
        for _ in range(count):
            io_id = reader.uint(id_size)
            raw_value = reader.uint(value_size)
            elements[str(io_id)] = io_defs.describe(io_id, raw_value, value_size)
    return elements


def _parse_io_variable(
    reader: ByteReader,
    io_defs: IODefinitions,
) -> dict[str, Any]:
    """Codec 8E only: the NX group of variable length elements."""
    elements: dict[str, Any] = {}
    count = reader.u16()
    for _ in range(count):
        io_id = reader.u16()
        length = reader.u16()
        value = reader.bytes(length)
        spec = io_defs.table.get(io_id) or {}
        elements[str(io_id)] = {
            "id": io_id,
            "name": spec.get("name"),
            "raw": value.hex().upper(),
            "value": value.hex().upper(),
            "length": length,
        }
    return elements


def _parse_record_codec8(reader: ByteReader, io_defs: IODefinitions) -> dict[str, Any]:
    timestamp_ms = reader.u64()
    priority = reader.u8()
    gps = _parse_gps(reader)

    event_io_id = reader.u8()
    total_io_count = reader.u8()
    io_elements = _parse_io_groups(reader, id_size=1, count_size=1, io_defs=io_defs)

    return {
        "timestamp_ms": timestamp_ms,
        "timestamp": _decode_timestamp(timestamp_ms),
        "priority": priority,
        "priority_name": PRIORITY_NAMES.get(priority, f"unknown({priority})"),
        "gps": gps,
        "event_io_id": event_io_id,
        "io_count_declared": total_io_count,
        "io_count_parsed": len(io_elements),
        "io": io_elements,
    }


def _parse_record_codec8e(reader: ByteReader, io_defs: IODefinitions) -> dict[str, Any]:
    timestamp_ms = reader.u64()
    priority = reader.u8()
    gps = _parse_gps(reader)

    event_io_id = reader.u16()
    total_io_count = reader.u16()
    io_elements = _parse_io_groups(reader, id_size=2, count_size=2, io_defs=io_defs)
    io_elements.update(_parse_io_variable(reader, io_defs))

    return {
        "timestamp_ms": timestamp_ms,
        "timestamp": _decode_timestamp(timestamp_ms),
        "priority": priority,
        "priority_name": PRIORITY_NAMES.get(priority, f"unknown({priority})"),
        "gps": gps,
        "event_io_id": event_io_id,
        "io_count_declared": total_io_count,
        "io_count_parsed": len(io_elements),
        "io": io_elements,
    }


def _parse_record_codec16(reader: ByteReader, io_defs: IODefinitions) -> dict[str, Any]:
    timestamp_ms = reader.u64()
    priority = reader.u8()
    gps = _parse_gps(reader)

    event_io_id = reader.u16()
    generation_type = reader.u8()
    total_io_count = reader.u8()
    io_elements = _parse_io_groups(reader, id_size=2, count_size=1, io_defs=io_defs)

    return {
        "timestamp_ms": timestamp_ms,
        "timestamp": _decode_timestamp(timestamp_ms),
        "priority": priority,
        "priority_name": PRIORITY_NAMES.get(priority, f"unknown({priority})"),
        "gps": gps,
        "event_io_id": event_io_id,
        "generation_type": generation_type,
        "generation_type_name": GENERATION_TYPE_NAMES.get(
            generation_type, f"unknown({generation_type})"
        ),
        "io_count_declared": total_io_count,
        "io_count_parsed": len(io_elements),
        "io": io_elements,
    }


RECORD_PARSERS: dict[int, Callable[[ByteReader, IODefinitions], dict[str, Any]]] = {
    CODEC_8: _parse_record_codec8,
    CODEC_8_EXT: _parse_record_codec8e,
    CODEC_16: _parse_record_codec16,
}


def parse_avl_packet(packet: bytes, io_defs: IODefinitions) -> dict[str, Any]:
    """
    Decode a full AVL packet including preamble, length and CRC.

    Raises ParseError on anything malformed. Never returns partial data.
    """
    if len(packet) < 12:
        raise ParseError(f"packet too short: {len(packet)} bytes", 0)

    reader = ByteReader(packet)

    preamble = reader.u32()
    if preamble != 0:
        raise ParseError(f"bad preamble 0x{preamble:08X}, expected 0x00000000", 0)

    data_field_length = reader.u32()
    if data_field_length < 3:
        raise ParseError(f"data field length {data_field_length} is impossibly small", 4)
    if data_field_length > MAX_DATA_FIELD_LENGTH:
        raise ParseError(f"data field length {data_field_length} exceeds cap", 4)

    expected_total = 8 + data_field_length + 4
    if len(packet) != expected_total:
        raise ParseError(
            f"packet is {len(packet)} bytes, header implies {expected_total}", 4
        )

    body = packet[8:8 + data_field_length]
    crc_received = struct.unpack(">I", packet[8 + data_field_length:])[0]
    crc_expected = crc16_ibm(body)

    if crc_received != crc_expected:
        raise ParseError(
            f"CRC mismatch: got 0x{crc_received:08X}, computed 0x{crc_expected:04X}",
            8 + data_field_length,
        )

    body_reader = ByteReader(body)
    codec_id = body_reader.u8()

    if codec_id in COMMAND_CODECS:
        return {
            "codec_id": codec_id,
            "codec": CODEC_NAMES.get(codec_id, f"0x{codec_id:02X}"),
            "kind": "command",
            "record_count": 0,
            "records": [],
            "payload_hex": body[1:].hex().upper(),
        }

    if codec_id not in AVL_CODECS:
        raise ParseError(f"unsupported codec id 0x{codec_id:02X}", 8)

    record_count = body_reader.u8()
    if record_count == 0:
        raise ParseError("packet declares zero records", 9)
    if record_count > MAX_RECORDS_PER_PACKET:
        raise ParseError(f"record count {record_count} out of range", 9)

    parser = RECORD_PARSERS[codec_id]
    records: list[dict[str, Any]] = []
    for index in range(record_count):
        try:
            records.append(parser(body_reader, io_defs))
        except ParseError as exc:
            raise ParseError(
                f"record {index}/{record_count}: {exc}",
                8 + (exc.offset if exc.offset is not None else body_reader.offset),
            ) from exc

    record_count_2 = body_reader.u8()
    if record_count_2 != record_count:
        raise ParseError(
            f"record count mismatch: header {record_count}, footer {record_count_2}",
            8 + body_reader.offset - 1,
        )

    if body_reader.remaining() != 0:
        raise ParseError(
            f"{body_reader.remaining()} trailing byte(s) after last record, "
            "parser and packet disagree on layout",
            8 + body_reader.offset,
        )

    return {
        "codec_id": codec_id,
        "codec": CODEC_NAMES.get(codec_id, f"0x{codec_id:02X}"),
        "kind": "avl",
        "record_count": record_count,
        "records": records,
    }


# --------------------------------------------------------------------------
# Hex dump helper, used when a parse fails
# --------------------------------------------------------------------------

def hex_dump(data: bytes, width: int = 16, mark: int | None = None) -> str:
    lines = []
    for base in range(0, len(data), width):
        chunk = data[base:base + width]
        hex_part = " ".join(f"{b:02X}" for b in chunk).ljust(width * 3 - 1)
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        pointer = ""
        if mark is not None and base <= mark < base + width:
            pointer = f"   <-- offset {mark}"
        lines.append(f"{base:04X}  {hex_part}  |{ascii_part}|{pointer}")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Connection handling
# --------------------------------------------------------------------------

class ConnectionStats:
    __slots__ = ("packets_ok", "packets_bad", "records", "bytes_in", "connected_at")

    def __init__(self):
        self.packets_ok = 0
        self.packets_bad = 0
        self.records = 0
        self.bytes_in = 0
        self.connected_at = time.time()


class DeviceHandler(threading.Thread):
    """One thread per connected device."""

    def __init__(
        self,
        conn: socket.socket,
        addr: tuple[str, int],
        io_defs: IODefinitions,
        sink: "RecordSink",
        idle_timeout: float,
        allowed_imeis: set[str] | None,
        shutdown_event: threading.Event,
        store_raw: bool,
    ):
        super().__init__(daemon=True, name=f"dev-{addr[0]}:{addr[1]}")
        self.conn = conn
        self.addr = addr
        self.io_defs = io_defs
        self.sink = sink
        self.idle_timeout = idle_timeout
        self.allowed_imeis = allowed_imeis
        self.shutdown_event = shutdown_event
        self.store_raw = store_raw
        self.imei: str | None = None
        self.stats = ConnectionStats()

    # -- low level IO ------------------------------------------------------

    def _recv_exact(self, n: int) -> bytes | None:
        """
        Read exactly n bytes. Returns None if the peer closed cleanly
        before sending anything more. Raises on timeout or partial frame.
        """
        buf = bytearray()
        while len(buf) < n:
            if self.shutdown_event.is_set():
                return None
            try:
                chunk = self.conn.recv(n - len(buf))
            except socket.timeout:
                if not buf:
                    raise
                raise ConnectionError(
                    f"timed out mid-frame after {len(buf)}/{n} bytes"
                )
            if not chunk:
                if not buf:
                    return None
                raise ConnectionError(
                    f"peer closed mid-frame after {len(buf)}/{n} bytes"
                )
            buf.extend(chunk)
        self.stats.bytes_in += n
        return bytes(buf)

    def _send_all(self, data: bytes) -> None:
        self.conn.sendall(data)

    # -- protocol steps ----------------------------------------------------

    def _do_imei_handshake(self) -> bool:
        header = self._recv_exact(2)
        if header is None:
            log.info("%s closed before sending IMEI", self._tag())
            return False

        imei_len = struct.unpack(">H", header)[0]
        if not (IMEI_MIN_LEN <= imei_len <= IMEI_MAX_LEN):
            log.warning(
                "%s bogus IMEI length %d (raw %s), dropping",
                self._tag(), imei_len, header.hex().upper(),
            )
            return False

        imei_bytes = self._recv_exact(imei_len)
        if imei_bytes is None:
            log.warning("%s closed during IMEI body", self._tag())
            return False

        try:
            imei = imei_bytes.decode("ascii").strip()
        except UnicodeDecodeError:
            log.warning(
                "%s non-ASCII IMEI %s, dropping",
                self._tag(), imei_bytes.hex().upper(),
            )
            return False

        if not imei.isdigit():
            log.warning("%s non-numeric IMEI %r, dropping", self._tag(), imei)
            return False

        if self.allowed_imeis is not None and imei not in self.allowed_imeis:
            log.warning("%s IMEI %s not in allowlist, rejecting", self._tag(), imei)
            self._send_all(b"\x00")
            return False

        self.imei = imei
        self._send_all(b"\x01")
        log.info("%s accepted IMEI %s", self._tag(), imei)
        return True

    def _read_packet(self) -> bytes | None:
        """Read one framed AVL packet off the stream."""
        header = self._recv_exact(8)
        if header is None:
            return None

        preamble, data_field_length = struct.unpack(">II", header)

        if preamble != 0:
            # We cannot resync safely, the stream is desynchronised.
            raise ConnectionError(
                f"bad preamble 0x{preamble:08X}, stream out of sync"
            )
        if data_field_length < 3 or data_field_length > MAX_DATA_FIELD_LENGTH:
            raise ConnectionError(
                f"implausible data field length {data_field_length}"
            )

        rest = self._recv_exact(data_field_length + 4)
        if rest is None:
            raise ConnectionError("peer closed before packet body arrived")

        return header + rest

    # -- main loop ---------------------------------------------------------

    def _tag(self) -> str:
        who = self.imei or "unknown"
        return f"[{self.addr[0]}:{self.addr[1]} imei={who}]"

    def run(self) -> None:
        self.conn.settimeout(self.idle_timeout)
        try:
            self.conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass

        log.info("%s connected", self._tag())

        try:
            if not self._do_imei_handshake():
                return
            self._serve_packets()
        except socket.timeout:
            log.info("%s idle for %.0fs, closing", self._tag(), self.idle_timeout)
        except ConnectionError as exc:
            log.warning("%s connection error: %s", self._tag(), exc)
        except OSError as exc:
            log.warning("%s socket error: %s", self._tag(), exc)
        except Exception:
            log.exception("%s unexpected handler failure", self._tag())
        finally:
            self._close()

    def _serve_packets(self) -> None:
        while not self.shutdown_event.is_set():
            packet = self._read_packet()
            if packet is None:
                log.info("%s closed connection", self._tag())
                return

            if self.store_raw:
                raw_log.info(
                    json.dumps({
                        "received_at": datetime.now(timezone.utc).isoformat(
                            timespec="milliseconds"
                        ),
                        "imei": self.imei,
                        "peer": f"{self.addr[0]}:{self.addr[1]}",
                        "hex": packet.hex().upper(),
                    })
                )

            try:
                decoded = parse_avl_packet(packet, self.io_defs)
            except ParseError as exc:
                self.stats.packets_bad += 1
                log.error(
                    "%s parse failed: %s\nraw (%d bytes):\n%s",
                    self._tag(), exc, len(packet),
                    hex_dump(packet, mark=exc.offset),
                )
                # Do not ACK. The device will retransmit and we get another
                # chance once the parser is fixed.
                return
            except Exception:
                self.stats.packets_bad += 1
                log.exception(
                    "%s unexpected parser crash\nraw (%d bytes):\n%s",
                    self._tag(), len(packet), hex_dump(packet),
                )
                return

            if decoded["kind"] == "command":
                log.info(
                    "%s received %s command frame, payload %s",
                    self._tag(), decoded["codec"], decoded["payload_hex"],
                )
                continue

            self.stats.packets_ok += 1
            self.stats.records += decoded["record_count"]

            # Emit before acknowledging. If the sink throws we must not
            # tell the device the data is safe, because it drops it after
            # a successful ACK.
            self.sink.emit(self.imei, self.addr, decoded)

            self._send_all(struct.pack(">I", decoded["record_count"]))
            log.info(
                "%s ack %d record(s), codec %s",
                self._tag(), decoded["record_count"], decoded["codec"],
            )

    def _close(self) -> None:
        uptime = time.time() - self.stats.connected_at
        log.info(
            "%s disconnected after %.0fs: %d packet(s) ok, %d bad, "
            "%d record(s), %d byte(s)",
            self._tag(), uptime, self.stats.packets_ok,
            self.stats.packets_bad, self.stats.records, self.stats.bytes_in,
        )
        try:
            self.conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.conn.close()
        except OSError:
            pass


# --------------------------------------------------------------------------
# Output sink
# --------------------------------------------------------------------------

class RecordSink:
    """
    Writes decoded records out as JSON. One line per AVL record.

    This is deliberately the only place that knows about output format.
    When you move to Timescale, swap the body of emit() for a batched
    COPY and keep everything else as is.
    """

    def __init__(self, pretty: bool = False):
        self.pretty = pretty
        self.lock = threading.Lock()

    def emit(
        self,
        imei: str | None,
        addr: tuple[str, int],
        decoded: dict[str, Any],
    ) -> None:
        received_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        lines = []
        for index, record in enumerate(decoded["records"]):
            payload = {
                "imei": imei,
                "peer": f"{addr[0]}:{addr[1]}",
                "received_at": received_at,
                "codec": decoded["codec"],
                "codec_id": decoded["codec_id"],
                "record_index": index,
                "record_count": decoded["record_count"],
                **record,
            }
            lines.append(
                json.dumps(payload, ensure_ascii=False,
                           indent=2 if self.pretty else None)
            )
        with self.lock:
            for line in lines:
                record_log.info(line)


# --------------------------------------------------------------------------
# Server
# --------------------------------------------------------------------------

class TeltonikaServer:
    def __init__(
        self,
        host: str,
        port: int,
        io_defs: IODefinitions,
        sink: RecordSink,
        idle_timeout: float,
        max_connections: int,
        allowed_imeis: set[str] | None,
        store_raw: bool,
    ):
        self.host = host
        self.port = port
        self.io_defs = io_defs
        self.sink = sink
        self.idle_timeout = idle_timeout
        self.max_connections = max_connections
        self.allowed_imeis = allowed_imeis
        self.store_raw = store_raw
        self.shutdown_event = threading.Event()
        self.handlers: list[DeviceHandler] = []
        self.handlers_lock = threading.Lock()
        self.sock: socket.socket | None = None

    def _reap(self) -> int:
        with self.handlers_lock:
            self.handlers = [h for h in self.handlers if h.is_alive()]
            return len(self.handlers)

    def serve_forever(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(1.0)
        self.sock.bind((self.host, self.port))
        self.sock.listen(128)

        log.info("listening on %s:%d", self.host, self.port)
        if self.allowed_imeis is not None:
            log.info("IMEI allowlist active with %d entry(ies)", len(self.allowed_imeis))
        else:
            log.warning(
                "no IMEI allowlist configured, accepting every device "
                "(fine for a test droplet, not for production)"
            )

        while not self.shutdown_event.is_set():
            try:
                conn, addr = self.sock.accept()
            except socket.timeout:
                self._reap()
                continue
            except OSError as exc:
                if self.shutdown_event.is_set():
                    break
                log.error("accept failed: %s", exc)
                time.sleep(0.5)
                continue

            active = self._reap()
            if active >= self.max_connections:
                log.warning(
                    "connection limit %d reached, dropping %s:%d",
                    self.max_connections, addr[0], addr[1],
                )
                try:
                    conn.close()
                except OSError:
                    pass
                continue

            handler = DeviceHandler(
                conn=conn,
                addr=addr,
                io_defs=self.io_defs,
                sink=self.sink,
                idle_timeout=self.idle_timeout,
                allowed_imeis=self.allowed_imeis,
                shutdown_event=self.shutdown_event,
                store_raw=self.store_raw,
            )
            with self.handlers_lock:
                self.handlers.append(handler)
            handler.start()

        self._shutdown()

    def stop(self) -> None:
        self.shutdown_event.set()

    def _shutdown(self) -> None:
        log.info("shutting down")
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
        with self.handlers_lock:
            handlers = list(self.handlers)
        deadline = time.time() + 10
        for handler in handlers:
            remaining = max(0.1, deadline - time.time())
            handler.join(timeout=remaining)
        log.info("stopped")


# --------------------------------------------------------------------------
# Logging setup
# --------------------------------------------------------------------------

def setup_logging(log_dir: str, level: str, store_raw: bool, quiet: bool) -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    numeric_level = getattr(logging, level.upper(), logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    log.setLevel(numeric_level)
    log.propagate = False
    for handler in list(log.handlers):
        log.removeHandler(handler)

    if not quiet:
        stream = logging.StreamHandler(sys.stderr)
        stream.setFormatter(fmt)
        log.addHandler(stream)

    server_file = logging.handlers.RotatingFileHandler(
        Path(log_dir) / "server.log", maxBytes=50 * 1024 * 1024, backupCount=5,
        encoding="utf-8",
    )
    server_file.setFormatter(fmt)
    log.addHandler(server_file)

    # Decoded records: JSON lines, no log decoration.
    record_log.setLevel(logging.INFO)
    record_log.propagate = False
    for handler in list(record_log.handlers):
        record_log.removeHandler(handler)

    plain = logging.Formatter("%(message)s")

    record_stdout = logging.StreamHandler(sys.stdout)
    record_stdout.setFormatter(plain)
    record_log.addHandler(record_stdout)

    record_file = logging.handlers.RotatingFileHandler(
        Path(log_dir) / "records.jsonl", maxBytes=200 * 1024 * 1024, backupCount=10,
        encoding="utf-8",
    )
    record_file.setFormatter(plain)
    record_log.addHandler(record_file)

    # Raw packets: hex, for replay when the parser turns out to be wrong.
    raw_log.setLevel(logging.INFO if store_raw else logging.CRITICAL)
    raw_log.propagate = False
    for handler in list(raw_log.handlers):
        raw_log.removeHandler(handler)
    if store_raw:
        raw_file = logging.handlers.RotatingFileHandler(
            Path(log_dir) / "raw.jsonl", maxBytes=200 * 1024 * 1024, backupCount=10,
            encoding="utf-8",
        )
        raw_file.setFormatter(plain)
        raw_log.addHandler(raw_file)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def load_allowlist(path: str | None) -> set[str] | None:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        log.error("allowlist file %s not found, refusing to start", p)
        sys.exit(2)
    imeis = set()
    with p.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            imeis.add(line)
    return imeis


def cmd_decode_hex(hex_string: str, io_defs: IODefinitions) -> int:
    cleaned = "".join(hex_string.split()).replace("0x", "").replace("0X", "")
    try:
        packet = binascii.unhexlify(cleaned)
    except binascii.Error as exc:
        print(f"not valid hex: {exc}", file=sys.stderr)
        return 2

    print(f"# {len(packet)} bytes")
    try:
        decoded = parse_avl_packet(packet, io_defs)
    except ParseError as exc:
        print(f"# PARSE FAILED: {exc}", file=sys.stderr)
        print(hex_dump(packet, mark=exc.offset), file=sys.stderr)
        return 1

    print(hex_dump(packet))
    print()
    print(json.dumps(decoded, indent=2, ensure_ascii=False))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Teltonika AVL TCP listener (Codec 8 / 8E / 16)",
    )
    parser.add_argument("--host", default=os.environ.get("TELTONIKA_HOST", "0.0.0.0"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("TELTONIKA_PORT", "5027"))
    )
    parser.add_argument(
        "--io-definitions",
        default=os.environ.get("TELTONIKA_IO_DEFS", "io_definitions.json"),
        help="JSON file mapping IO ids to names, units and multipliers",
    )
    parser.add_argument(
        "--log-dir", default=os.environ.get("TELTONIKA_LOG_DIR", "./logs")
    )
    parser.add_argument(
        "--log-level", default=os.environ.get("TELTONIKA_LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument(
        "--idle-timeout", type=float,
        default=float(os.environ.get("TELTONIKA_IDLE_TIMEOUT", "600")),
        help="seconds of silence before a connection is dropped",
    )
    parser.add_argument(
        "--max-connections", type=int,
        default=int(os.environ.get("TELTONIKA_MAX_CONNECTIONS", "500")),
    )
    parser.add_argument(
        "--allowlist", default=os.environ.get("TELTONIKA_ALLOWLIST"),
        help="file with one permitted IMEI per line; omit to accept all",
    )
    parser.add_argument(
        "--no-raw", action="store_true",
        help="do not write raw packet hex to logs/raw.jsonl",
    )
    parser.add_argument(
        "--pretty", action="store_true",
        help="pretty print record JSON instead of one line per record",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="suppress server logs on stderr, keep the JSON stream on stdout",
    )
    parser.add_argument(
        "--decode-hex", metavar="HEX",
        help="decode a single packet from a hex string and exit",
    )

    args = parser.parse_args(argv)

    store_raw = not args.no_raw
    setup_logging(args.log_dir, args.log_level, store_raw, args.quiet)

    io_defs = IODefinitions.load(args.io_definitions)

    if args.decode_hex:
        return cmd_decode_hex(args.decode_hex, io_defs)

    allowed = load_allowlist(args.allowlist)
    sink = RecordSink(pretty=args.pretty)

    server = TeltonikaServer(
        host=args.host,
        port=args.port,
        io_defs=io_defs,
        sink=sink,
        idle_timeout=args.idle_timeout,
        max_connections=args.max_connections,
        allowed_imeis=allowed,
        store_raw=store_raw,
    )

    def handle_signal(signum, _frame):
        log.info("caught signal %s", signal.Signals(signum).name)
        server.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        server.serve_forever()
    except Exception:
        log.exception("server loop failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
