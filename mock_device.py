#!/usr/bin/env python3
"""
Mock Teltonika device with a vehicle state machine.

Real trackers do not send a fixed IO set. What they report depends on
what the vehicle is doing: a bus asleep in the depot sends a handful of
elements, the same bus on a highway with a fresh fix sends three times
as many, and event records carry whichever id triggered them. This mock
simulates that so the listener gets exercised the way real traffic will
exercise it.

    python3 mock_device.py --count 40                  # full drive cycle
    python3 mock_device.py --scenario driving          # pin one phase
    python3 mock_device.py --scenario all_io --count 1 # maximal IO set
    python3 mock_device.py --devices 50 --count 20     # concurrency
    python3 mock_device.py --evil                      # malformed inputs
    python3 mock_device.py --list-io                   # AVL id table

Also usable as a library. build_packet() returns bytes you can hand to
teltonika_listener.parse_avl_packet().

CONFIDENCE WARNING
------------------
Byte widths for ids 21, 1, 66, 241 and 78 are confirmed against a real
Codec 8 packet. Everything else in AVL_IDS is written from documentation
and marked with a confidence level. Run --list-io to see which. Verify
anything marked "med" against the Teltonika wiki page for your firmware
before you trust decoded values in production.
"""

from __future__ import annotations

import argparse
import math
import random
import socket
import struct
import sys
import threading
import time

from teltonika_listener import crc16_ibm

CODEC_8 = 0x08
CODEC_8_EXT = 0x8E
CODEC_16 = 0x10


# --------------------------------------------------------------------------
# AVL id catalogue
#
#   key -> (avl_id, byte_width, confidence)
#
#   confidence:
#     "verified"  width confirmed against a real Codec 8 packet
#     "high"      widely documented, I am confident
#     "med"       plausible from docs, verify against the wiki for your model
# --------------------------------------------------------------------------

AVL_IDS: dict[str, tuple[int, int, str]] = {
    # -- the must-haves --------------------------------------------------
    "ignition":            (239, 1, "high"),
    "movement":            (240, 1, "high"),
    "gsm_signal":          (21,  1, "verified"),
    "sleep_mode":          (200, 1, "high"),
    "gnss_status":         (69,  1, "high"),
    "gnss_pdop":           (181, 2, "med"),
    "gnss_hdop":           (182, 2, "med"),
    "external_voltage":    (66,  2, "verified"),
    "battery_voltage":     (67,  2, "high"),
    "battery_current":     (68,  2, "high"),
    "gsm_operator":        (241, 4, "verified"),
    "total_odometer":      (16,  4, "high"),

    # -- everything else the FMB920 can plausibly report ------------------
    "digital_input_1":     (1,   1, "verified"),
    "digital_input_2":     (2,   1, "high"),
    "digital_input_3":     (3,   1, "high"),
    "analog_input_1":      (9,   2, "high"),
    "gnss_speed":          (24,  2, "high"),
    "data_mode":           (80,  1, "high"),
    "battery_level":       (113, 1, "high"),
    "digital_output_1":    (179, 1, "high"),
    "digital_output_2":    (180, 1, "high"),
    "trip_odometer":       (199, 4, "high"),
    "gsm_cell_id":         (205, 2, "med"),
    "gsm_area_code":       (206, 2, "med"),
    "ibutton_id":          (78,  8, "verified"),
    "towing_detection":    (246, 1, "med"),
    "crash_detection":     (247, 1, "med"),
    "immobilizer":         (248, 1, "med"),
    "jamming":             (249, 1, "med"),
    "trip":                (250, 1, "high"),
    "idling":              (251, 1, "high"),
    "unplug":              (252, 1, "med"),
    "green_driving_type":  (253, 1, "high"),
    "green_driving_value": (254, 1, "med"),
    "over_speeding":       (255, 1, "high"),
}

# Codec 8E only. Variable length elements, value is raw bytes.
VARIABLE_IDS: dict[str, tuple[int, str]] = {
    "vin": (256, "med"),
}


def _resolve(named: dict[str, int]) -> dict[int, tuple[int, int]]:
    """Turn {"ignition": 1} into {239: (1, 1)} using the catalogue."""
    out: dict[int, tuple[int, int]] = {}
    for key, value in named.items():
        if key not in AVL_IDS:
            raise KeyError(f"unknown IO key {key!r}")
        avl_id, width, _conf = AVL_IDS[key]
        limit = 1 << (width * 8)
        if not (0 <= value < limit):
            raise ValueError(
                f"{key} value {value} does not fit in {width} byte(s)"
            )
        out[avl_id] = (value, width)
    return out


# --------------------------------------------------------------------------
# Packet construction
# --------------------------------------------------------------------------

def _gps_element(lat: float, lon: float, alt: int, angle: int,
                 sats: int, speed: int) -> bytes:
    return (
        struct.pack(">i", int(round(lon * 1e7)))
        + struct.pack(">i", int(round(lat * 1e7)))
        + struct.pack(">h", alt)
        + struct.pack(">H", angle)
        + struct.pack(">B", sats)
        + struct.pack(">H", speed)
    )


def _io_groups(io_map: dict[int, tuple[int, int]], id_size: int,
               count_size: int) -> bytes:
    out = b""
    for width in (1, 2, 4, 8):
        items = [(k, v) for k, (v, w) in io_map.items() if w == width]
        out += int(len(items)).to_bytes(count_size, "big")
        for io_id, value in sorted(items):
            out += io_id.to_bytes(id_size, "big")
            out += value.to_bytes(width, "big")
    return out


def build_record(codec: int, timestamp_ms: int, priority: int,
                 lat: float, lon: float, alt: int, angle: int,
                 sats: int, speed: int, event_io_id: int,
                 io_map: dict[int, tuple[int, int]],
                 variable_io: dict[int, bytes] | None = None) -> bytes:
    body = struct.pack(">Q", timestamp_ms)
    body += struct.pack(">B", priority)
    body += _gps_element(lat, lon, alt, angle, sats, speed)

    total = len(io_map) + len(variable_io or {})

    if codec == CODEC_8:
        body += struct.pack(">B", event_io_id & 0xFF)
        body += struct.pack(">B", total)
        body += _io_groups(io_map, id_size=1, count_size=1)
    elif codec == CODEC_8_EXT:
        body += struct.pack(">H", event_io_id)
        body += struct.pack(">H", total)
        body += _io_groups(io_map, id_size=2, count_size=2)
        variable_io = variable_io or {}
        body += struct.pack(">H", len(variable_io))
        for io_id, value in sorted(variable_io.items()):
            body += struct.pack(">HH", io_id, len(value)) + value
    elif codec == CODEC_16:
        body += struct.pack(">H", event_io_id)
        body += struct.pack(">B", 6)          # generation type: eventual
        body += struct.pack(">B", total)
        body += _io_groups(io_map, id_size=2, count_size=1)
    else:
        raise ValueError(f"unsupported codec 0x{codec:02X}")

    return body


def build_packet(codec: int, records: list[bytes],
                 corrupt_crc: bool = False,
                 mismatch_counts: bool = False) -> bytes:
    data = struct.pack(">BB", codec, len(records))
    data += b"".join(records)
    data += struct.pack(">B", len(records) + (1 if mismatch_counts else 0))

    crc = crc16_ibm(data)
    if corrupt_crc:
        crc ^= 0xFFFF

    return (
        struct.pack(">I", 0)
        + struct.pack(">I", len(data))
        + data
        + struct.pack(">I", crc)
    )


# --------------------------------------------------------------------------
# Vehicle simulator
# --------------------------------------------------------------------------

PHASES = ("deep_sleep", "waking", "acquiring_fix", "driving",
          "idling", "shutting_down")


class VehicleSimulator:
    """
    Simulates one bus moving through a realistic duty cycle and produces
    the IO set a tracker would actually send in each state.

    The point is variation. deep_sleep emits about 9 elements with no GPS
    fix, driving emits 25 plus with a full fix, and event records carry a
    non-zero event_io_id. If your parser has an off-by-one anywhere in the
    IO group handling, cycling through these will find it.
    """

    def __init__(self, imei: str, seed: int | None = None):
        self.imei = imei
        self.rng = random.Random(seed if seed is not None else hash(imei) & 0xFFFF)

        # Dhaka, roughly. Bus starts at a depot.
        self.lat = 23.7808 + self.rng.uniform(-0.03, 0.03)
        self.lon = 90.4093 + self.rng.uniform(-0.03, 0.03)
        self.altitude = self.rng.randint(2, 20)
        self.heading = self.rng.randint(0, 359)
        self.speed = 0

        self.total_odometer = self.rng.randint(80_000, 900_000)   # metres
        self.trip_odometer = 0

        self.ignition = 0
        self.moving = 0
        self.sats = 0
        self.gnss_status = 3          # sleep
        self.sleep_mode = 2           # deep sleep
        self.pdop = 0
        self.hdop = 0

        self.ext_voltage = 0          # mV
        self.bat_voltage = self.rng.randint(3900, 4180)
        self.bat_current = 0
        self.bat_level = self.rng.randint(70, 100)

        self.gsm_signal = self.rng.randint(2, 5)
        self.gsm_operator = self.rng.choice([47001, 47002, 47003, 47006])
        self.cell_id = self.rng.randint(1000, 60000)
        self.area_code = self.rng.randint(100, 9999)

        self.din1 = 0
        self.dout1 = 0
        self.trip_active = 0
        self.idling = 0

        self.phase = "deep_sleep"
        self.phase_ticks = 0
        self.pinned = False
        self.pending_event = 0
        self.green_type = 0
        self.green_value = 0
        self.vin = self.rng.choice([
            b"1G1JC5444R7252367", b"WDB9634031L123456", b"JTDKB20U887654321",
        ])

    # -- state machine ---------------------------------------------------

    def _advance_phase(self) -> None:
        """Move through the duty cycle: park, wake, drive, idle, park."""
        if self.pinned:
            self.phase_ticks += 1
            return

        transitions = {
            "deep_sleep":     ("waking",        4),
            "waking":         ("acquiring_fix", 2),
            "acquiring_fix":  ("driving",       3),
            "driving":        ("idling",        12),
            "idling":         ("driving",       3),
            "shutting_down":  ("deep_sleep",    2),
        }
        nxt, duration = transitions[self.phase]

        if self.phase_ticks >= duration:
            # After a stint of idling the bus sometimes parks for good.
            if self.phase == "idling" and self.rng.random() < 0.25:
                nxt = "shutting_down"
            self.phase = nxt
            self.phase_ticks = 0
        else:
            self.phase_ticks += 1

    def _apply_phase(self) -> int:
        """
        Update internal state for the current phase.
        Returns the event id that triggered this record, 0 if periodic.
        """
        event = 0
        p = self.phase

        if p == "deep_sleep":
            self.ignition = 0
            self.moving = 0
            self.speed = 0
            self.sleep_mode = self.rng.choice([2, 3])
            self.gnss_status = 3
            self.sats = 0
            self.pdop = 0
            self.hdop = 0
            self.ext_voltage = self.rng.randint(23_800, 25_200)
            self.bat_current = 0
            self.trip_active = 0
            self.idling = 0
            self.din1 = 0

        elif p == "waking":
            self.ignition = 1
            self.sleep_mode = 0
            self.gnss_status = 2          # on, no fix yet
            self.sats = 0
            self.speed = 0
            self.moving = 0
            self.ext_voltage = self.rng.randint(23_500, 24_600)
            self.bat_current = self.rng.randint(20, 120)
            self.din1 = 1
            if self.phase_ticks == 0:
                event = AVL_IDS["ignition"][0]

        elif p == "acquiring_fix":
            self.ignition = 1
            self.sleep_mode = 0
            self.din1 = 1
            self.gnss_status = self.rng.choice([1, 2])
            self.sats = self.rng.randint(3, 6)
            self.pdop = self.rng.randint(35, 99)      # 3.5 .. 9.9
            self.hdop = self.rng.randint(25, 80)
            self.speed = 0
            self.moving = 0
            self.ext_voltage = self.rng.randint(23_500, 24_600)
            self.bat_current = self.rng.randint(20, 120)

        elif p == "driving":
            self.ignition = 1
            self.moving = 1
            self.sleep_mode = 0
            self.gnss_status = 1
            self.sats = self.rng.randint(7, 14)
            self.pdop = self.rng.randint(8, 25)       # 0.8 .. 2.5
            self.hdop = self.rng.randint(5, 18)
            self.speed = max(5, min(95, self.speed + self.rng.randint(-12, 15)))
            self.trip_active = 1
            self.idling = 0
            self.din1 = 1

            # Alternator charging, so voltage is up and battery is charging.
            self.ext_voltage = self.rng.randint(26_800, 28_400)
            self.bat_current = self.rng.randint(150, 900)
            self.bat_level = min(100, self.bat_level + 1)

            # Move the bus.
            self.heading = (self.heading + self.rng.randint(-25, 25)) % 360
            distance = self.speed * 1000 / 3600 * 30       # 30s tick
            self.total_odometer += int(distance)
            self.trip_odometer += int(distance)
            rad = math.radians(self.heading)
            self.lat += (distance * math.cos(rad)) / 111_320
            self.lon += (distance * math.sin(rad)) / (
                111_320 * math.cos(math.radians(self.lat))
            )
            self.altitude = max(0, self.altitude + self.rng.randint(-2, 2))

            roll = self.rng.random()
            if roll < 0.08:
                event = AVL_IDS["green_driving_type"][0]
                self.green_type = self.rng.choice([1, 2, 3])
                self.green_value = self.rng.randint(15, 60)
            elif roll < 0.12 and self.speed > 70:
                event = AVL_IDS["over_speeding"][0]

        elif p == "idling":
            self.ignition = 1
            self.moving = 0
            self.speed = 0
            self.idling = 1
            self.sleep_mode = 0
            self.din1 = 1
            self.trip_active = 1
            self.gnss_status = 1
            self.sats = self.rng.randint(6, 12)
            self.pdop = self.rng.randint(10, 30)
            self.hdop = self.rng.randint(8, 22)
            self.ext_voltage = self.rng.randint(25_600, 27_200)
            self.bat_current = self.rng.randint(50, 400)
            if self.phase_ticks == 0:
                event = AVL_IDS["idling"][0]

        elif p == "shutting_down":
            self.ignition = 0
            self.moving = 0
            self.speed = 0
            self.sleep_mode = 0
            self.trip_active = 0
            self.idling = 0
            self.din1 = 0
            self.gnss_status = 1
            self.sats = self.rng.randint(5, 10)
            self.ext_voltage = self.rng.randint(23_900, 25_100)
            self.bat_current = 0
            self.trip_odometer = 0
            if self.phase_ticks == 0:
                event = AVL_IDS["ignition"][0]

        self.gsm_signal = max(1, min(5, self.gsm_signal + self.rng.randint(-1, 1)))
        if self.rng.random() < 0.10:
            self.cell_id = self.rng.randint(1000, 60000)

        return event

    # -- IO set per state -------------------------------------------------

    def _io_named(self) -> dict[str, int]:
        """
        The IO set for the current state.

        Base elements go on every record. The rest are conditional, which
        is the behaviour worth simulating: the dict genuinely changes shape
        between records.
        """
        io: dict[str, int] = {
            # always present
            "ignition":         self.ignition,
            "movement":         self.moving,
            "gsm_signal":       self.gsm_signal,
            "sleep_mode":       self.sleep_mode,
            "gnss_status":      self.gnss_status,
            "external_voltage": self.ext_voltage,
            "battery_voltage":  self.bat_voltage,
            "battery_level":    self.bat_level,
            "total_odometer":   self.total_odometer,
        }

        # Operator and cell info drop out when the modem is deeply asleep.
        if self.sleep_mode not in (3, 4):
            io["gsm_operator"] = self.gsm_operator
            io["gsm_cell_id"] = self.cell_id
            io["gsm_area_code"] = self.area_code
            io["data_mode"] = 1 if self.moving else 0

        # Battery current only reported when something is drawing or charging.
        if self.bat_current:
            io["battery_current"] = self.bat_current

        # DOP values are meaningless without a fix, so the device omits them.
        if self.gnss_status == 1 and self.sats > 0:
            io["gnss_pdop"] = self.pdop
            io["gnss_hdop"] = self.hdop
            io["gnss_speed"] = self.speed

        # Ignition-on extras.
        if self.ignition:
            io["digital_input_1"] = self.din1
            io["digital_output_1"] = self.dout1
            io["trip"] = self.trip_active
            io["idling"] = self.idling
            io["trip_odometer"] = self.trip_odometer
            io["analog_input_1"] = self.rng.randint(0, 12_000)

        # Driving-only extras.
        if self.phase == "driving":
            io["digital_input_2"] = self.rng.choice([0, 0, 0, 1])
            io["over_speeding"] = self.speed if self.speed > 70 else 0
            if self.rng.random() < 0.15:
                io["green_driving_type"] = self.rng.choice([1, 2, 3])
                io["green_driving_value"] = self.rng.randint(15, 60)

        # A record triggered by an event must carry the element that
        # triggered it. Real devices do this and a dashboard that keys off
        # event_io_id will look for the value alongside it.
        if self.pending_event == AVL_IDS["green_driving_type"][0]:
            io["green_driving_type"] = self.green_type
            io["green_driving_value"] = self.green_value
        elif self.pending_event == AVL_IDS["over_speeding"][0]:
            io["over_speeding"] = self.speed
        elif self.pending_event == AVL_IDS["crash_detection"][0]:
            io["crash_detection"] = 1

        # Occasional security and diagnostic elements.
        if self.rng.random() < 0.20:
            io["jamming"] = 0
            io["unplug"] = 0
            io["towing_detection"] = 0
        if self.rng.random() < 0.10:
            io["crash_detection"] = 0
            io["immobilizer"] = 0
            io["digital_input_3"] = 0
        if self.rng.random() < 0.08:
            io["ibutton_id"] = self.rng.randint(0, 2**40)

        return io

    # -- record production ------------------------------------------------

    def next_record(self, codec: int, timestamp_ms: int | None = None) -> bytes:
        self._advance_phase()
        event = self._apply_phase()
        self.pending_event = event

        io_map = _resolve(self._io_named())
        self.pending_event = 0

        has_fix = self.gnss_status == 1 and self.sats > 0
        lat = self.lat if has_fix else 0.0
        lon = self.lon if has_fix else 0.0

        priority = 1 if event else 0
        if event == AVL_IDS["crash_detection"][0]:
            priority = 2

        # Codec 8 carries a 1-byte event id, so ids above 255 cannot be
        # signalled as the trigger. Real Codec 8 devices share this limit.
        if codec == CODEC_8 and event > 255:
            event = 0

        variable = None
        if codec == CODEC_8_EXT and self.rng.random() < 0.30:
            variable = {VARIABLE_IDS["vin"][0]: self.vin}

        return build_record(
            codec=codec,
            timestamp_ms=timestamp_ms or int(time.time() * 1000),
            priority=priority,
            lat=lat,
            lon=lon,
            alt=self.altitude if has_fix else 0,
            angle=self.heading if has_fix else 0,
            sats=self.sats,
            speed=self.speed if has_fix else 0,
            event_io_id=event,
            io_map=io_map,
            variable_io=variable,
        )

    def describe(self) -> str:
        return (
            f"phase={self.phase:14s} ign={self.ignition} mov={self.moving} "
            f"spd={self.speed:3d} sats={self.sats:2d} sleep={self.sleep_mode} "
            f"gnss={self.gnss_status} io={len(self._io_named())}"
        )


def maximal_record(codec: int, seq: int = 0) -> bytes:
    """
    Every id in the catalogue at once. Not realistic, but it is the widest
    IO set the parser will ever see and a good structural stress test.
    """
    rng = random.Random(seq)
    io_named: dict[str, int] = {}
    for key, (_id, width, _conf) in AVL_IDS.items():
        limit = 1 << (width * 8)
        io_named[key] = rng.randrange(0, min(limit, 2**32))

    # Keep the boolean-ish ones sane so decoded output stays readable.
    for key in ("ignition", "movement", "digital_input_1", "digital_input_2",
                "digital_input_3", "digital_output_1", "digital_output_2",
                "trip", "idling", "jamming", "unplug", "towing_detection",
                "crash_detection", "immobilizer"):
        io_named[key] = rng.choice([0, 1])
    io_named["gsm_signal"] = rng.randint(1, 5)
    io_named["sleep_mode"] = rng.randint(0, 4)
    io_named["gnss_status"] = rng.randint(0, 3)
    io_named["battery_level"] = rng.randint(0, 100)
    io_named["green_driving_type"] = rng.randint(1, 3)

    variable = None
    if codec == CODEC_8_EXT:
        variable = {VARIABLE_IDS["vin"][0]: b"1G1JC5444R7252367"}

    return build_record(
        codec=codec,
        timestamp_ms=int(time.time() * 1000),
        priority=1,
        lat=23.7808, lon=90.4093, alt=15, angle=180, sats=11, speed=42,
        event_io_id=239,
        io_map=_resolve(io_named),
        variable_io=variable,
    )


# --------------------------------------------------------------------------
# Client
# --------------------------------------------------------------------------

def run_device(host: str, port: int, imei: str, count: int, codec: int,
               batch: int, delay: float, scenario: str,
               verbose: bool = True) -> bool:
    ok = True
    sim = VehicleSimulator(imei)

    if scenario in PHASES:
        sim.phase = scenario
        sim.pinned = True

    try:
        with socket.create_connection((host, port), timeout=10) as sock:
            sock.sendall(struct.pack(">H", len(imei)) + imei.encode("ascii"))
            reply = sock.recv(1)
            if reply != b"\x01":
                print(f"[{imei}] handshake rejected: {reply!r}", file=sys.stderr)
                return False
            if verbose:
                print(f"[{imei}] handshake accepted")

            sent = 0
            while sent < count:
                n = min(batch, count - sent)
                if scenario == "all_io":
                    records = [maximal_record(codec, sent + i) for i in range(n)]
                else:
                    records = [sim.next_record(codec) for _ in range(n)]

                sock.sendall(build_packet(codec, records))
                ack = sock.recv(4)
                if len(ack) != 4:
                    print(f"[{imei}] short ack {ack!r}", file=sys.stderr)
                    return False
                acked = struct.unpack(">I", ack)[0]
                if acked != n:
                    print(f"[{imei}] ack {acked} != sent {n}", file=sys.stderr)
                    ok = False
                elif verbose:
                    print(f"[{imei}] sent {n} acked {acked}  {sim.describe()}")
                sent += n
                if delay:
                    time.sleep(delay)
    except OSError as exc:
        print(f"[{imei}] socket error: {exc}", file=sys.stderr)
        return False
    return ok


# --------------------------------------------------------------------------
# Edge case suite
# --------------------------------------------------------------------------

def _raw_connect(host, port):
    return socket.create_connection((host, port), timeout=5)


def evil_suite(host: str, port: int) -> None:
    imei = "356307042441013"
    handshake = struct.pack(">H", len(imei)) + imei.encode()
    sim = VehicleSimulator(imei)

    cases: list[tuple[str, object]] = []

    def case(name):
        def deco(fn):
            cases.append((name, fn))
            return fn
        return deco

    @case("connect then close immediately")
    def _(s):
        pass

    @case("close during IMEI body")
    def _(s):
        s.sendall(struct.pack(">H", 15) + b"3563")

    @case("bogus IMEI length 0xFFFF")
    def _(s):
        s.sendall(struct.pack(">H", 0xFFFF))

    @case("non-numeric IMEI")
    def _(s):
        s.sendall(struct.pack(">H", 15) + b"HELLOWORLD12345")

    @case("valid handshake, then garbage preamble")
    def _(s):
        s.sendall(handshake); s.recv(1)
        s.sendall(b"\xDE\xAD\xBE\xEF" + struct.pack(">I", 10) + b"x" * 14)

    @case("valid handshake, then absurd length")
    def _(s):
        s.sendall(handshake); s.recv(1)
        s.sendall(struct.pack(">II", 0, 0x7FFFFFFF))

    @case("corrupt CRC")
    def _(s):
        s.sendall(handshake); s.recv(1)
        s.sendall(build_packet(CODEC_8, [sim.next_record(CODEC_8)],
                               corrupt_crc=True))

    @case("record count mismatch")
    def _(s):
        s.sendall(handshake); s.recv(1)
        s.sendall(build_packet(CODEC_8, [sim.next_record(CODEC_8)],
                               mismatch_counts=True))

    @case("unsupported codec 0x99")
    def _(s):
        s.sendall(handshake); s.recv(1)
        s.sendall(build_packet(0x99, [b"\x00" * 20]))

    @case("packet split across many tiny writes")
    def _(s):
        s.sendall(handshake); s.recv(1)
        pkt = build_packet(CODEC_8, [sim.next_record(CODEC_8) for _ in range(3)])
        for i in range(0, len(pkt), 3):
            s.sendall(pkt[i:i + 3])
            time.sleep(0.01)
        ack = s.recv(4)
        print(f"      ack after fragmented send: {struct.unpack('>I', ack)[0]}")

    @case("two packets in one write")
    def _(s):
        s.sendall(handshake); s.recv(1)
        a = build_packet(CODEC_8, [sim.next_record(CODEC_8)])
        b = build_packet(CODEC_8, [sim.next_record(CODEC_8)])
        s.sendall(a + b)
        for _ in range(2):
            ack = s.recv(4)
            print(f"      ack: {struct.unpack('>I', ack)[0]}")

    @case("truncated packet then close")
    def _(s):
        s.sendall(handshake); s.recv(1)
        pkt = build_packet(CODEC_8, [sim.next_record(CODEC_8)])
        s.sendall(pkt[:len(pkt) // 2])

    @case("maximal IO set, every catalogued id at once (Codec 8E)")
    def _(s):
        s.sendall(handshake); s.recv(1)
        s.sendall(build_packet(CODEC_8_EXT, [maximal_record(CODEC_8_EXT)]))
        ack = s.recv(4)
        print(f"      ack: {struct.unpack('>I', ack)[0]}")

    @case("50 records in one packet")
    def _(s):
        s.sendall(handshake); s.recv(1)
        recs = [sim.next_record(CODEC_8) for _ in range(50)]
        s.sendall(build_packet(CODEC_8, recs))
        ack = s.recv(4)
        print(f"      ack: {struct.unpack('>I', ack)[0]}")

    for name, fn in cases:
        print(f"  -> {name}")
        try:
            with _raw_connect(host, port) as s:
                fn(s)
                time.sleep(0.2)
        except OSError as exc:
            print(f"      client side saw: {exc}")
        time.sleep(0.1)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def list_io() -> None:
    print(f"{'key':22s} {'id':>5s} {'bytes':>6s}  confidence")
    print("-" * 52)
    for key, (avl_id, width, conf) in sorted(AVL_IDS.items(), key=lambda x: x[1][0]):
        print(f"{key:22s} {avl_id:5d} {width:6d}  {conf}")
    for key, (avl_id, conf) in VARIABLE_IDS.items():
        print(f"{key:22s} {avl_id:5d} {'var':>6s}  {conf}")
    print()
    print("verified = byte width confirmed against a real Codec 8 packet")
    print("high     = widely documented")
    print("med      = check the Teltonika wiki for your model before trusting")


def main() -> int:
    ap = argparse.ArgumentParser(description="Mock Teltonika device")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5027)
    ap.add_argument("--imei", default="356307042441013")
    ap.add_argument("--devices", type=int, default=1)
    ap.add_argument("--count", type=int, default=20, help="records per device")
    ap.add_argument("--batch", type=int, default=1, help="records per packet")
    ap.add_argument("--codec", type=int, default=8, choices=[8, 142, 16],
                    help="8, 142 (0x8E) or 16 (0x10)")
    ap.add_argument("--delay", type=float, default=0.1)
    ap.add_argument("--scenario", default="cycle",
                    choices=("cycle", "all_io") + PHASES,
                    help="cycle walks the full duty cycle, all_io sends "
                         "every catalogued id, or pin a single phase")
    ap.add_argument("--evil", action="store_true",
                    help="run the malformed input suite instead")
    ap.add_argument("--list-io", action="store_true",
                    help="print the AVL id catalogue and exit")
    args = ap.parse_args()

    if args.list_io:
        list_io()
        return 0

    if args.evil:
        print("running edge case suite")
        evil_suite(args.host, args.port)
        return 0

    codec = {8: CODEC_8, 142: CODEC_8_EXT, 16: CODEC_16}[args.codec]

    if args.devices == 1:
        ok = run_device(args.host, args.port, args.imei, args.count,
                        codec, args.batch, args.delay, args.scenario)
        return 0 if ok else 1

    results: list[bool] = []
    lock = threading.Lock()

    def worker(idx: int):
        imei = str(int(args.imei) + idx)
        ok = run_device(args.host, args.port, imei, args.count, codec,
                        args.batch, args.delay, args.scenario, verbose=False)
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(args.devices)]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - start

    good = sum(results)
    total_records = args.devices * args.count
    print(f"{good}/{len(results)} device(s) succeeded, "
          f"{total_records} record(s) in {elapsed:.2f}s "
          f"({total_records / elapsed:.0f} rec/s)")
    return 0 if good == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
