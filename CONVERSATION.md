# Teltonika FMB920 Fleet Dashboard: Design and Implementation Notes

Conversation transcript, ReserveDaily dev team.

**Date:** 25 August 2026
**Topic:** Database selection, Teltonika AVL protocol, and a production TCP listener
**Deliverables produced:** `teltonika_listener.py`, `mock_device.py`, `io_definitions.json`, `teltonika-listener.service`, `README.md`

---

## Table of contents

1. [Which database for telematics data](#1-which-database-for-telematics-data)
2. [Which protocol Teltonika uses](#2-which-protocol-teltonika-uses)
3. [Is JSON parsing necessary](#3-is-json-parsing-necessary)
4. [Viewing AVL packets in human readable form](#4-viewing-avl-packets-in-human-readable-form)
5. [Building the TCP listener](#5-building-the-tcp-listener)
6. [Making the mock device emit varied IO sets](#6-making-the-mock-device-emit-varied-io-sets)
7. [Verification summary](#verification-summary)
8. [Open items](#open-items)

---

## 1. Which database for telematics data

> **Context given:** Working with Teltonika FMB920 devices. Building a web dashboard where bus owners get an overview of their fleet. AVL packets use Codec 8; some newer models use Codec 8E or Codec 16. Packets need parsing and storage. Dashboard shows vehicle data plus related information.

### Short answer

PostgreSQL + TimescaleDB + PostGIS. One database for both the time-series AVL stream and the relational fleet data, with Redis in front for last-known-position.

### Why this fits telematics specifically

The workload is actually two workloads:

**Time-series, append-only, huge.** AVL position records. High insert rate, almost never updated, queried as "device X between time A and B". TimescaleDB is a Postgres extension that turns a table into an auto-partitioned hypertable, so this stays fast without hand-written partition management.

**Relational, small, transactional.** Owners, fleets, vehicles, drivers, SIM cards, geofences, alert rules, maintenance schedules, billing. This needs joins, foreign keys, and real transactions. Plain Postgres.

Splitting these across two systems means losing joins and gaining a sync problem. Timescale keeps both in one place, and PostGIS gives proper geofence containment, route distance, and snap-to-road prep without another service.

### Volume check

FMB920 at a 30s moving interval is about 2,880 records per device per day. At 1,000 buses that is roughly 2.9M rows/day, 1B rows/year. Comfortable for Timescale on a single well-specced node, especially with compression on (10-20x typical on this data shape, since consecutive rows from one device are highly similar).

Past roughly 20-30k devices, revisit and add ClickHouse for the analytics layer. Do not design for that on day one.

### The schema decision that actually matters

The hard part is not choosing the DB, it is modeling IO elements. Codec 8 gives variable 1/2/4/8-byte IO elements, Codec 8E adds 2-byte IO IDs plus variable-length elements, Codec 16 adds generation type. Three options:

1. **Column per IO ID.** Fast, but a 200-column table and a migration every time a device profile changes.
2. **Tall table** `(device_id, ts, io_id, value)`. Flexible, but explodes row count and every query becomes a pivot.
3. **Hybrid (recommended).** Promote the IO IDs actually queried into real columns, dump the rest into JSONB.

For a bus fleet the hot ones are typically ignition (239), movement (240), GSM signal (21), external voltage (66), total odometer (16), trip odometer (199), digital inputs (1-4), plus whatever the fuel or door sensor reports.

```sql
CREATE TABLE avl_records (
  device_id      BIGINT      NOT NULL,
  ts             TIMESTAMPTZ NOT NULL,   -- packet timestamp, UTC
  received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  codec_id       SMALLINT    NOT NULL,   -- 0x08, 0x8E, 0x10
  priority       SMALLINT,
  location       GEOGRAPHY(POINT, 4326),
  altitude       SMALLINT,
  angle          SMALLINT,
  satellites     SMALLINT,
  speed          SMALLINT,
  event_io_id    INTEGER,
  ignition       BOOLEAN,
  movement       BOOLEAN,
  ext_voltage    INTEGER,
  total_odometer BIGINT,
  gsm_signal     SMALLINT,
  io             JSONB       NOT NULL DEFAULT '{}'
);

SELECT create_hypertable('avl_records', 'ts', chunk_time_interval => INTERVAL '1 day');
CREATE UNIQUE INDEX ON avl_records (device_id, ts, event_io_id);
```

Compression policy segmented by `device_id`, ordered by `ts DESC`, compressing chunks older than about 7 days.

The unique index matters: devices retransmit if the ACK is lost, so inserts must be idempotent. Use `ON CONFLICT DO NOTHING`.

### Two things worth doing from day one

**Store the raw hex.** A separate table with `device_id`, `received_at`, `codec_id`, and the raw packet bytes, retained 30-90 days. Parsing bugs will surface (Codec 8E variable-length elements and signed lat/lng handling are the usual suspects), and replaying real traffic beats guessing.

**Never insert per packet.** The TCP listener should push parsed records into a queue (Redis Streams or NATS is enough at this scale, Kafka if replay guarantees are wanted) and a consumer batch-inserts via `COPY` every 1-2 seconds. Per-packet inserts will be the first bottleneck, well before the database itself is stressed.

### What not to use

| Option | Why not |
|---|---|
| MongoDB | Flexible IO schema is appealing and plenty of telematics platforms run on it, but PostGIS-quality geospatial is lost and relational integrity is still wanted for billing and fleet hierarchy. |
| InfluxDB | Built for metrics, not entity-rich records. Weak geospatial, and IO-element cardinality will hurt. |
| ClickHouse | Great analytics engine, wrong tool for the OLTP side (last-known-position updates, alert state, user CRUD). Add later as a sink if analytics outgrow Timescale. |
| Plain Postgres, no extension | Fine for a prototype, but partitioning and retention become hand-rolled within months. |

---

## 2. Which protocol Teltonika uses

### Transport

TCP/IP or UDP/IP over GPRS. The FMB920 is a 2G device (GSM/GPRS), so it opens a socket to whatever host:port is configured in the GPRS settings and pushes AVL packets. TCP is the default and the right choice. SMS exists as a fallback transport, but carries commands and occasional data, not the main stream.

All multi-byte fields are **big endian**.

### TCP session flow

**1. IMEI handshake.** Device connects and immediately sends:

```
[2 bytes: IMEI length, always 0x000F = 15]
[15 bytes: IMEI as ASCII, e.g. "356307042441013"]
```

**2. Server replies with one byte.** `0x01` = accepted, `0x00` = rejected (connection closed). This is the authentication hook. Reject IMEIs not in the `devices` table.

**3. Device sends the AVL data packet:**

```
[4 bytes: 0x00000000  preamble]
[4 bytes: Data Field Length]
[1 byte : Codec ID       0x08 | 0x8E | 0x10]
[1 byte : Number of Data 1]
[N bytes: AVL Data array]
[1 byte : Number of Data 2   must equal Number of Data 1]
[4 bytes: CRC-16]
```

**4. Server ACKs with 4 bytes** containing the number of records accepted, big endian. It must match Number of Data 1 or the device treats the batch as failed and retransmits the whole thing on the next attempt.

The connection stays open. The device keeps sending packets over the same socket until the link drops or the configured timeout expires.

### Details that will bite you

**CRC-16 is CRC-16/IBM (ARC).** Polynomial `0xA001`, init `0x0000`, reflected. Computed over the range from Codec ID through Number of Data 2 inclusive, so it excludes the preamble and length field. It is a 16-bit value transmitted in a 4-byte field with the two high bytes zeroed.

**Data Field Length** counts from Codec ID through Number of Data 2. It excludes the preamble and the CRC field itself. So total bytes on the wire = `8 + Data Field Length + 4`.

**Framing is on you.** TCP is a stream, not a message boundary. A single `read()` can return half a packet or two and a half packets. Read 8 bytes, parse the length, then read exactly that many plus 4 more. Do not assume one read equals one packet, and do not assume the preamble will be aligned to the start of the buffer after a partial read.

**ACK only after durably accepting the records.** If the server ACKs and then the insert fails, that data is gone. The device has already dropped it from internal memory.

**Retransmission is normal.** If the ACK is lost in transit, the device resends the identical batch. This is exactly why `ON CONFLICT DO NOTHING` on `(device_id, ts, event_io_id)` matters.

### UDP differs

Not the same packet. UDP adds a channel header and drops both the preamble and the CRC, since UDP has its own checksum:

```
[2 bytes: Length      of everything after this field]
[2 bytes: Packet ID]
[1 byte : Packet Type  0x01]
[1 byte : AVL Packet ID]
[2 bytes: IMEI length]
[N bytes: IMEI ASCII]
[1 byte : Codec ID]
[1 byte : Number of Data 1]
[...AVL Data...]
[1 byte : Number of Data 2]
```

The ACK echoes the Packet ID and AVL Packet ID back along with the accepted count. Verify the exact ACK byte layout against the Teltonika wiki before implementing; confidence on that one is lower than on the TCP side.

Unless there is a specific reason (very tight data plan, high-loss network), stick with TCP. Ordering and delivery come free, and the retransmit semantics are simpler to reason about.

### Codec IDs

| ID | Codec | Notes |
|---|---|---|
| `0x08` | Codec 8 | Standard. 1-byte IO IDs. FMB920 default. |
| `0x8E` | Codec 8 Extended | 2-byte IO IDs plus variable-length IO elements. |
| `0x10` | Codec 16 | Adds event generation type. |
| `0x0C` | Codec 12 | GPRS commands, server to device and replies. |
| `0x0D` / `0x0E` | Codec 13 / 14 | Command-related. |

The parser should branch on the codec ID rather than assuming, since newer models in the fleet will send `0x8E`.

Note that Codec 12 arrives on the **same socket** as AVL data. To send commands to a device (reboot, request current position, change config), write a Codec 12 frame to the open connection and the reply comes back the same way. Worth designing for now rather than retrofitting, since it means the TCP handler cannot just be a one-way parser.

---

## 3. Is JSON parsing necessary

### Short answer

No. JSON is a serialization choice, not a parsing requirement. The wire format is binary, and the parser should go straight from bytes to a typed struct. JSON only earns its place where something outside the process needs to read the data.

The mistake to watch for is treating "parse" and "convert to JSON" as the same step. Decoding a packet into a JSON object and then reading fields back out of it to build the insert pays for a serialize plus deserialize round trip that bought nothing.

### Where it belongs in the pipeline

| Stage | JSON? | Reasoning |
|---|---|---|
| TCP listener, byte decoding | No | Decode directly into a typed record. Every field has a known type and offset, so nothing dynamic justifies a schemaless intermediate. |
| Queue between listener and DB writer | Optional | JSON works and is easy to debug, but MessagePack or Protobuf is roughly a third the size and parses faster. At 1,000 buses this is not a bottleneck either way. |
| Database, hot columns | No | `ignition`, `speed`, `total_odometer`, `location` are typed columns. Indexes and comparisons need native types. |
| Database, cold IO elements | **Yes, JSONB** | Hundreds of possible IO IDs, most never queried, some device-model-specific. No migration when a new sensor appears, and GIN indexes still allow querying into it. |
| API to the dashboard | Yes | A boundary between systems. The frontend expects it. |
| Raw packet archive | Neither | Store the original hex or bytea exactly as received. Wrapping in JSON adds size and loses nothing but readability, which a hex dump gives back anyway. |

### The size argument

A Codec 8 record with ten IO elements is on the order of 50 to 60 bytes on the wire. The same record as JSON with descriptive key names runs several hundred bytes, so roughly an order of magnitude. That matters for queue throughput and long-term retention. It does not matter much for a single API response.

### Where JSON genuinely helps

**Debugging.** Having the parser emit a JSON representation of any packet on demand is worth building. When a Codec 8E variable-length element parses wrong, the decoded structure next to the raw hex beats stepping through a debugger.

**Third-party integrations.** If a bus owner or partner wants a webhook feed, JSON is what they expect.

**IO element definitions.** Not the data, the metadata. A JSON or YAML file mapping IO IDs to name, type, unit, and multiplier per device model is much nicer to maintain than a hardcoded switch statement, especially with FMB920s and newer models with different IO tables in the same fleet.

### Concretely

Binary in, typed struct, batch `COPY` into Timescale with the hot fields as columns and the leftover IO elements as a JSONB blob, JSON out at the API layer. Add a `toJSON()` on the record type for logging and for the packet replay tool, but never make it part of the write path.

---

## 4. Viewing AVL packets in human readable form

### The principle

One parser, two renderers. Do not write a separate debug decoder alongside the production parser, because they will drift and then the debug view will lie exactly when it matters most. The parser produces a typed record; a renderer walks that record and prints it.

To make an annotated dump useful, the parser needs to record byte offsets as it goes, not just values. That means the decode step emits something like `(field_name, offset, length, raw_bytes, decoded_value)` per field rather than just assigning to a struct. Cheap to add up front, painful to retrofit.

### What the output should look like

An annotated hex dump beats a pretty-printed object, because the whole point is correlating decoded values back to positions in the raw bytes.

```
0000  00 00 00 00                 preamble
0004  00 00 00 36                 data field length = 54
0008  08                          codec = 8 (Codec8)
0009  01                          record count = 1

  --- record 0 ---
000A  00 00 01 6B 40 D8 EA 30     timestamp = 1560161086000
                                            = 2019-06-10 10:04:46 UTC
0012  01                          priority = 1 (high)
0013  00 00 00 00                 longitude = 0 raw = 0.0000000
0017  00 00 00 00                 latitude  = 0 raw = 0.0000000
001B  00 00                       altitude  = 0 m
001D  00 00                       angle     = 0 deg
001F  00                          satellites = 0
0020  00 00                       speed     = 0 km/h
...
```

Two things this gives that a JSON object does not: an off-by-one offset becomes visible, and the raw field sits next to the interpreted field. Latitude and longitude are signed int32 scaled by 1e7, and sign handling on southern or western coordinates is a classic source of silently wrong data. Seeing the raw bytes next to the decoded value allows checking the arithmetic directly.

### The IO element problem

Raw IO output is unreadable:

```
io[239] = 1
io[240] = 0
io[21]  = 4
io[66]  = 12873
```

What is wanted:

```
io[239] Ignition          = ON
io[240] Movement          = STOPPED
io[21]  GSM Signal        = 4 (of 5)
io[66]  External Voltage  = 12873 mV = 12.873 V
```

That requires an IO definition table: id, name, type, unit, multiplier, and for enums a value map. Keep it as a JSON or YAML file keyed by device model, since the FMB920's IO table differs from newer models. Load it at startup, fall back to `io[<id>] = <raw>` for unknown IDs so an unrecognized sensor degrades gracefully instead of crashing.

### Where to hook it

**A CLI that takes hex.** `decode --hex "000000000000003608..."`. Highest-value tool to build during development. Paste a packet from a log, see it decoded in a second.

**On parse failure, always.** When the parser throws, log the full annotated dump plus raw hex at error level. Otherwise "parse error at offset 47" arrives with no context and no way to reproduce.

**A replay command against the raw archive.** This is why raw packets get stored. When a bus owner reports wrong odometer readings, go look at what the device actually sent.

**An admin route in the dashboard, eventually.** Same renderer, HTML output. Useful for support staff without shell access.

---

## 5. Building the TCP listener

> **Requirement:** A proper TCP listener, Python for now, runnable on a DigitalOcean droplet, JSON object output, all edge cases handled, proper logging, threading if necessary, multiple simultaneous devices.

### Verification before trusting the parser

Rather than assuming the byte layouts were right, the CRC was used as a correctness oracle. A reference Codec 8 packet was run through the CRC implementation:

```
total bytes: 66   data field length: 54   expected total: 66
crc in packet: 0xC7CF
crc computed : 0xC7CF
MATCH
```

The CRC matching confirms two things at once: the CRC-16/IBM implementation is correct, and the data-field-length semantics (which bytes are covered, which are excluded) are correct. A misremembered packet would almost certainly have failed.

Decoding that same packet end to end:

```json
{
  "codec_id": 8,
  "codec": "Codec8",
  "record_count": 1,
  "records": [{
    "timestamp": "2019-06-10T10:04:46.000+00:00",
    "priority": 1,
    "priority_name": "high",
    "gps": { "longitude": 0.0, "latitude": 0.0, "satellites": 0, "valid": false },
    "event_io_id": 1,
    "io_count_declared": 5,
    "io_count_parsed": 5,
    "io": {
      "21":  { "name": "GSM Signal",          "value": 3 },
      "1":   { "name": "Digital Input 1",     "state": "high" },
      "66":  { "name": "External Voltage",    "value": 24.079, "unit": "V" },
      "241": { "name": "Active GSM Operator", "value": 24602 },
      "78":  { "name": null,                  "value": 0 }
    }
  }]
}
```

Five declared IO elements, five parsed, matching footer count, zero trailing bytes. A one-byte layout error anywhere in the GPS element or IO groups would have tripped the trailing-byte check. 24.079V external voltage is a plausible bus electrical system.

### Byte widths extracted from the same packet

Rather than guessing IO element widths, they were read directly out of the verified packet's IO section:

```
IO section hex: 0105021503010101425E0F01F10000601A014E0000000000000000

event_io_id = 1
total count = 5
N1 count    = 2 -> pairs: 15 03  01 01          (ids 21, 1  -> 1 byte each)
N2 count    = 1 -> pairs: 42 5E0F               (id 66      -> 2 bytes)
N4 count    = 1 -> pairs: F1 0000601A           (id 241     -> 4 bytes)
N8 count    = 1 -> pairs: 4E 0000000000000000   (id 78      -> 8 bytes)
```

So ids 21 and 1 are 1-byte, id 66 is 2-byte, id 241 is 4-byte, id 78 (iButton) is 8-byte. These are marked `verified` in the catalogue. Everything else is marked `high` or `med` confidence.

### Architecture

- **One thread per connection.** Comfortable to roughly 500 concurrent devices on a modest droplet. Past that, move to `asyncio` or run several processes behind `SO_REUSEPORT`. The parser is pure and has no shared state, so it ports across unchanged.
- **`RecordSink.emit()` is the only place that knows about output format.** When moving to TimescaleDB, replace its body with a batched `COPY` and leave everything else alone.
- **Stdlib only.** No pip install, no virtualenv on the droplet.

### Two design decisions worth knowing about

**Records are written before the ACK is sent.** Devices drop data from internal memory once acknowledged, so acknowledging first would lose records on a write failure.

**Parse failures are not acknowledged.** The device retransmits, which is what is wanted while the parser is still being shaken out. The failure is logged with an annotated hex dump and an offset marker:

```
ERROR  parse failed: CRC mismatch: got 0x0000C954, computed 0x36AB
raw (72 bytes):
0000  00 00 00 00 00 00 00 3C 08 01 00 00 01 A0 35 46  |.......<......5F|
...
0040  B7 99 00 01 00 00 C9 54                          |.......T|   <-- offset 68
```

### Edge cases handled

| Case | Behaviour |
|---|---|
| Partial reads / packet split across writes | `recv_exact` loop; verified with 3-byte writes |
| Two packets in one write | Two separate ACKs; verified |
| Connection closed mid-frame | Logged, socket closed |
| Idle timeout | Configurable, default 600s |
| Bogus IMEI length (0xFFFF) | Rejected, connection dropped |
| Non-numeric or non-ASCII IMEI | Rejected |
| Bad preamble | Stream declared desynchronised, connection closed (no resync attempted) |
| Implausible data field length | Rejected before allocation |
| CRC mismatch | Not ACKed, hex dump logged |
| Record count header/footer mismatch | Not ACKed, hex dump logged |
| Trailing bytes after last record | Treated as a parse failure (parser and packet disagree on layout) |
| Unknown codec | Logged with hex, connection closed |
| Codec 12/13/14 command frames | Logged, not parsed as AVL |
| Zero records declared | Rejected |
| Graceful shutdown | SIGTERM/SIGINT, joins handler threads with a 10s deadline |
| Connection limit | Configurable, default 500 |

### Test results

- All three codecs decode, including a 17-byte variable-length element in Codec 8E
- 50 concurrent devices, 1000 records, zero loss, zero tracebacks, ~2900 rec/s
- Every malformed input case handled without a crash
- SIGTERM shuts down cleanly

### Security note

The server accepts any IMEI by default. Fine on a test droplet, wrong in production, because anyone who finds the port can inject fake positions for arbitrary IMEIs. Use `--allowlist` with a file of permitted IMEIs before this touches real data.

---

## 6. Making the mock device emit varied IO sets

> **Requirement:** The IO dict varies depending on conditions. The mock needs as many IO dicts as possible. Must have: ignition status, movement, gsm, sleep, gnss, pdop, hdop, ext voltage, bat voltage, bat current, gsm operator, odometer. Include other IO the device can send.

The observation was correct, and it changed the design. A mock that always sends the same dict does not exercise the parser properly. It was rebuilt as a vehicle state machine rather than a random field generator.

### Phases and what they emit

| Phase | Ignition | Fix | Characteristic |
|---|---|---|---|
| `deep_sleep` | off | none | operator/cell elements drop when modem sleeps, no DOP |
| `waking` | on | none | ignition event, GNSS on without fix |
| `acquiring_fix` | on | partial | 3-6 satellites, PDOP 3.5-9.9 |
| `driving` | on | full | alternator 27-28V, odometer ticking, green driving events |
| `idling` | on | full | idling event, speed 0, ignition still on |
| `shutting_down` | off | full | ignition event, trip odometer resets |

The conditional logic mirrors real device behaviour:

- PDOP and HDOP are omitted without a fix, because they are meaningless
- Battery current only appears when something is drawing or charging
- Operator, cell id, and area code drop out when the modem is deeply asleep
- Trip, idling, and analog input only appear with ignition on
- Green driving and over-speeding only appear while driving

IO count ranges **9 to 36** across the cycle.

### The 12 must-haves, all present

| Element | AVL ID | Bytes | Confidence |
|---|---|---|---|
| Ignition | 239 | 1 | high |
| Movement | 240 | 1 | high |
| GSM Signal | 21 | 1 | **verified** |
| Sleep Mode | 200 | 1 | high |
| GNSS Status | 69 | 1 | high |
| GNSS PDOP | 181 | 2 | med |
| GNSS HDOP | 182 | 2 | med |
| External Voltage | 66 | 2 | **verified** |
| Battery Voltage | 67 | 2 | high |
| Battery Current | 68 | 2 | high |
| Active GSM Operator | 241 | 4 | **verified** |
| Total Odometer | 16 | 4 | high |

### Plus 24 more

Digital Input 1-3 (1, 2, 3), Analog Input 1 (9), GNSS Speed (24), iButton ID (78), Data Mode (80), Battery Level (113), Digital Output 1-2 (179, 180), Trip Odometer (199), GSM Cell ID (205), GSM Area Code (206), Towing Detection (246), Crash Detection (247), Immobilizer (248), Jamming (249), Trip (250), Idling (251), Unplug (252), Green Driving Type (253), Green Driving Value (254), Over Speeding (255), VIN (256, variable length, Codec 8E only).

### Two bugs the testing caught

**Event records were missing their trigger.** The first run produced a record with `event_io_id=253` but no id 253 in its IO set. Real devices include the element that triggered the record, and a dashboard keying off `event_io_id` will look for the value alongside it. Fixed, then verified: 6 event records, 0 missing their trigger.

```
event=239 (Ignition)           prio=high  value=on
event=253 (Green Driving Type) prio=high  value=harsh_braking
event=251 (Idling)             prio=high  value=idling
event=253 (Green Driving Type) prio=high  value=harsh_acceleration
event=251 (Idling)             prio=high  value=idling
event=251 (Idling)             prio=high  value=idling
```

**Pinned phases had incoherent state.** Running `--scenario acquiring_fix` reported `ign=0 sleep=2`, because those phases relied on earlier phases to set those fields. Fine in a full cycle, wrong when pinned. Fixed by setting ignition, sleep mode, and trip state explicitly in each phase.

### Decoded comparison, sleep vs driving

**Deep sleep, 14 elements, no fix:**

```
  16  Total Odometer         233401 m
  21  GSM Signal             4
  66  External Voltage       24.319 V
  67  Battery Voltage        4.09 V
  69  GNSS Status            gnss_sleep
  78  iButton ID             915941218302
  80  Data Mode              home_on_stop
 113  Battery Level          74 %
 200  Sleep Mode             deep_sleep
 205  GSM Cell ID            56966
 206  GSM Area Code          6328
 239  Ignition               off
 240  Movement               stopped
 241  Active GSM Operator    47002
```

**Driving, 25 elements, full fix, green driving event:**

```
   1  Digital Input 1        high
   2  Digital Input 2        low
   9  Analog Input 1         9.536 V
  16  Total Odometer         233882 m
  21  GSM Signal             4
  24  GNSS Speed             28 km/h
  66  External Voltage       28.332 V     <- alternator charging
  67  Battery Voltage        4.09 V
  68  Battery Current        574 mA
  69  GNSS Status            gnss_on_with_fix
  80  Data Mode              home_on_moving
 113  Battery Level          79 %
 179  Digital Output 1       off
 181  GNSS PDOP              2.0
 182  GNSS HDOP              0.9
 199  Trip Odometer          481 m
 200  Sleep Mode             no_sleep
 205  GSM Cell ID            49297
 206  GSM Area Code          6328
 239  Ignition               on
 240  Movement               moving
 241  Active GSM Operator    47002
 250  Trip                   trip_start
 251  Idling                 moving
 255  Over Speeding          0 km/h
```

### Usage

```bash
python3 mock_device.py --count 40                    # full duty cycle
python3 mock_device.py --scenario driving            # pin a phase
python3 mock_device.py --scenario all_io --codec 142 # every catalogued id at once
python3 mock_device.py --devices 50 --count 20       # concurrency
python3 mock_device.py --evil                        # malformed input suite
python3 mock_device.py --list-io                     # catalogue with confidence
```

---

## Verification summary

| Check | Result |
|---|---|
| CRC-16/IBM against a real Codec 8 packet | Match (0xC7CF) |
| Codec 8 record layout | Verified: 5 declared = 5 parsed, footer matched, zero trailing bytes |
| Codec 8E, Codec 16 layouts | Round-trip tested against the mock only, **not** against hardware |
| Byte widths for ids 1, 21, 66, 78, 241 | Extracted from the verified packet |
| Records decoded in full regression | 1176 across three codecs |
| IO count range observed | 9 to 36 |
| Distinct named IO elements | 36 |
| Declared vs parsed count mismatches | 0 |
| Unnamed IO ids | 0 |
| Tracebacks | 0 |
| Concurrency | 50 devices, ~2000-2900 rec/s |
| Event records missing their trigger | 0 |
| Malformed input cases | All handled, no crashes |

### What is verified versus what is assumed

**Verified against real bytes.** The CRC implementation, the data-field-length semantics, the Codec 8 record layout, and the byte widths of five IO ids. Since the fleet is FMB920, the Codec 8 path is the one that matters on day one, and that one is confirmed.

**Round-trip tested only.** Codec 8E and Codec 16. The mock encoder and the listener decoder agree, which proves internal consistency but not that either matches hardware. Both were written from documented field layouts. Codec 16's generation-type byte in particular should be confirmed against a real device.

**Documented but unconfirmed.** The `med`-confidence IO ids: 10, 181, 182, 205, 206, 246, 247, 248, 249, 252, 254, 256.

---

## Open items

### 1. Verify the `med`-confidence IO ids

Most urgent: **181 and 182 (PDOP and HDOP)**, currently modelled as 2-byte with a 0.1 multiplier. If the firmware reports them as 1-byte, packets will still decode but every element after them in that group shifts, producing silently wrong values. Confirm those two first.

Then 205/206 (cell id, area code), which are modelled as 2-byte.

### 2. Transcribe the full AVL ID table

`io_definitions.json` covers 37 ids. The FMB920 publishes a few hundred. Unlisted ids still decode, they just come out with `"name": null` and the raw integer, so nothing is lost. But the dashboard will want labels.

### 3. Enable the IMEI allowlist before production

Currently accepts any device that connects.

### 4. Confirm the FMB920 GPRS configuration parameters

The server address and port parameter ids have moved between firmware releases. Verify before sending `setparam` commands.

### 5. Replace the sink with a Timescale writer

`RecordSink.emit()` is the single integration point. Batch the inserts, do not write per packet, and make them idempotent on `(device_id, timestamp, event_io_id)` because retransmits are normal.

### 6. Design for Codec 12 on the same socket

Command frames arrive on the same connection as AVL data. The TCP handler currently logs them without parsing. If remote commands (reboot, request position, change config) are wanted, that path needs building, and it means the handler cannot stay a one-way parser.

### 7. Send real packets back for checking

Once traffic is flowing into `logs/raw.jsonl`, a few hex lines can be checked against the parser. That is the fastest way to catch anything the mock cannot.

---

## Deliverables

| File | Purpose |
|---|---|
| `teltonika_listener.py` | The server. Also a standalone hex decoder via `--decode-hex`. |
| `mock_device.py` | Vehicle state machine, edge case suite, AVL id catalogue. |
| `io_definitions.json` | 37 IO ids with names, units, multipliers, enums, confidence levels. |
| `teltonika-listener.service` | systemd unit with hardening for the droplet. |
| `README.md` | Deployment, usage, design notes, verification status. |

Stdlib only. No pip install required.
