# Teltonika AVL TCP listener

Decodes Codec 8, Codec 8 Extended and Codec 16 AVL packets from Teltonika
FMB-series trackers and emits one JSON object per AVL record.

Stdlib only. No pip install, no virtualenv needed.

## Files

| File | Purpose |
|---|---|
| `teltonika_listener.py` | The server. Also a standalone hex decoder. |
| `io_definitions.json` | IO id to name/unit/scale table. **Partial, see below.** |
| `mock_device.py` | Fake device for testing without hardware. |
| `teltonika-listener.service` | systemd unit for the droplet. |

## Quick start

```bash
python3 teltonika_listener.py --port 5027
```

Server logs go to stderr and `logs/server.log`. The JSON record stream goes
to stdout and `logs/records.jsonl`. Raw packet hex goes to `logs/raw.jsonl`.

Watch the stream live:

```bash
python3 teltonika_listener.py --port 5027 --quiet | jq .
```

Decode a single packet you pasted from somewhere:

```bash
python3 teltonika_listener.py --decode-hex "000000000000003608010000016B40D8EA30..."
```

That prints an annotated hex dump plus the decoded JSON. On failure it marks
the exact byte offset where parsing broke.

## Testing without hardware

`mock_device.py` runs a vehicle state machine rather than emitting a fixed IO
set, because real trackers vary what they report by state. A bus asleep in the
depot sends about 9 elements with no GPS fix; the same bus driving sends 25 to
30 with a full fix and a charging alternator. Cycling through the phases is
what shakes out off-by-one bugs in the IO group handling.

```bash
# full duty cycle: park -> wake -> acquire fix -> drive -> idle -> park
python3 mock_device.py --count 40

# pin a single phase
python3 mock_device.py --scenario driving
python3 mock_device.py --scenario deep_sleep

# every catalogued id in one record, the widest IO set the parser will see
python3 mock_device.py --scenario all_io --codec 142 --count 1

# Codec 8E (0x8E = 142) and Codec 16
python3 mock_device.py --codec 142
python3 mock_device.py --codec 16

# 50 concurrent devices, 20 records each, 4 records per packet
python3 mock_device.py --devices 50 --count 20 --batch 4

# malformed input suite: bad CRC, garbage preamble, truncated frames,
# fragmented writes, two packets in one write, unknown codec,
# maximal IO set, 50 records in one packet
python3 mock_device.py --evil

# the AVL id catalogue with byte widths and confidence levels
python3 mock_device.py --list-io
```

### Phases and what they emit

| Phase | Ignition | Fix | Notes |
|---|---|---|---|
| `deep_sleep` | off | none | modem may drop operator/cell elements, no DOP |
| `waking` | on | none | ignition event, GNSS on without fix |
| `acquiring_fix` | on | partial | few satellites, high PDOP/HDOP |
| `driving` | on | full | alternator ~27-28V, odometer ticking, green driving events |
| `idling` | on | full | idling event, speed 0, ignition still on |
| `shutting_down` | off | full | ignition event, trip odometer resets |

Event records carry the element that triggered them. If `event_io_id` is 253,
id 253 is in that record's IO set with its value. Priority is raised to high on
events and panic on crash detection.

## Deploying to the droplet

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin teltonika
sudo mkdir -p /opt/teltonika /var/log/teltonika
sudo cp teltonika_listener.py io_definitions.json /opt/teltonika/
sudo chown -R teltonika:teltonika /opt/teltonika /var/log/teltonika

sudo cp teltonika-listener.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now teltonika-listener

sudo journalctl -u teltonika-listener -f
tail -f /var/log/teltonika/records.jsonl | jq .
```

Open the port. DigitalOcean has a cloud firewall in the control panel as well
as whatever runs on the droplet, and both need the rule:

```bash
sudo ufw allow 5027/tcp
```

Then point the devices at it. On the FMB920 that is the GPRS settings block,
via Teltonika Configurator or the SMS/GPRS command `setparam 2004:<droplet_ip>`
and `setparam 2005:5027`. Confirm the parameter ids against your firmware
version before sending, they have moved between releases.

## Log rotation

Handled in-process by `RotatingFileHandler`, so no logrotate config needed:

- `server.log` 50 MB x 5
- `records.jsonl` 200 MB x 10
- `raw.jsonl` 200 MB x 10

Pass `--no-raw` to stop storing raw hex once you trust the parser. Keep it on
for now, it is what lets you replay real traffic when a decode looks wrong.

## The IO definitions file is incomplete

`io_definitions.json` covers 37 AVL ids. The FMB920 publishes a few hundred.
Open the Teltonika wiki page for your exact model, find the AVL ID table, and
transcribe the rest into the same JSON shape.

Unlisted ids are not dropped. They decode with `"name": null` and the raw
integer as the value, so nothing is lost, it is just unlabelled.

Every entry carries a `confidence` field, which the parser ignores and you
should not:

- **verified** — byte width confirmed against a real Codec 8 packet.
  Ids 1, 21, 66, 78, 241.
- **high** — widely documented, I am confident.
- **med** — plausible from docs, check the wiki before trusting decoded values.
  Ids 10, 181, 182, 205, 206, 246, 247, 248, 249, 252, 254, 256.

`python3 mock_device.py --list-io` prints the same table with byte widths.

The `med` entries most likely to matter to you are 181/182 (PDOP/HDOP) and
205/206 (cell id, area code). PDOP and HDOP are listed here as 2-byte with a
0.1 multiplier; if your device reports them as 1-byte, the parser will still
decode the packet but the values will be wrong, so confirm those two first.

## Security note

By default the server accepts any IMEI that connects. That is fine on a test
droplet and wrong in production, because anyone who finds the port can inject
fake positions for arbitrary IMEIs. Before this touches real data:

```bash
printf '356307042441013\n356307042441020\n' > allowed_imeis.txt
python3 teltonika_listener.py --allowlist allowed_imeis.txt
```

Unknown IMEIs get a `0x00` rejection byte and the socket closes.

## Design notes

**ACK ordering.** The server writes the decoded records out before sending the
4-byte acknowledgement. Teltonika devices drop records from internal memory
once acknowledged, so acknowledging first would lose data on a write failure.

**Parse failures are not acknowledged.** The device retransmits the same batch,
which is what you want while the parser is still being shaken out. The failure
is logged with an annotated hex dump and an offset marker.

**Framing.** TCP is a byte stream. The reader pulls 8 bytes, parses the declared
length, then reads exactly that many plus 4. Both split packets and coalesced
packets are handled and covered by the `--evil` suite.

**Threading.** One thread per connection. Comfortable to roughly 500 concurrent
devices on a modest droplet. Past that, move to `asyncio` or run several
processes behind `SO_REUSEPORT`. The parser is pure and has no shared state, so
it ports across unchanged.

**Next step is the sink.** `RecordSink.emit()` is deliberately the only place
that knows about output format. When you move to TimescaleDB, replace its body
with a batched `COPY` and leave everything else alone. Batch the inserts, do not
write per packet, and make them idempotent on `(device_id, timestamp, event_io_id)`
because retransmits are normal.

## Verification status

- CRC-16/IBM implementation validated against a known-good reference Codec 8
  packet. Checksum matches, so the algorithm and the data-field-length
  semantics are confirmed.
- Codec 8 record layout confirmed against that same packet: declared IO count
  matched parsed count, footer record count matched header, zero trailing bytes.
  A one-byte layout error anywhere would have tripped the trailing-byte check.
- Codec 8E and Codec 16 layouts are exercised by round-trip tests against
  `mock_device.py`, which proves the encoder and decoder agree but does **not**
  prove either matches real hardware. Both are written from the documented field
  layouts. Codec 16's generation-type byte in particular is worth confirming
  against a real device before you rely on it.
- Since your fleet is FMB920, the Codec 8 path is the one that matters on day
  one, and that one is verified against real bytes.

Regression run covering all of the above: 1176 records decoded across three
codecs, IO counts ranging 9 to 36, 36 distinct named elements, zero declared
vs parsed count mismatches, zero unnamed ids, zero tracebacks, 50 concurrent
devices at roughly 2000 records/sec.
