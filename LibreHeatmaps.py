import argparse
import base64
import csv
import datetime as dt
import gzip
import hmac
import hashlib
import html
import io
import json
import math
import re
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import sqlite3
import shutil
import struct
import tempfile
import threading
from collections import OrderedDict
from array import array
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse
from urllib.request import Request, urlopen
import webbrowser
import zipfile
import zlib

VERSION = "1.0.0"
FORMATS = (".fit", ".fit.gz", ".gpx", ".gpx.gz", ".tcx", ".tcx.gz")
MAX_MEMBER = 512 * 1024 * 1024
MAX_UPLOAD = 8 * 1024 * 1024 * 1024
FIT_EPOCH = dt.datetime(1989, 12, 31, tzinfo=dt.timezone.utc)
MAX_POINTS = 2_000_000
TOKEN = secrets.token_urlsafe(24)
STATE_LOCK = threading.RLock()
JOB = {"status": "idle", "processed": 0, "total": 0, "added": 0, "skipped": 0, "failed": 0, "message": "", "metadata_rows": 0, "updated": 0}
VIEW_CACHE = OrderedDict()
VIEW_CACHE_BYTES = 0
MAX_VIEW_CACHE_BYTES = 32 * 1024 * 1024
# Reuse projected GPS coordinates across pan/zoom requests, with a hard memory cap.
TRACK_CACHE = OrderedDict()
TRACK_CACHE_BYTES = 0
MAX_TRACK_CACHE_BYTES = 96 * 1024 * 1024
CACHE_STATE = {"epoch": 0}
DB_PATH = None
UPDATE_READY = None
UPDATE_LOCK = threading.Lock()
RELEASE_REPO = "Gubna-Tech/LibreHeatmaps"
ACTIVE_CLIENTS = {}
CLIENT_LOCK = threading.RLock()
SERVER_STARTED = time.monotonic()


class BoundedStream:
    def __init__(self, stream, limit=MAX_MEMBER):
        self.stream = stream
        self.limit = limit
        self.total = 0

    def read(self, n=-1):
        if n < 0:
            n = self.limit - self.total + 1
        data = self.stream.read(min(n, self.limit - self.total + 1))
        self.total += len(data)
        if self.total > self.limit:
            raise ValueError("Activity exceeds the size safety limit")
        return data


def read_exact(stream, size):
    data = stream.read(size)
    if len(data) != size:
        raise ValueError("Truncated activity file")
    return data


def points_fit(stream, summary=None):
    header_size = read_exact(stream, 1)[0]
    if header_size < 12 or header_size > 64:
        raise ValueError("Invalid FIT header")
    header = bytes([header_size]) + read_exact(stream, header_size - 1)
    if header[8:12] != b".FIT":
        raise ValueError("Not a FIT activity")
    remaining = struct.unpack_from("<I", header, 4)[0]
    if remaining > MAX_MEMBER:
        raise ValueError("FIT payload too large")
    definitions = {}
    record_previous = None
    record_max_speed = None
    last_record_timestamp = None
    first_stamp = last_stamp = None
    ascent = moving = 0.0
    last_altitude = None
    altitude_samples = moving_samples = 0
    while remaining:
        h = read_exact(stream, 1)[0]
        remaining -= 1
        if h & 0x80:
            local = (h >> 5) & 3
            compressed_offset = h & 31
            is_definition = False
        else:
            local = h & 15
            compressed_offset = None
            is_definition = bool(h & 0x40)
        if is_definition:
            block = read_exact(stream, 5)
            remaining -= 5
            endian = ">" if block[1] else "<"
            global_number = struct.unpack(endian + "H", block[2:4])[0]
            count = block[4]
            details = read_exact(stream, count * 3)
            remaining -= count * 3
            fields = [(details[i], details[i + 1], details[i + 2]) for i in range(0, len(details), 3)]
            if h & 0x20:
                dev_count = read_exact(stream, 1)[0]
                remaining -= 1
                dev = read_exact(stream, dev_count * 3)
                remaining -= len(dev)
                fields.extend((255, dev[i + 1], dev[i + 2]) for i in range(0, len(dev), 3))
            definitions[local] = (global_number, endian, fields)
        else:
            if local not in definitions:
                raise ValueError("FIT record missing definition")
            global_number, endian, fields = definitions[local]
            record_len = sum(field[1] for field in fields)
            if record_len > remaining:
                raise ValueError("Invalid FIT record length")
            record = read_exact(stream, record_len)
            remaining -= record_len
            if global_number == 18 and summary is not None:
                at = 0
                for number, size, base in fields:
                    fmt = {2: "B", 4: "H", 6: "I", 5: "i"}.get(base & 31)
                    if number in (7, 8, 9, 14, 15, 22) and fmt and size == struct.calcsize(fmt):
                        raw = struct.unpack(endian + fmt, record[at:at + size])[0]
                        if raw >= 0 and raw != (1 << (8 * size)) - 1:
                            name, factor = {7: ("elapsed_seconds", 1000), 8: ("moving_seconds", 1000), 9: ("recorded_distance_m", 100), 14: ("avg_speed", 1000), 15: ("max_speed", 1000), 22: ("elevation_m", 1)}[number]
                            if raw > 0:
                                summary[name] = raw / factor
                    at += size
                continue
            if global_number != 20:
                continue
            lat = lon = stamp = altitude = None
            pos = 0
            for number, size, base in fields:
                if number in (0, 1) and size == 4 and (base & 31) == 5:
                    value = struct.unpack(endian + "i", record[pos:pos + 4])[0]
                    if value != 2147483647:
                        if number == 0:
                            lat = value * (180.0 / 2147483648)
                        else:
                            lon = value * (180.0 / 2147483648)
                if number == 2 and size == 2 and (base & 31) == 4:
                    raw_alt = struct.unpack(endian + "H", record[pos:pos + 2])[0]
                    if raw_alt != 65535:
                        altitude = raw_alt / 5.0 - 500
                if number == 78 and size == 4 and (base & 31) == 6:
                    raw_alt = struct.unpack(endian + "I", record[pos:pos + 4])[0]
                    if raw_alt != 4294967295:
                        altitude = raw_alt / 5.0 - 500
                if number == 6 and size == 2 and (base & 31) == 4:
                    raw_speed = struct.unpack(endian + "H", record[pos:pos + 2])[0]
                    if raw_speed != 65535:
                        speed = raw_speed / 1000
                        if math.isfinite(speed):
                            record_max_speed = max(record_max_speed or 0, speed)
                if number == 73 and size == 4 and (base & 31) == 6:
                    raw_speed = struct.unpack(endian + "I", record[pos:pos + 4])[0]
                    if raw_speed != 4294967295:
                        speed = raw_speed / 1000
                        if math.isfinite(speed):
                            record_max_speed = max(record_max_speed or 0, speed)
                if number == 253 and size == 4 and (base & 31) == 6:
                    value = struct.unpack(endian + "I", record[pos:pos + 4])[0]
                    if value != 0xffffffff:
                        stamp = value
                pos += size
            if compressed_offset is not None and stamp is None and last_record_timestamp is not None:
                stamp = (last_record_timestamp & ~31) + compressed_offset
                if stamp < last_record_timestamp:
                    stamp += 32
            if stamp is not None:
                last_record_timestamp = stamp
            if lat is not None and lon is not None:
                if stamp is not None:
                    if first_stamp is None:
                        first_stamp = stamp
                    last_stamp = stamp
                if altitude is not None and math.isfinite(altitude) and -500 <= altitude <= 10000:
                    altitude_samples += 1
                    if last_altitude is not None:
                        rise = altitude - last_altitude
                        if .75 <= rise <= 30:
                            ascent += rise
                    last_altitude = altitude
                if record_previous is not None and stamp is not None:
                    old_lat, old_lon, old_stamp = record_previous
                    if old_stamp is not None and 0 < stamp - old_stamp <= 120:
                        distance = haversine((old_lat, old_lon), (lat, lon))
                        if .7 <= distance / (stamp - old_stamp) <= 25:
                            moving += stamp - old_stamp
                            moving_samples += 1
                record_previous = (lat, lon, stamp)
                yield lat, lon, stamp
    read_exact(stream, 2)
    if summary is not None:
        if summary.get("max_speed") is None and record_max_speed is not None:
            summary["max_speed"] = record_max_speed
        if not summary.get("elapsed_seconds") and first_stamp is not None and last_stamp is not None and last_stamp > first_stamp:
            summary["elapsed_seconds"] = last_stamp - first_stamp
        if not summary.get("moving_seconds") and moving_samples >= 2:
            summary["moving_seconds"] = moving
            summary["moving_source"] = "track-estimate"
        if not summary.get("elevation_m") and altitude_samples >= 3 and ascent > 0:
            summary["elevation_m"] = round(ascent, 1)
            summary["elevation_source"] = "track-estimate"


def parse_track_timestamp(value):
    if not value:
        return None
    try:
        timestamp = dt.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
        return (timestamp - FIT_EPOCH).total_seconds()
    except ValueError:
        return None


def summarize_track_samples(samples, summary=None):
    first = last = None
    previous = None
    last_altitude = None
    moving = ascent = 0.0
    moving_count = altitude_count = 0
    for item in samples:
        if item is None:
            previous = last_altitude = None
            yield None
            continue
        lat, lon, stamp, altitude = item
        if stamp is not None:
            if first is None:
                first = stamp
            last = stamp
        if altitude is not None and math.isfinite(altitude):
            altitude_count += 1
            if last_altitude is not None:
                increase = altitude - last_altitude
                if .75 <= increase <= 30:
                    ascent += increase
            last_altitude = altitude
        if previous is not None and stamp is not None and previous[2] is not None:
            delta = stamp - previous[2]
            if 0 < delta <= 120:
                distance = haversine((previous[0], previous[1]), (lat, lon))
                if .7 <= distance / delta <= 25:
                    moving += delta
                    moving_count += 1
        previous = (lat, lon, stamp)
        yield lat, lon, stamp
    if summary is not None:
        if not summary.get("elapsed_seconds") and first is not None and last is not None and last > first:
            summary["elapsed_seconds"] = round(last - first)
        if not summary.get("moving_seconds") and moving_count >= 2:
            summary["moving_seconds"] = round(moving)
            summary["moving_source"] = "track-estimate"
        if not summary.get("elevation_m") and altitude_count >= 3 and ascent > 0:
            summary["elevation_m"] = round(ascent, 1)
            summary["elevation_source"] = "track-estimate"


def points_gpx(stream, summary=None):
    import xml.etree.ElementTree as ET
    def items():
        for event, element in ET.iterparse(stream, events=("start", "end")):
            tag = element.tag.rsplit("}", 1)[-1]
            if event == "start" and tag in ("trkseg", "rte"):
                yield None
            elif event == "end" and tag in ("trkpt", "rtept"):
                try:
                    lat = float(element.attrib["lat"])
                    lon = float(element.attrib["lon"])
                    altitude = stamp = None
                    for child in element:
                        field = child.tag.rsplit("}", 1)[-1]
                        if field == "ele" and child.text:
                            try:
                                altitude = float(child.text)
                            except ValueError:
                                pass
                        elif field == "time" and child.text:
                            stamp = parse_track_timestamp(child.text)
                    yield lat, lon, stamp, altitude
                except (ValueError, KeyError):
                    pass
                element.clear()
            elif event == "end" and tag in ("trkseg", "rte"):
                element.clear()
    yield from summarize_track_samples(items(), summary)


def points_tcx(stream, summary=None):
    import xml.etree.ElementTree as ET
    def items():
        for event, element in ET.iterparse(stream, events=("end",)):
            tag = element.tag.rsplit("}", 1)[-1]
            if tag == "Trackpoint":
                lat = lon = altitude = stamp = None
                for child in element.iter():
                    field = child.tag.rsplit("}", 1)[-1]
                    if field == "LatitudeDegrees" and child.text:
                        lat = float(child.text)
                    elif field == "LongitudeDegrees" and child.text:
                        lon = float(child.text)
                    elif field == "AltitudeMeters" and child.text:
                        altitude = float(child.text)
                    elif field == "Time" and child.text:
                        stamp = parse_track_timestamp(child.text)
                if lat is not None and lon is not None:
                    yield lat, lon, stamp, altitude
                element.clear()
            elif tag == "Track":
                yield None
                element.clear()
    yield from summarize_track_samples(items(), summary)

def haversine(a, b):
    lat1, lon1 = a
    lat2, lon2 = b
    x = math.radians(lat2 - lat1)
    y = math.radians(lon2 - lon1)
    q = math.sin(x / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(y / 2) ** 2
    return 12742017.6 * math.asin(min(1.0, math.sqrt(q)))


def decode_track(stream, filename, summary=None):
    lower = filename.lower()
    stream = BoundedStream(stream)
    if lower.endswith(".gz"):
        stream = BoundedStream(gzip.GzipFile(fileobj=stream))
    if lower.endswith((".fit", ".fit.gz")):
        iterator = points_fit(stream, summary)
    elif lower.endswith((".gpx", ".gpx.gz")):
        iterator = points_gpx(stream, summary)
    else:
        iterator = points_tcx(stream, summary)
    packed = bytearray()
    prev = None
    remaining = 25.0
    first_stamp = None
    bbox = [90.0, 180.0, -90.0, -180.0]
    count = 0
    distance_total = 0.0
    last_emit = None

    def append(point):
        nonlocal count, last_emit
        last_emit = point
        lat, lon = point
        packed.extend(struct.pack("<ii", round(lat * 1e6), round(lon * 1e6)))
        bbox[0] = min(bbox[0], lat)
        bbox[1] = min(bbox[1], lon)
        bbox[2] = max(bbox[2], lat)
        bbox[3] = max(bbox[3], lon)
        count += 1
        if count > MAX_POINTS:
            raise ValueError("Activity has more than two million sampled points")

    for value in iterator:
        if value is None:
            if prev is not None:
                if last_emit is not None and haversine(last_emit, prev) > 0.3:
                    append(prev)
                packed.extend(struct.pack("<ii", 2147483647, 2147483647))
            prev = None
            remaining = 25.0
            continue
        lat, lon, stamp = value
        if stamp is not None and first_stamp is None:
            first_stamp = stamp
        if not (math.isfinite(lat) and math.isfinite(lon) and -85.0511 <= lat <= 85.0511 and -180 <= lon <= 180):
            continue
        current = (lat, lon)
        if prev is None:
            append(current)
            prev = current
            remaining = 25.0
            continue
        length = haversine(prev, current)
        if length > 2000.0:
            if last_emit is not None and haversine(last_emit, prev) > 0.3:
                append(prev)
            packed.extend(struct.pack("<ii", 2147483647, 2147483647))
            append(current)
            prev = current
            remaining = 25.0
            continue
        distance_total += length
        if length > 0.01:
            traveled = 0.0
            while traveled + remaining <= length + 1e-8:
                target = traveled + remaining
                ratio = min(1.0, target / length)
                append((prev[0] + (current[0] - prev[0]) * ratio,
                        prev[1] + (current[1] - prev[1]) * ratio))
                traveled = target
                remaining = 25.0
            remaining -= max(0.0, length - traveled)
        prev = current
    if prev is not None and last_emit is not None and haversine(last_emit, prev) > 0.3:
        append(prev)
    if count < 2:
        return None
    start = (FIT_EPOCH + dt.timedelta(seconds=first_stamp)).date().isoformat() if first_stamp is not None else None
    return zlib.compress(bytes(packed), 6), count, bbox, start, round(distance_total)


def parse_date(value):
    if not value:
        return None
    value = value.strip()
    for pattern in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%b %d, %Y, %I:%M:%S %p", "%b %d, %Y, %I:%M %p", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S %Z"):
        try:
            return dt.datetime.strptime(value, pattern).date().isoformat()
        except ValueError:
            continue
    if len(value) >= 10 and value[4:5] == "-" and value[7:8] == "-":
        return value[:10]
    return None


def csv_number(value):
    if value is None or not str(value).strip():
        return None
    try:
        result = float(str(value).replace(",", "").strip())
        return result if math.isfinite(result) and result >= 0 else None
    except ValueError:
        return None


def csv_duration(value):
    value = str(value or "").strip()
    if ":" not in value:
        return csv_number(value)
    parts = value.split(":")
    if len(parts) not in (2, 3):
        return None
    try:
        numbers = [float(v) for v in parts]
        return sum(v * 60**i for i,v in enumerate(reversed(numbers))) if all(math.isfinite(v) and v >= 0 for v in numbers) else None
    except ValueError:
        return None


def metadata_from_csv(stream):
    text = io.TextIOWrapper(stream, encoding="utf-8-sig", errors="replace", newline="")
    reader = csv.reader(text)
    header = next(reader, None)
    if not header:
        return {}
    positions = {}
    for index, key in enumerate(header):
        positions.setdefault(re.sub(r"[^a-z0-9]", "", key.casefold()), []).append(index)

    def values(row, *names):
        for name in names:
            for index in reversed(positions.get(re.sub(r"[^a-z0-9]", "", name.casefold()), ())):
                if index < len(row) and row[index].strip():
                    yield row[index].strip()

    def cell(row, *names):
        return next(values(row, *names), "")

    def numeric(row, parser, *names):
        zero = None
        for value in values(row, *names):
            parsed = parser(value)
            if parsed is None:
                continue
            if parsed > 0:
                return parsed
            zero = parsed
        return zero

    metadata = {}
    for n, row in enumerate(reader):
        if n >= 150000:
            raise ValueError("Activity CSV exceeds the row limit")
        filename = cell(row, "Filename", "File Name", "Activity Filename", "File").replace("\\", "/").lstrip("/")
        activity_id = cell(row, "Activity ID", "Activity Id", "ActivityID", "ID")
        if not filename and not activity_id:
            continue
        info = {
            "name": cell(row, "Activity Name", "Name"),
            "sport": cell(row, "Activity Type", "Sport Type", "Type"),
            "day": parse_date(cell(row, "Activity Date", "Activity Date Local", "Date", "Start Date")),
            "recorded_distance_m": numeric(row, csv_number, "Distance (m)", "Distance", "Distance Meters"),
            "moving_seconds": numeric(row, csv_duration, "Moving Time", "Moving Time (s)", "Timer Time", "Moving Time Seconds"),
            "elapsed_seconds": numeric(row, csv_duration, "Elapsed Time", "Elapsed Time (s)"),
            "elevation_m": numeric(row, csv_number, "Elevation Gain", "Total Elevation Gain", "Total Ascent", "Elev Gain", "Elevation Gain (m)"),
            "avg_heart_rate": numeric(row, csv_number, "Average Heart Rate", "Avg Heart Rate"),
            "avg_speed": numeric(row, csv_number, "Average Speed"),
            "max_speed": numeric(row, csv_number, "Max Speed"),
            "bike": cell(row, "Activity Gear", "Gear Name", "Bike Name"),
            "gear_id": cell(row, "Gear ID", "Bike", "Gear"),
        }
        if info["bike"] and info["bike"].casefold() == info["gear_id"].casefold():
            info["bike"] = ""
        if info["moving_seconds"] is not None:
            info["moving_source"] = "strava"
        if info["elevation_m"] is not None:
            info["elevation_source"] = "strava"
        if filename:
            metadata[filename.casefold()] = info
            metadata[filename.rsplit("/", 1)[-1].casefold()] = info
        if activity_id:
            matched = re.search(r"\d{7,}", activity_id)
            if matched:
                metadata["id:" + matched.group()] = info
    return metadata


def gear_names_from_csv(stream):
    reader = csv.DictReader(io.TextIOWrapper(stream, encoding="utf-8-sig", errors="replace", newline=""))
    names = {}
    for count, row in enumerate(reader):
        if count >= 5000:
            raise ValueError("Gear CSV exceeds the row limit")
        normalized = {re.sub(r"[^a-z0-9]", "", key.casefold()): str(value or "").strip() for key, value in row.items() if key}
        gear_id = next((normalized[k] for k in ("bikeid", "gearid", "id") if normalized.get(k)), "")
        name = next((normalized[k] for k in ("bikename", "gearname", "name", "nickname") if normalized.get(k)), "")
        if gear_id and name:
            names[gear_id.casefold()] = name[:160]
    return names


def metadata_csv(zfile):
    options = [x for x in zfile.infolist() if x.filename.lower().endswith("activities.csv") and x.file_size < 32 * 1024 * 1024]
    if not options:
        return {}
    with zfile.open(min(options, key=lambda m: len(m.filename))) as stream:
        lookup = metadata_from_csv(stream)
    gear = {}
    for entry in zfile.infolist():
        if entry.filename.lower().endswith(("bikes.csv", "gear.csv")) and 0 < entry.file_size < 1024 * 1024:
            with zfile.open(entry) as stream:
                gear.update(gear_names_from_csv(stream))
    seen = set()
    for info in lookup.values():
        if id(info) in seen:
            continue
        seen.add(id(info))
        name = info.get("bike", "").strip()
        key = (info.get("gear_id") or "").casefold()
        if key in gear and (not name or name.casefold() == key or name.casefold() in ("unknown", "none")):
            info["bike"] = gear[key]
        elif name.casefold() in gear:
            info["bike"] = gear[name.casefold()]
    return lookup

def apply_metadata(con, lookup):
    updates = 0
    for ident, filename in con.execute("SELECT id,filename FROM activities"):
        info = get_metadata(filename, lookup)
        if not info:
            continue
        con.execute("""UPDATE activities SET name=COALESCE(NULLIF(?,''),name), sport=COALESCE(NULLIF(?,''),sport),
            day=COALESCE(?,day), recorded_distance_m=COALESCE(?,recorded_distance_m),
            moving_seconds=COALESCE(?,moving_seconds), elapsed_seconds=COALESCE(?,elapsed_seconds),
            elevation_m=COALESCE(?,elevation_m), avg_heart_rate=COALESCE(?,avg_heart_rate),
            avg_speed=COALESCE(?,avg_speed), max_speed=COALESCE(?,max_speed),
            bike=COALESCE(NULLIF(?,''),bike), gear_id=COALESCE(NULLIF(?,''),gear_id),
            moving_source=CASE WHEN ? IS NOT NULL THEN 'strava' ELSE moving_source END,
            elevation_source=CASE WHEN ? IS NOT NULL THEN 'strava' ELSE elevation_source END
            WHERE id=?""", ((info.get("name") or "")[:300], (info.get("sport") or "")[:80], info.get("day"),
            info.get("recorded_distance_m"), info.get("moving_seconds"), info.get("elapsed_seconds"),
            info.get("elevation_m"), info.get("avg_heart_rate"), info.get("avg_speed"), info.get("max_speed"),
            (info.get("bike") or "")[:160], (info.get("gear_id") or "")[:120],
            info.get("moving_seconds"), info.get("elevation_m"), ident))
        updates += 1
    con.commit()
    clear_render_caches()
    return updates


def get_metadata(filename, lookup):
    name = filename.replace("\\", "/")
    base = name.rsplit("/", 1)[-1]
    ident = re.match(r"^(\d{7,})(?:\.|_|$)", base)
    return (lookup.get(name.casefold()) or lookup.get(base.casefold()) or (lookup.get("id:" + ident.group(1)) if ident else None) or {})


def strava_activity_id(filename):
    name = filename.replace("\\", "/").rsplit("/", 1)[-1]
    match = re.match(r"^(\d{7,})(?:\.|_|$)", name)
    return match.group(1) if match else None


def db_connect():
    con = sqlite3.connect(str(DB_PATH), timeout=40)
    con.execute("PRAGMA busy_timeout=40000")
    return con


def init_db():
    with db_connect() as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("CREATE TABLE IF NOT EXISTS activities (id INTEGER PRIMARY KEY, unique_key TEXT NOT NULL UNIQUE, filename TEXT NOT NULL, name TEXT NOT NULL, sport TEXT NOT NULL, day TEXT, distance_m INTEGER, point_count INTEGER NOT NULL, west REAL NOT NULL, south REAL NOT NULL, east REAL NOT NULL, north REAL NOT NULL, coordinates BLOB NOT NULL)")
        columns = {row[1] for row in con.execute("PRAGMA table_info(activities)")}
        for name in ("recorded_distance_m", "moving_seconds", "elapsed_seconds", "elevation_m", "avg_heart_rate", "avg_speed", "max_speed"):
            if name not in columns:
                con.execute("ALTER TABLE activities ADD COLUMN {} REAL".format(name))
        for name in ("bike", "gear_id", "moving_source", "elevation_source"):
            if name not in columns:
                con.execute("ALTER TABLE activities ADD COLUMN {} TEXT".format(name))
        con.execute("CREATE INDEX IF NOT EXISTS activity_filters ON activities (sport, day)")
        con.execute("CREATE INDEX IF NOT EXISTS activity_bbox ON activities (west, east, south, north)")
        if "activity_id" not in columns:
            con.execute("ALTER TABLE activities ADD COLUMN activity_id TEXT")
            for row_id, filename in con.execute("SELECT id,filename FROM activities").fetchall():
                activity_id = strava_activity_id(filename)
                if activity_id:
                    con.execute("UPDATE activities SET activity_id=? WHERE id=?", (activity_id, row_id))
        con.execute("CREATE INDEX IF NOT EXISTS activity_id_idx ON activities(activity_id)")
        con.execute("CREATE INDEX IF NOT EXISTS activity_filename_idx ON activities(filename)")
        for row_id, filename in con.execute("SELECT id,filename FROM activities WHERE activity_id IS NULL OR activity_id='' ").fetchall():
            activity_id = strava_activity_id(filename)
            if activity_id:
                con.execute("UPDATE activities SET activity_id=? WHERE id=?", (activity_id, row_id))
        con.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")



DEFAULT_APPEARANCE = {"mode":"dark","online":True,"palette":"classic","routes":True,"heat":True,"radius":5,"definition":78,"intensity":120,"opacity":84}


def normalize_appearance(value):
    if not isinstance(value, dict) or set(value) - set(DEFAULT_APPEARANCE):
        raise ValueError("Invalid appearance settings")
    result = dict(DEFAULT_APPEARANCE)
    for key, item in value.items():
        if key == "mode":
            if item not in ("dark", "light", "satellite"):
                raise ValueError("Invalid map background")
        elif key == "palette":
            if item not in ("classic", "ember", "violet", "ocean", "forest"):
                raise ValueError("Invalid heat palette")
        elif key in ("online", "routes", "heat"):
            if not isinstance(item, bool):
                raise ValueError("Invalid toggle setting")
        elif key in ("radius", "definition", "intensity", "opacity"):
            if isinstance(item, bool) or not isinstance(item, (int, float)) or not math.isfinite(item):
                raise ValueError("Invalid display slider")
            limits = {"radius":(0,30),"definition":(0,100),"intensity":(0,200),"opacity":(0,100)}
            lower, upper = limits[key]
            if not lower <= item <= upper:
                raise ValueError("Appearance setting outside supported range")
            item = int(item)
        result[key] = item
    return result


def load_appearance():
    with db_connect() as con:
        result = con.execute("SELECT value FROM app_settings WHERE key='appearance'").fetchone()
    try:
        return normalize_appearance(json.loads(result[0])) if result else None
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def store_appearance(value):
    appearance = normalize_appearance(value)
    with db_connect() as con:
        con.execute("INSERT INTO app_settings(key,value) VALUES('appearance',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (json.dumps(appearance, separators=(',', ':')),))
    return appearance


def quantile(values, proportion):
    values = sorted(values)
    if not values:
        raise ValueError("No coordinates")
    index = (len(values)-1)*proportion
    low = int(index)
    high = min(len(values)-1,low+1)
    return values[low]+(values[high]-values[low])*(index-low)


def focus_activity_bounds(boxes):
    if not boxes:
        return None
    centers = [((b[0] + b[2]) / 2, (b[1] + b[3]) / 2) for b in boxes]
    latitude = quantile([p[1] for p in centers], .5)
    cosine = max(.25, math.cos(math.radians(latitude)))
    cell_lat = .12
    cell_lon = cell_lat / cosine
    cells = {}
    for lon, lat in centers:
        key = (math.floor(lon / cell_lon), math.floor(lat / cell_lat))
        cells.setdefault(key, []).append((lon, lat))
    selected = []
    best = -1
    for x, y in cells:
        nearby = [p for i in (-1, 0, 1) for j in (-1, 0, 1) for p in cells.get((x + i, y + j), ())]
        if len(nearby) > best:
            selected = nearby
            best = len(nearby)
    if not selected:
        selected = centers
    longitude = quantile([p[0] for p in selected], .5)
    latitude = quantile([p[1] for p in selected], .5)
    cosine = max(.25, math.cos(math.radians(latitude)))
    longitude_spread = (quantile([p[0] for p in selected], .8) - quantile([p[0] for p in selected], .2)) * cosine
    latitude_spread = quantile([p[1] for p in selected], .8) - quantile([p[1] for p in selected], .2)
    radius = min(.16, max(.045, max(longitude_spread, latitude_spread) * .9))
    half_lon = radius / cosine
    return [max(-180, longitude - half_lon), max(-85, latitude - radius), min(180, longitude + half_lon), min(85, latitude + radius)]

def recover_fit_summary(stream, filename):
    summary = {}
    source = BoundedStream(stream)
    if filename.lower().endswith(".gz"):
        source = BoundedStream(gzip.GzipFile(fileobj=source))
    for _ in points_fit(source, summary):
        pass
    return summary


def insert_activity(con, source, source_key, stream, info):
    filename = source.replace("\\", "/")
    activity_id = strava_activity_id(filename)
    existing = con.execute("SELECT id,moving_seconds,elapsed_seconds,elevation_m,recorded_distance_m,day,avg_speed,max_speed FROM activities WHERE unique_key=?", (source_key,)).fetchone()
    if not existing and activity_id:
        existing = con.execute("SELECT id,moving_seconds,elapsed_seconds,elevation_m,recorded_distance_m,day,avg_speed,max_speed FROM activities WHERE activity_id=? ORDER BY id LIMIT 1", (activity_id,)).fetchone()
    if not existing:
        existing = con.execute("SELECT id,moving_seconds,elapsed_seconds,elevation_m,recorded_distance_m,day,avg_speed,max_speed FROM activities WHERE filename=? ORDER BY id LIMIT 1", (filename,)).fetchone()
    if existing:
        if filename.lower().endswith((".fit", ".fit.gz")) and any(existing[i] is None and info.get(key) is None for i,key in ((1,"moving_seconds"),(2,"elapsed_seconds"),(3,"elevation_m"),(6,"avg_speed"),(7,"max_speed"))):
            try:
                recovered = recover_fit_summary(stream, filename)
                info = {**recovered, **{k: v for k, v in info.items() if v is not None and v != ""}}
            except (ValueError, OSError, EOFError, gzip.BadGzipFile) as exc:
                print("[LibreHeatmaps] FIT summary unavailable for {}: {}".format(Path(filename).name, exc), flush=True)
        if info:
            con.execute("""UPDATE activities SET name=COALESCE(NULLIF(?,''),name),sport=COALESCE(NULLIF(?,''),sport),day=COALESCE(?,day),recorded_distance_m=COALESCE(?,recorded_distance_m),moving_seconds=COALESCE(?,moving_seconds),elapsed_seconds=COALESCE(?,elapsed_seconds),elevation_m=COALESCE(?,elevation_m),avg_heart_rate=COALESCE(?,avg_heart_rate),avg_speed=COALESCE(?,avg_speed),max_speed=COALESCE(?,max_speed),bike=COALESCE(NULLIF(?,''),bike),gear_id=COALESCE(NULLIF(?,''),gear_id),moving_source=COALESCE(?,moving_source),elevation_source=COALESCE(?,elevation_source) WHERE id=?""", ((info.get("name") or "")[:300],(info.get("sport") or "")[:80],info.get("day"),info.get("recorded_distance_m"),info.get("moving_seconds"),info.get("elapsed_seconds"),info.get("elevation_m"),info.get("avg_heart_rate"),info.get("avg_speed"),info.get("max_speed"),(info.get("bike") or "")[:160],(info.get("gear_id") or "")[:120],info.get("moving_source"),info.get("elevation_source"),existing[0]))
            con.commit()
        return "skipped"
    embedded = {}
    result = decode_track(stream, filename, embedded)
    info = {**embedded, **{k: v for k, v in info.items() if v is not None and v != ""}}
    if result is None:
        return "no-gps"
    compressed, count, bbox, day, distance = result
    name = info.get("name") or Path(filename).name.split(".")[0]
    sport = info.get("sport") or "Unknown"
    con.execute("INSERT OR IGNORE INTO activities (unique_key,filename,name,sport,day,distance_m,point_count,west,south,east,north,coordinates,recorded_distance_m,moving_seconds,elapsed_seconds,elevation_m,avg_heart_rate,avg_speed,max_speed,bike,gear_id,moving_source,elevation_source,activity_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (source_key, filename, name[:300], sport[:80], info.get("day") or day, distance, count, bbox[1], bbox[0], bbox[3], bbox[2], compressed, info.get("recorded_distance_m"), info.get("moving_seconds"), info.get("elapsed_seconds"), info.get("elevation_m"), info.get("avg_heart_rate"), info.get("avg_speed"), info.get("max_speed"), (info.get("bike") or "")[:160], (info.get("gear_id") or "")[:120], info.get("moving_source"), info.get("elevation_source"), activity_id))
    con.commit()
    clear_render_caches()
    return "added"


def set_job(**changes):
    with STATE_LOCK:
        JOB.update(changes)


def import_job(path, display_name):
    added = skipped = failed = 0
    no_gps = 0
    try:
        with db_connect() as con:
            if display_name.lower().endswith(".csv"):
                with open(path, "rb") as file:
                    lookup = metadata_from_csv(file)
                set_job(total=1, message="Updating existing activity statistics")
                updated = apply_metadata(con, lookup)
                set_job(processed=1, added=0, skipped=0, failed=0, updated=updated)
            elif display_name.lower().endswith(".zip"):
                with zipfile.ZipFile(path) as archive:
                    entries = [m for m in archive.infolist() if not m.is_dir() and m.filename.lower().endswith(FORMATS)]
                    if len(entries) > 30000:
                        raise ValueError("Too many activity files in one archive")
                    if sum(m.file_size for m in entries) > 12 * 1024 * 1024 * 1024:
                        raise ValueError("Uncompressed activity data exceeds the safety limit")
                    lookup = metadata_csv(archive)
                    if not lookup:
                        print("[LibreHeatmaps] This ZIP does not contain activities.csv. Bike names and official Strava totals may be unavailable.", flush=True)
                    set_job(total=len(entries), message="Reading Strava export", metadata_rows=len({id(v) for v in lookup.values()}))
                    for index, member in enumerate(entries, 1):
                        set_job(processed=index - 1, message=Path(member.filename).name[:110])
                        try:
                            if member.file_size > 1000 * max(1, member.compress_size) and member.file_size > 1024 * 1024:
                                raise ValueError("Archive entry has an unsafe compression ratio")
                            if member.file_size > MAX_MEMBER:
                                raise ValueError("Activity entry exceeds size limit")
                            key = hashlib.sha256((member.filename.rsplit("/", 1)[-1].casefold() + ":" + str(member.CRC) + ":" + str(member.file_size)).encode()).hexdigest()
                            with archive.open(member) as file:
                                outcome = insert_activity(con, member.filename, key, file, get_metadata(member.filename, lookup))
                            if outcome == "added":
                                added += 1
                            elif outcome == "no-gps":
                                no_gps += 1
                            else:
                                skipped += 1
                        except Exception as exc:
                            failed += 1
                            print("[LibreHeatmaps] Could not import {}: {}".format(member.filename, exc), flush=True)
                        set_job(processed=index, added=added, skipped=skipped, failed=failed, no_gps=no_gps)
                    if lookup:
                        set_job(updated=apply_metadata(con, lookup))
                    else:
                        set_job(updated=0)
            else:
                set_job(total=1, message="Reading activity")
                size = os.stat(path).st_size
                key = hashlib.sha256((display_name.casefold() + ":" + str(size) + ":" + hashlib.sha256(Path(path).read_bytes()[:1024 * 1024]).hexdigest()).encode()).hexdigest()
                with open(path, "rb") as file:
                    outcome = insert_activity(con, display_name, key, file, {})
                if outcome == "added":
                    added = 1
                elif outcome == "no-gps":
                    no_gps = 1
                else:
                    skipped = 1
                set_job(processed=1, added=added, skipped=skipped, no_gps=no_gps)
        set_job(status="done", message="Import complete")
    except Exception as exc:
        set_job(status="error", message=str(exc)[:400])
        print("[LibreHeatmaps] Import stopped: {}".format(exc), flush=True)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def statistics():
    with db_connect() as con:
        count, points, west, south, east, north = con.execute("SELECT COUNT(*), COALESCE(SUM(point_count),0), MIN(west), MIN(south), MAX(east), MAX(north) FROM activities").fetchone()
        types = con.execute("SELECT sport,COUNT(*) FROM activities GROUP BY sport ORDER BY COUNT(*) DESC").fetchall()
        bikes = con.execute("SELECT DISTINCT bike FROM activities WHERE bike IS NOT NULL AND bike<>'' ORDER BY bike COLLATE NOCASE").fetchall()
        first, last = con.execute("SELECT MIN(day),MAX(day) FROM activities WHERE day IS NOT NULL").fetchone()
    focus = None
    if count:
        with db_connect() as con:
            boxes = con.execute("SELECT west,south,east,north FROM activities WHERE west > -180 AND east < 180 AND south > -85 AND north < 85 AND east>=west AND north>=south").fetchall()
        focus = focus_activity_bounds(boxes)
    with db_connect() as con:
        first_launch = con.execute("SELECT value FROM app_settings WHERE key=?", ("welcome_dismissed",)).fetchone() is None
    return {"count": count, "mapped_gps_count":count, "points": points, "bounds": [west, south, east, north] if count else None, "focus_bounds":focus, "types": types, "bikes": [b[0] for b in bikes], "first": first, "last": last, "first_launch":first_launch}


def insights(params):
    sql, args = filter_query(params)
    with db_connect() as con:
        query = "SELECT name,sport,day,COALESCE(recorded_distance_m,distance_m),moving_seconds,elapsed_seconds,elevation_m,recorded_distance_m,avg_heart_rate,avg_speed,max_speed,bike,moving_source,elevation_source,id FROM activities" + sql
        rows = con.execute(query, args).fetchall()
    count = len(rows)
    total_distance = sum(row[3] or 0 for row in rows)
    moving = sum(row[4] or 0 for row in rows)
    elapsed = sum(row[5] or 0 for row in rows)
    ascent = sum(row[6] or 0 for row in rows)
    official = sum(row[7] is not None for row in rows)
    moving_count = sum(row[4] is not None for row in rows)
    elevation_count = sum(row[6] is not None for row in rows)
    dates = []
    years, months, sports, weekday_distance = {}, {}, {}, [0.0]*7
    bikes = {}
    moving_estimates = sum(r[12] == "track-estimate" for r in rows)
    elevation_estimates = sum(r[13] == "track-estimate" for r in rows)
    for row in rows:
        name, sport, day, dist = row[:4]
        sport = sport or "Unknown"
        totals = sports.setdefault(sport, [0, 0])
        totals[0] += 1
        totals[1] += dist or 0
        if any(w in sport.casefold() for w in ("ride", "bik", "cycl")):
            key = (sport, row[11] or "Unidentified bike")
            bt = bikes.setdefault(key, [0, 0.0])
            bt[0] += 1
            bt[1] += dist or 0
        if day:
            try:
                parsed = dt.date.fromisoformat(day)
            except ValueError:
                continue
            dates.append(parsed)
            weekday_distance[parsed.weekday()] += dist or 0
            year = str(parsed.year)
            month = day[:7]
            y = years.setdefault(year, [0, 0])
            m = months.setdefault(month, [0, 0])
            y[0] += 1; y[1] += dist or 0
            m[0] += 1; m[1] += dist or 0
    sorted_days = sorted(set(dates))
    streak = best = 0
    earlier = None
    for current in sorted_days:
        streak = streak + 1 if earlier and (current-earlier).days == 1 else 1
        best = max(best, streak)
        earlier = current
    weekday_counts = [0]*7
    for day in dates:
        weekday_counts[day.weekday()] += 1
    longest = max(rows, key=lambda r:r[3] or 0, default=None)
    max_ascent = max((r for r in rows if r[6] is not None), key=lambda r:r[6], default=None)
    longest_moving = max((r for r in rows if r[4] is not None), key=lambda r:r[4], default=None)
    top_month = max(months.items(), key=lambda x:(x[1][0], x[0]), default=None)
    top_year = max(years.items(), key=lambda x:(x[1][0],x[0]), default=None)
    fastest = max((r for r in rows if (r[4] or 0) >= 600 and (r[3] or 0) >= 1000), key=lambda r:r[3] / r[4], default=None)
    peak_speed = max((r for r in rows if r[10] is not None and math.isfinite(r[10]) and 0 <= r[10] <= 200), key=lambda r:r[10], default=None)
    active_weeks = len({d.isocalendar()[:2] for d in dates})
    dist_per_week = total_distance / active_weeks if active_weeks else 0
    avg_speed = total_distance / moving * 3.6 if moving > 0 and moving_count == count else None
    top_distance_month = max(months.items(), key=lambda x:(x[1][1],x[0]), default=None)
    def ride_record(row):
        return {"id":row[14], "name":row[0], "sport":row[1], "day":row[2],
                "distance_m":row[3], "moving_seconds":row[4], "elapsed_seconds":row[5],
                "elevation_m":row[6], "bike":row[11]}
    top_routes = sorted(rows, key=lambda row:(row[3] or 0, row[14]), reverse=True)[:6]
    recent_routes = sorted(rows, key=lambda row:(row[2] or "", row[14]), reverse=True)[:10]
    return {
        "count":count,"distance_m":round(total_distance,1),"moving_seconds":round(moving),
        "elapsed_seconds":round(elapsed),"elevation_m":round(ascent,2),
        "strava_distance_count":official,"mapped_gps_count":count,
        "moving_count":moving_count,"elapsed_count":sum(r[5] is not None for r in rows),"elevation_count":elevation_count,
        "moving_estimated_count":moving_estimates,"elevation_estimated_count":elevation_estimates,
        "bikes":[{"sport":k[0],"name":k[1],"count":v[0],"distance_m":round(v[1],1)} for k,v in sorted(bikes.items(),key=lambda x:(x[0][0].casefold(),-x[1][1],x[0][1].casefold()))],
        "avg_distance_m":round(total_distance/count,1) if count else 0,
        "first":min((r[2] for r in rows if r[2]),default=None),"last":max((r[2] for r in rows if r[2]),default=None),
        "longest":ride_record(longest) if longest else None,
        "max_ascent":ride_record(max_ascent) if max_ascent else None,
        "longest_moving":ride_record(longest_moving) if longest_moving else None,
        "sports":[{"name":k,"count":v[0],"distance_m":round(v[1],1)} for k,v in sorted(sports.items(),key=lambda x:x[1][0],reverse=True)],
        "top_month":{"month":top_month[0],"count":top_month[1][0]} if top_month else None,
        "top_year":{"year":top_year[0],"count":top_year[1][0]} if top_year else None,
        "favorite_weekday":(["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"][max(range(7),key=lambda k:weekday_counts[k])] if any(weekday_counts) else None),
        "active_days":len(sorted_days),"active_weeks":active_weeks,"longest_streak":best,
        "average_weekly_distance_m":round(dist_per_week,1),"overall_avg_speed_kmh":round(avg_speed,2) if avg_speed is not None else None,
        "peak_speed_mps":peak_speed[10] if peak_speed else None,
        "peak_speed":dict(ride_record(peak_speed),speed_mps=peak_speed[10]) if peak_speed else None,
        "peak_speed_count":sum(r[10] is not None and math.isfinite(r[10]) and 0 <= r[10] <= 200 for r in rows),
        "top_routes":[ride_record(r) for r in top_routes],"recent_routes":[ride_record(r) for r in recent_routes],
        "fastest":dict(ride_record(fastest), speed_kmh=round(fastest[3] / fastest[4] * 3.6,1)) if fastest else None,
        "biggest_distance_month":{"month":top_distance_month[0],"distance_m":round(top_distance_month[1][1],1)} if top_distance_month else None,
        "weekdays":[{"name":name,"count":weekday_counts[i],"distance_m":round(weekday_distance[i],1)} for i,name in enumerate(("Mon","Tue","Wed","Thu","Fri","Sat","Sun"))],
        "years":[{"year":k,"count":v[0],"distance_m":round(v[1],1)} for k,v in sorted(years.items())],
        "months":[{"month":k,"count":v[0],"distance_m":round(v[1],1)} for k,v in sorted(months.items())],
    }


def filter_query(params, bounds=False):
    clauses = []
    values = []
    sport = params.get("sport", [""])[0].strip()[:100]
    if sport:
        clauses.append("sport=?")
        values.append(sport)
    bike = params.get("bike", [""])[0].strip()[:160]
    if bike:
        clauses.append("bike=?")
        values.append(bike)
    after = params.get("from", [""])[0]
    before = params.get("to", [""])[0]
    if after and parse_date(after) == after:
        clauses.append("day>=?")
        values.append(after)
    if before and parse_date(before) == before:
        clauses.append("day<=?")
        values.append(before)
    year = params.get("year", [""])[0].strip()
    if len(year) == 4 and year.isdecimal():
        clauses.append("substr(day,1,4)=?")
        values.append(year)
    month = params.get("month", [""])[0].strip()
    if len(month) == 7 and month[4] == "-" and month[:4].isdecimal() and month[5:].isdecimal() and 1 <= int(month[5:]) <= 12:
        clauses.append("substr(day,1,7)=?")
        values.append(month)
    keyword = params.get("q", [""])[0].strip()[:100]
    if keyword:
        clauses.append("name LIKE ? ESCAPE '\\'")
        values.append("%" + keyword.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
    activity_id = params.get("activity_id", [""])[0]
    if activity_id:
        if not activity_id.isascii() or not activity_id.isdecimal() or len(activity_id) > 12 or int(activity_id) <= 0:
            raise ValueError("Invalid activity selection")
        clauses.append("id=?")
        values.append(int(activity_id))
    if bounds:
        for name in ("west", "south", "east", "north"):
            try:
                float(params[name][0])
            except (KeyError, ValueError, IndexError):
                raise ValueError("Missing map bounds")
        west, south, east, north = [float(params[n][0]) for n in ("west", "south", "east", "north")]
        if not all(math.isfinite(v) for v in (west, south, east, north)):
            raise ValueError("Invalid map bounds")
        west, east = max(-180, west), min(180, east)
        south, north = max(-85.0511, south), min(85.0511, north)
        clauses.append("east>=? AND west<=? AND north>=? AND south<=?")
        values.extend((west, east, south, north))
    return (" WHERE " + " AND ".join(clauses) if clauses else ""), values


def iter_coordinates(blob):
    raw = zlib.decompress(blob)
    for lat, lon in struct.iter_unpack("<ii", raw):
        if lat == 2147483647:
            yield None
        else:
            yield lat / 1e6, lon / 1e6


def project_xy(lat, lon, zoom):
    scale = 256 * (2 ** zoom)
    x = (lon + 180.0) / 360.0 * scale
    a = math.radians(max(-85.0511, min(85.0511, lat)))
    y = (1 - math.asinh(math.tan(a)) / math.pi) / 2 * scale
    return x, y


def activity_preview(params):
    identity = params.get("id", [""])[0]
    if not identity.isascii() or not identity.isdecimal() or not 1 <= len(identity) <= 12 or int(identity) <= 0:
        raise ValueError("Invalid activity selection")
    with db_connect() as con:
        row = con.execute("""SELECT id,name,sport,day,COALESCE(recorded_distance_m,distance_m),
            moving_seconds,elapsed_seconds,elevation_m,bike,avg_heart_rate,avg_speed,max_speed,
            coordinates,point_count,west,south,east,north FROM activities WHERE id=?""", (int(identity),)).fetchone()
    if row is None:
        raise ValueError("Activity not found")
    step = max(1, math.ceil(max(1, row[13]) / 3800))
    points, i, last = [], 0, None
    for point in iter_coordinates(row[12]):
        if point is None:
            if last is not None and (not points or points[-1] != last):
                points.append(last)
            if points and points[-1] is not None:
                points.append(None)
            last, i = None, 0
            continue
        last = [round(point[0], 6), round(point[1], 6)]
        if i % step == 0:
            points.append(last)
        i += 1
    if last is not None and (not points or points[-1] != last):
        points.append(last)
    return {"id":row[0],"name":row[1],"sport":row[2],"day":row[3],
            "distance_m":row[4],"moving_seconds":row[5],"elapsed_seconds":row[6],
            "elevation_m":row[7],"bike":row[8],"avg_heart_rate":row[9],
            "avg_speed":row[10],"max_speed":row[11],
            "bounds":[row[14],row[15],row[16],row[17]],"points":points}



def cached_projected_track(activity_id, compressed, epoch):
    """Mercator coordinates at zoom zero; pan/zoom only multiplies these values.

    Existing databases remain unchanged. NaN pairs preserve track segment breaks.
    """
    global TRACK_CACHE_BYTES
    cache_key = (epoch, activity_id)
    with STATE_LOCK:
        track = TRACK_CACHE.get(cache_key)
        if track is not None:
            TRACK_CACHE.move_to_end(cache_key)
            return track
    track = array("d")
    append = track.append
    for point in iter_coordinates(compressed):
        if point is None:
            append(float("nan"))
            append(float("nan"))
        else:
            lat, lon = point
            a = math.radians(max(-85.0511, min(85.0511, lat)))
            append((lon + 180.0) * (256.0 / 360.0))
            append((1.0 - math.asinh(math.tan(a)) / math.pi) * 128.0)
    size = len(track) * 8
    if size <= MAX_TRACK_CACHE_BYTES:
        with STATE_LOCK:
            previous = TRACK_CACHE.get(cache_key)
            if previous is not None:
                TRACK_CACHE.move_to_end(cache_key)
                return previous
            while TRACK_CACHE and TRACK_CACHE_BYTES + size > MAX_TRACK_CACHE_BYTES:
                _, stale = TRACK_CACHE.popitem(last=False)
                TRACK_CACHE_BYTES -= len(stale) * 8
            TRACK_CACHE[cache_key] = track
            TRACK_CACHE_BYTES += size
    return track


def clear_render_caches(include_tracks=False):
    """Invalidate saved map views after data changes without touching user GPS."""
    global TRACK_CACHE_BYTES, VIEW_CACHE_BYTES
    with STATE_LOCK:
        CACHE_STATE["epoch"] += 1
        VIEW_CACHE.clear()
        VIEW_CACHE_BYTES = 0
        if include_tracks:
            TRACK_CACHE.clear()
            TRACK_CACHE_BYTES = 0


def _clip_segment(x0, y0, x1, y1, xmin, ymin, xmax, ymax):
    """Clip segments before grid walking, including both-outside crossings."""
    dx, dy = x1 - x0, y1 - y0
    start, end = 0.0, 1.0
    for p, q in ((-dx, x0 - xmin), (dx, xmax - x0), (-dy, y0 - ymin), (dy, ymax - y0)):
        if p == 0:
            if q < 0:
                return None
        else:
            t = q / p
            if p < 0:
                start = max(start, t)
            else:
                end = min(end, t)
            if start > end:
                return None
    return x0 + dx * start, y0 + dy * start, x0 + dx * end, y0 + dy * end


def create_view(params):
    global VIEW_CACHE_BYTES
    key=json.dumps(params,sort_keys=True,separators=(',',':'))
    with STATE_LOCK:
        if key in VIEW_CACHE:
            VIEW_CACHE.move_to_end(key)
            return VIEW_CACHE[key]
        epoch=CACHE_STATE['epoch']
    sql,args=filter_query(params,True)
    z=max(0,min(19,int(float(params.get('z',['4'])[0]))))
    scale=2**z
    west,south,east,north=[float(params[n][0]) for n in ('west','south','east','north')]
    x0,y0=project_xy(north,west,z)
    x1,y1=project_xy(south,east,z)
    # Extend calculation outside the actual crop so features remain seamless while panning.
    left,right=min(x0,x1)-48,max(x0,x1)+48
    top,bottom=min(y0,y1)-48,max(y0,y1)+48
    lo_x,hi_x=left/scale,right/scale
    lo_y,hi_y=top/scale,bottom/scale
    # Preserve fine-grained route geometry for smooth, anti-aliased paths.
    geom_step=1.1 if z>=13 else 1.35
    density_step=6.5 if z>=13 else 7.5
    gscale=scale/geom_step
    dscale=scale/density_step
    paths=set();counts={};selected=0
    with db_connect() as con:
        for activity_id,compressed in con.execute('SELECT id,coordinates FROM activities'+sql,args):
            selected+=1
            coords=cached_projected_track(activity_id,compressed,epoch)
            visited=set()
            previous=None
            for i in range(0,len(coords),2):
                x,y=coords[i],coords[i+1]
                if x!=x:
                    previous=None
                    continue
                if previous is not None:
                    px,py=previous
                    if not(max(px,x)<lo_x or min(px,x)>hi_x or max(py,y)<lo_y or min(py,y)>hi_y):
                        if lo_x<=px<=hi_x and lo_x<=x<=hi_x and lo_y<=py<=hi_y and lo_y<=y<=hi_y:
                            ax,ay,bx,by=px,py,x,y
                        else:
                            clipped=_clip_segment(px,py,x,y,lo_x,lo_y,hi_x,hi_y)
                            if clipped is None:
                                previous=(x,y);continue
                            ax,ay,bx,by=clipped
                        da=(round(ax*dscale),round(ay*dscale)); db=(round(bx*dscale),round(by*dscale))
                        visited.add(da);visited.add(db)
                        dsteps=max(abs(db[0]-da[0]),abs(db[1]-da[1]))
                        if dsteps>2:
                            dcx,dcy=db[0]-da[0],db[1]-da[1]
                            for t in range(2,dsteps,2):
                                visited.add((da[0]+round(dcx*t/dsteps),da[1]+round(dcy*t/dsteps)))
                        ga=(round(ax*gscale),round(ay*gscale)); gb=(round(bx*gscale),round(by*gscale))
                        if ga!=gb:
                            # Sparse GPS legs can cross different-density roads.
                            # Split those long strokes so one midpoint never colors
                            # the whole crossing incorrectly; short legs stay intact.
                            span=max(abs(gb[0]-ga[0]),abs(gb[1]-ga[1]))
                            pieces=max(1,math.ceil(span*geom_step/24.0))
                            if pieces==1:
                                a,b=(ga,gb) if ga<gb else (gb,ga)
                                paths.add((a[0],a[1],b[0],b[1]))
                            else:
                                ox,oy=ga
                                for k in range(1,pieces+1):
                                    ex=round(ga[0]+(gb[0]-ga[0])*k/pieces)
                                    ey=round(ga[1]+(gb[1]-ga[1])*k/pieces)
                                    if (ex,ey)!=(ox,oy):
                                        a,b=((ox,oy),(ex,ey)) if (ox,oy)<(ex,ey) else ((ex,ey),(ox,oy))
                                        paths.add((a[0],a[1],b[0],b[1]))
                                    ox,oy=ex,ey
                previous=(x,y)
            for c in visited:
                counts[c]=counts.get(c,0)+1
    # Use continuous bilinear interpolation of nearby unique-activity visit counts.
    # These densities color the original GPS polylines; coordinates are never
    # snapped to the density grid. A plateau across neighboring GPS noise keeps
    # heavily repeated roads bright with gradual color transitions.
    neighborhood={}
    def nearby_peak(cx,cy):
        key=(cx,cy)
        value=neighborhood.get(key)
        if value is None:
            value=max(counts.get((cx+dx,cy+dy),0)
                      for dx in (-1,0,1) for dy in (-1,0,1))
            neighborhood[key]=value
        return value
    def smooth_visits(gx,gy):
        cx,cy=math.floor(gx),math.floor(gy)
        tx,ty=gx-cx,gy-cy
        a=nearby_peak(cx,cy)*(1-tx)+nearby_peak(cx+1,cy)*tx
        b=nearby_peak(cx,cy+1)*(1-tx)+nearby_peak(cx+1,cy+1)*tx
        return round(a*(1-ty)+b*ty,4)
    levels=[]
    factor=geom_step/density_step
    for ax,ay,bx,by in sorted(paths):
        start_visit=smooth_visits(ax*factor,ay*factor)
        end_visit=smooth_visits(bx*factor,by*factor)
        levels.append([ax,ay,bx,by,start_visit,end_visit])
    peak=max(counts.values(),default=0)
    data={'step':geom_step,'z':z,'segments':levels,'activities':selected,'peak':peak}
    encoded=json.dumps(data,separators=(',',':')).encode()
    with STATE_LOCK:
        if CACHE_STATE['epoch']==epoch and len(encoded)<=MAX_VIEW_CACHE_BYTES:
            while VIEW_CACHE and (len(VIEW_CACHE)>=12 or VIEW_CACHE_BYTES +len(encoded)>MAX_VIEW_CACHE_BYTES):
                _,stale=VIEW_CACHE.popitem(last=False);VIEW_CACHE_BYTES-=len(stale)
            VIEW_CACHE[key]=encoded;VIEW_CACHE_BYTES+=len(encoded)
    return encoded

def export_gpx(handler, params):
    sql, args = filter_query(params)
    handler.send_response(200)
    handler.send_header("Content-Type", "application/gpx+xml; charset=utf-8")
    handler.send_header("Content-Disposition", 'attachment; filename="LibreHeatmaps_routes.gpx"')
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(b'<?xml version="1.0" encoding="UTF-8"?><gpx version="1.1" creator="LibreHeatmaps" xmlns="http://www.topografix.com/GPX/1/1">\n')
    with db_connect() as con:
        for name, coords in con.execute("SELECT name,coordinates FROM activities" + sql + " ORDER BY id", args):
            handler.wfile.write(("<trk><name>" + html.escape(name) + "</name><trkseg>\n").encode("utf-8"))
            for point in iter_coordinates(coords):
                if point is None:
                    handler.wfile.write(b"</trkseg><trkseg>\n")
                else:
                    handler.wfile.write(("<trkpt lat=\"%.6f\" lon=\"%.6f\"/>\n" % point).encode("ascii"))
            handler.wfile.write(b"</trkseg></trk>\n")
    handler.wfile.write(b"</gpx>\n")


PAGE = '''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>LibreHeatmaps</title>
<style>
*{box-sizing:border-box}body{margin:0;overflow:hidden}button,input,select{font:inherit}button{cursor:pointer}select,input[type=date],input[type=search]{padding:9px;border:1px solid #46345f;color:#f4efff;border-radius:9px;min-width:0}input[type=range]{width:100%;accent-color:#b993ff}label{display:flex;align-items:center;gap:8px}small,.muted{color:#9cadc2}h1{margin:0}h2{margin:0 0 10px}header{border-bottom:1px solid #38264f;display:flex;align-items:center;justify-content:space-between}header .brand{display:flex;align-items:center}.workspace{display:flex}aside{flex:none;overflow-y:auto;border-right:1px solid #38264f}section{border-bottom:1px solid #39284f}.field{display:flex;flex-direction:column;gap:7px;margin-top:12px}.field label{justify-content:space-between;font-weight:600;font-size:12px;color:#d7caeb}.field .range{display:flex;gap:12px;align-items:center}.field output{font-variant-numeric:tabular-nums;color:#b993ff;min-width:40px;text-align:right}.row{display:flex;gap:8px}.row>*{flex:1;min-width:0}.primary{font-weight:700}.subtle{background:transparent}.full{width:100%;margin-top:12px}.status{font-size:12px;color:#a8b9cb;line-height:1.5;margin-top:9px;white-space:pre-wrap;overflow-wrap:anywhere}.status.error{color:#ff9d9d}.stats{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin-top:10px}.stat{background:#271a3d;border:1px solid #423052;padding:12px;border-radius:10px}.stat strong{display:block;font-size:20px;color:#fafafa;margin-bottom:4px}.stat span{color:#b1a2c7;font-size:11px}.toggle{justify-content:space-between;padding:8px 0;color:#d6e1ee}.toggle input{accent-color:#b993ff}.foot{line-height:1.5;font-size:11px;color:#a793bd}main{flex:1;min-width:0;position:relative;overflow:hidden;background:#110d1d}#tiles{position:absolute;inset:0;overflow:hidden}#tiles img{position:absolute;left:0;top:0;width:256px;height:256px;user-select:none;pointer-events:none}#overlay{position:absolute;inset:0;width:100%;height:100%;pointer-events:none}#interaction{position:absolute;inset:0;cursor:grab;touch-action:none}#interaction:active{cursor:grabbing}.maptop{position:absolute;top:17px;left:18px;pointer-events:none;background:#160e27de;color:white}.maptop span{display:block}.tools{position:absolute;display:flex;flex-direction:column}.tools button{font-size:19px}.attribution{position:absolute;bottom:10px;right:12px;padding:5px 8px;border-radius:5px;font-size:11px;background:#180f28e0;color:#e0e8ef}.attribution a{color:#b993ff}.legend{position:absolute;bottom:16px;left:16px;background:#160e27d9;border-radius:11px;border:1px solid #45305b}.legend .bar{border-radius:9px;margin:8px 0}.empty{position:absolute;inset:0;display:flex;justify-content:center;align-items:center;pointer-events:none}.empty .panel{background:#201433ea;color:#ede3ff;border:1px solid #4b3567;border-radius:16px;padding:28px 30px;text-align:center;max-width:460px;box-shadow:0 15px 40px #0005}.empty strong{font-size:20px;display:block;margin-bottom:9px}.empty span{line-height:1.6;color:#c3b0dc}.hidden{display:none!important}#file{display:none}#busy{position:absolute;bottom:20px;left:50%;transform:translateX(-50%);background:#251736;border:1px solid #5b4178;color:#eee;border-radius:9px;padding:11px 15px;z-index:5}#dateRange{font-size:11px;color:#a995c1;margin-top:4px}@media(max-width:750px){.workspace{min-height:calc(100vh - 64px)}aside{border-right:0;border-bottom:1px solid #293749}}

header .header-actions{display:flex;align-items:center}.private-pill{border:1px solid #67498e;border-radius:20px;padding:6px 10px;font-size:11px}#shutdown{font-size:12px;padding:7px 10px;background:#20132f}#tileNotice{font-size:11px;line-height:1.45;padding:7px 2px 8px}.privacy-notice{color:#bea8d7}#tiles{pointer-events:none}.logo{filter:drop-shadow(0 0 10px #9c67ff55)}dialog{border:1px solid #604480;border-radius:18px;max-width:min(790px,94vw);max-height:88vh;width:790px;overflow-y:auto;padding:23px;box-shadow:0 20px 80px #080312aa}dialog::backdrop{backdrop-filter:blur(4px)}.dialog-head{display:flex;align-items:start;justify-content:space-between;gap:16px;margin-bottom:18px}.dialog-head h2{letter-spacing:normal;text-transform:none;margin-bottom:4px}.dialog-filters{display:flex;align-items:center;margin:0 0 18px}.dialog-filters>*{flex:unset}.metric-grid{display:grid;gap:9px}.metric{background:#291b3e;border:1px solid #48315f;border-radius:12px;padding:16px 12px}.metric strong{display:block;font-size:21px;color:#ead9ff;letter-spacing:-.5px}.metric span{display:block;font-size:11px;color:#b7a5ce;margin-top:6px}.insight-grid{display:grid;grid-template-columns:1fr 1fr;gap:15px;margin:19px 0}.insight-grid article{padding:16px;border:1px solid #463057;border-radius:13px}.facts{display:grid;gap:12px}.fact{display:flex;flex-direction:column;gap:3px;overflow-wrap:anywhere}.fact strong{font-size:13px;color:#f2e8ff}.fact span{font-size:11px;color:#b8a5cb}.sport-summary{display:grid;gap:10px}.sport-row{display:flex;justify-content:space-between;gap:12px;font-size:12px}.sport-row span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.sport-row strong{color:#c9a8ff;font-variant-numeric:tabular-nums}#statAccuracy{font-size:11px;line-height:1.6}button:focus-visible{outline:2px solid #b993ff}@media(max-width:680px){.metric-grid{grid-template-columns:repeat(2,1fr)}.insight-grid{grid-template-columns:1fr}header .brand small{display:none}header{padding:0 10px}h1{font-size:16px}}header{border-color:#302a43}aside{border-color:#353041}section{border-color:#342e41}button{background:#2a2436;border-color:#4c425b;color:#f4edf8}button:hover{background:#373049}.primary{background:#baa6d1;border-color:#baa6d1;color:#201a2b}.primary:hover{background:#cab6dc}select,input[type=date],input[type=search]{background:#242031;border-color:#494051}input[type=range],.toggle input{accent-color:#ab91ca}.stat,.metric{background:#242033;border-color:#3e364b}.stat strong,.metric strong{color:#f7f2fc}.field output,.attribution a{color:#c6addf}.private-pill{color:#d1bde5;border-color:#5e4c71}.maptop,.legend{background:#171523ea;border-color:#4b3c60}.legend .bar{background:linear-gradient(90deg,#a470dc 0%,#e044bb 25%,#f44362 50%,#ff941f 75%,#fff3de 100%)}#tiles.dark img{filter:invert(.90) hue-rotate(180deg) brightness(.72) saturate(.55)}dialog{background:#1d192b;border-color:#59496a}dialog::backdrop{background:#080710c9}.insight-grid article{background:#242033;border-color:#3b3449}.dialog-filters{flex-wrap:wrap;justify-content:flex-start;gap:12px}.dialog-filters label{display:flex;flex-direction:column;align-items:flex-start;font-size:11px;color:#c9bdd5}.dialog-filters select{min-width:130px}.charts{margin-top:0}.stat-chart{display:grid;gap:10px;max-height:235px;overflow-y:auto}.chart-row{display:grid;grid-template-columns:67px 1fr 78px;gap:10px;align-items:center;font-size:11px}.chart-row>span{color:#c0b1d5;white-space:nowrap}.chart-row>strong{font-size:11px;color:#e3d2f6;text-align:right;white-space:nowrap}.chart-track{height:9px;border-radius:10px;background:#42394e;overflow:hidden}.chart-bar{height:100%;border-radius:10px;background:linear-gradient(90deg,#9675c5,#e285a7)}#tileNotice{color:#c6b8d4}#map.offline-dark{background:#11121b}#map.offline-light{background:#e7e5eb}
@media(max-width:680px){.dialog-filters select{min-width:110px}.chart-row{grid-template-columns:58px 1fr 64px}}
header .logo{clip-path:none;object-fit:contain;background:none;flex:none;filter:none} .stats-cta,.stats-header{background:#8b72b3;color:#fff;border-color:#9a84b8;font-weight:700}.stats-cta:hover,.stats-header:hover{background:#9d87c4}.stats-header{font-size:12px;padding:8px 12px}button.primary:not(.stats-cta):not(.stats-header){background:#b5a3cd}section{padding-bottom:15px;margin-bottom:15px} .metric-grid{grid-template-columns:repeat(4,minmax(0,1fr))}.weekday-panel{background:#242033;border:1px solid #40364b;border-radius:13px;padding:15px;margin:12px 0}.weekday-chart{display:grid;gap:8px;margin-top:10px}.weekday-row{display:grid;grid-template-columns:55px 1fr 62px;gap:9px;align-items:center;font-size:11px}.weekday-row .chart-track{height:11px}#legendLabel{color:#c7b9d1}select#palette{width:100%}@media(max-width:750px){}
 .settings-stack{display:grid;gap:5px}.settings-section{margin-bottom:5px;padding-bottom:17px}.settings-section h2{font-size:13px;letter-spacing:.07em}.settings-note{font-size:12px;line-height:1.55;margin:12px 0}.settings-actions{display:flex;align-items:center;justify-content:space-between;gap:20px;padding:14px 0;border-top:1px solid #3e354b}.settings-actions strong{display:block;font-size:13px;color:#f0e7fc}.settings-actions small{display:block;max-width:460px;line-height:1.5;margin-top:5px;color:#b7a9c4}.settings-actions button{flex:none}.settings-status{min-height:18px;font-size:12px;color:#ccb6e7;white-space:pre-wrap}.settings-status.error{color:#ffa5a5}button.danger{border-color:#994d61;color:#ffc5cf;background:#3c202d}button.danger:hover{background:#532735}#openSettings{padding:8px 12px;font-size:12px}#settingsDialog .field select{max-width:100%;width:100%}@media(max-width:670px){.settings-actions{align-items:stretch;flex-direction:column}.settings-actions button{align-self:flex-start}}
.bike-row{padding-left:16px;margin-left:9px;border-left:2px solid #655078;font-size:12px;color:#cdbed9}.bike-row strong{font-weight:600}.local-storage{padding:14px 0}.local-storage>strong{display:block;font-size:13px;margin-bottom:8px}.path-label{font-size:11px;color:#b8a9cb;margin-top:10px}.local-storage code{display:block;word-break:break-all;font-size:12px;line-height:1.5;padding:9px 10px;background:#171421;border:1px solid #42374f;border-radius:7px;color:#e2d4f0}#releaseLink{display:inline-block;margin-top:8px;color:#cdb4f0}#updateStatus{margin-top:8px}#settingsDialog .settings-stack{display:block}
.palette-preview{height:22px;margin-top:10px;border-radius:6px;border:1px solid #665474;background:linear-gradient(90deg,#a16cdf,#df398f,#f34547,#ff8d23,#fff3dd)}.palette-ends{display:flex;justify-content:space-between;font-size:11px;color:#b9a9cb;margin-top:5px}#releaseLink[hidden]{display:none!important}#settingsDialog{padding:0;overflow:auto;scrollbar-gutter:stable}#settingsDialog>.dialog-head{position:sticky;top:0;z-index:4;align-items:center;margin:0;border-bottom:1px solid #4e3e61;box-shadow:0 3px 10px #09061255}#settingsDialog .settings-close{display:grid;place-items:center;flex:none;border-radius:10px;line-height:1;color:#f5edf8;background:#33263f}#settingsDialog .settings-close:hover{background:#51395b}#settingsDialog .settings-savebar{position:sticky;bottom:0;z-index:4;display:flex;justify-content:space-between;align-items:center;border-top:1px solid #4e3e61;box-shadow:0 -3px 12px #09061255}.settings-save-state{font-size:12px;color:#bcb2c8}.settings-save-state.dirty{color:#ffcb79}.settings-save-state.error{color:#ff9eaa}#saveSettings{min-width:142px;font-weight:750}#saveSettings:disabled{opacity:.55;cursor:default}@media(max-width:560px){#settingsDialog>.dialog-head,#settingsDialog>.settings-stack,#settingsDialog .settings-savebar{padding-left:14px;padding-right:14px}}
aside .status,aside .foot,#dateRange{font-size:13px}
#statsDialog h2{font-size:14px}
#statsDialog .metric span,#statsDialog .fact span,#statsDialog #statAccuracy{font-size:13px;line-height:1.55}
#statsDialog .sport-row,#statsDialog .bike-row{white-space:normal}
#statsDialog .sport-row span{white-space:normal;overflow:visible;text-overflow:clip}
#statsDialog .stat-chart{min-width:0}
#statsDialog .chart-row>span{white-space:nowrap;font-size:13px}
#statsDialog .chart-row>strong{white-space:nowrap;font-size:13px}
@media(max-width:820px){#statsDialog{padding:18px 16px}#statsDialog .metric-grid{grid-template-columns:repeat(2,minmax(0,1fr))}#statsDialog .chart-row{grid-template-columns:85px minmax(50px,1fr) 73px}}
@media(max-width:470px){#statsDialog .metric-grid{grid-template-columns:repeat(2,minmax(0,1fr))}#statsDialog .metric strong{font-size:20px}}

:root{font-size:16px;line-height:1.5;font-weight:400;color:#efeaf5;background:#10101c;--lh-text:#efeaf5;--lh-muted:#c2b8d0;--lh-border:#494053;--lh-surface:#242033;--lh-accent:#bca5de}
body{font-size:16px}
button,input,select{font-family:inherit;font-size:15px;line-height:1.35}
button{padding:10px 14px;border:1px solid #554a64;border-radius:10px;font-weight:600;letter-spacing:0}
button:hover:not(:disabled){border-color:#88739e}
button:disabled{cursor:not-allowed;opacity:.56}
input[type=date],input[type=search],select{min-height:42px;padding:9px 11px;font-size:15px;border-radius:10px}
button:focus-visible,input:focus-visible,select:focus-visible{outline:3px solid #c6a1ec;outline-offset:2px}
h1{font-size:23px;font-weight:700;line-height:1.15;letter-spacing:-.45px}
h2{font-size:16px;line-height:1.35;font-weight:650;letter-spacing:0;text-transform:none;color:#f2ecfa}
small,.muted,.foot{font-size:14px;line-height:1.55;color:var(--lh-muted)}
header{min-height:76px;height:76px;padding:0 22px;gap:16px}
header .brand{gap:14px}
header .logo{width:51px;height:51px}
header .brand small{display:block;margin-top:4px}
header .header-actions{flex:0 0 auto;flex-wrap:wrap;justify-content:flex-end}
header .header-actions button,header .header-actions #openSettings,header .header-actions #shutdown,header .header-actions .stats-header{font-size:15px;line-height:1.2;min-height:42px;padding:10px 15px;font-weight:600;border-radius:10px}
header .header-actions #shutdown{background:#242033;border-color:#554a64;color:#ddd3e9}
header .header-actions .stats-header{background:#bca5de;border-color:#c8b4e6;color:#21182e}
header .header-actions .stats-header:hover{background:#ccb7ea}
.workspace{height:calc(100vh - 76px)}
aside{width:324px;padding:20px 18px 30px}
aside section{padding-bottom:22px;margin-bottom:22px}
aside h2{margin-bottom:10px}
aside .field label{font-size:14px;font-weight:600;color:#e2d7ef}
aside .field{gap:8px;margin-top:15px}
aside .status,aside .foot,aside #dateRange,#dateRange,aside .muted,#tileNotice{font-size:14px;line-height:1.55}
aside .stats{gap:10px}
aside .stat{padding:13px 14px}
aside .stat strong{font-size:23px;line-height:1.2;font-weight:700}
aside .stat span{line-height:1.4}
aside .toggle{gap:12px}
aside .toggle input{width:18px;height:18px;flex-shrink:0}
aside .row{gap:9px}
aside .row button{padding:10px 7px;min-width:0}
.maptop{padding:13px 16px;font-size:15px;font-weight:650;line-height:1.3;border-radius:12px;max-width:min(500px,calc(100% - 100px))}
.maptop span{line-height:1.4;color:#d3c9df;font-weight:400;margin-top:5px}
.tools{top:18px;right:18px;z-index:2}
.tools button{min-width:46px;padding:0;display:flex;align-items:center;justify-content:center;line-height:0}
.tools button svg{display:block;width:22px;height:22px;flex:none;stroke:currentColor;stroke-width:2.4;fill:none;stroke-linecap:round;stroke-linejoin:round}
.legend{line-height:1.45;padding:12px 14px}
.legend .bar{width:174px;height:10px}
#legendLabel,.attribution{font-size:13px;line-height:1.4}
#busy{font-size:14px}
.empty .panel strong{font-size:22px}
.empty .panel span{font-size:15px}
dialog{font-size:15px;color:var(--lh-text);line-height:1.55}
dialog h2{font-size:17px;letter-spacing:0;text-transform:none}
.dialog-head h2{font-size:23px;font-weight:700;line-height:1.2}
.dialog-head .muted{font-size:14px}
#statsDialog .metric-grid{gap:12px}
#statsDialog .metric{padding:17px 15px;border-radius:12px}
#statsDialog .metric strong{font-size:25px;line-height:1.15;letter-spacing:-.35px;overflow-wrap:anywhere}
#statsDialog .metric span{font-size:14px;font-weight:500;line-height:1.4;margin-top:9px;color:#d0c4dc}
#statsDialog .sport-row,#statsDialog .bike-row{font-size:15px;line-height:1.45;gap:14px}
#statsDialog .sport-row strong{min-width:0;text-align:right}
#statsDialog .bike-row{padding-left:15px}
#statsDialog .chart-row{min-width:0}
#statsDialog .chart-row>strong{font-weight:650}
#statsDialog .weekday-row{gap:12px}
#statsDialog .weekday-row strong{font-size:14px;font-weight:650}
#statsDialog #statAccuracy{border-radius:11px;margin-top:14px}
#statsDialog .details-repair{margin-top:17px}
#statsDialog .details-repair strong{display:block}
#statsDialog .details-repair button{white-space:nowrap}
#settingsDialog{width:min(730px,95vw);max-height:calc(100vh - 40px)}
#settingsDialog>.dialog-head{padding:18px 22px}
#settingsDialog>.dialog-head h2{font-size:23px}
#settingsDialog .settings-close{width:42px;height:42px;min-width:42px;min-height:42px;padding:0;font-size:28px;font-weight:400}
#settingsDialog>.settings-stack{padding:22px 24px 15px}
#settingsDialog .settings-section{padding-bottom:23px;margin-bottom:21px}
#settingsDialog .settings-section h2{letter-spacing:0}
#settingsDialog .field label{font-size:15px;font-weight:600;line-height:1.4}
#settingsDialog .settings-note,#settingsDialog .muted{font-size:14px;line-height:1.6}
#settingsDialog .settings-actions{gap:20px;padding:17px 0}
#settingsDialog .settings-actions strong,#settingsDialog .local-storage>strong{font-size:15px;font-weight:650}
#settingsDialog .settings-actions small{font-size:14px;line-height:1.55;margin-top:6px;color:#c1b6d0}
#settingsDialog .settings-status,#settingsDialog .settings-save-state{font-size:14px;line-height:1.5}
#settingsDialog .path-label,#settingsDialog .palette-ends{font-size:14px}
#settingsDialog .local-storage code{line-height:1.55}
#settingsDialog .settings-savebar{padding:15px 22px;gap:15px}
#settingsDialog #saveSettings{min-width:155px}
#releaseLink{font-size:14px}
@media(max-width:950px){header{padding:0 15px}header .brand small{font-size:13px}aside{width:300px}#statsDialog .metric-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:750px){body{overflow:auto}header{height:auto;min-height:74px;padding:12px 14px;flex-wrap:wrap}header .header-actions{width:100%;justify-content:flex-start}header .header-actions button,header .header-actions #openSettings,header .header-actions #shutdown,header .header-actions .stats-header{min-height:40px;padding:9px 13px;font-size:14px}header .brand small{display:block}.workspace{height:auto;display:flex;flex-direction:column}aside{width:100%;max-height:none;overflow:visible;padding:17px 16px}main{height:62vh;min-height:390px}#statsDialog{padding:20px 18px}}
@media(max-width:540px){#statsDialog{padding:16px 14px}#statsDialog .metric{padding:15px 12px}#statsDialog .metric strong{font-size:22px}#statsDialog .chart-row{grid-template-columns:68px minmax(25px,1fr) 70px;gap:8px}#statsDialog .details-repair{align-items:stretch;flex-direction:column}#statsDialog .details-repair button{align-self:flex-start}#settingsDialog>.dialog-head,#settingsDialog>.settings-stack,#settingsDialog .settings-savebar{padding-left:16px;padding-right:16px}.settings-actions{flex-direction:column;align-items:stretch}.settings-actions button{align-self:flex-start}.maptop{left:10px;top:12px;max-width:calc(100% - 82px);padding:10px 11px;font-size:14px}.tools{right:10px;top:12px}.tools button{width:44px;height:44px;min-width:44px;min-height:44px}}

:root{font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif}
aside .field label,aside .field output,aside .status,aside .foot,aside #dateRange,aside .muted,#tileNotice{font-size:15px;line-height:1.5}
aside .field output{font-weight:600;min-width:54px}
aside .stat span{font-size:15px;font-weight:400}
aside .toggle{font-size:15px;font-weight:500}
.maptop span{font-size:15px}
.legend,#legendLabel,.attribution{font-size:14px}
#statsDialog .metric span,#statsDialog .chart-row>span,#statsDialog .chart-row>strong,#statsDialog .fact span,#statsDialog .weekday-row,#statsDialog .weekday-row strong{font-size:15px}
#statsDialog .details-repair span,#statsDialog #statAccuracy{font-size:15px}
#statsDialog .metric strong{font-weight:700}
#statsDialog .fact strong,#statsDialog .sport-row strong,#statsDialog .chart-row strong,#statsDialog .weekday-row strong{font-weight:600}
dialog h2{font-weight:600}
#settingsDialog .settings-note,#settingsDialog .muted,#settingsDialog .settings-actions small,#settingsDialog .settings-save-state,#settingsDialog .settings-status,#settingsDialog .path-label,#settingsDialog .palette-ends{font-size:15px}
#settingsDialog .settings-section h2,#settingsDialog .settings-actions strong{font-weight:600}
#settingsDialog .local-storage code{font-size:15px}
#settingsDialog #releaseLink{font-size:15px}
.tools button:focus-visible{outline-offset:3px}
@media(max-width:750px){header .header-actions button{font-size:15px}header .brand small{font-size:14px}}
@media(max-width:540px){.maptop span{font-size:14px}#statsDialog .chart-row>span,#statsDialog .chart-row>strong{font-size:14px}}



[hidden]{display:none!important}
#mapCaption{z-index:3}
#mapCaption #clearRide{pointer-events:auto}
button{vertical-align:middle}
header .header-actions button,aside button,dialog button,#map button{display:inline-flex;align-items:center;justify-content:center;text-align:center}
button svg{flex-shrink:0;vertical-align:middle}
#mapCaption .map-caption-title{display:block;font-size:15px;font-weight:650;color:#eee7f9;line-height:1.3}
#mapCaption #mapDetail{font-weight:400}
#mapCaption #clearRide{margin-top:12px;min-height:36px;font-size:13px;font-weight:650;padding:7px 10px;border:1px solid #806e9d;color:#f5ebff;background:#302443;border-radius:9px}
#statsDialog .fact-link{cursor:pointer}
#statsDialog .ride-list-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;margin:19px 0}
#statsDialog .ride-list-panel{padding:20px;border-radius:14px;background:#242033;border:1px solid #3b3449;min-width:0}
#statsDialog .ride-list-panel h2{font-size:17px;font-weight:650;margin:0 0 6px}
#statsDialog .ride-list-panel .muted{font-size:14px;line-height:1.45}
#statsDialog .ride-list{margin-top:15px}
#statsDialog .ride-list-item{width:100%;justify-content:center;overflow-wrap:anywhere}
#statsDialog .ride-list-item strong{color:#f3eafb}
#statsDialog .ride-list-item span{font-weight:400}
#ridePreviewDialog{width:min(760px,96vw);max-width:min(760px,96vw);max-height:calc(100vh - 40px);padding:24px;overflow-y:auto}
#ridePreviewDialog .dialog-head{display:flex;align-items:center;gap:16px;margin-bottom:15px}
#ridePreviewDialog .dialog-head h2{letter-spacing:0;text-transform:none;overflow-wrap:anywhere}
#ridePreviewDialog .dialog-head button{flex:none}
#ridePreviewDialog .ride-preview-map{height:320px;max-height:42vh;background:#151522;border-radius:13px;border:1px solid #514466;position:relative;overflow:hidden}
#ridePreviewCanvas{width:100%;height:100%;display:block}
#ridePreviewDialog .ride-preview-badge{position:absolute;top:13px;left:13px;font-weight:600;line-height:1.3;background:#272037dc;border:1px solid #574766;border-radius:8px;padding:7px 10px;pointer-events:none}
#ridePreviewDialog .ride-preview-details{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));margin:14px 0}
#ridePreviewDialog .ride-preview-detail{border:1px solid #493a5b;min-width:0;overflow-wrap:anywhere}
#ridePreviewDialog .ride-preview-detail span{display:block;line-height:1.4;margin-bottom:6px}
#ridePreviewDialog .ride-preview-detail strong{display:block;line-height:1.3;color:#f3ecfa}
#ridePreviewDialog .ride-preview-actions{display:flex;flex-wrap:wrap;gap:10px;justify-content:flex-end;margin:16px 0 8px}
#ridePreviewDialog #ridePreviewStatus{line-height:1.45}
@media(max-width:700px){#statsDialog .ride-list-grid{grid-template-columns:1fr}#ridePreviewDialog{padding:16px}#ridePreviewDialog .dialog-head{flex-wrap:wrap}#ridePreviewDialog .ride-preview-map{height:245px}#ridePreviewDialog .ride-preview-details{grid-template-columns:repeat(2,minmax(0,1fr))}#ridePreviewDialog .ride-preview-actions{justify-content:stretch}#ridePreviewDialog .ride-preview-actions button{flex:1}}
#statsDialog #otherMetrics .metric span,#statsDialog .fact span,#statsDialog .stats-sport-overview p,#statsDialog .stats-panel p.muted{font-size:15px}
#statsDialog .stats-tabs{grid-template-columns:repeat(4,minmax(0,1fr))}
#statsDialog .stats-tabs button{min-width:0;font-size:15px}
#statsDialog .stats-tabs button[aria-selected="true"]{color:#fff}
#statsDialog .stats-tabs button:hover:not([aria-selected="true"]){background:#30283d;color:#fff}
#statsDialog .stats-panel{min-width:0}
#statsDialog .stats-section-label{font-size:17px;font-weight:650;color:#ece5f5;margin:0 0 12px;letter-spacing:0;text-transform:none}
#statsDialog .stats-section-label.secondary-label{margin:22px 0 12px}
#statsDialog #metricGrid .metric span{margin-top:8px}
#statsDialog .stats-sport-overview{margin:20px 0}
#statsDialog .stats-sport-overview article{min-width:0}
#statsDialog .stats-sport-overview p,.stats-panel p.muted{font-size:14px;line-height:1.5;color:#c9bed7;margin:0 0 14px}
#statsDialog .stats-note-card{display:flex;flex-direction:column;align-items:start;gap:12px;justify-content:center}
#statsDialog .stats-note-card p{font-size:15px;line-height:1.65;margin:0;color:#d3c9dd}
#statsDialog .stats-note-card button{margin-top:5px}
#statsDialog .records-layout{margin:0 0 18px;align-items:start}
#statsDialog .records-layout article{min-width:0}
#statsDialog .fact{min-height:0}
#statsDialog .fact strong{white-space:normal}
#statsDialog .fact-link{width:100%}
#statsDialog .stats-panel .charts{margin:0 0 18px}
#statsDialog .recent-panel{padding:20px;max-width:100%;border-radius:14px;background:#242033;border:1px solid #3b3449}
#statsDialog .recent-panel .ride-list{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
#statsDialog .stats-footer{padding-top:10px}
#statsDialog .stats-accuracy summary{color:#d1c5df}
#statsDialog #statAccuracy{background:transparent;border:0;padding:10px 0 0;margin:0}
#ridePreviewDialog .ride-preview-map #ridePreviewTiles{position:absolute;inset:0;overflow:hidden;z-index:0;background:#161621}
#ridePreviewDialog .ride-preview-map #ridePreviewTiles img{position:absolute;left:0;top:0;width:256px;height:256px;pointer-events:none;user-select:none;filter:invert(.90) hue-rotate(180deg) brightness(.72) saturate(.55)}
#ridePreviewDialog #ridePreviewCanvas{position:absolute;inset:0;z-index:1;pointer-events:none}
#ridePreviewDialog .ride-preview-badge{z-index:2}
#ridePreviewDialog .ride-preview-attribution{position:absolute;bottom:7px;right:9px;z-index:2;padding:4px 7px;background:#15131fdc;border-radius:5px;color:#e5dbf0;font-size:11px;line-height:1.2;text-decoration:none}
#ridePreviewDialog .ride-preview-attribution[hidden]{display:none}
#welcomeDialog{width:min(530px,94vw);max-width:94vw;max-height:calc(100vh - 30px);padding:29px;border-radius:18px;line-height:1.6}
#welcomeDialog .welcome-icon{width:49px;height:49px;border:1px solid #8366a3;background:#433257;border-radius:13px;display:grid;place-items:center;font-size:29px;margin-bottom:17px}
#welcomeDialog h2{font-size:25px;line-height:1.2;font-weight:700;letter-spacing:-.3px;text-transform:none;margin:0 0 10px}
#welcomeDialog p{font-size:16px;line-height:1.6;color:#d7cbdf;margin:0 0 17px}
#welcomeDialog .welcome-steps{padding:15px 16px;border:1px solid #534462;border-radius:11px;background:#262034;display:grid;gap:6px;margin:13px 0 15px}
#welcomeDialog .welcome-steps strong{font-size:15px;color:#f3eaff}
#welcomeDialog .welcome-steps span{font-size:15px;line-height:1.55;color:#d4c8de}
#welcomeDialog .welcome-privacy{color:#b6aac4}
#welcomeDialog .welcome-actions{display:flex;gap:12px;justify-content:flex-end;flex-wrap:wrap}
#welcomeDialog .welcome-actions button{min-height:44px;font-size:15px;padding:10px 16px}
@media(max-width:760px){#statsDialog .stats-tabs{top:88px}#statsDialog #metricGrid,#statsDialog #otherMetrics{grid-template-columns:repeat(2,minmax(0,1fr))}#statsDialog .recent-panel .ride-list{grid-template-columns:1fr}}
@media(max-width:520px){#statsDialog .stats-tabs{gap:3px;top:88px;padding:4px}#statsDialog .stats-tabs button{min-height:43px;padding:8px 3px;font-size:13px}#statsDialog #metricGrid,#statsDialog #otherMetrics{grid-template-columns:repeat(2,minmax(0,1fr))}#statsDialog #metricGrid .metric{padding:15px 12px;min-height:95px}#statsDialog #metricGrid .metric strong{font-size:24px}#statsDialog #metricGrid .metric span{font-size:14px}#statsDialog .stats-note-card{padding:16px}#welcomeDialog{padding:22px}#welcomeDialog .welcome-actions{flex-direction:column-reverse}#welcomeDialog .welcome-actions button{width:100%}}

#statsDialog{width:min(1320px,calc(100vw - 36px));max-width:1320px;height:min(870px,calc(100dvh - 34px));max-height:calc(100dvh - 34px);padding:0;overflow:hidden;border:1px solid #51445f;border-radius:18px;background:#181521;color:#f3edf8;scrollbar-gutter:auto}
#statsDialog:not([open]){display:none!important}
#statsDialog[open]{display:flex;flex-direction:column}
#statsDialog .insights-shell{display:flex;flex-direction:column;flex:1;min-height:0;min-width:0}
#statsDialog .insights-topbar{display:flex;align-items:center;justify-content:space-between;gap:14px;flex:none;padding:19px 27px;border-bottom:1px solid #463b51}
#statsDialog .insights-heading{display:flex;align-items:center;gap:15px;min-width:0}
#statsDialog .insights-heading .insights-mark{width:47px;height:47px;flex:none;display:grid;place-items:center;border:1px solid #725b8d;border-radius:12px}
#statsDialog .insights-mark svg{width:30px;height:30px}
#statsDialog .insights-heading h2{letter-spacing:-.45px;line-height:1.2;text-transform:none;margin:0}
#statsDialog .insights-heading p{margin:5px 0 0;line-height:1.4}
#statsDialog #closeStats{min-width:94px;flex:none;gap:9px;font-size:14px;min-height:42px}
#statsDialog #closeStats svg{display:block}
#statsDialog .insights-layout{display:grid;grid-template-columns:214px minmax(0,1fr);min-height:0;flex:1}
#statsDialog .stats-tabs{position:static;top:auto;display:flex;flex-direction:column;gap:5px;margin:0;padding:24px 12px 17px;border:0;border-right:1px solid #42364d;border-radius:0;min-width:0;z-index:1}
#statsDialog .insights-nav-caption{margin:0 12px 11px;font-size:12px;font-weight:700;letter-spacing:.1em}
#statsDialog .stats-tabs button{display:flex;align-items:center;justify-content:flex-start;width:100%;min-height:65px;padding:12px 13px;background:transparent;border:1px solid transparent;border-radius:11px;text-align:left;font-weight:500;color:#d4c8df}
#statsDialog .stats-tabs button[aria-selected=true]{color:#fefaff;border:1px solid #746087}
#statsDialog .stats-tabs button:hover:not([aria-selected=true]){background:#2a2235;border-color:#51425e}
#statsDialog .stats-tabs button .insights-nav-symbol{flex:none;font-size:22px;line-height:1;text-align:center;font-weight:500}
#statsDialog .stats-tabs button strong{display:block;line-height:1.3}
#statsDialog .stats-tabs button small{display:block;line-height:1.35;font-weight:400;margin-top:2px}
#statsDialog .insights-nav-privacy{display:flex;align-items:flex-start;gap:9px;margin:auto 13px 3px;line-height:1.55;border-top:1px solid #3e3348;padding-top:17px}
#statsDialog .insights-nav-privacy svg{flex:none;margin-top:2px;color:#b898cf}
#statsDialog .insights-main{overflow-y:auto;overscroll-behavior:contain;min-width:0;min-height:0;padding:25px clamp(20px,3vw,34px) 31px;scrollbar-gutter:stable}
#statsDialog .insights-intro{display:flex;align-items:center;justify-content:space-between;gap:15px;margin-bottom:18px}
#statsDialog .insights-eyebrow{font-size:12px;line-height:1.4;font-weight:700;letter-spacing:.12em;margin:0 0 6px}
#statsDialog .insights-intro h3{line-height:1.2;letter-spacing:-.5px;margin:0;color:#f8f3ff}
#statsDialog .insights-intro p{margin:7px 0 0;line-height:1.5}
#statsDialog .insights-units{display:flex;flex-direction:column;align-items:flex-start;gap:5px;color:#c9b9d5;font-weight:650;flex:none}
#statsDialog .insights-units select{min-width:108px}
#statsDialog .insights-filter-drawer{border:1px solid #44394f;border-radius:10px;margin:0 0 20px;overflow:hidden}
#statsDialog .insights-filter-drawer summary{display:flex;align-items:center;gap:10px;padding:10px 16px;list-style:none;cursor:pointer;color:#e5d9ee}
#statsDialog .insights-filter-drawer summary::-webkit-details-marker{display:none}
#statsDialog .insights-filter-drawer summary strong{font-weight:650}
#statsDialog .insights-filter-drawer summary span:not(.filter-caret){margin-left:6px;font-weight:400}
#statsDialog .insights-filter-drawer .filter-caret{font-size:20px;color:#c9b5d9;margin-left:auto;line-height:1}
#statsDialog .insights-filter-drawer[open] .filter-caret{transform:rotate(180deg)}
#statsDialog .insights-filter-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:11px;padding:15px;border-top:1px solid #44394f}
#statsDialog .insights-filter-grid label{display:flex;flex-direction:column;align-items:stretch;gap:6px;min-width:0;color:#d4c7df;font-weight:600}
#statsDialog .insights-filter-grid select{min-width:0;width:100%;font-size:14px}
#statsDialog .stats-panel{min-height:0;margin:0}
#statsDialog .stats-panel[hidden]{display:none!important}
#statsDialog #metricGrid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr))}
#statsDialog #metricGrid .metric{display:flex;flex-direction:column;justify-content:space-between;min-width:0;padding:19px 17px;border:1px solid #5c4b6b;border-radius:12px;gap:14px}
#statsDialog #metricGrid .metric strong{font-weight:750;color:#fbf7ff;overflow-wrap:anywhere;margin:0}
#statsDialog #metricGrid .metric span{line-height:1.4;margin:0}
#statsDialog #metricGrid button.metric{text-align:left;align-items:stretch;cursor:pointer}
#statsDialog .insights-overview-secondary h4,#statsDialog .insights-content-card h4{font-size:18px;font-weight:700;line-height:1.3;color:#f2e9f8;margin:0}
#statsDialog #otherMetrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:0;margin-top:10px;overflow:hidden}
#statsDialog #otherMetrics .metric{background:transparent;border:0;border-radius:0;padding:14px 15px;min-height:78px;border-right:1px solid #3d3349;border-bottom:1px solid #3d3349}
#statsDialog #otherMetrics .metric:nth-child(4n){border-right:0}
#statsDialog #otherMetrics .metric:nth-last-child(-n+4){border-bottom:0}
#statsDialog #otherMetrics .metric strong{font-weight:650;color:#eee7f5;letter-spacing:-.25px;margin:0;line-height:1.3}
#statsDialog #otherMetrics .metric span{line-height:1.4;margin-top:5px;color:#c8bad2}
#statsDialog .insights-overview-grid,#statsDialog .insights-records-grid,#statsDialog .insights-trends-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));margin-bottom:20px}
#statsDialog .insights-content-card{border:1px solid #453a50;min-width:0;padding:20px}
#statsDialog .insights-card-title{display:flex;justify-content:space-between;align-items:start;gap:12px;margin-bottom:16px}
#statsDialog .insights-card-title p{line-height:1.45;margin:5px 0 0}
#statsDialog .insights-card-title button{flex:none;padding:7px 11px}
#statsDialog .sport-summary{gap:12px}
#statsDialog .sport-row{padding:10px 0;border-bottom:1px solid #3b3247;font-size:14px;gap:10px}
#statsDialog .sport-row strong{color:#e0c6f5;font-size:14px;white-space:nowrap;font-weight:650}
#statsDialog .bike-row{padding:7px 0 7px 8px;margin-left:8px;border-left:2px solid #7c6796}
#statsDialog .ride-list{display:grid;grid-template-columns:minmax(0,1fr);gap:8px;margin:0}
#statsDialog .ride-list-item{display:flex;flex-direction:column;align-items:stretch;text-align:left;gap:5px;min-width:0;border:1px solid #50415e;border-radius:9px}
#statsDialog .ride-list-item strong{line-height:1.35}
#statsDialog .ride-list-item span{line-height:1.4}
#statsDialog .insights-inline-link{background:transparent;border:0;padding:8px 0 0;min-height:34px;justify-content:start;font-weight:650;margin-top:7px}
#statsDialog .insights-inline-link:hover{background:transparent;color:#fff;text-decoration:underline}
#statsDialog .facts{gap:0}
#statsDialog .fact{display:flex;flex-direction:column;gap:5px;align-items:stretch;padding:12px 4px;border-bottom:1px solid #43384e}
#statsDialog .fact strong{font-weight:650;line-height:1.45;color:#f0e9f5}
#statsDialog .fact span{line-height:1.35;order:-1}
#statsDialog .fact-link{display:flex;flex-direction:column;align-items:stretch;justify-content:start;text-align:left;min-height:65px;border:1px solid transparent;padding:12px 13px;border-radius:9px;margin:5px 0;gap:6px}
#statsDialog .fact-link:hover{background:#44334e}
#statsDialog .fact-link::after{content:'Preview route  ↗';line-height:1.2;font-weight:650;margin-top:2px}
#statsDialog .fact-link strong{font-size:15px}
#statsDialog .stat-chart{display:grid;gap:13px;max-height:none;overflow:visible}
#statsDialog .chart-row{grid-template-columns:minmax(68px,max-content) minmax(0,1fr) minmax(55px,max-content);gap:10px;align-items:center;font-size:14px}
#statsDialog .chart-row>span,#statsDialog .chart-row>strong{line-height:1.3}
#statsDialog .chart-track{min-width:0;height:11px}
#statsDialog .insights-weekday{margin-bottom:20px}
#statsDialog .weekday-row{font-size:14px;grid-template-columns:64px minmax(0,1fr) 60px}
#statsDialog .insights-activities-card .ride-list{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
#statsDialog .stats-footer{border-top:1px solid #43384e;margin-top:25px;padding:20px 0 0}
#statsDialog .details-repair{background:#211c2b;border:1px solid #45394f;padding:14px 16px;border-radius:11px;display:flex;align-items:center;justify-content:space-between;gap:16px}
#statsDialog .details-repair strong{font-size:14px;font-weight:650;color:#e9dfef}
#statsDialog .details-repair span{color:#bcafc9;line-height:1.5;margin-top:3px;display:block}
#statsDialog .details-repair button{flex:none;min-height:40px}
#statsDialog .stats-accuracy{margin:12px 0 0;border:1px solid #45394f;border-radius:11px;background:#211c2b;padding:11px 14px}
#statsDialog .stats-accuracy summary{font-weight:600;cursor:pointer}
#statsDialog #statAccuracy{line-height:1.6}
@media(max-width:980px){#statsDialog .insights-layout{grid-template-columns:172px minmax(0,1fr)}#statsDialog .stats-tabs{padding:19px 9px}#statsDialog .stats-tabs button{gap:7px;padding:10px 8px}#statsDialog .insights-nav-caption{margin-left:8px}#statsDialog .insights-nav-privacy{margin-left:8px;margin-right:8px}#statsDialog .insights-main{padding:22px 20px}#statsDialog #metricGrid{grid-template-columns:repeat(2,minmax(0,1fr))}#statsDialog .insights-overview-grid{grid-template-columns:1fr}#statsDialog #otherMetrics{grid-template-columns:repeat(2,minmax(0,1fr))}#statsDialog #otherMetrics .metric:nth-child(4n){border-right:1px solid #3d3349}#statsDialog #otherMetrics .metric:nth-child(2n){border-right:0}#statsDialog #otherMetrics .metric:nth-last-child(-n+4){border-bottom:1px solid #3d3349}#statsDialog #otherMetrics .metric:nth-last-child(-n+2){border-bottom:0}#statsDialog .insights-trends-grid,#statsDialog .insights-records-grid{grid-template-columns:1fr}#statsDialog .insights-filter-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:620px){#statsDialog{margin:0;width:100vw;max-width:100vw;height:100dvh;max-height:100dvh;border-radius:0}#statsDialog .insights-topbar{padding:13px 16px}#statsDialog .insights-heading{gap:10px}#statsDialog .insights-heading .insights-mark{width:38px;height:38px}#statsDialog .insights-mark svg{width:26px;height:26px}#statsDialog #closeStats{min-width:42px;width:42px;min-height:42px;padding:0}#statsDialog #closeStats span{display:none}#statsDialog .insights-layout{display:flex;flex-direction:column}#statsDialog .stats-tabs{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));flex:none;gap:4px;padding:8px 10px;border-right:0;border-bottom:1px solid #42364d}#statsDialog .insights-nav-caption,#statsDialog .insights-nav-privacy{display:none}#statsDialog .stats-tabs button{flex-direction:column;justify-content:center;align-items:center;min-height:57px;gap:2px;padding:6px 2px}#statsDialog .stats-tabs button .insights-nav-symbol{font-size:18px;line-height:1.1}#statsDialog .stats-tabs button small{display:none}#statsDialog .insights-main{overflow-y:auto;padding:18px 15px 24px}#statsDialog .insights-filter-drawer summary span:not(.filter-caret){display:none}#statsDialog #metricGrid{grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}#statsDialog #metricGrid .metric{padding:14px 12px;gap:11px}#statsDialog .insights-overview-secondary{margin:20px 0}#statsDialog #otherMetrics .metric{min-height:72px;padding:12px}#statsDialog .insights-content-card{padding:15px}#statsDialog .insights-activities-card .ride-list{grid-template-columns:1fr}#statsDialog .details-repair{flex-direction:column;align-items:stretch}#statsDialog .details-repair button{width:100%}}

header{position:relative}
header::after{content:"";position:absolute;left:0;right:0;bottom:0;pointer-events:none}
#statsDialog .insights-topbar{position:relative;isolation:isolate}
#statsDialog .insights-topbar::after{content:"";position:absolute;left:0;right:0;bottom:0;pointer-events:none;background:linear-gradient(90deg,#8358d6,#b95cb9 37%,#ed6d80 68%,#ffb26a)}
#statsDialog .stats-tabs button{position:relative;overflow:hidden;transition:background-color .14s,border-color .14s}
#statsDialog .stats-tabs button[aria-selected=true]{border-color:#886b9e;background:linear-gradient(110deg,#4b315b,#382b46);box-shadow:inset 0 1px #ffffff12}
#statsDialog .stats-tabs button[aria-selected=true]::before{content:"";position:absolute;top:10px;bottom:10px;left:0;width:3px;border-radius:0 4px 4px 0;background:linear-gradient(#ce8eea,#f19a9c)}
#statsDialog .stats-tabs button[aria-selected=true] .insights-nav-symbol{color:#f1c5ff}
#statsDialog #metricGrid .metric{--flare:#b58be9;--flare-wash:#b58be91a}
#statsDialog #metricGrid .metric:nth-child(2){--flare:#d681cf;--flare-wash:#d681cf18}
#statsDialog #metricGrid .metric:nth-child(3){--flare:#f38a92;--flare-wash:#f38a9219}
#statsDialog #metricGrid .metric:nth-child(4){--flare:#ffc078;--flare-wash:#ffc07818}
#statsDialog #metricGrid .metric::before{top:0;left:0;right:0;opacity:.95}
#statsDialog #metricGrid button.metric:focus-visible,#statsDialog .stats-tabs button:focus-visible,#statsDialog .ride-list-item:focus-visible,#statsDialog .fact-link:focus-visible{outline:2px solid #efb9ff;outline-offset:2px}
#statsDialog .insights-content-card:hover{border-color:#604b68}
#statsDialog .ride-list-item:hover,#statsDialog .fact-link:hover{border-color:#d9a2e5;box-shadow:0 0 0 1px #a95ec022}
#statsDialog .insights-filter-drawer:focus-within{border-color:#886c9f}
#statsDialog .chart-track{background:#3d3249}
#statsDialog .chart-bar{border-radius:99px}
#statsDialog .insights-topbar button,#statsDialog .insights-card-title button,#statsDialog .details-repair button{display:inline-flex;align-items:center;justify-content:center;text-align:center}
@media(max-width:620px){
  #statsDialog{border:0;box-shadow:none}
  #statsDialog .stats-tabs button strong{letter-spacing:-.15px}
  #statsDialog .stats-tabs button[aria-selected=true]::before{top:auto;bottom:0;left:15px;right:15px;width:auto;height:3px;border-radius:3px 3px 0 0;background:linear-gradient(90deg,#b685eb,#f19a9c)}
}
@media(prefers-reduced-motion:reduce){#statsDialog .stats-tabs button{transition:none}}
#statsDialog .insights-overview-secondary summary{display:flex;align-items:center;gap:10px;cursor:pointer;list-style:none;padding:14px 17px;min-height:50px}
#statsDialog .insights-overview-secondary summary::-webkit-details-marker{display:none}
#statsDialog .insights-overview-secondary summary strong{font-size:15px;color:#efe2f9;font-weight:650}
#statsDialog .insights-overview-secondary summary span:not(.filter-caret){font-size:13px;color:#c5b7d2;font-weight:400}
#statsDialog .insights-overview-secondary summary .filter-caret{font-size:20px;margin-left:auto;color:#ccabd9}
#statsDialog .insights-overview-secondary[open] summary .filter-caret{transform:rotate(180deg)}
#statsDialog .insights-overview-secondary[open] summary{border-bottom:1px solid #493b55}
#statsDialog .insights-overview-secondary #otherMetrics{border:0;border-radius:0;margin:0}
#statsDialog .insights-overview-secondary summary:focus-visible{outline:2px solid #efb9ff;outline-offset:-3px}
@media(max-width:620px){#statsDialog .insights-overview-secondary summary{padding:13px}#statsDialog .insights-overview-secondary summary strong{font-size:14px}#statsDialog .insights-overview-secondary summary span:not(.filter-caret){font-size:12px}}

:root{--lh-ink:#f6f0fb;--lh-subtle:#ccc0d8;--lh-stroke:#59475f;--lh-violet:#b98af0;--lh-pink:#f078aa;--lh-coral:#ff987b;--lh-amber:#ffcf88}
button{display:inline-flex;align-items:center;justify-content:center;gap:8px;text-align:center;line-height:1.25;min-height:40px;transition:background-color .15s ease,border-color .15s ease,box-shadow .15s ease}
button svg{display:block;flex:none}
#plus,#minus,#fit{display:grid;place-items:center;padding:0;line-height:1}
header{background:linear-gradient(115deg,#20162f 0%,#20192c 56%,#261a31 100%)}
header .brand{min-width:0}
header .brand h1{font-weight:750;letter-spacing:-.6px}
header .brand small{font-size:13px;line-height:1.35;letter-spacing:0;color:#d0bfdc}
header .header-actions{gap:9px}
header .header-actions button{font-weight:650;font-size:14px;min-height:42px;padding:10px 15px;white-space:nowrap;border-color:#5e4b6d}
header #statsTop{background:linear-gradient(120deg,#8159b0 0%,#a16cb4 68%,#b878ab 100%);border-color:#b88bc5;box-shadow:inset 0 1px #fff2,0 0 18px #af62b51c}
header #statsTop:hover{background:linear-gradient(120deg,#9568c7,#b680c2)}
header #openSettings,header #shutdown{background:#30243c}
header #openSettings:hover,header #shutdown:hover{background:#463151}
header::after{height:2px;opacity:1;background:linear-gradient(90deg,#8959d7 0%,#c65fbb 35%,#f76a82 67%,#ffb876 98%)}
aside{background:linear-gradient(180deg,#1c1728,#17131f);font-size:14px}
aside h2{font-size:14px;font-weight:700;letter-spacing:.035em;text-transform:uppercase;color:#e8d6f5}
aside .muted,aside .status,aside .foot,aside #tileNotice{font-size:13px;line-height:1.55;color:#c2b4ce}
aside .field label,aside .toggle{font-size:14px;line-height:1.4;color:#e1d5eb}
aside .stat span,aside #dateRange{font-size:13px}
aside button,aside select,aside input{font-size:14px}
.maptop{backdrop-filter:blur(13px);border:1px solid #765785;box-shadow:0 8px 26px #0e081872}
.tools{gap:8px}
.tools button{width:44px;height:44px;min-height:44px;border-radius:12px;background:#2c203edc;border-color:#7d668d;box-shadow:0 5px 17px #130c2299,inset 0 1px #fff1}
.tools button:hover{background:#533663;border-color:#c397d4}
.legend{backdrop-filter:blur(12px);font-size:12px}
#statsDialog{border-color:#66546c;box-shadow:0 25px 90px #090512d9,0 0 32px #ba69b520}
#statsDialog .insights-topbar{background:radial-gradient(ellipse 55% 180% at 6% -75%,#6b3d75 0%,transparent 73%),linear-gradient(100deg,#2a2034,#21192d);border-color:#61466b;padding-block:20px}
#statsDialog .insights-topbar::after{height:2px}
#statsDialog .insights-heading .insights-mark{background:linear-gradient(145deg,#673b75,#402951);border-color:#976eaa;color:#f7d7fc;box-shadow:inset 0 1px #fff3,0 0 24px #ad68d840}
#statsDialog .insights-heading h2{font-size:23px;font-weight:750}
#statsDialog .insights-heading p{font-size:14px;color:#d5c4df}
#statsDialog .stats-tabs{background:linear-gradient(180deg,#201929,#1a1623)}
#statsDialog .stats-tabs button{gap:14px}
#statsDialog .stats-tabs button .insights-nav-symbol{width:26px;height:26px;display:flex;align-items:center;justify-content:center;color:#c5a9da}
#statsDialog .stats-tabs button .insights-nav-symbol svg{width:22px;height:22px;fill:none;stroke:currentColor;stroke-width:1.7;stroke-linecap:round;stroke-linejoin:round}
#statsDialog .stats-tabs button[aria-selected="true"]{background:linear-gradient(105deg,#523361,#392a48);border-color:#9673a4;box-shadow:0 0 19px #9d60c01f,inset 0 1px #fff1}
#statsDialog .stats-tabs button[aria-selected="true"] .insights-nav-symbol{color:#e8baf8}
#statsDialog .stats-tabs button strong{font-size:15px;font-weight:650}
#statsDialog .stats-tabs button small{font-size:13px;color:#c3b4d0}
#statsDialog .insights-nav-caption{color:#cdb3de}
#statsDialog .insights-nav-privacy{color:#c7b9d5;font-size:13px}
#statsDialog .insights-main{background:radial-gradient(ellipse 40% 12% at 99% 0%,#39213c55,transparent 100%),#181421}
#statsDialog .insights-eyebrow{color:#dcaae4}
#statsDialog .insights-intro h3{font-weight:750;font-size:27px}
#statsDialog .insights-intro p{color:#d1c2dc;font-size:15px}
#statsDialog .insights-units{font-size:14px}
#statsDialog .insights-filter-drawer{border-color:#5d4867;background:#241c2e}
#statsDialog .insights-filter-drawer summary{min-height:50px}
#statsDialog .insights-filter-drawer summary strong{font-size:15px}
#statsDialog .insights-filter-drawer summary span:not(.filter-caret){font-size:14px;color:#cbbad6}
#statsDialog .insights-filter-grid label{font-size:14px}
#statsDialog #metricGrid{gap:13px}
#statsDialog #metricGrid .metric{position:relative;isolation:isolate;overflow:hidden;min-height:139px;background:linear-gradient(145deg,#372742,#2c2238);border-color:#75567c;box-shadow:inset 0 1px #fff1;--lh-accent:var(--lh-violet);--lh-wash:#bb8bfb20}
#statsDialog #metricGrid .metric:nth-child(2){--lh-accent:var(--lh-pink);--lh-wash:#df7ccf20}
#statsDialog #metricGrid .metric:nth-child(3){--lh-accent:var(--lh-coral);--lh-wash:#f2878220}
#statsDialog #metricGrid .metric:nth-child(4){--lh-accent:var(--lh-amber);--lh-wash:#ffd18820}
#statsDialog #metricGrid .metric::before{content:"";position:absolute;inset:0 auto auto 0;width:100%;height:3px;background:linear-gradient(90deg,var(--lh-accent),transparent 88%);pointer-events:none;z-index:0}
#statsDialog #metricGrid .metric::after{content:"";position:absolute;width:175px;height:175px;top:-73px;right:-70px;border-radius:50%;background:radial-gradient(circle,var(--lh-wash) 0%,transparent 70%);pointer-events:none;z-index:0}
#statsDialog #metricGrid .metric strong,#statsDialog #metricGrid .metric span{position:relative;z-index:1}
#statsDialog #metricGrid .metric strong{font-size:clamp(23px,2.05vw,31px);font-variant-numeric:tabular-nums;letter-spacing:-.85px;line-height:1.2}
#statsDialog #metricGrid .metric span{font-size:15px;font-weight:550;color:#e8dbed}
#statsDialog #metricGrid button.metric:hover{border-color:var(--lh-accent);background:linear-gradient(140deg,#4d3055,#37243f);box-shadow:0 0 20px var(--lh-wash)}
#statsDialog .insights-overview-secondary{display:block;margin:21px 0 25px;background:#241c2e;border:1px solid #57445e;border-radius:13px;padding:0;overflow:hidden}
#statsDialog .insights-more-trigger{cursor:pointer;display:flex;align-items:center;justify-content:space-between;padding:16px 20px;gap:12px;list-style:none;min-height:70px}
#statsDialog .insights-more-trigger::-webkit-details-marker{display:none}
#statsDialog .insights-more-trigger:focus-visible{outline:2px solid #e2aef3;outline-offset:-3px}
#statsDialog .insights-more-trigger .insights-more-label{display:flex;gap:13px;align-items:center;min-width:0}
#statsDialog .insights-more-icon{width:35px;height:35px;border:1px solid #775581;background:#422e4d;border-radius:10px;display:grid;place-items:center;color:#edb6e6;font-size:20px;flex:none}
#statsDialog .insights-more-trigger strong{display:block;font-size:16px;font-weight:700;color:#f6eafb;line-height:1.2}
#statsDialog .insights-more-trigger small{display:block;font-size:14px;margin-top:5px;color:#cdbbd8;line-height:1.25}
#statsDialog .insights-more-trigger>svg{width:21px;height:21px;stroke:currentColor;stroke-width:2;fill:none;flex:none;color:#e0bee9;transition:transform .14s}
#statsDialog #insightsMore[open] .insights-more-trigger>svg{transform:rotate(180deg)}
#statsDialog #otherMetrics{margin:0;border:0;border-top:1px solid #584660;border-radius:0;background:#211a2c}
#statsDialog #otherMetrics .metric strong{font-size:21px}
#statsDialog #otherMetrics .metric span{font-size:14px}
#statsDialog .insights-overview-grid,#statsDialog .insights-records-grid,#statsDialog .insights-trends-grid{gap:17px}
#statsDialog .insights-content-card{background:linear-gradient(145deg,#2c2335,#241e2d);border-color:#5c4763;border-radius:14px}
#statsDialog .insights-card-title h4{font-size:19px;font-weight:700}
#statsDialog .insights-card-title p{font-size:14px;color:#d0bed7}
#statsDialog .insights-card-title button{font-size:14px;min-height:39px}
#statsDialog .sport-row,#statsDialog .sport-row strong{font-size:14px}
#statsDialog .ride-list-item{min-height:68px;padding:12px 14px;background:#332740;border-color:#5f4b6b;transition:background-color .15s,border-color .15s,transform .15s}
#statsDialog .ride-list-item:hover{background:#47314c;border-color:#c08bc9;box-shadow:0 0 0 1px #ba72bc26;transform:translateY(-1px)}
#statsDialog .ride-list-item strong{font-size:15px;font-weight:650}
#statsDialog .ride-list-item span{font-size:14px;color:#d1c1dc}
#statsDialog .insights-inline-link{font-size:15px;color:#eec1fa}
#statsDialog .fact strong{font-size:16px}
#statsDialog .fact span{font-size:14px;color:#c9b9d4}
#statsDialog .fact-link{background:#33273c;border-color:#584366}
#statsDialog .fact-link:hover{border-color:#c98fd1;box-shadow:0 0 12px #aa5caa20}
#statsDialog .fact-link::after{font-size:13px;color:#f0bbef}
#statsDialog .chart-row>span,#statsDialog .chart-row>strong{font-size:14px}
#statsDialog .chart-bar{background:linear-gradient(90deg,#9568d4 0%,#dd68ba 49%,#fa9b75 95%)}
#statsDialog .stats-accuracy summary{font-size:14px}
#statsDialog #statAccuracy{font-size:14px}
#statsDialog .details-repair span{font-size:14px}
#statsDialog .details-repair button{font-size:14px}
@media(max-width:980px){#statsDialog #metricGrid .metric{min-height:128px}}
@media(max-width:620px){
header .header-actions{gap:6px}
#statsDialog .insights-heading h2{font-size:19px}
#statsDialog .insights-heading p{font-size:13px}
#statsDialog .stats-tabs button .insights-nav-symbol{width:23px;height:23px}
#statsDialog .stats-tabs button .insights-nav-symbol svg{width:21px;height:21px}
#statsDialog .stats-tabs button strong{font-size:14px}
#statsDialog .insights-intro h3{font-size:22px}
#statsDialog .insights-intro p{font-size:14px}
#statsDialog #metricGrid .metric{min-height:115px}
#statsDialog #metricGrid .metric strong{font-size:clamp(20px,5.8vw,26px)}
#statsDialog #metricGrid .metric span{font-size:14px}
#statsDialog .insights-more-trigger{padding:14px 12px}
#statsDialog .insights-more-trigger small{font-size:13px}
#statsDialog .insights-card-title h4{font-size:18px}
}
@media(prefers-reduced-motion:reduce){button,#statsDialog .ride-list-item,#statsDialog .insights-more-trigger>svg{transition:none}#statsDialog .ride-list-item:hover{transform:none}}


#ridePreviewDialog,#settingsDialog,#welcomeDialog{border-color:#72567b;box-shadow:0 25px 88px #080513d0,0 0 25px #a55e9a17}
#ridePreviewDialog{background:radial-gradient(ellipse at 5% 0%,#47294d66,transparent 36%),#211a2a}
#ridePreviewDialog .dialog-head h2{font-size:24px;font-weight:750}
#ridePreviewDialog .dialog-head .muted{color:#d0c0de;font-size:15px}
#ridePreviewDialog .ride-preview-map{border-color:#74547e;box-shadow:inset 0 0 27px #6d2f7821}
#ridePreviewDialog .ride-preview-badge{border-color:#80618d;color:#f6dffb;font-size:14px}
#ridePreviewDialog .ride-preview-details{gap:11px}
#ridePreviewDialog .ride-preview-detail{position:relative;overflow:hidden;background:linear-gradient(145deg,#31243b,#272031);border-color:#654c70;border-radius:11px;padding:14px}
#ridePreviewDialog .ride-preview-detail::before{content:"";position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,#b77cda,#e582ad55,transparent)}
#ridePreviewDialog .ride-preview-detail span{color:#d2c1da;font-size:14px}
#ridePreviewDialog .ride-preview-detail strong{font-size:17px;font-weight:650}
#ridePreviewDialog .ride-preview-actions button{min-height:43px;font-size:14px;font-weight:650}
#ridePreviewDialog #showRideMap{background:linear-gradient(110deg,#9875bb,#c494b7);border-color:#c8a0ce;color:#21162c}
#ridePreviewDialog #showRideMap:disabled{opacity:.5}
#ridePreviewDialog #ridePreviewStatus{font-size:14px;color:#d0c1dd}
#settingsDialog>.dialog-head{background:linear-gradient(95deg,#32243e,#241c30);border-color:#624d6e}
#settingsDialog>.dialog-head::after{content:"";position:absolute;bottom:0;left:0;right:0;height:2px;background:linear-gradient(90deg,#9b67d4,#d86aaf,#f4ac79)}
#settingsDialog .settings-section h2{font-size:18px;font-weight:700;color:#f0e6f5}
#settingsDialog .settings-savebar{background:#292033;border-color:#6a5178}
#settingsDialog #saveSettings{font-weight:700}
#welcomeDialog .welcome-icon{color:#dfb1f1;text-shadow:0 0 23px #dc87ef6a}
#welcomeDialog .welcome-privacy{font-size:14px;line-height:1.5}
@media(max-width:700px){#ridePreviewDialog .ride-preview-detail{padding:12px}#ridePreviewDialog .ride-preview-detail strong{font-size:16px}}


/* Unified statistical language across the overview, personal records, and route previews. */
#statsDialog #metricGrid{gap:12px;margin-bottom:25px}
#statsDialog #metricGrid .metric{min-height:128px;gap:13px}
#statsDialog #metricGrid .metric:nth-child(4n+1){--lh-accent:#b98af0;--lh-wash:#bb8bfb20}
#statsDialog #metricGrid .metric:nth-child(4n+2){--lh-accent:#f078aa;--lh-wash:#df7ccf20}
#statsDialog #metricGrid .metric:nth-child(4n+3){--lh-accent:#ff987b;--lh-wash:#f2878220}
#statsDialog #metricGrid .metric:nth-child(4n+4){--lh-accent:#ffcf88;--lh-wash:#ffd18820}
#statsDialog .insights-records-grid{display:flex;flex-direction:column;gap:18px}
#statsDialog .insights-records-grid>.insights-content-card{padding:21px}
#statsDialog #facts{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
#statsDialog #facts .record-metric{position:relative;isolation:isolate;display:flex;flex-direction:column;align-items:stretch;justify-content:space-between;gap:8px;overflow:hidden;min-height:138px;min-width:0;padding:19px 17px;text-align:left;border:1px solid #75567c;border-radius:12px;background:linear-gradient(145deg,#372742,#2c2238);box-shadow:inset 0 1px #fff1}
#statsDialog #facts .record-metric::before{content:"";position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,var(--record-accent,#b98af0),transparent 88%)}
#statsDialog #facts .record-metric:nth-child(4n+2){--record-accent:#f078aa}
#statsDialog #facts .record-metric:nth-child(4n+3){--record-accent:#ff987b}
#statsDialog #facts .record-metric:nth-child(4n+4){--record-accent:#ffcf88}
#statsDialog #facts .record-metric strong{order:0;font-weight:750;color:#fbf7ff;font-size:clamp(21px,2vw,28px);line-height:1.15;letter-spacing:-.5px;overflow-wrap:anywhere}
#statsDialog #facts .record-metric span{order:0;font-size:14px;color:#eee0f2;font-weight:600;line-height:1.4}
#statsDialog #facts .record-metric small{font-size:13px;color:#c9b6d3;line-height:1.35;overflow-wrap:anywhere}
#statsDialog #facts .record-metric-link{cursor:pointer}
#statsDialog #facts .record-metric-link:hover{background:linear-gradient(145deg,#48304f,#31253c);border-color:#e5a9e8;box-shadow:0 0 19px #c16bd02b}
#statsDialog #facts .record-metric-link small::after{content:'  ↗';color:#f2bde9;font-weight:700}
#statsDialog #topRoutes{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:11px}
#statsDialog #topRoutes .ride-list-item{background:linear-gradient(145deg,#32253b,#292035);min-height:83px;padding:14px 16px;border-color:#654e73}
#statsDialog #topRoutes .ride-list-item:hover{background:#3e2c48;border-color:#dd9ade}
#statsDialog #topRoutes .ride-list-item strong{font-size:15px}
#statsDialog #topRoutes .ride-list-item span{font-size:13px}
#ridePreviewDialog .ride-preview-details{grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
#ridePreviewDialog .ride-preview-detail{min-height:85px;padding:15px}
#ridePreviewDialog .ride-preview-detail strong{font-size:18px;font-weight:700}
#ridePreviewDialog .ride-preview-detail span{font-size:13px;margin:0 0 10px}
.legend{padding:12px 14px;min-width:235px;backdrop-filter:blur(12px);z-index:2}
.legend .bar{height:9px}
.legend-select{margin-top:11px;padding-top:10px;border-top:1px solid #594568;display:flex;justify-content:space-between;gap:11px;align-items:center}
.legend-select span{font-size:12px;font-weight:600;color:#d7c5e4;white-space:nowrap}
.legend-select select{font-size:13px;font-weight:600;padding:7px 8px;min-width:0;max-width:137px;background:#31233e;border-color:#785d8a;color:#f6e9fb}
@media(max-width:980px){#statsDialog #facts{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:700px){#ridePreviewDialog .ride-preview-details{grid-template-columns:repeat(2,minmax(0,1fr))}#statsDialog #topRoutes{grid-template-columns:1fr}}
@media(max-width:520px){#statsDialog #facts{grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}#statsDialog #facts .record-metric{min-height:125px;padding:13px}#statsDialog #facts .record-metric strong{font-size:20px}#statsDialog #metricGrid .metric{min-height:105px}#statsDialog #metricGrid{margin-bottom:20px}.legend{left:10px;bottom:10px;max-width:calc(100% - 96px);min-width:215px}}


/* Overview is one coherent story: totals, records, trends, history, and activity mix. */
#statsDialog .stats-tabs{padding-top:22px}
#statsDialog .stats-story-section{min-width:0;margin:0 0 31px}
#statsDialog .stats-story-heading{display:flex;align-items:end;justify-content:space-between;gap:12px;margin:0 0 17px}
#statsDialog .stats-story-kicker{display:block;color:#d6a4df;letter-spacing:.11em;font-size:11px;font-weight:750;line-height:1.4;margin:0 0 6px}
#statsDialog .stats-story-heading h4{margin:0;font-size:21px;font-weight:750;letter-spacing:-.3px;line-height:1.25;color:#f5ecfa}
#statsDialog .stats-story-heading p{margin:5px 0 0;color:#cbbcd5;font-size:14px;line-height:1.48}
#statsDialog #metricGrid{display:flex;flex-wrap:wrap;align-items:flex-start;column-gap:14px;row-gap:19px;margin:0}
#statsDialog #metricGrid .stats-metric-group{min-width:0;flex:0 0 100%}
#statsDialog #metricGrid .stats-group--time{flex-basis:calc(60% - 8.4px)}
#statsDialog #metricGrid .stats-group--speed{flex-basis:calc(40% - 5.6px)}
#statsDialog #metricGrid .stats-metric-group-label{font-weight:700;color:#e7d5ef;font-size:14px;line-height:1.35;margin:0 0 10px}
#statsDialog #metricGrid .stats-metric-cards{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
#statsDialog #metricGrid .stats-group--time .stats-metric-cards{grid-template-columns:repeat(3,minmax(0,1fr))}
#statsDialog #metricGrid .stats-group--speed .stats-metric-cards{grid-template-columns:repeat(2,minmax(0,1fr))}
#statsDialog #metricGrid .stats-group--rhythm .stats-metric-cards{grid-template-columns:repeat(3,minmax(0,1fr))}
#statsDialog #metricGrid .metric{min-height:118px;padding:18px 16px;gap:11px}
#statsDialog #metricGrid .metric strong{font-size:clamp(22px,2vw,30px);line-height:1.12}
#statsDialog #metricGrid .metric span{font-size:14px;line-height:1.35}
#statsDialog .facts,#statsDialog #facts,#statsDialog #milestones{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
#statsDialog #facts .record-metric,#statsDialog #milestones .record-metric{position:relative;isolation:isolate;display:flex;flex-direction:column;align-items:stretch;justify-content:space-between;gap:8px;overflow:hidden;min-width:0;min-height:130px;padding:17px 16px;text-align:left;border:1px solid #75567c;border-radius:12px;background:linear-gradient(145deg,#372742,#2c2238);box-shadow:inset 0 1px #fff1}
#statsDialog #facts .record-metric::before,#statsDialog #milestones .record-metric::before{content:"";position:absolute;inset:0 0 auto;height:3px;background:linear-gradient(90deg,var(--record-accent,#b98af0),transparent 88%)}
#statsDialog #facts .record-metric:nth-child(4n+2),#statsDialog #milestones .record-metric:nth-child(4n+2){--record-accent:#f078aa}
#statsDialog #facts .record-metric:nth-child(4n+3),#statsDialog #milestones .record-metric:nth-child(4n+3){--record-accent:#ff987b}
#statsDialog #facts .record-metric:nth-child(4n+4),#statsDialog #milestones .record-metric:nth-child(4n+4){--record-accent:#ffcf88}
#statsDialog #facts .record-metric strong,#statsDialog #milestones .record-metric strong{order:0;font-size:clamp(21px,2vw,28px);line-height:1.2;font-weight:750;letter-spacing:-.5px;color:#fbf7ff;overflow-wrap:anywhere}
#statsDialog #facts .record-metric span,#statsDialog #milestones .record-metric span{order:0;font-size:14px;color:#eee0f2;line-height:1.35;font-weight:600}
#statsDialog #facts .record-metric small,#statsDialog #milestones .record-metric small{font-size:13px;color:#c9b6d3;line-height:1.4;overflow-wrap:anywhere}
#statsDialog #facts button.record-metric:hover,#statsDialog #milestones button.record-metric:hover{background:linear-gradient(145deg,#48304f,#31253c);border-color:#e5a9e8;box-shadow:0 0 19px #c16bd02b}
#statsDialog #facts .record-metric-link:focus-visible,#statsDialog #milestones .record-metric-link:focus-visible{outline:2px solid #efb9ff;outline-offset:2px}
#statsDialog .insights-weekday{margin:0}
#statsDialog .insights-trends-grid{margin-bottom:15px}
#statsDialog .stats-sport-card{padding:18px 21px}
#statsDialog .stats-sport-card .sport-row:last-child{border-bottom:0}
#statsDialog #statsActivities .insights-activities-card .ride-list{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:11px}
#statsDialog .stats-footer{margin-top:7px}
@media(max-width:1100px){
 #statsDialog #metricGrid .stats-group--time,#statsDialog #metricGrid .stats-group--speed{flex-basis:100%}
 #statsDialog #metricGrid .stats-group--distance .stats-metric-cards{grid-template-columns:repeat(2,minmax(0,1fr))}
 #statsDialog #metricGrid .stats-group--time .stats-metric-cards,#statsDialog #metricGrid .stats-group--rhythm .stats-metric-cards{grid-template-columns:repeat(3,minmax(0,1fr))}
}
@media(max-width:730px){
 #statsDialog #metricGrid .stats-group--time .stats-metric-cards,#statsDialog #metricGrid .stats-group--rhythm .stats-metric-cards{grid-template-columns:repeat(2,minmax(0,1fr))}
 #statsDialog #metricGrid .stats-group--time .metric:last-child,#statsDialog #metricGrid .stats-group--rhythm .metric:last-child{grid-column:span 2}
 #statsDialog #facts,#statsDialog #milestones{grid-template-columns:repeat(2,minmax(0,1fr))}
 #statsDialog #statsActivities .insights-activities-card .ride-list{grid-template-columns:1fr}
}
@media(max-width:620px){
 #statsDialog .stats-tabs{grid-template-columns:repeat(2,minmax(0,1fr))}
 #statsDialog .stats-tabs button{min-height:61px;flex-direction:row;justify-content:center;gap:10px}
 #statsDialog .stats-tabs button small{display:block;font-size:11px}
 #statsDialog .stats-story-section{margin-bottom:28px}
 #statsDialog .stats-story-heading h4{font-size:19px}
 #statsDialog #metricGrid{gap:19px}
 #statsDialog #metricGrid .metric{min-height:108px;padding:14px 12px}
 #statsDialog #metricGrid .metric strong{font-size:clamp(20px,5vw,27px)}
 #statsDialog #facts .record-metric,#statsDialog #milestones .record-metric{min-height:123px;padding:13px 12px}
 #statsDialog #facts .record-metric strong,#statsDialog #milestones .record-metric strong{font-size:clamp(18px,5vw,23px)}
 #statsDialog #facts .record-metric span,#statsDialog #milestones .record-metric span{font-size:13px}
}
@media(max-width:375px){#statsDialog #facts,#statsDialog #milestones{grid-template-columns:1fr}}

/* Discreet, consistent contextual help. The tooltip lives at body level so modal scrolling never clips it. */
.lh-help-trigger{display:inline-flex;flex:none;align-items:center;justify-content:center;width:19px;height:19px;min-width:19px;min-height:19px;padding:0;border:1px solid #8a719d;border-radius:50%;background:#30263c;color:#e2c9f4;font:700 12px/1 system-ui,sans-serif;cursor:help;vertical-align:middle;box-shadow:0 0 0 1px #100c18aa;z-index:3}
.lh-help-trigger:hover,.lh-help-trigger:focus-visible{color:#1b1324;background:#e7c2ff;border-color:#efceff;outline:2px solid #d19eeb;outline-offset:2px}
#lhHelpPopover{position:fixed;z-index:2147483647;max-width:350px;padding:14px 16px 15px;border:1px solid #aa87bf;border-radius:12px;background:#221a2e;color:#f6effa;box-shadow:0 14px 42px #000b;pointer-events:none;font-size:13px;line-height:1.55;white-space:normal;overflow-wrap:break-word}
#lhHelpPopover[hidden]{display:none!important}
#lhHelpPopover strong{display:block;margin-bottom:5px;font-size:14px;color:#f0ceff;font-weight:750}
#lhHelpPopover span{display:block;color:#e7daee}
.lh-help-heading{display:inline-flex;align-items:center;gap:9px;flex-wrap:wrap;max-width:100%}
.lh-help-heading .lh-help-trigger{font-size:11px;width:18px;height:18px;min-width:18px;min-height:18px}
.lh-help-field{position:relative;min-width:0}.lh-help-field>label{padding-right:26px}.lh-help-field>.lh-help-trigger{position:absolute;right:0;top:0}
.lh-help-toggle-wrap,.lh-help-label-wrap{display:flex;align-items:center;gap:7px;min-width:0}
.lh-help-toggle-wrap>label,.lh-help-label-wrap>label{flex:1;min-width:0}
.lh-help-label-wrap{align-items:flex-start}.lh-help-label-wrap>.lh-help-trigger{margin-top:2px}
.lh-help-action-wrap{display:inline-flex;align-items:center;gap:6px;min-width:0;max-width:100%}
.lh-help-action-wrap>button:not(.lh-help-trigger){min-width:0}.lh-help-action-wrap>.lh-help-trigger{align-self:center}
.lh-help-action-full{display:flex;width:100%}.lh-help-action-full>button:not(.lh-help-trigger){flex:1;width:auto;margin-top:0}
.row>.lh-help-action-wrap{flex:1}
.header-actions>.lh-help-action-wrap{flex:none}
.tools>.lh-help-action-wrap{display:flex;align-items:center;gap:5px}
.tools .lh-help-trigger{width:18px!important;min-width:18px!important;height:18px!important;min-height:18px!important;padding:0!important;font-size:12px!important}
#statsDialog .stats-tabs .lh-help-tab{position:relative;padding-right:29px}
.lh-tab-question{position:absolute;right:7px;top:8px;width:17px;height:17px;border-radius:50%;border:1px solid #997baa;display:grid;place-items:center;font-size:11px;font-weight:700;color:#d9b9ee}
.lh-help-card{position:relative;min-width:0;padding-right:max(25px,1em)}
.lh-help-card>.lh-help-trigger{position:absolute;right:9px;top:9px}
.lh-help-cardwrap{position:relative;min-width:0;display:flex;max-width:100%;height:100%}
.lh-help-cardwrap>.lh-help-card-content{flex:1;min-width:0;padding-right:35px!important}
.lh-help-cardwrap>.lh-help-trigger{position:absolute;top:9px;right:8px}
#statsDialog .stats-metric-cards>.lh-help-cardwrap,#statsDialog #facts>.lh-help-cardwrap,#statsDialog #milestones>.lh-help-cardwrap{width:100%}
#statsDialog .stats-metric-cards>.lh-help-cardwrap .metric,#statsDialog #facts>.lh-help-cardwrap .metric,#statsDialog #milestones>.lh-help-cardwrap .metric{width:100%}
#statsDialog .ride-list>.lh-help-action-wrap{display:flex;width:100%;min-width:0}
#statsDialog .ride-list>.lh-help-action-wrap>.ride-list-item{width:100%;flex:1}
#statsDialog .insights-content-card.lh-help-card{padding-right:33px}
#statsDialog .insights-content-card.lh-help-card>.lh-help-trigger{top:15px;right:12px}
#statsDialog .stats-metric-group-label .lh-help-trigger{position:static}
#statsDialog .stats-accuracy .lh-help-heading{cursor:pointer}
#settingsDialog .settings-actions .lh-help-heading{display:inline-flex}
#settingsDialog .settings-section>.lh-help-heading{margin-right:0}
#settingsDialog .settings-savebar>.lh-help-action-wrap{flex:none}
.legend.lh-help-card{padding-right:13px}.legend.lh-help-card>.lh-help-trigger{right:9px;top:8px}
.legend .lh-help-label-wrap{gap:5px}.legend .lh-help-label-wrap select{max-width:117px}
.maptop.lh-help-card{padding-right:35px}.maptop.lh-help-card>.lh-help-trigger{pointer-events:auto}
.empty .panel.lh-help-card{padding-right:36px}
.ride-preview-map.lh-help-card{padding-right:0}.ride-preview-map.lh-help-card>.lh-help-trigger{top:10px;right:10px;z-index:7}
@media(max-width:600px){#lhHelpPopover{font-size:12px;padding:12px 13px}.lh-help-action-wrap{gap:4px}.lh-help-trigger{width:18px;height:18px;min-width:18px;min-height:18px}#statsDialog .stats-tabs .lh-help-tab{padding-right:20px}.lh-tab-question{top:3px;right:3px}}

#statsDialog #metricGrid .metric.lh-help-card,#statsDialog .record-metric.lh-help-card{padding-right:35px!important}
#statsDialog .insights-intro>.lh-help-label-wrap{flex:none;align-items:center}
#statsDialog .insights-intro>.lh-help-label-wrap>label{flex:none}

@media(max-width:1060px){#statsDialog .details-repair{flex-direction:column;align-items:stretch}#statsDialog .details-repair>.lh-help-action-wrap{width:fit-content;max-width:100%}#statsDialog .details-repair>.lh-help-action-wrap>button:not(.lh-help-trigger){white-space:normal}}
#statsDialog .details-repair>div:first-child{min-width:0}
#statsDialog .insights-filter-drawer summary>.lh-help-trigger{margin-left:2px;order:2}

.lh-help-date-wrap{display:flex;flex:1;align-items:center;gap:5px;min-width:0}
.lh-help-date-wrap>input{flex:1;width:100%;min-width:0}
.lh-help-date-wrap>.lh-help-trigger{min-width:18px}
@media(max-width:420px){.lh-help-date-wrap{gap:2px}.lh-help-date-wrap>.lh-help-trigger{width:16px;min-width:16px;height:16px;min-height:16px;font-size:10px}}

/* Positioned map overlays must not inherit the generic help-card position. */
#map .maptop.lh-help-card{position:absolute;top:17px;left:18px;width:max-content;max-width:min(380px,calc(100% - 85px))}
#map .legend.lh-help-card{position:absolute;bottom:16px;left:16px;top:auto;width:238px;max-width:calc(100% - 95px);padding-right:28px}
@media(max-width:540px){#map .maptop.lh-help-card{top:12px;left:10px;max-width:calc(100% - 100px)}#map .legend.lh-help-card{bottom:10px;left:10px;max-width:calc(100% - 98px)}}

/* Hide help for conditionally hidden actions, without changing their existing show/hide logic. */
.lh-help-action-wrap:has(>button[hidden]){display:none!important}
#map .legend.lh-help-card{width:268px}
.legend .lh-help-label-wrap select{max-width:139px}

/* Uniform help affordance: no inherited button sizing, pills, or oval shapes. */
button.lh-help-trigger,#statsDialog button.lh-help-trigger,#settingsDialog button.lh-help-trigger,#map button.lh-help-trigger{
 box-sizing:border-box!important;inline-size:18px!important;block-size:18px!important;min-inline-size:18px!important;min-block-size:18px!important;
 max-inline-size:18px!important;max-block-size:18px!important;flex:0 0 18px!important;
 margin:0!important;padding:0!important;border-radius:50%!important;
 font:700 11px/16px system-ui,sans-serif!important;letter-spacing:0!important;white-space:nowrap!important;
 text-align:center!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;
}
/* Keep card explanations clear of text, values, and actions. */
#statsDialog .stats-sport-card.lh-help-card{padding-top:39px;padding-right:21px}
#statsDialog .details-repair.lh-help-card{padding-right:38px}
#statsDialog .details-repair.lh-help-card>.lh-help-trigger{top:10px;right:10px}
#statsDialog .insights-content-card.lh-help-card .insights-card-title{padding-right:20px}
#statsDialog .insights-content-card.lh-help-card>.lh-help-trigger{top:13px;right:12px}

/* Keep dialog branding and contextual help aligned with the layout. */
#statsDialog .insights-heading .insights-mark{width:50px;height:50px;display:grid;place-items:center;padding:0;border:0;background:none;box-shadow:none}
#statsDialog .insights-mark img{display:block;width:100%;height:100%;object-fit:contain;filter:drop-shadow(0 0 9px #db72e750)}
#settingsDialog .settings-dialog-brand{display:flex;align-items:center;gap:13px;min-width:0}
#settingsDialog .settings-dialog-brand img{display:block;width:46px;height:46px;flex:none;object-fit:contain;filter:drop-shadow(0 0 9px #db72e750)}
#settingsDialog .settings-dialog-brand>div{min-width:0}
#settingsDialog>.dialog-head{gap:20px}
#settingsDialog>.dialog-head .muted{max-width:525px}
#settingsDialog .settings-section.lh-help-card{position:relative;padding-right:0;min-width:0}
#settingsDialog .settings-section.lh-help-card>h2{padding-right:30px}
#settingsDialog .settings-section.lh-help-card>.lh-help-trigger{top:3px;right:1px}
#settingsDialog .settings-section .field{min-width:0}
#settingsDialog .settings-section .settings-actions>div{flex:1;min-width:0}
#settingsDialog .settings-section .settings-actions>button{flex:none;min-width:0;max-width:100%;text-align:center;white-space:normal;overflow-wrap:normal;line-height:1.3}
#settingsDialog .settings-savebar{min-width:0;gap:15px}
#settingsDialog .settings-save-state{min-width:0;overflow-wrap:anywhere}
#statsDialog .insights-filter-drawer.lh-help-card{padding-right:0;position:relative}
#statsDialog .insights-filter-drawer.lh-help-card>summary{padding-right:40px;min-width:0}
#statsDialog .insights-filter-drawer.lh-help-card>.lh-help-trigger{right:11px;top:12px}
#statsDialog .details-repair.lh-help-card{padding-right:40px;gap:14px}
#statsDialog .details-repair.lh-help-card>div{flex:1;min-width:0}
#statsDialog .details-repair.lh-help-card>button{flex:none;min-width:0;max-width:100%;white-space:normal;line-height:1.35;text-align:center}
#statsDialog .insights-content-card.lh-help-card{padding-right:29px}
#statsDialog .insights-content-card.lh-help-card>.lh-help-trigger{right:11px;top:11px}
#statsDialog .stats-sport-card.lh-help-card{padding-top:35px;padding-right:21px}
#statsDialog .stats-sport-card .sport-row{gap:14px;align-items:baseline}
#statsDialog .stats-sport-card .sport-row span{min-width:0;flex:1;overflow-wrap:anywhere}
#statsDialog .stats-sport-card .sport-row strong{flex:none;text-align:right;white-space:nowrap}
#statsDialog .insights-nav-caption{display:none}
#statsDialog #closeStats{display:inline-flex;align-items:center;justify-content:center;gap:9px;line-height:1.2;padding:10px 15px}
@media(max-width:700px){
 #settingsDialog .settings-dialog-brand img{width:39px;height:39px}
 #statsDialog .insights-heading .insights-mark{width:40px;height:40px}
 #statsDialog .details-repair.lh-help-card{padding-right:35px}
 #settingsDialog .settings-actions{gap:11px}
 #settingsDialog .settings-actions>button{width:auto;align-self:flex-start}
}
@media(max-width:420px){
 #settingsDialog>.dialog-head{gap:10px}
 #settingsDialog .settings-dialog-brand{gap:9px}
 #settingsDialog .settings-dialog-brand img{width:34px;height:34px}
 #settingsDialog>.dialog-head h2{font-size:21px}
 #settingsDialog>.dialog-head .muted{font-size:12px;line-height:1.35}
 #statsDialog #closeStats{padding:8px;min-width:40px}
}
</style></head>
<body><header><div class="brand"><img class="logo" src="/logo.png" alt="LibreHeatmaps route heatmap"><div><h1>LibreHeatmaps</h1><small>Free, open-source GPS heatmaps. Your data stays local. Your history stays yours.</small></div></div><div class="header-actions"><button class="primary stats-header" id="statsTop">Activity statistics</button><button id="openSettings" title="Adjust appearance and manage your local activity data">Settings</button><button id="shutdown" title="Stop the local application">Exit app</button></div></header>
<div class="workspace"><aside>
<section><h2>Import activities</h2><div class="muted">Choose your complete Strava export ZIP as-is—no extraction needed—or import activities.csv or individual FIT, GPX, and TCX tracks.</div><button class="primary full" id="import">Import Strava ZIP, activity, or CSV</button><input id="file" type="file" accept=".zip,.csv,.fit,.fit.gz,.gpx,.gpx.gz,.tcx,.tcx.gz"><div class="status" id="importStatus">Your activities remain on your computer. No uploads to external services.</div><div class="stats"><div class="stat"><strong id="activityCount">0</strong><span>Activities</span></div><div class="stat"><strong id="pointCount">0</strong><span>Route points</span></div></div></section>
<section><h2>Map view</h2><label class="toggle" for="onlineTiles">Online map tiles <input id="onlineTiles" type="checkbox" checked></label><div class="muted privacy-notice" id="tileNotice">Online basemaps load map tiles from third-party providers. Your Strava files and statistics stay local.</div><div class="row"><button data-map="dark" class="primary">Dark</button><button data-map="light">Light</button><button data-map="satellite">Satellite</button></div><label class="toggle">Show all routes <input type="checkbox" id="routes" checked></label><label class="toggle">Heatmap layer <input type="checkbox" id="heat" checked></label><div class="field"><label>Route definition <output id="definitionValue">78%</output></label><input type="range" id="definition" min="15" max="100" value="78"></div><div class="field"><label>Glow radius <output id="radiusValue">5 px</output></label><input type="range" id="radius" min="2" max="24" value="5"></div><div class="field"><label>Intensity <output id="intensityValue">120%</output></label><input type="range" id="intensity" min="10" max="200" value="120"></div><div class="field"><label>Transparency <output id="opacityValue">84%</output></label><input type="range" id="opacity" min="10" max="100" value="84"></div></section>
<section><h2>Filter activities</h2><div class="field"><label for="sport">Activity type</label><select id="sport"><option value="">All activity types</option></select></div><div class="field"><label for="search">Activity name</label><input type="search" id="search" placeholder="Search activities..."></div><div class="field"><label>Date range</label><div class="row"><input id="dateFrom" type="date" aria-label="Start date"><input id="dateTo" type="date" aria-label="End date"></div><div id="dateRange"></div></div><button class="full subtle" id="clear">Clear filters</button></section>
<section><h2>Activity insights</h2><div class="muted">Explore your habits, totals, streaks, yearly trends, personal records, and data coverage.</div><button class="primary full stats-cta" id="statsButton">Open activity statistics</button></section>
<section><h2>Export</h2><button class="full" id="gpx">Download filtered GPX</button><button class="full" id="png">Download routes-only PNG</button><p class="foot">GPX uses evenly sampled route coordinates. PNG contains the heatmap and route overlay on a solid background; third-party map imagery is not included.</p></section>
<div class="foot">Your GPS tracks and statistics are stored locally. Online map tiles are enabled by default and contact third-party map providers; turning them off keeps your map requests local.</div>
</aside><main id="map"><div id="tiles" class="dark"></div><canvas id="overlay"></canvas><div id="interaction"></div><div class="maptop" id="mapCaption"><span class="map-caption-title">Your GPS heatmap</span><span id="mapDetail">Import a Strava ZIP to begin</span><button id="clearRide" type="button" hidden>Show all activities</button></div><div class="tools" role="group" aria-label="Map zoom controls"><button id="plus" type="button" title="Zoom in" aria-label="Zoom in"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 5v14M5 12h14"/></svg></button><button id="minus" type="button" title="Zoom out" aria-label="Zoom out"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h14"/></svg></button><button id="fit" type="button" title="Focus on main activity area. Shift-click to show every route." aria-label="Fit map to activities"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 3H5a2 2 0 0 0-2 2v3M16 3h3a2 2 0 0 1 2 2v3M3 16v3a2 2 0 0 0 2 2h3M21 16v3a2 2 0 0 1-2 2h-3"/><circle cx="12" cy="12" r="3"/></svg></button></div><div class="legend">LESS TRAVELED · MORE TRAVELED<div class="bar" id="legendBar"></div><span id="legendLabel">Purple → red → orange → white</span><label class="legend-select" for="legendPalette"><span>Heat colors</span><select id="legendPalette" aria-label="Heatmap color scale"><option value="classic">Classic glow</option><option value="ember">Ember</option><option value="violet">Violet</option><option value="ocean">Ocean</option><option value="forest">Forest</option></select></label></div><div class="attribution" id="attribution"><a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noreferrer">© OpenStreetMap contributors</a></div><div class="empty" id="empty"><div class="panel"><strong>Explore everywhere you’ve been</strong><span>Import a Strava backup or GPS track to see your routes on an interactive heatmap. No GIS tools or Python packages required.</span></div></div><div id="busy" class="hidden">Loading map data…</div></main></div><dialog id="statsDialog" aria-labelledby="insightsTitle">
  <div class="insights-shell">
    <div class="insights-topbar">
      <div class="insights-heading"><div class="insights-mark" aria-hidden="true"><img src="/logo.png" alt="" width="46" height="46"></div><div><h2 id="insightsTitle">Activity insights</h2><p>Every journey adds another line to your story</p></div></div>
      <button id="closeStats" type="button" aria-label="Close activity insights"><svg viewBox="0 0 24 24" width="19" height="19" aria-hidden="true"><path d="M5 5 L19 19 M19 5 L5 19" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg><span>Close</span></button>
    </div>
    <div class="insights-layout">
      <nav aria-label="Activity insights sections" class="stats-tabs" role="tablist" aria-orientation="vertical">
        
        <button aria-controls="statsOverview" aria-selected="true" data-stats-tab="overview" id="tabOverview" role="tab" type="button"><span class="insights-nav-symbol" aria-hidden="true"><svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3.5" y="3.5" width="7" height="7" rx="1.5"/><rect x="13.5" y="3.5" width="7" height="7" rx="1.5"/><rect x="3.5" y="13.5" width="7" height="7" rx="1.5"/><rect x="13.5" y="13.5" width="7" height="7" rx="1.5"/></svg></span><span><strong>Overview</strong><small>The big picture</small></span></button>
        <button aria-controls="statsActivities" aria-selected="false" data-stats-tab="activities" id="tabActivities" role="tab" tabindex="-1" type="button"><span class="insights-nav-symbol" aria-hidden="true"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 18c3-1 2-6 6-6s3 5 7 3 2-8 3-9"/><circle cx="4" cy="18" r="1.5"/><circle cx="20" cy="6" r="1.5"/></svg></span><span><strong>Activities</strong><small>Individual routes</small></span></button>
        <div class="insights-nav-privacy"><svg viewBox="0 0 24 24" width="17" height="17" aria-hidden="true"><path d="M12 2 4 6v5c0 5 3.4 8.4 8 11 4.6-2.6 8-6 8-11V6z" fill="none" stroke="currentColor" stroke-width="1.7"/></svg><span>Activity data stored locally</span></div>
      </nav>
      <div class="insights-main" id="insightsMain">
        <div class="insights-intro">
          <div><div class="insights-eyebrow" id="insightsEyebrow">OVERVIEW</div><h3 id="insightsViewTitle">Your activity, at a glance</h3><p id="insightsViewDesc">A clear summary of your GPS history.</p></div>
          <label class="insights-units">Units <select id="units"><option value="mi">Miles</option><option value="km">Kilometers</option></select></label>
        </div>
        <details class="insights-filter-drawer" id="insightsFilters">
          <summary><svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true"><path d="M4 7h16M7 12h10M10 17h4" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/></svg><strong>Filter your activities</strong><span>Sport, bike, year and month</span><span class="filter-caret" aria-hidden="true">⌄</span></summary>
          <div class="insights-filter-grid">
            <label>Sport<select id="statsSport"><option value="">All types</option></select></label>
            <label>Bike<select id="statsBike"><option value="">All bikes</option></select></label>
            <label>Year<select id="statsYear"><option value="">All years</option></select></label>
            <label>Month<select id="statsMonth"><option value="">All months</option></select></label>
          </div>
        </details>
        <div aria-labelledby="tabOverview" class="stats-panel" data-stats-panel="overview" id="statsOverview" role="tabpanel">
          <section class="stats-story-section stats-summary" aria-labelledby="overviewSummaryHeading">
            <div class="stats-story-heading"><div><span class="stats-story-kicker">THE BIG PICTURE</span><h4 id="overviewSummaryHeading">Your activity totals</h4><p>Related metrics together, so every number has context.</p></div></div>
            <div class="insights-hero-metrics" id="metricGrid"></div>
          </section>
          <section class="stats-story-section" aria-labelledby="overviewRecordsHeading">
            <div class="stats-story-heading"><div><span class="stats-story-kicker">PERSONAL BESTS</span><h4 id="overviewRecordsHeading">Your standout moments</h4><p>Peak performances and the routes behind them. Select a highlighted card to preview its activity.</p></div></div>
            <div class="facts" id="facts"></div>
          </section>
          <section class="stats-story-section" aria-labelledby="overviewTrendsHeading">
            <div class="stats-story-heading"><div><span class="stats-story-kicker">OVER TIME</span><h4 id="overviewTrendsHeading">Trends &amp; patterns</h4><p>How your distance and activity have changed over time.</p></div></div>
            <div class="insights-trends-grid"><article class="insights-content-card"><div class="insights-card-title"><div><h4>Distance by year</h4><p>Your annual recorded distance</p></div></div><div class="stat-chart" id="yearChart"></div></article><article class="insights-content-card"><div class="insights-card-title"><div><h4>Monthly activity</h4><p>Number of recorded activities</p></div></div><div class="stat-chart" id="monthChart"></div></article></div>
            <article class="insights-content-card insights-weekday"><div class="insights-card-title"><div><h4>Activity by weekday</h4><p>When you get out most often</p></div></div><div class="weekday-chart" id="weekdayChart"></div></article>
          </section>
          <section class="stats-story-section" aria-labelledby="overviewHistoryHeading">
            <div class="stats-story-heading"><div><span class="stats-story-kicker">YOUR HISTORY</span><h4 id="overviewHistoryHeading">Milestones &amp; habits</h4><p>The days, months and seasons that make up your history.</p></div></div>
            <div class="facts" id="milestones"></div>
          </section>
          <section class="stats-story-section" aria-labelledby="overviewMixHeading">
            <div class="stats-story-heading"><div><span class="stats-story-kicker">ACTIVITY MIX</span><h4 id="overviewMixHeading">Types &amp; gear</h4><p>See how your miles are shared across activities and bikes.</p></div></div>
            <article class="insights-content-card stats-sport-card"><div class="sport-summary" id="sportSummary"></div></article>
          </section>
        </div>
        <div aria-labelledby="tabActivities" class="stats-panel" data-stats-panel="activities" hidden id="statsActivities" role="tabpanel">
          <section class="stats-story-section" aria-labelledby="recentActivityHeading">
            <div class="stats-story-heading"><div><span class="stats-story-kicker">ACTIVITY LIBRARY</span><h4 id="recentActivityHeading">Recently recorded</h4><p>Open a route for its map, moving time, speeds, elevation and gear.</p></div></div>
            <article class="insights-content-card insights-activities-card"><div class="ride-list" id="recentRoutes"></div></article>
          </section>
          <section class="stats-story-section" aria-labelledby="longestActivityHeading">
            <div class="stats-story-heading"><div><span class="stats-story-kicker">DISTANCE HIGHLIGHTS</span><h4 id="longestActivityHeading">Longest routes</h4><p>Your longest recorded routes, ready to explore.</p></div></div>
            <article class="insights-content-card insights-activities-card"><div class="ride-list" id="topRoutes"></div></article>
          </section>
        </div>
        <div class="stats-footer"><div class="details-repair"><div><strong>Want more complete activity information?</strong><span>Import your full Strava account ZIP to add available moving time, elevation, and gear information to matching activities. Existing routes are not duplicated.</span></div><button id="repairDetails" type="button">Add activity details</button></div><details class="stats-accuracy"><summary>About these statistics &amp; data coverage</summary><div class="muted" id="statAccuracy"></div></details></div>
      </div>
    </div>
  </div>
</dialog><dialog id="ridePreviewDialog" class="ride-preview-dialog" aria-labelledby="ridePreviewTitle"><div class="dialog-head"><div><h2 id="ridePreviewTitle">Ride preview</h2><div class="muted" id="ridePreviewSubtitle">Recorded activity</div></div><button id="closeRidePreview" type="button">Close preview</button></div><div class="ride-preview-map"><div id="ridePreviewTiles" class="dark" aria-hidden="true"></div><canvas id="ridePreviewCanvas" role="img" aria-label="GPS activity route over a dark online map when online tiles are enabled"></canvas><span class="ride-preview-badge" id="ridePreviewBadge">Dark map · GPS route</span><a class="ride-preview-attribution" id="ridePreviewAttribution" href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">© OpenStreetMap contributors</a></div><div id="ridePreviewDetails" class="ride-preview-details"></div><div class="ride-preview-actions"><button id="showRideMap" class="primary" type="button" disabled>Show only this activity on map</button><button id="backToStats" type="button">Back to statistics</button></div><div id="ridePreviewStatus" class="muted" role="status" aria-live="polite"></div></dialog><dialog id="welcomeDialog" aria-labelledby="welcomeTitle"><div class="welcome-icon" aria-hidden="true">✦</div><h2 id="welcomeTitle">Welcome to LibreHeatmaps</h2><p>Your own GPS heatmaps and activity history, on your computer. Get started by importing a Strava account ZIP (no extraction needed) or FIT, GPX, or TCX track.</p><div class="welcome-steps"><strong>Have a Strava account?</strong><span>Strava → Settings → My Account → Download My Account → Get Started. Select the entire downloaded ZIP here when it arrives—do not extract it or select the activity files individually.</span></div><p class="welcome-privacy">Your GPS data stays on this device. Online map backgrounds request map tiles from external providers.</p><div class="welcome-actions"><button id="welcomeLater" type="button">Explore first</button><button id="welcomeImport" type="button" class="primary">Import my GPS data</button></div></dialog><dialog id="settingsDialog"><div class="dialog-head"><div class="settings-dialog-brand"><img src="/logo.png" alt="" aria-hidden="true" width="44" height="44"><div><h2>Settings</h2><div class="muted">Customize the map and manage locally stored activity data</div></div></div><button id="closeSettings" type="button" class="settings-close" aria-label="Close settings" title="Close settings">×</button></div><div class="settings-stack"><section class="settings-section"><h2>Appearance</h2><div class="field"><label for="palette">Heat color scale</label><select id="palette"><option value="classic">Purple → red → orange → white</option><option value="ember">Ember · red → orange → gold</option><option value="violet">Violet · purple → magenta → white</option><option value="ocean">Ocean · blue → cyan → white</option><option value="forest">Forest · teal → lime → yellow</option></select><div id="palettePreview" class="palette-preview" role="img" aria-label="Selected heat scale preview"></div><div class="palette-ends"><span>Less traveled</span><span>More traveled</span></div></div><div class="field"><label for="defaultMap">Default map background</label><select id="defaultMap"><option value="dark">Dark</option><option value="light">Light</option><option value="satellite">Satellite</option></select></div><p class="muted settings-note">Changes can be previewed immediately and saved to your local data folder. Close without saving to discard them. Online map tiles can be disabled in Map view at any time.</p><button id="resetAppearance">Restore appearance defaults</button></section><section class="settings-section"><h2>Local data &amp; cache</h2><p class="muted settings-note">LibreHeatmaps stores imported GPS activities locally in a SQLite database. These options never delete your original downloaded files.</p><div class="settings-actions"><div><strong>Refresh rendered map data</strong><small>Discard temporary map calculations and reload the current view. Imported activities remain untouched.</small></div><button id="clearViewCache">Refresh map cache</button></div><div class="settings-actions danger-block"><div><strong>Delete all imported activities</strong><small>Permanently remove imported tracks and statistics from this local LibreHeatmaps database. Reimport the original files to restore them.</small></div><button id="clearActivities" class="danger">Delete local activity data</button></div><div class="local-storage"><strong>Local data location</strong><div class="path-label">Data folder</div><code id="dataFolder">Loading…</code><div class="path-label">SQLite database</div><code id="dataFile">Loading…</code><div class="settings-actions"><div><strong>Open local data folder</strong><small>View your database and diagnostic log in your file manager. Your original exports are never moved or deleted by this control.</small></div><button id="openDataFolder">Open folder</button></div></div><div id="settingsStatus" role="status" aria-live="polite" class="settings-status"></div></section><section class="settings-section"><h2>Software update</h2><p class="muted settings-note">Current version: <strong id="installedVersion">1.0.0</strong>. Updates are checked only when you request them. Downloads are from Gubna-Tech GitHub releases and SHA-256 verified before installation.</p><div class="settings-actions"><div><strong>Check for updates</strong><small>Contact GitHub for the latest published release. Your activity data is not transmitted.</small></div><button id="checkUpdate">Check for updates</button></div><div class="settings-actions"><div><strong>Install a verified release</strong><small>Back up the app files before installing. Your SQLite database is preserved. Restart the app afterward.</small></div><button id="installUpdate" disabled>Install update</button></div><div id="updateStatus" class="settings-status" role="status" aria-live="polite"></div><a id="releaseLink" href="https://github.com/Gubna-Tech" rel="noreferrer noopener" target="_blank" hidden>View release on GitHub</a></section></div><div class="settings-savebar"><div id="settingsSaveState" role="status" aria-live="polite" class="settings-save-state">All changes saved</div><button id="saveSettings" class="primary" disabled>Save changes</button></div></dialog>
<script>
const TOKEN = '__APP_TOKEN__';
const PALETTES={
classic:{colors:['#a16cdf','#df398f','#f34547','#ff8d23','#fff3dd'],name:'Purple → red → orange → white'},
ember:{colors:['#a83161','#d94335','#f07626','#ffc34b','#fff0be'],name:'Red → orange → gold'},
violet:{colors:['#6a56cb','#a04ddb','#e34fae','#f3acd6','#fff5ff'],name:'Purple → magenta → white'},
ocean:{colors:['#354ebd','#367fd8','#31c5d0','#9bf2ff','#f6fcff'],name:'Blue → cyan → white'},
forest:{colors:['#206f7f','#30ab82','#76d373','#d4e86b','#fff7c6'],name:'Teal → lime → yellow'}
};
const $ = id => document.getElementById(id);
const map = $('map'), tiles = $('tiles'), canvas = $('overlay'), ctx = canvas.getContext('2d');
const state = {lat:36.1627,lon:-86.7816,z:12,activeRideId:null,online:true,mode:'dark',bounds:null,data:{step:2,cells:[],activities:0,peak:0},tileNodes:new Map(),generation:0,known:0};
const earth = z => 256*Math.pow(2,z);
const clamp = (v,a,b) => Math.max(a,Math.min(b,v));
const px = (lat,lon,z=state.z) => {const e=earth(z),r=clamp(lat,-85.0511,85.0511)*Math.PI/180;return [(lon+180)/360*e,(1-Math.asinh(Math.tan(r))/Math.PI)/2*e]};
const geo = (x,y,z=state.z) => {const e=earth(z);return [(Math.atan(Math.sinh(Math.PI*(1-2*y/e)))*180/Math.PI), x/e*360-180]};
const dimensions = () => [map.clientWidth,map.clientHeight];
const viewport = () => {const [w,h]=dimensions(),p=px(state.lat,state.lon);return {left:p[0]-w/2,top:p[1]-h/2,w,h}};
const fromCoordinate = (lat,lon,v=viewport()) => {const p=px(lat,lon);return [p[0]-v.left,p[1]-v.top]};
function fitBounds(b,tight=true){if(!b||b.some(x=>x===null))return;const [w,h]=dimensions();if(w<100||h<100){requestAnimationFrame(()=>fitBounds(b,tight));return;}const a=px(b[1],b[0],0),c=px(b[3],b[2],0);const dx=Math.max(1,Math.abs(c[0]-a[0])),dy=Math.max(1,Math.abs(a[1]-c[1]));const zoom=clamp(Math.floor(Math.log2(Math.min(w*.90/dx,h*.90/dy)))+(tight?2:0),tight?12:1,17);state.z=zoom;const center=geo((a[0]+c[0])/2,(a[1]+c[1])/2,0);state.lat=center[0];state.lon=center[1];updateMap(true)}
function tileUrl(z,x,y){if(state.mode==='satellite')return `https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/${z}/${y}/${x}`;return `https://tile.openstreetmap.org/${z}/${x}/${y}.png`}
function tileMap(){
const enabled=state.online;
map.classList.toggle('offline-light',!enabled&&state.mode==='light');map.classList.toggle('offline-dark',!enabled&&state.mode!=='light');
if(!enabled){tiles.replaceChildren();state.tileNodes.clear();$('attribution').textContent='Offline · Local activity routes';return}
const v=viewport(),t=256,used=new Set(),count=Math.pow(2,state.z);
tiles.className=state.mode==='dark'?'dark':'';
for(let y=Math.floor(v.top/t);y<=Math.floor((v.top+v.h)/t);y++){
if(y<0||y>=count)continue;
for(let x=Math.floor(v.left/t);x<=Math.floor((v.left+v.w)/t);x++){
const wrap=((x%count)+count)%count,key=[state.mode,state.z,wrap,y,x].join('-');
used.add(key);let item=state.tileNodes.get(key);
if(!item){item=document.createElement('img');item.alt='';item.decoding='async';item.loading='lazy';item.onerror=()=>{item.style.visibility='hidden'};item.onload=()=>{item.style.visibility='visible'};item.referrerPolicy='strict-origin-when-cross-origin';item.src=tileUrl(state.z,wrap,y);tiles.appendChild(item);state.tileNodes.set(key,item)}
item.style.transform=`translate3d(${x*t-v.left}px,${y*t-v.top}px,0)`
}}
for(const [key,item] of state.tileNodes){if(!used.has(key)){item.remove();state.tileNodes.delete(key)}}
$('attribution').innerHTML=state.mode==='satellite'?'Imagery © <a href="https://www.esri.com/" target="_blank" rel="noopener noreferrer">Esri</a> and imagery providers':'<a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">© OpenStreetMap contributors</a>';
}
let tileFrame=0;function queueTiles(){if(tileFrame)return;tileFrame=requestAnimationFrame(()=>{tileFrame=0;tileMap()})}
const renderSurface=document.createElement('canvas'),renderContext=renderSurface.getContext('2d',{alpha:true});
let renderFrame=0,previousGeometry=null,zoomWaiting=false;
function queueRender(){if(renderFrame)return;renderFrame=requestAnimationFrame(()=>{renderFrame=0;render()})}
// Interpolated heat colors use reusable canvas paths for efficient drawing.
const LH_HEAT_BANDS=32;
function heatColor(stops,t){
 const u=clamp(t,0,1)*(stops.length-1),i=Math.min(stops.length-2,Math.floor(u)),f=u-i;
 const ca=stops[i],cb=stops[i+1];
 const toRgb=x=>[1,3,5].map(j=>parseInt(x.slice(j,j+2),16));
 const a=toRgb(ca),b=toRgb(cb);
 return 'rgb('+a.map((n,j)=>Math.round(n+(b[j]-n)*f)).join(',')+')';
}
function geometry(){
 if(previousGeometry&&previousGeometry.data===state.data)return previousGeometry;
 const step=state.data.step||1,segments=state.data.segments||[];
 const originX=segments.length?segments[0][0]*step:0,originY=segments.length?segments[0][1]*step:0;
 const all=new Path2D(),bands=Array.from({length:LH_HEAT_BANDS},()=>new Path2D());
 const peak=Math.max(1,state.data.peak||0),logPeak=Math.log1p(peak);
 const toLevel=n=>clamp(Math.log1p(Math.max(0,n))/logPeak,0,1);
 for(const [ax,ay,bx,by,visitsStart,visitsEnd=visitsStart] of segments){
  const x=ax*step-originX,y=ay*step-originY,x2=bx*step-originX,y2=by*step-originY;
  if(x===x2&&y===y2)continue;
  all.moveTo(x,y);all.lineTo(x2,y2);
  const t0=toLevel(visitsStart),t1=toLevel(visitsEnd);
  // Interpolate transitions *along* longer lines, so intersections and other
  // density changes never turn an entire GPS segment into one flat color.
  const len=Math.hypot(x2-x,y2-y),changes=Math.abs(t1-t0);
  const pieces=Math.min(24,Math.max(1,Math.ceil(changes*LH_HEAT_BANDS/1.5)));
  for(let j=0;j<pieces;j++){
   const a=j/pieces,b=(j+1)/pieces,mid=(a+b)*.5;
   const level=Math.min(LH_HEAT_BANDS-1,Math.round((t0+(t1-t0)*mid)*(LH_HEAT_BANDS-1)));
   const p=bands[level];p.moveTo(x+(x2-x)*a,y+(y2-y)*a);p.lineTo(x+(x2-x)*b,y+(y2-y)*b);
  }
 }
 previousGeometry={data:state.data,zoom:state.data.z??state.z,originX,originY,all,bands};return previousGeometry;
}
function render(){
 if(pointer!==null)return;
 const w=map.clientWidth,h=map.clientHeight;if(!w||!h)return;
 const dpr=clamp(Math.max(2,window.devicePixelRatio||1),1,2),rw=Math.round(w*dpr),rh=Math.round(h*dpr);
 if(renderSurface.width!==rw||renderSurface.height!==rh){renderSurface.width=rw;renderSurface.height=rh}
 const rc=renderContext;rc.setTransform(dpr,0,0,dpr,0,0);rc.clearRect(0,0,w,h);
 const g=geometry(),v=viewport(),definition=+$('definition').value/100,opacity=+$('opacity').value/100,intensity=+$('intensity').value/100,radius=+$('radius').value;
 const factor=Math.pow(2,state.z-g.zoom);
 rc.translate(g.originX*factor-v.left,g.originY*factor-v.top);
 rc.scale(factor,factor);
 const width=value=>value/factor;
 const colors=PALETTES[$('palette').value].colors;
 const satellite=state.mode==='satellite',light=state.mode==='light';
 rc.lineCap='round';rc.lineJoin='round';
 if($('routes').checked){
  rc.strokeStyle=satellite?'#101523':light?'#241b35':'#130d21';
  rc.globalAlpha=definition*(satellite?.88:.82);rc.lineWidth=width(satellite?4.2:3.8);rc.stroke(g.all);
 }
 if($('heat').checked){
  if(radius>0){for(let band=0;band<LH_HEAT_BANDS;band++){
   rc.strokeStyle=heatColor(colors,band/(LH_HEAT_BANDS-1));
   rc.lineWidth=width(Math.max(2.2,radius*(1.3+(band/(LH_HEAT_BANDS-1))*.36)));
   rc.globalAlpha=Math.min(.23,opacity*intensity*(.045+(band/(LH_HEAT_BANDS-1))*.06));rc.stroke(g.bands[band]);
  }}
  for(let band=0;band<LH_HEAT_BANDS;band++){
   rc.strokeStyle=heatColor(colors,band/(LH_HEAT_BANDS-1));rc.lineWidth=width(1.95);
   rc.globalAlpha=Math.min(1,opacity*Math.max(.82,intensity));rc.stroke(g.bands[band]);
  }
 }else if($('routes').checked){
  rc.strokeStyle=satellite?'#ffdfaf':light?'#643b80':'#e2c7f2';
  rc.lineWidth=width(1.45);rc.globalAlpha=Math.min(1,definition*(satellite?.95:light?.78:.8));rc.stroke(g.all);
 }
 rc.globalAlpha=1;
 if(canvas.width!==rw||canvas.height!==rh){canvas.width=rw;canvas.height=rh}
 ctx.setTransform(1,0,0,1,0,0);ctx.clearRect(0,0,rw,rh);ctx.drawImage(renderSurface,0,0);canvas.style.transform='';canvas.style.opacity='1';
}
function prepareZoom(){
 zoomWaiting=true;
 ++state.generation;clearTimeout(timer);
 if(controller)controller.abort();
 $('busy').textContent='Refining heatmap…';$('busy').classList.remove('hidden');
 queueRender();
}
function updateMap(deferRender=false){if(deferRender)prepareZoom();else zoomWaiting=false;state.lat=clamp(state.lat,-85,85);state.lon=clamp(state.lon,-180,180);tileMap();queueRender();scheduleData(deferRender?35:100)}
let timer=null,controller=null,loadedKey='';function scheduleData(delay=100){clearTimeout(timer);if(controller)controller.abort();++state.generation;timer=setTimeout(loadView,delay)}
function filterParams(){const p=new URLSearchParams();if(state.activeRideId){p.set('activity_id',String(state.activeRideId));return p}for(const [key,id] of [['sport','sport'],['from','dateFrom'],['to','dateTo'],['q','search']]){const val=$(id).value.trim();if(val)p.set(key,val)}return p}
async function getJson(url){const res=await fetch(url,{cache:'no-store'});if(!res.ok)throw new Error((await res.text()).slice(0,300));return await res.json()}
async function loadView(){
if(state.known===0){zoomWaiting=false;canvas.style.opacity='1';$('busy').classList.add('hidden');queueRender();return;}
const v=viewport(),t=256;
const x0=Math.floor((v.left-256)/t)*t,x1=Math.ceil((v.left+v.w+256)/t)*t;
const y0=Math.floor((v.top-256)/t)*t,y1=Math.ceil((v.top+v.h+256)/t)*t;
const nw=geo(x0,y0),se=geo(x1,y1),p=filterParams();
p.set('west',clamp(nw[1],-180,180).toString());p.set('east',clamp(se[1],-180,180).toString());
p.set('north',clamp(nw[0],-85,85).toString());p.set('south',clamp(se[0],-85,85).toString());p.set('z',state.z);
const key=p.toString();if(key===loadedKey){zoomWaiting=false;canvas.style.transform='';canvas.style.opacity='1';$('busy').classList.add('hidden');queueRender();return;}
const id=state.generation;if(controller)controller.abort();controller=new AbortController();$('busy').textContent='Refining heatmap…';$('busy').classList.remove('hidden');
try{const res=await fetch('/api/view?'+key,{signal:controller.signal});if(!res.ok)throw Error('Map request failed: '+res.status);
const data=await res.json();if(id!==state.generation)return;state.data=data;previousGeometry=null;loadedKey=key;zoomWaiting=false;
$('mapDetail').textContent=state.activeRideId?'Selected route · '+data.activities.toLocaleString()+' matching activity':data.activities.toLocaleString()+' matching activities in map area';queueRender()}
catch(e){if(e.name!=='AbortError'){zoomWaiting=false;canvas.style.transform='';canvas.style.opacity='1';$('busy').classList.add('hidden');loadedKey='';queueRender();$('mapDetail').textContent='Map update failed: '+e.message}}
finally{if(id===state.generation)$('busy').classList.add('hidden')}
}
async function refreshStats(zoom=false){loadedKey='';const s=await getJson('/api/stats');if(s.count===0&&s.first_launch&&!$('welcomeDialog').open&&!welcomeHandled){welcomeHandled=true;requestAnimationFrame(()=>{$('welcomeDialog').showModal()})}const previous=state.known;state.known=s.count;state.bounds=s.bounds;state.focusBounds=s.focus_bounds||s.bounds;$('activityCount').textContent=s.count.toLocaleString();$('pointCount').textContent=s.points.toLocaleString();$('empty').classList.toggle('hidden',s.count>0);const current=$('sport').value;$('sport').innerHTML='<option value="">All activity types</option>';for(const [name,n] of s.types){const opt=document.createElement('option');opt.value=name;opt.textContent=name+' ('+n+')';$('sport').appendChild(opt)}$('sport').value=current;$('dateRange').textContent=s.first?'Available: '+s.first+' to '+s.last:'';if(s.count&&(zoom||!previous))fitBounds(state.focusBounds);else scheduleData()}
function chooseMap(mode){state.mode=mode;$('defaultMap').value=mode;document.querySelectorAll('[data-map]').forEach(b=>b.classList.toggle('primary',b.dataset.map===mode));tileMap();queueRender();savePreferences()}
document.querySelectorAll('[data-map]').forEach(b=>b.onclick=()=>chooseMap(b.dataset.map));
function updatePrivacy(){

$('tileNotice').textContent=state.online?'Map tiles come from external providers and may reveal your IP and map area. Track files and statistics remain local.':'Offline mode: no third-party map requests. Your route overlay remains available.';
}
$('onlineTiles').addEventListener('change',()=>{state.online=$('onlineTiles').checked;if($('ridePreviewDialog').open&&previewRide)drawRidePreview(previewRide);state.tileNodes.clear();tiles.replaceChildren();updatePrivacy();tileMap();queueRender();savePreferences()});
updatePrivacy();

let currentInsights=null,statsYears=[],welcomeHandled=false;
function selectStatsTab(target){
if(!['overview','activities'].includes(target))target='overview';
for(const tab of document.querySelectorAll('[data-stats-tab]')){const active=tab.dataset.statsTab===target;tab.setAttribute('aria-selected',String(active));tab.tabIndex=active?0:-1}
for(const panel of document.querySelectorAll('[data-stats-panel]'))panel.hidden=panel.dataset.statsPanel!==target;
const labels={overview:['OVERVIEW','Your activity, at a glance','Totals, personal records and trends—together in one view.'],activities:['ACTIVITIES','Your recorded routes','Browse your recent and longest GPS activities.']};
const item=labels[target];$('insightsEyebrow').textContent=item[0];$('insightsViewTitle').textContent=item[1];$('insightsViewDesc').textContent=item[2];$('insightsMain').scrollTop=0;
}
for(const tab of document.querySelectorAll('[data-stats-tab]')){tab.onclick=()=>selectStatsTab(tab.dataset.statsTab);tab.onkeydown=e=>{if(!['ArrowLeft','ArrowRight','ArrowUp','ArrowDown','Home','End'].includes(e.key))return;e.preventDefault();const tabs=Array.from(document.querySelectorAll('[data-stats-tab]'));let i=tabs.indexOf(tab);i=e.key==='Home'?0:e.key==='End'?tabs.length-1:(i+(['ArrowRight','ArrowDown'].includes(e.key)?1:-1)+tabs.length)%tabs.length;selectStatsTab(tabs[i].dataset.statsTab);tabs[i].focus()}}
async function dismissWelcome(){if(!$('welcomeDialog').open)return;$('welcomeDialog').close();try{await protectedAction('/api/welcome/dismiss')}catch(e){console.warn('Could not save welcome choice:',e.message)}}
$('welcomeLater').onclick=()=>dismissWelcome();
$('welcomeImport').onclick=()=>{dismissWelcome();$('file').click()};
$('welcomeDialog').addEventListener('cancel',e=>{e.preventDefault();dismissWelcome()});
function nice(n,dec=0){return Number(n||0).toLocaleString(undefined,{maximumFractionDigits:dec,minimumFractionDigits:dec})}
function distanceLabel(m){const miles=$('units').value==='mi';return nice(m/(miles?1609.344:1000),1)+(miles?' mi':' km')}
function timeLabel(s){if(s===null||s===undefined||!Number.isFinite(s))return 'Not recorded';return s<3600?Math.round(Math.max(0,s)/60).toLocaleString()+' min':nice(s/3600,1)+' h'}
function statsParams(){const p=new URLSearchParams();for(const [k,id] of [['sport','statsSport'],['bike','statsBike'],['year','statsYear'],['month','statsMonth']]){const v=$(id).value;if(v)p.set(k,v)}return p}
function bars(root,rows,key,asDistance=true){const el=$(root);el.replaceChildren();if(!rows.length){el.textContent='No dated activities in this selection.';return}const top=Math.max(...rows.map(r=>r[key]),1);for(const row of rows){const line=document.createElement('div');line.className='chart-row';const label=document.createElement('span'),track=document.createElement('div'),bar=document.createElement('div'),value=document.createElement('strong');label.textContent=row.year||(row.month?new Date(row.month+'-01T12:00:00').toLocaleDateString(undefined,{month:'short',year:'2-digit'}):'');track.className='chart-track';bar.className='chart-bar';bar.style.width=Math.max(1.5,100*row[key]/top)+'%';track.append(bar);value.textContent=asDistance?distanceLabel(row[key]):nice(row[key]);line.append(label,track,value);el.append(line)}}
function fillStats(s){
currentInsights=s;const grid=$('metricGrid');grid.replaceChildren();
const speedUnit=$('units').value==='mi'?' mph':' km/h';
const peakSpeed=s.peak_speed_mps===null?'Not recorded':nice(s.peak_speed_mps*($('units').value==='mi'?2.236936:3.6),1)+speedUnit;
const metricGroups=[
 {key:'distance',title:'Routes & distance',items:[['Total distance',distanceLabel(s.distance_m)],['Average route',distanceLabel(s.avg_distance_m)],['Activities',nice(s.count)],['GPS tracks',nice(s.mapped_gps_count)]]},
 {key:'time',title:'Time on the move',items:[['Moving time',s.moving_count?timeLabel(s.moving_seconds):'Not recorded'],['Elapsed time',s.elapsed_count?timeLabel(s.elapsed_seconds):'Not recorded'],['Average moving time',s.moving_count?timeLabel(s.moving_seconds/s.moving_count):'Not recorded']]},
 {key:'speed',title:'Speed',items:[['Average moving speed',s.overall_avg_speed_kmh!==null?nice(s.overall_avg_speed_kmh*($('units').value==='mi'?.621371:1),1)+speedUnit:'Not recorded'],['Peak recorded speed',peakSpeed,s.peak_speed?.id]]},
 {key:'rhythm',title:'Elevation & consistency',items:[['Elevation gained',s.elevation_count?nice(s.elevation_m*($('units').value==='mi'?3.28084:1),0)+($('units').value==='mi'?' ft':' m'):'Not recorded'],['Active days',nice(s.active_days)],['Longest streak',nice(s.longest_streak)+' days']]}
];
for(const group of metricGroups){
 const section=document.createElement('section');section.className='stats-metric-group stats-group--'+group.key;
 const heading=document.createElement('h5');heading.className='stats-metric-group-label';heading.textContent=group.title;
 const cards=document.createElement('div');cards.className='stats-metric-cards';
 for(const [label,value,rideId] of group.items){const el=document.createElement(rideId?'button':'div');el.className='metric';if(rideId){el.type='button';el.title='Preview the fastest recorded activity';el.onclick=()=>openRidePreview(rideId)}const strong=document.createElement('strong'),small=document.createElement('span');strong.textContent=value;small.textContent=label;el.append(strong,small);cards.append(el)}
 section.append(heading,cards);grid.append(section);
}
const facts=$('facts'),milestones=$('milestones');facts.replaceChildren();milestones.replaceChildren();
const records=[],history=[];
if(s.peak_speed)records.push(['Highest recorded speed',nice(s.peak_speed.speed_mps*($('units').value==='mi'?2.236936:3.6),1)+speedUnit,s.peak_speed.name,s.peak_speed.id]);
if(s.fastest)records.push(['Fastest average speed',nice(s.fastest.speed_kmh*($('units').value==='mi'?.621371:1),1)+speedUnit,s.fastest.name,s.fastest.id]);
if(s.longest)records.push(['Longest mapped route',distanceLabel(s.longest.distance_m),s.longest.name,s.longest.id]);
if(s.longest_moving)records.push(['Longest moving time',timeLabel(s.longest_moving.moving_seconds),s.longest_moving.name,s.longest_moving.id]);
if(s.max_ascent)records.push(['Greatest elevation gain',nice(s.max_ascent.elevation_m*($('units').value==='mi'?3.28084:1),0)+($('units').value==='mi'?' ft':' m'),s.max_ascent.name,s.max_ascent.id]);
if(s.biggest_distance_month)records.push(['Greatest distance in a month',distanceLabel(s.biggest_distance_month.distance_m),s.biggest_distance_month.month]);
if(s.top_month)history.push(['Most active month',nice(s.top_month.count)+' activities',s.top_month.month]);
if(s.top_year)history.push(['Most active year',nice(s.top_year.count)+' activities',s.top_year.year]);
if(s.favorite_weekday)history.push(['Favorite activity day',s.favorite_weekday,'Most frequently recorded']);
if(s.active_weeks)history.push(['Weeks with activity',nice(s.active_weeks),'Recorded activity weeks']);
if(s.average_weekly_distance_m)history.push(['Distance per active week',distanceLabel(s.average_weekly_distance_m),'Average across active weeks']);
if(s.first&&s.last)history.push(['History range',s.first,s.last]);
if(!records.length)records.push(['No records yet','Import GPS data','Your personal bests will appear here']);
if(!history.length)history.push(['No milestones yet','Import GPS data','Your history will appear here']);
function renderRecordCards(root,items){for(const [label,value,detail,id] of items){
 const el=document.createElement(id?'button':'div');el.className='record-metric metric'+(id?' record-metric-link':'');
 if(id){el.type='button';el.title='Preview '+label.toLowerCase()+' activity';el.onclick=()=>openRidePreview(id)}
 const stat=document.createElement('strong'),caption=document.createElement('span'),foot=document.createElement('small');
 stat.textContent=value;caption.textContent=label;foot.textContent=detail||'';
 el.append(stat,caption,foot);root.append(el)
}}
renderRecordCards(facts,records);renderRecordCards(milestones,history);
renderRideList('topRoutes',s.top_routes);renderRideList('recentRoutes',s.recent_routes);
const sports=$('sportSummary');sports.replaceChildren();if(!s.sports.length)sports.textContent='No matching activities';for(const sport of s.sports){const div=document.createElement('div'),label=document.createElement('span'),value=document.createElement('strong');div.className='sport-row';label.textContent=sport.name+' ('+sport.count+')';value.textContent=distanceLabel(sport.distance_m);div.append(label,value);sports.append(div);for(const bike of s.bikes.filter(b=>b.sport===sport.name)){const line=document.createElement('div'),name=document.createElement('span'),distance=document.createElement('strong');line.className='sport-row bike-row';name.textContent='↳ '+bike.name+' ('+bike.count+')';distance.textContent=distanceLabel(bike.distance_m);line.append(name,distance);sports.append(line)}}
bars('yearChart',s.years,'distance_m');bars('monthChart',s.months.slice(-12),'count',false);
const weekdays=$('weekdayChart');weekdays.replaceChildren();
const highest=Math.max(1,...s.weekdays.map(v=>v.count));
for(const day of s.weekdays){const row=document.createElement('div');row.className='weekday-row';const lbl=document.createElement('span');lbl.textContent=day.name;const track=document.createElement('div');track.className='chart-track';const fill=document.createElement('div');fill.className='chart-bar';fill.style.width=(day.count/highest*100)+'%';track.append(fill);const number=document.createElement('strong');number.textContent=nice(day.count);row.append(lbl,track,number);weekdays.append(row)}
$('statAccuracy').textContent=`GPS coverage: ${s.mapped_gps_count}/${s.count} mapped routes. Strava-supplied distance: ${s.strava_distance_count}/${s.count}. Moving time: ${s.moving_count}/${s.count} (${s.moving_estimated_count} GPS estimates). Elevation: ${s.elevation_count}/${s.count} (${s.elevation_estimated_count} GPS estimates). Peak speed: ${s.peak_speed_count}/${s.count} with recorded Max Speed; this is a single reported maximum, so GPS spikes may affect it. Other distances are derived from sampled GPS tracks. Import your complete Strava export ZIP (including activities.csv and bikes.csv where available), or import activities.csv separately, to fill missing official totals and bike assignments.`;
}
function renderRideList(target,rides){
const root=$(target);root.replaceChildren();if(!rides||!rides.length){root.textContent='No activities in this selection.';return}
for(const ride of rides){const button=document.createElement('button');button.type='button';button.className='ride-list-item';button.title='Open route preview';const name=document.createElement('strong');name.textContent=ride.name||'Untitled activity';const meta=document.createElement('span');meta.textContent=[ride.day,ride.sport,distanceLabel(ride.distance_m)].filter(Boolean).join(' · ');button.append(name,meta);button.onclick=()=>openRidePreview(ride.id);root.append(button)}
}
let previewRide=null;
async function openRidePreview(id){
const dialog=$('ridePreviewDialog');previewRide=null;$('showRideMap').disabled=true;$('ridePreviewTitle').textContent='Loading activity…';$('ridePreviewSubtitle').textContent='Private, local GPS preview';$('ridePreviewDetails').replaceChildren();$('ridePreviewStatus').textContent='Reading the route from your local database…';if(!dialog.open)dialog.showModal();
try{const data=await getJson('/api/activity?id='+encodeURIComponent(id));if(!dialog.open)return;previewRide=data;$('ridePreviewTitle').textContent=data.name||'Untitled activity';$('ridePreviewSubtitle').textContent=[data.day,data.sport].filter(Boolean).join(' · ');$('ridePreviewStatus').textContent=state.online?'Route remains local. Dark map tiles come from OpenStreetMap and reveal the displayed map area and your IP to the provider.':'Online map tiles are disabled. Enable them in Map view to see the dark basemap behind this route.';$('showRideMap').disabled=false;
const facts=[['Distance',distanceLabel(data.distance_m)],['Moving time',timeLabel(data.moving_seconds)],['Elapsed time',timeLabel(data.elapsed_seconds)],['Elevation gain',data.elevation_m===null?'Not recorded':nice(data.elevation_m*($('units').value==='mi'?3.28084:1),0)+($('units').value==='mi'?' ft':' m')],['Gear',data.bike||'Not recorded'],['Average speed',data.avg_speed===null?'Not recorded':nice(data.avg_speed*($('units').value==='mi'?2.236936:3.6),1)+($('units').value==='mi'?' mph':' km/h')],['Heart rate',data.avg_heart_rate===null?'Not recorded':nice(data.avg_heart_rate)+' bpm'],['Maximum speed',data.max_speed===null?'Not recorded':nice(data.max_speed*($('units').value==='mi'?2.236936:3.6),1)+($('units').value==='mi'?' mph':' km/h')]];
for(const [label,value] of facts){const box=document.createElement('div');box.className='ride-preview-detail';const k=document.createElement('span');k.textContent=label;const v=document.createElement('strong');v.textContent=value;box.append(k,v);$('ridePreviewDetails').append(box)}requestAnimationFrame(()=>drawRidePreview(data));
}catch(e){$('ridePreviewTitle').textContent='Could not load this activity';$('ridePreviewStatus').textContent=e.message}
}
function drawRidePreview(data){
const el=$('ridePreviewCanvas'),context=el.getContext('2d'),layer=$('ridePreviewTiles');if(!el||!context)return;
const w=Math.max(240,el.clientWidth),h=Math.max(180,el.clientHeight),dpr=Math.min(2,window.devicePixelRatio||1);
el.width=Math.round(w*dpr);el.height=Math.round(h*dpr);context.setTransform(dpr,0,0,dpr,0,0);context.clearRect(0,0,w,h);
const all=data.points.filter(Boolean);const projected=all.map(p=>px(p[0],p[1],0));
if(projected.length<2){layer.replaceChildren();context.fillStyle='#ece2f8';context.font='15px system-ui';context.textAlign='center';context.fillText('No GPS track available to preview',w/2,h/2);return}
const xs=projected.map(p=>p[0]),ys=projected.map(p=>p[1]);
const minx=Math.min(...xs),maxx=Math.max(...xs),miny=Math.min(...ys),maxy=Math.max(...ys);
const padding=40,worldSpan=Math.max((maxx-minx)/Math.max(1,w-padding*2),(maxy-miny)/Math.max(1,h-padding*2),1e-7);
const zoom=clamp(Math.floor(Math.log2(1/worldSpan)),1,17),worldScale=Math.pow(2,zoom);
const center=[(minx+maxx)/2*worldScale,(miny+maxy)/2*worldScale],left=center[0]-w/2,top=center[1]-h/2;
layer.replaceChildren();const enabled=state.online;$('ridePreviewAttribution').hidden=!enabled;$('ridePreviewBadge').textContent=enabled?'Dark map · GPS route':'Offline · GPS route';
if(enabled){const maxTile=Math.pow(2,zoom);for(let y=Math.floor(top/256);y<=Math.floor((top+h)/256);y++){if(y<0||y>=maxTile)continue;for(let x=Math.floor(left/256);x<=Math.floor((left+w)/256);x++){const wrapped=((x%maxTile)+maxTile)%maxTile;const tile=document.createElement('img');tile.alt='';tile.loading='lazy';tile.decoding='async';tile.onerror=()=>{tile.style.visibility='hidden';$('ridePreviewBadge').textContent='Map tiles unavailable · GPS route';$('ridePreviewStatus').textContent='The map provider could not load its background tiles. Your route is still available locally.'};tile.onload=()=>{tile.style.visibility='visible'};tile.referrerPolicy='strict-origin-when-cross-origin';tile.style.transform=`translate3d(${x*256-left}px,${y*256-top}px,0)`;tile.src=`https://tile.openstreetmap.org/${zoom}/${wrapped}/${y}.png`;layer.append(tile)}}}
const coord=p=>{const world=px(p[0],p[1],0);return [world[0]*worldScale-left,world[1]*worldScale-top]};
context.beginPath();let first=null,last=null;for(const p of data.points){if(p===null){last=null;continue}const xy=coord(p);if(!first)first=xy;if(last)context.lineTo(xy[0],xy[1]);else context.moveTo(xy[0],xy[1]);last=xy}
const grad=context.createLinearGradient(0,0,w,h);grad.addColorStop(0,'#ae82f2');grad.addColorStop(.55,'#f66e9b');grad.addColorStop(1,'#ffb35b');context.lineCap='round';context.lineJoin='round';context.strokeStyle='#090718';context.lineWidth=7;context.stroke();context.strokeStyle=grad;context.lineWidth=3.6;context.shadowColor='#cb83f1';context.shadowBlur=12;context.stroke();context.shadowBlur=0;
if(first){context.beginPath();context.arc(first[0],first[1],5,0,Math.PI*2);context.fillStyle='#d6fbe6';context.fill()}
const endpoint=all.at(-1);if(endpoint){const end=coord(endpoint);context.beginPath();context.arc(end[0],end[1],5,0,Math.PI*2);context.fillStyle='#ffc58b';context.fill()}
}
$('closeRidePreview').onclick=()=>$('ridePreviewDialog').close();$('backToStats').onclick=()=>$('ridePreviewDialog').close();$('ridePreviewDialog').addEventListener('close',()=>{previewRide=null;$('ridePreviewTiles').replaceChildren()});
$('showRideMap').onclick=()=>{if(!previewRide)return;const ride=previewRide;state.activeRideId=ride.id;$('clearRide').hidden=false;$('mapCaption').querySelector('.map-caption-title').textContent=ride.name||'Selected GPS activity';$('ridePreviewDialog').close();if($('statsDialog').open)$('statsDialog').close();loadedKey='';fitBounds(ride.bounds,false)};
$('clearRide').onclick=()=>{state.activeRideId=null;$('clearRide').hidden=true;$('mapCaption').querySelector('.map-caption-title').textContent='Your GPS heatmap';loadedKey='';fitBounds(state.focusBounds||state.bounds)};
window.addEventListener('resize',()=>{if($('ridePreviewDialog').open&&previewRide)drawRidePreview(previewRide)});
async function refreshInsights(){try{const info=await getJson('/api/insights?'+statsParams());fillStats(info)}catch(e){$('statAccuracy').textContent='Unable to load statistics: '+e.message}}
async function refreshStatsFilters(){const s=await getJson('/api/stats');const oldSport=$('statsSport').value,oldYear=$('statsYear').value,oldBike=$('statsBike').value;$('statsSport').replaceChildren();let o=document.createElement('option');o.value='';o.textContent='All types';$('statsSport').append(o);for(const [name] of s.types){let e=document.createElement('option');e.value=name;e.textContent=name;$('statsSport').append(e)}$('statsSport').value=oldSport;$('statsBike').replaceChildren();let allBikes=document.createElement('option');allBikes.value='';allBikes.textContent='All bikes';$('statsBike').append(allBikes);for(const name of s.bikes||[]){const e=document.createElement('option');e.value=name;e.textContent=name;$('statsBike').append(e)}$('statsBike').value=oldBike;const overview=await getJson('/api/insights');$('statsYear').replaceChildren();o=document.createElement('option');o.value='';o.textContent='All years';$('statsYear').append(o);for(const y of overview.years){const e=document.createElement('option');e.value=y.year;e.textContent=y.year;$('statsYear').append(e)}$('statsYear').value=oldYear;statsYears=overview.months;updateStatsMonths()}
function updateStatsMonths(){const old=$('statsMonth').value,year=$('statsYear').value;$('statsMonth').replaceChildren();const blank=document.createElement('option');blank.value='';blank.textContent='All months';$('statsMonth').append(blank);for(const m of statsYears.filter(m=>!year||m.month.startsWith(year))){const e=document.createElement('option');e.value=m.month;e.textContent=m.month;$('statsMonth').append(e)}$('statsMonth').value=old;if($('statsMonth').value!==old)$('statsMonth').value=''}
$('statsTop').onclick=()=>{$('statsButton').click()};$('statsButton').onclick=async()=>{selectStatsTab('overview');$('statsDialog').showModal();try{await refreshStatsFilters()}catch(e){$('statAccuracy').textContent=e.message}refreshInsights()};
$('closeStats').onclick=()=>$('statsDialog').close();$('units').onchange=()=>{if(currentInsights)fillStats(currentInsights)};
$('statsSport').onchange=refreshInsights;$('statsBike').onchange=refreshInsights;$('statsMonth').onchange=refreshInsights;$('statsYear').onchange=()=>{updateStatsMonths();refreshInsights()};
let sliderTimer=0;
for(const id of ['radius','intensity','opacity','definition']){$(id).addEventListener('input',()=>{$(id+'Value').textContent=$(id).value+(id==='radius'?' px':'%');clearTimeout(sliderTimer);sliderTimer=setTimeout(queueRender,85);savePreferences()})}
for(const id of ['routes','heat'])$(id).addEventListener('change',()=>{queueRender();savePreferences()});
$('palette').addEventListener('change',()=>{paintPalette();savePreferences()});
$('legendPalette').addEventListener('change',()=>{$('palette').value=$('legendPalette').value;paintPalette();savePreferences()});
for(const id of ['sport','dateFrom','dateTo','search'])$(id).addEventListener('input',()=>scheduleData());
let statsTimer=0;for(const id of ['sport','dateFrom','dateTo','search'])$(id).addEventListener('input',()=>{if($('statsDialog').open){clearTimeout(statsTimer);statsTimer=setTimeout(refreshInsights,250)}});$('clear').onclick=()=>{for(const id of ['sport','dateFrom','dateTo','search'])$(id).value='';scheduleData();if($('statsDialog').open)refreshInsights()};
let pointer=null;const interactive=$('interaction');
interactive.addEventListener('pointerdown',e=>{if(e.button!==0)return;pointer={x:e.clientX,y:e.clientY,lat:state.lat,lon:state.lon};interactive.setPointerCapture(e.pointerId)});
interactive.addEventListener('pointermove',e=>{if(!pointer)return;const [x,y]=px(pointer.lat,pointer.lon);const dx=e.clientX-pointer.x,dy=e.clientY-pointer.y;const [lat,lon]=geo(x-dx,y-dy);state.lat=lat;state.lon=lon;queueTiles();canvas.style.transform=`translate3d(${dx}px,${dy}px,0)`});
function finishPan(){if(!pointer)return;pointer=null;updateMap()}
interactive.addEventListener('pointerup',finishPan);interactive.addEventListener('pointercancel',finishPan);
let wheelTimer=0,wheelBase=null;interactive.addEventListener('wheel',e=>{e.preventDefault();if(wheelBase===null)wheelBase=state.z;const next=clamp(state.z+(e.deltaY<0?1:-1),1,18);if(next===state.z)return;state.z=next;prepareZoom();queueTiles();clearTimeout(wheelTimer);wheelTimer=setTimeout(()=>{wheelBase=null;updateMap(true)},95)},{passive:false});
interactive.addEventListener('dblclick',e=>{const old=state.z;state.z=clamp(old+1,1,18);if(state.z!==old)updateMap(true)});
function zoomButton(delta){const old=state.z;state.z=clamp(old+delta,1,18);if(state.z!==old)updateMap(true)}
$('plus').onclick=()=>zoomButton(1);$('minus').onclick=()=>zoomButton(-1);$('fit').onclick=e=>fitBounds(e.shiftKey?state.bounds:state.focusBounds||state.bounds,!e.shiftKey);
let restoringDetails=false;$('import').onclick=()=>{restoringDetails=false;$('file').click()};$('repairDetails').onclick=()=>{restoringDetails=true;if($('statsDialog').open)$('statsDialog').close();$('file').click()};$('file').addEventListener('change',()=>{if($('file').files.length)upload($('file').files[0])});
function upload(file){$('import').disabled=true;const status=$('importStatus');status.classList.remove('error');status.textContent='Uploading '+file.name+' to the local importer…';const xhr=new XMLHttpRequest();xhr.open('POST','/api/import');xhr.setRequestHeader('X-LibreHeatmaps-Token',TOKEN);xhr.setRequestHeader('X-File-Name',encodeURIComponent(file.name));xhr.upload.onprogress=e=>{if(e.lengthComputable)status.textContent='Copying ZIP locally: '+Math.floor(e.loaded/e.total*100)+'%'};xhr.onload=()=>{if(xhr.status===202){pollImport()}else{status.classList.add('error');status.textContent='Import could not start: '+xhr.responseText;$('import').disabled=false}};xhr.onerror=()=>{status.classList.add('error');status.textContent='Could not transfer activity to local app.';$('import').disabled=false};xhr.send(file)}
async function pollImport(){try{const job=await getJson('/api/job');$('importStatus').textContent=job.status==='running'?`Processing ${job.processed} / ${job.total}: ${job.message}
Added ${job.added} · Already imported ${job.skipped} · No GPS ${job.no_gps||0} · Metadata updated ${job.updated||0} · Failed ${job.failed}`:`${job.message}
Added ${job.added} · Already imported ${job.skipped} · No GPS ${job.no_gps||0} · Metadata updated ${job.updated||0} · Failed ${job.failed}`;if(job.status==='running'){setTimeout(pollImport,800);return}$('import').disabled=false;$('file').value='';if(job.status==='error')$('importStatus').classList.add('error');await refreshStats(true);if(restoringDetails&&job.status==='done'){restoringDetails=false;$('statsButton').click()}else if($('statsDialog').open){await refreshStatsFilters();await refreshInsights()}}catch(e){$('importStatus').textContent=e.message;$('import').disabled=false}}
$('gpx').onclick=()=>{if(!state.known)return alert('Import activities first.');window.location.href='/api/export.gpx?'+filterParams()};$('png').onclick=()=>{if(!state.known)return alert('Import activities first.');if(zoomWaiting){alert('Map is still updating. Try again when the heatmap has finished rendering.');return;}if(canvas.style.transform)render();const x=document.createElement('canvas'),w=canvas.width,h=canvas.height;x.width=w;x.height=h;const c=x.getContext('2d');c.fillStyle=state.mode==='light'?'#e7e9eb':'#100b1c';c.fillRect(0,0,w,h);c.drawImage(canvas,0,0);x.toBlob(blob=>{if(!blob)return;const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='LibreHeatmaps.png';a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000)},'image/png')};

const CLIENT_ID=Math.random().toString(36).slice(2)+Date.now().toString(36);
function ping(){fetch('/api/ping',{method:'POST',headers:{'X-LibreHeatmaps-Token':TOKEN,'Content-Type':'text/plain'},body:CLIENT_ID,cache:'no-store'}).catch(()=>{})}
ping();setInterval(ping,12000);
window.addEventListener('pagehide',()=>fetch('/api/leave',{method:'POST',headers:{'X-LibreHeatmaps-Token':TOKEN,'Content-Type':'text/plain'},body:CLIENT_ID,keepalive:true}).catch(()=>{}));
$('shutdown').onclick=async()=>{if(!confirm('Close the local LibreHeatmaps application?'))return;try{await fetch('/api/shutdown',{method:'POST',headers:{'X-LibreHeatmaps-Token':TOKEN}})}catch(e){}document.body.replaceChildren();const d=document.createElement('div');d.style.cssText='font:16px system-ui;color:#ddd;background:#160d27;min-height:100vh;display:grid;place-items:center';d.textContent='LibreHeatmaps has stopped. You can close this tab.';document.body.append(d)};
let savedSettings=null,settingsDirty=false,settingsSaving=false,settingsSaveError=null,prefTimer=null,prefSequence=Promise.resolve(),appearanceApplying=false;
function captureAppearance(){return {mode:state.mode,online:state.online,palette:$('palette').value,routes:$('routes').checked,heat:$('heat').checked,radius:Number($('radius').value),definition:Number($('definition').value),intensity:Number($('intensity').value),opacity:Number($('opacity').value)}}
function paintPalette(){$('legendPalette').value=$('palette').value;const p=PALETTES[$('palette').value];$('legendBar').style.background='linear-gradient(90deg,'+p.colors.join(',')+')';$('palettePreview').style.background=$('legendBar').style.background;$('legendLabel').textContent=p.name;queueRender()}
function applyAppearance(p){if(!p)return;appearanceApplying=true;try{if(PALETTES[p.palette])$('palette').value=p.palette;state.mode=['dark','light','satellite'].includes(p.mode)?p.mode:'dark';if(typeof p.online==='boolean')state.online=p.online;for(const id of ['routes','heat'])if(typeof p[id]==='boolean')$(id).checked=p[id];for(const id of ['radius','definition','intensity','opacity'])if(p[id]!==undefined&&Number.isFinite(Number(p[id]))){const e=$(id);e.value=clamp(Number(p[id]),Number(e.min),Number(e.max));$(id+'Value').textContent=e.value+(id==='radius'?' px':'%')}$('onlineTiles').checked=state.online;$('defaultMap').value=state.mode;document.querySelectorAll('[data-map]').forEach(b=>b.classList.toggle('primary',b.dataset.map===state.mode));state.tileNodes.clear();tiles.replaceChildren();updatePrivacy();tileMap();paintPalette()}finally{appearanceApplying=false}}
async function writeAppearance(p){const response=await fetch('/api/preferences',{method:'POST',headers:{'X-LibreHeatmaps-Token':TOKEN,'Content-Type':'application/json'},body:JSON.stringify(p),cache:'no-store'});const data=await response.json();if(!response.ok)throw Error(data.error||'Could not save settings');return data.appearance}
function enqueueAppearance(p){prefSequence=prefSequence.catch(()=>{}).then(()=>writeAppearance(p));return prefSequence}
function savePreferences(){if(appearanceApplying||$('settingsDialog').open)return;clearTimeout(prefTimer);prefTimer=setTimeout(()=>{enqueueAppearance(captureAppearance()).catch(e=>console.warn('Local preference save failed:',e.message))},220)}
async function loadPreferences(){let p=null;try{p=(await getJson('/api/preferences')).appearance}catch(e){console.warn('Local preferences unavailable:',e.message)}applyAppearance(p||{mode:'dark',online:true,palette:'classic',routes:true,heat:true,radius:5,definition:78,intensity:120,opacity:84})}
function updateSaveState(){const indicator=$('settingsSaveState'),button=$('saveSettings');indicator.classList.toggle('dirty',settingsDirty);indicator.classList.toggle('error',!!settingsSaveError);indicator.textContent=settingsSaving?'Saving…':settingsSaveError|| (settingsDirty?'Unsaved changes':'All changes saved');button.disabled=!settingsDirty||settingsSaving}
function markSettingsDirty(){if(!$('settingsDialog').open||appearanceApplying)return;settingsSaveError=null;settingsDirty=JSON.stringify(captureAppearance())!==JSON.stringify(savedSettings);updateSaveState()}
function closeSettings(){if(settingsSaving)return;if(settingsDirty){if(!confirm('Discard unsaved appearance changes?'))return;applyAppearance(savedSettings);settingsDirty=false;updateSaveState()}$('settingsDialog').close()}
$('openSettings').onclick=async()=>{if(prefTimer){clearTimeout(prefTimer);prefTimer=null;try{await enqueueAppearance(captureAppearance())}catch(e){console.warn('Preference save failed:',e.message)}}try{await prefSequence}catch(e){}savedSettings=captureAppearance();settingsDirty=false;settingsSaving=false;settingsSaveError=null;updateSaveState();$('settingsDialog').showModal();try{const d=await getJson('/api/system');$('dataFolder').textContent=d.data_directory;$('dataFile').textContent=d.database;$('installedVersion').textContent=d.version}catch(e){showSettingsStatus('Could not read local storage path: '+e.message,true)}};
$('closeSettings').onclick=closeSettings;
$('settingsDialog').addEventListener('cancel',e=>{e.preventDefault();closeSettings()});
$('defaultMap').onchange=()=>{chooseMap($('defaultMap').value);markSettingsDirty()};
$('palette').addEventListener('change',markSettingsDirty);
$('saveSettings').onclick=async()=>{if(!settingsDirty||settingsSaving)return;settingsSaveError=null;settingsSaving=true;updateSaveState();try{clearTimeout(prefTimer);const p=captureAppearance();savedSettings=await enqueueAppearance(p);settingsDirty=false;showSettingsStatus('Appearance preferences saved locally.')}catch(e){settingsSaveError='Save failed. Retry to save changes.';showSettingsStatus('Could not save appearance: '+e.message,true)}finally{settingsSaving=false;updateSaveState()}};
$('resetAppearance').onclick=()=>{applyAppearance({mode:'dark',online:true,palette:'classic',routes:true,heat:true,radius:5,definition:78,intensity:120,opacity:84});markSettingsDirty();showSettingsStatus('Defaults previewed. Select Save changes to keep them.')};
async function protectedAction(endpoint){const response=await fetch(endpoint,{method:'POST',headers:{'X-LibreHeatmaps-Token':TOKEN},cache:'no-store'});let data={};try{data=await response.json()}catch(e){}if(!response.ok)throw Error(data.error||('Request failed: '+response.status));return data}
function showSettingsStatus(message,error=false){$('settingsStatus').textContent=message;$('settingsStatus').classList.toggle('error',error)}
$('clearViewCache').onclick=async()=>{const button=$('clearViewCache');button.disabled=true;try{await protectedAction('/api/cache/clear-view');loadedKey='';previousGeometry=null;++state.generation;showSettingsStatus('Temporary map calculations cleared. Reloading the view.');scheduleData()}catch(e){showSettingsStatus(e.message,true)}finally{button.disabled=false}};
$('clearActivities').onclick=async()=>{if(!state.known){showSettingsStatus('There are no imported activities to delete.');return}if(prompt('This permanently deletes all imported activity tracks and statistics in LibreHeatmaps. Your original files will not be changed. Type DELETE to confirm:')!=='DELETE')return;const button=$('clearActivities');button.disabled=true;try{const result=await protectedAction('/api/cache/clear-activities');loadedKey='';++state.generation;if(controller)controller.abort();state.data={step:2,cells:[],activities:0,peak:0};previousGeometry=null;for(const id of ['sport','dateFrom','dateTo','search','statsSport','statsBike','statsYear','statsMonth'])$(id).value='';state.bounds=null;state.focusBounds=null;state.known=0;queueRender();await refreshStats(false);if($('statsDialog').open){await refreshStatsFilters();await refreshInsights()}showSettingsStatus('Deleted '+result.deleted.toLocaleString()+' imported activities. Original downloaded files are unchanged.')}catch(e){showSettingsStatus(e.message,true)}finally{button.disabled=false}};
$('openDataFolder').onclick=async()=>{try{await protectedAction('/api/system/open-folder');showSettingsStatus('Opened your local data folder.')}catch(e){showSettingsStatus(e.message,true)}};
$('checkUpdate').onclick=async()=>{const b=$('checkUpdate');b.disabled=true;$('installUpdate').disabled=true;$('releaseLink').hidden=true;$('updateStatus').textContent='Checking GitHub releases…';try{const d=await protectedAction('/api/update/check');$('updateStatus').textContent='Installed '+d.installed+' · Latest '+d.latest+'. '+d.message;$('installUpdate').disabled=!d.installable;$('releaseLink').href=d.release_url;$('releaseLink').hidden=false}catch(e){$('updateStatus').textContent='Update check unavailable: '+e.message}finally{b.disabled=false}};
$('installUpdate').onclick=async()=>{if(!confirm('Install the verified LibreHeatmaps release? This backs up the existing application files and preserves imported data. Close and relaunch the app after installation.'))return;const b=$('installUpdate');b.disabled=true;$('updateStatus').textContent='Downloading and verifying release…';try{const d=await protectedAction('/api/update/install');$('updateStatus').textContent=d.message}catch(e){$('updateStatus').textContent='Update not installed: '+e.message}};
window.addEventListener('resize',()=>updateMap());loadPreferences().then(()=>refreshStats(true)).then(()=>tileMap()).catch(e=>{$('mapDetail').textContent=e.message});updatePrivacy();
// Contextual help for all parts of the local viewer. Help buttons never invoke the underlying action.
const LH_HELP={
 'LibreHeatmaps':'LibreHeatmaps is a local-first GPS heatmap viewer. Import activities from a Strava ZIP, FIT, GPX, or TCX file to explore routes and trends without uploading your activity database. Optional online map tiles contact the selected provider.',
 'Import activities':'Choose a complete Strava account export ZIP without extracting it, or a CSV, FIT, GPX, or TCX file. Imports are processed locally. Reimporting matching activities updates available details rather than adding duplicate routes.',
 'Activities':'Number of imported activities in the local database. This can include activities with limited metadata; filtering changes the map but does not delete records.',
 'Route points':'Count of sampled GPS coordinates held in your activity library. More points can provide finer route detail but can require additional processing when generating a heatmap.',
 'Map view':'Choose the basemap, whether routes and visit-density glow appear, and how sharply and brightly paths are drawn. Only background map tiles use an external map provider when online tiles are enabled.',
 'Online map tiles':'Load background map tiles from a third-party provider. The provider can see your IP address and the geographical area requested; your activity files and statistics are not uploaded. Turn this off for offline viewing.',
 'Dark':'Use a high-contrast dark background style for the online street map. The underlying GPS data and heat colors are unchanged.',
 'Light':'Use the light street-map style. This changes only the background; route data remains local.',
 'Satellite':'Request aerial/satellite imagery from an external map provider while online tiles are enabled. Requests expose the approximate map area and your IP, not your activity files.',
 'Show all routes':'Draw the base route lines for matching activities. Turn off to hide that route layer without losing imported GPS tracks.',
 'Heatmap layer':'Show or hide heat-colored visit density on your GPS routes. The heatmap colors represent relative frequency of visits within the current view and filters.',
 'Route definition':'Adjust visibility and strength of the route-line detail. Higher percentages make the original GPS paths more pronounced; lower values reduce their visual emphasis.',
 'Glow radius':'Adjust the soft halo around route strokes, measured in screen pixels. Larger radii create wider glow and may increase canvas draw cost, especially in dense areas.',
 'Intensity':'Change the brightness of the heat glow. 120% is the fresh-install default; higher values brighten the effect without changing visit counts.',
 'Transparency':'Control overall opacity of drawn GPS routes and heatmap relative to the background map. Higher percentages make the overlay more opaque.',
 'Filter activities':'Limit which activities contribute to the map by type, name, or date. Clearing filters restores the complete set; your library is never modified by filtering.',
 'Activity type':'Choose a recorded activity type to restrict the visible map routes. All activity types combines your imported activities.',
 'Activity name':'Find routes whose stored activity names match a search. Search affects the visible routes, not the underlying database.',
 'Date range':'Choose inclusive starting and ending dates for activities that appear on the map. Blank dates leave that bound unrestricted.',
 'Start date':'Show only map activities on or after this recorded date. Leave blank for no start-date restriction.',
 'End date':'Show only map activities on or before this recorded date. Leave blank for no end-date restriction.',
 'Activity insights':'Open the local activity dashboard to explore totals, performance highlights, time trends, and individual GPS routes.',
 'Export':'Save the selected GPS data to your computer. GPX exports track coordinates; PNG exports rendered route overlays without the third-party background tiles.',
 'Your GPS heatmap':'The map combines imported routes and color-coded visit intensity. The number of matching activities depends on the current map area and filters; zooming changes the viewed area, not your stored routes.',
 'Heatmap legend':'The color scale goes from lower to higher relative visit frequency in the currently selected view. White or the lightest color indicates the most frequented segments for that view, not a fixed number of visits.',
 'Heat colors':'Choose the color palette used to represent less- and more-traveled GPS routes. This affects presentation only, not recorded visits or distance.',
 'Zoom in':'Zoom into a smaller geographical area to examine individual roads and routes. The density view is recomputed for that area.',
 'Zoom out':'Zoom out to show a larger area. Nearby routes can visually overlap at lower zoom levels.',
 'Fit map to activities':'Center and zoom on the main activity area. Hold Shift while using this button to fit every imported route, including distant journeys.',
 'Overview':'The full summary of your selected local activities, including distance, time, speeds, personal records, trends, habits, and activity breakdowns.',
 'Your activity totals':'Totals for the currently selected Stats filters, organized by distance, time, speed, and consistency. Missing source fields are reported as not recorded instead of assumed to be zero.',
 'Routes & distance':'Distance-based measures from your selected library: total traveled, average activity length, number of activity records, and GPS-mapped tracks.',
 'Time on the move':'Moving time captures time in motion; elapsed time includes stopped or paused periods as well. Comparing them can show how much of your outings was spent stopped. Missing durations are excluded rather than estimated as zero.',
 'Speed':'Average moving speed describes the overall distance divided by known moving time, when every selected activity has time data. Peak speed comes from a recorded per-activity maximum and can include GPS spikes.',
 'Elevation & consistency':'Elevation gained adds recorded positive climb. Active days and streaks summarize calendar dates of recorded activities; they do not measure fitness or daily duration.',
 'Total distance':'Total distance across matching activities, using imported Strava distance where available and GPS-based measurements otherwise. Display units follow the Units selector.',
 'Average route':'Total recorded distance divided by the number of matching activities. This average may include activities with little or no mapped distance.',
 'GPS tracks':'Number of selected activities with mapped GPS routes available for drawing. Statistics may still contain source metadata beyond the route geometry.',
 'Moving time':'Sum of time spent moving across activities with usable duration data. Pauses and stops are excluded when the recording source can identify them; estimated durations may differ from your device or Strava.',
 'Elapsed time':'Total time from recorded start to finish, including stops and breaks, across activities with available elapsed-time data. The difference from moving time helps put riding or running time in context.',
 'Average moving time':'The typical moving duration of an activity in your selection: combined known moving time divided by activities with usable moving-time data. Activities missing this field are excluded, which may affect how representative the average is.',
 'Average moving speed':'For selections with complete moving-time coverage, total selected distance divided by total moving time. This is not the arithmetic mean of each ride’s speed. Shown in mph or km/h.',
 'Peak recorded speed':'Highest reported instantaneous maximum speed among matching activities. It can be affected by GPS spikes; select the card to inspect the corresponding activity.',
 'Elevation gained':'Sum of reported climb for activities with elevation information. Some imports may use GPS estimates; missing elevation data is excluded.',
 'Active days':'Number of distinct calendar dates with at least one matching activity. Several activities recorded on one day count as one active day.',
 'Longest streak':'Longest sequence of consecutive recorded activity days in the filtered selection. Days without a matching activity break the streak.',
 'Your standout moments':'Personal-best activity records for the current selection. Cards with an associated activity can open its route preview; records depend on available source metadata.',
 'Highest recorded speed':'The single highest recorded maximum speed. It is not an average and may be unusually high because of a GPS or sensor spike.',
 'Fastest average speed':'The activity with the greatest average moving speed among activities with valid speed data. Select it to inspect the source activity.',
 'Longest mapped route':'The selected activity with the greatest available route distance. Open the card to view its individual GPS path.',
 'Longest moving time':'The single activity with the greatest recorded time actually in motion. It may differ from the outing with the longest elapsed time if there were extended breaks.',
 'Greatest elevation gain':'The activity with the most reported or estimated accumulated climbing. The figure is ascent, not net elevation change.',
 'Greatest distance in a month':'Calendar month containing the most total recorded distance in the current Stats selection.',
 'Trends & patterns':'Long-term charts calculated from dates and distances in the selected library. Filters in Stats affect these comparisons.',
 'Distance by year':'Total imported activity distance grouped by calendar year. Years with no matching recorded activities may not appear.',
 'Monthly activity':'Number of selected activities recorded in each displayed month. The bar length is relative to the largest displayed month.',
 'Activity by weekday':'Counts of activities grouped by their recorded weekday. A day can contribute multiple activities; bars compare relative frequency.',
 'Milestones & habits':'Calendar-based summaries of your recorded history. These describe import coverage and repeated activity, not any training target.',
 'Most active month':'The calendar month with the most matching recorded activities.',
 'Most active year':'The calendar year containing the largest number of matching recorded activities.',
 'Favorite activity day':'The weekday with the highest number of recorded activities across the selected history.',
 'Weeks with activity':'Number of distinct calendar weeks with at least one matching activity.',
 'Distance per active week':'Total selected distance divided by the number of weeks in which at least one activity was recorded. This is not distance averaged across inactive weeks.',
 'History range':'Earliest to latest recorded activity dates in the selected library; missing dates can limit the range shown.',
 'Types & gear':'Activity types and assigned gear from imported metadata. The displayed distances can be limited by missing Strava CSV fields or unidentified equipment.',
 'Recently recorded':'Most recent imported activities by recorded date. Choose an entry to preview its route and available statistics without leaving the dashboard.',
 'Longest routes':'A ranked list of selected activities by recorded route distance. Choose one to inspect the GPS track and detailed statistics.',
 'Units':'Select miles/mph/feet or kilometers/km/h/meters for displayed figures. The underlying stored measurements do not change.',
 'Filter your activities':'Limit the Stats dashboard to a sport, assigned bike, calendar year, and month. These filters are independent of the main map’s activity filters.',
 'Sport':'Choose one activity type for the Stats dashboard. “All types” includes every type available in your library.',
 'Bike':'Filter Stats by a recorded bike assignment from imported Strava data. Activities with no gear assignment may not appear under a named bike.',
 'Year':'Restrict Stats to activities recorded in one calendar year; “All years” removes the year restriction.',
 'Month':'Restrict Stats to a recorded month. Available month choices follow the selected year.',
 'About these statistics & data coverage':'Review how many routes contain official distance, moving time, elevation, and maximum-speed information. GPS-derived estimates may differ from Strava or device readings.',
 'Add activity details':'Import your complete Strava account ZIP to enrich matching activities with available moving time, elevation and gear information, without duplicating their GPS routes.',
 'Activity preview':'A single imported activity with its locally drawn route, date, sport, and supported stats. Online background tiles may contact OpenStreetMap when enabled.',
 'Activity route map':'The colored line shows the individual GPS track. Dark background tiles come from a map provider when enabled, but the GPS coordinates stay on this device.',
 'Distance':'Length of this activity from imported distance metadata when available or sampled GPS points otherwise.',
 'Elevation gain':'Positive climbing during this activity, from recorded source information or an available GPS estimate.',
 'Gear':'Bike or equipment name attached to this activity, if included in the original imported metadata.',
 'Average speed':'Distance divided by recorded moving time. This averages speed while moving rather than over the entire outing; long breaks can make elapsed-time pace quite different.',
 'Heart rate':'Average recorded heart rate in beats per minute, if it was included in the imported activity metadata.',
 'Maximum speed':'Highest reported instantaneous speed for this activity. It can be affected by short-lived GPS spikes.',
 'Settings':'Change map appearance, review local storage and map cache, or check for a verified software update. Appearance changes can be previewed before saving.',
 'Appearance':'Choose the color palette and default basemap. You can restore appearance defaults without altering GPS activities.',
 'Heat color scale':'Pick the low-to-high visit color progression. All palettes represent the same underlying visit density; no activity data changes.',
 'Default map background':'Choose the street or imagery basemap used when opening the app. Online backgrounds contact external tile providers; offline mode can disable those requests.',
 'Restore appearance defaults':'Revert presentation settings to their built-in defaults, including the classic heat palette and the default map styling. It does not erase activities.',
 'Local data & cache':'Manage the locally stored SQLite activity database and temporary map-render cache. Original FIT, GPX, TCX and Strava ZIP files are not deleted by these controls.',
 'Refresh rendered map data':'Clear the temporary calculated map views and redraw the current area. This does not remove or alter imported activities.',
 'Delete all imported activities':'Permanently clear all imported activities and associated statistics from the LibreHeatmaps local database. Your original export files remain wherever you saved them; reimport to restore the data.',
 'Local data location':'The computer folders containing your local SQLite database and related application files. These paths are shown so you can back up your own data.',
 'Open local data folder':'Open the app’s storage folder in the operating system file manager. This exposes your local database and log without sending them online.',
 'Software update':'Updates are checked manually against the project’s published GitHub releases and verified with SHA-256 before installation. Your activity database is kept separately.',
 'Check for updates':'Contact the GitHub release endpoint to see whether a newer version exists. No GPS activity files or database records are uploaded.',
 'Install a verified release':'Install a downloaded release only after its SHA-256 matches the published digest. App files are backed up, and your local activity database is preserved.',
 'Save changes':'Save the previewed appearance preferences locally so they persist after restarting LibreHeatmaps.',
 'Explore everywhere you’ve been':'The map is empty until an activity with GPS coordinates is imported. A complete Strava export ZIP can be selected directly; no manual extraction is needed.',
 'Welcome to LibreHeatmaps':'First-launch guide to importing your GPS history. You can import a complete Strava ZIP or individual FIT, GPX, or TCX files; everything is processed on your computer.',
 'Have a Strava account?':'Strava can export a ZIP of your account history. Download it from your Strava account settings and select the whole ZIP without unzipping it.',
 'Export GPX':'Download the currently filtered activity tracks as a GPX file. This is an on-device export; it does not include the third-party basemap.',
 'Export PNG':'Download the visible local GPS route and heatmap drawing as an image without online map tiles.',
 'Show only this activity on map':'Close this preview and focus the main heatmap on only this selected activity, without removing any other activities from storage.',
 'Back to statistics':'Close the individual route preview and return to the Statistics dashboard.',
 'Explore first':'Close the introduction and explore the app without importing an activity now. You can import data later.',
 'Import my GPS data':'Open a local file chooser to import a Strava account ZIP or individual activity tracks. Your files are processed locally.',
 'Loading map data':'Calculating the currently visible heatmap. The existing route overlay remains geographically aligned while updated visit-density colors are prepared.'
};
const LH_HELP_ALIASES={
 'Import Strava ZIP, activity, or CSV':'Import activities',
 'Open activity statistics':'Activity insights','Activity statistics':'Activity insights',
 'Download filtered GPX':'Export GPX','Download routes-only PNG':'Export PNG',
 'Clear filters':'Filter activities','Show all activities':'Your GPS heatmap',
 'Close preview':'Activity preview','Close':'Activity insights','Close settings':'Settings',
 'Show all activities':'Your GPS heatmap',
 'Heatmap color scale':'Heat colors','Less traveled':'Heatmap legend',
 'Refresh map cache':'Refresh rendered map data','Delete local activity data':'Delete all imported activities',
 'Open folder':'Open local data folder',
 'Install update':'Install a verified release',
 'Welcome':'Welcome to LibreHeatmaps',
 'Peak speed':'Peak recorded speed',
 'Add activity details':'Add activity details',
 'Recorded activity':'Activity preview',
 'Route preview':'Activity preview',
 'Clear selection':'Your GPS heatmap'
};
function lhHelpDescription(label){return LH_HELP[label]||LH_HELP[LH_HELP_ALIASES[label]]||''}
const lhPopover=document.createElement('div');lhPopover.id='lhHelpPopover';lhPopover.setAttribute('role','tooltip');lhPopover.hidden=true;document.body.append(lhPopover);
let lhStickyHelp=null,lhActiveHelp=null;
function lhPositionHelp(el){if(!el||lhPopover.hidden)return;const r=el.getBoundingClientRect();const margin=10,maxWidth=Math.min(350,window.innerWidth-24);lhPopover.style.maxWidth=maxWidth+'px';let x=Math.max(margin,Math.min(window.innerWidth-maxWidth-margin,r.left+r.width/2-maxWidth/2));lhPopover.style.left=x+'px';let top=r.bottom+10;if(top+lhPopover.offsetHeight>window.innerHeight-margin&&r.top>lhPopover.offsetHeight+14)top=r.top-lhPopover.offsetHeight-9;lhPopover.style.top=Math.max(margin,Math.min(top,window.innerHeight-lhPopover.offsetHeight-margin))+'px'}
function lhShowHelp(button){const dialogs=Array.from(document.querySelectorAll('dialog[open]'));const host=dialogs.length?dialogs[dialogs.length-1]:document.body;if(lhPopover.parentElement!==host)host.append(lhPopover);const label=button.dataset.helpLabel,detail=button.dataset.helpText;if(!detail)return;lhActiveHelp=button;lhPopover.replaceChildren();const heading=document.createElement('strong');heading.textContent=label;const body=document.createElement('span');body.textContent=detail;lhPopover.append(heading,body);lhPopover.hidden=false;lhPositionHelp(button);button.setAttribute('aria-describedby','lhHelpPopover')}
function lhHideHelp(force=false){if(lhStickyHelp&&!force)return;lhPopover.hidden=true;if(lhActiveHelp)lhActiveHelp.removeAttribute('aria-describedby');lhActiveHelp=null;if(force)lhStickyHelp=null}
function lhHelpButton(label,text){const b=document.createElement('button');b.type='button';b.className='lh-help-trigger';b.textContent='?';b.dataset.helpLabel=label;b.dataset.helpText=text;b.setAttribute('aria-label','Help: '+label);
 b.addEventListener('pointerenter',()=>{if(!lhStickyHelp)lhShowHelp(b)});
 b.addEventListener('pointerleave',()=>{if(!lhStickyHelp)lhHideHelp()});
 b.addEventListener('focus',()=>{if(!lhStickyHelp)lhShowHelp(b)});
 b.addEventListener('blur',()=>{if(!lhStickyHelp)lhHideHelp()});
 b.addEventListener('click',e=>{e.preventDefault();e.stopPropagation();if(lhStickyHelp===b){lhHideHelp(true)}else{lhStickyHelp=b;lhShowHelp(b)}});
 b.addEventListener('keydown',e=>{if(e.key==='Escape'){e.stopPropagation();lhHideHelp(true)}});return b}
document.addEventListener('pointerdown',e=>{if(lhStickyHelp&&!e.target.closest('.lh-help-trigger'))lhHideHelp(true)},true);
document.addEventListener('keydown',e=>{if(e.key==='Escape')lhHideHelp(true)},true);
window.addEventListener('resize',()=>{if(lhActiveHelp)lhPositionHelp(lhActiveHelp)});
document.addEventListener('scroll',()=>{if(lhActiveHelp)lhPositionHelp(lhActiveHelp)},true);
function lhAddCardHelp(target,label,description){if(!target||target.dataset.lhHelpAdded)return;const text=description||lhHelpDescription(label);if(!text)return;
 target.dataset.lhHelpAdded='1';const b=lhHelpButton(label,text);
 if(target.tagName==='BUTTON') {const wrapper=document.createElement('div');wrapper.className='lh-help-cardwrap';target.replaceWith(wrapper);wrapper.append(target,b);target.classList.add('lh-help-card-content')}else {target.classList.add('lh-help-card');target.append(b)}
}
function lhAddHeadingHelp(h,label,description){if(!h||h.dataset.lhHelpAdded)return;const t=label||h.textContent.trim(),d=description||lhHelpDescription(t);if(!d)return;h.dataset.lhHelpAdded='1';h.classList.add('lh-help-heading');h.append(lhHelpButton(t,d))}
function lhAddFieldHelp(field,label,description){if(!field||field.dataset.lhHelpAdded)return;const d=description||lhHelpDescription(label);if(!d)return;field.dataset.lhHelpAdded='1';field.classList.add('lh-help-field');field.append(lhHelpButton(label,d))}
function lhAddActionHelp(button,label,description){if(!button||button.classList.contains('lh-help-trigger')||button.dataset.lhHelpAdded||button.closest('.lh-help-cardwrap'))return;const text=description||lhHelpDescription(label);if(!text)return;button.dataset.lhHelpAdded='1';
 if(button.matches('[data-stats-tab]')){button.classList.add('lh-help-tab');const mark=document.createElement('span');mark.className='lh-tab-question';mark.textContent='?';mark.setAttribute('aria-hidden','true');button.append(mark);button.addEventListener('pointerenter',()=>lhShowHelp({dataset:{helpLabel:label,helpText:text},getBoundingClientRect:()=>button.getBoundingClientRect(),setAttribute:()=>{},removeAttribute:()=>{}}));button.addEventListener('pointerleave',()=>lhHideHelp());return}
 const wrapper=document.createElement('span');wrapper.className='lh-help-action-wrap';if(button.classList.contains('full')||button.classList.contains('ride-list-item'))wrapper.classList.add('lh-help-action-full');button.replaceWith(wrapper);wrapper.append(button,lhHelpButton(label,text));
}
function lhInlineLabelHelp(label,title){if(!label||label.dataset.lhHelpAdded)return;const d=lhHelpDescription(title);if(!d)return;label.dataset.lhHelpAdded='1';const p=label.parentElement;
 if(p&&p.classList.contains('field')){lhAddFieldHelp(p,title,d);return}
 const wrapper=document.createElement('div');wrapper.className=label.classList.contains('toggle')?'lh-help-toggle-wrap':'lh-help-label-wrap';label.replaceWith(wrapper);wrapper.append(label,lhHelpButton(title,d));
}
function lhLabelForInput(input){const label=input.closest('label');if(label){const text=label.cloneNode(true);text.querySelectorAll('input,select,output').forEach(n=>n.remove());return text.textContent.trim()||input.getAttribute('aria-label')||''}return document.querySelector('label[for="'+input.id+'"]')?.textContent.trim()||input.getAttribute('aria-label')||''}
function lhInstallHelp(){
 // Contextual help is deliberately scoped to informative Stats cards and Settings sections.
 // The map, header, navigation, headings, action buttons, and activity previews remain clean.
 document.querySelectorAll('#metricGrid .metric,#facts .record-metric,#milestones .record-metric').forEach(el=>{
  const title=el.querySelector('span')?.textContent.trim()||'';
  lhAddCardHelp(el,title);
 });
 // Explanations belong to the relevant chart/card, not to its title or each row.
 for(const [selector,title] of [
  ['#yearChart','Distance by year'],['#monthChart','Monthly activity'],
  ['#weekdayChart','Activity by weekday'],['#sportSummary','Types & gear'],
  ['#recentRoutes','Recently recorded'],['#topRoutes','Longest routes']]){
  const chart=document.querySelector(selector);
  const card=chart?.closest('.insights-content-card');
  if(card)lhAddCardHelp(card,title);
 }
 // These are informational sections in their own right.
 const details=document.querySelector('#statsDialog .details-repair');
 if(details)lhAddCardHelp(details,'Add activity details');
 const filter=document.querySelector('#statsDialog .insights-filter-drawer');
 if(filter)lhAddCardHelp(filter,'Filter your activities');
 for(const section of document.querySelectorAll('#settingsDialog .settings-section')){
  const title=section.querySelector(':scope > h2')?.textContent.trim()||'';
  lhAddCardHelp(section,title);
 }
}
const lhObserved=['metricGrid','facts','milestones','ridePreviewDetails','recentRoutes','topRoutes'];
let lhEnhancePending=false;
function lhQueueEnhance(){if(lhEnhancePending)return;lhEnhancePending=true;queueMicrotask(()=>{lhEnhancePending=false;lhInstallHelp()})}
for(const id of lhObserved){const node=$(id);if(node)new MutationObserver(lhQueueEnhance).observe(node,{childList:true,subtree:true})}
lhInstallHelp();

</script></body></html>'''


def version_tuple(value):
    match = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", str(value))
    return tuple(map(int, match.groups())) if match else None


def latest_release():
    global UPDATE_READY
    address = "https://api.github.com/repos/{}/releases/latest".format(RELEASE_REPO)
    request = Request(address, headers={"User-Agent": "LibreHeatmaps/" + VERSION, "Accept": "application/vnd.github+json"})
    with urlopen(request, timeout=10) as response:
        payload = response.read(256 * 1024 + 1)
    if len(payload) > 256 * 1024:
        raise ValueError("GitHub release metadata exceeds the size limit")
    data = json.loads(payload)
    tag = data.get("tag_name", "")
    number = version_tuple(tag)
    if not number:
        raise ValueError("No compatible version number in the GitHub release")
    release = ".".join(map(str, number))
    asset_name = "LibreHeatmaps-v{}.zip".format(release)
    asset = next((a for a in data.get("assets", []) if a.get("name") == asset_name), {})
    digest = asset.get("digest", "")
    url = asset.get("browser_download_url", "")
    trusted = url == "https://github.com/{}/releases/download/{}/{}".format(RELEASE_REPO, tag, asset_name) and bool(re.fullmatch(r"sha256:[a-fA-F0-9]{64}", digest))
    newer = number > version_tuple(VERSION)
    UPDATE_READY = {"version": release, "url": url, "digest": digest} if trusted and newer else None
    return {"installed": VERSION, "latest": release, "newer": newer, "installable": UPDATE_READY is not None, "release_url": "https://github.com/{}/releases/tag/{}".format(RELEASE_REPO, tag), "message": "Ready to install" if UPDATE_READY else "You already have the latest release" if not newer else "GitHub release is missing a verified ZIP and SHA-256 digest"}


def install_verified_release():
    global UPDATE_READY
    with UPDATE_LOCK:
        ready = UPDATE_READY
        if not ready or version_tuple(ready["version"]) <= version_tuple(VERSION):
            raise ValueError("Check for updates first")
        with STATE_LOCK:
            if JOB["status"] == "running":
                raise ValueError("Wait until importing finishes")
        request = Request(ready["url"], headers={"User-Agent": "LibreHeatmaps/" + VERSION})
        with urlopen(request, timeout=25) as response:
            raw = response.read(3 * 1024 * 1024 + 1)
        if len(raw) > 3 * 1024 * 1024:
            raise ValueError("The update ZIP is too large")
        if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), ready["digest"].split(":",1)[1].lower()):
            raise ValueError("Update checksum verification failed")
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            members = [m for m in archive.infolist() if not m.is_dir()]
            names = [m.filename.replace("\\", "/") for m in members]
            roots = set(n.rsplit("/", 1)[0] if "/" in n else "" for n in names)
            if len(names) != 3 or len(set(names)) != 3 or len(roots) != 1 or {n.rsplit("/", 1)[-1] for n in names} != {"LibreHeatmaps.py", "README.md", "LICENSE"} or any(n.startswith("/") or ".." in n.split("/") or n.count("/") > 1 for n in names):
                raise ValueError("Unsafe or incomplete release ZIP")
            files = {}
            for member in members:
                if member.file_size > 2 * 1024 * 1024:
                    raise ValueError("Release file exceeds the size limit")
                files[member.filename.rsplit("/",1)[-1]] = archive.read(member)
        try:
            source = files["LibreHeatmaps.py"].decode("utf-8")
            compile(source, "LibreHeatmaps.py", "exec")
        except (UnicodeDecodeError, SyntaxError) as exc:
            raise ValueError("The release script is invalid") from exc
        if 'VERSION = "{}"'.format(ready["version"]) not in source[:4000]:
            raise ValueError("Release ZIP and version do not match")
        target = Path(__file__).resolve()
        directory = target.parent
        license_path = directory / "LICENSE"
        if license_path.exists() and license_path.read_bytes() != files["LICENSE"]:
            raise ValueError("The new release unexpectedly changed the project license")
        destinations = [(target, files["LibreHeatmaps.py"]), (directory / "README.md", files["README.md"]), (license_path, files["LICENSE"])]
        staged, backups, replaced = [], [], []
        try:
            for dest, content in destinations:
                fd, path = tempfile.mkstemp(prefix=".libreheatmaps-", dir=str(directory))
                with os.fdopen(fd, "wb") as f:
                    f.write(content)
                    f.flush()
                    os.fsync(f.fileno())
                staged.append((dest, Path(path)))
            for dest, _ in staged:
                if dest.exists():
                    backup = dest.with_name(dest.name + ".backup-before-v" + ready["version"])
                    shutil.copy2(dest, backup)
                    backups.append((dest, backup))
            for dest, stage in staged:
                os.replace(stage, dest)
                replaced.append(dest)
        except Exception:
            for dest in reversed(replaced):
                backup = next((b for original,b in backups if original == dest), None)
                if backup:
                    os.replace(backup, dest)
                else:
                    dest.unlink(missing_ok=True)
            raise
        finally:
            for _, stage in staged:
                stage.unlink(missing_ok=True)
        UPDATE_READY = None
        return {"status":"installed", "version":ready["version"], "message":"Update installed. Exit and reopen LibreHeatmaps to use the new version. Your database remains untouched."}


def open_local_data_folder():
    directory = DB_PATH.parent.resolve()
    if not directory.is_dir():
        raise ValueError("Data folder not found")
    if os.name == "nt":
        os.startfile(str(directory))
    elif sys.platform == "darwin":
        subprocess.Popen(["open", str(directory)], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        subprocess.Popen(["xdg-open", str(directory)], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return {"status":"opened"}


class Handler(BaseHTTPRequestHandler):
    def valid_local_request(self):
        host = self.headers.get("Host", "")
        expected = "127.0.0.1:{}".format(self.server.server_port)
        if host != expected:
            self.respond({"error": "Invalid local host"}, 403)
            return False
        if self.headers.get("Sec-Fetch-Site", "").lower() == "cross-site":
            self.respond({"error": "Cross-site access is not allowed"}, 403)
            return False
        return True

    def log_message(self, fmt, *args):
        return

    def send_bytes(self, data, content_type="application/json", code=200):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
        self.send_header("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data: https://tile.openstreetmap.org https://server.arcgisonline.com; connect-src 'self'; form-action 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        self.wfile.write(data)

    def respond(self, payload, code=200):
        self.send_bytes(json.dumps(payload, separators=(",", ":")).encode(), code=code)

    def do_GET(self):
        if not self.valid_local_request():
            return
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        try:
            if parsed.path == "/":
                self.send_bytes(PAGE.replace('__APP_TOKEN__', TOKEN).encode(), "text/html; charset=utf-8")
            elif parsed.path == "/api/preferences":
                self.respond({"appearance":load_appearance()})
            elif parsed.path == "/api/system":
                self.respond({"version":VERSION,"data_directory":str(DB_PATH.parent),"database":str(DB_PATH),"log":str(DB_PATH.parent/"application.log"),"script":str(Path(__file__).resolve())})
            elif parsed.path == "/api/stats":
                self.respond(statistics())
            elif parsed.path == "/api/insights":
                self.respond(insights(params))
            elif parsed.path == "/api/activity":
                self.respond(activity_preview(params))
            elif parsed.path == "/api/job":
                with STATE_LOCK:
                    self.respond(dict(JOB))
            elif parsed.path == "/api/view":
                self.send_bytes(create_view(params))
            elif parsed.path == "/api/export.gpx":
                export_gpx(self, params)
            elif parsed.path in ("/logo.png", "/favicon.ico"):
                self.send_bytes(LOGO_PNG, "image/png")
            else:
                self.respond({"error": "Not found"}, 404)
        except (ValueError, sqlite3.Error) as exc:
            self.respond({"error": str(exc)}, 400)
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as exc:
            print("[LibreHeatmaps] Request error:", exc, flush=True)
            try:
                self.respond({"error": "Request failed"}, 500)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def do_POST(self):
        if not self.valid_local_request():
            return
        endpoint = urlparse(self.path).path
        if endpoint == "/api/welcome/dismiss":
            origin = self.headers.get("Origin", "")
            if self.headers.get("X-LibreHeatmaps-Token") != TOKEN or (origin and origin != "http://127.0.0.1:{}".format(self.server.server_port)):
                self.respond({"error":"Forbidden"}, 403)
                return
            with db_connect() as con:
                con.execute("INSERT OR REPLACE INTO app_settings (key,value) VALUES (?,?)", ("welcome_dismissed", "1"))
            self.respond({"ok":True})
            return
        if endpoint == "/api/preferences":
            origin = self.headers.get("Origin", "")
            if self.headers.get("X-LibreHeatmaps-Token") != TOKEN or (origin and origin != "http://127.0.0.1:{}".format(self.server.server_port)):
                self.respond({"error":"Forbidden"},403)
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 2 <= size <= 4096:
                    raise ValueError("Invalid settings request size")
                content = json.loads(self.rfile.read(size).decode("utf-8"))
                self.respond({"appearance":store_appearance(content)})
            except (ValueError, UnicodeDecodeError, sqlite3.Error) as exc:
                self.respond({"error":str(exc)[:200]},400)
            return
        if endpoint in ("/api/system/open-folder", "/api/update/check", "/api/update/install"):
            origin = self.headers.get("Origin", "")
            if self.headers.get("X-LibreHeatmaps-Token") != TOKEN or (origin and origin != "http://127.0.0.1:{}".format(self.server.server_port)):
                self.respond({"error":"Forbidden"},403)
                return
            try:
                if endpoint == "/api/system/open-folder":
                    self.respond(open_local_data_folder())
                elif endpoint == "/api/update/check":
                    self.respond(latest_release())
                else:
                    self.respond(install_verified_release())
            except Exception as exc:
                self.respond({"error":str(exc)[:300]},400)
            return
        if endpoint in ("/api/cache/clear-view", "/api/cache/clear-activities"):
            origin = self.headers.get("Origin", "")
            if self.headers.get("X-LibreHeatmaps-Token") != TOKEN or (origin and origin != "http://127.0.0.1:{}".format(self.server.server_port)):
                self.respond({"error": "Forbidden"}, 403)
                return
            with STATE_LOCK:
                if JOB["status"] == "running":
                    self.respond({"error": "Wait until the current import finishes"}, 409)
                    return
                if endpoint == "/api/cache/clear-view":
                    clear_render_caches(include_tracks=True)
                    self.respond({"status": "cleared"})
                    return
                connection = db_connect()
                try:
                    with connection:
                        removed = connection.execute("SELECT COUNT(*) FROM activities").fetchone()[0]
                        connection.execute("DELETE FROM activities")
                except sqlite3.Error:
                    self.respond({"error": "Unable to clear local activities"}, 500)
                    return
                try:
                    connection.execute("VACUUM")
                except sqlite3.Error:
                    pass
                finally:
                    connection.close()
                clear_render_caches(include_tracks=True)
                self.respond({"status": "cleared", "deleted": removed})
            return
        if endpoint in ("/api/ping", "/api/leave", "/api/shutdown"):
            origin = self.headers.get("Origin", "")
            if self.headers.get("X-LibreHeatmaps-Token") != TOKEN or (origin and origin != "http://127.0.0.1:{}".format(self.server.server_port)):
                self.respond({"error": "Forbidden"}, 403)
                return
            if endpoint == "/api/shutdown":
                self.respond({"status": "closing"})
                threading.Thread(target=self.server.shutdown, daemon=True).start()
                return
            length = min(128, max(0, int(self.headers.get("Content-Length", "0"))))
            client_id = self.rfile.read(length).decode("ascii", errors="ignore")
            if len(client_id) < 4 or len(client_id) > 80 or not client_id.isalnum():
                self.respond({"error": "Invalid client ID"}, 400)
                return
            with CLIENT_LOCK:
                if endpoint == "/api/ping":
                    ACTIVE_CLIENTS[client_id] = time.monotonic()
                else:
                    ACTIVE_CLIENTS.pop(client_id, None)
            self.respond({"ok": True})
            return
        if endpoint != "/api/import":
            self.respond({"error": "Not found"}, 404)
            return
        origin = self.headers.get("Origin", "")
        if self.headers.get("X-LibreHeatmaps-Token") != TOKEN or (origin and origin != "http://127.0.0.1:{}".format(self.server.server_port)):
            self.respond({"error": "Forbidden"}, 403)
            return
        with STATE_LOCK:
            if JOB["status"] == "running":
                self.respond({"error": "An import is already running"}, 409)
                return
            JOB.update(status="running", processed=0, total=0, added=0, skipped=0, failed=0, updated=0, no_gps=0, message="Receiving file")
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 1 or length > MAX_UPLOAD:
                raise ValueError("Invalid file size")
            filename = unquote(self.headers.get("X-File-Name", "")).rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
            if not filename.lower().endswith((".zip", ".csv") + FORMATS):
                raise ValueError("Select a Strava ZIP, activities.csv, FIT, GPX or TCX file")
            fd, temporary = tempfile.mkstemp(prefix="libreheatmaps-import-", suffix=".tmp", dir=str(DB_PATH.parent))
            with os.fdopen(fd, "wb") as output:
                remaining = length
                while remaining:
                    chunk = self.rfile.read(min(256 * 1024, remaining))
                    if not chunk:
                        raise ValueError("Upload ended prematurely")
                    output.write(chunk)
                    remaining -= len(chunk)
            if filename.lower().endswith(".zip") and not zipfile.is_zipfile(temporary):
                raise ValueError("The selected file is not a valid ZIP")
            threading.Thread(target=import_job, args=(temporary, filename), daemon=True).start()
            self.respond({"status": "started"}, 202)
        except Exception as exc:
            set_job(status="error", message=str(exc))
            if "temporary" in locals():
                try:
                    os.unlink(temporary)
                except OSError:
                    pass
            self.respond({"error": str(exc)}, 400)


def idle_watcher(server):
    while True:
        time.sleep(10)
        with CLIENT_LOCK:
            now = time.monotonic()
            expired = [key for key, stamp in ACTIVE_CLIENTS.items() if now - stamp > 42]
            for key in expired:
                ACTIVE_CLIENTS.pop(key, None)
            active = bool(ACTIVE_CLIENTS)
        with STATE_LOCK:
            busy = JOB["status"] == "running"
        if not active and not busy and now - SERVER_STARTED > 90:
            server.shutdown()
            return


def main():
    global DB_PATH, SERVER_STARTED
    parser = argparse.ArgumentParser(description="LibreHeatmaps local GPS activity viewer")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--no-browser", action="store_true")
    default_data = Path.home() / ".libreheatmaps"
    parser.add_argument("--data-dir", type=Path, default=default_data)
    parser.add_argument("--background", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.background and not args.no_browser and args.port == 0 and args.data_dir == default_data:
        gui_python = Path(sys.executable).with_name("pythonw.exe") if os.name == "nt" else Path(sys.executable)
        if not gui_python.is_file():
            gui_python = Path(sys.executable)
        if gui_python.is_file() and Path(__file__).is_file():
            log_dir = args.data_dir.expanduser().resolve()
            log_dir.mkdir(parents=True, exist_ok=True)
            flags = (subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP) if os.name == "nt" else 0
            with open(log_dir / "application.log", "ab") as log:
                subprocess.Popen([str(gui_python), str(Path(__file__).resolve()), "--background"], cwd=str(Path(__file__).resolve().parent), stdin=subprocess.DEVNULL, stdout=log, stderr=log, creationflags=flags, start_new_session=(os.name != "nt"), close_fds=True)
            return
    data_dir = args.data_dir.expanduser().resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    DB_PATH = data_dir / "activities.sqlite3"
    init_db()
    SERVER_STARTED = time.monotonic()
    with ThreadingHTTPServer(("127.0.0.1", args.port), Handler) as server:
        url = "http://127.0.0.1:{}".format(server.server_port)
        print("\nLibreHeatmaps {}".format(VERSION), flush=True)
        print("Open {} in your browser".format(url), flush=True)
        print("Activity cache: {}".format(DB_PATH), flush=True)
        if args.background:
            threading.Thread(target=idle_watcher, args=(server,), daemon=True).start()
        if not args.no_browser:
            threading.Timer(0.65, lambda: webbrowser.open(url)).start()
        try:
            server.serve_forever(poll_interval=.25)
        except KeyboardInterrupt:
            print("\nClosing LibreHeatmaps.")


LOGO_PNG = base64.b64decode(
    """
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
    """
)


if __name__ == "__main__":
    main()
