#!/usr/bin/env python3
import argparse
import csv
import os
import sys
from datetime import datetime

import psycopg2


def parse_args():
    p = argparse.ArgumentParser(description="Load Metro Transit CSVs into PostgreSQL")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--dbname", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--datadir", required=True, help="Directory containing CSV files")
    return p.parse_args()


def connect(args):
    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )
    conn.autocommit = False
    return conn


def run_schema(conn):
    here = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(here, "schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def load_lines(conn, path):
    count = 0
    with open(path, newline="", encoding="utf-8") as f, conn.cursor() as cur:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute(
                """
                INSERT INTO lines (line_name, vehicle_type)
                VALUES (%s, %s)
                ON CONFLICT (line_name) DO UPDATE SET vehicle_type = EXCLUDED.vehicle_type
                RETURNING line_id
                """,
                (row["line_name"].strip(), row["vehicle_type"].strip()),
            )
            _ = cur.fetchone()[0]
            count += 1
    conn.commit()
    return count


def build_line_map(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT line_id, line_name FROM lines")
        return {name: lid for (lid, name) in cur.fetchall()}


def load_stops(conn, path):
    count = 0
    with open(path, newline="", encoding="utf-8") as f, conn.cursor() as cur:
        reader = csv.DictReader(f)
        for row in reader:
            stop_name = row["stop_name"].strip()
            lat = float(row["latitude"]) if row["latitude"] else None
            lon = float(row["longitude"]) if row["longitude"] else None
            cur.execute(
                """
                INSERT INTO stops (stop_name, latitude, longitude)
                VALUES (%s, %s, %s)
                ON CONFLICT (stop_name) DO UPDATE SET latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude
                RETURNING stop_id
                """,
                (stop_name, lat, lon),
            )
            _ = cur.fetchone()[0]
            count += 1
    conn.commit()
    return count


def build_stop_map(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT stop_id, stop_name FROM stops")
        return {name: sid for (sid, name) in cur.fetchall()}


def load_line_stops(conn, path, line_map, stop_map):
    count = 0
    with open(path, newline="", encoding="utf-8") as f, conn.cursor() as cur:
        reader = csv.DictReader(f)
        for row in reader:
            line_name = row["line_name"].strip()
            stop_name = row["stop_name"].strip()
            sequence = int(row["sequence"]) if row["sequence"] else None
            time_offset = int(row["time_offset"]) if row["time_offset"] else 0

            line_id = line_map.get(line_name)
            stop_id = stop_map.get(stop_name)
            if line_id is None or stop_id is None:
                raise ValueError(f"Unknown reference in line_stops: line={line_name}, stop={stop_name}")

            cur.execute(
                """
                INSERT INTO line_stops (line_id, stop_id, sequence, time_offset_minutes)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (line_id, sequence) DO UPDATE SET stop_id = EXCLUDED.stop_id, time_offset_minutes = EXCLUDED.time_offset_minutes
                """,
                (line_id, stop_id, sequence, time_offset),
            )
            count += 1
    conn.commit()
    return count


def load_trips(conn, path, line_map):
    count = 0
    with open(path, newline="", encoding="utf-8") as f, conn.cursor() as cur:
        reader = csv.DictReader(f)
        for row in reader:
            trip_id = row["trip_id"].strip()
            line_name = row["line_name"].strip()
            sched = datetime.fromisoformat(row["scheduled_departure"].strip())
            vehicle_id = row["vehicle_id"].strip()

            line_id = line_map.get(line_name)
            if line_id is None:
                raise ValueError(f"Unknown line for trip {trip_id}: {line_name}")

            cur.execute(
                """
                INSERT INTO trips (trip_id, line_id, scheduled_departure, vehicle_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (trip_id) DO UPDATE SET line_id = EXCLUDED.line_id, scheduled_departure = EXCLUDED.scheduled_departure, vehicle_id = EXCLUDED.vehicle_id
                """,
                (trip_id, line_id, sched, vehicle_id),
            )
            count += 1
    conn.commit()
    return count


def load_stop_events(conn, path, stop_map):
    count = 0
    with open(path, newline="", encoding="utf-8") as f, conn.cursor() as cur:
        reader = csv.DictReader(f)
        for row in reader:
            trip_id = row["trip_id"].strip()
            stop_name = row["stop_name"].strip()
            stop_id = stop_map.get(stop_name)
            if stop_id is None:
                raise ValueError(f"Unknown stop for stop_event: {stop_name}")
            scheduled = datetime.fromisoformat(row["scheduled"].strip())
            actual = datetime.fromisoformat(row["actual"].strip())
            on = int(row["passengers_on"]) if row["passengers_on"] else 0
            off = int(row["passengers_off"]) if row["passengers_off"] else 0

            cur.execute(
                """
                INSERT INTO stop_events (trip_id, stop_id, scheduled, actual, passengers_on, passengers_off)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (trip_id, stop_id) DO UPDATE SET scheduled = EXCLUDED.scheduled, actual = EXCLUDED.actual, passengers_on = EXCLUDED.passengers_on, passengers_off = EXCLUDED.passengers_off
                """,
                (trip_id, stop_id, scheduled, actual, on, off),
            )
            count += 1
    conn.commit()
    return count


def main():
    args = parse_args()
    print(f"Connected to {args.dbname}@{args.host}")
    conn = connect(args)
    try:
        print("Creating schema...")
        run_schema(conn)
        print("Tables created: lines, stops, line_stops, trips, stop_events")

        total = 0
        def p(fname, n):
            print(f"Loading {fname}... {n} rows")

        # Load in order
        n = load_lines(conn, os.path.join(args.datadir, "lines.csv")); total += n; p(os.path.join(args.datadir, "lines.csv"), n)
        n = load_stops(conn, os.path.join(args.datadir, "stops.csv")); total += n; p(os.path.join(args.datadir, "stops.csv"), n)
        line_map = build_line_map(conn)
        stop_map = build_stop_map(conn)
        n = load_line_stops(conn, os.path.join(args.datadir, "line_stops.csv"), line_map, stop_map); total += n; p(os.path.join(args.datadir, "line_stops.csv"), n)
        n = load_trips(conn, os.path.join(args.datadir, "trips.csv"), line_map); total += n; p(os.path.join(args.datadir, "trips.csv"), n)
        stop_map = build_stop_map(conn)  # refresh just in case
        n = load_stop_events(conn, os.path.join(args.datadir, "stop_events.csv"), stop_map); total += n; p(os.path.join(args.datadir, "stop_events.csv"), n)

        print(f"\nTotal: {total} rows loaded")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
