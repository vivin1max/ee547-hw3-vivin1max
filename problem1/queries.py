#!/usr/bin/env python3
import argparse
import json
import sys
from typing import Any, Dict, List

import psycopg2


def parse_args():
    p = argparse.ArgumentParser(description="Run predefined transit queries")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--dbname", required=True)
    p.add_argument("--user", default="transit")
    p.add_argument("--password", default="transit123")
    p.add_argument("--query", choices=[f"Q{i}" for i in range(1, 10 + 1)])
    p.add_argument("--all", action="store_true")
    p.add_argument("--format", choices=["json", "text"], default="text")
    return p.parse_args()


def connect(args):
    return psycopg2.connect(
        host=args.host, port=args.port, dbname=args.dbname, user=args.user, password=args.password
    )


def rows_to_dicts(cur) -> List[Dict[str, Any]]:
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


QUERIES = {
    "Q1": {
        "description": "List all stops on Route 20 in order",
        "sql": (
            """
            SELECT s.stop_name, ls.sequence, ls.time_offset_minutes AS time_offset
            FROM line_stops ls
            JOIN lines l ON l.line_id = ls.line_id
            JOIN stops s ON s.stop_id = ls.stop_id
            WHERE l.line_name = %s
            ORDER BY ls.sequence
            """,
            ("Route 20",),
        ),
    },
    "Q2": {
        "description": "Trips during morning rush (7-9 AM)",
        "sql": (
            """
            SELECT t.trip_id, l.line_name, t.scheduled_departure
            FROM trips t
            JOIN lines l ON l.line_id = t.line_id
            WHERE (t.scheduled_departure::time) >= TIME '07:00' AND (t.scheduled_departure::time) < TIME '09:00'
            ORDER BY t.scheduled_departure
            """,
            tuple(),
        ),
    },
    "Q3": {
        "description": "Transfer stops (stops on 2+ routes)",
        "sql": (
            """
            SELECT s.stop_name, COUNT(DISTINCT ls.line_id) AS line_count
            FROM line_stops ls
            JOIN stops s ON s.stop_id = ls.stop_id
            GROUP BY s.stop_id, s.stop_name
            HAVING COUNT(DISTINCT ls.line_id) >= 2
            ORDER BY line_count DESC, s.stop_name
            """,
            tuple(),
        ),
    },
    "Q4": {
        "description": "Complete route for trip T0001 (per-stop scheduled and actual)",
        "sql": (
            """
            SELECT s.stop_name, se.scheduled, se.actual
            FROM stop_events se
            JOIN stops s ON s.stop_id = se.stop_id
            WHERE se.trip_id = %s
            ORDER BY se.scheduled
            """,
            ("T0001",),
        ),
    },
    "Q5": {
        "description": "Routes serving both Wilshire / Veteran and Le Conte / Broxton",
        "sql": (
            """
            SELECT DISTINCT l.line_name
            FROM lines l
            JOIN line_stops ls1 ON ls1.line_id = l.line_id
            JOIN stops s1 ON s1.stop_id = ls1.stop_id AND s1.stop_name = %s
            JOIN line_stops ls2 ON ls2.line_id = l.line_id
            JOIN stops s2 ON s2.stop_id = ls2.stop_id AND s2.stop_name = %s
            ORDER BY l.line_name
            """,
            ("Wilshire / Veteran", "Le Conte / Broxton"),
        ),
    },
    "Q6": {
        "description": "Average ridership by line (per stop event)",
        "sql": (
            """
            SELECT l.line_name, AVG((se.passengers_on + se.passengers_off))::numeric(10,2) AS avg_passengers
            FROM stop_events se
            JOIN trips t ON t.trip_id = se.trip_id
            JOIN lines l ON l.line_id = t.line_id
            GROUP BY l.line_id, l.line_name
            ORDER BY avg_passengers DESC
            """,
            tuple(),
        ),
    },
    "Q7": {
        "description": "Top 10 busiest stops (total activity)",
        "sql": (
            """
            SELECT s.stop_name, SUM(se.passengers_on + se.passengers_off) AS total_activity
            FROM stop_events se
            JOIN stops s ON s.stop_id = se.stop_id
            GROUP BY s.stop_id, s.stop_name
            ORDER BY total_activity DESC, s.stop_name
            LIMIT 10
            """,
            tuple(),
        ),
    },
    "Q8": {
        "description": "Count delays by line (> 2 min late)",
        "sql": (
            """
            SELECT l.line_name, COUNT(*) AS delay_count
            FROM stop_events se
            JOIN trips t ON t.trip_id = se.trip_id
            JOIN lines l ON l.line_id = t.line_id
            WHERE se.actual > se.scheduled + INTERVAL '2 minutes'
            GROUP BY l.line_id, l.line_name
            ORDER BY delay_count DESC, l.line_name
            """,
            tuple(),
        ),
    },
    "Q9": {
        "description": "Trips with 3+ delayed stops (> 2 min)",
        "sql": (
            """
            SELECT t.trip_id, COUNT(*) AS delayed_stop_count
            FROM stop_events se
            JOIN trips t ON t.trip_id = se.trip_id
            WHERE se.actual > se.scheduled + INTERVAL '2 minutes'
            GROUP BY t.trip_id
            HAVING COUNT(*) >= 3
            ORDER BY delayed_stop_count DESC, t.trip_id
            """,
            tuple(),
        ),
    },
    "Q10": {
        "description": "Stops with above-average ridership (by total boardings)",
        "sql": (
            """
            WITH totals AS (
                SELECT se.stop_id, SUM(se.passengers_on) AS total_boardings
                FROM stop_events se
                GROUP BY se.stop_id
            ),
            avg_total AS (
                SELECT AVG(total_boardings) AS avg_boardings FROM totals
            )
            SELECT s.stop_name, t.total_boardings
            FROM totals t
            JOIN stops s ON s.stop_id = t.stop_id
            CROSS JOIN avg_total a
            WHERE t.total_boardings > a.avg_boardings
            ORDER BY t.total_boardings DESC, s.stop_name
            """,
            tuple(),
        ),
    },
}


def run_query(conn, key: str, fmt: str):
    meta = QUERIES[key]
    sql, params = meta["sql"]
    desc = meta["description"]
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = rows_to_dicts(cur)
    if fmt == "json":
        out = {"query": key, "description": desc, "results": rows, "count": len(rows)}
        print(json.dumps(out, default=str, indent=2))
    else:
        print(f"{key}: {desc}")
        for r in rows:
            print(r)
        print(f"Count: {len(rows)}\n")


def main():
    args = parse_args()
    if not args.query and not args.all:
        print("Specify --query Q# or --all", file=sys.stderr)
        sys.exit(2)
    conn = connect(args)
    try:
        if args.all:
            for i in range(1, 11):
                run_query(conn, f"Q{i}", args.format)
        else:
            run_query(conn, args.query, args.format)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
