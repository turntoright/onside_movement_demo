"""Tracing over the warehouse (spec §5 Tracing): the CLI for sim and the Dagster job's code path."""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from .core import Edge, trace

NZ = ZoneInfo("Pacific/Auckland")

EDGES_SQL = """
select origin_property_id, dest_property_id, epoch(departure_time), epoch(arrival_time)
from fct_movements
where connected
union all
select
    source_property_id,
    dest_property_id,
    epoch(cast(movement_date as timestamp) at time zone 'Pacific/Auckland'),
    epoch(cast(movement_date + 1 as timestamp) at time zone 'Pacific/Auckland') - 1
from stg_plant_movements
"""


class TraceError(ValueError):
    """A user-facing tracing error (unknown property, bad start time, no warehouse)."""


def load_edges(con: duckdb.DuckDBPyConnection) -> list[Edge]:
    return [
        Edge(src, dst, float(dep), float(arr))
        for src, dst, dep, arr in con.sql(EDGES_SQL).fetchall()
    ]


def parse_start(value: str) -> datetime:
    try:
        start = datetime.fromisoformat(value)
    except ValueError:
        raise TraceError(f"start time {value!r} is not ISO 8601") from None
    if start.tzinfo is None:
        raise TraceError(f"start time {value!r} needs a UTC offset, e.g. +12:00")
    return start


def trace_warehouse(db: Path | str, property_id: str, start: str, max_hops: int) -> list[dict]:
    """Trace from one property over a read-only warehouse; one dict per reached property."""
    started = parse_start(start)
    if max_hops < 0:
        raise TraceError(f"max_hops must be 0 or more, not {max_hops}")
    if not Path(db).exists():
        raise TraceError(f"no warehouse at {db}; build it first (dbt build)")
    con = duckdb.connect(str(db), read_only=True)
    try:
        known = con.execute(
            "select property_type from stg_properties where property_id = ?", [property_id]
        ).fetchone()
        if known is None:
            raise TraceError(f"unknown property {property_id}")
        types = dict(con.sql("select property_id, property_type from stg_properties").fetchall())
        reached = trace(load_edges(con), property_id, started.timestamp(), max_hops)
    finally:
        con.close()
    return [
        {
            "property_id": r.property_id,
            "property_type": types.get(r.property_id, "?"),
            "hops": r.hops,
            "arrival": datetime.fromtimestamp(r.arrival, tz=UTC).astimezone(NZ).isoformat(),
            "path": list(r.path),
        }
        for r in reached
    ]


def format_table(rows: list[dict], property_id: str, max_hops: int) -> str:
    lines = [
        f"{len(rows)} properties reached from {property_id} within {max_hops} hops",
        f"{'hops':>4}  {'property':<8}  {'type':<10}  {'earliest arrival':<16}  path",
    ]
    for r in rows:
        local = datetime.fromisoformat(r["arrival"]).astimezone(NZ).strftime("%Y-%m-%d %H:%M")
        lines.append(
            f"{r['hops']:>4}  {r['property_id']:<8}  {r['property_type']:<10}  "
            f"{local:<16}  {' -> '.join(r['path'])}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tracing")
    parser.add_argument("--db", required=True)
    parser.add_argument("--property", required=True)
    parser.add_argument("--start", required=True, help="infection start, ISO 8601 with offset")
    parser.add_argument("--max-hops", type=int, default=3)
    args = parser.parse_args(argv)
    try:
        rows = trace_warehouse(args.db, args.property, args.start, args.max_hops)
    except TraceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(format_table(rows, args.property, args.max_hops))
    return 0
