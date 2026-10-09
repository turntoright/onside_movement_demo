"""Command line tracing over the warehouse (spec §5 Tracing; Plan 3 adds a Dagster job)."""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
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


def load_edges(con: duckdb.DuckDBPyConnection) -> list[Edge]:
    return [
        Edge(src, dst, float(dep), float(arr))
        for src, dst, dep, arr in con.sql(EDGES_SQL).fetchall()
    ]


def _local(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=UTC).astimezone(NZ).strftime("%Y-%m-%d %H:%M")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tracing")
    parser.add_argument("--db", required=True)
    parser.add_argument("--property", required=True)
    parser.add_argument("--start", required=True, help="infection start, ISO 8601 with offset")
    parser.add_argument("--max-hops", type=int, default=3)
    args = parser.parse_args(argv)

    con = duckdb.connect(args.db, read_only=True)
    known = con.execute(
        "select property_type from stg_properties where property_id = ?", [args.property]
    ).fetchone()
    if known is None:
        print(f"error: unknown property {args.property}", file=sys.stderr)
        return 2
    types = dict(con.sql("select property_id, property_type from stg_properties").fetchall())
    start = datetime.fromisoformat(args.start).timestamp()
    results = trace(load_edges(con), args.property, start, args.max_hops)
    con.close()

    print(f"{len(results)} properties reached from {args.property} within {args.max_hops} hops")
    print(f"{'hops':>4}  {'property':<8}  {'type':<10}  {'earliest arrival':<16}  path")
    for r in results:
        print(
            f"{r.hops:>4}  {r.property_id:<8}  {types.get(r.property_id, '?'):<10}  "
            f"{_local(r.arrival):<16}  {' -> '.join(r.path)}"
        )
    return 0
