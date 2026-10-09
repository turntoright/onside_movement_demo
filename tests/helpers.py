"""Shared builders for tests."""

import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
from standdown_checker.processor import process_event
from standdown_checker.rules import load_rules

ROOT = Path(__file__).resolve().parents[1]
RULES = load_rules(ROOT / "shared" / "standdown_rules.csv")
PROPERTIES = {
    "P1": {"property_type": "poultry", "owner_company_id": "C2"},
    "P2": {"property_type": "office", "owner_company_id": "C3"},
    "P3": {"property_type": "poultry", "owner_company_id": "C1"},
    "P4": {"property_type": "poultry", "owner_company_id": "C1"},
    "P5": {"property_type": "pork", "owner_company_id": "C4"},
}


class TickingClock:
    """Returns a later time on every call, so re-used values are detectable."""

    def __init__(self) -> None:
        self.t = datetime(2026, 3, 10, tzinfo=UTC)

    def __call__(self) -> datetime:
        self.t += timedelta(seconds=1)
        return self.t


def checkin(event_id, prop, event_time, *, person="U1", received=None) -> dict:
    return {
        "event_id": event_id,
        "person_id": person,
        "visitor_company_id": "V1",
        "property_id": prop,
        "event_time": event_time,
        "received_time": received or event_time,
        "method": "app",
        "job_type": "delivery",
        "additional_visitors": 0,
        "answers": [],
    }


def checkout(event_id, checkin_event_id, prop, event_time, *, person="U1", received=None) -> dict:
    return {
        "event_id": event_id,
        "checkin_event_id": checkin_event_id,
        "person_id": person,
        "property_id": prop,
        "event_time": event_time,
        "received_time": received or event_time,
        "method": "manual",
    }


def run(store, detail_type, detail, *, clock=None, sleep=None):
    return process_event(
        detail_type,
        detail,
        store,
        RULES,
        clock or TickingClock(),
        sleep=sleep or (lambda seconds: None),
    )


def types(outbound) -> list[str]:
    return [o.detail_type for o in outbound]


def build_trace_warehouse(path: Path) -> Path:
    """A tiny warehouse for tracing: P1 -> P2 (connected), P3 isolated."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute("create table stg_properties (property_id varchar, property_type varchar)")
    con.execute(
        "insert into stg_properties values ('P1','poultry'), ('P2','poultry'), ('P3','office')"
    )
    con.execute(
        "create table fct_movements (origin_property_id varchar, dest_property_id varchar, "
        "departure_time timestamptz, arrival_time timestamptz, connected boolean)"
    )
    con.execute(
        "insert into fct_movements values "
        "('P1','P2','2026-03-03 10:00:00+13','2026-03-03 12:00:00+13', true)"
    )
    con.execute(
        "create table stg_plant_movements (source_property_id varchar, dest_property_id varchar, "
        "movement_date date)"
    )
    con.close()
    return path


def make(*args: str, root: Path | None = None) -> subprocess.CompletedProcess:
    """Run make on this repo; with `root`, state/ and warehouse/ live there instead."""
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MFLAGS", "MAKELEVEL")}
    cmd = ["make", "--no-print-directory", "-C", str(ROOT), *args]
    if root is not None:
        cmd += [f"REPO_ROOT={root}", f"VENV={ROOT / '.venv'}"]
    return subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=60)
