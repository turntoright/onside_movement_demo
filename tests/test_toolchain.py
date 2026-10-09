"""Checks that the local toolchain can do what the design relies on."""

import subprocess
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

REPO = Path(__file__).resolve().parents[1]


def test_zoneinfo_has_new_zealand():
    assert ZoneInfo("Pacific/Auckland").key == "Pacific/Auckland"


def test_duckdb_converts_timestamptz_to_new_zealand_date():
    # 2026-04-05 02:30 NZDT is after midnight local time, so the local date is 5 April.
    row = duckdb.sql(
        "select (cast('2026-04-05T02:30:00+13:00' as timestamptz) "
        "at time zone 'Pacific/Auckland')::date"
    ).fetchone()
    assert row[0] == date(2026, 4, 5)


def test_project_dbt_is_dbt_core_with_duckdb_adapter():
    out = subprocess.run(
        [str(REPO / ".venv" / "bin" / "dbt"), "--version"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert "duckdb" in out.lower()
