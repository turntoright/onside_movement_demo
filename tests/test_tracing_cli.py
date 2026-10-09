import pytest

from tests.helpers import build_trace_warehouse
from tracing.cli import TraceError, format_table, main, trace_warehouse


@pytest.fixture()
def db(tmp_path):
    return build_trace_warehouse(tmp_path / "w.duckdb")


def test_traces_downstream_properties(db, capsys):
    code = main(["--db", str(db), "--property", "P1", "--start", "2026-03-03T09:00:00+13:00"])
    assert code == 0
    out = capsys.readouterr().out
    assert "P2" in out and "P1 -> P2" in out


def test_unknown_property_is_a_clear_error(db, capsys):
    code = main(["--db", str(db), "--property", "NOPE", "--start", "2026-03-03T09:00:00+13:00"])
    assert code == 2
    assert "unknown property NOPE" in capsys.readouterr().err


def test_property_without_edges_returns_only_itself(db, capsys):
    code = main(["--db", str(db), "--property", "P3", "--start", "2026-03-03T09:00:00+13:00"])
    assert code == 0
    assert "1 properties reached" in capsys.readouterr().out


def test_trace_warehouse_returns_one_row_per_reached_property(db):
    rows = trace_warehouse(db, "P1", "2026-03-03T09:00:00+13:00", 3)
    assert rows == [
        {
            "property_id": "P1",
            "property_type": "poultry",
            "hops": 0,
            "arrival": "2026-03-03T09:00:00+13:00",
            "path": ["P1"],
        },
        {
            "property_id": "P2",
            "property_type": "poultry",
            "hops": 1,
            "arrival": "2026-03-03T12:00:00+13:00",
            "path": ["P1", "P2"],
        },
    ]
    table = format_table(rows, "P1", 3).splitlines()
    assert table[0] == "2 properties reached from P1 within 3 hops"
    assert table[-1].split() == ["1", "P2", "poultry", "2026-03-03", "12:00", "P1", "->", "P2"]


def test_start_time_without_an_offset_is_refused(db, capsys):
    code = main(["--db", str(db), "--property", "P1", "--start", "2026-03-03T09:00:00"])
    assert code == 2
    assert "needs a UTC offset" in capsys.readouterr().err


def test_missing_warehouse_is_a_clear_error(tmp_path):
    with pytest.raises(TraceError, match="no warehouse at"):
        trace_warehouse(tmp_path / "none.duckdb", "P1", "2026-03-03T09:00:00+13:00", 3)
    assert not (tmp_path / "none.duckdb").exists()  # read-only: nothing is created
