import duckdb
import pytest

from tracing.cli import main


@pytest.fixture()
def db(tmp_path):
    path = tmp_path / "w.duckdb"
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
