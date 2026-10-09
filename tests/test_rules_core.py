from datetime import date, datetime
from pathlib import Path

import pytest
from standdown_checker.rules import Rule, load_rules, local_date, nights_between, pick_rule

RULES_CSV = Path(__file__).resolve().parents[1] / "shared" / "standdown_rules.csv"


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def test_local_date_uses_new_zealand_time():
    # 11:30 UTC on 2 March is 00:30 NZDT on 3 March.
    assert local_date(ts("2026-03-02T11:30:00+00:00")) == date(2026, 3, 3)


def test_local_date_rejects_naive_timestamps():
    with pytest.raises(ValueError):
        local_date(datetime(2026, 3, 3, 9, 0))


def test_nights_count_local_midnights_not_hours():
    # Two seconds apart, but a local midnight lies between them.
    assert nights_between(ts("2026-03-02T23:59:59+13:00"), ts("2026-03-03T00:00:01+13:00")) == 1


def test_nights_across_spring_forward_use_local_dates():
    # 46 elapsed hours across the 2026-09-27 clock change, yet two local midnights.
    assert nights_between(ts("2026-09-27T01:30:00+12:00"), ts("2026-09-29T00:30:00+13:00")) == 2


def test_nights_are_clipped_at_zero():
    assert nights_between(ts("2026-03-04T10:00:00+13:00"), ts("2026-03-03T20:00:00+13:00")) == 0


def test_load_rules_reads_the_shared_csv():
    rules = load_rules(RULES_CSV)
    assert {"R01", "R02", "R05"} <= {r.rule_id for r in rules}
    assert all(1 <= r.nights <= 14 for r in rules)


def test_load_rules_rejects_duplicate_keys(tmp_path):
    path = tmp_path / "rules.csv"
    path.write_text(
        "rule_id,from_type,to_type,scope,nights\nA,poultry,poultry,any,3\nB,poultry,poultry,any,2\n"
    )
    with pytest.raises(ValueError, match="duplicate"):
        load_rules(path)


def test_load_rules_rejects_nights_out_of_range(tmp_path):
    path = tmp_path / "rules.csv"
    path.write_text("rule_id,from_type,to_type,scope,nights\nA,poultry,poultry,any,15\n")
    with pytest.raises(ValueError, match="nights"):
        load_rules(path)


def test_pick_rule_prefers_the_specific_scope_over_any():
    rules = [
        Rule("ANY", "poultry", "poultry", "any", 3),
        Rule("INT", "poultry", "poultry", "internal", 1),
    ]
    assert pick_rule(rules, "poultry", "poultry", "internal").rule_id == "INT"
    assert pick_rule(rules, "poultry", "poultry", "external").rule_id == "ANY"
    assert pick_rule(rules, "office", "poultry", "external") is None
