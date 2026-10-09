import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pytest
from standdown_checker.rules import OriginSeen, Visit, evaluate, load_rules

ROOT = Path(__file__).resolve().parents[1]
RULES = load_rules(ROOT / "shared" / "standdown_rules.csv")


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def load_cases() -> dict[str, list[dict]]:
    cases: dict[str, list[dict]] = defaultdict(list)
    with open(ROOT / "shared" / "golden_cases.csv", newline="") as fh:
        for row in csv.DictReader(fh):
            cases[row["case_id"]].append(row)
    return dict(cases)


CASES = load_cases()


def to_visit(row: dict) -> Visit:
    return Visit(
        checkin_event_id=row["visit_id"],
        property_id=row["property_id"],
        property_type=row["property_type"],
        owner_company_id=row["owner_company_id"],
        checkin_time=ts(row["checkin_time"]),
        checkout_time=ts(row["checkout_time"]) if row["checkout_time"] else None,
    )


@pytest.mark.parametrize("case_id", sorted(CASES))
def test_golden_case(case_id):
    rows = CASES[case_id]
    target_row = next(r for r in rows if r["role"] == "target")
    history = [to_visit(r) for r in rows if r["role"] == "origin"]
    result = evaluate(to_visit(target_row), history, RULES)
    assert result.breach == (target_row["expected_breach"] == "true")
    assert (result.rule_id or "") == target_row["expected_rule_id"]
    assert (result.origin_checkin_event_id or "") == target_row["expected_origin_visit_id"]


def v(cid, pid, ptype, owner, checkin, checkout=None) -> Visit:
    return Visit(cid, pid, ptype, owner, ts(checkin), ts(checkout) if checkout else None)


def test_origins_seen_bound_is_sixteen_local_days():
    target = v("T", "P9", "poultry", "C1", "2026-03-20T09:00:00+13:00")
    inside = v("A", "P1", "office", "C2", "2026-03-04T09:00:00+13:00")
    outside = v("B", "P2", "office", "C2", "2026-03-03T09:00:00+13:00")
    result = evaluate(target, [inside, outside], RULES)
    assert [o.checkin_event_id for o in result.origins_seen] == ["A"]


def test_origins_seen_records_end_source_and_nights():
    target = v("T", "P9", "poultry", "C1", "2026-03-03T09:00:00+13:00")
    with_checkout = v(
        "A", "P1", "poultry", "C2", "2026-03-01T22:00:00+13:00", "2026-03-02T06:00:00+13:00"
    )
    without_checkout = v("B", "P2", "office", "C2", "2026-03-02T10:00:00+13:00")
    result = evaluate(target, [without_checkout, with_checkout], RULES)
    assert result.origins_seen == (
        OriginSeen("A", ts("2026-03-02T06:00:00+13:00"), "checkout", 1),
        OriginSeen("B", ts("2026-03-02T10:00:00+13:00"), "checkin", 1),
    )


def test_equal_checkin_times_are_ordered_by_event_id():
    target = v("b", "P9", "poultry", "C1", "2026-03-03T09:00:00+13:00")
    earlier_id = v("a", "P1", "poultry", "C2", "2026-03-03T09:00:00+13:00")
    later_id = v("z", "P2", "poultry", "C2", "2026-03-03T09:00:00+13:00")
    result = evaluate(target, [earlier_id, later_id], RULES)
    assert [o.checkin_event_id for o in result.origins_seen] == ["a"]
    assert result.origin_checkin_event_id == "a"


def test_target_itself_in_history_is_ignored():
    target = v("T", "P9", "poultry", "C1", "2026-03-03T09:00:00+13:00")
    result = evaluate(target, [target], RULES)
    assert result.origins_seen == ()
    assert result.breach is False
