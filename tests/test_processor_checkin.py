import pytest
from standdown_checker.events import ValidationError, derived_id

from generator.local_sim import InMemoryStore
from tests.helpers import PROPERTIES, TickingClock, checkin, checkout, run, types


def new_store() -> InMemoryStore:
    return InMemoryStore(PROPERTIES)


def test_first_checkin_commits_version_one_without_breach():
    store = new_store()
    out = run(store, "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"))
    assert types(out) == ["CheckInEvaluated"]
    evaluation = out[0].detail
    assert evaluation["event_id"] == derived_id("eval", "A")
    assert (evaluation["version_read"], evaluation["version_committed"]) == (0, 1)
    assert evaluation["breach"] is False
    assert evaluation["detected_time"] == "2026-03-03T09:00:00+13:00"
    assert store.load_person("U1")[0] == 1


def test_worked_example_emits_alert_with_property_ids():
    store = new_store()
    run(store, "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"))
    run(store, "CheckOut", checkout("A-out", "A", "P1", "2026-03-03T11:00:00+13:00"))
    run(store, "CheckIn", checkin("B", "P2", "2026-03-04T08:00:00+13:00"))
    out = run(store, "CheckIn", checkin("C", "P3", "2026-03-04T14:00:00+13:00"))
    assert types(out) == ["CheckInEvaluated", "HighRiskVisitDetected"]
    evaluation, alert = out[0].detail, out[1].detail
    assert evaluation["rule_id"] == "R02"
    assert evaluation["origin_checkin_event_id"] == "A"
    assert (evaluation["nights_between"], evaluation["required_nights"]) == (1, 3)
    assert evaluation["version_read"] == 3
    assert alert["event_id"] == derived_id("alert", "C")
    assert (alert["origin_property_id"], alert["dest_property_id"]) == ("P1", "P3")
    assert alert["processed_at"] == evaluation["processed_at"]
    seen = {o["checkin_event_id"]: o for o in evaluation["origins_seen"]}
    assert seen["A"]["end_source"] == "checkout"
    assert seen["B"]["end_source"] == "checkin"  # B never checked out


def test_late_origin_is_inserted_in_order_and_does_not_reevaluate_later_checkins():
    store = new_store()
    out_c = run(store, "CheckIn", checkin("C", "P3", "2026-03-04T14:00:00+13:00"))
    assert out_c[0].detail["breach"] is False
    late = checkin("A", "P1", "2026-03-03T09:00:00+13:00", received="2026-03-05T09:00:00+13:00")
    out_a = run(store, "CheckIn", late)
    assert out_a[0].detail["breach"] is False
    _, visits = store.load_person("U1")
    assert [v["checkin_event_id"] for v in visits] == ["A", "C"]
    assert store.get_evaluation("C")["breach"] is False  # the miss batch will report


def test_duplicate_checkin_reemits_identical_bodies_without_committing():
    store = new_store()
    clock = TickingClock()
    run(store, "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"), clock=clock)
    first = run(store, "CheckIn", checkin("C", "P3", "2026-03-03T14:00:00+13:00"), clock=clock)
    version = store.load_person("U1")[0]
    second = run(store, "CheckIn", checkin("C", "P3", "2026-03-03T14:00:00+13:00"), clock=clock)
    assert second == first
    assert store.load_person("U1")[0] == version


def test_unknown_property_is_a_validation_error():
    with pytest.raises(ValidationError):
        run(new_store(), "CheckIn", checkin("X", "NOPE", "2026-03-03T09:00:00+13:00"))
