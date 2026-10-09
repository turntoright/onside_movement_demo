from datetime import UTC, datetime

import pytest
from standdown_checker.events import (
    CheckIn,
    CheckOut,
    ValidationError,
    derived_id,
    iso,
    parse_event,
)


def checkin_detail(**overrides):
    detail = {
        "event_id": "e1",
        "person_id": "U1",
        "visitor_company_id": "V1",
        "property_id": "P1",
        "event_time": "2026-03-03T09:00:00+13:00",
        "received_time": "2026-03-03T09:00:05+13:00",
        "method": "app",
        "job_type": "delivery",
        "additional_visitors": 0,
        "answers": [],
    }
    detail.update(overrides)
    return detail


def test_parses_a_valid_checkin():
    event = parse_event("CheckIn", checkin_detail())
    assert isinstance(event, CheckIn)
    assert event.event_time.utcoffset().total_seconds() == 13 * 3600


def test_parses_a_valid_checkout():
    detail = {
        "event_id": "o1",
        "checkin_event_id": "e1",
        "person_id": "U1",
        "property_id": "P1",
        "event_time": "2026-03-03T10:00:00+13:00",
        "received_time": "2026-03-03T10:00:03+13:00",
        "method": "manual",
    }
    assert isinstance(parse_event("CheckOut", detail), CheckOut)


@pytest.mark.parametrize(
    "overrides",
    [
        {"person_id": ""},
        {"event_time": "2026-03-03T09:00:00"},  # no UTC offset
        {"event_time": "not a time"},
        {"method": "carrier_pigeon"},
        {"additional_visitors": True},
        {"additional_visitors": -1},
        {"poison": True},
    ],
)
def test_rejects_invalid_checkins(overrides):
    with pytest.raises(ValidationError):
        parse_event("CheckIn", checkin_detail(**overrides))


def test_rejects_unknown_detail_type_and_non_objects():
    with pytest.raises(ValidationError):
        parse_event("PlantMovement", checkin_detail())
    with pytest.raises(ValidationError):
        parse_event("CheckIn", ["not", "an", "object"])


def test_derived_ids_are_deterministic_and_distinct_per_kind():
    assert derived_id("eval", "e1") == derived_id("eval", "e1")
    assert derived_id("eval", "e1") != derived_id("alert", "e1")


def test_iso_writes_new_zealand_offset():
    assert iso(datetime(2026, 3, 2, 20, 0, tzinfo=UTC)) == "2026-03-03T09:00:00+13:00"
    assert iso(datetime(2026, 3, 2, 20, 0, tzinfo=UTC), ms=True) == "2026-03-03T09:00:00.000+13:00"
