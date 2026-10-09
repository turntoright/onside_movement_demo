"""Shared builders for processor tests."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

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
