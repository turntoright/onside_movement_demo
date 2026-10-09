"""Event parsing and ids shared by the Lambda and the simulator (spec §3.5)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

NZ = ZoneInfo("Pacific/Auckland")
APP_SOURCE = "onside.app"
STANDDOWN_SOURCE = "onside.standdown"
CHECKIN_METHODS = frozenset({"app", "geofence", "qr", "kiosk", "link"})
CHECKOUT_METHODS = frozenset({"manual", "geofence_auto", "end_of_day"})
_NAMESPACE = uuid.UUID("6f1c2a52-3b8e-4f0a-9a52-0d1e7c4b9a10")


class ValidationError(ValueError):
    """The event can never be processed; after retries it belongs in the DLQ."""


def derived_id(kind: str, key: str) -> str:
    """Deterministic id for events the stand-down service emits (eval, co, alert)."""
    return str(uuid.uuid5(_NAMESPACE, f"{kind}:{key}"))


def iso(ts: datetime, *, ms: bool = False) -> str:
    return ts.astimezone(NZ).isoformat(timespec="milliseconds" if ms else "seconds")


def parse_ts(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValidationError(f"{field}: expected an ISO 8601 string")
    try:
        ts = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValidationError(f"{field}: {exc}") from exc
    if ts.tzinfo is None:
        raise ValidationError(f"{field}: missing UTC offset")
    return ts


@dataclass(frozen=True)
class CheckIn:
    event_id: str
    person_id: str
    visitor_company_id: str
    property_id: str
    event_time: datetime
    received_time: datetime
    method: str
    job_type: str
    additional_visitors: int


@dataclass(frozen=True)
class CheckOut:
    event_id: str
    checkin_event_id: str
    person_id: str
    property_id: str
    event_time: datetime
    received_time: datetime
    method: str


def _text(detail: dict, field: str) -> str:
    value = detail.get(field)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{field}: required non-empty string")
    return value


def parse_event(detail_type: str, detail: object) -> CheckIn | CheckOut:
    if not isinstance(detail, dict):
        raise ValidationError("detail must be a JSON object")
    if "poison" in detail:
        raise ValidationError("poison event")
    if detail_type == "CheckIn":
        method = _text(detail, "method")
        if method not in CHECKIN_METHODS:
            raise ValidationError(f"method: unknown {method!r}")
        visitors = detail.get("additional_visitors", 0)
        if not isinstance(visitors, int) or isinstance(visitors, bool) or visitors < 0:
            raise ValidationError("additional_visitors: expected a non-negative integer")
        return CheckIn(
            event_id=_text(detail, "event_id"),
            person_id=_text(detail, "person_id"),
            visitor_company_id=_text(detail, "visitor_company_id"),
            property_id=_text(detail, "property_id"),
            event_time=parse_ts(detail.get("event_time"), "event_time"),
            received_time=parse_ts(detail.get("received_time"), "received_time"),
            method=method,
            job_type=_text(detail, "job_type"),
            additional_visitors=visitors,
        )
    if detail_type == "CheckOut":
        method = _text(detail, "method")
        if method not in CHECKOUT_METHODS:
            raise ValidationError(f"method: unknown {method!r}")
        return CheckOut(
            event_id=_text(detail, "event_id"),
            checkin_event_id=_text(detail, "checkin_event_id"),
            person_id=_text(detail, "person_id"),
            property_id=_text(detail, "property_id"),
            event_time=parse_ts(detail.get("event_time"), "event_time"),
            received_time=parse_ts(detail.get("received_time"), "received_time"),
            method=method,
        )
    raise ValidationError(f"unsupported detail-type {detail_type!r}")
