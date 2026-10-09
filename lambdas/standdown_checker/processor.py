"""Stand-down processing over a StateStore (spec §6.1). Order-independent by design."""

from __future__ import annotations

import copy
import json
import random
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime

from . import rules as R
from .events import STANDDOWN_SOURCE, CheckIn, CheckOut, derived_id, iso, parse_event
from .store import ConflictError, EvaluationExistsError, StateStore

MAX_CONFLICT_RETRIES = 3


def serialize_detail(detail: dict) -> str:
    """The one canonical JSON form for every event the processor emits."""
    return json.dumps(detail, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class Outbound:
    detail_type: str
    detail: dict
    source: str = STANDDOWN_SOURCE

    def detail_json(self) -> str:
        return serialize_detail(self.detail)


class RetryExhaustedError(Exception):
    """Version conflicts kept happening; fail the record so SQS redelivers it."""


def process_event(
    detail_type: str,
    detail: object,
    store: StateStore,
    rules: Sequence[R.Rule],
    now: Callable[[], datetime],
    *,
    sleep: Callable[[float], None] = time.sleep,
    jitter: Callable[[], float] = random.random,
) -> list[Outbound]:
    event = parse_event(detail_type, detail)
    conflicts = 0
    # Each pass restarts from step 2 (load), including the duplicate check.
    for _ in range(MAX_CONFLICT_RETRIES + 10):
        try:
            if isinstance(event, CheckIn):
                return _process_checkin(event, store, rules, now)
            return _process_checkout(event, store, now)
        except EvaluationExistsError:
            continue  # another worker committed it; the next pass re-emits the stored result
        except ConflictError:
            conflicts += 1
            if conflicts > MAX_CONFLICT_RETRIES:
                raise RetryExhaustedError(f"{event.event_id}: {conflicts} conflicts") from None
            sleep(jitter() * 0.05 * 2**conflicts)
    raise RetryExhaustedError(f"{event.event_id}: did not converge")


def _parse(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


def _sort_key(item: dict) -> tuple[bool, float, str]:
    ts = _parse(item["checkin_time"])
    return (ts is None, ts.timestamp() if ts else 0.0, item["checkin_event_id"])


def _find(visits: list[dict], checkin_event_id: str | None) -> dict | None:
    return next((v for v in visits if v["checkin_event_id"] == checkin_event_id), None)


def _new_item(checkin_event_id: str, property_id: str) -> dict:
    return {
        "checkin_event_id": checkin_event_id,
        "property_id": property_id,
        "property_type": None,
        "owner_company_id": None,
        "checkin_time": None,
        "checkout_event_id": None,
        "checkout_time": None,
        "applied_checkouts": {},
    }


def _to_rule_visit(item: dict) -> R.Visit:
    return R.Visit(
        checkin_event_id=item["checkin_event_id"],
        property_id=item["property_id"],
        property_type=item["property_type"],
        owner_company_id=item["owner_company_id"],
        checkin_time=_parse(item["checkin_time"]),
        checkout_time=_parse(item["checkout_time"]),
    )


def _process_checkin(
    ci: CheckIn, store: StateStore, rules: Sequence[R.Rule], now: Callable[[], datetime]
) -> list[Outbound]:
    stored = store.get_evaluation(ci.event_id)
    if stored is not None:
        _, visits = store.load_person(ci.person_id)
        return _checkin_outbound(stored, visits)

    version_read, visits = store.load_person(ci.person_id)
    prop = store.get_property(ci.property_id)
    target = R.Visit(
        ci.event_id, ci.property_id, prop["property_type"], prop["owner_company_id"], ci.event_time
    )
    # Placeholders (no observed check-in) are never origins.
    history = [_to_rule_visit(v) for v in visits if v["checkin_time"] is not None]
    result = R.evaluate(target, history, rules)

    visits = copy.deepcopy(visits)
    item = _find(visits, ci.event_id)
    if item is None:
        item = _new_item(ci.event_id, ci.property_id)
        visits.append(item)
    item.update(
        property_id=ci.property_id,
        property_type=prop["property_type"],
        owner_company_id=prop["owner_company_id"],
        checkin_time=iso(ci.event_time),
    )
    visits.sort(key=_sort_key)

    evaluation = {
        "event_id": derived_id("eval", ci.event_id),
        "checkin_event_id": ci.event_id,
        "person_id": ci.person_id,
        "version_read": version_read,
        "version_committed": version_read + 1,
        "detected_time": iso(ci.received_time),
        "processed_at": iso(now(), ms=True),
        "origins_seen": [
            {
                "checkin_event_id": o.checkin_event_id,
                "end": iso(o.end),
                "end_source": o.end_source,
                "nights_between": o.nights_between,
            }
            for o in result.origins_seen
        ],
        "breach": result.breach,
        "rule_id": result.rule_id,
        "origin_checkin_event_id": result.origin_checkin_event_id,
        "required_nights": result.required_nights,
        "nights_between": result.nights_between,
    }
    store.commit(ci.person_id, version_read, visits, evaluation)
    return _checkin_outbound(evaluation, visits)


def _checkin_outbound(evaluation: dict, visits: list[dict]) -> list[Outbound]:
    out = [Outbound("CheckInEvaluated", copy.deepcopy(evaluation))]
    if evaluation["breach"]:
        origin = _find(visits, evaluation["origin_checkin_event_id"])
        dest = _find(visits, evaluation["checkin_event_id"])
        out.append(
            Outbound(
                "HighRiskVisitDetected",
                {
                    "event_id": derived_id("alert", evaluation["checkin_event_id"]),
                    "checkin_event_id": evaluation["checkin_event_id"],
                    "person_id": evaluation["person_id"],
                    "origin_property_id": origin["property_id"],
                    "dest_property_id": dest["property_id"],
                    "origin_checkin_event_id": evaluation["origin_checkin_event_id"],
                    "nights_between": evaluation["nights_between"],
                    "required_nights": evaluation["required_nights"],
                    "rule_id": evaluation["rule_id"],
                    "detected_time": evaluation["detected_time"],
                    "processed_at": evaluation["processed_at"],
                },
            )
        )
    return out


def _process_checkout(
    co: CheckOut, store: StateStore, now: Callable[[], datetime]
) -> list[Outbound]:
    version_read, visits = store.load_person(co.person_id)
    item = _find(visits, co.checkin_event_id)
    if item is not None and co.event_id in item["applied_checkouts"]:
        applied = item["applied_checkouts"][co.event_id]
        return [_applied(co, applied["version"], applied["processed_at"])]

    visits = copy.deepcopy(visits)
    item = _find(visits, co.checkin_event_id)
    if item is None:
        item = _new_item(co.checkin_event_id, co.property_id)  # placeholder until the check-in
        visits.append(item)
    if item["checkout_time"] is None or co.event_time < _parse(item["checkout_time"]):
        item["checkout_event_id"] = co.event_id  # keep the earliest check-out
        item["checkout_time"] = iso(co.event_time)
    version_committed = version_read + 1
    processed_at = iso(now(), ms=True)
    item["applied_checkouts"][co.event_id] = {
        "version": version_committed,
        "processed_at": processed_at,
    }
    visits.sort(key=_sort_key)
    store.commit(co.person_id, version_read, visits, None)
    return [_applied(co, version_committed, processed_at)]


def _applied(co: CheckOut, version: int, processed_at: str) -> Outbound:
    return Outbound(
        "CheckOutApplied",
        {
            "event_id": derived_id("co", co.event_id),
            "checkout_event_id": co.event_id,
            "checkin_event_id": co.checkin_event_id,
            "person_id": co.person_id,
            "version_committed": version,
            "processed_at": processed_at,
        },
    )
