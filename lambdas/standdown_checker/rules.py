"""Stand-down rules (spec §4). Pure functions; stdlib only, so the Lambda zip stays small."""

from __future__ import annotations

import csv
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

NZ = ZoneInfo("Pacific/Auckland")
WINDOW_NIGHTS = 14
ORIGINS_SEEN_BOUND_DAYS = 16
SCOPES = ("internal", "external", "any")


@dataclass(frozen=True)
class Rule:
    rule_id: str
    from_type: str
    to_type: str
    scope: str
    nights: int


@dataclass(frozen=True)
class Visit:
    checkin_event_id: str
    property_id: str
    property_type: str
    owner_company_id: str
    checkin_time: datetime
    checkout_time: datetime | None = None

    @property
    def standdown_end(self) -> datetime:
        """§4.1: the check-out if known (never before the check-in), else the check-in."""
        if self.checkout_time is None:
            return self.checkin_time
        return max(self.checkin_time, self.checkout_time)

    @property
    def order_key(self) -> tuple[datetime, str]:
        return (self.checkin_time, self.checkin_event_id)


def local_date(ts: datetime) -> date:
    if ts.tzinfo is None:
        raise ValueError(f"timestamp has no UTC offset: {ts!r}")
    return ts.astimezone(NZ).date()


def nights_between(end: datetime, checkin: datetime) -> int:
    """§4.2: local midnights between the end of one visit and the next check-in."""
    return max(0, (local_date(checkin) - local_date(end)).days)


def load_rules(path: str | Path) -> list[Rule]:
    rules: list[Rule] = []
    seen: set[tuple[str, str, str]] = set()
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            rule = Rule(
                rule_id=row["rule_id"],
                from_type=row["from_type"],
                to_type=row["to_type"],
                scope=row["scope"],
                nights=int(row["nights"]),
            )
            if rule.scope not in SCOPES:
                raise ValueError(f"{rule.rule_id}: unknown scope {rule.scope!r}")
            if not 1 <= rule.nights <= WINDOW_NIGHTS:
                raise ValueError(f"{rule.rule_id}: nights must be 1..{WINDOW_NIGHTS}")
            key = (rule.from_type, rule.to_type, rule.scope)
            if key in seen:
                raise ValueError(f"duplicate rule for {key}")
            seen.add(key)
            rules.append(rule)
    return rules


def pick_rule(rules: Iterable[Rule], from_type: str, to_type: str, scope: str) -> Rule | None:
    """§4.5: the matching internal/external rule beats `any`; None if nothing matches."""
    fallback = None
    for rule in rules:
        if rule.from_type != from_type or rule.to_type != to_type:
            continue
        if rule.scope == scope:
            return rule
        if rule.scope == "any":
            fallback = rule
    return fallback


@dataclass(frozen=True)
class OriginSeen:
    checkin_event_id: str
    end: datetime
    end_source: str
    nights_between: int


@dataclass(frozen=True)
class Evaluation:
    breach: bool
    origins_seen: tuple[OriginSeen, ...]
    rule_id: str | None = None
    origin_checkin_event_id: str | None = None
    origin_property_id: str | None = None
    required_nights: int | None = None
    nights_between: int | None = None


def scope_of(origin: Visit, target: Visit) -> str:
    """§4.4: internal when both properties have the same owner; the visitor plays no part."""
    return "internal" if origin.owner_company_id == target.owner_company_id else "external"


def evaluate(target: Visit, history: Iterable[Visit], rules: Sequence[Rule]) -> Evaluation:
    """§4: one result per target check-in; also records every origin inside the 16-day bound."""
    candidates = sorted(
        (
            v
            for v in history
            if v.checkin_event_id != target.checkin_event_id
            and v.order_key < target.order_key
            and v.property_id != target.property_id
        ),
        key=lambda v: v.order_key,
    )
    target_date = local_date(target.checkin_time)
    seen: list[OriginSeen] = []
    best_key = None
    best = None
    for origin in candidates:
        nights = nights_between(origin.standdown_end, target.checkin_time)
        if (target_date - local_date(origin.checkin_time)).days <= ORIGINS_SEEN_BOUND_DAYS:
            seen.append(
                OriginSeen(
                    checkin_event_id=origin.checkin_event_id,
                    end=origin.standdown_end,
                    end_source="checkout" if origin.checkout_time is not None else "checkin",
                    nights_between=nights,
                )
            )
        if nights > WINDOW_NIGHTS:
            continue
        rule = pick_rule(
            rules, origin.property_type, target.property_type, scope_of(origin, target)
        )
        if rule is None or nights >= rule.nights:
            continue
        key = (rule.nights, rule.nights - nights, origin.standdown_end, origin.checkin_event_id)
        if best_key is None or key > best_key:
            best_key, best = key, (origin, rule, nights)
    if best is None:
        return Evaluation(breach=False, origins_seen=tuple(seen))
    origin, rule, nights = best
    return Evaluation(
        breach=True,
        origins_seen=tuple(seen),
        rule_id=rule.rule_id,
        origin_checkin_event_id=origin.checkin_event_id,
        origin_property_id=origin.property_id,
        required_nights=rule.nights,
        nights_between=nights,
    )
