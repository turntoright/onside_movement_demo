"""Stand-down rules (spec §4). Pure functions; stdlib only, so the Lambda zip stays small."""

from __future__ import annotations

import csv
from collections.abc import Iterable
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
