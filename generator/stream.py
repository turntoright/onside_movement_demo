"""Visit planning and event emission (spec §3.2, §3.3)."""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from standdown_checker.events import APP_SOURCE, NZ, iso

from .config import Scale
from .reference import NEAR_KM, ROLES, Person, Property, Reference, haversine_km

OFFLINE_P = 0.05
DUPLICATE_P = 0.02
NO_CHECKOUT_P = 0.08
NEAR_P = 0.7
CHECKIN_METHODS = (("app", 0.45), ("geofence", 0.35), ("qr", 0.10), ("kiosk", 0.07), ("link", 0.03))
CHECKOUT_METHODS = (("geofence_auto", 0.5), ("manual", 0.4), ("end_of_day", 0.1))
JOB_TYPES = (
    "delivery", "maintenance", "vet_visit", "feed_delivery", "stock_cartage",
    "spraying", "harvest", "inspection", "consulting",
)  # fmt: skip
MATERIAL_TYPES = ("budwood", "rootstock", "grafted_plants", "pollen")
LIVE_WINDOW = timedelta(minutes=30)
LIVE_MARGIN = timedelta(minutes=2)
PAIR_SPAN = timedelta(minutes=15)


@dataclass(frozen=True)
class PlannedVisit:
    person_id: str
    property_id: str
    checkin: datetime
    departs: datetime
    has_checkout: bool


@dataclass(frozen=True)
class PlannedPlantMovement:
    source_property_id: str
    dest_property_id: str
    movement_date: date
    material_type: str
    volume: int


@dataclass(frozen=True)
class Emission:
    send_time: datetime
    detail_type: str
    detail: dict
    source: str = APP_SOURCE


@dataclass(frozen=True)
class Lateness:
    max_offline: timedelta
    max_duplicate_delay: timedelta


BACKFILL_LATENESS = Lateness(timedelta(days=7), timedelta(days=7))
LIVE_LATENESS = Lateness(timedelta(minutes=10), timedelta(minutes=1))


def event_id(run_id: str, seq: int) -> str:
    return str(uuid.uuid5(uuid.UUID(run_id), str(seq)))


def _seconds(ts: datetime) -> datetime:
    return ts.replace(microsecond=0)


def _weighted(rng: random.Random, options) -> str:
    names, weights = zip(*options, strict=True)
    return rng.choices(names, weights)[0]


def _duration(rng: random.Random) -> timedelta:
    u = rng.random()
    if u < 0.70:
        hours = rng.uniform(0.25, 3)
    elif u < 0.95:
        hours = rng.uniform(3, 10)
    else:
        hours = rng.uniform(10, 47.5)
    return timedelta(hours=hours)


class Mover:
    """Chooses the next property: mostly nearby and of a type the person's role works with."""

    def __init__(self, ref: Reference, rng: random.Random) -> None:
        self.rng = rng
        self.near = {
            p.property_id: [
                q
                for q in ref.properties
                if q.property_id != p.property_id
                and haversine_km(p.lat, p.lon, q.lat, q.lon) <= NEAR_KM
            ]
            for p in ref.properties
        }
        self.pools = {
            role: [p for p in ref.properties if p.property_type in types] or list(ref.properties)
            for role, types in ROLES.items()
        }

    def next(self, person: Person, current: str | None) -> Property:
        pool = self.pools[person.role]
        if current is not None:
            types = {p.property_type for p in pool}
            near = [p for p in self.near.get(current, []) if p.property_type in types]
            if near and self.rng.random() < NEAR_P:
                return self.rng.choice(near)
        choices = [p for p in pool if p.property_id != current] or pool
        return self.rng.choice(choices)


def plan_backfill(
    ref: Reference, scale: Scale, rng: random.Random, anchor: datetime
) -> tuple[list[PlannedVisit], list[PlannedPlantMovement]]:
    start = anchor - timedelta(days=365)
    mover = Mover(ref, rng)
    shares = [rng.paretovariate(1.5) for _ in ref.people]
    total_share = sum(shares)
    visits: list[PlannedVisit] = []
    for person, share in zip(ref.people, shares, strict=True):
        k = round(scale.n_checkins * share / total_share)
        k = max(1, min(scale.max_checkins_per_person, k))
        mean_gap = (anchor - start).total_seconds() / k
        t = start + timedelta(seconds=rng.uniform(0, mean_gap))
        current: str | None = None
        for _ in range(k):
            if t >= anchor:
                break
            prop = mover.next(person, current)
            checkin = _seconds(t)
            departs = _seconds(checkin + _duration(rng))
            has_checkout = departs <= anchor and rng.random() >= NO_CHECKOUT_P
            visits.append(
                PlannedVisit(person.person_id, prop.property_id, checkin, departs, has_checkout)
            )
            current = prop.property_id
            gap = rng.expovariate(1.0 / max(mean_gap - 4 * 3600, 3600))
            t = departs + timedelta(seconds=max(gap, 600))
    plants = _plan_plants(ref, rng, start, anchor, max(1, scale.n_checkins // 40))
    return visits, plants


def _plan_plants(
    ref: Reference, rng: random.Random, start: datetime, end: datetime, count: int
) -> list[PlannedPlantMovement]:
    plant_props = [p for p in ref.properties if p.property_type in ("kiwifruit", "vineyard")]
    out: list[PlannedPlantMovement] = []
    for _ in range(count):
        if len(plant_props) < 2:
            break
        src = rng.choice(plant_props)
        same = [
            p
            for p in plant_props
            if p.property_type == src.property_type and p.property_id != src.property_id
        ]
        if not same:
            continue
        dst = rng.choice(same)
        moment = start + timedelta(seconds=rng.uniform(0, (end - start).total_seconds()))
        out.append(
            PlannedPlantMovement(
                src.property_id,
                dst.property_id,
                moment.astimezone(NZ).date(),
                rng.choice(MATERIAL_TYPES),
                rng.randint(10, 500),
            )
        )
    return out


def plan_live(
    ref: Reference,
    cursors: dict[str, dict],
    rng: random.Random,
    now: datetime,
    n: int,
    cap: int,
) -> tuple[list[PlannedVisit], int]:
    """§3.2 live mode. Returns the visits and how many requested check-ins could not fit."""
    window_start, latest = now - LIVE_WINDOW, now - LIVE_MARGIN
    mover = Mover(ref, rng)
    people = list(ref.people)
    rng.shuffle(people)
    poultry = [p for p in ref.properties if p.property_type == "poultry"]
    pairs = min(round(0.2 * n), n // 2)
    regular = n - 2 * pairs
    visits: list[PlannedVisit] = []
    used: set[str] = set()

    def earliest(person: Person) -> datetime:
        cursor = cursors.get(person.person_id)
        if cursor and cursor["last_end"]:
            last_end = datetime.fromisoformat(cursor["last_end"]) + timedelta(minutes=1)
            return max(window_start, last_end)
        return window_start

    def remaining(person: Person) -> int:
        cursor = cursors.get(person.person_id)
        return cap - (cursor["n_checkins"] if cursor else 0)

    for person in people:  # deliberate breaches: two poultry sites with different owners
        if pairs == 0:
            break
        start = earliest(person)
        if remaining(person) < 2 or start + PAIR_SPAN > latest:
            continue
        p1 = rng.choice(poultry)
        others = [p for p in poultry if p.owner_company_id != p1.owner_company_id]
        if not others:
            break
        p2 = rng.choice(others)
        span = (latest - PAIR_SPAN - start).total_seconds()
        t1 = _seconds(start + timedelta(seconds=rng.uniform(0, span)))
        visits.append(
            PlannedVisit(person.person_id, p1.property_id, t1, t1 + timedelta(minutes=5), True)
        )
        visits.append(
            PlannedVisit(
                person.person_id,
                p2.property_id,
                t1 + timedelta(minutes=8),
                t1 + timedelta(minutes=13),
                True,
            )
        )
        used.add(person.person_id)
        pairs -= 1

    for person in people:
        if regular == 0:
            break
        if person.person_id in used:
            continue
        start = earliest(person)
        if remaining(person) < 1 or start + timedelta(minutes=3) > latest:
            continue
        current = (cursors.get(person.person_id) or {}).get("last_property_id")
        prop = mover.next(person, current)
        span = (latest - timedelta(minutes=3) - start).total_seconds()
        checkin = _seconds(start + timedelta(seconds=rng.uniform(0, span)))
        departs = _seconds(checkin + timedelta(seconds=rng.uniform(60, 600)))
        has_checkout = departs <= latest
        visits.append(
            PlannedVisit(
                person.person_id,
                prop.property_id,
                checkin,
                departs if has_checkout else now,  # open visit: physical end unknown
                has_checkout,
            )
        )
        used.add(person.person_id)
        regular -= 1
    return visits, 2 * pairs + regular


def build_emissions(
    visits: list[PlannedVisit],
    plants: list[PlannedPlantMovement],
    ref: Reference,
    run_id: str,
    rng: random.Random,
    run_end: datetime,
    lateness: Lateness,
) -> tuple[list[Emission], dict[str, int]]:
    people = {p.person_id: p for p in ref.people}
    counts = {"CheckIn": 0, "CheckOut": 0, "PlantMovement": 0}
    seq = 0

    def next_id() -> str:
        nonlocal seq
        eid = event_id(run_id, seq)
        seq += 1
        return eid

    min_offline = min(3600.0, lateness.max_offline.total_seconds() / 10)

    def received(event_time: datetime) -> datetime:
        if rng.random() < OFFLINE_P:
            delay = rng.uniform(min_offline, lateness.max_offline.total_seconds())
        else:
            delay = rng.uniform(1, 30)
        return min(_seconds(event_time + timedelta(seconds=delay)), run_end)

    originals: list[Emission] = []
    for v in visits:
        cid = next_id()
        r_in = received(v.checkin)
        originals.append(
            Emission(
                r_in,
                "CheckIn",
                {
                    "event_id": cid,
                    "person_id": v.person_id,
                    "visitor_company_id": people[v.person_id].visitor_company_id,
                    "property_id": v.property_id,
                    "event_time": iso(v.checkin),
                    "received_time": iso(r_in),
                    "method": _weighted(rng, CHECKIN_METHODS),
                    "job_type": rng.choice(JOB_TYPES),
                    "additional_visitors": 0 if rng.random() < 0.85 else rng.randint(1, 3),
                    "answers": [
                        {"question_id": "Q-BIO-1", "answer": rng.choice(("no", "no", "no", "yes"))}
                    ],
                },
            )
        )
        counts["CheckIn"] += 1
        if v.has_checkout:
            oid = next_id()
            r_out = received(v.departs)
            originals.append(
                Emission(
                    r_out,
                    "CheckOut",
                    {
                        "event_id": oid,
                        "checkin_event_id": cid,
                        "person_id": v.person_id,
                        "property_id": v.property_id,
                        "event_time": iso(v.departs),
                        "received_time": iso(r_out),
                        "method": _weighted(rng, CHECKOUT_METHODS),
                    },
                )
            )
            counts["CheckOut"] += 1
    for pm in plants:
        pid = next_id()
        day_end = datetime.combine(pm.movement_date + timedelta(days=1), time.min, tzinfo=NZ)
        r_pm = min(
            _seconds(day_end.astimezone(UTC) + timedelta(seconds=rng.uniform(0, 7200))), run_end
        )
        originals.append(
            Emission(
                r_pm,
                "PlantMovement",
                {
                    "event_id": pid,
                    "source_property_id": pm.source_property_id,
                    "dest_property_id": pm.dest_property_id,
                    "movement_date": pm.movement_date.isoformat(),
                    "material_type": pm.material_type,
                    "volume": pm.volume,
                    "received_time": iso(r_pm),
                },
            )
        )
        counts["PlantMovement"] += 1

    emissions = list(originals)
    max_dup = lateness.max_duplicate_delay.total_seconds()
    for em in originals:
        if rng.random() < DUPLICATE_P:
            when = min(_seconds(em.send_time + timedelta(seconds=rng.uniform(1, max_dup))), run_end)
            emissions.append(Emission(when, em.detail_type, em.detail, em.source))
    emissions.sort(key=lambda e: e.send_time)  # stable: ties keep creation order
    return emissions, counts


def update_cursors(cursors: dict[str, dict], visits: list[PlannedVisit]) -> None:
    """Remember each person's last visit so the next live run continues their timeline."""
    for v in sorted(visits, key=lambda v: v.checkin):
        cursor = cursors.setdefault(
            v.person_id,
            {"last_property_id": None, "last_checkin": None, "last_end": None, "n_checkins": 0},
        )
        cursor["last_property_id"] = v.property_id
        cursor["last_checkin"] = iso(v.checkin)
        cursor["last_end"] = iso(v.departs)
        cursor["n_checkins"] += 1
