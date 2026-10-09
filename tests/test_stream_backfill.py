import random
import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest

from generator.config import SCALES
from generator.reference import build_reference, haversine_km
from generator.stream import BACKFILL_LATENESS, build_emissions, event_id, plan_backfill

ANCHOR = datetime(2026, 10, 1, tzinfo=UTC)
RUN_ID = str(uuid.UUID(int=1))


@pytest.fixture(scope="module")
def backfill():
    scale = SCALES["ci"]
    ref = build_reference(scale, random.Random(1))
    rng = random.Random("42:backfill")
    visits, plants = plan_backfill(ref, scale, rng, ANCHOR)
    emissions, counts = build_emissions(visits, plants, ref, RUN_ID, rng, ANCHOR, BACKFILL_LATENESS)
    return ref, visits, plants, emissions, counts


def parse(value):
    return datetime.fromisoformat(value)


def originals(emissions):
    first = {}
    for em in emissions:
        first.setdefault(em.detail["event_id"], em)
    return first


def test_is_deterministic():
    scale = SCALES["ci"]
    ref = build_reference(scale, random.Random(1))
    a = plan_backfill(ref, scale, random.Random("x"), ANCHOR)
    b = plan_backfill(ref, scale, random.Random("x"), ANCHOR)
    assert a == b


def test_visits_never_overlap_and_last_at_most_48_hours(backfill):
    _, visits, _, _, _ = backfill
    by_person = defaultdict(list)
    for v in visits:
        assert v.checkin <= v.departs <= v.checkin + timedelta(hours=48)
        by_person[v.person_id].append(v)
    for person_visits in by_person.values():
        person_visits.sort(key=lambda v: v.checkin)
        for prev, nxt in zip(person_visits, person_visits[1:], strict=False):
            assert nxt.checkin >= prev.departs


def test_per_person_cap_and_total_scale(backfill):
    _, visits, _, _, _ = backfill
    counts = defaultdict(int)
    for v in visits:
        counts[v.person_id] += 1
    assert max(counts.values()) <= 300
    assert 0.5 * SCALES["ci"].n_checkins <= len(visits) <= 1.1 * SCALES["ci"].n_checkins


def test_about_seventy_percent_of_moves_are_under_10_km(backfill):
    ref, visits, _, _, _ = backfill
    props = ref.property_map()
    by_person = defaultdict(list)
    for v in visits:
        by_person[v.person_id].append(v)
    near = total = 0
    for person_visits in by_person.values():
        person_visits.sort(key=lambda v: v.checkin)
        for a, b in zip(person_visits, person_visits[1:], strict=False):
            if a.property_id == b.property_id:
                continue
            pa, pb = props[a.property_id], props[b.property_id]
            total += 1
            near += haversine_km(pa.lat, pa.lon, pb.lat, pb.lon) < 10
    assert 0.6 <= near / total <= 0.9


def test_checkouts_belong_to_checkins_and_are_unique(backfill):
    _, _, _, emissions, _ = backfill
    first = originals(emissions)
    checkin_ids = {k for k, em in first.items() if em.detail_type == "CheckIn"}
    per_checkin = defaultdict(set)
    for em in first.values():
        if em.detail_type == "CheckOut":
            assert em.detail["checkin_event_id"] in checkin_ids
            per_checkin[em.detail["checkin_event_id"]].add(em.detail["event_id"])
    assert all(len(ids) == 1 for ids in per_checkin.values())


def test_lateness_bounds_and_clamping(backfill):
    _, _, _, emissions, _ = backfill
    for em in emissions:
        received = parse(em.detail["received_time"])
        assert received <= ANCHOR
        if em.detail_type in ("CheckIn", "CheckOut"):
            lag = received - parse(em.detail["event_time"])
            assert timedelta(0) <= lag <= timedelta(days=7)


def test_offline_share_is_about_five_percent(backfill):
    _, _, _, emissions, _ = backfill
    first = [em for em in originals(emissions).values() if em.detail_type == "CheckIn"]
    late = sum(
        parse(em.detail["received_time"]) - parse(em.detail["event_time"]) > timedelta(hours=1)
        for em in first
    )
    assert 0.03 <= late / len(first) <= 0.07


def test_duplicates_are_identical_and_within_seven_days(backfill):
    _, _, _, emissions, _ = backfill
    first = originals(emissions)
    duplicates = 0
    for em in emissions:
        orig = first[em.detail["event_id"]]
        if em is not orig:
            duplicates += 1
            assert em.detail == orig.detail and em.detail_type == orig.detail_type
            assert timedelta(0) <= em.send_time - orig.send_time <= timedelta(days=7)
    assert 0.01 <= duplicates / len(first) <= 0.03


def test_send_order_and_ids(backfill):
    _, _, _, emissions, counts = backfill
    times = [em.send_time for em in emissions]
    assert times == sorted(times)
    total = sum(counts.values())
    assert set(originals(emissions)) == {event_id(RUN_ID, s) for s in range(total)}
    assert counts["PlantMovement"] > 0
