import random
from datetime import UTC, datetime, timedelta

from standdown_checker.events import iso
from standdown_checker.rules import Visit, evaluate

from generator.config import SCALES
from generator.reference import build_reference
from generator.stream import PlannedVisit, plan_live, update_cursors
from tests.helpers import RULES

NOW = datetime(2026, 10, 9, 4, 0, tzinfo=UTC)
REF = build_reference(SCALES["ci"], random.Random(1))


def cursors_with(last_end, n_checkins=10):
    return {
        p.person_id: {
            "last_property_id": REF.properties[2].property_id,
            "last_checkin": iso(last_end - timedelta(hours=1)),
            "last_end": iso(last_end),
            "n_checkins": n_checkins,
        }
        for p in REF.people
    }


def test_live_visits_start_after_each_persons_last_end_and_inside_the_window():
    cursors = cursors_with(NOW - timedelta(minutes=20))
    visits, shortfall = plan_live(REF, cursors, random.Random(3), NOW, 20, 300)
    assert shortfall == 0 and len(visits) == 20
    for v in visits:
        last_end = datetime.fromisoformat(cursors[v.person_id]["last_end"])
        assert v.checkin >= last_end + timedelta(minutes=1)
        assert NOW - timedelta(minutes=30) <= v.checkin <= NOW
        assert v.departs <= NOW


def test_live_shortfall_when_no_one_has_room():
    visits, shortfall = plan_live(REF, cursors_with(NOW), random.Random(3), NOW, 10, 300)
    assert visits == []
    assert shortfall == 10


def test_live_respects_the_per_person_cap():
    cursors = cursors_with(NOW - timedelta(days=1), n_checkins=300)
    visits, shortfall = plan_live(REF, cursors, random.Random(3), NOW, 10, 300)
    assert visits == [] and shortfall == 10


def test_live_pairs_are_standdown_breaches():
    visits, _ = plan_live(REF, {}, random.Random(5), NOW, 10, 300)
    props = REF.property_map()
    by_person: dict[str, list[PlannedVisit]] = {}
    for v in visits:
        by_person.setdefault(v.person_id, []).append(v)
    pairs = [vs for vs in by_person.values() if len(vs) == 2]
    assert len(pairs) == 2  # round(0.2 * 10)
    for first, second in pairs:
        p1, p2 = props[first.property_id], props[second.property_id]
        origin = Visit("a", p1.property_id, p1.property_type, p1.owner_company_id,
                       first.checkin, first.departs)  # fmt: skip
        target = Visit("b", p2.property_id, p2.property_type, p2.owner_company_id, second.checkin)
        assert evaluate(target, [origin], RULES).breach


def test_update_cursors_tracks_the_last_visit():
    cursors: dict = {}
    t = NOW - timedelta(hours=2)
    update_cursors(
        cursors,
        [
            PlannedVisit(
                "U1", "P2", t + timedelta(hours=1), t + timedelta(hours=1, minutes=5), True
            ),
            PlannedVisit("U1", "P1", t, t + timedelta(minutes=5), True),
        ],
    )
    assert cursors["U1"]["last_property_id"] == "P2"
    assert cursors["U1"]["n_checkins"] == 2
