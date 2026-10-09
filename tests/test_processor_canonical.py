"""Re-emitted outbound events must be byte-identical (spec §6.1 idempotent re-emit)."""

from generator.local_sim import InMemoryStore
from tests.helpers import PROPERTIES, TickingClock, checkin, checkout, run, types


def _reversed(value):
    if isinstance(value, dict):
        return {k: _reversed(value[k]) for k in reversed(list(value))}
    if isinstance(value, list):
        return [_reversed(v) for v in value]
    return value


class KeyReorderingStore(InMemoryStore):
    """Returns dicts with reversed key order, as a different backend might."""

    def load_person(self, person_id):
        version, visits = super().load_person(person_id)
        return version, _reversed(visits)

    def get_evaluation(self, checkin_event_id):
        found = super().get_evaluation(checkin_event_id)
        return None if found is None else _reversed(found)


def _worked_example(store, clock):
    run(store, "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"), clock=clock)
    run(store, "CheckOut", checkout("A-out", "A", "P1", "2026-03-03T11:00:00+13:00"), clock=clock)
    run(store, "CheckIn", checkin("B", "P2", "2026-03-04T08:00:00+13:00"), clock=clock)
    return run(store, "CheckIn", checkin("C", "P3", "2026-03-04T14:00:00+13:00"), clock=clock)


def _bodies(outbound):
    return [o.detail_json() for o in outbound]


def test_duplicate_breaching_checkin_reemits_identical_json():
    store, clock = InMemoryStore(PROPERTIES), TickingClock()
    first = _worked_example(store, clock)
    assert types(first) == ["CheckInEvaluated", "HighRiskVisitDetected"]
    again = run(store, "CheckIn", checkin("C", "P3", "2026-03-04T14:00:00+13:00"), clock=clock)
    assert _bodies(again) == _bodies(first)


def test_duplicate_checkout_reemits_identical_json():
    store, clock = InMemoryStore(PROPERTIES), TickingClock()
    run(store, "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"), clock=clock)
    detail = checkout("A-out", "A", "P1", "2026-03-03T11:00:00+13:00")
    first = run(store, "CheckOut", detail, clock=clock)
    again = run(store, "CheckOut", detail, clock=clock)
    assert _bodies(again) == _bodies(first)


def test_key_order_of_the_store_does_not_change_the_json():
    plain = _worked_example(InMemoryStore(PROPERTIES), TickingClock())
    store, clock = KeyReorderingStore(PROPERTIES), TickingClock()
    first = _worked_example(store, clock)
    again = run(store, "CheckIn", checkin("C", "P3", "2026-03-04T14:00:00+13:00"), clock=clock)
    assert _bodies(first) == _bodies(plain)
    assert _bodies(again) == _bodies(first)


def test_detail_json_is_canonical():
    out = _worked_example(InMemoryStore(PROPERTIES), TickingClock())[0]
    assert out.detail_json().startswith('{"breach":true,"checkin_event_id":"C",')
