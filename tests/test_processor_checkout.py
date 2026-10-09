import pytest
from standdown_checker.events import derived_id
from standdown_checker.processor import RetryExhaustedError, process_event
from standdown_checker.store import ConflictError, EvaluationExistsError

from generator.local_sim import InMemoryStore
from tests.helpers import PROPERTIES, RULES, TickingClock, checkin, checkout, run, types


def new_store() -> InMemoryStore:
    return InMemoryStore(PROPERTIES)


def test_orphan_checkout_creates_placeholder_then_checkin_is_evaluated():
    store = new_store()
    out_co = run(store, "CheckOut", checkout("A-out", "A", "P1", "2026-03-02T06:00:00+13:00"))
    assert types(out_co) == ["CheckOutApplied"]
    assert out_co[0].detail["event_id"] == derived_id("co", "A-out")
    assert out_co[0].detail["version_committed"] == 1
    out_ci = run(store, "CheckIn", checkin("A", "P1", "2026-03-01T22:00:00+13:00"))
    assert types(out_ci) == ["CheckInEvaluated"]  # not mistaken for a duplicate
    _, visits = store.load_person("U1")
    assert visits[0]["checkin_time"] == "2026-03-01T22:00:00+13:00"
    assert visits[0]["checkout_time"] == "2026-03-02T06:00:00+13:00"


def test_placeholder_is_never_an_origin():
    store = new_store()
    run(store, "CheckOut", checkout("X-out", "X", "P1", "2026-03-03T10:00:00+13:00"))
    out = run(store, "CheckIn", checkin("C", "P3", "2026-03-03T14:00:00+13:00"))
    assert out[0].detail["breach"] is False
    assert out[0].detail["origins_seen"] == []


def test_duplicate_checkout_before_checkin_reemits_same_applied_event():
    store = new_store()
    clock = TickingClock()
    co = checkout("A-out", "A", "P1", "2026-03-02T06:00:00+13:00")
    first = run(store, "CheckOut", co, clock=clock)
    assert run(store, "CheckOut", co, clock=clock) == first
    run(store, "CheckIn", checkin("A", "P1", "2026-03-01T22:00:00+13:00"), clock=clock)
    assert run(store, "CheckOut", co, clock=clock) == first


def test_earliest_checkout_is_kept_and_each_is_applied_once():
    store = new_store()
    run(store, "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"))
    late = run(store, "CheckOut", checkout("A-out2", "A", "P1", "2026-03-03T18:00:00+13:00"))
    early = run(store, "CheckOut", checkout("A-out1", "A", "P1", "2026-03-03T11:00:00+13:00"))
    _, visits = store.load_person("U1")
    assert visits[0]["checkout_event_id"] == "A-out1"
    assert late[0].detail["version_committed"] == 2
    assert early[0].detail["version_committed"] == 3


def test_checkout_end_feeds_the_standdown_rule():
    store = new_store()
    run(store, "CheckIn", checkin("A", "P5", "2026-03-01T22:00:00+13:00"))
    run(store, "CheckOut", checkout("A-out", "A", "P5", "2026-03-02T06:00:00+13:00"))
    out = run(store, "CheckIn", checkin("B", "P3", "2026-03-03T09:00:00+13:00"))
    assert out[0].detail["breach"] is True
    assert out[0].detail["rule_id"] == "R05"  # pork -> poultry, 1 night < 2


def test_version_conflict_retries_with_fresh_state():
    class ConflictOnce(InMemoryStore):
        armed = True

        def commit(self, person_id, version_read, visits, evaluation):
            if self.armed:
                self.armed = False
                run(self, "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"))
            super().commit(person_id, version_read, visits, evaluation)

    store = ConflictOnce(PROPERTIES)
    sleeps: list[float] = []
    out = run(
        store, "CheckIn", checkin("C", "P3", "2026-03-04T14:00:00+13:00"), sleep=sleeps.append
    )
    assert len(sleeps) == 1
    assert out[0].detail["version_read"] == 1
    assert out[0].detail["breach"] is True


def test_retry_exhausted_after_three_conflicts():
    class AlwaysConflict(InMemoryStore):
        def commit(self, *args, **kwargs):
            raise ConflictError("busy")

    sleeps: list[float] = []
    with pytest.raises(RetryExhaustedError):
        process_event(
            "CheckIn",
            checkin("A", "P1", "2026-03-03T09:00:00+13:00"),
            AlwaysConflict(PROPERTIES),
            RULES,
            TickingClock(),
            sleep=sleeps.append,
        )
    assert len(sleeps) == 3


def test_evaluation_exists_race_takes_the_already_processed_path():
    class Race(InMemoryStore):
        armed = True

        def commit(self, person_id, version_read, visits, evaluation):
            if self.armed and evaluation is not None:
                self.armed = False
                winner = dict(evaluation, processed_at="2026-03-09T00:00:00.000+13:00")
                super().commit(person_id, version_read, visits, winner)
                raise EvaluationExistsError(evaluation["checkin_event_id"])
            super().commit(person_id, version_read, visits, evaluation)

    out = run(Race(PROPERTIES), "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"))
    assert out[0].detail["processed_at"] == "2026-03-09T00:00:00.000+13:00"
