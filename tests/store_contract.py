"""Store-agnostic StateStore contract tests (spec §6.1).

Each test takes a ``store_factory(properties)`` fixture returning a fresh, empty store.
Import the tests into a module that defines that fixture; Plan 2 adds the DynamoDB store.
"""

import pytest
from standdown_checker.store import ConflictError, EvaluationExistsError, UnknownPropertyError

PROPS = {"P1": {"property_type": "poultry", "owner_company_id": "C1"}}
VISIT = {"checkin_event_id": "A", "property_id": "P1"}


def test_missing_person_reads_as_version_zero(store_factory):
    assert store_factory(PROPS).load_person("U1") == (0, [])


def test_commit_creates_version_one_and_stores_evaluation(store_factory):
    store = store_factory(PROPS)
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A", "breach": False})
    assert store.load_person("U1") == (1, [VISIT])
    assert store.get_evaluation("A") == {"checkin_event_id": "A", "breach": False}


def test_missing_evaluation_reads_as_none(store_factory):
    assert store_factory(PROPS).get_evaluation("A") is None


def test_stale_version_conflicts(store_factory):
    store = store_factory(PROPS)
    store.commit("U1", 0, [VISIT], None)
    with pytest.raises(ConflictError):
        store.commit("U1", 0, [], None)


def test_missing_person_committed_with_nonzero_version_conflicts(store_factory):
    store = store_factory(PROPS)
    with pytest.raises(ConflictError):
        store.commit("U1", 1, [VISIT], {"checkin_event_id": "A"})
    assert store.load_person("U1") == (0, [])
    assert store.get_evaluation("A") is None


def test_existing_evaluation_cancels_the_whole_commit(store_factory):
    store = store_factory(PROPS)
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A"})
    with pytest.raises(EvaluationExistsError):
        store.commit("U1", 1, [], {"checkin_event_id": "A"})
    assert store.load_person("U1") == (1, [VISIT])


def test_both_conditions_failing_raises_evaluation_exists(store_factory):
    store = store_factory(PROPS)
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A"})
    with pytest.raises(EvaluationExistsError):
        store.commit("U1", 0, [], {"checkin_event_id": "A"})  # stale version AND duplicate


def test_stale_version_with_evaluation_leaves_everything_unchanged(store_factory):
    store = store_factory(PROPS)
    store.commit("U1", 0, [VISIT], None)
    with pytest.raises(ConflictError):
        store.commit("U1", 0, [], {"checkin_event_id": "B"})
    assert store.get_evaluation("B") is None
    assert store.load_person("U1") == (1, [VISIT])


def test_loaded_values_are_plain_ints(store_factory):
    store = store_factory(PROPS)
    store.commit("U1", 0, [{**VISIT, "n": 3}], {"checkin_event_id": "A", "nights": 2})
    version, visits = store.load_person("U1")
    for value in (version, visits[0]["n"], store.get_evaluation("A")["nights"]):
        assert isinstance(value, int) and not isinstance(value, bool)
    store.commit("U1", 1, visits, None)
    assert type(store.load_person("U1")[0]) is int


def test_returned_visits_are_copies(store_factory):
    store = store_factory(PROPS)
    store.commit("U1", 0, [dict(VISIT)], {"checkin_event_id": "A", "breach": False})
    _, visits = store.load_person("U1")
    visits[0]["property_id"] = "changed"
    store.get_evaluation("A")["breach"] = True
    assert store.load_person("U1")[1][0]["property_id"] == "P1"
    assert store.get_evaluation("A")["breach"] is False


def test_unknown_property_raises_validation_error(store_factory):
    with pytest.raises(UnknownPropertyError):
        store_factory(PROPS).get_property("NOPE")


def test_known_property_is_returned(store_factory):
    assert store_factory(PROPS).get_property("P1") == PROPS["P1"]
