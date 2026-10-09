import pytest
from standdown_checker.store import ConflictError, EvaluationExistsError, UnknownPropertyError

from generator.local_sim import InMemoryStore

PROPS = {"P1": {"property_type": "poultry", "owner_company_id": "C1"}}
VISIT = {"checkin_event_id": "A", "property_id": "P1"}


def test_missing_person_reads_as_version_zero():
    assert InMemoryStore(PROPS).load_person("U1") == (0, [])


def test_commit_creates_version_one_and_stores_evaluation():
    store = InMemoryStore(PROPS)
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A", "breach": False})
    assert store.load_person("U1") == (1, [VISIT])
    assert store.get_evaluation("A") == {"checkin_event_id": "A", "breach": False}


def test_stale_version_conflicts():
    store = InMemoryStore(PROPS)
    store.commit("U1", 0, [VISIT], None)
    with pytest.raises(ConflictError):
        store.commit("U1", 0, [], None)


def test_existing_evaluation_cancels_the_whole_commit():
    store = InMemoryStore(PROPS)
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A"})
    with pytest.raises(EvaluationExistsError):
        store.commit("U1", 1, [], {"checkin_event_id": "A"})
    assert store.load_person("U1") == (1, [VISIT])


def test_returned_visits_are_copies():
    store = InMemoryStore(PROPS)
    store.commit("U1", 0, [dict(VISIT)], None)
    _, visits = store.load_person("U1")
    visits[0]["property_id"] = "changed"
    assert store.load_person("U1")[1][0]["property_id"] == "P1"


def test_unknown_property_raises_validation_error():
    with pytest.raises(UnknownPropertyError):
        InMemoryStore(PROPS).get_property("NOPE")


def test_save_and_load_round_trip(tmp_path):
    path = tmp_path / "sim_store.json"
    store = InMemoryStore(PROPS)
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A"})
    store.save(path)
    loaded = InMemoryStore.load(path, PROPS)
    assert loaded.load_person("U1") == (1, [VISIT])
    assert loaded.get_evaluation("A") == {"checkin_event_id": "A"}
    assert InMemoryStore.load(tmp_path / "missing.json", PROPS).load_person("U1") == (0, [])
