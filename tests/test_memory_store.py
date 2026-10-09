import pytest

from generator.local_sim import InMemoryStore
from tests.store_contract import *  # noqa: F403  (contract suite, collected for InMemoryStore)
from tests.store_contract import PROPS, VISIT


@pytest.fixture
def store_factory():
    return InMemoryStore


def test_save_and_load_round_trip(tmp_path):
    path = tmp_path / "sim_store.json"
    store = InMemoryStore(PROPS)
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A"})
    store.save(path)
    loaded = InMemoryStore.load(path, PROPS)
    assert loaded.load_person("U1") == (1, [VISIT])
    assert loaded.get_evaluation("A") == {"checkin_event_id": "A"}
    assert InMemoryStore.load(tmp_path / "missing.json", PROPS).load_person("U1") == (0, [])
