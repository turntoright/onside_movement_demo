import pytest

from generator import state as S


def test_setup_state_and_manifests_round_trip(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    S.write_setup("sim", 42, "ci")
    assert S.read_setup("sim") == {"seed": 42, "scale": "ci"}
    assert S.load_state("sim") is None
    manifest = S.write_manifest("sim", "run-1", "backfill", 42, {"CheckIn": 3, "CheckOut": 2})
    assert manifest["total"] == 5
    S.save_state("sim", {"runs": ["run-1"], "cursors": {}})
    assert S.manifest_ids("sim") == {"run-1"}
    S.check_consistency("sim", S.load_state("sim"))


def test_missing_setup_is_a_clear_error(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    with pytest.raises(S.GeneratorError, match="make setup"):
        S.read_setup("sim")


def test_inconsistent_manifests_are_detected(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    S.write_manifest("sim", "run-1", "backfill", 42, {"CheckIn": 1})
    with pytest.raises(S.GeneratorError, match="reset"):
        S.check_consistency("sim", None)
