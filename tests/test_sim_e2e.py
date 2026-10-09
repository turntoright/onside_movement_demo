import json
import shutil
from collections import Counter
from pathlib import Path

import pytest

from generator.cli import main

ROOT = Path(__file__).resolve().parents[1]
ANCHOR = "2026-10-01T00:00:00+00:00"
NOW = "2026-10-09T04:00:00+00:00"


@pytest.fixture()
def repo(tmp_path, monkeypatch):
    shutil.copytree(ROOT / "shared", tmp_path / "shared")
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    return tmp_path


def read_ndjson(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def run_dirs(repo: Path) -> list[Path]:
    return sorted((repo / "data" / "sim" / "compacted").iterdir())


def test_backfill_then_live_produces_complete_derived_events(repo):
    assert main(["setup", "--env", "sim", "--scale", "ci"]) == 0
    assert main(["generate", "--env", "sim", "--mode", "backfill", "--anchor", ANCHOR]) == 0
    assert main(["generate", "--env", "sim", "--mode", "live", "--n", "20", "--now", NOW]) == 0

    dirs = run_dirs(repo)
    assert len(dirs) == 2
    generated, derived = [], []
    for d in dirs:
        generated += read_ndjson(d / "generator.ndjson")
        derived += read_ndjson(d / "standdown.ndjson")
    checkins = {e["detail"]["event_id"] for e in generated if e["detail-type"] == "CheckIn"}
    checkouts = {e["detail"]["event_id"] for e in generated if e["detail-type"] == "CheckOut"}
    evaluated = {e["detail"]["checkin_event_id"] for e in derived
                 if e["detail-type"] == "CheckInEvaluated"}  # fmt: skip
    applied = {e["detail"]["checkout_event_id"] for e in derived
               if e["detail-type"] == "CheckOutApplied"}  # fmt: skip
    assert evaluated == checkins
    assert applied == checkouts
    kinds = Counter(e["detail-type"] for e in derived)
    assert kinds["HighRiskVisitDetected"] > 0

    # Versions continue across runs: live evaluations read state the backfill committed.
    runs = json.loads((repo / "state" / "sim" / "generator_state.json").read_text())["runs"]
    live = read_ndjson(repo / "data" / "sim" / "compacted" / runs[1] / "standdown.ndjson")
    evaluations = [e["detail"] for e in live if e["detail-type"] == "CheckInEvaluated"]
    assert evaluations and all(e["version_read"] > 0 for e in evaluations)


def test_second_backfill_is_refused(repo, capsys):
    main(["setup", "--env", "sim", "--scale", "ci"])
    main(["generate", "--env", "sim", "--mode", "backfill", "--anchor", ANCHOR])
    assert main(["generate", "--env", "sim", "--mode", "backfill", "--anchor", ANCHOR]) == 2
    assert "make reset ENV=sim" in capsys.readouterr().err


def test_live_without_backfill_is_refused(repo, capsys):
    main(["setup", "--env", "sim", "--scale", "ci"])
    assert main(["generate", "--env", "sim", "--mode", "live", "--now", NOW]) == 2
    assert "MODE=backfill" in capsys.readouterr().err


def test_deleted_manifest_is_detected(repo, capsys):
    main(["setup", "--env", "sim", "--scale", "ci"])
    main(["generate", "--env", "sim", "--mode", "backfill", "--anchor", ANCHOR])
    for manifest in (repo / "state" / "sim" / "manifests").glob("*.json"):
        manifest.unlink()
    assert main(["generate", "--env", "sim", "--mode", "live", "--now", NOW]) == 2
    assert "do not match" in capsys.readouterr().err


def test_reset_clears_state_and_data(repo):
    main(["setup", "--env", "sim", "--scale", "ci"])
    main(["generate", "--env", "sim", "--mode", "backfill", "--anchor", ANCHOR])
    assert main(["reset", "--env", "sim"]) == 0
    assert not (repo / "state" / "sim").exists()
    assert not (repo / "data" / "sim").exists()


def test_backfills_with_different_anchors_get_different_run_ids(repo):
    ids = []
    for anchor in (ANCHOR, "2026-10-02T00:00:00+00:00"):
        main(["reset", "--env", "sim"])
        assert main(["setup", "--env", "sim", "--scale", "ci"]) == 0
        assert main(["generate", "--env", "sim", "--mode", "backfill", "--anchor", anchor]) == 0
        ids.append(run_dirs(repo)[0].name)
    assert ids[0] != ids[1]
