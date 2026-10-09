import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from dagster import Definitions
from dagster_dbt import get_asset_key_for_model

from generator.state import GeneratorError
from orchestration import definitions as D
from orchestration.checks import STG_EVENTS
from orchestration.dbt_assets import DBT_DIR, DBT_EXECUTABLE, manifest_dir


@pytest.fixture(scope="session")
def parsed_manifest(tmp_path_factory) -> Path:
    """`dbt parse` for the local target, once per session (what `make dagster` runs)."""
    out = tmp_path_factory.mktemp("dbt")
    subprocess.run(
        [
            DBT_EXECUTABLE,
            "parse",
            "--quiet",
            "--project-dir",
            DBT_DIR,
            "--profiles-dir",
            DBT_DIR,
            "--target",
            "local",
            "--target-path",
            out / "target",
            "--log-path",
            out / "logs",
        ],
        check=True,
    )
    return out / "target" / "manifest.json"


@pytest.fixture
def local_defs(deployed, parsed_manifest) -> Definitions:
    manifest_dir("local").mkdir(parents=True)
    shutil.copy(parsed_manifest, manifest_dir("local") / "manifest.json")
    return D.build_definitions("local")


def test_definitions_load_for_local(local_defs):
    Definitions.validate_loadable(local_defs)
    assert {j.name for j in local_defs.resolve_all_job_defs()} >= {"dbt_build", "trace"}
    assert local_defs.resolve_sensor_def("settled_runs").job_name == "dbt_build"
    assert local_defs.resolve_schedule_def("daily_dbt_build").job_name == "dbt_build"


def test_the_dbt_job_runs_every_model_then_the_checks_all_in_the_duckdb_pool(local_defs):
    job = local_defs.resolve_job_def("dbt_build")
    pools = {n.name: n.definition.pool for n in job.nodes}
    assert pools == {
        "onside_dbt": "duckdb",
        "stg_events_completeness": "duckdb",
        "stg_events_dead_letter_queues": None,  # SQS and S3 only, no DuckDB
        "stg_events_freshness": "duckdb",
    }
    onside_dbt = local_defs.resolve_assets_def(STG_EVENTS)
    assert get_asset_key_for_model([onside_dbt], "stg_events") == STG_EVENTS
    assert get_asset_key_for_model([onside_dbt], "dim_property_network")  # the Python model
    checks = job.asset_layer.asset_graph.asset_check_keys
    assert {c.name for c in checks if c.asset_key == STG_EVENTS} >= {
        "completeness",
        "dead_letter_queues",
        "freshness",
    }
    assert len(checks) > 3  # dbt tests are asset checks too


def test_dbt_runs_with_the_venv_dbt_core_and_the_env_target(local_defs):
    dbt = local_defs.resources["dbt"]
    assert dbt.dbt_executable == str(Path(sys.executable).parent / "dbt")
    assert Path(dbt.dbt_executable).exists()
    assert dbt.target == "local"
    # dbt Fusion has no DuckDB adapter, so its --version never lists duckdb.
    version = subprocess.run(
        [dbt.dbt_executable, "--version"], capture_output=True, text=True, check=True
    )
    assert "duckdb" in version.stdout + version.stderr
    assert local_defs.resources["onside"].env == "local"


@pytest.mark.parametrize("env", ["sim", None])
def test_loading_for_sim_or_no_env_is_an_error(env, monkeypatch):
    if env is None:
        monkeypatch.delenv("ONSIDE_ENV", raising=False)
    else:
        monkeypatch.setenv("ONSIDE_ENV", env)
    with pytest.raises(GeneratorError, match="ENV=local or ENV=aws"):
        D.defs()


def test_an_undeployed_env_says_make_deploy(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    with pytest.raises(GeneratorError, match="make deploy ENV=local"):
        D.build_definitions("local")


def test_a_missing_manifest_says_make_dagster(deployed):
    with pytest.raises(GeneratorError, match="make dagster ENV=local"):
        D.build_definitions("local")
