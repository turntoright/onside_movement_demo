"""Makefile wiring, checked with `make -n` (prints, runs nothing) or with guards that stop first."""

import json
from pathlib import Path

import pytest
import yaml

from tests.helpers import ROOT as REPO
from tests.helpers import make


@pytest.fixture
def deployed_root(tmp_path) -> Path:
    outputs = {"bucket": "onside-local-000000000000", "region": "us-west-2"}
    (tmp_path / "state" / "local").mkdir(parents=True)
    (tmp_path / "state" / "local" / "outputs.json").write_text(
        json.dumps({k: {"value": v} for k, v in outputs.items()})
    )
    return tmp_path


@pytest.mark.parametrize("args", [["dagster", "ENV=sim"], ["dagster"]])
def test_make_dagster_refuses_sim_and_a_missing_env(args, tmp_path):
    result = make(*args, root=tmp_path)
    assert result.returncode == 2
    assert "needs ENV=local or ENV=aws on the command line" in result.stderr
    assert not (tmp_path / "state").exists()


def test_make_dagster_needs_a_deployed_stack(tmp_path):
    result = make("dagster", "ENV=local", root=tmp_path)
    assert result.returncode == 2
    assert "run make deploy ENV=local" in result.stderr


def test_make_dagster_parses_dbt_then_runs_dagster_dev_on_loopback(deployed_root):
    out = make("-n", "dagster", "ENV=local", root=deployed_root).stdout
    home = deployed_root / "state" / "local" / "dagster"
    assert f"/orchestration/dagster.yaml {home}/dagster.yaml" in out
    assert f"parse --quiet --target local --target-path {home}/dbt" in out
    dev = next(line for line in out.splitlines() if "dagster dev" in line)
    assert dev.endswith("dagster dev -h 127.0.0.1 -p 3000 -m orchestration.definitions")
    for part in (
        "ONSIDE_ENV=local",
        f"DAGSTER_HOME={home}",
        f"DBT_TARGET_PATH={home}/dbt/runs",
        "DBT_COMPACTED_GLOB='s3://onside-local-000000000000/compacted/local/*/*.ndjson'",
        "DBT_REF_ROOT='s3://onside-local-000000000000/ref/'",
        "DBT_S3_REGION='us-west-2'",
        "LOCALSTACK_S3_HOST='localhost:4566'",
        f"PYTHONPATH={deployed_root}/lambdas",
    ):
        assert part in dev
    # The parse writes the manifest to an explicit --target-path; the variable is for dev only.
    parse = next(line for line in out.splitlines() if " parse " in line)
    assert "DBT_TARGET_PATH" not in parse
    build = make("-n", "dbt-build", "ENV=local", root=deployed_root).stdout
    assert "DBT_TARGET_PATH" not in build


def test_make_trace_goes_through_dagster_except_for_sim(deployed_root):
    args = ["trace", "PROPERTY=P0001", "START=2026-06-01T00:00:00+12:00"]
    local = make("-n", *args, "ENV=local", root=deployed_root).stdout
    assert "-m orchestration.trace_job --env local --property P0001" in local
    assert "--max-hops 3 --host 127.0.0.1 --port 3000" in local
    sim = make("-n", *args, root=deployed_root).stdout
    assert f"-m tracing --db {deployed_root}/warehouse/sim.duckdb --property P0001" in sim


def test_dbt_s3_secrets_take_the_region_of_the_stack():
    profiles = yaml.safe_load((REPO / "dbt" / "profiles.yml").read_text())["onside_movement"]
    for target in ("local", "aws"):
        [secret] = profiles["outputs"][target]["secrets"]
        assert secret["region"] == "{{ env_var('DBT_S3_REGION', 'ap-southeast-2') }}"


def test_dbt_debug_checks_the_target_with_the_stack_region(deployed_root):
    out = make("-n", "dbt-debug", "ENV=local", root=deployed_root).stdout
    assert "DBT_S3_REGION='us-west-2'" in out
    assert out.rstrip().endswith("debug --target local")
