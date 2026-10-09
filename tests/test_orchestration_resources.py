import pytest

from generator.state import GeneratorError
from orchestration.resources import OnsideResource, check_env


def test_the_resource_connects_through_the_terraform_outputs(aws, deployed):
    onside = OnsideResource(env="local")
    assert onside.connection().outputs.bucket == aws.bucket
    assert onside.warehouse() == deployed / "warehouse" / "local.duckdb"
    assert onside.trace_dir() == deployed / "state" / "local" / "trace"


@pytest.mark.parametrize("env", ["sim", None, "prod"])
def test_dagster_refuses_every_env_but_local_and_aws(env):
    with pytest.raises(GeneratorError, match="ENV=sim uses `make dbt-build ENV=sim`"):
        check_env(env)
    with pytest.raises(GeneratorError):
        OnsideResource(env=str(env)).warehouse()


def test_an_undeployed_env_says_make_deploy(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    with pytest.raises(GeneratorError, match="make deploy ENV=aws"):
        OnsideResource(env="aws").connection()
