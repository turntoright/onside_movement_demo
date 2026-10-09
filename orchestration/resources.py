"""The Dagster resource for one environment: AWS access through generator.aws, and paths."""

from pathlib import Path

from dagster import ConfigurableResource

from generator import aws as A
from generator.config import state_dir, warehouse_path
from generator.state import GeneratorError

DAGSTER_ENVS = ("local", "aws")
# The one concurrency pool (limit 1, orchestration/dagster.yaml): every op that opens
# warehouse/<env>.duckdb claims it, so dbt and the trace job never meet on the file lock.
POOL = "duckdb"


def check_env(env: str | None) -> str:
    if env not in DAGSTER_ENVS:
        raise GeneratorError(
            f"Dagster runs for ENV=local or ENV=aws, not {env!r}; ENV=sim uses "
            "`make dbt-build ENV=sim` and `make trace ENV=sim` (spec §6.4)"
        )
    return env


class OnsideResource(ConfigurableResource):
    """One environment's Terraform outputs, AWS clients and local paths."""

    env: str

    def connection(self) -> A.Connection:
        return A.connect(check_env(self.env))

    def warehouse(self) -> Path:
        return warehouse_path(check_env(self.env))

    def trace_dir(self) -> Path:
        return state_dir(check_env(self.env)) / "trace"
