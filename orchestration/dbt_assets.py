"""dbt seeds, models and tests as Dagster assets and asset checks (spec §6.4)."""

import sys
from pathlib import Path

from dagster import AssetExecutionContext, AssetsDefinition
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from generator.config import state_dir
from generator.state import GeneratorError

from .resources import POOL

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"
# The project's dbt-core, never a `dbt` found on PATH (~/.local/bin/dbt is dbt Fusion,
# which has no DuckDB adapter).
DBT_EXECUTABLE = Path(sys.executable).parent / "dbt"


def manifest_dir(env: str) -> Path:
    """Where `make dagster` runs `dbt parse` for this env's target (gitignored state)."""
    return state_dir(env) / "dagster" / "dbt"


def dbt_project(env: str) -> DbtProject:
    """The dbt target equals the env; the profile builds warehouse/<env>.duckdb."""
    return DbtProject(
        project_dir=DBT_DIR, profiles_dir=DBT_DIR, target=env, target_path=manifest_dir(env)
    )


def dbt_resource(project: DbtProject) -> DbtCliResource:
    return DbtCliResource(project_dir=project, dbt_executable=str(DBT_EXECUTABLE))


def build_dbt_assets(project: DbtProject) -> AssetsDefinition:
    if not project.manifest_path.exists():
        raise GeneratorError(
            f"no dbt manifest at {project.manifest_path}; start Dagster with "
            f"`make dagster ENV={project.target}` (it runs dbt parse first)"
        )

    # `dbt build` runs the tests too; dagster-dbt reports them as asset checks.
    @dbt_assets(manifest=project.manifest_path, project=project, name="onside_dbt", pool=POOL)
    def onside_dbt(context: AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

    return onside_dbt
