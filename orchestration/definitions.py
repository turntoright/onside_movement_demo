"""The Dagster code location: one Definitions object for ONSIDE_ENV=local|aws (spec §6.4).

`make dagster ENV=local|aws` sets ONSIDE_ENV and runs `dagster dev -m orchestration.definitions`.
"""

import os

from dagster import AssetSelection, Definitions, define_asset_job, definitions

from generator.aws import load_outputs

from .checks import completeness, dead_letter_queues, freshness
from .dbt_assets import build_dbt_assets, dbt_project, dbt_resource
from .resources import OnsideResource, check_env
from .sensors import DBT_JOB, daily_dbt_build, settled_runs
from .trace_job import trace_job


def build_definitions(env: str | None) -> Definitions:
    env = check_env(env)  # sim (or nothing) is an error, not a silent default
    load_outputs(env)  # not deployed: say so at load time, not at the first sensor tick
    project = dbt_project(env)
    onside_dbt = build_dbt_assets(project)
    dbt_build = define_asset_job(
        DBT_JOB,
        selection=AssetSelection.assets(onside_dbt),
        description="dbt build over S3 compacted/<env>/, then the asset checks on stg_events.",
    )
    return Definitions(
        assets=[onside_dbt],
        asset_checks=[completeness, dead_letter_queues, freshness],
        jobs=[dbt_build, trace_job],
        sensors=[settled_runs],
        schedules=[daily_dbt_build],
        resources={"dbt": dbt_resource(project), "onside": OnsideResource(env=env)},
    )


@definitions
def defs() -> Definitions:
    return build_definitions(os.environ.get("ONSIDE_ENV"))
