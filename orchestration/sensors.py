"""Run dbt when a run settles (spec §6.4): a sensor on control/settled/, a backup schedule."""

import json
from datetime import datetime, timedelta

from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from generator.state import GeneratorError

from .resources import OnsideResource

DBT_JOB = "dbt_build"
SETTLED_PREFIX = "control/settled/"  # the sensor lists only this prefix, never raw/
# An object can become listable after a later-stamped one was seen (concurrent settles across a
# second boundary), so candidates reach back this far; the run key absorbs any repeat.
LOOKBACK = timedelta(minutes=2)


def settled_run_id(key: str) -> str:
    return key[len(SETTLED_PREFIX) :].removesuffix(".json")


def new_settled(objects: list[dict], cursor: str | None) -> tuple[list[dict], str | None]:
    """Settled objects not yet seen, oldest first, and the cursor that covers them.

    The cursor is the greatest LastModified plus every key within LOOKBACK of it. Candidates are
    unseen keys stamped no earlier than LOOKBACK before the cursor, so objects written in the same
    second, or listable late within the window, each fire exactly once.
    """
    state = json.loads(cursor) if cursor else None
    last = datetime.fromisoformat(state["last_modified"]) if state else None
    seen = set(state["keys"]) if state else set()
    floor = last - LOOKBACK if last else None
    fresh = sorted(
        (
            o
            for o in objects
            if o["Key"].endswith(".json")
            and o["Key"] not in seen
            and (floor is None or o["LastModified"] >= floor)
        ),
        key=lambda o: (o["LastModified"], o["Key"]),
    )
    if not fresh:
        return [], cursor
    top = max(fresh[-1]["LastModified"], last) if last else fresh[-1]["LastModified"]
    keys = sorted(o["Key"] for o in objects if o["LastModified"] >= top - LOOKBACK)
    return fresh, json.dumps({"last_modified": top.isoformat(), "keys": keys})


@sensor(
    name="settled_runs",
    job_name=DBT_JOB,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    description="One dbt build per new control/settled/<run_id>.json (run key = run_id).",
)
def settled_runs(context: SensorEvaluationContext, onside: OnsideResource):
    try:
        objects = onside.connection().objects(SETTLED_PREFIX)
    except GeneratorError as exc:  # e.g. LocalStack restarted: the bucket is gone
        return SkipReason(str(exc))
    fresh, cursor = new_settled(objects, context.cursor)
    if not fresh:
        return SkipReason(f"no new objects under {SETTLED_PREFIX}")
    requests = [
        RunRequest(
            run_key=settled_run_id(o["Key"]),  # Dagster never launches a run key twice
            tags={"onside/settled_run": settled_run_id(o["Key"])},
        )
        for o in fresh
    ]
    return SensorResult(run_requests=requests, cursor=cursor)


daily_dbt_build = ScheduleDefinition(
    name="daily_dbt_build",
    job_name=DBT_JOB,
    cron_schedule="0 6 * * *",
    execution_timezone="Pacific/Auckland",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Backup for the settled_runs sensor (spec §6.4); stopped by default.",
)
