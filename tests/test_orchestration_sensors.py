import json
from datetime import UTC, datetime, timedelta

import boto3
from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    SensorResult,
    SkipReason,
    build_sensor_context,
)

from generator import aws as A
from orchestration.resources import OnsideResource
from orchestration.sensors import daily_dbt_build, new_settled, settled_runs


def tick(cursor=None):
    context = build_sensor_context(cursor=cursor, resources={"onside": OnsideResource(env="local")})
    return settled_runs(context)


def settle(aws, run_id: str) -> None:
    key = f"control/settled/{run_id}.json"
    aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=json.dumps({"run_id": run_id}))


def test_each_settled_run_fires_one_dbt_build(aws, deployed):
    settle(aws, "run-a")
    first = tick()
    assert isinstance(first, SensorResult)
    assert [r.run_key for r in first.run_requests] == ["run-a"]
    assert first.run_requests[0].tags["onside/settled_run"] == "run-a"
    again = tick(first.cursor)
    assert isinstance(again, SkipReason)  # never twice for the same settled object
    settle(aws, "run-b")
    second = tick(first.cursor)
    assert [r.run_key for r in second.run_requests] == ["run-b"]


def test_raw_objects_are_never_listed_and_never_fire(aws, deployed, monkeypatch):
    prefixes = []

    def client(env, service, region):
        c = boto3.client(service, region_name=region)
        if service == "s3":
            c.meta.events.register(
                "provide-client-params.s3.ListObjectsV2",
                lambda params, **kwargs: prefixes.append(params["Prefix"]),
            )
        return c

    monkeypatch.setattr(A, "client", client)
    aws.s3.put_object(Bucket=aws.bucket, Key="raw/2026/10/10/01/x.ndjson", Body=b"{}\n")
    assert isinstance(tick(), SkipReason)
    assert prefixes == ["control/settled/"]


def test_objects_written_in_the_same_second_each_fire_once():
    t = datetime(2026, 10, 10, 1, 0, 0, tzinfo=UTC)
    a = {"Key": "control/settled/a.json", "LastModified": t}
    b = {"Key": "control/settled/b.json", "LastModified": t}
    fired, cursor = new_settled([b], None)
    assert [o["Key"] for o in fired] == ["control/settled/b.json"]
    fired, cursor = new_settled([a, b], cursor)  # a lands later in the same second
    assert [o["Key"] for o in fired] == ["control/settled/a.json"]
    assert new_settled([a, b], cursor) == ([], cursor)


def test_an_object_that_becomes_listable_late_inside_the_window_fires_once():
    t = datetime(2026, 10, 10, 1, 0, 0, tzinfo=UTC)
    a = {"Key": "control/settled/a.json", "LastModified": t}
    fired, cursor = new_settled([a], None)
    assert [o["Key"] for o in fired] == ["control/settled/a.json"]
    # b was stamped 1 s before the cursor's instant but only became listable now
    b = {"Key": "control/settled/b.json", "LastModified": t - timedelta(seconds=1)}
    fired, cursor = new_settled([a, b], cursor)
    assert [o["Key"] for o in fired] == ["control/settled/b.json"]
    assert new_settled([a, b], cursor) == ([], cursor)


def test_objects_older_than_the_lookback_window_never_fire():
    t = datetime(2026, 10, 10, 1, 0, 0, tzinfo=UTC)
    a = {"Key": "control/settled/a.json", "LastModified": t}
    old = {"Key": "control/settled/old.json", "LastModified": t - timedelta(minutes=3)}
    _, cursor = new_settled([a], None)
    assert new_settled([a, old], cursor) == ([], cursor)


def test_a_lost_stack_skips_with_the_make_up_hint(aws, deployed):
    aws.s3.delete_bucket(Bucket=aws.bucket)
    result = tick()
    assert isinstance(result, SkipReason)
    assert "make up" in result.skip_message


def test_sensor_polls_every_30s_and_the_backup_schedule_is_stopped():
    assert settled_runs.minimum_interval_seconds == 30
    assert settled_runs.default_status == DefaultSensorStatus.RUNNING
    assert settled_runs.job_name == "dbt_build"
    assert daily_dbt_build.default_status == DefaultScheduleStatus.STOPPED
    assert daily_dbt_build.job_name == "dbt_build"
