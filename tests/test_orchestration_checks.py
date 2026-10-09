import json
import uuid
from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from generator import queues as Q
from generator.stream import event_id
from orchestration.checks import completeness, dead_letter_queues, freshness
from orchestration.resources import OnsideResource

T0 = datetime(2026, 10, 10, 1, 0, tzinfo=UTC)
COUNTS = {"CheckIn": 3, "CheckOut": 2, "PlantMovement": 1}


@pytest.fixture
def onside(deployed):
    (deployed / "warehouse").mkdir()
    con = duckdb.connect(str(deployed / "warehouse" / "local.duckdb"))
    con.execute("create table stg_events (source varchar, detail_type varchar, detail json)")
    con.close()
    return OnsideResource(env="local")


def put(aws, key: str, value: dict) -> None:
    aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=json.dumps(value))


def add_run(
    aws, deployed, mode="backfill", *, settled=True, received=T0, settled_at=None, drop=0, load=True
) -> str:
    """Manifest (+ settled record) in S3 and the run's events in stg_events."""
    run_id = str(uuid.uuid4())
    total = sum(COUNTS.values())
    put(
        aws,
        f"control/manifests/{run_id}.json",
        {"run_id": run_id, "mode": mode, "counts": COUNTS, "total": total},
    )
    if settled:
        settled_at = settled_at or received + timedelta(minutes=2)
        put(
            aws,
            f"control/settled/{run_id}.json",
            {
                "run_id": run_id,
                "mode": mode,
                "counts": COUNTS,
                "settled_at": settled_at.isoformat(timespec="seconds"),
            },
        )
    if load:
        kinds = [t for t, n in COUNTS.items() for _ in range(n)]
        rows = [
            (
                "onside.app",
                kind,
                json.dumps(
                    {"event_id": event_id(run_id, seq), "received_time": received.isoformat()}
                ),
            )
            for seq, kind in enumerate(kinds)
        ][drop:]
        rows.append(rows[-1])  # a duplicate delivery: counted once
        con = duckdb.connect(str(deployed / "warehouse" / "local.duckdb"))
        con.executemany("insert into stg_events values (?, ?, ?)", rows)
        con.close()
    return run_id


def test_completeness_passes_when_every_settled_event_is_in_staging(aws, deployed, onside):
    add_run(aws, deployed)
    add_run(aws, deployed, settled=False)  # compacted early, not settled: not counted
    add_run(aws, deployed, settled=False, load=False)  # not settled, nothing loaded: not missing
    result = completeness(onside=onside)
    assert result.passed
    meta = {k: v.value for k, v in result.metadata.items()}
    assert meta["CheckIn expected"] == meta["CheckIn found"] == 3
    assert meta["PlantMovement found"] == 1
    assert (meta["settled_runs"], meta["unsettled_runs"]) == (1, 2)


def test_completeness_fails_when_a_settled_event_is_missing(aws, deployed, onside):
    add_run(aws, deployed, drop=1)  # the first CheckIn never reached the warehouse
    result = completeness(onside=onside)
    assert not result.passed
    meta = {k: v.value for k, v in result.metadata.items()}
    assert (meta["CheckIn expected"], meta["CheckIn found"]) == (3, 2)
    assert result.description.startswith("found/expected: CheckIn 2/3, CheckOut 2/2, ")
    assert "PlantMovement 1/1" in result.description


def test_completeness_with_no_settled_run_passes_and_counts_nothing(aws, deployed, onside):
    add_run(aws, deployed, settled=False)  # manifest and events exist, but nothing settled
    result = completeness(onside=onside)
    assert result.passed
    meta = {k: v.value for k, v in result.metadata.items()}
    assert (meta["settled_runs"], meta["unsettled_runs"]) == (0, 1)
    for kind in COUNTS:
        assert (meta[f"{kind} expected"], meta[f"{kind} found"]) == (0, 0)


def test_completeness_ignores_a_staged_event_that_belongs_to_no_settled_run(aws, deployed, onside):
    add_run(aws, deployed)
    stray = ("onside.app", "CheckIn", json.dumps({"event_id": str(uuid.uuid4())}))
    con = duckdb.connect(str(deployed / "warehouse" / "local.duckdb"))
    con.execute("insert into stg_events values (?, ?, ?)", stray)
    con.close()
    result = completeness(onside=onside)
    assert result.passed
    meta = {k: v.value for k, v in result.metadata.items()}
    assert meta["CheckIn expected"] == meta["CheckIn found"] == 3


@pytest.mark.parametrize(
    ("poison", "visit", "archive", "passed", "why"),
    [
        (0, 0, 0, True, None),
        (2, 2, 0, True, None),
        (2, 4, 0, True, None),
        (2, 1, 0, False, "visit DLQ 1 < P=2"),
        (2, 5, 0, False, "visit DLQ 5 > 2P=4"),
        (0, 0, 1, False, "archive DLQ 1 > 0"),
    ],
)
def test_dlq_check_bounds_the_visit_dlq_and_wants_an_empty_archive_dlq(
    aws, deployed, onside, poison, visit, archive, passed, why
):
    if poison:
        put(aws, "control/poison/p1.json", {"run_id": "p1", "k": poison})
    for _ in range(visit):
        aws.sqs.send_message(QueueUrl=aws.queue_urls["visit_dlq"], MessageBody="{}")
    for _ in range(archive):
        aws.sqs.send_message(QueueUrl=aws.queue_urls["archive_dlq"], MessageBody="{}")
    result = dead_letter_queues(onside=onside)
    assert result.passed is passed
    assert result.description == why
    # attributes only: nothing was received, so every message is still visible
    assert Q.visible(aws.sqs, aws.queue_urls["visit_dlq"]) == visit


def test_freshness_is_skipped_after_a_backfill(aws, deployed, onside):
    add_run(aws, deployed, mode="backfill")
    result = freshness(onside=onside)
    assert result.passed
    assert result.metadata["skipped"].value == "backfill"


def test_freshness_reports_the_actual_mode_it_skips(aws, deployed, onside):
    add_run(aws, deployed, mode="replay")
    result = freshness(onside=onside)
    assert result.passed
    assert result.metadata["skipped"].value == "replay"


def test_freshness_is_skipped_when_no_run_has_settled(aws, deployed, onside):
    add_run(aws, deployed, mode="live", settled=False)
    result = freshness(onside=onside)
    assert result.passed
    assert result.metadata["skipped"].value == "no settled run"


@pytest.mark.parametrize(
    ("lag", "passed", "why"),
    [
        (timedelta(minutes=5), True, None),
        (timedelta(minutes=15), True, None),  # the bound is inclusive
        (timedelta(minutes=20), False, "lag 20.0 min > 15 min"),
    ],
)
def test_freshness_of_the_latest_live_run(aws, deployed, onside, lag, passed, why):
    add_run(aws, deployed, mode="backfill", received=T0 - timedelta(days=1))
    add_run(aws, deployed, mode="live", received=T0, settled_at=T0 + lag)
    result = freshness(onside=onside)
    assert result.passed is passed
    assert result.metadata["lag minutes"].value == lag.total_seconds() / 60
    assert (result.description or "").startswith(why or "")
    assert (result.description is None) is passed


def test_freshness_fails_when_the_live_run_is_not_in_the_warehouse(aws, deployed, onside):
    run_id = add_run(aws, deployed, mode="live", load=False)
    result = freshness(onside=onside)
    assert not result.passed
    assert result.description.startswith(f"no event of run {run_id} in stg_events")
