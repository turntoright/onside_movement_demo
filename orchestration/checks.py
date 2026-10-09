"""Asset checks on stg_events (spec §6.4): completeness, dead-letter queues, freshness.

They only read: S3 control/ records, SQS queue attributes (never a message) and the
warehouse, which they open read-only.
"""

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from dagster import AssetCheckResult, AssetKey, asset_check

from generator import queues as Q
from generator.aws import Connection
from generator.stream import event_id

from .resources import POOL, OnsideResource
from .sensors import SETTLED_PREFIX, settled_run_id

STG_EVENTS = AssetKey("stg_events")
APP_TYPES = ("CheckIn", "CheckOut", "PlantMovement")
FRESHNESS = timedelta(minutes=15)
APP_EVENTS_SQL = """
select
    detail_type,
    json_extract_string(detail, '$.event_id'),
    json_extract_string(detail, '$.received_time')
from stg_events
where source = 'onside.app' and detail_type in ('CheckIn', 'CheckOut', 'PlantMovement')
"""


def control_records(conn: Connection, prefix: str) -> dict[str, dict]:
    """run_id -> the JSON record, for every control/<kind>/<run_id>.json."""
    return {
        key[len(prefix) :].removesuffix(".json"): conn.get_json(key)
        for key in conn.keys(prefix)
        if key.endswith(".json")
    }


def app_events(db: Path) -> list[tuple[str, str, str | None]]:
    """(detail_type, event_id, received_time) for every app event row in staging."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.sql(APP_EVENTS_SQL).fetchall()
    finally:
        con.close()


def run_ids(run_id: str, total: int) -> set[str]:
    return {event_id(run_id, seq) for seq in range(total)}


@asset_check(
    asset=STG_EVENTS,
    name="completeness",
    pool=POOL,
    description="Distinct app event_ids per type in the warehouse (stg_events reads "
    "compacted/<env>/) equal the summed manifests of the settled runs.",
)
def completeness(onside: OnsideResource) -> AssetCheckResult:
    conn = onside.connection()
    settled = {settled_run_id(key) for key in conn.keys(SETTLED_PREFIX)}
    manifests = control_records(conn, "control/manifests/")
    runs = sorted(run for run in manifests if run in settled)  # unsettled: not missing yet
    expected = {t: sum(manifests[r]["counts"].get(t, 0) for r in runs) for t in APP_TYPES}
    ids = set().union(*(run_ids(r, manifests[r]["total"]) for r in runs))
    found: dict[str, set[str]] = {t: set() for t in APP_TYPES}
    for kind, eid, _ in app_events(onside.warehouse()):
        if eid in ids:  # events of unsettled runs may already be compacted; skip them
            found[kind].add(eid)
    metadata = {"settled_runs": len(runs), "unsettled_runs": len(manifests) - len(runs)}
    for t in APP_TYPES:
        metadata[f"{t} expected"] = expected[t]
        metadata[f"{t} found"] = len(found[t])
    passed = all(len(found[t]) == expected[t] for t in APP_TYPES)
    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(
    asset=STG_EVENTS,
    name="dead_letter_queues",
    description="Visit DLQ holds between P and 2P messages (P = summed control/poison/ k); "
    "the archive DLQ holds none. Reads queue attributes only.",
)
def dead_letter_queues(onside: OnsideResource) -> AssetCheckResult:
    conn = onside.connection()
    poison = sum(int(r.get("k", 0)) for r in control_records(conn, "control/poison/").values())
    visit = Q.visible(conn.sqs, conn.outputs.visit_dlq_url)
    archive = Q.visible(conn.sqs, conn.outputs.archive_dlq_url)
    passed = poison <= visit <= 2 * poison and archive == 0
    return AssetCheckResult(
        passed=passed,
        metadata={"poison injected": poison, "visit DLQ": visit, "archive DLQ": archive},
    )


@asset_check(
    asset=STG_EVENTS,
    name="freshness",
    pool=POOL,
    description="After a live run, its latest received_time in the warehouse is within "
    "15 minutes of its settle time. Skipped for a backfill.",
)
def freshness(onside: OnsideResource) -> AssetCheckResult:
    settled = control_records(onside.connection(), SETTLED_PREFIX)
    if not settled:
        return AssetCheckResult(passed=True, metadata={"skipped": "no settled run"})
    latest = max(
        settled.values(), key=lambda r: (datetime.fromisoformat(r["settled_at"]), r["run_id"])
    )
    run_id = latest["run_id"]
    if latest["mode"] != "live":
        return AssetCheckResult(passed=True, metadata={"skipped": "backfill", "run_id": run_id})
    ids = run_ids(run_id, sum(latest["counts"].values()))
    received = [
        datetime.fromisoformat(rt)
        for _, eid, rt in app_events(onside.warehouse())
        if eid in ids and rt
    ]
    if not received:
        return AssetCheckResult(
            passed=False, metadata={"run_id": run_id, "error": "no event of this run in stg_events"}
        )
    settled_at = datetime.fromisoformat(latest["settled_at"])
    lag = settled_at - max(received)
    return AssetCheckResult(
        passed=lag <= FRESHNESS,
        metadata={
            "run_id": run_id,
            "settled_at": latest["settled_at"],
            "max received_time": max(received).isoformat(),
            "lag minutes": round(lag.total_seconds() / 60, 1),
        },
    )
