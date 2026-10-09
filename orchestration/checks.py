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
    description="Every app event_id of the settled runs' manifests is in stg_events (reads "
    "compacted/<env>/; counts per type shown); staged events of unsettled runs or of no "
    "manifest are ignored.",
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
    # A transient red: a run that settles between this run's stg_events build and this check
    # counts as settled, but its events are not staged yet. Its own sensor run turns it green.
    why = None
    if not passed:
        why = "found/expected: " + ", ".join(
            f"{t} {len(found[t])}/{expected[t]}" for t in APP_TYPES
        )
        why += " (a run settled after this build? its own dbt_build re-checks)"
    return AssetCheckResult(passed=passed, metadata=metadata, description=why)


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
    failures = []
    if visit < poison:
        failures.append(f"visit DLQ {visit} < P={poison}")
    if visit > 2 * poison:
        failures.append(f"visit DLQ {visit} > 2P={2 * poison}")
    if archive:
        failures.append(f"archive DLQ {archive} > 0")
    return AssetCheckResult(
        passed=not failures,
        metadata={"poison injected": poison, "visit DLQ": visit, "archive DLQ": archive},
        description="; ".join(failures) or None,
    )


@asset_check(
    asset=STG_EVENTS,
    name="freshness",
    pool=POOL,
    description="After a live run, its latest received_time in the warehouse is within "
    "15 minutes of its settle time. Skipped when the latest settled run is not live.",
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
        return AssetCheckResult(passed=True, metadata={"skipped": latest["mode"], "run_id": run_id})
    ids = run_ids(run_id, sum(latest["counts"].values()))
    received = [
        datetime.fromisoformat(rt)
        for _, eid, rt in app_events(onside.warehouse())
        if eid in ids and rt
    ]
    # A transient red: a live run that settles between this run's stg_events build and this
    # check becomes the latest, but its events are not staged yet. Its own sensor run turns
    # it green.
    if not received:
        return AssetCheckResult(
            passed=False,
            metadata={"run_id": run_id, "error": "no event of this run in stg_events"},
            description=f"no event of run {run_id} in stg_events "
            "(settled after this build? its own dbt_build re-checks)",
        )
    settled_at = datetime.fromisoformat(latest["settled_at"])
    lag = settled_at - max(received)
    minutes = round(lag.total_seconds() / 60, 1)
    passed = lag <= FRESHNESS
    return AssetCheckResult(
        passed=passed,
        description=None if passed else f"lag {minutes} min > {FRESHNESS.seconds // 60} min",
        metadata={
            "run_id": run_id,
            "settled_at": latest["settled_at"],
            "max received_time": max(received).isoformat(),
            "lag minutes": minutes,
        },
    )
