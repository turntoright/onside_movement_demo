"""Lambda entry point: SQS batch -> processor -> PutEvents (spec §6.1).

Every record that fails (invalid, retries exhausted, any error, or an outbound event that
PutEvents rejected) is reported in batchItemFailures, so SQS redelivers only that record.
A redelivery takes the processor's "already processed" path and re-emits identical bodies.
"""

from __future__ import annotations

import json
import os
import traceback
import zoneinfo
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path

from .dynamo_store import DynamoDbStore
from .processor import Outbound, process_event
from .rules import Rule, load_rules
from .store import StateStore

PUT_EVENTS_MAX_ENTRIES = 10
# In the Lambda zip the rules file sits next to the package (make package).
RULES_FILE = Path(__file__).resolve().parents[1] / "standdown_rules.csv"


def _wall_clock() -> datetime:
    return datetime.now(UTC)


def _log(**fields) -> None:
    print(json.dumps(fields, sort_keys=True))


def _send(events_client, bus_name: str, chunk: list[tuple[str, Outbound]]) -> set[int]:
    """PutEvents for up to 10 entries; returns the indexes (within chunk) of rejected entries."""
    entries = [
        {
            "Source": out.source,
            "DetailType": out.detail_type,
            "Detail": out.detail_json(),
            "EventBusName": bus_name,
        }
        for _, out in chunk
    ]
    try:
        response = events_client.put_events(Entries=entries)
    except Exception as exc:  # noqa: BLE001 - any error fails the records, never the batch
        _log(
            level="error",
            error=f"PutEvents: {type(exc).__name__}: {exc}",
            traceback=traceback.format_exc(),
        )
        return set(range(len(chunk)))
    if not response.get("FailedEntryCount"):
        return set()
    results = response.get("Entries", [])
    failed = {
        i
        for i, (_, result) in enumerate(zip(chunk, results, strict=False))
        if result.get("ErrorCode")
    }
    _log(level="error", error=f"PutEvents FailedEntryCount={response['FailedEntryCount']}")
    return failed or set(range(len(chunk)))


def handle(
    event: dict,
    *,
    store: StateStore,
    rules: Sequence[Rule],
    events_client,
    bus_name: str,
    now: Callable[[], datetime] = _wall_clock,
) -> dict:
    failed: list[str] = []
    pending: list[tuple[str, Outbound]] = []
    for record in event.get("Records", []):
        message_id = record["messageId"]
        try:
            envelope = json.loads(record["body"])
            outbound = process_event(
                envelope.get("detail-type"), envelope.get("detail"), store, rules, now
            )
        except Exception as exc:  # noqa: BLE001 - ValidationError, RetryExhaustedError, any
            _log(
                level="error",
                messageId=message_id,
                error=f"{type(exc).__name__}: {exc}",
                traceback=traceback.format_exc(),
            )
            failed.append(message_id)
            continue
        pending.extend((message_id, out) for out in outbound)

    rejected: set[int] = set()  # indexes into pending
    for start in range(0, len(pending), PUT_EVENTS_MAX_ENTRIES):
        chunk = pending[start : start + PUT_EVENTS_MAX_ENTRIES]
        rejected |= {start + i for i in _send(events_client, bus_name, chunk)}
    for index, (_, out) in enumerate(pending):
        if out.detail_type == "HighRiskVisitDetected" and index not in rejected:
            _log(level="alert", **out.detail)
    rejected_ids = {pending[i][0] for i in rejected}
    failed.extend(m for m in dict.fromkeys(m for m, _ in pending) if m in rejected_ids)
    return {"batchItemFailures": [{"itemIdentifier": m} for m in failed]}


_DEPENDENCIES: dict | None = None


def _dependencies() -> dict:
    """Built once per Lambda container: clients, the store (with its property cache), rules."""
    global _DEPENDENCIES
    if _DEPENDENCIES is None:
        import boto3

        _DEPENDENCIES = {
            "store": DynamoDbStore(
                boto3.client("dynamodb"),
                os.environ["PERSON_TABLE"],
                os.environ["EVALUATION_TABLE"],
                os.environ["PROPERTY_TABLE"],
            ),
            "rules": load_rules(os.environ.get("RULES_PATH", RULES_FILE)),
            "events_client": boto3.client("events"),
            "bus_name": os.environ["EVENT_BUS_NAME"],
        }
        # Once per container: () proves zoneinfo reads only the bundled tzdata (spec §2).
        _log(level="info", event="cold_start", tzpath=list(zoneinfo.TZPATH))
    return _DEPENDENCIES


def handler(event: dict, context: object) -> dict:
    return handle(event, **_dependencies())
