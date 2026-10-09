"""Archiver Lambda: one SQS batch -> one NDJSON object in S3 raw/ (spec §6.5).

Each line is one compact EventBridge envelope. The key uses the archiver's own UTC wall
clock (the arrival hour), never the envelope's `time`. Any failure raises, so the whole
batch is retried: delivery is at least once and settle/staging deduplicate.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Callable, Iterable
from datetime import UTC, datetime


def object_key(now: datetime, object_id: str) -> str:
    return f"raw/{now.astimezone(UTC):%Y/%m/%d/%H}/{object_id}.ndjson"


def to_ndjson(bodies: Iterable[str]) -> bytes:
    """One compact JSON document per line; a body that is not JSON raises."""
    lines = (json.dumps(json.loads(body), separators=(",", ":")) + "\n" for body in bodies)
    return "".join(lines).encode()


def handle(
    event: dict,
    *,
    s3,
    bucket: str,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    new_id: Callable[[], str] = lambda: str(uuid.uuid4()),
) -> dict:
    records = event.get("Records", [])
    if not records:
        return {"archived": 0, "key": None}
    body = to_ndjson(record["body"] for record in records)
    key = object_key(now(), new_id())
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")
    print(json.dumps({"archived": len(records), "key": key}))
    return {"archived": len(records), "key": key}


_S3 = None


def handler(event: dict, context: object) -> dict:
    global _S3
    if _S3 is None:
        import boto3
        from botocore.config import Config

        # LocalStack injects AWS_ENDPOINT_URL (a container IP), which needs path-style S3.
        style = "path" if os.environ.get("AWS_ENDPOINT_URL") else "auto"
        _S3 = boto3.client("s3", config=Config(s3={"addressing_style": style}))
    return handle(event, s3=_S3, bucket=os.environ["BUCKET"])
