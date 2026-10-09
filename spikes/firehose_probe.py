"""Day-1 spike: EventBridge -> Firehose -> S3 on LocalStack. Throwaway; kept as a record."""

import argparse
import json
import time
import uuid

import boto3

ENDPOINT = "http://localhost:4566"
REGION = "ap-southeast-2"
ACCOUNT = "000000000000"


def client(name):
    return boto3.client(
        name,
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=10_000)
    args = ap.parse_args()

    run = uuid.uuid4().hex[:8]
    bucket, stream, bus = f"spike-{run}", f"spike-{run}", f"spike-{run}"
    role = f"arn:aws:iam::{ACCOUNT}:role/spike"
    s3, fh, ev = client("s3"), client("firehose"), client("events")

    s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": REGION})
    fh.create_delivery_stream(
        DeliveryStreamName=stream,
        DeliveryStreamType="DirectPut",
        ExtendedS3DestinationConfiguration={
            "RoleARN": role,
            "BucketARN": f"arn:aws:s3:::{bucket}",
            "Prefix": "raw/",
            "BufferingHints": {"IntervalInSeconds": 60, "SizeInMBs": 1},
        },
    )
    stream_arn = fh.describe_delivery_stream(DeliveryStreamName=stream)[
        "DeliveryStreamDescription"
    ]["DeliveryStreamARN"]
    ev.create_event_bus(Name=bus)
    pattern = {
        "source": ["onside.app", "onside.standdown"],
        "detail": {"poison": [{"exists": False}]},
    }
    ev.put_rule(Name="rule-b", EventBusName=bus, EventPattern=json.dumps(pattern))
    ev.put_targets(
        Rule="rule-b",
        EventBusName=bus,
        Targets=[{"Id": "firehose", "Arn": stream_arn, "RoleArn": role}],
    )

    poison = set(range(0, args.events, 1000))
    t0 = time.time()
    batch = []
    for i in range(args.events):
        detail = {"event_id": str(uuid.uuid4()), "seq": i}
        if i in poison:
            detail["poison"] = True
        batch.append(
            {
                "Source": "onside.app",
                "DetailType": "CheckIn",
                "Detail": json.dumps(detail),
                "EventBusName": bus,
            }
        )
        if len(batch) == 10:
            resp = ev.put_events(Entries=batch)
            assert resp["FailedEntryCount"] == 0, resp
            batch = []
    if batch:
        ev.put_events(Entries=batch)
    put_seconds = time.time() - t0

    decoder = json.JSONDecoder()
    keys, seen = set(), {}
    poison_in_s3 = missing_detail = 0
    parse_seconds = 0.0
    last_change = time.time()
    while time.time() - last_change < 90:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="raw/"):
            for obj in page.get("Contents", []):
                if obj["Key"] in keys:
                    continue
                keys.add(obj["Key"])
                last_change = time.time()
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode()
                p0 = time.time()
                pos = 0
                while pos < len(body):
                    while pos < len(body) and body[pos].isspace():
                        pos += 1
                    if pos >= len(body):
                        break
                    envelope, pos = decoder.raw_decode(body, pos)
                    detail = envelope.get("detail")
                    if detail is None:
                        missing_detail += 1
                        continue
                    if detail.get("poison"):
                        poison_in_s3 += 1
                    seen[detail["event_id"]] = envelope
                parse_seconds += time.time() - p0
        time.sleep(5)

    sample = next(iter(seen.values()), {})
    print(
        json.dumps(
            {
                "events_sent": args.events,
                "expected_in_s3": args.events - len(poison),
                "distinct_in_s3": len(seen),
                "objects": len(keys),
                "poison_in_s3": poison_in_s3,
                "missing_detail": missing_detail,
                "put_seconds": round(put_seconds, 1),
                "parse_seconds": round(parse_seconds, 2),
                "total_seconds": round(time.time() - t0, 1),
                "envelope_keys": sorted(sample),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
