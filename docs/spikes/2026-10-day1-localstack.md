# Day-1 LocalStack spike — findings

Date: 2026-10-09

## Decision: image and auth
- Tag used: 4.14.0 (newest 4.x before 2026-03-01; published 2026-02-26); token needed: no
- Decision and reason: use `localstack/localstack:4.14.0` community image with an empty `LOCALSTACK_AUTH_TOKEN`. It started cleanly ("edition": "community") and all of s3, sqs, events, firehose, dynamodb, lambda, iam, sts and logs reported `available`. The log only shows a banner that the community image will be unified in March 2026.

## EventBridge → Firehose → S3 (10,000 events, 10 poison)
Run A, `--events 10000`:
- distinct_in_s3 / expected_in_s3: 268 / 9990 (**about 97% lost**)
- objects written: 270 (≈ 1 event per object)
- poison reached S3: 0 → `exists:false` honoured: yes
- envelope keys: account, detail, detail-type, id, region, resources, source, time, version
- put_seconds: 211.4; parse_seconds: 0.0; total_seconds: 454.5
- Cause: LocalStack logged `OSError: [Errno 24] Too many open files` and `urllib3 Connection pool is full` (pool size 150). EventBridge-to-Firehose delivery spawns one async thread and one HTTP call per event, and runs out of file descriptors in the container. The loss is silent: `put_events` returned `FailedEntryCount == 0`.

Run B, `--events 1000` (same container, fresh bucket):
- distinct_in_s3 / expected_in_s3: 999 / 999 (no loss)
- objects written: 999 (exactly 1 event per object)
- poison reached S3: 0; missing_detail: 0
- put_seconds: 34.7; parse_seconds: 0.01; total_seconds: 138.4

## Projection to the default backfill (≈ 400k envelopes including Lambda events)
- objects ≈ 400k (one object per envelope, no batching); parse time is dominated by one S3 GET per object, not by JSON parsing (parse_seconds ≈ 0.01 s per 1,000 objects).
- put rate is about 29 events/s (run B) to 47 events/s (run A, while dropping events) → 400k envelopes ≈ 2.4–3.8 hours of puts alone, and the delivery path loses events above a few thousand in a burst.
- Verdict on the 45-minute limit (spec §3.4): **drop LocalStack default to 30,000 check-ins**, and send in bursts of at most about 1,000 events with a pause or drain between them (or poll S3 until the count matches before sending more). Even 30,000 check-ins is borderline at roughly 30–47 events/s (≈ 11–17 min of puts), so Plan 2 should keep the per-burst drain and verify counts at the end.

## Notes for Plan 2
- Firehose `BufferingHints` (60 s / 1 MB) is **ignored**: every event becomes its own S3 object. Key format: `raw/YYYY/MM/DD/HH/<stream>-YYYY-MM-DD-HH-MM-SS-<uuid>`. The consumer must list many small objects and tolerate that; the NDJSON-style `raw_decode` loop is still correct for the 1-line bodies.
- Each body is the full EventBridge envelope (`detail` is a nested object, not a string); no trailing newline separator was needed by the decoder.
- Pattern matching with `{"detail": {"poison": [{"exists": false}]}}` works: all 10 / 1 poison events were filtered out in both runs.
- Delivery is asynchronous and lags the puts by roughly 1–2 minutes at 1k events; the consumer needs a settle/drain wait before counting.
- Silent loss under burst is the main risk. Do not trust `FailedEntryCount`; reconcile counts against what was sent. Raising the container's file-descriptor limit (`ulimits: nofile` in the compose file) was NOT tried and is a candidate fix to test in Plan 2.
- Not tested: Lambda targets and the SQS path.
