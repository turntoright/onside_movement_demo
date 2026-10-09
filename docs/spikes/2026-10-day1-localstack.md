# Day-1 LocalStack spike — findings

Date: 2026-10-09

## Decision: image and auth
- Tag used: 4.14.0 (newest 4.x before 2026-03-01; published 2026-02-26); token needed: no
- Decision and reason: use `localstack/localstack:4.14.0` community image with an empty `LOCALSTACK_AUTH_TOKEN`. It started cleanly ("edition": "community") and all of s3, sqs, events, firehose, dynamodb, lambda, iam, sts and logs reported `available`. The log only shows a banner that the community image will be unified in March 2026.

## EventBridge → Firehose → S3 (10,000 events, 10 poison)
Run A, `--events 10000`:
- distinct_in_s3 / expected_in_s3: 268 / 9990 (**about 97% lost**)
- objects written: 270 for 268 distinct ids, i.e. at least 2 duplicate deliveries, so at-least-once delivery matters (dedupe by event_id)
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
- objects ≈ 400k (one object per envelope, no batching). The per-object S3 GET cost was NOT measured: `parse_seconds` starts after `.read()`, so it excludes GET time by construction (it is only the JSON decode, ≈ 0.01 s per 1,000 objects). Plan 2 must measure per-object GET cost, since 400k GETs is the likely consumer bottleneck.
- put rate is about 29 events/s (run B) to 47 events/s (run A, while dropping events) → 400k envelopes ≈ 2.4–3.8 hours of puts alone, and the delivery path loses events above a few thousand in a burst.
- Verdict on the 45-minute limit (spec §3.4): **drop LocalStack default to 30,000 check-ins**. Even that is borderline at roughly 30–47 events/s (≈ 11–17 min of puts). Mitigation is an UNTESTED hypothesis for Plan 2 to verify: Run A was a sustained sequential stream (~47 ev/s) and failed, while Run B (clean) ran on the same container afterwards, which suggests a backlog/concurrency limit rather than a cumulative leak. Plan 2 should test (a) bursts of at most about 1,000 events with a drain between them and (b) `ulimits: nofile` in the compose file, and in any case reconcile counts at the end.

## Notes for Plan 2
- Firehose `BufferingHints` (60 s / 1 MB) is **ignored**: every event becomes its own S3 object. Key format: `raw/YYYY/MM/DD/HH/<stream>-YYYY-MM-DD-HH-MM-SS-<uuid>`. The consumer must list many small objects and tolerate that; the NDJSON-style `raw_decode` loop is still correct for the 1-line bodies.
- Each body is the full EventBridge envelope (`detail` is a nested object, not a string); no trailing newline separator was needed by the decoder.
- Run B is the valid proof of the poison filter (all 999 non-poison arrived, 0 poison; Run A lost most events so its 0 proves little). Pattern matching with `{"detail": {"poison": [{"exists": false}]}}` works: the 1 poison event was filtered out.
- Delivery is asynchronous, but lag was not measured: `total_seconds` includes the probe's 90 s idle window plus 5 s sleeps. The consumer still needs a settle/drain wait before counting.
- Silent loss under burst is the main risk. Do not trust `FailedEntryCount`; reconcile counts against what was sent. `ulimits: nofile` was NOT tried; test it in Plan 2.
- Not tested: Lambda targets and the SQS path.
