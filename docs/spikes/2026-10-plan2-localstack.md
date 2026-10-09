# Plan 2 spike findings (LocalStack 4.14.0 community, no token)

> Copied from the spike scratch dir on 2026-10-10. Spike sources kept under `spikes/plan2/` (main.tf, handler.py, q3_transactions.py, docker-compose.yml).

Date 2026-10-10. Everything lives in this dir; the repo was not modified. Compose project `plan2spike`, brought `down -v` at the end.
Files: docker-compose.yml, probe.py (copy), probe_mt.py (4-thread variant), q2.py, lambda/handler.py, poison.py, logs.py, q3.py, tf/main.tf, *.out/*.json raw outputs.
Fact = measured here. Hypothesis = marked as such.

## Q1  EventBridge -> Firehose -> S3 with ulimits nofile=65536

Setup: compose adds `ulimits: nofile: {soft: 65536, hard: 65536}` (verified `ulimit -n` = 65536 in the container). Run `probe.py --events 10000` (the Day-1 probe, sequential PutEvents, batches of 10), fresh container.

Measured:
| run | sender | distinct / expected | objects | put rate | "Too many open files" in log |
|---|---|---|---|---|---|
| Day 1 (no ulimit) | 1 thread | 268 / 9990 | 270 | 47 ev/s | yes |
| A: ulimit 65536 | 1 thread | **9990 / 9990** | 10,071 (81 duplicates, 0.8%) | 10000/428 s = **23 ev/s** | 0 |
| B: ulimit 65536 | 4 threads (probe_mt.py) | **9988 / 9990** (seq 696, 932 lost) | 11,585 (1,595 duplicates, 16%) | 10000/499 s = **20 ev/s** (threads did not help) | 0 |

- poison filter (`exists:false`) honoured in both runs (poison_in_s3 = 0).
- Run B loss cause, from LocalStack log: `ERROR l.s.firehose.provider : Unable to put records [...]` and `Connect timeout on endpoint URL: "http://s3.localhost.localstack.cloud:4566/spike-.../raw/..."`. So Firehose's internal S3 write timed out and the record was dropped without a retry reaching the sender; `put_events` still returned FailedEntryCount 0 (silent loss persists, only much smaller).
- Also seen in run B: `I/O operation on closed file` errors; duplicates (at-least-once) rose from 0.8% to 16% under concurrency.
- Delivery is synchronous-ish: put rate with the Firehose target is 20-23 ev/s versus ~265 ev/s when the same bus has only SQS targets (Q2). Firehose delivery is the throughput cap of the whole bus on LocalStack.
- 1 event = 1 S3 object still holds (buffering hints ignored).
- Consumer cost (previously unmeasured): 11,585 GetObject calls with 8 threads took 21.8 s (~530 GET/s). 400k objects would be ~12.5 min of GETs; fine.
- Bursts of 1,000 with 30 s drain: NOT run, because the sequential run already had no loss with the ulimit. (Hypothesis only: it would also be fine.)

Conclusion Q1: the ulimit fixes the catastrophic loss (97% -> 0% for a sequential 10k stream). It is NOT fully reliable: with 4 concurrent senders 2/9,990 were dropped (S3 connect timeout) and 16% duplicated. Reliable only for a single sequential sender at ~20 ev/s, plus end-of-run count reconciliation. I did not test above 10k events.

## Q2  EventBridge -> SQS -> Lambda on LocalStack

Built by `q2.py` (boto3): bus `q2-bus`; rule A (source onside.app, detail-type CheckIn|CheckOut) -> SQS standard (visibility 30 s, DLQ maxReceiveCount 5); rule "derived" (source onside.standdown) -> a separate SQS queue used only to COUNT derived events; Lambda python3.12 (256 MB, timeout 25 s), ESM batch size 10 + ReportBatchItemFailures. Handler (lambda/handler.py): parse envelope from SQS body, put DynamoDB item, PutEvents one derived event (source onside.standdown) per record, return batchItemFailures for `detail.poison`.

Functional (measured):
- End to end works. 2,000 events -> exactly 2,000 DynamoDB items; derived queue holds exactly 2,000 derived events; the visit queue is empty (0 visible, 0 in flight) afterwards => derived events do NOT loop back (source filter works).
- Poison: the Lambda fails the record (`batchItemFailures` honoured, other 9 records in the batch deleted), message redelivered 5 times and lands in the DLQ. Invocation timestamps of the retries (s): 0, 36, 83, 119, 155, then DLQ. Poison put -> visible in DLQ: **181.8 s** (poison.py, idle system) and ~179 s in the 2k run. That is ~3 min, not the spec's "about 2.5 min" (LocalStack redrive timing is looser than 5 x 30 s). The spec's 5-minute `make poison` timeout still fits, with ~2 min margin.

Config needed (measured):
- Mount `/var/run/docker.sock`; LocalStack spawns `public.ecr.aws/lambda/python:3.12` as a sibling container. No LAMBDA_* env vars and no docker network config were needed for connectivity: the Lambda got `AWS_ENDPOINT_URL=http://172.19.0.2:4566` injected, so plain `boto3.client("dynamodb")` in the handler hits LocalStack with no code change.
- First create_function -> Active: **29.6 s** when the Lambda runtime image had to be pulled (one time); **2.9 s** afterwards. Python init in the handler: 0.18-0.22 s (COLD_INIT_SECONDS); first invocation duration 1.7-3.2 s (includes container start), then ~0.25-0.27 s median per 10-record batch.
- **Important knob**: `LAMBDA_EVENT_SOURCE_MAPPING_POLL_INTERVAL_SEC` (default 1, found in localstack/config.py inside the image). With the default, the SQS poller makes one poll per second, so batch size 10 caps throughput at ~10 ev/s.

Throughput (2,000 events, PutEvents 4 threads at ~260 ev/s, time from first put until DynamoDB count = 2,000):
| setting | E2E seconds | events/s |
|---|---|---|
| POLL_INTERVAL default (1 s), batch 10 | 269.5 | **7.4** |
| POLL_INTERVAL 0.05, batch 10 | 78.4 | **25.5** |
| POLL_INTERVAL 0.05 + ESM ScalingConfig MaximumConcurrency 5 | 77.8 | 25.7 (no gain; LocalStack ran one Lambda container, 203 invocations, ~0.38 s each) |
Ceiling at 0.05 s is the serial Lambda execution (10 DynamoDB puts + 1 PutEvents per invocation). The real handler (reads + TransactWriteItems + PutEvents per record) will be heavier; expect lower (hypothesis, not measured), perhaps 10-15 ev/s.

Conclusion Q2: Lambda+SQS+ESM+DLQ+ReportBatchItemFailures are all functional on community LocalStack 4.14.0. Set `LAMBDA_EVENT_SOURCE_MAPPING_POLL_INTERVAL_SEC=0.05` in compose. Do not rely on MaximumConcurrency.

## Q3  TransactWriteItems semantics (LocalStack and moto 5.2.3, boto3 1.43.110; q3.py, q3.out)

Transaction = Update person_state (`SET version = :new`, condition `attribute_not_exists(person_id)` if read==0 else `version = :read`) + Put evaluations (`attribute_not_exists(checkin_event_id)`). Item 0 = person_state, item 1 = evaluations.

Results are IDENTICAL on LocalStack and moto:
| case | exception | CancellationReasons (index 0, index 1) |
|---|---|---|
| (a) version conflict only | `TransactionCanceledException` | `[{"Code":"ConditionalCheckFailed","Message":"The conditional request failed"}, {"Code":"None"}]` |
| (b) evaluation exists only | same | `[{"Code":"None"}, {"Code":"ConditionalCheckFailed","Message":"The conditional request failed"}]` |
| (c) both | same | `[{"Code":"ConditionalCheckFailed","Message":"The conditional request failed"}, {"Code":"ConditionalCheckFailed","Message":"The conditional request failed"}]` |
- Error dict: `e.response["Error"] = {"Code": "TransactionCanceledException", "Message": "Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed, None]"}`. `e.response["CancellationReasons"]` is a list aligned to TransactItems order; an item that did not fail has `{"Code":"None"}` with NO Message key. Catch via `client.exceptions.TransactionCanceledException` (or `ClientError` with that Code).
- The same exception shape comes from `resource.meta.client.transact_write_items` (cases tested: one reason in a one-item transaction).
- Decimal: client API returns `{'N': '2'}` (string); the resource API (`Table.get_item`) returns `Decimal('2')`. Identical on both backends.
- Gotcha (my own test bug, worth knowing): LocalStack rejects unused ExpressionAttributeValues with `ValidationException: Value provided in ExpressionAttributeValues unused in expressions: keys: {:read}` (real AWS does too). Moto does NOT validate this and accepted it. So when read==0, do not pass `:read`.
- The CancellationReasons code for the person_state update is also ConditionalCheckFailed for a missing item when `version = :read` is used (case (b) in my first, flawed run showed this), so the "version conflict vs evaluation exists" discrimination must use index 1 (evaluation) as the spec describes.

Conclusion Q3: the spec's retry logic (index 1 ConditionalCheckFailed => already processed; otherwise retry) behaves identically on LocalStack and moto. Moto works as the unit-test double for DynamoDbStore.

## Q4  Terraform against LocalStack (hashicorp/aws 6.68.0 resolved from `~> 6.0`, archive 2.x; tf/main.tf)

Key provider block (as used):
```hcl
provider "aws" {
  region                      = "ap-southeast-2"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true
  endpoints {
    s3 = "http://localhost:4566"   sqs = "http://localhost:4566"   events = "http://localhost:4566"
    firehose = "http://localhost:4566"   dynamodb = "http://localhost:4566"   lambda = "http://localhost:4566"
    iam = "http://localhost:4566"   sts = "http://localhost:4566"   logs = "http://localhost:4566"
  }
}
```
(in the file each endpoint is on its own line.) Stack: S3 bucket (force_destroy), DynamoDB table, SQS queue + DLQ (redrive 5), event bus + rule A + SQS target + aws_sqs_queue_policy, Lambda from archive_file + IAM role/policy + aws_lambda_event_source_mapping (batch 10, ReportBatchItemFailures), Firehose extended_s3 + IAM roles + rule B (exists:false pattern) + Firehose target with role_arn.

Measured:
- `terraform apply`: **succeeded first time, 19 resources, no workaround needed, no errors or warnings**. Wall time **1m29s**. The time is dominated by waiting: aws_sqs_queue ~25-27 s each (x2), aws_sqs_queue_policy 25 s, aws_dynamodb_table 10 s, aws_lambda_function 29 s (first Lambda image start). Hypothesis: the provider polls queue attributes until consistent, which is slow against LocalStack; not investigated.
- Functional check of the TF-built stack: 5 events + 1 poison to the bus -> 5 DynamoDB items, 10 S3 objects (5 app + 5 derived), poison absent from S3.
- Second `terraform plan -detailed-exitcode`: no differences (9 s), i.e. no perpetual drift.
- `terraform destroy`: **succeeded, 19 destroyed, 2m04s** (non-empty bucket destroyed thanks to force_destroy; DLQ destroy alone 42 s).
- `terraform init` fetched providers from the registry without issue (needs network on first run; lock file in tf/).

Conclusion Q4: the whole planned resource set applies, drifts nothing and destroys cleanly on LocalStack 4.14.0. Budget ~1.5 min apply + ~2 min destroy.

## Q5  Recommendation

Facts driving it: Firehose path gives 0 loss only for ONE sequential sender at ~20-23 ev/s, with ulimit; 2 drops + 16% duplicates with 4 senders; it throttles the whole bus (rule B target makes PutEvents ~12x slower than without it: ~23 vs ~265 ev/s). In the real design the generator and the Lambda (derived events) both PutEvents concurrently, which is closer to run B than run A (not tested together).

1. LocalStack S3 path: switch rule B to an "archiver" Lambda (SQS-buffered, batching events into NDJSON objects, key `raw/<arrival-hour>/<uuid>.ndjson`) -- reasoning: it removes the Firehose bottleneck and the silent-loss mode, cuts object count 100x+ (hypothesis: batch of 10-100 per object), and the downstream reader (`settle.py` with raw_decode) already handles concatenated records. Spec cut-line item 6 allows it. Caveat: untested here; the archiver shares the serial-Lambda limit (~25 ev/s at batch 10; use batch size 100 with a batching window to raise it -- hypothesis).
   Keep Firehose as the real-AWS path behind a Terraform variable (e.g. `s3_path = "firehose" | "archiver"`), since the Firehose resources apply and work in TF (Q4) and AWS Firehose does not have this problem. If you refuse to build the archiver, minimum viable Firehose settings are: ulimit 65536, single sequential sender, <= 10k events, dedupe by event_id, and an end-of-run count reconciliation that fails loudly.
2. Compose settings: `ulimits nofile 65536`, `LAMBDA_EVENT_SOURCE_MAPPING_POLL_INTERVAL_SEC=0.05`, docker.sock mounted, pre-pull `public.ecr.aws/lambda/python:3.12`. Raise the `make poison` timeout margin (DLQ arrival ~3 min).
3. LocalStack default backfill size. Budget 45 min = 2,700 s. A check-in = ~2 app events (Lambda-processed) + ~2 derived events.
   - Lambda-bound (measured simple handler 25.5 ev/s => 12.7 check-ins/s => ~34k check-ins; real handler assumed 2-3x slower => ~12-17k).
   - Firehose-bound (if kept): bus puts incl. derived = 4 events per check-in at ~20-23 ev/s => ~5.5 check-ins/s => ~15k at 100% of budget, ~12k with headroom.
   - Recommended LocalStack default: **10,000 check-ins** (~40k envelopes; est. 25-35 min), with `--checkins` override; 30,000 only as an explicit long run. Day-1's 30,000 default is too high given measured rates unless the archiver path and a lighter handler are confirmed. All of these projections beyond 2,000 events are extrapolations; measure the real handler in Plan 2 before fixing the number.
