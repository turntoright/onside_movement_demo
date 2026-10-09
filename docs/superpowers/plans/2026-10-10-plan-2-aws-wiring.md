# Plan 2 — AWS wiring on LocalStack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the whole Onside event chain on LocalStack with the same Terraform that will
deploy to AWS: generator → EventBridge → SQS → stand-down Lambda (DynamoDB) and archiver Lambda
(S3 `raw/`) → settle and compact → dbt reading S3, with a passing integration test.

**Architecture:**
- **Lambdas.** `standdown_checker.handler` wraps the Plan 1 processor with a `DynamoDbStore`
  (the same `StateStore` contract as the in-memory store, now run against both) and reports
  partial batch failures. `archiver.handler` writes each SQS batch as one NDJSON object.
- **Infrastructure.** One Terraform codebase (`infra/terraform/`); a `localstack_endpoint`
  variable switches the provider to LocalStack. State per environment through a local backend
  path. LocalStack 4.14.0 runs from `docker-compose.yml`.
- **Generator for `local`/`aws`.** Reference data to S3 and DynamoDB, manifests mirrored to S3
  before emitting, PutEvents in send order, then `settle` scans `raw/`, gates on completeness
  and quiet queues, and compacts to `compacted/<env>/<run_id>/`. dbt targets `local`/`aws`
  read that prefix with DuckDB `httpfs`.

**Tech Stack:**
- Python 3.12, boto3 (runtime-provided in Lambda), moto 5.2 for unit tests
- Terraform ≥ 1.6 (dry run: 1.15.8) with hashicorp/aws `~> 6.0` and hashicorp/archive `~> 2.4`
- LocalStack `4.14.0` community (no token), Docker Compose v2
- DuckDB 1.5 + dbt-duckdb 1.11 (`httpfs`, `aws` extensions)
- pytest 9, ruff 0.16

**Spec:** `docs/superpowers/specs/2026-10-09-onside-movement-demo-design.md` (revision 7.1).
**Design brief (binding decisions):** `docs/superpowers/plans/2026-10-10-plan-2-design-brief.md`.
Also read: `docs/superpowers/plans/2026-10-09-plan-1-execution-notes.md` (rulings),
`docs/spikes/2026-10-plan2-localstack.md` (measured LocalStack behaviour). Authority:
spec > brief > this plan. Section numbers (§) refer to the spec.

**Dry run (2026-10-10, while writing the plan).** Every code block was applied to a scratch
worktree of `feat/plan-2-aws-wiring` with its own venv, task by task (each RED step was run and
failed as stated, each GREEN step passed), and the result was diffed against the tree that ran
the LocalStack checks: identical. Results:
- unit tests: 177 passed, 1 skipped (the integration test without `LOCALSTACK=1`) in 84 s; `make fmt` is a no-op on the plan's code; `ruff check` and `ruff format --check` clean; `make tf-check` valid.
- `make up`: 12 s. `make deploy ENV=local` on a fresh stack: 1 min 37 s, 25 resources (2 min 00 s
  the first time, including the provider download); redeploy 0 changes; `plan -detailed-exitcode` 0;
  `make destroy ENV=local` 2 min 10 s.
- Smoke (Task 10 Step 8): 30 evaluations, 60 envelopes archived in 8 raw objects (max 25 lines), 10 s.
- Reduced end-to-end (`SCALE=aws`: 2,828 check-ins, 2,602 check-outs, 75 plant movements,
  5,604 events emitted at 131 events/s): settled in 280 s, 324 s wall for `make generate`
  (≈ 17 events/s end to end; ≈ 22–24 events/s steady through the stand-down Lambda).
  11,624 envelopes archived in 1,150 raw objects (mean 10.1 lines, max 100: batch size 100
  with the 5 s window works on LocalStack).
- `make dbt-build ENV=local`: PASS=77 WARN=0 ERROR=0 (13 s). Reconciliation: 485 matched,
  19 missed `origin_not_seen`/offline, 4 missed `origin_not_seen`/reordered, 0 false alerts,
  0 unexplained.
- Live run `N=50`: settled in 45 s; dbt still ERROR=0. `make poison K=2`: visit DLQ reached 2
  after 2 min 56 s.
- `make integration` (fresh stack): 1 passed in 187.5 s (4 min 56 s in total); on the replayed
  tree 194 s (5 min 00 s). `make sim-e2e` (pinned anchor): PASS=77 ERROR=0.
- Full `local` scale (Task 13, run in the dry run too): 9,227 check-ins, 8,494 check-outs, 250 plant movements = 18,313 events emitted
  at 125 events/s; settled in 829 s; 979 s wall including setup (≈ 18.7 events/s end to end,
  well under 45 minutes, so the `local` scale stays at 10,000 check-ins); 37,371 envelopes
  compacted; dbt PASS=77 ERROR=0; reconciliation 975 matched, 42 offline, 1 reordered,
  4 `origin_end_not_seen`, 0 false alerts, 0 unexplained.
- Teardown: `docker compose down -v`, worktree removed. Nothing ran against real AWS.

A failure while executing therefore most likely means a copy slip or an environment difference,
not a design gap.

**Out of scope (Plan 3):** Dagster, the CI LocalStack job, the real AWS deploy and budget alarm,
the README rewrite. **Never run anything against real AWS in this plan, and never push.**

## Global Constraints

- Python **3.12**; the project venv is `.venv`. **Always call `.venv/bin/dbt`** (the global
  `~/.local/bin/dbt` is dbt Fusion 2.0 without a DuckDB adapter).
- **Version pins:** `dbt-duckdb~=1.11.0`, `duckdb>=1.5,<1.6`, `networkx~=3.7`, `pytest~=9.1`,
  `ruff~=0.16`, `boto3>=1.40`, `tzdata>=2026.1`, **`moto[s3,dynamodb,sqs,events]~=5.2`**,
  Terraform providers **`hashicorp/aws ~> 6.0`** and `hashicorp/archive ~> 2.4`, image
  **`localstack/localstack:4.14.0`**, Lambda runtime `python3.12`.
- **LocalStack settings:** `ulimits nofile 65536`, `LAMBDA_EVENT_SOURCE_MAPPING_POLL_INTERVAL_SEC=0.05`,
  `/var/run/docker.sock` mounted, `PERSISTENCE=0`, `SERVICES=s3,sqs,events,dynamodb,lambda,iam,sts,logs`;
  endpoint `http://localhost:4566`, region `ap-southeast-2`, credentials `test`/`test`.
  Only one LocalStack may listen on :4566 — check `docker ps` before `make up`.
- **Never touch real AWS.** Never run `make deploy|destroy|setup|generate|settle|poison|dbt-build`
  with `ENV=aws`. Unit tests run inside moto (the `aws` fixture) or with `generator.aws.client`
  monkeypatched; no test may build a client for `localhost:4566` unless it is an integration test
  (skipped unless `LOCALSTACK=1`). **Never push.**
- **Timestamps:** ISO 8601 with a UTC offset; "local" means `Pacific/Auckland`; event times via
  `standdown_checker.events.iso()` (seconds), `processed_at` in milliseconds. Archive keys and
  manifest `started_at` use the UTC wall clock.
- **Ids:** generator events `uuid5(run_id, str(seq))`; derived events
  `uuid5(<fixed namespace>, "<kind>:<key>")`, kind in `eval`, `co`, `alert`.
- **Rules:** `shared/standdown_rules.csv` is the single source (bundled next to the Lambda package).
- **Environments:** `ENV=sim|local|aws`. The Makefile keeps `ENV = sim` for data targets;
  infrastructure targets (`deploy`, `destroy`) refuse unless `ENV=local|aws` is on the command line.
- **Paths (gitignored):** `state/<env>/` (`outputs.json`, `setup.json`, `generator_state.json`,
  `manifests/`, `poison/`, `spool/`, `settled/`), `data/<env>/ref/`, `warehouse/<env>.duckdb`,
  `build/`, `infra/terraform/state/<env>.tfstate`, `infra/terraform/.terraform/`.
  Commit `infra/terraform/.terraform.lock.hcl`.
- **S3 layout:** `ref/{properties,people,companies}.json`, `raw/YYYY/MM/DD/HH/<uuid>.ndjson`,
  `compacted/<env>/<run_id>/part-NNNN.ndjson`, `control/manifests/<run_id>.json`,
  `control/poison/<run_id>.json`, `control/settled/<run_id>.json`.
- All code comments and docs are in English.
- Before each commit, run `make fmt` and `make test`; both must pass (`make test` takes about
  1.5 minutes; it needs no Docker).
- **Commit trailers:** end every commit message with these two lines:
  ```
  Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
  ```

## Review Focus

Five conditions the spec implies that the obvious tests would miss. Each is pinned by a test in
the task that owns the code:

1. **A person's first commit on LocalStack/AWS.** moto accepts an unused `:read` value; LocalStack
   and AWS reject it, so every first check-in would fail only in the real chain. Pinned in Task 2,
   `test_first_commit_sends_no_unused_read_value`.
2. **PutEvents rejects part of a batch after the commit.** Only the owning record may fail, and its
   redelivery must re-emit byte-identical bodies (the "already processed" path). Pinned in Task 3,
   `test_rejected_entry_fails_only_its_record_and_redelivery_reemits_identically`.
3. **Settle interrupted (timeout or crash) and resumed with `make settle`.** The key cache must
   skip parsed objects, the spool must survive, and a torn last line must not break the resume.
   Pinned in Task 7, `test_timeout_keeps_the_spool_and_a_resume_reads_only_new_objects` and
   `test_a_torn_spool_line_is_dropped_on_resume`.
4. **Archived data stuck in the archive DLQ.** Settle would otherwise wait until its timeout and
   then blame the wrong thing. Pinned in Task 7, `test_archive_dlq_messages_fail_fast`.
5. **LocalStack restarted (or Docker restarted) under existing local state.** `generate` must stop
   with "run `make up`" instead of emitting into an empty stack. Pinned in Task 8,
   `test_lost_localstack_data_is_detected`.

---

## File Structure

```
onside_movement_demo/
├── pyproject.toml                          # Task 1: moto pin, archiver package, integration marker
├── docker-compose.yml  .env.example        # Task 9: LocalStack 4.14.0 settings
├── Makefile                                # Task 8 (data), 9 (up/down/package), 10 (deploy/destroy/
│                                           #   tf-check), 11 (dbt on S3), 12 (integration)
├── .github/workflows/ci.yml                # Task 10: terraform fmt + validate job
├── README.md                               # Task 13: Plan 2 status
├── lambdas/
│   ├── standdown_checker/
│   │   ├── store.py                        # Task 1: contract docstring (copies of properties)
│   │   ├── dynamo_store.py                 # Task 2: StateStore on DynamoDB
│   │   └── handler.py                      # Task 3: SQS batch → processor → PutEvents
│   └── archiver/
│       ├── __init__.py
│       └── handler.py                      # Task 4: SQS batch → one NDJSON object in raw/
├── generator/
│   ├── local_sim.py                        # Task 1: get_property returns a copy
│   ├── config.py  state.py                 # Task 5: envs, local scale, manifest start, hints
│   ├── aws.py                              # Task 5: clients, Terraform outputs, Connection
│   ├── emit.py                             # Task 6: PutEvents batching and retry
│   ├── queues.py  settle.py                # Task 7: queue depth; settle and compact
│   └── cli.py                              # Task 8: setup/generate/settle/poison for local/aws
├── infra/terraform/                        # Task 10: versions, variables, provider, storage,
│                                           #   events, lambdas, outputs (+ .terraform.lock.hcl)
├── dbt/profiles.yml                        # Task 11: targets local and aws
└── tests/
    ├── store_contract.py                   # Task 1: stale non-zero version tests
    ├── aws_fixtures.py  conftest.py        # Task 2: moto stack and the `aws` fixture
    ├── test_dynamo_store.py                # Task 2
    ├── test_standdown_handler.py           # Task 3
    ├── test_archiver.py                    # Task 4
    ├── test_aws_env.py                     # Task 5
    ├── test_emit.py                        # Task 6
    ├── test_settle.py                      # Task 7
    ├── test_cli_aws.py                     # Task 8
    └── integration/test_localstack_chain.py  # Task 12 (LOCALSTACK=1 only)
```

Order: unit-testable pieces first (Tasks 1–8, no Docker), then infrastructure (9–10), then the
end-to-end proofs on LocalStack (11–13).

---

### Task 1: Close the store-contract gap and add the AWS test toolchain

The Plan 1 final review parked a real gap: the contract suite never commits on a stale
**non-zero** version, so a DynamoDB store with a broken `version = :read` condition could pass.
This task adds those tests (and a "both conditions fail leaves state unchanged" test) before any
DynamoDB code exists. It also pins `moto` and registers the `integration` pytest marker.
The one RED test here is `test_returned_property_is_a_copy` (a deferred Plan 1 minor:
`InMemoryStore.get_property` returned its shared dict).

**Files:**
- Modify: `pyproject.toml` (moto dev pin, `archiver` package, `integration` marker)
- Modify: `tests/store_contract.py` (append five contract tests)
- Modify: `generator/local_sim.py` (`get_property` returns a copy)
- Modify: `lambdas/standdown_checker/store.py` (contract docstring)

**Interfaces:**
- Consumes: the Plan 1 contract suite (`tests/store_contract.py`, fixture `store_factory(properties)`).
- Produces: `VISIT_B` in `tests.store_contract`; the contract now also requires `get_property`
  to return a copy. Every store (InMemoryStore now, DynamoDbStore in Task 2) runs the whole suite.

- [ ] **Step 1: Pin the test dependencies**

Replace `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"

[project]
name = "onside-movement-demo"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
  "boto3>=1.40",
  "tzdata>=2026.1",
  "duckdb>=1.5,<1.6",
  "dbt-duckdb~=1.11.0",
  "networkx~=3.7",
  "scipy>=1.14",
  "pandas>=2.2",
]

[project.optional-dependencies]
dev = ["pytest~=9.1", "ruff~=0.16", "moto[s3,dynamodb,sqs,events]~=5.2"]

[tool.setuptools.packages.find]
where = [".", "lambdas"]
include = ["generator*", "tracing*", "standdown_checker*", "archiver*"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = [".", "lambdas"]
markers = ["integration: needs a deployed LocalStack stack; runs only with LOCALSTACK=1"]

[tool.ruff]
line-length = 100
target-version = "py312"
extend-exclude = ["dbt/target", "dbt/logs", "dbt/dbt_packages", "spikes", "docs"]

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP"]
```

Run: `.venv/bin/pip install -e '.[dev]'`
Expected: ends with `Successfully installed ... moto-5.2.x ...` (or "Requirement already satisfied").

- [ ] **Step 2: Append the contract tests**

Append to the end of `tests/store_contract.py`:

```python


VISIT_B = {"checkin_event_id": "B", "property_id": "P1"}


def test_commits_chain_versions(store_factory):
    store = store_factory(PROPS)
    store.commit("U1", 0, [VISIT], None)
    store.commit("U1", 1, [VISIT, VISIT_B], None)
    store.commit("U1", 2, [VISIT_B], None)
    assert store.load_person("U1") == (3, [VISIT_B])


def test_stale_nonzero_version_conflicts(store_factory):
    store = store_factory(PROPS)
    store.commit("U1", 0, [VISIT], None)
    store.commit("U1", 1, [VISIT, VISIT_B], None)
    with pytest.raises(ConflictError):
        store.commit("U1", 1, [], None)  # read at version 1, but the item is now at 2
    assert store.load_person("U1") == (2, [VISIT, VISIT_B])


def test_stale_nonzero_version_with_evaluation_leaves_everything_unchanged(store_factory):
    store = store_factory(PROPS)
    store.commit("U1", 0, [VISIT], None)
    store.commit("U1", 1, [VISIT, VISIT_B], None)
    with pytest.raises(ConflictError):
        store.commit("U1", 1, [], {"checkin_event_id": "C"})
    assert store.get_evaluation("C") is None
    assert store.load_person("U1") == (2, [VISIT, VISIT_B])


def test_both_conditions_failing_leaves_state_unchanged(store_factory):
    store = store_factory(PROPS)
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A", "breach": False})
    store.commit("U1", 1, [VISIT, VISIT_B], None)
    with pytest.raises(EvaluationExistsError):
        store.commit("U1", 1, [], {"checkin_event_id": "A", "breach": True})
    assert store.load_person("U1") == (2, [VISIT, VISIT_B])
    assert store.get_evaluation("A") == {"checkin_event_id": "A", "breach": False}


def test_returned_property_is_a_copy(store_factory):
    store = store_factory(PROPS)
    store.get_property("P1")["property_type"] = "changed"
    assert store.get_property("P1") == {"property_type": "poultry", "owner_company_id": "C1"}
```

- [ ] **Step 3: Run them against the in-memory store**

Run: `.venv/bin/pytest tests/test_memory_store.py -q`
Expected: `1 failed, 17 passed`; the failure is `test_returned_property_is_a_copy`.
The four version tests already pass on `InMemoryStore` (it implemented the branched condition in
Plan 1); they exist to hold DynamoDB to it in Task 2.

- [ ] **Step 4: Return a copy from `InMemoryStore.get_property`**

In `generator/local_sim.py`, replace this text (it occurs once):

```python
            return self.properties[property_id]
```

with:

```python
            return dict(self.properties[property_id])
```

In `lambdas/standdown_checker/store.py`, replace this text (it occurs once):

```python
    - Visits and evaluations returned are copies; mutating them never changes the store.
```

with:

```python
    - Visits, evaluations and properties returned are copies; mutating them never changes
      the store.
```

- [ ] **Step 5: Run the store tests**

Run: `.venv/bin/pytest tests/test_memory_store.py -q`
Expected: `18 passed`.

- [ ] **Step 6: Commit**

```bash
make fmt && make test
git add pyproject.toml \
  tests/store_contract.py \
  generator/local_sim.py \
  lambdas/standdown_checker/store.py
git commit -m "$(cat <<'EOF'
Store contract: stale non-zero versions, copies of properties; moto for tests

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

---

### Task 2: `DynamoDbStore` (spec §6.1, brief decision 3)

**Files:**
- Create: `tests/aws_fixtures.py` (a moto copy of the Terraform stack, reused by Tasks 3–8)
- Create: `tests/conftest.py` (the `aws` fixture)
- Create: `lambdas/standdown_checker/dynamo_store.py`
- Test: `tests/test_dynamo_store.py` (runs the whole contract suite on moto, plus DynamoDB specifics)

**Interfaces:**
- Consumes: `standdown_checker.store` errors and contract; `tests.helpers` (`PROPERTIES`, `checkin`,
  `checkout`, `run`).
- Produces:
  - `DynamoDbStore(client, person_table: str, evaluation_table: str, property_table: str)`
    implementing `StateStore`; attribute `.client` (a boto3 low-level DynamoDB client).
    Items: `person_state` {person_id S, version N, visits S (JSON)}, `evaluations`
    {checkin_event_id S, body S (JSON)}, `properties` {property_id S, property_type S,
    owner_company_id S}.
  - pytest fixture `aws` → `tests.aws_fixtures.MotoStack` with `.dynamodb .s3 .sqs .events`
    clients, `.bucket` (`onside-local-000000000000`), `.bus_name` (`onside-events`),
    `.queue_urls` (keys `visit_queue`, `visit_dlq`, `archive_queue`, `archive_dlq`),
    `.outputs()` (same shape as `terraform output -json`), `.put_properties(dict)`.
  - `tests.aws_fixtures.REGION = "ap-southeast-2"`.

Notes:
- moto accepts unused `ExpressionAttributeValues`, but LocalStack and AWS reject them (spike Q3),
  so a spy test pins that `:read` is only sent when `version_read > 0`.
- Cancellation reasons: index 1 (the evaluation put) `ConditionalCheckFailed` means "already
  processed" and wins; any other cancellation is a retryable `ConflictError` (spec §6.1).
- The `aws` fixture sets fake credentials and removes `AWS_PROFILE`/`AWS_ENDPOINT_URL`, so no
  unit test can reach real AWS or LocalStack.

- [ ] **Step 1: Write the moto stack, the fixture and the failing tests**

Create `tests/aws_fixtures.py`:

```python
"""A moto copy of the Terraform stack (infra/terraform), for unit tests only."""

from __future__ import annotations

from dataclasses import dataclass

import boto3

REGION = "ap-southeast-2"
TABLE_KEYS = {
    "person_state": "person_id",
    "evaluations": "checkin_event_id",
    "properties": "property_id",
}


@dataclass
class MotoStack:
    dynamodb: object
    s3: object
    sqs: object
    events: object
    bucket: str
    bus_name: str
    queue_urls: dict[str, str]

    @classmethod
    def create(cls) -> MotoStack:
        dynamodb = boto3.client("dynamodb", region_name=REGION)
        for table, key in TABLE_KEYS.items():
            dynamodb.create_table(
                TableName=table,
                KeySchema=[{"AttributeName": key, "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": key, "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
        s3 = boto3.client("s3", region_name=REGION)
        bucket = "onside-local-000000000000"
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": REGION})
        sqs = boto3.client("sqs", region_name=REGION)
        queue_urls = {
            name: sqs.create_queue(QueueName=f"onside-{name.replace('_', '-')}")["QueueUrl"]
            for name in ("visit_queue", "visit_dlq", "archive_queue", "archive_dlq")
        }
        events = boto3.client("events", region_name=REGION)
        events.create_event_bus(Name="onside-events")
        return cls(dynamodb, s3, sqs, events, bucket, "onside-events", queue_urls)

    def outputs(self) -> dict:
        """The same shape as `terraform output -json` (infra/terraform/outputs.tf)."""
        values = {
            "region": REGION,
            "bucket": self.bucket,
            "person_table": "person_state",
            "evaluation_table": "evaluations",
            "property_table": "properties",
            "bus_name": self.bus_name,
            "visit_queue_url": self.queue_urls["visit_queue"],
            "visit_dlq_url": self.queue_urls["visit_dlq"],
            "archive_queue_url": self.queue_urls["archive_queue"],
            "archive_dlq_url": self.queue_urls["archive_dlq"],
            "standdown_function": "onside-standdown-checker",
            "archiver_function": "onside-archiver",
        }
        return {k: {"sensitive": False, "type": "string", "value": v} for k, v in values.items()}

    def put_properties(self, properties: dict[str, dict]) -> None:
        for property_id, prop in properties.items():
            self.dynamodb.put_item(
                TableName="properties",
                Item={
                    "property_id": {"S": property_id},
                    "property_type": {"S": prop["property_type"]},
                    "owner_company_id": {"S": prop["owner_company_id"]},
                },
            )
```

Create `tests/conftest.py`:

```python
import pytest
from moto import mock_aws

from tests.aws_fixtures import REGION, MotoStack


@pytest.fixture
def aws(monkeypatch):
    """A fresh moto account holding the Terraform stack. Nothing leaves the process."""
    for name, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": REGION,
    }.items():
        monkeypatch.setenv(name, value)
    for name in ("AWS_PROFILE", "AWS_ENDPOINT_URL"):
        monkeypatch.delenv(name, raising=False)
    with mock_aws():
        yield MotoStack.create()
```

Create `tests/test_dynamo_store.py`:

```python
import pytest
from botocore.stub import Stubber
from standdown_checker.dynamo_store import DynamoDbStore
from standdown_checker.store import ConflictError

from tests.helpers import PROPERTIES, checkin, checkout, run
from tests.store_contract import *  # noqa: F403  (contract suite, collected for DynamoDbStore)
from tests.store_contract import PROPS, VISIT


def make_store(stack) -> DynamoDbStore:
    return DynamoDbStore(stack.dynamodb, "person_state", "evaluations", "properties")


@pytest.fixture
def store_factory(aws):
    def factory(properties):
        aws.put_properties(properties)
        return make_store(aws)

    return factory


def test_first_commit_sends_no_unused_read_value(aws):
    store = make_store(aws)
    sent = []
    original = store.client.transact_write_items

    def spy(**kwargs):
        sent.append(kwargs)
        return original(**kwargs)

    store.client.transact_write_items = spy
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A"})
    store.commit("U1", 1, [VISIT], {"checkin_event_id": "B"})
    first, second = (s["TransactItems"][0]["Update"] for s in sent)
    assert first["ConditionExpression"] == "attribute_not_exists(person_id)"
    assert ":read" not in first["ExpressionAttributeValues"]
    assert second["ConditionExpression"] == "version = :read"
    assert second["ExpressionAttributeValues"][":read"] == {"N": "1"}


def test_items_hold_json_strings(aws):
    make_store(aws).commit("U1", 0, [VISIT], {"checkin_event_id": "A", "nights": 2})
    person = aws.dynamodb.get_item(TableName="person_state", Key={"person_id": {"S": "U1"}})
    evaluation = aws.dynamodb.get_item(
        TableName="evaluations", Key={"checkin_event_id": {"S": "A"}}
    )
    assert person["Item"]["version"] == {"N": "1"}
    assert person["Item"]["visits"]["S"].startswith("[{")
    assert evaluation["Item"]["body"]["S"] == '{"checkin_event_id":"A","nights":2}'


def test_property_is_cached_per_store(aws):
    aws.put_properties(PROPS)
    store = make_store(aws)
    assert store.get_property("P1") == PROPS["P1"]
    aws.dynamodb.delete_item(TableName="properties", Key={"property_id": {"S": "P1"}})
    assert store.get_property("P1") == PROPS["P1"]  # served from the container cache


def test_other_cancellation_reasons_are_conflicts(aws):
    store = make_store(aws)
    with Stubber(store.client) as stub:
        stub.add_client_error(
            "transact_write_items",
            service_error_code="TransactionCanceledException",
            service_message="Transaction cancelled [TransactionConflict, None]",
            http_status_code=400,
            modeled_fields={
                "CancellationReasons": [{"Code": "TransactionConflict"}, {"Code": "None"}]
            },
        )
        with pytest.raises(ConflictError):
            store.commit("U1", 0, [VISIT], {"checkin_event_id": "A"})


def test_processor_retries_after_a_conflicting_commit(aws):
    aws.put_properties(PROPERTIES)
    store, racer = make_store(aws), make_store(aws)
    run(store, "CheckIn", checkin("A", "P1", "2026-03-01T10:00:00+13:00"))
    original, raced = store.commit, []

    def commit_after_a_race(*args):
        if not raced:  # another container commits between our read and our write
            raced.append(True)
            run(racer, "CheckOut", checkout("X", "A", "P1", "2026-03-01T11:00:00+13:00"))
        return original(*args)

    store.commit = commit_after_a_race
    out = run(store, "CheckIn", checkin("B", "P3", "2026-03-02T10:00:00+13:00"))
    evaluation = out[0].detail
    assert (evaluation["version_read"], evaluation["version_committed"]) == (2, 3)
    assert [o["end_source"] for o in evaluation["origins_seen"]] == ["checkout"]
    assert store.load_person("U1")[0] == 3
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_dynamo_store.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'standdown_checker.dynamo_store'`.

- [ ] **Step 3: Implement the store**

Create `lambdas/standdown_checker/dynamo_store.py`:

```python
"""StateStore on DynamoDB (spec §6.1): consistent reads and TransactWriteItems commits."""

from __future__ import annotations

import json

from .store import ConflictError, EvaluationExistsError, UnknownPropertyError


def _dumps(value: object) -> str:
    return json.dumps(value, separators=(",", ":"))


class DynamoDbStore:
    """Implements the StateStore contract in store.py with the low-level client API.

    `visits` and evaluation bodies are JSON strings (attribute type S), so reads return plain
    JSON types. Properties are cached for the life of the object (one per Lambda container).
    """

    def __init__(
        self, client, person_table: str, evaluation_table: str, property_table: str
    ) -> None:
        self.client = client
        self.person_table = person_table
        self.evaluation_table = evaluation_table
        self.property_table = property_table
        self._properties: dict[str, dict] = {}

    def _get(self, table: str, key: str, value: str) -> dict | None:
        response = self.client.get_item(
            TableName=table, Key={key: {"S": value}}, ConsistentRead=True
        )
        return response.get("Item")

    def load_person(self, person_id: str) -> tuple[int, list[dict]]:
        item = self._get(self.person_table, "person_id", person_id)
        if item is None:
            return 0, []
        return int(item["version"]["N"]), json.loads(item["visits"]["S"])

    def get_evaluation(self, checkin_event_id: str) -> dict | None:
        item = self._get(self.evaluation_table, "checkin_event_id", checkin_event_id)
        return None if item is None else json.loads(item["body"]["S"])

    def get_property(self, property_id: str) -> dict:
        cached = self._properties.get(property_id)
        if cached is None:
            item = self._get(self.property_table, "property_id", property_id)
            if item is None:
                raise UnknownPropertyError(f"unknown property {property_id!r}")
            cached = {
                "property_type": item["property_type"]["S"],
                "owner_company_id": item["owner_company_id"]["S"],
            }
            self._properties[property_id] = cached
        return dict(cached)

    def _person_update(self, person_id: str, version_read: int, visits: list[dict]) -> dict:
        values = {":new": {"N": str(version_read + 1)}, ":visits": {"S": _dumps(visits)}}
        if version_read == 0:
            condition = "attribute_not_exists(person_id)"
        else:
            condition = "version = :read"
            values[":read"] = {"N": str(version_read)}  # only when used: unused values fail
        return {
            "TableName": self.person_table,
            "Key": {"person_id": {"S": person_id}},
            "UpdateExpression": "SET version = :new, visits = :visits",
            "ConditionExpression": condition,
            "ExpressionAttributeValues": values,
        }

    def commit(
        self,
        person_id: str,
        version_read: int,
        visits: list[dict],
        evaluation: dict | None,
    ) -> None:
        update = self._person_update(person_id, version_read, visits)
        if evaluation is None:
            try:
                self.client.update_item(**update)
            except self.client.exceptions.ConditionalCheckFailedException:
                raise ConflictError(f"{person_id}: version {version_read} is stale") from None
            return
        put = {
            "TableName": self.evaluation_table,
            "Item": {
                "checkin_event_id": {"S": evaluation["checkin_event_id"]},
                "body": {"S": _dumps(evaluation)},
            },
            "ConditionExpression": "attribute_not_exists(checkin_event_id)",
        }
        try:
            self.client.transact_write_items(TransactItems=[{"Update": update}, {"Put": put}])
        except self.client.exceptions.TransactionCanceledException as exc:
            reasons = exc.response.get("CancellationReasons") or []
            codes = [reason.get("Code") for reason in reasons]
            if len(codes) > 1 and codes[1] == "ConditionalCheckFailed":
                raise EvaluationExistsError(evaluation["checkin_event_id"]) from None
            # A stale version or any other cancellation: reload and retry (spec §6.1).
            raise ConflictError(f"{person_id}: transaction cancelled {codes}") from None
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_dynamo_store.py tests/test_memory_store.py -q`
Expected: `40 passed`.

- [ ] **Step 5: Prove the new contract tests discriminate (then revert)**

Temporarily change `condition = "version = :read"` to
`condition = "attribute_exists(person_id) AND :read = :read"` in `dynamo_store.py` and run
`.venv/bin/pytest tests/test_dynamo_store.py -q`.
Expected: 4 failures, including `test_stale_nonzero_version_conflicts` and
`test_stale_nonzero_version_with_evaluation_leaves_everything_unchanged`.
Restore `condition = "version = :read"` exactly as in Step 3 and rerun: `22 passed`.

- [ ] **Step 6: Commit**

```bash
make fmt && make test
git add tests/aws_fixtures.py \
  tests/conftest.py \
  tests/test_dynamo_store.py \
  lambdas/standdown_checker/dynamo_store.py
git commit -m "$(cat <<'EOF'
DynamoDbStore: consistent reads, TransactWriteItems commits, contract on moto

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

---

### Task 3: Stand-down Lambda handler (spec §6.1, brief decision 4)

**Files:**
- Create: `lambdas/standdown_checker/handler.py`
- Test: `tests/test_standdown_handler.py`

**Interfaces:**
- Consumes: `process_event`, `Outbound.detail_json()` (canonical JSON), `RetryExhaustedError`
  (`standdown_checker.processor`); `load_rules`; `DynamoDbStore` (Task 2).
- Produces:
  - `handle(event, *, store, rules, events_client, bus_name, now=wall clock) -> {"batchItemFailures": [{"itemIdentifier": message_id}]}`.
  - Lambda entry `standdown_checker.handler.handler(event, context)`; env `PERSON_TABLE`,
    `EVALUATION_TABLE`, `PROPERTY_TABLE`, `EVENT_BUS_NAME`, optional `RULES_PATH`.
  - `RULES_FILE` = `<zip root>/standdown_rules.csv` (next to the package; `make package`, Task 9).

Behaviour:
- Each SQS record body is the EventBridge envelope; `detail-type` and `detail` go to
  `process_event`. Any exception (ValidationError, RetryExhaustedError, boto errors, bad JSON)
  fails that record only.
- Outbound events of all successful records are sent with PutEvents in calls of at most 10
  entries. A rejected entry (`FailedEntryCount > 0`, `ErrorCode` on the entry) fails the record
  that produced it; an exception from PutEvents fails every record in that call. The commit has
  already happened, so the redelivery takes the "already processed" path and re-emits
  byte-identical bodies.
- Each published `HighRiskVisitDetected` is logged as one JSON line (`"level": "alert"`), so
  alerts are visible in the Lambda logs during the demo (spec §9 step 2).
- Clients, the store (with its property cache) and the rules are built once per container.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_standdown_handler.py`:

```python
import json

import pytest
from standdown_checker import handler as H
from standdown_checker.dynamo_store import DynamoDbStore
from standdown_checker.store import ConflictError

from tests.helpers import PROPERTIES, ROOT, RULES, TickingClock, checkin, checkout


class FakeEvents:
    """Records PutEvents calls; rejects entries of the given detail types."""

    def __init__(self, reject: tuple[str, ...] = (), error: Exception | None = None) -> None:
        self.reject, self.error, self.calls = reject, error, []

    def put_events(self, Entries):
        self.calls.append(Entries)
        if self.error is not None:
            raise self.error
        results = [
            {"ErrorCode": "InternalFailure", "ErrorMessage": "rejected"}
            if entry["DetailType"] in self.reject
            else {"EventId": f"e{i}"}
            for i, entry in enumerate(Entries)
        ]
        return {"FailedEntryCount": sum("ErrorCode" in r for r in results), "Entries": results}

    def details(self, detail_type: str) -> list[str]:
        return [e["Detail"] for call in self.calls for e in call if e["DetailType"] == detail_type]


A_IN = checkin("A", "P1", "2026-03-03T09:00:00+13:00")


def record(message_id: str, detail_type: str, detail: dict) -> dict:
    body = {
        "version": "0",
        "id": f"env-{message_id}",
        "source": "onside.app",
        "detail-type": detail_type,
        "detail": detail,
    }
    return {"messageId": message_id, "body": json.dumps(body)}


def sqs_event(*records: dict) -> dict:
    return {"Records": list(records)}


@pytest.fixture
def store(aws):
    aws.put_properties(PROPERTIES)
    return DynamoDbStore(aws.dynamodb, "person_state", "evaluations", "properties")


def call(event, store, events, **kwargs):
    return H.handle(
        event,
        store=store,
        rules=RULES,
        events_client=events,
        bus_name="onside-events",
        now=TickingClock(),
        **kwargs,
    )


def failures(result) -> list[str]:
    return [f["itemIdentifier"] for f in result["batchItemFailures"]]


def test_checkin_is_committed_and_its_evaluation_published(store):
    events = FakeEvents()
    result = call(sqs_event(record("m1", "CheckIn", A_IN)), store, events)
    assert failures(result) == []
    (entry,) = events.calls[0]
    assert entry["Source"] == "onside.standdown"
    assert entry["DetailType"] == "CheckInEvaluated"
    assert entry["EventBusName"] == "onside-events"
    stored = store.get_evaluation("A")
    assert entry["Detail"] == json.dumps(stored, sort_keys=True, separators=(",", ":"))
    assert store.load_person("U1")[0] == 1


def test_invalid_records_fail_alone(store):
    events = FakeEvents()
    batch = sqs_event(
        record("good", "CheckIn", A_IN),
        record("poison", "CheckIn", {"poison": True, "event_id": "x"}),
        {"messageId": "not-json", "body": "{"},
        record("unknown-property", "CheckIn", checkin("B", "NOPE", "2026-03-03T10:00:00+13:00")),
    )
    result = call(batch, store, events)
    assert failures(result) == ["poison", "not-json", "unknown-property"]
    assert store.get_evaluation("A") is not None


def test_rejected_entry_fails_only_its_record_and_redelivery_reemits_identically(store):
    breach = sqs_event(
        record("m1", "CheckIn", A_IN),
        record("m2", "CheckIn", checkin("C", "P3", "2026-03-04T14:00:00+13:00")),
    )
    events = FakeEvents(reject=("HighRiskVisitDetected",))
    assert failures(call(breach, store, events)) == ["m2"]  # m2's alert was rejected
    retry = FakeEvents()
    assert failures(call(sqs_event(breach["Records"][1]), store, retry)) == []
    assert retry.details("CheckInEvaluated") == events.details("CheckInEvaluated")[1:]
    assert retry.details("HighRiskVisitDetected") == events.details("HighRiskVisitDetected")


def test_put_events_exception_fails_every_record_in_that_call(store):
    events = FakeEvents(error=RuntimeError("bus unavailable"))
    batch = sqs_event(
        record("m1", "CheckIn", A_IN),
        record("m2", "CheckOut", checkout("A-out", "A", "P1", "2026-03-03T10:00:00+13:00")),
    )
    result = call(batch, store, events)
    assert failures(result) == ["m1", "m2"]
    assert store.load_person("U1")[0] == 2  # committed; a redelivery re-emits


def test_outbound_entries_are_sent_in_calls_of_at_most_ten(store):
    records = [
        record(f"m{i}", "CheckIn", checkin(f"A{i}", "P1", A_IN["event_time"], person=f"U{i}"))
        for i in range(12)
    ]
    events = FakeEvents()
    assert failures(call(sqs_event(*records), store, events)) == []
    assert [len(c) for c in events.calls] == [10, 2]


def test_exhausted_retries_fail_the_record(store, monkeypatch):
    def always_conflict(*args):
        raise ConflictError("busy")

    monkeypatch.setattr(store, "commit", always_conflict)
    result = call(sqs_event(record("m1", "CheckIn", A_IN)), store, FakeEvents())
    assert failures(result) == ["m1"]


def test_lambda_entry_point_uses_environment(aws, monkeypatch):
    aws.put_properties(PROPERTIES)
    monkeypatch.setenv("PERSON_TABLE", "person_state")
    monkeypatch.setenv("EVALUATION_TABLE", "evaluations")
    monkeypatch.setenv("PROPERTY_TABLE", "properties")
    monkeypatch.setenv("EVENT_BUS_NAME", aws.bus_name)
    monkeypatch.setenv("RULES_PATH", str(ROOT / "shared" / "standdown_rules.csv"))
    monkeypatch.setattr(H, "_DEPENDENCIES", None)
    batch = sqs_event(
        record("m1", "CheckIn", A_IN),
        record("m2", "CheckIn", {"poison": True, "event_id": "x"}),
    )
    result = H.handler(batch, None)
    assert failures(result) == ["m2"]
    item = aws.dynamodb.get_item(TableName="evaluations", Key={"checkin_event_id": {"S": "A"}})
    assert "Item" in item
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_standdown_handler.py -q`
Expected: collection error, `ImportError: cannot import name 'handler' from 'standdown_checker'`.

- [ ] **Step 3: Implement the handler**

Create `lambdas/standdown_checker/handler.py`:

```python
"""Lambda entry point: SQS batch -> processor -> PutEvents (spec §6.1).

Every record that fails (invalid, retries exhausted, any error, or an outbound event that
PutEvents rejected) is reported in batchItemFailures, so SQS redelivers only that record.
A redelivery takes the processor's "already processed" path and re-emits identical bodies.
"""

from __future__ import annotations

import json
import os
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


def _send(events_client, bus_name: str, chunk: list[tuple[str, Outbound]]) -> set[str]:
    """PutEvents for up to 10 entries; returns the message ids whose entries failed."""
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
        _log(level="error", error=f"PutEvents: {type(exc).__name__}: {exc}")
        return {message_id for message_id, _ in chunk}
    if not response.get("FailedEntryCount"):
        return set()
    results = response.get("Entries", [])
    failed = {chunk[i][0] for i, result in enumerate(results) if result.get("ErrorCode")}
    _log(level="error", error=f"PutEvents FailedEntryCount={response['FailedEntryCount']}")
    return failed or {message_id for message_id, _ in chunk}


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
            _log(level="error", messageId=message_id, error=f"{type(exc).__name__}: {exc}")
            failed.append(message_id)
            continue
        pending.extend((message_id, out) for out in outbound)

    rejected: set[str] = set()
    for start in range(0, len(pending), PUT_EVENTS_MAX_ENTRIES):
        rejected |= _send(events_client, bus_name, pending[start : start + PUT_EVENTS_MAX_ENTRIES])
    for message_id, out in pending:
        if out.detail_type == "HighRiskVisitDetected" and message_id not in rejected:
            _log(level="alert", **out.detail)
    failed.extend(m for m in dict.fromkeys(m for m, _ in pending) if m in rejected)
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
    return _DEPENDENCIES


def handler(event: dict, context: object) -> dict:
    return handle(event, **_dependencies())
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_standdown_handler.py -q`
Expected: `7 passed`.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add lambdas/standdown_checker/handler.py \
  tests/test_standdown_handler.py
git commit -m "$(cat <<'EOF'
Stand-down Lambda handler: partial batch failures, PutEvents entries checked

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

---

### Task 4: Archiver Lambda (spec §6.5, brief decision 5)

**Files:**
- Create: `lambdas/archiver/__init__.py` (empty)
- Create: `lambdas/archiver/handler.py`
- Test: `tests/test_archiver.py`

**Interfaces:**
- Produces:
  - `object_key(now: datetime, object_id: str) -> str` → `raw/YYYY/MM/DD/HH/<id>.ndjson` (UTC hour).
  - `to_ndjson(bodies) -> bytes`: one compact JSON envelope per line.
  - `handle(event, *, s3, bucket, now=wall clock, new_id=uuid4) -> {"archived": n, "key": key | None}`.
  - Lambda entry `archiver.handler.handler(event, context)`; env `BUCKET`.
- Consumed by: Terraform (Task 10) with batch size 100 and a 5 s batching window; settle (Task 7)
  reads the objects.

Behaviour: one object per invocation; the key uses the archiver's own UTC wall clock, never the
envelope `time`; any failure (S3 error, a body that is not JSON) raises so the whole batch is
retried (at least once). Inside LocalStack the runtime injects `AWS_ENDPOINT_URL` (a container IP),
so the S3 client switches to path-style addressing then.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_archiver.py`:

```python
import json
from datetime import datetime

import pytest
from archiver import handler as A
from botocore.exceptions import ClientError

NZ_MORNING = datetime.fromisoformat("2026-03-04T09:30:00+13:00")  # 2026-03-03 20:30 UTC


def envelope(i: int) -> dict:
    return {
        "id": f"env-{i}",
        "source": "onside.app",
        "detail-type": "CheckIn",
        "time": "2025-01-01T00:00:00Z",  # never used for the key
        "detail": {"event_id": f"E{i}", "answers": [{"q": "a b"}]},
    }


def sqs_event(n: int) -> dict:
    # Bodies arrive pretty-printed to prove the archiver rewrites them as compact lines.
    records = [{"messageId": f"m{i}", "body": json.dumps(envelope(i), indent=2)} for i in range(n)]
    return {"Records": records}


def objects(aws) -> list[str]:
    return [o["Key"] for o in aws.s3.list_objects_v2(Bucket=aws.bucket).get("Contents", [])]


def test_one_object_per_batch_with_one_compact_line_per_envelope(aws):
    clock, new_id = (lambda: NZ_MORNING), (lambda: "k")
    result = A.handle(sqs_event(100), s3=aws.s3, bucket=aws.bucket, now=clock, new_id=new_id)
    assert result == {"archived": 100, "key": "raw/2026/03/03/20/k.ndjson"}
    assert objects(aws) == ["raw/2026/03/03/20/k.ndjson"]
    body = aws.s3.get_object(Bucket=aws.bucket, Key=result["key"])["Body"].read().decode()
    lines = body.splitlines()
    assert len(lines) == 100 and body.endswith("\n")
    assert lines[0] == json.dumps(envelope(0), separators=(",", ":"))
    assert [json.loads(line) for line in lines] == [envelope(i) for i in range(100)]


def test_each_invocation_writes_its_own_object(aws):
    A.handle(sqs_event(3), s3=aws.s3, bucket=aws.bucket)
    A.handle(sqs_event(3), s3=aws.s3, bucket=aws.bucket)
    keys = objects(aws)
    assert len(keys) == 2 and all(k.startswith("raw/") and k.endswith(".ndjson") for k in keys)


def test_failed_s3_write_fails_the_whole_batch(aws):
    with pytest.raises(ClientError):
        A.handle(sqs_event(5), s3=aws.s3, bucket="no-such-bucket")
    assert objects(aws) == []


def test_a_body_that_is_not_json_fails_the_whole_batch(aws):
    event = sqs_event(2)
    event["Records"].append({"messageId": "bad", "body": "{"})
    with pytest.raises(json.JSONDecodeError):
        A.handle(event, s3=aws.s3, bucket=aws.bucket)
    assert objects(aws) == []


def test_empty_batch_writes_nothing(aws):
    assert A.handle({"Records": []}, s3=aws.s3, bucket=aws.bucket)["archived"] == 0
    assert objects(aws) == []


def test_lambda_entry_point_uses_environment(aws, monkeypatch):
    monkeypatch.setenv("BUCKET", aws.bucket)
    monkeypatch.setattr(A, "_S3", None)
    assert A.handler(sqs_event(2), None)["archived"] == 2
    assert len(objects(aws)) == 1
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_archiver.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'archiver'`.

- [ ] **Step 3: Implement the archiver**

Create an empty `lambdas/archiver/__init__.py`, then:

Create `lambdas/archiver/handler.py`:

```python
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
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_archiver.py -q`
Expected: `6 passed`.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add lambdas/archiver/__init__.py \
  lambdas/archiver/handler.py \
  tests/test_archiver.py
git commit -m "$(cat <<'EOF'
Archiver Lambda: one NDJSON object per SQS batch under the arrival hour

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

---

### Task 5: Environments `local` and `aws`, AWS connection, manifests with a start time

**Files:**
- Modify: `generator/config.py` (ENVS, the `local` scale, per-environment default scale)
- Modify: `generator/state.py` (`reset_hint`, `started_at`, `read_manifest`, `check_remote_consistency`)
- Create: `generator/aws.py`
- Test: `tests/test_aws_env.py`

**Interfaces:**
- Consumes: `generator.config.state_dir`, `GeneratorError`.
- Produces:
  - `generator.config`: `ENVS = ("sim", "local", "aws")`; `SCALES["local"] = Scale("local", 200, 600, 50, 15, 10_000)`;
    `DEFAULT_SCALE = {"sim": "ci", "local": "local", "aws": "aws"}`.
  - `generator.state`: `reset_hint(env) -> str`; `write_manifest(env, run_id, mode, seed, counts, started_at=None) -> dict`
    (adds key `started_at`); `read_manifest(env, run_id) -> dict`;
    `check_remote_consistency(env, state, remote_ids: set[str]) -> None`.
  - `generator.aws`: `REGION`, `LOCALSTACK_ENDPOINT`, `client_kwargs(env) -> dict`,
    `client(env, service)`, `Outputs` (fields `region bucket person_table evaluation_table
    property_table bus_name visit_queue_url visit_dlq_url archive_queue_url archive_dlq_url
    standdown_function archiver_function`), `load_outputs(env) -> Outputs` (from
    `state/<env>/outputs.json`), `Connection(env, outputs, s3, dynamodb, events, sqs)` with
    `put_json(key, value)`, `keys(prefix) -> list[str]`, `remote_manifest_ids() -> set[str]`;
    `connect(env) -> Connection`.

`local` talks to `http://localhost:4566` (override: env `LOCALSTACK_ENDPOINT`) with `test`/`test`
credentials; `aws` uses only the region and the default credential chain. Tests never call
`client()` for real: they use the `aws` moto fixture or monkeypatch `generator.aws.client`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_aws_env.py`:

```python
import json

import pytest

from generator import aws as A
from generator import state as S
from generator.config import DEFAULT_SCALE, ENVS, SCALES


def test_environments_and_their_default_scales():
    assert ENVS == ("sim", "local", "aws")
    assert DEFAULT_SCALE == {"sim": "ci", "local": "local", "aws": "aws"}
    local = SCALES["local"]
    assert (local.n_properties, local.n_people, local.n_checkins) == (200, 600, 10_000)


def test_local_clients_go_to_localstack_with_test_credentials(monkeypatch):
    monkeypatch.delenv("LOCALSTACK_ENDPOINT", raising=False)
    kwargs = A.client_kwargs("local")
    assert kwargs["endpoint_url"] == "http://localhost:4566"
    assert kwargs["region_name"] == "ap-southeast-2"
    assert (kwargs["aws_access_key_id"], kwargs["aws_secret_access_key"]) == ("test", "test")
    monkeypatch.setenv("LOCALSTACK_ENDPOINT", "http://localhost:4599")
    assert A.client_kwargs("local")["endpoint_url"] == "http://localhost:4599"


def test_aws_clients_use_the_default_credential_chain():
    assert A.client_kwargs("aws") == {"region_name": "ap-southeast-2"}


def test_sim_makes_no_aws_calls():
    with pytest.raises(S.GeneratorError, match="no AWS calls"):
        A.client_kwargs("sim")


def test_outputs_are_read_from_terraform_output_json(aws, tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    with pytest.raises(S.GeneratorError, match="make deploy ENV=local"):
        A.load_outputs("local")
    (tmp_path / "state" / "local").mkdir(parents=True)
    (tmp_path / "state" / "local" / "outputs.json").write_text(json.dumps(aws.outputs()))
    outputs = A.load_outputs("local")
    assert outputs.bucket == aws.bucket
    assert outputs.archive_dlq_url == aws.queue_urls["archive_dlq"]


def test_manifest_records_its_start_and_can_be_read_back(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    started = "2026-10-10T01:00:00+00:00"
    S.write_manifest("local", "run-1", "backfill", 42, {"CheckIn": 2}, started_at=started)
    assert S.read_manifest("local", "run-1")["started_at"] == started
    with pytest.raises(S.GeneratorError, match="no manifest"):
        S.read_manifest("local", "run-2")


def test_local_inconsistency_says_make_up(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    S.write_manifest("local", "run-1", "backfill", 42, {"CheckIn": 1})
    with pytest.raises(S.GeneratorError, match="make up"):
        S.check_consistency("local", None)
    with pytest.raises(S.GeneratorError, match="make up"):
        S.check_remote_consistency("local", {"runs": ["run-1"]}, set())
    with pytest.raises(S.GeneratorError, match="make destroy ENV=aws"):
        S.check_remote_consistency("aws", None, {"run-9"})
    S.check_remote_consistency("local", {"runs": ["run-1"]}, {"run-1"})
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_aws_env.py -q`
Expected: collection error, `ImportError: cannot import name 'aws' from 'generator'`.

- [ ] **Step 3: Implement**

Replace `generator/config.py`:

```python
"""Scales and paths (spec §3.2). Every path hangs off REPO_ROOT so dbt and tests agree."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ENVS = ("sim", "local", "aws")


@dataclass(frozen=True)
class Scale:
    name: str
    n_properties: int
    n_people: int
    n_owner_companies: int
    n_visitor_companies: int
    n_checkins: int
    max_checkins_per_person: int = 300


SCALES = {
    "default": Scale("default", 500, 2000, 120, 40, 100_000),
    "aws": Scale("aws", 40, 100, 12, 6, 3_000),
    "ci": Scale("ci", 80, 200, 20, 8, 5_000),
    "local": Scale("local", 200, 600, 50, 15, 10_000),
}
# The scale `setup` uses when none is given (spec §3.2 "Scale per environment").
DEFAULT_SCALE = {"sim": "ci", "local": "local", "aws": "aws"}


def repo_root() -> Path:
    return Path(os.environ.get("REPO_ROOT", Path.cwd())).resolve()


def state_dir(env: str) -> Path:
    return repo_root() / "state" / env


def data_dir(env: str) -> Path:
    return repo_root() / "data" / env


def ref_dir(env: str) -> Path:
    return data_dir(env) / "ref"


def compacted_dir(env: str) -> Path:
    return data_dir(env) / "compacted"


def rules_path() -> Path:
    return repo_root() / "shared" / "standdown_rules.csv"


def sim_store_path() -> Path:
    return state_dir("sim") / "sim_store.json"


def warehouse_path(env: str) -> Path:
    return repo_root() / "warehouse" / f"{env}.duckdb"
```

Replace `generator/state.py`:

```python
"""Generator state and manifests under state/<env>/ (spec §3.2)."""

from __future__ import annotations

import json
from pathlib import Path

from .config import state_dir


class GeneratorError(RuntimeError):
    """A user-facing error whose message says how to fix it."""


def write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True))
    tmp.replace(path)


def read_setup(env: str) -> dict:
    path = state_dir(env) / "setup.json"
    if not path.exists():
        raise GeneratorError(f"no setup for ENV={env}; run `make setup ENV={env}` first")
    return json.loads(path.read_text())


def write_setup(env: str, seed: int, scale_name: str) -> None:
    write_json(state_dir(env) / "setup.json", {"seed": seed, "scale": scale_name})


def load_state(env: str) -> dict | None:
    path = state_dir(env) / "generator_state.json"
    return json.loads(path.read_text()) if path.exists() else None


def save_state(env: str, state: dict) -> None:
    write_json(state_dir(env) / "generator_state.json", state)


def reset_hint(env: str) -> str:
    """How to start an environment again (spec §3.2)."""
    if env == "local":
        return "run `make up` (LocalStack restarts empty), then deploy and setup again"
    if env == "aws":
        return "run `make destroy ENV=aws`, then deploy and setup again"
    return f"run `make reset ENV={env}` and start again"


def write_manifest(
    env: str,
    run_id: str,
    mode: str,
    seed: int,
    counts: dict[str, int],
    started_at: str | None = None,
) -> dict:
    manifest = {
        "run_id": run_id,
        "mode": mode,
        "seed": seed,
        "env": env,
        "counts": counts,
        "total": sum(counts.values()),
        "started_at": started_at,  # wall clock (UTC) before emitting; None for sim
    }
    write_json(state_dir(env) / "manifests" / f"{run_id}.json", manifest)
    return manifest


def read_manifest(env: str, run_id: str) -> dict:
    path = state_dir(env) / "manifests" / f"{run_id}.json"
    if not path.exists():
        raise GeneratorError(f"no manifest for run {run_id!r} in state/{env}/manifests")
    return json.loads(path.read_text())


def manifest_ids(env: str) -> set[str]:
    directory = state_dir(env) / "manifests"
    return {p.stem for p in directory.glob("*.json")} if directory.exists() else set()


def check_consistency(env: str, state: dict | None) -> None:
    runs = set(state["runs"]) if state else set()
    found = manifest_ids(env)
    if found != runs:
        raise GeneratorError(
            f"ENV={env} manifests {sorted(found)} do not match generator state {sorted(runs)}; "
            f"{reset_hint(env)}"
        )


def check_remote_consistency(env: str, state: dict | None, remote_ids: set[str]) -> None:
    """S3 control/manifests/ must match state/<env>/ (spec §3.2), e.g. after a lost LocalStack."""
    runs = set(state["runs"]) if state else set()
    if remote_ids != runs:
        raise GeneratorError(
            f"ENV={env}: S3 control/manifests {sorted(remote_ids)} do not match "
            f"state/{env} runs {sorted(runs)}; {reset_hint(env)}"
        )
```

Create `generator/aws.py`:

```python
"""AWS access per environment (spec §3.2): LocalStack for local, the credential chain for aws."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, fields

import boto3
from botocore.config import Config

from .config import state_dir
from .state import GeneratorError

REGION = "ap-southeast-2"
LOCALSTACK_ENDPOINT = "http://localhost:4566"


def client_kwargs(env: str) -> dict:
    if env == "local":
        return {
            "region_name": REGION,
            "endpoint_url": os.environ.get("LOCALSTACK_ENDPOINT", LOCALSTACK_ENDPOINT),
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
        }
    if env == "aws":
        return {"region_name": REGION}  # default credential chain
    raise GeneratorError(f"ENV={env} makes no AWS calls; use ENV=local or ENV=aws")


def client(env: str, service: str):
    config = Config(
        retries={"max_attempts": 10, "mode": "standard"},
        s3={"addressing_style": "path"} if env == "local" else None,
    )
    return boto3.client(service, config=config, **client_kwargs(env))


@dataclass(frozen=True)
class Outputs:
    """`terraform output -json`, saved by `make deploy` to state/<env>/outputs.json."""

    region: str
    bucket: str
    person_table: str
    evaluation_table: str
    property_table: str
    bus_name: str
    visit_queue_url: str
    visit_dlq_url: str
    archive_queue_url: str
    archive_dlq_url: str
    standdown_function: str
    archiver_function: str


def load_outputs(env: str) -> Outputs:
    path = state_dir(env) / "outputs.json"
    if not path.exists():
        raise GeneratorError(f"no {path}; run `make deploy ENV={env}` first")
    raw = json.loads(path.read_text())
    try:
        return Outputs(**{f.name: raw[f.name]["value"] for f in fields(Outputs)})
    except KeyError as exc:
        raise GeneratorError(f"{path} lacks output {exc}; run `make deploy ENV={env}`") from None


@dataclass(frozen=True)
class Connection:
    env: str
    outputs: Outputs
    s3: object
    dynamodb: object
    events: object
    sqs: object

    def put_json(self, key: str, value: object) -> None:
        body = json.dumps(value, indent=1, sort_keys=True).encode()
        self.s3.put_object(Bucket=self.outputs.bucket, Key=key, Body=body)

    def keys(self, prefix: str) -> list[str]:
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.outputs.bucket, Prefix=prefix)
        return [obj["Key"] for page in pages for obj in page.get("Contents", [])]

    def remote_manifest_ids(self) -> set[str]:
        prefix = "control/manifests/"
        return {k[len(prefix) :].removesuffix(".json") for k in self.keys(prefix)}


def connect(env: str) -> Connection:
    outputs = load_outputs(env)
    return Connection(
        env,
        outputs,
        client(env, "s3"),
        client(env, "dynamodb"),
        client(env, "events"),
        client(env, "sqs"),
    )
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_aws_env.py tests/test_state.py -q`
Expected: `10 passed`.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add generator/config.py \
  generator/state.py \
  generator/aws.py \
  tests/test_aws_env.py
git commit -m "$(cat <<'EOF'
Generator: local and aws environments, Terraform outputs, manifest start time

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

---

### Task 6: Event emitter (spec §3.1, §3.2)

**Files:**
- Create: `generator/emit.py`
- Test: `tests/test_emit.py`

**Interfaces:**
- Consumes: `generator.stream.Emission(send_time, detail_type, detail, source="onside.app")`.
- Produces: `MAX_ENTRIES = 10`, `MAX_ATTEMPTS = 6`, `to_entry(emission, bus_name) -> dict`,
  `put_batch(events_client, entries, *, sleep, attempts) -> None` (raises `GeneratorError`),
  `emit(emissions, events_client, bus_name, *, sleep, log, every=2000) -> int` (prints
  `emitted <done>/<total> events (<rate> events/s)` every 2,000 events and at the end).

Only rejected entries are resent (backoff 0.2 s doubling, capped at 5 s); throttling and network
errors are retried by botocore (`standard` mode, 10 attempts, set in `generator.aws.client`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_emit.py`:

```python
import json
from datetime import UTC, datetime

import pytest

from generator.emit import MAX_ATTEMPTS, emit, put_batch, to_entry
from generator.state import GeneratorError
from generator.stream import Emission

T = datetime(2026, 3, 1, tzinfo=UTC)


def emissions(n: int) -> list[Emission]:
    return [Emission(T, "CheckIn", {"event_id": f"E{i:02d}", "n": i}) for i in range(n)]


class FlakyEvents:
    """Rejects each listed event id for the given number of attempts."""

    def __init__(self, reject: dict[str, int] | None = None) -> None:
        self.reject = dict(reject or {})
        self.calls: list[list[str]] = []

    def put_events(self, Entries):
        ids = [json.loads(e["Detail"])["event_id"] for e in Entries]
        self.calls.append(ids)
        results = []
        for event_id in ids:
            if self.reject.get(event_id, 0) > 0:
                self.reject[event_id] -= 1
                results.append({"ErrorCode": "ThrottlingException", "ErrorMessage": "slow down"})
            else:
                results.append({"EventId": event_id})
        failed = sum("ErrorCode" in r for r in results)
        return {"FailedEntryCount": failed, "Entries": results}


def test_entry_carries_source_type_bus_and_compact_detail():
    entry = to_entry(Emission(T, "CheckOut", {"event_id": "E1", "a": [1]}), "bus")
    assert entry == {
        "Source": "onside.app",
        "DetailType": "CheckOut",
        "Detail": '{"event_id":"E1","a":[1]}',
        "EventBusName": "bus",
    }


def test_batches_of_ten_in_send_order():
    client = FlakyEvents()
    assert emit(emissions(25), client, "bus", log=lambda line: None) == 25
    assert [len(c) for c in client.calls] == [10, 10, 5]
    assert [i for c in client.calls for i in c] == [f"E{i:02d}" for i in range(25)]


def test_only_rejected_entries_are_resent():
    client, sleeps = FlakyEvents({"E03": 1, "E07": 2}), []
    emit(emissions(10), client, "bus", sleep=sleeps.append, log=lambda line: None)
    assert client.calls == [[f"E{i:02d}" for i in range(10)], ["E03", "E07"], ["E07"]]
    assert sleeps == [0.2, 0.4]


def test_entries_rejected_on_every_attempt_raise():
    client = FlakyEvents({"E01": 99})
    with pytest.raises(GeneratorError, match="ThrottlingException"):
        put_batch(client, [to_entry(e, "bus") for e in emissions(3)], sleep=lambda s: None)
    assert len(client.calls) == MAX_ATTEMPTS


def test_emits_to_an_event_bus(aws):
    lines = []
    assert emit(emissions(12), aws.events, aws.bus_name, log=lines.append) == 12
    assert lines[-1].startswith("emitted 12/12 events")
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_emit.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'generator.emit'`.

- [ ] **Step 3: Implement the emitter**

Create `generator/emit.py`:

```python
"""PutEvents in send order, in batches of 10, retrying only the failed entries (spec §3.2)."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Sequence

from .state import GeneratorError
from .stream import Emission

MAX_ENTRIES = 10
MAX_ATTEMPTS = 6


def to_entry(emission: Emission, bus_name: str) -> dict:
    return {
        "Source": emission.source,
        "DetailType": emission.detail_type,
        "Detail": json.dumps(emission.detail, separators=(",", ":")),
        "EventBusName": bus_name,
    }


def put_batch(
    events_client,
    entries: list[dict],
    *,
    sleep: Callable[[float], None] = time.sleep,
    attempts: int = MAX_ATTEMPTS,
) -> None:
    """Send up to 10 entries; resend only the rejected ones, a bounded number of times."""
    remaining = entries
    errors: list[str] = []
    for attempt in range(attempts):
        response = events_client.put_events(Entries=remaining)
        if not response.get("FailedEntryCount"):
            return
        results = response["Entries"]
        errors = sorted({r["ErrorCode"] for r in results if r.get("ErrorCode")})
        remaining = [e for e, r in zip(remaining, results, strict=True) if r.get("ErrorCode")]
        if attempt + 1 < attempts:
            sleep(min(5.0, 0.2 * 2**attempt))
    raise GeneratorError(
        f"PutEvents still rejected {len(remaining)} entries after {attempts} attempts: {errors}"
    )


def emit(
    emissions: Sequence[Emission],
    events_client,
    bus_name: str,
    *,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = print,
    every: int = 2000,
) -> int:
    """Send every emission in order. Returns how many entries were sent."""
    entries = [to_entry(em, bus_name) for em in emissions]
    started = time.monotonic()
    for start in range(0, len(entries), MAX_ENTRIES):
        put_batch(events_client, entries[start : start + MAX_ENTRIES], sleep=sleep)
        done = min(start + MAX_ENTRIES, len(entries))
        if done % every < MAX_ENTRIES or done == len(entries):
            rate = done / max(time.monotonic() - started, 1e-6)
            log(f"emitted {done}/{len(entries)} events ({rate:.0f} events/s)")
    return len(entries)
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_emit.py -q`
Expected: `5 passed`.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add generator/emit.py \
  tests/test_emit.py
git commit -m "$(cat <<'EOF'
Generator: PutEvents in send order, batches of 10, bounded retry of rejected entries

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

---

### Task 7: Settle and compact (spec §3.4, §6.5)

**Files:**
- Create: `generator/queues.py` (queue attributes only; never receives messages)
- Create: `generator/settle.py`
- Test: `tests/test_settle.py` (moto S3, a stub SQS that returns queue attributes)

**Interfaces:**
- Consumes: `generator.aws.Connection` and `Outputs` (Task 5); `generator.stream.event_id(run_id, seq)`;
  a manifest dict with `run_id`, `mode`, `counts`, `total`, `started_at`.
- Produces:
  - `generator.queues`: `counts(sqs, url) -> dict[str, int]`, `visible(sqs, url) -> int`,
    `is_idle(sqs, url) -> bool`, `wait_for_dlq(sqs, dlq_url, expected, *, timeout, poll=5, sleep, clock, log) -> int`.
  - `generator.settle`: `QUIET_SECONDS = 30`, `POLL_SECONDS = 5`, `TIMEOUT_SECONDS = 3600`,
    `PART_LINES = 50_000`, `SettleTimeout(GeneratorError)`, `iter_json(text)`,
    `hour_prefixes(start, now) -> list[str]`, `Tally(manifest)`, and
    `Settler(manifest, conn, quiet_seconds, poll_seconds, timeout_seconds, sleep, clock, now, log).run() -> dict`
    returning the settled record `{run_id, env, mode, started_at, settled_at, counts, breaches, envelopes, parts}`.
  - Local files: `state/<env>/spool/<run_id>.ndjson` and `.keys` (removed after success),
    `state/<env>/settled/<run_id>.json`. S3: `compacted/<env>/<run_id>/part-NNNN.ndjson`,
    `control/settled/<run_id>.json`.

Behaviour (spec §3.4): list `raw/` hour prefixes from one hour before `started_at` up to now; skip
cached keys; append each new object's envelopes to the spool **before** caching its key; count only
this run's ids; settled when every manifest event is present, one `CheckInEvaluated` per
check-in, one `CheckOutApplied` per check-out, one alert per breach, and both the visit and archive
queues show 0 visible, 0 in flight and 0 delayed — then one quiet period and a rescan. The archive
DLQ is checked on every pass and any message there fails fast (archived data would be missing).
Compaction drops envelopes whose EventBridge `id` was archived twice. The spool holds every
envelope found in the window, including other runs' (harmless: staging deduplicates).
A crash can tear the last spool line; resume truncates it (its key was never cached).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_settle.py`:

```python
import json
from datetime import datetime

import pytest

from generator.aws import Connection, Outputs
from generator.settle import Settler, SettleTimeout, hour_prefixes, iter_json
from generator.state import GeneratorError
from generator.stream import event_id

RUN = "7d0e8c1e-0000-4000-8000-000000000001"
CI1, CO1, CI2 = (event_id(RUN, seq) for seq in range(3))
MANIFEST = {
    "run_id": RUN,
    "mode": "live",
    "counts": {"CheckIn": 2, "CheckOut": 1, "PlantMovement": 0},
    "total": 3,
    "started_at": "2026-10-10T01:30:00+00:00",
}
NOW = datetime.fromisoformat("2026-10-10T02:10:00+00:00")


def env(envelope_id: str, detail_type: str, detail: dict, source="onside.standdown") -> dict:
    return {"id": envelope_id, "source": source, "detail-type": detail_type, "detail": detail}


APP = [
    env("g1", "CheckIn", {"event_id": CI1}, "onside.app"),
    env("g2", "CheckOut", {"event_id": CO1, "checkin_event_id": CI1}, "onside.app"),
    env("g3", "CheckIn", {"event_id": CI2}, "onside.app"),
    env("g1", "CheckIn", {"event_id": CI1}, "onside.app"),  # the same delivery archived twice
]
DERIVED = [
    env("d1", "CheckInEvaluated", {"checkin_event_id": CI1, "breach": False}),
    env("d2", "CheckInEvaluated", {"checkin_event_id": CI2, "breach": True}),
    env("d3", "CheckOutApplied", {"checkout_event_id": CO1}),
    env("d4", "HighRiskVisitDetected", {"checkin_event_id": CI2}),
    env("x1", "CheckInEvaluated", {"checkin_event_id": "earlier-run", "breach": True}),
]


class FakeTime:
    def __init__(self) -> None:
        self.t, self.sleeps, self.on_sleep = 0.0, [], []

    def clock(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds
        if self.on_sleep:
            self.on_sleep.pop(0)()


class StubSqs:
    """Queue attributes only: depth per queue URL (visible messages)."""

    def __init__(self) -> None:
        self.depth: dict[str, int] = {}

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        visible = self.depth.get(QueueUrl, 0)
        return {
            "Attributes": {
                "ApproximateNumberOfMessages": str(visible),
                "ApproximateNumberOfMessagesNotVisible": "0",
                "ApproximateNumberOfMessagesDelayed": "0",
            }
        }


@pytest.fixture
def conn(aws, tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    outputs = Outputs(**{k: v["value"] for k, v in aws.outputs().items()})
    return Connection("local", outputs, aws.s3, aws.dynamodb, aws.events, StubSqs())


def put_raw(conn, key: str, envelopes: list[dict], *, concatenated: bool = False) -> None:
    sep = "" if concatenated else "\n"
    body = sep.join(json.dumps(e) for e in envelopes)
    conn.s3.put_object(Bucket=conn.outputs.bucket, Key=key, Body=body.encode())


def settler(conn, fake: FakeTime, **kwargs) -> Settler:
    options = dict(quiet_seconds=30, poll_seconds=5, timeout_seconds=600)
    options.update(kwargs)
    options.update(sleep=fake.sleep, clock=fake.clock, now=lambda: NOW, log=lambda line: None)
    return Settler(MANIFEST, conn, **options)


def compacted_ids(conn, key: str) -> list[str]:
    body = conn.s3.get_object(Bucket=conn.outputs.bucket, Key=key)["Body"].read().decode()
    return [json.loads(line)["id"] for line in body.splitlines()]


def test_iter_json_reads_ndjson_and_concatenated_documents():
    assert list(iter_json('{"a":1}{"b":2}\n\n{"c":3}\n')) == [{"a": 1}, {"b": 2}, {"c": 3}]


def test_hour_prefixes_start_one_hour_before_the_run():
    start = datetime.fromisoformat("2026-10-10T14:30:00+13:00")  # 01:30 UTC
    assert hour_prefixes(start, NOW) == [
        "raw/2026/10/10/00/",
        "raw/2026/10/10/01/",
        "raw/2026/10/10/02/",
    ]


def test_complete_run_is_compacted_after_one_quiet_period(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP)
    put_raw(conn, "raw/2026/10/10/00/b.ndjson", DERIVED, concatenated=True)  # skew hour
    put_raw(conn, "raw/2026/10/09/23/old.ndjson", [env("old", "CheckIn", {"event_id": "z"})])
    fake = FakeTime()
    settled = settler(conn, fake).run()
    part = f"compacted/local/{RUN}/part-0000.ndjson"
    assert settled["parts"] == [part]
    ids = compacted_ids(conn, part)  # the duplicate g1 is dropped, the old hour never read
    assert sorted(ids) == ["d1", "d2", "d3", "d4", "g1", "g2", "g3", "x1"]
    assert settled["breaches"] == 1 and settled["envelopes"] == 8
    control = conn.s3.get_object(Bucket=conn.outputs.bucket, Key=f"control/settled/{RUN}.json")
    assert json.loads(control["Body"].read())["parts"] == [part]
    assert fake.sleeps == [30]
    spool = settler(conn, fake).spool
    assert not spool.exists() and (spool.parents[1] / "settled" / f"{RUN}.json").exists()


def test_waits_for_missing_derived_events(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP)
    fake = FakeTime()
    fake.on_sleep.append(lambda: put_raw(conn, "raw/2026/10/10/02/b.ndjson", DERIVED))
    settler(conn, fake).run()
    assert fake.sleeps == [5, 30]


def test_alert_gate_waits_for_every_breach(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP + DERIVED[:3])  # no alert for CI2 yet
    fake = FakeTime()
    fake.on_sleep.append(lambda: put_raw(conn, "raw/2026/10/10/02/b.ndjson", DERIVED[3:]))
    settler(conn, fake).run()
    assert fake.sleeps == [5, 30]


def test_busy_queue_delays_settling(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP + DERIVED)
    conn.sqs.depth[conn.outputs.visit_queue_url] = 1
    fake = FakeTime()
    fake.on_sleep.append(lambda: conn.sqs.depth.clear())
    settler(conn, fake).run()
    assert fake.sleeps == [5, 30]


def test_timeout_keeps_the_spool_and_a_resume_reads_only_new_objects(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP)
    fake = FakeTime()
    with pytest.raises(SettleTimeout, match=f"make settle ENV=local RUN={RUN}"):
        settler(conn, fake, timeout_seconds=10).run()
    assert settler(conn, fake).spool.exists()

    put_raw(conn, "raw/2026/10/10/02/b.ndjson", DERIVED)
    read, original = [], conn.s3.get_object

    def spy(**kwargs):
        read.append(kwargs["Key"])
        return original(**kwargs)

    conn.s3.get_object = spy
    settled = settler(conn, FakeTime()).run()
    assert read == ["raw/2026/10/10/02/b.ndjson"]
    assert settled["envelopes"] == 8


def test_a_torn_spool_line_is_dropped_on_resume(conn):
    spool = settler(conn, FakeTime()).spool
    spool.parent.mkdir(parents=True)
    spool.write_text(json.dumps(APP[0]) + "\n" + '{"id": "torn')
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP + DERIVED)
    assert settler(conn, FakeTime()).run()["envelopes"] == 8


def test_archive_dlq_messages_fail_fast(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP + DERIVED)
    conn.sqs.depth[conn.outputs.archive_dlq_url] = 2
    with pytest.raises(GeneratorError, match="archive DLQ holds 2 messages.*[Rr]edrive"):
        settler(conn, FakeTime()).run()
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_settle.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'generator.settle'`.

- [ ] **Step 3: Implement**

Create `generator/queues.py`:

```python
"""SQS queue depth from attributes only; messages are never received (spec §6.4)."""

from __future__ import annotations

import time
from collections.abc import Callable

from .state import GeneratorError

DEPTH_ATTRIBUTES = (
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible",
    "ApproximateNumberOfMessagesDelayed",
)


def counts(sqs, queue_url: str) -> dict[str, int]:
    response = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=list(DEPTH_ATTRIBUTES))
    attributes = response.get("Attributes", {})
    return {name: int(attributes.get(name, 0)) for name in DEPTH_ATTRIBUTES}


def visible(sqs, queue_url: str) -> int:
    return counts(sqs, queue_url)["ApproximateNumberOfMessages"]


def is_idle(sqs, queue_url: str) -> bool:
    """No visible, in-flight or delayed messages."""
    return not any(counts(sqs, queue_url).values())


def wait_for_dlq(
    sqs,
    dlq_url: str,
    expected: int,
    *,
    timeout: float,
    poll: float = 5.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    log: Callable[[str], None] = print,
) -> int:
    """Wait until the DLQ shows at least `expected` messages (a redelivery may add more)."""
    deadline = clock() + timeout
    while True:
        found = visible(sqs, dlq_url)
        if found >= expected:
            log(f"visit DLQ holds {found} messages (expected at least {expected})")
            return found
        if clock() >= deadline:
            raise GeneratorError(
                f"visit DLQ holds {found} messages, expected at least {expected} "
                f"after {timeout:.0f}s (a poison message takes about 3 minutes on LocalStack)"
            )
        sleep(poll)
```

Create `generator/settle.py`:

```python
"""Settle and compact a run (spec §3.4).

Scan S3 raw/ incrementally, spool every envelope to state/<env>/spool/, count this run's
events, and once every gate holds (and a quiet period confirms it) write the spool to
compacted/<env>/<run_id>/ and control/settled/<run_id>.json.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import queues as Q
from .aws import Connection
from .config import state_dir
from .state import GeneratorError, write_json
from .stream import event_id

QUIET_SECONDS = 30
POLL_SECONDS = 5
TIMEOUT_SECONDS = 3600
LOG_EVERY_SECONDS = 30
PART_LINES = 50_000
SKEW = timedelta(hours=1)
APP_TYPES = ("CheckIn", "CheckOut", "PlantMovement")


class SettleTimeout(GeneratorError):
    """The run did not settle in time; the spool is kept for `make settle`."""


def iter_json(text: str) -> Iterator[dict]:
    """JSON documents from NDJSON or from documents concatenated without separators."""
    decoder = json.JSONDecoder()
    i, n = 0, len(text)
    while True:
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            return
        document, i = decoder.raw_decode(text, i)
        yield document


def hour_prefixes(start: datetime, now: datetime) -> list[str]:
    """Arrival-hour prefixes from one hour before the run's start up to now (UTC)."""
    hour = (start - SKEW).astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    prefixes = []
    while hour <= now:
        prefixes.append(f"raw/{hour:%Y/%m/%d/%H}/")
        hour += timedelta(hours=1)
    return prefixes


class Tally:
    """Counts only this run's event ids: uuid5(run_id, seq) for seq below the manifest total."""

    def __init__(self, manifest: dict) -> None:
        self.counts = {t: manifest["counts"].get(t, 0) for t in APP_TYPES}
        self.ids = {event_id(manifest["run_id"], seq) for seq in range(manifest["total"])}
        self.seen: dict[str, set[str]] = {t: set() for t in APP_TYPES}
        self.evaluated: set[str] = set()
        self.breaches: set[str] = set()
        self.applied: set[str] = set()
        self.alerted: set[str] = set()

    def add(self, envelope: dict) -> None:
        detail = envelope.get("detail")
        if not isinstance(detail, dict):
            return
        kind = envelope.get("detail-type")
        if kind in self.seen:
            if detail.get("event_id") in self.ids:
                self.seen[kind].add(detail["event_id"])
        elif kind == "CheckInEvaluated":
            if detail.get("checkin_event_id") in self.ids:
                self.evaluated.add(detail["checkin_event_id"])
                if detail.get("breach") is True:
                    self.breaches.add(detail["checkin_event_id"])
        elif kind == "CheckOutApplied":
            if detail.get("checkout_event_id") in self.ids:
                self.applied.add(detail["checkout_event_id"])
        elif kind == "HighRiskVisitDetected":
            if detail.get("checkin_event_id") in self.ids:
                self.alerted.add(detail["checkin_event_id"])

    def missing(self) -> list[str]:
        """The gates that do not hold yet; empty when the run is complete."""
        gaps = [
            f"{t} {len(self.seen[t])}/{self.counts[t]}"
            for t in APP_TYPES
            if len(self.seen[t]) != self.counts[t]
        ]
        if len(self.evaluated) != self.counts["CheckIn"]:
            gaps.append(f"CheckInEvaluated {len(self.evaluated)}/{self.counts['CheckIn']}")
        if len(self.applied) != self.counts["CheckOut"]:
            gaps.append(f"CheckOutApplied {len(self.applied)}/{self.counts['CheckOut']}")
        if self.alerted != self.breaches:
            gaps.append(f"HighRiskVisitDetected {len(self.alerted)}/{len(self.breaches)}")
        return gaps


@dataclass
class Settler:
    manifest: dict
    conn: Connection
    quiet_seconds: float = QUIET_SECONDS
    poll_seconds: float = POLL_SECONDS
    timeout_seconds: float = TIMEOUT_SECONDS
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    log: Callable[[str], None] = print

    @property
    def run_id(self) -> str:
        return self.manifest["run_id"]

    @property
    def env(self) -> str:
        return self.conn.env

    @property
    def spool(self) -> Path:
        return state_dir(self.env) / "spool" / f"{self.run_id}.ndjson"

    @property
    def key_cache(self) -> Path:
        return state_dir(self.env) / "spool" / f"{self.run_id}.keys"

    def run(self) -> dict:
        tally, keys = Tally(self.manifest), self._resume()
        for line in self._spool_lines():
            tally.add(json.loads(line))
        start = datetime.fromisoformat(self.manifest["started_at"])
        deadline = self.clock() + self.timeout_seconds
        next_log = self.clock()
        while True:
            self._check_archive_dlq()
            self._scan(start, keys, tally)
            if not tally.missing() and self._queues_idle():
                self.sleep(self.quiet_seconds)  # one more quiet period, then rescan
                self._scan(start, keys, tally)
                if not tally.missing() and self._queues_idle():
                    return self._compact(tally)
            waiting = ", ".join(tally.missing()) or "the queues to drain"
            if self.clock() >= deadline:
                raise SettleTimeout(
                    f"run {self.run_id} did not settle in {self.timeout_seconds:.0f}s (waiting "
                    f"for {waiting}); the spool is kept: resume with "
                    f"`make settle ENV={self.env} RUN={self.run_id}`"
                )
            if self.clock() >= next_log:
                self.log(f"settle {self.run_id}: waiting for {waiting}")
                next_log = self.clock() + LOG_EVERY_SECONDS
            self.sleep(self.poll_seconds)

    def _resume(self) -> set[str]:
        self.spool.parent.mkdir(parents=True, exist_ok=True)
        if self.spool.exists():  # drop a line torn by a crash; its key was never cached
            data = self.spool.read_bytes()
            cut = data.rfind(b"\n") + 1
            if cut != len(data):
                with open(self.spool, "r+b") as fh:
                    fh.truncate(cut)
        if not self.key_cache.exists():
            return set()
        return set(self.key_cache.read_text().split())

    def _spool_lines(self) -> Iterator[str]:
        if self.spool.exists():
            with open(self.spool) as fh:
                yield from (line for line in fh if line.strip())

    def _check_archive_dlq(self) -> None:
        stuck = Q.visible(self.conn.sqs, self.conn.outputs.archive_dlq_url)
        if stuck:
            raise GeneratorError(
                f"the archive DLQ holds {stuck} messages, so archived events are missing from "
                f"S3 raw/. Redrive them to the archive queue (SQS console 'Start DLQ redrive' "
                f"or `aws sqs start-message-move-task`), then run "
                f"`make settle ENV={self.env} RUN={self.run_id}`"
            )

    def _queues_idle(self) -> bool:
        outputs = self.conn.outputs
        return all(
            Q.is_idle(self.conn.sqs, url)
            for url in (outputs.visit_queue_url, outputs.archive_queue_url)
        )

    def _scan(self, start: datetime, keys: set[str], tally: Tally) -> int:
        new = 0
        for prefix in hour_prefixes(start, self.now()):
            for key in self.conn.keys(prefix):
                if key in keys:
                    continue
                obj = self.conn.s3.get_object(Bucket=self.conn.outputs.bucket, Key=key)
                envelopes = list(iter_json(obj["Body"].read().decode()))
                lines = "".join(json.dumps(e, separators=(",", ":")) + "\n" for e in envelopes)
                with open(self.spool, "a") as fh:  # spool before the key cache (spec §3.4)
                    fh.write(lines)
                with open(self.key_cache, "a") as fh:
                    fh.write(key + "\n")
                keys.add(key)
                for envelope in envelopes:
                    tally.add(envelope)
                new += 1
        return new

    def _compact(self, tally: Tally) -> dict:
        prefix = f"compacted/{self.env}/{self.run_id}/"
        parts: list[str] = []
        buffer: list[str] = []
        envelope_ids: set[str] = set()

        def flush() -> None:
            key = f"{prefix}part-{len(parts):04d}.ndjson"
            body = "".join(buffer).encode()
            self.conn.s3.put_object(Bucket=self.conn.outputs.bucket, Key=key, Body=body)
            parts.append(key)
            buffer.clear()

        written = 0
        for line in self._spool_lines():
            envelope_id = json.loads(line).get("id")
            if envelope_id is not None:
                if envelope_id in envelope_ids:  # the same delivery archived twice
                    continue
                envelope_ids.add(envelope_id)
            buffer.append(line)
            written += 1
            if len(buffer) >= PART_LINES:
                flush()
        if buffer:
            flush()
        settled = {
            "run_id": self.run_id,
            "env": self.env,
            "mode": self.manifest["mode"],
            "started_at": self.manifest["started_at"],
            "settled_at": self.now().isoformat(timespec="seconds"),
            "counts": self.manifest["counts"],
            "breaches": len(tally.breaches),
            "envelopes": written,
            "parts": parts,
        }
        self.conn.put_json(f"control/settled/{self.run_id}.json", settled)
        write_json(state_dir(self.env) / "settled" / f"{self.run_id}.json", settled)
        self.spool.unlink()
        self.key_cache.unlink(missing_ok=True)
        return settled
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_settle.py -q`
Expected: `9 passed`.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add generator/queues.py \
  generator/settle.py \
  tests/test_settle.py
git commit -m "$(cat <<'EOF'
Settle and compact: incremental raw scan, spool and key cache, gates, archive DLQ check

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

---

### Task 8: Generator CLI for `local`/`aws` and the Makefile data targets (spec §3.2, §8)

**Files:**
- Modify: `generator/cli.py` (full replacement; `sim` behaviour unchanged)
- Modify: `Makefile` (full replacement of the Plan 1 file; infrastructure targets come in Tasks 9–12)
- Test: `tests/test_cli_aws.py` (moto; `generator.aws.client` is monkeypatched)

**Interfaces:**
- Consumes: Tasks 5–7 (`generator.aws`, `generator.emit.emit`, `generator.settle.Settler`,
  `generator.queues.wait_for_dlq`), Plan 1 planning (`plan_backfill`, `plan_live`, `build_emissions`).
- Produces (CLI, `python -m generator …`):
  - `setup --env E [--scale S] [--seed 42]`: default scale per env; for local/aws also uploads
    `ref/{properties,people,companies}.json` and batch-writes DynamoDB `properties`; refuses
    when `data/<env>/ref/properties.json` or any S3 `ref/` object exists (message: `reset_hint`).
  - `generate --env E --mode backfill|live [--n] [--anchor] [--now] [--no-settle] [--timeout 3600]`:
    for local/aws, checks S3 `control/manifests/` against `state/<env>/`, writes the manifest
    (with `started_at`) locally and to S3 **before** emitting, emits, saves the generator state,
    then settles (`settle_run`).
  - `settle --env E --run RUN [--timeout 3600]`; `poison --env E [--k 3] [--timeout 300]`
    (writes `state/<env>/poison/<run>.json` and S3 `control/poison/<run>.json`, never
    `control/manifests/`; waits until the **visit** DLQ holds at least the sum of all poison K).
  - `cli.settle_run(conn, manifest, timeout) -> dict` and module attribute `cli.emit` (patched in tests).
  - Make: `setup`, `generate`, `settle RUN=`, `poison K=`, all with `ENV=`, optional `SCALE=`,
    `TIMEOUT=`; `GEN_PY` runs Python unbuffered so progress lines appear live.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_cli_aws.py`:

```python
import json
import shutil
from pathlib import Path

import boto3
import pytest

from generator import aws as A
from generator import cli
from generator.cli import main
from tests.aws_fixtures import REGION

ROOT = Path(__file__).resolve().parents[1]
ANCHOR = "2026-10-01T00:00:00+00:00"


@pytest.fixture
def repo(aws, tmp_path, monkeypatch):
    """A deployed `local` environment backed by moto; nothing reaches LocalStack or AWS."""
    shutil.copytree(ROOT / "shared", tmp_path / "shared")
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setattr(A, "client", lambda env, service: boto3.client(service, region_name=REGION))
    (tmp_path / "state" / "local").mkdir(parents=True)
    (tmp_path / "state" / "local" / "outputs.json").write_text(json.dumps(aws.outputs()))
    return tmp_path


def s3_keys(aws, prefix: str) -> list[str]:
    listing = aws.s3.list_objects_v2(Bucket=aws.bucket, Prefix=prefix)
    return sorted(o["Key"] for o in listing.get("Contents", []))


def s3_json(aws, key: str):
    return json.loads(aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read())


def test_setup_loads_the_same_snapshot_into_s3_and_dynamodb(aws, repo):
    assert main(["setup", "--env", "local", "--scale", "ci"]) == 0
    assert s3_keys(aws, "ref/") == ["ref/companies.json", "ref/people.json", "ref/properties.json"]
    items = aws.dynamodb.scan(TableName="properties")["Items"]
    local = json.loads((repo / "data" / "local" / "ref" / "properties.json").read_text())
    assert len(items) == len(local) == 80
    assert s3_json(aws, "ref/properties.json") == local
    setup = json.loads((repo / "state" / "local" / "setup.json").read_text())
    assert setup == {"scale": "ci", "seed": 42}


def test_setup_defaults_to_the_local_scale(aws, repo):
    assert main(["setup", "--env", "local"]) == 0
    assert json.loads((repo / "state" / "local" / "setup.json").read_text())["scale"] == "local"


def test_second_setup_is_refused_with_make_up(aws, repo, capsys):
    main(["setup", "--env", "local", "--scale", "aws"])
    shutil.rmtree(repo / "data" / "local")  # the S3 copy alone is enough to refuse
    assert main(["setup", "--env", "local", "--scale", "aws"]) == 2
    assert "make up" in capsys.readouterr().err


def test_setup_before_deploy_is_refused(aws, repo, capsys):
    (repo / "state" / "local" / "outputs.json").unlink()
    assert main(["setup", "--env", "local"]) == 2
    assert "make deploy ENV=local" in capsys.readouterr().err


def test_generate_writes_the_manifest_to_s3_before_emitting(aws, repo, monkeypatch):
    main(["setup", "--env", "local", "--scale", "aws"])
    seen = {}

    def fake_emit(emissions, events_client, bus_name):
        seen["manifests"] = s3_keys(aws, "control/manifests/")
        seen["types"] = {e.detail_type for e in emissions}
        return len(emissions)

    monkeypatch.setattr(cli, "emit", fake_emit)
    args = ["generate", "--env", "local", "--mode", "backfill", "--anchor", ANCHOR, "--no-settle"]
    assert main(args) == 0
    run_id = json.loads((repo / "state" / "local" / "generator_state.json").read_text())["runs"][0]
    assert seen["manifests"] == [f"control/manifests/{run_id}.json"]
    assert seen["types"] == {"CheckIn", "CheckOut", "PlantMovement"}
    manifest = s3_json(aws, f"control/manifests/{run_id}.json")
    assert manifest["started_at"] and manifest["total"] == sum(manifest["counts"].values())


def test_generate_emits_and_then_settles(aws, repo, monkeypatch):
    main(["setup", "--env", "local", "--scale", "aws"])
    settled = []
    monkeypatch.setattr(cli, "settle_run", lambda conn, manifest, timeout: settled.append(manifest))
    assert main(["generate", "--env", "local", "--mode", "backfill", "--anchor", ANCHOR]) == 0
    assert [m["mode"] for m in settled] == ["backfill"]


def test_lost_localstack_data_is_detected(aws, repo, capsys):
    main(["setup", "--env", "local", "--scale", "aws"])
    main(["generate", "--env", "local", "--mode", "backfill", "--anchor", ANCHOR, "--no-settle"])
    for key in s3_keys(aws, "control/manifests/"):
        aws.s3.delete_object(Bucket=aws.bucket, Key=key)
    assert main(["generate", "--env", "local", "--mode", "live", "--no-settle"]) == 2
    assert "make up" in capsys.readouterr().err


def test_poison_records_its_own_manifest_and_waits_for_the_visit_dlq(aws, repo):
    for _ in range(2):  # stands in for LocalStack moving the poison messages to the DLQ
        aws.sqs.send_message(QueueUrl=aws.queue_urls["visit_dlq"], MessageBody="{}")
    assert main(["poison", "--env", "local", "--k", "2", "--timeout", "0"]) == 0
    (key,) = s3_keys(aws, "control/poison/")
    assert s3_json(aws, key)["k"] == 2
    assert s3_keys(aws, "control/manifests/") == []
    assert len(list((repo / "state" / "local" / "poison").glob("*.json"))) == 1


def test_poison_fails_when_the_dlq_stays_short(aws, repo, capsys):
    assert main(["poison", "--env", "local", "--k", "1", "--timeout", "0"]) == 2
    assert "visit DLQ holds 0 messages" in capsys.readouterr().err


def test_settle_needs_a_known_run(aws, repo, capsys):
    assert main(["settle", "--env", "local", "--run", "nope"]) == 2
    assert "no manifest" in capsys.readouterr().err
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_cli_aws.py -q`
Expected: `10 failed` (the old CLI writes nothing to S3, defaults to the `ci` scale, and has
no `--no-settle` flag and no `settle`/`poison` commands: assertion errors and `SystemExit: 2`
from argparse).

- [ ] **Step 3: Replace the CLI**

Replace `generator/cli.py`:

```python
"""Command line for the generator: setup, generate, settle, poison, reset (spec §3.2, §8)."""

from __future__ import annotations

import argparse
import json
import random
import shutil
import sys
import uuid
from datetime import UTC, datetime

from standdown_checker.rules import load_rules

from . import aws as A
from . import queues as Q
from . import state as S
from .config import (
    DEFAULT_SCALE,
    ENVS,
    SCALES,
    compacted_dir,
    data_dir,
    ref_dir,
    rules_path,
    sim_store_path,
    state_dir,
    warehouse_path,
)
from .emit import emit
from .local_sim import InMemoryStore, run_sim
from .reference import Reference, build_reference
from .settle import TIMEOUT_SECONDS, Settler
from .stream import (
    BACKFILL_LATENESS,
    LIVE_LATENESS,
    Emission,
    build_emissions,
    event_id,
    plan_backfill,
    plan_live,
    update_cursors,
)

RUN_NAMESPACE = uuid.UUID("0b6c3f0e-58f4-4d0c-9d8a-1d6f1f0a7c21")
POISON_TIMEOUT_SECONDS = 300
DYNAMODB_BATCH = 25


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _load_properties(conn: A.Connection, ref: Reference) -> None:
    """Batch-write the snapshot to DynamoDB `properties`, resending unprocessed items."""
    requests = [
        {
            "PutRequest": {
                "Item": {
                    "property_id": {"S": p.property_id},
                    "property_type": {"S": p.property_type},
                    "owner_company_id": {"S": p.owner_company_id},
                }
            }
        }
        for p in ref.properties
    ]
    table = conn.outputs.property_table
    for start in range(0, len(requests), DYNAMODB_BATCH):
        pending = {table: requests[start : start + DYNAMODB_BATCH]}
        for _ in range(10):
            pending = conn.dynamodb.batch_write_item(RequestItems=pending).get("UnprocessedItems")
            if not pending:
                break
        else:
            raise S.GeneratorError("DynamoDB kept returning unprocessed property items")


def cmd_setup(args: argparse.Namespace) -> int:
    scale_name = args.scale or DEFAULT_SCALE[args.env]
    conn = A.connect(args.env) if args.env != "sim" else None
    if (ref_dir(args.env) / "properties.json").exists() or (conn and conn.keys("ref/")):
        raise S.GeneratorError(
            f"reference data already exists for ENV={args.env}; {S.reset_hint(args.env)}"
        )
    ref = build_reference(SCALES[scale_name], random.Random(args.seed))
    ref.write(ref_dir(args.env))
    if conn is not None:  # the same snapshot goes to S3 ref/ and DynamoDB (spec §3.6)
        for name in ("properties", "people", "companies"):
            body = (ref_dir(args.env) / f"{name}.json").read_bytes()
            conn.s3.put_object(Bucket=conn.outputs.bucket, Key=f"ref/{name}.json", Body=body)
        _load_properties(conn, ref)
    S.write_setup(args.env, args.seed, scale_name)
    print(f"setup ENV={args.env}: {len(ref.properties)} properties, {len(ref.people)} people")
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    setup = S.read_setup(args.env)
    state = S.load_state(args.env)
    S.check_consistency(args.env, state)
    conn = None
    if args.env != "sim":
        conn = A.connect(args.env)
        S.check_remote_consistency(args.env, state, conn.remote_manifest_ids())
    ref = Reference.read(ref_dir(args.env))
    scale = SCALES[setup["scale"]]
    seed = setup["seed"]

    if args.mode == "backfill":
        if state is not None:
            raise S.GeneratorError(
                f"ENV={args.env} already has generator state; a backfill runs once. "
                f"To start again, {S.reset_hint(args.env)}."
            )
        anchor = _ts(args.anchor) or _now()
        run_id = str(uuid.uuid5(RUN_NAMESPACE, f"{seed}:backfill:{anchor.isoformat()}"))
        rng = random.Random(f"{seed}:backfill")
        visits, plants = plan_backfill(ref, scale, rng, anchor)
        run_end, lateness, shortfall = anchor, BACKFILL_LATENESS, 0
        state = {
            "env": args.env,
            "seed": seed,
            "scale": setup["scale"],
            "anchor": anchor.isoformat(),
            "runs": [],
            "cursors": {},
        }
    else:
        if state is None:
            raise S.GeneratorError(
                f"no backfill yet for ENV={args.env}; "
                f"run `make generate ENV={args.env} MODE=backfill` first"
            )
        now = _ts(args.now) or _now()
        run_id = str(uuid.uuid4())
        rng = random.Random(f"{seed}:{run_id}")
        visits, shortfall = plan_live(
            ref, state["cursors"], rng, now, args.n, scale.max_checkins_per_person
        )
        plants, run_end, lateness = [], now, LIVE_LATENESS

    emissions, counts = build_emissions(visits, plants, ref, run_id, rng, run_end, lateness)
    if conn is None:
        S.write_manifest(args.env, run_id, args.mode, seed, counts)  # before "emitting" (§3.2)
        properties = {
            p.property_id: {
                "property_type": p.property_type,
                "owner_company_id": p.owner_company_id,
            }
            for p in ref.properties
        }
        store = InMemoryStore.load(sim_store_path(), properties)
        out_dir = compacted_dir(args.env) / run_id
        result = run_sim(emissions, store, load_rules(rules_path()), out_dir)
        store.save(sim_store_path())
    else:
        manifest = S.write_manifest(
            args.env, run_id, args.mode, seed, counts, started_at=_now().isoformat()
        )
        conn.put_json(f"control/manifests/{run_id}.json", manifest)  # before emitting (§3.2)
        result = {"sent": emit(emissions, conn.events, conn.outputs.bus_name)}
    update_cursors(state["cursors"], visits)
    state["runs"].append(run_id)
    S.save_state(args.env, state)
    anchor_note = f" anchor={state['anchor']}" if args.mode == "backfill" else ""
    print(f"{args.mode} run {run_id}{anchor_note}: {counts} -> {result}")
    if shortfall:
        print(f"note: {shortfall} requested check-ins did not fit (no one had room)")
    if conn is not None and not args.no_settle:
        settle_run(conn, S.read_manifest(args.env, run_id), args.timeout)
    return 0


def settle_run(conn: A.Connection, manifest: dict, timeout: float) -> dict:
    started = datetime.now(UTC)
    settled = Settler(manifest, conn, timeout_seconds=timeout).run()
    seconds = (datetime.now(UTC) - started).total_seconds()
    print(
        f"settled run {manifest['run_id']} in {seconds:.0f}s: {settled['envelopes']} envelopes, "
        f"{settled['breaches']} breaches -> {', '.join(settled['parts'])}"
    )
    return settled


def cmd_settle(args: argparse.Namespace) -> int:
    if args.env == "sim":
        raise S.GeneratorError("ENV=sim needs no settling; its runs are written complete")
    manifest = S.read_manifest(args.env, args.run)
    settle_run(A.connect(args.env), manifest, args.timeout)
    return 0


def cmd_poison(args: argparse.Namespace) -> int:
    if args.env == "sim":
        raise S.GeneratorError("poison needs ENV=local or ENV=aws (it demonstrates the DLQ)")
    conn = A.connect(args.env)
    run_id = str(uuid.uuid4())
    sent_at = _now()
    record = {"run_id": run_id, "k": args.k, "sent_at": sent_at.isoformat()}
    S.write_json(state_dir(args.env) / "poison" / f"{run_id}.json", record)
    conn.put_json(f"control/poison/{run_id}.json", record)  # never control/manifests/
    poison = [
        Emission(sent_at, "CheckIn", {"poison": True, "event_id": event_id(run_id, seq)})
        for seq in range(args.k)
    ]
    emit(poison, conn.events, conn.outputs.bus_name)
    expected = sum(
        json.loads(p.read_text())["k"] for p in (state_dir(args.env) / "poison").glob("*.json")
    )
    Q.wait_for_dlq(conn.sqs, conn.outputs.visit_dlq_url, expected, timeout=args.timeout)
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    shutil.rmtree(state_dir(args.env), ignore_errors=True)
    shutil.rmtree(data_dir(args.env), ignore_errors=True)
    warehouse_path(args.env).unlink(missing_ok=True)
    print(f"reset ENV={args.env}")
    return 0


COMMANDS = {
    "setup": cmd_setup,
    "generate": cmd_generate,
    "settle": cmd_settle,
    "poison": cmd_poison,
    "reset": cmd_reset,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="generator")
    sub = parser.add_subparsers(dest="command", required=True)
    p_setup = sub.add_parser("setup")
    p_setup.add_argument("--env", choices=ENVS, required=True)
    p_setup.add_argument("--scale", choices=sorted(SCALES), help="default: per environment")
    p_setup.add_argument("--seed", type=int, default=42)
    p_gen = sub.add_parser("generate")
    p_gen.add_argument("--env", choices=ENVS, required=True)
    p_gen.add_argument("--mode", choices=("backfill", "live"), required=True)
    p_gen.add_argument("--n", type=int, default=50)
    p_gen.add_argument("--anchor", help="backfill end time (ISO 8601); default now")
    p_gen.add_argument("--now", help="live run time (ISO 8601); default now")
    p_gen.add_argument("--no-settle", action="store_true", help="local/aws: emit only")
    p_gen.add_argument("--timeout", type=float, default=TIMEOUT_SECONDS)
    p_settle = sub.add_parser("settle")
    p_settle.add_argument("--env", choices=ENVS, required=True)
    p_settle.add_argument("--run", required=True)
    p_settle.add_argument("--timeout", type=float, default=TIMEOUT_SECONDS)
    p_poison = sub.add_parser("poison")
    p_poison.add_argument("--env", choices=ENVS, required=True)
    p_poison.add_argument("--k", type=int, default=3)
    p_poison.add_argument("--timeout", type=float, default=POISON_TIMEOUT_SECONDS)
    p_reset = sub.add_parser("reset")
    p_reset.add_argument("--env", choices=ENVS, required=True)
    args = parser.parse_args(argv)
    try:
        return COMMANDS[args.command](args)
    except S.GeneratorError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
```

- [ ] **Step 4: Replace the Makefile**

Recipe lines start with a TAB character.

Replace `Makefile`:

```make
SHELL := /bin/bash
REPO_ROOT := $(CURDIR)
VENV := $(REPO_ROOT)/.venv
PY := $(VENV)/bin/python
DBT := $(VENV)/bin/dbt
export REPO_ROOT

# Only a command-line ENV overrides this (a shell variable named ENV never does).
ENV = sim
# Empty SCALE means the per-environment default (sim: ci, local: local, aws: aws).
SCALE ?=
MODE ?= backfill
N ?= 50
K ?= 3
RUN ?=
# Optional settle/poison timeout in seconds; passed only when set.
TIMEOUT ?=
# Optional pins for reproducible sim data (ISO 8601); passed only when set.
ANCHOR ?=
NOW ?=
# standdown_checker lives under lambdas/; the editable install does not map it.
# Unbuffered, so emit and settle progress shows up while a long run is going.
GEN_PY := PYTHONUNBUFFERED=1 PYTHONPATH=$(REPO_ROOT)/lambdas $(PY)

.PHONY: venv fmt lint unit test setup generate settle poison reset dbt-build sim-data \
	sim-e2e trace

venv:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -e '.[dev]'

fmt:
	$(VENV)/bin/ruff format .
	$(VENV)/bin/ruff check --fix .

lint:
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .

unit:
	$(VENV)/bin/pytest -q

test: lint unit

# ---- data (ENV=sim|local|aws) ----

setup:
	$(GEN_PY) -m generator setup --env $(ENV) $(if $(SCALE),--scale $(SCALE))

generate:
	$(GEN_PY) -m generator generate --env $(ENV) --mode $(MODE) --n $(N) \
		$(if $(ANCHOR),--anchor $(ANCHOR)) $(if $(NOW),--now $(NOW)) \
		$(if $(TIMEOUT),--timeout $(TIMEOUT))

settle:
	$(GEN_PY) -m generator settle --env $(ENV) --run $(RUN) $(if $(TIMEOUT),--timeout $(TIMEOUT))

poison:
	$(GEN_PY) -m generator poison --env $(ENV) --k $(K) $(if $(TIMEOUT),--timeout $(TIMEOUT))

reset:
	$(GEN_PY) -m generator reset --env $(ENV)

DBT_ENV = DBT_PROFILES_DIR=$(REPO_ROOT)/dbt \
	DBT_COMPACTED_GLOB='$(REPO_ROOT)/data/$(ENV)/compacted/*/*.ndjson' \
	DBT_REF_ROOT='$(REPO_ROOT)/data/$(ENV)/ref/'

dbt-build:
	mkdir -p $(REPO_ROOT)/warehouse
	cd dbt && $(DBT_ENV) $(DBT) build --target $(ENV)

sim-data:
	$(MAKE) reset ENV=sim
	$(MAKE) setup ENV=sim SCALE=ci
	$(MAKE) generate ENV=sim MODE=backfill ANCHOR=$(ANCHOR)
	$(MAKE) generate ENV=sim MODE=live N=30 NOW=$(NOW)

sim-e2e: sim-data
	$(MAKE) dbt-build ENV=sim

HOPS ?= 3

trace:
	$(PY) -m tracing --db $(REPO_ROOT)/warehouse/$(ENV).duckdb --property $(PROPERTY) --start $(START) --max-hops $(HOPS)
```

- [ ] **Step 5: Run the tests (the sim end-to-end tests guard the unchanged `sim` path)**

Run: `.venv/bin/pytest tests/test_cli_aws.py tests/test_sim_e2e.py -q`
Expected: `16 passed` (about 70 s; the sim tests generate data).

Run: `make -n setup ENV=local && make -n poison ENV=local K=2`
Expected: prints `... -m generator setup --env local` (no `--scale`) and
`... -m generator poison --env local --k 2`.

- [ ] **Step 6: Commit**

```bash
make fmt && make test
git add generator/cli.py \
  Makefile \
  tests/test_cli_aws.py
git commit -m "$(cat <<'EOF'
Generator CLI: setup, generate, settle and poison for LocalStack and AWS

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

---

### Task 9: LocalStack compose, `make up`/`down`, Lambda packaging (brief decisions 6, 8)

**Files:**
- Modify: `docker-compose.yml` (full replacement)
- Modify: `.env.example` (the image tag is pinned in compose now)
- Modify: `Makefile` (append the LocalStack block)

**Interfaces:**
- Produces:
  - `make up`: compose down -v, up -d with a health wait, pre-pull `public.ecr.aws/lambda/python:3.12`,
    then delete `state/local/`, `data/local/`, `warehouse/local.duckdb` and
    `infra/terraform/state/local.tfstate*`. `make down`: compose down -v.
  - `make package`: `build/standdown_checker/` = `standdown_checker/` + `standdown_rules.csv` +
    `tzdata/` (pure Python); `build/archiver/` = `archiver/`. Consumed by Terraform `archive_file`
    (Task 10). `build/` is already gitignored.

- [ ] **Step 1: Make sure nothing else listens on port 4566**

Run: `docker ps --format '{{.Names}} {{.Ports}}' | grep 4566 || echo free`
Expected: `free`. If another LocalStack runs (for example a spike), stop it first
(`docker compose -p <its project> down -v`).

- [ ] **Step 2: Write the compose file and `.env.example`**

Replace `docker-compose.yml`:

```yaml
# LocalStack for ENV=local (spec §8). `make up` always starts it empty (no persistence).
services:
  localstack:
    image: localstack/localstack:4.14.0
    ports:
      - "4566:4566"
    ulimits:
      nofile: {soft: 65536, hard: 65536}
    environment:
      - SERVICES=s3,sqs,events,dynamodb,lambda,iam,sts,logs
      - LOCALSTACK_AUTH_TOKEN=${LOCALSTACK_AUTH_TOKEN:-}
      - PERSISTENCE=0
      # The SQS poller polls once a second by default, which caps a batch-10 Lambda at
      # about 10 events/s; 0.05 s measured about 25 events/s in the Plan 2 spike.
      - LAMBDA_EVENT_SOURCE_MAPPING_POLL_INTERVAL_SEC=0.05
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    healthcheck:
      test: ["CMD", "curl", "-fs", "http://localhost:4566/_localstack/health"]
      interval: 3s
      timeout: 3s
      retries: 40
```

Replace `.env.example`:

```bash
# Copy to .env if needed. The community image 4.14.0 needs no token (Day-1 spike).
LOCALSTACK_AUTH_TOKEN=
```

- [ ] **Step 3: Append the LocalStack targets to the Makefile**

Append to the end of `Makefile`:

```make

# ---- LocalStack (ENV=local) ----

LAMBDA_IMAGE := public.ecr.aws/lambda/python:3.12

.PHONY: up down package

# A full restart: LocalStack keeps nothing, so local state and Terraform state go too.
up:
	docker compose down -v --remove-orphans
	docker compose up -d --wait
	docker pull -q $(LAMBDA_IMAGE)
	rm -rf $(REPO_ROOT)/state/local $(REPO_ROOT)/data/local $(REPO_ROOT)/warehouse/local.duckdb
	rm -f $(REPO_ROOT)/infra/terraform/state/local.tfstate $(REPO_ROOT)/infra/terraform/state/local.tfstate.backup

down:
	docker compose down -v --remove-orphans

# Lambda bundles: the package plus, for the checker, the rules CSV next to it and the
# pure-Python tzdata. boto3 comes from the Lambda runtime.
package:
	rm -rf $(REPO_ROOT)/build/standdown_checker $(REPO_ROOT)/build/archiver
	mkdir -p $(REPO_ROOT)/build/standdown_checker $(REPO_ROOT)/build/archiver
	cp -r $(REPO_ROOT)/lambdas/standdown_checker $(REPO_ROOT)/build/standdown_checker/
	cp $(REPO_ROOT)/shared/standdown_rules.csv $(REPO_ROOT)/build/standdown_checker/
	$(VENV)/bin/pip install --quiet --no-deps --target $(REPO_ROOT)/build/standdown_checker 'tzdata>=2026.1'
	cp -r $(REPO_ROOT)/lambdas/archiver $(REPO_ROOT)/build/archiver/
	find $(REPO_ROOT)/build -name __pycache__ -type d -prune -exec rm -rf {} +
```

- [ ] **Step 4: Start LocalStack**

Run: `time make up && curl -s localhost:4566/_localstack/health | python3 -m json.tool | grep -E '"(dynamodb|lambda|s3|sqs|events)"'`
Expected: `Container ...-localstack-1  Healthy`, about 10–15 s (the first run also pulls the
images), and the five services `"available"` (or `"running"`).

Run: `docker exec $(docker compose ps -q localstack) sh -c 'ulimit -n; echo $LAMBDA_EVENT_SOURCE_MAPPING_POLL_INTERVAL_SEC'`
Expected: `65536` and `0.05`.

- [ ] **Step 5: Build the Lambda bundles and check them with a bare interpreter**

Run:
```bash
make package
cd build/standdown_checker && /usr/bin/python3 -I -c "
import sys; sys.path.insert(0, '.')
import importlib.util, standdown_checker.handler as h
from standdown_checker.rules import load_rules
print(h.RULES_FILE.name, len(load_rules(h.RULES_FILE)), importlib.util.find_spec('tzdata').origin)
" && cd ../.. && ls build/archiver/archiver
```
Expected: `standdown_rules.csv 11 <repo>/build/standdown_checker/tzdata/__init__.py`, then
`__init__.py  handler.py`. (`-I` ignores the venv and user site: the bundle needs only the
standard library, its own tzdata, and boto3 from the Lambda runtime, which is imported lazily.)

- [ ] **Step 6: Commit**

```bash
make fmt && make test
git add docker-compose.yml \
  .env.example \
  Makefile
git commit -m "$(cat <<'EOF'
LocalStack 4.14.0 compose with ulimit and fast SQS polling; make up, package

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

---

### Task 10: Terraform for LocalStack and AWS, `make deploy`/`destroy`/`tf-check` (brief decision 7, spec §8)

**Files:**
- Create: `infra/terraform/versions.tf`, `variables.tf`, `provider.tf`, `storage.tf`, `events.tf`,
  `lambdas.tf`, `outputs.tf`
- Create (generated by `terraform init`, commit it): `infra/terraform/.terraform.lock.hcl`
- Modify: `Makefile` (append the infrastructure block)
- Modify: `.github/workflows/ci.yml` (append a `terraform` job: fmt + validate)

**Interfaces:**
- Consumes: `build/standdown_checker/`, `build/archiver/` (Task 9); handlers
  `standdown_checker.handler.handler`, `archiver.handler.handler` and their env vars (Tasks 3–4).
- Produces: `state/<env>/outputs.json` (= `terraform output -json`, read by `generator.aws.load_outputs`)
  with the twelve outputs listed in `generator.aws.Outputs`; resource names `onside-events`,
  `onside-visits` (rule A), `onside-archive` (rule B), `onside-visit-queue`, `onside-visit-dlq`,
  `onside-archive-queue`, `onside-archive-dlq`, `onside-standdown-checker`, `onside-archiver`,
  tables `person_state`, `evaluations`, `properties`, bucket `onside-<env>-<account id>`
  (`onside-local-000000000000` on LocalStack).

**One codebase.** `variable "localstack_endpoint"` (empty by default = real AWS). When it is set
(the Makefile passes `-var localstack_endpoint=http://localhost:4566` only for `ENV=local`), a
`dynamic "endpoints"` block points every service at LocalStack and the provider uses test
credentials, skips the credential/account checks and uses path-style S3. With `ENV=aws` none of
that applies and the default credential chain is used. **Never run `ENV=aws` in this plan.**

**State per environment.** `backend "local" {}` plus `terraform init -reconfigure
-backend-config=path=infra/terraform/state/<env>.tfstate` on every deploy/destroy (the `-state`
flag is deprecated in Terraform 1.15). `infra/terraform/state/` and `.terraform/` are gitignored.

**Guard.** `deploy` and `destroy` refuse unless `ENV=local` or `ENV=aws` is given on the command
line (`$(origin ENV)`), so a shell variable or the `sim` default can never reach Terraform.

- [ ] **Step 1: Write the Terraform files**

Create `infra/terraform/versions.tf`:

```hcl
terraform {
  required_version = ">= 1.6"

  # The state file per environment comes from `make deploy` (-backend-config=path=...).
  backend "local" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}
```

Create `infra/terraform/variables.tf`:

```hcl
variable "env" {
  description = "local (LocalStack) or aws"
  type        = string

  validation {
    condition     = contains(["local", "aws"], var.env)
    error_message = "env must be local or aws."
  }
}

variable "localstack_endpoint" {
  description = "LocalStack URL for env=local; empty means real AWS with the default credential chain."
  type        = string
  default     = ""
}

variable "region" {
  type    = string
  default = "ap-southeast-2"
}
```

Create `infra/terraform/provider.tf`:

```hcl
# One codebase for LocalStack and AWS: a non-empty localstack_endpoint switches every
# service endpoint, test credentials and path-style S3 on (Plan 2 spike provider block).
locals {
  localstack = var.localstack_endpoint != ""
}

provider "aws" {
  region                      = var.region
  access_key                  = local.localstack ? "test" : null
  secret_key                  = local.localstack ? "test" : null
  skip_credentials_validation = local.localstack
  skip_metadata_api_check     = local.localstack
  skip_requesting_account_id  = local.localstack
  s3_use_path_style           = local.localstack

  dynamic "endpoints" {
    for_each = local.localstack ? [var.localstack_endpoint] : []
    content {
      dynamodb = endpoints.value
      events   = endpoints.value
      iam      = endpoints.value
      lambda   = endpoints.value
      logs     = endpoints.value
      s3       = endpoints.value
      sqs      = endpoints.value
      sts      = endpoints.value
    }
  }

  default_tags {
    tags = {
      project = "onside-movement-demo"
      env     = var.env
    }
  }
}
```

Create `infra/terraform/storage.tf`:

```hcl
data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "data" {
  # The account id keeps the name globally unique on AWS (LocalStack: 000000000000).
  bucket        = "onside-${var.env}-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}

resource "aws_dynamodb_table" "person_state" {
  name         = "person_state"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "person_id"

  attribute {
    name = "person_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "evaluations" {
  name         = "evaluations"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "checkin_event_id"

  attribute {
    name = "checkin_event_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "properties" {
  name         = "properties"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "property_id"

  attribute {
    name = "property_id"
    type = "S"
  }
}
```

Create `infra/terraform/events.tf`:

```hcl
locals {
  dlq_retention_seconds = 14 * 24 * 3600
}

resource "aws_cloudwatch_event_bus" "events" {
  name = "onside-events"
}

# Rule A: app check-ins and check-outs to the stand-down queue. No poison filter: poison
# events must reach the queue to demo the DLQ. The source filter stops Lambda output loops.
resource "aws_cloudwatch_event_rule" "visits" {
  name           = "onside-visits"
  event_bus_name = aws_cloudwatch_event_bus.events.name
  event_pattern = jsonencode({
    source        = ["onside.app"]
    "detail-type" = ["CheckIn", "CheckOut"]
  })
}

# Rule B: everything from both sources except poison, to the archive queue (no transformer).
resource "aws_cloudwatch_event_rule" "archive" {
  name           = "onside-archive"
  event_bus_name = aws_cloudwatch_event_bus.events.name
  event_pattern = jsonencode({
    source = ["onside.app", "onside.standdown"]
    detail = { poison = [{ exists = false }] }
  })
}

resource "aws_sqs_queue" "visit_dlq" {
  name                      = "onside-visit-dlq"
  message_retention_seconds = local.dlq_retention_seconds
}

resource "aws_sqs_queue" "visit" {
  name                       = "onside-visit-queue"
  visibility_timeout_seconds = 30
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.visit_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue" "archive_dlq" {
  name                      = "onside-archive-dlq"
  message_retention_seconds = local.dlq_retention_seconds
}

resource "aws_sqs_queue" "archive" {
  name                       = "onside-archive-queue"
  visibility_timeout_seconds = 30
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.archive_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_cloudwatch_event_target" "visits" {
  rule           = aws_cloudwatch_event_rule.visits.name
  event_bus_name = aws_cloudwatch_event_bus.events.name
  target_id      = "visit-queue"
  arn            = aws_sqs_queue.visit.arn
}

resource "aws_cloudwatch_event_target" "archive" {
  rule           = aws_cloudwatch_event_rule.archive.name
  event_bus_name = aws_cloudwatch_event_bus.events.name
  target_id      = "archive-queue"
  arn            = aws_sqs_queue.archive.arn
}

resource "aws_sqs_queue_policy" "visit" {
  queue_url = aws_sqs_queue.visit.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.visit.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.visits.arn } }
    }]
  })
}

resource "aws_sqs_queue_policy" "archive" {
  queue_url = aws_sqs_queue.archive.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.archive.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.archive.arn } }
    }]
  })
}
```

Create `infra/terraform/lambdas.tf`:

```hcl
# Built by `make package` (build/ is gitignored); boto3 comes from the runtime.
data "archive_file" "standdown_checker" {
  type        = "zip"
  source_dir  = "${path.module}/../../build/standdown_checker"
  output_path = "${path.module}/../../build/standdown_checker.zip"
}

data "archive_file" "archiver" {
  type        = "zip"
  source_dir  = "${path.module}/../../build/archiver"
  output_path = "${path.module}/../../build/archiver.zip"
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_cloudwatch_log_group" "standdown_checker" {
  name              = "/aws/lambda/onside-standdown-checker"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "archiver" {
  name              = "/aws/lambda/onside-archiver"
  retention_in_days = 7
}

resource "aws_iam_role" "standdown_checker" {
  name               = "onside-standdown-checker"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "standdown_checker" {
  name = "standdown-checker"
  role = aws_iam_role.standdown_checker.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
        Resource = aws_sqs_queue.visit.arn
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:PutItem", "dynamodb:ConditionCheckItem"]
        Resource = [aws_dynamodb_table.person_state.arn, aws_dynamodb_table.evaluations.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem"]
        Resource = aws_dynamodb_table.properties.arn
      },
      {
        Effect   = "Allow"
        Action   = ["events:PutEvents"]
        Resource = aws_cloudwatch_event_bus.events.arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.standdown_checker.arn}:*"
      },
    ]
  })
}

resource "aws_iam_role" "archiver" {
  name               = "onside-archiver"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "archiver" {
  name = "archiver"
  role = aws_iam_role.archiver.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
        Resource = aws_sqs_queue.archive.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.data.arn}/raw/*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.archiver.arn}:*"
      },
    ]
  })
}

resource "aws_lambda_function" "standdown_checker" {
  function_name    = "onside-standdown-checker"
  role             = aws_iam_role.standdown_checker.arn
  runtime          = "python3.12"
  handler          = "standdown_checker.handler.handler"
  filename         = data.archive_file.standdown_checker.output_path
  source_code_hash = data.archive_file.standdown_checker.output_base64sha256
  timeout          = 25 # below the visit queue's 30 s visibility timeout
  memory_size      = 256

  environment {
    variables = {
      PERSON_TABLE     = aws_dynamodb_table.person_state.name
      EVALUATION_TABLE = aws_dynamodb_table.evaluations.name
      PROPERTY_TABLE   = aws_dynamodb_table.properties.name
      EVENT_BUS_NAME   = aws_cloudwatch_event_bus.events.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.standdown_checker, aws_iam_role_policy.standdown_checker]
}

resource "aws_lambda_function" "archiver" {
  function_name    = "onside-archiver"
  role             = aws_iam_role.archiver.arn
  runtime          = "python3.12"
  handler          = "archiver.handler.handler"
  filename         = data.archive_file.archiver.output_path
  source_code_hash = data.archive_file.archiver.output_base64sha256
  timeout          = 25 # below the archive queue's 30 s visibility timeout
  memory_size      = 256

  environment {
    variables = {
      BUCKET = aws_s3_bucket.data.bucket
    }
  }

  depends_on = [aws_cloudwatch_log_group.archiver, aws_iam_role_policy.archiver]
}

resource "aws_lambda_event_source_mapping" "visits" {
  event_source_arn        = aws_sqs_queue.visit.arn
  function_name           = aws_lambda_function.standdown_checker.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]
}

resource "aws_lambda_event_source_mapping" "archive" {
  event_source_arn = aws_sqs_queue.archive.arn
  function_name    = aws_lambda_function.archiver.arn
  # AWS rejects a batch size above 10 without a batching window (spec §6.5).
  batch_size                         = 100
  maximum_batching_window_in_seconds = 5
}
```

Create `infra/terraform/outputs.tf`:

```hcl
# Read by the generator from state/<env>/outputs.json (`make deploy` saves them).
output "region" {
  value = var.region
}

output "bucket" {
  value = aws_s3_bucket.data.bucket
}

output "person_table" {
  value = aws_dynamodb_table.person_state.name
}

output "evaluation_table" {
  value = aws_dynamodb_table.evaluations.name
}

output "property_table" {
  value = aws_dynamodb_table.properties.name
}

output "bus_name" {
  value = aws_cloudwatch_event_bus.events.name
}

output "visit_queue_url" {
  value = aws_sqs_queue.visit.url
}

output "visit_dlq_url" {
  value = aws_sqs_queue.visit_dlq.url
}

output "archive_queue_url" {
  value = aws_sqs_queue.archive.url
}

output "archive_dlq_url" {
  value = aws_sqs_queue.archive_dlq.url
}

output "standdown_function" {
  value = aws_lambda_function.standdown_checker.function_name
}

output "archiver_function" {
  value = aws_lambda_function.archiver.function_name
}
```


- [ ] **Step 2: Append the infrastructure targets to the Makefile**

Append to the end of `Makefile`:

```make

# ---- infrastructure (ENV=local|aws, always explicit) ----

LOCALSTACK_ENDPOINT ?= http://localhost:4566
TF := terraform -chdir=$(REPO_ROOT)/infra/terraform
# One local state file per environment: infra/terraform/state/<env>.tfstate (spec §8).
TF_INIT = $(TF) init -input=false -reconfigure \
	-backend-config=path=$(REPO_ROOT)/infra/terraform/state/$(ENV).tfstate
TF_VARS = -var env=$(ENV) $(if $(filter local,$(ENV)),-var localstack_endpoint=$(LOCALSTACK_ENDPOINT))

.PHONY: infra-env deploy destroy tf-check

infra-env:
	@if [ "$(origin ENV)" != "command line" ] || { [ "$(ENV)" != local ] && [ "$(ENV)" != aws ]; }; then \
		echo "error: this target needs ENV=local or ENV=aws on the command line" >&2; exit 2; fi

deploy: infra-env package
	mkdir -p $(REPO_ROOT)/infra/terraform/state $(REPO_ROOT)/state/$(ENV)
	$(TF_INIT)
	$(TF) apply -input=false -auto-approve $(TF_VARS)
	$(TF) output -json > $(REPO_ROOT)/state/$(ENV)/outputs.json

destroy: infra-env package
	$(TF_INIT)
	$(TF) destroy -input=false -auto-approve $(TF_VARS)
	$(if $(filter aws,$(ENV)),rm -rf $(REPO_ROOT)/state/aws)

tf-check:
	$(TF) fmt -check -recursive
	$(TF) init -input=false -backend=false
	$(TF) validate
```

- [ ] **Step 3: Add the CI job**

Append to the end of `.github/workflows/ci.yml`:

```yaml

  terraform:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.15.8"
      - run: make tf-check
```

- [ ] **Step 4: Format and validate**

Run: `make tf-check`
Expected: no `fmt` output, `Terraform has been successfully initialized!` (first run downloads
hashicorp/aws 6.x and hashicorp/archive 2.x), then `Success! The configuration is valid.`

- [ ] **Step 5: Check the guards**

Run: `make deploy; echo "exit=$?"; make destroy ENV=sim; echo "exit=$?"; ENV=aws make destroy; echo "exit=$?"`
Expected: three times `error: this target needs ENV=local or ENV=aws on the command line` and `exit=2`.

- [ ] **Step 6: Deploy to LocalStack**

LocalStack must be up from Task 9 (`make up` if in doubt).
Run: `time make deploy ENV=local`
Expected: `Apply complete! Resources: 25 added, 0 changed, 0 destroyed.`, the outputs printed,
about 1.5–2 minutes (SQS queues and queue policies take ~25 s each on LocalStack).
Run: `python3 -m json.tool state/local/outputs.json | grep -c '"value"'`
Expected: `12`.

- [ ] **Step 7: Prove no drift and an idempotent redeploy**

Run: `terraform -chdir=infra/terraform plan -detailed-exitcode -input=false -var env=local -var localstack_endpoint=http://localhost:4566 > /dev/null; echo "plan_exit=$?"`
Expected: `plan_exit=0`.
Run: `make deploy ENV=local 2>&1 | grep 'Apply complete'`
Expected: `Apply complete! Resources: 0 added, 0 changed, 0 destroyed.`

- [ ] **Step 8: Smoke-test both Lambdas on LocalStack (the archiver writes S3 through the injected endpoint)**

Run:
```bash
PYTHONPATH=lambdas .venv/bin/python - <<'EOF'
import time
from datetime import UTC, datetime

from generator import aws as A
from generator.emit import emit
from generator.stream import Emission
from tests.helpers import checkin

conn = A.connect("local")
conn.dynamodb.put_item(
    TableName=conn.outputs.property_table,
    Item={"property_id": {"S": "SMOKE-P"}, "property_type": {"S": "poultry"},
          "owner_company_id": {"S": "SMOKE-C"}},
)
when = "2026-03-03T09:00:00+13:00"
emit([Emission(datetime.now(UTC), "CheckIn", checkin(f"smoke-{i}", "SMOKE-P", when,
                                                     person=f"SMOKE-U{i}"))
      for i in range(30)], conn.events, conn.outputs.bus_name)
deadline, lines = time.time() + 180, []
while time.time() < deadline:
    keys = conn.keys("raw/")
    lines = [conn.s3.get_object(Bucket=conn.outputs.bucket, Key=k)["Body"].read().count(b"\n")
             for k in keys]
    if sum(lines) >= 60:
        break
    time.sleep(3)
evaluations = conn.dynamodb.scan(TableName=conn.outputs.evaluation_table, Select="COUNT")["Count"]
print(f"evaluations={evaluations} raw_objects={len(lines)} archived={sum(lines)} max_lines={max(lines)}")
EOF
```
Expected: `evaluations=30 raw_objects=<n> archived=60 max_lines=<m>` with `n` < 60 (the archiver
batches; 30 app events plus 30 `CheckInEvaluated`). If `archived` stays below 60, read the logs:
`docker logs $(docker compose ps -q localstack) 2>&1 | grep -iE 'error|exception' | tail`.

- [ ] **Step 9: Destroy and recreate (proves `destroy` on a non-empty bucket)**

Run: `time make destroy ENV=local 2>&1 | grep -E 'Destroy complete|Error'`
Expected: `Destroy complete! Resources: 25 destroyed.` (about 2 minutes).
Run: `make up && make deploy ENV=local 2>&1 | grep 'Apply complete'`
Expected: `Apply complete! Resources: 25 added, 0 changed, 0 destroyed.`

- [ ] **Step 10: Commit**

```bash
make fmt && make test
git add infra/terraform/versions.tf \
  infra/terraform/variables.tf \
  infra/terraform/provider.tf \
  infra/terraform/storage.tf \
  infra/terraform/events.tf \
  infra/terraform/lambdas.tf \
  infra/terraform/outputs.tf \
  infra/terraform/.terraform.lock.hcl \
  Makefile \
  .github/workflows/ci.yml
git commit -m "$(cat <<'EOF'
Terraform: one codebase for LocalStack and AWS; deploy, destroy, tf-check

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

---

### Task 11: dbt on LocalStack S3 and the reduced end-to-end run (brief decision 10, spec §7.4)

**Files:**
- Modify: `dbt/profiles.yml` (targets `local` and `aws`)
- Modify: `Makefile` (the dbt block reads S3 for `local`/`aws`)

**Interfaces:**
- Consumes: `state/<env>/outputs.json` (`bucket`), S3 `compacted/<env>/*/*.ndjson` and `ref/*.json`
  (Tasks 7–8, 10); the dbt project and its `compacted_events()`/`ref_file()` macros (Plan 1).
- Produces: `make dbt-build ENV=local|aws` → `warehouse/<env>.duckdb`. The SQL is unchanged:
  only `DBT_COMPACTED_GLOB` and `DBT_REF_ROOT` differ per target (spec §3.3 table).

DuckDB reads S3 through `httpfs` with a profile-level secret: LocalStack (path style, no TLS,
`test`/`test`) for `local`, the default credential chain (`aws` extension) for `aws`. DuckDB
installs `httpfs`/`aws` from its extension repository on first use (network needed once; then
cached under `~/.duckdb/extensions`).

- [ ] **Step 1: Add the dbt targets**

Replace `dbt/profiles.yml`:

```yaml
onside_movement:
  target: sim
  outputs:
    sim:
      type: duckdb
      path: "{{ env_var('REPO_ROOT', '.') }}/warehouse/sim.duckdb"
      threads: 4
      extensions: [icu, json]
      settings:
        TimeZone: UTC
    # LocalStack S3 (spec §3.3): path-style, no TLS, LocalStack's fixed test credentials.
    local:
      type: duckdb
      path: "{{ env_var('REPO_ROOT', '.') }}/warehouse/local.duckdb"
      threads: 4
      extensions: [icu, json, httpfs]
      settings:
        TimeZone: UTC
      secrets:
        - type: s3
          key_id: test
          secret: test
          region: ap-southeast-2
          endpoint: localhost:4566
          url_style: path
          use_ssl: false
    # Real AWS: the default credential chain (Plan 3 deploys it).
    aws:
      type: duckdb
      path: "{{ env_var('REPO_ROOT', '.') }}/warehouse/aws.duckdb"
      threads: 4
      extensions: [icu, json, httpfs, aws]
      settings:
        TimeZone: UTC
      secrets:
        - type: s3
          provider: credential_chain
          region: ap-southeast-2
```

- [ ] **Step 2: Point the Makefile dbt block at S3 for `local`/`aws`**

In `Makefile`, replace this text (it occurs once):

```make
DBT_ENV = DBT_PROFILES_DIR=$(REPO_ROOT)/dbt \
	DBT_COMPACTED_GLOB='$(REPO_ROOT)/data/$(ENV)/compacted/*/*.ndjson' \
	DBT_REF_ROOT='$(REPO_ROOT)/data/$(ENV)/ref/'

dbt-build:
	mkdir -p $(REPO_ROOT)/warehouse
	cd dbt && $(DBT_ENV) $(DBT) build --target $(ENV)
```

with:

```make
# dbt reads local files for sim and S3 for local/aws (spec §3.3 table); the bucket comes
# from state/<env>/outputs.json, written by `make deploy`.
ifeq ($(ENV),sim)
DATA_GLOB = $(REPO_ROOT)/data/sim/compacted/*/*.ndjson
REF_ROOT = $(REPO_ROOT)/data/sim/ref/
else
BUCKET = $(shell $(PY) -c 'import json, sys; print(json.load(open(sys.argv[1]))["bucket"]["value"])' \
	$(REPO_ROOT)/state/$(ENV)/outputs.json 2>/dev/null)
DATA_GLOB = s3://$(BUCKET)/compacted/$(ENV)/*/*.ndjson
REF_ROOT = s3://$(BUCKET)/ref/
endif

DBT_ENV = DBT_PROFILES_DIR=$(REPO_ROOT)/dbt \
	DBT_COMPACTED_GLOB='$(DATA_GLOB)' \
	DBT_REF_ROOT='$(REF_ROOT)'

dbt-build:
	@if [ "$(ENV)" != sim ] && [ -z "$(BUCKET)" ]; then \
		echo "error: no bucket in state/$(ENV)/outputs.json; run make deploy ENV=$(ENV)" >&2; exit 2; fi
	mkdir -p $(REPO_ROOT)/warehouse
	cd dbt && $(DBT_ENV) $(DBT) build --target $(ENV)
```

- [ ] **Step 3: Fresh stack, and the guard without a deployment**

Run: `make up && make dbt-build ENV=local; echo "exit=$?"`
Expected: `error: no bucket in state/local/outputs.json; run make deploy ENV=local` and `exit=2`.

- [ ] **Step 4: Deploy, set up and backfill at a reduced scale (`SCALE=aws`: 40 properties, 100 people, 3,000 check-ins)**

Run: `make deploy ENV=local && make setup ENV=local SCALE=aws`
Expected: `setup ENV=local: 40 properties, 100 people`.
Run: `time make generate ENV=local MODE=backfill`
Expected (dry run, 4 CPUs): `emitted 5604/5604 events (131 events/s)`, then
`settle <run>: waiting for ...` lines every 30 s, then
`settled run <run> in ~280s: ~11600 envelopes, ~485 breaches -> compacted/local/<run>/part-0000.ndjson`;
wall time about 5–6 minutes. Exact counts depend on the anchor (wall clock).

- [ ] **Step 5: Build dbt on the LocalStack data**

Run: `make dbt-build ENV=local 2>&1 | tail -3`
Expected: `Done. PASS=77 WARN=0 ERROR=0 SKIP=0 ...` — the parity proof on the AWS-style chain.
Run:
```bash
.venv/bin/python -c "
import duckdb
con = duckdb.connect('warehouse/local.duckdb', read_only=True)
print(con.sql('select status, cause, sub_cause, count(*) from fct_alert_reconciliation group by all order by all').fetchall())"
```
Expected: only `matched` and `missed`/`origin_not_seen` or `origin_end_not_seen` rows (dry run:
485 matched, 19 offline, 4 reordered); never `false_alert`, `not_evaluated` or `unexplained`.

- [ ] **Step 6: The demo steps: a live run and poison**

Run: `time make generate ENV=local MODE=live N=50 && make dbt-build ENV=local 2>&1 | grep Done`
Expected: `settled run ... in ~45s`, then `PASS=77 WARN=0 ERROR=0`.
Run: `time make poison ENV=local K=2`
Expected: `visit DLQ holds 2 messages (expected at least 2)` after about 3 minutes.

- [ ] **Step 7: The sim path still builds**

Run: `make sim-e2e ANCHOR=2026-10-01T00:00:00+00:00 NOW=2026-10-09T04:00:00+00:00 2>&1 | grep Done`
Expected: `Done. PASS=77 WARN=0 ERROR=0 ...`.

- [ ] **Step 8: Commit**

```bash
make fmt && make test
git add dbt/profiles.yml \
  Makefile
git commit -m "$(cat <<'EOF'
dbt targets local and aws: DuckDB reads compacted NDJSON and ref from S3

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

---

### Task 12: LocalStack integration test and `make integration` (spec §7.3)

**Files:**
- Create: `tests/integration/__init__.py` (empty)
- Create: `tests/integration/test_localstack_chain.py`
- Modify: `Makefile` (append `integration`)

**Interfaces:**
- Consumes: a freshly deployed `local` stack (`state/local/outputs.json`), `generator.aws.connect`,
  `generator.emit.emit`, `generator.settle.Settler`, `generator.queues`, `DynamoDbStore`,
  `tests.helpers.checkin/checkout`.
- Produces: `make integration` = `make up` → `make deploy ENV=local` → `LOCALSTACK=1 pytest -m integration tests/integration`.
  The test is skipped unless `LOCALSTACK=1`, so `make test` never needs Docker.

The fixed event set (spec §7.3): the worked example (poultry → office → poultry, a breach), a
duplicate check-in, an out-of-order pair (U2's later visit is sent and processed first), an orphan
check-out (its check-in never arrives → a placeholder), a plant movement, and one poison event.
Phase 1 is processed before phase 2 is sent, so the breach and the reordering are deterministic.
It then settles (the poison message keeps the visit queue busy until it reaches the DLQ, so this
takes about 3 minutes) and asserts person_state, evaluations, the compacted objects, archiver
batching and both DLQs. Properties use `IT-*` ids that the generator never produces.

- [ ] **Step 1: Write the test and the target**

Create an empty `tests/integration/__init__.py`, then:

Create `tests/integration/test_localstack_chain.py`:

```python
"""The full chain on a fresh LocalStack stack (spec §7.3). Run with `make integration`."""

import json
import os
import time
import uuid
from datetime import UTC, datetime

import pytest
from standdown_checker.dynamo_store import DynamoDbStore

from generator import aws as A
from generator import queues as Q
from generator.emit import emit
from generator.settle import Settler
from generator.stream import Emission, event_id
from tests.helpers import checkin, checkout

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("LOCALSTACK") != "1",
        reason="needs LOCALSTACK=1 and a freshly deployed local stack (make integration)",
    ),
]

PROPERTIES = {  # ids that the generator never uses, so they cannot clash with demo data
    "IT-P1": ("poultry", "IT-C2"),
    "IT-P2": ("office", "IT-C3"),
    "IT-P3": ("poultry", "IT-C1"),
}
ORPHAN = "00000000-0000-4000-8000-0000000000aa"  # never sent


def app(detail_type: str, detail: dict) -> Emission:
    return Emission(datetime.now(UTC), detail_type, detail)


def wait_until(condition, timeout: float = 180, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(2)


@pytest.fixture
def chain(tmp_path, monkeypatch):
    conn = A.connect("local")  # reads state/local/outputs.json written by make deploy
    if conn.keys("control/") or conn.keys("ref/"):
        pytest.fail("the stack is not fresh; run `make integration` (it starts with make up)")
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))  # the settle spool goes to a temp dir
    for property_id, (ptype, owner) in PROPERTIES.items():
        conn.dynamodb.put_item(
            TableName=conn.outputs.property_table,
            Item={
                "property_id": {"S": property_id},
                "property_type": {"S": ptype},
                "owner_company_id": {"S": owner},
            },
        )
    store = DynamoDbStore(
        conn.dynamodb,
        conn.outputs.person_table,
        conn.outputs.evaluation_table,
        conn.outputs.property_table,
    )
    return conn, store


def test_events_flow_through_the_chain_and_settle(chain):
    conn, store = chain
    run_id = str(uuid.uuid4())
    a, co_a, b, c, d, e, orphan, plant = (event_id(run_id, seq) for seq in range(8))
    started_at = datetime.now(UTC).replace(microsecond=0)
    u1, u2, u3 = {"person": "IT-U1"}, {"person": "IT-U2"}, {"person": "IT-U3"}
    plant_movement = {
        "event_id": plant,
        "source_property_id": "IT-P1",
        "dest_property_id": "IT-P3",
        "movement_date": "2026-03-05",
        "material_type": "budwood",
        "volume": 10,
        "received_time": "2026-03-06T01:00:00+13:00",
    }

    # Phase 1: U1's first two visits; U2's LATER visit E; an orphan check-out; a plant
    # movement; one poison event.
    phase_1 = [
        app("CheckIn", checkin(a, "IT-P1", "2026-03-03T09:00:00+13:00", **u1)),
        app("CheckOut", checkout(co_a, a, "IT-P1", "2026-03-03T11:00:00+13:00", **u1)),
        app("CheckIn", checkin(b, "IT-P2", "2026-03-04T08:00:00+13:00", **u1)),
        app("CheckIn", checkin(e, "IT-P3", "2026-03-05T16:00:00+13:00", **u2)),
        app("CheckOut", checkout(orphan, ORPHAN, "IT-P2", "2026-03-05T12:00:00+13:00", **u3)),
        app("PlantMovement", plant_movement),
        app("CheckIn", {"poison": True, "event_id": str(uuid.uuid4())}),
    ]
    emit(phase_1, conn.events, conn.outputs.bus_name)

    def phase_1_done() -> bool:
        return (
            store.load_person("IT-U1")[0] == 3
            and store.load_person("IT-U3")[0] == 1
            and store.get_evaluation(e) is not None
        )

    wait_until(phase_1_done, what="phase 1 to be processed")

    # Phase 2: the breach C (sent twice: a duplicate) and U2's EARLIER visit D, out of order.
    c_detail = checkin(c, "IT-P3", "2026-03-04T14:00:00+13:00", **u1)
    emit(
        [
            app("CheckIn", c_detail),
            app("CheckIn", c_detail),
            app("CheckIn", checkin(d, "IT-P1", "2026-03-05T09:00:00+13:00", **u2)),
        ],
        conn.events,
        conn.outputs.bus_name,
    )

    manifest = {
        "run_id": run_id,
        "mode": "live",
        "counts": {"CheckIn": 5, "CheckOut": 2, "PlantMovement": 1},
        "total": 8,
        "started_at": started_at.isoformat(),
    }
    settled = Settler(manifest, conn, quiet_seconds=10, timeout_seconds=600).run()

    # person_state: every distinct event committed exactly once, whatever the order.
    version, visits = store.load_person("IT-U1")
    assert version == 4
    assert [v["checkin_event_id"] for v in visits] == [a, b, c]
    assert visits[0]["checkout_event_id"] == co_a
    version, visits = store.load_person("IT-U2")
    assert version == 2
    assert [v["checkin_event_id"] for v in visits] == [d, e]  # the late D sorted first
    assert store.get_evaluation(e)["version_read"] == 0
    assert store.get_evaluation(d)["version_read"] == 1
    version, visits = store.load_person("IT-U3")
    assert version == 1
    assert visits[0]["checkin_time"] is None and visits[0]["checkout_event_id"] == orphan

    # evaluations: the worked example breaches (poultry -> office -> poultry, 1 night of 3).
    evaluation = store.get_evaluation(c)
    assert evaluation["breach"] is True
    assert (evaluation["rule_id"], evaluation["origin_checkin_event_id"]) == ("R02", a)

    # compacted objects: complete, poison-free, and the duplicate re-emitted identically.
    lines = []
    for key in settled["parts"]:
        body = conn.s3.get_object(Bucket=conn.outputs.bucket, Key=key)["Body"].read().decode()
        lines += [json.loads(line) for line in body.splitlines()]
    assert not any("poison" in env["detail"] for env in lines)
    ids = {env["detail"].get("event_id") for env in lines if env["source"] == "onside.app"}
    assert {a, co_a, b, c, d, e, orphan, plant} <= ids
    c_evaluations = [
        env["detail"]
        for env in lines
        if env["detail-type"] == "CheckInEvaluated" and env["detail"]["checkin_event_id"] == c
    ]
    assert len(c_evaluations) == 2 and c_evaluations[0] == c_evaluations[1]
    alerts = [env for env in lines if env["detail-type"] == "HighRiskVisitDetected"]
    assert {env["detail"]["checkin_event_id"] for env in alerts} == {c}
    assert conn.keys(f"control/settled/{run_id}.json")

    # The archiver batches: fewer raw objects than archived envelopes.
    raw = conn.keys("raw/")
    assert 0 < len(raw) < len(lines)

    # DLQs: the poison event reaches the visit DLQ; the archive DLQ stays empty.
    assert Q.wait_for_dlq(conn.sqs, conn.outputs.visit_dlq_url, 1, timeout=300) >= 1
    assert Q.visible(conn.sqs, conn.outputs.archive_dlq_url) == 0
```

Append to the end of `Makefile`:

```make

# ---- integration (spec §7.3) ----

.PHONY: integration

# The full chain on a fresh LocalStack. Run `make up` again before a demo backfill.
integration: up
	$(MAKE) deploy ENV=local
	LOCALSTACK=1 $(VENV)/bin/pytest -q -m integration tests/integration
```

- [ ] **Step 2: It is skipped by default**

Run: `.venv/bin/pytest tests/integration -q`
Expected: `1 skipped`.

- [ ] **Step 3: Run it on a fresh stack**

Run: `time make integration 2>&1 | tail -3`
Expected: `1 passed in ~190s`; about 5 minutes in total (up ~12 s, deploy ~1.5 min, test ~3 min).

- [ ] **Step 4: Reset the stack (integration data must never mix with demo data)**

Run: `make up`
Expected: `Healthy`; `state/local/` is gone.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add tests/integration/__init__.py \
  tests/integration/test_localstack_chain.py \
  Makefile
git commit -m "$(cat <<'EOF'
LocalStack integration test: out-of-order, duplicate, orphan and poison cases

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

---

### Task 13: Measured `local`-scale run, the scale check, README status (spec §3.4, brief decision 12)

**Files:**
- Modify: `README.md` (Plan 2 status section)
- Modify: `docs/superpowers/plans/2026-10-10-plan-2-aws-wiring.md` (append "Measured `local`-scale run")
- Modify only if the run exceeds 45 minutes: `generator/config.py`, `tests/test_aws_env.py`

**Interfaces:**
- Consumes: everything above; no new code.
- Produces: the measured numbers (throughput and wall time) that justify the `local` scale, and a
  LocalStack stack holding a settled backfill (the demo's starting state for Plan 3).

- [ ] **Step 1: A fresh stack with the default `local` scale**

Run: `docker ps --format '{{.Names}} {{.Ports}}' | grep 4566` — only this repo's LocalStack (or nothing).
Run: `make up && make deploy ENV=local && make setup ENV=local`
Expected: `Apply complete! Resources: 25 added ...` and `setup ENV=local: 200 properties, 600 people`.

- [ ] **Step 2: The measured backfill**

Run: `/usr/bin/time -f 'wall %e s' make generate ENV=local MODE=backfill 2>&1 | tee /tmp/local-backfill.log | grep -E 'emitted [0-9]+/[0-9]+ events|backfill run|settled run|^wall'`
Expected (dry run, 4 CPUs, other work on the machine):
```
emitted 18313/18313 events (125 events/s)
backfill run <run_id> anchor=...: {'CheckIn': 9227, 'CheckOut': 8494, 'PlantMovement': 250} -> {'sent': 18313}
settled run <run_id> in 829s: 37371 envelopes, 975 breaches -> compacted/local/<run_id>/part-0000.ndjson
wall 979 s
```
(The dry-run wall time also includes the few seconds of `make setup`.)
Counts vary slightly with the anchor (the wall clock). Throughput = events sent ÷ wall seconds
(dry run: 18,313 ÷ 979 ≈ 19 events/s end to end; the stand-down Lambda runs at 22–24 events/s
once the emitter is done).

- [ ] **Step 3: Apply the 45-minute rule**

If `wall` ≤ 2,700 s: keep `local` at 10,000 check-ins (the dry run needed about 16 minutes).

Only if `wall` > 2,700 s: set the `local` scale's `n_checkins` to
`1000 × floor(10 × 2700 / wall)` (for example 3,600 s → 7,000) in `generator/config.py`
(`"local": Scale("local", 200, 600, 50, 15, <new>)`) and the assertion
`(200, 600, <new>)` in `tests/test_aws_env.py::test_environments_and_their_default_scales`,
run `make test`, then repeat Steps 1–2 and record both runs. Tell the controller, because spec
§3.2 says "about 10,000" and needs an amendment.

- [ ] **Step 4: dbt on the full LocalStack dataset**

Run: `make dbt-build ENV=local 2>&1 | tail -1`
Expected: `Done. PASS=77 WARN=0 ERROR=0 ...`.
Run:
```bash
.venv/bin/python -c "
import duckdb
con = duckdb.connect('warehouse/local.duckdb', read_only=True)
print(con.sql('select status, cause, sub_cause, count(*) from fct_alert_reconciliation group by all order by all').fetchall())"
```
Expected (dry run): `[('matched', None, None, 975), ('missed', 'origin_end_not_seen', None, 4),
('missed', 'origin_not_seen', 'offline', 42), ('missed', 'origin_not_seen', 'reordered', 1)]` —
no `false_alert`, `not_evaluated` or `unexplained`.

- [ ] **Step 5: Record the measurement in this plan**

Append to the end of `docs/superpowers/plans/2026-10-10-plan-2-aws-wiring.md`, with your numbers
from Steps 2–4 in place of the dry-run ones shown here:

```markdown
## Measured `local`-scale run (Task 13)

| | Dry run (2026-10-10) | Execution |
|---|---|---|
| Check-ins / check-outs / plant movements | 9,227 / 8,494 / 250 | … |
| Events emitted, emitter rate | 18,313 at 125 events/s | … |
| Settle time | 829 s | … |
| Wall time (`make generate MODE=backfill`) | 979 s (16.3 min, with setup) | … |
| End-to-end throughput | 18.7 events/s | … |
| `dbt build` | PASS=77 WARN=0 ERROR=0 | … |
| Reconciliation | 975 matched; missed 42 offline, 1 reordered, 4 origin_end_not_seen; 0 false, 0 unexplained | … |
| `local` scale decision | keep 10,000 check-ins (< 45 min) | … |
```

Fill the "Execution" column with the measured values (a value, never "…", in the committed file).

- [ ] **Step 6: README status**

In `README.md`, replace this text (it occurs once):

```markdown
Next: Plan 2 (AWS wiring on LocalStack), Plan 3 (Dagster, AWS deploy, demo).
```

with:

```markdown
## Status (Plan 2: AWS wiring on LocalStack)

The same chain that Terraform deploys to AWS, on LocalStack 4.14.0 (community, no token):

    make up                                   # a fresh, empty LocalStack (also wipes state/local)
    make deploy ENV=local                     # Terraform: bus, 2 rules, 2 queues + 2 DLQs,
                                              # 2 Lambdas, 3 DynamoDB tables, the bucket
    make setup ENV=local                      # reference data -> S3 ref/ and DynamoDB properties
    make generate ENV=local MODE=backfill     # emit, then settle and compact (about 16 min)
    make dbt-build ENV=local                  # dbt reads s3://<bucket>/compacted/local/
    make generate ENV=local MODE=live N=50    # the demo step; settles in under a minute
    make poison ENV=local K=2                 # malformed events reach the visit DLQ (~3 min)
    make settle ENV=local RUN=<run_id>        # resume a settle that timed out
    make integration                          # the full chain on a fresh stack; make up after

`make deploy` and `make destroy` never default: they need `ENV=local` or `ENV=aws` on the
command line. `make test` needs no Docker (moto stands in for AWS).

Measured on a 4-CPU laptop (`local` scale: 200 properties, 600 people, 9,227 check-ins,
18,313 events): emitted at 125 events/s, settled 14 minutes later, 16 minutes in total
(about 19 events/s end to end); `dbt build` PASS=77, ERROR=0; reconciliation 975 matched,
47 missed with a version-proven cause, 0 false alerts, 0 unexplained.

Next: Plan 3 (Dagster, CI LocalStack job, the AWS deploy, the demo).
```

If your measured numbers differ from the dry run's, change the "Measured on a 4-CPU laptop"
paragraph to yours.

- [ ] **Step 7: Commit**

```bash
make fmt && make test
git add README.md \
  docs/superpowers/plans/2026-10-10-plan-2-aws-wiring.md
git commit -m "$(cat <<'EOF'
Measured local-scale LocalStack run; README Plan 2 status

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
EOF
)"
```

Leave the stack running if Plan 3 follows (it starts from this settled backfill); otherwise
`make down`.


---

## Decisions this plan makes beyond the brief

- **Terraform state per environment** uses `backend "local" {}` with
  `init -reconfigure -backend-config=path=infra/terraform/state/<env>.tfstate`, because `-state`
  is deprecated in Terraform 1.15 (the dry run printed the warning). Spec §8 allows a backend
  config per environment.
- **One codebase** = `var.localstack_endpoint` plus a `dynamic "endpoints"` provider block and
  conditional test credentials/skip flags; proven by apply, a zero-diff plan, redeploy, destroy.
- **Handler PutEvents** are sent for the whole SQS batch in calls of up to 10 entries; a rejected
  entry fails only the record that produced it (`Entries[i].ErrorCode`).
- **Archiver** re-serialises each body as compact JSON (guarantees one envelope per line) and
  uses path-style S3 inside LocalStack (the injected endpoint is a container IP).
- **Manifests** gain `started_at` (UTC wall clock before emitting): settle's listing window.
- **Settle** removes the spool and key cache after a successful compaction, drops envelopes
  archived twice (same EventBridge `id`) when compacting, and spools other runs' envelopes found
  in the window (staging deduplicates them; a live run's compacted part therefore also repeats
  the backfill's envelopes from the same hours).
- **`InMemoryStore.get_property` returns a copy**, now part of the contract (deferred Plan 1 minor).
- **Integration test** seeds `IT-*` properties directly instead of `make setup`, and
  `make integration` always starts with `make up`.
- **Per-environment default scale** in the CLI (`sim: ci`, `local: local`, `aws: aws`); the
  Makefile's `SCALE` is empty by default. `make tf-check` and a CI `terraform` job (fmt + validate,
  no credentials) are added; the LocalStack CI job stays in Plan 3.
- **Measurement:** the full `local` scale ran in the dry run (16 minutes), so 10,000 check-ins stay.
