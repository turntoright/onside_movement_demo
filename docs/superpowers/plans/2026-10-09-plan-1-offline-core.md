# Plan 1 — Offline core (rules, processor, generator, sim, dbt, tracing) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build everything in the Onside movement demo that runs without AWS:
- the stand-down rules and event processor;
- the synthetic generator;
- the `ENV=sim` simulator;
- all dbt models and tests, including reconciliation;
- the tracing algorithm.

This plan also runs the Day-1 LocalStack spike that Plan 2 depends on.

**Architecture:**
- **Shared logic.** Pure stand-down logic (`standdown_checker.rules`) and an order-independent processor (`standdown_checker.processor`). The processor runs over a `StateStore` interface; Plan 2 adds a DynamoDB store. Here an in-memory store, persisted between runs, drives the simulator.
- **Data path.** The generator emits EventBridge-style envelopes. The simulator processes them in received order and writes compacted NDJSON. dbt-duckdb builds staging, intermediate and mart models from those files, and its tests prove that realtime and batch agree.

**Tech Stack:**
- Python 3.12 (stdlib `zoneinfo` + `tzdata`)
- DuckDB 1.5, dbt-core with dbt-duckdb 1.11
- networkx 3.7 (+ scipy, pandas)
- pytest 9, ruff 0.16
- Docker plus LocalStack, for the spike only

**Spec:** `docs/superpowers/specs/2026-10-09-onside-movement-demo-design.md` (revision 6.1). Read it before starting. Section numbers below (§) refer to it.

**Dry run (2026-10-09, while writing the plan).** Every code block in this plan was extracted into a scratch repo and run. Results:
- 92 pytest tests pass;
- `ruff check` is clean after `make fmt`;
- `dbt build` on sim data gives PASS=72, WARN=0, ERROR=0;
- reconciliation: 649 matched, 23 missed (`origin_not_seen` / offline), 2 missed (`origin_end_not_seen`), 0 false alerts, 0 unexplained.

A failure while executing therefore most likely means a copy slip or an environment difference, not a design gap.

**Plans 2 and 3 come later:**
- **Plan 2:** AWS wiring (Terraform, DynamoDB store, handler, settle and compact, LocalStack integration).
- **Plan 3:** Dagster, CI for AWS, the real AWS deploy, the README.

Both are written after this plan finishes, using the Task 1 spike findings.

## Global Constraints

- Python **3.12**. The project venv lives at `.venv`.
- **Always call `.venv/bin/dbt`.** The global `~/.local/bin/dbt` is dbt Fusion 2.0, which has no DuckDB adapter.
- **Version pins:**
  - `dbt-duckdb~=1.11.0`
  - `duckdb>=1.5,<1.6`
  - `networkx~=3.7`
  - `pytest~=9.1`
  - `ruff~=0.16`
  - `boto3>=1.40`
  - `tzdata>=2026.1`
- **Timestamps:**
  - Every timestamp is ISO 8601 with a UTC offset. "Local" means `Pacific/Auckland`.
  - Event times are written by `standdown_checker.events.iso()` with NZ offset and seconds precision. `processed_at` uses milliseconds.
- **Rules:**
  - `shared/standdown_rules.csv` is the single source of rules.
  - `(from_type, to_type, scope)` is unique; `1 ≤ nights ≤ 14`.
- **Constants (spec §3.3, §4):**
  - window = 14 nights;
  - `origins_seen` bound = 16 local days;
  - visits last ≤ 48 h;
  - ≤ 300 check-ins per person;
  - offline ≈ 5%, lateness ≤ 7 days in backfill and ≤ 10 minutes in live;
  - duplicates ≈ 2%, delay ≤ 7 days in backfill and ≤ 1 minute in live.
- **Ids:**
  - Generator events: `event_id = uuid5(run_id, str(seq))`.
  - Derived events: `uuid5(<fixed namespace>, "<kind>:<key>")`, with kind in `eval`, `co`, `alert`.
- **Environments:** this plan implements only `ENV=sim`. The values `local` and `aws` arrive in Plan 2.
- **Paths (all gitignored):** `state/<env>/`, `data/<env>/ref/`, `data/<env>/compacted/<run_id>/*.ndjson`, `warehouse/<env>.duckdb`.
- All code comments and docs are in English.
- Before each commit, run `make fmt` and `make test`; both must pass.
- **Commit trailers:** end every commit message with these two lines:
  ```
  Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL
  ```

## Review Focus

Five input classes that the spec implies but the obvious tests would miss. Each has a pinned test in the task that owns the code:

1. **Timestamps at a local midnight or a daylight-saving change.** Nights must count local dates on both sides. Pinned by golden cases `G07_dst_local_dates` and `G08_midnight_boundary` (Task 3), which dbt also runs (Task 14).
2. **A duplicate CheckOut that arrives before its CheckIn.** A placeholder holds the check-out. The repeat must re-emit an identical `CheckOutApplied`, and the later CheckIn must still be evaluated. Pinned in Task 7, `test_duplicate_checkout_before_checkin_reemits_same_applied_event`.
3. **A live run that asks for more check-ins than people with room.** The generator must emit fewer, report the shortfall, and never overlap visits. Pinned in Task 10, `test_live_shortfall_when_no_one_has_room`.
4. **Running `generate` in the wrong order.** A second backfill, a live run before any backfill, or a deleted manifest must each fail with a clear fix-it message and exit code 2. Pinned in Task 11.
5. **Tracing from an unknown property or one with no outgoing edges.** An unknown id gives a clear error; a property with no edges gives a seed-only result. Pinned in Task 18.

---

## File Structure

```
onside_movement_demo/
├── pyproject.toml                         # Task 0
├── Makefile                               # Task 0, extended in Tasks 11, 12, 18, 19
├── .gitignore                             # Task 0
├── README.md                              # Task 0 stub, Task 19 status section
├── docker-compose.yml                     # Task 1 (kept for Plan 2)
├── .env.example                           # Task 1
├── spikes/firehose_probe.py               # Task 1 (throwaway, kept as a record)
├── docs/spikes/2026-10-day1-localstack.md # Task 1 findings
├── shared/
│   ├── standdown_rules.csv                # Task 2
│   └── golden_cases.csv                   # Task 3
├── lambdas/standdown_checker/
│   ├── __init__.py                        # Task 2
│   ├── rules.py                           # Tasks 2–3: pure §4 logic
│   ├── events.py                          # Task 4: parsing, ids, iso()
│   ├── store.py                           # Task 5: StateStore protocol and errors
│   └── processor.py                       # Tasks 6–7: §6.1 processing
├── generator/
│   ├── __init__.py  __main__.py           # Task 8 / Task 11
│   ├── config.py                          # Task 8: scales and paths
│   ├── reference.py                       # Task 8: properties, people, companies
│   ├── stream.py                          # Tasks 9–10: planning and emissions
│   ├── state.py                           # Task 10: generator state and manifests
│   ├── local_sim.py                       # Task 5 store, Task 11 runner
│   └── cli.py                             # Task 11
├── tracing/
│   ├── __init__.py  __main__.py           # Task 18
│   ├── core.py                            # Task 18: hop-limited earliest arrival
│   └── cli.py                             # Task 18
├── dbt/                                   # Tasks 12–17
│   ├── dbt_project.yml  profiles.yml
│   ├── seeds/ (symlinks into ../../shared)
│   ├── macros/sources.sql  macros/standdown.sql
│   ├── models/staging/…  models/intermediate/…  models/marts/…
│   └── tests/…
├── tests/                                 # pytest
└── .github/workflows/ci.yml               # Task 19
```

---

### Task 0: Scaffold and toolchain

**Files:**
- Create: `pyproject.toml`
- Create: `Makefile`
- Create: `.gitignore`
- Create: `README.md`
- Create: `tests/__init__.py`
- Create: `tests/test_toolchain.py`

**Interfaces:**
- Produces:
  - `make venv`, `make fmt`, `make lint`, `make unit`, `make test`.
  - Every later task relies on `.venv/bin/{python,pytest,ruff,dbt}` existing.

- [ ] **Step 1: Write `pyproject.toml`**

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
dev = ["pytest~=9.1", "ruff~=0.16"]

[tool.setuptools.packages.find]
where = [".", "lambdas"]
include = ["generator*", "tracing*", "standdown_checker*"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = [".", "lambdas"]

[tool.ruff]
line-length = 100
target-version = "py312"
extend-exclude = ["dbt/target", "dbt/dbt_packages", "dbt/logs", "spikes"]

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP"]
```

- [ ] **Step 2: Write `Makefile`** (recipe lines start with a TAB character)

```make
SHELL := /bin/bash
REPO_ROOT := $(CURDIR)
VENV := $(REPO_ROOT)/.venv
PY := $(VENV)/bin/python
DBT := $(VENV)/bin/dbt
export REPO_ROOT

.PHONY: venv fmt lint unit test

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
```

- [ ] **Step 3: Write `.gitignore`**

```gitignore
.venv/
__pycache__/
*.egg-info/
.pytest_cache/
.ruff_cache/
state/
data/
warehouse/
build/
.env
dbt/target/
dbt/logs/
dbt/dbt_packages/
infra/terraform/state/
infra/terraform/.terraform/
```

- [ ] **Step 4: Write a `README.md` stub**

```markdown
# Onside movement demo

A portfolio demo: farm check-ins → stand-down (biosecurity) alerts in real time →
a warehouse that recomputes and reconciles them. Design:
`docs/superpowers/specs/2026-10-09-onside-movement-demo-design.md`.

Quick start (offline):

    make venv
    make test
```

- [ ] **Step 5: Write the failing toolchain test.** Create an empty `tests/__init__.py` and `tests/test_toolchain.py`:

```python
"""Checks that the local toolchain can do what the design relies on."""

import subprocess
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

REPO = Path(__file__).resolve().parents[1]


def test_zoneinfo_has_new_zealand():
    assert ZoneInfo("Pacific/Auckland").key == "Pacific/Auckland"


def test_duckdb_converts_timestamptz_to_new_zealand_date():
    # 2026-04-05 02:30 NZDT is after midnight local time, so the local date is 5 April.
    row = duckdb.sql(
        "select (cast('2026-04-05T02:30:00+13:00' as timestamptz) "
        "at time zone 'Pacific/Auckland')::date"
    ).fetchone()
    assert row[0] == date(2026, 4, 5)


def test_project_dbt_is_dbt_core_with_duckdb_adapter():
    out = subprocess.run(
        [str(REPO / ".venv" / "bin" / "dbt"), "--version"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert "duckdb" in out.lower()
```

- [ ] **Step 6: Create the venv and run the test**

Run: `make venv && make unit`
Expected: 3 passed. If `test_project_dbt_is_dbt_core_with_duckdb_adapter` fails, `pip install` did not install dbt-duckdb. Fix that before continuing.

- [ ] **Step 7: Lint and commit**

```bash
make fmt && make test
git add pyproject.toml Makefile .gitignore README.md tests/__init__.py tests/test_toolchain.py
git commit -m "Scaffold: pyproject, Makefile, toolchain checks

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 1: Day-1 LocalStack spike (throwaway)

This is a feasibility probe, not product code, so there is no TDD. Its output is a findings document that Plan 2 reads. See spec §8 (LocalStack), §3.4 (scale checks) and §6.5 (Firehose).

**Files:**
- Create: `docker-compose.yml`
- Create: `.env.example`
- Create: `spikes/firehose_probe.py`
- Create: `docs/spikes/2026-10-day1-localstack.md`

**Interfaces:**
- Produces:
  - `docker-compose.yml` (Plan 2's `make up` uses it);
  - the decided `LOCALSTACK_TAG` and auth approach;
  - measured Firehose object counts and parse throughput.

- [ ] **Step 1: Find the newest LocalStack 4.x tag published before 2026-03-01**

Run:
```bash
curl -s 'https://hub.docker.com/v2/repositories/localstack/localstack/tags?page_size=100&name=4.' \
  | python3 -c "import json,sys; [print(t['name'], t['last_updated']) for t in json.load(sys.stdin)['results']]" \
  | sort -V
```
Expected: a list of tags with dates. Pick the highest `4.x.y` tag whose date is before `2026-03-01`. Call it `<TAG>`.

- [ ] **Step 2: Write `docker-compose.yml` and `.env.example`**

```yaml
services:
  localstack:
    image: localstack/localstack:${LOCALSTACK_TAG}
    ports:
      - "4566:4566"
    environment:
      - SERVICES=s3,sqs,events,firehose,dynamodb,lambda,iam,sts,logs
      - LOCALSTACK_AUTH_TOKEN=${LOCALSTACK_AUTH_TOKEN:-}
      - PERSISTENCE=0
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

`.env.example`:
```
LOCALSTACK_TAG=<TAG from Step 1>
LOCALSTACK_AUTH_TOKEN=
```
Copy it to `.env`, which is gitignored, and fill in `LOCALSTACK_TAG`.

- [ ] **Step 3: Start LocalStack and check its health**

Run: `docker compose up -d && sleep 20 && curl -s localhost:4566/_localstack/health`
Expected: JSON listing the services as `available` or `running`.

If the container exits, check `docker compose logs localstack`. If it mentions a licence or auth token, **stop and ask Harvey** whether to create a free LocalStack account token, then set `LOCALSTACK_AUTH_TOKEN` in `.env` and use the latest tag. Record the decision in the findings doc.

- [ ] **Step 4: Write `spikes/firehose_probe.py`**

```python
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
```

- [ ] **Step 5: Run the probe at 10,000 events**

Run: `.venv/bin/python spikes/firehose_probe.py --events 10000`

Expected: a JSON report. Success criteria:
- `distinct_in_s3 == expected_in_s3` (9,990);
- `poison_in_s3 == 0`;
- `missing_detail == 0`;
- `envelope_keys` includes `detail`, `detail-type` and `source`.

If `poison_in_s3 > 0`, LocalStack ignores `exists:false`. Record that; Plan 2 then filters poison only in staging and settle.

- [ ] **Step 6: Write the findings doc** `docs/spikes/2026-10-day1-localstack.md`. Fill each section with the values from Steps 1–5:

```markdown
# Day-1 LocalStack spike — findings

Date: <run date>

## Decision: image and auth
- Tag used: <TAG>; token needed: <yes/no>; decision and reason: <one line>

## EventBridge → Firehose → S3 (10,000 events, 10 poison)
- distinct_in_s3 / expected_in_s3: <a> / <b>
- objects written: <n> (≈ events per object: <events/objects>)
- poison reached S3: <count>  → `exists:false` honoured: <yes/no>
- envelope keys: <list>
- put_seconds: <s>; parse_seconds: <s>; total_seconds: <s>

## Projection to the default backfill (≈ 400k envelopes including Lambda events)
- objects ≈ <objects × 40>; parse time ≈ <parse_seconds × 40> s
- Verdict on the 45-minute limit (spec §3.4): <keep default scale / drop LocalStack default to 30,000 check-ins>

## Notes for Plan 2
- <anything surprising, e.g. prefix format, buffering ignored, API differences>
```

- [ ] **Step 7: Stop LocalStack and commit**

```bash
docker compose down
make fmt && make test
git add docker-compose.yml .env.example spikes/firehose_probe.py docs/spikes/2026-10-day1-localstack.md
git commit -m "Day-1 spike: LocalStack Firehose delivery and scale findings

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 2: Rules core — local dates, nights, rule loading, rule choice

**Files:**
- Create: `shared/standdown_rules.csv`
- Create: `lambdas/standdown_checker/__init__.py` (empty)
- Create: `lambdas/standdown_checker/rules.py`
- Test: `tests/test_rules_core.py`

**Interfaces:**
- Produces, in `standdown_checker.rules`:
  - Constants: `NZ: ZoneInfo`, `WINDOW_NIGHTS = 14`, `ORIGINS_SEEN_BOUND_DAYS = 16`, `SCOPES`.
  - `Rule(rule_id: str, from_type: str, to_type: str, scope: str, nights: int)` (frozen dataclass).
  - `Visit(checkin_event_id, property_id, property_type, owner_company_id, checkin_time: datetime, checkout_time: datetime | None = None)`, with properties `standdown_end -> datetime` and `order_key -> tuple[datetime, str]`.
  - `local_date(ts: datetime) -> date`: raises `ValueError` on naive input.
  - `nights_between(end: datetime, checkin: datetime) -> int`.
  - `load_rules(path) -> list[Rule]`.
  - `pick_rule(rules, from_type, to_type, scope) -> Rule | None`.

- [ ] **Step 1: Write `shared/standdown_rules.csv`**

```csv
rule_id,from_type,to_type,scope,nights
R01,poultry,poultry,internal,1
R02,poultry,poultry,any,3
R03,pork,pork,internal,1
R04,pork,pork,any,3
R05,pork,poultry,any,2
R06,poultry,pork,any,2
R07,feed_mill,poultry,any,1
R08,beef,dairy,any,1
R09,dairy,dairy,external,2
R10,kiwifruit,kiwifruit,external,1
R11,vineyard,vineyard,external,1
```

- [ ] **Step 2: Write the failing tests** `tests/test_rules_core.py`

```python
from datetime import date, datetime
from pathlib import Path

import pytest

from standdown_checker.rules import Rule, load_rules, local_date, nights_between, pick_rule

RULES_CSV = Path(__file__).resolve().parents[1] / "shared" / "standdown_rules.csv"


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def test_local_date_uses_new_zealand_time():
    # 11:30 UTC on 2 March is 00:30 NZDT on 3 March.
    assert local_date(ts("2026-03-02T11:30:00+00:00")) == date(2026, 3, 3)


def test_local_date_rejects_naive_timestamps():
    with pytest.raises(ValueError):
        local_date(datetime(2026, 3, 3, 9, 0))


def test_nights_count_local_midnights_not_hours():
    # Two seconds apart, but a local midnight lies between them.
    assert nights_between(ts("2026-03-02T23:59:59+13:00"), ts("2026-03-03T00:00:01+13:00")) == 1


def test_nights_across_spring_forward_use_local_dates():
    # 46 elapsed hours across the 2026-09-27 clock change, yet two local midnights.
    assert nights_between(ts("2026-09-27T01:30:00+12:00"), ts("2026-09-29T00:30:00+13:00")) == 2


def test_nights_are_clipped_at_zero():
    assert nights_between(ts("2026-03-04T10:00:00+13:00"), ts("2026-03-03T20:00:00+13:00")) == 0


def test_load_rules_reads_the_shared_csv():
    rules = load_rules(RULES_CSV)
    assert {"R01", "R02", "R05"} <= {r.rule_id for r in rules}
    assert all(1 <= r.nights <= 14 for r in rules)


def test_load_rules_rejects_duplicate_keys(tmp_path):
    path = tmp_path / "rules.csv"
    path.write_text(
        "rule_id,from_type,to_type,scope,nights\n"
        "A,poultry,poultry,any,3\n"
        "B,poultry,poultry,any,2\n"
    )
    with pytest.raises(ValueError, match="duplicate"):
        load_rules(path)


def test_load_rules_rejects_nights_out_of_range(tmp_path):
    path = tmp_path / "rules.csv"
    path.write_text("rule_id,from_type,to_type,scope,nights\nA,poultry,poultry,any,15\n")
    with pytest.raises(ValueError, match="nights"):
        load_rules(path)


def test_pick_rule_prefers_the_specific_scope_over_any():
    rules = [
        Rule("ANY", "poultry", "poultry", "any", 3),
        Rule("INT", "poultry", "poultry", "internal", 1),
    ]
    assert pick_rule(rules, "poultry", "poultry", "internal").rule_id == "INT"
    assert pick_rule(rules, "poultry", "poultry", "external").rule_id == "ANY"
    assert pick_rule(rules, "office", "poultry", "external") is None
```

- [ ] **Step 3: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_rules_core.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'standdown_checker'`.

- [ ] **Step 4: Implement.** Create an empty `lambdas/standdown_checker/__init__.py` and `lambdas/standdown_checker/rules.py`:

```python
"""Stand-down rules (spec §4). Pure functions; stdlib only, so the Lambda zip stays small."""

from __future__ import annotations

import csv
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

NZ = ZoneInfo("Pacific/Auckland")
WINDOW_NIGHTS = 14
ORIGINS_SEEN_BOUND_DAYS = 16
SCOPES = ("internal", "external", "any")


@dataclass(frozen=True)
class Rule:
    rule_id: str
    from_type: str
    to_type: str
    scope: str
    nights: int


@dataclass(frozen=True)
class Visit:
    checkin_event_id: str
    property_id: str
    property_type: str
    owner_company_id: str
    checkin_time: datetime
    checkout_time: datetime | None = None

    @property
    def standdown_end(self) -> datetime:
        """§4.1: the check-out if known (never before the check-in), else the check-in."""
        if self.checkout_time is None:
            return self.checkin_time
        return max(self.checkin_time, self.checkout_time)

    @property
    def order_key(self) -> tuple[datetime, str]:
        return (self.checkin_time, self.checkin_event_id)


def local_date(ts: datetime) -> date:
    if ts.tzinfo is None:
        raise ValueError(f"timestamp has no UTC offset: {ts!r}")
    return ts.astimezone(NZ).date()


def nights_between(end: datetime, checkin: datetime) -> int:
    """§4.2: local midnights between the end of one visit and the next check-in."""
    return max(0, (local_date(checkin) - local_date(end)).days)


def load_rules(path: str | Path) -> list[Rule]:
    rules: list[Rule] = []
    seen: set[tuple[str, str, str]] = set()
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            rule = Rule(
                rule_id=row["rule_id"],
                from_type=row["from_type"],
                to_type=row["to_type"],
                scope=row["scope"],
                nights=int(row["nights"]),
            )
            if rule.scope not in SCOPES:
                raise ValueError(f"{rule.rule_id}: unknown scope {rule.scope!r}")
            if not 1 <= rule.nights <= WINDOW_NIGHTS:
                raise ValueError(f"{rule.rule_id}: nights must be 1..{WINDOW_NIGHTS}")
            key = (rule.from_type, rule.to_type, rule.scope)
            if key in seen:
                raise ValueError(f"duplicate rule for {key}")
            seen.add(key)
            rules.append(rule)
    return rules


def pick_rule(rules: Iterable[Rule], from_type: str, to_type: str, scope: str) -> Rule | None:
    """§4.5: the matching internal/external rule beats `any`; None if nothing matches."""
    fallback = None
    for rule in rules:
        if rule.from_type != from_type or rule.to_type != to_type:
            continue
        if rule.scope == scope:
            return rule
        if rule.scope == "any":
            fallback = rule
    return fallback
```

- [ ] **Step 5: Run the tests to confirm they pass**

Run: `.venv/bin/pytest tests/test_rules_core.py -q`
Expected: 9 passed.

- [ ] **Step 6: Commit**

```bash
make fmt && make test
git add shared/standdown_rules.csv lambdas/standdown_checker tests/test_rules_core.py
git commit -m "Rules core: local dates, nights, rule loading and choice

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 3: `evaluate` with golden cases

**Files:**
- Create: `shared/golden_cases.csv`
- Modify: `lambdas/standdown_checker/rules.py` (append)
- Test: `tests/test_rules_evaluate.py`

**Interfaces:**
- Consumes: Task 2's `Rule`, `Visit`, `nights_between`, `local_date` and `pick_rule`.
- Produces, in `standdown_checker.rules`:
  - `OriginSeen(checkin_event_id: str, end: datetime, end_source: str, nights_between: int)`, where `end_source` is `"checkout"` or `"checkin"`.
  - `Evaluation(breach: bool, origins_seen: tuple[OriginSeen, ...], rule_id=None, origin_checkin_event_id=None, origin_property_id=None, required_nights=None, nights_between=None)`.
  - `scope_of(origin: Visit, target: Visit) -> str`.
  - `evaluate(target: Visit, history: Iterable[Visit], rules: Sequence[Rule]) -> Evaluation`.
- Golden CSV columns (dbt reuses them in Task 14): case_id, role, visit_id, property_id, property_type, owner_company_id, checkin_time, checkout_time, expected_breach, expected_rule_id, expected_origin_visit_id.

- [ ] **Step 1: Write `shared/golden_cases.csv`**

```csv
case_id,role,visit_id,property_id,property_type,owner_company_id,checkin_time,checkout_time,expected_breach,expected_rule_id,expected_origin_visit_id
G01_intervening_visit,origin,A,P1,poultry,C2,2026-03-03T09:00:00+13:00,2026-03-03T11:00:00+13:00,,,
G01_intervening_visit,origin,B,P2,office,C3,2026-03-04T08:00:00+13:00,2026-03-04T09:00:00+13:00,,,
G01_intervening_visit,target,C,P3,poultry,C1,2026-03-04T14:00:00+13:00,,true,R02,A
G02_most_specific_scope,origin,A,P1,poultry,C1,2026-03-03T09:00:00+13:00,2026-03-03T11:00:00+13:00,,,
G02_most_specific_scope,target,B,P2,poultry,C1,2026-03-05T09:00:00+13:00,,false,,
G03_same_property,origin,A,P1,poultry,C2,2026-03-03T09:00:00+13:00,2026-03-03T10:00:00+13:00,,,
G03_same_property,target,B,P1,poultry,C2,2026-03-03T15:00:00+13:00,,false,,
G04_pick_largest_nights,origin,A,P1,poultry,C2,2026-03-01T10:00:00+13:00,2026-03-01T12:00:00+13:00,,,
G04_pick_largest_nights,origin,B,P2,pork,C3,2026-03-03T08:00:00+13:00,2026-03-03T09:00:00+13:00,,,
G04_pick_largest_nights,target,C,P3,poultry,C1,2026-03-03T15:00:00+13:00,,true,R02,A
G05_pick_shortfall,origin,A,P1,poultry,C2,2026-03-01T10:00:00+13:00,2026-03-01T11:00:00+13:00,,,
G05_pick_shortfall,origin,B,P2,poultry,C3,2026-03-02T10:00:00+13:00,2026-03-02T11:00:00+13:00,,,
G05_pick_shortfall,target,C,P3,poultry,C1,2026-03-03T10:00:00+13:00,,true,R02,B
G06_pick_latest_end,origin,A,P1,poultry,C2,2026-03-02T08:00:00+13:00,2026-03-02T09:00:00+13:00,,,
G06_pick_latest_end,origin,B,P2,poultry,C3,2026-03-02T10:00:00+13:00,2026-03-02T11:00:00+13:00,,,
G06_pick_latest_end,target,C,P3,poultry,C1,2026-03-03T09:00:00+13:00,,true,R02,B
G07_dst_local_dates,origin,A,P1,pork,C2,2026-09-27T00:30:00+12:00,2026-09-27T01:30:00+12:00,,,
G07_dst_local_dates,target,B,P2,poultry,C1,2026-09-29T00:30:00+13:00,,false,,
G08_midnight_boundary,origin,A,P1,poultry,C2,2026-03-02T22:00:00+13:00,2026-03-02T23:59:59+13:00,,,
G08_midnight_boundary,target,B,P2,poultry,C1,2026-03-03T00:00:01+13:00,,true,R02,A
G09_window_edge,origin,A,P1,poultry,C2,2026-03-01T10:00:00+13:00,2026-03-01T11:00:00+13:00,,,
G09_window_edge,target,B,P2,poultry,C1,2026-03-15T10:00:00+13:00,,false,,
G10_clip_at_zero,origin,A,P1,poultry,C2,2026-03-03T08:00:00+13:00,2026-03-04T10:00:00+13:00,,,
G10_clip_at_zero,target,B,P2,poultry,C1,2026-03-03T20:00:00+13:00,,true,R02,A
G11_no_rule,origin,A,P1,office,C2,2026-03-03T09:00:00+13:00,2026-03-03T10:00:00+13:00,,,
G11_no_rule,target,B,P2,poultry,C1,2026-03-03T15:00:00+13:00,,false,,
G12_no_checkout_uses_checkin,origin,A,P1,poultry,C2,2026-03-01T22:00:00+13:00,,,,
G12_no_checkout_uses_checkin,target,B,P2,poultry,C1,2026-03-03T09:00:00+13:00,,true,R02,A
G13_checkout_moves_end,origin,A,P1,pork,C2,2026-03-01T22:00:00+13:00,2026-03-02T06:00:00+13:00,,,
G13_checkout_moves_end,target,B,P2,poultry,C1,2026-03-03T09:00:00+13:00,,true,R05,A
```

Why each case expects what it does:

| Case | Reasoning |
|---|---|
| G01 | Worked example in §4 |
| G02 | The internal rule R01 (1 night) beats `any` R02 (3), so 2 nights is not a breach |
| G03 | Same-property origins are excluded |
| G04 | R02 (3 nights) beats R05 (2) |
| G05 | Same nights; the larger shortfall wins (B) |
| G06 | Same shortfall; the later end wins (B) |
| G07 | Two local dates across spring-forward, so 2 < 2 is false. Counting hours would give 1 and wrongly breach |
| G08 | Midnight gives 1 night |
| G09 | 14 nights is not a breach |
| G10 | An overlapping origin clips to 0 nights |
| G11 | No rule from office |
| G12 | No check-out, so the check-in is the end |
| G13 | The check-out moves the end later: 1 < 2 is a breach under R05 |

- [ ] **Step 2: Write the failing tests** `tests/test_rules_evaluate.py`

```python
import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pytest

from standdown_checker.rules import OriginSeen, Visit, evaluate, load_rules

ROOT = Path(__file__).resolve().parents[1]
RULES = load_rules(ROOT / "shared" / "standdown_rules.csv")


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def load_cases() -> dict[str, list[dict]]:
    cases: dict[str, list[dict]] = defaultdict(list)
    with open(ROOT / "shared" / "golden_cases.csv", newline="") as fh:
        for row in csv.DictReader(fh):
            cases[row["case_id"]].append(row)
    return dict(cases)


CASES = load_cases()


def to_visit(row: dict) -> Visit:
    return Visit(
        checkin_event_id=row["visit_id"],
        property_id=row["property_id"],
        property_type=row["property_type"],
        owner_company_id=row["owner_company_id"],
        checkin_time=ts(row["checkin_time"]),
        checkout_time=ts(row["checkout_time"]) if row["checkout_time"] else None,
    )


@pytest.mark.parametrize("case_id", sorted(CASES))
def test_golden_case(case_id):
    rows = CASES[case_id]
    target_row = next(r for r in rows if r["role"] == "target")
    history = [to_visit(r) for r in rows if r["role"] == "origin"]
    result = evaluate(to_visit(target_row), history, RULES)
    assert result.breach == (target_row["expected_breach"] == "true")
    assert (result.rule_id or "") == target_row["expected_rule_id"]
    assert (result.origin_checkin_event_id or "") == target_row["expected_origin_visit_id"]


def v(cid, pid, ptype, owner, checkin, checkout=None) -> Visit:
    return Visit(cid, pid, ptype, owner, ts(checkin), ts(checkout) if checkout else None)


def test_origins_seen_bound_is_sixteen_local_days():
    target = v("T", "P9", "poultry", "C1", "2026-03-20T09:00:00+13:00")
    inside = v("A", "P1", "office", "C2", "2026-03-04T09:00:00+13:00")
    outside = v("B", "P2", "office", "C2", "2026-03-03T09:00:00+13:00")
    result = evaluate(target, [inside, outside], RULES)
    assert [o.checkin_event_id for o in result.origins_seen] == ["A"]


def test_origins_seen_records_end_source_and_nights():
    target = v("T", "P9", "poultry", "C1", "2026-03-03T09:00:00+13:00")
    with_checkout = v(
        "A", "P1", "poultry", "C2", "2026-03-01T22:00:00+13:00", "2026-03-02T06:00:00+13:00"
    )
    without_checkout = v("B", "P2", "office", "C2", "2026-03-02T10:00:00+13:00")
    result = evaluate(target, [without_checkout, with_checkout], RULES)
    assert result.origins_seen == (
        OriginSeen("A", ts("2026-03-02T06:00:00+13:00"), "checkout", 1),
        OriginSeen("B", ts("2026-03-02T10:00:00+13:00"), "checkin", 1),
    )


def test_equal_checkin_times_are_ordered_by_event_id():
    target = v("b", "P9", "poultry", "C1", "2026-03-03T09:00:00+13:00")
    earlier_id = v("a", "P1", "poultry", "C2", "2026-03-03T09:00:00+13:00")
    later_id = v("z", "P2", "poultry", "C2", "2026-03-03T09:00:00+13:00")
    result = evaluate(target, [earlier_id, later_id], RULES)
    assert [o.checkin_event_id for o in result.origins_seen] == ["a"]
    assert result.origin_checkin_event_id == "a"


def test_target_itself_in_history_is_ignored():
    target = v("T", "P9", "poultry", "C1", "2026-03-03T09:00:00+13:00")
    result = evaluate(target, [target], RULES)
    assert result.origins_seen == ()
    assert result.breach is False
```

- [ ] **Step 3: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_rules_evaluate.py -q`
Expected: FAIL with `ImportError: cannot import name 'OriginSeen'`.

- [ ] **Step 4: Implement.** Append to `lambdas/standdown_checker/rules.py`, and add `Sequence` to the `collections.abc` import line (`from collections.abc import Iterable, Sequence`):

```python
@dataclass(frozen=True)
class OriginSeen:
    checkin_event_id: str
    end: datetime
    end_source: str
    nights_between: int


@dataclass(frozen=True)
class Evaluation:
    breach: bool
    origins_seen: tuple[OriginSeen, ...]
    rule_id: str | None = None
    origin_checkin_event_id: str | None = None
    origin_property_id: str | None = None
    required_nights: int | None = None
    nights_between: int | None = None


def scope_of(origin: Visit, target: Visit) -> str:
    """§4.4: internal when both properties have the same owner; the visitor plays no part."""
    return "internal" if origin.owner_company_id == target.owner_company_id else "external"


def evaluate(target: Visit, history: Iterable[Visit], rules: Sequence[Rule]) -> Evaluation:
    """§4: one result per target check-in; also records every origin inside the 16-day bound."""
    candidates = sorted(
        (
            v
            for v in history
            if v.checkin_event_id != target.checkin_event_id
            and v.order_key < target.order_key
            and v.property_id != target.property_id
        ),
        key=lambda v: v.order_key,
    )
    target_date = local_date(target.checkin_time)
    seen: list[OriginSeen] = []
    best_key = None
    best = None
    for origin in candidates:
        nights = nights_between(origin.standdown_end, target.checkin_time)
        if (target_date - local_date(origin.checkin_time)).days <= ORIGINS_SEEN_BOUND_DAYS:
            seen.append(
                OriginSeen(
                    checkin_event_id=origin.checkin_event_id,
                    end=origin.standdown_end,
                    end_source="checkout" if origin.checkout_time is not None else "checkin",
                    nights_between=nights,
                )
            )
        if nights > WINDOW_NIGHTS:
            continue
        rule = pick_rule(rules, origin.property_type, target.property_type, scope_of(origin, target))
        if rule is None or nights >= rule.nights:
            continue
        key = (rule.nights, rule.nights - nights, origin.standdown_end, origin.checkin_event_id)
        if best_key is None or key > best_key:
            best_key, best = key, (origin, rule, nights)
    if best is None:
        return Evaluation(breach=False, origins_seen=tuple(seen))
    origin, rule, nights = best
    return Evaluation(
        breach=True,
        origins_seen=tuple(seen),
        rule_id=rule.rule_id,
        origin_checkin_event_id=origin.checkin_event_id,
        origin_property_id=origin.property_id,
        required_nights=rule.nights,
        nights_between=nights,
    )
```

- [ ] **Step 5: Run the tests to confirm they pass**

Run: `.venv/bin/pytest tests/test_rules_evaluate.py -q`
Expected: 17 passed (13 golden cases + 4).

- [ ] **Step 6: Commit**

```bash
make fmt && make test
git add shared/golden_cases.csv lambdas/standdown_checker/rules.py tests/test_rules_evaluate.py
git commit -m "Rules: evaluate() with shared golden cases

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 4: Event parsing, derived ids, `iso()`

**Files:**
- Create: `lambdas/standdown_checker/events.py`
- Test: `tests/test_events.py`

**Interfaces:**
- Produces, in `standdown_checker.events`:
  - Constants: `NZ`, `APP_SOURCE = "onside.app"`, `STANDDOWN_SOURCE = "onside.standdown"`, `CHECKIN_METHODS`, `CHECKOUT_METHODS`.
  - `class ValidationError(ValueError)`.
  - `derived_id(kind: str, key: str) -> str`.
  - `iso(ts: datetime, *, ms: bool = False) -> str`.
  - `parse_ts(value, field) -> datetime`.
  - `CheckIn(event_id, person_id, visitor_company_id, property_id, event_time, received_time, method, job_type, additional_visitors)`.
  - `CheckOut(event_id, checkin_event_id, person_id, property_id, event_time, received_time, method)`.
  - `parse_event(detail_type: str, detail: object) -> CheckIn | CheckOut`.

- [ ] **Step 1: Write the failing tests** `tests/test_events.py`

```python
from datetime import UTC, datetime

import pytest

from standdown_checker.events import (
    CheckIn,
    CheckOut,
    ValidationError,
    derived_id,
    iso,
    parse_event,
)


def checkin_detail(**overrides):
    detail = {
        "event_id": "e1",
        "person_id": "U1",
        "visitor_company_id": "V1",
        "property_id": "P1",
        "event_time": "2026-03-03T09:00:00+13:00",
        "received_time": "2026-03-03T09:00:05+13:00",
        "method": "app",
        "job_type": "delivery",
        "additional_visitors": 0,
        "answers": [],
    }
    detail.update(overrides)
    return detail


def test_parses_a_valid_checkin():
    event = parse_event("CheckIn", checkin_detail())
    assert isinstance(event, CheckIn)
    assert event.event_time.utcoffset().total_seconds() == 13 * 3600


def test_parses_a_valid_checkout():
    detail = {
        "event_id": "o1",
        "checkin_event_id": "e1",
        "person_id": "U1",
        "property_id": "P1",
        "event_time": "2026-03-03T10:00:00+13:00",
        "received_time": "2026-03-03T10:00:03+13:00",
        "method": "manual",
    }
    assert isinstance(parse_event("CheckOut", detail), CheckOut)


@pytest.mark.parametrize(
    "overrides",
    [
        {"person_id": ""},
        {"event_time": "2026-03-03T09:00:00"},  # no UTC offset
        {"event_time": "not a time"},
        {"method": "carrier_pigeon"},
        {"additional_visitors": True},
        {"additional_visitors": -1},
        {"poison": True},
    ],
)
def test_rejects_invalid_checkins(overrides):
    with pytest.raises(ValidationError):
        parse_event("CheckIn", checkin_detail(**overrides))


def test_rejects_unknown_detail_type_and_non_objects():
    with pytest.raises(ValidationError):
        parse_event("PlantMovement", checkin_detail())
    with pytest.raises(ValidationError):
        parse_event("CheckIn", ["not", "an", "object"])


def test_derived_ids_are_deterministic_and_distinct_per_kind():
    assert derived_id("eval", "e1") == derived_id("eval", "e1")
    assert derived_id("eval", "e1") != derived_id("alert", "e1")


def test_iso_writes_new_zealand_offset():
    assert iso(datetime(2026, 3, 2, 20, 0, tzinfo=UTC)) == "2026-03-03T09:00:00+13:00"
    assert iso(datetime(2026, 3, 2, 20, 0, tzinfo=UTC), ms=True) == "2026-03-03T09:00:00.000+13:00"
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_events.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'standdown_checker.events'`.

- [ ] **Step 3: Implement** `lambdas/standdown_checker/events.py`

```python
"""Event parsing and ids shared by the Lambda and the simulator (spec §3.5)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

NZ = ZoneInfo("Pacific/Auckland")
APP_SOURCE = "onside.app"
STANDDOWN_SOURCE = "onside.standdown"
CHECKIN_METHODS = frozenset({"app", "geofence", "qr", "kiosk", "link"})
CHECKOUT_METHODS = frozenset({"manual", "geofence_auto", "end_of_day"})
_NAMESPACE = uuid.UUID("6f1c2a52-3b8e-4f0a-9a52-0d1e7c4b9a10")


class ValidationError(ValueError):
    """The event can never be processed; after retries it belongs in the DLQ."""


def derived_id(kind: str, key: str) -> str:
    """Deterministic id for events the stand-down service emits (eval, co, alert)."""
    return str(uuid.uuid5(_NAMESPACE, f"{kind}:{key}"))


def iso(ts: datetime, *, ms: bool = False) -> str:
    return ts.astimezone(NZ).isoformat(timespec="milliseconds" if ms else "seconds")


def parse_ts(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValidationError(f"{field}: expected an ISO 8601 string")
    try:
        ts = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValidationError(f"{field}: {exc}") from exc
    if ts.tzinfo is None:
        raise ValidationError(f"{field}: missing UTC offset")
    return ts


@dataclass(frozen=True)
class CheckIn:
    event_id: str
    person_id: str
    visitor_company_id: str
    property_id: str
    event_time: datetime
    received_time: datetime
    method: str
    job_type: str
    additional_visitors: int


@dataclass(frozen=True)
class CheckOut:
    event_id: str
    checkin_event_id: str
    person_id: str
    property_id: str
    event_time: datetime
    received_time: datetime
    method: str


def _text(detail: dict, field: str) -> str:
    value = detail.get(field)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{field}: required non-empty string")
    return value


def parse_event(detail_type: str, detail: object) -> CheckIn | CheckOut:
    if not isinstance(detail, dict):
        raise ValidationError("detail must be a JSON object")
    if "poison" in detail:
        raise ValidationError("poison event")
    if detail_type == "CheckIn":
        method = _text(detail, "method")
        if method not in CHECKIN_METHODS:
            raise ValidationError(f"method: unknown {method!r}")
        visitors = detail.get("additional_visitors", 0)
        if not isinstance(visitors, int) or isinstance(visitors, bool) or visitors < 0:
            raise ValidationError("additional_visitors: expected a non-negative integer")
        return CheckIn(
            event_id=_text(detail, "event_id"),
            person_id=_text(detail, "person_id"),
            visitor_company_id=_text(detail, "visitor_company_id"),
            property_id=_text(detail, "property_id"),
            event_time=parse_ts(detail.get("event_time"), "event_time"),
            received_time=parse_ts(detail.get("received_time"), "received_time"),
            method=method,
            job_type=_text(detail, "job_type"),
            additional_visitors=visitors,
        )
    if detail_type == "CheckOut":
        method = _text(detail, "method")
        if method not in CHECKOUT_METHODS:
            raise ValidationError(f"method: unknown {method!r}")
        return CheckOut(
            event_id=_text(detail, "event_id"),
            checkin_event_id=_text(detail, "checkin_event_id"),
            person_id=_text(detail, "person_id"),
            property_id=_text(detail, "property_id"),
            event_time=parse_ts(detail.get("event_time"), "event_time"),
            received_time=parse_ts(detail.get("received_time"), "received_time"),
            method=method,
        )
    raise ValidationError(f"unsupported detail-type {detail_type!r}")
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `.venv/bin/pytest tests/test_events.py -q`
Expected: 12 passed.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add lambdas/standdown_checker/events.py tests/test_events.py
git commit -m "Events: validation, derived ids, NZ iso formatting

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 5: `StateStore` protocol and the in-memory store

**Files:**
- Create: `lambdas/standdown_checker/store.py`
- Create: `generator/__init__.py` (empty)
- Create: `generator/local_sim.py`
- Test: `tests/test_memory_store.py`

**Interfaces:**
- Consumes: `standdown_checker.events.ValidationError`.
- Produces, in `standdown_checker.store`:
  - Exceptions: `ConflictError`, `EvaluationExistsError`, `UnknownPropertyError(ValidationError)`.
  - `StateStore`, a Protocol with:
    - `load_person(person_id) -> tuple[int, list[dict]]`: returns `(0, [])` when the person has no item yet.
    - `get_evaluation(checkin_event_id) -> dict | None`.
    - `get_property(property_id) -> dict`: returns keys `property_type` and `owner_company_id`.
    - `commit(person_id, version_read, visits, evaluation: dict | None) -> None`.
- Produces, in `generator.local_sim`:
  - `InMemoryStore(properties: dict[str, dict])`, implementing `StateStore`.
  - Persistence: `.save(path)` and `InMemoryStore.load(path, properties)`. `load` returns an empty store if the file is missing.

- [ ] **Step 1: Write the failing tests** `tests/test_memory_store.py`

```python
import pytest

from generator.local_sim import InMemoryStore
from standdown_checker.store import ConflictError, EvaluationExistsError, UnknownPropertyError

PROPS = {"P1": {"property_type": "poultry", "owner_company_id": "C1"}}
VISIT = {"checkin_event_id": "A", "property_id": "P1"}


def test_missing_person_reads_as_version_zero():
    assert InMemoryStore(PROPS).load_person("U1") == (0, [])


def test_commit_creates_version_one_and_stores_evaluation():
    store = InMemoryStore(PROPS)
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A", "breach": False})
    assert store.load_person("U1") == (1, [VISIT])
    assert store.get_evaluation("A") == {"checkin_event_id": "A", "breach": False}


def test_stale_version_conflicts():
    store = InMemoryStore(PROPS)
    store.commit("U1", 0, [VISIT], None)
    with pytest.raises(ConflictError):
        store.commit("U1", 0, [], None)


def test_existing_evaluation_cancels_the_whole_commit():
    store = InMemoryStore(PROPS)
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A"})
    with pytest.raises(EvaluationExistsError):
        store.commit("U1", 1, [], {"checkin_event_id": "A"})
    assert store.load_person("U1") == (1, [VISIT])


def test_returned_visits_are_copies():
    store = InMemoryStore(PROPS)
    store.commit("U1", 0, [dict(VISIT)], None)
    _, visits = store.load_person("U1")
    visits[0]["property_id"] = "changed"
    assert store.load_person("U1")[1][0]["property_id"] == "P1"


def test_unknown_property_raises_validation_error():
    with pytest.raises(UnknownPropertyError):
        InMemoryStore(PROPS).get_property("NOPE")


def test_save_and_load_round_trip(tmp_path):
    path = tmp_path / "sim_store.json"
    store = InMemoryStore(PROPS)
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A"})
    store.save(path)
    loaded = InMemoryStore.load(path, PROPS)
    assert loaded.load_person("U1") == (1, [VISIT])
    assert loaded.get_evaluation("A") == {"checkin_event_id": "A"}
    assert InMemoryStore.load(tmp_path / "missing.json", PROPS).load_person("U1") == (0, [])
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_memory_store.py -q`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement** `lambdas/standdown_checker/store.py`

```python
"""State store interface used by the processor (spec §6.1)."""

from __future__ import annotations

from typing import Protocol

from .events import ValidationError


class ConflictError(Exception):
    """person_state changed since it was read; reload and retry."""


class EvaluationExistsError(Exception):
    """Another worker already committed the evaluation for this check-in."""


class UnknownPropertyError(ValidationError):
    """The check-in names a property that is not in the reference snapshot."""


class StateStore(Protocol):
    def load_person(self, person_id: str) -> tuple[int, list[dict]]: ...

    def get_evaluation(self, checkin_event_id: str) -> dict | None: ...

    def get_property(self, property_id: str) -> dict: ...

    def commit(
        self,
        person_id: str,
        version_read: int,
        visits: list[dict],
        evaluation: dict | None,
    ) -> None: ...
```

Create an empty `generator/__init__.py`, then `generator/local_sim.py`:

```python
"""In-memory StateStore and the ENV=sim runner (spec §3.3 Local output)."""

from __future__ import annotations

import copy
import json
from pathlib import Path

from standdown_checker.store import ConflictError, EvaluationExistsError, UnknownPropertyError


class InMemoryStore:
    """Implements the same conditions as the DynamoDB transaction in spec §6.1."""

    def __init__(self, properties: dict[str, dict]) -> None:
        self.properties = properties
        self.persons: dict[str, dict] = {}
        self.evaluations: dict[str, dict] = {}

    def load_person(self, person_id: str) -> tuple[int, list[dict]]:
        item = self.persons.get(person_id)
        if item is None:
            return 0, []
        return item["version"], copy.deepcopy(item["visits"])

    def get_evaluation(self, checkin_event_id: str) -> dict | None:
        found = self.evaluations.get(checkin_event_id)
        return copy.deepcopy(found) if found is not None else None

    def get_property(self, property_id: str) -> dict:
        try:
            return self.properties[property_id]
        except KeyError:
            raise UnknownPropertyError(f"unknown property {property_id!r}") from None

    def commit(
        self,
        person_id: str,
        version_read: int,
        visits: list[dict],
        evaluation: dict | None,
    ) -> None:
        if evaluation is not None and evaluation["checkin_event_id"] in self.evaluations:
            raise EvaluationExistsError(evaluation["checkin_event_id"])
        current = self.persons.get(person_id, {"version": 0})["version"]
        if current != version_read:
            raise ConflictError(f"{person_id}: read {version_read}, now {current}")
        self.persons[person_id] = {"version": version_read + 1, "visits": copy.deepcopy(visits)}
        if evaluation is not None:
            self.evaluations[evaluation["checkin_event_id"]] = copy.deepcopy(evaluation)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"persons": self.persons, "evaluations": self.evaluations}))
        tmp.replace(path)

    @classmethod
    def load(cls, path: Path, properties: dict[str, dict]) -> InMemoryStore:
        store = cls(properties)
        if path.exists():
            data = json.loads(path.read_text())
            store.persons = data["persons"]
            store.evaluations = data["evaluations"]
        return store
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `.venv/bin/pytest tests/test_memory_store.py -q`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add lambdas/standdown_checker/store.py generator/__init__.py generator/local_sim.py tests/test_memory_store.py
git commit -m "StateStore protocol and persisted in-memory store

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 6: Processor — CheckIn path

**Files:**
- Create: `lambdas/standdown_checker/processor.py`
- Create: `tests/helpers.py`
- Test: `tests/test_processor_checkin.py`

**Interfaces:**
- Consumes:
  - `rules.evaluate`, `rules.load_rules`, `rules.Visit`;
  - `events.parse_event`, `events.iso`, `events.derived_id`, `events.STANDDOWN_SOURCE`;
  - the `store` exceptions;
  - `generator.local_sim.InMemoryStore` (tests only).
- Produces, in `standdown_checker.processor`:
  - `Outbound(detail_type: str, detail: dict, source: str = "onside.standdown")` (frozen).
  - `class RetryExhaustedError(Exception)`.
  - `MAX_CONFLICT_RETRIES = 3`.
  - `process_event(detail_type, detail, store, rules, now: Callable[[], datetime], *, sleep=time.sleep, jitter=random.random) -> list[Outbound]`.
- **Visit item schema** (stored in `person_state.visits[]`; Plan 2's DynamoDB store uses the same):
  ```
  {"checkin_event_id", "property_id", "property_type", "owner_company_id",
   "checkin_time": iso | None, "checkout_event_id": str | None,
   "checkout_time": iso | None,
   "applied_checkouts": {checkout_event_id: {"version": int, "processed_at": iso_ms}}}
  ```
  `applied_checkouts` covers the spec's `checkout_applied_version` and `checkout_processed_at` for each check-out.
- **CheckInEvaluated detail keys:** event_id, checkin_event_id, person_id, version_read, version_committed, detected_time, processed_at, origins_seen[{checkin_event_id, end, end_source, nights_between}], breach, rule_id, origin_checkin_event_id, required_nights, nights_between.

- [ ] **Step 1: Write the test helpers** `tests/helpers.py`

```python
"""Shared builders for processor tests."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from standdown_checker.processor import process_event
from standdown_checker.rules import load_rules

ROOT = Path(__file__).resolve().parents[1]
RULES = load_rules(ROOT / "shared" / "standdown_rules.csv")
PROPERTIES = {
    "P1": {"property_type": "poultry", "owner_company_id": "C2"},
    "P2": {"property_type": "office", "owner_company_id": "C3"},
    "P3": {"property_type": "poultry", "owner_company_id": "C1"},
    "P4": {"property_type": "poultry", "owner_company_id": "C1"},
    "P5": {"property_type": "pork", "owner_company_id": "C4"},
}


class TickingClock:
    """Returns a later time on every call, so re-used values are detectable."""

    def __init__(self) -> None:
        self.t = datetime(2026, 3, 10, tzinfo=UTC)

    def __call__(self) -> datetime:
        self.t += timedelta(seconds=1)
        return self.t


def checkin(event_id, prop, event_time, *, person="U1", received=None) -> dict:
    return {
        "event_id": event_id,
        "person_id": person,
        "visitor_company_id": "V1",
        "property_id": prop,
        "event_time": event_time,
        "received_time": received or event_time,
        "method": "app",
        "job_type": "delivery",
        "additional_visitors": 0,
        "answers": [],
    }


def checkout(event_id, checkin_event_id, prop, event_time, *, person="U1", received=None) -> dict:
    return {
        "event_id": event_id,
        "checkin_event_id": checkin_event_id,
        "person_id": person,
        "property_id": prop,
        "event_time": event_time,
        "received_time": received or event_time,
        "method": "manual",
    }


def run(store, detail_type, detail, *, clock=None, sleep=None):
    return process_event(
        detail_type,
        detail,
        store,
        RULES,
        clock or TickingClock(),
        sleep=sleep or (lambda seconds: None),
    )


def types(outbound) -> list[str]:
    return [o.detail_type for o in outbound]
```

- [ ] **Step 2: Write the failing tests** `tests/test_processor_checkin.py`

```python
import pytest

from generator.local_sim import InMemoryStore
from standdown_checker.events import ValidationError, derived_id
from tests.helpers import PROPERTIES, TickingClock, checkin, checkout, run, types


def new_store() -> InMemoryStore:
    return InMemoryStore(PROPERTIES)


def test_first_checkin_commits_version_one_without_breach():
    store = new_store()
    out = run(store, "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"))
    assert types(out) == ["CheckInEvaluated"]
    evaluation = out[0].detail
    assert evaluation["event_id"] == derived_id("eval", "A")
    assert (evaluation["version_read"], evaluation["version_committed"]) == (0, 1)
    assert evaluation["breach"] is False
    assert evaluation["detected_time"] == "2026-03-03T09:00:00+13:00"
    assert store.load_person("U1")[0] == 1


def test_worked_example_emits_alert_with_property_ids():
    store = new_store()
    run(store, "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"))
    run(store, "CheckOut", checkout("A-out", "A", "P1", "2026-03-03T11:00:00+13:00"))
    run(store, "CheckIn", checkin("B", "P2", "2026-03-04T08:00:00+13:00"))
    out = run(store, "CheckIn", checkin("C", "P3", "2026-03-04T14:00:00+13:00"))
    assert types(out) == ["CheckInEvaluated", "HighRiskVisitDetected"]
    evaluation, alert = out[0].detail, out[1].detail
    assert evaluation["rule_id"] == "R02"
    assert evaluation["origin_checkin_event_id"] == "A"
    assert (evaluation["nights_between"], evaluation["required_nights"]) == (1, 3)
    assert evaluation["version_read"] == 3
    assert alert["event_id"] == derived_id("alert", "C")
    assert (alert["origin_property_id"], alert["dest_property_id"]) == ("P1", "P3")
    assert alert["processed_at"] == evaluation["processed_at"]
    seen = {o["checkin_event_id"]: o for o in evaluation["origins_seen"]}
    assert seen["A"]["end_source"] == "checkout"
    assert seen["B"]["end_source"] == "checkin"  # B never checked out


def test_late_origin_is_inserted_in_order_and_does_not_reevaluate_later_checkins():
    store = new_store()
    out_c = run(store, "CheckIn", checkin("C", "P3", "2026-03-04T14:00:00+13:00"))
    assert out_c[0].detail["breach"] is False
    late = checkin("A", "P1", "2026-03-03T09:00:00+13:00", received="2026-03-05T09:00:00+13:00")
    out_a = run(store, "CheckIn", late)
    assert out_a[0].detail["breach"] is False
    _, visits = store.load_person("U1")
    assert [v["checkin_event_id"] for v in visits] == ["A", "C"]
    assert store.get_evaluation("C")["breach"] is False  # the miss batch will report


def test_duplicate_checkin_reemits_identical_bodies_without_committing():
    store = new_store()
    clock = TickingClock()
    run(store, "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"), clock=clock)
    first = run(store, "CheckIn", checkin("C", "P3", "2026-03-03T14:00:00+13:00"), clock=clock)
    version = store.load_person("U1")[0]
    second = run(store, "CheckIn", checkin("C", "P3", "2026-03-03T14:00:00+13:00"), clock=clock)
    assert second == first
    assert store.load_person("U1")[0] == version


def test_unknown_property_is_a_validation_error():
    with pytest.raises(ValidationError):
        run(new_store(), "CheckIn", checkin("X", "NOPE", "2026-03-03T09:00:00+13:00"))
```

- [ ] **Step 3: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_processor_checkin.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'standdown_checker.processor'`.

- [ ] **Step 4: Implement** `lambdas/standdown_checker/processor.py`. It includes the CheckOut path that Task 7 tests, so both tasks share one module:

```python
"""Stand-down processing over a StateStore (spec §6.1). Order-independent by design."""

from __future__ import annotations

import copy
import random
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime

from . import rules as R
from .events import STANDDOWN_SOURCE, CheckIn, CheckOut, derived_id, iso, parse_event
from .store import ConflictError, EvaluationExistsError, StateStore

MAX_CONFLICT_RETRIES = 3


@dataclass(frozen=True)
class Outbound:
    detail_type: str
    detail: dict
    source: str = STANDDOWN_SOURCE


class RetryExhaustedError(Exception):
    """Version conflicts kept happening; fail the record so SQS redelivers it."""


def process_event(
    detail_type: str,
    detail: object,
    store: StateStore,
    rules: Sequence[R.Rule],
    now: Callable[[], datetime],
    *,
    sleep: Callable[[float], None] = time.sleep,
    jitter: Callable[[], float] = random.random,
) -> list[Outbound]:
    event = parse_event(detail_type, detail)
    conflicts = 0
    # Each pass restarts from step 2 (load), including the duplicate check.
    for _ in range(MAX_CONFLICT_RETRIES + 10):
        try:
            if isinstance(event, CheckIn):
                return _process_checkin(event, store, rules, now)
            return _process_checkout(event, store, now)
        except EvaluationExistsError:
            continue  # another worker committed it; the next pass re-emits the stored result
        except ConflictError:
            conflicts += 1
            if conflicts > MAX_CONFLICT_RETRIES:
                raise RetryExhaustedError(f"{event.event_id}: {conflicts} conflicts") from None
            sleep(jitter() * 0.05 * 2**conflicts)
    raise RetryExhaustedError(f"{event.event_id}: did not converge")


def _parse(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


def _sort_key(item: dict) -> tuple[bool, float, str]:
    ts = _parse(item["checkin_time"])
    return (ts is None, ts.timestamp() if ts else 0.0, item["checkin_event_id"])


def _find(visits: list[dict], checkin_event_id: str | None) -> dict | None:
    return next((v for v in visits if v["checkin_event_id"] == checkin_event_id), None)


def _new_item(checkin_event_id: str, property_id: str) -> dict:
    return {
        "checkin_event_id": checkin_event_id,
        "property_id": property_id,
        "property_type": None,
        "owner_company_id": None,
        "checkin_time": None,
        "checkout_event_id": None,
        "checkout_time": None,
        "applied_checkouts": {},
    }


def _to_rule_visit(item: dict) -> R.Visit:
    return R.Visit(
        checkin_event_id=item["checkin_event_id"],
        property_id=item["property_id"],
        property_type=item["property_type"],
        owner_company_id=item["owner_company_id"],
        checkin_time=_parse(item["checkin_time"]),
        checkout_time=_parse(item["checkout_time"]),
    )


def _process_checkin(
    ci: CheckIn, store: StateStore, rules: Sequence[R.Rule], now: Callable[[], datetime]
) -> list[Outbound]:
    stored = store.get_evaluation(ci.event_id)
    if stored is not None:
        _, visits = store.load_person(ci.person_id)
        return _checkin_outbound(stored, visits)

    version_read, visits = store.load_person(ci.person_id)
    prop = store.get_property(ci.property_id)
    target = R.Visit(
        ci.event_id, ci.property_id, prop["property_type"], prop["owner_company_id"], ci.event_time
    )
    # Placeholders (no observed check-in) are never origins.
    history = [_to_rule_visit(v) for v in visits if v["checkin_time"] is not None]
    result = R.evaluate(target, history, rules)

    visits = copy.deepcopy(visits)
    item = _find(visits, ci.event_id)
    if item is None:
        item = _new_item(ci.event_id, ci.property_id)
        visits.append(item)
    item.update(
        property_id=ci.property_id,
        property_type=prop["property_type"],
        owner_company_id=prop["owner_company_id"],
        checkin_time=iso(ci.event_time),
    )
    visits.sort(key=_sort_key)

    evaluation = {
        "event_id": derived_id("eval", ci.event_id),
        "checkin_event_id": ci.event_id,
        "person_id": ci.person_id,
        "version_read": version_read,
        "version_committed": version_read + 1,
        "detected_time": iso(ci.received_time),
        "processed_at": iso(now(), ms=True),
        "origins_seen": [
            {
                "checkin_event_id": o.checkin_event_id,
                "end": iso(o.end),
                "end_source": o.end_source,
                "nights_between": o.nights_between,
            }
            for o in result.origins_seen
        ],
        "breach": result.breach,
        "rule_id": result.rule_id,
        "origin_checkin_event_id": result.origin_checkin_event_id,
        "required_nights": result.required_nights,
        "nights_between": result.nights_between,
    }
    store.commit(ci.person_id, version_read, visits, evaluation)
    return _checkin_outbound(evaluation, visits)


def _checkin_outbound(evaluation: dict, visits: list[dict]) -> list[Outbound]:
    out = [Outbound("CheckInEvaluated", copy.deepcopy(evaluation))]
    if evaluation["breach"]:
        origin = _find(visits, evaluation["origin_checkin_event_id"])
        dest = _find(visits, evaluation["checkin_event_id"])
        out.append(
            Outbound(
                "HighRiskVisitDetected",
                {
                    "event_id": derived_id("alert", evaluation["checkin_event_id"]),
                    "checkin_event_id": evaluation["checkin_event_id"],
                    "person_id": evaluation["person_id"],
                    "origin_property_id": origin["property_id"],
                    "dest_property_id": dest["property_id"],
                    "origin_checkin_event_id": evaluation["origin_checkin_event_id"],
                    "nights_between": evaluation["nights_between"],
                    "required_nights": evaluation["required_nights"],
                    "rule_id": evaluation["rule_id"],
                    "detected_time": evaluation["detected_time"],
                    "processed_at": evaluation["processed_at"],
                },
            )
        )
    return out


def _process_checkout(
    co: CheckOut, store: StateStore, now: Callable[[], datetime]
) -> list[Outbound]:
    version_read, visits = store.load_person(co.person_id)
    item = _find(visits, co.checkin_event_id)
    if item is not None and co.event_id in item["applied_checkouts"]:
        applied = item["applied_checkouts"][co.event_id]
        return [_applied(co, applied["version"], applied["processed_at"])]

    visits = copy.deepcopy(visits)
    item = _find(visits, co.checkin_event_id)
    if item is None:
        item = _new_item(co.checkin_event_id, co.property_id)  # placeholder until the check-in
        visits.append(item)
    if item["checkout_time"] is None or co.event_time < _parse(item["checkout_time"]):
        item["checkout_event_id"] = co.event_id  # keep the earliest check-out
        item["checkout_time"] = iso(co.event_time)
    version_committed = version_read + 1
    processed_at = iso(now(), ms=True)
    item["applied_checkouts"][co.event_id] = {
        "version": version_committed,
        "processed_at": processed_at,
    }
    visits.sort(key=_sort_key)
    store.commit(co.person_id, version_read, visits, None)
    return [_applied(co, version_committed, processed_at)]


def _applied(co: CheckOut, version: int, processed_at: str) -> Outbound:
    return Outbound(
        "CheckOutApplied",
        {
            "event_id": derived_id("co", co.event_id),
            "checkout_event_id": co.event_id,
            "checkin_event_id": co.checkin_event_id,
            "person_id": co.person_id,
            "version_committed": version,
            "processed_at": processed_at,
        },
    )
```

- [ ] **Step 5: Run the tests to confirm they pass**

Run: `.venv/bin/pytest tests/test_processor_checkin.py -q`
Expected: 5 passed.

- [ ] **Step 6: Commit**

```bash
make fmt && make test
git add lambdas/standdown_checker/processor.py tests/helpers.py tests/test_processor_checkin.py
git commit -m "Processor: check-in evaluation, commit, idempotent re-emit

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 7: Processor — CheckOut path, placeholders, retries

**Files:**
- Test: `tests/test_processor_checkout.py`
- Modify: `lambdas/standdown_checker/processor.py`, only if a test exposes a defect

**Interfaces:**
- Consumes: Task 6's `process_event`, `Outbound` and `RetryExhaustedError`, plus `tests/helpers.py`.
- Produces: no new names. This task pins §6.1 steps 4–5 and the retry rules.

- [ ] **Step 1: Write the tests** `tests/test_processor_checkout.py`

```python
import pytest

from generator.local_sim import InMemoryStore
from standdown_checker.events import derived_id
from standdown_checker.processor import RetryExhaustedError, process_event
from standdown_checker.store import ConflictError, EvaluationExistsError
from tests.helpers import PROPERTIES, RULES, TickingClock, checkin, checkout, run, types


def new_store() -> InMemoryStore:
    return InMemoryStore(PROPERTIES)


def test_orphan_checkout_creates_placeholder_then_checkin_is_evaluated():
    store = new_store()
    out_co = run(store, "CheckOut", checkout("A-out", "A", "P1", "2026-03-02T06:00:00+13:00"))
    assert types(out_co) == ["CheckOutApplied"]
    assert out_co[0].detail["event_id"] == derived_id("co", "A-out")
    assert out_co[0].detail["version_committed"] == 1
    out_ci = run(store, "CheckIn", checkin("A", "P1", "2026-03-01T22:00:00+13:00"))
    assert types(out_ci) == ["CheckInEvaluated"]  # not mistaken for a duplicate
    _, visits = store.load_person("U1")
    assert visits[0]["checkin_time"] == "2026-03-01T22:00:00+13:00"
    assert visits[0]["checkout_time"] == "2026-03-02T06:00:00+13:00"


def test_placeholder_is_never_an_origin():
    store = new_store()
    run(store, "CheckOut", checkout("X-out", "X", "P1", "2026-03-03T10:00:00+13:00"))
    out = run(store, "CheckIn", checkin("C", "P3", "2026-03-03T14:00:00+13:00"))
    assert out[0].detail["breach"] is False
    assert out[0].detail["origins_seen"] == []


def test_duplicate_checkout_before_checkin_reemits_same_applied_event():
    store = new_store()
    clock = TickingClock()
    co = checkout("A-out", "A", "P1", "2026-03-02T06:00:00+13:00")
    first = run(store, "CheckOut", co, clock=clock)
    assert run(store, "CheckOut", co, clock=clock) == first
    run(store, "CheckIn", checkin("A", "P1", "2026-03-01T22:00:00+13:00"), clock=clock)
    assert run(store, "CheckOut", co, clock=clock) == first


def test_earliest_checkout_is_kept_and_each_is_applied_once():
    store = new_store()
    run(store, "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"))
    late = run(store, "CheckOut", checkout("A-out2", "A", "P1", "2026-03-03T18:00:00+13:00"))
    early = run(store, "CheckOut", checkout("A-out1", "A", "P1", "2026-03-03T11:00:00+13:00"))
    _, visits = store.load_person("U1")
    assert visits[0]["checkout_event_id"] == "A-out1"
    assert late[0].detail["version_committed"] == 2
    assert early[0].detail["version_committed"] == 3


def test_checkout_end_feeds_the_standdown_rule():
    store = new_store()
    run(store, "CheckIn", checkin("A", "P5", "2026-03-01T22:00:00+13:00"))
    run(store, "CheckOut", checkout("A-out", "A", "P5", "2026-03-02T06:00:00+13:00"))
    out = run(store, "CheckIn", checkin("B", "P3", "2026-03-03T09:00:00+13:00"))
    assert out[0].detail["breach"] is True
    assert out[0].detail["rule_id"] == "R05"  # pork -> poultry, 1 night < 2


def test_version_conflict_retries_with_fresh_state():
    class ConflictOnce(InMemoryStore):
        armed = True

        def commit(self, person_id, version_read, visits, evaluation):
            if self.armed:
                self.armed = False
                run(self, "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"))
            super().commit(person_id, version_read, visits, evaluation)

    store = ConflictOnce(PROPERTIES)
    sleeps: list[float] = []
    out = run(
        store, "CheckIn", checkin("C", "P3", "2026-03-04T14:00:00+13:00"), sleep=sleeps.append
    )
    assert len(sleeps) == 1
    assert out[0].detail["version_read"] == 1
    assert out[0].detail["breach"] is True


def test_retry_exhausted_after_three_conflicts():
    class AlwaysConflict(InMemoryStore):
        def commit(self, *args, **kwargs):
            raise ConflictError("busy")

    sleeps: list[float] = []
    with pytest.raises(RetryExhaustedError):
        process_event(
            "CheckIn",
            checkin("A", "P1", "2026-03-03T09:00:00+13:00"),
            AlwaysConflict(PROPERTIES),
            RULES,
            TickingClock(),
            sleep=sleeps.append,
        )
    assert len(sleeps) == 3


def test_evaluation_exists_race_takes_the_already_processed_path():
    class Race(InMemoryStore):
        armed = True

        def commit(self, person_id, version_read, visits, evaluation):
            if self.armed and evaluation is not None:
                self.armed = False
                winner = dict(evaluation, processed_at="2026-03-09T00:00:00.000+13:00")
                super().commit(person_id, version_read, visits, winner)
                raise EvaluationExistsError(evaluation["checkin_event_id"])
            super().commit(person_id, version_read, visits, evaluation)

    out = run(Race(PROPERTIES), "CheckIn", checkin("A", "P1", "2026-03-03T09:00:00+13:00"))
    assert out[0].detail["processed_at"] == "2026-03-09T00:00:00.000+13:00"
```

- [ ] **Step 2: Run the tests**

Run: `.venv/bin/pytest tests/test_processor_checkout.py -q`
Expected: 8 passed. If any test fails, fix `processor.py` (not the test) until the spec §6.1 behaviour holds, then re-run.

- [ ] **Step 3: Commit**

```bash
make fmt && make test
git add tests/test_processor_checkout.py lambdas/standdown_checker/processor.py
git commit -m "Processor: check-out placeholders, earliest check-out, retries

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 8: Generator config and reference data

**Files:**
- Create: `generator/config.py`
- Create: `generator/reference.py`
- Test: `tests/test_reference.py`

**Interfaces:**
- Produces, in `generator.config`:
  - `ENVS = ("sim",)`.
  - `Scale(name, n_properties, n_people, n_owner_companies, n_visitor_companies, n_checkins, max_checkins_per_person=300)`.
  - `SCALES` with keys `default`, `aws` and `ci`.
  - Path functions: `repo_root()`, `state_dir(env)`, `data_dir(env)`, `ref_dir(env)`, `compacted_dir(env)`, `rules_path()`, `sim_store_path()`, `warehouse_path(env)`.
- Produces, in `generator.reference`:
  - Constants: `PROPERTY_TYPES`, `REGIONS`, `ROLES` (role → tuple of property types), `NEAR_KM = 10.0`.
  - Dataclasses:
    - `Property(property_id, name, property_type, group_id, owner_company_id, region, lat, lon)`;
    - `Person(person_id, visitor_company_id, role, home_region)`;
    - `Company(company_id, name, kind)`;
    - `Reference(properties, people, companies)` with `.property_map()`, `.write(dir)` and `Reference.read(dir)`.
  - `haversine_km(lat1, lon1, lat2, lon2) -> float`.
  - `build_reference(scale, rng) -> Reference`.
- JSON files written: `properties.json`, `people.json`, `companies.json`. Each is a JSON array of objects whose keys are the dataclass fields; dbt staging reads them.

- [ ] **Step 1: Write the failing tests** `tests/test_reference.py`

```python
import random

from generator.config import SCALES
from generator.reference import PROPERTY_TYPES, ROLES, Reference, build_reference, haversine_km


def test_haversine_christchurch_to_lincoln_is_about_17_km():
    assert 16 < haversine_km(-43.5321, 172.6362, -43.6400, 172.4860) < 18


def test_build_reference_matches_scale_and_is_deterministic():
    scale = SCALES["ci"]
    ref = build_reference(scale, random.Random(1))
    assert len(ref.properties) == scale.n_properties
    assert len(ref.people) == scale.n_people
    assert ref == build_reference(scale, random.Random(1))
    assert {p.property_type for p in ref.properties} <= set(PROPERTY_TYPES)
    assert {p.role for p in ref.people} <= set(ROLES)


def test_two_poultry_properties_with_different_owners_always_exist():
    ref = build_reference(SCALES["aws"], random.Random(7))
    poultry_owners = {p.owner_company_id for p in ref.properties if p.property_type == "poultry"}
    assert len(poultry_owners) >= 2


def test_write_and_read_round_trip(tmp_path):
    ref = build_reference(SCALES["ci"], random.Random(1))
    ref.write(tmp_path)
    assert Reference.read(tmp_path) == ref
    assert (tmp_path / "properties.json").read_text().startswith("[")
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_reference.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'generator.config'`.

- [ ] **Step 3: Implement** `generator/config.py`

```python
"""Scales and paths (spec §3.2). Every path hangs off REPO_ROOT so dbt and tests agree."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ENVS = ("sim",)  # Plan 2 adds "local" and "aws"


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
}


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

`generator/reference.py`:

```python
"""Reference data: properties, people, companies (spec §3.3, §3.6)."""

from __future__ import annotations

import json
import math
import random
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import Scale

PROPERTY_TYPES = (
    "poultry", "pork", "beef", "dairy", "kiwifruit", "vineyard", "office", "feed_mill",
)  # fmt: skip
TYPE_WEIGHTS = (0.22, 0.12, 0.14, 0.18, 0.12, 0.10, 0.06, 0.06)
REGIONS = {
    "canterbury": (-43.53, 172.64),
    "waikato": (-37.79, 175.28),
    "bay_of_plenty": (-37.88, 176.70),
    "hawkes_bay": (-39.49, 176.91),
    "marlborough": (-41.51, 173.95),
    "southland": (-46.41, 168.35),
}
TYPE_REGIONS = {
    "kiwifruit": ("bay_of_plenty", "waikato"),
    "vineyard": ("marlborough", "hawkes_bay"),
}
ROLES = {
    "poultry_contractor": ("poultry", "feed_mill"),
    "pork_vet": ("pork",),
    "livestock_carrier": ("beef", "dairy", "pork"),
    "dairy_contractor": ("dairy", "beef"),
    "orchard_worker": ("kiwifruit",),
    "vineyard_contractor": ("vineyard",),
    "consultant": PROPERTY_TYPES,
}
ROLE_WEIGHTS = {
    "poultry_contractor": 0.22,
    "pork_vet": 0.08,
    "livestock_carrier": 0.20,
    "dairy_contractor": 0.18,
    "orchard_worker": 0.12,
    "vineyard_contractor": 0.10,
    "consultant": 0.10,
}
NEAR_KM = 10.0


@dataclass(frozen=True)
class Property:
    property_id: str
    name: str
    property_type: str
    group_id: str
    owner_company_id: str
    region: str
    lat: float
    lon: float


@dataclass(frozen=True)
class Person:
    person_id: str
    visitor_company_id: str
    role: str
    home_region: str


@dataclass(frozen=True)
class Company:
    company_id: str
    name: str
    kind: str


@dataclass(frozen=True)
class Reference:
    properties: tuple[Property, ...]
    people: tuple[Person, ...]
    companies: tuple[Company, ...]

    def property_map(self) -> dict[str, Property]:
        return {p.property_id: p for p in self.properties}

    def write(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        for name, rows in (
            ("properties", self.properties),
            ("people", self.people),
            ("companies", self.companies),
        ):
            (directory / f"{name}.json").write_text(json.dumps([asdict(r) for r in rows], indent=1))

    @classmethod
    def read(cls, directory: Path) -> Reference:
        def load(name: str) -> list[dict]:
            return json.loads((directory / f"{name}.json").read_text())

        return cls(
            tuple(Property(**r) for r in load("properties")),
            tuple(Person(**r) for r in load("people")),
            tuple(Company(**r) for r in load("companies")),
        )


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def build_reference(scale: Scale, rng: random.Random) -> Reference:
    owners = [
        Company(f"C{i:03d}", f"Owner {i}", "owner") for i in range(1, scale.n_owner_companies + 1)
    ]
    visitors = [
        Company(f"V{i:03d}", f"Contractor {i}", "visitor")
        for i in range(1, scale.n_visitor_companies + 1)
    ]
    types = [rng.choices(PROPERTY_TYPES, TYPE_WEIGHTS)[0] for _ in range(scale.n_properties)]
    types[0] = types[1] = "poultry"  # live breach pairs need two poultry sites, different owners
    properties = []
    for i, ptype in enumerate(types, start=1):
        owner = owners[0] if i == 1 else owners[1] if i == 2 else rng.choice(owners)
        region = rng.choice(TYPE_REGIONS.get(ptype, tuple(REGIONS)))
        lat0, lon0 = REGIONS[region]
        properties.append(
            Property(
                property_id=f"P{i:04d}",
                name=f"{ptype.replace('_', ' ').title()} {i}",
                property_type=ptype,
                group_id=f"{owner.company_id}-G{rng.randint(1, 3)}",
                owner_company_id=owner.company_id,
                region=region,
                # Spread measured so ~70% of moves are under 10 km at the ci and default
                # scales (spec §3.3); the tiny aws scale is sparser (~40%), which is accepted.
                lat=round(lat0 + rng.gauss(0, 0.06), 5),
                lon=round(lon0 + rng.gauss(0, 0.07), 5),
            )
        )
    roles, weights = list(ROLE_WEIGHTS), list(ROLE_WEIGHTS.values())
    people = [
        Person(
            person_id=f"U{i:05d}",
            visitor_company_id=rng.choice(visitors).company_id,
            role=rng.choices(roles, weights)[0],
            home_region=rng.choice(tuple(REGIONS)),
        )
        for i in range(1, scale.n_people + 1)
    ]
    return Reference(tuple(properties), tuple(people), tuple(owners + visitors))
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `.venv/bin/pytest tests/test_reference.py -q`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add generator/config.py generator/reference.py tests/test_reference.py
git commit -m "Generator: scales, paths and reference data

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 9: Backfill planning and event emission

**Files:**
- Create: `generator/stream.py`
- Test: `tests/test_stream_backfill.py`

**Interfaces:**
- Consumes:
  - `generator.reference` (`Reference`, `Person`, `Property`, `ROLES`, `NEAR_KM`, `haversine_km`);
  - `generator.config.Scale`;
  - `standdown_checker.events` (`APP_SOURCE`, `NZ`, `iso`).
- Produces, in `generator.stream`:
  - Constants: `OFFLINE_P = 0.05`, `DUPLICATE_P = 0.02`, `NO_CHECKOUT_P = 0.08`, `NEAR_P = 0.7`.
  - `PlannedVisit(person_id, property_id, checkin: datetime, departs: datetime, has_checkout: bool)`.
  - `PlannedPlantMovement(source_property_id, dest_property_id, movement_date: date, material_type, volume)`.
  - `Emission(send_time: datetime, detail_type: str, detail: dict, source: str = "onside.app")`.
  - `Lateness(max_offline: timedelta, max_duplicate_delay: timedelta)`, with `BACKFILL_LATENESS` and `LIVE_LATENESS`.
  - `event_id(run_id: str, seq: int) -> str`.
  - `class Mover(ref, rng)` with `.next(person, current_property_id | None) -> Property`.
  - `plan_backfill(ref, scale, rng, anchor) -> tuple[list[PlannedVisit], list[PlannedPlantMovement]]`.
  - `build_emissions(visits, plants, ref, run_id, rng, run_end, lateness) -> tuple[list[Emission], dict[str, int]]`. The counts dict has keys `CheckIn`, `CheckOut` and `PlantMovement`.

- [ ] **Step 1: Write the failing tests** `tests/test_stream_backfill.py`

```python
import random
import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest

from generator.config import SCALES
from generator.reference import build_reference, haversine_km
from generator.stream import BACKFILL_LATENESS, build_emissions, event_id, plan_backfill

ANCHOR = datetime(2026, 10, 1, tzinfo=UTC)
RUN_ID = str(uuid.UUID(int=1))


@pytest.fixture(scope="module")
def backfill():
    scale = SCALES["ci"]
    ref = build_reference(scale, random.Random(1))
    rng = random.Random("42:backfill")
    visits, plants = plan_backfill(ref, scale, rng, ANCHOR)
    emissions, counts = build_emissions(
        visits, plants, ref, RUN_ID, rng, ANCHOR, BACKFILL_LATENESS
    )
    return ref, visits, plants, emissions, counts


def parse(value):
    return datetime.fromisoformat(value)


def originals(emissions):
    first = {}
    for em in emissions:
        first.setdefault(em.detail["event_id"], em)
    return first


def test_is_deterministic():
    scale = SCALES["ci"]
    ref = build_reference(scale, random.Random(1))
    a = plan_backfill(ref, scale, random.Random("x"), ANCHOR)
    b = plan_backfill(ref, scale, random.Random("x"), ANCHOR)
    assert a == b


def test_visits_never_overlap_and_last_at_most_48_hours(backfill):
    _, visits, _, _, _ = backfill
    by_person = defaultdict(list)
    for v in visits:
        assert v.checkin <= v.departs <= v.checkin + timedelta(hours=48)
        by_person[v.person_id].append(v)
    for person_visits in by_person.values():
        person_visits.sort(key=lambda v: v.checkin)
        for prev, nxt in zip(person_visits, person_visits[1:], strict=False):
            assert nxt.checkin >= prev.departs


def test_per_person_cap_and_total_scale(backfill):
    _, visits, _, _, _ = backfill
    counts = defaultdict(int)
    for v in visits:
        counts[v.person_id] += 1
    assert max(counts.values()) <= 300
    assert 0.5 * SCALES["ci"].n_checkins <= len(visits) <= 1.1 * SCALES["ci"].n_checkins


def test_about_seventy_percent_of_moves_are_under_10_km(backfill):
    ref, visits, _, _, _ = backfill
    props = ref.property_map()
    by_person = defaultdict(list)
    for v in visits:
        by_person[v.person_id].append(v)
    near = total = 0
    for person_visits in by_person.values():
        person_visits.sort(key=lambda v: v.checkin)
        for a, b in zip(person_visits, person_visits[1:], strict=False):
            if a.property_id == b.property_id:
                continue
            pa, pb = props[a.property_id], props[b.property_id]
            total += 1
            near += haversine_km(pa.lat, pa.lon, pb.lat, pb.lon) < 10
    assert 0.6 <= near / total <= 0.9


def test_checkouts_belong_to_checkins_and_are_unique(backfill):
    _, _, _, emissions, _ = backfill
    first = originals(emissions)
    checkin_ids = {k for k, em in first.items() if em.detail_type == "CheckIn"}
    per_checkin = defaultdict(set)
    for em in first.values():
        if em.detail_type == "CheckOut":
            assert em.detail["checkin_event_id"] in checkin_ids
            per_checkin[em.detail["checkin_event_id"]].add(em.detail["event_id"])
    assert all(len(ids) == 1 for ids in per_checkin.values())


def test_lateness_bounds_and_clamping(backfill):
    _, _, _, emissions, _ = backfill
    for em in emissions:
        received = parse(em.detail["received_time"])
        assert received <= ANCHOR
        if em.detail_type in ("CheckIn", "CheckOut"):
            lag = received - parse(em.detail["event_time"])
            assert timedelta(0) <= lag <= timedelta(days=7)


def test_offline_share_is_about_five_percent(backfill):
    _, _, _, emissions, _ = backfill
    first = [em for em in originals(emissions).values() if em.detail_type == "CheckIn"]
    late = sum(
        parse(em.detail["received_time"]) - parse(em.detail["event_time"]) > timedelta(hours=1)
        for em in first
    )
    assert 0.03 <= late / len(first) <= 0.07


def test_duplicates_are_identical_and_within_seven_days(backfill):
    _, _, _, emissions, _ = backfill
    first = originals(emissions)
    duplicates = 0
    for em in emissions:
        orig = first[em.detail["event_id"]]
        if em is not orig:
            duplicates += 1
            assert em.detail == orig.detail and em.detail_type == orig.detail_type
            assert timedelta(0) <= em.send_time - orig.send_time <= timedelta(days=7)
    assert 0.01 <= duplicates / len(first) <= 0.03


def test_send_order_and_ids(backfill):
    _, _, _, emissions, counts = backfill
    times = [em.send_time for em in emissions]
    assert times == sorted(times)
    total = sum(counts.values())
    assert set(originals(emissions)) == {event_id(RUN_ID, s) for s in range(total)}
    assert counts["PlantMovement"] > 0
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_stream_backfill.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'generator.stream'`.

- [ ] **Step 3: Implement** `generator/stream.py`. It includes `plan_live` and `update_cursors`, which Task 10 tests:

```python
"""Visit planning and event emission (spec §3.2, §3.3)."""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from standdown_checker.events import APP_SOURCE, NZ, iso

from .config import Scale
from .reference import NEAR_KM, ROLES, Person, Property, Reference, haversine_km

OFFLINE_P = 0.05
DUPLICATE_P = 0.02
NO_CHECKOUT_P = 0.08
NEAR_P = 0.7
CHECKIN_METHODS = (("app", 0.45), ("geofence", 0.35), ("qr", 0.10), ("kiosk", 0.07), ("link", 0.03))
CHECKOUT_METHODS = (("geofence_auto", 0.5), ("manual", 0.4), ("end_of_day", 0.1))
JOB_TYPES = (
    "delivery", "maintenance", "vet_visit", "feed_delivery", "stock_cartage",
    "spraying", "harvest", "inspection", "consulting",
)  # fmt: skip
MATERIAL_TYPES = ("budwood", "rootstock", "grafted_plants", "pollen")
LIVE_WINDOW = timedelta(minutes=30)
LIVE_MARGIN = timedelta(minutes=2)
PAIR_SPAN = timedelta(minutes=15)


@dataclass(frozen=True)
class PlannedVisit:
    person_id: str
    property_id: str
    checkin: datetime
    departs: datetime
    has_checkout: bool


@dataclass(frozen=True)
class PlannedPlantMovement:
    source_property_id: str
    dest_property_id: str
    movement_date: date
    material_type: str
    volume: int


@dataclass(frozen=True)
class Emission:
    send_time: datetime
    detail_type: str
    detail: dict
    source: str = APP_SOURCE


@dataclass(frozen=True)
class Lateness:
    max_offline: timedelta
    max_duplicate_delay: timedelta


BACKFILL_LATENESS = Lateness(timedelta(days=7), timedelta(days=7))
LIVE_LATENESS = Lateness(timedelta(minutes=10), timedelta(minutes=1))


def event_id(run_id: str, seq: int) -> str:
    return str(uuid.uuid5(uuid.UUID(run_id), str(seq)))


def _seconds(ts: datetime) -> datetime:
    return ts.replace(microsecond=0)


def _weighted(rng: random.Random, options) -> str:
    names, weights = zip(*options, strict=True)
    return rng.choices(names, weights)[0]


def _duration(rng: random.Random) -> timedelta:
    u = rng.random()
    if u < 0.70:
        hours = rng.uniform(0.25, 3)
    elif u < 0.95:
        hours = rng.uniform(3, 10)
    else:
        hours = rng.uniform(10, 47.5)
    return timedelta(hours=hours)


class Mover:
    """Chooses the next property: mostly nearby and of a type the person's role works with."""

    def __init__(self, ref: Reference, rng: random.Random) -> None:
        self.rng = rng
        self.near = {
            p.property_id: [
                q
                for q in ref.properties
                if q.property_id != p.property_id
                and haversine_km(p.lat, p.lon, q.lat, q.lon) <= NEAR_KM
            ]
            for p in ref.properties
        }
        self.pools = {
            role: [p for p in ref.properties if p.property_type in types] or list(ref.properties)
            for role, types in ROLES.items()
        }

    def next(self, person: Person, current: str | None) -> Property:
        pool = self.pools[person.role]
        if current is not None:
            types = {p.property_type for p in pool}
            near = [p for p in self.near.get(current, []) if p.property_type in types]
            if near and self.rng.random() < NEAR_P:
                return self.rng.choice(near)
        choices = [p for p in pool if p.property_id != current] or pool
        return self.rng.choice(choices)


def plan_backfill(
    ref: Reference, scale: Scale, rng: random.Random, anchor: datetime
) -> tuple[list[PlannedVisit], list[PlannedPlantMovement]]:
    start = anchor - timedelta(days=365)
    mover = Mover(ref, rng)
    shares = [rng.paretovariate(1.5) for _ in ref.people]
    total_share = sum(shares)
    visits: list[PlannedVisit] = []
    for person, share in zip(ref.people, shares, strict=True):
        k = round(scale.n_checkins * share / total_share)
        k = max(1, min(scale.max_checkins_per_person, k))
        mean_gap = (anchor - start).total_seconds() / k
        t = start + timedelta(seconds=rng.uniform(0, mean_gap))
        current: str | None = None
        for _ in range(k):
            if t >= anchor:
                break
            prop = mover.next(person, current)
            checkin = _seconds(t)
            departs = _seconds(checkin + _duration(rng))
            has_checkout = departs <= anchor and rng.random() >= NO_CHECKOUT_P
            visits.append(
                PlannedVisit(person.person_id, prop.property_id, checkin, departs, has_checkout)
            )
            current = prop.property_id
            gap = rng.expovariate(1.0 / max(mean_gap - 4 * 3600, 3600))
            t = departs + timedelta(seconds=max(gap, 600))
    plants = _plan_plants(ref, rng, start, anchor, max(1, scale.n_checkins // 40))
    return visits, plants


def _plan_plants(
    ref: Reference, rng: random.Random, start: datetime, end: datetime, count: int
) -> list[PlannedPlantMovement]:
    plant_props = [p for p in ref.properties if p.property_type in ("kiwifruit", "vineyard")]
    out: list[PlannedPlantMovement] = []
    for _ in range(count):
        if len(plant_props) < 2:
            break
        src = rng.choice(plant_props)
        same = [
            p
            for p in plant_props
            if p.property_type == src.property_type and p.property_id != src.property_id
        ]
        if not same:
            continue
        dst = rng.choice(same)
        moment = start + timedelta(seconds=rng.uniform(0, (end - start).total_seconds()))
        out.append(
            PlannedPlantMovement(
                src.property_id,
                dst.property_id,
                moment.astimezone(NZ).date(),
                rng.choice(MATERIAL_TYPES),
                rng.randint(10, 500),
            )
        )
    return out


def plan_live(
    ref: Reference,
    cursors: dict[str, dict],
    rng: random.Random,
    now: datetime,
    n: int,
    cap: int,
) -> tuple[list[PlannedVisit], int]:
    """§3.2 live mode. Returns the visits and how many requested check-ins could not fit."""
    window_start, latest = now - LIVE_WINDOW, now - LIVE_MARGIN
    mover = Mover(ref, rng)
    people = list(ref.people)
    rng.shuffle(people)
    poultry = [p for p in ref.properties if p.property_type == "poultry"]
    pairs = min(round(0.2 * n), n // 2)
    regular = n - 2 * pairs
    visits: list[PlannedVisit] = []
    used: set[str] = set()

    def earliest(person: Person) -> datetime:
        cursor = cursors.get(person.person_id)
        if cursor and cursor["last_end"]:
            last_end = datetime.fromisoformat(cursor["last_end"]) + timedelta(minutes=1)
            return max(window_start, last_end)
        return window_start

    def remaining(person: Person) -> int:
        cursor = cursors.get(person.person_id)
        return cap - (cursor["n_checkins"] if cursor else 0)

    for person in people:  # deliberate breaches: two poultry sites with different owners
        if pairs == 0:
            break
        start = earliest(person)
        if remaining(person) < 2 or start + PAIR_SPAN > latest:
            continue
        p1 = rng.choice(poultry)
        others = [p for p in poultry if p.owner_company_id != p1.owner_company_id]
        if not others:
            break
        p2 = rng.choice(others)
        span = (latest - PAIR_SPAN - start).total_seconds()
        t1 = _seconds(start + timedelta(seconds=rng.uniform(0, span)))
        visits.append(
            PlannedVisit(person.person_id, p1.property_id, t1, t1 + timedelta(minutes=5), True)
        )
        visits.append(
            PlannedVisit(
                person.person_id,
                p2.property_id,
                t1 + timedelta(minutes=8),
                t1 + timedelta(minutes=13),
                True,
            )
        )
        used.add(person.person_id)
        pairs -= 1

    for person in people:
        if regular == 0:
            break
        if person.person_id in used:
            continue
        start = earliest(person)
        if remaining(person) < 1 or start + timedelta(minutes=3) > latest:
            continue
        current = (cursors.get(person.person_id) or {}).get("last_property_id")
        prop = mover.next(person, current)
        span = (latest - timedelta(minutes=3) - start).total_seconds()
        checkin = _seconds(start + timedelta(seconds=rng.uniform(0, span)))
        departs = _seconds(checkin + timedelta(seconds=rng.uniform(60, 600)))
        has_checkout = departs <= latest
        visits.append(
            PlannedVisit(
                person.person_id,
                prop.property_id,
                checkin,
                departs if has_checkout else now,  # open visit: physical end unknown
                has_checkout,
            )
        )
        used.add(person.person_id)
        regular -= 1
    return visits, 2 * pairs + regular


def build_emissions(
    visits: list[PlannedVisit],
    plants: list[PlannedPlantMovement],
    ref: Reference,
    run_id: str,
    rng: random.Random,
    run_end: datetime,
    lateness: Lateness,
) -> tuple[list[Emission], dict[str, int]]:
    people = {p.person_id: p for p in ref.people}
    counts = {"CheckIn": 0, "CheckOut": 0, "PlantMovement": 0}
    seq = 0

    def next_id() -> str:
        nonlocal seq
        eid = event_id(run_id, seq)
        seq += 1
        return eid

    min_offline = min(3600.0, lateness.max_offline.total_seconds() / 10)

    def received(event_time: datetime) -> datetime:
        if rng.random() < OFFLINE_P:
            delay = rng.uniform(min_offline, lateness.max_offline.total_seconds())
        else:
            delay = rng.uniform(1, 30)
        return min(_seconds(event_time + timedelta(seconds=delay)), run_end)

    originals: list[Emission] = []
    for v in visits:
        cid = next_id()
        r_in = received(v.checkin)
        originals.append(
            Emission(
                r_in,
                "CheckIn",
                {
                    "event_id": cid,
                    "person_id": v.person_id,
                    "visitor_company_id": people[v.person_id].visitor_company_id,
                    "property_id": v.property_id,
                    "event_time": iso(v.checkin),
                    "received_time": iso(r_in),
                    "method": _weighted(rng, CHECKIN_METHODS),
                    "job_type": rng.choice(JOB_TYPES),
                    "additional_visitors": 0 if rng.random() < 0.85 else rng.randint(1, 3),
                    "answers": [
                        {"question_id": "Q-BIO-1", "answer": rng.choice(("no", "no", "no", "yes"))}
                    ],
                },
            )
        )
        counts["CheckIn"] += 1
        if v.has_checkout:
            oid = next_id()
            r_out = received(v.departs)
            originals.append(
                Emission(
                    r_out,
                    "CheckOut",
                    {
                        "event_id": oid,
                        "checkin_event_id": cid,
                        "person_id": v.person_id,
                        "property_id": v.property_id,
                        "event_time": iso(v.departs),
                        "received_time": iso(r_out),
                        "method": _weighted(rng, CHECKOUT_METHODS),
                    },
                )
            )
            counts["CheckOut"] += 1
    for pm in plants:
        pid = next_id()
        day_end = datetime.combine(pm.movement_date + timedelta(days=1), time.min, tzinfo=NZ)
        r_pm = min(_seconds(day_end.astimezone(UTC) + timedelta(seconds=rng.uniform(0, 7200))), run_end)
        originals.append(
            Emission(
                r_pm,
                "PlantMovement",
                {
                    "event_id": pid,
                    "source_property_id": pm.source_property_id,
                    "dest_property_id": pm.dest_property_id,
                    "movement_date": pm.movement_date.isoformat(),
                    "material_type": pm.material_type,
                    "volume": pm.volume,
                    "received_time": iso(r_pm),
                },
            )
        )
        counts["PlantMovement"] += 1

    emissions = list(originals)
    max_dup = lateness.max_duplicate_delay.total_seconds()
    for em in originals:
        if rng.random() < DUPLICATE_P:
            when = min(_seconds(em.send_time + timedelta(seconds=rng.uniform(1, max_dup))), run_end)
            emissions.append(Emission(when, em.detail_type, em.detail, em.source))
    emissions.sort(key=lambda e: e.send_time)  # stable: ties keep creation order
    return emissions, counts


def update_cursors(cursors: dict[str, dict], visits: list[PlannedVisit]) -> None:
    """Remember each person's last visit so the next live run continues their timeline."""
    for v in sorted(visits, key=lambda v: v.checkin):
        cursor = cursors.setdefault(
            v.person_id,
            {"last_property_id": None, "last_checkin": None, "last_end": None, "n_checkins": 0},
        )
        cursor["last_property_id"] = v.property_id
        cursor["last_checkin"] = iso(v.checkin)
        cursor["last_end"] = iso(v.departs)
        cursor["n_checkins"] += 1
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `.venv/bin/pytest tests/test_stream_backfill.py -q`
Expected: 9 passed.

If a share test (distance, offline or duplicates) fails by a small margin, check the fixture seed before changing a constant. The constants come from the spec.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add generator/stream.py tests/test_stream_backfill.py
git commit -m "Generator: backfill planning and event emission with spec guarantees

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 10: Live mode, cursors, generator state and manifests

**Files:**
- Create: `generator/state.py`
- Test: `tests/test_stream_live.py`
- Test: `tests/test_state.py`

**Interfaces:**
- Consumes:
  - Task 9's `plan_live`, `update_cursors`, `PlannedVisit` and `build_emissions`;
  - `generator.config` paths;
  - `standdown_checker.rules.evaluate` (tests).
- Produces, in `generator.state`:
  - `class GeneratorError(RuntimeError)`: a user-facing fix-it message.
  - `write_json(path, obj)` (atomic).
  - `read_setup(env) -> dict` and `write_setup(env, seed, scale_name)`.
  - `load_state(env) -> dict | None` and `save_state(env, state)`.
  - `write_manifest(env, run_id, mode, seed, counts) -> dict`, which writes `state/<env>/manifests/<run_id>.json`.
  - `manifest_ids(env) -> set[str]`.
  - `check_consistency(env, state) -> None`.
- State JSON: `{"env", "seed", "scale", "anchor", "runs": [run_id...], "cursors": {person_id: {"last_property_id", "last_checkin", "last_end", "n_checkins"}}}`.

- [ ] **Step 1: Write the failing tests** `tests/test_stream_live.py`

```python
import random
from datetime import UTC, datetime, timedelta

from generator.config import SCALES
from generator.reference import build_reference
from generator.stream import PlannedVisit, plan_live, update_cursors
from standdown_checker.events import iso
from standdown_checker.rules import Visit, evaluate
from tests.helpers import RULES

NOW = datetime(2026, 10, 9, 4, 0, tzinfo=UTC)
REF = build_reference(SCALES["ci"], random.Random(1))


def cursors_with(last_end, n_checkins=10):
    return {
        p.person_id: {
            "last_property_id": REF.properties[2].property_id,
            "last_checkin": iso(last_end - timedelta(hours=1)),
            "last_end": iso(last_end),
            "n_checkins": n_checkins,
        }
        for p in REF.people
    }


def test_live_visits_start_after_each_persons_last_end_and_inside_the_window():
    cursors = cursors_with(NOW - timedelta(minutes=20))
    visits, shortfall = plan_live(REF, cursors, random.Random(3), NOW, 20, 300)
    assert shortfall == 0 and len(visits) == 20
    for v in visits:
        last_end = datetime.fromisoformat(cursors[v.person_id]["last_end"])
        assert v.checkin >= last_end + timedelta(minutes=1)
        assert NOW - timedelta(minutes=30) <= v.checkin <= NOW
        assert v.departs <= NOW


def test_live_shortfall_when_no_one_has_room():
    visits, shortfall = plan_live(REF, cursors_with(NOW), random.Random(3), NOW, 10, 300)
    assert visits == []
    assert shortfall == 10


def test_live_respects_the_per_person_cap():
    cursors = cursors_with(NOW - timedelta(days=1), n_checkins=300)
    visits, shortfall = plan_live(REF, cursors, random.Random(3), NOW, 10, 300)
    assert visits == [] and shortfall == 10


def test_live_pairs_are_standdown_breaches():
    visits, _ = plan_live(REF, {}, random.Random(5), NOW, 10, 300)
    props = REF.property_map()
    by_person: dict[str, list[PlannedVisit]] = {}
    for v in visits:
        by_person.setdefault(v.person_id, []).append(v)
    pairs = [vs for vs in by_person.values() if len(vs) == 2]
    assert len(pairs) == 2  # round(0.2 * 10)
    for first, second in pairs:
        p1, p2 = props[first.property_id], props[second.property_id]
        origin = Visit("a", p1.property_id, p1.property_type, p1.owner_company_id,
                       first.checkin, first.departs)  # fmt: skip
        target = Visit("b", p2.property_id, p2.property_type, p2.owner_company_id, second.checkin)
        assert evaluate(target, [origin], RULES).breach


def test_update_cursors_tracks_the_last_visit():
    cursors: dict = {}
    t = NOW - timedelta(hours=2)
    update_cursors(
        cursors,
        [
            PlannedVisit("U1", "P2", t + timedelta(hours=1), t + timedelta(hours=1, minutes=5), True),
            PlannedVisit("U1", "P1", t, t + timedelta(minutes=5), True),
        ],
    )
    assert cursors["U1"]["last_property_id"] == "P2"
    assert cursors["U1"]["n_checkins"] == 2
```

`tests/test_state.py`:

```python
import pytest

from generator import state as S


def test_setup_state_and_manifests_round_trip(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    S.write_setup("sim", 42, "ci")
    assert S.read_setup("sim") == {"seed": 42, "scale": "ci"}
    assert S.load_state("sim") is None
    manifest = S.write_manifest("sim", "run-1", "backfill", 42, {"CheckIn": 3, "CheckOut": 2})
    assert manifest["total"] == 5
    S.save_state("sim", {"runs": ["run-1"], "cursors": {}})
    assert S.manifest_ids("sim") == {"run-1"}
    S.check_consistency("sim", S.load_state("sim"))


def test_missing_setup_is_a_clear_error(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    with pytest.raises(S.GeneratorError, match="make setup"):
        S.read_setup("sim")


def test_inconsistent_manifests_are_detected(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    S.write_manifest("sim", "run-1", "backfill", 42, {"CheckIn": 1})
    with pytest.raises(S.GeneratorError, match="reset"):
        S.check_consistency("sim", None)
```

- [ ] **Step 2: Run them to confirm `test_state.py` fails**

Run: `.venv/bin/pytest tests/test_stream_live.py tests/test_state.py -q`
Expected:
- `test_stream_live.py`: passes (5), because `plan_live` came with Task 9.
- `test_state.py`: FAIL with `ImportError: cannot import name 'state'`.

- [ ] **Step 3: Implement** `generator/state.py`

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


def write_manifest(env: str, run_id: str, mode: str, seed: int, counts: dict[str, int]) -> dict:
    manifest = {
        "run_id": run_id,
        "mode": mode,
        "seed": seed,
        "env": env,
        "counts": counts,
        "total": sum(counts.values()),
    }
    write_json(state_dir(env) / "manifests" / f"{run_id}.json", manifest)
    return manifest


def manifest_ids(env: str) -> set[str]:
    directory = state_dir(env) / "manifests"
    return {p.stem for p in directory.glob("*.json")} if directory.exists() else set()


def check_consistency(env: str, state: dict | None) -> None:
    runs = set(state["runs"]) if state else set()
    found = manifest_ids(env)
    if found != runs:
        raise GeneratorError(
            f"ENV={env} manifests {sorted(found)} do not match generator state {sorted(runs)}; "
            f"run `make reset ENV={env}` and start again"
        )
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `.venv/bin/pytest tests/test_stream_live.py tests/test_state.py -q`
Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add generator/state.py tests/test_stream_live.py tests/test_state.py
git commit -m "Generator: live continuation, cursors, state and manifests

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 11: Sim runner, CLI and Makefile data targets

**Files:**
- Modify: `generator/local_sim.py` (append the runner)
- Create: `generator/cli.py`
- Create: `generator/__main__.py`
- Modify: `Makefile` (add the data targets)
- Test: `tests/test_sim_e2e.py`

**Interfaces:**
- Consumes: everything from Tasks 2–10.
- Produces:
  - `generator.local_sim.envelope(source, detail_type, time_iso, detail, envelope_id) -> dict`, with keys `id`, `source`, `detail-type`, `time` and `detail`.
  - `generator.local_sim.run_sim(emissions, store, rules, out_dir) -> dict` (counts). It writes:
    - `out_dir/generator.ndjson`: one envelope per emission, duplicates included;
    - `out_dir/standdown.ndjson`: the processor's outbound envelopes.
  - `generator.cli.main(argv: list[str] | None = None) -> int`, with exit code 0 on success and 2 on `GeneratorError`. Subcommands:
    - `setup --env sim [--scale ci] [--seed 42]`
    - `generate --env sim --mode backfill|live [--n 50] [--anchor ISO] [--now ISO]`
    - `reset --env sim`
  - Makefile targets: `setup`, `generate`, `reset`, with variables `ENV ?= sim`, `SCALE ?= ci`, `MODE ?= backfill`, `N ?= 50`.

- [ ] **Step 1: Write the failing tests** `tests/test_sim_e2e.py`

```python
import json
import shutil
from collections import Counter
from pathlib import Path

import pytest

from generator.cli import main

ROOT = Path(__file__).resolve().parents[1]
ANCHOR = "2026-10-01T00:00:00+00:00"
NOW = "2026-10-09T04:00:00+00:00"


@pytest.fixture()
def repo(tmp_path, monkeypatch):
    shutil.copytree(ROOT / "shared", tmp_path / "shared")
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    return tmp_path


def read_ndjson(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def run_dirs(repo: Path) -> list[Path]:
    return sorted((repo / "data" / "sim" / "compacted").iterdir())


def test_backfill_then_live_produces_complete_derived_events(repo):
    assert main(["setup", "--env", "sim", "--scale", "ci"]) == 0
    assert main(["generate", "--env", "sim", "--mode", "backfill", "--anchor", ANCHOR]) == 0
    assert main(["generate", "--env", "sim", "--mode", "live", "--n", "20", "--now", NOW]) == 0

    dirs = run_dirs(repo)
    assert len(dirs) == 2
    generated, derived = [], []
    for d in dirs:
        generated += read_ndjson(d / "generator.ndjson")
        derived += read_ndjson(d / "standdown.ndjson")
    checkins = {e["detail"]["event_id"] for e in generated if e["detail-type"] == "CheckIn"}
    checkouts = {e["detail"]["event_id"] for e in generated if e["detail-type"] == "CheckOut"}
    evaluated = {e["detail"]["checkin_event_id"] for e in derived
                 if e["detail-type"] == "CheckInEvaluated"}  # fmt: skip
    applied = {e["detail"]["checkout_event_id"] for e in derived
               if e["detail-type"] == "CheckOutApplied"}  # fmt: skip
    assert evaluated == checkins
    assert applied == checkouts
    kinds = Counter(e["detail-type"] for e in derived)
    assert kinds["HighRiskVisitDetected"] > 0

    # Versions continue across runs: live evaluations read state the backfill committed.
    runs = json.loads((repo / "state" / "sim" / "generator_state.json").read_text())["runs"]
    live = read_ndjson(repo / "data" / "sim" / "compacted" / runs[1] / "standdown.ndjson")
    evaluations = [e["detail"] for e in live if e["detail-type"] == "CheckInEvaluated"]
    assert evaluations and all(e["version_read"] > 0 for e in evaluations)


def test_second_backfill_is_refused(repo, capsys):
    main(["setup", "--env", "sim", "--scale", "ci"])
    main(["generate", "--env", "sim", "--mode", "backfill", "--anchor", ANCHOR])
    assert main(["generate", "--env", "sim", "--mode", "backfill", "--anchor", ANCHOR]) == 2
    assert "make reset ENV=sim" in capsys.readouterr().err


def test_live_without_backfill_is_refused(repo, capsys):
    main(["setup", "--env", "sim", "--scale", "ci"])
    assert main(["generate", "--env", "sim", "--mode", "live", "--now", NOW]) == 2
    assert "MODE=backfill" in capsys.readouterr().err


def test_deleted_manifest_is_detected(repo, capsys):
    main(["setup", "--env", "sim", "--scale", "ci"])
    main(["generate", "--env", "sim", "--mode", "backfill", "--anchor", ANCHOR])
    for manifest in (repo / "state" / "sim" / "manifests").glob("*.json"):
        manifest.unlink()
    assert main(["generate", "--env", "sim", "--mode", "live", "--now", NOW]) == 2
    assert "do not match" in capsys.readouterr().err


def test_reset_clears_state_and_data(repo):
    main(["setup", "--env", "sim", "--scale", "ci"])
    main(["generate", "--env", "sim", "--mode", "backfill", "--anchor", ANCHOR])
    assert main(["reset", "--env", "sim"]) == 0
    assert not (repo / "state" / "sim").exists()
    assert not (repo / "data" / "sim").exists()
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_sim_e2e.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'generator.cli'`.

- [ ] **Step 3: Implement.** Append to `generator/local_sim.py`:

```python
def envelope(source: str, detail_type: str, time_iso: str, detail: dict, envelope_id: str) -> dict:
    """The subset of an EventBridge envelope that staging reads (spec §5)."""
    return {
        "id": envelope_id,
        "source": source,
        "detail-type": detail_type,
        "time": time_iso,
        "detail": detail,
    }


def run_sim(emissions, store, rules, out_dir: Path) -> dict[str, int]:
    """Process emissions in send order, as the Lambda would, and write NDJSON like compacted/."""
    from standdown_checker.events import iso
    from standdown_checker.processor import process_event

    out_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    with (
        open(out_dir / "generator.ndjson", "w") as gen,
        open(out_dir / "standdown.ndjson", "w") as derived,
    ):
        for i, em in enumerate(emissions):
            env = envelope(
                em.source, em.detail_type, iso(em.send_time), em.detail,
                f"{em.detail['event_id']}#{i}",
            )  # fmt: skip
            gen.write(json.dumps(env, sort_keys=True) + "\n")
            if em.detail_type not in ("CheckIn", "CheckOut"):
                continue
            outbound = process_event(
                em.detail_type,
                em.detail,
                store,
                rules,
                lambda em=em: em.send_time,  # simulated clock: processed when received
                sleep=lambda seconds: None,
            )
            for j, out in enumerate(outbound):
                counts[out.detail_type] = counts.get(out.detail_type, 0) + 1
                env = envelope(
                    out.source, out.detail_type, out.detail["processed_at"], out.detail,
                    f"{out.detail['event_id']}#{i}.{j}",
                )  # fmt: skip
                derived.write(json.dumps(env, sort_keys=True) + "\n")
    return counts
```

Create `generator/cli.py`:

```python
"""Command line for the generator: setup, generate, reset (spec §3.2, §8)."""

from __future__ import annotations

import argparse
import random
import shutil
import sys
import uuid
from datetime import UTC, datetime

from standdown_checker.rules import load_rules

from . import state as S
from .config import (
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
from .local_sim import InMemoryStore, run_sim
from .reference import Reference, build_reference
from .stream import (
    BACKFILL_LATENESS,
    LIVE_LATENESS,
    build_emissions,
    plan_backfill,
    plan_live,
    update_cursors,
)

RUN_NAMESPACE = uuid.UUID("0b6c3f0e-58f4-4d0c-9d8a-1d6f1f0a7c21")


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def cmd_setup(args: argparse.Namespace) -> int:
    if (ref_dir(args.env) / "properties.json").exists():
        raise S.GeneratorError(
            f"reference data already exists for ENV={args.env}; "
            f"run `make reset ENV={args.env}` first"
        )
    ref = build_reference(SCALES[args.scale], random.Random(args.seed))
    ref.write(ref_dir(args.env))
    S.write_setup(args.env, args.seed, args.scale)
    print(f"setup ENV={args.env}: {len(ref.properties)} properties, {len(ref.people)} people")
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    setup = S.read_setup(args.env)
    state = S.load_state(args.env)
    S.check_consistency(args.env, state)
    ref = Reference.read(ref_dir(args.env))
    scale = SCALES[setup["scale"]]
    seed = setup["seed"]

    if args.mode == "backfill":
        if state is not None:
            raise S.GeneratorError(
                f"ENV={args.env} already has generator state; a backfill runs once. "
                f"Run `make reset ENV={args.env}` to start again."
            )
        anchor = _ts(args.anchor) or _now()
        run_id = str(uuid.uuid5(RUN_NAMESPACE, f"{seed}:backfill"))
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
    S.write_manifest(args.env, run_id, args.mode, seed, counts)  # before "emitting" (§3.2)
    properties = {
        p.property_id: {"property_type": p.property_type, "owner_company_id": p.owner_company_id}
        for p in ref.properties
    }
    store = InMemoryStore.load(sim_store_path(), properties)
    derived = run_sim(emissions, store, load_rules(rules_path()), compacted_dir(args.env) / run_id)
    store.save(sim_store_path())
    update_cursors(state["cursors"], visits)
    state["runs"].append(run_id)
    S.save_state(args.env, state)
    print(f"{args.mode} run {run_id}: {counts} -> {derived}")
    if shortfall:
        print(f"note: {shortfall} requested check-ins did not fit (no one had room)")
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    shutil.rmtree(state_dir(args.env), ignore_errors=True)
    shutil.rmtree(data_dir(args.env), ignore_errors=True)
    warehouse_path(args.env).unlink(missing_ok=True)
    print(f"reset ENV={args.env}")
    return 0


COMMANDS = {"setup": cmd_setup, "generate": cmd_generate, "reset": cmd_reset}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="generator")
    sub = parser.add_subparsers(dest="command", required=True)
    p_setup = sub.add_parser("setup")
    p_setup.add_argument("--env", choices=ENVS, required=True)
    p_setup.add_argument("--scale", choices=sorted(SCALES), default="ci")
    p_setup.add_argument("--seed", type=int, default=42)
    p_gen = sub.add_parser("generate")
    p_gen.add_argument("--env", choices=ENVS, required=True)
    p_gen.add_argument("--mode", choices=("backfill", "live"), required=True)
    p_gen.add_argument("--n", type=int, default=50)
    p_gen.add_argument("--anchor", help="backfill end time (ISO 8601); default now")
    p_gen.add_argument("--now", help="live run time (ISO 8601); default now")
    p_reset = sub.add_parser("reset")
    p_reset.add_argument("--env", choices=ENVS, required=True)
    args = parser.parse_args(argv)
    try:
        return COMMANDS[args.command](args)
    except S.GeneratorError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
```

Create `generator/__main__.py`:

```python
import sys

from .cli import main

sys.exit(main())
```

Append to `Makefile` (tabs in recipes), and add `setup generate reset` to `.PHONY`:

```make
ENV ?= sim
SCALE ?= ci
MODE ?= backfill
N ?= 50

setup:
	$(PY) -m generator setup --env $(ENV) --scale $(SCALE)

generate:
	$(PY) -m generator generate --env $(ENV) --mode $(MODE) --n $(N)

reset:
	$(PY) -m generator reset --env $(ENV)
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `.venv/bin/pytest tests/test_sim_e2e.py -q`
Expected: 5 passed (allow up to about a minute).

- [ ] **Step 5: Try it through make**

Run: `make reset && make setup && make generate MODE=backfill && make generate MODE=live N=30`
Expected:
- the setup line, a backfill summary with `CheckInEvaluated` and `HighRiskVisitDetected` counts, and a live summary;
- no error lines.

- [ ] **Step 6: Commit**

```bash
make fmt && make test
git add generator/local_sim.py generator/cli.py generator/__main__.py Makefile tests/test_sim_e2e.py
git commit -m "Sim runner and generator CLI (setup, generate, reset)

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 12: dbt project, sources and staging

**Files:**
- Create: `dbt/dbt_project.yml`
- Create: `dbt/profiles.yml`
- Create (symlinks): `dbt/seeds/standdown_rules.csv`, `dbt/seeds/golden_cases.csv`
- Create: `dbt/macros/sources.sql`
- Create: `dbt/models/staging/stg_events.sql`
- Create: `dbt/models/staging/stg_checkins.sql`, `stg_checkouts.sql`, `stg_plant_movements.sql`
- Create: `dbt/models/staging/stg_evaluations.sql`, `stg_evaluation_origins.sql`, `stg_checkout_applied.sql`, `stg_realtime_alerts.sql`
- Create: `dbt/models/staging/stg_properties.sql`, `stg_people.sql`, `stg_companies.sql`
- Create: `dbt/models/staging/_staging.yml`
- Modify: `Makefile` (add `dbt-build` and `sim-data`)

**Interfaces:**
- Consumes: the `data/sim/compacted/<run_id>/*.ndjson` and `data/sim/ref/*.json` files from Task 11.
- Produces these staging columns, which later tasks rely on:

| Model | Columns |
|---|---|
| `stg_checkins` | event_id, person_id, visitor_company_id, property_id, checkin_time (timestamptz), received_time, method, job_type, additional_visitors |
| `stg_checkouts` | event_id, checkin_event_id, person_id, property_id, checkout_time, received_time, method |
| `stg_plant_movements` | event_id, source_property_id, dest_property_id, movement_date (date), material_type, volume, received_time |
| `stg_evaluations` | event_id, checkin_event_id, person_id, version_read, version_committed, detected_time, processed_at, breach, rule_id, origin_checkin_event_id, required_nights, nights_between, origins_seen (JSON) |
| `stg_evaluation_origins` | target_checkin_event_id, origin_checkin_event_id, origin_end, end_source, nights_between |
| `stg_checkout_applied` | event_id, checkout_event_id, checkin_event_id, person_id, version_committed, processed_at |
| `stg_realtime_alerts` | event_id, checkin_event_id, person_id, origin_property_id, dest_property_id, origin_checkin_event_id, nights_between, required_nights, rule_id, detected_time, processed_at |
| `stg_properties` | property_id, name, property_type, group_id, owner_company_id, region, lat, lon |
| `stg_people` | person_id, visitor_company_id, role, home_region |
| `stg_companies` | company_id, name, kind |

- Macros: `compacted_events()` and `ref_file(name)`.
- Make targets: `dbt-build` (any `ENV`) and `sim-data` (reset, setup, backfill and live for sim).

- [ ] **Step 1: Write `dbt/dbt_project.yml`**

```yaml
name: onside_movement
version: "0.1.0"
config-version: 2
profile: onside_movement

model-paths: ["models"]
macro-paths: ["macros"]
seed-paths: ["seeds"]
test-paths: ["tests"]
target-path: target
clean-targets: [target, dbt_packages]

vars:
  connected_window_days: 7
  missed_rate_warn: 0.25

models:
  onside_movement:
    staging:
      +materialized: table
    intermediate:
      +materialized: table
    marts:
      +materialized: table

seeds:
  onside_movement:
    standdown_rules:
      +column_types:
        rule_id: varchar
        from_type: varchar
        to_type: varchar
        scope: varchar
        nights: integer
    golden_cases:
      +column_types:
        case_id: varchar
        role: varchar
        visit_id: varchar
        property_id: varchar
        property_type: varchar
        owner_company_id: varchar
        checkin_time: varchar
        checkout_time: varchar
        expected_breach: boolean
        expected_rule_id: varchar
        expected_origin_visit_id: varchar
```

- [ ] **Step 2: Write `dbt/profiles.yml`** (Plan 2 adds `local` and `aws` targets)

```yaml
onside_movement:
  target: sim
  outputs:
    sim:
      type: duckdb
      path: "{{ env_var('REPO_ROOT') }}/warehouse/sim.duckdb"
      threads: 4
      extensions: [icu, json]
      settings:
        TimeZone: UTC
```

- [ ] **Step 3: Link the seeds to the shared CSVs**

Run:
```bash
mkdir -p dbt/seeds && cd dbt/seeds \
  && ln -s ../../shared/standdown_rules.csv standdown_rules.csv \
  && ln -s ../../shared/golden_cases.csv golden_cases.csv && cd ../..
```
Expected: `ls -l dbt/seeds` shows two symlinks.

- [ ] **Step 4: Write `dbt/macros/sources.sql`**

```sql
{% macro compacted_events() -%}
read_json('{{ env_var("DBT_COMPACTED_GLOB") }}',
    format = 'newline_delimited',
    columns = {
        'id': 'VARCHAR',
        'source': 'VARCHAR',
        'detail-type': 'VARCHAR',
        'time': 'VARCHAR',
        'detail': 'JSON'
    })
{%- endmacro %}

{% macro ref_file(name) -%}
read_json('{{ env_var("DBT_REF_ROOT") }}{{ name }}.json', format = 'array')
{%- endmacro %}
```

- [ ] **Step 5: Write the staging models**

`dbt/models/staging/stg_events.sql`:
```sql
-- All envelopes from compacted files; poison events never count (spec §5).
select
    source,
    "detail-type" as detail_type,
    detail
from {{ compacted_events() }}
where json_extract(detail, '$.poison') is null
```

`dbt/models/staging/stg_checkins.sql`:
```sql
with typed as (
    select
        detail ->> 'event_id' as event_id,
        detail ->> 'person_id' as person_id,
        detail ->> 'visitor_company_id' as visitor_company_id,
        detail ->> 'property_id' as property_id,
        cast(detail ->> 'event_time' as timestamptz) as checkin_time,
        cast(detail ->> 'received_time' as timestamptz) as received_time,
        detail ->> 'method' as method,
        detail ->> 'job_type' as job_type,
        cast(detail ->> 'additional_visitors' as integer) as additional_visitors
    from {{ ref('stg_events') }}
    where detail_type = 'CheckIn'
)

select * from typed
qualify row_number() over (partition by event_id order by received_time) = 1
```

`dbt/models/staging/stg_checkouts.sql`:
```sql
with typed as (
    select
        detail ->> 'event_id' as event_id,
        detail ->> 'checkin_event_id' as checkin_event_id,
        detail ->> 'person_id' as person_id,
        detail ->> 'property_id' as property_id,
        cast(detail ->> 'event_time' as timestamptz) as checkout_time,
        cast(detail ->> 'received_time' as timestamptz) as received_time,
        detail ->> 'method' as method
    from {{ ref('stg_events') }}
    where detail_type = 'CheckOut'
)

select * from typed
qualify row_number() over (partition by event_id order by received_time) = 1
```

`dbt/models/staging/stg_plant_movements.sql`:
```sql
with typed as (
    select
        detail ->> 'event_id' as event_id,
        detail ->> 'source_property_id' as source_property_id,
        detail ->> 'dest_property_id' as dest_property_id,
        cast(detail ->> 'movement_date' as date) as movement_date,
        detail ->> 'material_type' as material_type,
        cast(detail ->> 'volume' as integer) as volume,
        cast(detail ->> 'received_time' as timestamptz) as received_time
    from {{ ref('stg_events') }}
    where detail_type = 'PlantMovement'
)

select * from typed
qualify row_number() over (partition by event_id order by received_time) = 1
```

`dbt/models/staging/stg_evaluations.sql`:
```sql
with typed as (
    select
        detail ->> 'event_id' as event_id,
        detail ->> 'checkin_event_id' as checkin_event_id,
        detail ->> 'person_id' as person_id,
        cast(detail ->> 'version_read' as integer) as version_read,
        cast(detail ->> 'version_committed' as integer) as version_committed,
        cast(detail ->> 'detected_time' as timestamptz) as detected_time,
        cast(detail ->> 'processed_at' as timestamptz) as processed_at,
        cast(detail ->> 'breach' as boolean) as breach,
        detail ->> 'rule_id' as rule_id,
        detail ->> 'origin_checkin_event_id' as origin_checkin_event_id,
        cast(detail ->> 'required_nights' as integer) as required_nights,
        cast(detail ->> 'nights_between' as integer) as nights_between,
        detail -> 'origins_seen' as origins_seen
    from {{ ref('stg_events') }}
    where detail_type = 'CheckInEvaluated'
)

select * from typed
qualify row_number() over (partition by checkin_event_id order by processed_at) = 1
```

`dbt/models/staging/stg_evaluation_origins.sql`:
```sql
-- One row per origin the Lambda saw when it evaluated a check-in (spec §4 bound).
with exploded as (
    select
        checkin_event_id as target_checkin_event_id,
        unnest(from_json(
            origins_seen,
            '[{"checkin_event_id": "VARCHAR", "end": "VARCHAR", "end_source": "VARCHAR", "nights_between": "INTEGER"}]'
        )) as origin
    from {{ ref('stg_evaluations') }}
)

select
    target_checkin_event_id,
    struct_extract(origin, 'checkin_event_id') as origin_checkin_event_id,
    cast(struct_extract(origin, 'end') as timestamptz) as origin_end,
    struct_extract(origin, 'end_source') as end_source,
    struct_extract(origin, 'nights_between') as nights_between
from exploded
```

`dbt/models/staging/stg_checkout_applied.sql`:
```sql
with typed as (
    select
        detail ->> 'event_id' as event_id,
        detail ->> 'checkout_event_id' as checkout_event_id,
        detail ->> 'checkin_event_id' as checkin_event_id,
        detail ->> 'person_id' as person_id,
        cast(detail ->> 'version_committed' as integer) as version_committed,
        cast(detail ->> 'processed_at' as timestamptz) as processed_at
    from {{ ref('stg_events') }}
    where detail_type = 'CheckOutApplied'
)

select * from typed
qualify row_number() over (partition by checkout_event_id order by processed_at) = 1
```

`dbt/models/staging/stg_realtime_alerts.sql`:
```sql
with typed as (
    select
        detail ->> 'event_id' as event_id,
        detail ->> 'checkin_event_id' as checkin_event_id,
        detail ->> 'person_id' as person_id,
        detail ->> 'origin_property_id' as origin_property_id,
        detail ->> 'dest_property_id' as dest_property_id,
        detail ->> 'origin_checkin_event_id' as origin_checkin_event_id,
        cast(detail ->> 'nights_between' as integer) as nights_between,
        cast(detail ->> 'required_nights' as integer) as required_nights,
        detail ->> 'rule_id' as rule_id,
        cast(detail ->> 'detected_time' as timestamptz) as detected_time,
        cast(detail ->> 'processed_at' as timestamptz) as processed_at
    from {{ ref('stg_events') }}
    where detail_type = 'HighRiskVisitDetected'
)

select * from typed
qualify row_number() over (partition by checkin_event_id order by processed_at) = 1
```

`dbt/models/staging/stg_properties.sql`:
```sql
select
    property_id,
    name,
    property_type,
    group_id,
    owner_company_id,
    region,
    cast(lat as double) as lat,
    cast(lon as double) as lon
from {{ ref_file('properties') }}
```

`dbt/models/staging/stg_people.sql`:
```sql
select person_id, visitor_company_id, role, home_region
from {{ ref_file('people') }}
```

`dbt/models/staging/stg_companies.sql`:
```sql
select company_id, name, kind
from {{ ref_file('companies') }}
```

- [ ] **Step 6: Write the staging tests** `dbt/models/staging/_staging.yml`

```yaml
version: 2

models:
  - name: stg_checkins
    columns:
      - name: event_id
        data_tests: [unique, not_null]
      - name: property_id
        data_tests:
          - relationships:
              arguments: {to: ref('stg_properties'), field: property_id}
      - name: person_id
        data_tests:
          - relationships:
              arguments: {to: ref('stg_people'), field: person_id}
      - name: method
        data_tests:
          - accepted_values:
              arguments: {values: [app, geofence, qr, kiosk, link]}
  - name: stg_checkouts
    columns:
      - name: event_id
        data_tests: [unique, not_null]
      - name: checkin_event_id
        data_tests:
          - relationships:
              arguments: {to: ref('stg_checkins'), field: event_id}
      - name: method
        data_tests:
          - accepted_values:
              arguments: {values: [manual, geofence_auto, end_of_day]}
  - name: stg_plant_movements
    columns:
      - name: event_id
        data_tests: [unique, not_null]
  - name: stg_evaluations
    columns:
      - name: checkin_event_id
        data_tests: [unique, not_null]
  - name: stg_checkout_applied
    columns:
      - name: checkout_event_id
        data_tests: [unique, not_null]
  - name: stg_realtime_alerts
    columns:
      - name: checkin_event_id
        data_tests: [unique, not_null]
  - name: stg_properties
    columns:
      - name: property_id
        data_tests: [unique, not_null]
      - name: property_type
        data_tests:
          - accepted_values:
              arguments:
                values: [poultry, pork, beef, dairy, kiwifruit, vineyard, office, feed_mill]
  - name: stg_people
    columns:
      - name: person_id
        data_tests: [unique, not_null]
```

- [ ] **Step 7: Add the Makefile targets** (tabs in recipes; add `dbt-build sim-data` to `.PHONY`)

```make
DBT_ENV = DBT_PROFILES_DIR=$(REPO_ROOT)/dbt \
	DBT_COMPACTED_GLOB='$(REPO_ROOT)/data/$(ENV)/compacted/*/*.ndjson' \
	DBT_REF_ROOT='$(REPO_ROOT)/data/$(ENV)/ref/'

dbt-build:
	mkdir -p $(REPO_ROOT)/warehouse
	cd dbt && $(DBT_ENV) $(DBT) build --target $(ENV)

sim-data:
	$(MAKE) reset ENV=sim
	$(MAKE) setup ENV=sim SCALE=ci
	$(MAKE) generate ENV=sim MODE=backfill
	$(MAKE) generate ENV=sim MODE=live N=30
```

- [ ] **Step 8: Build and test the staging layer**

Run: `make sim-data && make dbt-build`
Expected:
- dbt reports `PASS` for 2 seeds, 11 models and all staging data tests;
- `Completed successfully`;
- `ERROR=0`.

If `relationships` on `stg_checkouts.checkin_event_id` fails, the generator broke the "every CheckOut has a CheckIn" guarantee. Fix it in `generator/stream.py`, not in the test.

- [ ] **Step 9: Commit**

```bash
make fmt && make test
git add dbt Makefile
git commit -m "dbt: project, sim profile, sources and staging with tests

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 13: `int_visits` with inferred check-outs

**Files:**
- Create: `dbt/models/intermediate/int_visits.sql`
- Create: `dbt/models/intermediate/_intermediate.yml` (schema and unit test)
- Create: `dbt/tests/one_checkout_per_checkin.sql`
- Create: `dbt/tests/checkout_within_48_hours.sql`

**Interfaces:**
- Consumes: `stg_checkins` and `stg_checkouts`.
- Produces `int_visits`: checkin_event_id, person_id, property_id, checkin_time, received_time, checkout_time, standdown_end, visit_end, checkout_source (`actual`, `inferred_next_checkin` or `inferred_end_of_day`).

- [ ] **Step 1: Write the unit test and singular tests first**

`dbt/models/intermediate/_intermediate.yml`:
```yaml
version: 2

models:
  - name: int_visits
    columns:
      - name: checkin_event_id
        data_tests: [unique, not_null]
      - name: checkout_source
        data_tests:
          - accepted_values:
              arguments: {values: [actual, inferred_next_checkin, inferred_end_of_day]}

unit_tests:
  - name: int_visits_infers_checkouts
    model: int_visits
    given:
      - input: ref('stg_checkins')
        rows:
          - {event_id: a, person_id: u1, property_id: p1, checkin_time: "2026-03-03 09:00:00+13", received_time: "2026-03-03 09:00:05+13"}
          - {event_id: b, person_id: u1, property_id: p2, checkin_time: "2026-03-03 13:00:00+13", received_time: "2026-03-03 13:00:05+13"}
          - {event_id: c, person_id: u1, property_id: p3, checkin_time: "2026-03-05 10:00:00+13", received_time: "2026-03-05 10:00:05+13"}
          - {event_id: d, person_id: u1, property_id: p1, checkin_time: "2026-03-06 10:00:00+13", received_time: "2026-03-06 10:00:05+13"}
      - input: ref('stg_checkouts')
        rows:
          - {event_id: x, checkin_event_id: c, checkout_time: "2026-03-05 12:00:00+13"}
    expect:
      rows:
        - {checkin_event_id: a, visit_end: "2026-03-03 13:00:00+13", checkout_source: inferred_next_checkin, standdown_end: "2026-03-03 09:00:00+13"}
        - {checkin_event_id: b, visit_end: "2026-03-03 23:59:59+13", checkout_source: inferred_end_of_day, standdown_end: "2026-03-03 13:00:00+13"}
        - {checkin_event_id: c, visit_end: "2026-03-05 12:00:00+13", checkout_source: actual, standdown_end: "2026-03-05 12:00:00+13"}
        - {checkin_event_id: d, visit_end: "2026-03-06 23:59:59+13", checkout_source: inferred_end_of_day, standdown_end: "2026-03-06 10:00:00+13"}
```

`dbt/tests/one_checkout_per_checkin.sql`:
```sql
-- Spec §3.3: at most one distinct check-out per check-in.
select checkin_event_id, count(*) as checkouts
from {{ ref('stg_checkouts') }}
group by checkin_event_id
having count(*) > 1
```

`dbt/tests/checkout_within_48_hours.sql`:
```sql
-- Spec §3.3: check-in <= check-out <= check-in + 48 hours.
select o.event_id, c.checkin_time, o.checkout_time
from {{ ref('stg_checkouts') }} as o
join {{ ref('stg_checkins') }} as c on c.event_id = o.checkin_event_id
where o.checkout_time < c.checkin_time
   or o.checkout_time > c.checkin_time + interval 48 hours
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `make dbt-build`
Expected: ERROR. The unit test and schema tests refer to the missing `int_visits` model.

- [ ] **Step 3: Implement** `dbt/models/intermediate/int_visits.sql`

```sql
-- One row per check-in. standdown_end ignores inferred check-outs (spec §4.1);
-- visit_end allows them (spec §5) and drives movements and tracing.
with checkins as (
    select * from {{ ref('stg_checkins') }}
),

checkouts as (
    select checkin_event_id, min(checkout_time) as checkout_time
    from {{ ref('stg_checkouts') }}
    group by checkin_event_id
),

joined as (
    select
        ci.event_id as checkin_event_id,
        ci.person_id,
        ci.property_id,
        ci.checkin_time,
        ci.received_time,
        co.checkout_time,
        lead(ci.checkin_time) over (
            partition by ci.person_id order by ci.checkin_time, ci.event_id
        ) as next_checkin_time,
        (
            cast((ci.checkin_time at time zone 'Pacific/Auckland') as date)
            + interval 1 day - interval 1 second
        ) at time zone 'Pacific/Auckland' as end_of_day
    from checkins as ci
    left join checkouts as co on co.checkin_event_id = ci.event_id
)

select
    checkin_event_id,
    person_id,
    property_id,
    checkin_time,
    received_time,
    checkout_time,
    case
        when checkout_time is null then checkin_time
        else greatest(checkin_time, checkout_time)
    end as standdown_end,
    case
        when checkout_time is not null then checkout_time
        when next_checkin_time is not null and next_checkin_time < end_of_day then next_checkin_time
        else end_of_day
    end as visit_end,
    case
        when checkout_time is not null then 'actual'
        when next_checkin_time is not null and next_checkin_time < end_of_day
            then 'inferred_next_checkin'
        else 'inferred_end_of_day'
    end as checkout_source
from joined
```

- [ ] **Step 4: Run them to confirm they pass**

Run: `make dbt-build`
Expected: `int_visits_infers_checkouts` passes, both singular tests pass, `ERROR=0`.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add dbt
git commit -m "dbt: int_visits with inferred check-outs and check-out guarantees

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 14: The `standdown` macro, `fct_high_risk_visits` and golden parity in dbt

**Files:**
- Create: `dbt/macros/standdown.sql`
- Create: `dbt/models/staging/golden/stg_golden_visits.sql`
- Create: `dbt/models/staging/golden/stg_golden_properties.sql`
- Create: `dbt/models/marts/fct_high_risk_visits.sql`
- Create: `dbt/models/marts/_marts.yml`
- Create: `dbt/tests/golden_standdown_matches.sql`
- Create: `dbt/tests/rules_unique_scope.sql`
- Create: `dbt/tests/rules_nights_range.sql`

**Interfaces:**
- Consumes:
  - `int_visits` (person_id, checkin_event_id, property_id, checkin_time, standdown_end);
  - `stg_properties` (property_id, property_type, owner_company_id);
  - the seeds `standdown_rules` and `golden_cases`.
- Produces:
  - Macro `standdown(visits, properties, rules)`. It returns one row per breaching check-in: person_id, checkin_event_id, origin_checkin_event_id, origin_property_id, dest_property_id, rule_id, required_nights, nights_between.
  - Model `fct_high_risk_visits`, with the same columns.

- [ ] **Step 1: Write the golden staging models and the failing tests**

`dbt/models/staging/golden/stg_golden_visits.sql`:
```sql
-- Golden cases reshaped into the relations the standdown macro expects (spec §6.3).
select
    case_id as person_id,
    case_id || ':' || visit_id as checkin_event_id,
    case_id || ':' || property_id as property_id,
    cast(checkin_time as timestamptz) as checkin_time,
    case
        when checkout_time is null or checkout_time = '' then cast(checkin_time as timestamptz)
        else greatest(cast(checkin_time as timestamptz), cast(checkout_time as timestamptz))
    end as standdown_end,
    role,
    expected_breach,
    nullif(expected_rule_id, '') as expected_rule_id,
    case
        when expected_origin_visit_id is null or expected_origin_visit_id = '' then null
        else case_id || ':' || expected_origin_visit_id
    end as expected_origin_checkin_event_id
from {{ ref('golden_cases') }}
```

`dbt/models/staging/golden/stg_golden_properties.sql`:
```sql
select distinct
    case_id || ':' || property_id as property_id,
    property_type,
    owner_company_id
from {{ ref('golden_cases') }}
```

`dbt/tests/golden_standdown_matches.sql`:
```sql
-- The SQL stand-down logic must agree with rules.py on every golden case.
with actual as (
    {{ standdown(ref('stg_golden_visits'), ref('stg_golden_properties'), ref('standdown_rules')) }}
),

targets as (
    select * from {{ ref('stg_golden_visits') }} where role = 'target'
)

select
    t.checkin_event_id,
    t.expected_breach,
    t.expected_rule_id,
    t.expected_origin_checkin_event_id,
    a.rule_id,
    a.origin_checkin_event_id
from targets as t
left join actual as a on a.checkin_event_id = t.checkin_event_id
where (a.checkin_event_id is not null) <> t.expected_breach
   or coalesce(a.rule_id, '') <> coalesce(t.expected_rule_id, '')
   or coalesce(a.origin_checkin_event_id, '') <> coalesce(t.expected_origin_checkin_event_id, '')
```

`dbt/tests/rules_unique_scope.sql`:
```sql
select from_type, to_type, scope, count(*) as n
from {{ ref('standdown_rules') }}
group by from_type, to_type, scope
having count(*) > 1
```

`dbt/tests/rules_nights_range.sql`:
```sql
select * from {{ ref('standdown_rules') }} where nights < 1 or nights > 14
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `make dbt-build`
Expected: ERROR in `golden_standdown_matches`: `'standdown' is undefined`.

- [ ] **Step 3: Implement the macro** `dbt/macros/standdown.sql`

```sql
{#
  Spec §4: one row per breaching check-in.
  visits:     person_id, checkin_event_id, property_id, checkin_time, standdown_end
  properties: property_id, property_type, owner_company_id
  rules:      rule_id, from_type, to_type, scope, nights
#}
{% macro standdown(visits, properties, rules) -%}
with v as (
    select
        v.person_id,
        v.checkin_event_id,
        v.property_id,
        v.checkin_time,
        v.standdown_end,
        p.property_type,
        p.owner_company_id
    from {{ visits }} as v
    join {{ properties }} as p on p.property_id = v.property_id
),

pairs as (
    select
        d.person_id,
        d.checkin_event_id as target_id,
        o.checkin_event_id as origin_id,
        o.property_id as origin_property_id,
        d.property_id as dest_property_id,
        o.standdown_end as origin_end,
        greatest(0, datediff(
            'day',
            cast((o.standdown_end at time zone 'Pacific/Auckland') as date),
            cast((d.checkin_time at time zone 'Pacific/Auckland') as date)
        )) as nights_between,
        o.property_type as from_type,
        d.property_type as to_type,
        case when o.owner_company_id = d.owner_company_id then 'internal' else 'external' end
            as pair_scope
    from v as d
    join v as o
        on o.person_id = d.person_id
        and (
            o.checkin_time < d.checkin_time
            or (o.checkin_time = d.checkin_time and o.checkin_event_id < d.checkin_event_id)
        )
        and o.property_id <> d.property_id
),

ruled as (
    select
        pairs.*,
        r.rule_id,
        r.nights as required_nights,
        row_number() over (
            partition by pairs.target_id, pairs.origin_id
            order by case when r.scope = pairs.pair_scope then 0 else 1 end
        ) as rule_rank
    from pairs
    join {{ rules }} as r
        on r.from_type = pairs.from_type
        and r.to_type = pairs.to_type
        and (r.scope = pairs.pair_scope or r.scope = 'any')
    where pairs.nights_between <= 14
),

breaches as (
    select * from ruled
    where rule_rank = 1 and nights_between < required_nights
),

picked as (
    select
        *,
        row_number() over (
            partition by target_id
            order by
                required_nights desc,
                required_nights - nights_between desc,
                origin_end desc,
                origin_id desc
        ) as pick
    from breaches
)

select
    person_id,
    target_id as checkin_event_id,
    origin_id as origin_checkin_event_id,
    origin_property_id,
    dest_property_id,
    rule_id,
    required_nights,
    nights_between
from picked
where pick = 1
{%- endmacro %}
```

`dbt/models/marts/fct_high_risk_visits.sql`:
```sql
{{ standdown(ref('int_visits'), ref('stg_properties'), ref('standdown_rules')) }}
```

`dbt/models/marts/_marts.yml`:
```yaml
version: 2

models:
  - name: fct_high_risk_visits
    columns:
      - name: checkin_event_id
        data_tests: [unique, not_null]
```

- [ ] **Step 4: Run them to confirm they pass**

Run: `make dbt-build`
Expected: `golden_standdown_matches` PASS (0 rows), the rules tests PASS, `fct_high_risk_visits` builds, `ERROR=0`.

If the golden test fails, its output rows name the case. Compare them with the Python result from `.venv/bin/pytest tests/test_rules_evaluate.py -k <case>`. The SQL must change, not the CSV.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add dbt
git commit -m "dbt: standdown macro, fct_high_risk_visits, golden parity with rules.py

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 15: `fct_movements` and `fct_network_edges_monthly`

**Files:**
- Create: `dbt/models/marts/fct_movements.sql`
- Create: `dbt/models/marts/fct_network_edges_monthly.sql`
- Modify: `dbt/models/marts/_marts.yml`
- Create: `dbt/tests/movement_origin_not_dest.sql`

**Interfaces:**
- Consumes: `int_visits` and `stg_properties`.
- Produces:
  - `fct_movements`: origin_checkin_event_id, dest_checkin_event_id, person_id, origin_property_id, dest_property_id, departure_time, arrival_time, gap_hours, nights_between, distance_km, scope, month (date), connected (boolean).
  - `fct_network_edges_monthly`: month, origin_property_id, dest_property_id, movements, avg_daily_movements.

- [ ] **Step 1: Write the failing test and schema entries**

`dbt/tests/movement_origin_not_dest.sql`:
```sql
select * from {{ ref('fct_movements') }} where origin_property_id = dest_property_id
```

Append to the `models:` list in `dbt/models/marts/_marts.yml`:
```yaml
  - name: fct_movements
    columns:
      - name: dest_checkin_event_id
        data_tests: [unique, not_null]
      - name: gap_hours
        data_tests: [not_null]
  - name: fct_network_edges_monthly
    columns:
      - name: movements
        data_tests: [not_null]
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `make dbt-build`
Expected: ERROR. `fct_movements` was not found.

- [ ] **Step 3: Implement** `dbt/models/marts/fct_movements.sql`

```sql
-- Consecutive visits by the same person at different properties (arXiv 2602.21491;
-- spec §5). Timing uses visit_end, so inferred check-outs are allowed here.
with v as (
    select
        v.*,
        p.owner_company_id,
        p.lat,
        p.lon
    from {{ ref('int_visits') }} as v
    join {{ ref('stg_properties') }} as p on p.property_id = v.property_id
),

seq as (
    select
        *,
        lag(checkin_event_id) over w as prev_checkin_event_id,
        lag(property_id) over w as prev_property_id,
        lag(visit_end) over w as prev_visit_end,
        lag(owner_company_id) over w as prev_owner_company_id,
        lag(lat) over w as prev_lat,
        lag(lon) over w as prev_lon
    from v
    window w as (partition by person_id order by checkin_time, checkin_event_id)
),

moves as (
    select
        prev_checkin_event_id as origin_checkin_event_id,
        checkin_event_id as dest_checkin_event_id,
        person_id,
        prev_property_id as origin_property_id,
        property_id as dest_property_id,
        prev_visit_end as departure_time,
        checkin_time as arrival_time,
        greatest(0, epoch(checkin_time) - epoch(prev_visit_end)) / 3600.0 as gap_hours,
        greatest(0, datediff(
            'day',
            cast((prev_visit_end at time zone 'Pacific/Auckland') as date),
            cast((checkin_time at time zone 'Pacific/Auckland') as date)
        )) as nights_between,
        2 * 6371 * asin(sqrt(
            pow(sin(radians(lat - prev_lat) / 2), 2)
            + cos(radians(prev_lat)) * cos(radians(lat)) * pow(sin(radians(lon - prev_lon) / 2), 2)
        )) as distance_km,
        case when prev_owner_company_id = owner_company_id then 'internal' else 'external' end
            as scope,
        cast(date_trunc('month', checkin_time at time zone 'Pacific/Auckland') as date) as month
    from seq
    where prev_checkin_event_id is not null
      and prev_property_id <> property_id
)

select
    *,
    gap_hours < 24 * {{ var('connected_window_days') }} as connected
from moves
```

`dbt/models/marts/fct_network_edges_monthly.sql`:
```sql
-- Monthly directed edges; weight = average daily movements, as in the paper.
select
    month,
    origin_property_id,
    dest_property_id,
    count(*) as movements,
    count(*) / cast(day(last_day(month)) as double) as avg_daily_movements
from {{ ref('fct_movements') }}
group by month, origin_property_id, dest_property_id
```

- [ ] **Step 4: Run them to confirm they pass, then sanity-check the distance share**

Run: `make dbt-build`
Expected: `ERROR=0`.

Run:
```bash
.venv/bin/python -c "import duckdb; print(duckdb.connect('warehouse/sim.duckdb', read_only=True).sql('select round(avg((distance_km < 10)::int), 2) as near_share, count(*) as moves from fct_movements'))"
```
Expected: `near_share` between 0.60 and 0.90.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add dbt
git commit -m "dbt: movements and monthly network edges

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 16: Reconciliation and the parity tests

**Files:**
- Create: `dbt/models/marts/fct_alert_reconciliation.sql`
- Modify: `dbt/models/marts/_marts.yml` (schema and unit test)
- Create: `dbt/tests/recon_no_false_alerts.sql`
- Create: `dbt/tests/recon_all_misses_explained.sql`
- Create: `dbt/tests/recon_missed_rate.sql`
- Create: `dbt/tests/alert_iff_breach.sql`
- Create: `dbt/tests/evaluation_completeness.sql`
- Create: `dbt/tests/origins_seen_matches_batch.sql`
- Create: `dbt/tests/origin_end_source_matches_versions.sql`

**Interfaces:**
- Consumes: `fct_high_risk_visits`, `stg_evaluations`, `stg_evaluation_origins`, `stg_checkout_applied` and `stg_checkins`.
- Produces `fct_alert_reconciliation` with these columns:
  - checkin_event_id;
  - status (`matched`, `missed` or `false_alert`);
  - cause (`origin_not_seen`, `origin_end_not_seen`, `not_evaluated`, `unexplained` or null);
  - sub_cause (`offline`, `reordered` or null);
  - same_attribution, late_alert;
  - batch_origin_checkin_event_id, batch_rule_id, realtime_origin_checkin_event_id, realtime_rule_id;
  - version_read, origin_checkin_version, origin_checkout_version.

- [ ] **Step 1: Write the unit test.** Append it to `dbt/models/marts/_marts.yml`, at the top level next to `models:`:

```yaml
unit_tests:
  - name: reconciliation_classifies_every_case
    model: fct_alert_reconciliation
    given:
      - input: ref('fct_high_risk_visits')
        rows:
          - {checkin_event_id: d1, origin_checkin_event_id: o1, rule_id: R02}
          - {checkin_event_id: d2, origin_checkin_event_id: o2, rule_id: R02}
          - {checkin_event_id: d3, origin_checkin_event_id: o3, rule_id: R05}
          - {checkin_event_id: d4, origin_checkin_event_id: o4, rule_id: R02}
          - {checkin_event_id: d5, origin_checkin_event_id: o5, rule_id: R02}
          - {checkin_event_id: d6, origin_checkin_event_id: o6, rule_id: R02}
      - input: ref('stg_evaluations')
        rows:
          - {checkin_event_id: d1, breach: true, rule_id: R02, origin_checkin_event_id: o1, version_read: 5, version_committed: 6}
          - {checkin_event_id: d2, breach: false, version_read: 3, version_committed: 4}
          - {checkin_event_id: o2, breach: false, version_read: 4, version_committed: 5}
          - {checkin_event_id: d3, breach: false, version_read: 3, version_committed: 4}
          - {checkin_event_id: o3, breach: false, version_read: 1, version_committed: 2}
          - {checkin_event_id: d5, breach: false, version_read: 9, version_committed: 10}
          - {checkin_event_id: o5, breach: false, version_read: 1, version_committed: 2}
          - {checkin_event_id: d6, breach: false, version_read: 3, version_committed: 4}
          - {checkin_event_id: o6, breach: false, version_read: 4, version_committed: 5}
          - {checkin_event_id: d7, breach: true, rule_id: R02, origin_checkin_event_id: o7, version_read: 2, version_committed: 3}
      - input: ref('stg_evaluation_origins')
        rows:
          - {target_checkin_event_id: d3, origin_checkin_event_id: o3, end_source: checkin}
      - input: ref('stg_checkout_applied')
        rows:
          - {checkin_event_id: o3, checkout_event_id: o3-out, version_committed: 5}
      - input: ref('stg_checkins')
        rows:
          - {event_id: d1, checkin_time: "2026-03-03 09:00:00+13", received_time: "2026-03-03 11:00:00+13"}
          - {event_id: d2, checkin_time: "2026-03-03 09:00:00+13", received_time: "2026-03-03 09:00:10+13"}
          - {event_id: o2, checkin_time: "2026-03-02 09:00:00+13", received_time: "2026-03-04 09:00:00+13"}
          - {event_id: d6, checkin_time: "2026-03-03 09:00:00+13", received_time: "2026-03-03 09:00:30+13"}
          - {event_id: o6, checkin_time: "2026-03-02 09:00:00+13", received_time: "2026-03-02 09:00:10+13"}
    expect:
      rows:
        - {checkin_event_id: d1, status: matched, cause: null, sub_cause: null, same_attribution: true, late_alert: true}
        - {checkin_event_id: d2, status: missed, cause: origin_not_seen, sub_cause: offline, same_attribution: null, late_alert: null}
        - {checkin_event_id: d3, status: missed, cause: origin_end_not_seen, sub_cause: null, same_attribution: null, late_alert: null}
        - {checkin_event_id: d4, status: missed, cause: not_evaluated, sub_cause: null, same_attribution: null, late_alert: null}
        - {checkin_event_id: d5, status: missed, cause: unexplained, sub_cause: null, same_attribution: null, late_alert: null}
        - {checkin_event_id: d6, status: missed, cause: origin_not_seen, sub_cause: reordered, same_attribution: null, late_alert: null}
        - {checkin_event_id: d7, status: false_alert, cause: null, sub_cause: null, same_attribution: null, late_alert: null}
```

Why each row expects what it does:

| Row | Reasoning |
|---|---|
| d1 | Matched; received 2 h after the check-in, so `late_alert` |
| d2 | o2 committed at version 5 > 3 and is not in `origins_seen`; o2 was received after d2, so `offline` |
| d3 | o3 was seen with `end_source=checkin`, and its check-out was applied at version 5 > 3 |
| d4 | No evaluation |
| d5 | o5 committed at version 2 ≤ 9 but was not seen, so the proof fails |
| d6 | Like d2, but o6 was received before d6, so `reordered` |
| d7 | Realtime-only breach |

- [ ] **Step 2: Write the singular tests**

`dbt/tests/recon_no_false_alerts.sql`:
```sql
-- Spec §4: realtime can only miss, never false-alert.
select * from {{ ref('fct_alert_reconciliation') }} where status = 'false_alert'
```

`dbt/tests/recon_all_misses_explained.sql`:
```sql
select * from {{ ref('fct_alert_reconciliation') }}
where cause in ('not_evaluated', 'unexplained')
```

`dbt/tests/recon_missed_rate.sql`:
```sql
{{ config(severity='warn') }}
with s as (
    select
        count(*) filter (where status = 'missed') as missed,
        count(*) filter (where status in ('matched', 'missed')) as batch_breaches
    from {{ ref('fct_alert_reconciliation') }}
)
select * from s
where batch_breaches > 0
  and missed / cast(batch_breaches as double) > {{ var('missed_rate_warn') }}
```

`dbt/tests/alert_iff_breach.sql`:
```sql
select e.checkin_event_id as evaluated_id, a.checkin_event_id as alert_id, e.breach
from {{ ref('stg_evaluations') }} as e
full outer join {{ ref('stg_realtime_alerts') }} as a on a.checkin_event_id = e.checkin_event_id
where coalesce(e.breach, false) <> (a.checkin_event_id is not null)
```

`dbt/tests/evaluation_completeness.sql`:
```sql
select 'checkin_without_evaluation' as problem, c.event_id
from {{ ref('stg_checkins') }} as c
left join {{ ref('stg_evaluations') }} as e on e.checkin_event_id = c.event_id
where e.checkin_event_id is null
union all
select 'checkout_without_applied' as problem, o.event_id
from {{ ref('stg_checkouts') }} as o
left join {{ ref('stg_checkout_applied') }} as a on a.checkout_event_id = o.event_id
where a.checkout_event_id is null
```

`dbt/tests/origins_seen_matches_batch.sql`:
```sql
-- Spec §6.3: origins_seen = batch candidates within 16 local days whose own check-in was
-- committed at or before the version the Lambda read.
with ev as (
    select * from {{ ref('stg_evaluations') }}
),

ci as (
    select event_id, person_id, property_id, checkin_time from {{ ref('stg_checkins') }}
),

d as (
    select ev.checkin_event_id, ev.version_read, ci.person_id, ci.property_id, ci.checkin_time
    from ev
    join ci on ci.event_id = ev.checkin_event_id
),

expected as (
    select d.checkin_event_id as target_checkin_event_id, o.event_id as origin_checkin_event_id
    from d
    join ci as o
        on o.person_id = d.person_id
        and (
            o.checkin_time < d.checkin_time
            or (o.checkin_time = d.checkin_time and o.event_id < d.checkin_event_id)
        )
        and o.property_id <> d.property_id
        and datediff(
            'day',
            cast((o.checkin_time at time zone 'Pacific/Auckland') as date),
            cast((d.checkin_time at time zone 'Pacific/Auckland') as date)
        ) <= 16
    join ev as oe
        on oe.checkin_event_id = o.event_id
        and oe.version_committed <= d.version_read
),

actual as (
    select target_checkin_event_id, origin_checkin_event_id
    from {{ ref('stg_evaluation_origins') }}
),

missing as (
    select * from expected
    except
    select * from actual
),

unexpected as (
    select * from actual
    except
    select * from expected
)

select 'missing' as problem, * from missing
union all
select 'unexpected' as problem, * from unexpected
```

`dbt/tests/origin_end_source_matches_versions.sql`:
```sql
-- An origin's end_source is 'checkout' exactly when its check-out was applied before the read.
select os.*, e.version_read, oa.version_committed
from {{ ref('stg_evaluation_origins') }} as os
join {{ ref('stg_evaluations') }} as e on e.checkin_event_id = os.target_checkin_event_id
left join (
    select checkin_event_id, min(version_committed) as version_committed
    from {{ ref('stg_checkout_applied') }}
    group by checkin_event_id
) as oa on oa.checkin_event_id = os.origin_checkin_event_id
where (os.end_source = 'checkout') <> coalesce(oa.version_committed <= e.version_read, false)
```

- [ ] **Step 3: Run them to confirm they fail**

Run: `make dbt-build`
Expected: ERROR. `fct_alert_reconciliation` was not found.

- [ ] **Step 4: Implement** `dbt/models/marts/fct_alert_reconciliation.sql`

```sql
-- Spec §6.2: realtime vs batch, with a provable cause for every miss.
with batch as (
    select * from {{ ref('fct_high_risk_visits') }}
),

evaluations as (
    select * from {{ ref('stg_evaluations') }}
),

checkins as (
    select event_id, checkin_time, received_time from {{ ref('stg_checkins') }}
),

checkout_versions as (
    select checkin_event_id, min(version_committed) as version_committed
    from {{ ref('stg_checkout_applied') }}
    group by checkin_event_id
),

keys as (
    select checkin_event_id from batch
    union
    select checkin_event_id from evaluations where breach
),

joined as (
    select
        k.checkin_event_id,
        b.checkin_event_id is not null as batch_breach,
        coalesce(e.breach, false) as realtime_breach,
        e.checkin_event_id is not null as evaluated,
        e.version_read,
        b.origin_checkin_event_id as batch_origin_checkin_event_id,
        b.rule_id as batch_rule_id,
        e.origin_checkin_event_id as realtime_origin_checkin_event_id,
        e.rule_id as realtime_rule_id
    from keys as k
    left join batch as b on b.checkin_event_id = k.checkin_event_id
    left join evaluations as e on e.checkin_event_id = k.checkin_event_id
),

evidence as (
    select
        j.*,
        os.origin_checkin_event_id is not null as origin_seen,
        os.end_source as origin_end_source,
        oe.version_committed as origin_checkin_version,
        oc.version_committed as origin_checkout_version,
        d.checkin_time,
        d.received_time,
        o.received_time as origin_received_time
    from joined as j
    left join {{ ref('stg_evaluation_origins') }} as os
        on os.target_checkin_event_id = j.checkin_event_id
        and os.origin_checkin_event_id = j.batch_origin_checkin_event_id
    left join evaluations as oe on oe.checkin_event_id = j.batch_origin_checkin_event_id
    left join checkout_versions as oc on oc.checkin_event_id = j.batch_origin_checkin_event_id
    left join checkins as d on d.event_id = j.checkin_event_id
    left join checkins as o on o.event_id = j.batch_origin_checkin_event_id
),

classified as (
    select
        *,
        case
            when batch_breach and realtime_breach then 'matched'
            when realtime_breach then 'false_alert'
            else 'missed'
        end as status,
        case
            when realtime_breach then null
            when not evaluated then 'not_evaluated'
            when not origin_seen and origin_checkin_version > version_read then 'origin_not_seen'
            when origin_seen and origin_end_source = 'checkin'
                and origin_checkout_version > version_read then 'origin_end_not_seen'
            else 'unexplained'
        end as cause
    from evidence
)

select
    checkin_event_id,
    status,
    cause,
    case
        when cause = 'origin_not_seen' and origin_received_time > received_time then 'offline'
        when cause = 'origin_not_seen' then 'reordered'
    end as sub_cause,
    case
        when status = 'matched'
            then batch_rule_id = realtime_rule_id
            and batch_origin_checkin_event_id = realtime_origin_checkin_event_id
    end as same_attribution,
    case
        when status = 'matched' then epoch(received_time) - epoch(checkin_time) > 3600
    end as late_alert,
    batch_origin_checkin_event_id,
    batch_rule_id,
    realtime_origin_checkin_event_id,
    realtime_rule_id,
    version_read,
    origin_checkin_version,
    origin_checkout_version
from classified
```

Note the `cause` rule. When `realtime_breach` is true the cause is null, whether the row is matched or a false alert. When both sides breach, `realtime_breach` is also true, so matched rows get a null cause too.

- [ ] **Step 5: Run them to confirm they pass**

Run: `make dbt-build`
Expected:
- `reconciliation_classifies_every_case` PASS;
- every singular test PASS;
- `recon_missed_rate` PASS or WARN, but never ERROR;
- `ERROR=0`.

If `origins_seen_matches_batch` or `recon_all_misses_explained` fails, that is a real parity bug between `processor.py` and the SQL. Debug it with the returned rows, using superpowers:systematic-debugging, and fix the code rather than the test.

- [ ] **Step 6: Look at the reconciliation summary** (this is the demo story)

Run:
```bash
.venv/bin/python -c "import duckdb; print(duckdb.connect('warehouse/sim.duckdb', read_only=True).sql('select status, cause, sub_cause, count(*) n from fct_alert_reconciliation group by all order by n desc'))"
```
Expected: mostly `matched`, some `missed / origin_not_seen / offline`, possibly a few `missed / origin_end_not_seen`, and no `false_alert`, `not_evaluated` or `unexplained`.

- [ ] **Step 7: Commit**

```bash
make fmt && make test
git add dbt
git commit -m "dbt: alert reconciliation with version-proven causes and parity tests

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 17: `dim_property_network` (Python model, networkx)

**Files:**
- Create: `dbt/models/marts/dim_property_network.py`
- Modify: `dbt/models/marts/_marts.yml`
- Create: `dbt/tests/pagerank_sums_to_one.sql`

**Interfaces:**
- Consumes: `fct_movements` and `stg_properties`.
- Produces `dim_property_network`: property_id, property_type, region, in_degree, out_degree, weighted_in, weighted_out, pagerank, pagerank_rank.

- [ ] **Step 1: Write the failing test and schema entry**

`dbt/tests/pagerank_sums_to_one.sql`:
```sql
select sum(pagerank) as total
from {{ ref('dim_property_network') }}
having abs(sum(pagerank) - 1) > 1e-6
```

Append to `models:` in `_marts.yml`:
```yaml
  - name: dim_property_network
    columns:
      - name: property_id
        data_tests: [unique, not_null]
      - name: pagerank_rank
        data_tests: [unique, not_null]
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `make dbt-build`
Expected: ERROR. `dim_property_network` was not found.

- [ ] **Step 3: Implement** `dbt/models/marts/dim_property_network.py`

```python
"""Key property identification (spec §5): degree, strength and weighted PageRank."""

import networkx as nx
import pandas as pd


def model(dbt, session):
    dbt.config(materialized="table")
    movements = dbt.ref("fct_movements").df()
    properties = dbt.ref("stg_properties").df()[["property_id", "property_type", "region"]]

    edges = (
        movements.groupby(["origin_property_id", "dest_property_id"])
        .size()
        .reset_index(name="movements")
    )
    graph = nx.DiGraph()
    graph.add_nodes_from(properties["property_id"])
    for row in edges.itertuples(index=False):
        graph.add_edge(row.origin_property_id, row.dest_property_id, weight=float(row.movements))

    pagerank = nx.pagerank(graph, weight="weight")
    rows = [
        {
            "property_id": node,
            "in_degree": graph.in_degree(node),
            "out_degree": graph.out_degree(node),
            "weighted_in": graph.in_degree(node, weight="weight"),
            "weighted_out": graph.out_degree(node, weight="weight"),
            "pagerank": pagerank[node],
        }
        for node in graph.nodes
    ]
    df = properties.merge(pd.DataFrame(rows), on="property_id", how="left")
    df["pagerank_rank"] = df["pagerank"].rank(ascending=False, method="first").astype(int)
    return df.sort_values("pagerank_rank").reset_index(drop=True)
```

- [ ] **Step 4: Run them to confirm they pass**

Run: `make dbt-build`
Expected: `dim_property_network` builds, `pagerank_sums_to_one` PASS, `ERROR=0`.

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add dbt
git commit -m "dbt: property network metrics with weighted PageRank

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 18: Tracing (hop-limited earliest arrival)

**Files:**
- Create: `tracing/__init__.py` (empty)
- Create: `tracing/core.py`
- Create: `tracing/cli.py`
- Create: `tracing/__main__.py`
- Modify: `Makefile` (add `trace`)
- Test: `tests/test_tracing_core.py`
- Test: `tests/test_tracing_cli.py`

**Interfaces:**
- Consumes (CLI): `fct_movements` (connected rows only), `stg_plant_movements` and `stg_properties` in a DuckDB file.
- Produces:
  - `tracing.core`:
    - `Edge(src: str, dst: str, departure: float, arrival: float)` (epoch seconds);
    - `Reached(property_id: str, arrival: float, hops: int, path: tuple[str, ...])`;
    - `trace(edges, seed, start, max_hops) -> list[Reached]`, sorted by (arrival, property_id).
  - `tracing.cli`: `load_edges(con) -> list[Edge]` and `main(argv=None) -> int` (exit code 2 on an unknown property).
  - `make trace PROPERTY=… START=… [HOPS=3]`.

- [ ] **Step 1: Write the failing core tests** `tests/test_tracing_core.py`

```python
from tracing.core import Edge, trace


def by_id(results):
    return {r.property_id: r for r in results}


def test_chain_respects_max_hops():
    edges = [Edge("S", "A", 0, 10), Edge("A", "B", 20, 30)]
    one = by_id(trace(edges, "S", 0, 1))
    assert set(one) == {"S", "A"}
    two = by_id(trace(edges, "S", 0, 2))
    assert two["B"].path == ("S", "A", "B") and two["B"].hops == 2


def test_edges_must_depart_after_arrival_at_their_origin():
    edges = [Edge("S", "A", 0, 10), Edge("A", "B", 5, 30)]  # leaves A before infection arrives
    assert "B" not in by_id(trace(edges, "S", 0, 3))


def test_earliest_arrival_wins():
    edges = [Edge("S", "A", 0, 50), Edge("S", "B", 0, 5), Edge("B", "A", 6, 20)]
    result = by_id(trace(edges, "S", 0, 3))
    assert result["A"].arrival == 20
    assert result["A"].path == ("S", "B", "A")


def test_witness_path_is_consistent_with_hop_limit():
    # Review-2 counterexample: u improves in round 2 while v used u's round-1 value.
    edges = [
        Edge("S", "u", 0, 10),
        Edge("S", "x", 0, 2),
        Edge("x", "u", 3, 5),
        Edge("u", "v", 10, 20),
    ]
    result = by_id(trace(edges, "S", 0, 2))
    assert result["u"].arrival == 5 and result["u"].path == ("S", "x", "u")
    assert result["v"].arrival == 20 and result["v"].path == ("S", "u", "v")
    assert all(r.hops <= 2 for r in result.values())


def test_seed_without_edges_returns_only_the_seed():
    assert [r.property_id for r in trace([], "S", 0, 3)] == ["S"]
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_tracing_core.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'tracing'`.

- [ ] **Step 3: Implement.** Create an empty `tracing/__init__.py` and `tracing/core.py`:

```python
"""Hop-limited earliest-arrival tracing over time-respecting edges (spec §5 Tracing)."""

from __future__ import annotations

from bisect import bisect_left
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

INF = float("inf")


@dataclass(frozen=True)
class Edge:
    src: str
    dst: str
    departure: float
    arrival: float


@dataclass(frozen=True)
class Reached:
    property_id: str
    arrival: float
    hops: int
    path: tuple[str, ...]


def trace(edges: Iterable[Edge], seed: str, start: float, max_hops: int) -> list[Reached]:
    out: dict[str, list[Edge]] = defaultdict(list)
    for edge in edges:
        if edge.arrival >= edge.departure:
            out[edge.src].append(edge)
    for lst in out.values():
        lst.sort(key=lambda e: e.departure)
    departures = {src: [e.departure for e in lst] for src, lst in out.items()}

    arr: dict[str, float] = {seed: start}
    improved_at: list[dict[str, str]] = [{}]  # improved_at[r][v] = predecessor in layer r-1
    frontier = {seed}
    for _ in range(max_hops):
        previous = dict(arr)  # arr_{r-1}: every relaxation this round reads it
        best: dict[str, tuple[float, str]] = {}
        for u in sorted(frontier):
            lst = out.get(u)
            if not lst:
                continue
            for edge in lst[bisect_left(departures[u], previous[u]) :]:
                if edge.arrival >= previous.get(edge.dst, INF):
                    continue
                current = best.get(edge.dst)
                if current is None or (edge.arrival, u) < current:
                    best[edge.dst] = (edge.arrival, u)
        if not best:
            break
        layer = {}
        for v, (arrival, u) in best.items():
            arr[v] = arrival
            layer[v] = u
        improved_at.append(layer)
        frontier = set(best)

    final = len(improved_at) - 1

    def path_to(v: str) -> tuple[str, ...]:
        nodes = [v]
        layer = final
        while True:
            found = next((k for k in range(layer, 0, -1) if v in improved_at[k]), 0)
            if found == 0:
                break
            v = improved_at[found][v]
            nodes.append(v)
            layer = found - 1
        return tuple(reversed(nodes))

    results = []
    for v, arrival in arr.items():
        path = path_to(v)
        results.append(Reached(v, arrival, len(path) - 1, path))
    return sorted(results, key=lambda r: (r.arrival, r.property_id))
```

- [ ] **Step 4: Run the core tests to confirm they pass**

Run: `.venv/bin/pytest tests/test_tracing_core.py -q`
Expected: 5 passed.

- [ ] **Step 5: Write the failing CLI tests** `tests/test_tracing_cli.py`

```python
import duckdb
import pytest

from tracing.cli import main


@pytest.fixture()
def db(tmp_path):
    path = tmp_path / "w.duckdb"
    con = duckdb.connect(str(path))
    con.execute("create table stg_properties (property_id varchar, property_type varchar)")
    con.execute("insert into stg_properties values ('P1','poultry'), ('P2','poultry'), ('P3','office')")
    con.execute(
        "create table fct_movements (origin_property_id varchar, dest_property_id varchar, "
        "departure_time timestamptz, arrival_time timestamptz, connected boolean)"
    )
    con.execute(
        "insert into fct_movements values "
        "('P1','P2','2026-03-03 10:00:00+13','2026-03-03 12:00:00+13', true)"
    )
    con.execute(
        "create table stg_plant_movements (source_property_id varchar, dest_property_id varchar, "
        "movement_date date)"
    )
    con.close()
    return path


def test_traces_downstream_properties(db, capsys):
    code = main(["--db", str(db), "--property", "P1", "--start", "2026-03-03T09:00:00+13:00"])
    assert code == 0
    out = capsys.readouterr().out
    assert "P2" in out and "P1 -> P2" in out


def test_unknown_property_is_a_clear_error(db, capsys):
    code = main(["--db", str(db), "--property", "NOPE", "--start", "2026-03-03T09:00:00+13:00"])
    assert code == 2
    assert "unknown property NOPE" in capsys.readouterr().err


def test_property_without_edges_returns_only_itself(db, capsys):
    code = main(["--db", str(db), "--property", "P3", "--start", "2026-03-03T09:00:00+13:00"])
    assert code == 0
    assert "1 properties reached" in capsys.readouterr().out
```

- [ ] **Step 6: Run them to confirm they fail**

Run: `.venv/bin/pytest tests/test_tracing_cli.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'tracing.cli'`.

- [ ] **Step 7: Implement** `tracing/cli.py` and `tracing/__main__.py`

```python
"""Command line tracing over the warehouse (spec §5 Tracing; Plan 3 adds a Dagster job)."""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import duckdb

from .core import Edge, trace

NZ = ZoneInfo("Pacific/Auckland")

EDGES_SQL = """
select origin_property_id, dest_property_id, epoch(departure_time), epoch(arrival_time)
from fct_movements
where connected
union all
select
    source_property_id,
    dest_property_id,
    epoch(cast(movement_date as timestamp) at time zone 'Pacific/Auckland'),
    epoch(cast(movement_date + 1 as timestamp) at time zone 'Pacific/Auckland') - 1
from stg_plant_movements
"""


def load_edges(con: duckdb.DuckDBPyConnection) -> list[Edge]:
    return [Edge(src, dst, float(dep), float(arr)) for src, dst, dep, arr in con.sql(EDGES_SQL).fetchall()]


def _local(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=UTC).astimezone(NZ).strftime("%Y-%m-%d %H:%M")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tracing")
    parser.add_argument("--db", required=True)
    parser.add_argument("--property", required=True)
    parser.add_argument("--start", required=True, help="infection start, ISO 8601 with offset")
    parser.add_argument("--max-hops", type=int, default=3)
    args = parser.parse_args(argv)

    con = duckdb.connect(args.db, read_only=True)
    known = con.execute(
        "select property_type from stg_properties where property_id = ?", [args.property]
    ).fetchone()
    if known is None:
        print(f"error: unknown property {args.property}", file=sys.stderr)
        return 2
    types = dict(con.sql("select property_id, property_type from stg_properties").fetchall())
    start = datetime.fromisoformat(args.start).timestamp()
    results = trace(load_edges(con), args.property, start, args.max_hops)
    con.close()

    print(f"{len(results)} properties reached from {args.property} within {args.max_hops} hops")
    print(f"{'hops':>4}  {'property':<8}  {'type':<10}  {'earliest arrival':<16}  path")
    for r in results:
        print(
            f"{r.hops:>4}  {r.property_id:<8}  {types.get(r.property_id, '?'):<10}  "
            f"{_local(r.arrival):<16}  {' -> '.join(r.path)}"
        )
    return 0
```

`tracing/__main__.py`:
```python
import sys

from .cli import main

sys.exit(main())
```

Append to the Makefile (tab in the recipe; add `trace` to `.PHONY`):
```make
HOPS ?= 3

trace:
	$(PY) -m tracing --db $(REPO_ROOT)/warehouse/$(ENV).duckdb --property $(PROPERTY) --start $(START) --max-hops $(HOPS)
```

- [ ] **Step 8: Run the tests, then trace the sim warehouse**

Run: `.venv/bin/pytest tests/test_tracing_core.py tests/test_tracing_cli.py -q`
Expected: 8 passed.

Run:
```bash
PROP=$(.venv/bin/python -c "import duckdb; print(duckdb.connect('warehouse/sim.duckdb', read_only=True).sql('select property_id from dim_property_network order by pagerank_rank limit 1').fetchone()[0])")
make trace PROPERTY=$PROP START=2026-06-01T00:00:00+12:00
```
Expected: a header line followed by a table of reached properties with hop counts ≤ 3.

- [ ] **Step 9: Commit**

```bash
make fmt && make test
git add tracing Makefile tests/test_tracing_core.py tests/test_tracing_cli.py
git commit -m "Tracing: hop-limited earliest arrival with per-layer witnesses, CLI

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

### Task 19: End-to-end sim target, CI workflow, README status

**Files:**
- Modify: `Makefile` (add `sim-e2e`)
- Create: `.github/workflows/ci.yml`
- Modify: `README.md`

**Interfaces:**
- Consumes: every earlier target.
- Produces: `make sim-e2e` (reset, setup, backfill, live and `dbt build` on sim), and a CI job that runs lint, unit tests and `sim-e2e`.

- [ ] **Step 1: Add the `sim-e2e` target** (tab in the recipe; add it to `.PHONY`)

```make
sim-e2e: sim-data
	$(MAKE) dbt-build ENV=sim
```

- [ ] **Step 2: Run the full offline chain from a clean state**

Run: `make sim-e2e`
Expected: the generator summaries, then dbt `Completed successfully` with `ERROR=0`. A `WARN` from `recon_missed_rate` is acceptable.

- [ ] **Step 3: Write `.github/workflows/ci.yml`**

```yaml
name: ci

on:
  push:
  pull_request:

jobs:
  offline:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: make venv
      - run: make lint
      - run: make unit
      - run: make sim-e2e
```

- [ ] **Step 4: Add a status section to `README.md`**

```markdown
## Status (Plan 1: offline core)

Works without AWS:

    make venv && make test        # lint + unit tests
    make sim-e2e                  # generate a year of check-ins, simulate the Lambda, build dbt
    make trace PROPERTY=P0001 START=2026-06-01T00:00:00+12:00

What `make sim-e2e` proves:
- `fct_high_risk_visits` (batch) and the simulated Lambda's evaluations agree:
  no false alerts, and every missed alert has a version-proven cause
  (`fct_alert_reconciliation`).
- The SQL stand-down logic matches `rules.py` on every golden case
  (`shared/golden_cases.csv`).

Next: Plan 2 (AWS wiring on LocalStack), Plan 3 (Dagster, AWS deploy, demo).
```

- [ ] **Step 5: Commit**

```bash
make fmt && make test
git add Makefile .github/workflows/ci.yml README.md
git commit -m "CI: lint, unit tests and the offline end-to-end sim build

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_012WEa92bbXCjYuZcpoBBmiL"
```

---

## Spec coverage (Plan 1)

| Spec item | Task |
|---|---|
| §4 stand-down logic, bound, pick order, scope | 2, 3 (Python); 14 (SQL); 16 (parity) |
| §3.5 events, derived ids | 4, 6, 7 |
| §6.1 processing, placeholders, retries, re-emit | 5, 6, 7 |
| §3.2–3.3 generator, backfill, live, cursors, manifests, sim env | 8, 9, 10, 11 |
| §5 staging, `int_visits`, movements, edges, network, high-risk | 12, 13, 14, 15, 17 |
| §6.2 reconciliation classes | 16 |
| §6.3 tests (generic, singular, golden, unit, reconciliation) | 12–17 |
| §5 tracing | 18 |
| §8 CI (offline part), Makefile | 0, 11, 12, 18, 19 |
| §8 LocalStack decision, §3.4 Day-1 scale check | 1 |

**Plan 2:**
- §2 AWS wiring, §6.1 DynamoDB store and handler;
- §3.4 settle and compact;
- §6.5 Firehose;
- §7.3 integration;
- the `local` and `aws` environments.

**Plan 3:**
- §6.4 Dagster;
- §8 AWS deploy and budget;
- §9 demo script;
- the full README.
