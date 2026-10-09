# Plan 3 design brief: Dagster, CI, AWS deploy prep, README

Written by the controller on 2026-10-10, after Plan 2 was executed (branch
`feat/plan-2-aws-wiring`, execution notes `2026-10-10-plan-2-execution-notes.md`).
Plan 3 is on branch `feat/plan-3-dagster`, which starts from Plan 2's head.

**Authority:** spec rev 7.2 > this brief > the plan. The binding sections of the spec are:
- §1 (success criteria);
- §2 (`orchestration/`);
- §5 Tracing;
- §6.4 Dagster;
- §7;
- §8;
- §9 (demo script);
- §10 (cut line).

**Harvey decides these, and nothing in Plan 3 may do them:**
- any command that reaches real AWS (deploy, budget, screenshots);
- `git push` or anything that needs GitHub, such as running CI;
- merging branches;
- a LocalStack token.

Plan 3 prepares all of them so that Harvey only has to run documented commands.

## Binding decisions

### 1. Versions

Pin `dagster~=1.13.25`, `dagster-webserver~=1.13.25` and `dagster-dbt~=0.29.25`, which were
the latest releases on 2026-10-10.
- Add them as an optional extra `[orchestration]`.
- Also add them to `dev`, so `make test` covers the orchestration code.
- Keep Python 3.12. Verify that the pins resolve together with `dbt-core 1.12` and
  `dbt-duckdb 1.11` before writing any code.
- If dagster-dbt 0.29 does not support dbt-core 1.12, record the conflict, pick the closest
  compatible set and pin it. Never upgrade dbt-duckdb outside `~=1.11.0`.

### 2. Layout

The package `orchestration/` contains:
- `definitions.py`: one `Definitions` object, chosen by `ONSIDE_ENV=local|aws`. Loading it
  with `sim` is an error.
- `dbt_assets.py`
- `sensors.py`
- `checks.py`
- `trace_job.py`
- `resources.py`: the AWS clients, built from `generator.aws`, and the paths.

Rules for the code:
- Reuse `generator.aws` (`connect`, `Outputs`, `client`) and `generator.state` helpers.
  Do not duplicate AWS wiring.
- Reuse `tracing.core.trace` for the algorithm.

### 3. Dagster instance

Use `DAGSTER_HOME=state/<env>/dagster`, which is gitignored, with a `dagster.yaml`
generated or copied by `make dagster`.
- **Run queue:** `QueuedRunCoordinator`.
- **Concurrency pool:** one pool named `duckdb` with a limit of 1 (spec §6.4). Both the dbt
  assets and the trace job's op use it. Use the 1.13 pools API; verify the config keys
  against the installed version.
- **`make dagster ENV=local|aws`:** runs `dagster dev`, which includes the webserver and
  the daemon, on `127.0.0.1:3000` only. It sets the environment from the Makefile:
  - `REPO_ROOT`, `ONSIDE_ENV`;
  - `DBT_PROFILES_DIR`;
  - the dbt target (equal to the env);
  - `DBT_COMPACTED_GLOB` and `DBT_REF_ROOT` from the bucket, as `dbt-build` already does;
  - `LOCALSTACK_S3_HOST`;
  - the DuckDB file `warehouse/<env>.duckdb`.

  The command refuses `ENV=sim`, as the spec says.

### 4. dbt assets

Use `@dbt_assets` over the manifest produced by `dbt parse` for the env's target.
- Generate the manifest at `make dagster` time, or lazily through `DbtProject.prepare_if_dev()`.
  Choose one and make it work offline.
- Run `dbt build` so tests run as asset checks, which dagster-dbt does by default.
- Give every dbt asset the `duckdb` pool.
- The Python model `dim_property_network` must work under Dagster the same way it works
  under `make dbt-build`.

### 5. Sensor and schedule

**Sensor.**
- It lists `control/settled/` in the env's bucket and fires when there are new keys.
- The cursor is the greatest `LastModified` together with the key; never fire twice for the
  same settled object.
- It never lists `raw/`.
- Each new settled object produces one `RunRequest`. The run key is the settled `run_id`, and
  the request materializes all dbt assets.
- Minimum interval: 30 s.

**Schedule.** A daily schedule backs up the sensor. It is stopped by default (spec: "a schedule
exists as a backup").

### 6. Asset checks (spec §6.4)

Attach them to the dbt staging asset `stg_events`, or to a source asset, whichever makes them
show green in the UI after the dbt run. Read only: never receive SQS messages.

- **Completeness.** For CheckIn, CheckOut and PlantMovement, distinct `event_id`s in
  `compacted/<env>/` must equal the sum over the env's `control/manifests/*.json` counts.
  - Count from DuckDB staging, which is already deduplicated, or straight from S3. Pick one
    and document it.
  - Count only manifests that have a matching `control/settled/<run_id>.json`. An unsettled
    run is not missing data yet.
- **DLQ.**
  - Let P be the sum of `k` over `control/poison/*.json`. The visit DLQ's
    `ApproximateNumberOfMessages` must be at least P and at most 2P (no more than one extra
    per poison event).
  - The archive DLQ must be 0.
- **Freshness.** Use the latest settled run record. When it is a live run, its max
  `received_time` must be within 15 minutes of its settle time. When it is a backfill, the
  check is skipped (pass with metadata `skipped: backfill`).

### 7. Trace job

The job takes config `property_id`, `start_time` (ISO with offset) and `max_hops` (default 3).
- It opens `warehouse/<env>.duckdb` read-only, builds edges exactly as `make trace ENV=sim`
  does today (reuse that code path), and runs `tracing.core.trace`.
- It logs a table of results and writes `state/<env>/trace/<run_id>.json`.
- Its op uses the `duckdb` pool.

**`make trace ENV=local|aws PROPERTY= START= [HOPS=]`.** It submits the job to the run queue
of the running `dagster dev`. Use the instance with `DAGSTER_HOME`, or the GraphQL client
against `127.0.0.1:3000`, so the concurrency pool applies. It then waits for completion and
prints the result file.
- If `dagster dev` is not running, it exits 2 with "start `make dagster ENV=…` first".
- `make trace ENV=sim` stays as it is today.

### 8. M7: table name prefixes (Plan 2 deferral)

The DynamoDB tables become `onside-person-state`, `onside-evaluations` and `onside-properties`.
Change them in all of these places together:
- Terraform;
- the Terraform outputs;
- `tests/aws_fixtures.py` (`MotoStack`);
- the integration fixture;
- any hard-coded name.

On LocalStack this replaces the tables, so the plan's last task must rebuild the demo state.

### 9. Real-AWS preparation (written and validated, never run)

- **Budget.** A separate Terraform root `infra/budget/` with one `aws_budgets_budget`:
  - $5 per month, alerts at 80% actual and 100% forecast, emailed to `var.email`;
  - its own state at `infra/budget/state/aws.tfstate`;
  - target `make budget ENV=aws AWS_ACCOUNT_ID=… BUDGET_EMAIL=…`, which uses the same
    account allow-list and the plan → y/N → apply flow as `deploy ENV=aws`;
  - `make deploy ENV=aws` refuses unless the budget state file shows the budget exists. The
    spec says it is created first.
  - `terraform validate` (through `tf-check`) covers both roots.
- **Runbook.** `docs/aws-deploy.md` is a step-by-step guide for Harvey, built from the
  execution-notes "Plan 3 preconditions":
  1. identity and account settings;
  2. unset the endpoint variables;
  3. the budget;
  4. deploy;
  5. `dbt debug --target aws` (M14);
  6. a small `setup` and `generate` with SCALE=aws, then settle;
  7. `make dagster ENV=aws` and the screenshots list (which UI pages, saved to
     `docs/screenshots/`);
  8. `make destroy ENV=aws` and the budget teardown;
  9. a cost estimate.
- **dbt `aws` target.** Read the region from an env var whose default is ap-southeast-2,
  rather than hard-coding it.

### 10. CI LocalStack job (`.github/workflows/ci.yml`)

Add a job `localstack`:
- triggers on push to main and on workflow_dispatch;
- `timeout-minutes: 30`;
- permissions read-only;
- runs `make venv`, then `make integration`;
- needs no token (community 4.14.0).

Validate the workflow file locally with `actionlint` if it is installed. If it is not
installed, parse the YAML and check job keys by hand. The job never runs before Harvey pushes.

### 11. README rewrite (spec §1, §9)

- **Opening:** what the demo is and which Onside features it maps to, as a table: check-ins
  and movements; the stand-down checker gives "High risk visits"; tracing gives the Tracing
  add-on; PageRank gives "Key property identification"; reconciliation; and
  offline/late/duplicate check-ins.
- **Architecture:** a mermaid diagram of the chain.
- **Quick starts:** sim, which needs no Docker; LocalStack, the full demo; and AWS, which
  points to the runbook.
- **Demo script:** spec §9, with the real commands and the measured timings.
- **Results:** the reconciliation numbers.
- **Testing:** unit, dbt, integration and parity tests.
- **Production next steps:** spec §1 out-of-scope list and §9 step 6.
- **Repo layout.**

Keep it accurate: every command shown must exist, and every number must come from a recorded
run.

### 12. Tests

All tests run offline, using moto or tmp DuckDB, with no `dagster dev`. They must cover:
- definitions load for `local` with fake outputs;
- the sensor with moto S3: the cursor, no double firing, and `raw/` is ignored;
- each asset check pass/fail with moto (SQS attributes, S3 manifests) and a tiny DuckDB;
- the trace job executed in-process against a small DuckDB fixture;
- the `make trace` submit path's error when `dagster dev` is not running;
- the budget guard in the Makefile (`make -n`).

### 13. End-to-end proof on LocalStack (the plan's last tasks)

1. Redeploy for M7 with `make up` and `make deploy ENV=local`, then setup and a full `local`
   backfill (about 17 min).
2. Start `make dagster ENV=local` in the background. The sensor triggers a dbt run, and all
   checks pass.
3. Run `make generate ENV=local MODE=live N=50`. The sensor fires once more, and the
   freshness check runs and passes.
4. `make trace ENV=local` with a real property gives a non-empty result.
5. Run an offline rehearsal of the demo path: `unshare -rn` together with the LocalStack
   relay technique from the Plan 2 fix report. If the full offline rehearsal is not possible,
   prove that the offline-relevant steps start, and record exactly what was proved.
6. Record timings and numbers in the README and the execution notes.
7. Leave LocalStack running with the demo state, and stop `dagster dev`.

## Out of scope for Plan 3

- Executing anything on real AWS.
- Pushing, merging, and running CI on GitHub.
- Screenshots of AWS. A list of the screenshots to take goes in the runbook.
- Optionally, LocalStack and Dagster UI screenshots under `docs/screenshots/` (taken
  headlessly, if a browser is available).
