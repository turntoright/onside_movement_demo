# Plan 3 execution notes

Measurements from Tasks 13–15 of `2026-10-10-plan-3-dagster-ci-aws-prep.md`, on LocalStack 4.14.0
community. The controller adds the rulings and the review outcomes.

## Measurements

| | Dry run (2026-10-10, reduced `SCALE=aws`) | Execution (full `local` scale) |
|---|---|---|
| `make integration` (Task 13 Step 2) | 1 passed in 196.5 s; 295 s wall | 1 passed in 198.46 s; 306.0 s wall (deploy inside it: 26 added) |
| Backfill events, emitter rate | 5,604 at 126 events/s | 18,313 (CheckIn 9,227, CheckOut 8,494, PlantMovement 250) at 108 events/s |
| Backfill settle time, wall time | 281 s; 326 s | 953 s (37,399 envelopes, 1,008 breaches); 1,123.7 s |
| Dagster start to webserver up | 16 s (with `dbt parse`) | 25 s (with `dbt parse`); 127.0.0.1:3000 only |
| First sensor run (backfill): start to SUCCESS | 58 s; run 31.9 s; checks 51/51 | about 96 s; run 59.0 s; checks 51/51 (48 dbt tests + completeness, dead_letter_queues, freshness) |
| `dbt build` inside Dagster | PASS=77 WARN=0 ERROR=0 | PASS=77 WARN=0 ERROR=0 |
| Reconciliation | 476 matched; missed 20 offline, 5 reordered, 3 origin_end_not_seen; 0 false, 0 unexplained | 1,008 matched = 1,008 distinct alerts from `make alerts` (1,029 alert log lines), both at 09:08:48 after the backfill only; missed 40 offline (origin_not_seen), 0 reordered, 4 origin_end_not_seen; 0 false, 0 not evaluated, 0 unexplained |
| Live run N=50: settle; second sensor run; freshness lag | 46 s; SUCCESS 51/51; 2.9 min | 72 s (wall 74 s; 106 events sent, 10 breaches); SUCCESS 51/51, completeness settled_runs=2; 4.6 min |
| `make trace ENV=local` | P0003: 28 properties in 14.9 s; identical to the module | P0172 (the top PageRank property): 62 properties within 3 hops in 18.4 s; identical to the module |
| Pool: trace step start vs dbt run's last pool step | 35.1 s vs 24.2 s (queued, no lock error) | 56.1 s vs 43.5 s (trace submitted 3 s after the dbt run; queued, no lock error) |
| Cold-start log | `"tzpath": []` | `{"event": "cold_start", "level": "info", "tzpath": []}` (one distinct line) |
| Offline rehearsal | all steps in 402 s; live settled 58 s; poison in the DLQ after 3 min 11 s; DLQ check P=2 passed | all steps in 514 s (rc=0); live settled 105 s (14 breaches); poison in the DLQ after 3 min 02 s; DLQ check passed (poison injected 2, visit DLQ 2, archive DLQ 0) |

Other recorded values: `make deploy ENV=local` after `make up` 90.4 s (init skipped, 26 added);
offline redeploy 19 s (0 added, 0 changed, 0 destroyed); PageRank top 3 P0172 (out_degree 34,
0.0103), P0111 (25, 0.0094), P0175 (25, 0.0089); `make test` 278 passed, 1 skipped at Task 16; 293 passed, 1 skipped (4 min 54 s) after the final fix wave (`d10adfb`).

## Deviations from the plan

- `make trace ENV=local|aws` prints the trace table and then the result file's path
  (`result: state/<env>/trace/<run_id>.json`); brief decision 7 says it "prints the result file".
- The demo traces from P0172, the top PageRank property of this backfill, instead of P0003.

## How it was executed

On 2026-10-10, using subagent-driven development on `feat/plan-3-dagster`. That branch starts from
Plan 2's head `fd6abec`; neither branch has been merged. Every code task went through three stages:
1. an implementer subagent wrote it;
2. a task review checked it for spec compliance and quality, using a stronger model for the sensor,
   the checks, the trace job, the budget and the runbook;
3. fix rounds ran when the review asked for them.

The controller then ran Tasks 13–15 on LocalStack. A final whole-branch review followed, with one fix
wave and a scoped re-review. Nothing ran against real AWS, and nothing was pushed or merged.

## Before execution: independent plan review

The review found 1 blocking, 4 important and 20 minor issues. Each fix went into the task that owns
the code, as a controller amendment.

| Id | Finding | Ruling | Landed in |
|---|---|---|---|
| B1 | Full-scale LocalStack runs take longer than an agent's 10-minute tool limit, and some waits never end on failure | Run detached; poll with a time limit; waits stop on any terminal run status; EXIT trap; resume with `make settle RUN=`, never `make up` | Tasks 13–15 (controller) |
| I1 | The budget guard only proved that *a* budget exists | The guard now requires a budget whose `account_id` equals `AWS_ACCOUNT_ID` | Task 10 |
| I2 | "matched = alerts" could be measured at two different moments | Both numbers recorded at the same moment, after the backfill | Task 14, README |
| I3 | Runbook: the second terminal skipped the endpoint unset and the account check | Repeat steps 1–2 and the `sts` check; never run `make destroy` because something says the stack is gone | Task 12 |
| I4 | A trace sent to the other env's Dagster lost the reason it failed | `submit()` searches every env's trace dir; new test added | Task 7 |
| M1–M20 | Minor issues | Accepted, except M15's optional account check | Tasks 5–16 |

## Rulings made during execution (each with its cost if wrong)

- **M15:** no STS-versus-bucket account check in the aws data commands, because the runbook's `sts`
  check covers the human path.
  - Cost if wrong: an aws data command run with the wrong credentials fails with AccessDenied or
    NotFound instead of a clear message. It never writes anywhere wrong.
- **Parallel reviews:** task N's review ran in parallel with task N+1's implementer whenever the files
  did not overlap.
  - Cost if wrong: a fix for task N lands after task N+1's commit (it happened once, Task 7, and was
    sequenced after Task 8 to avoid sharing the checkout).
- **Task 3:** the reviewer said the `max_hops < 0` check was "not in the brief". It is (brief line
  141), so it stays.
  - Cost if wrong: none.
- **Task 6 minors:** five were parked to the final wave instead of fixed in place: failure
  descriptions, the 15-minute boundary, the race comment, the mode label, and the duplicated
  `APP_TYPES` (plan-mandated, kept).
  - Cost if wrong: none; they were fixed in the final wave.
- **Task 12:** the controller added `AWS_REGION` to step 7's list itself (`23ea95f`) after the
  re-review.
  - Cost if wrong: none.
- **Tasks 13–15:** the controller ran these directly as background scripts rather than through
  implementer agents.
  - Cost if wrong: none; they ran the same commands with the B1 amendments.
- **Task 15:** the first attempt aborted before running anything: the socket path was 111 characters
  long and the limit is 108. It was rerun with `/tmp/claude-1000/p3.sock`.
  - Cost if wrong: none.
- **Final review I1 (stuck `duckdb` slot):** handled by turning on run monitoring
  (`free_slots_after_run_end_seconds: 60`) and documenting the recovery: force-terminate the run, and
  its slot frees within 3 minutes.
  - Cost if wrong: if a presenter closes Dagster's terminal in the middle of a run, the next runs wait
    until that run is force-terminated. The docs explain how.
- **Recovery path, not proven end to end:** the reviewer found in the 1.13.25 source that run
  monitoring frees slots only for finished runs. My dead-worker probe on LocalStack did not isolate
  that path, because the kill command matched its own shell. Killing the orphaned `dbt` instead
  failed the run, and the slot was free within 70 s.
  - Cost if wrong: the documented recovery (force-terminate) might take longer than 3 minutes.
- **`reset_hint` split:** split into `reset_hint` and `gone_hint` after the re-review, so the
  credentials-first wording appears only where the stack looks gone.
  - Cost if wrong: none.

## Task completions

| Task | Commits | Review outcome |
|---|---|---|
| 1 Toolchain pins | `087f98a` | approved |
| 2 `onside-*` tables (M7) | `49f92ec` | approved; a `git grep` found no bare name in runtime paths |
| 3 Tracing code path | `3981978` | approved |
| 4 S3 helpers, `OnsideResource` | `cffd385` | approved; the M4 editable reinstall was verified |
| 5 Sensor, schedule | `198cbab` | approved (opus); cursor verified by a 3,000-trial simulation |
| 6 Asset checks | `a985d1f` | approved (opus) |
| 7 Trace job, submit path | `aba0826`, `f86aa16` | 1 important fixed: the real `DagsterGraphQLClient` wraps transport errors, so "lost Dagster" was reported as "rejected" |
| 8 dbt assets, `Definitions` | `5776f59` | approved |
| 9 `make dagster` / `make trace` | `23635f6` | approved; instance config loaded under 1.13.25 |
| 10 Budget root, guard | `110471d` | approved (opus); the guard fails closed in every edge case |
| 11 CI `localstack` job | `b243c09` | approved |
| 12 Runbook, `dbt-debug`, cold-start log | `87d283b`, `cccb669`, `23ea95f` | 1 important fixed: `AWS_REGION` overrides `AWS_DEFAULT_REGION`, so teardown checks could have looked clean in the wrong region |
| 13–15 LocalStack runs | none (runs) | see Measurements |
| 16 README, these notes | `b6bc52d`, `9aaf4d0` | 1 important fixed: the README stated the AWS deploy as fact |

## Final whole-branch review and fix wave

- **Final review (opus):** 0 critical, 1 important, 8 minor. It agreed with every parked item.
- **I1:** a run that dies mid-step keeps the `duckdb` slot claimed.
- **Fix wave** (`8e34714`, `39bc12c`, `4077ec8`, `4ad3777`, `6040ae8`, `de0cac7`):
  - I1: run monitoring plus the recovery docs;
  - M1: a credentials-first hint when the aws stack looks gone;
  - M2/M3: a one-line reason on every red check, race comments, and completeness worded as
    "present" rather than "equal";
  - M4/M5: `make venv` first, LocalStack restarts, and how to run the checks after poison;
  - M6/M7: budget tagging permissions; stop the local Dagster before the aws one;
  - M8: `dagster-graphql` and `pyyaml` declared;
  - parked items: the freshness boundary and mode label, the empty-message case in `short_reason`,
    the 12-digit `AWS_ACCOUNT_ID` guard, budget tests that pin PERCENTAGE and USD, a
    `budget-destroy` test, quoted `-var` values, `.gitignore`, and SIGPIPE for
    `make alerts | head`.
- **Scoped re-review:** two minor residuals, fixed in `d10adfb`: the recovery takes up to 3 minutes,
  and `gone_hint` was split out from `reset_hint`.
- **Smoke test on LocalStack after the wave:**
  - Dagster restarted with run monitoring `{enabled: true, 60}`.
  - Live N=10 settled in 61 s.
  - The sensor's `dbt_build` passed 51/51 with `settled_runs=4`.
  - `make trace` P0172 reached 62 properties.

## Deferred minor findings (not fixed; none blocks the demo)

- **Task 2:** the spike docs keep the bare table names; they are historical and create their own
  tables.
- **Task 3:** a path that exists but is not a DuckDB file gives DuckDB's raw error.
- **Task 4:** no test lists more than 1,000 keys (the paginator is standard). `get_json` has no hint
  mapping, but callers list first.
- **Task 5:** non-`.json` keys sit in the cursor's window set; they are harmless and age out.
- **Task 6:** `APP_TYPES` is duplicated with `generator/settle.py` (plan-mandated).
- **Task 9:** the `dagster dev` recipe is one line of about 190 characters (the `make -n` test needs
  it that way).
- **Task 10:** `tf-check` downloads the aws provider twice, about 50 s on CI.
- **Task 11:** `cancel-in-progress` also applies to main.
- **Final:** `budget-env` checks only that the account id is non-empty, and `destroy ENV=aws` has no
  12-digit guard. Terraform's variable validation rejects a bad id before any AWS call.

## Left for Harvey

- **Real AWS:** follow `docs/aws-deploy.md`, budget first.
- **Push:** remember `gh auth switch --user turntoright`. The CI `localstack` job then runs on pushes
  to main, or by hand.
- **Merge:** `feat/plan-1-offline-core`, then `feat/plan-2-aws-wiring`, then `feat/plan-3-dagster`.
  Each starts from the previous one.
- **Optional:** a LocalStack token. Community 4.14.0 needs none.
- **Demo state:** LocalStack is left running with it: the full backfill (run `982118bf`), live runs of
  50 (online), 50 (offline rehearsal) and 10 (smoke test after the fix wave), and the K=2 poison from
  the offline rehearsal. Dagster is stopped; its history keeps one FAILURE `dbt_build` from the
  recovery probe. LocalStack keeps nothing across a Docker
  or laptop restart. Rebuilding with `make up` and the quick start takes about 30 minutes.
