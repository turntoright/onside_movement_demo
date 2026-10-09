# Plan 2 execution notes (AWS wiring on LocalStack)

Executed on 2026-10-10 with subagent-driven development on branch `feat/plan-2-aws-wiring`
(it branches from `feat/plan-1-offline-core`). Each task went through:
1. an implementer subagent;
2. a task review covering spec and quality;
3. fix rounds where needed.

After the last task came:
4. a final whole-branch review;
5. one fix wave.

Nothing ran against real AWS, and nothing was pushed.

## Before execution: independent plan review

The review found 0 blocking, 5 important and 15 minor issues. I ruled on each one and folded
it into the task that owns the code. Spec rev 7.2 (`b907fdc`) records the changes that alter
the design.

| Id | Finding | Ruling | Landed in |
|---|---|---|---|
| I1 | With `ENV=local` and an empty endpoint, Terraform falls back to the real AWS account | Cross-variable validation, `required_version >= 1.9`, `LOCALSTACK_ENDPOINT := $(or …)`, and `allowed_account_ids` with `aws_account_id` for aws | Task 10 |
| I2 | On a low-quota AWS account, throttled retries can dead-letter valid messages | `maximum_concurrency` 5 for visits and 2 for archive; archive visibility raised from 30 s to 60 s (spec 7.2) | Task 10 |
| I3 | A lost LocalStack gave a traceback instead of "run `make up`" | `Connection.keys` maps NoSuchBucket and connection errors to `GeneratorError`; the test deletes the bucket | Tasks 5, 8 |
| I4 | The offline demo needed the network (`docker pull`, tzdata from PyPI) | `up` resets state first and pulls only a missing image; `package` copies tzdata from the venv | Task 9 |
| I5 | LocalStack listened on 0.0.0.0 while holding the Docker socket | Bound to `127.0.0.1:4566` | Task 9 |
| M1–M15 | Minor issues | M1 in Task 2; M2 and M11 in Task 7; M3, M9 and M12 in Task 8; M5, M6, M10 and M15 in Task 10; M8 and M13 in Task 11; M4 in Task 13 | as listed |
| M7, M14 | Table names have no prefix; `dbt debug --target aws` | **Deferred to Plan 3**; they only matter for the real AWS deploy | see below |

## Rulings made during execution

Each ruling is listed with the cost if it is wrong.

- **Task 8:** `ANCHOR` and `NOW` keep `?=` in the Makefile, as in the Plan 1 design (CI passes
  them on the command line). The other data knobs now use plain `=`.
  - Cost if wrong: an ambient `ANCHOR` in the shell could pin a run unexpectedly.
- **Task 10:** commit `2faa283` was reworded to `aee32da` so its Co-Authored-By trailer matches
  the global constraints.
  - Cost if wrong: none (local and unpushed).
- **Task 11:** `make alerts` filters on the client side, with pagination. LocalStack ignores
  `filterPattern`, so the first version counted the whole log group (2,172 events against 476
  real alerts).
  - Cost if wrong: none. On AWS the server-side filter only makes it faster.
- **Spec rev 7.2:** archive-queue visibility goes from 30 s to 60 s, and both event source
  mappings get concurrency caps. These are spec changes made without asking Harvey, based on the
  plan review's analysis of real AWS.
  - Cost if wrong: after a failure, archive retries arrive later, and throughput on AWS is capped
    at 5 plus 2 concurrent invocations.

## Task completions

| Task | Commits | Review outcome |
|---|---|---|
| 1 Store contract, moto | `e1064ff` | approved |
| 2 DynamoDbStore | `8bda488`, `2345d2b` | approved; mutation check gave 4 failures, as in the brief |
| 3 Stand-down handler | `0034429`, `ba3384b` | approved; 6 minor fixes |
| 4 Archiver | `bd7f673` | approved |
| 5 Envs, Connection | `88dab93` | approved, including I3 |
| 6 Emitter | `430077f`, `18c55d7` | approved; 3 minor fixes |
| 7 Settle and compact | `db01833`, `5a42f0f` | 1 important fixed: `delete_objects` errors were swallowed; parts are now written before stale ones are deleted |
| 8 CLI and Makefile | `ee5e914`, `1892941` | 1 important fixed: botocore errors skipped the reset hint |
| 9 Compose, up, package | `0ea1aa0` | approved |
| 10 Terraform | `aee32da`, `9c87558` | approved; 26 resources (a TLS-deny bucket policy was added) |
| 11 dbt on S3, reduced end-to-end run | `c9c5b5b`, `f7bc18b` | `make alerts` fixed |
| 12 Integration test | `95d9369` | approved; 1 passed in 203.8 s |
| 13 Measured `local` run | `b87b8b7` | see the measurements below |

## Measurements (LocalStack 4.14.0 community, 4-CPU laptop)

- **Reduced end-to-end run (`SCALE=aws`):**
  - Settled in 291 s.
  - Reconciliation: 466 matched; 19 missed for offline, 7 reordered, 1 `origin_end_not_seen`; 0 false, 0 unexplained.
  - Live run of N=50: settled in 46 s.
  - Poison K=2: reached the visit DLQ in 3 min 06 s.
- **Full `local` scale:**
  - Events: 18,313, emitted at 131 events/s.
  - Settle took 855 s; wall time 996 s, about 18 events/s end to end. That is under the
    45-minute limit, so the scale stays at 10,000 check-ins.
  - Envelopes compacted: 37,375.
  - dbt: PASS=77, ERROR=0.
  - Reconciliation: 981 matched; 39 missed for offline, 6 reordered, 7 `origin_end_not_seen`; 0 false, 0 unexplained.
  - `make alerts`: 981 distinct alerts, the same as the 981 matched.
- **`make integration`:** 1 passed in 203.8 s.

## Final whole-branch review and fix wave

The final review (opus) found 0 critical, 2 important and 7 minor issues. One fix wave
covered I1 and m1–m7 in commits `955f627..9db373a`, and a scoped re-review then came back
clean.

- **I1, the offline deploy.** `terraform init` always queried the registry.
  - Fix: each environment now has its own `TF_DATA_DIR` (`infra/terraform/.terraform-<env>`),
    and init runs only when the stamp built from the lock file, `versions.tf` and the backend
    path changes.
  - Proof: `make deploy ENV=local` ran inside `unshare -rn` (no network; LocalStack relayed
    over a Unix socket) and gave `Apply complete! Resources: 0 added`. The old unconditional
    init failed in the same namespace.
- **I2, deferred items not committed.** This file now records them, including the Plan 3
  preconditions below.
- **m1.** Botocore errors now end with a hint instead of a traceback. Local clients use
  3 attempts with a 2 s connect timeout. Before, a down LocalStack took 86 s to fail.
- **m2.** `make deploy ENV=aws` shows the plan and asks y/N before applying the saved plan.
  `destroy ENV=aws` is interactive and then removes `state/aws`.
- **m3.** Settling a run with no raw objects no longer crashes.
- **m4.** The region and the checker's log group are read from the Terraform outputs.
- **m5.** The README lists `make integration` first and warns that it wipes the stack.
- **m6.** `make alerts` also lists the last 5 alerts.
- **m7.** Stale facts in the plan doc are corrected.

After the fixes:
- 207 unit tests pass and 1 is skipped.
- `make deploy ENV=local` shows 0 changes.
- `make dbt-build ENV=local` gives PASS=77, ERROR=0.
- `make alerts ENV=local` shows 981 distinct alerts.

## Deferred minor findings (not fixed; none blocks the demo)

- **Task 1:** `get_property` returns a shallow `dict` copy, which is enough for flat property
  dicts.
- **Task 3:** the per-entry logging of an accepted alert (when another entry of the same record
  was rejected) has no test.
- **Task 4:** `ensure_ascii` re-escapes non-ASCII text, which is still equivalent JSON. A naive
  clock would be read as local time; nothing produces one.
- **Task 5:** an ambient `AWS_ENDPOINT_URL` would redirect aws clients. `allowed_account_ids`
  catches this at deploy, but the generator does not.
- **Task 7:** the in-flight and delayed branches of `is_idle` have no test, and neither does the
  DLQ warning that fires once per depth change.
- **Task 9:** a missing tzdata in the venv gives an unhelpful `cp` error. `make down` keeps
  `local.tfstate`; only `make up` resets everything.
- **Task 12:** the `wait_until` timeout message doesn't report the observed state, and there is
  no explicit `ENV=aws` refusal on `integration` (it is safe: local is hard-coded). The
  archive-DLQ check is one-shot.
- **Final review:**
  - `destroy ENV=aws` keeps `data/aws` and `warehouse/aws.duckdb`, which are stale but
    harmless.
  - A half-deleted `.terraform-<env>/providers` with its stamp intact would skip init. The fix
    is to delete that directory.
  - The unused `infra/terraform/.terraform/` from before the per-environment data directories
    can be deleted.

## Plan 3 preconditions (real AWS; Harvey runs these)

1. **M7.** Prefix the DynamoDB table names (`onside-*`), as a brief amendment. Update
   `MotoStack`, the integration fixture and the outputs together.
2. **M14.** Run `dbt debug --target aws` and let DuckDB install the `aws` extension before the
   first AWS build.
3. **Account checks.** Run `aws sts get-caller-identity`, then
   `aws lambda get-account-settings`. ConcurrentExecutions must be at least 10, because the
   event source mapping caps add up to 7.
4. **Budget.** Create the $5 budget alarm before anything else.
5. **Endpoint.** `unset AWS_ENDPOINT_URL` and any `AWS_ENDPOINT_URL_*`.
6. **Deploy.** Run `make deploy ENV=aws AWS_ACCOUNT_ID=<12 digits>`. It shows the plan and asks
   for confirmation.
7. **State backup.** Back up `infra/terraform/state/aws.tfstate` until
   `make destroy ENV=aws` has run.
8. **Time zones.** After the first AWS invocation, log `zoneinfo.TZPATH` once and expect `()`.
   This confirms that the bundled tzdata is used and that the empty `PYTHONTZPATH` was accepted.
9. **Region.** `dbt/profiles.yml` hard-codes `ap-southeast-2` for the aws target; update it
   if the region changes.
10. **Offline rehearsal.** Rehearse the demo offline: Wi-Fi off, then
   `make up → deploy → setup → generate (small) → dbt-build → live → poison`.
