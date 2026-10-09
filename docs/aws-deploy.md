# Deploying the demo to real AWS once (runbook)

This is the one real-AWS run that spec §1 asks for: the same Terraform as LocalStack, a small
dataset, screenshots, then teardown. Every step is a command in this repo. Run them in order,
from the repo root, in one shell (step 7 adds a second). Expected cost: well under US$1 (step 9); the budget alarm
from step 3 emails at US$4 actual and at a US$5 forecast.

Nothing in this repo runs against AWS by itself: `ENV=aws` is never a default, `make deploy`
and `make budget` show the plan and ask before applying, and Terraform refuses any account
other than `AWS_ACCOUNT_ID`.

## Prerequisites

- IAM permissions: admin-equivalent for the deploy (it creates IAM roles, Lambdas, queues,
  tables, a bucket and an EventBridge bus), plus `budgets:ModifyBudget`,
  `budgets:ViewBudget`, `budgets:TagResource` and `budgets:ListTagsForResource` for the
  budget in step 3 (its provider tags it with `project` and `env`).
- An IAM user (not the root user) needs Billing console access activated for the account to
  take the `aws-01-budget` screenshot.
- The 100% FORECASTED budget alert cannot fire on a new account until AWS has enough usage
  history to forecast. The 80% ACTUAL alert is the real guard.

## 1. Identity and account settings

```bash
export AWS_PROFILE=<your profile>          # or other credentials for the demo account
export AWS_DEFAULT_REGION=ap-southeast-2    # for the aws CLI; Terraform has its own default
export AWS_REGION=ap-southeast-2            # AWS CLI v2 lets this override AWS_DEFAULT_REGION
aws sts get-caller-identity                 # note the 12-digit Account
export AWS_ACCOUNT_ID=<the 12 digits>
```

The quota check comes after step 2, once nothing can point at LocalStack.

## 2. Make sure nothing points at LocalStack

```bash
unset AWS_ENDPOINT_URL LOCALSTACK_ENDPOINT
for v in $(env | grep -o '^AWS_ENDPOINT_URL_[A-Z0-9_]*'); do unset "$v"; done
env | grep -E 'AWS_ENDPOINT_URL|LOCALSTACK_ENDPOINT' || echo "clean"
aws configure get endpoint_url || echo none
```

Now confirm the account and the Lambda quota:

```bash
aws sts get-caller-identity --query Account --output text      # must equal $AWS_ACCOUNT_ID
aws lambda get-account-settings --region ap-southeast-2 --query 'AccountLimit.ConcurrentExecutions'
```

`000000000000` means a LocalStack endpoint is still active. `ConcurrentExecutions` must be at
least 10: the two event source mappings run up to 5 + 2 concurrent invocations, and a new
account can start with a quota of 10. If it is lower, ask for a quota increase (Service Quotas
→ Lambda) before going on.

Expected from the check above: `clean` and `none`. An endpoint variable (or an `endpoint_url` in your profile in
`~/.aws/config`) would send the generator's AWS clients somewhere else (Terraform would refuse
at deploy, through `allowed_account_ids`).

## 3. The budget, before anything else (spec §8)

```bash
make budget ENV=aws AWS_ACCOUNT_ID=$AWS_ACCOUNT_ID BUDGET_EMAIL=<your address>
```

It shows a plan with one `aws_budgets_budget` (US$5 a month; alerts at 80% actual and 100%
forecast to your address) and asks `Create this budget in account ...? [y/N]`. Answer `y`.
Its state lands in `infra/budget/state/aws.tfstate`; `make deploy ENV=aws` refuses to run
until that file holds a budget for `AWS_ACCOUNT_ID`.

- The guard checks the local state file only. If you delete the budget in the console, run
  `make budget` again before deploying.
- The budget state is one file per machine (`infra/budget/state/aws.tfstate`). To use a
  different AWS account, run `make budget-destroy` for the old account first (or move the state
  file aside) before `make budget` for the new one.

## 4. Deploy the stack

```bash
make deploy ENV=aws AWS_ACCOUNT_ID=$AWS_ACCOUNT_ID
```

The first run downloads the providers (`infra/terraform/.terraform-aws/`). The plan creates
26 resources, among them the bus, 2 rules, 2 queues and 2 DLQs, 2 Lambdas with roles and log
groups, 3 DynamoDB tables (`onside-person-state`, `onside-evaluations`, `onside-properties`)
and the bucket `onside-aws-<account id>`, plus EventBridge targets, queue and role policies,
the two event source mappings and the bucket policy: 26 in all. Answer `y`. Then back up the state until step 8 is done:

```bash
cp infra/terraform/state/aws.tfstate ~/onside-aws.tfstate.backup
```

## 5. Let DuckDB fetch its AWS extensions (M14)

```bash
make dbt-debug ENV=aws
```

Expected: `All checks passed!`. This installs DuckDB's `httpfs` and `aws` extensions once and
proves that the `aws` profile resolves your credentials (the region comes from the deployed
stack's outputs).

## 6. A small dataset, settled

```bash
make setup ENV=aws                          # 40 properties, 100 people (the aws scale)
make generate ENV=aws MODE=backfill         # about 2,800 check-ins; emits, then settles
make generate ENV=aws MODE=live N=50        # the demo step
make alerts ENV=aws                         # alert count and the last 5 alerts
```

Each `generate` ends with `settled run <run_id> in <n>s ...`. On LocalStack the same scale
settles in about 5 minutes; AWS is usually faster. If a settle times out, resume it with the
`make settle ENV=aws RUN=<run_id>` command that it prints.

If a command says the stack is gone, do NOT run `make destroy`; first check steps 1–2 in this
terminal (credentials, account, region and no endpoint variable).

Check the time-zone data once (spec §2: the Lambda reads only its bundled tzdata):

```bash
aws logs filter-log-events --log-group-name /aws/lambda/onside-standdown-checker \
  --region ap-southeast-2 --filter-pattern '{ $.event = "cold_start" }' --max-items 1 \
  --query 'events[0].message' --output text
```

Expected: a line containing `"tzpath": []`.

Then the DLQ demo (about 3 minutes). Run it last:

```bash
make poison ENV=aws K=2
```

- `make poison` writes `control/poison/` records before the messages reach the DLQ, about
  3 minutes later. A `dead_letter_queues` check can be red in that window, so run poison last
  and let it finish before any dbt build or screenshot.
- DLQ messages expire after 14 days, while the `control/poison/` records stay. Old demo state
  therefore turns the check red: rebuild the state if the last poison is older than 14 days: `make destroy ENV=aws`, then
  `make deploy` (step 4) and `make setup` (step 6) again.

## 7. Dagster and the screenshots

In a second terminal, repeat steps 1 and 2: the same `AWS_PROFILE`, `AWS_REGION`,
`AWS_DEFAULT_REGION` and `AWS_ACCOUNT_ID` exports, then the endpoint unset and the `clean`/`none` check. Confirm that

```bash
aws sts get-caller-identity --query Account --output text
```

prints the same value as `$AWS_ACCOUNT_ID`. If the Dagster UI shows the `settled_runs` sensor
skipping with "the stack is gone or unreachable", do NOT run `make destroy ENV=aws`: it usually
means this terminal cannot see the stack, so check steps 1–2 first, as the hint says. Stop any `make dagster ENV=local`
first (one Dagster at a time, on port 3000). Then:

```bash
make dagster ENV=aws        # http://127.0.0.1:3000; Ctrl-C stops it
```

Stop Dagster with Ctrl-C, never by closing its terminal: a run killed mid-step keeps its
`duckdb` pool slot. If runs then stall waiting for that slot: Runs → the old `STARTED` run →
Terminate (force termination); its slot frees within a minute (or Deployment → Concurrency →
`duckdb` → "Free all concurrency slots for run").

Within a minute the `settled_runs` sensor starts one `dbt_build` run per settled run (the
backfill and the live run). Then:

```bash
make trace ENV=aws PROPERTY=<a property id, e.g. from make alerts> START=<ISO time with offset>
```

The DynamoDB console's item count refreshes only about every 6 hours: use "Get live item
count" before taking `aws-06-dynamodb.png`.

Take these screenshots and save them as PNG files under `docs/screenshots/`:

| File | Where | What it shows |
|---|---|---|
| `aws-01-budget.png` | Billing → Budgets | `onside-demo-monthly`, US$5, both alerts |
| `aws-02-eventbridge.png` | EventBridge → Event buses → `onside-events` → Rules | rules `onside-visits` and `onside-archive` |
| `aws-03-sqs.png` | SQS → Queues | the four queues; `onside-visit-dlq` holding the poison messages |
| `aws-04-lambda.png` | Lambda → `onside-standdown-checker` → Monitor | invocations and the SQS trigger |
| `aws-05-alert-log.png` | CloudWatch → Log groups → `/aws/lambda/onside-standdown-checker` | a `"level": "alert"` line (HighRiskVisitDetected) |
| `aws-06-dynamodb.png` | DynamoDB → Tables | the three `onside-*` tables with item counts |
| `aws-07-s3.png` | S3 → `onside-aws-<account>` | the `raw/`, `compacted/aws/`, `control/` and `ref/` prefixes |
| `dagster-01-lineage.png` | Dagster → Assets → Lineage | the dbt graph from staging to marts |
| `dagster-02-sensor.png` | Dagster → Automation → `settled_runs` | ticks that requested runs |
| `dagster-03-checks.png` | Dagster → `stg_events` → Checks | completeness, dead_letter_queues and freshness, green |
| `dagster-04-trace.png` | Dagster → Runs → the `trace` run | the logged trace table |

Leave `make dagster` running until the screenshots are done, then stop it with Ctrl-C.

## 8. Teardown

```bash
make destroy ENV=aws AWS_ACCOUNT_ID=$AWS_ACCOUNT_ID     # type "yes"; also removes state/aws
aws resourcegroupstaggingapi get-resources --region ap-southeast-2 \
  --tag-filters Key=project,Values=onside-movement-demo --query 'ResourceTagMappingList[].ResourceARN'
aws iam get-role --role-name onside-standdown-checker
aws iam get-role --role-name onside-archiver
aws logs describe-log-groups --region ap-southeast-2 --log-group-name-prefix /aws/lambda/onside \
  --query 'logGroups[].logGroupName'
```

Expected: the destroy plan lists 26 resources; the tag query then returns only the budget
(or `[]`; tagged resources can take a few minutes to drop out of the index). The tagging API
is regional and does not list IAM roles (which are global), hence the two extra checks:
both `get-role` commands must fail with `NoSuchEntity`, and the log-group query must print
`[]`. The bucket has `force_destroy`, so its
objects go with it.

A cost budget without actions has no charge (check the AWS Budgets pricing page), so it can
stay as a guard. To remove it too:

```bash
make budget-destroy ENV=aws AWS_ACCOUNT_ID=$AWS_ACCOUNT_ID BUDGET_EMAIL=<your address>
```

Delete `~/onside-aws.tfstate.backup` once both are gone.

## 9. Cost estimate

This is an estimate from list prices and the LocalStack run's volumes at the same scale
(Plan 2, Task 13: 2,828 check-ins, 2,602 check-outs and 75 plant movements; one live run of 50;
two poison messages), not a measured bill. Prices are ap-southeast-2 list prices; check the AWS
pricing pages before relying on them. Most of it falls in the always-free tiers.

| Service | Volume (estimate) | Cost |
|---|---|---|
| EventBridge custom events | about 12,000 (app events plus the checker's events) | about US$0.01 |
| SQS requests | under 100,000 | free tier (1 million a month) |
| Lambda | about 2,000 invocations, 256 MB, under 1 s each | free tier |
| DynamoDB on demand | about 6,000 transactional writes of items up to about 10 KB, and as many consistent reads | about US$0.15 |
| S3 | about 2,000 PUTs, a few thousand LIST/GET, under 50 MB | about US$0.02 |
| CloudWatch Logs | under 10 MB ingested, 7-day retention | under US$0.01 |
| Data transfer | dbt reads a few MB from S3 to the laptop | under US$0.01 |
| **Total** | | **under US$0.50** |

Leaving the stack deployed costs almost nothing while idle (no provisioned capacity), but the
bucket and logs keep a small storage charge, so step 8 should follow on the same day.
