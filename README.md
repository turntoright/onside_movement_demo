# Onside movement demo

A portfolio demo: farm check-ins → stand-down (biosecurity) alerts in real time →
a warehouse that recomputes and reconciles them. Design:
`docs/superpowers/specs/2026-10-09-onside-movement-demo-design.md`.

Quick start (offline):

    make venv
    make test

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

## Status (Plan 2: AWS wiring on LocalStack)

The same chain that Terraform deploys to AWS, on LocalStack 4.14.0 (community, no token):

    make up                                   # a fresh, empty LocalStack (also wipes state/local)
    make deploy ENV=local                     # Terraform (26 resources): bus, 2 rules, 2 queues + 2 DLQs,
                                              # 2 Lambdas, 3 DynamoDB tables, the bucket
    make setup ENV=local                      # reference data -> S3 ref/ and DynamoDB properties
    make generate ENV=local MODE=backfill     # emit, then settle and compact (about 16 min)
    make dbt-build ENV=local                  # dbt reads s3://<bucket>/compacted/local/
    make alerts ENV=local                     # alert log lines and distinct alerts
    make generate ENV=local MODE=live N=50    # the demo step; settles in under a minute
    make poison ENV=local K=2                 # malformed events reach the visit DLQ (~3 min)
    make settle ENV=local RUN=<run_id>        # resume a settle that timed out
    make integration                          # the full chain on a fresh stack; make up after

`make deploy` and `make destroy` never default: they need `ENV=local` or `ENV=aws` on the
command line. `make test` needs no Docker (moto stands in for AWS).
Only the first `make deploy` of an environment needs the network (`terraform init` fetches the
providers into `infra/terraform/.terraform-<env>/`); later deploys, including after `make up`,
skip init and run offline.

Measured on a 4-CPU laptop (`local` scale: 200 properties, 600 people, 9,227 check-ins,
18,313 events): emitted at 131 events/s, settled 14 minutes later, 16.6 minutes in total
(about 18 events/s end to end); `dbt build` PASS=77, ERROR=0; reconciliation 981 matched
(equal to the 981 distinct alerts from `make alerts`), 52 missed with a version-proven cause,
0 false alerts, 0 unexplained.

Next: Plan 3 (Dagster, CI LocalStack job, the AWS deploy, the demo).
