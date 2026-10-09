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

Next: Plan 2 (AWS wiring on LocalStack), Plan 3 (Dagster, AWS deploy, demo).
