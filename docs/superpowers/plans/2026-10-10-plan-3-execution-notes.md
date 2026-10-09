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
0.0103), P0111 (25, 0.0094), P0175 (25, 0.0089); `make test` 278 passed, 1 skipped.

## Deviations from the plan

- `make trace ENV=local|aws` prints the trace table and then the result file's path
  (`result: state/<env>/trace/<run_id>.json`); brief decision 7 says it "prints the result file".
- The demo traces from P0172, the top PageRank property of this backfill, instead of P0003.
