"""The trace job (spec §5 Tracing) and `make trace ENV=local|aws`, which submits it.

The job opens warehouse/<env>.duckdb read-only inside the `duckdb` pool. `make trace`
submits it to the running `dagster dev` over GraphQL, so it goes through the run queue
and the pool like every other run, then waits and prints state/<env>/trace/<run_id>.json.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

from dagster import Config, Failure, OpExecutionContext, job, op
from dagster_graphql import DagsterGraphQLClient

from generator.config import state_dir
from generator.state import write_json
from tracing.cli import TraceError, format_table, parse_start, trace_warehouse

from .resources import DAGSTER_ENVS, POOL, OnsideResource

TRACE_JOB = "trace"
HOST = "127.0.0.1"
PORT = 3000
WAIT_SECONDS = 600
TERMINAL = {"SUCCESS", "FAILURE", "CANCELED"}


class TraceConfig(Config):
    property_id: str
    start_time: str  # ISO 8601 with a UTC offset
    max_hops: int = 3
    env: str | None = None  # set by `make trace`; must match the Dagster that runs it


@op(pool=POOL, description="Hop-limited earliest-arrival trace over the read-only warehouse.")
def trace_property(context: OpExecutionContext, config: TraceConfig, onside: OnsideResource):
    result_path = onside.trace_dir() / f"{context.run_id}.json"
    record = {
        "run_id": context.run_id,
        "env": onside.env,
        "property_id": config.property_id,
        "start_time": config.start_time,
        "max_hops": config.max_hops,
    }
    try:
        if config.env is not None and config.env != onside.env:
            raise TraceError(
                f"this Dagster serves ENV={onside.env}, the trace asked for ENV={config.env}; "
                f"start `make dagster ENV={config.env}` instead"
            )
        rows = trace_warehouse(
            onside.warehouse(), config.property_id, config.start_time, config.max_hops
        )
    except TraceError as exc:
        write_json(result_path, {**record, "error": str(exc)})
        raise Failure(description=str(exc)) from None
    except Exception as exc:  # e.g. a missing table or a locked warehouse: keep the contract total
        write_json(result_path, {**record, "error": f"{type(exc).__name__}: {exc}"})
        raise
    context.log.info("\n" + format_table(rows, config.property_id, config.max_hops))
    write_json(result_path, {**record, "reached": rows})


@job(name=TRACE_JOB, description="Trace one property's downstream reach (spec §5 Tracing).")
def trace_job():
    trace_property()


def short_reason(exc: BaseException) -> str:
    """DagsterGraphQLClientError's text carries the whole query; prefer the underlying cause."""
    return (str(exc.__cause__ or exc).splitlines() or [type(exc).__name__])[0]


def dagster_running(host: str, port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://{host}:{port}/server_info", timeout=2):
            return True
    except (urllib.error.URLError, OSError):
        return False


def submit(
    env: str,
    property_id: str,
    start: str,
    max_hops: int,
    *,
    host: str = HOST,
    port: int = PORT,
    timeout: float = WAIT_SECONDS,
    poll: float = 1.0,
    client_factory=DagsterGraphQLClient,
    sleep=time.sleep,
    clock=time.monotonic,
) -> int:
    try:
        parse_start(start)  # fail fast, before a run is queued
    except TraceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if not dagster_running(host, port):
        print(
            f"error: Dagster is not running on {host}:{port}; start `make dagster ENV={env}` first",
            file=sys.stderr,
        )
        return 2
    client = client_factory(host, port_number=port)
    config = {"property_id": property_id, "start_time": start, "max_hops": max_hops, "env": env}
    try:
        run_id = client.submit_job_execution(
            TRACE_JOB, run_config={"ops": {"trace_property": {"config": config}}}
        )
    except Exception as exc:  # the client wraps every transport error in DagsterGraphQLClientError
        print(f"error: Dagster did not accept the trace run: {short_reason(exc)}", file=sys.stderr)
        return 2
    url = f"http://{host}:{port}/runs/{run_id}"
    print(f"queued trace run {run_id} ({url})", flush=True)
    deadline = clock() + timeout
    try:
        while (status := client.get_run_status(run_id).value) not in TERMINAL:
            if clock() > deadline:
                print(f"error: run {run_id} still {status} after {timeout:.0f}s", file=sys.stderr)
                return 1
            sleep(poll)
    except Exception as exc:  # e.g. Dagster stopped mid-wait
        print(
            f"error: lost Dagster while waiting for run {run_id}: {short_reason(exc)}",
            file=sys.stderr,
        )
        return 1
    # The op records under the env of the Dagster that ran it, which may not be the CLI's env.
    candidates = [
        state_dir(e) / "trace" / f"{run_id}.json" for e in dict.fromkeys((env, *DAGSTER_ENVS))
    ]
    path = next((p for p in candidates if p.exists()), candidates[0])
    result = json.loads(path.read_text()) if path.exists() else {}
    if status != "SUCCESS" or "reached" not in result:
        reason = result.get("error", "see the run's logs")
        print(f"error: trace run {run_id} {status}: {reason} ({url})", file=sys.stderr)
        return 1
    print(format_table(result["reached"], property_id, max_hops))
    print(f"result: {path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="orchestration.trace_job")
    parser.add_argument("--env", choices=DAGSTER_ENVS, required=True)
    parser.add_argument("--property", required=True)
    parser.add_argument("--start", required=True, help="infection start, ISO 8601 with offset")
    parser.add_argument("--max-hops", type=int, default=3)
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--timeout", type=float, default=WAIT_SECONDS)
    args = parser.parse_args(argv)
    return submit(
        args.env,
        args.property,
        args.start,
        args.max_hops,
        host=args.host,
        port=args.port,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    sys.exit(main())
