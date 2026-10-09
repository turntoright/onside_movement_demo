import json
import socket

import pytest
from dagster import DagsterRunStatus

from orchestration import trace_job as T
from orchestration.resources import OnsideResource
from tests.helpers import build_trace_warehouse

START = "2026-03-03T09:00:00+13:00"


@pytest.fixture
def repo(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    build_trace_warehouse(tmp_path / "warehouse" / "local.duckdb")
    return tmp_path


def execute(property_id="P1", env=None):
    config = {"property_id": property_id, "start_time": START, "max_hops": 3}
    if env:
        config["env"] = env
    return T.trace_job.execute_in_process(
        run_config={"ops": {"trace_property": {"config": config}}},
        resources={"onside": OnsideResource(env="local")},
        raise_on_error=False,
    )


def result_file(repo, run_id):
    return json.loads((repo / "state" / "local" / "trace" / f"{run_id}.json").read_text())


def test_the_trace_job_writes_its_result_per_run(repo):
    result = execute(env="local")
    assert result.success
    saved = result_file(repo, result.run_id)
    assert [r["path"] for r in saved["reached"]] == [["P1"], ["P1", "P2"]]
    assert saved["reached"][1]["arrival"] == "2026-03-03T12:00:00+13:00"
    assert (saved["env"], saved["property_id"], saved["max_hops"]) == ("local", "P1", 3)


def test_the_op_claims_the_duckdb_pool():
    assert T.trace_property.pool == "duckdb"


def test_an_unknown_property_fails_the_run_and_records_why(repo):
    result = execute("NOPE")
    assert not result.success
    assert result_file(repo, result.run_id)["error"] == "unknown property NOPE"


def test_a_trace_for_another_env_fails(repo):
    result = execute(env="aws")
    assert not result.success
    assert "this Dagster serves ENV=local" in result_file(repo, result.run_id)["error"]


def closed_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_submit_without_dagster_dev_says_start_it_first(capsys):
    code = T.main(
        ["--env", "local", "--property", "P1", "--start", START, "--port", str(closed_port())]
    )
    assert code == 2
    assert "start `make dagster ENV=local` first" in capsys.readouterr().err


def test_submit_refuses_a_start_without_offset_before_queueing(capsys):
    code = T.main(["--env", "local", "--property", "P1", "--start", "2026-03-03T09:00:00"])
    assert code == 2
    assert "needs a UTC offset" in capsys.readouterr().err


class FakeClient:
    def __init__(self, statuses, host=None, port_number=None):
        self.statuses = list(statuses)
        self.submitted = None

    def submit_job_execution(self, job_name, run_config):
        self.submitted = (job_name, run_config)
        return "run-1"

    def get_run_status(self, run_id):
        return self.statuses.pop(0)


def fake_submit(repo, monkeypatch, statuses, saved):
    monkeypatch.setattr(T, "dagster_running", lambda host, port: True)
    (repo / "state" / "local" / "trace").mkdir(parents=True)
    (repo / "state" / "local" / "trace" / "run-1.json").write_text(json.dumps(saved))
    client = FakeClient(statuses)
    code = T.submit(
        "local", "P1", START, 3, client_factory=lambda h, port_number: client, sleep=lambda s: None
    )
    return code, client


def test_submit_queues_the_job_waits_and_prints_the_result(repo, monkeypatch, capsys):
    reached = [
        {
            "property_id": "P1",
            "property_type": "poultry",
            "hops": 0,
            "arrival": "2026-03-03T09:00:00+13:00",
            "path": ["P1"],
        }
    ]
    code, client = fake_submit(
        repo,
        monkeypatch,
        [DagsterRunStatus.QUEUED, DagsterRunStatus.STARTED, DagsterRunStatus.SUCCESS],
        {"reached": reached},
    )
    assert code == 0
    assert client.submitted == (
        "trace",
        {
            "ops": {
                "trace_property": {
                    "config": {
                        "property_id": "P1",
                        "start_time": START,
                        "max_hops": 3,
                        "env": "local",
                    }
                }
            }
        },
    )
    out = capsys.readouterr().out
    assert "1 properties reached from P1 within 3 hops" in out
    assert "state/local/trace/run-1.json" in out


def test_submit_reports_a_failed_run_with_its_reason(repo, monkeypatch, capsys):
    code, _ = fake_submit(
        repo, monkeypatch, [DagsterRunStatus.FAILURE], {"error": "unknown property P1"}
    )
    assert code == 1
    assert "unknown property P1" in capsys.readouterr().err


def test_submit_prints_the_reason_when_another_envs_dagster_ran_it(repo, monkeypatch, capsys):
    monkeypatch.setattr(T, "dagster_running", lambda host, port: True)
    (repo / "state" / "aws" / "trace").mkdir(parents=True)
    (repo / "state" / "aws" / "trace" / "run-1.json").write_text(
        json.dumps({"error": "this Dagster serves ENV=aws, the trace asked for ENV=local"})
    )
    client = FakeClient([DagsterRunStatus.FAILURE])
    code = T.submit(
        "local", "P1", START, 3, client_factory=lambda h, port_number: client, sleep=lambda s: None
    )
    assert code == 1
    assert "this Dagster serves ENV=aws" in capsys.readouterr().err


def test_submit_reports_lost_dagster_instead_of_a_traceback(repo, monkeypatch, capsys):
    class DyingClient(FakeClient):
        def get_run_status(self, run_id):
            raise ConnectionError("connection refused")

    monkeypatch.setattr(T, "dagster_running", lambda host, port: True)
    client = DyingClient([])
    code = T.submit(
        "local", "P1", START, 3, client_factory=lambda h, port_number: client, sleep=lambda s: None
    )
    assert code == 1
    err = capsys.readouterr().err
    assert "error: lost Dagster while waiting for run run-1: connection refused" in err
