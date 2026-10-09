import json
import shutil
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from generator import aws as A
from generator import cli
from generator.cli import main

ROOT = Path(__file__).resolve().parents[1]
ANCHOR = "2026-10-01T00:00:00+00:00"


@pytest.fixture
def repo(aws, tmp_path, monkeypatch):
    """A deployed `local` environment backed by moto; nothing reaches LocalStack or AWS."""
    shutil.copytree(ROOT / "shared", tmp_path / "shared")
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setattr(
        A, "client", lambda env, service, region: boto3.client(service, region_name=region)
    )
    (tmp_path / "state" / "local").mkdir(parents=True)
    (tmp_path / "state" / "local" / "outputs.json").write_text(json.dumps(aws.outputs()))
    return tmp_path


def s3_keys(aws, prefix: str) -> list[str]:
    listing = aws.s3.list_objects_v2(Bucket=aws.bucket, Prefix=prefix)
    return sorted(o["Key"] for o in listing.get("Contents", []))


def s3_json(aws, key: str):
    return json.loads(aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read())


def test_setup_loads_the_same_snapshot_into_s3_and_dynamodb(aws, repo):
    assert main(["setup", "--env", "local", "--scale", "ci"]) == 0
    assert s3_keys(aws, "ref/") == ["ref/companies.json", "ref/people.json", "ref/properties.json"]
    items = aws.dynamodb.scan(TableName="properties")["Items"]
    local = json.loads((repo / "data" / "local" / "ref" / "properties.json").read_text())
    assert len(items) == len(local) == 80
    assert s3_json(aws, "ref/properties.json") == local
    setup = json.loads((repo / "state" / "local" / "setup.json").read_text())
    assert setup == {"scale": "ci", "seed": 42}


def test_setup_defaults_to_the_local_scale(aws, repo):
    assert main(["setup", "--env", "local"]) == 0
    assert json.loads((repo / "state" / "local" / "setup.json").read_text())["scale"] == "local"


def test_second_setup_is_refused_with_make_up(aws, repo, capsys):
    main(["setup", "--env", "local", "--scale", "aws"])
    shutil.rmtree(repo / "data" / "local")  # the S3 copy alone is enough to refuse
    assert main(["setup", "--env", "local", "--scale", "aws"]) == 2
    assert "make up" in capsys.readouterr().err


def test_setup_before_deploy_is_refused(aws, repo, capsys):
    (repo / "state" / "local" / "outputs.json").unlink()
    assert main(["setup", "--env", "local"]) == 2
    assert "make deploy ENV=local" in capsys.readouterr().err


def test_generate_writes_the_manifest_to_s3_before_emitting(aws, repo, monkeypatch):
    main(["setup", "--env", "local", "--scale", "aws"])
    seen = {}

    def fake_emit(emissions, events_client, bus_name):
        seen["manifests"] = s3_keys(aws, "control/manifests/")
        seen["types"] = {e.detail_type for e in emissions}
        return len(emissions)

    monkeypatch.setattr(cli, "emit", fake_emit)
    args = ["generate", "--env", "local", "--mode", "backfill", "--anchor", ANCHOR, "--no-settle"]
    assert main(args) == 0
    run_id = json.loads((repo / "state" / "local" / "generator_state.json").read_text())["runs"][0]
    assert seen["manifests"] == [f"control/manifests/{run_id}.json"]
    assert seen["types"] == {"CheckIn", "CheckOut", "PlantMovement"}
    manifest = s3_json(aws, f"control/manifests/{run_id}.json")
    assert manifest["started_at"] and manifest["total"] == sum(manifest["counts"].values())


def test_generate_emits_and_then_settles(aws, repo, monkeypatch):
    main(["setup", "--env", "local", "--scale", "aws"])
    settled = []
    monkeypatch.setattr(cli, "settle_run", lambda conn, manifest, timeout: settled.append(manifest))
    assert main(["generate", "--env", "local", "--mode", "backfill", "--anchor", ANCHOR]) == 0
    assert [m["mode"] for m in settled] == ["backfill"]


def test_lost_localstack_data_is_detected(aws, repo, capsys):
    main(["setup", "--env", "local", "--scale", "aws"])
    main(["generate", "--env", "local", "--mode", "backfill", "--anchor", ANCHOR, "--no-settle"])
    for key in s3_keys(aws, "control/manifests/"):
        aws.s3.delete_object(Bucket=aws.bucket, Key=key)
    assert main(["generate", "--env", "local", "--mode", "live", "--no-settle"]) == 2
    assert "make up" in capsys.readouterr().err


def test_poison_records_its_own_manifest_and_waits_for_the_visit_dlq(aws, repo):
    for _ in range(2):  # stands in for LocalStack moving the poison messages to the DLQ
        aws.sqs.send_message(QueueUrl=aws.queue_urls["visit_dlq"], MessageBody="{}")
    assert main(["poison", "--env", "local", "--k", "2", "--timeout", "0"]) == 0
    (key,) = s3_keys(aws, "control/poison/")
    assert s3_json(aws, key)["k"] == 2
    assert s3_keys(aws, "control/manifests/") == []
    assert len(list((repo / "state" / "local" / "poison").glob("*.json"))) == 1


def test_poison_fails_when_the_dlq_stays_short(aws, repo, capsys):
    assert main(["poison", "--env", "local", "--k", "1", "--timeout", "0"]) == 2
    assert "visit DLQ holds 0 messages" in capsys.readouterr().err


def test_settle_needs_a_known_run(aws, repo, capsys):
    assert main(["settle", "--env", "local", "--run", "nope"]) == 2
    assert "no manifest" in capsys.readouterr().err


def test_restarted_localstack_is_detected_with_make_up(aws, repo, capsys):
    """A real restart loses every object and the bucket itself."""
    main(["setup", "--env", "local", "--scale", "aws"])
    main(["generate", "--env", "local", "--mode", "backfill", "--anchor", ANCHOR, "--no-settle"])
    for key in s3_keys(aws, ""):
        aws.s3.delete_object(Bucket=aws.bucket, Key=key)
    aws.s3.delete_bucket(Bucket=aws.bucket)
    capsys.readouterr()
    assert main(["generate", "--env", "local", "--mode", "live", "--no-settle"]) == 2
    err = capsys.readouterr().err
    assert "make up" in err
    assert "Traceback" not in err
    shutil.rmtree(repo / "data" / "local")
    assert main(["setup", "--env", "local", "--scale", "aws"]) == 2
    assert "make up" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("env", "hint"),
    [("local", "make up"), ("aws", "make destroy ENV=aws")],
)
def test_reset_refuses_deployed_environments(repo, capsys, env, hint):
    keep = repo / "state" / env
    keep.mkdir(parents=True, exist_ok=True)
    (keep / "outputs.json").write_text("{}")
    assert main(["reset", "--env", env]) == 2
    assert hint in capsys.readouterr().err
    assert (keep / "outputs.json").exists()


def test_failed_emit_after_the_manifest_points_to_the_recovery(aws, repo, monkeypatch, capsys):
    main(["setup", "--env", "local", "--scale", "aws"])

    def failing_emit(emissions, events_client, bus_name):
        raise cli.S.GeneratorError("PutEvents rejected")

    monkeypatch.setattr(cli, "emit", failing_emit)
    args = ["generate", "--env", "local", "--mode", "backfill", "--anchor", ANCHOR, "--no-settle"]
    assert main(args) == 2
    err = capsys.readouterr().err
    assert "PutEvents rejected" in err
    assert "make up" in err
    # The state the hint explains: manifest recorded, generator state never saved.
    (manifest_key,) = s3_keys(aws, "control/manifests/")
    assert len(list((repo / "state" / "local" / "manifests").glob("*.json"))) == 1
    assert manifest_key.endswith(".json")
    assert not (repo / "state" / "local" / "generator_state.json").exists()


@pytest.mark.parametrize(
    "error",
    [
        EndpointConnectionError(endpoint_url="http://localhost:4566"),
        ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "PutEvents"),
    ],
)
def test_aws_errors_from_emit_get_the_recovery_hint(aws, repo, monkeypatch, capsys, error):
    main(["setup", "--env", "local", "--scale", "aws"])

    def failing_emit(emissions, events_client, bus_name):
        raise error

    monkeypatch.setattr(cli, "emit", failing_emit)
    args = ["generate", "--env", "local", "--mode", "backfill", "--anchor", ANCHOR, "--no-settle"]
    assert main(args) == 2
    err = capsys.readouterr().err
    assert "make up" in err
    assert "Traceback" not in err


def test_poison_events_carry_the_poison_flag_and_the_dlq_target_sums_runs(aws, repo, monkeypatch):
    sent = []

    def fake_emit(emissions, events_client, bus_name):
        sent.extend(emissions)
        return len(emissions)

    waits = []
    monkeypatch.setattr(cli, "emit", fake_emit)
    monkeypatch.setattr(
        cli.Q, "wait_for_dlq", lambda sqs, url, expected, **kw: waits.append(expected)
    )
    assert main(["poison", "--env", "local", "--k", "2"]) == 0
    assert main(["poison", "--env", "local", "--k", "3"]) == 0
    assert all(e.detail["poison"] is True for e in sent) and len(sent) == 5
    assert waits == [2, 5]


def test_poison_on_a_lost_stack_writes_no_local_record(aws, repo, capsys):
    for key in s3_keys(aws, ""):
        aws.s3.delete_object(Bucket=aws.bucket, Key=key)
    aws.s3.delete_bucket(Bucket=aws.bucket)
    assert main(["poison", "--env", "local", "--k", "1", "--timeout", "0"]) == 2
    assert "make up" in capsys.readouterr().err
    assert not (repo / "state" / "local" / "poison").exists()


def test_unprocessed_property_items_are_resent_with_backoff(aws, repo, monkeypatch):
    main(["setup", "--env", "local", "--scale", "ci"])
    delays = []
    monkeypatch.setattr(cli, "sleep", delays.append)

    class Flaky:
        def __init__(self):
            self.calls = 0

        def batch_write_item(self, RequestItems):
            self.calls += 1
            return {"UnprocessedItems": RequestItems if self.calls <= 3 else {}}

    conn = A.connect("local")
    flaky = Flaky()
    conn = type(conn)(conn.env, conn.outputs, conn.s3, flaky, conn.events, conn.sqs)
    ref = cli.Reference.read(cli.ref_dir("local"))
    cli._load_properties(conn, ref)
    assert delays[:3] == [0.1, 0.2, 0.4]


def test_failed_poison_emit_points_to_the_recovery(aws, repo, monkeypatch, capsys):
    def failing_emit(emissions, events_client, bus_name):
        raise cli.S.GeneratorError("PutEvents rejected")

    monkeypatch.setattr(cli, "emit", failing_emit)
    assert main(["poison", "--env", "local", "--k", "1", "--timeout", "0"]) == 2
    err = capsys.readouterr().err
    assert "PutEvents rejected" in err
    assert "make up" in err


def test_alerts_counts_json_alert_lines_across_pages(repo, monkeypatch, capsys):
    def alert(n: int, detected: str) -> str:
        return json.dumps(
            {
                "level": "alert",
                "checkin_event_id": f"c{n}",
                "person_id": f"U{n:05d}",
                "origin_property_id": f"P{n:04d}",
                "dest_property_id": f"P{n + 100:04d}",
                "rule_id": "R02",
                "detected_time": detected,
            }
        )

    pages = [
        {
            "events": [
                {"message": "START RequestId: x Version: $LATEST\n"},
                {"message": alert(1, "2026-10-10T09:00:00+13:00")},
                {"message": alert(2, "2026-10-10T08:00:00+13:00")},
                {"message": alert(3, "2026-10-09T21:30:00+00:00")},  # 10:30 in Auckland
                {"message": json.dumps({"level": "error", "msg": "poison"})},
            ],
            "nextToken": "t1",
        },
        {
            "events": [
                {"message": alert(4, "2026-10-10T10:00:00+13:00")},
                {"message": alert(1, "2026-10-10T09:00:00+13:00")},  # a repeat logs again
                {"message": alert(5, "2026-10-10T11:00:00+13:00")},
                {"message": alert(6, "2026-10-10T07:00:00+13:00")},
                {"message": alert(7, "2026-10-10T12:00:00+13:00")},
                {"message": alert(8, "2026-10-10T23:00:00")},  # no offset: counted, not listed
                {"message": "END RequestId: x\n"},
                {"message": "REPORT RequestId: x Duration: 1 ms\n"},
            ]
        },
    ]

    class Logs:
        def __init__(self):
            self.calls = []

        def filter_log_events(self, **kwargs):
            self.calls.append(kwargs)
            return pages[len(self.calls) - 1]

    logs = Logs()
    monkeypatch.setattr(A, "client", lambda env, service, region: logs)
    assert main(["alerts", "--env", "local"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0] == (
        "9 alert log lines, 8 distinct alerts "
        "(by checkin_event_id; repeats are redeliveries or duplicate events)"
    )
    assert out[1].startswith("last 5 alerts (detected time, person")
    assert out[2:] == [  # the latest five by time, oldest first; c2 and c6 are older
        "  2026-10-10T09:00:00+13:00  U00001  P0001 -> P0101  R02",
        "  2026-10-10T10:00:00+13:00  U00004  P0004 -> P0104  R02",
        "  2026-10-09T21:30:00+00:00  U00003  P0003 -> P0103  R02",
        "  2026-10-10T11:00:00+13:00  U00005  P0005 -> P0105  R02",
        "  2026-10-10T12:00:00+13:00  U00007  P0007 -> P0107  R02",
    ]
    assert logs.calls[1]["nextToken"] == "t1"
    assert logs.calls[0]["logGroupName"] == "/aws/lambda/onside-standdown-checker"


def test_alerts_and_clients_follow_the_terraform_outputs(repo, monkeypatch, capsys):
    """Renamed function, other region: nothing hard-coded drifts from the deployed stack."""
    path = repo / "state" / "local" / "outputs.json"
    outputs = json.loads(path.read_text())
    outputs["standdown_function"]["value"] = "plan3-standdown-checker"
    outputs["region"]["value"] = "us-west-2"
    path.write_text(json.dumps(outputs))
    built, groups = [], []

    class Logs:
        def filter_log_events(self, **kwargs):
            groups.append(kwargs["logGroupName"])
            return {"events": []}

    def fake_client(env, service, region):
        built.append((service, region))
        return Logs()

    monkeypatch.setattr(A, "client", fake_client)
    assert main(["alerts", "--env", "local"]) == 0
    assert groups == ["/aws/lambda/plan3-standdown-checker"]
    A.connect("local")
    assert {region for _, region in built} == {"us-west-2"}
    assert sorted(service for service, _ in built) == ["dynamodb", "events", "logs", "s3", "sqs"]


def test_alerts_refuses_sim(capsys):
    assert main(["alerts", "--env", "sim"]) == 2


@pytest.mark.parametrize(
    "error",
    [
        EndpointConnectionError(endpoint_url="http://localhost:4566"),
        ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "no log group"}},
            "FilterLogEvents",
        ),
    ],
)
def test_unhandled_aws_errors_print_the_reset_hint_without_a_traceback(
    repo, monkeypatch, capsys, error
):
    class Logs:
        def filter_log_events(self, **kwargs):
            raise error

    monkeypatch.setattr(A, "client", lambda env, service, region: Logs())
    assert main(["alerts", "--env", "local"]) == 2
    err = capsys.readouterr().err
    assert err.startswith("error: ")
    assert str(error) in err
    assert "make up" in err
    assert "Traceback" not in err


def test_aws_errors_while_settling_point_to_make_settle(aws, repo, capsys):
    run_id = "6f1c2a3e-0000-4000-8000-000000000001"
    started = "2026-10-10T01:00:00+00:00"
    cli.S.write_manifest("local", run_id, "live", 42, {"CheckIn": 1}, started_at=started)
    aws.sqs.delete_queue(QueueUrl=aws.queue_urls["archive_dlq"])  # e.g. LocalStack restarted
    assert main(["settle", "--env", "local", "--run", run_id, "--timeout", "0"]) == 2
    err = capsys.readouterr().err
    assert "NonExistentQueue" in err or "QueueDoesNotExist" in err
    assert "the spool is kept" in err
    assert f"`make settle ENV=local RUN={run_id}`" in err
    assert "Traceback" not in err
