import json
import shutil
from pathlib import Path

import boto3
import pytest

from generator import aws as A
from generator import cli
from generator.cli import main
from tests.aws_fixtures import REGION

ROOT = Path(__file__).resolve().parents[1]
ANCHOR = "2026-10-01T00:00:00+00:00"


@pytest.fixture
def repo(aws, tmp_path, monkeypatch):
    """A deployed `local` environment backed by moto; nothing reaches LocalStack or AWS."""
    shutil.copytree(ROOT / "shared", tmp_path / "shared")
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setattr(A, "client", lambda env, service: boto3.client(service, region_name=REGION))
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


def test_failed_poison_emit_points_to_the_recovery(aws, repo, monkeypatch, capsys):
    def failing_emit(emissions, events_client, bus_name):
        raise cli.S.GeneratorError("PutEvents rejected")

    monkeypatch.setattr(cli, "emit", failing_emit)
    assert main(["poison", "--env", "local", "--k", "1", "--timeout", "0"]) == 2
    err = capsys.readouterr().err
    assert "PutEvents rejected" in err
    assert "make up" in err
