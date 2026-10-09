import json

import pytest

from generator import aws as A
from generator import state as S
from generator.config import DEFAULT_SCALE, ENVS, SCALES


def test_environments_and_their_default_scales():
    assert ENVS == ("sim", "local", "aws")
    assert DEFAULT_SCALE == {"sim": "ci", "local": "local", "aws": "aws"}
    local = SCALES["local"]
    assert (local.n_properties, local.n_people, local.n_checkins) == (200, 600, 10_000)


def test_local_clients_go_to_localstack_with_test_credentials(monkeypatch):
    monkeypatch.delenv("LOCALSTACK_ENDPOINT", raising=False)
    kwargs = A.client_kwargs("local")
    assert kwargs["endpoint_url"] == "http://localhost:4566"
    assert kwargs["region_name"] == "ap-southeast-2"
    assert (kwargs["aws_access_key_id"], kwargs["aws_secret_access_key"]) == ("test", "test")
    monkeypatch.setenv("LOCALSTACK_ENDPOINT", "http://localhost:4599")
    assert A.client_kwargs("local")["endpoint_url"] == "http://localhost:4599"


def test_aws_clients_use_the_default_credential_chain():
    assert A.client_kwargs("aws") == {"region_name": "ap-southeast-2"}
    assert A.client_kwargs("aws", "us-west-2") == {"region_name": "us-west-2"}
    assert A.client_kwargs("local", "us-west-2")["region_name"] == "us-west-2"


def test_local_clients_fail_fast_and_aws_clients_keep_retrying():
    local = A.client_config("local")
    assert local.retries == {"max_attempts": 3, "mode": "standard"}
    assert local.connect_timeout == 2
    assert local.s3 == {"addressing_style": "path"}
    remote = A.client_config("aws")
    assert remote.retries == {"max_attempts": 10, "mode": "standard"}
    assert remote.connect_timeout == 60  # the botocore default
    assert remote.s3 is None


def test_sim_makes_no_aws_calls():
    with pytest.raises(S.GeneratorError, match="no AWS calls"):
        A.client_kwargs("sim")


def test_outputs_are_read_from_terraform_output_json(aws, tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    with pytest.raises(S.GeneratorError, match="make deploy ENV=local"):
        A.load_outputs("local")
    (tmp_path / "state" / "local").mkdir(parents=True)
    (tmp_path / "state" / "local" / "outputs.json").write_text(json.dumps(aws.outputs()))
    outputs = A.load_outputs("local")
    assert outputs.bucket == aws.bucket
    assert outputs.archive_dlq_url == aws.queue_urls["archive_dlq"]


def test_manifest_records_its_start_and_can_be_read_back(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    started = "2026-10-10T01:00:00+00:00"
    S.write_manifest("local", "run-1", "backfill", 42, {"CheckIn": 2}, started_at=started)
    assert S.read_manifest("local", "run-1")["started_at"] == started
    with pytest.raises(S.GeneratorError, match="no manifest"):
        S.read_manifest("local", "run-2")


def test_the_aws_hint_checks_the_setup_before_any_destroy():
    # An expired session or a stray endpoint variable looks like a lost stack (spec §3.2).
    hint = S.reset_hint("aws")
    assert hint.startswith(
        "check credentials, region and endpoint variables (docs/aws-deploy.md steps 1-2); "
    )
    assert hint.endswith(
        "only if the stack is really gone: run `make destroy ENV=aws`, then deploy and setup again"
    )
    assert S.reset_hint("local").startswith("run `make up`")


def test_local_inconsistency_says_make_up(tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    S.write_manifest("local", "run-1", "backfill", 42, {"CheckIn": 1})
    with pytest.raises(S.GeneratorError, match="make up"):
        S.check_consistency("local", None)
    with pytest.raises(S.GeneratorError, match="make up"):
        S.check_remote_consistency("local", {"runs": ["run-1"]}, set())
    with pytest.raises(S.GeneratorError, match="make destroy ENV=aws"):
        S.check_remote_consistency("aws", None, {"run-9"})
    S.check_remote_consistency("local", {"runs": ["run-1"]}, {"run-1"})


def _connection(aws):
    return A.Connection(
        "local",
        A.Outputs(**{k: v["value"] for k, v in aws.outputs().items()}),
        aws.s3,
        aws.dynamodb,
        aws.events,
        aws.sqs,
    )


def test_listing_a_lost_bucket_says_make_up(aws):
    conn = _connection(aws)
    conn.put_json("control/manifests/run-1.json", {})
    assert conn.remote_manifest_ids() == {"run-1"}
    aws.s3.delete_object(Bucket=aws.bucket, Key="control/manifests/run-1.json")
    aws.s3.delete_bucket(Bucket=aws.bucket)
    with pytest.raises(S.GeneratorError, match="make up"):
        conn.keys("control/")
    with pytest.raises(S.GeneratorError, match="make up"):
        conn.remote_manifest_ids()


def test_listing_with_localstack_down_says_make_up(aws):
    from botocore.exceptions import EndpointConnectionError

    class Down:
        def get_paginator(self, name):
            raise EndpointConnectionError(endpoint_url="http://localhost:4566")

    conn = A.Connection("local", _connection(aws).outputs, Down(), None, None, None)
    with pytest.raises(S.GeneratorError, match="make up"):
        conn.keys("control/")


def test_objects_carry_last_modified_and_get_json_reads_back(aws):
    conn = _connection(aws)
    conn.put_json("control/settled/run-1.json", {"run_id": "run-1"})
    conn.put_json("raw/2026/10/10/01/x.ndjson", {})
    [entry] = conn.objects("control/settled/")
    assert entry["Key"] == "control/settled/run-1.json"
    assert entry["LastModified"].tzinfo is not None
    assert conn.get_json(entry["Key"]) == {"run_id": "run-1"}
    aws.s3.delete_object(Bucket=aws.bucket, Key="control/settled/run-1.json")
    aws.s3.delete_object(Bucket=aws.bucket, Key="raw/2026/10/10/01/x.ndjson")
    aws.s3.delete_bucket(Bucket=aws.bucket)
    with pytest.raises(S.GeneratorError, match="make up"):
        conn.objects("control/settled/")
