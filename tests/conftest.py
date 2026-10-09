import json
import os

import boto3
import pytest
from moto import mock_aws

from generator import aws as A
from tests.aws_fixtures import REGION, MotoStack


@pytest.fixture
def aws(monkeypatch):
    """A fresh moto account holding the Terraform stack. Nothing leaves the process."""
    for name, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": REGION,
    }.items():
        monkeypatch.setenv(name, value)
    for name in ("AWS_PROFILE", "AWS_ENDPOINT_URL"):
        monkeypatch.delenv(name, raising=False)
    for name in [n for n in os.environ if n.startswith("AWS_ENDPOINT_URL_")]:
        monkeypatch.delenv(name)
    with mock_aws():
        yield MotoStack.create()


@pytest.fixture
def deployed(aws, tmp_path, monkeypatch):
    """REPO_ROOT with a deployed `local` env; its AWS clients are moto's."""
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    monkeypatch.setattr(
        A, "client", lambda env, service, region: boto3.client(service, region_name=region)
    )
    (tmp_path / "state" / "local").mkdir(parents=True)
    (tmp_path / "state" / "local" / "outputs.json").write_text(json.dumps(aws.outputs()))
    return tmp_path
