"""AWS access per environment (spec §3.2): LocalStack for local, the credential chain for aws."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, fields

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError as BotoConnectionError

from .config import state_dir
from .state import GeneratorError, reset_hint

REGION = "ap-southeast-2"  # the default only; deployed stacks use outputs.region
LOCALSTACK_ENDPOINT = "http://localhost:4566"


def client_kwargs(env: str, region: str = REGION) -> dict:
    if env == "local":
        return {
            "region_name": region,
            "endpoint_url": os.environ.get("LOCALSTACK_ENDPOINT", LOCALSTACK_ENDPOINT),
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
        }
    if env == "aws":
        return {"region_name": region}  # default credential chain
    raise GeneratorError(f"ENV={env} makes no AWS calls; use ENV=local or ENV=aws")


def client_config(env: str) -> Config:
    if env == "local":  # LocalStack down should fail in seconds, not after a minute of retries
        return Config(
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=2,
            s3={"addressing_style": "path"},
        )
    return Config(retries={"max_attempts": 10, "mode": "standard"})


def client(env: str, service: str, region: str = REGION):
    return boto3.client(service, config=client_config(env), **client_kwargs(env, region))


@dataclass(frozen=True)
class Outputs:
    """`terraform output -json`, saved by `make deploy` to state/<env>/outputs.json."""

    region: str
    bucket: str
    person_table: str
    evaluation_table: str
    property_table: str
    bus_name: str
    visit_queue_url: str
    visit_dlq_url: str
    archive_queue_url: str
    archive_dlq_url: str
    standdown_function: str
    archiver_function: str


def load_outputs(env: str) -> Outputs:
    path = state_dir(env) / "outputs.json"
    if not path.exists():
        raise GeneratorError(f"no {path}; run `make deploy ENV={env}` first")
    raw = json.loads(path.read_text())
    try:
        return Outputs(**{f.name: raw[f.name]["value"] for f in fields(Outputs)})
    except KeyError as exc:
        raise GeneratorError(f"{path} lacks output {exc}; run `make deploy ENV={env}`") from None


@dataclass(frozen=True)
class Connection:
    env: str
    outputs: Outputs
    s3: object
    dynamodb: object
    events: object
    sqs: object

    def put_json(self, key: str, value: object) -> None:
        body = json.dumps(value, indent=1, sort_keys=True).encode()
        self.s3.put_object(Bucket=self.outputs.bucket, Key=key, Body=body)

    def objects(self, prefix: str) -> list[dict]:
        """The listing entries (Key, LastModified, ...) under a prefix."""
        # After a LocalStack restart the bucket is gone while state/<env>/outputs.json survives.
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.outputs.bucket, Prefix=prefix)
            return [obj for page in pages for obj in page.get("Contents", [])]
        except BotoConnectionError:
            raise self._gone() from None
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "NoSuchBucket":
                raise self._gone() from None
            raise

    def keys(self, prefix: str) -> list[str]:
        return [obj["Key"] for obj in self.objects(prefix)]

    def get_json(self, key: str):
        body = self.s3.get_object(Bucket=self.outputs.bucket, Key=key)["Body"].read()
        return json.loads(body)

    def _gone(self) -> GeneratorError:
        return GeneratorError(
            f"ENV={self.env}: the stack is gone or unreachable; {reset_hint(self.env)}"
        )

    def remote_manifest_ids(self) -> set[str]:
        prefix = "control/manifests/"
        return {k[len(prefix) :].removesuffix(".json") for k in self.keys(prefix)}


def connect(env: str) -> Connection:
    outputs = load_outputs(env)  # the region Terraform deployed to, not a second copy of it
    return Connection(
        env,
        outputs,
        client(env, "s3", outputs.region),
        client(env, "dynamodb", outputs.region),
        client(env, "events", outputs.region),
        client(env, "sqs", outputs.region),
    )
