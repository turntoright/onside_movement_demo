import json
from datetime import datetime

import pytest
from archiver import handler as A
from botocore.exceptions import ClientError

NZ_MORNING = datetime.fromisoformat("2026-03-04T09:30:00+13:00")  # 2026-03-03 20:30 UTC


def envelope(i: int) -> dict:
    return {
        "id": f"env-{i}",
        "source": "onside.app",
        "detail-type": "CheckIn",
        "time": "2025-01-01T00:00:00Z",  # never used for the key
        "detail": {"event_id": f"E{i}", "answers": [{"q": "a b"}]},
    }


def sqs_event(n: int) -> dict:
    # Bodies arrive pretty-printed to prove the archiver rewrites them as compact lines.
    records = [{"messageId": f"m{i}", "body": json.dumps(envelope(i), indent=2)} for i in range(n)]
    return {"Records": records}


def objects(aws) -> list[str]:
    return [o["Key"] for o in aws.s3.list_objects_v2(Bucket=aws.bucket).get("Contents", [])]


def test_one_object_per_batch_with_one_compact_line_per_envelope(aws):
    clock, new_id = (lambda: NZ_MORNING), (lambda: "k")
    result = A.handle(sqs_event(100), s3=aws.s3, bucket=aws.bucket, now=clock, new_id=new_id)
    assert result == {"archived": 100, "key": "raw/2026/03/03/20/k.ndjson"}
    assert objects(aws) == ["raw/2026/03/03/20/k.ndjson"]
    body = aws.s3.get_object(Bucket=aws.bucket, Key=result["key"])["Body"].read().decode()
    lines = body.splitlines()
    assert len(lines) == 100 and body.endswith("\n")
    assert lines[0] == json.dumps(envelope(0), separators=(",", ":"))
    assert [json.loads(line) for line in lines] == [envelope(i) for i in range(100)]


def test_each_invocation_writes_its_own_object(aws):
    A.handle(sqs_event(3), s3=aws.s3, bucket=aws.bucket)
    A.handle(sqs_event(3), s3=aws.s3, bucket=aws.bucket)
    keys = objects(aws)
    assert len(keys) == 2 and all(k.startswith("raw/") and k.endswith(".ndjson") for k in keys)


def test_failed_s3_write_fails_the_whole_batch(aws):
    with pytest.raises(ClientError):
        A.handle(sqs_event(5), s3=aws.s3, bucket="no-such-bucket")
    assert objects(aws) == []


def test_a_body_that_is_not_json_fails_the_whole_batch(aws):
    event = sqs_event(2)
    event["Records"].append({"messageId": "bad", "body": "{"})
    with pytest.raises(json.JSONDecodeError):
        A.handle(event, s3=aws.s3, bucket=aws.bucket)
    assert objects(aws) == []


def test_empty_batch_writes_nothing(aws):
    assert A.handle({"Records": []}, s3=aws.s3, bucket=aws.bucket)["archived"] == 0
    assert objects(aws) == []


def test_lambda_entry_point_uses_environment(aws, monkeypatch):
    monkeypatch.setenv("BUCKET", aws.bucket)
    monkeypatch.setattr(A, "_S3", None)
    assert A.handler(sqs_event(2), None)["archived"] == 2
    assert len(objects(aws)) == 1
