import json
from datetime import datetime

import pytest

from generator.aws import Connection, Outputs
from generator.settle import Settler, SettleTimeout, hour_prefixes, iter_json
from generator.state import GeneratorError
from generator.stream import event_id

RUN = "7d0e8c1e-0000-4000-8000-000000000001"
CI1, CO1, CI2 = (event_id(RUN, seq) for seq in range(3))
MANIFEST = {
    "run_id": RUN,
    "mode": "live",
    "counts": {"CheckIn": 2, "CheckOut": 1, "PlantMovement": 0},
    "total": 3,
    "started_at": "2026-10-10T01:30:00+00:00",
}
NOW = datetime.fromisoformat("2026-10-10T02:10:00+00:00")


def env(envelope_id: str, detail_type: str, detail: dict, source="onside.standdown") -> dict:
    return {"id": envelope_id, "source": source, "detail-type": detail_type, "detail": detail}


APP = [
    env("g1", "CheckIn", {"event_id": CI1}, "onside.app"),
    env("g2", "CheckOut", {"event_id": CO1, "checkin_event_id": CI1}, "onside.app"),
    env("g3", "CheckIn", {"event_id": CI2}, "onside.app"),
    env("g1", "CheckIn", {"event_id": CI1}, "onside.app"),  # the same delivery archived twice
]
DERIVED = [
    env("d1", "CheckInEvaluated", {"checkin_event_id": CI1, "breach": False}),
    env("d2", "CheckInEvaluated", {"checkin_event_id": CI2, "breach": True}),
    env("d3", "CheckOutApplied", {"checkout_event_id": CO1}),
    env("d4", "HighRiskVisitDetected", {"checkin_event_id": CI2}),
    env("x1", "CheckInEvaluated", {"checkin_event_id": "earlier-run", "breach": True}),
]


class FakeTime:
    def __init__(self) -> None:
        self.t, self.sleeps, self.on_sleep = 0.0, [], []

    def clock(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds
        if self.on_sleep:
            self.on_sleep.pop(0)()


class StubSqs:
    """Queue attributes only: depth per queue URL (visible messages)."""

    def __init__(self) -> None:
        self.depth: dict[str, int] = {}

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        visible = self.depth.get(QueueUrl, 0)
        return {
            "Attributes": {
                "ApproximateNumberOfMessages": str(visible),
                "ApproximateNumberOfMessagesNotVisible": "0",
                "ApproximateNumberOfMessagesDelayed": "0",
            }
        }


@pytest.fixture
def conn(aws, tmp_path, monkeypatch):
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))
    outputs = Outputs(**{k: v["value"] for k, v in aws.outputs().items()})
    return Connection("local", outputs, aws.s3, aws.dynamodb, aws.events, StubSqs())


def put_raw(conn, key: str, envelopes: list[dict], *, concatenated: bool = False) -> None:
    sep = "" if concatenated else "\n"
    body = sep.join(json.dumps(e) for e in envelopes)
    conn.s3.put_object(Bucket=conn.outputs.bucket, Key=key, Body=body.encode())


def settler(conn, fake: FakeTime, **kwargs) -> Settler:
    options = dict(quiet_seconds=30, poll_seconds=5, timeout_seconds=600)
    options.update(kwargs)
    options.setdefault("log", lambda line: None)
    options.update(sleep=fake.sleep, clock=fake.clock, now=lambda: NOW)
    return Settler(MANIFEST, conn, **options)


def compacted_ids(conn, key: str) -> list[str]:
    body = conn.s3.get_object(Bucket=conn.outputs.bucket, Key=key)["Body"].read().decode()
    return [json.loads(line)["id"] for line in body.splitlines()]


def test_iter_json_reads_ndjson_and_concatenated_documents():
    assert list(iter_json('{"a":1}{"b":2}\n\n{"c":3}\n')) == [{"a": 1}, {"b": 2}, {"c": 3}]


def test_hour_prefixes_start_one_hour_before_the_run():
    start = datetime.fromisoformat("2026-10-10T14:30:00+13:00")  # 01:30 UTC
    assert hour_prefixes(start, NOW) == [
        "raw/2026/10/10/00/",
        "raw/2026/10/10/01/",
        "raw/2026/10/10/02/",
    ]


def test_complete_run_is_compacted_after_one_quiet_period(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP)
    put_raw(conn, "raw/2026/10/10/00/b.ndjson", DERIVED, concatenated=True)  # skew hour
    put_raw(conn, "raw/2026/10/09/23/old.ndjson", [env("old", "CheckIn", {"event_id": "z"})])
    fake = FakeTime()
    settled = settler(conn, fake).run()
    part = f"compacted/local/{RUN}/part-0000.ndjson"
    assert settled["parts"] == [part]
    ids = compacted_ids(conn, part)  # the duplicate g1 is dropped, the old hour never read
    assert sorted(ids) == ["d1", "d2", "d3", "d4", "g1", "g2", "g3", "x1"]
    assert settled["breaches"] == 1 and settled["envelopes"] == 8
    control = conn.s3.get_object(Bucket=conn.outputs.bucket, Key=f"control/settled/{RUN}.json")
    assert json.loads(control["Body"].read())["parts"] == [part]
    assert fake.sleeps == [30]
    spool = settler(conn, fake).spool
    assert not spool.exists() and (spool.parents[1] / "settled" / f"{RUN}.json").exists()


def test_waits_for_missing_derived_events(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP)
    fake = FakeTime()
    fake.on_sleep.append(lambda: put_raw(conn, "raw/2026/10/10/02/b.ndjson", DERIVED))
    settler(conn, fake).run()
    assert fake.sleeps == [5, 30]


def test_alert_gate_waits_for_every_breach(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP + DERIVED[:3])  # no alert for CI2 yet
    fake = FakeTime()
    fake.on_sleep.append(lambda: put_raw(conn, "raw/2026/10/10/02/b.ndjson", DERIVED[3:]))
    settler(conn, fake).run()
    assert fake.sleeps == [5, 30]


def test_busy_queue_delays_settling(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP + DERIVED)
    conn.sqs.depth[conn.outputs.visit_queue_url] = 1
    fake = FakeTime()
    fake.on_sleep.append(lambda: conn.sqs.depth.clear())
    settler(conn, fake).run()
    assert fake.sleeps == [5, 30]


def test_timeout_keeps_the_spool_and_a_resume_reads_only_new_objects(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP)
    fake = FakeTime()
    with pytest.raises(SettleTimeout, match=f"make settle ENV=local RUN={RUN}"):
        settler(conn, fake, timeout_seconds=10).run()
    assert settler(conn, fake).spool.exists()

    put_raw(conn, "raw/2026/10/10/02/b.ndjson", DERIVED)
    read, original = [], conn.s3.get_object

    def spy(**kwargs):
        read.append(kwargs["Key"])
        return original(**kwargs)

    conn.s3.get_object = spy
    settled = settler(conn, FakeTime()).run()
    assert read == ["raw/2026/10/10/02/b.ndjson"]
    assert settled["envelopes"] == 8


def test_a_torn_spool_line_is_dropped_on_resume(conn):
    spool = settler(conn, FakeTime()).spool
    spool.parent.mkdir(parents=True)
    spool.write_text(json.dumps(APP[0]) + "\n" + '{"id": "torn')
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP + DERIVED)
    assert settler(conn, FakeTime()).run()["envelopes"] == 8


def test_archive_dlq_messages_fail_fast(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP + DERIVED)
    conn.sqs.depth[conn.outputs.archive_dlq_url] = 2
    with pytest.raises(GeneratorError, match="archive DLQ holds 2 messages.*[Rr]edrive"):
        settler(conn, FakeTime()).run()


def test_recompaction_removes_stale_parts(conn):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP + DERIVED)
    stale = f"compacted/local/{RUN}/part-0005.ndjson"
    conn.s3.put_object(Bucket=conn.outputs.bucket, Key=stale, Body=b"{}\n")
    settled = settler(conn, FakeTime()).run()
    assert settled["parts"] == [f"compacted/local/{RUN}/part-0000.ndjson"]
    assert list(conn.keys(f"compacted/local/{RUN}/")) == settled["parts"]


def seed_poison(tmp_path, k: int) -> None:
    directory = tmp_path / "state" / "local" / "poison"
    directory.mkdir(parents=True)
    (directory / "p1.json").write_text(json.dumps({"k": k}))


def test_visit_dlq_beyond_poison_warns_and_is_named_in_the_timeout(conn, tmp_path):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP)  # derived events never arrive
    seed_poison(tmp_path, 1)
    conn.sqs.depth[conn.outputs.visit_dlq_url] = 3
    lines = []
    fake = FakeTime()
    with pytest.raises(SettleTimeout, match="visit DLQ holds 3 messages but only 1 poison"):
        settler(conn, fake, timeout_seconds=10, log=lines.append).run()
    assert any("valid events may have dead-lettered" in line for line in lines)


def test_visit_dlq_matching_poison_is_silent(conn, tmp_path):
    put_raw(conn, "raw/2026/10/10/01/a.ndjson", APP)
    seed_poison(tmp_path, 2)
    conn.sqs.depth[conn.outputs.visit_dlq_url] = 2
    lines = []
    with pytest.raises(SettleTimeout) as caught:
        settler(conn, FakeTime(), timeout_seconds=10, log=lines.append).run()
    assert "dead-lettered" not in str(caught.value)
    assert not any("dead-lettered" in line for line in lines)
