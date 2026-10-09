import json

import pytest
from standdown_checker import handler as H
from standdown_checker.dynamo_store import DynamoDbStore
from standdown_checker.store import ConflictError

from tests.aws_fixtures import EVALUATION_TABLE, PERSON_TABLE, PROPERTY_TABLE
from tests.helpers import PROPERTIES, ROOT, RULES, TickingClock, checkin, checkout


class FakeEvents:
    """Records PutEvents calls; rejects entries of the given detail types."""

    def __init__(self, reject: tuple[str, ...] = (), error: Exception | None = None) -> None:
        self.reject, self.error, self.calls = reject, error, []

    def put_events(self, Entries):
        self.calls.append(Entries)
        if self.error is not None:
            raise self.error
        results = [
            {"ErrorCode": "InternalFailure", "ErrorMessage": "rejected"}
            if entry["DetailType"] in self.reject
            else {"EventId": f"e{i}"}
            for i, entry in enumerate(Entries)
        ]
        return {"FailedEntryCount": sum("ErrorCode" in r for r in results), "Entries": results}

    def details(self, detail_type: str) -> list[str]:
        return [e["Detail"] for call in self.calls for e in call if e["DetailType"] == detail_type]


A_IN = checkin("A", "P1", "2026-03-03T09:00:00+13:00")


def record(message_id: str, detail_type: str, detail: dict) -> dict:
    body = {
        "version": "0",
        "id": f"env-{message_id}",
        "source": "onside.app",
        "detail-type": detail_type,
        "detail": detail,
    }
    return {"messageId": message_id, "body": json.dumps(body)}


def sqs_event(*records: dict) -> dict:
    return {"Records": list(records)}


@pytest.fixture
def store(aws):
    aws.put_properties(PROPERTIES)
    return DynamoDbStore(aws.dynamodb, PERSON_TABLE, EVALUATION_TABLE, PROPERTY_TABLE)


def call(event, store, events, **kwargs):
    return H.handle(
        event,
        store=store,
        rules=RULES,
        events_client=events,
        bus_name="onside-events",
        now=TickingClock(),
        **kwargs,
    )


def failures(result) -> list[str]:
    return [f["itemIdentifier"] for f in result["batchItemFailures"]]


def test_checkin_is_committed_and_its_evaluation_published(store):
    events = FakeEvents()
    result = call(sqs_event(record("m1", "CheckIn", A_IN)), store, events)
    assert failures(result) == []
    (entry,) = events.calls[0]
    assert entry["Source"] == "onside.standdown"
    assert entry["DetailType"] == "CheckInEvaluated"
    assert entry["EventBusName"] == "onside-events"
    stored = store.get_evaluation("A")
    assert entry["Detail"] == json.dumps(stored, sort_keys=True, separators=(",", ":"))
    assert store.load_person("U1")[0] == 1


def test_invalid_records_fail_alone(store):
    events = FakeEvents()
    batch = sqs_event(
        record("good", "CheckIn", A_IN),
        record("poison", "CheckIn", {"poison": True, "event_id": "x"}),
        {"messageId": "not-json", "body": "{"},
        record("unknown-property", "CheckIn", checkin("B", "NOPE", "2026-03-03T10:00:00+13:00")),
    )
    result = call(batch, store, events)
    assert failures(result) == ["poison", "not-json", "unknown-property"]
    assert store.get_evaluation("A") is not None
    assert events.details("CheckInEvaluated") == [
        json.dumps(store.get_evaluation("A"), sort_keys=True, separators=(",", ":"))
    ]
    assert [e["DetailType"] for call in events.calls for e in call] == ["CheckInEvaluated"]


def test_rejected_entry_fails_only_its_record_and_redelivery_reemits_identically(store):
    breach = sqs_event(
        record("m1", "CheckIn", A_IN),
        record("m2", "CheckIn", checkin("C", "P3", "2026-03-04T14:00:00+13:00")),
    )
    events = FakeEvents(reject=("HighRiskVisitDetected",))
    assert failures(call(breach, store, events)) == ["m2"]  # m2's alert was rejected
    retry = FakeEvents()
    assert failures(call(sqs_event(breach["Records"][1]), store, retry)) == []
    assert retry.details("CheckInEvaluated") == events.details("CheckInEvaluated")[1:]
    assert retry.details("HighRiskVisitDetected") == events.details("HighRiskVisitDetected")


def test_put_events_exception_fails_every_record_in_that_call(store):
    events = FakeEvents(error=RuntimeError("bus unavailable"))
    batch = sqs_event(
        record("m1", "CheckIn", A_IN),
        record("m2", "CheckOut", checkout("A-out", "A", "P1", "2026-03-03T10:00:00+13:00")),
    )
    result = call(batch, store, events)
    assert failures(result) == ["m1", "m2"]
    assert store.load_person("U1")[0] == 2  # committed; a redelivery re-emits


def test_outbound_entries_are_sent_in_calls_of_at_most_ten(store):
    records = [
        record(f"m{i}", "CheckIn", checkin(f"A{i}", "P1", A_IN["event_time"], person=f"U{i}"))
        for i in range(12)
    ]
    events = FakeEvents()
    assert failures(call(sqs_event(*records), store, events)) == []
    assert [len(c) for c in events.calls] == [10, 2]


def test_rejected_entry_in_second_chunk_fails_only_that_record(store):
    records = [
        record(f"m{i}", "CheckIn", checkin(f"A{i}", "P1", A_IN["event_time"], person=f"U{i}"))
        for i in range(12)
    ]

    class RejectNth(FakeEvents):
        def put_events(self, Entries):
            response = super().put_events(Entries)
            if len(self.calls) == 2:
                response["Entries"][1] = {"ErrorCode": "InternalFailure"}
                response["FailedEntryCount"] = 1
            return response

    events = RejectNth()
    assert failures(call(sqs_event(*records), store, events)) == ["m11"]


def test_exhausted_retries_fail_the_record(store, monkeypatch):
    real = H.process_event
    monkeypatch.setattr(H, "process_event", lambda *a: real(*a, sleep=lambda seconds: None))

    def always_conflict(*args):
        raise ConflictError("busy")

    monkeypatch.setattr(store, "commit", always_conflict)
    result = call(sqs_event(record("m1", "CheckIn", A_IN)), store, FakeEvents())
    assert failures(result) == ["m1"]


def test_lambda_entry_point_uses_environment(aws, monkeypatch):
    aws.put_properties(PROPERTIES)
    monkeypatch.setenv("PERSON_TABLE", PERSON_TABLE)
    monkeypatch.setenv("EVALUATION_TABLE", EVALUATION_TABLE)
    monkeypatch.setenv("PROPERTY_TABLE", PROPERTY_TABLE)
    monkeypatch.setenv("EVENT_BUS_NAME", aws.bus_name)
    monkeypatch.setenv("RULES_PATH", str(ROOT / "shared" / "standdown_rules.csv"))
    monkeypatch.setattr(H, "_DEPENDENCIES", None)
    batch = sqs_event(
        record("m1", "CheckIn", A_IN),
        record("m2", "CheckIn", {"poison": True, "event_id": "x"}),
    )
    result = H.handler(batch, None)
    assert failures(result) == ["m2"]
    item = aws.dynamodb.get_item(TableName=EVALUATION_TABLE, Key={"checkin_event_id": {"S": "A"}})
    assert "Item" in item
