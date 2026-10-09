import json
from datetime import UTC, datetime

import pytest

from generator.emit import MAX_ATTEMPTS, emit, put_batch, to_entry
from generator.state import GeneratorError
from generator.stream import Emission

T = datetime(2026, 3, 1, tzinfo=UTC)


def emissions(n: int) -> list[Emission]:
    return [Emission(T, "CheckIn", {"event_id": f"E{i:02d}", "n": i}) for i in range(n)]


class FlakyEvents:
    """Rejects each listed event id for the given number of attempts."""

    def __init__(self, reject: dict[str, int] | None = None) -> None:
        self.reject = dict(reject or {})
        self.calls: list[list[str]] = []

    def put_events(self, Entries):
        ids = [json.loads(e["Detail"])["event_id"] for e in Entries]
        self.calls.append(ids)
        results = []
        for event_id in ids:
            if self.reject.get(event_id, 0) > 0:
                self.reject[event_id] -= 1
                results.append({"ErrorCode": "ThrottlingException", "ErrorMessage": "slow down"})
            else:
                results.append({"EventId": event_id})
        failed = sum("ErrorCode" in r for r in results)
        return {"FailedEntryCount": failed, "Entries": results}


def test_entry_carries_source_type_bus_and_compact_detail():
    entry = to_entry(Emission(T, "CheckOut", {"event_id": "E1", "a": [1]}), "bus")
    assert entry == {
        "Source": "onside.app",
        "DetailType": "CheckOut",
        "Detail": '{"event_id":"E1","a":[1]}',
        "EventBusName": "bus",
    }


def test_batches_of_ten_in_send_order():
    client = FlakyEvents()
    assert emit(emissions(25), client, "bus", log=lambda line: None) == 25
    assert [len(c) for c in client.calls] == [10, 10, 5]
    assert [i for c in client.calls for i in c] == [f"E{i:02d}" for i in range(25)]


def test_only_rejected_entries_are_resent():
    client, sleeps = FlakyEvents({"E03": 1, "E07": 2}), []
    emit(emissions(10), client, "bus", sleep=sleeps.append, log=lambda line: None)
    assert client.calls == [[f"E{i:02d}" for i in range(10)], ["E03", "E07"], ["E07"]]
    assert sleeps == [0.2, 0.4]


def test_entries_rejected_on_every_attempt_raise():
    client = FlakyEvents({"E01": 99})
    with pytest.raises(GeneratorError, match="ThrottlingException"):
        put_batch(client, [to_entry(e, "bus") for e in emissions(3)], sleep=lambda s: None)
    assert len(client.calls) == MAX_ATTEMPTS


def test_emits_to_an_event_bus(aws):
    lines = []
    assert emit(emissions(12), aws.events, aws.bus_name, log=lines.append) == 12
    assert lines[-1].startswith("emitted 12/12 events")
