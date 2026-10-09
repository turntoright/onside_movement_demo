import pytest
from botocore.stub import Stubber
from standdown_checker.dynamo_store import DynamoDbStore
from standdown_checker.store import ConflictError

from tests.helpers import PROPERTIES, checkin, checkout, run
from tests.store_contract import *  # noqa: F403  (contract suite, collected for DynamoDbStore)
from tests.store_contract import PROPS, VISIT


def make_store(stack) -> DynamoDbStore:
    return DynamoDbStore(stack.dynamodb, "person_state", "evaluations", "properties")


@pytest.fixture
def store_factory(aws):
    def factory(properties):
        aws.put_properties(properties)
        return make_store(aws)

    return factory


def test_first_commit_sends_no_unused_read_value(aws):
    store = make_store(aws)
    sent = []
    original = store.client.transact_write_items

    def spy(**kwargs):
        sent.append(kwargs)
        return original(**kwargs)

    store.client.transact_write_items = spy
    store.commit("U1", 0, [VISIT], {"checkin_event_id": "A"})
    store.commit("U1", 1, [VISIT], {"checkin_event_id": "B"})
    first, second = (s["TransactItems"][0]["Update"] for s in sent)
    assert first["ConditionExpression"] == "attribute_not_exists(person_id)"
    assert ":read" not in first["ExpressionAttributeValues"]
    assert second["ConditionExpression"] == "version = :read"
    assert second["ExpressionAttributeValues"][":read"] == {"N": "1"}


def test_items_hold_json_strings(aws):
    make_store(aws).commit("U1", 0, [VISIT], {"checkin_event_id": "A", "nights": 2})
    person = aws.dynamodb.get_item(TableName="person_state", Key={"person_id": {"S": "U1"}})
    evaluation = aws.dynamodb.get_item(
        TableName="evaluations", Key={"checkin_event_id": {"S": "A"}}
    )
    assert person["Item"]["version"] == {"N": "1"}
    assert person["Item"]["visits"]["S"].startswith("[{")
    assert evaluation["Item"]["body"]["S"] == '{"checkin_event_id":"A","nights":2}'


def test_property_is_cached_per_store(aws):
    aws.put_properties(PROPS)
    store = make_store(aws)
    assert store.get_property("P1") == PROPS["P1"]
    aws.dynamodb.delete_item(TableName="properties", Key={"property_id": {"S": "P1"}})
    assert store.get_property("P1") == PROPS["P1"]  # served from the container cache


def test_other_cancellation_reasons_are_conflicts(aws):
    store = make_store(aws)
    with Stubber(store.client) as stub:
        stub.add_client_error(
            "transact_write_items",
            service_error_code="TransactionCanceledException",
            service_message="Transaction cancelled [TransactionConflict, None]",
            http_status_code=400,
            modeled_fields={
                "CancellationReasons": [{"Code": "TransactionConflict"}, {"Code": "None"}]
            },
        )
        with pytest.raises(ConflictError):
            store.commit("U1", 0, [VISIT], {"checkin_event_id": "A"})


def test_transaction_conflict_without_evaluation_is_a_conflict(aws):
    store = make_store(aws)

    def conflicting_update(**kwargs):
        raise store.client.exceptions.TransactionConflictException(
            {"Error": {"Code": "TransactionConflictException", "Message": "in flight"}},
            "UpdateItem",
        )

    store.client.update_item = conflicting_update
    with pytest.raises(ConflictError):
        store.commit("U1", 0, [VISIT], None)


def test_processor_retries_after_a_conflicting_commit(aws):
    aws.put_properties(PROPERTIES)
    store, racer = make_store(aws), make_store(aws)
    run(store, "CheckIn", checkin("A", "P1", "2026-03-01T10:00:00+13:00"))
    original, raced = store.commit, []

    def commit_after_a_race(*args):
        if not raced:  # another container commits between our read and our write
            raced.append(True)
            run(racer, "CheckOut", checkout("X", "A", "P1", "2026-03-01T11:00:00+13:00"))
        return original(*args)

    store.commit = commit_after_a_race
    out = run(store, "CheckIn", checkin("B", "P3", "2026-03-02T10:00:00+13:00"))
    evaluation = out[0].detail
    assert (evaluation["version_read"], evaluation["version_committed"]) == (2, 3)
    assert [o["end_source"] for o in evaluation["origins_seen"]] == ["checkout"]
    assert store.load_person("U1")[0] == 3
