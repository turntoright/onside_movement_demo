"""The full chain on a fresh LocalStack stack (spec §7.3). Run with `make integration`."""

import json
import os
import time
import uuid
from datetime import UTC, datetime

import pytest
from standdown_checker.dynamo_store import DynamoDbStore

from generator import aws as A
from generator import queues as Q
from generator.emit import emit
from generator.settle import Settler
from generator.stream import Emission, event_id
from tests.helpers import checkin, checkout

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("LOCALSTACK") != "1",
        reason="needs LOCALSTACK=1 and a freshly deployed local stack (make integration)",
    ),
]

PROPERTIES = {  # ids that the generator never uses, so they cannot clash with demo data
    "IT-P1": ("poultry", "IT-C2"),
    "IT-P2": ("office", "IT-C3"),
    "IT-P3": ("poultry", "IT-C1"),
}
ORPHAN = "00000000-0000-4000-8000-0000000000aa"  # never sent


def app(detail_type: str, detail: dict) -> Emission:
    return Emission(datetime.now(UTC), detail_type, detail)


def wait_until(condition, timeout: float = 180, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(2)


@pytest.fixture
def chain(tmp_path, monkeypatch):
    conn = A.connect("local")  # reads state/local/outputs.json written by make deploy
    if conn.keys("control/") or conn.keys("ref/"):
        pytest.fail("the stack is not fresh; run `make integration` (it starts with make up)")
    monkeypatch.setenv("REPO_ROOT", str(tmp_path))  # the settle spool goes to a temp dir
    for property_id, (ptype, owner) in PROPERTIES.items():
        conn.dynamodb.put_item(
            TableName=conn.outputs.property_table,
            Item={
                "property_id": {"S": property_id},
                "property_type": {"S": ptype},
                "owner_company_id": {"S": owner},
            },
        )
    store = DynamoDbStore(
        conn.dynamodb,
        conn.outputs.person_table,
        conn.outputs.evaluation_table,
        conn.outputs.property_table,
    )
    return conn, store


def test_events_flow_through_the_chain_and_settle(chain):
    conn, store = chain
    run_id = str(uuid.uuid4())
    a, co_a, b, c, d, e, orphan, plant = (event_id(run_id, seq) for seq in range(8))
    started_at = datetime.now(UTC).replace(microsecond=0)
    u1, u2, u3 = {"person": "IT-U1"}, {"person": "IT-U2"}, {"person": "IT-U3"}
    plant_movement = {
        "event_id": plant,
        "source_property_id": "IT-P1",
        "dest_property_id": "IT-P3",
        "movement_date": "2026-03-05",
        "material_type": "budwood",
        "volume": 10,
        "received_time": "2026-03-06T01:00:00+13:00",
    }

    # Phase 1: U1's first two visits; U2's LATER visit E; an orphan check-out; a plant
    # movement; one poison event.
    phase_1 = [
        app("CheckIn", checkin(a, "IT-P1", "2026-03-03T09:00:00+13:00", **u1)),
        app("CheckOut", checkout(co_a, a, "IT-P1", "2026-03-03T11:00:00+13:00", **u1)),
        app("CheckIn", checkin(b, "IT-P2", "2026-03-04T08:00:00+13:00", **u1)),
        app("CheckIn", checkin(e, "IT-P3", "2026-03-05T16:00:00+13:00", **u2)),
        app("CheckOut", checkout(orphan, ORPHAN, "IT-P2", "2026-03-05T12:00:00+13:00", **u3)),
        app("PlantMovement", plant_movement),
        app("CheckIn", {"poison": True, "event_id": str(uuid.uuid4())}),
    ]
    emit(phase_1, conn.events, conn.outputs.bus_name)

    def phase_1_done() -> bool:
        return (
            store.load_person("IT-U1")[0] == 3
            and store.load_person("IT-U3")[0] == 1
            and store.get_evaluation(e) is not None
        )

    wait_until(phase_1_done, what="phase 1 to be processed")

    # Phase 2: the breach C (sent twice: a duplicate) and U2's EARLIER visit D, out of order.
    c_detail = checkin(c, "IT-P3", "2026-03-04T14:00:00+13:00", **u1)
    emit(
        [
            app("CheckIn", c_detail),
            app("CheckIn", c_detail),
            app("CheckIn", checkin(d, "IT-P1", "2026-03-05T09:00:00+13:00", **u2)),
        ],
        conn.events,
        conn.outputs.bus_name,
    )

    manifest = {
        "run_id": run_id,
        "mode": "live",
        "counts": {"CheckIn": 5, "CheckOut": 2, "PlantMovement": 1},
        "total": 8,
        "started_at": started_at.isoformat(),
    }
    settled = Settler(manifest, conn, quiet_seconds=10, timeout_seconds=600).run()

    # person_state: every distinct event committed exactly once, whatever the order.
    version, visits = store.load_person("IT-U1")
    assert version == 4
    assert [v["checkin_event_id"] for v in visits] == [a, b, c]
    assert visits[0]["checkout_event_id"] == co_a
    version, visits = store.load_person("IT-U2")
    assert version == 2
    assert [v["checkin_event_id"] for v in visits] == [d, e]  # the late D sorted first
    assert store.get_evaluation(e)["version_read"] == 0
    assert store.get_evaluation(d)["version_read"] == 1
    version, visits = store.load_person("IT-U3")
    assert version == 1
    assert visits[0]["checkin_time"] is None and visits[0]["checkout_event_id"] == orphan

    # evaluations: the worked example breaches (poultry -> office -> poultry, 1 night of 3).
    evaluation = store.get_evaluation(c)
    assert evaluation["breach"] is True
    assert (evaluation["rule_id"], evaluation["origin_checkin_event_id"]) == ("R02", a)

    # compacted objects: complete, poison-free, and the duplicate re-emitted identically.
    lines = []
    for key in settled["parts"]:
        body = conn.s3.get_object(Bucket=conn.outputs.bucket, Key=key)["Body"].read().decode()
        lines += [json.loads(line) for line in body.splitlines()]
    assert not any("poison" in env["detail"] for env in lines)
    ids = {env["detail"].get("event_id") for env in lines if env["source"] == "onside.app"}
    assert {a, co_a, b, c, d, e, orphan, plant} <= ids
    c_evaluations = [
        env["detail"]
        for env in lines
        if env["detail-type"] == "CheckInEvaluated" and env["detail"]["checkin_event_id"] == c
    ]
    assert len(c_evaluations) == 2 and c_evaluations[0] == c_evaluations[1]
    alerts = [env for env in lines if env["detail-type"] == "HighRiskVisitDetected"]
    assert {env["detail"]["checkin_event_id"] for env in alerts} == {c}
    assert conn.keys(f"control/settled/{run_id}.json")

    # The archiver batches: fewer raw objects than archived envelopes.
    raw = conn.keys("raw/")
    assert 0 < len(raw) < len(lines)

    # DLQs: the poison event reaches the visit DLQ; the archive DLQ stays empty.
    assert Q.wait_for_dlq(conn.sqs, conn.outputs.visit_dlq_url, 1, timeout=300) >= 1
    assert Q.visible(conn.sqs, conn.outputs.archive_dlq_url) == 0
