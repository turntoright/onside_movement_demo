"""StateStore on DynamoDB (spec §6.1): consistent reads and TransactWriteItems commits."""

from __future__ import annotations

import json

from .store import ConflictError, EvaluationExistsError, UnknownPropertyError


def _dumps(value: object) -> str:
    return json.dumps(value, separators=(",", ":"))


class DynamoDbStore:
    """Implements the StateStore contract in store.py with the low-level client API.

    `visits` and evaluation bodies are JSON strings (attribute type S), so reads return plain
    JSON types. Properties are cached for the life of the object (one per Lambda container).
    """

    def __init__(
        self, client, person_table: str, evaluation_table: str, property_table: str
    ) -> None:
        self.client = client
        self.person_table = person_table
        self.evaluation_table = evaluation_table
        self.property_table = property_table
        self._properties: dict[str, dict] = {}

    def _get(self, table: str, key: str, value: str) -> dict | None:
        response = self.client.get_item(
            TableName=table, Key={key: {"S": value}}, ConsistentRead=True
        )
        return response.get("Item")

    def load_person(self, person_id: str) -> tuple[int, list[dict]]:
        item = self._get(self.person_table, "person_id", person_id)
        if item is None:
            return 0, []
        return int(item["version"]["N"]), json.loads(item["visits"]["S"])

    def get_evaluation(self, checkin_event_id: str) -> dict | None:
        item = self._get(self.evaluation_table, "checkin_event_id", checkin_event_id)
        return None if item is None else json.loads(item["body"]["S"])

    def get_property(self, property_id: str) -> dict:
        cached = self._properties.get(property_id)
        if cached is None:
            item = self._get(self.property_table, "property_id", property_id)
            if item is None:
                raise UnknownPropertyError(f"unknown property {property_id!r}")
            cached = {
                "property_type": item["property_type"]["S"],
                "owner_company_id": item["owner_company_id"]["S"],
            }
            self._properties[property_id] = cached
        return dict(cached)

    def _person_update(self, person_id: str, version_read: int, visits: list[dict]) -> dict:
        values = {":new": {"N": str(version_read + 1)}, ":visits": {"S": _dumps(visits)}}
        if version_read == 0:
            condition = "attribute_not_exists(person_id)"
        else:
            condition = "version = :read"
            values[":read"] = {"N": str(version_read)}  # only when used: unused values fail
        return {
            "TableName": self.person_table,
            "Key": {"person_id": {"S": person_id}},
            "UpdateExpression": "SET version = :new, visits = :visits",
            "ConditionExpression": condition,
            "ExpressionAttributeValues": values,
        }

    def commit(
        self,
        person_id: str,
        version_read: int,
        visits: list[dict],
        evaluation: dict | None,
    ) -> None:
        update = self._person_update(person_id, version_read, visits)
        if evaluation is None:
            try:
                self.client.update_item(**update)
            except (
                self.client.exceptions.ConditionalCheckFailedException,
                self.client.exceptions.TransactionConflictException,
            ) as exc:
                # Stale version, or a concurrent transaction on this item: reload and retry.
                code = exc.response["Error"]["Code"]
                raise ConflictError(
                    f"{person_id}: version {version_read} not committed ({code})"
                ) from None
            return
        put = {
            "TableName": self.evaluation_table,
            "Item": {
                "checkin_event_id": {"S": evaluation["checkin_event_id"]},
                "body": {"S": _dumps(evaluation)},
            },
            "ConditionExpression": "attribute_not_exists(checkin_event_id)",
        }
        try:
            self.client.transact_write_items(TransactItems=[{"Update": update}, {"Put": put}])
        except self.client.exceptions.TransactionCanceledException as exc:
            reasons = exc.response.get("CancellationReasons") or []
            codes = [reason.get("Code") for reason in reasons]
            if len(codes) > 1 and codes[1] == "ConditionalCheckFailed":
                raise EvaluationExistsError(evaluation["checkin_event_id"]) from None
            # A stale version or any other cancellation: reload and retry (spec §6.1).
            raise ConflictError(f"{person_id}: transaction cancelled {codes}") from None
