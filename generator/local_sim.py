"""In-memory StateStore and the ENV=sim runner (spec §3.3 Local output)."""

from __future__ import annotations

import copy
import json
from pathlib import Path

from standdown_checker.store import ConflictError, EvaluationExistsError, UnknownPropertyError


class InMemoryStore:
    """Implements the same conditions as the DynamoDB transaction in spec §6.1."""

    def __init__(self, properties: dict[str, dict]) -> None:
        self.properties = properties
        self.persons: dict[str, dict] = {}
        self.evaluations: dict[str, dict] = {}

    def load_person(self, person_id: str) -> tuple[int, list[dict]]:
        item = self.persons.get(person_id)
        if item is None:
            return 0, []
        return item["version"], copy.deepcopy(item["visits"])

    def get_evaluation(self, checkin_event_id: str) -> dict | None:
        found = self.evaluations.get(checkin_event_id)
        return copy.deepcopy(found) if found is not None else None

    def get_property(self, property_id: str) -> dict:
        try:
            return self.properties[property_id]
        except KeyError:
            raise UnknownPropertyError(f"unknown property {property_id!r}") from None

    def commit(
        self,
        person_id: str,
        version_read: int,
        visits: list[dict],
        evaluation: dict | None,
    ) -> None:
        if evaluation is not None and evaluation["checkin_event_id"] in self.evaluations:
            raise EvaluationExistsError(evaluation["checkin_event_id"])
        current = self.persons.get(person_id, {"version": 0})["version"]
        if current != version_read:
            raise ConflictError(f"{person_id}: read {version_read}, now {current}")
        self.persons[person_id] = {"version": version_read + 1, "visits": copy.deepcopy(visits)}
        if evaluation is not None:
            self.evaluations[evaluation["checkin_event_id"]] = copy.deepcopy(evaluation)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"persons": self.persons, "evaluations": self.evaluations}))
        tmp.replace(path)

    @classmethod
    def load(cls, path: Path, properties: dict[str, dict]) -> InMemoryStore:
        store = cls(properties)
        if path.exists():
            data = json.loads(path.read_text())
            store.persons = data["persons"]
            store.evaluations = data["evaluations"]
        return store
