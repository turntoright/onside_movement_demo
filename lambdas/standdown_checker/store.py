"""State store interface used by the processor (spec §6.1)."""

from __future__ import annotations

from typing import Protocol

from .events import ValidationError


class ConflictError(Exception):
    """person_state changed since it was read; reload and retry."""


class EvaluationExistsError(Exception):
    """Another worker already committed the evaluation for this check-in."""


class UnknownPropertyError(ValidationError):
    """The check-in names a property that is not in the reference snapshot."""


class StateStore(Protocol):
    def load_person(self, person_id: str) -> tuple[int, list[dict]]: ...

    def get_evaluation(self, checkin_event_id: str) -> dict | None: ...

    def get_property(self, property_id: str) -> dict: ...

    def commit(
        self,
        person_id: str,
        version_read: int,
        visits: list[dict],
        evaluation: dict | None,
    ) -> None: ...
