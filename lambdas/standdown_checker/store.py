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
    """Contract every store implementation must satisfy (spec §6.1).

    - Loaded values are plain JSON types: ``int`` (never Decimal), ``str``, dict, list.
    - ``load_person`` returns ``(0, [])`` for a person that has never been committed.
    - ``commit`` succeeds only if the person item is absent when ``version_read == 0``,
      otherwise only if the stored version equals ``version_read``; it then stores
      ``version_read + 1``. A failed version condition raises ``ConflictError``.
    - If ``evaluation`` is given and one already exists for its ``checkin_event_id``, the
      commit raises ``EvaluationExistsError``. When both the version and the evaluation
      conditions fail, ``EvaluationExistsError`` wins.
    - ``commit`` is all-or-nothing: on any error neither the person nor the evaluation changes.
    - Reads are strongly consistent (a read after a successful commit sees it).
    - Visits and evaluations returned are copies; mutating them never changes the store.
    """

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
