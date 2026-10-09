"""PutEvents in send order, in batches of 10, retrying only the failed entries (spec §3.2)."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Sequence

from .state import GeneratorError
from .stream import Emission

MAX_ENTRIES = 10
MAX_ATTEMPTS = 6


def to_entry(emission: Emission, bus_name: str) -> dict:
    return {
        "Source": emission.source,
        "DetailType": emission.detail_type,
        "Detail": json.dumps(emission.detail, separators=(",", ":")),
        "EventBusName": bus_name,
    }


def put_batch(
    events_client,
    entries: list[dict],
    *,
    sleep: Callable[[float], None] = time.sleep,
    attempts: int = MAX_ATTEMPTS,
) -> None:
    """Send up to 10 entries; resend only the rejected ones, a bounded number of times."""
    remaining = entries
    errors: list[str] = []
    for attempt in range(attempts):
        response = events_client.put_events(Entries=remaining)
        if not response.get("FailedEntryCount"):
            return
        results = response["Entries"]
        errors = sorted({r["ErrorCode"] for r in results if r.get("ErrorCode")})
        remaining = [e for e, r in zip(remaining, results, strict=True) if r.get("ErrorCode")]
        if attempt + 1 < attempts:
            sleep(min(5.0, 0.2 * 2**attempt))
    raise GeneratorError(
        f"PutEvents still rejected {len(remaining)} entries after {attempts} attempts: {errors}"
    )


def emit(
    emissions: Sequence[Emission],
    events_client,
    bus_name: str,
    *,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = print,
    every: int = 2000,
) -> int:
    """Send every emission in order. Returns how many entries were sent."""
    entries = [to_entry(em, bus_name) for em in emissions]
    started = time.monotonic()
    for start in range(0, len(entries), MAX_ENTRIES):
        put_batch(events_client, entries[start : start + MAX_ENTRIES], sleep=sleep)
        done = min(start + MAX_ENTRIES, len(entries))
        if done % every < MAX_ENTRIES or done == len(entries):
            rate = done / max(time.monotonic() - started, 1e-6)
            log(f"emitted {done}/{len(entries)} events ({rate:.0f} events/s)")
    return len(entries)
