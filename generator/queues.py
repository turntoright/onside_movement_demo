"""SQS queue depth from attributes only; messages are never received (spec §6.4)."""

from __future__ import annotations

import time
from collections.abc import Callable

from .state import GeneratorError

DEPTH_ATTRIBUTES = (
    "ApproximateNumberOfMessages",
    "ApproximateNumberOfMessagesNotVisible",
    "ApproximateNumberOfMessagesDelayed",
)


def counts(sqs, queue_url: str) -> dict[str, int]:
    response = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=list(DEPTH_ATTRIBUTES))
    attributes = response.get("Attributes", {})
    return {name: int(attributes.get(name, 0)) for name in DEPTH_ATTRIBUTES}


def visible(sqs, queue_url: str) -> int:
    return counts(sqs, queue_url)["ApproximateNumberOfMessages"]


def is_idle(sqs, queue_url: str) -> bool:
    """No visible, in-flight or delayed messages."""
    return not any(counts(sqs, queue_url).values())


def wait_for_dlq(
    sqs,
    dlq_url: str,
    expected: int,
    *,
    timeout: float,
    poll: float = 5.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    log: Callable[[str], None] = print,
) -> int:
    """Wait until the DLQ shows at least `expected` messages (a redelivery may add more)."""
    deadline = clock() + timeout
    while True:
        found = visible(sqs, dlq_url)
        if found >= expected:
            log(f"visit DLQ holds {found} messages (expected at least {expected})")
            return found
        if clock() >= deadline:
            raise GeneratorError(
                f"visit DLQ holds {found} messages, expected at least {expected} "
                f"after {timeout:.0f}s (a poison message takes about 3 minutes on LocalStack)"
            )
        sleep(poll)
