"""Settle and compact a run (spec §3.4).

Scan S3 raw/ incrementally, spool every envelope to state/<env>/spool/, count this run's
events, and once every gate holds (and a quiet period confirms it) write the spool to
compacted/<env>/<run_id>/ and control/settled/<run_id>.json.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from . import queues as Q
from .aws import Connection
from .config import state_dir
from .state import GeneratorError, write_json
from .stream import event_id

QUIET_SECONDS = 30
POLL_SECONDS = 5
TIMEOUT_SECONDS = 3600
LOG_EVERY_SECONDS = 30
PART_LINES = 50_000
SKEW = timedelta(hours=1)
APP_TYPES = ("CheckIn", "CheckOut", "PlantMovement")


class SettleTimeout(GeneratorError):
    """The run did not settle in time; the spool is kept for `make settle`."""


def iter_json(text: str) -> Iterator[dict]:
    """JSON documents from NDJSON or from documents concatenated without separators."""
    decoder = json.JSONDecoder()
    i, n = 0, len(text)
    while True:
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            return
        document, i = decoder.raw_decode(text, i)
        yield document


def hour_prefixes(start: datetime, now: datetime) -> list[str]:
    """Arrival-hour prefixes from one hour before the run's start up to now (UTC)."""
    hour = (start - SKEW).astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    prefixes = []
    while hour <= now:
        prefixes.append(f"raw/{hour:%Y/%m/%d/%H}/")
        hour += timedelta(hours=1)
    return prefixes


class Tally:
    """Counts only this run's event ids: uuid5(run_id, seq) for seq below the manifest total."""

    def __init__(self, manifest: dict) -> None:
        self.counts = {t: manifest["counts"].get(t, 0) for t in APP_TYPES}
        self.ids = {event_id(manifest["run_id"], seq) for seq in range(manifest["total"])}
        self.seen: dict[str, set[str]] = {t: set() for t in APP_TYPES}
        self.evaluated: set[str] = set()
        self.breaches: set[str] = set()
        self.applied: set[str] = set()
        self.alerted: set[str] = set()

    def add(self, envelope: dict) -> None:
        detail = envelope.get("detail")
        if not isinstance(detail, dict):
            return
        kind = envelope.get("detail-type")
        if kind in self.seen:
            if detail.get("event_id") in self.ids:
                self.seen[kind].add(detail["event_id"])
        elif kind == "CheckInEvaluated":
            if detail.get("checkin_event_id") in self.ids:
                self.evaluated.add(detail["checkin_event_id"])
                if detail.get("breach") is True:
                    self.breaches.add(detail["checkin_event_id"])
        elif kind == "CheckOutApplied":
            if detail.get("checkout_event_id") in self.ids:
                self.applied.add(detail["checkout_event_id"])
        elif kind == "HighRiskVisitDetected":
            if detail.get("checkin_event_id") in self.ids:
                self.alerted.add(detail["checkin_event_id"])

    def missing(self) -> list[str]:
        """The gates that do not hold yet; empty when the run is complete."""
        gaps = [
            f"{t} {len(self.seen[t])}/{self.counts[t]}"
            for t in APP_TYPES
            if len(self.seen[t]) != self.counts[t]
        ]
        if len(self.evaluated) != self.counts["CheckIn"]:
            gaps.append(f"CheckInEvaluated {len(self.evaluated)}/{self.counts['CheckIn']}")
        if len(self.applied) != self.counts["CheckOut"]:
            gaps.append(f"CheckOutApplied {len(self.applied)}/{self.counts['CheckOut']}")
        if self.alerted != self.breaches:
            gaps.append(f"HighRiskVisitDetected {len(self.alerted)}/{len(self.breaches)}")
        return gaps


@dataclass
class Settler:
    manifest: dict
    conn: Connection
    quiet_seconds: float = QUIET_SECONDS
    poll_seconds: float = POLL_SECONDS
    timeout_seconds: float = TIMEOUT_SECONDS
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    log: Callable[[str], None] = print

    @property
    def run_id(self) -> str:
        return self.manifest["run_id"]

    @property
    def env(self) -> str:
        return self.conn.env

    @property
    def spool(self) -> Path:
        return state_dir(self.env) / "spool" / f"{self.run_id}.ndjson"

    @property
    def key_cache(self) -> Path:
        return state_dir(self.env) / "spool" / f"{self.run_id}.keys"

    def run(self) -> dict:
        tally, keys = Tally(self.manifest), self._resume()
        for line in self._spool_lines():
            tally.add(json.loads(line))
        start = datetime.fromisoformat(self.manifest["started_at"])
        deadline = self.clock() + self.timeout_seconds
        next_log = self.clock()
        warned_depth = 0
        while True:
            self._check_archive_dlq()
            self._scan(start, keys, tally)
            if not tally.missing() and self._queues_idle():
                self.sleep(self.quiet_seconds)  # one more quiet period, then rescan
                self._scan(start, keys, tally)
                if not tally.missing() and self._queues_idle():
                    return self._compact(tally)
            waiting = ", ".join(tally.missing()) or "the queues to drain"
            hint = self._visit_dlq_hint()
            if hint and hint[0] != warned_depth:  # warn once per change in depth
                warned_depth = hint[0]
                self.log(f"settle {self.run_id}: warning: {hint[1]}")
            if self.clock() >= deadline:
                raise SettleTimeout(
                    f"run {self.run_id} did not settle in {self.timeout_seconds:.0f}s (waiting "
                    f"for {waiting}); the spool is kept: resume with "
                    f"`make settle ENV={self.env} RUN={self.run_id}`"
                    + (f". Warning: {hint[1]}" if hint else "")
                )
            if self.clock() >= next_log:
                self.log(f"settle {self.run_id}: waiting for {waiting}")
                next_log = self.clock() + LOG_EVERY_SECONDS
            self.sleep(self.poll_seconds)

    def _resume(self) -> set[str]:
        self.spool.parent.mkdir(parents=True, exist_ok=True)
        if self.spool.exists():  # drop a line torn by a crash; its key was never cached
            data = self.spool.read_bytes()
            cut = data.rfind(b"\n") + 1
            if cut != len(data):
                with open(self.spool, "r+b") as fh:
                    fh.truncate(cut)
        if not self.key_cache.exists():
            return set()
        return set(self.key_cache.read_text().split())

    def _spool_lines(self) -> Iterator[str]:
        if self.spool.exists():
            with open(self.spool) as fh:
                yield from (line for line in fh if line.strip())

    def _check_archive_dlq(self) -> None:
        stuck = Q.visible(self.conn.sqs, self.conn.outputs.archive_dlq_url)
        if stuck:
            raise GeneratorError(
                f"the archive DLQ holds {stuck} messages, so archived events are missing from "
                f"S3 raw/. Redrive them to the archive queue (SQS console 'Start DLQ redrive' "
                f"or `aws sqs start-message-move-task`), then run "
                f"`make settle ENV={self.env} RUN={self.run_id}`"
            )

    def _poison_injected(self) -> int:
        """Poison messages this env has injected: the sum of `k` over state/<env>/poison/*.json."""
        directory = state_dir(self.env) / "poison"
        if not directory.is_dir():
            return 0
        return sum(int(json.loads(p.read_text()).get("k", 0)) for p in directory.glob("*.json"))

    def _visit_dlq_hint(self) -> tuple[int, str] | None:
        """A warning when the visit DLQ holds more than the injected poison messages."""
        held = Q.visible(self.conn.sqs, self.conn.outputs.visit_dlq_url)
        injected = self._poison_injected()
        if held <= injected:
            return None
        return held, (
            f"visit DLQ holds {held} messages but only {injected} poison messages were "
            "injected: valid events may have dead-lettered; inspect the DLQ"
        )

    def _queues_idle(self) -> bool:
        outputs = self.conn.outputs
        return all(
            Q.is_idle(self.conn.sqs, url)
            for url in (outputs.visit_queue_url, outputs.archive_queue_url)
        )

    def _scan(self, start: datetime, keys: set[str], tally: Tally) -> int:
        new = 0
        for prefix in hour_prefixes(start, self.now()):
            for key in self.conn.keys(prefix):
                if key in keys:
                    continue
                obj = self.conn.s3.get_object(Bucket=self.conn.outputs.bucket, Key=key)
                envelopes = list(iter_json(obj["Body"].read().decode()))
                lines = "".join(json.dumps(e, separators=(",", ":")) + "\n" for e in envelopes)
                with open(self.spool, "a") as fh:  # spool before the key cache (spec §3.4)
                    fh.write(lines)
                with open(self.key_cache, "a") as fh:
                    fh.write(key + "\n")
                keys.add(key)
                for envelope in envelopes:
                    tally.add(envelope)
                new += 1
        return new

    def _compact(self, tally: Tally) -> dict:
        prefix = f"compacted/{self.env}/{self.run_id}/"
        parts: list[str] = []
        buffer: list[str] = []
        envelope_ids: set[str] = set()

        def flush() -> None:
            key = f"{prefix}part-{len(parts):04d}.ndjson"
            body = "".join(buffer).encode()
            self.conn.s3.put_object(Bucket=self.conn.outputs.bucket, Key=key, Body=body)
            parts.append(key)
            buffer.clear()

        stale = list(
            self.conn.keys(prefix)
        )  # a re-compaction may have fewer parts than a crashed one
        for i in range(0, len(stale), 1000):
            objects = [{"Key": k} for k in stale[i : i + 1000]]
            self.conn.s3.delete_objects(
                Bucket=self.conn.outputs.bucket, Delete={"Objects": objects, "Quiet": True}
            )
        written = 0
        for line in self._spool_lines():
            envelope_id = json.loads(line).get("id")
            if envelope_id is not None:
                if envelope_id in envelope_ids:  # the same delivery archived twice
                    continue
                envelope_ids.add(envelope_id)
            buffer.append(line)
            written += 1
            if len(buffer) >= PART_LINES:
                flush()
        if buffer:
            flush()
        settled = {
            "run_id": self.run_id,
            "env": self.env,
            "mode": self.manifest["mode"],
            "started_at": self.manifest["started_at"],
            "settled_at": self.now().isoformat(timespec="seconds"),
            "counts": self.manifest["counts"],
            "breaches": len(tally.breaches),
            "envelopes": written,
            "parts": parts,
        }
        self.conn.put_json(f"control/settled/{self.run_id}.json", settled)
        write_json(state_dir(self.env) / "settled" / f"{self.run_id}.json", settled)
        self.spool.unlink()
        self.key_cache.unlink(missing_ok=True)
        return settled
