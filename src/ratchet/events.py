"""Event schema and hash chain for ratchet's append-only session event log."""

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict

from ratchet.errors import ChainVerificationError

type JsonValue = str | bool | int | float | None | list["JsonValue"] | dict[str, "JsonValue"]

GENESIS_PREV_HASH: str = "0" * 64


class EventType(str, Enum):  # noqa: UP042 - exact base class required by the event schema spec
    TASK_STARTED = "task_started"
    STEP_PLANNED = "step_planned"
    TOOL_CALLED = "tool_called"
    TOOL_RESULT = "tool_result"
    CHECKPOINT = "checkpoint"
    BUDGET_TICK = "budget_tick"
    STEP_FAILED = "step_failed"
    RESUMED = "resumed"
    TASK_DONE = "task_done"


class Event(BaseModel):
    model_config = ConfigDict(frozen=True)

    session_id: str
    seq: int
    type: EventType
    payload: dict[str, JsonValue]
    prev_hash: str
    ts: datetime
    hash: str


def canonical_json(value: JsonValue) -> bytes:
    """Serialize a JSON value to canonical bytes, used only for hash input."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def now_utc() -> datetime:
    return datetime.now(UTC)


def _compute_hash(
    session_id: str,
    seq: int,
    event_type: EventType,
    payload: Mapping[str, JsonValue],
    prev_hash: str,
    ts: datetime,
) -> str:
    body: JsonValue = {
        "session_id": session_id,
        "seq": seq,
        "type": event_type.value,
        "payload": dict(payload),
        "prev_hash": prev_hash,
        "ts": ts.isoformat(),
    }
    return sha256_hex(canonical_json(body))


def make_event(
    session_id: str,
    seq: int,
    type: EventType,
    payload: Mapping[str, JsonValue],
    prev_hash: str,
    ts: datetime,
) -> Event:
    """Build a hashed Event. Rejects naive datetimes; normalizes ts to UTC."""
    if ts.tzinfo is None:
        raise ValueError("ts must be timezone-aware")
    ts_utc = ts.astimezone(UTC)
    payload_dict = dict(payload)
    event_hash = _compute_hash(session_id, seq, type, payload_dict, prev_hash, ts_utc)
    return Event(
        session_id=session_id,
        seq=seq,
        type=type,
        payload=payload_dict,
        prev_hash=prev_hash,
        ts=ts_utc,
        hash=event_hash,
    )


def link(
    prev: Event | None,
    session_id: str,
    type: EventType,
    payload: Mapping[str, JsonValue],
    ts: datetime | None = None,
) -> Event:
    """Build the next Event after prev, or a genesis event when prev is None."""
    if prev is not None and prev.session_id != session_id:
        raise ValueError("session_id must match prev.session_id")
    seq = prev.seq + 1 if prev is not None else 0
    prev_hash = prev.hash if prev is not None else GENESIS_PREV_HASH
    ts_value = ts if ts is not None else now_utc()
    return make_event(session_id, seq, type, payload, prev_hash, ts_value)


def verify_event(event: Event) -> bool:
    """Recompute the event hash from its own fields and compare to event.hash."""
    expected = _compute_hash(
        event.session_id, event.seq, event.type, event.payload, event.prev_hash, event.ts
    )
    return expected == event.hash


def verify_chain(events: Sequence[Event]) -> None:
    """Verify session, seq, prev_hash linkage and hashes; raise at the first bad index."""
    if not events:
        return
    session_id = events[0].session_id
    expected_prev_hash = GENESIS_PREV_HASH
    for index, event in enumerate(events):
        if event.session_id != session_id:
            raise ChainVerificationError(index, "session_id mismatch")
        if event.seq != index:
            raise ChainVerificationError(index, "seq out of order")
        if event.prev_hash != expected_prev_hash:
            raise ChainVerificationError(index, "prev_hash mismatch")
        if not verify_event(event):
            raise ChainVerificationError(index, "hash mismatch")
        expected_prev_hash = event.hash
