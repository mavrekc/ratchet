from datetime import UTC, datetime

import pytest

from ratchet.events import (
    GENESIS_PREV_HASH,
    ChainVerificationError,
    Event,
    EventType,
    canonical_json,
    link,
    make_event,
    verify_chain,
    verify_event,
)


def test_canonical_json_stable_unicode() -> None:
    payload = {"note": "line\u2028separator and accent é", "emoji": "\U0001f680"}
    first = canonical_json(payload)
    second = canonical_json(payload)
    assert first == second

    event = make_event(
        "sess-unicode", 0, EventType.TASK_STARTED, payload, GENESIS_PREV_HASH, datetime.now(UTC)
    )
    restored = Event.model_validate_json(event.model_dump_json())
    assert restored == event
    assert verify_event(restored)


def test_event_round_trip() -> None:
    event = link(None, "sess-a", EventType.TASK_STARTED, {"x": 1})
    restored = Event.model_validate_json(event.model_dump_json())
    assert restored.hash == event.hash
    assert verify_event(restored)


def test_make_event_hash_matches() -> None:
    ts = datetime.now(UTC)
    event = make_event("sess-b", 0, EventType.TASK_STARTED, {"a": 1}, GENESIS_PREV_HASH, ts)
    assert verify_event(event)


def test_verify_event_detects_tamper() -> None:
    event = make_event(
        "sess-c", 0, EventType.TASK_STARTED, {"a": 1}, GENESIS_PREV_HASH, datetime.now(UTC)
    )
    tampered = event.model_copy(update={"payload": {"a": 2}})
    assert not verify_event(tampered)


def test_make_event_rejects_naive_datetime() -> None:
    naive_ts = datetime(2026, 1, 1)
    with pytest.raises(ValueError, match="timezone-aware"):
        make_event("sess-d", 0, EventType.TASK_STARTED, {}, GENESIS_PREV_HASH, naive_ts)


def test_link_chains_prev_hash_and_seq() -> None:
    first = link(None, "sess-e", EventType.TASK_STARTED, {})
    second = link(first, "sess-e", EventType.STEP_PLANNED, {"step": 1})
    assert second.seq == first.seq + 1
    assert second.prev_hash == first.hash


def test_first_event_uses_genesis() -> None:
    first = link(None, "sess-f", EventType.TASK_STARTED, {})
    assert first.seq == 0
    assert first.prev_hash == GENESIS_PREV_HASH


def test_link_rejects_session_mismatch() -> None:
    first = link(None, "sess-g", EventType.TASK_STARTED, {})
    with pytest.raises(ValueError, match="session_id"):
        link(first, "sess-other", EventType.STEP_PLANNED, {})


def _build_chain(session_id: str) -> list[Event]:
    events: list[Event] = []
    prev: Event | None = None
    for event_type, payload in [
        (EventType.TASK_STARTED, {"goal": "demo"}),
        (EventType.STEP_PLANNED, {"step": 1}),
        (EventType.TOOL_CALLED, {"tool": "search"}),
        (EventType.TOOL_RESULT, {"result": "ok"}),
        (EventType.TASK_DONE, {"status": "done"}),
    ]:
        event = link(prev, session_id, event_type, payload)
        events.append(event)
        prev = event
    return events


def test_verify_chain_accepts_valid() -> None:
    events = _build_chain("sess-h")
    verify_chain(events)


def test_verify_chain_rejects_tampered_payload() -> None:
    events = _build_chain("sess-i")
    tampered = events[2].model_copy(update={"payload": {"tool": "tampered"}})
    events[2] = tampered
    with pytest.raises(ChainVerificationError) as exc_info:
        verify_chain(events)
    assert exc_info.value.index == 2


def test_verify_chain_rejects_bad_seq() -> None:
    events = _build_chain("sess-j")
    events[3] = events[3].model_copy(update={"seq": 99})
    with pytest.raises(ChainVerificationError) as exc_info:
        verify_chain(events)
    assert exc_info.value.index == 3


def test_verify_chain_rejects_broken_prev_hash() -> None:
    events = _build_chain("sess-k")
    events[1] = events[1].model_copy(update={"prev_hash": "f" * 64})
    with pytest.raises(ChainVerificationError) as exc_info:
        verify_chain(events)
    assert exc_info.value.index == 1


def test_verify_chain_rejects_mixed_sessions() -> None:
    events = _build_chain("sess-l")
    events[4] = events[4].model_copy(update={"session_id": "sess-other"})
    with pytest.raises(ChainVerificationError) as exc_info:
        verify_chain(events)
    assert exc_info.value.index == 4


def test_verify_chain_empty_ok() -> None:
    verify_chain([])
