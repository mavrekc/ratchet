import pytest
from pydantic import ValidationError

from ratchet.executor import StepMessage, make_step_message


def test_step_message_round_trip() -> None:
    msg = make_step_message("sess-1", "step-1", "echo", {"a": [1, {"b": "x"}]}, attempt=2)

    restored = StepMessage.from_fields(msg.to_fields())

    assert restored == msg


def test_idempotency_key_deterministic() -> None:
    first = make_step_message("sess-1", "step-1", "echo", {"a": 1})
    second = make_step_message("sess-1", "step-1", "echo", {"a": 1})

    assert first.idempotency_key == second.idempotency_key


def test_idempotency_key_varies_with_args() -> None:
    base = make_step_message("sess-1", "step-1", "echo", {"a": 1})
    diff_args = make_step_message("sess-1", "step-1", "echo", {"a": 2})
    diff_step = make_step_message("sess-1", "step-2", "echo", {"a": 1})
    diff_tool = make_step_message("sess-1", "step-1", "sleep", {"a": 1})

    assert base.idempotency_key != diff_args.idempotency_key
    assert base.idempotency_key != diff_step.idempotency_key
    assert base.idempotency_key != diff_tool.idempotency_key


def test_from_fields_rejects_missing_tool() -> None:
    fields = {
        "session_id": "sess-1",
        "step_id": "step-1",
        "idempotency_key": "deadbeef",
        "args": "{}",
    }

    with pytest.raises(ValidationError):
        StepMessage.from_fields(fields)


def test_from_fields_rejects_bad_args_json() -> None:
    fields = {
        "session_id": "sess-1",
        "step_id": "step-1",
        "tool": "echo",
        "idempotency_key": "deadbeef",
        "args": "{not-json",
    }

    with pytest.raises(ValueError, match="malformed args JSON"):
        StepMessage.from_fields(fields)
