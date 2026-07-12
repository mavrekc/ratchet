import pytest
from pydantic import ValidationError

from ratchet.executor import TaskMessage, make_task_message


def _three_step_plan() -> list[tuple[str, str, dict[str, object]]]:
    return [
        ("step-0", "echo", {"a": [1, {"b": "x"}]}),
        ("step-1", "sleep", {"seconds": 0}),
        ("step-2", "echo", {"nested": {"deep": [True, None, 3.5]}}),
    ]


def test_task_message_round_trip() -> None:
    msg = make_task_message("sess-1", _three_step_plan(), cursor=1, attempt=2)

    restored = TaskMessage.from_fields(msg.to_fields())

    assert restored == msg
    assert len(restored.plan) == 3
    assert restored.plan[0].args == {"a": [1, {"b": "x"}]}


def test_idempotency_key_deterministic() -> None:
    first = make_task_message("sess-1", [("step-1", "echo", {"a": 1})])
    second = make_task_message("sess-1", [("step-1", "echo", {"a": 1})])

    assert first.plan[0].idempotency_key == second.plan[0].idempotency_key


def test_idempotency_key_varies_with_args_step_and_tool() -> None:
    base = make_task_message("sess-1", [("step-1", "echo", {"a": 1})]).plan[0].idempotency_key
    diff_args = make_task_message("sess-1", [("step-1", "echo", {"a": 2})]).plan[0].idempotency_key
    diff_step = make_task_message("sess-1", [("step-2", "echo", {"a": 1})]).plan[0].idempotency_key
    diff_tool = make_task_message("sess-1", [("step-1", "sleep", {"a": 1})]).plan[0].idempotency_key

    assert base != diff_args
    assert base != diff_step
    assert base != diff_tool


def test_idempotency_key_differs_between_steps_of_one_plan() -> None:
    msg = make_task_message(
        "sess-1",
        [("step-0", "echo", {"n": 0}), ("step-1", "echo", {"n": 1})],
    )

    keys = [step.idempotency_key for step in msg.plan]
    assert keys[0] != keys[1]
    assert len(set(keys)) == 2


def test_from_fields_rejects_missing_plan() -> None:
    with pytest.raises(ValueError, match="malformed plan JSON"):
        TaskMessage.from_fields({"session_id": "sess-1", "cursor": "0", "attempt": "0"})


def test_from_fields_rejects_malformed_plan_json() -> None:
    fields = {"session_id": "sess-1", "plan": "{not-json", "cursor": "0", "attempt": "0"}

    with pytest.raises(ValueError, match="malformed plan JSON"):
        TaskMessage.from_fields(fields)


def test_from_fields_rejects_missing_session_id() -> None:
    msg = make_task_message("sess-1", [("step-1", "echo", {})])
    fields = msg.to_fields()
    del fields["session_id"]

    with pytest.raises(ValidationError):
        TaskMessage.from_fields(fields)


def test_from_fields_rejects_bad_cursor() -> None:
    msg = make_task_message("sess-1", [("step-1", "echo", {})])
    fields = msg.to_fields()
    fields["cursor"] = "not-an-int"

    with pytest.raises(ValueError, match="malformed cursor"):
        TaskMessage.from_fields(fields)


def test_from_fields_rejects_empty_plan_list() -> None:
    fields = {"session_id": "sess-1", "plan": "[]", "cursor": "0", "attempt": "0"}

    with pytest.raises(ValidationError):
        TaskMessage.from_fields(fields)


def test_cursor_and_attempt_default_zero_and_survive_the_wire() -> None:
    msg = make_task_message("sess-1", [("step-1", "echo", {})])
    assert msg.cursor == 0
    assert msg.attempt == 0

    restored = TaskMessage.from_fields(msg.to_fields())
    assert restored.cursor == 0
    assert restored.attempt == 0

    with_values = make_task_message("sess-1", [("step-1", "echo", {})], cursor=3, attempt=5)
    restored_values = TaskMessage.from_fields(with_values.to_fields())
    assert restored_values.cursor == 3
    assert restored_values.attempt == 5
