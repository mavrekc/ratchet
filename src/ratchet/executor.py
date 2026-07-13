"""Consumer-group executor: multi-step plans with log-derived checkpoint/resume."""

import json
import logging
import threading
from collections.abc import Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from redis import Redis

from ratchet.broker import Broker, Message
from ratchet.dlq import DeadLetterQueue
from ratchet.errors import BrokerError, ChainForkError, UnknownStepError
from ratchet.eventlog import EventLog
from ratchet.events import Event, EventType, JsonValue, canonical_json, sha256_hex
from ratchet.steps import STEP_REGISTRY, StepFn

logger = logging.getLogger("ratchet.executor")


class PlanStep(BaseModel):
    model_config = ConfigDict(frozen=True)

    step_id: str
    tool: str
    args: dict[str, JsonValue] = Field(default_factory=dict)
    idempotency_key: str


class TaskMessage(BaseModel):
    session_id: str
    plan: list[PlanStep] = Field(min_length=1)
    cursor: int = 0
    attempt: int = 0

    def to_fields(self) -> dict[str, str]:
        return {
            "session_id": self.session_id,
            "plan": json.dumps(
                [step.model_dump() for step in self.plan],
                sort_keys=True,
                separators=(",", ":"),
            ),
            "cursor": str(self.cursor),
            "attempt": str(self.attempt),
        }

    @classmethod
    def from_fields(cls, fields: Mapping[str, str]) -> "TaskMessage":
        try:
            plan = json.loads(fields["plan"])
        except (KeyError, json.JSONDecodeError) as e:
            raise ValueError(f"malformed plan JSON: {e}") from e
        payload: dict[str, object] = {"plan": plan}
        if "session_id" in fields:
            payload["session_id"] = fields["session_id"]
        for key in ("cursor", "attempt"):
            if key in fields:
                try:
                    payload[key] = int(fields[key])
                except ValueError as e:
                    raise ValueError(f"malformed {key}: {e}") from e
        return cls.model_validate(payload)


def make_task_message(
    session_id: str,
    plan: Sequence[tuple[str, str, Mapping[str, JsonValue]]],
    *,
    cursor: int = 0,
    attempt: int = 0,
) -> TaskMessage:
    steps: list[PlanStep] = []
    for step_id, tool, args in plan:
        args_dict = dict(args)
        idempotency_key = sha256_hex(
            canonical_json(
                {"session_id": session_id, "step_id": step_id, "tool": tool, "args": args_dict}
            )
        )
        steps.append(
            PlanStep(step_id=step_id, tool=tool, args=args_dict, idempotency_key=idempotency_key)
        )
    return TaskMessage(session_id=session_id, plan=steps, cursor=cursor, attempt=attempt)


class Worker:
    """Consumes task messages, drives the plan, and writes the event lifecycle."""

    def __init__(
        self,
        broker: Broker,
        redis: Redis,
        consumer: str,
        *,
        min_idle_ms: int = 30000,
        dlq: DeadLetterQueue | None = None,
        registry: Mapping[str, StepFn] = STEP_REGISTRY,
    ) -> None:
        self._broker = broker
        self._redis = redis
        self._consumer = consumer
        self._min_idle_ms = min_idle_ms
        self._dlq = dlq if dlq is not None else DeadLetterQueue(redis)
        self._registry = registry
        self._stop_event = threading.Event()

    def run_once(self, block_ms: int = 5000, count: int = 10) -> int:
        # Infra errors (BrokerError/ChainForkError) propagate unacked: the message
        # stays in the PEL for another claim cycle. Recovery is log-state driven.
        claimed = self._broker.claim(self._consumer, self._min_idle_ms, count)
        for message in claimed:
            self._process(message)
        fresh = self._broker.consume(self._consumer, count=count, block_ms=block_ms)
        for message in fresh:
            self._process(message)
        return len(claimed) + len(fresh)

    def run_forever(
        self,
        block_ms: int = 5000,
        max_consecutive_errors: int = 5,
        error_backoff_s: float = 0.5,
    ) -> None:
        consecutive = 0
        while not self._stop_event.is_set():
            try:
                self.run_once(block_ms=block_ms)
            except ChainForkError as e:
                logger.warning("benign double-claim fork, message left pending: %s", e)
            except BrokerError as e:
                consecutive += 1
                logger.warning("broker error %d/%d: %s", consecutive, max_consecutive_errors, e)
                if consecutive >= max_consecutive_errors:
                    logger.error("broker error threshold reached, exiting: %s", e)
                    raise
                self._stop_event.wait(error_backoff_s)
            else:
                consecutive = 0

    def stop(self) -> None:
        self._stop_event.set()

    def _process(self, message: Message) -> None:
        try:
            task = TaskMessage.from_fields(message.fields)
        except (ValidationError, ValueError) as e:
            self._handle_poison(message, e)
            return

        log = EventLog(self._redis, task.session_id)
        events = log.read()
        plan = task.plan

        if any(e.type in (EventType.TASK_DONE, EventType.STEP_FAILED) for e in events):
            self._broker.ack(message.id)
            logger.info(
                "session_id=%s consumer=%s outcome=already_terminal",
                task.session_id,
                self._consumer,
            )
            return

        resumed = False
        if not events:
            log.append(EventType.TASK_STARTED, {})
            start = task.cursor
        else:
            resumed = True
            start = self._resume(log, events, plan)

        for i in range(start, len(plan)):
            step = plan[i]
            log.append(EventType.STEP_PLANNED, {"step_id": step.step_id, "tool": step.tool})
            log.append(
                EventType.TOOL_CALLED,
                {
                    "step_id": step.step_id,
                    "tool": step.tool,
                    "args": step.args,
                    "idempotency_key": step.idempotency_key,
                },
            )
            try:
                fn = self._registry.get(step.tool)
                if fn is None:
                    raise UnknownStepError(f"unknown tool: {step.tool!r}")
                result = fn(step.args)
            except Exception as e:
                self._dead_letter_step(message, log, task, step, e)
                return
            log.append(EventType.TOOL_RESULT, {"step_id": step.step_id, "result": result})
            log.append(EventType.CHECKPOINT, {"cursor": i + 1, "completed_step_id": step.step_id})

        log.append(EventType.TASK_DONE, {"steps": len(plan)})
        self._broker.ack(message.id)
        logger.info(
            "session_id=%s consumer=%s steps=%d outcome=ok resumed=%s",
            task.session_id,
            self._consumer,
            len(plan),
            "yes" if resumed else "no",
        )

    def _resume(self, log: EventLog, events: Sequence[Event], plan: Sequence[PlanStep]) -> int:
        checkpoint_cursors: list[int] = []
        for e in events:
            if e.type == EventType.CHECKPOINT:
                cursor = e.payload.get("cursor")
                if isinstance(cursor, int):
                    checkpoint_cursors.append(cursor)
        resume_cursor = max(checkpoint_cursors) if checkpoint_cursors else 0

        if resume_cursor >= len(plan):
            start = resume_cursor
        else:
            interrupted = plan[resume_cursor]
            completed = any(
                e.type == EventType.TOOL_RESULT and e.payload.get("step_id") == interrupted.step_id
                for e in events
            )
            if completed:
                log.append(
                    EventType.CHECKPOINT,
                    {"cursor": resume_cursor + 1, "completed_step_id": interrupted.step_id},
                )
                start = resume_cursor + 1
            else:
                # Re-execute the interrupted step: safe on side-effect-free stubs; the
                # tool_called-without-tool_result window is closed honestly only by R3 dedup.
                start = resume_cursor
        log.append(EventType.RESUMED, {"cursor": start, "consumer": self._consumer})
        return start

    def _dead_letter_step(
        self,
        message: Message,
        log: EventLog,
        task: TaskMessage,
        step: PlanStep,
        error: Exception,
    ) -> None:
        # Push BEFORE the terminal marker so terminal-failed implies a DLQ entry; a
        # push BrokerError propagates unacked and the message is reclaimed.
        self._dlq.push(
            session_id=task.session_id,
            step_id=step.step_id,
            tool=step.tool,
            error=str(error),
            error_type=type(error).__name__,
            attempt=task.attempt,
            events=log.read()[-50:],
            original=dict(message.fields),
        )
        log.append(
            EventType.STEP_FAILED,
            {
                "step_id": step.step_id,
                "error": str(error),
                "error_type": type(error).__name__,
                "attempt": task.attempt,
            },
        )
        self._broker.ack(message.id)
        logger.info(
            "session_id=%s step_id=%s tool=%s outcome=failed",
            task.session_id,
            step.step_id,
            step.tool,
        )

    def _handle_poison(self, message: Message, error: Exception) -> None:
        # Never write to the session log here: a corrupt message carrying a live
        # session's id must not inject a terminal marker into that session.
        session_id = message.fields.get("session_id", "")
        events: list[Event] = []
        if session_id:
            events = EventLog(self._redis, session_id).read()[-50:]
        self._dlq.push(
            session_id=session_id,
            step_id="",
            tool="",
            error=str(error),
            error_type="validation",
            attempt=0,
            events=events,
            original=dict(message.fields),
        )
        self._broker.ack(message.id)
        logger.warning("poison message id=%s error=%s", message.id, error)
