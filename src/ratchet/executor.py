"""Consumer-group step executor: XREADGROUP -> execute -> append -> XACK."""

import json
import logging
import threading
from collections.abc import Mapping

from pydantic import BaseModel, Field, ValidationError
from redis import Redis

from ratchet.broker import Broker, Message
from ratchet.errors import ChainForkError, UnknownStepError
from ratchet.eventlog import EventLog
from ratchet.events import EventType, JsonValue, canonical_json, sha256_hex
from ratchet.steps import STEP_REGISTRY, StepFn

logger = logging.getLogger("ratchet.executor")


class StepMessage(BaseModel):
    session_id: str
    step_id: str
    tool: str
    args: dict[str, JsonValue] = Field(default_factory=dict)
    idempotency_key: str
    attempt: int = 0

    def to_fields(self) -> dict[str, str]:
        return {
            "session_id": self.session_id,
            "step_id": self.step_id,
            "tool": self.tool,
            "args": json.dumps(self.args, sort_keys=True, separators=(",", ":")),
            "idempotency_key": self.idempotency_key,
            "attempt": str(self.attempt),
        }

    @classmethod
    def from_fields(cls, fields: Mapping[str, str]) -> "StepMessage":
        try:
            args = json.loads(fields.get("args", "{}"))
        except json.JSONDecodeError as e:
            raise ValueError(f"malformed args JSON: {e}") from e
        payload: dict[str, object] = {"args": args}
        for key in ("session_id", "step_id", "tool", "idempotency_key"):
            if key in fields:
                payload[key] = fields[key]
        if "attempt" in fields:
            try:
                payload["attempt"] = int(fields["attempt"])
            except ValueError as e:
                raise ValueError(f"malformed attempt: {e}") from e
        return cls.model_validate(payload)


def make_step_message(
    session_id: str,
    step_id: str,
    tool: str,
    args: Mapping[str, JsonValue],
    attempt: int = 0,
) -> StepMessage:
    args_dict = dict(args)
    idempotency_key = sha256_hex(
        canonical_json(
            {"session_id": session_id, "step_id": step_id, "tool": tool, "args": args_dict}
        )
    )
    return StepMessage(
        session_id=session_id,
        step_id=step_id,
        tool=tool,
        args=args_dict,
        idempotency_key=idempotency_key,
        attempt=attempt,
    )


class Worker:
    """Consumes step messages from a Broker and writes the event lifecycle."""

    def __init__(
        self,
        broker: Broker,
        redis: Redis,
        consumer: str,
        registry: Mapping[str, StepFn] = STEP_REGISTRY,
    ) -> None:
        self._broker = broker
        self._redis = redis
        self._consumer = consumer
        self._registry = registry
        self._stop_event = threading.Event()

    def run_once(self, block_ms: int = 5000, count: int = 10) -> int:
        # BrokerError/ChainForkError propagate unacked: the message stays in the
        # PEL for R2's recovery to claim - correct at-least-once behavior.
        messages = self._broker.consume(self._consumer, count=count, block_ms=block_ms)
        for message in messages:
            self._process(message)
        return len(messages)

    def run_forever(self, block_ms: int = 5000) -> None:
        # A lost same-session append race must not kill the worker; the message
        # stays unacked in the PEL for R2's claim path. BrokerError still exits.
        while not self._stop_event.is_set():
            try:
                self.run_once(block_ms=block_ms)
            except ChainForkError as e:
                logger.error("chain fork, message left pending: %s", e)

    def stop(self) -> None:
        self._stop_event.set()

    def _process(self, message: Message) -> None:
        try:
            step = StepMessage.from_fields(message.fields)
        except (ValidationError, ValueError) as e:
            self._handle_unparseable(message, e)
            return

        log = EventLog(self._redis, step.session_id)
        if log.tail() is None:
            log.append(EventType.TASK_STARTED, {})
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

        outcome = "ok"
        try:
            fn = self._registry.get(step.tool)
            if fn is None:
                raise UnknownStepError(f"unknown tool: {step.tool!r}")
            result = fn(step.args)
        except Exception as e:
            outcome = "failed"
            log.append(
                EventType.STEP_FAILED,
                {
                    "step_id": step.step_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "attempt": step.attempt,
                },
            )
        else:
            log.append(EventType.TOOL_RESULT, {"step_id": step.step_id, "result": result})
            # R1 contract: one step per session task, so task_done follows each
            # successful step. Task-boundary semantics arrive with R2 checkpoints.
            log.append(EventType.TASK_DONE, {"step_id": step.step_id})

        # ack-on-failure is interim until R2 DLQ/claim + R3 retry exist; without a
        # claimer an unacked failure would strand in the PEL forever.
        self._broker.ack(message.id)
        logger.info(
            "session_id=%s step_id=%s tool=%s outcome=%s",
            step.session_id,
            step.step_id,
            step.tool,
            outcome,
        )

    def _handle_unparseable(self, message: Message, error: Exception) -> None:
        session_id = message.fields.get("session_id")
        if session_id:
            log = EventLog(self._redis, session_id)
            log.append(EventType.STEP_FAILED, {"error": str(error), "error_type": "validation"})
        else:
            logger.warning("poison message id=%s fields=%s", message.id, message.fields)
        self._broker.ack(message.id)
