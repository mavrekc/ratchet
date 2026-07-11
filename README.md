# ratchet

Agents that survive `kill -9`.

ratchet is a durable, queue-native runtime for AI agents: checkpointed, replayable, budget-capped
step execution built on message-queue engineering rather than a workflow DSL. If each step of an
8-step agent loop independently succeeds 85% of the time, the loop completes end to end only about
27% of the time, which is a large part of why so many agent projects stall before production.
Reliability at that layer is a distributed-systems problem: retries, idempotency, dead-letter
queues, backpressure, and checkpoint/resume. ratchet is a reference implementation of that layer,
sitting underneath whatever agent loop you already have.

## Status

Early stage. The design is complete and implementation proceeds milestone by milestone (see
Roadmap). Milestone 1 has landed: the append-only hash-chained session event log (atomic
compare-and-append, fork detection, full-chain verification) and the Redis Streams consumer-group
executor (explicit XREADGROUP/XACK/PEL handling behind a thin broker interface), running stubbed
steps with no model calls. Next up: checkpoint/resume with a `kill -9` chaos suite and a
dead-letter stream.

## Architecture

```mermaid
flowchart LR
    P[Producer<br/>task_started] --> S[(Redis Stream<br/>consumer group)]
    S --> W[Worker<br/>step executor + budget meter]
    W -->|tool_called, tool_result| L[(Session event log<br/>append-only, hash-chained)]
    W -->|checkpoint| L
    W -->|retries exhausted| DLQ[(Dead-letter stream)]
    L -->|replay, no live calls| R[Deterministic replay]
    R --> E[Labeled trajectory export]
    DLQ -->|inspect, requeue| S
```

## Why this exists

- Compounding per-step failure is what breaks agents in production rather than in demos, and
  reliability engineering is the named gap
  ([why agent projects fail](https://www.digitalapplied.com/blog/88-percent-ai-agents-never-reach-production-failure-framework)).
- Durable execution is the missing layer, as Temporal frames it, but their answer is a general
  workflow DSL rather than an agent-native one
  ([AI reliability is a decade-old problem](https://temporal.io/blog/ai-reliability-is-a-decade-old-problem)).
- Anthropic's managed-agents architecture separates brain, hands, and session into independently
  failable parts, with the session as a durable append-only event log
  ([managed agents](https://www.anthropic.com/engineering/managed-agents)). ratchet implements that
  session layer as an open, inspectable system.
- Multi-agent fan-out is expensive and often loses to a well-engineered single agent, at roughly
  15x the token cost
  ([multi-agent research system](https://www.anthropic.com/engineering/built-multi-agent-research-system)).
  ratchet prioritizes single-agent reliability and makes that cost trade-off visible per step.

## What the first release delivers

The first release delivers, with stubbed steps and no model calls: an append-only, hash-chained
session event log; a Redis Streams consumer-group executor with at-least-once delivery; checkpoint
and resume that survive a worker `kill -9` in a chaos test suite; a dead-letter stream for
exhausted retries; and idempotency keys that guarantee zero duplicate side effects under retry. The
real agent loop, tool layer, budgets, tracing, and additional brokers follow.

## Roadmap

1. Session event-log schema and a Redis Streams consumer-group executor (stubbed steps).
2. Checkpoint and resume, a `kill -9` chaos suite, and a dead-letter stream.
3. Idempotency keys, retry policies, and a backpressure governor.
4. A real agent loop (plan, act, reflect) on top of the runtime.
5. A tool layer with per-tool idempotency contracts.
6. Per-step budgets, cost attribution, and a RabbitMQ adapter.
7. Tracing and a Kafka event log.
8. Deterministic replay exported as labeled trajectories.

## Try it

With a local Redis (`docker compose up -d redis`, which enables AOF persistence):

```bash
uv sync
uv run python -m ratchet --sessions 5 --workers 2
```

Output from a real run - two workers drain five sessions from the consumer group, append each
step's lifecycle to its session's hash-chained event log, and verify every chain:

```
INFO ratchet.executor session_id=demo-de329c4a-0 step_id=step-0 tool=echo outcome=ok
INFO ratchet.executor session_id=demo-de329c4a-1 step_id=step-1 tool=echo outcome=ok
INFO ratchet.executor session_id=demo-de329c4a-2 step_id=step-2 tool=echo outcome=ok
INFO ratchet.executor session_id=demo-de329c4a-3 step_id=step-3 tool=echo outcome=ok
INFO ratchet.executor session_id=demo-de329c4a-4 step_id=step-4 tool=echo outcome=ok
session=demo-de329c4a-0 chain=verified events=task_started,step_planned,tool_called,tool_result,task_done
session=demo-de329c4a-1 chain=verified events=task_started,step_planned,tool_called,tool_result,task_done
session=demo-de329c4a-2 chain=verified events=task_started,step_planned,tool_called,tool_result,task_done
session=demo-de329c4a-3 chain=verified events=task_started,step_planned,tool_called,tool_result,task_done
session=demo-de329c4a-4 chain=verified events=task_started,step_planned,tool_called,tool_result,task_done
sessions=5 completed=5 workers=2
all session event chains verified
```

Fully containerized, the same demo runs with `docker compose up --build app`.

## Development

```bash
uv sync
docker compose up -d redis
make check       # lint, typecheck, test (integration tests need the redis service)
make docker-build
```

## License

Apache-2.0. See [LICENSE](LICENSE).
