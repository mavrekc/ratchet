import json
from datetime import UTC, datetime

from hypothesis import given, settings
from hypothesis import strategies as st

from ratchet.events import (
    Event,
    EventType,
    JsonValue,
    canonical_json,
    link,
    verify_chain,
    verify_event,
)

json_values: st.SearchStrategy[JsonValue] = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(),
    lambda children: (
        st.lists(children, max_size=5) | st.dictionaries(st.text(), children, max_size=5)
    ),
    max_leaves=20,
)

json_payloads: st.SearchStrategy[dict[str, JsonValue]] = st.dictionaries(
    st.text(), json_values, max_size=5
)

event_types = st.sampled_from(list(EventType))


@given(json_payloads)
@settings(deadline=None)
def test_canonical_json_key_order_independent(payload: dict[str, JsonValue]) -> None:
    reordered = dict(reversed(payload.items()))
    assert canonical_json(payload) == canonical_json(reordered)


@given(json_values)
@settings(deadline=None)
def test_canonical_json_fixpoint(value: JsonValue) -> None:
    encoded = canonical_json(value)
    assert canonical_json(json.loads(encoded)) == encoded


@given(json_payloads, event_types)
@settings(deadline=None)
def test_event_hash_round_trip_property(
    payload: dict[str, JsonValue], event_type: EventType
) -> None:
    event = link(None, "sess-prop", event_type, payload, ts=datetime.now(UTC))
    restored = Event.model_validate_json(event.model_dump_json())
    assert restored.hash == event.hash
    assert verify_event(restored)


@given(st.lists(st.tuples(event_types, json_payloads), min_size=1, max_size=10))
@settings(deadline=None)
def test_verify_chain_property(steps: list[tuple[EventType, dict[str, JsonValue]]]) -> None:
    events: list[Event] = []
    prev: Event | None = None
    for event_type, payload in steps:
        event = link(prev, "sess-chain-prop", event_type, payload)
        events.append(event)
        prev = event
    verify_chain(events)
