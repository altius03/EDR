from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from backend.kafka import VALIDATED_TOPIC
from tests.load.storage_fault_lab import FaultingProducer, FaultState, ObservedRegistry


@pytest.mark.parametrize("mode,forwarded", [("broken", 0), ("fixed", 1)])
def test_first_publish_failure_exposes_duplicate_branch(mode: str, forwarded: int) -> None:
    state = FaultState(mode)

    class Registry:
        @contextmanager
        def claim(self):
            yield SimpleNamespace(created=False)

    class Producer:
        def __init__(self) -> None:
            self.messages = []

        def publish(self, topic, *, key, value, headers=None):
            self.messages.append((topic, key, value, headers))
            return True

    with ObservedRegistry(Registry(), state).claim():
        assert state.claim_created is False
    delegate = Producer()
    producer = FaultingProducer(delegate, state)
    assert producer.publish(VALIDATED_TOPIC, key="1", value=b"event") is False
    assert producer.publish(VALIDATED_TOPIC, key="1", value=b"event") is True
    assert len(delegate.messages) == forwarded
