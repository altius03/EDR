"""Local-only model of a lost validated publish and a broken duplicate path."""

import os
from contextlib import contextmanager
from dataclasses import dataclass

from backend.kafka import VALIDATED_TOPIC
from tools import run_event_storage_worker


@dataclass
class FaultState:
    mode: str
    failed_once: bool = False
    claim_created: bool = True
    logged_drop: bool = False


class ObservedRegistry:
    def __init__(self, delegate, state: FaultState) -> None:
        self.delegate = delegate
        self.state = state

    @contextmanager
    def claim(self, **kwargs):
        with self.delegate.claim(**kwargs) as claim:
            self.state.claim_created = claim.created
            yield claim


class FaultingProducer:
    def __init__(self, delegate, state: FaultState) -> None:
        self.delegate = delegate
        self.state = state

    def publish(self, topic, *, key, value, headers=None):
        if topic == VALIDATED_TOPIC:
            if not self.state.failed_once:
                self.state.failed_once = True
                print("LAB: first validated publish returns no ACK; storage worker will retry", flush=True)
                return False
            if self.state.mode == "broken" and not self.state.claim_created:
                if not self.state.logged_drop:
                    print("LAB: broken duplicate path drops validated publish", flush=True)
                    self.state.logged_drop = True
                return True
        return self.delegate.publish(topic, key=key, value=value, headers=headers)


def main() -> int:
    if os.getenv("EDR_ENV") != "local" or os.getenv("EDR_LOCAL_FAULT_EXPERIMENT") != "1":
        raise SystemExit("fault lab requires the explicit local Compose overlay")
    mode = os.getenv("EDR_EXPERIMENT_MODE")
    if mode not in {"broken", "fixed"}:
        raise SystemExit("EDR_EXPERIMENT_MODE must be broken or fixed")

    state = FaultState(mode)
    original_factory = run_event_storage_worker._worker

    def lab_worker(runtime, consumer, connection):
        worker = original_factory(runtime, consumer, connection)
        worker.registry = ObservedRegistry(worker.registry, state)
        worker.producer = FaultingProducer(worker.producer, state)
        return worker

    run_event_storage_worker._worker = lab_worker
    return run_event_storage_worker.main()


if __name__ == "__main__":
    raise SystemExit(main())
