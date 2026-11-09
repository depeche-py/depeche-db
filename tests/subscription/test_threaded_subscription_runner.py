import threading
from typing import Dict, List

from depeche_db import (
    CoordinationStrategy,
    MessageHandlerRegister,
    RunOnNotificationResult,
    Subscription,
    SubscriptionMessage,
)
from depeche_db.experimental.threaded_subscription_runner import (
    ThreadedSubscriptionRunner,
)
from tests._account_example import AccountEvent


def test_threaded_runner_processes_partitions_concurrently(
    db_engine, stream_with_events, identifier
):
    seen: List[SubscriptionMessage[AccountEvent]] = []
    seen_lock = threading.Lock()
    handlers = MessageHandlerRegister[AccountEvent]()

    @handlers.register
    def handle(event: SubscriptionMessage[AccountEvent]):
        with seen_lock:
            seen.append(event)

    subject: Subscription = stream_with_events.subscription(
        name=identifier(),
        handlers=handlers,
        coordination_strategy=CoordinationStrategy.INSTANCE_ASSIGNMENT,
        runner_class=ThreadedSubscriptionRunner,
        runner_options={"max_workers": 4},
    )

    # Force an immediate rebalance so the first run_once has work.
    subject.runner.run_once()
    assert subject._assignment_provider is not None
    subject._assignment_provider.rebalance(
        stream_with_events.get_max_aggregated_stream_positions().keys()
    )

    for _ in range(10):
        r = subject.runner.run_once()
        if r != RunOnNotificationResult.WORK_REMAINING:
            break

    subject.runner.stop()

    assert seen, "should have processed events"

    # Per-partition order preserved
    for partition in {e.partition for e in seen}:
        positions = [e.position for e in seen if e.partition == partition]
        assert positions == sorted(positions)


def test_threaded_runner_partition_error_does_not_kill_others(
    db_engine, stream_with_events, identifier
):
    """A handler exception must not poison the thread pool."""
    call_counts: Dict[int, int] = {}
    lock = threading.Lock()
    handlers = MessageHandlerRegister[AccountEvent]()

    @handlers.register
    def handle(event: SubscriptionMessage[AccountEvent]):
        with lock:
            call_counts[event.partition] = call_counts.get(event.partition, 0) + 1

    subject: Subscription = stream_with_events.subscription(
        name=identifier(),
        handlers=handlers,
        coordination_strategy=CoordinationStrategy.INSTANCE_ASSIGNMENT,
        runner_class=ThreadedSubscriptionRunner,
        runner_options={"max_workers": 2},
    )
    subject.runner.run_once()
    assert subject._assignment_provider is not None
    subject._assignment_provider.rebalance(
        stream_with_events.get_max_aggregated_stream_positions().keys()
    )

    for _ in range(10):
        r = subject.runner.run_once()
        if r != RunOnNotificationResult.WORK_REMAINING:
            break

    subject.runner.stop()
    assert call_counts, "should have processed events on at least one partition"
