import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List

import pytest

from depeche_db import (
    AckStrategy,
    FixedTimeBudget,
    MessageHandlerRegister,
    RunOnNotificationResult,
    Subscription,
    SubscriptionMessage,
)
from depeche_db._subscription import (
    PooledBatchedAckSubscriptionRunner,
    PooledSubscriptionRunner,
)
from depeche_db.tools import DbSubscriptionStateProvider

# from ._tools import MyLockProvider, MyStateProvider, MyThreadLockProvider
from tests._account_example import (
    AccountEvent,
)


def test_subscription(db_engine, stream_with_events, subscription_factory):
    subject: Subscription = subscription_factory(stream_with_events)

    events = []
    while True:
        found = False
        for event in subject.get_next_messages(count=100):
            found = True
            events.append(event)
        if not found:
            break

    for _ in subject.get_next_messages(count=100):
        raise AssertionError("Should not have any more events")

    assert_subscription_event_order(events)


def test_db_subscription_state(
    identifier, db_engine, stream_with_events, lock_provider
):
    state_provider_name = identifier()
    subject: Subscription = stream_with_events.subscription(
        name=identifier(),
        lock_provider=lock_provider,
        state_provider=DbSubscriptionStateProvider(
            engine=db_engine, name=state_provider_name
        ),
    )

    events = []
    while True:
        found = False
        for event in subject.get_next_messages(count=100):
            found = True
            events.append(event)
        if not found:
            break

    assert_subscription_event_order(events)

    subject = stream_with_events.subscription(
        name=subject.name,
        lock_provider=lock_provider,
        state_provider=DbSubscriptionStateProvider(
            engine=db_engine, name=state_provider_name
        ),
    )

    for _ in subject.get_next_messages(count=100):
        raise AssertionError("Should not have any more events")


def test_db_subscription_state_batched(
    identifier, db_engine, stream_with_events, lock_provider
):
    state_provider_name = identifier()
    subject: Subscription = stream_with_events.subscription(
        name=identifier(),
        lock_provider=lock_provider,
        state_provider=DbSubscriptionStateProvider(
            engine=db_engine, name=state_provider_name
        ),
    )

    events = []
    while True:
        batch = subject.get_next_message_batch(count=100)
        if not batch or not batch.messages:
            break
        for event in batch.messages:
            events.append(event)
            batch.ack(event)
        subject.ack_message_batch(batch, success=True)

    assert_subscription_event_order(events)

    subject = stream_with_events.subscription(
        name=subject.name,
        lock_provider=lock_provider,
        state_provider=DbSubscriptionStateProvider(
            engine=db_engine, name=state_provider_name
        ),
    )

    for _ in subject.get_next_messages(count=100):
        raise AssertionError("Should not have any more events")


def test_subscription_in_parallel(db_engine, stream_with_events, subscription_factory):
    subject: Subscription = subscription_factory(stream_with_events)

    start = time.time()
    events = []

    def consume(n):
        failures = 0
        while failures < 10:
            found = False
            for event in subject.get_next_messages(count=1):
                events.append((event, time.time() - start))
                found = True
            if not found:
                time.sleep(0.001)
                failures += 1

    threads = [
        threading.Thread(target=consume, args=(1,)),
        threading.Thread(target=consume, args=(2,)),
        threading.Thread(target=consume, args=(3,)),
    ]
    for t in threads:
        t.start()

    for t in threads:
        t.join(timeout=2)

    assert_subscription_event_order([e for e, _ in sorted(events, key=lambda x: x[-1])])


@pytest.mark.parametrize("ack_strategy", [AckStrategy.SINGLE, AckStrategy.BATCHED])
def test_subscription_runner(
    db_engine, stream_with_events, lock_provider, identifier, ack_strategy
):
    events = []

    handlers = MessageHandlerRegister[AccountEvent]()

    @handlers.register
    def handle_account_event(event: SubscriptionMessage[AccountEvent]):
        events.append(event)

    subject: Subscription = stream_with_events.subscription(
        name=identifier(),
        lock_provider=lock_provider,
        handlers=handlers,
        ack_strategy=ack_strategy,
    )

    result = subject.runner.run_once()
    assert result == RunOnNotificationResult.DONE_FOR_NOW

    assert_subscription_event_order(events)

    for _ in subject.get_next_messages(count=100):
        raise AssertionError("Should not have any more events")


@pytest.mark.parametrize("ack_strategy", [AckStrategy.SINGLE, AckStrategy.BATCHED])
def test_subscription_runner_time_budget(
    db_engine, stream_with_events, lock_provider, identifier, ack_strategy
):
    events = []

    handlers = MessageHandlerRegister[AccountEvent]()

    @handlers.register
    def handle_account_event(event: SubscriptionMessage[AccountEvent]):
        events.append(event)
        # run over time budget to test that we stop processing
        time.sleep(0.1)

    subject: Subscription = stream_with_events.subscription(
        name=identifier(),
        lock_provider=lock_provider,
        handlers=handlers,
        ack_strategy=ack_strategy,
    )

    result = subject.runner.run_once(FixedTimeBudget(0.1))
    assert result == RunOnNotificationResult.WORK_REMAINING

    assert len(events) > 0, "Should have processed some events"
    assert_subscription_event_order(events)


def assert_subscription_event_order(events: List[SubscriptionMessage[AccountEvent]]):
    for partition in {evt.partition for evt in events}:
        partition_events = [evt for evt in events if evt.partition == partition]
        assert partition_events == sorted(
            partition_events, key=lambda evt: evt.position
        )


def run_pooled_runner_until_idle(runner, max_iterations=50, poll_interval=0.02):
    for _ in range(max_iterations):
        runner.run_once()
        if not runner._active_partitions:
            # No active workers, check if there's more work
            if runner.run_once() == RunOnNotificationResult.DONE_FOR_NOW:
                if not runner._active_partitions:
                    return  # Truly idle
        time.sleep(poll_interval)
    raise TimeoutError("Runner did not become idle within max_iterations")


@pytest.mark.parametrize("ack_strategy", [AckStrategy.SINGLE, AckStrategy.BATCHED])
def test_pooled_subscription_runner_with_executor_true(
    db_engine, stream_with_events, lock_provider, identifier, ack_strategy
):
    """Test pooled runner with executor=True (auto-creates ThreadPoolExecutor)."""
    events = []

    handlers = MessageHandlerRegister[AccountEvent]()

    @handlers.register
    def handle_account_event(event: SubscriptionMessage[AccountEvent]):
        events.append(event)

    subject: Subscription = stream_with_events.subscription(
        name=identifier(),
        lock_provider=lock_provider,
        handlers=handlers,
        ack_strategy=ack_strategy,
        executor=True,
    )

    if ack_strategy == AckStrategy.SINGLE:
        assert isinstance(subject.runner, PooledSubscriptionRunner)
    else:
        assert isinstance(subject.runner, PooledBatchedAckSubscriptionRunner)

    run_pooled_runner_until_idle(subject.runner)
    subject.runner.stop()

    assert len(events) == 5, f"Expected 5 events, got {len(events)}"
    assert_subscription_event_order(events)


@pytest.mark.parametrize("ack_strategy", [AckStrategy.SINGLE, AckStrategy.BATCHED])
def test_pooled_subscription_runner_with_shared_executor(
    db_engine, stream_with_events, lock_provider, identifier, ack_strategy
):
    events = []

    handlers = MessageHandlerRegister[AccountEvent]()

    @handlers.register
    def handle_account_event(event: SubscriptionMessage[AccountEvent]):
        events.append(event)

    shared_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="shared_")

    try:
        subject: Subscription = stream_with_events.subscription(
            name=identifier(),
            lock_provider=lock_provider,
            handlers=handlers,
            ack_strategy=ack_strategy,
            executor=shared_executor,
        )

        # Verify the runner does not own the executor
        assert not subject.runner._own_executor  # type: ignore

        run_pooled_runner_until_idle(subject.runner)
        subject.runner.stop()

        assert len(events) == 5, f"Expected 5 events, got {len(events)}"
        assert_subscription_event_order(events)
    finally:
        shared_executor.shutdown(wait=True)


@pytest.mark.parametrize("ack_strategy", [AckStrategy.SINGLE, AckStrategy.BATCHED])
def test_pooled_subscription_runner_error_propagation(
    db_engine, stream_with_events, lock_provider, identifier, ack_strategy
):
    error_message = "Test error from handler"

    handlers = MessageHandlerRegister[AccountEvent]()

    @handlers.register
    def handle_account_event(event: SubscriptionMessage[AccountEvent]):
        raise RuntimeError(error_message)

    subject: Subscription = stream_with_events.subscription(
        name=identifier(),
        lock_provider=lock_provider,
        handlers=handlers,
        ack_strategy=ack_strategy,
        executor=True,
    )

    result = subject.runner.run_once()
    assert result == RunOnNotificationResult.WORK_REMAINING

    # Wait for worker threads to process and fail
    time.sleep(0.2)

    # Next run_once should raise the error from the worker thread
    with pytest.raises(RuntimeError) as exc_info:
        subject.runner.run_once()

    assert error_message in str(exc_info.value)
    subject.runner.stop()


@pytest.mark.parametrize("ack_strategy", [AckStrategy.SINGLE, AckStrategy.BATCHED])
def test_pooled_subscription_runner_stop(
    db_engine, stream_with_events, lock_provider, identifier, ack_strategy
):
    """Test that stop() causes early exit - doesn't process all messages."""
    events = []
    handler_sleep_time = 0.3  # Slow handler

    handlers = MessageHandlerRegister[AccountEvent]()

    @handlers.register
    def handle_account_event(event: SubscriptionMessage[AccountEvent]):
        events.append(event)
        time.sleep(handler_sleep_time)

    subject: Subscription = stream_with_events.subscription(
        name=identifier(),
        lock_provider=lock_provider,
        handlers=handlers,
        ack_strategy=ack_strategy,
        executor=True,
    )

    subject.runner.run_once()

    # Wait just enough for some messages to start processing
    time.sleep(0.05)

    # Stop the runner and measure how long it takes
    stop_start = time.time()
    subject.runner.stop()
    stop_duration = time.time() - stop_start

    # Verify the runner is stopped
    assert not subject.runner._keep_running

    # stop() should complete relatively quickly - it waits for currently
    # processing messages but doesn't process new ones.
    # If it processed all 5 messages sequentially, it would take ~1.5s
    # With early exit, it should be much faster (just finishing current work)
    assert stop_duration < 1.0, f"stop() took too long: {stop_duration:.2f}s"

    # We should have processed some but not all events (early exit)
    # At minimum 1 event (the one being processed when stop was called)
    assert len(events) >= 1, "Should have processed at least one event"
    assert (
        len(events) < 5
    ), f"Should have exited early, but processed all {len(events)} events"


def test_pooled_subscription_runner_concurrent_partitions(
    db_engine, stream_with_events, lock_provider, identifier
):
    """Test that multiple partitions are actually processed concurrently."""
    processing_times = []
    processing_lock = threading.Lock()

    handlers = MessageHandlerRegister[AccountEvent]()

    @handlers.register
    def handle_account_event(event: SubscriptionMessage[AccountEvent]):
        start = time.time()
        time.sleep(0.1)  # Simulate work
        with processing_lock:
            processing_times.append((event.partition, start, time.time()))

    subject: Subscription = stream_with_events.subscription(
        name=identifier(),
        lock_provider=lock_provider,
        handlers=handlers,
        ack_strategy=AckStrategy.SINGLE,
        executor=True,
    )

    run_pooled_runner_until_idle(subject.runner)
    subject.runner.stop()

    # Verify events were processed
    assert len(processing_times) == 5

    # If partitions are processed concurrently, some time ranges should overlap
    has_overlap = False
    for i, (p1, s1, e1) in enumerate(processing_times):
        for p2, s2, e2 in processing_times[i + 1 :]:
            if p1 != p2:
                # Check for overlap: ranges overlap if one starts before the other ends
                if s1 < e2 and s2 < e1:
                    has_overlap = True
                    break
        if has_overlap:
            break

    assert has_overlap, "Expected concurrent processing of different partitions"


def test_pooled_subscription_runner_state_persistence(
    identifier, db_engine, stream_with_events, lock_provider
):
    state_provider_name = identifier()
    events = []

    handlers = MessageHandlerRegister[AccountEvent]()

    @handlers.register
    def handle_account_event(event: SubscriptionMessage[AccountEvent]):
        events.append(event)

    subject: Subscription = stream_with_events.subscription(
        name=identifier(),
        lock_provider=lock_provider,
        handlers=handlers,
        state_provider=DbSubscriptionStateProvider(
            engine=db_engine, name=state_provider_name
        ),
        ack_strategy=AckStrategy.SINGLE,
        executor=True,
    )

    run_pooled_runner_until_idle(subject.runner)
    subject.runner.stop()

    assert len(events) == 5
    assert_subscription_event_order(events)

    # Create new subscription with same state - should have no new events
    events2 = []

    handlers2 = MessageHandlerRegister[AccountEvent]()

    @handlers2.register
    def handle_account_event2(event: SubscriptionMessage[AccountEvent]):
        events2.append(event)

    subject2: Subscription = stream_with_events.subscription(
        name=subject.name,
        lock_provider=lock_provider,
        handlers=handlers2,
        state_provider=DbSubscriptionStateProvider(
            engine=db_engine, name=state_provider_name
        ),
        ack_strategy=AckStrategy.SINGLE,
        executor=True,
    )

    subject2.runner.run_once()
    time.sleep(0.1)
    subject2.runner.stop()

    assert len(events2) == 0, "Should not have any more events after resuming"
