import threading
import time
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
