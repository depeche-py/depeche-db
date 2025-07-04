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
    AccountRepository,
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


def test_subscription_cached_stats_update_via_state(
    db_engine, stream_with_events, subscription_factory
):
    subject: Subscription = subscription_factory(stream_with_events)

    def get_stats():
        with db_engine.connect() as conn:
            return {
                stat.partition_number: stat
                for stat in subject._get_cached_partition_statistics(conn)
            }

    stats = get_stats()
    assert stats[1].next_message_position == 0

    subject._state_provider.store(
        subscription_name=subject.name, partition=1, position=0
    )
    stats_after_state_update = get_stats()
    assert stats_after_state_update[1].next_message_position == 1
    assert stats_after_state_update[2] == stats[2]


def test_subscription_cached_stats_update_via_stream_update(
    db_engine, stream_with_events, subscription_factory, store_with_events
):
    subject: Subscription = subscription_factory(stream_with_events)

    def get_stats():
        with db_engine.connect() as conn:
            return {
                stat.partition_number: stat
                for stat in subject._get_cached_partition_statistics(conn)
            }

    # Initial load with empty cache
    stats = get_stats()
    assert stats[1].next_message_position == 0
    assert stats[1].max_position == 2

    # We "consume" one of events in partition 1
    subject._state_provider.store(
        subscription_name=subject.name, partition=1, position=1
    )
    stats = get_stats()
    assert stats[1].next_message_position == 2
    assert stats[1].max_position == 2

    # Another event is added to partition 1
    store, account, _ = store_with_events
    account_repo = AccountRepository(store)
    account.credit(100)
    account_repo.save(account, expected_version=3)
    stream_with_events.projector.update_full()

    # The stats should now reflect the new event
    stats_after_state_update = get_stats()
    assert stats_after_state_update[1].next_message_position == 2
    assert stats_after_state_update[1].max_position == 3


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


def test_subscription_get_next_message_batch_from_partition(
    db_engine, stream_with_events, subscription_factory, store_with_events
):
    subject: Subscription = subscription_factory(stream_with_events)
    messages = []

    # We "consume" one of events in partition 1
    subject._state_provider.store(
        subscription_name=subject.name, partition=1, position=1
    )
    batch = subject.get_next_message_batch_from_partition(partition_number=1, count=2)
    assert batch is not None
    assert len(batch.messages) == 1
    for message in batch.messages:
        messages.append(message)
        assert message.partition == 1
        assert message.position >= 1
        batch.ack(message)
    subject.ack_message_batch(message_batch=batch, success=True)

    batch = subject.get_next_message_batch_from_partition(partition_number=1, count=2)
    assert batch is None

    # Another event is added to partition 1
    store, account, _ = store_with_events
    account_repo = AccountRepository(store)
    account.credit(100)
    account.credit(100)
    account_repo.save(account, expected_version=3)
    stream_with_events.projector.update_full()

    batch = subject.get_next_message_batch_from_partition(partition_number=1, count=2)
    assert batch is not None
    assert len(batch.messages) == 2
    for message in batch.messages:
        messages.append(message)
        assert message.partition == 1
        assert message.position >= 1

    assert_subscription_event_order(messages)
