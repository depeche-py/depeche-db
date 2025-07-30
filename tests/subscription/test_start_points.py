import datetime as dt
from typing import List

import pytest

from depeche_db import (
    StartAtNextMessage,
    StartAtPointInTime,
    Subscription,
    SubscriptionMessage,
    SubscriptionState,
)
from depeche_db.tools import DbSubscriptionStateProvider


def test_default_beginning(
    identifier, stream_with_events, lock_provider, state_provider
):
    subject = stream_with_events.subscription(
        name=identifier(),
        lock_provider=lock_provider,
        state_provider=state_provider,
        start_point=None,
    )
    assert not state_provider.initialized(subject.name)
    assert len(list(subject.get_next_messages(count=1))) == 1
    assert state_provider.initialized(subject.name)
    assert len(state_provider.read(subject.name).positions) == 1


def test_next_message(identifier, stream_with_events, lock_provider, state_provider):
    subject = stream_with_events.subscription(
        name=identifier(),
        lock_provider=lock_provider,
        state_provider=state_provider,
        start_point=StartAtNextMessage(),
    )
    assert not state_provider.initialized(subject.name)
    assert list(subject.get_next_messages(count=1)) == []
    assert state_provider.initialized(subject.name)
    assert state_provider.read(subject.name) == SubscriptionState(
        positions={1: 2, 2: 1}
    )


def test_point_in_time(
    identifier, store_with_events, stream_with_events, lock_provider, state_provider
):
    _, account, account2 = store_with_events
    events = sorted(
        account.events + account2.events, key=lambda e: e.get_message_time()
    )
    times = [e.get_message_time().replace(tzinfo=dt.timezone.utc) for e in events]

    for idx, time in enumerate(times):
        print(idx, time)
        subject = stream_with_events.subscription(
            name=identifier(),
            lock_provider=lock_provider,
            state_provider=state_provider,
            start_point=StartAtPointInTime(time),
        )
        assert not state_provider.initialized(subject.name)
        assert {msg.stored_message.message_id for msg in exhaust(subject)} == {
            e.get_message_id() for e in events[idx:]
        }
        assert state_provider.initialized(subject.name)


def exhaust(subscription: Subscription) -> List[SubscriptionMessage]:
    result: List[SubscriptionMessage] = []
    while True:
        batch = list(subscription.get_next_messages(count=100))
        if not batch:
            break
        result.extend(batch)
    return result


@pytest.fixture
def state_provider(identifier, db_engine):
    return DbSubscriptionStateProvider(engine=db_engine, name=identifier())
