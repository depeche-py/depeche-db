import sys as _sys
from typing import List, Union

import pytest

from depeche_db import Subscription, SubscriptionMessage

# from ._tools import MyLockProvider, MyStateProvider, MyThreadLockProvider
from tests._account_example import (
    AccountCreditedEvent,
    AccountEvent,
    AccountRegisteredEvent,
)


def test_register_negative_cases(stream_with_events, subscription_factory):
    subscription: Subscription = subscription_factory(stream_with_events)
    subject = subscription.handler

    with pytest.raises(ValueError):
        subject.register(lambda: None)  # type: ignore

    with pytest.raises(ValueError):

        @subject.register  # type: ignore
        def handler1():
            pass

    with pytest.raises(ValueError):

        @subject.register  # type: ignore
        def handler2(message: AccountCreditedEvent, other: int):
            pass


@pytest.mark.skipif(_sys.version_info < (3, 10), reason="requires python3.10 or higher")
def test_register_overlap_union(stream_with_events, subscription_factory):
    subscription: Subscription = subscription_factory(stream_with_events)
    subject = subscription.handler

    @subject.register
    def handler1(event: AccountEvent):
        pass

    with pytest.raises(ValueError):

        @subject.register
        def handler2(event: AccountEvent):
            pass

    with pytest.raises(ValueError):

        @subject.register
        def handler3(event: AccountCreditedEvent):
            pass


def test_register_overlap_direct(stream_with_events, subscription_factory):
    subscription: Subscription = subscription_factory(stream_with_events)
    subject = subscription.handler

    @subject.register
    def handler1(event: AccountCreditedEvent):
        pass

    with pytest.raises(ValueError):

        @subject.register
        def handler2(event: AccountCreditedEvent):
            pass


def test_passes_right_type(stream_with_events, subscription_factory):
    subscription: Subscription = subscription_factory(stream_with_events)
    subject = subscription.handler

    seen: List[Union[SubscriptionMessage[AccountRegisteredEvent], AccountEvent]] = []

    @subject.register
    def handle_account_registered(event: SubscriptionMessage[AccountRegisteredEvent]):
        seen.append(event)

    @subject.register
    def handle_account_credited(event: AccountCreditedEvent):
        seen.append(event)

    subject.run_once()
    assert [type(obj) for obj in seen] == [
        SubscriptionMessage,
        SubscriptionMessage,
        AccountCreditedEvent,
        AccountCreditedEvent,
        AccountCreditedEvent,
    ]
