from depeche_db import SubscriptionHandler, SubscriptionMessage

# from ._tools import MyLockProvider, MyStateProvider, MyThreadLockProvider
from tests._account_example import (
    AccountCreditedEvent,
    AccountEvent,
    AccountRegisteredEvent,
)


def test_register(stream_with_events, subscription_factory):
    subscription = subscription_factory(stream_with_events)
    SubscriptionHandler(subscription)


def test_subscription_handler(stream_with_events, subscription_factory):
    subscription = subscription_factory(stream_with_events)
    handler = SubscriptionHandler(subscription)

    seen: list[SubscriptionMessage[AccountRegisteredEvent] | AccountEvent] = []

    @handler.register
    def handle_account_registered(event: SubscriptionMessage[AccountRegisteredEvent]):
        seen.append(event)

    @handler.register
    def handle_account_credited(event: AccountCreditedEvent):
        seen.append(event)

    handler.run_once()
    assert [type(obj) for obj in seen] == [
        SubscriptionMessage,
        SubscriptionMessage,
        AccountCreditedEvent,
        AccountCreditedEvent,
        AccountCreditedEvent,
    ]
