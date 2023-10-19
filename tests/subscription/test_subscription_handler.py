import uuid as _uuid
from typing import List

import pytest

from depeche_db import (
    ExitSubscriptionErrorHandler,
    LogAndIgnoreSubscriptionErrorHandler,
    StoredMessage,
    Subscription,
    SubscriptionHandler,
    SubscriptionMessage,
)
from tests._account_example import (
    AccountCreditedEvent,
    AccountEvent,
    AccountRegisteredEvent,
)


def test_register_negative_cases(stream_with_events, subscription_factory):
    subscription: Subscription = subscription_factory(stream_with_events)
    subject = subscription.handler

    with pytest.raises(ValueError):
        subject.register(lambda: None)

    with pytest.raises(ValueError):

        @subject.register
        def handler1():
            pass

    with pytest.raises(TypeError):

        @subject.register
        def handler2(foo: int):
            pass


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


MSG_ID = _uuid.uuid4()
SUB_MSG = SubscriptionMessage(
    partition=0,
    position=0,
    stored_message=StoredMessage(
        message_id=MSG_ID,
        stream="stream",
        version=0,
        message=AccountCreditedEvent(
            event_id=MSG_ID,
            account_id=_uuid.uuid4(),
            amount=1,
            balance=1,
        ),
        global_position=0,
    ),
)


def test_passes_undecorated_type():
    subject = SubscriptionHandler(None, None)  # type: ignore

    seen: List[AccountEvent] = []

    @subject.register
    def handle_account_credited(event: AccountCreditedEvent):
        seen.append(event)

    subject.handle(SUB_MSG)
    assert [type(obj) for obj in seen] == [AccountCreditedEvent]


def test_passes_stored_message():
    subject = SubscriptionHandler(None, None)  # type: ignore

    seen: List[StoredMessage[AccountCreditedEvent]] = []

    @subject.register
    def handle_account_credited(event: StoredMessage[AccountCreditedEvent]):
        seen.append(event)

    subject.handle(SUB_MSG)
    assert [type(obj) for obj in seen] == [StoredMessage]


def test_passes_subscription_message():
    subject = SubscriptionHandler(None, None)  # type: ignore

    seen: List[SubscriptionMessage[AccountCreditedEvent]] = []

    @subject.register
    def handle_account_credited(event: SubscriptionMessage[AccountCreditedEvent]):
        seen.append(event)

    subject.handle(SUB_MSG)
    assert [type(obj) for obj in seen] == [SubscriptionMessage]


def test_register_with_additional_params_requires_call_middleware():
    subject = SubscriptionHandler(None, None)  # type: ignore

    with pytest.raises(ValueError):

        @subject.register
        def handle_account_credited(event: AccountCreditedEvent, foo: int):
            pass


def test_register_with_additional_params_is_ok():
    subject = SubscriptionHandler(None, None, call_middleware="dummy")  # type: ignore

    @subject.register
    def handle_account_credited(event: AccountCreditedEvent, foo: int):
        pass

    @subject.register
    def handle_account_registered(
        event: SubscriptionMessage[AccountRegisteredEvent], foo: int
    ):
        pass


def test_uses_call_middleware():
    class Middleware:
        def call(self, handler, message):
            return handler(message, 123)

    subject = SubscriptionHandler(None, ExitSubscriptionErrorHandler(), call_middleware=Middleware())  # type: ignore

    seen: List[int] = []

    @subject.register
    def handle_account_credited(event: AccountCreditedEvent, foo: int):
        seen.append(foo)

    subject.handle(SUB_MSG)
    assert seen == [123]


def test_exhausts_the_aggregated_stream(stream_with_events, subscription_factory):
    subscription: Subscription = subscription_factory(stream_with_events)
    subject = subscription.handler

    seen: List[AccountEvent] = []

    @subject.register
    def handle(event: AccountEvent):
        seen.append(event)

    subject.run_once()
    assert len(seen) == 5


def test_ignores_exception():
    subject = SubscriptionHandler(None, error_handler=LogAndIgnoreSubscriptionErrorHandler("foo"))  # type: ignore

    @subject.register
    def handle_account_credited(event: SubscriptionMessage[AccountCreditedEvent]):
        raise ValueError("foo")

    subject.handle(SUB_MSG)
    # No exception raised


def test_reraises_exception():
    subject = SubscriptionHandler(None, error_handler=ExitSubscriptionErrorHandler())  # type: ignore

    @subject.register
    def handle_account_credited(event: SubscriptionMessage[AccountCreditedEvent]):
        raise ValueError("foo")

    with pytest.raises(ValueError):
        subject.handle(SUB_MSG)
