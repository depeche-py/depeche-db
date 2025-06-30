import datetime as _dt
import uuid as _uuid
from typing import Any, List

import pytest

from depeche_db import (
    ExitSubscriptionErrorHandler,
    LogAndIgnoreSubscriptionErrorHandler,
    MessageHandler,
    MessageHandlerRegister,
    StoredMessage,
    Subscription,
    SubscriptionMessage,
    SubscriptionMessageHandler,
)
from tests._account_example import (
    AccountCreditedEvent,
    AccountEvent,
    AccountRegisteredEvent,
)

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
        added_at=_dt.datetime(2025, 1, 1, 13),
    ),
    ack=None,
)


def test_passes_undecorated_type():
    register: MessageHandlerRegister[Any] = MessageHandlerRegister()
    seen: List[AccountEvent] = []

    @register.register
    def handle_account_credited(event: AccountCreditedEvent):
        seen.append(event)

    subject = SubscriptionMessageHandler(register)
    subject.handle(SUB_MSG)
    assert [type(obj) for obj in seen] == [AccountCreditedEvent]


def test_passes_stored_message():
    register: MessageHandlerRegister[Any] = MessageHandlerRegister()

    seen: List[StoredMessage[AccountCreditedEvent]] = []

    @register.register
    def handle_account_credited(event: StoredMessage[AccountCreditedEvent]):
        seen.append(event)

    subject = SubscriptionMessageHandler(register)
    subject.handle(SUB_MSG)
    assert [type(obj) for obj in seen] == [StoredMessage]


def test_accepts_class_based_handlers():
    seen: List[StoredMessage[AccountCreditedEvent]] = []

    class Handler(MessageHandler[AccountEvent]):
        @MessageHandler.register
        def handle(self, event: StoredMessage[AccountCreditedEvent]):
            seen.append(event)

    register = Handler()
    subject = SubscriptionMessageHandler(register)
    subject.handle(SUB_MSG)
    assert [type(obj) for obj in seen] == [StoredMessage]


def test_passes_subscription_message():
    register: MessageHandlerRegister[Any] = MessageHandlerRegister()

    seen: List[SubscriptionMessage[AccountCreditedEvent]] = []

    @register.register
    def handle_account_credited(event: SubscriptionMessage[AccountCreditedEvent]):
        seen.append(event)

    subject = SubscriptionMessageHandler(register)
    subject.handle(SUB_MSG)
    assert [type(obj) for obj in seen] == [SubscriptionMessage]


def test_register_with_additional_params_requires_call_middleware():
    register: MessageHandlerRegister[Any] = MessageHandlerRegister()

    @register.register
    def handle_account_credited(event: AccountCreditedEvent, foo: int):
        pass

    with pytest.raises(ValueError):
        SubscriptionMessageHandler(register)


def test_register_with_additional_params_is_ok():
    register: MessageHandlerRegister[Any] = MessageHandlerRegister()

    @register.register
    def handle_account_credited(event: AccountCreditedEvent, foo: int):
        pass

    @register.register
    def handle_account_registered(
        event: SubscriptionMessage[AccountRegisteredEvent], foo: int
    ):
        pass

    SubscriptionMessageHandler(register, call_middleware="dummy")  # type: ignore[arg-type]


def test_uses_call_middleware():
    class Middleware:
        def call(self, handler, message):
            return handler(message, 123)

    register: MessageHandlerRegister[Any] = MessageHandlerRegister()
    seen: List[int] = []

    @register.register
    def handle_account_credited(event: AccountCreditedEvent, foo: int):
        seen.append(foo)

    subject = SubscriptionMessageHandler(register, call_middleware=Middleware())  # type: ignore
    subject.handle(SUB_MSG)
    assert seen == [123]


def test_exhausts_the_aggregated_stream(identifier, stream_with_events):
    register: MessageHandlerRegister[Any] = MessageHandlerRegister()
    seen: List[AccountEvent] = []

    @register.register
    def handle(event: AccountEvent):
        seen.append(event)

    subscription: Subscription = stream_with_events.subscription(
        name=identifier(), handlers=register
    )
    subscription.runner.run_once()
    assert len(seen) == 5


def test_ignores_exception():
    register: MessageHandlerRegister[Any] = MessageHandlerRegister()

    @register.register
    def handle_account_credited(event: SubscriptionMessage[AccountCreditedEvent]):
        raise ValueError("foo")

    subject = SubscriptionMessageHandler(
        register, error_handler=LogAndIgnoreSubscriptionErrorHandler("foo")
    )
    subject.handle(SUB_MSG)
    # No exception raised


def test_reraises_exception():
    register: MessageHandlerRegister[Any] = MessageHandlerRegister()

    @register.register
    def handle_account_credited(event: SubscriptionMessage[AccountCreditedEvent]):
        raise ValueError("foo")

    subject = SubscriptionMessageHandler(
        register, error_handler=ExitSubscriptionErrorHandler()
    )
    with pytest.raises(ValueError):
        subject.handle(SUB_MSG)
