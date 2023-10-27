from typing import Any, List

import pytest

from depeche_db import MessageHandler, MessageHandlerRegister
from tests._account_example import (
    AccountCreditedEvent,
    AccountEvent,
    AccountRegisteredEvent,
)


def test_register_negative_cases():
    subject: MessageHandlerRegister[Any] = MessageHandlerRegister()

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


def test_register_overlap_union():
    subject: MessageHandlerRegister[Any] = MessageHandlerRegister()

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


def test_register_overlap_direct():
    subject: MessageHandlerRegister[Any] = MessageHandlerRegister()

    @subject.register
    def handler1(event: AccountCreditedEvent):
        pass

    with pytest.raises(ValueError):

        @subject.register
        def handler2(event: AccountCreditedEvent):
            pass


def test_class_based_handler():
    seen: List[AccountEvent] = []

    class MyClassBasedHandler(MessageHandler[AccountEvent]):
        @MessageHandler.register
        def handler(self, event: AccountCreditedEvent):
            seen.append(event)

    subject = MyClassBasedHandler()
    assert subject.get_handler(AccountCreditedEvent) is not None
    assert subject.get_handler(AccountRegisteredEvent) is None

    subject.get_handler(AccountCreditedEvent).handler(1)
    assert len(seen) == 1


def test_register_manual():
    subject: MessageHandlerRegister[Any] = MessageHandlerRegister()

    def handler1(event):
        pass

    subject.register_manual(handler1, AccountCreditedEvent)
