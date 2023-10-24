from typing import Any

import pytest

from depeche_db import (
    MessageHandlerRegister,
)
from tests._account_example import (
    AccountCreditedEvent,
    AccountEvent,
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
