import dataclasses as _dc
import inspect as _inspect
import typing as _typing
from typing import (
    Callable,
    Dict,
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
    no_type_check,
)

from ._compat import UNION_TYPES, issubclass_with_union
from ._interfaces import (
    MessageProtocol,
    StoredMessage,
    SubscriptionMessage,
)

E = TypeVar("E", bound=MessageProtocol)


@_dc.dataclass
class _Handler:
    handler: Callable
    pass_subscription_message: bool
    pass_stored_message: bool
    requires_middleware: bool

    @no_type_check
    def adapt_message_type(
        self, message: Union[SubscriptionMessage[E], StoredMessage[E], E]
    ) -> Union[SubscriptionMessage[E], StoredMessage[E], E]:
        if isinstance(message, SubscriptionMessage):
            if self.pass_subscription_message:
                return message
            elif self.pass_stored_message:
                return message.stored_message
            else:
                return message.stored_message.message
        elif isinstance(message, StoredMessage):
            if self.pass_subscription_message:
                raise ValueError(
                    "SubscriptionMessage was requested, but StoredMessage was provided"
                )
            elif self.pass_stored_message:
                return message.stored_message
            else:
                return message.stored_message.message
        else:
            if self.pass_subscription_message or self.pass_stored_message:
                raise ValueError(
                    "SubscriptionMessage or StoredMessage was requested, but plain message was provided"
                )
            else:
                return message


H = TypeVar("H", bound=Callable)


class MessageHandlerRegister(Generic[E]):
    def __init__(
        self,
    ):
        self._handlers: Dict[Type[E], _Handler] = {}

    def register(self, handler: H) -> H:
        signature = _inspect.signature(handler)
        if len(signature.parameters) < 1:
            raise ValueError("Handler must have at least one parameter")

        pass_subscription_message = False
        pass_stored_message = False
        handled_type = list(signature.parameters.values())[0].annotation
        origin = _typing.get_origin(handled_type)
        if origin == SubscriptionMessage:
            pass_subscription_message = True
            handled_type = handled_type.__args__[0]
        elif origin == StoredMessage:
            pass_stored_message = True
            handled_type = handled_type.__args__[0]

        if not issubclass_with_union(handled_type, MessageProtocol):
            raise TypeError(
                "Handled type (ie. the first argument) must be a subclass of MessageProtocol"
            )

        self.assert_not_registered(handled_type)
        self._handlers[handled_type] = _Handler(
            handler=handler,
            pass_subscription_message=pass_subscription_message,
            pass_stored_message=pass_stored_message,
            requires_middleware=len(signature.parameters) > 1,
        )
        return handler

    def assert_not_registered(self, handled_type: Type[E]):
        if _typing.get_origin(handled_type) in UNION_TYPES:
            for member in _typing.get_args(handled_type):
                self.assert_not_registered(member)
        else:
            for registered_type in self._handlers:
                if issubclass_with_union(handled_type, registered_type):
                    raise ValueError(
                        f"Handler for {handled_type} is already registered for {registered_type}"
                    )

    def get_handler(self, message_type: Type[E]) -> Optional[_Handler]:
        for handled_type, handler in self._handlers.items():
            if issubclass_with_union(message_type, handled_type):
                return handler
        return None
