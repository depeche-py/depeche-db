import inspect as _inspect
import typing as _typing
from typing import (
    Callable,
    Dict,
    Generic,
    Optional,
    Type,
    TypeVar,
)

from ._compat import UNION_TYPES, issubclass_with_union
from ._interfaces import (
    HandlerDescriptor,
    MessageProtocol,
    StoredMessage,
    SubscriptionMessage,
)

E = TypeVar("E", bound=MessageProtocol)
H = TypeVar("H", bound=Callable)


class MessageHandlerRegister(Generic[E]):
    def __init__(
        self,
    ):
        self._handlers: Dict[Type[E], HandlerDescriptor] = {}

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
        self._handlers[handled_type] = HandlerDescriptor(
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

    def get_handler(self, message_type: Type[E]) -> Optional[HandlerDescriptor[E]]:
        for handled_type, handler in self._handlers.items():
            if issubclass_with_union(message_type, handled_type):
                return handler
        return None

    def get_all_handlers(self) -> _typing.Iterator[HandlerDescriptor[E]]:
        yield from self._handlers.values()


class MessageHandler(Generic[E]):
    _register: MessageHandlerRegister[E]

    def __init__(self):
        self._register = MessageHandlerRegister()
        for attrname in dir(self):
            attr = getattr(self, attrname)
            if getattr(attr, "__message_handler__", False):
                self._register.register(attr)

    @staticmethod
    def register(handler: H) -> H:
        handler.__message_handler__ = True  # type: ignore
        return handler

    def get_handler(self, message_type: Type[E]) -> Optional[HandlerDescriptor[E]]:
        return self._register.get_handler(message_type)

    def get_all_handlers(self) -> _typing.Iterator[HandlerDescriptor[E]]:
        yield from self._register.get_all_handlers()
