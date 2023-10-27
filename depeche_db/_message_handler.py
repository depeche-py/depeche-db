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
    """
    Message handler register is a registry of message handlers.

    Typical usage:

        handlers = MessageHandlerRegister()

        @handlers.register
        def handle_message(message: MyMessage):
            ...

        @handlers.register
        def handle_other_message(message: StoredMessage[MyOtherMessage]):
            ...

    Implements: [MessageHandlerRegisterProtocol][depeche_db.MessageHandlerRegisterProtocol]
    """

    def __init__(
        self,
    ):
        self._handlers: Dict[Type[E], HandlerDescriptor] = {}

    def register(self, handler: H) -> H:
        """
        Registers a handler for a given message type.

        The handler must have at least one parameter. The first parameter must
        be of a message type. `E` being your message type, the parameter can be
        of type `E`, `SubscriptionMessage[E]` or `StoredMessage[E]`. When a
        handler is called, the message will be passed in the requested type.

        Multiple handlers can be registered for non-overlapping types of
        messages. Overlaps will cause a `ValueError`.

        Args:
            handler: A handler function.

        Returns:
            The unaltered handler function.
        """
        signature = _inspect.signature(handler)
        if len(signature.parameters) < 1:
            raise ValueError("Handler must have at least one parameter")

        handled_type = list(signature.parameters.values())[0].annotation
        self.register_manual(
            handler=handler,
            handled_type=handled_type,
            requires_middleware=len(signature.parameters) > 1,
        )
        return handler

    def register_manual(
        self, handler: H, handled_type: Type, requires_middleware: bool = False
    ):
        """
        Registers a handler for a given message type.

        The handler must have at least one parameter. The first parameter must
        be of a message type.

        Same overlap rules apply to this method as to the `register` method.

        If the handler takes more than one parameter, you must set the
        `requires_middleware` parameter to `True`!

        Args:
            handler: A handler function.
            handled_type: The type of message to handle.
            requires_middleware: Whether the handler requires middleware.
        """
        pass_subscription_message = False
        pass_stored_message = False
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
            requires_middleware=requires_middleware,
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
    """
    Message handler is a base class for message handlers.

    This is basically a class-based version of the `MessageHandlerRegister`.

    Typical usage (equivalent to the example in `MessageHandlerRegister`):

        class MyMessageHandler(MessageHandler):
            @MessageHandler.register
            def handle_message(self, message: MyMessage):
                ...

            @MessageHandler.register
            def handle_other_message(self, message: StoredMessage[MyOtherMessage]):
                ...

    Implements: [MessageHandlerRegisterProtocol][depeche_db.MessageHandlerRegisterProtocol]
    """

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
