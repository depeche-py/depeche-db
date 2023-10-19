import dataclasses as _dc
import enum as _enum
import inspect as _inspect
import logging as _logging
import typing as _typing
from typing import Callable, Dict, Generic, Iterator, Optional, Type, TypeVar, Union

from . import tools as _tools
from ._aggregated_stream import AggregatedStream
from ._compat import UNION_TYPES, issubclass_with_union
from ._interfaces import (
    LockProvider,
    MessageProtocol,
    StoredMessage,
    SubscriptionStateProvider,
)

E = TypeVar("E", bound=MessageProtocol)

DEPECHE_LOGGER = _logging.getLogger("depeche_db")


@_dc.dataclass(frozen=True)
class SubscriptionMessage(Generic[E]):
    partition: int
    position: int
    stored_message: StoredMessage[E]


class SubscriptionErrorHandler(Generic[E]):
    class Action(_enum.Enum):
        IGNORE = "ignore"
        EXIT = "exit"

    def handle_error(self, error: Exception, message: SubscriptionMessage[E]) -> Action:
        raise NotImplementedError


class ExitSubscriptionErrorHandler(SubscriptionErrorHandler):
    def handle_error(
        self, error: Exception, message: SubscriptionMessage[E]
    ) -> SubscriptionErrorHandler.Action:
        return SubscriptionErrorHandler.Action.EXIT


class LogAndIgnoreSubscriptionErrorHandler(SubscriptionErrorHandler):
    def __init__(self, subscription_name: str):
        self._logger = _logging.getLogger(
            f"depeche_db.subscription.{subscription_name}"
        )

    def handle_error(
        self, error: Exception, message: SubscriptionMessage[E]
    ) -> SubscriptionErrorHandler.Action:
        self._logger.exception(
            "Error while handling message {message.stored_message.message_id}:{message.stored_message.message.__class__.__name__}"
        )
        return SubscriptionErrorHandler.Action.IGNORE


class CallMiddleware(Generic[E]):
    def call(
        self,
        handler: Callable,
        message: Union[SubscriptionMessage[E], StoredMessage[E], E],
    ):
        raise NotImplementedError


class Subscription(Generic[E]):
    def __init__(
        self,
        name: str,
        stream: AggregatedStream[E],
        state_provider: Optional[SubscriptionStateProvider] = None,
        lock_provider: Optional[LockProvider] = None,
        error_handler: Optional[SubscriptionErrorHandler] = None,
        call_middleware: Optional[CallMiddleware] = None,
        # TODO start at time
        # TODO start at "next message"
    ):
        assert name.isidentifier(), "Group name must be a valid identifier"
        self.name = name
        self._stream = stream
        self._lock_provider = lock_provider or _tools.DbLockProvider(
            name, self._stream._store.engine
        )
        self._state_provider = state_provider or _tools.DbSubscriptionStateProvider(
            name, self._stream._store.engine
        )
        self.handler = SubscriptionHandler(
            subscription=self,
            error_handler=error_handler,
            call_middleware=call_middleware,
        )

    def get_next_messages(self, count: int) -> Iterator[SubscriptionMessage[E]]:
        state = self._state_provider.read(self.name)
        statistics = list(
            self._stream.get_partition_statistics(
                position_limits=state.positions, result_limit=10
            )
        )
        for statistic in statistics:
            lock_key = f"subscription-{self.name}-{statistic.partition_number}"
            if not self._lock_provider.lock(lock_key):
                continue
            # now we have the lock, we need to check if the position is still valid
            # if not, we need to release the lock and try the next partition
            state = self._state_provider.read(self.name)
            if state.positions.get(statistic.partition_number, -1) != (
                statistic.next_message_position - 1
            ):
                self._lock_provider.unlock(lock_key)
                continue
            try:
                with self._stream._store.reader() as reader:
                    message_pointers = list(
                        self._stream.read_slice(
                            partition=statistic.partition_number,
                            start=statistic.next_message_position,
                            count=count,
                        )
                    )
                    stored_messages = {
                        message.message_id: message
                        for message in reader.get_messages_by_ids(
                            [pointer.message_id for pointer in message_pointers]
                        )
                    }

                    for pointer in message_pointers:
                        yield SubscriptionMessage(
                            partition=pointer.partition,
                            position=pointer.position,
                            stored_message=stored_messages[pointer.message_id],
                        )
                        self._ack(
                            partition=pointer.partition,
                            position=pointer.position,
                        )
                break
            finally:
                self._lock_provider.unlock(lock_key)

    def _ack(self, partition: int, position: int):
        state = self._state_provider.read(self.name)
        assert (
            state.positions.get(partition, -1) == position - 1
        ), f"{partition} should have {position - 1} as last position, but has {state.positions.get(partition, -1)}"
        self._state_provider.store(
            subscription_name=self.name, partition=partition, position=position
        )


@_dc.dataclass
class _Handler:
    handler: Callable
    pass_subscription_message: bool
    pass_stored_message: bool


H = TypeVar("H", bound=Callable)


class SubscriptionHandler(Generic[E]):
    def __init__(
        self,
        subscription: Subscription[E],
        error_handler: Optional[SubscriptionErrorHandler] = None,
        call_middleware: Optional[CallMiddleware] = None,
    ):
        self._subscription = subscription
        self._handlers: Dict[Type[E], _Handler] = {}
        self._batch_size = 100
        self._error_handler = error_handler or ExitSubscriptionErrorHandler()
        self._call_middleware = call_middleware
        self._keep_running = True

    def register(self, handler: H) -> H:
        signature = _inspect.signature(handler)
        if len(signature.parameters) < 1:
            raise ValueError("Handler must have at least one parameter")
        if len(signature.parameters) > 1 and not self._call_middleware:
            raise ValueError(
                "If handler has more than one parameter, a call_middleware must be provided"
            )

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

    @property
    def notification_channel(self) -> str:
        return self._subscription._stream.notification_channel

    def run(self):
        assert (
            self._subscription.handler is self
        ), "A subscription can only have one handler"
        self.run_once()

    def stop(self):
        self._keep_running = False

    def run_once(self):
        while self._keep_running:
            n = 0
            for message in self._subscription.get_next_messages(count=self._batch_size):
                n += 1
                self.handle(message)
            if n == 0:
                break

    def handle(self, message: SubscriptionMessage):
        message_type = type(message.stored_message.message)
        for handled_type, handler in self._handlers.items():
            if issubclass_with_union(message_type, handled_type):
                try:
                    self._exec(
                        handler.handler, self._adapt_message_type(handler, message)
                    )
                except Exception as error:
                    error_handling_result = self._error_handler.handle_error(
                        error=error, message=message
                    )
                    if error_handling_result == SubscriptionErrorHandler.Action.EXIT:
                        raise
                return

    def _exec(
        self,
        handler: Callable[..., None],
        message: Union[SubscriptionMessage[E], StoredMessage[E], E],
    ) -> None:
        if self._call_middleware:
            self._call_middleware.call(handler, message)
        else:
            handler(message)

    def _adapt_message_type(
        self, handler: _Handler, message: SubscriptionMessage[E]
    ) -> Union[SubscriptionMessage[E], StoredMessage[E], E]:
        if handler.pass_subscription_message:
            return message
        elif handler.pass_stored_message:
            return message.stored_message
        else:
            return message.stored_message.message
