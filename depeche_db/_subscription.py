import logging as _logging
from typing import (
    Callable,
    Generic,
    Iterator,
    Optional,
    TypeVar,
    Union,
)

from . import tools as _tools
from ._aggregated_stream import AggregatedStream
from ._interfaces import (
    CallMiddleware,
    ErrorAction,
    LockProvider,
    MessageHandlerRegisterProtocol,
    MessageProtocol,
    StoredMessage,
    SubscriptionErrorHandler,
    SubscriptionMessage,
    SubscriptionStateProvider,
)

E = TypeVar("E", bound=MessageProtocol)

DEPECHE_LOGGER = _logging.getLogger("depeche_db")


class ExitSubscriptionErrorHandler(SubscriptionErrorHandler):
    def handle_error(
        self, error: Exception, message: SubscriptionMessage[E]
    ) -> ErrorAction:
        return ErrorAction.EXIT


class LogAndIgnoreSubscriptionErrorHandler(SubscriptionErrorHandler):
    def __init__(self, subscription_name: str):
        self._logger = _logging.getLogger(
            f"depeche_db.subscription.{subscription_name}"
        )

    def handle_error(
        self, error: Exception, message: SubscriptionMessage[E]
    ) -> ErrorAction:
        self._logger.exception(
            "Error while handling message {message.stored_message.message_id}:{message.stored_message.message.__class__.__name__}"
        )
        return ErrorAction.IGNORE


class Subscription(Generic[E]):
    def __init__(
        self,
        name: str,
        stream: AggregatedStream[E],
        state_provider: Optional[SubscriptionStateProvider] = None,
        lock_provider: Optional[LockProvider] = None,
        # TODO start at time
        # TODO start at "next message"
    ):
        """
        A subscription is a way to read messages from an aggregated stream.

        Args:
            name: Name of the subscription
            stream: Stream to read from
            state_provider: Provider for the subscription state
            lock_provider: Provider for the locks
        """
        assert name.isidentifier(), "Group name must be a valid identifier"
        self.name = name
        self._stream = stream
        self._lock_provider = lock_provider or _tools.DbLockProvider(
            name, self._stream._store.engine
        )
        self._state_provider = state_provider or _tools.DbSubscriptionStateProvider(
            name, self._stream._store.engine
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


class SubscriptionMessageHandler(Generic[E]):
    def __init__(
        self,
        handler_register: MessageHandlerRegisterProtocol[E],
        error_handler: Optional[SubscriptionErrorHandler] = None,
        call_middleware: Optional[CallMiddleware] = None,
    ):
        """
        Handles messages

        Args:
            handler_register: The handler register to use
            error_handler: The error handler to use
            call_middleware: The middleware to call before calling the handler

        """
        self._register = handler_register
        self._error_handler = error_handler or ExitSubscriptionErrorHandler()
        self._call_middleware = call_middleware

        if not self._call_middleware and any(
            handler.requires_middleware for handler in self._register.get_all_handlers()
        ):
            raise ValueError(
                "If handler has more than one parameter, a call_middleware must be provided"
            )

    def handle(self, message: SubscriptionMessage):
        handler = self._register.get_handler(type(message.stored_message.message))
        if handler:
            try:
                self._exec(handler.handler, handler.adapt_message_type(message))
            except Exception as error:
                error_handling_result = self._error_handler.handle_error(
                    error=error, message=message
                )
                if error_handling_result == ErrorAction.EXIT:
                    raise
        # TODO else raise error?

    def _exec(
        self,
        handler: Callable[..., None],
        message: Union[SubscriptionMessage[E], StoredMessage[E], E],
    ) -> None:
        if self._call_middleware:
            self._call_middleware.call(handler, message)
        else:
            handler(message)


class SubscriptionRunner(Generic[E]):
    @classmethod
    def create(
        cls,
        subscription: Subscription[E],
        handlers: MessageHandlerRegisterProtocol[E],
        call_middleware: Optional[CallMiddleware[E]] = None,
        error_handler: Optional[SubscriptionErrorHandler[E]] = None,
    ) -> "SubscriptionRunner[E]":
        return cls(
            subscription=subscription,
            handler=SubscriptionMessageHandler(
                handler_register=handlers,
                call_middleware=call_middleware,
                error_handler=error_handler,
            ),
        )

    def __init__(
        self,
        subscription: Subscription[E],
        handler: SubscriptionMessageHandler,
    ):
        """
        Handles messages from a subscription using a handler

        Implements: [RunOnNotification][depeche_db.RunOnNotification]

        Args:
            subscription: The subscription to handle
            handler: The handler to use
        """
        self._subscription = subscription
        self._batch_size = 100
        self._keep_running = True
        self._handler = handler

    @property
    def notification_channel(self) -> str:
        return self._subscription._stream.notification_channel

    def run(self):
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
        self._handler.handle(message)
