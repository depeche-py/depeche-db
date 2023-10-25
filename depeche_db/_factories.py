from typing import (
    TYPE_CHECKING,
    Generic,
    List,
    Optional,
    TypeVar,
)

from ._interfaces import (
    CallMiddleware,
    LockProvider,
    MessageHandlerRegisterProtocol,
    MessagePartitioner,
    MessageProtocol,
    SubscriptionErrorHandler,
    SubscriptionStateProvider,
)

if TYPE_CHECKING:
    from ._aggregated_stream import AggregatedStream
    from ._message_store import MessageStore
    from ._subscription import Subscription

E = TypeVar("E", bound=MessageProtocol)


class AggregatedStreamFactory(Generic[E]):
    def __init__(self, store: "MessageStore[E]"):
        self._store = store

    def __call__(
        self,
        name: str,
        partitioner: "MessagePartitioner[E]",
        stream_wildcards: List[str],
    ) -> "AggregatedStream[E]":
        """
        Create an aggregated stream.

        Args:
            name: The name of the stream
            partitioner: A partitioner for the stream
            stream_wildcards: A list of stream wildcards to be aggregated
        """
        from ._aggregated_stream import AggregatedStream

        return AggregatedStream(
            store=self._store,
            name=name,
            partitioner=partitioner,
            stream_wildcards=stream_wildcards,
        )


class SubscriptionFactory(Generic[E]):
    def __init__(self, stream: "AggregatedStream[E]"):
        self._stream = stream

    def __call__(
        self,
        name: str,
        handlers: MessageHandlerRegisterProtocol[E] = None,
        call_middleware: Optional[CallMiddleware] = None,
        error_handler: Optional[SubscriptionErrorHandler] = None,
        state_provider: Optional[SubscriptionStateProvider] = None,
        lock_provider: Optional[LockProvider] = None,
    ) -> "Subscription[E]":
        """
        Create a subscription.

        Args:
            name: The name of the subscription
            handlers: Handlers to be called when a message is received
            call_middleware: A middleware to be called before the handlers
            error_handler: A handler for errors raised by the handlers
            state_provider: A provider for the subscription state
            lock_provider: A provider for the subscription locks
        """
        from ._message_handler import MessageHandlerRegister
        from ._subscription import Subscription, SubscriptionMessageHandler

        if handlers is None:
            # allow constructing a subscription without handlers
            handlers = MessageHandlerRegister()

        return Subscription(
            name=name,
            stream=self._stream,
            message_handler=SubscriptionMessageHandler(
                handler_register=handlers,
                call_middleware=call_middleware,
                error_handler=error_handler,
            ),
            state_provider=state_provider,
            lock_provider=lock_provider,
        )
