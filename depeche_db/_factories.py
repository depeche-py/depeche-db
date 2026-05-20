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
    SubscriptionStartPoint,
    SubscriptionStateProvider,
)

if TYPE_CHECKING:
    from ._aggregated_stream import AggregatedStream
    from ._message_store import MessageStore
    from ._subscription import AckStrategy, Subscription

E = TypeVar("E", bound=MessageProtocol)


class AggregatedStreamFactory(Generic[E]):
    def __init__(self, store: "MessageStore[E]"):
        """
        This factory is accessible on the message store:

            store = MessageStore(...)
            stream = store.aggregated_stream(
                name="stream_name",
                partitioner=...,
                stream_wildcards=["stream_%"]
            )
        """
        self._store = store

    def __call__(
        self,
        name: str,
        partitioner: "MessagePartitioner[E]",
        stream_wildcards: List[str],
        update_batch_size: Optional[int] = None,
    ) -> "AggregatedStream[E]":
        """
        Create an aggregated stream.

        Args:
            name: The name of the stream, needs to be a valid python identifier
            partitioner: A partitioner for the stream
            stream_wildcards: A list of stream wildcards
            update_batch_size: The batch size for updating the stream
        """
        from ._aggregated_stream import AggregatedStream

        return AggregatedStream(
            store=self._store,
            name=name,
            partitioner=partitioner,
            stream_wildcards=stream_wildcards,
            update_batch_size=update_batch_size,
        )


class SubscriptionFactory(Generic[E]):
    def __init__(self, stream: "AggregatedStream[E]"):
        """
        This factory is accessible on the aggregated stream:

            stream : AggregatedStream = ...
            subscription = stream.subscription(
                name="subscription_name",
                handlers=...,
                call_middleware=...,
                error_handler=...,
                state_provider=...,
                lock_provider=...,
            )
        """
        self._stream = stream

    def __call__(
        self,
        name: str,
        handlers: Optional[MessageHandlerRegisterProtocol[E]] = None,
        batch_size: Optional[int] = None,
        call_middleware: Optional[CallMiddleware] = None,
        error_handler: Optional[SubscriptionErrorHandler] = None,
        state_provider: Optional[SubscriptionStateProvider] = None,
        lock_provider: Optional[LockProvider] = None,
        start_point: Optional[SubscriptionStartPoint] = None,
        ack_strategy: Optional["AckStrategy"] = None,
    ) -> "Subscription[E]":
        """
        Create a subscription.

        Args:
            name: The name of the subscription, needs to be a valid python identifier
            handlers: Handlers to be called when a message is received, defaults to an empty register
            batch_size: Number of messages to read at once, defaults to 10, read more [here][depeche_db.SubscriptionRunner]
            call_middleware: A middleware to customize the call to the handlers
            error_handler: A handler for errors raised by the handlers, defaults to handler that will exit the subscription
            state_provider: Provider for the subscription state, defaults to a PostgreSQL provider
            lock_provider: Provider for the locks, defaults to a PostgreSQL provider
            start_point: The start point for the subscription, defaults to beginning of the stream
            ack_strategy: Strategy for acknowledging messages, defaults to AckStrategy.SINGLE
        """
        from ._message_handler import MessageHandlerRegister
        from ._subscription import AckStrategy, Subscription, SubscriptionMessageHandler

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
            start_point=start_point,
            batch_size=batch_size,
            ack_strategy=ack_strategy or AckStrategy.SINGLE,
        )
