import dataclasses as _dc
import datetime as _dt
import enum as _enum
import functools as _ft
import time as _time
import uuid as _uuid
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Generic,
    Iterator,
    Optional,
    Protocol,
    Type,
    TypeVar,
    Union,
    no_type_check,
    runtime_checkable,
)

if TYPE_CHECKING:
    from ._aggregated_stream import AggregatedStream


@runtime_checkable
class MessageProtocol(Protocol):
    """
    Message protocol is a base class for all messages that are used in the system.
    """

    def get_message_id(self) -> _uuid.UUID:
        """
        Returns message ID
        """
        ...

    def get_message_time(self) -> _dt.datetime:
        """
        Returns message time
        """
        ...


E = TypeVar("E", bound=MessageProtocol)
M = TypeVar("M")


@_dc.dataclass(frozen=True)
class StoredMessage(Generic[E]):
    """
    Stored message is a message that is stored in the stream.

    Attributes:
        message_id: Message ID
        stream: Stream name
        version: Message version
        message: Message (`E` subtype of `MessageProtocol`)
        global_position: Global position
    """

    message_id: _uuid.UUID
    stream: str
    version: int
    message: E
    global_position: int


class AckOpProtocol(Protocol):
    def execute(self, **kwargs):
        ...


@_dc.dataclass(frozen=True)
class SubscriptionMessage(Generic[E]):
    """
    Subscription message is a message that is received from the subscription.

    Attributes:
        partition: Partition number
        position: Position in the partition
        stored_message: Stored message (`E` subtype of `MessageProtocol`)
    """

    partition: int
    position: int
    stored_message: StoredMessage[E]
    ack: AckOpProtocol


@_dc.dataclass()
class SubscriptionMessageBatch(Generic[E]):
    """

    Attributes:
    """

    partition: int
    first_position: int
    last_position: int
    lock_key: str
    messages: list[SubscriptionMessage[E]]
    ackd_position: int = -1

    @_ft.cached_property
    def message_ids(self) -> set[_uuid.UUID]:
        return {message.stored_message.message_id for message in self.messages}

    def ack(self, message: SubscriptionMessage[E]):
        """
        Acknowledges a message. This method is used to acknowledge messages in a batch.
        """
        if message.stored_message.message_id in self.message_ids:
            if self.ackd_position == -1:
                self.ackd_position = message.position
            else:
                assert (
                    self.ackd_position + 1 == message.position
                ), "You cannot leave gaps in the acknowledged messages"
                self.ackd_position = message.position
        else:
            raise ValueError("Message not found in the batch")


@_dc.dataclass(frozen=True)
class MessagePosition:
    """
    Message position is a position of the message in the stream.

    Attributes:
        stream: Stream name
        version: Message version
        global_position: Global position
    """

    stream: str
    version: int
    global_position: int


@_dc.dataclass
class StreamPartitionStatistic:
    partition_number: int
    next_message_id: _uuid.UUID
    next_message_position: int
    next_message_occurred_at: _dt.datetime
    max_position: int


@_dc.dataclass
class AggregatedStreamMessage:
    """
    Aggregated stream message is a message that can be read from an aggregated stream.

    Attributes:
        message_id: Message ID
        partition: Partition number
        position: Position in the partition
    """

    partition: int
    position: int
    message_id: _uuid.UUID


@_dc.dataclass
class SubscriptionState:
    """
    Subscription state is a state of the subscription.

    Attributes:
        positions: Mapping of partition number to the position in the partition
    """

    positions: Dict[int, int]


class MessageSerializer(Protocol, Generic[M]):
    """
    Message serializer is a protocol that is used to serialize and deserialize messages.

    The following must be true for any serializer:

    - `deserialize(serialize(message)) == message`
    - `type(deserialize(serialize(message))) is type(message)`
    - `serialize(deserialize(data)) == data`
    """

    def serialize(self, message: M) -> dict:
        """
        Serializes message to a dictionary. The dictionary must be JSON serializable.
        """
        raise NotImplementedError()

    def deserialize(self, message: dict) -> M:
        """
        Deserializes message from a dictionary.
        """
        raise NotImplementedError()


class MessagePartitioner(Protocol, Generic[E]):
    """
    Message partitioner is a protocol that is used to determine partition number for a message.
    """

    def get_partition(self, message: StoredMessage[E]) -> int:
        """
        Returns partition number for a message. The partition number must be a
        positive integer. The partition number must be deterministic for a given message.
        """
        raise NotImplementedError


class LockProvider(Protocol):
    """
    Lock provider is a protocol that is used to lock and unlock resources.
    """

    def lock(self, name: str) -> bool:
        """
        Locks resource with a given name. Returns `True` if the resource was locked.
        This method must not block!
        """
        raise NotImplementedError

    def unlock(self, name: str):
        """
        Unlocks resource with a given name.
        """
        raise NotImplementedError


class SubscriptionStateProvider(Protocol):
    """
    Subscription state provider is a protocol that is used to store and read subscription state.
    """

    def store(self, subscription_name: str, partition: int, position: int):
        """
        Stores subscription state for a given partition.
        """
        raise NotImplementedError

    def read(self, subscription_name: str) -> SubscriptionState:
        """
        Reads subscription state.

        Returns:
            Subscription state
        """
        raise NotImplementedError

    def initialize(self, subscription_name: str):
        """
        Marks subscription state as initialized.
        """
        raise NotImplementedError

    def initialized(self, subscription_name: str) -> bool:
        """
        Returns `True` if the subscription state was already initialized.
        """
        raise NotImplementedError

    def session(self, **kwargs) -> "SubscriptionStateProvider":
        """
        Returns a session for the subscription state provider.

        This can be used to run the state reads/updates in a transaction
        provided by the client.

        See [DbSubscriptionStateProvider][depeche_db.tools.DbSubscriptionStateProvider]
        for an example implementation.
        """
        raise NotImplementedError


class CallMiddleware(Generic[E]):
    """
    Call middleware is a protocol that is used to wrap a call to a handler.

    Typical implementation:

        class MyCallMiddleware(CallMiddleware):
            def __init__(self, some_dependency):
                self.some_dependency = some_dependency

            def call(self, handler, message):
                # or use a DI container here
                handler(message, some_dependency=self.some_dependency)
    """

    def call(
        self,
        handler: Callable,
        message: Union[SubscriptionMessage[E], StoredMessage[E], E],
    ):
        """
        Calls a handler with a given message.

        The type of the message depends on the type annotation of the handler function.
        See [MessageHandlerRegister][depeche_db.MessageHandlerRegister] for more details.

        Args:
            handler: Handler
            message: Message to be passed to the handler
        """
        raise NotImplementedError


class TimeBudget(Protocol):
    def over_budget(self) -> bool:
        """
        Returns `True` if the budget is over.
        """
        raise NotImplementedError


class FixedTimeBudget:
    def __init__(self, seconds: float):
        self.seconds = seconds
        self.start_time = _time.time()

    def over_budget(self) -> bool:
        """
        Returns `True` if the budget is over.
        """
        return _time.time() - self.start_time > self.seconds


class RunOnNotificationResult(_enum.Enum):
    WORK_REMAINING = "work_remaining"
    DONE_FOR_NOW = "done_for_now"


class RunOnNotification(Protocol):
    """
    Run on notification is a protocol that allows objects to be run when a
    notification is received on a channel. Objects that implement this protocol
    can be registered with a [Executor][depeche_db.Executor] object.

    Implemented by:
        - [SubscriptionRunner][depeche_db.SubscriptionRunner]
        - [StreamProjector][depeche_db.StreamProjector]
    """

    @property
    def notification_channel(self) -> str:
        """
        Returns notification channel name.
        """
        raise NotImplementedError

    def run(self, budget: TimeBudget) -> Optional[RunOnNotificationResult]:
        """
        Runs the object. This method needs to return when a chunk of work has been
        done.
        It needs to return within reasonable time after the given time budget is over.

        Returns:
            WORK_REMAINING if there is still work to be done
            DONE_FOR_NOW if there is no work to be done
            None will be interpreted as DONE_FOR_NOW (backwards compatibility)
        """
        raise NotImplementedError

    def stop(self):
        """
        If the object's `run` method has a loop, this method can be used to
        exit the loop earlier.
        Will be called in a separate thread.
        """
        raise NotImplementedError


class ErrorAction(_enum.Enum):
    """
    Error action is an action that is taken when an error occurs during message processing.

    Attributes:
        IGNORE: Ignore the error and continue processing.
        EXIT: Exit processing.
    """

    IGNORE = "ignore"
    EXIT = "exit"


class SubscriptionErrorHandler(Generic[E]):
    """
    Subscription error handler is a protocol that is used to handle errors that occur.
    """

    def handle_error(
        self, error: Exception, message: SubscriptionMessage[E]
    ) -> ErrorAction:
        """
        Handles an error that occurred during message processing.

        Args:
            error: Error
            message: Message that was being processed when the error occurred

        Returns:
            Action to be taken
        """
        raise NotImplementedError


class SubscriptionStartPoint:
    """
    Defines the start point of a subscription.
    """

    def init_state(
        self,
        subscription_name: str,
        stream: "AggregatedStream",
        state_provider: SubscriptionStateProvider,
    ):
        """
        Initializes subscription state (if not yet initialized).
        """
        raise NotImplementedError


@_dc.dataclass
class HandlerDescriptor(Generic[E]):
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


class MessageHandlerRegisterProtocol(Protocol, Generic[E]):
    """
    Message handler register protocol is used by runners to find handlers for messages.

    Implemented by:
        - [MessageHandlerRegister][depeche_db.MessageHandlerRegister]
        - [MessageHandler][depeche_db.MessageHandler]
    """

    def get_all_handlers(self) -> Iterator[HandlerDescriptor[E]]:
        """
        Returns all registered handlers.
        """
        raise NotImplementedError

    def get_handler(self, message_type: Type[E]) -> Optional[HandlerDescriptor[E]]:
        """
        Returns a handler for a given message type.
        """
        raise NotImplementedError
