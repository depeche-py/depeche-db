import dataclasses as _dc
import datetime as _dt
import enum as _enum
import uuid as _uuid
from typing import Callable, Dict, Generic, Protocol, TypeVar, Union


class MessageProtocol:
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

        Args:
            handler: Handler
            message: Message to be passed to the handler
        """
        raise NotImplementedError


class RunOnNotification(Protocol):
    """
    Run on notification is a protocol that allows objects to be run when a
    notification is received on a channel. Objects that implement this protocol
    can be registered with a [Executor][depeche_db.Executor] object.

    Implemented by:

    - [SubscriptionHandler][depeche_db.SubscriptionHandler]
    - [StreamProjector][depeche_db.StreamProjector]
    """

    @property
    def notification_channel(self) -> str:
        """
        Returns notification channel name.
        """
        raise NotImplementedError

    def run(self):
        """
        Runs the object. This method needs to return when a chunk of work has been
        done.
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
