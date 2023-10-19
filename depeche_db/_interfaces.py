import dataclasses as _dc
import datetime as _dt
import uuid as _uuid
from typing import Dict, Generic, Protocol, TypeVar


class MessageProtocol:
    def get_message_id(self) -> _uuid.UUID:
        ...

    def get_message_time(self) -> _dt.datetime:
        ...


E = TypeVar("E", bound=MessageProtocol)
M = TypeVar("M")


@_dc.dataclass(frozen=True)
class StoredMessage(Generic[E]):
    message_id: _uuid.UUID
    stream: str
    version: int
    message: E
    global_position: int


@_dc.dataclass(frozen=True)
class MessagePosition:
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
    partition: int
    position: int
    message_id: _uuid.UUID


@_dc.dataclass
class SubscriptionState:
    positions: Dict[int, int]


class MessageSerializer(Protocol, Generic[M]):
    def serialize(self, message: M) -> dict:
        raise NotImplementedError()

    def deserialize(self, message: dict) -> M:
        raise NotImplementedError()


class MessagePartitioner(Protocol, Generic[E]):
    def get_partition(self, message: StoredMessage[E]) -> int:
        raise NotImplementedError


class LockProvider(Protocol):
    def lock(self, name: str) -> bool:
        raise NotImplementedError

    def unlock(self, name: str):
        raise NotImplementedError


class SubscriptionStateProvider(Protocol):
    def store(self, subscription_name: str, partition: int, position: int):
        raise NotImplementedError

    def read(self, subscription_name: str) -> SubscriptionState:
        raise NotImplementedError


class RunOnNotification(Protocol):
    @property
    def notification_channel(self) -> str:
        raise NotImplementedError

    def run(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError
