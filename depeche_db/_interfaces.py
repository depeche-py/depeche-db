import dataclasses as _dc
import datetime as _dt
import uuid as _uuid
from typing import Generic, Protocol, TypeVar


class MessageProtocol:
    def get_message_id(self) -> _uuid.UUID:
        ...

    def get_message_time(self) -> _dt.datetime:
        ...


E = TypeVar("E", bound=MessageProtocol)


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
class SubscriptionState:
    positions: dict[int, int]


class MessageSerializer(Protocol, Generic[E]):
    def serialize(self, message: E) -> dict:
        raise NotImplementedError()

    def deserialize(self, message: dict) -> E:
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
    def store(self, group_name: str, partition: int, position: int):
        raise NotImplementedError

    def read(self, group_name: str) -> SubscriptionState:
        raise NotImplementedError
