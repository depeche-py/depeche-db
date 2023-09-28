from psycopg2.errors import LockNotAvailable
import contextlib as _contextlib
import dataclasses as _dc
import datetime as _dt
import uuid as _uuid
from typing import Generic, Iterable, Iterator, Protocol, TypeVar

import sqlalchemy as _sa
import sqlalchemy.orm as _orm
from sqlalchemy_utils import UUIDType as _UUIDType

from ._interfaces import (
    MessageProtocol,
    SubscriptionStateProvider,
    LockProvider,
    StoredMessage,
)
from ._link_stream import LinkStream

E = TypeVar("E", bound=MessageProtocol)


@_dc.dataclass(frozen=True)
class SubscriptionMessage(Generic[E]):
    partition: int
    position: int
    stored_message: StoredMessage[E]
    _subscription: "Subscription[E]"

    def ack(self):
        self._subscription.ack_message(self)


class Subscription(Generic[E]):
    def __init__(
        self,
        group_name: str,
        stream: LinkStream[E],
        state_provider: SubscriptionStateProvider,
        lock_provider: LockProvider,
        # TODO start at time
        # TODO start at "next message"
    ):
        assert group_name.isidentifier(), "Group name must be a valid identifier"
        self.group_name = group_name
        self._stream = stream
        self._lock_provider = lock_provider
        self._state_provider = state_provider

    @_contextlib.contextmanager
    def get_next_message(self) -> Iterator[SubscriptionMessage[E]]:
        # TODO get more than one message (accept a batch size parameter)
        state = self._state_provider.read(self.group_name)
        statistics = list(
            self._stream.get_partition_statistics(
                position_limits=state.positions, result_limit=10
            )
        )
        for statistic in statistics:
            lock_key = f"subscription-{self.group_name}-{statistic.partition_number}"
            if not self._lock_provider.lock(lock_key):
                continue
            # now we have the lock, we need to check if the position is still valid
            # if not, we need to release the lock and try the next partition
            state = self._state_provider.read(self.group_name)
            if state.positions.get(statistic.partition_number, -1) != (
                statistic.next_message_position - 1
            ):
                self._lock_provider.unlock(lock_key)
                continue
            try:
                with self._stream._store.reader() as reader:
                    yield SubscriptionMessage(
                        partition=statistic.partition_number,
                        position=statistic.next_message_position,
                        stored_message=reader.get_message_by_id(
                            statistic.next_message_id
                        ),
                        _subscription=self,
                    )
                break
            finally:
                self._lock_provider.unlock(lock_key)
        else:
            yield None

    def ack(self, partition: int, position: int):
        state = self._state_provider.read(self.group_name)
        assert (
            state.positions.get(partition, -1) == position - 1
        ), f"{partition} should have {position - 1} as last position, but has {state.positions.get(partition, -1)}"
        self._state_provider.store(
            group_name=self.group_name, partition=partition, position=position
        )

    def ack_message(self, message: SubscriptionMessage[E]):
        self.ack(partition=message.partition, position=message.position)
