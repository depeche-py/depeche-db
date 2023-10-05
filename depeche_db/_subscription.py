import contextlib as _contextlib
import dataclasses as _dc
from typing import Callable, Generic, Iterator, TypeVar

from ._interfaces import (
    LockProvider,
    MessageProtocol,
    StoredMessage,
    SubscriptionStateProvider,
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
        # TODO just name?!
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


@_dc.dataclass
class _Handler:
    handler: Callable
    pass_subscription_message: bool
    pass_stored_message: bool

    def exec(self, message: SubscriptionMessage):
        if self.pass_subscription_message:
            self.handler(message)
        elif self.pass_stored_message:
            self.handler(message.stored_message)
        else:
            self.handler(message.stored_message.message)


class SubscriptionHandler(Generic[E]):
    def __init__(self, subscription: Subscription[E]):
        self._subscription = subscription
        self._handlers = {}

    @property
    def notification_channel(self) -> str:
        return self._subscription._stream.notification_channel

    def run(self):
        self.run_once()

    def register(self, handler):  # TODO type
        assert len(handler.__annotations__) == 1
        pass_subscription_message = False
        pass_stored_message = False
        handled_type = list(handler.__annotations__.values())[0]
        if str(handled_type).startswith(
            "depeche_db._subscription.SubscriptionMessage["
        ):
            pass_subscription_message = True
            handled_type = handled_type.__args__[0]
        if str(handled_type).startswith("depeche_db._interfaces.StoredMessage["):
            pass_stored_message = True
            handled_type = handled_type.__args__[0]
        # TODO assert no overlap of handled types
        self._handlers[handled_type] = _Handler(
            handler=handler,
            pass_subscription_message=pass_subscription_message,
            pass_stored_message=pass_stored_message,
        )
        return handler

    def handle(self, message: SubscriptionMessage):
        message_type = type(message.stored_message.message)
        for handled_type, handler in self._handlers.items():
            if issubclass(message_type, handled_type):
                try:
                    handler.exec(message)
                except Exception:
                    # TODO error handler!
                    pass
                return

    def run_once(self):
        while True:
            with self._subscription.get_next_message() as message:
                if message is None:
                    break
                self.handle(message)
                message.ack()
