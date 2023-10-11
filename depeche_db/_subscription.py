import dataclasses as _dc
import types as _types
import typing as _typing
from typing import Callable, Generic, Iterator, Type, TypeVar

from ._aggregated_stream import AggregatedStream
from ._interfaces import (
    LockProvider,
    MessageProtocol,
    StoredMessage,
    SubscriptionStateProvider,
)

E = TypeVar("E", bound=MessageProtocol)


@_dc.dataclass(frozen=True)
class SubscriptionMessage(Generic[E]):
    partition: int
    position: int
    stored_message: StoredMessage[E]


class Subscription(Generic[E]):
    def __init__(
        self,
        name: str,
        stream: AggregatedStream[E],
        state_provider: SubscriptionStateProvider,
        lock_provider: LockProvider,
        # TODO start at time
        # TODO start at "next message"
    ):
        assert name.isidentifier(), "Group name must be a valid identifier"
        self.name = name
        self._stream = stream
        self._lock_provider = lock_provider
        self._state_provider = state_provider
        self.handler = SubscriptionHandler(self)

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


HandlerCallable = (
    Callable[[E], None]
    | Callable[[StoredMessage[E]], None]
    | Callable[[SubscriptionMessage[E]], None]
)


@_dc.dataclass
class _Handler:
    handler: HandlerCallable
    pass_subscription_message: bool
    pass_stored_message: bool

    def exec(self, message: SubscriptionMessage):
        if self.pass_subscription_message:
            self.handler(message)  # type: ignore
        elif self.pass_stored_message:
            self.handler(message.stored_message)  # type: ignore
        else:
            self.handler(message.stored_message.message)


H = TypeVar("H", bound=HandlerCallable)


class SubscriptionHandler(Generic[E]):
    def __init__(self, subscription: Subscription[E]):
        self._subscription = subscription
        self._handlers: dict[Type[E], _Handler] = {}
        self._batch_size = 1

    @property
    def notification_channel(self) -> str:
        return self._subscription._stream.notification_channel

    def run(self):
        assert (
            self._subscription.handler is self
        ), "A subscription can only have one handler"
        self.run_once()

    def register(self, handler: H) -> H:
        if len(handler.__annotations__) != 1:
            raise ValueError(
                "Handler must accept exactly one (type annotated) argument"
            )

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

        self.assert_not_registered(handled_type)
        self._handlers[handled_type] = _Handler(
            handler=handler,
            pass_subscription_message=pass_subscription_message,
            pass_stored_message=pass_stored_message,
        )
        return handler

    def assert_not_registered(self, handled_type: Type[E]):
        if _typing.get_origin(handled_type) in (_typing.Union, _types.UnionType):
            for member in _typing.get_args(handled_type):
                self.assert_not_registered(member)
        else:
            for registered_type in self._handlers:
                if issubclass(handled_type, registered_type):
                    raise ValueError(
                        f"Handler for {handled_type} is already registered for {registered_type}"
                    )

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
            n = 0
            for message in self._subscription.get_next_messages(count=self._batch_size):
                n += 1
                self.handle(message)
            if n == 0:
                break
