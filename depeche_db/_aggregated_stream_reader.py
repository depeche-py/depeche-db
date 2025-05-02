from typing import AsyncGenerator, Generic, Iterable, Optional, TypeVar

import asyncer as _asyncer

from ._aggregated_stream import AggregatedStream
from ._interfaces import MessageProtocol, StoredMessage, SubscriptionStartPoint
from ._subscription import Subscription
from .tools import immem_subscription as _inmem_subscription
from .tools.async_pg_notification_listener import AsyncPgNotificationListener
from .tools.pg_notification_listener import PgNotificationListener

E = TypeVar("E", bound=MessageProtocol)


class AggregatedStreamReader(Generic[E]):
    def __init__(
        self,
        stream: AggregatedStream[E],
        start_point: Optional[SubscriptionStartPoint] = None,
    ):
        self.stream = stream
        self.subscription = stream.subscription(
            name=f"{stream.name}_reader",
            lock_provider=_inmem_subscription.NullLockProvider(),
            state_provider=_inmem_subscription.InMemorySubscriptionState(),
            start_point=start_point,
        )
        self.notification_listener = PgNotificationListener(
            dsn=stream._store.engine.url.render_as_string(hide_password=False),
            channels=[stream.notification_channel],
            select_timeout=0.5,
        )

    def start(self):
        self.notification_listener.start()

    def get_messages(self, timeout: int = 0) -> Iterable[StoredMessage[E]]:
        # Get messages already in the stream (after start_point)
        yield from _exhaust_subscription(self.subscription)

        # Get messages every time a notification is received
        for _ in self.notification_listener.messages(timeout=timeout):
            yield from _exhaust_subscription(self.subscription)

    def stop(self):
        self.notification_listener.stop()


class AsyncAggregatedStreamReader(Generic[E]):
    def __init__(
        self,
        stream: AggregatedStream[E],
        start_point: Optional[SubscriptionStartPoint] = None,
    ):
        self.stream = stream
        self.subscription = stream.subscription(
            name=f"{stream.name}_reader",
            lock_provider=_inmem_subscription.NullLockProvider(),
            state_provider=_inmem_subscription.InMemorySubscriptionState(),
            start_point=start_point,
        )
        self.notification_listener = AsyncPgNotificationListener(
            dsn=stream._store.engine.url.render_as_string(hide_password=False),
            channels=[stream.notification_channel],
            select_timeout=0.5,
        )

    async def start(self):
        await self.notification_listener.start()

    async def get_messages(
        self, timeout: int = 0
    ) -> AsyncGenerator[StoredMessage[E], None]:
        async_exhaust = _asyncer.asyncify(_exhaust_subscription)

        # Get messages already in the stream (after start_point)
        for message in await async_exhaust(self.subscription):
            yield message

        # Get messages every time a notification is received
        async for _ in self.notification_listener.messages(timeout=timeout):
            for message in await async_exhaust(self.subscription):
                yield message

    async def stop(self):
        await self.notification_listener.stop()


def _exhaust_subscription(subscription: Subscription[E]) -> Iterable[StoredMessage[E]]:
    had_new_messages = True
    while had_new_messages:
        had_new_messages = False
        for message in subscription.get_next_messages(count=100):
            had_new_messages = True
            yield message.stored_message
