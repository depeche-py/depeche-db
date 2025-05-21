from typing import AsyncGenerator, Generator, Generic, Iterable, Optional, TypeVar

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
        """
        Start the notification listener.

        This method should be called before calling get_messages.

        The notification listener will listen for notifications on the
        notification channel of the stream. In order to do so, it creates a
        a connection to the database and listens for notifications on a separate
        thread. The connection is closed when the notification listener is stopped.
        """
        self.notification_listener.start()

    def get_messages(self, timeout: int = 0) -> Generator[StoredMessage[E], None, None]:
        """
        On the first call, get all messages in the stream after the start_point.
        On subsequent calls, get all messages in the stream after the last returned message.

        Args:
            timeout (int): Only wait for this many seconds when there are no new messages.
        """
        # Get messages already in the stream (after start_point)
        yield from _exhaust_subscription(self.subscription)

        # Get messages every time a notification is received
        for _ in self.notification_listener.messages(timeout=timeout):
            yield from _exhaust_subscription(self.subscription)

    def stop(self):
        """
        Stop the notification listener.
        """
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
        """
        Start the notification listener.
        """
        await self.notification_listener.start()

    async def get_messages(
        self, timeout: int = 0
    ) -> AsyncGenerator[StoredMessage[E], None]:
        """
        On the first call, get all messages in the stream after the start_point.
        On subsequent calls, get all messages in the stream after the last returned message.

        Args:
            timeout (int): Only wait for this many seconds when there are no new messages.
        """
        async_exhaust = _asyncer.asyncify(_exhaust_subscription)

        # Get messages already in the stream (after start_point)
        for message in await async_exhaust(self.subscription):
            yield message

        # Get messages every time a notification is received
        async for _ in self.notification_listener.messages(timeout=timeout):
            for message in await async_exhaust(self.subscription):
                yield message

    async def stop(self):
        """
        Stop the notification listener.
        """
        await self.notification_listener.stop()


def _exhaust_subscription(subscription: Subscription[E]) -> Iterable[StoredMessage[E]]:
    had_new_messages = True
    while had_new_messages:
        had_new_messages = False
        for message in subscription.get_next_messages(count=100):
            had_new_messages = True
            yield message.stored_message
