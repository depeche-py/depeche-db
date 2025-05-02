from typing import Generic, Iterable, Optional, TypeVar

from depeche_db.tools.pg_notification_listener import PgNotificationListener

from ._aggregated_stream import AggregatedStream
from ._interfaces import MessageProtocol, StoredMessage, SubscriptionStartPoint
from .tools import immem_subscription as _inmem_subscription

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
            dsn=stream._store.engine.url,
            channels=[stream.notification_channel],
            select_timeout=0.5,
        )

    def start(self):
        self.notification_listener.start()

    def get_messages(self, timeout: int = 0) -> Iterable[StoredMessage[E]]:
        # Get messages already in the stream (after start_point)
        yield from self._exhaust_subscription()

        # Get messages every time a notification is received
        for _ in self.notification_listener.messages(timeout=timeout):
            yield from self._exhaust_subscription()

    def _exhaust_subscription(self) -> Iterable[StoredMessage[E]]:
        had_new_messages = True
        while had_new_messages:
            had_new_messages = False
            for message in self.subscription.get_next_messages(count=100):
                had_new_messages = True
                yield message.stored_message

    def stop(self):
        self.notification_listener.stop()
