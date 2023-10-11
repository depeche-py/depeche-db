import collections as _collections
import signal as _signal
from typing import Callable, Dict, List

from ._interfaces import RunOnNotification
from .tools import PgNotificationListener


class Executor:
    def __init__(self, db_dsn: str):
        self._db_dsn = db_dsn
        self.channel_register: Dict[
            str, List[Callable[[], None]]
        ] = _collections.defaultdict(list)

    def register(self, handler: RunOnNotification):
        self.channel_register[handler.notification_channel].append(handler.run)
        return handler

    def run(self):
        listener = PgNotificationListener(
            dsn=self._db_dsn,
            channels=list(self.channel_register),
            ignore_payload=True,
        )

        def stop():
            print("Stopping...")
            listener.stop()

        _signal.signal(_signal.SIGINT, lambda *_: stop())
        _signal.signal(_signal.SIGTERM, lambda *_: stop())

        listener.start()
        print("Started")
        for notification in listener.messages():
            for handler in self.channel_register[notification.channel]:
                handler()
