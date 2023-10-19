import collections as _collections
import signal as _signal
import threading as _threading
import time as _time
from typing import Callable, Dict, List, Optional

from ._interfaces import RunOnNotification
from .tools import PgNotificationListener


class Executor:
    listener: Optional[PgNotificationListener] = None

    def __init__(self, db_dsn: str):
        self._db_dsn = db_dsn
        self.channel_register: Dict[
            str, List[RunOnNotification]
        ] = _collections.defaultdict(list)
        self.stimulation_interval = 0.5
        self.keep_running = True
        self.handler_queue = HandlerQueue()
        self.stimulator_thread = _threading.Thread(target=self.stimulate, daemon=True)
        self.handler_thread = _threading.Thread(target=self.run_handlers, daemon=True)
        self.listener = None
        _signal.signal(_signal.SIGINT, lambda *_: self.stop())
        _signal.signal(_signal.SIGTERM, lambda *_: self.stop())

    def register(self, handler: RunOnNotification):
        self.channel_register[handler.notification_channel].append(handler)
        return handler

    def stop(self):
        self.keep_running = False
        if self.listener is not None:
            self.listener.stop()

        for handlers in self.channel_register.values():
            for handler in handlers:
                handler.stop()

    def run(self):
        self.listener = PgNotificationListener(
            dsn=self._db_dsn,
            channels=list(self.channel_register),
            ignore_payload=True,
        )

        self.handler_thread.start()
        self.stimulator_thread.start()
        self.listener.start()

        for notification in self.listener.messages():
            for handler in self.channel_register[notification.channel]:
                self.handler_queue.put(handler.run)

        self.handler_thread.join()
        self.stimulator_thread.join()

    def run_handlers(self):
        while self.keep_running:
            if self.handler_queue:
                handler = self.handler_queue.get()
                try:
                    handler()
                except Exception:
                    self.stop()
                    raise
            else:
                self.handler_queue.wait()

    def stimulate(self):
        while self.keep_running:
            for handlers in self.channel_register.values():
                for handler in handlers:
                    self.handler_queue.put(handler.run)
            _time.sleep(self.stimulation_interval)


class HandlerQueue:
    def __init__(self):
        self._queue: list[Callable[[], None]] = []
        self._event = _threading.Event()

    def put(self, handler: Callable[[], None]):
        if handler not in self._queue:
            self._event.set()
            self._queue.append(handler)

    @property
    def should_wait(self):
        return not self._event.is_set()

    def wait(self):
        self._event.wait()

    def get(self) -> Callable[[], None]:
        return self._queue.pop(0)

    def __bool__(self):
        return bool(self._queue)

    def __len__(self):
        return len(self._queue)
