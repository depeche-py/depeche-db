import collections as _collections
import queue as _queue
import signal as _signal
import threading as _threading
import time as _time
from typing import Dict, List, Optional

from ._interfaces import RunOnNotification
from .tools import PgNotificationListener


class Executor:
    """
    Executor is a class that runs handlers on notifications.

    Typical usage:

        executor = Executor(db_dsn="postgresql://localhost:5432/mydb")
        executor.register(MyRunOnNotification())
        executor.run()   # this will stop on SIGTERM or SIGINT

    Args:
        db_dsn: DSN for the PostgreSQL database
    """

    listener: Optional[PgNotificationListener] = None

    def __init__(self, db_dsn: str):
        self._db_dsn = db_dsn
        self.channel_register: Dict[
            str, List[RunOnNotification]
        ] = _collections.defaultdict(list)
        self.stimulation_interval = 0.5
        self.keep_running = True
        self.handler_queue = UniqueQueue()
        self.stimulator_thread = _threading.Thread(target=self._stimulate, daemon=True)
        self.handler_thread = _threading.Thread(target=self._run_handlers, daemon=True)
        self.listener = None
        _signal.signal(_signal.SIGINT, lambda *_: self._stop())
        _signal.signal(_signal.SIGTERM, lambda *_: self._stop())

    def register(self, handler: RunOnNotification):
        """
        Registers a handler to be run on notifications.

        Args:
            handler: Handler to register
        """
        self.channel_register[handler.notification_channel].append(handler)
        return handler

    def _stop(self):
        self.keep_running = False
        if self.listener is not None:
            self.listener.stop()

        for handlers in self.channel_register.values():
            for handler in handlers:
                handler.stop()

    def run(self):
        """Runs the executor."""
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

    def _run_handlers(self):
        while self.keep_running:
            try:
                handler = self.handler_queue.get(timeout=0.5)
                try:
                    handler()
                except Exception:
                    self._stop()
                    raise
            except _queue.Empty:
                pass

    def _stimulate(self):
        while self.keep_running:
            for handlers in self.channel_register.values():
                for handler in handlers:
                    self.handler_queue.put(handler.run)
            _time.sleep(self.stimulation_interval)


class UniqueQueue(_queue.Queue):
    def _put(self, item):
        if item not in self.queue:
            self.queue.append(item)
