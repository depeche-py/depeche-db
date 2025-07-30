import collections as _collections
import dataclasses as _dc
import queue as _queue
import signal as _signal
import threading as _threading
import time as _time
import uuid as _uuid
from typing import Dict, List, Optional

from .._executor import UniqueQueue
from .._interfaces import FixedTimeBudget, RunOnNotification, RunOnNotificationResult
from ..tools import PgNotificationListener


@_dc.dataclass
class HandlerRegistration:
    handler: RunOnNotification
    concurrency: int
    id: _uuid.UUID = _dc.field(default_factory=_uuid.uuid4)


class ThreadedExecutor:
    """
    Executor is a class that runs handlers on notifications.

    This version of the executor runs handlers in separate threads, allowing
    for concurrent processing of notifications.

    This is an experimental feature and may
      a) not be suitable for all use cases and
      b) change without notice in future versions.

    Typical usage:

        executor = ThreadedExecutor(db_dsn="postgresql://localhost:5432/mydb")
        executor.register(MyRunOnNotification())
        executor.run()   # this will stop on SIGTERM or SIGINT

    Args:
        db_dsn: DSN for the PostgreSQL database
        stimulation_interval: Seconds; Every interval, all the handlers will run once. Zero & negative values disable stimulation.
        disable_signals: Disable catching SIGINT/SIGTERM, useful if you want to run the executor in a sub-thread
    """

    listener: Optional[PgNotificationListener] = None

    def __init__(
        self,
        db_dsn: str,
        stimulation_interval: float = 0.5,
        disable_signals: bool = False,
    ):
        self._db_dsn = db_dsn
        self.channel_register: Dict[
            str, List[HandlerRegistration]
        ] = _collections.defaultdict(list)
        self.stimulation_interval = stimulation_interval
        self.keep_running = True
        self.handler_queues: Dict[_uuid.UUID, UniqueQueue] = {}
        self.handler_threads: List[_threading.Thread] = []
        self.stimulator_thread = _threading.Thread(target=self._stimulate, daemon=True)
        self.listener = None
        if not disable_signals:
            _signal.signal(_signal.SIGINT, lambda *_: self._stop())
            _signal.signal(_signal.SIGTERM, lambda *_: self._stop())

    def register(self, handler: RunOnNotification):
        """
        Registers a handler to be run on notifications.

        Args:
            handler: Handler to register
        """
        concurrency = 1  # TODO v2: allow setting concurrency
        self.channel_register[handler.notification_channel].append(
            HandlerRegistration(handler=handler, concurrency=concurrency)
        )
        return handler

    def _stop(self):
        self.keep_running = False
        if self.listener is not None:
            self.listener.stop()

        for registrations in self.channel_register.values():
            for registration in registrations:
                registration.handler.stop()

    def run(self):
        """Runs the executor."""
        self.listener = PgNotificationListener(
            dsn=self._db_dsn,
            channels=list(self.channel_register),
            ignore_payload=False,
        )

        for registrations in self.channel_register.values():
            for registration in registrations:
                self.handler_queues[registration.id] = UniqueQueue()
                for _ in range(registration.concurrency):
                    self.handler_threads.append(
                        _threading.Thread(
                            target=self._run_handler(registration),
                            daemon=True,
                        )
                    )
        for thread in self.handler_threads:
            thread.start()
        self.stimulator_thread.start()
        self.listener.start()

        for notification in self.listener.messages():
            for registration in self.channel_register[notification.channel]:
                if registration.handler.interested_in_notification(
                    notification.payload
                ):
                    registration.handler.take_notification_hint(notification.payload)
                    self.handler_queues[registration.id].put(registration.id)

        for thread in self.handler_threads:
            thread.join()
        self.stimulator_thread.join()

    def _run_handler(self, registration: HandlerRegistration):
        def run_handler():
            while self.keep_running:
                try:
                    self.handler_queues[registration.id].get(timeout=0.5)
                    result = registration.handler.run(budget=FixedTimeBudget(seconds=3))
                    result = result or RunOnNotificationResult.DONE_FOR_NOW
                    if result == RunOnNotificationResult.WORK_REMAINING:
                        # Re-queue the handler if it has work remaining
                        self.handler_queues[registration.id].put(registration.id)
                except _queue.Empty:
                    pass

        return run_handler

    def _stimulate(self):
        if self.stimulation_interval <= 0:
            return
        while self.keep_running:
            for handlers in self.channel_register.values():
                for handler in handlers:
                    self.handler_queues[handler.id].put(handler.id)

            # Wait for the stimulation interval to pass
            started_at = _time.time()
            while (
                self.keep_running
                and _time.time() - started_at < self.stimulation_interval
            ):
                _time.sleep(0.1)
