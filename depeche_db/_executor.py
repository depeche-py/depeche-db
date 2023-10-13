import collections as _collections
import signal as _signal
import threading as _threading
import time as _time
from typing import Callable, Dict, List

from ._interfaces import RunOnNotification
from .tools import PgNotificationListener


class Executor:
    def __init__(self, db_dsn: str):
        self._db_dsn = db_dsn
        self.channel_register: Dict[
            str, List[Callable[[], None]]
        ] = _collections.defaultdict(list)
        self.stimulation_interval = 0.5

    def register(self, handler: RunOnNotification):
        self.channel_register[handler.notification_channel].append(handler.run)
        return handler

    def run(self):
        listener = PgNotificationListener(
            dsn=self._db_dsn,
            channels=list(self.channel_register),
            ignore_payload=True,
        )
        keep_running = True
        handler_queue = []
        handler_queue_event = _threading.Event()

        def stop():
            print("Stopping...")
            nonlocal keep_running
            keep_running = False
            listener.stop()

        _signal.signal(_signal.SIGINT, lambda *_: stop())
        _signal.signal(_signal.SIGTERM, lambda *_: stop())

        def run_handlers():
            while keep_running:
                if handler_queue:
                    handler = handler_queue.pop(0)
                    handler()
                else:
                    handler_queue_event.wait()
                    handler_queue_event.clear()

        handler_thread = _threading.Thread(target=run_handlers, daemon=True)
        handler_thread.start()

        def stimulate():
            while keep_running:
                for handlers in self.channel_register.values():
                    for handler in handlers:
                        if handler not in handler_queue:
                            handler_queue.append(handler)
                            handler_queue_event.set()
                _time.sleep(self.stimulation_interval)

        stimulator = _threading.Thread(target=stimulate, daemon=True)
        stimulator.start()

        listener.start()
        print("Started")
        for notification in listener.messages():
            for handler in self.channel_register[notification.channel]:
                if handler not in handler_queue:
                    handler_queue.append(handler)
                    handler_queue_event.set()
