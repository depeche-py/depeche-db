import dataclasses as _dc
import json
import logging
import queue
import select
import threading
from typing import Iterator, Sequence

import psycopg2

logger = logging.getLogger(__name__)


@_dc.dataclass
class PgNotification:
    channel: str
    payload: dict


class PgNotificationListener:
    def __init__(
        self,
        dsn: str,
        channels: Sequence[str],
        select_timeout: float = 3.0,
        ignore_payload: bool = False,
    ):
        self.dsn = dsn
        self.channels = channels
        self._keep_running = True
        self._thread = threading.Thread(target=self._loop)
        self._queue = queue.Queue[PgNotification]()
        self._ignore_payload = ignore_payload
        self._select_timeout = select_timeout
        self._queue_timeout = select_timeout / 2

    def messages(self) -> Iterator[PgNotification]:
        while self._keep_running:
            try:
                yield self._queue.get(block=True, timeout=self._queue_timeout)
            except queue.Empty:
                pass

    def start(self):
        self._thread.start()

    def stop(self):
        self._keep_running = False
        self._thread.join()

    def _loop(self):
        conn = psycopg2.connect(self.dsn)

        try:
            curs = conn.cursor()
            for channel in self.channels:
                curs.execute(f"LISTEN {channel};")
            curs.execute("commit;")

            while self._keep_running:
                if select.select([conn], [], [], self._select_timeout) == ([], [], []):
                    pass
                else:
                    conn.poll()
                    while conn.notifies:
                        notification = conn.notifies.pop(0)
                        try:
                            if self._ignore_payload:
                                payload = {}
                            else:
                                payload = json.loads(notification.payload)
                            self._queue.put(
                                PgNotification(notification.channel, payload)
                            )
                        except Exception:
                            logger.exception(
                                f"Error processing notification payload: {notification}"
                            )

        finally:
            conn.close()
