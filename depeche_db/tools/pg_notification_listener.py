import dataclasses as _dc
import json
import logging
import queue
import select
import threading
import time
from typing import Iterator, Sequence

from depeche_db._compat import PSYCOPG_VERSION
from depeche_db._compat import psycopg as _psycopg

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

    def messages(self, timeout: float = 0) -> Iterator[PgNotification]:
        last_message_at = time.time()
        while self._keep_running:
            try:
                yield self._queue.get(block=True, timeout=self._queue_timeout)
                last_message_at = time.time()
            except queue.Empty:
                if timeout > 0:
                    if time.time() - last_message_at > timeout:
                        break
                pass

    def start(self):
        self._thread.start()

    def stop(self):
        self._keep_running = False
        self._thread.join()

    def _parse_dsn(self, dsn: str) -> tuple[str, str]:
        import urllib.parse as _urlparse

        parts = list(_urlparse.urlparse(dsn))
        expected_psycopg_version = "2"
        if parts[0] == "postgresql+psycopg":
            parts[0] = "postgresql"
            expected_psycopg_version = "3"
        if parts[0] == "postgresql+psycopg2":
            parts[0] = "postgresql"
        dsn = _urlparse.urlunparse(parts)

        return dsn, expected_psycopg_version

    def _loop(self):
        dsn, expected_psycopg_version = self._parse_dsn(self.dsn)
        assert expected_psycopg_version == PSYCOPG_VERSION, "Invalid psycopg version"
        conn = _psycopg.connect(dsn)

        try:
            curs = conn.cursor()
            for channel in self.channels:
                curs.execute(f"LISTEN {channel};")
            curs.execute("commit;")

            while self._keep_running:
                if select.select([conn], [], [], self._select_timeout) == ([], [], []):
                    pass
                else:
                    if PSYCOPG_VERSION == "3":
                        self._psycopg3_loop(conn)
                    else:
                        self._psycopg2_loop(conn)
        finally:
            conn.close()

    def _psycopg3_loop(self, conn):
        for notification in conn.notifies(timeout=0.2):
            self._process_notification(notification)

    def _psycopg2_loop(self, conn):
        conn.poll()
        while conn.notifies:
            self._process_notification(conn.notifies.pop(0))

    def _process_notification(self, notification):
        try:
            if self._ignore_payload:
                payload = {}
            else:
                payload = json.loads(notification.payload)
            self._queue.put(PgNotification(notification.channel, payload))
        except Exception:
            logger.exception(f"Error processing notification payload: {notification}")
