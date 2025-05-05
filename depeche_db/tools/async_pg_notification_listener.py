import asyncio as _asyncio
import json as _json
import logging as _logging
import time
from typing import Any, AsyncIterator, Sequence

from .. import _compat
from .pg_notification_listener import PgNotification

logger = _logging.getLogger(__name__)


class AsyncPgNotificationListener:
    _conn: Any

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
        self._ignore_payload = ignore_payload
        self._select_timeout = select_timeout
        self._conn = None
        self._task = None

    async def start(self):
        """Start the notification listener."""
        assert (
            _compat.PSYCOPG3_AVAILABLE
        ), "AsyncPgNotificationListener requires psycopg3"
        import psycopg as _psycopg3

        dsn = self._parse_dsn(self.dsn)

        self._conn = await _psycopg3.AsyncConnection.connect(dsn)
        assert self._conn is not None

        async with self._conn.cursor() as cursor:
            for channel in self.channels:
                await cursor.execute(f"LISTEN {channel};")
            await self._conn.commit()

    async def messages(self, timeout: float = 0) -> AsyncIterator[PgNotification]:
        """Yield notifications as they arrive."""
        if self._conn is None:
            await self.start()
        assert self._conn is not None

        last_message_at = time.time()
        while self._keep_running:
            try:
                print("Waiting for notifications...", self.channels)
                async for notification in self._conn.notifies(
                    timeout=timeout if timeout > 0 else self._select_timeout
                ):
                    notification = await self._process_notification(notification)
                    last_message_at = time.time()
                    print("Async notification received:", notification)
                    if notification:
                        yield notification

                if timeout > 0:
                    if time.time() - last_message_at > timeout:
                        break
            except Exception as e:
                logger.exception(f"Error receiving notifications: {e}")
                await _asyncio.sleep(0.1)  # Prevent tight loop in case of errors

    async def stop(self):
        """Stop the notification listener and close the connection."""
        self._keep_running = False
        if self._conn:
            await self._conn.close()
            self._conn = None

    def _parse_dsn(self, dsn: str) -> str:
        import urllib.parse as _urlparse

        parts = list(_urlparse.urlparse(dsn))
        if parts[0] == "postgresql+psycopg":
            parts[0] = "postgresql"
        if parts[0] == "postgresql+psycopg2":
            parts[0] = "postgresql"
        dsn = _urlparse.urlunparse(parts)

        return dsn

    async def _process_notification(self, notification):
        try:
            if self._ignore_payload:
                payload = {}
            else:
                payload = _json.loads(notification.payload)
            return PgNotification(notification.channel, payload)
        except Exception:
            logger.exception(f"Error processing notification payload: {notification}")
            return None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
