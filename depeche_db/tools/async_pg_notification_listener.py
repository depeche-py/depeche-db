import asyncio
import dataclasses as _dc
import json
import logging
from typing import AsyncIterator, Sequence

from depeche_db._compat import PSYCOPG_VERSION
from depeche_db._compat import psycopg as _psycopg

logger = logging.getLogger(__name__)


@_dc.dataclass
class PgNotification:
    channel: str
    payload: dict


class AsyncPgNotificationListener:
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
        dsn, expected_psycopg_version = self._parse_dsn(self.dsn)
        assert expected_psycopg_version == PSYCOPG_VERSION, "Invalid psycopg version"
        
        if PSYCOPG_VERSION == "3":
            self._conn = await _psycopg.AsyncConnection.connect(dsn)
        else:
            # For psycopg2, we need to use the aiopg library
            import aiopg
            self._conn = await aiopg.connect(dsn)
        
        async with self._conn.cursor() as cursor:
            for channel in self.channels:
                await cursor.execute(f"LISTEN {channel};")
            await self._conn.commit()

    async def messages(self) -> AsyncIterator[PgNotification]:
        """Yield notifications as they arrive."""
        if self._conn is None:
            await self.start()
            
        while self._keep_running:
            try:
                if PSYCOPG_VERSION == "3":
                    async for notification in self._conn.notifies(timeout=self._select_timeout):
                        yield await self._process_notification(notification)
                else:
                    # For psycopg2 with aiopg
                    notification = await self._conn.notifies.get()
                    yield await self._process_notification(notification)
            except asyncio.TimeoutError:
                # No notifications received within timeout
                pass
            except Exception as e:
                logger.exception(f"Error receiving notifications: {e}")
                await asyncio.sleep(0.1)  # Prevent tight loop in case of errors

    async def stop(self):
        """Stop the notification listener and close the connection."""
        self._keep_running = False
        if self._conn:
            await self._conn.close()
            self._conn = None

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

    async def _process_notification(self, notification):
        try:
            if self._ignore_payload:
                payload = {}
            else:
                payload = json.loads(notification.payload)
            return PgNotification(notification.channel, payload)
        except Exception:
            logger.exception(f"Error processing notification payload: {notification}")
            return None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
