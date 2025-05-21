import asyncio
import json
import uuid

import pytest

from depeche_db import _compat
from depeche_db.tools.async_pg_notification_listener import (
    AsyncPgNotificationListener,
    PgNotification,
)


@pytest.mark.asyncio
@pytest.mark.skipif(not _compat.PSYCOPG3_AVAILABLE, reason="Requires psycopg3")
async def test_async_notification_listener(pg_db):
    pg_db = pg_db.replace("postgresql+psycopg:", "postgresql:")
    channel = f"test_channel_{uuid.uuid4().hex[:8]}"

    listener = AsyncPgNotificationListener(
        dsn=pg_db,
        channels=[channel],
        select_timeout=1.0,
    )

    await listener.start()

    notifications = []

    async def collect_notifications():
        async for notification in listener.messages():
            notifications.append(notification)
            if len(notifications) >= 3:
                break

    task = asyncio.create_task(collect_notifications())

    await asyncio.sleep(0.5)

    _send_notifications(pg_db, channel, 3)

    try:
        await asyncio.wait_for(task, timeout=5.0)
    except asyncio.TimeoutError:
        pass

    await listener.stop()

    assert len(notifications) == 3
    for i, notification in enumerate(notifications):
        assert isinstance(notification, PgNotification)
        assert notification.channel == channel
        assert notification.payload["message"] == f"Hello {i}"


@pytest.mark.asyncio
@pytest.mark.skipif(not _compat.PSYCOPG3_AVAILABLE, reason="Requires psycopg3")
async def test_async_notification_listener_context_manager(pg_db):
    pg_db = pg_db.replace("postgresql+psycopg:", "postgresql:")
    channel = f"test_channel_{uuid.uuid4().hex[:8]}"

    notifications = []

    async with AsyncPgNotificationListener(
        dsn=pg_db,
        channels=[channel],
        select_timeout=1.0,
    ) as listener:

        async def collect_notifications():
            async for notification in listener.messages():
                notifications.append(notification)
                if len(notifications) >= 2:
                    break

        task = asyncio.create_task(collect_notifications())

        await asyncio.sleep(0.5)

        _send_notifications(pg_db, channel, 2)

        try:
            await asyncio.wait_for(task, timeout=5.0)
        except asyncio.TimeoutError:
            pass

    assert len(notifications) == 2
    for i, notification in enumerate(notifications):
        assert isinstance(notification, PgNotification)
        assert notification.channel == channel
        assert notification.payload["message"] == f"Hello {i}"


def _send_notifications(pg_db: str, channel: str, count: int):
    import psycopg

    print(pg_db)
    conn = psycopg.connect(pg_db)
    try:
        with conn.cursor() as cursor:
            for i in range(count):
                payload = json.dumps({"message": f"Hello {i}"})
                cursor.execute(f"NOTIFY {channel}, '{payload}'")
            conn.commit()
    finally:
        conn.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not _compat.PSYCOPG3_AVAILABLE, reason="Requires psycopg3")
async def test_async_notification_listener_timeout(pg_db):
    pg_db = pg_db.replace("postgresql+psycopg:", "postgresql:")
    channel = f"test_channel_{uuid.uuid4().hex[:8]}"

    listener = AsyncPgNotificationListener(
        dsn=pg_db,
        channels=[channel],
        select_timeout=1.0,
    )

    await listener.start()

    notifications = []

    async def collect_notifications():
        async for notification in listener.messages(timeout=0.5):
            notifications.append(notification)

    task = asyncio.create_task(collect_notifications())

    await asyncio.sleep(0.5)

    try:
        await asyncio.wait_for(task, timeout=5.0)
    except asyncio.TimeoutError:
        pass

    await listener.stop()

    assert len(notifications) == 0
