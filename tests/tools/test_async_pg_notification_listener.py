import asyncio
import json
import os
import pytest
import uuid

from depeche_db.tools.async_pg_notification_listener import AsyncPgNotificationListener, PgNotification

pytestmark = pytest.mark.asyncio

# Skip tests if no PostgreSQL connection is available
pg_uri = os.environ.get("POSTGRES_URI", "postgresql://postgres:postgres@localhost/postgres")
requires_postgres = pytest.mark.skipif(
    "POSTGRES_URI" not in os.environ and not os.path.exists("/.dockerenv"),
    reason="Requires PostgreSQL connection",
)


@requires_postgres
async def test_async_notification_listener():
    """Test that the async notification listener receives notifications."""
    # Create a unique channel name for this test
    channel = f"test_channel_{uuid.uuid4().hex[:8]}"
    
    # Create the listener
    listener = AsyncPgNotificationListener(
        dsn=pg_uri,
        channels=[channel],
        select_timeout=1.0,
    )
    
    # Start the listener
    await listener.start()
    
    # Create a task to collect notifications
    notifications = []
    
    async def collect_notifications():
        async for notification in listener.messages():
            notifications.append(notification)
            if len(notifications) >= 3:
                break
    
    # Start collecting notifications
    task = asyncio.create_task(collect_notifications())
    
    # Give the listener time to start
    await asyncio.sleep(0.5)
    
    # Send some notifications
    import psycopg
    conn = psycopg.connect(pg_uri)
    try:
        with conn.cursor() as cursor:
            for i in range(3):
                payload = json.dumps({"message": f"Hello {i}"})
                cursor.execute(f"NOTIFY {channel}, %s", (payload,))
            conn.commit()
    finally:
        conn.close()
    
    # Wait for the task to complete or timeout
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except asyncio.TimeoutError:
        pass
    
    # Stop the listener
    await listener.stop()
    
    # Check that we received the notifications
    assert len(notifications) == 3
    for i, notification in enumerate(notifications):
        assert isinstance(notification, PgNotification)
        assert notification.channel == channel
        assert notification.payload["message"] == f"Hello {i}"


@requires_postgres
async def test_async_notification_listener_context_manager():
    """Test that the async notification listener works as a context manager."""
    # Create a unique channel name for this test
    channel = f"test_channel_{uuid.uuid4().hex[:8]}"
    
    # Create a task to collect notifications
    notifications = []
    
    # Use the listener as a context manager
    async with AsyncPgNotificationListener(
        dsn=pg_uri,
        channels=[channel],
        select_timeout=1.0,
    ) as listener:
        # Start collecting notifications
        async def collect_notifications():
            async for notification in listener.messages():
                notifications.append(notification)
                if len(notifications) >= 2:
                    break
        
        # Start collecting notifications
        task = asyncio.create_task(collect_notifications())
        
        # Give the listener time to start
        await asyncio.sleep(0.5)
        
        # Send some notifications
        import psycopg
        conn = psycopg.connect(pg_uri)
        try:
            with conn.cursor() as cursor:
                for i in range(2):
                    payload = json.dumps({"message": f"Hello {i}"})
                    cursor.execute(f"NOTIFY {channel}, %s", (payload,))
                conn.commit()
        finally:
            conn.close()
        
        # Wait for the task to complete or timeout
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
    
    # Check that we received the notifications
    assert len(notifications) == 2
    for i, notification in enumerate(notifications):
        assert isinstance(notification, PgNotification)
        assert notification.channel == channel
        assert notification.payload["message"] == f"Hello {i}"


@requires_postgres
async def test_async_notification_listener_ignore_payload():
    """Test that the async notification listener can ignore payloads."""
    # Create a unique channel name for this test
    channel = f"test_channel_{uuid.uuid4().hex[:8]}"
    
    # Create the listener with ignore_payload=True
    listener = AsyncPgNotificationListener(
        dsn=pg_uri,
        channels=[channel],
        select_timeout=1.0,
        ignore_payload=True,
    )
    
    # Start the listener
    await listener.start()
    
    # Create a task to collect notifications
    notifications = []
    
    async def collect_notifications():
        async for notification in listener.messages():
            notifications.append(notification)
            if len(notifications) >= 1:
                break
    
    # Start collecting notifications
    task = asyncio.create_task(collect_notifications())
    
    # Give the listener time to start
    await asyncio.sleep(0.5)
    
    # Send a notification
    import psycopg
    conn = psycopg.connect(pg_uri)
    try:
        with conn.cursor() as cursor:
            payload = json.dumps({"message": "Hello"})
            cursor.execute(f"NOTIFY {channel}, %s", (payload,))
            conn.commit()
    finally:
        conn.close()
    
    # Wait for the task to complete or timeout
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except asyncio.TimeoutError:
        pass
    
    # Stop the listener
    await listener.stop()
    
    # Check that we received the notification with an empty payload
    assert len(notifications) == 1
    notification = notifications[0]
    assert isinstance(notification, PgNotification)
    assert notification.channel == channel
    assert notification.payload == {}
