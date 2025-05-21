import asyncio

import pytest

from depeche_db import _compat


def test_reader(store_with_events, stream_factory):
    event_store, *_ = store_with_events
    subject = stream_factory(event_store)
    subject.projector.update_full()

    reader = subject.reader()

    messages = list(reader.get_messages(timeout=2))
    assert len(messages) == 5

    messages = list(reader.get_messages(timeout=2))
    assert len(messages) == 0


@pytest.mark.asyncio
@pytest.mark.skipif(not _compat.PSYCOPG3_AVAILABLE, reason="Requires psycopg3")
async def test_async_reader(store_with_events, stream_factory):
    event_store, *_ = store_with_events
    subject = stream_factory(event_store)
    subject.projector.update_full()

    reader = subject.async_reader()
    messages = []

    async def collect_messages():
        async for message in reader.get_messages(timeout=2):
            messages.append(message)

        async for message in reader.get_messages(timeout=2):
            messages.append(message)
            raise Exception("Shoud not reach here")

    task = asyncio.create_task(collect_messages())

    await asyncio.wait_for(task, timeout=5.0)

    assert len(messages) == 5
