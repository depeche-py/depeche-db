import dataclasses as _dc
import datetime as _dt
import uuid as _uuid

import pytest

from depeche_db import (
    AggregatedStream,
    MessagePartitioner,
    MessageProtocol,
    MessageSerializer,
    MessageStore,
    StoredMessage,
)
from tests._tools import identifier


def test_pg_notify_storage_and_stream(db_engine, pg_notification_listener):
    stream_name = "aaa"
    store = MessageStore(
        name=identifier(), engine=db_engine, serializer=MyEventSerializer()
    )
    stream = AggregatedStream[MyEvent](
        name=identifier(),
        store=store,
        partitioner=MyPartitioner(),
        stream_wildcards=[stream_name],
    )

    events = [
        MyEvent(event_id=_uuid.uuid4(), num=1),
        MyEvent(event_id=_uuid.uuid4(), num=2),
    ]

    with pg_notification_listener(
        store._storage.notification_channel
    ) as notifications_storage:
        store.synchronize(stream=stream_name, messages=events, expected_version=0)
    assert notifications_storage == [
        {
            "stream": stream_name,
            "message_id": str(events[0].event_id),
            "version": 1,
            "global_position": 1,
        },
        {
            "stream": stream_name,
            "message_id": str(events[1].event_id),
            "version": 2,
            "global_position": 2,
        },
    ]

    with pg_notification_listener(stream.notification_channel) as notifications_stream:
        stream.projector.update_full()
    assert notifications_stream == [
        {"message_id": str(events[0].event_id), "partition": 1, "position": 0},
        {"message_id": str(events[1].event_id), "partition": 2, "position": 0},
    ]


@_dc.dataclass
class MyEvent(MessageProtocol):
    event_id: _uuid.UUID
    num: int

    def get_message_id(self) -> _uuid.UUID:
        return self.event_id

    def get_message_time(self) -> _dt.datetime:
        return _dt.datetime.now()


class MyEventSerializer(MessageSerializer[MyEvent]):
    def serialize(self, message: MyEvent) -> dict:
        val = _dc.asdict(message)
        val["event_id"] = str(val["event_id"])
        return val

    def deserialize(self, message: dict) -> MyEvent:
        return MyEvent(**message)


class MyPartitioner(MessagePartitioner[MyEvent]):
    def get_partition(self, event: StoredMessage[MyEvent]) -> int:
        return event.message.num


@pytest.fixture
def pg_notification_listener(pg_db):
    import contextlib
    import threading
    import time

    from depeche_db.tools import PgNotificationListener

    @contextlib.contextmanager
    def _inner(channel: str):
        result = []

        instance = PgNotificationListener(
            dsn=pg_db, channels=[channel], select_timeout=0.1
        )

        def _loop():
            for message in instance.messages():
                result.append(message.payload)

        thread = threading.Thread(target=_loop)
        thread.start()
        instance.start()

        try:
            time.sleep(0.1)
            yield result
            time.sleep(0.1)
        finally:
            instance.stop()
            thread.join()

    return _inner
