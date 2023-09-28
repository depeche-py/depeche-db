import dataclasses as _dc
import datetime as _dt
import uuid as _uuid

import pytest

from depeche_db import (
    LinkStream,
    MessagePartitioner,
    MessageProtocol,
    MessageSerializer,
    MessageStore,
    StoredMessage,
    StreamProjector,
)

from .tools import identifier


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


def test_eventstore_send_notification(db_engine, pg_notification_listener):
    stream_name = "aaa"
    store = MessageStore(
        name=identifier(), engine=db_engine, serializer=MyEventSerializer()
    )
    stream = LinkStream[MyEvent](name=identifier(), store=store)
    proj = StreamProjector(
        stream=stream, partitioner=MyPartitioner(), stream_wildcards=[stream_name]
    )

    events = [
        MyEvent(event_id=_uuid.uuid4(), num=1),
        MyEvent(event_id=_uuid.uuid4(), num=2),
    ]

    with pg_notification_listener(
        store._storage.notification_channel
    ) as notifications_storage, pg_notification_listener(
        stream.notification_channel
    ) as notifications_stream:
        store.synchronize(stream=stream_name, messages=events, expected_version=0)
        proj.update_full()
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
    assert notifications_stream == [
        {"message_id": str(events[0].event_id), "partition": 1, "position": 0},
        {"message_id": str(events[1].event_id), "partition": 2, "position": 0},
    ]


@pytest.fixture
def pg_notification_listener(pg_db):
    import contextlib
    import json
    import select
    import threading
    import time

    import psycopg2

    @contextlib.contextmanager
    def _inner(channel: str):
        keep_running = True
        result = []

        def _loop():
            conn = psycopg2.connect(pg_db)

            curs = conn.cursor()
            curs.execute(f"LISTEN {channel};")
            curs.execute("commit;")

            while keep_running:
                if select.select([conn], [], [], 0.1) == ([], [], []):
                    pass
                else:
                    conn.poll()
                    while conn.notifies:
                        notify = conn.notifies.pop(0)
                        try:
                            print(notify)
                            payload = json.loads(notify.payload)
                            result.append(payload)
                        except Exception as e:
                            print(e)

            conn.close()

        thread = threading.Thread(target=_loop)
        thread.start()

        try:
            time.sleep(0.1)
            yield result
            time.sleep(0.1)
        finally:
            keep_running = False
            thread.join()

    return _inner
