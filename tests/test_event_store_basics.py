import datetime as _dt
import dataclasses as _dc
import uuid as _uuid
from typing import TypeVar

import pytest

from depeche_db import (
    MessageProtocol,
    MessageSerializer,
    MessageStore,
    Storage,
)

from .tools import identifier

E = TypeVar("E", bound=MessageProtocol)


@pytest.fixture
def storage(db_engine):
    return Storage(name=identifier(), engine=db_engine)


def test_storage(db_engine, storage):
    with db_engine.connect() as conn:
        subject = storage

        id1 = _uuid.uuid4()
        result = subject.add(conn, "stream1", 0, id1, {"foo": "bar"})
        assert result.version == 1

        id3 = _uuid.uuid4()
        result = subject.add(conn, "stream2", 0, id3, {"foo": "bar"})
        assert result.version == 1

        id2 = _uuid.uuid4()
        result = subject.add(conn, "stream1", 1, id2, {"foo": "bar"})
        assert result.version == 2

        assert list(subject.read(conn, "stream1")) == [
            (id1, 1, {"foo": "bar"}, 1),
            (id2, 2, {"foo": "bar"}, 3),
        ]

        assert list(subject.read_multiple(conn, ["stream1", "stream2"])) == [
            (id1, "stream1", 1, {"foo": "bar"}, 1),
            (id3, "stream2", 1, {"foo": "bar"}, 2),
            (id2, "stream1", 2, {"foo": "bar"}, 3),
        ]

        assert list(subject.read_wildcard(conn, "stream%")) == [
            (id1, "stream1", 1, {"foo": "bar"}, 1),
            (id3, "stream2", 1, {"foo": "bar"}, 2),
            (id2, "stream1", 2, {"foo": "bar"}, 3),
        ]


@_dc.dataclass
class MyEvent(MessageProtocol):
    event_id: _uuid.UUID
    num: int

    def get_message_id(self) -> _uuid.UUID:
        return self.event_id

    def get_message_time(self) -> _dt.datetime:
        raise NotImplementedError


class MyEventSerializer(MessageSerializer[MyEvent]):
    def serialize(self, message: MyEvent) -> dict:
        val = _dc.asdict(message)
        val["event_id"] = str(val["event_id"])
        return val

    def deserialize(self, message: dict) -> MyEvent:
        return MyEvent(**message)


def test_eventstore_idempotency(db_engine):
    subject = MessageStore(
        name=identifier(), engine=db_engine, serializer=MyEventSerializer()
    )

    stream = "aaa"
    events = [
        MyEvent(event_id=_uuid.uuid4(), num=1),
        MyEvent(event_id=_uuid.uuid4(), num=2),
        MyEvent(event_id=_uuid.uuid4(), num=3),
        MyEvent(event_id=_uuid.uuid4(), num=4),
    ]

    subject.synchronize(stream=stream, expected_version=0, messages=events[:-1])
    assert len(list(subject.read(stream))) == 3

    subject.synchronize(stream=stream, expected_version=3, messages=events)
    assert len(list(subject.read(stream))) == 4

    with pytest.raises(ValueError):
        subject.synchronize(stream=stream, expected_version=4, messages=events[1:])
