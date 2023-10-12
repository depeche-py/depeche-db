import dataclasses as _dc
import datetime as _dt
import uuid as _uuid
from typing import TypeVar

import pytest

from depeche_db import MessagePosition, MessageProtocol, MessageSerializer, MessageStore

STREAM = "some-stream"


def test_write(subject, events):
    result = subject.write(stream=STREAM, message=events[0])
    assert result == MessagePosition(stream=STREAM, version=1, global_position=1)


def test_write_concurrency_failure_empty(subject, events):
    with pytest.raises(ValueError):
        subject.write(stream=STREAM, message=events[0], expected_version=1)


def test_write_concurrency_failure(subject, events):
    subject.write(stream=STREAM, message=events[0], expected_version=0)
    with pytest.raises(ValueError):
        subject.write(stream=STREAM, message=events[0], expected_version=0)
    with pytest.raises(ValueError):
        subject.write(stream=STREAM, message=events[0], expected_version=2)


def test_synchronize_idempotency(subject, events):
    result = subject.synchronize(
        stream=STREAM, expected_version=0, messages=events[:-1]
    )
    assert len(list(subject.read(STREAM))) == 3
    assert result == MessagePosition(stream=STREAM, version=3, global_position=3)

    result = subject.synchronize(
        stream=STREAM, expected_version=0, messages=events[:-1]
    )
    assert len(list(subject.read(STREAM))) == 3
    assert result == MessagePosition(stream=STREAM, version=3, global_position=3)

    result = subject.synchronize(stream=STREAM, expected_version=3, messages=events)
    assert len(list(subject.read(STREAM))) == 4
    assert result == MessagePosition(stream=STREAM, version=4, global_position=4)


def test_synchronize_id_mismatch(subject, events):
    subject.synchronize(stream=STREAM, expected_version=0, messages=events)

    with pytest.raises(ValueError):
        subject.synchronize(
            stream=STREAM,
            expected_version=4,
            messages=[MyEvent(event_id=_uuid.uuid4(), num=1)] + events[1:],
        )


def test_synchronize_fails_on_missing_message(subject, events):
    subject.synchronize(stream=STREAM, expected_version=0, messages=events)
    with pytest.raises(ValueError):
        subject.synchronize(stream=STREAM, expected_version=4, messages=events[1:])


def test_truncate(subject, events):
    subject.write(stream=STREAM, message=events[0])
    assert len(list(subject.read(STREAM))) == 1

    subject.truncate()
    assert len(list(subject.read(STREAM))) == 0


@pytest.fixture
def subject(identifier, db_engine):
    return MessageStore(
        name=identifier(), engine=db_engine, serializer=MyEventSerializer()
    )


@pytest.fixture
def events():
    return [
        MyEvent(event_id=_uuid.uuid4(), num=1),
        MyEvent(event_id=_uuid.uuid4(), num=2),
        MyEvent(event_id=_uuid.uuid4(), num=3),
        MyEvent(event_id=_uuid.uuid4(), num=4),
    ]


E = TypeVar("E", bound=MessageProtocol)


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
