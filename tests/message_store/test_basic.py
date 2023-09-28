import dataclasses as _dc
import datetime as _dt
import uuid as _uuid
from typing import TypeVar

import pytest

from depeche_db import (
    MessageProtocol,
    MessageSerializer,
    MessageStore,
)
from tests._tools import identifier

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
