from typing import Union
import datetime as _dt
import uuid as _uuid

import pydantic as _pydantic
import sqlalchemy as _sa

from depeche_db import (
    MessageProtocol,
    MessageStore,
)
from depeche_db.tools import PydanticMessageSerializer


class MyEvent(_pydantic.BaseModel, MessageProtocol):
    event_id: _uuid.UUID = _pydantic.Field(default_factory=_uuid.uuid4)
    num: int
    happened_at: _dt.datetime = _pydantic.Field(default_factory=_dt.datetime.utcnow)

    def get_message_id(self) -> _uuid.UUID:
        return self.event_id

    def get_message_time(self) -> _dt.datetime:
        return self.happened_at


class EventA(MyEvent):
    num: int


class EventB(MyEvent):
    text: str


EventType = Union[EventA, EventB]

message_store = MessageStore[EventType](
    name="example_basic",
    engine=_sa.create_engine(
        "postgresql://depeche:depeche@localhost:4888/depeche_demo"
    ),
    serializer=PydanticMessageSerializer(EventType),
)

stream = f"stream-{_uuid.uuid4()}"
events = [EventA(num=n + 1) for n in range(3)]

result = message_store.write(stream=stream, message=events[0])
print(result)
message_store.write(stream=stream, message=events[1])
print("Wrote 2 events")
print([e.message.num for e in message_store.read(stream)])
print(next(message_store.read(stream)))

try:
    # this fails because the expected version is 0, but the stream already has 2 events
    message_store.write(stream=stream, message=events[2], expected_version=0)
except Exception as e:
    print("Failed to write:", e)

# this is fine, because we expect the right version
message_store.write(stream=stream, message=events[2], expected_version=2)

# You can also just use the `synchronize` method to write a list of events
message_store.synchronize(stream=stream, messages=events, expected_version=3)
print("Now we have 4 events")
print([e.message.num for e in message_store.read(stream)])
