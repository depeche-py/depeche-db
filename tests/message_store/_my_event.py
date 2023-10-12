import dataclasses as _dc
import datetime as _dt
import uuid as _uuid
from typing import TypeVar

from depeche_db import MessageProtocol, MessageSerializer

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
        return MyEvent(event_id=_uuid.UUID(message["event_id"]), num=message["num"])
