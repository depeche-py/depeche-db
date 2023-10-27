import datetime as _dt
import uuid as _uuid
from typing import Optional, Union

import pydantic as _pydantic
import pytest

from depeche_db import MessageStore
from depeche_db import event_sourcing as _es
from depeche_db.tools import PydanticMessageSerializer


class Event(_pydantic.BaseModel):
    id: _uuid.UUID = _pydantic.Field(default_factory=_uuid.uuid4)
    created_at: _dt.datetime = _pydantic.Field(default_factory=_dt.datetime.utcnow)

    def get_message_id(self) -> _uuid.UUID:
        return self.id

    def get_message_time(self) -> _dt.datetime:
        return self.created_at


class FooCreatedEvent(Event):
    foo_id: _uuid.UUID
    name: str


class FooRenamedEvent(Event):
    foo_id: _uuid.UUID
    new_name: str


FooEvent = Union[FooCreatedEvent, FooRenamedEvent]


class Foo(_es.EventSourcedAggregateRoot[_uuid.UUID, FooEvent]):
    id: _uuid.UUID
    name: str

    def get_id(self) -> Optional[_uuid.UUID]:
        if hasattr(self, "id"):
            return self.id
        return None

    def _apply(self, event: FooEvent) -> None:
        if isinstance(event, FooCreatedEvent):
            self.id = event.foo_id
            self.name = event.name
        elif isinstance(event, FooRenamedEvent):
            self.name = event.new_name
        else:
            raise NotImplementedError(f"Event {type(event)} is not supported")

    def _check_invariants(self):
        assert self.name != "", "Name cannot be empty"
        assert self.name != self.name.upper(), "Name cannot be all uppercase"

    @property
    def version(self) -> int:
        return self._version

    @classmethod
    def create(cls, id: _uuid.UUID, name: str) -> "Foo":
        foo = cls()
        foo.apply(FooCreatedEvent(foo_id=id, name=name))
        return foo

    def rename(self, new_name: str) -> None:
        self.apply(FooRenamedEvent(foo_id=self.id, new_name=new_name))


def test_event_sourced_aggregate():
    foo = Foo.create(_uuid.uuid4(), "foo")
    assert foo.version == 1
    assert foo.name == "foo"
    assert len(foo.events) == 1

    foo.rename("bar")
    assert foo.version == 2
    assert foo.name == "bar"
    assert len(foo.events) == 2


def test_event_sourced_aggregate_enforces_invariants():
    with pytest.raises(AssertionError):
        Foo.create(_uuid.uuid4(), "")

    foo = Foo.create(_uuid.uuid4(), "a")
    with pytest.raises(AssertionError):
        foo.rename("BAR")
    assert len(foo.events) == 1


class FooRepo(_es.EventStoreRepo[FooEvent, Foo, _uuid.UUID]):
    def __init__(self, event_store: MessageStore[FooEvent]):
        super().__init__(
            event_store=event_store,
            constructor=Foo,
            stream_prefix="foo",
        )


def test_repository(identifier, db_engine):
    store = MessageStore[FooEvent](
        name=identifier(),
        engine=db_engine,
        serializer=PydanticMessageSerializer(FooEvent),
    )
    repo = FooRepo(store)

    foo = Foo.create(_uuid.uuid4(), "foo")
    position = repo.add(foo)
    assert position.version == 1

    foo.rename("bar")
    position = repo.save(foo, expected_version=1)
    assert position.version == 2

    stored_messages = list(store.read(f"foo-{foo.id}"))
    assert [m.message for m in stored_messages] == foo._events

    foo2 = repo.get(foo.id)
    assert foo2 == foo
