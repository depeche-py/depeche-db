import abc as _abc
from typing import Callable, Generic, TypeVar

from depeche_db import (
    MessagePosition,
    MessageProtocol,
    MessageStore,
    MessageStoreReader,
)

from .aggregate_root import EventSourcedAggregateRoot

E = TypeVar("E", bound=MessageProtocol)
OBJ = TypeVar("OBJ", bound=EventSourcedAggregateRoot)
ID = TypeVar("ID")


class Repo(_abc.ABC, Generic[OBJ, ID]):
    @_abc.abstractmethod
    def add(self, entity: OBJ) -> MessagePosition:
        raise NotImplementedError

    @_abc.abstractmethod
    def save(self, entity: OBJ, expected_version: int) -> MessagePosition:
        raise NotImplementedError

    @_abc.abstractmethod
    def get(self, id: ID) -> OBJ:
        raise NotImplementedError


class EventStoreRepo(Generic[E, OBJ, ID], Repo[OBJ, ID]):
    def __init__(
        self,
        event_store: MessageStore[E],
        constructor: Callable[[], OBJ],
        stream_prefix: str,
    ):
        self._event_store = event_store
        self._constructor = constructor
        self._stream_prefix = stream_prefix

    def add(self, obj: OBJ) -> MessagePosition:
        return self.save(obj, expected_version=0)

    def save(self, obj: OBJ, expected_version: int) -> MessagePosition:
        return self._event_store.synchronize(
            stream=f"{self._stream_prefix}-{obj.get_id()}",
            messages=obj.events,
            expected_version=expected_version,
        )

    def get(self, id: ID) -> OBJ:
        with self._event_store.reader() as reader:
            return ReadRepository[E, OBJ, ID](
                event_store_reader=reader,
                constructor=self._constructor,
                stream_prefix=self._stream_prefix,
            ).get(id)


class NotFound(Exception):
    pass


class ReadRepository(Generic[E, OBJ, ID]):
    def __init__(
        self,
        event_store_reader: MessageStoreReader[E],
        constructor: Callable[[], OBJ],
        stream_prefix: str,
    ):
        self._event_store_reader = event_store_reader
        self._constructor = constructor
        self._stream_prefix = stream_prefix

    def get(self, id: ID) -> OBJ:
        obj = self._constructor()
        for event in self._event_store_reader.read(f"{self._stream_prefix}-{id}"):
            obj.apply(event.message)
        if obj.get_id() is None:
            raise NotFound(id)
        return obj
