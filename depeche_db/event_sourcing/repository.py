import abc as _abc
import logging as _logging
import pickle as _pickle
import threading as _threading
from typing import Callable, Generic, Optional, Protocol, TypeVar, cast

from depeche_db import (
    MessagePosition,
    MessageProtocol,
    MessageStoreProtocol,
    MessageStoreReaderProtocol,
)

from .aggregate_root import EventSourcedAggregateRoot

E = TypeVar("E", bound=MessageProtocol)
OBJ = TypeVar("OBJ", bound=EventSourcedAggregateRoot)
ID = TypeVar("ID", contravariant=True)

LOGGER = _logging.getLogger(__name__)


class RepoCache(Protocol, Generic[OBJ, ID]):
    def get(self, id: ID) -> Optional[OBJ]:
        ...

    def set(self, id: ID, obj: OBJ) -> None:
        ...


class Repo(_abc.ABC, Generic[OBJ, ID]):
    """
    A repository is a collection of objects that can be queried and
    persisted.

    This is an abstract base class that defines the interface that
    all repositories implement.
    """

    @_abc.abstractmethod
    def add(self, entity: OBJ) -> MessagePosition:
        """
        Add a new entity to the repository.
        """
        raise NotImplementedError

    @_abc.abstractmethod
    def save(self, entity: OBJ, expected_version: int) -> MessagePosition:
        """
        Save an existing entity to the repository.
        """
        raise NotImplementedError

    @_abc.abstractmethod
    def get(self, id: ID) -> OBJ:
        """
        Get an entity from the repository by its ID.
        """
        raise NotImplementedError


class EventStoreRepo(Generic[E, OBJ, ID], Repo[OBJ, ID]):
    def __init__(
        self,
        event_store: MessageStoreProtocol[E],
        constructor: Callable[[], OBJ],
        stream_prefix: str,
        cache: Optional[RepoCache[OBJ, ID]] = None,
    ):
        self._event_store = event_store
        self._constructor = constructor
        self._stream_prefix = stream_prefix
        self._cache = cache or NoOpCache()

    def add(self, entity: OBJ) -> MessagePosition:
        return self.save(entity, expected_version=0)

    def save(self, entity: OBJ, expected_version: int) -> MessagePosition:
        return self._event_store.synchronize(
            stream=f"{self._stream_prefix}-{entity.get_id()}",
            messages=entity.events,
            expected_version=expected_version,
        )

    def get(self, id: ID) -> OBJ:
        with self._event_store.reader() as reader:
            return ReadRepository[E, OBJ, ID](
                event_store_reader=reader,
                constructor=self._constructor,
                stream_prefix=self._stream_prefix,
                cache=self._cache,
            ).get(id)


class NotFound(Exception):
    pass


class ReadRepository(Generic[E, OBJ, ID]):
    def __init__(
        self,
        event_store_reader: MessageStoreReaderProtocol[E],
        constructor: Callable[[], OBJ],
        stream_prefix: str,
        cache: RepoCache[OBJ, ID],
    ):
        self._event_store_reader = event_store_reader
        self._constructor = constructor
        self._stream_prefix = stream_prefix
        self._cache = cache

    def get(self, id: ID) -> OBJ:
        obj = self._get_from_cache(id)
        if obj is not None:
            return obj

        obj = self._constructor()
        self._update_object(id, obj, cache_hit=False)

        if obj.get_id() is None:
            raise NotFound(id)
        self._cache.set(id, obj)
        return obj

    def _get_from_cache(self, id: ID) -> Optional[OBJ]:
        obj: Optional[OBJ] = self._cache.get(id)
        if obj is not None:
            try:
                self._update_object(id, obj, cache_hit=True)
                self._cache.set(id, obj)
                return obj
            except (KeyError, ValueError, TypeError, _pickle.UnpicklingError) as e:
                LOGGER.warning(f"Failed to update cached object {id}, refetching: {e}")
        return None

    def _update_object(self, id: ID, obj: OBJ, *, cache_hit: bool) -> int:
        del cache_hit  # Unused here, but useful for o11y purposes
        min_version = obj._version + 1 if obj._version > 0 else None
        applied_events = 0
        for event in self._event_store_reader.read(
            f"{self._stream_prefix}-{id}", min_version=min_version
        ):
            applied_events += 1
            obj.apply(event.message)
        return applied_events


class BaseCache(Generic[OBJ, ID]):
    _versions: dict[ID, int]
    _cache_every_x_version: int
    _lock: _threading.Lock

    def __init__(self, cache_every_x_version: int = 20) -> None:
        self._versions = {}
        self._cache_every_x_version = cache_every_x_version
        self._lock = _threading.Lock()

    def get(self, id: ID) -> Optional[OBJ]:
        with self._lock:
            obj = self._get(id)
            if obj is not None:
                self._versions[id] = obj._version
            else:
                self._versions.pop(id, None)
            return obj

    def _get(self, id: ID) -> Optional[OBJ]:
        raise NotImplementedError

    def set(self, id: ID, obj: OBJ) -> None:
        with self._lock:
            cached_version = self._versions.get(id)
            if (
                cached_version is None
                or (obj._version - cached_version) >= self._cache_every_x_version
            ):
                self._versions[id] = obj._version
                self._set(id, obj)

    def _set(self, id: ID, obj: OBJ) -> None:
        raise NotImplementedError


class NoOpCache(Generic[OBJ, ID]):
    def get(self, id: ID) -> Optional[OBJ]:
        return None

    def set(self, id: ID, obj: OBJ) -> None:
        pass


class InMemoryCache(BaseCache[OBJ, ID], Generic[OBJ, ID]):
    _store: dict[ID, bytes]

    def __init__(self, cache_every_x_version: int = 20):
        super().__init__(cache_every_x_version=cache_every_x_version)
        self._store = {}

    def _get(self, id: ID) -> Optional[OBJ]:
        data = self._store.get(id)
        if data is None:
            return None
        return cast(OBJ, _pickle.loads(data))

    def _set(self, id: ID, obj: OBJ) -> None:
        self._store[id] = _pickle.dumps(obj)
