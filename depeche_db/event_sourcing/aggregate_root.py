import abc as _abc
from typing import Generic, TypeVar

from depeche_db import MessageProtocol

ID = TypeVar("ID")
E = TypeVar("E", bound=MessageProtocol)


class EventSourcedAggregateRoot(_abc.ABC, Generic[ID, E]):
    def __init__(self):
        self._events: list[E] = []
        self._version = 0

    @property
    def events(self) -> list[E]:
        return list(self._events)

    @_abc.abstractmethod
    def get_id(self) -> ID:
        raise NotImplementedError

    def _add_event(self, event: E) -> None:
        self._version += 1
        self._events.append(event)

    def apply(self, event: E) -> None:
        self._apply(event)
        self._check_invariants()
        self._add_event(event)

    @_abc.abstractmethod
    def _apply(self, event: E) -> None:
        raise NotImplementedError

    def _check_invariants(self) -> None:
        pass

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, EventSourcedAggregateRoot):
            raise NotImplementedError()
        return self.get_id() == other.get_id() and self._version == other._version
