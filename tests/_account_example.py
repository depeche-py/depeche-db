import datetime as _dt
import functools as _ft
import uuid as _uuid
from typing import Generic, List, Optional, TypeVar, Union

import pydantic as _pydantic

from depeche_db import (
    MessageProtocol,
    MessageSerializer,
    MessageStore,
    MessageStoreReader,
)

E = TypeVar("E", bound=MessageProtocol)


class Event(_pydantic.BaseModel):
    event_id: _uuid.UUID = _pydantic.Field(default_factory=_uuid.uuid4)
    occurred_at: _dt.datetime = _pydantic.Field(default_factory=_dt.datetime.utcnow)

    def get_message_id(self) -> _uuid.UUID:
        return self.event_id

    def get_message_time(self) -> _dt.datetime:
        return self.occurred_at

    model_config = _pydantic.ConfigDict(frozen=False, extra="forbid")


class AccountRegisteredEvent(Event):
    account_id: _uuid.UUID
    owner_id: _uuid.UUID
    number: str
    balance: int


class AccountCreditedEvent(Event):
    account_id: _uuid.UUID
    amount: int
    balance: int


AccountEvent = Union[AccountRegisteredEvent, AccountCreditedEvent]


# TODO use pydantic serializer
class AccountEventSerializer(MessageSerializer[AccountEvent]):
    def serialize(self, message: AccountEvent) -> dict:
        return message.model_dump(mode="json")

    def deserialize(self, message: dict) -> AccountEvent:
        return _pydantic.TypeAdapter(AccountEvent).validate_python(message)  # type: ignore


# TODO use event_sourceing.EventSourcedAggregateRoot
class AggregateRoot(Generic[E]):
    def __init__(self):
        self._events: List[E] = []
        self._version = 0

    @property
    def events(self) -> List[E]:
        return list(self._events)

    def _add_event(self, event: E) -> None:
        self._events.append(event)

    @property
    def id(self) -> _uuid.UUID:
        raise NotImplementedError()

    @property
    def version(self) -> int:
        return self._version

    def apply(self, event: E):
        self._apply(event)
        self._version += 1
        self._add_event(event)

    def _apply(self, event: E) -> None:
        raise NotImplementedError()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AggregateRoot):
            raise NotImplementedError()
        return self.id == other.id and self.version == other.version


class Account(AggregateRoot[AccountEvent]):
    def __init__(self):
        super().__init__()
        self._id = None
        self._owner_id = None
        self._number = None
        self._balance = 0
        self._events = []

    @property
    def id(self) -> _uuid.UUID:
        return self._id

    @property
    def owner_id(self) -> _uuid.UUID:
        return self._owner_id

    @_ft.singledispatchmethod
    def _apply(self, event: AccountEvent) -> None:  # type: ignore
        raise NotImplementedError()

    @_apply.register
    def _(self, event: AccountRegisteredEvent):
        self._id = event.account_id
        self._owner_id = event.owner_id
        self._number = event.number
        self._balance = event.balance

    @_apply.register
    def _(self, event: AccountCreditedEvent):
        self._balance += event.amount
        assert self._balance == event.balance

    @classmethod
    def register(
        cls, owner_id: _uuid.UUID, number: str, id: Optional[_uuid.UUID] = None
    ) -> "Account":
        account = cls()
        account.apply(
            AccountRegisteredEvent(
                account_id=id or _uuid.uuid4(),
                owner_id=owner_id,
                number=number,
                balance=0,
            )
        )
        return account

    def credit(self, amount: int) -> None:
        self.apply(
            AccountCreditedEvent(
                account_id=self.id, amount=amount, balance=self._balance + amount
            )
        )


class AccountNotFound(Exception):
    pass


class AccountReadRepository:
    def __init__(self, event_store_reader: MessageStoreReader[AccountEvent]):
        self._event_store_reader = event_store_reader

    def get(self, id: _uuid.UUID) -> Account:
        account = Account()
        for event in self._event_store_reader.read(f"account-{id}"):
            account.apply(event.message)
        if account.id is None:
            raise AccountNotFound(id)
        return account


class AccountRepository:
    def __init__(
        self,
        event_store: MessageStore[AccountEvent],
    ):
        self._event_store = event_store

    def save(self, account: Account, expected_version: int) -> None:
        self._event_store.synchronize(
            stream=f"account-{account.id}",
            messages=account.events,
            expected_version=expected_version,
        )

    def get(self, id: _uuid.UUID) -> Account:
        with self._event_store.reader() as reader:
            return AccountReadRepository(reader).get(id)
