import uuid as _uuid

from depeche_db import (
    MessageStore,
)

from .account import (
    Account,
    AccountEventSerializer,
    AccountRepository,
)
from .tools import identifier


def test_read_write_domain_object(db_engine):
    account = Account.register(owner_id=_uuid.uuid4(), number="123")
    account.credit(100)

    event_store = MessageStore(
        name=identifier(),
        engine=db_engine,
        serializer=AccountEventSerializer(),
    )
    account_repo = AccountRepository(event_store)
    account_repo.save(account, expected_version=0)

    account_again = account_repo.get(account.id)
    assert account_again.id == account.id
    assert account_again.version == account.version
    assert account_again._number == account._number
    assert account_again._balance == account._balance

    account_repo.save(account_again, expected_version=2)
    account_again.credit(100)
    account_repo.save(account_again, expected_version=2)
