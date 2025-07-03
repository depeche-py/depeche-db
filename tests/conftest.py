import os as _os
import threading
import uuid as _uuid

import pytest
import sqlalchemy as _sa
import sqlalchemy.orm as _orm

from depeche_db import (
    AggregatedStream,
    MessagePartitioner,
    MessageStore,
    StoredMessage,
    SubscriptionState,
)
from tests._account_example import (
    Account,
    AccountEvent,
    AccountEventSerializer,
    AccountRepository,
)

from . import _tools


@pytest.fixture(scope="session")
def pg_db():
    from depeche_db import _compat

    if _compat.PSYCOPG_VERSION == "3":
        dsn = f"postgresql+psycopg://depeche:depeche@localhost:4888/depeche_test_{_os.getpid()}"
    else:
        dsn = f"postgresql://depeche:depeche@localhost:4888/depeche_test_{_os.getpid()}"
    if _tools.pg_check_if_db_exists(dsn):
        _tools.pg_drop_db(dsn)
    _tools.pg_create_db(dsn)

    yield dsn

    _tools.pg_drop_db(dsn)


@pytest.fixture
def db_engine(pg_db):
    engine = _sa.create_engine(
        pg_db,
        future=True,
    )
    # _create_all(engine)

    yield engine

    # _drop_all(engine)
    engine.dispose()


@pytest.fixture
def db_session_factory(db_engine):
    session_factory = _orm.sessionmaker(db_engine)
    return session_factory


@pytest.fixture
def log_queries():
    import logging

    logger = logging.getLogger("sqlalchemy.engine")
    level = logger.getEffectiveLevel()
    logger.setLevel(logging.INFO)
    yield
    logger.setLevel(level)


@pytest.fixture
def identifier():
    def _inner() -> str:
        return f"id-{_uuid.uuid4()}".replace("-", "_")

    return _inner


ACCOUNT1_ID = _uuid.UUID("3cf31a23-5b95-42f3-b7d8-000000000001")
ACCOUNT2_ID = _uuid.UUID("3cf31a23-5b95-42f3-b7d8-000000000002")


class MyPartitioner(MessagePartitioner[AccountEvent]):
    def get_partition(self, event: StoredMessage[AccountEvent]) -> int:
        return int(str(event.message.account_id)[-1])


@pytest.fixture
def account_ids():
    return [ACCOUNT1_ID, ACCOUNT2_ID]


@pytest.fixture
def store_factory(identifier, db_engine):
    def _inner():
        return MessageStore(
            name=identifier(),
            engine=db_engine,
            serializer=AccountEventSerializer(),
        )

    return _inner


@pytest.fixture
def store_with_events(store_factory):
    store = store_factory()
    account_repo = AccountRepository(store)

    account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
    account2 = Account.register(id=ACCOUNT2_ID, owner_id=_uuid.uuid4(), number="234")
    account.credit(100)
    account2.credit(100)

    account_repo.save(account, expected_version=0)
    account_repo.save(account2, expected_version=0)
    account.credit(100)
    account_repo.save(account, expected_version=2)

    return store, account, account2


@pytest.fixture
def stream_factory(identifier, db_engine):
    def _inner(store: MessageStore[AccountEvent], partitioner=None, batch_size=None):
        stream = AggregatedStream[AccountEvent](
            name=identifier(),
            store=store,
            partitioner=partitioner or MyPartitioner(),
            stream_wildcards=["account-%"],
            update_batch_size=batch_size,
        )
        with db_engine.connect() as conn:
            stream.truncate(conn)
            conn.commit()
        return stream

    return _inner


@pytest.fixture
def stream_with_events(identifier, db_engine, store_with_events, stream_factory):
    store, account, account2 = store_with_events
    stream = stream_factory(store)
    stream.projector.update_full()
    return stream


@pytest.fixture
def subscription_factory(identifier, lock_provider):
    def _inner(stream, state_provider=None, handlers=None):
        return stream.subscription(
            name=identifier(),
            lock_provider=lock_provider,
            state_provider=state_provider or MyStateProvider(),
            handlers=handlers,
        )

    return _inner


class MyStateProvider:
    def __init__(self):
        self._state = SubscriptionState(positions={})
        self._initialized = False

    def store(self, subscription_name: str, partition: int, position: int):
        self._state.positions[partition] = position

    def read(self, subscription_name: str) -> SubscriptionState:
        return self._state

    def initialize(self, subscription_name: str):
        self._initialized = True

    def initialized(self, subscription_name: str) -> bool:
        return self._initialized

    def session(self, **kwargs):
        return self


@pytest.fixture
def lock_provider():
    return MyThreadLockProvider()


class MyThreadLockProvider:
    def __init__(self):
        self._lock = threading.Lock()
        self._locks = {}

    def lock(self, name: str) -> bool:
        with self._lock:
            if name in self._locks:
                return False
            self._locks[name] = True
            return True

    def unlock(self, name: str):
        with self._lock:
            del self._locks[name]
