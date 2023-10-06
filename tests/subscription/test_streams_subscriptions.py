import threading
import time
import uuid as _uuid

import pytest
import sqlalchemy as _sa

from depeche_db import (
    LinkStream,
    MessagePartitioner,
    MessageStore,
    StoredMessage,
    StreamPartitionStatistic,
    StreamProjector,
    Subscription,
    SubscriptionHandler,
    SubscriptionMessage,
    SubscriptionState,
)
from depeche_db.tools import DbSubscriptionStateProvider
from tests._account_example import (
    Account,
    AccountCreditedEvent,
    AccountEvent,
    AccountEventSerializer,
    AccountRegisteredEvent,
    AccountRepository,
)
from tests._tools import identifier

ACCOUNT1_ID = _uuid.UUID("3cf31a23-5b95-42f3-b7d8-000000000001")
ACCOUNT2_ID = _uuid.UUID("3cf31a23-5b95-42f3-b7d8-000000000002")


class MyPartitioner(MessagePartitioner[AccountEvent]):
    def get_partition(self, event: StoredMessage[AccountEvent]) -> int:
        return int(str(event.message.account_id)[-1])


@pytest.fixture
def store_with_events(db_engine):
    event_store = MessageStore(
        name=identifier(),
        engine=db_engine,
        serializer=AccountEventSerializer(),
    )
    account_repo = AccountRepository(event_store)

    account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
    account2 = Account.register(id=ACCOUNT2_ID, owner_id=_uuid.uuid4(), number="234")
    account.credit(100)
    account2.credit(100)

    account_repo.save(account, expected_version=0)
    account_repo.save(account2, expected_version=0)
    account.credit(100)
    account_repo.save(account, expected_version=2)

    return event_store, account, account2


@pytest.fixture
def stream(db_engine, store_with_events):
    event_store, _, _ = store_with_events
    stream = LinkStream[AccountEvent](name=identifier(), store=event_store)
    with db_engine.connect() as conn:
        stream.truncate(conn)
        conn.commit()
    return stream


@pytest.fixture
def stream_projector(db_engine, store_with_events, stream):
    event_store, _, _ = store_with_events
    proj = StreamProjector(
        stream=stream, partitioner=MyPartitioner(), stream_wildcards=["account-%"]
    )
    proj.update_full()
    return proj


def test_link_stream(db_engine, store_with_events, stream, stream_projector):
    _, account, account2 = store_with_events

    with db_engine.connect() as conn:
        assert list(stream.read(conn, partition=1)) == [
            evt.event_id for evt in account.events
        ]
        assert list(stream.read(conn, partition=2)) == [
            evt.event_id for evt in account2.events
        ]
    assert list(
        stream.get_partition_statistics(position_limits={1: 1}, result_limit=1)
    )[0] == StreamPartitionStatistic(
        partition_number=2,
        next_message_id=account2.events[0].event_id,
        next_message_position=0,
        next_message_occurred_at=account2.events[0].occurred_at,
    )


class MyStateProvider:
    def __init__(self):
        self._state = SubscriptionState(positions={})

    def store(self, group_name: str, partition: int, position: int):
        self._state.positions[partition] = position

    def read(self, group_name: str) -> SubscriptionState:
        return self._state


class MyLockProvider:
    def lock(self, name: str) -> bool:
        return True

    def unlock(self, name: str):
        pass


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


def assert_subscription_event_order(events: list[SubscriptionMessage[AccountEvent]]):
    for partition in {evt.partition for evt in events}:
        partition_events = [evt for evt in events if evt.partition == partition]
        assert partition_events == sorted(
            partition_events, key=lambda evt: evt.position
        )


def test_subscription(db_engine, stream, stream_projector):
    subject = Subscription[AccountEvent](
        group_name=identifier(),
        stream=stream,
        lock_provider=MyLockProvider(),
        state_provider=MyStateProvider(),
    )

    events = []
    while True:
        with subject.get_next_message() as event:
            if event is None:
                break
            events.append(event)
            event.ack()

    with subject.get_next_message() as event:
        assert event is None

    assert_subscription_event_order(events)


def test_db_subscription_state(db_engine, stream, stream_projector):
    state_provider_name = identifier()
    subject = Subscription[AccountEvent](
        group_name=identifier(),
        stream=stream,
        lock_provider=MyLockProvider(),
        state_provider=DbSubscriptionStateProvider(
            engine=db_engine, name=state_provider_name
        ),
    )

    events = []
    while True:
        with subject.get_next_message() as event:
            if event is None:
                break
            events.append(event)
            event.ack()

    assert_subscription_event_order(events)

    subject = Subscription[AccountEvent](
        group_name=subject.group_name,
        stream=stream,
        lock_provider=MyLockProvider(),
        state_provider=DbSubscriptionStateProvider(
            engine=db_engine, name=state_provider_name
        ),
    )

    with subject.get_next_message() as event:
        assert event is None


def test_subscription_in_parallel(db_engine, stream, stream_projector):
    subject = Subscription[AccountEvent](
        group_name=identifier(),
        stream=stream,
        lock_provider=MyThreadLockProvider(),
        state_provider=MyStateProvider(),
    )

    start = time.time()
    events = []

    def consume(n):
        failures = 0
        while failures < 10:
            with subject.get_next_message() as event:
                if event is None:
                    time.sleep(0.001)
                    failures += 1
                    continue
                events.append((event, time.time() - start))
                event.ack()

    threads = [
        threading.Thread(target=consume, args=(1,)),
        threading.Thread(target=consume, args=(2,)),
        threading.Thread(target=consume, args=(3,)),
    ]
    for t in threads:
        t.start()

    for t in threads:
        t.join(timeout=2)

    assert_subscription_event_order([e for e, _ in sorted(events, key=lambda x: x[-1])])


def test_stream_projector(db_engine, log_queries):
    event_store = MessageStore(
        name=identifier(),
        engine=db_engine,
        serializer=AccountEventSerializer(),
    )
    stream = LinkStream[AccountEvent](name=identifier(), store=event_store)
    subject = StreamProjector(
        stream=stream, partitioner=MyPartitioner(), stream_wildcards=["account-%"]
    )
    assert subject.update_full() == 0

    account_repo = AccountRepository(event_store)

    account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
    account.credit(100)
    account_repo.save(account, expected_version=0)
    assert subject.update_full() == 2

    account2 = Account.register(id=ACCOUNT2_ID, owner_id=_uuid.uuid4(), number="234")
    account2.credit(100)
    account_repo.save(account2, expected_version=0)
    assert subject.update_full() == 2

    account2.credit(100)
    account2.credit(100)
    account2.credit(100)
    account2.credit(100)
    account_repo.save(account2, expected_version=2)
    assert subject.update_full() == 4

    assert_stream_projection(stream, db_engine, account, account2)


def test_stream_projector_locking(db_engine):
    event_store = MessageStore(
        name=identifier(),
        engine=db_engine,
        serializer=AccountEventSerializer(),
    )
    stream = LinkStream[AccountEvent](name=identifier(), store=event_store)
    subject = StreamProjector(
        stream=stream, partitioner=MyPartitioner(), stream_wildcards=["account-%"]
    )
    with db_engine.connect() as conn:
        conn.execute(
            _sa.text(f"LOCK TABLE {stream._table.name} IN ACCESS EXCLUSIVE MODE")
        )
        with pytest.raises(RuntimeError):
            subject.update_full()


def assert_stream_projection(stream, db_engine, account, account2):
    with db_engine.connect() as conn:
        assert set(stream.read(conn, partition=1)).union(
            set(stream.read(conn, partition=2))
        ) == {event.event_id for event in account.events + account2.events}
        assert list(stream.read(conn, partition=1)) == [
            event.event_id for event in account.events
        ]
        assert list(stream.read(conn, partition=2)) == [
            event.event_id for event in account2.events
        ]


def test_subscription_handler(db_engine, stream, stream_projector):
    subject = Subscription[AccountEvent](
        group_name=identifier(),
        stream=stream,
        lock_provider=MyLockProvider(),
        state_provider=MyStateProvider(),
    )
    handler = SubscriptionHandler(subject)

    seen = []

    @handler.register
    def handle_account_registered(event: SubscriptionMessage[AccountRegisteredEvent]):
        seen.append(event)

    @handler.register
    def handle_account_credited(event: AccountCreditedEvent):
        seen.append(event)

    handler.run_once()
    assert [type(obj) for obj in seen] == [
        SubscriptionMessage,
        SubscriptionMessage,
        AccountCreditedEvent,
        AccountCreditedEvent,
        AccountCreditedEvent,
    ]
