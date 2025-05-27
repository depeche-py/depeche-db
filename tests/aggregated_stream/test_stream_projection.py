import threading as _threading
import uuid as _uuid

import pytest
import sqlalchemy as _sa

from depeche_db._aggregated_stream import AggregatedStream, SelectedOriginStream
from tests._account_example import Account, AccountRepository


def test_stream_projector(db_engine, store_factory, stream_factory, account_ids):
    ACCOUNT1_ID, ACCOUNT2_ID = account_ids
    store = store_factory()
    subject = stream_factory(store)
    assert subject.projector.update_full() == 0

    account_repo = AccountRepository(store)

    account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
    account.credit(100)
    account_repo.save(account, expected_version=0)
    assert subject.projector.update_full() == 2

    account2 = Account.register(id=ACCOUNT2_ID, owner_id=_uuid.uuid4(), number="234")
    account2.credit(100)
    account_repo.save(account2, expected_version=0)
    assert subject.projector.update_full() == 2

    account2.credit(100)
    account2.credit(100)
    account2.credit(100)
    account2.credit(100)
    account_repo.save(account2, expected_version=2)
    assert subject.projector.update_full() == 4

    assert_stream_projection(subject, db_engine, account, account2)


def test_stream_projector_origin_selection(
    db_engine, store_factory, stream_factory, account_ids
):
    ACCOUNT1_ID, ACCOUNT2_ID = account_ids
    store = store_factory()
    subject: AggregatedStream = stream_factory(store)
    subject.projector.batch_size = 2
    subject.projector.update_full()

    def get_select_origin_streams():
        with db_engine.connect() as conn:
            return subject.projector._select_origin_streams(conn=conn, cutoff_cond=[])

    account_repo = AccountRepository(store)

    account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
    account.credit(100)
    account.credit(100)
    account_repo.save(account, expected_version=0)

    assert get_select_origin_streams() == [
        SelectedOriginStream(f"account-{account.id}", 0, 1, 3)
    ]

    account2 = Account.register(id=ACCOUNT2_ID, owner_id=_uuid.uuid4(), number="234")
    account2.credit(100)
    account2.credit(100)
    account2.credit(100)
    account_repo.save(account2, expected_version=0)

    assert get_select_origin_streams() == [
        SelectedOriginStream(f"account-{account.id}", 0, 1, 3),
        # account 2 is beyond the batch size, so it is not selected yet
    ]
    assert subject.projector.update_full() == 7

    account.credit(100)
    account_repo.save(account, expected_version=3)
    account2.credit(100)
    account_repo.save(account2, expected_version=4)

    assert get_select_origin_streams() == [
        SelectedOriginStream(f"account-{account.id}", 3, 1, 1),
        SelectedOriginStream(f"account-{account2.id}", 4, 4, 1),
    ]


def test_stream_projector_cutoff(db_engine, store_factory, stream_factory, account_ids):
    ACCOUNT1_ID, ACCOUNT2_ID = account_ids
    store = store_factory()
    subject = stream_factory(store)

    # create events
    account_repo = AccountRepository(store)

    account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
    account.credit(100)
    account_repo.save(account, expected_version=0)

    orig_projector_add = subject.projector._add

    def slow_projector_add(conn, messages):
        import time

        time.sleep(0.2)
        orig_projector_add(conn, messages)

    subject.projector._add = slow_projector_add

    # start update_full in another thread
    thread = _threading.Thread(target=subject.projector.update_full)
    thread.start()

    # add more events
    account.credit(100)
    account_repo.save(account, expected_version=2)

    # wait for update_full to finish
    thread.join()

    # assert that the new events are not in the projection
    assert account.events[-1].event_id not in [
        msg.message_id for msg in subject.read(partition=1)
    ]


def test_stream_projector_locking(db_engine, store_factory, stream_factory):
    subject = stream_factory(store_factory())
    with db_engine.connect() as conn:
        conn.execute(
            _sa.text(f"LOCK TABLE {subject._table.name} IN ACCESS EXCLUSIVE MODE")
        )
        with pytest.raises(RuntimeError):
            subject.projector.update_full()


def assert_stream_projection(stream, db_engine, account, account2):
    assert {msg.message_id for msg in stream.read(partition=1)}.union(
        {msg.message_id for msg in stream.read(partition=2)}
    ) == {event.event_id for event in account.events + account2.events}
    assert [msg.message_id for msg in stream.read(partition=1)] == [
        event.event_id for event in account.events
    ]
    assert [msg.message_id for msg in stream.read(partition=2)] == [
        event.event_id for event in account2.events
    ]


def test_only_positive_partitions(
    db_engine, store_with_events, stream_factory, account_ids
):
    class IllegalPartitioner:
        def get_partition(self, event):
            return -1

    store = store_with_events[0]
    subject = stream_factory(store, partitioner=IllegalPartitioner())
    with pytest.raises(ValueError):
        subject.projector.update_full()
