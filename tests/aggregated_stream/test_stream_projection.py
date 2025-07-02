import threading as _threading
import uuid as _uuid

import pytest
import sqlalchemy as _sa

from depeche_db._aggregated_stream import (
    AggregatedStream,
    SelectedOriginStream,
)
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
    account_repo = AccountRepository(store)

    def get_select_origin_streams():
        with db_engine.connect() as conn:
            return subject.projector._select_origin_streams_naive(conn=conn)

    account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
    account.credit(100)
    account.credit(100)
    account_repo.save(account, expected_version=0)

    assert get_select_origin_streams() == [
        SelectedOriginStream(stream=f"account-{account.id}", start_at_global_position=1)
        # SelectedOriginStream(f"account-{account.id}", 0, 1, 3)
    ]

    account2 = Account.register(id=ACCOUNT2_ID, owner_id=_uuid.uuid4(), number="234")
    account2.credit(100)
    account2.credit(100)
    account2.credit(100)
    account_repo.save(account2, expected_version=0)

    assert get_select_origin_streams() == [
        SelectedOriginStream(
            stream=f"account-{account.id}", start_at_global_position=1
        ),
        SelectedOriginStream(
            stream=f"account-{account2.id}", start_at_global_position=4
        ),
    ]
    assert subject.projector.update_full() == 7


def test_stream_projector_origin_selection_late_commit(
    db_engine, store_factory, stream_factory, account_ids
):
    ACCOUNT1_ID, ACCOUNT2_ID = account_ids
    store = store_factory()
    subject: AggregatedStream = stream_factory(store)
    subject.projector.update_full()
    account_repo = AccountRepository(store)

    def get_select_origin_streams():
        with db_engine.connect() as conn:
            return subject.projector._select_origin_streams_naive(conn=conn)

    with db_engine.connect() as long_running_conn:
        # Simulate a long-running transaction that commits later
        # This object's events will not be visible to the projector until the transaction commits
        account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
        account.credit(100)
        account_repo._event_store.synchronize(
            stream=f"account-{account.id}",
            messages=account.events,
            expected_version=0,
            conn=long_running_conn,
        )

        assert get_select_origin_streams() == []

        account2 = Account.register(
            id=ACCOUNT2_ID, owner_id=_uuid.uuid4(), number="234"
        )
        account2.credit(100)
        account_repo.save(account2, expected_version=0)
        print("HERE")
        assert get_select_origin_streams() == [
            # min_global_position == 3 here because the events above already
            # consumed sequence numbers 1 and 2 even though they are not visible yet
            SelectedOriginStream(
                stream=f"account-{account2.id}", start_at_global_position=3
            ),
        ]
        assert subject.projector.update_full() == 2
        long_running_conn.commit()

    assert get_select_origin_streams() == [
        # min_global_position == 1 because the events from the long-running
        # transaction are now visible
        SelectedOriginStream(f"account-{account.id}", start_at_global_position=1),
    ]
    assert subject.projector.update_full() == 2
    assert_stream_projection(subject, db_engine, account, account2)


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


def test_stream_projector_interest(store_factory, stream_factory):
    subject: AggregatedStream = stream_factory(store_factory())
    assert subject.projector.interested_in_notification({"stream": "account-123"})
    assert not subject.projector.interested_in_notification({"stream": "foo"})


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
