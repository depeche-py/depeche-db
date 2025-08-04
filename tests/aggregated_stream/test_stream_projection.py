import threading as _threading
import uuid as _uuid
from unittest import mock as _mock

import pytest
import sqlalchemy as _sa

from depeche_db._aggregated_stream import (
    AggregatedStream,
    FullUpdateResult,
    SelectedOriginStream,
)
from depeche_db._interfaces import FixedTimeBudget, RunOnNotificationResult
from tests._account_example import Account, AccountRepository


def test_stream_projector(db_engine, store_factory, stream_factory, account_ids):
    ACCOUNT1_ID, ACCOUNT2_ID = account_ids
    store = store_factory()
    subject = stream_factory(store)
    assert subject.projector.update_full() == FullUpdateResult(0, False)
    with db_engine.connect() as conn:
        assert subject.get_max_aggregated_stream_positions(conn) == {}

    account_repo = AccountRepository(store)

    account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
    account.credit(100)
    account_repo.save(account, expected_version=0)
    assert subject.projector.update_full() == FullUpdateResult(2, False)
    with db_engine.connect() as conn:
        assert subject.get_max_aggregated_stream_positions(conn) == {
            1: 1,
        }

    account2 = Account.register(id=ACCOUNT2_ID, owner_id=_uuid.uuid4(), number="234")
    account2.credit(100)
    account_repo.save(account2, expected_version=0)
    assert subject.projector.update_full() == FullUpdateResult(2, False)
    with db_engine.connect() as conn:
        assert subject.get_max_aggregated_stream_positions(conn) == {
            1: 1,
            2: 1,
        }

    account2.credit(100)
    account2.credit(100)
    account2.credit(100)
    account2.credit(100)
    account_repo.save(account2, expected_version=2)
    assert subject.projector.update_full() == FullUpdateResult(4, False)
    with db_engine.connect() as conn:
        assert subject.get_max_aggregated_stream_positions(conn) == {
            1: 1,
            2: 5,
        }

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
            return subject.projector._select_origin_streams(conn=conn)

    account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
    account.credit(100)
    account.credit(100)
    account_repo.save(account, expected_version=0)

    assert get_select_origin_streams() == [
        SelectedOriginStream(stream=f"account-{account.id}", start_at_global_position=1)
    ]

    account2 = Account.register(id=ACCOUNT2_ID, owner_id=_uuid.uuid4(), number="234")
    account2.credit(100)
    account2.credit(100)
    account2.credit(100)
    account_repo.save(account2, expected_version=0)

    # because of the cached origin streams, the projector will not update
    # right away...
    assert get_select_origin_streams() == [
        SelectedOriginStream(
            stream=f"account-{account.id}", start_at_global_position=1
        ),
    ]

    # ... but only when the cache is cleared
    subject.projector._get_origin_stream_positions_cache = None
    assert get_select_origin_streams() == [
        SelectedOriginStream(
            stream=f"account-{account.id}", start_at_global_position=1
        ),
        SelectedOriginStream(
            stream=f"account-{account2.id}", start_at_global_position=4
        ),
    ]

    assert subject.projector.update_full() == FullUpdateResult(7, False)


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
            return subject.projector._select_origin_streams(conn=conn)

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
        assert subject.projector.update_full() == FullUpdateResult(2, False)
        long_running_conn.commit()

    assert get_select_origin_streams() == [
        # min_global_position == 1 because the events from the long-running
        # transaction are now visible
        SelectedOriginStream(f"account-{account.id}", start_at_global_position=1),
    ]
    assert subject.projector.update_full() == FullUpdateResult(2, False)
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


def test_stream_projector_run(db_engine, store_factory, stream_factory, account_ids):
    ACCOUNT1_ID, _ = account_ids
    store = store_factory()
    subject = stream_factory(store, batch_size=2)
    assert (
        subject.projector.run(FixedTimeBudget(-1))
        == RunOnNotificationResult.DONE_FOR_NOW
    )

    account_repo = AccountRepository(store)

    account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
    account.credit(100)
    account_repo.save(account, expected_version=0)
    assert (
        subject.projector.run(FixedTimeBudget(-1))
        == RunOnNotificationResult.WORK_REMAINING
    )


def test_stream_projector_work_left(
    db_engine, store_factory, stream_factory, account_ids
):
    ACCOUNT1_ID, _ = account_ids
    store = store_factory()
    subject = stream_factory(store, batch_size=2)
    with _mock.patch.object(
        subject.projector, "_update_batch", wraps=subject.projector._update_batch
    ) as mock_update_full:
        account_repo = AccountRepository(store)

        account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
        account.credit(100)
        account.credit(100)
        account_repo.save(account, expected_version=0)

        # This will only do one batch because of the batch size and the already exhausted time budget
        assert subject.projector.update_full(FixedTimeBudget(-1)) == FullUpdateResult(
            2, True
        )

        # This will do the rest of the batches
        assert subject.projector.update_full() == FullUpdateResult(1, False)

        # There are only 3 messages in total, so the total number of calls
        # to _update_batch should be 2.
        assert mock_update_full.call_count == 2


def test_stream_projector_creates_maxpos_table(
    db_engine, store_with_events, stream_factory, account_ids
):
    stream: AggregatedStream = stream_factory(store_with_events[0])
    with db_engine.connect() as conn:
        assert stream.get_max_aggregated_stream_positions(conn) == {}

    stream.projector.update_full()

    with db_engine.connect() as conn:
        assert stream.get_max_aggregated_stream_positions(conn) == {
            1: 2,
            2: 1,
        }
        # Simulate empty table because the stream was created before the maxpos table was created
        conn.execute(stream._maxpos_table.delete())
        conn.commit()

    # New instance of the stream should recreate the maxpos table
    stream = stream_factory(store_with_events[0])
    stream.projector.update_full()
    with db_engine.connect() as conn:
        assert stream.get_max_aggregated_stream_positions(conn) == {
            1: 2,
            2: 1,
        }
