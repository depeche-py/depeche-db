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
from tests._account_example import Account, AccountCreditedEvent, AccountRepository


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

    # New streams show up immediately — _select_origin_streams reads the
    # meta tables fresh on every call, so there is no per-call caching that
    # would hide the new account2 stream.
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

    def slow_projector_add(conn, messages, **kwargs):
        import time

        time.sleep(0.2)
        orig_projector_add(conn, messages, **kwargs)

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


def test_select_origin_streams_excludes_caught_up(
    db_engine, store_factory, stream_factory, account_ids
):
    """
    The msgs_meta.max > omax.max predicate is the heart of the SQL filter.
    Once a stream is fully projected, it must not appear in the candidate
    list — otherwise the projector would loop forever or hit unique
    constraints on re-projection.
    """
    ACCOUNT1_ID, ACCOUNT2_ID = account_ids
    store = store_factory()
    stream: AggregatedStream = stream_factory(store)
    repo = AccountRepository(store)

    a = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="1")
    a.credit(100)
    repo.save(a, expected_version=0)
    b = Account.register(id=ACCOUNT2_ID, owner_id=_uuid.uuid4(), number="2")
    b.credit(100)
    repo.save(b, expected_version=0)

    stream.projector.update_full()

    with db_engine.connect() as conn:
        assert stream.projector._select_origin_streams(conn=conn) == []

    # Add a new message to one stream; only that stream should now be a
    # candidate.
    a.credit(100)
    repo.save(a, expected_version=2)

    with db_engine.connect() as conn:
        candidates = stream.projector._select_origin_streams(conn=conn)
    # start_at = omax(a) + 1 = 2 + 1 = 3, not the position of the new message
    # itself — the projector then filters per-stream when reading.
    assert candidates == [
        SelectedOriginStream(stream=f"account-{a.id}", start_at_global_position=3),
    ]


def test_select_origin_streams_respects_batch_size(
    db_engine, store_factory, stream_factory, account_ids
):
    """
    The query limits its result to batch_size to keep each projector
    iteration bounded. A regression that drops the LIMIT would let the
    projector pull every behind stream into a single batch query.
    """
    ACCOUNT1_ID, ACCOUNT2_ID = account_ids
    store = store_factory()
    stream: AggregatedStream = stream_factory(store)
    stream.projector.batch_size = 2
    repo = AccountRepository(store)

    # 4 fresh streams that are all behind the agg stream.
    for i, acct_id in enumerate(
        [
            ACCOUNT1_ID,
            ACCOUNT2_ID,
            _uuid.UUID("3cf31a23-5b95-42f3-b7d8-000000000003"),
            _uuid.UUID("3cf31a23-5b95-42f3-b7d8-000000000004"),
        ]
    ):
        acct = Account.register(id=acct_id, owner_id=_uuid.uuid4(), number=str(i))
        repo.save(acct, expected_version=0)

    with db_engine.connect() as conn:
        candidates = stream.projector._select_origin_streams(conn=conn)
    assert (
        len(candidates) == 2
    ), f"expected at most batch_size (2) candidates, got {len(candidates)}"


def test_select_origin_streams_filters_by_wildcard(
    db_engine, store_factory, stream_factory, account_ids
):
    """
    The query has a LIKE filter against the configured stream wildcards;
    streams written to the same message store but not matching the
    wildcard must not appear in the candidate list.
    """
    ACCOUNT1_ID, _ = account_ids
    store = store_factory()
    stream: AggregatedStream = stream_factory(store)
    repo = AccountRepository(store)

    # Stream that matches the wildcard.
    a = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="1")
    repo.save(a, expected_version=0)

    # Stream that does NOT match "account-%". Bypass the Account abstraction
    # and write directly to a non-matching stream name.
    store.write(
        stream="other-thing",
        message=AccountCreditedEvent(account_id=ACCOUNT1_ID, amount=1, balance=1),
    )

    with db_engine.connect() as conn:
        candidates = stream.projector._select_origin_streams(conn=conn)
    candidate_streams = [c.stream for c in candidates]
    assert f"account-{a.id}" in candidate_streams
    assert "other-thing" not in candidate_streams


def test_update_origin_meta_is_monotonic(db_engine, store_factory, stream_factory):
    """
    _update_origin_meta UPSERT uses GREATEST so a stale or out-of-order
    write can never lower the recorded max. This is defensive — the
    projector only ever calls it with growing values — but it's the
    contract that lets re-runs and concurrent attempts be safe.
    """
    stream: AggregatedStream = stream_factory(store_factory())

    with db_engine.connect() as conn:
        stream._update_origin_meta(conn, {"some-stream": 100})
        conn.commit()
    with db_engine.connect() as conn:
        # A "stale" update with a lower value must not lower the recorded max.
        stream._update_origin_meta(conn, {"some-stream": 50})
        conn.commit()
    with db_engine.connect() as conn:
        result = conn.execute(
            _sa.select(
                stream._origin_meta_table.c.max_aggregated_origin_global_position
            ).where(stream._origin_meta_table.c.origin_stream == "some-stream")
        ).scalar()
    assert result == 100
