"""Tests for stream closing, archiving, and subscription resilience."""

import uuid

import pytest

from depeche_db import (
    OptimisticConcurrencyError,
    Storage,
    StreamClosedError,
    StreamNotClosedError,
)
from tests._account_example import AccountCreditedEvent
from tests.conftest import MyPartitioner


def _close_event(account):
    return AccountCreditedEvent(
        account_id=account.id, amount=0, balance=account._balance
    )


# --- close_stream ---


def test_close_stream_writes_tombstone(store_with_events):
    store, account, _ = store_with_events
    stream_name = f"account-{account.id}"

    close_event = _close_event(account)
    store.close_stream(stream=stream_name, message=close_event, expected_version=3)

    messages = list(store.read(stream_name))
    assert messages[-1].message.get_message_id() == close_event.event_id


def test_close_stream_prevents_further_writes(store_with_events):
    store, account, _ = store_with_events
    stream_name = f"account-{account.id}"

    store.close_stream(
        stream=stream_name, message=_close_event(account), expected_version=3
    )

    with pytest.raises(StreamClosedError):
        store.write(
            stream=stream_name,
            message=AccountCreditedEvent(
                account_id=account.id, amount=50, balance=account._balance + 50
            ),
        )


def test_close_stream_requires_expected_version(store_with_events):
    store, account, _ = store_with_events
    stream_name = f"account-{account.id}"

    with pytest.raises(OptimisticConcurrencyError):
        store.close_stream(
            stream=stream_name,
            message=_close_event(account),
            expected_version=1,
        )


def test_close_already_closed_stream_fails(store_with_events):
    store, account, _ = store_with_events
    stream_name = f"account-{account.id}"

    store.close_stream(
        stream=stream_name, message=_close_event(account), expected_version=3
    )

    with pytest.raises(StreamClosedError):
        store.close_stream(
            stream=stream_name,
            message=_close_event(account),
            expected_version=4,
        )


def test_close_event_flows_through_pipeline(
    store_with_events, stream_factory, subscription_factory
):
    store, account, _ = store_with_events
    stream_name = f"account-{account.id}"
    stream = stream_factory(store)
    stream.projector.update_full()

    close_event = _close_event(account)
    store.close_stream(stream=stream_name, message=close_event, expected_version=3)
    stream.projector.update_full()

    sub = subscription_factory(stream)
    all_message_ids: set[uuid.UUID] = set()
    for _ in range(10):
        messages = list(sub.get_next_messages(count=100))
        if not messages:
            break
        all_message_ids.update(m.stored_message.message_id for m in messages)
    assert close_event.event_id in all_message_ids


def test_close_stream_with_connection(store_with_events):
    store, account, _ = store_with_events
    stream_name = f"account-{account.id}"

    close_event = _close_event(account)
    with store.engine.connect() as conn:
        store.close_stream(
            stream=stream_name, message=close_event, expected_version=3, conn=conn
        )
        conn.commit()

    with pytest.raises(StreamClosedError):
        store.write(
            stream=stream_name,
            message=AccountCreditedEvent(
                account_id=account.id, amount=50, balance=account._balance + 50
            ),
        )


# --- archive_stream ---


@pytest.fixture
def archive_table(db_engine):
    table = Storage.create_archive_table("test_archive", db_engine)
    yield table
    with db_engine.connect() as conn:
        conn.execute(table.delete())
        conn.commit()


def test_archive_requires_closed_stream(store_with_events, archive_table):
    store, account, _ = store_with_events
    stream_name = f"account-{account.id}"

    with pytest.raises(StreamNotClosedError):
        store.archive_stream(stream_name, archive_table)


def test_archive_moves_messages_to_archive_table(store_with_events, archive_table):
    store, account, _ = store_with_events
    stream_name = f"account-{account.id}"

    close_event = _close_event(account)
    store.close_stream(stream=stream_name, message=close_event, expected_version=3)
    store.archive_stream(stream_name, archive_table)

    # Main table: only the close message
    main_messages = list(store.read(stream_name))
    assert len(main_messages) == 1
    assert main_messages[0].message.get_message_id() == close_event.event_id

    # Archive table: all messages (3 events + close = 4)
    with store.engine.connect() as conn:
        archive_rows = list(
            conn.execute(
                archive_table.select().where(archive_table.c.stream == stream_name)
            )
        )
    assert len(archive_rows) == 4


def test_archive_does_not_affect_other_streams(store_with_events, archive_table):
    store, account, account2 = store_with_events
    stream_name = f"account-{account.id}"
    other_stream = f"account-{account2.id}"

    store.close_stream(
        stream=stream_name, message=_close_event(account), expected_version=3
    )
    store.archive_stream(stream_name, archive_table)

    other_messages = list(store.read(other_stream))
    assert len(other_messages) == 2  # register + credit


def test_archive_removes_agg_stream_pointers(
    store_with_events, identifier, db_engine, archive_table
):
    store, account, _ = store_with_events
    stream_name = f"account-{account.id}"
    stream = store.aggregated_stream(
        name=identifier(),
        partitioner=MyPartitioner(),
        stream_wildcards=["account-%"],
    )
    with db_engine.connect() as conn:
        stream.truncate(conn)
        conn.commit()
    stream.projector.update_full()

    close_event = _close_event(account)
    store.close_stream(stream=stream_name, message=close_event, expected_version=3)
    stream.projector.update_full()

    store.archive_stream(stream_name, archive_table)

    with store.engine.connect() as conn:
        remaining = list(
            conn.execute(
                stream._table.select().where(
                    stream._table.c.origin_stream == stream_name
                )
            )
        )
    assert len(remaining) == 1
    assert remaining[0].message_id == close_event.event_id


def test_archive_tombstone_prevents_reuse(store_with_events, archive_table):
    store, account, _ = store_with_events
    stream_name = f"account-{account.id}"

    store.close_stream(
        stream=stream_name, message=_close_event(account), expected_version=3
    )
    store.archive_stream(stream_name, archive_table)

    with pytest.raises(StreamClosedError):
        store.write(
            stream=stream_name,
            message=AccountCreditedEvent(account_id=account.id, amount=50, balance=150),
        )


# --- subscription resilience ---


def test_subscription_skips_missing_messages(
    store_with_events, stream_factory, subscription_factory
):
    store, account, _ = store_with_events
    stream_name = f"account-{account.id}"
    stream = stream_factory(store)
    stream.projector.update_full()

    store.close_stream(
        stream=stream_name, message=_close_event(account), expected_version=3
    )
    stream.projector.update_full()

    # Simulate race: delete messages from store while pointers still exist
    with store.engine.connect() as conn:
        conn.execute(
            store._storage.message_table.delete()
            .where(store._storage.message_table.c.stream == stream_name)
            .where(~store._storage.message_table.c.message.has_key("__depeche_closed"))
        )
        conn.commit()

    sub = subscription_factory(stream)
    messages = list(sub.get_next_messages(count=100))
    assert len(messages) > 0  # should not raise KeyError
