import threading as _threading
import uuid as _uuid

import pytest
import sqlalchemy as _sa

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


def test_stream_projector_cutoff(db_engine, store_factory, stream_factory, account_ids):
    ACCOUNT1_ID, ACCOUNT2_ID = account_ids
    store = store_factory()
    subject = stream_factory(store)
    # TODO
    # create events
    account_repo = AccountRepository(store)

    account = Account.register(id=ACCOUNT1_ID, owner_id=_uuid.uuid4(), number="123")
    account.credit(100)
    account_repo.save(account, expected_version=0)

    orig_projector_add = subject.projector.add

    def slow_projector_add(conn, messages):
        import time

        time.sleep(0.2)
        orig_projector_add(conn, messages)

    subject.projector.add = slow_projector_add

    # start update_full in another thread
    thread = _threading.Thread(target=subject.projector.update_full)
    thread.start()

    # add more events
    account.credit(100)
    account_repo.save(account, expected_version=2)

    # wait for update_full to finish
    thread.join()

    # assert that the new events are not in the projection
    with db_engine.connect() as conn:
        assert account.events[-1].event_id not in [
            msg.message_id for msg in subject.read(conn, partition=1)
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
    with db_engine.connect() as conn:
        assert {msg.message_id for msg in stream.read(conn, partition=1)}.union(
            {msg.message_id for msg in stream.read(conn, partition=2)}
        ) == {event.event_id for event in account.events + account2.events}
        assert [msg.message_id for msg in stream.read(conn, partition=1)] == [
            event.event_id for event in account.events
        ]
        assert [msg.message_id for msg in stream.read(conn, partition=2)] == [
            event.event_id for event in account2.events
        ]