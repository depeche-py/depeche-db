from depeche_db._aggregated_stream import FullUpdateResult
from tests._account_example import AccountCreditedEvent


def test_out_of_order_commits(db_engine, store_factory, stream_factory, account_ids):
    ACCOUNT1_ID, _ = account_ids
    store = store_factory()
    subject = stream_factory(store)

    conn1, conn2 = db_engine.connect(), db_engine.connect()
    msg1, msg2 = msg(ACCOUNT1_ID), msg(ACCOUNT1_ID)
    try:
        # This only works on different streams, because the second write
        # on the same stream would block until the first one is committed.
        store.write(stream="account-a", message=msg1, conn=conn1)
        store.write(stream="account-b", message=msg2, conn=conn2)

        # not committed yet
        assert subject.projector.update_full() == FullUpdateResult(0, False)

        conn2.commit()
        assert subject.projector.update_full() == FullUpdateResult(1, False)

        conn1.commit()
        assert subject.projector.update_full() == FullUpdateResult(1, False)

        # Because of the out-of-order commit, the messages in partition 1
        # are not in the order given by their global position.
        # This tests checks that no messages are lost in this case.
        assert [msg.message_id for msg in subject.read(partition=1)] == [
            msg2.event_id,
            msg1.event_id,
        ]
    finally:
        conn1.close()
        conn2.close()


def test_gaps_in_global_position(db_engine, store_factory, stream_factory, account_ids):
    ACCOUNT1_ID, _ = account_ids
    store = store_factory()
    subject = stream_factory(store)

    with db_engine.connect() as conn:
        store.write(stream="account-a", message=msg(ACCOUNT1_ID), conn=conn)
        conn.rollback()  # global position 1 is now missing
    with db_engine.connect() as conn:
        msg1 = msg(ACCOUNT1_ID)
        store.write(stream="account-a", message=msg1, conn=conn)
        conn.commit()
    with db_engine.connect() as conn:
        store.write(stream="account-a", message=msg(ACCOUNT1_ID), conn=conn)
        conn.rollback()  # global position 3 is now missing
    with db_engine.connect() as conn:
        msg2 = msg(ACCOUNT1_ID)
        store.write(stream="account-a", message=msg2, conn=conn)
        conn.commit()

    assert subject.projector.update_full() == FullUpdateResult(2, False)
    assert [msg.message_id for msg in subject.read(partition=1)] == [
        msg1.event_id,
        msg2.event_id,
    ]


def msg(account_id):
    return AccountCreditedEvent(
        account_id=account_id,
        amount=0,
        balance=0,
    )
