from depeche_db import (
    StreamPartitionStatistic,
)


def test_stream_statisitics(db_engine, store_with_events, stream_factory):
    event_store, account, account2 = store_with_events
    subject = stream_factory(event_store)
    subject.projector.update_full()

    with db_engine.connect() as conn:
        assert list(subject.read(conn, partition=1)) == [
            evt.event_id for evt in account.events
        ]
        assert list(subject.read(conn, partition=2)) == [
            evt.event_id for evt in account2.events
        ]
    assert list(
        subject.get_partition_statistics(position_limits={1: 1}, result_limit=1)
    )[0] == StreamPartitionStatistic(
        partition_number=2,
        next_message_id=account2.events[0].event_id,
        next_message_position=0,
        next_message_occurred_at=account2.events[0].occurred_at,
    )
