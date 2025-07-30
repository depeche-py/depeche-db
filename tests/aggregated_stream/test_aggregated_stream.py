import datetime as dt

from depeche_db import StreamPartitionStatistic
from depeche_db._aggregated_stream import AggregatedStream


def test_stream_statisitics(db_engine, store_with_events, stream_factory):
    event_store, account, account2 = store_with_events
    subject: AggregatedStream = stream_factory(event_store)
    subject.projector.update_full()

    assert [msg.message_id for msg in subject.read(partition=1)] == [
        evt.event_id for evt in account.events
    ]
    assert [msg.message_id for msg in subject.read(partition=2)] == [
        evt.event_id for evt in account2.events
    ]

    assert list(subject.get_partition_statistics(result_limit=1))[
        0
    ] == StreamPartitionStatistic(
        partition_number=1,
        next_message_id=account.events[0].event_id,
        next_message_position=0,
        next_message_occurred_at=account.events[0].occurred_at,
        max_position=2,
    )


def test_time_to_positions(db_engine, store_with_events, stream_factory):
    event_store, account, account2 = store_with_events
    times = [
        e.get_message_time().replace(tzinfo=dt.timezone.utc)
        for e in sorted(
            account.events + account2.events, key=lambda e: e.get_message_time()
        )
    ]
    subject = stream_factory(event_store)
    subject.projector.update_full()

    for idx, time in enumerate(times):
        print(time, subject.time_to_positions(time))
        assert (
            sum(position for _, position in subject.time_to_positions(time).items())
            == idx
        )


def test_global_position_to_position(db_engine, store_with_events, stream_factory):
    event_store, account, account2 = store_with_events
    subject: AggregatedStream = stream_factory(event_store)
    subject.projector.update_full()

    assert subject.global_position_to_positions(0) == {1: -1, 2: -1}
    assert subject.global_position_to_positions(3) == {1: 1, 2: 0}
    assert subject.global_position_to_positions(5) == {1: 2, 2: 1}


def test_passing_connection(db_engine, store_with_events, stream_factory):
    event_store, _, _ = store_with_events
    subject: AggregatedStream = stream_factory(event_store)
    subject.projector.update_full()
    with db_engine.connect() as conn:
        assert list(subject.read(partition=1)) == list(
            subject.read(partition=1, conn=conn)
        )
        assert list(subject.read_slice(partition=1, start=0, count=2)) == list(
            subject.read_slice(partition=1, start=0, count=2, conn=conn)
        )
        assert list(subject.get_partition_statistics()) == list(
            subject.get_partition_statistics(conn=conn)
        )
