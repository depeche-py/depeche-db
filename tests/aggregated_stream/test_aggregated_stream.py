import datetime as dt

from depeche_db import StreamPartitionStatistic


def test_stream_statisitics(db_engine, store_with_events, stream_factory):
    event_store, account, account2 = store_with_events
    subject = stream_factory(event_store)
    subject.projector.update_full()

    assert [msg.message_id for msg in subject.read(partition=1)] == [
        evt.event_id for evt in account.events
    ]
    assert [msg.message_id for msg in subject.read(partition=2)] == [
        evt.event_id for evt in account2.events
    ]

    assert list(
        subject.get_partition_statistics(position_limits={1: 1}, result_limit=1)
    )[0] == StreamPartitionStatistic(
        partition_number=2,
        next_message_id=account2.events[0].event_id,
        next_message_position=0,
        next_message_occurred_at=account2.events[0].occurred_at,
        max_position=1,
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
