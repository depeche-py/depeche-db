from depeche_db import DeletedAggregatedStreamMessage, LoadedAggregatedStreamMessage


def test_deleted_events_in_stream(db_engine, store_with_events, stream_factory):
    event_store, account, account2 = store_with_events
    subject = stream_factory(event_store)
    subject.projector.update_full()

    event_store.delete(stream=f"account-{account.id}", keep_versions_greater_than=2)

    messages = list(subject.loaded_reader.read(1))
    assert messages[:-1] == [
        DeletedAggregatedStreamMessage(
            message_id=account.events[0].event_id, partition=1, position=0
        ),
        DeletedAggregatedStreamMessage(
            message_id=account.events[1].event_id, partition=1, position=1
        ),
    ]
    assert type(messages[-1]) == LoadedAggregatedStreamMessage


def test_case():
    # TODO:
    #  * Add 1 message to a stream
    #  * set up aggregated stream and update it
    #  * add 2 more messages
    #  * delete all but the last message
    #  * update aggregated stream
    # -> what happened?!
    pass
