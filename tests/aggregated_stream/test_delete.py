def test_deleted_events_in_stream(db_engine, store_with_events, stream_factory):
    event_store, account, account2 = store_with_events
    subject = stream_factory(event_store)
    subject.projector.update_full()

    event_store.delete(stream=f"account-{account.id}", keep_versions_greater_than=2)

    assert len(list(subject.loaded_reader.read(1))) == 3
