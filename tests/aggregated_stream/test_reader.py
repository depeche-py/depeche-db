def test_reader(store_with_events, stream_factory):
    event_store, *_ = store_with_events
    subject = stream_factory(event_store)
    subject.projector.update_full()

    reader = subject.reader()

    messages = list(reader.get_messages(timeout=2))
    assert len(messages) == 5

    messages = list(reader.get_messages(timeout=2))
    assert len(messages) == 0
