STREAM = "some-stream"


def test_read(subject, events):
    event = events[0]
    subject.write(stream=STREAM, message=event)
    result = list(subject.read(stream=STREAM))
    assert len(result) == 1
    assert result[0].message_id == event.event_id
    assert result[0].message == event


def test_read_own_connection(subject, events, db_engine):
    event = events[0]
    subject.write(stream=STREAM, message=event)
    with db_engine.connect() as conn:
        with subject.reader(conn) as reader:
            result = list(reader.read(stream=STREAM))
    assert len(result) == 1
    assert result[0].message_id == event.event_id
    assert result[0].message == event
