import uuid as _uuid

import pytest

from depeche_db._exceptions import MessageNotFound

STREAM = "some-stream"


def test_read(subject, events):
    event = events[0]
    subject.write(stream=STREAM, message=event)

    result = list(subject.read(stream=STREAM))

    assert len(result) == 1
    assert result[0].message_id == event.event_id
    assert result[0].message == event


def test_reader(subject, events, db_engine):
    event = events[0]
    subject.write(stream=STREAM, message=event)

    with subject.reader() as reader:
        result = list(reader.read(stream=STREAM))

    assert len(result) == 1
    assert result[0].message_id == event.event_id
    assert result[0].message == event


def test_reader_own_connection(subject, events, db_engine):
    event = events[0]
    subject.write(stream=STREAM, message=event)

    with db_engine.connect() as conn:
        with subject.reader(conn) as reader:
            result = list(reader.read(stream=STREAM))

    assert len(result) == 1
    assert result[0].message_id == event.event_id
    assert result[0].message == event


def test_read_wildcard(subject, events):
    subject.write(stream=STREAM, message=events[0])
    subject.write(stream="other-stream", message=events[1])
    subject.write(stream="foo", message=events[2])

    with subject.reader() as reader:
        result = list(reader.read_wildcard(stream_wildcard="%-stream"))

    assert len(result) == 2
    assert {r.message_id for r in result} == {events[0].event_id, events[1].event_id}


def test_get_messages_by_ids(subject, events):
    subject.synchronize(stream=STREAM, expected_version=0, messages=events)

    with subject.reader() as reader:
        result = list(
            reader.get_messages_by_ids([events[0].event_id, events[3].event_id])
        )

    assert len(result) == 2
    assert {r.message_id for r in result} == {events[0].event_id, events[3].event_id}


def test_get_message_by_id(subject, events):
    event = events[0]
    subject.synchronize(stream=STREAM, expected_version=0, messages=events)

    with subject.reader() as reader:
        result = reader.get_message_by_id(event.event_id)

    assert result.message_id == event.event_id
    assert result.message == event


def test_get_message_by_id_not_found(subject, events):
    subject.synchronize(stream=STREAM, expected_version=0, messages=events)

    with pytest.raises(MessageNotFound):
        with subject.reader() as reader:
            reader.get_message_by_id(_uuid.uuid4())
