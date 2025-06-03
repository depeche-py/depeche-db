import uuid as _uuid

import pytest

from depeche_db import (
    MessageIdMismatchError,
    MessagePosition,
    OptimisticConcurrencyError,
)

from ._my_event import MyEvent

STREAM = "some-stream"


def test_write(subject, events):
    result = subject.write(stream=STREAM, message=events[0])
    assert result == MessagePosition(stream=STREAM, version=1, global_position=1)


def test_write_with_connection(db_engine, subject, events):
    with db_engine.connect() as conn:
        result = subject.write(stream=STREAM, message=events[0], conn=conn)
        assert result == MessagePosition(stream=STREAM, version=1, global_position=1)

        # before commit, reading through another connection should not see the message
        assert len(list(subject.read(STREAM))) == 0

        conn.commit()
        # now we should see the message
        assert len(list(subject.read(STREAM))) == 1


def test_write_concurrency_failure_empty(subject, events):
    with pytest.raises(OptimisticConcurrencyError):
        subject.write(stream=STREAM, message=events[0], expected_version=1)


def test_write_concurrency_failure(subject, events):
    subject.write(stream=STREAM, message=events[0], expected_version=0)
    with pytest.raises(OptimisticConcurrencyError):
        subject.write(stream=STREAM, message=events[0], expected_version=0)
    with pytest.raises(OptimisticConcurrencyError):
        subject.write(stream=STREAM, message=events[0], expected_version=2)


def test_synchronize_idempotency(subject, events):
    result = subject.synchronize(
        stream=STREAM, expected_version=0, messages=events[:-1]
    )
    assert len(list(subject.read(STREAM))) == 3
    assert result == MessagePosition(stream=STREAM, version=3, global_position=3)

    result = subject.synchronize(
        stream=STREAM, expected_version=0, messages=events[:-1]
    )
    assert len(list(subject.read(STREAM))) == 3
    assert result == MessagePosition(stream=STREAM, version=3, global_position=3)

    result = subject.synchronize(stream=STREAM, expected_version=3, messages=events)
    assert len(list(subject.read(STREAM))) == 4
    assert result == MessagePosition(stream=STREAM, version=4, global_position=4)


def test_synchronize_id_mismatch(subject, events):
    subject.synchronize(stream=STREAM, expected_version=0, messages=events)

    with pytest.raises(MessageIdMismatchError):
        subject.synchronize(
            stream=STREAM,
            expected_version=4,
            messages=[MyEvent(event_id=_uuid.uuid4(), num=1)] + events[1:],
        )


def test_synchronize_with_connection(db_engine, subject, events):
    with db_engine.connect() as conn:
        subject.synchronize(
            stream=STREAM, expected_version=0, messages=events, conn=conn
        )

        # before commit, reading through another connection should not see the messages
        assert len(list(subject.read(STREAM))) == 0

        conn.commit()

        # now we should see the messages
        assert len(list(subject.read(STREAM))) == 4


def test_synchronize_fails_on_missing_message(subject, events):
    subject.synchronize(stream=STREAM, expected_version=0, messages=events)
    with pytest.raises(MessageIdMismatchError):
        subject.synchronize(stream=STREAM, expected_version=4, messages=events[1:])


def test_truncate(subject, events):
    subject.write(stream=STREAM, message=events[0])
    assert len(list(subject.read(STREAM))) == 1

    subject.truncate()
    assert len(list(subject.read(STREAM))) == 0
