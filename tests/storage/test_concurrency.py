import uuid as _uuid

import pytest


def test_concurrent_writes_to_separate_streams_work(db_engine, storage):
    with db_engine.connect() as conn1, db_engine.connect() as conn2:
        subject = storage

        subject.add(conn1, "stream1", 0, _uuid.uuid4(), {"foo": "bar"})
        subject.add(conn2, "stream2", 0, _uuid.uuid4(), {"foo": "bar"})

        conn1.commit()
        conn2.commit()


@pytest.mark.skip("TODO how to test (does not terminate)")
def test_concurrent_writes_to_same_stream_fail(db_engine, storage):
    with db_engine.connect() as conn1, db_engine.connect() as conn2:
        subject = storage

        subject.add(conn1, "stream1", 0, _uuid.uuid4(), {"foo": "bar"})
        subject.add(conn2, "stream1", 0, _uuid.uuid4(), {"foo": "bar"})

        conn1.commit()
        conn2.commit()
