import threading as _threading
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


def test_multiple_writes_do_not_interfere(db_engine, storage):
    N = 5
    success = []

    def write():
        for _ in range(20):
            with db_engine.connect() as conn:
                subject = storage

                subject.add(conn, "stream1", None, _uuid.uuid4(), {"foo": "bar"})
                conn.commit()
        success.append(True)

    threads = [_threading.Thread(target=write) for _ in range(N)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(success) == N
