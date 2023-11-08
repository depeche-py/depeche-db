import threading as _threading
import uuid as _uuid


def test_concurrent_writes_to_separate_streams_work(db_engine, storage):
    with db_engine.connect() as conn1, db_engine.connect() as conn2:
        subject = storage

        subject.add(conn1, "stream1", 0, _uuid.uuid4(), {"foo": "bar"})
        subject.add(conn2, "stream2", 0, _uuid.uuid4(), {"foo": "bar"})

        conn1.commit()
        conn2.commit()


def test_concurrent_writes_to_same_stream_fail(db_engine, storage):
    N = 5
    success = []

    def writer(n):
        with db_engine.connect() as conn:
            subject = storage
            try:
                subject.add(
                    conn=conn,
                    stream="stream1",
                    expected_version=0,
                    message_id=_uuid.uuid4(),
                    message={"foo": "bar"},
                )
                conn.commit()
                success.append(True)
            except Exception:
                pass

    threads = [_threading.Thread(target=writer, args=(i,)) for i in range(N)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(success) == 1


def test_multiple_writes_do_not_interfere(db_engine, storage):
    N = 5
    success = []

    def write():
        for _ in range(20):
            with db_engine.connect() as conn:
                subject = storage

                subject.add(
                    conn=conn,
                    stream="stream1",
                    expected_version=None,
                    message_id=_uuid.uuid4(),
                    message={"foo": "bar"},
                )
                conn.commit()
        success.append(True)

    threads = [_threading.Thread(target=write) for _ in range(N)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(success) == N
