import datetime as _dt
import time as _time
import uuid as _uuid
from unittest import mock as _mock

import pytest

from depeche_db import OptimisticConcurrencyError


def test_write(db_engine, storage):
    with db_engine.connect() as conn:
        subject = storage

        id1 = _uuid.uuid4()
        result = subject.add(conn, "stream1", 0, id1, {"foo": "bar1"})
        assert result.version == 1

        id2 = _uuid.uuid4()
        result = subject.add(conn, "stream2", 0, id2, {"foo": "bar2"})
        assert result.version == 1

        id3 = _uuid.uuid4()
        result = subject.add(conn, "stream1", 1, id3, {"foo": "bar3"})
        assert result.version == 2

        table_content = conn.execute(
            storage.message_table.select().order_by(
                storage.message_table.c.global_position
            )
        ).fetchall()
        assert len(table_content) == 3
        assert [row.global_position for row in table_content] == [1, 2, 3]
        assert [(row.message_id, row.stream, row.version) for row in table_content] == [
            (id1, "stream1", 1),
            (id2, "stream2", 1),
            (id3, "stream1", 2),
        ]
        assert [row.message for row in table_content] == [
            {"foo": "bar1"},
            {"foo": "bar2"},
            {"foo": "bar3"},
        ]


def test_write_multiple(db_engine, storage):
    with db_engine.connect() as conn:
        subject = storage

        id1 = _uuid.uuid4()
        result = subject.add_all(conn, "stream1", 0, [(id1, {"foo": "bar1"})])
        assert result.version == 1

        id2 = _uuid.uuid4()
        id3 = _uuid.uuid4()
        result = subject.add_all(
            conn, "stream1", 1, [(id2, {"foo": "bar1"}), (id3, {"foo": "bar2"})]
        )
        assert result.version == 3


def test_write_multiple_no_expected_version(db_engine, storage):
    with db_engine.connect() as conn:
        subject = storage

        id1 = _uuid.uuid4()
        id2 = _uuid.uuid4()
        result = subject.add_all(
            conn, "stream1", None, [(id1, {"foo": "bar1"}), (id2, {"foo": "bar2"})]
        )
        assert result.version == 2


def test_write_concurrency_failure(db_engine, storage):
    with db_engine.connect() as conn:
        subject = storage

        id1 = _uuid.uuid4()
        result = subject.add(conn, "stream1", 0, id1, {"foo": "bar1"})
        assert result.version == 1

        id2 = _uuid.uuid4()
        with pytest.raises(OptimisticConcurrencyError):
            result = subject.add(conn, "stream1", 0, id2, {"foo": "bar2"})


def test_read_streams(db_engine, storage):
    with db_engine.connect() as conn:
        subject = storage

        id1 = _uuid.uuid4()
        subject.add(conn, "stream1", 0, id1, {"foo": "bar1"})
        id2 = _uuid.uuid4()
        subject.add(conn, "stream2", 0, id2, {"foo": "bar2"})
        id3 = _uuid.uuid4()
        subject.add(conn, "stream1", 1, id3, {"foo": "bar3"})

        assert set(subject.get_message_ids(conn, "stream1")) == {id1, id3}
        result = list(subject.read(conn, "stream1"))
        assert result == [
            (id1, 1, {"foo": "bar1"}, 1, _mock.ANY),
            (id3, 2, {"foo": "bar3"}, 3, _mock.ANY),
        ]
        assert isinstance(result[0][4], _dt.datetime)

        assert list(subject.read_multiple(conn, ["stream1", "stream2"])) == [
            (id1, "stream1", 1, {"foo": "bar1"}, 1, _mock.ANY),
            (id2, "stream2", 1, {"foo": "bar2"}, 2, _mock.ANY),
            (id3, "stream1", 2, {"foo": "bar3"}, 3, _mock.ANY),
        ]

        assert list(subject.read_wildcard(conn, "stream%")) == [
            (id1, "stream1", 1, {"foo": "bar1"}, 1, _mock.ANY),
            (id2, "stream2", 1, {"foo": "bar2"}, 2, _mock.ANY),
            (id3, "stream1", 2, {"foo": "bar3"}, 3, _mock.ANY),
        ]


def test_read_messages(db_engine, storage):
    with db_engine.connect() as conn:
        subject = storage

        id1 = _uuid.uuid4()
        subject.add(conn, "stream1", 0, id1, {"foo": "bar1"})
        id2 = _uuid.uuid4()
        subject.add(conn, "stream2", 0, id2, {"foo": "bar2"})
        id3 = _uuid.uuid4()
        subject.add(conn, "stream1", 1, id3, {"foo": "bar3"})

        result = subject.get_message_by_id(conn, id1)
        assert result == (
            id1,
            "stream1",
            1,
            {"foo": "bar1"},
            1,
            _mock.ANY,
        )
        assert isinstance(result[5], _dt.datetime)

        result = sorted(subject.get_messages_by_ids(conn, [id1, id2]))
        assert result == sorted(
            [
                (id1, "stream1", 1, {"foo": "bar1"}, 1, _mock.ANY),
                (id2, "stream2", 1, {"foo": "bar2"}, 2, _mock.ANY),
            ]
        )
        assert isinstance(result[0][5], _dt.datetime)


def test_truncate(db_engine, storage):
    with db_engine.connect() as conn:
        subject = storage
        id1 = _uuid.uuid4()
        subject.add(conn, "stream1", 0, id1, {"foo": "bar1"})

        subject.truncate(conn)

        table_content = conn.execute(storage.message_table.select()).fetchall()
        assert len(table_content) == 0


def test_write_performance(db_engine, storage):
    N = 2000
    start = _time.time()
    with db_engine.connect() as conn:
        storage.add_all(
            conn, "stream1", 0, [(_uuid.uuid4(), {"foo": "bar1"}) for _ in range(N)]
        )
        conn.commit()
    end = _time.time()
    rate = N / (end - start)
    print(f"Write performance: {rate:.2f} messages/s")
    assert rate > 500  # Low value for CI


def test_get_global_position(db_engine, storage):
    with db_engine.connect() as conn:
        subject = storage
        assert subject.get_global_position(conn) == 0
        subject.add(conn, "stream1", 0, _uuid.uuid4(), {"foo": "bar1"})
        assert subject.get_global_position(conn) == 1
