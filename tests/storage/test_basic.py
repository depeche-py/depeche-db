import uuid as _uuid

import pytest

from depeche_db import (
    Storage,
)


def test_storage(db_engine, storage):
    with db_engine.connect() as conn:
        subject = storage

        id1 = _uuid.uuid4()
        result = subject.add(conn, "stream1", 0, id1, {"foo": "bar"})
        assert result.version == 1

        id3 = _uuid.uuid4()
        result = subject.add(conn, "stream2", 0, id3, {"foo": "bar"})
        assert result.version == 1

        id2 = _uuid.uuid4()
        result = subject.add(conn, "stream1", 1, id2, {"foo": "bar"})
        assert result.version == 2

        assert list(subject.read(conn, "stream1")) == [
            (id1, 1, {"foo": "bar"}, 1),
            (id2, 2, {"foo": "bar"}, 3),
        ]

        assert list(subject.read_multiple(conn, ["stream1", "stream2"])) == [
            (id1, "stream1", 1, {"foo": "bar"}, 1),
            (id3, "stream2", 1, {"foo": "bar"}, 2),
            (id2, "stream1", 2, {"foo": "bar"}, 3),
        ]

        assert list(subject.read_wildcard(conn, "stream%")) == [
            (id1, "stream1", 1, {"foo": "bar"}, 1),
            (id3, "stream2", 1, {"foo": "bar"}, 2),
            (id2, "stream1", 2, {"foo": "bar"}, 3),
        ]
