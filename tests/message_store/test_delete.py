import pytest

from depeche_db import CannotWriteToDeletedStream, LastMessageCannotBeDeleted


def test_delete_all_but_last(subject, events):
    for msg in events:
        subject.write(stream="a", message=msg)

    subject.delete(stream="a", keep_versions_greater_than=-1)

    with subject.reader() as reader:
        result = list(reader.read(stream="a"))

    assert [msg.message for msg in result] == events[-1:]


def test_delete_all_but_last_two(subject, events):
    for msg in events:
        subject.write(stream="a", message=msg)

    subject.delete(stream="a", keep_versions_greater_than=-2)

    with subject.reader() as reader:
        result = list(reader.read(stream="a"))

    assert [msg.message for msg in result] == events[-2:]


def test_cant_write_after_delete(subject, events):
    for msg in events[:-1]:
        subject.write(stream="a", message=msg)

    subject.delete(stream="a", keep_versions_greater_than=2)

    with pytest.raises(CannotWriteToDeletedStream):
        subject.write(stream="a", message=events[-1])


def test_cant_delete_last_or_greater(subject, events):
    for msg in events:
        subject.write(stream="a", message=msg)

    with pytest.raises(LastMessageCannotBeDeleted):
        subject.delete(stream="a", keep_versions_greater_than=4)

    with pytest.raises(LastMessageCannotBeDeleted):
        subject.delete(stream="a", keep_versions_greater_than=5)


def test_read_deleted(subject, events):
    for msg in events:
        subject.write(stream="a", message=msg)

    assert subject.delete(stream="a", keep_versions_greater_than=2) == 2

    with subject.reader() as reader:
        result = list(reader.read(stream="a"))

    assert [msg.message for msg in result] == events[2:]
