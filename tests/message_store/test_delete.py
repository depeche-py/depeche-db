def test_read_deleted(subject, events):
    for msg in events:
        subject.write(stream="a", message=msg)

    assert subject.delete(stream="a", keep_versions_greater_than=2) == 2

    with subject.reader() as reader:
        result = list(reader.read(stream="a"))

    assert len(result) == 2
