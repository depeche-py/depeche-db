from depeche_db._executor import UniqueQueue


def test_put_no_duplicates():
    def handler():
        return None

    queue = UniqueQueue()
    queue.put(handler)
    queue.put(handler)
    assert len(queue.queue) == 1
