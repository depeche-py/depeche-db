import threading as _threading

from depeche_db._executor import HandlerQueue


def test_put():
    queue = HandlerQueue()
    queue.put(lambda: None)
    assert len(queue) == 1


def test_put_no_duplicates():
    def handler():
        return None

    queue = HandlerQueue()
    queue.put(handler)
    queue.put(handler)
    assert len(queue) == 1


def test_put_sets_event():
    queue = HandlerQueue()
    assert queue.should_wait
    queue.put(lambda: None)
    assert not queue.should_wait


def test_get():
    def handler():
        return None

    queue = HandlerQueue()
    queue.put(handler)
    assert queue.get() is handler
    assert len(queue) == 0


def test_wait():
    def handler():
        return None

    queue = HandlerQueue()

    def wait():
        print("Waiting")
        queue.wait()
        print("done Waiting")

    thread = _threading.Thread(target=wait, daemon=True)
    thread.start()

    queue.put(handler)

    thread.join(timeout=0.1)
    assert not thread.is_alive()
