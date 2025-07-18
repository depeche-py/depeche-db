import contextlib
import threading
import time

import pytest

from depeche_db._executor import Executor
from depeche_db._interfaces import (
    RunOnNotification,
    RunOnNotificationResult,
    TimeBudget,
)
from depeche_db.experimental.threaded_executor import ThreadedExecutor

EXECUTOR_CLASSES = [Executor, ThreadedExecutor]


class FakeHandler(RunOnNotification):
    @property
    def notification_channel(self) -> str:
        return "test_channel"

    def interested_in_notification(self, notification: dict) -> bool:
        print(f"Checking interest in notification: {notification}")
        return True

    def take_notification_hint(self, notification: dict):
        print(f"Taking notification hint: {notification}")
        self.hints.append(notification)

    def __init__(self, slow=False):
        self.slow = slow
        self.calls = []
        self.hints = []

    def run(self, budget: TimeBudget):
        self.calls.append(budget)
        if self.slow:
            return RunOnNotificationResult.WORK_REMAINING
        return RunOnNotificationResult.DONE_FOR_NOW

    def stop(self):
        pass


@contextlib.contextmanager
def setup(pg_db, subject_class, stimulation_interval=20.0):
    obj = subject_class(
        pg_db, stimulation_interval=stimulation_interval, disable_signals=True
    )

    def run_executor():
        obj.run()

    executor_thread = threading.Thread(target=run_executor)

    try:
        yield executor_thread, obj
    finally:
        obj._stop()
        executor_thread.join()


@pytest.mark.parametrize("subject_class", EXECUTOR_CLASSES)
def test_basic(pg_db, subject_class):
    with setup(pg_db, subject_class) as (thread, subject):
        handler = FakeHandler()
        subject.register(handler)
        thread.start()

        time.sleep(0.3)
        _send_notifications(pg_db, handler.notification_channel, 1)
        time.sleep(1)
        _send_notifications(pg_db, handler.notification_channel, 1)
        time.sleep(0.3)

        assert len(handler.calls) == 3


@pytest.mark.parametrize("subject_class", EXECUTOR_CLASSES)
def test_stimulation(pg_db, subject_class):
    with setup(pg_db, subject_class, stimulation_interval=0.1) as (thread, subject):
        handler = FakeHandler()
        subject.register(handler)
        thread.start()
        time.sleep(0.5)
        assert 3 < len(handler.calls) < 6


@pytest.mark.parametrize("subject_class", EXECUTOR_CLASSES)
def test_no_stimulation(pg_db, subject_class):
    with setup(pg_db, subject_class, stimulation_interval=0) as (thread, subject):
        handler = FakeHandler()
        subject.register(handler)
        thread.start()
        time.sleep(1)
        assert len(handler.calls) == 0


@pytest.mark.parametrize("subject_class", EXECUTOR_CLASSES)
def test_requeue_when_work_remains(pg_db, subject_class):
    with setup(pg_db, subject_class) as (thread, subject):
        handler = FakeHandler(slow=True)
        subject.register(handler)
        thread.start()
        time.sleep(0.5)
        assert len(handler.calls) > 1


@pytest.mark.parametrize("subject_class", EXECUTOR_CLASSES)
def test_no_call_if_not_interested(pg_db, subject_class):
    with setup(pg_db, subject_class, stimulation_interval=0) as (thread, subject):
        handler = FakeHandler()
        handler.interested_in_notification = lambda x: x.get("message") == "Hello 1"  # type: ignore
        subject.register(handler)
        thread.start()

        time.sleep(0.3)
        _send_notifications(pg_db, handler.notification_channel, 1)
        time.sleep(0.5)

        assert len(handler.calls) == 0
        assert len(handler.hints) == 0

        _send_notifications(pg_db, handler.notification_channel, 2)
        time.sleep(0.5)

        assert len(handler.calls) == 1


@pytest.mark.parametrize("subject_class", EXECUTOR_CLASSES)
def test_take_notification_hint(pg_db, subject_class):
    with setup(pg_db, subject_class, stimulation_interval=0) as (thread, subject):
        handler = FakeHandler()
        subject.register(handler)
        thread.start()

        time.sleep(0.3)
        _send_notifications(pg_db, handler.notification_channel, 2)
        time.sleep(0.5)

        assert 1 <= len(handler.calls) <= 2
        assert handler.hints == [{"message": "Hello 0"}, {"message": "Hello 1"}]


def _send_notifications(pg_db: str, channel: str, count: int):
    import json

    try:
        import psycopg

        _psycopg = psycopg

        pg_db = pg_db.replace("postgresql+psycopg:", "postgresql:")
    except ImportError:
        import psycopg2

        _psycopg = psycopg2

    conn = _psycopg.connect(pg_db)
    try:
        with conn.cursor() as cursor:
            for i in range(count):
                payload = json.dumps({"message": f"Hello {i}"})
                cursor.execute(f"NOTIFY {channel}, '{payload}'")
                print(f"NOTIFY {channel}, '{payload}'")
            conn.commit()
    finally:
        conn.close()
