import threading
import time

import pytest

from depeche_db._executor import Executor
from depeche_db._interfaces import (
    RunOnNotification,
    RunOnNotificationResult,
    TimeBudget,
)


class FakeHandler(RunOnNotification):
    @property
    def notification_channel(self) -> str:
        return "test_channel"

    def __init__(self, slow=False):
        self.slow = slow
        self.calls = []

    def run(self, budget: TimeBudget):
        self.calls.append(budget)
        if self.slow:
            return RunOnNotificationResult.WORK_REMAINING
        return RunOnNotificationResult.DONE_FOR_NOW

    def stop(self):
        pass


@pytest.fixture
def setup(pg_db):
    obj = Executor(pg_db, stimulation_interval=20)

    def run_executor():
        obj.run()

    executor_thread = threading.Thread(target=run_executor)

    yield executor_thread, obj

    obj._stop()
    executor_thread.join()


def test_basic(setup, pg_db):
    thread, subject = setup
    handler = FakeHandler()
    subject.register(handler)
    thread.start()

    time.sleep(0.3)
    _send_notifications(pg_db, handler.notification_channel, 1)
    time.sleep(1)
    _send_notifications(pg_db, handler.notification_channel, 1)
    time.sleep(0.3)

    assert len(handler.calls) == 3


def test_stimulation(setup):
    thread, subject = setup
    subject.stimulation_interval = 0.1
    handler = FakeHandler()
    subject.register(handler)
    thread.start()
    time.sleep(0.5)
    assert 3 < len(handler.calls) < 6


def test_requeue_when_work_remains(setup):
    thread, subject = setup
    handler = FakeHandler(slow=True)
    subject.register(handler)
    thread.start()
    time.sleep(0.5)
    assert len(handler.calls) > 1


def _send_notifications(pg_db: str, channel: str, count: int):
    import json

    try:
        import psycopg

        pg_db = pg_db.replace("postgresql+psycopg:", "postgresql:")
    except ImportError:
        import psycopg2 as psycopg

    conn = psycopg.connect(pg_db)
    try:
        with conn.cursor() as cursor:
            for i in range(count):
                payload = json.dumps({"message": f"Hello {i}"})
                cursor.execute(f"NOTIFY {channel}, '{payload}'")
                print(f"NOTIFY {channel}, '{payload}'")
            conn.commit()
    finally:
        conn.close()
