import threading as _threading
from typing import Dict

import pals as _pals
import sqlalchemy as _sa


class DbLockProvider:
    def __init__(self, name: str, engine: _sa.engine.Engine):
        assert name.isidentifier(), "Name must be a valid identifier"
        self._engine = engine
        self.locker = _pals.Locker(
            app_name=name,
            create_engine_callable=lambda: self._engine,
            blocking_default=False,
        )
        self._thread_lock = _threading.Lock()
        self._locks: Dict[str, _pals.Lock] = {}  # type: ignore

    def lock(self, name: str) -> bool:
        # assert name not in self._locks, "Lock already acquired"
        with self._thread_lock:
            if name in self._locks:
                return False
            lock = self._locks[name] = self.locker.lock(name, blocking=False)
        result = lock.acquire()
        if not result:
            if lock.conn:
                lock.conn.close()
            with self._thread_lock:
                del self._locks[name]
        return result  # type: ignore

    def unlock(self, name: str):
        with self._thread_lock:
            lock = self._locks.pop(name)
            lock.release()

    def finalize(self):
        try:
            self.locker.engine.pool.dispose()
        except Exception:
            pass

    def __del__(self):
        self.finalize()
