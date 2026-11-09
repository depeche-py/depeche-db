import concurrent.futures as _futures
import logging as _logging
import threading as _threading
from typing import TYPE_CHECKING, Optional

from .._exceptions import PartitionRevoked
from .._interfaces import RunOnNotificationResult, TimeBudget
from .._subscription import AckStrategy, AssignedSubscriptionRunner

if TYPE_CHECKING:
    from .._subscription import Subscription, SubscriptionMessageHandler

LOGGER = _logging.getLogger("depeche_db.threaded_subscription_runner")


class ThreadedSubscriptionRunner(AssignedSubscriptionRunner):
    """
    Experimental runner that processes multiple partitions concurrently in a
    thread pool. Only useful when handlers are IO-heavy (DB queries, HTTP
    calls, etc.) because pure-Python work is bottlenecked by the GIL.

    Each worker thread opens its own implicit connections through the engine
    and processes one partition at a time. Because SQLAlchemy ``Connection``
    objects are not thread-safe, the runner never shares connections between
    threads -- each call to ``get_next_message_batch_for_partition`` inside a
    worker opens its own connection.

    Engine pool sizing: make sure ``pool_size >= max_workers * 2`` on the
    engine you pass to the stream / subscription. The runner logs a warning
    at startup if the pool looks too small.

    Args:
        subscription: Must be configured with
            ``coordination_strategy=CoordinationStrategy.INSTANCE_ASSIGNMENT``.
        message_handler: The handler register wrapper.
        batch_size: Messages per batch per partition, defaults to 10.
        max_workers: Size of the thread pool, defaults to 8.
        heartbeat_interval: Seconds between heartbeats, defaults to 5.
        rebalance_interval: Seconds between rebalance attempts, defaults to 10.
        ack_strategy: SINGLE or BATCHED. Defaults to SINGLE.
        instance_label: Optional human-readable label stored on the instance row.
    """

    def __init__(
        self,
        subscription: "Subscription",
        message_handler: "SubscriptionMessageHandler",
        batch_size: Optional[int] = None,
        max_workers: int = 8,
        heartbeat_interval: float = 5.0,
        rebalance_interval: float = 10.0,
        ack_strategy: AckStrategy = AckStrategy.SINGLE,
        instance_label: Optional[str] = None,
    ):
        super().__init__(
            subscription=subscription,
            message_handler=message_handler,
            batch_size=batch_size,
            heartbeat_interval=heartbeat_interval,
            rebalance_interval=rebalance_interval,
            ack_strategy=ack_strategy,
            instance_label=instance_label,
        )
        self._max_workers = max_workers
        self._executor: Optional[_futures.ThreadPoolExecutor] = None
        self._executor_lock = _threading.Lock()
        self._check_pool_size_once = False

    def _ensure_pool(self) -> _futures.ThreadPoolExecutor:
        with self._executor_lock:
            if self._executor is None:
                self._executor = _futures.ThreadPoolExecutor(
                    max_workers=self._max_workers,
                    thread_name_prefix=(f"depeche-runner-{self._subscription.name}"),
                )
                self._warn_if_pool_too_small()
            return self._executor

    def _warn_if_pool_too_small(self) -> None:
        if self._check_pool_size_once:
            return
        self._check_pool_size_once = True
        try:
            engine = self._subscription._stream._store.engine
            pool = engine.pool
            size = getattr(pool, "_pool", None)
            pool_size = getattr(pool, "size", lambda: None)()
            overflow = getattr(pool, "_max_overflow", None)
            total = (pool_size or 0) + (overflow or 0)
            if pool_size is not None and total < self._max_workers * 2:
                LOGGER.warning(
                    "Engine pool (size=%s, overflow=%s) is small for "
                    "max_workers=%s. Consider pool_size>=%d.",
                    pool_size,
                    overflow,
                    self._max_workers,
                    self._max_workers * 2,
                )
            del size
        except Exception:
            pass

    def stop(self):
        super().stop()
        with self._executor_lock:
            if self._executor is not None:
                self._executor.shutdown(wait=True, cancel_futures=False)
                self._executor = None

    def run_once(self, budget: Optional[TimeBudget] = None) -> RunOnNotificationResult:
        self._ensure_registered()
        if not self._heartbeat_if_due():
            return RunOnNotificationResult.DONE_FOR_NOW
        self._rebalance_if_due()

        assignments = self._refresh_assignments()
        if not assignments:
            return RunOnNotificationResult.DONE_FOR_NOW

        pool = self._ensure_pool()
        futures = {
            pool.submit(
                self._process_partition_threadsafe, partition, generation
            ): partition
            for partition, generation in assignments.items()
        }

        did_work = False
        work_remaining = False
        revoked = False
        for future in _futures.as_completed(futures):
            partition = futures[future]
            try:
                n = future.result()
            except PartitionRevoked:
                LOGGER.info(
                    "Partition %s of subscription %s was revoked",
                    partition,
                    self._subscription.name,
                )
                revoked = True
                continue
            except Exception:
                LOGGER.exception("Worker for partition %s raised", partition)
                raise
            if n > 0:
                did_work = True
                if n >= self._batch_size:
                    work_remaining = True
            if budget and budget.over_budget():
                work_remaining = True
                break
        if revoked:
            self._refresh_assignments(force=True)
        if work_remaining:
            return RunOnNotificationResult.WORK_REMAINING
        if did_work:
            return RunOnNotificationResult.WORK_REMAINING
        return RunOnNotificationResult.DONE_FOR_NOW

    def _process_partition_threadsafe(self, partition: int, generation: int) -> int:
        if not self._keep_running:
            return 0
        return self._process_partition(partition, generation)
