import datetime as _dt
import enum as _enum
import logging as _logging
import random as _random
import threading as _threading
import time as _time
import uuid as _uuid
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)

from . import tools as _tools
from ._aggregated_stream import AggregatedStream
from ._compat import SAConnection
from ._exceptions import PartitionRevoked
from ._interfaces import (
    CallMiddleware,
    ErrorAction,
    LockProvider,
    MessageHandlerRegisterProtocol,
    MessageProtocol,
    PartitionAssignmentProvider,
    RunOnNotificationResult,
    StoredMessage,
    SubscriptionErrorHandler,
    SubscriptionMessage,
    SubscriptionMessageBatch,
    SubscriptionStartPoint,
    SubscriptionStateProvider,
    TimeBudget,
)

E = TypeVar("E", bound=MessageProtocol)

DEPECHE_LOGGER = _logging.getLogger("depeche_db")


class ExitSubscriptionErrorHandler(SubscriptionErrorHandler):
    """
    Exit the subscription on error
    """

    def handle_error(
        self, error: Exception, message: SubscriptionMessage[E]
    ) -> ErrorAction:
        return ErrorAction.EXIT


class LogAndIgnoreSubscriptionErrorHandler(SubscriptionErrorHandler):
    """
    Log the error and ignore the message
    """

    def __init__(self, subscription_name: str):
        self._logger = _logging.getLogger(
            f"depeche_db.subscription.{subscription_name}"
        )

    def handle_error(
        self, error: Exception, message: SubscriptionMessage[E]
    ) -> ErrorAction:
        self._logger.exception(
            f"Error while handling message {message.stored_message.message_id}:{message.stored_message.message.__class__.__name__}"
        )
        return ErrorAction.IGNORE


class AckStrategy(_enum.Enum):
    SINGLE = "single"
    BATCHED = "batched"


class NoAckOp:
    def execute(self, **kwargs):
        raise RuntimeError(
            "NoAckOp cannot be executed, choose AckStrategy.SINGLE if you want to use this operation"
        )


class AckRolledback(Exception):
    pass


class AckOp:
    def __init__(
        self,
        name: str,
        partition: int,
        position: int,
        state_provider: SubscriptionStateProvider,
        expected_generation: Optional[int] = None,
        expected_instance_id: Optional[_uuid.UUID] = None,
        assignment_table: Optional[Any] = None,
    ):
        self.name = name
        self.partition = partition
        self.position = position
        self._state_provider = state_provider
        self._expected_generation = expected_generation
        self._expected_instance_id = expected_instance_id
        self._assignment_table = assignment_table
        self._executed = False
        self._rolled_back = False

    def execute(self, **subscription_state_provider_kwargs):
        if self._executed:
            self._check()
            return

        if subscription_state_provider_kwargs:
            provider = self._state_provider.session(
                **subscription_state_provider_kwargs
            )
        else:
            provider = self._state_provider

        store_kwargs: Dict[str, Any] = {
            "subscription_name": self.name,
            "partition": self.partition,
            "position": self.position,
        }
        if self._expected_generation is not None:
            store_kwargs["expected_generation"] = self._expected_generation
            store_kwargs["expected_instance_id"] = self._expected_instance_id
            store_kwargs["assignment_table"] = self._assignment_table

        self._executed = True
        provider.store(**store_kwargs)

    def _check(self):
        state = self._state_provider.read(self.name)
        if state.positions.get(self.partition, -1) != self.position:
            raise AckRolledback()

    def rollback(self):
        self._rolled_back = True

    @property
    def rolled_back(self):
        return self._rolled_back


class CoordinationStrategy(_enum.Enum):
    """
    Strategy for coordinating multiple subscription instances.

    - ``ADVISORY_LOCK`` (default): each poll, an instance probes per-partition
      Postgres advisory locks to claim partitions. Fully dynamic, no setup,
      but chatty under load.
    - ``INSTANCE_ASSIGNMENT``: instances register in a table with a
      heartbeat; a leader periodically reassigns partitions for longer
      periods. Requires a
      [PartitionAssignmentProvider][depeche_db.PartitionAssignmentProvider].
    """

    ADVISORY_LOCK = "advisory_lock"
    INSTANCE_ASSIGNMENT = "instance_assignment"


class Subscription(Generic[E]):
    _state_provider: SubscriptionStateProvider
    runner: "Union[SubscriptionRunner[E], BatchedAckSubscriptionRunner[E], AssignedSubscriptionRunner[E]]"

    def __init__(
        self,
        name: str,
        stream: AggregatedStream[E],
        message_handler: "SubscriptionMessageHandler[E]",
        batch_size: Optional[int] = None,
        state_provider: Optional[SubscriptionStateProvider] = None,
        lock_provider: Optional[LockProvider] = None,
        start_point: Optional[SubscriptionStartPoint] = None,
        ack_strategy: AckStrategy = AckStrategy.SINGLE,
        coordination_strategy: CoordinationStrategy = CoordinationStrategy.ADVISORY_LOCK,
        assignment_provider: Optional[PartitionAssignmentProvider] = None,
        assignment_table: Optional[Any] = None,
        runner_class: Optional[type] = None,
        runner_options: Optional[Dict[str, Any]] = None,
    ):
        """
        A subscription is a way to read messages from an aggregated stream.

        Read more about the subscription in the [concepts section](../concepts/subscriptions.md).

        Args:
            name: Name of the subscription, needs to be a valid python identifier
            stream: Stream to read from
            message_handler: Handler for the messages
            batch_size: Number of messages to read at once, defaults to 10, read more [here][depeche_db.SubscriptionRunner]
            state_provider: Provider for the subscription state, defaults to a PostgreSQL provider
            lock_provider: Provider for the locks, defaults to a PostgreSQL provider
            start_point: The start point for the subscription, defaults to beginning of the stream
            ack_strategy: The strategy to use for acknowledging messages, defaults to AckStrategy.SINGLE.
            coordination_strategy: How multiple instances coordinate, defaults to ADVISORY_LOCK.
            assignment_provider: Required when ``coordination_strategy=INSTANCE_ASSIGNMENT``. Defaults to a Postgres-backed provider using the same engine as the stream.
            assignment_table: The SQLAlchemy table used for generation fencing on position writes. Defaults to the one on ``assignment_provider``.
            runner_class: Override the runner class (e.g. ``ThreadedSubscriptionRunner``). Defaults pick based on the coordination/ack strategy.
            runner_options: Extra keyword arguments passed to the runner constructor.
        """
        assert name.isidentifier(), "Group name must be a valid identifier"
        self.name = name
        self._stream = stream
        self._lock_provider = lock_provider or _tools.DbLockProvider(
            name, self._stream._store.engine
        )
        self._state_provider = state_provider or _tools.DbSubscriptionStateProvider(
            name, self._stream._store.engine
        )
        self._start_point = start_point
        self._coordination_strategy = coordination_strategy
        self._assignment_provider: Optional[
            PartitionAssignmentProvider
        ] = assignment_provider
        self._assignment_table = assignment_table
        if coordination_strategy == CoordinationStrategy.INSTANCE_ASSIGNMENT:
            if self._assignment_provider is None:
                from .tools import DbPartitionAssignmentProvider

                provider = DbPartitionAssignmentProvider(
                    name=name, engine=self._stream._store.engine
                )
                self._assignment_provider = provider
                if self._assignment_table is None:
                    self._assignment_table = provider.assignments_table
            elif self._assignment_table is None:
                self._assignment_table = getattr(
                    self._assignment_provider, "assignments_table", None
                )
        runner_options = dict(runner_options or {})
        if batch_size is not None:
            runner_options.setdefault("batch_size", batch_size)

        if runner_class is not None:
            self.runner = runner_class(
                subscription=self,
                message_handler=message_handler,
                **runner_options,
            )
        elif coordination_strategy == CoordinationStrategy.INSTANCE_ASSIGNMENT:
            self.runner = AssignedSubscriptionRunner(
                subscription=self,
                message_handler=message_handler,
                ack_strategy=ack_strategy,
                **runner_options,
            )
        elif ack_strategy == AckStrategy.BATCHED:
            self.runner = BatchedAckSubscriptionRunner(
                subscription=self,
                message_handler=message_handler,
                **runner_options,
            )
        elif ack_strategy == AckStrategy.SINGLE:
            self.runner = SubscriptionRunner(
                subscription=self,
                message_handler=message_handler,
                **runner_options,
            )
        else:
            raise NotImplementedError(f"Ack strategy {ack_strategy} is not implemented")

        self._max_aggregated_stream_positions_cache: Dict[int, int] = {}

    def _init_state(self):
        if not self._state_provider.initialized(self.name):
            lock_key = f"subscription-{self.name}-init"
            if not self._lock_provider.lock(lock_key):
                # another instance is already initializing the state
                # wait until it is initialized
                while not self._state_provider.initialized(self.name):
                    _time.sleep(0.05)
                return
            try:
                if self._start_point is not None:
                    self._start_point.init_state(
                        subscription_name=self.name,
                        stream=self._stream,
                        state_provider=self._state_provider,
                    )
                self._state_provider.initialize(self.name)
            finally:
                self._lock_provider.unlock(lock_key)

    def _update_max_aggregated_stream_positions_cache(
        self, partition_number: int, max_position: int
    ) -> None:
        """
        Updates the cache of max aggregated stream positions for a specific partition.
        This is used to keep the cache up-to-date when new messages are added to the stream.
        """
        current_value = self._max_aggregated_stream_positions_cache.get(
            partition_number, -1
        )
        self._max_aggregated_stream_positions_cache[partition_number] = max(
            max_position, current_value
        )

    def _get_next_partitions(self, conn: SAConnection) -> List[int]:
        state = self._state_provider.read(self.name)

        def _refresh_max_aggregated_stream_positions_cache():
            self._max_aggregated_stream_positions_cache = (
                self._stream.get_max_aggregated_stream_positions(conn=conn)
            )

        def _calculate_unprocessed_message_counts():
            if not self._max_aggregated_stream_positions_cache:
                return {}

            unprocessed_message_counts = []
            for (
                partition_number,
                max_position,
            ) in self._max_aggregated_stream_positions_cache.copy().items():
                current_position = state.positions.get(partition_number, -1)
                if current_position < max_position:
                    # there are still messages to read in this partition
                    unprocessed_message_counts.append(
                        (max_position - current_position, partition_number)
                    )
            return unprocessed_message_counts

        unprocessed_message_counts = _calculate_unprocessed_message_counts()
        if not unprocessed_message_counts:
            _refresh_max_aggregated_stream_positions_cache()
            unprocessed_message_counts = _calculate_unprocessed_message_counts()

        # Take the top 20 partitions with the most messages to read
        result = [
            partition_number
            for _, partition_number in sorted(
                unprocessed_message_counts,
                reverse=True,
            )
        ]
        result = result[:20]
        # Shuffle the partitions to avoid reading them in the same order on multiple instances
        _random.shuffle(result)
        return result

    def get_next_message_batch(
        self, count: int
    ) -> Optional[SubscriptionMessageBatch[E]]:
        if not self._state_provider.initialized(self.name):
            self._init_state()
        assert self._state_provider.initialized(self.name)

        with self._stream._store.engine.connect() as conn:
            for partition_number in self._get_next_partitions(conn=conn):
                lock_key = f"subscription-{self.name}-{partition_number}"
                if not self._lock_provider.lock(lock_key):
                    continue

                # now we have the lock, we need to get the current state of
                # the partition to determine where to start reading
                state = self._state_provider.read(self.name)
                current_position = state.positions.get(partition_number, -1)
                next_message_position = current_position + 1

                message_pointers = list(
                    self._stream.read_slice(
                        partition=partition_number,
                        start=next_message_position,
                        count=count,
                        conn=conn,
                    )
                )
                if not message_pointers:
                    # No messages -> try the next partition
                    self._lock_provider.unlock(lock_key)
                    continue

                with self._stream._store.reader(conn=conn) as reader:
                    stored_messages = {
                        message.message_id: message
                        for message in reader.get_messages_by_ids(
                            [pointer.message_id for pointer in message_pointers]
                        )
                    }
                messages = [
                    SubscriptionMessage(
                        partition=pointer.partition,
                        position=pointer.position,
                        stored_message=stored_messages[pointer.message_id],
                        ack=NoAckOp(),
                    )
                    for pointer in message_pointers
                ]
                return SubscriptionMessageBatch(
                    partition=partition_number,
                    first_position=min(msg.position for msg in messages),
                    last_position=max(msg.position for msg in messages),
                    lock_key=lock_key,
                    messages=messages,
                )
            return None

    def ack_message_batch(
        self, message_batch: SubscriptionMessageBatch[E], success: bool
    ) -> None:
        if success:
            self._state_provider.store(
                subscription_name=self.name,
                partition=message_batch.partition,
                position=message_batch.ackd_position,
            )
        self.unlock_message_batch(message_batch)

    def unlock_message_batch(self, message_batch: SubscriptionMessageBatch[E]) -> None:
        self._lock_provider.unlock(message_batch.lock_key)

    def get_next_messages(self, count: int) -> Iterator[SubscriptionMessage[E]]:
        batch = None
        try:
            batch = self.get_next_message_batch(count=count)
            if batch:
                for message in batch.messages:
                    ack = AckOp(
                        name=self.name,
                        partition=message.partition,
                        position=message.position,
                        state_provider=self._state_provider,
                    )

                    yield SubscriptionMessage(
                        partition=message.partition,
                        position=message.position,
                        stored_message=message.stored_message,
                        ack=ack,
                    )
                    if ack.rolled_back:
                        # the message was not ack'd or the acknolwedgement was rolled back
                        break
                    try:
                        ack.execute()
                    except AckRolledback:
                        # the message was not ack'd or the acknolwedgement was rolled back
                        break
        finally:
            if batch:
                self.unlock_message_batch(batch)

    def get_next_message_batch_for_partition(
        self,
        partition: int,
        count: int,
        conn: Optional[SAConnection] = None,
    ) -> Optional[SubscriptionMessageBatch[E]]:
        """
        Build a batch for a specific partition without acquiring any
        per-partition advisory lock. Used by
        [AssignedSubscriptionRunner][depeche_db.AssignedSubscriptionRunner]
        where ownership is already guaranteed by the assignment table.

        Callers must have the state initialized already (call ``_init_state``
        or rely on the runner to do so).
        """
        if not self._state_provider.initialized(self.name):
            self._init_state()
        assert self._state_provider.initialized(self.name)

        def _inner(conn: SAConnection) -> Optional[SubscriptionMessageBatch[E]]:
            state = self._state_provider.read(self.name)
            current_position = state.positions.get(partition, -1)
            next_message_position = current_position + 1

            message_pointers = list(
                self._stream.read_slice(
                    partition=partition,
                    start=next_message_position,
                    count=count,
                    conn=conn,
                )
            )
            if not message_pointers:
                return None

            with self._stream._store.reader(conn=conn) as reader:
                stored_messages = {
                    message.message_id: message
                    for message in reader.get_messages_by_ids(
                        [pointer.message_id for pointer in message_pointers]
                    )
                }
            messages = [
                SubscriptionMessage(
                    partition=pointer.partition,
                    position=pointer.position,
                    stored_message=stored_messages[pointer.message_id],
                    ack=NoAckOp(),
                )
                for pointer in message_pointers
            ]
            return SubscriptionMessageBatch(
                partition=partition,
                first_position=min(msg.position for msg in messages),
                last_position=max(msg.position for msg in messages),
                lock_key="",  # no lock in the assigned path
                messages=messages,
            )

        if conn is None:
            with self._stream._store.engine.connect() as owned_conn:
                return _inner(owned_conn)
        return _inner(conn)

    def iter_assigned_messages(
        self, partition: int, generation: int, count: int
    ) -> Iterator[SubscriptionMessage[E]]:
        """
        Yield a batch of messages for a partition the caller currently owns,
        threading ``generation`` into each AckOp so stale owners are rejected
        at the DB layer (``PartitionRevoked``).
        """
        if self._assignment_table is None:
            raise RuntimeError(
                "iter_assigned_messages requires an assignment_table on the Subscription"
            )
        batch = self.get_next_message_batch_for_partition(
            partition=partition, count=count
        )
        if batch is None:
            return
        for message in batch.messages:
            ack = AckOp(
                name=self.name,
                partition=message.partition,
                position=message.position,
                state_provider=self._state_provider,
                expected_generation=generation,
                expected_instance_id=self._current_instance_id,
                assignment_table=self._assignment_table,
            )
            yield SubscriptionMessage(
                partition=message.partition,
                position=message.position,
                stored_message=message.stored_message,
                ack=ack,
            )
            if ack.rolled_back:
                break
            try:
                ack.execute()
            except AckRolledback:
                break

    _current_instance_id: Optional[_uuid.UUID] = None


class SubscriptionMessageHandler(Generic[E]):
    def __init__(
        self,
        handler_register: MessageHandlerRegisterProtocol[E],
        error_handler: Optional[SubscriptionErrorHandler] = None,
        call_middleware: Optional[CallMiddleware] = None,
    ):
        """
        Handles messages

        Args:
            handler_register: The handler register to use
            error_handler: A handler for errors raised by the handlers, defaults to handler that will exit the subscription
            call_middleware: The middleware to call before calling the handler
        """
        self._register = handler_register
        self._error_handler = error_handler or ExitSubscriptionErrorHandler()
        self._call_middleware = call_middleware

        if not self._call_middleware and any(
            handler.requires_middleware for handler in self._register.get_all_handlers()
        ):
            raise ValueError(
                "If handler has more than one parameter, a call_middleware must be provided"
            )

    def handle(self, message: SubscriptionMessage):
        handler = self._register.get_handler(type(message.stored_message.message))
        if handler:
            try:
                self._exec(handler.handler, handler.adapt_message_type(message))
            except Exception as error:
                error_handling_result = self._error_handler.handle_error(
                    error=error, message=message
                )
                if error_handling_result == ErrorAction.EXIT:
                    raise

    def _exec(
        self,
        handler: Callable[..., None],
        message: Union[SubscriptionMessage[E], StoredMessage[E], E],
    ) -> None:
        if self._call_middleware:
            self._call_middleware.call(handler, message)
        else:
            handler(message)


class SubscriptionRunner(Generic[E]):
    def __init__(
        self,
        subscription: Subscription[E],
        message_handler: SubscriptionMessageHandler,
        batch_size: Optional[int] = None,
    ):
        """
        Handles messages from a subscription using a handler

        The `batch_size` argument controls how many messages to handle in each
        batch. If not provided, the default is 10. A larger batch size will
        result less round trips to the database, but will also make it more
        likely that messages from _different partitions_ will be processed out of
        the order defined by their `global_position` on the message store.

        A batch size of 1 will ensure that messages are processed in order
        regarding to their `global_position`.
        Messages in the same partition will always be processed in order.

        Implements: [RunOnNotification][depeche_db.RunOnNotification]

        Args:
            subscription: The subscription to handle
            message_handler: The handler to use
            batch_size: The number of messages to handle in each batch, defaults to 10
        """
        self._subscription = subscription
        self._batch_size = batch_size or 10
        self._keep_running = True
        self._handler = message_handler

    def interested_in_notification(self, notification: dict) -> bool:
        return True

    def take_notification_hint(self, notification: dict):
        partition_number = notification.get("partition")
        position = notification.get("position")
        if partition_number is not None and position is not None:
            self._subscription._update_max_aggregated_stream_positions_cache(
                partition_number, position
            )

    @property
    def notification_channel(self) -> str:
        return self._subscription._stream.notification_channel

    def run(self, budget: Optional[TimeBudget] = None) -> RunOnNotificationResult:
        return self.run_once(budget=budget)

    def stop(self):
        self._keep_running = False

    def run_once(self, budget: Optional[TimeBudget] = None) -> RunOnNotificationResult:
        while self._keep_running:
            n = 0
            for message in self._subscription.get_next_messages(count=self._batch_size):
                n += 1
                self.handle(message)
            if n == 0:
                break
            if budget and budget.over_budget():
                return RunOnNotificationResult.WORK_REMAINING
        return RunOnNotificationResult.DONE_FOR_NOW

    def handle(self, message: SubscriptionMessage):
        self._handler.handle(message)


class BatchedAckSubscriptionRunner(SubscriptionRunner[E]):
    def run_once(self, budget: Optional[TimeBudget] = None) -> RunOnNotificationResult:
        while self._keep_running:
            message_batch = self._subscription.get_next_message_batch(
                count=self._batch_size
            )
            if message_batch is None:
                break
            try:
                for message in message_batch.messages:
                    self.handle(message)
                    message_batch.ack(message)
            finally:
                self._subscription.ack_message_batch(
                    message_batch=message_batch, success=True
                )
            if budget and budget.over_budget():
                return RunOnNotificationResult.WORK_REMAINING
        return RunOnNotificationResult.DONE_FOR_NOW


class AssignedSubscriptionRunner(Generic[E]):
    """
    Subscription runner that relies on a
    [PartitionAssignmentProvider][depeche_db.PartitionAssignmentProvider]
    for coordination instead of per-batch advisory locks.

    Each tick, the runner:

    1. Registers (once) and sends a heartbeat.
    2. Opportunistically runs the rebalance (leader-elected inside the
       provider; non-leaders return immediately).
    3. Reads its current assignments and processes one batch per partition.

    Generation fencing on every position write guarantees that a partition
    reassigned mid-batch will raise
    [PartitionRevoked][depeche_db.PartitionRevoked] on the previous owner's
    next ack.

    Args:
        subscription: The subscription to run.
        message_handler: The handler register wrapper.
        batch_size: Messages per batch per partition, defaults to 10.
        heartbeat_interval: Seconds between heartbeats, defaults to 5.
        rebalance_interval: Seconds between rebalance attempts, defaults to 10.
        ack_strategy: Ack strategy, defaults to SINGLE. (BATCHED also supported.)
        instance_label: Optional human-readable label stored on the instance row.
    """

    def __init__(
        self,
        subscription: "Subscription[E]",
        message_handler: "SubscriptionMessageHandler[E]",
        batch_size: Optional[int] = None,
        heartbeat_interval: float = 5.0,
        rebalance_interval: float = 10.0,
        ack_strategy: AckStrategy = AckStrategy.SINGLE,
        instance_label: Optional[str] = None,
    ):
        if subscription._assignment_provider is None:
            raise ValueError(
                "AssignedSubscriptionRunner requires a Subscription configured "
                "with coordination_strategy=CoordinationStrategy.INSTANCE_ASSIGNMENT"
            )
        self._subscription = subscription
        self._handler = message_handler
        self._batch_size = batch_size or 10
        self._heartbeat_interval = heartbeat_interval
        self._rebalance_interval = rebalance_interval
        self._ack_strategy = ack_strategy
        self._instance_label = instance_label
        self._keep_running = True

        self._instance_id: _uuid.UUID = _uuid.uuid4()
        self._subscription._current_instance_id = self._instance_id
        self._registered = False
        self._assignments: Dict[int, int] = {}
        self._assignments_lock = _threading.Lock()
        self._assignments_fetched_at = 0.0
        self._last_heartbeat_at = 0.0
        self._last_rebalance_at = 0.0
        self._assignment_cache_ttl = max(1.0, heartbeat_interval / 2.0)

    @property
    def instance_id(self) -> _uuid.UUID:
        return self._instance_id

    @property
    def notification_channel(self) -> str:
        return self._subscription._stream.notification_channel

    def interested_in_notification(self, notification: dict) -> bool:
        partition = notification.get("partition")
        if partition is None:
            return True
        with self._assignments_lock:
            # If we haven't refreshed assignments yet, let the runner wake up
            # so it can register & fetch; the stimulator would do it anyway.
            if not self._assignments:
                return True
            return partition in self._assignments

    def take_notification_hint(self, notification: dict):
        partition_number = notification.get("partition")
        position = notification.get("position")
        if partition_number is not None and position is not None:
            self._subscription._update_max_aggregated_stream_positions_cache(
                partition_number, position
            )

    def stop(self):
        self._keep_running = False
        self._deregister_safely()

    def run(self, budget: Optional[TimeBudget] = None) -> RunOnNotificationResult:
        return self.run_once(budget=budget)

    # ---------------------------------------------------- lifecycle helpers

    def _ensure_registered(self) -> None:
        if self._registered:
            return
        import os
        import socket

        assert self._subscription._assignment_provider is not None
        self._subscription._assignment_provider.register(
            instance_id=self._instance_id,
            host=socket.gethostname(),
            pid=os.getpid(),
            label=self._instance_label,
        )
        self._registered = True
        self._last_heartbeat_at = _time.time()

    def _heartbeat_if_due(self) -> bool:
        now = _time.time()
        if now - self._last_heartbeat_at < self._heartbeat_interval:
            return True
        assert self._subscription._assignment_provider is not None
        alive = self._subscription._assignment_provider.heartbeat(self._instance_id)
        self._last_heartbeat_at = now
        if not alive:
            # We were evicted by the TTL. Re-register so we can continue.
            self._registered = False
            self._ensure_registered()
        return alive

    def _rebalance_if_due(self) -> None:
        now = _time.time()
        if now - self._last_rebalance_at < self._rebalance_interval:
            return
        self._last_rebalance_at = now
        assert self._subscription._assignment_provider is not None
        partitions = (
            self._subscription._stream.get_max_aggregated_stream_positions().keys()
        )
        try:
            self._subscription._assignment_provider.rebalance(partitions)
        except Exception:
            DEPECHE_LOGGER.exception(
                "Rebalance failed for subscription %r", self._subscription.name
            )

    def _refresh_assignments(self, force: bool = False) -> Dict[int, int]:
        now = _time.time()
        if (
            not force
            and now - self._assignments_fetched_at < self._assignment_cache_ttl
        ):
            with self._assignments_lock:
                return dict(self._assignments)
        assert self._subscription._assignment_provider is not None
        new = self._subscription._assignment_provider.get_my_assignments(
            self._instance_id
        )
        with self._assignments_lock:
            self._assignments = new
            self._assignments_fetched_at = now
            return dict(new)

    def _deregister_safely(self) -> None:
        if not self._registered:
            return
        try:
            assert self._subscription._assignment_provider is not None
            self._subscription._assignment_provider.deregister(self._instance_id)
        except Exception:
            DEPECHE_LOGGER.exception(
                "Deregister failed for subscription %r", self._subscription.name
            )
        self._registered = False

    # -------------------------------------------------------------- run_once

    def run_once(self, budget: Optional[TimeBudget] = None) -> RunOnNotificationResult:
        self._ensure_registered()
        if not self._heartbeat_if_due():
            return RunOnNotificationResult.DONE_FOR_NOW
        self._rebalance_if_due()

        assignments = self._refresh_assignments()
        if not assignments:
            return RunOnNotificationResult.DONE_FOR_NOW

        did_work = False
        work_remaining = False
        for partition, generation in assignments.items():
            if not self._keep_running:
                break
            try:
                n = self._process_partition(partition, generation)
            except PartitionRevoked:
                DEPECHE_LOGGER.info(
                    "Partition %s of subscription %s was revoked",
                    partition,
                    self._subscription.name,
                )
                self._refresh_assignments(force=True)
                continue
            if n > 0:
                did_work = True
                if n >= self._batch_size:
                    work_remaining = True
            if budget and budget.over_budget():
                return RunOnNotificationResult.WORK_REMAINING
        if work_remaining:
            return RunOnNotificationResult.WORK_REMAINING
        if did_work:
            return RunOnNotificationResult.WORK_REMAINING
        return RunOnNotificationResult.DONE_FOR_NOW

    def _process_partition(self, partition: int, generation: int) -> int:
        if self._ack_strategy == AckStrategy.BATCHED:
            batch = self._subscription.get_next_message_batch_for_partition(
                partition=partition, count=self._batch_size
            )
            if batch is None or not batch.messages:
                return 0
            for message in batch.messages:
                self.handle(message)
                batch.ack(message)
            self._subscription._state_provider.store(
                subscription_name=self._subscription.name,
                partition=partition,
                position=batch.ackd_position,
                expected_generation=generation,
                expected_instance_id=self._instance_id,
                assignment_table=self._subscription._assignment_table,
            )
            return len(batch.messages)

        n = 0
        for message in self._subscription.iter_assigned_messages(
            partition=partition, generation=generation, count=self._batch_size
        ):
            n += 1
            self.handle(message)
        return n

    def handle(self, message: SubscriptionMessage):
        self._handler.handle(message)


class StartAtNextMessage(SubscriptionStartPoint):
    """
    Starts consuming messages from the next message in the stream.
    """

    def init_state(
        self,
        subscription_name: str,
        stream: "AggregatedStream",
        state_provider: SubscriptionStateProvider,
    ):
        for partition_statistic in stream.get_partition_statistics():
            state_provider.store(
                subscription_name=subscription_name,
                partition=partition_statistic.partition_number,
                position=partition_statistic.max_position,
            )


class StartAtPointInTime(SubscriptionStartPoint):
    def __init__(self, point_in_time: _dt.datetime):
        """
        Starts consuming messages from a point in time.

        Args:
            point_in_time: The point in time to start consuming messages from. The point in time must be timezone aware.
        """
        if not point_in_time.tzinfo:
            raise ValueError("Point in time must be timezone aware")
        self._point_in_time = point_in_time

    def init_state(
        self,
        subscription_name: str,
        stream: "AggregatedStream",
        state_provider: SubscriptionStateProvider,
    ):
        for partition, position in stream.time_to_positions(
            self._point_in_time
        ).items():
            if position > 0:
                state_provider.store(
                    subscription_name=subscription_name,
                    partition=partition,
                    position=position - 1,
                )
