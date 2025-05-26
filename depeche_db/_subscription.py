import datetime as _dt
import enum as _enum
import logging as _logging
from typing import (
    Callable,
    Generic,
    Iterator,
    Optional,
    TypeVar,
    Union,
)

from . import tools as _tools
from ._aggregated_stream import AggregatedStream
from ._interfaces import (
    CallMiddleware,
    ErrorAction,
    LockProvider,
    MessageHandlerRegisterProtocol,
    MessageProtocol,
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
    ):
        self.name = name
        self.partition = partition
        self.position = position
        self._state_provider = state_provider
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

        self._executed = True
        provider.store(
            subscription_name=self.name,
            partition=self.partition,
            position=self.position,
        )

    def _check(self):
        state = self._state_provider.read(self.name)
        if state.positions.get(self.partition, -1) != self.position:
            raise AckRolledback()

    def rollback(self):
        self._rolled_back = True

    @property
    def rolled_back(self):
        return self._rolled_back


class Subscription(Generic[E]):
    _state_provider: SubscriptionStateProvider
    runner: "Union[SubscriptionRunner[E], BatchedAckSubscriptionRunner[E]]"

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
        if ack_strategy == AckStrategy.BATCHED:
            self.runner = BatchedAckSubscriptionRunner(
                subscription=self,
                message_handler=message_handler,
                batch_size=batch_size,
            )
        elif ack_strategy == AckStrategy.SINGLE:
            self.runner = SubscriptionRunner(
                subscription=self,
                message_handler=message_handler,
                batch_size=batch_size,
            )
        else:
            raise NotImplementedError(f"Ack strategy {ack_strategy} is not implemented")

    def _init_state(self):
        if not self._state_provider.initialized(self.name):
            lock_key = f"subscription-{self.name}-init"
            if not self._lock_provider.lock(lock_key):
                # another instance is already initializing the state
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

    def get_next_message_batch(
        self, count: int
    ) -> Optional[SubscriptionMessageBatch[E]]:
        if not self._state_provider.initialized(self.name):
            self._init_state()
        assert self._state_provider.initialized(self.name)

        state = self._state_provider.read(self.name)
        with self._stream._store.engine.connect() as conn:
            statistics = list(
                self._stream.get_partition_statistics(
                    position_limits=state.positions, result_limit=10, conn=conn
                )
            )
            for statistic in statistics:
                lock_key = f"subscription-{self.name}-{statistic.partition_number}"
                if not self._lock_provider.lock(lock_key):
                    continue
                # now we have the lock, we need to check if the position is still valid
                # if not, we need to release the lock and try the next partition
                state = self._state_provider.read(self.name)
                if state.positions.get(statistic.partition_number, -1) != (
                    statistic.next_message_position - 1
                ):
                    self._lock_provider.unlock(lock_key)
                    continue

                message_pointers = list(
                    self._stream.read_slice(
                        partition=statistic.partition_number,
                        start=statistic.next_message_position,
                        count=count,
                        conn=conn,
                    )
                )
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
                    partition=statistic.partition_number,
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
                # TODO check budget here?
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
