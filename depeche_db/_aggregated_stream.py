import contextlib as _contextlib
import datetime as _dt
import uuid as _uuid
from collections import namedtuple
from typing import (
    TYPE_CHECKING,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
)

import sqlalchemy as _sa
from sqlalchemy_utils import UUIDType as _UUIDType

from ._compat import PsycoPgLockNotAvailable, SAColumnElement, SAConnection
from ._factories import SubscriptionFactory
from ._interfaces import (
    AggregatedStreamMessage,
    MessagePartitioner,
    MessageProtocol,
    RunOnNotificationResult,
    StreamPartitionStatistic,
    SubscriptionStartPoint,
    TimeBudget,
)
from ._message_store import MessageStore

if TYPE_CHECKING:
    from ._aggregated_stream_reader import (
        AggregatedStreamReader,
        AsyncAggregatedStreamReader,
    )

E = TypeVar("E", bound=MessageProtocol)


class AggregatedStream(Generic[E]):
    def __init__(
        self,
        name: str,
        store: MessageStore[E],
        partitioner: MessagePartitioner[E],
        stream_wildcards: List[str],
        update_batch_size: Optional[int] = None,
    ) -> None:
        """
        AggregatedStream aggregates multiple streams into one (partitioned) stream.

        Read more about aggregated streams under [Data Model](../concepts/data-model.md#aggregated-streams).

        The `update_batch_size` argument can be used to control the batch size of the
        update process. Higher numbers will result in less database roundtrips but
        also in higher memory usage.

        Args:
            name: Stream name, needs to be a valid python identifier
            store: Message store
            partitioner: Message partitioner
            stream_wildcards: List of stream wildcards
            update_batch_size: Batch size for updating the stream, defaults to 100

        Attributes:
            name (str): Stream name
            projector (StreamProjector): Stream projector
            subscription (SubscriptionFactory): Factory to create subscriptions on this stream
        """
        assert name.isidentifier(), "name must be a valid identifier"
        self.name = name
        self.subscription = SubscriptionFactory(self)
        self._store = store
        self._metadata = _sa.MetaData()
        self._table = _sa.Table(
            self.stream_table_name(name),
            self._metadata,
            _sa.Column("message_id", _UUIDType(), primary_key=True),
            _sa.Column("origin_stream", _sa.String(255), nullable=False),
            _sa.Column("origin_stream_version", _sa.Integer, nullable=False),
            _sa.Column(
                "partition",
                _sa.Integer,
                nullable=False,
                index=True,
            ),
            _sa.Column(
                "position",
                _sa.Integer,
                nullable=False,
            ),
            _sa.Column(
                "message_occurred_at",
                _sa.DateTime,
                nullable=False,
            ),
            _sa.UniqueConstraint(
                "partition",
                "position",
                name=f"depeche_stream_{name}_uq",
            ),
        )
        self.notification_channel = self.notification_channel_name(name)
        trigger = _sa.DDL(
            _notify_trigger(
                name=name,
                tablename=self._table.name,
                notification_channel=self.notification_channel,
            )
        )
        _sa.event.listen(
            self._table, "after_create", trigger.execute_if(dialect="postgresql")
        )
        self._metadata.create_all(store.engine, checkfirst=True)
        self.projector = StreamProjector(
            stream=self,
            partitioner=partitioner,
            stream_wildcards=stream_wildcards,
            batch_size=update_batch_size,
        )

    def truncate(self, conn: SAConnection):
        """
        Truncate aggregated stream.
        """
        conn.execute(self._table.delete())

    def add(
        self,
        conn: SAConnection,
        message_id: _uuid.UUID,
        stream: str,
        stream_version: int,
        partition: int,
        position: int,
        message_occurred_at: _dt.datetime,
    ) -> None:
        # TODO assert message_occurred_at is UTC (or time zone aware)
        if partition < 0:
            raise ValueError("partition must be >= 0")
        conn.execute(
            self._table.insert().values(
                message_id=message_id,
                origin_stream=stream,
                origin_stream_version=stream_version,
                partition=partition,
                position=position,
                message_occurred_at=message_occurred_at,
            )
        )

    def read(
        self, partition: int, conn: Optional[SAConnection] = None
    ) -> Iterator[AggregatedStreamMessage]:
        """
        Read all messages from a partition of the aggregated stream.

        Args:
            partition: Partition number
            conn: Optional connection to use for reading. If not provided, a new connection will be created.
        """

        def _inner(conn):
            for row in conn.execute(
                _sa.select(self._table.c.message_id, self._table.c.position)
                .where(self._table.c.partition == partition)
                .order_by(self._table.c.position)
            ):
                yield AggregatedStreamMessage(
                    message_id=row.message_id,
                    position=row.position,
                    partition=partition,
                )

        if conn is None:
            with self._connection() as conn:
                yield from _inner(conn)
        else:
            yield from _inner(conn)

    def read_slice(
        self,
        partition: int,
        start: int,
        count: int,
        conn: Optional[SAConnection] = None,
    ) -> Iterator[AggregatedStreamMessage]:
        """
        Read a slice of messages from a partition of the aggregated stream.

        Args:
            partition: Partition number
            start: Start position
            count: Number of messages to read
            conn: Optional connection to use for reading. If not provided, a new connection will be created.
        """

        def _inner(conn):
            for row in conn.execute(
                _sa.select(self._table.c.message_id, self._table.c.position)
                .where(
                    _sa.and_(
                        self._table.c.partition == partition,
                        self._table.c.position >= start,
                    )
                )
                .order_by(self._table.c.position)
                .limit(count)
            ):
                yield AggregatedStreamMessage(
                    message_id=row.message_id,
                    position=row.position,
                    partition=partition,
                )

        if conn is None:
            with self._connection() as conn:
                yield from _inner(conn)
        else:
            yield from _inner(conn)

    def reader(
        self, start_point: Optional[SubscriptionStartPoint] = None
    ) -> "AggregatedStreamReader":
        """
        Get a reader for the aggregated stream.

        Args:
            start_point: Start point for the reader
        """
        from ._aggregated_stream_reader import AggregatedStreamReader

        return AggregatedStreamReader(self, start_point=start_point)

    def async_reader(
        self, start_point: Optional[SubscriptionStartPoint] = None
    ) -> "AsyncAggregatedStreamReader":
        """
        Get an async reader for the aggregated stream.

        Args:
            start_point: Start point for the reader
        """
        from ._aggregated_stream_reader import AsyncAggregatedStreamReader

        return AsyncAggregatedStreamReader(self, start_point=start_point)

    @_contextlib.contextmanager
    def _connection(self):
        conn = self._store.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def get_partition_statistics(
        self,
        position_limits: Optional[Dict[int, int]] = None,
        result_limit: Optional[int] = None,
        conn: Optional[SAConnection] = None,
    ) -> Iterator[StreamPartitionStatistic]:
        """
        Get partition statistics for deciding which partitions to read from. This
        is used by subscriptions.
        """

        position_limits = position_limits or {-1: -1}

        def _inner(conn):
            tbl = self._table.alias()
            next_messages_tbl = (
                _sa.select(
                    tbl.c.partition,
                    _sa.func.min(tbl.c.position).label("min_position"),
                    _sa.func.max(tbl.c.position).label("max_position"),
                )
                .where(
                    _sa.or_(
                        *[
                            _sa.and_(
                                tbl.c.partition == partition, tbl.c.position > limit
                            )
                            for partition, limit in position_limits.items()
                        ],
                        _sa.not_(tbl.c.partition.in_(list(position_limits))),
                    )
                )
                .group_by(tbl.c.partition)
                .cte()
            )

            qry = (
                _sa.select(tbl, next_messages_tbl.c.max_position)
                .select_from(
                    next_messages_tbl.join(
                        tbl,
                        _sa.and_(
                            tbl.c.partition == next_messages_tbl.c.partition,
                            tbl.c.position == next_messages_tbl.c.min_position,
                        ),
                    )
                )
                .order_by(tbl.c.message_occurred_at)
                .limit(result_limit)
            )
            result = conn.execute(qry)
            for row in result.fetchall():
                yield StreamPartitionStatistic(
                    partition_number=row.partition,
                    next_message_id=row.message_id,
                    next_message_position=row.position,
                    next_message_occurred_at=row.message_occurred_at,
                    max_position=row.max_position,
                )
            result.close()
            del result

        if conn is None:
            with self._connection() as conn:
                yield from _inner(conn)
        else:
            yield from _inner(conn)

    def time_to_positions(self, time: _dt.datetime) -> Dict[int, int]:
        """
        Get the positions for each partition at a given time.

        Args:
            time: Time to get positions for (must be timezone aware)

        Returns:
            A dictionary mapping partition numbers to positions
        """
        if time.tzinfo is None:
            raise ValueError("time must be timezone aware")
        with self._connection() as conn:
            tbl = self._table.alias()

            positions_after_time = (
                _sa.select(
                    tbl.c.partition,
                    _sa.func.min(tbl.c.position).label("min_position"),
                )
                .where(tbl.c.message_occurred_at >= time.astimezone(_dt.timezone.utc))
                .group_by(tbl.c.partition)
                .cte()
            )
            max_positions = (
                _sa.select(
                    tbl.c.partition,
                    _sa.func.max(tbl.c.position).label("max_position"),
                )
                .group_by(tbl.c.partition)
                .cte()
            )
            qry = _sa.select(
                max_positions.c.partition,
                _sa.func.coalesce(
                    positions_after_time.c.min_position,
                    max_positions.c.max_position + 1,
                ).label("position"),
            ).select_from(
                max_positions.outerjoin(
                    positions_after_time,
                    max_positions.c.partition == positions_after_time.c.partition,
                )
            )
            return {row.partition: row.position for row in conn.execute(qry)}

    def global_position_to_positions(self, global_position: int) -> Dict[int, int]:
        """
        Get the positions for each partition at a given global position.

        Args:
            global_position: Global position

        Returns:
            A dictionary mapping partition numbers to positions
        """
        with self._connection() as conn:
            tbl = self._table.alias()
            messages_tbl = self._store._storage.message_table.alias()

            positions_upto_global_pos = (
                _sa.select(
                    tbl.c.partition,
                    _sa.func.max(tbl.c.position).label("position"),
                )
                .select_from(
                    tbl.join(
                        messages_tbl,
                        messages_tbl.c.message_id == tbl.c.message_id,
                    )
                )
                .where(messages_tbl.c.global_position <= global_position)
                .group_by(tbl.c.partition)
                .cte()
            )
            partitions = (
                _sa.select(
                    tbl.c.partition,
                )
                .group_by(tbl.c.partition)
                .cte()
            )
            qry = _sa.select(
                partitions.c.partition,
                _sa.func.coalesce(
                    positions_upto_global_pos.c.position,
                    -1,
                ).label("position"),
            ).select_from(
                partitions.outerjoin(
                    positions_upto_global_pos,
                    partitions.c.partition == positions_upto_global_pos.c.partition,
                )
            )
            return {row.partition: row.position for row in conn.execute(qry)}

    @staticmethod
    def stream_table_name(name: str) -> str:
        return f"depeche_stream_{name}"

    @staticmethod
    def notification_channel_name(name: str) -> str:
        return f"depeche_{name}_messages"

    @classmethod
    def get_migration_ddl(cls, name: str):
        """
        DDL Script to migrate from <=0.8.0
        """
        tablename = cls.stream_table_name(name)
        new_objects = _notify_trigger(
            name=name,
            tablename=tablename,
            notification_channel=cls.notification_channel_name(name),
        )
        return f"""
            ALTER TABLE "{name}_projected_stream"
                 RENAME TO {tablename};
            DROP TRIGGER IF EXISTS {name}_stream_notify_message_inserted;
            DROP FUNCTION IF EXISTS {name}_stream_notify_message_inserted;
            {new_objects}
            """

    @classmethod
    def migrate_db_objects(cls, name: str, conn: SAConnection):
        """
        Migrate from <=0.8.0
        """
        conn.execute(cls.get_migration_ddl(name=name))


def _notify_trigger(name: str, tablename: str, notification_channel: str) -> str:
    trigger_name = f"depeche_stream_new_msg_{name}"
    return f"""
        CREATE OR REPLACE FUNCTION {trigger_name}()
          RETURNS trigger AS $$
        DECLARE
        BEGIN
          PERFORM pg_notify(
            '{notification_channel}',
            json_build_object(
                'message_id', NEW.message_id,
                'partition', NEW.partition,
                'position', NEW.position
            )::text);
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER {trigger_name}
          AFTER INSERT ON {tablename}
          FOR EACH ROW
          EXECUTE PROCEDURE {trigger_name}();
        """


class _AlreadyUpdating(RuntimeError):
    pass


SelectedOriginStream = namedtuple(
    "SelectedOriginStream",
    ["stream", "max_aggregated_stream_version", "min_global_position", "message_count"],
)


class StreamProjector(Generic[E]):
    def __init__(
        self,
        stream: AggregatedStream[E],
        partitioner: MessagePartitioner[E],
        stream_wildcards: List[str],
        batch_size: Optional[int] = None,
    ):
        """
        Stream projector is responsible for updating an aggregated stream.

        The update process is locked to prevent concurrent updates. Thus, it is
        fine to run the projector in multiple processes.

        Implements: [RunOnNotification][depeche_db.RunOnNotification]
        """
        self.stream = stream
        self.stream_wildcards = stream_wildcards
        self.partitioner = partitioner
        self.batch_size = batch_size or 100

    @property
    def notification_channel(self) -> str:
        """
        Returns the notification channel name for this projector.
        """
        return self.stream._store._storage.notification_channel

    def run(self, budget: Optional[TimeBudget] = None) -> RunOnNotificationResult:
        """
        Runs the projector once.
        """
        try:
            self.update_full(budget=budget)
            if budget and budget.over_budget():
                return RunOnNotificationResult.WORK_REMAINING
        except _AlreadyUpdating:
            pass
        return RunOnNotificationResult.DONE_FOR_NOW

    def stop(self):
        """
        No-Op on this class.
        """
        pass

    def update_full(self, budget: Optional[TimeBudget] = None) -> int:
        """
        Updates the projection from the last known position to the current position.
        """
        result = 0
        with self.stream._store.engine.connect() as conn:
            cutoff = self.stream._store._storage.get_global_position(conn)
            try:
                conn.execute(
                    _sa.text(
                        f"LOCK TABLE {self.stream._table.name} IN EXCLUSIVE MODE NOWAIT"
                    )
                )
            except _sa.exc.OperationalError as exc:
                if isinstance(exc.orig, PsycoPgLockNotAvailable):
                    raise _AlreadyUpdating(
                        "Cannot update stream projection, because another process is already updating it."
                    )
                raise
            while True:
                batch_num = self._update_batch(conn, cutoff)
                if batch_num == 0:
                    break
                if budget and budget.over_budget():
                    break
                result += batch_num
            conn.commit()
        return result

    def _select_origin_streams(
        self, conn: SAConnection, cutoff_cond: List[SAColumnElement[bool]]
    ) -> List[SelectedOriginStream]:
        tbl = self.stream._table.alias()
        message_table = self.stream._store._storage.message_table
        max_origin_stream_version = (
            # Versions of the origin streams, used to determine which streams
            # have new messages.
            _sa.select(
                message_table.c.stream,
                _sa.func.max(message_table.c.version).label(
                    "max_origin_stream_version"
                ),
                _sa.func.min(message_table.c.global_position).label(
                    "min_global_position"
                ),
            )
            .where(
                _sa.and_(
                    _sa.or_(
                        *[
                            message_table.c.stream.like(wildcard)
                            for wildcard in self.stream_wildcards
                        ]
                    ),
                    *cutoff_cond,
                )
            )
            .group_by(message_table.c.stream)
            .cte("max_origin_stream_version")
        )
        max_aggregated_stream_version = (
            # Current version of all the streams in the aggregated stream.
            _sa.select(
                tbl.c.origin_stream,
                _sa.func.max(tbl.c.origin_stream_version).label(
                    "max_aggregated_stream_version"
                ),
            )
            .group_by(tbl.c.origin_stream)
            .cte("max_aggregated_stream_version")
        )
        streams_to_be_updated = (
            _sa.select(
                max_origin_stream_version.c.stream,
                _sa.func.coalesce(
                    max_aggregated_stream_version.c.max_aggregated_stream_version, 0
                ).label("max_aggregated_stream_version"),
                max_origin_stream_version.c.min_global_position,
                (
                    max_origin_stream_version.c.max_origin_stream_version
                    - _sa.func.coalesce(
                        max_aggregated_stream_version.c.max_aggregated_stream_version, 0
                    )
                ).label("message_count"),
            )
            .select_from(
                max_origin_stream_version.join(
                    max_aggregated_stream_version,
                    max_origin_stream_version.c.stream
                    == max_aggregated_stream_version.c.origin_stream,
                    isouter=True,
                )
            )
            .where(
                _sa.or_(
                    max_aggregated_stream_version.c.max_aggregated_stream_version.is_(
                        None
                    ),
                    max_origin_stream_version.c.max_origin_stream_version
                    > max_aggregated_stream_version.c.max_aggregated_stream_version,
                )
            )
            # Oldest (= first message the oldest) streams first
            .order_by(max_origin_stream_version.c.min_global_position)
        )

        selected_streams: list[SelectedOriginStream] = []
        for row in conn.execute(streams_to_be_updated):
            if len(selected_streams) >= min(self.batch_size, 20):
                # We select the 20 oldest streams to help with staying true
                # to our best effort guarantee of a.global_position < b.global_position
                # => a.position_in_partition < b.position_in_partition.
                break
            selected_streams.append(
                SelectedOriginStream(
                    row.stream,
                    row.max_aggregated_stream_version,
                    row.min_global_position,
                    row.message_count,
                )
            )
        return selected_streams

    def _update_batch(self, conn: SAConnection, cutoff: Optional[int] = None) -> int:
        message_table = self.stream._store._storage.message_table
        cutoff_cond = []
        if cutoff is not None:
            cutoff_cond = [message_table.c.global_position <= cutoff]

        selected_streams = self._select_origin_streams(conn, cutoff_cond=cutoff_cond)
        if not selected_streams:
            return 0

        # Minimal global_position from the relevant streams. This can/will be a lot
        # lower than the global position of the messages that need to be added
        # to the stream. It still is a helpful optimization as it limits the
        # amount of messages which have to be considered in the query below.
        min_global_position = min(
            selected_stream.min_global_position for selected_stream in selected_streams
        )
        qry = (
            _sa.select(
                message_table.c.message_id,
                message_table.c.stream,
                message_table.c.version,
            )
            .where(
                _sa.and_(
                    message_table.c.global_position >= min_global_position,
                    *cutoff_cond,
                    _sa.or_(
                        *[
                            _sa.and_(
                                message_table.c.stream == selected_stream.stream,
                                message_table.c.version
                                > selected_stream.max_aggregated_stream_version,
                            )
                            for selected_stream in selected_streams
                        ]
                    ),
                )
            )
            .order_by(message_table.c.global_position)
            .limit(self.batch_size)
        )
        messages = list(conn.execute(qry))
        if not messages:
            return 0

        self._add(conn, messages)
        return len(messages)

    def _add(self, conn, messages):
        positions = {
            row.partition: row.max_position
            for row in conn.execute(
                _sa.select(
                    self.stream._table.c.partition,
                    _sa.func.max(self.stream._table.c.position).label("max_position"),
                ).group_by(self.stream._table.c.partition)
            )
        }

        with self.stream._store.reader(conn) as reader:
            stored_messages = {
                message.message_id: message
                for message in reader.get_messages_by_ids(
                    [message_id for message_id, *_ in messages]
                )
            }

        for message_id, stream, version in messages:
            message = stored_messages[message_id]
            partition = self.partitioner.get_partition(message)
            position = positions.get(partition, -1) + 1
            self.stream.add(
                conn=conn,
                message_id=message_id,
                stream=stream,
                stream_version=version,
                partition=partition,
                position=position,
                message_occurred_at=message.message.get_message_time(),
            )
            positions[partition] = position
