import contextlib as _contextlib
import datetime as _dt
import uuid as _uuid
from typing import Dict, Generic, Iterator, List, Optional, TypeVar

import sqlalchemy as _sa
from psycopg2.errors import LockNotAvailable
from sqlalchemy_utils import UUIDType as _UUIDType

from ._compat import SAConnection
from ._factories import SubscriptionFactory
from ._interfaces import (
    AggregatedStreamMessage,
    MessagePartitioner,
    MessageProtocol,
    StreamPartitionStatistic,
)
from ._message_store import MessageStore

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
            f"{name}_projected_stream",
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
                name=f"uq_{name}_partition_position",
            ),
        )
        self.notification_channel = f"{name}_notifications"
        trigger = _sa.DDL(
            f"""
            CREATE OR REPLACE FUNCTION {name}_stream_notify_message_inserted()
              RETURNS trigger AS $$
            DECLARE
            BEGIN
              PERFORM pg_notify(
                '{self.notification_channel}',
                json_build_object(
                    'message_id', NEW.message_id,
                    'partition', NEW.partition,
                    'position', NEW.position
                )::text);
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER {name}_stream_notify_message_inserted
              AFTER INSERT ON {name}_projected_stream
              FOR EACH ROW
              EXECUTE PROCEDURE {name}_stream_notify_message_inserted();
            """
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

    def read(self, partition: int) -> Iterator[AggregatedStreamMessage]:
        """
        Read all messages from a partition of the aggregated stream.

        Args:
            partition: Partition number
        """
        with self._connection() as conn:
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

    def read_slice(
        self, partition: int, start: int, count: int
    ) -> Iterator[AggregatedStreamMessage]:
        """
        Read a slice of messages from a partition of the aggregated stream.

        Args:
            partition: Partition number
            start: Start position
            count: Number of messages to read
        """
        with self._connection() as conn:
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

    @_contextlib.contextmanager
    def _connection(self):
        conn = self._store.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def get_partition_statistics(
        self,
        position_limits: Dict[int, int] = None,
        result_limit: Optional[int] = None,
    ) -> Iterator[StreamPartitionStatistic]:
        """
        Get partition statistics for deciding which partitions to read from. This
        is used by subscriptions.
        """
        with self._connection() as conn:
            position_limits = position_limits or {-1: -1}
            tbl = self._table.alias()
            next_messages_tbl = (
                _sa.select(
                    tbl.c.partition,
                    _sa.func.min(tbl.c.position).label("min_position"),
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
                _sa.select(tbl)
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
                )
            result.close()
            del result


class _AlreadyUpdating(RuntimeError):
    pass


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

    def run(self):
        """
        Runs the projector once.
        """
        try:
            self.update_full()
        except _AlreadyUpdating:
            pass

    def stop(self):
        """
        No-Op on this class.
        """
        pass

    def update_full(self) -> int:
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
                if isinstance(exc.orig, LockNotAvailable):
                    raise _AlreadyUpdating(
                        "Cannot update stream projection, because another process is already updating it."
                    )
                raise
            while True:
                batch_num = self._update_batch(conn, cutoff)
                if batch_num == 0:
                    break
                result += batch_num
            conn.commit()
        return result

    def _update_batch(self, conn, cutoff: Optional[int] = None) -> int:
        tbl = self.stream._table.alias()
        message_table = self.stream._store._storage.message_table
        last_seen = (
            _sa.select(
                tbl.c.origin_stream,
                _sa.func.max(tbl.c.origin_stream_version).label("max_version"),
            )
            .group_by(tbl.c.origin_stream)
            .cte()
        )
        cutoff_cond = []
        if cutoff is not None:
            cutoff_cond = [message_table.c.global_position <= cutoff]
        q = (
            _sa.select(
                message_table.c.message_id,
                message_table.c.stream,
                message_table.c.version,
            )
            .select_from(
                message_table.join(
                    last_seen,
                    message_table.c.stream == last_seen.c.origin_stream,
                    isouter=True,
                )
            )
            .where(
                _sa.and_(
                    _sa.or_(
                        *[
                            message_table.c.stream.like(wildcard)
                            for wildcard in self.stream_wildcards
                        ]
                    ),
                    _sa.or_(
                        message_table.c.version > last_seen.c.max_version,
                        last_seen.c.max_version.is_(None),
                    ),
                    *cutoff_cond,
                )
            )
            .order_by(message_table.c.global_position)
            .limit(self.batch_size)
        )
        messages = list(conn.execute(q))
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
