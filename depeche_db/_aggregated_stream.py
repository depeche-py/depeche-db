import contextlib as _contextlib
import datetime as _dt
import logging as _logging
import re as _re
import textwrap as _textwrap
from collections import namedtuple
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

import sqlalchemy as _sa
from sqlalchemy_utils import UUIDType as _UUIDType

from ._compat import PsycoPgLockNotAvailable, SAConnection, SARow
from ._factories import SubscriptionFactory
from ._interfaces import (
    AggregatedStreamMessage,
    MessagePartitioner,
    MessageProtocol,
    RunOnNotificationResult,
    StoredMessage,
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

LOGGER = _logging.getLogger(__name__)


class AggregatedStream(Generic[E]):
    def __init__(
        self,
        name: str,
        store: MessageStore[E],
        partitioner: MessagePartitioner[E],
        stream_wildcards: List[str],
        update_batch_size: Optional[int] = None,
        lookback_for_gaps_hours: Optional[int] = None,
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
            lookback_for_gaps_hours: How many hours we should look aback for gaps in global positions. (Default is 6 hours, set this to 2-4x the time your longest transaction takes)

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
            _sa.Column(
                "origin_stream_global_position", _sa.Integer, nullable=False, index=True
            ),
            _sa.Column("origin_stream_added_at", _sa.DateTime, nullable=False),
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
            lookback_for_gaps_hours=lookback_for_gaps_hours,
        )

    def truncate(self, conn: SAConnection):
        """
        Truncate aggregated stream.
        """
        conn.execute(self._table.delete())

    def _add(
        self,
        conn: SAConnection,
        rows,
    ) -> None:
        conn.execute(self._table.insert().values(rows))

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
        result_limit: Optional[int] = None,
        conn: Optional[SAConnection] = None,
    ) -> Iterator[StreamPartitionStatistic]:
        def _inner(conn):
            tbl = self._table.alias()
            next_messages_tbl = (
                _sa.select(
                    tbl.c.partition,
                    _sa.func.min(tbl.c.position).label("min_position"),
                    _sa.func.max(tbl.c.position).label("max_position"),
                )
                .group_by(tbl.c.partition)
                .cte()
            )

            qry = _sa.select(tbl, next_messages_tbl.c.max_position).select_from(
                next_messages_tbl.join(
                    tbl,
                    _sa.and_(
                        tbl.c.partition == next_messages_tbl.c.partition,
                        tbl.c.position == next_messages_tbl.c.min_position,
                    ),
                )
            )
            if result_limit is not None:
                qry = qry.order_by(tbl.c.message_occurred_at).limit(result_limit)
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

    def _get_max_aggregated_stream_positions(
        self,
        conn: SAConnection,
        # min_position: int,
    ) -> Dict[int, int]:
        # Relatively expensive operation, so we should try hard to do it only when required
        tbl = self._table
        qry = (
            _sa.select(
                tbl.c.partition,
                _sa.func.max(tbl.c.position).label("max_position"),
            )
            # TODO this filter would be very helpful performance-wise!
            # .where(tbl.c.position >= min_position)
            .group_by(tbl.c.partition)
        )
        result = {
            row.partition: row.max_position for row in conn.execute(qry).fetchall()
        }
        return result

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
    def get_migration_ddl_0_8_0(
        cls, aggregated_stream_name: str, message_store_name: str
    ) -> str:
        """
        DDL Script to migrate from < 0.8.0
        """
        del message_store_name  # Unused in this migration
        tablename = cls.stream_table_name(aggregated_stream_name)
        new_objects = _notify_trigger(
            name=aggregated_stream_name,
            tablename=tablename,
            notification_channel=cls.notification_channel_name(aggregated_stream_name),
        )
        return _textwrap.dedent(
            f"""
            ALTER TABLE "{aggregated_stream_name}_projected_stream"
                 RENAME TO {tablename};
            DROP TRIGGER IF EXISTS {aggregated_stream_name}_stream_notify_message_inserted;
            DROP FUNCTION IF EXISTS {aggregated_stream_name}_stream_notify_message_inserted;
            {new_objects}
            """
        )

    @classmethod
    def get_migration_ddl_0_11_0(
        cls, aggregated_stream_name: str, message_store_name: str
    ) -> str:
        """
        DDL Script to migrate from < 0.11.0
        """
        aggregated_stream_tablename = cls.stream_table_name(aggregated_stream_name)
        message_tablename = f"depeche_msgs_{message_store_name}"
        return _textwrap.dedent(
            f"""
            -- Add columns (nullable)
            ALTER TABLE {aggregated_stream_tablename}
                ADD COLUMN origin_stream_global_position INTEGER NULL,
                ADD COLUMN origin_stream_added_at TIMESTAMP NULL;

            -- Copy data from the message store to the aggregated stream
            UPDATE {aggregated_stream_tablename} AS agg
                SET origin_stream_global_position = msg.global_position,
                origin_stream_added_at = msg.added_at
                FROM {message_tablename} AS msg
                WHERE agg.message_id = msg.message_id;

            -- Make the new columns NOT NULL
            ALTER TABLE {aggregated_stream_tablename}
                ALTER COLUMN origin_stream_global_position SET NOT NULL,
                ALTER COLUMN origin_stream_added_at SET NOT NULL;

            --- Add index
            CREATE INDEX ix_{aggregated_stream_tablename}_origin_stream_global_position
                ON {aggregated_stream_tablename} (origin_stream_global_position);
            """
        )

    @classmethod
    def migration_script_generators(
        cls,
    ) -> Dict[Tuple[int, int], List[Callable[[str, str], str]]]:
        return {
            (0, 8): [
                cls.get_migration_ddl_0_8_0,
            ],
            (0, 11): [
                cls.get_migration_ddl_0_11_0,
            ],
        }


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
    [
        "stream",
        "start_at_global_position",
        # "estimated_message_count",
    ],
)

AggregatedStreamPositon = namedtuple(
    "AggregatedStreamPositon",
    [
        "origin_stream",
        "max_aggregated_stream_global_position",
        "min_aggregated_stream_global_position",
    ],
)

OriginStreamPositon = namedtuple(
    "OriginStreamPositon",
    [
        "origin_stream",
        "max_global_position",
        "min_global_position",
    ],
)

AggregatedStreamHead = namedtuple(
    "AggregatedStreamHead",
    [
        "global_position",
        "added_at",
    ],
)

LookbackCache = namedtuple(
    "LookbackCache",
    [
        "head_added_at",
        "value",
    ],
)

OriginStreamPositionsCache = namedtuple(
    "OriginStreamPositionsCache",
    [
        "estimated_gap_look_back_start",
        "origin_streams",
    ],
)

FullUpdateResult = namedtuple(
    "FullUpdateResult",
    [
        "n_updated_messages",
        "more_messages_available",
    ],
)


class StreamProjector(Generic[E]):
    def __init__(
        self,
        stream: AggregatedStream[E],
        partitioner: MessagePartitioner[E],
        stream_wildcards: List[str],
        batch_size: Optional[int] = None,
        lookback_for_gaps_hours: Optional[int] = None,
    ):
        """
        Stream projector is responsible for updating an aggregated stream.

        The update process is locked to prevent concurrent updates. Thus, it is
        fine to run the projector in multiple processes.

        Implements: [RunOnNotification][depeche_db.RunOnNotification]
        """
        self.stream = stream
        self.stream_wildcards = stream_wildcards
        self.stream_regexes = [
            _re.compile(
                wildcard.replace("\\", "\\\\").replace(".", "\\.").replace("%", ".*")
            )
            for wildcard in stream_wildcards
        ]
        self.partitioner = partitioner
        self.batch_size = batch_size or 100
        self.lookback_for_gaps_hours = lookback_for_gaps_hours or 6
        self._lookback_cache: Optional[LookbackCache] = None
        self._get_origin_stream_positions_cache: Optional[
            OriginStreamPositionsCache
        ] = None

    def interested_in_notification(self, notification: dict) -> bool:
        # Check if the projector is interested in the notification.
        stream = notification.get("stream")
        if isinstance(stream, str):
            for regex in self.stream_regexes:
                if regex.fullmatch(stream):
                    return True
        return False

    def take_notification_hint(self, notification: dict):
        pass

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
            result = self.update_full(budget=budget)
            if budget and budget.over_budget() and result.more_messages_available:
                return RunOnNotificationResult.WORK_REMAINING
        except _AlreadyUpdating:
            pass
        return RunOnNotificationResult.DONE_FOR_NOW

    def stop(self):
        """
        No-Op on this class.
        """
        pass

    def update_full(self, budget: Optional[TimeBudget] = None) -> FullUpdateResult:
        """
        Updates the projection from the last known position to the current position.
        """
        result = 0
        batch_num = 0
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
                    self._aggregate_stream_positions_cache = None
                    raise _AlreadyUpdating(
                        "Cannot update stream projection, because another process is already updating it."
                    )
                raise
            while True:
                batch_num = self._update_batch(conn, cutoff)
                result += batch_num
                LOGGER.debug(f"{self.stream.name}: Batch updated: {batch_num} messages")
                if batch_num < self.batch_size:
                    # No more messages to process
                    break
                if budget and budget.over_budget():
                    # Budget exceeded, stop processing
                    break
            conn.commit()
        return FullUpdateResult(result, batch_num == self.batch_size)

    def get_aggregate_stream_positions(
        self,
        conn: SAConnection,
        estimated_gap_look_back_start: int,
    ) -> Dict[str, AggregatedStreamPositon]:
        stream_table = self.stream._table.alias()
        return {
            row.origin_stream: AggregatedStreamPositon(
                origin_stream=row.origin_stream,
                max_aggregated_stream_global_position=row.max_aggregated_stream_global_position,
                min_aggregated_stream_global_position=row.min_aggregated_stream_global_position,
            )
            for row in conn.execute(
                _sa.select(
                    stream_table.c.origin_stream,
                    _sa.func.max(stream_table.c.origin_stream_global_position).label(
                        "max_aggregated_stream_global_position"
                    ),
                    _sa.func.min(stream_table.c.origin_stream_global_position).label(
                        "min_aggregated_stream_global_position"
                    ),
                )
                .where(
                    stream_table.c.origin_stream_global_position
                    >= estimated_gap_look_back_start,
                )
                .group_by(stream_table.c.origin_stream),
            )
        }

    def get_origin_stream_positions(
        self,
        conn: SAConnection,
        min_global_position: int,
        max_global_position: Optional[int] = None,
    ) -> Dict[str, OriginStreamPositon]:
        origin_table = self.stream._store._storage.message_table
        cutoff_cond = []
        if max_global_position is not None:
            cutoff_cond = [origin_table.c.global_position <= max_global_position]
        return {
            stream: OriginStreamPositon(
                origin_stream=stream,
                max_global_position=max_global_position,
                min_global_position=min_global_position,
            )
            for (
                stream,
                max_global_position,
                min_global_position,
            ) in conn.execute(
                # TODO execute this with pyscopg's `prepare` option False
                # https://www.psycopg.org/psycopg3/docs/api/connections.html#psycopg.Connection.execute
                _sa.select(
                    origin_table.c.stream,
                    _sa.func.max(origin_table.c.global_position).label(
                        "max_origin_stream_global_position"
                    ),
                    _sa.func.min(origin_table.c.global_position).label(
                        "min_origin_stream_global_position"
                    ),
                )
                .where(
                    _sa.and_(
                        origin_table.c.global_position >= min_global_position,
                        _sa.or_(
                            *[
                                origin_table.c.stream.like(wildcard)
                                for wildcard in self.stream_wildcards
                            ]
                        ),
                        *cutoff_cond,
                    )
                )
                .group_by(origin_table.c.stream),
            ).fetchall()
        }

    def _get_aggregated_stream_head(self, conn: SAConnection) -> AggregatedStreamHead:
        stream_table = self.stream._table.alias()
        row = conn.execute(
            _sa.select(
                stream_table.c.origin_stream_global_position,
                stream_table.c.origin_stream_added_at,
            )
            .order_by(stream_table.c.origin_stream_global_position.desc())
            .limit(1)
        ).fetchone()
        if row:
            head_global_position, head_added_at = row
            return AggregatedStreamHead(
                global_position=head_global_position, added_at=head_added_at
            )
        return AggregatedStreamHead(
            global_position=-1,
            added_at=_dt.datetime(1980, 1, 1, tzinfo=_dt.timezone.utc),
        )

    def _estimate_gap_look_back_start(
        self, conn: SAConnection, head_added_at: _dt.datetime
    ) -> int:
        if self._lookback_cache is not None:
            if (head_added_at - self._lookback_cache.head_added_at) > _dt.timedelta(
                hours=1
            ):
                self._lookback_cache = None

        if self._lookback_cache is None:
            LOGGER.debug(f"{self.stream.name}: Updating lookback estimation")
            origin_table = self.stream._store._storage.message_table
            value = (
                conn.execute(
                    _sa.select(_sa.func.max(origin_table.c.global_position)).where(
                        origin_table.c.added_at
                        <= head_added_at
                        - _dt.timedelta(hours=self.lookback_for_gaps_hours)
                    )
                ).scalar_one_or_none()
            ) or 0
            self._lookback_cache = LookbackCache(
                head_added_at=head_added_at, value=value
            )

        return self._lookback_cache.value  # type: ignore

    def _select_origin_streams(
        self, conn: SAConnection, cutoff: Optional[int] = None
    ) -> List[SelectedOriginStream]:
        head = self._get_aggregated_stream_head(conn)
        if head.global_position > 0:
            estimated_gap_look_back_start = self._estimate_gap_look_back_start(
                conn, head.added_at
            )
        else:
            estimated_gap_look_back_start = 0
        LOGGER.debug(
            f"{self.stream.name}: Estimated gap look back start: {estimated_gap_look_back_start}"
        )

        stream_positions = self.get_aggregate_stream_positions(
            conn,
            estimated_gap_look_back_start=estimated_gap_look_back_start,
        )

        candidate_streams: List[SelectedOriginStream] = []
        if self._get_origin_stream_positions_cache is not None:
            if (
                self._get_origin_stream_positions_cache.estimated_gap_look_back_start
                == estimated_gap_look_back_start
            ):
                # We are using the cached origin streams
                candidate_streams = self._calculate_selected_streams(
                    origin_streams=self._get_origin_stream_positions_cache.origin_streams,
                    stream_positions=stream_positions,
                    estimated_gap_look_back_start=estimated_gap_look_back_start,
                )
                if candidate_streams:
                    LOGGER.debug(
                        f"{self.stream.name}: Found {len(candidate_streams)} candidate streams (using cached origin streams)"
                    )
        if not candidate_streams:
            # Cached origin streams either did not give us any candidates or
            # were not available.
            origin_streams = self.get_origin_stream_positions(
                conn=conn,
                min_global_position=estimated_gap_look_back_start,
                max_global_position=cutoff,
            )
            self._get_origin_stream_positions_cache = OriginStreamPositionsCache(
                estimated_gap_look_back_start=estimated_gap_look_back_start,
                origin_streams=origin_streams,
            )
            candidate_streams = self._calculate_selected_streams(
                origin_streams=origin_streams,
                stream_positions=stream_positions,
                estimated_gap_look_back_start=estimated_gap_look_back_start,
            )
            LOGGER.debug(
                f"{self.stream.name}: Found {len(candidate_streams)} candidate streams (using live origin streams)"
            )

        # TODO limit so that sum(estimated_message_count) close to batch_size
        return sorted(candidate_streams, key=lambda x: x.start_at_global_position)[
            : self.batch_size
        ]

    def _calculate_selected_streams(
        self,
        origin_streams: Dict[str, OriginStreamPositon],
        stream_positions: Dict[str, AggregatedStreamPositon],
        estimated_gap_look_back_start: int,
    ) -> List[SelectedOriginStream]:
        candidate_streams: List[SelectedOriginStream] = []
        for origin_stream in origin_streams.values():
            stream_position = stream_positions.get(origin_stream.origin_stream)
            if stream_position is None:
                assert (
                    origin_stream.min_global_position >= estimated_gap_look_back_start
                )
                candidate_streams.append(
                    SelectedOriginStream(
                        stream=origin_stream.origin_stream,
                        start_at_global_position=origin_stream.min_global_position,
                        # TODO add estimate? or exact count?
                        # estimated_message_count=origin_stream.max_version,
                    )
                )
            elif (
                stream_position.max_aggregated_stream_global_position
                < origin_stream.max_global_position
            ):
                candidate_streams.append(
                    SelectedOriginStream(
                        stream=origin_stream.origin_stream,
                        start_at_global_position=(
                            stream_position.max_aggregated_stream_global_position + 1
                        ),
                        # TODO add estimate? or exact count?
                        # estimated_message_count=origin_stream.max_version - stream_position.max_aggregated_stream_version,
                    )
                )
        return candidate_streams

    def _update_batch(self, conn: SAConnection, cutoff: Optional[int] = None) -> int:
        message_table = self.stream._store._storage.message_table

        selected_origin_streams = self._select_origin_streams(conn, cutoff=cutoff)
        if not selected_origin_streams:
            return 0

        min_global_position = min(
            selected_stream.start_at_global_position
            for selected_stream in selected_origin_streams
        )
        cutoff_cond = []
        if cutoff is not None:
            cutoff_cond = [message_table.c.global_position <= cutoff]
        qry = (
            _sa.select(
                message_table.c.message_id,
                message_table.c.stream,
                message_table.c.version,
                message_table.c.message,
                message_table.c.global_position,
                message_table.c.added_at,
            )
            .where(
                _sa.and_(
                    message_table.c.global_position >= min_global_position,
                    *cutoff_cond,
                    _sa.or_(
                        *[
                            _sa.and_(
                                message_table.c.stream == selected_stream.stream,
                                message_table.c.global_position
                                >= selected_stream.start_at_global_position,
                            )
                            for selected_stream in selected_origin_streams
                        ]
                    ),
                )
            )
            .order_by(message_table.c.global_position)
            .limit(self.batch_size)
        )
        messages = list(conn.execute(qry).fetchall())

        LOGGER.debug(f"{self.stream.name}: Found {len(messages)} new messages")
        if not messages:
            return 0

        self._add(conn, messages)
        return len(messages)

    def _add(self, conn: SAConnection, messages: List[SARow]) -> None:
        positions = {
            row.partition: row.max_position
            for row in conn.execute(
                _sa.select(
                    self.stream._table.c.partition,
                    _sa.func.max(self.stream._table.c.position).label("max_position"),
                ).group_by(self.stream._table.c.partition)
            )
        }

        rows = []
        for message_id, stream, version, message, global_position, added_at in messages:
            message = StoredMessage(
                message_id=message_id,
                stream=stream,
                version=version,
                message=self.stream._store._serializer.deserialize(message),
                global_position=global_position,
                added_at=added_at,
            )
            partition = self.partitioner.get_partition(message)
            if partition < 0:
                raise ValueError("partition must be >= 0")
            position = positions.get(partition, -1) + 1
            positions[partition] = position
            rows.append(
                (
                    message_id,
                    stream,
                    version,
                    partition,
                    position,
                    message.message.get_message_time(),
                    global_position,
                    added_at,
                )
            )
        self.stream._add(
            conn=conn,
            rows=rows,
        )
