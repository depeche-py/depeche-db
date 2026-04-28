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
    Set,
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
from ._storage import Storage as _Storage
from ._storage import _notify_trigger_function
from ._timings import Timings

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
        self._maxpos_table = _sa.Table(
            self.stream_table_name(name) + "_maxpos",
            self._metadata,
            _sa.Column("partition", _sa.Integer, primary_key=True, autoincrement=False),
            _sa.Column(
                "max_position",
                _sa.Integer,
                nullable=False,
            ),
        )
        # Per-origin-stream meta: the highest origin_stream_global_position
        # we've already projected into the aggregated stream. Maintained by
        # the projector under the EXCLUSIVE lock, so it doesn't need a
        # trigger. Lets _select_origin_streams identify behind streams with
        # a small lookup keyed by origin_stream instead of a GROUP BY scan
        # over the aggregated stream.
        self._origin_meta_table = _sa.Table(
            self.stream_table_name(name) + "_omax",
            self._metadata,
            _sa.Column("origin_stream", _sa.String(255), primary_key=True),
            _sa.Column(
                "max_aggregated_origin_global_position",
                _sa.Integer,
                nullable=False,
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
        # Backfill from the existing aggregated stream rows on first creation.
        # Idempotent thanks to ON CONFLICT DO NOTHING.
        origin_meta_backfill = _sa.DDL(
            f"""
            INSERT INTO {self._origin_meta_table.name}
                (origin_stream, max_aggregated_origin_global_position)
            SELECT origin_stream, MAX(origin_stream_global_position)
            FROM {self._table.name}
            GROUP BY origin_stream
            ON CONFLICT (origin_stream) DO NOTHING;
            """
        )
        _sa.event.listen(
            self._origin_meta_table,
            "after_create",
            origin_meta_backfill.execute_if(dialect="postgresql"),
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

    def get_max_aggregated_stream_positions(
        self,
        conn: Optional[SAConnection] = None,
    ) -> Dict[int, int]:
        def _inner(conn: SAConnection) -> Dict[int, int]:
            result = {
                row.partition: row.max_position
                for row in conn.execute(_sa.select(self._maxpos_table)).fetchall()
            }
            return result

        if conn is None:
            with self._connection() as conn:
                return _inner(conn)
        else:
            return _inner(conn)

    def _update_max_aggregated_stream_positions(
        self, conn: SAConnection, positions: Dict[int, int]
    ) -> None:
        from sqlalchemy.dialects.postgresql import insert

        insert_stmt = insert(self._maxpos_table).values(
            [
                {"partition": partition, "max_position": max_position}
                for partition, max_position in positions.items()
            ]
        )
        insert_with_update = insert_stmt.on_conflict_do_update(
            index_elements=[
                self._maxpos_table.c.partition,
            ],
            set_={self._maxpos_table.c.max_position: insert_stmt.excluded.max_position},
        )
        conn.execute(insert_with_update)

    def _update_origin_meta(
        self, conn: SAConnection, origin_max: Dict[str, int]
    ) -> None:
        if not origin_max:
            return
        from sqlalchemy.dialects.postgresql import insert

        col = "max_aggregated_origin_global_position"
        insert_stmt = insert(self._origin_meta_table).values(
            [
                {"origin_stream": stream, col: max_pos}
                for stream, max_pos in origin_max.items()
            ]
        )
        # GREATEST so we never go backwards if (somehow) a stale write arrives.
        insert_with_update = insert_stmt.on_conflict_do_update(
            index_elements=[self._origin_meta_table.c.origin_stream],
            set_={
                col: _sa.func.greatest(
                    self._origin_meta_table.c[col],
                    insert_stmt.excluded[col],
                ),
            },
        )
        conn.execute(insert_with_update)

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
    def get_migration_ddl_0_14_0(
        cls, aggregated_stream_name: str, message_store_name: str
    ) -> str:
        """
        DDL Script to migrate from < 0.14.0.

        Creates the per-store and per-aggregated-stream meta tables that back
        the projector's fast-path candidate selection, replaces the
        message-store INSERT trigger function with the meta-aware body, and
        backfills both meta tables from the existing rows. The meta-table
        creates and the backfill INSERTs are guarded so re-running the script
        is a no-op.

        The message-store half (meta table, trigger function, meta backfill)
        is identical for every aggregated stream that targets the same store,
        so when generating one combined script for several streams the
        store-side block is emitted once per stream and runs idempotently.
        """
        message_tablename = _Storage.message_table_name(message_store_name)
        meta_tablename = _Storage.meta_table_name(message_store_name)
        aggregated_stream_tablename = cls.stream_table_name(aggregated_stream_name)
        omax_tablename = aggregated_stream_tablename + "_omax"
        new_trigger_function = _notify_trigger_function(
            name=message_store_name,
            meta_tablename=meta_tablename,
            notification_channel=_Storage.notification_channel_name(message_store_name),
        )
        return _textwrap.dedent(
            f"""
            -- Per-stream meta on the message store (idempotent)
            CREATE TABLE IF NOT EXISTS {meta_tablename} (
                stream VARCHAR(255) PRIMARY KEY,
                min_global_position INTEGER NOT NULL,
                max_global_position INTEGER NOT NULL
            );

            -- Refresh the message-store INSERT trigger function so future
            -- writes upsert into the meta table. The trigger itself does not
            -- need to be re-created — it references the function by name.
            {new_trigger_function}

            -- Backfill the per-stream meta from existing messages
            INSERT INTO {meta_tablename}
                (stream, min_global_position, max_global_position)
            SELECT stream, MIN(global_position), MAX(global_position)
            FROM {message_tablename}
            GROUP BY stream
            ON CONFLICT (stream) DO NOTHING;

            -- Per-origin-stream omax on the aggregated stream (idempotent)
            CREATE TABLE IF NOT EXISTS {omax_tablename} (
                origin_stream VARCHAR(255) PRIMARY KEY,
                max_aggregated_origin_global_position INTEGER NOT NULL
            );

            -- Backfill omax from the aggregated stream rows
            INSERT INTO {omax_tablename}
                (origin_stream, max_aggregated_origin_global_position)
            SELECT origin_stream, MAX(origin_stream_global_position)
            FROM {aggregated_stream_tablename}
            GROUP BY origin_stream
            ON CONFLICT (origin_stream) DO NOTHING;
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
            (0, 14): [
                cls.get_migration_ddl_0_14_0,
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
        self._checked_maxpos_table = False
        self.timings = Timings(enabled=False)

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
        with self.timings.span("update_full"):
            with self.stream._store.engine.connect() as conn:
                with self.timings.span("get_global_position"):
                    cutoff = self.stream._store._storage.get_global_position(conn)
                try:
                    with self.timings.span("lock_table"):
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

                with self.timings.span("check_maxpos_table"):
                    self._check_and_create_maxpos_table(conn)

                # Read the per-partition maxpos once and mutate the dict in
                # memory across batches, writing it back once at the end.
                with self.timings.span("get_maxpos"):
                    maxpos = self.stream.get_max_aggregated_stream_positions(conn)
                touched_partitions: Set[int] = set()

                while True:
                    with self.timings.span("update_batch"):
                        batch_num = self._update_batch(
                            conn,
                            cutoff,
                            maxpos=maxpos,
                            touched_partitions=touched_partitions,
                        )
                    result += batch_num
                    LOGGER.debug(
                        f"{self.stream.name}: Batch updated: {batch_num} messages"
                    )
                    if batch_num < self.batch_size:
                        # No more messages to process
                        break
                    if budget and budget.over_budget():
                        # Budget exceeded, stop processing
                        break

                if touched_partitions:
                    with self.timings.span("update_maxpos"):
                        self.stream._update_max_aggregated_stream_positions(
                            conn=conn,
                            positions={p: maxpos[p] for p in touched_partitions},
                        )

                with self.timings.span("commit"):
                    conn.commit()
        return FullUpdateResult(result, batch_num == self.batch_size)

    def _check_and_create_maxpos_table(self, conn: SAConnection) -> None:
        # The maxpos table was only introduced in 0.12.3, so we need to check
        # if it has data and fill it if not.
        if self._checked_maxpos_table:
            return

        if self.stream.get_max_aggregated_stream_positions(conn):
            return

        positions = {
            row.partition: row.max_position
            for row in conn.execute(
                _sa.select(
                    self.stream._table.c.partition,
                    _sa.func.max(self.stream._table.c.position).label("max_position"),
                ).group_by(self.stream._table.c.partition)
            )
        }
        if positions:
            self.stream._update_max_aggregated_stream_positions(
                conn=conn, positions=positions
            )
        self._checked_maxpos_table = True

    def _select_origin_streams(
        self, conn: SAConnection, cutoff: Optional[int] = None
    ) -> List[SelectedOriginStream]:
        # Single joined query against the two meta tables: returns only the
        # origin streams that are actually behind (msgs_meta has more than
        # has been projected into omax), with start_at already computed
        # SQL-side. Limited to batch_size candidates ordered by start
        # position so the per-batch read covers the lowest-position work
        # first.
        del cutoff  # _update_batch caps by cutoff when reading messages

        msgs_meta = self.stream._store._storage.meta_table
        omax = self.stream._origin_meta_table

        agg_max_or_minus_one = _sa.func.coalesce(
            omax.c.max_aggregated_origin_global_position, -1
        )
        # For never-projected streams: start at the stream's first ever
        # message. For partially-projected streams: the next position after
        # what we already have.
        start_at_expr = _sa.case(
            (
                omax.c.max_aggregated_origin_global_position.is_(None),
                msgs_meta.c.min_global_position,
            ),
            else_=omax.c.max_aggregated_origin_global_position + 1,
        )

        qry = (
            _sa.select(
                msgs_meta.c.stream.label("stream"),
                start_at_expr.label("start_at_global_position"),
            )
            .select_from(
                msgs_meta.outerjoin(omax, omax.c.origin_stream == msgs_meta.c.stream)
            )
            .where(
                _sa.and_(
                    msgs_meta.c.max_global_position > agg_max_or_minus_one,
                    _sa.or_(
                        *[
                            msgs_meta.c.stream.like(wildcard)
                            for wildcard in self.stream_wildcards
                        ]
                    ),
                )
            )
            .order_by(start_at_expr)
            .limit(self.batch_size)
        )

        with self.timings.span("select.candidates"):
            candidates = [
                SelectedOriginStream(
                    stream=row.stream,
                    start_at_global_position=row.start_at_global_position,
                )
                for row in conn.execute(qry)
            ]

        LOGGER.debug(f"{self.stream.name}: Found {len(candidates)} candidate streams")
        return candidates

    def _update_batch(
        self,
        conn: SAConnection,
        cutoff: Optional[int],
        *,
        maxpos: Dict[int, int],
        touched_partitions: Set[int],
    ) -> int:
        message_table = self.stream._store._storage.message_table

        with self.timings.span("select_origin_streams"):
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
        with self.timings.span("read_messages"):
            messages = list(conn.execute(qry).fetchall())

        LOGGER.debug(f"{self.stream.name}: Found {len(messages)} new messages")
        if not messages:
            return 0

        with self.timings.span("add"):
            self._add(
                conn,
                messages,
                maxpos=maxpos,
                touched_partitions=touched_partitions,
            )
        return len(messages)

    def _add(
        self,
        conn: SAConnection,
        messages: List[SARow],
        *,
        maxpos: Dict[int, int],
        touched_partitions: Set[int],
    ) -> None:
        # omax is always tracked per batch and written at the end of the
        # batch so the next iteration's _select_origin_streams sees the
        # updated state via the omax table.
        origin_max_in_batch: Dict[str, int] = {}

        with self.timings.span("add.deserialize_and_partition"):
            rows = []
            for (
                message_id,
                stream,
                version,
                message,
                global_position,
                added_at,
            ) in messages:
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
                position = maxpos.get(partition, -1) + 1
                maxpos[partition] = position
                touched_partitions.add(partition)
                if global_position > origin_max_in_batch.get(stream, -1):
                    origin_max_in_batch[stream] = global_position
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

        with self.timings.span("add.insert_rows"):
            conn.execute(self.stream._table.insert().values(rows))
        with self.timings.span("add.update_origin_meta"):
            self.stream._update_origin_meta(conn=conn, origin_max=origin_max_in_batch)
