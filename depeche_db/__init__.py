from psycopg2.errors import LockNotAvailable
import contextlib as _contextlib
import dataclasses as _dc
import datetime as _dt
import uuid as _uuid
from typing import Generic, Iterable, Iterator, Protocol, TypeVar

import sqlalchemy as _sa
import sqlalchemy.orm as _orm
from sqlalchemy_utils import UUIDType as _UUIDType


class MessageProtocol:
    def get_message_id(self) -> _uuid.UUID:
        ...

    def get_message_time(self) -> _dt.datetime:
        ...


E = TypeVar("E", bound=MessageProtocol)


@_dc.dataclass(frozen=True)
class StoredMessage(Generic[E]):
    message_id: _uuid.UUID
    stream: str
    version: int
    message: E
    global_position: int


@_dc.dataclass(frozen=True)
class MessagePosition:
    stream: str
    version: int
    global_position: int


class Storage:
    name: str

    def __init__(self, name: str, engine: _sa.engine.Engine):
        assert name.isidentifier(), "name must be a valid identifier"
        self.name = name
        self.metadata = _sa.MetaData()
        self.message_table = _sa.Table(
            f"{name}_messages",
            self.metadata,
            _sa.Column("message_id", _UUIDType(), primary_key=True),
            _sa.Column(
                "global_position",
                _sa.Integer,
                _sa.Sequence(f"{name}_messages_global_position_seq"),
                unique=True,
                nullable=False,
            ),
            _sa.Column(
                "added_at", _sa.DateTime, nullable=False, server_default=_sa.func.now()
            ),
            _sa.Column("stream", _sa.String(255), nullable=False),
            _sa.Column("version", _sa.Integer, nullable=False),
            _sa.Column("message", _sa.JSON, nullable=False),
            # TODO ensure concurrent writes do not insert same version into
            # a stream twice:
            # _sa.UniqueConstraint("stream", "version", name="stream_version_unique"),
            # OR: lock `add_all` over stream name?
        )
        self.notification_channel = f"{name}_messages"
        trigger = _sa.DDL(
            f"""
            CREATE OR REPLACE FUNCTION {name}_notify_message_inserted()
              RETURNS trigger AS $$
            DECLARE
            BEGIN
              PERFORM pg_notify(
                '{self.notification_channel}',
                json_build_object(
                    'message_id', NEW.message_id,
                    'stream', NEW.stream,
                    'version', NEW.version,
                    'global_position', NEW.global_position
                )::text);
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER {name}_notify_message_inserted
              AFTER INSERT ON {name}_messages
              FOR EACH ROW
              EXECUTE PROCEDURE {name}_notify_message_inserted();
            """
        )
        _sa.event.listen(
            self.message_table, "after_create", trigger.execute_if(dialect="postgresql")
        )
        self.metadata.create_all(engine, checkfirst=True)

    def add(
        self,
        conn: _sa.Connection,
        stream: str,
        expected_version: int,
        message_id: _uuid.UUID,
        message: dict,
    ) -> MessagePosition:
        return self.add_all(conn, stream, expected_version, [(message_id, message)])

    def add_all(
        self,
        conn: _sa.Connection,
        stream: str,
        expected_version: int,
        messages: list[tuple[_uuid.UUID, dict]],
    ) -> MessagePosition:
        if expected_version > 0:
            if self.get_max_version(conn, stream).version != expected_version:
                raise ValueError("optimistic concurrency failure")
        conn.execute(
            self.message_table.insert(),
            [
                {
                    "message_id": message_id,
                    "stream": stream,
                    "version": expected_version + i + 1,
                    "message": message,
                }
                for i, (message_id, message) in enumerate(messages)
            ],
        )
        return self.get_max_version(conn, stream)

    def get_max_version(self, conn: _sa.Connection, stream: str) -> MessagePosition:
        row = conn.execute(
            _sa.select(
                _sa.func.max(self.message_table.c.version).label("version"),
                _sa.func.max(self.message_table.c.global_position).label(
                    "global_position"
                ),
            )
            .select_from(self.message_table)
            .where(self.message_table.c.stream == stream),
        ).fetchone()
        if not row:
            return MessagePosition(stream, 0, 0)
        return MessagePosition(stream, row.version, row.global_position)

    def get_message_ids(
        self, conn: _sa.Connection, stream: str
    ) -> Iterable[_uuid.UUID]:
        return conn.execute(
            _sa.select(self.message_table.c.message_id)
            .select_from(self.message_table)
            .where(self.message_table.c.stream == stream)
            .order_by(self.message_table.c.version)
        ).scalars()

    def read(
        self, conn: _sa.Connection, stream: str
    ) -> Iterable[tuple[_uuid.UUID, int, dict, int]]:
        return conn.execute(  # type: ignore
            _sa.select(
                self.message_table.c.message_id,
                self.message_table.c.version,
                self.message_table.c.message,
                self.message_table.c.global_position,
            )
            .select_from(self.message_table)
            .where(self.message_table.c.stream == stream)
            .order_by(self.message_table.c.version)
        )

    def read_multiple(
        self, conn: _sa.Connection, streams: list[str]
    ) -> Iterable[tuple[_uuid.UUID, str, int, dict, int]]:
        return conn.execute(  # type: ignore
            _sa.select(
                self.message_table.c.message_id,
                self.message_table.c.stream,
                self.message_table.c.version,
                self.message_table.c.message,
                self.message_table.c.global_position,
            )
            .select_from(self.message_table)
            .where(self.message_table.c.stream.in_(streams))
            .order_by(self.message_table.c.global_position)
        )

    def read_wildcard(
        self, conn: _sa.Connection, stream_wildcard: str
    ) -> Iterable[tuple[_uuid.UUID, str, int, dict, int]]:
        return conn.execute(  # type: ignore
            _sa.select(
                self.message_table.c.message_id,
                self.message_table.c.stream,
                self.message_table.c.version,
                self.message_table.c.message,
                self.message_table.c.global_position,
            )
            .select_from(self.message_table)
            .where(self.message_table.c.stream.like(stream_wildcard))
            .order_by(self.message_table.c.global_position)
        )

    def get_message_by_id(
        self, conn: _sa.Connection, message_id: _uuid.UUID
    ) -> tuple[_uuid.UUID, str, int, dict, int]:
        return conn.execute(  # type: ignore
            _sa.select(
                self.message_table.c.message_id,
                self.message_table.c.stream,
                self.message_table.c.version,
                self.message_table.c.message,
                self.message_table.c.global_position,
            ).where(self.message_table.c.message_id == message_id)
        ).first()

    def get_messages_by_ids(
        self, conn: _sa.Connection, message_ids: list[_uuid.UUID]
    ) -> Iterable[tuple[_uuid.UUID, str, int, dict, int]]:
        return conn.execute(  # type: ignore
            _sa.select(
                self.message_table.c.message_id,
                self.message_table.c.stream,
                self.message_table.c.version,
                self.message_table.c.message,
                self.message_table.c.global_position,
            ).where(self.message_table.c.message_id.in_(message_ids))
        )

    def truncate(self, conn: _sa.Connection):
        conn.execute(self.message_table.delete())


class MessageSerializer(Generic[E]):
    def serialize(self, message: E) -> dict:
        raise NotImplementedError()

    def deserialize(self, message: dict) -> E:
        raise NotImplementedError()


class MessageNotFound(Exception):
    pass


class MessageStoreReader(Generic[E]):
    def __init__(
        self, conn: _sa.Connection, storage: Storage, serializer: MessageSerializer[E]
    ):
        self._conn = conn
        self._storage = storage
        self._serializer = serializer

    def get_message_by_id(self, message_id: _uuid.UUID) -> StoredMessage[E]:
        row = self._storage.get_message_by_id(conn=self._conn, message_id=message_id)
        if row:
            message_id, stream, version, message, global_position = row
            return StoredMessage(
                message_id=message_id,
                stream=stream,
                version=version,
                message=self._serializer.deserialize(message),
                global_position=global_position,
            )
        raise MessageNotFound(message_id)

    def get_messages_by_ids(
        self, message_ids: list[_uuid.UUID]
    ) -> Iterable[StoredMessage[E]]:
        for row in self._storage.get_messages_by_ids(
            conn=self._conn, message_ids=message_ids
        ):
            message_id, stream, version, message, global_position = row
            yield StoredMessage(
                message_id=message_id,
                stream=stream,
                version=version,
                message=self._serializer.deserialize(message),
                global_position=global_position,
            )

    def read(self, stream: str) -> Iterable[StoredMessage[E]]:
        for message_id, version, message, global_position in self._storage.read(
            self._conn, stream
        ):
            yield StoredMessage(
                message_id=message_id,
                stream=stream,
                version=version,
                message=self._serializer.deserialize(message),
                global_position=global_position,
            )

    def read_wildcard(self, stream_wildcard: str) -> Iterable[StoredMessage[E]]:
        for (
            message_id,
            stream,
            version,
            message,
            global_position,
        ) in self._storage.read_wildcard(self._conn, stream_wildcard):
            yield StoredMessage(
                message_id=message_id,
                stream=stream,
                version=version,
                message=self._serializer.deserialize(message),
                global_position=global_position,
            )


class MessageStore(Generic[E]):
    def __init__(
        self,
        name: str,
        engine: _sa.engine.Engine,
        serializer: MessageSerializer[E],
    ):
        self.engine = engine
        self._storage = Storage(name=name, engine=engine)
        self._serializer = serializer

    def _get_connection(self) -> _sa.Connection:
        return self.engine.connect()

    def truncate(self):
        with self._get_connection() as conn:
            self._storage.truncate(conn)

    def write(
        self, stream: str, message: E, expected_version: int = -1
    ) -> MessagePosition:
        with self._get_connection() as conn:
            result = self._storage.add(
                conn=conn,
                stream=stream,
                expected_version=expected_version,
                message_id=message.get_message_id(),
                message=self._serializer.serialize(message),
            )
            conn.commit()
            return result

    def synchronize(
        self, stream: str, expected_version: int, messages: list[E]
    ) -> MessagePosition:
        with self._get_connection() as conn:
            stored_version = self._storage.get_max_version(conn, stream)
            if stored_version is not None:
                stored_ids = list(self._storage.get_message_ids(conn, stream))
                message_ids = [message.get_message_id() for message in messages]
                for stored, given in zip(stored_ids, message_ids):
                    if stored != given:
                        raise ValueError("Message ID mismatch")
                messages = messages[len(stored_ids) :]
            if messages:
                result = self._storage.add_all(
                    conn,
                    stream,
                    expected_version,
                    [
                        (
                            message.get_message_id(),
                            self._serializer.serialize(message),
                        )
                        for message in messages
                    ],
                )
                conn.commit()
            else:
                result = stored_version
            return result

    def _get_reader(self, conn: _sa.Connection) -> MessageStoreReader[E]:
        return MessageStoreReader(
            conn=conn,
            storage=self._storage,
            serializer=self._serializer,
        )

    @_contextlib.contextmanager
    def reader(
        self, conn: _sa.Connection | None = None
    ) -> Iterator[MessageStoreReader[E]]:
        if conn:
            yield self._get_reader(conn)
        else:
            with self._get_connection() as conn:
                yield self._get_reader(conn)

    def read(self, stream: str) -> Iterable[StoredMessage[E]]:
        with self.reader() as reader:
            yield from reader.read(stream)


@_dc.dataclass
class StreamPartitionStatistic:
    partition_number: int
    next_message_id: _uuid.UUID
    next_message_position: int
    next_message_occurred_at: _dt.datetime


class LinkStream(Generic[E]):
    def __init__(self, name: str, store: MessageStore):
        # TODO start at "next message"
        # TODO start at time
        assert name.isidentifier(), "name must be a valid identifier"
        self.name = name
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

    def has_message(self, conn: _sa.Connection, message_id: _uuid.UUID) -> bool:
        return conn.execute(
            _sa.select(_sa.exists().where(self._table.c.message_id == message_id))
        ).scalar()

    def truncate(self, conn: _sa.Connection):
        conn.execute(self._table.delete())

    def add(
        self,
        conn: _sa.Connection,
        message_id: _uuid.UUID,
        stream: str,
        stream_version: int,
        partition: int,
        position: int,
        message_occurred_at: _dt.datetime,
    ) -> None:
        res = conn.execute(
            self._table.insert().values(
                message_id=message_id,
                origin_stream=stream,
                origin_stream_version=stream_version,
                partition=partition,
                position=position,
                message_occurred_at=message_occurred_at,
            )
        )

    def read(self, conn: _sa.Connection, partition: int) -> Iterable[_uuid.UUID]:
        for row in conn.execute(
            _sa.select(self._table.c.message_id)
            .where(self._table.c.partition == partition)
            .order_by(self._table.c.position)
        ):
            yield row.message_id

    @_contextlib.contextmanager
    def _connection(self):
        conn = self._store.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def get_partition_statistics(
        self,
        position_limits: dict[int, int] = None,
        result_limit: int | None = None,
    ) -> Iterable[StreamPartitionStatistic]:
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
                # TODO randomize order a bit, e.g. order by (hour(occurred_at), random())
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


class MessagePartitioner(Generic[E]):
    def get_partition(self, message: StoredMessage[E]) -> int:
        # TODO why not return `list[int]`?
        # -> an message can be part of multiple partitions
        raise NotImplementedError


class StreamNamePartitioner(MessagePartitioner[E]):
    def get_partition(self, message: StoredMessage[E]) -> int:
        import hashlib

        return int.from_bytes(
            hashlib.sha256(message.stream.encode()).digest()[:2], "little"
        )


class StreamProjector(Generic[E]):
    def __init__(
        self,
        stream: LinkStream[E],
        partitioner: MessagePartitioner[E],
        stream_wildcards: list[str],
    ):
        self.stream = stream
        self.stream_wildcards = stream_wildcards
        self.partitioner = partitioner
        self.batch_size = 100

    def update_full(self) -> int:
        result = 0
        with self.stream._store.engine.connect() as conn:
            try:
                res = conn.execute(
                    _sa.text(
                        f"LOCK TABLE {self.stream._table.name} IN SHARE MODE NOWAIT"
                    )
                )
            except _sa.exc.OperationalError as exc:
                if isinstance(exc.orig, LockNotAvailable):
                    raise RuntimeError(
                        "Cannot update stream projection, because another process is already updating it."
                    )
                raise
            while True:
                batch_num = self._update_batch(conn)
                if batch_num == 0:
                    break
                result += batch_num
            conn.commit()
        return result

    def _update_batch(self, conn):
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
                )
            )
            .order_by(message_table.c.global_position)
            .limit(self.batch_size)
        )
        messages = list(conn.execute(q))
        self.add(conn, messages)
        return len(messages)

    def add(self, conn, messages):
        positions = {
            row.partition: row.position
            for row in conn.execute(
                _sa.select(
                    self.stream._table.c.partition,
                    self.stream._table.c.position,
                )
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


class LockProvider(Protocol):
    def lock(self, name: str) -> bool:
        raise NotImplementedError

    def unlock(self, name: str):
        raise NotImplementedError


@_dc.dataclass
class SubscriptionState:
    positions: dict[int, int]


class SubscriptionStateProvider(Protocol):
    def store(self, group_name: str, partition: int, position: int):
        raise NotImplementedError

    def read(self, group_name: str) -> SubscriptionState:
        raise NotImplementedError


class DbSubscriptionStateProvider:
    def __init__(self, name: str, engine: _sa.engine.Engine):
        assert name.isidentifier(), "Name must be a valid identifier"
        self.name = name
        self._engine = engine

        self.metadata = _sa.MetaData()
        self.state_table = _sa.Table(
            f"{name}_subscription_state",
            self.metadata,
            _sa.Column("group_name", _sa.String, primary_key=True),
            _sa.Column("partition", _sa.Integer, primary_key=True),
            _sa.Column("position", _sa.Integer, nullable=False),
        )
        self.metadata.create_all(self._engine)

    def store(self, group_name: str, partition: int, position: int):
        from sqlalchemy.dialects.postgresql import insert

        with self._engine.connect() as conn:
            conn.execute(
                insert(self.state_table)
                .values(group_name=group_name, partition=partition, position=position)
                .on_conflict_do_update(
                    index_elements=[
                        self.state_table.c.group_name,
                        self.state_table.c.partition,
                    ],
                    set_={
                        self.state_table.c.position: position,
                    },
                )
            )
            conn.commit()

    def read(self, group_name: str) -> SubscriptionState:
        with self._engine.connect() as conn:
            return SubscriptionState(
                {
                    row.partition: row.position
                    for row in conn.execute(
                        _sa.select(
                            self.state_table.c.partition,
                            self.state_table.c.position,
                        ).where(self.state_table.c.group_name == group_name)
                    )
                }
            )


@_dc.dataclass(frozen=True)
class SubscriptionMessage(Generic[E]):
    partition: int
    position: int
    stored_message: StoredMessage[E]
    _subscription: "Subscription[E]"

    def ack(self):
        self._subscription.ack_message(self)

    # TODO nack (dead letter queue etc?)


class Subscription(Generic[E]):
    def __init__(
        self,
        group_name: str,
        stream: LinkStream[E],
        state_provider: SubscriptionStateProvider,
        lock_provider: LockProvider,
        # TODO start at time
        # TODO start at "next message"
    ):
        assert group_name.isidentifier(), "Group name must be a valid identifier"
        self.group_name = group_name
        self._stream = stream
        self._lock_provider = lock_provider
        self._state_provider = state_provider

    @_contextlib.contextmanager
    def get_next_message(self) -> Iterator[SubscriptionMessage[E]]:
        # TODO get more than one message (accept a batch size parameter)
        state = self._state_provider.read(self.group_name)
        statistics = list(
            self._stream.get_partition_statistics(
                position_limits=state.positions, result_limit=10
            )
        )
        for statistic in statistics:
            lock_key = f"subscription-{self.group_name}-{statistic.partition_number}"
            if not self._lock_provider.lock(lock_key):
                continue
            # now we have the lock, we need to check if the position is still valid
            # if not, we need to release the lock and try the next partition
            state = self._state_provider.read(self.group_name)
            if state.positions.get(statistic.partition_number, -1) != (
                statistic.next_message_position - 1
            ):
                self._lock_provider.unlock(lock_key)
                continue
            try:
                with self._stream._store.reader() as reader:
                    yield SubscriptionMessage(
                        partition=statistic.partition_number,
                        position=statistic.next_message_position,
                        stored_message=reader.get_message_by_id(
                            statistic.next_message_id
                        ),
                        _subscription=self,
                    )
                break
            finally:
                self._lock_provider.unlock(lock_key)
        else:
            yield None

    def ack(self, partition: int, position: int):
        state = self._state_provider.read(self.group_name)
        assert (
            state.positions.get(partition, -1) == position - 1
        ), f"{partition} should have {position - 1} as last position, but has {state.positions.get(partition, -1)}"
        self._state_provider.store(
            group_name=self.group_name, partition=partition, position=position
        )

    def ack_message(self, message: SubscriptionMessage[E]):
        self.ack(partition=message.partition, position=message.position)
