"""
The storage implementation in PL/SQL is heavily inspired by
https://github.com/message-db/message-db
"""

import datetime as _dt
import uuid as _uuid
from typing import Any, Callable, Iterator, Optional, Sequence, Tuple

import sqlalchemy as _sa
from sqlalchemy.dialects.postgresql import JSONB as _PostgresJsonb
from sqlalchemy_utils import UUIDType as _UUIDType

from ._compat import PsycoPgJson, SAConnection
from ._exceptions import OptimisticConcurrencyError
from ._interfaces import MessagePosition


class Storage:
    name: str

    def __init__(self, name: str, engine: _sa.engine.Engine):
        assert name.isidentifier(), "name must be a valid identifier"
        self.name = name
        self.metadata = _sa.MetaData()
        self.message_table = _sa.Table(
            self.message_table_name(name),
            self.metadata,
            _sa.Column("message_id", _UUIDType(), primary_key=True),
            _sa.Column(
                "global_position",
                _sa.Integer,
                _sa.Sequence(f"depeche_msgs_{name}_global_seq"),
                unique=True,
                nullable=False,
            ),
            _sa.Column(
                "added_at", _sa.DateTime, nullable=False, server_default=_sa.func.now()
            ),
            _sa.Column("stream", _sa.String(255), nullable=False),
            _sa.Column("version", _sa.Integer, nullable=False),
            _sa.Column("message", _PostgresJsonb, nullable=False),
            # TODO is this still required? only add in tests?
            _sa.UniqueConstraint(
                "stream", "version", name=f"depeche_msgs_{name}_version_uq"
            ),
        )
        # Per-stream meta: the lowest and highest global_position written for
        # each stream. Maintained by the INSERT trigger so the projector can
        # avoid the slow GROUP BY scan it would otherwise need to discover
        # stream positions. min_global_position is set on first insert and
        # never updated; max_global_position grows monotonically.
        self.meta_table = _sa.Table(
            self.meta_table_name(name),
            self.metadata,
            _sa.Column("stream", _sa.String(255), primary_key=True),
            _sa.Column("min_global_position", _sa.Integer, nullable=False),
            _sa.Column("max_global_position", _sa.Integer, nullable=False),
        )
        self.notification_channel = self.notification_channel_name(name)
        ddl = _sa.DDL(
            "\n".join(
                [
                    _notify_trigger(
                        name=name,
                        tablename=self.message_table.name,
                        meta_tablename=self.meta_table.name,
                        notification_channel=self.notification_channel,
                    ),
                    _write_message_fn(name=name, tablename=self.message_table.name),
                ]
            )
        )
        _sa.event.listen(
            self.message_table, "after_create", ddl.execute_if(dialect="postgresql")
        )
        # Create the tables and, when the meta table is added (a fresh
        # install, or an upgrade from a pre-meta version), run the meta
        # migration. _setup_schema serializes this across processes with an
        # advisory lock and runs each migration step in its own committed
        # transaction -- so the meta-aware trigger function is committed, and
        # visible to concurrent writers, *before* the backfill scan that
        # depends on it. That keeps the upgrade safe without pausing writers.
        _setup_schema(
            engine,
            self.metadata,
            lock_name=self.meta_table.name,
            needs_migration=lambda conn: self._meta_migration_needed(conn, name),
            migration_steps=self._meta_migration_steps(name),
        )
        self._select = _sa.select(
            self.message_table.c.message_id,
            self.message_table.c.stream,
            self.message_table.c.version,
            self.message_table.c.message,
            self.message_table.c.global_position,
            self.message_table.c.added_at,
        )
        self._select_without_stream = _sa.select(
            self.message_table.c.message_id,
            self.message_table.c.version,
            self.message_table.c.message,
            self.message_table.c.global_position,
            self.message_table.c.added_at,
        )

    def add(
        self,
        conn: SAConnection,
        stream: str,
        expected_version: Optional[int],
        message_id: _uuid.UUID,
        message: dict,
    ) -> MessagePosition:
        return self.add_all(conn, stream, expected_version, [(message_id, message)])

    def add_all(
        self,
        conn: SAConnection,
        stream: str,
        expected_version: Optional[int],
        messages: Sequence[Tuple[_uuid.UUID, dict]],
    ) -> MessagePosition:
        assert len(messages) > 0
        func = getattr(_sa.func, f"depeche_write_message_{self.name}")
        try:
            for idx, (message_id, message) in enumerate(messages):
                _expected_version = (
                    expected_version + idx if expected_version is not None else None
                )
                result: Any = conn.execute(
                    _sa.select(
                        _sa.column("version"), _sa.column("global_position")
                    ).select_from(
                        func(
                            message_id, stream, PsycoPgJson(message), _expected_version
                        ).alias()
                    )
                )
        except _sa.exc.InternalError as exc:
            # psycopg2
            from depeche_db._compat import PsycoPgRaiseException

            if isinstance(exc.orig, PsycoPgRaiseException):
                raise OptimisticConcurrencyError(
                    f"optimistic concurrency failure: {exc.orig}"
                )
            raise
        except _sa.exc.ProgrammingError as exc:
            # psycopg3
            from depeche_db._compat import PsycoPgRaiseException

            if isinstance(exc.orig, PsycoPgRaiseException):
                raise OptimisticConcurrencyError(
                    f"optimistic concurrency failure: {exc.orig}"
                )
            raise
        row = result.fetchone()
        return MessagePosition(
            stream=stream,
            version=row.version,
            global_position=row.global_position,
        )

    def get_global_position(self, conn: SAConnection) -> int:
        result: Any = conn.execute(
            _sa.select(_sa.func.max(self.message_table.c.global_position))
        )
        return result.scalar() or 0

    def get_max_version(self, conn: SAConnection, stream: str) -> MessagePosition:
        row = conn.execute(
            _sa.select(
                _sa.func.max(self.message_table.c.version).label("version"),
                _sa.func.max(self.message_table.c.global_position).label(
                    "global_position"
                ),
            ).where(self.message_table.c.stream == stream),
        ).fetchone()
        if not row or row.version is None:
            return MessagePosition(stream, 0, None)
        return MessagePosition(stream, row.version, row.global_position)

    def get_message_ids(self, conn: SAConnection, stream: str) -> Iterator[_uuid.UUID]:
        for id in conn.execute(
            _sa.select(self.message_table.c.message_id)
            .where(self.message_table.c.stream == stream)
            .order_by(self.message_table.c.version)
        ).scalars():
            yield id

    def read(
        self, conn: SAConnection, stream: str, min_version: Optional[int] = None
    ) -> Iterator[Tuple[_uuid.UUID, int, dict, int, _dt.datetime]]:
        query = self._select_without_stream.where(self.message_table.c.stream == stream)
        if min_version is not None:
            query = query.where(self.message_table.c.version >= min_version)
        return conn.execute(query.order_by(self.message_table.c.version))  # type: ignore

    def read_multiple(
        self, conn: SAConnection, streams: Sequence[str]
    ) -> Iterator[Tuple[_uuid.UUID, str, int, dict, int, _dt.datetime]]:
        return conn.execute(  # type: ignore
            self._select.where(self.message_table.c.stream.in_(streams)).order_by(
                self.message_table.c.global_position
            )
        )

    def read_wildcard(
        self, conn: SAConnection, stream_wildcard: str
    ) -> Iterator[Tuple[_uuid.UUID, str, int, dict, int, _dt.datetime]]:
        return conn.execute(  # type: ignore
            self._select.where(
                self.message_table.c.stream.like(stream_wildcard)
            ).order_by(self.message_table.c.global_position)
        )

    def get_message_by_id(
        self, conn: SAConnection, message_id: _uuid.UUID
    ) -> Tuple[_uuid.UUID, str, int, dict, int, _dt.datetime]:
        return conn.execute(  # type: ignore
            self._select.where(self.message_table.c.message_id == message_id)
        ).first()

    def get_messages_by_ids(
        self, conn: SAConnection, message_ids: Sequence[_uuid.UUID]
    ) -> Iterator[Tuple[_uuid.UUID, str, int, dict, int, _dt.datetime]]:
        return conn.execute(  # type: ignore
            self._select.where(self.message_table.c.message_id.in_(message_ids))
        )

    def truncate(self, conn: SAConnection):
        conn.execute(self.message_table.delete())

    @staticmethod
    def message_table_name(name: str) -> str:
        return f"depeche_msgs_{name}"

    @staticmethod
    def meta_table_name(name: str) -> str:
        return f"depeche_msgs_{name}_meta"

    @staticmethod
    def notification_channel_name(name: str) -> str:
        return f"depeche_{name}_messages"

    @classmethod
    def get_migration_ddl(cls, name: str):
        """
        DDL Script to migrate from <=0.8.0
        """
        tablename = cls.message_table_name(name)
        new_objects = "\n".join(
            [
                _notify_trigger(
                    name=name,
                    tablename=tablename,
                    meta_tablename=cls.meta_table_name(name),
                    notification_channel=cls.notification_channel_name(name),
                ),
                _write_message_fn(name=name, tablename=tablename),
            ]
        )
        return f"""
            ALTER TABLE {name}_messages
                 RENAME TO {tablename};
            DROP TRIGGER {name}_notify_message_inserted;
            DROP FUNCTION IF EXISTS {name}_notify_message_inserted;
            DROP FUNCTION IF EXISTS {name}_write_message;
            {new_objects}
            """

    @classmethod
    def migrate_db_objects(cls, name: str, conn: SAConnection):
        """
        Migrate from <=0.8.0
        """
        conn.execute(cls.get_migration_ddl(name=name))

    @classmethod
    def _meta_migration_steps(cls, name: str) -> Sequence[str]:
        """
        SQL steps that add the meta-table machinery to a message store.

        Run on a fresh install (where they are harmless no-ops) and, more
        importantly, on an upgrade from a pre-0.14 version. _setup_schema
        executes each step in its own committed transaction:

          1. Swap the INSERT trigger function for the meta-aware body. Once
             this commits, every subsequent write maintains the meta table.
          2. Backfill the meta table from the existing messages. Because the
             swap is already committed, writes landing during this
             (potentially slow) scan are captured by the new trigger; the
             LEAST/GREATEST upsert then merges the backfilled bounds with
             whatever the trigger has written -- so no writer downtime is
             needed and a stream first written mid-migration keeps its true
             min/max.
        """
        meta_tablename = cls.meta_table_name(name)
        message_tablename = cls.message_table_name(name)
        return [
            _notify_trigger_function(
                name=name,
                meta_tablename=meta_tablename,
                notification_channel=cls.notification_channel_name(name),
            ),
            f"""
            INSERT INTO {meta_tablename}
                (stream, min_global_position, max_global_position)
            SELECT stream, MIN(global_position), MAX(global_position)
            FROM {message_tablename}
            GROUP BY stream
            ON CONFLICT (stream) DO UPDATE SET
                min_global_position = LEAST(
                    {meta_tablename}.min_global_position,
                    EXCLUDED.min_global_position
                ),
                max_global_position = GREATEST(
                    {meta_tablename}.max_global_position,
                    EXCLUDED.max_global_position
                );
            """,
        ]

    @classmethod
    def _meta_migration_needed(cls, conn: SAConnection, name: str) -> bool:
        """
        Decide whether the meta migration steps should run. Evaluated by
        _setup_schema before the tables are created, so it observes the
        pre-upgrade state.

        True when the meta table is absent, or present but empty while the
        message store already has rows. The latter case means a previous
        backfill did not finish (e.g. the process died mid-scan): because the
        steps are idempotent, retrying is safe and the LEAST/GREATEST upsert
        merges cleanly with anything the trigger has written meanwhile.
        """
        meta_tablename = cls.meta_table_name(name)
        if not _sa.inspect(conn).has_table(meta_tablename):
            return True
        meta_has_rows = (
            conn.execute(_sa.text(f"SELECT 1 FROM {meta_tablename} LIMIT 1")).first()
            is not None
        )
        if meta_has_rows:
            return False
        message_tablename = cls.message_table_name(name)
        return (
            conn.execute(_sa.text(f"SELECT 1 FROM {message_tablename} LIMIT 1")).first()
            is not None
        )


def _notify_trigger_function(
    name: str, meta_tablename: str, notification_channel: str
) -> str:
    """
    DDL for the trigger function only. Idempotent (CREATE OR REPLACE) so it
    can be run on both fresh creation and upgrades to refresh the body when
    the meta table is added to an existing install.
    """
    trigger_name = f"depeche_storage_new_msg_{name}"
    return f"""
        CREATE OR REPLACE FUNCTION {trigger_name}()
          RETURNS trigger AS $$
        DECLARE
        BEGIN
          INSERT INTO {meta_tablename}
            (stream, min_global_position, max_global_position)
          VALUES (NEW.stream, NEW.global_position, NEW.global_position)
          ON CONFLICT (stream) DO UPDATE
            SET max_global_position = GREATEST(
              {meta_tablename}.max_global_position,
              EXCLUDED.max_global_position
            );
          PERFORM pg_notify(
            '{notification_channel}',
            json_build_object(
                'message_id', NEW.message_id,
                'stream', NEW.stream,
                'version', NEW.version,
                'global_position', NEW.global_position
            )::text);
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
     """


def _notify_trigger_create(name: str, tablename: str) -> str:
    """DDL to attach the trigger to the message table. One-time."""
    trigger_name = f"depeche_storage_new_msg_{name}"
    return f"""
        CREATE TRIGGER {trigger_name}
          AFTER INSERT ON {tablename}
          FOR EACH ROW
          EXECUTE PROCEDURE {trigger_name}();
     """


def _notify_trigger(
    name: str, tablename: str, meta_tablename: str, notification_channel: str
) -> str:
    """
    Combined DDL for fresh creation: defines the function and creates the
    trigger. Used by the message-table after_create event and by the
    pre-0.8.0 migration helper.
    """
    return _notify_trigger_function(
        name=name,
        meta_tablename=meta_tablename,
        notification_channel=notification_channel,
    ) + _notify_trigger_create(name=name, tablename=tablename)


# Hash an arbitrary name down to the bigint advisory-lock keyspace. Mirrors
# the stream-hash trick used in _write_message_fn.
_ADVISORY_LOCK_KEY_SQL = "left('x' || md5(:lock_name), 17)::bit(64)::bigint"


def _setup_schema(
    engine: _sa.engine.Engine,
    metadata: _sa.MetaData,
    lock_name: str,
    *,
    needs_migration: Optional[Callable[[SAConnection], bool]] = None,
    migration_steps: Sequence[str] = (),
) -> None:
    """
    Create every table in ``metadata``, serialized across processes by a
    session-level advisory lock keyed on ``lock_name``.

    Without the lock, every process of a rolling deploy that restarts at once
    races on ``CREATE TABLE``: the losers block on the winner's uncommitted
    ``CREATE`` for the full duration of any backfill and then crash with
    "relation already exists". The advisory lock makes them queue instead --
    one process creates and migrates, the rest wait and then no-op.

    If ``needs_migration`` is given, it is called *before* the tables are
    created (so it can observe the pre-upgrade state). When it returns True,
    each statement in ``migration_steps`` is executed afterwards, each in its
    own committed transaction. The separate transactions are deliberate: a
    step that swaps a trigger function commits -- and so becomes visible to
    concurrent writers -- before a later step that backfills based on it.
    ``needs_migration`` should keep returning True until the migration has
    fully completed, so a backfill interrupted partway through is retried on
    the next construction rather than skipped.
    """
    with engine.connect() as conn:
        is_pg = conn.dialect.name == "postgresql"
        if is_pg:
            conn.execute(
                _sa.text(f"SELECT pg_advisory_lock({_ADVISORY_LOCK_KEY_SQL})"),
                {"lock_name": lock_name},
            )
            conn.commit()
        try:
            migrate = bool(
                is_pg and needs_migration is not None and needs_migration(conn)
            )
            metadata.create_all(conn, checkfirst=True)
            conn.commit()
            if migrate:
                for step in migration_steps:
                    conn.execute(_sa.DDL(step))
                    conn.commit()
        finally:
            if is_pg:
                conn.execute(
                    _sa.text(f"SELECT pg_advisory_unlock({_ADVISORY_LOCK_KEY_SQL})"),
                    {"lock_name": lock_name},
                )
                conn.commit()


def _write_message_fn(name: str, tablename: str) -> str:
    function_name = f"depeche_write_message_{name}"
    return f"""
        CREATE OR REPLACE FUNCTION {function_name}(
          message_id uuid,
          stream varchar,
          message json,
          expected_version bigint DEFAULT NULL,
          OUT version bigint,
          OUT global_position bigint
        )
        AS $$
        DECLARE
          _stream_hash bigint;
          _stream_version bigint;
          _next_version bigint;
          _next_global_position bigint;
        BEGIN
          _stream_hash := left('x' || md5({function_name}.stream), 17)::bit(64)::bigint;
          PERFORM pg_advisory_xact_lock(_stream_hash);

          SELECT
            max({tablename}.version) into _stream_version
          FROM
            {tablename}
          WHERE
            {tablename}.stream = {function_name}.stream;

          IF _stream_version IS NULL THEN
            _stream_version := 0;
          END IF;

          IF {function_name}.expected_version IS NOT NULL THEN
            IF {function_name}.expected_version != _stream_version THEN
              RAISE EXCEPTION
                'Wrong expected version: %% (Stream: %%, Stream Version: %%)',
                {function_name}.expected_version,
                {function_name}.stream,
                _stream_version;
            END IF;
          END IF;

          _next_version := _stream_version + 1;
          _next_global_position := nextval('depeche_msgs_{name}_global_seq');

          INSERT INTO {tablename}
            (
              message_id,
              stream,
              version,
              message,
              global_position
            )
          VALUES
            (
              {function_name}.message_id,
              {function_name}.stream,
              _next_version,
              {function_name}.message,
              _next_global_position
            )
          ;

          version := _next_version;
          global_position := _next_global_position;
        END;
        $$ LANGUAGE plpgsql
        VOLATILE;
    """
