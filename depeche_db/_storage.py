"""
The storage implementation in PL/SQL is heavily inspired by
https://github.com/message-db/message-db
"""

import datetime as _dt
import uuid as _uuid
from typing import Any, Iterator, Optional, Sequence, Tuple

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
        # When the meta table is being created in this DDL pass, also (re-)
        # install the trigger function with its meta-aware body and backfill
        # from existing rows. The event fires only when the meta table is
        # actually created, so on a fresh install this runs once next to the
        # main DDL above; on an upgrade from a pre-meta version it runs once
        # to install the new function body and seed the meta table.
        #
        # Note: only the *function* is refreshed, not the trigger itself.
        # Triggers reference functions by name so an existing trigger picks
        # up the new body automatically once we CREATE OR REPLACE FUNCTION.
        meta_create_ddl = _sa.DDL(
            _notify_trigger_function(
                name=name,
                meta_tablename=self.meta_table.name,
                notification_channel=self.notification_channel,
            )
            + f"""
            INSERT INTO {self.meta_table.name}
                (stream, min_global_position, max_global_position)
            SELECT stream, MIN(global_position), MAX(global_position)
            FROM {self.message_table.name}
            GROUP BY stream
            ON CONFLICT (stream) DO NOTHING;
            """
        )
        _sa.event.listen(
            self.meta_table,
            "after_create",
            meta_create_ddl.execute_if(dialect="postgresql"),
        )
        self.metadata.create_all(engine, checkfirst=True)
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
