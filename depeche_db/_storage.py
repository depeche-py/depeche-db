"""
The storage implementation in PL/SQL is heavily inspired by
https://github.com/message-db/message-db
"""
import uuid as _uuid
from typing import Iterator, Optional, Sequence, Tuple

import sqlalchemy as _sa
from psycopg2.extras import Json as _PsycoPgJson
from sqlalchemy_utils import UUIDType as _UUIDType

from ._compat import SAConnection
from ._interfaces import MessagePosition


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
            # TODO is this still required? only add in tests?
            _sa.UniqueConstraint(
                "stream", "version", name=f"{name}_stream_version_unique"
            ),
        )
        # TODO second table for stream metadata (e.g. last version, last global position)
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

            CREATE OR REPLACE FUNCTION {name}_stream_version(
              stream varchar
            )
            RETURNS bigint
            AS $$
            DECLARE
              _stream_version bigint;
            BEGIN
              SELECT
                max(version) into _stream_version
              FROM
                {name}_messages
              WHERE
                {name}_messages.stream = {name}_stream_version.stream;

              RETURN _stream_version;
            END;
            $$ LANGUAGE plpgsql
            VOLATILE;

            CREATE OR REPLACE FUNCTION {name}_hash_64(
              value varchar
            )
            RETURNS bigint
            AS $$
            DECLARE
              _hash bigint;
            BEGIN
              SELECT left('x' || md5({name}_hash_64.value), 17)::bit(64)::bigint INTO _hash;
              return _hash;
            END;
            $$ LANGUAGE plpgsql
            IMMUTABLE;

            CREATE OR REPLACE FUNCTION {name}_acquire_lock(
              stream varchar
            )
            RETURNS bigint
            AS $$
            DECLARE
              _stream_hash bigint;
            BEGIN
              _stream_hash := {name}_hash_64({name}_acquire_lock.stream);
              PERFORM pg_advisory_xact_lock(_stream_hash);

              RETURN _stream_hash;
            END;
            $$ LANGUAGE plpgsql
            VOLATILE;

            CREATE OR REPLACE FUNCTION {name}_write_message(
              message_id uuid,
              stream varchar,
              message json,
              expected_version bigint DEFAULT NULL,
              OUT version bigint,
              OUT global_position bigint
            )
            AS $$
            DECLARE
              _stream_version bigint;
              _next_version bigint;
              _next_global_position bigint;
            BEGIN
              PERFORM {name}_acquire_lock({name}_write_message.stream);

              _stream_version := {name}_stream_version({name}_write_message.stream);

              IF _stream_version IS NULL THEN
                _stream_version := 0;
              END IF;

              IF {name}_write_message.expected_version IS NOT NULL THEN
                IF {name}_write_message.expected_version != _stream_version THEN
                  RAISE EXCEPTION
                    'Wrong expected version: %% (Stream: %%, Stream Version: %%)',
                    {name}_write_message.expected_version,
                    {name}_write_message.stream,
                    _stream_version;
                END IF;
              END IF;

              _next_version := _stream_version + 1;
              _next_global_position := nextval('{name}_messages_global_position_seq');

              INSERT INTO {name}_messages
                (
                  message_id,
                  stream,
                  version,
                  message,
                  global_position
                )
              VALUES
                (
                  {name}_write_message.message_id,
                  {name}_write_message.stream,
                  _next_version,
                  {name}_write_message.message,
                  _next_global_position
                )
              ;

              -- TODO update stream metadata?!
              version := _next_version;
              global_position := _next_global_position;
            END;
            $$ LANGUAGE plpgsql
            VOLATILE;
            """
        )
        _sa.event.listen(
            self.message_table, "after_create", trigger.execute_if(dialect="postgresql")
        )
        self.metadata.create_all(engine, checkfirst=True)

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
        func = getattr(_sa.func, f"{self.name}_write_message")
        try:
            for idx, (message_id, message) in enumerate(messages):
                _expected_version = (
                    expected_version + idx if expected_version is not None else None,
                )
                result = conn.execute(  # type: ignore
                    _sa.select(
                        _sa.column("version"), _sa.column("global_position")
                    ).select_from(
                        func(
                            message_id, stream, _PsycoPgJson(message), _expected_version
                        ).alias()
                    )
                )
        except _sa.exc.InternalError:
            raise ValueError("optimistic concurrency failure")
        row = result.fetchone()
        return MessagePosition(
            stream=stream,
            version=row.version,
            global_position=row.global_position,
        )

    def get_max_version(self, conn: SAConnection, stream: str) -> MessagePosition:
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
        if not row or row.version is None:
            return MessagePosition(stream, 0, None)
        return MessagePosition(stream, row.version, row.global_position)

    def get_message_ids(self, conn: SAConnection, stream: str) -> Iterator[_uuid.UUID]:
        for id in conn.execute(
            _sa.select(self.message_table.c.message_id)
            .select_from(self.message_table)
            .where(self.message_table.c.stream == stream)
            .order_by(self.message_table.c.version)
        ).scalars():
            yield id

    def read(
        self, conn: SAConnection, stream: str
    ) -> Iterator[Tuple[_uuid.UUID, int, dict, int]]:
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
        self, conn: SAConnection, streams: Sequence[str]
    ) -> Iterator[Tuple[_uuid.UUID, str, int, dict, int]]:
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
        self, conn: SAConnection, stream_wildcard: str
    ) -> Iterator[Tuple[_uuid.UUID, str, int, dict, int]]:
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
        self, conn: SAConnection, message_id: _uuid.UUID
    ) -> Tuple[_uuid.UUID, str, int, dict, int]:
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
        self, conn: SAConnection, message_ids: Sequence[_uuid.UUID]
    ) -> Iterator[Tuple[_uuid.UUID, str, int, dict, int]]:
        return conn.execute(  # type: ignore
            _sa.select(
                self.message_table.c.message_id,
                self.message_table.c.stream,
                self.message_table.c.version,
                self.message_table.c.message,
                self.message_table.c.global_position,
            ).where(self.message_table.c.message_id.in_(message_ids))
        )

    def truncate(self, conn: SAConnection):
        conn.execute(self.message_table.delete())
