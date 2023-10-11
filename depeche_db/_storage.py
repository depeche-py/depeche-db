import uuid as _uuid
from typing import Iterator, Sequence, Tuple

import sqlalchemy as _sa
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
            _sa.UniqueConstraint(
                "stream", "version", name=f"{name}_stream_version_unique"
            ),
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
        conn: SAConnection,
        stream: str,
        expected_version: int,
        message_id: _uuid.UUID,
        message: dict,
    ) -> MessagePosition:
        return self.add_all(conn, stream, expected_version, [(message_id, message)])

    def add_all(
        self,
        conn: SAConnection,
        stream: str,
        expected_version: int,
        messages: Sequence[Tuple[_uuid.UUID, dict]],
    ) -> MessagePosition:
        max_version = self.get_max_version(conn, stream).version
        if expected_version > -1:
            if max_version != expected_version:
                raise ValueError("optimistic concurrency failure")
        conn.execute(
            self.message_table.insert(),
            [
                {
                    "message_id": message_id,
                    "stream": stream,
                    "version": max_version + i + 1,
                    "message": message,
                }
                for i, (message_id, message) in enumerate(messages)
            ],
        )
        return self.get_max_version(conn, stream)

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
