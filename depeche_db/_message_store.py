from psycopg2.errors import LockNotAvailable
import contextlib as _contextlib
import dataclasses as _dc
import datetime as _dt
import uuid as _uuid
from typing import Generic, Iterable, Iterator, Protocol, TypeVar

import sqlalchemy as _sa
import sqlalchemy.orm as _orm
from sqlalchemy_utils import UUIDType as _UUIDType

from ._storage import Storage
from ._interfaces import (
    MessageProtocol,
    MessageSerializer,
    StoredMessage,
    MessagePosition,
)
from ._exceptions import MessageNotFound

E = TypeVar("E", bound=MessageProtocol)


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
