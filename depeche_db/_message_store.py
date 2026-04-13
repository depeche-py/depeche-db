import contextlib as _contextlib
import uuid as _uuid
from typing import (
    TYPE_CHECKING,
    Generic,
    Iterator,
    List,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
)

import sqlalchemy as _sa

from ._compat import SAConnection
from ._exceptions import (
    MessageIdMismatchError,
    MessageNotFound,
    StreamNotClosedError,
)

if TYPE_CHECKING:
    from ._aggregated_stream import AggregatedStream
from ._factories import AggregatedStreamFactory
from ._interfaces import (
    MessagePosition,
    MessageProtocol,
    MessageSerializer,
    StoredMessage,
)
from ._storage import Storage

E = TypeVar("E", bound=MessageProtocol)


class MessageStoreReaderProtocol(Protocol, Generic[E]):
    def get_message_by_id(self, message_id: _uuid.UUID) -> StoredMessage[E]:
        ...

    def get_messages_by_ids(
        self, message_ids: Sequence[_uuid.UUID]
    ) -> Iterator[StoredMessage[E]]:
        ...

    def read(
        self, stream: str, min_version: Optional[int] = None
    ) -> Iterator[StoredMessage[E]]:
        ...

    def read_wildcard(self, stream_wildcard: str) -> Iterator[StoredMessage[E]]:
        ...


class MessageStoreProtocol(Protocol, Generic[E]):
    def truncate(self):
        ...

    def write(
        self,
        stream: str,
        message: E,
        expected_version: Optional[int] = None,
        conn: Optional[SAConnection] = None,
    ) -> MessagePosition:
        ...

    def synchronize(
        self,
        stream: str,
        expected_version: int,
        messages: Sequence[E],
        conn: Optional[SAConnection] = None,
    ) -> MessagePosition:
        ...

    @_contextlib.contextmanager
    def reader(
        self, conn: Optional[SAConnection] = None
    ) -> Iterator[MessageStoreReaderProtocol[E]]:
        ...

    def read(
        self, stream: str, min_version: Optional[int] = None
    ) -> Iterator[StoredMessage[E]]:
        ...


class MessageStoreReader(Generic[E]):
    """
    Message store reader.
    """

    def __init__(
        self, conn: SAConnection, storage: Storage, serializer: MessageSerializer[E]
    ):
        self._conn = conn
        self._storage = storage
        self._serializer = serializer

    def get_message_by_id(self, message_id: _uuid.UUID) -> StoredMessage[E]:
        """
        Returns a message by ID.

        Args:
            message_id (UUID): Message ID.
        """
        row = self._storage.get_message_by_id(conn=self._conn, message_id=message_id)
        if row:
            message_id, stream, version, message, global_position, addet_at = row
            return StoredMessage(
                message_id=message_id,
                stream=stream,
                version=version,
                message=self._serializer.deserialize(message),
                global_position=global_position,
                added_at=addet_at,
            )
        raise MessageNotFound(message_id)

    def get_messages_by_ids(
        self, message_ids: Sequence[_uuid.UUID]
    ) -> Iterator[StoredMessage[E]]:
        """
        Returns multiple messages by IDs.

        Args:
            message_ids (Sequence[UUID]): Message IDs.
        """
        for row in self._storage.get_messages_by_ids(
            conn=self._conn, message_ids=message_ids
        ):
            message_id, stream, version, message, global_position, added_at = row
            yield StoredMessage(
                message_id=message_id,
                stream=stream,
                version=version,
                message=self._serializer.deserialize(message),
                global_position=global_position,
                added_at=added_at,
            )

    def read(
        self, stream: str, min_version: Optional[int] = None
    ) -> Iterator[StoredMessage[E]]:
        """
        Returns all messages from a stream.

        Args:
            stream (str): Stream name
            min_version (Optional[int]): If provided, only return messages with
                version >= min_version
        """
        for (
            message_id,
            version,
            message,
            global_position,
            added_at,
        ) in self._storage.read(self._conn, stream, min_version):
            yield StoredMessage(
                message_id=message_id,
                stream=stream,
                version=version,
                message=self._serializer.deserialize(message),
                global_position=global_position,
                added_at=added_at,
            )

    def read_wildcard(self, stream_wildcard: str) -> Iterator[StoredMessage[E]]:
        """
        Returns all messages from streams that match the wildcard.

        Use like syntax to match multiple streams:

        - `stream-%` - match all streams that start with `stream-`
        - `%` - match all streams
        - `%-%` - match all streams that contain `-`

        Args:
            stream_wildcard (str): Stream name wildcard
        """
        for (
            message_id,
            stream,
            version,
            message,
            global_position,
            added_at,
        ) in self._storage.read_wildcard(self._conn, stream_wildcard):
            yield StoredMessage(
                message_id=message_id,
                stream=stream,
                version=version,
                message=self._serializer.deserialize(message),
                global_position=global_position,
                added_at=added_at,
            )


class _ClosedStreamAwareSerializer(Generic[E]):
    def __init__(self, inner: MessageSerializer[E]):
        self._inner = inner

    def serialize(self, message: E) -> dict:
        return self._inner.serialize(message)

    def deserialize(self, message: dict) -> E:
        clean = {k: v for k, v in message.items() if k != "__depeche_closed"}
        return self._inner.deserialize(clean)


class MessageStore(Generic[E]):
    _aggregated_streams: List["AggregatedStream[E]"]

    def __init__(
        self,
        name: str,
        engine: _sa.engine.Engine,
        serializer: MessageSerializer[E],
    ):
        """
        Message store.

        Args:
            name (str): A valid python identifier which is used as a prefix for
                the database objects that are created.
            engine (Engine): A SQLAlchemy engine.
            serializer (MessageSerializer): A serializer for the messages.

        Attributes:
            aggregated_stream (AggregatedStreamFactory): A factory for aggregated streams.
        """
        self.engine = engine
        self._storage = Storage(name=name, engine=engine)
        self._serializer = _ClosedStreamAwareSerializer(serializer)
        self._aggregated_streams = []
        self.aggregated_stream = AggregatedStreamFactory(store=self)

    def _get_connection(self) -> SAConnection:
        return self.engine.connect()

    def truncate(self):
        with self._get_connection() as conn:
            self._storage.truncate(conn)
            conn.commit()

    def write(
        self,
        stream: str,
        message: E,
        expected_version: Optional[int] = None,
        conn: Optional[SAConnection] = None,
    ) -> MessagePosition:
        """
        Write a message to the store.

        Optimistic concurrency control can used to ensure that the stream is
        not modified by another process between reading the last message and
        writing the new message. You have to give `expected_version` to use it.
        If the stream has been modified, a `OptimisticConcurrencyError` will be
        raised.

        You can give a connection to use for the write as `conn`. If you don't
        give a connection, a new connection will be created and the write will
        be committed. You can use this to write messages and other data in a
        single transaction. Therefore, if you give a connection, **you have to
        commit the transaction yourself**.

        Args:
            stream (str): The name of the stream to which the message should be written.
            message (MessageProtocol): The message to write.
            expected_version (Optional[int], optional): The expected version of the stream.
            conn (Optional[SAConnection], optional): A database connection.

        Returns:
            MessagePosition: The position of the last message in the stream.
        """
        if conn is None:
            with self._get_connection() as conn:
                result = self._write(
                    conn=conn,
                    stream=stream,
                    message=message,
                    expected_version=expected_version,
                )
                conn.commit()
                return result
        else:
            return self._write(
                conn=conn,
                stream=stream,
                message=message,
                expected_version=expected_version,
            )

    def _write(
        self,
        conn: SAConnection,
        stream: str,
        message: E,
        expected_version: Optional[int] = None,
    ) -> MessagePosition:
        return self._storage.add(
            conn=conn,
            stream=stream,
            expected_version=expected_version,
            message_id=message.get_message_id(),
            message=self._serializer.serialize(message),
        )

    def synchronize(
        self,
        stream: str,
        expected_version: int,
        messages: Sequence[E],
        conn: Optional[SAConnection] = None,
    ) -> MessagePosition:
        """
        Synchronize a stream with a sequence of messages.

        Given a stream and a sequence of messages, this method will write the
        messages to the stream that are not already in it.

        Optimistic concurrency control must used to ensure that the stream is
        not modified by another process between reading the last message and
        writing the new message. You have to give `expected_version`. If the
        stream has been modified, a `OptimisticConcurrencyError` will be raised.

        You can give a connection to use for the write as `conn`. If you don't
        give a connection, a new connection will be created and the write will
        be committed. You can use this to write messages and other data in a
        single transaction. Therefore, if you give a connection, **you have to
        commit the transaction yourself**.

        Args:
            stream (str): The name of the stream to which the message should be written.
            expected_version (int): The expected version of the stream.
            messages (Sequence[MessageProtocol]): The messages that should be in the stream after the synchronization.
            conn (Optional[SAConnection], optional): A database connection.

        Returns:
            MessagePosition: The position of the last message in the stream.
        """
        if conn is None:
            with self._get_connection() as conn:
                result = self._synchronize(
                    conn=conn,
                    stream=stream,
                    expected_version=expected_version,
                    messages=messages,
                )
                conn.commit()
                return result
        else:
            return self._synchronize(
                conn=conn,
                stream=stream,
                expected_version=expected_version,
                messages=messages,
            )

    def _synchronize(
        self,
        conn: SAConnection,
        stream: str,
        expected_version: int,
        messages: Sequence[E],
    ) -> MessagePosition:
        stored_ids = list(self._storage.get_message_ids(conn, stream))
        message_ids = [message.get_message_id() for message in messages]
        for stored, given in zip(stored_ids, message_ids):
            if stored != given:
                raise MessageIdMismatchError(
                    "Message ID mismatch, has the stream been modified in the meantime?"
                )
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
        else:
            result = self._storage.get_max_version(conn, stream)
        return result

    def _get_reader(self, conn: SAConnection) -> MessageStoreReader[E]:
        return MessageStoreReader(
            conn=conn,
            storage=self._storage,
            serializer=self._serializer,
        )

    @_contextlib.contextmanager
    def reader(
        self, conn: Optional[SAConnection] = None
    ) -> Iterator[MessageStoreReaderProtocol[E]]:
        """
        Get a reader for the store.

        You can give a connection to use for the read as `conn`. If you don't
        give a connection, a new connection will be used (and discarded after
        the reader context has been left).

        Example usage:

            with store.reader() as reader:
                message = reader.get_message_id(some_id)

        Args:
            conn (Optional[SAConnection], optional): A database connection.

        Yields:
            MessageStoreReader: A reader for the store.
        """
        if conn:
            yield self._get_reader(conn)
        else:
            with self._get_connection() as conn:
                yield self._get_reader(conn)

    def read(
        self, stream: str, min_version: Optional[int] = None
    ) -> Iterator[StoredMessage[E]]:
        """
        Read all messages from a stream.

        Args:
            stream (str): The name of the stream.
            min_version (Optional[int]): If provided, only return messages with
                version >= min_version

        Returns:
            Iterator[StoredMessage]: An iterator over the messages.
        """
        with self.reader() as reader:
            yield from reader.read(stream, min_version)

    def close_stream(
        self,
        stream: str,
        message: E,
        expected_version: int,
        conn: Optional[SAConnection] = None,
    ) -> MessagePosition:
        """
        Close a stream by writing a tombstone message.

        After closing, no further messages can be written to the stream.
        The close message flows through the normal pipeline (projector,
        aggregated streams, subscriptions) so consumers can react to it.

        Args:
            stream: The name of the stream to close.
            message: The close message (user-provided, implements MessageProtocol).
            expected_version: The expected current version of the stream (required).
            conn: Optional database connection for transactional use.

        Returns:
            MessagePosition: The position of the close message.

        Raises:
            OptimisticConcurrencyError: If the stream version doesn't match.
            StreamClosedError: If the stream is already closed.
        """
        serialized = self._serializer.serialize(message)
        serialized["__depeche_closed"] = True
        if conn is None:
            with self._get_connection() as conn:
                result = self._storage.add(
                    conn=conn,
                    stream=stream,
                    expected_version=expected_version,
                    message_id=message.get_message_id(),
                    message=serialized,
                )
                conn.commit()
                return result
        else:
            return self._storage.add(
                conn=conn,
                stream=stream,
                expected_version=expected_version,
                message_id=message.get_message_id(),
                message=serialized,
            )

    def archive_stream(
        self,
        stream: str,
        archive_table: "_sa.Table",
        conn: Optional[SAConnection] = None,
    ):
        """
        Archive a closed stream.

        Copies all messages (including the close event) to the given archive
        table, then deletes all non-close messages from the main table and
        removes aggregated stream pointers (except for the close event).

        The close message (tombstone) remains in the main table permanently,
        preventing re-use of the stream name.

        Use `Storage.create_archive_table()` to create archive tables.

        Args:
            stream: The name of the stream to archive.
            archive_table: The SQLAlchemy Table to copy messages into.
            conn: Optional database connection. If not provided, a new
                connection is created and the operation is committed. If
                provided, **you must commit the transaction yourself**.

        Raises:
            StreamNotClosedError: If the stream is not closed.
        """
        if conn is None:
            with self._get_connection() as conn:
                self._archive_stream(conn, stream, archive_table)
                conn.commit()
        else:
            self._archive_stream(conn, stream, archive_table)

    def _archive_stream(
        self, conn: SAConnection, stream: str, archive_table: "_sa.Table"
    ):
        close_message_id = self._storage.get_close_message_id(conn, stream)
        if close_message_id is None:
            raise StreamNotClosedError(
                f"Stream '{stream}' is not closed. Call close_stream() first."
            )

        self._storage.copy_stream_to_archive(conn, stream, archive_table)
        self._storage.delete_stream_messages(conn, stream, keep_closed=True)

        for agg_stream in self._aggregated_streams:
            agg_stream.remove_origin_stream(
                origin_stream=stream,
                conn=conn,
                exclude_message_ids=[close_message_id],
            )
