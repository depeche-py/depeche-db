import contextlib as _contextlib
import uuid as _uuid
from typing import Generic, Iterator, Optional, Sequence, TypeVar

import sqlalchemy as _sa

from ._compat import SAConnection
from ._exceptions import MessageIdMismatchError, MessageNotFound
from ._factories import AggregatedStreamFactory
from ._interfaces import (
    MessagePosition,
    MessageProtocol,
    MessageSerializer,
    StoredMessage,
)
from ._storage import Storage

E = TypeVar("E", bound=MessageProtocol)


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
            message_id, stream, version, message, global_position = row
            yield StoredMessage(
                message_id=message_id,
                stream=stream,
                version=version,
                message=self._serializer.deserialize(message),
                global_position=global_position,
            )

    def read(self, stream: str) -> Iterator[StoredMessage[E]]:
        """
        Returns all messages from a stream.

        Args:
            stream (str): Stream name
        """
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
        self._serializer = serializer
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
        stored_version = self._storage.get_max_version(conn, stream)
        if stored_version is not None:
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
            result = stored_version
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
    ) -> Iterator[MessageStoreReader[E]]:
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

    def read(self, stream: str) -> Iterator[StoredMessage[E]]:
        """
        Read all messages from a stream.

        Args:
            stream (str): The name of the stream.

        Returns:
            Iterator[StoredMessage]: An iterator over the messages.
        """
        with self.reader() as reader:
            yield from reader.read(stream)
