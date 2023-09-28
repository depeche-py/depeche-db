from typing import TypeVar

from .._interfaces import MessagePartitioner, MessageProtocol, StoredMessage

E = TypeVar("E", bound=MessageProtocol)


class StreamNamePartitioner(MessagePartitioner[E]):
    def get_partition(self, message: StoredMessage[E]) -> int:
        import hashlib

        return int.from_bytes(
            hashlib.sha256(message.stream.encode()).digest()[:2], "little"
        )
