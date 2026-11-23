import dataclasses
import json
import uuid as _uuid
from typing import Awaitable, Generic, List, Optional, TypeVar, Union

import redis as _redis

from depeche_db._interfaces import MessageProtocol, SubscriptionMessage
from depeche_db.experimental.dead_letter_box import DeadLetterQueue

E = TypeVar("E", bound=MessageProtocol)

# Version of the JSON payload stored in Redis. Readers ignore unknown keys,
# so adding fields is backwards compatible; bump this only for breaking changes.
_SCHEMA_VERSION = 1


@dataclasses.dataclass
class DeadLetter:
    subscription_key: str
    partition: int
    position: int
    stream: str
    stream_version: int
    message_type: str
    message_id: _uuid.UUID
    error: str


class RedisDeadLetterQueue(DeadLetterQueue[E], Generic[E]):
    def __init__(
        self,
        subscription_key: str,
        redis: _redis.Redis,
        key_prefix: Optional[str] = None,
    ):
        self.subscription_key = subscription_key
        self.key_prefix = key_prefix
        if key_prefix:
            self._write_key = f"{key_prefix}-depeche-dlq-{subscription_key}"
        else:
            self._write_key = f"depeche-dlq-{subscription_key}"
        self._redis = redis

    def append(self, error: Exception, message: SubscriptionMessage[E]) -> None:
        self._redis.rpush(
            self._write_key,
            json.dumps(
                {
                    "v": _SCHEMA_VERSION,
                    "partition": message.partition,
                    "position": message.position,
                    "stream": message.stored_message.stream,
                    "stream_version": message.stored_message.version,
                    "message_type": message.stored_message.message.__class__.__name__,
                    "message_id": str(message.stored_message.message_id),
                    "error": repr(error),
                }
            ),
        )

    def length(self) -> int:
        return self._redis.llen(self._write_key)

    def read(self, limit: int = 10) -> List[DeadLetter]:
        if limit <= 0:
            raise ValueError(f"limit must be positive, got {limit}")
        result = self._redis.lrange(self._write_key, 0, limit - 1)
        assert not isinstance(result, Awaitable)
        return [self._parse(item) for item in result]

    def pop(self) -> Optional[DeadLetter]:
        item = self._redis.lpop(self._write_key)
        assert not isinstance(item, Awaitable)
        if item is None:
            return None
        # list only gets here if we query more than one
        assert not isinstance(item, list)
        return self._parse(item)

    def _parse(self, item: Union[str, bytes]) -> DeadLetter:
        # Read known keys explicitly so that entries written by a newer
        # version (with additional keys) can still be read.
        parsed = json.loads(item)
        return DeadLetter(
            subscription_key=self.subscription_key,
            partition=parsed["partition"],
            position=parsed["position"],
            stream=parsed["stream"],
            stream_version=parsed["stream_version"],
            message_type=parsed["message_type"],
            message_id=_uuid.UUID(parsed["message_id"]),
            error=parsed["error"],
        )
