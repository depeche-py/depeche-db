import dataclasses
import json
import uuid as _uuid
from typing import Generic, Iterable, TypeVar

import redis as _redis

from depeche_db._interfaces import MessageProtocol, SubscriptionMessage
from depeche_db.experimental.dead_letter_box import DeadLetterQueue

E = TypeVar("E", bound=MessageProtocol)


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
        self, subscription_key: str, redis: _redis.Redis, key_prefix: str | None = None
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

    def read(self, limit: int = 10) -> Iterable[DeadLetter]:
        assert limit > 0
        for item in self._redis.lrange(self._write_key, 0, limit - 1):
            yield self._parse(item)

    def pop(self) -> DeadLetter | None:
        item = self._redis.lpop(self._write_key)
        if item is None:
            return None
        return self._parse(item)

    def _parse(self, item: str):
        parsed = json.loads(item)
        message_id = parsed.pop("message_id")
        return DeadLetter(
            subscription_key=self.subscription_key,
            message_id=_uuid.UUID(message_id),
            **parsed,
        )
