import datetime as _dt
import uuid as _uuid

import pytest
import redis

from depeche_db import (
    StoredMessage,
    SubscriptionMessage,
)
from depeche_db.experimental.redis_dead_letter_queue import RedisDeadLetterQueue
from tests._account_example import (
    AccountCreditedEvent,
)

MSG_ID = _uuid.uuid4()
SUB_MSG = SubscriptionMessage(
    partition=17,
    position=19,
    stored_message=StoredMessage(
        message_id=MSG_ID,
        stream="stream",
        version=12,
        message=AccountCreditedEvent(
            event_id=MSG_ID,
            account_id=_uuid.uuid4(),
            amount=1,
            balance=1,
        ),
        global_position=99,
        added_at=_dt.datetime(2025, 1, 1, 13),
    ),
    ack=None,
)


@pytest.fixture
def connected_redis():
    return redis.Redis(host="localhost", port=4999, db=0)


def test_foo(connected_redis):
    key = str(_uuid.uuid4()).replace("-", "")
    subject = RedisDeadLetterQueue(subscription_key=key, redis=connected_redis)
    subject.append(error=ValueError("foo"), message=SUB_MSG)

    assert subject.length() == 1
    letters = list(subject.read())
    assert len(letters) == 1
    letter = letters[0]
    print(letter)
    assert letter.subscription_key == key
    assert letter.partition == SUB_MSG.partition
    assert letter.position == SUB_MSG.position
    assert letter.stream == "stream"
    assert letter.stream_version == SUB_MSG.stored_message.version
    assert letter.message_type == "AccountCreditedEvent"
    assert letter.message_id == SUB_MSG.stored_message.message_id
    assert letter.error == "ValueError('foo')"

    letter_again = subject.pop()
    assert letter == letter_again

    assert subject.length() == 0
    assert subject.pop() is None
