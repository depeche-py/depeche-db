import datetime as _dt
import uuid as _uuid
from typing import Any

from depeche_db import (
    MessageHandlerRegister,
    StoredMessage,
    SubscriptionMessage,
    SubscriptionMessageHandler,
)
from depeche_db.experimental.dead_letter_box import (
    DeadLetterBoxSubscriptionErrorHandler,
    DeadLetterQueue,
)
from tests._account_example import (
    AccountCreditedEvent,
)

MSG_ID = _uuid.uuid4()
SUB_MSG = SubscriptionMessage(
    partition=0,
    position=0,
    stored_message=StoredMessage(
        message_id=MSG_ID,
        stream="stream",
        version=0,
        message=AccountCreditedEvent(
            event_id=MSG_ID,
            account_id=_uuid.uuid4(),
            amount=1,
            balance=1,
        ),
        global_position=0,
        added_at=_dt.datetime(2025, 1, 1, 13),
    ),
    ack=None,
)


class FakeQueue(DeadLetterQueue):
    def __init__(self):
        self.items = []

    def append(self, error, message):
        self.items.append((error, message))


def test_store_message_in_dead_letter_queue():
    queue = FakeQueue()
    register: MessageHandlerRegister[Any] = MessageHandlerRegister()

    @register.register
    def handle_account_credited(event: SubscriptionMessage[AccountCreditedEvent]):
        raise ValueError("foo")

    subject = SubscriptionMessageHandler(
        register, error_handler=DeadLetterBoxSubscriptionErrorHandler(queue=queue)
    )
    subject.handle(SUB_MSG)
    # No exception raised

    assert len(queue.items) == 1
    error, msg = queue.items[0]
    assert isinstance(error, ValueError)
    assert msg == SUB_MSG
