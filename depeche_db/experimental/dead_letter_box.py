import logging as _logging
from typing import Generic, Protocol, TypeVar

from depeche_db._interfaces import ErrorAction, MessageProtocol, SubscriptionMessage

from .._subscription import SubscriptionErrorHandler

E = TypeVar("E", bound=MessageProtocol)

_logger = _logging.getLogger("depeche_db.dead_letter_box")


class DeadLetterQueue(Protocol, Generic[E]):
    def append(self, error: Exception, message: SubscriptionMessage[E]) -> None:
        raise NotImplementedError()


class DeadLetterBoxSubscriptionErrorHandler(SubscriptionErrorHandler[E], Generic[E]):
    def __init__(self, queue: DeadLetterQueue[E]) -> None:
        self._queue = queue

    def handle_error(
        self, error: Exception, message: SubscriptionMessage[E]
    ) -> ErrorAction:
        _logger.error(
            "Error while handling message %s:%s, moving it to the dead letter queue",
            message.stored_message.message_id,
            message.stored_message.message.__class__.__name__,
            exc_info=error,
        )
        self._queue.append(message=message, error=error)
        return ErrorAction.IGNORE
