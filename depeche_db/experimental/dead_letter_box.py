from typing import Generic, Protocol, TypeVar

from depeche_db._interfaces import ErrorAction, MessageProtocol, SubscriptionMessage

from .._subscription import SubscriptionErrorHandler

E = TypeVar("E", bound=MessageProtocol)


class DeadLetterQueue(Protocol, Generic[E]):
    def append(self, error: Exception, message: SubscriptionMessage[E]) -> None:
        raise NotImplementedError()


class DeadLetterBoxSubscriptionErrorHandler(SubscriptionErrorHandler[E], Generic[E]):
    def __init__(self, queue: DeadLetterQueue) -> None:
        self._queue = queue

    def handle_error(
        self, error: Exception, message: SubscriptionMessage[E]
    ) -> ErrorAction:
        print(error, message)
        self._queue.append(message=message, error=error)
        return ErrorAction.IGNORE
