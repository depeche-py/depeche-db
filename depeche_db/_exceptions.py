class MessageNotFound(Exception):
    pass


class OptimisticConcurrencyError(Exception):
    pass


class MessageIdMismatchError(ValueError):
    pass


class StreamClosedError(Exception):
    pass


class StreamNotClosedError(Exception):
    pass
