class MessageNotFound(Exception):
    pass


class OptimisticConcurrencyError(Exception):
    pass


class MessageIdMismatchError(ValueError):
    pass
