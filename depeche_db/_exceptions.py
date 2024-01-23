class MessageNotFound(Exception):
    pass


class OptimisticConcurrencyError(Exception):
    pass


class StreamNotFoundError(Exception):
    pass


class LastMessageCannotBeDeleted(Exception):
    pass


class CannotWriteToDeletedStream(Exception):
    pass
