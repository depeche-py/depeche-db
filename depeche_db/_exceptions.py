class MessageNotFound(Exception):
    pass


class OptimisticConcurrencyError(Exception):
    pass


class MessageIdMismatchError(ValueError):
    pass


class PartitionRevoked(Exception):
    """
    Raised when an instance tries to ack/store a position for a partition it no
    longer owns. Callers should drop the partition from their in-memory working
    set and refresh their assignment view.
    """

    def __init__(
        self,
        subscription_name: str,
        partition: int,
        expected_generation: int,
    ):
        super().__init__(
            f"Partition {partition} of subscription '{subscription_name}' was "
            f"revoked (expected generation {expected_generation})."
        )
        self.subscription_name = subscription_name
        self.partition = partition
        self.expected_generation = expected_generation
