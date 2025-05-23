from depeche_db import MessagePartitioner, StoredMessage
from depeche_db._message_store import MessageStore
from tests._account_example import (
    AccountEvent,
)


class MyPartitioner(MessagePartitioner[AccountEvent]):
    def get_partition(self, event: StoredMessage[AccountEvent]) -> int:
        return int(str(event.message.account_id)[-1])


def test_factory(store_factory, identifier):
    store: MessageStore = store_factory()
    stream = store.aggregated_stream(
        name=identifier(),
        partitioner=MyPartitioner(),
        stream_wildcards=["%"],
        update_batch_size=1,
    )
    assert stream.projector.batch_size == 1
    assert stream.projector.stream_wildcards == ["%"]
