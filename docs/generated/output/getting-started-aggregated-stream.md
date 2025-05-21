
# Aggregated stream

Aggregated streams are our main way to read and consume messages from multiple streams.
An aggregated stream contains all the messages from the
matched streams, partitioned according to a partition scheme.
See [data model](../../concepts/data-model.md) for more details.

We will use the same message store as in the previous chapter here, but we will
create a new set of streams within it:

```python
import random

for _ in range(20):
    n = random.randint(0, 200)
    stream = f"aggregate-me-{n % 5}"
    message_store.write(stream=stream, message=EventA(num=n))
```

For our aggregated stream, we need to prepare a partition function (or rather class).

```python
from depeche_db import StoredMessage


class NumMessagePartitioner:
    def get_partition(self, message: StoredMessage[EventA | EventB]) -> int:
        if isinstance(message.message, EventA):
            return message.message.num % 3
        return 0
```

Now we can put together the aggregated stream.

```python
aggregated_stream = message_store.aggregated_stream(
    name="example_docs_aggregate_me2",
    partitioner=NumMessagePartitioner(),
    stream_wildcards=["aggregate-me-%"],
)
aggregated_stream.projector.update_full()
```

Whenever we call `update_full`, all new messages in the origin streams will be
appended to the relevant partition of the aggregated stream in the right order.
We will not have to call this manually though. We can use the
[`Executor`](../../getting-started/executor.md) to do it for us.

We can read from the aggregated stream directly:

```python
print(next(aggregated_stream.read(partition=2)))
#  AggregatedStreamMessage(
#      partition=2,
#      position=0,
#      message_id=UUID("1f804185-e63d-462e-b996-d6f16e5ff8af")
#  )
```

The `AggregatedStreamMessage` object contains minimal metadata about the message
in the context of the aggregated stream. It does not contain the original message
though. To get that, we need to use the message store reader.

Usually though we will not read the aggregated stream directly, but rather use
a reader or a subscription to consume it. We will cover subscriptions in the [next
chapter](getting-started-subscription.md).
```python
reader = aggregated_stream.reader()
reader.start()
print(next(reader.get_messages(timeout=1)))
reader.stop()
#  SubscriptionMessage(
#      partition=2,
#      position=0,
#      stored_message=StoredMessage(
#          message_id=UUID("1f804185-e63d-462e-b996-d6f16e5ff8af"),
#          stream="aggregate-me-1",
#          version=1,
#          message=EventA(
#              event_id=UUID("1f804185-e63d-462e-b996-d6f16e5ff8af"),
#              happened_at=datetime.datetime(2023, 10, 5, 20, 3, 26, 658725),
#              num=176,
#          ),
#          global_position=4,
#      ),
#  )
```

The `AggregatedStreamReader` will read the messages from the aggregated
stream and record its position in the stream while doing so.
If you specify a `timeout`, it will wait for that long for new messages before returning.
The next call to `get_messages` will only return new messages that have been
written to the stream since the last call.

`aggregated_stream.reader()` takes an optional `start_point` argument, which
specifies where to start reading from. See the [subscription chapter](getting-started-subscription.md)
for more details on this.

The main use case of an `AggregatedStreamReader` is for streaming
information based on messages. E.g. we can use it to implement a
GraphQL subscription that is used by a UI to live-update.

There is also an asynchonous version of the reader: `aggregated_stream.async_reader()`.

The readers use PostgreSQL's `LISTEN`/`NOTIFY` mechanism to get notified of new messages.
The synchronous version of the reader starts a new thread to listen for notifications.
The asynchronous version uses an async listener.
