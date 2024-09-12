
# Aggregated stream

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

There is also a reader that will resolve the link to the message store for us:

```python
print(next(aggregated_stream.loaded_reader.read(partition=2)))
#  LoadedAggregatedStreamMessage(
#      partition=2,
#      position=0,
#      stored_message=StoredMessage(
#          message_id=UUID("1f804185-e63d-462e-b996-d6f16e5ff8af"),
#          stream="aggregate-me-4",
#          version=1,
#          message=EventA(
#              event_id=UUID("1f804185-e63d-462e-b996-d6f16e5ff8af"),
#              happened_at=datetime.datetime(2024, 1, 21, 20, 25, 7, 217530),
#              num=194,
#          ),
#          global_position=8,
#      ),
#  )
```

Usually though we will not read the aggregated stream directly, but rather use
a subscription to consume it. We will get to that in the [next
chapter](getting-started-subscription.md).
