
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
from depeche_db import AggregatedStream, StoredMessage


class NumMessagePartitioner:
    def get_partition(self, message: StoredMessage[EventA | EventB]) -> int:
        if isinstance(message.message, EventA):
            return message.message.num % 3
        return 0
```

Now we can put together the aggregated stream.

```python
aggregated_stream = AggregatedStream[EventA | EventB](
    name="example_docs_aggregate_me2",
    store=message_store,
    partitioner=NumMessagePartitioner(),
    stream_wildcards=["aggregate-me-%"],
)
aggregated_stream.projector.update_full()
```

Whenever we call `update_full`, all new messages in the origin streams will be
appended to the relevant partition of the aggregated stream in the right order.
We will not have to call this manually though. We can use the
[`Executor`](../../getting-started/executor.md) to do it for us.


Usually, we do not read the aggregated stream directly, but we would use
a subscription to consume it. We will get to that in the [next
chapter](getting-started-subscription.md).
