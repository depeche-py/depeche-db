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
    def get_partition(self, message: StoredMessage[EventA]) -> int:
        return message.message.num % 3
```

Now we can put together the aggregated stream.

```python
from depeche_db import LinkStream, StreamProjector

link_stream = LinkStream(
    name="example_docs_aggregate_me",
    store=message_store,
)
stream_projector = StreamProjector(
    stream=link_stream,
    partitioner=NumMessagePartitioner(),
    stream_wildcards=["aggregate-me-%"],
)
stream_projector.update_full()
```

Whenever we call `update_full`, all new messages in the origin streams will be
appended to the relevant partition of the aggregated stream in the right order.
We will not have to call this manually, but we can use the `TODO executor?!`.

Now let's read from the aggregated stream. (We would usually not to this, because
we rather consume aggregated stream through subscriptions.)

```python
result = next(link_stream.read(conn=db_engine.connect(), partition=0))
print(result)
# 4680cbaf-977e-43a4-afcb-f88e92043e9c (this is the message ID of the first message in partition 0)

with message_store.reader() as reader:
    print(reader.get_message_by_id(result))
# StoredMessage(
#     message_id=UUID("4680cbaf-977e-43a4-afcb-f88e92043e9c"),
#     stream="aggregate-me-0",
#     ...
# )
```
