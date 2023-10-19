
# Subscription

Given the aggregated stream from the previous chapter, we can put together a
subscription.

```python
from depeche_db import Subscription

subscription = Subscription(
    name="sub_example_docs_aggregate_me2",
    stream=aggregated_stream,
)
```

You can read from a subscription directly (but you would probably want to use
a `SubscriptionHandler` as shown next).
The emitted message is wrapped in a `SubscriptionMessage` object which contains
the metadata about the message in the context of the subscription/aggregated stream.

```python
for message in subscription.get_next_messages(count=1):
    print(message)
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

In order to continously handle messages on a subscription we would use the
`SubscriptionHandler`:

```python
from depeche_db import SubscriptionMessage


@subscription.handler.register
def handle_event_a(msg: SubscriptionMessage[EventA]):
    real_message = msg.stored_message.message
    print(f"num={real_message.num} (partition {msg.partition} at {msg.position})")
```

You can register multiple handlers for different message types. The handled
message types must not overlap. Given your event type `E`, you can request
`SubscriptionMessage[E]`, `StoredMessage[E]` or `E` as the type of the
argument to the handler by using type hints.

```python
subscription.handler.run_once()
#  num=111 (partition 0 at 0)
#  num=199 (partition 1 at 0)
#  num=166 (partition 1 at 1)
#  num=0 (partition 0 at 1)
#  num=152 (partition 2 at 1) # we already saw 2:0 above!
#  num=172 (partition 1 at 2)
#  num=12 (partition 0 at 2)
#  ...
```

Running `run_once` will read the unprocessed messages from the subscription and call
the registered handlers (if any).

In a real application, we would not call `run_once` directly, but we would use
the [`Executor`](../../getting-started/executor.md) to do it for us.
