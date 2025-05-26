
# Subscription

Given the aggregated stream from the previous chapter, we can put together a
subscription.

```python
subscription = aggregated_stream.subscription(
    name="sub_example_docs_aggregate_me",
)
```

You can read from a subscription directly. Whenever `get_next_messages` emits
a message, it will update the position of the subscription, so that the next
call will return the next message.

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

Reading from a subscription directly is not the most common use case though.
In order to continously handle messages on a subscription we create a
`MessageHandlerRegister` and pass this in when we create the subscription.

On the `MessageHandlerRegister` we register a handler for the
message type(s) we are interested in.
You can register multiple handlers for different message types but the handled
message types must not overlap. Given your message type `E`, you can request
`SubscriptionMessage[E]`, `StoredMessage[E]` or `E` as the type of the
argument to the handler by using type hints.

```python
from depeche_db import SubscriptionMessage, MessageHandlerRegister

handlers = MessageHandlerRegister[EventA | EventB]()


@handlers.register
def handle_event_a(msg: SubscriptionMessage[EventA]):
    real_message = msg.stored_message.message
    print(f"num={real_message.num} (partition {msg.partition} at {msg.position})")
```

Now we can create a new subscription with these handlers.

```python
subscription = aggregated_stream.subscription(
    name="sub_example_docs_with_handlers",
    handlers=handlers,
)
```

Running `run_once` will read the unprocessed messages from the subscription and call
the registered handlers (if any).

```python
subscription.runner.run_once()
#  num=111 (partition 0 at 0)
#  num=199 (partition 1 at 0)
#  num=166 (partition 1 at 1)
#  num=0 (partition 0 at 1)
#  num=152 (partition 2 at 0)
#  num=172 (partition 1 at 2)
#  num=12 (partition 0 at 2)
#  ...
```

Running `run_once` will read the unprocessed messages from the subscription and call
the registered handlers (if any).

In a real application, we would not call `run_once` directly, but we would use
the [`Executor`](../../getting-started/executor.md) to do it for us.

## Starting position

A subscription by default starts at the beginning of the stream. If we want to
change this behaviour, we can pass in a `SubscriptionStartPosition` object when we
create the subscription. This object can be one of the following:
```python
from datetime import timezone
from depeche_db import StartAtNextMessage, StartAtPointInTime

subscription_next = aggregated_stream.subscription(
    name="sub_example_docs_aggregate_me_next", start_point=StartAtNextMessage()
)

subscription_point_in_time = aggregated_stream.subscription(
    name="sub_example_docs_aggregate_me_next",
    start_point=StartAtPointInTime(
        datetime(2023, 10, 5, 14, 0, 0, 0, tzinfo=timezone.utc)
    ),
)
```

## Acknowledgement strategy

By default, a subscription will use `AckStrategy.SINGLE` which acknowledges
messages as soon as they are processed. This gives you the best guarantees
for message delivery, but it can lead to performance issues if you have a
lot of messages. It is a database write for each message after all.

If your application has to process a high number of messages, you can use
`AckStrategy.BATCHED` which will acknowledge messages in batches. Together with
the `batch_size` parameter, you can control how many messages will be
processed (at most) before an acknowledgement is forced.

You can change the acknowledgement strategy of a subscription. Thus, it is
possible to use the batched strategy for the initial processing of a high
number of messages and then switch to the single strategy for continuous
processing of new messages.
