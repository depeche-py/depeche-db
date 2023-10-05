# Subscription

Given the aggregated stream from the previous chapter, we can put together a
subscription.

```python
from depeche_db import Subscription
from depeche_db.tools import DbLockProvider, DbSubscriptionStateProvider

subscription = Subscription(
    group_name="sub_example_docs_aggregate_me",
    stream=link_stream,
    state_provider=DbSubscriptionStateProvider(
        name="sub_state1",
        engine=db_engine,
    ),
    lock_provider=DbLockProvider(name="locks1", engine=db_engine),
)
```

You can read from a subscription directly (but you would probably want to use
a `SubscriptionHandler` as shown next).
The emitted message is wrapped in a `SubscriptionMessage` object which contains
the metadata about the message in the context of the subscription/aggregated stream.

```python
with subscription.get_next_message() as message:
    print(message)
    message.ack()
# SubscriptionMessage(
#     partition=2,
#     position=0,
#     stored_message=StoredMessage(...)
# )
```


In order to really handle the message on a subscription we would use this instead:

```python

from depeche_db import SubscriptionHandler, SubscriptionMessage

my_handler = SubscriptionHandler(
    subscription=subscription,
)

@my_handler.register
def handle_event_a(message: SubscriptionMessage[EventA]):
    real_message = message.stored_message.message
    print(
        f"Got EventA: {real_message.num} on partition {message.partition} at {message.position}"
    )

my_handler.run_once()
# Got EventA: 150 on partition 0 at 0
# Got EventA: 175 on partition 1 at 0
# Got EventA: 94 on partition 1 at 1
# Got EventA: 89 on partition 2 at 1
# Got EventA: 73 on partition 1 at 2
# Got EventA: 66 on partition 0 at 1
# ...
```
