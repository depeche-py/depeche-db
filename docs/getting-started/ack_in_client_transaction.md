# Exactly once Delivery

If the actions that follow the consumption of messages in a subscription take
place in the **same** database that is also used for the subscription state
recording, you can achieve `exactly once` delivery. You need to use
`ack_strategy=AckStrategy.SINGLE`, otherwise `message.ack.execute` will
raise a `RuntimeError`.

Consider the following code:

```python
@handlers.register
def handle_some_message(message: SubscriptionMessage[MyMessage]):
    with db_engine.connect() as conn:
        real_message = message.stored_message.message

        # do work on conn

        message.ack.execute(conn=conn)
        conn.commit()
```

In this example, the update of the subscription state is done in the same
transaction as the work of the handler. Thus, if the work fails for some reason,
the message will be delivered again. If on the other hand, the handler transaction
manages to commit, the message is also marked as processed.

Please note that it is **very important** that the handler finishes its transaction
before the control is returned to the subscription. If you keep the transaction
open for longer, the subscription behavior is undefined and it will very likely
crash or - even worse - it might skip messages.

When the acknowledgement is executed and later rolled back from the client
code, the current batch of messages is cancelled by the subscription. This does
not influence the overall semantics of the subscription but it has a
performance impact. Having a high error rate on a handler might decrease the
performance of a subscription because the effective batch size may be a lot
lower than the configured batch size.
