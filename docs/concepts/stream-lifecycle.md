# Stream lifecycle

Streams in a message store are append-only by default and grow indefinitely.
Depeche DB provides two operations to manage stream lifecycle: **closing** and
**archiving**.

## Closing a stream

Closing a stream writes a user-provided **tombstone message** as the final
event on the stream. After closing:

- No further messages can be written to the stream (`StreamClosedError`).
- The close message flows through the normal pipeline (projector, aggregated
  streams, subscriptions), so consumers can react to it.
- All existing messages remain readable.

This is useful when a domain object reaches a terminal state (e.g. an account
is deactivated) and no further events should be recorded.

```python
store.close_stream(
    stream="account-123",
    message=AccountDeactivatedEvent(...),
    expected_version=42,
)
```

The `expected_version` parameter is required to prevent accidental closes.
Like `write()`, an optional `conn` parameter allows closing a stream as part
of a larger transaction.

### How it works

The close message is serialized normally via the store's serializer, then a
`__depeche_closed` marker is injected into the JSONB payload. The database's
write function checks the last message in a stream for this marker before
allowing any new writes. This check runs inside the existing advisory lock,
so there are no race conditions.

When the close message is read back, the `__depeche_closed` marker is
transparently stripped before deserialization, so your serializer (including
strict Pydantic models) will not see it.


## Archiving a stream

Archiving moves messages from a closed stream into a separate **archive table**,
reducing the size of the main message store table. Only the tombstone message
remains in the main table, continuing to prevent re-use of the stream name.

```python
from depeche_db import Storage

# Create an archive table (you control the name and lifecycle)
archive = Storage.create_archive_table("my_archive_2026_03", engine)

# Archive a closed stream
store.archive_stream("account-123", archive_table=archive)
```

### What happens during archive

1. All messages (including the close event) are **copied** to the archive table.
2. All messages **except** the close event are **deleted** from the main table.
3. Aggregated stream pointers are removed, except for the close event's pointer
   (so late-starting subscriptions still see it).

All of this happens in a single transaction. Like other write methods,
`archive_stream()` accepts an optional `conn` parameter.

### Archive table management

Archive tables are created and managed by you, not by the library. This gives
you full control over:

- **Naming**: use date-based names (`archive_2026_03`), per-tenant names, or
  whatever fits your needs.
- **Cold storage**: export archive tables to object storage, then drop them.
- **Retention**: keep archive tables as long as you need, drop them when you
  don't.

Use `Storage.create_archive_table(name, engine)` to create a table with the
correct schema. You can reuse the same archive table for multiple streams or
create a new one for each archival batch.

### Archiving requires a closed stream

Calling `archive_stream()` on a stream that has not been closed raises
`StreamNotClosedError`. This ensures consumers have a chance to see the close
event and clean up their state before messages are removed.

A typical workflow:

1. Close the stream (consumers see the close event).
2. Wait for consumers to process the close event (hours, days — your choice).
3. Archive the stream.


## Subscription resilience

During archiving, there is a brief window where a subscription may read
pointers from the aggregated stream whose underlying messages have already been
moved to the archive table. Subscriptions handle this gracefully by **skipping
missing messages** and advancing their position. No errors are raised.


## Example: full lifecycle

```python
from depeche_db import MessageStore, Storage

store = MessageStore(name="example", engine=engine, serializer=my_serializer)

# 1. Write events
store.write("order-1", OrderCreated(...))
store.write("order-1", OrderShipped(...))

# 2. Close the stream when the order is finalized
store.close_stream(
    stream="order-1",
    message=OrderCompleted(...),
    expected_version=2,
)

# 3. Writing after close raises StreamClosedError
# store.write("order-1", ...)  # raises StreamClosedError

# 4. Later, archive the stream
archive = Storage.create_archive_table("orders_archive_2026_q1", engine)
store.archive_stream("order-1", archive_table=archive)

# The main table now only has the OrderCompleted tombstone for order-1.
# The archive table has the complete history.
```
