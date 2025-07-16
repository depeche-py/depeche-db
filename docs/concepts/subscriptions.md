# Subscriptions

Given an aggregated stream, we can create a number of subscriptions on it which
are discriminated by a name. A subscription works roughly like this:

1. Wait for a trigger
1. Get the current state (pairs of partition number & last processed position)
1. For each partition that has messages after the known position
    1. Try to acquire a lock (subscription group name, partition)
    1. Re-validate the state (a parallel process might have advanced it already)
    1. Process the message(s) (depending on the batch size)
    1. Update the state with the new position
    1. Release the lock

This algorithm allows us to process messages concurrently while still honoring
the ordering guarantees within a partition because only one instance can
process messages from any given partition at any time. It can be described as
an instance of the competing consumers pattern.

The order of steps 3c and 3d makes this a "at least once" delivery system,
because the message is processed before the new position is recorded.


### Change in 0.12.0

Partition selection in step 3 is NOT anymore preferring the partition with the
oldest message. It now prefers the partition with the most unprocessed messages.
In order to help with fairness this selection is randomized a bit.


## Start point

When a subscription run for the first time, it decide where it should start consuming
messages. Three options exist:

* Beginning of the aggregated stream (default)
* Next message: Start at the first message that is appended to the stream
  after the subscription started
* Point in time: Start at messages with a message time greater or equal a given
  datetime.

## Services required by subscriptions

A subscription requires two services provided to it:

1. Subscription state storage
1. (Distributed) Locks

Both of the services have a default implementation but it may be a good idea to
check if you should customize/replace it to your needs. The interfaces of both
services are pretty simple.

### State

The best location for the subscription state is - especially in the context of
transactionally safe storage - close to the state that is altered in the course
of the application code handling messages.

If you can manage to get the subscription state updates within the same
transaction as the application state, you essentially implemented a
"exactly-once" message processing mechanism.

### Locks

The default locking provider uses again PostgreSQL, namely advisory locks
through the `pals` library. If for some reason your system is not actually
distributed to multiple machines, you could replace if with a simpler & faster
locking system. Or maybe you already have a distributed locking infrastructure,
then it might be worth a look to use it here too.
