# Subscriptions

Given an aggregated stream, we can create a number of subscriptions on it which
are discriminated by a name. A subscription works roughly like this:

1. Wait for a trigger
1. Get the current state (pairs of partition number & last processed position)
1. For each partition that has messages after the known position (ordered by the
   message time of the oldest unprocessed message)
    1. Try to acquire a lock (subscription group name, partition)
    1. Re-validate the state (a parallel process might have advanced it already)
    1. Process the message(s) (depending on the batch size)
    1. Update the state with the new position
    1. Release the lock

This algorithm allows us to process messages concurrently while still honoring
the ordering guarantees within a partition because only one instance can
process messages from any given partition at any time. It can be described as
an instance of the competing consumers pattern.

The ordering done in step 3 is not necessary to keep any of the guarantees but
helps to keep up with the expectation that messages in the system are processed
roughly in the order given by their message time. It also helps with fairness,
because it will favor processing older messages first.

The order of steps 3c and 3d makes this a "at least once" delivery system,
because the message is processed before the new position is recorded.

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
