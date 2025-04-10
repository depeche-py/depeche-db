# Depeche DB

Depeche DB is a modern Python library for building event-based systems

Key features:

* Message store with optimistic concurrency control & strong ordering guarantees
* Subscriptions with "at least once" semantics
* Parallel processing of (partitioned) subscriptions
* No database polling

## Use cases

Depeche DB can be used to cover the following:

* Building event-sourced systems
* Building pub-sub systems
* Asynchronous command handling
* Use as a transactional outbox when application state and event store need to be in-sync

## Documentation

If you are interested in a quick tour of the features shown with example code,
please have a look at the [Getting Started
Guide](getting-started/installation.md).

More details on the data model and algorithms can be found on the
[Concepts](concepts/data-model.md) pages.

The [API Docs](api/message_store.md) give you all the details on how to use
the library.


## Justification

You might ask "Why on earth use PostgreSQL to store events?". For many use
cases, you are probably right to assume that this is not the best solution.

The main driver behind the development of this library is simplicity. We want
to use boring tools and benefit from the knowledge built over years. With a
RDBMS like PostgreSQL, you know what you get. You have proven ways of scaling
them. You know how a backup works. You known their limits.

So, if an application does not generate or handle hundreds of events per second
and will not accumulate millions of messages, maybe using a RDBMS might be a
good choice.

## Background & prior art

Depeche DB is obviously influenced by message stores like
[EventStoreDB](https://www.eventstore.com/) and takes some inspiration from
[Kafka](https://kafka.apache.org/). More inspiration is taken from
[Marten](https://martendb.io/), a project that also implements an event store
on top of PostgreSQL. Another strong influence has been the [Message
DB](https://github.com/message-db/message-db) project which is pretty similar
on the implementation side.
