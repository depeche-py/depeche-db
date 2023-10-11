# Depeche DB

A library for building event-based systems on top of PostgreSQL

## Features

Depeche DB has two main parts:

### Message store

* store & read messages
* multiple streams
* optimistic concurrency control

## Subscriptions

* aggregate messages from multiple streams
* subscribe to aggregated stream
* no polling (uses PostgreSQL `NOTIFY`/`LISTEN`)
* tools for keeping track of subscription state
* concurrent processing (aggregated streams can be partioned)


## Background

Depeche DB is obviously influenced by message stores like
[EventStoreDB](https://www.eventstore.com/) and takes some inspiration from
[Kafka](https://kafka.apache.org/). More inspiration is taken from
[Marten](https://martendb.io/), a project that implements an event store on top
of PostgreSQL. Another strong influence has been the [Message
DB](https://github.com/message-db/message-db) project which is pretty similar
on the implementation side.
