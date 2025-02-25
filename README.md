

<p align="center">
  <img src="https://depeche-py.github.io/depeche-db/assets/logo-bg.png" width="200" />
</p>

# Depeche DB

A library for building event-based systems on top of PostgreSQL

[![Tests](https://github.com/depeche-py/depeche-db/actions/workflows/tests.yml/badge.svg)](https://github.com/depeche-py/depeche-db/actions/workflows/tests.yml)
[![pypi](https://img.shields.io/pypi/v/depeche-db.svg)](https://pypi.python.org/pypi/depeche-db)
[![versions](https://img.shields.io/pypi/pyversions/depeche-db.svg)](https://github.com/depeche-py/depeche-db)
[![Docs](https://img.shields.io/badge/docs-here-green.svg)](https://depeche-py.github.io/depeche-db/)
[![license](https://img.shields.io/github/license/depeche-py/depeche-db.svg)](https://github.com/depeche-py/depeche-db/blob/main/LICENSE)

---

**Documentation**: [https://depeche-py.github.io/depeche-db/](https://depeche-py.github.io/depeche-db/)

**Source code**: [https://github.com/depeche-py/depeche-db](https://github.com/depeche-py/depeche-db)

---

Depeche DB is modern Python library for building event-based systems

Key features:

* Message store with optimistic concurrency control & strong ordering guarantees
* Subscriptions with "at least once" semantics
* Parallel processing of (partitioned) subscriptions
* No database polling

## Requirements

Python 3.9+
SQLAlchemy 1.4 or 2+
PostgreSQL 12+
Psycopg (Version 2 >= 2.9.3 or Version 3 >= 3.1)


## Installation

```bash
pip install depeche-db
# OR
poetry add depeche-db
```

## Example

```python
import pydantic, sqlalchemy, uuid, datetime as dt

from depeche_db import (
    MessageStore,
    StoredMessage,
    MessageHandler,
    SubscriptionMessage,
)
from depeche_db.tools import PydanticMessageSerializer

DB_DSN = "postgresql://depeche:depeche@localhost:4888/depeche_demo"
db_engine = sqlalchemy.create_engine(DB_DSN)


class MyMessage(pydantic.BaseModel):
    content: int
    message_id: uuid.UUID = pydantic.Field(default_factory=uuid.uuid4)
    sent_at: dt.datetime = pydantic.Field(default_factory=dt.datetime.utcnow)

    def get_message_id(self) -> uuid.UUID:
        return self.message_id

    def get_message_time(self) -> dt.datetime:
        return self.sent_at


message_store = MessageStore[MyMessage](
    name="example_store",
    engine=db_engine,
    serializer=PydanticMessageSerializer(MyMessage),
)
message_store.write(stream="aggregate-me-1", message=MyMessage(content=2))
print(list(message_store.read(stream="aggregate-me-1")))
# [StoredMessage(message_id=UUID('...'), stream='aggregate-me-1', version=1, message=MyMessage(content=2, message_id=UUID('...'), sent_at=datetime.datetime(...)), global_position=1)]


class ContentMessagePartitioner:
    def get_partition(self, message: StoredMessage[MyMessage]) -> int:
        return message.message.content % 10


class MyHandlers(MessageHandler[MyMessage]):
    @MessageHandler.register
    def handle_message(self, message: SubscriptionMessage[MyMessage]):
        print(message)


aggregated_stream = message_store.aggregated_stream(
    name="aggregated",
    partitioner=ContentMessagePartitioner(),
    stream_wildcards=["aggregate-me-%"],
)
subscription = aggregated_stream.subscription(
    name="example_subscription",
    handlers=MyHandlers(),
)

aggregated_stream.projector.run()
subscription.runner.run()
# MyHandlers.handle_message prints:
# SubscriptionMessage(partition=2, position=0, stored_message=StoredMessage(...))

```


## Contribute

Contributions in the form of issues, questions, feedback and pull requests are
welcome. Before investing a lot of time, let me know what you are up to so
we can see if your contribution fits the vision of the project.
