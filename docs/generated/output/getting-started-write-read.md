
# Writing & reading messages

First, create a SQLAlchemy engine with your database connection:
```python
from sqlalchemy import create_engine

DB_DSN = "postgresql://depeche:depeche@localhost:4888/depeche_demo"
# If you are using psycopg 3, use the following DSN instead:
# DB_DSN = "postgresql+psycopg://depeche:depeche@localhost:4888/depeche_demo"
db_engine = create_engine(DB_DSN)
```

Then we define our message types using pydantic. Using pydantic is optional,
but it makes serialization straightforward.
```python
from datetime import datetime
from uuid import UUID, uuid4

import pydantic


class MyEvent(pydantic.BaseModel):
    event_id: UUID = pydantic.Field(default_factory=uuid4)
    happened_at: datetime = pydantic.Field(default_factory=datetime.utcnow)

    def get_message_id(self) -> UUID:
        return self.event_id

    def get_message_time(self) -> datetime:
        return self.happened_at


class EventA(MyEvent):
    num: int


class EventB(MyEvent):
    text: str
```

Now we are ready to create our message store. This will create a new table
`example_basic_messages` when it is called the first time.
```python
from depeche_db import MessageStore
from depeche_db.tools import PydanticMessageSerializer

message_store = MessageStore[EventA | EventB](
    name="example_docs2",
    engine=db_engine,
    serializer=PydanticMessageSerializer(EventA | EventB),
)
```

Now we write an event to the stream

```python
stream = f"stream-{uuid4()}"

result = message_store.write(stream=stream, message=EventA(num=42))
print(result)
#  MessagePosition(stream='stream-<uuid>', version=1, global_position=1)
```

Here is how we can read the messages:
```python
print(next(message_store.read(stream)))
#  StoredMessage(
#    message_id=UUID('<uuid>'),
#    stream='stream-<uuid>',
#    version=1,
#    message=EventA(
#      event_id=UUID('<uuid>'),
#      num=42,
#      happened_at=datetime.datetime(...)
#    ),
#    global_position=1
#  )
```

Please note that when reading, the original message is wrapped in a `StoredMessage`
object, which contains the metadata about the message.

When we write, we can pass an `expected_version` parameter, which gives us
optimistic concurrency control.
```python
# this fails because the expected version is 0, but the stream already has a message
message_store.write(stream=stream, message=EventA(num=23), expected_version=0)
# this is fine, because we expect the right version
message_store.write(stream=stream, message=EventA(num=23), expected_version=1)
```
