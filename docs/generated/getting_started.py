import _docgen

doc = _docgen.DocGen(__file__)

doc.output("getting-started-write-read.md")
doc.md(
    """\
    # Writing & reading messages

    First, create a SQLAlchemy engine with your database connection:\
    """
)
from sqlalchemy import create_engine

DB_DSN = "postgresql://depeche:depeche@localhost:4888/depeche_demo"
db_engine = create_engine(DB_DSN)

doc.md(
    """\
    Then we define our message types using pydantic. Using pydantic is optional,
    but it makes serialization straightforward.\
    """
)
from datetime import datetime
from uuid import UUID, uuid4

import pydantic

from depeche_db import MessageProtocol


class MyEvent(pydantic.BaseModel, MessageProtocol):
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


doc.md(
    """\
    Now we are ready to create our message store. This will create a new table
    `example_basic_messages` when it is called the first time.\
    """
)

from depeche_db import MessageStore
from depeche_db.tools import PydanticMessageSerializer

message_store = MessageStore[EventA | EventB](
    name="example_docs2",
    engine=db_engine,
    serializer=PydanticMessageSerializer(EventA | EventB),
)

doc.md(
    """\
    Now we write an event to the stream
    """
)

stream = f"stream-{uuid4()}"

result = message_store.write(stream=stream, message=EventA(num=42))
doc.show(result)
# > MessagePosition(stream='stream-<uuid>', version=1, global_position=1)

doc.md(
    """\
    Here is how we can read the messages:\
    """
)

doc.show(next(message_store.read(stream)))
# > StoredMessage(
# >   message_id=UUID('<uuid>'),
# >   stream='stream-<uuid>',
# >   version=1,
# >   message=EventA(
# >     event_id=UUID('<uuid>'),
# >     num=42,
# >     happened_at=datetime.datetime(...)
# >   ),
# >   global_position=1
# > )

doc.md(
    """\
    Please note that when reading, the original message is wrapped in a `StoredMessage`
    object, which contains the metadata about the message.

    When we write, we can pass an `expected_version` parameter, which gives us
    optimistic concurrency control.\
    """
)

# this fails because the expected version is 0, but the stream already has a message
with doc.catch():
    message_store.write(stream=stream, message=EventA(num=23), expected_version=0)
# this is fine, because we expect the right version
message_store.write(stream=stream, message=EventA(num=23), expected_version=1)

doc.output("getting-started-aggregated-stream.md")
doc.md(
    """\
    # Aggregated stream

    We will use the same message store as in the previous chapter here, but we will
    create a new set of streams within it:
    """
)
import random

for _ in range(20):
    n = random.randint(0, 200)
    stream = f"aggregate-me-{n % 5}"
    message_store.write(stream=stream, message=EventA(num=n))


doc.md(
    """\
    For our aggregated stream, we need to prepare a partition function (or rather class).
    """
)
from depeche_db import AggregatedStream, StoredMessage


class NumMessagePartitioner:
    def get_partition(self, message: StoredMessage[EventA | EventB]) -> int:
        if isinstance(message.message, EventA):
            return message.message.num % 3
        return 0


doc.md(
    """\
    Now we can put together the aggregated stream.
    """
)

aggregated_stream = AggregatedStream[EventA | EventB](
    name="example_docs_aggregate_me2",
    store=message_store,
    partitioner=NumMessagePartitioner(),
    stream_wildcards=["aggregate-me-%"],
)
aggregated_stream.projector.update_full()

doc.md(
    """\
    Whenever we call `update_full`, all new messages in the origin streams will be
    appended to the relevant partition of the aggregated stream in the right order.
    We will not have to call this manually though. We can use the
    [`Executor`](../../getting-started/executor.md) to do it for us.

    We can read the aggregated stream. The items returned from it are just
    pointers to messages in the message store.
    """
)

first_on_partition0 = next(
    aggregated_stream.read(conn=db_engine.connect(), partition=0)
)
doc.show(first_on_partition0.message_id)
# > 4680cbaf-977e-43a4-afcb-f88e92043e9c (this is the message ID of the first message in partition 0)

with message_store.reader() as reader:
    doc.show(reader.get_message_by_id(first_on_partition0.message_id))
# > StoredMessage(
# >     message_id=UUID("4680cbaf-977e-43a4-afcb-f88e92043e9c"),
# >     stream="aggregate-me-0",
# >     ...
# > )

doc.md(
    """\
    Usually, we would not read the aggregated stream directly, but we would use
    a subscription to consume it. We will get to that in the [next
    chapter](getting-started-subscription.md).\
    """
)

doc.output("getting-started-subscription.md")
doc.md(
    """\
    # Subscription

    Given the aggregated stream from the previous chapter, we can put together a
    subscription.
    """
)


from depeche_db import Subscription
from depeche_db.tools import DbLockProvider, DbSubscriptionStateProvider

subscription = Subscription(
    name="sub_example_docs_aggregate_me2",
    stream=aggregated_stream,
    state_provider=DbSubscriptionStateProvider(
        name="sub_state1",
        engine=db_engine,
    ),
    lock_provider=DbLockProvider(name="locks1", engine=db_engine),
)

doc.md(
    """\
    You can read from a subscription directly (but you would probably want to use
    a `SubscriptionHandler` as shown next).
    The emitted message is wrapped in a `SubscriptionMessage` object which contains
    the metadata about the message in the context of the subscription/aggregated stream.
    """
)

for message in subscription.get_next_messages(count=1):
    doc.show(message)
# > SubscriptionMessage(
# >     partition=2,
# >     position=0,
# >     stored_message=StoredMessage(
# >         message_id=UUID("1f804185-e63d-462e-b996-d6f16e5ff8af"),
# >         stream="aggregate-me-1",
# >         version=1,
# >         message=EventA(
# >             event_id=UUID("1f804185-e63d-462e-b996-d6f16e5ff8af"),
# >             happened_at=datetime.datetime(2023, 10, 5, 20, 3, 26, 658725),
# >             num=176,
# >         ),
# >         global_position=4,
# >     ),
# > )

doc.md(
    """\
    In order to continously handle messages on a subscription we would use the
    `SubscriptionHandler`:
    """
)

from depeche_db import SubscriptionMessage


@subscription.handler.register
def handle_event_a(msg: SubscriptionMessage[EventA]):
    real_message = msg.stored_message.message
    doc.show(f"num={real_message.num} (partition {msg.partition} at {msg.position})")


doc.md(
    """\
    You can register multiple handlers for different message types. The handled
    message types must not overlap. Given your event type `E`, you can request
    `SubscriptionMessage[E]`, `StoredMessage[E]` or `E` as the type of the
    argument to the handler by using type hints.
    """
)


doc.begin_show()
subscription.handler.run_once()
doc.end_show()
# > num=111 (partition 0 at 0)
# > num=199 (partition 1 at 0)
# > num=166 (partition 1 at 1)
# > num=0 (partition 0 at 1)
# > num=152 (partition 2 at 1) # we already saw 2:0 above!
# > num=172 (partition 1 at 2)
# > num=12 (partition 0 at 2)
# > ...

doc.md(
    """\
    Running `run_once` will read the unprocessed messages from the subscription and call
    the registered handlers (if any).

    In a real application, we would not call `run_once` directly, but we would use
    the [`Executor`](../../getting-started/executor.md) to do it for us.\
    """
)
