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
# If you are using psycopg 3, use the following DSN instead:
# DB_DSN = "postgresql+psycopg://depeche:depeche@localhost:4888/depeche_demo"
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
from depeche_db import StoredMessage


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

aggregated_stream = message_store.aggregated_stream(
    name="example_docs_aggregate_me2",
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

    We can read from the aggregated stream directly:
    """
)

doc.show(next(aggregated_stream.read(partition=2)))
# > AggregatedStreamMessage(
# >     partition=2,
# >     position=0,
# >     message_id=UUID("1f804185-e63d-462e-b996-d6f16e5ff8af")
# > )


doc.md(
    """\
    The `AggregatedStreamMessage` object contains minimal metadata about the message
    in the context of the aggregated stream. It does not contain the original message
    though. To get that, we need to use the message store reader.

    Usually though we will not read the aggregated stream directly, but rather use
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


subscription = aggregated_stream.subscription(
    name="sub_example_docs_aggregate_me",
)

doc.md(
    """\
    You can read from a subscription directly. Whenever `get_next_messages` emits
    a message, it will update the position of the subscription, so that the next
    call will return the next message.

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
    Reading from a subscription directly is not the most common use case though.
    In order to continously handle messages on a subscription we create a
    `MessageHandlerRegister` and pass this in when we create the subscription.

    On the `MessageHandlerRegister` we register a handler for the
    message type(s) we are interested in.
    You can register multiple handlers for different message types but the handled
    message types must not overlap. Given your message type `E`, you can request
    `SubscriptionMessage[E]`, `StoredMessage[E]` or `E` as the type of the
    argument to the handler by using type hints.
    """
)

from depeche_db import SubscriptionMessage, MessageHandlerRegister

handlers = MessageHandlerRegister[EventA | EventB]()


@handlers.register
def handle_event_a(msg: SubscriptionMessage[EventA]):
    real_message = msg.stored_message.message
    doc.show(f"num={real_message.num} (partition {msg.partition} at {msg.position})")


doc.md(
    """\
    Now we can create a new subscription with these handlers.
    """
)

subscription = aggregated_stream.subscription(
    name="sub_example_docs_with_handlers",
    handlers=handlers,
)

doc.md(
    """\
    Running `run_once` will read the unprocessed messages from the subscription and call
    the registered handlers (if any).
    """
)

doc.begin_show()
subscription.runner.run_once()
doc.end_show()
# > num=111 (partition 0 at 0)
# > num=199 (partition 1 at 0)
# > num=166 (partition 1 at 1)
# > num=0 (partition 0 at 1)
# > num=152 (partition 2 at 0)
# > num=172 (partition 1 at 2)
# > num=12 (partition 0 at 2)
# > ...

doc.md(
    """\
    Running `run_once` will read the unprocessed messages from the subscription and call
    the registered handlers (if any).

    In a real application, we would not call `run_once` directly, but we would use
    the [`Executor`](../../getting-started/executor.md) to do it for us.


    A subscription by default starts at the beginning of the stream. If we want to
    change this behaviour, we can pass in a `SubscriptionStartPosition` object when we
    create the subscription. This object can be one of the following:\
    """
)

from datetime import timezone
from depeche_db import StartAtNextMessage, StartAtPointInTime

subscription_next = aggregated_stream.subscription(
    name="sub_example_docs_aggregate_me_next", start_point=StartAtNextMessage()
)

subscription_point_in_time = aggregated_stream.subscription(
    name="sub_example_docs_aggregate_me_next",
    start_point=StartAtPointInTime(
        datetime(2023, 10, 5, 14, 0, 0, 0, tzinfo=timezone.utc)
    ),
)
