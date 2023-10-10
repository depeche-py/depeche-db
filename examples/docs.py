from sqlalchemy import create_engine

DB_DSN = "postgresql://depeche:depeche@localhost:4888/depeche_demo"
db_engine = create_engine(DB_DSN)

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


from depeche_db import MessageStore
from depeche_db.tools import PydanticMessageSerializer

message_store = MessageStore[EventA | EventB](
    name="example_docs2",
    engine=db_engine,
    serializer=PydanticMessageSerializer(EventA | EventB),
)

stream = f"stream-{uuid4()}"

result = message_store.write(stream=stream, message=EventA(num=42))
print(result)
# MessagePosition(stream='stream-<uuid>', version=1, global_position=1)

print(next(message_store.read(stream)))
# StoredMessage(
#   message_id=UUID('<uuid>'),
#   stream='stream-<uuid>',
#   version=1,
#   message=EventA(
#     event_id=UUID('<uuid>'),
#     num=42,
#     happened_at=datetime.datetime(...)
#   ),
#   global_position=1
# )

import random

for _ in range(20):
    n = random.randint(0, 200)
    stream = f"aggregate-me-{n % 5}"
    message_store.write(stream=stream, message=EventA(num=n))


from depeche_db import LinkStream, StoredMessage


class NumMessagePartitioner:
    def get_partition(self, message: StoredMessage[EventA | EventB]) -> int:
        if isinstance(message.message, EventA):
            return message.message.num % 3
        return 0


link_stream = LinkStream[EventA | EventB](
    name="example_docs_aggregate_me2",
    store=message_store,
    partitioner=NumMessagePartitioner(),
    stream_wildcards=["aggregate-me-%"],
)
link_stream.projector.update_full()

first_id = next(link_stream.read(conn=db_engine.connect(), partition=0))
print(first_id)
# 4680cbaf-977e-43a4-afcb-f88e92043e9c (this is the message ID of the first message in partition 0)

with message_store.reader() as reader:
    print(reader.get_message_by_id(first_id))
# StoredMessage(
#     message_id=UUID("4680cbaf-977e-43a4-afcb-f88e92043e9c"),
#     stream="aggregate-me-0",
#     ...
# )

from depeche_db import Subscription
from depeche_db.tools import DbLockProvider, DbSubscriptionStateProvider

subscription = Subscription(
    group_name="sub_example_docs_aggregate_me2",
    stream=link_stream,
    state_provider=DbSubscriptionStateProvider(
        name="sub_state1",
        engine=db_engine,
    ),
    lock_provider=DbLockProvider(name="locks1", engine=db_engine),
)
with subscription.get_next_message() as message:
    print(message)
    message.ack()
# SubscriptionMessage(
#     partition=2,
#     position=0,
#     stored_message=StoredMessage(
#         message_id=UUID("1f804185-e63d-462e-b996-d6f16e5ff8af"),
#         stream="aggregate-me-1",
#         version=1,
#         message=EventA(
#             event_id=UUID("1f804185-e63d-462e-b996-d6f16e5ff8af"),
#             happened_at=datetime.datetime(2023, 10, 5, 20, 3, 26, 658725),
#             num=176,
#         ),
#         global_position=4,
#     ),
# )

from depeche_db import SubscriptionHandler, SubscriptionMessage

my_handler = SubscriptionHandler(
    subscription=subscription,
)


@my_handler.register
def handle_event_a(message: SubscriptionMessage[EventA]):
    real_message = message.stored_message.message
    print(
        f"Got EventA: {real_message.num} on partition {message.partition} at {message.position}"
    )


my_handler.run_once()
