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
