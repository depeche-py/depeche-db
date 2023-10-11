import random
import sys
import time
from datetime import datetime
from uuid import UUID, uuid4

import pydantic
from sqlalchemy import create_engine

from depeche_db import (
    AggregatedStream,
    Executor,
    MessageProtocol,
    MessageStore,
    StoredMessage,
    Subscription,
    SubscriptionMessage,
)
from depeche_db.tools import (
    DbLockProvider,
    DbSubscriptionStateProvider,
    PydanticMessageSerializer,
)

DB_DSN = "postgresql://depeche:depeche@localhost:4888/depeche_demo"
db_engine = create_engine(DB_DSN)


class MyMessage(pydantic.BaseModel, MessageProtocol):
    content: int
    message_id: UUID = pydantic.Field(default_factory=uuid4)
    sent_at: datetime = pydantic.Field(default_factory=datetime.utcnow)

    def get_message_id(self) -> UUID:
        return self.message_id

    def get_message_time(self) -> datetime:
        return self.sent_at


message_store = MessageStore[MyMessage](
    name="example_pub_sub",
    engine=db_engine,
    serializer=PydanticMessageSerializer(MyMessage),
)


class NumMessagePartitioner:
    def get_partition(self, message: StoredMessage[MyMessage]) -> int:
        return message.message.content % 10


stream = AggregatedStream[MyMessage](
    name="example_pub_sub1",
    store=message_store,
    partitioner=NumMessagePartitioner(),
    stream_wildcards=["aggregate-me-%"],
)

subscription = Subscription(
    name="example_pub_sub",
    stream=stream,
    state_provider=DbSubscriptionStateProvider(
        name="sub_state2",
        engine=db_engine,
    ),
    lock_provider=DbLockProvider(name="locks1", engine=db_engine),
)


@subscription.handler.register
def handle_event_a(message: SubscriptionMessage[MyMessage]):
    real_message = message.stored_message.message
    print(
        f"Got message #{real_message.content} at {message.partition}:{message.position}"
    )
    time.sleep(0.05)


def pub():
    while True:
        stream = random.choice(["aggregate-me-1", "aggregate-me-2"])
        print(
            message_store.write(
                stream=stream,
                message=MyMessage(content=random.randint(1, 100)),
            )
        )
        time.sleep(0.08)


def sub():
    executor = Executor(db_dsn=DB_DSN)
    executor.register(stream.projector)
    executor.register(subscription.handler)
    executor.run()


def usage():
    print("Usage: pub_sub.py [pub|sub]")
    sys.exit(1)


def main():
    if len(sys.argv) < 2:
        usage()
    if sys.argv[1] == "pub":
        pub()
    elif sys.argv[1] == "sub":
        sub()
    else:
        usage()


if __name__ == "__main__":
    main()
