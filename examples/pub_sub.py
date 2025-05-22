import asyncio
import logging
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
    MessageStore,
    StoredMessage,
    Subscription,
    MessageHandlerRegister,
    SubscriptionMessage,
    SubscriptionMessageHandler,
    SubscriptionRunner,
    StartAtNextMessage,
)
from depeche_db.tools import PydanticMessageSerializer

DB_DSN = "postgresql+psycopg://depeche:depeche@localhost:4888/depeche_demo"
db_engine = create_engine(DB_DSN)


class MyMessage(pydantic.BaseModel):
    content: int
    message_id: UUID = pydantic.Field(default_factory=uuid4)
    sent_at: datetime = pydantic.Field(default_factory=datetime.utcnow)

    def get_message_id(self) -> UUID:
        return self.message_id

    def get_message_time(self) -> datetime:
        return self.sent_at


message_store = MessageStore[MyMessage](
    name="store_example_pub_sub",
    engine=db_engine,
    serializer=PydanticMessageSerializer(MyMessage),
)


class NumMessagePartitioner:
    def get_partition(self, message: StoredMessage[MyMessage]) -> int:
        return message.message.content % 10


stream = message_store.aggregated_stream(
    name="stream_example_pub_sub1",
    partitioner=NumMessagePartitioner(),
    stream_wildcards=["aggregate-me-%"],
)


handlers = MessageHandlerRegister[MyMessage]()


@handlers.register
def handle_event_a(message: SubscriptionMessage[MyMessage]):
    with db_engine.connect() as conn:
        real_message = message.stored_message.message
        print(
            f"Got message #{real_message.content} at {message.partition}:{message.position}"
        )
        time.sleep(0.05)
        message.ack.execute(conn=conn)
        if random.random() < 0.05:
            print("Simulating error")
            conn.rollback()
        else:
            conn.commit()


subscription = stream.subscription(
    name="subscription_example_pub_sub",
    handlers=handlers,
    start_point=StartAtNextMessage(),
)


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


def projector():
    executor = Executor(db_dsn=DB_DSN)
    executor.register(stream.projector)
    executor.run()


def sub():
    executor = Executor(db_dsn=DB_DSN)
    executor.register(stream.projector)
    executor.register(subscription.runner)
    executor.run()


def sub_direct():
    while True:
        subscription.runner.run_once()


def reader():
    reader = stream.reader()
    reader.start()
    try:
        for stored_message in reader.get_messages():
            print(f"Reader got message: {stored_message.message.content}")
    finally:
        reader.stop()


async def async_reader():
    reader = stream.async_reader()
    await reader.start()
    try:
        async for stored_message in reader.get_messages():
            print(f"Async reader got message: {stored_message.message.content}")
    finally:
        await reader.stop()


def usage():
    print("Usage: pub_sub.py [pub|sub]")
    sys.exit(1)


def main():
    logging.basicConfig()
    if len(sys.argv) < 2:
        usage()
    if sys.argv[1] == "pub":
        pub()
    elif sys.argv[1] == "sub":
        sub()
    elif sys.argv[1] == "sub_direct":
        sub_direct()
    elif sys.argv[1] == "projector":
        projector()
    elif sys.argv[1] == "reader":
        reader()
    elif sys.argv[1] == "async_reader":
        asyncio.run(async_reader())
    else:
        usage()


if __name__ == "__main__":
    main()
