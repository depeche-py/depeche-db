import threading
import asyncio
import logging
import random
import time
from datetime import datetime
from uuid import UUID, uuid4

import pydantic
from sqlalchemy import create_engine

from depeche_db import (
    Executor,
    MessageStore,
    StoredMessage,
    MessageHandlerRegister,
    SubscriptionMessage,
    StartAtNextMessage,
)
from depeche_db._executor import ThreadedExecutor
from depeche_db._subscription import AckStrategy
from depeche_db.tools import PydanticMessageSerializer

DB_DSN = "postgresql+psycopg://depeche:depeche@localhost:4888/depeche_demo"
db_engine = create_engine(DB_DSN, pool_size=50)


class MyMessage(pydantic.BaseModel):
    content: int
    message_id: UUID = pydantic.Field(default_factory=uuid4)
    sent_at: datetime = pydantic.Field(default_factory=datetime.utcnow)

    def get_message_id(self) -> UUID:
        return self.message_id

    def get_message_time(self) -> datetime:
        return self.sent_at


message_store = MessageStore[MyMessage](
    name="store_example_rtt",
    engine=db_engine,
    serializer=PydanticMessageSerializer(MyMessage),
)


class NumMessagePartitioner:
    def get_partition(self, message: StoredMessage[MyMessage]) -> int:
        return message.message.content % 10


stream = message_store.aggregated_stream(
    name="stream_example_rtt1",
    partitioner=NumMessagePartitioner(),
    stream_wildcards=["aggregate-me-%"],
)


handlers = MessageHandlerRegister[MyMessage]()

msg_sub_recv = {}


@handlers.register
def handle_event_a(message: SubscriptionMessage[MyMessage]):
    msg_sub_recv[message.stored_message.message_id] = time.time()
    time.sleep(0.5 * random.random())  # Simulate some processing delay


msg_write_start = {}
msg_write_end = {}


def pub():
    for _ in range(50):
        stream = random.choice(["aggregate-me-1", "aggregate-me-2"])
        msg = MyMessage(content=random.randint(1, 100))
        msg_write_start[msg.get_message_id()] = time.time()
        message_store.write(stream=stream, message=msg)
        msg_write_end[msg.get_message_id()] = time.time()
        # logging.info(f"Published message")
        # time.sleep(0.01 * random.random())  # Simulate some delay between writes


def projector():
    executor = Executor(db_dsn=DB_DSN, disable_signals=True)
    executor.register(stream.projector)
    executor.run()


subscription = stream.subscription(
    name="subscription_example_rtt",
    handlers=handlers,
    start_point=StartAtNextMessage(),
    # batch_size=30,
    ack_strategy=AckStrategy.BATCHED,
)


def sub():
    executor = ThreadedExecutor(
        db_dsn=DB_DSN, disable_signals=True, stimulation_interval=0
    )
    executor.register(stream.projector)
    executor.register(subscription.runner)
    executor.run()


def main():
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
    )

    # threading.Thread(target=projector, daemon=True).start()
    threading.Thread(target=sub, daemon=True).start()

    time.sleep(1)  # Ensure the subscription is ready
    pub()
    time.sleep(5)  # Allow some time for messages to be processed

    n, acc = 0, 0
    for msg_id, start_time in msg_write_start.items():
        if msg_id in msg_sub_recv:
            recv_time = msg_sub_recv[msg_id]
            write_end_time = msg_write_end.get(msg_id, start_time)
            rtt = recv_time - write_end_time
            write_duration = write_end_time - start_time
            n += 1
            acc += rtt
            print(
                f"Message ID: {msg_id}, RTT: {rtt * 1000:.0f}ms, Write Duration: {write_duration * 1000:.0f}ms"
            )
        else:
            print(f"Message ID: {msg_id} was not received by the subscription.")
    print(
        f"Average RTT for {n} messages: {acc * 1000 / n:.0f}ms"
        if n > 0
        else "No messages received."
    )


if __name__ == "__main__":
    main()
