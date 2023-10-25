import os
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
    SubscriptionMessageHandler,
    SubscriptionRunner,
    MessageHandlerRegister,
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


stream = message_store.aggregated_stream(
    name="example_pub_sub1",
    partitioner=NumMessagePartitioner(),
    stream_wildcards=["aggregate-me-%"],
)

HANDLED = 0
handlers = MessageHandlerRegister[MyMessage]()


@handlers.register
def handle_event_a(message: SubscriptionMessage[MyMessage]):
    real_message = message.stored_message.message
    print(
        f"Got message #{real_message.content} at {message.partition}:{message.position}"
    )
    time.sleep(0.05)


subscription = stream.subscription(name="example_pub_sub", handlers=handlers)


def pub():
    print("Publishing messages")
    start = time.time()
    n = 0
    while time.time() - start < 10:
        n += 1
        stream = random.choice(["aggregate-me-1", "aggregate-me-2"])
        message_store.write(
            stream=stream,
            message=MyMessage(content=random.randint(1, 100)),
        )
    print(f"Publisher sent {n / (time.time() - start):.2f} msg/s")


def sub():
    print("Subscribing to messages")
    executor = Executor(db_dsn=DB_DSN)
    executor.register(stream.projector)
    executor.register(subscription.runner)
    start = time.time()
    executor.run()
    print(f"Subscriber handled {HANDLED / (time.time() - start):.2f} mgs/s")


def run_test():
    import subprocess
    import select

    pub = subprocess.Popen(
        ["python", "-u", "-m", "examples.pub_sub_performance", "pub"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    subs = [
        subprocess.Popen(
            ["python", "-u", "-m", "examples.pub_sub_performance", "sub"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        for _ in range(4)
    ]
    pub_done = False
    try:
        while True:
            if not pub_done and pub.poll() is not None:
                pub_done = True
                for sub in subs:
                    sub.terminate()
            if pub_done and all(sub.poll() is not None for sub in subs):
                break
            r, _, _ = select.select(
                [pub.stdout, *[sub.stdout for sub in subs]], [], [], 1
            )
            for fd in r:
                prefix = "pub" if fd == pub.stdout else "sub"
                line = fd.readline()
                if line:
                    print(prefix, line.decode("utf-8"), end="")
        for sub in subs:
            for line in sub.stdout.readlines():
                print("sub", line.decode("utf-8"), end="")
    finally:
        pub.kill()
        for sub in subs:
            sub.kill()


def usage():
    print("Usage: pub_sub.py [pub|sub|run_test]")
    sys.exit(1)


def main():
    if len(sys.argv) < 2:
        usage()

    if sys.argv[1] == "pub":
        pub()
    elif sys.argv[1] == "sub":
        sub()
    elif sys.argv[1] == "run_test":
        run_test()
    else:
        usage()


if __name__ == "__main__":
    main()
