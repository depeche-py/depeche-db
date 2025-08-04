import os
import random
import sys
import time
from datetime import date, datetime, timedelta, timezone
import pytz
from uuid import UUID, uuid4

import pydantic
from sqlalchemy import create_engine

from depeche_db import (
    AggregatedStream,
    Executor,
    MessageStore,
    StoredMessage,
    Subscription,
    SubscriptionMessage,
    SubscriptionMessageHandler,
    SubscriptionRunner,
    MessageHandlerRegister,
)
from depeche_db._subscription import AckStrategy, StartAtPointInTime
from depeche_db.tools import (
    DbLockProvider,
    DbSubscriptionStateProvider,
    PydanticMessageSerializer,
)

DB_DSN = "postgresql+psycopg://depeche:depeche@localhost:4888/depeche_demo"
db_engine = create_engine(DB_DSN)
original_connect = db_engine.connect

CONNECTIONS = 0


def connect(*args, **kwargs):
    global CONNECTIONS
    CONNECTIONS += 1
    return original_connect(*args, **kwargs)


db_engine.connect = connect  # type: ignore


class MyMessage(pydantic.BaseModel):
    content: int
    message_id: UUID = pydantic.Field(default_factory=uuid4)
    sent_at: datetime = pydantic.Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

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
HANDLER_DELAY = 0.001
handlers = MessageHandlerRegister[MyMessage]()


@handlers.register
def handle_event_a(message: SubscriptionMessage[MyMessage]):
    global HANDLED
    HANDLED += 1
    real_message = message.stored_message.message
    # print(
    #    f"Got message #{real_message.content} at {message.partition}:{message.position}"
    # )
    time.sleep(HANDLER_DELAY)


subscription = stream.subscription(
    name="example_pub_sub",
    handlers=handlers,
    batch_size=100,
    ack_strategy=AckStrategy.BATCHED,
    start_point=StartAtPointInTime(datetime.now(timezone.utc) - timedelta(days=1)),
)


def pub():
    start = time.time()
    n = 0
    while time.time() - start < 30:
        n += 1
        stream = random.choice(["aggregate-me-1", "aggregate-me-2"])
        message_store.write(
            stream=stream,
            message=MyMessage(content=random.randint(1, 100)),
        )
    duration = time.time() - start
    print(f"Publisher sent {n / duration:.2f} msg/s (Duration: {duration:.2f}s)")


STIMULATION_INTERVAL = 5


def projector():
    executor = Executor(db_dsn=DB_DSN, stimulation_interval=STIMULATION_INTERVAL)
    executor.register(stream.projector)
    executor.run()


def sub():
    time.sleep(random.random())
    executor = Executor(db_dsn=DB_DSN, stimulation_interval=STIMULATION_INTERVAL)
    executor.register(subscription.runner)
    start = time.time()
    executor.run()
    duration = time.time() - start
    min_run_time = HANDLER_DELAY * HANDLED
    overhead = duration - min_run_time
    print(
        f"Subscriber handled {HANDLED / duration:.2f} mgs/s "
        f"(Time overhead: {overhead:.2f}s ({overhead / duration * 100:.1f}% {overhead / HANDLED * 1000:.2f}ms per message)) "
        f"(Used {CONNECTIONS / HANDLED:.2f} connections per message)"
    )


PUBLISHER_COUNT = 2
SUBSCRIBER_COUNT = 1


def run_test():
    import subprocess
    import select

    print("Starting publisher and subscribers...")
    print(
        f"{STIMULATION_INTERVAL=} {subscription.runner._batch_size=}, {subscription.runner.__class__.__name__}"
    )
    pubs = [
        subprocess.Popen(
            ["python", "-u", "-m", "examples.pub_sub_performance", "pub"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        for _ in range(PUBLISHER_COUNT)
    ]
    pub = pubs[0]
    projector = subprocess.Popen(
        ["python", "-u", "-m", "examples.pub_sub_performance", "projector"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    subs = [
        subprocess.Popen(
            [
                "python",
                "-u",
                "-m",
                "examples.pub_sub_performance",
                "sub",
                # "sub_profile" if i == 0 else "sub",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        for i in range(SUBSCRIBER_COUNT)
    ]
    pub_done = False

    try:
        while True:
            if not pub_done and pub.poll() is not None:
                pub_done = True
                for sub in subs:
                    sub.terminate()
                for pub1 in pubs:
                    pub1.terminate()
            if (
                pub_done
                and all(sub.poll() is not None for sub in subs)
                and all(pub1.poll() is not None for pub1 in pubs)
            ):
                break
            r, _, _ = select.select(
                [*[pub1.stdout for pub1 in pubs], *[sub.stdout for sub in subs]],
                [],
                [],
                1,
            )
            for fd in r:
                prefix = "pub" if any(fd == pub1.stdout for pub1 in pubs) else "sub"
                line = fd.readline()
                if line:
                    print(prefix, line.decode("utf-8"), end="")

        for sub in subs:
            for line in sub.stdout.readlines():
                print("sub", line.decode("utf-8"), end="")
        projector.terminate()
        for line in projector.stdout.readlines():
            print("proj", line.decode("utf-8"), end="")
    finally:
        for pub1 in pubs:
            pub1.kill()
        projector.kill()
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
    elif sys.argv[1] == "sub_profile":
        import yappi

        yappi.start()
        sub()
        yappi.stop()
        yappi.get_func_stats().save("yappi_profile.prof", "callgrind")
    elif sys.argv[1] == "projector":
        projector()
    elif sys.argv[1] == "run_test":
        run_test()
    else:
        usage()


if __name__ == "__main__":
    main()
