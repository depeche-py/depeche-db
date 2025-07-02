import sqlalchemy as _sa
import os
import random
import sys
import time
from datetime import date, datetime, timedelta
from typing import Callable
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
from depeche_db._interfaces import FixedTimeBudget, RunOnNotificationResult, TimeBudget
from depeche_db._subscription import AckStrategy, StartAtPointInTime
from depeche_db.tools import (
    DbLockProvider,
    DbSubscriptionStateProvider,
    PydanticMessageSerializer,
)
from depeche_db.tools.immem_subscription import InMemorySubscriptionState


def measure_timing(name: str, fn: Callable) -> Callable:
    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            return fn(*args, **kwargs)
        finally:
            duration = time.time() - start
            print(f"{name}:{duration:.6f}")

    return wrapper


DB_DSN = "postgresql+psycopg://depeche:depeche@localhost:4888/depeche_demo"
db_engine = create_engine(
    DB_DSN,
    pool_size=10,
    max_overflow=20,
    echo=False,
    # Prevents degradation of performance due to prepare statements
    connect_args={"prepare_threshold": None},
)


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
    sent_at: datetime = pydantic.Field(default_factory=datetime.utcnow)

    def get_message_id(self) -> UUID:
        return self.message_id

    def get_message_time(self) -> datetime:
        return self.sent_at


message_store = MessageStore[MyMessage](
    name="ex_perf",
    engine=db_engine,
    serializer=PydanticMessageSerializer(MyMessage),
)


class NumMessagePartitioner:
    def get_partition(self, message: StoredMessage[MyMessage]) -> int:
        return message.message.content % 100


stream: AggregatedStream = message_store.aggregated_stream(
    name="ex_perf07",
    partitioner=NumMessagePartitioner(),
    stream_wildcards=["aggregate-me-%"],
    update_batch_size=100,
)

HANDLED = 0
HANDLER_DELAY = 0.02
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


subscription: Subscription = stream.subscription(
    name="ex_perf",
    handlers=handlers,
    batch_size=1,
    # ack_strategy=AckStrategy.BATCHED,
    start_point=StartAtPointInTime(
        datetime.utcnow().replace(tzinfo=pytz.UTC) - timedelta(days=1)
    ),
    state_provider=InMemorySubscriptionState(),
)


def pub():
    start = time.time()
    n = 0
    while n < 50000:
        n += 1
        message_store.write(
            stream=f"aggregate-me-{random.randint(1, 500)}",
            message=MyMessage(content=random.randint(1, 1000)),
        )
    duration = time.time() - start
    print(f"Publisher sent {n / duration:.2f} msg/s (Duration: {duration:.2f}s)")


def project():
    stream.projector._update_batch = measure_timing(
        "update_batch", stream.projector._update_batch
    )
    # stream.projector._select_origin_streams_naive = measure_timing(
    #    "_select_origin_streams_naive", stream.projector._select_origin_streams_naive
    # )
    # stream.projector.get_aggregate_stream_positions = measure_timing(
    #    "get_aggregate_stream_positions",
    #    stream.projector.get_aggregate_stream_positions,
    # )
    # stream.projector.get_origin_stream_positions = measure_timing(
    #    "get_origin_stream_positions",
    #    stream.projector.get_origin_stream_positions,
    # )

    start = time.time()
    while (
        stream.projector.run(FixedTimeBudget(seconds=0.5))
        == RunOnNotificationResult.WORK_REMAINING
    ):
        pass
    duration = time.time() - start
    print("Projector finished in {:.2f}s".format(duration))


def sub():
    start = time.time()
    subscription.runner.run(FixedTimeBudget(30))
    duration = time.time() - start
    min_run_time = HANDLER_DELAY * HANDLED
    overhead = duration - min_run_time
    print(
        f"Subscriber handled {HANDLED / duration:.2f} mgs/s "
        f"(Time overhead: {overhead:.2f}s ({overhead / duration * 100:.1f}% {overhead / HANDLED * 1000:.2f}ms per message)) "
        f"(Used {CONNECTIONS / HANDLED:.2f} connections per message)"
    )


def usage():
    print("Usage: pub_sub.py [pub|sub|run_test]")
    sys.exit(1)


def main():
    if len(sys.argv) < 2:
        usage()

    if sys.argv[1] == "pub":
        pub()
    elif sys.argv[1] == "project":
        project()
    elif sys.argv[1] == "sub":
        sub()
    else:
        usage()


if __name__ == "__main__":
    main()
