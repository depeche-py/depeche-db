"""
Measure per-stage latency for the pipeline:

    publisher -> message store -> aggregated stream -> subscription handler

Each message passes through four measurable checkpoints:

  t_pub_start  - just before MessageStore.write()
  t_pub_end    - just after MessageStore.write() returned
  t_msg_notify - LISTEN'er received the NOTIFY fired by the message-store
                 INSERT trigger (i.e. the row is committed and visible)
  t_agg_notify - LISTEN'er received the NOTIFY fired by the aggregated-stream
                 INSERT trigger (projector has inserted the row)
  t_handler    - subscription handler was invoked with the message

From those we derive:

  write            = t_pub_end    - t_pub_start    (client-side write call)
  notify_delivery  = t_msg_notify - t_pub_end      (pg NOTIFY -> listener)
  projection       = t_agg_notify - t_msg_notify   (projector pickup + insert)
  sub_dispatch     = t_handler    - t_agg_notify   (NOTIFY -> batch read -> hydrate -> handler)
  end_to_end       = t_handler    - t_pub_start

Run:
    python examples/latency_breakdown.py [N_MESSAGES] [PUB_INTERVAL_MS]

Defaults: 200 messages, 20ms between publishes.
"""

import logging
import random
import statistics
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List
from uuid import UUID, uuid4

import pydantic
from sqlalchemy import create_engine

from depeche_db import (
    MessageStore,
    MessageHandlerRegister,
    StartAtNextMessage,
    StoredMessage,
    SubscriptionMessage,
)
from depeche_db._subscription import AckStrategy
from depeche_db.experimental.threaded_executor import ThreadedExecutor
from depeche_db.tools import PydanticMessageSerializer
from depeche_db.tools.pg_notification_listener import PgNotificationListener

DB_DSN = "postgresql+psycopg://depeche:depeche@localhost:4888/depeche_demo"

STORE_NAME = "latency_store"
STREAM_NAME = "latency_stream"
SUB_NAME = "latency_sub"

MSG_CHANNEL = f"depeche_{STORE_NAME}_messages"
AGG_CHANNEL = f"depeche_{STREAM_NAME}_messages"


class MyMessage(pydantic.BaseModel):
    content: int
    message_id: UUID = pydantic.Field(default_factory=uuid4)
    sent_at: datetime = pydantic.Field(default_factory=datetime.utcnow)

    def get_message_id(self) -> UUID:
        return self.message_id

    def get_message_time(self) -> datetime:
        return self.sent_at


class NumMessagePartitioner:
    def get_partition(self, message: StoredMessage[MyMessage]) -> int:
        return message.message.content % 10


# --- Timing buckets, keyed by message_id ---------------------------------
t_pub_start: Dict[UUID, float] = {}
t_pub_end: Dict[UUID, float] = {}
t_msg_notify: Dict[UUID, float] = {}
t_agg_notify: Dict[UUID, float] = {}
t_handler: Dict[UUID, float] = {}


def build_store_and_stream(engine):
    message_store = MessageStore[MyMessage](
        name=STORE_NAME,
        engine=engine,
        serializer=PydanticMessageSerializer(MyMessage),
    )
    stream = message_store.aggregated_stream(
        name=STREAM_NAME,
        partitioner=NumMessagePartitioner(),
        stream_wildcards=["latency-src-%"],
    )
    return message_store, stream


def run_notification_listener(
    channel: str, bucket: Dict[UUID, float], ready: threading.Event
) -> PgNotificationListener:
    listener = PgNotificationListener(dsn=DB_DSN, channels=[channel])
    listener.start()

    def consume():
        ready.set()
        for note in listener.messages():
            mid = note.payload.get("message_id")
            if mid is None:
                continue
            try:
                uid = UUID(mid)
            except (TypeError, ValueError):
                continue
            # First-write wins; NOTIFY is fired once per row insert.
            bucket.setdefault(uid, time.time())

    threading.Thread(target=consume, daemon=True).start()
    return listener


def pct(values: List[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def summarize(name: str, values: List[float]) -> str:
    if not values:
        return f"{name:<18} (no samples)"
    ms = [v * 1000 for v in values]
    return (
        f"{name:<18} n={len(ms):4d}  "
        f"min={min(ms):7.2f}  "
        f"p50={statistics.median(ms):7.2f}  "
        f"p95={pct(ms, 95):7.2f}  "
        f"p99={pct(ms, 99):7.2f}  "
        f"max={max(ms):7.2f}  "
        f"mean={statistics.mean(ms):7.2f} ms"
    )


def main():
    n_messages = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    pub_interval = (float(sys.argv[2]) / 1000.0) if len(sys.argv) > 2 else 0.02

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
    )

    engine = create_engine(DB_DSN, pool_size=20)
    message_store, stream = build_store_and_stream(engine)

    # Subscription + handler that just records the invocation time
    handlers = MessageHandlerRegister[MyMessage]()

    @handlers.register
    def record(message: SubscriptionMessage[MyMessage]):
        t_handler[message.stored_message.message.message_id] = time.time()

    subscription = stream.subscription(
        name=SUB_NAME,
        handlers=handlers,
        start_point=StartAtNextMessage(),
        # SINGLE ack keeps the subscription crisp & realistic; switch to
        # BATCHED here if you want to see its effect on dispatch latency.
        ack_strategy=AckStrategy.SINGLE,
    )

    # Turn on the projector's internal timing so we can see where time goes
    # inside update_full / _update_batch / _select_origin_streams / _add.
    stream.projector.timings.enabled = True

    # --- Spin up the listener threads before anything writes --------------
    msg_listener_ready = threading.Event()
    agg_listener_ready = threading.Event()
    msg_listener = run_notification_listener(
        MSG_CHANNEL, t_msg_notify, msg_listener_ready
    )
    agg_listener = run_notification_listener(
        AGG_CHANNEL, t_agg_notify, agg_listener_ready
    )
    msg_listener_ready.wait(timeout=5)
    agg_listener_ready.wait(timeout=5)

    # --- Run the projector + subscription in a background thread ---------
    executor = ThreadedExecutor(
        db_dsn=DB_DSN, disable_signals=True, stimulation_interval=0.5
    )
    executor.register(stream.projector)
    executor.register(subscription.runner)

    exec_thread = threading.Thread(target=executor.run, daemon=True)
    exec_thread.start()

    # Give the executor time to register LISTEN and finish first stimulation
    time.sleep(1.5)

    # --- Publish ----------------------------------------------------------
    logging.info(
        "Publishing %d messages (interval=%.0fms)", n_messages, pub_interval * 1000
    )
    pub_started = time.time()
    streams = [f"latency-src-{n}" for n in range(200)]
    for i in range(n_messages):
        target_stream = random.choice(streams)
        msg = MyMessage(content=i)
        mid = msg.get_message_id()
        t_pub_start[mid] = time.time()
        message_store.write(stream=target_stream, message=msg)
        t_pub_end[mid] = time.time()
        if pub_interval > 0:
            time.sleep(pub_interval)
    pub_duration = time.time() - pub_started
    logging.info(
        "Publishing done in %.2fs (%.0f msg/s nominal)",
        pub_duration,
        n_messages / pub_duration if pub_duration else 0,
    )

    # --- Wait for handlers to drain --------------------------------------
    deadline = time.time() + 15
    while time.time() < deadline:
        if len(t_handler) >= n_messages:
            break
        time.sleep(0.1)
    else:
        logging.warning(
            "Timed out waiting for all messages to be handled: got %d/%d",
            len(t_handler),
            n_messages,
        )

    # Shut down background workers
    try:
        executor._stop()
    except Exception:
        pass
    msg_listener.stop()
    agg_listener.stop()

    # --- Compute stage latencies -----------------------------------------
    write_d: List[float] = []
    notify_d: List[float] = []
    projection_d: List[float] = []
    dispatch_d: List[float] = []
    e2e_d: List[float] = []
    missing_msg_notify = 0
    missing_agg_notify = 0
    missing_handler = 0

    for mid, start in t_pub_start.items():
        end = t_pub_end.get(mid)
        mnotify = t_msg_notify.get(mid)
        anotify = t_agg_notify.get(mid)
        hdlr = t_handler.get(mid)

        if end is not None:
            write_d.append(end - start)
        if end is not None and mnotify is not None:
            notify_d.append(max(0.0, mnotify - end))
        else:
            missing_msg_notify += 1
        if mnotify is not None and anotify is not None:
            projection_d.append(max(0.0, anotify - mnotify))
        else:
            missing_agg_notify += 1
        if anotify is not None and hdlr is not None:
            dispatch_d.append(max(0.0, hdlr - anotify))
        else:
            missing_handler += 1
        if hdlr is not None:
            e2e_d.append(hdlr - start)

    print()
    print("=" * 96)
    print(f"Latency breakdown over {n_messages} messages (values in ms)")
    print("=" * 96)
    print(summarize("write()", write_d))
    print(summarize("msg NOTIFY", notify_d))
    print(summarize("projection", projection_d))
    print(summarize("sub dispatch", dispatch_d))
    print(summarize("end-to-end", e2e_d))
    print("-" * 96)
    print(
        f"missing msg-notify:{missing_msg_notify}  "
        f"missing agg-notify:{missing_agg_notify}  "
        f"missing handler:{missing_handler}"
    )
    print("=" * 96)
    print()
    print("Legend:")
    print(
        "  write        = MessageStore.write() wall-clock (client + DB insert + commit)"
    )
    print(
        "  msg NOTIFY   = pg_notify from message-store trigger -> listener receives it"
    )
    print(
        "  projection   = msg-notify -> aggregated-stream NOTIFY (projector runs & inserts)"
    )
    print(
        "  sub dispatch = agg-notify -> handler invoked (executor + lock + batch read + hydrate)"
    )
    print("  end-to-end   = write()-start -> handler invoked")

    # --- Projector internal breakdown ------------------------------------
    samples = dict(stream.projector.timings.samples)
    if samples:
        # Print in a sensible order: outer scopes first, then their sub-stages.
        order = [
            "update_full",
            "get_global_position",
            "lock_table",
            "check_maxpos_table",
            "update_batch",
            "select_origin_streams",
            "select.head",
            "select.lookback_estimate",
            "select.aggregated_positions",
            "select.origin_positions",
            "select.calculate_cached",
            "select.calculate_live",
            "read_messages",
            "add",
            "add.get_maxpos",
            "add.deserialize_and_partition",
            "add.insert_rows",
            "add.update_maxpos",
            "commit",
        ]
        ordered = [k for k in order if k in samples] + [
            k for k in samples if k not in order
        ]
        total_update_full = sum(samples.get("update_full", [])) or 0.0

        print()
        print("=" * 96)
        print("Projector internal breakdown (values in ms)")
        print("=" * 96)
        for key in ordered:
            values = samples[key]
            if not values:
                continue
            ms = [v * 1000 for v in values]
            total_ms = sum(ms)
            pct_of_full = (
                (total_ms / (total_update_full * 1000) * 100)
                if total_update_full > 0
                else float("nan")
            )
            print(
                f"{key:<32} n={len(ms):4d}  "
                f"sum={total_ms:8.1f}  "
                f"mean={statistics.mean(ms):7.2f}  "
                f"p50={statistics.median(ms):7.2f}  "
                f"p95={pct(ms, 95):7.2f}  "
                f"max={max(ms):7.2f}  "
                f"({pct_of_full:5.1f}% of update_full)"
            )
        print("=" * 96)
        print(
            "Note: update_full is the outer scope; its sub-stages' percentages\n"
            "show where projector wall-time is spent. 'add' wraps the three\n"
            "add.* entries; 'select_origin_streams' wraps the select.* entries."
        )


if __name__ == "__main__":
    main()
