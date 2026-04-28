"""
Bulk-seed the latency_breakdown store with N streams x M messages, then catch
up the aggregated stream. Use before running examples/latency_breakdown.py to
benchmark at realistic data volumes (the empty-DB case doesn't exercise the
expensive GROUP BY queries inside _select_origin_streams).

What it does:
  1. Drops + recreates the message-store and aggregated-stream tables for the
     names used by latency_breakdown.py (so the two scripts share state).
  2. Bulk-inserts messages directly via SQL (bypasses MessageStore.write so we
     don't pay the per-row PL/pgSQL function call cost). Triggers are disabled
     during the bulk load so we don't fire 100k pg_notify calls into the void.
  3. Runs the projector to populate the aggregated stream from the seeded
     messages, so the agg-side GROUP BY is also slow.

Run:
    python examples/seed_db.py [N_STREAMS] [N_MESSAGES] [HOURS_SPREAD]

Defaults: 5000 streams, 100000 messages, 1 hour timestamp spread (so all rows
fall inside the default 6h gap-lookback window — worst case for the
GROUP BYs we want to optimize).
"""

import logging
import random
import sys
import time
from datetime import datetime, timedelta
from uuid import UUID, uuid4

import pydantic
import sqlalchemy as sa
from sqlalchemy import create_engine

from depeche_db import MessageStore, StoredMessage
from depeche_db.tools import PydanticMessageSerializer

# Must match the names used by examples/latency_breakdown.py
DB_DSN = "postgresql+psycopg://depeche:depeche@localhost:4888/depeche_demo"
STORE_NAME = "latency_store"
STREAM_NAME = "latency_stream"
SUB_NAME = "latency_sub"


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


def drop_existing(engine: sa.engine.Engine):
    """Drop any existing depeche objects for our store/stream so we reseed clean."""
    tables = [
        # Subscription state uses the *subscription* name, not the stream name.
        f"depeche_subscriptions_{SUB_NAME}",
        f"depeche_stream_{STREAM_NAME}_maxpos",
        f"depeche_stream_{STREAM_NAME}_omax",
        f"depeche_stream_{STREAM_NAME}",
        f"depeche_msgs_{STORE_NAME}_meta",
        f"depeche_msgs_{STORE_NAME}",
    ]
    with engine.begin() as conn:
        # CASCADE drops the per-table triggers and trigger functions too.
        for tbl in tables:
            conn.execute(sa.text(f'DROP TABLE IF EXISTS "{tbl}" CASCADE'))
        conn.execute(
            sa.text(f"DROP SEQUENCE IF EXISTS depeche_msgs_{STORE_NAME}_global_seq")
        )


def build_store_and_stream(engine):
    """Recreates the message store + aggregated stream tables (matches latency_breakdown.py)."""
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


def bulk_insert_messages(
    engine: sa.engine.Engine,
    n_streams: int,
    n_messages: int,
    hours_spread: float,
):
    """
    Insert n_messages rows directly into the message-store table, distributed
    across n_streams streams, with timestamps spread over hours_spread hours
    ending at "now".
    """
    msgs_table = f"depeche_msgs_{STORE_NAME}"
    trigger_name = f"depeche_storage_new_msg_{STORE_NAME}"

    # Pre-pick a stream name per message and per-stream version counters so
    # the (stream, version) unique constraint is satisfied.
    stream_names = [f"latency-src-{i}" for i in range(n_streams)]
    versions = [0] * n_streams
    end_time = datetime.utcnow()
    spread_seconds = hours_spread * 3600

    rows = []
    for _ in range(n_messages):
        s_idx = random.randrange(n_streams)
        versions[s_idx] += 1
        # Random timestamp in [end - spread, end]; spread over evenly-ish.
        offset_s = random.uniform(0, spread_seconds)
        added_at = end_time - timedelta(seconds=spread_seconds - offset_s)
        msg_id = uuid4()
        content = random.randint(1, 1000)
        body = {
            "content": content,
            "message_id": str(msg_id),
            "sent_at": added_at.isoformat(),
            # Required by PydanticMessageSerializer.deserialize().
            "__typename__": "MyMessage",
        }
        rows.append(
            {
                "message_id": msg_id,
                "added_at": added_at,
                "stream": stream_names[s_idx],
                "version": versions[s_idx],
                "message": body,
            }
        )

    seq_name = f"depeche_msgs_{STORE_NAME}_global_seq"
    insert_sql = sa.text(
        f"""
        INSERT INTO {msgs_table} (message_id, global_position, added_at, stream, version, message)
        VALUES (
            :message_id,
            nextval('{seq_name}'),
            :added_at,
            :stream,
            :version,
            CAST(:message AS jsonb)
        )
        """
    )

    # Disable the NOTIFY trigger during bulk load — nobody's listening and
    # pg_notify per-row would be wasteful. Each ALTER TABLE / INSERT batch is
    # committed independently so a failing batch doesn't strand the trigger
    # in the disabled state.
    with engine.begin() as conn:
        conn.execute(
            sa.text(f"ALTER TABLE {msgs_table} DISABLE TRIGGER {trigger_name}")
        )

    import json as _json

    try:
        chunk = 5000
        t0 = time.time()
        for start in range(0, len(rows), chunk):
            batch = rows[start : start + chunk]
            batch_for_sql = [{**r, "message": _json.dumps(r["message"])} for r in batch]
            with engine.begin() as conn:
                conn.execute(insert_sql, batch_for_sql)
            logging.info(
                "  inserted %d / %d (%.0f msg/s)",
                start + len(batch),
                len(rows),
                (start + len(batch)) / max(time.time() - t0, 1e-6),
            )
    finally:
        with engine.begin() as conn:
            conn.execute(
                sa.text(f"ALTER TABLE {msgs_table} ENABLE TRIGGER {trigger_name}")
            )

    # The trigger was disabled during the bulk load, so the per-stream meta
    # table is empty. Backfill it in one pass before the projector runs.
    meta_table = f"depeche_msgs_{STORE_NAME}_meta"
    logging.info("  backfilling %s", meta_table)
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                f"""
                INSERT INTO {meta_table}
                    (stream, min_global_position, max_global_position)
                SELECT stream, MIN(global_position), MAX(global_position)
                FROM {msgs_table}
                GROUP BY stream
                ON CONFLICT (stream) DO UPDATE
                    SET min_global_position = LEAST(
                        {meta_table}.min_global_position,
                        EXCLUDED.min_global_position
                    ),
                    max_global_position = GREATEST(
                        {meta_table}.max_global_position,
                        EXCLUDED.max_global_position
                    );
                """
            )
        )


def project_until_caught_up(stream):
    """Run update_full repeatedly until no more messages are added."""
    total = 0
    iterations = 0
    while True:
        iterations += 1
        result = stream.projector.update_full()
        total += result.n_updated_messages
        if not result.more_messages_available:
            break
    logging.info("  projected %d messages in %d update_full passes", total, iterations)


def main():
    n_streams = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    n_messages = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000
    hours_spread = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
    )

    logging.info(
        "Seeding: %d streams, %d messages, %.1f hour spread (DSN=%s)",
        n_streams,
        n_messages,
        hours_spread,
        DB_DSN,
    )
    # Use a sizable pool so the projector + main-thread engines don't fight.
    engine = create_engine(DB_DSN, pool_size=10)

    logging.info("Step 1/3: dropping any existing depeche objects for this store")
    drop_existing(engine)

    logging.info("Step 2/3: creating tables and bulk-inserting messages")
    _, stream = build_store_and_stream(engine)
    t0 = time.time()
    bulk_insert_messages(engine, n_streams, n_messages, hours_spread)
    logging.info("  bulk insert took %.1fs", time.time() - t0)

    logging.info("Step 3/3: running projector to populate aggregated stream")
    t0 = time.time()
    project_until_caught_up(stream)
    logging.info("  projection took %.1fs", time.time() - t0)

    logging.info(
        "Done. examples/latency_breakdown.py can now run against the seeded DB."
    )


if __name__ == "__main__":
    main()
