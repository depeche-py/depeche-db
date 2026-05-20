"""
Migration tests for the meta tables introduced for projector speedups.

The new code adds two tables that older installs don't have:
  * depeche_msgs_<store>_meta — maintained by the message-store INSERT trigger
  * depeche_stream_<stream>_omax — maintained by the projector's _add()

Both are created via metadata.create_all(checkfirst=True). When an existing
deployment is upgraded the existing tables stay in place and the new ones get
created and backfilled. For the msgs side the trigger function also needs to
be (re-)installed with its meta-aware body. These tests pin that behaviour.
"""

import uuid as _uuid

import sqlalchemy as _sa

from depeche_db import MessageStore
from depeche_db._aggregated_stream import AggregatedStream
from depeche_db._storage import Storage
from tests._account_example import (
    Account,
    AccountEvent,
    AccountEventSerializer,
    AccountRepository,
)
from tests.conftest import MyPartitioner

# UUIDs that end in digits, so the test partitioner (which takes the last
# char of account_id as an int) doesn't blow up.
ACCT_A_ID = _uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")
ACCT_B_ID = _uuid.UUID("bbbbbbbb-0000-0000-0000-000000000002")
ACCT_C_ID = _uuid.UUID("cccccccc-0000-0000-0000-000000000003")


# --- helpers --------------------------------------------------------------


def _drop_meta_tables(engine, store_name: str, stream_name: str) -> None:
    """Simulate the pre-meta-table state of an existing install."""
    with engine.begin() as conn:
        conn.execute(
            _sa.text(f'DROP TABLE IF EXISTS "depeche_msgs_{store_name}_meta" CASCADE')
        )
        conn.execute(
            _sa.text(
                f'DROP TABLE IF EXISTS "depeche_stream_{stream_name}_omax" CASCADE'
            )
        )


def _install_pre_meta_trigger_function(
    engine, store_name: str, notification_channel: str
) -> None:
    """
    Re-install the pre-0.13 trigger function body — the version that only
    fires the NOTIFY and doesn't touch the meta table. Used to set up the
    "old install" state.
    """
    trigger_name = f"depeche_storage_new_msg_{store_name}"
    with engine.begin() as conn:
        conn.execute(
            _sa.text(
                f"""
                CREATE OR REPLACE FUNCTION {trigger_name}()
                  RETURNS trigger AS $$
                BEGIN
                  PERFORM pg_notify(
                    '{notification_channel}',
                    json_build_object(
                        'message_id', NEW.message_id,
                        'stream', NEW.stream,
                        'version', NEW.version,
                        'global_position', NEW.global_position
                    )::text);
                  RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """
            )
        )


def _meta_rows(engine, store_name: str):
    with engine.connect() as conn:
        return {
            row.stream: (row.min_global_position, row.max_global_position)
            for row in conn.execute(
                _sa.text(f"SELECT * FROM depeche_msgs_{store_name}_meta")
            )
        }


def _omax_rows(engine, stream_name: str):
    with engine.connect() as conn:
        return {
            row.origin_stream: row.max_aggregated_origin_global_position
            for row in conn.execute(
                _sa.text(f"SELECT * FROM depeche_stream_{stream_name}_omax")
            )
        }


# --- tests ----------------------------------------------------------------


def test_msgs_meta_backfilled_on_upgrade(db_engine, identifier):
    """Existing message rows must seed the meta table when it's first created."""
    store_name = identifier()
    store = MessageStore[AccountEvent](
        name=store_name, engine=db_engine, serializer=AccountEventSerializer()
    )

    acct = Account.register(id=ACCT_A_ID, owner_id=_uuid.uuid4(), number="1")
    acct.credit(100)
    AccountRepository(store).save(acct, expected_version=0)

    # Pre-state: meta exists and has the rows. Drop it to simulate the
    # pre-meta install state.
    assert _meta_rows(db_engine, store_name)
    _drop_meta_tables(db_engine, store_name, stream_name="ignored")
    assert not _table_exists(db_engine, f"depeche_msgs_{store_name}_meta")

    # Re-init the store — Storage.__init__'s metadata.create_all runs the
    # after_create event for the meta table, which backfills it.
    MessageStore[AccountEvent](
        name=store_name, engine=db_engine, serializer=AccountEventSerializer()
    )

    rows = _meta_rows(db_engine, store_name)
    expected_stream = f"account-{acct.id}"
    assert expected_stream in rows
    min_pos, max_pos = rows[expected_stream]
    assert min_pos == 1 and max_pos == 2


def test_msgs_meta_trigger_refreshed_on_upgrade(db_engine, identifier):
    """
    On upgrade, the old (pre-meta) trigger function must be replaced by the
    new one that UPSERTs into the meta table. New writes after re-init must
    keep the meta table current.
    """
    store_name = identifier()
    store = MessageStore[AccountEvent](
        name=store_name, engine=db_engine, serializer=AccountEventSerializer()
    )
    repo = AccountRepository(store)

    acct1 = Account.register(id=ACCT_A_ID, owner_id=_uuid.uuid4(), number="1")
    acct1.credit(100)
    repo.save(acct1, expected_version=0)

    # Simulate pre-meta install: drop meta + revert trigger function body.
    notification_channel = Storage.notification_channel_name(store_name)
    _drop_meta_tables(db_engine, store_name, stream_name="ignored")
    _install_pre_meta_trigger_function(
        db_engine, store_name=store_name, notification_channel=notification_channel
    )

    # Re-init — should backfill meta AND replace the trigger function with
    # the meta-aware body.
    store = MessageStore[AccountEvent](
        name=store_name, engine=db_engine, serializer=AccountEventSerializer()
    )

    # Backfill captured the existing rows.
    pre_upgrade_rows = _meta_rows(db_engine, store_name)
    assert pre_upgrade_rows[f"account-{acct1.id}"] == (1, 2)

    # New writes after upgrade flow through the refreshed trigger and update
    # the meta table.
    acct2 = Account.register(id=ACCT_B_ID, owner_id=_uuid.uuid4(), number="2")
    acct2.credit(100)
    AccountRepository(store).save(acct2, expected_version=0)

    rows = _meta_rows(db_engine, store_name)
    assert f"account-{acct2.id}" in rows, (
        "trigger function must be refreshed on upgrade so new writes "
        "populate the meta table"
    )
    min_pos, max_pos = rows[f"account-{acct2.id}"]
    assert min_pos == 3 and max_pos == 4

    # The pre-existing stream's max also moves forward when more messages
    # are written, proving the GREATEST update path.
    acct1.credit(100)
    AccountRepository(store).save(acct1, expected_version=2)
    rows = _meta_rows(db_engine, store_name)
    assert rows[f"account-{acct1.id}"][1] == 5


def test_omax_backfilled_on_upgrade(
    db_engine, identifier, store_factory, stream_factory
):
    """
    Existing aggregated-stream rows must seed the omax meta table. omax is
    reconciled by the projector on its first run (under the EXCLUSIVE lock),
    not at construction time — so it stays consistent with the aggregated
    stream even when an old projector is still draining during an upgrade.
    """
    store = store_factory()
    stream = stream_factory(store)
    repo = AccountRepository(store)

    acct = Account.register(id=ACCT_A_ID, owner_id=_uuid.uuid4(), number="1")
    acct.credit(100)
    repo.save(acct, expected_version=0)

    # Project the existing messages so the agg stream has rows.
    stream.projector.update_full()
    omax_before = _omax_rows(db_engine, stream.name)
    assert omax_before, "projector should have populated omax already"

    # Simulate the pre-meta install state.
    _drop_meta_tables(db_engine, store_name="ignored", stream_name=stream.name)
    assert not _table_exists(db_engine, f"depeche_stream_{stream.name}_omax")

    # Re-init the AggregatedStream — _setup_schema re-creates the omax table
    # (empty); it is the projector that reconciles it.
    stream = AggregatedStream[AccountEvent](
        name=stream.name,
        store=store,
        partitioner=MyPartitioner(),
        stream_wildcards=["account-%"],
    )
    assert (
        _omax_rows(db_engine, stream.name) == {}
    ), "omax is reconciled by the projector, not at construction time"

    # First projector run reconciles omax from the existing agg rows.
    stream.projector.update_full()
    omax_after = _omax_rows(db_engine, stream.name)
    assert (
        omax_after == omax_before
    ), "omax must be reconciled to match the agg-stream state on upgrade"


def test_full_upgrade_round_trip(db_engine, identifier, store_factory, stream_factory):
    """
    End-to-end: existing install with messages and projected agg rows is
    upgraded by re-init. Both meta tables are populated, the trigger is
    refreshed, and a new write+project cycle works correctly.
    """
    store = store_factory()
    stream = stream_factory(store)
    repo = AccountRepository(store)

    acct = Account.register(id=ACCT_A_ID, owner_id=_uuid.uuid4(), number="1")
    acct.credit(100)
    repo.save(acct, expected_version=0)

    stream.projector.update_full()
    pre_upgrade_agg = _agg_message_count(db_engine, stream.name)
    assert pre_upgrade_agg == 2

    # Simulate full pre-meta install state: drop both meta tables and
    # downgrade the trigger function.
    notification_channel = Storage.notification_channel_name(store._storage.name)
    _drop_meta_tables(
        db_engine, store_name=store._storage.name, stream_name=stream.name
    )
    _install_pre_meta_trigger_function(
        db_engine,
        store_name=store._storage.name,
        notification_channel=notification_channel,
    )

    # Re-init: this is what a deploy does — construct fresh objects against
    # the existing schema.
    store = MessageStore[AccountEvent](
        name=store._storage.name,
        engine=db_engine,
        serializer=AccountEventSerializer(),
    )
    stream = AggregatedStream[AccountEvent](
        name=stream.name,
        store=store,
        partitioner=MyPartitioner(),
        stream_wildcards=["account-%"],
    )

    # The message-store meta table is backfilled at re-init; omax is left to
    # the projector's first run, so it is still empty here.
    assert _meta_rows(db_engine, store._storage.name)[f"account-{acct.id}"] == (1, 2)
    assert _omax_rows(db_engine, stream.name) == {}

    # Write more messages — trigger updates meta — and project them.
    acct.credit(100)
    AccountRepository(store).save(acct, expected_version=2)
    assert _meta_rows(db_engine, store._storage.name)[f"account-{acct.id}"][1] == 3

    result = stream.projector.update_full()
    assert result.n_updated_messages == 1
    assert _agg_message_count(db_engine, stream.name) == 3
    assert _omax_rows(db_engine, stream.name)[f"account-{acct.id}"] == 3


def test_backfill_handles_multiple_streams(
    db_engine, identifier, store_factory, stream_factory
):
    """
    Backfill must compute the correct min / max per stream when many streams
    are present, and produce empty meta tables when the source is empty.
    """
    store = store_factory()
    stream = stream_factory(store)
    repo = AccountRepository(store)

    # 3 streams with different shapes:
    #   A: 2 messages (positions 1..2)
    #   B: 3 messages (positions 3..5)
    #   C: 1 message  (position 6)
    acct_a = Account.register(id=ACCT_A_ID, owner_id=_uuid.uuid4(), number="1")
    acct_a.credit(100)
    repo.save(acct_a, expected_version=0)

    acct_b = Account.register(id=ACCT_B_ID, owner_id=_uuid.uuid4(), number="2")
    acct_b.credit(100)
    acct_b.credit(100)
    repo.save(acct_b, expected_version=0)

    acct_c = Account.register(id=ACCT_C_ID, owner_id=_uuid.uuid4(), number="3")
    repo.save(acct_c, expected_version=0)

    stream.projector.update_full()

    # Simulate the pre-meta state and re-init.
    notification_channel = Storage.notification_channel_name(store._storage.name)
    _drop_meta_tables(
        db_engine, store_name=store._storage.name, stream_name=stream.name
    )
    _install_pre_meta_trigger_function(
        db_engine,
        store_name=store._storage.name,
        notification_channel=notification_channel,
    )

    store = MessageStore[AccountEvent](
        name=store._storage.name,
        engine=db_engine,
        serializer=AccountEventSerializer(),
    )
    stream = AggregatedStream[AccountEvent](
        name=stream.name,
        store=store,
        partitioner=MyPartitioner(),
        stream_wildcards=["account-%"],
    )

    msgs_meta = _meta_rows(db_engine, store._storage.name)
    assert msgs_meta == {
        f"account-{acct_a.id}": (1, 2),
        f"account-{acct_b.id}": (3, 5),
        f"account-{acct_c.id}": (6, 6),
    }

    # omax is reconciled by the projector's first run, not at re-init.
    stream.projector.update_full()
    omax = _omax_rows(db_engine, stream.name)
    assert omax == {
        f"account-{acct_a.id}": 2,
        f"account-{acct_b.id}": 5,
        f"account-{acct_c.id}": 6,
    }


def test_backfill_on_empty_store_is_noop(db_engine, identifier):
    """
    Migrating a store that has tables but no message rows is a clean no-op:
    the backfill SELECT returns nothing, no errors, meta is empty.
    """
    store_name = identifier()
    MessageStore[AccountEvent](
        name=store_name, engine=db_engine, serializer=AccountEventSerializer()
    )

    _drop_meta_tables(db_engine, store_name, stream_name="ignored")
    MessageStore[AccountEvent](
        name=store_name, engine=db_engine, serializer=AccountEventSerializer()
    )

    assert _meta_rows(db_engine, store_name) == {}


def test_re_init_is_idempotent(db_engine, identifier, store_factory, stream_factory):
    """
    Process startup runs metadata.create_all every time. Constructing the
    store + stream multiple times in a row on an already-migrated install
    must not change observable state, even with writes interleaved.
    """
    store = store_factory()
    stream = stream_factory(store)
    repo = AccountRepository(store)

    acct_a = Account.register(id=ACCT_A_ID, owner_id=_uuid.uuid4(), number="1")
    acct_a.credit(100)
    repo.save(acct_a, expected_version=0)
    stream.projector.update_full()

    snapshot_meta = _meta_rows(db_engine, store._storage.name)
    snapshot_omax = _omax_rows(db_engine, stream.name)
    snapshot_agg = _agg_message_count(db_engine, stream.name)

    # Re-init twice — simulates two consecutive process restarts after the
    # initial migration is already in place.
    for _ in range(2):
        store = MessageStore[AccountEvent](
            name=store._storage.name,
            engine=db_engine,
            serializer=AccountEventSerializer(),
        )
        stream = AggregatedStream[AccountEvent](
            name=stream.name,
            store=store,
            partitioner=MyPartitioner(),
            stream_wildcards=["account-%"],
        )
        assert _meta_rows(db_engine, store._storage.name) == snapshot_meta
        assert _omax_rows(db_engine, stream.name) == snapshot_omax
        assert _agg_message_count(db_engine, stream.name) == snapshot_agg

    # Normal operation still works after the redundant inits.
    acct_b = Account.register(id=ACCT_B_ID, owner_id=_uuid.uuid4(), number="2")
    acct_b.credit(100)
    AccountRepository(store).save(acct_b, expected_version=0)
    result = stream.projector.update_full()
    assert result.n_updated_messages == 2  # register + credit


def test_projector_after_upgrade_is_noop_with_no_new_messages(
    db_engine, identifier, store_factory, stream_factory
):
    """
    Right after an upgrade, the projector must see "everything already
    projected" and do nothing — no double-insert, no unique-constraint
    violation. This pins that omax backfill captures the correct max
    (a min/max swap or off-by-one would make the projector try to insert
    rows that already exist).
    """
    store = store_factory()
    stream = stream_factory(store)
    repo = AccountRepository(store)

    acct_a = Account.register(id=ACCT_A_ID, owner_id=_uuid.uuid4(), number="1")
    acct_a.credit(100)
    repo.save(acct_a, expected_version=0)
    acct_b = Account.register(id=ACCT_B_ID, owner_id=_uuid.uuid4(), number="2")
    acct_b.credit(100)
    repo.save(acct_b, expected_version=0)

    stream.projector.update_full()
    pre_count = _agg_message_count(db_engine, stream.name)
    assert pre_count == 4

    # Simulate the pre-meta state and re-init.
    notification_channel = Storage.notification_channel_name(store._storage.name)
    _drop_meta_tables(
        db_engine, store_name=store._storage.name, stream_name=stream.name
    )
    _install_pre_meta_trigger_function(
        db_engine,
        store_name=store._storage.name,
        notification_channel=notification_channel,
    )
    store = MessageStore[AccountEvent](
        name=store._storage.name,
        engine=db_engine,
        serializer=AccountEventSerializer(),
    )
    stream = AggregatedStream[AccountEvent](
        name=stream.name,
        store=store,
        partitioner=MyPartitioner(),
        stream_wildcards=["account-%"],
    )

    # No new messages have been written; projector should pick up nothing.
    result = stream.projector.update_full()
    assert result.n_updated_messages == 0
    assert result.more_messages_available is False
    assert _agg_message_count(db_engine, stream.name) == pre_count


def test_get_migration_ddl_0_14_0_brings_pre_meta_install_to_current_state(
    db_engine, store_factory, stream_factory
):
    """
    Users who manage schema changes out of band run the SQL emitted by
    `python -m depeche_db generate-migration-script ... 0.14 ...` instead of
    relying on `metadata.create_all`. This test pins that the generated DDL
    is sufficient to bring a pre-meta install up to the new schema:
    backfilled meta tables, refreshed trigger, working projector.
    """
    from depeche_db._aggregated_stream import AggregatedStream

    store = store_factory()
    stream = stream_factory(store)
    repo = AccountRepository(store)

    acct = Account.register(id=ACCT_A_ID, owner_id=_uuid.uuid4(), number="1")
    acct.credit(100)
    repo.save(acct, expected_version=0)
    stream.projector.update_full()

    # Drop the meta tables and roll the trigger function back to its
    # pre-meta body. Schema now matches a < 0.14 install.
    _drop_meta_tables(
        db_engine, store_name=store._storage.name, stream_name=stream.name
    )
    _install_pre_meta_trigger_function(
        db_engine,
        store_name=store._storage.name,
        notification_channel=Storage.notification_channel_name(store._storage.name),
    )

    ddl = AggregatedStream.get_migration_ddl_0_14_0(
        aggregated_stream_name=stream.name,
        message_store_name=store._storage.name,
    )
    with db_engine.begin() as conn:
        conn.execute(_sa.text(ddl))

    assert _meta_rows(db_engine, store._storage.name)[f"account-{acct.id}"] == (1, 2)
    assert _omax_rows(db_engine, stream.name)[f"account-{acct.id}"] == 2

    # Re-running the script must be a no-op (idempotency claim in the docstring).
    with db_engine.begin() as conn:
        conn.execute(_sa.text(ddl))
    assert _meta_rows(db_engine, store._storage.name)[f"account-{acct.id}"] == (1, 2)
    assert _omax_rows(db_engine, stream.name)[f"account-{acct.id}"] == 2

    # New writes after the migration flow through the refreshed trigger and
    # are picked up by the projector.
    acct.credit(100)
    AccountRepository(store).save(acct, expected_version=2)
    assert _meta_rows(db_engine, store._storage.name)[f"account-{acct.id}"][1] == 3

    result = stream.projector.update_full()
    assert result.n_updated_messages == 1
    assert _omax_rows(db_engine, stream.name)[f"account-{acct.id}"] == 3


def test_interrupted_backfill_is_retried(db_engine, identifier):
    """
    If a previous migration created the meta table but its backfill did not
    finish (e.g. the process died mid-scan), the next construction must
    retry the backfill — not skip it just because the table now exists.
    """
    store_name = identifier()
    store = MessageStore[AccountEvent](
        name=store_name, engine=db_engine, serializer=AccountEventSerializer()
    )
    acct = Account.register(id=ACCT_A_ID, owner_id=_uuid.uuid4(), number="1")
    acct.credit(100)
    AccountRepository(store).save(acct, expected_version=0)

    # Simulate a crashed migration: the meta table exists but is empty, as
    # if the process died after CREATE TABLE but before the backfill ran.
    with db_engine.begin() as conn:
        conn.execute(_sa.text(f"DELETE FROM depeche_msgs_{store_name}_meta"))
    assert _meta_rows(db_engine, store_name) == {}

    # Re-init: _meta_migration_needed sees an empty meta table over a
    # non-empty store and retries the backfill.
    MessageStore[AccountEvent](
        name=store_name, engine=db_engine, serializer=AccountEventSerializer()
    )
    assert _meta_rows(db_engine, store_name)[f"account-{acct.id}"] == (1, 2)


def test_meta_migration_is_downtime_free(db_engine, store_factory, stream_factory):
    """
    The meta migration swaps the trigger function in a committed transaction
    *before* the backfill runs. A write that lands in that window — to a
    brand-new stream, or to a pre-existing un-projected stream — is captured
    with its true min/max, so no writer downtime is needed.

    This pins the exact interleaving by running the two migration steps by
    hand with writes in between.
    """
    store = store_factory()
    stream = stream_factory(store)
    repo = AccountRepository(store)
    store_name = store._storage.name

    # Pre-existing stream A, written under the current schema but *not*
    # projected — so the projector will later use its meta min as the start
    # position, which is exactly what a wrong min would corrupt.
    acct_a = Account.register(id=ACCT_A_ID, owner_id=_uuid.uuid4(), number="1")
    acct_a.credit(100)
    repo.save(acct_a, expected_version=0)  # A: global positions 1..2

    # Roll the schema back to a pre-0.14 install.
    _drop_meta_tables(db_engine, store_name=store_name, stream_name=stream.name)
    _install_pre_meta_trigger_function(
        db_engine,
        store_name=store_name,
        notification_channel=Storage.notification_channel_name(store_name),
    )

    steps = Storage._meta_migration_steps(store_name)
    assert len(steps) == 2, "expected a trigger-swap step and a backfill step"

    # The meta table must exist before the steps run. _setup_schema creates
    # it; here we create it by hand so we can interleave writes.
    meta_tablename = f"depeche_msgs_{store_name}_meta"
    with db_engine.begin() as conn:
        conn.execute(
            _sa.text(
                f'CREATE TABLE "{meta_tablename}" ('
                "stream VARCHAR(255) PRIMARY KEY, "
                "min_global_position INTEGER NOT NULL, "
                "max_global_position INTEGER NOT NULL)"
            )
        )

    # Step 1: swap the trigger function — committed on its own.
    with db_engine.begin() as conn:
        conn.execute(_sa.DDL(steps[0]))

    # --- the "writer downtime" window: writes land AFTER the trigger swap
    # but BEFORE the backfill, flowing through the new meta-aware trigger.
    # A brand-new stream B...
    acct_b = Account.register(id=ACCT_B_ID, owner_id=_uuid.uuid4(), number="2")
    acct_b.credit(100)
    repo.save(acct_b, expected_version=0)  # B: global positions 3..4
    # ...and a further write to the pre-existing, un-projected stream A. No
    # meta row exists for A yet, so the trigger inserts one with min == 5.
    acct_a.credit(100)
    repo.save(acct_a, expected_version=2)  # A: global position 5

    # Step 2: the backfill.
    with db_engine.begin() as conn:
        conn.execute(_sa.DDL(steps[1]))

    meta = _meta_rows(db_engine, store_name)
    # Stream A: the backfill's LEAST upsert corrects min back down from the
    # trigger's 5 to A's true first position. With ON CONFLICT DO NOTHING
    # this would stay (5, 5) and A would project from 5, skipping 1..2.
    assert meta[f"account-{acct_a.id}"] == (1, 5)
    # Stream B: brand-new, written entirely inside the window.
    assert meta[f"account-{acct_b.id}"] == (3, 4)

    # End to end: re-init and project — every message lands, none skipped.
    store = MessageStore[AccountEvent](
        name=store_name, engine=db_engine, serializer=AccountEventSerializer()
    )
    stream = AggregatedStream[AccountEvent](
        name=stream.name,
        store=store,
        partitioner=MyPartitioner(),
        stream_wildcards=["account-%"],
    )
    stream.projector.update_full()
    assert _agg_message_count(db_engine, stream.name) == 5


def test_projector_repairs_stale_omax(db_engine, store_factory, stream_factory):
    """
    A stale-low omax — as left behind when a pre-0.14 projector advances the
    aggregated stream during a rolling upgrade without maintaining omax —
    must be repaired by the projector on its next run, under the EXCLUSIVE
    lock, rather than making it re-project rows and hit the message_id PK.
    """
    store = store_factory()
    stream = stream_factory(store)
    repo = AccountRepository(store)

    acct = Account.register(id=ACCT_A_ID, owner_id=_uuid.uuid4(), number="1")
    acct.credit(100)
    acct.credit(100)
    repo.save(acct, expected_version=0)  # global positions 1..3

    stream.projector.update_full()
    pre_count = _agg_message_count(db_engine, stream.name)
    assert pre_count == 3
    assert _omax_rows(db_engine, stream.name)[f"account-{acct.id}"] == 3

    # Corrupt omax to a stale-low value, simulating an older projector that
    # advanced the aggregated stream without knowing about omax.
    with db_engine.begin() as conn:
        conn.execute(
            _sa.text(
                f"UPDATE depeche_stream_{stream.name}_omax "
                "SET max_aggregated_origin_global_position = 1"
            )
        )

    # A fresh projector instance (i.e. a new process) reconciles omax on its
    # first run. Without the reconcile it would re-select global positions
    # 2..3 and raise a primary-key violation.
    fresh_stream = AggregatedStream[AccountEvent](
        name=stream.name,
        store=store,
        partitioner=MyPartitioner(),
        stream_wildcards=["account-%"],
    )
    result = fresh_stream.projector.update_full()

    assert result.n_updated_messages == 0
    assert _agg_message_count(db_engine, fresh_stream.name) == pre_count
    assert _omax_rows(db_engine, fresh_stream.name)[f"account-{acct.id}"] == 3


# --- internal helpers used by the tests above -----------------------------


def _table_exists(engine, name: str) -> bool:
    with engine.connect() as conn:
        return bool(
            conn.execute(
                _sa.text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = :name"
                ),
                {"name": name},
            ).fetchone()
        )


def _agg_message_count(engine, stream_name: str) -> int:
    with engine.connect() as conn:
        count: int = conn.execute(
            _sa.text(f"SELECT COUNT(*) FROM depeche_stream_{stream_name}")
        ).scalar()
        return count
