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
    Existing aggregated-stream rows must seed the omax meta table when it's
    first created during an upgrade.
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

    # Re-init the AggregatedStream — its meta_create event re-runs and
    # populates omax from the existing agg rows.
    AggregatedStream[AccountEvent](
        name=stream.name,
        store=store,
        partitioner=MyPartitioner(),
        stream_wildcards=["account-%"],
    )

    omax_after = _omax_rows(db_engine, stream.name)
    assert (
        omax_after == omax_before
    ), "omax must be backfilled to match the agg-stream state on upgrade"


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

    # Both meta tables are backfilled after re-init.
    assert _meta_rows(db_engine, store._storage.name)[f"account-{acct.id}"] == (1, 2)
    assert _omax_rows(db_engine, stream.name)[f"account-{acct.id}"] == 2

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
