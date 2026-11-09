import logging as _logging
import uuid as _uuid
from typing import Dict, Iterable, Iterator, Optional

import sqlalchemy as _sa
from sqlalchemy_utils import UUIDType as _UUIDType

from .._partition_assignment import compute_assignment
from .db_lock_provider import DbLockProvider

LOGGER = _logging.getLogger("depeche_db.partition_assignment")


class DbPartitionAssignmentProvider:
    """
    PostgreSQL-backed implementation of
    [PartitionAssignmentProvider][depeche_db.PartitionAssignmentProvider].

    Maintains two tables per subscription:

    - ``depeche_subscription_instances_{name}`` — one row per live instance,
      with a ``last_heartbeat_at`` timestamp used for TTL-based eviction.
    - ``depeche_subscription_assignments_{name}`` — one row per assigned
      partition, with a monotonic ``generation`` fencing token and an
      ``instance_id`` foreign key.

    Rebalancing is leader-elected via a single advisory lock
    (``rebalance-{name}``); only one instance actually runs the algorithm per
    cycle. All other calls return quickly. The leader reaps stale instances
    (via the TTL), reads materialized partitions from the stream's
    ``_maxpos`` table, and computes a sticky fair assignment via
    [compute_assignment][depeche_db.compute_assignment].
    """

    def __init__(
        self,
        name: str,
        engine: _sa.engine.Engine,
        instance_ttl_seconds: float = 20.0,
    ):
        assert name.isidentifier(), "Name must be a valid identifier"
        self.name = name
        self._engine = engine
        self._instance_ttl_seconds = instance_ttl_seconds

        instances_name = f"depeche_subinst_{name}"
        assignments_name = f"depeche_subasgn_{name}"
        rebalance_name = f"depeche_subrebal_{name}"
        # Postgres max identifier length is 63; also allow room for FK names.
        for tn in (instances_name, assignments_name, rebalance_name):
            assert (
                len(tn) <= 63
            ), f"Subscription name '{name}' produces table {tn!r} longer than 63 chars"

        self.metadata = _sa.MetaData()
        self.instances_table = _sa.Table(
            instances_name,
            self.metadata,
            _sa.Column("instance_id", _UUIDType(), primary_key=True),
            _sa.Column("subscription_name", _sa.String, nullable=False),
            _sa.Column(
                "registered_at",
                _sa.DateTime(timezone=True),
                nullable=False,
                server_default=_sa.func.now(),
            ),
            _sa.Column(
                "last_heartbeat_at",
                _sa.DateTime(timezone=True),
                nullable=False,
                server_default=_sa.func.now(),
                index=True,
            ),
            _sa.Column("host", _sa.String, nullable=True),
            _sa.Column("pid", _sa.Integer, nullable=True),
            _sa.Column("label", _sa.String, nullable=True),
        )
        self.assignments_table = _sa.Table(
            assignments_name,
            self.metadata,
            _sa.Column("subscription_name", _sa.String, nullable=False),
            _sa.Column("partition", _sa.Integer, primary_key=True, autoincrement=False),
            _sa.Column(
                "instance_id",
                _UUIDType(),
                _sa.ForeignKey(
                    f"{instances_name}.instance_id",
                    ondelete="CASCADE",
                ),
                nullable=False,
                index=True,
            ),
            _sa.Column("generation", _sa.BigInteger, nullable=False),
            _sa.Column(
                "assigned_at",
                _sa.DateTime(timezone=True),
                nullable=False,
                server_default=_sa.func.now(),
            ),
        )
        self.rebalance_table = _sa.Table(
            rebalance_name,
            self.metadata,
            _sa.Column("subscription_name", _sa.String, primary_key=True),
            _sa.Column("current_generation", _sa.BigInteger, nullable=False),
            _sa.Column(
                "last_rebalance_at",
                _sa.DateTime(timezone=True),
                nullable=False,
                server_default=_sa.func.now(),
            ),
            _sa.Column("last_leader_id", _UUIDType(), nullable=True),
        )
        self.metadata.create_all(self._engine)

        # Locker holds the single leader advisory lock during rebalance.
        self._locker = DbLockProvider(name=f"pa_{name}", engine=self._engine)
        self._leader_lock_name = f"rebalance-{name}"

    # ------------------------------------------------------------------ API

    def register(
        self,
        instance_id: _uuid.UUID,
        host: Optional[str] = None,
        pid: Optional[int] = None,
        label: Optional[str] = None,
    ) -> None:
        from sqlalchemy.dialects.postgresql import insert

        with self._engine.begin() as conn:
            stmt = (
                insert(self.instances_table)
                .values(
                    instance_id=instance_id,
                    subscription_name=self.name,
                    host=host,
                    pid=pid,
                    label=label,
                )
                .on_conflict_do_update(
                    index_elements=[self.instances_table.c.instance_id],
                    set_={
                        self.instances_table.c.last_heartbeat_at: _sa.func.now(),
                        self.instances_table.c.host: host,
                        self.instances_table.c.pid: pid,
                        self.instances_table.c.label: label,
                    },
                )
            )
            conn.execute(stmt)

    def heartbeat(self, instance_id: _uuid.UUID) -> bool:
        with self._engine.begin() as conn:
            result = conn.execute(
                self.instances_table.update()
                .where(self.instances_table.c.instance_id == instance_id)
                .values(last_heartbeat_at=_sa.func.now())
            )
            return (result.rowcount or 0) > 0

    def deregister(self, instance_id: _uuid.UUID) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                self.instances_table.delete().where(
                    self.instances_table.c.instance_id == instance_id
                )
            )

    def get_my_assignments(self, instance_id: _uuid.UUID) -> Dict[int, int]:
        with self._engine.connect() as conn:
            rows = conn.execute(
                _sa.select(
                    self.assignments_table.c.partition,
                    self.assignments_table.c.generation,
                ).where(self.assignments_table.c.instance_id == instance_id)
            ).fetchall()
        return {row.partition: row.generation for row in rows}

    def active_instances(self) -> Iterator[_uuid.UUID]:
        with self._engine.connect() as conn:
            for row in conn.execute(
                _sa.select(self.instances_table.c.instance_id)
            ).fetchall():
                yield row.instance_id

    def rebalance(self, known_partitions: Iterable[int]) -> bool:
        if not self._locker.lock(self._leader_lock_name):
            return False
        try:
            self._do_rebalance(known_partitions)
            return True
        finally:
            self._locker.unlock(self._leader_lock_name)

    # -------------------------------------------------------------- helpers

    def _do_rebalance(self, known_partitions: Iterable[int]) -> None:
        from sqlalchemy.dialects.postgresql import insert

        partitions = sorted(set(known_partitions))
        with self._engine.begin() as conn:
            cutoff = _sa.func.now() - _sa.text(
                f"interval '{self._instance_ttl_seconds} seconds'"
            )
            # Reap stale instances. CASCADE wipes their assignments.
            conn.execute(
                self.instances_table.delete().where(
                    self.instances_table.c.last_heartbeat_at < cutoff
                )
            )

            alive = [
                row.instance_id
                for row in conn.execute(
                    _sa.select(self.instances_table.c.instance_id)
                ).fetchall()
            ]
            current = {
                row.partition: row.instance_id
                for row in conn.execute(
                    _sa.select(
                        self.assignments_table.c.partition,
                        self.assignments_table.c.instance_id,
                    )
                ).fetchall()
            }

            desired = compute_assignment(alive, partitions, current)

            # Upsert the rebalance row (and get the current global generation).
            gen_row = conn.execute(
                _sa.select(self.rebalance_table.c.current_generation).where(
                    self.rebalance_table.c.subscription_name == self.name
                )
            ).fetchone()
            current_generation = gen_row.current_generation if gen_row else 0

            # Determine which partitions need to change.
            changes = []
            for partition, instance_id in desired.items():
                prev_owner = current.get(partition)
                if prev_owner != instance_id:
                    changes.append((partition, instance_id))

            # Remove assignments for partitions no longer known (e.g. shrunk set).
            stale_partitions = [p for p in current if p not in desired]
            if stale_partitions:
                conn.execute(
                    self.assignments_table.delete().where(
                        self.assignments_table.c.partition.in_(stale_partitions)
                    )
                )

            if changes:
                current_generation += 1
                for partition, instance_id in changes:
                    stmt = (
                        insert(self.assignments_table)
                        .values(
                            subscription_name=self.name,
                            partition=partition,
                            instance_id=instance_id,
                            generation=current_generation,
                        )
                        .on_conflict_do_update(
                            index_elements=[self.assignments_table.c.partition],
                            set_={
                                self.assignments_table.c.instance_id: instance_id,
                                self.assignments_table.c.generation: current_generation,
                                self.assignments_table.c.assigned_at: _sa.func.now(),
                            },
                        )
                    )
                    conn.execute(stmt)

            conn.execute(
                insert(self.rebalance_table)
                .values(
                    subscription_name=self.name,
                    current_generation=current_generation,
                    last_rebalance_at=_sa.func.now(),
                    last_leader_id=None,
                )
                .on_conflict_do_update(
                    index_elements=[self.rebalance_table.c.subscription_name],
                    set_={
                        self.rebalance_table.c.current_generation: current_generation,
                        self.rebalance_table.c.last_rebalance_at: _sa.func.now(),
                    },
                )
            )

    def finalize(self):
        try:
            self._locker.finalize()
        except Exception:
            pass

    def __del__(self):
        self.finalize()
