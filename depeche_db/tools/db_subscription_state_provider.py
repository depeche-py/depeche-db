import contextlib as _contextlib
from typing import Iterator, Set

import sqlalchemy as _sa

from .._compat import SAConnection
from .._interfaces import SubscriptionState, SubscriptionStateProvider


class DbSubscriptionStateProvider(SubscriptionStateProvider):
    SPECIAL_PARTITION_FOR_INIT_STATE = -1

    def __init__(self, name: str, engine: _sa.engine.Engine):
        assert name.isidentifier(), "Name must be a valid identifier"
        self.name = name
        self._engine = engine

        self.metadata = _sa.MetaData()
        self.state_table = _sa.Table(
            f"depeche_subscriptions_{name}",
            self.metadata,
            _sa.Column("subscription_name", _sa.String, primary_key=True),
            _sa.Column("partition", _sa.Integer, primary_key=True),
            _sa.Column("position", _sa.Integer, nullable=False),
        )
        self.metadata.create_all(self._engine)
        self._initialized_subscriptions: Set[str] = set()

    def initialize(self, subscription_name: str):
        """
        Marks subscription state as initialized.
        """
        self.store(
            subscription_name=subscription_name,
            partition=self.SPECIAL_PARTITION_FOR_INIT_STATE,
            position=-1,
        )

    def initialized(self, subscription_name: str) -> bool:
        """
        Returns `True` if the subscription state was already initialized.
        """
        # Caches the result if True. It cannot go to False again!
        if subscription_name in self._initialized_subscriptions:
            return True

        with self._engine.connect() as conn:
            result: bool = conn.execute(
                _sa.select(
                    self.state_table.c.partition.isnot(None),
                ).where(
                    _sa.and_(
                        self.state_table.c.subscription_name == subscription_name,
                        self.state_table.c.partition
                        == self.SPECIAL_PARTITION_FOR_INIT_STATE,
                    )
                )
            ).scalar()

        if result:
            self._initialized_subscriptions.add(subscription_name)
        return result

    def store(self, subscription_name: str, partition: int, position: int):
        with self._session() as session:
            session.store(
                subscription_name=subscription_name,
                partition=partition,
                position=position,
            )

    def read(self, subscription_name: str) -> SubscriptionState:
        with self._session() as session:
            return session.read(subscription_name=subscription_name)

    @_contextlib.contextmanager
    def _session(self) -> Iterator["_Session"]:
        with self._engine.connect() as conn:
            yield _Session(conn=conn, parent=self)
            conn.commit()

    def session(self, conn: _sa.engine.Connection = None, **kwargs):
        assert conn, "Connection is required"
        assert not kwargs, "More arguments are not supported"
        return _Session(conn=conn, parent=self)

    @classmethod
    def get_migration_ddl(cls, name: str):
        """
        DDL Script to migrate from <=0.8.0
        """
        tablename = (f"depeche_subscriptions_{name}",)
        return f"""
            ALTER TABLE "{name}_subscription_state"
                 RENAME TO {tablename};
            """

    @classmethod
    def migrate_db_objects(cls, name: str, conn: SAConnection):
        """
        Migrate from <=0.8.0
        """
        conn.execute(cls.get_migration_ddl(name=name))


class _Session:
    def __init__(
        self, conn: _sa.engine.Connection, parent: DbSubscriptionStateProvider
    ):
        self._conn = conn
        self._parent = parent

    def initialize(self, subscription_name: str):
        self._parent.initialize(subscription_name=subscription_name)

    def initialized(self, subscription_name: str) -> bool:
        return self._parent.initialized(subscription_name=subscription_name)

    def store(self, subscription_name: str, partition: int, position: int):
        from sqlalchemy.dialects.postgresql import insert

        state_table = self._parent.state_table
        self._conn.execute(
            insert(state_table)
            .values(
                subscription_name=subscription_name,
                partition=partition,
                position=position,
            )
            .on_conflict_do_update(
                index_elements=[
                    state_table.c.subscription_name,
                    state_table.c.partition,
                ],
                set_={
                    state_table.c.position: position,
                },
            )
        )

    def read(self, subscription_name: str) -> SubscriptionState:
        state_table = self._parent.state_table
        return SubscriptionState(
            {
                row.partition: row.position
                for row in self._conn.execute(
                    _sa.select(
                        state_table.c.partition,
                        state_table.c.position,
                    ).where(
                        _sa.and_(
                            state_table.c.subscription_name == subscription_name,
                            state_table.c.partition
                            != self._parent.SPECIAL_PARTITION_FOR_INIT_STATE,
                        )
                    )
                )
            }
        )
