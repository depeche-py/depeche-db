from typing import Set

import sqlalchemy as _sa

from .._interfaces import SubscriptionState


class DbSubscriptionStateProvider:
    SPECIAL_PARTITION_FOR_INIT_STATE = -1

    def __init__(self, name: str, engine: _sa.engine.Engine):
        assert name.isidentifier(), "Name must be a valid identifier"
        self.name = name
        self._engine = engine

        self.metadata = _sa.MetaData()
        self.state_table = _sa.Table(
            f"{name}_subscription_state",
            self.metadata,
            _sa.Column("subscription_name", _sa.String, primary_key=True),
            _sa.Column("partition", _sa.Integer, primary_key=True),
            _sa.Column("position", _sa.Integer, nullable=False),
        )
        self.metadata.create_all(self._engine)
        self._initialized_subscriptions: Set[str] = set()

    def store(self, subscription_name: str, partition: int, position: int):
        from sqlalchemy.dialects.postgresql import insert

        with self._engine.connect() as conn:
            conn.execute(
                insert(self.state_table)
                .values(
                    subscription_name=subscription_name,
                    partition=partition,
                    position=position,
                )
                .on_conflict_do_update(
                    index_elements=[
                        self.state_table.c.subscription_name,
                        self.state_table.c.partition,
                    ],
                    set_={
                        self.state_table.c.position: position,
                    },
                )
            )
            conn.commit()

    def read(self, subscription_name: str) -> SubscriptionState:
        with self._engine.connect() as conn:
            return SubscriptionState(
                {
                    row.partition: row.position
                    for row in conn.execute(
                        _sa.select(
                            self.state_table.c.partition,
                            self.state_table.c.position,
                        ).where(
                            _sa.and_(
                                self.state_table.c.subscription_name
                                == subscription_name,
                                self.state_table.c.partition
                                != self.SPECIAL_PARTITION_FOR_INIT_STATE,
                            )
                        )
                    )
                }
            )

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
