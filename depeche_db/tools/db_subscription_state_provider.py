import sqlalchemy as _sa

from .._interfaces import SubscriptionState


class DbSubscriptionStateProvider:
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
                            self.state_table.c.subscription_name == subscription_name
                        )
                    )
                }
            )
