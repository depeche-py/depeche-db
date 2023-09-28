from psycopg2.errors import LockNotAvailable
import contextlib as _contextlib
import dataclasses as _dc
import datetime as _dt
import uuid as _uuid
from typing import Generic, Iterable, Iterator, Protocol, TypeVar

import sqlalchemy as _sa
import sqlalchemy.orm as _orm
from sqlalchemy_utils import UUIDType as _UUIDType

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
            _sa.Column("group_name", _sa.String, primary_key=True),
            _sa.Column("partition", _sa.Integer, primary_key=True),
            _sa.Column("position", _sa.Integer, nullable=False),
        )
        self.metadata.create_all(self._engine)

    def store(self, group_name: str, partition: int, position: int):
        from sqlalchemy.dialects.postgresql import insert

        with self._engine.connect() as conn:
            conn.execute(
                insert(self.state_table)
                .values(group_name=group_name, partition=partition, position=position)
                .on_conflict_do_update(
                    index_elements=[
                        self.state_table.c.group_name,
                        self.state_table.c.partition,
                    ],
                    set_={
                        self.state_table.c.position: position,
                    },
                )
            )
            conn.commit()

    def read(self, group_name: str) -> SubscriptionState:
        with self._engine.connect() as conn:
            return SubscriptionState(
                {
                    row.partition: row.position
                    for row in conn.execute(
                        _sa.select(
                            self.state_table.c.partition,
                            self.state_table.c.position,
                        ).where(self.state_table.c.group_name == group_name)
                    )
                }
            )
