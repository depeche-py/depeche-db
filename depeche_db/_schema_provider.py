import sqlalchemy as _sa

from ._interfaces import SchemaProvider


class DefaultSchemaProvider(SchemaProvider):
    def __init__(self, engine: _sa.engine.Engine):
        self._engine = engine

    def register(self, metadata: _sa.MetaData):
        metadata.create_all(bind=self._engine, checkfirst=True)
