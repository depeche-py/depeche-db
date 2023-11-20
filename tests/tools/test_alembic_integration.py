import sqlalchemy as _sa
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext

from depeche_db import Storage
from depeche_db.tools import AlembicSchemaProvider


def test_migrations(clean_pg_db, identifier):
    provider = AlembicSchemaProvider()
    engine = _sa.create_engine(clean_pg_db, future=True, poolclass=_sa.pool.StaticPool)
    Storage(
        name=identifier(),
        engine=engine,
        schema_provider=provider,
    )

    with engine.connect() as connection:
        mc = MigrationContext.configure(connection=connection)
        diff = compare_metadata(mc, provider.get_metadata())  # type: ignore[arg-type]
    engine.dispose()
    assert diff == []
