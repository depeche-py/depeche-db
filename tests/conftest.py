import os as _os

import pytest
import sqlalchemy as _sa
import sqlalchemy.orm as _orm

from . import tools


@pytest.fixture(scope="session")
def pg_db():
    dsn = f"postgresql://depeche:depeche@localhost:4888/depeche_test_{_os.getpid()}"
    if tools.pg_check_if_db_exists(dsn):
        tools.pg_drop_db(dsn)
    tools.pg_create_db(dsn)

    yield dsn

    tools.pg_drop_db(dsn)


@pytest.fixture
def db_engine(pg_db):
    engine = _sa.create_engine(
        pg_db,
        future=True,
    )
    # _create_all(engine)

    yield engine

    # _drop_all(engine)
    engine.dispose()


@pytest.fixture
def db_session_factory(db_engine):
    session_factory = _orm.sessionmaker(db_engine)
    return session_factory


@pytest.fixture
def log_queries():
    import logging

    logger = logging.getLogger("sqlalchemy.engine")
    level = logger.getEffectiveLevel()
    logger.setLevel(logging.INFO)
    yield
    logger.setLevel(level)
