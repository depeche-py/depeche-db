import sqlalchemy as _sa

from depeche_db.tools import DbLockProvider
from tests._tools import identifier


def test_db_lock_provider(pg_db):
    name = identifier()
    alt = DbLockProvider(engine=_sa.create_engine(pg_db), name=name)
    subject = DbLockProvider(engine=_sa.create_engine(pg_db), name=name)
    try:
        assert alt.lock("foo")
        assert not subject.lock("foo")
        alt.unlock("foo")
        assert subject.lock("foo")
        subject.unlock("foo")
    finally:
        alt.finalize()
        subject.finalize()
