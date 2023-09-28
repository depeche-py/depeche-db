import pytest

from depeche_db import Storage
from tests._tools import identifier


@pytest.fixture
def storage(db_engine):
    return Storage(name=identifier(), engine=db_engine)
