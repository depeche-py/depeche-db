import pytest

from depeche_db import DefaultSchemaProvider, Storage
from tests._tools import identifier


@pytest.fixture
def storage(db_engine) -> Storage:
    return Storage(
        name=identifier(),
        engine=db_engine,
        schema_provider=DefaultSchemaProvider(engine=db_engine),
    )
