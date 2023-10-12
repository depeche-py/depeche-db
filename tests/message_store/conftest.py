import uuid as _uuid

import pytest

from depeche_db import MessageStore

from ._my_event import MyEvent, MyEventSerializer


@pytest.fixture
def subject(identifier, db_engine):
    return MessageStore(
        name=identifier(), engine=db_engine, serializer=MyEventSerializer()
    )


@pytest.fixture
def events():
    return [
        MyEvent(event_id=_uuid.uuid4(), num=1),
        MyEvent(event_id=_uuid.uuid4(), num=2),
        MyEvent(event_id=_uuid.uuid4(), num=3),
        MyEvent(event_id=_uuid.uuid4(), num=4),
    ]
