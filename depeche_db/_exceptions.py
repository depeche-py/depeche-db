from psycopg2.errors import LockNotAvailable
import contextlib as _contextlib
import dataclasses as _dc
import datetime as _dt
import uuid as _uuid
from typing import Generic, Iterable, Iterator, Protocol, TypeVar

import sqlalchemy as _sa
import sqlalchemy.orm as _orm
from sqlalchemy_utils import UUIDType as _UUIDType


class MessageNotFound(Exception):
    pass
