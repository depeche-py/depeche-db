import sys
import typing

if sys.version_info < (3, 9):
    raise RuntimeError("DepecheDB requires Python 3.9 or newer")
elif sys.version_info < (3, 10):
    UNION_TYPES = (typing.Union,)
else:
    import types

    UNION_TYPES = (typing.Union, types.UnionType)


SA_VERSION = "2.x"
try:
    from sqlalchemy import Connection as SAConnection  # noqa
except ImportError:
    SA_VERSION = "1.4.x"
    from sqlalchemy.engine import Connection as SAConnection  # noqa
