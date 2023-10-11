import sys
import typing

if sys.version_info < (3, 9):
    raise RuntimeError("DepecheDB requires Python 3.9 or newer")
elif sys.version_info < (3, 10):
    UNION_TYPES = (typing.Union,)
else:
    import types

    UNION_TYPES = (typing.Union, types.UnionType)
