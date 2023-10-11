import sys
import typing

if sys.version < "3.8":
    raise RuntimeError("DepecheDB requires Python 3.8 or newer")
elif sys.version < "3.10":
    UNION_TYPES = (typing.Union,)
else:
    import types

    UNION_TYPES = (typing.Union, types.UnionType)
