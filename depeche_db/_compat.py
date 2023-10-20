import sys
import typing

if sys.version_info < (3, 9):
    raise RuntimeError("DepecheDB requires Python 3.9 or newer")
elif sys.version_info < (3, 10):
    UNION_TYPES = (typing.Union,)

    def issubclass_with_union(cls, superclass):
        if typing.get_origin(superclass) in UNION_TYPES:
            if cls == superclass:
                return True
            return any(
                issubclass_with_union(cls, member)
                for member in typing.get_args(superclass)
            )

        if typing.get_origin(cls) in UNION_TYPES:
            return all(
                issubclass_with_union(member, superclass)
                for member in typing.get_args(cls)
            )

        return issubclass(cls, superclass)

else:
    import types

    UNION_TYPES = (typing.Union, types.UnionType)

    def issubclass_with_union(cls, class_or_tuple):
        if typing.get_origin(cls) in UNION_TYPES:
            return all(
                issubclass_with_union(member, class_or_tuple)
                for member in typing.get_args(cls)
            )
        return issubclass(cls, class_or_tuple)


def get_union_members(union_or_type) -> typing.Generator[typing.Type, None, None]:
    if typing.get_origin(union_or_type) in UNION_TYPES:
        for member in typing.get_args(union_or_type):
            yield from get_union_members(member)
    else:
        yield union_or_type


SA_VERSION = "2.x"
try:
    from sqlalchemy import Connection as SAConnection  # noqa
except ImportError:
    SA_VERSION = "1.4.x"
    from sqlalchemy.engine import Connection as SAConnection  # noqa
