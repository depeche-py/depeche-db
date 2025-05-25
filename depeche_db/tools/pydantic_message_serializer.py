import datetime as _dt
import decimal as _decimal
import enum as _enum
import uuid as _uuid
from typing import Any, Callable, Optional, Type, TypeVar, no_type_check

import pydantic as _pydantic

from depeche_db._compat import get_union_members, issubclass_with_union

from .._interfaces import MessageSerializer

pydantic_v2 = _pydantic.VERSION.split(".")[0] == "2"
T = TypeVar("T", bound=_pydantic.BaseModel)


def _get_de_serializer(
    type_: Type[T],
) -> tuple[Callable[[dict], T], Callable[[T], dict]]:
    if pydantic_v2:

        def deserializer_v2(message: dict) -> T:
            return type_.model_validate(message)

        def serializer_v2(message: T) -> dict:
            return message.model_dump(mode="json")

        return deserializer_v2, serializer_v2

    def deserializer(message: dict) -> T:
        return type_.parse_obj(message)

    def serializer(message: T) -> dict:
        return make_jsonable(message.dict())  # type: ignore[no-any-return]

    return deserializer, serializer


class PydanticMessageSerializer(MessageSerializer[T]):
    # the real type would be: (self, message_type: Type[T])
    # but this is not supported by mypy (yet)
    def __init__(
        self, message_type: Any, aliases: Optional[dict[str, Type]] = None
    ) -> None:
        self.message_type: type[T] = message_type
        self.message_types: set[str] = set()
        self.serializers: dict[str, Callable[[Any], dict]] = {}
        self.deserializers: dict[str, Callable[[dict], Any]] = {}
        for member in get_union_members(message_type):
            if member.__name__ in self.message_types:
                raise ValueError(
                    f"Duplicate class name {member.__name__} in union {message_type}"
                )
            self.message_types.add(member.__name__)
            (
                self.deserializers[member.__name__],
                self.serializers[member.__name__],
            ) = _get_de_serializer(member)

        if aliases is not None:
            for alias, member in aliases.items():
                if alias in self.message_types:
                    raise ValueError(
                        f"Alias {alias} conflicts with existing class name in union {message_type}"
                    )
                if not issubclass_with_union(member, self.message_type):
                    raise ValueError(
                        f"Type {member} is not a subclass of {self.message_type}"
                    )
                self.message_types.add(alias)
                self.deserializers[alias], self.serializers[alias] = _get_de_serializer(
                    member
                )

    def serialize(self, message: T) -> dict:
        typename = message.__class__.__name__
        if typename not in self.message_types:
            raise TypeError(
                f"Unknown message type {typename} (expected one of {self.message_types})"
            )
        result: dict = self.serializers[typename](message)
        result["__typename__"] = typename
        return result

    @no_type_check
    def deserialize(self, message: dict) -> T:
        typename = message.pop("__typename__")
        if typename not in self.message_types:
            raise TypeError(
                f"Unknown message type {typename} (expected one of {self.message_types})"
            )
        return self.deserializers[typename](message)


def make_jsonable(obj):
    if isinstance(obj, list):
        return [make_jsonable(elem) for elem in obj]
    if isinstance(obj, dict):
        return {key: make_jsonable(value) for key, value in obj.items()}
    if isinstance(obj, _uuid.UUID):
        return str(obj)
    if isinstance(obj, _decimal.Decimal):
        return str(obj)
    if isinstance(obj, _dt.datetime):
        return obj.isoformat()
    if isinstance(obj, _dt.date):
        return obj.isoformat()
    if isinstance(obj, _enum.Enum):
        return obj.value
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    raise NotImplementedError(f"Cannot make object of type {type(obj)} jsonable")
