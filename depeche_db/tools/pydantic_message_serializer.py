import datetime as _dt
import decimal as _decimal
import enum as _enum
import uuid as _uuid
from typing import Any, Optional, Type, TypeVar, no_type_check

from depeche_db._compat import get_union_members, issubclass_with_union

from .._interfaces import MessageSerializer

T = TypeVar("T")


class PydanticMessageSerializer(MessageSerializer[T]):
    # the real type would be: (self, message_type: Type[T])
    # but this is not supported by mypy (yet)
    def __init__(
        self, message_type: Any, aliases: Optional[dict[str, Type]] = None
    ) -> None:
        import pydantic

        version = pydantic.VERSION.split(".")[0]
        self.pydantic_v2 = version == "2"

        self.message_type: type[T] = message_type
        self.message_types: dict[str, Type] = {}
        for member in get_union_members(message_type):
            if member.__name__ in self.message_types:
                raise ValueError(
                    f"Duplicate class name {member.__name__} in union {message_type}"
                )
            self.message_types[member.__name__] = member

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
                self.message_types[alias] = member

    def serialize(self, message: T) -> dict:
        typename = message.__class__.__name__
        if typename not in self.message_types:
            raise TypeError(
                f"Unknown message type {typename} (expected one of {self.message_types})"
            )
        result: dict = self._serialize(message)
        result["__typename__"] = typename
        return result

    @no_type_check
    def _serialize(self, message: T) -> dict:
        if self.pydantic_v2:
            return message.model_dump(mode="json")
        return make_jsonable(message.dict())

    @no_type_check
    def deserialize(self, message: dict) -> T:
        import pydantic as _pydantic

        typename = message.pop("__typename__")
        if typename not in self.message_types:
            raise TypeError(
                f"Unknown message type {typename} (expected one of {self.message_types})"
            )
        message_type = self.message_types[typename]

        if self.pydantic_v2:
            return _pydantic.TypeAdapter(message_type).validate_python(message)
        return _pydantic.parse_obj_as(message_type, message)


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
