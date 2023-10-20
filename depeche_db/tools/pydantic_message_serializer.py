from typing import Any, TypeVar, no_type_check

from depeche_db._compat import get_union_members

from .._interfaces import MessageSerializer

T = TypeVar("T")


class PydanticMessageSerializer(MessageSerializer[T]):
    # the real type would be: (self, message_type: Type[T])
    # but this is not supported by mypy (yet)
    def __init__(self, message_type: Any) -> None:
        import pydantic

        version = pydantic.VERSION.split(".")[0]
        self.pydantic_v2 = version == "2"

        self.message_type: type[T] = message_type
        self.message_types = {}
        for member in get_union_members(message_type):
            if member.__name__ in self.message_types:
                raise ValueError(
                    f"Duplicate class name {member.__name__} in union {message_type}"
                )
            self.message_types[member.__name__] = member

    def serialize(self, message: T) -> dict:
        typename = message.__class__.__name__
        if typename not in self.message_types:
            raise TypeError(
                f"Unknown message type {typename} (expected one of {self.message_types})"
            )
        result = self._serialize(message)
        result["__typename__"] = typename
        return result

    @no_type_check
    def _serialize(self, message: T) -> dict:
        if self.pydantic_v2:
            return message.model_dump(mode="json")
        return message.dict()

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
