from typing import Any, TypeVar, no_type_check

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

    @no_type_check
    def serialize(self, message: T) -> dict:
        if self.pydantic_v2:
            return message.model_dump(mode="json")
        return message.dict()

    @no_type_check
    def deserialize(self, message: dict) -> T:
        import pydantic as _pydantic

        if self.pydantic_v2:
            return _pydantic.TypeAdapter(self.message_type).validate_python(message)
        return _pydantic.parse_obj_as(self.message_type, message)
