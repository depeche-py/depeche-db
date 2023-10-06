from typing import Any, TypeVar

from .._interfaces import MessageSerializer

T = TypeVar("T")


class PydanticMessageSerializer(MessageSerializer[T]):
    # the real type would be: (self, message_type: Type[T])
    # but this is not supported by mypy (yet)
    def __init__(self, message_type: Any) -> None:
        self.message_type: type[T] = message_type

    def serialize(self, message: T) -> dict:
        # TODO pydantic v1 compatibility
        return message.model_dump(mode="json")  # type: ignore

    def deserialize(self, message: dict) -> T:
        import pydantic as _pydantic

        # TODO pydantic v1 compatibility
        return _pydantic.TypeAdapter(self.message_type).validate_python(message)
