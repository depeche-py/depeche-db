from typing import Type, TypeVar

from .._interfaces import MessageSerializer

E = TypeVar("E")


class PydanticMessageSerializer(MessageSerializer[E]):
    # TODO fix typing when Union is given (see typing of pydantic TypeAdapter)
    def __init__(self, message_type: Type[E]):
        self.message_type = message_type

    def serialize(self, message: E) -> dict:
        # TODO pydantic v1 compatibility
        return message.model_dump(mode="json")  # type: ignore

    def deserialize(self, message: dict) -> E:
        import pydantic as _pydantic

        # TODO pydantic v1 compatibility
        return _pydantic.TypeAdapter(self.message_type).validate_python(message)
