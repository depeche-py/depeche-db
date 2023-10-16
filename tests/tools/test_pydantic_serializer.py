from typing import Any, Union

import pydantic
import pytest

from depeche_db.tools import PydanticMessageSerializer


class Foo(pydantic.BaseModel):
    foo: str


class Bar(pydantic.BaseModel):
    bar: str


def test_serialize():
    subject: Any = PydanticMessageSerializer(Union[Foo, Bar])
    assert subject.serialize(Foo(foo="bar")) == {"foo": "bar"}
    assert subject.serialize(Bar(bar="bar")) == {"bar": "bar"}


def test_deserialize():
    subject: Any = PydanticMessageSerializer(Union[Foo, Bar])
    assert subject.deserialize({"foo": "bar"}) == Foo(foo="bar")
    assert subject.deserialize({"bar": "bar"}) == Bar(bar="bar")


@pytest.mark.xfail(reason="This needs handling in the serializer")
def test_overlapping_classes():
    class A(pydantic.BaseModel):
        a: str

    class B(pydantic.BaseModel):
        a: str

    subject: Any = PydanticMessageSerializer(Union[B, A])
    assert subject.deserialize({"a": "foo"}) == A(a="foo")
    assert subject.deserialize({"a": "foo"}) == B(a="foo")
