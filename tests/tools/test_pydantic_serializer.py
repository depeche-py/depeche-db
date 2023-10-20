import datetime as _dt
import decimal as _decimal
import uuid as _uuid
from typing import Any, Union

import pydantic
import pytest

from depeche_db.tools import PydanticMessageSerializer


class Foo(pydantic.BaseModel):
    foo: str


class Bar(pydantic.BaseModel):
    bar: str


class Scalars(pydantic.BaseModel):
    dt: _dt.datetime
    d: _dt.date
    uuid: _uuid.UUID
    decimal: _decimal.Decimal
    flt: float


class A(pydantic.BaseModel):
    a: str


class B(pydantic.BaseModel):
    a: str


def test_serialize():
    subject: Any = PydanticMessageSerializer(Union[Foo, Bar])
    assert subject.serialize(Foo(foo="bar")) == {"__typename__": "Foo", "foo": "bar"}
    assert subject.serialize(Bar(bar="bar")) == {"__typename__": "Bar", "bar": "bar"}


def test_serialize_scalars():
    subject: Any = PydanticMessageSerializer(Scalars)
    assert subject.serialize(
        Scalars(
            dt=_dt.datetime(2020, 1, 1, 0, 0, 0, 0),
            d=_dt.date(2020, 1, 1),
            uuid=_uuid.UUID("00000000-0000-0000-0000-000000000000"),
            decimal=_decimal.Decimal("1.0"),
            flt=1.0,
        )
    ) == {
        "__typename__": "Scalars",
        "dt": "2020-01-01T00:00:00",
        "d": "2020-01-01",
        "uuid": "00000000-0000-0000-0000-000000000000",
        "decimal": "1.0",
        "flt": 1.0,
    }


def test_deserialize_scalars():
    subject: Any = PydanticMessageSerializer(Scalars)
    assert subject.deserialize(
        {
            "__typename__": "Scalars",
            "dt": "2020-01-01T00:00:00",
            "d": "2020-01-01",
            "uuid": "00000000-0000-0000-0000-000000000000",
            "decimal": "1.0",
            "flt": 1.0,
        }
    ) == Scalars(
        dt=_dt.datetime(2020, 1, 1, 0, 0, 0, 0),
        d=_dt.date(2020, 1, 1),
        uuid=_uuid.UUID("00000000-0000-0000-0000-000000000000"),
        decimal=_decimal.Decimal("1.0"),
        flt=1.0,
    )


def test_deserialize():
    subject: Any = PydanticMessageSerializer(Union[Foo, Bar])
    assert subject.deserialize({"__typename__": "Foo", "foo": "bar"}) == Foo(foo="bar")
    assert subject.deserialize({"__typename__": "Bar", "bar": "bar"}) == Bar(bar="bar")


def test_overlapping_classes():
    subject: Any = PydanticMessageSerializer(Union[B, A])
    assert subject.deserialize({"__typename__": "A", "a": "foo"}) == A(a="foo")
    assert subject.deserialize({"__typename__": "B", "a": "foo"}) == B(a="foo")


def test_serialize_unknown_class():
    subject: Any = PydanticMessageSerializer(Union[Foo, Bar])
    with pytest.raises(TypeError):
        subject.serialize(A(a="foo"))


def test_deserialize_unknown_class():
    subject: Any = PydanticMessageSerializer(Union[Foo, Bar])
    with pytest.raises(TypeError):
        subject.deserialize({"__typename__": "A", "a": "foo"})


OuterFoo = Foo


def test_duplicate_class_name():
    class Foo(pydantic.BaseModel):
        xxx: str

    with pytest.raises(ValueError):
        PydanticMessageSerializer(Union[OuterFoo, Union[A, Foo]])


def test_aliases():
    subject: Any = PydanticMessageSerializer(Union[A, B], aliases={"OldNameOfA": A})
    assert subject.deserialize({"__typename__": "OldNameOfA", "a": "foo"}) == A(a="foo")


def test_aliases_not_valid():
    with pytest.raises(ValueError):
        PydanticMessageSerializer(Union[A], aliases={"OldNameOfB": B})

    with pytest.raises(ValueError):
        PydanticMessageSerializer(Union[A, B], aliases={"A": B})
