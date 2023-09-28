import datetime as _dt
import json as _json
import uuid as _uuid


class AnyInt(int):
    def __eq__(self, other):
        return isinstance(other, int)


class AnyFloat(float):
    def __eq__(self, other):
        return isinstance(other, float)


class AnyStr(str):
    def __eq__(self, other):
        return isinstance(other, str)


class AnyUuid(_uuid.UUID):
    def __init__(self):
        super().__init__("12345678-1234-4123-1234-1234567890ab")

    def __eq__(self, other):
        return isinstance(other, _uuid.UUID)

    def __str__(self):
        return "ANY_UUID"


class AnyUUID(AnyUuid):
    pass


class UuidOrUuidString(_uuid.UUID):
    def __init__(self, value: str | _uuid.UUID):
        if isinstance(value, _uuid.UUID):
            value = str(value)
        super().__init__(hex=value)

    def __eq__(self, other):
        return (isinstance(other, _uuid.UUID) and super().__eq__(other)) or (
            isinstance(other, str) and str(self) == other
        )


class AnyUuidOrUuidString(str):
    def __eq__(self, other):
        return (isinstance(other, _uuid.UUID)) or (
            isinstance(other, str) and AnyUuidString() == other
        )


class AnyUuidString:
    def __eq__(self, other):
        if not isinstance(other, str):
            return False
        try:
            _uuid.UUID(other)
            return True
        except ValueError:
            return False


class AnyDatetime(_dt.datetime):
    def __new__(cls):
        obj = _dt.datetime.__new__(cls, 1969, 7, 21, 20, 17, 40)
        return obj

    def __eq__(self, other):
        return isinstance(other, _dt.datetime)

    def __str__(self):
        return "ANY_DATETIME"


class AnyDatetimeOrDatetimeString(str):
    def __eq__(self, other):
        try:
            if isinstance(other, _dt.datetime):
                return True
            _dt.datetime.fromisoformat(other)
            return True
        except (ValueError, TypeError):
            return False

    def __str__(self):
        return "ANY_DATETIME_STRING"


class AnyDate(_dt.date):
    def __new__(cls):
        obj = _dt.date.__new__(cls, 1969, 7, 21)
        return obj

    def __eq__(self, other):
        return isinstance(other, _dt.date)

    def __str__(self):
        return "ANY_DATE"


class JsonOrParsedJson(str):
    def __eq__(self, other):
        if not isinstance(other, str):
            other = _json.dumps(other)
        return super().__eq__(other)
