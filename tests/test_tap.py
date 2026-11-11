import pytest

from tap_gitlab import flatten_id, format_timestamp


def test_format_timestamp():
    assert format_timestamp("not a date", "number", {}) == "not a date"
    assert format_timestamp("2021-01-01T00:00:00Z", "string", {"format": "date-time"}) == "2021-01-01T00:00:00.000000Z"

    with pytest.raises(ValueError, match="Invalid isoformat string"):
        format_timestamp("not a date", "string", {"format": "date-time"})


def test_flatten_id_dict_value():
    item = {"author": {"id": 1, "username": "someone"}}
    flatten_id(item, "author")
    assert item == {"author_id": 1}


def test_flatten_id_scalar_value_does_not_crash():
    # Some records carry a scalar instead of an object; keep the value
    # instead of raising AttributeError.
    item = {"author": "someone"}
    flatten_id(item, "author")
    assert item == {"author_id": "someone"}


def test_flatten_id_missing_or_none_value():
    item = {}
    flatten_id(item, "author")
    assert item == {"author_id": None}
    item = {"author": None}
    flatten_id(item, "author")
    assert item == {"author": None, "author_id": None}
