import pytest

from tap_gitlab import format_timestamp


def test_format_timestamp():
    assert format_timestamp("not a date", "number", {}) == "not a date"
    assert format_timestamp("2021-01-01T00:00:00Z", "string", {"format": "date-time"}) == "2021-01-01T00:00:00.000000Z"

    with pytest.raises(ValueError, match="Invalid isoformat string"):
        format_timestamp("not a date", "string", {"format": "date-time"})
