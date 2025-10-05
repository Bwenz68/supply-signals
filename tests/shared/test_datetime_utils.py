from datetime import datetime, timezone
import pytest

from shared.datetime_utils import parse_to_utc, to_iso_utc, STRICT_Z_ISO_PATTERN

def test_iso_z_passthrough():
    dt = parse_to_utc("2025-10-05T06:20:00Z")
    assert to_iso_utc(dt) == "2025-10-05T06:20:00Z"
    assert STRICT_Z_ISO_PATTERN.match(to_iso_utc(dt))

def test_iso_with_offset():
    dt = parse_to_utc("2025-10-05T02:20:00-04:00")
    assert to_iso_utc(dt) == "2025-10-05T06:20:00Z"

def test_naive_iso_with_minutes_and_default_tz():
    dt = parse_to_utc("2025-10-05 02:20", naive_tz="America/New_York")
    assert to_iso_utc(dt) == "2025-10-05T06:20:00Z"

def test_rfc2822_rss():
    dt = parse_to_utc("Sun, 05 Oct 2025 06:20:00 GMT")
    assert to_iso_utc(dt) == "2025-10-05T06:20:00Z"

def test_slash_date_and_utc_offset():
    dt = parse_to_utc("2025/10/05 06:20:00 +0000")
    assert to_iso_utc(dt) == "2025-10-05T06:20:00Z"

def test_missing_seconds_added():
    dt = parse_to_utc("2025-10-05T06:20Z")
    assert to_iso_utc(dt) == "2025-10-05T06:20:00Z"

def test_missing_raises():
    with pytest.raises(ValueError):
        parse_to_utc("", naive_tz="UTC")

def test_out_of_range():
    with pytest.raises(ValueError):
        parse_to_utc("1900-01-01T00:00:00Z")
