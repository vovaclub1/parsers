from api.bybit_ws_private import _epoch_ns


def test_epoch_milliseconds_are_converted_to_nanoseconds():
    assert _epoch_ns("1720000000123") == 1720000000123_000_000


def test_epoch_microseconds_are_converted_to_nanoseconds():
    assert _epoch_ns(1720000000123456) == 1720000000123456_000


def test_epoch_nanoseconds_are_preserved():
    assert _epoch_ns(1720000000123456789) == 1720000000123456789


def test_missing_timestamp_uses_supplied_fallback():
    assert _epoch_ns("", fallback=123) == 123
