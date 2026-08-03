import inspect

import parsers.parser_listing as listing
import parsers.parser_delist as delist


def test_low_value_notice_sources_default_off():
    assert listing.BITHUMB_NOTICE_POLLER_ENABLED is False
    assert listing.BINANCE_NOTICE_POLLER_ENABLED is False
    assert delist.BINANCE_DIRECT_POLLER_ENABLED is False


def test_listing_startup_guards_notice_threads_by_flags():
    source = inspect.getsource(listing)
    assert "if BITHUMB_NOTICE_POLLER_ENABLED:" in source
    assert "if BINANCE_NOTICE_POLLER_ENABLED:" in source


def test_delist_startup_skips_pollers_and_watchdog_when_disabled():
    source = inspect.getsource(delist)
    assert "if BINANCE_DIRECT_POLLER_ENABLED:" in source
    assert "direct Binance article poller ОТКЛЮЧЁН" in source
