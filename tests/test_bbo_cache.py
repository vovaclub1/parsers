from api import delist_api


def test_update_market_cache_includes_bbo():
    delist_api._replace_market_cache({
        "AAA": {
            "last": 100.0,
            "bid": 99.0,
            "ask": 101.0,
            "bid_qty": 10.0,
            "ask_qty": 12.0,
        }
    }, updated_at=50.0)
    assert delist_api.get_bbo("AAA", now=51.0) == {
        "bid1Price": 99.0,
        "ask1Price": 101.0,
        "bid1Size": 10.0,
        "ask1Size": 12.0,
        "updated_at": 50.0,
    }


def test_bbo_rejects_stale_cache():
    delist_api._replace_market_cache({
        "AAA": {"last": 100, "bid": 99, "ask": 101, "bid_qty": 1, "ask_qty": 1}
    }, updated_at=50.0)
    assert delist_api.get_bbo("AAA", now=60.0) is None
