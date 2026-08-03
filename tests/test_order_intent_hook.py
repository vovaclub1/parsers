from api import delist_api


class FakeWriter:
    def __init__(self):
        self.calls = []

    def submit(self, method, **kwargs):
        self.calls.append((method, kwargs))
        return True


def test_record_order_intent_enqueues_intent_and_cached_bbo(monkeypatch):
    writer = FakeWriter()
    monkeypatch.setattr("storage.runtime.get_writer", lambda: writer)
    monkeypatch.setattr(delist_api, "get_bbo", lambda coin: {
        "bid1Price": 99, "ask1Price": 101,
        "bid1Size": 10, "ask1Size": 12, "updated_at": 1,
    })
    delist_api._record_order_intent(
        "listing", "AAA", "AAAUSDT", "Buy", 4, "link-1", "sync_ws",
    )
    assert [name for name, _ in writer.calls] == [
        "record_intent", "record_market_snapshot",
    ]
    assert writer.calls[0][1]["client_order_id"] == "link-1"
    assert writer.calls[1][1]["stage"] == "send"


def test_record_order_intent_is_noop_without_runtime(monkeypatch):
    monkeypatch.setattr("storage.runtime.get_writer", lambda: None)
    delist_api._record_order_intent(
        "listing", "AAA", "AAAUSDT", "Buy", 4, "link-1", "sync_ws",
    )


def test_production_inversion_event_labels_are_not_derived_from_api_module():
    import inspect
    from api import listing_api

    short_src = inspect.getsource(delist_api.market_open_short)
    long_src = inspect.getsource(listing_api.market_open_long)
    batch_src = inspect.getsource(listing_api.market_open_long_batch)
    assert '"listing", ticker_name, symbol, "Sell"' in short_src
    assert '"delisting", ticker_name, symbol, "Buy"' in long_src
    assert '"delisting", ticker, symbol, "Buy"' in batch_src
