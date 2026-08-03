from storage.execution_store import ExecutionStore
from api.bybit_ws_private import BybitWsPrivate


def make_client(writer):
    client = BybitWsPrivate.__new__(BybitWsPrivate)
    client._execution_store = writer
    return client


class DirectWriter:
    def __init__(self, store):
        self.store = store

    def submit(self, method, **kwargs):
        getattr(self.store, method)(**kwargs)
        return True


def test_execution_push_captures_fill_bbo(tmp_path, monkeypatch):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    client = make_client(DirectWriter(store))
    monkeypatch.setattr("api.delist_api.get_bbo", lambda coin: {
        "bid1Price": 98, "ask1Price": 102,
        "bid1Size": 10, "ask1Size": 12, "updated_at": 1,
    })
    client._on_execution([{
        "execId": "exec-1", "orderLinkId": "link-1",
        "orderId": "order-1", "symbol": "AAAUSDT", "side": "Sell",
        "execPrice": "99", "execQty": "4", "execFee": "0.01",
        "feeCurrency": "USDT", "execTime": "456",
    }])
    snap = store.get_market_snapshot("link-1", "fill")
    assert snap["bid"] == 98
    assert snap["ask"] == 102
    assert store.count_fills("link-1") == 1
