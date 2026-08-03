from storage.execution_store import ExecutionStore
from research.market_capture import capture_bbo_snapshot


def test_capture_records_valid_bbo(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")

    def fetch(symbol):
        assert symbol == "AAAUSDT"
        return {"bid1Price": "99", "bid1Size": "10", "ask1Price": "101", "ask1Size": "12"}

    assert capture_bbo_snapshot(
        store=store, fetch_ticker=fetch, signal_id="sig-1",
        client_order_id="link-1", symbol="AAAUSDT", stage="send",
    ) is True
    assert store.get_market_snapshot("link-1", "send")["mid"] == 100


def test_capture_failure_is_non_fatal(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    assert capture_bbo_snapshot(
        store=store, fetch_ticker=lambda symbol: (_ for _ in ()).throw(TimeoutError()),
        signal_id="sig-1", client_order_id="link-1",
        symbol="AAAUSDT", stage="send",
    ) is False
