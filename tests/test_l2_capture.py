from research.l2_capture import L2CaptureWorker
from storage.execution_store import ExecutionStore


def test_l2_worker_fetches_and_persists_depth(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")

    def fetch(symbol, limit):
        assert symbol == "AAAUSDT"
        assert limit == 50
        return {"b": [["99", "10"], ["98", "20"]],
                "a": [["101", "12"], ["102", "30"]], "ts": 123}

    worker = L2CaptureWorker(store, fetch_orderbook=fetch, max_queue=10)
    assert worker.submit(
        signal_id="listing:AAA:1", client_order_id="link-1",
        symbol="AAAUSDT", stage="send_l2",
    )
    worker.flush(timeout=2)
    snap = store.get_market_snapshot("link-1", "send_l2")
    assert snap["bid"] == 99
    assert snap["ask"] == 101
    assert snap["bid_qty"] == 10
    assert snap["ask_qty"] == 12
    worker.stop()


def test_l2_worker_failure_does_not_block_or_create_snapshot(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    worker = L2CaptureWorker(
        store, fetch_orderbook=lambda symbol, limit: (_ for _ in ()).throw(TimeoutError()),
    )
    assert worker.submit(
        signal_id="s", client_order_id="l", symbol="AAAUSDT", stage="send_l2",
    )
    worker.flush(timeout=2)
    assert store.get_market_snapshot("l", "send_l2") is None
    assert worker.errors == 1
    worker.stop()
