from storage.execution_store import ExecutionStore


def test_market_snapshot_roundtrip(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    store.record_market_snapshot(
        signal_id="sig-1", client_order_id="link-1", venue="bybit",
        symbol="AAAUSDT", stage="arrival", bid=99, ask=101,
        bid_qty=10, ask_qty=12, depth_bids=[[99, 10]],
        depth_asks=[[101, 12]], ts_ns=123,
    )
    snap = store.get_market_snapshot("link-1", "arrival")
    assert snap["mid"] == 100
    assert snap["spread_bps"] == 200
    assert snap["bid_qty"] == 10


def test_newer_snapshot_replaces_same_stage(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    args = dict(
        signal_id="sig-1", client_order_id="link-1", venue="bybit",
        symbol="AAAUSDT", stage="arrival", bid_qty=1, ask_qty=1,
        depth_bids=[], depth_asks=[],
    )
    store.record_market_snapshot(**args, bid=99, ask=101, ts_ns=100)
    store.record_market_snapshot(**args, bid=100, ask=102, ts_ns=200)
    snap = store.get_market_snapshot("link-1", "arrival")
    assert snap["ts_ns"] == 200
    assert store.count_market_snapshot_events("link-1", "arrival") == 2
