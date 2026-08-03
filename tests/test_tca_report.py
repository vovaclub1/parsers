from storage.execution_store import ExecutionStore
from tools.tca_report import build_report


def test_tca_report_joins_fill_to_bbo(tmp_path):
    db = tmp_path / "execution.sqlite3"
    store = ExecutionStore(db)
    store.record_market_snapshot(
        signal_id="s", client_order_id="l", venue="bybit", symbol="AAAUSDT",
        stage="send", bid=99, ask=101, ts_ns=1,
    )
    store.record_fill(
        exec_id="e", client_order_id="l", exchange_order_id="o",
        symbol="AAAUSDT", side="Buy", price=101, qty=1,
        fee=0.01, fee_asset="USDT", ts_ns=2,
    )
    report = build_report(db)
    assert report["fills"] == 1
    assert report["fills_with_bbo"] == 1
    assert report["coverage_pct"] == 100
    assert report["implementation_shortfall_bps_mean"] == 100
