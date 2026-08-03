import pytest

from storage.execution_store import ExecutionStore
from tools.tca_report import build_report


def test_report_aggregates_partial_fills_by_order_and_cohort(tmp_path):
    db = tmp_path / "execution.sqlite3"
    store = ExecutionStore(db)
    store.record_intent(
        signal_id="listing:AAA:1", client_order_id="link-1",
        venue="bybit", symbol="AAAUSDT", side="Sell",
        requested_qty=4, route="sync_ws", ts_ns=1_000_000_000,
    )
    store.record_market_snapshot(
        signal_id="listing:AAA:1", client_order_id="link-1",
        venue="bybit", symbol="AAAUSDT", stage="send",
        bid=99, ask=101, ts_ns=1_000_000_000,
    )
    store.record_fill(
        exec_id="e1", client_order_id="link-1", exchange_order_id="o1",
        symbol="AAAUSDT", side="Sell", price=99, qty=1,
        fee=0.01, fee_asset="USDT", ts_ns=1_010_000_000,
    )
    store.record_fill(
        exec_id="e2", client_order_id="link-1", exchange_order_id="o1",
        symbol="AAAUSDT", side="Sell", price=98, qty=3,
        fee=0.03, fee_asset="USDT", ts_ns=1_020_000_000,
    )

    report = build_report(db)
    assert report["fills"] == 2
    assert report["orders_filled"] == 1
    assert report["orders_with_send_bbo"] == 1
    assert report["cohorts"]["listing"]["orders"] == 1
    assert report["cohorts"]["listing"]["fill_vwap_mean"] == pytest.approx(98.25)
    assert report["cohorts"]["listing"]["first_fill_latency_ms_median"] == 10
    assert report["cohorts"]["listing"]["implementation_shortfall_bps_median"] == pytest.approx(175)
