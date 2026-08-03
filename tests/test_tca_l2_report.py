import pytest

from storage.execution_store import ExecutionStore
from tools.tca_report import build_report


def test_report_computes_expected_l2_vwap_and_extra_slippage(tmp_path):
    db = tmp_path / "execution.sqlite3"
    store = ExecutionStore(db)
    store.record_intent(
        signal_id="listing:AAA:1", client_order_id="l", venue="bybit",
        symbol="AAAUSDT", side="Sell", requested_qty=2,
        route="ws", ts_ns=1,
    )
    store.record_market_snapshot(
        signal_id="listing:AAA:1", client_order_id="l", venue="bybit",
        symbol="AAAUSDT", stage="send", bid=99, ask=101, ts_ns=1,
    )
    store.record_market_snapshot(
        signal_id="listing:AAA:1", client_order_id="l", venue="bybit",
        symbol="AAAUSDT", stage="send_l2", bid=99, ask=101,
        depth_bids=[[99, 1], [98, 10]], depth_asks=[[101, 10]], ts_ns=2,
    )
    store.record_fill(
        exec_id="e", client_order_id="l", exchange_order_id="o",
        symbol="AAAUSDT", side="Sell", price=97.5, qty=2,
        ts_ns=10,
    )
    report = build_report(db)
    cohort = report["cohorts"]["listing"]
    # requested notional=2*100=200; sell book: 99 + 101 at 98.
    expected_vwap = 200 / (1 + 101 / 98)
    assert cohort["expected_l2_vwap_mean"] == pytest.approx(expected_vwap)
    assert cohort["l2_complete_pct"] == 100
    assert cohort["fill_vs_l2_slippage_bps_median"] > 0
