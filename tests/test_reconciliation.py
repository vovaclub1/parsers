import pytest

from storage.execution_store import ExecutionStore
from research.reconciliation import reconcile_trades


def test_reconcile_entry_and_unlinked_exit_fills(tmp_path):
    db = tmp_path / "execution.sqlite3"
    s = ExecutionStore(db)
    s.record_intent(
        signal_id="listing:AAA:1", client_order_id="entry-link",
        venue="bybit", symbol="AAAUSDT", side="Sell",
        requested_qty=4, route="ws", ts_ns=100,
    )
    s.record_fill(
        exec_id="entry", client_order_id="entry-link", exchange_order_id="open-order",
        symbol="AAAUSDT", side="Sell", price=100, qty=4,
        fee=0.4, fee_asset="USDT", ts_ns=200,
    )
    s.record_fill(
        exec_id="exit-1", client_order_id="", exchange_order_id="close-order",
        symbol="AAAUSDT", side="Buy", price=90, qty=3,
        fee=0.27, fee_asset="USDT", ts_ns=300,
        raw={"execPnl": "30"},
    )
    s.record_fill(
        exec_id="exit-2", client_order_id="", exchange_order_id="close-order",
        symbol="AAAUSDT", side="Buy", price=90, qty=1,
        fee=0.09, fee_asset="USDT", ts_ns=301,
        raw={"execPnl": "10"},
    )
    result = reconcile_trades(db)
    assert len(result) == 1
    trade = result[0]
    assert trade["status"] == "CLOSED"
    assert trade["entry_vwap"] == 100
    assert trade["exit_vwap"] == 90
    assert trade["realized_pnl"] == 40
    assert trade["fees"] == pytest.approx(0.76)
    assert trade["net_pnl"] == pytest.approx(39.24)


def test_partial_exit_remains_open(tmp_path):
    db = tmp_path / "execution.sqlite3"
    s = ExecutionStore(db)
    s.record_intent(signal_id="listing:A:1", client_order_id="l", venue="bybit", symbol="AUSDT", side="Sell", requested_qty=4, route="ws", ts_ns=1)
    s.record_fill(exec_id="e", client_order_id="l", exchange_order_id="o", symbol="AUSDT", side="Sell", price=100, qty=4, ts_ns=2)
    s.record_fill(exec_id="x", client_order_id="", exchange_order_id="c", symbol="AUSDT", side="Buy", price=90, qty=2, ts_ns=3, raw={"execPnl":"20"})
    trade = reconcile_trades(db)[0]
    assert trade["status"] == "PARTIALLY_CLOSED"
    assert trade["open_qty"] == 2
