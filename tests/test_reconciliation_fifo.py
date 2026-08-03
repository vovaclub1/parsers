from storage.execution_store import ExecutionStore
from research.reconciliation import reconcile_trades


def test_exit_fill_is_not_reused_when_entries_overlap(tmp_path):
    db = tmp_path / "e.db"; s = ExecutionStore(db)
    for n, ts in [(1, 100), (2, 110)]:
        link=f"l{n}"
        s.record_intent(signal_id=f"listing:A:{n}", client_order_id=link, venue="bybit", symbol="AUSDT", side="Sell", requested_qty=1, route="ws", ts_ns=ts)
        s.record_fill(exec_id=f"entry{n}", client_order_id=link, exchange_order_id=f"o{n}", symbol="AUSDT", side="Sell", price=100, qty=1, ts_ns=ts+1)
    s.record_fill(exec_id="exit1", client_order_id="", exchange_order_id="c1", symbol="AUSDT", side="Buy", price=90, qty=1, ts_ns=200, raw={"execPnl":"10"})
    s.record_fill(exec_id="exit2", client_order_id="", exchange_order_id="c2", symbol="AUSDT", side="Buy", price=80, qty=1, ts_ns=201, raw={"execPnl":"20"})
    trades = reconcile_trades(db)
    assert [x["exit_vwap"] for x in trades] == [90, 80]
    assert sum(x["exit_qty"] for x in trades) == 2
