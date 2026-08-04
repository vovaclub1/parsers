import json

from storage.execution_store import ExecutionStore
from tools.production_report import build_production_report


def test_production_report_combines_execution_tca_and_reconciliation(tmp_path):
    state = tmp_path
    db = state / "execution.sqlite3"
    s = ExecutionStore(db)
    s.record_intent(signal_id="listing:A:1", client_order_id="l", venue="bybit", symbol="AUSDT", side="Sell", requested_qty=1, route="ws", ts_ns=1)
    s.record_market_snapshot(signal_id="listing:A:1", client_order_id="l", venue="bybit", symbol="AUSDT", stage="send", bid=99, ask=101, ts_ns=1)
    s.record_fill(exec_id="e", client_order_id="l", exchange_order_id="o", symbol="AUSDT", side="Sell", price=99, qty=1, fee=.1, ts_ns=2)
    s.record_fill(exec_id="x", client_order_id="", exchange_order_id="c", symbol="AUSDT", side="Buy", price=90, qty=1, fee=.1, ts_ns=3, raw={"execPnl":"9"})
    s.record_authoritative_pnl(venue="bybit", close_id="b1", signal_id="listing:A:1", client_order_id="l", symbol="AUSDT", side="Sell", net_pnl=8.75, closed_ts_ns=4)
    s.record_authoritative_pnl(venue="gate", close_id="g1", signal_id="delisting:B:1", client_order_id="gl", symbol="B_USDT", side="long", net_pnl=1.25, fees=-.2, funding=-.1, closed_ts_ns=5)
    (state / "source_stats.json").write_text(json.dumps({"first_wins":{"TOA-WS":5}}))
    report = build_production_report(state)
    assert report["database"]["integrity"] == "ok"
    assert report["execution"]["orders"] == 1
    assert report["execution"]["fills"] == 2
    assert report["authoritative_pnl"]["closed"] == 2
    assert report["authoritative_pnl"]["net_pnl"] == 10
    assert report["authoritative_pnl"]["by_venue"]["bybit"]["net_pnl"] == 8.75
    assert report["authoritative_pnl"]["by_venue"]["gate"]["net_pnl"] == 1.25
    assert report["execution_diagnostics"]["closed"] == 1
    assert report["execution_diagnostics"]["sum_exec_pnl"] == 9
    assert report["execution_diagnostics"]["recorded_fees"] == .2
    assert report["execution_diagnostics"]["exec_pnl_minus_fees"] == 8.8
    assert report["sources"]["first_wins"]["TOA-WS"] == 5
