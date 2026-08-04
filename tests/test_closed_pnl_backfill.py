import pytest

from storage.execution_store import ExecutionStore
from research.closed_pnl_backfill import backfill_records


def test_backfill_matches_exchange_records_to_intents(tmp_path):
    s = ExecutionStore(tmp_path / "e.db")
    s.record_intent(signal_id="listing:A:1", client_order_id="l", venue="bybit", symbol="AUSDT", side="Sell", requested_qty=1, route="ws", ts_ns=1_700_000_000_000_000_000)
    recs = [{"orderId":"c1","symbol":"AUSDT","side":"Sell","closedPnl":"2.5","avgExitPrice":"90","updatedTime":"1700000001000"}]
    n = backfill_records(s, venue="bybit", symbol="AUSDT", records=recs)
    assert n == 1
    assert s.get_authoritative_pnl()[0]["signal_id"] == "listing:A:1"
    assert backfill_records(s, venue="bybit", symbol="AUSDT", records=recs) == 0


def test_backfill_gate_matches_contract(tmp_path):
    s = ExecutionStore(tmp_path / "e.db")
    s.record_intent(signal_id="delisting:B:1", client_order_id="g", venue="gate", symbol="B_USDT", side="Buy", requested_qty=1, route="gate", ts_ns=1_700_000_000_000_000_000)
    recs=[{"id":"g1","contract":"B_USDT","side":"long","pnl_pnl":"3","pnl_fee":"-.2","pnl_fund":"-.1","time":1700000001}]
    assert backfill_records(s, venue="gate", symbol="B_USDT", records=recs) == 1
    assert s.get_authoritative_pnl()[0]["net_pnl"] == pytest.approx(2.7)
