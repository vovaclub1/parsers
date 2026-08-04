from storage.execution_store import ExecutionStore


def test_authoritative_pnl_is_idempotent_and_queryable(tmp_path):
    s = ExecutionStore(tmp_path / "e.db")
    kwargs = dict(
        venue="bybit", close_id="close-1", signal_id="listing:A:1",
        client_order_id="link-1", symbol="AUSDT", side="Sell",
        net_pnl=1.23, fees=-0.2, funding=0.03, exit_price=90,
        closed_ts_ns=100, raw={"closedPnl":"1.23"},
    )
    assert s.record_authoritative_pnl(**kwargs) is True
    assert s.record_authoritative_pnl(**kwargs) is False
    rows = s.get_authoritative_pnl()
    assert len(rows) == 1
    assert rows[0]["venue"] == "bybit"
    assert rows[0]["net_pnl"] == 1.23


def test_find_recent_intent_by_venue_and_symbol(tmp_path):
    s = ExecutionStore(tmp_path / "e.db")
    s.record_intent(signal_id="old", client_order_id="l1", venue="bybit", symbol="AUSDT", side="Sell", requested_qty=1, route="ws", ts_ns=100)
    s.record_intent(signal_id="new", client_order_id="l2", venue="bybit", symbol="AUSDT", side="Sell", requested_qty=1, route="ws", ts_ns=200)
    row = s.find_recent_intent(venue="bybit", symbol="AUSDT", opened_after_ns=150)
    assert row["signal_id"] == "new"
    assert row["client_order_id"] == "l2"
