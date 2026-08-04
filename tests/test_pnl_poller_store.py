from api.bybit_pnl import BybitClosedPnL
from api.gate_pnl import GateClosedPnL


def test_bybit_tick_emits_authoritative_record(monkeypatch):
    p = BybitClosedPnL("k", "s", poll_interval=1)
    got=[]
    p.register("AUSDT", "A", 1700000000, lambda *a: None,
               signal_id="listing:A:1", client_order_id="l", on_record=got.append)
    monkeypatch.setattr(p, "_fetch_closed", lambda *a: [{"orderId":"c","symbol":"AUSDT","closedPnl":"1.2","avgExitPrice":"90","updatedTime":"1700000001000"}])
    monkeypatch.setattr("api.bybit_pnl.time.time", lambda: 1700000002)
    p._tick()
    assert got[0]["net_pnl"] == 1.2
    assert got[0]["signal_id"] == "listing:A:1"


def test_gate_tick_emits_authoritative_record(monkeypatch):
    p = GateClosedPnL("k", "s", poll_interval=1)
    got=[]
    p.register("A_USDT", "A", 1700000000, lambda *a: None,
               signal_id="listing:A:1", client_order_id="g", on_record=got.append)
    monkeypatch.setattr(p, "_fetch_closed", lambda *a: [{"id":"c","contract":"A_USDT","side":"short","pnl_pnl":"2","pnl_fee":"-.2","pnl_fund":"-.1","long_price":"90","time":1700000001}])
    monkeypatch.setattr("api.gate_pnl.time.time", lambda: 1700000002)
    p._tick()
    assert got[0]["net_pnl"] == 1.7
    assert got[0]["signal_id"] == "listing:A:1"
