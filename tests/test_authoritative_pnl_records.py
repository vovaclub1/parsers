import pytest

from api.bybit_pnl import authoritative_record as bybit_record
from api.gate_pnl import authoritative_record as gate_record


def test_bybit_authoritative_record_uses_closed_pnl():
    row = bybit_record(
        {"orderId":"close-1", "closedPnl":"1.23", "avgExitPrice":"90",
         "updatedTime":"1700000000123", "side":"Sell", "symbol":"AUSDT"},
        signal_id="listing:A:1", client_order_id="l",
    )
    assert row["venue"] == "bybit"
    assert row["close_id"] == "close-1"
    assert row["net_pnl"] == 1.23
    assert row["closed_ts_ns"] == 1700000000123_000_000


def test_gate_authoritative_record_sums_pnl_fee_and_funding():
    row = gate_record(
        {"id":"42", "contract":"A_USDT", "side":"short",
         "pnl_pnl":"2", "pnl_fee":"-0.3", "pnl_fund":"-0.1",
         "long_price":"90", "time":1700000000},
        signal_id="listing:A:1", client_order_id="gate-l",
    )
    assert row["venue"] == "gate"
    assert row["close_id"] == "42"
    assert row["net_pnl"] == pytest.approx(1.6)
    assert row["fees"] == -0.3
    assert row["funding"] == -0.1
