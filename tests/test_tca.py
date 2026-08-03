import pytest

from research.tca import bbo_metrics, sweep_book, implementation_shortfall_bps, markout_bps


def test_bbo_metrics():
    m = bbo_metrics(99.0, 101.0)
    assert m["mid"] == 100.0
    assert m["spread_bps"] == pytest.approx(200.0)


def test_sweep_buy_consumes_asks_and_returns_vwap():
    result = sweep_book([[101, 1], [102, 2], [103, 10]], notional=250, side="buy")
    assert result["filled_notional"] == pytest.approx(250)
    assert result["filled_qty"] == pytest.approx(1 + 149 / 102)
    assert result["vwap"] == pytest.approx(250 / (1 + 149 / 102))
    assert result["complete"] is True
    assert result["worst_price"] == 102


def test_sweep_sell_consumes_bids():
    result = sweep_book([[99, 1], [98, 2]], notional=200, side="sell")
    assert result["complete"] is True
    assert result["worst_price"] == 98
    assert result["vwap"] < 99


def test_sweep_reports_insufficient_depth():
    result = sweep_book([[100, 1]], notional=200, side="buy")
    assert result["complete"] is False
    assert result["filled_notional"] == 100


def test_implementation_shortfall_is_cost_positive_for_both_sides():
    assert implementation_shortfall_bps("buy", 100, 101, fee_bps=5) == pytest.approx(105)
    assert implementation_shortfall_bps("sell", 100, 99, fee_bps=5) == pytest.approx(105)


def test_markout_uses_position_direction():
    assert markout_bps("buy", fill_price=100, future_mid=102) == pytest.approx(200)
    assert markout_bps("sell", fill_price=100, future_mid=98) == pytest.approx(200)
