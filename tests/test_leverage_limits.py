"""Тесты биржевых лимитов инструмента: maxLeverage и minOrderQty.

Проверено на живом Bybit API (2026-07-30): из ~773 linear-USDT инструментов
у 35 штук maxLeverage = 5, а не 10. Код считал объём под жёстко зашитое
LEVERAGE=10, из-за чего ордер требовал вдвое больше маржи, чем доступно,
и Bybit отклонял его с retCode 110007 — сигнал делистинга терялся.
"""
from __future__ import annotations

import pytest

from api import delist_api
from api.delist_api import LEVERAGE, effective_leverage, min_order_qty


@pytest.fixture(autouse=True)
def _clean_caches(monkeypatch):
    monkeypatch.setattr(delist_api, "_max_leverage_cache", {}, raising=False)
    monkeypatch.setattr(delist_api, "_min_qty_cache", {}, raising=False)


# ── эффективное плечо ─────────────────────────────────────────────

def test_leverage_capped_by_instrument_limit():
    """REGRESSION: инструмент с maxLeverage=5 не должен считаться под 10x."""
    delist_api._max_leverage_cache["AGI"] = 5.0
    assert effective_leverage("AGI") == 5.0


def test_leverage_uses_config_value_when_limit_is_higher():
    delist_api._max_leverage_cache["BTC"] = 100.0
    assert effective_leverage("BTC") == float(LEVERAGE)


def test_leverage_falls_back_to_config_when_unknown():
    """Нет данных по инструменту — поведение как раньше, без сюрпризов."""
    assert effective_leverage("NEWCOIN") == float(LEVERAGE)


@pytest.mark.parametrize("bad", [0.0, -1.0])
def test_leverage_ignores_nonsense_limits(bad):
    delist_api._max_leverage_cache["WEIRD"] = bad
    assert effective_leverage("WEIRD") == float(LEVERAGE)


def test_capped_leverage_reduces_position_size():
    """Ключевое следствие: при плече 5 объём вдвое меньше, чем при 10."""
    delist_api._max_leverage_cache["AGI"] = 5.0
    margin, price = 10.0, 2.0

    qty_capped = (margin / price) * effective_leverage("AGI")
    qty_naive = (margin / price) * LEVERAGE

    assert qty_capped == pytest.approx(25.0)
    assert qty_naive == pytest.approx(50.0)
    assert qty_capped < qty_naive


# ── минимальный размер ордера ─────────────────────────────────────

def test_min_order_qty_returns_cached_value():
    delist_api._min_qty_cache["BTC"] = 0.001
    assert min_order_qty("BTC") == 0.001


def test_min_order_qty_defaults_to_zero_when_unknown():
    """0.0 = «лимит неизвестен», проверка на минимум не применяется."""
    assert min_order_qty("NEWCOIN") == 0.0


def test_preload_populates_leverage_and_min_qty(monkeypatch):
    """preload_lot_steps обязан забирать maxLeverage/minOrderQty из того же
    ответа instruments-info — без дополнительных сетевых запросов."""
    fake = {
        "result": {
            "list": [
                {
                    "symbol": "AGIUSDT",
                    "lotSizeFilter": {"qtyStep": "1", "minOrderQty": "1"},
                    "leverageFilter": {"maxLeverage": "5.00"},
                },
                {
                    "symbol": "BTCUSDT",
                    "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
                    "leverageFilter": {"maxLeverage": "100.00"},
                },
            ]
        }
    }
    monkeypatch.setattr(delist_api, "_get", lambda *a, **kw: fake)
    delist_api.preload_lot_steps()

    assert delist_api._max_leverage_cache["AGI"] == 5.0
    assert delist_api._max_leverage_cache["BTC"] == 100.0
    assert delist_api._min_qty_cache["BTC"] == 0.001
    assert effective_leverage("AGI") == 5.0
    assert effective_leverage("BTC") == float(LEVERAGE)


def test_preload_survives_malformed_instrument(monkeypatch):
    """Битая запись не должна ронять предзагрузку целиком."""
    fake = {
        "result": {
            "list": [
                {"symbol": "BADUSDT", "lotSizeFilter": {}},
                {"symbol": "NOLOTUSDT"},
                {
                    "symbol": "ETHUSDT",
                    "lotSizeFilter": {"qtyStep": "0.01", "minOrderQty": "0.01"},
                    "leverageFilter": {"maxLeverage": "50"},
                },
            ]
        }
    }
    monkeypatch.setattr(delist_api, "_get", lambda *a, **kw: fake)
    delist_api.preload_lot_steps()

    assert delist_api._lot_step_cache.get("ETH") == 0.01
    assert "BAD" not in delist_api._max_leverage_cache
