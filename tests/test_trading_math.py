"""Тесты торговой математики и Telegram-логгера."""
from __future__ import annotations

import pytest

from api.delist_api import _round_qty
from tg.tg_logger import _escape_for_html


# ── округление количества до шага лота ────────────────────────────

@pytest.mark.parametrize(("qty", "step", "expected"), [
    (0.123456, 0.001, 0.123),
    (1234.9, 1.0, 1234.0),
    (0.00012345, 0.00001, 0.00012),
    (5.0, 0.1, 5.0),
    # 1e-05 в str() = '1e-05' — старый код давал precision=0 и обнулял qty.
    (0.000123, 1e-05, 0.00012),
])
def test_round_qty_floors_to_step(qty, step, expected):
    assert _round_qty(qty, step) == pytest.approx(expected)


def test_round_qty_never_rounds_up():
    """Округление вверх = ордер больше запрошенного = превышение риска."""
    for step in (0.001, 0.01, 0.1, 1.0):
        for raw in (0.999, 12.345, 7.77, 1000.001):
            assert _round_qty(raw, step) <= raw + 1e-12


def test_round_qty_rejects_zero_step():
    """step=0 приводил к ZeroDivisionError прямо в hot-path открытия позиции."""
    with pytest.raises((ValueError, ZeroDivisionError)):
        _round_qty(1.0, 0.0)


# ── HTML-экранирование для Telegram ───────────────────────────────

def test_escape_for_html_escapes_unbalanced_tags():
    """REGRESSION: обе ветки _escape_for_html возвращали msg без изменений.

    Telegram с parse_mode=HTML отвечает 400 на несбалансированный '<',
    из-за чего алерт о сделке молча терялся.
    """
    out = _escape_for_html("price < 5 & qty > 3")
    assert "<" not in out.replace("&lt;", "")
    assert "&lt;" in out
    assert "&gt;" in out
    assert "&amp;" in out


def test_escape_for_html_preserves_allowed_tags():
    """Наши собственные <b>/<i>/<code> должны выживать — на них строится вёрстка."""
    out = _escape_for_html("<b>DELIST SHORT</b> BTC")
    assert "<b>" in out
    assert "</b>" in out
    assert "DELIST SHORT" in out


def test_escape_for_html_escapes_stray_lt_but_keeps_bold():
    out = _escape_for_html("<b>BTC</b> price < 100")
    assert "<b>" in out
    assert "&lt; 100" in out


def test_escape_for_html_plain_text_unchanged():
    assert _escape_for_html("simple message") == "simple message"
