"""Тесты извлечения тикеров: find_pairs (делистинг) и find_listing_pairs (листинг).

Все кейсы взяты из РЕАЛЬНЫХ заголовков Binance announcement API
(catalogId=161 «Delisting» и catalogId=48 «New Cryptocurrency Listing»),
снятых 2026-07-30.
"""
from __future__ import annotations

import pytest

from api import delist_api
from api.delist_api import EXCLUDED_TOKENS, find_pairs
from api.listing_api import find_listing_pairs


@pytest.fixture(autouse=True)
def _known_coins(monkeypatch):
    """Подменяем known_coins — обычно его наполняет price_updater из сети."""
    coins = {
        "BTC", "ETH", "ALCX", "ARDR", "NFP", "POND", "TST", "IOTX", "HOT",
        "THE", "AERGO", "IP", "ACT", "BLUR", "PIVX", "QKC", "AERO", "AT",
        "OPEN", "ORDER", "CROSS", "APR", "IN", "TAG", "BE", "COIN", "BOT",
        "NOT", "ON", "ALL", "AEUR",
    }
    monkeypatch.setattr(delist_api, "known_coins", coins, raising=False)
    return coins


# ── делистинг ─────────────────────────────────────────────────────

@pytest.mark.parametrize(("title", "expected"), [
    (
        "Binance Will Delist ALCX, ARDR, NFP, POND on 2026-07-10",
        {"ALCX", "ARDR", "NFP", "POND"},
    ),
    (
        "Binance Margin And Loan Will Delist TST & IOTX on 2026-07-10",
        {"TST", "IOTX"},
    ),
    (
        "Binance Futures Will Delist USDⓈ-M AERGOUSDT Perpetual Contract (2026-07-24)",
        {"AERGO"},
    ),
    (
        "Binance Will Extend the Monitoring Tag to Include ACT, BLUR, PIVX & QKC on 2026-06-18",
        {"ACT", "BLUR", "PIVX", "QKC"},
    ),
])
def test_find_pairs_real_binance_titles(title, expected):
    assert set(find_pairs(title)) == expected


def test_find_pairs_does_not_drop_ticker_named_like_english_word():
    """REGRESSION: 'THE' (Thena) — реальный перп на Bybit и Binance Futures.

    Он лежал в EXCLUDED_TOKENS, поэтому из сигнала
    'Will Delist HOT, THE' извлекался только HOT — половина сигнала терялась.
    """
    got = set(find_pairs("Binance Margin And Loan Will Delist HOT, THE on 2026-07-03"))
    assert got == {"HOT", "THE"}


def test_excluded_tokens_do_not_shadow_real_tickers():
    """REGRESSION: стоп-лист не должен содержать реально торгуемых тикеров.

    Каждый такой токен — это делистинг/листинг, который бот молча пропустит.
    Список ниже проверен по живым API Bybit / Binance Futures / Upbit / Bithumb.
    """
    real_tickers = {
        "THE", "AT", "OPEN", "ORDER", "CROSS", "APR", "IN", "TAG",
        "BE", "COIN", "BOT", "NOT", "ON", "ALL", "NFT", "MAY",
    }
    collisions = EXCLUDED_TOKENS & real_tickers
    assert collisions == set(), (
        f"EXCLUDED_TOKENS перекрывает реальные тикеры: {sorted(collisions)}"
    )


def test_find_pairs_ignores_margin_pair_notice():
    """«Removal of Margin Trading Pairs» — не делистинг монеты, шортить нечего."""
    assert find_pairs("Notice of Removal of Margin Trading Pairs - 2026-07-30") == []


def test_find_pairs_empty_on_generic_notice():
    assert find_pairs("Notice of Removal of Spot Trading Pairs - 2026-07-31") == []


# ── листинг ───────────────────────────────────────────────────────

@pytest.mark.parametrize(("title", "expected"), [
    ("Binance Will List Aerodrome (AERO) with Seed Tag Applied", {"AERO"}),
    ("[UPBIT] $PRL listed on Upbit (KRW, BTC, USDT)", {"PRL"}),
    (
        "Binance Futures Will Launch POPMARTUSDT USDⓈ-Margined Perpetual Contract",
        {"POPMART"},
    ),
])
def test_find_listing_pairs_real_titles(title, expected):
    assert set(find_listing_pairs(title)) == expected


def test_find_listing_pairs_ignores_delisting():
    assert find_listing_pairs("Binance Will Delist ALCX, ARDR on 2026-07-10") == []
