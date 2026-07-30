"""Тесты дедупликации сигналов L1 (TTL) и её производительности.

L1 не даёт открыть дубль позиции, когда об одном листинге почти одновременно
сообщают несколько источников (TG-каналы, Tree of Alpha, CoinListing WS).

Проверка «занята ли монета» происходит в самом горячем месте — между приходом
сигнала и отправкой ордера, под общим _fired_lock. Раньше она была O(n)
(полный скан словаря), теперь O(1) через индекс _coin_claim_expiry.
"""
from __future__ import annotations

import time

import pytest

import parsers.parser_listing as pl


@pytest.fixture(autouse=True)
def _clean_state():
    with pl._fired_lock:
        pl._recent_signals.clear()
        pl._coin_claim_expiry.clear()
        pl._first_claim_ts.clear()
        pl._global_fired.clear()
        for bucket in pl._per_exchange_fired.values():
            bucket.clear()
    yield


def test_first_claim_wins():
    assert pl._try_claim("PRL", "TG:123") is True


def test_second_source_is_rejected_within_ttl():
    """First-wins: второй канал с той же монетой не должен открывать дубль."""
    assert pl._try_claim("PRL", "TG:123") is True
    assert pl._try_claim("PRL", "TOA-WS") is False
    assert pl._try_claim("PRL", "COINLISTING-WS") is False


def test_different_coins_do_not_block_each_other():
    assert pl._try_claim("PRL", "TG:1") is True
    assert pl._try_claim("GENIUS", "TG:1") is True
    assert pl._try_claim("OPG", "TOA-WS") is True


def test_claim_indexes_are_kept_in_sync():
    """_coin_claim_expiry — индекс над _recent_signals, они не должны разъезжаться."""
    pl._try_claim("PRL", "TG:1")
    assert ("PRL", "TG:1") in pl._recent_signals
    assert "PRL" in pl._coin_claim_expiry


def test_expired_claim_is_released_by_sweeper():
    """После TTL монета снова доступна — иначе повторный листинг не отработает."""
    pl._try_claim("PRL", "TG:1")
    assert pl._try_claim("PRL", "TOA-WS") is False

    # Симулируем истечение TTL.
    past = time.monotonic() - 1
    with pl._fired_lock:
        pl._recent_signals[("PRL", "TG:1")] = past
        pl._coin_claim_expiry["PRL"] = past

    assert pl._try_claim("PRL", "TOA-WS") is True


def test_stale_index_entry_cannot_block_coin_forever():
    """REGRESSION: если индекс не чистить синхронно, протухшая запись
    заблокировала бы монету навсегда."""
    with pl._fired_lock:
        pl._coin_claim_expiry["GHOST"] = time.monotonic() - 999
    assert pl._try_claim("GHOST", "TG:1") is True


def test_l2_blocks_recently_traded_coin():
    """L2: монету, по которой недавно открывали позицию, повторно не берём."""
    with pl._fired_lock:
        pl._global_fired["OLDCOIN"] = time.time()
    assert pl._try_claim("OLDCOIN", "TG:1") is False


def test_l2_releases_coin_after_ttl():
    """REGRESSION: L2 блокировал монету НАВСЕГДА.

    По каталогу Binance повторные делистинги одной монеты реальны:
      ARDR — 2026-03-10 и 2026-06-26 (108 дней)
      ALCX — 2026-03-13 и 2026-06-26 (105 дней)
    Второй сигнал молча пропускался.
    """
    with pl._fired_lock:
        pl._global_fired["ARDR"] = time.time() - (pl._L2_TTL + 1)
    assert pl._try_claim("ARDR", "TG:1") is True


def test_l2_per_exchange_also_expires():
    with pl._fired_lock:
        pl._per_exchange_fired["UPBIT"]["PRL"] = time.time() - (pl._L2_TTL + 1)
    assert pl._try_claim("PRL", "UPBIT") is True


def test_l2_per_exchange_blocks_same_exchange_within_ttl():
    with pl._fired_lock:
        pl._per_exchange_fired["UPBIT"]["PRL"] = time.time()
    assert pl._try_claim("PRL", "UPBIT") is False


def test_l2_per_exchange_does_not_block_other_exchange():
    """Листинг на Upbit не должен мешать поймать тот же тикер на Binance."""
    with pl._fired_lock:
        pl._per_exchange_fired["UPBIT"]["PRL"] = time.time()
    assert pl._try_claim("PRL", "BINANCE") is True


def test_claim_is_o1_and_not_linear_scan():
    """REGRESSION (perf): _try_claim не должен деградировать с ростом числа
    живых claim'ов — раньше он линейно сканировал весь словарь.

    Сравниваем время claim'а на пустом состоянии и при 5000 активных записях.
    O(1) даёт примерно одинаковое время; O(n) — кратный рост.
    """
    def _measure() -> float:
        best = float("inf")
        for i in range(200):
            coin = f"BENCH{i}"
            t0 = time.perf_counter()
            pl._try_claim(coin, "TG:bench")
            best = min(best, time.perf_counter() - t0)
        return best

    baseline = _measure()

    future = time.monotonic() + 3600
    with pl._fired_lock:
        for i in range(5000):
            pl._recent_signals[(f"FILL{i}", f"TG:{i}")] = future
            pl._coin_claim_expiry[f"FILL{i}"] = future

    loaded = _measure()

    # Порог с большим запасом: линейный скан 5000 записей дал бы
    # рост на порядки, O(1) остаётся в пределах шума планировщика.
    assert loaded < baseline * 20 + 5e-5, (
        f"claim деградирует под нагрузкой: {baseline*1e6:.1f}мкс -> {loaded*1e6:.1f}мкс"
    )
