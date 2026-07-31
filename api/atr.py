from __future__ import annotations

# ── atr.py ─────────────────────────────────────────────────────────
# Live ATR (Average True Range) для адаптивного трейлинга.
#
#   compute_atr(symbol, interval="1", period=14, min_candles=3) -> float | None
#
# Источник — публичный Bybit V5 /v5/market/kline (auth не нужна).
# True Range каждой свечи: max(H-L, |H-prevC|, |L-prevC|).
# ATR = простое среднее TR за `period` свечей (Wilder vs SMA — для коротких
# окон, которые мы используем на листингах (3-7 свечей), разница минимальна;
# SMA проще и не требует разогрева EMA).
#
# Используется в:
#   - api/listing_api.py :: _set_tp_sl_bybit (LONG trailing)
#   - api/delist_api.py  :: _set_tp_sl_bybit_short (SHORT trailing)
#
# Кеш: in-memory 30s TTL по ключу (symbol, interval, period). Сильно
# защищает от повторных вызовов в верификационном retry-цикле.
#
# Robustness: возвращает None при HTTP-ошибке / retCode!=0 / нехватке
# свечей / любом исключении. Вызывающий код обязан иметь pct-fallback.
# ─────────────────────────────────────────────────────────────────

import json
import time
from typing import Optional

import requests

try:
    import orjson as _orjson  # type: ignore[import-not-found]
    def _loads(b):
        return _orjson.loads(b)
except ImportError:
    def _loads(b):
        if isinstance(b, (bytes, bytearray)):
            b = b.decode()
        return json.loads(b)


_BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"

_session = requests.Session()
_session.headers.update({"Accept": "application/json"})

# (symbol, interval, period) -> (atr, ts)
_ATR_CACHE: dict[tuple[str, str, int], tuple[float, float]] = {}
_ATR_CACHE_TTL = 30.0

_last_warn_ts = 0.0


def _warn_once(msg: str) -> None:
    global _last_warn_ts
    now = time.time()
    if now - _last_warn_ts >= 300.0:
        _last_warn_ts = now
        print(f"[ATR] {msg}", flush=True)


def compute_atr(
    symbol: str,
    interval: str = "1",
    period: int = 14,
    min_candles: int = 3,
    timeout: float = 2.0,
) -> Optional[float]:
    """
    ATR (Average True Range) за `period` свечей таймфрейма `interval` (минуты
    как str: "1", "3", "5", "15", "60", "240"; "D"/"W"/"M" — день/неделя/месяц).
    Возвращает float (цена-дистанция) либо None.

    min_candles: минимум фактически полученных TR для расчёта. Если рынок
    свежелистинговый и Bybit вернул меньше — None (вызывающий должен
    fall-back'ить на фикс-%).
    """
    if not symbol or period < 1 or min_candles < 1:
        return None

    key = (symbol, interval, period)
    now = time.time()
    cached = _ATR_CACHE.get(key)
    if cached is not None and (now - cached[1]) < _ATR_CACHE_TTL:
        return cached[0]

    try:
        # Нужно period+1 свечей: первая даёт prevClose, дальше period TR.
        params = {
            "category": "linear",
            "symbol":   symbol,
            "interval": str(interval),
            "limit":    int(period) + 1,
        }
        r = _session.get(_BYBIT_KLINE_URL, params=params, timeout=timeout)
        r.raise_for_status()
        data = _loads(r.content)
    except Exception as e:  # noqa: BLE001
        _warn_once(f"{symbol} {interval}m kline fetch failed: {e!r}")
        return None

    if not isinstance(data, dict) or data.get("retCode") != 0:
        _warn_once(f"{symbol} retMsg={(data or {}).get('retMsg')!r}")
        return None

    rows = ((data.get("result") or {}).get("list")) or []
    # Bybit отдаёт новые сверху — разворачиваем в хронологический порядок.
    rows = list(reversed(rows))

    if len(rows) < min_candles + 1:
        # Свечей физически мало (свежий контракт) — не считаем.
        return None

    trs: list[float] = []
    try:
        prev_close = float(rows[0][4])
    except (TypeError, ValueError, IndexError):
        return None

    for row in rows[1:]:
        try:
            high = float(row[2])
            low = float(row[3])
            close = float(row[4])
        except (TypeError, ValueError, IndexError):
            continue
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        if tr > 0:
            trs.append(tr)
        prev_close = close

    if len(trs) < min_candles:
        return None

    atr = sum(trs) / len(trs)
    if atr <= 0:
        return None

    _ATR_CACHE[key] = (atr, now)
    return atr


def live_sample_atr_frac(
    get_price_fn,
    coin: str,
    entry: float,
    samples: int = 30,
    period: float = 1.0,
    min_valid: int = 5,
) -> Optional[tuple[float, int, float, float]]:
    """
    ТОЧНЫЙ порт tg/exit_strategies.py:_atr_frac(pts[:warmup_n], entry) в live-режиме.

    Читает get_price_fn(coin) `samples` раз с интервалом `period` сек (default
    30 × 1сек = 30 sec warmup — как симулятор пишет через recorder на 1-сек
    cadence). Собирает валидные (non-None) цены, считает atr_frac по формуле
    симулятора: mean(|price_i - price_{i-1}|) / entry.

    Возвращает (atr_frac, n_used, peak, trough) или None если len(valid) < min_valid.

    ВНИМАНИЕ: блокирует ~samples*period сек. Вызывать в фоновом потоке!
    Пропущенные (None) сэмплы не считаются "тик" — попытки продолжаются до
    samples*2 итераций пока не наберётся достаточно валидных.
    """
    if entry <= 0 or samples < 2 or period <= 0 or min_valid < 2:
        return None

    valid: list[float] = []
    max_attempts = samples * 2
    attempts = 0
    while len(valid) < samples and attempts < max_attempts:
        try:
            p = get_price_fn(coin)
        except Exception:  # noqa: BLE001
            p = None
        if p:
            try:
                pf = float(p)
                if pf > 0:
                    valid.append(pf)
            except (TypeError, ValueError):
                pass
        attempts += 1
        time.sleep(period)

    if len(valid) < min_valid:
        return None

    diffs = [abs(valid[i] - valid[i - 1]) for i in range(1, len(valid))]
    if not diffs:
        return None

    atr_frac = (sum(diffs) / len(diffs)) / entry
    if atr_frac <= 0:
        # Всё те же цены (кэш ни разу не рефрешнулся, ноль движения) —
        # 0 volatility, вызывающий код сам решит fallback.
        return None

    return atr_frac, len(valid), max(valid), min(valid)


def clamp_distance(
    raw_distance: float,
    base_price: float,
    min_pct: float,
    max_pct: float,
) -> float:
    """
    Зажимает price-distance в коридор [base*min_pct, base*max_pct]. Защита
    от вырожденных ATR (0 или гигантских) и нулевого base.
    """
    if base_price <= 0:
        return raw_distance
    lo = base_price * min_pct
    hi = base_price * max_pct
    if hi < lo:
        lo, hi = hi, lo
    return max(lo, min(hi, raw_distance))
