from __future__ import annotations

# ── klines.py ─────────────────────────────────────────────────────
# Публичные исторические свечи (klines) для дотягивания «хвоста» траектории
# цены задним числом (гибрид: рекордер ловит первые ~10 мин по 1с, остальное
# до 6ч берём свечами 1m). Публичные эндпоинты — подпись/ключи не нужны.
#
#   fetch_klines("bybit"|"gate", coin, start_ms, end_ms) -> [(ts_sec, close), ...]
#
# Bybit: GET /v5/market/kline (category=linear, interval=1, start/end в МС).
#        result.list = [[startMs, o, h, l, c, v, turnover], ...] (новые сверху).
# Gate:  GET /api/v4/futures/usdt/candlesticks (contract, from/to в СЕК, 1m).
#        список объектов {"t":sec, "c":close, ...}. limit нельзя с from/to.
# ─────────────────────────────────────────────────────────────────

import json
import time

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

_BYBIT = "https://api.bybit.com/v5/market/kline"
_GATE = "https://api.gateio.ws/api/v4/futures/usdt/candlesticks"

_session = requests.Session()
_session.headers.update({"Accept": "application/json"})

_last_warn_ts = 0.0


def _warn_once(msg: str) -> None:
    global _last_warn_ts
    now = time.time()
    if now - _last_warn_ts >= 300.0:
        _last_warn_ts = now
        print(f"[KLINES] {msg}", flush=True)


def _bybit(coin: str, start_ms: int, end_ms: int) -> list:
    params = {
        "category": "linear",
        "symbol": f"{coin}USDT",
        "interval": "1",
        "start": int(start_ms),
        "end": int(end_ms),
        "limit": 1000,
    }
    r = _session.get(_BYBIT, params=params, timeout=8)
    r.raise_for_status()
    data = _loads(r.content)
    if not isinstance(data, dict) or data.get("retCode") != 0:
        _warn_once(f"bybit {coin}: retMsg={(data or {}).get('retMsg')!r}")
        return []
    rows = ((data.get("result") or {}).get("list")) or []
    out = []
    for row in rows:
        try:
            out.append((int(row[0]) // 1000, float(row[4])))  # startMs→sec, close
        except (TypeError, ValueError, IndexError):
            continue
    return out


def _gate(coin: str, start_ms: int, end_ms: int) -> list:
    params = {
        "contract": f"{coin}_USDT",
        "from": int(start_ms // 1000),
        "to": int(end_ms // 1000),
        "interval": "1m",
    }
    r = _session.get(_GATE, params=params, timeout=8)
    r.raise_for_status()
    data = _loads(r.content)
    if not isinstance(data, list):
        _warn_once(f"gate {coin}: неожиданный ответ {str(data)[:120]}")
        return []
    out = []
    for c in data:
        try:
            out.append((int(c["t"]), float(c["c"])))  # сек, close
        except (TypeError, ValueError, KeyError):
            continue
    return out


def fetch_klines(venue: str, coin: str, start_ms: int, end_ms: int) -> list:
    """
    Публичные 1m-свечи [start_ms, end_ms] для venue (bybit|gate).
    Возвращает [(ts_sec, close), ...] по возрастанию времени. [] при ошибке.
    """
    v = (venue or "").lower()
    try:
        if v == "bybit":
            rows = _bybit(coin, start_ms, end_ms)
        elif v == "gate":
            rows = _gate(coin, start_ms, end_ms)
        else:
            return []
    except Exception as e:  # noqa: BLE001
        _warn_once(f"{v} {coin} fetch failed: {e!r}")
        return []
    rows.sort(key=lambda x: x[0])
    return rows
