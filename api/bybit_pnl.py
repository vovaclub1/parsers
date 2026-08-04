from __future__ import annotations

# ── bybit_pnl.py ──────────────────────────────────────────────────
# Поллер закрытого PnL по позициям Bybit (/v5/position/closed-pnl).
#
# Зачем: listing/delist парсеры работают по схеме «выстрелил и забыл» —
# открыли позицию, повесили нативный trailing+TP на бирже, и всё. Закрытие
# происходит движком Bybit, бот об этом не узнаёт. Чтобы знать PnL сделки,
# периодически тянем closed-pnl и матчим по symbol с недавно открытыми
# позициями.
#
# Использование (общее для обоих парсеров):
#   pnl = BybitClosedPnL(api_key, secret)
#   pnl.register(symbol="FLRUSDT", coin="FLR", opened_ts=..., on_pnl=cb)
#   threading.Thread(target=pnl.poll_loop, daemon=True).start()
# Когда позиция закроется, cb(coin, closed_pnl_usdt, exit_avg_price) вызовется
# один раз. Только Bybit — Gate-сделки сюда не регистрируются (pnl=None).
#
# Self-contained: подписывает свои запросы (HMAC-SHA256), не зависит от
# listing_api/delist_api. Тот же формат подписи, что в _sign() этих модулей.
# ─────────────────────────────────────────────────────────────────

import hashlib
import hmac
import json
import threading
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


_BASE_URL = "https://api.bybit.com"
_RECV_WINDOW = "5000"
_CLOSED_PNL_PATH = "/v5/position/closed-pnl"

# Сколько держать ожидающую позицию в реестре, прежде чем сдаться (позиция
# может висеть долго под trailing-stop). 24ч с запасом.
_PENDING_TTL = 24 * 3600
# Период опроса closed-pnl. Закрытие не время-критично (это пост-фактум
# статистика), поэтому редко — бережём rate-limit. 60с.
_POLL_INTERVAL = 60.0
_RECONCILE_HORIZON = 30 * 24 * 3600


def _epoch_ns(value) -> int:
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return time.time_ns()
    return raw * 1_000_000 if len(str(abs(raw))) <= 13 else raw


def authoritative_record(rec: dict, *, signal_id: str,
                         client_order_id: str) -> dict:
    """Normalize Bybit closed-pnl response; closedPnl is authoritative net."""
    symbol = str(rec.get("symbol") or "")
    close_id = str(rec.get("orderId") or rec.get("execId") or
                   f"{symbol}:{rec.get('updatedTime') or rec.get('createdTime')}")
    return {
        "venue": "bybit", "close_id": close_id,
        "signal_id": signal_id, "client_order_id": client_order_id,
        "symbol": symbol, "side": str(rec.get("side") or ""),
        "net_pnl": float(rec.get("closedPnl", 0) or 0),
        "fees": float(rec.get("openFee", 0) or 0) + float(rec.get("closeFee", 0) or 0),
        "funding": 0.0,
        "exit_price": float(rec.get("avgExitPrice", 0) or 0),
        "closed_ts_ns": _epoch_ns(rec.get("updatedTime") or rec.get("createdTime")),
        "raw": rec,
    }


class BybitClosedPnL:
    """Матчит закрытые позиции Bybit с открытыми нами и зовёт callback с PnL."""

    def __init__(self, api_key: str, secret_key: str,
                 poll_interval: float = _POLL_INTERVAL, execution_store=None) -> None:
        self._key = api_key or ""
        self._secret = (secret_key or "").encode()
        self._poll_interval = poll_interval
        self._execution_store = execution_store
        self._lock = threading.Lock()
        # symbol -> [{coin, opened_ts, lifecycle...}, ...]
        self._pending: dict[str, list[dict]] = {}
        self._session = requests.Session()
        self._session.headers.update({
            "X-BAPI-API-KEY":     self._key,
            "X-BAPI-RECV-WINDOW": _RECV_WINDOW,
        })

    @property
    def enabled(self) -> bool:
        return bool(self._key and self._secret)

    # ── регистрация открытой позиции ──────────────────────────────
    def register(self, symbol: str, coin: str, opened_ts: float,
                 on_pnl, *, signal_id: str = "", client_order_id: str = "",
                 on_record=None) -> None:
        """
        Регистрирует только что открытую Bybit-позицию для отслеживания.
        on_pnl(coin: str, pnl_usdt: float, exit_price: float) — вызовётся
        один раз когда позиция закроется.
        """
        if not self.enabled:
            return
        with self._lock:
            self._pending.setdefault(symbol, []).append({
                "coin": coin,
                "opened_ts": opened_ts,
                "on_pnl": on_pnl,
                "signal_id": signal_id,
                "client_order_id": client_order_id,
                "on_record": on_record,
            })

    # ── подпись и запрос ──────────────────────────────────────────
    def _signed_get(self, path: str, params: dict) -> dict:
        # Bybit V5 GET: sign = ts + api_key + recv_window + query_string.
        # ВАЖНО: query_string должен совпадать с тем, что реально уйдёт в URL.
        query = "&".join(f"{k}={v}" for k, v in params.items())
        ts = str(int(time.time() * 1000))
        sign_str = ts + self._key + _RECV_WINDOW + query
        sign = hmac.new(self._secret, sign_str.encode(), hashlib.sha256).hexdigest()
        resp = self._session.get(
            _BASE_URL + path,
            params=params,
            headers={"X-BAPI-TIMESTAMP": ts, "X-BAPI-SIGN": sign},
            timeout=5,
        )
        resp.raise_for_status()
        return _loads(resp.content)

    def _fetch_closed(self, symbol: str, start_ms: int) -> list[dict]:
        """Закрытые PnL-записи по symbol с момента start_ms."""
        try:
            data = self._signed_get(_CLOSED_PNL_PATH, {
                "category":  "linear",
                "symbol":    symbol,
                "startTime": start_ms,
                "limit":     50,
            })
        except Exception:  # noqa: BLE001
            return []
        if not isinstance(data, dict) or data.get("retCode") != 0:
            return []
        return ((data.get("result") or {}).get("list")) or []

    # ── основной цикл ─────────────────────────────────────────────
    def poll_loop(self) -> None:
        if not self.enabled:
            print("[BYBIT-PNL] нет ключей — поллер не стартует", flush=True)
            return
        print(f"[BYBIT-PNL] closed-pnl поллер запущен (интервал {self._poll_interval:.0f}с)",
              flush=True)
        while True:
            time.sleep(self._poll_interval)
            try:
                self._tick()
                self._reconcile_store()
            except Exception as e:  # noqa: BLE001
                print(f"[BYBIT-PNL] tick err: {e!r}", flush=True)

    def set_execution_store(self, store) -> None:
        self._execution_store = store

    def _reconcile_store(self) -> None:
        store = self._execution_store
        if store is None:
            return
        from research.closed_pnl_backfill import backfill_records
        cutoff = time.time_ns() - int(_RECONCILE_HORIZON * 1_000_000_000)
        for symbol in store.list_intent_symbols(venue="bybit", opened_after_ns=cutoff):
            first = store.earliest_intent_ts(venue="bybit", symbol=symbol)
            start_ms = int((first or cutoff) / 1_000_000)
            backfill_records(store, venue="bybit", symbol=symbol,
                             records=self._fetch_closed(symbol, start_ms))

    def _tick(self) -> None:
        now = time.time()
        with self._lock:
            items = list(self._pending.items())
        for symbol, regs in items:
            alive = [r for r in regs if now - r["opened_ts"] <= _PENDING_TTL]
            if not alive:
                with self._lock:
                    self._pending.pop(symbol, None)
                continue
            start_ms = int(min(r["opened_ts"] for r in alive) * 1000)
            recs = self._fetch_closed(symbol, start_ms)
            if not recs:
                with self._lock:
                    self._pending[symbol] = alive
                continue
            def _rec_ms(rec):
                try:
                    return int(rec.get("updatedTime") or rec.get("createdTime") or 0)
                except (TypeError, ValueError):
                    return 0
            available = sorted(recs, key=_rec_ms)
            remaining = []
            for info in sorted(alive, key=lambda x: x["opened_ts"]):
                opened_ms = int(info["opened_ts"] * 1000)
                match_idx = next((i for i, rec in enumerate(available)
                                  if _rec_ms(rec) >= opened_ms), None)
                if match_idx is None:
                    remaining.append(info)
                    continue
                rec = available.pop(match_idx)
                try:
                    closed_pnl = float(rec.get("closedPnl", 0))
                    exit_price = float(rec.get("avgExitPrice", 0) or 0)
                except (TypeError, ValueError):
                    remaining.append(info)
                    continue
                coin = info["coin"]
                on_record = info.get("on_record")
                if on_record is not None:
                    try:
                        on_record(authoritative_record(
                            rec, signal_id=info.get("signal_id", ""),
                            client_order_id=info.get("client_order_id", ""),
                        ))
                    except Exception as e:  # noqa: BLE001
                        print(f"[BYBIT-PNL] durable record err {coin}: {e!r}", flush=True)
                try:
                    info["on_pnl"](coin, closed_pnl, exit_price)
                except Exception as e:  # noqa: BLE001
                    print(f"[BYBIT-PNL] callback err {coin}: {e!r}", flush=True)
            with self._lock:
                if remaining:
                    self._pending[symbol] = remaining
                else:
                    self._pending.pop(symbol, None)
