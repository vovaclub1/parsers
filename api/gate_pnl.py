from __future__ import annotations

# ── gate_pnl.py ───────────────────────────────────────────────────
# Поллер закрытого (realized) PnL по позициям Gate.io USDT-futures.
# Зеркало api/bybit_pnl.py для Gate: listing/delist открывают позицию на Gate
# (когда Bybit недоступен/нет пары), вешают TP/SL, и закрытие делает движок
# Gate. Чтобы знать реальный PnL Gate-сделки (а не «н/д»), периодически тянем
# историю закрытий и матчим по contract с недавно открытыми позициями.
#
# Использование (как BybitClosedPnL):
#   pnl = GateClosedPnL(api_key, secret)
#   pnl.register(contract="FLR_USDT", coin="FLR", opened_ts=..., on_pnl=cb)
#   threading.Thread(target=pnl.poll_loop, daemon=True).start()
# cb(coin, pnl_usdt, exit_price) вызовётся один раз при закрытии.
#
# Эндпоинт: GET /api/v4/futures/usdt/position_close — история закрытий позиций
# (поле pnl / pnl_pnl+pnl_fee+pnl_fund — realized в USDT). Self-contained:
# подписывает свои запросы (HMAC-SHA512, тот же формат, что в gate_api._sign).
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


_BASE_URL = "https://api.gateio.ws"
_SETTLE = "usdt"
_CLOSE_PATH = "/api/v4/futures/%s/position_close" % _SETTLE

# Позиция может висеть долго под trailing — держим в реестре 24ч.
_PENDING_TTL = 24 * 3600
# Период опроса (closed-pnl не время-критичен — пост-фактум статистика).
_POLL_INTERVAL = 60.0


def _extract_pnl(rec: dict) -> float:
    """
    Realized PnL записи закрытия. Новый API разбивает на pnl_pnl (трейдинг),
    pnl_fee (комиссия), pnl_fund (фандинг) — суммируем для NET (сопоставимо с
    bybit closedPnl). Старый API — единое поле pnl.
    """
    if "pnl_pnl" in rec:
        total = 0.0
        for k in ("pnl_pnl", "pnl_fee", "pnl_fund"):
            try:
                total += float(rec.get(k, 0) or 0)
            except (TypeError, ValueError):
                pass
        return total
    try:
        return float(rec.get("pnl", 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def _extract_exit_price(rec: dict) -> float:
    """
    Средняя цена закрытия. Для закрытого лонга закрывающие сделки = шорт-сторона
    (short_price); для шорта — long_price. Поля могут отсутствовать → 0.
    """
    side = (rec.get("side") or "").lower()
    key = "short_price" if side == "long" else "long_price"
    try:
        return float(rec.get(key, 0) or 0)
    except (TypeError, ValueError):
        return 0.0


class GateClosedPnL:
    """Матчит закрытия Gate-позиций с открытыми нами и зовёт callback с PnL."""

    def __init__(self, api_key: str, secret_key: str,
                 poll_interval: float = _POLL_INTERVAL) -> None:
        self._key = api_key or ""
        self._secret = secret_key or ""
        self._poll_interval = poll_interval
        self._lock = threading.Lock()
        # contract -> [ {coin, opened_ts, on_pnl}, ... ] (список — не теряем
        # регистрации при повторном входе на тот же контракт)
        self._pending: dict[str, list] = {}
        self._last_warn_ts = 0.0
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    @property
    def enabled(self) -> bool:
        return bool(self._key and self._secret)

    # ── регистрация открытой позиции ──────────────────────────────
    def register(self, contract: str, coin: str, opened_ts: float,
                 on_pnl) -> None:
        """
        contract — Gate-формат "FLR_USDT". on_pnl(coin, pnl_usdt, exit_price)
        вызовется один раз когда позиция закроется.

        Несколько регистраций на один contract (одну монету могли заявить две
        биржи в пределах TTL → два входа на тот же Gate-контракт) НЕ
        перезатираются: храним список, при закрытии зовём все callback'и.
        """
        if not self.enabled:
            return
        with self._lock:
            self._pending.setdefault(contract, []).append({
                "coin": coin,
                "opened_ts": opened_ts,
                "on_pnl": on_pnl,
            })

    # ── подпись и запрос ──────────────────────────────────────────
    def _signed_get(self, path: str, query: str) -> list:
        # Gate v4: SIGN = HMAC-SHA512(method\npath\nquery\nSHA512(body)\nts).
        # query в подписи ДОЛЖЕН совпадать с тем, что уйдёт в URL.
        ts = str(int(time.time()))
        body_hash = hashlib.sha512(b"").hexdigest()
        msg = "\n".join(["GET", path, query, body_hash, ts])
        sig = hmac.new(self._secret.encode(), msg.encode(), hashlib.sha512).hexdigest()
        headers = {"KEY": self._key, "Timestamp": ts, "SIGN": sig}
        url = f"{_BASE_URL}{path}?{query}" if query else f"{_BASE_URL}{path}"
        resp = self._session.get(url, headers=headers, timeout=5)
        resp.raise_for_status()
        data = _loads(resp.content)
        if isinstance(data, list):
            return data
        # 2xx с не-list телом — почти всегда error-объект {"label":...}.
        self._warn_once(f"неожиданный ответ Gate: {str(data)[:160]}")
        return []

    def _warn_once(self, msg: str) -> None:
        """Rate-limited warn (раз в 5 мин): битый ключ/clock-skew/IP-whitelist
        иначе выглядят как «закрытий ещё нет» и через TTL все позиции тихо
        отваливаются без единой строки в логе."""
        now = time.time()
        if now - self._last_warn_ts >= 300.0:
            self._last_warn_ts = now
            print(f"[GATE-PNL] {msg}", flush=True)

    def _fetch_closed(self, contract: str, from_ts: int) -> list:
        """Записи закрытий по contract с момента from_ts (unix сек)."""
        try:
            query = "contract=%s&from=%d&limit=50" % (contract, from_ts)
            return self._signed_get(_CLOSE_PATH, query)
        except Exception as e:  # noqa: BLE001
            self._warn_once(f"fetch closed {contract} failed: {e!r}")
            return []

    # ── основной цикл ─────────────────────────────────────────────
    def poll_loop(self) -> None:
        if not self.enabled:
            print("[GATE-PNL] нет ключей — поллер не стартует", flush=True)
            return
        print(f"[GATE-PNL] closed-pnl поллер запущен (интервал {self._poll_interval:.0f}с)",
              flush=True)
        while True:
            time.sleep(self._poll_interval)
            try:
                self._tick()
            except Exception as e:  # noqa: BLE001
                print(f"[GATE-PNL] tick err: {e!r}", flush=True)

    def _tick(self) -> None:
        now = time.time()
        with self._lock:
            contracts = list(self._pending.keys())
        for contract in contracts:
            with self._lock:
                regs = list(self._pending.get(contract, ()))
            if not regs:
                continue
            # Снимаем просроченные регистрации (каждую по своему opened_ts).
            alive = [r for r in regs if now - r["opened_ts"] <= _PENDING_TTL]
            if not alive:
                with self._lock:
                    self._pending.pop(contract, None)
                continue
            if len(alive) != len(regs):
                with self._lock:
                    self._pending[contract] = alive
            from_ts = int(min(r["opened_ts"] for r in alive))
            recs = self._fetch_closed(contract, from_ts)
            if not recs:
                continue
            # Берём самое позднее закрытие после самого раннего открытия.
            best = None
            for rec in recs:
                try:
                    rt = int(rec.get("time", 0))
                except (TypeError, ValueError):
                    continue
                if rt < from_ts:
                    continue
                if best is None or rt >= int(best.get("time", 0)):
                    best = rec
            if best is None:
                continue
            pnl = _extract_pnl(best)
            exit_price = _extract_exit_price(best)
            # Закрытие net-позиции относится ко всем активным регистрациям —
            # снимаем контракт и зовём все callback'и.
            with self._lock:
                self._pending.pop(contract, None)
            for r in alive:
                try:
                    r["on_pnl"](r["coin"], pnl, exit_price)
                except Exception as e:  # noqa: BLE001
                    print(f"[GATE-PNL] callback err {r['coin']}: {e!r}", flush=True)
