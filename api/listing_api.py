from __future__ import annotations

# ── listing_api.py ────────────────────────────────────────────────
# Bybit (приоритет) + Gate.io (fallback) для лонгов при листингах.
# Если токена нет на Bybit — открываем на Gate.io.
# ─────────────────────────────────────────────────────────────────

import os
import re
import threading
import time

from api.delist_api import (
    _post,
    _post_http2,             # FIX: HTTP/2 client для background TP/SL
    _trading_stop_settle,    # FIX 2026-06-06: ретрай trading-stop на осёдку
    post_order,              # FIX-10: публичный алиас вместо _post_order
    new_order_link_id,       # FIX-10: публичный алиас вместо _new_order_link_id
    _get_qty_step,
    _round_qty,
    get_price,
    known_coins,
    EXCLUDED_TOKENS,
    price_updater,
    warmup_bybit_connection,
    start_bybit_heartbeat,   # FIX: TLS pool heartbeat
    preload_lot_steps,
    QtyStepUnavailable,
    LEVERAGE,
)
from api.gate_api import (
    gate_get_price,
    gate_open_long,
    gate_set_tp_sl_long,
    gate_price_updater,      # noqa: F401  — реэкспорт
    gate_preload_lot_steps,  # noqa: F401  — реэкспорт
    warmup_gate_connection,  # noqa: F401  — реэкспорт
)

# FIX: вынесли импорт из хот-функции market_open_long на модульный уровень.
# FIX 2026-06-05: перешли с fire-and-forget на ack-вариант — нужно знать
# retCode для ретраев на 30208 (price-cap на быстром листинге). _ws_place_order_ack
# ждёт ack (~10-70мс RTT), но это цена за точность: иначе отвергнутые ордера
# логировались как успешные и позиция по факту не открывалась.
try:
    from api.bybit_ws_trade import (
        place_order_ws_fast as _ws_place_order,
        place_order_ws_ack as _ws_place_order_ack,
        WSOrderRejected,
    )
except Exception as _ws_import_exc:  # noqa: BLE001 — graceful
    print(f"[BYBIT-WS] модуль не подгружен: {_ws_import_exc!r} — будет только REST")
    def _ws_place_order(args: dict, _warmup_mode: bool = False) -> dict | None:  # type: ignore[misc]
        return None
    def _ws_place_order_ack(args: dict, timeout: float = 1.5) -> dict | None:  # type: ignore[misc]
        return None
    class WSOrderRejected(Exception):  # type: ignore[no-redef]
        """Stub если bybit_ws_trade не подгрузился — никогда не raise-нется."""
        pass

__all__ = [
    "market_open_long",
    "set_tp_sl_long",
    "calculate_margin_for_listing",
    "find_listing_pairs",
    "price_updater",
    "gate_price_updater",
    "warmup_bybit_connection",
    "start_bybit_heartbeat",
    "preload_lot_steps",
    "gate_preload_lot_steps",
    "warmup_gate_connection",
    "get_price",
]


# ── Параметры TP/SL (FIX 2026-06-04: новая стратегия) ────────────
# Диагностика closed-pnl/execution-list показала: связка takeProfit +
# trailingStop + tpslMode:Partial в одном /v5/position/trading-stop почти
# не срабатывала (14 закрытий, только 1 через PartialTakeProfit, остальные
# UNKNOWN = ручное закрытие). Новая схема: ТОЛЬКО trailing в Full-режиме,
# без фиксированного TP. Все стопы максимум 1%.
_SL_MULT       = 0.99    # стоп-лосс: -1% от entry (было -8%)
_TRAIL_PCT     = 0.01    # трейлинг-дистанция: 1% (было 3.5%)
_TRAIL_ACT     = 1.01    # активация трейлинга: +1% от entry (было +3.5%)
# Robinhood exit: тугой трейлинг + страховочный стоп (см. set_robinhood_exit).
# FIX 2026-06-09: было time-based (партиалы 30/30/40 + БУ) → теперь чистый
# трейлинг 0.75% + страховочный SL 0.5% на всю позицию.
_RH_SL_MULT    = 0.995    # страховочный SL: -0.5% от entry
_RH_TRAIL_PCT  = 0.0075   # трейлинг-дистанция: 0.75%
_RH_TRAIL_ACT  = 1.0075   # активация трейлинга: +0.75% от entry

# FIX 2026-06-05: ретраи на price-cap reject (30208 "order price higher than
# maximum buying price") — на быстром листинге цена уходит за cap Bybit.
# 10 попыток × ~7мс. Заходим всегда (без лимита проскальзывания, по решению).
_PRICE_CAP_RETCODES = {30208, 30209}  # 30208 buy-cap, 30209 sell-floor (шорт)
_ORDER_RETRIES = int(os.getenv("LISTING_ORDER_RETRIES", "10"))
_ORDER_RETRY_SLEEP = float(os.getenv("LISTING_ORDER_RETRY_MS", "7")) / 1000.0


# ── Маржа ─────────────────────────────────────────────────────────

def calculate_margin_for_listing() -> float:
    """
    Рассчитывает размер маржи для открытия позиции при листинге.
    :return: float - размер маржи в USDT.
    """
    balance = 100
    return balance * 0.14


# ── Открытие лонга ────────────────────────────────────────────────

# Биржа на которой открыта позиция — нужна для set_tp_sl_long
_last_exchange: dict[str, str] = {}  # {ticker: "bybit" | "gate"}
_last_exchange_lock = threading.Lock()


def market_open_long(ticker_name: str, usdt_amount: float) -> tuple[float, float]:
    """
    Открывает рыночный лонг:
      1. Bybit (приоритет — ниже комиссия)
      2. Gate.io (fallback — если токена нет на Bybit)
    :param ticker_name: str - тикер монеты (например "PRL").
    :param usdt_amount: float - маржа в USDT.
    :return: tuple[float, float] - (количество, цена входа), (0, 0) если нигде нет.
    """
    bybit_price = get_price(ticker_name)

    # FIX: явная проверка > 0 (защита от деления на ноль и отрицательных цен)
    if bybit_price and bybit_price > 0:
        symbol  = f"{ticker_name}USDT"
        raw_qty = (usdt_amount / bybit_price) * LEVERAGE   # FIX: магия → константа

        try:
            step = _get_qty_step(symbol)
        except QtyStepUnavailable as e:
            # FIX: если шаг неизвестен — не открываем на Bybit, идём на Gate.io.
            print(f"[QTY STEP MISSING BYBIT] {e} — пробуем Gate.io")
            bybit_price = None
        else:
            amount_tokens = _round_qty(raw_qty, step)

            if amount_tokens <= 0:
                print(f"[QTY ZERO BYBIT] {symbol}")
            else:
                qty_str = str(amount_tokens)
                # FIX: один orderLinkId на WS и REST fallback — Bybit отвергнет
                # дубль с retCode 30050, защита от double-position при WS
                # ack timeout (см. post_order в delist_api).
                order_link_id = new_order_link_id()
                # Bundle SL в order.create — failsafe stop loss попадает на
                # сервер в одной WS-фрейме с открытием, без зависимости от
                # отдельного /v5/position/trading-stop (который добавляет
                # trailing уже в фоне). Если open-frame пройдёт, а trading-stop
                # задержится — SL уже стоит. trailingStop в order.create
                # Bybit'ом не поддерживается, ставится отдельно.
                # FIX 2026-06-04: убран TP1 — стратегия теперь чистый trailing.
                sl_price  = round(bybit_price * _SL_MULT, 8)
                order_args = {
                    "category":    "linear",
                    "symbol":      symbol,
                    "side":        "Buy",
                    "orderType":   "Market",
                    "qty":         qty_str,
                    "positionIdx": 1,
                    "orderLinkId": order_link_id,
                    "stopLoss":    str(sl_price),
                    "slTriggerBy": "LastPrice",
                }
                # FIX 2026-06-05: ack-waiting + ретраи на 30208 (price-cap).
                # Быстрые листинги: цена уходит за cap Bybit (30208), Market
                # reject. Ретраим до _ORDER_RETRIES × _ORDER_RETRY_SLEEP.
                # orderLinkId одинаков во всех попытках → идемпотентность.
                success = False
                for attempt in range(1, _ORDER_RETRIES + 1):
                    ack = _ws_place_order_ack(order_args)
                    if ack is None:
                        # WS недоступен/таймаут ack → REST fallback один раз.
                        post_order(symbol, "Buy", qty_str, 1,
                                   order_link_id=order_link_id,
                                   stop_loss=str(sl_price))
                        success = True
                        break
                    rc = ack.get("retCode", -1)
                    if rc == 0:
                        success = True
                        break
                    if rc not in _PRICE_CAP_RETCODES:
                        # Не price-cap reject (нет маржи, symbol not found
                        # и т.д.) — ретраи бессмысленны, fail fast.
                        print(
                            f"[BYBIT-WS] REJECTED {symbol} retCode={rc} "
                            f"retMsg={ack.get('retMsg','?')!r} — fail fast",
                            flush=True,
                        )
                        return 0, 0
                    if attempt < _ORDER_RETRIES:
                        time.sleep(_ORDER_RETRY_SLEEP)

                if not success:
                    print(
                        f"[BYBIT-WS] {symbol}: все {_ORDER_RETRIES} ретраев "
                        f"на price-cap — не зашли", flush=True,
                    )
                    return 0, 0

                with _last_exchange_lock:
                    _last_exchange[ticker_name] = "bybit"
                return amount_tokens, bybit_price

    # ── Gate.io fallback ─────────────────────────────────────────
    gate_price = gate_get_price(ticker_name)
    if not gate_price:
        print(f"[NO PRICE ANYWHERE] {ticker_name} — нет ни на Bybit ни на Gate.io")
        return 0, 0

    amount, fill_price = gate_open_long(ticker_name, usdt_amount)
    if amount:
        with _last_exchange_lock:
            _last_exchange[ticker_name] = "gate"
    return amount, fill_price


# ── Chain warmup (PEP-659 specialization) ────────────────────────
# Прогрев CPython adaptive interpreter'а: первый запуск любого
# bytecode'а до specialization ~1.5-3x медленнее. На market_open_long
# с её 14-полевым dict, function-call'ами get_price/_get_qty_step/
# _round_qty/uuid это даёт +5-15мс на ПЕРВОМ листинге vs warm.
# Прогоняем тот же путь N раз с диверсией финального ws.send в
# cancel-fake (через _warmup_mode=True в place_order_ws_fast).

def warmup_chain(n: int = 30) -> int:
    """
    Прогоняет ТОТ ЖЕ Python-путь, что и market_open_long, N раз —
    без создания реальных ордеров. Возвращает число успешных итераций.

    Прогревает:
      • get_price → cache.get
      • f-string symbol build
      • _get_qty_step
      • _round_qty (с Decimal precision cache)
      • new_order_link_id (uuid4)
      • round(price * coef, 8) — SL/TP вычисления
      • dict-литерал из 14 полей (CPython BUILD_MAP)
      • _ws_place_order routing → sync.warmup() (cancel-fake)
      • json.dumps + ws.send (через warmup'овский payload)

    BTC выбран потому что:
      • Всегда в price_cache
      • Шаг лота preloaded
      • amount_tokens > 0 при margin=14 USDT
    """
    sample_ticker = "BTC"
    sample_margin = 14.0
    symbol = f"{sample_ticker}USDT"
    ok = 0

    for _ in range(n):
        try:
            bybit_price = get_price(sample_ticker)
            if not bybit_price:
                continue
            raw_qty = (sample_margin / bybit_price) * LEVERAGE
            try:
                step = _get_qty_step(symbol)
            except QtyStepUnavailable:
                continue
            amount_tokens = _round_qty(raw_qty, step)
            if amount_tokens <= 0:
                continue
            qty_str = str(amount_tokens)
            order_link_id = new_order_link_id()
            sl_price  = round(bybit_price * _SL_MULT, 8)
            order_args = {
                "category":    "linear",
                "symbol":      symbol,
                "side":        "Buy",
                "orderType":   "Market",
                "qty":         qty_str,
                "positionIdx": 1,
                "orderLinkId": order_link_id,
                "stopLoss":    str(sl_price),
                "slTriggerBy": "LastPrice",
            }
            # _warmup_mode=True → диверсия в cancel-fake. Никакого ордера.
            _ws_place_order(order_args, _warmup_mode=True)
            ok += 1
        except Exception:  # noqa: BLE001
            # Все ошибки игнорим — warmup best-effort, не должен валить bootstrap.
            pass
    return ok


# ── TP/SL для лонга ───────────────────────────────────────────────

def _set_tp_sl_bybit(ticker_name: str, entry_price: float, amount: float) -> str:
    """
    Выставляет нативный trailing stop 1% + аварийный SL 1% на ВСЮ позицию
    (Full-режим). Без фиксированного TP.

    FIX 2026-06-04: старая схема (TP1 +4.5% на 30% + trailing на 70% в
    tpslMode:Partial) на проде НЕ срабатывала — диагностика closed-pnl
    показала закрытия через UNKNOWN, а не PartialTakeProfit/TrailingStop.
    Bybit требует Full-режим для trailing на всю позицию (как на делисте).

    Стратегия «поймать импульс листинга»:
      - trailingStop 1% (тугой) — фиксируем максимум с пика
      - activePrice = entry × 1.01 — активация после +1% в плюс
      - аварийный SL = entry × 0.99 (-1%) — если цена сразу пошла против
      - БЕЗ фиксированных TP — чистый трейлинг на всю позицию (Full)
    """
    symbol = f"{ticker_name}USDT"

    try:
        step = _get_qty_step(symbol)
    except QtyStepUnavailable as e:
        print(f"[TP/SL SKIP] {e}")
        return "skip"

    sl = round(entry_price * _SL_MULT, 8)              # -1% аварийный стоп
    trailing_distance = round(entry_price * _TRAIL_PCT, 8)  # 1% дистанция
    active_price = round(entry_price * _TRAIL_ACT, 8)  # активация после +1%

    sl_size = str(_round_qty(amount, step))            # вся позиция
    if sl_size == "0.0" or float(sl_size) <= 0:
        print(f"[TP/SL SKIP] {ticker_name}: slSize={sl_size} (amount={amount}, step={step}) — слишком мало")
        return "skip"

    # FIX 2026-06-06: через _trading_stop_settle — ретраи на 10001 (осёдка позиции).
    _trading_stop_settle({
        "category":     "linear",
        "symbol":       symbol,
        "positionIdx":  1,
        "stopLoss":     str(sl),
        "slTriggerBy":  "LastPrice",
        "slSize":       sl_size,
        "trailingStop": str(trailing_distance),
        "activePrice":  str(active_price),
        # tpslMode не указываем → Full-режим (trailing на всю позицию)
    }, tag=ticker_name)

    print(
        f"[TP/SL SET LONG] {ticker_name} | entry={entry_price} | "
        f"SL={sl}(-1%) | Trailing=1% (active@{active_price})"
    )
    return "Выставил trailing (лонг)"


def set_tp_sl_long(ticker_name: str, entry_price: float, amount: float) -> str:
    """
    Роутер — выставляет TP/SL на той бирже где открыта позиция.

    M3: обёрнуто в try/except. На Bybit fast-path ордер мог быть ОТВЕРГНУТ
    (reject логируется асинхронно в _watch_ack), а market_open_long всё равно
    вернул "успех" — тогда позиции нет и /v5/position/trading-stop падает с
    retCode≠0. Раньше это исключение тихо глохло в _tp_sl_executor. Теперь —
    громкий лог, чтобы оператор знал о пропущенном/отвергнутом входе.
    """
    try:
        with _last_exchange_lock:
            exchange = _last_exchange.get(ticker_name, "bybit")

        if exchange == "gate":
            return gate_set_tp_sl_long(ticker_name, entry_price, amount)
        return _set_tp_sl_bybit(ticker_name, entry_price, amount)
    except Exception as e:
        print(
            f"[TP/SL FAIL] {ticker_name}: {e} — возможно ордер отвергнут "
            f"биржей (позиции нет). ПРОВЕРЬ ПОЗИЦИЮ!"
        )
        return "error"


# ── Robinhood exit: трейлинг 0.75% + страховочный SL 0.5% ─────────
# FIX 2026-06-09: time-based партиалы (30/30/40 + БУ) заменены на тугой
# нативный трейлинг 0.75% + аварийный стоп 0.5% на всю позицию.

def set_robinhood_exit(ticker_name: str, entry_price: float, amount: float,
                       venue: str = "bybit") -> str:
    """
    Robinhood-стратегия выхода: нативный трейлинг 0.75% + страховочный SL 0.5%
    на всю позицию (туже обычного листинга 1%/1%). Bybit — через trading-stop
    (Full-режим), Gate — через gate_set_tp_sl_long с теми же процентами.
    """
    if (venue or "").lower() == "gate":
        return gate_set_tp_sl_long(ticker_name, entry_price, amount,
                                   sl_pct=(1.0 - _RH_SL_MULT), trail_pct=_RH_TRAIL_PCT)

    symbol = f"{ticker_name}USDT"
    try:
        step = _get_qty_step(symbol)
    except QtyStepUnavailable as e:
        print(f"[RH-EXIT SKIP] {e}")
        return "skip"

    sl = round(entry_price * _RH_SL_MULT, 8)                  # -0.5% страховочный
    trailing_distance = round(entry_price * _RH_TRAIL_PCT, 8)  # 0.75% дистанция
    active_price = round(entry_price * _RH_TRAIL_ACT, 8)       # активация +0.75%

    sl_size = str(_round_qty(amount, step))                   # вся позиция
    if sl_size == "0.0" or float(sl_size) <= 0:
        print(f"[RH-EXIT SKIP] {ticker_name}: slSize={sl_size} — слишком мало")
        return "skip"

    try:
        # FIX 2026-06-06: ретрай на осёдку позиции (ставится сразу после open).
        _trading_stop_settle({
            "category":     "linear",
            "symbol":       symbol,
            "positionIdx":  1,
            "stopLoss":     str(sl),
            "slTriggerBy":  "LastPrice",
            "slSize":       sl_size,
            "trailingStop": str(trailing_distance),
            "activePrice":  str(active_price),
            # tpslMode не указываем → Full-режим (trailing на всю позицию)
        }, tag=ticker_name)
    except Exception as e:  # noqa: BLE001
        print(f"[RH-EXIT] {ticker_name}: trailing/SL упал: {e!r}")

    print(f"[RH-EXIT SET] {ticker_name} | entry={entry_price} | "
          f"SL={sl}(-0.5%) | Trailing=0.75% (active@{active_price})")
    return "Robinhood trailing 0.75% + SL 0.5%"


# ── Парсинг тикера из TG-сообщения ───────────────────────────────

# Форматы:
#   [UPBIT] $PRL listed on Upbit (KRW, BTC, USDT)
#   [BITHUMB] $PRL listed on Bithumb
_RE_LISTING_TG   = re.compile(r"\$([A-Z0-9]{2,10})\s+listed\s+on\s+(Upbit|Bithumb|Binance|Bybit)", re.IGNORECASE)
# FIX-batch-6: тикеры в скобках после coin-name: "Genius Terminal (GENIUS) and OpenGradient (OPG)"
# FIX: первый символ может быть цифрой — тикеры 1INCH, 1000PEPE, 1000BONK
# раньше пропускались. Чисто цифровые `(123)` всё ещё отсекаются `_filter_tokens`
# (isdigit() check) ниже.
_RE_TICKER_PAREN = re.compile(r"\(([A-Z0-9][A-Z0-9]{1,9})\)")
# FIX-batch-6: $TICKER маркер (используется в coin_listing, ListingCryptoCoinChat)
# FIX: добавлен IGNORECASE — каналы изредка шлют "$alcx" вместо "$ALCX".
_RE_TICKER_DOLLAR = re.compile(r"\$([A-Za-z][A-Za-z0-9]{1,9})\b")
# FIX-batch-6: явные пары "ABCUSDT", "ABC/USDT", "ABC-USDT"
_RE_USDT_PAIR    = re.compile(r"\b([A-Z0-9]{2,10})(?:/|-|_)?USDT\b")
# FIX: поддержка тикеров с цифрами (1INCH, 1000PEPE)
_RE_TICKER_PLAIN = re.compile(r"\b([A-Z0-9]{2,8})\b")


def find_listing_pairs(text: str) -> list[str]:
    """
    Извлекает тикеры из сообщения о листинге.
    Порядок поиска (FIX-batch-6, от точного к общему):
      1. Явный $TICKER listed on Upbit/Bithumb/Binance/Bybit
      2. Тикеры в скобках после coin-name: "Genius Terminal (GENIUS)"
      3. $TICKER маркеры (coin_listing, ListingCryptoCoinChat)
      4. Явные USDT-пары: ABCUSDT / ABC/USDT
      5. Fallback по known_coins
    :param text: str - текст сообщения.
    :return: list[str] - список тикеров.
    """
    # FIX-PERF: убраны print'ы "[FIND LISTING] метод=..." из тела функции.
    # find_listing_pairs вызывается в hot-path TG handler'а ДО submit'а,
    # каждый print с PYTHONUNBUFFERED=1 = ~0.5-1мс. Метод теперь известен
    # только через counters, но "Монеты : [...]" в LISTING-блоке всё равно
    # показывает результат. Для диагностики — раскомментировать print'ы.
    # Метод 1: прямой паттерн TG-канала
    matches = _RE_LISTING_TG.findall(text)
    if matches:
        return list(dict.fromkeys(t.upper() for t, _ in matches))

    # FIX-batch-6: Метод 2 — тикеры в скобках (Binance "Will List X (TICKER)")
    # FIX (review M12): .upper() для консистентности с методами 1/3 (биржа
    # ждёт uppercase-символы; защита если regex когда-то расширят на [a-z]).
    paren_tickers = list(dict.fromkeys(
        t.upper() for t in _RE_TICKER_PAREN.findall(text)
        if t.upper() not in EXCLUDED_TOKENS
        and 2 <= len(t) <= 10
        and not t.isdigit()
    ))
    if paren_tickers:
        return paren_tickers

    # FIX-batch-6: Метод 3 — $TICKER маркеры.
    # FIX: нормализуем в uppercase (каналы изредка пишут "$alcx").
    dollar_tickers = list(dict.fromkeys(
        t.upper() for t in _RE_TICKER_DOLLAR.findall(text)
        if t.upper() not in EXCLUDED_TOKENS
        and 2 <= len(t) <= 10
        and not t.isdigit()
    ))
    if dollar_tickers:
        return dollar_tickers

    # FIX-batch-6: Метод 4 — явные USDT-пары
    text_upper = text.upper()
    usdt_tickers = list(dict.fromkeys(
        t for t in _RE_USDT_PAIR.findall(text_upper)
        if t not in EXCLUDED_TOKENS
        and 2 <= len(t) <= 10
        and not t.isdigit()
    ))
    if usdt_tickers:
        return usdt_tickers

    # Метод 5: fallback по known_coins
    return list(dict.fromkeys(
        t for t in _RE_TICKER_PLAIN.findall(text_upper)
        if t not in EXCLUDED_TOKENS
        and 2 <= len(t) <= 8
        and not t.isdigit()
        and t in known_coins
    ))
