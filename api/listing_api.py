from __future__ import annotations

# ── listing_api.py ────────────────────────────────────────────────
# Bybit (приоритет) + Gate.io (fallback) для лонгов при листингах.
# Если токена нет на Bybit — открываем на Gate.io.
# ─────────────────────────────────────────────────────────────────

import re
import threading

from api.delist_api import (
    _post,
    post_order,            # FIX-10: публичный алиас вместо _post_order
    new_order_link_id,     # FIX-10: публичный алиас вместо _new_order_link_id
    _get_qty_step,
    _round_qty,
    get_price,
    known_coins,
    EXCLUDED_TOKENS,
    price_updater,
    warmup_bybit_connection,
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
try:
    from api.bybit_ws_trade import place_order_ws as _ws_place_order, WSOrderRejected
except Exception as _ws_import_exc:  # noqa: BLE001 — graceful
    print(f"[BYBIT-WS] модуль не подгружен: {_ws_import_exc!r} — будет только REST")
    def _ws_place_order(args: dict) -> dict | None:  # type: ignore[misc]
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
    "preload_lot_steps",
    "gate_preload_lot_steps",
    "warmup_gate_connection",
    "get_price",
]


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

    if bybit_price:
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
                order_args = {
                    "category":    "linear",
                    "symbol":      symbol,
                    "side":        "Buy",
                    "orderType":   "Market",
                    "qty":         qty_str,
                    "positionIdx": 1,
                    "orderLinkId": order_link_id,
                }
                # FIX-batch-5: WS Trade API → fallback REST.
                placed_via = "REST"
                try:
                    ws_ack = _ws_place_order(order_args)
                except WSOrderRejected as e:
                    # FIX-2: WS работает, Bybit ОТВЕРГ ордер логически.
                    # REST повтор бесполезен → сразу сдаёмся.
                    print(f"[BYBIT-WS] reject — пропускаем REST fallback: {e}")
                    return 0, 0
                except Exception as e:
                    print(f"[BYBIT-WS] ошибка place_order_ws: {e!r} — REST")
                    ws_ack = None

                if ws_ack is None:
                    # FIX-batch-8 #5: fast-path post_order вместо _post (f-string,
                    # -2..-5мс) + тот же orderLinkId — idempotency.
                    post_order(symbol, "Buy", qty_str, 1, order_link_id=order_link_id)
                else:
                    placed_via = "WS"

                with _last_exchange_lock:
                    _last_exchange[ticker_name] = "bybit"
                print(f"[BYBIT LONG/{placed_via}] {symbol} | tokens={amount_tokens} | price≈{bybit_price}")
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


# ── TP/SL для лонга ───────────────────────────────────────────────

def _set_tp_sl_bybit(ticker_name: str, entry_price: float, amount: float) -> str:
    """
    Выставляет TP1 +5.5% на 30% + трейлинг 5.5% на 70% на Bybit.
    :param ticker_name: str - тикер монеты.
    :param entry_price: float - цена входа.
    :param amount: float - количество токенов в позиции.
    :return: str - результат.
    """
    symbol = f"{ticker_name}USDT"

    try:
        step = _get_qty_step(symbol)
    except QtyStepUnavailable as e:
        print(f"[TP/SL SKIP] {e}")
        return "skip"

    sl  = round(entry_price * 0.92, 8)   # -8%
    tp1 = round(entry_price * 1.055, 8)  # +5.5%

    # FIX: убрал неиспользуемую sl_size — Bybit /position/trading-stop ставит SL
    # на ВСЮ позицию автоматически (без slSize), а TP1 на 30%, остальное под трейлинг.
    tp1_size = str(_round_qty(amount * 0.30, step))  # 30% на TP1

    # trailingStop = абсолютное расстояние в USDT от максимума до стопа.
    trailing_distance = round(entry_price * 0.055, 8)

    _post("/v5/position/trading-stop", {
        "category":     "linear",
        "symbol":       symbol,
        "stopLoss":     str(sl),
        "slTriggerBy":  "LastPrice",
        "takeProfit":   str(tp1),
        "tpTriggerBy":  "LastPrice",
        "trailingStop": str(trailing_distance),
        "tpslMode":     "Partial",
        "tpSize":       tp1_size,
        "positionIdx":  1,
    })

    print(f"[TP/SL SET LONG] {ticker_name} | SL={sl}(-8%) | TP1={tp1}(+5.5%/30%) | Trailing=5.5%(70%)")
    return "Выставил цели (лонг)"


def set_tp_sl_long(ticker_name: str, entry_price: float, amount: float) -> str:
    """
    Роутер — выставляет TP/SL на той бирже где открыта позиция.
    """
    with _last_exchange_lock:
        exchange = _last_exchange.get(ticker_name, "bybit")

    if exchange == "gate":
        return gate_set_tp_sl_long(ticker_name, entry_price, amount)
    return _set_tp_sl_bybit(ticker_name, entry_price, amount)


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
    # Метод 1: прямой паттерн TG-канала
    matches = _RE_LISTING_TG.findall(text)
    if matches:
        tickers = list(dict.fromkeys(t.upper() for t, _ in matches))
        print(f"[FIND LISTING] метод=TG-паттерн → {tickers}")
        return tickers

    # FIX-batch-6: Метод 2 — тикеры в скобках (Binance "Will List X (TICKER)" формат)
    paren_matches = _RE_TICKER_PAREN.findall(text)
    paren_tickers = [
        t for t in paren_matches
        if t not in EXCLUDED_TOKENS
        and 2 <= len(t) <= 10
        and not t.isdigit()
    ]
    paren_tickers = list(dict.fromkeys(paren_tickers))
    if paren_tickers:
        print(f"[FIND LISTING] метод=скобки → {paren_tickers}")
        return paren_tickers

    # FIX-batch-6: Метод 3 — $TICKER маркеры
    # FIX: нормализуем в uppercase (каналы изредка пишут "$alcx").
    dollar_matches = [t.upper() for t in _RE_TICKER_DOLLAR.findall(text)]
    dollar_tickers = [
        t for t in dollar_matches
        if t not in EXCLUDED_TOKENS
        and 2 <= len(t) <= 10
        and not t.isdigit()
    ]
    dollar_tickers = list(dict.fromkeys(dollar_tickers))
    if dollar_tickers:
        print(f"[FIND LISTING] метод=$ticker → {dollar_tickers}")
        return dollar_tickers

    # FIX-batch-6: Метод 4 — явные USDT-пары
    text_upper = text.upper()
    usdt_matches = _RE_USDT_PAIR.findall(text_upper)
    usdt_tickers = [
        t for t in usdt_matches
        if t not in EXCLUDED_TOKENS
        and 2 <= len(t) <= 10
        and not t.isdigit()
    ]
    usdt_tickers = list(dict.fromkeys(usdt_tickers))
    if usdt_tickers:
        print(f"[FIND LISTING] метод=USDT-пары → {usdt_tickers}")
        return usdt_tickers

    # Метод 5: fallback по known_coins
    candidates = _RE_TICKER_PLAIN.findall(text_upper)
    found = [
        t for t in candidates
        if t not in EXCLUDED_TOKENS
        and 2 <= len(t) <= 8
        and not t.isdigit()
        and t in known_coins
    ]
    found = list(dict.fromkeys(found))
    if found:
        print(f"[FIND LISTING] метод=fallback(known_coins) → {found}")
    else:
        print(f"[FIND LISTING] ничего не найдено: {text[:80]}")
    return found
