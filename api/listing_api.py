from __future__ import annotations

# ── listing_api.py ────────────────────────────────────────────────
# Bybit (приоритет) + Gate.io (fallback) для лонгов при листингах.
# Если токена нет на Bybit — открываем на Gate.io.
# ─────────────────────────────────────────────────────────────────

import re
import threading

from api.delist_api import (
    _post,
    _post_http2,             # FIX: HTTP/2 client для background TP/SL
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
# FIX-PERF: используем fire-and-forget вариант (place_order_ws_fast) — не
# ждём 70-100мс RTT до Bybit на ack. Send возвращает за ~1-5мс, reject
# логируется в фоне через _watch_ack в bybit_ws_trade.
try:
    from api.bybit_ws_trade import place_order_ws_fast as _ws_place_order, WSOrderRejected
except Exception as _ws_import_exc:  # noqa: BLE001 — graceful
    print(f"[BYBIT-WS] модуль не подгружен: {_ws_import_exc!r} — будет только REST")
    def _ws_place_order(args: dict, _warmup_mode: bool = False) -> dict | None:  # type: ignore[misc]
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
                # Bundle SL + TP1 в order.create — failsafe stop loss попадает
                # на сервер в одной WS-фрейме с открытием, без зависимости от
                # отдельного /v5/position/trading-stop (который добавляет
                # trailing уже в фоне). Если open-frame пройдёт, а trading-stop
                # задержится — SL уже стоит. trailingStop в order.create
                # Bybit'ом не поддерживается, поэтому ставится отдельно.
                sl_price  = round(bybit_price * 0.92, 8)   # -8%
                tp1_price = round(bybit_price * 1.045, 8)  # +4.5%
                tp1_qty   = _round_qty(amount_tokens * 0.30, step)
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
                    "takeProfit":  str(tp1_price),
                    "tpTriggerBy": "LastPrice",
                    "tpslMode":    "Partial",
                    "tpSize":      str(tp1_qty),
                }
                # FIX-PERF: WS Trade fire-and-forget (place_order_ws_fast).
                # Возврат не-None означает «frame ушёл на провод»; ack от
                # Bybit ждётся в фоне (см. _watch_ack). Reject логируется
                # как [BYBIT-WS-FAST] REJECTED в stdout.
                placed_via = "REST"
                try:
                    ws_ack = _ws_place_order(order_args)
                except WSOrderRejected as e:
                    # Защитный путь: fast-вариант не должен бросать reject
                    # (он логирует асинхронно). Но если bybit_ws_trade не
                    # подгрузился и упал на старую sync-обёртку — обрабатываем.
                    print(f"[BYBIT-WS] reject — пропускаем REST fallback: {e}")
                    return 0, 0
                except Exception as e:  # noqa: BLE001
                    print(f"[BYBIT-WS] ошибка place_order_ws: {e!r} — REST")
                    ws_ack = None

                if ws_ack is None:
                    # FIX-batch-8 #5: fast-path post_order вместо _post (f-string,
                    # -2..-5мс) + тот же orderLinkId — idempotency.
                    post_order(symbol, "Buy", qty_str, 1, order_link_id=order_link_id)
                else:
                    placed_via = "WS-FAST"

                with _last_exchange_lock:
                    _last_exchange[ticker_name] = "bybit"
                # FIX-PERF: удалён print "[BYBIT LONG/{placed_via}] ..." — он
                # стоял ПЕРЕД return и добавлял ~1мс к open_ms (PYTHONUNBUFFERED=1).
                # WS-vs-REST на success-пути не информативен; на failure-пути
                # bybit_ws_trade уже логирует "[BYBIT-WS-FAST] send failed/timeout".
                # Worker сразу после return пишет [OPEN] с timing.
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
            sl_price  = round(bybit_price * 0.92, 8)
            tp1_price = round(bybit_price * 1.045, 8)
            tp1_qty   = _round_qty(amount_tokens * 0.30, step)
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
                "takeProfit":  str(tp1_price),
                "tpTriggerBy": "LastPrice",
                "tpslMode":    "Partial",
                "tpSize":      str(tp1_qty),
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
    Добавляет trailing stop 3.5% на 70% позиции на Bybit. SL (-8%) и
    TP1 (+4.5% на 30%) уже выставлены в момент открытия — они летят
    в одном WS-фрейме с order.create (см. market_open_long). Эта функция
    только добавляет trailing (Bybit не поддерживает trailingStop в
    order.create — только через /v5/position/trading-stop).

    Если open прошёл через REST fallback и не нёс SL/TP — повторно
    выставляем их через trading-stop здесь же (idempotent).
    """
    symbol = f"{ticker_name}USDT"

    try:
        step = _get_qty_step(symbol)
    except QtyStepUnavailable as e:
        print(f"[TP/SL SKIP] {e}")
        return "skip"

    sl  = round(entry_price * 0.92, 8)    # -8%
    tp1 = round(entry_price * 1.045, 8)   # +4.5%

    tp1_size = str(_round_qty(amount * 0.30, step))  # 30% на TP1

    # trailingStop = абсолютное расстояние в USDT от максимума до стопа.
    # 3.5% — потуже чем было (5.5%), чтобы меньше отдавать с пика.
    trailing_distance = round(entry_price * 0.035, 8)
    # activePrice для лонга: активация когда цена поднимется на 3.5% (в плюс).
    # Защита от входного шума — trailing не активен пока не зафиксируем профит.
    active_price = round(entry_price * 1.035, 8)

    _post_http2("/v5/position/trading-stop", {
        "category":     "linear",
        "symbol":       symbol,
        "stopLoss":     str(sl),
        "slTriggerBy":  "LastPrice",
        "takeProfit":   str(tp1),
        "tpTriggerBy":  "LastPrice",
        "trailingStop": str(trailing_distance),
        "activePrice":  str(active_price),  # Активация после +3.5%
        "tpslMode":     "Partial",
        "tpSize":       tp1_size,
        "positionIdx":  1,
    })

    print(f"[TP/SL SET LONG] {ticker_name} | SL={sl}(-8%) | TP1={tp1}(+4.5%/30%) | Trailing=3.5%(70%)")
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
    paren_tickers = list(dict.fromkeys(
        t for t in _RE_TICKER_PAREN.findall(text)
        if t not in EXCLUDED_TOKENS
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
