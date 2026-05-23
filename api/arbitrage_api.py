from __future__ import annotations

# ── arbitrage_api.py ──────────────────────────────────────────────
# Межбиржевой арбитраж Bybit <-> Gate.io <-> Hyperliquid
# Два режима:
#   SPREAD  — лонг на дешёвой + шорт на дорогой, выход когда спред схлопнулся
#   FUNDING — лонг на дешёвой + шорт на дорогой, держим ради фандинга,
#             выход когда фандинг упал ниже MIN_FUNDING_HOURLY
# ─────────────────────────────────────────────────────────────────

import math
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from api.gate_api import _quanto_gate
from api.delist_api import (
    _post,
    _get,
    _get_qty_step,
    _round_qty,
    get_price,
    known_coins,
)
from api.gate_api import (
    gate_get_price,
    gate_open_long,
    gate_open_short,
    gate_open_short_by_contracts,
    gate_known_coins,
    _get as gate_get,
    _post_signed as gate_post_signed,
    _SETTLE,
    _LEVERAGE as _GATE_LEVERAGE_INTERNAL,
)
from api.hl_api import (
    hl_get_price,
    hl_known_coins,
    hl_price_updater,
    hl_open_long,
    hl_open_short,
    hl_open_short_by_usdt,
    hl_close_position,
    hl_get_funding,
    warmup_hl_connection,
)
from tg.tg_logger import tg_log

ALL_EXCHANGES = ["bybit", "gate", "hyperliquid"]

# ── ANSI цвета ────────────────────────────────────────────────────
GREEN   = "\033[92m"
RED     = "\033[91m"
YELLOW  = "\033[93m"
CYAN    = "\033[96m"
MAGENTA = "\033[95m"
BOLD    = "\033[1m"
RESET   = "\033[0m"

def _tag(label: str, color: str) -> str:
    ts = time.strftime("%H:%M:%S")
    return f"{color}{BOLD}[{ts}][{label}]{RESET}"


# ── Константы ─────────────────────────────────────────────────────
BYBIT_LEVERAGE  = 10
# Плечо Gate.io задаётся внутри gate_api (_LEVERAGE=10) через _gate_set_leverage().
USDT_PER_SIDE   = 5.0       # $5 на каждой бирже
MAX_POSITIONS   = 3         # максимум одновременных арбитражей

MIN_SPREAD_PCT  = 5.0       # минимальный спред для входа %
HALF_CLOSE_PCT  = 0.5       # закрываем 50% когда спред сократился до 50% от входного
FULL_CLOSE_PCT  = 0.02      # закрываем остаток когда спред <= 2%

# Стопы (общие)
MAX_FUNDING_HOURLY = -0.0005  # -0.05%/час суммарный фандинг → стоп для SPREAD-режима
MAX_HOLD_HOURS     = 24       # максимум держим позицию часов
MAX_LOSS_PCT       = 0.03     # -3% от депозита → стоп

# ── Пороги фандинг-режима ─────────────────────────────────────────
#
# Расчёт основан на реальных комиссиях бирж (taker, маркет-ордера):
#   Bybit:  0.055% taker  | Gate: 0.050% taker  | HL: 0.025% taker
#   Полная стоимость позиции (вход + выход, обе стороны):
#     Bybit ↔ Gate  = (0.055 + 0.050) × 2 = 0.210%
#     Bybit ↔ HL    = (0.055 + 0.025) × 2 = 0.160%
#     Gate  ↔ HL    = (0.050 + 0.025) × 2 = 0.150%
#
# Цель входа: отбить все комиссии за TARGET_HOURS = 6 часов.
# MIN_FUNDING_HOURLY = max(0.210, 0.160, 0.150)% / 6ч = 0.035%/ч
#
# Цель выхода: если текущий фандинг не отобьёт оставшиеся комиссии
# за 24 часа — выходим (позиция убыточна в долгосроке).
# EXIT_FUNDING_HOURLY = 0.210% / 24ч ≈ 0.009%/ч
#
# Минимальное удержание: не выходим раньше MIN_HOLD_HOURS = 2ч,
# даже если фандинг упал — нужно дождаться хотя бы 2 выплат на HL
# (каждый час) чтобы гарантированно покрыть комиссию входа.
#
# Проверка реальных сценариев (Gate↔HL, комиссии 0.150%):
#   HL 0.02%/ч (слабый спайк) → breakeven 8ч  → НЕ входим (8 > 6)
#   HL 0.04%/ч (средний)      → breakeven 4ч  → ВХОДИМ   (4 < 6) ✓
#   HL 0.08%/ч (сильный)      → breakeven 2ч  → ВХОДИМ   ✓
#   Базовый рынок 0.00125%/ч  → breakeven 120ч → НЕ входим ✓
#   После спайка 0.005%/ч     → ниже EXIT → выходим ✓

MIN_FUNDING_HOURLY     = 0.0035  # 0.035%/ч — минимум для входа (breakeven за 6ч)
EXIT_FUNDING_HOURLY    = 0.00009  # 0.009%/ч — выходим (breakeven > 24ч)
MIN_HOLD_HOURS         = 2.0      # минимум держим 2ч (ждём 2 выплаты на HL)
FUNDING_CHECK_INTERVAL = 300      # проверяем фандинг каждые 5 минут (был 10)


# ── Активные позиции ──────────────────────────────────────────────
# {ticker: ArbPosition}
active_positions: dict[str, "ArbPosition"] = {}
positions_lock = threading.Lock()


class ArbPosition:
    """Хранит состояние одного арбитража."""

    # Комиссии (taker, маркет) по парам бирж — вход + выход обеих сторон.
    # Используется для точного расчёта breakeven при мониторинге.
    _FEE_COST: dict[frozenset, float] = {
        frozenset({"bybit", "gate"}):        0.00210,  # 0.210%
        frozenset({"bybit", "hyperliquid"}): 0.00160,  # 0.160%
        frozenset({"gate",  "hyperliquid"}): 0.00150,  # 0.150%
    }

    def __init__(
        self,
        ticker: str,
        long_exchange: str,
        short_exchange: str,
        long_amount: float,
        short_amount: float,
        entry_spread_pct: float,
        entry_time: float,
        deposit: float,
    ):
        self.ticker          = ticker
        self.long_exchange   = long_exchange
        self.short_exchange  = short_exchange
        self.long_amount     = long_amount
        self.short_amount    = short_amount
        self.entry_spread    = entry_spread_pct
        self.entry_time      = entry_time
        self.deposit         = deposit
        self.half_closed     = False
        self.closed          = False
        self.mode            = "spread"  # "spread" | "funding"

        # Точная стоимость комиссий для этой пары бирж
        key            = frozenset({long_exchange, short_exchange})
        self.fee_cost  = self._FEE_COST.get(key, 0.00210)

        # Минимальный hourly-фандинг для безубыточности за 24ч
        self.exit_funding_hourly = self.fee_cost / 24  # например 0.00210/24 = 0.0000875

        # Минимальный hourly-фандинг для входа (breakeven за 6ч)
        self.min_funding_hourly  = self.fee_cost / 6


# ── Фандинг ───────────────────────────────────────────────────────

def get_bybit_funding(ticker: str) -> tuple[float, float]:
    """
    Возвращает (funding_rate, next_funding_time_unix) с Bybit.
    funding_rate — ставка за один интервал (обычно 8ч).
    """
    try:
        symbol    = f"{ticker}USDT"
        resp      = _get(f"/v5/market/tickers?category=linear&symbol={symbol}")
        lst       = resp.get("result", {}).get("list", [])
        if not lst:
            return 0.0, 0.0
        data      = lst[0]
        rate      = float(data.get("fundingRate", 0))
        next_time = float(data.get("nextFundingTime", 0)) / 1000
        return rate, next_time
    except Exception:
        return 0.0, 0.0


def get_gate_funding(ticker: str) -> tuple[float, float]:
    """
    Возвращает (funding_rate, next_funding_time_unix) с Gate.io.
    """
    try:
        contract  = f"{ticker}_USDT"
        data      = gate_get(f"/api/v4/futures/{_SETTLE}/contracts/{contract}")
        rate      = float(data.get("funding_rate", 0))
        next_time = float(data.get("funding_next_apply", 0))
        return rate, next_time
    except Exception:
        return 0.0, 0.0


def calc_hourly_funding(
    rate: float,
    next_funding_time: float,
    is_long: bool,
) -> float:
    """
    Пересчитывает ставку фандинга в часовую.
    Если мы в лонге — платим при положительном фандинге.
    Если в шорте — получаем при положительном фандинге.
    Возвращает почасовой P&L от фандинга (положительный = хорошо для нас).
    """
    now            = time.time()
    interval_sec   = max(next_funding_time - now, 3600)  # минимум 1 час
    interval_hours = interval_sec / 3600
    hourly_rate    = rate / interval_hours
    return -hourly_rate if is_long else hourly_rate


def get_hl_funding(ticker: str) -> tuple[float, float]:
    """Возвращает (funding_rate, next_funding_time) с Hyperliquid."""
    try:
        return hl_get_funding(ticker)
    except Exception as e:
        print(f"{_tag('ARB ERR', RED)} hl funding {ticker}: {e}")
        return 0.0, 0.0


def get_all_bybit_funding() -> dict[str, tuple[float, float]]:
    """
    Получает фандинг по всем линейным контрактам Bybit за один запрос.
    Возвращает {ticker: (rate, next_time)}
    """
    try:
        resp   = _get("/v5/market/tickers?category=linear")
        result = {}
        for item in resp.get("result", {}).get("list", []):
            symbol = item.get("symbol", "")
            if not symbol.endswith("USDT"):
                continue
            ticker    = symbol[:-4]
            rate      = float(item.get("fundingRate", 0))
            next_time = float(item.get("nextFundingTime", 0)) / 1000
            result[ticker] = (rate, next_time)
        return result
    except Exception as e:
        print(f"{_tag('ARB ERR', RED)} bybit bulk funding: {e}")
        return {}


def get_all_gate_funding() -> dict[str, tuple[float, float]]:
    """
    Получает фандинг по всем контрактам Gate.io за один запрос.
    Возвращает {ticker: (rate, next_time)}
    """
    try:
        data   = gate_get(f"/api/v4/futures/{_SETTLE}/contracts")
        result = {}
        for item in data:
            contract = item.get("name", "")
            if not contract.endswith("_USDT"):
                continue
            ticker    = contract[:-5]
            rate      = float(item.get("funding_rate", 0))
            next_time = float(item.get("funding_next_apply", 0))
            result[ticker] = (rate, next_time)
        return result
    except Exception as e:
        print(f"{_tag('ARB ERR', RED)} gate bulk funding: {e}")
        return {}


def get_all_hl_funding() -> dict[str, tuple[float, float]]:
    """
    Получает фандинг по всем контрактам Hyperliquid за один запрос.
    Возвращает {ticker: (rate, next_time)}
    """
    try:
        from api.hl_api import _info
        data     = _info({"type": "metaAndAssetCtxs"})
        universe = data[0].get("universe", [])
        ctxs     = data[1]
        now      = time.time()
        result   = {}
        for i, asset in enumerate(universe):
            ticker    = asset.get("name", "")
            ctx       = ctxs[i]
            rate      = float(ctx.get("funding", 0))
            next_time = now - (now % 3600) + 3600  # следующий час
            result[ticker] = (rate, next_time)
        return result
    except Exception as e:
        print(f"{_tag('ARB ERR', RED)} hl bulk funding: {e}")
        return {}


def _get_exchange_funding(ticker: str, exchange: str) -> tuple[float, float]:
    """Возвращает (rate, next_time) для нужной биржи."""
    if exchange == "bybit":
        return get_bybit_funding(ticker)
    elif exchange == "gate":
        return get_gate_funding(ticker)
    elif exchange == "hyperliquid":
        return get_hl_funding(ticker)
    return 0.0, 0.0


def get_combined_hourly_funding(
    ticker: str,
    long_exchange: str,
    short_exchange: str,
) -> float:
    """
    Суммарный почасовой фандинг для арбитража (Bybit/Gate/Hyperliquid).
    Положительный = фандинг в нашу пользу.
    """
    long_rate,  long_next  = _get_exchange_funding(ticker, long_exchange)
    short_rate, short_next = _get_exchange_funding(ticker, short_exchange)

    long_hourly  = calc_hourly_funding(long_rate,  long_next,  is_long=True)
    short_hourly = calc_hourly_funding(short_rate, short_next, is_long=False)

    total = long_hourly + short_hourly
    print(
        f"{_tag('FUNDING', CYAN)} {ticker} | "
        f"{long_exchange}={long_rate*100:.4f}% {short_exchange}={short_rate*100:.4f}% | "
        f"Итого почасовой={'+'if total>=0 else ''}{total*100:.4f}%"
    )
    return total


# ── Депозиты ──────────────────────────────────────────────────────

def check_hl_deposits(ticker: str) -> bool:
    """Hyperliquid — всегда открыты (нет традиционных депозитов)."""
    return True


def check_deposits(ticker: str, exchange: str) -> bool:
    """Роутер проверки депозитов."""
    if exchange == "bybit":
        return check_bybit_deposits(ticker)
    elif exchange == "gate":
        return check_gate_deposits(ticker)
    elif exchange == "hyperliquid":
        return check_hl_deposits(ticker)
    return True


def check_bybit_deposits(ticker: str) -> bool:
    """Проверяет открыты ли депозиты на Bybit."""
    try:
        resp   = _get(f"/v5/asset/coin/query-info?coin={ticker}")
        chains = resp.get("result", {}).get("rows", [{}])[0].get("chains", [])
        for chain in chains:
            if chain.get("chainDeposit") == "1":
                return True
        return False
    except Exception as e:
        print(f"{_tag('ARB ERR', RED)} bybit deposits {ticker}: {e}")
        return True  # не блокируем если API недоступен


def check_gate_deposits(ticker: str) -> bool:
    """Проверяет открыты ли депозиты на Gate.io."""
    try:
        data = gate_get(f"/api/v4/wallet/currency_chains?currency={ticker}")
        for chain in data:
            if chain.get("is_deposit_disabled") == 0:
                return True
        return False
    except Exception as e:
        print(f"{_tag('ARB ERR', RED)} gate deposits {ticker}: {e}")
        return True  # не блокируем если API недоступен


# ── Спред ─────────────────────────────────────────────────────────

def get_spread(ticker: str) -> tuple[float, str, str] | None:
    """
    Считает максимальный спред среди Bybit, Gate.io, Hyperliquid.
    Возвращает (spread_pct, long_exchange, short_exchange) или None.
    long_exchange — где дешевле (туда лонг).
    short_exchange — где дороже (туда шорт).
    """
    prices: dict[str, float] = {}
    bybit_price = get_price(ticker)
    gate_price  = gate_get_price(ticker)
    hl_price    = hl_get_price(ticker)

    if bybit_price: prices["bybit"]       = bybit_price
    if gate_price:  prices["gate"]        = gate_price
    if hl_price:    prices["hyperliquid"] = hl_price

    if len(prices) < 2:
        return None

    best_spread     = 0.0
    best_long_exch  = ""
    best_short_exch = ""

    exchanges = list(prices.keys())
    for i, ex1 in enumerate(exchanges):
        for ex2 in exchanges[i+1:]:
            p1, p2 = prices[ex1], prices[ex2]
            if p1 < p2:
                spread = (p2 - p1) / p1 * 100
                le, se = ex1, ex2
            else:
                spread = (p1 - p2) / p2 * 100
                le, se = ex2, ex1
            if spread > best_spread:
                best_spread     = spread
                best_long_exch  = le
                best_short_exch = se

    if best_spread == 0:
        return None

    return best_spread, best_long_exch, best_short_exch


# ── Открытие позиций ──────────────────────────────────────────────

def open_long_bybit(ticker: str, usdt_amount: float) -> tuple[float, float]:
    """
    Открывает лонг на Bybit.
    Возвращает (amount_tokens, price) — количество монет реально купленных.
    amount_tokens используется для открытия ровно такого же шорта на второй бирже.
    """
    price = get_price(ticker)
    if not price:
        return 0, 0

    symbol        = f"{ticker}USDT"
    raw_qty       = (usdt_amount / price) * BYBIT_LEVERAGE
    step          = _get_qty_step(symbol)
    amount_tokens = _round_qty(raw_qty, step)

    if amount_tokens <= 0:
        return 0, 0

    # Bybit требует минимум 5 USDT номинала на ордер
    MIN_ORDER_USDT = 5.0
    order_value    = amount_tokens * price
    if order_value < MIN_ORDER_USDT:
        min_tokens    = MIN_ORDER_USDT / price
        amount_tokens = _round_qty(min_tokens, step)
        print(f"{_tag('ARB LONG', YELLOW)} Bybit {ticker} | {order_value:.2f}$ < 5$, увеличено до {amount_tokens} токенов")
        if amount_tokens <= 0:
            print(f"{_tag('ARB ERR', RED)} Bybit {ticker} | не удалось добить до 5 USDT")
            return 0, 0

    _post("/v5/order/create", {
        "category":    "linear",
        "symbol":      symbol,
        "side":        "Buy",
        "orderType":   "Market",
        "qty":         str(amount_tokens),
        "positionIdx": 1,
    })
    print(f"{_tag('ARB LONG', GREEN)} Bybit {ticker} | tokens={amount_tokens} | price≈{price}")
    return amount_tokens, price


def open_short_bybit(ticker: str, tokens: float) -> tuple[float, float]:
    """Открывает шорт на Bybit на заданное количество токенов. Возвращает (amount, price)."""
    price = get_price(ticker)
    if not price:
        return 0, 0

    symbol        = f"{ticker}USDT"
    step          = _get_qty_step(symbol)
    amount_tokens = _round_qty(tokens, step)

    if amount_tokens <= 0:
        return 0, 0

    # FIX: import math вынесен в верхние импорты
    MIN_ORDER_USDT = 5.0
    order_value    = amount_tokens * price
    if order_value < MIN_ORDER_USDT:
        min_tokens    = MIN_ORDER_USDT / price
        amount_tokens = math.ceil(min_tokens / step) * step if step > 0 else min_tokens
        amount_tokens = round(amount_tokens, 8)
        print(f"{_tag('ARB SHORT', YELLOW)} Bybit {ticker} | увеличено до {amount_tokens} токенов")
        if amount_tokens * price < MIN_ORDER_USDT:
            print(f"{_tag('ARB ERR', RED)} Bybit {ticker} | не удалось добить до 5 USDT")
            return 0, 0

    _post("/v5/order/create", {
        "category":    "linear",
        "symbol":      symbol,
        "side":        "Sell",
        "orderType":   "Market",
        "qty":         str(amount_tokens),
        "positionIdx": 2,
    })
    print(f"{_tag('ARB SHORT', RED)} Bybit {ticker} | tokens={amount_tokens} | price≈{price}")
    return amount_tokens, price


def close_bybit_position(ticker: str, amount: float, side: str) -> None:
    """
    Закрывает позицию на Bybit.
    side: "long" | "short"
    """
    symbol       = f"{ticker}USDT"
    step         = _get_qty_step(symbol)
    close_amount = _round_qty(amount, step)
    if close_amount <= 0:
        return

    order_side   = "Sell" if side == "long" else "Buy"
    position_idx = 1 if side == "long" else 2

    _post("/v5/order/create", {
        "category":    "linear",
        "symbol":      symbol,
        "side":        order_side,
        "orderType":   "Market",
        "qty":         str(close_amount),
        "reduceOnly":  True,
        "positionIdx": position_idx,
    })
    print(f"{_tag('ARB CLOSE', YELLOW)} Bybit {side} {ticker} | amount={close_amount}")


def close_gate_position(ticker: str, amount: float, side: str) -> None:
    """
    Закрывает позицию на Gate.io.

    amount хранится по-разному в зависимости от того как открывали:
      - Лонг Gate (через _open_long_on_exchange): amount = fill_coins (монеты)
        → конвертируем в контракты: contracts = coins * price
      - Шорт Gate (через open_short_any): amount = contracts (контракты)
        → используем напрямую

    Gate.io: 1 контракт = 1 USDT номинала.
    Закрытие лонга = продажа (size отрицательный).
    Закрытие шорта = покупка (size положительный).
    """
    from api.gate_api import _gate_min_order, gate_get_price

    contract = f"{ticker}_USDT"
    price    = gate_get_price(ticker) or 0

    if side == "long":
        # amount — монеты → конвертируем в контракты
        quanto = _quanto_gate.get(ticker, 1.0)
        contracts = max(1, int(amount / quanto)) if price else max(1, int(amount))
    else:
        # amount — контракты → используем напрямую
        contracts = max(1, int(amount))

    contracts = max(contracts, _gate_min_order(ticker))
    close_size = -contracts if side == "long" else contracts

    gate_post_signed(f"/api/v4/futures/{_SETTLE}/orders", {
        "contract":    contract,
        "size":        close_size,
        "price":       "0",
        "tif":         "ioc",
        "reduce_only": True,
        "auto_size":   "",
    })
    print(
        f"{_tag('ARB CLOSE', YELLOW)} Gate {side} {ticker} | "
        f"amount={amount:.4f} → {contracts} контрактов"
    )


# ── Вспомогательные функции открытия ─────────────────────────────

def _open_long_on_exchange(ticker: str, exchange: str, usdt_amount: float) -> tuple[float, float]:
    """
    Открывает лонг на нужной бирже.
    Возвращает (fill_coins, fill_price) — реальное количество монет после исполнения.

    Важно: Gate возвращает (contracts, price) где 1 контракт = 1 USDT.
    Конвертируем в монеты: fill_coins = contracts / fill_price.
    Это нужно чтобы шорт на второй бирже открылся ровно на то же количество монет.

    :return: (fill_coins, fill_price)
    """
    if exchange == "bybit":
        # Bybit: возвращает (tokens, price) — уже в монетах
        return open_long_bybit(ticker, usdt_amount)
    elif exchange == "gate":
        # Gate: возвращает (contracts, fill_price), 1 контракт = 1 USDT
        # Конвертируем в монеты для синхронизации с другой биржей
        contracts, fill_price = gate_open_long(ticker, usdt_amount)
        if not contracts or not fill_price:
            return 0, 0

        quanto = _quanto_gate.get(ticker, 1.0)
        fill_coins = contracts * quanto
        print(f"[ARB GATE→COINS] {ticker} | {contracts} контрактов / {fill_price} = {fill_coins:.4f} монет")
        return fill_coins, fill_price
    else:  # hyperliquid
        # HL: возвращает (fill_sz, fill_price) уже в монетах
        return hl_open_long(ticker, usdt_amount)


def open_short_any(
    ticker: str,
    long_fill_coins: float,
    exchange: str,
    usdt_amount: float | None = None,
) -> tuple[float, float]:
    """
    Открывает шорт на нужной бирже ровно на то же количество монет что в лонге.
    Это гарантирует дельта-нейтральность — позиция застрахована точно.

    long_fill_coins — количество монет из лонга (уже нормализовано в монеты).
    exchange        — биржа для шорта.

    Логика по биржам:
      Bybit:  шорт в токенах → передаём long_fill_coins напрямую
      Gate:   шорт в контрактах (1 контракт = 1 USDT) → contracts = coins * price
      HL:     шорт в монетах с szDecimals-округлением → передаём long_fill_coins
    """
    if exchange == "bybit":
        return open_short_bybit(ticker, long_fill_coins)

    elif exchange == "gate":
        # Gate принимает size в контрактах. 1 контракт = 1 USDT.
        # contracts = монеты * цена
        gate_price = gate_get_price(ticker)
        if not gate_price:
            print(f"{_tag('ARB ERR', RED)} {ticker}: нет цены Gate для конвертации монет→контракты")
            return 0, 0

        quanto = _quanto_gate.get(ticker, 1.0)
        contracts = max(1, int(long_fill_coins / quanto))
        print(f"[ARB COINS→GATE] {ticker} | {long_fill_coins:.4f} монет × {gate_price} = {contracts} контрактов")
        return gate_open_short_by_contracts(ticker, contracts, usdt_amount=None)

    else:  # hyperliquid
        # HL принимает size в монетах, округляет по szDecimals внутри
        return hl_open_short(ticker, long_fill_coins)


def close_position_on_exchange(ticker: str, amount: float, side: str, exchange: str) -> None:
    """Роутер закрытия позиции на нужной бирже."""
    if exchange == "bybit":
        close_bybit_position(ticker, amount, side)
    elif exchange == "gate":
        close_gate_position(ticker, amount, side)
    else:  # hyperliquid
        hl_close_position(ticker, amount, side)


# ── Открытие арбитражной позиции ─────────────────────────────────

def _open_arb(
    ticker: str,
    long_exchange: str,
    short_exchange: str,
    tag_label: str,
    entry_info: str,
    entry_spread_pct: float = 0.0,
    mode: str = "spread",
) -> "ArbPosition | None":
    """
    FIX: общая логика открытия арбитражной/фандинговой позиции.
    Устраняет дублирование между open_arb_position и open_funding_position.
    """
    print(f"\n{BOLD}{'═'*60}{RESET}")
    print(f"{_tag(tag_label, MAGENTA)} {ticker} | {entry_info} | лонг={long_exchange} шорт={short_exchange}")

    long_fill_coins, long_price = _open_long_on_exchange(ticker, long_exchange, USDT_PER_SIDE)
    if not long_fill_coins:
        print(f"{_tag('ARB ERR', RED)} Не удалось открыть лонг на {long_exchange}")
        return None

    print(
        f"{_tag('ARB SIZE', CYAN)} {ticker} | "
        f"лонг={long_exchange} fill={long_fill_coins:.6f} монет @ {long_price} | "
        f"шорт={short_exchange} будет {long_fill_coins:.6f} монет"
    )

    # Шорт открывается ровно на то же количество монет — дельта-нейтральность
    short_amount, _ = open_short_any(
        ticker,
        long_fill_coins = long_fill_coins,
        exchange        = short_exchange,
    )
    if not short_amount:
        print(f"{_tag('ARB ERR', RED)} Не удалось открыть шорт — закрываем лонг")
        close_position_on_exchange(ticker, long_fill_coins, "long", long_exchange)
        return None

    pos = ArbPosition(
        ticker           = ticker,
        long_exchange    = long_exchange,
        short_exchange   = short_exchange,
        long_amount      = long_fill_coins,   # монеты
        short_amount     = short_amount,      # монеты (или контракты для Gate-шорта)
        entry_spread_pct = entry_spread_pct,
        entry_time       = time.time(),
        deposit          = USDT_PER_SIDE * 2,
    )
    pos.mode = mode
    print(f"{_tag('ARB OPENED', GREEN)} {ticker} | депозит={pos.deposit}$ | {entry_info}")
    print(f"{BOLD}{'═'*60}{RESET}\n")
    return pos


def open_arb_position(
    ticker: str,
    long_exchange: str,
    short_exchange: str,
    spread_pct: float,
) -> "ArbPosition | None":
    """
    Открывает арбитражную позицию: лонг на одной бирже, шорт на другой.
    Возвращает ArbPosition или None если не удалось.
    """
    return _open_arb(
        ticker, long_exchange, short_exchange,
        tag_label   = "ARB OPEN",
        entry_info  = f"спред={spread_pct:.2f}%",
        entry_spread_pct = spread_pct,
        mode        = "spread",
    )


def open_funding_position(
    ticker: str,
    long_exchange: str,
    short_exchange: str,
    entry_funding: float,
) -> "ArbPosition | None":
    """
    Открывает позицию в FUNDING-режиме.
    Лонг там где фандинг меньше, шорт там где фандинг больше.
    """
    return _open_arb(
        ticker, long_exchange, short_exchange,
        tag_label   = "FUNDING OPEN",
        entry_info  = f"фандинг={entry_funding*100:.4f}%/ч",
        entry_spread_pct = 0.0,
        mode        = "funding",
    )


# ── Закрытие позиций ──────────────────────────────────────────────

def close_position_partial(pos: ArbPosition, fraction: float, reason: str) -> None:
    """
    Закрывает часть позиции.
    fraction: 0.5 = 50%, 1.0 = 100%

    Важно: long_amount / short_amount хранятся в единицах биржи:
      - Bybit/HL: количество монет
      - Gate: количество контрактов (1 контракт = 1 USDT номинала)
    """
    long_close  = pos.long_amount  * fraction
    short_close = pos.short_amount * fraction

    print(f"{_tag('ARB CLOSE', YELLOW)} {pos.ticker} | {reason} | закрываем {fraction*100:.0f}%")

    with ThreadPoolExecutor(max_workers=2) as ex:
        ex.submit(close_position_on_exchange, pos.ticker, long_close,  "long",  pos.long_exchange)
        ex.submit(close_position_on_exchange, pos.ticker, short_close, "short", pos.short_exchange)

    pos.long_amount  -= long_close
    pos.short_amount -= short_close

    if fraction >= 1.0:
        pos.closed = True


def close_full(pos: ArbPosition, reason: str) -> None:
    """Полностью закрывает арбитражную позицию и уведомляет в TG."""
    close_position_partial(pos, 1.0, reason)
    hold_hours    = (time.time() - pos.entry_time) / 3600
    mode_label    = "FUNDING" if pos.mode == "funding" else "ARB"
    # FIX: экранируем reason — может содержать < > из сравнений фандинга
    safe_reason   = reason.replace("<", "&lt;").replace(">", "&gt;")
    tg_log(
        f"🔴 <b>{mode_label} ЗАКРЫТ</b> {pos.ticker}\n"
        f"Причина: {safe_reason}\n"
        f"Держали: {hold_hours:.1f}ч\n"
        f"Пара: {pos.long_exchange.upper()} ↔ {pos.short_exchange.upper()}"
    )


# ── Мониторинг позиций ────────────────────────────────────────────

def monitor_position(pos: ArbPosition) -> None:
    """
    Фоновый поток — следит за позицией и закрывает по условиям.

    FUNDING-режим:
      - Не выходим раньше MIN_HOLD_HOURS (ждём выплаты)
      - Выходим если текущий hourly-фандинг < pos.exit_funding_hourly
        (т.е. позиция не отобьёт оставшиеся комиссии за 24ч)
      - Выходим по MAX_HOLD_HOURS или MAX_LOSS_PCT

    SPREAD-режим:
      - Следим за схлопыванием спреда (50% → закрываем половину, 2% → всё)
      - Раз в час проверяем фандинг — если стал против нас → стоп
    """
    print(
        f"{_tag('MONITOR', CYAN)} {pos.ticker} | режим={pos.mode} | "
        f"пара={pos.long_exchange}↔{pos.short_exchange} | "
        f"fee_cost={pos.fee_cost*100:.3f}% | "
        f"exit_threshold={pos.exit_funding_hourly*100:.4f}%/ч"
    )

    last_funding_check = time.time()

    while not pos.closed:
        time.sleep(5)

        now        = time.time()
        hold_hours = (now - pos.entry_time) / 3600

        # ── Стоп по времени (общий) ───────────────────────────────
        if hold_hours >= MAX_HOLD_HOURS:
            close_full(pos, f"СТОП ВРЕМЯ ({hold_hours:.1f}ч >= {MAX_HOLD_HOURS}ч)")
            break

        if pos.mode == "funding":
            # ════════════════════════════════════════════════════════
            # FUNDING-режим
            # ════════════════════════════════════════════════════════

            # Минимальное удержание — ждём хотя бы MIN_HOLD_HOURS
            # чтобы получить несколько выплат и покрыть комиссию входа
            if hold_hours < MIN_HOLD_HOURS:
                continue

            # Проверяем фандинг каждые FUNDING_CHECK_INTERVAL секунд
            if now - last_funding_check < FUNDING_CHECK_INTERVAL:
                continue

            last_funding_check = now
            hourly_funding     = get_combined_hourly_funding(
                pos.ticker, pos.long_exchange, pos.short_exchange
            )

            # Accumulated funding (приблизительно): hourly * часов удержания
            # Используем для информации в логах
            approx_earned = hourly_funding * hold_hours

            print(
                f"{_tag('MONITOR', CYAN)} {pos.ticker} [FUNDING] | "
                f"фандинг={hourly_funding*100:.4f}%/ч | "
                f"порог_выхода={pos.exit_funding_hourly*100:.4f}%/ч | "
                f"держим={hold_hours:.1f}ч | "
                f"заработано≈{approx_earned*100:.3f}%"
            )

            # Выходим если фандинг упал ниже порога выхода
            # (позиция не отобьёт комиссии за следующие 24 часа)
            if hourly_funding < pos.exit_funding_hourly:
                close_full(
                    pos,
                    f"ФАНДИНГ УПАЛ "
                    f"({hourly_funding*100:.4f}%/ч &lt; {pos.exit_funding_hourly*100:.4f}%/ч)"
                )
                break

            # Стоп по убытку: если фандинг резко ушёл в минус
            if hourly_funding < -pos.exit_funding_hourly:
                close_full(
                    pos,
                    f"ФАНДИНГ ОТРИЦАТЕЛЬНЫЙ ({hourly_funding*100:.4f}%/ч) — срочный выход"
                )
                break

        else:
            # ════════════════════════════════════════════════════════
            # SPREAD-режим
            # ════════════════════════════════════════════════════════
            result = get_spread(pos.ticker)
            if not result:
                continue

            current_spread, _, _ = result
            entry_spread          = pos.entry_spread if pos.entry_spread > 0 else 1.0
            spread_ratio          = current_spread / entry_spread

            print(
                f"{_tag('MONITOR', CYAN)} {pos.ticker} [SPREAD] | "
                f"спред={current_spread:.2f}% (вход={entry_spread:.2f}%) | "
                f"ratio={spread_ratio:.2f}"
            )

            # Проверяем фандинг раз в час
            if now - last_funding_check >= 3600:
                last_funding_check = now
                hourly_funding     = get_combined_hourly_funding(
                    pos.ticker, pos.long_exchange, pos.short_exchange
                )
                if hourly_funding < -pos.exit_funding_hourly:
                    close_full(pos, f"СТОП ФАНДИНГ ({hourly_funding*100:.4f}%/ч)")
                    break

            # Стоп по убытку: спред расширился сильнее чем при входе
            if current_spread > entry_spread * 1.5:
                loss_pct = (current_spread - entry_spread) / 100 * pos.deposit / pos.deposit
                if loss_pct >= MAX_LOSS_PCT:
                    close_full(pos, f"СТОП УБЫТОК ({loss_pct*100:.1f}%)")
                    break

            # Закрываем 50% при схлопывании до HALF_CLOSE_PCT от входного спреда
            if not pos.half_closed and spread_ratio <= HALF_CLOSE_PCT:
                close_position_partial(pos, 0.5, f"ПОЛОВИНА СПРЕДА ({current_spread:.2f}%)")
                pos.half_closed = True

            # Закрываем остаток при почти полном схлопывании (≤ FULL_CLOSE_PCT%)
            if pos.half_closed and current_spread <= FULL_CLOSE_PCT * 100:
                close_full(pos, f"СПРЕД ЗАКРЫЛСЯ ({current_spread:.2f}%)")
                break

    # Убираем из активных
    with positions_lock:
        active_positions.pop(pos.ticker, None)

    print(f"{_tag('MONITOR', GREEN)} {pos.ticker} | позиция закрыта и удалена")