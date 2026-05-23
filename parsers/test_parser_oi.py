from __future__ import annotations

# ── test_parser_oi.py ─────────────────────────────────────────────
# Мониторит открытый интерес (OI) на Bybit.
# Резкий рост OI без роста цены → крупный игрок набирает позицию.
# Входим в ту же сторону что и крупный игрок.
#
# Логика определения стороны:
#   OI растёт + цена растёт   → набирают лонги → входим в лонг
#   OI растёт + цена падает   → набирают шорты → входим в шорт
#   OI падает                 → позиции закрываются → пропускаем
# ─────────────────────────────────────────────────────────────────

import threading
import time
from collections import defaultdict, deque

import requests

from api.delist_api import (
    _post,
    _get_qty_step,
    _round_qty,
    get_price,
    known_coins,
    warmup_bybit_connection,
    preload_lot_steps,
    price_updater,
)
from tg.tg_logger import tg_log

# ── ANSI цвета ────────────────────────────────────────────────────
GREEN   = "\033[92m"
RED     = "\033[91m"
YELLOW  = "\033[93m"
CYAN    = "\033[96m"
BOLD    = "\033[1m"
RESET   = "\033[0m"

def _log(tag: str, color: str, msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"{color}{BOLD}[{ts}][{tag}]{RESET} {msg}", flush=True)

def log_info(tag, msg): _log(tag, CYAN,    msg)
def log_ok(tag, msg):   _log(tag, GREEN,   msg)
def log_warn(tag, msg): _log(tag, YELLOW,  msg)
def log_err(tag, msg):  _log(tag, RED,     msg)


# ── Настройки ─────────────────────────────────────────────────────
OI_WINDOW          = 10       # свечей для расчёта среднего OI
OI_SPIKE_PCT       = 5.0      # % рост OI за одну свечу для сигнала
MIN_PRICE_MOVE_PCT = 0.2      # минимальное движение цены чтобы определить сторону
USDT_AMOUNT        = 5
LEVERAGE           = 10
TP_PCT             = 0.04     # +4% тейкпрофит
SL_PCT             = 0.02     # -2% стоплосс
SCAN_INTERVAL      = 300      # сканируем каждые 5 минут (5м свечи)
COOLDOWN_SEC       = 600      # кулдаун 10 мин на монету

# ── Cooldown ──────────────────────────────────────────────────────
_cooldown: dict[str, float] = {}
_cooldown_lock = threading.Lock()

def _in_cooldown(ticker: str) -> bool:
    with _cooldown_lock:
        until = _cooldown.get(ticker)
        return bool(until and time.time() < until)

def _set_cooldown(ticker: str) -> None:
    with _cooldown_lock:
        _cooldown[ticker] = time.time() + COOLDOWN_SEC


# ── Получение данных ──────────────────────────────────────────────

def _fetch_oi_history(ticker: str, limit: int = 12) -> list[dict] | None:
    """
    Получает историю открытого интереса с Bybit (5-минутные интервалы).
    Возвращает список [{timestamp, openInterest}, ...]
    """
    try:
        resp = requests.get(
            "https://api.bybit.com/v5/market/open-interest",
            params={
                "category":     "linear",
                "symbol":       f"{ticker}USDT",
                "intervalTime": "5min",
                "limit":        limit,
            },
            timeout=5,
        )
        data = resp.json()
        if data.get("retCode") != 0:
            return None
        return data["result"]["list"]  # новейший первый
    except Exception:
        return None


def _fetch_price_change(ticker: str) -> float | None:
    """
    Получает изменение цены за последние 5 минут через kline.
    Возвращает % изменения (положительное = рост).
    """
    try:
        resp = requests.get(
            "https://api.bybit.com/v5/market/kline",
            params={
                "category": "linear",
                "symbol":   f"{ticker}USDT",
                "interval": "5",
                "limit":    2,
            },
            timeout=5,
        )
        data = resp.json()
        if data.get("retCode") != 0:
            return None
        candles = data["result"]["list"]
        if len(candles) < 2:
            return None
        # candles[0] = текущая, candles[1] = предыдущая закрытая
        prev_close = float(candles[1][4])
        curr_close = float(candles[0][4])
        if prev_close == 0:
            return None
        return (curr_close - prev_close) / prev_close * 100
    except Exception:
        return None


# ── Торговля ──────────────────────────────────────────────────────

def _open_and_protect(ticker: str, side: str, oi_spike_pct: float, price_move_pct: float) -> None:
    price = get_price(ticker)
    if not price:
        log_err("OI", f"{ticker} | нет цены")
        return

    symbol        = f"{ticker}USDT"
    raw_qty       = (USDT_AMOUNT / price) * LEVERAGE
    step          = _get_qty_step(symbol)
    amount_tokens = _round_qty(raw_qty, step)

    if amount_tokens <= 0:
        return

    pos_idx = 1 if side == "Buy" else 2

    _post("/v5/order/create", {
        "category":    "linear",
        "symbol":      symbol,
        "side":        side,
        "orderType":   "Market",
        "qty":         str(amount_tokens),
        "positionIdx": pos_idx,
    })

    if side == "Buy":
        tp = round(price * (1 + TP_PCT), 8)
        sl = round(price * (1 - SL_PCT), 8)
    else:
        tp = round(price * (1 - TP_PCT), 8)
        sl = round(price * (1 + SL_PCT), 8)

    _post("/v5/position/trading-stop", {
        "category":    "linear",
        "symbol":      symbol,
        "takeProfit":  str(tp),
        "stopLoss":    str(sl),
        "tpTriggerBy": "LastPrice",
        "slTriggerBy": "LastPrice",
        "tpslMode":    "Full",
        "positionIdx": pos_idx,
    })

    direction = "LONG ↑" if side == "Buy" else "SHORT ↓"
    log_ok(
        "OI TRADE",
        f"{ticker} [{direction}] | entry={price} | amount={amount_tokens} | "
        f"OI+{oi_spike_pct:.1f}% | цена{price_move_pct:+.2f}%"
    )

    tg_log(
        f"📈 <b>OI SPIKE TRADE</b> {ticker}\n"
        f"Направление: {direction}\n"
        f"OI вырос: +{oi_spike_pct:.1f}%\n"
        f"Цена: {price_move_pct:+.2f}%\n"
        f"Entry: {price} | TP: {tp} | SL: {sl}"
    )


# ── Основной сканер ───────────────────────────────────────────────

def scan_oi() -> None:
    log_ok("OI", f"Сканер OI запущен | спайк >= {OI_SPIKE_PCT}% | окно={OI_WINDOW} свечей")

    while True:
        time.sleep(SCAN_INTERVAL)

        coins = list(known_coins)
        log_info("OI", f"Сканируем OI по {len(coins)} монетам...")

        for ticker in coins:
            if _in_cooldown(ticker):
                continue

            oi_list = _fetch_oi_history(ticker, limit=OI_WINDOW + 1)
            if not oi_list or len(oi_list) < 2:
                continue

            # oi_list[0] = текущий, oi_list[1] = предыдущий
            current_oi = float(oi_list[0]["openInterest"])
            prev_oi    = float(oi_list[1]["openInterest"])

            if prev_oi == 0:
                continue

            oi_change_pct = (current_oi - prev_oi) / prev_oi * 100

            # OI должен расти — значит позиции открываются, а не закрываются
            if oi_change_pct < OI_SPIKE_PCT:
                continue

            # Определяем сторону по движению цены
            price_move = _fetch_price_change(ticker)
            if price_move is None:
                continue

            if abs(price_move) < MIN_PRICE_MOVE_PCT:
                log_info(
                    "OI",
                    f"{ticker} | OI+{oi_change_pct:.1f}% но цена почти не двигалась "
                    f"({price_move:+.2f}%) — неясное направление, пропуск"
                )
                continue

            side      = "Buy" if price_move > 0 else "Sell"
            direction = "LONG ↑" if side == "Buy" else "SHORT ↓"

            log_ok(
                "OI",
                f"{ticker} | OI+{oi_change_pct:.1f}% | "
                f"цена{price_move:+.2f}% | → {direction}"
            )

            _set_cooldown(ticker)

            threading.Thread(
                target=_open_and_protect,
                args=(ticker, side, oi_change_pct, price_move),
                daemon=True,
            ).start()

        log_info("OI", "Скан завершён.")


# ── Запуск ────────────────────────────────────────────────────────

if __name__ == "__main__":
    threading.Thread(target=price_updater, daemon=True).start()
    log_ok("CACHE", "price_updater запущен")

    warmup_bybit_connection()
    preload_lot_steps()

    log_ok("OI", "Ждём 5с...")
    time.sleep(5)

    tg_log(
        f"📈 <b>OI SPIKE парсер запущен</b>\n"
        f"Мин. спайк OI: +{OI_SPIKE_PCT}% за 5 минут\n"
        f"TP: +{TP_PCT*100:.1f}% | SL: -{SL_PCT*100:.1f}%\n"
        f"Стратегия: следуем за крупным игроком"
    )

    def _heartbeat():
        while True:
            time.sleep(3600)
            tg_log("✅ <b>OI SPIKE парсер работает</b>")

    threading.Thread(target=_heartbeat, daemon=True).start()

    scan_oi()