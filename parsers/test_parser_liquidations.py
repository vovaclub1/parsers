from __future__ import annotations

# ── test_parser_liquidations.py ───────────────────────────────────
# Следит за крупными ликвидациями на Bybit через WebSocket.
# Крупная ликвидация лонгов → цена резко упала → ждём отскок → шорт.
# Крупная ликвидация шортов → цена резко выросла → ждём отскок → лонг.
# Стратегия контрарианская: торгуем против ликвидации.
# ─────────────────────────────────────────────────────────────────

import json
import threading
import time

import websocket

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
MIN_LIQ_USDT    = 50_000     # минимальный размер ликвидации в USDT
USDT_AMOUNT     = 5       # маржа на сделку
LEVERAGE        = 10
TP_PCT          = 0.025      # +2.5% тейкпрофит
SL_PCT          = 0.012      # -1.2% стоплосс
ENTRY_DELAY     = 0.5        # секунд подождать после ликвидации перед входом
COOLDOWN_SEC    = 120        # не торговать одну монету чаще раз в 2 мин

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

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


# ── Торговля ──────────────────────────────────────────────────────

def _open_and_protect(ticker: str, side: str, liq_usdt: float) -> None:
    """
    Открывает позицию и сразу выставляет TP/SL.
    side: "Buy" (лонг — торгуем против ликвидации шортов)
          "Sell" (шорт — торгуем против ликвидации лонгов)
    """
    time.sleep(ENTRY_DELAY)  # небольшая пауза — даём цене стабилизироваться

    price = get_price(ticker)
    if not price:
        log_err("LIQ", f"{ticker} | нет цены")
        return

    symbol        = f"{ticker}USDT"
    raw_qty       = (USDT_AMOUNT / price) * LEVERAGE
    step          = _get_qty_step(symbol)
    amount_tokens = _round_qty(raw_qty, step)

    if amount_tokens <= 0:
        log_err("LIQ", f"{ticker} | нулевой размер позиции")
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

    # TP/SL
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
        "LIQ TRADE",
        f"{ticker} [{direction}] | entry={price} | amount={amount_tokens} | "
        f"TP={tp} | SL={sl} | ликвидация={liq_usdt/1000:.0f}K$"
    )

    tg_log(
        f"💥 <b>LIQUIDATION TRADE</b> {ticker}\n"
        f"Направление: {direction}\n"
        f"Ликвидация: ${liq_usdt/1000:.0f}K\n"
        f"Entry: {price} | TP: {tp} | SL: {sl}"
    )


# ── WebSocket обработчик ──────────────────────────────────────────

def _on_message(ws, message: str) -> None:
    try:
        data = json.loads(message)
    except Exception:
        return

    if data.get("topic") != "liquidation.USDT":
        return

    liq_data = data.get("data", {})

    symbol    = liq_data.get("symbol", "")        # BTCUSDT
    side      = liq_data.get("side", "")          # Buy (ликвидирован лонг) | Sell (ликвидирован шорт)
    price     = float(liq_data.get("price", 0))
    qty       = float(liq_data.get("size", 0))
    liq_usdt  = price * qty

    if not symbol.endswith("USDT"):
        return

    ticker = symbol.replace("USDT", "")

    if ticker not in known_coins:
        return

    if liq_usdt < MIN_LIQ_USDT:
        return

    if _in_cooldown(ticker):
        return

    # Контрарианская логика:
    # Ликвидирован лонг (side=Buy) → цена падала → ожидаем отскок вверх → открываем лонг
    # Ликвидирован шорт (side=Sell) → цена росла → ожидаем откат вниз → открываем шорт
    trade_side = "Buy" if side == "Buy" else "Sell"
    liq_type   = "лонгов" if side == "Buy" else "шортов"
    direction  = "LONG ↑" if trade_side == "Buy" else "SHORT ↓"

    log_ok(
        "LIQ",
        f"{ticker} | ликвидация {liq_type} ${liq_usdt/1000:.0f}K | "
        f"цена={price} | → {direction}"
    )

    _set_cooldown(ticker)

    threading.Thread(
        target=_open_and_protect,
        args=(ticker, trade_side, liq_usdt),
        daemon=True,
    ).start()


def _on_error(ws, error):
    log_err("WS", f"Ошибка: {error}")

def _on_close(ws, *args):
    log_warn("WS", "Соединение закрыто, переподключаемся через 5с...")

def _on_open(ws):
    # Подписываемся на все ликвидации линейных фьючерсов
    ws.send(json.dumps({
        "op":   "subscribe",
        "args": ["liquidation.USDT"],
    }))
    log_ok("WS", "Подписались на liquidation.USDT")


# ── Запуск WebSocket с авторестартом ─────────────────────────────

def run_ws() -> None:
    while True:
        try:
            ws = websocket.WebSocketApp(
                BYBIT_WS_URL,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log_err("WS", f"WebSocket упал: {e}")
        time.sleep(5)


# ── Запуск ────────────────────────────────────────────────────────

if __name__ == "__main__":
    threading.Thread(target=price_updater, daemon=True).start()
    log_ok("CACHE", "price_updater запущен")

    warmup_bybit_connection()
    preload_lot_steps()

    log_ok("LIQ", "Ждём 5с...")
    time.sleep(5)

    tg_log(
        f"💥 <b>LIQUIDATION парсер запущен</b>\n"
        f"Мин. ликвидация: ${MIN_LIQ_USDT/1000:.0f}K\n"
        f"TP: +{TP_PCT*100:.1f}% | SL: -{SL_PCT*100:.1f}%\n"
        f"Стратегия: контрарианская (против ликвидации)"
    )

    def _heartbeat():
        while True:
            time.sleep(3600)
            tg_log("✅ <b>LIQUIDATION парсер работает</b>")

    threading.Thread(target=_heartbeat, daemon=True).start()

    run_ws()