from __future__ import annotations

# ── test_parser_volume.py ─────────────────────────────────────────
# Мониторит резкий рост объёма на Bybit.
# Если объём последней ЗАКРЫТОЙ свечи в X раз выше среднего,
# монета ликвидна (avg turnover >= MIN_AVG_TURNOVER_USDT)
# и цена двинулась >= MIN_PRICE_MOVE_PCT —
# запоминаем сигнал. На следующем скане (+1 мин) проверяем
# подтверждение: следующая свеча тоже идёт в ту же сторону.
# Только тогда входим.
# ─────────────────────────────────────────────────────────────────

import threading
import time

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
MAGENTA = "\033[95m"
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
VOLUME_WINDOW           = 20       # сколько свечей для расчёта среднего объёма
VOLUME_SPIKE_X          = 10       # объём должен быть в X раз выше среднего
MIN_PRICE_MOVE_PCT      = 1.0      # минимальное движение цены % (было 0.3 — слишком мало)
MIN_AVG_TURNOVER_USDT   = 100_000  # минимальный средний оборот свечи в USDT (фильтр неликвида)
USDT_AMOUNT             = 5        # маржа на сделку
LEVERAGE                = 10
TP_PCT                  = 0.03     # +3% тейкпрофит
SL_PCT                  = 0.015    # -1.5% стоплосс
SCAN_INTERVAL           = 60       # секунд между сканированиями
COOLDOWN_SEC            = 300      # не входить в одну монету чаще чем раз в 5 мин

# ── Защита от дублей ──────────────────────────────────────────────
_cooldown: dict[str, float] = {}
_cooldown_lock = threading.Lock()

def _in_cooldown(ticker: str) -> bool:
    with _cooldown_lock:
        until = _cooldown.get(ticker)
        return bool(until and time.time() < until)

def _set_cooldown(ticker: str) -> None:
    with _cooldown_lock:
        _cooldown[ticker] = time.time() + COOLDOWN_SEC


# ── Ожидающие подтверждения сигналы ──────────────────────────────
# Структура: ticker -> {"side": "Buy"|"Sell", "spike": float, "move": float, "turnover": float, "ts": float}
_pending: dict[str, dict] = {}
_pending_lock = threading.Lock()

# ── Получение данных с Bybit ──────────────────────────────────────

def _fetch_bybit_kline(ticker: str, interval: str = "1", limit: int = 23) -> list | None:
    """
    Получает свечи с Bybit.
    interval: "1"=1мин, "5"=5мин и т.д.
    Возвращает список [timestamp, open, high, low, close, volume, turnover]
    Bybit отдаёт новейшую первой:
      klines[0] = текущая незакрытая свеча  ← НЕ используем
      klines[1] = последняя закрытая свеча  ← анализируем
      klines[2..] = история для среднего
    """
    try:
        resp = requests.get(
            "https://api.bybit.com/v5/market/kline",
            params={
                "category": "linear",
                "symbol":   f"{ticker}USDT",
                "interval": interval,
                "limit":    limit,
            },
            timeout=5,
        )
        data = resp.json()
        if data.get("retCode") != 0:
            return None
        return data["result"]["list"]
    except Exception:
        return None


# ── Основной сканер ───────────────────────────────────────────────

def _open_position(ticker: str, side: str) -> tuple[float, float]:
    """
    Открывает позицию на Bybit.
    side: "Buy" (лонг) | "Sell" (шорт)
    Возвращает (amount_tokens, entry_price)
    """
    price = get_price(ticker)
    if not price:
        return 0, 0

    symbol        = f"{ticker}USDT"
    raw_qty       = (USDT_AMOUNT / price) * LEVERAGE
    step          = _get_qty_step(symbol)
    amount_tokens = _round_qty(raw_qty, step)

    if amount_tokens <= 0:
        return 0, 0

    pos_idx = 1 if side == "Buy" else 2

    _post("/v5/order/create", {
        "category":    "linear",
        "symbol":      symbol,
        "side":        side,
        "orderType":   "Market",
        "qty":         str(amount_tokens),
        "positionIdx": pos_idx,
    })

    return amount_tokens, price


def _set_tp_sl(ticker: str, side: str, entry_price: float, amount: float) -> None:
    """Выставляет TP и SL сразу после открытия."""
    symbol = f"{ticker}USDT"

    if side == "Buy":
        tp = round(entry_price * (1 + TP_PCT), 8)
        sl = round(entry_price * (1 - SL_PCT), 8)
        pos_idx = 1
    else:
        tp = round(entry_price * (1 - TP_PCT), 8)
        sl = round(entry_price * (1 + SL_PCT), 8)
        pos_idx = 2

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

    direction = "LONG" if side == "Buy" else "SHORT"
    log_ok("TP/SL", f"{ticker} [{direction}] | TP={tp}(+{TP_PCT*100:.1f}%) | SL={sl}(-{SL_PCT*100:.1f}%)")


def scan_volume() -> None:
    """
    Основной цикл — каждую минуту проверяет объём по всем монетам.

    Логика двухшаговая:
      ШАГ 1 — Обнаружение сигнала:
        1. Берём последнюю ЗАКРЫТУЮ свечу (klines[1]).
        2. Средний оборот по истории >= MIN_AVG_TURNOVER_USDT (фильтр неликвида).
        3. Спайк объёма >= VOLUME_SPIKE_X.
        4. Движение цены свечи-спайка >= MIN_PRICE_MOVE_PCT.
        → Записываем в _pending, ждём следующего скана.

      ШАГ 2 — Подтверждение (+1 мин):
        5. Смотрим на новую закрытую свечу (klines[1]).
        6. Она должна идти в ту же сторону (close > open для Buy, close < open для Sell).
        → Только тогда входим.
    """
    log_ok(
        "VOL",
        f"Сканер объёмов запущен | спайк={VOLUME_SPIKE_X}x | окно={VOLUME_WINDOW} свечей "
        f"| мин.движение={MIN_PRICE_MOVE_PCT}% | мин.оборот={MIN_AVG_TURNOVER_USDT:,} USDT"
    )

    while True:
        time.sleep(SCAN_INTERVAL)

        coins = list(known_coins)
        log_info("VOL", f"Сканируем {len(coins)} монет...")

        # Снимаем текущий список ожидающих до начала скана
        with _pending_lock:
            pending_snapshot = dict(_pending)

        for ticker in coins:
            if _in_cooldown(ticker):
                with _pending_lock:
                    _pending.pop(ticker, None)
                continue

            klines = _fetch_bybit_kline(ticker, interval="1", limit=VOLUME_WINDOW + 2)
            if not klines or len(klines) < VOLUME_WINDOW + 2:
                continue

            # klines[0] = незакрытая (пропускаем)
            # klines[1] = последняя закрытая
            # klines[2:] = история
            last_closed     = klines[1]
            history_candles = klines[2:]

            last_vol      = float(last_closed[5])
            last_turnover = float(last_closed[6])
            last_close    = float(last_closed[4])
            last_open     = float(last_closed[1])

            hist_volumes   = [float(c[5]) for c in history_candles]
            hist_turnovers = [float(c[6]) for c in history_candles]

            avg_vol      = sum(hist_volumes)   / len(hist_volumes)   if hist_volumes   else 0
            avg_turnover = sum(hist_turnovers) / len(hist_turnovers) if hist_turnovers else 0

            if avg_vol == 0:
                continue

            # ── Фильтр ликвидности ────────────────────────────────
            if avg_turnover < MIN_AVG_TURNOVER_USDT:
                continue

            # ── ШАГ 2: Проверяем подтверждение для ожидающих ─────
            if ticker in pending_snapshot:
                signal    = pending_snapshot[ticker]
                sig_side  = signal["side"]
                confirmed = (
                    (sig_side == "Buy"  and last_close > last_open) or
                    (sig_side == "Sell" and last_close < last_open)
                )
                confirm_move = (last_close - last_open) / last_open * 100

                with _pending_lock:
                    _pending.pop(ticker, None)

                if confirmed:
                    direction = "LONG ↑" if sig_side == "Buy" else "SHORT ↓"
                    log_ok(
                        "CONF",
                        f"{ticker} | подтверждение {direction} | "
                        f"свеча {confirm_move:+.2f}% | спайк был {signal['spike']:.1f}x"
                    )

                    amount, entry_price = _open_position(ticker, sig_side)
                    if not amount:
                        log_err("CONF", f"{ticker} | не удалось открыть позицию")
                        continue

                    _set_cooldown(ticker)

                    threading.Thread(
                        target=_set_tp_sl,
                        args=(ticker, sig_side, entry_price, amount),
                        daemon=True,
                    ).start()

                    tg_log(
                        f"📊 <b>VOLUME SPIKE</b> {ticker}\n"
                        f"Спайк: {signal['spike']:.1f}x среднего\n"
                        f"Оборот: {signal['turnover']:,.0f} USDT\n"
                        f"Движение спайка: {signal['move']:+.2f}%\n"
                        f"Подтверждение: {confirm_move:+.2f}%\n"
                        f"Направление: {direction}\n"
                        f"Entry: {entry_price} | Amount: {amount}"
                    )
                else:
                    log_warn(
                        "CONF",
                        f"{ticker} | подтверждение НЕ получено ({confirm_move:+.2f}%) — сигнал отменён"
                    )
                continue  # этот тикер уже обработан как pending

            # ── ШАГ 1: Ищем новый спайк ──────────────────────────
            spike_ratio    = last_vol / avg_vol
            price_move_pct = (last_close - last_open) / last_open * 100

            if spike_ratio < VOLUME_SPIKE_X:
                continue

            if abs(price_move_pct) < MIN_PRICE_MOVE_PCT:
                log_info(
                    "VOL",
                    f"{ticker} | спайк {spike_ratio:.1f}x | оборот={avg_turnover:,.0f}$ "
                    f"но движение слабое ({price_move_pct:.2f}%) — пропуск"
                )
                continue

            side      = "Buy" if price_move_pct > 0 else "Sell"
            direction = "LONG ↑" if side == "Buy" else "SHORT ↓"

            log_warn(
                "VOL",
                f"{ticker} | спайк {spike_ratio:.1f}x | оборот={last_turnover:,.0f}$ | "
                f"движение={price_move_pct:+.2f}% → {direction} | ждём подтверждения..."
            )

            with _pending_lock:
                _pending[ticker] = {
                    "side":     side,
                    "spike":    spike_ratio,
                    "move":     price_move_pct,
                    "turnover": last_turnover,
                    "ts":       time.time(),
                }

        log_info("VOL", f"Скан завершён | ожидают подтверждения: {len(_pending)} монет...")


# ── Запуск ────────────────────────────────────────────────────────

if __name__ == "__main__":
    threading.Thread(target=price_updater, daemon=True).start()
    log_ok("CACHE", "price_updater запущен")

    warmup_bybit_connection()
    preload_lot_steps()

    log_ok("VOL", "Ждём 5с...")
    time.sleep(5)

    tg_log(
        f"📊 <b>VOLUME SPIKE парсер запущен</b>\n"
        f"Спайк: {VOLUME_SPIKE_X}x среднего объёма\n"
        f"Окно: {VOLUME_WINDOW} свечей\n"
        f"Мин. движение: {MIN_PRICE_MOVE_PCT}%\n"
        f"Мин. оборот: {MIN_AVG_TURNOVER_USDT:,} USDT\n"
        f"TP: +{TP_PCT*100:.1f}% | SL: -{SL_PCT*100:.1f}%"
    )

    def _heartbeat():
        while True:
            time.sleep(3600)
            tg_log("✅ <b>VOLUME SPIKE парсер работает</b>")

    threading.Thread(target=_heartbeat, daemon=True).start()

    scan_volume()