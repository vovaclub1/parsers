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
    # FIX 2026-06-19: price_ago/fetch_live_price удалены вместе с python
    # late-entry filter (заменён на slippageTolerance на стороне Bybit).
    get_position,            # FIX 2026-06-17: реальная позиция (size/avgPrice/trailing) для верификации TP/SL
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
from api.atr import compute_atr, clamp_distance, live_sample_atr_frac

# FIX: вынесли импорт из хот-функции market_open_long на модульный уровень.
# FIX 2026-06-05: перешли с fire-and-forget на ack-вариант — нужно знать
# retCode для ретраев на 30208 (price-cap на быстром листинге). _ws_place_order_ack
# ждёт ack (~10-70мс RTT), но это цена за точность: иначе отвергнутые ордера
# логировались как успешные и позиция по факту не открывалась.
# Private WS (R3) для real-time чтения position/order вместо REST polling'а.
# Опциональный — если не подгружен, fallback на REST get_position.
try:
    from api import bybit_ws_private as _ws_private  # noqa: F401
except Exception:  # noqa: BLE001
    _ws_private = None  # type: ignore[assignment]

try:
    from api.bybit_ws_trade import (
        place_order_ws_fast as _ws_place_order,
        place_order_ws_ack as _ws_place_order_ack,
        place_batch_orders_ws_ack as _ws_place_batch_orders_ack,
        WSOrderRejected,
    )
except Exception as _ws_import_exc:  # noqa: BLE001 — graceful
    print(f"[BYBIT-WS] модуль не подгружен: {_ws_import_exc!r} — будет только REST")
    def _ws_place_order(args: dict, _warmup_mode: bool = False) -> dict | None:  # type: ignore[misc]
        return None
    def _ws_place_order_ack(args: dict, timeout: float = 1.5) -> dict | None:  # type: ignore[misc]
        return None
    def _ws_place_batch_orders_ack(args_list: list[dict], timeout: float = 1.5) -> dict | None:  # type: ignore[misc]
        return None
    class WSOrderRejected(Exception):  # type: ignore[no-redef]
        """Stub если bybit_ws_trade не подгрузился — никогда не raise-нется."""
        pass

__all__ = [
    "market_open_long",
    "market_open_long_batch",
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
# FIX 2026-06-12: «дать бежать» — было 1%/1%/-1% (закрывало в безубыток на первом
# откате 1% после спайка листинга). Шире трейлинг (2.5%) ловит реальный импульс,
# SL -1.5% переживает начальный шейк-аут. Все три — env-настраиваемые (без правки кода):
#   LISTING_SL_PCT (доля стопа, 0.015=-1.5%), LISTING_TRAIL_PCT (0.025=2.5%),
#   LISTING_ACTIVE_PCT (0.005=+0.5% активация).
# FIX 2026-06-18: симметрично с делистом — активация сдвинута 0.5% → 1.0%
#   (трейлинг встаёт ТОЛЬКО после реального движения вверх на 1%, до этого
#   позицию держит bundled SL/переякоренный SL). Trailing 2.5% оставлен.
_SL_PCT        = float(os.getenv("LISTING_SL_PCT", "0.015"))      # -1.5% стоп-лосс
_TRAIL_PCT     = float(os.getenv("LISTING_TRAIL_PCT", "0.025"))   # 2.5% трейлинг-дистанция
_ACTIVE_PCT    = float(os.getenv("LISTING_ACTIVE_PCT", "0.01"))   # +1.0% активация
_SL_MULT       = 1.0 - _SL_PCT       # множитель цены стопа (ниже входа для лонга)
_TRAIL_ACT     = 1.0 + _ACTIVE_PCT   # множитель цены активации (выше входа для лонга)

# FIX 2026-06-24: ATR-based адаптивный трейлинг. Свежий листинг с короткой
# историей даст None → graceful fallback на фикс-% (см. _compute_trail_params_long).
# Период 5 — компромисс: достаточно стат-устойчивости, но успевает накопиться
# за первые ~5 минут торгов. Floor/ceiling в % защищают от вырожденных ATR
# (ноль на flat-monent / гигант на спайке листинга).
_LISTING_TRAIL_MODE      = os.getenv("LISTING_TRAIL_MODE", "sim_atr").lower()  # "sim_atr"|"atr"|"pct"
_LISTING_ATR_INTERVAL    = os.getenv("LISTING_ATR_INTERVAL", "1")           # 1m свечи
_LISTING_ATR_PERIOD      = int(os.getenv("LISTING_ATR_PERIOD", "5"))
_LISTING_ATR_MIN_CANDLES = int(os.getenv("LISTING_ATR_MIN_CANDLES", "3"))
_LISTING_ATR_TRAIL_MULT  = float(os.getenv("LISTING_ATR_TRAIL_MULT",  "2.5"))   # trailing = ATR×2.5
_LISTING_ATR_ACT_MULT    = float(os.getenv("LISTING_ATR_ACT_MULT",    "1.0"))   # active  = entry + ATR×1.0
_LISTING_ATR_SL_MULT     = float(os.getenv("LISTING_ATR_SL_MULT",     "1.5"))   # SL      = entry − ATR×1.5
# Floor/ceiling как доли от base_price — защита коридора.
_LISTING_ATR_TRAIL_MIN_PCT = float(os.getenv("LISTING_ATR_TRAIL_MIN_PCT", "0.008"))  # 0.8%
_LISTING_ATR_TRAIL_MAX_PCT = float(os.getenv("LISTING_ATR_TRAIL_MAX_PCT", "0.05"))   # 5%
_LISTING_ATR_ACT_MIN_PCT   = float(os.getenv("LISTING_ATR_ACT_MIN_PCT",   "0.005"))  # 0.5%
_LISTING_ATR_ACT_MAX_PCT   = float(os.getenv("LISTING_ATR_ACT_MAX_PCT",   "0.03"))   # 3%
_LISTING_ATR_SL_MIN_PCT    = float(os.getenv("LISTING_ATR_SL_MIN_PCT",    "0.008"))  # 0.8%
_LISTING_ATR_SL_MAX_PCT    = float(os.getenv("LISTING_ATR_SL_MAX_PCT",    "0.04"))   # 4%

# FIX 2026-07-07: sim_atr mode — точный порт tg/exit_strategies.py:exit_atr_trailing
# (та стратегия что в 6ч-карточках показывает "atr_trail" с +14%/+56%).
# Отличия от "atr" mode:
#   - формула atr = mean(|close_i - close_{i-1}|) / entry (НЕ Wilder True Range)
#   - активация ПРИ дистанции trail (не при фикс +1%) — act = trail
#   - SL = 1% (не 1.5%)
#   - clamp [0.5%, 20%] (не [0.8%, 5%]) — sim позволяет очень широкий трейлинг
#   - period=30 × 1m klines (сим смотрел первые 30 сек, мы берём 30 min pre-fill
#     как proxy — peek-ahead не возможен в проде)
# Установить LISTING_TRAIL_MODE=sim_atr чтобы включить. Дефолт "atr" остаётся
# сохранным на случай отката.
_LISTING_SIM_ATR_K            = float(os.getenv("LISTING_SIM_ATR_K",        "2.5"))
_LISTING_SIM_ATR_SL           = float(os.getenv("LISTING_SIM_ATR_SL",       "0.01"))    # 1%
_LISTING_SIM_ATR_PERIOD       = int(os.getenv("LISTING_SIM_ATR_PERIOD",     "30"))
_LISTING_SIM_ATR_INTERVAL     = os.getenv("LISTING_SIM_ATR_INTERVAL",       "1")
_LISTING_SIM_ATR_MIN_CANDLES  = int(os.getenv("LISTING_SIM_ATR_MIN_CANDLES","5"))
_LISTING_SIM_ATR_LO           = float(os.getenv("LISTING_SIM_ATR_LO",       "0.005"))   # 0.5%
_LISTING_SIM_ATR_HI           = float(os.getenv("LISTING_SIM_ATR_HI",       "0.20"))    # 20%
_LISTING_SIM_ATR_FALLBACK     = float(os.getenv("LISTING_SIM_ATR_FALLBACK", "0.02"))    # 2% (симовский fallback при atr=0)

# FIX 2026-06-19: Python late-entry filter УДАЛЁН. Он сам добавлял до 340мс
# задержки в hot-path (probe 40мс + dip-retry 6×50мс = 340мс), за это окно
# цена уезжала ещё дальше. Заменён на ESTественный фильтр на стороне Bybit:
# slippageToleranceType=Percent в order.create — Bybit конвертит market в
# IOC limit с потолком фила, без ликвидности в коридоре сам cancel'ит.
# Нулевая python-задержка, фильтр работает на уровне matching engine.
#   LISTING_SLIPPAGE_TOL_PCT — потолок проскальзывания, % (деф. 1.5; 0=ВЫКЛ)
# Эмпирика 2026-06-19: Upbit-листинги типично заходят с проскальзыванием
# 0.5-1.5%. Cap 1.5% отсеивает только реальные «ракеты» >1.5% за RTT.
_SLIPPAGE_TOL_PCT = float(os.getenv("LISTING_SLIPPAGE_TOL_PCT", "1.5"))

# FIX 2026-06-17: верификация TP/SL по РЕАЛЬНОЙ позиции (а не вслепую). Ждём появления
# позиции, ставим трейлинг к реальной avgPrice, проверяем что он СЕЛ; ретраим пока
# позиция жива. Если позиция уже закрыта стопом — спокойный лог, не «FAIL».
_POS_WAIT_RETRIES     = int(os.getenv("LISTING_POS_WAIT_RETRIES", "10"))       # ~3с ждём осёдку
_POS_WAIT_SLEEP       = float(os.getenv("LISTING_POS_WAIT_SLEEP_MS", "300")) / 1000.0
_TRAIL_VERIFY_RETRIES = int(os.getenv("LISTING_TRAIL_VERIFY_RETRIES", "4"))    # ставим трейлинг до подтверждения
_TRAIL_VERIFY_SLEEP   = float(os.getenv("LISTING_TRAIL_VERIFY_SLEEP_MS", "300")) / 1000.0
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

    # FIX 2026-06-19: python late-entry filter удалён — он САМ добавлял 40-340мс
    # задержки (probe + dip-retry sleep'ы) в hot-path. Замена — slippageTolerance
    # на стороне Bybit в order_args ниже. Bybit matching engine отвергнет фил
    # за пределами коридора, без блокирующих sleep'ов в python.

    # FIX 2026-06-24: флаг видим И в Bybit-блоке (где он выставляется на reject),
    # И в Gate-fallback внизу (где он определяет — вернуть -1,0 или 0,0).
    bybit_rejected = False

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
                # FIX 2026-06-19: slippageTolerance — Bybit отвергнет фил за
                # пределами коридора (`±_SLIPPAGE_TOL_PCT %`), без python-sleep.
                # Заменяет удалённый late-entry filter в hot-path.
                if _SLIPPAGE_TOL_PCT > 0:
                    order_args["slippageToleranceType"] = "Percent"
                    order_args["slippageTolerance"] = str(_SLIPPAGE_TOL_PCT)
                # FIX 2026-06-05: ack-waiting + ретраи на 30208 (price-cap).
                # Быстрые листинги: цена уходит за cap Bybit (30208), Market
                # reject. Ретраим до _ORDER_RETRIES × _ORDER_RETRY_SLEEP.
                # orderLinkId одинаков во всех попытках → идемпотентность.
                # FIX 2026-06-24: на reject (symbol-not-found / нет маржи /
                # slippage cap) — НЕ return -1,0 сразу, а break → Gate fallback
                # ниже. Раньше Gate-блок (line 291) был недостижим: даже когда
                # листинг на Bybit отсутствовал (symbol not found), worker
                # получал sentinel и fail-fast'ил.
                # bybit_rejected (func-scope, объявлен вверху) = True означает
                # «Bybit отверг ордер по причине которая не починится ретраем»;
                # используется в Gate-fallback ниже чтобы при пустом Gate
                # вернуть -1,0 (worker fail-fast) вместо 0,0 (3×0.1с retry).
                success = False
                for attempt in range(1, _ORDER_RETRIES + 1):
                    ack = _ws_place_order_ack(order_args)
                    if ack is None:
                        # WS недоступен/таймаут ack → REST fallback один раз.
                        # FIX 2026-07-08 (NEO-инцидент, зеркально delist):
                        # REST-исключение (33004 expired key и т.п.) раньше
                        # пролетало МИМО Gate-fallback и убивало функцию.
                        try:
                            post_order(symbol, "Buy", qty_str, 1,
                                       order_link_id=order_link_id,
                                       stop_loss=str(sl_price),
                                       slippage_tol_pct=_SLIPPAGE_TOL_PCT)
                            success = True
                        except Exception as e_rest:  # noqa: BLE001
                            print(
                                f"[BYBIT-REST] REJECTED {symbol}: {e_rest} "
                                f"— пробуем Gate", flush=True,
                            )
                            bybit_rejected = True
                        break
                    rc = ack.get("retCode", -1)
                    if rc == 0:
                        success = True
                        break
                    if rc not in _PRICE_CAP_RETCODES:
                        print(
                            f"[BYBIT-WS] REJECTED {symbol} retCode={rc} "
                            f"retMsg={ack.get('retMsg','?')!r} — пробуем Gate",
                            flush=True,
                        )
                        bybit_rejected = True
                        break
                    if attempt < _ORDER_RETRIES:
                        time.sleep(_ORDER_RETRY_SLEEP)

                if not success and not bybit_rejected:
                    print(
                        f"[BYBIT-WS] {symbol}: все {_ORDER_RETRIES} ретраев "
                        f"на price-cap — пробуем Gate", flush=True,
                    )

                if success:
                    with _last_exchange_lock:
                        _last_exchange[ticker_name] = "bybit"
                    return amount_tokens, bybit_price
                # Не вышло на Bybit — проваливаемся в Gate fallback (ниже).

    # ── Gate.io fallback ─────────────────────────────────────────
    gate_price = gate_get_price(ticker_name)
    if not gate_price:
        # FIX 2026-06-24: на Bybit-reject (symbol-not-found / нет маржи / etc)
        # И пустой Gate — fail-fast sentinel (-1,0). 3 retry'я воркера всё
        # равно не помогут: причина перманентная, не "no price".
        if bybit_rejected:
            print(f"[NO PRICE ANYWHERE] {ticker_name} — Bybit reject + Gate пустой "
                  f"— fail-fast", flush=True)
            return -1, 0
        print(f"[NO PRICE ANYWHERE] {ticker_name} — нет ни на Bybit ни на Gate.io")
        return 0, 0

    print(f"[GATE-FALLBACK] {ticker_name}: открываем лонг на Gate "
          f"(price={gate_price})", flush=True)
    amount, fill_price = gate_open_long(ticker_name, usdt_amount)
    if amount:
        with _last_exchange_lock:
            _last_exchange[ticker_name] = "gate"
    return amount, fill_price


# ── Batch open: N лонгов одним WS-фреймом ────────────────────────
# FIX 2026-06-19 (R2): на мульти-листинге (2-5 монет) отдельные WS-send'ы
# идут под общим _send_lock последовательно. Bybit op:order.create-batch
# пакует до 10 ордеров в ОДИН фрейм — одна сетевая RTT, одна
# matching-engine queue insertion. Экономия 100-800μs на burst.
#
# Семантика:
#   • Bybit-known монеты идут в batch.
#   • Gate-only монеты (нет на Bybit) → fallback per-coin на market_open_long.
#   • Per-order reject (не price-cap) → fallback на single market_open_long
#     для этой монеты (он сам сделает REST или Gate).
#   • Batch вернул None (sync WS не подключён / timeout) → каждая монета
#     идёт через обычный market_open_long.

def market_open_long_batch(
    items: list[tuple[str, float]],
) -> dict[str, tuple[float, float]]:
    """
    Открывает несколько лонгов одним батчем на Bybit.
    items: [(ticker, usdt_margin), ...] — до 10 элементов.
    Возврат: {ticker: (amount, entry_price)}; (-1, 0) — биржевой REJECT,
             (0, 0) — нет цены/нигде нет. Для отсутствующих монет caller
             должен сам решать.
    """
    out: dict[str, tuple[float, float]] = {}

    # ── 1. Разделяем Bybit-known и Gate-only ─────────────────────
    # Для Bybit-known готовим order_args; Gate-only → сразу single-path.
    batch_meta: list[dict] = []   # [{ticker, symbol, order_args, sl_price, amount, link_id}]
    batch_args: list[dict] = []
    gate_only: list[tuple[str, float]] = []

    for ticker, margin in items:
        bybit_price = get_price(ticker)
        if not (bybit_price and bybit_price > 0):
            gate_only.append((ticker, margin))
            continue

        symbol = f"{ticker}USDT"
        try:
            step = _get_qty_step(symbol)
        except QtyStepUnavailable as e:
            print(f"[QTY STEP MISSING BYBIT] {e} — пробуем Gate.io", flush=True)
            gate_only.append((ticker, margin))
            continue

        raw_qty = (margin / bybit_price) * LEVERAGE
        amount_tokens = _round_qty(raw_qty, step)
        if amount_tokens <= 0:
            print(f"[QTY ZERO BYBIT] {symbol}", flush=True)
            out[ticker] = (0, 0)
            continue

        qty_str = str(amount_tokens)
        link_id = new_order_link_id()
        sl_price = round(bybit_price * _SL_MULT, 8)
        order_args: dict = {
            "category":    "linear",
            "symbol":      symbol,
            "side":        "Buy",
            "orderType":   "Market",
            "qty":         qty_str,
            "positionIdx": 1,
            "orderLinkId": link_id,
            "stopLoss":    str(sl_price),
            "slTriggerBy": "LastPrice",
        }
        if _SLIPPAGE_TOL_PCT > 0:
            order_args["slippageToleranceType"] = "Percent"
            order_args["slippageTolerance"] = str(_SLIPPAGE_TOL_PCT)
        batch_args.append(order_args)
        batch_meta.append({
            "ticker": ticker, "symbol": symbol, "qty_str": qty_str,
            "amount": amount_tokens, "price": bybit_price,
            "sl_price": sl_price, "link_id": link_id, "margin": margin,
        })

    # ── 2. Отправка batch (если есть что слать) ──────────────────
    if batch_args:
        # Bybit V5 linear: до 10 ордеров в одном create-batch.
        # На листинге обычно 2-5, лимит почти никогда не достигается;
        # на всякий случай chunk'ом по 10.
        retry_singles: list[str] = []  # тикеры → single-path retry
        for chunk_start in range(0, len(batch_args), 10):
            chunk_args = batch_args[chunk_start:chunk_start + 10]
            chunk_meta = batch_meta[chunk_start:chunk_start + 10]
            ack = _ws_place_batch_orders_ack(chunk_args)
            if ack is None:
                # Batch не прошёл (sync WS не подключён / timeout) — все
                # тикеры этого chunk'а на single-path fallback.
                for m in chunk_meta:
                    retry_singles.append(m["ticker"])
                continue
            # Bybit V5 create-batch: result.list[] и retExtInfo.list[] —
            # параллельные массивы. retExtInfo[i].code — per-order retCode,
            # 0 = ok.
            result = ack.get("data") or ack.get("result") or {}
            order_list = result.get("list", []) if isinstance(result, dict) else []
            ext_info = (ack.get("retExtInfo") or {}).get("list", [])

            # FIX 2026-06-19 (audit): если batch-level retCode!=0 И ext_info
            # пуст — Bybit отверг ВЕСЬ batch (валидация подписи / payload /
            # rate-limit). Нет смысла говорить worker'у "fail-fast" — single-path
            # либо починит транзиент, либо сам залогирует ту же ошибку. Шлём
            # весь chunk в retry_singles.
            batch_rc = ack.get("retCode", 0)
            if batch_rc != 0 and not ext_info:
                print(
                    f"[BYBIT-WS-BATCH] batch-level reject retCode={batch_rc} "
                    f"retMsg={ack.get('retMsg')!r} — fallback всех {len(chunk_meta)} "
                    f"монет на single-path",
                    flush=True,
                )
                for m in chunk_meta:
                    retry_singles.append(m["ticker"])
                continue

            for i, m in enumerate(chunk_meta):
                per_rc = -1
                per_msg = "?"
                if i < len(ext_info):
                    per_rc = ext_info[i].get("code", -1)
                    per_msg = ext_info[i].get("msg", "?")
                # Bybit может вернуть ack-level reject (retCode!=0), но при
                # этом часть ордеров отдельно успешна — используем per-order.
                if per_rc == 0:
                    # orderId полезен для трекинга; не используется в out.
                    with _last_exchange_lock:
                        _last_exchange[m["ticker"]] = "bybit"
                    out[m["ticker"]] = (m["amount"], m["price"])
                    continue
                if per_rc in _PRICE_CAP_RETCODES:
                    # Price-cap: каждый отдельный ретрай нужен с актуальной
                    # ценой → single-path (он сам сделает _ORDER_RETRIES).
                    retry_singles.append(m["ticker"])
                    continue
                # Другой reject → fail fast для этой монеты.
                print(
                    f"[BYBIT-WS-BATCH] REJECTED {m['symbol']} "
                    f"retCode={per_rc} retMsg={per_msg!r} — fail fast",
                    flush=True,
                )
                out[m["ticker"]] = (-1, 0)

        # ── 3. Single-path fallback для непрошедших ──────────────
        for ticker in retry_singles:
            # margin из meta (1:1 с входом).
            margin = next((m["margin"] for m in batch_meta if m["ticker"] == ticker), 0.0)
            if margin <= 0:
                out[ticker] = (-1, 0)
                continue
            out[ticker] = market_open_long(ticker, margin)

    # ── 4. Gate-only монеты ──────────────────────────────────────
    for ticker, margin in gate_only:
        out[ticker] = market_open_long(ticker, margin)

    return out


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

def _sim_atr_live_trail_params_long(ticker: str, base: float) -> tuple[float, float, float, str]:
    """
    ЖИВОЙ порт tg/exit_strategies.py:exit_atr_trailing для ЛОНГ-позиции.

    Блокирует ~_LISTING_SIM_ATR_PERIOD секунд (default 30) sampling'ом
    price_cache через `get_price` — тот же источник что использует recorder
    в _atr_frac (см. tg/price_recorder.py:_tick + tg/exit_strategies.py:_atr_frac).

    После warmup'а вычисляет atr_frac = mean(|Δprice|)/entry, применяет клэмп
    [_LISTING_SIM_ATR_LO, _LISTING_SIM_ATR_HI], k×atr_frac → trail_frac.

    Активация = trail_frac × base ВЫШЕ base (симовская семантика act = trail).
    SL = _LISTING_SIM_ATR_SL × base НИЖЕ base.
    """
    from api.delist_api import get_price   # shared price_cache reader

    sample_res = live_sample_atr_frac(
        get_price,
        coin=ticker,
        entry=base,
        samples=_LISTING_SIM_ATR_PERIOD,
        period=1.0,
        min_valid=_LISTING_SIM_ATR_MIN_CANDLES,
    )
    if sample_res is not None:
        atr_frac, n_used, peak, trough = sample_res
        raw = _LISTING_SIM_ATR_K * atr_frac
        trail_frac = min(_LISTING_SIM_ATR_HI, max(_LISTING_SIM_ATR_LO, raw))
        atr_tag = (f"atr_frac={atr_frac:.5f} (n={n_used}, "
                   f"peak={peak:.6g}, trough={trough:.6g})")
    else:
        # Симовский fallback (exit_atr_trailing:120): atr=0 → trail=0.02.
        trail_frac = _LISTING_SIM_ATR_FALLBACK
        atr_tag = f"sample=n/a → fallback={_LISTING_SIM_ATR_FALLBACK*100:.1f}%"

    trail_dist  = round(base * trail_frac, 8)
    active_pric = round(base * (1.0 + trail_frac), 8)   # act = trail
    sl_price    = round(base * (1.0 - _LISTING_SIM_ATR_SL), 8)
    tag = (f"sim_atr LIVE {atr_tag} "
           f"({_LISTING_SIM_ATR_PERIOD}×1s post-fill) "
           f"trail={trail_frac*100:.2f}% act={trail_frac*100:.2f}% "
           f"SL={_LISTING_SIM_ATR_SL*100:.1f}%")
    return trail_dist, active_pric, sl_price, tag


def _compute_trail_params_long(symbol: str, base: float) -> tuple[float, float, float, str]:
    """
    Возвращает (trailing_distance, active_price, sl_price, log_tag) для ЛОНГ-позиции.

    Режимы (LISTING_TRAIL_MODE):
      - "sim_atr": порт симуляторного exit_atr_trailing (+14%/+56% в 6ч-карточках).
                   atr_frac = mean(|Δclose|)/entry, trail = k×atr clamped[lo,hi],
                   act = trail, SL = 1%. См. tg/exit_strategies.py:_atr_frac + exit_atr_trailing.
      - "atr":     Wilder-style True Range с floor/ceiling в % от base.
      - "pct":     фикс % (legacy).
    Fallback на pct если ATR-функция вернула None.

    Активация и SL для лонга: ВЫШЕ и НИЖЕ base соответственно.
    """
    # NB: mode "sim_atr" НЕ здесь — sim_atr требует post-fill live sampling
    # (см. _sim_atr_live_trail_params_long, вызывается напрямую из _set_tp_sl_bybit).

    if _LISTING_TRAIL_MODE == "atr" and base > 0:
        atr = compute_atr(
            symbol,
            interval=_LISTING_ATR_INTERVAL,
            period=_LISTING_ATR_PERIOD,
            min_candles=_LISTING_ATR_MIN_CANDLES,
        )
        if atr is not None and atr > 0:
            td = clamp_distance(atr * _LISTING_ATR_TRAIL_MULT, base,
                                _LISTING_ATR_TRAIL_MIN_PCT, _LISTING_ATR_TRAIL_MAX_PCT)
            ad = clamp_distance(atr * _LISTING_ATR_ACT_MULT,   base,
                                _LISTING_ATR_ACT_MIN_PCT,   _LISTING_ATR_ACT_MAX_PCT)
            sd = clamp_distance(atr * _LISTING_ATR_SL_MULT,    base,
                                _LISTING_ATR_SL_MIN_PCT,    _LISTING_ATR_SL_MAX_PCT)
            trail_dist  = round(td, 8)
            active_pric = round(base + ad, 8)
            sl_price    = round(base - sd, 8)
            tag = (f"ATR={atr:.6g} ({_LISTING_ATR_PERIOD}×{_LISTING_ATR_INTERVAL}m) "
                   f"trail={td/base*100:.2f}% SL={sd/base*100:.2f}%")
            return trail_dist, active_pric, sl_price, tag

    # Fallback / pct-mode — legacy формулы.
    trail_dist  = round(base * _TRAIL_PCT, 8)
    active_pric = round(base * _TRAIL_ACT, 8)
    sl_price    = round(base * _SL_MULT,  8)
    tag = f"pct trail={_TRAIL_PCT*100:.1f}% SL={_SL_PCT*100:.1f}%"
    return trail_dist, active_pric, sl_price, tag


def _set_tp_sl_bybit(ticker_name: str, entry_price: float, amount: float) -> str:
    """
    Выставляет нативный trailing stop 1% + аварийный SL 1% на ВСЮ позицию
    (Full-режим). Без фиксированного TP.

    FIX 2026-06-04: старая схема (TP1 +4.5% на 30% + trailing на 70% в
    tpslMode:Partial) на проде НЕ срабатывала — диагностика closed-pnl
    показала закрытия через UNKNOWN, а не PartialTakeProfit/TrailingStop.
    Bybit требует Full-режим для trailing на всю позицию (как на делисте).

    Стратегия «дать импульсу листинга бежать» (env-настраиваемая):
      - trailingStop _TRAIL_PCT (деф. 2.5%) — закрывает только при откате от пика
      - activePrice = entry × (1+_ACTIVE_PCT) (деф. +0.5%) — взвод трейлинга
      - аварийный SL = entry × (1-_SL_PCT) (деф. -1.5%) — если цена сразу против
      - БЕЗ фиксированных TP — чистый трейлинг на всю позицию (Full)
    """
    symbol = f"{ticker_name}USDT"

    try:
        step = _get_qty_step(symbol)
    except QtyStepUnavailable as e:
        print(f"[TP/SL SKIP] {e}")
        return "skip"

    sl_size = str(_round_qty(amount, step))            # вся позиция
    if sl_size == "0.0" or float(sl_size) <= 0:
        print(f"[TP/SL SKIP] {ticker_name}: slSize={sl_size} (amount={amount}, step={step}) — слишком мало")
        return "skip"

    # 1) Ждём появления позиции на бирже и берём РЕАЛЬНУЮ avgPrice. Bundled SL из
    #    order.create уже защищает позицию, пока мы цепляем трейлинг. Если позиция
    #    так и не появилась за окно — она закрылась стопом (дамп первой свечи) ИЛИ
    #    ордер не исполнился. И то и другое — НЕ голая позиция, трейлинг не нужен.
    # FIX 2026-06-19 (R3): сначала пробуем private-WS push-event. Bybit пушит
    # снапшот позиции за ~10-30мс после fill'а — без REST poll'инга (0.3-3с).
    # Если WS не подключён / не дождались — fallback на REST poll как раньше.
    avg = 0.0
    size = 0.0
    pos_deadline = time.monotonic() + _POS_WAIT_RETRIES * _POS_WAIT_SLEEP
    ws_priv_ready = _ws_private is not None and _ws_private.is_ready()
    if ws_priv_ready:
        ws_timeout = max(0.0, pos_deadline - time.monotonic())
        ws_res = _ws_private.wait_for_position(symbol, 1, timeout=ws_timeout)
        if ws_res is not None:
            size, avg, _trailing = ws_res
    if size <= 0:
        # WS не дал — добиваем REST poll'ом, но БЕЗ удвоения окна: ограничиваем
        # общий бюджет pos_deadline. Гарантируем как минимум 1 last-check REST
        # запрос (мог прийти ack между WS-таймаутом и сейчас).
        while True:
            size, avg, _trailing = get_position(symbol, 1)
            if size > 0:
                break
            remaining = pos_deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(_POS_WAIT_SLEEP, remaining))
    if size <= 0:
        print(f"[TP/SL] {ticker_name}: позиции нет — закрыта стопом или не исполнилась "
              f"(bundled SL отработал; трейлить нечего)", flush=True)
        return "no-position"

    # 2) Якорим SL/трейлинг к РЕАЛЬНОЙ цене входа (кеш-цена сигнала бывает на проценты ниже).
    base = avg if avg > 0 else entry_price

    # FIX 2026-07-07: sim_atr режим — точный порт tg/exit_strategies.py:exit_atr_trailing.
    # БЛОКИРУЕТ поток на _LISTING_SIM_ATR_PERIOD сек (~30), сэмплит price_cache
    # с 1-сек cadence через тот же get_price что recorder — точная имитация того,
    # что видит sim в warmup фазе. Позицию защищает bundled SL из order.create.
    # Если позиция закрылась во время sampling'а (bundled SL fired) — return "closed".
    if _LISTING_TRAIL_MODE == "sim_atr":
        print(f"[TP/SL SIM_ATR] {ticker_name}: warmup {_LISTING_SIM_ATR_PERIOD}с "
              f"(bundled SL держит)...", flush=True)
        trailing_distance, active_price, sl, trail_tag = \
            _sim_atr_live_trail_params_long(ticker_name, base)
        # Позиция могла закрыться bundled-SL за warmup — перечитываем.
        size, _, _ = get_position(symbol, 1)
        if size <= 0:
            print(f"[TP/SL] {ticker_name}: позиция закрылась за sim_atr warmup "
                  f"(bundled SL сработал) — трейлить нечего", flush=True)
            return "closed"
    else:
        trailing_distance, active_price, sl, trail_tag = _compute_trail_params_long(symbol, base)

    # 3) Ставим трейлинг (+ переякоренный SL), ПРОВЕРЯЯ что трейлинг реально сел.
    #    Пока позиция жива — ретраим (это и есть «трейлинг если возможно»). При отказе
    #    комбинированного вызова шлём trailing-only (omit stopLoss НЕ сбрасывает bundled SL).
    for attempt in range(1, _TRAIL_VERIFY_RETRIES + 1):
        try:
            _trading_stop_settle({
                "category":     "linear", "symbol": symbol, "positionIdx": 1,
                "stopLoss":     str(sl), "slTriggerBy": "LastPrice", "slSize": sl_size,
                "trailingStop": str(trailing_distance), "activePrice": str(active_price),
            }, tag=ticker_name)
        except Exception as e_comb:  # noqa: BLE001
            try:  # SL переякорить не вышло — хотя бы трейлинг (bundled SL остаётся)
                _trading_stop_settle({
                    "category": "linear", "symbol": symbol, "positionIdx": 1,
                    "trailingStop": str(trailing_distance),
                }, tag=f"{ticker_name}-TS")
            except Exception as e_ts:  # noqa: BLE001
                print(f"[TP/SL] {ticker_name} попытка {attempt}: "
                      f"комбо={e_comb}; trailing-only={e_ts}", flush=True)

        # FIX 2026-06-19 (R3): сначала пробуем WS-cache (push trailing). Если push
        # пришёл за период sleep'а — подтверждение мгновенное; иначе REST добивает.
        # trailing_on — bool: либо WS показал trailing>0, либо REST вернул True.
        size_w = 0.0
        trail_w = 0.0
        if ws_priv_ready:
            snap = _ws_private.get_position_cached(symbol, 1)
            if snap is not None:
                size_w, _, trail_w = snap
        if size_w > 0 and trail_w > 0:
            size, trailing_on = size_w, True
        else:
            size, _, trailing_on = get_position(symbol, 1)
        if size <= 0:
            print(f"[TP/SL] {ticker_name}: позиция закрылась пока ставили трейлинг "
                  f"(стоп/трейлинг отработал) — ок", flush=True)
            return "closed"
        if trailing_on:
            print(f"[TP/SL SET LONG] {ticker_name} | entry={base:.8g}(реал) | "
                  f"SL={sl} | Trailing={trailing_distance} "
                  f"(active@{active_price}) | {trail_tag} — ПОДТВЕРЖДЁН на бирже",
                  flush=True)
            return "ok"
        if attempt < _TRAIL_VERIFY_RETRIES:
            # Ждём WS push'а (если подключён) — придёт за 10-50мс если биржа
            # реально приняла trading-stop. wait_for_position_trailing уже спит
            # timeout — отдельный sleep не нужен (хоть push пришёл, хоть нет).
            if ws_priv_ready:
                _ws_private.wait_for_position_trailing(
                    symbol, 1, timeout=_TRAIL_VERIFY_SLEEP,
                )
            else:
                time.sleep(_TRAIL_VERIFY_SLEEP)

    # Позиция жива, но трейлинг не подтвердился. Bundled SL её защищает — громкий warn.
    print(f"[TP/SL WARN] {ticker_name}: трейлинг НЕ подтверждён за {_TRAIL_VERIFY_RETRIES} "
          f"попыток, позиция ЖИВА и защищена bundled SL. ПРОВЕРЬ ТРЕЙЛИНГ!", flush=True)
    return "sl-only"


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
