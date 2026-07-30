from __future__ import annotations

# ── parser_arbitrage.py ───────────────────────────────────────────
# Мониторит спред между Bybit, Gate.io и Hyperliquid.
# При хорошем спреде + фандинге + открытых депозитах — входит в арбитраж.
# ─────────────────────────────────────────────────────────────────

import threading
import time

from api.delist_api import (
    price_updater,
    warmup_bybit_connection,
    preload_lot_steps,
    known_coins,
    get_price,
)
from api.gate_api import (
    gate_price_updater,
    gate_preload_lot_steps,
    gate_known_coins,
    warmup_gate_connection,
    gate_get_price,
)
from api.hl_api import (
    hl_price_updater,
    hl_known_coins,
    hl_get_price,
    warmup_hl_connection,
)
from api.arbitrage_api import (
    get_spread,
    get_combined_hourly_funding,
    get_bybit_funding,   # noqa: F401 — публичное API модуля
    get_gate_funding,    # noqa: F401
    get_hl_funding,      # noqa: F401
    calc_hourly_funding,
    get_all_bybit_funding,
    get_all_gate_funding,
    get_all_hl_funding,
    check_bybit_deposits,
    check_gate_deposits,
    open_arb_position,
    open_funding_position,
    monitor_position,
    active_positions,
    positions_lock,
    MIN_SPREAD_PCT,
    MAX_POSITIONS,
    MAX_FUNDING_HOURLY,
    MIN_FUNDING_HOURLY,
    EXIT_FUNDING_HOURLY,
    MIN_HOLD_HOURS,
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

def log_info(tag, msg):  _log(tag, CYAN,    msg)
def log_ok(tag, msg):    _log(tag, GREEN,   msg)
def log_warn(tag, msg):  _log(tag, YELLOW,  msg)
def log_err(tag, msg):   _log(tag, RED,     msg)


# ── Настройки ─────────────────────────────────────────────────────
SCAN_INTERVAL     = 10    # секунд между сканированиями
DEPOSIT_CACHE_TTL = 300   # кэш депозитов 5 минут

# Максимальный допустимый спред.
# Спред > 100% почти наверняка означает разные токены с одинаковым тикером
# (например EDGE на Gate — это другой проект, чем EDGE на Bybit).
MAX_SPREAD_PCT = 100.0

# Сколько секунд не трогать монету после того как она не прошла временные фильтры
SKIP_CACHE_TTL = 3600  # 1 час


# ── Кэш депозитов (чтобы не долбить API каждые 10 сек) ───────────
_deposit_cache: dict[str, tuple[bool, float]] = {}  # {ticker: (ok, timestamp)}
_deposit_lock = threading.Lock()


def _check_deposits_cached(ticker: str) -> bool:
    """Проверяет депозиты с кэшированием на 5 минут."""
    now = time.time()
    with _deposit_lock:
        cached = _deposit_cache.get(ticker)
        if cached and now - cached[1] < DEPOSIT_CACHE_TTL:
            return cached[0]

    bybit_ok = check_bybit_deposits(ticker)
    gate_ok  = check_gate_deposits(ticker)
    result   = bybit_ok and gate_ok

    with _deposit_lock:
        _deposit_cache[ticker] = (result, now)

    return result


# ── Чёрный список разных токенов (сохраняется на диск) ───────────
# Если спред аномальный (>MAX_SPREAD_PCT) — на Gate и Bybit разные проекты
# с одинаковым тикером. Это не изменится никогда, поэтому пишем в файл.
BLACKLIST_FILE = "different_tokens_blacklist.txt"
_different_token_blacklist: set[str] = set()
_blacklist_lock = threading.Lock()


def _load_blacklist() -> None:
    """Загружает чёрный список с диска при старте."""
    try:
        with open(BLACKLIST_FILE, "r") as f:
            for line in f:
                ticker = line.strip()
                if ticker:
                    _different_token_blacklist.add(ticker)
        log_warn("BLACKLIST", f"Загружено {len(_different_token_blacklist)} тикеров из {BLACKLIST_FILE}")
    except FileNotFoundError:
        pass  # первый запуск — файла ещё нет


def _blacklist(ticker: str, spread_pct: float) -> None:
    """Добавляет тикер в постоянный чёрный список и сохраняет на диск."""
    with _blacklist_lock:
        if ticker in _different_token_blacklist:
            return
        _different_token_blacklist.add(ticker)
        with open(BLACKLIST_FILE, "a") as f:
            f.write(ticker + "\n")
    log_warn("BLACKLIST", f"{ticker} → разные токены на биржах (спред {spread_pct:.0f}%), записан в {BLACKLIST_FILE}")


def _is_blacklisted(ticker: str) -> bool:
    with _blacklist_lock:
        return ticker in _different_token_blacklist


# ── Временный кэш пропуска монет ─────────────────────────────────
# Для депозитов/фандинга — временный бан, они могут измениться.
_skip_cache: dict[str, float] = {}  # {ticker: banned_until_timestamp}
_skip_lock = threading.Lock()


def _is_skipped(ticker: str) -> bool:
    """Возвращает True если монета сейчас во временном бане."""
    now = time.time()
    with _skip_lock:
        until = _skip_cache.get(ticker)
        return bool(until and now < until)


def _skip(ticker: str, reason: str, ttl: int = SKIP_CACHE_TTL) -> None:
    """Временно банит монету на ttl секунд."""
    until = time.time() + ttl
    with _skip_lock:
        _skip_cache[ticker] = until
    log_warn("SKIP", f"{ticker} → бан на {ttl//60} мин | причина: {reason}")


# ── Основной сканер ───────────────────────────────────────────────

def scan_spreads() -> None:
    """
    Основной цикл — сканирует все монеты которые есть на обеих биржах.
    При нахождении хорошего спреда — проверяет фандинг и депозиты,
    затем открывает арбитражную позицию.
    """
    log_ok("ARB", "Сканер спредов запущен")

    while True:
        time.sleep(SCAN_INTERVAL)

        with positions_lock:
            if len(active_positions) >= MAX_POSITIONS:
                log_warn("ARB", f"Достигнут лимит позиций ({MAX_POSITIONS}), ждём...")
                continue

        # Монеты которые есть хотя бы на двух биржах
        common_coins = (
            (known_coins & gate_known_coins) |
            (known_coins & hl_known_coins)   |
            (gate_known_coins & hl_known_coins)
        )
        if not common_coins:
            log_warn("ARB", "Нет общих монет между биржами")
            continue

        best_ticker     = None
        best_spread     = 0.0
        best_long_exch  = ""
        best_short_exch = ""

        for ticker in common_coins:
            with positions_lock:
                if ticker in active_positions:
                    continue

            if _is_blacklisted(ticker):
                continue

            if _is_skipped(ticker):
                continue

            result = get_spread(ticker)
            if not result:
                continue

            spread_pct, long_exchange, short_exchange = result

            # Аномальный спред → разные проекты с одинаковым тикером
            if spread_pct > MAX_SPREAD_PCT:
                _blacklist(ticker, spread_pct)
                continue

            if spread_pct >= MIN_SPREAD_PCT and spread_pct > best_spread:
                best_spread     = spread_pct
                best_ticker     = ticker
                best_long_exch  = long_exchange
                best_short_exch = short_exchange

        if not best_ticker:
            log_info("ARB", f"Нет монет со спредом >= {MIN_SPREAD_PCT}% | проверено {len(common_coins)} монет")
            continue

        log_ok("ARB", f"Найден спред: {best_ticker} | {best_spread:.2f}% | лонг={best_long_exch} шорт={best_short_exch}")

        # ── Проверка фандинга ─────────────────────────────────────
        hourly_funding = get_combined_hourly_funding(best_ticker, best_long_exch, best_short_exch)

        if hourly_funding < MAX_FUNDING_HOURLY:
            log_warn("ARB", f"{best_ticker} | фандинг против нас ({hourly_funding*100:.4f}%/ч) — пропускаем")
            _skip(best_ticker, f"плохой фандинг {hourly_funding*100:.4f}%/ч", ttl=3600)
            continue

        log_ok("ARB", f"{best_ticker} | фандинг ОК ({hourly_funding*100:.4f}%/ч)")

        # ── Проверка депозитов ────────────────────────────────────
        if not _check_deposits_cached(best_ticker):
            log_warn("ARB", f"{best_ticker} | депозиты закрыты — пропускаем")
            _skip(best_ticker, "депозиты закрыты", ttl=DEPOSIT_CACHE_TTL)
            continue

        log_ok("ARB", f"{best_ticker} | депозиты открыты ✓")

        # ── Открываем позицию ─────────────────────────────────────
        with positions_lock:
            if len(active_positions) >= MAX_POSITIONS:
                continue
            if best_ticker in active_positions:
                continue

        pos = open_arb_position(best_ticker, best_long_exch, best_short_exch, best_spread)

        if not pos:
            log_err("ARB", f"{best_ticker} | не удалось открыть позицию")
            _skip(best_ticker, "ошибка открытия позиции", ttl=60)
            continue

        with positions_lock:
            active_positions[best_ticker] = pos

        threading.Thread(
            target=monitor_position,
            args=(pos,),
            daemon=True,
            name=f"arb-monitor-{best_ticker}",
        ).start()

        tg_log(
            f"⚖️ <b>ARB ОТКРЫТ</b> {best_ticker}\n"
            f"Спред: {best_spread:.2f}%\n"
            f"Лонг: {best_long_exch.upper()} | Шорт: {best_short_exch.upper()}\n"
            f"Фандинг: {hourly_funding*100:.4f}%/ч"
        )
        log_ok("ARB", f"{best_ticker} | позиция открыта, мониторинг запущен")


# ── Сканер фандинга ───────────────────────────────────────────────

def scan_funding() -> None:
    """
    Фоновый цикл — ищет монеты с высоким суммарным фандингом.

    Логика входа (пар-специфичная):
      Для каждой пары бирж считаем точную стоимость комиссий.
      MIN = fee_cost / 6ч  (хотим отбить комиссии за 6 часов).
        Bybit↔Gate:  0.210% / 6 = 0.035%/ч
        Bybit↔HL:    0.160% / 6 = 0.027%/ч
        Gate↔HL:     0.150% / 6 = 0.025%/ч

    Логика выбора стороны:
      Шортим там где фандинг ВЫШЕ (получаем больше).
      Лонг там где фандинг НИЖЕ (платим меньше).
      HL обычно имеет более высокий и волатильный фандинг → шорт на HL.

    Проверка спреда перед входом:
      Если лонг-биржа дороже шорт-биржи → мы теряем на открытии.
      Считаем сколько часов фандинга нужно чтобы отбить этот спред.
      Если > MAX_SPREAD_BREAKEVEN_HOURS → пропускаем.
    """
    log_ok("FUND", (
        f"Сканер фандинга запущен | "
        f"Gate↔HL вход: {MIN_FUNDING_HOURLY*100:.3f}%/ч | "
        f"выход: {EXIT_FUNDING_HOURLY*100:.4f}%/ч | "
        f"мин.удержание: {MIN_HOLD_HOURS}ч"
    ))

    # Пар-специфичные пороги входа
    # fee_cost / TARGET_HOURS = мин.фандинг для отбития комиссий за TARGET_HOURS
    PAIR_MIN_FUNDING: dict[frozenset, float] = {
        frozenset({"bybit", "gate"}):        0.00210 / 6,   # 0.0350%/ч
        frozenset({"bybit", "hyperliquid"}): 0.00160 / 6,   # 0.0267%/ч
        frozenset({"gate",  "hyperliquid"}): 0.00150 / 6,   # 0.0250%/ч
    }
    MAX_SPREAD_BREAKEVEN_HOURS = 3.0  # максимум часов чтобы отбить спред входа

    while True:
        time.sleep(SCAN_INTERVAL)

        with positions_lock:
            if len(active_positions) >= MAX_POSITIONS:
                continue

        common_coins = (
            (known_coins & gate_known_coins) |
            (known_coins & hl_known_coins)   |
            (gate_known_coins & hl_known_coins)
        )
        if not common_coins:
            continue

        # Загружаем фандинг всех бирж за 3 запроса
        bybit_funding_all = get_all_bybit_funding()
        gate_funding_all  = get_all_gate_funding()
        hl_funding_all    = get_all_hl_funding()
        log_info("FUND", (
            f"Загружен фандинг: "
            f"bybit={len(bybit_funding_all)} "
            f"gate={len(gate_funding_all)} "
            f"hl={len(hl_funding_all)}"
        ))

        best_ticker     = None
        best_net        = 0.0
        best_long_exch  = ""
        best_short_exch = ""

        for ticker in common_coins:
            with positions_lock:
                if ticker in active_positions:
                    continue

            if _is_blacklisted(ticker) or _is_skipped(ticker):
                continue

            bybit_rate, bybit_next = bybit_funding_all.get(ticker, (0.0, 0.0))
            gate_rate,  gate_next  = gate_funding_all.get(ticker,  (0.0, 0.0))
            hl_rate,    hl_next    = hl_funding_all.get(ticker,    (0.0, 0.0))

            bybit_price = get_price(ticker)
            gate_price  = gate_get_price(ticker)
            hl_price    = hl_get_price(ticker)

            exchanges: dict[str, tuple] = {}
            if bybit_price: exchanges["bybit"]       = (bybit_price, bybit_rate, bybit_next)
            if gate_price:  exchanges["gate"]        = (gate_price,  gate_rate,  gate_next)
            if hl_price:    exchanges["hyperliquid"] = (hl_price,    hl_rate,    hl_next)

            if len(exchanges) < 2:
                continue

            ticker_best_net   = 0.0
            ticker_long_exch  = ""
            ticker_short_exch = ""

            exch_list = list(exchanges.keys())
            for i, ex_a in enumerate(exch_list):
                for ex_b in exch_list[i+1:]:
                    price_a, rate_a, next_a = exchanges[ex_a]
                    price_b, rate_b, next_b = exchanges[ex_b]

                    pair_key    = frozenset({ex_a, ex_b})
                    pair_min    = PAIR_MIN_FUNDING.get(pair_key, MIN_FUNDING_HOURLY)

                    # Считаем нетто-фандинг для обоих вариантов направления
                    # Вариант A: лонг на ex_a, шорт на ex_b
                    net_ab = (
                        calc_hourly_funding(rate_a, next_a, is_long=True) +
                        calc_hourly_funding(rate_b, next_b, is_long=False)
                    )
                    # Вариант B: лонг на ex_b, шорт на ex_a
                    net_ba = (
                        calc_hourly_funding(rate_b, next_b, is_long=True) +
                        calc_hourly_funding(rate_a, next_a, is_long=False)
                    )

                    # Выбираем лучший вариант
                    if net_ab >= net_ba:
                        net_best        = net_ab
                        long_exch_pair  = ex_a
                        short_exch_pair = ex_b
                        long_price      = price_a
                        short_price     = price_b
                    else:
                        net_best        = net_ba
                        long_exch_pair  = ex_b
                        short_exch_pair = ex_a
                        long_price      = price_b
                        short_price     = price_a

                    # Фильтр 1: нетто-фандинг должен быть выше пар-специфичного порога
                    if net_best < pair_min:
                        continue

                    # Фильтр 2: если лонг-биржа дороже шорт-биржи — теряем на входе.
                    # Считаем сколько часов фандинга нужно чтобы отбить этот спред.
                    if long_price > short_price and short_price > 0:
                        entry_loss_pct     = (long_price - short_price) / short_price
                        fee_cost           = 0.00210   # берём максимальную
                        hours_to_breakeven = (entry_loss_pct + fee_cost) / net_best
                        if hours_to_breakeven > MAX_SPREAD_BREAKEVEN_HOURS:
                            log_warn(
                                "FUND",
                                f"{ticker} {long_exch_pair}→{short_exch_pair} | "
                                f"спред входа -{entry_loss_pct*100:.2f}% | "
                                f"отобьётся за {hours_to_breakeven:.1f}ч > {MAX_SPREAD_BREAKEVEN_HOURS}ч — пропуск"
                            )
                            continue

                    if net_best > ticker_best_net:
                        ticker_best_net   = net_best
                        ticker_long_exch  = long_exch_pair
                        ticker_short_exch = short_exch_pair

            if ticker_best_net <= 0:
                continue

            if ticker_best_net > best_net:
                best_net        = ticker_best_net
                best_ticker     = ticker
                best_long_exch  = ticker_long_exch
                best_short_exch = ticker_short_exch

        if not best_ticker:
            continue

        pair_key  = frozenset({best_long_exch, best_short_exch})
        pair_min  = PAIR_MIN_FUNDING.get(pair_key, MIN_FUNDING_HOURLY)
        be_hours  = 0.00210 / best_net if best_net > 0 else 0

        log_ok("FUND", (
            f"Найден фандинг: {best_ticker} | "
            f"net={best_net*100:.4f}%/ч | "
            f"лонг={best_long_exch} шорт={best_short_exch} | "
            f"порог={pair_min*100:.4f}%/ч | "
            f"breakeven≈{be_hours:.1f}ч"
        ))

        with positions_lock:
            if len(active_positions) >= MAX_POSITIONS:
                continue
            if best_ticker in active_positions:
                continue

        pos = open_funding_position(best_ticker, best_long_exch, best_short_exch, best_net)

        if not pos:
            log_err("FUND", f"{best_ticker} | не удалось открыть позицию")
            _skip(best_ticker, "ошибка открытия (funding)", ttl=60)
            continue

        with positions_lock:
            active_positions[best_ticker] = pos

        threading.Thread(
            target=monitor_position,
            args=(pos,),
            daemon=True,
            name=f"fund-monitor-{best_ticker}",
        ).start()

        tg_log(
            f"💰 <b>FUNDING ОТКРЫТ</b> {best_ticker}\n"
            f"Net фандинг: +{best_net*100:.4f}%/ч\n"
            f"Лонг: {best_long_exch.upper()} | Шорт: {best_short_exch.upper()}\n"
            f"Breakeven: ≈{be_hours:.1f}ч\n"
            f"Выход при фандинге &lt; {pos.exit_funding_hourly*100:.4f}%/ч\n"
            f"Мин. удержание: {MIN_HOLD_HOURS}ч"
        )
        log_ok("FUND", f"{best_ticker} | позиция открыта, мониторинг запущен")


# ── Heartbeat ─────────────────────────────────────────────────────

def _heartbeat() -> None:
    while True:
        time.sleep(3600)
        with positions_lock:
            n       = len(active_positions)
            tickers = list(active_positions.keys())
        tg_log(
            f"✅ <b>ARB парсер работает</b>\n"
            f"Активных позиций: {n}\n"
            f"Монеты: {', '.join(tickers) if tickers else 'нет'}"
        )


# ── Запуск ────────────────────────────────────────────────────────

if __name__ == "__main__":
    threading.Thread(target=price_updater,      daemon=True).start()
    threading.Thread(target=gate_price_updater, daemon=True).start()
    threading.Thread(target=hl_price_updater,   daemon=True).start()
    log_ok("CACHE", "price_updater (Bybit + Gate.io + Hyperliquid) запущен в фоне")

    warmup_bybit_connection()
    warmup_gate_connection()
    warmup_hl_connection()
    preload_lot_steps()
    gate_preload_lot_steps()

    _load_blacklist()
    log_ok("ARB", "Ждём 5с пока price_cache наполнится...")
    time.sleep(5)

    tg_log(
        "⚖️ <b>ARB парсер запущен</b>\n"
        f"Мин. спред: {MIN_SPREAD_PCT}%\n"
        f"Фандинг (Gate↔HL):  вход ≥ 0.025%/ч | выход &lt; 0.009%/ч\n"
        f"Фандинг (Bybit↔HL): вход ≥ 0.027%/ч | выход &lt; 0.009%/ч\n"
        f"Фандинг (CEX↔CEX):  вход ≥ 0.035%/ч | выход &lt; 0.009%/ч\n"
        f"Мин. удержание: {MIN_HOLD_HOURS}ч\n"
        f"Макс. позиций: {MAX_POSITIONS}\n"
        f"Биржи: Bybit + Gate.io + Hyperliquid"
    )

    log_ok("ARB", f"Запускаем сканер (интервал {SCAN_INTERVAL}с)")

    threading.Thread(target=_heartbeat,   daemon=True).start()
    threading.Thread(target=scan_funding, daemon=True, name="scan-funding").start()

    # Основной сканер спредов (блокирующий)
    scan_spreads()