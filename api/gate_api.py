from __future__ import annotations

# ── gate_api.py ───────────────────────────────────────────────────
# Gate.io фьючерсы (бессрочные контракты USDT).
# Используется как fallback когда токена нет на Bybit.
# Кэширует цены заранее через gate_price_updater() —
# при сигнале открытие такое же быстрое как на Bybit.
# ─────────────────────────────────────────────────────────────────

import hashlib
import hmac
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, ROUND_HALF_UP

import requests

from config.config import GATEIO_API_KEY, GATEIO_SECRET_KEY

# Защита от None если ключи не заданы в .env
GATEIO_API_KEY    = GATEIO_API_KEY    or ""
GATEIO_SECRET_KEY = GATEIO_SECRET_KEY or ""

# ── ANSI цвета ────────────────────────────────────────────────────
GREEN   = "\033[92m"
RED     = "\033[91m"
YELLOW  = "\033[93m"
CYAN    = "\033[96m"
MAGENTA = "\033[95m"
BOLD    = "\033[1m"
RESET   = "\033[0m"

def _tag(label: str, color: str) -> str:
    return f"{color}{BOLD}[{label}]{RESET}"

# ── константы ─────────────────────────────────────────────────────
_GATE_BASE = "https://api.gateio.ws"
_SETTLE    = "usdt"   # USDT-маргинальные бессрочники
_LEVERAGE  = 10
_TRAILING_MAX_LIFETIME = 24 * 3600   # FIX: трейлинг не висит дольше 24ч

# ── кэш цен ───────────────────────────────────────────────────────
gate_price_cache: dict[str, float] = {}   # {"BTC": 65000.0, ...}
gate_known_coins: set[str]         = set()
_cache_lock = threading.Lock()
# FIX (review high): age-gate против money-losing сайзинга по замороженной
# цене при сбое Gate. gate_get_price → 0 если кэш протух → caller не торгует
# по старой цене (market_open_* трактует 0 как "нет цены").
_gate_cache_updated_at = 0.0
_GATE_STALE_SEC = 10.0   # обновление каждые 3с → 3 пропуска = устарело

# ── HTTP сессия ───────────────────────────────────────────────────
_session = requests.Session()
_session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

# FIX: кэш установленных плеч, чтобы не дёргать API перед каждым ордером.
_leverage_set_for: set[str] = set()
_leverage_lock = threading.Lock()

# M9: фоновый пул для установки плеча вне hot-path market_open_long.
_leverage_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="gate-lev")


# ── авторизация ───────────────────────────────────────────────────

def _sign(method: str, path: str, query: str = "", body: str = "") -> dict:
    ts        = str(int(time.time()))
    body_hash = hashlib.sha512(body.encode()).hexdigest()
    msg       = "\n".join([method.upper(), path, query, body_hash, ts])
    sig       = hmac.new(GATEIO_SECRET_KEY.encode(), msg.encode(), hashlib.sha512).hexdigest()
    return {"KEY": GATEIO_API_KEY, "Timestamp": ts, "SIGN": sig}


def _get(path: str, params: dict | None = None) -> dict:
    resp = _session.get(f"{_GATE_BASE}{path}", params=params, timeout=5)
    resp.raise_for_status()
    return resp.json()


def _post_signed(path: str, body: dict) -> dict:
    body_str = json.dumps(body)
    headers  = _sign("POST", path, "", body_str)
    resp     = _session.post(f"{_GATE_BASE}{path}", data=body_str, headers=headers, timeout=5)
    resp.raise_for_status()
    return resp.json()


# ── кэш цен ───────────────────────────────────────────────────────

def gate_price_updater() -> None:
    """Бесконечный цикл — каждые 3 секунды обновляет кэш цен с Gate.io."""
    print(f"{_tag('GATE', CYAN)} price_updater запущен")
    while True:
        try:
            tickers   = _get(f"/api/v4/futures/{_SETTLE}/tickers")
            new_cache: dict[str, float] = {}
            new_coins: set[str]         = set()

            for t in tickers:
                contract = t.get("contract", "")
                last     = t.get("last", "0")
                if not contract.endswith("_USDT"):
                    continue
                ticker = contract.replace("_USDT", "")
                price  = float(last)
                if price > 0:
                    new_cache[ticker] = price
                    new_coins.add(ticker)

            with _cache_lock:
                gate_price_cache.clear()
                gate_price_cache.update(new_cache)
                gate_known_coins.clear()
                gate_known_coins.update(new_coins)
                global _gate_cache_updated_at
                _gate_cache_updated_at = time.monotonic()

        except Exception as e:
            print(f"{_tag('GATE ERR', RED)} price_updater: {type(e).__name__}: {e}")

        time.sleep(3)


def gate_get_price(ticker: str) -> float:
    # FIX (review high): age-gate — 0 если кэш протух (сбой Gate).
    if (time.monotonic() - _gate_cache_updated_at) > _GATE_STALE_SEC:
        return 0
    with _cache_lock:
        return gate_price_cache.get(ticker, 0)


# ── шаг лота и цены ───────────────────────────────────────────────

_price_steps_gate: dict[str, float] = {}
_min_order_gate:   dict[str, int]   = {}
_quanto_gate:      dict[str, float] = {}

_lot_steps_lock = threading.Lock()


def gate_preload_lot_steps() -> None:
    """
    Загружает шаги цены и минимальные размеры ордеров с Gate.io.

    Gate.io USDT-фьючерсы:
        quanto_multiplier — количество монет в одном контракте.
        contract_value (USDT) = quanto_multiplier * price
        contracts = notional / contract_value = (margin * leverage) / (quanto * price)
    """
    try:
        contracts = _get(f"/api/v4/futures/{_SETTLE}/contracts")
        with _lot_steps_lock:
            for c in contracts:
                name       = c.get("name", "").replace("_USDT", "")
                price_step = float(c.get("order_price_round", "0.0001") or "0.0001")
                min_size = int(c.get("order_size_min", 1) or 1)
                quanto = float(c.get("quanto_multiplier", "1") or "1")

                _price_steps_gate[name] = price_step
                _min_order_gate[name] = min_size
                _quanto_gate[name] = quanto
        print(f"{_tag('GATE', CYAN)} Загружено {len(_price_steps_gate)} контрактов")
    except Exception as e:
        print(f"{_tag('GATE ERR', RED)} preload_lot_steps: {e}")


def _gate_min_order(ticker: str) -> int:
    """Возвращает минимальный размер ордера в контрактах (обычно 1)."""
    with _lot_steps_lock:
        return _min_order_gate.get(ticker, 1)


def _gate_calc_contracts(
    ticker: str,
    usdt_amount: float,
    leverage: int = _LEVERAGE,
) -> int:
    """
    Расчёт количества контрактов для Gate.io USDT-фьючерсов.

    notional = usdt_amount * leverage           — стоимость позиции в USDT
    contract_value = quanto_multiplier * price  — стоимость 1 контракта в USDT
    contracts = notional / contract_value
    """
    price = gate_get_price(ticker)
    if not price or price <= 0:
        return _gate_min_order(ticker)

    quanto = _quanto_gate.get(ticker, 1.0)

    notional = usdt_amount * leverage
    contract_value = quanto * price

    if contract_value <= 0:
        return _gate_min_order(ticker)

    contracts = int(notional / contract_value)
    min_sz = _gate_min_order(ticker)
    return max(contracts, min_sz)


def _gate_round_price(ticker: str, price: float) -> str:
    """Округляет цену до шага котировки контракта."""
    step = _price_steps_gate.get(ticker, 0.0001)
    if step <= 0:
        step = 0.0001
    # M7: было round(round(price/step)*step, 10) — двойное округление с
    # banker's rounding (round-half-to-even) на первом round() давало ±1 тик
    # (напр. 100.005 при step=0.01 → 100.00 вместо 100.01). Decimal с
    # ROUND_HALF_UP считает шаг точно, без float-погрешности.
    _d_step  = Decimal(str(step))
    rounded  = float((Decimal(str(price)) / _d_step).quantize(Decimal(1), rounding=ROUND_HALF_UP) * _d_step)
    step_str = f"{step:.10f}".rstrip("0")
    if "." in step_str:
        decimals = len(step_str.split(".")[1])
    else:
        decimals = 0
    decimals = max(0, decimals)
    return f"{rounded:.{decimals}f}"


# ── установка плеча ───────────────────────────────────────────────

def _gate_set_leverage(contract: str, leverage: int = _LEVERAGE, cross: bool = True) -> bool:
    """
    Устанавливает плечо для контракта на Gate.io.
    Возвращает True если успешно.

    ВАЖНО: Gate.io API /positions/{contract}/leverage принимает параметры
    НЕ в теле запроса, а как query-параметры в URL. Тело должно быть пустым.

    Gate.io кросс-маржа:  leverage=0 & cross_leverage_limit=N
    Gate.io изолированная: leverage=N & cross_leverage_limit=0
    """
    # FIX: кэшируем — если плечо для этого контракта уже устанавливали,
    # не дёргаем Gate API повторно (экономит 100-300мс перед ордером).
    cache_key = f"{contract}:{leverage}:{int(cross)}"
    with _leverage_lock:
        if cache_key in _leverage_set_for:
            return True

    try:
        path = f"/api/v4/futures/{_SETTLE}/positions/{contract}/leverage"

        if cross:
            query = f"leverage=0&cross_leverage_limit={leverage}"
        else:
            query = f"leverage={leverage}&cross_leverage_limit=0"

        headers = _sign("POST", path, query, "")
        url     = f"{_GATE_BASE}{path}?{query}"

        resp = _session.post(url, data="", headers=headers, timeout=5)

        if resp.status_code == 200:
            with _leverage_lock:
                _leverage_set_for.add(cache_key)
            # FIX-LOG: success-print убран — на старте gate_leverage_presetter
            # делает 700+ вызовов, забивает stdout. Кэш виден в итоговом
            # "GATE PRESET sweep#N ✓ ... новых в кэше=...".
            return True
        else:
            # Тихо игнорим LEVERAGE_EXCEEDED — для некоторых контрактов
            # Gate ограничивает плечо. Этих контрактов мало, ошибки не помогают.
            if "LEVERAGE_EXCEEDED" not in resp.text:
                print(
                    f"{_tag('GATE LEV ERR', RED)} {contract}: "
                    f"HTTP {resp.status_code} | {resp.text[:120]}"
                )
            return False
    except Exception as e:
        print(f"{_tag('GATE LEV ERR', RED)} {contract}: {e}")
        return False


def _gate_set_leverage_async(contract: str, leverage: int = _LEVERAGE, cross: bool = True) -> None:
    """
    M9: ставит плечо в ФОНЕ — не блокирует hot-path. На cache-hit ничего не
    делает (плечо уже установлено). На cold-тикере submit'ит установку в пул
    и сразу возвращается; ордер уходит, не дожидаясь ~150мс HTTP POST.
    Корректность размера: contracts считаются от КОНСТАНТЫ _LEVERAGE, а НЕ от
    состояния биржи. Если фоновая установка не успеет и Gate отвергнет ордер
    по марже — self-heal в _gate_open доставит плечо синхронно и повторит.
    """
    cache_key = f"{contract}:{leverage}:{int(cross)}"
    with _leverage_lock:
        if cache_key in _leverage_set_for:
            return
    try:
        _leverage_executor.submit(_gate_set_leverage, contract, leverage, cross)
    except Exception:
        # Пул переполнен/закрыт — ставим синхронно как fallback (хуже по
        # латентности, но корректность важнее).
        _gate_set_leverage(contract, leverage, cross)


def _is_leverage_margin_error(exc: Exception) -> bool:
    """True, если ошибка Gate похожа на «плечо не установлено / не хватает маржи»."""
    resp = getattr(exc, "response", None)
    if resp is None:
        return False
    try:
        label = str((resp.json() or {}).get("label", ""))
    except Exception:
        label = resp.text or ""
    label = label.upper()
    return any(k in label for k in ("LEVERAGE", "MARGIN", "INSUFFICIENT", "RISK_LIMIT"))


def _gate_open(ticker: str, usdt_amount: float, is_long: bool) -> tuple[float, float]:
    """
    Общая функция открытия позиции на Gate.io (лонг или шорт).

    Порядок:
      1. Устанавливаем плечо ДО расчёта (иначе биржа применит старое).
      2. Считаем contracts через _gate_calc_contracts (с учётом quanto_multiplier).
      3. Открываем рыночный ордер (price="0", tif="ioc").

    :param ticker: str - тикер монеты.
    :param usdt_amount: float - маржа в USDT (без плеча).
    :param is_long: bool - True = лонг, False = шорт.
    :return: (количество контрактов, цена входа)
    """
    price = gate_get_price(ticker)
    if not price:
        print(f"{_tag('GATE NO PRICE', RED)} {ticker}")
        return 0, 0

    contract = f"{ticker}_USDT"

    # M9: плечо ставим в ФОНЕ — не блокируем hot-path на ~150мс. Требование:
    # на аккаунте дефолтный cross-leverage ≥ _LEVERAGE (тогда первый ордер по
    # свежему тикеру не отвергнут по марже). Если всё же отвергнут — self-heal
    # на _post_signed ниже доставит плечо синхронно и повторит ровно один раз.
    _gate_set_leverage_async(contract, _LEVERAGE, cross=True)

    contracts = _gate_calc_contracts(ticker, usdt_amount, _LEVERAGE)
    min_sz    = _gate_min_order(ticker)
    contracts = max(contracts, min_sz)
    quanto    = _quanto_gate.get(ticker, 1.0)
    coins     = contracts * quanto

    # FIX-LOG: GATE CALC print убран — то же contracts/coins/price видно в
    # GATE LONG ниже. Перед каждым ордером дублировал инфу.

    order_size = contracts if is_long else -contracts
    _order_body = {
        "contract":    contract,
        "size":        order_size,
        "price":       "0",
        "tif":         "ioc",
        "reduce_only": False,
        "auto_size":   "",
    }
    try:
        result = _post_signed(f"/api/v4/futures/{_SETTLE}/orders", _order_body)
    except requests.HTTPError as e:
        # M9 self-heal: фоновая установка плеча могла не успеть (или дефолт
        # аккаунта ниже _LEVERAGE) → Gate отверг по марже/плечу. Ставим плечо
        # СИНХРОННО и повторяем РОВНО один раз. Отвергнутый ордер позиции не
        # создал → повтор безопасен (без double-position).
        if not _is_leverage_margin_error(e):
            raise
        print(f"{_tag('GATE RETRY-LEV', YELLOW)} {BOLD}{contract}{RESET} | margin/leverage reject → sync set + retry")
        _gate_set_leverage(contract, _LEVERAGE, cross=True)
        result = _post_signed(f"/api/v4/futures/{_SETTLE}/orders", _order_body)

    # M1: IOC market-ордер мог быть ПРИНЯТ (HTTP 200), но не исполнен —
    # на свежем листинге с тонкой ликвидностью. Раньше `or price` подставлял
    # кэш-цену → ложный success → worker ставил TP/SL на НЕсуществующую
    # позицию. Авторитетный признак исполнения = |size|-|left| (Gate, IOC):
    # сколько контрактов реально набралось (НЕ полагаемся на fill_price — он
    # может отсутствовать у реально исполненного ордера, и тогда ложный (0,0)
    # → retry → ДВОЙНАЯ позиция на Gate, где нет orderLinkId-идемпотентности).
    # Нет филла → (0,0), worker сделает retry (unfilled-ордер позиции не
    # создаёт, повтор безопасен).
    # (0,0) возвращаем ТОЛЬКО при ПОЛОЖИТЕЛЬНОМ признаке неисполнения — иначе
    # ложный (0,0) → retry → ДВОЙНАЯ позиция на Gate (нет orderLinkId). Если
    # size/left отсутствуют или не парсятся → считаем исполненным (без retry).
    _size_raw, _left_raw = result.get("size"), result.get("left")
    filled = None
    if _size_raw is not None and _left_raw is not None:
        try:
            filled = abs(int(_size_raw)) - abs(int(_left_raw))
        except (TypeError, ValueError):
            filled = None
    fill_price = float(result.get("fill_price") or 0)
    if filled is not None and filled <= 0:
        print(
            f"{_tag('GATE NO FILL', RED)} {BOLD}{contract}{RESET} | "
            f"IOC не исполнен | size={_size_raw} left={_left_raw} "
            f"status={result.get('status')} finish_as={result.get('finish_as')}"
        )
        return 0, 0
    if fill_price <= 0:
        fill_price = price  # исполнилось, но цена не пришла — берём кэш-цену
    direction  = "LONG" if is_long else "SHORT"
    color      = GREEN if is_long else RED
    print(
        f"{_tag(f'GATE {direction}', color)} {BOLD}{contract}{RESET} | "
        f"contracts={contracts} | price≈{fill_price}"
    )
    return contracts, fill_price


# ── открытие лонга ────────────────────────────────────────────────

def gate_open_long(ticker: str, usdt_amount: float) -> tuple[float, float]:
    """
    Открывает рыночный лонг на Gate.io фьючерсах.
    :return: (количество контрактов, цена входа)
    """
    return _gate_open(ticker, usdt_amount, is_long=True)


# ── открытие шорта ────────────────────────────────────────────────

def gate_open_short(ticker: str, usdt_amount: float) -> tuple[float, float]:
    """
    Открывает рыночный шорт на Gate.io фьючерсах за $usdt_amount.
    :return: (количество контрактов, цена входа)
    """
    return _gate_open(ticker, usdt_amount, is_long=False)


def gate_open_short_by_contracts(
    ticker: str,
    contracts: int,
    usdt_amount: float | None = None,
) -> tuple[float, float]:
    """
    Открывает шорт на Gate.io на заданное количество контрактов.

    contracts   — точное количество контрактов (используется в арбитраже,
                  когда количество уже посчитано из лонга на другой бирже).
    usdt_amount — если передан, пересчитывает contracts через _gate_calc_contracts.
    """
    price = gate_get_price(ticker)
    if not price:
        print(f"{_tag('GATE NO PRICE', RED)} {ticker}")
        return 0, 0

    contract = f"{ticker}_USDT"
    _gate_set_leverage(contract, _LEVERAGE, cross=True)

    if usdt_amount is not None:
        contracts = _gate_calc_contracts(ticker, usdt_amount, _LEVERAGE)

    min_sz    = _gate_min_order(ticker)
    contracts = max(int(contracts), min_sz)
    quanto    = _quanto_gate.get(ticker, 1.0)
    coins     = contracts * quanto

    # FIX-LOG: GATE CALC шорт-print убран — дублирует GATE SHORT ниже.

    result = _post_signed(f"/api/v4/futures/{_SETTLE}/orders", {
        "contract":    contract,
        "size":        -contracts,
        "price":       "0",
        "tif":         "ioc",
        "reduce_only": False,
        "auto_size":   "",
    })

    # M1: см. _gate_open — (0,0) только при ПОЛОЖИТЕЛЬНОМ признаке неисполнения.
    _size_raw, _left_raw = result.get("size"), result.get("left")
    filled = None
    if _size_raw is not None and _left_raw is not None:
        try:
            filled = abs(int(_size_raw)) - abs(int(_left_raw))
        except (TypeError, ValueError):
            filled = None
    fill_price = float(result.get("fill_price") or 0)
    if filled is not None and filled <= 0:
        print(
            f"{_tag('GATE NO FILL', RED)} {BOLD}{contract}{RESET} | "
            f"IOC не исполнен | size={_size_raw} left={_left_raw} "
            f"status={result.get('status')}"
        )
        return 0, 0
    if fill_price <= 0:
        fill_price = price
    fill_coins = contracts * quanto
    print(
        f"{_tag('GATE SHORT', RED)} {BOLD}{contract}{RESET} | "
        f"contracts={contracts} | price≈{fill_price} | ≈{fill_coins:.4f} монет"
    )
    return contracts, fill_price


# ── проверка открытой позиции ─────────────────────────────────────

def _gate_position_size(contract: str) -> int:
    """
    Возвращает текущий размер открытой позиции по контракту.
    >0  — лонг, <0 — шорт, 0 — позиции нет.
    """
    try:
        path    = f"/api/v4/futures/{_SETTLE}/positions/{contract}"
        headers = _sign("GET", path, "", "")
        resp    = _session.get(f"{_GATE_BASE}{path}", headers=headers, timeout=3)
        if resp.status_code != 200:
            return 0
        return int(resp.json().get("size", 0) or 0)
    except Exception:
        return 0


# ── Trailing stop через поллинг ───────────────────────────────────

def _run_trailing_stop(ticker: str, entry_price: float, contracts: int, trail_pct: float = 0.06) -> None:
    """
    Фоновый поток — trailing stop для Gate.io (для ЛОНГ позиции).
    Каждую секунду читает цену из кэша.
    Закрывает позицию если цена откатила на trail_pct от достигнутого максимума.

    FIX: проверяет, что позиция ещё открыта (через _gate_position_size раз в минуту).
    Если позицию закрыли вручную / по SL биржей — поток завершается.
    FIX: hard timeout _TRAILING_MAX_LIFETIME, чтобы поток не висел бесконечно.
    """
    contract  = f"{ticker}_USDT"
    max_price = entry_price
    started   = time.monotonic()
    last_check = 0.0

    print(
        f"{_tag('GATE TRAIL', MAGENTA)} {BOLD}{ticker}{RESET} | "
        f"старт trailing {trail_pct*100:.0f}% | "
        f"contracts={contracts} | entry={entry_price:.6f}"
    )

    while True:
        time.sleep(1)

        if time.monotonic() - started > _TRAILING_MAX_LIFETIME:
            print(f"{_tag('GATE TRAIL', YELLOW)} {ticker} | таймаут {_TRAILING_MAX_LIFETIME}с, выход")
            return

        # FIX: каждую минуту проверяем что позиция ещё жива (биржа могла закрыть по SL)
        if time.monotonic() - last_check > 60:
            last_check = time.monotonic()
            if _gate_position_size(contract) == 0:
                print(f"{_tag('GATE TRAIL', YELLOW)} {ticker} | позиция уже закрыта, выход")
                return

        price = gate_get_price(ticker)
        if not price:
            continue

        if price > max_price:
            max_price = price
            # FIX-LOG: 'new max=' print убран — забивал логи на каждом тике
            # роста цены. Старт trailing и выход уже логируются.

        stop_price = max_price * (1 - trail_pct)

        if price <= stop_price:
            print(
                f"{_tag('GATE TRAIL HIT', RED)} {BOLD}{ticker}{RESET} | "
                f"цена={price:.6f} ≤ стоп={stop_price:.6f} | макс был={max_price:.6f}"
            )
            try:
                _post_signed(f"/api/v4/futures/{_SETTLE}/orders", {
                    "contract":    contract,
                    "size":        -int(abs(contracts)),
                    "price":       "0",
                    "tif":         "ioc",
                    "reduce_only": True,
                })
                print(
                    f"{_tag('GATE TRAIL CLOSED', GREEN)} {BOLD}{ticker}{RESET} | "
                    f"{contracts} контрактов закрыто по ~{price:.6f}"
                )
            except Exception as e:
                print(f"{_tag('GATE TRAIL ERR', RED)} {ticker}: {e}")
            return


def gate_start_trailing(ticker: str, entry_price: float, contracts: int, trail_pct: float = 0.06) -> None:
    """Запускает trailing stop в daemon-потоке."""
    threading.Thread(
        target=_run_trailing_stop,
        args=(ticker, entry_price, int(contracts), trail_pct),
        daemon=True,
        name=f"trail-{ticker}",
    ).start()


# ── TP/SL для лонга на Gate.io ────────────────────────────────────

def gate_set_tp_sl_long(ticker: str, entry_price: float, amount: float) -> str:
    """
    Выставляет:
      - SL  -8%   на 100% позиции (через price_order)
      - TP1 +5.5% на 30%  позиции (через price_order)
      - Trailing 6% на оставшиеся 70% (через поллинг-поток)
    """
    contract = f"{ticker}_USDT"

    sl  = round(entry_price * 0.92, 8)   # -8%
    tp1 = round(entry_price * 1.055, 8)  # +5.5%

    sl_contracts  = int(amount)
    tp1_contracts = max(1, int(amount * 0.30))
    tail_contracts = max(0, sl_contracts - tp1_contracts)

    def _place_price_order(trigger: float, order_price: float, size: int, label: str) -> None:
        try:
            price_str   = _gate_round_price(ticker, order_price)
            trigger_str = _gate_round_price(ticker, trigger)
            _post_signed(f"/api/v4/futures/{_SETTLE}/price_orders", {
                "initial": {
                    "contract":    contract,
                    "size":        -int(abs(size)),
                    "price":       price_str,
                    "tif":         "ioc",
                    "reduce_only": True,
                },
                "trigger": {
                    "strategy_type": 0,
                    "price_type":    0,
                    "price":         trigger_str,
                    # rule 1 = цена >= триггер (TP для лонга),
                    # rule 2 = цена <= триггер (SL для лонга).
                    "rule":          1 if order_price >= entry_price else 2,
                },
            })
            color = GREEN if order_price >= entry_price else RED
            print(
                f"{_tag('GATE ORDER', color)} {ticker} | "
                f"{label} → trigger={trigger_str} | size={size} контрактов"
            )
        except Exception as e:
            if hasattr(e, "response") and getattr(e, "response", None) is not None:
                resp = e.response
                print(
                    f"{_tag('GATE TP/SL ERR', RED)} {ticker} [{label}]: "
                    f"{resp.status_code} | {resp.text[:200]}"
                )
            else:
                print(f"{_tag('GATE TP/SL ERR', RED)} {ticker} [{label}]: {e}")

    _place_price_order(sl, sl, sl_contracts, "SL -8%")
    _place_price_order(tp1, tp1, tp1_contracts, "TP1 +5.5%")

    if tail_contracts > 0:
        gate_start_trailing(ticker, entry_price, tail_contracts, trail_pct=0.06)

    print(
        f"{_tag('GATE TP/SL SET', CYAN)} {BOLD}{ticker}{RESET} | "
        f"SL={sl}(-8%/100%) | TP1={tp1}(+5.5%/30%) | "
        f"Trailing=6%(70%/{tail_contracts}контр)"
    )
    return "Gate TP/SL выставлен"


# ── TP/SL для шорта на Gate.io ────────────────────────────────────

def gate_set_tp_sl_short(ticker: str, entry_price: float, amount: float) -> str:
    """
    Выставляет для шорт-позиции на Gate.io:
      - SL  +5%   на 100% позиции
      - TP1 -8%   на 20%  позиции
      - TP2 -15%  на 30%  позиции
      - TP3 -45%  на 50%  позиции
    Для шорта: TP ниже цены входа, SL выше.
    """
    contract = f"{ticker}_USDT"

    sl  = round(entry_price * 1.05, 8)   # +5%  — стоп выше входа
    tp1 = round(entry_price * 0.92, 8)   # -8%
    tp2 = round(entry_price * 0.85, 8)   # -15%
    tp3 = round(entry_price * 0.55, 8)   # -45%

    tp1_contracts = max(1, int(amount * 0.20))
    tp2_contracts = max(1, int(amount * 0.30))
    tp3_contracts = max(1, int(amount * 0.50))
    sl_contracts  = int(amount)

    def _place(trigger: float, order_price: float, size: int, label: str) -> None:
        try:
            price_str   = _gate_round_price(ticker, order_price)
            trigger_str = _gate_round_price(ticker, trigger)
            # Для шорта: закрытие = покупка (положительный size).
            # rule 1 = цена >= триггер (SL для шорта),
            # rule 2 = цена <= триггер (TP для шорта).
            rule = 1 if order_price >= entry_price else 2
            _post_signed(f"/api/v4/futures/{_SETTLE}/price_orders", {
                "initial": {
                    "contract":    contract,
                    "size":        int(abs(size)),
                    "price":       price_str,
                    "tif":         "ioc",
                    "reduce_only": True,
                },
                "trigger": {
                    "strategy_type": 0,
                    "price_type":    0,
                    "price":         trigger_str,
                    "rule":          rule,
                },
            })
            color = RED if order_price >= entry_price else GREEN
            print(
                f"{_tag('GATE ORDER', color)} {ticker} | "
                f"{label} → trigger={trigger_str} | size={size} контрактов"
            )
        except Exception as e:
            print(f"{_tag('GATE TP/SL ERR', RED)} {ticker} [{label}]: {e}")

    _place(sl,  sl,  sl_contracts,  "SL  +5%")
    _place(tp1, tp1, tp1_contracts, "TP1 -8%")
    _place(tp2, tp2, tp2_contracts, "TP2 -15%")
    _place(tp3, tp3, tp3_contracts, "TP3 -45%")

    print(
        f"{_tag('GATE TP/SL SHORT', CYAN)} {BOLD}{ticker}{RESET} | "
        f"SL={sl}(+5%) | TP1={tp1}(-8%/20%) | "
        f"TP2={tp2}(-15%/30%) | TP3={tp3}(-45%/50%)"
    )
    return "Gate TP/SL SHORT выставлен"


def warmup_gate_connection() -> None:
    """Прогревает HTTP соединение с Gate.io заранее."""
    try:
        _get(f"/api/v4/futures/{_SETTLE}/tickers", {"limit": 1})
        print(f"{_tag('GATE WARMUP', CYAN)} соединение прогрето")
    except Exception as e:
        print(f"{_tag('GATE WARMUP ERR', RED)} {e}")


# ── Pre-set leverage sweep ────────────────────────────────────────
# FIX-LATENCY: на cold ticker'е _gate_set_leverage делает HTTP POST
# (100-200мс), который сидит в hot-path market_open_long → gate_open_long.
# Кэш _leverage_set_for помогает только со 2-го листинга по той же
# монете — а у нас каждый листинг это свежий тикер. Решение: фоновый
# sweep, который проходит по gate_known_coins и пред-устанавливает
# leverage для каждой. К моменту fresh-листинга кэш уже тёплый,
# market_open_long пропускает 150мс.
def gate_leverage_presetter(
    throttle_ms: int = 50,
    sweep_interval_sec: int = 3600,
    init_wait_sec: float = 30.0,
) -> None:
    """
    Фоновый sweep — для каждой монеты из gate_known_coins вызываем
    _gate_set_leverage(coin_USDT, _LEVERAGE, cross=True).

    Throttle 50мс/coin × ~300 контрактов ≈ 15с на полный sweep.
    Sweep interval 1ч — Gate.io редко добавляет новые контракты.
    На уже-кэшированных _gate_set_leverage отдаёт за ~1мкс (dict lookup),
    поэтому повторные sweep'ы практически бесплатные по сети.

    Ждём на старте, пока gate_price_updater наполнит gate_known_coins
    (первый цикл = 3с). Если за init_wait_sec пусто — отменяем.
    """
    waited = 0.0
    while not gate_known_coins and waited < init_wait_sec:
        time.sleep(1.0)
        waited += 1.0
    if not gate_known_coins:
        print(
            f"{_tag('GATE PRESET', YELLOW)} gate_known_coins пуст после "
            f"{waited:.0f}с — sweep отменён"
        )
        return

    throttle = throttle_ms / 1000.0
    sweep_no = 0
    while True:
        sweep_no += 1
        snapshot = sorted(gate_known_coins)  # детерминированный порядок для логов
        t0 = time.monotonic()
        with _leverage_lock:
            before = len(_leverage_set_for)
        errors = 0
        for i, coin in enumerate(snapshot, 1):
            contract = f"{coin}_USDT"
            try:
                _gate_set_leverage(contract, _LEVERAGE, cross=True)
            except Exception:  # noqa: BLE001
                errors += 1
            # throttle и для cached, и для cold — не вредно: фоновая нагрузка.
            time.sleep(throttle)
            # FIX-LOG: промежуточный print каждые 50 убран — забивал stdout.
            # Прогресс смотреть в итоговом sweep#N ✓ сообщении внизу.
        elapsed = time.monotonic() - t0
        with _leverage_lock:
            after = len(_leverage_set_for)
        new_count = after - before
        print(
            f"{_tag('GATE PRESET', GREEN)} sweep#{sweep_no} ✓ "
            f"{len(snapshot)} контрактов за {elapsed:.1f}с | "
            f"новых в кэше={new_count} err={errors} | "
            f"следующий через {sweep_interval_sec}с"
        )
        time.sleep(sweep_interval_sec)
