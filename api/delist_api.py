from __future__ import annotations

import hashlib
import hmac
import json
import re
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from config.config import BYBIT_API_KEY, BYBIT_SECRET_KEY
import requests

# FIX-batch-1: orjson для парсинга ответов Bybit (3-5x быстрее).
try:
    import orjson as _orjson  # type: ignore[import-not-found]
    def _json_loads(b):
        if isinstance(b, str):
            b = b.encode()
        return _orjson.loads(b)
except ImportError:
    def _json_loads(b):
        if isinstance(b, (bytes, bytearray)):
            b = b.decode()
        return json.loads(b)

# ── DNS кэш с TTL (убирает повторные DNS-запросы) ────────────────
_original_getaddrinfo = socket.getaddrinfo
_dns_cache: dict[tuple, tuple[float, list]] = {}   # FIX: с TTL
_DNS_TTL = 300  # 5 минут — Bybit IP редко меняется, но не насовсем

def _cached_getaddrinfo(*args, **kwargs):
    now = time.monotonic()
    cached = _dns_cache.get(args)
    if cached and (now - cached[0]) < _DNS_TTL:
        return cached[1]
    result = _original_getaddrinfo(*args, **kwargs)
    _dns_cache[args] = (now, result)
    return result

socket.getaddrinfo = _cached_getaddrinfo  # патч до любых сетевых импортов

from api.gate_api import (   # noqa: E402  (импорт после monkey-patch)
    gate_get_price,
    gate_open_short,
    gate_set_tp_sl_short,
    gate_price_updater,         # noqa: F401  — реэкспорт для parser_delist
    gate_preload_lot_steps,     # noqa: F401  — реэкспорт для parser_delist
    warmup_gate_connection,     # noqa: F401  — реэкспорт для parser_delist
)

# ── Конфиг Bybit ─────────────────────────────────────────────────
BYBIT_BASE_URL = "https://api.bybit.com"
RECV_WINDOW    = "5000"
LEVERAGE       = 10   # FIX: вынес магическое число в константу

# Защита от None если ключи не заданы в .env
BYBIT_API_KEY    = BYBIT_API_KEY    or ""
BYBIT_SECRET_KEY = BYBIT_SECRET_KEY or ""

# Одна переиспользуемая сессия для всех ордеров (keep-alive)
_bybit_session = requests.Session()
_bybit_session.headers.update({
    "Content-Type":       "application/json",
    "X-BAPI-API-KEY":     BYBIT_API_KEY,
    "X-BAPI-RECV-WINDOW": RECV_WINDOW,
})

# ── Слова-исключения ──────────────────────────────────────────────
EXCLUDED_TOKENS = {
    "USDT", "BUSD", "USDC", "TUSD", "DAI",
    "BINANCE", "SPOT", "MARGIN", "FUTURES", "EARN",
    "WILL", "AND", "ON", "FOR", "THE", "ALL",
    "USD", "UTC", "API", "VIP", "KYC", "AML",
    "FAQ", "TBA", "TBD", "NFT", "DEFI",
    "P2P", "OTC", "IPO", "ICO", "IEO",
    "THIS", "IS", "GENERAL", "EXCHANGE", "NOTICE", "PRODUCTS", "SERVICES",
    "REFERRED", "TO", "HERE", "MAY", "NOT", "BE", "IN", "YOUR", "REGION",
    "FELLOW", "CLOSE", "CONDUCT", "AN", "SUPPORT", "AIRDROP", "PLAN",
    "COIN", "MULTIPLE", "WITH", "FROM", "THAT", "ALSO", "HAVE",
    # FIX-batch-6: расширение под форматы каналов пользователя
    # (Monitoring Tag, removed from spot, alpha removals и т.п.)
    "MONITORING", "TAG", "EXTEND", "EXTENDED", "INCLUDE", "INCLUDED",
    "DELIST", "DELISTS", "DELISTED", "DELISTING", "DELISTINGS",
    "REMOVE", "REMOVED", "REMOVING", "REMOVAL", "REMOVES",
    "LIST", "LISTED", "LISTING", "LISTINGS",
    "ALPHA", "BUY", "SELL", "TRADE", "TRADING",
    "POSTPONED", "PERPETUAL", "PERPETUALS", "LAUNCH", "LAUNCHED",
    "OPEN", "OPENED", "OPENS", "ADD", "ADDED", "ADDS",
    "NEW", "TOKEN", "TOKENS", "POOL", "POOLS", "PAIRS", "PAIR",
    "BORROW", "LOAN", "LOANS", "SIMPLE", "BUYBACK",
    "USDⓈ", "USDS",  # Binance liquid staking
    "ANNOUNCEMENT", "ANNOUNCEMENTS",
}

# ── Кэш цен ──────────────────────────────────────────────────────
price_cache: dict[str, float] = {}
cache_lock  = threading.Lock()
known_coins: set[str] = set()


# ── Подпись Bybit ─────────────────────────────────────────────────

def _sign(ts: str, body_str: str) -> str:
    """
    Генерирует HMAC-SHA256 подпись для Bybit V5 API.
    sign_str = timestamp + api_key + recv_window + body
    """
    sign_str = ts + BYBIT_API_KEY + RECV_WINDOW + body_str
    return hmac.new(
        BYBIT_SECRET_KEY.encode(),
        sign_str.encode(),
        hashlib.sha256,
    ).hexdigest()


def _post(endpoint: str, params: dict, retries: int = 2) -> dict:
    """
    Отправляет подписанный POST-запрос к Bybit V5.
    Использует persist-сессию (keep-alive) — без переустановки TCP.

    FIX: добавлен ретрай при сетевых ошибках (timeout/connection).
    Бизнес-ошибки Bybit (retCode != 0) НЕ ретраим — они стабильны.
    """
    body_str = json.dumps(params, separators=(",", ":"))  # компактный JSON

    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        ts = str(int(time.time() * 1000))
        sign = _sign(ts, body_str)
        headers = {"X-BAPI-TIMESTAMP": ts, "X-BAPI-SIGN": sign}

        try:
            resp = _bybit_session.post(
                BYBIT_BASE_URL + endpoint,
                data=body_str,
                headers=headers,
                timeout=3,
            )
            resp.raise_for_status()
            data = _json_loads(resp.content)  # FIX-batch-1: orjson
            ret_code = data.get("retCode", -1)
            if ret_code != 0:
                raise RuntimeError(
                    f"Bybit error retCode={ret_code} msg={data.get('retMsg')} params={params}"
                )
            return data
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if attempt < retries:
                time.sleep(0.1 * (attempt + 1))
                continue
            raise
        except Exception:
            raise

    if last_exc:
        raise last_exc
    return {}


def _get(endpoint: str, params: dict | None = None) -> dict:
    """
    GET запрос к Bybit V5 (публичный, без подписи).
    """
    resp = _bybit_session.get(
        BYBIT_BASE_URL + endpoint,
        params=params,
        timeout=3,
    )
    resp.raise_for_status()
    return _json_loads(resp.content)  # FIX-batch-1: orjson


# ── Price updater (тикеры через REST, без ccxt) ───────────────────

def price_updater() -> None:
    """
    Каждые ~2с тянет все linear-тикеры с Bybit и обновляет price_cache.
    Заменяет ccxt.fetch_tickers() — без overhead ccxt.
    Использует clear() + update() чтобы удалять делистингованные монеты.
    """
    url     = BYBIT_BASE_URL + "/v5/market/tickers"
    session = requests.Session()

    while True:
        try:
            resp = session.get(
                url,
                params={"category": "linear"},
                timeout=4,
            )
            resp.raise_for_status()
            items = _json_loads(resp.content).get("result", {}).get("list", [])  # FIX-batch-1

            new_cache: dict[str, float] = {}
            new_known: set[str] = set()

            for item in items:
                symbol     = item.get("symbol", "")       # BTCUSDT
                last_price = item.get("lastPrice")
                if last_price and symbol.endswith("USDT"):
                    price = float(last_price)
                    key = symbol[:-4] + "/USDT:USDT"
                    new_cache[key] = price
                    new_known.add(symbol[:-4])

            with cache_lock:
                price_cache.clear()
                price_cache.update(new_cache)
                known_coins.clear()
                known_coins.update(new_known)

        except Exception as e:
            print(f"[PRICE CACHE ERROR] {e}")

        time.sleep(2)


# ── Торговые функции ──────────────────────────────────────────────

def get_price(coin: str) -> Optional[float]:
    """
    Возвращает последнюю цену монеты из кэша.
    :param coin: str - тикер монеты (например "BTC").
    :return: float | None - цена в USDT или None если монеты нет в кэше.
    """
    return price_cache.get(f"{coin}/USDT:USDT")


def calculate_margin_for_delist() -> float:
    """
    Рассчитывает размер маржи для открытия позиции при делистинге.
    # TODO: брать реальный баланс с API вместо захардкоженного значения.
    :return: float - размер маржи в USDT.
    """
    balance = 100
    return balance * 0.1


_lot_step_cache: dict[str, float] = {}
_lot_step_lock = threading.Lock()


def preload_lot_steps() -> None:
    """
    Предзагружает шаги лота для всех linear-инструментов при старте.
    Убирает задержку _get_qty_step в момент открытия ордера.
    """
    try:
        data  = _get("/v5/market/instruments-info", {"category": "linear", "limit": 1000})
        items = data.get("result", {}).get("list", [])
        with _lot_step_lock:
            for item in items:
                symbol = item.get("symbol", "")
                if symbol.endswith("USDT"):
                    coin = symbol[:-4]
                    step = float(item["lotSizeFilter"]["qtyStep"])
                    _lot_step_cache[coin] = step
        print(f"[PRELOAD] Загружено {len(_lot_step_cache)} шагов лота")
    except Exception as e:
        print(f"[PRELOAD ERROR] {e}")


class QtyStepUnavailable(Exception):
    """Бросается, когда шаг лота не удалось определить — лучше не открывать
    позицию, чем открыть в неверном размере."""


def _get_qty_step(symbol: str) -> float:
    """
    Возвращает минимальный шаг qty для символа с Bybit.
    Берёт из кэша или делает запрос к instruments-info.

    FIX: если шаг недоступен — бросаем QtyStepUnavailable вместо
    подстановки 1.0 (которая могла привести к открытию ордера в 10× больше).
    :param symbol: str - символ вида VINEUSDT.
    :return: float - шаг количества.
    :raises QtyStepUnavailable: если шаг не получили.
    """
    coin = symbol.replace("USDT", "")
    with _lot_step_lock:
        if coin in _lot_step_cache:
            return _lot_step_cache[coin]

    try:
        data       = _get("/v5/market/instruments-info", {"category": "linear", "symbol": symbol})
        lot_filter = data["result"]["list"][0]["lotSizeFilter"]
        step       = float(lot_filter["qtyStep"])
    except Exception as e:
        raise QtyStepUnavailable(f"{symbol}: не получили шаг лота — {e}") from e

    with _lot_step_lock:
        _lot_step_cache[coin] = step
    return step


def _round_qty(qty: float, step: float) -> float:
    """
    Округляет количество вниз до ближайшего шага лота.
    :param qty: float - исходное количество.
    :param step: float - шаг лота от биржи.
    :return: float - округлённое количество.
    """
    precision = len(str(step).rstrip("0").split(".")[-1]) if "." in str(step) else 0
    rounded   = (qty // step) * step
    return round(rounded, precision)


# Биржа на которой открыт шорт — нужна для роутера set_tp_sl
_delist_exchange: dict[str, str] = {}   # {ticker: "bybit" | "gate"}
_delist_exchange_lock = threading.Lock()


def market_open_short(ticker_name: str, usdt_amount: float) -> tuple[float, float]:
    """
    Открывает рыночный шорт напрямую через Bybit V5 REST.
    Без ccxt — убирает overhead load_markets / нормализации.
    :param ticker_name: str - тикер монеты.
    :param usdt_amount: float - маржа в USDT.
    :return: tuple[float, float] - (количество, цена входа), (0, 0) если не удалось.
    """
    bybit_price = get_price(ticker_name)

    if bybit_price:
        symbol  = f"{ticker_name}USDT"
        raw_qty = (usdt_amount / bybit_price) * LEVERAGE   # FIX: магическое число → константа

        try:
            step = _get_qty_step(symbol)
        except QtyStepUnavailable as e:
            # FIX: раньше step=1.0 → ордер мог уйти в 10× больше.
            # Лучше упасть на Gate.io fallback.
            print(f"[QTY STEP MISSING BYBIT] {e} — пробуем Gate.io")
            bybit_price = None
        else:
            amount_tokens = _round_qty(raw_qty, step)

            if amount_tokens > 0:
                order_args = {
                    "category":    "linear",
                    "symbol":      symbol,
                    "side":        "Sell",
                    "orderType":   "Market",
                    "qty":         str(amount_tokens),
                    "positionIdx": 2,
                }
                # FIX-batch-5: пробуем WS Trade API (−30...−80мс), при ошибке/None — REST.
                placed_via = "REST"
                ws_ack = None
                try:
                    from api.bybit_ws_trade import place_order_ws
                    ws_ack = place_order_ws(order_args)
                except Exception as e:
                    print(f"[BYBIT-WS] не доступен ({e}) — используем REST")

                if ws_ack is None:
                    _post("/v5/order/create", order_args)
                else:
                    placed_via = "WS"

                with _delist_exchange_lock:
                    _delist_exchange[ticker_name] = "bybit"
                print(f"[BYBIT SHORT/{placed_via}] {symbol} | tokens={amount_tokens} | price≈{bybit_price}")
                return amount_tokens, bybit_price

    # ── Gate.io fallback ─────────────────────────────────────────
    gate_price = gate_get_price(ticker_name)
    if not gate_price:
        print(f"[NO PRICE ANYWHERE] {ticker_name}")
        return 0, 0

    amount, fill_price = gate_open_short(ticker_name, usdt_amount)
    if amount:
        with _delist_exchange_lock:
            _delist_exchange[ticker_name] = "gate"
    return amount, fill_price


def _set_tp_sl_bybit_short(ticker_name: str, entry_price: float, amount: float) -> str:
    """
    Выставляет SL и 3 уровня TP для шорта на Bybit.
    """
    sl  = round(entry_price * 1.05, 8)
    tp1 = round(entry_price * 0.92, 8)
    tp2 = round(entry_price * 0.85, 8)
    tp3 = round(entry_price * 0.55, 8)

    symbol = f"{ticker_name}USDT"
    try:
        step = _get_qty_step(symbol)
    except QtyStepUnavailable as e:
        print(f"[TP/SL SKIP] {e}")
        return "skip"

    sl_size  = str(_round_qty(amount,       step))
    tp1_size = str(_round_qty(amount * 0.2, step))
    tp2_size = str(_round_qty(amount * 0.3, step))
    tp3_size = str(_round_qty(amount * 0.5, step))

    def _place_tp(tp_price: str, tp_size: str) -> None:
        """
        Выставляет один уровень TP через trading-stop.
        :param tp_price: str - цена тейк-профита.
        :param tp_size: str - размер в токенах.
        """
        _post("/v5/position/trading-stop", {
            "category":    "linear",
            "symbol":      symbol,
            "takeProfit":  tp_price,
            "tpTriggerBy": "LastPrice",
            "tpslMode":    "Partial",
            "tpSize":      tp_size,
            "positionIdx": 2,
        })

    # SL ставим отдельно (один на всю позицию)
    _post("/v5/position/trading-stop", {
        "category":    "linear",
        "symbol":      symbol,
        "stopLoss":    str(sl),
        "slTriggerBy": "LastPrice",
        "tpslMode":    "Partial",
        "slSize":      sl_size,
        "positionIdx": 2,
    })

    # TP ставим параллельно
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [
            ex.submit(_place_tp, str(tp1), tp1_size),
            ex.submit(_place_tp, str(tp2), tp2_size),
            ex.submit(_place_tp, str(tp3), tp3_size),
        ]
        for f in futures:
            try:
                f.result()
            except Exception as e:
                print(f"[TP PLACE ERR] {ticker_name}: {e}")

    print(f"[TP/SL SET] {ticker_name} | SL={sl} TP1={tp1} TP2={tp2} TP3={tp3}")
    return "Выставил цели"


def set_tp_sl(ticker_name: str, entry_price: float, amount: float) -> str:
    """
    Роутер — выставляет TP/SL на той бирже где открыт шорт.
    """
    with _delist_exchange_lock:
        exchange = _delist_exchange.get(ticker_name, "bybit")
    if exchange == "gate":
        return gate_set_tp_sl_short(ticker_name, entry_price, amount)
    return _set_tp_sl_bybit_short(ticker_name, entry_price, amount)


# ── find_pairs ────────────────────────────────────────────────────

# FIX: тикер может начинаться с цифры (1INCH, 1000PEPE), поэтому
# первая группа разрешает цифру.
_RE_USDT        = re.compile(r"\b([A-Z0-9]{2,10})(?:/|-|_)?USDT\b")
_RE_WILL_DELIST = re.compile(r"WILL\s+DELIST\s+(.*?)\s+ON\b")
_RE_DELIST_BLOCK = re.compile(r"(?:DELIST|REMOVE|DELISTING\s+OF)\s{0,5}(.{10,200}?)(?:\.|ON\s|\n)")
_RE_ALL_TOKENS  = re.compile(r"\b([A-Z0-9]{2,8})\b")
_RE_PAIR_TOKENS = re.compile(r"\b([A-Z0-9]{2,10})\b")
# FIX-batch-6: ловим "Monitoring Tag to Include X, Y, Z on ..." (BWEnews/binance_announcements).
_RE_MONITORING  = re.compile(r"MONITORING\s+TAG\s+TO\s+INCLUDE\s+(.*?)(?:\s+ON\b|\.|\n)")
# FIX-batch-7: $TICKER маркеры (cryptolistingwebsocket: "Monitoring Tag Added – $ALCX, $COOKIE")
_RE_DOLLAR_TKN  = re.compile(r"\$([A-Z][A-Z0-9]{1,9})\b")


def find_pairs(text: str) -> list[str]:
    """
    Извлекает тикеры монет из полного текста статьи о делистинге.
    Порядок поиска (от точного к общему):
      1. Явные пары с USDT: ABC/USDT, ABC-USDT, ABCUSDT
      2. Список после «Will Delist»: "Will Delist ABC, DEF, GHI on..."
      3. Список после «delist»/«remove» + перечисление токенов
      4. Аккуратный fallback по всему тексту
    :param text: str - текст статьи о делистинге.
    :return: list[str] - список тикеров монет.
    """
    text_upper = text.upper()

    usdt_pairs = _RE_USDT.findall(text_upper)
    if usdt_pairs:
        found = _filter_tokens(usdt_pairs)
        if found:
            print(f"[FIND PAIRS] метод=USDT-пары → {found}")
            return found

    # FIX-batch-7: $TICKER маркеры (CLW: "Monitoring Tag Added – $ALCX, $COOKIE...")
    # Используем оригинальный регистр для $ паттерна — там точно uppercase.
    dollar_pairs = _RE_DOLLAR_TKN.findall(text)
    if dollar_pairs:
        found = _filter_tokens(dollar_pairs)
        if found:
            print(f"[FIND PAIRS] метод=$ticker → {found}")
            return found

    m = _RE_WILL_DELIST.search(text_upper)
    if m:
        tokens = _RE_PAIR_TOKENS.findall(m.group(1))
        found  = _filter_tokens(tokens)
        if found:
            print(f"[FIND PAIRS] метод=Will-Delist → {found}")
            return found

    # FIX-batch-6: "Will Extend the Monitoring Tag to Include ALCX, COOKIE, DODO..."
    m = _RE_MONITORING.search(text_upper)
    if m:
        tokens = _RE_PAIR_TOKENS.findall(m.group(1))
        found  = _filter_tokens(tokens)
        if found:
            print(f"[FIND PAIRS] метод=Monitoring-Tag → {found}")
            return found

    m = _RE_DELIST_BLOCK.search(text_upper)
    if m:
        tokens = _RE_PAIR_TOKENS.findall(m.group(1))
        found  = _filter_tokens(tokens)
        if found:
            print(f"[FIND PAIRS] метод=delist-block → {found}")
            return found

    all_tokens = _RE_ALL_TOKENS.findall(text_upper)
    found = [t for t in _filter_tokens(all_tokens) if t in known_coins]

    if found:
        print(f"[FIND PAIRS] метод=fallback(price_cache) → {found}")
    else:
        print("[FIND PAIRS] ничего не найдено в тексте")
    return found


def _filter_tokens(tokens: list[str]) -> list[str]:
    """
    Фильтрует список токенов: убирает стоп-слова, дубликаты и слишком короткие/длинные.
    Также отсеивает токены состоящие только из цифр.
    :param tokens: list[str] - список токенов из regex.
    :return: list[str] - отфильтрованный список тикеров.
    """
    seen:   set[str]  = set()
    result: list[str] = []
    for t in tokens:
        if t in EXCLUDED_TOKENS:
            continue
        if not (2 <= len(t) <= 10):
            continue
        if t.isdigit():
            continue
        if t in seen:
            continue
        seen.add(t)
        result.append(t)
    return result


def _dedupe(tokens: list[str]) -> list[str]:
    """
    Убирает дубликаты из списка, сохраняя порядок первого вхождения.
    :param tokens: list[str] - список токенов с возможными дублями.
    :return: list[str] - список без дублей.
    """
    return list(dict.fromkeys(tokens))


def warmup_bybit_connection() -> None:
    """
    Прогревает keep-alive соединение к Bybit при старте.
    Чтобы первый боевой запрос не тратил время на TCP+TLS handshake.
    """
    try:
        _bybit_session.get(BYBIT_BASE_URL + "/v5/market/time", timeout=3)
        print("[WARMUP] Bybit соединение прогрето")
    except Exception as e:
        print(f"[WARMUP ERROR] {e}")
