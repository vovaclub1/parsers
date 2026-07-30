from __future__ import annotations

import hashlib
import hmac
import json
import re
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional
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

# FIX-PERF: msgspec.Struct для /v5/market/tickers (price_updater). Раньше
# orjson возвращал dict с 1000+ tickers, потом for-loop делал .get() на
# каждом элементе под GIL — 5-15мс на цикл = окно jitter'а для worker'а
# когда сигнал прилетал в эту же миллисекунду. msgspec парсит сразу в
# typed Struct (на C-уровне, частично освобождает GIL для больших payloads)
# и доступ через атрибут (.symbol vs ["symbol"]) — суммарно ~30-50% быстрее.
try:
    import msgspec as _msgspec  # type: ignore[import-not-found]

    class _BybitTicker(_msgspec.Struct, frozen=True):
        symbol:    str = ""
        lastPrice: str = ""

    # Поле называется `list` (как в JSON), но в аннотации используем
    # typing.List — иначе `from __future__ import annotations` превращает
    # `list[_BybitTicker]` в строку, и msgspec при eval'е резолвит `list`
    # в member_descriptor этого же поля (TypeError: not subscriptable).
    class _BybitTickerList(_msgspec.Struct, frozen=True):
        list: List[_BybitTicker] = []  # noqa: RUF012

    class _BybitTickersResp(_msgspec.Struct, frozen=True):
        result: _BybitTickerList | None = None

    _bybit_tickers_decoder = _msgspec.json.Decoder(_BybitTickersResp)

    def _parse_bybit_tickers(raw: bytes) -> list[_BybitTicker]:
        resp = _bybit_tickers_decoder.decode(raw)
        if resp.result is None:
            return []
        return resp.result.list
except ImportError:
    _parse_bybit_tickers = None  # type: ignore[assignment]

# ── DNS кэш с TTL (убирает повторные DNS-запросы) ────────────────
_original_getaddrinfo = socket.getaddrinfo
_dns_cache: dict[tuple, tuple[float, list]] = {}   # FIX: с TTL
_DNS_TTL = 300  # 5 минут — Bybit IP редко меняется, но не насовсем

def _cached_getaddrinfo(*args, **kwargs):
    now = time.monotonic()
    # FIX: kwargs тоже должны быть частью ключа. Раньше два вызова с разным
    # `proto`/`flags` могли получить одинаковый кешированный результат и
    # некоторые asyncio/aiohttp пути ломались на неправильном flag-наборе.
    key = (args, tuple(sorted(kwargs.items())))
    cached = _dns_cache.get(key)
    if cached and (now - cached[0]) < _DNS_TTL:
        return cached[1]
    result = _original_getaddrinfo(*args, **kwargs)
    _dns_cache[key] = (now, result)
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

# FIX: вынесли импорт из хот-функции market_open_short на модульный уровень.
# FIX-PERF: fire-and-forget вариант place_order_ws_fast — экономит ~70-100мс
# на ack-roundtrip. Reject логируется в фоне через _watch_ack.
try:
    from api.bybit_ws_trade import place_order_ws_fast as _ws_place_order, WSOrderRejected
except Exception as _ws_import_exc:  # noqa: BLE001 — graceful
    print(f"[BYBIT-WS] модуль не подгружен: {_ws_import_exc!r} — будет только REST")
    def _ws_place_order(args: dict, _warmup_mode: bool = False) -> dict | None:  # type: ignore[misc]
        return None
    class WSOrderRejected(Exception):  # type: ignore[no-redef]
        """Stub если bybit_ws_trade не подгрузился — никогда не raise-нется."""
        pass

# ── Конфиг Bybit ─────────────────────────────────────────────────
BYBIT_BASE_URL = "https://api.bybit.com"
ORDER_CREATE_URL = BYBIT_BASE_URL + "/v5/order/create"   # FIX-batch-8: pre-built URL
RECV_WINDOW    = "5000"
LEVERAGE       = 10   # FIX: вынес магическое число в константу

# Защита от None если ключи не заданы в .env
BYBIT_API_KEY    = BYBIT_API_KEY    or ""
BYBIT_SECRET_KEY = BYBIT_SECRET_KEY or ""

# FIX-batch-8: pre-encoded byte secret для HMAC — экономим .encode() на каждый ордер.
_BYBIT_SECRET_BYTES = BYBIT_SECRET_KEY.encode()

# Одна переиспользуемая сессия для всех ордеров (keep-alive)
_bybit_session = requests.Session()
_bybit_session.headers.update({
    "Content-Type":       "application/json",
    "X-BAPI-API-KEY":     BYBIT_API_KEY,
    "X-BAPI-RECV-WINDOW": RECV_WINDOW,
})

# ── Слова-исключения ──────────────────────────────────────────────
#
# FIX-AUDIT (важно): раньше здесь лежали токены, которые ОДНОВРЕМЕННО
# являются реально торгуемыми тикерами. Проверка по живым API Bybit /
# Binance Futures / Upbit / Bithumb (2026-07-30) нашла 19 таких коллизий:
#
#   THE (Thena), AT (AITECH), OPEN, ORDER, CROSS, APR, IN, TAG, BE,
#   COIN, BOT, NOT, ON, ALL, NFT, MAY, USDS, USDC, USDT
#
# Каждый из них — делистинг/листинг, который бот молча пропускал.
# Реальный пример из Binance-каталога:
#   "Binance Margin And Loan Will Delist HOT, THE on 2026-07-03"
#   -> find_pairs() возвращал ['HOT'], тикер THE терялся.
#
# Решение: стоп-лист разбит на два уровня.
#   ALWAYS_EXCLUDED   — служебные слова, которые НИКОГДА не тикеры
#                       (стейблкоины-квоты и грамматика вроде THIS/PLEASE).
#   AMBIGUOUS_TOKENS  — слова, которые бывают и тикерами тоже. Они
#                       отбрасываются ТОЛЬКО если не подтверждены
#                       known_coins (живой список инструментов биржи).
#
# Так «THE» в тексте делистинга проходит (он есть в known_coins), а «THE»
# как английский артикль в теле статьи — нет.

# Квотируемые валюты: как база для шорта не рассматриваем никогда.
_QUOTE_ASSETS = {
    "USDT", "BUSD", "TUSD", "DAI", "USD",
}

ALWAYS_EXCLUDED = {
    *_QUOTE_ASSETS,
    "BINANCE", "SPOT", "MARGIN", "FUTURES", "EARN",
    "WILL", "AND", "FOR", "THE_ARTICLE_PLACEHOLDER",
    "UTC", "API", "VIP", "KYC", "AML",
    "FAQ", "TBA", "TBD", "DEFI",
    "P2P", "OTC", "IPO", "ICO", "IEO",
    "THIS", "IS", "GENERAL", "EXCHANGE", "NOTICE", "PRODUCTS", "SERVICES",
    "REFERRED", "TO", "HERE", "YOUR", "REGION",
    "FELLOW", "CLOSE", "CONDUCT", "AN", "SUPPORT", "AIRDROP", "PLAN",
    "MULTIPLE", "WITH", "FROM", "THAT", "ALSO", "HAVE",
    "MONITORING", "EXTEND", "EXTENDED", "INCLUDE", "INCLUDED",
    "DELIST", "DELISTS", "DELISTED", "DELISTING", "DELISTINGS",
    "REMOVE", "REMOVED", "REMOVING", "REMOVAL", "REMOVES",
    "LIST", "LISTED", "LISTING", "LISTINGS",
    "ALPHA", "BUY", "SELL", "TRADE", "TRADING",
    "POSTPONED", "PERPETUAL", "PERPETUALS", "LAUNCH", "LAUNCHED",
    "OPENED", "OPENS", "ADD", "ADDED", "ADDS",
    "NEW", "TOKEN", "TOKENS", "POOL", "POOLS", "PAIRS", "PAIR",
    "BORROW", "LOAN", "LOANS", "SIMPLE", "BUYBACK",
    "ANNOUNCEMENT", "ANNOUNCEMENTS",
    "AS", "BY", "OR", "OF", "IT", "IF", "SO", "DO",
    "FOLLOWING", "FOLLOWED", "FOLLOWS",
    "PLEASE", "NOTE", "NOTED", "NOTES",
    "EFFECTIVE", "STARTING", "BEGINNING", "ENDING", "ENDS",
    "DATE", "TIME", "TIMES", "HOUR", "HOURS",
    "USERS", "USER", "CLIENTS", "CLIENT",
    "WITHDRAWAL", "WITHDRAWALS", "WITHDRAW",
    "DEPOSIT", "DEPOSITS",
    "ORDERS", "POSITION", "POSITIONS",
    "BALANCE", "BALANCES", "ACCOUNT", "ACCOUNTS",
    "FUND", "FUNDS", "FUNDING",
    "ISOLATED", "LEVERAGE", "LEVERAGED",
    "CONVERT", "CONVERTED", "CONVERTING",
    "COPY", "BOTS",
    "REGIONS", "COUNTRY", "COUNTRIES",
    "SUBJECT", "TERMS", "AGREEMENT", "POLICY", "POLICIES",
    "DUE", "PER", "VIA", "INTO", "OUT", "AFTER", "BEFORE",
    "ABOVE", "BELOW", "BETWEEN",
    "DETAILS", "DETAIL", "MORE", "LESS", "ABOUT",
    "APY",
    "DAYS", "DAY", "WEEK", "WEEKS", "MONTH", "MONTHS", "YEAR", "YEARS",
    "SPECIAL", "OFFER", "OFFERS",
    "SUBSCRIBE", "SUBSCRIPTION",
    "LOCKED", "FLEXIBLE",
    "ENJOY", "ENJOYS", "ENJOYED",
    "REWARD", "REWARDS",
    "PROMO", "PROMOTION", "PROMOTIONAL",
    "STAKE", "STAKING", "STAKED",
    "LAUNCHPOOL", "MEGADROP", "AIRDROPS",
    "BONUS", "BONUSES",
    "LIMITED", "EXCLUSIVE",
    "EVENT", "EVENTS", "CELEBRATION", "CELEBRATE",
}
ALWAYS_EXCLUDED.discard("THE_ARTICLE_PLACEHOLDER")

# Слова, которые встречаются в тексте анонсов, НО одновременно являются
# реальными тикерами. Пропускаем только при подтверждении known_coins.
AMBIGUOUS_TOKENS = {
    "THE", "AT", "ON", "IN", "ALL", "NOT", "BE", "MAY",
    "OPEN", "ORDER", "CROSS", "TAG", "COIN", "BOT",
    "APR", "NFT", "USDC", "USDS",
}

# Обратная совместимость: внешний код (и старые тесты) импортируют
# EXCLUDED_TOKENS. Оставляем как объединение — но фильтрация
# в _filter_tokens теперь смотрит на два множества раздельно.
EXCLUDED_TOKENS = ALWAYS_EXCLUDED

# ── Кэш цен ──────────────────────────────────────────────────────
price_cache: dict[str, float] = {}
cache_lock  = threading.Lock()
known_coins: set[str] = set()


# ── Подпись Bybit ─────────────────────────────────────────────────

def _sign(ts: str, body_str: str) -> str:
    """
    Генерирует HMAC-SHA256 подпись для Bybit V5 API.
    sign_str = timestamp + api_key + recv_window + body
    FIX-batch-8: используем pre-encoded _BYBIT_SECRET_BYTES — экономим
    .encode() на каждый запрос (мелочь, но в хот-path ордера полезно).
    """
    sign_str = ts + BYBIT_API_KEY + RECV_WINDOW + body_str
    return hmac.new(
        _BYBIT_SECRET_BYTES,
        sign_str.encode(),
        hashlib.sha256,
    ).hexdigest()


# ── FIX-batch-8: специализированный fast-path для создания ордера ──
# Что улучшаем относительно общего _post:
#   1. URL пре-склеен в ORDER_CREATE_URL (без BYBIT_BASE_URL + endpoint конкатенации).
#   2. JSON собирается f-string'ом без json.dumps() — экономит 1-2мс,
#      и фиксирует точную форму payload (Bybit чувствителен к пробелам
#      в подписи: body для signing должен совпадать с body, который
#      реально отправляется).
#   3. Headers собираются как минимальный dict (только timestamp+sign);
#      статичные (Content-Type / API-KEY / RECV-WINDOW) уже на _bybit_session.headers.
#   4. retCode проверяем без лишнего .get(..., default).
# Win: −2...−5мс на REST-fallback ордера (когда WS Trade не сработал).

# Bybit retCode для дублирующегося orderLinkId — ордер УЖЕ принят на сервере,
# можно считать success (первая попытка прошла, ответ просто потерялся в сети).
_BYBIT_DUPLICATE_RET_CODES = {30050}  # OrderLinkID is duplicate


# FIX-8: разрешённые символы в symbol/qty/order_link_id — guard против
# случайного JSON injection при f-string сборке body_str. Все наши callers
# формируют их контролируемо (ticker+USDT, str(float), uuid4().hex), но
# assert ловит regression если когда-нибудь упадёт нестандартный ввод.
_RE_ORDER_SAFE = re.compile(r"^[A-Za-z0-9._\-]+$")


def _post_order(symbol: str, side: str, qty: str, position_idx: int,
                order_link_id: str | None = None,
                retries: int = 2) -> dict:
    """
    Размещает Market ордер через Bybit V5 REST максимально быстро.
    :param symbol: "BTCUSDT" (linear).
    :param side: "Buy" | "Sell".
    :param qty: str с количеством в токенах.
    :param position_idx: 1=long-hedge, 2=short-hedge.
    :param order_link_id: уникальный ID ордера (max 36 chars). Защищает от
        дубля при retry: если Timeout произошёл из-за потерянного ответа,
        а ордер реально принят — повтор с тем же orderLinkId Bybit отвергнет
        с retCode 30050, и мы это интерпретируем как success (первый прошёл).
        Если None — retries безопаснее НЕ делать (см. retries-логику ниже).
    """
    # FIX-8: проверяем safe-символы — иначе f-string выше может сломать JSON/HMAC.
    if not _RE_ORDER_SAFE.match(symbol):
        raise ValueError(f"unsafe symbol for _post_order: {symbol!r}")
    if side not in ("Buy", "Sell"):
        raise ValueError(f"invalid side: {side!r}")
    if not _RE_ORDER_SAFE.match(qty):
        raise ValueError(f"unsafe qty for _post_order: {qty!r}")
    if order_link_id is not None and not _RE_ORDER_SAFE.match(order_link_id):
        raise ValueError(f"unsafe order_link_id: {order_link_id!r}")

    if order_link_id:
        body_str = (
            f'{{"category":"linear","symbol":"{symbol}","side":"{side}",'
            f'"orderType":"Market","qty":"{qty}","positionIdx":{position_idx},'
            f'"orderLinkId":"{order_link_id}"}}'
        )
    else:
        body_str = (
            f'{{"category":"linear","symbol":"{symbol}","side":"{side}",'
            f'"orderType":"Market","qty":"{qty}","positionIdx":{position_idx}}}'
        )

    # FIX: ретраи на Timeout/ConnectionError ОПАСНЫ для market-ордеров без
    # idempotency: Timeout не значит "ордер не принят", только "ответ не дошёл".
    # Поэтому ретраим только если есть orderLinkId — тогда Bybit сам отвергнет
    # дубль с retCode 30050, и мы это поймаем как success.
    effective_retries = retries if order_link_id else 0

    last_exc: Exception | None = None
    for attempt in range(effective_retries + 1):
        ts = str(int(time.time() * 1000))
        sign = _sign(ts, body_str)

        try:
            resp = _bybit_session.post(
                ORDER_CREATE_URL,
                data=body_str,
                headers={"X-BAPI-TIMESTAMP": ts, "X-BAPI-SIGN": sign},
                timeout=3,
            )
            # FIX-14: 5xx — серверная ошибка Bybit, безопасно retry с orderLinkId.
            # 4xx — клиентская, retry бесполезен.
            if 500 <= resp.status_code < 600:
                last_exc = requests.HTTPError(f"server {resp.status_code}", response=resp)
                if attempt < effective_retries:
                    time.sleep(0.1 * (attempt + 1))
                    continue
                raise last_exc
            resp.raise_for_status()
            data = _json_loads(resp.content)
            ret_code = data.get("retCode")
            if ret_code == 0:
                return data
            # FIX-7: дубль orderLinkId → нормализуем ответ к success-формату,
            # чтобы caller'ы которые проверяют retCode == 0 не запутались.
            if order_link_id and ret_code in _BYBIT_DUPLICATE_RET_CODES:
                print(
                    f"[BYBIT] duplicate orderLinkId={order_link_id} "
                    f"(attempt {attempt + 1}) — первый запрос прошёл, считаем success"
                )
                return {
                    "retCode": 0,
                    "retMsg":  "OK (deduped via orderLinkId)",
                    "result":  data.get("result", {}),
                    "_deduped": True,
                }
            raise RuntimeError(
                f"Bybit order error retCode={ret_code} "
                f"msg={data.get('retMsg')} symbol={symbol} qty={qty}"
            )
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if attempt < effective_retries:
                time.sleep(0.1 * (attempt + 1))
                continue
            raise

    # Недостижимо: либо return data, либо raise выше. Но pyright/mypy без этого
    # ругаются на "function may return None".
    assert last_exc is not None
    raise last_exc


def _new_order_link_id() -> str:
    """
    Уникальный orderLinkId для idempotency. Bybit допускает до 36 символов
    [A-Za-z0-9_-]. uuid4().hex = 32 hex chars — влезает с запасом.
    """
    return uuid.uuid4().hex


# FIX-10: публичные алиасы для импорта из других модулей. Прямой импорт
# приватных `_post_order` / `_new_order_link_id` в listing_api.py нарушал
# Python-конвенцию. Старые имена сохраняем для backward-compat внутри модуля.
post_order = _post_order
new_order_link_id = _new_order_link_id


# httpx-клиент с HTTP/2 для не-hot-path запросов (TP/SL trading-stop).
# Мультиплексирует параллельные _post через один TLS-stream → 3 параллельных
# TP-постановки идут одной connection-pool записью без повторных handshake.
# Lazy init: создаётся при первом обращении (httpx опциональная зависимость).
_httpx_client = None  # type: ignore[var-annotated]
_httpx_lock = threading.Lock()


def _get_httpx_client():
    """
    Возвращает httpx.Client(http2=True) или None если httpx не установлен.
    Singleton, переиспользует connection-pool до Bybit.
    """
    global _httpx_client
    if _httpx_client is not None:
        return _httpx_client
    with _httpx_lock:
        if _httpx_client is not None:
            return _httpx_client
        try:
            import httpx  # type: ignore[import-not-found]
        except ImportError:
            return None
        try:
            _httpx_client = httpx.Client(
                http2=True,
                timeout=httpx.Timeout(3.0, connect=2.0),
                limits=httpx.Limits(max_keepalive_connections=8, max_connections=16),
                headers={
                    "Content-Type":       "application/json",
                    "X-BAPI-API-KEY":     BYBIT_API_KEY,
                    "X-BAPI-RECV-WINDOW": RECV_WINDOW,
                },
            )
            print("[BYBIT-HTTP2] httpx http2-client готов")
            return _httpx_client
        except Exception as e:  # noqa: BLE001
            print(f"[BYBIT-HTTP2] init упал: {e!r} — будет requests fallback")
            return None


def _post_http2(endpoint: str, params: dict, retries: int = 2) -> dict:
    """
    HTTP/2 версия _post — мультиплексирует параллельные TP-постановки.
    Если httpx недоступен / connection broken → graceful fallback на _post.
    """
    client = _get_httpx_client()
    if client is None:
        return _post(endpoint, params, retries=retries)

    body_str = json.dumps(params, separators=(",", ":"))
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        ts = str(int(time.time() * 1000))
        sign = _sign(ts, body_str)
        try:
            resp = client.post(
                BYBIT_BASE_URL + endpoint,
                content=body_str,
                headers={"X-BAPI-TIMESTAMP": ts, "X-BAPI-SIGN": sign},
            )
            resp.raise_for_status()
            data = _json_loads(resp.content)
            ret_code = data.get("retCode", -1)
            if ret_code != 0:
                raise RuntimeError(
                    f"Bybit error retCode={ret_code} msg={data.get('retMsg')} params={params}"
                )
            return data
        except Exception as e:  # noqa: BLE001
            last_exc = e
            if attempt < retries:
                time.sleep(0.1 * (attempt + 1))
                continue
            # Последний шанс: попробуем через requests, если httpx стрельнул
            # transport-ошибкой. Только если это похоже на сетевой fault.
            try:
                return _post(endpoint, params, retries=0)
            except Exception:
                pass
            raise

    assert last_exc is not None
    raise last_exc


def _post(endpoint: str, params: dict, retries: int = 2) -> dict:
    """
    Общий подписанный POST к Bybit V5 (для TP/SL и других не-ордер запросов).
    Использует persist-сессию (keep-alive) — без переустановки TCP.

    Для размещения ордеров используется _post_order — он быстрее.
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

    # Недостижимо (см. _post_order), оставлено только для type-checker'ов.
    assert last_exc is not None
    raise last_exc


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

    FIX-PERF: парсит через msgspec.Struct (см. _parse_bybit_tickers) — на
    1000+ tickers экономит ~5-10мс GIL hold time vs orjson+dict. Это окно
    в котором worker может оказаться зажат если сигнал придёт прямо на
    JSON-parse. Fallback на orjson+dict если msgspec не установлен.
    """
    url     = BYBIT_BASE_URL + "/v5/market/tickers"
    session = requests.Session()

    use_msgspec = _parse_bybit_tickers is not None

    while True:
        try:
            resp = session.get(
                url,
                params={"category": "linear"},
                timeout=4,
            )
            resp.raise_for_status()

            new_cache: dict[str, float] = {}
            new_known: set[str] = set()

            if use_msgspec:
                # FIX-PERF: typed Struct, attribute access (.symbol vs ["symbol"]).
                for tk in _parse_bybit_tickers(resp.content):
                    symbol     = tk.symbol
                    last_price = tk.lastPrice
                    if last_price and symbol.endswith("USDT"):
                        try:
                            price = float(last_price)
                        except ValueError:
                            continue
                        coin = symbol[:-4]
                        new_cache[coin + "/USDT:USDT"] = price
                        new_known.add(coin)
            else:
                items = _json_loads(resp.content).get("result", {}).get("list", [])
                for item in items:
                    symbol     = item.get("symbol", "")
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
    # FIX-PERF: lockless fast-path. dict.get атомарен под GIL, lock нужен
    # только для записи (которая случается 1 раз на символ — preload или
    # первый ордер по неизвестному тикеру). Hot-path просто читает.
    step = _lot_step_cache.get(coin)
    if step is not None:
        return step

    try:
        data       = _get("/v5/market/instruments-info", {"category": "linear", "symbol": symbol})
        lot_filter = data["result"]["list"][0]["lotSizeFilter"]
        step       = float(lot_filter["qtyStep"])
    except Exception as e:
        raise QtyStepUnavailable(f"{symbol}: не получили шаг лота — {e}") from e

    with _lot_step_lock:
        _lot_step_cache[coin] = step
    return step


# FIX-PERF: import на модульном уровне + кеш precision по step.
# Раньше _round_qty каждый раз делал from decimal import (~0.01мс) и
# Decimal(str(step)).as_tuple().exponent (~0.05мс). Кеш убирает оба
# на повторных вызовах одного шага.
from decimal import Decimal as _Decimal
_qty_precision_cache: dict[float, int] = {}


def _round_qty(qty: float, step: float) -> float:
    """
    Округляет количество ВНИЗ до ближайшего шага лота.

    FIX: считаем precision через Decimal, потому что str(1e-05) == '1e-05'
    и старый код возвращал precision=0 (нет точки → ветка else), а потом
    round(..., 0) обнулял дробную часть → qty=0 → ордер не размещался либо
    падал на минимальный 1 контракт.

    FIX-AUDIT (критично): floor-division на float'ах теряла целый шаг лота.
    `5.0 // 0.1` == 49.0, а не 50.0, потому что 0.1 в двоичном виде чуть
    больше десятой. Итог — систематически заниженный объём позиции:

        _round_qty(5.0,  0.1  ) -> 4.9    (-2.0 %)
        _round_qty(3.0,  0.1  ) -> 2.9    (-3.3 %)
        _round_qty(0.3,  0.1  ) -> 0.2    (-33  %)
        _round_qty(1.0,  0.001) -> 0.999  (-0.1 %)

    Считаем через Decimal: точное деление, floor, обратно в float.
    """
    if step <= 0:
        raise ValueError(f"qty step должен быть > 0, получен {step!r}")

    precision = _qty_precision_cache.get(step)
    if precision is None:
        exponent  = _Decimal(str(step)).as_tuple().exponent
        precision = max(0, -int(exponent))
        _qty_precision_cache[step] = precision

    # Decimal(str(x)) — точное десятичное представление того, что видит
    # пользователь ("0.1", а не 0.1000000000000000055511151231257827).
    d_qty  = _Decimal(str(qty))
    d_step = _Decimal(str(step))
    steps  = int(d_qty / d_step)          # усечение вниз, без float-дрейфа
    return float(round(steps * d_step, precision))


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
                qty_str = str(amount_tokens)

                # FIX: один orderLinkId на оба пути (WS и REST fallback) —
                # защита от double-position при WS ack timeout: если WS-ордер
                # реально прошёл, но ack не пришёл, REST с тем же
                # orderLinkId Bybit отвергнет (retCode 30050 → success).
                order_link_id = _new_order_link_id()

                # FIX-batch-5: пробуем WS Trade API (−30...−80мс), при ошибке/None — REST.
                placed_via = "REST"
                ws_args = {
                    "category":    "linear",
                    "symbol":      symbol,
                    "side":        "Sell",
                    "orderType":   "Market",
                    "qty":         qty_str,
                    "positionIdx": 2,
                    "orderLinkId": order_link_id,
                }
                try:
                    ws_ack = _ws_place_order(ws_args)
                except WSOrderRejected as e:
                    # Защитный путь (fast вариант reject не бросает — он
                    # логирует асинхронно). Остался на случай если bybit_ws
                    # модуль не подгрузился и упал на старую sync-обёртку.
                    print(f"[BYBIT-WS] reject — пропускаем REST fallback: {e}")
                    return 0, 0
                except Exception as e:  # noqa: BLE001
                    print(f"[BYBIT-WS] ошибка place_order_ws: {e!r} — REST")
                    ws_ack = None

                if ws_ack is None:
                    # FIX-batch-8: специализированный _post_order (f-string JSON, −2..−5мс)
                    # FIX: тот же orderLinkId, что и в WS-попытке — idempotency.
                    _post_order(symbol, "Sell", qty_str, 2, order_link_id=order_link_id)
                else:
                    placed_via = "WS-FAST"

                with _delist_exchange_lock:
                    _delist_exchange[ticker_name] = "bybit"
                # FIX-PERF: удалён print "[BYBIT SHORT/{placed_via}]" — он стоял
                # ПЕРЕД return и добавлял ~1мс к open_ms (PYTHONUNBUFFERED=1).
                # На WS-failure bybit_ws_trade сам пишет "[BYBIT-WS-FAST] ...".
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


# ── Chain warmup (PEP-659 specialization) ────────────────────────
# Аналог listing_api.warmup_chain — прогрев adaptive interpreter
# для market_open_short. Подробности — см. listing_api.warmup_chain.

def warmup_chain(n: int = 30) -> int:
    """
    Прогоняет ТОТ ЖЕ Python-путь, что и market_open_short, N раз —
    без реальных ордеров. Возвращает число успешных итераций.

    BTC — гарантированно в price_cache и preloaded lot steps.
    """
    sample_ticker = "BTC"
    sample_margin = 10.0
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
            order_link_id = _new_order_link_id()
            ws_args = {
                "category":    "linear",
                "symbol":      symbol,
                "side":        "Sell",
                "orderType":   "Market",
                "qty":         qty_str,
                "positionIdx": 2,
                "orderLinkId": order_link_id,
            }
            _ws_place_order(ws_args, _warmup_mode=True)
            ok += 1
        except Exception:  # noqa: BLE001
            pass
    return ok


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
        Выставляет один уровень TP через trading-stop (http/2 — мультиплекс
        3 параллельных TP-постановок через 1 connection).
        :param tp_price: str - цена тейк-профита.
        :param tp_size: str - размер в токенах.
        """
        _post_http2("/v5/position/trading-stop", {
            "category":    "linear",
            "symbol":      symbol,
            "takeProfit":  tp_price,
            "tpTriggerBy": "LastPrice",
            "tpslMode":    "Partial",
            "tpSize":      tp_size,
            "positionIdx": 2,
        })

    # SL ставим отдельно (один на всю позицию).
    _post_http2("/v5/position/trading-stop", {
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
# FIX: добавлен IGNORECASE — иногда каналы шлют "$alcx" вместо "$ALCX".
_RE_DOLLAR_TKN  = re.compile(r"\$([A-Za-z][A-Za-z0-9]{1,9})\b")


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
    # FIX-PERF: убраны print'ы "[FIND PAIRS] метод=..." — find_pairs
    # вызывается в hot-path TG/article handler'а перед submit'ом, каждый
    # print с PYTHONUNBUFFERED=1 ≈ ~0.5-1мс. "Монеты: [...]" в DELIST-блоке
    # показывает найденные тикеры; method для debug — раскомментировать.
    text_upper = text.upper()

    usdt_pairs = _RE_USDT.findall(text_upper)
    if usdt_pairs:
        found = _filter_tokens(usdt_pairs)
        if found:
            return found

    # FIX-batch-7: $TICKER маркеры (CLW: "Monitoring Tag Added – $ALCX...")
    # FIX: нормализуем в uppercase — каналы изредка пишут "$alcx".
    dollar_pairs = [t.upper() for t in _RE_DOLLAR_TKN.findall(text)]
    if dollar_pairs:
        found = _filter_tokens(dollar_pairs)
        if found:
            return found

    m = _RE_WILL_DELIST.search(text_upper)
    if m:
        tokens = _RE_PAIR_TOKENS.findall(m.group(1))
        found  = _filter_tokens(tokens)
        if found:
            return found

    # FIX-batch-6: "Will Extend the Monitoring Tag to Include ALCX..."
    m = _RE_MONITORING.search(text_upper)
    if m:
        tokens = _RE_PAIR_TOKENS.findall(m.group(1))
        found  = _filter_tokens(tokens)
        if found:
            return found

    m = _RE_DELIST_BLOCK.search(text_upper)
    if m:
        tokens = _RE_PAIR_TOKENS.findall(m.group(1))
        found  = _filter_tokens(tokens)
        if found:
            return found

    # Широкий fallback-скан всего текста. Здесь любое английское слово может
    # случайно совпасть с тикером, поэтому AMBIGUOUS_TOKENS режем безусловно
    # (allow_ambiguous=False) — иначе артикль «the» в «Notice Regarding the
    # Removal of AEUR» дал бы ложный сигнал шортить THE.
    all_tokens = _RE_ALL_TOKENS.findall(text_upper)
    return [
        t for t in _filter_tokens(all_tokens, allow_ambiguous=False)
        if t in known_coins
    ]


def _filter_tokens(tokens: list[str], allow_ambiguous: bool = True) -> list[str]:
    """
    Фильтрует список токенов: убирает стоп-слова, дубликаты и слишком
    короткие/длинные. Также отсеивает токены состоящие только из цифр.

    FIX-AUDIT: двухуровневый стоп-лист.
      • ALWAYS_EXCLUDED  — режем всегда (грамматика, квоты, служебные слова).
      • AMBIGUOUS_TOKENS — слово И тикер одновременно (THE, AT, OPEN, ORDER…).

    :param allow_ambiguous:
        True  — «явный» контекст: токен извлечён из структурированного
                паттерна ("Will Delist X, Y on ...", "$TICKER", "ABCUSDT").
                Здесь AMBIGUOUS_TOKENS пропускаются при подтверждении
                known_coins — так «Will Delist HOT, THE» больше не теряет
                THE (Thena).
        False — «широкий» fallback-скан всего текста статьи. Тут любое
                английское слово может случайно совпасть с тикером, поэтому
                AMBIGUOUS_TOKENS режем безусловно. Иначе на заголовке
                «Notice Regarding the Removal of AEUR» артикль «the»
                превращался бы в сигнал шортить THE.

    :param tokens: list[str] - список токенов из regex.
    :return: list[str] - отфильтрованный список тикеров.
    """
    seen:   set[str]  = set()
    result: list[str] = []
    for t in tokens:
        if t in ALWAYS_EXCLUDED:
            continue
        if not (2 <= len(t) <= 10):
            continue
        if t.isdigit():
            continue
        if t in AMBIGUOUS_TOKENS:
            # В широком скане — никогда. В явном — только если биржа
            # действительно торгует такой инструмент.
            if not allow_ambiguous or t not in known_coins:
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


# ── TLS heartbeat: держим pool горячим ───────────────────────────
# Если между ордерами проходит > ~90с (keep-alive idle timeout), connection
# в пуле закроется, и следующий ордер заплатит полный TCP+TLS handshake
# (15-30мс из Singapore). Heartbeat-поток шлёт лёгкий /v5/market/time каждые
# 8с — переиспользует тот же _bybit_session.pool, держит socket hot.
# Win: гарантированно 0 handshake на сигнале.
_HEARTBEAT_INTERVAL = 8.0


def _bybit_heartbeat_loop() -> None:
    """Бесконечный keep-alive heartbeat к Bybit. Запускается из main bootstrap."""
    while True:
        try:
            _bybit_session.get(BYBIT_BASE_URL + "/v5/market/time", timeout=3)
        except Exception:
            # Глотаем — сеть могла мигнуть, следующий цикл попробует снова.
            pass
        time.sleep(_HEARTBEAT_INTERVAL)


_heartbeat_thread: threading.Thread | None = None
_heartbeat_lock = threading.Lock()


def start_bybit_heartbeat() -> None:
    """
    Запускает фоновый heartbeat (один раз, idempotent). Безопасно вызывать
    из bootstrap'а — повторные вызовы no-op.
    """
    global _heartbeat_thread
    with _heartbeat_lock:
        if _heartbeat_thread is not None and _heartbeat_thread.is_alive():
            return
        t = threading.Thread(
            target=_bybit_heartbeat_loop,
            daemon=True,
            name="bybit-tls-heartbeat",
        )
        t.start()
        _heartbeat_thread = t
        print(f"[WARMUP] Bybit TLS-heartbeat запущен ({_HEARTBEAT_INTERVAL:.0f}с интервал)")