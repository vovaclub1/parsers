from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional
from config.config import (
    BYBIT_API_KEY,
    BYBIT_SECRET_KEY,
    DELIST_TRAILING_PCT,
    DELIST_ACTIVE_PCT,
    DELIST_SL_PCT,
    DELIST_TRAIL_MODE,
    DELIST_ATR_INTERVAL,
    DELIST_ATR_PERIOD,
    DELIST_ATR_MIN_CANDLES,
    DELIST_ATR_TRAIL_MULT,
    DELIST_ATR_ACT_MULT,
    DELIST_ATR_SL_MULT,
    DELIST_ATR_TRAIL_MIN_PCT,
    DELIST_ATR_TRAIL_MAX_PCT,
    DELIST_ATR_ACT_MIN_PCT,
    DELIST_ATR_ACT_MAX_PCT,
    DELIST_ATR_SL_MIN_PCT,
    DELIST_ATR_SL_MAX_PCT,
    DELIST_SIM_ATR_K,
    DELIST_SIM_ATR_SL,
    DELIST_SIM_ATR_PERIOD,
    DELIST_SIM_ATR_INTERVAL,
    DELIST_SIM_ATR_MIN_CANDLES,
    DELIST_SIM_ATR_LO,
    DELIST_SIM_ATR_HI,
    DELIST_SIM_ATR_FALLBACK,
)
from api.atr import compute_atr, clamp_distance, live_sample_atr_frac
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
    gate_known_snapshot,        # FIX: Gate-only делисты (XNO) — шортятся на Gate
    gate_price_updater,         # noqa: F401  — реэкспорт для parser_delist
    gate_preload_lot_steps,     # noqa: F401  — реэкспорт для parser_delist
    warmup_gate_connection,     # noqa: F401  — реэкспорт для parser_delist
)

# FIX: вынесли импорт из хот-функции market_open_short на модульный уровень.
# FIX-PERF: fire-and-forget вариант place_order_ws_fast — экономит ~70-100мс
# на ack-roundtrip. Reject логируется в фоне через _watch_ack.
try:
    from api.bybit_ws_trade import (
        place_order_ws_fast as _ws_place_order,
        place_order_ws_ack as _ws_place_order_ack,
        WSOrderRejected,
    )
except Exception as _ws_import_exc:  # noqa: BLE001 — graceful
    print(f"[BYBIT-WS] модуль не подгружен: {_ws_import_exc!r} — будет только REST")
    def _ws_place_order(args: dict, _warmup_mode: bool = False) -> dict | None:  # type: ignore[misc]
        return None
    def _ws_place_order_ack(args: dict, timeout: float = 1.5) -> dict | None:  # type: ignore[misc]
        return None
    class WSOrderRejected(Exception):  # type: ignore[no-redef]
        """Stub если bybit_ws_trade не подгрузился — никогда не raise-нется."""
        pass

# FIX 2026-06-19 (R3): private WS для real-time чтения position вместо REST poll.
try:
    from api import bybit_ws_private as _ws_private  # noqa: F401
except Exception:  # noqa: BLE001
    _ws_private = None  # type: ignore[assignment]

# ── Конфиг Bybit ─────────────────────────────────────────────────
BYBIT_BASE_URL = "https://api.bybit.com"
ORDER_CREATE_URL = BYBIT_BASE_URL + "/v5/order/create"   # FIX-batch-8: pre-built URL
RECV_WINDOW    = "5000"
LEVERAGE       = 10   # FIX: вынес магическое число в константу

# FIX 2026-06-05: ретраи на price-cap reject (30208/30209 — см. listing_api).
# Для шорта: 30209 = "order price lower than minimum selling price" (cap снизу).
_PRICE_CAP_RETCODES = {30208, 30209}
_ORDER_RETRIES = int(os.getenv("DELIST_ORDER_RETRIES", "10"))
_ORDER_RETRY_SLEEP = float(os.getenv("DELIST_ORDER_RETRY_MS", "7")) / 1000.0

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
EXCLUDED_TOKENS = {
    "USDT", "BUSD", "USDC", "TUSD", "DAI",
    # FIX 2026-06-19: КВОУТ-валюты пар. На «removal of trading pairs» нотисах
    # (margin/spot) пара вида CVC/USDC, ADX/BTC, DOT/BNB — правая часть это квоут,
    # её НИКОГДА не делистят и шортить нельзя (иначе зашортим BTC/ETH по посту про
    # удаление пары ADX/BTC). Базовая монета (левая часть) — да, квоут — нет.
    "BTC", "ETH", "BNB", "FDUSD", "EUR", "TRY", "BRL", "GBP", "AUD", "ARS",
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
    # FIX: убрана "USDⓈ" — regex [A-Z0-9] её никогда не вернёт (Ⓢ U+24C8 не ASCII).
    "USDS",  # Binance liquid staking
    "ANNOUNCEMENT", "ANNOUNCEMENTS",
    # FIX: ложные срабатывания на «margin trading pairs» нотисах
    # (см. инцидент 2026-05-25: матчилось 'FOLLOWING' и 'AT',
    # последнее реально шортилось как тикер AT). Расширяем список
    # самыми частыми «английскими словами длиной 2-8», которые могут
    # появиться в теле уведомления и пройти regex [A-Z0-9]{2,10}.
    "AT", "AS", "BY", "OR", "OF", "IT", "IF", "SO", "DO",
    "FOLLOWING", "FOLLOWED", "FOLLOWS",
    "PLEASE", "NOTE", "NOTED", "NOTES",
    "EFFECTIVE", "STARTING", "BEGINNING", "ENDING", "ENDS",
    "DATE", "TIME", "TIMES", "HOUR", "HOURS",
    "USERS", "USER", "CLIENTS", "CLIENT",
    "WITHDRAWAL", "WITHDRAWALS", "WITHDRAW",
    "DEPOSIT", "DEPOSITS",
    "ORDER", "ORDERS", "POSITION", "POSITIONS",
    "BALANCE", "BALANCES", "ACCOUNT", "ACCOUNTS",
    "FUND", "FUNDS", "FUNDING",
    "ISOLATED", "CROSS", "LEVERAGE", "LEVERAGED",
    "CONVERT", "CONVERTED", "CONVERTING",
    "COPY", "BOT", "BOTS",
    "REGION", "REGIONS", "COUNTRY", "COUNTRIES",
    "SUBJECT", "TERMS", "AGREEMENT", "POLICY", "POLICIES",
    "DUE", "PER", "VIA", "INTO", "OUT", "AFTER", "BEFORE",
    "ABOVE", "BELOW", "BETWEEN",
    "DETAILS", "DETAIL", "MORE", "LESS", "ABOUT",
    # FIX: словарные английские слова из Binance Earn / Launchpool /
    # promo-заголовков. Метод 5 (fallback по known_coins) в
    # find_listing_pairs выдирает любые \b[A-Z0-9]{2,8}\b слова и
    # отсеивает по EXCLUDED_TOKENS + known_coins. На длинных промо-
    # текстах ('Subscribe to ... Locked Products ... Enjoy 200% APR
    # for 7 Days') проходило APR/DAYS/etc. Все они тут.
    "APR", "APY",
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
    # Часто встречаются в Bithumb/Upbit заголовках на корейском
    # транслите/английском, но не тикеры:
    "EVENT", "EVENTS", "CELEBRATION", "CELEBRATE",
}

# Слова, которые одновременно являются реальными тикерами. В строгом
# структурированном контексте их разрешаем при подтверждении инструментом;
# в широком скане блокируем, чтобы английские слова не стали сигналом.
AMBIGUOUS_TOKENS = {
    "THE", "AT", "ON", "IN", "ALL", "NOT", "BE", "MAY",
    "OPEN", "ORDER", "CROSS", "TAG", "COIN", "BOT",
    "APR", "NFT", "USDC", "USDS",
}
EXCLUDED_TOKENS = EXCLUDED_TOKENS - AMBIGUOUS_TOKENS

# ── Кэш цен ──────────────────────────────────────────────────────
price_cache: dict[str, float] = {}
price_cache_prev: dict[str, float] = {}   # снапшот предыдущего цикла (~2с назад)
_price_prev_at = 0.0
cache_lock  = threading.Lock()
known_coins: set[str] = set()

# FIX (review high): монотонный timestamp последнего УСПЕШНОГО обновления.
# Без него при сбое Bybit (outage / parse-fail) price_updater молча
# держит замороженный кэш, а market_open_short сайзит реальный шорт по
# старой цене во время делистинг-памп/дампа — это money-losing путь.
# get_price гейтит по возрасту → None → авто-fallback на Gate.io (свежая цена).
_price_cache_updated_at = 0.0   # time.monotonic() последнего успеха
_PRICE_STALE_SEC = 6.0          # 3 пропущенных 2с-цикла → считаем устаревшим


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

# FIX 2026-06-10: retCode «not modified» на /v5/position/trading-stop —
# отправленный SL/TP/trailing РАВЕН уже стоящему на позиции. Значит защита
# УЖЕ на месте (SL бандлится в order.create при открытии) → это НЕ ошибка,
# а no-op. Раньше _post_http2/_post бросали RuntimeError на любой retCode≠0 и
# ещё ретраили 3-4 раза впустую (инцидент с RH-EXIT). Трактуем как success.
_BYBIT_NOOP_RET_CODES = {34040}  # not modified


# FIX-8: разрешённые символы в symbol/qty/order_link_id — guard против
# случайного JSON injection при f-string сборке body_str. Все наши callers
# формируют их контролируемо (ticker+USDT, str(float), uuid4().hex), но
# assert ловит regression если когда-нибудь упадёт нестандартный ввод.
_RE_ORDER_SAFE = re.compile(r"^[A-Za-z0-9._\-]+$")


def _post_order(symbol: str, side: str, qty: str, position_idx: int,
                order_link_id: str | None = None,
                retries: int = 2,
                stop_loss: str | None = None,
                take_profit: str | None = None,
                tp_size: str | None = None,
                reduce_only: bool = False,
                slippage_tol_pct: float = 0.0) -> dict:
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
    for _v in (stop_loss, take_profit, tp_size):
        if _v is not None and not _RE_ORDER_SAFE.match(_v):
            raise ValueError(f"unsafe sl/tp value for _post_order: {_v!r}")

    _inner = (
        f'"category":"linear","symbol":"{symbol}","side":"{side}",'
        f'"orderType":"Market","qty":"{qty}","positionIdx":{position_idx}'
    )
    if reduce_only:
        # reduce-only: ордер только уменьшает позицию, не откроет противоположную.
        _inner += ',"reduceOnly":true'
    if order_link_id:
        _inner += f',"orderLinkId":"{order_link_id}"'
    # M5: failsafe SL/TP прямо в теле REST-fallback ордера — позиция защищена
    # в момент филла, как в WS-фрейме. Раньше fallback открывался голым, а
    # SL/TP доезжали отдельным /v5/position/trading-stop с задержкой.
    if stop_loss is not None:
        _inner += f',"stopLoss":"{stop_loss}","slTriggerBy":"LastPrice"'
    if take_profit is not None:
        _inner += f',"takeProfit":"{take_profit}","tpTriggerBy":"LastPrice"'
        # tpslMode:"Partial" ВАЛИДЕН только вместе с tpSize — иначе Bybit
        # отвергнет ордер. Без tpSize оставляем дефолтный Full-режим TP.
        if tp_size is not None:
            _inner += f',"tpslMode":"Partial","tpSize":"{tp_size}"'
    # FIX 2026-06-19: slippage cap на стороне Bybit (REST fallback для листинга).
    # Принимаем число от listing_api._SLIPPAGE_TOL_PCT, в payload — строка.
    if slippage_tol_pct and slippage_tol_pct > 0:
        _inner += f',"slippageToleranceType":"Percent","slippageTolerance":"{slippage_tol_pct}"'
    body_str = "{" + _inner + "}"

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
            ret_code = data.get("retCode", -1)  # FIX: consistent default с остальными вызовами
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
            if ret_code in _BYBIT_NOOP_RET_CODES:
                # «not modified» — SL/TP уже стоит. Success, без raise/ретраев.
                return {"retCode": 0, "retMsg": "OK (not modified, no-op)",
                        "result": data.get("result", {}), "_noop": True}
            if ret_code != 0:
                raise RuntimeError(
                    f"Bybit error retCode={ret_code} msg={data.get('retMsg')} params={params}"
                )
            return data
        except RuntimeError:
            # Бизнес-ошибка Bybit (retCode!=0) сама не починится ретраем — пробрасываем
            # сразу, чтобы _trading_stop_settle разрулил (осёдка / снятие activePrice).
            raise
        except Exception as e:  # noqa: BLE001 — транспортные сбои httpx
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


# FIX 2026-06-06: ретраи trading-stop на retCode=10001 "zero position".
# Позиция оседает на Bybit с задержкой после market-ордера; trading-stop
# выставленный сразу падает 10001, и трейлинг/SL НЕ ставятся (инцидент:
# позиция без трейлинга, цена развернулась, закрывали руками). Ждём осёдку.
_TPSL_SETTLE_RETRIES = int(os.getenv("TPSL_SETTLE_RETRIES", "8"))
_TPSL_SETTLE_SLEEP = float(os.getenv("TPSL_SETTLE_SLEEP_MS", "300")) / 1000.0

# FIX 2026-06-17: верификация TP/SL делиста по РЕАЛЬНОЙ позиции (как на листинге).
# Ждём осёдку позиции, ставим трейлинг к реальной avgPrice, проверяем что он СЕЛ,
# ретраим пока позиция жива. Если уже закрылась стопом — спокойный лог, не FAIL.
_DELIST_POS_WAIT_RETRIES     = int(os.getenv("DELIST_POS_WAIT_RETRIES", "10"))
_DELIST_POS_WAIT_SLEEP       = float(os.getenv("DELIST_POS_WAIT_SLEEP_MS", "300")) / 1000.0
_DELIST_TRAIL_VERIFY_RETRIES = int(os.getenv("DELIST_TRAIL_VERIFY_RETRIES", "4"))
_DELIST_TRAIL_VERIFY_SLEEP   = float(os.getenv("DELIST_TRAIL_VERIFY_SLEEP_MS", "300")) / 1000.0


def _is_zero_position_err(low: str) -> bool:
    """Подлинная гонка осёдки: позиция ещё не появилась на Bybit (retCode 10001
    "...zero position"). НЕ путать с другими 10001-ошибками (валидация цены и т.п.)."""
    return ("zero position" in low
            or "position is closed" in low
            or "position not exist" in low
            or "position does not exist" in low)


def _is_trailing_active_err(low: str) -> bool:
    """activePrice невалидна: цена УЖЕ прошла точку активации трейлинга.
    Пример Bybit: "TrailingProfit:... set for Sell position should be less than
    ... last_price:...". Лечится снятием activePrice (немедленная активация)."""
    return ("trailingprofit" in low
            or ("trailing" in low and ("last_price" in low or "should be" in low)))


def _trading_stop_settle(params: dict, tag: str = "") -> dict:
    """
    /v5/position/trading-stop с устойчивыми ретраями:
      • гонка осёдки позиции (10001 "zero position") → ждём _TPSL_SETTLE_SLEEP и повторяем;
      • невалидная activePrice (цена делистнутой монеты уже прошла точку активации —
        Bybit "...should be less than last_price...") → снимаем activePrice и повторяем
        НЕМЕДЛЕННО (трейлинг активируется от текущей цены — то, что и нужно при дампе);
      • прочие ошибки пробрасываем сразу.
    Возвращает data при успехе, либо raise.
    """
    params = dict(params)  # копия — не мутируем словарь вызывающего
    settle_attempts = 0
    stripped_active = False
    last_exc: Exception | None = None
    while True:
        try:
            return _post_http2("/v5/position/trading-stop", params)
        except Exception as e:  # noqa: BLE001
            low = str(e).lower()
            # (1) activePrice уже пройдена ценой → активируем трейлинг немедленно
            if (not stripped_active and "activePrice" in params
                    and _is_trailing_active_err(low)):
                params.pop("activePrice", None)
                stripped_active = True
                print(f"[TP/SL SETTLE] {tag}: цена прошла точку активации — "
                      f"активирую трейлинг немедленно (без activePrice)", flush=True)
                continue  # немедленный повтор, не тратим попытку осёдки
            # (2) подлинная осёдка нулевой позиции → ждём и ретраим
            if _is_zero_position_err(low):
                last_exc = e
                settle_attempts += 1
                if settle_attempts < _TPSL_SETTLE_RETRIES:
                    print(f"[TP/SL SETTLE] {tag} zero-position, ждём осёдку "
                          f"(попытка {settle_attempts}/{_TPSL_SETTLE_RETRIES})", flush=True)
                    time.sleep(_TPSL_SETTLE_SLEEP)
                    continue
                raise  # исчерпали ретраи осёдки
            # (3) прочее — реальная ошибка, не ретраим
            raise


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
            if ret_code in _BYBIT_NOOP_RET_CODES:
                return {"retCode": 0, "retMsg": "OK (not modified, no-op)",
                        "result": data.get("result", {}), "_noop": True}
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


def _signed_get(endpoint: str, params: dict, timeout: float = 3.0) -> dict:
    """Подписанный GET к приватным эндпоинтам Bybit V5 (позиция и т.п.).
    Подпись = HMAC(ts + apiKey + recvWindow + queryString)."""
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    ts = str(int(time.time() * 1000))
    sign = _sign(ts, qs)
    headers = {
        "X-BAPI-API-KEY":     BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP":   ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN":        sign,
    }
    resp = _bybit_session.get(BYBIT_BASE_URL + endpoint + "?" + qs,
                              headers=headers, timeout=timeout)
    resp.raise_for_status()
    return _json_loads(resp.content)


def get_position(symbol: str, position_idx: int = 1):
    """РЕАЛЬНОЕ состояние позиции с биржи: (size, avg_price, trailing_on).

    (0.0, 0.0, False) если позиции нет (не открылась / уже закрыта стопом).
    avg_price — фактическая средняя цена входа (для якоря SL/трейлинга вместо
    устаревшей кеш-цены сигнала). trailing_on — стоит ли уже trailingStop.
    """
    try:
        data = _signed_get("/v5/position/list",
                           {"category": "linear", "symbol": symbol})
    except Exception as e:  # noqa: BLE001 — сеть/подпись: считаем «неизвестно»
        print(f"[POSITION] {symbol} query err: {e!r}", flush=True)
        return 0.0, 0.0, False
    if str(data.get("retCode")) != "0":
        return 0.0, 0.0, False
    for p in (data.get("result") or {}).get("list") or []:
        try:
            if int(p.get("positionIdx", -1)) != position_idx:
                continue
            sz = float(p.get("size") or 0)
        except (TypeError, ValueError):
            continue
        if sz > 0:
            try:
                avg = float(p.get("avgPrice") or 0)
            except (TypeError, ValueError):
                avg = 0.0
            try:
                trailing_on = float(p.get("trailingStop") or 0) > 0
            except (TypeError, ValueError):
                trailing_on = False
            return sz, avg, trailing_on
    return 0.0, 0.0, False


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
                # FIX 2026-06-12: снапшот ПРЕДЫДУЩего цикла (~2с назад) — референс
                # для фильтра позднего входа (понять, успела ли цена уже улететь
                # до того как мы получили сигнал листинга).
                global _price_cache_updated_at, _price_prev_at
                if price_cache:
                    price_cache_prev.clear()
                    price_cache_prev.update(price_cache)
                    _price_prev_at = _price_cache_updated_at
                price_cache.clear()
                price_cache.update(new_cache)
                known_coins.clear()
                known_coins.update(new_known)
                # FIX (review high): отмечаем успешное обновление.
                _price_cache_updated_at = time.monotonic()

        except Exception as e:
            # FIX: тип ошибки в логе — отличить network-blip от code-contract
            # бага (напр. изменение _parse_bybit_tickers).
            print(f"[PRICE CACHE ERROR] {type(e).__name__}: {e}")

        time.sleep(2)


# ── Торговые функции ──────────────────────────────────────────────

def get_price(coin: str) -> Optional[float]:
    """
    Возвращает последнюю цену монеты из кэша, ИЛИ None если кэш устарел.
    :param coin: str - тикер монеты (например "BTC").
    :return: float | None - цена в USDT, None если монеты нет ИЛИ кэш протух.

    FIX (review high): age-gate. Если price_updater не обновлял кэш дольше
    _PRICE_STALE_SEC (Bybit outage / parse-fail), возвращаем None — это
    переводит market_open_* на свежий Gate.io fallback вместо сайзинга
    ордера по замороженной цене во время делистинг-движения.
    """
    if (time.monotonic() - _price_cache_updated_at) > _PRICE_STALE_SEC:
        return None
    # FIX: race condition — price_cache.clear() в price_updater может
    # произойти между этим чтением и возвратом. Берём под lock.
    with cache_lock:
        return price_cache.get(f"{coin}/USDT:USDT")


def price_ago(coin: str) -> Optional[float]:
    """Цена монеты из ПРЕДЫДУЩего цикла (~2с назад), или None если её там не было.
    Референс для фильтра позднего входа: сравнить текущую цену с этой."""
    with cache_lock:
        return price_cache_prev.get(f"{coin}/USDT:USDT")


def fetch_live_price(coin: str, timeout: float = 0.7) -> Optional[float]:
    """СВЕЖАЯ цена одного символа прямым REST-запросом (НЕ из 2с-кеша).

    Нужна для ретрая-на-откат при фильтре позднего входа: кеш обновляется раз в 2с,
    поэтому перечитывание get_price() через десятки мс вернуло бы то же значение.
    Этот запрос (~30-80мс) даёт актуальный lastPrice. None при ошибке/таймауте.
    """
    symbol = f"{coin}USDT"
    try:
        resp = _bybit_session.get(
            BYBIT_BASE_URL + "/v5/market/tickers",
            params={"category": "linear", "symbol": symbol},
            timeout=timeout,
        )
        resp.raise_for_status()
        data = _json_loads(resp.content)
        lst = ((data.get("result") or {}).get("list")) or []
        if lst:
            lp = lst[0].get("lastPrice")
            if lp:
                return float(lp)
    except Exception:  # noqa: BLE001 — сеть/parse: вернём None, caller решит
        return None
    return None


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
_max_leverage_cache: dict[str, float] = {}
_min_qty_cache: dict[str, float] = {}


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
                    # FIX: защита от KeyError если структура ответа изменилась
                    lot_filter = item.get("lotSizeFilter")
                    if lot_filter and "qtyStep" in lot_filter:
                        try:
                            step = float(lot_filter["qtyStep"])
                            _lot_step_cache[coin] = step
                        except (ValueError, TypeError):
                            continue  # Пропускаем некорректные данные
                        try:
                            _min_qty_cache[coin] = float(lot_filter.get("minOrderQty", 0) or 0)
                        except (ValueError, TypeError):
                            _min_qty_cache[coin] = 0.0
                        try:
                            max_lev = (item.get("leverageFilter") or {}).get("maxLeverage")
                            if max_lev is not None:
                                _max_leverage_cache[coin] = float(max_lev)
                        except (ValueError, TypeError):
                            pass
        print(f"[PRELOAD] Загружено {len(_lot_step_cache)} шагов лота")
    except Exception as e:
        print(f"[PRELOAD ERROR] {e}")


def effective_leverage(coin: str) -> float:
    limit = _max_leverage_cache.get(coin)
    if limit is None or limit <= 0:
        return float(LEVERAGE)
    return float(min(LEVERAGE, limit))


def min_order_qty(coin: str) -> float:
    return _min_qty_cache.get(coin, 0.0)


def calculate_bybit_qty(coin: str, margin: float, price: float, step: float) -> tuple[float, float]:
    """Возвращает (qty, effective_leverage); qty=0 если ниже minOrderQty."""
    lev = effective_leverage(coin)
    qty = _round_qty((margin / price) * lev, step)
    minimum = min_order_qty(coin)
    if qty > 0 and minimum > 0 and qty < minimum:
        return 0.0, lev
    return qty, lev


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

    # FIX (review high): отличаем «символа нет на Bybit» (пустой list →
    # QtyStepUnavailable → корректный Gate fallback) от TRANSIENT-сбоя
    # (timeout/connection → Bybit на самом деле жив, просто медленный).
    # Раньше любой timeout молча переводил шорт на Gate (другая ликвидность/
    # комиссия). Теперь на transient делаем ОДИН быстрый retry перед сдачей.
    last_exc: Exception | None = None
    for attempt in range(2):  # 1 попытка + 1 retry
        try:
            data = _get("/v5/market/instruments-info", {"category": "linear", "symbol": symbol})
            result = data.get("result", {})
            items = result.get("list", [])
            if not items:
                raise QtyStepUnavailable(f"{symbol}: пустой список инструментов в ответе")
            lot_filter = items[0].get("lotSizeFilter")
            if not lot_filter or "qtyStep" not in lot_filter:
                raise QtyStepUnavailable(f"{symbol}: отсутствует lotSizeFilter.qtyStep в ответе")
            step = float(lot_filter["qtyStep"])
            break
        except QtyStepUnavailable:
            raise  # Символ реально отсутствует — пробрасываем (Gate fallback корректен)
        except (requests.Timeout, requests.ConnectionError) as e:
            # Transient — быстрый retry (50мс), Bybit жив.
            last_exc = e
            if attempt == 0:
                time.sleep(0.05)
                continue
            raise QtyStepUnavailable(f"{symbol}: transient после retry — {e}") from e
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
    """Округляет количество вниз до шага без артефактов float division."""
    if step <= 0:
        raise ValueError(f"qty step должен быть > 0, получено {step}")
    qty_dec = _Decimal(str(qty))
    step_dec = _Decimal(str(step))
    precision = _qty_precision_cache.get(step)
    if precision is None:
        precision = max(0, -int(step_dec.as_tuple().exponent))
        _qty_precision_cache[step] = precision
    units = qty_dec // step_dec
    rounded = units * step_dec
    return round(float(rounded), precision)


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

    # FIX 2026-06-24: флаг видим И в Bybit-блоке (где он выставляется на reject
    # типа 30228 "No new positions during delisting"), И в Gate-fallback внизу
    # (где он определяет — фейл-фастить или ретраиться).
    bybit_rejected = False

    # FIX: проверка на 0 или отрицательную цену (защита от деления на ноль)
    if bybit_price and bybit_price > 0:
        symbol  = f"{ticker_name}USDT"

        try:
            step = _get_qty_step(symbol)
        except QtyStepUnavailable as e:
            # FIX: раньше step=1.0 → ордер мог уйти в 10× больше.
            # Лучше упасть на Gate.io fallback.
            print(f"[QTY STEP MISSING BYBIT] {e} — пробуем Gate.io")
            bybit_price = None
        else:
            amount_tokens, lev = calculate_bybit_qty(ticker_name, usdt_amount, bybit_price, step)
            if amount_tokens <= 0 and min_order_qty(ticker_name) > 0:
                print(f"[QTY BELOW MIN BYBIT] {symbol}: margin={usdt_amount}, lev={lev:g} — Gate fallback")
                bybit_price = None

            if amount_tokens > 0:
                qty_str = str(amount_tokens)

                # FIX: один orderLinkId на оба пути (WS и REST fallback) —
                # защита от double-position при WS ack timeout: если WS-ордер
                # реально прошёл, но ack не пришёл, REST с тем же
                # orderLinkId Bybit отвергнет (retCode 30050 → success).
                order_link_id = _new_order_link_id()

                # FIX 2026-06-17: bundled SL прямо в order.create (как на листинге).
                # Раньше делистовый шорт открывался ГОЛЫМ — защита приходила только
                # отдельным /v5/position/trading-stop, и при его провале (34040/
                # zero-position/trailing-invalid) позиция оставалась без стопа
                # (инцидент HIGH: закрывали руками). SL для шорта ВЫШЕ входа.
                sl_price = str(round(bybit_price * (1 + DELIST_SL_PCT), 8))

                # FIX 2026-06-05: ack-waiting + ретраи на price-cap (зеркало listing).
                ws_args = {
                    "category":    "linear",
                    "symbol":      symbol,
                    "side":        "Sell",
                    "orderType":   "Market",
                    "qty":         qty_str,
                    "positionIdx": 2,
                    "orderLinkId": order_link_id,
                    "stopLoss":    sl_price,
                    "slTriggerBy": "LastPrice",
                }
                success = False
                # FIX 2026-06-24: на reject (30228 "No new positions during delisting",
                # symbol-not-found и т.д.) — НЕ return 0,0 сразу, а break → Gate fallback
                # ниже. Раньше Gate-блок был недостижим: воркер видел (0,0) → "нет цены"
                # → 3×0.1с retry → "сдаёмся", даже когда Gate имел эту монету.
                # Кейс из прода: IPUSDT 17:01:18 — Bybit 30228, Gate имел IP.
                # bybit_rejected объявлен на func-scope выше.
                for attempt in range(1, _ORDER_RETRIES + 1):
                    ack = _ws_place_order_ack(ws_args)
                    if ack is None:
                        # WS недоступен/таймаут → REST fallback один раз (тоже с SL).
                        # FIX 2026-07-08 (NEO-инцидент): REST-исключение (33004
                        # expired key и т.п.) раньше пролетало МИМО Gate-fallback
                        # и убивало весь market_open_short — воркер видел
                        # «попытка упала», Gate даже не пробовался.
                        try:
                            _post_order(symbol, "Sell", qty_str, 2,
                                        order_link_id=order_link_id, stop_loss=sl_price)
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
                    with _delist_exchange_lock:
                        _delist_exchange[ticker_name] = "bybit"
                    # FIX-PERF: удалён print "[BYBIT SHORT/{placed_via}]" — он стоял
                    # ПЕРЕД return и добавлял ~1мс к open_ms (PYTHONUNBUFFERED=1).
                    # На WS-failure bybit_ws_trade сам пишет "[BYBIT-WS-FAST] ...".
                    return amount_tokens, bybit_price
                # Не вышло на Bybit — проваливаемся в Gate fallback (ниже).

    # ── Gate.io fallback ─────────────────────────────────────────
    gate_price = gate_get_price(ticker_name)
    if not gate_price:
        # FIX 2026-06-24: на Bybit-reject (30228 etc) + пустой Gate → fail-fast
        # sentinel (-1,0). Без этого воркер на (0,0) делает 3×0.1с retry'я
        # бессмысленно (причина перманентная).
        if bybit_rejected:
            print(f"[NO PRICE ANYWHERE] {ticker_name} — Bybit reject + Gate пустой "
                  f"— fail-fast", flush=True)
            return -1, 0
        print(f"[NO PRICE ANYWHERE] {ticker_name}")
        return 0, 0

    print(f"[GATE-FALLBACK] {ticker_name}: открываем шорт на Gate "
          f"(price={gate_price})", flush=True)
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


def _sim_atr_live_trail_params_short(ticker: str, base_px: float) -> tuple[float, float, float, str]:
    """
    ЖИВОЙ порт tg/exit_strategies.py:exit_atr_trailing для SHORT-позиции.

    Блокирует ~DELIST_SIM_ATR_PERIOD секунд (default 30) sampling'ом
    price_cache через get_price — тот же источник что recorder в _atr_frac.

    После warmup'а: trail_frac = clamp(k×atr_frac, lo, hi), act = trail,
    SL = _SIM_ATR_SL. Для SHORT: активация НИЖЕ base, SL ВЫШЕ base.
    """
    sample_res = live_sample_atr_frac(
        get_price,
        coin=ticker,
        entry=base_px,
        samples=DELIST_SIM_ATR_PERIOD,
        period=1.0,
        min_valid=DELIST_SIM_ATR_MIN_CANDLES,
    )
    if sample_res is not None:
        atr_frac, n_used, peak, trough = sample_res
        raw = DELIST_SIM_ATR_K * atr_frac
        trail_frac = min(DELIST_SIM_ATR_HI, max(DELIST_SIM_ATR_LO, raw))
        atr_tag = (f"atr_frac={atr_frac:.5f} (n={n_used}, "
                   f"peak={peak:.6g}, trough={trough:.6g})")
    else:
        trail_frac = DELIST_SIM_ATR_FALLBACK
        atr_tag = f"sample=n/a → fallback={DELIST_SIM_ATR_FALLBACK*100:.1f}%"

    trail_dist  = round(base_px * trail_frac, 8)
    active_pric = round(base_px * (1.0 - trail_frac), 8)   # SHORT act ниже
    sl_price    = round(base_px * (1.0 + DELIST_SIM_ATR_SL), 8)  # SHORT SL выше
    tag = (f"sim_atr LIVE {atr_tag} "
           f"({DELIST_SIM_ATR_PERIOD}×1s post-fill) "
           f"trail={trail_frac*100:.2f}% act={trail_frac*100:.2f}% "
           f"SL={DELIST_SIM_ATR_SL*100:.1f}%")
    return trail_dist, active_pric, sl_price, tag


def _compute_trail_params_short(symbol: str, base_px: float) -> tuple[float, float, float, str]:
    """
    Возвращает (trailing_distance, active_price, sl_price, log_tag) для ШОРТ.

    Режимы (DELIST_TRAIL_MODE):
      - "sim_atr": порт симуляторного exit_atr_trailing (тот "atr_trail" из
                   6ч-карточек, +56% на IOTX). atr_frac=mean(|Δclose|)/entry,
                   trail=k×atr clamped[lo,hi], act=trail, SL=1%.
      - "atr":     Wilder True Range с clamp'ом в %.
      - "pct":     фикс % (legacy).

    Для шорта: активация НИЖЕ base_px, SL ВЫШЕ base_px.
    """
    # NB: mode "sim_atr" НЕ здесь — sim_atr требует post-fill live sampling
    # (см. _sim_atr_live_trail_params_short, вызывается напрямую из
    # _set_tp_sl_bybit_short).

    if DELIST_TRAIL_MODE == "atr" and base_px > 0:
        atr = compute_atr(
            symbol,
            interval=DELIST_ATR_INTERVAL,
            period=DELIST_ATR_PERIOD,
            min_candles=DELIST_ATR_MIN_CANDLES,
        )
        if atr is not None and atr > 0:
            td = clamp_distance(atr * DELIST_ATR_TRAIL_MULT, base_px,
                                DELIST_ATR_TRAIL_MIN_PCT, DELIST_ATR_TRAIL_MAX_PCT)
            ad = clamp_distance(atr * DELIST_ATR_ACT_MULT,   base_px,
                                DELIST_ATR_ACT_MIN_PCT,   DELIST_ATR_ACT_MAX_PCT)
            sd = clamp_distance(atr * DELIST_ATR_SL_MULT,    base_px,
                                DELIST_ATR_SL_MIN_PCT,    DELIST_ATR_SL_MAX_PCT)
            trail_dist  = round(td, 8)
            active_pric = round(base_px - ad, 8)
            sl_price    = round(base_px + sd, 8)
            tag = (f"ATR={atr:.6g} ({DELIST_ATR_PERIOD}×{DELIST_ATR_INTERVAL}m) "
                   f"trail={td/base_px*100:.2f}% SL={sd/base_px*100:.2f}%")
            return trail_dist, active_pric, sl_price, tag

    # Fallback / pct-mode — legacy формулы.
    trail_dist  = round(base_px * DELIST_TRAILING_PCT, 8)
    active_pric = round(base_px * (1 - DELIST_ACTIVE_PCT), 8)
    sl_price    = round(base_px * (1 + DELIST_SL_PCT),     8)
    tag = f"pct trail={DELIST_TRAILING_PCT*100:.1f}% SL={DELIST_SL_PCT*100:.1f}%"
    return trail_dist, active_pric, sl_price, tag


def _set_tp_sl_bybit_short(ticker_name: str, entry_price: float, amount: float) -> str:
    """
    Выставляет нативный trailing stop + аварийный SL для шорта на Bybit.
    Значения из config (FIX 2026-06-04): trailing 0.5%, SL 1%, активация -0.5%.

    Стратегия «первая быстрая свеча»:
      - trailingStop DELIST_TRAILING_PCT (тугой) — фиксируем максимум с пика дампа
      - activePrice = entry × (1 - DELIST_ACTIVE_PCT) — активация после движения вниз
      - аварийный SL = entry × (1 + DELIST_SL_PCT) — защита если цена пошла против
      - БЕЗ фиксированных TP — чистый трейлинг на всю позицию (Full-режим)
    """
    symbol = f"{ticker_name}USDT"
    try:
        step = _get_qty_step(symbol)
    except QtyStepUnavailable as e:
        print(f"[TP/SL SKIP] {e}")
        return "skip"

    sl_size = str(_round_qty(amount, step))  # Вся позиция
    if sl_size == "0.0" or float(sl_size) <= 0:
        print(f"[TP/SL SKIP] {ticker_name}: slSize={sl_size} (amount={amount}, step={step}) — слишком мало")
        return "skip"

    # 1) Ждём появления позиции и берём РЕАЛЬНУЮ avgPrice. Bundled SL из order.create
    #    уже защищает шорт, пока цепляем трейлинг. Нет позиции за окно → закрылась
    #    стопом / не исполнилась (НЕ голая, трейлить нечего).
    # FIX 2026-06-19 (R3): сначала private-WS push (10-30мс); REST poll — fallback.
    avg = 0.0
    size = 0.0
    pos_deadline = time.monotonic() + _DELIST_POS_WAIT_RETRIES * _DELIST_POS_WAIT_SLEEP
    ws_priv_ready = _ws_private is not None and _ws_private.is_ready()
    if ws_priv_ready:
        ws_timeout = max(0.0, pos_deadline - time.monotonic())
        ws_res = _ws_private.wait_for_position(symbol, 2, timeout=ws_timeout)
        if ws_res is not None:
            size, avg, _trailing = ws_res
    if size <= 0:
        # REST poll с общим бюджетом pos_deadline (БЕЗ удвоения окна).
        # Гарантируем 1 last-check (мог прийти ack между WS-таймаутом и сейчас).
        while True:
            size, avg, _trailing = get_position(symbol, 2)
            if size > 0:
                break
            remaining = pos_deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(_DELIST_POS_WAIT_SLEEP, remaining))
    if size <= 0:
        print(f"[TP/SL] {ticker_name}: позиции нет — закрыта стопом или не исполнилась "
              f"(bundled SL отработал; трейлить нечего)", flush=True)
        return "no-position"

    # 2) Якорим к РЕАЛЬНОЙ цене входа. Для short: SL ВЫШЕ входа, активация НИЖЕ.
    base_px = avg if avg > 0 else entry_price

    # FIX 2026-07-07: sim_atr — блокирующий 30-сек warmup + live sample атра.
    # Позиция защищена bundled SL из order.create (см. market_open_short).
    if DELIST_TRAIL_MODE == "sim_atr":
        print(f"[TP/SL SIM_ATR] {ticker_name}: warmup {DELIST_SIM_ATR_PERIOD}с "
              f"(bundled SL держит)...", flush=True)
        trailing_distance, active_price, sl_price, trail_tag = \
            _sim_atr_live_trail_params_short(ticker_name, base_px)
        # Позиция могла закрыться bundled SL за warmup.
        size, _, _ = get_position(symbol, 2)
        if size <= 0:
            print(f"[TP/SL] {ticker_name}: позиция закрылась за sim_atr warmup "
                  f"(bundled SL сработал) — трейлить нечего", flush=True)
            return "closed"
    else:
        trailing_distance, active_price, sl_price, trail_tag = _compute_trail_params_short(symbol, base_px)

    base = {
        "category":    "linear", "symbol": symbol, "positionIdx": 2,
        "stopLoss":    str(sl_price), "slTriggerBy": "LastPrice", "slSize": sl_size,
    }

    # 3) Ставим трейлинг с ВЕРИФИКАЦИЕЙ (перечитываем позицию — реально ли сел трейлинг),
    #    ретраим пока позиция жива. past_activation пересчитываем каждый раз: делистнутая
    #    монета падает, и activePrice могла стать невалидной (Sell требует activePrice<last).
    for attempt in range(1, _DELIST_TRAIL_VERIFY_RETRIES + 1):
        last = get_price(ticker_name)
        past_activation = last is not None and last <= active_price
        trail = {"trailingStop": str(trailing_distance)}
        if not past_activation:
            trail["activePrice"] = str(active_price)
        try:
            _trading_stop_settle({**base, **trail}, tag=ticker_name)
        except Exception as e_comb:  # noqa: BLE001
            # SL переякорить не вышло — хотя бы трейлинг (bundled SL из order.create остаётся)
            try:
                _trading_stop_settle({"category": "linear", "symbol": symbol,
                                      "positionIdx": 2,
                                      "trailingStop": str(trailing_distance)},
                                     tag=f"{ticker_name}-TS")
            except Exception as e_ts:  # noqa: BLE001
                print(f"[TP/SL] {ticker_name} попытка {attempt}: "
                      f"комбо={e_comb}; trailing-only={e_ts}", flush=True)

        # FIX 2026-06-19 (R3): WS-cache first (~push 10-50мс) → REST fallback.
        # trailing_on — bool: либо WS показал trailing>0, либо REST вернул True.
        size_w = 0.0
        trail_w = 0.0
        if ws_priv_ready:
            snap = _ws_private.get_position_cached(symbol, 2)
            if snap is not None:
                size_w, _, trail_w = snap
        if size_w > 0 and trail_w > 0:
            size, trailing_on = size_w, True
        else:
            size, _, trailing_on = get_position(symbol, 2)
        if size <= 0:
            print(f"[TP/SL] {ticker_name}: позиция закрылась пока ставили трейлинг "
                  f"(стоп/трейлинг отработал) — ок", flush=True)
            return "closed"
        if trailing_on:
            act_txt = "немедленно (цена прошла активацию)" if past_activation else f"@{active_price:.6f}"
            print(f"[TP/SL SET SHORT] {ticker_name} | entry={base_px:.6f}(реал) | "
                  f"SL={sl_price:.6f} | Trailing={trailing_distance} "
                  f"(active {act_txt}) | {trail_tag} — ПОДТВЕРЖДЁН на бирже",
                  flush=True)
            return "ok"
        if attempt < _DELIST_TRAIL_VERIFY_RETRIES:
            if ws_priv_ready:
                # wait_for_position_trailing уже спит timeout=sleep'а; либо
                # пришёл push (continue), либо истёк — оба варианта = задержка
                # покрыта, второй sleep не нужен.
                _ws_private.wait_for_position_trailing(
                    symbol, 2, timeout=_DELIST_TRAIL_VERIFY_SLEEP,
                )
            else:
                time.sleep(_DELIST_TRAIL_VERIFY_SLEEP)

    # Позиция жива, трейлинг не подтвердился. Bundled SL защищает — громкий warn.
    print(f"[TP/SL WARN] {ticker_name}: трейлинг НЕ подтверждён за "
          f"{_DELIST_TRAIL_VERIFY_RETRIES} попыток, позиция ЖИВА и защищена bundled SL. "
          f"ПРОВЕРЬ ТРЕЙЛИНГ!", flush=True)
    return "sl-only"


def set_tp_sl(ticker_name: str, entry_price: float, amount: float) -> str:
    """
    Роутер — выставляет TP/SL на той бирже где открыт шорт.

    FIX 2026-06-06: обёрнуто в try/except (как listing.set_tp_sl_long). Раньше
    исключение из _set_tp_sl_bybit_short (напр. trading-stop не сел) МОЛЧА
    гибло в _tp_sl_executor-future → позиция оставалась БЕЗ трейлинга/SL, и
    никто не знал. Теперь — громкий лог.
    """
    try:
        with _delist_exchange_lock:
            exchange = _delist_exchange.get(ticker_name, "bybit")
        if exchange == "gate":
            return gate_set_tp_sl_short(ticker_name, entry_price, amount)
        return _set_tp_sl_bybit_short(ticker_name, entry_price, amount)
    except Exception as e:  # noqa: BLE001
        print(
            f"[TP/SL FAIL] {ticker_name}: {e} — трейлинг/SL НЕ выставлен! "
            f"ПРОВЕРЬ ПОЗИЦИЮ!", flush=True,
        )
        return "error"


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
# FIX 2026-06-06: вырезаем "link"-контекст (Source link / link:) перед жадным
# fallback — слово "link" → LINK (реальный символ) выдиралось как тикер.
_RE_LINK_CONTEXT = re.compile(r"\b(?:SOURCE\s+)?LINK\b\s*:?", re.IGNORECASE)


def _extract_from_segment(segment: str) -> list[str]:
    """
    Извлекает тикеры из захваченного делист-СЕГМЕНТА (то, что идёт после
    «Will Delist»/«delisting of»…). Ловит и полные пары (ABCUSDT→ABC), и голые
    тикеры (XNO, IQ). Всё фильтруется по реально торгуемым (Bybit ∪ Gate) —
    отсекает английские слова из текста.
    """
    pairs = _RE_USDT.findall(segment)        # ABCUSDT / ABC-USDT → ABC
    bare = _RE_PAIR_TOKENS.findall(segment)  # XNO, IQ, QUICK, DGB
    return _filter_known(pairs + bare)


def find_pairs(text: str) -> list[str]:
    """
    Извлекает тикеры монет из текста статьи о делистинге.
    Приоритет (FIX 2026-06-08): точный делист-список Binance — источник истины.
      1. «Will Delist A, B, C on …» / monitoring-tag — ТЕРМИНАЛЬНО: тикеры идут
         сразу за «Will Delist», берём ТОЛЬКО их (даже если пусто — значит ни
         одна делистнутая монета у нас не торгуется, шортить нечего). Это убивает
         класс ошибок «промо-пара из body (FORMUSDT) перебила реальный делист» —
         инцидент: "Will Delist XNO,IQ,QUICK,DGB on" → шорт по FORM из тела.
      2. Иначе → явные пары (ABCUSDT) → $TICKER → широкий delist-блок → fallback.
    """
    text_upper = text.upper()

    # ── 1. Точные делист-списки (тикеры сразу за словами) — терминально ─
    for rx in (_RE_WILL_DELIST, _RE_MONITORING):
        m = rx.search(text_upper)
        if m:
            return _extract_from_segment(m.group(1))

    # ── 2. Явные USDT-пары (формат "FOOUSDT delisted") ─────────────
    usdt_pairs = _RE_USDT.findall(text_upper)
    if usdt_pairs:
        found = _filter_tokens(usdt_pairs)
        if found:
            return found

    # FIX-batch-7: $TICKER маркеры ("Monitoring Tag Added – $ALCX…").
    dollar_pairs = [t.upper() for t in _RE_DOLLAR_TKN.findall(text)]
    if dollar_pairs:
        found = _filter_tokens(dollar_pairs)
        if found:
            return found

    # ── 3. Широкий delist-блок (loose) — только торгуемые ──────────
    m = _RE_DELIST_BLOCK.search(text_upper)
    if m:
        found = _extract_from_segment(m.group(1))
        if found:
            return found

    # ── 4. Аккуратный fallback по всему тексту (только торгуемые) ───
    # FIX: вырезаем "source link" контекст — иначе LINK выдирается из "link".
    cleaned = _RE_LINK_CONTEXT.sub(" ", text_upper)
    all_tokens = _RE_ALL_TOKENS.findall(cleaned)
    known = _known_union()
    return [
        t for t in _filter_tokens(all_tokens, allow_ambiguous=False)
        if t in known
    ]


def _filter_tokens(tokens: list[str], allow_ambiguous: bool = True) -> list[str]:
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
        if t in AMBIGUOUS_TOKENS:
            if not allow_ambiguous or t not in _known_union():
                continue
        if t in seen:
            continue
        seen.add(t)
        result.append(t)
    return result


def _known_union() -> frozenset:
    """Монеты, торгуемые на Bybit ИЛИ Gate (где можем реально открыть шорт).
    Bybit-only known_coins пропускал Gate-делисты (XNO) → их не шортили."""
    with cache_lock:
        kb = frozenset(known_coins)
    return kb | gate_known_snapshot()


def _filter_known(tokens: list[str]) -> list[str]:
    """
    FIX 2026-06-06: как _filter_tokens, но дополнительно требует присутствия
    среди реально торгуемых символов (Bybit ∪ Gate). Для ПРОЗОВЫХ методов
    извлечения (_RE_WILL_DELIST/_RE_MONITORING/_RE_DELIST_BLOCK), где захват
    может содержать английские слова из текста статьи. Инцидент: body
    "...delist... our priority ensure best while continuing adapt evolving
    market dynamics" → шорты по 10 словам-мусору. Реальные делист-тикеры всегда
    торгуются (делистят то что есть на бирже) → отсекаем мусор.
    """
    base = _filter_tokens(tokens)
    if not base:
        return []
    known = _known_union()
    return [t for t in base if t in known]


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
    # FIX 2026-06-10: HTTP/2-клиент для TP/SL/trailing (_post_http2) создавался
    # лениво на ПЕРВОЙ постановке стопа → первый листинг платил cold-init httpx
    # + TCP+TLS+H2 handshake (видно как 64мс vs 9мс). Прогреваем здесь: создаём
    # клиент и открываем TLS публичным GET (без подписи). Best-effort.
    try:
        _client = _get_httpx_client()
        if _client is not None:
            _client.get(BYBIT_BASE_URL + "/v5/market/time", timeout=3)
            print("[WARMUP] Bybit HTTP/2-клиент прогрет (TP/SL путь)")
    except Exception as e:  # noqa: BLE001
        print(f"[WARMUP HTTP2] {e}")


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