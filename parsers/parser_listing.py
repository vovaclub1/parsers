from __future__ import annotations

# ── parser_listing.py ─────────────────────────────────────────────
# Источники сигналов:
#   ANNOUNCEMENT  — TG (coin_listing + extras), TOA WS, CoinListing WS
#   DIRECT POLL   — Upbit (100мс), Bithumb (100мс), Binance futures (~500мс)
#
# Дедуп — две полки, обе без TTL для L2:
#   L1 (in-memory TTL 60с) — отсекает шум от множества источников
#   L2 (persistent JSON)   — глобальная память: «эту монету уже отстреливали».
#                            ANNOUNCEMENT → coin помечается ГЛОБАЛЬНО (любая
#                            биржа). DIRECT POLL → (coin, exchange) — мьется
#                            только эта связка.
#
# При сигнале: market_open_long → set_tp_sl_long (в фоне)
# ─────────────────────────────────────────────────────────────────

import asyncio
import atexit
import os
import random
import re
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from telethon import TelegramClient, events

# FIX-batch-1: orjson в хот path Upbit/Bithumb (3-5x быстрее стандартного json).
try:
    import orjson as _orjson  # type: ignore[import-not-found]
    def _json_loads(b: bytes | str):
        if isinstance(b, str):
            b = b.encode()
        return _orjson.loads(b)
    def _json_dumps(obj, indent: bool = False):
        opts = _orjson.OPT_INDENT_2 if indent else 0
        return _orjson.dumps(obj, option=opts)
except ImportError:
    import json as _stdjson
    def _json_loads(b: bytes | str):
        if isinstance(b, (bytes, bytearray)):
            b = b.decode()
        return _stdjson.loads(b)
    def _json_dumps(obj, indent: bool = False):
        if indent:
            return _stdjson.dumps(obj, indent=2).encode()
        return _stdjson.dumps(obj).encode()

# FIX-PERF: msgspec.Struct для Binance exchangeInfo (300-500KB JSON,
# ~600+ symbols). orjson+dict.get в for-loop держал GIL ~10-25мс каждые
# 2с — окно где worker мог оказаться зажат. msgspec парсит на C-уровне и
# доступ через атрибут (.status vs ["status"]) — ~40% быстрее total.
try:
    import msgspec as _msgspec_l  # type: ignore[import-not-found]

    class _BinanceSym(_msgspec_l.Struct, frozen=True):
        symbol:       str = ""
        status:       str = ""
        contractType: str = ""
        baseAsset:    str = ""
        quoteAsset:   str = ""

    class _BinanceExInfo(_msgspec_l.Struct, frozen=True):
        symbols: list[_BinanceSym] = []  # noqa: RUF012

    _binance_exinfo_decoder = _msgspec_l.json.Decoder(_BinanceExInfo)

    def _parse_binance_exinfo(raw: bytes) -> list[_BinanceSym]:
        return _binance_exinfo_decoder.decode(raw).symbols
except ImportError:
    _parse_binance_exinfo = None  # type: ignore[assignment]

from tg.tg_logger import tg_log
from api.coinlisting_ws import run_coinlisting
from config.config import (
    TG_API_ID, TG_API_HASH, SESSION_DIR, STATE_DIR,
    EXTRA_LISTING_CHANNELS, parse_channels,    # FIX-batch-3: multi-channel
    TREE_OF_ALPHA_WS_ENABLED,                   # FIX-batch-4: TOA WS
    BYBIT_WS_TRADE_ENABLED,                     # FIX-batch-5: Bybit WS Trade
    BYBIT_SYNC_WS_ENABLED,                      # FIX-PERF: sync WS hot-path
    BYBIT_API_KEY, BYBIT_SECRET_KEY,
    LISTING_PROXIES,                            # FIX-LATENCY (Patch #2): proxy pool
)
import itertools  # FIX-LATENCY (Patch #2): round-robin для proxy pool

from api.listing_api import (
    find_listing_pairs,
    calculate_margin_for_listing,
    price_updater,
    gate_price_updater,
    warmup_bybit_connection,
    start_bybit_heartbeat,
    preload_lot_steps,
    gate_preload_lot_steps,
    warmup_gate_connection,
)
# FIX 2026-07-07 (INVERT): по решению — на ЛИСТИНГАХ открываем ШОРТ
# (fade the pump), на делистах — лонг. Шорт-механика (Bybit+Gate fallback,
# bundled SL выше входа, трейлинг DELIST_*) переиспользуется из delist_api.
from api.delist_api import (
    market_open_short,
    set_tp_sl as set_tp_sl_short,
)
# FIX-LATENCY: pre-set leverage sweep — убирает 150мс HTTP POST из
# market_open_long на cold-Gate-листинге.
from api.gate_api import gate_leverage_presetter
from collections import Counter  # source-first телеметрия

# ── цвета ─────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# ── конфиг ────────────────────────────────────────────────────────
TG_CHANNEL  = "-1001124574831"

# FIX-batch-6/7: реальные форматы из BWEnews, binance_announcements, coin_listing,
# listing_binance_mids, cryptolistingwebsocket. Подобраны под примеры юзера.
TG_LISTING_PHRASES = [
    # Английские (Binance / international)
    "will list", "will add", "will launch", "will open trading",
    "listing notice", "listing announcement", "new listing",
    "added to spot", "added to binance", "new market",
    "will add to spot", "will be listed",
    # FIX-batch-7: структурированные форматы
    # listing_binance_mids: "✅ BINANCE | Listing"
    # cryptolistingwebsocket: "BINANCE Listing Announcement"
    # FIX 2026-06-05: "bybit listing" убран из позитивных — Bybit-листинги
    # блокируются (см. TG_LISTING_NEG). Мы открываем позицию НА Bybit, листинг
    # там не даёт пампа/асимметрии (токен уже торгуется).
    "| listing", "binance listing",
    # Корейские биржи (Upbit / Bithumb)
    "listed on upbit", "listed on bithumb",
    "listed on binance",
    "upbit will list", "bithumb will list",
    "upbit listing", "bithumb listing",
    "news listing",  # listing_binance_mids: "News listing: UP2/USDT"
    # Русские
    "новый листинг", "будет лист", "добавит",
]
# Если в тексте есть что-то из этого — это НЕ листинг, отсечь.
TG_LISTING_NEG = [
    "delist", "delisted", "delisting", "delists",
    "will be removed", "will remove", "removal of", "removed from",
    "monitoring tag", "extend the monitoring",
    "postponed", "delay", "delayed", "cancelled", "canceled",
    "alpha will remove", "from the featuring list",
    "hodler airdrop",  # FIX-batch-7: Binance HODLer Airdrop — не листинг
    # FIX: Earn / Launchpool / promo — инцидент 2026-05-28 04:00:03
    # ('Binance Earn New Listing Special Offer: ...') открыл фейковые
    # лонги GENIUS/OPG/APR. Тот же фильтр в treeofalpha_ws.LISTING_NEG.
    "earn", "locked product", "locked products",
    "flexible product", "flexible products",
    "simple earn",
    "subscribe to", "subscribe and",
    "special offer",
    "launchpool",
    "% apr", "apr for",
    "% apy", "apy for",
    "promotion", "promotional",
    "rewards pool", "staking pool",
    # FIX 2026-06-02: Pre-IPO / TradFi синтетика. Это бессрочные фьючерсы
    # на акции компаний до IPO (Anthropic, OpenAI и т.п.) — наша стратегия
    # на этом не работает, цена не зависит от Korean-листинга. Инцидент:
    # @coin_listing прислал "Binance Futures Will Launch $ANTHROPIC ...
    # Perpetual Contract Pre-IPO Trading" — TG_LISTING_NEG не блочил,
    # хотя _BINANCE_LISTING_NEG (для notice-поллера) уже содержит pre-ipo.
    "pre-ipo", "pre ipo",
    "tradfi",
    "perpetual contract pre",   # формат Binance "... Perpetual Contract Pre-IPO"
    "multiple usd",              # bulk-add нескольких TradFi/Pre-IPO за раз
    "multiple usdⓈ",            # тот же с символом Ⓢ как у Binance
    "делист", "делисты",
    # FIX 2026-06-05: Bybit-листинги — НЕ торгуем. Инцидент ZEST:
    # "[BYBIT] $ZEST listed on Bybit Futures" открыл лонг, хотя это
    # добавление фьючерсной пары на уже торгуемый токен (нет пампа).
    # Мы открываем позицию НА Bybit — листинг там не создаёт асимметрии.
    "listed on bybit", "bybit futures", "bybit listing",
    "[bybit]", "on bybit futures",
]

# FIX-perf: компилируем фильтры в regex-альтернацию ОДИН раз из списков выше
# (списки остаются единственным источником правды). re.search по DFA вместо
# N×substring-сканов в hot-path TG-handler'а: ~100-200µs → ~20-40µs/сообщение.
_TG_LISTING_POS_RE = re.compile("|".join(re.escape(p) for p in TG_LISTING_PHRASES))
_TG_LISTING_NEG_RE = re.compile("|".join(re.escape(p) for p in TG_LISTING_NEG))

UPBIT_MARKETS_URL    = "https://api.upbit.com/v1/market/all"
BITHUMB_ASSETS_URL   = "https://api.bithumb.com/public/assetsstatus/all"
# Binance futures list — все linear-инструменты (USDT, USDC, BUSD, COIN-M unused).
BINANCE_FAPI_URL     = "https://fapi.binance.com/fapi/v1/exchangeInfo"

# FIX-batch-8: интервал опроса 300мс → 100мс.
# Upbit rate limit = 10 req/sec на IP — 100мс лимит выдерживает с запасом.
# Win: −200мс медианной детекции Korean листингов.
# Хардкод (не env) — это публичный лимит биржи, не персональные данные.
POLL_INTERVAL       = 0.1
# Binance futures — публичный rate-limit 2400 req/min / IP.
# FIX-PERF: было 500мс — приводило к GIL contention в hot-path трейда.
# exchangeInfo = 300-500KB JSON, msgspec decode держит GIL ~5-15мс.
# Если торговый сигнал прилетал в окно decode — терял +5-15мс на GIL acquire.
# Зафиксировано: первый трейд после регрессии = 31мс (норма 7-11мс).
# 2500мс = 24 req/min, decode-окно открыто ~1% времени вместо ~5% при 500мс.
# Цена: median детекция Binance листингов хуже на ~1с — приемлемо
# (Binance анонсирует листинги, мы и через TG-канал поймаем быстрее).
BINANCE_POLL_INTERVAL = 2.5
# При ошибке/429 — отдельный (более длинный) sleep, чтобы не флудить.
POLL_ERROR_BACKOFF  = 3.0

WATCHDOG_TIMEOUT    = 60   # секунд

# FIX 2026-07-07: тротлинг повторяющихся ошибок поллеров. Мёртвый прокси
# давал "poll error: ProxyError..." каждые 2-3с СУТКАМИ — лог нечитаем.
# Одинаковая (tag, сигнатура) ошибка теперь логируется раз в 5 минут
# со счётчиком подавленных повторов.
_POLL_ERR_LOG_INTERVAL = 300.0
_poll_err_last: dict[tuple[str, str], tuple[float, int]] = {}   # (tag, sig) → (last_log_ts, suppressed)
_poll_err_lock = threading.Lock()


def _log_poll_error_throttled(tag: str, e: Exception) -> None:
    """log_err с подавлением повторов: одна и та же ошибка — раз в 5 мин."""
    # Сигнатура без изменчивых частей (портов/адресов достаточно — они
    # в тексте, но одинаковы для одного и того же мёртвого прокси).
    sig = f"{type(e).__name__}:{str(e)[:120]}"
    now = time.monotonic()
    with _poll_err_lock:
        last_ts, suppressed = _poll_err_last.get((tag, sig), (0.0, 0))
        if now - last_ts < _POLL_ERR_LOG_INTERVAL:
            _poll_err_last[(tag, sig)] = (last_ts, suppressed + 1)
            return
        _poll_err_last[(tag, sig)] = (now, 0)
    extra = f" (+{suppressed} подавлено за 5мин)" if suppressed else ""
    log_err(tag, f"poll error: {type(e).__name__}: {e}{extra}")


# ── Proxy pool для notice-поллеров (Patch #2) ─────────────────────
# LISTING_PROXIES в .env, формат через запятую:
#   "user:pass@host:port,host:port:user:pass,..." — поддерживаем все
#   популярные форматы Webshare / IPRoyal / прочих провайдеров.
# Цель: с 3+ IP каждый видит ≤10 req/s даже при общей частоте 30 req/s,
# что позволяет уплотнить BITHUMB_NOTICE_POLL_INTERVAL до 30-40мс без
# 429/ban'ов от биржи.
def _parse_listing_proxies(raw: str) -> list[str | None]:
    """
    Возвращает [None, "{scheme}://user:pass@host:port", ...].
    Поддерживает форматы:
      http://user:pass@host:port      / https:// / socks5:// / socks5h://
      user:pass@host:port             — без схемы → http://
      host:port:user:pass             — Webshare/IPRoyal стандарт
      user:pass:host:port             — некоторые провайдеры
      host:port                       — без auth
    Схема сохраняется из входа. Без схемы — http.
    ВАЖНО: для socks5:// в `requests` нужен PySocks
    (`pip install pysocks` или `requests[socks]`).
    """
    proxies: list[str | None] = [None]
    for p in (raw or "").split(","):
        p = p.strip()
        if not p:
            continue

        # Извлечь схему
        scheme = "http"
        rest = p
        if "://" in rest:
            scheme, rest = rest.split("://", 1)

        # user:pass@host:port → готово
        if "@" in rest:
            proxies.append(f"{scheme}://{rest}")
            continue

        parts = rest.split(":")
        if len(parts) == 4:
            if parts[1].isdigit():
                host, port, user, pwd = parts
            elif parts[3].isdigit():
                user, pwd, host, port = parts
            else:
                print(f"[PROXY WARN] LISTING_PROXIES непонятный формат: {p[:40]}", flush=True)
                continue
            proxies.append(f"{scheme}://{user}:{pwd}@{host}:{port}")
            continue

        if len(parts) == 2 and parts[1].isdigit():
            proxies.append(f"{scheme}://{rest}")
            continue

        print(f"[PROXY WARN] LISTING_PROXIES непонятный формат: {p[:40]}", flush=True)
    return proxies


# FIX-LATENCY: keep-alive adapter с TCP_NODELAY (отключаем Nagle — мелкие
# GET'ы уходят без буферизации) + увеличенный pool, чтобы round-robin сессии
# держали тёплые TCP+TLS соединения и не переустанавливали их.
def _mount_keepalive_adapter(s: requests.Session) -> None:
    try:
        from requests.adapters import HTTPAdapter
        from urllib3.connection import HTTPConnection
        # TCP_NODELAY на все новые urllib3-сокеты (глобально, идемпотентно).
        import socket as _sock
        _opts = list(getattr(HTTPConnection, "default_socket_options", []) or [])
        if (_sock.IPPROTO_TCP, _sock.TCP_NODELAY, 1) not in _opts:
            _opts.append((_sock.IPPROTO_TCP, _sock.TCP_NODELAY, 1))
            HTTPConnection.default_socket_options = _opts
        adapter = HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=0)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
    except Exception as e:  # noqa: BLE001
        print(f"[ADAPTER WARN] keep-alive adapter не смонтирован: {e!r}", flush=True)


def _make_listing_session(proxy: str | None) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "User-Agent": "Mozilla/5.0"})
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    _mount_keepalive_adapter(s)
    return s


# FIX-LATENCY: отдельный пул для Binance — нужны браузер-подобные заголовки
# (UA Chrome, clientType=web, Referer) которые отличаются от голого
# "Mozilla/5.0" для Bithumb/Upbit notice. WAF /bapi/composite очень
# чувствителен к UA, поэтому одна сессия с другими заголовками может
# словить 429 чаще на тех же прокси.
def _make_binance_session(proxy: str | None) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "clientType": "web",
        "lang": "en",
        "Referer": "https://www.binance.com/en/support/announcement/list/48",
    })
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    _mount_keepalive_adapter(s)
    return s


_LISTING_PROXY_POOL: list[requests.Session] = []
_BINANCE_PROXY_POOL: list[requests.Session] = []
_listing_session_cycle: "itertools.cycle | None" = None
_binance_session_cycle: "itertools.cycle | None" = None
_LISTING_PROXY_POOL_LOCK = threading.Lock()


def _init_listing_proxy_pool() -> int:
    """Создаёт пулы сессий: один для Bithumb/Upbit notice, второй для
    Binance (свои заголовки). Возвращает размер пула. Идемпотентно."""
    global _listing_session_cycle, _binance_session_cycle
    with _LISTING_PROXY_POOL_LOCK:
        proxies = _parse_listing_proxies(LISTING_PROXIES)
        _LISTING_PROXY_POOL.clear()
        _BINANCE_PROXY_POOL.clear()
        for p in proxies:
            _LISTING_PROXY_POOL.append(_make_listing_session(p))
            _BINANCE_PROXY_POOL.append(_make_binance_session(p))
        _listing_session_cycle = itertools.cycle(_LISTING_PROXY_POOL)
        _binance_session_cycle = itertools.cycle(_BINANCE_PROXY_POOL)
        return len(_LISTING_PROXY_POOL)


def _next_listing_session() -> requests.Session:
    """Round-robin сессия из пула. Лениво инициализируется при первом
    вызове, если main не успел стартовать пул."""
    global _listing_session_cycle
    if _listing_session_cycle is None:
        _init_listing_proxy_pool()
    return next(_listing_session_cycle)  # type: ignore[arg-type]


def _next_binance_session() -> requests.Session:
    """Round-robin сессия Binance (с браузер-headers). Lazy init."""
    global _binance_session_cycle
    if _binance_session_cycle is None:
        _init_listing_proxy_pool()
    return next(_binance_session_cycle)  # type: ignore[arg-type]


def _warmup_listing_sessions() -> None:
    """
    FIX-LATENCY: прогревает TCP+TLS для всех notice-сессий ко всем трём
    endpoint'ам параллельно. Убирает cold-TLS handshake (~300-500мс до
    Сеула) с ПЕРВОГО боевого запроса. Best-effort — ошибки игнорим.
    Binance прогреваем мягко (1 запрос на сессию) чтоб не словить 429.
    """
    import concurrent.futures as _cf

    targets: list[tuple[requests.Session, str]] = []
    # Upbit/Bithumb notice — каждую listing-сессию к обоим хостам.
    for s in _LISTING_PROXY_POOL:
        targets.append((s, "https://api-manager.upbit.com/api/v1/announcements?page=1&per_page=1"))
        targets.append((s, BITHUMB_NOTICES_URL))
    # Binance — отдельные сессии, по одному прогреву (WAF чувствителен).
    for s in _BINANCE_PROXY_POOL:
        targets.append((s, _binance_notice_url()))

    if not targets:
        return

    def _ping(item: tuple[requests.Session, str]) -> bool:
        sess, url = item
        try:
            sess.get(url, timeout=4)
            return True
        except Exception:  # noqa: BLE001
            return False

    ok = 0
    with _cf.ThreadPoolExecutor(max_workers=min(16, len(targets))) as ex:
        try:
            # FIX (review L0): timeout на map — прогрев не должен блокировать
            # boot если один прокси завис (хотя _ping и так timeout=4).
            for r in ex.map(_ping, targets, timeout=8):
                ok += 1 if r else 0
        except _cf.TimeoutError:
            log_warn("WARMUP", "часть notice-сессий не прогрелась за 8с — продолжаем")
    log_ok("WARMUP", f"notice-сессии прогреты: {ok}/{len(targets)} (TCP+TLS установлены)")


# ── Upbit throttle circuit-breaker (FIX throttle-storm 2026-06-04) ─
# Инцидент №1: Upbit-поллер уходил в бесконечный throttle-loop — CF 1015/429
# на каждом запросе, ротация IP не помогала (WAF банил весь пул).
# Инцидент №2 (этот фикс): consecutive-streak счётчик НЕ срабатывал — с 13
# round-robin прокси один незабаненный IP изредка отдавал 200 OK, success
# сбрасывал streak в 0, до cooldown-порога (10 подряд) дело не доходило
# НИКОГДА. Поллер вечно долбил каждые ~15с фиксированным backoff'ом, держа
# CF-бан тёплым.
#
# Решение: sliding-window circuit breaker вместо consecutive-streak.
#   - храним последние N исходов (throttle/success) в deque
#   - если throttle-rate за окно ≥ порога (редкие успехи НЕ сбрасывают) →
#     trip: эскалирующий cooldown (5→10→20→30мин cap), чистим окно, пробуем
#     снова. Если снова троттлят — cooldown растёт. Если пускают — сброс.
#   - real-time Upbit-нотисы в это время идут через Seoul-relay.
from collections import deque as _deque

_UPBIT_OUTCOME_WINDOW = int(os.getenv("UPBIT_OUTCOME_WINDOW", "20"))
_upbit_outcomes: "_deque[bool]" = _deque(maxlen=_UPBIT_OUTCOME_WINDOW)  # True = throttled
_upbit_throttle_lock = threading.Lock()


def _record_upbit_throttle() -> None:
    """Вызывается при каждом 429/503/CF-1015 от Upbit API."""
    with _upbit_throttle_lock:
        _upbit_outcomes.append(True)


def _record_upbit_success() -> None:
    """Вызывается при каждом нетроттленом ответе (200/404) от Upbit API."""
    with _upbit_throttle_lock:
        _upbit_outcomes.append(False)


def _upbit_throttle_rate() -> tuple[int, int]:
    """Возвращает (throttled, total) за скользящее окно."""
    with _upbit_throttle_lock:
        total = len(_upbit_outcomes)
        throttled = sum(_upbit_outcomes)
    return throttled, total


def _clear_upbit_outcomes() -> None:
    """Чистый старт окна (после cooldown — пробуем заново)."""
    with _upbit_throttle_lock:
        _upbit_outcomes.clear()


def _adjust_notice_intervals_for_pool() -> None:
    """С 3+ IP уменьшаем интервал notice-поллеров. Каждый IP видит ~10 req/s
    даже на самой агрессивной частоте, остаёмся в rate-limit.
    FIX (throttle-storm): Upbit floor = 100мс (10 req/s) — Cloudflare WAF
    на api-manager.upbit.com агрессивнее Bithumb, 60мс(16.6 rps) банит."""
    global BITHUMB_NOTICE_POLL_INTERVAL, UPBIT_ANNOUNCEMENT_POLL_INTERVAL
    size = len(_LISTING_PROXY_POOL)
    if size >= 3:
        BITHUMB_NOTICE_POLL_INTERVAL = 0.04
        UPBIT_ANNOUNCEMENT_POLL_INTERVAL = max(UPBIT_INTERVAL_MIN, 0.06)
    elif size == 2:
        BITHUMB_NOTICE_POLL_INTERVAL = 0.06
        UPBIT_ANNOUNCEMENT_POLL_INTERVAL = max(UPBIT_INTERVAL_MIN, 0.10)
    # size == 1 (direct only): оставляем дефолты (100/150мс)

# ── Дедуп: L1 (TTL) + L2 (permanent persisted) ────────────────────
# L1 — отсекает шумовые дубли в окне нескольких секунд (несколько каналов
#      пишут об одном листинге). L2 — «уже торговали эту монету» навсегда.
#
# Принцип L2 (FIX 2026-06-02 — per-exchange семантика):
#   Токен блокируется ТОЛЬКО для той биржи, из-за которой открылся — новый
#   рынок (другая биржа) = новый памп, торгуем снова.
#     • биржа распозналась (из source-тега UPBIT-NOTICE/SEOUL-RELAY-*/
#       COINLISTING-* или распарсена из текста TG/TOA) →
#       _per_exchange_fired[exchange]; skip только если ЭТА биржа уже стреляла.
#     • биржа не распозналась (TG/TOA без явной биржи) → _global_fired
#       (блок везде, safe fallback). Сюда же — легаси-записи со старой схемы.
#   Резолв — в _resolve_exchange(); парсинг текста — _detect_exchange().
#
# L2 заполняется ТОЛЬКО после успешного открытия позиции (worker callback),
# чтобы провалившийся open (нет цены/ликвидности) не сделал монету «забытой».
_fired_lock     = threading.Lock()

# L1: короткоживущая защита от near-simultaneous дублей.
_recent_signals: dict[tuple[str, str], float] = {}   # (coin, source) -> expiry
_FIRED_TTL      = 60

# FIX: метрика отставания между источниками. Для каждой свежеcклеймленной
# монеты запоминаем (первый_источник, perf_counter()) — когда второй
# источник по той же монете попадает в _try_claim → skip, считаем дельту
# и логируем "CoinListing опоздал на 4.2с от BWEnews". Это нужно чтобы
# отличать «WS-источник не сработал» от «WS-источник работает, но медленный».
_first_claim_ts: dict[str, tuple[str, float]] = {}   # coin -> (source, t)
_FIRST_CLAIM_TTL = 300  # храним 5 минут — на дольше дельта уже неинтересна

# L2: постоянное хранилище опыта.
# FIX 2026-07-07 (MORPHO-инцидент): _global_fired перманентный блок пропустил
# реальный Upbit-листинг MORPHO — монета отстрелялась 14 июня по TOA-WS (биржа
# не распозналась → global), и месяц спустя настоящий листинг был отброшен.
# global-блок нужен только чтобы погасить ВОЛНУ одной новости (разные источники
# дублируют часами) → теперь dict{coin: fired_ts} с TTL (деф. 72ч,
# env LISTING_GLOBAL_FIRED_TTL_H). per_exchange остаётся перманентным —
# та же биржа не листит ту же монету дважды.
_GLOBAL_FIRED_TTL = float(os.getenv("LISTING_GLOBAL_FIRED_TTL_H", "72")) * 3600.0
_global_fired: dict[str, float] = {}                  # coin → fired_ts (time.time())
_per_exchange_fired: dict[str, set[str]] = {          # биржа распознана — (coin, exchange)
    "UPBIT": set(),
    "BITHUMB": set(),
    "BINANCE": set(),
}

# FIX 2026-06-02: _ANNOUNCEMENT_SOURCE_PREFIXES / _DIRECT_POLL_SOURCES удалены
# вместе с _classify_source. Новая модель (_resolve_exchange) не делит источники
# на ANNOUNCE/DIRECT — она извлекает биржу из source-тега напрямую, а решение
# global-vs-per-exchange принимается по факту «распозналась ли биржа».

_FIRED_FILE = Path(STATE_DIR) / "listing_fired.json"

# FIX-PERF: dirty-flag для фонового L2-writer'а — вместо thread.start() на
# каждый успешный open (то стоило ~3-15мс в hot-path worker'а под GIL contention).
_fired_dirty = threading.Event()

# ── Source-first телеметрия (Patch #3) ────────────────────────────
# Цель: за неделю накопить статистику «кто первым поймал монету» по
# источникам (TOA-WS, TG:BWEnews, COINLISTING-SEOUL, BITHUMB-NOTICE и т.д.),
# чтобы обосновать инвестицию в платную подписку (TOA Sapling /
# CL Premium) — или наоборот, что free-tier хватает.
#
# Что считаем:
#   _source_first_wins[source]            — сколько раз источник был первым
#   _source_lag_stats[(loser, winner)]    — running stats отставаний
#                                            {count, sum_ms, max_ms, min_ms}
#
# Хук в _try_claim (success-путь) → first_wins[source]++.
# Хук в process_signal (когда _measure_lag != None) → lag_stats[(curr, first)].
#
# Persist раз в 5 мин по dirty-flag, summary раз в 6 часов.
_source_stats_lock = threading.Lock()
_source_first_wins: Counter[str] = Counter()
_source_lag_stats: dict[tuple[str, str], dict[str, float]] = {}
_source_stats_dirty = threading.Event()
_SOURCE_STATS_FILE = Path(STATE_DIR) / "source_stats.json"

# ── Health-stats: персистентная диагностика «слабых мест» ─────────
# throttle/error/disconnect/latency по компонентам → TG-отчёт ГДЕ узко.
try:
    from tg.health_stats import HealthStats
    _health = HealthStats(Path(STATE_DIR) / "health_stats.json")
except Exception as _he:  # noqa: BLE001
    print(f"[HEALTH] init failed: {_he!r} — health-stats отключены", flush=True)
    _health = None

# ── Price-recorder (захват головы) + eval-поллер 6ч-итогов (LISTING) ─
# Гибрид: рекордер ловит только первые ~10 мин входа (price_paths.jsonl), а по
# истечении 6ч eval-поллер дотягивает хвост свечами (klines), склеивает,
# оценивает 12 стратегий, копит винрейт и шлёт 6ч-итоги в TG.
# get_price из listing_api читает тот же price_cache, что наполняет price_updater.
_EVAL_LEVERAGE, _EVAL_TAKER, _EVAL_MARGIN = 10.0, 0.00055, 14.0
_EVAL_WINDOW_SEC = 21600          # 6ч — момент оценки
_price_paths_path = Path(STATE_DIR) / "price_paths.jsonl"
_strat_results_path = Path(STATE_DIR) / "strategy_results.jsonl"
_eval_paths_path = Path(STATE_DIR) / "eval_paths.jsonl"
try:
    from tg.price_recorder import PriceRecorder
    from tg.strategy_eval import evaluate as _strat_evaluate, stitch as _stitch
    from api.listing_api import get_price as _gp_for_rec
    # Рекордер = чистый захват головы (без on_complete; оценку делает eval-поллер).
    _recorder = PriceRecorder(_price_paths_path, _gp_for_rec)
except Exception as _re:  # noqa: BLE001
    print(f"[PRICE-REC] init failed: {_re!r} — рекордер отключён", flush=True)
    _recorder = None
    _strat_evaluate = None


def _record_path(coin: str, side: str, src: str, venue: str, entry) -> None:
    """Безопасная регистрация траектории (no-op если рекордер не загрузился)."""
    if _recorder is not None:
        try:
            _recorder.track(coin, side, src, venue, entry)
        except Exception:  # noqa: BLE001
            pass


def _eval_loop() -> None:
    """
    Раз в 60с: для записей price_paths старше 6ч и ещё не оценённых — тянет
    klines-хвост, склеивает с головой, оценивает стратегии, шлёт 6ч-итоги в TG
    и пишет полную траекторию в eval_paths (для оффлайн-бэктеста). Дедуп по
    (coin, entry_ts) из strategy_results — переживает рестарт (догоняет старое).
    """
    import json as _json
    from api.klines import fetch_klines
    if _strat_evaluate is None:
        return
    evaluated = set()
    try:
        if _strat_results_path.exists():
            with _strat_results_path.open("rb") as f:
                for raw in f:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        row = _json.loads(raw)
                    except Exception:  # noqa: BLE001
                        continue
                    evaluated.add((row.get("coin"), int(row.get("entry_ts", 0))))
    except Exception as e:  # noqa: BLE001
        print(f"[EVAL] preload evaluated failed: {e!r}", flush=True)
    print(f"[EVAL] поллер 6ч-итогов активен (оценено ранее: {len(evaluated)})", flush=True)
    while True:
        time.sleep(60)
        try:
            if not _price_paths_path.exists():
                continue
            now = int(time.time())
            with _price_paths_path.open("rb") as f:
                lines = f.readlines()
            for raw in lines:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    rec = _json.loads(raw)
                except Exception:  # noqa: BLE001
                    continue
                coin = rec.get("coin")
                ets = int(rec.get("entry_ts", 0) or 0)
                key = (coin, ets)
                if not coin or ets <= 0 or key in evaluated:
                    continue
                age = now - ets
                if age < _EVAL_WINDOW_SEC:
                    continue                       # ещё не 6ч
                if age > 7 * 86400:
                    evaluated.add(key)             # слишком старая — пропускаем
                    continue
                venue = rec.get("venue", "bybit")
                head = rec.get("samples") or []
                last_head_dt = 0.0
                for s in head:
                    try:
                        if s[1] is not None and float(s[0]) > last_head_dt:
                            last_head_dt = float(s[0])
                    except (TypeError, ValueError, IndexError):
                        pass
                start_ms = (ets + int(last_head_dt)) * 1000
                end_ms = (ets + _EVAL_WINDOW_SEC) * 1000
                tail = fetch_klines(venue, coin, start_ms, end_ms)
                if not tail:
                    print(f"[EVAL] {coin}: klines пусты (venue={venue}) — оценка по голове",
                          flush=True)
                full = _stitch(head, tail, ets)
                actual = _sig_stats.latest_pnl(coin) if _sig_stats is not None else None
                rec2 = dict(rec)
                rec2["samples"] = full
                try:
                    text = _strat_evaluate(rec2, actual, _strat_results_path,
                                           _EVAL_LEVERAGE, _EVAL_TAKER, _EVAL_MARGIN)
                    tg_log(text)
                    with _eval_paths_path.open("ab") as ef:
                        ef.write((_json.dumps(rec2, ensure_ascii=False) + "\n").encode())
                except Exception as e:  # noqa: BLE001
                    print(f"[EVAL] {coin} eval failed: {e!r}", flush=True)
                evaluated.add(key)
        except Exception as e:  # noqa: BLE001
            print(f"[EVAL] loop err: {e!r}", flush=True)


def _hstat(method: str, *args) -> None:
    """Безопасный вызов health-метрики (no-op если модуль не загрузился)."""
    if _health is not None:
        try:
            getattr(_health, method)(*args)
        except Exception:  # noqa: BLE001
            pass


# ── Signal-stats: журнал событий + отчёты 09:00/22:00 МСК ─────────
# Отдельно от health: здесь append-only журнал с таймстампами, из которого
# считаются окна «с прошлого отчёта / 7 дней / 30 дней / месяц». См.
# tg/signal_stats.py.
try:
    from tg.signal_stats import SignalStats
    _sig_stats = SignalStats(
        Path(STATE_DIR) / "signal_events.jsonl",
        Path(STATE_DIR) / "stats_report_state.json",
        kind="LISTING",
    )
except Exception as _se:  # noqa: BLE001
    print(f"[SIGSTATS] init failed: {_se!r} — signal-stats отключены", flush=True)
    _sig_stats = None

# Closed-PnL поллер Bybit — дописывает PnL в статистику когда позиция закрылась.
try:
    from api.bybit_pnl import BybitClosedPnL
    _pnl_poller = BybitClosedPnL(BYBIT_API_KEY, BYBIT_SECRET_KEY)
except Exception as _pe:  # noqa: BLE001
    print(f"[BYBIT-PNL] init failed: {_pe!r} — PnL-трекинг отключён", flush=True)
    _pnl_poller = None

# Closed-PnL поллер Gate.io — реальный PnL Gate-сделок (раньше был «н/д»).
try:
    from api.gate_pnl import GateClosedPnL
    from config.config import GATEIO_API_KEY, GATEIO_SECRET_KEY
    _gate_pnl_poller = GateClosedPnL(GATEIO_API_KEY, GATEIO_SECRET_KEY)
except Exception as _gpe:  # noqa: BLE001
    print(f"[GATE-PNL] init failed: {_gpe!r} — Gate-PnL отключён", flush=True)
    _gate_pnl_poller = None


def _sig_open(coin: str, src: str, venue: str, outcome: str,
              open_ms: float = 0.0, entry: float = 0.0, amount: float = 0.0,
              listing_exchange: str = "") -> None:
    """Безопасный emit open-события (no-op если модуль не загрузился)."""
    if _sig_stats is not None:
        try:
            _sig_stats.log_open(coin, src, venue, outcome, open_ms, entry,
                                amount, listing_exchange)
        except Exception:  # noqa: BLE001
            pass


def _register_pnl(coin: str, src: str, venue: str) -> None:
    """Регистрирует позицию в closed-pnl поллере (Bybit ИЛИ Gate)."""
    v = (venue or "").lower()

    def _on_pnl(c: str, pnl: float, exit_price: float, _s=src) -> None:
        if _sig_stats is not None:
            _sig_stats.log_pnl(c, _s, pnl, exit_price)
    try:
        if v == "bybit" and _pnl_poller is not None:
            _pnl_poller.register(f"{coin}USDT", coin, time.time(), _on_pnl)
        elif v == "gate" and _gate_pnl_poller is not None:
            _gate_pnl_poller.register(f"{coin}_USDT", coin, time.time(), _on_pnl)
    except Exception:  # noqa: BLE001
        pass


def _sig_lag(coin: str, loser: str, winner: str, lag_ms: float) -> None:
    """Безопасный emit lag-события."""
    if _sig_stats is not None:
        try:
            _sig_stats.log_lag(coin, loser, winner, lag_ms)
        except Exception:  # noqa: BLE001
            pass


# ── Watchdog: время последнего успешного запроса ───────────────────
_upbit_last_ts   = time.monotonic()
_bithumb_last_ts = time.monotonic()
_binance_last_ts = time.monotonic()
_ts_lock         = threading.Lock()

# FIX: отслеживаем активные потоки поллеров, чтобы watchdog не плодил дубли.
_upbit_thread:   threading.Thread | None = None
_bithumb_thread: threading.Thread | None = None
_binance_thread: threading.Thread | None = None
_thread_lock     = threading.Lock()

# ── Heartbeat для docker healthcheck ──────────────────────────────
HEARTBEAT_FILE = Path(os.getenv("HEARTBEAT_FILE", "/tmp/listing_heartbeat"))


def _touch_heartbeat() -> None:
    try:
        HEARTBEAT_FILE.touch(exist_ok=True)
    except Exception:
        pass


# ── логирование ───────────────────────────────────────────────────

def _log(tag: str, color: str, msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"{color}{BOLD}[{ts}][{tag}]{RESET} {msg}", flush=True)

def log_info(tag: str, msg: str): _log(tag, CYAN,   msg)
def log_ok(tag: str, msg: str):   _log(tag, GREEN,  msg)
def log_warn(tag: str, msg: str): _log(tag, YELLOW, msg)
def log_err(tag: str, msg: str):  _log(tag, RED,    msg)


# ── дедупликация сигналов (L1 TTL + L2 permanent) ─────────────────

# Биржи, для которых ведём отдельный per-exchange L2-бакет.
_KNOWN_EXCHANGES = ("UPBIT", "BITHUMB", "BINANCE")


def _resolve_exchange(source: str, hint: str = "") -> str | None:
    """
    FIX 2026-06-02: новая семантика L2 — токен блокируется ТОЛЬКО для той биржи,
    из-за которой открылся (новый рынок = новый памп, торгуем снова).

    Возвращает имя биржи (UPBIT/BITHUMB/BINANCE) → ключим per-exchange,
    либо None → global-fallback (блок везде, как раньше).

    Биржу извлекаем из самого source-тега — она уже закодирована во всех
    форматах:
        UPBIT / BITHUMB / BINANCE              (DIRECT-поллер)
        UPBIT-NOTICE / BITHUMB-NOTICE / ...    (notice-поллер)
        SEOUL-RELAY-BITHUMB / -UPBIT           (seoul relay)
        COINLISTING-UPBIT / -BITHUMB           (coinlisting_ws)
    Для TG:{chat_id} и TOA-WS биржи в теге нет → берём hint (распарсенный из
    текста сообщения). Если hint пуст/невалиден → None (global fallback).
    """
    for ex in _KNOWN_EXCHANGES:
        if ex in source:
            return ex
    hint = (hint or "").upper()
    if hint in _KNOWN_EXCHANGES:
        return hint
    return None


def _detect_exchange(text: str) -> str:
    """
    Парсит биржу из текста анонса (TG/TOA). Возвращает UPBIT/BITHUMB/BINANCE
    при ОДНОЗНАЧНОМ матче, иначе "" (ноль или несколько бирж → global fallback,
    как договорено: лучше переблокировать, чем открыть дубль на чужой бирже).
    """
    tl = text.lower()
    found = [ex for ex, kw in (("UPBIT", "upbit"), ("BITHUMB", "bithumb"),
                               ("BINANCE", "binance")) if kw in tl]
    return found[0] if len(found) == 1 else ""


def _load_fired_state() -> None:
    """Подгружает L2 с диска. Безопасно к отсутствию файла / битым данным."""
    try:
        if not _FIRED_FILE.exists():
            return
        raw = _FIRED_FILE.read_bytes()
        if not raw.strip():
            return
        try:
            # FIX: используем уже импортированный _json_loads вместо дубликата
            data = _json_loads(raw)
        except Exception as e:
            log_warn("DEDUP", f"L2 parse failed: {e!r}")
            return
        gf = data.get("global", [])
        with _fired_lock:
            # FIX 2026-07-07: новый формат — dict {coin: fired_ts}; легаси —
            # list[str] без времени → ставим mtime файла (= последний open,
            # консервативно: блок ещё максимум TTL от последнего открытия,
            # потом само истечёт).
            if isinstance(gf, dict):
                for c, ts in gf.items():
                    try:
                        _global_fired[str(c)] = float(ts)
                    except (TypeError, ValueError):
                        continue
            elif isinstance(gf, list):
                legacy_ts = _FIRED_FILE.stat().st_mtime
                for c in gf:
                    if isinstance(c, str):
                        _global_fired[c] = legacy_ts
            for ex, coins in (data.get("per_exchange") or {}).items():
                if ex in _per_exchange_fired and isinstance(coins, list):
                    _per_exchange_fired[ex].update(
                        str(c) for c in coins if isinstance(c, str)
                    )
        per_ex_summary = ", ".join(f"{k}={len(v)}" for k, v in _per_exchange_fired.items())
        log_ok("DEDUP", f"L2 загружен: global={len(_global_fired)} (TTL {_GLOBAL_FIRED_TTL/3600:.0f}ч) | {per_ex_summary}")
    except Exception as e:  # noqa: BLE001
        log_warn("DEDUP", f"L2 load failed: {e!r} — стартуем с пустого")


def _persist_fired_state() -> None:
    """
    Атомарный сейв L2. Вызывается из worker callback после успешного open.
    File-level write — не критично к скорости, делаем sync (write+rename).
    """
    try:
        with _fired_lock:
            # FIX 2026-07-07: global теперь dict {coin: fired_ts} (TTL-семантика).
            snapshot = {
                "global": dict(sorted(_global_fired.items())),
                "per_exchange": {k: sorted(v) for k, v in _per_exchange_fired.items()},
            }
        # FIX: используем уже импортированный _json_dumps вместо дубликата
        payload = _json_dumps(snapshot, indent=True)
        _FIRED_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _FIRED_FILE.with_suffix(".json.tmp")
        tmp.write_bytes(payload)
        tmp.replace(_FIRED_FILE)
    except Exception as e:  # noqa: BLE001
        log_warn("DEDUP", f"L2 persist failed: {e!r}")


def _is_already_fired(coin: str, source: str, exchange: str = "") -> bool:
    """
    L2-проверка. Должна вызываться под _fired_lock.

    Блокируем если:
      • coin в _global_fired И запись моложе TTL (гасим волну одной новости;
        просроченные записи лениво вычищаем — MORPHO-инцидент 2026-07-07:
        вечный global-блок пропустил реальный Upbit-листинг); ИЛИ
      • coin уже стреляли на ТОЙ ЖЕ бирже (_per_exchange_fired[ex]).
    Другая биржа той же монеты — НЕ блок (новый рынок = новый памп).
    """
    ts = _global_fired.get(coin)
    if ts is not None:
        if (time.time() - ts) < _GLOBAL_FIRED_TTL:
            return True
        _global_fired.pop(coin, None)   # просрочена — чистим, персист догонит
        _fired_dirty.set()
    ex = _resolve_exchange(source, exchange)
    if ex is not None:
        return coin in _per_exchange_fired.get(ex, set())
    return False


def _try_claim(coin: str, source: str, exchange: str = "") -> bool:
    """
    L1: ставим claim в окно TTL чтобы шум из множества каналов в течение
    секунд не открывал дубль. L2 — проверяем «уже торговали» перед claim'ом.
    L2 ЗАПИСЫВАЕТСЯ только после успешного open (см. _mark_opened).

    exchange — биржа, распарсенная из текста анонса (TG/TOA); для источников
    с биржей в теге не нужна (_resolve_exchange достанет сам).
    """
    now = time.monotonic()
    with _fired_lock:
        if _is_already_fired(coin, source, exchange):
            return False
        # L1: уже взят (любым источником) в пределах TTL → skip.
        for (c, _src), ts in _recent_signals.items():
            if c == coin and ts > now:
                return False
        _recent_signals[(coin, source)] = now + _FIRED_TTL
        # FIX: запоминаем первый источник, который заявил эту монету.
        # _first_claim_ts заполняется только один раз — пока запись жива,
        # последующие claim'ы по этой монете уйдут в _is_already_fired/L1
        # и сюда не попадут. Это нормально: нам нужна метка именно ПЕРВОГО.
        _first_claim_ts[coin] = (source, now)
    # FIX-LATENCY (Patch #3): source-first counter — ВНЕ _fired_lock,
    # чтобы не удлинять hot-path. _source_stats_lock uncontended ~0.1мкс.
    with _source_stats_lock:
        _source_first_wins[source] += 1
    _source_stats_dirty.set()
    return True


def _measure_lag(coin: str) -> tuple[str, float] | None:
    """
    Возвращает (первый_источник, отставание_в_сек) если по этой монете
    есть запомненный первый источник, иначе None. Используется в
    process_signal для метрики отставания опоздавших источников.
    """
    now = time.monotonic()
    with _fired_lock:
        first = _first_claim_ts.get(coin)
        if first is None:
            return None
        first_src, t0 = first
        if now - t0 > _FIRST_CLAIM_TTL:
            return None
        return first_src, now - t0


def _mark_opened(coin: str, source: str, exchange: str = "") -> None:
    """
    Worker зовёт после успешного open. Обновляет L2 (in-memory) и поднимает
    dirty-flag — фоновый writer (_fired_persist_loop) сохранит на диск.

    FIX-PERF: НЕ спавним поток здесь — это hot-path worker'а. Раньше
    threading.Thread(_persist_fired_state).start() стоил ~3-15мс под GIL
    contention (создание потока — это OS-syscall + GIL acquire несколько раз).
    Теперь только set.add (~1мкс) + Event.set (~1мкс).

    FIX 2026-06-02: биржа резолвится → per-exchange бакет; не резолвится
    (TG/TOA без распознанной биржи) → _global_fired (блок везде).
    """
    ex = _resolve_exchange(source, exchange)
    dirty = False
    with _fired_lock:
        if ex is not None:
            bucket = _per_exchange_fired.setdefault(ex, set())
            if coin not in bucket:
                bucket.add(coin)
                dirty = True
        else:
            # FIX 2026-07-07: dict с fired_ts (TTL-семантика, см. _is_already_fired).
            if coin not in _global_fired:
                _global_fired[coin] = time.time()
                dirty = True
    if dirty:
        _fired_dirty.set()




def _fired_persist_loop() -> None:
    """
    Единый фоновый writer L2 на диск. Просыпается по dirty-flag, ждёт ещё
    1с (батчинг — если за это окно прилетит несколько open'ов на burst'е,
    всё запишется одним write+rename), потом сохраняет.
    """
    while True:
        _fired_dirty.wait()
        time.sleep(1.0)
        _fired_dirty.clear()
        _persist_fired_state()


# ── Source-first stats: persist / load / summary (Patch #3) ──────

def _persist_source_stats() -> None:
    """
    Атомарный sync write source_stats.json. Идентичный паттерн с
    _persist_fired_state: write tmp → rename. Без блокировок hot-path.
    """
    try:
        with _source_stats_lock:
            lag_snapshot: dict[str, dict[str, float]] = {}
            for (loser, winner), stats in _source_lag_stats.items():
                count = stats["count"]
                lag_snapshot[f"{loser}<-{winner}"] = {
                    "count":   count,
                    "sum_ms":  stats["sum_ms"],
                    "max_ms":  stats["max_ms"],
                    "min_ms":  stats["min_ms"],
                    "mean_ms": (stats["sum_ms"] / count) if count else 0.0,
                }
            snapshot = {
                "first_wins": dict(_source_first_wins),
                "lag_stats":  lag_snapshot,
                "updated_at": int(time.time()),
            }
        payload = _json_dumps(snapshot, indent=True)
        _SOURCE_STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _SOURCE_STATS_FILE.with_suffix(".json.tmp")
        tmp.write_bytes(payload)
        tmp.replace(_SOURCE_STATS_FILE)
    except Exception as e:  # noqa: BLE001
        log_warn("STATS", f"persist failed: {e!r}")


def _load_source_stats() -> None:
    """Подгрузка прошлых счётчиков. Терпит отсутствие/битый файл."""
    try:
        if not _SOURCE_STATS_FILE.exists():
            return
        raw = _SOURCE_STATS_FILE.read_bytes()
        if not raw.strip():
            return
        try:
            data = _json_loads(raw)
        except Exception as e:
            log_warn("STATS", f"parse failed: {e!r}")
            return
        first_wins = data.get("first_wins") or {}
        lag_stats  = data.get("lag_stats")  or {}
        with _source_stats_lock:
            if isinstance(first_wins, dict):
                for src, n in first_wins.items():
                    if isinstance(src, str) and isinstance(n, int):
                        _source_first_wins[src] = n
            if isinstance(lag_stats, dict):
                for key, stats in lag_stats.items():
                    if not isinstance(key, str) or "<-" not in key:
                        continue
                    if not isinstance(stats, dict):
                        continue
                    loser, winner = key.split("<-", 1)
                    try:
                        _source_lag_stats[(loser, winner)] = {
                            "count":  float(stats.get("count", 0)),
                            "sum_ms": float(stats.get("sum_ms", 0)),
                            "max_ms": float(stats.get("max_ms", 0)),
                            "min_ms": float(stats.get("min_ms", 0)),
                        }
                    except (TypeError, ValueError):
                        continue
        log_ok(
            "STATS",
            f"source-first загружен: {len(_source_first_wins)} sources, "
            f"{len(_source_lag_stats)} лаг-пар",
        )
    except Exception as e:  # noqa: BLE001
        log_warn("STATS", f"load failed: {e!r}")


def _source_stats_persist_loop() -> None:
    """
    dirty-flag + 5мин батчинг. Записываем на диск редко, но регулярно.
    Параллелим с _fired_persist_loop — лок'и разные.

    FIX: persist() ДО clear(). Раньше clear() стоял первым — если
    _persist_source_stats упадёт (disk flap, FS readonly) → dirty уже
    очищен, и следующая партия будет ждать ЕЩЁ 5 мин до следующего set().
    Теперь: при падении persist dirty остаётся true → next loop iteration
    повторит сразу.
    """
    _PERSIST_BATCH_SEC = 300
    while True:
        _source_stats_dirty.wait()
        time.sleep(_PERSIST_BATCH_SEC)
        _persist_source_stats()
        _source_stats_dirty.clear()


def _print_source_stats_summary() -> None:
    """Кумулятивный summary: топ источников + топ лаг-пары."""
    with _source_stats_lock:
        total = sum(_source_first_wins.values())
        if total == 0:
            return
        top_winners = _source_first_wins.most_common(10)
        # Сортируем лаг-пары по числу наблюдений (информативность).
        lag_pairs = sorted(
            _source_lag_stats.items(),
            key=lambda kv: kv[1]["count"],
            reverse=True,
        )[:10]
        # Делаем копии stats внутри lock'а, чтобы дальше форматировать без него.
        lag_pairs = [(key, dict(stats)) for key, stats in lag_pairs]

    print(f"\n{BOLD}{'═' * 60}{RESET}")
    log_ok("STATS", f"SOURCE-FIRST cumulative: total={total} клеймов")
    for src, n in top_winners:
        pct = 100.0 * n / total
        log_info("STATS", f"  {src:32s} {n:6d}  ({pct:5.1f}%)")
    if lag_pairs:
        log_info("STATS", "Топ лаг-пары (loser ← winner | mean / max / n):")
        for (loser, winner), stats in lag_pairs:
            count = stats["count"]
            mean_ms = (stats["sum_ms"] / count) if count else 0.0
            log_info(
                "STATS",
                f"  {loser:28s} ← {winner:20s} | "
                f"mean {mean_ms:6.0f}мс | max {stats['max_ms']:6.0f} | n={int(count)}",
            )
    print(f"{BOLD}{'═' * 60}{RESET}\n")


def _source_stats_summary_loop() -> None:
    """Каждые 6 часов печатает summary. Первый тик через 1 час после старта."""
    time.sleep(3600)
    while True:
        try:
            _print_source_stats_summary()
        except Exception as e:  # noqa: BLE001
            log_warn("STATS", f"summary failed: {e!r}")
        time.sleep(6 * 3600)


# FIX 2026-06-18: _health_report_loop удалён — TG-отчёт каждые 6ч отключён.


def _fired_sweeper() -> None:
    """L1: очистка истёкших claim'ов. L2 — постоянно, не трогаем."""
    while True:
        time.sleep(5)
        now = time.monotonic()
        with _fired_lock:
            expired = [k for k, ts in _recent_signals.items() if ts <= now]
            for k in expired:
                _recent_signals.pop(k, None)
            # FIX: чистим _first_claim_ts по TTL, иначе на длинной дистанции
            # словарь растёт неограниченно (1 запись на каждую монету,
            # которая когда-либо стреляла).
            expired_first = [
                c for c, (_src, t0) in _first_claim_ts.items()
                if now - t0 > _FIRST_CLAIM_TTL
            ]
            for c in expired_first:
                _first_claim_ts.pop(c, None)


# ── воркер (открытие лонга) ───────────────────────────────────────

def worker(coin: str, margin: float, t_start: float,
           source: str, retries: int = 3, exchange: str = "",
           prefilled: tuple[float, float] | None = None) -> None:
    # Signal-stats: причина последнего провала (для emit в конце).
    # no_price → market_open_short вернул 0; rejected → биржа 400/404.
    # prefilled — результат уже открытого ордера (например, через batch);
    # пропускает market_open_short и идёт сразу к TP/SL postprocess.
    last_fail = "failed"
    for attempt in range(1, retries + 1):
        try:
            # FIX-PERF: пропускаем "Старт"-print на attempt 1. Это
            # форматированный print → stdout = ~1-3мс перед market_open_short
            # в hot-path. Открытие позиции важнее, чем factual лог "сейчас открываем".
            # На retry'ях лог остаётся — там диагностика нужна.
            if attempt > 1:
                log_info("WORKER", f"[{source}] Retry {attempt}/{retries} → {coin} | margin={margin} USDT")
            if prefilled is not None and attempt == 1:
                amount, entry_price = prefilled
            else:
                # FIX 2026-07-07 (INVERT): листинг → ШОРТ (fade the pump).
                amount, entry_price = market_open_short(coin, margin)
            # sentinel (-1, 0) = биржевой REJECT (нет маржи /
            # symbol-not-found / slippage cap превышен). Ретрай не починит —
            # fail-fast, экономит 0.2-0.3с на отказе.
            if amount == -1:
                log_warn("WORKER", f"{coin}: биржа отвергла ордер — fail-fast")
                last_fail = "rejected"
                break
            if not amount:
                log_warn("WORKER", f"{coin}: нет цены, повтор через 0.1с...")
                last_fail = "no_price"
                time.sleep(0.1)
                continue

            # FIX 2026-07-07 (INVERT): выход для ШОРТА — set_tp_sl_short
            # (роутер bybit/gate из delist_api, DELIST_TRAIL_MODE управляет
            # трейлингом). Robinhood-специфика (тугой лонг-выход) не
            # применима к шорту — все идут единым путём.
            _tp_sl_executor.submit(set_tp_sl_short, coin, entry_price, amount)

            open_ms = (time.perf_counter() - t_start) * 1000
            # FIX-PERF: один print вместо двух — раньше OPEN-print + intermediate
            # work + LONG-print между метриками съедали ~4мс на stdout flush
            # (PYTHONUNBUFFERED=1, f-string с ANSI escape codes). Теперь open_ms
            # = total path time, делать второй замер бессмысленно (был бы +100μs).
            log_ok("OPEN", (
                f"[{source}] {coin} | ордер за {BOLD}{open_ms:.0f}мс{RESET}{GREEN} | "
                f"entry={entry_price} | amount={amount:.4f}"
            ))
            # Health: латентность сигнал→ордер по источнику (видно кто медленный).
            _hstat("latency", f"{source}→order", open_ms)
            # FIX-PERF: tg_log теперь fire-and-forget (см. tg/tg_logger.py) —
            # возвращается за ~10мкс, реальный HTTP уходит в фоне.
            # Информативный лог: источник, биржа исполнения, notional, маржа,
            # попытка — по нему сразу видно ОТКУДА сигнал и КАК исполнился.
            # FIX 2026-07-07 (INVERT): venue шорта пишется в
            # delist_api._delist_exchange (выставлен в market_open_short).
            try:
                from api.delist_api import _delist_exchange as _lx
                venue = _lx.get(coin, "bybit").upper()
            except Exception:  # noqa: BLE001
                venue = "?"
            notional = entry_price * amount if (entry_price and amount) else 0.0
            tg_log(
                f"🔴 <b>LISTING SHORT</b> <code>{coin}</code>\n"
                f"📡 Источник: <b>{source}</b>\n"
                f"🏦 Биржа: <b>{venue}</b>\n"
                f"💵 Entry: <code>{entry_price}</code>\n"
                f"📦 Объём: <code>{amount:.4f}</code> (~{notional:.1f} USDT)\n"
                f"💰 Маржа: {margin:.1f} USDT\n"
                f"⏱ Сигнал→ордер: <b>{open_ms:.0f}мс</b>"
                + (f" (попытка {attempt})" if attempt > 1 else "")
            )

            # FIX-PERF: bookkeeping ПОСЛЕ метрики и tg_log — не должен влиять
            # на «время от сигнала до ордера». _mark_opened теперь только
            # set-add + Event.set (~2мкс), без thread.start.
            _mark_opened(coin, source, exchange)
            # Signal-stats: успешное открытие (venue/латентность/объём + биржа
            # листинга). lex = биржа, ОТКУДА сигнал (Upbit/Bithumb/Binance).
            lex = _resolve_exchange(source, exchange) or ""
            _sig_open(coin, source, venue, "opened",
                      open_ms=open_ms, entry=entry_price, amount=amount,
                      listing_exchange=lex)
            # Регистрируем позицию для отслеживания закрытого PnL (только Bybit).
            _register_pnl(coin, source, venue)
            # Бэктест: пишем пост-входную траекторию цены (ШОРТ — инверт).
            _record_path(coin, "short", source, venue, entry_price)
            return
        except Exception as e:
            err_str = str(e)
            log_err("WORKER", f"{coin}: попытка {attempt}/{retries} упала → {e}")
            # FIX: если это 400/404 от Gate.io — повтор бессмысленен,
            # бирж'а семантически отказала (тикер ещё не залистен / запрещён
            # для нашего IP / неверный leverage). Инцидент CTR 2026-05-28:
            # 3 × 400 Bad Request за 0мс, забивали лог и зря тратили retries.
            # 400/404 — fail fast, без sleep.
            if "400 Client Error" in err_str or "404 Client Error" in err_str:
                log_warn("WORKER", f"{coin}: {err_str.split('http')[0].strip()} — fail fast, биржа отвергла")
                last_fail = "rejected"
                break
            last_fail = "failed"
            if attempt < retries:
                # FIX: 0.1с → 0.05с. На retry-path основная задержка — это
                # сетевой round-trip, дополнительные 50мс не имеют смысла.
                # Также worker блокирует TG-handler thread (process_signal
                # вызывает worker inline для single-coin), поэтому 0.3с
                # суммарной блокировки урезаем до 0.15с.
                time.sleep(0.05)

    log_err("WORKER", f"{coin}: все {retries} попытки провалились")
    _hstat("error", f"{source}→order")
    # Signal-stats: финальный провал с распознанной причиной.
    _sig_open(coin, source, "?", last_fail)
    tg_log(
        f"⚠️ <b>LISTING FAIL</b> <code>{coin}</code>\n"
        f"📡 Источник: <b>{source}</b>\n"
        f"❌ Все {retries} попытки провалились — позиция НЕ открыта"
    )


# ── FIX-batch-8: callback для Tree of Alpha WS ───────────────────
# Раньше функция была сломана:
#   (process_signal(pairs, "TOA-WS", t_start=t_start))   ← TypeError (нет t_start в сигнатуре)
#   process_signal(pairs, "TOA-WS", t_start=t_start)     ← unreachable
#   process_signal(pairs, "TOA-WS")                       ← unreachable
# Результат: TOA листинги вообще не открывались, в логах был ловимый TypeError.
# Сейчас: process_signal принимает t_start (опц.), один корректный вызов.

def _on_toa_listing(full_text: str, t_start: float, source_hint: str = "") -> None:
    """
    Callback из treeofalpha_ws — листинг.

    source_hint — поле msg.source из TOA ("Binance"/"Upbit"/"Bithumb"/...).
    FIX 2026-06-02: используем его для per-exchange L2; если TOA не дал
    распознаваемую биржу — fallback на парсинг из текста, иначе "" (global).
    """
    pairs = find_listing_pairs(full_text)
    if not pairs:
        log_warn("TOA-LIST", f"тикеры не найдены: {full_text[:120]}")
        return
    log_ok("TOA-LIST", f"Листинг-сигнал из TOA WS: {pairs}")
    exchange = _detect_exchange(source_hint) or _detect_exchange(full_text)
    # FIX 2026-06-04: Robinhood-листинги закрываются по времени (см. worker).
    # Метку несём в source-теге — воркер проверит подстроку ROBINHOOD.
    blob = f"{source_hint} {full_text}".lower()
    source = "TOA-WS:ROBINHOOD" if "robinhood" in blob else "TOA-WS"
    # FIX-batch-8: пробрасываем t_start из WS-loop. Раньше perf_counter()
    # перезаписывался в process_signal и латентность от прихода сообщения
    # до ордера терялась.
    process_signal(pairs, source, t_start=t_start, exchange=exchange)


# ── общая обработка сигнала ───────────────────────────────────────

# FIX-PERF: модульный long-lived pool вместо `with ThreadPoolExecutor(...)`
# на каждый сигнал. Создание пула + блокировка __exit__ до завершения всех
# worker'ов съедали ~5-15мс из hot-path. Pool живёт всё время работы парсера.
_signal_executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="listing-signal")

# FIX-PERF: отдельный preheated pool для set_tp_sl_long. Раньше worker делал
# `threading.Thread(target=set_tp_sl_long).start()` — это OS-syscall + GIL
# acquire несколько раз = 3-15мс jitter в hot-path под contention. submit
# в уже-запущенный pool ~5-20мкс. Pool отделён от _signal_executor чтобы
# burst листингов (5 worker'ов) не блокировал TP/SL постановку у уже открытых.
_tp_sl_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="listing-tpsl")


def process_signal(pairs: list[str], source: str, t_start: float | None = None,
                   exchange: str = "") -> None:
    """
    FIX-batch-8: t_start теперь параметр. Если None — замеряем сами (бэк-совместимость).
    Если передан (из TOA WS / TG handler / Upbit-Bithumb) — используем точный момент
    прихода сигнала, чтобы метрики OPEN/LONG в логах отражали полный путь.

    FIX 2026-06-02: exchange — биржа, распарсенная из текста анонса (TG/TOA).
    Прокидывается в L2-дедуп, чтобы блокировать монету только для своей биржи.
    Для источников с биржей в source-теге не нужна (_resolve_exchange достанет).
    """
    if t_start is None:
        t_start = time.perf_counter()

    # Health: считаем приход сигнала от КАЖДОГО источника (не только
    # first-wins) — видно кто реально приносит сигналы, кто молчит.
    _hstat("signal", source)

    new_pairs = [c for c in pairs if _try_claim(c, source, exchange)]
    if not new_pairs:
        # FIX: метрика отставания — для каждой отброшенной монеты считаем,
        # на сколько секунд опоздал текущий источник относительно того,
        # кто первым заявил эту монету. Это zero-cost для success-path
        # (мы сюда не попадаем когда new_pairs не пуст). _measure_lag —
        # одно чтение dict под уже существующим lock'ом, O(1).
        lag_info: list[str] = []
        for coin in pairs:
            lag = _measure_lag(coin)
            if lag is not None:
                first_src, dt = lag
                lag_info.append(f"{coin}: {source} опоздал на {dt:.2f}с от {first_src}")
                dt_ms = dt * 1000.0
                # Signal-stats: журнал lag-события (loser=source, winner=first_src).
                _sig_lag(coin, source, first_src, dt_ms)
                # FIX-LATENCY (Patch #3): running stats отставаний.
                # Ключ (loser, winner) — loser=текущий поздний source,
                # winner=первый. Это даёт пары вида
                # ("TG:coin_listing", "TOA-WS") и позволяет ответить
                # на вопрос «насколько TOA опережает TG в среднем».
                key = (source, first_src)
                with _source_stats_lock:
                    entry = _source_lag_stats.get(key)
                    if entry is None:
                        entry = {
                            "count":  0.0,
                            "sum_ms": 0.0,
                            "max_ms": 0.0,
                            "min_ms": dt_ms,
                        }
                        _source_lag_stats[key] = entry
                    entry["count"]  += 1
                    entry["sum_ms"] += dt_ms
                    if dt_ms > entry["max_ms"]:
                        entry["max_ms"] = dt_ms
                    if dt_ms < entry["min_ms"]:
                        entry["min_ms"] = dt_ms
                _source_stats_dirty.set()
        if lag_info:
            log_warn("SIGNAL-LAG", " | ".join(lag_info))
        else:
            log_warn("SIGNAL", f"[{source}] монеты уже в работе или ранее отстреливались: {pairs}")
        return

    margin = calculate_margin_for_listing()

    # FIX-PERF: для single-coin (99% случаев) — inline-execute worker'а
    # в caller-thread. Экономим thread hop через executor (1-3мс под GIL
    # contention, что мы видели как разницу 12мс vs 27мс warm vs cold).
    # Trade-off: caller (TG handler / poller-thread / TOA WS callback)
    # блокируется на ~5-10мс на market_open_long. Это приемлемо:
    #   • TG/TOA async loop: задержка следующего event'а ~10мс OK для
    #     листингов раз в минуту;
    #   • Upbit/Bithumb/Binance pollers: сдвиг следующего poll'а ~10мс
    #     OK при 100мс/2500мс интервалах.
    # На rare retry-path (price отсутствует, sleep 0.1с × до 2 раз) caller
    # блокируется до 300мс — приемлемо, т.к. это означает что и так
    # exchange не отвечает.
    # Multi-coin (редко: одно сообщение про 2+ листинга) → executor для
    # параллелизма.
    if len(new_pairs) == 1:
        worker(new_pairs[0], margin, t_start, source, exchange=exchange)
    else:
        # FIX 2026-07-07 (INVERT): batch-открытие (market_open_long_batch)
        # открывало ЛОНГИ — для шорта batch-пути нет. Multi-coin →
        # параллельные single-short workers через executor. Потеря ~100-800μs
        # на burst vs batch — приемлемо, multi-coin листинги редки.
        for coin in new_pairs:
            _signal_executor.submit(worker, coin, margin, t_start, source,
                                    exchange=exchange)

    print(f"\n{BOLD}{'═' * 60}{RESET}")
    log_ok("LISTING", f"[{source}] Новый листинг!")
    log_info("LISTING", f"Монеты : {new_pairs}")
    log_info("TRADE",   f"Маржа={margin} USDT | открываем {len(new_pairs)} шорт(ов) [INVERT]...")
    print(f"{BOLD}{'═' * 60}{RESET}\n")


# ══════════════════════════════════════════════════════════════════
# ИСТОЧНИК 1: Telegram
# ══════════════════════════════════════════════════════════════════

def run_telegram_listener() -> None:
    """
    TG listener на Telethon (основной + EXTRA_LISTING_CHANNELS).
    FIX-batch-3: multi-channel first-wins. Дедуп: L1 _recent_signals (TTL 60с)
    + L2 _global_fired (постоянно, любой анонс).

    FIX: TelegramClient создаётся ВНУТРИ _run, а не снаружи. Telethon хранит
    привязку к event-loop'у; при reconnect-loop (asyncio.run в while True)
    второй цикл получал бы клиента из предыдущего закрытого loop'а и падал.
    """
    session_path = str(Path(SESSION_DIR) / "listing_session")

    # FIX-batch-3: основной + extra. TG_CHANNEL — это int (-100...).
    channels: list[str | int] = []
    try:
        channels.append(int(TG_CHANNEL))
    except (TypeError, ValueError):
        channels.append(TG_CHANNEL)
    for c in parse_channels(EXTRA_LISTING_CHANNELS):
        if c not in channels:
            channels.append(c)

    async def _run():
        client = TelegramClient(
            session_path,
            api_id=int(TG_API_ID) if TG_API_ID else 0,
            api_hash=TG_API_HASH or "",
            auto_reconnect=True,
            retry_delay=5,
            connection_retries=None,
            request_retries=5,
        )

        @client.on(events.NewMessage(chats=channels))
        async def handler(event):
            # FIX-batch-8: замеряем t_start как можно раньше, до фильтрации и
            # get_chat() — это правильный момент "сигнал получен".
            t_start = time.perf_counter()
            try:
                text = event.message.message or ""
            except Exception:
                return

            if not text:
                return

            # FIX-batch-6: расширенный фильтр под форматы каналов пользователя
            # (BWEnews, binance_announcements, coin_listing, и т.д.).
            tl = text.lower()
            if not _TG_LISTING_POS_RE.search(tl):
                return
            if _TG_LISTING_NEG_RE.search(tl):
                return

            # FIX-PERF: НЕ дёргаем await event.get_chat() здесь — это network
            # round-trip к Telegram, 50–200мс на холодном кеше Telethon. Берём
            # event.chat_id (sync, ~0мс). Username нужен только для лога —
            # подтягиваем уже ПОСЛЕ запуска воркера (см. ниже). Это даёт
            # −50…−150мс на самом первом сигнале из канала.
            chat_id = getattr(event, "chat_id", 0) or 0

            pairs = find_listing_pairs(text)
            if not pairs:
                log_warn("TG", f"Тикер не найден (фильтр пропустил): {text[:80]}")
                return

            source = f"TG:{chat_id}"
            # FIX 2026-06-02: парсим биржу из текста для per-exchange L2.
            # Однозначный матч (upbit/bithumb/binance) → блок только этой биржи;
            # ноль/несколько → "" → global fallback (блок везде).
            exchange = _detect_exchange(text)

            # FIX-PERF: вызываем process_signal НАПРЯМУЮ вместо
            # threading.Thread(target=_safe_signal).start() — экономия ~3-5мс
            # на спавн дополнительного потока. process_signal сам делает
            # executor.submit (уже не блокирует), а оставшиеся print'ы идут
            # ПОСЛЕ submit'а, так что задержка до OPEN не растёт.
            try:
                process_signal(pairs, source, t_start, exchange=exchange)
            except Exception as exc:  # noqa: BLE001
                log_err("TG", f"process_signal упал: {exc!r}")

            # FIX-PERF: get_chat() теперь ПОСЛЕ spawn'а worker'а. Сетевой
            # round-trip к Telegram больше не блокирует открытие ордера.
            try:
                chat  = await event.get_chat()
                uname = getattr(chat, "username", "") or ""
            except Exception:
                uname = ""
            log_ok("TG", f"Листинг-сигнал! [{chat_id} @{uname}]: {text[:120]}")

        await client.start()
        log_ok("TG", "Telethon подключён | сессия: listing_session")

        # FIX-batch-3: пробуем get_entity для каждого канала.
        # FIX 2026-07-07: печатаем и числовой ID (-100...) — чтобы мапить
        # source-теги из source_stats.json (TG:-100...) на юзернеймы
        # и выпиливать бесполезные каналы по статистике.
        for ch in channels:
            try:
                entity = await client.get_entity(ch)
                eid = getattr(entity, "id", "?")
                log_ok("TG", f"  + {getattr(entity, 'title', ch)!r} "
                             f"(@{getattr(entity, 'username', '?')} | id=-100{eid}) ✓")
            except Exception as e:
                log_warn("TG", f"  ! {ch}: {e}")

        log_ok("TG", f"Слушаем листинги из {len(channels)} каналов (first-wins)...")
        await client.run_until_disconnected()

    # FIX: asyncio.run вместо get_event_loop().run_until_complete (deprecated)
    asyncio.run(_run())


# ══════════════════════════════════════════════════════════════════
# ИСТОЧНИК 2: Upbit — polling новых тикеров
# ══════════════════════════════════════════════════════════════════

def _load_upbit_tickers(session: requests.Session) -> set[str]:
    for attempt, timeout in enumerate([4, 8, 15], 1):
        try:
            resp = session.get(UPBIT_MARKETS_URL, params={"isDetails": "false"}, timeout=timeout)
            resp.raise_for_status()
            # FIX-batch-1: orjson — список Upbit может быть 50KB+, orjson в 3-5x быстрее.
            # FIX: явный guard на malformed market — раньше split("-")[1] кидал
            # IndexError для невалидной записи и убивал весь tick.
            tickers: set[str] = set()
            for m in _json_loads(resp.content):
                market = (m or {}).get("market", "")
                if not isinstance(market, str) or not market.startswith("KRW-"):
                    continue
                parts = market.split("-", 1)
                if len(parts) == 2 and parts[1]:
                    tickers.add(parts[1])
            return tickers
        except Exception:
            if attempt == 3:
                raise
            time.sleep(attempt * 2)
    return set()


def run_upbit_poller() -> None:
    global _upbit_last_ts
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    log_ok("UPBIT", "Загружаем начальный список тикеров...")
    try:
        known: set[str] = _load_upbit_tickers(session)
        log_ok("UPBIT", f"Загружено {len(known)} тикеров, жду новые (poll {POLL_INTERVAL*1000:.0f}мс)...")
        with _ts_lock:
            _upbit_last_ts = time.monotonic()
    except Exception as e:
        log_err("UPBIT", f"Ошибка инициализации: {e}")
        known = set()

    ever_seen: set[str] = set(known)

    while True:
        try:
            time.sleep(POLL_INTERVAL)
            # FIX: засекаем t_send ДО HTTP-запроса и t_recv ПОСЛЕ. t_send
            # передаём в process_signal как "момент начала запроса к Upbit"
            # — это полная latency сигнала, включая сеть. t_recv − t_send
            # логируем как fetch_ms для диагностики.
            t_send = time.perf_counter()
            current = _load_upbit_tickers(session)
            t_recv = time.perf_counter()

            with _ts_lock:
                _upbit_last_ts = time.monotonic()

            new_tickers = current - ever_seen

            if new_tickers:
                fetch_ms = (t_recv - t_send) * 1000
                log_ok("UPBIT", f"Новые тикеры: {new_tickers} (fetch={fetch_ms:.0f}мс)")
                ever_seen |= new_tickers
                # t_start = t_send → метрика включает сетевую задержку до Upbit.
                process_signal(list(new_tickers), "UPBIT", t_start=t_send)

        except Exception as e:
            log_err("UPBIT", f"Ошибка поллера: {e}")
            # FIX-batch-8: backoff на 3с при ошибке (был 3с — оставляем),
            # чтобы не флудить упавший Upbit с интервалом 100мс.
            time.sleep(POLL_ERROR_BACKOFF)


# ══════════════════════════════════════════════════════════════════
# ИСТОЧНИК 3: Bithumb — polling новых тикеров
# ══════════════════════════════════════════════════════════════════

def _load_bithumb_tickers(session: requests.Session) -> set[str]:
    for attempt, timeout in enumerate([4, 8, 15], 1):
        try:
            resp = session.get(BITHUMB_ASSETS_URL, timeout=timeout)
            resp.raise_for_status()
            # FIX-batch-1: orjson.
            data = _json_loads(resp.content).get("data", {})
            # FIX: убран фильтр `withdrawal_status==1 AND deposit_status==1`.
            # Bithumb открывает торги ДО включения переводов — фильтр
            # отсекал свежий листинг и ловил его только через 50+ минут
            # после открытия маркета (инцидент BILL 2026-05-28:
            # реальный листинг 08:16:30 UTC, наш поллер засёк в 09:10).
            # Защита от bulk-апдейтов (>10 новых тикеров за тик)
            # сохранена в run_bithumb_poller — она и фильтрует мусор от
            # maintenance/первой инициализации списка после рестарта.
            # Дополнительно: keys любого типа кроме str отсекаем явно,
            # чтобы не словить exception в .upper() / set ops дальше по пайплайну.
            return {
                ticker.upper()
                for ticker in data.keys()
                if isinstance(ticker, str) and ticker
            }
        except Exception:
            if attempt == 3:
                raise
            time.sleep(attempt * 2)
    return set()


def run_bithumb_poller() -> None:
    global _bithumb_last_ts
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    log_ok("BITHUMB", "Загружаем начальный список тикеров...")
    try:
        known: set[str] = _load_bithumb_tickers(session)
        log_ok("BITHUMB", f"Загружено {len(known)} тикеров, жду новые (poll {POLL_INTERVAL*1000:.0f}мс)...")
        with _ts_lock:
            _bithumb_last_ts = time.monotonic()
    except Exception as e:
        log_err("BITHUMB", f"Ошибка инициализации: {e}")
        known = set()

    ever_seen: set[str] = set(known)

    while True:
        try:
            time.sleep(POLL_INTERVAL)
            # FIX: t_send до HTTP, t_recv после — см. комментарий в run_upbit_poller.
            t_send = time.perf_counter()
            current = _load_bithumb_tickers(session)
            t_recv = time.perf_counter()

            with _ts_lock:
                _bithumb_last_ts = time.monotonic()

            new_tickers = current - ever_seen

            # FIX: порог 3 → 10. После maintenance Bithumb может разово отдать
            # 5-8 новых тикеров — мы их пропускали все, теряя реальный листинг.
            if len(new_tickers) > 10:
                log_warn("BITHUMB", f"Подозрительно много новых тикеров ({len(new_tickers)}), пропускаем")
                ever_seen |= current
                continue

            if new_tickers:
                fetch_ms = (t_recv - t_send) * 1000
                log_ok("BITHUMB", f"Новые тикеры: {new_tickers} (fetch={fetch_ms:.0f}мс)")
                ever_seen |= new_tickers
                process_signal(list(new_tickers), "BITHUMB", t_start=t_send)

        except Exception as e:
            log_err("BITHUMB", f"Ошибка поллера: {e}")
            time.sleep(POLL_ERROR_BACKOFF)


# ══════════════════════════════════════════════════════════════════
# ИСТОЧНИК 3a: Bithumb Announcements — polling notice-feed API.
# Ловит листинг ИЗ АНОНСА (биржа сначала анонсирует, потом включает
# торговлю — успеваем войти раньше, чем market poller заметит новый
# тикер в /assetsstatus). Дополняет run_bithumb_poller, не заменяет.
# ══════════════════════════════════════════════════════════════════
# count=20 vs дефолт=5: получаем 4× больше нотисов за один запрос
# (~4.5KB vs 1.1KB). Защита от пропуска при бурсте, когда биржа
# публикует несколько анонсов одновременно.
BITHUMB_NOTICES_URL = "https://api.bithumb.com/v1/notices?count=20"
# FIX 2026-06-18: 166 ReadTimeout'ов (timeout=2с) на проде — Bithumb из датацентрового
# IP часто отвечает >2с. Поднимаем timeout и делаем ретрай на ДРУГОМ IP (round-robin
# сессия) перед тем как сдаться: одиночный таймаут конкретного IP больше не теряет нотис.
BITHUMB_NOTICE_TIMEOUT = float(os.getenv("BITHUMB_NOTICE_TIMEOUT", "4.0"))


def _bithumb_get_notices(timeout: float = BITHUMB_NOTICE_TIMEOUT):
    """GET /v1/notices с ретраем на другой round-robin сессии (другой IP).
    Возвращает распарсенный JSON (list) или поднимает последнее исключение."""
    last_exc = None
    for _ in range(2):   # 2 попытки: разные IP из пула — лечит транзиентный таймаут IP
        try:
            resp = _next_listing_session().get(BITHUMB_NOTICES_URL, timeout=timeout)
            resp.raise_for_status()
            return _json_loads(resp.content)
        except Exception as e:  # noqa: BLE001
            last_exc = e
    raise last_exc

# Категории-стопы (поле "categories" в JSON). Bithumb сам помечает анонс.
_BITHUMB_NOTICE_NEG_CATEGORIES = {
    "거래지원종료",  # делистинг
    "입출금",        # depo/withdraw (не торговля)
    "안내",          # общая инфа
    "점검",          # тех. работы
    "이벤트",        # event/promo
}

# Та же логика, что в coinlisting_ws._BITHUMB_NEG_KEYWORDS — на случай
# если категория пустая/незнакомая, защищаемся по тексту title.
_BITHUMB_NOTICE_NEG_KEYWORDS = (
    "종료", "중단", "유의", "상장폐지", "해제",
    "연기", "일시", "점검", "재개",
)

# Тикер в title: 코인이름(TICKER) ... — тот же паттерн, что в Upbit.
_BITHUMB_NOTICE_TICKER_RE = re.compile(r"\(([A-Z][A-Z0-9]{1,9})\)")

# В каталоге id'ы из pc_url: https://feed.bithumb.com/notice/1653467
_BITHUMB_NOTICE_ID_RE = re.compile(r"/notice/(\d+)")

# 100мс — медиана детекции ~50мс после публикации. Bithumb-feed
# не имеет публичного rate-limit на этот endpoint (открытый, без auth) —
# 10 RPS endpoint точно вытянет. count=20 страхует от пропусков, если
# биржа за один тик опубликует несколько анонсов.
BITHUMB_NOTICE_POLL_INTERVAL = 0.1

_BITHUMB_NOTICE_BANNED = {
    "KRW", "BTC", "ETH", "USD", "USDT", "USDC", "BUSD", "DAI",
    "NFT", "KST", "UTC", "API", "VIP",
    "MARKET", "MARKETS", "LIST",
    "UPBIT", "BITHUMB", "BINANCE", "COINBASE",
}


def _extract_bithumb_notice_tickers(title: str, categories: list[str]) -> list[str]:
    """
    Возвращает список тикеров из title, если notice — листинг.
    Возвращает [] для делиста, service-нотисов, warning'ов.
    """
    for cat in categories:
        if cat in _BITHUMB_NOTICE_NEG_CATEGORIES:
            return []
    for kw in _BITHUMB_NOTICE_NEG_KEYWORDS:
        if kw in title:
            return []
    found: list[str] = []
    seen: set[str] = set()
    for tok in _BITHUMB_NOTICE_TICKER_RE.findall(title):
        token = tok.upper()
        if token in _BITHUMB_NOTICE_BANNED or token in seen:
            continue
        seen.add(token)
        found.append(token)
    return found


def run_bithumb_announcement_poller() -> None:
    # FIX-LATENCY (Patch #2): сессия теперь не одна, а round-robin из
    # _LISTING_PROXY_POOL. С 3+ Korean datacenter IP'ами можно безопасно
    # ужать интервал до 30-50мс (см. _adjust_notice_intervals_for_pool).
    pool_size = len(_LISTING_PROXY_POOL)
    log_ok("BITHUMB-NOTICE", f"Старт (poll {BITHUMB_NOTICE_POLL_INTERVAL*1000:.0f}мс, pool={pool_size})")

    # FIX-CRITICAL: инициализация ОБЯЗАТЕЛЬНА. Если init упадёт, seen_ids
    # останется пустым → первый успешный poll сочтёт ВСЕ 20 нотисов
    # "новыми" и откроет позиции на backlog'е недельной давности.
    # Ретраим init до победы; ДО победы в poll-loop ниже не идём.
    seen_ids: set[str] | None = None
    init_attempt = 0
    while seen_ids is None:
        init_attempt += 1
        try:
            tmp: set[str] = set()
            for item in _bithumb_get_notices():
                url = (item or {}).get("pc_url", "")
                m = _BITHUMB_NOTICE_ID_RE.search(url)
                if m:
                    tmp.add(m.group(1))
            seen_ids = tmp
            log_ok("BITHUMB-NOTICE",
                   f"Инициализирован после #{init_attempt}, известно {len(seen_ids)} нотисов")
        except Exception as e:
            log_warn("BITHUMB-NOTICE",
                     f"init #{init_attempt} упал: {e!r} — retry через 5с (трейды БЛОКИРОВАНЫ)")
            time.sleep(5)

    while True:
        try:
            time.sleep(BITHUMB_NOTICE_POLL_INTERVAL)
            t_send = time.perf_counter()
            # FIX-LATENCY: round-robin сессия на КАЖДЫЙ запрос (размазывает нагрузку
            # по IP). FIX 2026-06-18: + ретрай на другом IP и timeout 4с (было 2с —
            # 166 таймаутов теряли нотисы).
            items = _bithumb_get_notices()
            t_recv = time.perf_counter()

            if not isinstance(items, list):
                continue

            # Новые id (от первого = свежайшего к последнему).
            for item in items:
                if not isinstance(item, dict):
                    continue
                url = item.get("pc_url", "") or ""
                m = _BITHUMB_NOTICE_ID_RE.search(url)
                if not m:
                    continue
                notice_id = m.group(1)
                if notice_id in seen_ids:
                    continue
                seen_ids.add(notice_id)

                title = item.get("title") or ""
                categories = item.get("categories") or []
                if not isinstance(categories, list):
                    categories = []

                tickers = _extract_bithumb_notice_tickers(title, categories)
                if not tickers:
                    log_info(
                        "BITHUMB-NOTICE",
                        f"skip [{','.join(categories)}] {title[:80]}",
                    )
                    continue

                fetch_ms = (t_recv - t_send) * 1000
                log_ok(
                    "BITHUMB-NOTICE",
                    f"LISTING {tickers} ({fetch_ms:.0f}мс fetch) | {title[:80]}",
                )
                process_signal(tickers, "BITHUMB-NOTICE", t_start=t_send)

        except Exception as e:
            _log_poll_error_throttled("BITHUMB-NOTICE", e)
            _hstat("error", "BITHUMB-NOTICE")
            time.sleep(POLL_ERROR_BACKOFF)


# ══════════════════════════════════════════════════════════════════
# ИСТОЧНИК 3b: Upbit Announcements — polling по next_id.
# Upbit list-endpoint защищён, но api-manager.upbit.com/api/v1/announcements/{id}
# для отдельной статьи открыт и отдаёт JSON с title типа "...(IO) KRW 마켓...".
# Стратегия: знаем последний известный id; раз в 200мс пробуем next_id,
# next_id+1, next_id+2 — если ответ success=true, парсим title и инкрементим.
# Это даёт ~100мс медианной детекции против ~1-2с у CoinListing-MASKED-path.
# ══════════════════════════════════════════════════════════════════
UPBIT_ANNOUNCEMENT_URL = "https://api-manager.upbit.com/api/v1/announcements/{id}"

# FIX (ресерч 2026-06): LIST endpoint — один запрос отдаёт N свежих нотисов
# с title/category/listed_at/first_listed_at, отсортированных по id убыв.
# Ловит ЛЮБЫЕ гэпы id мгновенно (id-increment lookahead их пропускал) и
# даёт title сразу → не нужен per-id body GET для извлечения тикера.
# category=trade фильтрует только торговые нотисы на стороне сервера.
UPBIT_ANNOUNCEMENT_LIST_URL = (
    "https://api-manager.upbit.com/api/v1/announcements"
    "?os=web&page=1&per_page=5&category=trade"
)

# Стартовый id: захардкоден на дату коммита (2026-05-29, последний
# известный id = 6255 — это листинг IO). На проде поллер быстро
# доберётся до текущего за несколько итераций. Если id уже занят
# (success=true) — инкрементим без задержки; если 404 — ждём интервал.
UPBIT_ANNOUNCEMENT_START_ID = 6256

# 150мс на цикл проверки. При lookahead=3 это 6.6-20 RPS пик —
# в пределах Upbit public limit 10 req/s (api-manager). В обычном
# режиме (нет новых id) делаем 1 probe + sleep = ~6.6 RPS.
UPBIT_ANNOUNCEMENT_POLL_INTERVAL = 0.15

# Сколько id'ов вперёд пробовать за один тик (защита от прыжков id'ов
# через служебные категории, которые мы пропускаем).
# FIX (ресерч 2026-06): наблюдались реальные гэпы id до +14 (6248→6262).
# lookahead=3 МОЛЧА пропускал листинги при таких прыжках — это потерянные
# сделки, не латентность. Подняли до 15. Лишние probe идут только когда
# реально есть новые id подряд (на холостом ходу — 1 probe + sleep).
UPBIT_ANNOUNCEMENT_LOOKAHEAD = 15

# FIX (throttle-storm 2026-06-04): Upbit-поллер без backoff-а уходил в
# бесконечный throttle-loop. Cloudflare WAF на api-manager.upbit.com банит
# агрессивнее Bithumb. Circuit-breaker по throttle-rate в скользящем окне
# (см. _upbit_outcomes выше):
#   - если throttle-rate за окно ≥ UPBIT_TRIP_RATE → trip breaker
#   - cooldown эскалирует: UPBIT_COOLDOWN_BASE → ×2 → ... → UPBIT_COOLDOWN_MAX
#   - real-time покрытие Upbit в это время держит Seoul-relay
#   - минимальный интервал: UPBIT_INTERVAL_MIN = 100мс (10 req/s)
UPBIT_TRIP_RATE = float(os.getenv("UPBIT_TRIP_RATE", "0.8"))           # ≥80% окна throttled → trip
UPBIT_TRIP_MIN_SAMPLES = int(os.getenv("UPBIT_TRIP_MIN_SAMPLES", "8")) # не триггерим пока < N исходов
UPBIT_COOLDOWN_BASE = float(os.getenv("UPBIT_COOLDOWN_BASE", "300.0")) # 5 мин
UPBIT_COOLDOWN_MAX = float(os.getenv("UPBIT_COOLDOWN_MAX", "1800.0"))  # 30 мин cap
UPBIT_INTERVAL_MIN = float(os.getenv("UPBIT_INTERVAL_MIN", "0.10"))    # 100мс floor

# FIX (2026-06-04): мастер-выключатель Upbit direct-поллера. По умолчанию OFF —
# datacenter-прокси банятся Cloudflare наглухо (см. main()). Включать только
# с корейскими residential прокси.
UPBIT_DIRECT_POLLER_ENABLED = os.getenv("UPBIT_DIRECT_POLLER_ENABLED", "0").lower() in ("1", "true", "yes", "on")

# Категории, которые ВЕДУТ к торговле. У Upbit поле "category" в JSON:
# наблюдали "거래" для трейд-нотисов (включая листинги, делисты, изменения).
_UPBIT_NOTICE_TRADE_CATEGORIES = {"거래"}

# FIX (fake-long 2026-06-04, id=6258 SLX): инцидент.
# Нотис "솔스티스(SLX) 신규 거래지원 안내 ... (거래지원 개시 시점 추가 변경 안내)"
# был апдейтом времени старта уже анонсированного листинга — НЕ новым листингом.
# Прошёл потому что "변경" (изменение) не было в негативном списке.
#
# Двухуровневая защита:
#  1. NEG-список расширен словами update/reschedule/announce-ahead.
#  2. Требуем ПОЗИТИВНЫЙ паттерн открытия торгов в title.
#
# Базовые стопы Bithumb (делист/пауза/тех.работы) + Upbit-специфика.
_UPBIT_NOTICE_NEG_KEYWORDS = _BITHUMB_NOTICE_NEG_KEYWORDS + (
    "변경",      # изменение (времени/условий) — апдейт, не листинг
    "추가 변경",  # доп. изменение
    "지연",      # задержка
    "재공지",    # повторное уведомление
    "예정",      # запланировано (анонс заранее, торги ещё не открыты)
    "정정",      # исправление/корректировка
)

# FIX: позитивный паттерн — title ДОЛЖЕН содержать хотя бы одну из фраз,
# означающих фактическое открытие торгов (а не анонс/изменение).
# "거래지원 개시" = старт торговой поддержки; "신규 거래지원" = новая
# торговая поддержка; "원화 마켓 추가"/"KRW 마켓" = добавление KRW-рынка.
# (Полная таксономия уточняется по результатам research — это hotfix.)
_UPBIT_NOTICE_POS_KEYWORDS = (
    "거래지원 개시",
    "신규 거래지원",
    "원화 마켓",
    "거래 개시",
    "디지털 자산 추가",   # FIX: "KRW 마켓 디지털 자산 추가" (IO id=6255) — реальный
                          # листинг, отсекался по nopos. Латиница "KRW", не "원화".
    "마켓 추가",          # "... 마켓 추가" — добавление рынка
)

_UPBIT_NOTICE_TICKER_RE = _BITHUMB_NOTICE_TICKER_RE
_UPBIT_NOTICE_BANNED = _BITHUMB_NOTICE_BANNED


def _try_fetch_upbit_announcement(
    session: requests.Session,
    notice_id: int,
) -> tuple[str | None, str | None, bool]:
    """
    Возвращает (title, category, is_reannounce) если id существует,
    или (None, None, False) если 404 / ошибка.

    is_reannounce=True означает, что нотис — ПЕРЕанонс уже опубликованного
    (listed_at != first_listed_at). Машиночитаемый признак апдейта/переноса
    времени старта (язык-независимый, надёжнее корейских keyword'ов).
    Подтверждено на проде: SLX(id=6258), TRAC, IRYS, UP2, B3 имели
    listed_at != first_listed_at и НЕ были новыми листингами; реальные
    листинги IO/VVV/PROS имели listed_at == first_listed_at.
    """
    try:
        url = UPBIT_ANNOUNCEMENT_URL.format(id=notice_id)
        resp = session.get(url, timeout=2)
        if resp.status_code == 404:
            _record_upbit_success()  # 404 — НЕ троттл, API отвечает штатно
            return None, None, False
        # FIX (ресерч): api-manager за Cloudflare. При троттле он отдаёт
        # 429/503 ИЛИ статус 200 с HTML-телом "error code: 1015" (НЕ JSON).
        # Раньше _json_loads на таком теле бросал → except → (None,None,False),
        # что неотличимо от 404 «нет нотиса» → СЛЕПОЕ ПЯТНО в секунды листинга.
        # Теперь детектим явно и логируем (round-robin прокси сменит IP).
        if resp.status_code in (429, 503) or b"error code: 1015" in resp.content[:200]:
            _record_upbit_throttle()
            log_warn("UPBIT-NOTICE", f"throttle (CF 1015/{resp.status_code}) id={notice_id} — смена IP на след. probe")
            _hstat("throttle", "UPBIT-NOTICE")
            return None, None, False
        resp.raise_for_status()
        data = _json_loads(resp.content)
    except Exception:
        return None, None, False

    if not isinstance(data, dict) or not data.get("success"):
        return None, None, False
    _record_upbit_success()  # FIX throttle-storm: штатный ответ
    payload = data.get("data") or {}
    title = payload.get("title") or ""
    category = payload.get("category") or ""
    # listed_at != first_listed_at → нотис переанонсирован (апдейт времени).
    listed_at = payload.get("listed_at")
    first_listed_at = payload.get("first_listed_at")
    is_reannounce = bool(listed_at and first_listed_at and listed_at != first_listed_at)
    return title, category, is_reannounce


def _fetch_upbit_list(session: requests.Session) -> list[dict] | None:
    """
    LIST endpoint: возвращает список свежих trade-нотисов (id убыв.) или None
    при ошибке/троттле. Каждый элемент: {id, title, category, is_reannounce}.
    Один запрос вместо id-increment — ловит гэпы id, даёт title сразу.
    """
    try:
        resp = session.get(UPBIT_ANNOUNCEMENT_LIST_URL, timeout=2)
        if resp.status_code in (429, 503) or b"error code: 1015" in resp.content[:200]:
            _record_upbit_throttle()
            log_warn("UPBIT-NOTICE", f"LIST throttle (CF 1015/{resp.status_code}) — смена IP")
            _hstat("throttle", "UPBIT-NOTICE")
            return None
        resp.raise_for_status()
        data = _json_loads(resp.content)
    except Exception:
        return None

    if not isinstance(data, dict) or not data.get("success"):
        return None
    _record_upbit_success()  # FIX throttle-storm: штатный LIST-ответ
    notices = ((data.get("data") or {}).get("notices")) or []
    out: list[dict] = []
    for n in notices:
        if not isinstance(n, dict):
            continue
        nid = n.get("id")
        if nid is None:
            continue
        la = n.get("listed_at")
        fl = n.get("first_listed_at")
        out.append({
            "id": int(nid),
            "title": n.get("title") or "",
            "category": n.get("category") or "",
            "is_reannounce": bool(la and fl and la != fl),
        })
    return out


def _upbit_id_exists(session: requests.Session, notice_id: int) -> bool:
    """True если /announcements/{id} вернул success=true (что угодно)."""
    title, _, _ = _try_fetch_upbit_announcement(session, notice_id)
    return title is not None


def _discover_upbit_max_id(session: requests.Session) -> int:
    """
    Бинарный поиск максимального существующего announcement-id.
    Возвращает (max + 1) — с этого id поллер начнёт работу.

    Алгоритм:
      1. baseline = UPBIT_ANNOUNCEMENT_START_ID — заведомо существующий
         id на момент коммита. Если он сам уже стал 404 (Upbit удалил
         старые) — фоллбэк на baseline без discovery, бот будет
         догонять реальный max естественным инкрементом.
      2. Экспоненциальный рост: пробуем baseline+1, +2, +4, +8, ...
         пока не получим 404 → найдена верхняя граница [lo, hi].
      3. Бинарный поиск в [lo, hi] → lo = последний существующий.

    Latency: для разрыва ~N итераций нужно ~2*log2(N) запросов.
    N=1000 → ~20 req, N=10000 → ~26 req. При 50мс sleep между ними
    discovery занимает ~1-3с на проде.
    """
    baseline = UPBIT_ANNOUNCEMENT_START_ID

    # 0. Sanity check: baseline должен существовать. Если нет —
    # bail-out, иначе бесконечный 404-loop в экспоненциальной фазе.
    if not _upbit_id_exists(session, baseline):
        log_warn(
            "UPBIT-NOTICE",
            f"baseline id={baseline} уже не существует — discovery skip, "
            f"начнём с baseline (будем догонять max естественным инкрементом)",
        )
        return baseline

    # 1. Экспоненциальный рост до первого 404.
    lo = baseline
    step = 1
    hi: int | None = None
    while step <= 1_000_000:  # safety net
        probe = baseline + step
        if _upbit_id_exists(session, probe):
            lo = probe
            step *= 2
        else:
            hi = probe
            break
        time.sleep(0.05)  # не флудим Upbit
    if hi is None:
        # Не нашли 404 за 1M шагов — что-то странное, fallback.
        log_warn("UPBIT-NOTICE", f"discovery: не нашли границу за 1M, fallback lo={lo}")
        return lo + 1

    # 2. Бинарный поиск.
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if _upbit_id_exists(session, mid):
            lo = mid
        else:
            hi = mid
        time.sleep(0.05)

    return lo + 1


def _handle_upbit_notice(nid: int, title: str, category: str,
                         is_reannounce: bool, t_start: float) -> bool:
    """
    Общий фильтр+fire для одного Upbit-нотиса (LIST и id-increment делят).
    Возвращает True если выстрелили (передали в process_signal).
    Порядок: reannounce → category → neg → pos → извлечь тикеры.
    """
    if is_reannounce:
        log_info("UPBIT-NOTICE", f"skip id={nid} reannounce (listed_at≠first) | {title[:55]}")
        return False
    if category not in _UPBIT_NOTICE_TRADE_CATEGORIES:
        log_info("UPBIT-NOTICE", f"skip id={nid} cat={category} | {title[:60]}")
        return False
    neg_hit = next((kw for kw in _UPBIT_NOTICE_NEG_KEYWORDS if kw in title), None)
    if neg_hit:
        log_info("UPBIT-NOTICE", f"skip id={nid} neg='{neg_hit}' | {title[:60]}")
        return False
    if not any(kw in title for kw in _UPBIT_NOTICE_POS_KEYWORDS):
        log_info("UPBIT-NOTICE", f"skip id={nid} no-pos-kw | {title[:60]}")
        return False

    tickers: list[str] = []
    seen: set[str] = set()
    for tok in _UPBIT_NOTICE_TICKER_RE.findall(title):
        t = tok.upper()
        if t in _UPBIT_NOTICE_BANNED or t in seen:
            continue
        seen.add(t)
        tickers.append(t)
    if not tickers:
        log_warn("UPBIT-NOTICE", f"id={nid} нет тикеров в title | {title[:80]}")
        return False

    fetch_ms = (time.perf_counter() - t_start) * 1000
    log_ok("UPBIT-NOTICE", f"LISTING id={nid} {tickers} ({fetch_ms:.0f}мс) | {title[:80]}")
    _hstat("latency", "UPBIT-NOTICE→detect", fetch_ms)
    process_signal(tickers, "UPBIT-NOTICE", t_start=t_start)
    return True


def run_upbit_announcement_poller() -> None:
    # FIX-LATENCY (Patch #2): proxy pool round-robin вместо одной session.
    pool_size = len(_LISTING_PROXY_POOL)

    # FIX: обязательно стартуем с реально актуального id, иначе после
    # каждого рестарта бот будет ловить старые листинги, пока не дойдёт
    # до сегодняшнего id'а — а это сотни ложных «листингов» за итерации.
    try:
        t0 = time.perf_counter()
        next_id = _discover_upbit_max_id(_next_listing_session())
        elapsed = (time.perf_counter() - t0) * 1000
        log_ok("UPBIT-NOTICE", f"discovery: текущий max+1 = {next_id} ({elapsed:.0f}мс)")
    except Exception as e:
        log_err("UPBIT-NOTICE", f"discovery упал ({e!r}), fallback baseline={UPBIT_ANNOUNCEMENT_START_ID}")
        next_id = UPBIT_ANNOUNCEMENT_START_ID

    log_ok(
        "UPBIT-NOTICE",
        f"Старт с id={next_id} (poll {UPBIT_ANNOUNCEMENT_POLL_INTERVAL*1000:.0f}мс, pool={pool_size})",
    )

    # last_seen_id — наибольший обработанный id. Стартуем на (next_id - 1),
    # т.к. next_id это «первый ещё не виденный».
    last_seen_id = next_id - 1

    # FIX (throttle-storm 2026-06-04): circuit-breaker по throttle-rate.
    # Consecutive-streak не работал — редкие успехи с 13 прокси сбрасывали
    # счётчик, cooldown не срабатывал, поллер вечно долбил CF-бан. Теперь:
    # если ≥UPBIT_TRIP_RATE окна затроттлено → trip → эскалирующий cooldown.
    cur_interval = UPBIT_ANNOUNCEMENT_POLL_INTERVAL
    cooldown = UPBIT_COOLDOWN_BASE

    while True:
        try:
            # ── Circuit breaker: проверяем throttle-rate за окно ─────
            throttled, total = _upbit_throttle_rate()
            if total >= UPBIT_TRIP_MIN_SAMPLES and throttled / total >= UPBIT_TRIP_RATE:
                log_err(
                    "UPBIT-NOTICE",
                    f"breaker TRIP: {throttled}/{total} throttled (≥{UPBIT_TRIP_RATE:.0%}) "
                    f"→ cooldown {cooldown/60:.0f}мин (Upbit идёт через Seoul-relay)",
                )
                time.sleep(cooldown)
                # Эскалация на следующий trip; чистим окно → пробуем заново.
                cooldown = min(cooldown * 2, UPBIT_COOLDOWN_MAX)
                _clear_upbit_outcomes()
                continue

            t_send = time.perf_counter()
            # ── ПЕРВИЧНО: LIST-poll. Один запрос отдаёт свежие нотисы с
            # title сразу. Ловит ЛЮБЫЕ гэпы id (id-increment их пропускал).
            notices = _fetch_upbit_list(_next_listing_session())

            if notices is not None:
                # LIST успешен — здоровый API. Сбрасываем cooldown к базовому.
                cooldown = UPBIT_COOLDOWN_BASE
                # Новые = id > last_seen_id. Обрабатываем в порядке возр. id.
                fresh = sorted((n for n in notices if n["id"] > last_seen_id),
                               key=lambda n: n["id"])
                for n in fresh:
                    _handle_upbit_notice(n["id"], n["title"], n["category"],
                                         n["is_reannounce"], t_send)
                    last_seen_id = max(last_seen_id, n["id"])
                next_id = last_seen_id + 1
                time.sleep(cur_interval)
                continue

            # ── LIST вернул None — троттл или ошибка. ───────────────
            # FIX (throttle-storm): при недавнем троттле НЕ дёргаем fallback
            # id-increment (каждый probe = ещё запрос к забаненому API,
            # продлевает бан). Спим интервал — breaker наверху разрулит,
            # если троттл устойчивый.
            recent_throttled, recent_total = _upbit_throttle_rate()
            if recent_total > 0 and recent_throttled / recent_total >= 0.5:
                time.sleep(cur_interval)
                continue

            # ── FALLBACK: LIST упал не из-за троттла (сеть/ошибка) →
            # id-increment probe (lookahead). Гарантирует детект даже
            # если LIST временно лёг.
            advanced = False
            for offset in range(UPBIT_ANNOUNCEMENT_LOOKAHEAD):
                probe_id = next_id + offset
                t_probe = time.perf_counter()
                title, category, is_reannounce = _try_fetch_upbit_announcement(
                    _next_listing_session(), probe_id)
                if title is None:
                    break  # 404/throttle — дальше пусто (id монотонны)
                next_id = probe_id + 1
                last_seen_id = max(last_seen_id, probe_id)
                advanced = True
                _handle_upbit_notice(probe_id, title, category, is_reannounce, t_probe)

            if not advanced:
                time.sleep(cur_interval)

        except Exception as e:
            _log_poll_error_throttled("UPBIT-NOTICE", e)
            _hstat("error", "UPBIT-NOTICE")
            time.sleep(POLL_ERROR_BACKOFF)


# ══════════════════════════════════════════════════════════════════
# ИСТОЧНИК 3c: Binance Announcements — polling catalogId=48.
# bapi/composite/v1/public/cms/article/list/query?catalogId=48 — это
# официальный фид "New Cryptocurrency Listing". Содержит и SPOT-листинги
# ("Will List X"), и Futures-launch ("Will Launch XUSDT Perpetual Contract"),
# и шум (HODLer Airdrops, Pre-IPO/TradFi, Margin/Earn service-нотисы).
# Двухступенчатый фильтр (whitelist phrases + blacklist negatives) +
# извлечение тикера из (TICKER) или из XUSDT/XUSDC.
# Цель: −1-5с против TG-мирроров Binance (анонс приходит на bapi
# одновременно или раньше публикации в TG-каналы).
# ══════════════════════════════════════════════════════════════════
BINANCE_NOTICE_BASE_URL = (
    "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
)
BINANCE_NOTICE_CATALOG_ID = 48

# FIX: 500мс (2 RPS) стабильно банило IP по 429 в первые ~24с после старта
# (по логам: init-запрос 200 OK, потом ~48 поллов 500мс → 429 навсегда).
# /bapi/composite — это WEB-фронт endpoint за анти-бот WAF (не документ.
# REST API с лимитом 1200 req/min!), особенно жёсткий к datacenter-IP.
# Тот же урок уже закодирован в parser_delist.py: безопасно ~3с = ~20 req/min.
# Env BINANCE_NOTICE_POLL_INTERVAL=Xс позволяет тюнить без передеплоя.
BINANCE_NOTICE_POLL_INTERVAL = float(os.getenv("BINANCE_NOTICE_POLL_INTERVAL", "5.0"))
# Анти-стадо: ±jitter к каждому sleep, размывает пики req/с на стороне Binance.
BINANCE_NOTICE_JITTER = 0.10
# При HTTP 429 — длинная пауза, иначе долбёжка каждые 3с держит бан живым
# (каждый запрос в окне бана продлевает окно). 30с даёт WAF разбаниться.
BINANCE_NOTICE_BACKOFF_429 = float(os.getenv("BINANCE_NOTICE_BACKOFF_429", "30.0"))
# FIX: адаптивная защита от стойкого IP-бана. После N подряд 429 поллер
# уходит в длинный cooldown (даём WAF реально разбаниться). При каждом бане
# базовый интервал растёт на STEP (5-10%) до потолка MAX; успешный ответ
# сбрасывает интервал и счётчик банов к базовым значениям.
BINANCE_NOTICE_BAN_THRESHOLD = int(os.getenv("BINANCE_NOTICE_BAN_THRESHOLD", "20"))   # N подряд 429 → cooldown
BINANCE_NOTICE_COOLDOWN = float(os.getenv("BINANCE_NOTICE_COOLDOWN", "600.0"))         # длинная пауза (10 мин)
BINANCE_NOTICE_INTERVAL_STEP = float(os.getenv("BINANCE_NOTICE_INTERVAL_STEP", "0.07"))  # +7% на каждый бан
BINANCE_NOTICE_INTERVAL_MAX = float(os.getenv("BINANCE_NOTICE_INTERVAL_MAX", "30.0"))    # потолок интервала
# Cache-busting: CloudFront кеширует по query-ключу. Ротация pageSize ∈ набора
# даёт разные cache key. ВАЖНО: нельзя добавлять «левые» query-параметры
# (_t/timestamp и т.п.) — WAF режет их в 400 (урок из delist-инцидента).
# Разрешены только type/catalogId/pageNo/pageSize.
BINANCE_NOTICE_PAGE_SIZES = (10, 15, 20)


def _binance_notice_url() -> str:
    """Cache-busting URL: ротация pageSize, только whitelisted query-параметры."""
    page_sz = random.choice(BINANCE_NOTICE_PAGE_SIZES)
    return (
        f"{BINANCE_NOTICE_BASE_URL}"
        f"?type=1&pageNo=1&pageSize={page_sz}&catalogId={BINANCE_NOTICE_CATALOG_ID}"
    )


# Phrases которые ДОЛЖНЫ быть в title для трейда. Lowercase-сравнение.
_BINANCE_LISTING_POS = (
    "will list",
    "will launch",
)

# Если есть хоть один такой — skip (override whitelist). Lowercase.
_BINANCE_LISTING_NEG = (
    "hodler airdrops",       # promo с airdrop'ами, не настоящий листинг
    "on earn",               # "Will Add X on Earn" — сервис, не листинг
    "earn,",                 # вариант разделителя в той же фразе
    "locked product",
    "locked products",
    "flexible product",
    "pre-ipo",               # фьючерсы на акции до IPO — не крипта
    "tradfi",                # TradFi-перпы на стоки — не крипта
    "margin will add",       # margin pair add — не SPOT/Futures листинг
    "trading bots",          # bot service
    "convert",               # convert pair add
    "buy crypto",            # service
    "vip loan",              # service
    "multiple usd",          # bulk-add нескольких TradFi за раз
    "new trading pairs",     # service notification
    "% apr",
    "% apy",
    "launchpool",
    "subscribe",
    "promotion",
    "promotional",
    "rewards pool",
    "staking pool",
    "simple earn",
    "special offer",
)

_BINANCE_PAREN_RE = re.compile(r"\(([A-Z][A-Z0-9]{1,9})\)")
# XUSDT / XUSDC — фьючерсный символ, где X = базовая валюта.
_BINANCE_FUTURES_RE = re.compile(r"\b([A-Z][A-Z0-9]{1,9})(?:USDT|USDC)\b")

_BINANCE_NOTICE_BANNED = _BITHUMB_NOTICE_BANNED


def _extract_binance_notice_tickers(title: str) -> list[str]:
    """
    Возвращает тикеры из title если это настоящий листинг,
    или [] для promo/service/pre-IPO.
    """
    lower = title.lower()
    if not any(p in lower for p in _BINANCE_LISTING_POS):
        return []
    if any(n in lower for n in _BINANCE_LISTING_NEG):
        return []

    found: list[str] = []
    seen: set[str] = set()

    # Сначала из явных скобок (для SPOT-листингов).
    for tok in _BINANCE_PAREN_RE.findall(title):
        t = tok.upper()
        if t in _BINANCE_NOTICE_BANNED or t in seen:
            continue
        seen.add(t)
        found.append(t)

    # Из XUSDT/XUSDC если в скобках не нашли (futures-launch).
    if not found:
        for tok in _BINANCE_FUTURES_RE.findall(title):
            t = tok.upper()
            if t in _BINANCE_NOTICE_BANNED or t in seen:
                continue
            seen.add(t)
            found.append(t)

    return found


def run_binance_announcement_poller() -> None:
    # FIX-LATENCY: используем round-robin proxy pool. WAF /bapi/composite
    # очень чувствителен к одинаковому IP — на каждый запрос новый прокси
    # из пула, чтобы 429-streak не висел на одном IP. Заголовки уже
    # прописаны в _make_binance_session (UA Chrome, clientType, Referer).
    pool_size = len(_BINANCE_PROXY_POOL)
    log_ok("BINANCE-NOTICE", f"Старт (poll {BINANCE_NOTICE_POLL_INTERVAL:.1f}с, pool={pool_size})")

    # FIX-CRITICAL: инициализация ОБЯЗАТЕЛЬНА. Если init упадёт (429 или
    # сеть), last_max_id=0 и на ПЕРВОМ успешном поллинге всё что найдётся
    # на странице сочтётся "новым" → отстрел backlog'а на 5-7 дней.
    # Поэтому ретраим init до победы, а ДО неё в while-loop ниже не входим.
    last_max_id: int | None = None
    init_attempt = 0
    while last_max_id is None:
        init_attempt += 1
        try:
            resp = _next_binance_session().get(_binance_notice_url(), timeout=4)
            if resp.status_code == 429:
                log_warn("BINANCE-NOTICE",
                         f"init #{init_attempt}: 429 — пауза {BINANCE_NOTICE_BACKOFF_429:.0f}с (трейды БЛОКИРОВАНЫ до init OK)")
                time.sleep(BINANCE_NOTICE_BACKOFF_429)
                continue
            resp.raise_for_status()
            data = _json_loads(resp.content)
            catalogs = (data.get("data") or {}).get("catalogs") or []
            # FIX (review M4): catalogs[0] может быть None/не-dict.
            _c0 = catalogs[0] if (catalogs and isinstance(catalogs[0], dict)) else {}
            articles = _c0.get("articles", [])
            if articles:
                last_max_id = max(int(a.get("id", 0)) for a in articles)
            else:
                # Empty list — Binance вернул пусто. Странно, но не падаем
                # с откатом backlog'а: ставим эфемерно большой sentinel, чтобы
                # любой реально-новый id (>= 1 000 000) был >, а пустую страницу
                # перетрём при следующем успешном поллинге. Безопасно.
                last_max_id = 10**12
            log_ok("BINANCE-NOTICE",
                   f"Инициализирован после #{init_attempt}, max id={last_max_id} ({len(articles)} нотисов)")
        except Exception as e:
            log_warn("BINANCE-NOTICE",
                     f"init #{init_attempt} упал: {e!r} — retry через 10с (трейды БЛОКИРОВАНЫ)")
            time.sleep(10)

    # Адаптивное состояние: текущий интервал растёт при банах, счётчик
    # подряд идущих 429 триггерит cooldown при достижении порога.
    cur_interval = BINANCE_NOTICE_POLL_INTERVAL
    ban_streak = 0

    while True:
        try:
            # ±jitter, чтобы не бить Binance синхронно с другими поллерами.
            jitter = cur_interval * BINANCE_NOTICE_JITTER * (2 * random.random() - 1)
            time.sleep(max(0.05, cur_interval + jitter))
            t_send = time.perf_counter()
            resp = _next_binance_session().get(_binance_notice_url(), timeout=2)

            # 429 ловим ДО raise_for_status: долбёжка держит бан живым,
            # поэтому отступаем надолго — даём WAF разбаниться.
            if resp.status_code == 429:
                ban_streak += 1
                _hstat("throttle", "BINANCE-NOTICE")
                # Адаптивно растим базовый интервал (+STEP%) до потолка MAX.
                cur_interval = min(cur_interval * (1 + BINANCE_NOTICE_INTERVAL_STEP),
                                   BINANCE_NOTICE_INTERVAL_MAX)
                if ban_streak >= BINANCE_NOTICE_BAN_THRESHOLD:
                    # Стойкий бан — длинный cooldown, чтобы WAF реально снял
                    # блок. После cooldown сбрасываем streak (но НЕ интервал —
                    # пусть остаётся повышенным, мягче давим на IP).
                    log_err("BINANCE-NOTICE",
                            f"{ban_streak} подряд 429 → cooldown {BINANCE_NOTICE_COOLDOWN/60:.0f}мин "
                            f"(интервал теперь {cur_interval:.1f}с)")
                    time.sleep(BINANCE_NOTICE_COOLDOWN)
                    ban_streak = 0
                else:
                    log_err("BINANCE-NOTICE",
                            f"429 #{ban_streak} — пауза {BINANCE_NOTICE_BACKOFF_429:.0f}с "
                            f"(интервал {cur_interval:.1f}с)")
                    time.sleep(BINANCE_NOTICE_BACKOFF_429)
                continue

            resp.raise_for_status()
            t_recv = time.perf_counter()

            # FIX (429-пила): НЕ сбрасываем интервал на одном успехе — иначе
            # success → cur_interval=3.0с → снова 429 → backoff → success →
            # ... бесконечная пила (видно в логах: 429 каждые ~минуту).
            # Вместо этого плавный decay (−10% за успех) до базового потолка,
            # и сброс ban_streak только после ПОДРЯД успехов. Так интервал,
            # выросший из-за банов, опускается медленно и находит равновесие
            # выше «банящего» 3.0с.
            ban_streak = 0
            if cur_interval > BINANCE_NOTICE_POLL_INTERVAL:
                cur_interval = max(BINANCE_NOTICE_POLL_INTERVAL,
                                   cur_interval * 0.90)

            data = _json_loads(resp.content)
            if data.get("code") != "000000":
                continue
            catalogs = (data.get("data") or {}).get("catalogs") or []
            if not catalogs:
                continue
            # FIX (review M4): catalogs[0] может быть None/не-dict.
            first_cat = catalogs[0] if isinstance(catalogs[0], dict) else {}
            articles = first_cat.get("articles", [])

            new_max = last_max_id
            for art in articles:
                if not isinstance(art, dict):
                    continue
                aid = int(art.get("id", 0))
                if aid <= last_max_id:
                    continue
                if aid > new_max:
                    new_max = aid

                title = art.get("title") or ""
                tickers = _extract_binance_notice_tickers(title)

                if not tickers:
                    log_info("BINANCE-NOTICE", f"skip id={aid} | {title[:80]}")
                    continue

                fetch_ms = (t_recv - t_send) * 1000
                log_ok(
                    "BINANCE-NOTICE",
                    f"LISTING id={aid} {tickers} ({fetch_ms:.0f}мс) | {title[:80]}",
                )
                process_signal(tickers, "BINANCE-NOTICE", t_start=t_send)

            last_max_id = new_max

        except Exception as e:
            # FIX (review M0): тип ошибки → отличить parse от network в логе.
            _log_poll_error_throttled("BINANCE-NOTICE", e)
            _hstat("error", "BINANCE-NOTICE")
            time.sleep(POLL_ERROR_BACKOFF)


# ══════════════════════════════════════════════════════════════════
# ИСТОЧНИК 4: Binance Futures — polling новых linear-пар
# Ловит листинг БЕЗ анонса (например Binance листит сразу без notice).
# ══════════════════════════════════════════════════════════════════

def _load_binance_futures_tickers(session: requests.Session) -> set[str]:
    """
    Возвращает множество base-валют (BTC, ETH, ...) из активных linear-пар
    Binance futures. Фильтр: status == TRADING, quote ∈ {USDT, USDC}.

    FIX-PERF: парсинг через msgspec.Struct (см. _parse_binance_exinfo) —
    typed access, ~40% быстрее vs orjson+dict.get на 600+ символах.
    """
    use_msgspec = _parse_binance_exinfo is not None

    for attempt, timeout in enumerate([3, 6, 12], 1):
        try:
            resp = session.get(BINANCE_FAPI_URL, timeout=timeout)
            resp.raise_for_status()
            tickers: set[str] = set()

            if use_msgspec:
                for s in _parse_binance_exinfo(resp.content):
                    if s.status != "TRADING":
                        continue
                    if s.contractType != "PERPETUAL":
                        continue
                    if s.quoteAsset in ("USDT", "USDC") and s.baseAsset:
                        tickers.add(s.baseAsset.upper())
            else:
                data = _json_loads(resp.content)
                for s in data.get("symbols", []) or []:
                    if not isinstance(s, dict):
                        continue
                    if s.get("status") != "TRADING":
                        continue
                    if s.get("contractType") != "PERPETUAL":
                        continue
                    quote = s.get("quoteAsset")
                    base  = s.get("baseAsset")
                    if quote in ("USDT", "USDC") and isinstance(base, str) and base:
                        tickers.add(base.upper())
            return tickers
        except Exception:
            if attempt == 3:
                raise
            time.sleep(attempt * 2)
    return set()


def run_binance_futures_poller() -> None:
    """
    Polling Binance futures /fapi/v1/exchangeInfo каждые ~500мс.
    Замечает новые тикеры (которых не было в предыдущем снимке) и шлёт
    их в process_signal как source="BINANCE". L2-дедуп по (coin, BINANCE)
    гарантирует, что после открытия монета не отстрелит повторно при
    включении на других котировках/контрактах.
    """
    global _binance_last_ts
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    })

    log_ok("BINANCE", "Загружаем начальный список futures-тикеров...")
    try:
        known: set[str] = _load_binance_futures_tickers(session)
        log_ok("BINANCE", f"Загружено {len(known)} тикеров, жду новые "
                          f"(poll {BINANCE_POLL_INTERVAL*1000:.0f}мс)...")
        with _ts_lock:
            _binance_last_ts = time.monotonic()
    except Exception as e:
        log_err("BINANCE", f"Ошибка инициализации: {e}")
        known = set()

    ever_seen: set[str] = set(known)

    while True:
        try:
            time.sleep(BINANCE_POLL_INTERVAL)
            t_send = time.perf_counter()
            current = _load_binance_futures_tickers(session)
            t_recv = time.perf_counter()

            with _ts_lock:
                _binance_last_ts = time.monotonic()

            new_tickers = current - ever_seen

            # Защита от bulk-апдейта (рестарт API / временная подгрузка
            # списка после maintenance): >10 новых за тик — почти точно
            # not-a-listing event.
            if len(new_tickers) > 10:
                log_warn("BINANCE", f"Подозрительно много новых тикеров "
                                    f"({len(new_tickers)}), пропускаем")
                ever_seen |= current
                continue

            if new_tickers:
                fetch_ms = (t_recv - t_send) * 1000
                log_ok("BINANCE", f"Новые тикеры: {new_tickers} (fetch={fetch_ms:.0f}мс)")
                ever_seen |= new_tickers
                process_signal(list(new_tickers), "BINANCE", t_start=t_send)

        except Exception as e:
            log_err("BINANCE", f"Ошибка поллера: {e}")
            time.sleep(POLL_ERROR_BACKOFF)


# ══════════════════════════════════════════════════════════════════
# WATCHDOG — перезапускает зависшие поллеры
# ══════════════════════════════════════════════════════════════════

def _watchdog() -> None:
    """
    Каждые 30 секунд проверяет что Upbit / Bithumb / Binance поллеры живы.
    FIX: не плодит дубли — если предыдущий поток ещё жив, не запускает новый.
    """
    global _upbit_last_ts, _bithumb_last_ts, _binance_last_ts
    global _upbit_thread, _bithumb_thread, _binance_thread

    time.sleep(30)

    while True:
        time.sleep(30)
        now = time.monotonic()

        with _ts_lock:
            upbit_age   = now - _upbit_last_ts
            bithumb_age = now - _bithumb_last_ts
            binance_age = now - _binance_last_ts

        if upbit_age > WATCHDOG_TIMEOUT:
            with _thread_lock:
                alive = _upbit_thread is not None and _upbit_thread.is_alive()
            if not alive:
                log_err("WATCHDOG", f"Upbit поллер завис ({upbit_age:.0f}с) — перезапускаем")
                tg_log(f"⚠️ <b>WATCHDOG</b>: Upbit поллер завис {upbit_age:.0f}с, перезапуск")
                with _ts_lock:
                    _upbit_last_ts = now
                t = threading.Thread(target=run_upbit_poller, daemon=True, name="upbit-poller")
                t.start()
                with _thread_lock:
                    _upbit_thread = t
            else:
                # Поток жив, но не пишет timestamp — что-то завис.
                # Не убиваем (Python не умеет), просто сбрасываем timestamp.
                log_warn("WATCHDOG", f"Upbit поллер не отвечает {upbit_age:.0f}с, но поток жив — ждём")
                with _ts_lock:
                    _upbit_last_ts = now

        if bithumb_age > WATCHDOG_TIMEOUT:
            with _thread_lock:
                alive = _bithumb_thread is not None and _bithumb_thread.is_alive()
            if not alive:
                log_err("WATCHDOG", f"Bithumb поллер завис ({bithumb_age:.0f}с) — перезапускаем")
                tg_log(f"⚠️ <b>WATCHDOG</b>: Bithumb поллер завис {bithumb_age:.0f}с, перезапуск")
                with _ts_lock:
                    _bithumb_last_ts = now
                t = threading.Thread(target=run_bithumb_poller, daemon=True, name="bithumb-poller")
                t.start()
                with _thread_lock:
                    _bithumb_thread = t
            else:
                log_warn("WATCHDOG", f"Bithumb поллер не отвечает {bithumb_age:.0f}с, но поток жив — ждём")
                with _ts_lock:
                    _bithumb_last_ts = now

        if binance_age > WATCHDOG_TIMEOUT:
            with _thread_lock:
                alive = _binance_thread is not None and _binance_thread.is_alive()
            if not alive:
                log_err("WATCHDOG", f"Binance futures поллер завис ({binance_age:.0f}с) — перезапускаем")
                tg_log(f"⚠️ <b>WATCHDOG</b>: Binance futures поллер завис {binance_age:.0f}с, перезапуск")
                with _ts_lock:
                    _binance_last_ts = now
                t = threading.Thread(target=run_binance_futures_poller, daemon=True, name="binance-poller")
                t.start()
                with _thread_lock:
                    _binance_thread = t
            else:
                log_warn("WATCHDOG", f"Binance futures поллер не отвечает {binance_age:.0f}с, но поток жив — ждём")
                with _ts_lock:
                    _binance_last_ts = now


# ══════════════════════════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════════════════════════

# ── Graceful shutdown: flush статистики на диск перед остановкой ───
# FIX 2026-06-06: docker при рестарте шлёт SIGTERM → daemon-потоки
# (_source_stats_persist_loop / _health.persist_loop / _sig_stats.flush_loop /
# _fired_persist_loop) убиваются мгновенно, не успев записать накопленное.
# Из-за этого статистика «сбрасывалась» при перезапуске. Здесь синхронно
# сбрасываем всё на диск по SIGTERM/SIGINT/atexit. Идемпотентно (флаг).
_shutdown_done = threading.Event()


def _graceful_shutdown(*_a) -> None:
    if _shutdown_done.is_set():
        return
    _shutdown_done.set()
    print("[SHUTDOWN] flush статистики на диск...", flush=True)
    for label, fn in (
        ("fired-state", _persist_fired_state),
        ("source-stats", _persist_source_stats),
        ("health", (lambda: _health.persist()) if _health is not None else None),
        ("sig-stats", (lambda: _sig_stats._flush()) if _sig_stats is not None else None),
        ("price-paths", _recorder.flush_active if _recorder is not None else None),
    ):
        if fn is None:
            continue
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            print(f"[SHUTDOWN] {label} flush failed: {e!r}", flush=True)
    print("[SHUTDOWN] flush завершён", flush=True)


def _install_shutdown_handlers() -> None:
    atexit.register(_graceful_shutdown)

    def _on_signal(signum, _frame):
        _graceful_shutdown()
        sys.exit(0)

    for _sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(_sig, _on_signal)
        except (ValueError, OSError):
            pass  # не главный поток / платформа без сигнала


if __name__ == "__main__":
    # FIX-batch-1: uvloop — asyncio event loop в 2-4x быстрее.
    # FIX: install() deprecated с 0.18+, используем set_event_loop_policy.
    try:
        import uvloop  # type: ignore[import-not-found]
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("[BOOT] uvloop активирован")
    except ImportError:
        print("[BOOT] uvloop не установлен, использую стандартный asyncio")

    # NOTE: switchinterval оставлен дефолтный (5мс). Раньше тут было
    # `sys.setswitchinterval(0.001)` — попытка снизить max GIL hold.
    # Эмпирически это давало РЕГРЕССИЮ p50 на trade-открытии (27мс vs 9мс):
    # под нагрузкой (price_updater Bybit+Gate, telethon polling, WS heartbeat,
    # 5 worker-thread'ов) 1мс switch → слишком частые context-switches →
    # каждый thread тратит больше времени на GIL acquire/release pingpong,
    # а worker'у нужно несколько раз перехватить GIL за один трейд
    # (handler thread → WS-loop thread → handler thread). 5мс позволяет
    # каждой стадии завершиться атомарно и быстрее отдать управление дальше.

    # FIX-PERF: только gc.freeze() — все module-level объекты (regex,
    # imported classes, globals) переезжают в permanent gen и больше не
    # сканируются ни на одном GC-цикле. Дефолтные thresholds=(700, 10, 10)
    # оставляем: gen0 sweep каждые ~700 аллокаций обходит горстку объектов
    # за десятки микросекунд. Подъём порога до 50k делал sweep'ы редкими,
    # но КАЖДЫЙ ИЗ НИХ стал ~3-15мс stop-the-world — это p99 регрессия
    # для hot-path (зафиксировано: 33мс trade vs 9мс на default'ах).
    import gc
    gc.freeze()
    print(f"[BOOT] GC frozen: {gc.get_freeze_count()} objects (thresholds={gc.get_threshold()})")

    # FIX 2026-06-06: ставим shutdown-хендлеры в главном потоке — flush
    # статистики на диск при SIGTERM/SIGINT (docker restart) и atexit.
    _install_shutdown_handlers()

    threading.Thread(target=price_updater,      daemon=True).start()
    threading.Thread(target=gate_price_updater, daemon=True).start()
    log_ok("CACHE", "price_updater (Bybit + Gate.io) запущен в фоне")
    warmup_bybit_connection()
    warmup_gate_connection()
    preload_lot_steps()
    gate_preload_lot_steps()
    start_bybit_heartbeat()

    # FIX-LATENCY (Patch #1): фоновый sweep _gate_set_leverage по всем
    # gate_known_coins. После первого прохода _leverage_set_for будет
    # содержать ~300 контрактов; market_open_long при Gate.io fallback
    # пропустит 150мс HTTP POST на установку плеча.
    threading.Thread(
        target=gate_leverage_presetter,
        daemon=True,
        name="gate-leverage-preset",
    ).start()
    log_ok("CACHE", "gate_leverage_presetter запущен (sweep 1ч)")

    # FIX-LATENCY (Patch #2): инициализируем пул сессий для notice-поллеров
    # ДО старта поллеров. С Korean datacenter IP'ами BITHUMB/UPBIT-notice
    # интервалы автоматически уплотняются (см. _adjust_notice_intervals_for_pool).
    pool_size = _init_listing_proxy_pool()
    _adjust_notice_intervals_for_pool()
    if pool_size > 1:
        log_ok(
            "PROXY",
            f"LISTING proxy pool: {pool_size} sessions "
            f"(notice intervals → Bithumb {BITHUMB_NOTICE_POLL_INTERVAL*1000:.0f}мс, "
            f"Upbit {UPBIT_ANNOUNCEMENT_POLL_INTERVAL*1000:.0f}мс)",
        )
    else:
        log_info("PROXY", "LISTING_PROXIES пуст — поллеры на прямом подключении (default intervals)")

    # FIX (регресс): запускаем фоновый connect+auth Bybit WS Trade ДО
    # блокирующего прогрева notice-сессий, чтобы WS-handshake (manager-thread)
    # шёл ПАРАЛЛЕЛЬНО с прогревом (~4с полезной работы). К моменту is_ready
    # ниже соединение уже почти готово — больше не упираемся в дедлайн.
    _pre_sync_inst = None
    _pre_async_inst = None
    if BYBIT_WS_TRADE_ENABLED and BYBIT_API_KEY and BYBIT_SECRET_KEY:
        try:
            if BYBIT_SYNC_WS_ENABLED:
                from api import bybit_sync_ws_trade as _sync_mod
                _pre_sync_inst = _sync_mod.init(BYBIT_API_KEY, BYBIT_SECRET_KEY)
            from api.bybit_ws_trade import init as _bybit_ws_init
            _pre_async_inst = _bybit_ws_init(BYBIT_API_KEY, BYBIT_SECRET_KEY)
        except Exception as e:  # noqa: BLE001
            log_warn("PARSER", f"Bybit WS pre-init упал: {e!r} — init произойдёт ниже")
            _pre_sync_inst = None
            _pre_async_inst = None

    # FIX-LATENCY: прогрев notice-сессий — устанавливаем TCP+TLS ко всем
    # трём endpoint'ам ЗАРАНЕЕ, параллельно. Без этого ПЕРВЫЙ боевой запрос
    # к Upbit/Bithumb/Binance ест cold-TLS handshake (несколько RTT до
    # Сеула = 300-500мс, видно в логах: discovery 1463мс). Прогретые
    # сессии переиспользуют соединение (keep-alive adapter) → первый
    # реальный листинг ловится на тёплом сокете.
    _warmup_listing_sessions()

    # FIX-batch-5: Bybit V5 WS Trade — persistent connection для ордеров.
    if BYBIT_WS_TRADE_ENABLED and BYBIT_API_KEY and BYBIT_SECRET_KEY:
        # FIX-PERF: пробуем сначала SYNC вариант (api/bybit_sync_ws_trade.py).
        # Если он успешно подключился и аутентифицировался — используем его как
        # основной hot-path (place_order_ws_fast маршрутизирует через него).
        # Async-инстанс инициализируется как fallback на случай sync-disconnect.
        sync_ready = False
        if BYBIT_SYNC_WS_ENABLED:
            try:
                from api import bybit_sync_ws_trade as _sync_mod
                from api.bybit_ws_trade import use_sync_ws
                sync_inst = _sync_mod.init(BYBIT_API_KEY, BYBIT_SECRET_KEY)
                # FIX (регресс «SYNC WS не подключился за 3с»): cold WS+TLS
                # handshake до Bybit из SG иногда >3с (auth OK приходил уже
                # ПОСЛЕ дедлайна → fallback на REST на весь сеанс). WS Trade —
                # критичный hot-path (−15-30мс/ордер), ждём дольше: 8с.
                if sync_inst.is_ready(wait_sec=8.0):
                    sync_warm = sync_inst.warmup()
                    use_sync_ws(sync_inst)
                    _sync_mod.start_periodic_warmup()
                    sync_ready = True
                    suffix = "+ прогрет" if sync_warm else "(warmup не прошёл)"
                    log_ok("PARSER", f"Bybit SYNC WS Trade готов {suffix} ✓ (no cross-thread)")
                else:
                    log_warn("PARSER", "Bybit SYNC WS не подключился за 8с — fallback на async")
            except Exception as e:
                log_warn("PARSER", f"Bybit SYNC WS init упал: {e!r} — fallback на async")

        # Async-инстанс: если sync уже работает, async всё равно нужен как
        # fallback при sync reconnect. Если sync не запустился — async основной.
        try:
            from api.bybit_ws_trade import init as bybit_ws_init, start_periodic_warmup
            inst = bybit_ws_init(BYBIT_API_KEY, BYBIT_SECRET_KEY)
            # FIX (регресс): если sync уже основной — async лишь fallback,
            # не блокируем boot надолго (1с). Если sync НЕ готов — async
            # становится основным hot-path, ждём полноценные 8с (cold WS+TLS).
            _async_wait = 1.0 if sync_ready else 8.0
            if inst.is_ready(wait_sec=_async_wait):
                if not sync_ready:
                    # Async — основной hot-path. Прогреваем как раньше.
                    if inst.warmup():
                        log_ok("PARSER", "Bybit ASYNC WS Trade готов + прогрет ✓")
                    else:
                        log_ok("PARSER", "Bybit ASYNC WS Trade готов ✓ (warmup не прошёл)")
                    start_periodic_warmup()
                else:
                    log_ok("PARSER", "Bybit ASYNC WS готов (резерв на случай sync-disconnect)")
            else:
                log_warn("PARSER", "Bybit ASYNC WS не подключился за 8с — fallback на REST")
        except Exception as e:
            log_err("PARSER", f"Bybit ASYNC WS init упал: {e!r} — будет REST")

        # FIX 2026-06-19 (R3): private WS (order+position). Без wallet.
        # Используется в _set_tp_sl_bybit для замены REST poll'инга позиции.
        from config.config import BYBIT_WS_PRIVATE_ENABLED
        if BYBIT_WS_PRIVATE_ENABLED:
            try:
                from api import bybit_ws_private as _priv_mod
                priv_inst = _priv_mod.init(BYBIT_API_KEY, BYBIT_SECRET_KEY)
                if priv_inst.is_ready(wait_sec=5.0):
                    log_ok("PARSER", "Bybit PRIVATE WS (order+position) готов ✓")
                else:
                    log_warn("PARSER", "Bybit PRIVATE WS не подключился за 5с — fallback на REST poll")
            except Exception as e:
                log_warn("PARSER", f"Bybit PRIVATE WS init упал: {e!r} — fallback на REST poll")
    else:
        log_info("PARSER", "Bybit WS Trade отключён — используем REST")

    log_ok("PARSER", "Ждём 5с пока price_cache наполнится...")
    time.sleep(5)

    # Загружаем L2-дедуп с диска до запуска поллеров — иначе первый тик
    # после рестарта может повторно отстрелить уже отторгованную монету.
    _load_fired_state()
    # FIX-LATENCY (Patch #3): загружаем накопленную source-first статистику.
    _load_source_stats()

    # FIX-PERF: pre-warm executors + chain warmup — ДО запуска поллеров и
    # callback'ов. Иначе если poller сразу детектит новый тикер на старте,
    # первый листинг попадает на cold-path (27мс наблюдалось).
    #
    # pre-warm executors: N=40 на каждый — submit-call тоже bytecode
    # (LOAD_ATTR, CALL), специализируется в CPython adaptive interpreter
    # после ~32 проходов.
    _warm = [_signal_executor.submit(lambda: None) for _ in range(40)]
    _warm += [_tp_sl_executor.submit(lambda: None) for _ in range(40)]
    for f in _warm:
        f.result()
    log_ok("PARSER", "_signal_executor + _tp_sl_executor pre-warmed (40+40)")

    # Chain warmup: прогрев всей Python-цепочки сигнал → ws.send.
    # Без этого первый листинг платит +15-20мс на PEP-659 cold-specialization
    # (CPython 3.12+, см. https://peps.python.org/pep-0659/).
    # Что прогревается:
    #   1. find_listing_pairs(sample_text) — регексы _RE_LISTING_TG и co.
    #   2. market_open_long Python-путь (listing_api.warmup_chain) —
    #      get_price, _get_qty_step, _round_qty, dict-build из 14 полей,
    #      json.dumps, ws.send (диверсия в cancel-fake — никаких ордеров).
    # ~600мс на бутстрапе → первый реальный листинг 5-9мс вместо 27мс cold.
    try:
        # FIX 2026-07-07 (INVERT): листинг открывает ШОРТ → греем short-путь
        # (delist_api.warmup_chain), а не лонговый.
        from api.delist_api import warmup_chain as _short_warmup_chain
        sample_signal = "[BITHUMB] $BTC listed on Bithumb"
        for _ in range(30):
            find_listing_pairs(sample_signal)
        ok = _short_warmup_chain(n=30)
        log_ok("PARSER", f"Chain warmup: regex×30 + market_open_short path×{ok}/30 ✓")
    except Exception as e:  # noqa: BLE001
        log_warn("PARSER", f"Chain warmup упал: {e!r} — первый листинг будет медленнее")

    # Регистрируем CoinListing-сигналы в общем дедупе (он шёл мимо).
    try:
        from api import coinlisting_ws as _cl_mod
        def _coinlisting_callback(tickers: list[str], source: str, t_signal: float) -> None:
            process_signal(tickers, source, t_start=t_signal)
        _cl_mod.set_signal_callback(_coinlisting_callback)
        log_ok("PARSER", "CoinListing WS подключён к общему L1+L2 дедупу")
    except Exception as e:  # noqa: BLE001
        log_warn("PARSER", f"Не удалось привязать CoinListing callback: {e!r}")

    # FIX: сохраняем ссылки на потоки для watchdog
    _upbit_thread   = threading.Thread(target=run_upbit_poller,   daemon=True, name="upbit-poller")
    _bithumb_thread = threading.Thread(target=run_bithumb_poller, daemon=True, name="bithumb-poller")
    _binance_thread = threading.Thread(target=run_binance_futures_poller, daemon=True, name="binance-poller")
    _upbit_thread.start()
    _bithumb_thread.start()
    _binance_thread.start()
    threading.Thread(target=run_coinlisting, daemon=True, name="coinlisting-ws").start()

    # FIX: announcement-поллеры — собственный быстрый аналог CoinListing
    # для двух корейских бирж. Дёргают anouncement-каталоги напрямую:
    #   Bithumb — api.bithumb.com/v1/notices (готовый list)
    #   Upbit   — api-manager.upbit.com/api/v1/announcements/{id} (по next_id)
    # Цель: −1-2с до сигнала против MASKED-path CoinListing.
    threading.Thread(
        target=run_bithumb_announcement_poller,
        daemon=True,
        name="bithumb-notice-poller",
    ).start()
    # FIX (2026-06-04): Upbit direct-поллер ОТКЛЮЧЁН по умолчанию. На текущем
    # datacenter proxy-пуле (pool=4) Cloudflare на api-manager.upbit.com банит
    # все IP наглухо — за прогон 4712 CF-1015 троттлов, 0 успешных ответов,
    # 0 пойманных листингов. Backoff/breaker это не лечат (проблема в самих
    # флагнутых IP, не в частоте). FIX 2026-07-07: Seoul-relay удалён —
    # Upbit-покрытие держат TG + TOA (+ CoinListing при валидном ключе).
    # Включить обратно: UPBIT_DIRECT_POLLER_ENABLED=1 (нужны корейские
    # residential прокси, иначе снова throttle-storm).
    if UPBIT_DIRECT_POLLER_ENABLED:
        threading.Thread(
            target=run_upbit_announcement_poller,
            daemon=True,
            name="upbit-notice-poller",
        ).start()
    else:
        log_warn("UPBIT-NOTICE", "direct-поллер ОТКЛЮЧЁН (UPBIT_DIRECT_POLLER_ENABLED=0) "
                                 "— Upbit идёт через TG + TOA + CoinListing")
    threading.Thread(
        target=run_binance_announcement_poller,
        daemon=True,
        name="binance-notice-poller",
    ).start()

    log_ok("PARSER", f"Upbit/Bithumb ({POLL_INTERVAL*1000:.0f}мс) + Binance futures "
                     f"({BINANCE_POLL_INTERVAL*1000:.0f}мс) + CoinListing WS + "
                     f"Notice-поллеры (Bithumb {BITHUMB_NOTICE_POLL_INTERVAL*1000:.0f}мс, "
                     f"Upbit {UPBIT_ANNOUNCEMENT_POLL_INTERVAL*1000:.0f}мс, "
                     f"Binance {BINANCE_NOTICE_POLL_INTERVAL:.1f}с) запущены")

    threading.Thread(target=_watchdog, daemon=True, name="watchdog").start()
    log_ok("PARSER", f"Watchdog запущен (таймаут {WATCHDOG_TIMEOUT}с)")

    # FIX-PERF: глобальный sweeper вместо thread-per-claim (см. _try_claim).
    threading.Thread(target=_fired_sweeper, daemon=True, name="fired-sweeper").start()
    # FIX-PERF: единый фоновый L2-writer (dirty-flag + 1с batching) вместо
    # thread.start на каждый _mark_opened в hot-path.
    threading.Thread(target=_fired_persist_loop, daemon=True, name="fired-persist").start()
    # FIX-LATENCY (Patch #3): персистенс source-first stats + периодический summary.
    threading.Thread(
        target=_source_stats_persist_loop, daemon=True, name="stats-persist",
    ).start()
    threading.Thread(
        target=_source_stats_summary_loop, daemon=True, name="stats-summary",
    ).start()
    log_ok("STATS", "source-first телеметрия активна (persist 5мин, summary 6ч)")

    # Health-stats: только персистентность метрик. FIX 2026-06-18: TG-отчёт
    # «слабые места» каждые 6ч отключён по запросу. Сбор _hstat и persist_loop
    # остались (метрики копятся на диск).
    if _health is not None:
        threading.Thread(
            target=_health.persist_loop, kwargs={"batch_sec": 30.0},
            daemon=True, name="health-persist",
        ).start()
        log_ok("HEALTH", "сбор диагностики листинга активен (TG-отчёт отключён)")

    # Price-recorder: захват «головы» входа (первые 10 мин) для бэктеста выходов.
    if _recorder is not None:
        threading.Thread(
            target=_recorder.run_loop, daemon=True, name="price-recorder",
        ).start()
        log_ok("PRICE-REC", "рекордер головы входа активен (10 мин, klines-хвост)")
    # Eval-поллер: через 6ч дотягивает klines, оценивает стратегии, шлёт итоги.
    if _strat_evaluate is not None:
        threading.Thread(target=_eval_loop, daemon=True, name="strat-eval").start()

    # Signal-stats: журнал событий (flush 10с). Авто-отчёт 09:00/22:00 МСК
    # теперь шлёт лог-бот одним сообщением (оба парсера) — см. tg/log_bot.py.
    if _sig_stats is not None:
        threading.Thread(
            target=_sig_stats.flush_loop, kwargs={"batch_sec": 10.0},
            daemon=True, name="sigstats-flush",
        ).start()
        log_ok("STATS", "signal-stats журнал активен")

    # Closed-PnL поллер Bybit — дописывает PnL закрытых позиций в статистику.
    if _pnl_poller is not None and _pnl_poller.enabled:
        threading.Thread(
            target=_pnl_poller.poll_loop, daemon=True, name="bybit-pnl",
        ).start()
        log_ok("STATS", "closed-PnL поллер Bybit активен (PnL в статистику)")
    # Closed-PnL поллер Gate — реальный PnL Gate-сделок (раньше «н/д»).
    if _gate_pnl_poller is not None and _gate_pnl_poller.enabled:
        threading.Thread(
            target=_gate_pnl_poller.poll_loop, daemon=True, name="gate-pnl",
        ).start()
        log_ok("STATS", "closed-PnL поллер Gate активен (PnL в статистику)")

    # FIX-batch-4: Tree of Alpha free WS — параллельный источник листингов.
    if TREE_OF_ALPHA_WS_ENABLED:
        try:
            from api.treeofalpha_ws import run_tree_of_alpha_listener
            threading.Thread(
                target=run_tree_of_alpha_listener,
                kwargs={"listing_callback": _on_toa_listing},
                daemon=True,
                name="toa-ws",
            ).start()
            log_ok("PARSER", "Tree of Alpha WS запущен (free public feed)")
        except Exception as e:
            log_err("PARSER", f"Не удалось запустить TOA WS: {e}")

    # FIX 2026-07-07: Seoul relay (edge-нода) УДАЛЁН по решению — сервис был
    # мёртв (вечный reconnect), выигрывал всего 5 сигналов за всю статистику,
    # TOA-WS + CoinListing + TG перекрывают. Код: api/seoul_relay_receiver.py
    # удалён, серверная часть seoul_relay/ остановлена на VPS.

    log_ok("PARSER", "Запускаем Telegram listener (нужна авторизация при первом запуске)...")
    tg_log("🚀 <b>LISTING парсер запущен</b>\nUpbit + Bithumb + Telegram + Watchdog")

    # FIX: heartbeat — для docker healthcheck + TG алерт раз в 2 часа.
    def heartbeat():
        # FIX (review L1): window-based — ровно 1 алерт на 2ч-окно.
        _last_alert_window = -1
        while True:
            _touch_heartbeat()
            time.sleep(60)
            window = int(time.time()) // 7200
            if window != _last_alert_window:
                _last_alert_window = window
                tg_log("✅ <b>LISTING парсер работает</b>")

    threading.Thread(target=heartbeat, daemon=True, name="heartbeat").start()

    # FIX: TG listener — в while True с автоперезапуском (раньше падал и не вставал).
    while True:
        try:
            run_telegram_listener()
        except KeyboardInterrupt:
            _graceful_shutdown()
            break
        except Exception as e:
            log_err("PARSER", f"TG listener упал: {e} — перезапуск через 10с")
            tg_log(f"⚠️ <b>LISTING</b>: TG listener упал, перезапуск через 10с\n{e}")
        time.sleep(10)