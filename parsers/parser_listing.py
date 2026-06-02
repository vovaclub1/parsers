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
import os
import random
import re
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
    SEOUL_RELAY_URL,                            # FIX-LATENCY (Patch #1): edge relay (auth внутри URL ?key=...)
)
import itertools  # FIX-LATENCY (Patch #2): round-robin для proxy pool

from api.listing_api import (
    find_listing_pairs,
    calculate_margin_for_listing,
    market_open_long,
    set_tp_sl_long,
    price_updater,
    gate_price_updater,
    warmup_bybit_connection,
    start_bybit_heartbeat,
    preload_lot_steps,
    gate_preload_lot_steps,
    warmup_gate_connection,
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
    "| listing", "binance listing", "bybit listing",
    # Корейские биржи (Upbit / Bithumb)
    "listed on upbit", "listed on bithumb",
    "listed on binance", "listed on bybit",
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
]

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


def _make_listing_session(proxy: str | None) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "User-Agent": "Mozilla/5.0"})
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
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


def _adjust_notice_intervals_for_pool() -> None:
    """С 3+ IP уменьшаем интервал notice-поллеров. Каждый IP видит ~10 req/s
    даже на самой агрессивной частоте, остаёмся в rate-limit."""
    global BITHUMB_NOTICE_POLL_INTERVAL, UPBIT_ANNOUNCEMENT_POLL_INTERVAL
    size = len(_LISTING_PROXY_POOL)
    if size >= 3:
        BITHUMB_NOTICE_POLL_INTERVAL = 0.04
        UPBIT_ANNOUNCEMENT_POLL_INTERVAL = 0.06
    elif size == 2:
        BITHUMB_NOTICE_POLL_INTERVAL = 0.06
        UPBIT_ANNOUNCEMENT_POLL_INTERVAL = 0.10
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
_global_fired: set[str] = set()                       # биржа не распознана / легаси
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
            if isinstance(gf, list):
                _global_fired.update(str(c) for c in gf if isinstance(c, str))
            for ex, coins in (data.get("per_exchange") or {}).items():
                if ex in _per_exchange_fired and isinstance(coins, list):
                    _per_exchange_fired[ex].update(
                        str(c) for c in coins if isinstance(c, str)
                    )
        per_ex_summary = ", ".join(f"{k}={len(v)}" for k, v in _per_exchange_fired.items())
        log_ok("DEDUP", f"L2 загружен: global={len(_global_fired)} | {per_ex_summary}")
    except Exception as e:  # noqa: BLE001
        log_warn("DEDUP", f"L2 load failed: {e!r} — стартуем с пустого")


def _persist_fired_state() -> None:
    """
    Атомарный сейв L2. Вызывается из worker callback после успешного open.
    File-level write — не критично к скорости, делаем sync (write+rename).
    """
    try:
        with _fired_lock:
            snapshot = {
                "global": sorted(_global_fired),
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
      • coin в _global_fired — легаси-записи + fallback для источников без
        привязки к бирже (TG/TOA без распознанной биржи); ИЛИ
      • coin уже стреляли на ТОЙ ЖЕ бирже (_per_exchange_fired[ex]).
    Другая биржа той же монеты — НЕ блок (новый рынок = новый памп).
    """
    if coin in _global_fired:
        return True
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
            if coin not in _global_fired:
                _global_fired.add(coin)
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
           source: str, retries: int = 3, exchange: str = "") -> None:
    for attempt in range(1, retries + 1):
        try:
            # FIX-PERF: пропускаем "Старт"-print на attempt 1. Это
            # форматированный print → stdout = ~1-3мс перед market_open_long
            # в hot-path. Открытие позиции важнее, чем factual лог "сейчас открываем".
            # На retry'ях лог остаётся — там диагностика нужна.
            if attempt > 1:
                log_info("WORKER", f"[{source}] Retry {attempt}/{retries} → {coin} | margin={margin} USDT")
            amount, entry_price = market_open_long(coin, margin)
            if not amount:
                log_warn("WORKER", f"{coin}: нет цены, повтор через 0.1с...")
                time.sleep(0.1)
                continue

            # FIX-PERF: failsafe SL+TP1 уже улетели в одной order.create-фрейме
            # (stopLoss/takeProfit полях). Эта submit'ка добавляет trailing-stop
            # 3.5% через trading-stop endpoint — не критично для failsafe, поэтому
            # делаем ДО открытия метрики (submit ~5-20μs не сдвинет open_ms).
            _tp_sl_executor.submit(set_tp_sl_long, coin, entry_price, amount)

            open_ms = (time.perf_counter() - t_start) * 1000
            # FIX-PERF: один print вместо двух — раньше OPEN-print + intermediate
            # work + LONG-print между метриками съедали ~4мс на stdout flush
            # (PYTHONUNBUFFERED=1, f-string с ANSI escape codes). Теперь open_ms
            # = total path time, делать второй замер бессмысленно (был бы +100μs).
            log_ok("OPEN", (
                f"[{source}] {coin} | ордер за {BOLD}{open_ms:.0f}мс{RESET}{GREEN} | "
                f"entry={entry_price} | amount={amount:.4f}"
            ))
            # FIX-PERF: tg_log теперь fire-and-forget (см. tg/tg_logger.py) —
            # возвращается за ~10мкс, реальный HTTP уходит в фоне.
            tg_log(f"🟢 <b>LISTING LONG</b> {coin}\nEntry: {entry_price}\nAmount: {amount:.4f}\nВремя: {open_ms:.0f}мс")

            # FIX-PERF: bookkeeping ПОСЛЕ метрики и tg_log — не должен влиять
            # на «время от сигнала до ордера». _mark_opened теперь только
            # set-add + Event.set (~2мкс), без thread.start.
            _mark_opened(coin, source, exchange)
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
                break
            if attempt < retries:
                # FIX: 0.1с → 0.05с. На retry-path основная задержка — это
                # сетевой round-trip, дополнительные 50мс не имеют смысла.
                # Также worker блокирует TG-handler thread (process_signal
                # вызывает worker inline для single-coin), поэтому 0.3с
                # суммарной блокировки урезаем до 0.15с.
                time.sleep(0.05)

    log_err("WORKER", f"{coin}: все {retries} попытки провалились")
    tg_log(f"⚠️Listing {coin}: все попытки провалились")


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
    # FIX-batch-8: пробрасываем t_start из WS-loop. Раньше perf_counter()
    # перезаписывался в process_signal и латентность от прихода сообщения
    # до ордера терялась.
    process_signal(pairs, "TOA-WS", t_start=t_start, exchange=exchange)


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
                # FIX-LATENCY (Patch #3): running stats отставаний.
                # Ключ (loser, winner) — loser=текущий поздний source,
                # winner=первый. Это даёт пары вида
                # ("TG:coin_listing", "TOA-WS") и позволяет ответить
                # на вопрос «насколько TOA опережает TG в среднем».
                dt_ms = dt * 1000.0
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
        for coin in new_pairs:
            _signal_executor.submit(worker, coin, margin, t_start, source,
                                    exchange=exchange)

    print(f"\n{BOLD}{'═' * 60}{RESET}")
    log_ok("LISTING", f"[{source}] Новый листинг!")
    log_info("LISTING", f"Монеты : {new_pairs}")
    log_info("TRADE",   f"Маржа={margin} USDT | открываем {len(new_pairs)} лонг(ов)...")
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
            if not any(p in tl for p in TG_LISTING_PHRASES):
                return
            if any(neg in tl for neg in TG_LISTING_NEG):
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
        for ch in channels:
            try:
                entity = await client.get_entity(ch)
                log_ok("TG", f"  + {getattr(entity, 'title', ch)!r} (@{getattr(entity, 'username', '?')}) ✓")
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
            resp = _next_listing_session().get(BITHUMB_NOTICES_URL, timeout=4)
            resp.raise_for_status()
            tmp: set[str] = set()
            for item in _json_loads(resp.content):
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
            # FIX-LATENCY: новая сессия (round-robin) на КАЖДЫЙ запрос —
            # размазывает нагрузку по IP'ам, минимизирует 429 от Bithumb.
            resp = _next_listing_session().get(BITHUMB_NOTICES_URL, timeout=2)
            resp.raise_for_status()
            t_recv = time.perf_counter()

            items = _json_loads(resp.content)
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
            log_err("BITHUMB-NOTICE", f"poll error: {e}")
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
UPBIT_ANNOUNCEMENT_LOOKAHEAD = 3

# Категории, которые ВЕДУТ к торговле. У Upbit поле "category" в JSON:
# наблюдали "거래" для трейд-нотисов (включая листинги, делисты, изменения).
_UPBIT_NOTICE_TRADE_CATEGORIES = {"거래"}

# Те же корейские стопы, что и для Bithumb.
_UPBIT_NOTICE_NEG_KEYWORDS = _BITHUMB_NOTICE_NEG_KEYWORDS

_UPBIT_NOTICE_TICKER_RE = _BITHUMB_NOTICE_TICKER_RE
_UPBIT_NOTICE_BANNED = _BITHUMB_NOTICE_BANNED


def _try_fetch_upbit_announcement(
    session: requests.Session,
    notice_id: int,
) -> tuple[str | None, str | None]:
    """
    Возвращает (title, category) если id существует и листинг,
    или (None, None) если 404 / делист / служебное.
    """
    try:
        url = UPBIT_ANNOUNCEMENT_URL.format(id=notice_id)
        resp = session.get(url, timeout=2)
        if resp.status_code == 404:
            return None, None
        resp.raise_for_status()
        data = _json_loads(resp.content)
    except Exception:
        return None, None

    if not isinstance(data, dict) or not data.get("success"):
        return None, None
    payload = data.get("data") or {}
    title = payload.get("title") or ""
    category = payload.get("category") or ""
    return title, category


def _upbit_id_exists(session: requests.Session, notice_id: int) -> bool:
    """True если /announcements/{id} вернул success=true (что угодно)."""
    title, _ = _try_fetch_upbit_announcement(session, notice_id)
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

    while True:
        try:
            advanced = False
            for offset in range(UPBIT_ANNOUNCEMENT_LOOKAHEAD):
                probe_id = next_id + offset
                t_send = time.perf_counter()
                # FIX-LATENCY: round-robin сессии на каждый probe.
                title, category = _try_fetch_upbit_announcement(_next_listing_session(), probe_id)
                t_recv = time.perf_counter()

                if title is None:
                    # 404 — id ещё не выпущен. Без lookahead'а — стоп.
                    if offset == 0:
                        break
                    continue

                # id существует. Двигаемся вперёд независимо от того,
                # листинг это или нет.
                next_id = probe_id + 1
                advanced = True

                # Фильтры: категория должна быть "거래", и title без
                # негативных корейских ключей.
                if category not in _UPBIT_NOTICE_TRADE_CATEGORIES:
                    log_info("UPBIT-NOTICE", f"skip id={probe_id} cat={category} | {title[:60]}")
                    continue
                if any(kw in title for kw in _UPBIT_NOTICE_NEG_KEYWORDS):
                    log_info("UPBIT-NOTICE", f"skip id={probe_id} negative | {title[:60]}")
                    continue

                tickers: list[str] = []
                seen: set[str] = set()
                for tok in _UPBIT_NOTICE_TICKER_RE.findall(title):
                    t = tok.upper()
                    if t in _UPBIT_NOTICE_BANNED or t in seen:
                        continue
                    seen.add(t)
                    tickers.append(t)

                if not tickers:
                    log_warn("UPBIT-NOTICE", f"id={probe_id} нет тикеров в title | {title[:80]}")
                    continue

                fetch_ms = (t_recv - t_send) * 1000
                log_ok(
                    "UPBIT-NOTICE",
                    f"LISTING id={probe_id} {tickers} ({fetch_ms:.0f}мс) | {title[:80]}",
                )
                process_signal(tickers, "UPBIT-NOTICE", t_start=t_send)

            if not advanced:
                # Не нашли новых id — обычная пауза.
                time.sleep(UPBIT_ANNOUNCEMENT_POLL_INTERVAL)

        except Exception as e:
            log_err("UPBIT-NOTICE", f"poll error: {e}")
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
BINANCE_NOTICE_POLL_INTERVAL = float(os.getenv("BINANCE_NOTICE_POLL_INTERVAL", "3.0"))
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
            articles = catalogs[0].get("articles", []) if catalogs else []
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

            # Успех — сбрасываем бан-стейт к базовым значениям.
            if ban_streak or cur_interval != BINANCE_NOTICE_POLL_INTERVAL:
                ban_streak = 0
                cur_interval = BINANCE_NOTICE_POLL_INTERVAL

            data = _json_loads(resp.content)
            if data.get("code") != "000000":
                continue
            catalogs = (data.get("data") or {}).get("catalogs") or []
            if not catalogs:
                continue
            articles = catalogs[0].get("articles", [])

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
            log_err("BINANCE-NOTICE", f"poll error: {e}")
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
                if sync_inst.is_ready(wait_sec=3.0):
                    sync_warm = sync_inst.warmup()
                    use_sync_ws(sync_inst)
                    _sync_mod.start_periodic_warmup()
                    sync_ready = True
                    suffix = "+ прогрет" if sync_warm else "(warmup не прошёл)"
                    log_ok("PARSER", f"Bybit SYNC WS Trade готов {suffix} ✓ (no cross-thread)")
                else:
                    log_warn("PARSER", "Bybit SYNC WS не подключился за 3с — fallback на async")
            except Exception as e:
                log_warn("PARSER", f"Bybit SYNC WS init упал: {e!r} — fallback на async")

        # Async-инстанс: если sync уже работает, async всё равно нужен как
        # fallback при sync reconnect. Если sync не запустился — async основной.
        try:
            from api.bybit_ws_trade import init as bybit_ws_init, start_periodic_warmup
            inst = bybit_ws_init(BYBIT_API_KEY, BYBIT_SECRET_KEY)
            if inst.is_ready(wait_sec=3.0):
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
                log_warn("PARSER", "Bybit ASYNC WS не подключился за 3с — fallback на REST")
        except Exception as e:
            log_err("PARSER", f"Bybit ASYNC WS init упал: {e!r} — будет REST")
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
        from api.listing_api import warmup_chain as _lst_warmup_chain
        sample_signal = "[BITHUMB] $BTC listed on Bithumb"
        for _ in range(30):
            find_listing_pairs(sample_signal)
        ok = _lst_warmup_chain(n=30)
        log_ok("PARSER", f"Chain warmup: regex×30 + market_open_long path×{ok}/30 ✓")
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
    threading.Thread(
        target=run_upbit_announcement_poller,
        daemon=True,
        name="upbit-notice-poller",
    ).start()
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

    # FIX-LATENCY (Patch #1): Seoul edge-нода. Если SEOUL_RELAY_URL задан —
    # подключаемся к Seoul WS и получаем сигналы за ~85-120мс KR→SG (vs
    # ~240мс прямым поллингом из SG). См. seoul_relay/README.md для
    # деплоя серверной части.
    if SEOUL_RELAY_URL:
        try:
            from api.seoul_relay_receiver import run_seoul_relay_listener
            def _on_seoul_listing(tickers: list[str], source: str, t_start: float) -> None:
                # source приходит как SEOUL-RELAY-BITHUMB / SEOUL-RELAY-UPBIT —
                # биржа в теге, _resolve_exchange извлечёт → per-exchange L2.
                process_signal(tickers, source, t_start=t_start)
            threading.Thread(
                target=run_seoul_relay_listener,
                kwargs={"url": SEOUL_RELAY_URL, "listing_callback": _on_seoul_listing},
                daemon=True,
                name="seoul-relay-rx",
            ).start()
            log_ok("PARSER", f"Seoul relay listener подключён → {SEOUL_RELAY_URL.split('?', 1)[0]}")
        except Exception as e:
            log_err("PARSER", f"Не удалось запустить Seoul relay: {e!r}")
    else:
        log_info("PARSER", "SEOUL_RELAY_URL пуст — Seoul relay отключён (см. seoul_relay/README.md)")

    log_ok("PARSER", "Запускаем Telegram listener (нужна авторизация при первом запуске)...")
    tg_log("🚀 <b>LISTING парсер запущен</b>\nUpbit + Bithumb + Telegram + Watchdog")

    # FIX: heartbeat — для docker healthcheck + TG алерт раз в 2 часа.
    def heartbeat():
        while True:
            _touch_heartbeat()
            time.sleep(60)
            if int(time.time()) % 7200 < 60:
                tg_log("✅ <b>LISTING парсер работает</b>")

    threading.Thread(target=heartbeat, daemon=True, name="heartbeat").start()

    # FIX: TG listener — в while True с автоперезапуском (раньше падал и не вставал).
    while True:
        try:
            run_telegram_listener()
        except KeyboardInterrupt:
            break
        except Exception as e:
            log_err("PARSER", f"TG listener упал: {e} — перезапуск через 10с")
            tg_log(f"⚠️ <b>LISTING</b>: TG listener упал, перезапуск через 10с\n{e}")
        time.sleep(10)