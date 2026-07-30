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
except ImportError:
    import json as _stdjson
    def _json_loads(b: bytes | str):
        if isinstance(b, (bytes, bytearray)):
            b = b.decode()
        return _stdjson.loads(b)

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
)

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

# ── Защита от массового открытия позиций ──────────────────────────
# FIX-AUDIT (CRITICAL): если стартовая загрузка списка тикеров падает
# (таймаут / 403 Cloudflare / сетевой сбой), except-ветка ставила
# known = set(), а значит ever_seen = set(). Первый же УСПЕШНЫЙ poll
# возвращал весь рынок — 276 KRW-тикеров у Upbit — и каждый считался
# «новым листингом». Результат: process_signal открывал лонги по всему
# рынку сразу.
#
# У Bithumb и Binance guard `len(new) > 10` был. У Upbit — нет.
# Выносим порог в константу и применяем во ВСЕХ трёх поллерах.
MAX_NEW_TICKERS_PER_TICK = 10


def _is_bulk_update(new_tickers: set[str]) -> bool:
    """
    True, если за один тик появилось подозрительно много «новых» тикеров.

    Настоящий листинг — это 1-3 монеты. Десятки одновременно означают
    восстановление после сбоя, окончание maintenance биржи или первый
    успешный poll после неудачной инициализации. Торговать по такому
    событию нельзя.
    """
    return len(new_tickers) > MAX_NEW_TICKERS_PER_TICK


# Удался ли стартовый снимок рынка для каждого поллера. Пока False —
# поллер НЕ торгует: первый валидный ответ используется только как baseline.
_initial_snapshot_ok: dict[str, bool] = {
    "UPBIT": False,
    "BITHUMB": False,
    "BINANCE": False,
}

# ── Дедуп: L1 (TTL) + L2 (permanent persisted) ────────────────────
# L1 — отсекает шумовые дубли в окне нескольких секунд (несколько каналов
#      пишут об одном листинге). L2 — «уже торговали эту монету» навсегда.
#
# Принцип L2:
#   ANNOUNCEMENT_SOURCES (TG, TOA WS, CoinListing WS) — claim сразу пишет
#       coin в _global_fired (без TTL, на диск). Любой будущий сигнал по
#       этой монете отовсюду — skip.
#   DIRECT_POLL_SOURCES (UPBIT, BITHUMB, BINANCE) — claim пишет в
#       _per_exchange_fired[exchange]. Skip только если эта же биржа уже
#       стреляла эту монету.
#
# L2 заполняется ТОЛЬКО после успешного открытия позиции (worker callback),
# чтобы провалившийся open (нет цены/ликвидности) не сделал монету «забытой».
_fired_lock     = threading.Lock()

# L1: короткоживущая защита от near-simultaneous дублей.
_recent_signals: dict[tuple[str, str], float] = {}   # (coin, source) -> expiry
# FIX-AUDIT (perf): индекс L1 по монете. _recent_signals ключуется парой
# (coin, source), поэтому проверка «занята ли монета любым источником»
# раньше требовала полного скана словаря O(n) в hot-path. Держим отдельный
# coin -> expiry, чтобы _try_claim работал за O(1). Оба словаря живут под
# _fired_lock и чистятся одним sweeper'ом.
_coin_claim_expiry: dict[str, float] = {}
_FIRED_TTL      = 60

# FIX: метрика отставания между источниками. Для каждой свежеcклеймленной
# монеты запоминаем (первый_источник, perf_counter()) — когда второй
# источник по той же монете попадает в _try_claim → skip, считаем дельту
# и логируем "CoinListing опоздал на 4.2с от BWEnews". Это нужно чтобы
# отличать «WS-источник не сработал» от «WS-источник работает, но медленный».
_first_claim_ts: dict[str, tuple[str, float]] = {}   # coin -> (source, t)
_FIRST_CLAIM_TTL = 300  # храним 5 минут — на дольше дельта уже неинтересна

# L2: хранилище опыта «эту монету уже торговали».
#
# FIX-AUDIT: раньше это были множества без срока давности — монета
# блокировалась НАВСЕГДА. Но повторные события по одной монете реальны:
# монета делистится с одной биржи и листится на другой, возвращается после
# делистинга, либо листится сначала на Upbit, а через месяцы на Binance.
# Второй сигнал молча пропускался.
#
# Теперь храним время открытия и истекаем через _L2_TTL. Защита от дублей
# при рестарте и от повторной обработки того же анонса сохраняется.
_global_fired: dict[str, float] = {}                  # ANNOUNCEMENT: coin -> ts
_per_exchange_fired: dict[str, dict[str, float]] = {  # DIRECT POLL: coin -> ts
    "UPBIT": {},
    "BITHUMB": {},
    "BINANCE": {},
}

# 30 дней — дольше «эха» одного события, но кратно меньше интервала между
# независимыми листингами одной монеты.
_L2_TTL = float(os.getenv("LISTING_L2_TTL_SEC", 30 * 24 * 3600))

# Источники, по которым coin помечается ГЛОБАЛЬНО (anywhere fired).
# Все TG-каналы (включая extra) — это анонсы. TOA/CoinListing — тоже анонсы.
_ANNOUNCEMENT_SOURCE_PREFIXES = ("TG:", "TOA-", "COINLISTING-")

# Источники прямого детектора листингов (без анонса).
_DIRECT_POLL_SOURCES = {"UPBIT", "BITHUMB", "BINANCE"}

# FIX-AUDIT: см. config.config.STATE_DIR — состояние вне образа.
_FIRED_FILE = Path(STATE_DIR) / "listing_fired.json"

# FIX-PERF: dirty-flag для фонового L2-writer'а — вместо thread.start() на
# каждый успешный open (то стоило ~3-15мс в hot-path worker'а под GIL contention).
_fired_dirty = threading.Event()

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

def _classify_source(source: str) -> tuple[str, str]:
    """
    Возвращает (kind, exchange).
      kind ∈ {"ANNOUNCE", "DIRECT", "OTHER"}
      exchange — биржа для DIRECT (UPBIT/BITHUMB/BINANCE), иначе "".
    """
    if source in _DIRECT_POLL_SOURCES:
        return "DIRECT", source
    if source.startswith(_ANNOUNCEMENT_SOURCE_PREFIXES):
        return "ANNOUNCE", ""
    return "OTHER", ""


def _load_fired_state() -> None:
    """Подгружает L2 с диска. Безопасно к отсутствию файла / битым данным."""
    try:
        if not _FIRED_FILE.exists():
            return
        raw = _FIRED_FILE.read_bytes()
        if not raw.strip():
            return
        try:
            import orjson as _oj  # type: ignore[import-not-found]
            data = _oj.loads(raw)
        except ImportError:
            import json as _stdj
            data = _stdj.loads(raw.decode())
        gf = data.get("global", [])
        now = time.time()

        def _ingest(target: dict[str, float], payload) -> None:
            """FIX-AUDIT: принимаем и старый формат (list без времени), и новый
            (dict coin -> ts). Записи старого формата считаем свежими, чтобы
            апгрейд не разблокировал разом всю историю."""
            if isinstance(payload, list):
                for c in payload:
                    if isinstance(c, str):
                        target[c] = now
            elif isinstance(payload, dict):
                for c, ts in payload.items():
                    if not isinstance(c, str):
                        continue
                    try:
                        ts_f = float(ts)
                    except (TypeError, ValueError):
                        continue
                    if now - ts_f < _L2_TTL:
                        target[c] = ts_f

        with _fired_lock:
            _ingest(_global_fired, gf)
            for ex, coins in (data.get("per_exchange") or {}).items():
                if ex in _per_exchange_fired:
                    _ingest(_per_exchange_fired[ex], coins)
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
            # FIX-AUDIT: сохраняем время открытия — TTL должен переживать рестарт.
            snapshot = {
                "global": dict(_global_fired),
                "per_exchange": {k: dict(v) for k, v in _per_exchange_fired.items()},
            }
        try:
            import orjson as _oj  # type: ignore[import-not-found]
            payload = _oj.dumps(snapshot, option=_oj.OPT_INDENT_2)
        except ImportError:
            import json as _stdj
            payload = _stdj.dumps(snapshot, indent=2).encode()
        _FIRED_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _FIRED_FILE.with_suffix(".json.tmp")
        tmp.write_bytes(payload)
        tmp.replace(_FIRED_FILE)
    except Exception as e:  # noqa: BLE001
        log_warn("DEDUP", f"L2 persist failed: {e!r}")


def _is_already_fired(coin: str, source: str) -> bool:
    """
    L2-проверка. Должна вызываться под _fired_lock.

    FIX-AUDIT: блокировка действует только _L2_TTL секунд. Протухшие записи
    удаляются здесь же (lazy expiry) — на случай если sweeper ещё не дошёл.
    """
    now = time.time()

    fired_at = _global_fired.get(coin)
    if fired_at is not None:
        if now - fired_at < _L2_TTL:
            return True
        _global_fired.pop(coin, None)

    kind, exchange = _classify_source(source)
    if kind == "DIRECT" and exchange:
        bucket = _per_exchange_fired.get(exchange)
        if bucket:
            ex_ts = bucket.get(coin)
            if ex_ts is not None:
                if now - ex_ts < _L2_TTL:
                    return True
                bucket.pop(coin, None)
    return False


def _try_claim(coin: str, source: str) -> bool:
    """
    L1: ставим claim в окно TTL чтобы шум из множества каналов в течение
    секунд не открывал дубль. L2 — проверяем «уже торговали» перед claim'ом.
    L2 ЗАПИСЫВАЕТСЯ только после успешного open (см. _mark_opened).

    FIX-AUDIT (perf): раньше L1-проверка линейно сканировала весь
    _recent_signals, сравнивая `c == coin` по каждому ключу (coin, source).
    Это O(n) в самом горячем месте — между приходом сигнала и отправкой
    ордера, под общим _fired_lock, который держат ещё и sweeper с
    _measure_lag. При заметном числе живых claim'ов скан добавлял задержку
    ровно там, где мы боремся за миллисекунды.

    Теперь L1 индексирован по монете: _coin_claim_expiry[coin] -> expiry.
    Проверка и вставка — O(1).
    """
    now = time.monotonic()
    with _fired_lock:
        if _is_already_fired(coin, source):
            return False
        # L1: уже взят (любым источником) в пределах TTL → skip. O(1).
        expiry = _coin_claim_expiry.get(coin)
        if expiry is not None and expiry > now:
            return False
        _recent_signals[(coin, source)] = now + _FIRED_TTL
        _coin_claim_expiry[coin] = now + _FIRED_TTL
        # FIX: запоминаем первый источник, который заявил эту монету.
        # _first_claim_ts заполняется только один раз — пока запись жива,
        # последующие claim'ы по этой монете уйдут в _is_already_fired/L1
        # и сюда не попадут. Это нормально: нам нужна метка именно ПЕРВОГО.
        _first_claim_ts[coin] = (source, now)
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


def _mark_opened(coin: str, source: str) -> None:
    """
    Worker зовёт после успешного open. Обновляет L2 (in-memory) и поднимает
    dirty-flag — фоновый writer (_fired_persist_loop) сохранит на диск.

    FIX-PERF: НЕ спавним поток здесь — это hot-path worker'а. Раньше
    threading.Thread(_persist_fired_state).start() стоил ~3-15мс под GIL
    contention (создание потока — это OS-syscall + GIL acquire несколько раз).
    Теперь только set.add (~1мкс) + Event.set (~1мкс).

    ANNOUNCEMENT → global. DIRECT → per-exchange.
    """
    kind, exchange = _classify_source(source)
    dirty = False
    now = time.time()
    with _fired_lock:
        if kind == "ANNOUNCE":
            # Повторное открытие сдвигает окно блокировки.
            _global_fired[coin] = now
            dirty = True
        elif kind == "DIRECT" and exchange:
            bucket = _per_exchange_fired.setdefault(exchange, {})
            bucket[coin] = now
            dirty = True
        # OTHER — не пишем в L2 (на всякий случай).
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


def _fired_sweeper() -> None:
    """
    L1: очистка истёкших claim'ов.
    FIX-AUDIT: L2 больше не «навсегда» — протухшие по _L2_TTL записи тоже
    удаляются, иначе повторный листинг той же монеты был бы пропущен.
    """
    while True:
        time.sleep(5)
        now = time.monotonic()
        now_wall = time.time()
        with _fired_lock:
            expired = [k for k, ts in _recent_signals.items() if ts <= now]
            for k in expired:
                _recent_signals.pop(k, None)
            # FIX-AUDIT: чистим индекс L1 синхронно с _recent_signals —
            # иначе протухшая запись навсегда заблокировала бы монету.
            expired_coins = [c for c, ts in _coin_claim_expiry.items() if ts <= now]
            for c in expired_coins:
                _coin_claim_expiry.pop(c, None)
            # FIX: чистим _first_claim_ts по TTL, иначе на длинной дистанции
            # словарь растёт неограниченно (1 запись на каждую монету,
            # которая когда-либо стреляла).
            expired_first = [
                c for c, (_src, t0) in _first_claim_ts.items()
                if now - t0 > _FIRST_CLAIM_TTL
            ]
            for c in expired_first:
                _first_claim_ts.pop(c, None)

            # FIX-AUDIT: L2 по TTL — снимаем «пожизненную» блокировку монеты.
            stale = [c for c, ts in _global_fired.items() if now_wall - ts >= _L2_TTL]
            for c in stale:
                _global_fired.pop(c, None)
            stale_ex = 0
            for bucket in _per_exchange_fired.values():
                for c in [c for c, ts in bucket.items() if now_wall - ts >= _L2_TTL]:
                    bucket.pop(c, None)
                    stale_ex += 1
        if stale or stale_ex:
            log_info("DEDUP", f"L2: снята блокировка — global={len(stale)}, "
                              f"per_exchange={stale_ex}")


# ── воркер (открытие лонга) ───────────────────────────────────────

def worker(coin: str, margin: float, t_start: float,
           source: str, retries: int = 3) -> None:
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
            _mark_opened(coin, source)
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

def _on_toa_listing(full_text: str, t_start: float) -> None:
    """Callback из treeofalpha_ws — листинг."""
    pairs = find_listing_pairs(full_text)
    if not pairs:
        log_warn("TOA-LIST", f"тикеры не найдены: {full_text[:120]}")
        return
    log_ok("TOA-LIST", f"Листинг-сигнал из TOA WS: {pairs}")
    # FIX-batch-8: пробрасываем t_start из WS-loop. Раньше perf_counter()
    # перезаписывался в process_signal и латентность от прихода сообщения
    # до ордера терялась.
    process_signal(pairs, "TOA-WS", t_start=t_start)


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


def process_signal(pairs: list[str], source: str, t_start: float | None = None) -> None:
    """
    FIX-batch-8: t_start теперь параметр. Если None — замеряем сами (бэк-совместимость).
    Если передан (из TOA WS / TG handler / Upbit-Bithumb) — используем точный момент
    прихода сигнала, чтобы метрики OPEN/LONG в логах отражали полный путь.
    """
    if t_start is None:
        t_start = time.perf_counter()

    new_pairs = [c for c in pairs if _try_claim(c, source)]
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
        worker(new_pairs[0], margin, t_start, source)
    else:
        for coin in new_pairs:
            _signal_executor.submit(worker, coin, margin, t_start, source)

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

            # FIX-PERF: вызываем process_signal НАПРЯМУЮ вместо
            # threading.Thread(target=_safe_signal).start() — экономия ~3-5мс
            # на спавн дополнительного потока. process_signal сам делает
            # executor.submit (уже не блокирует), а оставшиеся print'ы идут
            # ПОСЛЕ submit'а, так что задержка до OPEN не растёт.
            try:
                process_signal(pairs, source, t_start)
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
    # FIX-AUDIT (CRITICAL): отслеживаем, удалась ли стартовая загрузка.
    # Раньше при сбое known=set() и первый успешный poll принимал ВЕСЬ
    # рынок (276 тикеров) за пачку листингов.
    snapshot_ok = False
    known: set[str] = set()
    try:
        known = _load_upbit_tickers(session)
        snapshot_ok = bool(known)
        log_ok("UPBIT", f"Загружено {len(known)} тикеров, жду новые (poll {POLL_INTERVAL*1000:.0f}мс)...")
        with _ts_lock:
            _upbit_last_ts = time.monotonic()
    except Exception as e:
        log_err("UPBIT", f"Ошибка инициализации: {e}")

    _initial_snapshot_ok["UPBIT"] = snapshot_ok
    if not snapshot_ok:
        log_warn("UPBIT", "Стартовый снимок не получен — торговля отложена "
                          "до первого валидного ответа (защита от массового открытия)")

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

            # FIX-AUDIT: стартовый снимок не удался — первый валидный ответ
            # используем как baseline, НЕ торгуем по нему.
            if not _initial_snapshot_ok.get("UPBIT"):
                ever_seen |= current
                _initial_snapshot_ok["UPBIT"] = True
                log_ok("UPBIT", f"Baseline восстановлен ({len(current)} тикеров) — "
                                f"торговля активна")
                continue

            new_tickers = current - ever_seen

            # FIX-AUDIT (CRITICAL): bulk-guard, которого у Upbit не было.
            if _is_bulk_update(new_tickers):
                log_warn("UPBIT", f"Подозрительно много новых тикеров "
                                  f"({len(new_tickers)}), пропускаем")
                ever_seen |= current
                continue

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
    snapshot_ok = False
    known: set[str] = set()
    try:
        known = _load_bithumb_tickers(session)
        snapshot_ok = bool(known)
        log_ok("BITHUMB", f"Загружено {len(known)} тикеров, жду новые (poll {POLL_INTERVAL*1000:.0f}мс)...")
        with _ts_lock:
            _bithumb_last_ts = time.monotonic()
    except Exception as e:
        log_err("BITHUMB", f"Ошибка инициализации: {e}")

    _initial_snapshot_ok["BITHUMB"] = snapshot_ok
    if not snapshot_ok:
        log_warn("BITHUMB", "Стартовый снимок не получен — торговля отложена "
                            "до первого валидного ответа")

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

            # FIX-AUDIT: baseline после неудачного старта — без торговли.
            if not _initial_snapshot_ok.get("BITHUMB"):
                ever_seen |= current
                _initial_snapshot_ok["BITHUMB"] = True
                log_ok("BITHUMB", f"Baseline восстановлен ({len(current)} тикеров)")
                continue

            new_tickers = current - ever_seen

            # FIX: порог 3 → 10. После maintenance Bithumb может разово отдать
            # 5-8 новых тикеров — мы их пропускали все, теряя реальный листинг.
            if _is_bulk_update(new_tickers):
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
    snapshot_ok = False
    known: set[str] = set()
    try:
        known = _load_binance_futures_tickers(session)
        snapshot_ok = bool(known)
        log_ok("BINANCE", f"Загружено {len(known)} тикеров, жду новые "
                          f"(poll {BINANCE_POLL_INTERVAL*1000:.0f}мс)...")
        with _ts_lock:
            _binance_last_ts = time.monotonic()
    except Exception as e:
        log_err("BINANCE", f"Ошибка инициализации: {e}")

    _initial_snapshot_ok["BINANCE"] = snapshot_ok
    if not snapshot_ok:
        log_warn("BINANCE", "Стартовый снимок не получен — торговля отложена "
                            "до первого валидного ответа")

    ever_seen: set[str] = set(known)

    while True:
        try:
            time.sleep(BINANCE_POLL_INTERVAL)
            t_send = time.perf_counter()
            current = _load_binance_futures_tickers(session)
            t_recv = time.perf_counter()

            with _ts_lock:
                _binance_last_ts = time.monotonic()

            # FIX-AUDIT: baseline после неудачного старта — без торговли.
            if not _initial_snapshot_ok.get("BINANCE"):
                ever_seen |= current
                _initial_snapshot_ok["BINANCE"] = True
                log_ok("BINANCE", f"Baseline восстановлен ({len(current)} тикеров)")
                continue

            new_tickers = current - ever_seen

            # Защита от bulk-апдейта (рестарт API / временная подгрузка
            # списка после maintenance): >10 новых за тик — почти точно
            # not-a-listing event.
            if _is_bulk_update(new_tickers):
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
    log_ok("PARSER", f"Upbit/Bithumb ({POLL_INTERVAL*1000:.0f}мс) + Binance futures "
                     f"({BINANCE_POLL_INTERVAL*1000:.0f}мс) + CoinListing WS запущены")

    threading.Thread(target=_watchdog, daemon=True, name="watchdog").start()
    log_ok("PARSER", f"Watchdog запущен (таймаут {WATCHDOG_TIMEOUT}с)")

    # FIX-PERF: глобальный sweeper вместо thread-per-claim (см. _try_claim).
    threading.Thread(target=_fired_sweeper, daemon=True, name="fired-sweeper").start()
    # FIX-PERF: единый фоновый L2-writer (dirty-flag + 1с batching) вместо
    # thread.start на каждый _mark_opened в hot-path.
    threading.Thread(target=_fired_persist_loop, daemon=True, name="fired-persist").start()

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