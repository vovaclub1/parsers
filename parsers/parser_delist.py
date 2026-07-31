from __future__ import annotations

import asyncio
import atexit
import os
import random
import re
import signal
import sys
import threading
import time
import itertools
import requests
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# FIX: Binance CDN (Akamai/CloudFront) фингерпринтит TLS-handshake
# python-requests (JA3/JA4) и возвращает 400 на
# /bapi/composite/v1/public/cms/article/list/query независимо от
# headers. curl_cffi эмулирует реальный TLS Chrome через
# libcurl-impersonate; API совместим с requests.Session
# (.get/.headers/.proxies/raise_for_status). При отсутствии пакета —
# graceful fallback на requests (400 продолжатся, но контейнер
# не падает).
try:
    from curl_cffi import requests as _cffi_requests  # type: ignore[import-not-found]
    # "chrome" — алиас на самый свежий поддерживаемый профиль
    # (см. curl_cffi.requests.BrowserType). Переопределить точную
    # версию можно через BINANCE_TLS_IMPERSONATE=chrome120 в .env.
    _HTTP_IMPERSONATE = os.getenv("BINANCE_TLS_IMPERSONATE", "chrome")

    def _new_http_session(proxy: str | None):
        kwargs: dict = {"impersonate": _HTTP_IMPERSONATE}
        if proxy:
            kwargs["proxies"] = {"http": proxy, "https": proxy}
        return _cffi_requests.Session(**kwargs)

    _HTTP_BACKEND = "curl_cffi"
except ImportError:
    def _new_http_session(proxy: str | None):
        s = requests.Session()
        if proxy:
            s.proxies = {"http": proxy, "https": proxy}
        return s

    _HTTP_BACKEND = "requests"

# FIX-batch-1: orjson в хот path Binance article parsing (3-5x быстрее).
try:
    import orjson as _orjson  # type: ignore[import-not-found]
    def _json_loads(b: bytes | str):
        if isinstance(b, str):
            b = b.encode()
        return _orjson.loads(b)
    def _json_dumps(obj, indent: bool = False):
        opts = _orjson.OPT_INDENT_2 if indent else 0
        return _orjson.dumps(obj, option=opts)
except ImportError:  # graceful fallback
    import json as _stdjson
    def _json_loads(b: bytes | str):
        if isinstance(b, (bytes, bytearray)):
            b = b.decode()
        return _stdjson.loads(b)
    def _json_dumps(obj, indent: bool = False):
        if indent:
            return _stdjson.dumps(obj, indent=2).encode()
        return _stdjson.dumps(obj).encode()

# msgspec — schema-based парсинг Binance article-list ответа (~5-20KB).
# Вместо json → dict → field-access используем typed Struct: парсер сразу
# проходит структуру и валидирует поля (≈2-3x быстрее orjson+dict для
# структурированных ответов). Fallback на _json_loads если msgspec не
# установлен — поведение бит-в-бит совпадает.
try:
    import msgspec as _msgspec  # type: ignore[import-not-found]

    class _BinanceArticle(_msgspec.Struct, frozen=True):
        id:    int | None = None
        code:  str | None = None
        title: str | None = ""

    class _BinanceCatalog(_msgspec.Struct, frozen=True):
        articles: list[_BinanceArticle] = []  # noqa: RUF012 — msgspec Struct field

    class _BinanceData(_msgspec.Struct, frozen=True):
        articles: list[_BinanceArticle] | None = None
        catalogs: list[_BinanceCatalog] | None = None

    class _BinanceResp(_msgspec.Struct, frozen=True):
        data: _BinanceData | None = None

    _binance_decoder = _msgspec.json.Decoder(_BinanceResp)

    def _parse_binance_articles(raw: bytes) -> list[dict]:
        """Возвращает список dict'ов с ключами id/code/title для совместимости
        с process_article (он работает на dict-API)."""
        resp = _binance_decoder.decode(raw)
        if resp.data is None:
            return []
        if resp.data.articles:
            return [{"id": a.id, "code": a.code, "title": a.title or ""}
                    for a in resp.data.articles]
        if resp.data.catalogs:
            first = resp.data.catalogs[0]
            return [{"id": a.id, "code": a.code, "title": a.title or ""}
                    for a in first.articles]
        return []
except ImportError:  # graceful fallback на orjson-путь
    _parse_binance_articles = None  # type: ignore[assignment]

from config.config import (
    TG_API_ID, TG_API_HASH, DELIST_PROXIES, SESSION_DIR, STATE_DIR,
    EXTRA_DELIST_CHANNELS, parse_channels,    # FIX-batch-3: multi-channel
    TREE_OF_ALPHA_WS_ENABLED,                  # FIX-batch-4: TOA WS
    BYBIT_WS_TRADE_ENABLED,                    # FIX-batch-5: Bybit WS Trade
    BYBIT_SYNC_WS_ENABLED,                     # FIX-PERF: sync WS hot-path
    BYBIT_API_KEY, BYBIT_SECRET_KEY,
)
from api.delist_api import (
    find_pairs,
    calculate_margin_for_delist,
    price_updater,
    get_price,
    warmup_bybit_connection,
    start_bybit_heartbeat,
    preload_lot_steps,
    gate_price_updater,
    gate_preload_lot_steps,
    warmup_gate_connection,
)
# FIX 2026-07-07 (INVERT): по решению — на ДЕЛИСТАХ открываем ЛОНГ
# (контр-тренд на отскок после дампа), на листингах — шорт. Лонг-механика
# (Bybit+Gate fallback, bundled SL, трейлинг LISTING_*) переиспользуется
# из listing_api.
from api.listing_api import (
    market_open_long,
    set_tp_sl_long,
)
from tg.tg_logger import tg_log

# ── цвета ────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# ── служебные переменные ──────────────────────────────────────────
BINANCE_API_URL    = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
ARTICLE_DETAIL_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query"
CATEGORY_ID        = 161

# Базовый интервал между запросами одного поллера (секунды).
# FIX: 1.0с давал 60 req/min на IP — Binance стабильно банил 429 в первые
# ~15с (по логам: 14 запросов от direct → 429, потом каждый раз после
# POLL_BACKOFF_429 паузы цикл повторялся). 3.0с даёт ~20 req/min на IP —
# с запасом ниже наблюдаемого лимита /bapi/composite, при этом с 3-4
# поллерами эффективный глобальный rate остаётся ~1 req/с (median
# детекции ~500мс — приемлемо для делистинга).
# Env DELIST_POLL_INTERVAL=Xс позволяет тюнить без передеплоя: уменьшить
# если 3с надёжно держит и хочется быстрее, или увеличить если 3с всё
# ещё ловит 429 (тогда Binance срезал лимиты — попробуй 5.0 или 6.0).
POLL_INTERVAL_BASE = float(os.getenv("DELIST_POLL_INTERVAL", "3.0"))
# Анти-стадо: добавляем ±jitter на каждый sleep, чтобы N поллеров,
# стартовавших на одном CPU-тике, не били Binance синхронно.
# 0.10 = ±10% — при 3с интервале это ±300мс, незаметно для детекции,
# но размывает пики req/с на стороне Binance.
POLL_INTERVAL_JITTER = 0.10
# FIX-batch-8: per-poller адаптивный backoff при 429 — поллер, который
# нарвался на rate limit, делает свой следующий запрос с увеличенным
# интервалом, восстанавливается через POLL_BACKOFF_RECOVERY секунд.
POLL_BACKOFF_429   = 30.0   # пауза при HTTP 429 (как раньше)
POLL_BACKOFF_MULT  = 1.5    # после ошибки этот поллер замедляется в 1.5x
# FIX: при POLL_INTERVAL_BASE=3.0 потолок 5.0 = всего 1.7x запаса. Поднимаем
# до 5× базового, чтобы backoff реально мог растянуться при флапающем
# Binance/прокси (иначе MULT=1.5 упирается в потолок после первого bump'а).
POLL_BACKOFF_MAX   = max(15.0, POLL_INTERVAL_BASE * 5)
POLL_BACKOFF_RECOVERY = 60.0  # секунд до возврата к POLL_INTERVAL_BASE

# FIX: «постоянные» отказы (мёртвый прокси: Connection refused,
# timeout на handshake) поднимают интервал до DEAD_INTERVAL, иначе
# лог захлёбывается 1 ошибкой в секунду на каждый dead proxy.
# Триггер — N подряд connection error'ов. Любой успешный ответ —
# выход из DEAD-режима. Логируем редко (раз в N тиков), чтобы
# проблема была видна, но не спамила.
POLL_DEAD_THRESHOLD     = 5      # подряд connection-error'ов = «прокси мёртв»
POLL_DEAD_INTERVAL      = 60.0   # интервал retry для «мёртвого» поллера
POLL_DEAD_LOG_EVERY     = 30     # лог только каждый N-й тик пока мёртвый

# Heartbeat: раз в N успешных циклов поллер логирует «alive ✓ K pollов».
# Это видимое подтверждение что Binance отдаёт 200 (после fix `_t`) и
# парсер крутится — иначе после `первый запуск — запомнили K статей`
# логи молчат пока не появится новая статья (что бывает раз в часы/сутки).
# 60 × ~1с = ~60с между heartbeat'ами на каждый живой поллер → 3-4
# строки в минуту суммарно, не спам.
POLL_HEARTBEAT_EVERY    = 60

DELIST_KEYWORDS    = ["Will Delist", "Delisting Notice", "Binance Will Delist"]

# FIX-batch-8: cache-busting — разрешённые значения pageSize.
# По наблюдениям community CloudFront кеширует ответы по cache key из query.
# pageSize вне этого набора → возвращается cached page с 20 items.
# Меняя pageSize между запросами + добавляя random query params, мы
# увеличиваем шанс попасть на свежий fresh-fetch от Binance API.
BINANCE_PAGE_SIZES = [5, 10, 15, 20]

# Telegram listener
TG_CHANNEL        = "coin_listing"
# FIX-batch-6/7: расширено под реальные форматы каналов пользователя.
TG_DELIST_KEYWORDS = [
    # Binance EN
    "will delist", "delisting notice", "binance will delist",
    "will be delisted", "remove from spot", "removed from spot",
    "removing from spot",
    # FIX-batch-7: структурированные форматы каналов
    # (listing_binance_mids: "🆘 BINANCE | Delisting"; CLW: "BINANCE Delisting")
    "| delisting", "binance delisting", "bybit delisting",
    "upbit delisting", "bithumb delisting",
    # Monitoring Tag — pre-delist warning (часто −10..−30% сразу)
    "monitoring tag", "extend the monitoring",
    # Korean exchanges
    "delisted from upbit", "upbit will delist",
    "delisted from bithumb", "bithumb will delist",
    # Russian (@delistingscreener и т.п.)
    "делисты", "делистинг", "делист ",
    "будет делист", "удаление с",
]
# Negative phrases — даже если есть delist keyword, эти сигналы шум:
TG_DELIST_NEG = [
    "alpha will remove",    # Binance Alpha (low-cap meme tokens)
    "removed from the featuring list",
    "alpha removal",
    "from alpha",
    "delisting postponed",  # отменили
    # FIX 2026-06-19: margin-делисты ТЕПЕРЬ отрабатываем (шортим перп базовой монеты).
    # Раньше блокировались строками "margin trading pairs"/"cross margin"/... как ложные.
    # Но монеты реально торгуются перпами (CVC/RPL/RVN/XAI), а защита от ложных тикеров
    # (AT/FOLLOWING + квоуты BTC/ETH/BNB) теперь в EXCLUDED_TOKENS + фильтре по торгуемым
    # (Bybit∪Gate). Поэтому margin-строки УБРАНЫ из NEG. Spot-пар-removal по-прежнему
    # не проходит (нет positive-keyword "will delist"/"remove from spot").
    # Аналогичные «не-делисты»: leveraged token shutdowns, copy-trading
    # ограничения и т.п. — это не уход монеты с биржи.
    "leveraged tokens",
    "copy trading",
    "convert pairs",
    "convert pair",
]

# Watchdog
WATCHDOG_TIMEOUT   = 120   # секунд без успешного запроса → перезапуск поллера

# ── прокси ───────────────────────────────────────────────────────
# FIX: креды больше не в коде. DELIST_PROXIES = "user1:pass1@host1:port1,user2:..."
# из .env. Прямое подключение всегда добавляем (None в начало).
def _parse_proxies(raw: str) -> list[str | None]:
    """
    Парсит DELIST_PROXIES в список URL-ов прокси.
    Поддерживает оба формата (через запятую):
      1. user:pass@host:port              (стандарт)
      2. host:port:user:pass              (формат Webshare/большинства провайдеров)
      3. user:pass:host:port              (тоже встречается)
    Возвращает [None, "http://...", ...] — None означает прямое подключение.
    """
    proxies: list[str | None] = [None]
    for p in (raw or "").split(","):
        p = p.strip()
        if not p:
            continue

        # Уже с http(s)://
        if p.startswith("http://") or p.startswith("https://"):
            proxies.append(p)
            continue

        # Формат с @ → user:pass@host:port
        if "@" in p:
            proxies.append(f"http://{p}")
            continue

        # 4 части через ":" → host:port:user:pass ИЛИ user:pass:host:port
        parts = p.split(":")
        if len(parts) == 4:
            # Если второй элемент — число, это port → формат host:port:user:pass
            if parts[1].isdigit():
                host, port, user, pwd = parts
            # Если четвёртый — число, это port → формат user:pass:host:port
            elif parts[3].isdigit():
                user, pwd, host, port = parts
            else:
                print(f"[PROXY WARN] не понял формат: {p} — пропускаем")
                continue
            proxies.append(f"http://{user}:{pwd}@{host}:{port}")
            continue

        # 2 части → host:port (без авторизации)
        if len(parts) == 2 and parts[1].isdigit():
            proxies.append(f"http://{p}")
            continue

        print(f"[PROXY WARN] не понял формат: {p} — пропускаем")
    return proxies


PROXIES: list[str | None] = _parse_proxies(DELIST_PROXIES)

# ── дедупликация сигналов: L1 (TTL) + L2 (persistent) ─────────────
# L1 — короткоживущая защита от near-simultaneous дублей (несколько каналов
#      пишут об одном делистинге).
# L2 — постоянная память: «эту монету уже шортили». Защищает от повторного
#      открытия после рестарта контейнера / получения той же статьи через
#      разные источники (TG, BINANCE poller, TOA).
# L2 пишется только после УСПЕШНОГО open (worker callback), чтобы провалившийся
# open не сделал монету «забытой».
_fired_lock    = threading.Lock()
_fired_coins:  set[str] = set()
_fired_expiry: dict[str, float] = {}
_FIRED_TTL     = 60   # L1: секунд до снятия блокировки монеты

# L2: хранилище отстрелянных монет (любой источник).
# FIX 2026-07-07 (MORPHO-инцидент, зеркально с listing): вечный global-блок
# гасит и НОВЫЕ события по той же монете (делист на другой бирже месяц
# спустя). Теперь dict{coin: fired_ts} с TTL (деф. 72ч,
# env DELIST_GLOBAL_FIRED_TTL_H) — блок живёт на волну одной новости.
_GLOBAL_FIRED_TTL = float(os.getenv("DELIST_GLOBAL_FIRED_TTL_H", "72")) * 3600.0
_global_fired: dict[str, float] = {}    # coin → fired_ts (time.time())
_FIRED_FILE = Path(STATE_DIR) / "delist_fired.json"

# FIX-PERF: dirty-flag для фонового L2-writer'а — вместо thread.start() на
# каждый успешный open (то стоило ~3-15мс в hot-path worker'а под GIL contention).
_fired_dirty = threading.Event()

seen_ids: set[int] = set()
seen_lock = threading.Lock()

# FIX: Binance API gateway требует Binance-специфичные хедера
# (clienttype/lang/Referer/Origin). Браузерные UA/Accept/Sec-* при
# наличии curl_cffi подставляет сам через impersonate-профиль —
# дублировать их нельзя, иначе перетрём «реальный» Chrome-набор и
# Akamai снова увидит несоответствие JA3 ↔ headers.
# В fallback-режиме (requests) добавляем минимальный набор браузерных
# заголовков — TLS-fingerprint всё равно выдаст python-requests, и
# 400 продолжатся, но хотя бы headers будут не «пустые».
_BINANCE_API_HEADERS = {
    "clienttype": "web",
    "lang": "en",
    "Referer": "https://www.binance.com/en/support/announcement-list/",
    "Origin": "https://www.binance.com",
}
_BASE_HEADERS_FALLBACK = {
    **_BINANCE_API_HEADERS,
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
_BASE_HEADERS = _BINANCE_API_HEADERS if _HTTP_BACKEND == "curl_cffi" else _BASE_HEADERS_FALLBACK

# ── Watchdog timestamps ───────────────────────────────────────────
_poller_last_ts: dict[int, float] = {}        # {session_idx: timestamp}
_poller_ts_lock = threading.Lock()
_poller_threads: dict[int, threading.Thread] = {}   # FIX: учёт активных потоков

# FIX-batch-8: per-poller адаптивный интервал — каждый поллер имеет свой
# текущий интервал. После 429/ошибки он растёт в POLL_BACKOFF_MULT раз
# (до POLL_BACKOFF_MAX), через POLL_BACKOFF_RECOVERY секунд после
# последнего bump'а (а не успеха!) возвращается к POLL_INTERVAL_BASE.
# FIX: трекаем last_bump_ts, а не last_ok_ts — иначе reset никогда не
# срабатывал, потому что _mark_ok обновлял last_ok_ts перед проверкой.
_poller_intervals: dict[int, float] = {}
_poller_last_bump_ts: dict[int, float] = {}
_poller_intervals_lock = threading.Lock()

# Создаём отдельную сессию для каждого прокси.
# FIX: при наличии curl_cffi — используем его (TLS-impersonation
# Chrome). Headers всё равно ставим — это hint для CDN'а
# (User-Agent логируется), но решающий фактор для Akamai —
# JA3/JA4 fingerprint.
_sessions: list = []
for proxy in PROXIES:
    s = _new_http_session(proxy)
    s.headers.update(_BASE_HEADERS)
    _sessions.append(s)

# Инициализируем timestamps для watchdog + интервалы для backoff
for _i in range(len(_sessions)):
    _poller_last_ts[_i] = time.monotonic()
    _poller_intervals[_i] = POLL_INTERVAL_BASE
    # 0 = ни одного bump'а ещё не было → нет смысла ресетить
    _poller_last_bump_ts[_i] = 0.0

# Startup-probe: сессии (прокси), которые не прошли первичный health-check.
# Поллер для них стартует сразу в DEAD-режиме (60с интервал), чтобы:
#   • не выпускать burst из 5 одинаковых ошибок в первые 15с,
#   • не молотить TCP connect() в воду 1× в секунду по мёртвому endpoint'у,
#   • DEAD-режим всё равно переодически перепроверяет — если прокси
#     поднимется, поллер сам выйдет из DEAD на первом 200.
# Заполняется ниже в _probe_proxy_health() (после построения сессий, до
# запуска поток-поллеров main()).
_poller_initial_dead: set[int] = set()


def _probe_proxy_health() -> None:
    """
    Быстрая проверка каждой сессии: 1 запрос к реальному Binance endpoint
    с коротким timeout. Сессии, которые мгновенно падают (мёртвый прокси:
    Connection refused / DNS-фейл / SOCKS handshake error) сразу помечаются
    как DEAD и стартуют поллер в DEAD-режиме.

    Это не валидация раз и навсегда: DEAD-режим перепроверяет с интервалом
    POLL_DEAD_INTERVAL (60с), так что временно недоступный прокси сам
    воскреснет на первом 200. Цель — убрать лог-шум на старте.

    Запускается в N параллельных потоках, чтобы probe не растягивался на
    N×timeout секунд (для 4 прокси с 3с timeout — 12с старта → 3с).
    """
    if not _sessions:
        return

    # FIX-INCIDENT: форсим pageSize=max, чтобы probe-ответ принёс полный
    # набор актуальных статей — этим набором pre-fill'ним seen_ids ДО
    # старта поллеров. Иначе первый запрос поллера может попасть на
    # random pageSize=5, в seen_ids уйдут 5 ID, а следующий запрос с
    # pageSize=20 принесёт 15 «новых» (исторических) → mass open shorts.
    test_url = _build_binance_url(force_page_size=max(BINANCE_PAGE_SIZES))
    results: dict[int, str] = {}
    results_lock = threading.Lock()
    prefilled_ids: list[int] = []
    prefilled_lock = threading.Lock()

    def _probe_one(idx: int) -> None:
        sess = _sessions[idx]
        label = f"proxy[{idx}]" if idx > 0 else "direct"
        try:
            r = sess.get(test_url, timeout=3)
            r.raise_for_status()
            with results_lock:
                results[idx] = f"[{label}] OK ({r.status_code})"
            # FIX-INCIDENT: pre-fill seen_ids из ответа любой живой сессии.
            # Несколько сессий могут отдать пересекающиеся наборы — set'у
            # пофиг на дубликаты. Берём всё что распарсилось; ошибка
            # парсинга не валит probe — мы здесь не для этого.
            try:
                if _parse_binance_articles is not None:
                    arts = _parse_binance_articles(r.content)
                else:
                    d = _json_loads(r.content).get("data", {})
                    cats = d.get("catalogs") or [{}]
                    arts = d.get("articles") or (
                        cats[0].get("articles", []) if isinstance(cats[0], dict) else []
                    )
                with prefilled_lock:
                    for a in arts:
                        aid = a.get("id")
                        if aid is not None:
                            prefilled_ids.append(aid)
            except Exception:  # noqa: BLE001
                pass
        except Exception as e:  # noqa: BLE001
            with results_lock:
                results[idx] = f"[{label}] DEAD: {e!r}"
            # Помечаем как DEAD только при connection-уровне ошибке
            # или HTTP 4xx-блоке (это не лечится в коде; нужны другие IP).
            if _is_connection_error(e) or _is_http_block(e):
                _poller_initial_dead.add(idx)

    threads = []
    for i in range(len(_sessions)):
        t = threading.Thread(target=_probe_one, args=(i,), daemon=True,
                             name=f"probe-{i}")
        t.start()
        threads.append(t)
    for t in threads:
        t.join(timeout=5)

    alive = len(_sessions) - len(_poller_initial_dead)
    print(f"[PROXY-PROBE] {alive}/{len(_sessions)} сессий живы:", flush=True)
    for idx in sorted(results):
        marker = "✗" if idx in _poller_initial_dead else "✓"
        print(f"  {marker} {results[idx]}", flush=True)

    # FIX-INCIDENT: pre-fill seen_ids ДО старта поллеров. После этого даже
    # если у поллера random выдаст pageSize=5 на первом запросе — все 5
    # ID уже в seen_ids (из probe c pageSize=20), и логика "новые статьи"
    # сработает корректно (0 новых → 0 process_article).
    if prefilled_ids:
        with seen_lock:
            seen_ids.update(prefilled_ids)
        print(
            f"[PROXY-PROBE] seen_ids pre-filled: {len(set(prefilled_ids))} "
            f"уникальных статей забанено до старта поллеров",
            flush=True,
        )


# Probe вызывается из main(), перед запуском поллер-потоков —
# к тому моменту все вспомогательные функции (_build_binance_url,
# _is_connection_error, _is_http_block) уже определены ниже по модулю.

# Циклический итератор по сессиям
_session_cycle = itertools.cycle(enumerate(_sessions))
_session_lock  = threading.Lock()


def _next_session() -> tuple[int, object]:
    with _session_lock:
        return next(_session_cycle)


# ── FIX-batch-8: cache-busting URL builder ────────────────────────

def _build_binance_url(force_page_size: int | None = None) -> str:
    """
    Собирает URL к Binance article API.

    FIX: убран `_t=<ms-timestamp>` cache-busting параметр. Binance WAF
    отбивает HTTP 400 любой запрос с неизвестным query-параметром (whitelist:
    только type/catalogId/pageNo/pageSize). Был воспроизведён `curl` без
    кук/headers — с `_t` → 400, без `_t` → 200. Это и было настоящей причиной
    «delist parser ничего не получает» (а не TLS/JA3/IP-блок, как казалось).

    Cache-busting на стороне CloudFront остаётся через ротацию pageSize ∈
    {5,10,15,20} — это 4 разных cache key, чего достаточно при interval=1с,
    учитывая что у нас несколько поллеров (direct + N proxies).

    FIX-INCIDENT: force_page_size форсит конкретное значение pageSize. Нужно
    для первого запроса поллера: если на нём попадёт случайно pageSize=5,
    в seen_ids уйдёт только 5 ID, и на следующем запросе с pageSize=20 ещё
    15 «старых» статей будут считаться новыми → массовый process_article →
    шорты по всем подряд тикерам. Поллер обязан на старте взять max(20)
    чтобы гарантированно набрать полный «бан-лист» исторических статей.
    """
    page_sz = force_page_size if force_page_size is not None else random.choice(BINANCE_PAGE_SIZES)
    return (
        f"{BINANCE_API_URL}"
        f"?type=1&catalogId={CATEGORY_ID}&pageNo=1&pageSize={page_sz}"
    )


# ── Heartbeat для docker healthcheck ──────────────────────────────
# FIX: пишем timestamp в файл — docker healthcheck читает его,
# и если он "протух" — перезапускает контейнер.
HEARTBEAT_FILE = Path(os.getenv("HEARTBEAT_FILE", "/tmp/delist_heartbeat"))


def _touch_heartbeat() -> None:
    try:
        HEARTBEAT_FILE.touch(exist_ok=True)
    except Exception:
        pass


# ── Логирование ──────────────────────────────────────────────────

def _log(tag: str, color: str, msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"{color}{BOLD}[{ts}][{tag}]{RESET} {msg}", flush=True)

def log_info(tag: str, msg: str):  _log(tag, CYAN,   msg)
def log_ok(tag: str, msg: str):    _log(tag, GREEN,  msg)
def log_warn(tag: str, msg: str):  _log(tag, YELLOW, msg)
def log_err(tag: str, msg: str):   _log(tag, RED,    msg)


# ── Дедупликация сигналов ─────────────────────────────────────────

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
            # FIX 2026-07-07: новый формат dict {coin: fired_ts}; легаси-список
            # получает mtime файла (консервативно — истечёт максимум через TTL).
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
        log_ok("DEDUP", f"L2 загружен: global={len(_global_fired)} (TTL {_GLOBAL_FIRED_TTL/3600:.0f}ч)")
    except Exception as e:  # noqa: BLE001
        log_warn("DEDUP", f"L2 load failed: {e!r} — стартуем с пустого")


def _persist_fired_state() -> None:
    """Атомарный сейв L2 (write+rename). Вызывается фоновым writer'ом."""
    try:
        with _fired_lock:
            # FIX 2026-07-07: global — dict {coin: fired_ts} (TTL-семантика).
            snapshot = {"global": dict(sorted(_global_fired.items()))}
        # FIX: используем уже импортированный _json_dumps вместо дубликата
        payload = _json_dumps(snapshot, indent=True)
        _FIRED_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _FIRED_FILE.with_suffix(".json.tmp")
        tmp.write_bytes(payload)
        tmp.replace(_FIRED_FILE)
    except Exception as e:  # noqa: BLE001
        log_warn("DEDUP", f"L2 persist failed: {e!r}")


# ── Signal-stats: журнал событий + отчёты 09:00/22:00 МСК (DELIST) ─
# Параллель listing-парсера. Отдельные файлы (delist_*) — не конфликтуют
# с listing при общем STATE_DIR volume. Отчёт идёт отдельным TG-сообщением
# с заголовком 🔴 DELIST.
try:
    from tg.signal_stats import SignalStats
    _sig_stats = SignalStats(
        Path(STATE_DIR) / "delist_signal_events.jsonl",
        Path(STATE_DIR) / "delist_stats_report_state.json",
        kind="DELIST",
    )
except Exception as _se:  # noqa: BLE001
    print(f"[SIGSTATS] init failed: {_se!r} — signal-stats отключены", flush=True)
    _sig_stats = None

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

# ── Health-stats (DELIST) ─────────────────────────────────────────
# FIX 2026-06-06: раньше health был ТОЛЬКО в listing-парсере — делисты не
# попадали в health-отчёт. Отдельный файл delist_health_stats.json.
try:
    from tg.health_stats import HealthStats
    _health = HealthStats(Path(STATE_DIR) / "delist_health_stats.json")
except Exception as _he:  # noqa: BLE001
    print(f"[HEALTH] init failed: {_he!r} — health-stats отключены", flush=True)
    _health = None

# ── Price-recorder (захват головы) + eval-поллер 6ч-итогов (DELIST) ─
# Гибрид: рекордер ловит только первые ~10 мин входа (price_paths.jsonl), а по
# истечении 6ч eval-поллер дотягивает хвост свечами (klines), склеивает,
# оценивает 12 стратегий, копит винрейт и шлёт 6ч-итоги в TG.
# get_price импортирован выше из api.delist_api (читает тот же price_cache).
_EVAL_LEVERAGE, _EVAL_TAKER, _EVAL_MARGIN = 10.0, 0.00055, 10.0
_EVAL_WINDOW_SEC = 21600          # 6ч — момент оценки
_price_paths_path = Path(STATE_DIR) / "delist_price_paths.jsonl"
_strat_results_path = Path(STATE_DIR) / "delist_strategy_results.jsonl"
_eval_paths_path = Path(STATE_DIR) / "delist_eval_paths.jsonl"
try:
    from tg.price_recorder import PriceRecorder
    from tg.strategy_eval import evaluate as _strat_evaluate, stitch as _stitch
    # Рекордер = чистый захват головы (без on_complete; оценку делает eval-поллер).
    _recorder = PriceRecorder(_price_paths_path, get_price)
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


# FIX 2026-06-18: _health_report_loop удалён — TG-отчёт каждые 6ч отключён.

# Первый источник, заявивший монету (для метрики отставания). coin -> (src, t).
_first_claim_ts: dict[str, tuple[str, float]] = {}
_FIRST_CLAIM_TTL = 300


def _sig_open(coin: str, src: str, venue: str, outcome: str,
              open_ms: float = 0.0, entry: float = 0.0, amount: float = 0.0,
              listing_exchange: str = "") -> None:
    if _sig_stats is not None:
        try:
            _sig_stats.log_open(coin, src, venue, outcome, open_ms, entry,
                                amount, listing_exchange)
        except Exception:  # noqa: BLE001
            pass


def _sig_lag(coin: str, loser: str, winner: str, lag_ms: float) -> None:
    if _sig_stats is not None:
        try:
            _sig_stats.log_lag(coin, loser, winner, lag_ms)
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


def _measure_lag(coin: str) -> tuple[str, float] | None:
    """(первый_источник, отставание_сек) если монету уже заявлял другой src."""
    now = time.monotonic()
    with _fired_lock:
        first = _first_claim_ts.get(coin)
    if first is None:
        return None
    first_src, t0 = first
    return first_src, max(0.0, now - t0)


def _try_claim(coin: str, source: str = "") -> bool:
    """
    Возвращает True если монета ещё не в работе и блокирует её на _FIRED_TTL секунд.
    Защищает от двойного открытия одной монеты.
    L2 (_global_fired) — проверка «уже шортили эту монету когда-либо».
    FIX-PERF: было — на каждый claim спавнили sleep-thread для TTL-cleanup.
    threading.Thread.start() ≈ 3-5мс на coin (для 5 монет = 15-25мс в hot-path).
    Теперь один фоновый sweeper (_fired_sweeper) подметает по таймстампу.
    FIX (signal-stats): source-параметр — запоминаем ПЕРВЫЙ источник монеты
    для метрики отставания (_measure_lag).
    """
    with _fired_lock:
        # L2: уже торговали эту монету в окне TTL → skip. Просроченные записи
        # лениво чистим (FIX 2026-07-07: вечный блок гасил новые события
        # по той же монете — MORPHO-инцидент на listing-стороне).
        ts = _global_fired.get(coin)
        if ts is not None:
            if (time.time() - ts) < _GLOBAL_FIRED_TTL:
                return False
            _global_fired.pop(coin, None)
            _fired_dirty.set()
        # L1: монета сейчас обрабатывается (TTL) → skip.
        if coin in _fired_coins:
            return False
        _fired_coins.add(coin)
        _fired_expiry[coin] = time.monotonic() + _FIRED_TTL
        if source and coin not in _first_claim_ts:
            _first_claim_ts[coin] = (source, time.monotonic())
    return True


def _mark_opened(coin: str) -> None:
    """
    Worker зовёт после успешного open. Обновляет L2 (in-memory) и поднимает
    dirty-flag — фоновый writer (_fired_persist_loop) сохранит на диск.

    FIX-PERF: НЕ спавним поток здесь — это hot-path worker'а. Только dict-set
    (~1мкс) + Event.set (~1мкс).
    """
    dirty = False
    with _fired_lock:
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


def _fired_sweeper() -> None:
    """FIX-PERF: единый поток для TTL-cleanup L1 _fired_coins. L2 — навсегда."""
    while True:
        time.sleep(5)
        now = time.monotonic()
        with _fired_lock:
            expired = [c for c, ts in _fired_expiry.items() if ts <= now]
            for c in expired:
                _fired_coins.discard(c)
                _fired_expiry.pop(c, None)
            # Чистим _first_claim_ts (метрика отставания) по своему TTL.
            stale = [c for c, (_s, t0) in _first_claim_ts.items()
                     if now - t0 > _FIRST_CLAIM_TTL]
            for c in stale:
                _first_claim_ts.pop(c, None)


# ── Fetch ────────────────────────────────────────────────────────

def extract_text_from_node(node: dict | str | list) -> str:
    if isinstance(node, str):  return node
    if isinstance(node, list): return " ".join(extract_text_from_node(n) for n in node)
    if isinstance(node, dict):
        if node.get("node") == "text": return node.get("text", "")
        return " ".join(extract_text_from_node(c) for c in node.get("child", []))
    return ""


def fetch_article_content(article_code: str) -> str:
    _, session = _next_session()
    try:
        resp = session.get(
            ARTICLE_DETAIL_URL,
            params={"articleCode": article_code},
            timeout=5,
        )
        resp.raise_for_status()
        # FIX-batch-1: orjson вместо resp.json() (3-5x быстрее).
        data = _json_loads(resp.content).get("data", {})
        body = data.get("body", "")
        if isinstance(body, str):
            try: body = _json_loads(body)
            except Exception: pass
        if isinstance(body, dict):
            return extract_text_from_node(body)
        content = data.get("content", "")
        if isinstance(content, dict): content = content.get("body", "")
        return re.sub(r"<.*?>", " ", content)
    except Exception as e:
        log_err("FETCH", f"Ошибка загрузки статьи code={article_code}: {e}")
        return ""


def is_delist_article(title: str) -> bool:
    tl = title.lower()
    return any(kw.lower() in tl for kw in DELIST_KEYWORDS)


# ── Воркер ───────────────────────────────────────────────────────

def worker(coin: str, margin: float, t_start: float, source: str = "BINANCE", retries: int = 3) -> None:
    """
    Воркер — FIX 2026-07-07 (INVERT): на делисте открываем ЛОНГ (отскок после
    дампа), логирует время, ставит TP/SL в фоне через listing-механику.
    """
    last_fail = "failed"
    for attempt in range(1, retries + 1):
        try:
            # FIX-PERF: "Старт"-print только на retry'ях — на attempt 1
            # форматированный print → stdout = ~1-3мс перед market_open_long
            # в hot-path. Открытие позиции важнее, чем factual лог.
            if attempt > 1:
                log_info("WORKER", f"[{source}] Retry {attempt}/{retries} → {coin} | margin={margin} USDT")
            amount, entry_price = market_open_long(coin, margin)
            # sentinel (-1, 0) = биржевой REJECT (30228 + пустой Gate и т.д.).
            # Ретрай не починит — fail-fast.
            if amount == -1:
                log_warn("WORKER", f"{coin}: биржи отвергли ордер — fail-fast")
                last_fail = "rejected"
                break
            if not amount:
                # FIX: проверяем что это не последняя попытка, иначе зависнем
                # в бесконечном цикле если market_open_long всегда возвращает 0.
                last_fail = "no_price"
                if attempt < retries:
                    log_warn("WORKER", f"{coin}: нет цены, повтор через 0.1с...")
                    time.sleep(0.1)
                    continue
                else:
                    log_err("WORKER", f"{coin}: нет цены после {retries} попыток")
                    break

            # FIX-PERF: TP/SL submit ДО метрики (~5-20μs, не сдвинет open_ms).
            # ЛОНГ-механика: set_tp_sl_long (роутер bybit/gate из listing_api).
            _tp_sl_executor.submit(set_tp_sl_long, coin, entry_price, amount)

            open_ms = (time.perf_counter() - t_start) * 1000
            log_ok("OPEN", (
                f"[{source}] {coin} | ордер за {BOLD}{open_ms:.0f}мс{RESET}{GREEN} | "
                f"entry={entry_price} | amount={amount:.4f}"
            ))
            # Health: латентность сигнал→ордер по источнику.
            _hstat("latency", f"{source}→order", open_ms)
            # Биржа исполнения лонга (bybit/gate) для venue/PnL —
            # ЛОНГ пишется в listing_api._last_exchange.
            try:
                from api.listing_api import _last_exchange as _dx
                venue = _dx.get(coin, "bybit")
            except Exception:  # noqa: BLE001
                venue = "?"
            # FIX-PERF: tg_log fire-and-forget (см. tg/tg_logger.py).
            tg_log(
                f"🟢 <b>DELIST LONG</b> {coin}\nEntry: {entry_price}\nAmount: {amount:.4f}\nВремя: {open_ms:.0f}мс")

            # FIX-PERF: bookkeeping L2 ПОСЛЕ метрики и tg_log — не влияет
            # на «время от сигнала до ордера». _mark_opened только set.add +
            # Event.set (~2мкс), без thread.start (см. _fired_persist_loop).
            _mark_opened(coin)
            # Signal-stats: успешное открытие + регистрация PnL (только Bybit).
            _sig_open(coin, source, venue, "opened",
                      open_ms=open_ms, entry=entry_price, amount=amount)
            _register_pnl(coin, source, venue)
            # Бэктест: пишем пост-входную траекторию цены (ЛОНГ — инверт).
            _record_path(coin, "long", source, venue, entry_price)
            return
        except Exception as e:
            err_str = str(e)
            log_err("WORKER", f"{coin}: попытка {attempt}/{retries} упала → {e}")
            if "400 Client Error" in err_str or "404 Client Error" in err_str:
                last_fail = "rejected"
                break
            last_fail = "failed"
            if attempt < retries:
                time.sleep(0.1)

    log_err("WORKER", f"{coin}: все {retries} попытки провалились, сдаёмся")
    _hstat("error", f"{source}→order")
    _sig_open(coin, source, "?", last_fail)
    tg_log(f"⚠️ {coin}: все попытки провалились")


# ── FIX-batch-4: callback для Tree of Alpha WS ───────────────────

def _on_toa_delist(full_text: str, t_start: float) -> None:
    """
    Callback из treeofalpha_ws.run_tree_of_alpha_listener.
    Извлекает тикеры и делегирует в process_signal.
    """
    pairs = find_pairs(full_text)
    if not pairs:
        log_warn("TOA-DELIST", f"тикеры не найдены: {full_text[:120]}")
        return
    log_ok("TOA-DELIST", f"Делистинг-сигнал из TOA WS: {pairs}")
    process_signal(pairs, "TOA-WS", t_start)


# ── Общая обработка сигнала ───────────────────────────────────────

# FIX-PERF: модульный long-lived pool для worker'ов делистинга. Раньше
# `with ThreadPoolExecutor(...)` создавал новый пул на каждый сигнал, а
# __exit__ блокировал до завершения всех worker'ов — +5-15мс overhead.
_signal_executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="delist-signal")

# FIX-PERF: отдельный preheated pool для set_tp_sl. Замена thread.start
# (~3-15мс под GIL contention) на submit (~5-20мкс) в hot-path после OPEN.
_tp_sl_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="delist-tpsl")


def process_signal(pairs: list[str], source: str, t_start: float | None = None) -> None:
    """
    Общая функция обработки сигнала делистинга.
    Фильтрует дубли через _try_claim (L1 TTL + L2 persistent), открывает шорты.

    FIX: t_start теперь опционален. Если None — замеряем сами (бэк-совместимость).
    Если передан (из TOA WS / TG handler / poller) — используем точный момент
    прихода сигнала, чтобы метрики OPEN в логах отражали полный путь.
    """
    if t_start is None:
        t_start = time.perf_counter()

    # Health: приход сигнала от каждого источника (видно кто приносит делисты).
    _hstat("signal", source)

    new_pairs = [c for c in pairs if _try_claim(c, source)]
    if not new_pairs:
        # Метрика отставания: для каждой отброшенной монеты считаем, на сколько
        # текущий source опоздал относительно первого (для статистики «кто
        # опережает кого»). См. parser_listing.process_signal.
        lag_info: list[str] = []
        for coin in pairs:
            lag = _measure_lag(coin)
            if lag is not None:
                first_src, dt = lag
                lag_info.append(f"{coin}: {source} опоздал на {dt:.2f}с от {first_src}")
                _sig_lag(coin, source, first_src, dt * 1000.0)
        if lag_info:
            log_warn("SIGNAL-LAG", " | ".join(lag_info))
        else:
            log_warn("SIGNAL", f"[{source}] монеты уже в работе или ранее отстреливались: {pairs}")
        return

    margin = calculate_margin_for_delist()

    # FIX-PERF: для single-coin (99%) — inline worker, экономим executor
    # hop (~1-3мс под GIL contention). Multi-coin → executor для параллелизма.
    # Trade-off: caller блокируется на ~5-10мс на market_open_short.
    # Подробности — см. parser_listing.process_signal.
    if len(new_pairs) == 1:
        worker(new_pairs[0], margin, t_start, source)
    else:
        for coin in new_pairs:
            _signal_executor.submit(worker, coin, margin, t_start, source)

    print(f"\n{BOLD}{'═' * 60}{RESET}")
    log_ok("DELIST", f"[{source}] Новый делистинг!")
    log_info("DELIST", f"Монеты : {new_pairs}")
    log_info("TRADE",  f"Маржа={margin} USDT | открываем {len(new_pairs)} лонг(ов) [INVERT]...")
    print(f"{BOLD}{'═' * 60}{RESET}\n")


# ── Обработка статьи ─────────────────────────────────────────────

# FIX-batch-2: общий ThreadPoolExecutor для всех background fetch'ей,
# вместо создания нового на каждую статью (создание thread'а = ~5-10мс overhead).
_article_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="article-bg")


def _fetch_extra_pairs(article_code: str, already_fired: set[str], t_start: float) -> None:
    """
    FIX-batch-2: фоновый дозабор тикеров из body статьи.
    Запускается ПОСЛЕ того как fast-path уже открыл шорт по тикерам из title.
    Часто в title только 1-2 монеты ("BCC, FUEL"), а в body перечислены ВСЕ
    делистингуемые ("BCC, FUEL, OAX, IRIS, REP, RDN") — мы их подбираем здесь.
    """
    if not article_code:
        return
    try:
        body = fetch_article_content(article_code)
    except Exception as e:
        log_err("FETCH-BG", f"{article_code}: {e}")
        return
    if not body:
        return
    all_pairs = find_pairs(body)
    extras = [p for p in all_pairs if p not in already_fired]
    if extras:
        log_ok("PAIRS-BG", f"Доп тикеры из body: {extras} (fast-path уже открыл: {sorted(already_fired)})")
        process_signal(extras, "BINANCE", t_start)


def process_article(article: dict) -> None:
    article_id   = article.get("id")
    title        = article.get("title", "")
    article_code = article.get("code", "")

    if article_id is None:
        return

    with seen_lock:
        if article_id in seen_ids:
            return
        seen_ids.add(article_id)

    if not is_delist_article(title):
        return

    t_start = time.perf_counter()

    log_ok("DELIST", "Найдена статья о делистинге!")
    log_info("DELIST", f"Title : {title}")
    log_info("DELIST", f"Code  : {article_code}")

    # ── FAST PATH: тикеры в заголовке (0мс) ─────────────────────
    # FIX-batch-2: не дёргаем fetch_article_content зря — в 95% случаев
    # тикеры уже в title. Экономим 200-500мс задержки + HTTP-запрос.
    pairs = find_pairs(title)

    if pairs:
        log_ok("PAIRS", f"Найдено из заголовка (0мс): {pairs}")
        process_signal(pairs, "BINANCE", t_start)

        # FIX-batch-2: после открытия шортов — в фоне дочитываем body,
        # вдруг там есть ДОПОЛНИТЕЛЬНЫЕ тикеры, которых не было в title.
        if article_code:
            _article_executor.submit(
                _fetch_extra_pairs,
                article_code,
                set(pairs),
                t_start,
            )
        return

    # ── SLOW PATH: title без тикеров — тянем body синхронно ─────
    log_info("FETCH", "В заголовке тикеров нет, тянем body...")
    t0 = time.perf_counter()
    full_content = fetch_article_content(article_code) if article_code else ""
    fetch_ms = (time.perf_counter() - t0) * 1000
    full_text = title + " " + full_content
    # Health: латентность детекта источника (зеркало listing UPBIT-NOTICE→detect).
    _hstat("latency", "BINANCE-NOTICE→detect", fetch_ms)

    log_info("FETCH", f"Body получен за {fetch_ms:.0f}мс ({len(full_text)} символов)")

    pairs = find_pairs(full_text)
    if not pairs:
        log_warn("PAIRS", f"Тикеры не найдены: {title}")
        return

    log_ok("PAIRS", f"Найдено из текста: {pairs}")
    process_signal(pairs, "BINANCE", t_start)


# ── Поллеры ──────────────────────────────────────────────────────

def _bump_interval(session_idx: int) -> None:
    """FIX-batch-8: при ошибке/429 — увеличить интервал поллера."""
    with _poller_intervals_lock:
        cur = _poller_intervals.get(session_idx, POLL_INTERVAL_BASE)
        _poller_intervals[session_idx] = min(cur * POLL_BACKOFF_MULT, POLL_BACKOFF_MAX)
        _poller_last_bump_ts[session_idx] = time.monotonic()


def _reset_interval_if_recovered(session_idx: int) -> None:
    """
    Если с момента последнего bump'а прошло POLL_BACKOFF_RECOVERY секунд —
    возвращаем базовый интервал.

    FIX: раньше reset смотрел на last_ok_ts, который только что обновил _mark_ok.
    Условие (now - last_ok) < POLL_BACKOFF_RECOVERY всегда было True → reset
    никогда не срабатывал → один 429 поднимал интервал до POLL_BACKOFF_MAX
    навсегда. Теперь трекаем last_bump_ts — это единственный осмысленный
    timestamp для recovery-логики.
    """
    now = time.monotonic()
    with _poller_intervals_lock:
        cur_interval = _poller_intervals.get(session_idx, POLL_INTERVAL_BASE)
        if cur_interval <= POLL_INTERVAL_BASE:
            return
        last_bump = _poller_last_bump_ts.get(session_idx, 0.0)
        if last_bump == 0.0:
            # bump'ов не было — нечего сбрасывать
            return
        if (now - last_bump) < POLL_BACKOFF_RECOVERY:
            return
        _poller_intervals[session_idx] = POLL_INTERVAL_BASE
        _poller_last_bump_ts[session_idx] = 0.0


# FIX-3: _mark_ok был no-op после реструктуризации (FIX-batch-8) — удалён,
# его вызов в poller() тоже снят. Watchdog timestamp обновляется напрямую.


def _get_interval(session_idx: int) -> float:
    with _poller_intervals_lock:
        return _poller_intervals.get(session_idx, POLL_INTERVAL_BASE)


def _is_connection_error(exc: BaseException) -> bool:
    """
    True если ошибка похожа на «прокси/сеть мертва» (Connection refused,
    timeout на handshake, ProxyError). Используется для перехода в
    DEAD-режим с длинным интервалом — иначе мёртвый прокси спамит
    1 ошибкой в секунду.

    FIX: добавлены curl_cffi-паттерны — раньше ловили только requests'ы
    стиль ('Connection refused', 'Max retries exceeded'), а curl_cffi
    шлёт другие сообщения ('Failed to perform, curl: (7) ...',
    'Resolving timed out after ...'). Без этого DEAD-режим не
    срабатывал и логи захлёбывались 1 ошибкой/сек.
    """
    s = repr(exc)
    return (
        isinstance(exc, (ConnectionError, TimeoutError))
        # requests/urllib3 стиль
        or "ProxyError" in s
        or "Connection refused" in s
        or "Failed to establish a new connection" in s
        or "Max retries exceeded" in s
        or "timed out" in s
        or "Connection reset" in s
        # curl_cffi стиль (libcurl error messages)
        or "Failed to perform" in s
        or "Failed to connect to" in s
        or "Could not connect to server" in s
        or "Resolving timed out" in s
        or "Could not resolve host" in s
        or "curl: (7)" in s   # CURLE_COULDNT_CONNECT
        or "curl: (28)" in s  # CURLE_OPERATION_TIMEDOUT
        or "curl: (6)" in s   # CURLE_COULDNT_RESOLVE_HOST
        or "curl: (35)" in s  # CURLE_SSL_CONNECT_ERROR
        or "curl: (56)" in s  # CURLE_RECV_ERROR
    )


def _is_http_block(exc: BaseException) -> bool:
    """
    True если ошибка — устойчивый HTTP-блок (400/403). Это не транзиентка
    и не сетевая дыра, а WAF/IP-блок на стороне Binance: дальнейшие
    запросы 100% дадут то же самое. Лечится только сменой egress IP
    (другой прокси / резидентные прокси / VPN), кодом — никак.
    Используется тем же DEAD-режимом, чтобы не спамить лог 400-ми.

    Распознаём по тексту, потому что:
      • requests шлёт HTTPError с '400 Client Error' / '403 Forbidden'
      • curl_cffi (через .raise_for_status()) шлёт 'HTTP Error 400:'
    """
    s = repr(exc)
    return (
        "HTTP Error 400" in s
        or "HTTP Error 403" in s
        or "400 Client Error" in s
        or "403 Client Error" in s
    )


def poller(session_idx: int) -> None:
    label   = f"proxy[{session_idx}]" if session_idx > 0 else "direct"
    session = _sessions[session_idx]

    log_ok("POLLER", f"[{label}] запущен (interval={POLL_INTERVAL_BASE}с base, http={_HTTP_BACKEND})")

    first_run = True
    # FIX-4: счётчик циклов с повышенным интервалом — раз в N циклов
    # логируем варн, иначе деградация невидима (флапающий прокси →
    # interval застрянет на POLL_BACKOFF_MAX, recovery никогда не сработает).
    elevated_cycles = 0
    _ELEVATED_WARN_EVERY = 60

    # FIX: трекинг подряд connection-error'ов → переход в DEAD-режим
    # (60с интервал, лог раз в N тиков). Любой успех — сброс.
    conn_err_streak = 0
    # Если startup-probe пометил эту сессию мёртвой — стартуем сразу в DEAD,
    # чтобы не выпускать burst из 5 ошибок подряд в логи в первые 15с.
    dead_mode       = session_idx in _poller_initial_dead
    dead_log_skip   = 0
    if dead_mode:
        log_warn(
            "POLLER",
            f"[{label}] стартую сразу в DEAD-режиме (startup-probe не прошёл), "
            f"интервал {POLL_DEAD_INTERVAL:.0f}с"
        )

    # FIX: heartbeat для видимости — после первого запуска поллер логирует
    # только при появлении новых статей (line с `if new_count > 0`). Это
    # правильно (не спамить каждую секунду), но создаёт иллюзию что система
    # «висит». Раз в POLL_HEARTBEAT_EVERY успешных циклов печатаем «alive» —
    # тогда пользователь видит что Binance отдаёт 200 и парсер крутится.
    success_count = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        while True:
            try:
                # FIX-batch-8: cache-busting URL — на каждый запрос новый cache key.
                # FIX-INCIDENT: на первом запросе ФОРСИМ pageSize=max(BINANCE_PAGE_SIZES).
                # Иначе если random выдаст pageSize=5, в seen_ids уйдёт всего 5 ID,
                # а следующий запрос с pageSize=20 принесёт 15 «новых» (на самом деле
                # старых) статей → массовый process_article → шорты пачкой. См. инцидент
                # 2026-05-25: 5 → 20 пейдж-сайз скачок открыл позиции по 30+ тикерам.
                url = _build_binance_url(
                    force_page_size=max(BINANCE_PAGE_SIZES) if first_run else None
                )
                resp = session.get(url, timeout=4)
                if resp.status_code == 429:
                    log_warn("POLLER", f"[{label}] 429 — пауза {POLL_BACKOFF_429}с + замедление")
                    _hstat("throttle", "BINANCE-NOTICE")
                    _bump_interval(session_idx)
                    # FIX: обновляем watchdog timestamp — поток жив и делает
                    # прогресс. Иначе после нескольких 429 watchdog ложно
                    # считал поток zombie.
                    with _poller_ts_lock:
                        _poller_last_ts[session_idx] = time.monotonic()
                    time.sleep(POLL_BACKOFF_429)
                    continue
                resp.raise_for_status()

                # FIX-batch-8: в DEBUG mode — печатать X-Cache header, чтобы видеть,
                # попадают ли запросы в CloudFront cache или в origin.
                if os.getenv("DELIST_DEBUG_CACHE") == "1":
                    xc = resp.headers.get("X-Cache", "?")
                    age = resp.headers.get("Age", "?")
                    log_info("POLLER", f"[{label}] X-Cache={xc} Age={age}")

                # msgspec.Struct fast-path — типизированный decoder в 2-3x
                # быстрее orjson+dict при той же логике извлечения.
                if _parse_binance_articles is not None:
                    articles = _parse_binance_articles(resp.content)
                else:
                    # FIX-batch-1: orjson — fallback (3-5x быстрее std json на ~5-20KB).
                    data     = _json_loads(resp.content).get("data", {})
                    # FIX: catalogs может вернуться как [] — старый код data.get("catalogs", [{}])[0]
                    # падал с IndexError, потому что default срабатывал только при отсутствии ключа.
                    catalogs = data.get("catalogs") or [{}]
                    articles = data.get("articles") or (
                        catalogs[0].get("articles", []) if isinstance(catalogs[0], dict) else []
                    )

                # Обновляем watchdog timestamp + успешный ответ — пробуем сбросить backoff
                with _poller_ts_lock:
                    _poller_last_ts[session_idx] = time.monotonic()
                _reset_interval_if_recovered(session_idx)

                # FIX: успех — выходим из DEAD-режима, обнуляем streak.
                if dead_mode or conn_err_streak > 0:
                    if dead_mode:
                        log_ok("POLLER", f"[{label}] восстановилось ✓ выходим из DEAD-режима")
                    dead_mode = False
                    conn_err_streak = 0
                    dead_log_skip = 0

                # Heartbeat: видимое подтверждение что поллер живой и Binance
                # отдаёт 200. Печатаем РЕДКО (раз в POLL_HEARTBEAT_EVERY циклов),
                # чтобы не спамить, но достаточно часто чтобы быть полезным.
                success_count += 1
                if not first_run and success_count % POLL_HEARTBEAT_EVERY == 0:
                    log_ok(
                        "POLLER",
                        f"[{label}] alive ✓ {success_count} успешных pollов, "
                        f"последний ответ — {len(articles) if articles else 0} статей"
                    )

                if articles:
                    if first_run:
                        with seen_lock:
                            for a in articles:
                                aid = a.get("id")
                                if aid is not None:
                                    seen_ids.add(aid)
                        log_info("POLLER", f"[{label}] первый запуск — запомнили {len(articles)} статей, жду новые...")
                        first_run = False
                    else:
                        # FIX-batch-8: логируем только если новые статьи (иначе спам каждую секунду)
                        new_count = 0
                        with seen_lock:
                            for a in articles:
                                aid = a.get("id")
                                if aid is not None and aid not in seen_ids:
                                    new_count += 1
                        if new_count > 0:
                            log_info("POLLER", f"[{label}] получено {len(articles)} статей (новых: {new_count})")
                        # FIX: executor.submit вместо ленивого .map. add_done_callback —
                        # чтобы исключения внутри process_article не тонули в Future
                        # (раньше падал тихо при schema-change у Binance API).
                        def _log_future_exc(fut, _label=label):
                            exc = fut.exception()
                            if exc is not None:
                                log_err("POLLER", f"[{_label}] process_article упал: {exc!r}")
                        for a in articles:
                            fut = executor.submit(process_article, a)
                            fut.add_done_callback(_log_future_exc)
                else:
                    log_warn("POLLER", f"[{label}] статей нет или пустой ответ")

            except Exception as e:
                # FIX: классифицируем ошибку.
                #   • connection error (прокси/DNS/handshake умер) → DEAD
                #   • HTTP 400/403 (IP/ASN-блок Binance WAF) → тоже DEAD,
                #     потому что кодом не лечится: нужны другие egress IP
                #     (резидентные прокси) или другой источник. Лог раз в
                #     N тиков, иначе захлёбываемся 1 ошибкой/сек × 4 поллера.
                #   • прочее (HTTP 5xx, schema-mismatch и т.п.) → обычный
                #     bump (cap 5с) — это транзиентка, должно само пройти.
                is_conn  = _is_connection_error(e)
                is_block = _is_http_block(e)
                _hstat("error", "BINANCE-NOTICE")
                if is_conn or is_block:
                    conn_err_streak += 1
                else:
                    conn_err_streak = 0

                if dead_mode:
                    # В DEAD-режиме логируем только каждый N-й тик.
                    if dead_log_skip == 0:
                        log_warn("POLLER", f"[{label}] DEAD ({conn_err_streak} подряд): {e}")
                    dead_log_skip = (dead_log_skip + 1) % POLL_DEAD_LOG_EVERY
                else:
                    log_err("POLLER", f"[{label}] ошибка: {e}")
                    if (is_conn or is_block) and conn_err_streak >= POLL_DEAD_THRESHOLD:
                        dead_mode = True
                        dead_log_skip = 0
                        reason = "IP/ASN-блок (HTTP 400/403)" if is_block else "сеть/прокси мертва"
                        log_warn("POLLER", f"[{label}] → DEAD-режим: {reason}, интервал {POLL_DEAD_INTERVAL:.0f}с (лог раз в {POLL_DEAD_LOG_EVERY} тика)")
                    else:
                        _bump_interval(session_idx)

                # FIX: обновляем watchdog timestamp даже на ошибке — поток
                # жив и делает прогресс, просто API/прокси отвечает плохо.
                # Без этого watchdog ложно считал поток «zombie» и слал TG
                # алерты каждые 90с (на постоянно отдающем 400 endpoint'е).
                with _poller_ts_lock:
                    _poller_last_ts[session_idx] = time.monotonic()

            if dead_mode:
                # FIX: длинный интервал → меньше шума в логах + меньше
                # бессмысленных connect()'ов на мёртвый прокси.
                time.sleep(POLL_DEAD_INTERVAL)
                continue

            cur_interval = _get_interval(session_idx)
            # FIX-4: видимость деградации — если интервал давно не сбрасывался,
            # значит ошибки случаются чаще чем POLL_BACKOFF_RECOVERY.
            if cur_interval > POLL_INTERVAL_BASE:
                elevated_cycles += 1
                if elevated_cycles % _ELEVATED_WARN_EVERY == 0:
                    log_warn("POLLER", f"[{label}] интервал {cur_interval:.1f}с уже {elevated_cycles} циклов — нестабильное соединение?")
            else:
                elevated_cycles = 0
            # ±jitter чтобы поллеры не били Binance синхронно (анти-стадо).
            # Без jitter'а 3 stagger'нутых поллера сходятся в фазе через
            # несколько циклов и создают пики req/с на стороне Binance.
            jitter = cur_interval * POLL_INTERVAL_JITTER * (2 * random.random() - 1)
            time.sleep(cur_interval + jitter)


# ── Watchdog ──────────────────────────────────────────────────────

# FIX-10: трекаем подряд "alive but stuck" циклы — Python не может убить
# поток, зависший в TLS handshake / requests.get(), поэтому хотя бы
# сообщаем юзеру в TG, чтобы он рестартанул контейнер.
# FIX-13: single-writer (только _watchdog thread читает/пишет) — lock не нужен.
_WATCHDOG_STUCK_ALERT_AFTER = 3   # 3 цикла × 30с = 90с тишины — точно zombie
_poller_stuck_counts: dict[int, int] = {}


def _watchdog() -> None:
    """
    Каждые 30 секунд проверяет что все поллеры живы.
    Если поллер не делал успешных запросов дольше WATCHDOG_TIMEOUT — перезапускает.

    FIX: раньше watchdog был сломан — отсутствовала проверка
    `if age > WATCHDOG_TIMEOUT`, плюс был re-entrant deadlock
    (вложенный `with _poller_ts_lock` внутри уже захваченного _poller_ts_lock).
    Теперь корректно проверяет возраст timestamp и перезапускает только
    зависшие поллеры.
    """
    time.sleep(30)  # даём поллерам время запуститься

    while True:
        time.sleep(30)
        now = time.monotonic()

        # FIX: снимаем snapshot под lock, потом обрабатываем без lock
        with _poller_ts_lock:
            ages = {idx: now - ts for idx, ts in _poller_last_ts.items()}

        for idx, age in ages.items():
            if age <= WATCHDOG_TIMEOUT:
                # Поллер ожил — сбрасываем счётчик stuck
                _poller_stuck_counts[idx] = 0
                continue

            # FIX: проверяем, что старый поток уже мёртв (или мы не знаем о нём)
            old = _poller_threads.get(idx)
            if old is not None and old.is_alive():
                # FIX-10: поток жив, но не пишет timestamp — TLS handshake / socket stuck.
                # Считаем циклы — после N подряд алертим в TG (Python не может убить поток).
                stuck = _poller_stuck_counts.get(idx, 0) + 1
                _poller_stuck_counts[idx] = stuck
                log_warn("WATCHDOG", f"poller[{idx}] завис {age:.0f}с но поток жив — пропускаем рестарт (stuck cycle {stuck})")
                if stuck == _WATCHDOG_STUCK_ALERT_AFTER:
                    tg_log(
                        f"⚠️ <b>WATCHDOG</b>: poller[{idx}] zombie {stuck * 30}с "
                        f"(поток жив, не отвечает). Рекомендуем рестарт контейнера."
                    )
                # Сбрасываем timestamp чтобы не спамить логи каждые 30с
                with _poller_ts_lock:
                    _poller_last_ts[idx] = now
                continue

            log_err("WATCHDOG", f"poller[{idx}] завис ({age:.0f}с без ответа) — перезапускаем")
            tg_log(f"⚠️ <b>WATCHDOG</b>: poller[{idx}] завис {age:.0f}с, перезапуск")
            _poller_stuck_counts[idx] = 0

            with _poller_ts_lock:
                _poller_last_ts[idx] = now

            t = threading.Thread(target=poller, args=(idx,), daemon=True, name=f"poller-{idx}")
            t.start()
            _poller_threads[idx] = t


# ── Telegram listener ─────────────────────────────────────────────

def run_telegram_delist_listener() -> None:
    """
    Слушает TG каналы через Telethon (основной + EXTRA_DELIST_CHANNELS).
    TG_DELIST_KEYWORDS: делистинги Binance, Upbit, Bithumb.
    FIX-batch-3: подписываемся сразу на несколько каналов.
    Кто из каналов опередил — тот и победил (дедуп через _fired_coins, TTL 60с).

    FIX: TelegramClient создаётся ВНУТРИ _run, иначе при reconnect-loop
    (asyncio.run в while True) клиент привязан к закрытому event-loop'у и падает.
    """
    from telethon import TelegramClient, events

    session_path = str(Path(SESSION_DIR) / "delist_session")

    # FIX-batch-3: собираем список всех каналов для подписки.
    # Основной + extras из .env. Дубликаты убираем.
    channels: list[str | int] = [TG_CHANNEL]
    for c in parse_channels(EXTRA_DELIST_CHANNELS):
        if c not in channels:
            channels.append(c)

    async def _run():
        client = TelegramClient(
            session_path,
            api_id=int(TG_API_ID) if TG_API_ID else 0,
            api_hash=TG_API_HASH or "",
            auto_reconnect=True,
            retry_delay=5,
            connection_retries=None,   # бесконечные попытки переподключения
            request_retries=5,
        )

        @client.on(events.NewMessage(chats=channels))
        async def handler(event):
            # FIX-9: t_start первой строкой — унифицируем с parser_listing.py.
            t_start = time.perf_counter()
            try:
                text = event.message.message or ""
            except Exception:
                return

            if not text:
                return

            tl = text.lower()
            if not any(kw in tl for kw in TG_DELIST_KEYWORDS):
                return

            # FIX-batch-6: negative filter — отсекаем Binance Alpha removals и т.п.
            if any(neg in tl for neg in TG_DELIST_NEG):
                return

            # FIX-PERF: НЕ дёргаем await event.get_chat() здесь — это network
            # round-trip к Telegram, 50–200мс на холодном кеше. Берём
            # event.chat_id (sync). Username нужен только для лога — fetch'им
            # ПОСЛЕ запуска worker'а (см. ниже).
            chat_id = getattr(event, "chat_id", 0) or 0

            pairs = find_pairs(text)
            if not pairs:
                log_warn("TG-DELIST", f"Тикер не найден: {text[:80]}")
                return

            source = f"TG:{chat_id}"

            # FIX-PERF: process_signal НАПРЯМУЮ вместо threading.Thread —
            # экономия ~3-5мс на спавн доп. потока. Submit'ы внутри
            # process_signal уже идут до print'ов (см. process_signal).
            try:
                process_signal(pairs, source, t_start)
            except Exception as exc:  # noqa: BLE001
                log_err("TG-DELIST", f"process_signal упал: {exc!r}")

            # FIX-PERF: get_chat() ПОСЛЕ spawn'а — больше не блокирует
            # открытие шорта.
            try:
                chat  = await event.get_chat()
                uname = getattr(chat, "username", "") or ""
            except Exception:
                uname = ""
            log_ok("TG-DELIST", f"Делистинг-сигнал! [{chat_id} @{uname}]: {text[:100]}")

        await client.start()
        log_ok("TG-DELIST", "Telethon подключён | сессия: delist_session")

        # FIX-batch-3: пробуем get_entity для каждого канала
        # (логируем какие нашли, какие нет — для дебага).
        for ch in channels:
            try:
                entity = await client.get_entity(ch)
                log_ok("TG-DELIST", f"  + {getattr(entity, 'title', ch)!r} (@{getattr(entity, 'username', '?')}) ✓")
            except Exception as e:
                log_warn("TG-DELIST", f"  ! {ch}: {e}")

        log_ok("TG-DELIST", f"Слушаем делистинги из {len(channels)} каналов (first-wins)...")
        await client.run_until_disconnected()

    # FIX: asyncio.run вместо get_event_loop().run_until_complete (deprecated)
    asyncio.run(_run())


# ── Graceful shutdown: flush статистики на диск перед остановкой ───
# FIX 2026-06-06: docker при рестарте шлёт SIGTERM → daemon-потоки
# (_health.persist_loop / _sig_stats.flush_loop / _fired_persist_loop)
# убиваются мгновенно, не успев записать накопленное. Из-за этого
# статистика «сбрасывалась» при перезапуске. Здесь синхронно сбрасываем
# всё на диск по SIGTERM/SIGINT/atexit. Идемпотентно (флаг).
_shutdown_done = threading.Event()


def _graceful_shutdown(*_a) -> None:
    if _shutdown_done.is_set():
        return
    _shutdown_done.set()
    print("[SHUTDOWN] flush статистики на диск...", flush=True)
    for label, fn in (
        ("fired-state", _persist_fired_state),
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

    # NOTE: switchinterval оставлен дефолтный (5мс). См. parser_listing.py
    # для деталей: 1мс эмпирически давал регрессию p50 на trade-открытии
    # из-за избыточного GIL pingpong'а между handler/WS-loop/worker thread'ами.

    # FIX-PERF: только gc.freeze() — module-level объекты выезжают в
    # permanent gen и не сканируются. Дефолтные thresholds=(700, 10, 10)
    # оставляем: каждый gen0 sweep остаётся в десятках микросекунд.
    # Подъём порога до 50k превращал паузы в редкие, но крупные (3-15мс)
    # stop-the-world окна — p99 регрессия для hot-path.
    import gc
    gc.freeze()
    print(f"[BOOT] GC frozen: {gc.get_freeze_count()} objects (thresholds={gc.get_threshold()})")

    # FIX 2026-06-06: ставим shutdown-хендлеры в главном потоке — flush
    # статистики на диск при SIGTERM/SIGINT (docker restart) и atexit.
    _install_shutdown_handlers()

    # ── Прогрев бирж ─────────────────────────────────────────────
    threading.Thread(target=price_updater,      daemon=True).start()
    threading.Thread(target=gate_price_updater, daemon=True).start()
    log_ok("CACHE", "price_updater (Bybit + Gate.io) запущен в фоне")

    warmup_bybit_connection()
    warmup_gate_connection()
    preload_lot_steps()
    gate_preload_lot_steps()
    start_bybit_heartbeat()

    # FIX-batch-5: инициализация Bybit V5 WS Trade (persistent connection).
    if BYBIT_WS_TRADE_ENABLED and BYBIT_API_KEY and BYBIT_SECRET_KEY:
        # FIX-PERF: sync WS как основной hot-path; async как резерв.
        # См. parser_listing.py — та же логика.
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

        try:
            from api.bybit_ws_trade import init as bybit_ws_init, start_periodic_warmup
            inst = bybit_ws_init(BYBIT_API_KEY, BYBIT_SECRET_KEY)
            if inst.is_ready(wait_sec=3.0):
                if not sync_ready:
                    if inst.warmup():
                        log_ok("PARSER", "Bybit ASYNC WS Trade готов + прогрет ✓")
                    else:
                        log_ok("PARSER", "Bybit ASYNC WS Trade готов ✓ (warmup не прошёл)")
                    start_periodic_warmup()
                else:
                    log_ok("PARSER", "Bybit ASYNC WS готов (резерв на случай sync-disconnect)")
            else:
                log_warn("PARSER", "Bybit ASYNC WS не подключился за 3с — fallback на REST до коннекта")
        except Exception as e:
            log_err("PARSER", f"Bybit ASYNC WS init упал: {e!r} — будет REST")

        # FIX 2026-06-19 (R3): private WS (order+position). Без wallet.
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
        log_info("PARSER", "Bybit WS Trade отключён (BYBIT_WS_TRADE_ENABLED=0 или нет ключей) — используем REST")

    log_ok("PARSER", "Ждём 5с пока price_cache наполнится...")
    time.sleep(5)

    # Загружаем L2-дедуп с диска ДО запуска поллеров — иначе первый тик
    # после рестарта может повторно отстрелить уже отшорченную монету.
    _load_fired_state()

    # FIX-PERF: pre-warm + chain warmup ДО запуска поллеров. Иначе если
    # poller сразу детектит свежую статью на старте — первый делистинг
    # попадает на cold-path (видели 27мс на listing'е аналогично).
    # См. parser_listing.py для деталей PEP-659 motivation.
    _warm = [_signal_executor.submit(lambda: None) for _ in range(40)]
    _warm += [_tp_sl_executor.submit(lambda: None) for _ in range(40)]
    for f in _warm:
        f.result()
    log_ok("PARSER", "_signal_executor + _tp_sl_executor pre-warmed (40+40)")

    try:
        # FIX 2026-07-07 (INVERT): делист открывает ЛОНГ → греем long-путь
        # (listing_api.warmup_chain), а не шортовый.
        from api.listing_api import warmup_chain as _long_warmup_chain
        sample_signal = "Binance Will Delist BTC"
        for _ in range(30):
            find_pairs(sample_signal)
        ok = _long_warmup_chain(n=30)
        log_ok("PARSER", f"Chain warmup: regex×30 + market_open_long path×{ok}/30 ✓")
    except Exception as e:  # noqa: BLE001
        log_warn("PARSER", f"Chain warmup упал: {e!r} — первый делистинг будет медленнее")

    tg_log(f"🚀 <b>DELIST парсер запущен</b>\nПоллеры: {len(_sessions)}\nБиржи: Bybit + Gate.io (fallback)")
    log_ok("PARSER", f"Запускаем {len(_sessions)} поллера(ов) → base={POLL_INTERVAL_BASE:.1f}с, jitter=±{POLL_INTERVAL_JITTER*100:.0f}%, ~{POLL_INTERVAL_BASE / max(len(_sessions),1):.2f}с между запросами глобально")

    # Startup proxy health-probe — отсеиваем заведомо мёртвые прокси
    # (Connection refused / DNS-фейл / 4xx-блок), чтобы не молотить TCP-connect
    # 1× в секунду по dead endpoint'у и не наполнять лог 5-ю одинаковыми
    # ошибками за первые 15с. Помеченные DEAD поллеры стартуют в DEAD-режиме
    # (60с интервал) и сами восстанавливаются на первом 200.
    _probe_proxy_health()

    # FIX: поллеры — daemon=True. Если main thread (TG listener) умирает,
    # Docker должен иметь возможность перезапустить контейнер.
    for i in range(len(_sessions)):
        t = threading.Thread(target=poller, args=(i,), daemon=True, name=f"poller-{i}")
        t.start()
        _poller_threads[i] = t
        # FIX-batch-8: stagger между запусками поллеров.
        # Делим POLL_INTERVAL_BASE на число поллеров, чтобы они стартовали равномерно
        # и median детекции была не INTERVAL/2, а INTERVAL/(2*N).
        time.sleep(POLL_INTERVAL_BASE / max(len(_sessions), 1))

    # Watchdog
    threading.Thread(target=_watchdog, daemon=True, name="watchdog").start()
    log_ok("PARSER", f"Watchdog запущен (таймаут {WATCHDOG_TIMEOUT}с)")

    # FIX-PERF: глобальный sweeper для L1 _fired_coins вместо thread-per-claim.
    threading.Thread(target=_fired_sweeper, daemon=True, name="fired-sweeper").start()
    # FIX-PERF: единый фоновый L2-writer (dirty-flag + 1с batching) вместо
    # thread.start на каждый _mark_opened в hot-path.
    threading.Thread(target=_fired_persist_loop, daemon=True, name="fired-persist").start()

    # Signal-stats: журнал событий (flush 10с). Авто-отчёт 09:00/22:00 МСК
    # теперь шлёт лог-бот одним сообщением (оба парсера) — см. tg/log_bot.py.
    if _sig_stats is not None:
        threading.Thread(
            target=_sig_stats.flush_loop, kwargs={"batch_sec": 10.0},
            daemon=True, name="sigstats-flush",
        ).start()
        log_ok("STATS", "signal-stats журнал активен")
    # Closed-PnL поллер Bybit — дописывает PnL закрытых шортов в статистику.
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

    # Health-stats: только персистентность метрик. FIX 2026-06-18: TG-отчёт
    # «слабые места» каждые 6ч отключён по запросу — спамил без пользы.
    # Сбор _hstat и persist_loop остались (метрики копятся на диск).
    if _health is not None:
        threading.Thread(
            target=_health.persist_loop, kwargs={"batch_sec": 30.0},
            daemon=True, name="health-persist",
        ).start()
        log_ok("HEALTH", "сбор диагностики делиста активен (TG-отчёт отключён)")

    # Price-recorder: захват «головы» входа (первые 10 мин) для бэктеста выходов.
    if _recorder is not None:
        threading.Thread(
            target=_recorder.run_loop, daemon=True, name="price-recorder",
        ).start()
        log_ok("PRICE-REC", "рекордер головы входа активен (10 мин, klines-хвост)")
    # Eval-поллер: через 6ч дотягивает klines, оценивает стратегии, шлёт итоги.
    if _strat_evaluate is not None:
        threading.Thread(target=_eval_loop, daemon=True, name="strat-eval").start()

    # FIX-batch-4: Tree of Alpha free WS — параллельный источник делистингов.
    if TREE_OF_ALPHA_WS_ENABLED:
        try:
            from api.treeofalpha_ws import run_tree_of_alpha_listener
            threading.Thread(
                target=run_tree_of_alpha_listener,
                kwargs={
                    "delist_callback": _on_toa_delist,
                    "on_disconnect": lambda: _hstat("disconnect", "TOA-WS"),
                },
                daemon=True,
                name="toa-ws",
            ).start()
            log_ok("PARSER", "Tree of Alpha WS запущен (free public feed)")
        except Exception as e:
            log_err("PARSER", f"Не удалось запустить TOA WS: {e}")

    # FIX: heartbeat пишет timestamp в файл — docker healthcheck читает.
    def heartbeat():
        # FIX (review L1): window-based — ровно 1 алерт на 2ч-окно.
        # Раньше `% 7200 < 60` мог дать 0 или 2 алерта (дрейф sleep).
        _last_alert_window = -1
        while True:
            _touch_heartbeat()
            time.sleep(60)
            window = int(time.time()) // 7200
            if window != _last_alert_window:
                _last_alert_window = window
                tg_log("✅ <b>DELIST парсер работает</b>")

    threading.Thread(target=heartbeat, daemon=True, name="heartbeat").start()

    log_ok("PARSER", "Запускаем Telegram delist listener...")
    while True:
        try:
            run_telegram_delist_listener()
        except KeyboardInterrupt:
            _graceful_shutdown()
            break
        except Exception as e:
            _hstat("error", "TG-LISTENER")
            log_err("PARSER", f"TG listener упал: {e} — перезапуск через 10с")
            tg_log(f"⚠️ <b>DELIST</b>: TG listener упал, перезапуск через 10с\n{e}")
        time.sleep(10)