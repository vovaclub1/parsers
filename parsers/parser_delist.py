from __future__ import annotations

import asyncio
import os
import random
import re
import threading
import time
import itertools
import requests
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# FIX-batch-1: orjson в хот path Binance article parsing (3-5x быстрее).
try:
    import orjson as _orjson  # type: ignore[import-not-found]
    def _json_loads(b: bytes | str):
        if isinstance(b, str):
            b = b.encode()
        return _orjson.loads(b)
except ImportError:  # graceful fallback
    import json as _stdjson
    def _json_loads(b: bytes | str):
        if isinstance(b, (bytes, bytearray)):
            b = b.decode()
        return _stdjson.loads(b)

from config.config import (
    TG_API_ID, TG_API_HASH, DELIST_PROXIES, SESSION_DIR,
    EXTRA_DELIST_CHANNELS, parse_channels,    # FIX-batch-3: multi-channel
    TREE_OF_ALPHA_WS_ENABLED,                  # FIX-batch-4: TOA WS
    BYBIT_WS_TRADE_ENABLED,                    # FIX-batch-5: Bybit WS Trade
    BYBIT_API_KEY, BYBIT_SECRET_KEY,
)
from api.delist_api import (
    find_pairs,
    calculate_margin_for_delist,
    price_updater,
    set_tp_sl,
    market_open_short,
    warmup_bybit_connection,
    preload_lot_steps,
    gate_price_updater,
    gate_preload_lot_steps,
    warmup_gate_connection,
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

# FIX-batch-8: 3с → 1с базового интервала.
# С N=3 прокси и stagger даёт median детекции ~500мс вместо ~1500мс.
# Хардкод (не env) — это публичный параметр поллера, не персональные данные.
POLL_INTERVAL_BASE = 1.0
# FIX-batch-8: per-poller адаптивный backoff при 429 — поллер, который
# нарвался на rate limit, делает свой следующий запрос с увеличенным
# интервалом, восстанавливается через POLL_BACKOFF_RECOVERY секунд.
POLL_BACKOFF_429   = 30.0   # пауза при HTTP 429 (как раньше)
POLL_BACKOFF_MULT  = 1.5    # после ошибки этот поллер замедляется в 1.5x
POLL_BACKOFF_MAX   = 5.0    # потолок индивидуального интервала
POLL_BACKOFF_RECOVERY = 60.0  # секунд до возврата к POLL_INTERVAL_BASE

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

# ── дедупликация сигналов ────────────────────────────────────────
_fired_lock   = threading.Lock()
_fired_coins: set[str] = set()
_FIRED_TTL    = 60   # секунд до снятия блокировки монеты

seen_ids: set[int] = set()
seen_lock = threading.Lock()

_BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

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

# Создаём отдельную сессию для каждого прокси
_sessions: list[requests.Session] = []
for proxy in PROXIES:
    s = requests.Session()
    s.headers.update(_BASE_HEADERS)
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    _sessions.append(s)

# Инициализируем timestamps для watchdog + интервалы для backoff
for _i in range(len(_sessions)):
    _poller_last_ts[_i] = time.monotonic()
    _poller_intervals[_i] = POLL_INTERVAL_BASE
    # 0 = ни одного bump'а ещё не было → нет смысла ресетить
    _poller_last_bump_ts[_i] = 0.0

# Циклический итератор по сессиям
_session_cycle = itertools.cycle(enumerate(_sessions))
_session_lock  = threading.Lock()


def _next_session() -> tuple[int, requests.Session]:
    with _session_lock:
        return next(_session_cycle)


# ── FIX-batch-8: cache-busting URL builder ────────────────────────

def _build_binance_url() -> str:
    """
    Собирает URL к Binance article API так, чтобы CloudFront cache key
    был уникальным на каждом запросе. Иначе разные edge'ы могут вернуть
    stale (10-30с задержки от момента публикации статьи).

    Что делаем:
      1. pageSize крутим из whitelist BINANCE_PAGE_SIZES — все эти значения
         валидны для API. Меняя его, попадаем на разные cache key.
      2. _t=<ms-timestamp> — гарантированный uniqueness каждого запроса.
      3. Перемешиваем порядок параметров — у некоторых CDN порядок учитывается.

    FIX: убран дополнительный rnd_name=rnd_val — _t уже даёт уникальный
    cache key, второй random был избыточен (не вреден, но шум).
    """
    page_sz = random.choice(BINANCE_PAGE_SIZES)

    params = [
        ("type", "1"),
        ("pageNo", "1"),
        ("pageSize", str(page_sz)),
        ("catalogId", str(CATEGORY_ID)),
        ("_t", str(int(time.time() * 1000))),  # ms timestamp — гарантированный uniqueness
    ]
    random.shuffle(params)
    qs = "&".join(f"{k}={v}" for k, v in params)
    return f"{BINANCE_API_URL}?{qs}"


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

def _try_claim(coin: str) -> bool:
    """
    Возвращает True если монета ещё не в работе и блокирует её на _FIRED_TTL секунд.
    Защищает от двойного открытия одной монеты.
    """
    with _fired_lock:
        if coin in _fired_coins:
            return False
        _fired_coins.add(coin)

    def _release():
        time.sleep(_FIRED_TTL)
        with _fired_lock:
            _fired_coins.discard(coin)

    threading.Thread(target=_release, daemon=True).start()
    return True


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
    Воркер — открывает шорт, логирует время, ставит TP/SL в фоне.
    """
    for attempt in range(1, retries + 1):
        try:
            log_info("WORKER", f"[{source}] Старт → {coin} | margin={margin} USDT | попытка {attempt}/{retries}")
            amount, entry_price = market_open_short(coin, margin)
            if not amount:
                log_warn("WORKER", f"{coin}: нет цены, повтор через 0.1с...")
                time.sleep(0.1)
                continue

            open_ms = (time.perf_counter() - t_start) * 1000
            log_ok("OPEN", f"[{source}] {coin} | ордер открыт за {BOLD}{open_ms:.0f}мс{RESET}{GREEN}")

            threading.Thread(
                target=set_tp_sl,
                args=(coin, entry_price, amount),
                daemon=True,
            ).start()

            elapsed_ms = (time.perf_counter() - t_start) * 1000
            log_ok("SHORT", (
                f"[{source}] {coin} | entry={entry_price} | amount={amount:.4f} | "
                f"время от статьи до ордера: {BOLD}{elapsed_ms:.0f}мс{RESET}{GREEN}"
            ))
            tg_log(
                f"🔴 <b>DELIST SHORT</b> {coin}\nEntry: {entry_price}\nAmount: {amount:.4f}\nВремя: {elapsed_ms:.0f}мс")
            return
        except Exception as e:
            log_err("WORKER", f"{coin}: попытка {attempt}/{retries} упала → {e}")
            if attempt < retries:
                time.sleep(0.1)

    log_err("WORKER", f"{coin}: все {retries} попытки провалились, сдаёмся")
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

def process_signal(pairs: list[str], source: str, t_start: float) -> None:
    """
    Общая функция обработки сигнала делистинга.
    Фильтрует дубли через _try_claim, открывает шорты.
    """
    new_pairs = [c for c in pairs if _try_claim(c)]
    if not new_pairs:
        log_warn("SIGNAL", f"[{source}] все монеты уже в работе: {pairs}")
        return

    margin = calculate_margin_for_delist()

    print(f"\n{BOLD}{'═' * 60}{RESET}")
    log_ok("DELIST", f"[{source}] Новый делистинг!")
    log_info("DELIST", f"Монеты : {new_pairs}")
    log_info("TRADE",  f"Маржа={margin} USDT | открываем {len(new_pairs)} шорт(ов)...")
    print(f"{BOLD}{'─' * 60}{RESET}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        for coin in new_pairs:
            executor.submit(worker, coin, margin, t_start, source)

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


def _mark_ok(session_idx: int) -> None:
    """Успех. Сам по себе ничего не делает (кроме watchdog ts ниже)."""
    # Watchdog timestamp обновляется отдельно в poller()
    pass


def _get_interval(session_idx: int) -> float:
    with _poller_intervals_lock:
        return _poller_intervals.get(session_idx, POLL_INTERVAL_BASE)


def poller(session_idx: int) -> None:
    label   = f"proxy[{session_idx}]" if session_idx > 0 else "direct"
    session = _sessions[session_idx]

    log_ok("POLLER", f"[{label}] запущен (interval={POLL_INTERVAL_BASE}с base)")

    first_run = True

    with ThreadPoolExecutor(max_workers=10) as executor:
        while True:
            try:
                # FIX-batch-8: cache-busting URL — на каждый запрос новый cache key.
                url = _build_binance_url()
                resp = session.get(url, timeout=4)
                if resp.status_code == 429:
                    log_warn("POLLER", f"[{label}] 429 — пауза {POLL_BACKOFF_429}с + замедление")
                    _bump_interval(session_idx)
                    time.sleep(POLL_BACKOFF_429)
                    continue
                resp.raise_for_status()

                # FIX-batch-8: в DEBUG mode — печатать X-Cache header, чтобы видеть,
                # попадают ли запросы в CloudFront cache или в origin.
                if os.getenv("DELIST_DEBUG_CACHE") == "1":
                    xc = resp.headers.get("X-Cache", "?")
                    age = resp.headers.get("Age", "?")
                    log_info("POLLER", f"[{label}] X-Cache={xc} Age={age}")

                # FIX-batch-1: orjson вместо resp.json() (3-5x быстрее на Binance ответах ~5-20KB).
                data     = _json_loads(resp.content).get("data", {})
                articles = data.get("articles") or (data.get("catalogs", [{}])[0].get("articles", []))

                # Обновляем watchdog timestamp + успешный ответ — сбросить backoff
                with _poller_ts_lock:
                    _poller_last_ts[session_idx] = time.monotonic()
                _mark_ok(session_idx)
                _reset_interval_if_recovered(session_idx)

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
                        # FIX: executor.submit вместо ленивого .map — исключения логируются
                        for a in articles:
                            executor.submit(process_article, a)
                else:
                    log_warn("POLLER", f"[{label}] статей нет или пустой ответ")

            except Exception as e:
                log_err("POLLER", f"[{label}] ошибка: {e}")
                _bump_interval(session_idx)

            time.sleep(_get_interval(session_idx))


# ── Watchdog ──────────────────────────────────────────────────────

# FIX-10: трекаем подряд "alive but stuck" циклы — Python не может убить
# поток, зависший в TLS handshake / requests.get(), поэтому хотя бы
# сообщаем юзеру в TG, чтобы он рестартанул контейнер.
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
    """
    from telethon import TelegramClient, events

    session_path = str(Path(SESSION_DIR) / "delist_session")
    client = TelegramClient(
        session_path,
        api_id=int(TG_API_ID) if TG_API_ID else 0,
        api_hash=TG_API_HASH or "",
        auto_reconnect=True,
        retry_delay=5,
        connection_retries=None,   # бесконечные попытки переподключения
        request_retries=5,
    )

    # FIX-batch-3: собираем список всех каналов для подписки.
    # Основной + extras из .env. Дубликаты убираем.
    channels: list[str | int] = [TG_CHANNEL]
    for c in parse_channels(EXTRA_DELIST_CHANNELS):
        if c not in channels:
            channels.append(c)

    @client.on(events.NewMessage(chats=channels))
    async def handler(event):
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

        t_start = time.perf_counter()

        try:
            chat    = await event.get_chat()
            chat_id = getattr(chat, "id", 0)
            uname   = getattr(chat, "username", "") or ""
        except Exception:
            chat_id, uname = 0, ""

        log_ok("TG-DELIST", f"Делистинг-сигнал! [{chat_id} @{uname}]: {text[:100]}")

        pairs = find_pairs(text)
        if not pairs:
            log_warn("TG-DELIST", f"Тикер не найден: {text[:80]}")
            return

        threading.Thread(
            target=process_signal,
            args=(pairs, f"TG:@{uname}" if uname else "TG", t_start),
            daemon=True,
        ).start()

    async def _run():
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


if __name__ == "__main__":
    # FIX-batch-1: uvloop — asyncio event loop в 2-4x быстрее.
    # FIX: install() deprecated с 0.18+, используем set_event_loop_policy.
    try:
        import uvloop  # type: ignore[import-not-found]
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("[BOOT] uvloop активирован")
    except ImportError:
        print("[BOOT] uvloop не установлен, использую стандартный asyncio")

    # ── Прогрев бирж ─────────────────────────────────────────────
    threading.Thread(target=price_updater,      daemon=True).start()
    threading.Thread(target=gate_price_updater, daemon=True).start()
    log_ok("CACHE", "price_updater (Bybit + Gate.io) запущен в фоне")

    warmup_bybit_connection()
    warmup_gate_connection()
    preload_lot_steps()
    gate_preload_lot_steps()

    # FIX-batch-5: инициализация Bybit V5 WS Trade (persistent connection).
    if BYBIT_WS_TRADE_ENABLED and BYBIT_API_KEY and BYBIT_SECRET_KEY:
        try:
            from api.bybit_ws_trade import init as bybit_ws_init
            inst = bybit_ws_init(BYBIT_API_KEY, BYBIT_SECRET_KEY)
            if inst.is_ready(wait_sec=3.0):
                log_ok("PARSER", "Bybit WS Trade готов ✓ (−30...−80мс на ордер)")
            else:
                log_warn("PARSER", "Bybit WS Trade не успел подключиться за 3с — fallback на REST до коннекта")
        except Exception as e:
            log_err("PARSER", f"Bybit WS Trade init упал: {e} — будет REST")
    else:
        log_info("PARSER", "Bybit WS Trade отключён (BYBIT_WS_TRADE_ENABLED=0 или нет ключей) — используем REST")

    log_ok("PARSER", "Ждём 5с пока price_cache наполнится...")
    time.sleep(5)

    tg_log(f"🚀 <b>DELIST парсер запущен</b>\nПоллеры: {len(_sessions)}\nБиржи: Bybit + Gate.io (fallback)")
    log_ok("PARSER", f"Запускаем {len(_sessions)} поллера(ов) → ~{POLL_INTERVAL_BASE / max(len(_sessions),1):.2f}с между запросами")

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

    # FIX-batch-4: Tree of Alpha free WS — параллельный источник делистингов.
    if TREE_OF_ALPHA_WS_ENABLED:
        try:
            from api.treeofalpha_ws import run_tree_of_alpha_listener
            threading.Thread(
                target=run_tree_of_alpha_listener,
                kwargs={"delist_callback": _on_toa_delist},
                daemon=True,
                name="toa-ws",
            ).start()
            log_ok("PARSER", "Tree of Alpha WS запущен (free public feed)")
        except Exception as e:
            log_err("PARSER", f"Не удалось запустить TOA WS: {e}")

    # FIX: heartbeat пишет timestamp в файл — docker healthcheck читает.
    def heartbeat():
        while True:
            _touch_heartbeat()
            time.sleep(60)
            # Раз в 2 часа — алерт в TG.
            if int(time.time()) % 7200 < 60:
                tg_log("✅ <b>DELIST парсер работает</b>")

    threading.Thread(target=heartbeat, daemon=True, name="heartbeat").start()

    log_ok("PARSER", "Запускаем Telegram delist listener...")
    while True:
        try:
            run_telegram_delist_listener()
        except KeyboardInterrupt:
            break
        except Exception as e:
            log_err("PARSER", f"TG listener упал: {e} — перезапуск через 10с")
            tg_log(f"⚠️ <b>DELIST</b>: TG listener упал, перезапуск через 10с\n{e}")
        time.sleep(10)