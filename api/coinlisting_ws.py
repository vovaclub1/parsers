from __future__ import annotations

# ── coinlisting_ws.py ─────────────────────────────────────────────
# ULTRA-LOW-LATENCY CoinListing parser
# WSS-листенер для seoul.coinlisting.pro / tokyo.coinlisting.pro.
# ─────────────────────────────────────────────────────────────────

import asyncio
import json
import time
import threading
import re

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosedError

# FIX-batch-1: orjson для парсинга WS-сообщений (3-5x быстрее).
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

from api.listing_api import (
    market_open_long,
    set_tp_sl_long,
    calculate_margin_for_listing,
    find_listing_pairs,
    price_updater,
    gate_price_updater,
    warmup_bybit_connection,
    warmup_gate_connection,
    preload_lot_steps,
    gate_preload_lot_steps,
)
from config.config import COINLISTING_API_KEY
from tg.tg_logger import tg_log

# ── ANSI ──────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def _log(tag: str, color: str, msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"{color}{BOLD}[{ts}][{tag}]{RESET} {msg}", flush=True)

def log_info(tag, msg): _log(tag, CYAN,   msg)
def log_ok(tag, msg):   _log(tag, GREEN,  msg)
def log_warn(tag, msg): _log(tag, YELLOW, msg)
def log_err(tag, msg):  _log(tag, RED,    msg)


# ── Настройки ─────────────────────────────────────────────────────
# FIX: API_KEY больше не захардкожен — берём из .env через config.
if not COINLISTING_API_KEY:
    log_warn("CL", "COINLISTING_API_KEY не задан в .env — WS не подключится")

URL_SEOUL = f"wss://seoul.coinlisting.pro/listings?key={COINLISTING_API_KEY}"
URL_TOKYO = f"wss://tokyo.coinlisting.pro/listings?key={COINLISTING_API_KEY}"

TRADE_SOURCES = {"UPBIT", "BITHUMB"}

COOLDOWN_SEC = 120

_cooldown: dict[str, float] = {}
_cooldown_lock = threading.Lock()

# FIX: _cooldown рос бесконечно — каждый тикер, который когда-либо стрелял,
# оставался в словаре. На длинной дистанции — утечка памяти. Чистим
# протухшие записи на каждой проверке/вставке (это дёшево, dict небольшой).
def _purge_expired_cooldown(now: float | None = None) -> None:
    if now is None:
        now = time.time()
    expired = [t for t, until in _cooldown.items() if until <= now]
    for t in expired:
        _cooldown.pop(t, None)

# FIX: держим ссылки на async-таски, чтобы GC их не убил
# ("Task was destroyed but it is pending").
_pending_tasks: set[asyncio.Task] = set()


def _retire_task(task: "asyncio.Task") -> None:
    """
    done_callback для fire-and-forget create_task'ов. Убирает из множества
    pending'ов и РЕТРИВИТ exception() — иначе asyncio печатает
    "Future exception was never retrieved" при GC Future-объекта. Например,
    `_handle()` может упасть на gaierror внутри `_parse_tokens_from_article_fast`
    в момент DNS-flap'а на старте.
    """
    _pending_tasks.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        log_err("WS", f"handler crashed: {exc!r}")


# ── HTTP Session ─────────────────────────────────────────────────
_http_session: aiohttp.ClientSession | None = None
# FIX: без локa два listener'а (SEOUL/TOKYO) могли одновременно увидеть
# None и создать ДВЕ сессии — первая утекала. Lock ленивая инициализация
# асинхронного singleton'а.
_http_session_lock: asyncio.Lock | None = None


async def get_http_session() -> aiohttp.ClientSession:
    global _http_session, _http_session_lock

    if _http_session is not None and not _http_session.closed:
        return _http_session

    if _http_session_lock is None:
        _http_session_lock = asyncio.Lock()

    async with _http_session_lock:
        if _http_session is not None and not _http_session.closed:
            return _http_session

        timeout = aiohttp.ClientTimeout(
            total=2,
            connect=1,
            sock_read=1,
        )

        connector = aiohttp.TCPConnector(
            limit=100,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )

        _http_session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            },
        )

    return _http_session


# ── Regex ultra-fast ─────────────────────────────────────────────
# FIX: первый символ — буква. Старый паттерн `[A-Z0-9]{2,10}` ловил чисто
# цифровые `(123)` (цены, годы) как «тикер» и потом BANNED их не отсекало.
TOKEN_REGEX = re.compile(rb'\(([A-Z][A-Z0-9]{1,9})\)')

# FIX: убрал дубли (раньше "KRW"/"BTC"/"USDT" повторялись)
BANNED = {
    # Валюты и стейблы
    "KRW", "BTC", "ETH", "USD", "USDT", "USDC", "BUSD", "DAI",
    # Технические
    "NFT", "KST", "UTC", "API", "VIP",
    # Биржи и общие слова в заголовках
    "MARKET", "MARKETS", "LIST",
    "UPBIT", "BITHUMB", "BINANCE", "COINBASE",
}


# ── Cooldown ─────────────────────────────────────────────────────
def _in_cooldown(ticker: str) -> bool:
    now = time.time()
    with _cooldown_lock:
        # FIX: попутно подчищаем протухшие записи — иначе _cooldown
        # растёт неограниченно.
        _purge_expired_cooldown(now)
        until = _cooldown.get(ticker)
        return bool(until and now < until)

def _set_cooldown(ticker: str) -> None:
    now = time.time()
    with _cooldown_lock:
        _purge_expired_cooldown(now)
        _cooldown[ticker] = now + COOLDOWN_SEC


# ── ULTRA FAST ARTICLE PARSER ────────────────────────────────────
# FIX: upbit.com/service_center/notice — SPA-страница с 301 на www, тяжёлая
# (~40KB), а в "плохих" сетевых условиях aiohttp нередко отдаёт пустое тело
# раньше, чем regex успевает что-то найти (видели "no tokens found (217мс)"
# на реальном IO-листинге 2026-05-29 09:00). У Upbit есть лёгкий JSON-эндпоинт
# api-manager.upbit.com/api/v1/announcements/{id}: ~7KB, без редиректа, с
# полем "title" типа "아이오넷(IO) KRW 마켓...". Та же regex по байтам ловит
# (IO) сразу в первом чанке. Сокращает гэп до TG-фоллбэка на ~2с.
_UPBIT_NOTICE_RE = re.compile(
    r"^https?://(?:www\.)?upbit\.com/service_center/notice\?id=(\d+)\b"
)

# FIX: Bithumb-нотисы (feed.bithumb.com/notice/{id}) — Next.js SPA.
# Тикер в чистом виде лежит в <h2 class="NoticeDetailContent_detail__title__...">
# в формате "코인이름(TICKER) ..." — например:
#   <h2>오키드(OXT) 거래지원 종료</h2>          ← делист
#   <h2>아이오넷(IO) KRW 마켓 디지털 자산 추가</h2> ← листинг
# Просто матчить (TICKER) опасно: парсер найдёт OXT и бот откроет лонг
# на делисте. Поэтому: парсим title целиком и режем, если в нём есть
# корейские негативные ключевые слова.
_BITHUMB_NOTICE_RE = re.compile(rb'<h2\s+class="NoticeDetailContent_detail__title[^"]*"[^>]*>([^<]+)</h2>')

# Корейские стоп-слова в title, при которых сигнал — НЕ листинг:
#   거래지원 종료 — поддержка торгов прекращена (делист)
#   거래지원 중단 — приостановка торгов
#   유의 종목   — warning / monitoring
#   상장폐지   — делистинг (буквально)
#   거래 종료   — окончание торгов
#   입출금     — depo/withdraw (служебное, не торговля)
#   거래지원 일시 — временная приостановка
#   해제       — снятие/отмена
#   연기       — отложен/перенос
_BITHUMB_NEG_KEYWORDS = (
    "종료",       # окончание (покрывает 거래지원 종료, 거래 종료)
    "중단",       # приостановка
    "유의",       # warning
    "상장폐지",  # делистинг
    "해제",       # снятие
    "연기",       # отложено
    "일시",       # временная
    "점검",       # тех. работы
)

# FIX-FALSE-LISTING (Patch #5 risk): title-only-parse теперь идёт ПЕРЕД
# article fetch. Раньше find_listing_pairs(title) срабатывал лишь как
# last-resort после неуспешного article-parse, и delist-нотисы обычно
# отфильтровывались самим парсером. Теперь — нужно явно блочить title'ы
# с delist-маркерами, иначе откроем лонг на умирающую монету.
# Korean уже в _BITHUMB_NEG_KEYWORDS, плюс английские/смешанные:
_TITLE_DELIST_NEG_LOWER = (
    "delist",         # покрывает "delisted", "delisting", "delists"
    "will remove",
    "removal of",
    "removed from",
    "trading will end",
    "trading end",
    "cease trading",
    "discontinue",
    "discontinued",
    "monitoring tag",
    "delistat",       # на всякий, опечатки
)


def _title_is_delist(title: str) -> bool:
    """True если в title есть KR или EN-маркер delisting'а.
    Используется чтобы early-title-parse не открыл лонг на delist-нотисе."""
    if not title:
        return False
    for kw in _BITHUMB_NEG_KEYWORDS:
        if kw in title:
            return True
    lower = title.lower()
    for kw in _TITLE_DELIST_NEG_LOWER:
        if kw in lower:
            return True
    return False


def _is_bithumb_notice(url: str) -> bool:
    return "feed.bithumb.com/notice/" in url


def _rewrite_url_to_api(url: str) -> str:
    m = _UPBIT_NOTICE_RE.match(url)
    if m:
        return f"https://api-manager.upbit.com/api/v1/announcements/{m.group(1)}"
    return url


def _extract_bithumb_tokens(html: bytes) -> list[str]:
    """
    Парсинг Bithumb-нотиса:
      1. Ищем <h2 class="NoticeDetailContent_detail__title__..."> ... </h2>
      2. Если в title есть негативный корейский ключ — возвращаем []
         (фоллбэк find_listing_pairs тоже ничего не найдёт в MASKED title,
          ордер не откроется — это и нужно).
      3. Иначе — экстрактим (TICKER) из title.
    """
    m = _BITHUMB_NOTICE_RE.search(html)
    if not m:
        return []
    title_bytes = m.group(1)
    try:
        title = title_bytes.decode("utf-8", errors="replace").strip()
    except Exception:
        return []
    for kw in _BITHUMB_NEG_KEYWORDS:
        if kw in title:
            log_warn("FAST", f"Bithumb negative keyword '{kw}' в title — skip: {title}")
            return []
    found: list[str] = []
    seen: set[str] = set()
    for tok in TOKEN_REGEX.findall(title_bytes):
        token = tok.decode().upper()
        if token in BANNED:
            continue
        if token not in seen:
            seen.add(token)
            found.append(token)
    return found


async def _parse_tokens_from_article_fast(url: str) -> list[str]:
    """
    Ultra-fast HTML парсер:
      ✓ не качает весь HTML
      ✓ читает поток chunk-by-chunk
      ✓ early stop после нахождения тикера
      ✓ regex on bytes
    """
    started = time.perf_counter()
    original_url = url
    url = _rewrite_url_to_api(url)

    try:
        # FIX: жёсткий 500мс бюджет на весь fetch+parse. Bithumb-нотисы
        # (feed.bithumb.com/notice/{id}) — Next.js-SPA, тикер лежит в
        # <h2 class="NoticeDetailContent_detail__title__...">. Без таймаута
        # парсер 2.6с молча качал HTML и только потом отдавал управление
        # фоллбэку find_listing_pairs (который и так находит тикер из
        # текста WS-сообщения). Видели на BTH-листинге 2026-05-29 06:00:
        # "no tokens found (2616.5ms)". 500мс достаточно для cold-connect
        # к Upbit-API (~250-400мс с TLS handshake), и в 5x быстрее текущего
        # провала на Bithumb-SPA. У warm-сессии Upbit отдаёт за 50-100мс.
        return await asyncio.wait_for(
            _parse_tokens_streaming(url, started, _is_bithumb_notice(original_url)),
            timeout=0.5,
        )
    except asyncio.TimeoutError:
        elapsed = (time.perf_counter() - started) * 1000
        log_warn("FAST", f"timeout after {elapsed:.1f}ms — fallback to text-parser")
        return []
    except Exception as e:
        log_err("FAST", f"parse error: {e}")
        return []


async def _parse_tokens_streaming(url: str, started: float, is_bithumb: bool) -> list[str]:
    session = await get_http_session()

    async with session.get(url) as resp:

        found: list[str] = []
        seen: set[str] = set()
        buffer = b""

        async for chunk in resp.content.iter_chunked(2048):

            buffer += chunk

            # Bithumb-нотис: ищем <h2 class="NoticeDetailContent_detail__title__...">
            # с фильтром негативных корейских ключей (종료, 중단, 유의, etc).
            if is_bithumb:
                found = _extract_bithumb_tokens(buffer)
                if found:
                    elapsed = (time.perf_counter() - started) * 1000
                    log_ok("FAST", f"Bithumb title parsed in {elapsed:.1f}ms | {found}")
                    return found
                # Если нашли <h2> но токенов нет (негативный ключ) — сразу выходим
                if _BITHUMB_NOTICE_RE.search(buffer):
                    elapsed = (time.perf_counter() - started) * 1000
                    log_warn("FAST", f"Bithumb title found but filtered ({elapsed:.1f}ms)")
                    return []
            else:
                # Upbit-API / другие: стандартный regex \(TICKER\)
                matches = TOKEN_REGEX.findall(buffer)
                for m in matches:
                    token = m.decode().upper()
                    if token in BANNED:
                        continue
                    if token not in seen:
                        seen.add(token)
                        found.append(token)

                if found:
                    elapsed = (time.perf_counter() - started) * 1000
                    log_ok("FAST", f"article parsed in {elapsed:.1f}ms | {found}")
                    return found

            if len(buffer) > 15000:
                # FIX: оставляем больший хвост (5000 был мало для длинных
                # HTML-блоков с тикерами в конце; токены до 12 байт включая
                # скобки — 5000 безопасно для разделения, но мало для
                # переменных-длины структур). Главное — сохраняем последние
                # 64 байта точно, чтобы `(SOMETOKEN)` на границе не терялся.
                buffer = buffer[-5000:]

        elapsed = (time.perf_counter() - started) * 1000
        if found:
            log_ok("FAST", f"parsed in {elapsed:.1f}ms | {found}")
        elif len(buffer) < 200 and (b"success" in buffer[:200] or b"error code" in buffer[:200]):
            # FIX 2026-06-05: api-manager ответил success:false/404/CF-throttle —
            # нотис ещё не проиндексирован или API заблокирован. Это не ошибка
            # парсинга. Fallback на text-parser/TG всё равно ловит сигнал.
            log_info("FAST", f"notice not indexed yet ({elapsed:.1f}ms) — fallback to text-parser")
        else:
            log_warn("FAST", f"no tokens found ({elapsed:.1f}ms)")
        return found


# ── External signal sink ─────────────────────────────────────────
# Если задан — _handle делегирует сигнал ему ВМЕСТО прямого _trade.
# Сигнатура: (tickers: list[str], source: str, t_signal: float) -> None.
# Используется parser_listing.run_coinlisting(..., signal_callback=...) для
# того, чтобы CoinListing-сигналы попадали в общий L1+L2 дедуп процесса
# (announcement → global fired). Cooldown _in_cooldown/_set_cooldown
# остаётся локальным фильтром этого модуля.
_signal_callback = None  # type: ignore[var-annotated]


def set_signal_callback(cb) -> None:
    """Регистрирует внешний обработчик сигналов. См. _signal_callback выше."""
    global _signal_callback
    _signal_callback = cb


def _emit_signal(tickers: list[str], source: str, t_signal: float) -> None:
    """Безопасный диспатч во внешний callback или в локальный _trade."""
    if _signal_callback is not None:
        try:
            _signal_callback(tickers, source, t_signal)
            return
        except Exception as e:  # noqa: BLE001
            log_err("CL", f"external callback failed ({e!r}) — fallback to local _trade")
    for t in tickers:
        threading.Thread(
            target=_trade,
            args=(t, source, "", t_signal),
            daemon=True,
        ).start()


# ── Trading ──────────────────────────────────────────────────────
def _trade(
    ticker: str,
    source: str,
    title: str,
    t_signal: float | None = None,
) -> None:

    if _in_cooldown(ticker):
        log_warn("CL", f"{ticker} cooldown")
        return

    _set_cooldown(ticker)

    if t_signal is None:
        t_signal = time.perf_counter()

    usdt_amount = calculate_margin_for_listing()

    try:
        amount, entry_price = market_open_long(ticker, usdt_amount)
    except Exception as e:
        log_err("TRADE", f"{ticker} market_open_long: {e}")
        return

    open_ms = (time.perf_counter() - t_signal) * 1000

    if not amount or not entry_price:
        log_err("TRADE", f"{ticker} failed ({open_ms:.0f}ms)")
        return

    log_ok(
        "TRADE",
        f"{ticker} OPENED | "
        f"entry={entry_price} "
        f"qty={amount} "
        f"time={open_ms:.0f}ms"
    )

    threading.Thread(
        target=set_tp_sl_long,
        args=(ticker, entry_price, amount),
        daemon=True,
    ).start()

    tg_log(
        f"🚀 {ticker}\n"
        f"Source: {source}\n"
        f"Time: {open_ms:.0f}ms"
    )


# ── Message handler ──────────────────────────────────────────────
async def _handle(msg: dict) -> None:

    msg_type = msg.get("type")

    if msg_type == "connection":
        log_ok("CL", "Connected")
        return

    if msg_type == "pong":
        return

    # FIX: если поле явно `null`, ".upper()" падал на NoneType.
    # Универсально: (msg.get(...) or "") как защита от None.
    source = (msg.get("source") or "").upper()

    if source not in TRADE_SOURCES:
        return

    title = msg.get("title") or ""
    url   = msg.get("url") or ""
    coins = msg.get("coins") or []

    t_signal = time.perf_counter()

    # ── REAL TICKERS ─────────────────────────────
    # FIX: isinstance-guard — в coins бывают не только строки (например, dict
    # с метаданными), и .upper() ронял весь handler.
    real = [
        c.upper()
        for c in coins
        if isinstance(c, str) and c and "█" not in c
    ]

    if real:
        log_ok("CL", f"REAL {real}")
        _emit_signal(real, f"COINLISTING-{source}", t_signal)
        return

    # ── EARLY TITLE PARSE (Patch #5) ────────────
    # FIX-LATENCY: раньше title-парсер шёл ПОСЛЕ article fetch (0-500мс
    # блокировка). На сообщениях, где coins замаскированы (█), но title
    # содержит $TICKER или known_coin — это даёт мгновенный сигнал.
    # Цена: ~1мкс на регекс. Risk false-positive: find_listing_pairs
    # требует либо явный "$TICKER listed on X" формат, либо матч на
    # known_coins Bybit — не сработает на голом "Bithumb Listing Notice".
    #
    # FIX-FALSE-LISTING: skip early-title-parse если в title есть delist-
    # маркер. Без этого, например title "거래지원 종료 (XYZ)" → match
    # на $XYZ → лонг на умирающую монету.
    if title and not _title_is_delist(title):
        title_tickers = find_listing_pairs(title)
        if title_tickers:
            log_ok("CL", f"TITLE-EARLY {title_tickers} (article fetch пропущен)")
            _emit_signal(title_tickers, f"COINLISTING-{source}", t_signal)
            # Дальше article-parse не нужен — title уже дал ответ, дубли
            # отсеются в L1/L2 на стороне parser_listing.
            return

    # ── MASKED → ARTICLE PARSE ──────────────────
    if not url:
        log_warn("CL", "MASKED но url пустой — fallback ничего не дал")
        return

    log_ok("CL", f"MASKED → article parse: {url}")
    parsed = await _parse_tokens_from_article_fast(url)

    if parsed:
        log_ok("CL", f"ARTICLE TOKENS {parsed}")
        _emit_signal(parsed, f"COINLISTING-{source}", t_signal)
        return

    # ── LAST-RESORT: повтор title-парсера с возможно обновлённым known_coins
    # Дёшево, бесполезно почти всегда (мы уже пробовали выше), но иногда
    # gate_known_coins дозагрузился между этими двумя точками и теперь
    # match сработал. Оставляем как cheap safety net.
    # FIX-FALSE-LISTING: тот же delist-guard, что в early-path.
    if title and not _title_is_delist(title):
        tickers = find_listing_pairs(title)
        if tickers:
            log_warn("CL", f"FALLBACK-LATE {tickers}")
            _emit_signal(tickers, f"COINLISTING-{source}", t_signal)


# ── Websocket ────────────────────────────────────────────────────
# FIX: backoff параметры — раньше был плоский sleep(1), при недоступности
# сервера долбили 1 RPS бесконечно. Теперь экспоненциальный backoff.
_WS_BACKOFF_MIN = 1.0
_WS_BACKOFF_MAX = 30.0


async def _listen(url: str, label: str) -> None:
    # FIX: ранний выход если API_KEY не задан — иначе URL получает
    # `?key=None` и мы дёргаем сервер бесконечно с заведомо невалидной auth.
    if not COINLISTING_API_KEY:
        log_warn("WS", f"{label} отключён: COINLISTING_API_KEY не задан")
        return

    delay = _WS_BACKOFF_MIN
    while True:

        try:
            async with websockets.connect(
                url,
                ping_interval=10,
                ping_timeout=5,
                max_queue=None,
            ) as ws:

                log_ok("WS", f"{label} connected")
                delay = _WS_BACKOFF_MIN  # сбрасываем backoff после успешного коннекта

                async for raw in ws:

                    try:
                        msg = _json_loads(raw)  # FIX-batch-1: orjson
                    except Exception:
                        continue

                    # FIX: храним reference на task чтобы GC не убил.
                    # _retire_task ретривит исключение — иначе при падении
                    # _handle (например gaierror в article-parse при DNS flap'е)
                    # asyncio печатает "Future exception was never retrieved".
                    task = asyncio.create_task(_handle(msg))
                    _pending_tasks.add(task)
                    task.add_done_callback(_retire_task)

        except ConnectionClosedError as e:
            log_warn("WS", f"{label} disconnected {e.code} — reconnect через {delay:.0f}с")

        except Exception as e:
            log_err("WS", f"{label} error: {e} — reconnect через {delay:.0f}с")

        await asyncio.sleep(delay)
        delay = min(delay * 2, _WS_BACKOFF_MAX)


# ── Connection pre-warming ───────────────────────────────────────
# FIX: на cold-connect к Upbit-API уходит 250-400мс на TLS handshake
# (см. эксперимент в _parse_tokens_from_article_fast). Если между
# листингами проходит >60-90с — keep-alive дропается, и СЛЕДУЮЩИЙ
# MASKED-сигнал платит за рукопожатие заново. Удерживаем TLS-сессию
# горячей: раз в 60с дёргаем HEAD к обоим хостам через ту же
# aiohttp.ClientSession (важно: connection pool общий с боевым
# парсером, только так warm-up имеет смысл).
_PREWARM_HOSTS = (
    # Эти хосты использует CL article-parser через aiohttp ClientSession.
    "https://api-manager.upbit.com/",
    "https://feed.bithumb.com/",
    # FIX-LATENCY-REVERTED: api.bithumb.com и api.upbit.com убраны —
    # notice-поллеры в parser_listing.py используют `requests.Session`
    # из _LISTING_PROXY_POOL, это ОТДЕЛЬНЫЙ TLS-пул от aiohttp. Прогрев
    # тут keep-alive aiohttp-сессии им не помог бы. Поллеры держат свой
    # keep-alive через poll cadence 40-60мс — handshake платится только
    # один раз на старте.
)
_PREWARM_INTERVAL = 60.0


async def _prewarm_loop() -> None:
    # Маленькая задержка чтобы не конкурировать с WS-handshake на старте.
    await asyncio.sleep(2.0)
    while True:
        try:
            session = await get_http_session()
            for host in _PREWARM_HOSTS:
                try:
                    # allow_redirects=False: 301/302 в порядке, нам нужен
                    # только живой TLS-tunnel, тело не интересует.
                    # timeout 2с с запасом, чтобы случайный медленный CDN
                    # не съел весь интервал.
                    async with session.head(
                        host,
                        allow_redirects=False,
                        timeout=aiohttp.ClientTimeout(total=2),
                    ) as resp:
                        # Любой ответ (200/301/403/...) уже значит, что
                        # TLS+TCP установлены и сидят в пуле keep-alive.
                        resp.release()
                except Exception:
                    # Молча игнорим — прогрев best-effort. Логировать
                    # нет смысла, иначе при нестабильной сети флудим лог.
                    pass
        except Exception:
            pass
        await asyncio.sleep(_PREWARM_INTERVAL)


# ── Run ──────────────────────────────────────────────────────────
async def _run() -> None:

    await asyncio.gather(
        _listen(URL_SEOUL, "SEOUL"),
        _listen(URL_TOKYO, "TOKYO"),
        _prewarm_loop(),
    )


def run_coinlisting() -> None:
    asyncio.run(_run())


# ── Main ─────────────────────────────────────────────────────────
if __name__ == "__main__":

    threading.Thread(
        target=price_updater,
        daemon=True,
    ).start()

    threading.Thread(
        target=gate_price_updater,
        daemon=True,
    ).start()

    warmup_bybit_connection()
    warmup_gate_connection()

    preload_lot_steps()
    gate_preload_lot_steps()

    log_ok("BOOT", "started")

    run_coinlisting()
