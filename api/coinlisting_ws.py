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
async def _parse_tokens_from_article_fast(url: str) -> list[str]:
    """
    Ultra-fast HTML парсер:
      ✓ не качает весь HTML
      ✓ читает поток chunk-by-chunk
      ✓ early stop после нахождения тикера
      ✓ regex on bytes
    """
    started = time.perf_counter()

    try:
        session = await get_http_session()

        async with session.get(url) as resp:

            found: list[str] = []
            seen: set[str] = set()
            buffer = b""

            async for chunk in resp.content.iter_chunked(2048):

                buffer += chunk

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
            else:
                log_warn("FAST", f"no tokens found ({elapsed:.1f}ms)")
            return found

    except Exception as e:
        log_err("FAST", f"parse error: {e}")
        return []


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

    # ── MASKED → ARTICLE PARSE ──────────────────
    if not url:
        log_warn("CL", "MASKED но url пустой — пробуем fallback")
    else:
        log_ok("CL", f"MASKED → article parse: {url}")

    parsed = await _parse_tokens_from_article_fast(url) if url else []

    if parsed:
        log_ok("CL", f"ARTICLE TOKENS {parsed}")
        _emit_signal(parsed, f"COINLISTING-{source}", t_signal)
        return

    # ── FALLBACK ────────────────────────────────
    tickers = find_listing_pairs(title)

    if tickers:
        log_warn("CL", f"FALLBACK {tickers}")
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

                    # FIX: храним reference на task чтобы GC не убил
                    task = asyncio.create_task(_handle(msg))
                    _pending_tasks.add(task)
                    task.add_done_callback(_pending_tasks.discard)

        except ConnectionClosedError as e:
            log_warn("WS", f"{label} disconnected {e.code} — reconnect через {delay:.0f}с")

        except Exception as e:
            log_err("WS", f"{label} error: {e} — reconnect через {delay:.0f}с")

        await asyncio.sleep(delay)
        delay = min(delay * 2, _WS_BACKOFF_MAX)


# ── Run ──────────────────────────────────────────────────────────
async def _run() -> None:

    await asyncio.gather(
        _listen(URL_SEOUL, "SEOUL"),
        _listen(URL_TOKYO, "TOKYO"),
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
