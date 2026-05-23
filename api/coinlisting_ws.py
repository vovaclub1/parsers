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

# FIX: держим ссылки на async-таски, чтобы GC их не убил
# ("Task was destroyed but it is pending").
_pending_tasks: set[asyncio.Task] = set()


# ── HTTP Session ─────────────────────────────────────────────────
_http_session: aiohttp.ClientSession | None = None


async def get_http_session() -> aiohttp.ClientSession:
    global _http_session

    if _http_session is None or _http_session.closed:
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
TOKEN_REGEX = re.compile(rb'\(([A-Z0-9]{2,10})\)')

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
    with _cooldown_lock:
        until = _cooldown.get(ticker)
        return bool(until and time.time() < until)

def _set_cooldown(ticker: str) -> None:
    with _cooldown_lock:
        _cooldown[ticker] = time.time() + COOLDOWN_SEC


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

    source = msg.get("source", "").upper()

    if source not in TRADE_SOURCES:
        return

    title = msg.get("title", "")
    url   = msg.get("url", "")
    coins = msg.get("coins") or []

    t_signal = time.perf_counter()

    # ── REAL TICKERS ─────────────────────────────
    real = [
        c.upper()
        for c in coins
        if c and "█" not in c
    ]

    if real:
        log_ok("CL", f"REAL {real}")
        for ticker in real:
            threading.Thread(
                target=_trade,
                args=(ticker, source, title, t_signal),
                daemon=True,
            ).start()
        return

    # ── MASKED → ARTICLE PARSE ──────────────────
    if not url:
        log_warn("CL", "MASKED но url пустой — пробуем fallback")
    else:
        log_ok("CL", f"MASKED → article parse: {url}")

    parsed = await _parse_tokens_from_article_fast(url) if url else []

    if parsed:
        log_ok("CL", f"ARTICLE TOKENS {parsed}")
        for ticker in parsed:
            threading.Thread(
                target=_trade,
                args=(ticker, source, title, t_signal),
                daemon=True,
            ).start()
        return

    # ── FALLBACK ────────────────────────────────
    tickers = find_listing_pairs(title)

    if tickers:
        log_warn("CL", f"FALLBACK {tickers}")
        for ticker in tickers:
            threading.Thread(
                target=_trade,
                args=(ticker, source, title, t_signal),
                daemon=True,
            ).start()


# ── Websocket ────────────────────────────────────────────────────
async def _listen(url: str, label: str) -> None:

    while True:

        try:
            async with websockets.connect(
                url,
                ping_interval=10,
                ping_timeout=5,
                max_queue=None,
            ) as ws:

                log_ok("WS", f"{label} connected")

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
            log_warn("WS", f"{label} disconnected {e.code}")

        except Exception as e:
            log_err("WS", f"{label} error: {e}")

        await asyncio.sleep(1)


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
