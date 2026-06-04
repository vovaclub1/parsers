from __future__ import annotations

# ── treeofalpha_ws.py ─────────────────────────────────────────────
# FIX-batch-4: подписчик на free public WebSocket Tree of Alpha:
#   wss://news.treeofalpha.com/ws
#
# Tree of Alpha — аггрегатор anouncements (Binance/Bybit/Upbit/Bithumb/
# Twitter/Telegram). Бесплатный tier даёт live WS с задержкой 0-500мс
# vs paid tier (~0мс). Часто опережает TG-мирроры на 50-500мс.
#
# Сообщение приходит в формате:
#   {"title": "...", "body": "...", "source": "Binance", "time": 1700000000000}
#
# Мы извлекаем тикеры и пускаем их в callback'и delist/listing.
# Если callback бросает — мы это логируем, но не рвём WS.
#
# Запускается как отдельный thread с собственным asyncio loop (как
# coinlisting_ws.py — чтобы не блокировать main loop парсера).
# ─────────────────────────────────────────────────────────────────

import asyncio
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

import websockets
from websockets.exceptions import ConnectionClosedError

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


TOA_WS_URL = "wss://news.treeofalpha.com/ws"

# Reconnect parameters
_RECONNECT_DELAY_MIN = 1.0
_RECONNECT_DELAY_MAX = 30.0
_PING_INTERVAL       = 20.0
_PING_TIMEOUT        = 10.0

# S1: long-lived пул вместо threading.Thread(...).start() на каждое сообщение
# (как уже сделано в parser_delist/parser_listing). Спавн нового OS-потока =
# syscall + GIL ~50-200µs на сообщение; submit в тёплый пул ~5-20µs.
_toa_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="toa-cb")


# Keywords для классификации сообщений.
# FIX: убрал общий "remove" — он матчился на "remove your funds", "remove API key"
# и т.п. шум. Оставлены только конкретные фразы делистинга.
# FIX: убрал "removal of" — слишком широкое (ловило "removal of API key",
# "removal of futures contract" и т.п.). Если нужны такие сигналы, добавляем
# точные фразы типа "removal of trading pair".
DELIST_KEYWORDS = [
    "will delist", "delisting notice", "binance will delist",
    "will be delisted", "delisting of",
    "delisted from upbit", "upbit will delist",
    "delisted from bithumb", "bithumb will delist",
    "remove from spot", "removed from spot",
]

LISTING_KEYWORDS = [
    "will list", "listing notice", "binance will list",
    "listed on upbit", "upbit will list",
    "listed on bithumb", "bithumb will list",
    "new market", "new listing", "will add to spot",
    # FIX: корейские форматы — Bithumb/Upbit пушат на корейском
    # ("[마켓 추가] 빌리언즈(BILL) 원화 마켓 추가"). Английские
    # keywords не матчатся, и сигнал пропускался полностью.
    # Инцидент 2026-05-28: BILL не зашёл через TOA по этой причине.
    "마켓 추가",   # market added
    "원화 마켓",   # KRW market
    "상장",        # listing
    "신규",        # new
]

# FIX: расширенный negative-filter под Binance Earn / Launchpool /
# HODLer Airdrop промо. Без него Earn-промо вроде
# "Binance Earn New Listing Special Offer: Subscribe to GENIUS and
# OPG Locked Products to Enjoy 200% APR for 7 Days" триггерило
# открытие лонгов на названиях Earn-продуктов (инцидент 2026-05-28
# 04:00:03 — GENIUS/OPG/APR). Эти фразы НЕ встречаются в реальных
# листингах, поэтому риск false-negative по легитимному листингу
# минимальный.
_EARN_PROMO_NEG = [
    "earn",
    "locked product", "locked products",
    "flexible product", "flexible products",
    "simple earn",
    "subscribe to", "subscribe and",
    "special offer",
    "launchpool",
    "% apr", "apr for",
    "% apy", "apy for",
    "promotion", "promotional",
    "rewards pool",
    "staking pool",
]

# FIX: negative-filter — те же фразы, что в parser_delist.TG_DELIST_NEG /
# parser_listing.TG_LISTING_NEG. Без них TOA пробивает фильтры
# пользовательских каналов (Binance Alpha removals, postponements,
# HODLer Airdrop и т.п. — это НЕ листинг/делистинг для нашей стратегии).
DELIST_NEG = [
    "alpha will remove",
    "removed from the featuring list",
    "alpha removal",
    "from alpha",
    "delisting postponed",
]
LISTING_NEG = [
    "delist", "delisted", "delisting", "delists",
    "monitoring tag", "extend the monitoring",
    "postponed", "cancelled", "canceled",
    "alpha will remove", "from the featuring list",
    "hodler airdrop",
    # FIX: Earn / Launchpool / promo — раскрываем в общий LISTING_NEG
    # чтобы не дублировать сравнение в _classify.
    *_EARN_PROMO_NEG,
    # FIX 2026-06-02: Pre-IPO / TradFi (Anthropic, OpenAI ... Perpetual
    # Contract Pre-IPO Trading). Те же фильтры что в parser_listing.TG_LISTING_NEG.
    "pre-ipo", "pre ipo",
    "tradfi",
    "perpetual contract pre",
    "multiple usd",
    "multiple usdⓈ",
]


# FIX-perf: компилируем фильтры в regex-альтернацию ОДИН раз из списков выше
# (списки — источник правды). re.search по DFA вместо N×substring-сканов в
# hot-path WS-классификатора (~10-30µs/сообщение).
_DELIST_KW_RE   = re.compile("|".join(re.escape(k) for k in DELIST_KEYWORDS))
_LISTING_KW_RE  = re.compile("|".join(re.escape(k) for k in LISTING_KEYWORDS))
_DELIST_NEG_RE  = re.compile("|".join(re.escape(k) for k in DELIST_NEG))
_LISTING_NEG_RE = re.compile("|".join(re.escape(k) for k in LISTING_NEG))


def _classify(text: str) -> str | None:
    tl = text.lower()
    has_delist = bool(_DELIST_KW_RE.search(tl))
    has_listing = bool(_LISTING_KW_RE.search(tl))

    # FIX: если в одном сообщении есть И delist И listing-фразы (например,
    # "We will delist X and will list Y") — не классифицируем как одно,
    # потому что любой из двух выборов будет неверным. Лучше пропустить
    # сигнал, чем открыть позицию не в ту сторону.
    if has_delist and has_listing:
        return None

    if has_delist:
        if _DELIST_NEG_RE.search(tl):
            return None
        return "delist"
    if has_listing:
        if _LISTING_NEG_RE.search(tl):
            return None
        return "listing"
    return None


async def _listener(
    delist_callback: Callable[[str, float], None] | None,
    listing_callback: Callable[[str, float, str], None] | None,
) -> None:
    """
    Основной цикл WS-листенера. Reconnect с exp backoff.
    Колбэки вызываются как delist_callback(text, t_start) и
    listing_callback(text, t_start, source) — последний получает msg.source
    ("Binance"/"Upbit"/...) для per-exchange L2-дедупа. Оба должны быть
    thread-safe и БЫСТРЫЕ (не блокировать loop) — в идеале запускают
    threading.Thread.
    """
    delay = _RECONNECT_DELAY_MIN

    while True:
        try:
            print(f"[TOA-WS] Подключаюсь к {TOA_WS_URL}", flush=True)
            async with websockets.connect(
                TOA_WS_URL,
                ping_interval=_PING_INTERVAL,
                ping_timeout=_PING_TIMEOUT,
                close_timeout=5,
                max_size=2**20,  # 1MB messages — TOA шлёт мелочь, но c запасом
            ) as ws:
                print("[TOA-WS] Подключён ✓", flush=True)
                delay = _RECONNECT_DELAY_MIN  # сбрасываем backoff

                async for raw in ws:
                    t_start = time.perf_counter()
                    try:
                        msg = _json_loads(raw)
                    except Exception:
                        continue

                    title = msg.get("title", "") or ""
                    body  = msg.get("body",  "") or ""
                    source = msg.get("source", "") or msg.get("type", "") or "?"

                    if not title and not body:
                        continue

                    full_text = f"{title} {body}"
                    kind = _classify(full_text)
                    if not kind:
                        continue

                    short = title[:120].replace("\n", " ")
                    print(f"[TOA-WS] [{kind.upper()}] ({source}) {short}", flush=True)

                    # Передаём в callback через пул — НЕ блокируем WS loop
                    # сетевыми запросами и не платим за спавн потока (S1).
                    if kind == "delist" and delist_callback:
                        _toa_executor.submit(delist_callback, full_text, t_start)
                    elif kind == "listing" and listing_callback:
                        # FIX 2026-06-02: прокидываем msg.source ("Binance"/
                        # "Upbit"/"Bithumb") в callback — для per-exchange L2.
                        _toa_executor.submit(listing_callback, full_text, t_start, source)

        except (ConnectionClosedError, OSError, asyncio.TimeoutError) as e:
            print(f"[TOA-WS] разрыв: {e} — переподключение через {delay:.0f}с", flush=True)
        except Exception as e:
            print(f"[TOA-WS] неожиданная ошибка: {e!r}", flush=True)

        await asyncio.sleep(delay)
        delay = min(delay * 2, _RECONNECT_DELAY_MAX)


def run_tree_of_alpha_listener(
    delist_callback: Callable[[str, float], None] | None = None,
    listing_callback: Callable[[str, float, str], None] | None = None,
) -> None:
    """
    Точка входа для запуска в отдельном thread.
    Создаёт собственный asyncio event loop (с uvloop если установлен).

    Пример:
        threading.Thread(
            target=run_tree_of_alpha_listener,
            kwargs={"delist_callback": on_delist, "listing_callback": on_listing},
            daemon=True, name="toa-ws",
        ).start()
    """
    # FIX-12: создаём loop локально — НЕ мутируем глобальную asyncio policy
    # из background thread (паттерн bybit_ws_trade._run_loop). Главный
    # процесс уже выставил uvloop policy, а мы дополнительно делаем явный
    # new_event_loop() — это безопасно при многократном вызове из разных thread'ов.
    try:
        import uvloop  # type: ignore[import-not-found]
        loop = uvloop.new_event_loop()
    except ImportError:
        loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # FIX: outer-restart — если _listener сам по себе упадёт (баг в callback,
    # неожиданное исключение в websockets) — поток не должен умирать молча.
    # Внутренний `while True` в _listener защищает от разрывов WS, но не от
    # ошибок верхнего уровня. После 5 крашей подряд — сдаёмся.
    try:
        consecutive_failures = 0
        _last_crash_at = 0.0
        while consecutive_failures < 5:
            try:
                loop.run_until_complete(_listener(delist_callback, listing_callback))
                # _listener вернулся без исключения (что нормально только при stop) — выходим.
                break
            except Exception as e:  # noqa: BLE001
                # FIX (review): сбрасываем счётчик если с прошлого краша прошло
                # >60с — значит listener работал нормально, краши НЕ «подряд».
                # Иначе 5 редких крашей за всё время убивали источник навсегда.
                now = time.monotonic()
                if now - _last_crash_at > 60.0:
                    consecutive_failures = 0
                _last_crash_at = now
                consecutive_failures += 1
                print(
                    f"[TOA-WS] listener crashed ({consecutive_failures}/5): {e!r} — restart через 5с",
                    flush=True,
                )
                try:
                    loop.run_until_complete(asyncio.sleep(5))
                except Exception:
                    time.sleep(5)
        else:
            print("[TOA-WS] 5 крашей за <60с — listener остановлен", flush=True)
    finally:
        loop.close()