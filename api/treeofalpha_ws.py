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
import threading
import time
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


# Keywords для классификации сообщений.
# FIX: убрал общий "remove" — он матчился на "remove your funds", "remove API key"
# и т.п. шум. Оставлены только конкретные фразы делистинга.
DELIST_KEYWORDS = [
    "will delist", "delisting notice", "binance will delist",
    "will be delisted", "delisting of",
    "delisted from upbit", "upbit will delist",
    "delisted from bithumb", "bithumb will delist",
    "remove from spot", "removed from spot",
    "removal of",
]

LISTING_KEYWORDS = [
    "will list", "listing notice", "binance will list",
    "listed on upbit", "upbit will list",
    "listed on bithumb", "bithumb will list",
    "new market", "new listing", "will add to spot",
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
]


def _classify(text: str) -> str | None:
    tl = text.lower()
    # Делистинг: должны быть delist-фразы И отсутствовать negative-маркеры.
    if any(kw in tl for kw in DELIST_KEYWORDS):
        if any(neg in tl for neg in DELIST_NEG):
            return None
        return "delist"
    # Листинг: должны быть list-фразы И отсутствовать delist/postponed/airdrop.
    if any(kw in tl for kw in LISTING_KEYWORDS):
        if any(neg in tl for neg in LISTING_NEG):
            return None
        return "listing"
    return None


async def _listener(
    delist_callback: Callable[[str, float], None] | None,
    listing_callback: Callable[[str, float], None] | None,
) -> None:
    """
    Основной цикл WS-листенера. Reconnect с exp backoff.
    Колбэки вызываются как (text, t_start). Они должны быть thread-safe
    и БЫСТРЫЕ (не блокировать loop) — в идеале запускают threading.Thread.
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

                    # Передаём в callback в отдельном thread — НЕ блокируем
                    # WS loop никакими сетевыми запросами.
                    if kind == "delist" and delist_callback:
                        threading.Thread(
                            target=delist_callback,
                            args=(full_text, t_start),
                            daemon=True,
                            name="toa-delist-cb",
                        ).start()
                    elif kind == "listing" and listing_callback:
                        threading.Thread(
                            target=listing_callback,
                            args=(full_text, t_start),
                            daemon=True,
                            name="toa-listing-cb",
                        ).start()

        except (ConnectionClosedError, OSError, asyncio.TimeoutError) as e:
            print(f"[TOA-WS] разрыв: {e} — переподключение через {delay:.0f}с", flush=True)
        except Exception as e:
            print(f"[TOA-WS] неожиданная ошибка: {e!r}", flush=True)

        await asyncio.sleep(delay)
        delay = min(delay * 2, _RECONNECT_DELAY_MAX)


def run_tree_of_alpha_listener(
    delist_callback: Callable[[str, float], None] | None = None,
    listing_callback: Callable[[str, float], None] | None = None,
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
    try:
        loop.run_until_complete(_listener(delist_callback, listing_callback))
    finally:
        loop.close()