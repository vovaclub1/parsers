from __future__ import annotations

# ── parser_listing.py ─────────────────────────────────────────────
# Три параллельных источника сигналов:
#   1. Telegram (Telethon) — канал coin_listing, личный аккаунт
#   2. Upbit + Bithumb REST API — polling каждые 300мс (новые тикеры)
#   3. CoinListing WS — wss://*.coinlisting.pro
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

from tg.tg_logger import tg_log
from api.coinlisting_ws import run_coinlisting
from config.config import (
    TG_API_ID, TG_API_HASH, SESSION_DIR,
    EXTRA_LISTING_CHANNELS, parse_channels,    # FIX-batch-3: multi-channel
    TREE_OF_ALPHA_WS_ENABLED,                   # FIX-batch-4: TOA WS
    BYBIT_WS_TRADE_ENABLED,                     # FIX-batch-5: Bybit WS Trade
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
    "делист", "делисты",
]

UPBIT_MARKETS_URL   = "https://api.upbit.com/v1/market/all"
BITHUMB_ASSETS_URL  = "https://api.bithumb.com/public/assetsstatus/all"
POLL_INTERVAL       = 0.3

WATCHDOG_TIMEOUT    = 60   # секунд

# Защита от дублей
_fired_lock    = threading.Lock()
_fired_coins:  set[str] = set()
_FIRED_TTL     = 60

# ── Watchdog: время последнего успешного запроса ───────────────────
_upbit_last_ts   = time.monotonic()
_bithumb_last_ts = time.monotonic()
_ts_lock         = threading.Lock()

# FIX: отслеживаем активные потоки поллеров, чтобы watchdog не плодил дубли.
_upbit_thread:   threading.Thread | None = None
_bithumb_thread: threading.Thread | None = None
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


# ── дедупликация сигналов ─────────────────────────────────────────

def _try_claim(coin: str) -> bool:
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


# ── воркер (открытие лонга) ───────────────────────────────────────

def worker(coin: str, margin: float, t_start: float,
           source: str, retries: int = 3) -> None:
    for attempt in range(1, retries + 1):
        try:
            log_info("WORKER", f"[{source}] Старт → {coin} | margin={margin} USDT | попытка {attempt}/{retries}")
            amount, entry_price = market_open_long(coin, margin)
            if not amount:
                log_warn("WORKER", f"{coin}: нет цены, повтор через 0.1с...")
                time.sleep(0.1)
                continue

            open_ms = (time.perf_counter() - t_start) * 1000
            log_ok("OPEN", f"[{source}] {coin} | ордер открыт за {BOLD}{open_ms:.0f}мс{RESET}{GREEN}")

            threading.Thread(
                target=set_tp_sl_long,
                args=(coin, entry_price, amount),
                daemon=True,
            ).start()

            elapsed_ms = (time.perf_counter() - t_start) * 1000
            log_ok("LONG", (
                f"[{source}] {coin} | entry={entry_price} | amount={amount:.4f} | "
                f"время от сигнала до ордера: {BOLD}{elapsed_ms:.0f}мс{RESET}{GREEN}"
            ))
            tg_log(f"🟢 <b>LISTING LONG</b> {coin}\nEntry: {entry_price}\nAmount: {amount:.4f}\nВремя: {elapsed_ms:.0f}мс")
            return
        except Exception as e:
            log_err("WORKER", f"{coin}: попытка {attempt}/{retries} упала → {e}")
            if attempt < retries:
                time.sleep(0.1)

    log_err("WORKER", f"{coin}: все {retries} попытки провалились")
    tg_log(f"⚠️Listing {coin}: все попытки провалились")


# ── FIX-batch-4: callback для Tree of Alpha WS ───────────────────

def _on_toa_listing(full_text: str, t_start: float) -> None:
    """Callback из treeofalpha_ws — листинг."""
    pairs = find_listing_pairs(full_text)
    if not pairs:
        log_warn("TOA-LIST", f"тикеры не найдены: {full_text[:120]}")
        return
    log_ok("TOA-LIST", f"Листинг-сигнал из TOA WS: {pairs}")
    # NB: process_signal в listing-парсере вычисляет свой t_start; передавать
    # извне не нужно (различие в 1-3мс некритично).
    process_signal(pairs, "TOA-WS")


# ── общая обработка сигнала ───────────────────────────────────────

def process_signal(pairs: list[str], source: str) -> None:
    t_start = time.perf_counter()

    new_pairs = [c for c in pairs if _try_claim(c)]
    if not new_pairs:
        log_warn("SIGNAL", f"[{source}] все монеты уже в работе: {pairs}")
        return

    margin = calculate_margin_for_listing()

    print(f"\n{BOLD}{'═' * 60}{RESET}")
    log_ok("LISTING", f"[{source}] Новый листинг!")
    log_info("LISTING", f"Монеты : {new_pairs}")
    log_info("TRADE",   f"Маржа={margin} USDT | открываем {len(new_pairs)} лонг(ов)...")
    print(f"{BOLD}{'─' * 60}{RESET}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        for coin in new_pairs:
            executor.submit(worker, coin, margin, t_start, source)

    print(f"{BOLD}{'═' * 60}{RESET}\n")


# ══════════════════════════════════════════════════════════════════
# ИСТОЧНИК 1: Telegram
# ══════════════════════════════════════════════════════════════════

def run_telegram_listener() -> None:
    """
    TG listener на Telethon (основной + EXTRA_LISTING_CHANNELS).
    FIX-batch-3: multi-channel first-wins. Дедуп через _fired_coins (TTL 60с).
    """
    session_path = str(Path(SESSION_DIR) / "listing_session")
    client = TelegramClient(
        session_path,
        api_id=int(TG_API_ID) if TG_API_ID else 0,
        api_hash=TG_API_HASH or "",
        auto_reconnect=True,
        retry_delay=5,
        connection_retries=None,
        request_retries=5,
    )

    # FIX-batch-3: основной + extra. TG_CHANNEL — это int (-100...).
    channels: list[str | int] = []
    try:
        channels.append(int(TG_CHANNEL))
    except (TypeError, ValueError):
        channels.append(TG_CHANNEL)
    for c in parse_channels(EXTRA_LISTING_CHANNELS):
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

        # FIX-batch-6: расширенный фильтр под форматы каналов пользователя
        # (BWEnews, binance_announcements, coin_listing, и т.д.).
        tl = text.lower()
        if not any(p in tl for p in TG_LISTING_PHRASES):
            return
        if any(neg in tl for neg in TG_LISTING_NEG):
            return

        try:
            chat    = await event.get_chat()
            chat_id = getattr(chat, "id", 0)
            uname   = getattr(chat, "username", "") or ""
        except Exception:
            chat_id, uname = 0, ""

        log_ok("TG", f"Листинг-сигнал! [{chat_id} @{uname}]: {text[:120]}")

        pairs = find_listing_pairs(text)
        if not pairs:
            log_warn("TG", f"Тикер не найден (фильтр пропустил): {text[:80]}")
            return

        threading.Thread(
            target=process_signal,
            args=(pairs, f"TG:@{uname}" if uname else "TG"),
            daemon=True,
        ).start()

    async def _run():
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
            return {
                m["market"].split("-")[1]
                for m in _json_loads(resp.content)
                if m["market"].startswith("KRW-")
            }
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
        log_ok("UPBIT", f"Загружено {len(known)} тикеров, жду новые...")
        with _ts_lock:
            _upbit_last_ts = time.monotonic()
    except Exception as e:
        log_err("UPBIT", f"Ошибка инициализации: {e}")
        known = set()

    ever_seen: set[str] = set(known)

    while True:
        try:
            time.sleep(POLL_INTERVAL)
            current = _load_upbit_tickers(session)

            with _ts_lock:
                _upbit_last_ts = time.monotonic()

            new_tickers = current - ever_seen

            if new_tickers:
                log_ok("UPBIT", f"Новые тикеры: {new_tickers}")
                ever_seen |= new_tickers
                process_signal(list(new_tickers), "UPBIT")

        except Exception as e:
            log_err("UPBIT", f"Ошибка поллера: {e}")
            time.sleep(3)


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
            return {
                ticker
                for ticker, info in data.items()
                if isinstance(info, dict)
                and info.get("withdrawal_status") == 1
                and info.get("deposit_status") == 1
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
        log_ok("BITHUMB", f"Загружено {len(known)} тикеров, жду новые...")
        with _ts_lock:
            _bithumb_last_ts = time.monotonic()
    except Exception as e:
        log_err("BITHUMB", f"Ошибка инициализации: {e}")
        known = set()

    ever_seen: set[str] = set(known)

    while True:
        try:
            time.sleep(POLL_INTERVAL)
            current = _load_bithumb_tickers(session)

            with _ts_lock:
                _bithumb_last_ts = time.monotonic()

            new_tickers = current - ever_seen

            if len(new_tickers) > 3:
                log_warn("BITHUMB", f"Подозрительно много новых тикеров ({len(new_tickers)}), пропускаем")
                ever_seen |= current
                continue

            if new_tickers:
                log_ok("BITHUMB", f"Новые тикеры: {new_tickers}")
                ever_seen |= new_tickers
                process_signal(list(new_tickers), "BITHUMB")

        except Exception as e:
            log_err("BITHUMB", f"Ошибка поллера: {e}")
            time.sleep(3)


# ══════════════════════════════════════════════════════════════════
# WATCHDOG — перезапускает зависшие поллеры
# ══════════════════════════════════════════════════════════════════

def _watchdog() -> None:
    """
    Каждые 30 секунд проверяет что Upbit и Bithumb поллеры живы.
    FIX: не плодит дубли — если предыдущий поток ещё жив, не запускает новый.
    """
    global _upbit_last_ts, _bithumb_last_ts, _upbit_thread, _bithumb_thread

    time.sleep(30)

    while True:
        time.sleep(30)
        now = time.monotonic()

        with _ts_lock:
            upbit_age   = now - _upbit_last_ts
            bithumb_age = now - _bithumb_last_ts

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


# ══════════════════════════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # FIX-batch-1: uvloop — asyncio event loop в 2-4x быстрее. Drop-in.
    try:
        import uvloop  # type: ignore[import-not-found]
        uvloop.install()
        print("[BOOT] uvloop активирован")
    except ImportError:
        print("[BOOT] uvloop не установлен, использую стандартный asyncio")

    threading.Thread(target=price_updater,      daemon=True).start()
    threading.Thread(target=gate_price_updater, daemon=True).start()
    log_ok("CACHE", "price_updater (Bybit + Gate.io) запущен в фоне")
    warmup_bybit_connection()
    warmup_gate_connection()
    preload_lot_steps()
    gate_preload_lot_steps()

    # FIX-batch-5: Bybit V5 WS Trade — persistent connection для ордеров.
    if BYBIT_WS_TRADE_ENABLED and BYBIT_API_KEY and BYBIT_SECRET_KEY:
        try:
            from api.bybit_ws_trade import init as bybit_ws_init
            inst = bybit_ws_init(BYBIT_API_KEY, BYBIT_SECRET_KEY)
            if inst.is_ready(wait_sec=3.0):
                log_ok("PARSER", "Bybit WS Trade готов ✓ (−30...−80мс на ордер)")
            else:
                log_warn("PARSER", "Bybit WS Trade не успел подключиться за 3с — fallback на REST")
        except Exception as e:
            log_err("PARSER", f"Bybit WS Trade init упал: {e} — будет REST")
    else:
        log_info("PARSER", "Bybit WS Trade отключён — используем REST")

    log_ok("PARSER", "Ждём 5с пока price_cache наполнится...")
    time.sleep(5)

    # FIX: сохраняем ссылки на потоки для watchdog
    _upbit_thread   = threading.Thread(target=run_upbit_poller,   daemon=True, name="upbit-poller")
    _bithumb_thread = threading.Thread(target=run_bithumb_poller, daemon=True, name="bithumb-poller")
    _upbit_thread.start()
    _bithumb_thread.start()
    threading.Thread(target=run_coinlisting, daemon=True, name="coinlisting-ws").start()
    log_ok("PARSER", f"Upbit + Bithumb поллеры + CoinListing WS запущены (интервал {POLL_INTERVAL}с)")

    threading.Thread(target=_watchdog, daemon=True, name="watchdog").start()
    log_ok("PARSER", f"Watchdog запущен (таймаут {WATCHDOG_TIMEOUT}с)")

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
