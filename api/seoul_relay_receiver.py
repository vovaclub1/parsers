from __future__ import annotations

# ── seoul_relay_receiver.py ───────────────────────────────────────
# Клиент Seoul edge-ноды (см. seoul_relay/seoul_relay.py).
# Подключается к WS из main-парсера (parser_listing.py) и при каждом
# сигнале вызывает listing_callback(tickers, source, t_start) — точно
# та же сигнатура, что у TOA-WS.
#
# Источник сигнала помечается как SEOUL-RELAY-BITHUMB / SEOUL-RELAY-UPBIT,
# биржа в теге → _resolve_exchange извлечёт → per-exchange L2 dedup (FIX
# 2026-06-02: раньше шло в global). Source-first stats покажут, реально ли
# Seoul выигрывает у других источников и насколько.
#
# Запускается из parser_listing.py main:
#     threading.Thread(
#         target=run_seoul_relay_listener,
#         kwargs={"url": SEOUL_RELAY_URL, "listing_callback": _on_seoul_listing},
#         daemon=True, name="seoul-relay-rx",
#     ).start()
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

# FIX (review M24): bounded pool вместо thread-per-message. На бурсте
# сигналов thread-per-message плодил неограниченно потоки (resource leak).
_cb_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="seoul-cb")

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


_RECONNECT_DELAY_MIN = 1.0
_RECONNECT_DELAY_MAX = 30.0
_PING_INTERVAL       = 20.0
_PING_TIMEOUT        = 10.0


def _log(tag: str, msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}][SEOUL-RELAY-{tag}] {msg}", flush=True)


async def _listener(
    url: str,
    listing_callback: Callable[[list[str], str, float], None] | None,
    on_disconnect: Callable[[], None] | None = None,
) -> None:
    """
    Главный loop: коннект к Seoul WS, чтение сообщений, диспатч в callback.
    Reconnect с экспоненциальным backoff. URL должен включать `?key=...`
    с тем же SECRET, что на Seoul-ноде.

    listing_callback: thread-safe, не async. Вызывается синхронно в этом
    loop'е, но БЫСТРО (микросекунды) — он сам должен спавнить тяжёлую
    работу. process_signal в parser_listing.py для single-coin путей
    делает inline-execute worker'а, что приемлемо (5-10мс блокировка
    WS loop'а — для relay'а с редкими сообщениями ОК).
    """
    if not url:
        _log("RX", "url пуст — listener не стартует")
        return

    delay = _RECONNECT_DELAY_MIN
    # Не печатаем URL целиком в лог — там SECRET в query.
    safe_url = url.split("?", 1)[0]

    while True:
        try:
            _log("RX", f"Подключаюсь к {safe_url}")
            async with websockets.connect(
                url,
                ping_interval=_PING_INTERVAL,
                ping_timeout=_PING_TIMEOUT,
                close_timeout=5,
                max_size=2**16,
            ) as ws:
                _log("RX", "Подключён ✓")
                delay = _RECONNECT_DELAY_MIN

                async for raw in ws:
                    # FIX: t_start — момент прихода фрейма НА SG-сервер.
                    # Latency между t_pub_ms (когда Seoul задетектил) и
                    # t_start ≈ KR→SG one-way (~85-120мс). Эта latency
                    # уже неизбежна, в метриках OPEN видна как разница
                    # source-detect → open. process_signal будет
                    # использовать t_start как полный path-time.
                    t_start = time.perf_counter()
                    try:
                        msg = _json_loads(raw)
                    except Exception:
                        continue

                    msg_type = msg.get("type")
                    if msg_type == "hello":
                        # server-side приветствие, игнорируем
                        continue
                    if msg_type != "listing":
                        continue

                    # FIX (review M25): санитизируем source из сети — он идёт
                    # как метка источника в stats/логи. Ограничиваем алфавит и длину.
                    _raw_src = msg.get("source") or "SEOUL-RELAY-?"
                    source = re.sub(r"[^A-Za-z0-9_\-]", "_", str(_raw_src))[:24]
                    coins  = msg.get("coins")  or []
                    title  = msg.get("title")  or ""
                    fetch_ms = msg.get("fetch_ms")

                    if not isinstance(coins, list):
                        _log("RX", f"WARN: coins не list ({type(coins).__name__}) — skip")
                        continue
                    coins = [c for c in coins if isinstance(c, str) and c]
                    if not coins:
                        continue

                    _log("RX", f"{source} {coins} | fetch={fetch_ms}мс | {title[:60]}")

                    if listing_callback is None:
                        continue

                    # Колбэк в bounded pool — не блокируем WS loop, не плодим
                    # потоки на бурсте (FIX M24). source уже санитизирован выше.
                    _cb_executor.submit(listing_callback, coins, source, t_start)

        except (ConnectionClosedError, OSError, asyncio.TimeoutError) as e:
            _log("RX", f"разрыв: {e!r} — переподключение через {delay:.0f}с")
            if on_disconnect is not None:
                try:
                    on_disconnect()
                except Exception:  # noqa: BLE001
                    pass
        except Exception as e:  # noqa: BLE001
            _log("RX", f"неожиданная ошибка: {e!r}")

        await asyncio.sleep(delay)
        delay = min(delay * 2, _RECONNECT_DELAY_MAX)


def run_seoul_relay_listener(
    url: str,
    listing_callback: Callable[[list[str], str, float], None] | None = None,
    on_disconnect: Callable[[], None] | None = None,
) -> None:
    """
    Точка входа для threading.Thread. Создаёт собственный asyncio loop
    (с uvloop если установлен), запускает _listener в бесконечном цикле
    с автоперезапуском внешнего уровня (на случай если _listener сам
    бросит исключение через все его try/except).
    """
    try:
        import uvloop  # type: ignore[import-not-found]
        loop = uvloop.new_event_loop()
    except ImportError:
        loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        consecutive_failures = 0
        _last_crash_at = 0.0
        while consecutive_failures < 5:
            try:
                loop.run_until_complete(_listener(url, listing_callback, on_disconnect))
                break  # _listener вернулся без exception
            except Exception as e:  # noqa: BLE001
                # FIX (review): сброс счётчика если краши НЕ подряд (>60с).
                now = time.monotonic()
                if now - _last_crash_at > 60.0:
                    consecutive_failures = 0
                _last_crash_at = now
                consecutive_failures += 1
                _log(
                    "RX",
                    f"listener crashed ({consecutive_failures}/5): {e!r} — restart через 5с",
                )
                try:
                    loop.run_until_complete(asyncio.sleep(5))
                except Exception:
                    time.sleep(5)
        else:
            _log("RX", "5 крашей за <60с — listener остановлен")
    finally:
        loop.close()
