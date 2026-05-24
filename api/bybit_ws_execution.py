from __future__ import annotations

# ── bybit_ws_execution.py ─────────────────────────────────────────
# Замер end-to-end latency: от прихода сигнала (perf_counter в caller'е)
# до фактического fill'а на бирже Bybit (execTime в private execution stream).
#
# Зачем нужен отдельный поток / отдельный WS:
#   /v5/trade endpoint, через который мы отправляем ордера, НЕ доставляет
#   execution-сообщения. Для этого нужен private stream:
#     wss://stream.bybit.com/v5/private  →  subscribe ["execution"]
#   Тут приходят фактические сделки с execTime (биржевые часы), orderLinkId,
#   execPrice, execQty — именно то, что нужно для "когда позиция реально
#   открылась на бирже".
#
# Hot-path overhead:
#   В hot caller'е вызывается track(link_id, t_signal_perf):
#     • check флага ENABLED:        ~10ns
#     • module-level dict[k]=v:     ~50-200ns  (атомарно под GIL)
#   Итого: 100-300ns на ордер. Это в ~10_000 раз меньше, чем сам путь сигнал→ws.send (~2мс).
#
#   Никаких syscall'ов в hot-path: wall-clock считается фоновым потоком при
#   приёме execution (offset perf↔wall зафиксирован один раз в момент импорта).
#
# Что логируется при fill'е:
#   [FILL] SYMBOL exchange_fill_delay=Xмс e2e_recv=Yмс price=... qty=... link_id=...
#     exchange_fill_delay — биржевой execTime минус наш signal wall-time.
#       Включает: local_send + network_to_bybit + matching engine.
#       Не включает: network_to_us + recv_parse (для этого e2e_recv).
#     e2e_recv — полный round-trip: signal → send → network → matching
#       → network back → recv. Считается по perf_counter.
#
# Документация:
#   https://bybit-exchange.github.io/docs/v5/websocket/private/execution
# ─────────────────────────────────────────────────────────────────

import hashlib
import hmac
import json
import os
import threading
import time
from typing import Any

try:
    import orjson as _orjson  # type: ignore[import-not-found]
    def _json_loads(b):
        if isinstance(b, str):
            b = b.encode()
        return _orjson.loads(b)
    def _json_dumps(obj) -> str:
        return _orjson.dumps(obj).decode()
except ImportError:
    def _json_loads(b):
        if isinstance(b, (bytes, bytearray)):
            b = b.decode()
        return json.loads(b)
    def _json_dumps(obj) -> str:
        return json.dumps(obj, separators=(",", ":"))


WS_PRIVATE_URL = "wss://stream.bybit.com/v5/private"

_RECONNECT_DELAY_MIN = 1.0
_RECONNECT_DELAY_MAX = 30.0
_AUTH_EXPIRES_SEC    = 60
_READER_RECV_TIMEOUT = 30.0

# Offset для перевода perf_counter → wall_clock (наносек). Фиксируется один
# раз при импорте модуля. На Linux perf_counter == CLOCK_MONOTONIC, slewed
# вместе с REALTIME через NTP, drift ≤1мкс/час. Для измерения сетевой
# latency точность с запасом — error ≪ передаваемых нами 30-100мс RTT.
_PERF_TO_WALL_NS: int = time.time_ns() - int(time.perf_counter() * 1_000_000_000)

# Module-level pending dict. orderLinkId → t_signal_perf.
# Операции dict[str]=float и dict.pop(str) атомарны под GIL — синхронизация
# между hot-caller'ом и background reader'ом без явного lock'а. Это
# критично, потому что иначе hot-path был бы +0.5-2мкс на acquire+release.
_pending: dict[str, float] = {}

# Cap чтобы не утечь память если приёмник встал (рейс-сценарий: WS
# disconnect, ордера всё ещё отправляются REST'ом, никто не разгребает _pending).
_MAX_PENDING = 4096

# TTL: если ордер попал в _pending >30с назад и не получил execution —
# orphan (отвергнут, lost, fill ушёл в другой канал). Чистим фоновым GC.
_PENDING_TTL_SEC = 30.0

# Глобальный toggle через env. Если "0" — track() становится no-op,
# фоновое подключение не поднимается. На случай регрессии в проде.
_ENABLED = os.getenv("BYBIT_EXEC_LATENCY", "1").lower() in ("1", "true", "yes", "on")


def track(order_link_id: str, t_signal_perf: float) -> None:
    """
    HOT PATH. Регистрирует ордер для замера latency сигнал→fill.

    Параметры:
      order_link_id  — тот же, что уйдёт в Bybit как orderLinkId.
      t_signal_perf  — time.perf_counter() в момент прихода сигнала
                       (TG message / WS frame). Caller должен передать
                       реальный t_start, не локальный момент вызова.

    Стоимость: ~100-300ns (1 if + 1 dict-set). Никаких syscall'ов, никаких lock'ов.
    Если модуль отключён — instant return.
    """
    if not _ENABLED:
        return
    # dict-set атомарен под GIL.
    _pending[order_link_id] = t_signal_perf
    # Safety net: если приёмник встал, не растём бесконечно.
    if len(_pending) > _MAX_PENDING:
        try:
            _pending.popitem()
        except KeyError:
            pass


class BybitWsExecution:
    """
    Singleton: 1 background-thread с persistent private WS к Bybit.
    Подписан на execution-стрим, матчит входящие fills по orderLinkId,
    считает latency, логирует. Reconnect с exp-backoff на любую ошибку.
    """
    _instance: "BybitWsExecution | None" = None
    _instance_lock = threading.Lock()

    @classmethod
    def get(cls, api_key: str, api_secret: str) -> "BybitWsExecution":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls(api_key, api_secret)
                cls._instance._start()
            return cls._instance

    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key    = api_key
        self.api_secret = api_secret
        self._stop      = False
        self._thread:    threading.Thread | None = None
        self._gc_thread: threading.Thread | None = None

    def _start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._mgr_loop,
            daemon=True,
            name="bybit-ws-execution",
        )
        self._thread.start()
        self._gc_thread = threading.Thread(
            target=self._gc_loop,
            daemon=True,
            name="bybit-ws-execution-gc",
        )
        self._gc_thread.start()

    def stop(self) -> None:
        self._stop = True

    def _gc_loop(self) -> None:
        """Чистит orphan'ы из _pending (ордер был, fill не пришёл)."""
        while not self._stop:
            time.sleep(10.0)
            try:
                now = time.perf_counter()
                # list() — snapshot, иначе RuntimeError при concurrent mutation.
                stale = [k for k, t in list(_pending.items()) if now - t > _PENDING_TTL_SEC]
                for k in stale:
                    _pending.pop(k, None)
            except Exception:
                pass

    def _mgr_loop(self) -> None:
        try:
            from websockets.sync.client import connect as ws_connect
            from websockets.exceptions import ConnectionClosed
        except ImportError as e:
            print(f"[BYBIT-WS-EXEC] websockets не установлен: {e!r}", flush=True)
            return

        delay = _RECONNECT_DELAY_MIN
        while not self._stop:
            try:
                print(f"[BYBIT-WS-EXEC] connect → {WS_PRIVATE_URL}", flush=True)
                # Pinned websockets>=12,<13 — те же ограничения по
                # ping_interval/timeout, что и в trade-клиенте: не передаём.
                ws = ws_connect(
                    WS_PRIVATE_URL,
                    open_timeout=10,
                    close_timeout=5,
                    max_size=2**20,
                )
            except Exception as e:
                print(f"[BYBIT-WS-EXEC] connect failed: {e!r} — retry {delay:.0f}с", flush=True)
                time.sleep(delay)
                delay = min(delay * 2, _RECONNECT_DELAY_MAX)
                continue

            try:
                # ── Auth ─────────────────────────────────────────
                expires = int((time.time() + _AUTH_EXPIRES_SEC) * 1000)
                sig_msg = f"GET/realtime{expires}"
                signature = hmac.new(
                    self.api_secret.encode(),
                    sig_msg.encode(),
                    hashlib.sha256,
                ).hexdigest()
                ws.send(_json_dumps({
                    "op": "auth",
                    "args": [self.api_key, expires, signature],
                }))
                auth_resp = _json_loads(ws.recv(timeout=5))
                # /v5/private возвращает {"success":true,...}, иногда с
                # retCode. Проверяем оба варианта для совместимости.
                ok = bool(auth_resp.get("success"))
                if not ok and auth_resp.get("retCode") == 0:
                    ok = True
                if not ok:
                    print(f"[BYBIT-WS-EXEC] auth failed: {auth_resp}", flush=True)
                    try:
                        ws.close()
                    except Exception:
                        pass
                    time.sleep(delay)
                    delay = min(delay * 2, _RECONNECT_DELAY_MAX)
                    continue

                # ── Subscribe ────────────────────────────────────
                # "execution" — полный execution-стрим (orderLinkId, execTime,
                # execPrice, execQty). Альтернатива "execution.fast" даёт
                # меньшие payload'ы и (возможно) меньшую latency, но не для
                # всех аккаунтов доступна; "execution" — гарантированно есть.
                ws.send(_json_dumps({"op": "subscribe", "args": ["execution"]}))
                # subscribe ack — может прийти сразу, может после первого
                # heartbeat. Просто читаем raw recv и проверяем.
                sub_resp_raw = ws.recv(timeout=5)
                try:
                    sub_resp = _json_loads(sub_resp_raw)
                except Exception:
                    sub_resp = {}
                sub_ok = sub_resp.get("success", True) if isinstance(sub_resp, dict) else True
                if not sub_ok:
                    print(f"[BYBIT-WS-EXEC] subscribe failed: {sub_resp}", flush=True)
                    try:
                        ws.close()
                    except Exception:
                        pass
                    time.sleep(delay)
                    delay = min(delay * 2, _RECONNECT_DELAY_MAX)
                    continue

                print("[BYBIT-WS-EXEC] subscribed to execution ✓", flush=True)
                delay = _RECONNECT_DELAY_MIN

                # ── Reader-loop ──────────────────────────────────
                while not self._stop:
                    try:
                        raw = ws.recv(timeout=_READER_RECV_TIMEOUT)
                    except TimeoutError:
                        continue
                    except ConnectionClosed:
                        print("[BYBIT-WS-EXEC] connection closed by peer", flush=True)
                        break
                    except Exception as e:
                        print(f"[BYBIT-WS-EXEC] recv error: {e!r}", flush=True)
                        break

                    # КРИТИЧНО: t_recv_perf снимается СРАЗУ после recv,
                    # до парсинга. Это самая близкая к "wire arrival"
                    # точка, что у нас есть в Python.
                    t_recv_perf = time.perf_counter()
                    try:
                        msg = _json_loads(raw)
                    except Exception:
                        continue
                    self._dispatch(msg, t_recv_perf)

            except Exception as e:
                print(f"[BYBIT-WS-EXEC] mgr error: {e!r}", flush=True)
            finally:
                try:
                    ws.close()
                except Exception:
                    pass

            time.sleep(delay)
            delay = min(delay * 2, _RECONNECT_DELAY_MAX)

    def _dispatch(self, msg: dict, t_recv_perf: float) -> None:
        if msg.get("topic") != "execution":
            return
        data = msg.get("data") or []
        for ex in data:
            link_id = ex.get("orderLinkId")
            if not link_id:
                continue
            # pop под GIL атомарен; если двух execution'ов с одним link_id
            # (partial fills), первый забирает t_signal, остальные мимо.
            # Это правильно: мы хотим "когда позиция впервые открылась".
            t_signal_perf = _pending.pop(link_id, None)
            if t_signal_perf is None:
                continue

            try:
                exec_time_ms = int(ex.get("execTime", 0))
            except (TypeError, ValueError):
                exec_time_ms = 0

            symbol     = ex.get("symbol", "?")
            exec_price = ex.get("execPrice", "?")
            exec_qty   = ex.get("execQty", "?")
            side       = ex.get("side", "?")

            # Wall-time момента сигнала в ms (через зафиксированный offset).
            signal_wall_ms = (
                int(t_signal_perf * 1_000_000_000) + _PERF_TO_WALL_NS
            ) // 1_000_000

            # Биржевые часы: T_fill - T_signal. Это и есть "от сигнала до
            # фактического открытия позиции на бирже".
            #   = local_send + net_to_bybit + matching
            exchange_fill_delay_ms = exec_time_ms - signal_wall_ms

            # Наш round-trip по локальным monotonic-часам: signal → fill_known.
            #   = exchange_fill_delay + net_from_bybit + recv_parse
            roundtrip_ms = (t_recv_perf - t_signal_perf) * 1000.0

            print(
                f"[FILL] {symbol} {side} "
                f"exchange_fill_delay={exchange_fill_delay_ms}мс "
                f"e2e_recv={roundtrip_ms:.1f}мс "
                f"price={exec_price} qty={exec_qty} link_id={link_id}",
                flush=True,
            )


# ── удобные обёртки ──────────────────────────────────────────────

_global_instance: BybitWsExecution | None = None
_global_instance_lock = threading.Lock()


def init(api_key: str, api_secret: str) -> BybitWsExecution | None:
    """
    Инициализирует singleton private WS execution-стрима. Idempotent.
    Если BYBIT_EXEC_LATENCY=0 — не запускается (track() становится no-op).
    Вызывать ОДИН раз в bootstrap'е после init'а trading WS.
    """
    global _global_instance
    if not _ENABLED:
        print("[BYBIT-WS-EXEC] отключён через BYBIT_EXEC_LATENCY=0", flush=True)
        return None
    with _global_instance_lock:
        if _global_instance is None:
            _global_instance = BybitWsExecution.get(api_key, api_secret)
        return _global_instance


def get_instance() -> BybitWsExecution | None:
    return _global_instance


def is_enabled() -> bool:
    return _ENABLED
