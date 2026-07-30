from __future__ import annotations

# ── bybit_sync_ws_trade.py ────────────────────────────────────────
# Синхронный (NO asyncio) WS-клиент Bybit V5 Trade.
#
# Зачем нужен этот файл при наличии bybit_ws_trade.py (async):
#   В async-варианте hot-path трейда выглядит так:
#     caller thread → run_coroutine_threadsafe(_send_and_track, ws-loop)
#                   → ws-loop thread wakeup (~100-300μs селфпайп asyncio)
#                   → _send_and_track выполняет ws.send (~50-200μs net)
#                   → set send_done (~10-50μs)
#                   → caller thread wakeup (~10-50μs)
#   ≈ 200μs-2мс лишнего cross-thread оверхеда.
#
#   Здесь caller thread сам делает sock.send (под send_lock). 0 cross-thread:
#     caller thread → send_lock.acquire (~0.1μs uncontended)
#                   → ws.send (frame encode + sock.sendall ~50-200μs)
#                   → send_lock.release → return
#   Pure win по сравнению с async: -200μs..-2мс на трейд (зависит от
#   нагрузки loop'а на момент wakeup'а).
#
# Архитектура:
#   • 1 manager-thread "bybit-sync-ws-mgr" — connect + auth + reader-loop
#     (читает фреймы, разбирает ack, кладёт в _pending). Если соединение
#     рвётся — он же делает reconnect с exponential backoff.
#   • Caller-thread'ы (TG handler / pollers) вызывают place_order_fast()
#     синхронно, прямо в своём контексте.
#   • send_lock сериализует concurrent ws.send (websockets.sync.send
#     thread-safe сам по себе, но lock делает поведение детерминированным).
#
# Безопасность (избежали bug'ов из код-ревью):
#   1. self._ws → None race: фиксируем в локальный ws перед send; catch
#      ConnectionClosed.
#   2. Reader получает ack ДО регистрации pending: register под
#      _pending_lock ПЕРЕД ws.send.
#   3. Reader thread dies silently: try/except в reader-loop → finally
#      сбрасывает pending + reconnects через manager-loop.
#   4. Memory leak в _pending: cleanup в timeout/disconnect/reader.
#   5. Stop signal: _stop + close(ws) при .stop() (для тестов; в проде
#      thread daemon=True живёт всё время процесса).
#
# Документация Bybit:
#   https://bybit-exchange.github.io/docs/v5/websocket/trade/guideline
# ─────────────────────────────────────────────────────────────────

import hashlib
import hmac
import json
import threading
import time
import uuid
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


WS_TRADE_URL = "wss://stream.bybit.com/v5/trade"

_RECONNECT_DELAY_MIN = 1.0
_RECONNECT_DELAY_MAX = 30.0

# Окно auth-подписи. Запас на NTP-дрифт клиента (см. bybit_ws_trade.py).
_AUTH_EXPIRES_SEC = 60

# Таймаут для ожидания ack в ack-варианте place_order().
_ORDER_ACK_TIMEOUT = 1.5

# Таймаут recv() в reader-loop'е. Используется чтобы периодически
# проверять _stop и (при отсутствии keepalive в библиотеке) активно
# пинговать соединение.
#
# FIX-AUDIT: прежний комментарий утверждал, что «внутри websockets.sync есть
# свой keepalive-thread с ping/pong». Для websockets 12.x это НЕВЕРНО:
# keepalive у sync-клиента появился только в 13.0. Проверено по исходникам
# 12.0 — в websockets/sync/connection.py нет ни keepalive, ни ping_interval.
_READER_RECV_TIMEOUT = 30.0

# FIX-AUDIT: ping_interval/ping_timeout поддерживаются sync-клиентом только
# с websockets 13.0. Определяем это по сигнатуре, а не по номеру версии —
# так код работает и на 12.x (pinned в requirements), и после апгрейда.
#
# Без keepalive обрыв соединения не детектируется: reader висит в recv,
# _connected остаётся выставленным, а первый send() в half-open TCP проходит
# успешно (ядро буферизует) — ордер уходит в никуда, и REST-fallback НЕ
# срабатывает, потому что place_order_fast вернул {"sent": True}.
# Воспроизведено на сокете: 1-й send в закрытый peer'ом сокет проходит
# без ошибки, BrokenPipeError прилетает только на 2-м.
_WS_PING_INTERVAL = 20.0
_WS_PING_TIMEOUT = 10.0


def _detect_keepalive_kwargs() -> dict:
    """Возвращает keepalive-kwargs, если их принимает установленный websockets."""
    try:
        import inspect

        from websockets.sync.client import connect as _sync_connect

        params = inspect.signature(_sync_connect).parameters
        if "ping_interval" in params and "ping_timeout" in params:
            return {
                "ping_interval": _WS_PING_INTERVAL,
                "ping_timeout": _WS_PING_TIMEOUT,
            }
    except Exception:  # noqa: BLE001 — библиотека может отсутствовать
        pass
    return {}


_KEEPALIVE_KW = _detect_keepalive_kwargs()

# Если библиотека keepalive не умеет — держим соединение живым сами:
# reader-thread после каждого idle-таймаута шлёт application-level ping
# и проверяет, что pong пришёл. Bybit V5 Trade поддерживает {"op":"ping"}.
_MANUAL_PING_NEEDED = not _KEEPALIVE_KW

# Сколько idle-таймаутов подряд (по _READER_RECV_TIMEOUT каждый) терпим без
# единого фрейма от Bybit, прежде чем признать соединение мёртвым.
# 2 x 30с = 60с тишины при том, что мы шлём ping каждые 30с и periodic
# warmup идёт каждые 45с — на живом соединении такого не бывает.
_MAX_SILENT_TIMEOUTS = 2


class BybitSyncWsTrade:
    """
    Singleton sync-обёртка над persistent WS Bybit V5 Trade.

    Использование:
      inst = BybitSyncWsTrade.get(key, secret)
      inst.is_ready(wait_sec=3.0)
      inst.warmup()
      ack = inst.place_order_fast({"category":"linear", ...})
    """
    _instance: "BybitSyncWsTrade | None" = None
    _instance_lock = threading.Lock()

    @classmethod
    def get(cls, api_key: str, api_secret: str) -> "BybitSyncWsTrade":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls(api_key, api_secret)
                cls._instance._start()
            return cls._instance

    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key    = api_key
        self.api_secret = api_secret

        self._ws: Any = None                   # websockets.sync.ClientConnection | None
        self._ws_lock = threading.Lock()       # serialize self._ws reassign
        self._send_lock = threading.Lock()     # serialize concurrent ws.send
        self._connected = threading.Event()

        # Pending: req_id → (Event, result_dict_to_fill)
        # Result-dict нужен, чтобы reader клал ack ДО того как ev.set();
        # caller потом читает результат под _pending_lock.
        self._pending: dict[str, threading.Event] = {}
        self._pending_results: dict[str, dict] = {}
        self._pending_lock = threading.Lock()

        self._stop = False
        self._mgr_thread: threading.Thread | None = None

    # ── thread entry ────────────────────────────────────────────────
    def _start(self) -> None:
        if self._mgr_thread is not None and self._mgr_thread.is_alive():
            return
        self._mgr_thread = threading.Thread(
            target=self._mgr_loop,
            daemon=True,
            name="bybit-sync-ws-mgr",
        )
        self._mgr_thread.start()

    def stop(self) -> None:
        """Для тестов / graceful shutdown. В проде не вызывается."""
        self._stop = True
        with self._ws_lock:
            ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def force_reconnect(self) -> None:
        """
        Принудительно рвёт текущее соединение, НЕ останавливая manager-loop —
        он тут же поднимет новое.

        FIX-AUDIT: нужно для случая «сокет формально открыт, но Bybit не
        отвечает». Без этого _connected оставался выставленным, а
        place_order_fast возвращал {"sent": True} на half-open TCP, и ордер
        терялся молча — REST-fallback не срабатывал.

        Сначала снимаем _connected, чтобы hot-path немедленно перестал
        считать канал живым и ушёл на REST/async, и только потом закрываем
        сокет.
        """
        self._connected.clear()
        with self._ws_lock:
            ws = self._ws
            self._ws = None
        if ws is not None:
            try:
                ws.close()
            except Exception:  # noqa: BLE001 — сокет уже мёртв, это норма
                pass

    # ── manager-loop: connect + auth + reader, с reconnect ──────────
    def _mgr_loop(self) -> None:
        # Импорты в функции: модуль bybit_sync_ws_trade грузится из
        # bootstrap'а до того как websockets разогреется; ленивый
        # импорт даёт чистую трассировку при ImportError.
        try:
            from websockets.sync.client import connect as ws_connect
            from websockets.exceptions import ConnectionClosed
        except ImportError as e:
            print(f"[BYBIT-SYNC-WS] websockets >= 12 не установлен: {e!r}", flush=True)
            return

        delay = _RECONNECT_DELAY_MIN
        while not self._stop:
            try:
                print(f"[BYBIT-SYNC-WS] connect → {WS_TRADE_URL}", flush=True)
                # websockets.sync ClientConnection:
                # • open_timeout — на TCP+TLS+WS handshake
                # • ping_interval/ping_timeout появились у sync-клиента
                #   только в websockets 13.0. На 12.x их передавать нельзя —
                #   TypeError. Поэтому пробрасываем их ТОЛЬКО если
                #   установленная версия их поддерживает (см. _KEEPALIVE_KW).
                #
                # FIX-AUDIT: комментарий здесь раньше утверждал, что
                # «websockets keepalive сам шлёт ping каждые 20с». Для
                # sync-клиента 12.x это неверно — в websockets/sync/
                # connection.py 12.0 нет ни keepalive-потока, ни
                # ping_interval. Без ping'ов оборванное соединение не
                # детектируется: reader молча висит в recv, _connected
                # остаётся выставленным, а первый send() в half-open TCP
                # проходит успешно (ядро буферизует) — ордер уходит
                # в никуда и REST-fallback НЕ срабатывает, потому что
                # place_order_fast вернул {"sent": True}.
                ws = ws_connect(
                    WS_TRADE_URL,
                    open_timeout=10,
                    close_timeout=5,
                    max_size=2**20,  # 1MB, ack-фреймы крошечные
                    **_KEEPALIVE_KW,
                )
            except Exception as e:
                print(f"[BYBIT-SYNC-WS] connect failed: {e!r} — retry через {delay:.0f}с", flush=True)
                time.sleep(delay)
                delay = min(delay * 2, _RECONNECT_DELAY_MAX)
                continue

            try:
                # ── Auth ────────────────────────────────────────────
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
                auth_resp_raw = ws.recv(timeout=5)
                auth_resp = _json_loads(auth_resp_raw)
                if auth_resp.get("retCode") != 0:
                    print(f"[BYBIT-SYNC-WS] auth failed: {auth_resp} — reconnect через {delay:.0f}с", flush=True)
                    try:
                        ws.close()
                    except Exception:
                        pass
                    time.sleep(delay)
                    delay = min(delay * 2, _RECONNECT_DELAY_MAX)
                    continue

                print("[BYBIT-SYNC-WS] auth OK ✓", flush=True)

                # ── Установка self._ws ПОСЛЕ успешной auth ─────────
                # Раньше: устанавливать сразу после connect — caller
                # мог послать ордер до auth, Bybit вернёт reject.
                with self._ws_lock:
                    self._ws = ws
                self._connected.set()
                delay = _RECONNECT_DELAY_MIN

                # ── Reader-loop ────────────────────────────────────
                # FIX-AUDIT: считаем idle-таймауты. Если библиотека не умеет
                # keepalive (websockets 12.x), сами шлём application-level
                # ping и требуем ответ. Молчание сверх _MAX_SILENT_TIMEOUTS
                # подряд означает мёртвое соединение — рвём и реконнектимся,
                # вместо того чтобы держать _connected выставленным и терять
                # ордера в half-open сокет.
                silent_timeouts = 0
                while not self._stop:
                    try:
                        raw = ws.recv(timeout=_READER_RECV_TIMEOUT)
                    except TimeoutError:
                        if not _MANUAL_PING_NEEDED:
                            # Библиотека сама шлёт ping/pong — idle это норма.
                            continue
                        silent_timeouts += 1
                        if silent_timeouts > _MAX_SILENT_TIMEOUTS:
                            print(
                                "[BYBIT-SYNC-WS] нет ответа на "
                                f"{silent_timeouts} ping'ов подряд — "
                                "считаем соединение мёртвым, reconnect",
                                flush=True,
                            )
                            break
                        # Application-level ping: Bybit V5 отвечает
                        # {"op":"pong"}, что сбросит счётчик на след. итерации.
                        try:
                            with self._send_lock:
                                ws.send(_json_dumps({"op": "ping"}))
                        except Exception as e:  # noqa: BLE001
                            print(
                                f"[BYBIT-SYNC-WS] ping failed: {type(e).__name__}: {e}"
                                " — reconnect",
                                flush=True,
                            )
                            break
                        continue
                    except ConnectionClosed:
                        print("[BYBIT-SYNC-WS] connection closed by peer", flush=True)
                        break
                    except Exception as e:
                        print(f"[BYBIT-SYNC-WS] recv error: {e!r}", flush=True)
                        break

                    # Любой полученный фрейм (в т.ч. pong) = соединение живое.
                    silent_timeouts = 0

                    try:
                        msg = _json_loads(raw)
                    except Exception:
                        continue
                    self._dispatch(msg)

            except Exception as e:
                print(f"[BYBIT-SYNC-WS] mgr error: {e!r}", flush=True)
            finally:
                # Прибраться: пометить disconnect, освободить pending,
                # закрыть ws. Manager уйдёт на reconnect через time.sleep.
                self._connected.clear()
                with self._ws_lock:
                    self._ws = None
                self._fail_pending()
                try:
                    ws.close()
                except Exception:
                    pass

            time.sleep(delay)
            delay = min(delay * 2, _RECONNECT_DELAY_MAX)

    def _fail_pending(self) -> None:
        """Будит всех ожидающих ack на disconnect — они уйдут в REST."""
        with self._pending_lock:
            events = list(self._pending.values())
            self._pending.clear()
            self._pending_results.clear()
        for ev in events:
            ev.set()

    # ── приём ack ──────────────────────────────────────────────────
    def _dispatch(self, msg: dict) -> None:
        req_id = msg.get("reqId") or msg.get("req_id")
        if not req_id:
            # pong / topic broadcast / system msg — игнор
            return

        with self._pending_lock:
            ev = self._pending.pop(req_id, None)
            if ev is not None:
                self._pending_results[req_id] = msg

        if ev is not None:
            ev.set()
        else:
            # Orphan ack (fire-and-forget) — логируем только rejection.
            # Success silently dropping, иначе flood логов от каждого
            # успешного ордера.
            ret_code = msg.get("retCode", 0)
            if ret_code != 0:
                op = msg.get("op", "?")
                ret_msg = msg.get("retMsg")
                # warmup ордер шлёт несуществующий orderId — Bybit вернёт
                # 110001 / 170213 / похожее; для warmup это OK, не флудим.
                if str(req_id).startswith("warmup-"):
                    return
                print(
                    f"[BYBIT-SYNC-WS] REJECTED op={op} req_id={req_id} "
                    f"retCode={ret_code} retMsg={ret_msg!r}",
                    flush=True,
                )

    # ── public sync API ────────────────────────────────────────────
    def is_ready(self, wait_sec: float = 0.0) -> bool:
        if self._connected.is_set():
            return True
        if wait_sec > 0:
            return self._connected.wait(wait_sec)
        return False

    def place_order_fast(self, args: dict) -> dict | None:
        """
        ГОРЯЧИЙ ПУТЬ. Синхронно отправляет ордер через WS из текущего thread'а.

        Без cross-thread hop'ов. ws.send блокирует только на TCP write
        (~50-200μs для ~400-байтного фрейма).

        Возврат:
          None — WS не подключён / transport error → caller должен сделать REST fallback.
          {"sent": True, "reqId": ...} — frame ушёл на провод; reject
              (если будет) логируется async в reader-thread.
        """
        # Быстрый check (без lock'а — Event.is_set атомарен).
        if not self._connected.is_set():
            return None

        # Фиксируем локальную ссылку: reader-thread может сбросить self._ws
        # между этой строкой и ws.send. ConnectionClosed мы поймаем.
        ws = self._ws
        if ws is None:
            return None

        req_id = str(uuid.uuid4())
        ts_ms = str(int(time.time() * 1000))
        symbol = args.get("symbol", "?")
        payload = {
            "reqId": req_id,
            "op":    "order.create",
            "header": {
                "X-BAPI-TIMESTAMP":   ts_ms,
                "X-BAPI-RECV-WINDOW": "5000",
                # NOTE: Bybit affiliate-code (см. bybit_ws_trade.py).
                "Referer":            "Parsers",
            },
            "args": [args],
        }
        payload_str = _json_dumps(payload)

        try:
            with self._send_lock:
                ws.send(payload_str)
        except Exception as e:
            # ConnectionClosed, OSError, AttributeError — всё на REST fallback.
            print(f"[BYBIT-SYNC-WS-FAST] send failed {symbol}: {type(e).__name__}: {e} — REST", flush=True)
            return None

        return {"sent": True, "reqId": req_id}

    def place_order(self, args: dict, timeout: float = _ORDER_ACK_TIMEOUT) -> dict | None:
        """
        Ack-waiting вариант. Регистрирует pending ПЕРЕД send (чтобы
        быстрый ack не оказался orphan'ом), потом ждёт результат.

        Возврат: ack-dict от Bybit, или None при таймауте / transport-ошибке.
        Не raise'ит на bybit-reject — caller сам решает (см. retCode).
        """
        if not self._connected.is_set():
            return None
        ws = self._ws
        if ws is None:
            return None

        req_id = str(uuid.uuid4())
        ts_ms = str(int(time.time() * 1000))
        payload = {
            "reqId": req_id,
            "op":    "order.create",
            "header": {
                "X-BAPI-TIMESTAMP":   ts_ms,
                "X-BAPI-RECV-WINDOW": "5000",
                "Referer":            "Parsers",
            },
            "args": [args],
        }
        payload_str = _json_dumps(payload)

        ev = threading.Event()
        # CRITICAL: регистрация pending ПЕРЕД send. Bybit может ответить
        # за 5-10мс; если send → recv → dispatch выполнятся до того как
        # мы зарегистрируемся, dispatch не найдёт req_id и сложит ack
        # в orphan. Поэтому: зарегистрировались → послали → ждём.
        with self._pending_lock:
            self._pending[req_id] = ev

        try:
            with self._send_lock:
                ws.send(payload_str)
        except Exception as e:
            with self._pending_lock:
                self._pending.pop(req_id, None)
                self._pending_results.pop(req_id, None)
            print(f"[BYBIT-SYNC-WS] place_order send failed: {type(e).__name__}: {e}", flush=True)
            return None

        if not ev.wait(timeout):
            with self._pending_lock:
                self._pending.pop(req_id, None)
                self._pending_results.pop(req_id, None)
            return None

        with self._pending_lock:
            return self._pending_results.pop(req_id, None)

    def warmup(self, timeout: float = 1.0) -> bool:
        """
        Прогревает РОВНО тот же hot-path, что и place_order_fast:
          uuid → ts → dict-build → _json_dumps(nested) → send_lock → ws.send
          → reader → _json_loads → _dispatch → pending.pop → ev.set.

        Сценарий: op:order.cancel с фиктивным orderId. Bybit вернёт reject
        (Order not found) за 50-100мс. БЕЗ бокового эффекта — позиция не
        открывается, баланс не задевается. reject не флудит логи (см.
        _dispatch: warmup-* префикс игнорируется).

        Возврат: True если ack пришёл; False — не подключён / timeout.
        """
        if not self._connected.is_set():
            return False
        ws = self._ws
        if ws is None:
            return False

        req_id = "warmup-" + str(uuid.uuid4())
        ts_ms = str(int(time.time() * 1000))
        payload = {
            "reqId": req_id,
            "op":    "order.cancel",
            "header": {
                "X-BAPI-TIMESTAMP":   ts_ms,
                "X-BAPI-RECV-WINDOW": "5000",
                "Referer":            "Parsers",
            },
            "args": [{
                "category": "linear",
                "symbol":   "BTCUSDT",
                "orderId":  "00000000-0000-0000-0000-000000000000",
            }],
        }
        payload_str = _json_dumps(payload)

        ev = threading.Event()
        with self._pending_lock:
            self._pending[req_id] = ev

        try:
            with self._send_lock:
                ws.send(payload_str)
        except Exception:
            with self._pending_lock:
                self._pending.pop(req_id, None)
                self._pending_results.pop(req_id, None)
            return False

        ok = ev.wait(timeout)
        if not ok:
            with self._pending_lock:
                self._pending.pop(req_id, None)
                self._pending_results.pop(req_id, None)
            return False
        with self._pending_lock:
            self._pending_results.pop(req_id, None)
        return True


# ── удобные обёртки ──────────────────────────────────────────────

_global_instance: BybitSyncWsTrade | None = None
_global_instance_lock = threading.Lock()


def init(api_key: str, api_secret: str) -> BybitSyncWsTrade:
    """
    Инициализирует singleton sync WS Trade. Вызвать один раз в bootstrap'е.
    Thread-safe.
    """
    global _global_instance
    with _global_instance_lock:
        if _global_instance is None:
            _global_instance = BybitSyncWsTrade.get(api_key, api_secret)
        return _global_instance


def get_instance() -> BybitSyncWsTrade | None:
    return _global_instance


def place_order_sync_fast(args: dict) -> dict | None:
    """Sync fire-and-forget — обёртка над инстансовым методом."""
    inst = _global_instance
    if inst is None:
        return None
    return inst.place_order_fast(args)


def place_order_sync(args: dict, timeout: float = _ORDER_ACK_TIMEOUT) -> dict | None:
    """Sync ack-waiting — для случаев когда нужен Bybit-ответ."""
    inst = _global_instance
    if inst is None:
        return None
    return inst.place_order(args, timeout=timeout)


# ── периодический прогрев ────────────────────────────────────────
# По тем же причинам, что и в async-варианте: PEP-659 inline caches,
# CPU branch predictor и kernel TCP state остывают между листингами.
_PERIODIC_WARMUP_INTERVAL = 45.0
# Сколько прогревов подряд без ack терпим до принудительного reconnect'а.
_WARMUP_FAIL_LIMIT = 2
_periodic_warmup_thread: threading.Thread | None = None
_periodic_warmup_lock = threading.Lock()


def _periodic_warmup_loop() -> None:
    """
    Периодический прогрев hot-path + косвенный health-check соединения.

    FIX-AUDIT: раньше результат warmup'а полностью игнорировался
    (`except Exception: pass`, возврат не проверялся). Warmup — это
    round-trip к Bybit: если он N раз подряд не получает ack, соединение
    мертво, даже если сокет формально «открыт». Раньше в таком состоянии
    _connected оставался выставленным, place_order_fast возвращал
    {"sent": True}, и ордер терялся без REST-fallback.

    Теперь N подряд неудачных прогревов принудительно рвут соединение —
    manager-loop поднимет новое.
    """
    failures = 0
    while True:
        time.sleep(_PERIODIC_WARMUP_INTERVAL)
        inst = _global_instance
        if inst is None:
            continue
        if not inst.is_ready():
            # Не подключены — manager сам занимается reconnect'ом.
            failures = 0
            continue
        try:
            ok = inst.warmup(timeout=1.0)
        except Exception as e:  # noqa: BLE001
            ok = False
            print(f"[BYBIT-SYNC-WS] warmup упал: {e!r}", flush=True)

        if ok:
            failures = 0
            continue

        failures += 1
        print(
            f"[BYBIT-SYNC-WS] warmup без ack ({failures}/{_WARMUP_FAIL_LIMIT})",
            flush=True,
        )
        if failures >= _WARMUP_FAIL_LIMIT:
            print(
                "[BYBIT-SYNC-WS] соединение не отвечает на прогрев — "
                "принудительный reconnect (иначе ордера уходили бы "
                "в мёртвый сокет без REST-fallback)",
                flush=True,
            )
            failures = 0
            try:
                inst.force_reconnect()
            except Exception as e:  # noqa: BLE001
                print(f"[BYBIT-SYNC-WS] force_reconnect упал: {e!r}", flush=True)


def start_periodic_warmup() -> None:
    """Запускает фоновый прогрев (idempotent)."""
    global _periodic_warmup_thread
    with _periodic_warmup_lock:
        if _periodic_warmup_thread is not None and _periodic_warmup_thread.is_alive():
            return
        t = threading.Thread(
            target=_periodic_warmup_loop,
            daemon=True,
            name="bybit-sync-ws-warmup",
        )
        t.start()
        _periodic_warmup_thread = t
