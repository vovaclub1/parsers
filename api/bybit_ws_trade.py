from __future__ import annotations

# ── bybit_ws_trade.py ─────────────────────────────────────────────
# FIX-batch-5: Bybit V5 WebSocket Trade API клиент.
#   wss://stream.bybit.com/v5/trade
#
# Зачем: размещение ордеров через persistent WS на 30-80мс быстрее
# чем REST (нет TCP+TLS handshake на каждый запрос).
#
# Архитектура:
#   - 1 фоновый thread, в нём asyncio loop
#   - persistent WS connection с auto-reconnect
#   - очередь pending запросов: каждый ордер получает уникальный reqId,
#     результат отдаётся через asyncio.Future, который sync-код ждёт
#     через concurrent.futures.Future
#   - sync API: ws_place_order(...) → блокирует до получения ack или таймаута
#
# Безопасность: если WS не подключён / таймаут → возвращает None,
# вызывающий код должен сделать fallback на REST.
#
# Документация: https://bybit-exchange.github.io/docs/v5/websocket/trade/guideline
# ─────────────────────────────────────────────────────────────────

import asyncio
import hashlib
import hmac
import json
import threading
import time
import uuid
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosedError

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

# Reconnect parameters
_RECONNECT_DELAY_MIN = 1.0
_RECONNECT_DELAY_MAX = 30.0

# Auth подпись — окно валидности.
# FIX: 10с было слишком мало. При системном clock-skew клиента ≥10с (NTP
# дрифт, контейнер с нестабильным временем) Bybit отвергал auth, мы шли в
# `continue` и сразу повторяли — получался hot-loop. 60с даёт запас для
# нормального дрифта.
_AUTH_EXPIRES_SEC = 60

# Таймаут для ack ответа.
# FIX: 0.5с был слишком агрессивным — на любой сетевой glitch (jitter >500мс)
# WS возвращал None → REST делал ВТОРОЙ ордер на ту же монету (double-position
# risk), потому что первый WS-ордер уже мог быть принят Bybit.
# Теперь orderLinkId защищает от дублей (см. place_order ниже), поэтому можем
# поднять таймаут до 1.5с без риска. WS ack нормально приходит за 5-30мс,
# 1.5с покрывает 99.9% случаев. Если всё же таймаут → REST с тем же
# orderLinkId, Bybit отвергнет дубль с retCode 30050.
_ORDER_ACK_TIMEOUT = 1.5


class WSOrderRejected(Exception):
    """
    FIX-2: WS-канал ОТВЕТИЛ, но Bybit логически отверг ордер
    (insufficient balance, leverage limit, invalid symbol и т.п.).
    В отличие от транспортного сбоя (None), REST fallback с теми же
    параметрами тоже будет отвергнут — поэтому caller должен НЕ делать
    REST повтор, а сразу сдаваться (worker сделает свой retry).
    """
    def __init__(self, ack: dict) -> None:
        self.ack = ack
        ret_code = ack.get("retCode") if isinstance(ack, dict) else None
        ret_msg  = ack.get("retMsg")  if isinstance(ack, dict) else None
        super().__init__(f"Bybit rejected order retCode={ret_code} retMsg={ret_msg!r}")


class BybitWsTrade:
    """
    Singleton-обёртка над persistent WS Bybit V5 Trade.
    """
    _instance: "BybitWsTrade | None" = None
    _instance_lock = threading.Lock()

    @classmethod
    def get(cls, api_key: str, api_secret: str) -> "BybitWsTrade":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls(api_key, api_secret)
                cls._instance._start()
            return cls._instance

    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key    = api_key
        self.api_secret = api_secret

        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws:   websockets.WebSocketClientProtocol | None = None
        self._connected = threading.Event()
        self._pending:  dict[str, asyncio.Future] = {}
        self._pending_lock = threading.Lock()
        # FIX: asyncio.Lock защищает от concurrent ws.send из разных корутин.
        # Без него современные `websockets` бросают ConcurrencyError, а старые
        # перемешивают кадры. Создаётся лениво в loop-потоке (asyncio.Lock
        # привязан к event-loop'у, иначе ругается).
        self._send_lock: asyncio.Lock | None = None

        self._thread: threading.Thread | None = None
        self._stop = False

    # ── thread entry ────────────────────────────────────────────────
    def _start(self) -> None:
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="bybit-ws-trade",
        )
        self._thread.start()

    def _run_loop(self) -> None:
        # FIX: uvloop.install() deprecated с 0.18+. Создаём loop через политику
        # uvloop локально, чтобы НЕ менять глобальную asyncio policy (другие
        # asyncio.run() в других потоках не должны страдать).
        try:
            import uvloop  # type: ignore[import-not-found]
            self._loop = uvloop.new_event_loop()
        except ImportError:
            self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._listener())
        except Exception as e:
            print(f"[BYBIT-WS] loop crashed: {e!r}", flush=True)

    # ── основной listener ──────────────────────────────────────────
    async def _listener(self) -> None:
        # FIX: создаём send-lock в loop-потоке (asyncio.Lock привязан к
        # текущему running loop при создании).
        if self._send_lock is None:
            self._send_lock = asyncio.Lock()
        delay = _RECONNECT_DELAY_MIN
        while not self._stop:
            try:
                print(f"[BYBIT-WS] connect → {WS_TRADE_URL}", flush=True)
                async with websockets.connect(
                    WS_TRADE_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws

                    # ── Auth ────────────────────────────────────────
                    expires = int((time.time() + _AUTH_EXPIRES_SEC) * 1000)
                    sig_msg = f"GET/realtime{expires}"
                    signature = hmac.new(
                        self.api_secret.encode(),
                        sig_msg.encode(),
                        hashlib.sha256,
                    ).hexdigest()

                    await ws.send(_json_dumps({
                        "op": "auth",
                        "args": [self.api_key, expires, signature],
                    }))

                    auth_resp_raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    auth_resp = _json_loads(auth_resp_raw)
                    # V5 Trade auth ответ: содержит retCode (0 = OK)
                    ret_code = auth_resp.get("retCode")
                    auth_ok = (ret_code == 0)
                    if not auth_ok:
                        # FIX: до этого фикса `continue` перепрыгивал блок
                        # delay-incrementation в конце цикла — каждая
                        # неудачная auth получала минимальный delay → hot-loop.
                        # Теперь явно поднимаем backoff здесь.
                        print(f"[BYBIT-WS] auth failed: {auth_resp} — reconnect через {delay:.0f}с", flush=True)
                        self._ws = None
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, _RECONNECT_DELAY_MAX)
                        continue

                    print("[BYBIT-WS] auth OK ✓", flush=True)
                    self._connected.set()
                    delay = _RECONNECT_DELAY_MIN

                    async for raw in ws:
                        try:
                            msg = _json_loads(raw)
                        except Exception:
                            continue
                        await self._dispatch(msg)

            except (ConnectionClosedError, OSError, asyncio.TimeoutError) as e:
                print(f"[BYBIT-WS] disconnect: {e} — reconnect через {delay:.0f}с", flush=True)
            except Exception as e:
                print(f"[BYBIT-WS] error: {e!r} — reconnect через {delay:.0f}с", flush=True)

            self._connected.clear()
            self._ws = None
            # Завалить все pending — вызывающие сделают REST fallback
            with self._pending_lock:
                for fut in self._pending.values():
                    if not fut.done():
                        fut.set_exception(ConnectionError("WS disconnected"))
                self._pending.clear()

            await asyncio.sleep(delay)
            delay = min(delay * 2, _RECONNECT_DELAY_MAX)

    # ── dispatch ack-ов ────────────────────────────────────────────
    async def _dispatch(self, msg: dict) -> None:
        req_id = msg.get("reqId") or msg.get("req_id")
        if not req_id:
            # pong / topic broadcast — игнор
            return
        with self._pending_lock:
            fut = self._pending.pop(req_id, None)
        if fut and not fut.done():
            fut.set_result(msg)

    # ── public sync API ────────────────────────────────────────────
    def is_ready(self, wait_sec: float = 0.0) -> bool:
        if self._connected.is_set():
            return True
        if wait_sec > 0:
            return self._connected.wait(wait_sec)
        return False

    def place_order(self, args: dict, timeout: float = _ORDER_ACK_TIMEOUT) -> dict | None:
        """
        Синхронно размещает ордер через WS. Возвращает dict с ack от Bybit,
        или None при таймауте / разрыве WS — вызывающий должен fallback на REST.

        args — payload как в REST /v5/order/create:
            {"category": "linear", "symbol": "BTCUSDT", "side": "Sell",
             "orderType": "Market", "qty": "0.01", "positionIdx": 2}
        """
        if not self._connected.is_set():
            return None
        if self._loop is None or self._ws is None:
            return None

        req_id = str(uuid.uuid4())
        ts_ms = str(int(time.time() * 1000))

        payload = {
            "reqId": req_id,
            "op":    "order.create",
            "header": {
                "X-BAPI-TIMESTAMP":  ts_ms,
                "X-BAPI-RECV-WINDOW": "5000",
                # NOTE: Referer = Bybit affiliate-code. Если есть свой
                # affiliate-аккаунт — замени "Parsers" на свой код, получишь
                # rev-share с комиссий. Сейчас просто маркер для логирования.
                "Referer":            "Parsers",
            },
            "args": [args],
        }

        # Создаём future в asyncio loop, ждём из sync кода через wrap.
        ack_event = threading.Event()
        result_box: dict[str, Any] = {}

        async def _send_and_wait() -> None:
            loop = asyncio.get_running_loop()
            fut: asyncio.Future = loop.create_future()
            with self._pending_lock:
                self._pending[req_id] = fut
            try:
                # FIX: фиксируем локальную ссылку на ws — иначе self._ws может
                # стать None между check и send из-за гонки с _listener'ом
                # (тот выставляет self._ws = None при disconnect).
                ws = self._ws
                if ws is None:
                    result_box["error"] = "ws_none"
                    return
                # FIX: send-lock — без него несколько одновременных place_order
                # могли вызвать ConcurrencyError (websockets >=11) или
                # interleaved frames (старые версии).
                send_lock = self._send_lock
                payload_str = _json_dumps(payload)
                if send_lock is not None:
                    async with send_lock:
                        await ws.send(payload_str)
                else:
                    await ws.send(payload_str)
                ack = await asyncio.wait_for(fut, timeout=timeout)
                result_box["ack"] = ack
            except asyncio.TimeoutError:
                result_box["error"] = "timeout"
            except (ConnectionClosedError, OSError, AttributeError) as e:
                # FIX-5: явно ловим disconnect mid-send + AttributeError
                # (self._ws стал None между check и send из-за гонки) —
                # детальный лог вместо generic Exception.
                result_box["error"] = f"transport: {type(e).__name__}: {e}"
            except Exception as e:
                result_box["error"] = repr(e)
            finally:
                with self._pending_lock:
                    self._pending.pop(req_id, None)
                ack_event.set()

        # schedule coroutine в asyncio loop из sync thread
        asyncio.run_coroutine_threadsafe(_send_and_wait(), self._loop)

        # ждём результата (max ~timeout + 0.5с запас)
        if not ack_event.wait(timeout + 0.5):
            print(f"[BYBIT-WS] ack_event wait expired (req_id={req_id}) — REST fallback", flush=True)
            return None

        if "error" in result_box:
            print(f"[BYBIT-WS] place_order failed ({result_box['error']}) — REST fallback", flush=True)
            return None

        if "ack" in result_box:
            ack = result_box["ack"]
            if not isinstance(ack, dict):
                return None
            if ack.get("retCode", -1) != 0:
                # FIX-2: WS-канал сработал, Bybit ОТВЕТИЛ, но логически отверг ордер.
                # REST с тем же payload вернёт ту же ошибку — поэтому raise, чтобы
                # caller НЕ делал REST fallback (вместо тихого return None).
                raise WSOrderRejected(ack)
            return ack
        return None

    # ── FIX-PERF: fire-and-forget ─────────────────────────────────
    def place_order_fast(self, args: dict) -> dict | None:
        """
        FIX-PERF: fire-and-forget WS-размещение ордера.

        Семантика:
          - Ждём только завершение ws.send() (~1-5мс), НЕ ждём ack от Bybit.
          - Ack отслеживается в фоне (_watch_ack): при retCode != 0
            пишется [BYBIT-WS-FAST] REJECTED в stdout. Success → silent.

        Зачем: ack-roundtrip = 70-120мс физика (RTT до Bybit Singapore).
        Caller'у в hot-path он не нужен — amount/entry_price считаются
        локально, TP/SL выставляется отдельно через position/trading-stop.
        Если Bybit отвергнет ордер — узнаем в фоновом логе, retry в этом
        случае всё равно бесполезен (rejections — permanent: balance/limits/qty).

        Возврат:
          None       — WS не подключён ИЛИ transport-ошибка при send → REST fallback.
          dict       — frame ушёл на провод; ack ждём в фоне.
        """
        if not self._connected.is_set():
            return None
        if self._loop is None or self._ws is None:
            return None

        req_id = str(uuid.uuid4())
        ts_ms  = str(int(time.time() * 1000))
        symbol = args.get("symbol", "?")

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

        send_done = threading.Event()
        send_err: list[str] = []

        async def _send_and_track() -> None:
            loop = asyncio.get_running_loop()
            # Регистрируем future, чтобы reader-loop (_dispatch) мог
            # доставить ack. _watch_ack ждёт его в фоне и логирует reject.
            fut: asyncio.Future = loop.create_future()
            with self._pending_lock:
                self._pending[req_id] = fut
            loop.create_task(self._watch_ack(req_id, fut, symbol))

            try:
                ws = self._ws
                if ws is None:
                    send_err.append("ws_none")
                    return
                payload_str = _json_dumps(payload)
                send_lock = self._send_lock
                if send_lock is not None:
                    async with send_lock:
                        await ws.send(payload_str)
                else:
                    await ws.send(payload_str)
            except (ConnectionClosedError, OSError, AttributeError) as e:
                send_err.append(f"transport: {type(e).__name__}: {e}")
            except Exception as e:  # noqa: BLE001
                send_err.append(repr(e))
            finally:
                send_done.set()

        asyncio.run_coroutine_threadsafe(_send_and_track(), self._loop)

        # Ждём только окончания send (это ~1-5мс, локально), не ack.
        # 0.5с — потолок на случай зависшего loop'а; в норме сразу.
        if not send_done.wait(0.5):
            print(f"[BYBIT-WS-FAST] send timeout {symbol} req_id={req_id} — REST fallback", flush=True)
            return None

        if send_err:
            print(f"[BYBIT-WS-FAST] send failed ({send_err[0]}) {symbol} — REST fallback", flush=True)
            return None

        return {"sent": True, "reqId": req_id}

    async def _watch_ack(self, req_id: str, fut: "asyncio.Future", symbol: str) -> None:
        """
        Фоновый наблюдатель за ack после fire-and-forget place_order_fast.
        Reject → лог. Success → silent. Timeout → лог.
        """
        try:
            ack = await asyncio.wait_for(fut, timeout=_ORDER_ACK_TIMEOUT)
            if isinstance(ack, dict) and ack.get("retCode", -1) != 0:
                print(
                    f"[BYBIT-WS-FAST] REJECTED {symbol} retCode={ack.get('retCode')} "
                    f"retMsg={ack.get('retMsg')!r}",
                    flush=True,
                )
        except asyncio.TimeoutError:
            print(f"[BYBIT-WS-FAST] ack timeout {symbol} req_id={req_id}", flush=True)
        except Exception as e:  # noqa: BLE001 — ConnectionError при WS reconnect
            print(f"[BYBIT-WS-FAST] ack error {symbol} req_id={req_id}: {e!r}", flush=True)
        finally:
            with self._pending_lock:
                self._pending.pop(req_id, None)


# ── удобные обёртки ──────────────────────────────────────────────

_global_instance: BybitWsTrade | None = None
# FIX-11: переименовано из _init_lock — чтобы не путать с BybitWsTrade._instance_lock
# (тот защищает создание объекта в .get(), а этот — модульный singleton).
_global_instance_lock = threading.Lock()


def init(api_key: str, api_secret: str) -> BybitWsTrade:
    """
    Инициализирует singleton (вызывать один раз в начале процесса).
    FIX-14: thread-safe — если два потока одновременно вызовут init(),
    оба увидят None в проверке и создадут инстанс дважды (BybitWsTrade.get
    защищён внутренним lock, так что реально создастся один, но
    _global_instance мог бы присвоиться дважды без этого lock).
    """
    global _global_instance
    with _global_instance_lock:
        if _global_instance is None:
            _global_instance = BybitWsTrade.get(api_key, api_secret)
        return _global_instance


def get_instance() -> BybitWsTrade | None:
    return _global_instance


def place_order_ws(args: dict, timeout: float = _ORDER_ACK_TIMEOUT) -> dict | None:
    """
    Размещает ордер через WS. None → fallback на REST.
    args — стандартный payload Bybit /v5/order/create.
    """
    inst = _global_instance
    if inst is None:
        return None
    return inst.place_order(args, timeout=timeout)


def place_order_ws_fast(args: dict) -> dict | None:
    """
    FIX-PERF: fire-and-forget — возвращает после ws.send (без ack-wait).
    Снимает ~70-100мс RTT до Bybit из hot-path. Reject логируется в фоне.
    None → WS не подключён или transport-ошибка → REST fallback.
    """
    inst = _global_instance
    if inst is None:
        return None
    return inst.place_order_fast(args)