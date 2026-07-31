from __future__ import annotations

# ── gate_ws.py ────────────────────────────────────────────────────
# Gate.io Futures WebSocket Trade клиент.
# Аналог bybit_sync_ws_trade.py: постоянное WS-соединение, размещение
# ордеров напрямую через WS (без REST roundtrip), приём ack в reader-потоке.
#
# FIX 2026-06-06: Gate-ордера шли через REST (_post_signed → HTTP POST)
# с задержкой 200-700мс. WS-путь даёт ~50-100мс (один frame до биржи
# вместо HMAC+HTTP+TLS-roundtrip). Прогрев TLS при старте убирает cold
# connect ~300-500мс с ПЕРВОГО ордера.
#
# Gate WS API (futures v4): wss://fx-ws.gateio.ws/v4/ws/usdt
#   подписка: event=subscribe, auth.method=api_key
#   ордер:    event=api, channel=futures.order_place
# Сигнатуры разные: subscribe использует &-sep, api-request \n-sep.
# ─────────────────────────────────────────────────────────────────

import hashlib
import hmac
import json
import threading
import time
import uuid

import websockets.sync.client as _ws_sync

try:
    import orjson as _orjson
    def _dumps(o) -> str:
        return _orjson.dumps(o).decode()
    def _loads(b: str | bytes):
        if isinstance(b, bytes): b = b.decode()
        return _orjson.loads(b)
except ImportError:
    def _dumps(o) -> str:
        return json.dumps(o, separators=(",", ":"), ensure_ascii=False)
    def _loads(b: str | bytes):
        if isinstance(b, bytes): b = b.decode()
        return json.loads(b)


_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
# FIX 2026-06-09: протокол приведён к офиц. Gate WS v4 (auth внутри payload,
# futures.login, ответ {request_id, ack, data.result/errs}). Таймаут 1.5с —
# при рабочем протоколе ack приходит ~200мс, запас против преждевременного
# REST-фолбэка (фолбэк после исполнившегося WS-ордера = двойная позиция).
_ORDER_ACK_TIMEOUT = 1.5      # ждать финального ответа на ордер
_LOGIN_TIMEOUT = 3.0          # ждать подтверждения futures.login на коннекте
_RECONNECT_DELAY_INIT = 1.0   # начальная задержка реконнекта
_RECONNECT_DELAY_MAX = 30.0
_PING_INTERVAL = 15            # Gate требует пинг каждые 15с


class GateWsClient:
    """Синглтон — один экземпляр на процесс."""

    def __init__(self, api_key: str, api_secret: str) -> None:
        self._key = api_key
        self._secret = api_secret.encode()
        self._ws: _ws_sync.ClientConnection | None = None
        self._connected = threading.Event()
        self._send_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._pending: dict[str, threading.Event] = {}
        self._pending_results: dict[str, dict | None] = {}
        self._connected_at = 0.0

    # ── подпись ──────────────────────────────────────────────────
    def _sign_api(self, channel: str, req_param_json: str, ts: int) -> str:
        # Gate v4 WS API: sign = HMAC_SHA512(secret, "api\n{channel}\n{req_param}\n{ts}").
        msg = f"api\n{channel}\n{req_param_json}\n{ts}"
        return hmac.new(self._secret, msg.encode(), hashlib.sha512).hexdigest()

    def _build_api_msg(self, channel: str, req_param, req_id: str) -> str:
        """
        Собирает api-request по формату Gate WS v4 (см. офиц. SDK gatews):
        верхний уровень {time, channel, event:'api', payload}; auth и req_param —
        ВНУТРИ payload. req_param=None для login (сериализуется как null).
        Подпись считается по тому же JSON req_param, что и отправляется (orjson
        детерминированен: nested == standalone), поэтому байты совпадают.
        """
        ts = int(time.time())
        req_param_json = _dumps(req_param)        # "null" для None, "{...}" для dict
        sign = self._sign_api(channel, req_param_json, ts)
        return _dumps({
            "time": ts,
            "channel": channel,
            "event": "api",
            "payload": {
                "api_key": self._key,
                "signature": sign,
                "timestamp": str(ts),
                "req_id": req_id,
                "req_header": {},
                "req_param": req_param,
            },
        })

    # ── подключение ──────────────────────────────────────────────
    def connect(self) -> bool:
        """Блокирующее подключение + futures.login. True = успех (залогинены)."""
        try:
            ws = _ws_sync.connect(_WS_URL, open_timeout=5)
        except Exception as e:  # noqa: BLE001
            print(f"[GATE-WS] connect failed: {e!r}", flush=True)
            return False
        try:
            # futures.login (api event) — аутентификация сессии для order_place.
            login_id = "login-" + uuid.uuid4().hex[:8]
            ws.send(self._build_api_msg("futures.login", None, login_id))
            if not self._await_login(ws, login_id, _LOGIN_TIMEOUT):
                print("[GATE-WS] login не подтверждён — закрываю сокет", flush=True)
                try:
                    ws.close()
                except Exception:  # noqa: BLE001
                    pass
                return False
            self._ws = ws
            self._connected.set()
            self._connected_at = time.monotonic()
            print("[GATE-WS] connect+login ✓", flush=True)
            return True
        except Exception as e:  # noqa: BLE001
            print(f"[GATE-WS] login failed: {e!r}", flush=True)
            try:
                ws.close()
            except Exception:  # noqa: BLE001
                pass
            return False

    def _await_login(self, ws, login_id: str, timeout: float) -> bool:
        """Ждёт ответ на futures.login (status ok / data.result), до timeout."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            try:
                raw = ws.recv(timeout=max(0.1, end - time.monotonic()))
            except TimeoutError:
                return False
            except Exception:  # noqa: BLE001
                return False
            try:
                msg = _loads(raw)
            except Exception:  # noqa: BLE001
                continue
            rid = msg.get("request_id") or msg.get("req_id") or ""
            ch = msg.get("channel") or (msg.get("header") or {}).get("channel") or ""
            if rid != login_id and ch != "futures.login":
                continue                      # не login-ответ (напр. broadcast)
            if msg.get("ack") is True:
                continue                      # промежуточный ack — ждём финал
            data = msg.get("data") or {}
            if data.get("errs") or data.get("error") or msg.get("error"):
                print(f"[GATE-WS] login err: {data.get('errs') or data.get('error') or msg.get('error')}",
                      flush=True)
                return False
            return True                       # финальный ответ без ошибки
        return False

    @property
    def is_ready(self) -> bool:
        return self._connected.is_set()

    def ready_time(self) -> float:
        return self._connected_at

    # ── реконнект-цикл (reader thread) ────────────────────────────
    def _run_loop(self) -> None:
        delay = _RECONNECT_DELAY_INIT
        while True:
            if self.connect():
                delay = _RECONNECT_DELAY_INIT
                self._read_loop()
                delay = _RECONNECT_DELAY_INIT
            self._connected.clear()
            self._ws = None
            # разбудить всех кто ждал аck (таймаут)
            with self._pending_lock:
                for ev in self._pending.values():
                    ev.set()
            time.sleep(delay)
            delay = min(delay * 2, _RECONNECT_DELAY_MAX)

    def _read_loop(self) -> None:
        ws = self._ws
        last_ping = time.monotonic()
        while True:
            try:
                raw = ws.recv(timeout=_PING_INTERVAL)
            except TimeoutError:
                # пинг для поддержания соединения
                now = time.monotonic()
                if now - last_ping >= _PING_INTERVAL:
                    try:
                        ws.send(_dumps({"time": int(time.time()), "channel": "futures.ping"}))
                    except Exception:
                        break
                    last_ping = now
                continue
            except Exception:
                break

            try:
                msg = _loads(raw)
            except Exception:
                continue

            self._handle_msg(msg)
        # вышли из recv — коннект разорван
        self._connected.clear()

    def _handle_msg(self, msg: dict) -> None:
        """
        Матчит ответ api-request с ожидающим req_id и резолвит его.
        Формат Gate v4: {request_id, ack, header, data:{result|errs}}.
        ack:true — промежуточный (ждём финал ack:false). На финале кладём
        data.result (успех) либо {"_error": ...} (ошибка) и будим ожидающего.
        """
        rid = msg.get("request_id") or msg.get("req_id") or ""
        if not rid:
            return
        with self._pending_lock:
            waiting = rid in self._pending
        if not waiting:
            return                                  # не наш / login / broadcast
        if msg.get("ack") is True:
            return                                  # промежуточный ack — ждём финал
        data = msg.get("data") or {}
        result = data.get("result")
        errs = data.get("errs") or data.get("error") or msg.get("error")
        with self._pending_lock:
            ev = self._pending.pop(rid, None)
            if ev is None:
                return
            if isinstance(result, dict):
                self._pending_results[rid] = result
            else:
                self._pending_results[rid] = {"_error": errs or data or msg}
            ev.set()

    def _send_api(self, ch: str, args: dict, req_id: str) -> None:
        """Отправляет api-request order_place по формату Gate WS v4."""
        full_msg = self._build_api_msg(ch, args, req_id)
        with self._send_lock:
            self._ws.send(full_msg)

    # ── размещение ордера через WS ────────────────────────────────
    def place_order(self, args: dict, timeout: float = _ORDER_ACK_TIMEOUT) -> dict | None:
        if not self._connected.is_set() or self._ws is None:
            return None
        ch = "futures.order_place"
        req_id = str(uuid.uuid4())[:12]
        ev = threading.Event()
        with self._pending_lock:
            self._pending[req_id] = ev
        try:
            self._send_api(ch, args, req_id)
        except Exception as e:
            with self._pending_lock:
                self._pending.pop(req_id, None)
            print(f"[GATE-WS] send failed: {e!r}", flush=True)
            return None
        if not ev.wait(timeout):
            with self._pending_lock:
                self._pending.pop(req_id, None)
                self._pending_results.pop(req_id, None)  # на случай гонки set/timeout
            # Финальный ответ не пришёл за timeout → REST-фолбэк. (Редко при
            # рабочем протоколе; ⚠️ остаточный риск двойной позиции, если
            # WS-ордер всё же исполнился — поэтому timeout с запасом.)
            print(f"[GATE-WS] order ack timeout {timeout:.1f}с → REST fallback",
                  flush=True)
            return None
        with self._pending_lock:
            res = self._pending_results.pop(req_id, None)
        if isinstance(res, dict) and res.get("_error") is not None:
            # Биржа отвергла ордер (ордер НЕ исполнен) → REST-фолбэк безопасен.
            print(f"[GATE-WS] order rejected: {res['_error']} → REST fallback", flush=True)
            return None
        return res

    def place_order_fast(self, args: dict) -> dict | None:
        if not self._connected.is_set() or self._ws is None:
            return None
        req_id = str(uuid.uuid4())[:12]
        try:
            self._send_api("futures.order_place", args, req_id)
            return {"sent": True, "req_id": req_id}
        except Exception as e:
            print(f"[GATE-WS] send failed: {e!r}", flush=True)
            return None


# ── singleton ─────────────────────────────────────────────────────
_inst: GateWsClient | None = None
_loop_thread: threading.Thread | None = None
_lock = threading.Lock()


def init(api_key: str, api_secret: str) -> GateWsClient:
    """Инициализирует (или возвращает существующий) синглтон."""
    global _inst, _loop_thread
    with _lock:
        if _inst is not None:
            return _inst
        _inst = GateWsClient(api_key, api_secret)
        _loop_thread = threading.Thread(target=_inst._run_loop, daemon=True, name="gate-ws-loop")
        _loop_thread.start()
        return _inst


def get_client() -> GateWsClient | None:
    return _inst


def place_order_ws(args: dict, timeout: float = _ORDER_ACK_TIMEOUT) -> dict | None:
    """Размещает ордер через Gate WS (ожидание ack). None → REST fallback."""
    inst = _inst
    if inst is None:
        return None
    return inst.place_order(args, timeout=timeout)


def warmup_gate_ws(timeout: float = 10.0) -> bool:
    """
    Прогревает Gate WS: инициализирует синглтон и ждёт подключения.
    Вызывается при старте — убирает холодный TCP+TLS (~300-500мс) с ПЕРВОГО ордера.
    """
    inst = _inst
    if inst is None:
        print("[GATE-WS] клиент не инициализирован — warmup skip", flush=True)
        return False
    print(f"[GATE-WS] прогрев WS (timeout {timeout:.0f}с)...", flush=True)
    ok = inst._connected.wait(timeout=timeout)
    if ok:
        print("[GATE-WS] прогрев WS ✓", flush=True)
    else:
        print("[GATE-WS] прогрев WS не дождался за таймаут", flush=True)
    return ok
