from __future__ import annotations

# ── bybit_ws_private.py ───────────────────────────────────────────
# Sync Bybit V5 Private WS: подписки `order`, `execution` и `position`.
# wallet НЕ подписываем (требование пользователя).
#
# Зачем: после market_open_long нам нужно дождаться появления позиции
# (size>0, avgPrice) чтобы навесить trailing к РЕАЛЬНОЙ цене входа.
# Раньше это делалось REST polling'ом в _set_tp_sl_bybit:
#   for _ in range(10):
#       size, avg, trail = get_position(symbol, 1)   # ~70-150мс REST
#       if size > 0: break
#       time.sleep(0.3)
# Итого 0.3-3с задержки от open до trailing-set. С private WS позиция
# приходит в виде push-event'а за 10-30мс после fill'а, без поллинга.
#
# Архитектура аналогична bybit_sync_ws_trade.py:
#   • 1 manager-thread (connect+auth+subscribe+reader+reconnect)
#   • Кеш позиций {(symbol, positionIdx): PositionSnap}
#   • Кеш ордеров {orderLinkId: OrderSnap}
#   • Event'ы для wait_for_position / wait_for_order_fill
#
# Документация Bybit:
#   https://bybit-exchange.github.io/docs/v5/websocket/private/order
#   https://bybit-exchange.github.io/docs/v5/websocket/private/position
# ─────────────────────────────────────────────────────────────────

import hashlib
import hmac
import json
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
_AUTH_EXPIRES_SEC = 60
_READER_RECV_TIMEOUT = 30.0
_PING_INTERVAL = 20.0   # Bybit V5: ping каждые ≤20с (см. /docs/v5/ws/connect)

# Сколько свежих ордеров держать в кеше (anti-leak; реально нужны только
# несколько секунд после open). Чистится по TTL в reader-loop.
_ORDER_CACHE_TTL = 60.0
_POSITION_CACHE_TTL = 24 * 3600  # позиция может жить долго


class _PositionSnap:
    __slots__ = ("size", "avg_price", "trailing_stop", "updated_ts")

    def __init__(self, size: float = 0.0, avg_price: float = 0.0,
                 trailing_stop: float = 0.0, updated_ts: float = 0.0) -> None:
        self.size = size
        self.avg_price = avg_price
        self.trailing_stop = trailing_stop
        self.updated_ts = updated_ts


class _OrderSnap:
    __slots__ = ("status", "avg_price", "cum_exec_qty", "order_id", "updated_ts")

    def __init__(self, status: str = "", avg_price: float = 0.0,
                 cum_exec_qty: float = 0.0, order_id: str = "",
                 updated_ts: float = 0.0) -> None:
        self.status = status
        self.avg_price = avg_price
        self.cum_exec_qty = cum_exec_qty
        self.order_id = order_id
        self.updated_ts = updated_ts


class BybitWsPrivate:
    """
    Sync persistent private WS Bybit V5. Подписан на ['order','position'].
    Singleton.
    """
    _instance: "BybitWsPrivate | None" = None
    _instance_lock = threading.Lock()

    @classmethod
    def get(cls, api_key: str, api_secret: str) -> "BybitWsPrivate":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls(api_key, api_secret)
                cls._instance._start()
            return cls._instance

    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key = api_key
        self.api_secret = api_secret

        self._ws: Any = None
        self._ws_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._connected = threading.Event()

        # Кеши + condition'ы для wait'ов.
        self._pos_lock = threading.Lock()
        self._positions: dict[tuple[str, int], _PositionSnap] = {}
        # Кondition'ы для wait_for_position(symbol, idx, timeout).
        self._pos_cond = threading.Condition(self._pos_lock)

        self._ord_lock = threading.Lock()
        self._orders: dict[str, _OrderSnap] = {}
        self._ord_cond = threading.Condition(self._ord_lock)

        self._stop = False
        self._mgr_thread: threading.Thread | None = None
        self._execution_store = None

    def set_execution_store(self, store) -> None:
        self._execution_store = store

    # ── lifecycle ───────────────────────────────────────────────────
    def _start(self) -> None:
        if self._mgr_thread is not None and self._mgr_thread.is_alive():
            return
        self._mgr_thread = threading.Thread(
            target=self._mgr_loop,
            daemon=True,
            name="bybit-ws-private-mgr",
        )
        self._mgr_thread.start()

    def stop(self) -> None:
        self._stop = True
        with self._ws_lock:
            ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    # ── manager loop ────────────────────────────────────────────────
    def _mgr_loop(self) -> None:
        try:
            from websockets.sync.client import connect as ws_connect
            from websockets.exceptions import ConnectionClosed
        except ImportError as e:
            print(f"[BYBIT-WS-PRIV] websockets не установлен: {e!r}", flush=True)
            return

        delay = _RECONNECT_DELAY_MIN
        while not self._stop:
            try:
                print(f"[BYBIT-WS-PRIV] connect → {WS_PRIVATE_URL}", flush=True)
                ws = ws_connect(
                    WS_PRIVATE_URL,
                    open_timeout=10,
                    close_timeout=5,
                    max_size=2**20,
                )
            except Exception as e:
                print(f"[BYBIT-WS-PRIV] connect failed: {e!r} — retry {delay:.0f}с", flush=True)
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
                    "op":   "auth",
                    "args": [self.api_key, expires, signature],
                }))
                auth_raw = ws.recv(timeout=5)
                auth_resp = _json_loads(auth_raw)
                # FIX: Bybit V5 auth-ok может вернуться без retCode (success=True/op=auth).
                if not (auth_resp.get("success") is True
                        or auth_resp.get("retCode") == 0):
                    print(f"[BYBIT-WS-PRIV] auth failed: {auth_resp}", flush=True)
                    # FIX 2026-07-08: TG-алерт (раз в час) на мёртвый ключ.
                    try:
                        from tg.tg_logger import tg_alert_throttled
                        tg_alert_throttled(
                            "bybit-auth-fail",
                            f"🚨 <b>BYBIT AUTH FAIL</b>\n"
                            f"msg={auth_resp.get('ret_msg', auth_resp.get('retMsg', '?'))}\n"
                            f"Проверь/обнови API-ключ — ордера НЕ открываются!",
                        )
                    except Exception:  # noqa: BLE001
                        pass
                    try:
                        ws.close()
                    except Exception:
                        pass
                    time.sleep(delay)
                    delay = min(delay * 2, _RECONNECT_DELAY_MAX)
                    continue

                # ── Subscribe ───────────────────────────────────────
                # wallet НЕ подписываем по требованию пользователя.
                ws.send(_json_dumps({
                    "op":   "subscribe",
                    "args": ["order", "execution", "position"],
                }))

                with self._ws_lock:
                    self._ws = ws
                self._connected.set()
                delay = _RECONNECT_DELAY_MIN
                print("[BYBIT-WS-PRIV] auth+subscribe OK ✓", flush=True)

                last_ping = time.time()

                while not self._stop:
                    # Периодический ping (Bybit требует <= 20с между фреймами,
                    # иначе закроет соединение).
                    now = time.time()
                    if now - last_ping >= _PING_INTERVAL:
                        try:
                            with self._send_lock:
                                ws.send(_json_dumps({"op": "ping"}))
                        except Exception:
                            break
                        last_ping = now

                    try:
                        raw = ws.recv(timeout=_READER_RECV_TIMEOUT)
                    except TimeoutError:
                        continue
                    except ConnectionClosed:
                        print("[BYBIT-WS-PRIV] connection closed by peer", flush=True)
                        break
                    except Exception as e:
                        print(f"[BYBIT-WS-PRIV] recv error: {e!r}", flush=True)
                        break

                    try:
                        msg = _json_loads(raw)
                    except Exception:
                        continue
                    self._dispatch(msg)

            except Exception as e:
                print(f"[BYBIT-WS-PRIV] mgr error: {e!r}", flush=True)
            finally:
                self._connected.clear()
                with self._ws_lock:
                    self._ws = None
                try:
                    ws.close()
                except Exception:
                    pass

            time.sleep(delay)
            delay = min(delay * 2, _RECONNECT_DELAY_MAX)

    # ── dispatch ────────────────────────────────────────────────────
    def _dispatch(self, msg: dict) -> None:
        topic = msg.get("topic", "")
        if topic == "position":
            self._on_position(msg.get("data", []))
        elif topic == "order":
            self._on_order(msg.get("data", []))
        elif topic == "execution":
            self._on_execution(msg.get("data", []))
        # auth/subscribe/pong ack — игнор

    def _on_position(self, items: list[dict]) -> None:
        now = time.time()
        notify: list[tuple[str, int]] = []
        with self._pos_lock:
            for it in items:
                sym = it.get("symbol", "")
                if not sym:
                    continue
                try:
                    idx = int(it.get("positionIdx", 0))
                except (TypeError, ValueError):
                    idx = 0
                try:
                    size = float(it.get("size", "0") or 0)
                except (TypeError, ValueError):
                    size = 0.0
                try:
                    avg = float(it.get("avgPrice", "0") or 0)
                except (TypeError, ValueError):
                    avg = 0.0
                try:
                    trail = float(it.get("trailingStop", "0") or 0)
                except (TypeError, ValueError):
                    trail = 0.0

                key = (sym, idx)
                snap = self._positions.get(key)
                if snap is None:
                    snap = _PositionSnap()
                    self._positions[key] = snap
                snap.size = size
                snap.avg_price = avg
                snap.trailing_stop = trail
                snap.updated_ts = now
                notify.append(key)
                store = self._execution_store
                if store is not None:
                    try:
                        store.record_position(
                            venue="bybit", symbol=sym, position_idx=idx,
                            side=it.get("side", ""), size=size,
                            avg_price=avg, trailing_stop=trail,
                            ts_ns=int(it.get("updatedTime") or time.time_ns()),
                            raw=it,
                        )
                    except Exception as e:  # noqa: BLE001
                        print(f"[BYBIT-WS-PRIV] position persist failed: {e!r}", flush=True)
            if notify:
                # notify_all: все wait_for_position'ы перепроверят свои keys.
                self._pos_cond.notify_all()

    def _on_order(self, items: list[dict]) -> None:
        now = time.time()
        prune_threshold = now - _ORDER_CACHE_TTL
        with self._ord_lock:
            # Цикл cleanup — раз в _on_order пробегаемся по кешу;
            # ордера приходят достаточно часто чтобы это не оказалось редким.
            if len(self._orders) > 256:
                stale = [k for k, v in self._orders.items() if v.updated_ts < prune_threshold]
                for k in stale:
                    del self._orders[k]

            for it in items:
                link = it.get("orderLinkId", "")
                if not link:
                    continue
                status = it.get("orderStatus", "")
                try:
                    avg = float(it.get("avgPrice", "0") or 0)
                except (TypeError, ValueError):
                    avg = 0.0
                try:
                    cum = float(it.get("cumExecQty", "0") or 0)
                except (TypeError, ValueError):
                    cum = 0.0
                order_id = it.get("orderId", "")

                snap = self._orders.get(link)
                if snap is None:
                    snap = _OrderSnap()
                    self._orders[link] = snap
                snap.status = status
                snap.avg_price = avg
                snap.cum_exec_qty = cum
                snap.order_id = order_id
                snap.updated_ts = now
                store = self._execution_store
                if store is not None:
                    try:
                        store.record_order_event(
                            client_order_id=link, status=status,
                            exchange_order_id=order_id, avg_price=avg,
                            cum_exec_qty=cum,
                            ts_ns=int(it.get("updatedTime") or time.time_ns()),
                            raw=it,
                        )
                    except Exception as e:  # noqa: BLE001
                        print(f"[BYBIT-WS-PRIV] order persist failed: {e!r}", flush=True)
            if items:
                self._ord_cond.notify_all()

    def _on_execution(self, items: list[dict]) -> None:
        store = self._execution_store
        if store is None:
            return
        for it in items:
            exec_id = it.get("execId", "")
            if not exec_id:
                continue
            try:
                store.record_fill(
                    exec_id=exec_id,
                    client_order_id=it.get("orderLinkId", ""),
                    exchange_order_id=it.get("orderId", ""),
                    symbol=it.get("symbol", ""), side=it.get("side", ""),
                    price=float(it.get("execPrice", 0) or 0),
                    qty=float(it.get("execQty", 0) or 0),
                    fee=float(it.get("execFee", 0) or 0),
                    fee_asset=it.get("feeCurrency", ""),
                    ts_ns=int(it.get("execTime") or time.time_ns()),
                    raw=it,
                )
            except Exception as e:  # noqa: BLE001
                print(f"[BYBIT-WS-PRIV] execution persist failed: {e!r}", flush=True)

    # ── public sync API ─────────────────────────────────────────────
    def is_ready(self, wait_sec: float = 0.0) -> bool:
        if self._connected.is_set():
            return True
        if wait_sec > 0:
            return self._connected.wait(wait_sec)
        return False

    def get_position_cached(self, symbol: str,
                            position_idx: int = 1) -> tuple[float, float, float] | None:
        """
        Возвращает (size, avgPrice, trailingStop) из локального WS-кеша
        или None если позиция ни разу не приходила.
        """
        with self._pos_lock:
            snap = self._positions.get((symbol, position_idx))
            if snap is None:
                return None
            return snap.size, snap.avg_price, snap.trailing_stop

    def wait_for_position(self, symbol: str, position_idx: int = 1,
                          timeout: float = 3.0) -> tuple[float, float, float] | None:
        """
        Ждёт первого push-event'а с size>0 по этой паре. Возвращает
        (size, avgPrice, trailing) или None при таймауте.
        """
        deadline = time.time() + timeout
        with self._pos_cond:
            while True:
                snap = self._positions.get((symbol, position_idx))
                if snap is not None and snap.size > 0:
                    return snap.size, snap.avg_price, snap.trailing_stop
                remain = deadline - time.time()
                if remain <= 0:
                    return None
                self._pos_cond.wait(remain)

    def wait_for_position_trailing(self, symbol: str, position_idx: int = 1,
                                   timeout: float = 2.0) -> bool:
        """
        Ждёт push-event'а где trailing_stop>0 по этой позиции. True если
        дождались, False — таймаут.
        """
        deadline = time.time() + timeout
        with self._pos_cond:
            while True:
                snap = self._positions.get((symbol, position_idx))
                if snap is not None and snap.trailing_stop > 0 and snap.size > 0:
                    return True
                remain = deadline - time.time()
                if remain <= 0:
                    return False
                self._pos_cond.wait(remain)

    def get_order_cached(self, order_link_id: str) -> _OrderSnap | None:
        with self._ord_lock:
            return self._orders.get(order_link_id)

    def wait_for_order_filled(self, order_link_id: str,
                              timeout: float = 2.0) -> _OrderSnap | None:
        """
        Ждёт когда наш ордер по orderLinkId перейдёт в Filled (или
        отвергнут — возвращаем последний snap, caller сам проверит status).
        """
        deadline = time.time() + timeout
        with self._ord_cond:
            while True:
                snap = self._orders.get(order_link_id)
                if snap is not None and snap.status in (
                    "Filled", "PartiallyFilledCanceled", "Cancelled",
                    "Rejected", "Deactivated",
                ):
                    return snap
                remain = deadline - time.time()
                if remain <= 0:
                    return snap  # last-known или None
                self._ord_cond.wait(remain)


# ── module-level singleton + wrappers ────────────────────────────

_global_instance: BybitWsPrivate | None = None
_global_instance_lock = threading.Lock()


def init(api_key: str, api_secret: str) -> BybitWsPrivate:
    global _global_instance
    with _global_instance_lock:
        if _global_instance is None:
            _global_instance = BybitWsPrivate.get(api_key, api_secret)
        return _global_instance


def get_instance() -> BybitWsPrivate | None:
    return _global_instance


def get_position_cached(symbol: str, position_idx: int = 1):
    inst = _global_instance
    if inst is None:
        return None
    return inst.get_position_cached(symbol, position_idx)


def wait_for_position(symbol: str, position_idx: int = 1,
                      timeout: float = 3.0):
    inst = _global_instance
    if inst is None:
        return None
    return inst.wait_for_position(symbol, position_idx, timeout)


def wait_for_position_trailing(symbol: str, position_idx: int = 1,
                               timeout: float = 2.0) -> bool:
    inst = _global_instance
    if inst is None:
        return False
    return inst.wait_for_position_trailing(symbol, position_idx, timeout)


def is_ready(wait_sec: float = 0.0) -> bool:
    inst = _global_instance
    if inst is None:
        return False
    return inst.is_ready(wait_sec=wait_sec)
