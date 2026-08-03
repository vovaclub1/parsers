"""Тесты живучести sync WS-клиента Bybit (детект мёртвого соединения).

Контекст бага: у sync-клиента websockets 12.x НЕТ автоматического keepalive
(ping_interval появился только в 13.0). Проверено по исходникам 12.0 —
в websockets/sync/connection.py нет ни keepalive-потока, ни ping_interval.

Без ping'ов обрыв не детектируется: reader висит в recv, _connected остаётся
выставленным, а первый send() в half-open TCP проходит успешно (ядро
буферизует). place_order_fast возвращает {"sent": True} — и REST-fallback
НЕ срабатывает. Ордер теряется молча.
"""
from __future__ import annotations

import inspect

import pytest

from api import bybit_sync_ws_trade as sw


# ── определение возможностей библиотеки ───────────────────────────

def test_keepalive_detection_matches_installed_library():
    """_KEEPALIVE_KW должен отражать реальную сигнатуру connect(),
    а не хардкод версии — иначе на 12.x получим TypeError, а на 13+
    зря откажемся от родного keepalive."""
    from websockets.sync.client import connect

    params = inspect.signature(connect).parameters
    supported = "ping_interval" in params and "ping_timeout" in params

    assert bool(sw._KEEPALIVE_KW) is supported
    if supported:
        assert sw._KEEPALIVE_KW["ping_interval"] == sw._WS_PING_INTERVAL
        assert sw._KEEPALIVE_KW["ping_timeout"] == sw._WS_PING_TIMEOUT


def test_manual_ping_is_inverse_of_library_keepalive():
    """Ровно один механизм keepalive: либо библиотечный, либо наш ручной."""
    assert sw._MANUAL_PING_NEEDED is (not sw._KEEPALIVE_KW)


def test_keepalive_detection_never_raises(monkeypatch):
    """Если websockets отсутствует/сломан — детект обязан вернуть {},
    а не уронить импорт модуля на боевом старте."""
    import builtins

    real_import = builtins.__import__

    def boom(name, *a, **kw):
        if name.startswith("websockets"):
            raise ImportError("no websockets")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", boom)
    assert sw._detect_keepalive_kwargs() == {}


def test_silent_timeout_limit_is_sane():
    """Порог тишины должен быть больше 0 и покрываться periodic warmup'ом."""
    assert sw._MAX_SILENT_TIMEOUTS >= 1
    silence_window = sw._MAX_SILENT_TIMEOUTS * sw._READER_RECV_TIMEOUT
    assert silence_window > sw._PERIODIC_WARMUP_INTERVAL


def test_warmup_fail_limit_is_sane():
    assert 1 <= sw._WARMUP_FAIL_LIMIT <= 5


# ── force_reconnect ───────────────────────────────────────────────

class _FakeWs:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def _make_instance() -> sw.BybitSyncWsTrade:
    """Инстанс без запуска фоновых потоков."""
    return sw.BybitSyncWsTrade.__new__(sw.BybitSyncWsTrade)


@pytest.fixture()
def inst():
    import threading

    o = _make_instance()
    o.api_key = "k"
    o.api_secret = "s"
    o._ws = None
    o._ws_lock = threading.Lock()
    o._send_lock = threading.Lock()
    o._connected = threading.Event()
    o._pending = {}
    o._pending_results = {}
    o._pending_lock = threading.Lock()
    o._stop = False
    o._mgr_thread = None
    return o


def test_force_reconnect_clears_connected_flag(inst):
    """Ключевое: hot-path обязан СРАЗУ перестать считать канал живым,
    иначе продолжит слать ордера в мёртвый сокет."""
    ws = _FakeWs()
    inst._ws = ws
    inst._connected.set()

    inst.force_reconnect()

    assert inst._connected.is_set() is False
    assert inst._ws is None
    assert ws.closed is True


def test_force_reconnect_does_not_stop_manager(inst):
    """Рвём только соединение — manager-loop должен поднять новое."""
    inst._ws = _FakeWs()
    inst._connected.set()

    inst.force_reconnect()

    assert inst._stop is False


def test_force_reconnect_survives_broken_close(inst):
    """Сокет уже мёртв и close() кидает — это нормальный сценарий."""
    class Exploding:
        def close(self):
            raise OSError("socket already dead")

    inst._ws = Exploding()
    inst._connected.set()

    inst.force_reconnect()  # не должно кинуть

    assert inst._connected.is_set() is False
    assert inst._ws is None


def test_force_reconnect_is_safe_without_connection(inst):
    inst._ws = None
    inst.force_reconnect()
    assert inst._ws is None


def test_place_order_fast_refuses_when_disconnected(inst):
    """После force_reconnect ордер обязан уйти на REST (None), а не
    делать вид, что отправлен."""
    inst._ws = _FakeWs()
    inst._connected.set()
    inst.force_reconnect()

    assert inst.place_order_fast({"symbol": "BTCUSDT"}) is None
