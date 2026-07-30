"""Тесты персистентности L2-дедупа: формат файла, TTL, миграция.

L2 хранит «эту монету уже торговали». Раньше он был вечным множеством,
теперь — словарь coin -> timestamp с истечением через _L2_TTL.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

import parsers.parser_listing as pl


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path, monkeypatch):
    monkeypatch.setattr(pl, "_FIRED_FILE", tmp_path / "listing_fired.json")
    with pl._fired_lock:
        pl._global_fired.clear()
        for b in pl._per_exchange_fired.values():
            b.clear()
        pl._recent_signals.clear()
        pl._coin_claim_expiry.clear()
    yield


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_roundtrip_preserves_timestamps():
    now = time.time()
    with pl._fired_lock:
        pl._global_fired["PRL"] = now
        pl._per_exchange_fired["UPBIT"]["GENIUS"] = now
    pl._persist_fired_state()

    with pl._fired_lock:
        pl._global_fired.clear()
        pl._per_exchange_fired["UPBIT"].clear()
    pl._load_fired_state()

    assert pl._global_fired["PRL"] == pytest.approx(now, abs=1)
    assert pl._per_exchange_fired["UPBIT"]["GENIUS"] == pytest.approx(now, abs=1)


def test_persisted_file_is_valid_json_with_timestamps():
    with pl._fired_lock:
        pl._global_fired["PRL"] = time.time()
    pl._persist_fired_state()

    data = json.loads(pl._FIRED_FILE.read_text(encoding="utf-8"))
    assert isinstance(data["global"], dict)
    assert isinstance(data["global"]["PRL"], (int, float))


def test_migration_from_legacy_list_format():
    """Старый формат — список без времени. Апгрейд не должен разом
    разблокировать всю историю, поэтому такие записи считаем свежими."""
    _write(pl._FIRED_FILE, {
        "global": ["OLDA", "OLDB"],
        "per_exchange": {"UPBIT": ["OLDC"]},
    })
    pl._load_fired_state()

    assert set(pl._global_fired) == {"OLDA", "OLDB"}
    assert "OLDC" in pl._per_exchange_fired["UPBIT"]
    # Свежие -> всё ещё блокируют.
    assert pl._try_claim("OLDA", "TG:1") is False


def test_expired_entries_are_not_loaded():
    """Протухшие записи незачем тащить в память при старте."""
    stale = time.time() - (pl._L2_TTL + 100)
    fresh = time.time()
    _write(pl._FIRED_FILE, {
        "global": {"STALE": stale, "FRESH": fresh},
        "per_exchange": {},
    })
    pl._load_fired_state()

    assert "STALE" not in pl._global_fired
    assert "FRESH" in pl._global_fired


def test_load_survives_corrupted_file():
    pl._FIRED_FILE.write_text("{ это не json", encoding="utf-8")
    pl._load_fired_state()          # не должно кинуть
    assert pl._global_fired == {}


def test_load_survives_missing_file():
    pl._load_fired_state()
    assert pl._global_fired == {}


def test_load_ignores_malformed_timestamps():
    _write(pl._FIRED_FILE, {"global": {"BAD": "не число", "OK": time.time()}})
    pl._load_fired_state()
    assert "BAD" not in pl._global_fired
    assert "OK" in pl._global_fired


def test_mark_opened_refreshes_window():
    """Повторное открытие сдвигает окно блокировки, а не игнорируется."""
    old = time.time() - 1000
    with pl._fired_lock:
        pl._global_fired["PRL"] = old
    pl._mark_opened("PRL", "TG:1")
    assert pl._global_fired["PRL"] > old
