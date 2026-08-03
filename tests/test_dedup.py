import json
import time

import pytest

import parsers.parser_listing as pl


@pytest.fixture(autouse=True)
def clean_state(tmp_path, monkeypatch):
    monkeypatch.setattr(pl, "_FIRED_FILE", tmp_path / "listing_fired.json")
    monkeypatch.setattr(pl, "_GLOBAL_FIRED_TTL", 100.0)
    with pl._fired_lock:
        pl._recent_signals.clear()
        pl._first_claim_ts.clear()
        pl._global_fired.clear()
        for bucket in pl._per_exchange_fired.values():
            bucket.clear()


def test_first_source_claims_and_second_source_is_blocked():
    assert pl._try_claim("AAA", "TG:first") is True
    assert pl._try_claim("AAA", "TOA-WS") is False


def test_expired_l1_claim_no_longer_blocks():
    with pl._fired_lock:
        pl._recent_signals[("AAA", "old")] = time.monotonic() - 1
    assert pl._try_claim("AAA", "new") is True


def test_recent_global_l2_blocks_all_sources(monkeypatch):
    now = 1_000.0
    monkeypatch.setattr(pl.time, "time", lambda: now)
    pl._global_fired["AAA"] = now - 10
    assert pl._try_claim("AAA", "TG:any") is False


def test_expired_global_l2_is_released(monkeypatch):
    now = 1_000.0
    monkeypatch.setattr(pl.time, "time", lambda: now)
    pl._global_fired["AAA"] = now - 101
    assert pl._try_claim("AAA", "TG:any") is True
    assert "AAA" not in pl._global_fired


def test_per_exchange_l2_blocks_same_exchange_only():
    pl._per_exchange_fired["UPBIT"].add("AAA")
    assert pl._try_claim("AAA", "UPBIT") is False
    assert pl._try_claim("AAA", "BITHUMB") is True


def test_persistence_roundtrip_preserves_runtime_schema():
    pl._global_fired["AAA"] = 123.0
    pl._per_exchange_fired["UPBIT"].add("BBB")
    pl._persist_fired_state()

    raw = json.loads(pl._FIRED_FILE.read_text())
    assert raw["global"] == {"AAA": 123.0}
    assert raw["per_exchange"]["UPBIT"] == ["BBB"]

    pl._global_fired.clear()
    pl._per_exchange_fired["UPBIT"].clear()
    pl._load_fired_state()
    assert pl._global_fired == {"AAA": 123.0}
    assert pl._per_exchange_fired["UPBIT"] == {"BBB"}


def test_corrupt_persistence_does_not_crash():
    pl._FIRED_FILE.write_text("not-json")
    pl._load_fired_state()
    assert pl._global_fired == {}
