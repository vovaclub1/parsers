import sqlite3
from concurrent.futures import ThreadPoolExecutor

from storage.execution_store import ExecutionStore


def test_order_intent_and_lifecycle_are_durable(tmp_path):
    db = tmp_path / "execution.sqlite3"
    store = ExecutionStore(db)
    store.record_intent(
        signal_id="sig-1",
        client_order_id="link-1",
        venue="bybit",
        symbol="AAAUSDT",
        side="Sell",
        requested_qty=12.0,
        route="sync_ws",
        ts_ns=100,
    )
    store.record_order_event(
        client_order_id="link-1",
        status="Filled",
        exchange_order_id="order-1",
        avg_price=2.5,
        cum_exec_qty=12.0,
        ts_ns=200,
        raw={"orderStatus": "Filled"},
    )
    store.close()

    reopened = ExecutionStore(db)
    order = reopened.get_order("link-1")
    assert order["signal_id"] == "sig-1"
    assert order["status"] == "Filled"
    assert order["avg_price"] == 2.5
    assert order["cum_exec_qty"] == 12.0
    assert reopened.count_order_events("link-1") == 2


def test_duplicate_execution_id_is_idempotent(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    kwargs = dict(
        exec_id="exec-1",
        client_order_id="link-1",
        exchange_order_id="order-1",
        symbol="AAAUSDT",
        side="Sell",
        price=2.5,
        qty=3.0,
        fee=0.01,
        fee_asset="USDT",
        ts_ns=300,
        raw={"execId": "exec-1"},
    )
    assert store.record_fill(**kwargs) is True
    assert store.record_fill(**kwargs) is False
    assert store.count_fills("link-1") == 1


def test_position_snapshot_keeps_latest_state(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    store.record_position(
        venue="bybit", symbol="AAAUSDT", position_idx=2,
        side="Sell", size=10, avg_price=2.5, trailing_stop=0.02,
        ts_ns=100, raw={},
    )
    store.record_position(
        venue="bybit", symbol="AAAUSDT", position_idx=2,
        side="Sell", size=0, avg_price=0, trailing_stop=0,
        ts_ns=200, raw={},
    )
    pos = store.get_position("bybit", "AAAUSDT", 2)
    assert pos["size"] == 0
    assert pos["ts_ns"] == 200
    assert store.count_position_events("bybit", "AAAUSDT", 2) == 2


def test_concurrent_writes_do_not_lose_events(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")

    def write(i):
        store.record_order_event(
            client_order_id="link-1",
            status=f"state-{i}",
            exchange_order_id="order-1",
            avg_price=0,
            cum_exec_qty=0,
            ts_ns=i,
            raw={"i": i},
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(100)))

    assert store.count_order_events("link-1") == 100


def test_database_uses_wal_and_foreign_keys(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    row = store.connection.execute("PRAGMA journal_mode").fetchone()
    assert row[0].lower() == "wal"
    fk = store.connection.execute("PRAGMA foreign_keys").fetchone()
    assert fk[0] == 1
    store.close()


def test_two_process_style_connections_share_wal_database(tmp_path):
    path = tmp_path / "execution.sqlite3"
    listing = ExecutionStore(path)
    delisting = ExecutionStore(path)
    listing.record_order_event(
        client_order_id="listing-1", status="Filled", ts_ns=1,
    )
    delisting.record_order_event(
        client_order_id="delist-1", status="Filled", ts_ns=2,
    )
    assert listing.get_order("delist-1")["status"] == "Filled"
    assert delisting.get_order("listing-1")["status"] == "Filled"
