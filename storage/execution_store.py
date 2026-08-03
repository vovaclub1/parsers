from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


_SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    client_order_id TEXT PRIMARY KEY,
    signal_id TEXT,
    venue TEXT NOT NULL DEFAULT '',
    symbol TEXT NOT NULL DEFAULT '',
    side TEXT NOT NULL DEFAULT '',
    requested_qty REAL NOT NULL DEFAULT 0,
    route TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'INTENT',
    exchange_order_id TEXT NOT NULL DEFAULT '',
    avg_price REAL NOT NULL DEFAULT 0,
    cum_exec_qty REAL NOT NULL DEFAULT 0,
    created_ts_ns INTEGER NOT NULL,
    updated_ts_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS order_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT NOT NULL,
    status TEXT NOT NULL,
    exchange_order_id TEXT NOT NULL DEFAULT '',
    avg_price REAL NOT NULL DEFAULT 0,
    cum_exec_qty REAL NOT NULL DEFAULT 0,
    ts_ns INTEGER NOT NULL,
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_order_events_link_ts
    ON order_events(client_order_id, ts_ns);

CREATE TABLE IF NOT EXISTS fills (
    exec_id TEXT PRIMARY KEY,
    client_order_id TEXT NOT NULL DEFAULT '',
    exchange_order_id TEXT NOT NULL DEFAULT '',
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    qty REAL NOT NULL,
    fee REAL NOT NULL DEFAULT 0,
    fee_asset TEXT NOT NULL DEFAULT '',
    ts_ns INTEGER NOT NULL,
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_fills_link_ts
    ON fills(client_order_id, ts_ns);

CREATE TABLE IF NOT EXISTS positions (
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    position_idx INTEGER NOT NULL,
    side TEXT NOT NULL DEFAULT '',
    size REAL NOT NULL DEFAULT 0,
    avg_price REAL NOT NULL DEFAULT 0,
    trailing_stop REAL NOT NULL DEFAULT 0,
    ts_ns INTEGER NOT NULL,
    PRIMARY KEY(venue, symbol, position_idx)
);

CREATE TABLE IF NOT EXISTS position_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    position_idx INTEGER NOT NULL,
    side TEXT NOT NULL DEFAULT '',
    size REAL NOT NULL DEFAULT 0,
    avg_price REAL NOT NULL DEFAULT 0,
    trailing_stop REAL NOT NULL DEFAULT 0,
    ts_ns INTEGER NOT NULL,
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_position_events_key_ts
    ON position_events(venue, symbol, position_idx, ts_ns);

CREATE TABLE IF NOT EXISTS market_snapshots (
    client_order_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    signal_id TEXT NOT NULL DEFAULT '',
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    bid REAL NOT NULL,
    ask REAL NOT NULL,
    bid_qty REAL NOT NULL DEFAULT 0,
    ask_qty REAL NOT NULL DEFAULT 0,
    mid REAL NOT NULL,
    spread_bps REAL NOT NULL,
    depth_bids_json TEXT NOT NULL DEFAULT '[]',
    depth_asks_json TEXT NOT NULL DEFAULT '[]',
    ts_ns INTEGER NOT NULL,
    PRIMARY KEY(client_order_id, stage)
);

CREATE TABLE IF NOT EXISTS market_snapshot_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    signal_id TEXT NOT NULL DEFAULT '',
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    bid REAL NOT NULL,
    ask REAL NOT NULL,
    bid_qty REAL NOT NULL DEFAULT 0,
    ask_qty REAL NOT NULL DEFAULT 0,
    mid REAL NOT NULL,
    spread_bps REAL NOT NULL,
    depth_bids_json TEXT NOT NULL DEFAULT '[]',
    depth_asks_json TEXT NOT NULL DEFAULT '[]',
    ts_ns INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_market_snapshot_events_link_stage
    ON market_snapshot_events(client_order_id, stage, ts_ns);
"""


def _json(raw: Any) -> str:
    try:
        return json.dumps(raw or {}, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return json.dumps({"unserializable": repr(raw)}, separators=(",", ":"))


class ExecutionStore:
    """Thread-safe SQLite WAL store for order, fill and position truth."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.connection = sqlite3.connect(
            str(self.path), timeout=10.0, check_same_thread=False,
        )
        self.connection.row_factory = sqlite3.Row
        with self._lock:
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=NORMAL")
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.connection.executescript(_SCHEMA)
            self.connection.commit()

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    def record_intent(
        self, *, signal_id: str, client_order_id: str, venue: str,
        symbol: str, side: str, requested_qty: float, route: str,
        ts_ns: int | None = None,
    ) -> None:
        ts = int(ts_ns if ts_ns is not None else time.time_ns())
        with self._lock, self.connection:
            self.connection.execute(
                """INSERT INTO orders(
                       client_order_id,signal_id,venue,symbol,side,requested_qty,
                       route,status,created_ts_ns,updated_ts_ns
                   ) VALUES(?,?,?,?,?,?,?,'INTENT',?,?)
                   ON CONFLICT(client_order_id) DO UPDATE SET
                       signal_id=excluded.signal_id, venue=excluded.venue,
                       symbol=excluded.symbol, side=excluded.side,
                       requested_qty=excluded.requested_qty, route=excluded.route,
                       updated_ts_ns=excluded.updated_ts_ns""",
                (client_order_id, signal_id, venue, symbol, side,
                 float(requested_qty), route, ts, ts),
            )
            self._insert_order_event(
                client_order_id, "INTENT", "", 0, 0, ts,
                {"signal_id": signal_id, "route": route},
            )

    def _insert_order_event(self, link: str, status: str, order_id: str,
                            avg: float, qty: float, ts: int, raw: Any) -> None:
        self.connection.execute(
            """INSERT INTO order_events(
                   client_order_id,status,exchange_order_id,avg_price,
                   cum_exec_qty,ts_ns,raw_json) VALUES(?,?,?,?,?,?,?)""",
            (link, status, order_id or "", float(avg or 0),
             float(qty or 0), int(ts), _json(raw)),
        )

    def record_order_event(
        self, *, client_order_id: str, status: str,
        exchange_order_id: str = "", avg_price: float = 0,
        cum_exec_qty: float = 0, ts_ns: int | None = None,
        raw: Any = None,
    ) -> None:
        ts = int(ts_ns if ts_ns is not None else time.time_ns())
        with self._lock, self.connection:
            self.connection.execute(
                """INSERT INTO orders(
                       client_order_id,status,exchange_order_id,avg_price,
                       cum_exec_qty,created_ts_ns,updated_ts_ns
                   ) VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(client_order_id) DO UPDATE SET
                       status=excluded.status,
                       exchange_order_id=CASE WHEN excluded.exchange_order_id!=''
                           THEN excluded.exchange_order_id ELSE orders.exchange_order_id END,
                       avg_price=excluded.avg_price,
                       cum_exec_qty=excluded.cum_exec_qty,
                       updated_ts_ns=excluded.updated_ts_ns""",
                (client_order_id, status, exchange_order_id or "",
                 float(avg_price or 0), float(cum_exec_qty or 0), ts, ts),
            )
            self._insert_order_event(
                client_order_id, status, exchange_order_id,
                avg_price, cum_exec_qty, ts, raw,
            )

    def record_fill(
        self, *, exec_id: str, client_order_id: str,
        exchange_order_id: str, symbol: str, side: str, price: float,
        qty: float, fee: float = 0, fee_asset: str = "",
        ts_ns: int | None = None, raw: Any = None,
    ) -> bool:
        ts = int(ts_ns if ts_ns is not None else time.time_ns())
        with self._lock, self.connection:
            cur = self.connection.execute(
                """INSERT OR IGNORE INTO fills(
                       exec_id,client_order_id,exchange_order_id,symbol,side,
                       price,qty,fee,fee_asset,ts_ns,raw_json)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (exec_id, client_order_id or "", exchange_order_id or "",
                 symbol, side, float(price), float(qty), float(fee or 0),
                 fee_asset or "", ts, _json(raw)),
            )
            return cur.rowcount == 1

    def record_position(
        self, *, venue: str, symbol: str, position_idx: int, side: str,
        size: float, avg_price: float, trailing_stop: float,
        ts_ns: int | None = None, raw: Any = None,
    ) -> None:
        ts = int(ts_ns if ts_ns is not None else time.time_ns())
        values = (venue, symbol, int(position_idx), side or "", float(size or 0),
                  float(avg_price or 0), float(trailing_stop or 0), ts)
        with self._lock, self.connection:
            self.connection.execute(
                """INSERT INTO positions(
                       venue,symbol,position_idx,side,size,avg_price,
                       trailing_stop,ts_ns) VALUES(?,?,?,?,?,?,?,?)
                   ON CONFLICT(venue,symbol,position_idx) DO UPDATE SET
                       side=excluded.side,size=excluded.size,
                       avg_price=excluded.avg_price,
                       trailing_stop=excluded.trailing_stop,
                       ts_ns=excluded.ts_ns""",
                values,
            )
            self.connection.execute(
                """INSERT INTO position_events(
                       venue,symbol,position_idx,side,size,avg_price,
                       trailing_stop,ts_ns,raw_json) VALUES(?,?,?,?,?,?,?,?,?)""",
                values + (_json(raw),),
            )

    def record_market_snapshot(
        self, *, signal_id: str, client_order_id: str, venue: str,
        symbol: str, stage: str, bid: float, ask: float,
        bid_qty: float = 0, ask_qty: float = 0,
        depth_bids=None, depth_asks=None, ts_ns: int | None = None,
    ) -> None:
        from research.tca import bbo_metrics
        metrics = bbo_metrics(bid, ask)
        ts = int(ts_ns if ts_ns is not None else time.time_ns())
        values = (
            client_order_id, stage, signal_id or "", venue, symbol,
            float(bid), float(ask), float(bid_qty or 0), float(ask_qty or 0),
            metrics["mid"], metrics["spread_bps"],
            _json(depth_bids or []), _json(depth_asks or []), ts,
        )
        with self._lock, self.connection:
            self.connection.execute(
                """INSERT INTO market_snapshots(
                       client_order_id,stage,signal_id,venue,symbol,bid,ask,
                       bid_qty,ask_qty,mid,spread_bps,depth_bids_json,
                       depth_asks_json,ts_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(client_order_id,stage) DO UPDATE SET
                       signal_id=excluded.signal_id,venue=excluded.venue,
                       symbol=excluded.symbol,bid=excluded.bid,ask=excluded.ask,
                       bid_qty=excluded.bid_qty,ask_qty=excluded.ask_qty,
                       mid=excluded.mid,spread_bps=excluded.spread_bps,
                       depth_bids_json=excluded.depth_bids_json,
                       depth_asks_json=excluded.depth_asks_json,
                       ts_ns=excluded.ts_ns""",
                values,
            )
            self.connection.execute(
                """INSERT INTO market_snapshot_events(
                       client_order_id,stage,signal_id,venue,symbol,bid,ask,
                       bid_qty,ask_qty,mid,spread_bps,depth_bids_json,
                       depth_asks_json,ts_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                values,
            )

    def get_market_snapshot(self, client_order_id: str, stage: str) -> dict | None:
        with self._lock:
            row = self.connection.execute(
                """SELECT * FROM market_snapshots
                   WHERE client_order_id=? AND stage=?""",
                (client_order_id, stage),
            ).fetchone()
            return dict(row) if row else None

    def count_market_snapshot_events(self, client_order_id: str, stage: str) -> int:
        with self._lock:
            return int(self.connection.execute(
                """SELECT COUNT(*) FROM market_snapshot_events
                   WHERE client_order_id=? AND stage=?""",
                (client_order_id, stage),
            ).fetchone()[0])

    def get_order(self, client_order_id: str) -> dict | None:
        with self._lock:
            row = self.connection.execute(
                "SELECT * FROM orders WHERE client_order_id=?",
                (client_order_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_position(self, venue: str, symbol: str,
                     position_idx: int) -> dict | None:
        with self._lock:
            row = self.connection.execute(
                """SELECT * FROM positions
                   WHERE venue=? AND symbol=? AND position_idx=?""",
                (venue, symbol, int(position_idx)),
            ).fetchone()
            return dict(row) if row else None

    def count_order_events(self, client_order_id: str) -> int:
        with self._lock:
            return int(self.connection.execute(
                "SELECT COUNT(*) FROM order_events WHERE client_order_id=?",
                (client_order_id,),
            ).fetchone()[0])

    def count_fills(self, client_order_id: str) -> int:
        with self._lock:
            return int(self.connection.execute(
                "SELECT COUNT(*) FROM fills WHERE client_order_id=?",
                (client_order_id,),
            ).fetchone()[0])

    def count_position_events(self, venue: str, symbol: str,
                              position_idx: int) -> int:
        with self._lock:
            return int(self.connection.execute(
                """SELECT COUNT(*) FROM position_events
                   WHERE venue=? AND symbol=? AND position_idx=?""",
                (venue, symbol, int(position_idx)),
            ).fetchone()[0])
