from __future__ import annotations

import json
import sqlite3
from collections import defaultdict, deque
from pathlib import Path


def _raw_pnl(row) -> float:
    try:
        return float((json.loads(row["raw_json"] or "{}") or {}).get("execPnl", 0) or 0)
    except Exception:
        return 0.0


def reconcile_trades(db_path: str | Path) -> list[dict]:
    """FIFO match entry orders to later opposite-side fills per symbol."""
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    orders = con.execute(
        "SELECT * FROM orders WHERE signal_id!='' ORDER BY created_ts_ns"
    ).fetchall()
    fills = con.execute("SELECT * FROM fills ORDER BY ts_ns, exec_id").fetchall()
    by_link = defaultdict(list)
    for fill in fills:
        if fill["client_order_id"]:
            by_link[fill["client_order_id"]].append(fill)

    trades = []
    consumed_qty = defaultdict(float)
    for order in orders:
        entries = by_link.get(order["client_order_id"], [])
        if not entries:
            continue
        entry_qty = sum(float(x["qty"] or 0) for x in entries)
        entry_value = sum(float(x["qty"]) * float(x["price"]) for x in entries)
        entry_fee = sum(float(x["fee"] or 0) for x in entries)
        entry_ts = min(int(x["ts_ns"]) for x in entries)
        opposite = "Buy" if str(order["side"]).lower() == "sell" else "Sell"
        exits = [
            x for x in fills
            if x["symbol"] == order["symbol"]
            and str(x["side"]).lower() == opposite.lower()
            and int(x["ts_ns"]) >= entry_ts
            and not x["client_order_id"]
        ]
        remaining = entry_qty
        used = []
        for fill in exits:
            if remaining <= 1e-12:
                break
            available = max(0.0, float(fill["qty"] or 0) - consumed_qty[fill["exec_id"]])
            qty = min(remaining, available)
            if qty <= 0:
                continue
            used.append((fill, qty))
            consumed_qty[fill["exec_id"]] += qty
            remaining -= qty
        exit_qty = sum(q for _, q in used)
        exit_value = sum(float(x["price"]) * q for x, q in used)
        exit_fee = sum(float(x["fee"] or 0) * (q / float(x["qty"])) for x, q in used)
        realized = sum(_raw_pnl(x) * (q / float(x["qty"])) for x, q in used)
        status = "OPEN" if exit_qty == 0 else "CLOSED" if remaining <= 1e-12 else "PARTIALLY_CLOSED"
        trades.append({
            "signal_id": order["signal_id"],
            "client_order_id": order["client_order_id"],
            "symbol": order["symbol"], "side": order["side"],
            "status": status, "entry_qty": entry_qty,
            "exit_qty": exit_qty, "open_qty": max(0.0, remaining),
            "entry_vwap": entry_value / entry_qty if entry_qty else 0,
            "exit_vwap": exit_value / exit_qty if exit_qty else 0,
            "realized_pnl": realized, "fees": entry_fee + exit_fee,
            "net_pnl": realized - entry_fee - exit_fee,
            "entry_ts_ns": entry_ts,
            "exit_ts_ns": max((int(x["ts_ns"]) for x, _ in used), default=0),
        })
    return trades
