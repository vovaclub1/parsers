from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from research.reconciliation import reconcile_trades
from tools.tca_report import build_report as build_tca_report


def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def build_production_report(state_dir: str | Path) -> dict:
    state = Path(state_dir)
    db = state / "execution.sqlite3"
    con = sqlite3.connect(str(db), timeout=10)
    con.row_factory = sqlite3.Row
    integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
    counts = {}
    for table in ("orders", "order_events", "fills", "positions",
                  "position_events", "market_snapshots"):
        try:
            counts[table] = int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        except sqlite3.Error:
            counts[table] = 0
    unmatched_fills = int(con.execute(
        "SELECT COUNT(*) FROM fills WHERE client_order_id=''"
    ).fetchone()[0])
    authoritative = [dict(r) for r in con.execute(
        "SELECT * FROM authoritative_pnl ORDER BY closed_ts_ns"
    ).fetchall()]
    con.close()

    by_venue = {}
    for venue in ("bybit", "gate"):
        rows = [r for r in authoritative if r["venue"] == venue]
        by_venue[venue] = {
            "closed": len(rows),
            "net_pnl": sum(float(r["net_pnl"] or 0) for r in rows),
            "fees": sum(float(r["fees"] or 0) for r in rows),
            "funding": sum(float(r["funding"] or 0) for r in rows),
            "unlinked": sum(not r["signal_id"] for r in rows),
        }
    trades = reconcile_trades(db)  # execution-derived diagnostics only
    statuses = {name: sum(x["status"] == name for x in trades)
                for name in ("OPEN", "PARTIALLY_CLOSED", "CLOSED")}
    source_stats = _read_json(state / "source_stats.json")
    health = _read_json(state / "health_stats.json")
    delist_health = _read_json(state / "delist_health_stats.json")
    return {
        "database": {"path": str(db), "integrity": integrity, "wal": True},
        "execution": {
            **counts,
            "unmatched_exit_fills": unmatched_fills,
        },
        "tca": build_tca_report(db),
        "authoritative_pnl": {
            "source": "exchange_closed_pnl",
            "closed": len(authoritative),
            "net_pnl": sum(float(r["net_pnl"] or 0) for r in authoritative),
            "by_venue": by_venue,
            "unlinked": sum(not r["signal_id"] for r in authoritative),
        },
        "execution_diagnostics": {
            "trades": len(trades),
            "open": statuses["OPEN"],
            "partially_closed": statuses["PARTIALLY_CLOSED"],
            "closed": statuses["CLOSED"],
            "sum_exec_pnl": sum(x["realized_pnl"] for x in trades),
            "recorded_fees": sum(x["fees"] for x in trades),
            "exec_pnl_minus_fees": sum(x["net_pnl"] for x in trades),
        },
        "sources": {
            "first_wins": source_stats.get("first_wins", {}),
            "listing_health": health.get("components", {}),
            "delisting_health": delist_health.get("components", {}),
        },
    }


def render_text(report: dict) -> str:
    e = report["execution"]
    a = report["authoritative_pnl"]
    d = report["execution_diagnostics"]
    t = report["tca"]
    bybit = a["by_venue"]["bybit"]
    gate = a["by_venue"]["gate"]
    authoritative_line = (
        f"Authoritative net PnL: {a['net_pnl']:.4f} USDT "
        f"(Bybit {bybit['net_pnl']:.4f}, Gate {gate['net_pnl']:.4f}; "
        f"closed {a['closed']}, unlinked {a['unlinked']})"
        if a["closed"] else
        "Authoritative net PnL: нет новых exchange closed-PnL records"
    )
    return "\n".join([
        "📊 PRODUCTION REPORT",
        f"DB integrity: {report['database']['integrity']}",
        f"Orders/fills/snapshots: {e['orders']}/{e['fills']}/{e['market_snapshots']}",
        authoritative_line,
        f"Execution diagnostics only — closed/open/partial: {d['closed']}/{d['open']}/{d['partially_closed']}",
        f"Diagnostic execPnl/fees/difference: {d['sum_exec_pnl']:.4f} / {d['recorded_fees']:.4f} / {d['exec_pnl_minus_fees']:.4f} USDT",
        f"BBO coverage: {t['coverage_pct']:.1f}%",
        f"Unlinked exit fills: {e['unmatched_exit_fills']}",
    ])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("state_dir")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    report = build_production_report(args.state_dir)
    print(json.dumps(report, ensure_ascii=False, indent=2) if args.json else render_text(report))


if __name__ == "__main__":
    main()
