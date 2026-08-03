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
    con.close()

    trades = reconcile_trades(db)
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
        "reconciliation": {
            "trades": len(trades),
            "open": statuses["OPEN"],
            "partially_closed": statuses["PARTIALLY_CLOSED"],
            "closed": statuses["CLOSED"],
            "realized_exec_pnl": sum(x["realized_pnl"] for x in trades),
            "recorded_fees": sum(x["fees"] for x in trades),
            "pnl_after_recorded_fees": sum(x["net_pnl"] for x in trades),
        },
        "sources": {
            "first_wins": source_stats.get("first_wins", {}),
            "listing_health": health.get("components", {}),
            "delisting_health": delist_health.get("components", {}),
        },
    }


def render_text(report: dict) -> str:
    e, r, t = report["execution"], report["reconciliation"], report["tca"]
    return "\n".join([
        "📊 PRODUCTION REPORT",
        f"DB integrity: {report['database']['integrity']}",
        f"Orders/fills/snapshots: {e['orders']}/{e['fills']}/{e['market_snapshots']}",
        f"Trades closed/open/partial: {r['closed']}/{r['open']}/{r['partially_closed']}",
        f"ExecPnl/fees/after fees: {r['realized_exec_pnl']:.4f} / {r['recorded_fees']:.4f} / {r['pnl_after_recorded_fees']:.4f} USDT",
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
