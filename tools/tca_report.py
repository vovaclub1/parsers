from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
from pathlib import Path

from research.tca import implementation_shortfall_bps


def build_report(db_path: str | Path) -> dict:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    fills = con.execute("SELECT * FROM fills ORDER BY ts_ns").fetchall()
    snapshots = {
        r["client_order_id"]: r
        for r in con.execute("SELECT * FROM market_snapshots WHERE stage IN ('arrival','send') ORDER BY ts_ns").fetchall()
    }
    costs = []
    fees = 0.0
    covered = 0
    for fill in fills:
        fees += float(fill["fee"] or 0)
        snap = snapshots.get(fill["client_order_id"])
        if not snap:
            continue
        covered += 1
        side = "buy" if str(fill["side"]).lower() == "buy" else "sell"
        costs.append(implementation_shortfall_bps(
            side, float(snap["mid"]), float(fill["price"]), fee_bps=0,
        ))
    return {
        "fills": len(fills),
        "fills_with_bbo": covered,
        "coverage_pct": round(100 * covered / len(fills), 2) if fills else 0.0,
        "fees_total": fees,
        "implementation_shortfall_bps_mean": statistics.mean(costs) if costs else None,
        "implementation_shortfall_bps_median": statistics.median(costs) if costs else None,
        "implementation_shortfall_bps_p95": sorted(costs)[max(0, int(len(costs) * .95) - 1)] if costs else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("db")
    args = parser.parse_args()
    print(json.dumps(build_report(args.db), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
