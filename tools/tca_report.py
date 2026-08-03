from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path

from research.tca import implementation_shortfall_bps


def _summary(values):
    return {
        "mean": statistics.mean(values) if values else None,
        "median": statistics.median(values) if values else None,
        "p95": sorted(values)[max(0, int(len(values) * .95) - 1)] if values else None,
    }


def build_report(db_path: str | Path) -> dict:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    fills = con.execute("SELECT * FROM fills ORDER BY ts_ns").fetchall()
    orders = {
        r["client_order_id"]: r
        for r in con.execute("SELECT * FROM orders").fetchall()
    }
    snapshots = {
        r["client_order_id"]: r
        for r in con.execute(
            "SELECT * FROM market_snapshots WHERE stage='send' ORDER BY ts_ns"
        ).fetchall()
    }
    grouped = defaultdict(list)
    for fill in fills:
        grouped[fill["client_order_id"]].append(fill)

    per_order = []
    for link, parts in grouped.items():
        total_qty = sum(float(x["qty"] or 0) for x in parts)
        if total_qty <= 0:
            continue
        vwap = sum(float(x["price"]) * float(x["qty"]) for x in parts) / total_qty
        fee = sum(float(x["fee"] or 0) for x in parts)
        first_fill_ns = min(int(x["ts_ns"]) for x in parts)
        order = orders.get(link)
        snap = snapshots.get(link)
        signal_id = str(order["signal_id"] if order else "")
        event_type = signal_id.split(":", 1)[0] if ":" in signal_id else "unknown"
        row = {
            "link": link, "event_type": event_type,
            "fill_vwap": vwap, "qty": total_qty, "fee": fee,
            "first_fill_ns": first_fill_ns, "has_bbo": snap is not None,
            "shortfall_bps": None, "latency_ms": None, "spread_bps": None,
        }
        if snap is not None:
            side = "buy" if str(parts[0]["side"]).lower() == "buy" else "sell"
            row["shortfall_bps"] = implementation_shortfall_bps(
                side, float(snap["mid"]), vwap, fee_bps=0,
            )
            row["spread_bps"] = float(snap["spread_bps"])
            row["latency_ms"] = (first_fill_ns - int(snap["ts_ns"])) / 1_000_000
        per_order.append(row)

    covered = [x for x in per_order if x["has_bbo"]]
    costs = [x["shortfall_bps"] for x in covered]
    cohorts = {}
    for event_type in sorted({x["event_type"] for x in per_order}):
        rows = [x for x in per_order if x["event_type"] == event_type]
        cov = [x for x in rows if x["has_bbo"]]
        cohorts[event_type] = {
            "orders": len(rows),
            "orders_with_send_bbo": len(cov),
            "coverage_pct": round(100 * len(cov) / len(rows), 2) if rows else 0,
            "fill_vwap_mean": statistics.mean(x["fill_vwap"] for x in rows),
            "fees_total": sum(x["fee"] for x in rows),
            "spread_bps_median": statistics.median(x["spread_bps"] for x in cov) if cov else None,
            "implementation_shortfall_bps_median": statistics.median(x["shortfall_bps"] for x in cov) if cov else None,
            "first_fill_latency_ms_median": statistics.median(x["latency_ms"] for x in cov) if cov else None,
        }

    cost_summary = _summary(costs)
    return {
        "fills": len(fills),
        "orders_filled": len(per_order),
        "orders_with_send_bbo": len(covered),
        "fills_with_bbo": sum(len(grouped[x["link"]]) for x in covered),
        "coverage_pct": round(100 * len(covered) / len(per_order), 2) if per_order else 0.0,
        "fees_total": sum(x["fee"] for x in per_order),
        "implementation_shortfall_bps_mean": cost_summary["mean"],
        "implementation_shortfall_bps_median": cost_summary["median"],
        "implementation_shortfall_bps_p95": cost_summary["p95"],
        "cohorts": cohorts,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("db")
    args = parser.parse_args()
    print(json.dumps(build_report(args.db), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
