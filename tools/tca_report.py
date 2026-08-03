from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path

from research.tca import implementation_shortfall_bps, sweep_book


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
    l2_snapshots = {
        r["client_order_id"]: r
        for r in con.execute(
            "SELECT * FROM market_snapshots WHERE stage='send_l2' ORDER BY ts_ns"
        ).fetchall()
    }
    grouped = defaultdict(list)
    for fill in fills:
        link = fill["client_order_id"]
        if link:
            key = f"link:{link}"
        elif fill["exchange_order_id"]:
            key = f"order:{fill['exchange_order_id']}"
        else:
            key = f"exec:{fill['exec_id']}"
        grouped[key].append(fill)

    per_order = []
    for execution_key, parts in grouped.items():
        link = parts[0]["client_order_id"] or ""
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
            "link": link, "execution_key": execution_key,
            "event_type": event_type,
            "fill_vwap": vwap, "qty": total_qty, "fee": fee,
            "first_fill_ns": first_fill_ns, "has_bbo": snap is not None,
            "shortfall_bps": None, "latency_ms": None, "spread_bps": None,
            "expected_l2_vwap": None, "l2_complete": False,
            "fill_vs_l2_slippage_bps": None,
        }
        if snap is not None:
            side = "buy" if str(parts[0]["side"]).lower() == "buy" else "sell"
            row["shortfall_bps"] = implementation_shortfall_bps(
                side, float(snap["mid"]), vwap, fee_bps=0,
            )
            row["spread_bps"] = float(snap["spread_bps"])
            row["latency_ms"] = (first_fill_ns - int(snap["ts_ns"])) / 1_000_000
            l2 = l2_snapshots.get(link)
            if l2 is not None and order is not None:
                levels_json = l2["depth_asks_json"] if side == "buy" else l2["depth_bids_json"]
                levels = json.loads(levels_json or "[]")
                requested_notional = float(order["requested_qty"] or 0) * float(snap["mid"])
                if requested_notional > 0:
                    sweep = sweep_book(levels, requested_notional, side)
                    row["expected_l2_vwap"] = sweep["vwap"]
                    row["l2_complete"] = sweep["complete"]
                    if sweep["vwap"] > 0:
                        row["fill_vs_l2_slippage_bps"] = implementation_shortfall_bps(
                            side, sweep["vwap"], vwap, fee_bps=0,
                        )
        per_order.append(row)

    covered = [x for x in per_order if x["has_bbo"]]
    costs = [x["shortfall_bps"] for x in covered]
    cohorts = {}
    for event_type in sorted({x["event_type"] for x in per_order}):
        rows = [x for x in per_order if x["event_type"] == event_type]
        cov = [x for x in rows if x["has_bbo"]]
        l2_rows = [x for x in rows if x["expected_l2_vwap"] is not None]
        cohorts[event_type] = {
            "orders": len(rows),
            "orders_with_send_bbo": len(cov),
            "coverage_pct": round(100 * len(cov) / len(rows), 2) if rows else 0,
            "fill_vwap_mean": statistics.mean(x["fill_vwap"] for x in rows),
            "fees_total": sum(x["fee"] for x in rows),
            "spread_bps_median": statistics.median(x["spread_bps"] for x in cov) if cov else None,
            "implementation_shortfall_bps_median": statistics.median(x["shortfall_bps"] for x in cov) if cov else None,
            "first_fill_latency_ms_median": statistics.median(x["latency_ms"] for x in cov) if cov else None,
            "expected_l2_vwap_mean": statistics.mean(x["expected_l2_vwap"] for x in l2_rows) if l2_rows else None,
            "l2_complete_pct": round(100 * sum(x["l2_complete"] for x in l2_rows) / len(l2_rows), 2) if l2_rows else 0.0,
            "fill_vs_l2_slippage_bps_median": statistics.median(
                x["fill_vs_l2_slippage_bps"] for x in l2_rows
                if x["fill_vs_l2_slippage_bps"] is not None
            ) if any(x["fill_vs_l2_slippage_bps"] is not None for x in l2_rows) else None,
        }

    cost_summary = _summary(costs)
    return {
        "fills": len(fills),
        "orders_filled": len(per_order),
        "orders_with_send_bbo": len(covered),
        "fills_with_bbo": sum(len(grouped[x["execution_key"]]) for x in covered),
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
