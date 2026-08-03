from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path

from research.cohorts import classify_subtype
from research.strategy_gate import assess_strategy_readiness


def build_cohort_report(path: str | Path, horizon: str = "time=5m") -> dict:
    groups = defaultdict(list)
    for line in Path(path).read_text(errors="ignore").splitlines():
        try:
            row = json.loads(line)
            shadow = row.get("direction_shadow") or {}
            momentum = shadow.get("momentum") or {}
            contrarian = shadow.get("contrarian") or {}
            if horizon not in momentum or horizon not in contrarian:
                continue
            event_type = row.get("event_type", "unknown")
            source = row.get("src", "unknown")
            subtype = row.get("event_subtype") or classify_subtype(source)
            groups[(event_type, source, subtype)].append(
                (float(momentum[horizon]), float(contrarian[horizon]))
            )
        except (ValueError, TypeError, json.JSONDecodeError):
            continue

    out = {}
    for (event_type, source, subtype), values in sorted(groups.items()):
        deltas_bps = [(contra - momentum) * 100 for momentum, contra in values]
        metrics = {
            "orders": len(values),
            # Execution metrics intentionally absent until joined from SQLite.
            "bbo_coverage_pct": 0,
            "complete_fill_pct": 0,
            "edge_bps_median": statistics.median(deltas_bps),
            "bootstrap_positive_pct": 0,
        }
        out[f"{event_type}|{source}|{subtype}"] = {
            "event_type": event_type, "source": source, "subtype": subtype,
            "horizon": horizon, "observations": len(values),
            "momentum_pnl_pct_median": statistics.median(x[0] for x in values),
            "contrarian_pnl_pct_median": statistics.median(x[1] for x in values),
            "contrarian_minus_momentum_bps_median": statistics.median(deltas_bps),
            "readiness": assess_strategy_readiness(metrics),
        }
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("strategy_results")
    parser.add_argument("--horizon", default="time=5m")
    args = parser.parse_args()
    print(json.dumps(
        build_cohort_report(args.strategy_results, args.horizon),
        ensure_ascii=False, indent=2,
    ))


if __name__ == "__main__":
    main()
