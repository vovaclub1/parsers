from __future__ import annotations


DEFAULT_THRESHOLDS = {
    "orders": 60,
    "bbo_coverage_pct": 90.0,
    "complete_fill_pct": 90.0,
    "edge_bps_median": 0.0,
    "bootstrap_positive_pct": 95.0,
}


def assess_strategy_readiness(metrics: dict, thresholds: dict | None = None) -> dict:
    """Data gate only; never authorizes an automatic live strategy change."""
    limits = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    if float(metrics.get("edge_bps_median", 0)) <= limits["edge_bps_median"]:
        return {
            "decision": "NO_TRADE",
            "live_change_allowed": False,
            "failed": ["edge_bps_median"],
        }
    failed = [
        name for name in (
            "orders", "bbo_coverage_pct", "complete_fill_pct",
            "bootstrap_positive_pct",
        )
        if float(metrics.get(name, 0)) < float(limits[name])
    ]
    return {
        "decision": "NO_DECISION" if failed else "PAPER_CANDIDATE",
        "live_change_allowed": False,
        "failed": failed,
        "thresholds": limits,
    }
