import json

from tools.strategy_cohort_report import build_cohort_report


def test_cohort_report_groups_source_and_returns_no_decision_without_execution(tmp_path):
    path = tmp_path / "strategy.jsonl"
    rows = [
        {
            "event_type": "listing", "src": "UPBIT-NOTICE", "side": "short",
            "direction_shadow": {
                "momentum": {"side": "long", "time=5m": -5},
                "contrarian": {"side": "short", "time=5m": 3},
            },
        }
        for _ in range(10)
    ]
    path.write_text("\n".join(json.dumps(x) for x in rows))
    report = build_cohort_report(path)
    cohort = report["listing|UPBIT-NOTICE|announcement"]
    assert cohort["observations"] == 10
    assert cohort["contrarian_minus_momentum_bps_median"] == 800
    assert cohort["readiness"]["decision"] == "NO_DECISION"
    assert cohort["readiness"]["live_change_allowed"] is False
