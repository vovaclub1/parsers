from research.strategy_gate import assess_strategy_readiness


def test_small_sample_returns_no_decision():
    result = assess_strategy_readiness({
        "orders": 12, "bbo_coverage_pct": 100,
        "complete_fill_pct": 100, "edge_bps_median": 50,
        "bootstrap_positive_pct": 99,
    })
    assert result["decision"] == "NO_DECISION"
    assert "orders" in result["failed"]


def test_low_execution_coverage_returns_no_decision():
    result = assess_strategy_readiness({
        "orders": 100, "bbo_coverage_pct": 50,
        "complete_fill_pct": 95, "edge_bps_median": 50,
        "bootstrap_positive_pct": 99,
    })
    assert result["decision"] == "NO_DECISION"
    assert "bbo_coverage_pct" in result["failed"]


def test_negative_edge_never_promotes_strategy():
    result = assess_strategy_readiness({
        "orders": 100, "bbo_coverage_pct": 95,
        "complete_fill_pct": 95, "edge_bps_median": -1,
        "bootstrap_positive_pct": 99,
    })
    assert result["decision"] == "NO_TRADE"


def test_all_thresholds_allow_paper_candidate_only():
    result = assess_strategy_readiness({
        "orders": 100, "bbo_coverage_pct": 95,
        "complete_fill_pct": 95, "edge_bps_median": 25,
        "bootstrap_positive_pct": 97,
    })
    assert result["decision"] == "PAPER_CANDIDATE"
    assert result["live_change_allowed"] is False
