from research.cohorts import classify_subtype, cohort_key


def test_classify_source_subtypes():
    assert classify_subtype("UPBIT-NOTICE") == "announcement"
    assert classify_subtype("TG:123") == "announcement"
    assert classify_subtype("TOA-WS") == "announcement"
    assert classify_subtype("COINLISTING-UPBIT") == "announcement"
    assert classify_subtype("SEOUL-RELAY-BITHUMB") == "relay_announcement"
    assert classify_subtype("UPBIT") == "market_appearance"
    assert classify_subtype("BITHUMB") == "market_appearance"
    assert classify_subtype("BINANCE") == "market_appearance"
    assert classify_subtype("OTHER") == "unknown"


def test_cohort_key_includes_event_source_subtype_and_side():
    assert cohort_key("listing", "UPBIT-NOTICE", "short") == (
        "listing", "UPBIT-NOTICE", "announcement", "short",
    )
