from __future__ import annotations


def classify_subtype(source: str) -> str:
    src = (source or "").upper()
    if "RELAY" in src:
        return "relay_announcement"
    if "NOTICE" in src or src.startswith("TG:") or "TOA" in src or "COINLISTING" in src:
        return "announcement"
    if src in {"UPBIT", "BITHUMB", "BINANCE"}:
        return "market_appearance"
    return "unknown"


def cohort_key(event_type: str, source: str, side: str) -> tuple[str, str, str, str]:
    return event_type, source, classify_subtype(source), side
