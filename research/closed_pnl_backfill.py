from __future__ import annotations

from api.bybit_pnl import authoritative_record as bybit_record
from api.gate_pnl import authoritative_record as gate_record


def _record_ts_ns(venue: str, rec: dict) -> int:
    if venue == "bybit":
        try:
            return int(rec.get("updatedTime") or rec.get("createdTime") or 0) * 1_000_000
        except (TypeError, ValueError):
            return 0
    try:
        return int(rec.get("time") or 0) * 1_000_000_000
    except (TypeError, ValueError):
        return 0


def backfill_records(store, *, venue: str, symbol: str, records: list[dict]) -> int:
    """FIFO link exchange closed-PnL records to prior intents, idempotently."""
    venue = venue.lower()
    intents = store.list_intents(
        venue=venue, symbol=symbol, executed_only=(venue == "bybit"),
    )
    records = sorted(records, key=lambda r: _record_ts_ns(venue, r))
    used = set()
    inserted = 0
    for intent in intents:
        idx = next((i for i, rec in enumerate(records)
                    if i not in used and _record_ts_ns(venue, rec) >= int(intent["created_ts_ns"])), None)
        if idx is None:
            continue
        used.add(idx)
        rec = records[idx]
        normalize = bybit_record if venue == "bybit" else gate_record
        row = normalize(rec, signal_id=intent["signal_id"],
                        client_order_id=intent["client_order_id"])
        inserted += int(store.record_authoritative_pnl(**row))
    return inserted
