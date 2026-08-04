from __future__ import annotations

import hashlib
import json
from pathlib import Path


def import_gate_intents(store, sources) -> int:
    """Import legacy Gate open events into unified lifecycle store."""
    inserted = 0
    for path, event_type in sources:
        path = Path(path)
        if not path.exists():
            continue
        for line in path.read_text(errors="ignore").splitlines():
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("k") != "open" or str(rec.get("venue", "")).lower() != "gate":
                continue
            if rec.get("outcome") != "opened" or not rec.get("coin"):
                continue
            coin = str(rec["coin"]).upper()
            ts = int(float(rec.get("ts", 0)) * 1_000_000_000)
            digest = hashlib.sha256(line.encode()).hexdigest()[:24]
            link = f"gate-legacy-{digest}"
            if store.get_order(link):
                continue
            store.record_intent(
                signal_id=f"{event_type}:{coin}:{ts}",
                client_order_id=link, venue="gate", symbol=f"{coin}_USDT",
                side="Sell" if event_type == "listing" else "Buy",
                requested_qty=float(rec.get("amount", 0) or 0),
                route="legacy_gate_import", ts_ns=ts,
            )
            inserted += 1
    return inserted
