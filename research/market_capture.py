from __future__ import annotations

import time


def capture_bbo_snapshot(*, store, fetch_ticker, signal_id: str,
                         client_order_id: str, symbol: str,
                         stage: str) -> bool:
    """Best-effort BBO capture; telemetry failure never blocks an order."""
    try:
        row = fetch_ticker(symbol) or {}
        bid = float(row.get("bid1Price", 0) or 0)
        ask = float(row.get("ask1Price", 0) or 0)
        bid_qty = float(row.get("bid1Size", 0) or 0)
        ask_qty = float(row.get("ask1Size", 0) or 0)
        store.record_market_snapshot(
            signal_id=signal_id, client_order_id=client_order_id,
            venue="bybit", symbol=symbol, stage=stage,
            bid=bid, ask=ask, bid_qty=bid_qty, ask_qty=ask_qty,
            depth_bids=[[bid, bid_qty]], depth_asks=[[ask, ask_qty]],
            ts_ns=time.time_ns(),
        )
        return True
    except Exception:
        return False
