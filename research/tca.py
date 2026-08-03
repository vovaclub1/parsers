from __future__ import annotations


def bbo_metrics(bid: float, ask: float) -> dict:
    bid, ask = float(bid), float(ask)
    if bid <= 0 or ask <= 0 or bid >= ask:
        raise ValueError("invalid BBO")
    mid = (bid + ask) / 2.0
    return {"bid": bid, "ask": ask, "mid": mid,
            "spread_bps": (ask - bid) / mid * 10_000.0}


def sweep_book(levels, notional: float, side: str) -> dict:
    """Consumes price/qty levels until quote notional is filled."""
    if side not in ("buy", "sell"):
        raise ValueError("side must be buy or sell")
    remaining = float(notional)
    if remaining <= 0:
        raise ValueError("notional must be positive")
    filled_notional = 0.0
    filled_qty = 0.0
    worst_price = 0.0
    for raw_price, raw_qty in levels:
        price, qty = float(raw_price), float(raw_qty)
        if price <= 0 or qty <= 0:
            continue
        available = price * qty
        used_notional = min(remaining, available)
        used_qty = used_notional / price
        filled_notional += used_notional
        filled_qty += used_qty
        remaining -= used_notional
        worst_price = price
        if remaining <= 1e-12:
            break
    return {
        "requested_notional": float(notional),
        "filled_notional": filled_notional,
        "filled_qty": filled_qty,
        "vwap": filled_notional / filled_qty if filled_qty else 0.0,
        "worst_price": worst_price,
        "complete": remaining <= 1e-12,
    }


def implementation_shortfall_bps(side: str, arrival_mid: float,
                                 fill_price: float, fee_bps: float = 0) -> float:
    direction = 1.0 if side == "buy" else -1.0 if side == "sell" else None
    if direction is None:
        raise ValueError("side must be buy or sell")
    return direction * (float(fill_price) - float(arrival_mid)) / float(arrival_mid) * 10_000 + float(fee_bps)


def markout_bps(side: str, fill_price: float, future_mid: float) -> float:
    direction = 1.0 if side == "buy" else -1.0 if side == "sell" else None
    if direction is None:
        raise ValueError("side must be buy or sell")
    return direction * (float(future_mid) - float(fill_price)) / float(fill_price) * 10_000
