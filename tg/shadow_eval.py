from __future__ import annotations

from tg.exit_strategies import (
    build_all_strategies,
    clean_samples,
    simulate_candidates,
)


_EVENT_SIDES = {
    "listing": {"momentum": "long", "contrarian": "short"},
    "delisting": {"momentum": "short", "contrarian": "long"},
}


def evaluate_directions(event_type: str, actual_side: str, entry: float,
                        samples, leverage: float = 10.0,
                        taker: float = 0.00055) -> dict:
    """Simulates momentum and contrarian sides on the same observed path."""
    if event_type not in _EVENT_SIDES:
        raise ValueError(f"unsupported event_type: {event_type}")
    pts = clean_samples(samples)
    result = {}
    for strategy, side in _EVENT_SIDES[event_type].items():
        sims = simulate_candidates(
            pts, side, float(entry), leverage, taker,
            strategies=build_all_strategies(side, sweep_trail=False),
        )
        result[strategy] = {
            "side": side,
            **{name: round(values["pnl"], 6) for name, values in sims.items()},
        }
    result["actual_strategy"] = next(
        (name for name, side in _EVENT_SIDES[event_type].items()
         if side == actual_side),
        "unknown",
    )
    return result
