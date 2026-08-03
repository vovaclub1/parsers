import json
from pathlib import Path

from tg.strategy_eval import render_card, winrates_from_results


def test_render_card_separates_event_type_from_position_side():
    text = render_card(
        side="short",
        event_type="listing",
        strategy_version="contrarian-v1",
        coin="TEST",
        complete_ts=1_700_000_000,
        src="fixture",
        actual_pnl=1.0,
        sims={},
        winner=None,
        wins={},
        total=0,
        margin_usdt=10.0,
        bot_username="",
    )

    assert "LISTING SHORT" in text
    assert "DELIST" not in text


def test_winrates_only_use_same_strategy_cohort(tmp_path: Path):
    path = tmp_path / "results.jsonl"
    rows = [
        {
            "complete_ts": 100,
            "event_type": "listing",
            "side": "short",
            "strategy_version": "contrarian-v1",
            "winner": "live",
        },
        {
            "complete_ts": 101,
            "event_type": "listing",
            "side": "long",
            "strategy_version": "momentum-v1",
            "winner": "trail=2%",
        },
        {
            "complete_ts": 102,
            "event_type": "delisting",
            "side": "long",
            "strategy_version": "contrarian-v1",
            "winner": "atr_trail",
        },
        # Legacy record: cohort is unknown and must not pollute current stats.
        {"complete_ts": 103, "side": "short", "winner": "grid"},
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")

    wins, total = winrates_from_results(
        path,
        event_type="listing",
        side="short",
        strategy_version="contrarian-v1",
        extra_winner="live",
    )

    assert total == 2
    assert wins == {"live": 2}
