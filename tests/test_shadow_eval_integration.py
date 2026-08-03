import json

from tg.strategy_eval import evaluate


def test_strategy_result_persists_direction_shadow(tmp_path, monkeypatch):
    monkeypatch.setattr("tg.strategy_eval._bot_username", lambda: "")
    path = tmp_path / "results.jsonl"
    evaluate(
        {
            "coin": "AAA", "side": "short", "event_type": "listing",
            "strategy_version": "contrarian-v1", "src": "fixture",
            "venue": "bybit", "entry": 100, "entry_ts": 1,
            "samples": [[1, 110], [2, 120], [3, 115]],
        },
        actual_pnl=None,
        results_path=path,
        leverage=1,
        taker=0,
        margin_usdt=10,
    )
    row = json.loads(path.read_text().strip())
    assert row["direction_shadow"]["actual_strategy"] == "contrarian"
    assert row["direction_shadow"]["momentum"]["side"] == "long"
    assert row["direction_shadow"]["contrarian"]["side"] == "short"
