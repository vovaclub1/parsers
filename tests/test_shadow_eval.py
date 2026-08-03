from tg.shadow_eval import evaluate_directions


def test_listing_compares_momentum_long_and_contrarian_short():
    result = evaluate_directions(
        event_type="listing",
        actual_side="short",
        entry=100.0,
        samples=[[1, 110.0], [2, 120.0], [3, 115.0]],
        leverage=1.0,
        taker=0.0,
    )
    assert result["momentum"]["side"] == "long"
    assert result["contrarian"]["side"] == "short"
    assert result["momentum"]["buyhold_window"] > 0
    assert result["contrarian"]["buyhold_window"] < 0


def test_delisting_compares_momentum_short_and_contrarian_long():
    result = evaluate_directions(
        event_type="delisting",
        actual_side="long",
        entry=100.0,
        samples=[[1, 90.0], [2, 80.0], [3, 85.0]],
        leverage=1.0,
        taker=0.0,
    )
    assert result["momentum"]["side"] == "short"
    assert result["contrarian"]["side"] == "long"
    assert result["momentum"]["buyhold_window"] > 0
    assert result["contrarian"]["buyhold_window"] < 0


def test_actual_side_is_labeled_without_assuming_event_semantics():
    result = evaluate_directions(
        event_type="listing", actual_side="short", entry=100,
        samples=[[1, 101]], leverage=1, taker=0,
    )
    assert result["actual_strategy"] == "contrarian"


def test_unknown_event_type_is_rejected():
    import pytest
    with pytest.raises(ValueError):
        evaluate_directions(
            event_type="other", actual_side="long", entry=100,
            samples=[[1, 101]], leverage=1, taker=0,
        )
