from storage.execution_store import ExecutionStore
from tools.tca_report import build_report


def test_unlinked_exit_partial_fills_are_one_execution_order(tmp_path):
    db = tmp_path / "execution.sqlite3"
    store = ExecutionStore(db)
    for i, qty in enumerate((1, 2, 3)):
        store.record_fill(
            exec_id=f"e{i}", client_order_id="", exchange_order_id="close-1",
            symbol="AAAUSDT", side="Buy", price=100, qty=qty,
            ts_ns=10 + i,
        )
    report = build_report(db)
    assert report["fills"] == 3
    assert report["orders_filled"] == 1
    assert report["cohorts"]["unknown"]["orders"] == 1
    assert report["fees_total"] == 0
