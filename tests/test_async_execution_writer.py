from storage.async_writer import AsyncExecutionWriter
from storage.execution_store import ExecutionStore


def test_writer_persists_queued_call(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    writer = AsyncExecutionWriter(store, max_queue=10)
    assert writer.submit("record_order_event", client_order_id="l", status="SENT", ts_ns=1)
    writer.flush(timeout=2)
    assert store.get_order("l")["status"] == "SENT"
    writer.stop()


def test_writer_is_non_blocking_and_counts_overflow(tmp_path):
    store = ExecutionStore(tmp_path / "execution.sqlite3")
    writer = AsyncExecutionWriter(store, max_queue=1, autostart=False)
    assert writer.submit("record_order_event", client_order_id="a", status="SENT")
    assert writer.submit("record_order_event", client_order_id="b", status="SENT") is False
    assert writer.dropped == 1
