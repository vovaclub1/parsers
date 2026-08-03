from storage.execution_store import ExecutionStore
from storage.async_writer import AsyncExecutionWriter
from storage import runtime


def test_runtime_initializes_once_per_process(tmp_path):
    runtime.reset_for_tests()
    first = runtime.init(tmp_path / "execution.sqlite3")
    second = runtime.init(tmp_path / "other.sqlite3")
    assert first is second
    assert runtime.get_writer() is first
    assert runtime.get_store() is first.store
    runtime.reset_for_tests()
