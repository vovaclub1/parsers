from __future__ import annotations

import threading

from storage.async_writer import AsyncExecutionWriter
from storage.execution_store import ExecutionStore

_lock = threading.Lock()
_writer: AsyncExecutionWriter | None = None


def init(path) -> AsyncExecutionWriter:
    global _writer
    with _lock:
        if _writer is None:
            _writer = AsyncExecutionWriter(ExecutionStore(path))
        return _writer


def get_writer() -> AsyncExecutionWriter | None:
    return _writer


def get_store() -> ExecutionStore | None:
    return _writer.store if _writer is not None else None


def reset_for_tests() -> None:
    global _writer
    with _lock:
        if _writer is not None:
            _writer.stop()
            _writer.store.close()
        _writer = None
