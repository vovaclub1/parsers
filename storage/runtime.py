from __future__ import annotations

import threading

from storage.async_writer import AsyncExecutionWriter
from storage.execution_store import ExecutionStore
from research.l2_capture import L2CaptureWorker

_lock = threading.Lock()
_writer: AsyncExecutionWriter | None = None
_l2_worker: L2CaptureWorker | None = None


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


def init_l2(fetch_orderbook) -> L2CaptureWorker:
    global _l2_worker
    with _lock:
        if _l2_worker is None:
            store = get_store()
            if store is None:
                raise RuntimeError("ExecutionStore must be initialized before L2")
            _l2_worker = L2CaptureWorker(store, fetch_orderbook)
        return _l2_worker


def get_l2_worker() -> L2CaptureWorker | None:
    return _l2_worker


def reset_for_tests() -> None:
    global _writer, _l2_worker
    with _lock:
        if _l2_worker is not None:
            _l2_worker.stop()
        _l2_worker = None
        if _writer is not None:
            _writer.stop()
            _writer.store.close()
        _writer = None
