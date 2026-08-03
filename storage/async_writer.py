from __future__ import annotations

import queue
import threading
import time


class AsyncExecutionWriter:
    """Bounded non-blocking queue for telemetry writes outside hot-path."""

    def __init__(self, store, max_queue: int = 4096, autostart: bool = True) -> None:
        self.store = store
        self.queue = queue.Queue(maxsize=max_queue)
        self.dropped = 0
        self.errors = 0
        self._stop = threading.Event()
        self._thread = None
        if autostart:
            self._thread = threading.Thread(
                target=self._loop, daemon=True, name="execution-store-writer",
            )
            self._thread.start()

    def submit(self, method: str, **kwargs) -> bool:
        try:
            self.queue.put_nowait((method, kwargs))
            return True
        except queue.Full:
            self.dropped += 1
            return False

    def _loop(self) -> None:
        while not self._stop.is_set() or not self.queue.empty():
            try:
                method, kwargs = self.queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                getattr(self.store, method)(**kwargs)
            except Exception as e:  # noqa: BLE001
                self.errors += 1
                print(f"[EXEC-STORE] async write failed {method}: {e!r}", flush=True)
            finally:
                self.queue.task_done()

    def flush(self, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while self.queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.005)
        return self.queue.unfinished_tasks == 0

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        self.flush(timeout)
        if self._thread is not None:
            self._thread.join(timeout=timeout)
