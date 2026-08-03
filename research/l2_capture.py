from __future__ import annotations

import queue
import threading
import time


class L2CaptureWorker:
    """Bounded post-send orderbook capture; never blocks trading hot-path."""

    def __init__(self, store, fetch_orderbook, max_queue: int = 256,
                 limit: int = 50) -> None:
        self.store = store
        self.fetch_orderbook = fetch_orderbook
        self.limit = limit
        self.queue = queue.Queue(maxsize=max_queue)
        self.dropped = 0
        self.errors = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="l2-capture",
        )
        self._thread.start()

    def submit(self, *, signal_id: str, client_order_id: str,
               symbol: str, stage: str = "send_l2") -> bool:
        try:
            self.queue.put_nowait((signal_id, client_order_id, symbol, stage))
            return True
        except queue.Full:
            self.dropped += 1
            return False

    @staticmethod
    def _levels(raw) -> list[list[float]]:
        levels = []
        for row in raw or []:
            try:
                price, qty = float(row[0]), float(row[1])
            except (IndexError, TypeError, ValueError):
                continue
            if price > 0 and qty > 0:
                levels.append([price, qty])
        return levels

    def _loop(self) -> None:
        while not self._stop.is_set() or not self.queue.empty():
            try:
                signal_id, link, symbol, stage = self.queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                data = self.fetch_orderbook(symbol, self.limit) or {}
                bids, asks = self._levels(data.get("b")), self._levels(data.get("a"))
                if not bids or not asks:
                    raise ValueError("empty orderbook")
                self.store.record_market_snapshot(
                    signal_id=signal_id, client_order_id=link,
                    venue="bybit", symbol=symbol, stage=stage,
                    bid=bids[0][0], ask=asks[0][0],
                    bid_qty=bids[0][1], ask_qty=asks[0][1],
                    depth_bids=bids, depth_asks=asks,
                    ts_ns=time.time_ns(),
                )
            except Exception:
                self.errors += 1
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
        self._thread.join(timeout=timeout)
