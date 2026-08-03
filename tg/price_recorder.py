from __future__ import annotations

# ── price_recorder.py ─────────────────────────────────────────────
# Рекордер пост-входной траектории цены. После каждого открытия позиции
# (листинг/делист) пишет, что было с ценой следующие ~6 часов — чтобы
# ОФФЛАЙН подбирать стратегию выхода (см. tools/backtest_exits.py), а не
# крутить параметры трейлинга вслепую в проде.
#
# Дизайн:
#   • track(coin, side, ...) — регистрирует запись (hot-path: только dict.set
#     под локом, ~µs; реальный сэмплинг — в фоновом потоке).
#   • run_loop() — один фоновый поток, тик 1с, сэмплит сразу все активные
#     монеты из общего price_cache через get_price_fn. Дёшево.
#   • Ступенчатое расписание: первые 5 мин по 1с, до 1ч по 5с, до 6ч по 30с.
#     Ловит и быстрый памп листинга, и медленный дрейф делиста; файл компактен.
#   • Персистентность: append-only jsonl (как signal_stats). Одна строка =
#     одна траектория. flush_active() досбрасывает частичные записи на
#     остановке (вызывается из _graceful_shutdown парсера).
#
# Схема строки:
#   {"coin","side","src","venue","entry_ts","entry","samples":[[dt_s,price|null],...]}
#   dt_s — секунды от входа; price=null если get_price вернул None (брэнд-новый
#   тикер ещё не появился на Bybit) — бэктест такие точки пропускает.
#
# Thread-safe: один lock вокруг словаря активных. get_price вызывается ВНЕ
# лока (может быть медленным). Кап max_active — защита от runaway.
# ─────────────────────────────────────────────────────────────────

import json
import threading
import time
from pathlib import Path

try:
    import orjson as _orjson  # type: ignore[import-not-found]
    def _dumps_line(o) -> bytes:
        return _orjson.dumps(o)
except ImportError:
    def _dumps_line(o) -> bytes:
        return json.dumps(o, ensure_ascii=False).encode()


# Ступени расписания: (граница_сек, интервал_сек). Действует пока
# elapsed < граница. После последней границы — окно закрыто.
# Гибрид: рекордер ловит только «голову» входа (первые 10 мин) с высокой
# гранулярностью; хвост до 6ч дотягивается klines-свечами в момент оценки.
_SCHEDULE = ((300.0, 1.0), (600.0, 5.0))   # 1с до 5мин, 5с до 10мин
_WINDOW_SEC = 600.0  # 10 минут (голова входа)


def _interval_for(elapsed: float):
    """Интервал сэмплинга для данного elapsed, или None если окно закрыто."""
    for boundary, interval in _SCHEDULE:
        if elapsed < boundary:
            return interval
    return None


class _Rec:
    """Одна активная запись траектории."""
    __slots__ = ("coin", "side", "event_type", "strategy_version",
                 "src", "venue", "entry", "entry_ts",
                 "t0", "samples", "next_at")

    def __init__(self, coin: str, side: str, event_type: str,
                 strategy_version: str, src: str, venue: str,
                 entry: float, entry_ts: int, t0: float) -> None:
        self.coin = coin
        self.side = side
        self.event_type = event_type
        self.strategy_version = strategy_version
        self.src = src
        self.venue = venue
        self.entry = entry
        self.entry_ts = entry_ts
        self.t0 = t0           # time.monotonic() входа
        self.samples: list = []  # [[dt_s, price|None], ...]
        self.next_at = t0      # monotonic времени следующего сэмпла


class PriceRecorder:
    """Фоновый рекордер пост-входных траекторий цены."""

    def __init__(self, path, get_price_fn, max_active: int = 300,
                 window_sec: float = _WINDOW_SEC, on_complete=None) -> None:
        self._path = Path(path)
        self._get_price = get_price_fn
        self._max_active = max_active
        self._window = window_sec
        # on_complete(record_dict) — вызывается РОВНО для завершённых по окну
        # записей (НЕ для partial из flush_active). Сюда парсер вешает 6ч-итоги.
        self._on_complete = on_complete
        self._lock = threading.Lock()
        self._active: dict = {}        # coin -> _Rec
        self._stopped = threading.Event()
        self._loop_thread = None       # хэндл run_loop-потока (для join на shutdown)

    # ── hot-path: регистрация (вызывается из worker после открытия) ──
    def track(self, coin: str, side: str, src: str, venue: str,
              entry, entry_ts=None, event_type="unknown",
              strategy_version="legacy") -> None:
        if not coin or entry is None:
            return
        try:
            entry_f = float(entry)
        except (TypeError, ValueError):
            return
        if entry_f <= 0:
            return
        now = time.monotonic()
        ets = int(entry_ts if entry_ts is not None else time.time())
        with self._lock:
            if coin in self._active:
                return  # уже пишем эту монету — не дублируем
            if len(self._active) >= self._max_active:
                print(f"[PRICE-REC] max_active={self._max_active} достигнут, "
                      f"{coin} пропущен", flush=True)
                return
            self._active[coin] = _Rec(coin, side, event_type, strategy_version,
                                      src or "?", venue or "?",
                                      entry_f, ets, now)

    # ── фоновый поток ─────────────────────────────────────────────
    def run_loop(self, tick: float = 1.0) -> None:
        """Бесконечный цикл сэмплинга. Запускать в daemon-потоке."""
        self._loop_thread = threading.current_thread()
        while not self._stopped.is_set():
            try:
                self._tick()
            except Exception as e:  # noqa: BLE001
                print(f"[PRICE-REC] tick failed: {e!r}", flush=True)
            # Прерываемый сон: на shutdown _stopped.set() будит сразу.
            if self._stopped.wait(tick):
                break

    def _tick(self) -> None:
        now = time.monotonic()
        due: list = []
        done: list = []
        with self._lock:
            for coin in list(self._active.keys()):
                rec = self._active[coin]
                elapsed = now - rec.t0
                if elapsed >= self._window:
                    done.append(self._active.pop(coin))
                    continue
                if now >= rec.next_at:
                    due.append(rec)
        # сэмплим ВНЕ лока — get_price может быть медленным.
        for rec in due:
            elapsed = now - rec.t0
            interval = _interval_for(elapsed)
            if interval is None:
                continue
            try:
                p = self._get_price(rec.coin)
            except Exception:  # noqa: BLE001
                p = None
            price_val = None
            if p:
                try:
                    price_val = float(p)
                except (TypeError, ValueError):
                    price_val = None
            rec.samples.append([round(elapsed, 1), price_val])
            rec.next_at = now + interval
        for rec in done:
            self._write(rec, partial=False)
            # 6ч-окно закрылось → дёргаем on_complete (только для завершённых).
            if self._on_complete is not None:
                try:
                    self._on_complete(self._to_row(rec))
                except Exception as e:  # noqa: BLE001
                    print(f"[PRICE-REC] on_complete failed для {rec.coin}: {e!r}",
                          flush=True)

    # ── персистентность ───────────────────────────────────────────
    @staticmethod
    def _to_row(rec: "_Rec") -> dict:
        return {
            "coin": rec.coin,
            "side": rec.side,
            "event_type": rec.event_type,
            "strategy_version": rec.strategy_version,
            "src": rec.src,
            "venue": rec.venue,
            "entry_ts": rec.entry_ts,
            "entry": rec.entry,
            "samples": rec.samples,
        }

    def _write(self, rec: "_Rec", partial: bool) -> None:
        try:
            row = self._to_row(rec)
            if partial:
                row["partial"] = True
            self._path.parent.mkdir(parents=True, exist_ok=True)
            line = _dumps_line(row) + b"\n"
            with self._path.open("ab") as f:
                f.write(line)
        except Exception as e:  # noqa: BLE001
            print(f"[PRICE-REC] write failed для {rec.coin}: {e!r}", flush=True)

    def flush_active(self) -> None:
        """
        Досбросить ВСЕ активные (частичные) записи на диск. Вызывается из
        _graceful_shutdown парсера по SIGTERM/atexit — чтобы не терять
        накопленную траекторию при рестарте/деплое.
        """
        self._stopped.set()
        # Дожидаемся завершения текущего _tick, чтобы его append'ы в jsonl не
        # обрезались при sys.exit (поток daemon). Не join'им сами себя.
        t = self._loop_thread
        if t is not None and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=3.0)
        with self._lock:
            recs = list(self._active.values())
            self._active.clear()
        for rec in recs:
            self._write(rec, partial=True)
