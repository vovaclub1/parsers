from __future__ import annotations

# ── health_stats.py ───────────────────────────────────────────────
# Диагностика «слабых мест» бота: персистентный сбор метрик по каждому
# компоненту (источники сигналов, биржи, соединения) + человекочитаемый
# отчёт в TG/stdout, по которому видно ГДЕ узкое место.
#
# Метрики (по ключу-компоненту):
#   signals     — сколько сигналов пришло
#   throttles   — 429 / Cloudflare 1015 / rate-limit
#   errors      — сетевые/парс-ошибки
#   disconnects — разрывы WS/relay
#   latency_ms  — running stats (count/sum/min/max) длительностей
#
# Персистентность: атомарный write tmp→rename, тот же паттерн что в
# source_stats. Переживает рестарт. Фоновый writer по dirty-flag.
#
# Thread-safe: один lock (uncontended ~0.1мкс, вне hot-path ордера).
# ─────────────────────────────────────────────────────────────────

import json
import threading
import time
from pathlib import Path

try:
    import orjson as _orjson
    def _dumps(o) -> bytes:
        return _orjson.dumps(o, option=_orjson.OPT_INDENT_2)
    def _loads(b):
        return _orjson.loads(b)
except ImportError:
    def _dumps(o) -> bytes:
        return json.dumps(o, indent=2, ensure_ascii=False).encode()
    def _loads(b):
        if isinstance(b, (bytes, bytearray)):
            b = b.decode()
        return json.loads(b)


def _empty_metric() -> dict:
    return {
        "signals": 0,
        "throttles": 0,
        "errors": 0,
        "disconnects": 0,
        "lat_count": 0,
        "lat_sum": 0.0,
        "lat_min": 0.0,
        "lat_max": 0.0,
    }


class HealthStats:
    """Персистентный сбор health-метрик по компонентам."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._comp: dict[str, dict] = {}
        self._started_at = int(time.time())
        self._dirty = threading.Event()
        self._load()

    # ── события (вызываются из кода компонентов) ──────────────────
    def _bump(self, comp: str, field: str, n: int = 1) -> None:
        with self._lock:
            m = self._comp.get(comp)
            if m is None:
                m = _empty_metric()
                self._comp[comp] = m
            m[field] += n
        self._dirty.set()

    def signal(self, comp: str) -> None:
        """Компонент успешно поймал сигнал (листинг/делист)."""
        self._bump(comp, "signals")

    def throttle(self, comp: str) -> None:
        """429 / Cloudflare 1015 / rate-limit — компонент ослеп."""
        self._bump(comp, "throttles")

    def error(self, comp: str) -> None:
        """Сетевая/парс-ошибка."""
        self._bump(comp, "errors")

    def disconnect(self, comp: str) -> None:
        """Разрыв WS/relay-соединения."""
        self._bump(comp, "disconnects")

    def latency(self, comp: str, ms: float) -> None:
        """Записать длительность этапа (детект, открытие ордера и т.д.)."""
        with self._lock:
            m = self._comp.get(comp)
            if m is None:
                m = _empty_metric()
                self._comp[comp] = m
            m["lat_count"] += 1
            m["lat_sum"] += ms
            if m["lat_count"] == 1:
                m["lat_min"] = ms
                m["lat_max"] = ms
            else:
                m["lat_min"] = min(m["lat_min"], ms)
                m["lat_max"] = max(m["lat_max"], ms)
        self._dirty.set()

    # ── персистентность ───────────────────────────────────────────
    def _load(self) -> None:
        try:
            if not self._path.exists():
                return
            raw = self._path.read_bytes()
            if not raw.strip():
                return
            data = _loads(raw)
            comp = data.get("components") if isinstance(data, dict) else None
            if isinstance(comp, dict):
                with self._lock:
                    for k, v in comp.items():
                        if isinstance(k, str) and isinstance(v, dict):
                            base = _empty_metric()
                            base.update({fk: v.get(fk, base[fk]) for fk in base})
                            self._comp[k] = base
        except Exception as e:  # noqa: BLE001
            print(f"[HEALTH] load failed: {e!r}", flush=True)

    def persist(self) -> None:
        """Атомарный write tmp→rename. Без блокировки hot-path."""
        try:
            with self._lock:
                snapshot = {
                    "started_at": self._started_at,
                    "updated_at": int(time.time()),
                    "components": {k: dict(v) for k, v in self._comp.items()},
                }
            payload = _dumps(snapshot)
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(".json.tmp")
            tmp.write_bytes(payload)
            tmp.replace(self._path)
        except Exception as e:  # noqa: BLE001
            print(f"[HEALTH] persist failed: {e!r}", flush=True)

    def persist_loop(self, batch_sec: float = 300.0) -> None:
        """Фоновый writer: dirty-flag + батчинг. persist ДО clear."""
        while True:
            self._dirty.wait()
            time.sleep(batch_sec)
            self.persist()
            self._dirty.clear()

    # ── отчёт «слабые места» ──────────────────────────────────────
    def build_report(self) -> str:
        """
        Человекочитаемый отчёт. Подсвечивает компоненты с проблемами
        (троттлы/ошибки/дисконнекты) — это и есть «слабые места».
        Возвращает HTML-строку для TG (и stdout).
        """
        with self._lock:
            comps = {k: dict(v) for k, v in self._comp.items()}
            uptime_h = (int(time.time()) - self._started_at) / 3600.0

        if not comps:
            return "📊 <b>HEALTH</b>\nНет данных ещё."

        lines = [f"📊 <b>HEALTH-отчёт</b> (uptime {uptime_h:.1f}ч)", ""]

        # Источники сигналов: кто сколько поймал.
        sig_rows = sorted(
            ((k, v["signals"]) for k, v in comps.items() if v["signals"] > 0),
            key=lambda kv: kv[1], reverse=True,
        )
        total_sig = sum(n for _, n in sig_rows)
        if sig_rows:
            lines.append("<b>Сигналы по источникам:</b>")
            for k, n in sig_rows:
                pct = 100.0 * n / total_sig if total_sig else 0
                lines.append(f"  • {k}: {n} ({pct:.0f}%)")
            lines.append("")

        # СЛАБЫЕ МЕСТА: троттлы/ошибки/дисконнекты.
        weak = []
        for k, v in comps.items():
            problems = []
            if v["throttles"]:
                problems.append(f"⛔throttle×{v['throttles']}")
            if v["errors"]:
                problems.append(f"⚠️err×{v['errors']}")
            if v["disconnects"]:
                problems.append(f"🔌drop×{v['disconnects']}")
            if problems:
                # вес проблемы = сумма для сортировки
                weight = v["throttles"] * 3 + v["errors"] + v["disconnects"] * 2
                weak.append((weight, k, " ".join(problems)))
        weak.sort(reverse=True)
        if weak:
            lines.append("<b>⚡ Слабые места:</b>")
            for _, k, desc in weak:
                lines.append(f"  • {k}: {desc}")
            lines.append("")
        else:
            lines.append("✅ Слабых мест не зафиксировано")
            lines.append("")

        # Латентность по этапам (где медленно).
        lat_rows = []
        for k, v in comps.items():
            if v["lat_count"] > 0:
                mean = v["lat_sum"] / v["lat_count"]
                lat_rows.append((mean, k, v))
        lat_rows.sort(reverse=True)
        if lat_rows:
            lines.append("<b>Латентность (mean / max / n):</b>")
            for mean, k, v in lat_rows:
                lines.append(
                    f"  • {k}: {mean:.0f}мс / {v['lat_max']:.0f}мс / n={v['lat_count']}"
                )

        return "\n".join(lines)
