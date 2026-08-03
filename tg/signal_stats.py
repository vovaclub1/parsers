from __future__ import annotations

# ── signal_stats.py ───────────────────────────────────────────────
# Журнал событий сигналов + периодические отчёты (09:00 / 22:00 МСК).
#
# Зачем отдельно от health_stats: health копит ТОЛЬКО кумулятивные счётчики
# (signals/throttles/latency) без таймстампов — по нему нельзя ответить «что
# было за последние 7 дней / за месяц». Здесь — append-only журнал событий с
# временем, из которого любой временной срез считается ретроспективно.
#
# Хранилище: STATE_DIR/signal_events.jsonl — одна строка = одно событие.
#   open: {"k":"open","ts":N,"coin":"FLR","src":"TOA-WS","venue":"bybit",
#          "outcome":"opened","open_ms":42.0,"entry":0.0072,"amount":19290.0}
#     outcome ∈ opened | no_price | rejected | failed
#   lag:  {"k":"lag","ts":N,"coin":"ZEST","loser":"BINANCE-NOTICE",
#          "winner":"TG:...","lag_ms":45570.0}
#
# Hot-path делает только deque.append (~µs). Фоновый flush-loop дописывает
# в файл батчами. Ретенция — старые строки (>RETENTION_DAYS) выкидываются
# при flush через compaction (раз в сутки).
#
# Отчёты: report_scheduler_loop спит до ближайших 09:00/22:00 МСК, шлёт в TG
# три секции (с прошлого отчёта / 7 полных дней / 30 дней) + в конце месяца
# полную статистику за завершившийся месяц.
#
# Зависимостей не добавляет: МСК = фикс. UTC+3 (Россия без переходов с 2014).
# ─────────────────────────────────────────────────────────────────

import json
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import orjson as _orjson  # type: ignore[import-not-found]
    def _dumps_line(o) -> bytes:
        return _orjson.dumps(o)  # без indent — построчный jsonl
    def _loads(b):
        return _orjson.loads(b)
except ImportError:
    def _dumps_line(o) -> bytes:
        return json.dumps(o, ensure_ascii=False).encode()
    def _loads(b):
        if isinstance(b, (bytes, bytearray)):
            b = b.decode()
        return json.loads(b)


# МСК — фиксированный UTC+3 (Россия не переходит на летнее время с 2014).
MSK = timezone(timedelta(hours=3))

RETENTION_DAYS = 400
REPORT_HOURS_MSK = (9, 22)   # во сколько по МСК слать отчёты

# Периоды статистики (скользящие окна) для бота и авто-отчёта.
PERIOD_WINDOWS = {"1d": 86400, "7d": 7 * 86400, "30d": 30 * 86400}
_PERIOD_LABEL = {"1d": ("День", "за 24 часа"),
                 "7d": ("Неделя", "за 7 дней"),
                 "30d": ("Месяц", "за 30 дней")}


def _pre_escape(s: str) -> str:
    """Экранирует & < > для текста внутри <pre> (Telegram HTML)."""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# outcome → человекочитаемая причина провала
_OUTCOME_LABEL = {
    "opened":   "✅ открыт",
    "no_price": "нет цены",
    "rejected": "биржа отвергла",
    "failed":   "провал",
}


def _now_ts() -> int:
    return int(time.time())


def _pct(n: float, total: float) -> float:
    return (100.0 * n / total) if total else 0.0


def _percentile(sorted_vals: list[float], q: float) -> float:
    """Линейная интерполяция перцентиля. sorted_vals должен быть отсортирован."""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = q * (len(sorted_vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


class SignalStats:
    """Append-only журнал событий сигналов + агрегации по временным окнам."""

    def __init__(self, events_path: str | Path, state_path: str | Path,
                 kind: str = "LISTING") -> None:
        self._events_path = Path(events_path)
        self._state_path = Path(state_path)   # last-report ts (переживает рестарт)
        self._kind = kind.upper()             # LISTING | DELIST — для заголовка
        self._lock = threading.Lock()
        self._buf: deque[dict] = deque()      # ещё не записанные на диск
        self._dirty = threading.Event()
        self._last_compact_day = ""           # YYYY-MM-DD последней компакции

    # ── события (hot-path, только append в буфер) ─────────────────
    def log_open(self, coin: str, src: str, venue: str, outcome: str,
                 open_ms: float = 0.0, entry: float = 0.0,
                 amount: float = 0.0, listing_exchange: str = "") -> None:
        """
        venue — биржа ИСПОЛНЕНИЯ (bybit/gate, где открыли позицию).
        listing_exchange — биржа ЛИСТИНГА (UPBIT/BITHUMB/BINANCE), откуда
        пришёл сигнал. Разные вещи: листят на Upbit, а лонг открываем на Bybit.
        pnl дописывается отложенно (update_pnl) когда позиция закроется.
        """
        ev = {
            "k": "open", "ts": _now_ts(), "coin": coin, "src": src,
            "venue": venue, "outcome": outcome,
            "open_ms": round(open_ms, 1), "entry": entry, "amount": amount,
            "lex": (listing_exchange or "").upper(),
            "pnl": None,   # заполнится позже поллером closed-pnl
        }
        with self._lock:
            self._buf.append(ev)
        self._dirty.set()

    def log_lag(self, coin: str, loser: str, winner: str, lag_ms: float) -> None:
        ev = {
            "k": "lag", "ts": _now_ts(), "coin": coin,
            "loser": loser, "winner": winner, "lag_ms": round(lag_ms, 1),
        }
        with self._lock:
            self._buf.append(ev)
        self._dirty.set()

    def log_pnl(self, coin: str, src: str, pnl_usdt: float,
                exit_price: float = 0.0) -> None:
        """
        Отложенный PnL закрытой позиции (приходит из closed-pnl поллера через
        минуты/часы после открытия). Журнал append-only, поэтому пишем отдельным
        событием k=pnl; при агрегации матчим к ближайшему open по coin+src.
        """
        ev = {
            "k": "pnl", "ts": _now_ts(), "coin": coin, "src": src,
            "pnl": round(pnl_usdt, 4), "exit": exit_price,
        }
        with self._lock:
            self._buf.append(ev)
        self._dirty.set()

    # ── персистентность ───────────────────────────────────────────
    def _flush(self) -> None:
        """Дописывает буфер в jsonl (append). Без блокировки hot-path."""
        with self._lock:
            pending = list(self._buf)
            self._buf.clear()
        if not pending:
            return
        try:
            self._events_path.parent.mkdir(parents=True, exist_ok=True)
            lines = b"\n".join(_dumps_line(ev) for ev in pending) + b"\n"
            with self._events_path.open("ab") as f:
                f.write(lines)
        except Exception as e:  # noqa: BLE001
            # При ошибке возвращаем в буфер — попробуем на след. flush.
            with self._lock:
                self._buf.extendleft(reversed(pending))
            print(f"[SIGSTATS] flush failed: {e!r}", flush=True)

    def _compact_if_needed(self) -> None:
        """Раз в сутки выкидывает строки старше RETENTION_DAYS (atomic rewrite)."""
        today = datetime.now(MSK).strftime("%Y-%m-%d")
        if today == self._last_compact_day:
            return
        self._last_compact_day = today
        try:
            if not self._events_path.exists():
                return
            cutoff = _now_ts() - RETENTION_DAYS * 86400
            kept: list[bytes] = []
            with self._events_path.open("rb") as f:
                for raw in f:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        ev = _loads(raw)
                        if int(ev.get("ts", 0)) >= cutoff:
                            kept.append(raw)
                    except Exception:  # noqa: BLE001
                        continue  # битую строку отбрасываем
            tmp = self._events_path.with_suffix(".jsonl.tmp")
            tmp.write_bytes(b"\n".join(kept) + (b"\n" if kept else b""))
            tmp.replace(self._events_path)
        except Exception as e:  # noqa: BLE001
            print(f"[SIGSTATS] compact failed: {e!r}", flush=True)

    def flush_loop(self, batch_sec: float = 10.0) -> None:
        """Фоновый writer: dirty-flag + батчинг. flush ДО clear."""
        while True:
            self._dirty.wait()
            time.sleep(batch_sec)
            self._flush()
            self._dirty.clear()
            self._compact_if_needed()

    # ── чтение событий за окно ────────────────────────────────────
    def _read_events(self, since_ts: int, until_ts: int | None = None) -> list[dict]:
        """Читает события с диска за [since_ts, until_ts). Включает буфер."""
        self._flush()   # гарантируем, что буфер на диске, чтобы не потерять
        out: list[dict] = []
        try:
            if self._events_path.exists():
                with self._events_path.open("rb") as f:
                    for raw in f:
                        raw = raw.strip()
                        if not raw:
                            continue
                        try:
                            ev = _loads(raw)
                        except Exception:  # noqa: BLE001
                            continue
                        ts = int(ev.get("ts", 0))
                        if ts < since_ts:
                            continue
                        if until_ts is not None and ts >= until_ts:
                            continue
                        out.append(ev)
        except Exception as e:  # noqa: BLE001
            print(f"[SIGSTATS] read failed: {e!r}", flush=True)
        return out

    # ── агрегация ─────────────────────────────────────────────────
    @staticmethod
    def _aggregate(events: list[dict]) -> dict:
        """
        Считает все метрики из списка событий:
          first_wins[src]       — кто первым (= число opened+любой open от src)
          appeared[src]         — сколько раз src вообще участвовал (win + lag-loss)
          opened[src]           — успешных открытий
          outcomes[src][o]      — разбивка исходов
          latency[src]          — список open_ms (opened)
          venue[v][ok|total]    — fill-rate по бирже
          lag[(loser,winner)]   — {sum,max,n}
          unique[src]           — монеты, которые видел ТОЛЬКО этот src
          listing_ex[ex]        — на какой бирже листили (UPBIT/BITHUMB/...)
          pnl_sum/pnl_n/pnl_win — сводка PnL по закрытым позициям
        """
        opens = [e for e in events if e.get("k") == "open"]
        lags  = [e for e in events if e.get("k") == "lag"]
        pnls  = [e for e in events if e.get("k") == "pnl"]

        first_wins: dict[str, int] = {}
        opened: dict[str, int] = {}
        outcomes: dict[str, dict[str, int]] = {}
        latency: dict[str, list[float]] = {}
        venue: dict[str, dict[str, int]] = {}
        listing_ex: dict[str, int] = {}      # биржа листинга → счёт
        # appeared = выигрыши + поражения (по lag-парам)
        appeared: dict[str, int] = {}
        # какие источники видели каждую монету (для unique)
        coin_srcs: dict[str, set[str]] = {}

        for e in opens:
            src = e.get("src", "?")
            outc = e.get("outcome", "failed")
            first_wins[src] = first_wins.get(src, 0) + 1
            appeared[src] = appeared.get(src, 0) + 1
            outcomes.setdefault(src, {})
            outcomes[src][outc] = outcomes[src].get(outc, 0) + 1
            coin_srcs.setdefault(e.get("coin", "?"), set()).add(src)
            # Биржа листинга — считаем только по успешно открытым (реально торгуем).
            lex = (e.get("lex") or "").upper()
            if outc == "opened" and lex:
                listing_ex[lex] = listing_ex.get(lex, 0) + 1
            if outc == "opened":
                opened[src] = opened.get(src, 0) + 1
                v = (e.get("venue") or "?").lower()
                venue.setdefault(v, {"ok": 0, "total": 0})
                venue[v]["ok"] += 1
                venue[v]["total"] += 1
                latency.setdefault(src, []).append(float(e.get("open_ms", 0)))
            else:
                # неуспех тоже считаем в venue.total если venue известна
                v = (e.get("venue") or "?").lower()
                if v != "?":
                    venue.setdefault(v, {"ok": 0, "total": 0})
                    venue[v]["total"] += 1

        # PnL: события k=pnl приходят отложенно. Суммируем по src.
        pnl_sum = 0.0
        pnl_n = 0
        pnl_win = 0
        pnl_by_src: dict[str, dict[str, float]] = {}
        for e in pnls:
            try:
                p = float(e.get("pnl", 0))
            except (TypeError, ValueError):
                continue
            src = e.get("src", "?")
            pnl_sum += p
            pnl_n += 1
            if p > 0:
                pnl_win += 1
            ent = pnl_by_src.setdefault(src, {"sum": 0.0, "n": 0, "win": 0})
            ent["sum"] += p
            ent["n"] += 1
            if p > 0:
                ent["win"] += 1

        lag_pairs: dict[tuple[str, str], dict[str, float]] = {}
        for e in lags:
            loser = e.get("loser", "?")
            winner = e.get("winner", "?")
            appeared[loser] = appeared.get(loser, 0) + 1
            coin_srcs.setdefault(e.get("coin", "?"), set()).add(loser)
            coin_srcs.setdefault(e.get("coin", "?"), set()).add(winner)
            key = (loser, winner)
            ms = float(e.get("lag_ms", 0))
            ent = lag_pairs.get(key)
            if ent is None:
                lag_pairs[key] = {"sum": ms, "max": ms, "n": 1}
            else:
                ent["sum"] += ms
                ent["max"] = max(ent["max"], ms)
                ent["n"] += 1

        # unique: монеты, которые видел ровно один источник
        unique: dict[str, int] = {}
        for coin, srcs in coin_srcs.items():
            if len(srcs) == 1:
                only = next(iter(srcs))
                unique[only] = unique.get(only, 0) + 1

        return {
            "n_open": len(opens),
            "n_lag": len(lags),
            "first_wins": first_wins,
            "opened": opened,
            "appeared": appeared,
            "outcomes": outcomes,
            "latency": latency,
            "venue": venue,
            "lag_pairs": lag_pairs,
            "unique": unique,
            "listing_ex": listing_ex,
            "pnl_sum": pnl_sum,
            "pnl_n": pnl_n,
            "pnl_win": pnl_win,
            "pnl_by_src": pnl_by_src,
        }

    # ── форматирование одной секции ───────────────────────────────
    def _format_section(self, title: str, events: list[dict], top: int = 8) -> list[str]:
        agg = self._aggregate(events)
        lines = [f"<b>━ {title} ━</b>"]
        if agg["n_open"] == 0 and agg["n_lag"] == 0:
            lines.append("  нет сигналов")
            return lines

        total_open = agg["n_open"]
        total_opened = sum(agg["opened"].values())
        lines.append(f"  сигналов: {total_open} | открыто: {total_opened}")

        # Кто первый (с win-rate и латентностью)
        winners = sorted(agg["first_wins"].items(), key=lambda kv: kv[1], reverse=True)[:top]
        if winners:
            lines.append("<b>Источники</b> (первый / win% / p50·p95):")
            for src, n in winners:
                appeared = agg["appeared"].get(src, n)
                winrate = _pct(n, appeared)
                lat = sorted(agg["latency"].get(src, []))
                if lat:
                    p50 = _percentile(lat, 0.50)
                    p95 = _percentile(lat, 0.95)
                    lat_s = f" | {p50:.0f}·{p95:.0f}мс"
                else:
                    lat_s = ""
                lines.append(f"  • {src}: {n} ({_pct(n, total_open):.0f}%) | win {winrate:.0f}%{lat_s}")

        # Fill-rate / провалы по источникам
        fails = []
        for src, oc in agg["outcomes"].items():
            bad = {k: v for k, v in oc.items() if k != "opened"}
            if bad:
                desc = " ".join(f"{_OUTCOME_LABEL.get(k, k)}×{v}" for k, v in bad.items())
                fails.append((sum(bad.values()), src, desc))
        fails.sort(reverse=True)
        if fails:
            lines.append("<b>Провалы:</b>")
            for _, src, desc in fails[:top]:
                lines.append(f"  • {src}: {desc}")

        # Биржа ЛИСТИНГА — где чаще листят/ловим (Upbit/Bithumb/Binance).
        if agg["listing_ex"]:
            total_lex = sum(agg["listing_ex"].values())
            lparts = []
            for ex, n in sorted(agg["listing_ex"].items(), key=lambda kv: kv[1], reverse=True):
                lparts.append(f"{ex}: {n} ({_pct(n, total_lex):.0f}%)")
            lines.append("<b>Биржа листинга:</b> " + " | ".join(lparts))

        # Fill-rate по биржам ИСПОЛНЕНИЯ (bybit/gate).
        if agg["venue"]:
            vparts = []
            for v, st in sorted(agg["venue"].items(), key=lambda kv: kv[1]["total"], reverse=True):
                vparts.append(f"{v}: {st['ok']}/{st['total']} ({_pct(st['ok'], st['total']):.0f}%)")
            lines.append("<b>Биржи (fill):</b> " + " | ".join(vparts))

        # PnL по закрытым позициям (приходит отложенно из closed-pnl поллера).
        if agg["pnl_n"] > 0:
            avg = agg["pnl_sum"] / agg["pnl_n"]
            wr = _pct(agg["pnl_win"], agg["pnl_n"])
            sign = "🟢" if agg["pnl_sum"] >= 0 else "🔴"
            lines.append(
                f"<b>PnL:</b> {sign} {agg['pnl_sum']:+.2f} USDT | "
                f"ср {avg:+.2f} | win {wr:.0f}% | закрыто {agg['pnl_n']}")
            # топ по источникам (кто приносит профит)
            pnl_src = sorted(agg["pnl_by_src"].items(),
                             key=lambda kv: kv[1]["sum"], reverse=True)[:top]
            for src, st in pnl_src:
                s_avg = st["sum"] / st["n"] if st["n"] else 0.0
                s_wr = _pct(st["win"], st["n"])
                lines.append(f"  • {src}: {st['sum']:+.2f} (ср {s_avg:+.2f}, win {s_wr:.0f}%, n={int(st['n'])})")

        # Топ лаг-пары
        lag_sorted = sorted(agg["lag_pairs"].items(), key=lambda kv: kv[1]["n"], reverse=True)[:top]
        if lag_sorted:
            lines.append("<b>Лаги</b> (loser ← winner | mean/max·n):")
            for (loser, winner), st in lag_sorted:
                mean = st["sum"] / st["n"] if st["n"] else 0.0
                lines.append(f"  • {loser} ← {winner}: {mean:.0f}/{st['max']:.0f}мс·{int(st['n'])}")

        # Уникальные сигналы (обоснование платных подписок)
        uniq = sorted(agg["unique"].items(), key=lambda kv: kv[1], reverse=True)[:top]
        uniq = [(s, n) for s, n in uniq if n > 0]
        if uniq:
            lines.append("<b>Уникальные</b> (только этот источник):")
            for src, n in uniq:
                lines.append(f"  • {src}: {n}")

        return lines

    # ── сборка полного отчёта ─────────────────────────────────────
    def build_report(self, last_report_ts: int) -> str:
        """Три секции: с прошлого отчёта / 7 полных дней / 30 дней.
        В последний день месяца добавляет полную статистику за месяц."""
        now = _now_ts()
        now_msk = datetime.now(MSK)

        # начало сегодняшних суток по МСК
        midnight_today = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
        # 7 ПОЛНЫХ дней = [midnight - 7д, midnight) — сегодня не входит
        since_7d = int((midnight_today - timedelta(days=7)).timestamp())
        until_7d = int(midnight_today.timestamp())
        since_30d = now - 30 * 86400

        kind_icon = "🟢 LISTING" if self._kind == "LISTING" else "🔴 DELIST"
        header = (f"📈 <b>СТАТИСТИКА — {kind_icon}</b>\n"
                  f"🕐 {now_msk.strftime('%Y-%m-%d %H:%M')} МСК")
        blocks = [header]

        # 1. С момента прошлого отчёта
        ev_since = self._read_events(last_report_ts)
        dt_h = (now - last_report_ts) / 3600.0
        blocks.append("\n".join(
            self._format_section(f"С прошлого отчёта ({dt_h:.0f}ч)", ev_since)))

        # 2. За последние 7 полных дней
        ev_7d = self._read_events(since_7d, until_7d)
        blocks.append("\n".join(self._format_section("7 полных дней", ev_7d)))

        # 3. За последние 30 дней
        ev_30d = self._read_events(since_30d)
        blocks.append("\n".join(self._format_section("30 дней", ev_30d)))

        # 4. Конец месяца — полная статистика за завершившийся месяц.
        if self._is_last_day_of_month(now_msk) and now_msk.hour == 22:
            m_since, m_until, m_name = self._month_bounds(now_msk)
            ev_month = self._read_events(m_since, m_until)
            blocks.append("\n".join(
                self._format_section(f"📅 ИТОГ МЕСЯЦА — {m_name}", ev_month, top=12)))

        return "\n\n".join(blocks)

    @staticmethod
    def _is_last_day_of_month(d: datetime) -> bool:
        return (d + timedelta(days=1)).month != d.month

    @staticmethod
    def _month_bounds(d: datetime) -> tuple[int, int, str]:
        """Границы текущего календарного месяца [начало, начало_след.) в МСК."""
        start = d.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            nxt = start.replace(year=start.year + 1, month=1)
        else:
            nxt = start.replace(month=start.month + 1)
        return int(start.timestamp()), int(nxt.timestamp()), start.strftime("%Y-%m")

    # ── last-report state (переживает рестарт) ────────────────────
    def _load_last_report_ts(self) -> int:
        try:
            if self._state_path.exists():
                data = _loads(self._state_path.read_bytes())
                return int(data.get("last_report_ts", 0))
        except Exception:  # noqa: BLE001
            pass
        return 0

    def _save_last_report_ts(self, ts: int) -> None:
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_path.with_suffix(".json.tmp")
            tmp.write_bytes(_dumps_line({"last_report_ts": ts}))
            tmp.replace(self._state_path)
        except Exception as e:  # noqa: BLE001
            print(f"[SIGSTATS] state save failed: {e!r}", flush=True)

    # ── планировщик 09:00 / 22:00 МСК ─────────────────────────────
    @staticmethod
    def _seconds_until_next_report() -> float:
        """Секунд до ближайших 09:00 или 22:00 МСК."""
        now = datetime.now(MSK)
        candidates = []
        for h in REPORT_HOURS_MSK:
            cand = now.replace(hour=h, minute=0, second=0, microsecond=0)
            if cand <= now:
                cand = cand + timedelta(days=1)
            candidates.append(cand)
        nxt = min(candidates)
        return max(1.0, (nxt - now).total_seconds())

    def report_scheduler_loop(self, send_fn) -> None:
        """
        Спит до ближайших 09:00/22:00 МСК, шлёт отчёт через send_fn(str).
        last_report_ts персистится → окно «с прошлого отчёта» переживает рестарт.
        """
        last_report_ts = self._load_last_report_ts()
        if last_report_ts == 0:
            # Первый запуск — точка отсчёта «с прошлого отчёта» = сейчас.
            last_report_ts = _now_ts()
            self._save_last_report_ts(last_report_ts)

        while True:
            sleep_s = self._seconds_until_next_report()
            time.sleep(sleep_s)
            try:
                report = self.build_report(last_report_ts)
                send_fn(report)
                last_report_ts = _now_ts()
                self._save_last_report_ts(last_report_ts)
            except Exception as e:  # noqa: BLE001
                print(f"[SIGSTATS] report failed: {e!r}", flush=True)
            # Защита от двойной отправки в одну минуту (sleep вернулся слишком рано).
            time.sleep(61)

    # ── хелперы для лог-бота / 6ч-итогов ──────────────────────────
    def latest_pnl(self, coin: str):
        """
        Последний реальный закрытый PnL (USDT) по монете из журнала, или None.
        Сканирует k=pnl события (их пишут BybitClosedPnL и GateClosedPnL через
        log_pnl). Читает ХВОСТ файла с конца и выходит на первом совпадении —
        не тянет весь журнал (вызывается на каждом 6ч-закрытии из потока).
        """
        if not coin:
            return None
        try:
            self._flush()
            if not self._events_path.exists():
                return None
            size = self._events_path.stat().st_size
            # Читаем хвост блоками с конца; свежие pnl-события в самом конце.
            chunk = 256 * 1024
            with self._events_path.open("rb") as f:
                pos = size
                buf = b""
                while pos > 0:
                    step = min(chunk, pos)
                    pos -= step
                    f.seek(pos)
                    buf = f.read(step) + buf
                    lines = buf.split(b"\n")
                    # первый (неполный) кусок оставляем на следующую итерацию
                    buf = lines[0] if pos > 0 else b""
                    tail = lines[1:] if pos > 0 else lines
                    for raw in reversed(tail):
                        raw = raw.strip()
                        if not raw:
                            continue
                        try:
                            ev = _loads(raw)
                        except Exception:  # noqa: BLE001
                            continue
                        if ev.get("k") == "pnl" and ev.get("coin") == coin:
                            try:
                                return float(ev.get("pnl"))
                            except (TypeError, ValueError):
                                return None
        except Exception as e:  # noqa: BLE001
            print(f"[SIGSTATS] latest_pnl failed: {e!r}", flush=True)
        return None

    @staticmethod
    def period_for_now(now=None) -> str:
        """Какой период слать авто-отчётом по дате: 1-го→месяц, пн→неделя, иначе день."""
        d = datetime.now(MSK) if now is None else datetime.fromtimestamp(int(now), MSK)
        if d.day == 1:
            return "30d"
        if d.weekday() == 0:          # понедельник
            return "7d"
        return "1d"

    def period_summary(self, window_key: str = "1d", now=None, top: int = 6) -> str:
        """
        Минималистичная сводка за скользящее окно (формат «моно-таблица»):
        иконка+период, PnL одной строкой, топ источников ровными колонками в
        <pre>, и хвост «открыто / биржи». Для кнопки бота и авто-отчёта.
        """
        win = PERIOD_WINDOWS.get(window_key, 86400)
        now = _now_ts() if now is None else int(now)
        agg = self._aggregate(self._read_events(now - win, now))
        icon = "🟢 LISTING" if self._kind == "LISTING" else "🔴 DELIST"
        title, range_lbl = _PERIOD_LABEL.get(window_key, ("?", ""))
        lines = [f"{icon} · <b>{title}</b>", f"└ {range_lbl}"]

        if agg["n_open"] == 0 and agg["n_lag"] == 0 and agg["pnl_n"] == 0:
            lines.append("")
            lines.append("<i>нет сигналов за период</i>")
            return "\n".join(lines)

        # PnL — главная строка.
        if agg["pnl_n"] > 0:
            wr = _pct(agg["pnl_win"], agg["pnl_n"])
            sign = "🟢" if agg["pnl_sum"] >= 0 else "🔴"
            lines.append("")
            lines.append(f"💰 {sign} <b>{agg['pnl_sum']:+.2f}$</b>   "
                         f"win {wr:.0f}%   {agg['pnl_n']} закр.")

        # Источники — ровными колонками (моно <pre>).
        winners = sorted(agg["first_wins"].items(), key=lambda kv: kv[1], reverse=True)[:top]
        if winners:
            rows = []
            for src, n in winners:
                appeared = agg["appeared"].get(src, n)
                wr = _pct(n, appeared)
                rows.append(f"{str(src)[:14]:<15}{n:>3}{wr:>5.0f}%")
            lines.append("")
            lines.append("📡 <b>Источники</b>")
            lines.append("<pre>" + _pre_escape("\n".join(rows)) + "</pre>")

        # Хвост: открыто / биржи исполнения.
        total_open = agg["n_open"]
        total_opened = sum(agg["opened"].values())
        tail = f"🎯 открыто {total_opened}/{total_open}"
        if agg["venue"]:
            vparts = " · ".join(
                f"{v.capitalize()} {st['ok']}"
                for v, st in sorted(agg["venue"].items(), key=lambda kv: kv[1]["ok"], reverse=True)
            )
            tail += f"   🏦 {vparts}"
        lines.append("")
        lines.append(tail)
        return "\n".join(lines)

    def month_section(self, top: int = 12) -> str:
        """Совместимость: сводка за 30 дней (тонкая обёртка period_summary)."""
        return self.period_summary("30d", top=top)
