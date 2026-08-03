from __future__ import annotations

# ── strategy_eval.py ──────────────────────────────────────────────
# Онлайн-оценка стратегий выхода на момент закрытия 6ч-окна рекордера.
# Для каждого завершённого входа:
#   1. симулирует НАБОР кандидатных стратегий на записанной траектории;
#   2. winner = стратегия с НАИБОЛЬШИМ PnL на этой сделке → +1 в винрейт;
#   3. копит персистентный винрейт по каждой стратегии (переживает рестарт);
#   4. дописывает результат (+готовый текст карточки) в *_strategy_results.jsonl;
#   5. возвращает текст 6ч-итогов для отправки в TG.
#
# Винрейт стратегии = wins/total, где total растёт для каждой стратегии,
# участвовавшей в сделке (честно к стратегиям, добавленным позже).
#
# Чистый stdlib + tg.exit_strategies. Без импорта парсеров.
# ─────────────────────────────────────────────────────────────────

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from tg.exit_strategies import (
    simulate_candidates, clean_samples, build_candidates, strategy_help,
    slug_for,
)
from tg.shadow_eval import evaluate_directions
from research.cohorts import classify_subtype
try:
    from config.config import TG_LOG_BOT_TOKEN
except Exception:  # noqa: BLE001
    TG_LOG_BOT_TOKEN = ""


def stitch(head_samples, tail_klines, entry_ts):
    """
    Склеивает «голову» рекордера (первые ~10 мин, dt от входа) с «хвостом»
    klines-свечей (абсолютные ts) в единую траекторию [[dt, price], ...].
    Хвостовые точки с dt <= последнего dt головы отбрасываем (перекрытие).
    """
    pts = []
    last_dt = -1.0
    for s in head_samples or []:
        if not isinstance(s, (list, tuple)) or len(s) < 2:
            continue
        dt, price = s[0], s[1]
        if price is None:
            continue
        try:
            dt = float(dt)
            price = float(price)
        except (TypeError, ValueError):
            continue
        pts.append([dt, price])
        if dt > last_dt:
            last_dt = dt
    for kl in tail_klines or []:
        try:
            ts, close = kl[0], kl[1]
            dt = float(int(ts) - int(entry_ts))
            close = float(close)
        except (TypeError, ValueError, IndexError):
            continue
        if dt <= last_dt:
            continue
        pts.append([dt, close])
    pts.sort(key=lambda x: x[0])
    return pts

try:
    import orjson as _orjson  # type: ignore[import-not-found]
    def _dumps(o, indent=False):
        opt = _orjson.OPT_INDENT_2 if indent else 0
        return _orjson.dumps(o, option=opt)
    def _loads(b):
        return _orjson.loads(b)
except ImportError:
    def _dumps(o, indent=False):
        return json.dumps(o, indent=2 if indent else None, ensure_ascii=False).encode()
    def _loads(b):
        if isinstance(b, (bytes, bytearray)):
            b = b.decode()
        return json.loads(b)


MSK = timezone(timedelta(hours=3))


# ── deep-link имя бота (для тапаемых ссылок в тексте) ─────────────
_bot_username_cache = None      # None=не пробовали, ""=нет/ошибка, "name"=ок


def _bot_username():
    """Username лог-бота через getMe (кэш). '' если недоступен → имена без ссылок."""
    global _bot_username_cache
    if _bot_username_cache is not None:
        return _bot_username_cache
    _bot_username_cache = ""
    if not TG_LOG_BOT_TOKEN:
        return ""
    try:
        r = requests.get(f"https://api.telegram.org/bot{TG_LOG_BOT_TOKEN}/getMe", timeout=5)
        j = r.json()
        if j.get("ok"):
            _bot_username_cache = (j.get("result") or {}).get("username") or ""
    except Exception as e:  # noqa: BLE001
        print(f"[STRAT-EVAL] getMe failed: {e!r}", flush=True)
    return _bot_username_cache


# ── винрейт из журнала результатов ────────────────────────────────
def winrates_from_results(results_path, since_ts=0, extra_winner=None,
                          event_type=None, side=None, strategy_version=None):
    """
    Считает {name: (wins, total)} из *_strategy_results.jsonl за окно
    [since_ts, ∞). wins = сколько раз стратегия была winner; total = число
    сделок в окне (каждая стратегия участвует в каждой). extra_winner —
    «текущая» сделка (ещё не записанная в файл): +1 total и +1 ей в wins.
    """
    wins, total = {}, 0
    try:
        rp = Path(results_path)
        if rp.exists():
            with rp.open("rb") as f:
                for raw in f:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        row = _loads(raw)
                    except Exception:  # noqa: BLE001
                        continue
                    if int(row.get("complete_ts", 0)) < since_ts:
                        continue
                    # Не смешиваем результаты разных торговых режимов. Legacy-
                    # записи без cohort metadata остаются в журнале, но не
                    # участвуют в статистике нового режима.
                    if event_type is not None and row.get("event_type") != event_type:
                        continue
                    if side is not None and row.get("side") != side:
                        continue
                    if (strategy_version is not None
                            and row.get("strategy_version") != strategy_version):
                        continue
                    total += 1
                    w = row.get("winner")
                    if w:
                        wins[w] = wins.get(w, 0) + 1
    except Exception as e:  # noqa: BLE001
        print(f"[STRAT-EVAL] winrates read failed: {e!r}", flush=True)
    if extra_winner is not None:
        total += 1
        wins[extra_winner] = wins.get(extra_winner, 0) + 1
    return wins, total


# ── рендер карточки 6ч-итогов ─────────────────────────────────────
def _strat_label(name, bot_username):
    """Имя стратегии как тапаемая ссылка (deep-link) или plain, если бота нет."""
    if bot_username:
        return f'<a href="https://t.me/{bot_username}?start=s_{slug_for(name)}">{name}</a>'
    return name


def render_card(side, coin, complete_ts, src, actual_pnl, sims, winner,
                wins, total, margin_usdt, bot_username=None,
                event_type="unknown", strategy_version="legacy"):
    """
    Карточка 6ч-итогов. Имена стратегий — тапаемые ссылки (тап → бот шлёт
    описание). Рядом с % — доллары (pct% × маржа сделки). wins/total — карта
    винрейта и общее число сделок (из журнала, вкл. текущую).
    """
    when = datetime.fromtimestamp(int(complete_ts), MSK).strftime("%d.%m %H:%M")
    event_lbl = {
        "listing": "LISTING",
        "delisting": "DELIST",
    }.get(event_type, str(event_type or "UNKNOWN").upper())
    side_lbl = f"{'🟢' if side == 'long' else '🔴'} {event_lbl} {side.upper()}"
    pnl_str = "н/д" if actual_pnl is None else f"{actual_pnl:+.2f}$"
    # Стиль = как в стате (Format A): эмодзи-заголовок, отступ "└", чистые строки.
    lines = [
        "🧪 <b>ИТОГИ 6ч</b>",
        f"{side_lbl} · <b>{coin}</b>",
        f"🕐 {when} МСК · 📡 {src or '?'}",
        f"💰 наш PnL: {pnl_str}",
    ]
    if not sims:
        lines.append("")
        lines.append("⚠️ нет данных цены за окно")
        return "\n".join(lines)

    lines.append("")
    lines.append("📊 <b>Стратегии</b> · тапни имя")
    for name in sorted(sims.keys(), key=lambda k: sims[k]["pnl"], reverse=True):
        pct = sims[name]["pnl"]
        dollar = pct / 100.0 * margin_usdt
        w = wins.get(name, 0)
        wr = (100.0 * w / total) if total else 0.0
        mark = "🥇" if name == winner else "▫️"
        lines.append(
            f"{mark} {_strat_label(name, bot_username)} · "
            f"{pct:+.0f}% · {dollar:+.1f}$ · WR {wr:.0f}%"
        )
    return "\n".join(lines)


# ── главная функция оценки (вызывается из парсера по 6ч-завершению) ─
def evaluate(record, actual_pnl, results_path,
             leverage=10.0, taker=0.00055, margin_usdt=14.0):
    """
    record: dict от рекордера {coin, side, src, venue, entry, entry_ts, samples}.
    actual_pnl: реальный закрытый PnL (USDT) или None.
    margin_usdt: маржа сделки (для $-расчёта стратегий: листинг 14, делист 10).
    Винрейт считается из *_strategy_results.jsonl (вкл. текущую сделку).
    Возвращает текст карточки 6ч-итогов (для tg_log).
    """
    coin = record.get("coin", "?")
    side = record.get("side", "long")
    event_type = record.get("event_type", "unknown")
    strategy_version = record.get("strategy_version", "legacy")
    src = record.get("src", "?")
    venue = record.get("venue", "?")
    entry = float(record.get("entry") or 0.0)
    entry_ts = int(record.get("entry_ts") or 0)
    complete_ts = int(time.time())

    pts = clean_samples(record.get("samples"))
    sims = simulate_candidates(pts, side, entry, leverage, taker) if (pts and entry > 0) else {}
    direction_shadow = {}
    if pts and entry > 0 and event_type in ("listing", "delisting"):
        direction_shadow = evaluate_directions(
            event_type, side, entry, record.get("samples"), leverage, taker,
        )

    # winner = макс PnL; тай → первый по порядку кандидатов.
    winner = None
    if sims:
        best = None
        for name in sims:  # порядок = порядок build_candidates
            pnl = sims[name]["pnl"]
            if best is None or pnl > best:
                best = pnl
                winner = name

    # Винрейт из журнала (all-time) + текущая сделка.
    wins, total = winrates_from_results(
        results_path, since_ts=0, extra_winner=winner,
        event_type=event_type, side=side, strategy_version=strategy_version,
    )
    text = render_card(side, coin, complete_ts, src, actual_pnl, sims, winner,
                       wins, total, margin_usdt, bot_username=_bot_username(),
                       event_type=event_type, strategy_version=strategy_version)

    # дописываем результат (с готовым текстом — деталь по тапу = тот же текст)
    try:
        row = {
            "coin": coin, "side": side, "event_type": event_type,
            "event_subtype": classify_subtype(src),
            "strategy_version": strategy_version, "src": src, "venue": venue,
            "entry_ts": entry_ts, "complete_ts": complete_ts,
            "actual_pnl": actual_pnl, "margin": margin_usdt,
            "strategies": {k: round(v["pnl"], 2) for k, v in sims.items()},
            "direction_shadow": direction_shadow,
            "winner": winner,
            "text": text,
        }
        rp = Path(results_path)
        rp.parent.mkdir(parents=True, exist_ok=True)
        with rp.open("ab") as f:
            f.write(_dumps(row) + b"\n")
    except Exception as e:  # noqa: BLE001
        print(f"[STRAT-EVAL] results write failed: {e!r}", flush=True)

    return text
