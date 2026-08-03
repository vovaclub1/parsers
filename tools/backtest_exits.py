#!/usr/bin/env python3
"""
backtest_exits.py — оффлайн-бэктест стратегий выхода по записанным траекториям.

Берёт *_eval_paths.jsonl (полные склеенные траектории head+klines, пишет
eval-поллер парсера в момент 6ч-оценки) и симулирует стратегии выхода на реальных
прошлых входах → net PnL / win-rate → ранжированная таблица. Параметры выхода
подбираются по данным, а не вслепую в проде. (Можно скормить и сырой
*_price_paths.jsonl — это только «голова» 10 мин, формат тот же.)

Логика стратегий вынесена в tg/exit_strategies.py (единый источник истины,
общий с онлайн-оценкой 6ч-итогов). Здесь — загрузка, прогон, печать.

Запуск:
  python3 tools/backtest_exits.py --file state/delist_eval_paths.jsonl --side short
  python3 tools/backtest_exits.py --file state/eval_paths.jsonl --side long --sweep-trail
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys

# Чтобы импортировать tg.exit_strategies при запуске из любого каталога.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from tg.exit_strategies import (  # noqa: E402
    signed_move, net_pnl_pct, build_all_strategies, clean_samples,
)


def load_paths(filepath):
    """Читает jsonl → список dict с очищенными (dt, price) точками."""
    out = []
    skipped = 0
    with open(filepath, "rb") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except Exception:
                skipped += 1
                continue
            entry = row.get("entry")
            pts = clean_samples(row.get("samples"))
            if not entry or entry <= 0 or not pts:
                skipped += 1
                continue
            out.append({
                "coin": row.get("coin", "?"),
                "side": row.get("side", "long"),
                "src": row.get("src", "?"),
                "venue": row.get("venue", "?"),
                "entry": float(entry),
                "pts": pts,
                "partial": bool(row.get("partial", False)),
            })
    return out, skipped


def run(paths, strategies, leverage, taker):
    rows = []
    for name, fn in strategies.items():
        pnls, holds, grosses = [], [], []
        for p in paths:
            try:
                ex_dt, ex_px = fn(p["pts"], p["side"], p["entry"])
            except Exception as e:  # noqa: BLE001
                print("  [warn] %s упала на %s: %r" % (name, p.get("coin"), e))
                continue
            pnls.append(net_pnl_pct(p["side"], p["entry"], ex_px, leverage, taker))
            holds.append(ex_dt)
            grosses.append(100.0 * signed_move(p["side"], p["entry"], ex_px))
        if not pnls:
            continue
        rows.append({
            "strategy": name,
            "n": len(pnls),
            "total": sum(pnls),
            "mean": statistics.mean(pnls),
            "median": statistics.median(pnls),
            "winrate": 100.0 * sum(1 for x in pnls if x > 0) / len(pnls),
            "worst": min(pnls),
            "best": max(pnls),
            "avg_gross": statistics.mean(grosses),
            "avg_hold_s": statistics.mean(holds),
        })
    rows.sort(key=lambda r: r["total"], reverse=True)
    return rows


def print_table(rows, leverage, taker):
    if not rows:
        print("Нет данных для прогона (пустой/нерелевантный файл).")
        return
    hdr = ("%-18s %4s %9s %8s %8s %7s %8s %8s %9s %8s"
           % ("strategy", "n", "total%", "mean%", "med%", "win%",
              "worst%", "best%", "gross%", "hold_s"))
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print("%-18s %4d %9.1f %8.2f %8.2f %7.0f %8.1f %8.1f %9.2f %8.0f"
              % (r["strategy"], r["n"], r["total"], r["mean"], r["median"],
                 r["winrate"], r["worst"], r["best"], r["avg_gross"],
                 r["avg_hold_s"]))
    print("-" * len(hdr))
    print("Плечо=%gx, тейкер=%.4f%% (вход+выход). %% = доходность на маржу."
          % (leverage, taker * 100))


def main():
    ap = argparse.ArgumentParser(description="Бэктест стратегий выхода по траекториям цены.")
    ap.add_argument("--file", required=True, help="путь к *_price_paths.jsonl")
    ap.add_argument("--side", choices=("long", "short", "both"), default="both")
    ap.add_argument("--leverage", type=float, default=10.0)
    ap.add_argument("--taker", type=float, default=0.00055,
                    help="тейкер-комиссия долей (Bybit ~0.00055)")
    ap.add_argument("--src", default=None, help="фильтр по источнику (подстрока)")
    ap.add_argument("--sweep-trail", action="store_true",
                    help="добавить перебор дистанции трейлинга и фикс TP/SL")
    args = ap.parse_args()

    paths, skipped = load_paths(args.file)
    print("Загружено траекторий: %d (пропущено битых/пустых: %d)" % (len(paths), skipped))
    if args.src:
        paths = [p for p in paths if args.src.lower() in str(p.get("src", "")).lower()]
        print("После фильтра src=%r: %d" % (args.src, len(paths)))

    sides = ("long", "short") if args.side == "both" else (args.side,)
    for side in sides:
        sub = [p for p in paths if p["side"] == side]
        print("\n=== SIDE=%s | траекторий: %d ===" % (side.upper(), len(sub)))
        if not sub:
            continue
        strategies = build_all_strategies(side, sweep_trail=args.sweep_trail)
        rows = run(sub, strategies, args.leverage, args.taker)
        print_table(rows, args.leverage, args.taker)


if __name__ == "__main__":
    main()
