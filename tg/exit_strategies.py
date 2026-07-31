from __future__ import annotations

# ── exit_strategies.py ────────────────────────────────────────────
# Единый «движок» симуляции стратегий выхода по записанной траектории цены.
# Чистый stdlib, БЕЗ импорта парсеров (3.9-safe) — используется и оффлайн-
# бэктестом (tools/backtest_exits.py), и онлайн-оценкой 6ч-итогов
# (tg/strategy_eval.py). Единый источник истины, чтобы логика не разъезжалась.
#
# Стратегия выхода: callable(pts, side, entry) -> (exit_dt, exit_price)
#   pts  = [(dt_s, price), ...] отсортирован по dt, БЕЗ None.
#   side = "long" | "short".
# ─────────────────────────────────────────────────────────────────

from functools import partial


def signed_move(side: str, entry: float, price: float) -> float:
    """Доходность в сторону позиции: long растёт, short падает."""
    if entry <= 0:
        return 0.0
    return (price - entry) / entry if side == "long" else (entry - price) / entry


# ── стратегии выхода ──────────────────────────────────────────────
def exit_trailing(pts, side, entry, sl, trail, act):
    """
    Прод-логика: аварийный SL -sl; после движения +act активируется трейлинг
    на дистанции trail от пика (в сторону профита). Что раньше тронет.
    """
    activated = False
    peak = entry  # лучшая цена в сторону профита (max для long, min для short)
    for dt, price in pts:
        move = signed_move(side, entry, price)
        if move <= -sl:
            return dt, price
        if side == "long":
            if price > peak:
                peak = price
        else:
            if price < peak:
                peak = price
        if not activated and move >= act:
            activated = True
        if activated:
            if side == "long":
                if price <= peak * (1.0 - trail):
                    return dt, price
            else:
                if price >= peak * (1.0 + trail):
                    return dt, price
    return pts[-1]


def exit_fixed_tp_sl(pts, side, entry, tp, sl):
    """Фикс тейк +tp / стоп -sl (что раньше тронет)."""
    for dt, price in pts:
        move = signed_move(side, entry, price)
        if move >= tp:
            return dt, price
        if move <= -sl:
            return dt, price
    return pts[-1]


def exit_buyhold(pts, side, entry):
    """Бенчмарк: держим до конца окна."""
    return pts[-1]


def exit_immediate(pts, side, entry):
    """Бенчмарк: выход на первом сэмпле (нижняя планка / эффект первого тика)."""
    return pts[0]


def exit_oracle_best(pts, side, entry):
    """Оракул (хайндсайт): лучший возможный выход. Верхняя планка движения."""
    best_dt, best_px = pts[0]
    best_move = signed_move(side, entry, best_px)
    for dt, price in pts:
        m = signed_move(side, entry, price)
        if m > best_move:
            best_move = m
            best_dt, best_px = dt, price
    return best_dt, best_px


# ── multi-exit: эффективная цена выхода ───────────────────────────
# Стратегии с ЧАСТИЧНЫМИ выходами (лесенка/раннер) вписываются в общую
# (exit_dt, exit_price)-модель так: считаем взвешенное движение по долям и
# возвращаем «эффективную» цену, дающую то же движение. Тогда net_pnl_pct и
# рендер работают без изменений. (Доп. комиссии за лишние выходы малы —
# 2*taker на ~N выходов; на фоне десятков % движения — пренебрежимо.)
def _price_from_move(side, entry, m):
    return entry * (1.0 + m) if side == "long" else entry * (1.0 - m)


def _effective_exit(side, entry, fills, last_dt):
    """fills: список (weight, exit_price). Возвращает (last_dt, eff_price)."""
    tw = sum(w for w, _ in fills) or 1.0
    m_eff = sum(w * signed_move(side, entry, px) for w, px in fills) / tw
    return last_dt, _price_from_move(side, entry, m_eff)


# ── ATR-трейлинг: дистанция = k × волатильность ───────────────────
def _atr_frac(pts, entry, warmup_n):
    """Оценка волатильности как средн. |Δцены| между соседними сэмплами на
    разогреве, в долях entry. Fallback 0 если данных мало."""
    seg = pts[:warmup_n] if warmup_n and len(pts) > warmup_n else pts
    if len(seg) < 2 or entry <= 0:
        return 0.0
    diffs = [abs(seg[i][1] - seg[i - 1][1]) for i in range(1, len(seg))]
    return (sum(diffs) / len(diffs)) / entry


def exit_atr_trailing(pts, side, entry, k=2.5, sl=0.01,
                      warmup_n=30, lo=0.005, hi=0.20):
    """Трейлинг с динамической дистанцией k×ATR (зажата в [lo, hi]). Активация
    сразу после +дистанция. Шире на волатильных монетах, туже на спокойных."""
    atr = _atr_frac(pts, entry, warmup_n)
    trail = min(hi, max(lo, k * atr)) if atr > 0 else 0.02
    return exit_trailing(pts, side, entry, sl=sl, trail=trail, act=trail)


# ── Лесенка скейл-аутов с раннером ────────────────────────────────
def _ladder_levels(side):
    """(пороги_тейков, веса, доля_раннера, трейлинг_раннера) по стороне."""
    if side == "long":
        return (0.10, 0.30, 0.60), (0.40, 0.30, 0.20), 0.10, 0.15
    return (0.05, 0.12, 0.25), (0.40, 0.30, 0.20), 0.10, 0.08


def exit_ladder(pts, side, entry):
    """
    Частичные выходы по достижении порогов (40/30/20%) + раннер (10%) на
    широком трейлинге. Если порог не достигнут — его доля выходит вместе с
    раннером (в конце/по трейлингу). Возвращает эффективную цену.
    """
    tps, weights, run_w, run_trail = _ladder_levels(side)
    fills = []
    taken = [False] * len(tps)
    peak = entry
    last_dt = pts[-1][0]
    runner_px = pts[-1][1]
    runner_done = False
    for dt, price in pts:
        move = signed_move(side, entry, price)
        # обновляем пик для раннер-трейлинга
        if side == "long":
            peak = max(peak, price)
        else:
            peak = min(peak, price)
        # частичные тейки
        for i, tp in enumerate(tps):
            if not taken[i] and move >= tp:
                taken[i] = True
                fills.append((weights[i], price))
                last_dt = dt
        # раннер: трейлинг от пика после того как взят хотя бы первый тейк
        if not runner_done and taken[0]:
            if side == "long" and price <= peak * (1.0 - run_trail):
                runner_px = price
                runner_done = True
                last_dt = dt
            elif side == "short" and price >= peak * (1.0 + run_trail):
                runner_px = price
                runner_done = True
                last_dt = dt
    # доли не взятых тейков + раннер закрываются по runner_px (трейл/конец окна)
    untaken_w = sum(weights[i] for i in range(len(tps)) if not taken[i])
    fills.append((run_w + untaken_w, runner_px))
    return _effective_exit(side, entry, fills, last_dt)


# ── Трейлинг основной доли + раннер (вариант пользователя) ────────
def exit_trail_runner(pts, side, entry, main_w=0.8, runner_trail=0.10):
    """
    Основную долю (main_w) закрываем по текущему live-трейлингу, остаток —
    раннер на широком трейлинге runner_trail. «Выходим в начале по трейлингу,
    раннер оставляем ехать.»
    """
    lp = live_params(side)
    main_dt, main_px = exit_trailing(pts, side, entry,
                                     sl=lp["sl"], trail=lp["trail"], act=lp["act"])
    # раннер продолжает от точки выхода основной доли до своего трейла/конца
    run_dt, run_px = exit_trailing(pts, side, entry,
                                   sl=lp["sl"], trail=runner_trail, act=lp["act"])
    fills = [(main_w, main_px), (1.0 - main_w, run_px)]
    return _effective_exit(side, entry, fills, max(main_dt, run_dt))


# ── Breakeven → trailing ──────────────────────────────────────────
def exit_breakeven(pts, side, entry, sl=0.01, be_act=0.03, be_lock=0.001,
                   trail=0.02):
    """
    Сначала аварийный SL. После +be_act двигаем стоп в безубыток (+be_lock) и
    включаем трейлинг trail. Защищает прибыль от разворота в минус.
    """
    armed = False
    peak = entry
    for dt, price in pts:
        move = signed_move(side, entry, price)
        if not armed:
            if move <= -sl:
                return dt, price
            if move >= be_act:
                armed = True
                peak = price
            continue
        # armed: трейлинг от пика + защита БУ
        if side == "long":
            peak = max(peak, price)
            be = entry * (1.0 + be_lock)
            stop = max(be, peak * (1.0 - trail))
            if price <= stop:
                return dt, price
        else:
            peak = min(peak, price)
            be = entry * (1.0 - be_lock)
            stop = min(be, peak * (1.0 + trail))
            if price >= stop:
                return dt, price
    return pts[-1]


# ── Time-stop: выход через N секунд ───────────────────────────────
def exit_time_stop(pts, side, entry, hold_s=300.0):
    """Выход на первом сэмпле с dt >= hold_s (ловит «первую свечу»)."""
    for dt, price in pts:
        if dt >= hold_s:
            return dt, price
    return pts[-1]


# ── Грид: база едет тренд + сетка собирает осцилляции ─────────────
def exit_grid(pts, side, entry, step=0.03, lot=0.25, max_lots=6,
              hard_sl=0.20, taker=0.00055):
    """
    Делист/листинг-грид. Базовая позиция (вес 1) едет весь тренд в сторону
    профита; сетка ДОБИРАЕТ по lot на откатах ПРОТИВ профита (через каждые
    step) и ОТКУПАЕТ при возврате на step в сторону профита — собирает «пилу».
    hard_sl — аварийный выход всей сетки при движении против на hard_sl.
    Возвращает эффективную цену, кодирующую суммарный PnL на базовую маржу
    (доп. комиссии ног учтены грубо). Модель симметрична для long/short.
    """
    def against(level):
        # цена на `level` шагов ПРОТИВ профита от entry
        return entry * (1 - level * step) if side == "long" else entry * (1 + level * step)

    realized = 0.0          # реализованный PnL грид-лотов (доля базовой маржи)
    open_adds = []          # цены входа активных добор-лотов
    n_adds = 0
    next_level = 1
    base_exit = pts[-1][1]
    last_dt = pts[-1][0]
    stopped = False
    for dt, price in pts:
        m = signed_move(side, entry, price)
        if m <= -hard_sl:           # вынос против профита — рубим всё
            base_exit = price
            for ep in open_adds:
                realized += lot * signed_move(side, ep, price)
            open_adds = []
            last_dt = dt
            stopped = True
            break
        # добор против профита по сетке
        while n_adds < max_lots:
            lvl = against(next_level)
            crossed = (price <= lvl) if side == "long" else (price >= lvl)
            if not crossed:
                break
            open_adds.append(lvl)
            n_adds += 1
            next_level += 1
        # откуп лотов на возврате в сторону профита на step
        kept = []
        for ep in open_adds:
            tp = ep * (1 + step) if side == "long" else ep * (1 - step)
            hit = (price >= tp) if side == "long" else (price <= tp)
            if hit:
                realized += lot * signed_move(side, ep, tp)
            else:
                kept.append(ep)
        open_adds = kept

    total = signed_move(side, entry, base_exit) + realized
    if not stopped:
        for ep in open_adds:        # незакрытые лоты — по последней цене
            total += lot * signed_move(side, ep, base_exit)
    total -= 2.0 * lot * n_adds * taker   # комиссии ног (грубо)
    return last_dt, _price_from_move(side, entry, total)


# ── Ratchet: ступенчатый стоп по уровням прибыли ──────────────────
def exit_ratchet(pts, side, entry, levels=(0.05, 0.10, 0.20, 0.40), sl=0.01):
    """
    На каждом новом уровне прибыли подтягиваем стоп к предыдущему уровню
    (первый уровень → безубыток). Фиксирует прибыль ступенями всей позицией.
    """
    locked = -sl                # текущий уровень стопа (в долях движения)
    for dt, price in pts:
        m = signed_move(side, entry, price)
        if m <= locked:
            return dt, price
        for i, lv in enumerate(levels):
            if m >= lv:
                locked = max(locked, levels[i - 1] if i > 0 else 0.0)
    return pts[-1]


# ── Momentum: выход при затухании импульса ────────────────────────
def exit_momentum(pts, side, entry, patience_n=6, sl=0.01):
    """
    Держим, пока цена обновляет экстремум в сторону профита. После выхода в
    плюс, если patience_n сэмплов подряд без нового экстремума — выходим
    (импульс выдохся). Плюс аварийный SL.
    """
    best_m = signed_move(side, entry, pts[0][1])
    stall = 0
    for dt, price in pts:
        m = signed_move(side, entry, price)
        if m <= -sl:
            return dt, price
        if m > best_m:
            best_m = m
            stall = 0
        else:
            stall += 1
            if stall >= patience_n and best_m > 0:
                return dt, price
    return pts[-1]


# ── PnL ───────────────────────────────────────────────────────────
def net_pnl_pct(side, entry, exit_price, leverage, taker):
    """Net доходность на маржу в % (плечо + тейкер-комиссия вход+выход)."""
    gross = signed_move(side, entry, exit_price)
    fees = 2.0 * taker
    return 100.0 * leverage * (gross - fees)


# ── наборы стратегий ──────────────────────────────────────────────
def live_params(side):
    """Текущие прод-параметры выхода по стороне (listing_api.py / config.py)."""
    if side == "long":
        return {"sl": 0.01, "trail": 0.01, "act": 0.01}
    return {"sl": 0.01, "trail": 0.005, "act": 0.005}


def build_candidates(side):
    """
    Набор КАНДИДАТНЫХ стратегий для 6ч-итогов и сравнения винрейта.
    name -> callable(pts, side, entry) -> (exit_dt, exit_price).
    «live» — симуляция текущих прод-правил (для apples-to-apples сравнения).
    Расширяется свободно (лесенка/ATR/грид — позже).
    """
    lp = live_params(side)
    return {
        "live":      partial(exit_trailing, sl=lp["sl"], trail=lp["trail"], act=lp["act"]),
        "trail=2%":  partial(exit_trailing, sl=lp["sl"], trail=0.02, act=lp["act"]),
        "trail=5%":  partial(exit_trailing, sl=lp["sl"], trail=0.05, act=lp["act"]),
        "fixTP=10%": partial(exit_fixed_tp_sl, tp=0.10, sl=0.01),
        "atr_trail": exit_atr_trailing,
        "ladder":    exit_ladder,
        "trail+run": exit_trail_runner,
        "be→trail":  exit_breakeven,
        "time=5m":   exit_time_stop,
        "grid":      exit_grid,
        "ratchet":   exit_ratchet,
        "momentum":  exit_momentum,
    }


def build_all_strategies(side, sweep_trail=False):
    """
    Полный набор для оффлайн-бэктеста (кандидаты + бенчмарки + опц. sweep).
    """
    lp = live_params(side)
    strategies = dict(build_candidates(side))
    strategies["buyhold_window"] = exit_buyhold
    strategies["immediate"] = exit_immediate
    strategies["oracle_best"] = exit_oracle_best
    if sweep_trail:
        for tr in (0.005, 0.01, 0.02, 0.05, 0.10, 0.20):
            strategies["trail=%.1f%%" % (tr * 100)] = partial(
                exit_trailing, sl=lp["sl"], trail=tr, act=lp["act"])
        for tp in (0.05, 0.10, 0.20):
            strategies["fixTP=%.0f%%/SL1%%" % (tp * 100)] = partial(
                exit_fixed_tp_sl, tp=tp, sl=0.01)
    return strategies


# ── человекочитаемые описания (для кнопки «Стратегии» в боте) ─────
STRATEGY_DESCRIPTIONS = {
    "live":
        "Текущий прод-выход. Трейлинг-стоп от пика (лонг 1% / шорт 0.5%), "
        "активация после +1%/+0.5%, аварийный SL 1%. Фиксирует на первом "
        "откате от максимума — ловит «первую быструю свечу», но отдаёт хвост "
        "сильных движений.",
    "trail=2%":
        "Трейлинг-стоп 2% от пика. Даёт цене больше места, чем live — ловит "
        "движение покрупнее, но на откате отдаёт чуть больше прибыли.",
    "trail=5%":
        "Широкий трейлинг 5%. Для сильных трендов: почти не выбивает на шуме, "
        "едет далеко, но фиксирует позднее и отдаёт больше на развороте.",
    "fixTP=10%":
        "Фиксированный тейк +10% / стоп −1%. Выходит на первом достижении "
        "+10% — простая цель, не пытается поймать максимум.",
    "atr_trail":
        "ATR-трейлинг: дистанция стопа = 2.5×волатильности (по первым ~5 мин), "
        "зажата в 0.5–20%. Сам РАСШИРЯЕТСЯ на диких монетах и СУЖАЕТСЯ на "
        "спокойных — не выбивает на шуме волатильного листинга.",
    "ladder":
        "Лесенка скейл-аутов: частичные выходы 40/30/20% на порогах прибыли "
        "(+10/+30/+60% лонг) + раннер 10% на широком трейлинге. Фиксирует "
        "прибыль ступенями и оставляет «лунный» хвост на сильный памп.",
    "trail+run":
        "Трейлинг + раннер: 80% позиции выходит по live-трейлингу (рано "
        "снимает риск), 20% — раннер едет дальше на широком трейлинге 10%.",
    "be→trail":
        "Безубыток → трейлинг: после +3% стоп переносится в безубыток, дальше "
        "трейлинг 2%. Защищает прибыль от разворота сделки в минус.",
    "time=5m":
        "Time-stop: выход ровно через 5 минут после входа. Ловит «первую "
        "свечу» пампа листинга / быстрый слив делиста, игнорируя откаты.",
    "grid":
        "Грид: базовая позиция едет весь тренд, а сетка ДОБИРАЕТ на откатах "
        "против профита и ОТКУПАЕТ на возвратах — собирает «пилу». ⚠️ Опасен "
        "на безоткатном выносе против позиции — усиливает убыток.",
    "ratchet":
        "Ступенчатый стоп: на каждом новом уровне прибыли (+5/+10/+20/+40%) "
        "подтягивает стоп к предыдущему уровню. Фиксирует прибыль ступенями "
        "всей позицией.",
    "momentum":
        "Выход при затухании импульса: держим, пока цена обновляет экстремум; "
        "если N сэмплов подряд без нового максимума (после выхода в плюс) — "
        "выходим. Плюс аварийный SL.",
    # бенчмарки (в бэктесте)
    "oracle_best":
        "Оракул (хайндсайт): идеальный выход в лучшей точке. Верхняя планка — "
        "сколько вообще было в движении. Не торгуемая, только ориентир.",
    "buyhold_window":
        "Бенчмарк: держим до конца окна (6ч). Показывает, сколько осталось бы "
        "без активного выхода.",
    "immediate":
        "Бенчмарк: выход на первом же сэмпле. Нижняя планка / эффект первого "
        "тика и проскальзывания входа.",
}


def strategy_help(name: str) -> str:
    """Описание стратегии по имени (для кнопки бота). '' если неизвестна."""
    return STRATEGY_DESCRIPTIONS.get(name, "")


# ── slug ↔ name (для deep-link t.me/<bot>?start=s_<slug>) ─────────
import re as _re


def slug_for(name: str) -> str:
    """Имя стратегии → slug для deep-link (только [a-z0-9])."""
    return _re.sub(r"[^a-z0-9]+", "", name.lower())


# Полный набор имён (кандидаты + бенчмарки) → slug, и обратно.
_ALL_STRAT_NAMES = list(build_candidates("long").keys()) + [
    "buyhold_window", "immediate", "oracle_best",
]
STRATEGY_SLUGS = {n: slug_for(n) for n in _ALL_STRAT_NAMES}
SLUG_TO_NAME = {v: k for k, v in STRATEGY_SLUGS.items()}


def name_for_slug(slug: str) -> str:
    """Slug → имя стратегии ('' если неизвестен)."""
    return SLUG_TO_NAME.get((slug or "").lower(), "")


def simulate_candidates(pts, side, entry, leverage=10.0, taker=0.00055,
                        strategies=None):
    """
    Прогоняет набор стратегий по одной траектории.
    Возвращает dict name -> {"pnl": net%, "exit_dt": s, "exit_price": px}.
    strategies=None → build_candidates(side).
    """
    if strategies is None:
        strategies = build_candidates(side)
    out = {}
    if not pts:
        return out
    for name, fn in strategies.items():
        try:
            ex_dt, ex_px = fn(pts, side, entry)
        except Exception:  # noqa: BLE001
            continue
        out[name] = {
            "pnl": net_pnl_pct(side, entry, ex_px, leverage, taker),
            "exit_dt": ex_dt,
            "exit_price": ex_px,
        }
    return out


def clean_samples(samples):
    """[[dt, price|null], ...] -> [(dt, price), ...] без None, по dt."""
    pts = []
    for s in samples or []:
        if not isinstance(s, (list, tuple)) or len(s) < 2:
            continue
        dt, price = s[0], s[1]
        if price is None:
            continue
        try:
            pts.append((float(dt), float(price)))
        except (TypeError, ValueError):
            continue
    pts.sort(key=lambda x: x[0])
    return pts
