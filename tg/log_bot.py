#!/usr/bin/env python3
from __future__ import annotations

# ── log_bot.py ────────────────────────────────────────────────────
# Интерактивный Telegram-лог-бот (ОТДЕЛЬНЫЙ процесс — ровно один владелец
# getUpdates, иначе апдейты делятся между процессами).
#
# Две кнопки:
#   📊 Стата за месяц  → полная стата по ОБОИМ парсерам за календарный месяц
#                        (signal_stats.month_section по listing+delist журналам).
#   🧪 Итоги стратегий → список входов за последний месяц (по 10 на страницу);
#                        тап по токену → карточка = ровно то, что бот шлёт в 6ч-итогах.
#
# Данные читает из общего STATE_DIR (volume): *_signal_events.jsonl и
# *_strategy_results.jsonl (их пишут парсеры). Сам ничего не считает — только
# отображает. Токен/чат reuse лог-бота (TG_LOG_BOT_TOKEN / TG_LOG_CHAT_ID).
#
# Рендер-функции (build_*/load_results/find_card_text) — чистые, без сети,
# тестируются отдельно. HTTP — в _api/send/edit/answer + poll_loop.
# ─────────────────────────────────────────────────────────────────

import html
import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

from config.config import TG_LOG_BOT_TOKEN, TG_LOG_CHAT_ID, STATE_DIR

MSK = timezone(timedelta(hours=3))
PAGE_SIZE = 10
MONTH_SEC = 30 * 86400

_STATE = Path(STATE_DIR)
_OFFSET_FILE = _STATE / "log_bot_offset.json"

# Журналы (общий volume): listing — без префикса, delist — delist_*.
_RESULT_FILES = [
    ("LISTING", _STATE / "strategy_results.jsonl"),
    ("DELIST", _STATE / "delist_strategy_results.jsonl"),
]
_EVENT_FILES = [
    ("LISTING", _STATE / "signal_events.jsonl"),
    ("DELIST", _STATE / "delist_signal_events.jsonl"),
]

_session = requests.Session()


def _now() -> int:
    return int(time.time())


# ── inline-клавиатуры / рендер (чистые) ───────────────────────────
def build_main_menu_kb() -> dict:
    return {"inline_keyboard": [
        [{"text": "📊 Статистика", "callback_data": "sm"}],
        [{"text": "🧪 Итоги стратегий", "callback_data": "sp:0"}],
        [{"text": "📖 Стратегии (описания)", "callback_data": "sh"}],
    ]}


def build_period_menu_kb() -> dict:
    """Подменю выбора периода статистики."""
    return {"inline_keyboard": [
        [{"text": "📅 День", "callback_data": "statp:1d"},
         {"text": "📆 Неделя", "callback_data": "statp:7d"},
         {"text": "🗓 Месяц", "callback_data": "statp:30d"}],
        [{"text": "↩︎ Меню", "callback_data": "menu"}],
    ]}


def build_period_text(window_key: str) -> str:
    """Сводка за период по ОБОИМ парсерам — два блока в одном сообщении."""
    from tg.signal_stats import SignalStats, _PERIOD_LABEL
    title = _PERIOD_LABEL.get(window_key, ("?", ""))[0]
    blocks = []
    for kind, fp in _EVENT_FILES:
        try:
            ss = SignalStats(fp, _STATE / "_logbot_unused.json", kind=kind)
            blocks.append(ss.period_summary(window_key))
        except Exception as e:  # noqa: BLE001
            blocks.append(f"({kind}: ошибка статы: {e!r})")
    return f"📊 <b>СТАТИСТИКА · {title}</b>\n\n" + "\n\n➖➖➖\n\n".join(blocks)


def build_strategy_help_kb() -> dict:
    """Клавиатура из названий стратегий (по 2 в ряд) → тап даёт описание."""
    from tg.exit_strategies import build_candidates
    names = list(build_candidates("long").keys())
    rows, row = [], []
    for n in names:
        row.append({"text": n, "callback_data": f"shd:{n}"[:64]})
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([{"text": "↩︎ Меню", "callback_data": "menu"}])
    return {"inline_keyboard": rows}


def load_results(since_ts: int) -> list:
    """Все result-записи за [since_ts, now] из обоих файлов, сортировка свежие→старые."""
    out = []
    for _kind, fp in _RESULT_FILES:
        try:
            if not fp.exists():
                continue
            with fp.open("rb") as f:
                for raw in f:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        row = json.loads(raw)
                    except Exception:
                        continue
                    cts = int(row.get("complete_ts", 0))
                    if cts < since_ts:
                        continue
                    out.append(row)
        except Exception as e:  # noqa: BLE001
            print(f"[LOG-BOT] load_results {fp.name}: {e!r}", flush=True)
    out.sort(key=lambda r: int(r.get("complete_ts", 0)), reverse=True)
    return out


def build_strategy_page(records: list, page: int, page_size: int = PAGE_SIZE):
    """(text, keyboard) для страницы списка входов. page клампится в диапазон."""
    n = len(records)
    if n == 0:
        return ("🧪 <b>Итоги стратегий</b>\n\nЗа последний месяц входов ещё нет.",
                {"inline_keyboard": [[{"text": "↩︎ Меню", "callback_data": "menu"}]]})
    pages = (n + page_size - 1) // page_size
    page = max(0, min(page, pages - 1))
    start = page * page_size
    chunk = records[start:start + page_size]

    rows = []
    for r in chunk:
        coin = r.get("coin", "?")
        side = r.get("side", "?")
        cts = int(r.get("complete_ts", 0))
        when = datetime.fromtimestamp(cts, MSK).strftime("%d.%m %H:%M") if cts else "?"
        icon = "🟢" if side == "long" else "🔴"
        win = r.get("winner") or "-"
        label = f"{icon} {coin} · {when} · 🥇{win}"
        # callback кодирует ts И coin — иначе две записи с одинаковым
        # complete_ts (две монеты закрылись в одну секунду) дают одну карточку.
        rows.append([{"text": label[:60], "callback_data": f"t:{cts}:{coin}"[:64]}])

    nav = []
    if page > 0:
        nav.append({"text": "◀", "callback_data": f"sp:{page - 1}"})
    nav.append({"text": f"{page + 1}/{pages}", "callback_data": "noop"})
    if page < pages - 1:
        nav.append({"text": "▶", "callback_data": f"sp:{page + 1}"})
    rows.append(nav)
    rows.append([{"text": "🧹 Очистить статистику", "callback_data": "clrmenu"}])
    rows.append([{"text": "↩︎ Меню", "callback_data": "menu"}])

    text = (f"🧪 <b>Итоги стратегий</b> — входов за месяц: {n}\n"
            f"Стр. {page + 1}/{pages}. Тапни токен для карточки.")
    return text, {"inline_keyboard": rows}


def build_clear_menu_kb() -> dict:
    """Подменю выбора периода очистки статистики стратегий."""
    return {"inline_keyboard": [
        [{"text": "За день", "callback_data": "clr:1d"},
         {"text": "За неделю", "callback_data": "clr:7d"}],
        [{"text": "За месяц", "callback_data": "clr:30d"},
         {"text": "Всё время", "callback_data": "clr:all"}],
        [{"text": "↩︎ Назад", "callback_data": "sp:0"}],
    ]}


_CLEAR_WINDOWS = {"1d": 86400, "7d": 7 * 86400, "30d": 30 * 86400, "all": None}


def clear_results(window_key: str) -> int:
    """
    Удаляет записи из ОБОИХ *_strategy_results.jsonl: за свежее окно
    (день/неделя/месяц) или все ('all'). Атомарно (tmp→rename). Возвращает
    число удалённых записей. *_eval_paths.jsonl (данные бэктеста) НЕ трогаем.
    """
    win = _CLEAR_WINDOWS.get(window_key)
    now = _now()
    removed = 0
    for _kind, fp in _RESULT_FILES:
        try:
            if not fp.exists():
                continue
            if win is None:
                # Полная очистка.
                with fp.open("rb") as f:
                    removed += sum(1 for ln in f if ln.strip())
                fp.write_bytes(b"")
                continue
            cutoff = now - win
            kept = []
            with fp.open("rb") as f:
                for raw in f:
                    s = raw.strip()
                    if not s:
                        continue
                    try:
                        row = json.loads(s)
                    except Exception:
                        kept.append(raw if raw.endswith(b"\n") else raw + b"\n")
                        continue
                    if int(row.get("complete_ts", 0)) >= cutoff:
                        removed += 1            # запись свежего окна → удаляем
                    else:
                        kept.append(s + b"\n")
            tmp = fp.with_suffix(".jsonl.tmp")
            tmp.write_bytes(b"".join(kept))
            tmp.replace(fp)
        except Exception as e:  # noqa: BLE001
            print(f"[LOG-BOT] clear_results {fp.name}: {e!r}", flush=True)
    return removed


def find_card_text(complete_ts: int, coin: str = None):
    """
    Текст карточки по complete_ts (+ coin для дизамбигуации). None если нет.
    coin отсекает коллизию двух записей с одинаковым complete_ts.
    """
    for _kind, fp in _RESULT_FILES:
        try:
            if not fp.exists():
                continue
            with fp.open("rb") as f:
                for raw in f:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        row = json.loads(raw)
                    except Exception:
                        continue
                    if int(row.get("complete_ts", 0)) != complete_ts:
                        continue
                    if coin is not None and row.get("coin") != coin:
                        continue
                    return row.get("text") or "(карточка без текста)"
        except Exception as e:  # noqa: BLE001
            print(f"[LOG-BOT] find_card_text {fp.name}: {e!r}", flush=True)
    return None




# ── offset persist ────────────────────────────────────────────────
def _load_offset() -> int:
    try:
        if _OFFSET_FILE.exists():
            return int(json.loads(_OFFSET_FILE.read_text()).get("offset", 0))
    except Exception:  # noqa: BLE001
        pass
    return 0


def _save_offset(off: int) -> None:
    try:
        _STATE.mkdir(parents=True, exist_ok=True)
        tmp = _OFFSET_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"offset": off}))
        tmp.replace(_OFFSET_FILE)
    except Exception as e:  # noqa: BLE001
        print(f"[LOG-BOT] save_offset: {e!r}", flush=True)


# ── Telegram API ──────────────────────────────────────────────────
def _api(method: str, payload: dict, timeout: int = 60):
    url = f"https://api.telegram.org/bot{TG_LOG_BOT_TOKEN}/{method}"
    try:
        r = _session.post(url, json=payload, timeout=timeout)
        return r.json() if r.content else {}
    except Exception as e:  # noqa: BLE001
        print(f"[LOG-BOT] api {method} failed: {e!r}", flush=True)
        return {}


def send_message(chat_id, text: str, reply_markup=None) -> None:
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
               "disable_web_page_preview": True}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    res = _api("sendMessage", payload)
    if not res.get("ok"):
        # fallback без parse_mode (битый HTML/спецсимволы)
        payload2 = {"chat_id": chat_id, "text": html.escape(text)}
        if reply_markup is not None:
            payload2["reply_markup"] = reply_markup
        _api("sendMessage", payload2)


def edit_message(chat_id, message_id, text: str, reply_markup=None) -> None:
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text,
               "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    res = _api("editMessageText", payload)
    if not res.get("ok"):
        # editMessageText кидает «message is not modified» / parse — не критично
        if "not modified" not in json.dumps(res).lower():
            send_message(chat_id, text, reply_markup)


def answer_callback(cb_id: str, text: str = None) -> None:
    payload = {"callback_query_id": cb_id}
    if text:
        payload["text"] = text
    _api("answerCallbackQuery", payload)


# ── обработка апдейтов ────────────────────────────────────────────
def _handle_message(msg: dict) -> None:
    chat_id = msg.get("chat", {}).get("id")
    if chat_id is None:
        return
    text = (msg.get("text") or "").strip()
    # deep-link тап по стратегии в карточке: "/start s_<slug>" → описание.
    if text.startswith("/start"):
        parts = text.split(maxsplit=1)
        payload = parts[1].strip() if len(parts) > 1 else ""
        if payload.startswith("s_"):
            from tg.exit_strategies import name_for_slug, strategy_help
            name = name_for_slug(payload[2:])
            desc = strategy_help(name) if name else ""
            if desc:
                send_message(chat_id, f"📖 <b>{name}</b>\n\n{desc}")
                return
    send_message(chat_id, "Лог-бот. Выбери, что показать:", build_main_menu_kb())


def _handle_callback(cb: dict) -> None:
    data = cb.get("data", "") or ""
    cb_id = cb.get("id")
    msg = cb.get("message", {}) or {}
    chat_id = msg.get("chat", {}).get("id")
    message_id = msg.get("message_id")
    answer_callback(cb_id)
    if chat_id is None:
        return

    if data == "noop":
        return
    if data == "menu":
        edit_message(chat_id, message_id, "Лог-бот. Выбери, что показать:",
                     build_main_menu_kb())
        return
    if data == "sm":
        edit_message(chat_id, message_id,
                     "📊 <b>Статистика</b>\nВыбери период:", build_period_menu_kb())
        return
    if data.startswith("statp:"):
        win = data.split(":", 1)[1]
        kb = {"inline_keyboard": [[{"text": "↩︎ Назад", "callback_data": "sm"}]]}
        edit_message(chat_id, message_id, build_period_text(win), kb)
        return
    if data.startswith("sp:"):
        try:
            page = int(data.split(":", 1)[1])
        except ValueError:
            page = 0
        records = load_results(_now() - MONTH_SEC)
        text, kb = build_strategy_page(records, page)
        edit_message(chat_id, message_id, text, kb)
        return
    if data == "sh":
        edit_message(chat_id, message_id,
                     "📖 <b>Стратегии</b>\nТапни стратегию — покажу описание.",
                     build_strategy_help_kb())
        return
    if data.startswith("shd:"):
        from tg.exit_strategies import strategy_help
        name = data.split(":", 1)[1]
        desc = strategy_help(name)
        body = (f"📖 <b>{name}</b>\n\n{desc}" if desc
                else f"Нет описания для «{name}».")
        edit_message(chat_id, message_id, body, {"inline_keyboard": [
            [{"text": "↩︎ К стратегиям", "callback_data": "sh"}]]})
        return
    if data == "clrmenu":
        edit_message(chat_id, message_id,
                     "🧹 <b>Очистить статистику стратегий</b>\nУдаляет записи итогов за "
                     "выбранный период (винрейт пересчитается). Данные бэктеста не трогаются.",
                     build_clear_menu_kb())
        return
    if data.startswith("clr:"):
        win = data.split(":", 1)[1]
        removed = clear_results(win)
        label = {"1d": "за день", "7d": "за неделю", "30d": "за месяц",
                 "all": "за всё время"}.get(win, win)
        edit_message(chat_id, message_id,
                     f"🧹 Очищено {label}: удалено записей — {removed}.",
                     {"inline_keyboard": [[{"text": "↩︎ К итогам", "callback_data": "sp:0"}]]})
        return
    if data.startswith("t:"):
        # формат: t:<ts>:<coin> (coin может содержать ':' — берём split с лимитом)
        parts = data.split(":", 2)
        try:
            ts = int(parts[1])
        except (ValueError, IndexError):
            return
        coin = parts[2] if len(parts) > 2 else None
        card = find_card_text(ts, coin)
        # Правим текущее сообщение карточкой (имена стратегий — тапаемые ссылки),
        # кнопка «Назад» возвращает к списку итогов.
        back_kb = {"inline_keyboard": [[{"text": "↩︎ К списку", "callback_data": "sp:0"}]]}
        edit_message(chat_id, message_id,
                     card or "Карточка не найдена (возможно, истёк месяц).", back_kb)
        return


def poll_loop() -> None:
    if not TG_LOG_BOT_TOKEN:
        print("[LOG-BOT] TG_LOG_BOT_TOKEN не задан — бот не стартует", flush=True)
        return
    # Снимаем webhook: пока он установлен, getUpdates возвращает 409 Conflict
    # и НИ ОДИН апдейт (включая callback_query от кнопок) не доходит.
    wh = _api("deleteWebhook", {"drop_pending_updates": False}, timeout=10)
    print(f"[LOG-BOT] deleteWebhook ok={wh.get('ok')}", flush=True)
    offset = _load_offset()
    print(f"[LOG-BOT] старт getUpdates poll (offset={offset})", flush=True)
    _warned_conflict = False
    while True:
        try:
            # allowed_updates ЯВНО включает callback_query — иначе, если ранее
            # бот вызывал getUpdates с другим allowed_updates, нажатия кнопок
            # (callback_query) могут не приходить (параметр персистентный).
            res = _api("getUpdates", {
                "offset": offset,
                "timeout": 50,
                "allowed_updates": ["message", "callback_query"],
            }, timeout=60)
            if not res.get("ok"):
                # 409 = другой поллер/webhook забирает апдейты; логируем раз.
                desc = res.get("description", res)
                if not _warned_conflict:
                    print(f"[LOG-BOT] getUpdates не ok: {desc}", flush=True)
                    _warned_conflict = True
                time.sleep(3)
                continue
            _warned_conflict = False
            for upd in res.get("result", []):
                offset = int(upd["update_id"]) + 1
                _save_offset(offset)
                try:
                    if "message" in upd:
                        _handle_message(upd["message"])
                    elif "callback_query" in upd:
                        _handle_callback(upd["callback_query"])
                except Exception as e:  # noqa: BLE001
                    print(f"[LOG-BOT] handle failed: {e!r}", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"[LOG-BOT] poll loop error: {e!r}", flush=True)
            time.sleep(3)


# ── авто-отчёт статистики (09:00/22:00 МСК, одним сообщением) ──────
_REPORT_STATE_FILE = _STATE / "log_bot_report_state.json"
_REPORT_HOURS_MSK = (9, 22)


def _seconds_until_next_report() -> float:
    now = datetime.now(MSK)
    cands = []
    for h in _REPORT_HOURS_MSK:
        c = now.replace(hour=h, minute=0, second=0, microsecond=0)
        if c <= now:
            c += timedelta(days=1)
        cands.append(c)
    return max(1.0, (min(cands) - now).total_seconds())


def _load_report_ts() -> int:
    try:
        if _REPORT_STATE_FILE.exists():
            return int(json.loads(_REPORT_STATE_FILE.read_text()).get("last", 0))
    except Exception:  # noqa: BLE001
        pass
    return 0


def _save_report_ts(ts: int) -> None:
    try:
        tmp = _REPORT_STATE_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"last": ts}))
        tmp.replace(_REPORT_STATE_FILE)
    except Exception as e:  # noqa: BLE001
        print(f"[LOG-BOT] save report ts: {e!r}", flush=True)


def report_scheduler_loop() -> None:
    """
    Шлёт авто-отчёт в чат в 09:00/22:00 МСК ОДНИМ сообщением (оба парсера).
    Период по дате: 1-го числа — месяц, по пн — неделя, иначе — день.
    Анти-дубль: last-ts персистится (не шлём дважды за то же окно).
    """
    from tg.signal_stats import SignalStats
    last = _load_report_ts()
    while True:
        time.sleep(_seconds_until_next_report())
        now = int(time.time())
        if now - last < 3600:          # уже слали в этот час-слот
            time.sleep(61)
            continue
        try:
            win = SignalStats.period_for_now()
            send_message(TG_LOG_CHAT_ID, build_period_text(win))
            last = now
            _save_report_ts(last)
        except Exception as e:  # noqa: BLE001
            print(f"[LOG-BOT] авто-отчёт failed: {e!r}", flush=True)
        time.sleep(61)               # защита от двойной отправки в ту же минуту


if __name__ == "__main__":
    import threading
    threading.Thread(target=report_scheduler_loop, daemon=True, name="stat-report").start()
    poll_loop()
