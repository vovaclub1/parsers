"""Лёгкий Telegram-логгер для уведомлений из парсеров."""
from __future__ import annotations

import html
from concurrent.futures import ThreadPoolExecutor

import requests
from config.config import TG_LOG_BOT_TOKEN, TG_LOG_CHAT_ID

# FIX: формируем URL внутри функции если токен не задан, чтобы не было
# `https://api.telegram.org/botNone/sendMessage` в импорте.
_session = requests.Session()

# FIX-PERF: fire-and-forget executor для tg_log. Раньше tg_log() был sync
# requests.post с timeout=5 — на RTT до Telegram (50-300мс) worker блокировался
# в hot-path после открытия позиции. С PYTHONUNBUFFERED=1 ещё и GIL держался
# на всё время HTTP, что мешало другим worker'ам и поллерам. Pool с 2 worker'ами
# pre-warmed: tg_log возвращается за ~5-20мкс (submit), отправка идёт в фоне.
_tg_log_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="tg-log")
# Pre-warm — ThreadPoolExecutor создаёт worker-thread лениво на первый submit
# (~3-5мс). Прогреваем сразу при импорте, чтобы первый tg_log не платил.
for _ in range(2):
    _tg_log_executor.submit(lambda: None)

# FIX: chat_id должен быть int если это числовой ID, иначе Telegram вернёт 400.
# Пробуем привести к int, fallback — строка (для @username каналов).
try:
    _CHAT_ID: int | str = int(TG_LOG_CHAT_ID) if TG_LOG_CHAT_ID else ""
except (TypeError, ValueError):
    _CHAT_ID = TG_LOG_CHAT_ID or ""


# Разрешённые Telegram HTML-теги (открывающие и закрывающие).
_ALLOWED_TAGS = ("<b>", "</b>", "<i>", "</i>", "<code>", "</code>", "<pre>", "</pre>")
_TAG_PLACEHOLDERS = [(t, f"\x00{i}\x00") for i, t in enumerate(_ALLOWED_TAGS)]


def _escape_for_html(msg: str) -> str:
    """
    FIX (review L6): раньше функция была NO-OP (оба return возвращали msg
    как есть), а комментарий врал что эскейпит. При тикере/тексте с '&'
    или несбалансированным '<' Telegram возвращал 400 (спасал только
    retry без parse_mode в _tg_log_blocking).

    Теперь: прячем разрешённые теги (<b>/<i>/<code>/<pre>) в плейсхолдеры,
    html.escape всё остальное (& < >), возвращаем теги назад. Так
    форматирование сохраняется, а спецсимволы в данных безопасны.
    """
    if "<" not in msg and "&" not in msg:
        return msg
    tmp = msg
    for tag, ph in _TAG_PLACEHOLDERS:
        tmp = tmp.replace(tag, ph)
    tmp = html.escape(tmp, quote=False)   # экранирует & < > (не трогает ")
    for tag, ph in _TAG_PLACEHOLDERS:
        tmp = tmp.replace(ph, tag)
    return tmp


def _tg_log_blocking(msg: str, reply_markup=None) -> None:
    """Sync-отправка в Telegram. Вызывается из _tg_log_executor в фоне."""
    if not _CHAT_ID or not TG_LOG_BOT_TOKEN:
        return

    url = f"https://api.telegram.org/bot{TG_LOG_BOT_TOKEN}/sendMessage"
    text = _escape_for_html(msg)

    payload = {"chat_id": _CHAT_ID, "text": text, "parse_mode": "HTML"}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    try:
        resp = _session.post(url, json=payload, timeout=5)
        if not resp.ok:
            # FIX: если HTML парсинг падает (битый тег / спецсимволы) —
            # повторяем БЕЗ parse_mode, чтобы сообщение всё-таки дошло.
            if resp.status_code == 400 and "parse" in resp.text.lower():
                payload2 = {"chat_id": _CHAT_ID, "text": html.escape(msg)}
                if reply_markup is not None:
                    payload2["reply_markup"] = reply_markup
                resp = _session.post(url, json=payload2, timeout=5)
                if resp.ok:
                    return
            print(f"[TG LOG ERR] HTTP {resp.status_code} | {resp.text[:160]}")
    except Exception as e:
        print(f"[TG LOG ERR] {e}")


def tg_log(msg: str, reply_markup=None) -> None:
    """
    Fire-and-forget отправка в TG лог-чат. Возвращается мгновенно (~5-20мкс),
    реальный HTTP-запрос идёт в _tg_log_executor.

    reply_markup — опциональная inline-клавиатура (dict). Используется 6ч-итогами
    стратегий: кнопки-стратегии под карточкой → тап даёт описание.

    FIX-PERF: раньше был sync requests.post — блокировал worker в hot-path
    на 50-300мс (RTT до Telegram) после открытия позиции. Это держало GIL
    и создавало jitter для следующих сигналов / поллеров.

    Если очередь executor'а переполнится (worker'ы зависли) — submit
    кинется в queue без блокировки caller'а.
    """
    if not _CHAT_ID or not TG_LOG_BOT_TOKEN:
        print("[TG LOG] токен или chat_id не заданы в .env, сообщение пропущено")
        return
    try:
        _tg_log_executor.submit(_tg_log_blocking, msg, reply_markup)
    except RuntimeError:
        # Executor закрыт (на shutdown) — игнор.
        pass
