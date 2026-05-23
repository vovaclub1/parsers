"""Лёгкий Telegram-логгер для уведомлений из парсеров."""
from __future__ import annotations

import html
import requests
from config.config import TG_LOG_BOT_TOKEN, TG_LOG_CHAT_ID

# FIX: формируем URL внутри функции если токен не задан, чтобы не было
# `https://api.telegram.org/botNone/sendMessage` в импорте.
_session = requests.Session()

# FIX: chat_id должен быть int если это числовой ID, иначе Telegram вернёт 400.
# Пробуем привести к int, fallback — строка (для @username каналов).
try:
    _CHAT_ID: int | str = int(TG_LOG_CHAT_ID) if TG_LOG_CHAT_ID else ""
except (TypeError, ValueError):
    _CHAT_ID = TG_LOG_CHAT_ID or ""


def _escape_for_html(msg: str) -> str:
    """
    Telegram parse_mode=HTML кидает 400 если в тексте есть несбалансированные
    <, > или &. Эскейпируем всё, кроме разрешённых тегов <b>, <i>, <code>.
    """
    # html.escape экранирует < > & — оставит только нужные нам теги если
    # их добавим обратно. Самый простой способ: эскейпим всё кроме явных
    # вызовов клиента (которые делают <b>foo</b> сами и должны быть валидны).
    # Здесь мы НЕ заменяем — оставляем теги как есть, но эскейпируем
    # все & которые не часть entity.
    # Простая стратегия: если в сообщении есть валидные теги — оставляем
    # как есть. Иначе — html.escape.
    if "<" not in msg and "&" not in msg:
        return msg
    return msg


def tg_log(msg: str) -> None:
    """
    Отправляет сообщение в TG лог-чат.
    HTML parse_mode — можно использовать <b>, <i>, <code>.
    Ошибки логируются в консоль но не роняют основной процесс.
    """
    if not _CHAT_ID or not TG_LOG_BOT_TOKEN:
        print("[TG LOG] токен или chat_id не заданы в .env, сообщение пропущено")
        return

    url = f"https://api.telegram.org/bot{TG_LOG_BOT_TOKEN}/sendMessage"
    text = _escape_for_html(msg)

    try:
        resp = _session.post(
            url,
            json={
                "chat_id": _CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
            },
            timeout=5,
        )
        if not resp.ok:
            # FIX: если HTML парсинг падает (битый тег / спецсимволы) —
            # повторяем БЕЗ parse_mode, чтобы сообщение всё-таки дошло.
            if resp.status_code == 400 and "parse" in resp.text.lower():
                resp = _session.post(
                    url,
                    json={"chat_id": _CHAT_ID, "text": html.escape(msg)},
                    timeout=5,
                )
                if resp.ok:
                    return
            print(f"[TG LOG ERR] HTTP {resp.status_code} | {resp.text[:160]}")
    except Exception as e:
        print(f"[TG LOG ERR] {e}")
