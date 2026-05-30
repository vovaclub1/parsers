"""Конфиг — читает переменные окружения из .env."""
from __future__ import annotations

import os
from dotenv import load_dotenv, find_dotenv

# FIX: load_dotenv возвращает True/False, не None. Если .env не найден —
# выводим warning, но не убиваем процесс (полезно в Docker, где переменные
# могут быть переданы через -e / docker-compose environment).
_dotenv_path = find_dotenv()
if _dotenv_path:
    load_dotenv(_dotenv_path)
else:
    print("[CONFIG WARN] .env не найден — берём переменные из окружения процесса")

BINGX_API_KEY = os.getenv("BINGX_API_KEY")
BINGX_SECRET_KEY = os.getenv("BINGX_SECRET_KEY")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_SECRET_KEY = os.getenv("BYBIT_SECRET_KEY")
TG_API_ID = os.getenv("TG_API_ID")
TG_API_HASH = os.getenv("TG_API_HASH")
SOLANA_PRIVATE_KEY = os.getenv("SOLANA_PRIVATE_KEY")
TG_LOG_BOT_TOKEN = os.getenv("TG_LOG_BOT_TOKEN")
TG_LOG_CHAT_ID = os.getenv("TG_LOG_CHAT_ID")
GATEIO_API_KEY = os.getenv("GATEIO_API_KEY")
GATEIO_SECRET_KEY = os.getenv("GATEIO_SECRET_KEY")
HYPERLIQUID_KEY = os.getenv("HYPERLIQUID_KEY")

# FIX: ранее были захардкожены в parser_delist.py — теперь читаются из .env.
# Формат: "user:pass@host:port,user2:pass2@host2:port2" (через запятую, без http://).
# Если переменной нет — используем только прямое подключение.
DELIST_PROXIES = os.getenv("DELIST_PROXIES", "")

# FIX: API key для wss://*.coinlisting.pro — раньше был захардкожен.
COINLISTING_API_KEY = os.getenv("COINLISTING_API_KEY", "")

# FIX: путь к Telethon сессиям — раньше был захардкожен `/Parsers/...`,
# теперь конфигурируется через env (по умолчанию /Parsers для Docker).
SESSION_DIR = os.getenv("SESSION_DIR", "/Parsers")

# FIX-batch-3: дополнительные TG-каналы для multi-source first-wins listener.
# CSV: username каналов без @ или ID (число с -100...). Дубликаты сигналов
# отсекаются через _fired_coins (TTL 60с), так что first-wins.
EXTRA_DELIST_CHANNELS  = os.getenv("EXTRA_DELIST_CHANNELS",  "")
EXTRA_LISTING_CHANNELS = os.getenv("EXTRA_LISTING_CHANNELS", "")

# FIX-batch-4: включает встроенный слушатель wss://news.treeofalpha.com/ws.
TREE_OF_ALPHA_WS_ENABLED = os.getenv("TREE_OF_ALPHA_WS_ENABLED", "1").lower() in ("1", "true", "yes", "on")

# FIX-batch-5: включает Bybit V5 WS Trading API для размещения ордеров.
BYBIT_WS_TRADE_ENABLED = os.getenv("BYBIT_WS_TRADE_ENABLED", "1").lower() in ("1", "true", "yes", "on")

# FIX-PERF: sync-вариант WS клиента (api/bybit_sync_ws_trade.py). Убирает
# cross-thread asyncio hop из hot-path (-0.5...-2мс на трейд).
# Default ON — async остаётся как fallback при init/transport ошибках.
# Поставить "0" если sync-вариант вызовет регрессии в проде.
BYBIT_SYNC_WS_ENABLED = os.getenv("BYBIT_SYNC_WS_ENABLED", "1").lower() in ("1", "true", "yes", "on")


# Delist trailing stop strategy (native Bybit trailing)
# FIX: нативный trailingStop вместо фиксированных TP — ловим «первую быструю свечу».
DELIST_TRAILING_PCT = float(os.getenv("DELIST_TRAILING_PCT", "0.01"))   # 1.0% дистанция трейла
DELIST_ACTIVE_PCT   = float(os.getenv("DELIST_ACTIVE_PCT", "0.01"))     # Активация после +1% в плюс
DELIST_SL_PCT       = float(os.getenv("DELIST_SL_PCT", "0.05"))         # Аварийный SL +5%


def parse_channels(csv: str) -> list[str | int]:
    """
    Парсит CSV каналов. Возвращает список — каждый элемент либо int (ID),
    либо str (username без @). Пустую строку игнорирует.
    """
    out: list[str | int] = []
    for x in (csv or "").split(","):
        x = x.strip().lstrip("@")
        if not x:
            continue
        # Если выглядит как число с -100... — это ID, иначе username
        if x.startswith("-") and x[1:].isdigit():
            out.append(int(x))
        elif x.isdigit():
            out.append(int(x))
        else:
            out.append(x)
    return out
