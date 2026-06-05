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

# FIX-LATENCY (Patch #2): прокси для notice-поллеров parser_listing
# (Bithumb-notice, Upbit-notice, Binance-notice). Тот же формат, что
# DELIST_PROXIES. С 3-5 IP можно безопасно уплотнить poll до 30-50мс
# cadence (каждый IP видит ~10 req/s, в пределах rate-limit бирж).
# Если переменной нет — все поллеры идут одним прямым подключением.
LISTING_PROXIES = os.getenv("LISTING_PROXIES", "")

# FIX-LATENCY (Patch #1): Seoul edge-нода — отдельная VPS в Корее, которая
# поллит api.bithumb.com и api.upbit.com с ~5мс RTT (vs 120мс из SG)
# и пушит свежие нотисы через WS на main-сервер. SEOUL_RELAY_URL —
# полный wss-эндпоинт с auth-токеном в query, например:
#   wss://seoul.mydomain.com/relay?key=SECRET
# Пустая строка отключает receiver. См. seoul_relay.py для серверной части.
SEOUL_RELAY_URL = os.getenv("SEOUL_RELAY_URL", "")
SEOUL_RELAY_KEY = os.getenv("SEOUL_RELAY_KEY", "")

# FIX: API key для wss://*.coinlisting.pro — раньше был захардкожен.
COINLISTING_API_KEY = os.getenv("COINLISTING_API_KEY", "")

# FIX: путь к Telethon сессиям — раньше был захардкожен `/Parsers/...`,
# теперь конфигурируется через env (по умолчанию /Parsers для Docker).
SESSION_DIR = os.getenv("SESSION_DIR", "/Parsers")

# FIX 2026-06-02: каталог для персистентного state (L2-дедуп listing_fired.json /
# delist_fired.json, source_stats.json). Вынесен из SESSION_DIR, потому что в
# Docker монтируется как volume-ДИРЕКТОРИЯ (а не пофайлово, как .session): иначе
# атомарный tmp.replace() в _persist_fired_state ловит EXDEV через границу
# bind-mount отдельного файла. Инцидент: SLX открылся повторно после
# `docker compose up --force-recreate`, т.к. listing_fired.json не был
# примонтирован и терялся. Дефолт = SESSION_DIR → локальный запуск без Docker
# не ломается.
STATE_DIR = os.getenv("STATE_DIR", SESSION_DIR)

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
# FIX 2026-06-04: trailing 0.5% (туже), SL 1% (было 5%), активация после -0.5%.
# ВНИМАНИЕ: если в .env заданы старые DELIST_* — они перебьют эти дефолты.
try:
    DELIST_TRAILING_PCT = float(os.getenv("DELIST_TRAILING_PCT", "0.005"))
except ValueError:
    print("[CONFIG WARN] Invalid DELIST_TRAILING_PCT, using default 0.005")
    DELIST_TRAILING_PCT = 0.005

try:
    DELIST_ACTIVE_PCT = float(os.getenv("DELIST_ACTIVE_PCT", "0.005"))
except ValueError:
    print("[CONFIG WARN] Invalid DELIST_ACTIVE_PCT, using default 0.005")
    DELIST_ACTIVE_PCT = 0.005

try:
    DELIST_SL_PCT = float(os.getenv("DELIST_SL_PCT", "0.01"))
except ValueError:
    print("[CONFIG WARN] Invalid DELIST_SL_PCT, using default 0.01")
    DELIST_SL_PCT = 0.01


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


# FIX (review M29): валидация критичных ключей на старте. Раньше
# отсутствие ключа всплывало только в рантайме (битая подпись / пустой
# TG). Теперь — явный warning сразу при импорте config.
def _validate_critical_env() -> None:
    critical = {
        "BYBIT_API_KEY": BYBIT_API_KEY,
        "BYBIT_SECRET_KEY": BYBIT_SECRET_KEY,
        "TG_LOG_BOT_TOKEN": TG_LOG_BOT_TOKEN,
        "TG_LOG_CHAT_ID": TG_LOG_CHAT_ID,
    }
    missing = [k for k, v in critical.items() if not v]
    if missing:
        print(f"[CONFIG WARN] не заданы критичные переменные: {', '.join(missing)}")


_validate_critical_env()
