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

# FIX 2026-07-07: Seoul relay (edge-нода) удалён по решению — сервис умер,
# TOA-WS + CoinListing + TG перекрывают. SEOUL_RELAY_URL/KEY выпилены.

# FIX: API key для wss://*.coinlisting.pro — раньше был захардкожен.
COINLISTING_API_KEY = os.getenv("COINLISTING_API_KEY", "")

# FIX 2026-07-07 (free tier): каким региональным endpoint'ам coinlisting.pro
# подключаться. Платный тир позволял SEOUL+TOKYO одновременно; на бесплатном
# второе подключение выбивает первое (пинг-понг 1008 в логах). Дефолт —
# только seoul (ближе к Upbit/Bithumb). CSV: "seoul,tokyo".
COINLISTING_ENDPOINTS = [
    e.strip().lower()
    for e in os.getenv("COINLISTING_ENDPOINTS", "seoul").split(",")
    if e.strip()
]

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

# FIX 2026-06-19 (R3): private WS (order+position) для real-time чтения
# позиции вместо REST polling'а в _set_tp_sl_bybit. wallet НЕ подписан.
# Default ON — REST остаётся как fallback при transport-ошибках.
BYBIT_WS_PRIVATE_ENABLED = os.getenv("BYBIT_WS_PRIVATE_ENABLED", "1").lower() in ("1", "true", "yes", "on")


# Delist trailing stop strategy (native Bybit trailing)
# FIX: нативный trailingStop вместо фиксированных TP — ловим «первую быструю свечу».
# FIX 2026-06-18: trailing расширен 0.5% → 1.5% (узкий 0.5% закрывался на первом же
#   микро-отскоке: «позиция закрылась пока ставили трейлинг»). Активация сдвинута
#   0.5% → 1.0% — трейлинг встаёт только после реального движения вниз, до этого
#   позицию держит bundled SL.
# ВНИМАНИЕ: если в .env заданы старые DELIST_* — они перебьют эти дефолты.
try:
    DELIST_TRAILING_PCT = float(os.getenv("DELIST_TRAILING_PCT", "0.015"))
except ValueError:
    print("[CONFIG WARN] Invalid DELIST_TRAILING_PCT, using default 0.015")
    DELIST_TRAILING_PCT = 0.015

try:
    DELIST_ACTIVE_PCT = float(os.getenv("DELIST_ACTIVE_PCT", "0.01"))
except ValueError:
    print("[CONFIG WARN] Invalid DELIST_ACTIVE_PCT, using default 0.01")
    DELIST_ACTIVE_PCT = 0.01

try:
    DELIST_SL_PCT = float(os.getenv("DELIST_SL_PCT", "0.01"))
except ValueError:
    print("[CONFIG WARN] Invalid DELIST_SL_PCT, using default 0.01")
    DELIST_SL_PCT = 0.01


# FIX 2026-06-24: ATR-based адаптивный трейлинг для делиста (SHORT).
# Делистнутые монеты обычно имеют ХОРОШУЮ историю свечей (часы/дни дампа
# до выхода уведомления) — ATR здесь надёжнее, чем на свежем листинге.
# Период 14 — классический. Floor/ceiling страхуют от вырожденных значений.
# Установить DELIST_TRAIL_MODE=pct чтобы вернуть фиксированный % трейлинг.
DELIST_TRAIL_MODE      = os.getenv("DELIST_TRAIL_MODE", "sim_atr").lower()  # "sim_atr"|"atr"|"pct"
DELIST_ATR_INTERVAL    = os.getenv("DELIST_ATR_INTERVAL", "1")
try:
    DELIST_ATR_PERIOD = int(os.getenv("DELIST_ATR_PERIOD", "14"))
except ValueError:
    DELIST_ATR_PERIOD = 14
try:
    DELIST_ATR_MIN_CANDLES = int(os.getenv("DELIST_ATR_MIN_CANDLES", "5"))
except ValueError:
    DELIST_ATR_MIN_CANDLES = 5
try:
    DELIST_ATR_TRAIL_MULT = float(os.getenv("DELIST_ATR_TRAIL_MULT", "2.0"))
except ValueError:
    DELIST_ATR_TRAIL_MULT = 2.0
try:
    DELIST_ATR_ACT_MULT = float(os.getenv("DELIST_ATR_ACT_MULT", "1.0"))
except ValueError:
    DELIST_ATR_ACT_MULT = 1.0
try:
    DELIST_ATR_SL_MULT = float(os.getenv("DELIST_ATR_SL_MULT", "1.5"))
except ValueError:
    DELIST_ATR_SL_MULT = 1.5
# Floor/ceiling как доли от base_price.
try:
    DELIST_ATR_TRAIL_MIN_PCT = float(os.getenv("DELIST_ATR_TRAIL_MIN_PCT", "0.006"))   # 0.6%
except ValueError:
    DELIST_ATR_TRAIL_MIN_PCT = 0.006
try:
    DELIST_ATR_TRAIL_MAX_PCT = float(os.getenv("DELIST_ATR_TRAIL_MAX_PCT", "0.04"))    # 4%
except ValueError:
    DELIST_ATR_TRAIL_MAX_PCT = 0.04
try:
    DELIST_ATR_ACT_MIN_PCT   = float(os.getenv("DELIST_ATR_ACT_MIN_PCT",   "0.005"))   # 0.5%
except ValueError:
    DELIST_ATR_ACT_MIN_PCT   = 0.005
try:
    DELIST_ATR_ACT_MAX_PCT   = float(os.getenv("DELIST_ATR_ACT_MAX_PCT",   "0.03"))    # 3%
except ValueError:
    DELIST_ATR_ACT_MAX_PCT   = 0.03
try:
    DELIST_ATR_SL_MIN_PCT    = float(os.getenv("DELIST_ATR_SL_MIN_PCT",    "0.006"))   # 0.6%
except ValueError:
    DELIST_ATR_SL_MIN_PCT    = 0.006
try:
    DELIST_ATR_SL_MAX_PCT    = float(os.getenv("DELIST_ATR_SL_MAX_PCT",    "0.03"))    # 3%
except ValueError:
    DELIST_ATR_SL_MAX_PCT    = 0.03


# FIX 2026-07-07: sim_atr mode — точный порт tg/exit_strategies.py:exit_atr_trailing
# (та стратегия что в 6ч-карточках показывает "atr_trail" с +14%/+56%).
# Отличия от "atr" mode:
#   - atr = mean(|Δclose|)/entry (НЕ True Range)
#   - активация ПРИ дистанции trail (не при фикс -1%) — act = trail
#   - SL = 1% (не 1%, но фикс, не clamped)
#   - clamp [0.5%, 20%]
#   - period=30 × 1m klines pre-fill (proxy на sim'овские 30 сек post-fill)
# DELIST_TRAIL_MODE=sim_atr чтобы включить.
try:
    DELIST_SIM_ATR_K = float(os.getenv("DELIST_SIM_ATR_K", "2.5"))
except ValueError:
    DELIST_SIM_ATR_K = 2.5
try:
    DELIST_SIM_ATR_SL = float(os.getenv("DELIST_SIM_ATR_SL", "0.01"))
except ValueError:
    DELIST_SIM_ATR_SL = 0.01
try:
    DELIST_SIM_ATR_PERIOD = int(os.getenv("DELIST_SIM_ATR_PERIOD", "30"))
except ValueError:
    DELIST_SIM_ATR_PERIOD = 30
DELIST_SIM_ATR_INTERVAL = os.getenv("DELIST_SIM_ATR_INTERVAL", "1")
try:
    DELIST_SIM_ATR_MIN_CANDLES = int(os.getenv("DELIST_SIM_ATR_MIN_CANDLES", "5"))
except ValueError:
    DELIST_SIM_ATR_MIN_CANDLES = 5
try:
    DELIST_SIM_ATR_LO = float(os.getenv("DELIST_SIM_ATR_LO", "0.005"))
except ValueError:
    DELIST_SIM_ATR_LO = 0.005
try:
    DELIST_SIM_ATR_HI = float(os.getenv("DELIST_SIM_ATR_HI", "0.20"))
except ValueError:
    DELIST_SIM_ATR_HI = 0.20
try:
    DELIST_SIM_ATR_FALLBACK = float(os.getenv("DELIST_SIM_ATR_FALLBACK", "0.02"))
except ValueError:
    DELIST_SIM_ATR_FALLBACK = 0.02


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
