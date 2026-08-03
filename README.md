# parsers

Боты для торговли по анонсам криптобирж: ловят листинги и делистинги за
десятки миллисекунд и автоматически открывают позицию на Bybit / Gate.io.

> **Research и развитие:** [knowledge/README.md](knowledge/README.md) — база
> знаний проекта: gap-анализ, модель прибыльности, event-store, микроструктура,
> risk management, 12-недельный план обучения, источники и backlog экспериментов.
> Главный принцип: скорость — не прибыль; edge подтверждается только после
> fills, costs и out-of-sample проверки.

- **`parser_delist.py`** — ловит делистинги → открывает **шорт**
- **`parser_listing.py`** — ловит листинги → открывает **лонг**
- **`parser_arbitrage.py`** — межбиржевой арбитраж (по умолчанию выключен)

---

## Как это работает

Оба парсера слушают несколько источников параллельно и торгуют по принципу
**first-wins**: кто первым принёс монету, тот и сработал, остальные отсекаются
дедупликацией.

### Источники делистингов (`parser_delist.py`)

| Источник | Механизм | Интервал |
|---|---|---|
| Binance announcement API | HTTP-поллинг `catalogId=161` через N прокси | 3 c (env `DELIST_POLL_INTERVAL`) |
| Telegram-каналы | Telethon, основной + `EXTRA_DELIST_CHANNELS` | push |
| Tree of Alpha | `wss://news.treeofalpha.com/ws` | push |

### Источники листингов (`parser_listing.py`)

| Источник | Механизм | Интервал |
|---|---|---|
| Upbit | `api.upbit.com/v1/market/all`, диф по KRW-тикерам | 100 мс |
| Bithumb | `api.bithumb.com/public/assetsstatus/all` | 100 мс |
| Binance Futures | `fapi/v1/exchangeInfo`, диф по `PERPETUAL` | 2.5 c |
| Telegram-каналы | Telethon | push |
| CoinListing WS | `wss://*.coinlisting.pro` | push |
| Tree of Alpha | `wss://news.treeofalpha.com/ws` | push |

### Путь сигнала

```
источник → извлечение тикера → дедуп (L1 TTL + L2 persistent)
        → market_open_* → Bybit WS Trade (fallback: REST → Gate.io)
        → TP/SL в фоне
```

Задержка сигнал→ордер в прогретом состоянии: **5–12 мс**.

---

## Быстрый старт

```bash
git clone https://github.com/vovaclub1/parsers.git
cd parsers

cp .env.example .env
$EDITOR .env          # заполнить ключи

docker compose up -d
docker compose logs -f delist
```

### Локально, без Docker

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export PYTHONPATH=$PWD
export SESSION_DIR=$PWD STATE_DIR=$PWD/state
python -u parsers/parser_delist.py
```

Первый запуск запросит авторизацию Telegram (Telethon создаст `.session`).

---

## Конфигурация

Все настройки — через `.env` (см. `.env.example`). Ключевые:

| Переменная | Назначение | Дефолт |
|---|---|---|
| `BYBIT_API_KEY` / `BYBIT_SECRET_KEY` | Торговый счёт Bybit | — |
| `GATEIO_API_KEY` / `GATEIO_SECRET_KEY` | Fallback-биржа | — |
| `TG_API_ID` / `TG_API_HASH` | Telethon (my.telegram.org) | — |
| `TG_LOG_BOT_TOKEN` / `TG_LOG_CHAT_ID` | Бот для алертов о сделках | — |
| `DELIST_PROXIES` | Прокси для Binance-поллера, через запятую | пусто |
| `SESSION_DIR` | Каталог `.session`-файлов Telethon | `/Parsers` |
| `STATE_DIR` | Каталог L2-дедупа — **монтировать наружу** | `$SESSION_DIR/state` |
| `DELIST_POLL_INTERVAL` | Интервал поллинга Binance, с | `3.0` |
| `DELIST_L2_TTL_SEC` | Срок блокировки монеты после шорта, с | `2592000` (30 д) |
| `LISTING_L2_TTL_SEC` | Срок блокировки монеты после лонга, с | `2592000` (30 д) |
| `BYBIT_WS_TRADE_ENABLED` | Ордера через WS вместо REST | `1` |

Поддерживаемые форматы прокси (можно смешивать):
`host:port:user:pass`, `user:pass@host:port`, `host:port`.

---

## Управление риском

Заложено в коде — **проверьте перед боевым запуском**:

| Параметр | Значение | Где |
|---|---|---|
| Плечо | `10x` | `LEVERAGE`, `api/delist_api.py` |
| Маржа на делистинг | 10 % от 100 USDT | `calculate_margin_for_delist()` |
| Маржа на листинг | 14 % от 100 USDT | `calculate_margin_for_listing()` |
| SL шорта | `+5 %` | `_set_tp_sl_bybit_short()` |
| TP шорта | `-8 % / -15 % / -45 %` (20/30/50 %) | там же |
| SL лонга | `-8 %` | `market_open_long()` |
| TP лонга | `+4.5 %` на 30 % + трейлинг 3.5 % | `_set_tp_sl_bybit()` |
| Макс. новых тикеров за тик | `10` | `MAX_NEW_TICKERS_PER_TICK` |

> **Внимание.** Баланс сейчас захардкожен (`balance = 100`), реальный размер
> счёта через API не запрашивается. Перед запуском на живых деньгах приведите
> `calculate_margin_for_*()` в соответствие с депозитом.

### Защита от ложных срабатываний

- **Bulk-guard** — если за один тик «появилось» больше 10 тикеров, это сбой
  API или окончание maintenance, а не листинг. Тик пропускается.
- **Отложенный старт** — если стартовый снимок рынка не загрузился, поллер не
  торгует, пока не получит валидный baseline.
- **L1-дедуп** (TTL 60 с) — несколько каналов об одном событии = одна сделка.
- **L2-дедуп** (на диске, `STATE_DIR`, TTL 30 дней) — «эту монету уже
  отрабатывали». Срок конечен намеренно: повторные делистинги одной монеты
  реальны (по каталогу Binance ARDR делистился дважды с интервалом 108 дней,
  ALCX — 105 дней), и вечная блокировка съедала бы второй сигнал.
  Настраивается через `DELIST_L2_TTL_SEC` / `LISTING_L2_TTL_SEC`.
- **Негативные фильтры** — отсекают «Removal of Margin Trading Pairs»,
  Binance Alpha removals, Earn/Launchpool-промо.
- **Idempotency** — единый `orderLinkId` на WS и REST-путь; при потере ack
  Bybit отвергнет дубль (`retCode 30050`), второй позиции не будет.

---

## Тесты

```bash
pip install pytest
PYTHONPATH=$PWD pytest tests/ -v
```

Тесты не ходят в сеть и не требуют боевых ключей. Кейсы извлечения тикеров
построены на реальных заголовках из Binance announcement API.

CI (`.github/workflows/ci.yml`) на каждый push прогоняет: ruff → compileall →
pytest на Python 3.11/3.12, скан на закоммиченные секреты и сборку Docker-образа
с проверкой, что `.env` и `.session` в него не попали.

---

## Безопасность

- `.env` и `*.session` — **никогда** не коммитить (закрыто `.gitignore`)
  и не класть в образ (закрыто `.dockerignore`).
- `.session` Telethon = полный доступ к Telegram-аккаунту. Утечка равнозначна
  краже аккаунта.
- Образы публикуются в публичный registry — перед `docker push` убедитесь,
  что `.dockerignore` на месте.
- Ключи Bybit/Gate создавайте **без права вывода средств**, только торговля.

---

## Структура

```
api/
  delist_api.py        Bybit REST/WS, извлечение тикеров, TP/SL шорта
  listing_api.py       открытие лонга, TP/SL, извлечение тикеров листинга
  bybit_ws_trade.py    асинхронный WS Trade V5
  bybit_sync_ws_trade.py  синхронный WS (основной hot-path)
  gate_api.py          Gate.io futures (fallback)
  hl_api.py            Hyperliquid
  coinlisting_ws.py    WS coinlisting.pro + парсинг нотисов Upbit/Bithumb
  treeofalpha_ws.py    WS Tree of Alpha
  arbitrage_api.py     межбиржевой арбитраж
parsers/               точки входа (delist / listing / arbitrage)
config/config.py       чтение .env
tg/tg_logger.py        fire-and-forget алерты в Telegram
tests/                 pytest
```

---

## Эксплуатация

```bash
docker compose ps                      # состояние + healthcheck
docker compose logs -f --tail=100 delist
docker compose restart delist
```

Healthcheck считает контейнер живым, пока heartbeat-файл моложе 5 минут.
Watchdog внутри процесса перезапускает зависшие поллеры и шлёт алерт в
Telegram, если поток жив, но не отвечает более 90 с.
