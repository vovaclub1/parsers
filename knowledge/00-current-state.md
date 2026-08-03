# Текущее состояние проекта и gap-анализ

Проверено повторно по репозиторию `vovaclub1/parsers`, текущей цепочке PR
#44–#48, живым API Binance / Bybit / Upbit / Bithumb и официальной
документации бирж.

## Что проект умеет сейчас

### Сигналы

- Binance delisting announcements: HTTP polling через direct + proxies.
- Upbit / Bithumb / Binance Futures: обнаружение новых рынков по diff списка.
- Telegram: Telethon, несколько каналов, first-wins.
- Tree of Alpha WebSocket.
- CoinListing.pro WebSocket и извлечение тикеров из Upbit/Bithumb notices.

### Исполнение

- Bybit linear perpetuals — основной venue.
- Gate.io futures — fallback.
- Hyperliquid — код есть, но основной parser его не использует.
- Bybit WS Trade sync — основной hot-path.
- Bybit WS Trade async — резерв.
- REST — второй резерв.
- `orderLinkId` защищает от дубля между WS и REST.
- TP/SL ставятся после входа; для listing часть защиты включена в order.create.

### Защита

- L1 дедуп на 60 секунд.
- L2 дедуп на диске с TTL 30 дней после PR #47.
- bulk-guard на массовое появление рынков.
- delayed baseline после сбоя стартовой загрузки.
- watchdog и Docker heartbeat.
- Docker secrets guard и CI после PR #44.

## Сильные стороны

1. **Несколько независимых источников.** Снижает риск пропустить событие.
2. **Hot-path не зависит от тяжёлого framework.** Нет ccxt на критическом пути Bybit.
3. **Idempotency.** Один `orderLinkId` между WS и REST снижает риск двойной позиции.
4. **Fail-fast guards.** Неизвестный lot step не заменяется опасным дефолтом.
5. **Практическая ориентация на latency.** Есть прогрев соединений, TLS pool,
   sync WS, предварительная загрузка instrument metadata.
6. **После аудита есть тестовая база.** Но она пока в основном про parsing,
   safety и mechanics, не про profitability.

## Главные пробелы

### P0 — нет доказательства торгового edge

Код предполагает:

- listing → цена растёт → long;
- delisting → цена падает → short.

Это только направление события. Оно не доказывает, что **после нашего
фактического времени входа** остаётся прибыль. Возможно:

- цена уже прошла движение до arrival;
- spread расширился сильнее ожидаемого drift;
- market order получает adverse selection;
- ликвидность исчезает и slippage съедает edge;
- TP/SL параметры оптимизированы на нескольких запомнившихся случаях;
- signal quality существенно различается по exchange / event subtype.

Нужен event study на миллисекундных / секундных данных и реальных fill-ценах.

### P0 — нет полной учётной записи сделки

Логи печатают локально рассчитанный `entry_price`, но это не обязательно
фактическая fill price. Для прибыли нужны:

- `signal_received_ns`;
- `order_sent_ns`;
- `ack_received_ns`;
- `first_fill_ns`, `last_fill_ns`;
- `avg_fill_price`;
- `filled_qty`;
- `fee`;
- `funding`;
- `exit_fill_price`;
- `realized_pnl`;
- снимок BBO / depth на arrival.

Без этого нельзя отличить:

- плохую стратегию;
- медленный источник;
- плохое исполнение;
- неверную оценку P&L;
- инфраструктурный reject.

### P0 — `ws.send()` трактуется как успех

PR #48 исправляет один класс мёртвого сокета, но архитектурно вход всё ещё
считает позицию открытой после отправки frame, не после execution/fill.

Правильная модель состояний:

```
SIGNAL -> VALIDATED -> ORDER_SENT -> ACKED -> PARTIALLY_FILLED -> FILLED
                                           -> REJECTED
                                           -> UNKNOWN (reconcile required)
```

TP/SL нельзя надёжно ставить по локально рассчитанному amount до подтверждения
реальной позиции. Нужен private WS `order` + `execution`/`position` и
reconciliation через REST при таймауте.

### P0 — нет risk engine уровня портфеля

Сейчас размер сделки задаётся процентом от фиктивного `balance = 100`.
Отсутствуют:

- реальный доступный баланс;
- лимит total gross notional;
- лимит по коррелированным одновременным сигналам;
- daily loss limit;
- max drawdown kill-switch;
- max consecutive failures;
- лимит slippage;
- проверка mark/liquidation distance;
- circuit breaker при stale market data;
- режим paper / shadow / live canary.

### P1 — нет transaction cost analysis

Нужно по каждой сделке считать:

- spread paid;
- slippage vs arrival mid / BBO;
- implementation shortfall;
- fee;
- market impact по нескольким размерам;
- adverse move через 100ms / 500ms / 1s после fill;
- missed-trade cost при ограничении slippage.

### P1 — market order без slippage cap

Официальный Bybit `order.create` поддерживает для futures market order:

- `slippageToleranceType = TickSize | Percent`;
- `slippageTolerance`.

Сейчас параметры не используются. На новостном событии spread и depth могут
исчезнуть. «Получить любой fill» может быть хуже, чем пропустить сделку.
Нужен эксперимент с IOC aggressive limit / market с cap.

### P1 — нет событийной классификации

Не все «листинги» и «делистинги» одинаковы:

- spot listing;
- perpetual listing;
- margin listing;
- new quote pair;
- monitoring tag;
- full spot delisting;
- futures-only delisting;
- margin pair removal;
- deposit/withdraw suspension;
- Alpha/Web3 listing;
- relisting.

У них разная экономическая сила и разные false positives. Стратегия должна
торговать event subtype, а не только слово `listing`.

### P1 — нет cross-sectional features

Для оценки ожидаемого движения важны:

- market cap / circulating float;
- средний объём и depth на venue входа;
- число уже существующих CEX listings;
- venue significance (Binance spot != margin pair);
- perpetual availability до анонса;
- funding / open interest;
- pre-event return и volume anomaly;
- spread / order-book imbalance;
- BTC/ETH market regime;
- время суток / день недели;
- источник и его историческое опережение.

### P1 — нет корректного backtest

Текущие `parsers/test_parser_*.py` — диагностические скрипты, не event-driven
backtester listing/delisting strategy. Нужен replay:

1. событие с известным arrival timestamp;
2. стакан на каждой доступной venue;
3. latency model;
4. fill model;
5. fees/funding;
6. exit policy;
7. portfolio risk.

### P2 — observability

Нужны dashboards / отчёты:

- source latency percentile;
- parser latency;
- order latency;
- ack/fill/reject rates;
- slippage distribution;
- P&L by source/event subtype/exchange;
- false-positive rate;
- missed event rate;
- stale data / reconnect count;
- cost of fallback paths.

## Что НЕ надо делать сейчас

- Добавлять ML до появления качественного event-store.
- Оптимизировать regex на микросекунды без метрики net P&L.
- Менять TP/SL по 5–10 кейсам.
- Увеличивать плечо ради доходности.
- Считать backtest по свечам достаточным для новостного market order.
- Верить публичным заявлениям «listing pump +41%»: выборка мала, entry timing
  обычно не соответствует реальному arrival нашего бота.

## Реальный критерий готовности к масштабированию

Увеличивать размер можно только если одновременно выполнено:

- положительное out-of-sample expectancy после costs;
- 95% bootstrap CI нижней границы expectancy > 0 или заранее согласованный
  Bayesian posterior threshold;
- стабильность результата по разным месяцам и event subtypes;
- live paper/shadow соответствует backtest по latency/slippage;
- reject/unknown execution state близок к нулю;
- max drawdown укладывается в лимит;
- стратегия переживает стресс-сценарии spread ×3, latency ×3, slippage ×2.
