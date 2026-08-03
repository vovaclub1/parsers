# Исполнение и рыночная микроструктура

## 1. Почему market order не «исполняется по текущей цене»

Market order — это инструкция немедленно потребить доступную ликвидность.
Цена зависит от:

- best bid / ask;
- количества на каждом уровне;
- размера нашего ордера;
- других агрессивных ордеров;
- latency между snapshot и matching engine;
- hidden/RPI liquidity и правил venue;
- защитных ограничений биржи.

На новости стакан меняется быстрее, чем обычный REST price cache.

## 2. Spread

```
mid = (bid + ask) / 2
spread_bps = (ask - bid) / mid * 10_000
```

Для немедленного round-trip taker/taker стоимость минимум:

```
spread + entry_fee + exit_fee
```

Без движения цены сделка уже убыточна.

## 3. Depth и sweep cost

Для buy размера `Q` потребляем asks снизу вверх. Для sell — bids сверху вниз.

Хранить:

- depth в $ на 5/10/25/50/100 bps;
- expected VWAP для сетки notional;
- worst fill;
- процент visible book consumed.

Размер позиции ограничивать ликвидностью, а не только балансом.

## 4. Arrival price

Reference для execution — BBO в момент, когда стратегия могла принять решение,
а не цена публикации и не локальный cache через 2 секунды.

Разделять:

- source latency;
- decision latency;
- send latency;
- exchange latency;
- fill latency.

## 5. Adverse selection

В новостной стратегии мы агрессивно торгуем против market makers, которые тоже
видят событие. Если они быстрее, они:

- снимают stale quotes;
- расширяют spread;
- уменьшают depth;
- оставляют нам только плохие цены.

Признак adverse selection — отрицательный short-horizon markout после fill.

## 6. Market order vs IOC limit

### Market

Плюсы:

- высокий шанс fill;
- минимальная logic latency.

Минусы:

- неопределённая цена;
- риск sweep тонкого стакана;
- на Bybit market фактически конвертируется в IOC limit с внутренним пределом.

### IOC aggressive limit

Плюсы:

- жёсткий price cap;
- контролируем worst-case;
- partial fill лучше катастрофического full fill.

Минусы:

- missed trade / partial fill;
- нужно управлять остатком;
- дополнительная state logic.

### Bybit slippage tolerance

Официальный `/v5/order/create` поддерживает для futures market orders:

```json
{
  "slippageToleranceType": "Percent",
  "slippageTolerance": "0.75"
}
```

или `TickSize`. TP/SL/conditional orders с этими параметрами имеют ограничения,
которые нужно сверять с актуальной документацией перед реализацией.

Нужен experiment, а не произвольный cap:

- 25 / 50 / 75 / 100 / 200 bps;
- fill rate;
- average slippage;
- missed move;
- net expectancy;
- tail loss.

## 7. State machine исполнения

Нельзя считать `send()` равным позиции.

### Состояния

```
CREATED
SENT
ACKED
PARTIALLY_FILLED
FILLED
REJECTED
CANCELLED
EXPIRED
UNKNOWN
RECONCILED
```

### Truth priority

1. private execution stream — фактические fills;
2. private order stream — order lifecycle;
3. private position stream — итоговая позиция;
4. REST query — reconciliation при gap/timeout;
5. локальный расчёт — только intent, не truth.

### Unknown state

Если frame отправлен, но ack/fill потерян:

- не повторять blindly;
- query по `orderLinkId`;
- проверить позицию;
- только после reconciliation решать retry;
- пока unknown — блокировать новую позицию по тому же symbol.

## 8. Private WebSocket topics

Проекту нужны:

- `order`;
- `execution`;
- `position`.

`wallet` не нужен на hot-path и может быть опрошен REST для risk snapshot.

Для каждой подписки:

- sequence/gap handling;
- reconnect/resubscribe;
- dedup по execution id;
- persistence до обработки;
- reconciliation после reconnect.

## 9. Keepalive

Официальная документация Bybit рекомендует ping каждые 20 секунд.

У проекта есть два клиента:

- async `websockets` с native ping interval;
- sync `websockets` 12.x, где native keepalive отсутствовал — исправлено в #48
  через application ping / adaptive native keepalive на новых версиях.

Keepalive должен измерять не только TCP, но application health:

- ping/pong RTT;
- warmup ack RTT;
- time since last server frame;
- reconnect count;
- consecutive failures.

## 10. Clock synchronization

Auth и order timestamps зависят от wall clock. Нужны:

- chrony;
- alert при offset > 20–50ms;
- запись NTP offset в telemetry;
- сравнение exchange timestamp с local receive;
- монотонные clocks для durations.

## 11. Venue selection

Bybit-first / Gate-fallback — слишком грубое правило.

Перед trade сравнить venues:

- instrument availability;
- BBO;
- depth для desired size;
- fee tier;
- funding;
- expected latency;
- recent reject rate;
- account available margin;
- historical event slippage.

Выбирать минимальный ожидаемый all-in cost, а не фиксированный приоритет.

## 12. Smart routing v1

Rule-based score:

```
venue_cost_bps = spread_bps
                 + expected_slippage_bps(size)
                 + taker_fee_bps
                 + latency_penalty_bps
                 + reliability_penalty_bps
```

Торговать на venue с минимальным cost, если он ниже expected edge.

## 13. Exit execution

TP/SL должны опираться на фактическую позицию:

- actual filled qty;
- actual average entry;
- tick size;
- reduceOnly;
- total TP quantities <= current position;
- stop covers остаток;
- partial fill корректирует exits;
- after reconnect verify active stops.

Текущая постановка TP/SL по локально рассчитанному `amount` рискует получить
size mismatch при partial fill или price/qty correction биржи.

## 14. Transaction Cost Analysis pipeline

По каждой сделке автоматически генерировать:

- arrival BBO;
- expected book VWAP;
- actual fill VWAP;
- spread paid;
- slippage vs book;
- fee;
- latency components;
- markouts;
- route;
- reject/fallback details.

Daily report:

- p50/p90/p99 latency;
- average IS bps;
- fill rate by cap;
- route reliability;
- source-to-fill distribution;
- worst 10 execution outliers с raw payload.

## 15. Полезные framework-и

### hftbacktest

Сильные стороны:

- L2/L3 replay;
- latency models;
- queue position;
- Binance/Bybit crypto examples.

Для нашей aggressive event strategy queue model менее важен, чем L2 sweep,
latency и partial fill, но framework полезен как reference.

### NautilusTrader

Сильные стороны:

- event-driven architecture;
- backtest/sandbox/live parity;
- order state machine;
- multi-venue.

Не надо немедленно переписывать проект. Сначала позаимствовать архитектурные
принципы и сравнить стоимость интеграции с собственным узким engine.
