# Roadmap развития проекта

## North Star

Не «минимальный `open_ms`», а:

> максимальный устойчивый net P&L при ограниченном drawdown и доказуемом
> execution state.

## Принцип приоритета

1. Не потерять счёт / позицию.
2. Знать фактический результат.
3. Доказать edge.
4. Улучшить execution.
5. Масштабировать.

---

# Phase 0 — завершить safety chain

Статус: PR #44–#48 открыты цепочкой.

- #44 critical audit: bulk guard, parsing, Decimal qty, secrets, CI.
- #45 maxLeverage / minOrderQty.
- #46 O(1) claim.
- #47 L2 TTL.
- #48 sync WS health/keepalive.

## Definition of Done

- PR merged по порядку;
- CI зелёный на main;
- compose/build проверен;
- state migration проверена на копии production state;
- deploy canary;
- реальные order placement не тестируются без отдельного разрешения.

---

# Phase 1 — Event Store MVP (P0)

## Задачи

1. Ввести `SignalEvent`, `OrderIntent`, `OrderEvent`, `FillEvent`,
   `PositionEvent` dataclasses/schema.
2. SQLite WAL + migrations.
3. Raw JSONL journal write-before-process.
4. Записывать все source observations, включая late duplicates.
5. BBO snapshot на signal/decision/send/ack/fill.
6. Structured reason codes.
7. CLI report latency/source/reject.

## Изменяемые модули

- новый `storage/`;
- adapters в parsers;
- минимальные hooks в `process_signal` / worker;
- не переписывать trading logic целиком.

## DoD

- событие полностью восстанавливается после restart;
- write failure блокирует entries;
- raw payload сохранён до parser decision;
- тест без сети доказывает transaction durability.

## Риск/стоимость

- Стоимость: средняя.
- Риск: низкий, если сначала shadow-only.
- Ценность: максимальная — разблокирует всю аналитику.

---

# Phase 2 — Execution Truth (P0)

## Задачи

1. Private Bybit WS: `order`, `execution`, `position`.
2. Order state machine.
3. Reconcile по `orderLinkId` при timeout/reconnect.
4. Entry price/qty только из fills/position.
5. TP/SL после фактического fill; partial-fill adjustment.
6. Startup reconciliation.
7. Unknown-state circuit breaker.

## Важное ограничение

Private WS подписывать только на `order`, `execution`, `position`. Не добавлять
`wallet` на hot-path; баланс обновлять отдельным REST snapshot.

## DoD

Fault-injection тесты:

- ack lost, fill exists;
- duplicate execution;
- partial fill;
- reconnect gap;
- REST timeout;
- process restart с открытой позицией.

Ни один сценарий не создаёт дубль и не считает intent фактом.

---

# Phase 3 — Market Data Recorder / TCA (P0)

## Задачи

1. BBO stream для всего Bybit linear universe.
2. Top-N ring buffer.
3. Flush event window на диск.
4. `sweep_vwap()`.
5. TCA по fills.
6. Markouts 100ms–60s.
7. Route/fallback reliability.

## DoD

Для каждой сделки автоматически известны:

- arrival mid;
- spread;
- expected book VWAP;
- actual VWAP;
- fees;
- implementation shortfall;
- markouts;
- route и latency.

---

# Phase 4 — Risk Engine (P0)

## Задачи

1. Account snapshot REST (баланс, margin, positions).
2. Убрать `balance = 100`.
3. Sizing = min(risk/liquidity/margin/hard cap).
4. Daily loss / drawdown / reject / unknown breakers.
5. Event burst aggregate risk.
6. Stale signal/quote guards.
7. Config + validation.

## Решения пользователя

Нужно согласовать:

- risk per trade;
- daily loss;
- max notional;
- max total positions;
- allowed drawdown;
- canary amount.

До согласования реализация может быть shadow-only с безопасными defaults, но
боевые параметры менять нельзя.

---

# Phase 5 — Event Taxonomy / Source Quality (P1)

## Задачи

1. Нормализованный event classifier.
2. Corpus реальных announcements.
3. Precision/recall report.
4. Source lead/lag/miss report.
5. Event identity across sources.
6. Whitelist tradeable subtypes.

## Первичный whitelist-кандидат для исследования

- Binance full spot listing;
- Upbit KRW listing;
- Bithumb listing;
- Binance full spot delisting;
- Binance monitoring warning — отдельно, не смешивать с delist.

Не торговать до evidence:

- margin pair removal;
- Earn/Loan/Collateral;
- Alpha/Web3;
- generic new quote pairs;
- futures-only event тем же правилом, что spot.

---

# Phase 6 — Historical Event Study (P1)

## Задачи

1. Dataset publication events.
2. Historical market data.
3. Taxonomy labels.
4. Arrival-latency simulation.
5. Conditional return distributions.
6. Cost/capacity estimates.

## DoD

Есть отчёт по каждому subtype:

- sample size;
- capturable returns;
- cost-adjusted expectancy;
- CI;
- liquidity/capacity;
- sensitivity to latency.

---

# Phase 7 — Backtester (P1)

## Build vs adopt

### Собственный узкий engine

Плюсы:

- быстрее реализовать event-specific aggressive execution;
- полный контроль;
- меньше зависимостей.

Минусы:

- выше риск незаметной ошибки;
- state/portfolio/replay придётся поддерживать.

### hftbacktest

Плюсы:

- latency/L2/queue models;
- production-grade reference.

Минусы:

- интеграционная сложность;
- наша event taxonomy/storage остаётся своей.

### NautilusTrader

Плюсы:

- live/backtest parity;
- order state/portfolio framework.

Минусы:

- большой migration cost;
- может быть тяжелее узкого low-latency hot-path.

## Решение

Сделать 1-недельный spike:

- один event;
- один symbol;
- один Bybit replay;
- один market/IOC strategy;

Сравнить correctness, effort, speed. Не переписывать production до spike.

---

# Phase 8 — Execution Optimization (P1)

Только после TCA.

## Эксперименты

- market vs IOC;
- slippage caps;
- Bybit vs Gate routing;
- position size curves;
- immediate vs delayed/fade;
- partial fill policy;
- TP/SL families.

## DoD

Rule выбран на untouched test и shadow/live подтверждении.

---

# Phase 9 — Shadow / Canary / Scale (P0 перед live scale)

1. Shadow 24/7 минимум 4 недели.
2. Paper/testnet fault validation.
3. Live canary minimum notional.
4. Daily reconciliation.
5. Weekly strategy report.
6. Scale ступенями.

## Scale gate

- positive net expectancy;
- enough independent events;
- CI / posterior threshold;
- max drawdown acceptable;
- live slippage within model;
- no unknown state;
- no P0 incident.

---

# Phase 10 — ML (P3)

Только при:

- 500+ high-quality labeled events;
- stable taxonomy;
- event-store без gaps;
- simple conditional baselines работают;
- untouched OOS process.

## Первый ML-кейс

Не direction prediction (event уже задаёт direction), а meta-label:

> Торговать ли конкретный event при текущей liquidity/source delay/regime?

Модели:

1. logistic regression;
2. gradient boosted trees;
3. calibrated probability;
4. monotonic constraints там, где уместно.

Deep learning/RL — не раньше доказанного ограничения простых моделей.

---

# Ближайшие 10 задач по порядку

1. Смёржить #44–#48 и проверить main.
2. Event-store schema + raw journal.
3. Source observation logger.
4. Bybit private execution/order/position tracking.
5. Order reconciliation/state machine.
6. BBO recorder + orderbook sweep cost.
7. Реальный account snapshot и risk engine (после согласования параметров).
8. Event taxonomy + fixture corpus.
9. TCA report.
10. Shadow mode.

Не начинать ML, новый venue или арбитраж до выполнения первых 9.
