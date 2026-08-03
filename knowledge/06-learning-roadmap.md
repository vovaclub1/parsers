# План обучения на 12 недель

## Принцип

Учёба считается завершённой только когда знания превращены в:

- код;
- тест;
- dataset;
- experiment report;
- production metric;
- конкретное решение по стратегии.

Нагрузка: 8–12 часов в неделю. Можно проходить быстрее, но нельзя пропускать
артефакты и проверки.

---

## Неделя 0 — постановка задачи и baseline

### Изучить

- формулу net expectancy;
- spread, slippage, fees, funding;
- разницу signal/send/ack/fill;
- текущую архитектуру проекта.

### Прочитать

- `knowledge/00-current-state.md`;
- `knowledge/01-profitability-model.md`;
- Larry Harris, *Trading and Exchanges*: части о orders, transaction costs,
  liquidity, informed trading;
- Barry Johnson, *Algorithmic Trading & DMA*: обзор, orders, transaction costs.

### Сделать

- зафиксировать текущие risk parameters;
- определить допустимый daily loss и max notional;
- создать baseline report по имеющимся логам;
- выписать, какие данные отсутствуют.

### Артефакт

`research/reports/BASELINE-001.md`.

### Проверка

Умеем объяснить, почему `open_ms=7` не означает прибыльную сделку.

---

## Неделя 1 — рыночная микроструктура

### Изучить

- bid/ask/mid;
- limit order book;
- maker/taker;
- spread/depth/market impact;
- adverse selection;
- order types: market, limit, IOC, FOK, post-only;
- queue priority — для будущих maker exits.

### Прочитать

- Harris: liquidity, order types, informed traders;
- Johnson: market microstructure, orders, execution tactics;
- `knowledge/04-execution-and-microstructure.md`;
- hftbacktest docs/examples.

### Сделать

- подписка на Bybit BBO/orderbook для затронутого symbol;
- функция `sweep_vwap(book, side, notional)`;
- расчет spread/depth/slippage curves.

### Артефакт

`tools/orderbook_cost.py` + unit tests.

### Проверка

Для заданного стакана и $25/$100/$500 получаем точный expected VWAP и worst
fill; доказано тестами.

---

## Неделя 2 — exchange APIs и execution state

### Изучить

- Bybit WS Trade guideline;
- private order/execution/position streams;
- idempotency/client order ID;
- reconnect/reconciliation;
- rate limits и retCodes;
- partial fills.

### Прочитать

- Bybit official: Connect, WS Trade Guideline, Place Order, Order/Execution/
  Position streams, Rate Limits;
- Gate API v4 futures / WS;
- Hyperliquid API: signing, limits, nonce, WS timeout/heartbeat.

### Сделать

- order state machine;
- private WS consumer;
- reconcile по `orderLinkId`;
- startup reconciliation.

### Артефакт

`execution/order_tracker.py` + fault-injection integration tests.

### Проверка

Сценарии ack lost / fill happened, partial fill, duplicate execution,
reconnect не создают дубль и не теряют позицию.

---

## Неделя 3 — event-store и observability

### Изучить

- event sourcing;
- WAL / durability;
- timestamp semantics;
- schema evolution;
- telemetry и SLO.

### Сделать

- SQLite WAL schema из `02-data-and-event-store.md`;
- raw JSONL journal;
- signals/orders/fills/positions/source observations;
- BBO snapshots;
- report latency and slippage.

### Артефакт

`storage/` + migrations + `tools/trade_report.py`.

### Проверка

Одна сделка полностью восстанавливается после restart без чтения stdout.

---

## Неделя 4 — статистика event study

### Изучить

- returns / abnormal returns;
- event windows;
- bootstrap confidence intervals;
- heavy tails;
- clustered observations;
- mean vs median;
- selection bias.

### Прочитать

- Ante, *Market Reaction to Exchange Listings of Cryptocurrencies*;
- Hashemi Joo et al., *Announcement Effects in the Cryptocurrency Market*;
- Brattle, *How Event Studies Can Be Applied to Crypto Markets*;
- Cryptocurrency Exchange Listings (AFT 2025 extended abstract).

### Сделать

- собрать исторические listing/delisting events;
- нормализовать taxonomy;
- event study по horizons;
- отделить publication time от our-arrival simulation.

### Артефакт

`research/event_study.py` + `reports/EVENT-001.md`.

### Проверка

Есть net move distributions и CI по subtype, а не один средний pump.

---

## Неделя 5 — realistic backtesting

### Изучить

- event-driven replay;
- latency model;
- L2 fill model;
- partial fills;
- implementation shortfall;
- data leakage.

### Прочитать

- hftbacktest architecture/examples;
- NautilusTrader backtest/live parity concepts;
- `03-research-protocol.md`.

### Сделать

- replay event + market data;
- empirical latency samples;
- IOC/market slippage models;
- fees/funding/instrument rules;
- baseline fixed-horizon strategies.

### Артефакт

`backtest/` + deterministic golden tests.

### Проверка

Backtest fill не может быть лучше доступного стакана, future data не читается.

---

## Неделя 6 — backtest overfitting

### Изучить

- train/validation/test по времени;
- purging/embargo;
- multiple testing;
- Deflated Sharpe Ratio;
- PBO/CSCV;
- robustness/sensitivity.

### Прочитать

- Bailey et al., *Probability of Backtest Overfitting*;
- Bailey & López de Prado, *Deflated Sharpe Ratio*;
- López de Prado, *Advances in Financial Machine Learning*: financial CV,
  backtest overfitting (не копировать ML рецепты без данных).

### Сделать

- experiment registry;
- purged group time split;
- trial counter;
- bootstrap/DSR report;
- untouched holdout.

### Артефакт

`research/experiments/` + experiment template.

### Проверка

Невозможно незаметно менять параметры после просмотра test period.

---

## Неделя 7 — risk engine

### Изучить

- position sizing;
- risk of ruin;
- drawdown;
- VaR/CVaR limitations;
- leverage/liquidation;
- portfolio concentration;
- kill-switch.

### Сделать

- account snapshot;
- sizing = min(risk, liquidity, margin, cap);
- portfolio limits;
- daily loss/drawdown breaker;
- stale data/unknown order breaker.

### Артефакт

`risk/engine.py` + scenario tests.

### Проверка

Ни один сигнал не обходит лимиты, unknown state блокирует новые entries, exits
остаются разрешены.

---

## Неделя 8 — source intelligence

### Изучить

- source lead/lag;
- dedup/event identity;
- precision/recall;
- concept drift;
- uptime/reliability scoring.

### Сделать

- source observation store;
- event matching;
- leaderboard: lead time, misses, false positives;
- автоматическое отключение деградировавшего source только от trading, не
  от observation.

### Артефакт

`tools/source_report.py`.

### Проверка

Знаем, какой источник действительно создаёт capturable edge, а какой только
дублирует быстрее/медленнее.

---

## Неделя 9 — event classification

### Изучить

- deterministic parser design;
- taxonomy;
- confidence / abstention;
- regression corpus;
- NLP только как fallback.

### Сделать

- `Event` dataclass/schema;
- subtype rules;
- corpus реальных announcements;
- false-positive/false-negative report;
- unknown/ambiguous → skip, не trade.

### Артефакт

`signals/classifier.py` + fixtures.

### Проверка

Margin pair removal не считается full delisting; Alpha/Earn/Web3 отделены;
каждое решение объяснимо reason code.

---

## Неделя 10 — execution experiments

### Гипотезы

- market vs IOC aggressive limit;
- Bybit slippage cap 25–200 bps;
- route Bybit vs Gate;
- immediate vs 50/100/250ms delay;
- size vs slippage curve;
- exit families.

### Сделать

- preregistered experiments;
- shadow orders;
- TCA reports;
- sensitivity analysis.

### Артефакт

`reports/EXEC-*.md`.

### Проверка

Выбран execution rule по net expectancy и tail loss, а не по fastest send.

---

## Неделя 11 — shadow и canary

### Сделать

- shadow mode 24/7;
- compare model fill vs observed book;
- testnet state machine;
- live minimum-notional canary;
- kill-switch drill;
- restart/reconciliation drill.

### Артефакт

`runbooks/PRODUCTION.md`, `runbooks/INCIDENTS.md`.

### Проверка

Live canary и shadow объяснимо расходятся; все позиции reconciled; recovery
проверен, не только описан.

---

## Неделя 12 — решение о стратегии

### Подвести итог

По каждому subtype:

- event count;
- source lead time;
- net expectancy;
- confidence interval;
- slippage curve;
- drawdown/CVaR;
- capacity;
- live vs backtest gap.

### Решение

- `SCALE` — подтверждено;
- `CANARY_MORE` — edge вероятен, данных мало;
- `SHADOW_ONLY` — execution/edge не готов;
- `STOP` — net edge отсутствует.

### Артефакт

`reports/STRATEGY-DECISION-001.md`.

---

# Параллельный учебный трек Python / Systems

По мере реализации:

- asyncio/threading ownership;
- WebSocket protocols;
- TCP half-open / keepalive;
- Decimal/instrument precision;
- SQLite WAL and transactions;
- Parquet/Polars/DuckDB;
- property-based testing (Hypothesis);
- fault injection;
- structured logging/metrics;
- Docker resource/security controls.

Каждая тема привязана к реальному модулю, абстрактный курс не нужен.

# Что изучать позже, не сейчас

- ML/gradient boosting — после event-store и 500+ качественных events;
- deep learning/RL — только если простой conditional table исчерпан и есть
  достаточная выборка;
- C++/Rust rewrite — только если profiler показывает Python bottleneck после
  доказанного edge;
- co-location — только если source/exchange latency, а не strategy economics,
  является ограничением.
