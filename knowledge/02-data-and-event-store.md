# Данные и event-store

## Почему это первый этап развития

Без данных нельзя ответить на главные вопросы:

- какой источник реально первый;
- сколько движения осталось на arrival;
- какой fill получили;
- сколько съели spread, slippage и комиссии;
- какие event subtype прибыльны;
- работают ли текущие TP/SL;
- стоит ли оптимизировать ещё 2 мс;
- почему конкретная сделка была убыточной.

Логи stdout и Telegram для этого недостаточны: они неполные, плохо
нормализованы и не обеспечивают reproducibility.

## Архитектура хранения

### Рекомендуемый минимум

- SQLite в WAL mode для метаданных и состояния сделок;
- append-only JSONL как аварийный raw journal;
- Parquet для market data и аналитики;
- UTC timestamps в integer nanoseconds;
- отдельное хранение raw payload — парсер можно перепроверить позже.

Почему не сразу PostgreSQL/ClickHouse:

- поток событий мал;
- SQLite проще, надёжнее и достаточно для одного процесса;
- Parquet хорошо читается Polars/DuckDB;
- усложнение инфраструктуры не создаёт edge.

Переход на ClickHouse оправдан, когда пишем много L2 order-book updates по
нескольким venues 24/7.

## Часы и timestamps

Для каждого события хранить:

- `ts_exchange_ns` — время внутри payload, если есть;
- `ts_socket_recv_ns` — сразу после получения bytes из socket;
- `ts_parsed_ns`;
- `ts_decision_ns`;
- `ts_order_send_start_ns`;
- `ts_order_send_done_ns`;
- `ts_ack_ns`;
- `ts_first_fill_ns`;
- `ts_last_fill_ns`;
- `ts_exit_ns`.

Использовать:

- `time.time_ns()` — wall clock для сопоставления систем;
- `time.perf_counter_ns()` — монотонные локальные durations;
- chrony/NTP monitoring — clock offset логировать;
- один host / регион на эксперимент, иначе latency не сравнима.

## Таблица `signals`

```sql
CREATE TABLE signals (
    signal_id TEXT PRIMARY KEY,
    event_key TEXT NOT NULL,
    source TEXT NOT NULL,
    source_message_id TEXT,
    event_exchange TEXT,
    event_type TEXT NOT NULL,
    event_subtype TEXT NOT NULL,
    symbol TEXT NOT NULL,
    raw_payload TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    ts_exchange_ns INTEGER,
    ts_recv_ns INTEGER NOT NULL,
    ts_parsed_ns INTEGER NOT NULL,
    classification_confidence REAL,
    traded INTEGER NOT NULL,
    skip_reason TEXT
);
```

`event_key` должен объединять дубли одного публичного события из разных
источников. Пример: hash(normalized exchange + subtype + symbols + date).

## Таблица `orders`

```sql
CREATE TABLE orders (
    client_order_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    requested_qty REAL NOT NULL,
    requested_notional REAL,
    slippage_cap_bps REAL,
    route TEXT NOT NULL,
    state TEXT NOT NULL,
    exchange_order_id TEXT,
    ret_code INTEGER,
    ret_message TEXT,
    ts_send_start_ns INTEGER NOT NULL,
    ts_send_done_ns INTEGER,
    ts_ack_ns INTEGER,
    FOREIGN KEY(signal_id) REFERENCES signals(signal_id)
);
```

Состояния должны быть monotonic и event-sourced, а не только последнее поле.
Лучше отдельная таблица `order_events`.

## Таблица `fills`

```sql
CREATE TABLE fills (
    fill_id TEXT PRIMARY KEY,
    client_order_id TEXT NOT NULL,
    exchange_order_id TEXT,
    price REAL NOT NULL,
    qty REAL NOT NULL,
    fee REAL NOT NULL DEFAULT 0,
    fee_asset TEXT,
    liquidity_role TEXT,
    ts_fill_ns INTEGER NOT NULL,
    FOREIGN KEY(client_order_id) REFERENCES orders(client_order_id)
);
```

## Таблица `market_snapshots`

На arrival, decision, send, ack, first fill:

```sql
CREATE TABLE market_snapshots (
    signal_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    stage TEXT NOT NULL,
    ts_ns INTEGER NOT NULL,
    bid REAL,
    ask REAL,
    bid_qty REAL,
    ask_qty REAL,
    mid REAL,
    spread_bps REAL,
    depth_10bps_bid REAL,
    depth_10bps_ask REAL,
    depth_25bps_bid REAL,
    depth_25bps_ask REAL,
    orderbook_json TEXT,
    PRIMARY KEY(signal_id, venue, stage)
);
```

Raw L2 updates лучше писать в Parquet, а не в SQLite.

## Таблица `positions`

```sql
CREATE TABLE positions (
    position_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_qty REAL NOT NULL,
    entry_vwap REAL NOT NULL,
    exit_qty REAL,
    exit_vwap REAL,
    entry_fee REAL NOT NULL DEFAULT 0,
    exit_fee REAL NOT NULL DEFAULT 0,
    funding REAL NOT NULL DEFAULT 0,
    realized_pnl REAL,
    max_favorable_excursion_bps REAL,
    max_adverse_excursion_bps REAL,
    close_reason TEXT,
    ts_open_ns INTEGER NOT NULL,
    ts_close_ns INTEGER
);
```

## Таблица `source_observations`

Нужна для гонки источников, даже если сигнал уже отторгован:

```sql
CREATE TABLE source_observations (
    event_key TEXT NOT NULL,
    source TEXT NOT NULL,
    ts_recv_ns INTEGER NOT NULL,
    message_id TEXT,
    raw_payload TEXT,
    PRIMARY KEY(event_key, source, message_id)
);
```

Тогда можно считать:

- win rate источника;
- median/p95 lag до первого;
- пропуски;
- дубли;
- source degradation по времени.

## Reason codes

Каждое решение обязано иметь reason code:

### Skip

- `UNKNOWN_SYMBOL`
- `EVENT_SUBTYPE_BLOCKED`
- `DUPLICATE_L1`
- `DUPLICATE_L2`
- `STALE_MARKET_DATA`
- `SPREAD_TOO_WIDE`
- `INSUFFICIENT_DEPTH`
- `EXPECTED_EDGE_TOO_LOW`
- `RISK_LIMIT`
- `NO_VENUE`
- `INVALID_INSTRUMENT_LIMITS`
- `BULK_GUARD`

### Failure

- `ORDER_REJECTED`
- `ACK_TIMEOUT`
- `FILL_TIMEOUT`
- `PARTIAL_FILL`
- `WS_DEAD`
- `REST_RECONCILE_FAILED`
- `TP_SL_FAILED`
- `POSITION_MISMATCH`

## Market-data collection

### Binance

Официальный `binance-public-data` даёт historical trades / aggTrades / klines,
но для нашего execution research лучше live L2 snapshots + diff stream.

### Bybit

Нужны:

- `orderbook.1` или `orderbook.50`;
- public trades;
- private order;
- private execution;
- private position.

### Upbit / Bithumb

Для события — market list / notice. Для цены входа всё равно критичен стакан
venue исполнения (Bybit/Gate), а не только event exchange.

## Retention

- raw signals: постоянно;
- orders/fills/positions: постоянно;
- BBO/summary: постоянно;
- full L2 around events: окно от `t_event - 60s` до `t_event + 15m`;
- continuous L2 всех 700 инструментов слишком дорого — использовать rolling
  in-memory buffer и сбрасывать на диск только при событии;
- logs: ротация, 30–90 дней.

## Rolling buffer

До события неизвестно, какой symbol понадобится. Возможные варианты:

1. Писать BBO для всех Bybit linear symbols постоянно — дёшево.
2. Писать L2 только для наиболее вероятного universe.
3. При событии включать full L2 немедленно, но pre-event стакан потерян.
4. Держать compact ring buffer top-10 levels для всех symbols на 60–120 сек.

Рекомендация: BBO для всех + ring buffer top-10 для всех активных symbols,
с flush только для затронутой монеты.

## Data quality checks

- monotonic timestamps внутри процесса;
- sequence gaps у order book;
- crossed book (`bid >= ask`) — reject snapshot;
- stale quote age;
- duplicate fills;
- order state cannot regress;
- filled qty <= requested qty (с учётом known exchange semantics);
- P&L reconciliation с биржей;
- daily checksum / row counts;
- schema version в каждой записи.

## Definition of Done для event-store

- одна реальная testnet/paper сделка восстанавливается полностью;
- frame send, ack, fills и position связаны одним client_order_id;
- P&L считается из fills, не из price_cache;
- restart не теряет незавершённую позицию;
- при старте выполняется reconciliation с биржей;
- аналитический notebook/CLI строит latency и slippage report без парсинга логов.
