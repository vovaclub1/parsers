# Risk Management

## Цель

Risk engine не делает отрицательный edge положительным. Он не даёт одной
ошибке, ложному сигналу или хвостовому событию уничтожить счёт до того, как
мы успеем доказать/опровергнуть стратегию.

## 1. Уровни риска

1. **Signal risk** — ложная классификация события.
2. **Market risk** — цена пошла против позиции.
3. **Liquidity risk** — не можем войти/выйти по приемлемой цене.
4. **Execution risk** — reject, partial fill, unknown order state.
5. **Leverage/liquidation risk**.
6. **Portfolio risk** — несколько коррелированных позиций.
7. **Operational risk** — stale data, clock drift, process/network failure.
8. **Exchange/counterparty risk**.

## 2. Risk budget

Установить в конфиге, не хардкодить в функции:

- `MAX_RISK_PER_TRADE_USDT`;
- `MAX_NOTIONAL_PER_TRADE_USDT`;
- `MAX_TOTAL_NOTIONAL_USDT`;
- `MAX_OPEN_POSITIONS`;
- `MAX_DAILY_LOSS_USDT`;
- `MAX_DRAWDOWN_PCT`;
- `MAX_CONSECUTIVE_LOSSES`;
- `MAX_SPREAD_BPS`;
- `MAX_SLIPPAGE_BPS`;
- `MAX_QUOTE_AGE_MS`;
- `MAX_SIGNAL_AGE_MS`;
- `MAX_REJECTS_PER_HOUR`;
- `MAX_UNKNOWN_ORDERS`.

## 3. Available balance

Хардкод `balance = 100` неприемлем для production sizing.

Risk snapshot берётся через account API вне hot-path и кешируется:

- total equity;
- available balance;
- initial margin;
- maintenance margin;
- unrealized P&L;
- open positions;
- active orders.

На сигнале используется последний валидный snapshot при условии:

```
now - snapshot_ts <= MAX_ACCOUNT_SNAPSHOT_AGE
```

Если stale — skip, а не fallback к 100.

## 4. Position sizing

```
risk_notional = risk_budget / stop_distance
liquidity_notional = max size under slippage cap
margin_notional = available_margin * effective_leverage * utilization
hard_cap = configured max

notional = min(risk_notional, liquidity_notional, margin_notional, hard_cap)
```

Дополнительный confidence multiplier допустим только после calibrated model:

```
notional *= confidence_multiplier in [0.25, 1.0]
```

Никогда не повышать плечо, чтобы компенсировать слабый edge.

## 5. Slippage circuit breaker

Перед entry:

- quote свежий;
- spread ниже cap;
- depth достаточен;
- expected fill ниже price cap.

После fill:

- если actual slippage > emergency threshold;
- или fill движется частями в исчезающую ликвидность;

остановить остаток / reconcile, не chase без ограничения.

## 6. Daily kill-switch

Останавливает новые сделки при любом условии:

- realized + unrealized daily P&L < `-MAX_DAILY_LOSS`;
- drawdown > cap;
- N consecutive execution unknowns;
- N rejects;
- market data stale;
- private WS disconnected;
- reconciliation mismatch;
- clock offset beyond threshold;
- disk/event-store write failed;
- event burst beyond bulk limit;
- source parser начинает выдавать аномальный symbol count.

Kill-switch должен:

1. запретить новые entries;
2. не мешать закрывать позиции;
3. отправить Telegram alert;
4. требовать явного reset или безопасного auto-reset по заранее заданному правилу;
5. записать reason и evidence.

## 7. Event burst risk

Одна статья может содержать 5–20 монет. Нельзя выделять полный risk budget на
каждую независимо.

```
event_risk_budget = fixed max
per_coin_budget = event_risk_budget / N
```

С учётом liquidity. Если одновременно несколько статей — portfolio cap.

## 8. Correlation

Несколько altcoin longs на одном listing burst имеют высокий общий beta.
Использовать:

- max positions per direction;
- max total alt notional;
- BTC/ETH regime guard;
- sector / ecosystem buckets при наличии данных;
- event-level aggregate risk.

## 9. Stop design

Stop — не гарантия цены. На gap/тонком стакане execution хуже trigger.

Для каждого subtype исследовать:

- fixed percent;
- volatility normalized;
- time stop;
- liquidity collapse stop;
- hard catastrophic stop на сервере;
- soft strategy exit.

На сервере всегда должен быть failsafe hard stop после подтверждения позиции.

## 10. Partial fills

Если заполнено меньше requested:

- position size = фактический fill;
- TP/SL пересчитать;
- остаток cancel или reroute по правилам;
- не считать полный intent открытой позицией;
- P&L и margin считать по факту.

## 11. Unknown execution state

Самое опасное состояние. Правило:

```
unknown != rejected
unknown != filled
```

При unknown:

- блокировать новый ордер по symbol;
- query order by client id;
- query position;
- reconcile fills;
- если доказать состояние не удалось — kill-switch для entries.

## 12. Exchange failure

Fallback разрешён только если исходный ордер **доказанно не принят** или
idempotency гарантирует отсутствие дубля.

Нельзя автоматически переключаться venue при unknown state без portfolio
reconciliation: можно получить позиции на двух биржах.

## 13. Deployment ladder

1. unit/integration tests;
2. deterministic simulator;
3. fault injection;
4. shadow mode;
5. paper/testnet;
6. live minimum notional;
7. gradual scale.

На каждой ступени отдельный go/no-go checklist.

## 14. Fault injection scenarios

Обязательно тестировать:

- WS disconnect до/после send;
- ack потерян, fill произошёл;
- partial fill;
- duplicate execution message;
- REST timeout после принятия;
- stale price cache;
- order book sequence gap;
- account snapshot stale;
- exchange retCode rate limit/system restart;
- event-store disk full;
- process restart с открытой позицией;
- NTP skew;
- Telegram/source flood;
- malformed announcement.

## 15. Production readiness checklist

- [ ] реальный balance, не `100`;
- [ ] fills — источник entry/exit truth;
- [ ] slippage cap;
- [ ] portfolio limits;
- [ ] daily kill-switch;
- [ ] reconciliation on startup/reconnect;
- [ ] state survives restart;
- [ ] all skip/failure reason codes logged;
- [ ] dashboard/alerts;
- [ ] runbook восстановления;
- [ ] rollback tested;
- [ ] API keys без withdraw permission;
- [ ] canary size задан конфигом.
