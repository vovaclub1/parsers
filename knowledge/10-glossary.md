# Глоссарий

## Ack

Подтверждение биржи, что запрос принят/обработан на определённом уровне.
Ack `order.create` не обязательно означает fill.

## Adverse selection

Ситуация, когда контрагент имеет более свежую информацию. В нашем случае
market makers успевают снять хорошие quotes после новости, и агрессивный
ордер получает плохую цену.

## Arrival price

Цена/BBO в момент, когда стратегия получила данные, достаточные для решения.
Основной benchmark implementation shortfall.

## BBO

Best Bid and Offer: лучшая цена покупки и продажи плюс объёмы.

## Capturable edge

Движение, оставшееся после фактического arrival нашей системы, которое можно
исполнить с учётом spread/depth/latency.

## CVaR / Expected Shortfall

Средний убыток в худшем хвосте распределения, например худшие 5% outcomes.
Полезнее одного worst case, но зависит от качества выборки.

## Deflated Sharpe Ratio

Корректировка наблюдаемого Sharpe на multiple testing, selection bias и
ненормальность returns.

## Depth

Доступный объём в стакане на нескольких уровнях цены.

## Event key

Стабильный идентификатор одного публичного события, объединяющий сообщения
разных sources.

## Event study

Статистический анализ returns/liquidity в окне вокруг события относительно
benchmark/expected return.

## Execution truth

Фактические fills/position биржи, а не локальный intent или `ws.send()`.

## Fill

Исполненная часть order: цена, количество, комиссия, timestamp.

## Implementation shortfall

Разница между фактическим результатом исполнения и arrival benchmark, включая
прямые издержки.

## IOC

Immediate-or-Cancel: доступная часть исполняется немедленно, остаток отменяется.
Позволяет ограничить цену, но допускает partial/no fill.

## L1 дедуп

Короткая in-memory блокировка near-simultaneous дублей источников.

## L2 дедуп

Персистентная блокировка повторной торговли одного event/coin после restart.
В проекте TTL 30 дней, потому что независимые повторные события реальны.

## Latency

Нужно всегда уточнять компонент:

- publication → source arrival;
- socket receive → parse;
- parse → decision;
- decision → send;
- send → ack;
- send → fill.

## Markout

Движение mid-price после fill на заданном горизонте с направлением сделки.
Показывает adverse selection / качество entry.

## Market impact

Изменение доступных цен из-за собственного ордера и реакции рынка.

## MFE

Maximum Favorable Excursion: максимальное движение в нашу сторону за жизнь
сделки.

## MAE

Maximum Adverse Excursion: максимальное движение против позиции.

## Mid

```
mid = (best_bid + best_ask) / 2
```

Не является гарантированно исполнимой ценой.

## Net expectancy

Средний P&L после fees, spread, slippage, funding и иных costs.

## Notional

Рыночная стоимость позиции. Не равно margin при leverage.

## PBO

Probability of Backtest Overfitting: вероятность, что выбранный in-sample
победитель будет слабым out-of-sample.

## Purging / embargo

Удаление overlapping observations и временной зазор между train/test для
предотвращения leakage в financial labels.

## Quote age

Время между timestamp последнего market update и decision. Старый quote нельзя
использовать для slippage/risk checks.

## Reconciliation

Сопоставление локального state с order/fills/position биржи, особенно после
timeout/reconnect/restart.

## Slippage

Разница между benchmark/expected execution price и фактической fill price.
Всегда указывать benchmark.

## Spread

```
spread_bps = (ask - bid) / mid * 10_000
```

## State machine

Допустимые состояния order/position и переходы между ними. Нужна, чтобы не
смешивать sent/acked/filled/rejected/unknown.

## TCA

Transaction Cost Analysis: измерение spread, slippage, fees, impact, delay и
качества исполнения.

## TTL

Time-to-live: срок действия дедуп-записи или кеша.

## Unknown order state

Request мог быть принят, но подтверждение потеряно. Нельзя считать ни fill,
ни reject; требуется reconciliation.

## VWAP

Volume-Weighted Average Price. Для собственного order-book sweep:

```
VWAP = sum(price_i * qty_i) / sum(qty_i)
```

## Walk-forward

Последовательное обучение/настройка на прошлом и проверка на следующем
неиспользованном периоде.
