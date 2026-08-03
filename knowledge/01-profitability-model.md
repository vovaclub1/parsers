# Модель прибыльности

## 1. Базовая единица — не сигнал, а завершённая сделка

Для сделки `i`:

```
gross_pnl_i = direction_i * qty_i * (exit_fill_i - entry_fill_i)

net_pnl_i = gross_pnl_i
            - entry_fee_i
            - exit_fee_i
            - funding_i
            - borrow_cost_i
            - other_cost_i
```

Где `direction = +1` для long, `-1` для short.

В процентах от фактически занятой маржи:

```
return_on_margin_i = net_pnl_i / margin_used_i
```

Нельзя подставлять в `entry_fill` цену из `price_cache`: это reference price,
не исполнение.

## 2. Expectancy

```
expectancy = P(win) * E[win]
             - P(loss) * E[loss]
             - E[costs]
```

Эквивалентно — среднее `net_pnl` по сделкам. Но одного среднего мало:
распределение новостных сделок имеет тяжёлые хвосты и выбросы.

Обязательные показатели:

- mean / median net P&L;
- win rate;
- average win / loss;
- payoff ratio;
- profit factor;
- max drawdown;
- expected shortfall (CVaR 95/99%);
- Sharpe / Sortino — только с оговоркой о малой выборке и non-IID;
- bootstrap confidence interval expectancy;
- P&L concentration: доля прибыли от top-1 / top-5 сделок;
- результат без лучшей сделки;
- result by source / event subtype / month / liquidity bucket.

## 3. Декомпозиция информационного edge

Для long listing:

```
raw_event_move(h) = mid(t_signal + h) / mid(t_signal) - 1
```

Для short delisting знак инвертируется.

Но бот видит не `t_signal` биржи, а `t_arrival`:

```
source_delay = t_arrival - t_event_publication
parser_delay = t_order_decision - t_arrival
execution_delay = t_fill - t_order_decision
```

Оставшийся edge:

```
capturable_move(h) = direction * (mid(t_arrival + h) / executable_entry - 1)
```

Важный вопрос исследования: **сколько движения осталось после нашего arrival?**
Если 90% движения произошло раньше, оптимизация 2 мс ничего не спасёт — нужен
более ранний источник или другая стратегия (например, fade после overshoot).

## 4. Transaction Cost Analysis (TCA)

### Arrival mid

```
arrival_mid = (best_bid_arrival + best_ask_arrival) / 2
```

### Effective spread

Для buy:

```
effective_spread_bps = 2 * (fill_price - arrival_mid) / arrival_mid * 10_000
```

Для sell знак инвертируется.

### Implementation shortfall

```
IS_bps = direction * (avg_fill_price - arrival_mid) / arrival_mid * 10_000
         + fees_bps
```

Для long положительный IS = стоимость; для short формулу нормализовать тем же
направлением.

### Slippage по стакану

Для размера `Q`:

```
expected_vwap(Q) = сумма(price_level * consumed_qty) / Q
```

Считать для сетки размеров:

- $25;
- $50;
- $100;
- $250;
- $500;
- $1,000.

Это даёт liquidity curve и максимальный размер до заданного slippage cap.

### Markout / adverse selection

После fill:

```
markout_h_bps = direction * (mid(t_fill + h) - fill_price) / fill_price * 10_000
```

Горизонты:

- 100 мс;
- 250 мс;
- 500 мс;
- 1 с;
- 3 с;
- 5 с;
- 15 с;
- 60 с.

Если markout сразу отрицательный, мы регулярно покупаем локальный пик или
продаём локальное дно — скорость не превращается в edge.

## 5. Минимально приемлемая сделка

Перед order send:

```
expected_move_bps(event, features, horizon)
    > spread_bps
      + expected_slippage_bps(size)
      + fees_bps
      + safety_margin_bps
```

`expected_move_bps` сначала строится как таблица historical conditional means,
не ML-модель. Пример сегментов:

- Binance full spot listing;
- Upbit KRW listing;
- Bithumb listing;
- Binance full spot delisting;
- futures-only delisting;
- monitoring-tag event;
- liquidity bucket;
- source;
- arrival delay bucket.

## 6. Position sizing

Нельзя использовать только `balance * fixed_percent`.

### Ограничение по ликвидности

```
size_liquidity = max Q, где expected_slippage(Q) <= slippage_cap
```

### Ограничение по риску стопа

```
size_risk = risk_budget_usdt / stop_distance_fraction
```

### Ограничение по leverage / margin

```
size_margin = available_margin * effective_leverage * utilization_limit
```

Итог:

```
notional = min(size_liquidity, size_risk, size_margin, hard_notional_cap)
```

### Kelly

Full Kelly для тяжёлохвостых новостных сделок слишком опасен. Если применять,
то только fractional Kelly (например 0.1–0.25) на консервативной нижней оценке
edge и variance, после большой out-of-sample выборки.

## 7. Break-even анализ

Для каждого event subtype считать:

```
break_even_move_bps = entry_cost_bps + exit_cost_bps + funding_bps
```

Если медианное capturable движение не превышает break-even хотя бы в 2–3 раза,
стратегия хрупкая: небольшой рост latency или spread сделает её убыточной.

## 8. Важные срезы

- источник сигнала;
- биржа события;
- venue входа;
- event subtype;
- delay bucket (0–50, 50–100, 100–250, 250–500, 500–1000, >1000 мс);
- market cap bucket;
- depth / spread bucket;
- pre-event return;
- time of day;
- volatility regime;
- success/fallback path (sync WS / async WS / REST / Gate);
- long vs short.

## 9. Решение «торговать или нет»

Первая production-версия должна быть rule-based:

```
TRADE, если:
  event_subtype входит в whitelist
  AND source исторически достаточно быстрый
  AND quote_age_ms < threshold
  AND spread_bps < max_spread
  AND depth_usdt_at_cap >= desired_notional
  AND predicted_edge_bps > total_cost_bps + margin
  AND portfolio limits позволяют
ELSE SKIP с reason_code
```

Каждый skip логируется. Иначе мы не узнаем opportunity cost и не сможем
улучшать фильтры.

## 10. Факт, который нельзя обещать

Ни одна база знаний, скорость, ML или аудит не гарантирует прибыль.
Правильная цель — построить процесс, который:

- быстро отвергает ложные идеи;
- измеряет реальный edge;
- ограничивает потери при ошибке;
- масштабирует только подтверждённый результат.
