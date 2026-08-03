# Backlog экспериментов

Статусы: `DRAFT`, `PREREGISTERED`, `RUNNING`, `COMPLETE`, `REJECTED`.

## Приоритетная матрица

| ID | Гипотеза | Ценность | Стоимость | Риск самообмана | Приоритет |
|---|---|---:|---:|---:|---:|
| HYP-001 | Full spot delisting имеет capturable short edge | высокая | средняя | средний | P0 |
| HYP-002 | Upbit KRW listing имеет capturable Bybit long edge | высокая | средняя | средний | P0 |
| HYP-003 | IOC cap улучшает tail P&L vs uncapped market | высокая | средняя | низкий | P0 |
| HYP-004 | Source lead time объясняет большую часть P&L | высокая | низкая после store | низкий | P0 |
| HYP-005 | Fixed TP/SL хуже time/MFE-based exit | средняя | средняя | высокий | P1 |
| HYP-006 | Gate иногда дешевле Bybit после all-in costs | средняя | средняя | средний | P1 |
| HYP-007 | Monitoring tag — отдельный прибыльный short event | средняя | низкая | высокий | P1 |
| HYP-008 | Delay 50–250ms улучшает entry после overshoot | высокая | средняя | высокий | P1 |
| HYP-009 | Liquidity filter повышает expectancy/cuts tails | высокая | низкая | низкий | P0 |
| HYP-010 | ML meta-label улучшает rule baseline | неизвестно | высокая | очень высокий | P3 |

---

## HYP-001 — Full spot delisting short edge

**Статус:** DRAFT  
**Гипотеза:** после нашего empirical arrival full spot delisting Binance имеет
отрицательный Bybit markout, превышающий all-in costs.

### Не смешивать

- futures-only delisting;
- margin pair removal;
- collateral/loan removal;
- Alpha/Web3 removal;
- monitoring warning.

### Primary metric

Net return 5s после executable entry.

### Secondary

1/3/15/60s, MFE/MAE, spread/slippage, capacity.

### Success

- untouched test mean > costs + 20 bps;
- bootstrap lower CI > 0;
- profit не зависит от top-3 events;
- положительно минимум в 2 time periods.

---

## HYP-002 — Upbit KRW listing long edge

**Статус:** DRAFT  
**Гипотеза:** первый публичный Upbit KRW listing signal создаёт positive Bybit
perp markout после latency/costs.

### Важные вопросы

- перп уже существует на Bybit до Upbit listing?
- deposits/trading timing;
- announcement vs market-list diff — кто первый?
- Korean-language parser latency;
- pre-announcement leakage.

### Segments

- market cap;
- existing CEX count;
- Bybit depth;
- source;
- arrival delay.

---

## HYP-003 — Slippage cap

**Статус:** PREREGISTER после TCA store.

Сравнить:

- uncapped current behavior;
- Bybit market + Percent 25/50/75/100/200 bps;
- IOC aggressive limit same caps.

Метрики:

- fill rate;
- partial fill;
- missed move;
- average/tail slippage;
- net P&L;
- CVaR.

Не выбирать cap по максимальному mean: нужен robust plateau.

---

## HYP-004 — Source value

**Статус:** blocked by source observation store.

Для каждого event/source:

- arrival rank;
- lag to first;
- miss rate;
- false-positive rate;
- uptime;
- P&L conditional on being first;
- marginal value: сколько событий source дал, которых не было раньше.

Решение:

- keep/trade;
- observe-only;
- remove.

---

## HYP-005 — Exit policy

**Статус:** blocked by fills + MFE/MAE.

Текущие levels:

- listing: SL -8%, TP +4.5% partial, trailing 3.5%;
- delisting: SL +5%, TP -8/-15/-45%.

Они выглядят как hand-tuned constants; evidence отсутствует.

Сравнить заранее:

- 1/3/5/15/60s fixed horizon;
- fixed stop/time exit;
- volatility-scaled;
- trailing;
- partial exit families.

Оценивать stability around parameter, не best point.

---

## HYP-006 — Smart venue routing

**Статус:** blocked by Gate/Bybit BBO + fills.

Сравнить all-in:

```
spread + sweep_slippage(size) + fee + latency penalty + reject probability
```

Gate fallback может иногда быть лучше Bybit primary, но это нужно измерить.

---

## HYP-007 — Monitoring tag

**Статус:** DRAFT.

Monitoring tag — warning, не delisting. Может дать ранний отрицательный edge,
но отличается по information strength и false positives.

Исследовать отдельно:

- eventual delisting probability;
- immediate markout;
- reversals;
- liquidity changes;
- holding horizon.

---

## HYP-008 — Immediate vs delayed entry

**Статус:** blocked by millisecond event data.

Иногда immediate market order покупает/продаёт экстремальный overshoot.
Сравнить entry delays:

- 0;
- 25ms;
- 50ms;
- 100ms;
- 250ms;
- 500ms;
- 1s.

Учитывать missed trades. Цель не «меньше latency любой ценой», а лучший net
entry.

---

## HYP-009 — Liquidity filter

**Статус:** P0 после BBO recorder.

Trade только если:

- spread < X;
- depth at cap > Y × desired size;
- quote age < Z;
- expected slippage < cap.

Ожидание: меньше сделок, выше expectancy, существенно лучше tail risk.

---

## HYP-010 — ML meta-label

**Статус:** REJECTED до data readiness.

Не прогнозировать direction. Event уже даёт direction. Позже модель отвечает:

> Есть ли после costs достаточный edge, чтобы торговать этот event?

Features только available-at-decision-time. Сначала logistic regression и
GBDT. Deep learning только если есть доказанный incremental value.

---

# Template нового эксперимента

```yaml
id:
status: DRAFT
owner:
created_at:
hypothesis:
mechanism:
data_version:
event_taxonomy_version:
universe:
train_period:
validation_period:
test_period:
entry_rule:
exit_rule:
latency_model:
fill_model:
cost_model:
risk_model:
primary_metric:
secondary_metrics:
success_criteria:
stop_criteria:
parameter_grid_predeclared:
number_of_prior_trials:
known_limitations:
```

# Реестр результатов

| ID | Commit/Data | Sample | OOS net expectancy | CI | Decision |
|---|---|---:|---:|---:|---|
| — | — | — | — | — | данных пока нет |
