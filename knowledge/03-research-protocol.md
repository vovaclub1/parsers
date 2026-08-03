# Протокол исследования стратегии

## Цель

Не найти параметры, которые красиво выглядят на истории, а оценить:

> При каких типах событий, источниках, задержках, market regimes и размерах
> позиции стратегия имеет положительное net expectancy после всех издержек?

## 1. Единица наблюдения

Один независимый публичный event, а не одно сообщение источника.

Если один Binance announcement пришёл через:

- Binance API;
- Tree of Alpha;
- три Telegram-канала;
- CoinListing;

это **одно событие**, а не шесть observations. Иначе стандартные ошибки и
sample size будут завышены.

## 2. Event taxonomy до анализа

### Listing

- `SPOT_FULL_LISTING`
- `PERP_LISTING`
- `MARGIN_LISTING`
- `NEW_QUOTE_PAIR`
- `REGIONAL_LISTING` (Upbit KRW, Bithumb KRW)
- `ALPHA_WEB3_LISTING`
- `RELISTING`
- `EARN_ONLY` (не торговать)

### Delisting

- `SPOT_FULL_DELISTING`
- `PERP_DELISTING`
- `MARGIN_ONLY_REMOVAL`
- `PAIR_ONLY_REMOVAL`
- `MONITORING_WARNING`
- `DEPOSIT_WITHDRAWAL_SUSPENSION`
- `ALPHA_WEB3_REMOVAL`
- `COLLATERAL_ONLY_REMOVAL`

Тип должен определяться явными правилами и иметь regression fixtures.

## 3. Пререгистрация гипотезы

Шаблон:

```yaml
id: HYP-001
created_at: 2026-08-03
hypothesis: >
  Binance full spot delisting имеет отрицательный 5-second markout на Bybit,
  достаточный для short после taker fee и observed slippage.
universe: Bybit linear USDT symbols
period_train: 2024-01-01..2025-12-31
period_validation: 2026-01-01..2026-04-30
period_test: 2026-05-01..2026-08-01
entry:
  trigger: first source arrival
  latency_ms: empirical distribution, not zero
  execution: IOC aggressive limit with 75 bps cap
exit:
  candidates_predeclared: [1s, 3s, 5s, 15s, 60s]
costs:
  fees: actual venue schedule
  slippage: replayed L2
metrics:
  primary: mean net return at 5s
  secondary: median, CVaR, MFE, MAE
success:
  test_expectancy_bps_gt: 20
  bootstrap_ci_lower_gt: 0
trials_so_far: 1
```

## 4. Train / validation / test

Разделять **по времени**, не случайным shuffle:

- train — построение правил / features;
- validation — выбор ограниченного числа параметров;
- test — трогается один раз;
- после test изменение гипотезы = новый experiment id.

Почему: рыночные режимы меняются, соседние события зависимы, случайный split
даёт leakage.

## 5. Purging и embargo

Если label одного события использует цены следующие 60 минут, события в этом
окне не должны одновременно попадать в train и test. Иначе training видит
часть будущего test label.

Использовать:

- purged time-series split;
- embargo после test interval;
- group split по `event_key`;
- при нескольких symbols в одной статье — group по announcement id.

## 6. Baselines

Стратегия должна победить:

1. no-trade;
2. random direction с тем же временем/размером;
3. immediate market order;
4. delayed entries (100/250/500/1000ms);
5. IOC limit с разными caps;
6. simple fixed exits;
7. buy-and-hold / BTC-regime adjusted abnormal return.

Если сложный фильтр не лучше простой baseline out-of-sample — он не нужен.

## 7. Event study

Для каждого event строить:

- returns `[-60s, -10s, -1s, 0, +100ms, +500ms, +1s, +3s, +5s, +15s,
  +60s, +5m, +15m, +1h]`;
- abnormal return относительно BTC/ETH market factor;
- volume / spread / depth change;
- source arrival timestamps;
- executable price curve по размерам.

Затем aggregate:

- mean / median;
- quantiles;
- bootstrap CI;
- sign test / Wilcoxon как robustness;
- cluster standard errors по coin и announcement date;
- отдельно listing/delisting subtype.

## 8. Execution realism

Backtest обязан учитывать:

- empirical source latency;
- parser/order latency distribution, а не среднее;
- spread at arrival;
- L2 depth и partial fills;
- exchange lot/tick/min notional;
- fees и funding;
- market order slippage cap;
- reject / timeout / unknown state;
- position already open;
- simultaneous events и portfolio limits.

OHLC candle не подходит: в одной 1s/1m свече неизвестен порядок high/low и
нет executable liquidity.

## 9. Multiple testing

Каждый проверенный вариант увеличивает шанс случайного «победителя».
Журналировать:

- число exit horizons;
- filters;
- thresholds;
- event subtypes;
- sizing rules;
- datasets.

Использовать:

- Deflated Sharpe Ratio;
- Probability of Backtest Overfitting (PBO/CSCV), когда вариантов много;
- White's Reality Check / Hansen SPA при сравнении правил;
- Bonferroni/FDR для таблиц гипотез — с пониманием зависимости тестов;
- untouched holdout.

## 10. Минимальная выборка

Фиксированного числа нет: зависит от variance и effect size. До эксперимента
делать power analysis или sequential Bayesian analysis.

Практический минимум для live решения:

- не менее 100 независимых событий в совокупности;
- не менее 30 на ключевой subtype — всё ещё мало, вывод tentative;
- результат не должен зависеть от 1–3 лучших сделок;
- несколько рыночных режимов;
- минимум 2–3 месяца shadow/live data с реальным latency.

## 11. TP/SL исследование

Текущие уровни нельзя считать оптимальными.

Сначала собрать MFE/MAE trajectories каждой сделки. Затем сравнить заранее
ограниченный набор exit families:

- fixed horizon;
- fixed TP/SL;
- time stop;
- trailing;
- volatility-scaled;
- liquidity/spread exit;
- two-stage partial exit.

Оценивать не только return, но:

- CVaR;
- drawdown;
- duration;
- probability of no fill;
- sensitivity к ±20% параметров.

Хорошее правило устойчиво в области параметров, а не только в одной точке.

## 12. Paper / shadow / canary

### Shadow

- сигнал классифицируется;
- решение и intended order пишутся;
- реальный ордер не отправляется;
- сохраняется стакан и моделируемый fill.

### Paper

- testnet либо внутренний simulator;
- проверяем state machine и reconciliation;
- testnet liquidity не доказывает live slippage.

### Canary live

- минимальный notional;
- один event subtype;
- hard daily loss;
- ручной kill-switch;
- автоматический rollback на reject/fill mismatch;
- сравнение live vs shadow.

### Scale

Увеличение размера ступенями, только если slippage curve и P&L соответствуют
ожиданиям.

## 13. Отчёт эксперимента

Каждый report содержит:

- data version / commit SHA;
- hypothesis id;
- event count;
- exclusions и причины;
- number of trials;
- cost assumptions;
- train/validation/test periods;
- primary metric + CI;
- robustness checks;
- failure cases;
- go / no-go / collect-more-data;
- следующий эксперимент.

## 14. Критерии отказа от стратегии

Стратегию останавливаем, если:

- net expectancy отрицательный на untouched test;
- положительный результат исчезает при +25% costs;
- прибыль создают 1–2 выброса;
- live slippage систематически хуже simulation;
- edge падает быстрее, чем мы можем улучшить source latency;
- drawdown превышает заранее заданный предел;
- статистическая неопределённость слишком велика для риска реальных денег.
