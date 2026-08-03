# Источники

Дата проверки ссылок: 2026-07-30 / 2026-08-03 по текущему контексту.
Перед реализацией API всегда повторно сверять официальную документацию:
параметры, rate limits и semantics меняются.

## 1. Официальная документация бирж — источник истины

### Bybit

1. **WebSocket Trade Guideline**  
   https://bybit-exchange.github.io/docs/v5/websocket/trade/guideline  
   Auth, `order.create`, retCodes, rate-limit headers, reconnect при service
   restart (`10016`, `10019`).

2. **WebSocket Connect**  
   https://bybit-exchange.github.io/docs/v5/ws/connect  
   Endpoints, auth, heartbeat. Официальная рекомендация: ping каждые 20 сек.

3. **Place Order**  
   https://bybit-exchange.github.io/docs/v5/order/create-order  
   Market/limit, `slippageToleranceType`, `slippageTolerance`, TP/SL,
   instrument precision.

4. **Private Order Stream**  
   https://bybit-exchange.github.io/docs/v5/websocket/private/order  
   Lifecycle order; order ack не равен fill.

5. **Rate Limits**  
   https://bybit-exchange.github.io/docs/v5/rate-limit  
   REST/WS лимиты и retCode 10006.

6. **Disconnect Cancel All**  
   https://bybit-exchange.github.io/docs/v5/order/dcp  
   Полезно для resting orders, но не заменяет reconciliation.

### Binance

1. **USD-M Futures WebSocket Trade**  
   https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/trade

2. **Futures Public WebSocket Market Streams**  
   https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public

3. **Binance Public Historical Data**  
   https://github.com/binance/binance-public-data  
   Trades/aggTrades/klines; полезно для broad historical context. Для точного
   event execution нужен собственный L2 capture/replay.

Важно: `bapi/composite/.../cms/article` — internal web API, не стабильный
public developer contract. Нужен schema monitoring и альтернативные sources.

### Upbit

1. **Rate Limits**  
   https://global-docs.upbit.com/reference/rate-limits

2. **WebSocket Guide**  
   https://global-docs.upbit.com/reference/websocket-guide

3. **WebSocket Best Practices**  
   https://global-docs.upbit.com/docs/websocket-best-practice

4. **Orderbook**  
   https://global-docs.upbit.com/reference/list-orderbooks

Практический факт: `api.upbit.com/v1/market/all` работает с текущего host,
`api-manager.upbit.com` announcements и web notice дают 403 Cloudflare.
Это ограничение egress IP, а не доказательство удаления API.

### Gate.io

1. **Gate API v4 REST**  
   https://www.gate.com/docs/developers/apiv4/en  
   Signing, futures contracts/orders, rate limits, quanto multiplier.

2. **Gate API v4 WebSocket**  
   https://www.gate.com/docs/developers/apiv4/ws/en

### Hyperliquid

1. **API index**  
   https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api

2. **Rate limits and user limits**  
   https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits

3. **Exchange endpoint**  
   https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint

4. **Machine-readable docs index**  
   https://hyperliquid.gitbook.io/llms.txt

## 2. Книги — фундамент

### Tier 1: обязательно

1. **Larry Harris — Trading and Exchanges: Market Microstructure for
   Practitioners**  
   Orders, liquidity, transaction costs, informed trading, dealers, market
   structure. Главная книга, чтобы перестать думать о market order как о
   «покупке по текущей цене».

   Draft reference:  
   https://www.acsu.buffalo.edu/~keechung/MGF743/Readings/Trading-Exchanges-Market-Microstructure-Practitioners%20Draft%20Copy.pdf

2. **Barry Johnson — Algorithmic Trading & DMA**  
   Orders, TCA, execution algorithms/tactics, infrastructure. Практический
   мост между microstructure и production execution.

   Table of contents/reference:  
   https://openlibrary.org/books/OL27015972M/Algorithmic_trading_DMA

3. **Joel Hasbrouck — Empirical Market Microstructure**  
   Quote/trade data, price impact, empirical measurement. После Harris.

4. **Thierry Foucault, Marco Pagano, Ailsa Röell — Market Liquidity:
   Theory, Evidence, and Policy**  
   Более теоретическая база liquidity/adverse selection.

### Tier 2: research discipline

5. **Marcos López de Prado — Advances in Financial Machine Learning**  
   Использовать главы про labeling, purged CV, backtest overfitting. Не
   воспринимать как лицензию добавить ML до появления данных.

6. **David Aronson — Evidence-Based Technical Analysis**  
   Data mining bias, objective rules, статистическая дисциплина.

7. **Robert Pardo — The Evaluation and Optimization of Trading Strategies**  
   Walk-forward и robustness. Читать критически, сверять с современными
   методами multiple testing.

### Tier 3: systems

8. **Martin Kleppmann — Designing Data-Intensive Applications**  
   Event logs, durability, state/recovery. Полезно для event-store.

9. **Michael Nygard — Release It!**  
   Circuit breakers, stability patterns, production failures.

10. **Brendan Gregg — Systems Performance**  
    Только когда profiler показывает реальный systems bottleneck.

## 3. Academic papers — listing/delisting events

1. **Ante (2019), Market Reaction to Exchange Listings of Cryptocurrencies**  
   https://www.blockchainresearchlab.org/wp-content/uploads/2019/10/Exploring-Market-Reactions-to-Exchange-Listings-of-Cryptocurrencies-BRL-working-paper3.pdf  
   На дневных окнах нашёл significant CAAR для ряда venues, включая Binance,
   Bithumb и OKEx. Не доказывает наш millisecond entry edge.

2. **Hashemi Joo, Nishikawa, Dandapani (2020), Announcement Effects in the
   Cryptocurrency Market**  
   DOI: https://doi.org/10.1080/00036846.2020.1745747  
   Общие announcement effects и pre-event information leakage.

3. **Li et al. (AFT 2025), Cryptocurrency Exchange Listings**  
   https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.AFT.2025.11  
   Listings Binance/Coinbase, short-term returns, selection incentives.

4. **How Event Studies Can Be Applied to Crypto Markets (Brattle)**  
   https://www.brattle.com/wp-content/uploads/2023/07/How-Event-Studies-Can-Be-Applied-to-Crypto-Markets.pdf  
   Методический обзор, выбор benchmark/data/republication effects.

5. **Competition in the Cryptocurrency Exchange Market**  
   https://saet.uiowa.edu/wp-content/uploads/sites/18/gravity_forms/11-4d2b366dcba748062160497d6fc362c1/2023/06/cryptoexchanges.pdf  
   Effects listing на volume/dispersion/returns.

Ограничение этих papers: большинство работает на дневных/часовых окнах и
не моделирует наш arrival, L2 depth и taker execution. Они обосновывают, что
события могут влиять на рынок, но не что наш bot прибыльный.

## 4. Academic papers — execution и overfitting

1. **Bailey et al. — The Probability of Backtest Overfitting**  
   https://carmamaths.org/jon/backtest2.pdf

2. **Bailey & López de Prado — The Deflated Sharpe Ratio**  
   https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf  
   DOI: 10.3905/jpm.2014.40.5.094

3. **Empirical Evidence and Order Placement**  
   https://arxiv.org/pdf/2307.04863  
   Queue/placement/market impact; предупреждает, что отсутствие собственного
   impact завышает backtest.

4. **Trading in the Sunshine or in the Shade: Market Impact and Adverse
   Selection on Hyperliquid**  
   https://arxiv.org/html/2606.15715v1  
   Современные evidence по CLOB/impact/adverse selection на Hyperliquid.

5. **Deep Learning for VWAP Execution in Crypto Markets**  
   https://arxiv.org/html/2502.13722v2  
   Полезно для формализации VWAP/slippage; deep learning пока не приоритет.

## 5. GitHub / open-source references

### Рекомендуемые

1. **hftbacktest**  
   https://github.com/nkaz001/hftbacktest  
   Tick replay, L2/L3, latency, queue models, Binance/Bybit examples.
   Использовать как reference/backtest component, не копировать вслепую.

2. **NautilusTrader**  
   https://github.com/nautechsystems/nautilus_trader  
   Event-driven backtest/sandbox/live parity, order state, multi-venue.

3. **binance-public-data**  
   https://github.com/binance/binance-public-data

4. **pfei-sa/binance-LOB**  
   https://github.com/pfei-sa/binance-LOB  
   Reconstruction historical LOB from snapshots + diff stream.

5. **awesome-quant**  
   https://github.com/ernie55ernie/awesome-quant  
   Индекс ресурсов; каждый источник проверять отдельно.

### Аналоги listing bots — только идеи

1. https://github.com/CyberPunkMetalHead/gateio-crypto-trading-bot-binance-announcements-new-coins
2. https://github.com/raytighe/new_coin_listings

Они подтверждают распространённость идеи, не прибыльность. Не принимать
GitHub stars или README claims за evidence.

## 6. Видео

Видео — вторичный формат. Использовать для intuition, проверять по книгам/
docs/papers.

1. **Cross-Validation in Finance and Backtesting**  
   https://www.youtube.com/watch?v=kyjmHHoMc80  
   Purged CV / CPCV обзор по López de Prado.

2. **Market Microstructure Analysis: HFT, Order Book**  
   https://www.youtube.com/watch?v=3FIfw6lkr9Y  
   Вводный материал; не источник для production semantics.

Лучше искать лекции авторов/университетов по темам:

- Larry Harris market microstructure;
- Marcos López de Prado backtest overfitting;
- Patrick Boyle market microstructure/execution;
- exchange official API talks.

## 7. Источники, которым доверять осторожно

- Medium/Substack с большим процентом pump без raw dataset;
- YouTube P&L screenshots;
- Telegram-каналы, продающие signal edge;
- backtest без spread/slippage/fees;
- GitHub bot без live audited fills;
- academic daily event study как доказательство millisecond strategy;
- exchange marketing pages вместо API docs.

## 8. Как добавлять новый источник

Для каждой записи фиксировать:

```yaml
title:
type: official_docs | book | paper | github | video | blog
url:
author:
year:
claim_relevant_to_project:
evidence_level: primary | academic | secondary | anecdotal
limitations:
verified_at:
actionable_project_change:
```

Источник считается усвоенным, когда из него появился test/metric/experiment
или аргументированное решение ничего не менять.
