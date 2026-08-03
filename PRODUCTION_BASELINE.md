# Production runtime baseline

Эта ветка восстановлена из фактически запущенного Docker image на основном
сервере. Она является исходной точкой для дальнейшей разработки, потому что
GitHub `main` значительно отставал от production runtime.

## Provenance

- Host: production Docker host (адрес намеренно не сохраняется здесь).
- Source: `/Parsers` внутри контейнера `parsers-listing-1`.
- Docker image digest:
  `sha256:db4730c6d5923a7c5966f0ccfe9d0e11e543311758a2c8892a517dab88977067`.
- Image created: `2026-07-31T23:01:43Z`.
- Snapshot date: `2026-08-03`.
- Both `listing` and `delist` services used the same image digest.

## Confirmed runtime behavior

- Listing events open **SHORT** via `market_open_short()` (`[INVERT]`).
- Delisting events open **LONG** via `market_open_long()` (`[INVERT]`).
- This behavior differs from GitHub `main`, which represented the older
  listing-long / delisting-short strategy.

## Sanitization

The snapshot deliberately excludes:

- root and nested `.env` files;
- Telethon `*.session*` files;
- persisted `state/`;
- Python caches;
- embedded relay secret;
- hardcoded proxy credentials found in two diagnostic scripts.

The diagnostic scripts now read proxy credentials only from environment
variables. `.dockerignore` excludes all nested `.env`, state, sessions and
relay test scripts.

## Known limitations before deployment

This baseline MUST NOT be deployed merely because it compiles:

1. It was recovered from an image without embedded `.git`, so the original
   commit history is unavailable.
2. Production telemetry before/after the 2026-07 strategy inversion is mixed.
3. Existing exit simulation uses reference prices/klines, not full fill-level
   execution truth or L2 order-book replay.
4. Safety changes from PRs #44–#48 must be re-audited and ported selectively;
   the old PR branches cannot be merged blindly because they are based on an
   older architecture.
5. Real exchange orders, TP/SL behavior and account risk have not been tested
   from this branch.

## First normalization fix

New price-path and strategy-result records include:

- `event_type`: `listing` or `delisting`;
- `side`: actual position side (`long` or `short`);
- `strategy_version`: currently `contrarian-v1`.

Win rates are cohort-filtered by all three fields. Legacy records remain on
 disk for audit but no longer pollute statistics for the new regime.
