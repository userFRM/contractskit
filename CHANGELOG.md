<!-- Canonical CHANGELOG header for every *kit. The body keeps each kit's real
release history; only this top block is standardized. -->
# Changelog

All notable changes to contractskit are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0]

Initial release.

- Async `Contractskit` client plus blocking siblings and one-shot free functions.
- Query surface: `contracts_for` (ticker or recipient-name substring), `by_agency`, `latest`, `largest`.
- Each award carries a best-effort SEC `ticker` matched from the public SEC company-tickers map by exact normalized recipient name. Most federal recipients are private or non-public, so `ticker` is empty on the majority of rows by design; the backfill reports the match rate.
- Bundled per-year parquet (`data/year=YYYY/contracts-YYYY.parquet`) served from GitHub raw with on-demand fetch, ETag revalidation, SHA-256 manifest verification, and a CDN mirror plus stale-cache fallback.
- `contractskit-cli` with `backfill`, `nightly-append`, `manifest`, and `query`.
- Source is USASpending.gov prime contract awards (award type codes A/B/C/D). The backfill applies a configurable award-amount floor to keep the bundled data signal-bearing; `--min-amount 0` disables it.
- Initial data seeds years 2019 through 2023 at a $1,000,000 floor (about 202,000 awards). The `backfill` workflow extends the range and floor from the CI runners, where the public API does not throttle sustained paging.
