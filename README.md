# contractskit

US federal government contract awards per company for Rust. Served from bundled parquet with on-demand fetch and a local cache. No API keys. Offline after the first query.

## Install

```toml
[dependencies]
contractskit = "0.1.0"
```

To track unreleased changes, depend on the repository directly:

```toml
contractskit = { git = "https://github.com/userFRM/contractskit" }
```

## Quick start

```rust,no_run
#[tokio::main]
async fn main() -> contractskit::Result<()> {
    for a in contractskit::contracts_for("LMT").await?.iter().take(5) {
        println!(
            "{} {} ({}) {} ${}",
            a.action_date, a.recipient_name, a.ticker, a.awarding_agency, a.amount_usd
        );
    }
    Ok(())
}
```

## Client pattern

```rust,no_run
use contractskit::Contractskit;

#[tokio::main]
async fn main() -> contractskit::Result<()> {
    let client = Contractskit::new();
    let dod = client.by_agency("Department of Defense").await?;
    println!("{} DoD awards", dod.len());
    let top = client.largest(20240101, 20241231, 10).await?;
    println!("largest 2024 award: ${}", top[0].amount_usd);
    Ok(())
}
```

`contracts_for` matches an exact SEC ticker first, then falls back to a recipient-name substring. `by_agency` matches an awarding-agency substring. `latest(n)` returns the most recent awards by action date. `largest(start, end, n)` returns the biggest awards by dollar amount in an inclusive `YYYYMMDD` window. Every async method has a `_blocking` sibling.

## Coverage

Source is [USASpending.gov](https://www.usaspending.gov), the public record of US federal spending, via its award-search API. Rows are prime contract awards (award type codes A/B/C/D: definitive contracts, purchase orders, delivery orders, and BPA calls). Grants, loans, and other non-contract assistance are excluded.

Each award's recipient is best-effort matched to an SEC stock ticker from the public [SEC company-tickers map](https://www.sec.gov/files/company_tickers.json). The match is conservative: a ticker is assigned only when the recipient's normalized name (uppercased, punctuation and common corporate suffixes stripped) equals an SEC company's normalized name exactly. The vast majority of federal recipients are private companies, subsidiaries, universities, and non-public entities, so most rows carry an empty `ticker`. The backfill reports the match rate; symbols are never guessed.

The bundled backfill applies an award-amount floor so the committed parquet stays a signal-bearing size rather than the full multi-million-row firehose. The shipped data uses a $1,000,000 floor and seeds fiscal/calendar years 2019 through 2023 (about 202,000 awards, 11 MB). The floor is configurable (`--min-amount`, `0` to disable, `100000` for the wider tier) and the seeded range is extended by the `backfill` workflow. The public API throttles sustained paging from a single IP, so very large years are filled from the GitHub Actions runners rather than in one local pull.

`action_date` is the award's most recent action date. The award-search endpoint exposes no per-award action-date field, so this is the award's last-modified date, which tracks the latest action and matches the `action_date` window the data is collected over. `parent_recipient` is reserved in the schema; the award-search endpoint does not return it, so it is empty in the current data.

## CLI

```bash
contractskit-cli backfill --from 2019 --to 2024 --min-amount 100000
contractskit-cli nightly-append
contractskit-cli manifest
contractskit-cli query --ticker LMT
contractskit-cli query --recipient "Lockheed"
contractskit-cli query --agency "Defense"
```

## Data

One row per contract award, partitioned by year as `data/year=YYYY/contracts-YYYY.parquet` (zstd), with a SHA-256 per file in `data/manifest.json`. Columns:

```text
action_date Int32(YYYYMMDD), award_id Utf8, recipient_name Utf8,
recipient_uei Utf8, ticker Utf8, parent_recipient Utf8,
awarding_agency Utf8, amount_usd Int64, award_type Utf8,
naics_code Utf8, description Utf8
```

`nightly-append` fetches awards on or after the latest `action_date` already in the current-year file and merges them, deduped by `award_id`.

## Attribution

Contract-award data is from [USASpending.gov](https://www.usaspending.gov) and is in the US public domain. Company tickers are from the US Securities and Exchange Commission's public [company-tickers map](https://www.sec.gov/files/company_tickers.json).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
