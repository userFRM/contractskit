//! `contractskit-cli` — build, refresh, and query the bundled federal
//! contract-award parquet data.
//!
//! # Commands
//!
//! ```text
//! contractskit-cli backfill [--from 2019] [--to 2024] [--min-amount 100000]
//! contractskit-cli nightly-append
//! contractskit-cli manifest
//! contractskit-cli query --ticker LMT
//! contractskit-cli query --recipient "Lockheed"
//! contractskit-cli query --agency "Defense"
//! ```
//!
//! `backfill` and `nightly-append` page the public USASpending.gov award-search
//! API (prime contracts, award type codes A/B/C/D), enrich each recipient with a
//! best-effort SEC ticker, and write `data/year=YYYY/contracts-YYYY.parquet`.
//! `nightly-append` fetches awards on or after the latest `action_date` already
//! in the current-year file and merges them, deduped by `award_id`.

mod tickers;
mod usaspending;

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use contractskit::{read_awards, write_awards, Award};
use sha2::{Digest, Sha256};

use tickers::TickerMap;

/// Default first backfill year. USASpending contract data is reliable from the
/// 2008+ FPDS era; the kit seeds the most recent several fiscal years by default.
const DEFAULT_FROM_YEAR: i32 = 2019;

/// Default award-amount floor in whole dollars. Federal contract data is very
/// high volume; a floor keeps the bundled parquet to a signal-bearing size. Set
/// `--min-amount 0` to disable.
const DEFAULT_MIN_AMOUNT: i64 = 100_000;

fn sec_user_agent() -> String {
    // SEC requires a bare "<name> <email>" User-Agent (no URL/parens).
    std::env::var("CONTRACTSKIT_SEC_USER_AGENT")
        .unwrap_or_else(|_| "contractskit contact@example.com".to_string())
}

#[derive(Parser)]
#[command(name = "contractskit-cli", about = "US federal contract awards")]
struct Cli {
    /// Data directory (default: `<cwd>/data`).
    #[arg(long, env = "CONTRACTSKIT_DATA_DIR", global = true)]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download and rebuild per-year parquet from USASpending.gov.
    Backfill {
        /// First fiscal/calendar year to include (default 2019).
        #[arg(long)]
        from: Option<i32>,
        /// Last year to include (default: current year).
        #[arg(long)]
        to: Option<i32>,
        /// Minimum award amount in whole dollars (default 100000; 0 = no floor).
        #[arg(long)]
        min_amount: Option<i64>,
    },
    /// Refresh the current year with awards since the latest stored action date.
    NightlyAppend {
        /// Minimum award amount in whole dollars (default 100000; 0 = no floor).
        #[arg(long)]
        min_amount: Option<i64>,
    },
    /// Generate `data/manifest.json` with a SHA-256 per parquet file.
    Manifest,
    /// Read bundled parquet and print matching awards.
    Query {
        /// SEC ticker (case-insensitive).
        #[arg(long)]
        ticker: Option<String>,
        /// Recipient-name substring (case-insensitive).
        #[arg(long)]
        recipient: Option<String>,
        /// Awarding-agency substring (case-insensitive).
        #[arg(long)]
        agency: Option<String>,
        /// Maximum rows to print.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let data_dir = cli.data_dir.unwrap_or_else(|| PathBuf::from("data"));

    match cli.cmd {
        Command::Backfill {
            from,
            to,
            min_amount,
        } => {
            let from = from.unwrap_or(DEFAULT_FROM_YEAR);
            let to = to.unwrap_or_else(current_year);
            let min = min_amount.unwrap_or(DEFAULT_MIN_AMOUNT);
            backfill(&data_dir, from, to, min).await
        }
        Command::NightlyAppend { min_amount } => {
            nightly_append(&data_dir, min_amount.unwrap_or(DEFAULT_MIN_AMOUNT)).await
        }
        Command::Manifest => write_manifest(&data_dir),
        Command::Query {
            ticker,
            recipient,
            agency,
            limit,
        } => query(&data_dir, ticker, recipient, agency, limit),
    }
}

// ---------------------------------------------------------------------------
// backfill
// ---------------------------------------------------------------------------

async fn backfill(data_dir: &Path, from: i32, to: i32, min_amount: i64) -> Result<()> {
    let client = http_client()?;
    let ticker_map = load_ticker_map(&client).await;

    let mut total_rows = 0usize;
    let mut total_matched = 0usize;
    let mut by_year: BTreeMap<i32, usize> = BTreeMap::new();

    for year in from..=to {
        let start = format!("{year}-01-01");
        let end = format!("{year}-12-31");
        let mut rows = match usaspending::fetch_range(&client, &start, &end, min_amount).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("{year}: fetch failed ({e}), skipping");
                continue;
            }
        };
        let matched = enrich(&mut rows, &ticker_map);
        total_rows += rows.len();
        total_matched += matched;
        by_year.insert(year, rows.len());
        eprintln!(
            "{year}: {} awards (>= ${min_amount}), {matched} ticker-matched",
            rows.len()
        );
        write_year(data_dir, year, &rows)?;
    }

    report(total_rows, total_matched, min_amount, &by_year);
    write_manifest(data_dir)
}

// ---------------------------------------------------------------------------
// nightly-append
// ---------------------------------------------------------------------------

async fn nightly_append(data_dir: &Path, min_amount: i64) -> Result<()> {
    let year = current_year();
    let client = http_client()?;
    let ticker_map = load_ticker_map(&client).await;

    let path = year_path(data_dir, year);
    let mut existing: Vec<Award> = if path.exists() {
        read_awards(&std::fs::read(&path)?)?
    } else {
        Vec::new()
    };

    // Fetch from the latest stored action date (inclusive) through today so
    // same-day corrections are re-pulled; dedup by award_id resolves overlap.
    let since = existing
        .iter()
        .map(|a| a.action_date)
        .max()
        .map(|d| format!("{:04}-{:02}-{:02}", d / 10000, (d / 100) % 100, d % 100))
        .unwrap_or_else(|| format!("{year}-01-01"));
    let end = format!("{year}-12-31");

    let mut fresh = usaspending::fetch_range(&client, &since, &end, min_amount).await?;
    let matched = enrich(&mut fresh, &ticker_map);
    eprintln!(
        "nightly {year}: fetched {} awards since {since}, {matched} ticker-matched",
        fresh.len()
    );

    let before = existing.len();
    existing.append(&mut fresh);
    dedup_by_award_id(&mut existing);
    eprintln!(
        "nightly {year}: {before} -> {} rows after dedup",
        existing.len()
    );

    write_year(data_dir, year, &existing)?;
    write_manifest(data_dir)
}

/// Keep the last-seen row per `award_id` (fresh rows are appended after existing,
/// so a re-pulled award wins), then sort most-recent action date first.
fn dedup_by_award_id(rows: &mut Vec<Award>) {
    let mut idx: HashMap<String, usize> = HashMap::new();
    for (i, r) in rows.iter().enumerate() {
        idx.insert(r.award_id.clone(), i);
    }
    let keep: std::collections::HashSet<usize> = idx.into_values().collect();
    let mut out: Vec<Award> = rows
        .drain(..)
        .enumerate()
        .filter(|(i, _)| keep.contains(i))
        .map(|(_, r)| r)
        .collect();
    out.sort_by_key(|r| std::cmp::Reverse(r.action_date));
    *rows = out;
}

// ---------------------------------------------------------------------------
// enrichment
// ---------------------------------------------------------------------------

/// Assign a best-effort SEC ticker to each row in place. Returns the match count.
fn enrich(rows: &mut [Award], map: &Option<TickerMap>) -> usize {
    let Some(map) = map else {
        return 0;
    };
    let mut matched = 0;
    for r in rows.iter_mut() {
        if let Some(t) = map.lookup(&r.recipient_name) {
            r.ticker = t.to_string();
            matched += 1;
        }
    }
    matched
}

async fn load_ticker_map(client: &reqwest::Client) -> Option<TickerMap> {
    match TickerMap::fetch(client).await {
        Ok(m) => {
            eprintln!("SEC ticker map: {} companies", m.len());
            Some(m)
        }
        Err(e) => {
            eprintln!("SEC ticker map fetch failed ({e}); awards will carry empty ticker");
            None
        }
    }
}

fn report(total_rows: usize, matched: usize, min_amount: i64, by_year: &BTreeMap<i32, usize>) {
    eprintln!("---");
    eprintln!("total awards: {total_rows}");
    let rate = if total_rows > 0 {
        100.0 * matched as f64 / total_rows as f64
    } else {
        0.0
    };
    eprintln!(
        "ticker matches: {matched} ({rate:.1}% of rows; most federal recipients are private)"
    );
    eprintln!("amount floor: ${min_amount}");
    for (y, n) in by_year {
        eprintln!("  {y}: {n}");
    }
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

fn http_client() -> Result<reqwest::Client> {
    // USASpending needs no key; SEC needs the bare-form User-Agent. One client
    // carrying the SEC-form UA satisfies both hosts.
    reqwest::Client::builder()
        .user_agent(sec_user_agent())
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .context("build http client")
}

// ---------------------------------------------------------------------------
// write per-year parquet
// ---------------------------------------------------------------------------

fn year_path(data_dir: &Path, year: i32) -> PathBuf {
    data_dir
        .join(format!("year={year}"))
        .join(format!("contracts-{year}.parquet"))
}

fn write_year(data_dir: &Path, year: i32, rows: &[Award]) -> Result<()> {
    if rows.is_empty() {
        eprintln!("{year}: no rows, leaving year file unchanged");
        return Ok(());
    }
    let path = year_path(data_dir, year);
    std::fs::create_dir_all(path.parent().unwrap())
        .with_context(|| format!("create {}", path.display()))?;
    write_awards(&path, rows).with_context(|| format!("write {}", path.display()))?;
    eprintln!("wrote {} ({} rows)", path.display(), rows.len());
    Ok(())
}

// ---------------------------------------------------------------------------
// manifest
// ---------------------------------------------------------------------------

fn write_manifest(data_dir: &Path) -> Result<()> {
    let mut entries: BTreeMap<String, String> = BTreeMap::new();
    for path in find_parquet(data_dir)? {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("parquet filename")?
            .to_string();
        let bytes = std::fs::read(&path)?;
        let mut h = Sha256::new();
        h.update(&bytes);
        let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
        entries.insert(name, format!("sha256:{hex}"));
    }
    let json = serde_json::to_string_pretty(&entries)?;
    let path = data_dir.join("manifest.json");
    std::fs::create_dir_all(data_dir)?;
    std::fs::write(&path, json)?;
    eprintln!("wrote {} ({} files)", path.display(), entries.len());
    Ok(())
}

fn find_parquet(data_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !data_dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(data_dir)? {
        let path = entry?.path();
        if path.is_dir() {
            for sub in std::fs::read_dir(&path)? {
                let p = sub?.path();
                if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    out.push(p);
                }
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

// ---------------------------------------------------------------------------
// query (reads local parquet)
// ---------------------------------------------------------------------------

fn query(
    data_dir: &Path,
    ticker: Option<String>,
    recipient: Option<String>,
    agency: Option<String>,
    limit: usize,
) -> Result<()> {
    let mut rows = Vec::new();
    for path in find_parquet(data_dir)? {
        rows.extend(read_awards(&std::fs::read(&path)?)?);
    }
    if rows.is_empty() {
        bail!(
            "no parquet found under {}; run backfill first",
            data_dir.display()
        );
    }

    if let Some(t) = &ticker {
        rows.retain(|r| r.ticker.eq_ignore_ascii_case(t));
    }
    if let Some(name) = &recipient {
        let needle = name.to_lowercase();
        rows.retain(|r| r.recipient_name.to_lowercase().contains(&needle));
    }
    if let Some(a) = &agency {
        let needle = a.to_lowercase();
        rows.retain(|r| r.awarding_agency.to_lowercase().contains(&needle));
    }
    rows.sort_by_key(|r| std::cmp::Reverse(r.amount_usd));

    println!(
        "{:<10} {:<6} {:<28} {:<26} {:>16}",
        "date", "tick", "recipient", "agency", "amount_usd"
    );
    for r in rows.iter().take(limit) {
        println!(
            "{:<10} {:<6} {:<28} {:<26} {:>16}",
            r.action_date,
            truncate(&r.ticker, 6),
            truncate(&r.recipient_name, 28),
            truncate(&r.awarding_agency, 26),
            r.amount_usd,
        );
    }
    Ok(())
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        s.chars().take(n.saturating_sub(1)).collect::<String>() + "…"
    }
}

// ---------------------------------------------------------------------------
// calendar helper (system clock; current year only)
// ---------------------------------------------------------------------------

fn current_year() -> i32 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let z = secs / 86_400 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn awd(id: &str, date: i32) -> Award {
        Award {
            action_date: date,
            award_id: id.into(),
            recipient_name: "ACME".into(),
            recipient_uei: String::new(),
            ticker: String::new(),
            parent_recipient: String::new(),
            awarding_agency: "DoD".into(),
            amount_usd: 1,
            award_type: "DEFINITIVE CONTRACT".into(),
            naics_code: String::new(),
            description: String::new(),
        }
    }

    #[test]
    fn dedup_keeps_latest_per_award_id() {
        let mut rows = vec![awd("A", 20240101), awd("B", 20240105), awd("A", 20240110)];
        dedup_by_award_id(&mut rows);
        assert_eq!(rows.len(), 2, "two distinct award ids");
        // The re-pulled "A" (appended later) wins and carries the newer date.
        let a = rows.iter().find(|r| r.award_id == "A").unwrap();
        assert_eq!(a.action_date, 20240110);
        // Sorted most-recent first.
        assert_eq!(rows[0].action_date, 20240110);
    }
}
