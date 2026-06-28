//! Stateful `Contractskit` client — async contract-award endpoints with
//! blocking wrappers.
//!
//! Fetches year-partitioned parquet shards from GitHub raw (or a configurable
//! origin) with ETag-aware caching, SHA-256 manifest verification, and CDN
//! mirror fallback. Falls back to stale cache on transient network failures.
//!
//! # Quick start — free functions
//!
//! ```no_run
//! use contractskit::contracts_for;
//!
//! #[tokio::main]
//! async fn main() -> contractskit::Result<()> {
//!     for a in contracts_for("LMT").await?.iter().take(5) {
//!         println!("{} {} {} ${}", a.action_date, a.award_id, a.recipient_name, a.amount_usd);
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # Client pattern (reuse across calls)
//!
//! ```no_run
//! use contractskit::Contractskit;
//!
//! #[tokio::main]
//! async fn main() -> contractskit::Result<()> {
//!     let client = Contractskit::new();
//!     let dod = client.by_agency("Department of Defense").await?;
//!     println!("{} DoD awards", dod.len());
//!     Ok(())
//! }
//! ```

use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::fetcher::{default_cache_dir, resolved_base_url, CachedFetcher};
use crate::parquet_io::read_awards;
use crate::record::Award;

/// Stateful contractskit client.
///
/// Wraps an ETag-aware cached fetcher and exposes flat async query methods.
/// Create once and reuse; the internal reqwest client is kept alive for
/// connection pooling.
///
/// ```no_run
/// use contractskit::Contractskit;
/// use std::path::PathBuf;
///
/// let client = Contractskit::new()
///     .with_base_url("https://my-mirror.example.com/contractskit")
///     .with_cache_dir(PathBuf::from("/tmp/contractskit-test"));
/// ```
#[derive(Clone)]
pub struct Contractskit {
    fetcher: CachedFetcher,
}

impl Contractskit {
    /// Create a client with the default GitHub raw backend and XDG cache.
    ///
    /// Reads `CONTRACTSKIT_BASE_URL` and `CONTRACTSKIT_CACHE_DIR` from the
    /// environment if set. **This function never fails.** Errors are deferred
    /// to the first fetch.
    pub fn new() -> Self {
        let http = reqwest::Client::builder()
            .user_agent("contractskit/0.1 (+https://github.com/userFRM/contractskit)")
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            fetcher: CachedFetcher::new(http, resolved_base_url(), default_cache_dir()),
        }
    }

    /// Override the origin URL.
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.fetcher.set_base_url(url.into());
        self
    }

    /// Override the on-disk cache directory.
    pub fn with_cache_dir(mut self, dir: PathBuf) -> Self {
        self.fetcher.set_cache_dir(dir);
        self
    }

    /// Override the CDN mirror URL. `None` disables mirror fallback.
    pub fn with_mirror_url(mut self, url: Option<String>) -> Self {
        self.fetcher.set_mirror_url(url);
        self
    }

    // ── Async query endpoints ───────────────────────────────────────────────

    /// Awards for a company, matched by exact SEC `ticker` (case-insensitive)
    /// if `query` resolves to any ticketed award, otherwise by case-insensitive
    /// substring of the recipient name. Most recent action date first.
    pub async fn contracts_for(&self, query: &str) -> Result<Vec<Award>> {
        let rows = self.load_all_rows().await?;
        let by_ticker: Vec<Award> = rows
            .iter()
            .filter(|r| !r.ticker.is_empty() && r.ticker.eq_ignore_ascii_case(query))
            .cloned()
            .collect();
        if !by_ticker.is_empty() {
            return Ok(sort_desc(by_ticker));
        }
        let needle = query.to_lowercase();
        Ok(sort_desc(
            rows.into_iter()
                .filter(|r| r.recipient_name.to_lowercase().contains(&needle))
                .collect(),
        ))
    }

    /// All awards from an awarding agency, matched by case-insensitive substring
    /// of the agency name. Most recent action date first.
    pub async fn by_agency(&self, name: &str) -> Result<Vec<Award>> {
        let needle = name.to_lowercase();
        let rows = self.load_all_rows().await?;
        Ok(sort_desc(
            rows.into_iter()
                .filter(|r| r.awarding_agency.to_lowercase().contains(&needle))
                .collect(),
        ))
    }

    /// The `n` most recent awards across all agencies, by action date.
    pub async fn latest(&self, n: usize) -> Result<Vec<Award>> {
        let mut rows = self.load_all_rows().await?;
        rows.sort_by_key(|r| std::cmp::Reverse(r.action_date));
        rows.truncate(n);
        Ok(rows)
    }

    /// The largest awards by dollar amount with `action_date` in the inclusive
    /// `[start, end]` `YYYYMMDD` window, biggest first, capped at `n`.
    pub async fn largest(&self, start: i32, end: i32, n: usize) -> Result<Vec<Award>> {
        let mut rows: Vec<Award> = self
            .load_all_rows()
            .await?
            .into_iter()
            .filter(|r| r.action_date >= start && r.action_date <= end)
            .collect();
        rows.sort_by_key(|r| std::cmp::Reverse(r.amount_usd));
        rows.truncate(n);
        Ok(rows)
    }

    // ── Blocking wrappers ───────────────────────────────────────────────────

    /// Blocking variant of [`contracts_for`](Self::contracts_for).
    pub fn contracts_for_blocking(&self, query: &str) -> Result<Vec<Award>> {
        let c = self.clone();
        let q = query.to_owned();
        block(async move { c.contracts_for(&q).await })
    }

    /// Blocking variant of [`by_agency`](Self::by_agency).
    pub fn by_agency_blocking(&self, name: &str) -> Result<Vec<Award>> {
        let c = self.clone();
        let n = name.to_owned();
        block(async move { c.by_agency(&n).await })
    }

    /// Blocking variant of [`latest`](Self::latest).
    pub fn latest_blocking(&self, n: usize) -> Result<Vec<Award>> {
        let c = self.clone();
        block(async move { c.latest(n).await })
    }

    /// Blocking variant of [`largest`](Self::largest).
    pub fn largest_blocking(&self, start: i32, end: i32, n: usize) -> Result<Vec<Award>> {
        let c = self.clone();
        block(async move { c.largest(start, end, n).await })
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    /// Fetch every `contracts-YYYY.parquet` shard listed in the manifest and
    /// flat-concatenate the rows.
    pub(crate) async fn load_all_rows(&self) -> Result<Vec<Award>> {
        let keys = self.discover_shards().await?;
        let mut all = Vec::new();
        for key in keys {
            let bytes = self.fetcher.fetch(&key).await?;
            all.extend(read_awards(&bytes)?);
        }
        Ok(all)
    }

    /// Fetch `manifest.json` and return sorted shard keys (without `.parquet`).
    async fn discover_shards(&self) -> Result<Vec<String>> {
        let url = format!("{}/manifest.json", self.fetcher.base_url);
        let resp = self
            .fetcher
            .http
            .get(&url)
            .send()
            .await
            .map_err(Error::Http)?;
        if !resp.status().is_success() {
            return Err(Error::Other(format!(
                "manifest.json: HTTP {} {}",
                resp.status().as_u16(),
                resp.status().canonical_reason().unwrap_or("")
            )));
        }
        let manifest: serde_json::Value = resp.json().await.map_err(Error::Http)?;
        let obj = manifest
            .as_object()
            .ok_or_else(|| Error::Other("manifest.json is not a JSON object".into()))?;
        let mut keys: Vec<String> = obj
            .keys()
            .filter(|k| is_contracts_shard(k))
            .map(|k| k.trim_end_matches(".parquet").to_string())
            .collect();
        keys.sort();
        Ok(keys)
    }
}

impl Default for Contractskit {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sort_desc(mut rows: Vec<Award>) -> Vec<Award> {
    rows.sort_by_key(|r| std::cmp::Reverse(r.action_date));
    rows
}

/// Return `true` for filenames matching `contracts-YYYY.parquet`.
fn is_contracts_shard(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("contracts-") else {
        return false;
    };
    let Some(year) = rest.strip_suffix(".parquet") else {
        return false;
    };
    !year.is_empty() && year.bytes().all(|b| b.is_ascii_digit())
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

/// Awards for a company (ticker or recipient-name substring), one-shot client.
pub async fn contracts_for(query: &str) -> Result<Vec<Award>> {
    Contractskit::new().contracts_for(query).await
}

/// All awards from an awarding agency (name substring), one-shot client.
pub async fn by_agency(name: &str) -> Result<Vec<Award>> {
    Contractskit::new().by_agency(name).await
}

/// The `n` most recent awards across all agencies, one-shot client.
pub async fn latest(n: usize) -> Result<Vec<Award>> {
    Contractskit::new().latest(n).await
}

// ---------------------------------------------------------------------------
// Blocking helper
// ---------------------------------------------------------------------------

/// Drive a future to completion from any context (sync or async).
///
/// - Inside a tokio **multi-thread** runtime: `block_in_place` + `block_on`.
/// - Inside a **current-thread** runtime or no runtime: the future is driven on
///   a dedicated OS thread with its own runtime so the caller is not re-entered.
pub(crate) fn block<F, T>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(fut))
        }
        _ => std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(Error::Io)
                .and_then(|rt| rt.block_on(fut))
        })
        .join()
        .expect("blocking thread panicked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_matches_year_files_only() {
        assert!(is_contracts_shard("contracts-2024.parquet"));
        assert!(!is_contracts_shard("manifest.json"));
        assert!(!is_contracts_shard("contracts-.parquet"));
        assert!(!is_contracts_shard("insider-2024.parquet"));
    }
}
