//! `contractskit` — US federal government contract awards per company for Rust.
//!
//! Fetches year-partitioned parquet files on demand from GitHub raw, caches
//! them locally with ETag revalidation, and falls back to stale cache on
//! network errors. No API keys. Offline after the first successful fetch of
//! each year file.
//!
//! Data comes from USASpending.gov, the public record of US federal spending.
//! Each row is one prime contract award; `action_date` is `i32` `YYYYMMDD` and
//! `amount_usd` is whole US dollars. Recipients are companies; where a recipient
//! name matches an SEC-listed company exactly, the [`Award::ticker`] field
//! carries that stock symbol. Most federal recipients are private or non-public,
//! so the `ticker` field is empty on the majority of rows by design.
//!
//! # Quick start — free functions
//!
//! ```no_run
//! use contractskit::contracts_for;
//!
//! #[tokio::main]
//! async fn main() -> contractskit::Result<()> {
//!     for a in contracts_for("LMT").await?.iter().take(5) {
//!         println!("{} {} ${}", a.action_date, a.recipient_name, a.amount_usd);
//!     }
//!     Ok(())
//! }
//! ```
//!
//! For connection-pool reuse across many lookups, create a [`Contractskit`]
//! client once and call its methods instead of the free functions.
//!
//! # Environment overrides
//!
//! | Variable | Effect |
//! |---|---|
//! | `CONTRACTSKIT_BASE_URL` | Replace the GitHub raw origin URL |
//! | `CONTRACTSKIT_CACHE_DIR` | Override `~/.cache/contractskit/` |
//! | `CONTRACTSKIT_MIRROR_URL` | Override the jsDelivr CDN mirror |
#![forbid(unsafe_code)]

mod error;
pub use error::{Error, Result};

mod record;
pub use record::Award;

pub mod parquet_io;
pub use parquet_io::{read_awards, write_awards};

mod fetcher;

mod client;
pub use client::{by_agency, contracts_for, latest, Contractskit};
