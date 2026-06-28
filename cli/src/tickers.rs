//! Recipient-name to SEC-ticker enrichment from the public SEC company-tickers
//! map (`https://www.sec.gov/files/company_tickers.json`).
//!
//! Matching is conservative: a recipient is assigned a ticker only when its
//! normalized name (uppercased, punctuation stripped, common corporate suffixes
//! and a leading "THE" removed, whitespace collapsed) equals an SEC company's
//! normalized name exactly. No fuzzy/substring guessing. Most federal recipients
//! are private or non-public entities and stay unticketed; the caller reports
//! the match rate rather than inventing symbols.

use std::collections::HashMap;

use anyhow::{Context, Result};
use serde::Deserialize;

const SEC_TICKERS_URL: &str = "https://www.sec.gov/files/company_tickers.json";

/// Corporate suffix tokens stripped from the tail of a name before matching.
const SUFFIXES: &[&str] = &[
    "INC",
    "INCORPORATED",
    "CORP",
    "CORPORATION",
    "CO",
    "COMPANY",
    "LLC",
    "LP",
    "LLP",
    "PLC",
    "LTD",
    "LIMITED",
    "HOLDINGS",
    "HOLDING",
    "GROUP",
    "THE",
];

#[derive(Deserialize)]
struct SecEntry {
    ticker: String,
    title: String,
}

/// Normalized-name to ticker index built from the SEC company-tickers map.
#[derive(Default)]
pub struct TickerMap {
    by_norm: HashMap<String, String>,
    entries: usize,
}

impl TickerMap {
    /// Fetch the SEC company-tickers map and build the normalized index.
    pub async fn fetch(client: &reqwest::Client) -> Result<TickerMap> {
        let bytes = client
            .get(SEC_TICKERS_URL)
            .send()
            .await
            .context("fetch SEC company_tickers.json")?
            .error_for_status()
            .context("SEC company_tickers.json status")?
            .bytes()
            .await
            .context("read SEC company_tickers.json body")?;
        // The file is a JSON object keyed by row index: {"0": {...}, "1": {...}}.
        let raw: HashMap<String, SecEntry> =
            serde_json::from_slice(&bytes).context("parse SEC company_tickers.json")?;
        Ok(Self::from_entries(raw.into_values()))
    }

    fn from_entries(entries: impl Iterator<Item = SecEntry>) -> TickerMap {
        let mut by_norm = HashMap::new();
        let mut count = 0usize;
        for e in entries {
            count += 1;
            let norm = normalize(&e.title);
            if norm.is_empty() {
                continue;
            }
            // First writer wins; the SEC map is largely unique by normalized name
            // and a later collision is most likely a less-canonical duplicate.
            by_norm.entry(norm).or_insert(e.ticker.to_uppercase());
        }
        TickerMap {
            by_norm,
            entries: count,
        }
    }

    /// Number of SEC entries loaded.
    pub fn len(&self) -> usize {
        self.entries
    }

    /// The ticker for a recipient name, or `None` on no confident match.
    pub fn lookup(&self, recipient_name: &str) -> Option<&str> {
        let norm = normalize(recipient_name);
        if norm.is_empty() {
            return None;
        }
        self.by_norm.get(&norm).map(String::as_str)
    }
}

/// Normalize a company name for exact matching: uppercase, drop everything but
/// `[A-Z0-9 ]`, strip trailing corporate suffix tokens and a leading `THE`,
/// collapse whitespace.
fn normalize(name: &str) -> String {
    let upper = name.to_uppercase();
    let cleaned: String = upper
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { ' ' })
        .collect();
    let mut tokens: Vec<&str> = cleaned.split_whitespace().collect();
    if tokens.first() == Some(&"THE") {
        tokens.remove(0);
    }
    // Strip trailing suffix tokens (repeatedly: "FOO HOLDINGS INC" -> "FOO").
    while let Some(last) = tokens.last() {
        if SUFFIXES.contains(last) {
            tokens.pop();
        } else {
            break;
        }
    }
    tokens.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(ticker: &str, title: &str) -> SecEntry {
        SecEntry {
            ticker: ticker.into(),
            title: title.into(),
        }
    }

    #[test]
    fn normalize_strips_suffix_and_punct() {
        assert_eq!(normalize("LOCKHEED MARTIN CORP"), "LOCKHEED MARTIN");
        assert_eq!(normalize("Apple Inc."), "APPLE");
        assert_eq!(normalize("The Boeing Company"), "BOEING");
        assert_eq!(normalize("Acme Holdings, LLC"), "ACME");
    }

    #[test]
    fn exact_match_only() {
        let map = TickerMap::from_entries(
            [
                entry("LMT", "LOCKHEED MARTIN CORP"),
                entry("HUM", "HUMANA INC"),
            ]
            .into_iter(),
        );
        // exact normalized hit
        assert_eq!(map.lookup("Lockheed Martin Corporation"), Some("LMT"));
        // subsidiary must NOT match the parent (conservative)
        assert_eq!(map.lookup("HUMANA GOVERNMENT BUSINESS INC"), None);
        // unrelated private recipient
        assert_eq!(map.lookup("ACME RESEARCH LLC"), None);
    }
}
