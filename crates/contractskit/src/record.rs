//! The federal contract-award record.
//!
//! One [`Award`] is one prime contract award (or modification) reported on
//! USASpending.gov. `action_date` is stored as `i32` `YYYYMMDD` (e.g.
//! `20240401`) so comparisons are integer-cheap and need no calendar library on
//! the hot path. `amount_usd` is the award amount rounded to whole dollars
//! (`i64`). `ticker` is the best-effort SEC stock symbol of the recipient and
//! is empty when no confident match exists, which is the common case because
//! most federal recipients are private or non-public entities.
use serde::{Deserialize, Serialize};

/// One federal contract award (one row in the bundled parquet).
///
/// Empty strings, never nulls, mark absent text fields. `action_date` is `0`
/// only when the source omits a usable award date. `ticker` is empty unless the
/// recipient name matched an SEC-listed company exactly after normalization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Award {
    /// Most recent action date on the award as `i32` `YYYYMMDD`. The
    /// award-search source exposes no per-award action field, so this is the
    /// award's last-modified date, which tracks the latest action and matches
    /// the `action_date` window the data is collected over.
    pub action_date: i32,
    /// Procurement Instrument Identifier (PIID) of the award.
    pub award_id: String,
    /// Recipient legal business name as reported to the government.
    pub recipient_name: String,
    /// Recipient Unique Entity Identifier (SAM.gov UEI); empty if absent.
    pub recipient_uei: String,
    /// Best-effort SEC ticker of the recipient; empty when no confident match.
    pub ticker: String,
    /// Parent (ultimate-owner) recipient name; empty when not reported.
    pub parent_recipient: String,
    /// Top-level awarding agency (e.g. `Department of Defense`).
    pub awarding_agency: String,
    /// Award amount in whole US dollars.
    pub amount_usd: i64,
    /// Contract award type (e.g. `DEFINITIVE CONTRACT`, `BPA CALL`).
    pub award_type: String,
    /// 6-digit NAICS industry code; empty when not reported.
    pub naics_code: String,
    /// Free-text award description; empty when not reported.
    pub description: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_serde() {
        let a = Award {
            action_date: 20240115,
            award_id: "HT940216C0001".into(),
            recipient_name: "LOCKHEED MARTIN CORP".into(),
            recipient_uei: "FYHNA5WC8XD7".into(),
            ticker: "LMT".into(),
            parent_recipient: String::new(),
            awarding_agency: "Department of Defense".into(),
            amount_usd: 51269205263,
            award_type: "DEFINITIVE CONTRACT".into(),
            naics_code: "336411".into(),
            description: "AIRCRAFT".into(),
        };
        let json = serde_json::to_string(&a).unwrap();
        assert_eq!(serde_json::from_str::<Award>(&json).unwrap(), a);
    }
}
