//! USASpending.gov award-search client.
//!
//! Pages the public `POST /api/v2/search/spending_by_award/` endpoint for prime
//! contract awards (award type codes A/B/C/D) in an `action_date` window. No API
//! key. Field names and the `date_type:"action_date"` filter are the documented
//! contract-award mappings (`https://api.usaspending.gov/docs/`).

use anyhow::{bail, Context, Result};
use contractskit::Award;
use serde::Deserialize;
use serde_json::json;

const SEARCH_URL: &str = "https://api.usaspending.gov/api/v2/search/spending_by_award/";
/// Contract award type codes: A=BPA call, B=purchase order, C=delivery order,
/// D=definitive contract (the prime-contract family; excludes grants/loans).
const CONTRACT_AWARD_TYPE_CODES: [&str; 4] = ["A", "B", "C", "D"];
/// Page size; 100 is the documented maximum for this endpoint.
const PAGE_LIMIT: u32 = 100;
/// The API rejects deep pagination past page 500 (50k rows) for a single query;
/// stay one short so a single window never trips it.
const MAX_PAGE: u32 = 499;

/// One row of the contract-award search response. Only the fields the schema
/// needs are deserialized; unknown fields are ignored.
#[derive(Deserialize)]
struct Row {
    #[serde(rename = "Award ID")]
    award_id: Option<String>,
    #[serde(rename = "Recipient Name")]
    recipient_name: Option<String>,
    #[serde(rename = "Recipient UEI")]
    recipient_uei: Option<String>,
    #[serde(rename = "Awarding Agency")]
    awarding_agency: Option<String>,
    #[serde(rename = "Award Amount")]
    award_amount: Option<f64>,
    #[serde(rename = "Contract Award Type")]
    award_type: Option<String>,
    #[serde(rename = "naics_code")]
    naics_code: Option<String>,
    #[serde(rename = "Description")]
    description: Option<String>,
    // The award-search endpoint exposes no per-award "action date"; Last Modified
    // Date is the returnable field that tracks the most recent action on the award
    // and so stays consistent with the action_date window the query filters on.
    #[serde(rename = "Last Modified Date")]
    last_modified_date: Option<String>,
}

#[derive(Deserialize)]
struct PageMeta {
    #[serde(rename = "hasNext")]
    has_next: bool,
}

#[derive(Deserialize)]
struct SearchResponse {
    results: Vec<Row>,
    page_metadata: PageMeta,
}

/// The award fields requested from the search endpoint, in API spelling.
fn request_fields() -> Vec<&'static str> {
    vec![
        "Award ID",
        "Recipient Name",
        "Recipient UEI",
        "Awarding Agency",
        "Award Amount",
        "Contract Award Type",
        "naics_code",
        "Description",
        "Last Modified Date",
    ]
}

/// Fetch all contract awards with `action_date` in the inclusive `[start, end]`
/// window (ISO `YYYY-MM-DD`), at or above `min_amount` dollars. Pages until the
/// API reports no next page or the deep-pagination ceiling is reached.
async fn fetch_window(
    client: &reqwest::Client,
    start: &str,
    end: &str,
    min_amount: i64,
) -> Result<Vec<Award>> {
    let mut out = Vec::new();
    for page in 1..=MAX_PAGE {
        let body = json!({
            "filters": {
                "award_type_codes": CONTRACT_AWARD_TYPE_CODES,
                "time_period": [{
                    "start_date": start,
                    "end_date": end,
                    "date_type": "action_date",
                }],
                "award_amounts": [{ "lower_bound": min_amount }],
            },
            "fields": request_fields(),
            "page": page,
            "limit": PAGE_LIMIT,
            "sort": "Award Amount",
            "order": "desc",
        });

        let parsed = fetch_page(client, &body, page).await?;
        // Polite pacing: the public API throttles datacenter IPs under sustained
        // paging, so space successive pages out a little.
        tokio::time::sleep(std::time::Duration::from_millis(PAGE_PACING_MS)).await;

        for row in parsed.results {
            if let Some(a) = to_award(row, min_amount) {
                out.push(a);
            }
        }
        if !parsed.page_metadata.has_next {
            break;
        }
        if page == MAX_PAGE {
            tracing::warn!(
                start,
                end,
                "hit deep-pagination ceiling; window may be truncated, narrow the date range"
            );
        }
    }
    Ok(out)
}

/// Max attempts per page (initial + retries) on transient API failures.
const PAGE_ATTEMPTS: u32 = 8;
/// Pause between successive pages to stay under the public API's throttle.
const PAGE_PACING_MS: u64 = 120;

/// POST one page with retry on transient failures. USASpending intermittently
/// returns 5xx HTML error pages under load; a few backed-off retries clear it
/// rather than aborting a multi-thousand-page backfill on one blip.
async fn fetch_page(
    client: &reqwest::Client,
    body: &serde_json::Value,
    page: u32,
) -> Result<SearchResponse> {
    let mut last = String::new();
    for attempt in 1..=PAGE_ATTEMPTS {
        if attempt > 1 {
            let secs = 1u64 << (attempt - 1).min(5); // 1,2,4,8,16s
            tracing::warn!(page, attempt, secs, last = %last, "retrying award page");
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
        }
        match client.post(SEARCH_URL).json(body).send().await {
            Ok(resp) if resp.status().is_success() => match resp.json::<SearchResponse>().await {
                Ok(p) => return Ok(p),
                Err(e) => last = format!("decode: {e}"),
            },
            // 4xx (other than 429) is a request bug, not transient: fail fast.
            Ok(resp) if resp.status().is_client_error() && resp.status().as_u16() != 429 => {
                let code = resp.status();
                let detail = truncate(&resp.text().await.unwrap_or_default(), 300);
                bail!("award search page {page}: HTTP {code}: {detail}");
            }
            Ok(resp) => last = format!("HTTP {}", resp.status()),
            Err(e) => last = e.to_string(),
        }
    }
    bail!("award search page {page}: {PAGE_ATTEMPTS} attempts failed: {last}")
}

fn truncate(s: &str, n: usize) -> String {
    let s = s.trim();
    match s.char_indices().nth(n) {
        Some((idx, _)) => format!("{}…", &s[..idx]),
        None => s.to_string(),
    }
}

/// Fetch all contract awards with `action_date` in the inclusive `[start, end]`
/// ISO `YYYY-MM-DD` range, at or above `min_amount` dollars.
///
/// The endpoint caps any single query at ~50k rows (deep-pagination ceiling),
/// and a busy month of $100k+ federal contracts is ~35k rows, so the range is
/// split into calendar-month sub-windows that each stay under the ceiling.
pub async fn fetch_range(
    client: &reqwest::Client,
    start: &str,
    end: &str,
    min_amount: i64,
) -> Result<Vec<Award>> {
    let mut out = Vec::new();
    for (ws, we) in month_windows(start, end)? {
        out.extend(fetch_window(client, &ws, &we, min_amount).await?);
    }
    Ok(out)
}

/// Split an inclusive ISO date range into per-calendar-month `[start, end]`
/// windows clamped to the original bounds.
fn month_windows(start: &str, end: &str) -> Result<Vec<(String, String)>> {
    let (sy, sm, sd) = parse_ymd(start).context("invalid start date")?;
    let (ey, em, ed) = parse_ymd(end).context("invalid end date")?;
    if (sy, sm, sd) > (ey, em, ed) {
        return Ok(Vec::new());
    }
    let mut wins = Vec::new();
    let (mut y, mut m) = (sy, sm);
    while (y, m) <= (ey, em) {
        let first = if (y, m) == (sy, sm) { sd } else { 1 };
        let last = if (y, m) == (ey, em) {
            ed
        } else {
            days_in_month(y, m)
        };
        wins.push((
            format!("{y:04}-{m:02}-{first:02}"),
            format!("{y:04}-{m:02}-{last:02}"),
        ));
        if m == 12 {
            y += 1;
            m = 1;
        } else {
            m += 1;
        }
    }
    Ok(wins)
}

fn parse_ymd(s: &str) -> Option<(i32, u32, u32)> {
    let d = s.get(..10).unwrap_or(s);
    let mut p = d.split('-');
    let y = p.next()?.parse().ok()?;
    let m = p.next()?.parse().ok()?;
    let day = p.next()?.parse().ok()?;
    if (1..=12).contains(&m) && (1..=31).contains(&day) {
        Some((y, m, day))
    } else {
        None
    }
}

fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) => 29,
        2 => 28,
        _ => 30,
    }
}

/// Convert one API row to an [`Award`], dropping rows missing the required
/// award id or date, or below the amount floor.
fn to_award(row: Row, min_amount: i64) -> Option<Award> {
    let award_id = row.award_id.filter(|s| !s.is_empty())?;
    let action_date = parse_iso_date(row.last_modified_date.as_deref()?)?;
    let amount_usd = row.award_amount.map(|a| a.round() as i64).unwrap_or(0);
    if amount_usd < min_amount {
        return None;
    }
    Some(Award {
        action_date,
        award_id,
        recipient_name: clean(row.recipient_name),
        recipient_uei: clean(row.recipient_uei),
        ticker: String::new(),
        // The award-search endpoint does not return parent recipient; left empty.
        parent_recipient: String::new(),
        awarding_agency: clean(row.awarding_agency),
        amount_usd,
        award_type: clean(row.award_type),
        naics_code: clean(row.naics_code),
        description: clean(row.description),
    })
}

fn clean(s: Option<String>) -> String {
    s.unwrap_or_default().trim().to_string()
}

/// Parse `YYYY-MM-DD` (the API date format) into an `i32` `YYYYMMDD`.
fn parse_iso_date(s: &str) -> Option<i32> {
    let d = s.get(..10).unwrap_or(s);
    let mut parts = d.split('-');
    let y: i32 = parts.next()?.parse().ok()?;
    let m: i32 = parts.next()?.parse().ok()?;
    let day: i32 = parts.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&day) {
        return None;
    }
    Some(y * 10_000 + m * 100 + day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn month_windows_split_and_clamp() {
        let w = month_windows("2024-02-15", "2024-04-10").unwrap();
        assert_eq!(
            w,
            vec![
                ("2024-02-15".into(), "2024-02-29".into()), // leap-year clamp
                ("2024-03-01".into(), "2024-03-31".into()),
                ("2024-04-01".into(), "2024-04-10".into()),
            ]
        );
        // single-month range
        assert_eq!(
            month_windows("2023-06-05", "2023-06-20").unwrap(),
            vec![("2023-06-05".into(), "2023-06-20".into())]
        );
        // reversed range yields nothing
        assert!(month_windows("2024-05-01", "2024-04-01")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn parses_iso_date_with_and_without_time() {
        assert_eq!(parse_iso_date("2024-01-15"), Some(20240115));
        assert_eq!(parse_iso_date("2016-08-01 00:00:00"), Some(20160801));
        assert_eq!(parse_iso_date("garbage"), None);
        assert_eq!(parse_iso_date("2024-13-01"), None);
    }

    #[test]
    fn row_below_floor_dropped() {
        let row = Row {
            award_id: Some("A1".into()),
            recipient_name: Some("ACME".into()),
            recipient_uei: None,
            awarding_agency: Some("DoD".into()),
            award_amount: Some(50_000.49),
            award_type: Some("DEFINITIVE CONTRACT".into()),
            naics_code: Some("336411".into()),
            description: None,
            last_modified_date: Some("2024-01-15 09:00:00".into()),
        };
        assert!(to_award(row, 100_000).is_none());
    }

    #[test]
    fn row_rounds_amount_and_keeps_required_fields() {
        let row = Row {
            award_id: Some("A1".into()),
            recipient_name: Some("ACME".into()),
            recipient_uei: Some("U1".into()),
            awarding_agency: Some("DoD".into()),
            award_amount: Some(100_000.49),
            award_type: Some("DEFINITIVE CONTRACT".into()),
            naics_code: Some("336411".into()),
            description: None,
            last_modified_date: Some("2024-01-15 09:00:00".into()),
        };
        let a = to_award(row, 100_000).unwrap();
        assert_eq!(a.amount_usd, 100_000);
        assert_eq!(a.action_date, 20240115);
        assert_eq!(a.description, "");
    }
}
