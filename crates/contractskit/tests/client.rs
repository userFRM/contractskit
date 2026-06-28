//! End-to-end: serve a manifest + a real parquet shard, then confirm the
//! client fetches, reads, and filters it.

use contractskit::{write_awards, Award, Contractskit};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn award(recipient: &str, ticker: &str, agency: &str, amount: i64, date: i32) -> Award {
    Award {
        action_date: date,
        award_id: format!("AWD{date}"),
        recipient_name: recipient.into(),
        recipient_uei: "ABCDEF123456".into(),
        ticker: ticker.into(),
        parent_recipient: String::new(),
        awarding_agency: agency.into(),
        amount_usd: amount,
        award_type: "DEFINITIVE CONTRACT".into(),
        naics_code: "336411".into(),
        description: "WIDGETS".into(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn client_reads_served_parquet() {
    let dir = tempfile::TempDir::new().unwrap();
    let shard_path = dir.path().join("contracts-2024.parquet");
    let rows = vec![
        award(
            "LOCKHEED MARTIN CORP",
            "LMT",
            "Department of Defense",
            9_000,
            20240201,
        ),
        award(
            "LOCKHEED MARTIN CORP",
            "LMT",
            "Department of Defense",
            5_000,
            20240105,
        ),
        award(
            "ACME RESEARCH LLC",
            "",
            "Department of Energy",
            7_000,
            20240115,
        ),
    ];
    write_awards(&shard_path, &rows).unwrap();
    let parquet = std::fs::read(&shard_path).unwrap();
    let digest = sha256_hex(&parquet);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/manifest.json"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!(r#"{{"contracts-2024.parquet":"sha256:{digest}"}}"#)),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/contracts-2024.parquet"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(parquet))
        .mount(&server)
        .await;

    let cache = tempfile::TempDir::new().unwrap();
    let client = Contractskit::new()
        .with_base_url(server.uri())
        .with_cache_dir(cache.path().to_path_buf())
        .with_mirror_url(None);

    // ticker match, sorted most-recent first
    let lmt = client.contracts_for("lmt").await.unwrap();
    assert_eq!(lmt.len(), 2, "two LMT rows");
    assert_eq!(lmt[0].action_date, 20240201, "sorted most-recent first");

    // recipient-name substring fallback (no ticker)
    let acme = client.contracts_for("acme").await.unwrap();
    assert_eq!(acme.len(), 1);
    assert_eq!(acme[0].awarding_agency, "Department of Energy");

    let doe = client.by_agency("Energy").await.unwrap();
    assert_eq!(doe.len(), 1);

    let latest = client.latest(2).await.unwrap();
    assert_eq!(latest.len(), 2);
    assert_eq!(latest[0].action_date, 20240201);

    // largest in window, biggest first
    let biggest = client.largest(20240101, 20241231, 1).await.unwrap();
    assert_eq!(biggest.len(), 1);
    assert_eq!(biggest[0].amount_usd, 9_000);
}
