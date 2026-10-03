//! The retention guard: a broker that keeps records for seconds, where the
//! relay needs days, makes the relay warn at startup and on the admin page.
//!
//! Needs `dev/up.sh --retention` and `RELAY_TEST_RETENTION=1`; without that
//! it does nothing.

mod common;

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use common::{ALICE, Relay, TENANT, eventually, id_token, unique};
use serde_json::json;

#[tokio::test]
#[ignore = "needs the short-retention stack"]
async fn short_retention_is_reported() {
    if std::env::var("RELAY_TEST_RETENTION").as_deref() != Ok("1") {
        eprintln!("skipped: start dev/up.sh --retention and set RELAY_TEST_RETENTION=1");
        return;
    }
    let source = unique("retention");
    let admin = Arc::new(Relay::start("intake,admin", &[]).await);
    let token = admin
        .create_source(&source, json!({ "scheme": { "type": "token" } }))
        .await
        .unwrap();
    let path = format!("/in/{TENANT}/{source}/{token}");
    let body = vec![b'x'; 1024];
    for _ in 0..300 {
        assert_eq!(
            admin.send(&path, &[], &body).await.status(),
            StatusCode::ACCEPTED
        );
    }

    // Retention runs every second and keeps five; the page warns once the
    // oldest segments are gone.
    let page = eventually("the page's retention warning", async || {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let html = admin
            .http
            .get(admin.url(&format!("/admin/{TENANT}")))
            .bearer_auth(id_token(ALICE).await)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        html.contains(&format!("<b>Retention:</b> source {source}:"))
            .then_some(html)
    })
    .await;
    assert!(page.contains("FELIX_DURABLE_RETENTION_SECONDS"));

    let log_path = std::env::temp_dir().join(format!("{source}.log"));
    let log = std::fs::File::create(&log_path).unwrap();
    let worker = Relay::start_logging(
        "deliver",
        &[("RELAY_ENDPOINT_PREFIXES", &source)],
        Stdio::from(log),
    )
    .await;
    drop(worker);
    let log = std::fs::read_to_string(&log_path).unwrap();
    let warning = log
        .lines()
        .find(|line| line.contains("retention:") && line.contains(&source));
    assert!(warning.is_some(), "no warning at startup");
    let _ = std::fs::remove_file(log_path);
}
