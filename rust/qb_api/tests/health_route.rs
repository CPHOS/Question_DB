use axum::{body::Body, http::{Request, StatusCode}};
use http_body_util::BodyExt;
use qb_api::api::{router, AppState};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

#[tokio::test]
async fn health_route_returns_ok_json() {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://postgres:postgres@localhost/qb")
        .unwrap();
    let app = router(AppState { pool });

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap();
    assert!(content_type.starts_with("application/json"));

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json.get("status").and_then(|v| v.as_str()), Some("ok"));
    assert_eq!(
        json.get("service").and_then(|v| v.as_str()),
        Some("qb_api_rust")
    );
}
